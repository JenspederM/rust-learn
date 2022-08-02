use crate::{adls, utils};
use chrono::{Datelike, Utc};
use dotenv::dotenv;
use paho_mqtt as mqtt;
use serde_json::{Result, Value};
use std::{process, sync::mpsc::Sender, thread, thread::JoinHandle, time::Duration};

/// Connection options for MQTT Client.
#[derive(Debug)]
pub struct MqttConnectOptions {
    pub broker: String,
    pub client_id: String,
    pub username: String,
    pub password: String,
    pub lwt_topic: String,
    pub lwt_payload: String,
}

impl Default for MqttConnectOptions {
    fn default() -> MqttConnectOptions {
        dotenv().ok();

        let broker = utils::env_default("MQTT_BROKER", "tcp://localhost:1883");
        let client_id = utils::env_default("MQTT_CLIENT_ID", "rust_client");
        let username = utils::env_default("MQTT_USERNAME", "");
        let password = utils::env_default("MQTT_PASSWORD", "");
        let lwt_topic = utils::env_default("MQTT_LWT_TOPIC", "lwt");
        let lwt_payload_default = format!("Last will for {}", client_id);
        let lwt_payload = utils::env_default("MQTT_LWT_PAYLOAD", lwt_payload_default.as_str());

        MqttConnectOptions {
            broker,
            client_id,
            username,
            password,
            lwt_topic,
            lwt_payload,
        }
    }
}

/// Contruct MqttPayload based on Topic.
///
/// Takes an `mqtt:Message` and constructs a `MqttPayload` based on the topic
/// from which the `mqtt::Message` is sent.
fn get_payload(msg: &mqtt::Message) -> Result<adls::WriteJob> {
    // Get current time
    let now = Utc::now();

    // Get the message payload and topic
    let payload: Value = serde_json::from_str(&msg.payload_str())?;
    let topic = msg.topic();
    // Get the payload as a String
    let payload_str: Value = serde_json::from_str(&msg.payload_str()).expect("Hello");
    // Set default path
    let mut path = String::from("");
    // Default for number of files to write at a time.
    let mut n_per_file = 1;

    // Handle PackML
    if topic.starts_with("packml") {
        let machine_idx = &payload["machineIDx"];
        let telegram_type = &payload["telegramTypeFriendly"];
        let telegram_version = &payload["telegramTypeVersion"];
        let service_name = &payload["ServiceName"];

        if topic.contains("event")
            && telegram_type != &Value::Null
            && machine_idx != &Value::Null
            && telegram_version != &Value::Null
        {
            path = format!(
                "packml/event/telegram_type={}/telegram_version={}/machine_idx={}/year={}/month={}/day={}",
                utils::value_to_string(&telegram_type),
                utils::value_to_string(&telegram_version),
                utils::value_to_string(&machine_idx),
                now.year(),
                now.month(),
                now.day()
            );
            n_per_file = 10;
            log::debug!("packml.contains('event') {n_per_file} files per path {path}");
        } else if topic.contains("status") && service_name != &Value::Null {
            path = format!(
                "packml/status/service_name={}",
                utils::value_to_string(&service_name),
            );
            log::debug!("packml.contains('status') {n_per_file} files per path {path}");
        }
    } else if topic.starts_with("service") {
        let host = &payload["Host"];
        if topic.contains("status") && host != &Value::Null {
            path = format!("master/status/host={}", utils::value_to_string(&host));
            log::debug!("service.contains('status') {n_per_file} files per path {path}");
        }
    }

    let payload = adls::WriteJob {
        path: path,
        payload: payload_str.to_string(),
        n_per_file: n_per_file,
    };
    log::debug!("{:?}", payload);

    Ok(payload)
}

/// Callback for a successful connection to the broker.
/// We subscribe to the topic(s) we want here.
fn on_connect_success(cli: &mqtt::AsyncClient, _msgid: u16) {
    log::info!("Connection succeeded");
    // Subscribe to the desired topic(s).
    //cli.subscribe_many(TOPICS, vec!(1, 1));
    // Since we are using subscribe_many, we have to transform the comma-separated
    // list of topics into a Vector of Strings.
    let topics: Vec<String> = utils::env_default("MQTT_TOPICS", "#")
        .split(",")
        .map(|el| el.trim().to_string())
        .collect();

    // TODO: Implement customizable QOS
    // Currently we default to QOS 1 for all topics.
    let qos: Vec<i32> = vec![1; topics.len()];

    cli.subscribe_many(&topics, &qos);
    log::info!("Subscribing to topics: {:?}", topics);
    // TODO: This doesn't yet handle a failed subscription.
}

/// Callback for a failed attempt to connect to the server.
/// We simply sleep and then try again.
///
/// Note that normally we don't want to do a blocking operation or sleep
/// from  within a callback. But in this case, we know that the client is
/// *not* conected, and thus not doing anything important. So we don't worry
/// too much about stopping its callback thread.
fn on_connect_failure(cli: &mqtt::AsyncClient, _msgid: u16, rc: i32) {
    log::warn!("Connection attempt failed with error code {}.\n", rc);
    thread::sleep(Duration::from_millis(2500));
    cli.reconnect_with_callbacks(on_connect_success, on_connect_failure);
}

pub fn start_mqtt_thread(tx: Sender<adls::WriteJob>) -> JoinHandle<()> {
    // Send MQTT client to it's own thread.
    let handle = thread::spawn(move || {
        // By default, values are loaded from env. See <MqttConnectOptions>
        let mqtt_connect_options: MqttConnectOptions = MqttConnectOptions {
            ..MqttConnectOptions::default()
        };

        // Create the client. Use an ID for a persistent session.
        // A real system should try harder to use a unique ID.
        let create_opts = mqtt::CreateOptionsBuilder::new()
            .server_uri(mqtt_connect_options.broker)
            .client_id(mqtt_connect_options.client_id)
            .finalize();

        // Create the client connection
        let cli = mqtt::AsyncClient::new(create_opts).unwrap_or_else(|e| {
            log::error!("Error creating the client: {:?}", e);
            process::exit(1);
        });

        // Set a closure to be called whenever the client loses the connection.
        // It will attempt to reconnect, and set up function callbacks to keep
        // retrying until the connection is re-established.
        cli.set_connection_lost_callback(|cli: &mqtt::AsyncClient| {
            log::warn!("Connection lost. Attempting reconnect.");
            thread::sleep(Duration::from_millis(2500));
            cli.reconnect_with_callbacks(on_connect_success, on_connect_failure);
        });

        // Attach a closure to the client to receive callback
        // on incoming messages.
        #[allow(unused)]
        cli.set_message_callback(move |_cli, msg| {
            if let Some(msg) = msg {
                // Get the path for the message
                let payload = get_payload(&msg).expect("Error getting path");

                // Send MqttPayload with the path and payload to main thread.
                tx.send(payload);
            }
        });

        // Set Last Will Message. This will be triggered if the client disconnects abrubtly.
        let lwt = mqtt::Message::new(
            mqtt_connect_options.lwt_topic,
            mqtt_connect_options.lwt_payload,
            1,
        );

        // Define Connection Options
        let conn_opts = mqtt::ConnectOptionsBuilder::new()
            .keep_alive_interval(Duration::from_secs(20))
            .clean_session(true)
            .will_message(lwt)
            .user_name(&mqtt_connect_options.username)
            .password(&mqtt_connect_options.password)
            .finalize();

        // Make the connection to the broker
        log::info!("Connecting to the MQTT server...");
        cli.connect_with_callbacks(conn_opts, on_connect_success, on_connect_failure);

        // Wait for incoming messages.
        loop {
            thread::sleep(Duration::from_millis(1000));
        }

        // Hitting ^C will exit the app and cause the broker to publish the
        // LWT message since we're not disconnecting cleanly.
    });

    return handle;
}
