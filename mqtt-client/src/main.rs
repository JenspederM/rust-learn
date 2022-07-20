extern crate env_logger;
extern crate log;
extern crate paho_mqtt as mqtt;

use chrono::{Datelike, Utc};
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::mpsc;
use std::time::Duration;
use std::{process, thread};

use dotenv::dotenv;
use serde_json::{Result, Value};

mod adls;
mod utils;

#[derive(Debug)]
pub struct MqttPayload {
    pub path: String,
    pub payload: String,
    pub n_per_file: i32,
}

fn value_to_string(v: &Value) -> String {
    return v.to_string().trim().replace("\"", "");
}

fn get_payload(msg: &mqtt::Message) -> Result<MqttPayload> {
    // Get current time
    let now = Utc::now();

    // Get the message payload and topic
    let payload: Value = serde_json::from_str(&msg.payload_str())?;
    let topic = msg.topic();
    // Get the payload as a String
    let payload_str: Value = serde_json::from_str(&msg.payload_str()).expect("Hello");
    // Set default path
    let path = String::from("");

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
            let path = format!(
                "packml/event/telegram_type={}/telegram_version={}/machine_idx={}/year={}/month={}/day={}",
                value_to_string(&telegram_type),
                value_to_string(&telegram_version),
                value_to_string(&machine_idx),
                now.year(),
                now.month(),
                now.day()
            );
            log::debug!("Created path: {path}");
        } else if topic.contains("status") && service_name != &Value::Null {
            let path = format!(
                "packml/status/service_name={}",
                value_to_string(&service_name),
            );
            log::debug!("Created path {path}");
        }
    } else if topic.starts_with("service") {
        let host = &payload["Host"];
        if topic.contains("status") && host != &Value::Null {
            let path = format!("master/status/host={}", value_to_string(&host));
            log::debug!("Created path {path}");
        }
    }

    let payload = MqttPayload {
        path: path,
        payload: payload_str.to_string(),
        n_per_file: 2,
    };
    log::info!("{:?}", payload);

    Ok(payload)
}

// Callback for a successful connection to the broker.
// We subscribe to the topic(s) we want here.
fn on_connect_success(cli: &mqtt::AsyncClient, _msgid: u16) {
    log::info!("Connection succeeded");
    // Subscribe to the desired topic(s).
    //cli.subscribe_many(TOPICS, vec!(1, 1));
    // Since we are using subscribe_many, we have to transform the comma-separated
    // list of topics into a Vector of Strings.
    let topics: Vec<String> = utils::get_env("MQTT_TOPICS")
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

// Callback for a failed attempt to connect to the server.
// We simply sleep and then try again.
//
// Note that normally we don't want to do a blocking operation or sleep
// from  within a callback. But in this case, we know that the client is
// *not* conected, and thus not doing anything important. So we don't worry
// too much about stopping its callback thread.
fn on_connect_failure(cli: &mqtt::AsyncClient, _msgid: u16, rc: i32) {
    log::warn!("Connection attempt failed with error code {}.\n", rc);
    thread::sleep(Duration::from_millis(2500));
    cli.reconnect_with_callbacks(on_connect_success, on_connect_failure);
}

/////////////////////////////////////////////////////////////////////////////

struct MqttConnectOptions {
    broker: String,
    client_id: String,
    username: String,
    password: String,
    lwt_topic: String,
    lwt_payload: String,
}

impl Default for MqttConnectOptions {
    fn default() -> MqttConnectOptions {
        dotenv().ok();

        let broker = utils::get_env("MQTT_BROKER");
        let client_id = utils::get_env("MQTT_CLIENT");
        let username = utils::get_env("MQTT_USERNAME");
        let password = utils::get_env("MQTT_PASSWORD");
        let lwt_topic = utils::get_env("MQTT_LWT_TOPIC");
        let lwt_payload = utils::get_env("MQTT_LWT_PAYLOAD");

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

#[tokio::main]
async fn main() -> azure_core::error::Result<()> {
    // Initialize Logging
    utils::init_log();

    // Create Sender and Receiver to pass messages between two threads.
    // One thread will run the MQTT client, and the other will send messages to ADLS.
    let (tx, rx) = mpsc::channel();

    // Send MQTT client to it's own thread.
    thread::spawn(move || {
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

    // Create client to interact with the datalake.
    let data_lake_client = adls::create_data_lake_client().await?;
    // Initialize HashMap (dictionary) to hold <path, Vec<payload_str>>
    let mut map: HashMap<String, Vec<String>> = HashMap::new();

    // For every message received by rx Receiver
    for received in rx {
        log::debug!("Received: {:?}", received);
        // If there is a path
        if received.path != "" {
            // Match on the path
            match map.entry(received.path.to_string()) {
                // If the path doesn't exist, then insert a vector with the payoad
                Entry::Vacant(e) => {
                    log::debug!("entry {} is vacant!", e.key());
                    e.insert(vec![received.payload]);
                }
                // If the path does exist
                Entry::Occupied(mut e) => {
                    log::debug!("entry {} is occupied!", e.key());
                    // Get the current vector of payloads from the topic as a mutable
                    let v = e.get_mut();
                    // Add the newly received payload to the vector
                    v.push(received.payload);
                    // If we have reached our write limit we flush our data
                    if v.len() as i32 == received.n_per_file {
                        log::info!("Flushing data from {}: {:?}", &received.path.to_string(), v);
                        // Upload multiline json to datalake
                        adls::upload_json_multiline(
                            &data_lake_client,
                            "raw".to_string(),
                            format!("rust-tests/{}", &received.path.to_string()),
                            v.to_vec(),
                            "json".to_string(),
                        )
                        .await?;
                        // Clear vector for new messages
                        v.clear();
                    }
                }
            };
            log::debug!("Current Map: {:?}", map);
        }
    }
    Ok(())
}
