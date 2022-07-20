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
}

fn value_to_string(v: &Value) -> String {
    return v.to_string().trim().replace("\"", "");
}

fn get_msg_path(msg: &mqtt::Message) -> Result<String> {
    let topic: &str = msg.topic();
    let payload: Value = serde_json::from_str(&msg.payload_str())?;

    let now = Utc::now();

    // Handle PackML
    if topic.starts_with("packml") {
        let telegram_type = &payload["telegramTypeFriendly"];
        let machine_idx = &payload["machineIDx"];
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
            return Ok(path);
        } else if topic.contains("status") && service_name != &Value::Null {
            let path = format!(
                "packml/status/service_name={}",
                value_to_string(&service_name),
            );
            log::debug!("Created path {path}");
            return Ok(path);
        }
    } else if topic.starts_with("service") {
        let host = &payload["Host"];
        if topic.contains("status") && host != &Value::Null {
            let path = format!("master/status/host={}", value_to_string(&host));
            log::debug!("Created path {path}");
            return Ok(path);
        }
    }

    Ok(String::new())
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

    let (tx, rx) = mpsc::channel();

    thread::spawn(move || {
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
                let path = get_msg_path(&msg).expect("Error getting path");
                let topic = msg.topic();
                let payload_str: Value = serde_json::from_str(&msg.payload_str()).expect("Hello");
                log::info!("{}: {} - {}", path, topic, payload_str);
                tx.send(MqttPayload {
                    path: path,
                    payload: payload_str.to_string(),
                });
            }
        });

        // Define the set of options for the connection
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

        // Just wait for incoming messages.
        loop {
            thread::sleep(Duration::from_millis(1000));
        }

        // Hitting ^C will exit the app and cause the broker to publish the
        // LWT message since we're not disconnecting cleanly.
    });

    let data_lake_client = adls::create_data_lake_client().await?;
    let mut map: HashMap<String, Vec<String>> = HashMap::new();

    for received in rx {
        if received.path != "" {
            let key = received.path.to_string();
            let kp = key.clone();

            match map.entry(key) {
                Entry::Vacant(e) => {
                    e.insert(vec![received.payload]);
                }
                Entry::Occupied(mut e) => {
                    let v = e.get_mut();
                    v.push(received.payload);
                    if v.len() == 2 {
                        let now = Utc::now();
                        log::info!("Flushing data from {}: {:?}", &kp, v);
                        adls::upload_data_multiple(
                            &data_lake_client,
                            "raw".to_string(),
                            format!("rust-tests/{}/{}.json", &kp, &now.timestamp()),
                            v.to_vec(),
                        )
                        .await?;
                        v.clear();
                    }
                }
            };
            log::debug!("Current Map: {:?}", map);
        }
    }

    Ok(())
}
