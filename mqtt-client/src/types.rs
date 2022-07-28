use dotenv::dotenv;

use crate::utils;

/// Custom payload produced by get_payload
#[derive(Debug)]
pub struct WriteJob {
    pub path: String,
    pub payload: String,
    pub n_per_file: i32,
}

impl Default for WriteJob {
    fn default() -> Self {
        WriteJob {
            path: "".to_string(),
            payload: "".to_string(),
            n_per_file: 1,
        }
    }
}

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
        let client_id = utils::env_default("MQTT_CLIENT", "rust_client");
        let username = utils::env_default("MQTT_USERNAME", "");
        let password = utils::env_default("MQTT_PASSWORD", "");
        let lwt_topic = utils::env_default("MQTT_LWT_TOPIC", "lwt");
        let lwt_payload = utils::env_default("MQTT_LWT_PAYLOAD", "Last will for 'rust_client'");

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
