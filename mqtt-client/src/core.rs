use std::{env, process, thread, time::Duration};

use mqtt::{Message, Receiver};
use paho_mqtt as mqtt;

use dotenv::dotenv;

use crate::packml;

pub struct MqttClient {
    pub broker: String,
    pub client_id: String,
    pub username: String,
    password: String,
    pub topics: Vec<String>,
    pub qos: Vec<i32>,
    pub lwt_topic: String,
    pub lwt_payload: String,
    pub client: mqtt::Client,
    pub rx: Receiver<Option<Message>>,
}

pub fn get_env(key: &str) -> String {
    let var = env::var(&key);

    match var {
        Ok(var) => var,
        Err(e) => {
            panic!("{} => {}", &key, e)
        }
    }
}

pub fn init_log() {
    dotenv().ok();

    match env::var("RUST_LOG") {
        Ok(s) => {
            info!("'RUST_LOG' set to '{s}'")
        }
        Err(_) => {
            info!("'RUST_LOG' not set. Defaults to 'info'");
            env::set_var("RUST_LOG", "info");
        }
    }

    env_logger::init();
}

impl MqttClient {
    pub fn new() -> Self {
        dotenv().ok();

        let broker = get_env("MQTT_BROKER");
        let client_id = get_env("MQTT_CLIENT");
        let username = get_env("MQTT_USERNAME");
        let password = get_env("MQTT_PASSWORD");
        let lwt_topic = get_env("MQTT_LWT_TOPIC");
        let lwt_payload = get_env("MQTT_LWT_PAYLOAD");

        // Since we are using subscribe_many, we have to transform the comma-separated
        // list of topics into a Vector of Strings.
        let topics: Vec<String> = get_env("MQTT_TOPICS")
            .split(",")
            .map(|el| el.trim().to_string())
            .collect();

        // TODO: Implement customizable QOS
        // Currently we default to QOS 1 for all topics.
        let mut qos: Vec<i32> = Vec::new();

        for _ in &topics {
            qos.append(&mut vec![1])
        }

        let client = Self::create_client(&broker, &client_id);
        let rx = client.start_consuming();

        info!("Initializing client...");

        let init = MqttClient {
            broker: broker,
            client_id: client_id,
            username: username,
            password: password,
            qos: qos,
            topics: topics,
            lwt_topic: lwt_topic,
            lwt_payload: lwt_payload,
            client: client,
            rx: rx,
        };

        Self::connect_client(&init);

        init
    }

    #[allow(dead_code)]
    pub fn start_consuming(&self) -> Receiver<Option<Message>> {
        info!("Start consuming...");
        self.client.start_consuming()
    }

    fn create_client(broker: &str, client_id: &str) -> mqtt::Client {
        info!("Creating MQTT Client for '{broker}' with ID '{client_id}'");
        // Define the set of options for the create.
        // Use an ID for a persistent session.
        let create_opts = mqtt::CreateOptionsBuilder::new()
            .server_uri(broker)
            .client_id(client_id)
            .finalize();

        // Create a client.
        return mqtt::Client::new(create_opts).unwrap_or_else(|err| {
            error!("{}", err);
            panic!("{}", err);
        });
    }

    fn connect_client(&self) -> &mqtt::Client {
        // Define last will

        let lwt = mqtt::MessageBuilder::new()
            .topic(self.lwt_topic.as_str())
            .payload(self.lwt_payload.as_str())
            .finalize();

        info!("Last Will: {}", lwt);

        // Define Connection Options
        let conn_opts = mqtt::ConnectOptionsBuilder::new()
            .keep_alive_interval(Duration::from_secs(20))
            .clean_session(true)
            .will_message(lwt)
            .user_name(&self.username)
            .password(&self.password)
            .finalize();

        // Connect and wait for it to complete or fail.
        match self.client.connect(conn_opts) {
            Ok(_) => {
                info!("Connection succesful!");
            }
            Err(e) => {
                error!("Unable to connect:\n\t{:?}", e);
                process::exit(1);
            }
        };

        return &self.client;
    }

    /// Reconnect to the broker when connection is lost.
    pub fn try_reconnect(&self) -> bool {
        warn!("Connection lost. Waiting to retry connection");

        let limit = 12;
        for i in 0..limit {
            info!("Retrying {i} of {limit} times");
            thread::sleep(Duration::from_millis(5000));
            if self.client.reconnect().is_ok() {
                info!("Successfully reconnected");
                return true;
            }
        }
        error!("Unable to reconnect after several attempts.");
        false
    }

    // Subscribes to multiple topics.
    pub fn subscribe_topics(&self) {
        match self.client.subscribe_many(&self.topics, &self.qos) {
            Ok(_) => {
                info!("Subscribing to topics: {:?}!", &self.topics);
            }
            Err(e) => {
                error!("Error subscribes topics: {:?}", e);
                process::exit(1);
            }
        }
    }

    pub fn disconnect(&self) {
        if self.client.is_connected() {
            info!("Disconnecting");
            self.client.unsubscribe_many(&self.topics).unwrap();
            self.client.disconnect(None).unwrap();
        }
        info!("Exiting");
    }

    pub fn process_messages(&self) {
        info!("Processing requests...");
        for msg in self.rx.iter() {
            if let Some(msg) = msg {
                let msg_topic = msg.topic();

                debug!("Received msg on {}", msg_topic);

                if msg_topic.starts_with("packml") {
                    if msg_topic.contains("status") {
                        packml::telegrams::handle_status(msg)
                            .expect("Error Processing PackML Status");
                    } else if msg_topic.contains("event") {
                        packml::telegrams::handle_event(msg)
                            .expect("Error Processing PackML Event");
                    }
                } else if msg_topic.starts_with("service") {
                    if msg_topic.contains("status") {
                        packml::master_service::handle_status(msg)
                            .expect("Error Processing Master Service Status");
                    }
                } else {
                    warn!("Unknown topic... {msg_topic}")
                }
            } else if !self.client.is_connected() {
                if self.try_reconnect() {
                    println!("Resubscribe topics...");
                    self.subscribe_topics();
                } else {
                    break;
                }
            }
        }
    }
}
