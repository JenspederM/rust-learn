use std::{process, thread, time::Duration};

use paho_mqtt as mqtt;

use dotenv::dotenv;

mod packml;

#[macro_use]
extern crate log;

// Reconnect to the broker when connection is lost.
fn try_reconnect(client: &mqtt::Client) -> bool {
    warn!("Connection lost. Waiting to retry connection");

    let limit = 12;
    for i in 0..limit {
        info!("Retrying {i} of {limit} times");
        thread::sleep(Duration::from_millis(5000));
        if client.reconnect().is_ok() {
            info!("Successfully reconnected");
            return true;
        }
    }
    error!("Unable to reconnect after several attempts.");
    false
}

// Subscribes to multiple topics.
fn subscribe_topics(cli: &mqtt::Client, topics: &[String], qos: &[i32]) {
    if let Err(e) = cli.subscribe_many(topics, qos) {
        error!("Error subscribes topics: {:?}", e);
        process::exit(1);
    }
}

fn create_client(broker: &String, client_id: &String) -> mqtt::Client {
    info!("Creating MQTT Client for '{broker}' with ID '{client_id}'");
    // Define the set of options for the create.
    // Use an ID for a persistent session.
    let create_opts = mqtt::CreateOptionsBuilder::new()
        .server_uri(broker)
        .client_id(client_id)
        .finalize();

    // Create a client.
    return mqtt::Client::new(create_opts).unwrap_or_else(|err| {
        error!("Error creating the client: {:?}", err);
        process::exit(1);
    });
}

fn connect_client(
    client: &mqtt::Client,
    username: String,
    password: String,
    lwt: mqtt::Message,
) -> &mqtt::Client {
    // Define Connection Options
    let conn_opts = mqtt::ConnectOptionsBuilder::new()
        .keep_alive_interval(Duration::from_secs(20))
        .clean_session(true)
        .will_message(lwt)
        .user_name(username)
        .password(password)
        .finalize();

    // Connect and wait for it to complete or fail.
    if let Err(e) = client.connect(conn_opts) {
        error!("Unable to connect:\n\t{:?}", e);
        process::exit(1);
    }

    return client;
}

fn process_messages(
    client: &mqtt::Client,
    rx: mqtt::Receiver<Option<mqtt::Message>>,
    topics: &[String],
    qos: &[i32],
) {
    info!("Processing requests...");
    for msg in rx.iter() {
        if let Some(msg) = msg {
            let msg_topic = msg.topic();

            debug!("Received msg on {}", msg_topic);

            if msg_topic.starts_with("packml") {
                if msg_topic.contains("status") {
                    packml::telegrams::handle_status(msg).expect("Error Processing PackML Status");
                } else if msg_topic.contains("event") {
                    packml::telegrams::handle_event(msg).expect("Error Processing PackML Event");
                }
            } else if msg_topic.starts_with("service") {
                if msg_topic.contains("status") {
                    packml::master_service::handle_status(msg)
                        .expect("Error Processing Master Service Status");
                }
            } else {
                println!("Unknown topic... {msg_topic}")
            }
        } else if !client.is_connected() {
            if try_reconnect(&client) {
                println!("Resubscribe topics...");
                subscribe_topics(&client, &topics, &qos);
            } else {
                break;
            }
        }
    }
}

use std::env;

fn get_env(key: &str) -> String {
    let var = env::var(&key);

    match var {
        Ok(var) => var,
        Err(e) => {
            panic!("{} => {}", &key, e)
        }
    }
}

fn init_log() {
    let res = env::var("RUST_LOG");

    if res.is_err() {
        env::set_var("RUST_LOG", "info");
    }

    env_logger::init();
}

fn main() {
    init_log();
    dotenv().ok();

    let broker: String = get_env("MQTT_BROKER");
    let client_id: String = get_env("MQTT_CLIENT");
    let username: String = get_env("MQTT_USERNAME");
    let password: String = get_env("MQTT_PASSWORD");
    let topics: Vec<String> = get_env("MQTT_TOPICS")
        .split(",")
        .map(|el| el.trim().to_string())
        .collect();

    let mut qos: Vec<i32> = Vec::new();

    for _ in &topics {
        qos.append(&mut vec![1])
    }

    info!("Connecting to '{broker}' as '{client_id}' with '{username}:{password}'");

    info!("Subscribing on topics: {:?}", topics);

    let client = create_client(&broker, &client_id);

    // Initialize the consumer before connecting.
    let rx = client.start_consuming();

    // Define last will options.
    let lwt = mqtt::MessageBuilder::new()
        .topic("test")
        .payload("Consumer lost connection")
        .finalize();

    let client = connect_client(&client, username, password, lwt);

    // Subscribe topics.
    subscribe_topics(&client, &topics, &qos);

    process_messages(&client, rx, &topics, &qos);

    // If still connected, then disconnect now.
    if client.is_connected() {
        println!("Disconnecting");
        client.unsubscribe_many(&topics).unwrap();
        client.disconnect(None).unwrap();
    }
    println!("Exiting");
}
