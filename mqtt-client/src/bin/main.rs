extern crate env_logger;
extern crate log;

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::mpsc::{self, Receiver, Sender};
use std::thread::JoinHandle;

use mqtt_adls_bridge::{
    adls::{create_data_lake_client, upload_json_multiline, WriteJob},
    mqtt::start_mqtt_thread,
    utils::init_log,
    ThreadPool,
};

/////////////////////////////////////////////////////////////////////////////

#[tokio::main]
async fn main() -> azure_core::error::Result<()> {
    // Initialize Logging
    init_log();

    // Create Sender and Receiver to pass messages between two threads.
    // One thread will run the MQTT client, and the other will send messages to ADLS.
    let (transmitter, receiver): (Sender<WriteJob>, Receiver<WriteJob>) = mpsc::channel();

    // Initiate MQTT client on it's own thread and send messages through a channel.
    let handle: JoinHandle<()> = start_mqtt_thread(transmitter);

    // Create client to interact with the datalake.
    let data_lake_client = create_data_lake_client().await?;

    // Initialize HashMap (dictionary) to hold <path, Vec<payload_str>>
    let mut map: HashMap<String, Vec<String>> = HashMap::new();

    // For every message received by receiver
    for received in receiver {
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
                        log::info!(
                            "Flushing {} lines to {}",
                            &received.n_per_file,
                            &received.path.to_string()
                        );
                        // Upload multiline json to datalake
                        upload_json_multiline(
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

    // Wait for the MQTT client to finish.
    handle.join().unwrap();
    Ok(())
}
