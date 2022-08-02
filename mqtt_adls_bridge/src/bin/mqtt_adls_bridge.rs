use mqtt_adls_bridge::{
    adls::{create_data_lake_client, handle_write_jobs, WriteJob},
    mqtt::start_mqtt_thread,
    utils::init_log,
};

use std::{
    sync::mpsc::{self, Receiver, Sender},
    thread::JoinHandle,
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
    let mqtt_thread: JoinHandle<()> = start_mqtt_thread(transmitter);

    // Create client to interact with the datalake.
    let data_lake_client = create_data_lake_client().await?;

    // Handle messages received from the MQTT client.
    handle_write_jobs(data_lake_client, receiver).await?;

    // Wait for the MQTT client to finish.
    mqtt_thread.join().unwrap();
    Ok(())
}
