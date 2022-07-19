mod core;
mod packml;

#[macro_use]
extern crate log;

fn main() {
    // Create a new MQTT Client.
    let client = core::MqttClient::new();

    // Start subscribing to topics
    client.subscribe_topics();

    // Start Message Processing
    // NB: This is a blocking action.
    client.process_messages();

    // If still connected, then disconnect now.
    client.disconnect()
}
