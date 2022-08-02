use dotenv::dotenv;
use log;
use serde_json::Value;
use std::env;

pub fn env_default(key: &str, default: &str) -> String {
    // Set partial keys where values should be masked in logs.
    let mask_array = ["password".to_string(), "cert".to_string()];

    // Mask any values where keys partially match those in `mask_array`.
    let mask = mask_array.iter().any(|e| key.to_lowercase().contains(e));

    // Get the value from the environment.
    match env::var(key) {
        Ok(s) => {
            if mask {
                log::info!("'{key}' set to '********'")
            } else {
                log::info!("'{key}' set to '{s}'")
            }
        }
        Err(_) => {
            log::info!("'{key}' not set. Defaults to '{default}'");
            env::set_var(key, default);
        }
    }

    return env::var(key).expect("Something went wrong while setting env vars...");
}

/// Initialize logging
///
/// Here we pull also initialize defaults
pub fn init_log() {
    // Possibly load .env file
    dotenv().ok();
    // Get the log level from the environment. Default to INFO.
    env_default("RUST_LOG", "info");
    // Initiate logger.
    env_logger::init();
}

/// Turn a serde_json::Value into a string.
///
/// Also removes "\"" since these are parsed literally by serde_json.
pub fn value_to_string(v: &Value) -> String {
    // Values evaluate "" literally when parsing jons, hence we replace here.
    return v.to_string().trim().replace("\"", "");
}
