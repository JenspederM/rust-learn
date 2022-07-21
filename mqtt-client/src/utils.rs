use dotenv::dotenv;
use log;
use serde_json::Value;
use std::env;

pub fn env_default(key: &str, default: &str) -> String {
    let masked = ["password", "cert"];
    match env::var(key) {
        Ok(s) => {
            if masked.contains(&key.to_lowercase().as_str()) {
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
    dotenv().ok();
    env_default("RUST_LOG", "info");
    env_logger::init();
}

/// Turn a serde_json::Value into a string.
///
/// This also removes "\"" since these are parsed literally by serde_json.
pub fn value_to_string(v: &Value) -> String {
    // Values evaluate "" literally when parsing jons, hence we replace here.
    return v.to_string().trim().replace("\"", "");
}
