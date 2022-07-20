use dotenv::dotenv;
use log;
use std::env;

pub fn get_env(key: &str) -> String {
    let var = env::var(&key);

    match var {
        Ok(var) => var,
        Err(e) => {
            panic!("{} => {}", &key, e)
        }
    }
}

pub fn env_default(key: &str, default: &str) -> String {
    let masked = ["password", "cert"];
    match env::var(key) {
        Ok(s) => {
            if masked.contains(&key) {
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
