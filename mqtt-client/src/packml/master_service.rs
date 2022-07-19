use chrono::{DateTime, Utc};

use serde::{Deserialize, Serialize};
use serde_json::Result;

use paho_mqtt as mqtt;

#[derive(Serialize, Deserialize, Debug)]
#[allow(non_snake_case)]
pub struct MasterServiceStatus {
    State: String,
    Host: String,
    TimeStamp: DateTime<Utc>,
}
/// Process Master Service Status
///
/// Example message:
///
/// {
///
///   "State": "Offline",
///   "Host": "POLNAVELPCKM001",
///   "TimeStamp": "2022-07-03T14:09:12.5506322+02:00"
///
/// }
pub fn handle_status(msg: mqtt::Message) -> Result<MasterServiceStatus> {
    let obj: MasterServiceStatus = match serde_json::from_str(&msg.payload_str()) {
        Ok(msg) => msg,
        Err(e) => {
            panic!("{}", e)
        }
    };

    info!("{:?}", obj);
    Ok(obj)
}
