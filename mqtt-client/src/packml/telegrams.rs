use chrono::{DateTime, Utc};

use serde::{Deserialize, Serialize};
use serde_json::Result;

use paho_mqtt as mqtt;

//// PackML Status ////

#[derive(Serialize, Deserialize, Debug)]
#[allow(non_snake_case)]
pub struct PackMLStatus {
    State: String,
    TimeStamp: DateTime<Utc>,
    ServiceName: String,
    Host: String,
    PLCVersion: String,
    HMIVersion: String,
    ProtocolVersion: String,
}

/// Process PackML Status Telegrams
///
/// Example message:
///
/// {
///
///     "State": "Online",
///     "TimeStamp": "2022-07-14T21:37:29.4697802+02:00",
///     "ServiceName": "POLNA_NM_A4_GATEWAY_IDx391",
///     "Host": "POLNAVELPCKM001",
///     "PLCVersion": "1.0.0",
///     "HMIVersion": "0.0.0",
///     "ProtocolVersion": "2.0.0"
///
/// }
pub fn handle_status(msg: mqtt::Message) -> Result<PackMLStatus> {
    let obj: PackMLStatus = match serde_json::from_str(&msg.payload_str()) {
        Ok(msg) => msg,
        Err(e) => {
            panic!("{}", e)
        }
    };

    debug!("{:?}", obj);
    Ok(obj)
}

//// PackML Event ////
///
#[derive(Serialize, Deserialize, Debug)]
pub struct ValidationSchema {
    header: String,
    content: String,
}

#[derive(Serialize, Deserialize, Debug)]
#[allow(non_snake_case)]
pub struct PackMLEvent0x03015100 {
    decodeToSQL: String,
    dataContentDecodingSchema: String,
    telegramTypeFriendly: String,
    machineIDx: i32,
    mode: i32,
    validationSchema: String,
    telegramDescription: String,
    telegramType: String,
    unitID: i32,
    state: i32,
    telegramTypeVersion: String,
    friendlyName: String,
    timestamp: DateTime<Utc>,
    dataContent: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum PackMLEvent {
    PackMLEvent0x03015100,
}
/// Process PackML Event Telegrams
///
/// Example message:
///
///{
///
///    "decodeToSQL": "false",
///    "dataContent": {
///      "controlStatus": 3,
///      "setpoint": 4.2949673e9,
///      "unitOfMeasure": 40,
///      "processValueMax": 9.201389,
///      "setpointString": "",
///      "processValueString": "",
///      "processValueSamples": 1,
///      "processValue": 9.201389,
///      "starttimestamp": "2022-05-10T07:07:29.85+02:00",
///      "controlStatus2": 0,
///      "duration": 97,
///      "processID": 63,
///      "loggingType": 5,
///      "unitPrefix": "Bar",
///      "unitDescription": "Pressure - Bar",
///      "processValueMin": 9.201389,
///      "stoptimestamp": "2022-05-10T07:07:29.947+02:00",
///      "settingsVersion": 1
///    },
///    "dataContentDecodingSchema": "",
///    "telegramTypeFriendly": "0x03015100",
///    "machineIDx": 198,
///    "mode": -1,
///    "validationSchema": { "header": "", "content": "" },
///    "telegramDescription": "Process sampling",
///    "telegramType": 50417920,
///    "unitID": 800,
///    "state": -1,
///    "telegramTypeVersion": 2,
///    "friendlyName": "DNKTH TBI-9BE-MC05_IDx198",
///    "timestamp": "2022-05-10T07:08:10.830+02:00"
///
///  }
pub fn handle_event(msg: mqtt::Message) -> Result<PackMLEvent> {
    let obj: PackMLEvent = match serde_json::from_str(&msg.payload_str()) {
        Ok(msg) => msg,
        Err(e) => {
            panic!("{}", e)
        }
    };

    info!("{:?}", obj);
    Ok(obj)
}
