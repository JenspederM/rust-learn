use std::{
    collections::{
        hash_map::Entry::{Occupied, Vacant},
        HashMap,
    },
    sync::mpsc::Receiver,
};

use azure_storage::storage_shared_key_credential::StorageSharedKeyCredential;
use azure_storage_datalake::prelude::*;
use bytes::Bytes;
use chrono::Utc;
use log;
use uuid::Uuid;

/// Definition of what is expected by worker for writing to ADLS.
#[derive(Debug)]
pub struct WriteJob {
    pub path: String,
    pub payload: String,
    pub n_per_file: i32,
}

impl Default for WriteJob {
    fn default() -> Self {
        WriteJob {
            path: "".to_string(),
            payload: "".to_string(),
            n_per_file: 1,
        }
    }
}

pub async fn handle_write_jobs(
    data_lake_client: DataLakeClient,
    receiver: Receiver<WriteJob>,
) -> azure_core::error::Result<()> {
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
                Vacant(e) => {
                    log::debug!("entry {} is vacant!", e.key());
                    e.insert(vec![received.payload]);
                }
                // If the path does exist
                Occupied(mut e) => {
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

    Ok(())
}

pub async fn upload_json_multiline(
    data_lake_client: &DataLakeClient,
    container: String,
    path: String,
    data: Vec<String>,
    ext: String,
) -> azure_core::error::Result<()> {
    log::debug!("Creating file system client for {container}");
    let file_system_client = data_lake_client.clone().into_file_system_client(&container);

    let now = Utc::now();
    let file_name = format!(
        "{ts}-{uid}.{ext}",
        uid = &Uuid::new_v4(),
        ts = &now.timestamp(),
        ext = ext
    );
    let file_path = format!("{}/{}", path, file_name);
    let file_client = file_system_client.get_file_client(&file_path);

    log::debug!("Creating file '{}' with {} lines...", file_path, data.len());
    let create_file_response = file_client.create().into_future().await?;
    log::debug!("Create file response == {:?}\n", create_file_response);

    let content = data.join("\n");
    let mut offset = 0;

    let byte_arr = Bytes::from(content.to_string());
    let file_size = byte_arr.len() as i64;

    log::debug!(
        "appending '{:?}' to file '{}' at offset {}...",
        byte_arr,
        file_path,
        offset
    );

    let append_to_file = file_client.append(offset, byte_arr).into_future().await?;
    log::debug!("append to file response == {:?}\n", append_to_file);

    offset += file_size;

    log::debug!("flushing file '{}'...", file_path);
    let flush_file_response = file_client.flush(offset).close(true).into_future().await?;
    log::debug!("flush file response == {:?}\n", flush_file_response);

    Ok(())
}

#[allow(unused)]
pub async fn upload_data_single(
    data_lake_client: &DataLakeClient,
    container: String,
    path: String,
    data: Vec<String>,
) -> azure_core::error::Result<()> {
    log::debug!("Creating file system client for {container}");
    let file_system_client = data_lake_client.clone().into_file_system_client(&container);

    let file_path = path;
    let file_client = file_system_client.get_file_client(&file_path);

    log::debug!("creating file '{}'...", file_path);
    let create_file_response = file_client.create().into_future().await?;
    log::debug!("create file response == {:?}\n", create_file_response);

    let data = data;
    let mut offset = 0;

    for el in data.iter() {
        let content = el;
        let byte_arr = Bytes::from(content.to_string());
        let file_size = byte_arr.len() as i64;

        log::debug!(
            "Appending '{:?}' to file '{}' at offset {}...",
            byte_arr,
            file_path,
            offset
        );

        let append_to_file = file_client.append(offset, byte_arr).into_future().await?;
        log::debug!("Append to file response == {:?}\n", append_to_file);

        offset += file_size;
    }

    log::debug!("Flushing file '{}'...", file_path);
    let flush_file_response = file_client.flush(offset).close(true).into_future().await?;
    log::debug!("Flush file response == {:?}\n", flush_file_response);

    Ok(())
}

#[allow(unused)]
pub async fn create_data_lake_client() -> azure_core::error::Result<DataLakeClient> {
    let account_name = std::env::var("ADLSGEN2_STORAGE_ACCOUNT")
        .expect("Set env variable ADLSGEN2_STORAGE_ACCOUNT first!");
    let account_key = std::env::var("ADLSGEN2_STORAGE_ACCESS_KEY")
        .expect("Set env variable ADLSGEN2_STORAGE_ACCESS_KEY first!");

    Ok(DataLakeClient::new(
        StorageSharedKeyCredential::new(account_name, account_key),
        None,
    ))
}
