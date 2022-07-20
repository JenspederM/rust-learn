use azure_storage::storage_shared_key_credential::StorageSharedKeyCredential;
use azure_storage_datalake::prelude::*;
use bytes::Bytes;
use log;

pub async fn upload_data_multiple(
    data_lake_client: &DataLakeClient,
    container: String,
    path: String,
    data: Vec<String>,
) -> azure_core::error::Result<()> {
    log::info!("Creating file system client for {container}");
    let file_system_client = data_lake_client.clone().into_file_system_client(&container);

    let file_path = path;
    let file_client = file_system_client.get_file_client(&file_path);

    log::info!("creating file '{}'...", file_path);
    let create_file_response = file_client.create().into_future().await?;
    log::info!("create file response == {:?}\n", create_file_response);

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

    log::info!("flushing file '{}'...", file_path);
    let flush_file_response = file_client.flush(offset).close(true).into_future().await?;
    log::info!("flush file response == {:?}\n", flush_file_response);

    Ok(())
}

#[allow(unused)]
pub async fn upload_data_single(
    data_lake_client: &DataLakeClient,
    container: String,
    path: String,
    data: Vec<String>,
) -> azure_core::error::Result<()> {
    log::info!("Creating file system client for {container}");
    let file_system_client = data_lake_client.clone().into_file_system_client(&container);

    let file_path = path;
    let file_client = file_system_client.get_file_client(&file_path);

    log::info!("creating file '{}'...", file_path);
    let create_file_response = file_client.create().into_future().await?;
    log::info!("create file response == {:?}\n", create_file_response);

    let data = data;
    let mut offset = 0;

    for el in data.iter() {
        let content = el;
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
    }

    log::info!("flushing file '{}'...", file_path);
    let flush_file_response = file_client.flush(offset).close(true).into_future().await?;
    log::info!("flush file response == {:?}\n", flush_file_response);

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
