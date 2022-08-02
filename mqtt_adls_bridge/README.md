# MQTT to Azure Datalake Gen2 Bridge

This is a simple bridge that will take messages from an MQTT broker and write them to Azure Datalake Gen2.

## Configuration

The bridge is configured using environment variables. The following variables are required:

| Environment Variable          | Description                                                       | Default                     |
| ----------------------------- | ----------------------------------------------------------------- | --------------------------- |
| MQTT_BROKER                   | The MQTT broker to connect to                                     | tcp://localhost:1883        |
| MQTT_CLIENT_ID                | The MQTT client ID to use                                         | rust_client                 |
| MQTT_TOPIC                    | The topic to subscribe to                                         | #                           |
| MQTT_LWT_TOPIC                | The topic to publish the last will and testament to               | lwt                         |
| MQTT_LWT_MESSAGE              | The message to publish as the last will and testament             | Last will for 'rust_client' |
| MQTT_USERNAME                 | The username to use when connecting to the broker                 |                             |
| MQTT_PASSWORD                 | The password to use when connecting to the broker                 |                             |
| ADLSGEN2_STORAGE_ACCOUNT_NAME | The name of the Azure Datalake Gen2 account                       |                             |
| ADLSGEN2_STORAGE_ACCOUNT_KEY  | The key to use when connecting to the Azure Datalake Gen2 account |                             |
| RUST_LOG                      | The log level to use                                              | info                        |

## Build Image

To build the image, run the following command:

```bash
$ bash ./scripts/build.sh
```

## Run Image

To run the image, run the following command:

```bash
$ bash ./scripts/run.sh
```
