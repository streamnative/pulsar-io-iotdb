---
id: io-iotdb-sink
title: IoTDB sink connector
sidebar_label: IoTDB sink connector
---

The IoTDB sink connector pulls messages from Pulsar topics
and persists the messages to IoTDB.


## Configuration

The configuration of the IoTDB sink connector has the following properties.

### Property

#### IoTDBConfig
| Name | Type|Required | Default | Description
|------|----------|----------|---------|-------------|
| `host` |String| true|" " (empty string) | The user of the IoTDB instance. |
| `port` |int| true|0| The port of the IoTDB instance. |
| `username` | String|true| " " (empty string) |The username used to authenticate to IoTDB. |
| `password` | String| true|" " (empty string)  | The password used to authenticate to IoTDB. |
| `storageGroup` |String| true | " " (empty string)| The IoTDB storage group to which write messages. |
| `flushIntervalMs` |int|false| 3000 |   The IoTDB operation time in milliseconds. |
| `batchSize` | int|false|100| The batch size of writing to IoTDB. |

### Example
Before using the IoTDB sink connector, you need to create a configuration file through one of the following methods.

* JSON
    ```json
    {
        "host": "127.0.0.1",
        "port": 6667,
        "user": "root",
        "password": "root",
        "batchSize": 2,
        "storageGroup": "root.testPulsar"
    }
    ```

* YAML
    ```yaml
    {
        "host": "127.0.0.1",
        "port": 6667,
        "user": "root",
        "password": "root",
        "batchSize": 2,
        "storageGroup": "root.testPulsar"
    }
    ```