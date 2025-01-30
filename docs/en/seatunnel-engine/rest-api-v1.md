---
sidebar_position: 11
---

# RESTful API V1

:::caution warn

It is recommended to use the v2 version of the Rest API. The v1 version is deprecated and will be removed in the future.

:::

SeaTunnel has a monitoring API that can be used to query status and statistics of running jobs, as well as recent
completed jobs. The monitoring API is a RESTful API that accepts HTTP requests and responds with JSON data.

## Overview

The monitoring API is backed by a web server that runs as part of the node, each node member can provide RESTful api capability.
By default, this server listens at port 5801, which can be configured in hazelcast.yaml like :

```yaml
network:
    rest-api:
      enabled: true
      endpoint-groups:
        CLUSTER_WRITE:
          enabled: true
        DATA:
          enabled: true
    join:
      tcp-ip:
        enabled: true
        member-list:
          - localhost
    port:
      auto-increment: true
      port-count: 100
      port: 5801
```

## API reference

### Returns an overview over the Zeta engine cluster.

<details>
 <summary><code>GET</code> <code><b>/hazelcast/rest/maps/overview?tag1=value1&tag2=value2</b></code> <code>(Returns an overview over the Zeta engine cluster.)</code></summary>

#### Parameters

> |   name   |   type   | data type |                                             description                                              |
> |----------|----------|-----------|------------------------------------------------------------------------------------------------------|
> | tag_name | optional | string    | the tags filter, you can add tag filter to get those matched worker count, and slot on those workers |

#### Responses

```json
{
    "projectVersion":"2.3.10-SNAPSHOT",
    "gitCommitAbbrev":"DeadD0d0",
    "totalSlot":"0",
    "unassignedSlot":"0",
    "works":"1",
    "runningJobs":"0",
    "finishedJobs":"0",
    "failedJobs":"0",
    "cancelledJobs":"0"
}
```

**Notes:**
- If you use `dynamic-slot`, the `totalSlot` and `unassignedSlot` always be `0`. when you set it to fix slot number, it will return the correct total and unassigned slot number
- If the url has tag filter, the `works`, `totalSlot` and `unassignedSlot` will return the result on the matched worker. but the job related metric will always return the cluster level information.

</details>

------------------------------------------------------------------------------------------

###  Returns thread dump information for the current node.

<details>
 <summary><code>GET</code> <code><b>/hazelcast/rest/maps/thread-dump</b></code> <code>(Returns thread dump information for the current node.)</code></summary>

#### Parameters


#### Responses

```json
[
  {
    "threadName": "",
    "threadId": 0,
    "threadState": "",
    "stackTrace": ""
  }
]
```

</details>

------------------------------------------------------------------------------------------



### Returns An Overview And State Of All Jobs

<details>
 <summary><code>GET</code> <code><b>/hazelcast/rest/maps/running-jobs</b></code> <code>(Returns an overview over all jobs and their current state.)</code></summary>

#### Parameters

#### Responses

```json
[
  {
    "jobId": "",
    "jobName": "",
    "jobStatus": "",
    "envOptions": {
    },
    "createTime": "",
    "jobDag": {
      "jobId": "",
      "envOptions": [],
      "vertexInfoMap": [
        {
          "vertexId": 1,
          "type": "",
          "vertexName": "",
          "tablePaths": [
            ""
          ]
        }
      ],
      "pipelineEdges": {}
    },
    "pluginJarsUrls": [
    ],
    "isStartWithSavePoint": false,
    "metrics": {
      "sourceReceivedCount": "",
      "sinkWriteCount": ""
    }
  }
]
```

</details>

------------------------------------------------------------------------------------------

### Return Details Of A Job

<details>
 <summary><code>GET</code> <code><b>/hazelcast/rest/maps/job-info/:jobId</b></code> <code>(Return details of a job. )</code></summary>

#### Parameters

> | name  |   type   | data type | description |
> |-------|----------|-----------|-------------|
> | jobId | required | long      | job id      |

#### Responses

```json
{
  "jobId": "",
  "jobName": "",
  "jobStatus": "",
  "createTime": "",
  "jobDag": {
    "jobId": "",
    "envOptions": [],
    "vertexInfoMap": [
      {
        "vertexId": 1,
        "type": "",
        "vertexName": "",
        "tablePaths": [
          ""
        ]
      }
    ],
    "pipelineEdges": {}
  },
  "metrics": {
    "sourceReceivedCount": "",
    "sinkWriteCount": ""
  },
  "finishedTime": "",
  "errorMsg": null,
  "envOptions": {
  },
  "pluginJarsUrls": [
  ],
  "isStartWithSavePoint": false
}
```

`jobId`, `jobName`, `jobStatus`, `createTime`, `jobDag`, `metrics` always be returned.
`envOptions`, `pluginJarsUrls`, `isStartWithSavePoint` will return when job is running.
`finishedTime`, `errorMsg` will return when job is finished.

When we can't get the job info, the response will be:

```json
{
  "jobId" : ""
}
```

</details>

------------------------------------------------------------------------------------------

### Return Details Of A Job

This API has been deprecated, please use /hazelcast/rest/maps/job-info/:jobId instead

<details>
 <summary><code>GET</code> <code><b>/hazelcast/rest/maps/running-job/:jobId</b></code> <code>(Return details of a job. )</code></summary>

#### Parameters

> | name  |   type   | data type | description |
> |-------|----------|-----------|-------------|
> | jobId | required | long      | job id      |

#### Responses

```json
{
  "jobId": "",
  "jobName": "",
  "jobStatus": "",
  "createTime": "",
  "jobDag": {
    "jobId": "",
    "envOptions": [],
    "vertexInfoMap": [
      {
        "vertexId": 1,
        "type": "",
        "vertexName": "",
        "tablePaths": [
          ""
        ]
      }
    ],
    "pipelineEdges": {}
  },
  "metrics": {
    "SourceReceivedCount": "",
    "SourceReceivedQPS": "",
    "SourceReceivedBytes": "",
    "SourceReceivedBytesPerSeconds": "",
    "SinkWriteCount": "",
    "SinkWriteQPS": "",
    "SinkWriteBytes": "",
    "SinkWriteBytesPerSeconds": "",
    "TransformOutputCount": {},
    "TableSourceReceivedCount": {},
    "TableSourceReceivedBytes": {},
    "TableSourceReceivedBytesPerSeconds": {},
    "TableSourceReceivedQPS": {},
    "TableSinkWriteCount": {},
    "TableSinkWriteQPS": {},
    "TableSinkWriteBytes": {},
    "TableSinkWriteBytesPerSeconds": {}
  },
  "finishedTime": "",
  "errorMsg": null,
  "envOptions": {
  },
  "pluginJarsUrls": [
  ],
  "isStartWithSavePoint": false
}
```

`jobId`, `jobName`, `jobStatus`, `createTime`, `jobDag`, `metrics` always be returned.
`envOptions`, `pluginJarsUrls`, `isStartWithSavePoint` will return when job is running.
`finishedTime`, `errorMsg` will return when job is finished.

When we can't get the job info, the response will be:

```json
{
  "jobId" : ""
}
```

</details>

------------------------------------------------------------------------------------------

### Return All Finished Jobs Info

<details>
 <summary><code>GET</code> <code><b>/hazelcast/rest/maps/finished-jobs/:state</b></code> <code>(Return all finished Jobs Info.)</code></summary>

#### Parameters

> | name  |   type   | data type |                           description                            |
> |-------|----------|-----------|------------------------------------------------------------------|
> | state | optional | string    | finished job status. `FINISHED`,`CANCELED`,`FAILED`,`UNKNOWABLE` |

#### Responses

```json
[
  {
    "jobId": "",
    "jobName": "",
    "jobStatus": "",
    "errorMsg": null,
    "createTime": "",
    "finishTime": "",
    "jobDag": {
      "jobId": "",
      "envOptions": [],
      "vertexInfoMap": [
        {
          "vertexId": 1,
          "type": "",
          "vertexName": "",
          "tablePaths": [
            ""
          ]
        }
      ],
      "pipelineEdges": {}
    },
    "metrics": ""
  }
]
```

</details>

------------------------------------------------------------------------------------------

### Returns System Monitoring Information

<details>
 <summary><code>GET</code> <code><b>/hazelcast/rest/maps/system-monitoring-information</b></code> <code>(Returns system monitoring information.)</code></summary>

#### Parameters

#### Responses

```json
[
  {
    "isMaster": "true",
    "host": "localhost",
    "port": "5801",
    "processors":"8",
    "physical.memory.total":"16.0G",
    "physical.memory.free":"16.3M",
    "swap.space.total":"0",
    "swap.space.free":"0",
    "heap.memory.used":"135.7M",
    "heap.memory.free":"440.8M",
    "heap.memory.total":"576.5M",
    "heap.memory.max":"3.6G",
    "heap.memory.used/total":"23.54%",
    "heap.memory.used/max":"3.73%",
    "minor.gc.count":"6",
    "minor.gc.time":"110ms",
    "major.gc.count":"2",
    "major.gc.time":"73ms",
    "load.process":"24.78%",
    "load.system":"60.00%",
    "load.systemAverage":"2.07",
    "thread.count":"117",
    "thread.peakCount":"118",
    "cluster.timeDiff":"0",
    "event.q.size":"0",
    "executor.q.async.size":"0",
    "executor.q.client.size":"0",
    "executor.q.client.query.size":"0",
    "executor.q.client.blocking.size":"0",
    "executor.q.query.size":"0",
    "executor.q.scheduled.size":"0",
    "executor.q.io.size":"0",
    "executor.q.system.size":"0",
    "executor.q.operations.size":"0",
    "executor.q.priorityOperation.size":"0",
    "operations.completed.count":"10",
    "executor.q.mapLoad.size":"0",
    "executor.q.mapLoadAllKeys.size":"0",
    "executor.q.cluster.size":"0",
    "executor.q.response.size":"0",
    "operations.running.count":"0",
    "operations.pending.invocations.percentage":"0.00%",
    "operations.pending.invocations.count":"0",
    "proxy.count":"8",
    "clientEndpoint.count":"0",
    "connection.active.count":"2",
    "client.connection.count":"0",
    "connection.count":"0"
  }
]
```

</details>

------------------------------------------------------------------------------------------

### Submit A Job

<details>
<summary><code>POST</code> <code><b>/hazelcast/rest/maps/submit-job</b></code> <code>(Returns jobId and jobName if job submitted successfully.)</code></summary>

#### Parameters

> |         name         |   type   | data type |            description            |
> |----------------------|----------|-----------|-----------------------------------|
> | jobId                | optional | string    | job id                            |
> | jobName              | optional | string    | job name                          |
> | isStartWithSavePoint | optional | string    | if job is started with save point |

#### Body

```json
{
    "env": {
        "job.mode": "batch"
    },
    "source": [
        {
            "plugin_name": "FakeSource",
            "plugin_output": "fake",
            "row.num": 100,
            "schema": {
                "fields": {
                    "name": "string",
                    "age": "int",
                    "card": "int"
                }
            }
        }
    ],
    "transform": [
    ],
    "sink": [
        {
            "plugin_name": "Console",
            "plugin_input": ["fake"]
        }
    ]
}
```

#### Responses

```json
{
    "jobId": 733584788375666689,
    "jobName": "rest_api_test"
}
```

</details>

------------------------------------------------------------------------------------------

### Batch Submit Jobs

<details>
<summary><code>POST</code> <code><b>/hazelcast/rest/maps/submit-jobs</b></code> <code>(Returns jobId and jobName if the job is successfully submitted.)</code></summary>

#### Parameters (add in the `params` field in the request body)

> |    Parameter Name     |   Required   |  Type   |              Description              |
> |----------------------|--------------|---------|---------------------------------------|
> | jobId                | optional     | string  | job id                                |
> | jobName              | optional     | string  | job name                              |
> | isStartWithSavePoint | optional     | string  | if the job is started with save point |

#### Request Body

```json
[
  {
    "params":{
      "jobId":"123456",
      "jobName":"SeaTunnel-01"
    },
    "env": {
      "job.mode": "batch"
    },
    "source": [
      {
        "plugin_name": "FakeSource",
        "plugin_output": "fake",
        "row.num": 1000,
        "schema": {
          "fields": {
            "name": "string",
            "age": "int",
            "card": "int"
          }
        }
      }
    ],
    "transform": [
    ],
    "sink": [
      {
        "plugin_name": "Console",
        "plugin_input": ["fake"]
      }
    ]
  },
  {
    "params":{
      "jobId":"1234567",
      "jobName":"SeaTunnel-02"
    },
    "env": {
      "job.mode": "batch"
    },
    "source": [
      {
        "plugin_name": "FakeSource",
        "plugin_output": "fake",
        "row.num": 1000,
        "schema": {
          "fields": {
            "name": "string",
            "age": "int",
            "card": "int"
          }
        }
      }
    ],
    "transform": [
    ],
    "sink": [
      {
        "plugin_name": "Console",
        "plugin_input": ["fake"]
      }
    ]
  }
]
```

#### Response

```json
[
  {
    "jobId": "123456",
    "jobName": "SeaTunnel-01"
  },{
    "jobId": "1234567",
    "jobName": "SeaTunnel-02"
  }
]
```

</details>

------------------------------------------------------------------------------------------

### Stop A Job

<details>
<summary><code>POST</code> <code><b>/hazelcast/rest/maps/stop-job</b></code> <code>(Returns jobId if job stoped successfully.)</code></summary>

#### Body

```json
{
    "jobId": 733584788375666689,
    "isStopWithSavePoint": false # if job is stopped with save point
}
```

#### Responses

```json
{
"jobId": 733584788375666689
}
```

</details>

------------------------------------------------------------------------------------------
### Batch Stop Jobs

<details>
<summary><code>POST</code> <code><b>/hazelcast/rest/maps/stop-jobs</b></code> <code>(Returns jobId if the job is successfully stopped.)</code></summary>

#### Request Body

```json
[
  {
    "jobId": 881432421482889220,
    "isStopWithSavePoint": false
  },
  {
    "jobId": 881432456517910529,
    "isStopWithSavePoint": false
  }
]
```

#### Response

```json
[
  {
    "jobId": 881432421482889220
  },
  {
    "jobId": 881432456517910529
  }
]
```

</details>

------------------------------------------------------------------------------------------
### Encrypt Config

<details>
<summary><code>POST</code> <code><b>/hazelcast/rest/maps/encrypt-config</b></code> <code>(Returns the encrypted config if config is encrypted successfully.)</code></summary>
For more information about customize encryption, please refer to the documentation [config-encryption-decryption](../connector-v2/Config-Encryption-Decryption.md).

#### Body

```json
{
    "env": {
        "parallelism": 1,
        "shade.identifier":"base64"
    },
    "source": [
        {
            "plugin_name": "MySQL-CDC",
            "schema" : {
                "fields": {
                    "name": "string",
                    "age": "int"
                }
            },
            "plugin_output": "fake",
            "parallelism": 1,
            "hostname": "127.0.0.1",
            "username": "seatunnel",
            "password": "seatunnel_password",
            "table-name": "inventory_vwyw0n"
        }
    ],
    "transform": [
    ],
    "sink": [
        {
            "plugin_name": "Clickhouse",
            "host": "localhost:8123",
            "database": "default",
            "table": "fake_all",
            "username": "seatunnel",
            "password": "seatunnel_password"
        }
    ]
}
```

#### Responses

```json
{
    "env": {
        "parallelism": 1,
        "shade.identifier": "base64"
    },
    "source": [
        {
            "plugin_name": "MySQL-CDC",
            "schema": {
                "fields": {
                    "name": "string",
                    "age": "int"
                }
            },
            "plugin_output": "fake",
            "parallelism": 1,
            "hostname": "127.0.0.1",
            "username": "c2VhdHVubmVs",
            "password": "c2VhdHVubmVsX3Bhc3N3b3Jk",
            "table-name": "inventory_vwyw0n"
        }
    ],
    "transform": [],
    "sink": [
        {
            "plugin_name": "Clickhouse",
            "host": "localhost:8123",
            "database": "default",
            "table": "fake_all",
            "username": "c2VhdHVubmVs",
            "password": "c2VhdHVubmVsX3Bhc3N3b3Jk"
        }
    ]
}
```

</details>


------------------------------------------------------------------------------------------

### Update the tags of running node

<details><summary><code>POST</code><code><b>/hazelcast/rest/maps/update-tags</b></code><code>Because the update can only target a specific node, the current node's `ip:port` needs to be used for the update</code><code>(If the update is successful, return a success message)</code></summary>


#### update node tags
##### Body
If the request parameter is a `Map` object, it indicates that the tags of the current node need to be updated
```json
{
  "tag1": "dev_1",
  "tag2": "dev_2"
}
```
##### Responses

```json
{
  "status": "success",
  "message": "update node tags done."
}
```
#### remove node tags
##### Body
If the parameter is an empty `Map` object, it means that the tags of the current node need to be cleared
```json
{}
```
##### Responses

```json
{
  "status": "success",
  "message": "update node tags done."
}
```

#### Request parameter exception
- If the parameter body is empty

##### Responses

```json
{
    "status": "fail",
    "message": "Request body is empty."
}
```
- If the parameter is not a `Map` object
##### Responses

```json
{
  "status": "fail",
  "message": "Invalid JSON format in request body."
}
```
</details>

------------------------------------------------------------------------------------------

### Get All Node Log Content

<details>
 <summary><code>GET</code> <code><b>/hazelcast/rest/maps/logs/:jobId</b></code> <code>(Returns a list of logs.)</code></summary>

#### Request Parameters

#### Parameters (Add in the `params` field of the request body)

> |     Parameter Name    |  Required  |  Type   |           Description           |
> |----------------------|------------|---------|---------------------------------|
> | jobId                |  optional  | string  | job id                          |

When `jobId` is empty, it returns log information for all nodes; otherwise, it returns the log list of the specified `jobId` across all nodes.

#### Response

Returns a list of logs and content from the requested nodes.

#### Get All Log Files List

If you'd like to view the log list first, you can use a `GET` request to retrieve the log list:
`http://localhost:5801/hazelcast/rest/maps/logs?format=json`

```json
[
  {
    "node": "localhost:5801",
    "logLink": "http://localhost:5801/hazelcast/rest/maps/logs/job-899485770241277953.log",
    "logName": "job-899485770241277953.log"
  },
  {
    "node": "localhost:5801",
    "logLink": "http://localhost:5801/hazelcast/rest/maps/logs/job-899470314109468673.log",
    "logName": "job-899470314109468673.log"
  }
]
```

The supported formats are `json` and `html`, with `html` as the default.

#### Examples

Retrieve logs for all nodes with the `jobId` of `733584788375666689`: `http://localhost:5801/hazelcast/rest/maps/logs/733584788375666689`
Retrieve the log list for all nodes: `http://localhost:5801/hazelcast/rest/maps/logs`
Retrieve the log list for all nodes in JSON format: `http://localhost:5801/hazelcast/rest/maps/logs?format=json`
Retrieve log file content: `http://localhost:5801/hazelcast/rest/maps/logs/job-898380162133917698.log`

</details>

### Get Log Content from a Single Node

<details>
 <summary><code>GET</code> <code><b>/hazelcast/rest/maps/log</b></code> <code>(Returns a list of logs.)</code></summary>

#### Response

Returns a list of logs from the requested node.

#### Examples

To get a list of logs from the current node: `http://localhost:5801/hazelcast/rest/maps/log`
To get the content of a log file: `http://localhost:5801/hazelcast/rest/maps/log/job-898380162133917698.log`

</details>