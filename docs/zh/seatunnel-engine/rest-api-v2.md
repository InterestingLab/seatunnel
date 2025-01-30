---
sidebar_position: 12
---

# RESTful API V2

SeaTunnel有一个用于监控的API，可用于查询运行作业的状态和统计信息，以及最近完成的作业。监控API是RESTful风格的，它接受HTTP请求并使用JSON数据格式进行响应。

## 概述

v2版本的api使用jetty支持，与v1版本的接口规范相同 ,可以通过修改`seatunnel.yaml`中的配置项来指定端口和context-path，
同时可以配置 `enable-dynamic-port` 开启动态端口(默认从 `port` 开始累加)，默认为关闭，
如果`enable-dynamic-port`为`true`，我们将使用`port`和`port`+`port-range`范围内未使用的端口，默认范围是100。

```yaml

seatunnel:
  engine:
    http:
      enable-http: true
      port: 8080
      enable-dynamic-port: true
      port-range: 100
```

同时也可以配置context-path,配置如下：

```yaml

seatunnel:
  engine:
    http:
      enable-http: true
      port: 8080
      context-path: /seatunnel
```

## API参考

### 返回Zeta集群的概览

<details>
 <summary><code>GET</code> <code><b>/overview?tag1=value1&tag2=value2</b></code> <code>(Returns an overview over the Zeta engine cluster.)</code></summary>

#### 参数

> |  参数名称  | 是否必传 | 参数类型 |           参数描述           |
> |--------|------|------|--------------------------|
> | tag键值对 | 否    | 字符串  | 一组标签值, 通过该标签值过滤满足条件的节点信息 |

#### 响应

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

**注意:**
- 当你使用`dynamic-slot`时, 返回结果中的`totalSlot`和`unassignedSlot`将始终为0. 设置为固定的slot值后, 将正确返回集群中总共的slot数量以及未分配的slot数量.
- 当添加标签过滤后, `works`, `totalSlot`, `unassignedSlot`将返回满足条件的节点的相关指标. 注意`runningJobs`等job相关指标为集群级别结果, 无法根据标签进行过滤.

</details>

------------------------------------------------------------------------------------------

### 返回所有作业及其当前状态的概览

<details>
 <summary><code>GET</code> <code><b>/running-jobs</b></code> <code>(返回所有作业及其当前状态的概览。)</code></summary>

#### 参数

#### 响应

```json
[
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

### 返回作业的详细信息

<details>
 <summary><code>GET</code> <code><b>/job-info/:jobId</b></code> <code>(返回作业的详细信息。)</code></summary>

#### 参数

> | 参数名称  | 是否必传 | 参数类型 |  参数描述  |
> |-------|------|------|--------|
> | jobId | 是    | long | job id |

#### 响应

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

`jobId`, `jobName`, `jobStatus`, `createTime`, `jobDag`, `metrics` 字段总会返回.
`envOptions`, `pluginJarsUrls`, `isStartWithSavePoint` 字段在Job在RUNNING状态时会返回
`finishedTime`, `errorMsg` 字段在Job结束时会返回，结束状态为不为RUNNING，可能为FINISHED，可能为CANCEL

当我们查询不到这个Job时，返回结果为：

```json
{
  "jobId" : ""
}
```

</details>

------------------------------------------------------------------------------------------

### 返回作业的详细信息

此API已经弃用，请使用/job-info/:jobId替代。

<details>
 <summary><code>GET</code> <code><b>/running-job/:jobId</b></code> <code>(返回作业的详细信息。)</code></summary>

#### 参数

> | 参数名称  | 是否必传 | 参数类型 |  参数描述  |
> |-------|------|------|--------|
> | jobId | 是    | long | job id |

#### 响应

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

`jobId`, `jobName`, `jobStatus`, `createTime`, `jobDag`, `metrics` 字段总会返回.
`envOptions`, `pluginJarsUrls`, `isStartWithSavePoint` 字段在Job在RUNNING状态时会返回
`finishedTime`, `errorMsg` 字段在Job结束时会返回，结束状态为不为RUNNING，可能为FINISHED，可能为CANCEL

当我们查询不到这个Job时，返回结果为：

```json
{
  "jobId" : ""
}
```

</details>

------------------------------------------------------------------------------------------

### 返回所有已完成的作业信息

<details>
 <summary><code>GET</code> <code><b>/finished-jobs/:state</b></code> <code>(返回所有已完成的作业信息。)</code></summary>

#### 参数

> | 参数名称  |   是否必传   |  参数类型  |                               参数描述                               |
> |-------|----------|--------|------------------------------------------------------------------|
> | state | optional | string | finished job status. `FINISHED`,`CANCELED`,`FAILED`,`UNKNOWABLE` |

#### 响应

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

### 返回系统监控信息

<details>
 <summary><code>GET</code> <code><b>/system-monitoring-information</b></code> <code>(返回系统监控信息。)</code></summary>

#### 参数

#### 响应

```json
[
  {
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

### 提交作业

<details>
<summary><code>POST</code> <code><b>/submit-job</b></code> <code>(如果作业提交成功，返回jobId和jobName。)</code></summary>

#### 参数

> |         参数名称         |   是否必传   |  参数类型  | 参数描述                              |
> |----------------------|----------|-----------------------------------|-----------------------------------|
> | jobId                | optional | string | job id                            |
> | jobName              | optional | string | job name                          |
> | isStartWithSavePoint | optional | string | if job is started with save point |
> | format               | optional | string    | 配置风格,支持json和hocon,默认 json         |

#### 请求体

你可以选择用json或者hocon的方式来传递请求体。
Json请求示例：
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

Hocon请求示例：
```hocon
env {
  job.mode = "batch"
}

source {
  FakeSource {
    plugin_output = "fake"
    row.num = 100
    schema = {
      fields {
        name = "string"
        age = "int"
        card = "int"
      }
    }
  }
}

transform {
}

sink {
  Console {
    plugin_input = "fake"
  }
}

```
#### 响应

```json
{
    "jobId": 733584788375666689,
    "jobName": "rest_api_test"
}
```

</details>

------------------------------------------------------------------------------------------
### 提交作业来源上传配置文件

<details>
<summary><code>POST</code> <code><b>/submit-job</b></code> <code>(如果作业提交成功，返回jobId和jobName。)</code></summary>

#### 参数

> |         参数名称         |   是否必传   |  参数类型  | 参数描述                              |
> |----------------------|----------|-----------------------------------|-----------------------------------|
> | jobId                | optional | string | job id                            |
> | jobName              | optional | string | job name                          |
> | isStartWithSavePoint | optional | string | if job is started with save point |

#### 请求体
上传文件key的名称是config_file,文件后缀json的按照json格式来解析,conf或config文件后缀按照hocon格式解析

curl Example

```
curl --location 'http://127.0.0.1:8080/submit-job/upload' --form 'config_file=@"/temp/fake_to_console.conf"'

```
#### 响应

```json
{
    "jobId": 733584788375666689,
    "jobName": "SeaTunnel_Job"
}
```

</details>

------------------------------------------------------------------------------------------

### 批量提交作业

<details>
<summary><code>POST</code> <code><b>/submit-jobs</b></code> <code>(如果作业提交成功，返回jobId和jobName。)</code></summary>

#### 参数(在请求体中params字段中添加)

> |         参数名称         |   是否必传   |  参数类型  |               参数描述                |
> |----------------------|----------|--------|-----------------------------------|
> | jobId                | optional | string | job id                            |
> | jobName              | optional | string | job name                          |
> | isStartWithSavePoint | optional | string | if job is started with save point |



#### 请求体

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

#### 响应

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

### 停止作业

<details>
<summary><code>POST</code> <code><b>/stop-job</b></code> <code>(如果作业成功停止，返回jobId。)</code></summary>

#### 请求体

```json
{
    "jobId": 733584788375666689,
    "isStopWithSavePoint": false # if job is stopped with save point
}
```

#### 响应

```json
{
"jobId": 733584788375666689
}
```

</details>


------------------------------------------------------------------------------------------

### 批量停止作业

<details>
<summary><code>POST</code> <code><b>/stop-jobs</b></code> <code>(如果作业成功停止，返回jobId。)</code></summary>

#### 请求体

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

#### 响应

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

### 加密配置

<details>
<summary><code>POST</code> <code><b>/encrypt-config</b></code> <code>(如果配置加密成功，则返回加密后的配置。)</code></summary>
有关自定义加密的更多信息，请参阅文档[配置-加密-解密](../connector-v2/Config-Encryption-Decryption.md).

#### 请求体

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

#### 响应

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

### 更新运行节点的tags

<details>
<summary><code>POST</code><code><b>/update-tags</b></code><code>因为更新只能针对于某个节点，因此需要用当前节点ip:port用于更新</code><code>(如果更新成功，则返回"success"信息)</code></summary>


#### 更新节点tags
##### 请求体
如果请求参数是`Map`对象，表示要更新当前节点的tags
```json
{
  "tag1": "dev_1",
  "tag2": "dev_2"
}
```
##### 响应

```json
{
  "status": "success",
  "message": "update node tags done."
}
```
#### 移除节点tags
##### 请求体
如果参数为空`Map`对象，表示要清除当前节点的tags
```json
{}
```
##### 响应
响应体将为：
```json
{
  "status": "success",
  "message": "update node tags done."
}
```

#### 请求参数异常
- 如果请求参数为空

##### 响应

```json
{
    "status": "fail",
    "message": "Request body is empty."
}
```
- 如果参数不是`Map`对象
##### 响应

```json
{
  "status": "fail",
  "message": "Invalid JSON format in request body."
}
```
</details>


------------------------------------------------------------------------------------------

### 获取所有节点日志内容

<details>
 <summary><code>GET</code> <code><b>/logs/:jobId</b></code> <code>(返回日志列表。)</code></summary>

#### 请求参数

#### 参数(在请求体中params字段中添加)

> |         参数名称         |   是否必传   |  参数类型  |               参数描述                |
> |----------------------|----------|--------|-----------------------------------|
> | jobId                | optional | string | job id                            |

当`jobId`为空时，返回所有节点的日志信息，否则返回指定`jobId`在所有节点的的日志列表。

#### 响应

返回请求节点的日志列表、内容

#### 返回所有日志文件列表

如果你想先查看日志列表，可以通过`GET`请求获取日志列表，`http://localhost:8080/logs?format=json`

```json
[
  {
    "node": "localhost:8080",
    "logLink": "http://localhost:8080/logs/job-899485770241277953.log",
    "logName": "job-899485770241277953.log"
  },
  {
    "node": "localhost:8080",
    "logLink": "http://localhost:8080/logs/job-899470314109468673.log",
    "logName": "job-899470314109468673.log"
  }
]
```

当前支持的格式有`json`和`html`，默认为`html`。


#### 例子

获取所有节点jobId为`733584788375666689`的日志信息：`http://localhost:8080/logs/733584788375666689`
获取所有节点日志列表：`http://localhost:8080/logs`
获取所有节点日志列表以JSON格式返回：`http://localhost:8080/logs?format=json`
获取日志文件内容：`http://localhost:8080/logs/job-898380162133917698.log`


</details>


### 获取单节点日志内容

<details>
 <summary><code>GET</code> <code><b>/log</b></code> <code>(返回日志列表。)</code></summary>

#### 响应

返回请求节点的日志列表

#### 例子

获取当前节点的日志列表：`http://localhost:5801/log`
获取日志文件内容：`http://localhost:5801/log/job-898380162133917698.log``

</details>

### 获取节点指标信息

<details>
 <summary>
    <code>GET</code> <code><b>/metrics</b></code>  
    <code>GET</code> <code><b>/openmetrics</b></code>
</summary>
你需要先打开`Telemetry`才能获取集群指标信息。否则将返回空信息。

更多关于`Telemetry`的信息可以在[Telemetry](telemetry.md)文档中找到。

</details>