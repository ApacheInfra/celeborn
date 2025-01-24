---
hide:
  - navigation

license: |
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

      https://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
---

# Migration Guide

# Upgrading from 0.5 to 0.6

- Since 0.6.0, Celeborn modified `celeborn.quota.tenant.diskBytesWritten` to `celeborn.quota.user.diskBytesWritten`. Please use `celeborn.quota.user.diskBytesWritten` if you want to set user level quota.

- Since 0.6.0, Celeborn modified `celeborn.quota.tenant.diskFileCount` to `celeborn.quota.user.diskFileCount`. Please use `celeborn.quota.user.diskFileCount` if you want to set user level quota.

- Since 0.6.0, Celeborn modified `celeborn.quota.tenant.hdfsBytesWritten` to `celeborn.quota.user.hdfsBytesWritten`. Please use `celeborn.quota.user.hdfsBytesWritten` if you want to set user level quota.

- Since 0.6.0, Celeborn modified `celeborn.quota.tenant.hdfsFileCount` to `celeborn.quota.user.hdfsFileCount`. Please use `celeborn.quota.user.hdfsFileCount` if you want to set user level quota.

- Since 0.6.0, Celeborn changed the default value of `celeborn.master.slot.assign.extraSlots` from `2` to `100`, which means Celeborn will involve more workers in offering slots.

- Since 0.6.0, Celeborn deprecate `celeborn.worker.congestionControl.low.watermark`. Please use `celeborn.worker.congestionControl.diskBuffer.low.watermark` instead.

- Since 0.6.0, Celeborn deprecate `celeborn.worker.congestionControl.high.watermark`. Please use `celeborn.worker.congestionControl.diskBuffer.high.watermark` instead.

- Since 0.6.0, Celeborn changed the default value of `celeborn.client.spark.fetch.throwsFetchFailure` from `false` to `true`, which means Celeborn will enable spark stage rerun at default.

- Since 0.6.0, Celeborn changed `celeborn.<module>.io.mode` optional, of which the default value changed from `NIO` to `EPOLL` if epoll mode is available, falling back to `NIO` otherwise.

- Since 0.6.0, Celeborn has introduced a new RESTful API namespace: /api/v1, which uses the application/json media type for requests and responses.
   The `celeborn-openapi-client` SDK is also available to help users interact with the new RESTful APIs.
   The legacy RESTful APIs have been deprecated and will be removed in future releases.
   Access the full [RESTful API documentation](./restapi.md) for detailed information.
  
  - The mappings of the old RESTful APIs to the new RESTful APIs for Master.

    | Old RESTful API          | New RESTful API                          | Note                                             |
    |--------------------------|------------------------------------------|--------------------------------------------------|
    | GET /conf                | GET /api/v1/conf                         |                                                  |
    | GET /listDynamicConfigs  | GET /api/v1/conf/dynamic                 |                                                  |
    | GET /threadDump          | GET /api/v1/thread_dump                  |                                                  |
    | GET /applications        | GET /api/v1/applications                 |                                                  |
    | GET /hostnames           | GET /api/v1/applications/hostnames       |                                                  |
    | GET /shuffle             | GET /api/v1/shuffles                     |                                                  |
    | GET /masterGroupInfo     | GET /api/v1/masters                      |                                                  |
    | GET /workerInfo          | GET /api/v1/workers                      |                                                  |
    | GET /lostWorkers         | GET /api/v1/workers                      | get the lostWorkers field in response            |
    | GET /excludedWorkers     | GET /api/v1/workers                      | get the excludedWorkers field in response        |
    | GET /shutdownWorkers     | GET /api/v1/workers                      | get the shutdownWorkers filed in response        |
    | GET /decommissionWorkers | GET /api/v1/workers                      | get the decommissioningWorkers filed in response |
    | POST /exclude            | POST /api/v1/workers/exclude             |                                                  |
    | GET /workerEventInfo     | GET /api/v1/workers/events               |                                                  |
    | POST /sendWorkerEvent    | POST /api/v1/workers/events              |                                                  |


  - The mappings of the old RESTful APIs to the new RESTful APIs for Worker.

    | Old RESTful API                | New RESTful API                          | Note                                        |
    |--------------------------------|------------------------------------------|---------------------------------------------|
    | GET /conf                      | GET /api/v1/conf                         |                                             |
    | GET /listDynamicConfigs        | GET /api/v1/conf/dynamic                 |                                             |
    | GET /threadDump                | GET /api/v1/thread_dump                  |                                             |
    | GET /applications              | GET /api/v1/applications                 |                                             |
    | GET /shuffle                   | GET /api/v1/shuffles                     |                                             |
    | GET /listPartitionLocationInfo | GET /api/v1/shuffles/partitions          |                                             |
    | GET /workerInfo                | GET /api/v1/workers                      |                                             |
    | GET /isRegistered              | GET /api/v1/workers                      | get the isRegistered field in response      |
    | GET /isDecommissioning         | GET /api/v1/workers                      | get the isDecommissioning field in response |
    | GET /isShutdown                | GET /api/v1/workers                      | get the isShutdown field in response        |
    | GET /unavailablePeers          | GET /api/v1/workers/unavailable_peers    |                                             |
    | POST /exit                     | POST /api/v1/workers/exit                |                                             |

- Since 0.6.0, the RESTful api `/listTopDiskUsedApps` both in Master and Worker has been removed. Please use the following PromQL query instead.
  ```text
  topK(50, sum by (applicationId) (metrics_diskBytesWritten_Value{role="worker", applicationId!=""}))
  ```

- Since 0.6.0, the out-of-dated Flink 1.14 and Flink 1.15 have been removed from the official support list.

## Upgrading from 0.5.0 to 0.5.1

- Since 0.5.1, Celeborn master REST API `/exclude` request uses media type `application/x-www-form-urlencoded` instead of `text/plain`.

- Since 0.5.1, Celeborn master REST API `/sendWorkerEvent` request uses POST method and the parameters `type` and `workers` use form parameters instead, and uses media type `application/x-www-form-urlencoded` instead of `text/plain`.

- Since 0.5.1, Celeborn worker REST API `/exit` request uses media type `application/x-www-form-urlencoded` instead of `text/plain`.

## Upgrading from 0.4 to 0.5

- Since 0.5.0, Celeborn master metrics `LostWorkers` is renamed as `LostWorkerCount`.

- Since 0.5.0, Celeborn worker metrics `ChunkStreamCount` is renamed as `ActiveChunkStreamCount`.

- Since 0.5.0, Celeborn worker metrics `CreditStreamCount` is renamed as `ActiveCreditStreamCount`.

- Since 0.5.0, Celeborn configurations support new tag `isDynamic` to represent whether the configuration is dynamic config.

- Since 0.5.0, Celeborn changed the default value of `celeborn.worker.graceful.shutdown.recoverDbBackend` from `LEVELDB` to `ROCKSDB`, which means Celeborn will use RocksDB store for recovery backend. 
   To restore the behavior before Celeborn 0.5, you can set `celeborn.worker.graceful.shutdown.recoverDbBackend` to `LEVELDB`.

- Since 0.5.0, Celeborn deprecate `celeborn.quota.configuration.path`. Please use `celeborn.dynamicConfig.store.fs.path` instead.

- Since 0.5.0, Celeborn client removes configuration `celeborn.client.push.splitPartition.threads`, `celeborn.client.flink.inputGate.minMemory` and `celeborn.client.flink.resultPartition.minMemory`.

- Since 0.5.0, Celeborn deprecate `celeborn.client.spark.shuffle.forceFallback.enabled`. Please use `celeborn.client.spark.shuffle.fallback.policy` instead.

- Since 0.5.0, Celeborn master REST API `/exclude` uses POST method and the parameters `add` and `remove` use form parameters instead.

- Since 0.5.0, Celeborn worker REST API `/exit` uses POST method and the parameter `type` uses form parameter instead.

- Since 0.5.0, Celeborn master and worker REST API `/shuffles` is renamed as `/shuffle`, and will be deprecated since 0.6.0.

## Upgrading from 0.4.0 to 0.4.1

- Since 0.4.1, Celeborn master adds a limit to the estimated partition size used for computing worker slots. 
  This size is now constrained within the range specified by `celeborn.master.estimatedPartitionSize.minSize` and `celeborn.master.estimatedPartitionSize.maxSize`.

- Since 0.4.1, Celeborn changed the fallback configuration of `celeborn.client.rpc.getReducerFileGroup.askTimeout`, `celeborn.client.rpc.registerShuffle.askTimeout` and `celeborn.client.rpc.requestPartition.askTimeout` from `celeborn.<module>.io.connectionTimeout` to `celeborn.rpc.askTimeout`.

## Upgrading from 0.3 to 0.4

- Since 0.4.0, Celeborn won't be compatible with Celeborn client that versions below 0.3.0.
  Note that: It's strongly recommended to use the same version of Client and Celeborn Master/Worker in production.

- Since 0.4.0, Celeborn won't support `org.apache.spark.shuffle.celeborn.RssShuffleManager`.

- Since 0.4.0, Celeborn changed the default value of `celeborn.<module>.io.numConnectionsPerPeer` from `2` to `1`.

- Since 0.4.0, Celeborn has changed the names of the prometheus master and worker configuration as shown in the table below:

    | Key Before v0.4.0                         | Key After v0.4.0            |
    |-------------------------------------------|-----------------------------|
    | `celeborn.metrics.master.prometheus.host` | `celeborn.master.http.host` |
    | `celeborn.metrics.master.prometheus.port` | `celeborn.master.http.port` |
    | `celeborn.metrics.worker.prometheus.host` | `celeborn.worker.http.host` |
    | `celeborn.metrics.worker.prometheus.port` | `celeborn.worker.http.port` |

- Since 0.4.0, Celeborn deprecate `celeborn.worker.storage.baseDir.prefix` and `celeborn.worker.storage.baseDir.number`.
  Please use `celeborn.worker.storage.dirs` instead.

- Since 0.4.0, Celeborn deprecate `celeborn.storage.activeTypes`. Please use `celeborn.storage.availableTypes` instead.

- Since 0.4.0, Celeborn worker removes configuration `celeborn.worker.userResourceConsumption.update.interval`.

- Since 0.4.0, Celeborn master metrics `PartitionWritten` is renamed as `ActiveShuffleSize`.

- Since 0.4.0, Celeborn master metrics `PartitionFileCount` is renamed as `ActiveShuffleFileCount`.

## Upgrading from 0.3.1 to 0.3.2

- Since 0.3.1, Celeborn changed the default value of `raft.client.rpc.request.timeout` from `3s` to `10s`.

- Since 0.3.1, Celeborn changed the default value of `raft.client.rpc.watch.request.timeout` from `10s` to `20s`.

## Upgrading from 0.3.0 to 0.3.1

- Since 0.3.1, Celeborn changed the default value of `celeborn.worker.directMemoryRatioToResume` from `0.5` to `0.7`.

- Since 0.3.1, Celeborn changed the default value of `celeborn.worker.monitor.disk.check.interval` from `60` to `30`.

- Since 0.3.1, name of JVM metrics changed, see details at CELEBORN-1007.

## Upgrading from 0.2 to 0.3

 - Celeborn 0.2 Client is compatible with 0.3 Master/Server, it allows to upgrade Master/Worker first then Client.
   Note that: It's strongly recommended to use the same version of Client and Celeborn Master/Worker in production.

 - Since 0.3.0, the support of deprecated configurations `rss.*` is removed.
   All configurations listed in 0.2.1 docs still take effect, but some of those are deprecated too, please read
   the bootstrap logs and follow the suggestion to migrate to the new configuration.

 - From 0.3.0 on the default value for `celeborn.client.push.replicate.enabled` is changed from `true` to `false`, users
   who want replication on should explicitly enable replication. For example, to enable replication for Spark
   users should add the spark config when submitting job: `spark.celeborn.client.push.replicate.enabled=true`

 - From 0.3.0 on the default value for `celeborn.worker.storage.workingDir` is changed from `hadoop/rss-worker/shuffle_data` to `celeborn-worker/shuffle_data`,
   users who want to use origin working dir path should set this configuration.

 - Since 0.3.0, configuration namespace `celeborn.ha.master` is deprecated, and will be removed in the future versions.
   All configurations `celeborn.ha.master.*` should migrate to `celeborn.master.ha.*`.

 - Since 0.3.0, environment variables `CELEBORN_MASTER_HOST` and `CELEBORN_MASTER_PORT` are removed.
   Instead `CELEBORN_LOCAL_HOSTNAME` works on both master and worker, which takes high priority than configurations defined in properties file.

 - Since 0.3.0, the Celeborn Master URL schema is changed from `rss://` to `celeborn://`, for users who start Worker by
   `sbin/start-worker.sh rss://<master-host>:<master-port>`, should migrate to `sbin/start-worker.sh celeborn://<master-host>:<master-port>`.

 - Since 0.3.0, Celeborn supports overriding Hadoop configuration(`core-site.xml`, `hdfs-site.xml`, etc.) from Celeborn configuration with the additional prefix `celeborn.hadoop.`. 
   On Spark client side, user should set Hadoop configuration like `spark.celeborn.hadoop.foo=bar`, note that `spark.hadoop.foo=bar` does not take effect;
   on Flink client and Celeborn Master/Worker side, user should set like `celeborn.hadoop.foo=bar`.

 - Since 0.3.0, Celeborn master metrics `BlacklistedWorkerCount` is renamed as `ExcludedWorkerCount`.

 - Since 0.3.0, Celeborn master http request url `/blacklistedWorkers` is renamed as `/excludedWorkers`.

 - Since 0.3.0, introduces a terminology update for Celeborn worker data replication, replacing the previous `master/slave` terminology with `primary/replica`. In alignment with this change, corresponding metrics keywords have been adjusted.
   The following table presents a comprehensive overview of the changes:

     | Key Before v0.3.0             | Key After v0.3.0               |
     |-------------------------------|--------------------------------|
     | `MasterPushDataTime`          | `PrimaryPushDataTime`          |
     | `MasterPushDataHandshakeTime` | `PrimaryPushDataHandshakeTime` |
     | `MasterRegionStartTime`       | `PrimaryRegionStartTime`       |
     | `MasterRegionFinishTime`      | `PrimaryRegionFinishTime`      |
     | `SlavePushDataTime`           | `ReplicaPushDataTime`          |
     | `SlavePushDataHandshakeTime`  | `ReplicaPushDataHandshakeTime` |
     | `SlaveRegionStartTime`        | `ReplicaRegionStartTime`       |
     | `SlaveRegionFinishTime`       | `ReplicaRegionFinishTime`      |

 - Since 0.3.0, Celeborn's spark shuffle manager change from `org.apache.spark.shuffle.celeborn.RssShuffleManager` to `org.apache.spark.shuffle.celeborn.SparkShuffleManager`. User can set spark property `spark.shuffle.manager` to `org.apache.spark.shuffle.celeborn.SparkShuffleManager` to use Celeborn remote shuffle service.
   In 0.3.0, Celeborn still support `org.apache.spark.shuffle.celeborn.RssShuffleManager`, it will be removed in 0.4.0.
