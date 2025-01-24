---
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

<!--begin-include-->
| Key | Default | isDynamic | Description | Since | Deprecated |
| --- | ------- | --------- | ----------- | ----- | ---------- |
| celeborn.cluster.name | default | false | Celeborn cluster name. | 0.5.0 |  | 
| celeborn.container.info.provider | org.apache.celeborn.server.common.container.DefaultContainerInfoProvider | false | ContainerInfoProvider class name. Default class is `org.apache.celeborn.server.common.container.DefaultContainerInfoProvider`.  | 0.6.0 |  | 
| celeborn.dynamicConfig.refresh.interval | 120s | false | Interval for refreshing the corresponding dynamic config periodically. | 0.4.0 |  | 
| celeborn.dynamicConfig.store.backend | &lt;undefined&gt; | false | Store backend for dynamic config service. The store backend can be specified in two ways: - Using the short name of the store backend defined in the implementation of `ConfigStore#getName` whose return value can be mapped to the corresponding backend implementation. Available options: FS, DB. - Using the service class name of the store backend implementation. If not provided, it means that dynamic configuration is disabled. | 0.4.0 |  | 
| celeborn.dynamicConfig.store.db.fetch.pageSize | 1000 | false | The page size for db store to query configurations. | 0.5.0 |  | 
| celeborn.dynamicConfig.store.db.hikari.connectionTimeout | 30s | false | The connection timeout that a client will wait for a connection from the pool for db store backend. | 0.5.0 |  | 
| celeborn.dynamicConfig.store.db.hikari.driverClassName |  | false | The jdbc driver class name of db store backend. | 0.5.0 |  | 
| celeborn.dynamicConfig.store.db.hikari.idleTimeout | 600s | false | The idle timeout that a connection is allowed to sit idle in the pool for db store backend. | 0.5.0 |  | 
| celeborn.dynamicConfig.store.db.hikari.jdbcUrl |  | false | The jdbc url of db store backend. | 0.5.0 |  | 
| celeborn.dynamicConfig.store.db.hikari.maxLifetime | 1800s | false | The maximum lifetime of a connection in the pool for db store backend. | 0.5.0 |  | 
| celeborn.dynamicConfig.store.db.hikari.maximumPoolSize | 2 | false | The maximum pool size of db store backend. | 0.5.0 |  | 
| celeborn.dynamicConfig.store.db.hikari.password |  | false | The password of db store backend. | 0.5.0 |  | 
| celeborn.dynamicConfig.store.db.hikari.username |  | false | The username of db store backend. | 0.5.0 |  | 
| celeborn.dynamicConfig.store.fs.path | &lt;undefined&gt; | false | The path of dynamic config file for fs store backend. The file format should be yaml. The default path is `${CELEBORN_CONF_DIR}/dynamicConfig.yaml`. | 0.5.0 |  | 
| celeborn.internal.port.enabled | false | false | Whether to create a internal port on Masters/Workers for inter-Masters/Workers communication. This is beneficial when SASL authentication is enforced for all interactions between clients and Celeborn Services, but the services can exchange messages without being subject to SASL authentication. | 0.5.0 |  | 
| celeborn.logConf.enabled | false | false | When `true`, log the CelebornConf for debugging purposes. | 0.5.0 |  | 
| celeborn.master.endpoints | &lt;localhost&gt;:9097 | false | Endpoints of master nodes for celeborn clients to connect. Client uses resolver provided by celeborn.master.endpoints.resolver to resolve the master endpoints. By default Celeborn uses `org.apache.celeborn.common.client.StaticMasterEndpointResolver` which take static master endpoints as input. Allowed pattern: `<host1>:<port1>[,<host2>:<port2>]*`, e.g. `clb1:9097,clb2:9098,clb3:9099`. If the port is omitted, 9097 will be used. If the master endpoints are not static then users can pass custom resolver implementation to discover master endpoints actively using celeborn.master.endpoints.resolver. | 0.2.0 |  | 
| celeborn.master.endpoints.resolver | org.apache.celeborn.common.client.StaticMasterEndpointResolver | false | Resolver class that can be used for discovering and updating the master endpoints. This allows users to provide a custom master endpoint resolver implementation. This is useful in environments where the master nodes might change due to scaling operations or infrastructure updates. Clients need to ensure that provided resolver class should be present in the classpath. | 0.6.0 |  | 
| celeborn.master.estimatedPartitionSize.minSize | 8mb | false | Ignore partition size smaller than this configuration of partition size for estimation. | 0.3.0 | celeborn.shuffle.minPartitionSizeToEstimate | 
| celeborn.master.internal.endpoints | &lt;localhost&gt;:8097 | false | Endpoints of master nodes just for celeborn workers to connect, allowed pattern is: `<host1>:<port1>[,<host2>:<port2>]*`, e.g. `clb1:8097,clb2:8097,clb3:8097`. If the port is omitted, 8097 will be used. | 0.5.0 |  | 
| celeborn.redaction.regex | (?i)secret|password|token|access[.]key | false | Regex to decide which Celeborn configuration properties and environment variables in master and worker environments contain sensitive information. When this regex matches a property key or value, the value is redacted from the logging. | 0.5.0 |  | 
| celeborn.shuffle.chunk.size | 8m | false | Max chunk size of reducer's merged shuffle data. For example, if a reducer's shuffle data is 128M and the data will need 16 fetch chunk requests to fetch. | 0.2.0 |  | 
| celeborn.shuffle.sortPartition.block.compactionFactor | 0.25 | false | Combine sorted shuffle blocks such that size of compacted shuffle block does not exceed compactionFactor * celeborn.shuffle.chunk.size | 0.4.2 |  | 
| celeborn.storage.availableTypes | HDD | false | Enabled storages. Available options: MEMORY,HDD,SSD,HDFS,S3. Note: HDD and SSD would be treated as identical. | 0.3.0 | celeborn.storage.activeTypes | 
| celeborn.storage.hdfs.dir | &lt;undefined&gt; | false | HDFS base directory for Celeborn to store shuffle data. | 0.2.0 |  | 
| celeborn.storage.hdfs.kerberos.keytab | &lt;undefined&gt; | false | Kerberos keytab file path for HDFS storage connection. | 0.3.2 |  | 
| celeborn.storage.hdfs.kerberos.principal | &lt;undefined&gt; | false | Kerberos principal for HDFS storage connection. | 0.3.2 |  | 
| celeborn.storage.s3.access.key | &lt;undefined&gt; | false | S3 access key for Celeborn to store shuffle data. | 0.6.0 |  | 
| celeborn.storage.s3.dir | &lt;undefined&gt; | false | S3 base directory for Celeborn to store shuffle data. | 0.6.0 |  | 
| celeborn.storage.s3.endpoint.region | &lt;undefined&gt; | false | S3 endpoint for Celeborn to store shuffle data. | 0.6.0 |  | 
| celeborn.storage.s3.mpu.maxRetries | 5 | false | S3 MPU upload max retries. | 0.6.0 |  | 
| celeborn.storage.s3.secret.key | &lt;undefined&gt; | false | S3 secret key for Celeborn to store shuffle data. | 0.6.0 |  | 
| celeborn.worker.activeConnection.max | &lt;undefined&gt; | false | If the number of active connections on a worker exceeds this configuration value, the worker will be marked as high-load in the heartbeat report, and the master will not include that node in the response of RequestSlots. | 0.3.1 |  | 
| celeborn.worker.applicationRegistry.cache.size | 10000 | false | Cache size of the application registry on Workers. | 0.5.0 |  | 
| celeborn.worker.bufferStream.threadsPerMountpoint | 8 | false | Threads count for read buffer per mount point. | 0.3.0 |  | 
| celeborn.worker.clean.threads | 64 | false | Thread number of worker to clean up expired shuffle keys. | 0.3.2 |  | 
| celeborn.worker.closeIdleConnections | false | false | Whether worker will close idle connections. | 0.2.0 |  | 
| celeborn.worker.commitFiles.check.interval | 100 | false | Time length for a window about checking whether commit shuffle data files finished. | 0.6.0 |  | 
| celeborn.worker.commitFiles.threads | 32 | false | Thread number of worker to commit shuffle data files asynchronously. It's recommended to set at least `128` when `HDFS` is enabled in `celeborn.storage.availableTypes`. | 0.3.0 | celeborn.worker.commit.threads | 
| celeborn.worker.commitFiles.timeout | 120s | false | Timeout for a Celeborn worker to commit files of a shuffle. It's recommended to set at least `240s` when `HDFS` is enabled in `celeborn.storage.availableTypes`. | 0.3.0 | celeborn.worker.shuffle.commit.timeout | 
| celeborn.worker.congestionControl.check.interval | 10ms | false | Interval of worker checks congestion if celeborn.worker.congestionControl.enabled is true. | 0.3.2 |  | 
| celeborn.worker.congestionControl.diskBuffer.high.watermark | 9223372036854775807b | false | If the total bytes in disk buffer exceeds this configure, will start to congest users whose produce rate is higher than the potential average consume rate. The congestion will stop if the produce rate is lower or equal to the average consume rate, or the total pending bytes lower than celeborn.worker.congestionControl.diskBuffer.low.watermark | 0.3.0 | celeborn.worker.congestionControl.high.watermark | 
| celeborn.worker.congestionControl.diskBuffer.low.watermark | 9223372036854775807b | false | Will stop congest users if the total pending bytes of disk buffer is lower than this configuration | 0.3.0 | celeborn.worker.congestionControl.low.watermark | 
| celeborn.worker.congestionControl.enabled | false | false | Whether to enable congestion control or not. | 0.3.0 |  | 
| celeborn.worker.congestionControl.sample.time.window | 10s | false | The worker holds a time sliding list to calculate users' produce/consume rate | 0.3.0 |  | 
| celeborn.worker.congestionControl.user.inactive.interval | 10min | false | How long will consider this user is inactive if it doesn't send data | 0.3.0 |  | 
| celeborn.worker.congestionControl.userProduceSpeed.high.watermark | 9223372036854775807b | false | For those users that produce byte speeds greater than this configuration, start congestion for these users | 0.6.0 |  | 
| celeborn.worker.congestionControl.userProduceSpeed.low.watermark | 9223372036854775807b | false | For those users that produce byte speeds less than this configuration, stop congestion for these users | 0.6.0 |  | 
| celeborn.worker.congestionControl.workerProduceSpeed.high.watermark | 9223372036854775807b | false | Start congestion If worker total produce speed greater than this configuration | 0.6.0 |  | 
| celeborn.worker.congestionControl.workerProduceSpeed.low.watermark | 9223372036854775807b | false | Stop congestion If worker total produce speed less than this configuration | 0.6.0 |  | 
| celeborn.worker.decommission.checkInterval | 30s | false | The wait interval of checking whether all the shuffle expired during worker decommission | 0.4.0 |  | 
| celeborn.worker.decommission.forceExitTimeout | 6h | false | The wait time of waiting for all the shuffle expire during worker decommission. | 0.4.0 |  | 
| celeborn.worker.directMemoryRatioForMemoryFileStorage | 0.0 | false | Max ratio of direct memory to store shuffle data. This feature is experimental and disabled by default. | 0.5.0 |  | 
| celeborn.worker.directMemoryRatioForReadBuffer | 0.1 | false | Max ratio of direct memory for read buffer | 0.2.0 |  | 
| celeborn.worker.directMemoryRatioToPauseReceive | 0.85 | false | If direct memory usage reaches this limit, the worker will stop to receive data from Celeborn shuffle clients. | 0.2.0 |  | 
| celeborn.worker.directMemoryRatioToPauseReplicate | 0.95 | false | If direct memory usage reaches this limit, the worker will stop to receive replication data from other workers. This value should be higher than celeborn.worker.directMemoryRatioToPauseReceive. | 0.2.0 |  | 
| celeborn.worker.directMemoryRatioToResume | 0.7 | false | If direct memory usage is less than this limit, worker will resume. | 0.2.0 |  | 
| celeborn.worker.disk.clean.threads | 4 | false | Thread number of worker to clean up directories of expired shuffle keys on disk. | 0.3.2 |  | 
| celeborn.worker.fetch.heartbeat.enabled | false | false | enable the heartbeat from worker to client when fetching data | 0.3.0 |  | 
| celeborn.worker.fetch.io.threads | &lt;undefined&gt; | false | Netty IO thread number of worker to handle client fetch data. The default threads number is the number of flush thread. | 0.2.0 |  | 
| celeborn.worker.fetch.port | 0 | false | Server port for Worker to receive fetch data request from ShuffleClient. | 0.2.0 |  | 
| celeborn.worker.flusher.buffer.size | 256k | false | Size of buffer used by a single flusher. | 0.2.0 |  | 
| celeborn.worker.flusher.diskTime.slidingWindow.size | 20 | false | The size of sliding windows used to calculate statistics about flushed time and count. | 0.3.0 | celeborn.worker.flusher.avgFlushTime.slidingWindow.size | 
| celeborn.worker.flusher.hdd.threads | 1 | false | Flusher's thread count per disk used for write data to HDD disks. | 0.2.0 |  | 
| celeborn.worker.flusher.hdfs.buffer.size | 4m | false | Size of buffer used by a HDFS flusher. | 0.3.0 |  | 
| celeborn.worker.flusher.hdfs.threads | 8 | false | Flusher's thread count used for write data to HDFS. | 0.2.0 |  | 
| celeborn.worker.flusher.s3.buffer.size | 6m | false | Size of buffer used by a S3 flusher. | 0.6.0 |  | 
| celeborn.worker.flusher.s3.threads | 8 | false | Flusher's thread count used for write data to S3. | 0.6.0 |  | 
| celeborn.worker.flusher.shutdownTimeout | 3s | false | Timeout for a flusher to shutdown. | 0.2.0 |  | 
| celeborn.worker.flusher.ssd.threads | 16 | false | Flusher's thread count per disk used for write data to SSD disks. | 0.2.0 |  | 
| celeborn.worker.flusher.threads | 16 | false | Flusher's thread count per disk for unknown-type disks. | 0.2.0 |  | 
| celeborn.worker.graceful.shutdown.checkSlotsFinished.interval | 1s | false | The wait interval of checking whether all released slots to be committed or destroyed during worker graceful shutdown | 0.2.0 |  | 
| celeborn.worker.graceful.shutdown.checkSlotsFinished.timeout | 480s | false | The wait time of waiting for the released slots to be committed or destroyed during worker graceful shutdown. | 0.2.0 |  | 
| celeborn.worker.graceful.shutdown.enabled | false | false | When true, during worker shutdown, the worker will wait for all released slots to be committed or destroyed. | 0.2.0 |  | 
| celeborn.worker.graceful.shutdown.partitionSorter.shutdownTimeout | 120s | false | The wait time of waiting for sorting partition files during worker graceful shutdown. | 0.2.0 |  | 
| celeborn.worker.graceful.shutdown.recoverDbBackend | ROCKSDB | false | Specifies a disk-based store used in local db. ROCKSDB or LEVELDB (deprecated). | 0.4.0 |  | 
| celeborn.worker.graceful.shutdown.recoverPath | &lt;tmp&gt;/recover | false | The path to store DB. | 0.2.0 |  | 
| celeborn.worker.graceful.shutdown.saveCommittedFileInfo.interval | 5s | false | Interval for a Celeborn worker to flush committed file infos into Level DB. | 0.3.1 |  | 
| celeborn.worker.graceful.shutdown.saveCommittedFileInfo.sync | false | false | Whether to call sync method to save committed file infos into Level DB to handle OS crash. | 0.3.1 |  | 
| celeborn.worker.graceful.shutdown.timeout | 600s | false | The worker's graceful shutdown timeout time. | 0.2.0 |  | 
| celeborn.worker.http.auth.administers |  | false | A comma-separated list of users who have admin privileges, Note, when celeborn.worker.http.auth.supportedSchemes is not set, everyone is treated as administrator. | 0.6.0 |  | 
| celeborn.worker.http.auth.basic.provider | org.apache.celeborn.common.authentication.AnonymousAuthenticationProviderImpl | false | User-defined password authentication implementation of org.apache.celeborn.common.authentication.PasswdAuthenticationProvider | 0.6.0 |  | 
| celeborn.worker.http.auth.bearer.provider | org.apache.celeborn.common.authentication.AnonymousAuthenticationProviderImpl | false | User-defined token authentication implementation of org.apache.celeborn.common.authentication.TokenAuthenticationProvider | 0.6.0 |  | 
| celeborn.worker.http.auth.supportedSchemes |  | false | A comma-separated list of worker http auth supported schemes.<ul> <li>SPNEGO: Kerberos/GSSAPI authentication.</li> <li>BASIC: User-defined password authentication, the concreted implementation is configurable via `celeborn.worker.http.auth.basic.provider`.</li> <li>BEARER: User-defined bearer token authentication, the concreted implementation is configurable via `celeborn.worker.http.auth.bearer.provider`.</li></ul> | 0.6.0 |  | 
| celeborn.worker.http.host | &lt;localhost&gt; | false | Worker's http host. | 0.4.0 | celeborn.metrics.worker.prometheus.host,celeborn.worker.metrics.prometheus.host | 
| celeborn.worker.http.idleTimeout | 30s | false | Worker http server idle timeout. | 0.5.0 |  | 
| celeborn.worker.http.maxWorkerThreads | 200 | false | Maximum number of threads in the worker http worker thread pool. | 0.5.0 |  | 
| celeborn.worker.http.port | 9096 | false | Worker's http port. | 0.4.0 | celeborn.metrics.worker.prometheus.port,celeborn.worker.metrics.prometheus.port | 
| celeborn.worker.http.proxy.client.ip.header | X-Real-IP | false | The HTTP header to record the real client IP address. If your server is behind a load balancer or other proxy, the server will see this load balancer or proxy IP address as the client IP address, to get around this common issue, most load balancers or proxies offer the ability to record the real remote IP address in an HTTP header that will be added to the request for other devices to use. Note that, because the header value can be specified to any IP address, so it will not be used for authentication. | 0.6.0 |  | 
| celeborn.worker.http.spnego.keytab | &lt;undefined&gt; | false | The keytab file for SPNego authentication. | 0.6.0 |  | 
| celeborn.worker.http.spnego.principal | &lt;undefined&gt; | false | SPNego service principal, typical value would look like HTTP/_HOST@EXAMPLE.COM. SPNego service principal would be used when celeborn http authentication is enabled. This needs to be set only if SPNEGO is to be used in authentication. | 0.6.0 |  | 
| celeborn.worker.http.ssl.disallowed.protocols | SSLv2,SSLv3 | false | SSL versions to disallow. | 0.6.0 |  | 
| celeborn.worker.http.ssl.enabled | false | false | Set this to true for using SSL encryption in http server. | 0.6.0 |  | 
| celeborn.worker.http.ssl.include.ciphersuites |  | false | A comma-separated list of include SSL cipher suite names. | 0.6.0 |  | 
| celeborn.worker.http.ssl.keystore.algorithm | &lt;undefined&gt; | false | SSL certificate keystore algorithm. | 0.6.0 |  | 
| celeborn.worker.http.ssl.keystore.password | &lt;undefined&gt; | false | SSL certificate keystore password. | 0.6.0 |  | 
| celeborn.worker.http.ssl.keystore.path | &lt;undefined&gt; | false | SSL certificate keystore location. | 0.6.0 |  | 
| celeborn.worker.http.ssl.keystore.type | &lt;undefined&gt; | false | SSL certificate keystore type. | 0.6.0 |  | 
| celeborn.worker.http.stopTimeout | 5s | false | Worker http server stop timeout. | 0.5.0 |  | 
| celeborn.worker.internal.port | 0 | false | Internal server port on the Worker where the master nodes connect. | 0.5.0 |  | 
| celeborn.worker.jvmProfiler.enabled | false | false | Turn on code profiling via async_profiler in workers. | 0.5.0 |  | 
| celeborn.worker.jvmProfiler.localDir | . | false | Local file system path on worker where profiler output is saved. Defaults to the working directory of the worker process. | 0.5.0 |  | 
| celeborn.worker.jvmProfiler.options | event=wall,interval=10ms,alloc=2m,lock=10ms,chunktime=300s | false | Options to pass on to the async profiler. | 0.5.0 |  | 
| celeborn.worker.jvmQuake.check.interval | 1s | false | Interval of gc behavior checking for worker jvm quake. | 0.4.0 |  | 
| celeborn.worker.jvmQuake.dump.enabled | true | false | Whether to heap dump for the maximum GC 'deficit' during worker jvm quake. | 0.4.0 |  | 
| celeborn.worker.jvmQuake.dump.path | &lt;tmp&gt;/jvm-quake/dump/&lt;pid&gt; | false | The path of heap dump for the maximum GC 'deficit' during worker jvm quake. | 0.4.0 |  | 
| celeborn.worker.jvmQuake.dump.threshold | 30s | false | The threshold of heap dump for the maximum GC 'deficit' which can be accumulated before jvmquake takes action. Meanwhile, there is no heap dump generated when dump threshold is greater than kill threshold. | 0.4.0 |  | 
| celeborn.worker.jvmQuake.enabled | false | false | When true, Celeborn worker will start the jvm quake to monitor of gc behavior, which enables early detection of memory management issues and facilitates fast failure. | 0.4.0 |  | 
| celeborn.worker.jvmQuake.exitCode | 502 | false | The exit code of system kill for the maximum GC 'deficit' during worker jvm quake. | 0.4.0 |  | 
| celeborn.worker.jvmQuake.kill.threshold | 60s | false | The threshold of system kill for the maximum GC 'deficit' which can be accumulated before jvmquake takes action. | 0.4.0 |  | 
| celeborn.worker.jvmQuake.runtimeWeight | 5.0 | false | The factor by which to multiply running JVM time, when weighing it against GCing time. 'Deficit' is accumulated as `gc_time - runtime * runtime_weight`, and is compared against threshold to determine whether to take action. | 0.4.0 |  | 
| celeborn.worker.memoryFileStorage.evict.aggressiveMode.enabled | false | false | If this set to true, memory shuffle files will be evicted when worker is in PAUSED state. If the worker's offheap memory is not ample, set this to true and decrease `celeborn.worker.directMemoryRatioForMemoryFileStorage` will be helpful. | 0.5.1 |  | 
| celeborn.worker.memoryFileStorage.evict.ratio | 0.5 | false | If memory shuffle storage usage rate is above this config, the memory storage shuffle files will evict to free memory. | 0.5.1 |  | 
| celeborn.worker.memoryFileStorage.maxFileSize | 8MB | false | Max size for a memory storage file. It must be less than 2GB. | 0.5.0 |  | 
| celeborn.worker.monitor.disk.check.interval | 30s | false | Intervals between device monitor to check disk. | 0.3.0 | celeborn.worker.monitor.disk.checkInterval | 
| celeborn.worker.monitor.disk.check.timeout | 30s | false | Timeout time for worker check device status. | 0.3.0 | celeborn.worker.disk.check.timeout | 
| celeborn.worker.monitor.disk.checklist | readwrite,diskusage | false | Monitor type for disk, available items are: iohang, readwrite and diskusage. | 0.2.0 |  | 
| celeborn.worker.monitor.disk.enabled | true | false | When true, worker will monitor device and report to master. | 0.3.0 |  | 
| celeborn.worker.monitor.disk.notifyError.expireTimeout | 10m | false | The expire timeout of non-critical device error. Only notify critical error when the number of non-critical errors for a period of time exceeds threshold. | 0.3.0 |  | 
| celeborn.worker.monitor.disk.notifyError.threshold | 64 | false | Device monitor will only notify critical error once the accumulated valid non-critical error number exceeding this threshold. | 0.3.0 |  | 
| celeborn.worker.monitor.disk.sys.block.dir | /sys/block | false | The directory where linux file block information is stored. | 0.2.0 |  | 
| celeborn.worker.monitor.memory.check.interval | 10ms | false | Interval of worker direct memory checking. | 0.3.0 | celeborn.worker.memory.checkInterval | 
| celeborn.worker.monitor.memory.report.interval | 10s | false | Interval of worker direct memory tracker reporting to log. | 0.3.0 | celeborn.worker.memory.reportInterval | 
| celeborn.worker.monitor.memory.trimChannelWaitInterval | 1s | false | Wait time after worker trigger channel to trim cache. | 0.3.0 |  | 
| celeborn.worker.monitor.memory.trimFlushWaitInterval | 1s | false | Wait time after worker trigger StorageManger to flush data. | 0.3.0 |  | 
| celeborn.worker.monitor.pinnedMemory.check.enabled | true | false | If true, MemoryManager will check worker should resume by pinned memory used. | 0.6.0 |  | 
| celeborn.worker.monitor.pinnedMemory.check.interval | 10s | false | Interval of worker direct pinned memory checking, only takes effect when celeborn.network.memory.allocator.pooled and celeborn.worker.monitor.pinnedMemory.check.enabled are enabled. | 0.6.0 |  | 
| celeborn.worker.partition.initial.readBuffersMax | 1024 | false | Max number of initial read buffers | 0.3.0 |  | 
| celeborn.worker.partition.initial.readBuffersMin | 1 | false | Min number of initial read buffers | 0.3.0 |  | 
| celeborn.worker.partitionSorter.directMemoryRatioThreshold | 0.1 | false | Max ratio of partition sorter's memory for sorting, when reserved memory is higher than max partition sorter memory, partition sorter will stop sorting. If this value is set to 0, partition files sorter will skip memory check and ServingState check. | 0.2.0 |  | 
| celeborn.worker.pinnedMemoryRatioToResume | 0.3 | false | If pinned memory usage is less than this limit, worker will resume, only takes effect when celeborn.network.memory.allocator.pooled and celeborn.worker.monitor.pinnedMemory.check.enabled are enabled | 0.6.0 |  | 
| celeborn.worker.push.heartbeat.enabled | false | false | enable the heartbeat from worker to client when pushing data | 0.3.0 |  | 
| celeborn.worker.push.io.threads | &lt;undefined&gt; | false | Netty IO thread number of worker to handle client push data. The default threads number is the number of flush thread. | 0.2.0 |  | 
| celeborn.worker.push.port | 0 | false | Server port for Worker to receive push data request from ShuffleClient. | 0.2.0 |  | 
| celeborn.worker.readBuffer.allocationWait | 50ms | false | The time to wait when buffer dispatcher can not allocate a buffer. | 0.3.0 |  | 
| celeborn.worker.readBuffer.target.changeThreshold | 1mb | false | The target ratio for pre read memory usage. | 0.3.0 |  | 
| celeborn.worker.readBuffer.target.ratio | 0.9 | false | The target ratio for read ahead buffer's memory usage. | 0.3.0 |  | 
| celeborn.worker.readBuffer.target.updateInterval | 100ms | false | The interval for memory manager to calculate new read buffer's target memory. | 0.3.0 |  | 
| celeborn.worker.readBuffer.toTriggerReadMin | 32 | false | Min buffers count for map data partition to trigger read. | 0.3.0 |  | 
| celeborn.worker.register.timeout | 180s | false | Worker register timeout. | 0.2.0 |  | 
| celeborn.worker.replicate.fastFail.duration | 60s | false | If a replicate request not replied during the duration, worker will mark the replicate data request as failed. It's recommended to set at least `240s` when `HDFS` is enabled in `celeborn.storage.availableTypes`. | 0.2.0 |  | 
| celeborn.worker.replicate.io.threads | &lt;undefined&gt; | false | Netty IO thread number of worker to replicate shuffle data. The default threads number is the number of flush thread. | 0.2.0 |  | 
| celeborn.worker.replicate.port | 0 | false | Server port for Worker to receive replicate data request from other Workers. | 0.2.0 |  | 
| celeborn.worker.replicate.randomConnection.enabled | true | false | Whether worker will create random connection to peer when replicate data. When false, worker tend to reuse the same cached TransportClient to a specific replicate worker; when true, worker tend to use different cached TransportClient. Netty will use the same thread to serve the same connection, so with more connections replicate server can leverage more netty threads | 0.2.1 |  | 
| celeborn.worker.replicate.threads | 64 | false | Thread number of worker to replicate shuffle data. | 0.2.0 |  | 
| celeborn.worker.rpc.port | 0 | false | Server port for Worker to receive RPC request. | 0.2.0 |  | 
| celeborn.worker.shuffle.partitionSplit.enabled | true | false | enable the partition split on worker side | 0.3.0 | celeborn.worker.partition.split.enabled | 
| celeborn.worker.shuffle.partitionSplit.max | 2g | false | Specify the maximum partition size for splitting, and ensure that individual partition files are always smaller than this limit. | 0.3.0 |  | 
| celeborn.worker.shuffle.partitionSplit.min | 1m | false | Min size for a partition to split | 0.3.0 | celeborn.shuffle.partitionSplit.min | 
| celeborn.worker.sortFiles.check.interval | 100 | false | Time length for a window about checking whether FileSorter task finished. | 0.6.0 |  | 
| celeborn.worker.sortPartition.indexCache.expire | 180s | false | PartitionSorter's cache item expire time. | 0.4.0 |  | 
| celeborn.worker.sortPartition.indexCache.maxWeight | 100000 | false | PartitionSorter's cache max weight for index buffer. | 0.4.0 |  | 
| celeborn.worker.sortPartition.prefetch.enabled | true | false | When true, partition sorter will prefetch the original partition files to page cache and reserve memory configured by `celeborn.worker.sortPartition.reservedMemoryPerPartition` to allocate a block of memory for prefetching while sorting a shuffle file off-heap with page cache for non-hdfs files. Otherwise, partition sorter seeks to position of each block and does not prefetch for non-hdfs files. | 0.5.0 |  | 
| celeborn.worker.sortPartition.reservedMemoryPerPartition | 1mb | false | Reserved memory when sorting a shuffle file off-heap. | 0.3.0 | celeborn.worker.partitionSorter.reservedMemoryPerPartition | 
| celeborn.worker.sortPartition.threads | &lt;undefined&gt; | false | PartitionSorter's thread counts. It's recommended to set at least `64` when `HDFS` is enabled in `celeborn.storage.availableTypes`. | 0.3.0 | celeborn.worker.partitionSorter.threads | 
| celeborn.worker.sortPartition.timeout | 220s | false | Timeout for a shuffle file to sort. | 0.3.0 | celeborn.worker.partitionSorter.sort.timeout | 
| celeborn.worker.storage.checkDirsEmpty.maxRetries | 3 | false | The number of retries for a worker to check if the working directory is cleaned up before registering with the master. | 0.3.0 | celeborn.worker.disk.checkFileClean.maxRetries | 
| celeborn.worker.storage.checkDirsEmpty.timeout | 1000ms | false | The wait time per retry for a worker to check if the working directory is cleaned up before registering with the master. | 0.3.0 | celeborn.worker.disk.checkFileClean.timeout | 
| celeborn.worker.storage.dirs | &lt;undefined&gt; | false | Directory list to store shuffle data. It's recommended to configure one directory on each disk. Storage size limit can be set for each directory. For the sake of performance, there should be no more than 2 flush threads on the same disk partition if you are using HDD, and should be 8 or more flush threads on the same disk partition if you are using SSD. For example: `dir1[:capacity=][:disktype=][:flushthread=],dir2[:capacity=][:disktype=][:flushthread=]` | 0.2.0 |  | 
| celeborn.worker.storage.disk.reserve.ratio | &lt;undefined&gt; | false | Celeborn worker reserved ratio for each disk. The minimum usable size for each disk is the max space between the reserved space and the space calculate via reserved ratio. | 0.3.2 |  | 
| celeborn.worker.storage.disk.reserve.size | 5G | false | Celeborn worker reserved space for each disk. | 0.3.0 | celeborn.worker.disk.reserve.size | 
| celeborn.worker.storage.expireDirs.timeout | 1h | false | The timeout for a expire dirs to be deleted on disk. | 0.3.2 |  | 
| celeborn.worker.storage.storagePolicy.createFilePolicy | &lt;undefined&gt; | false | This defined the order for creating files across available storages. Available storages options are: MEMORY,SSD,HDD,HDFS,OSS | 0.5.1 |  | 
| celeborn.worker.storage.storagePolicy.evictPolicy | &lt;undefined&gt; | false | This define the order of evict files if the storages are available. Available storages: MEMORY,SSD,HDD,HDFS. Definition: StorageTypes|StorageTypes|StorageTypes. Example: MEMORY,SSD|SSD,HDFS. The example means that a MEMORY shuffle file can be evicted to SSD and a SSD shuffle file can be evicted to HDFS. | 0.5.1 |  | 
| celeborn.worker.storage.workingDir | celeborn-worker/shuffle_data | false | Worker's working dir path name. | 0.3.0 | celeborn.worker.workingDir | 
| celeborn.worker.writer.close.timeout | 120s | false | Timeout for a file writer to close | 0.2.0 |  | 
| celeborn.worker.writer.create.maxAttempts | 3 | false | Retry count for a file writer to create if its creation was failed. | 0.2.0 |  | 
<!--end-include-->
