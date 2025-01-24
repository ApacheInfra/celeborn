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
| celeborn.quota.cluster.diskBytesWritten | 9223372036854775807b | true | Cluster level quota dynamic configuration for written disk bytes. | 0.6.0 |  | 
| celeborn.quota.cluster.diskFileCount | 9223372036854775807b | true | Cluster level quota dynamic configuration for written disk file count. | 0.6.0 |  | 
| celeborn.quota.cluster.hdfsBytesWritten | 9223372036854775807b | true | Cluster level quota dynamic configuration for written hdfs bytes. | 0.6.0 |  | 
| celeborn.quota.cluster.hdfsFileCount | 9223372036854775807b | true | Cluster level quota dynamic configuration for written hdfs file count. | 0.6.0 |  | 
| celeborn.quota.enabled | true | false | When Master side sets to true, the master will enable to check the quota via QuotaManager. When Client side sets to true, LifecycleManager will request Master side to check whether the current user has enough quota before registration of shuffle. Fallback to the default shuffle service when Master side checks that there is no enough quota for current user. | 0.2.0 |  | 
| celeborn.quota.interruptShuffle.enabled | false | false | Whether to enable interrupt shuffle when quota exceeds. | 0.6.0 |  | 
| celeborn.quota.tenant.diskBytesWritten | 9223372036854775807b | true | Tenant level quota dynamic configuration for written disk bytes. | 0.5.0 |  | 
| celeborn.quota.tenant.diskFileCount | 9223372036854775807b | true | Tenant level quota dynamic configuration for written disk file count. | 0.5.0 |  | 
| celeborn.quota.tenant.hdfsBytesWritten | 9223372036854775807b | true | Tenant level quota dynamic configuration for written hdfs bytes. | 0.5.0 |  | 
| celeborn.quota.tenant.hdfsFileCount | 9223372036854775807b | true | Tenant level quota dynamic configuration for written hdfs file count. | 0.5.0 |  | 
| celeborn.quota.user.diskBytesWritten | 9223372036854775807b | true | User level quota dynamic configuration for written disk bytes. | 0.6.0 |  | 
| celeborn.quota.user.diskFileCount | 9223372036854775807b | true | User level quota dynamic configuration for written disk file count. | 0.6.0 |  | 
| celeborn.quota.user.hdfsBytesWritten | 9223372036854775807b | true | User level quota dynamic configuration for written hdfs bytes. | 0.6.0 |  | 
| celeborn.quota.user.hdfsFileCount | 9223372036854775807b | true | User level quota dynamic configuration for written hdfs file count. | 0.6.0 |  | 
<!--end-include-->
