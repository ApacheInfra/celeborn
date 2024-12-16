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
| celeborn.metrics.application.enabled | true | false | When false, the metrics of application won't return to reduce the num of metrics. | 0.6.0 |  | 
| celeborn.metrics.capacity | 4096 | false | The maximum number of metrics which a source can use to generate output strings. | 0.2.0 |  | 
| celeborn.metrics.collectPerfCritical.enabled | false | false | It controls whether to collect metrics which may affect performance. When enable, Celeborn collects them. | 0.2.0 |  | 
| celeborn.metrics.conf | &lt;undefined&gt; | false | Custom metrics configuration file path. Default use `metrics.properties` in classpath. | 0.3.0 |  | 
| celeborn.metrics.enabled | true | false | When true, enable metrics system. | 0.2.0 |  | 
| celeborn.metrics.extraLabels |  | false | If default metric labels are not enough, extra metric labels can be customized. Labels' pattern is: `<label1_key>=<label1_value>[,<label2_key>=<label2_value>]*`; e.g. `env=prod,version=1` | 0.3.0 |  | 
| celeborn.metrics.json.path | /metrics/json | false | URI context path of json metrics HTTP server. | 0.4.0 |  | 
| celeborn.metrics.json.pretty.enabled | true | false | When true, view metrics in json pretty format | 0.4.0 |  | 
| celeborn.metrics.prometheus.path | /metrics/prometheus | false | URI context path of prometheus metrics HTTP server. | 0.4.0 |  | 
| celeborn.metrics.sample.rate | 1.0 | false | It controls if Celeborn collect timer metrics for some operations. Its value should be in [0.0, 1.0]. | 0.2.0 |  | 
| celeborn.metrics.timer.slidingWindow.size | 4096 | false | The sliding window size of timer metric. | 0.2.0 |  | 
| celeborn.metrics.worker.app.topResourceConsumption.count | 50 | false | Size for top items about top resource consumption applications list of worker. The top resource consumption is determined by sum of diskBytesWritten and hdfsBytesWritten. The top resource consumption count prevents the total number of metrics from exceeding the metrics capacity. | 0.6.0 |  | 
| celeborn.metrics.worker.pauseSpentTime.forceAppend.threshold | 10 | false | Force append worker pause spent time even if worker still in pause serving state. Help user can find worker pause spent time increase, when worker always been pause state. |  |  | 
<!--end-include-->
