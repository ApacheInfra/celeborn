#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# Starts the clb master and workers on the machine this script is executed on.

if [ -z "${CLB_HOME}" ]; then
  export CLB_HOME="$(cd "`dirname "$0"`"/..; pwd)"
fi

. "${CLB_HOME}/sbin/clb-config.sh"

if [ -f "${CLB_CONF_DIR}/hosts" ]; then
  HOST_LIST=$(awk '/\[/{prefix=$0; next} $1{print prefix,$0}' "${CLB_CONF_DIR}/hosts")
else
  HOST_LIST="[master] localhost\n[worker] localhost"
fi

# By default disable strict host key checking
if [ "$CLB_SSH_OPTS" = "" ]; then
  CLB_SSH_OPTS="-o StrictHostKeyChecking=no"
fi

# start masters
for host in `echo "$HOST_LIST" | sed  "s/#.*$//;/^$/d" | grep '\[master\]' | awk '{print $NF}'`
do
  if [ -n "${CLB_SSH_FOREGROUND}" ]; then
    ssh $CLB_SSH_OPTS "$host" "${CLB_HOME}/sbin/stop-master.sh"
  else
    ssh $CLB_SSH_OPTS "$host" "${CLB_HOME}/sbin/stop-master.sh" &
  fi
  if [ "$CLB_SLEEP" != "" ]; then
    sleep $CLB_SLEEP
  fi
done

# start workers
for host in `echo "$HOST_LIST"| sed  "s/#.*$//;/^$/d" | grep '\[worker\]' | awk '{print $NF}'`
do
  if [ -n "${CLB_SSH_FOREGROUND}" ]; then
    ssh $CLB_SSH_OPTS "$host" "${CLB_HOME}/sbin/stop-worker.sh"
  else
    ssh $CLB_SSH_OPTS "$host" "${CLB_HOME}/sbin/stop-worker.sh" &
  fi
  if [ "$CLB_SLEEP" != "" ]; then
    sleep $CLB_SLEEP
  fi
done

wait