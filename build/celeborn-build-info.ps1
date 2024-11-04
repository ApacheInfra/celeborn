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

# This script generates the build info for celeborn and places it into the celeborn-version-info.properties file.
# Arguments:
#   ResourceDir - The target directory where properties file would be created. [./core/target/extra-resources]
#   CelebornVersion - The current version of celeborn

param(
    # The resource directory.
    [Parameter(Position = 0)]
    [String]
    $ResourceDir,

    # The celeborn version.
    [Parameter(Position = 1)]
    [String]
    $CelebornVersion
)

$null = New-Item -Type Directory -Force $ResourceDir
$CelebornBuildInfoPath = $ResourceDir.TrimEnd('\').TrimEnd('/') + '\celeborn-version-info.properties'

$CelebornBuildInfoContent =
"version=$CelebornVersion
user=$($Env:USERNAME)
revision=$(git rev-parse HEAD)
branch=$(git rev-parse --abbrev-ref HEAD)
date=$([DateTime]::UtcNow | Get-Date -UFormat +%Y-%m-%dT%H:%M:%SZ)"

Set-Content -Path $CelebornBuildInfoPath -Value $CelebornBuildInfoContent
