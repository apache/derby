# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to you under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Place in ${SANDBOX} (Above ${derby_source}, i.e. trunk, 10.2, 10.1 ....)
# Modify to fit your environment
#
export TOPDIR=${HOME}/testingReportingScripts/outerWorld
export TESTSET=testset
export SANDBOX=sandbox
export DERBYDIR=${TOPDIR}/${TESTSET}/${SANDBOX}
export PUBLISHDIR=public_html/public/Apache/outerWorld/${TESTSET}/${SANDBOX}
export ANT_HOME=/usr/local/share/java/apache-ant-1.6.2 
export PATH=${ANT_HOME}/bin:/usr/local/java/jdk/bin/:$PATH
export JAVA_HOME=/usr/local/java/jdk1.4
export BRANCH_DIR=trunk
#                 10.1
#                 10.2
#                 10.3
#                 etc.
export derby_source=${DERBYDIR}/${BRANCH_DIR}
#
export TOOLDIR=${DERBYDIR}/${BRANCH_DIR}/tools/testing/reporting/scripts
export TEMPLATEDIR=${DERBYDIR}/${BRANCH_DIR}/tools/testing/reporting/TEMPLATES
