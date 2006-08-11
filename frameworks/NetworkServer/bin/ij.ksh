
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

# ---------------------------------------------------------
# -- This simple script is an example of how to start ij in 
# -- the Derby Network Server environment.
# --
#-- REQUIREMENTS: 
# -- You must have the Derby and DB2 JCC libraries in your classpath
# -- 
# -- See the setNetworkClientCP.ksh for an example of
# -- how to do this.
# --
# -- This file for use on Unix ksh systems
# ---------------------------------------------------------

# DERBY_INSTALL=

DERBY_HOME=${DERBY_HOME:-$DERBY_INSTALL}

[ -z "$DERBY_HOME" ] && {
  echo "\$DERBY_HOME or \$DERBY_INSTALL not set. Please set one of these variables"
  echo "to the location of your Derby installation."
  exit 1
}

[ -z "$JAVA_HOME" ] && {
  [ -x /usr/java/bin/java ] && {
    JAVA_HOME=/usr/java
  }
  [ -z "$JAVA_HOME" ] && {
    echo "JAVA_HOME not set. Please set JAVA_HOME to the location of your Java"
    echo "installation."
    exit 1
  }
} 

export IJ_HOST=localhost
export IJ_PORT=1527
export IJ_USER=APP
export IJ_PASSWORD=APP

# ---------------------------------------------------------
# -- start ij
# ---------------------------------------------------------
"$JAVA_HOME/bin/java" -cp "${DERBY_HOME}/lib/derbyclient.jar:${DERBY_HOME}/lib/derbytools.jar:${CLASSPATH}" -Dij.driver=org.apache.derby.jdbc.ClientDriver -Dij.protocol=jdbc:derby://$IJ_HOST:$IJ_PORT/ -Dij.user=$IJ_USER -Dij.password=$IJ_PASSWORD  org.apache.derby.tools.ij

# ---------------------------------------------------------
# -- To use a different JVM with a different syntax, simply edit
# -- this file
# ---------------------------------------------------------
