
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

## ---------------------------------------------------------
# -- This simple script is an example of how to shutdown Derby
# -- running as a server inside the Network Server framework
# --
# -- REQUIREMENTS:
# --     You must have the derby and Network Server jar files in your CLASSPATH
# --
# --  Check the setNetworkServerCP.ksh file for an example of
# --   what to set.
# --
# -- This file for use on Unix ksh systems
# ---------------------------------------------------------
# ---------------------------------------------------------
# -- shutdown Derby Network Server
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
 
# ---------------------------------------------------------
# -- Determine the host and port to use by:
# --  1. Check to see if the host and port are set on the command line
# --  2. Check to see if DERBY_SERVER_HOST and DERBY_SERVER_PORT
# --  3. Default to localhost/1527
# ---------------------------------------------------------

if [  "$1" ]
then
   DERBY_SERVER_HOST=$1
fi

if [ -z "$DERBY_SERVER_HOST" ]
then
   DERBY_SERVER_HOST=localhost
fi

if [  "$2" ]
then
   DERBY_SERVER_PORT=$2
fi

if [ -z "$DERBY_SERVER_PORT" ]
then
   DERBY_SERVER_PORT=1527
fi

"$JAVA_HOME/bin/java" -cp "${DERBY_HOME}/lib/derby.jar:${DERBY_HOME}/lib/derbytools.jar:${DERBY_HOME}/lib/derbynet.jar:${CLASSPATH}" org.apache.derby.drda.NetworkServerControl shutdown -h $DERBY_SERVER_HOST -p $DERBY_SERVER_PORT

# ---------------------------------------------------------
# -- To use a different JVM with a different syntax, simply edit
# -- this file
# ---------------------------------------------------------

