
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
# -- This script file is an example of how to use sysinfo to get
# -- important system information
# --
# -- REQUIREMENTS:
# --
# --  This utility will report important system info about 
# --  jar files which are in your classpath and the current setting of
# --  Derby Network Server parameters. Jar files which are not
# --  if your classpath will not be reported. 
# --  The Derby Network Server must be running for this utility to work.
# --
# -- Check the setNetworkServerCP.ksh to see an example of adding the
# -- the Derby jars to your classpath.
# -- 
# --
# -- This file for use on Unix korn shell systems
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
# -- start sysinfo
# ---------------------------------------------------------
"$JAVA_HOME/bin/java" -cp "${DERBY_HOME}/lib/derby.jar:${DERBY_HOME}/lib/derbytools.jar:${DERBY_HOME}/lib/derbynet.jar:${CLASSPATH}" org.apache.derby.drda.NetworkServerControl sysinfo $*
# ---------------------------------------------------------
# -- To use a different JVM with a different syntax, simply edit
# -- this file
# ---------------------------------------------------------
