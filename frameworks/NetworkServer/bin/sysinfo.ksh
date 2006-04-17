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
 
[ -z "$CLASSPATH" ] && {
  . "$DERBY_HOME"/frameworks/NetworkServer/bin/setNetworkServerCP.ksh
}

# ---------------------------------------------------------
# -- start sysinfo
# ---------------------------------------------------------
"$JAVA_HOME/bin/java" org.apache.derby.drda.NetworkServerControl sysinfo $*
# ---------------------------------------------------------
# -- To use a different JVM with a different syntax, simply edit
# -- this file
# ---------------------------------------------------------
