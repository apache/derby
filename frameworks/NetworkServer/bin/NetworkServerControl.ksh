## ---------------------------------------------------------
# -- This simple script is an example of how to run commands
# -- for the Network Server framework
# --
# -- REQUIREMENTS: 
# --	  You must have the derby and Network Server jar files in your CLASSPATH
# --
# --  Check the setNetworkServerCP.ksh file for an example of
# --   what to set.
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

[ -z "$CLASSPATH" ] && {
  . "$DERBY_HOME"/frameworks/NetworkServer/bin/setNetworkServerCP.ksh
}

# ---------------------------------------------------------
# -- start Derby Network Server
# ---------------------------------------------------------

"$JAVA_HOME/bin/java" org.apache.derby.drda.NetworkServerControl $*


# ---------------------------------------------------------
# -- To use a different JVM with a different syntax, simply edit
# -- this file
# ---------------------------------------------------------

