## ---------------------------------------------------------
# -- This simple script is an example of how to start Derby
# -- as a server inside the Network Server framework
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
# -- start Derby Network Server
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

"$JAVA_HOME/bin/java" org.apache.derby.drda.NetworkServerControl start -h $DERBY_SERVER_HOST -p $DERBY_SERVER_PORT

# ---------------------------------------------------------
# -- To use a different JVM with a different syntax, simply edit
# -- this file
# ---------------------------------------------------------

