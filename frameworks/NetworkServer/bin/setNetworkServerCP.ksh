# ---------------------------------------------------------
# -- This script file sets the CLASSPATH environment variable
# -- for use with Derby products in NetworkServer mode
# --
# -- To use this script from other locations, change the 
# -- value assigned to DERBY_INSTALL to be an absolute path 
# -- (export DERBY_INSTALL=/opt/derby) instead of the current relative path
# --
# -- This file for use on Unix ksh systems
# -- 
# ---------------------------------------------------------
# DERBY_INSTALL=

DERBY_HOME=${DERBY_HOME:-$DERBY_INSTALL}

[ -z "$DERBY_HOME" ] && {
  echo "\$DERBY_HOME or \$DERBY_INSTALL not set. Please set one of these variables"
  echo "to the location of your Derby installation."
  exit 1
}

export CLASSPATH="${DERBY_HOME}/lib/derby.jar:${DERBY_HOME}/lib/derbytools.jar:${DERBY_HOME}/lib/derbynet.jar:${CLASSPATH}"
