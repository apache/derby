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

[ -z "$CLASSPATH" ] && {
  . "$DERBY_HOME"/frameworks/NetworkServer/bin/setNetworkClientCP.ksh
}
 
export IJ_HOST=localhost
export IJ_PORT=1527
export IJ_USER=APP
export IJ_PASSWORD=APP

# ---------------------------------------------------------
# -- start ij
# ---------------------------------------------------------
"$JAVA_HOME/bin/java" -Dij.driver=org.apache.derby.jdbc.ClientDriver -Dij.protocol=jdbc:derby://$IJ_HOST:$IJ_PORT/ -Dij.user=$IJ_USER -Dij.password=$IJ_PASSWORD  org.apache.derby.tools.ij

# ---------------------------------------------------------
# -- To use a different JVM with a different syntax, simply edit
# -- this file
# ---------------------------------------------------------
