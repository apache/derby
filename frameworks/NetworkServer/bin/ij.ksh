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

# CLOUDSCAPE_INSTALL=

[ -z "$CLASSPATH" ] && {
  . "$CLOUDSCAPE_INSTALL"/frameworks/NetworkServer/bin/setNetworkClientCP.ksh
}
 
export IJ_HOST=localhost
export IJ_PORT=1527
export IJ_USER=APP
export IJ_PASSWORD=APP

# ---------------------------------------------------------
# -- start ij
# ---------------------------------------------------------
java -Dij.driver=com.ibm.db2.jcc.DB2Driver -Dij.protocol=jdbc:derby:net://$IJ_HOST:$IJ_PORT/ -Dij.user=$IJ_USER -Dij.password=$IJ_PASSWORD  org.apache.derby.tools.ij

# ---------------------------------------------------------
# -- To use a different JVM with a different syntax, simply edit
# -- this file
# ---------------------------------------------------------
