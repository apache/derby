## ---------------------------------------------------------
# -- This simple script is an example of how to start Derby 
# -- as a server inside the Network Server framework
# --
# -- REQUIREMENTS: 
# --	  You must have the derby and Network Server jar files in your CLASSPATH
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

[ -z "$CLASSPATH" ] && {
  . "$DERBY_INSTALL"/frameworks/NetworkServer/bin/setNetworkServerCP.ksh
}

java org.apache.derby.drda.NetworkServerControl start
# ---------------------------------------------------------
# -- To use a different JVM with a different syntax, simply edit
# -- this file
# ---------------------------------------------------------

