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

# CLOUDSCAPE_INSTALL=

[ -z "$CLASSPATH" ] && {
  . "$CLOUDSCAPE_INSTALL"/frameworks/NetworkServer/bin/setNetworkServerCP.ksh
}

# ---------------------------------------------------------
# -- start Derby Network Server
# ---------------------------------------------------------

java org.apache.derby.drda.NetworkServerControl $*


# ---------------------------------------------------------
# -- To use a different JVM with a different syntax, simply edit
# -- this file
# ---------------------------------------------------------

