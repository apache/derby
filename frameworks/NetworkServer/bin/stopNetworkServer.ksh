# ---------------------------------------------------------
# -- This simple script is an example of how to shutdown Derby 
# -- running as a server inside the Network Server framework
# --
# -- REQUIREMENTS: 
# --	 You must have the derby jar files as well as the 
# --   derby network server class files in your classpath.
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

java org.apache.derby.drda.NetworkServerControl shutdown
# ---------------------------------------------------------
# -- To use a different JVM with a different syntax, simply edit
# -- this file
# ---------------------------------------------------------
