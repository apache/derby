# ---------------------------------------------------------
# -- This script file is an example of how to use sysinfo to get
# -- important system information
# --
# -- REQUIREMENTS:
# --
# --  This utility will report important system info about 
# --  jar files which are in your classpath. Jar files which are not
# --  if your classpath will not be reported. 
# --
# -- Check the setCP.ksh to see an example of adding the
# -- the Cloudscape jars to your classpath.
# -- 
# --
# -- This file for use on Unix korn shell systems
# ---------------------------------------------------------

# CLOUDSCAPE_INSTALL=

[ -z "$CLASSPATH" ] && {
  . "$CLOUDSCAPE_INSTALL"/frameworks/embedded/bin/setEmbeddedCP.ksh
}
 
# ---------------------------------------------------------
# -- start sysinfo
# ---------------------------------------------------------
java org.apache.derby.tools.sysinfo

# ---------------------------------------------------------
# -- To use a different JVM with a different syntax, simply edit
# -- this file
# ---------------------------------------------------------


