# ---------------------------------------------------------
# -- This script file sets the CLASSPATH environment variable
# -- for use with Derby products in NetworkServer mode
# --
# -- To use this script from other locations, change the 
# -- value assigned to CLOUDSCAPE_INSTALL to be an absolute path 
# -- (export CLOUDSCAPE_INSTALL=/opt/derby) instead of the current relative path
# --
# -- This file for use on Unix ksh systems
# -- 
# ---------------------------------------------------------
# CLOUDSCAPE_INSTALL=

export CLASSPATH="${CLOUDSCAPE_INSTALL}/lib/derby.jar:${CLOUDSCAPE_INSTALL}/lib/derbytools.jar:${CLOUDSCAPE_INSTALL}/lib/derbynet.jar:${CLASSPATH}"
