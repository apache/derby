# ---------------------------------------------------------
# -- This simple script is an example of how to start ij in 
# -- and embedded environment.
# --
#-- REQUIREMENTS: 
# -- You must have the Cloudscape libraries in your classpath
# -- 
# -- See the setEmbeddedCP.ksh for an example of
# -- how to do this.
# --
# -- This file for use on Unix ksh systems
# ---------------------------------------------------------

# CLOUDSCAPE_INSTALL=

[ -z "$CLASSPATH" ] && {
  . "$CLOUDSCAPE_INSTALL"/frameworks/embedded/bin/setEmbeddedCP.ksh
}
 
# ---------------------------------------------------------
# -- start ij
# ---------------------------------------------------------
java -Dij.protocol=jdbc:derby: org.apache.derby.tools.ij

# ---------------------------------------------------------
# -- To use a different JVM with a different syntax, simply edit
# -- this file
# ---------------------------------------------------------

