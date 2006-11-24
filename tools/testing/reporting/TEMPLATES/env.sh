# Place in ${SANDBOX} (Above ${derby_source}, i.e. trunk, 10.2, 10.1 ....)
# Modify to fit your environment
#
export TOPDIR=${HOME}/testingReportingScripts/outerWorld
export TESTSET=testset
export SANDBOX=sandbox
export DERBYDIR=${TOPDIR}/${TESTSET}/${SANDBOX}
export PUBLISHDIR=public_html/public/Apache/outerWorld/${TESTSET}/${SANDBOX}
export ANT_HOME=/usr/local/share/java/apache-ant-1.6.2 
export PATH=${ANT_HOME}/bin:/usr/local/java/jdk/bin/:$PATH
export JAVA_HOME=/usr/local/java/jdk1.4
export BRANCH_DIR=trunk
#                 10.1
#                 10.2
#                 10.3
#                 etc.
export derby_source=${DERBYDIR}/${BRANCH_DIR}
#
export TOOLDIR=${DERBYDIR}/${BRANCH_DIR}/tools/testing/reporting/scripts
export TEMPLATEDIR=${DERBYDIR}/${BRANCH_DIR}/tools/testing/reporting/TEMPLATES
