# Place in ${SANDBOX} (Above ${derby_source}, i.e. trunk, 10.2, 10.1 ....)
# Modify to fit your environment
#
# Assumes . ./env.sh was first done:

DB2JCCDIR=${DERBYDIR}/db2jcc # Might be somewhere else ....

export ROOT=${derby_source}

JARDIR=$ROOT/jars/insane # Change if you use sane
DBTOOLDIR=$ROOT/tools/java
ORO=$DBTOOLDIR/jakarta-oro-2.0.8.jar
JUNIT=$DBTOOLDIR/junit.jar

DERBY=$JARDIR/derby.jar
DERBYTOOLS=$JARDIR/derbytools.jar
DERBYNET=$JARDIR/derbynet.jar
DERBYCLIENT=$JARDIR/derbyclient.jar
DB2JCC=$DB2JCCDIR/lib/db2jcc.jar:$DB2JCCDIR/lib/db2jcc_license_c.jar
DERBYTESTING=$JARDIR/derbyTesting.jar
DERBYRUN=$JARDIR/derbyrun.jar

LOCALES=$JARDIR/derbyLocale_de_DE.jar:\
$JARDIR/derbyLocale_es.jar:\
$JARDIR/derbyLocale_fr.jar:\
$JARDIR/derbyLocale_it.jar:\
$JARDIR/derbyLocale_ja_JP.jar:\
$JARDIR/derbyLocale_ko_KR.jar:\
$JARDIR/derbyLocale_pt_BR.jar:\
$JARDIR/derbyLocale_zh_CN.jar:\
$JARDIR/derbyLocale_zh_TW.jar
 
export CLASSPATH=$DERBY:$DERBYCLIENT:$DERBYTOOLS:$DERBYNET:$DB2JCC:$DERBYTESTING:$DERBYRUN:$ORO:$JUNIT:$LOCALES

# At least needed on SunOs/x86:
export LC_CTYPE=en_US

# Where the Derby tests are executed:
export TESTEXECUTIONDIR="/export/home/tmp/${USER}/derby_exec_${TESTSET}_${SANDBOX_}${BRANCH_DIR}"
