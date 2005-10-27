
#
#
# runphases old_major old_minor old_engine new_engine
#
# e.g.
#
# runphases 10 0 c:/derby/10.0.2.1/lib c:/derby/trunk/jars/sane

omajor=$1
ominor=$2
ocs=$3
ncs=$4

jvm=java

# classpaths

ocp="${ocs}/derby.jar;${ncs}/derbyTesting.jar"
ncp="${ncs}/derby.jar;${ncs}/derbyTesting.jar"

# database name
dbdir=updtest_${omajor}_${ominor}
db=${dbdir}/su_${omajor}_${ominor}
tempdb=${dbdir}/tdb
rm -fr ${dbdir}

set -x

# Phase 0 - create the old database
$jvm -classpath $ocp org.apache.derbyTesting.upgradeTests.phaseTester ${db} 0 ${omajor} ${ominor} ${tempdb}

# Phase 1 - soft upgrade mode with new version
$jvm -Dderby.database.allowPreReleaseUpgrade=true -classpath $ncp org.apache.derbyTesting.upgradeTests.phaseTester ${db} 1 ${omajor} ${ominor} ${tempdb}

# Phase 2 - post soft upgrade mode with old version
$jvm -classpath $ocp org.apache.derbyTesting.upgradeTests.phaseTester ${db} 2 ${omajor} ${ominor} ${tempdb}

# Phase 3 - hard upgrade mode with new version
$jvm -Dderby.database.allowPreReleaseUpgrade=true -classpath $ncp org.apache.derbyTesting.upgradeTests.phaseTester ${db} 3 ${omajor} ${ominor} ${tempdb}

# Phase 4 - post hard upgrade mode with old version 
$jvm -classpath $ocp org.apache.derbyTesting.upgradeTests.phaseTester ${db} 4 ${omajor} ${ominor} ${tempdb}

