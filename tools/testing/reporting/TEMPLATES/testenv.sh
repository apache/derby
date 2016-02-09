# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to you under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Place in ${SANDBOX} (Above ${derby_source}, i.e. trunk, 10.2, 10.1 ....)
# Modify to fit your environment
#
# Assumes . ./env.sh was first done:

DB2JCCDIR=${DERBYDIR}/db2jcc # Might be somewhere else ....

export ROOT=${derby_source}

JARDIR=$ROOT/jars/insane # Change if you use sane
DBTOOLDIR=$ROOT/tools/java
JUNIT=$DBTOOLDIR/junit.jar

DERBY=$JARDIR/derby.jar
DERBYTOOLS=$JARDIR/derbytools.jar
DERBYNET=$JARDIR/derbynet.jar
DERBYCLIENT=$JARDIR/derbyclient.jar
DB2JCC=$DB2JCCDIR/lib/db2jcc.jar:$DB2JCCDIR/lib/db2jcc_license_c.jar
DERBYTESTING=$JARDIR/derbyTesting.jar
DERBYRUN=$JARDIR/derbyrun.jar

LOCALES=$JARDIR/derbyLocale_cs.jar:\
$JARDIR/derbyLocale_de_DE.jar:\
$JARDIR/derbyLocale_es.jar:\
$JARDIR/derbyLocale_fr.jar:\
$JARDIR/derbyLocale_hu.jar:\
$JARDIR/derbyLocale_it.jar:\
$JARDIR/derbyLocale_ja_JP.jar:\
$JARDIR/derbyLocale_pl.jar:\
$JARDIR/derbyLocale_ko_KR.jar:\
$JARDIR/derbyLocale_ru.jar:\
$JARDIR/derbyLocale_pt_BR.jar:\
$JARDIR/derbyLocale_zh_CN.jar:\
$JARDIR/derbyLocale_zh_TW.jar
 
export CLASSPATH=$DERBY:$DERBYCLIENT:$DERBYTOOLS:$DERBYNET:$DB2JCC:$DERBYTESTING:$DERBYRUN:$JUNIT:$LOCALES

# At least needed on SunOs/x86:
export LC_CTYPE=en_US

# Where the Derby tests are executed:
export TESTEXECUTIONDIR="/export/home/tmp/${USER}/derby_exec_${TESTSET}_${SANDBOX_}${BRANCH_DIR}"
