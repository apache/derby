#!/bin/bash

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


TESTDIR=java/testing/org/apache/derbyTesting/functionTests/tests/i18n
ROOT=$1

GSED=`which gsed`
if [ "$GSED" = "" ]
then
  SED=sed
else
  SED=$GSED
fi

syntax()
{
  echo "syntax: $0 derby_rootdir"
}

if [ "$1" = "" ]
then
  echo "Please supply a root Derby directory"
  syntax
  exit 1
fi

if [ ! -d $ROOT ]
then
  echo "$ROOT is not found or is not a directory"
  syntax
  exit 1
fi

if [ ! -d $ROOT/$TESTDIR ]
then
  echo "$ROOT does not appear to be a valid Derby root directory:"
  echo "  $ROOT/$TESTDIR is not found or is not a directory"
  syntax
  exit 1
fi


FILE=$ROOT/$TESTDIR/TestClientMessages.java

rm -f $FILE $FILE.2


CLIENTROOT=$ROOT/java/client/org/apache/derby
if [ ! -d $CLIENTROOT ]
then
  echo $CLIENTROOT is not a valid directory
  exit 1
fi

FILES=`find $CLIENTROOT -name '*.java' -print`

if [ $? != 0 ]
then 
  exit $?
fi

MYDIR=$ROOT/tools/testing/i18nTestGenerator

#
# Extract all uses of the new SqlException and put it in our
# source file.  We'll then compile this file, run it, and make
# sure we have valid uses of message ids
#
touch $FILE
for i in $FILES
do
  echo "    // from source file $i" >> $FILE
  $SED -n -f $MYDIR/genClient1.sed $i | $SED -f $MYDIR/genClient2.sed >> $FILE 
  #sed -n -f $MYDIR/genClient1.sed $i >> $FILE
done

#
#
# Add the beginning lines of the class to the file
#
cat $MYDIR/clientPrologue.txt $FILE > $FILE.2
mv $FILE.2 $FILE

#
# Add the trailing lines of the class to the file
#
echo "  }" >> $FILE
echo "}" >> $FILE

exit 0
