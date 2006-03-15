#!/bin/bash

TESTDIR=java/testing/org/apache/derbyTesting/functionTests/tests/i18n
ROOT=$1

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


#
# Create the top of the file
#
FILE=$ROOT/$TESTDIR/TestClientMessages.java

echo "package org.apache.derbyTesting.functionTests.tests.i18n;" > $FILE
echo "" >> $FILE
echo "import org.apache.derby.client.am.MessageId;" >> $FILE
echo "import org.apache.derby.client.am.SqlException;" >> $FILE
echo "import org.apache.derby.shared.common.reference.SQLState;" >> $FILE
echo "" >> $FILE
echo "/**" >> $FILE
echo " * This class is a GENERATED FILE that tests as many of the messages" >> $FILE
echo " * in the client code as possible." >> $FILE
echo " */" >> $FILE
echo "public class TestClientMessages" >> $FILE
echo "{" >> $FILE
echo "  private static Exception e;" >> $FILE
echo '  private static String feature = "testFeature";' >> $FILE
echo "" >> $FILE
echo "  public static void main(String[] args) {" >> $FILE
echo "    try {" >> $FILE

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
for i in $FILES
do
  echo "    // from source file $i" >> $FILE
  sed -n -f $MYDIR/genClient1.sed $i >> $FILE 
done

#
# Use this sed script to clean things up so the source compiles 
#
sed -f $MYDIR/genClient2.sed $FILE > $FILE.2 

if [ $? != 0 ]
then
  rm -f $FILE
  rm -f $FILE.2
  exit 1
fi

mv $FILE.2 $FILE

echo "    }" >> $FILE
echo "    catch ( Throwable t ) {" >> $FILE
echo "      t.printStackTrace();" >> $FILE
echo "    }" >> $FILE
echo "  }" >> $FILE
echo "}" >> $FILE

exit 0
