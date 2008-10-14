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

# For those expressions that start with "new <something>Exception", pre-append
# a try block
/new[[:space:]]*[a-zA-Z]*Exception/i\
    try {

# For those expressions that start with "new ClientMessageId", pre-append
# a try block and create a SqlException to complete the expression
/^[[:space:]]*new ClientMessageId.*/i\
    try { \
       testException = new SqlException(null, 

# At the end of each statement, check and print out an error
# if there is a problem
/;/a\
    } catch ( Throwable t ) { \
      // We can get this on an assertion failure \
      t.printStackTrace(); \
    } \
    if ( testException.getMessage().startsWith("UNKNOWN") )  {\
      testException.printStackTrace(); \
      System.err.println("FAILURE: message id was not found"); \
    }

#
# Calling super is the same as new SqlException for subclasses...
#
/super(/i\
      try { \
          testException = new SqlException(
s/super(//

#
# Add substitution for various message parameters where you replace
# the variable with a string containing the variable.  This prevents
# compile errors saying "symbol not found"
#
s/[([:space:]]logWriter,/null,/g
s/[([:space:]]logWriter_,/null,/g
s/fileName/"fileName"/g
s/[[:space:]]e.getMessage()/"e.getMessage"/g
s/Configuration.packageNameForDNC/"Configuration.packageNameForDNC"/g
s/Configuration.dncDriverName/"Configuration.dncDriverName"/g
s/[^\.]packageNameForDNC/"packageNameForDNC"/g
s/source,/"source",/g
s/Types.getTypeString([^)]*)/"targetType"/g
s/encoding/"encoding"/g
s/source.getClass().getName()/"source"/g
s/e.getException()/new Exception("foo")/g
s/[[:space:]]e);/ testException);/g
s/[[:space:]]ae/ testException/g
s/{ae/{testException/g
s/[[:space:]]e.getClass().getName()/ "exceptionClassName"/g
s/{e.getClass().getName()/{"exceptionClassName"/g
s/[[:space:]]sourceType/ "sourceType"/g
s/[[:space:]]targetType/ "targetType"/g
s/[[:space:]]columnName)/ "columnName")/g
s/[[:space:]]charsetName/ "charsetName"/g
s/[[:space:]]cursorName/ "cursorName"/g
s/[[:space:]]cursorName/ "cursorName"/g
s/[[:space:]]methodName/ "methodName"/g
s/[[:space:]]interfaces/ "interfaces"/g
s/[[:space:]]method/ "method"/g
s/[[:space:]]instance/ "instance"/g
s/[[:space:]]method)/ "method")/g
s/[[:space:]]b.toString()/ "bytes"/g
s/[[:space:]]jdbcInterfaceName/ "jdbcInterfaceName"/g
s/[[:space:]]jdbcStatementInterfaceName/ "jdbcInterfaceName"/g
s/[[:space:]]name)/ "name")/g
s/[[:space:]]sql)/ "sql")/g
s/cause.getMessage()/testException.getMessage()/g
s/cause));/testException);/g
s/netConnection.databaseName_));/"netcondbname");/g
s/value));/"value");/g
s/GGRP"));/GGRP");/g
s/[[:space:]]e));/ testException);/g
s/[[:space:]]server/ "server"/
s/codpnt/"codpnt"/g
s/[[:space:]]codePoint/ "codePoint"/g
s/conversationProtocolErrorCode/"conerr"/g
s/[[:space:]]rdbnam/ "rdbnam"/g
s/[[:space:]]manager/ "manager"/g
s/[[:space:]]level));/ "level");/g
s/[[:space:]]operation/ "operation"/g
s/[[:space:]]attributeString/ "attributeString"/g
s/[[:space:]]attribute/ "attributeString"/g
s/[[:space:]]value/ "string"/g
s/[[:space:]]choicesStr/ "string"/g
s/[[:space:]]url/ "string"/g
s/[[:space:]]fp\.getFirstKey()/ "string"/g
s/[[:space:]]fp\.getFirstValue()/ "string"/g
s/,uee/,testException/g
s/[[:space:]]platform/ "string"/g
s/parsePKGNAMCT"))/parsePKGNAMCT")/
s/[[:space:]]identifier/ "string"/g
s/[[:space:]]arg2))/ "arg2")/g
s/[[:space:]]arg1/ "string"/g
s/[[:space:]]exceptionsOnXA/ testException/g
s/[[:space:]]getXAFuncStr(.*)/ "string"/g
s/[[:space:]]getXAExceptionText(.*)/ "string"/g
s/agent_.logWriter_,/ null,/g
s/NO_CURRENT_CONNECTION)),/NO_CURRENT_CONNECTION),/g
s/parseSQLDIAGSTT"))/parseSQLDIAGSTT")/g
s/parseSQLDIAGCN"))/parseSQLDIAGCN")/g
s/parseSQLDCTOKS"))/parseSQLDCTOKS")/g
s/parseSQLDCXGRP"))/parseSQLDCXGRP")/g
s/Integer.toString(port)/"port"/g




# 
# Deal with extra updateCounts argument to BatchUpdateExceptoins.
# This is a bit of a hack, but turn them into a Throwable.  Otherwise
# the SqlException code thinks its another Object argument to the
# message and we get a runtime assertion error
s/updateCounts/(Throwable)null/g


#
# Subsitute Long and Integer params with 0 as a default
#
s/new Long[[:space:]]*([^)]*/new Long(0/g
s/new Integer[[:space:]]*([^)]*/new Integer(0/g
s/Integer.toHexString[[:space:]]*([^)]*/Integer.toHexString(0/g

# Get rid of logWriter
#s/new SqlException[[:space:]]*(.*,/new SqlException(null,/g

# Don't throw, just assign
s/throw new/testException = new/g

# There are some odd situations where there is one too many parens
s/)))/))/g


