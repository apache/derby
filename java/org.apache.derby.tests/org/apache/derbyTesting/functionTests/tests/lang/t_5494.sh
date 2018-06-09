#! /bin/bash
#
#   Licensed to the Apache Software Foundation (ASF) under one or more
#   contributor license agreements.  See the NOTICE file distributed with
#   this work for additional information regarding copyright ownership.
#   The ASF licenses this file to you under the Apache License, Version 2.0
#   (the "License"); you may not use this file except in compliance with
#   the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
#
# This is a standalone test to hand-verify that DERBY-5494 has been fixed.
# This test requires that the VM should crash gracelessly. For that reason,
# it is hard to wire this test into our existing test frameworks.
#
#
# $1  location of derby library

export derbyLib=$1
export script1=t_5494_1.sql
export script2=t_5494_2.sql

echo --------------------
echo Test for DERBY-5495
echo This script runs two sql batches.
echo The bug causes both next value for invocations to return the same value.
echo The bug is fixed if the invocations return successive values.
echo --------------------

echo connect \'jdbc:derby:db\;create=true\'\;  > $script1
echo create procedure systemExit\( in exitCode int \) language java parameter style java no sql external name \'java.lang.System.exit\'\;  >> $script1
echo create sequence s\;  >> $script1
echo values next value for s\;  >> $script1
echo call systemExit\( 1 \)\;  >> $script1

echo connect \'jdbc:derby:db\'\;  > $script2
echo values next value for s\;  >> $script2

rm -rf db

java -jar $derbyLib/derbyrun.jar ij $script1
java -jar $derbyLib/derbyrun.jar ij $script2













