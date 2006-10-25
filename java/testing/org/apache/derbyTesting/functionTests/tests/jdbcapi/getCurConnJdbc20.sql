--
--   Licensed to the Apache Software Foundation (ASF) under one or more
--   contributor license agreements.  See the NOTICE file distributed with
--   this work for additional information regarding copyright ownership.
--   The ASF licenses this file to You under the Apache License, Version 2.0
--   (the "License"); you may not use this file except in compliance with
--   the License.  You may obtain a copy of the License at
--
--      http://www.apache.org/licenses/LICENSE-2.0
--
--   Unless required by applicable law or agreed to in writing, software
--   distributed under the License is distributed on an "AS IS" BASIS,
--   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
--   See the License for the specific language governing permissions and
--   limitations under the License.
--
-- test getCurConnJdbc20
-- this test will get run under jdk12 only. If run under jdk11x, will get an exception like
-- following for call to newToJdbc20Method
-- ERROR 38000: The exception 'java.lang.NoSuchMethodError: java.sql.Connection: method 
-- createStatement(II)Ljava/sql/Statement; not found' was thrown while evaluating an expression.


-- method alias and table used later
create procedure newToJdbc20Method() PARAMETER STYLE JAVA LANGUAGE JAVA EXTERNAL NAME 'org.apache.derbyTesting.functionTests.util.Jdbc20Test.newToJdbc20Method';
create table T (a int NOT NULL primary key);
insert into T values (1);

-- now lets try a variety of errors
call newToJdbc20Method();

------------------------------------------------------------
-- drop the table
drop table T;

drop procedure newToJdbc20Method;
