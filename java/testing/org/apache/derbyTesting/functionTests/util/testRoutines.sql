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
-- Changed to create individual procedures so that this will work with JSR169. 
-- Direct call to 'installRoutines' uses nested connection
CREATE PROCEDURE TESTROUTINE.SET_SYSTEM_PROPERTY(IN PROPERTY_KEY VARCHAR(32000), IN PROPERTY_VALUE VARCHAR(32000)) NO SQL EXTERNAL NAME 'org.apache.derbyTesting.functionTests.util.TestRoutines.setSystemProperty' language java parameter style java;
CREATE PROCEDURE TESTROUTINE.SLEEP(IN SLEEP_TIME_MS BIGINT) NO SQL EXTERNAL NAME 'org.apache.derbyTesting.functionTests.util.TestRoutines.sleep' language java parameter style java;
CREATE FUNCTION TESTROUTINE.HAS_SECURITY_MANAGER() RETURNS INT NO SQL EXTERNAL NAME 'org.apache.derbyTesting.functionTests.util.TestRoutines.hasSecurityManager' language java parameter style java;
CREATE FUNCTION TESTROUTINE.READ_FILE(FILE_NAME VARCHAR(60), ENCODING VARCHAR(60)) RETURNS VARCHAR(32000) NO SQL EXTERNAL NAME 'org.apache.derbyTesting.functionTests.util.TestRoutines.readFile' language java parameter style java;
