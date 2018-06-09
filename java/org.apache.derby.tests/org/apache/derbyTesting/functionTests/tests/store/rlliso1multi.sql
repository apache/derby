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
------------------------------------------------------------------------------
-- 2 CONCURRENT USER TESTS of READ UNCOMMITTED TEST CASES.
--
--    See rlliso1multi.subsql for description of individual test cases.  That
--    test is run 4 times each with the second user running in a different 
--    isolation level.
------------------------------------------------------------------------------

------------------------------------------------------------------------------
-- TEST READ COMMITTED INTERACTION:
------------------------------------------------------------------------------

connect 'wombat' as writer;
autocommit off;
set isolation CS;
commit;

run resource '/org/apache/derbyTesting/functionTests/tests/store/rlliso1multi.subsql';

------------------------------------------------------------------------------
-- TEST SERIALIZABLE INTERACTION:
------------------------------------------------------------------------------
connect 'wombat' as writer;
autocommit off;
set isolation RR;
commit;

run resource '/org/apache/derbyTesting/functionTests/tests/store/rlliso1multi.subsql';

------------------------------------------------------------------------------
-- TEST REPEATABLE READ INTERACTION:
------------------------------------------------------------------------------
connect 'wombat' as writer;
autocommit off;
set current isolation = RS;
commit;

run resource '/org/apache/derbyTesting/functionTests/tests/store/rlliso1multi.subsql';

------------------------------------------------------------------------------
-- TEST READ UNCOMMITTED INTERACTION:
------------------------------------------------------------------------------

connect 'wombat' as writer;
autocommit off;
set isolation UR;
commit;

run resource '/org/apache/derbyTesting/functionTests/tests/store/rlliso1multi.subsql';
