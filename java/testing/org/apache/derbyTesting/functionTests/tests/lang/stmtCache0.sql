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

--
-- testing a cache size of 0
--

-- see that it starts out empty; 
select count(*) from syscs_diag.statement_cache;

CREATE FUNCTION ConsistencyChecker() RETURNS VARCHAR(128)
EXTERNAL NAME 'org.apache.derbyTesting.functionTests.util.T_ConsistencyChecker.runConsistencyChecker'
LANGUAGE JAVA PARAMETER STYLE JAVA;
autocommit off;

autocommit off;
-- set up aliases
run resource 'stmtCacheAliases.subsql';
commit;

-- see that it starts out empty; 
select count(*) from SC_CONTENTS;

-- see that it stays empty; 
select count(*) from SC_CONTENTS;

-- expect everything to be okay
values consistencyChecker();

commit;

-- clear aliases
run resource 'stmtCacheAliasesRemove.subsql';
commit;
