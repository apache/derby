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
-- testing a cache size of 1
-- a little bit self-defeating since you can never remove
-- the emptyCache statement from the cache and the
-- statement to test what is in removes the previous one...
--

autocommit off;
-- set up aliases
CREATE FUNCTION ConsistencyChecker() RETURNS VARCHAR(128)
EXTERNAL NAME 'org.apache.derbyTesting.functionTests.util.T_ConsistencyChecker.runConsistencyChecker'
LANGUAGE JAVA PARAMETER STYLE JAVA;
CREATE PROCEDURE EC()
EXTERNAL NAME 'org.apache.derby.diag.StatementCache.emptyCache'
LANGUAGE JAVA PARAMETER STYLE JAVA;
autocommit off;
run resource 'stmtCacheAliases.subsql';
commit;

-- see that it starts out almost empty; well, just us...
select count(*) from SC_CONTENTS;

-- see if we can detect ourselves in the cache;
-- again, because the size is one, this is the
-- only statement we can look for...
select SQL_TEXT from SC_CONTENTS;

prepare p1 as 'values 1';
select SQL_TEXT from SC_CONTENTS;

-- kick 'em all out and then test the size
call EC();
select count(*) from SC_CONTENTS;

execute p1;
select SQL_TEXT from SC_CONTENTS;

remove p1;

call EC();
select count(*) from SC_CONTENTS;

-- expect everything to be okay
-- is a dependency on EMPTYCACHE
values ConsistencyChecker();

commit;

-- clean up aliases
run resource 'stmtCacheAliasesRemove.subsql';
commit;
