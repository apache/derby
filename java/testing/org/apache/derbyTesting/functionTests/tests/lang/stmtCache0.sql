
--
-- testing a cache size of 0
--

-- see that it starts out empty; 
select count(*) from new org.apache.derby.diag.StatementCache() as SC;

CREATE FUNCTION ConsistencyChecker() RETURNS VARCHAR(128)
EXTERNAL NAME 'org.apache.derbyTesting.functionTests.util.ConsistencyChecker.runConsistencyChecker'
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
