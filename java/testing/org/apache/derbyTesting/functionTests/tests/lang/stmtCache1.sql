
--
-- testing a cache size of 1
-- a little bit self-defeating since you can never remove
-- the emptyCache statement from the cache and the
-- statement to test what is in removes the previous one...
--

autocommit off;
-- set up aliases
CREATE FUNCTION ConsistencyChecker() RETURNS VARCHAR(128)
EXTERNAL NAME 'org.apache.derbyTesting.functionTests.util.ConsistencyChecker.runConsistencyChecker'
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
