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

run resource 'rlliso1multi.subsql';

------------------------------------------------------------------------------
-- TEST SERIALIZABLE INTERACTION:
------------------------------------------------------------------------------
connect 'wombat' as writer;
autocommit off;
set isolation RR;
commit;

run resource 'rlliso1multi.subsql';

------------------------------------------------------------------------------
-- TEST REPEATABLE READ INTERACTION:
------------------------------------------------------------------------------
connect 'wombat' as writer;
autocommit off;
set current isolation = RS;
commit;

run resource 'rlliso1multi.subsql';

------------------------------------------------------------------------------
-- TEST READ UNCOMMITTED INTERACTION:
------------------------------------------------------------------------------

connect 'wombat' as writer;
autocommit off;
set isolation UR;
commit;

run resource 'rlliso1multi.subsql';
