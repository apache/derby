-- check all tables
select tablename, 
		schemaname, 
		SYSCS_UTIL.SYSCS_CHECK_TABLE(schemaname, tablename)
from sys.sysschemas s, sys.systables t
where s.schemaid = t.schemaid;
disconnect;
