driver 'com.ibm.db2.jcc.DB2Driver';
--Bug 4632  Make the db italian to make sure string selects  are working
connect 'jdbc:derby:net://localhost:1527/wombat;create=true;territory=it:retrieveMessagesFromServerOnGetMessage=true;' USER 'dbadmin' PASSWORD 'dbadmin';

connect 'jdbc:derby:net://localhost:1527/wombat' USER 'dbadmin' PASSWORD 'dbadbmin';
-- this is a comment, a comment in front of a select should not cause an error
select * from sys.systables where 1=0;
-- this is a comment, a comment in front of a values clauses should not cause an error
values(1);

-- Try some URL attributes
disconnect all;
connect 'jdbc:derby:net://localhost:1527/junk;create=true:retrieveMessagesFromServerOnGetMessage=true;' USER 'dbadmin' PASSWORD 'dbadbmin';
select * from APP.notthere;


-- examples from the docs

connect 'jdbc:derby:net://localhost:1527/wombat;create=true:user=judy;password=judy;retrieveMessagesFromServerOnGetMessage=true;';

connect 'jdbc:derby:net://localhost:1527/"./wombat":user=judy;password=judy;retrieveMessagesFromServerOnGetMessage=true;';

connect 'jdbc:derby:net://localhost:1527/toursDB:retrieveMessagesFromServerOnGetMessage=true;';


connect 'jdbc:derby:net://localhost:1527/toursDB:retrieveMessagesFromServerOnGetMessage=true;' USER 'dbadmin' PASSWORD 'dbadbmin';

connect 'jdbc:derby:net://localhost:1527/wombat' USER 'APP' PASSWORD 'APP';

connect  'jdbc:derby:net://localhost:1527/my-db-name;create=true:user=usr;password=pwd;retrieveMessagesFromServerOnGetMessage=true;';

connect 'jdbc:derby:net://localhost:1527/my-db-name;upgrade=true:user=usr;password=pwd;retrieveMessagesFromServerOnGetMessage=true;';


connect 'jdbc:derby:net://localhost:1527/my-db-name;shutdown=true:user=usr;password=pwd;'

-- Quoted db and attributes
connect 'jdbc:derby:net://localhost:1527/"./my-dbname;create=true":user=usr;password=pwd;';

connect 'jdbc:derby:net://localhost:1527/"./my-dbname;create=true":user=usr;password=pwd;retrieveMessagesFromServerOnGetMessage=true;';



