driver 'org.apache.derby.jdbc.ClientDriver';
--Bug 4632  Make the db italian to make sure string selects  are working
connect 'jdbc:derby://localhost:1527/wombat;create=true;territory=it' USER 'dbadmin' PASSWORD 'dbadmin';

connect 'jdbc:derby://localhost:1527/wombat' USER 'dbadmin' PASSWORD 'dbadbmin';
-- this is a comment, a comment in front of a select should not cause an error
select * from sys.systables where 1=0;
-- this is a comment, a comment in front of a values clauses should not cause an error
values(1);

-- Try some URL attributes
disconnect all;
connect 'jdbc:derby://localhost:1527/junk;create=true' USER 'dbadmin' PASSWORD 'dbadbmin';
select * from APP.notthere;


-- examples from the docs

connect 'jdbc:derby://localhost:1527/wombat;create=true;user=judy;password=judy';

connect 'jdbc:derby://localhost:1527/./wombat;user=judy;password=judy';

connect 'jdbc:derby://localhost:1527/toursDB';


connect 'jdbc:derby://localhost:1527/toursDB' USER 'dbadmin' PASSWORD 'dbadbmin';

connect 'jdbc:derby://localhost:1527/wombat' USER 'APP' PASSWORD 'APP';

connect  'jdbc:derby://localhost:1527/my-db-name;create=true;user=usr;password=pwd';

connect 'jdbc:derby://localhost:1527/my-db-name;upgrade=true;user=usr;password=pwd';

connect 'jdbc:derby://localhost:1527/my-db-name;shutdown=true;user=usr;password=pwd';

connect 'jdbc:derby://localhost:1527/./my-dbname;create=true;user=usr;password=pwd';

connect 'jdbc:derby://localhost:1527/./my-dbname;create=true;user=usr;password=pwd';

connect 'jdbc:derby://localhost:1527/toursDB' USER 'dbadmin' PASSWORD 'dbadbmin';

connect 'jdbc:derby://localhost:1527/wombat' USER 'APP' PASSWORD 'APP';

connect  'jdbc:derby://localhost:1527/my-db-name;create=true;user=usr;password=pwd';

connect 'jdbc:derby://localhost:1527/my-db-name;upgrade=true;user=usr;password=pwd';

connect 'jdbc:derby://localhost:1527/my-db-name;shutdown=true;user=usr;password=pwd';

-- Database names with /'s
connect 'jdbc:derby://localhost:1527/./my-dbname;create=true;user=usr;password=pwd';

connect 'jdbc:derby://localhost:1527/./my-dbname;create=true;user=usr;password=pwd';



-- retrieveMessageText Testing
connect 'jdbc:derby://localhost:1527/my-db-name;create=true;user=usr;password=pwd;retrieveMessageText=false';

-- Should not get message text
select * from APP.notthere;

connect 'jdbc:derby://localhost:1527/my-db-name;create=true;user=usr;password=pwd;retrieveMessageText=true';

-- Should see message text
select * from APP.notthere;

-- just user security mechanism
connect 'jdbc:derby://localhost:1527/my-db-name;create=true;user=usr;retrieveMessageText=true';

connect 'jdbc:derby://localhost:1527/wombat' USER 'APP';
