--
--
-- URL locale handling
--

create procedure checkDatabaseLoc(in locale char(12)) parameter style java language java external name 'org.apache.derbyTesting.functionTests.tests.i18n.DefaultLocale.checkDatabaseLocale';
create procedure checkRDefaultLoc() parameter style java language java external name 'org.apache.derbyTesting.functionTests.tests.i18n.DefaultLocale.checkRDefaultLocale';
-- this current database was created with the default locale
call checkDatabaseLoc('en_US');
call checkRDefaultLoc();

disconnect;

-- create a Swiss database
connect 'jdbc:derby:swissdb;create=true;territory=fr_CH';
create procedure checkDatabaseLoc(in locale char(12)) parameter style java language java external name 'org.apache.derbyTesting.functionTests.tests.i18n.DefaultLocale.checkDatabaseLocale';
call checkDatabaseLoc('fr_CH');
disconnect;
connect 'jdbc:derby:swissdb;shutdown=true';

-- check it is still Swiss when we re-boot
connect 'jdbc:derby:swissdb';
call checkDatabaseLoc('fr_CH');
disconnect;
connect 'jdbc:derby:swissdb;shutdown=true';


-- Locale automatically converts the components to the correct case
-- create a Hindi in India database (hi_IN)
connect 'jdbc:derby:hindi;create=true;territory=HI_in';
create procedure checkDatabaseLoc(in locale char(12)) parameter style java language java external name 'org.apache.derbyTesting.functionTests.tests.i18n.DefaultLocale.checkDatabaseLocale';
call checkDatabaseLoc('hi_IN');
disconnect;
connect 'jdbc:derby:hindi;shutdown=true';


-- now try one with a variant
-- create a English in Israel database for JavaOS en_IL_JavaOS
connect 'jdbc:derby:Israel;create=true;territory=en_IL_JavaOS';
create procedure checkDatabaseLoc(in locale char(12)) parameter style java language java external name 'org.apache.derbyTesting.functionTests.tests.i18n.DefaultLocale.checkDatabaseLocale';
call checkDatabaseLoc('en_IL_JavaOS');
disconnect;
connect 'jdbc:derby:Israel;shutdown=true';

-- now try with just a language - we support this
-- as some vms do.
connect 'jdbc:derby:bacon;create=true;territory=da';
create procedure checkDatabaseLoc(in locale char(12)) parameter style java language java external name 'org.apache.derbyTesting.functionTests.tests.i18n.DefaultLocale.checkDatabaseLocale';
call checkDatabaseLoc('da');
disconnect;
connect 'jdbc:derby:bacon;shutdown=true';
connect 'jdbc:derby:bacon';
call checkDatabaseLoc('da');
disconnect;
connect 'jdbc:derby:bacon;shutdown=true';


--
-- some negative tests
--
connect 'jdbc:derby:fail1;create=true;territory=';
-- database will not have been created so this connection will fail
connect 'jdbc:derby:fail1;shutdown=true';

connect 'jdbc:derby:fail3;create=true;territory=en_';
connect 'jdbc:derby:fail4;create=true;territory=en_d';
connect 'jdbc:derby:fail5;create=true;territory=en_US_';
connect 'jdbc:derby:fail6;create=true;territory=en-US';

-- try using a database name with trailing blanks
-- beetle 4653
connect 'jdbc:derby:trailblank;create=true';
connect 'jdbc:derby:trailblank';
connect 'jdbc:derby:trailblank     ';
connect 'jdbc:derby:trailblank      ;shutdown=true';


connect 'jdbc:derby:;shutdown=true';
