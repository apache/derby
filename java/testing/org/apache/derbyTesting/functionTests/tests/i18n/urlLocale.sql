--
--
-- URL locale handling
--

create procedure checkDatabaseLoc(in locale char(12)) parameter style java language java external name 'org.apache.derbyTesting.functionTests.tests.i18n.DefaultLocale.checkDatabaseLocale';
create procedure checkRDefaultLoc() parameter style java language java external name 'org.apache.derbyTesting.functionTests.tests.i18n.DefaultLocale.checkRDefaultLocale';
-- this current database was created with the default locale
call checkRDefaultLoc();

disconnect;

-- create a Swiss database
connect 'swissdb;create=true;territory=fr_CH';
create procedure checkDatabaseLoc(in locale char(12)) parameter style java language java external name 'org.apache.derbyTesting.functionTests.tests.i18n.DefaultLocale.checkDatabaseLocale';
call checkDatabaseLoc('fr_CH');
disconnect;
connect 'swissdb;shutdown=true';

-- check it is still Swiss when we re-boot
connect 'swissdb';
call checkDatabaseLoc('fr_CH');
disconnect;
connect 'swissdb;shutdown=true';


-- Locale automatically converts the components to the correct case
-- create a Hindi in India database (hi_IN)
connect 'hindi;create=true;territory=HI_in';
create procedure checkDatabaseLoc(in locale char(12)) parameter style java language java external name 'org.apache.derbyTesting.functionTests.tests.i18n.DefaultLocale.checkDatabaseLocale';
call checkDatabaseLoc('hi_IN');
disconnect;
connect 'hindi;shutdown=true';


-- now try one with a variant
-- create a English in Israel database for JavaOS en_IL_JavaOS
connect 'Israel;create=true;territory=en_IL_JavaOS';
create procedure checkDatabaseLoc(in locale char(12)) parameter style java language java external name 'org.apache.derbyTesting.functionTests.tests.i18n.DefaultLocale.checkDatabaseLocale';
call checkDatabaseLoc('en_IL_JavaOS');
disconnect;
connect 'Israel;shutdown=true';

-- now try with just a language - we support this
-- as some vms do.
connect 'bacon;create=true;territory=da';
create procedure checkDatabaseLoc(in locale char(12)) parameter style java language java external name 'org.apache.derbyTesting.functionTests.tests.i18n.DefaultLocale.checkDatabaseLocale';
call checkDatabaseLoc('da');
disconnect;
connect 'bacon;shutdown=true';
connect 'bacon';
call checkDatabaseLoc('da');
disconnect;
connect 'bacon;shutdown=true';


--
-- some negative tests
--
connect 'fail1;create=true;territory=';
-- database will not have been created so this connection will fail
connect 'fail1;shutdown=true';

connect 'fail3;create=true;territory=en_';
connect 'fail4;create=true;territory=en_d';
connect 'fail5;create=true;territory=en_US_';
connect 'fail6;create=true;territory=en-US';

-- try using a database name with trailing blanks
-- beetle 4653
connect 'trailblank;create=true';
connect 'trailblank';
connect 'trailblank     ';
connect 'trailblank      ;shutdown=true';


connect ';shutdown=true';
