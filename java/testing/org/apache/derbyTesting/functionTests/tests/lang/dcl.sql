-- test database class loading.


create schema emc;
set schema emc;
create table contacts (id int primary key, e_mail varchar(30));

create procedure EMC.ADDCONTACT(id INT, e_mail VARCHAR(30))
MODIFIES SQL DATA
external name 'org.apache.derbyTesting.databaseclassloader.emc.addContact'
language java parameter style java;

-- fails because no class in classpath, 
CALL EMC.ADDCONTACT(1, 'bill@somecompany.com');

-- install the jar, copied there by the magic of supportfiles
-- in the test harness (dcl_app.properties). The source for
-- the class is contained within the jar for reference.
CALL SQLJ.INSTALL_JAR('file:extin/dcl_emc1.jar', 'EMC.MAIL_APP', 0);

-- fails because no class not in classpath, jar file not in database classpath.
CALL EMC.ADDCONTACT(1, 'bill@somecompany.com');

-- now add this into the database class path
call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.database.classpath', 'EMC.MAIL_APP');


-- all should work now
CALL EMC.ADDCONTACT(1, 'bill@ruletheworld.com');
CALL EMC.ADDCONTACT(2, 'penguin@antartic.com');
SELECT id, e_mail from EMC.CONTACTS;

-- now the application needs to track if e-mails are valid
ALTER TABLE EMC.CONTACTS ADD COLUMN OK SMALLINT;
SELECT id, e_mail, ok from EMC.CONTACTS;

-- well written application, INSERT used explicit column names
-- ok defaults to NULL
CALL EMC.ADDCONTACT(3, 'big@blue.com');
SELECT id, e_mail, ok from EMC.CONTACTS;

-- now change the application to run checks on the e-mail
-- address to ensure it is valid (in this case by seeing if
-- simply includes 'spam' in the title.
CALL SQLJ.REPLACE_JAR('file:extin/dcl_emc2.jar', 'EMC.MAIL_APP');

CALL EMC.ADDCONTACT(4, 'spammer@ripoff.com');
CALL EMC.ADDCONTACT(5, 'open@source.org');
SELECT id, e_mail, ok from EMC.CONTACTS;

-- now add another jar in to test two jars and
-- a quoted identifer for the jar names.

create schema "emcAddOn";
set schema emcAddOn;
set schema "emcAddOn";

create function "emcAddOn".VALIDCONTACT(e_mail VARCHAR(30))
RETURNS SMALLINT
READS SQL DATA
external name 'org.apache.derbyTesting.databaseclassloader.addon.vendor.util.valid'
language java parameter style java;

CALL SQLJ.INSTALL_JAR('file:extin/dcl_emcaddon.jar', '"emcAddOn"."MailAddOn"', 0);

call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.database.classpath', 'EMC.MAIL_APP:"emcAddOn"."MailAddOn"');

select e_mail, "emcAddOn".VALIDCONTACT(e_mail) from EMC.CONTACTS;

-- function that gets the signers of the class (loaded from the jar)
create function EMC.GETSIGNERS(CLASS_NAME VARCHAR(256))
RETURNS VARCHAR(60)
NO SQL
external name 'org.apache.derbyTesting.databaseclassloader.emc.getSigners'
language java parameter style java;

-- at this point the jar is not signed, NULL expected
VALUES EMC.GETSIGNERS('org.apache.derbyTesting.databaseclassloader.emc');

-- Replace with a signed jar
-- (self signed certificate)
--
-- Commands used to sign jar
-- keytool -genkey -dname "cn=EMC CTO, ou=EMC APP, o=Easy Mail Company, c=US" -alias emccto -keypass kpi135 -keystore emcks -storepass ab987c
-- keytool -selfcert -alias emccto -keypass kpi135 -keystore emcks -storepass ab987c
-- jarsigner -keystore emcks -storepass ab987c -keypass kpi135 -signedjar dcl_emc2s.jar dcl_emc2.jar emccto
--
--

CALL SQLJ.REPLACE_JAR('file:extin/dcl_emc2s.jar', 'EMC.MAIL_APP');
VALUES EMC.GETSIGNERS('org.apache.derbyTesting.databaseclassloader.emc');

-- other jar should not be signed
VALUES EMC.GETSIGNERS('org.apache.derbyTesting.databaseclassloader.addon.vendor.util');

-- replace with a hacked jar file, emc.class modified to diable
-- valid e-mail address check but using same signatures.
-- ie direct replacement of the .class file.
CALL SQLJ.REPLACE_JAR('file:extin/dcl_emc2sm.jar', 'EMC.MAIL_APP');
CALL EMC.ADDCONTACT(99, 'spamking@cracker.org');

-- replace with a hacked jar file, emc.class modified to 
-- be an invalid jar file (no signing on this jar).
CALL SQLJ.REPLACE_JAR('file:extin/dcl_emc2l.jar', 'EMC.MAIL_APP');
CALL EMC.ADDCONTACT(999, 'spamking2@cracker.org');


-- cleanup
CALL SQLJ.REMOVE_JAR('EMC.MAIL_APP', 0);
call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.database.classpath', '"emcAddOn"."MailAddOn"');
CALL EMC.ADDCONTACT(99, 'cash@venture.com');
CALL SQLJ.REMOVE_JAR('EMC.MAIL_APP', 0);
DROP PROCEDURE EMC.ADDCONTACT;
DROP FUNCTION EMC.GETSIGNERS;

select e_mail, "emcAddOn".VALIDCONTACT(e_mail) from EMC.CONTACTS;
call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.database.classpath', '');
select e_mail, "emcAddOn".VALIDCONTACT(e_mail) from EMC.CONTACTS;
CALL SQLJ.REMOVE_JAR('"emcAddOn"."MailAddOn"', 0);
DROP FUNCTION "emcAddOn".VALIDCONTACT;

DROP TABLE EMC.CONTACTS;
