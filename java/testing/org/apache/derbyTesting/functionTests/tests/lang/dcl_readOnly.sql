-- common tests for read-only jarred database

select * from EMC.CONTACTS;
select e_mail, "emcAddOn".VALIDCONTACT(e_mail) from EMC.CONTACTS;
insert into EMC.CONTACTS values(3, 'no@is_read_only.gov', NULL);
CALL EMC.ADDCONTACT(3, 'really@is_read_only.gov');

-- same set as dcl.sql for reading resources
-- VALUES EMC.GETARTICLE('graduate.txt');
-- VALUES EMC.GETARTICLE('/article/release.txt');
-- VALUES EMC.GETARTICLE('/article/fred.txt');
-- VALUES EMC.GETARTICLE('barney.txt');
-- VALUES EMC.GETARTICLE('emc.class');
-- VALUES EMC.GETARTICLE('/org/apache/derbyTesting/databaseclassloader/emc.class');

-- signed
VALUES EMC.GETSIGNERS('org.apache.derbyTesting.databaseclassloader.emc');
-- not signed
VALUES EMC.GETSIGNERS('org.apache.derbyTesting.databaseclassloader.addon.vendor.util');

-- ensure that a read-only database automatically gets table locking
autocommit off;
select * from EMC.CONTACTS WITH RR;
select TYPE, MODE, TABLENAME from syscs_diag.lock_table ORDER BY 1,2,3;