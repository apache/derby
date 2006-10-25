--
--   Licensed to the Apache Software Foundation (ASF) under one or more
--   contributor license agreements.  See the NOTICE file distributed with
--   this work for additional information regarding copyright ownership.
--   The ASF licenses this file to You under the Apache License, Version 2.0
--   (the "License"); you may not use this file except in compliance with
--   the License.  You may obtain a copy of the License at
--
--      http://www.apache.org/licenses/LICENSE-2.0
--
--   Unless required by applicable law or agreed to in writing, software
--   distributed under the License is distributed on an "AS IS" BASIS,
--   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
--   See the License for the specific language governing permissions and
--   limitations under the License.
--
-- testing Space table
-- unfilled pages column of space table is just a guess, thus it is
-- not consistent across runs, in particular for indexes, but also for
-- tables. 
-- Therefore tests do not report the numunfilledpages column
run resource 'createTestProcedures.subsql';
call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.storage.pageSize', '4096');
create table ideleteu (a varchar(2000), b varchar(2000)) ;
insert into ideleteu values (PADSTRING('rrrrrrrrrr',2000), PADSTRING('ssssssssssssssss',2000));
insert into ideleteu values (PADSTRING('rrrrrrrrrr',2000), PADSTRING('ssssssssssssssss',2000));
insert into ideleteu values (PADSTRING('rrrrrrrrrr',2000), PADSTRING('ssssssssssssssss',2000));
insert into ideleteu values (PADSTRING('rrrrrrrrrr',2000), PADSTRING('ssssssssssssssss',2000));
insert into ideleteu values (PADSTRING('rrrrrrrrrr',2000), PADSTRING('ssssssssssssssss',2000));

-- This query also tests the SpaceTable class alias
select conglomeratename, isindex, numallocatedpages, numfreepages, pagesize, estimspacesaving
	from new org.apache.derby.diag.SpaceTable('IDELETEU') t
	order by conglomeratename; 

delete from ideleteu;
CALL WAIT_FOR_POST_COMMIT();

select conglomeratename, isindex, numallocatedpages, numfreepages, pagesize, estimspacesaving
	from new org.apache.derby.diag.SpaceTable('IDELETEU') t
	order by conglomeratename; 


select conglomeratename, isindex, numallocatedpages, numfreepages, pagesize, estimspacesaving
	from new org.apache.derby.diag.SpaceTable('PLATYPUS') t
	order by conglomeratename; 

create table platypus (a varchar(1000), b varchar(3500), c varchar(400), d varchar(100)) ;
create index kookaburra on platypus (a) ;
create index echidna on platypus (c) ;
create index wallaby on platypus (a,c,d) ;

select conglomeratename, isindex, numallocatedpages, numfreepages, pagesize, estimspacesaving
	from new org.apache.derby.diag.SpaceTable('PLATYPUS') t
	order by conglomeratename; 

insert into platypus values (PADSTRING('wwwwwww',1000), PADSTRING('xxx',3500), PADSTRING('yy',400), PADSTRING('zzz',100));
insert into platypus values (PADSTRING('wwwwwww',1000), PADSTRING('xxx',3500), PADSTRING('yy',400), PADSTRING('zzz',100));
insert into platypus values (PADSTRING('wwwwwww',1000), PADSTRING('xxx',3500), PADSTRING('yy',400), PADSTRING('zzz',100));
insert into platypus values (PADSTRING('wwwwwww',1000), PADSTRING('xxx',3500), PADSTRING('yy',400), PADSTRING('zzz',100));
insert into platypus values (PADSTRING('wwwwwww',1000), PADSTRING('xxx',3500), PADSTRING('yy',400), PADSTRING('zzz',100));
insert into platypus values (PADSTRING('wwwwwww',1000), PADSTRING('xxx',3500), PADSTRING('yy',400), PADSTRING('zzz',100));



select conglomeratename, isindex, numallocatedpages, numfreepages, pagesize, estimspacesaving
	from new org.apache.derby.diag.SpaceTable('PLATYPUS') t
	order by conglomeratename; 


insert into platypus values (PADSTRING('wwwwwww',1000), PADSTRING('xxx',3500), PADSTRING('yy',400), PADSTRING('zzz',100));
insert into platypus values (PADSTRING('wwwwwww',1000), PADSTRING('xxx',3500), PADSTRING('yy',400), PADSTRING('zzz',100));
insert into platypus values (PADSTRING('wwwwwww',1000), PADSTRING('xxx',3500), PADSTRING('yy',400), PADSTRING('zzz',100));
insert into platypus values (PADSTRING('wwwwwww',1000), PADSTRING('xxx',3500), PADSTRING('yy',400), PADSTRING('zzz',100));


select conglomeratename, isindex, numallocatedpages, numfreepages, pagesize, estimspacesaving
	from new org.apache.derby.diag.SpaceTable('PLATYPUS') t
	order by conglomeratename; 

delete from platypus;
CALL WAIT_FOR_POST_COMMIT();


select conglomeratename, isindex, numallocatedpages, numfreepages, pagesize, estimspacesaving
	from new org.apache.derby.diag.SpaceTable('PLATYPUS') t
	order by conglomeratename; 


select conglomeratename, isindex, numallocatedpages, numfreepages, numunfilledpages, pagesize, estimspacesaving
	from new org.apache.derby.diag.SpaceTable('NONEXISTING') t
	order by conglomeratename; 


create table "platypus2" (a varchar(10), b varchar(1500), c varchar(400), d varchar(100)) ;


insert into "platypus2" (values (PADSTRING('wwwwwww',10), PADSTRING('xxx',1500), PADSTRING('yy',400), PADSTRING('zzz',100)));
insert into "platypus2" (values (PADSTRING('wwwwwww',10), PADSTRING('xxx',1500), PADSTRING('yy',400), PADSTRING('zzz',100)));
insert into "platypus2" (values (PADSTRING('wwwwwww',10), PADSTRING('xxx',1500), PADSTRING('yy',400), PADSTRING('zzz',100)));
insert into "platypus2" (values (PADSTRING('wwwwwww',10), PADSTRING('xxx',1500), PADSTRING('yy',400), PADSTRING('zzz',100)));
insert into "platypus2" (values (PADSTRING('wwwwwww',10), PADSTRING('xxx',1500), PADSTRING('yy',400), PADSTRING('zzz',100)));
insert into "platypus2" (values (PADSTRING('wwwwwww',10), PADSTRING('xxx',1500), PADSTRING('yy',400), PADSTRING('zzz',100)));



create index kookaburra2 on "platypus2" (a);
create index echidna2 on "platypus2" (c);
create index wallaby2 on "platypus2" (a,c,d) ;

select conglomeratename, isindex, numallocatedpages, numfreepages, pagesize, estimspacesaving
	from new org.apache.derby.diag.SpaceTable('platypus2') t
	order by conglomeratename; 

select conglomeratename, isindex, numallocatedpages, numfreepages, pagesize, estimspacesaving
    from SYS.SYSSCHEMAS s,
         SYS.SYSTABLES t,
         new org.apache.derby.diag.SpaceTable(SCHEMANAME,TABLENAME) v
    where s.SCHEMAID = t.SCHEMAID
    and s.SCHEMANAME = 'APP'
    order by conglomeratename;

drop table platypus;
drop table "platypus2";

autocommit off;

drop table foo_int;
create table foo_int (a int);
drop table foo_char;
create table foo_char (a char(100)) ;
drop table foo_varchar;
create table foo_varchar (a varchar(32000)) ;

-- let the foo_longxxx get created at 32K
call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.storage.pageSize', NULL);
drop table foo_longvarchar;
create table foo_longvarchar (a long varchar);

drop table foo_longvarbinary;
create table foo_longvarbinary (a long varchar for bit data);

-- Back to 4K
call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.storage.pageSize', '4096');
drop table foo_bit;
create table foo_bit (a CHAR(100) FOR BIT DATA) ;
drop table foo_varbinary;
create table foo_varbinary (a VARCHAR(10000) FOR BIT DATA) ;



select v.CONGLOMERATENAME, PAGESIZE
from SYS.SYSSCHEMAS s,
SYS.SYSTABLES t,
new org.apache.derby.diag.SpaceTable(SCHEMANAME,TABLENAME) v
where s.SCHEMAID = t.SCHEMAID and CONGLOMERATENAME in  
    ('FOO_INT', 'FOO_VARCHAR', 'FOO_CHAR', 'FOO_LONGVARCHAR', 'FOO_VARBINARY', 'FOO_LONGVARBINARY', 'FOO_BIT') order by 1;

drop table foo_int;
call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.storage.pageReservedSpace','65');
create table foo_int (a int);
drop table foo_char;
create table foo_char (a char(100));
drop table foo_varchar;
create table foo_varchar (a varchar(10000));

-- let the foo_longxxx get created at 32K
call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.storage.pageSize', NULL);
drop table foo_longvarchar;
create table foo_longvarchar (a long varchar) ;

drop table foo_longvarbinary;
create table foo_longvarbinary (a long varchar for bit data) ;

-- Back to 4K
call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.storage.pageSize', '4096');

drop table foo_bit;
create table foo_bit (a CHAR(100) FOR BIT DATA ) ;
drop table foo_varbinary;
create table foo_varbinary (a VARCHAR(10000) FOR BIT DATA) ;

call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.storage.pageReservedSpace',NULL);

select v.CONGLOMERATENAME, PAGESIZE
from SYS.SYSSCHEMAS s,
SYS.SYSTABLES t,
new org.apache.derby.diag.SpaceTable(SCHEMANAME,TABLENAME) v
where s.SCHEMAID = t.SCHEMAID and CONGLOMERATENAME in  
    ('FOO_INT', 'FOO_VARCHAR', 'FOO_CHAR', 'FOO_LONGVARCHAR', 'FOO_VARBINARY', 'FOO_LONGVARBINARY', 'FOO_BIT') order by 1;

--  8K pagesize 
call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.storage.pageSize', '8192');
drop table foo_int;
create table foo_int (a int);
drop table foo_char;
create table foo_char (a char(100));
drop table foo_varchar;
create table foo_varchar (a varchar(10000));
drop table foo_longvarchar;
create table foo_longvarchar (a long varchar);
drop table foo_bit;
create table foo_bit (a CHAR(100) FOR BIT DATA);
drop table foo_varbinary;
create table foo_varbinary (a varchar(10000) FOR BIT DATA);
drop table foo_longvarbinary;
create table foo_longvarbinary (a long varchar for bit data);


select v.CONGLOMERATENAME, PAGESIZE
from SYS.SYSSCHEMAS s,
SYS.SYSTABLES t,
new org.apache.derby.diag.SpaceTable(SCHEMANAME,TABLENAME) v
where s.SCHEMAID = t.SCHEMAID and CONGLOMERATENAME in  
    ('FOO_INT', 'FOO_VARCHAR', 'FOO_CHAR', 'FOO_LONGVARCHAR', 'FOO_VARBINARY', 'FOO_LONGVARBINARY', 'FOO_BIT') order by 1;
call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.storage.pageSize', '4096');
commit;

drop table foo_int;
create table foo_int (a int);
drop table foo_char;
create table foo_char (a char(100)) ;
drop table foo_varchar;
create table foo_varchar (a varchar(10000));
drop table foo_longvarchar;
create table foo_longvarchar (a long varchar);
drop table foo_bit;
create table foo_bit (a CHAR(100) for bit data);
drop table foo_varbinary;
create table foo_varbinary (a varchar(10000) for bit data);
drop table foo_longvarbinary;
create table foo_longvarbinary (a long varchar for bit data);


select v.CONGLOMERATENAME, PAGESIZE
from SYS.SYSSCHEMAS s,
SYS.SYSTABLES t,
new org.apache.derby.diag.SpaceTable(SCHEMANAME,TABLENAME) v
where s.SCHEMAID = t.SCHEMAID and CONGLOMERATENAME in  
    ('FOO_INT', 'FOO_VARCHAR', 'FOO_CHAR', 'FOO_LONGVARCHAR', 'FOO_VARBINARY', 'FOO_LONGVARBINARY', 'FOO_BIT') order by 1;

commit;

disconnect;




