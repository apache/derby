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
--
-- Consolidated BEFORE trigger tests from all trigger tests.

-- The following tests moved from TriggerValidate.sql to here.

create table x (x int, constraint ck check (x > 0));
create table unrelated (x int, constraint ckunrelated check (x > 0));
create index x on x(x);

------------------------------------
-- DDL
------------------------------------
create trigger tbad NO CASCADE before insert on x for each statement mode db2sql drop table x;

create trigger tbad NO CASCADE before insert on x for each statement mode db2sql drop index x;

create trigger tbad NO CASCADE before insert on x for each statement mode db2sql alter table x add column y int;

create trigger tbad NO CASCADE before insert on x for each statement mode db2sql alter table x add constraint ck2 check(x > 0);

create trigger tbad NO CASCADE before insert on x for each statement mode db2sql alter table x drop constraint ck;

create trigger tbad NO CASCADE before insert on x for each statement mode db2sql create index x2 on x (x);

create trigger tbad NO CASCADE before insert on x for each statement mode db2sql create index xunrelated on unrelated(x);

create trigger tbad NO CASCADE before insert on x for each statement mode db2sql drop index xunrelated; 

create trigger tbad NO CASCADE before insert on x for each statement mode db2sql drop trigger tbad;

create trigger tbad NO CASCADE before insert on x for each statement mode db2sql 
	create trigger tbad2 NO CASCADE before insert on x for each statement mode db2sql values 1;

create trigger tokv1 NO CASCADE before insert on x for each statement mode db2sql values 1;
insert into x values 1;
select * from x;
drop trigger tokv1;

------------------------------------
-- MISC
------------------------------------
create trigger tbad NO CASCADE before insert on x for each statement mode db2sql set isolation to rr;

create trigger tbad NO CASCADE before insert on x for each statement mode db2sql lock table x in share mode;

------------------------------------
-- DML, cannot perform dml on same
-- table for before trigger, of for
-- after
------------------------------------
-- before
create trigger tbadX NO CASCADE before insert on x for each statement mode db2sql insert into x values 1;

create trigger tbadX NO CASCADE before insert on x for each statement mode db2sql delete from x;

create trigger tbadX NO CASCADE before insert on x for each statement mode db2sql update x set x = x;

-- Following tests moved here from triggerRefClause, since these use BEFORE triggers
-- syntax
create trigger t1 NO CASCADE before update on x referencing badtoken as oldtable for each row mode db2sql values 1;
create trigger t1 NO CASCADE before update on x referencing old as oldrow new for each row mode db2sql values 1;

-- dup names
create trigger t1 NO CASCADE before update on x referencing old as oldrow new as newrow old as oldrow2 
	for each row mode db2sql values 1;
create trigger t1 NO CASCADE before update on x referencing new as newrow new as newrow2 old as oldrow2 
	for each row mode db2sql values 1;

-- mismatch: row->for each statement mode db2sql, table->for each row
create trigger t1 NO CASCADE before update on x referencing new_table as newtab for each row mode db2sql values 1;
create trigger t1 NO CASCADE before update on x referencing new as newrow for each statement mode db2sql values 1;

-- same as above, but using old
create trigger t1 NO CASCADE before update on x referencing old_table as old for each row mode db2sql select * from old;
create trigger t1 NO CASCADE before update on x referencing old_table as old for each statement mode db2sql values old.x;

-- old and new cannot be used once they have been redefined
create trigger t1 NO CASCADE before update on x referencing old_table as oldtable for each statement mode db2sql select * from old;
create trigger t1 NO CASCADE before update on x referencing old as oldtable for each row mode db2sql values old.x;

-- try some other likely uses
create table y (x int);
create trigger t1 NO CASCADE before insert on x referencing new_table as newrowtab for each statement mode db2sql insert into y select x from newrowtab;

drop table x;
drop table y;

