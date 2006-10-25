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
create view lock_table as select 
cast(l.type as char(8)) as type,cast(lockcount as char(3)) as
cnt,mode,cast(tablename as char(12)) as tabname,cast(lockname as char(10))
as lockname,state from syscs_diag.lock_table l ;
autocommit off;
create table derby94_t1(c1 int, c2 int not null primary key);
create table derby94_t2(c1 int);
insert into derby94_t1 values (0, 200), (1, 201), (2, 202), (3, 203), (4, 204), (5, 205), (6, 206), (7, 207), (8, 208), (9, 209);
insert into derby94_t1 select c1+10 , c2 +10 from derby94_t1;
insert into derby94_t1 select c1+20 , c2 +20 from derby94_t1;
insert into derby94_t1 select c1+40 , c2 +40 from derby94_t1;
insert into derby94_t1 select c1+80 , c2 +80 from derby94_t1;
insert into derby94_t2 values (0), (1), (2), (3), (4), (5), (6), (7), (8), (9);
commit;
get cursor c1 as 'select * from derby94_t1 FOR UPDATE of c1';
next c1;
update derby94_t1 set c1=c1+999 WHERE CURRENT OF c1;
next c1;
get cursor c2 as 'select *  from derby94_t2 FOR UPDATE of c1';
next c2 ;

select * from lock_table order by tabname, type desc, mode, cnt, lockname ;
--following insert should get X lock on derby94_t2 because of escalation , but should leave U lock on derby94_t1 as it is
insert into derby94_t2 select c1 from derby94_t1 ;
select * from lock_table order by tabname, type desc, mode, cnt, lockname ;

--following update statement should escalate the locks on derby94_t1 to table level X lock
update derby94_t1 set c1=c1+999 ;
select * from lock_table order by tabname, type desc, mode, cnt, lockname ;
close c1 ;
close c2 ;
commit ;
--following lock table dump should not show any  locks, above commit should have release them
select * from lock_table order by tabname, type desc, mode, cnt, lockname;
drop table derby94_t1;
drop table derby94_t2;
commit;
--end derby-94 case

