--
-- this test shows the current supported join functionality
--

-- create some tables
create table t1 (t1_c1 int, t1_c2 char(10));
create table t2 (t2_c1 int, t2_c2 char(10));
create table t3 (t3_c1 int, t3_c2 char(10));
create table t4 (t4_c1 int, t4_c2 char(10));

-- populate the tables
insert into t1 values (1, 't1-row1');
insert into t1 values (2, 't1-row2');
insert into t2 values (1, 't2-row1');
insert into t2 values (2, 't2-row2');
insert into t3 values (1, 't3-row1');
insert into t3 values (2, 't3-row2');
insert into t4 values (1, 't4-row1');
insert into t4 values (2, 't4-row2');

-- negative test, same exposed name
select * from t1, t1;

-- cartesian products
-- full projection
select * from t1, t2;
select * from t1 a, t2 b, t3 cc, t4 d order by 1,2,3,4,5,6;
-- reorder columns
select t2.*, t1.* from t1, t2;
select t2_c2, t1_c2, t1_c1, t2_c1 from t1, t2;

-- project out columns
select t2_c2, t1_c1 from t1, t2;
select a.t1_c1, cc.t1_c1, e.t1_c1, g.t1_c1, i.t1_c1 from t1 a, t1 cc, t1 e, t1 g, t1 i;

-- project/restricts
select a.t1_c1, b.t1_c1, cc.t1_c1, d.t1_c1, e.t1_c1, f.t1_c1, g.t1_c1, h.t1_c1, i.t1_c1, j.t1_c1
from t1 a, t1 b, t1 cc, t1 d, t1 e, t1 f, t1 g, t1 h, t1 i, t1 j
where a.t1_c2 = b.t1_c2 and b.t1_c2 = cc.t1_c2 and cc.t1_c2 = d.t1_c2 and
      d.t1_c2 = e.t1_c2 and e.t1_c2 = f.t1_c2 and f.t1_c2 = g.t1_c2 and
      g.t1_c2 = h.t1_c2 and h.t1_c2 = i.t1_c2 and i.t1_c2 = j.t1_c2;

select a.t1_c1, b.t1_c1, cc.t1_c1, d.t1_c1, e.t1_c1, f.t1_c1, g.t1_c1, h.t1_c1, i.t1_c1, j.t1_c1
from t1 a, t1 b, t1 cc, t1 d, t1 e, t1 f, t1 g, t1 h, t1 i, t1 j
where a.t1_c1 = 1 and b.t1_c1 = 1 and cc.t1_c1 = 1 and d.t1_c1 = 1 and e.t1_c1 = 1 and
	  f.t1_c1 = 1 and g.t1_c1 = 1 and h.t1_c1 = 1 and i.t1_c1 = 1 and
	  a.t1_c2 = b.t1_c2 and b.t1_c2 = cc.t1_c2 and cc.t1_c2 = d.t1_c2 and
      d.t1_c2 = e.t1_c2 and e.t1_c2 = f.t1_c2 and f.t1_c2 = g.t1_c2 and
      g.t1_c2 = h.t1_c2 and h.t1_c2 = i.t1_c2 and i.t1_c2 = j.t1_c2;


-- project out entire tables
select 1, 2 from t1, t2;
select 1, t1.t1_c1 from t1, t2;
select t2.t2_c2,1 from t1, t2;

-- bug #306
select c.t1_c1 from (select a.t1_c1 from t1 a, t1 b) c, t1 d where c.t1_c1 = d.t1_c1;

-- create a table for testing inserts
create table instab (instab_c1 int, instab_c2 char(10), instab_c3 int,
		     instab_c4 char(10));

-- insert select with joins
-- cartesian product
insert into instab select * from t1, t2;
select * from instab;
delete from instab;

insert into instab (instab_c1, instab_c2, instab_c3, instab_c4)
	select * from t1, t2;
select * from instab;
delete from instab;

insert into instab (instab_c1, instab_c2, instab_c3, instab_c4)
	select t2_c1, t2_c2, t1_c1, t1_c2 from t1, t2;
select * from instab;
delete from instab;

insert into instab (instab_c3, instab_c1, instab_c2, instab_c4)
	select t2_c1, t1_c1, t1_c2, t2_c2 from t1, t2;
select * from instab;
delete from instab;

-- projection
insert into instab (instab_c1, instab_c3)
	select t1_c1, t2_c1 from t1, t2;
select * from instab;
delete from instab;

-- project out 1 or more tables from join
insert into instab select 1, '2', 3, '4' from t1, t2;
select * from instab;
delete from instab;

insert into instab select 1, t1.t1_c2, 3, t1.t1_c2 from t1, t2;
select * from instab;
delete from instab;

insert into instab select t2.t2_c1, '2', t2.t2_c1, '4' from t1, t2;
select * from instab;
delete from instab;

------------------------------------------
-- test optimizations where we push around
-- predicates (remapColumnReferences)
------------------------------------------
-- case
select t1_c1 from t1, t2 where (case when t1_c1 = 1 then t2_c2 end) = t2_c2;

-- CHAR built-in function
select t1_c1 from t1, t2 where CHAR(t1_c1) = t2_c2;

-- logical operator OR
select t1_c1 from t1, t2 where t1_c1 = 1 or t2_c1 = 2;

-- logical operator AND
select t1_c1 from t1, t2 where t1_c1 = 2147483647 and 2147483647 = t2_c1;

-- beetle 5421
-- INT built-in function
select t1_c1 from t1, t2 where INT(t1_c1) = t2_c1;
select t1_c1 from t1, t2 where t1_c1 = INT(2147483647) and INT(2147483647) = t2_c1;

-- transitive closure - verify join condition doesn't get dropped
create table x(c1 int);
create table y(c1 int);
insert into x values 1, 2, null;
insert into y values 1, 2, null;

select * from x,y where x.c1 = y.c1 and x.c1 = 1 and y.c1 = 2;
select * from x,y where x.c1 = y.c1 and x.c1 is null;
select * from x,y where x.c1 = y.c1 and x.c1 is null and y.c1 = 2;
select * from x,y where x.c1 = y.c1 and x.c1 is null and y.c1 is null;

-- Beetle task 5000. Bug found by Websphere. Should not return any rows.
select t1_c1, t1_c2, t2_c1, t2_c2
  from t1, t2
  where t1_c1 = t2_c1
    and t1_c1 = 1
    and t2_c1 <> 1;

-- Beetle task 4736
create table a (a1 int not null primary key, a2 int, a3 int, a4 int, a5 int, a6 int);
create table b (b1 int not null primary key, b2 int, b3 int, b4 int, b5 int, b6 int);
create table c (c1 int not null, c2 int, c3 int not null, c4 int, c5 int, c6 int);
create table d (d1 int not null, d2 int, d3 int not null, d4 int, d5 int, d6 int);

alter table c add primary key (c1,c3);
alter table d add primary key (d1,d3);

insert into a values (1,1,3,6,NULL,2),(2,3,2,4,2,2),(3,4,2,NULL,NULL,NULL),
                     (4,NULL,4,2,5,2),(5,2,3,5,7,4),(7,1,4,2,3,4),
                     (8,8,8,8,8,8),(6,7,3,2,3,4);

insert into b values (6,7,2,3,NULL,1),(4,5,9,6,3,2),(1,4,2,NULL,NULL,NULL),
                     (5,NULL,2,2,5,2),(3,2,3,3,1,4),(7,3,3,3,3,3),(9,3,3,3,3,3);

insert into c values (3,7,7,3,NULL,1),(8,3,9,1,3,2),(1,4,1,NULL,NULL,NULL),
                     (3,NULL,1,2,4,2),(2,2,5,3,2,4),(1,7,2,3,1,1),(3,8,4,2,4,6);

insert into d values (1,7,2,3,NULL,3),(2,3,9,1,1,2),(2,2,2,NULL,3,2),
                     (1,NULL,3,2,2,1),(2,2,5,3,2,3),(2,5,6,3,7,2);

select a1,b1,c1,c3,d1,d3 
  from D join (A left outer join (B join C on b2=c2) on a1=b1) 
    on d3=b3 and d1=a2;

select a1,b1,c1,c3,d1,d3 
  from D join ((B join C on b2=c2) right outer join A on a1=b1) 
    on d3=b3 and d1=a2;

-----------------------------------
-- clean up
----------------------------------
drop table a;
drop table b;
drop table c;
drop table d;
drop table t1;
drop table t2;
drop table t3;
drop table t4;
drop table instab;
drop table x;
drop table y;
