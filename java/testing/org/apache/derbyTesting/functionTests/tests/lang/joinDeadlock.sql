disconnect;

connect 'jdbc:derby:wombat;user=U1' AS C1;
autocommit off;
connect 'jdbc:derby:wombat;user=U2' AS C2;
autocommit off;

set connection C1;
-- user 1 for bug 1573
-- a deadlock when reopening a join gets an assertion
-- violation in close()
create table outer1(c1 int);
create index o1_i1 on outer1(c1);
insert into outer1 (c1) values 1, 2;
commit;

create table inner1(c1 int, c2 char(254));
create index i1_i1 on inner1(c1);
insert into inner1 (c1) values 1, 2;
commit;

create table inner2(c1 int, c2 char(254));
create index i2_i1 on inner2(c1);
insert into inner2 (c1) values 1, 2;
commit;

-- this user will get lock timeout in subquery on 2nd next
get cursor c1 as 'select * from outer1 where c1 <= (select count(*) from inner1, inner2 where outer1.c1 = outer1.c1)';
next c1;

set connection C2;
update u1.inner1 set c1 = c1 where c1 = 1;


set connection C1;
next c1;

disconnect;

set connection C2;
disconnect;
