-- by default, holdability of ResultSet objects created using this Connection object is true. Following will set it to false for this connection.
NoHoldForConnection;

create table a (a int);
insert into a values (1);
select * from a;
drop table a;


create table b (si smallint,i int, bi bigint, r real, f float, d double precision, n5_2 numeric(5,2), dec10_3 decimal(10,3), ch20 char(20),vc varchar(20), lvc long varchar, blobCol blob(1000),  clobCol clob(1000));

insert into b values(2,3,4,5.3,5.3,5.3,31.13,123456.123, 'one','one','one',cast(X'01ABCD' as blob(1000)),'one');

insert into b values(-32768,-2147483648, -9223372036854775808 ,1.2E-37, 2.225E-307, +2.225E-307,-56.12, -123456.123,'one','one','one', cast(X'01ABCD' as blob(1000)),'one');

insert into b values(null,null,null,null,null,null,null,null,null,null,null,null,null);


insert into b values(32767,2147483647, 9223372036854775807 ,1.4 , 3.4028235E38 ,3.4028235E38  ,999.99, 9999999.999,'one','one','one',cast(X'01ABCD' as blob(1000)), 'one');

select * from b;
drop table b;

create table c (si smallint not null,i int not null , bi bigint not null, r real not null, f float not null, d double precision not null, n5_2 numeric(5,2) not null , dec10_3 decimal(10,3) not null, ch20 char(20) not null ,vc varchar(20) not null, lvc long varchar not null,  blobCol blob(1000) not null,  clobCol clob(1000) not null);


insert into c values(2,3,4,5.3,5.3,5.3,31.13,123456.123, 'one','one','one', cast(X'01ABCD' as blob(1000)), 'one');

insert into c values(-32768,-2147483648, -9223372036854775808 ,1.2E-37, 2.225E-307, +2.225E-307,-56.12, -123456.123,'one','one','one', cast(X'01ABCD' as blob(1000)),'one');

select * from c;

values(10); 
values('hello');
values(1.2);

drop table c;

-- bug 4430 aliasinfo nullability problem
select aliasinfo from sys.sysaliases where aliasinfo is null;



-- test commit and rollback
autocommit off;
create table a(a int);
insert into a values (1);
select * from a;
commit;
drop table a;
rollback;
select * from a;
drop table a;
commit;
autocommit on;

maximumdisplaywidth 5000;
--test 84 columns
 values(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 
11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
21, 22, 23, 24, 25, 26, 27, 28, 29, 30,
31, 32, 33, 34, 35, 36, 37, 38, 39, 40,
41, 42, 43, 44, 45, 46, 47, 48, 49, 50,
51, 52, 53, 54, 55, 56, 57, 58, 59, 60,
61, 62, 63, 64, 65, 66, 67, 68, 69, 70,
71, 72, 73, 74, 75, 76, 77, 78, 79, 80,
81, 82, 83, 84);

values(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 
11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
21, 22, 23, 24, 25, 26, 27, 28, 29, 30,
31, 32, 33, 34, 35, 36, 37, 38, 39, 40,
41, 42, 43, 44, 45, 46, 47, 48, 49, 50,
51, 52, 53, 54, 55, 56, 57, 58, 59, 60,
61, 62, 63, 64, 65, 66, 67, 68, 69, 70,
71, 72, 73, 74, 75, 76, 77, 78, 79, 80,
81, 82, 83, 84, 85, 86, 87, 88, 89, 90,
91, 92, 93, 94, 95, 96, 97, 98, 99, 100);


-- test SQL Error with non-string arguments
-- Make sure connection still ok (Bug 4657)
create table a (a int);
insert into a values(2342323423) ;
insert into a values(1);
select * from a;
drop table a;

-- Bug 4694 Test automatic rollback with close of connection
-- in ij
autocommit off;
create table a (a int);


-- Bug 4758 - Store error does not return properly to client
autocommit off;
create table t (i int);
insert into t values(1);
commit;
insert into t values(2);
connect 'wombat';
-- by default, holdability of ResultSet objects created using this Connection object is true. Following will set it to false for this connection.
NoHoldForConnection;
prepare s1 as 'select * from t';
execute s1;
execute s1;

-- Bug 5967 - Selecting from 2 lob columns w/ the first one having data of length 0
create table t1 (c1 clob(10), c2 clob(10));
insert into t1 values ('', 'some clob');
select * from t1;
select c2 from t1;
insert into t1 values ('', '');
insert into t1 values ('some clob', '');
select * from t1;
select c2 from t1;
drop table t1;

