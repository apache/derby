
-- Testing changing the length of a column.
-- Also testing the new syntax for generated column spec and identity attribute

create table alltypes (i int, tn int, s smallint, l bigint,
				c char(10), v varchar(50), lvc long varchar,
				nc char(10), nvc varchar(10),
				d double precision, r real, f float,
				dt date, t time, ts timestamp,
				b char(2) for bit data, bv varchar(2) for bit data, lbv long varchar for bit data,
				dc decimal(5,2), n numeric(8,4), o bigint);

-- lets start with negative tests first.

alter table alltypes alter c set data type char(20);
alter table alltypes alter b set data type char(4) for bit data;
alter table alltypes alter nc set data type char(20);
alter table alltypes alter dc set data type decimal (8,2);
alter table alltypes alter n set data type numeric (12,8);
alter table alltypes alter c set data type varchar(10);
alter table alltypes alter b set data type varchar(2) for bit data;
alter table alltypes alter dc set data type numeric(8,2);
alter table alltypes alter tn set data type int;

alter table alltypes alter v set data type varchar(1);
alter table alltypes alter v set data type varchar(49);
alter table alltypes alter bv set data type varchar(1) for bit data;
alter table alltypes alter bv set data type varchar(2) for bit data;
alter table alltypes alter nvc set data type varchar(0);
alter table alltypes alter nvc set data type varchar(9);

drop table alltypes;

create table t0 (i int not null, v varchar(1) not null, constraint pk primary key(v,i));
-- this should work. primary key constraint has no referencing fkey
-- constraints.
alter table t0 alter v set data type varchar(2);
create table t1 (i int, v varchar(2), constraint fk foreign key  (v,i) references t0(v,i));
alter table t0 alter v set data type varchar(3);
-- should fail; can't muck around with fkey constraints.
alter table t1 alter v set data type varchar(3);

drop table t1;
drop table t0;

-- do the same thing over again with a unique key constraint this time.
create table t0 (i int not null, v varchar(1) not null, constraint  uq unique(v,i));
-- this should work. unique constraint has no referencing fkey
-- constraints.
alter table t0 alter v set data type varchar(2);
create table t1 (i int, v varchar(2), constraint fk foreign key  (v,i) references t0(v,i));
-- this should fail-- someone is referencing me.
alter table t0 alter v set data type varchar(3);
drop table t1;
drop table t0;
--
-- test that we can't alter a column with an autoincrement default to nullable
create table t1(a int generated always as identity (start with 1, increment by 1));
insert into t1 values(DEFAULT);
select * from t1;
-- this should fail
alter table t1 modify a null;
insert into t1 values(DEFAULT);
select * from t1;
drop table t1;

-- lets get to positive tests.
create table t1 (vc varchar(1) not null, nvc varchar(1) not null, bv varchar(1) for bit data not null);
alter table t1 add constraint uq unique (vc, nvc, bv);

insert into t1 values ('p', 'p', x'01');
insert into t1 values ('pe', 'p', x'01');
alter table t1 alter vc set data type varchar(2);
insert into t1 values ('pe', 'p', x'01');
insert into t1 values ('pe', 'pe', x'01');
alter table t1 alter nvc set data type varchar(2);
insert into t1 values ('pe', 'pe', x'01');
insert into t1 values ('pe', 'pe', x'1000');
alter table t1 alter bv set data type varchar(2) for bit data;
insert into t1 values ('pe', 'pe', x'1000');

-- make sure constraints aren't lost due to an alter.
insert into t1 values ('pe','pe', x'01');

-- do some selects to ensure consistency of data.
select * from t1 where vc='pe';
select * from t1 where vc='pe';
alter table t1 alter vc set data type varchar(3);
select * from t1 where vc='pe';
select * from t1 where vc='pe';

-- clean up
drop table t1;
