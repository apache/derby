-- this also tests multiple indexes share one conglomerate if they essentially
-- are the same

autocommit off;

create table tab1 (c1 int, c2 smallint, c3 double precision, c4 varchar(30),
		   c5 varchar(1024));

insert into tab1 values (8, 12, 5.6, 'dfg', 'ghji');
insert into tab1 values (76, 2, -9.86, 'yudf', '45gd');
insert into tab1 values (-78, 45, -5.6, 'jakdsfh', 'df89g');
insert into tab1 values (56, -3, 6.7, 'dfgs', 'fds');

create index i1 on tab1 (c1, c3, c4);
create index i2 on tab1 (c1 desc, c3 desc, c4 desc);
create index i3 on tab1 (c1 desc, c3 asc, c4 desc);
create index i4 on tab1 (c2 desc, c3, c1);
create index i5 on tab1 (c1, c2 desc);

insert into tab1 values (34, 67, 5.3, 'rtgd', 'hds');
insert into tab1 values (100, 11, 9.0, '34sfg', 'ayupo');
insert into tab1 values (-100, 93, 9.1, 'egfh', 's6j');
insert into tab1 values (55, 44, -9.85, 'yudd', 'df89f');
insert into tab1 values (34, 68, 2.7, 'srg', 'iur');
insert into tab1 values (34, 66, 1.2, 'yty', 'wer');

call SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1);
maximumdisplaywidth 2500;

-- should use index i4
select c1, c3 from tab1 where c2 > 40 and c3 <= 5.3;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- should use index i5
select c2, c1 from tab1 where c2 <= 44 and c1 > 55;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- should use i1
select c1, c3, c4 from tab1 order by c1, c3;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- should use i2
select c1, c3, c4 from tab1 order by c1 desc, c3 desc, c4 desc;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- should use i3
select c1, c3, c4 from tab1 order by c1 desc, c3 asc, c4 desc;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- should use i4
select c1, c2, c3 from tab1 order by c2 desc, c3 asc;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- should use i5
select c1, c2 from tab1 order by c1, c2 desc;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- should use i4
select max(c2) from tab1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- should use i4
select min(c2) from tab1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- should use i5
select min(c2) from tab1 where c1 = 34;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- should use i5
select max(c2) from tab1 where c1 = 34;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- test if bulk insert rebuilds desc index right
call SYSCS_UTIL.SYSCS_COMPRESS_TABLE('APP', 'TAB1', 0);
select * from tab1 order by c1 desc;

-- this tests multiple indexes share one conglomerate if they essentially
-- are the same

create table tab2 (c1 int not null primary key, c2 int, c3 int);

-- not unique index, shouldn't share with primary key's index
create index i21 on tab2(c1);
-- desc index, shouldn't share with primary key's index
create index i22 on tab2(c1 desc);
-- this should share with primary key's index, and give a warning
create unique index i23 on tab2(c1);
create index i24 on tab2(c1, c3 desc);
-- this should share with i24's conglomerate
create index i25 on tab2(c1, c3 desc);
-- no share
create index i26 on tab2(c1, c3);
insert into tab2 values (6, 2, 8), (2, 8, 5), (28, 5, 9), (3, 12, 543);
create index i27 on tab2 (c1, c2 desc, c3);
-- no share
create index i28 on tab2 (c1, c2 desc, c3 desc);
-- share with i27
create index i29 on tab2 (c1, c2 desc, c3);
create index i20 on tab2 (c1, c2 desc, c3);

insert into tab2 values (56, 2, 7), (31, 5, 7), (-12, 5, 2);

select count(distinct conglomeratenumber) from sys.sysconglomerates
	where tableid = (select tableid from sys.systables
						where tablename = 'TAB2');

select * from tab2;

values SYSCS_UTIL.SYSCS_CHECK_TABLE('APP', 'TAB2');

-- see if rebuild indexes correctly
call SYSCS_UTIL.SYSCS_COMPRESS_TABLE('APP', 'TAB2', 0);

select count(distinct conglomeratenumber) from sys.sysconglomerates
	where tableid = (select tableid from sys.systables
						where tablename = 'TAB2');

select * from tab2;

update tab2 set c2 = 11 where c3 = 7;
select * from tab2;

delete from tab2 where c2 > 10 and c2 < 12;
select * from tab2;

values SYSCS_UTIL.SYSCS_CHECK_TABLE('APP', 'TAB2');

-- drop indexes
drop index i22;
drop index i24;
drop index i26;
drop index i28;
drop index i20;

select count(distinct conglomeratenumber) from sys.sysconglomerates
	where tableid = (select tableid from sys.systables
						where tablename = 'TAB2');

call SYSCS_UTIL.SYSCS_COMPRESS_TABLE('APP', 'TAB2', 0);
select * from tab2;

values SYSCS_UTIL.SYSCS_CHECK_TABLE('APP', 'TAB2');

drop index i21;
drop index i23;
drop index i25;
drop index i27;
drop index i29;

select count(distinct conglomeratenumber) from sys.sysconglomerates
	where tableid = (select tableid from sys.systables
						where tablename = 'TAB2');

values SYSCS_UTIL.SYSCS_CHECK_TABLE('APP', 'TAB2');

-- beetle 4974

create table b4974 (a BIGINT, b BIGINT, c INT, d CHAR(16), e BIGINT);
create index i4974 on b4974(a, d, c, e);
SELECT b from b4974 t1
where (T1.a = 10127 or T1.a = 0)
	and (T1.d = 'ProductBean' or T1.d = 'CatalogEntryBean')
	and (T1.e =0 or T1.e = 0);

rollback;
