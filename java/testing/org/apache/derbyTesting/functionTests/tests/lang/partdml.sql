--
-- Test partial row access for update and delete
--
maximumdisplaywidth 2000;

drop function getScanCols;
drop table basic;
drop table p;

CREATE FUNCTION getScanCols(value VARCHAR(32672)) 
       RETURNS VARCHAR (32672) EXTERNAL NAME 'org.apache.derbyTesting.functionTests.util.StatParser.getScanCols' 
       LANGUAGE JAVA PARAMETER STYLE JAVA NO SQL;

create table p (ccharForBitData char(1) for bit data not null, cdec dec(6,2) not null, unindexed smallint, cchar char(10) not null, 
		constraint pk1 primary key (cchar, ccharForBitData), constraint pk2 unique (cdec));

insert into p values (x'00', 0.0, 11, '00');
insert into p values (x'11', 1.1, 22, '11');
insert into p values (x'22', 2.2, 33, '22');
insert into p values (x'33', 3.3, 44, '33');

create table basic (cint int, cchar char(10), 
		ctime time, cdec dec(6,2), 
		ccharForBitData char(1) for bit data, unindexed int);

create index b1 on basic (cchar, ccharForBitData, cint);
create index b2 on basic (ctime);
create index b3 on basic (ctime, cint);
create index b4 on basic (cint);

maximumdisplaywidth 200;
call SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1);

-- the extra 33s are so we can ensure we'll use an index when looking for the others
insert into basic values (11, '11', TIME('11:11:11'), 1.1, x'11', 11);
insert into basic values (22, '22', TIME('22:22:22'), 2.2, x'22', 22);
insert into basic values (33, '33', TIME('03:33:33'), 3.3, x'33', 33);
insert into basic values (33, '33', TIME('03:33:33'), 3.3, x'33', 33);
insert into basic values (33, '33', TIME('03:33:33'), 3.3, x'33', 33);
insert into basic values (33, '33', TIME('03:33:33'), 3.3, x'33', 33);
insert into basic values (33, '33', TIME('03:33:33'), 3.3, x'33', 33);
insert into basic values (33, '33', TIME('03:33:33'), 3.3, x'33', 33);
insert into basic values (33, '33', TIME('03:33:33'), 3.3, x'33', 33);
insert into basic values (33, '33', TIME('03:33:33'), 3.3, x'33', 33);
insert into basic values (33, '33', TIME('03:33:33'), 3.3, x'33', 33);
insert into basic values (33, '33', TIME('03:33:33'), 3.3, x'33', 33);

-- simple update of each column
update basic set cint = cint;
values getScanCols(SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS());

update basic set cchar = cchar;
values getScanCols(SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS());

update basic set ctime = ctime;
values getScanCols(SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS());

update basic set cdec = cdec;
values getScanCols(SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS());

update basic set ccharForBitData = ccharForBitData;
values getScanCols(SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS());

update basic set unindexed = unindexed;
values getScanCols(SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS());

-- confirm the table is ok
select ccharForBitData, ctime, cdec, cint, cchar from basic;
values (SYSCS_UTIL.SYSCS_CHECK_TABLE('APP', 'BASIC'));

update basic set cint = cint where cint = 11;
values getScanCols(SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS());

update basic set cchar = cchar where cint = 11;
values getScanCols(SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS());

update basic set ctime = ctime where cint = 11;
values getScanCols(SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS());

update basic set ctime = ctime where cint = 11;
values getScanCols(SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS());

update basic set cdec = cdec where cint = 11;
values getScanCols(SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS());

update basic set ccharForBitData = ccharForBitData where cint = 11;
values getScanCols(SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS());

-- confirm the table is ok
select ccharForBitData, ctime, cdec, cint, cchar from basic;
values (SYSCS_UTIL.SYSCS_CHECK_TABLE('APP', 'BASIC'));

update basic set cint = cint where ccharForBitData = x'11';
values getScanCols(SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS());

update basic set cchar = cchar where ccharForBitData = x'11';
values getScanCols(SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS());

update basic set ctime = ctime where ccharForBitData = x'11';
values getScanCols(SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS());

update basic set cdec = cdec where ccharForBitData = x'11';
values getScanCols(SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS());

update basic set ccharForBitData = ccharForBitData where ccharForBitData = x'11';
values getScanCols(SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS());

-- confirm the table is ok
select ccharForBitData, ctime, cdec, cint, cchar from basic;
values (SYSCS_UTIL.SYSCS_CHECK_TABLE('APP', 'BASIC'));

autocommit off;

update basic set cdec = cint;
values getScanCols(SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS());

-- confirm the table is ok
select ccharForBitData, ctime, cdec, cint, cchar from basic;
values (SYSCS_UTIL.SYSCS_CHECK_TABLE('APP', 'BASIC'));

rollback;

update basic set cchar = cchar where cdec = 3.3 and ctime = TIME('03:33:33');
values getScanCols(SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS());

update basic set ctime = ctime, cchar = cchar, cint = cint, cdec = cdec, ccharForBitData = ccharForBitData;
values getScanCols(SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS());

-- confirm the table is ok
select ccharForBitData, ctime, cdec, cint, cchar from basic;
values (SYSCS_UTIL.SYSCS_CHECK_TABLE('APP', 'BASIC'));

--------------------------------------------------------------------------
-- deletes
--------------------------------------------------------------------------

--
-- index scans
--
delete from basic where cchar = '22';
values getScanCols(SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS());
rollback;

delete from basic where cint = 22;
values getScanCols(SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS());
rollback;

delete from basic where ctime = TIME('22:22:22');
values getScanCols(SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS());
rollback;

delete from basic where ccharForBitData = x'22';
values getScanCols(SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS());
rollback;

delete from basic where cdec = 2.2;
values getScanCols(SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS());
rollback;

--
-- index row to base row 
--
delete from basic where cchar = '22' and unindexed = 22;
values getScanCols(SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS());
rollback;

delete from basic where cint = 22 and unindexed = 22;
values getScanCols(SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS());
rollback;

delete from basic where ctime = TIME('22:22:22') and unindexed = 22;
values getScanCols(SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS());
rollback;

delete from basic where ccharForBitData = x'22' and unindexed = 22;
values getScanCols(SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS());
rollback;

delete from basic where cdec = 2.2 and unindexed = 22;
values getScanCols(SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS());
rollback;


--
-- table scans
--
delete from basic where cchar > '00';
values getScanCols(SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS());
rollback;

delete from basic where cint > 1;
values getScanCols(SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS());
rollback;

delete from basic where ctime > TIME('00:00:01');
values getScanCols(SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS());
rollback;

delete from basic where ccharForBitData > x'11';
values getScanCols(SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS());
rollback;

delete from basic where cdec > 2;
values getScanCols(SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS());
rollback;

delete from basic where unindexed = 22;
values getScanCols(SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS());
rollback;

--
-- some checks on deferred deletes
--
delete from basic where unindexed = (select min(cint) from basic);
values getScanCols(SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS());
rollback;


delete from basic where cint = (select min(cint) from basic);
values getScanCols(SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS());
rollback;

delete from basic where cdec = (select min(cdec) from basic);
values getScanCols(SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS());
rollback;

delete from basic where cdec = 1.1 and cchar = (select min(cchar) from basic);
values getScanCols(SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS());
rollback;


--
-- quickly confirm that we get all columns for updateable cursors
--
get cursor c as 'select cint from basic for update';
next c;

----------------------------------------------------------------- 
-- now lets try some constraints
----------------------------------------------------------------- 

--
-- check constraints
--
alter table basic add constraint ck check (unindexed > cdec);
commit;

update basic set unindexed = unindexed where cint = 11;
values getScanCols(SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS());
rollback;

update basic set unindexed = unindexed;
values getScanCols(SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS());
rollback;

update basic set cdec = cdec where cint = 11;
values getScanCols(SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS());
rollback;

delete from basic where cint = 11;
values getScanCols(SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS());
rollback;


-- one that isn't affected by contstraint
update basic set ctime = ctime;
values getScanCols(SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS());
rollback;

-- confirm it is working ok
update basic set unindexed = 0;
rollback;


--
-- foreign keys
--
alter table basic add constraint fk1 foreign key (cchar, ccharForBitData) references p;
commit;

update basic set cchar = cchar, ccharForBitData = ccharForBitData;
values getScanCols(SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS());

update basic set cchar = cchar;
values getScanCols(SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS());

update basic set ccharForBitData = ccharForBitData;
values getScanCols(SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS());
rollback;

--pk update

-- only this update should fail, does not satisfy fk1
update p set ccharForBitData = x'22', cchar = CAST(unindexed as CHAR(10));
values getScanCols(SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS());
rollback;

update p set cdec = cdec + 1.1;
values getScanCols(SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS());
rollback;

update p set unindexed = 666, cchar = 'fail';
rollback;

-- only this update should fail, does not satisfy fk1
update p set ccharForBitData = x'66' where ccharForBitData = x'22';
values getScanCols(SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS());
rollback;


alter table basic add constraint fk2 foreign key (cdec) references p(cdec);
commit;

update p set cdec = cdec + 1.1;
values getScanCols(SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS());
rollback;

update basic set cdec = cdec, cint = cint, ccharForBitData = ccharForBitData, cchar = cchar;
values getScanCols(SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS());
rollback;

update basic set cdec = cdec+1.1, cint = cint, ccharForBitData = ccharForBitData, cchar = cchar;
rollback;

delete from p where cdec = 1.1;
rollback;

-- clean up
drop function getScanCols;
drop table basic;
drop table p;
