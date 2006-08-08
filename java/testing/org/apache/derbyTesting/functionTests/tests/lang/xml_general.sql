
create table t0 (si smallint, i int, bi bigint, vcb varchar (32) for bit data, nu numeric(10,2), f float, d double, vc varchar(20), da date, ti time, ts timestamp, cl clob, bl blob);

-- XML column declarations should work like other built-in types.
create table t1 (i int, x xml);
create table t2 (i int, x xml not null);
create table t3 (i int, x xml default null);
create table t4 (vc varchar(100));
create table t5 (x2 xml not null);
alter table t5 add column x1 xml;

-- Check insertion of null XML values.
-- Next four should work.
insert into t1 values (1, null);
insert into t1 values (2, cast (null as xml));
insert into t1 (i) values (4);
insert into t1 values (3, default);
-- Next two should fail.
insert into t2 values (1, null);
insert into t2 values (2, cast (null as xml));

-- XML cols can't hold non-XML types.
insert into t1 values (3, 'hmm');
insert into t1 values (1, 2);
insert into t1 values (1, 123.456);
insert into t1 values (1, x'01');
insert into t1 values (1, x'ab');
insert into t1 values (1, current date);
insert into t1 values (1, current time);
insert into t1 values (1, current timestamp);
insert into t1 values (1, ('hmm' || 'andstuff'));

-- XML can't be stored in non-XML cols.
insert into t0 (si) values (cast (null as xml));
insert into t0 (i) values (cast (null as xml));
insert into t0 (bi) values (cast (null as xml));
insert into t0 (vcb) values (cast (null as xml));
insert into t0 (nu) values (cast (null as xml));
insert into t0 (f) values (cast (null as xml));
insert into t0 (d) values (cast (null as xml));
insert into t0 (vc) values (cast (null as xml));
insert into t0 (da) values (cast (null as xml));
insert into t0 (ti) values (cast (null as xml));
insert into t0 (ts) values (cast (null as xml));
insert into t0 (cl) values (cast (null as xml));
insert into t0 (bl) values (cast (null as xml));

-- No casting is allowed.
insert into t1 values (1, cast ('hmm' as xml));
insert into t1 values (1, cast (2 as xml));
insert into t1 values (1, cast (123.456 as xml));
insert into t1 values (1, cast (x'01' as xml));
insert into t1 values (1, cast (x'ab' as xml));
insert into t1 values (1, cast (current date as xml));
insert into t1 values (1, cast (current time as xml));
insert into t1 values (1, cast (current timestamp as xml));
insert into t1 values (1, cast (('hmm' || 'andstuff') as xml));

-- XML can't be used in non-XML operations.
select i + x from t1;
select i * x from t1;
select i / x from t1;
select i - x from t1;
select -x from t1;
select 'hi' || x from t1;
select substr(x, 0) from t1;
select i from t1 where x like 'hmm';
select max(x) from t1;
select min(x) from t1;
select length(x) from t1;

-- Comparsions against XML don't work.
select i from t1 where x = 'hmm';
select i from t1 where x > 0;
select i from t1 where x > x;
select i from t1 where x > 'some char';

-- Indexing/ordering on XML cols is not allowed.
create index oops_ix on t1(x);
select i from t1 where x is null order by x;

-- XML cols can be used in a SET clause, if target value is XML.
create trigger tr2 after insert on t1 for each row mode db2sql update t1 set x = 'hmm';
create trigger tr1 after insert on t1 for each row mode db2sql update t1 set x = null;
drop trigger tr1;

-- Test XMLPARSE operator.
-- These should fail.
insert into t1 values (1, xmlparse(document '<hmm/>' strip whitespace));
insert into t1 values (1, xmlparse(document '<hmm/>'));
insert into t1 values (1, xmlparse('<hmm/>' preserve whitespace));
insert into t1 values (1, xmlparse(content '<hmm/>' preserve whitespace));
select xmlparse(document xmlparse(document '<hein/>' preserve whitespace) preserve whitespace) from t1;
select i from t1 where xmlparse(document '<hein/>' preserve whitespace);
insert into t1 values (1, xmlparse(document '<oops>' preserve whitespace));
-- These should work.
insert into t1 values (5, xmlparse(document '<hmm/>' preserve whitespace));
insert into t1 values (6, xmlparse(document '<half> <masted> bass </masted> boosted. </half>' preserve whitespace));
insert into t2 values (1, xmlparse(document '<should> work as planned </should>' preserve whitespace));
insert into t5 (x1, x2) values (null, xmlparse(document '<notnull/>' preserve whitespace));
update t1 set x = xmlparse(document '<update> document was inserted as part of an UPDATE </update>' preserve whitespace) where i = 1;
update t1 set x = xmlparse(document '<update2> document was inserted as part of an UPDATE </update2>' preserve whitespace) where xmlexists('/update' passing by ref x);
select i from t1 where xmlparse(document '<hein/>' preserve whitespace) is not null;
select i from t1 where xmlparse(document '<hein/>' preserve whitespace) is not null order by i;

-- "is [not] null" should work with XML.
select i from t1 where x is not null;
select i from t1 where x is null;

-- XML columns can't be returned in a top-level result set.
select x from t1;
select * from t1;
select xmlparse(document vc preserve whitespace) from t4;
values xmlparse(document '<bye/>' preserve whitespace);
values xmlparse(document '<hel' || 'lo/>' preserve whitespace);

-- Test XMLSERIALIZE operator.
insert into t4 values ('<hmm/>');
insert into t4 values 'no good';
-- These should fail.
select xmlserialize(x) from t1;
select xmlserialize(x as) from t1;
select xmlserialize(x as int) from t1;
select xmlserialize(x as varchar(20) for bit data) from t1;
select xmlserialize(y as char(10)) from t1;
select xmlserialize(xmlserialize(x as clob) as clob) from t1;
values xmlserialize('<okay> dokie </okay>' as clob);
-- These should succeed.
select xmlserialize(x as clob) from t1;
select xmlserialize(x1 as clob), xmlserialize(x2 as clob) from t5;
select xmlserialize(x as char(100)) from t1;
select xmlserialize(x as varchar(300)) from t1;
-- These should succeed at the XMLEXISTS level, but fail with
-- parse/truncation errors.
select xmlserialize(xmlparse(document vc preserve whitespace) as char(10)) from t4;
select xmlserialize(x as char) from t1;
select xmlserialize(x as clob(10)) from t1;
select xmlserialize(x as char(1)) from t1;
select length(xmlserialize(x as char(1))) from t1;
select xmlserialize(x as varchar(1)) from t1;
select length(xmlserialize(x as varchar(1))) from t1;

-- These checks verify that the XMLSERIALIZE result is the correct
-- type (the type is indicated as part of the error message).
create table it (i int);
insert into it values (select xmlserialize(x as varchar(10)) from t1);
insert into it values (select xmlserialize(x as char(10)) from t1);
insert into it values (select xmlserialize(x as clob(10)) from t1);

-- Test XMLPARSE/XMLSERIALIZE combinations.
-- These should fail.
select xmlserialize(xmlparse(document '<hmm>' preserve whitespace) as clob) from t2;
select xmlserialize(xmlparse(document x preserve whitespace) as char(100)) from t1;
-- These should succeed.
select xmlserialize(xmlparse(document '<hmm/>' preserve whitespace) as clob) from t2;
select xmlserialize(xmlparse(document xmlserialize(x as clob) preserve whitespace) as clob) from t1;
values xmlserialize(xmlparse(document '<okay> dokie </okay>' preserve whitespace) as clob);
select i from t1 where xmlparse(document xmlserialize(x as clob) preserve whitespace) is not null order by i;

-- Test XMLEXISTS operator.
insert into t1 values (7, xmlparse(document '<lets> <try> this out </try> </lets>' preserve whitespace));
create table t7 (i int, x1 xml, x2 xml not null);
insert into t7 values (1, null, xmlparse(document '<ok/>' preserve whitespace));
-- These should fail.
select i from t1 where xmlexists(x);
select i from t1 where xmlexists(i);
select i from t1 where xmlexists('//*');
select i from t1 where xmlexists('//*' x);
select i from t1 where xmlexists('//*' passing x);
select i from t1 where xmlexists('//*' passing by value x);
select i from t1 where xmlexists('//*' passing by ref i);
select i from t1 where xmlexists(i passing by ref x);
select i from t1 where xmlexists(i passing by ref x, x);
-- These should succeed.
select i from t1 where xmlexists('//*' passing by ref x);
select i from t1 where xmlexists('//person' passing by ref x);
select i from t1 where xmlexists('//lets' passing by ref x);
select xmlexists('//lets' passing by ref x) from t1;
select xmlexists('//try[text()='' this out '']' passing by ref x) from t1;
select xmlexists('//let' passing by ref x) from t1;
select xmlexists('//try[text()='' this in '']' passing by ref x) from t1;
select i, xmlexists('//let' passing by ref x) from t1;
select i, xmlexists('//lets' passing by ref x) from t1;
values xmlexists('//let' passing by ref xmlparse(document '<lets> try this </lets>' preserve whitespace));
values xmlexists('//lets' passing by ref xmlparse(document '<lets> try this </lets>' preserve whitespace));
select xmlserialize(x1 as clob) from t5 where xmlexists('//*' passing by ref x1);
select xmlserialize(x2 as clob) from t5 where xmlexists('//*' passing by ref x2);
select xmlserialize(x1 as clob), xmlexists('//*' passing by ref xmlparse(document '<badboy/>' preserve whitespace)) from t5;
select xmlserialize(x1 as clob), xmlexists('//goodboy' passing by ref xmlparse(document '<badboy/>' preserve whitespace)) from t5;
select i, xmlserialize(x1 as char(10)), xmlserialize (x2 as char(10)) from t7;
select i from t7 where xmlexists('/ok' passing by ref x1) and xmlexists('/ok' passing by ref x2);
select i from t7 where xmlexists('/ok' passing by ref x1) or xmlexists('/ok' passing by ref x2);

-- XMLEXISTS can be used wherever a boolean function is allowed,
-- for ex, a check constraint...
create table t6 (i int, x xml check (xmlexists('//should' passing by ref x)));
insert into t6 values (1, xmlparse(document '<should/>' preserve whitespace));
insert into t6 values (1, xmlparse(document '<shouldnt/>' preserve whitespace));
select xmlserialize(x as char(20)) from t6;

-- Do some namespace queries/examples.
create table t8 (i int, x xml);
insert into t8 values (1, xmlparse(document '<a:hi xmlns:a="http://www.hi.there"/>' preserve whitespace));
insert into t8 values (2, xmlparse(document '<b:hi xmlns:b="http://www.hi.there"/>' preserve whitespace));
insert into t8 values (3, xmlparse(document '<a:bye xmlns:a="http://www.good.bye"/>' preserve whitespace));
insert into t8 values (4, xmlparse(document '<b:bye xmlns:b="http://www.hi.there"/>' preserve whitespace));
insert into t8 values (5, xmlparse(document '<hi/>' preserve whitespace));
select xmlexists('//child::*[name()="none"]' passing by ref x) from t8;
select xmlexists('//child::*[name()=''hi'']' passing by ref x) from t8;
select xmlexists('//child::*[local-name()=''hi'']' passing by ref x) from t8;
select xmlexists('//child::*[local-name()=''bye'']' passing by ref x) from t8;
select xmlexists('//*[namespace::*[string()=''http://www.hi.there'']]' passing by ref x) from t8;
select xmlexists('//*[namespace::*[string()=''http://www.good.bye'']]' passing by ref x) from t8;
select xmlexists('//child::*[local-name()=''hi'' and namespace::*[string()=''http://www.hi.there'']]' passing by ref x) from t8;
select xmlexists('//child::*[local-name()=''bye'' and namespace::*[string()=''http://www.good.bye'']]' passing by ref x) from t8;
select xmlexists('//child::*[local-name()=''bye'' and namespace::*[string()=''http://www.hi.there'']]' passing by ref x) from t8;

-- clean up.
drop table t0;
drop table t1;
drop table t2;
drop table t3;
drop table t4;
drop table t5;
drop table t6;
drop table t7;
drop table t8;
