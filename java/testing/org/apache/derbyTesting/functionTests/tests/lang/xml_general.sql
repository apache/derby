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

-- XML cannot be imported or exported.  These should all fail.
CALL SYSCS_UTIL.SYSCS_EXPORT_TABLE (
  null, 'T1', 'xmlexport.del', null, null, null);
CALL SYSCS_UTIL.SYSCS_EXPORT_QUERY(
  'select x from t1', 'xmlexport.del', null, null, null);
CALL SYSCS_UTIL.SYSCS_EXPORT_QUERY (
  'select xmlserialize(x as clob) from t1',
  'xmlexport.del', null, null, null); 
CALL SYSCS_UTIL.SYSCS_IMPORT_TABLE (
  null, 'T1', 'shouldntmatter.del', null, null, null, 0);
CALL SYSCS_UTIL.SYSCS_IMPORT_DATA (
  NULL, 'T1', null, '2', 'shouldntmatter.del', null, null, null,0);

-- XML cannot be used with procedures/functions.
create procedure hmmproc (in i int, in x xml)
  parameter style java language java external name 'hi.there';
create function hmmfunc (i int, x xml) returns int
  parameter style java language java external name 'hi.there';

-- XML columns cannot be used for global temporary tables.
declare global temporary table SESSION.xglobal (myx XML)
  not logged on commit preserve rows;

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
prepare ps1 as 'insert into t1(x) values XMLPARSE(document ? preserve whitespace)';
-- These should work.
insert into t1 values (5, xmlparse(document '<hmm/>' preserve whitespace));
insert into t1 values (6, xmlparse(document '<half> <masted> bass </masted> boosted. </half>' preserve whitespace));
insert into t2 values (1, xmlparse(document '<should> work as planned </should>' preserve whitespace));
insert into t5 (x1, x2) values (null, xmlparse(document '<notnull/>' preserve whitespace));
insert into t1 values (7, xmlparse(document '<?xml version="1.0" encoding= "UTF-8"?><umm> decl check </umm>' preserve whitespace));
update t1 set x = xmlparse(document '<update> document was inserted as part of an UPDATE </update>' preserve whitespace) where i = 1;
update t1 set x = xmlparse(document '<update2> document was inserted as part of an UPDATE </update2>' preserve whitespace) where xmlexists('/update' passing by ref x);
select i from t1 where xmlparse(document '<hein/>' preserve whitespace) is not null;
select i from t1 where xmlparse(document '<hein/>' preserve whitespace) is not null order by i;
prepare ps1 as 'insert into t3(i, x) values (0, XMLPARSE(document cast (? as CLOB) preserve whitespace))';
execute ps1 using 'values ''<ay>caramba</ay>''';

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
values xmlexists('//lets/@doit' passing by ref xmlparse(document '<lets doit="true"> try this </lets>' preserve whitespace));
values xmlexists('//lets/@dot' passing by ref xmlparse(document '<lets doit="true"> try this </lets>' preserve whitespace));
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

-- Test XMLQUERY operator.

-- These should fail w/ syntax errors.
select i, xmlquery('//*') from t1;
select i, xmlquery('//*' passing) from t1;
select i, xmlquery('//*' passing by ref x) from t1;
select i, xmlquery('//*' passing by ref x returning sequence) from t1;
select i, xmlquery(passing by ref x empty on empty) from t1;
select i, xmlquery(xmlquery('//*' returning sequence empty on empty) as char(75)) from t1;

-- These should fail with "not supported" errors.
select i, xmlquery('//*' passing by ref x returning sequence null on empty) from t1;
select i, xmlquery('//*' passing by ref x returning content empty on empty) from t1;

-- This should fail because XMLQUERY returns an XML value which
-- is not allowed in top-level result set.
select i, xmlquery('//*' passing by ref x empty on empty) from t1;

-- These should fail because context item must be XML.
select i, xmlquery('//*' passing by ref i empty on empty) from t1;
select i, xmlquery('//*' passing by ref 'hello' empty on empty) from t1;
select i, xmlquery('//*' passing by ref cast ('hello' as clob) empty on empty) from t1;

-- This should fail because the function is not recognized by Xalan.
-- The failure should be an error from Xalan saying what the problem
-- is; it should *NOT* be a NPE, which is what we were seeing before
-- DERBY-688 was completed.
select i,
  xmlserialize(
    xmlquery('data(//@*)' passing by ref x returning sequence empty on empty)
  as char(70))
from t1;

-- These should all succeed.  Since it's Xalan that's actually doing
-- the query evaluation we don't need to test very many queries; we
-- just want to make sure we get the correct results when there is
-- an empty sequence, when the xml context is null, and when there
-- is a sequence with one or more nodes/items in it.  So we just try
-- out some queries and look at the results.  The selection of queries
-- is random and is not meant to be exhaustive.

select i,
  xmlserialize(
    xmlquery('2+2' passing by ref x returning sequence empty on empty)
  as char(70))
from t1;

select i,
  xmlserialize(
    xmlquery('./notthere' passing by ref x returning sequence empty on empty)
  as char(70))
from t1;

select i,
  xmlserialize(
    xmlquery('//*' passing by ref x empty on empty)
  as char(70))
from t1;

select i,
  xmlserialize(
    xmlquery('//*[text() = " bass "]' passing by ref x empty on empty)
  as char(70))
from t1;

select i,
  xmlserialize(
    xmlquery('//lets' passing by ref x empty on empty)
  as char(70))
from t1;

select i,
  xmlserialize(
    xmlquery('//text()' passing by ref x empty on empty)
  as char(70))
from t1;

select i,
  xmlserialize(
    xmlquery('//try[text()='' this out '']' passing by ref x empty on empty)
  as char(70))
from t1;

select i,
  xmlserialize(
    xmlquery('//try[text()='' this in '']' passing by ref x empty on empty)
  as char(70))
from t1;

select i,
  xmlserialize(
    xmlquery('2+.//try' passing by ref x returning sequence empty on empty)
  as char(70))
from t1;

values xmlserialize(
  xmlquery('//let' passing by ref
    xmlparse(document '<lets> try this </lets>' preserve whitespace)
  empty on empty)
as char(30));

values xmlserialize(
  xmlquery('//lets' passing by ref
    xmlparse(document '<lets> try this </lets>' preserve whitespace)
  empty on empty)
as char(30));

-- Check insertion of XMLQUERY result into a table.  Should only allow
-- results that are a sequence of exactly one Document node.

insert into t1 values (
  9,
  xmlparse(document '<here><is><my height="4.4">attribute</my></is></here>' preserve whitespace)
);

insert into t3 values (
  0,
  xmlparse(document '<there><goes><my weight="180">attribute</my></goes></there>' preserve whitespace)
);

-- Show target tables before insertions.

select i, xmlserialize(x as char(75)) from t2;
select i, xmlserialize(x as char(75)) from t3;

-- These should all fail because the result of the XMLQUERY op is
-- not a valid document (it's either an empty sequence, a node that is
-- not a Document node, some undefined value, or a sequence with more
-- than one item in it).

insert into t2 (i, x) values (
  20, 
  (select
    xmlquery('./notthere' passing by ref x returning sequence empty on empty)
    from t1 where i = 9
  )
);

insert into t2 (i, x) values (
  21,
  (select
    xmlquery('//@*' passing by ref x returning sequence empty on empty)
    from t1 where i = 9
  )
);

insert into t2 (i, x) values (
  22,
  (select
    xmlquery('. + 2' passing by ref x returning sequence empty on empty)
    from t1 where i = 9
  )
);

insert into t2 (i, x) values (
  23,
  (select
    xmlquery('//*' passing by ref x returning sequence empty on empty)
    from t1 where i = 9
  )
);

insert into t2 (i, x) values (
  24,
  (select
    xmlquery('//*[//@*]' passing by ref x returning sequence empty on empty)
    from t1 where i = 9
  )
);

insert into t2 (i, x) values (
  25,
  (select
    xmlquery('//is' passing by ref x returning sequence empty on empty)
    from t1 where i = 9
  )
);

insert into t2 (i, x) values (
  26,
  (select
    xmlquery('//*[@*]' passing by ref x returning sequence empty on empty)
    from t1 where i = 9
  )
);

-- These should succeed.

insert into t2 (i, x) values (
  27,
  (select
    xmlquery('.' passing by ref x returning sequence empty on empty)
    from t1 where i = 9
  )
);

insert into t2 (i, x) values (
  28,
  (select
    xmlquery('/here/..' passing by ref x returning sequence empty on empty)
    from t1 where i = 9
  )
);

-- Verify results.
select i, xmlserialize(x as char(75)) from t2;

-- Next two should _both_ succeed because there's no row with i = 100
-- in t1, thus the SELECT will return null and XMLQuery operator should
-- never get executed.  x will be NULL in these cases.

insert into t3 (i, x) values (
  29,
  (select
    xmlquery('2+2' passing by ref x returning sequence empty on empty)
    from t1 where i = 100
  )
);

insert into t3 (i, x) values (
  30,
  (select
    xmlquery('.' passing by ref x returning sequence empty on empty)
    from t1 where i = 100
  )
);

-- Verify results.
select i, xmlserialize(x as char(75)) from t3;

-- Check updates using XMLQUERY results.  Should only allow results
-- that constitute a valid DOCUMENT node (i.e. that can be parsed
-- by the XMLPARSE operator).

-- These should succeed.

update t3
  set x = 
    xmlquery('.' passing by ref
      xmlparse(document '<none><here/></none>' preserve whitespace)
    returning sequence empty on empty)
where i = 29;

update t3
  set x = 
    xmlquery('self::node()[//@height]' passing by ref
      (select
        xmlquery('.' passing by ref x empty on empty)
        from t1
        where i = 9
      )
    empty on empty)
where i = 30;

-- These should fail because result of XMLQUERY isn't a DOCUMENT.
update t3
  set x = xmlquery('.//*' passing by ref x empty on empty)
where i = 29;

update t3
  set x = xmlquery('./notthere' passing by ref x empty on empty)
where i = 30;

update t3
  set x =
    xmlquery('//*[@weight]' passing by ref
      (select
        xmlquery('.' passing by ref x empty on empty)
        from t1
        where i = 9
      )
    empty on empty)
where i = 30;

update t3
  set x =
    xmlquery('//*/@height' passing by ref
      (select
        xmlquery('.' passing by ref x empty on empty)
        from t1
        where i = 9
      )
    empty on empty)
where i = 30;

-- Next two should succeed because there's no row with i = 100
-- in t3 and thus t3 should remain unchanged after these updates.

update t3
  set x = xmlquery('//*' passing by ref x empty on empty)
where i = 100;

update t3
  set x = xmlquery('4+4' passing by ref x empty on empty)
where i = 100;

-- Verify results.
select i, xmlserialize(x as char(75)) from t3;

-- Pass results of an XMLQUERY op into another XMLQUERY op.
-- Should work so long as results of the first op constitute
-- a valid document.

-- Should fail because result of inner XMLQUERY op isn't a valid document.

select i,
  xmlserialize(
    xmlquery('//lets/@*' passing by ref
      xmlquery('/okay/text()' passing by ref
        xmlparse(document '<okay><lets boki="inigo"/></okay>' preserve whitespace)
      empty on empty)
    empty on empty)
  as char(100))
from t1 where i > 5;

select i,
  xmlserialize(
    xmlquery('.' passing by ref
      xmlquery('//lets' passing by ref
        xmlparse(document '<okay><lets boki="inigo"/></okay>' preserve whitespace)
      empty on empty)
    empty on empty)
  as char(100))
from t1 where i > 5;

select i,
  xmlexists('.' passing by ref
    xmlquery('/okay' passing by ref
      xmlparse(document '<okay><lets boki="inigo"/></okay>' preserve whitespace)
    empty on empty)
  )
from t1 where i > 5;

-- Should succeed but result is empty sequence.

select i,
  xmlserialize(
    xmlquery('/not' passing by ref
      xmlquery('.' passing by ref
        xmlparse(document '<okay><lets boki="inigo"/></okay>' preserve whitespace)
      empty on empty)
    empty on empty)
  as char(100))
from t1 where i > 5;

-- Should succeed with various results.

select i,
  xmlserialize(
    xmlquery('//lets' passing by ref
      xmlquery('.' passing by ref
        xmlparse(document '<okay><lets boki="inigo"/></okay>' preserve whitespace)
      empty on empty)
    empty on empty)
  as char(100))
from t1 where i > 5;

select i,
  xmlserialize(
    xmlquery('string(//@boki)' passing by ref
      xmlquery('/okay/..' passing by ref
        xmlparse(document '<okay><lets boki="inigo"/></okay>' preserve whitespace)
      empty on empty)
    empty on empty)
  as char(100))
from t1 where i > 5;

select i,
  xmlserialize(
    xmlquery('/half/masted/text()' passing by ref
      xmlquery('.' passing by ref x empty on empty)
    empty on empty)
  as char(100))
from t1 where i = 6;

select i,
  xmlexists('/half/masted/text()' passing by ref
    xmlquery('.' passing by ref x empty on empty)
  )
from t1 where i = 6;

-- DERBY-1759: Serialization of attribute nodes.

-- Add a test row to t1.
insert into t1 values (10,
  xmlparse(document
    '<threeatts first="1" second="two" third="le 3 trois"/>'
    preserve whitespace
  ));

-- Echo t1 rows for reference.
select i, xmlserialize(x as char(75)) from t1;

-- This should fail because XML serialization dictates that
-- we throw an error if an attempt is made to serialize a
-- sequence that has one or more top-level attributes nodes.
select
  xmlserialize(
    xmlquery(
      '//@*' passing by ref x empty on empty
    )
  as char(50))
from t1
where xmlexists('//@*' passing by ref x);

-- Demonstrate that Xalan "string" function only returns
-- string value of first attribute and thus cannot be
-- used to retrieve a sequence of att values.
select
  xmlserialize(
    xmlquery(
      'string(//@*)'
      passing by ref x empty on empty
    )
  as char(50))
from t1
where xmlexists('//@*' passing by ref x);

-- Xalan doesn't have a function that allows retrieval of a
-- sequence of attribute values.  One can only retrieve a
-- sequence of attribute *nodes*, but since those can't be
-- serialized (because of SQL/XML rules) the user has no
-- way to get them.  The following is a very (VERY) ugly
-- two-part workaround that one could use until something
-- better is available.  First, get the max number of
-- attributes in the table.
select
  max(
    cast(
      xmlserialize(
        xmlquery('count(//@*)' passing by ref x empty on empty)
      as char(50))
    as int)
  )
from t1; 

-- Then use XPath position syntax to retrieve the attributes
-- and concatenate them.  We need one call to string(//@[i])
-- for every for every i between 1 and the value found in the
-- preceding query.  In this case we know the max is three,
-- so use that.
select
  xmlserialize(
    xmlquery(
      'concat(string(//@*[1]), " ",
        string(//@*[2]), " ",
        string(//@*[3]))'
      passing by ref x empty on empty
    )
  as char(50))
from t1
where xmlexists('//@*' passing by ref x);

-- DERBY-1718
-- create trigger fails when SPS contains XML related op.
create table t9 (i int, x xml);
create table t10 (i int, x xml);

insert into t9 values (1, xmlparse(document '<name> john </name>' preserve whitespace));
create trigger tx after insert on t9 for each statement mode db2sql
   insert into t10 values (1, xmlparse(document '<name> jane </name>' preserve whitespace));
insert into t9 values (2, xmlparse(document '<name> ally </name>' preserve whitespace));
select i, xmlserialize(x as varchar(20)) from t9;
select i, xmlserialize(x as varchar(20)) from t10;
insert into t9 select * from t9;
select i, xmlserialize(x as varchar(20)) from t9;
select i, xmlserialize(x as varchar(20)) from t10;
drop trigger tx;
delete from t9;
delete from t10;
insert into t9 values (1, xmlparse(document '<name> john </name>' preserve whitespace));
create trigger tx after insert on t9 for each statement mode db2sql
   insert into t10 values (1, (select xmlquery('.' passing by ref x returning sequence empty on empty) from t9 where i = 1));
insert into t9 values (2, xmlparse(document '<name> ally </name>' preserve whitespace));
select i, xmlserialize(x as varchar(20)) from t9;
select i, xmlserialize(x as varchar(20)) from t10;  
drop trigger tx;
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
drop table t9;
drop table t10;