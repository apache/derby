connect 'jdbc:derby:jonas;create=true';

----- TEST TO RUN IN DB2 COMPATIBLITY MODE
--

create table t1(c11 int);
insert into t1 values(1);

-- equal tests are allowed only for BLOB==BLOB
select c11 from t1 where cast(x'1111' as blob(5))=cast(x'1111' as blob(5));
select c11 from t1 where cast(x'1111' as blob(5))=cast(x'1111' as blob(7));
select c11 from t1 where cast(x'1110' as blob(5))=cast(x'1110' as blob(7));
select c11 from t1 where cast(x'1111' as blob(5))=cast(x'11100000' as blob(7));
select c11 from t1 where cast(x'1111' as blob(5))=cast(x'1110000000' as blob(7));

select c11 from t1 where x'11' = cast(x'11' as blob(1));
select c11 from t1 where cast(x'11' as blob(1)) = x'11';
select c11 from t1 where cast(x'11' as blob(1)) = cast(x'11' as blob(1));

select c11 from t1 where '1' = cast('1' as clob(1));
select c11 from t1 where cast('1' as clob(1)) = '1';
select c11 from t1 where cast('1' as clob(1)) = cast('1' as clob(1));

select c11 from t1 where '1' = cast('1' as nclob(1));
select c11 from t1 where cast('1' as nclob(1)) = '1';
select c11 from t1 where cast('1' as nclob(1)) = cast('1' as nclob(1));

-- NCLOB is comparable with CLOB

select c11 from t1 where cast('1' as nclob(10)) = cast('1' as clob(10));
select c11 from t1 where cast('1' as clob(10)) = cast('1' as nclob(10));

drop table b;
drop table c;
drop table n;

create table b(blob blob(3K));
create table c(clob clob(2M));
create table n(nclob nclob(1G));

insert into b values(cast(X'0031' as blob(3K)));
insert into c values(cast('2' as clob(2M)));
insert into n values(cast('3' as nclob(1G)));

insert into b values(cast(X'0031' as blob(3K)));
insert into c values(cast('2' as clob(2M)));
insert into n values(cast('3' as nclob(1G)));

insert into b values(cast(X'0031' as blob(3K)));
insert into c values(cast('2' as clob(2M)));
insert into n values(cast('3' as nclob(1G)));

select blob from b;
select clob from c;
select nclob from n;

-- comparsion using tables
select * from b as b1, b as b2 where b1.blob=b2.blob;
select * from b as b1, b as b2 where b1.blob!=b2.blob;

select * from b as b1, b as b2 where b1.blob=x'0001';
select * from b as b1, b as b2 where x'0001'=b1.blob;
select * from b as b1, b as b2 where x'0001'!=b1.blob;

select * from b as b1, b as b2 where b1.blob=X'7575';
select * from b as b1, b as b2 where X'7575'=b1.blob;

select c.clob from c where c.clob = '2';
select n.nclob from n where n.nclob = '3';

-- ORDER tests on LOB types (not allowed)
select c11 from t1 where cast(x'1111' as blob(5))=cast(x'1111' as blob(5));
select c11 from t1 where cast(x'1111' as blob(5))!=cast(x'1111' as blob(5));
select c11 from t1 where cast(x'1111' as blob(5))<cast(x'1111' as blob(5));
select c11 from t1 where cast(x'1111' as blob(5))>cast(x'1111' as blob(7));
select c11 from t1 where cast(x'1111' as blob(5))<=cast(x'1110' as blob(7));
select c11 from t1 where cast(x'1111' as blob(5))>=cast(x'11100000' as blob(7));

select c11 from t1 where cast('fish' as clob(5))=cast('fish' as clob(5));
select c11 from t1 where cast('fish' as clob(5))!=cast('fish' as clob(5));
select c11 from t1 where cast('fish' as clob(5))<cast('fish' as clob(5));
select c11 from t1 where cast('fish' as clob(5))>cast('fish' as clob(7));
select c11 from t1 where cast('fish' as clob(5))<=cast('fish' as clob(7));
select c11 from t1 where cast('fish' as clob(5))>=cast('fish' as clob(7));

select c11 from t1 where cast('fish' as nclob(5))=cast('fish' as nclob(5));
select c11 from t1 where cast('fish' as nclob(5))!=cast('fish' as nclob(5));
select c11 from t1 where cast('fish' as nclob(5))<cast('fish' as nclob(5));
select c11 from t1 where cast('fish' as nclob(5))>cast('fish' as nclob(7));
select c11 from t1 where cast('fish' as nclob(5))<=cast('fish' as nclob(7));
select c11 from t1 where cast('fish' as nclob(5))>=cast('fish' as nclob(7));

-- BIT STRING literal is not allowed in DB2
values cast(B'1' as blob(10));
