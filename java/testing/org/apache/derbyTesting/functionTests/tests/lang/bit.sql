--
-- Test the builtin type 'bit'
-- Specifically the base-16, hex bit literal

-- stupid test of literals
values(X'aAff');

-- casting to a for bit data type
values (cast (x'ee' as char(2) for bit data));
values x'aAff' || (cast (x'ee' as char(2) for bit data));

-- are the search conditions true?
create table tab1 (c1 char(25));
insert into tab1 values 'search condition is true';
select * from tab1 where ((X'1010' || X'0011' || X'0100') = X'101000110100');
select * From tab1 where ((X'1010' || X'0011' || X'0100') = X'101000110100');
select * from tab1 where (X'1100' > X'0011');
drop table tab1;

-- simple negative test
values(X'gg');
values(X'z');
values(X'zz');
-- fails after bug 5742 is fixed
values(X'9');

-- some quick tests of the length function
-- # bits in a string expression
values({fn length(X'ab')} * 8);
values({fn length(X'11')} * 8);
-- # characters in a string expression
values({fn length(X'ab')});
values({fn length(X'11')});
-- # octets in a string expression
values({fn length(X'ab')});
values({fn length(X'11')});

-- stupid test for syntax
create table t1 (b1 char for bit data, b2 char(2) for bit data, b3 varchar(2) for bit data, b4 LONG VARCHAR FOR BIT DATA,
				b5 LONG VARCHAR FOR BIT DATA, b6 LONG VARCHAR FOR BIT DATA);
drop table t1;
create table t1 (b1 char for bit data, b2 char(1) for bit data not null, b3 varchar(1) for bit data not null,
				b4 LONG VARCHAR FOR BIT DATA not null, b5 LONG VARCHAR FOR BIT DATA not null,
				b6 LONG VARCHAR FOR BIT DATA not null);
drop table t1;

create table t (i int, s smallint, c char(10), v varchar(50),
	d double precision, r real, b char (2) for bit data, bv varchar(8) for bit data,
	lbv LONG VARCHAR FOR BIT DATA);

-- explicit null
insert into t values (null, null, null, null, null, null, null, null, null);

-- implicit null
insert into t (i) values (null);
select b, bv, lbv from t;

-- sample data
insert into t values (0, 100, 'hello', 'everyone is here', 200.0e0, 200.0e0, 
			X'12af', X'0000111100001111', X'abc123');
insert into t values (-1, -100, 'goodbye', 'everyone is there', -200.0e0, -200.0e0,
			X'0000', X'', X'10101010');

-- truncation -- should get an error
insert into t (b, bv) values (X'ffffffff', X'ffffffff');
select b, bv, lbv from t;

-- padding -- will be warning, some day (not now)
insert into t (b, bv) values (X'01', X'01');
insert into t (b, bv) values (X'', X'');

select b, bv from t;
drop table t;

--
-- simple comparisons
-- returns 1 if the search conditions are true
-- 
create table nulltab (b char(1) for bit data);
insert into nulltab values (null);

select 1 from nulltab where X'0001' > X'0000';
select 1 from nulltab where X'0100' > X'0001';
select 1 from nulltab where X'ff00' > X'00ff';
select 1 from nulltab where X'0100' > X'0100';
select 1 from nulltab where X'0100' > b;

select 1 from nulltab where X'0001' >= X'0000';
select 1 from nulltab where X'0100' >= X'0001';
select 1 from nulltab where X'ff00' >= X'00ff';
select 1 from nulltab where X'0100' >= b;

select 1 from nulltab where X'0001' < X'0000';
select 1 from nulltab where X'0100' < X'0001';
select 1 from nulltab where X'ff00' < X'00ff';
select 1 from nulltab where X'0100' < b;

select 1 from nulltab where X'0001' <= X'0000';
select 1 from nulltab where X'0100' <= X'0001';
select 1 from nulltab where X'ff00' <= X'00ff';
select 1 from nulltab where X'0100' <= b;

drop table nulltab;

--
-- select comparisons
--

create table t (b10 char(20) for bit data, vb10 varchar(20) for bit data, b16 char(2) for bit data, vb16 varchar(2) for bit data, lbv LONG VARCHAR FOR BIT DATA, c20 char(20), cv20 varchar(20));
insert into t values (null, null, null, null, null, 'null', 'null columns');
insert into t values (X'',  X'',  X'',  X'', X'', '0', 'zero length column');
insert into t values (X'0000000001', X'0000000001', X'01', X'01', X'0000000001', '1', '1');
insert into t values (X'0000000011', X'0000000011', X'03', X'03', X'03', '3', '3');
insert into t values (X'1111111111', X'1111111111', X'ff', X'ff', X'1111111111', 'ff', 'ff');
insert into t values (X'11', X'11', X'aa', X'aa', X'aa', 'aa', 'aa');

-- make sure built-in functions work ok on binary types,
-- it is a little special since it maps to an
-- array.  use length to make sure it wont
-- diff from run to run
select {fn length(cast(b10 as char(10)))} from t where b10 is not null;
select {fn length(cast(vb10 as char(10)))} from t where vb10 is not null;
select {fn length(cast(lbv as char(10)))} from t where vb10 is not null;

select b10, c20, cv20 from t order by b10 asc;
select b10, c20, cv20 from t order by b10 desc;
select vb10, c20, cv20 from t order by vb10;
select b16, c20, cv20 from t order by b16;
select vb16, c20, cv20 from t order by vb16;
select vb16, c20, cv20, lbv from t order by lbv;

select b10 from t where b10 > X'0000000010';
select b10 from t where b10 < X'0000000010';
select b10 from t where b10 <= X'0000000011';
select b10 from t where b10 >= X'0000000011';
select b10 from t where b10 <> X'0000000011';

select vb10 from t where vb10 > X'0000000010';
select vb10 from t where vb10 < X'0000000010';
select vb10 from t where vb10 <= X'0000000011';
select vb10 from t where vb10 >= X'0000000011';
select vb10 from t where vb10 <> X'0000000011';

select b16 from t where b16 > X'0000000010';
select b16 from t where b16 < X'0000000010';
select b16 from t where b16 <= X'0000000011';
select b16 from t where b16 >= X'0000000011';
select b16 from t where b16 <> X'0000000011';

select vb16 from t where vb16 > X'0000000010';
select vb16 from t where vb16 < X'0000000010';
select vb16 from t where vb16 <= X'0000000011';
select vb16 from t where vb16 >= X'0000000011';
select vb16 from t where vb16 <> X'0000000011';

select lbv from t where lbv > X'0000000010';
select lbv from t where lbv < X'0000000010';
select lbv from t where lbv <= X'0000000011';
select lbv from t where lbv >= X'0000000011';
select lbv from t where lbv <> X'0000000011';

select b10, vb10||X'11' from t where vb10||X'11' > b10;
select b10, X'11'||vb10 from t where X'11'||vb10 > b10;
select b16, vb16||X'11' from t where vb16||X'11' > b16;

select b10 || vb10 from t;
select lbv || b10 from t;
select b10 || lbv from t;
select lbv || vb10 from t;
select vb10 || lbv from t;

select t1.b10 from t t1, t t2 where t1.b10 > t2.b10;

-- FUNCTIONS
-- some length functions
select {fn length(b10)} from t;
select {fn length(vb10)} from t;
select {fn length(lbv)} from t;

select {fn length(c20)} from t;
select {fn length(cv20)} from t;

drop table t;

-----------------------
-- test normalization
-----------------------
create table t1 (c1 char(2) for bit data);
insert into t1 values (X'0001');
insert into t1 values (X'0010');
insert into t1 values (X'0011');
select * from t1;

-- now insert something that needs to be expanded
insert into t1 values (X'11');
select * from t1;

-- insert select, expand 1 byte
create table t2 (c1 char(3) for bit data);
insert into t2 select c1 from t1;
select * from t2;
drop table t2;

-- insert select, expand many bytes
create table t2 (c1 char(20) for bit data);
insert into t2 select c1 from t1;
select * from t2;
drop table t2;

drop table t1;

--
-- some extra tests for truncation.  in 2.0
create table t1 (b1 char(1) for bit data);

-- ok
insert into t1 values (X'11');

-- valid length
insert into t1 values (X'10');
insert into t1 values (X'11');

-- truncation errors
insert into t1 values (X'1000');
insert into t1 values (X'100000');
insert into t1 values (X'10000000');
insert into t1 values (X'1000000000');
insert into t1 values (X'100001');
insert into t1 values (X'0001');
insert into t1 values (X'8001');
insert into t1 values (X'8000');

drop table t1;
create table t1 (b9 char(2) for bit data);

-- ok
insert into t1 values (X'1111');

-- truncation errors
insert into t1 values (X'111100');
insert into t1 values (X'11110000');
insert into t1 values (X'1111000000');

insert into t1 values (X'1111111100000000');
insert into t1 values (X'1111111111');
insert into t1 values (X'11111111100001');
insert into t1 values (X'0001');
insert into t1 values (X'8001');
insert into t1 values (X'8000');

drop table t1;

-- a few other conditions
create table t1 (b3 char(2) for bit data, b7 char(4) for bit data, b8 char (5) for bit data, b15 char(8) for bit data, b16 char(9) for bit data);

-- ok
insert into t1 values
(
		X'1111',
		X'11111111',
		X'1111111111',
		X'1111111111111111',
		X'111111111111111111'
);

-- ok
insert into t1 values
(
		X'1110',
		X'11111110',
		X'11111111',
		X'1111111111111110',
		X'1111111111111111'
);

-- bad
-- truncation error for column b8
insert into t1 values
(
		null,
		null,
		X'111111111110',
		null,
		null
);

-- truncation error for column b7
insert into t1 values
(
		null,
		X'1111111100',
		null,
		null,
		null
);

-- truncation error for column b7
insert into t1 values
(
		null,
		X'1111111111',
		null,
		null,
		null
);

-- truncation error for column b15
insert into t1 values
(
		null,
		null,
		null,
		X'111111111111111100',
		null
);

-- truncation error for column b15
insert into t1 values
(
		null,
		null,
		null,
		X'111111111111111111',
		null
);

-- truncation error for column b16
insert into t1 values
(
		null,
		null,
		null,
		null,
		X'11111111111111111110'
);


AUTOCOMMIT OFF;

-- bug 5160 - incorrect typing of VALUES table constructor on an insert;

create table iv (id int, vc varchar(12));
insert into iv values (1, 'abc'), (2, 'defghijk'), (3, 'lmnopqrstcc');
insert into iv values (4, null), (5, 'null ok?'), (6, '2blanks  ');
insert into iv values (7, 'dddd'), (8, '0123456789123'), (9, 'too long');
select id, vc, {fn length(vc)} AS LEN from iv order by 1;

-- the inner values must not be changed to VARCHAR as it is not the table constructor
insert into iv select * from (values (10, 'pad'), (11, 'pad me'), (12, 'anakin jedi')) as t(i, c);
select id, vc, {fn length(vc)} AS LEN from iv order by 1;

-- check values outside of table constructors retain their CHARness
select c, {fn length(c)} AS LEN from (values (1, 'abc'), (2, 'defghijk'), (3, 'lmnopqrstcc')) as t(i, c);

drop table iv;

create table bv (id int, vb varchar(16) for bit data);
insert into bv values (1, X'1a'), (2, X'cafebabe'), (3, null);
select id, vb, {fn length(vb)} AS LEN from bv order by 1;
drop table bv;

create table dv (id int, vc varchar(12));
-- beetle 5568
-- should fail because DB2 doesn't allow this implicit casting to string
insert into dv values (1, 1.2), (2, 34.5639), (3, null);
-- should pass
insert into dv values (1, '1.2'), (2, '34.5639'), (3, null);
select id, vc from dv order by 1;
drop table dv;

-- bug 5306 -- incorrect padding of VALUES table constructor on an insert,
-- when implicit casting (bit->char or char->bit) is used.

-- 5306: Char -> For Bit Data Types

create table bitTable (id int, bv LONG VARCHAR FOR BIT DATA);
insert into bitTable values (1, X'031'), (2, X'032'), (3, X'');
insert into bitTable values (4, null), (5, X'033'), (6, X'2020');
select id, bv, {fn length(bv)} as LEN from bitTable order by 1;

-- the inner values must not be changed to varying, as it is not the table constructor
insert into bitTable select * from (values (10, 'pad'), (11, 'pad me'), (12, 'anakin jedi')) as t(i, c);
select id, bv, {fn length(bv)} AS LEN from bitTable order by 1;

drop table bitTable;

-- 5306: Bit -> Char

create table charTable (id int, cv long varchar);
insert into charTable values (1, x'0101'), (2, x'00101100101001'), (3, x'');
insert into charTable values (4, null), (5, x'1010101111'), (6, x'1000');
select id, cv, {fn length(cv)} as LEN from charTable order by 1;

-- the inner values must not be changed to varying, as it is not the table constructor
insert into charTable select * from (values (10, x'001010'), (11, x'01011010101111'), (12, x'0101010101000010100101110101')) as t(i, c);
select id, cv, {fn length(cv)} AS LEN from charTable order by 1;

drop table charTable;

-- Verify that 5306 still works with Union.

create table pt5 (b5 char(2) for bit data);
create table pt10 (b10 char (4) for bit data);
insert into pt10 values (x'01000110');
insert into pt5 values (x'1010');
select {fn length(CM)} from (select b5 from pt5 union all select b10 from pt10) as t(CM);
drop table pt5;
drop table pt10;

-- beetle 5612

create table t5612 (c1 char(10), c2 varchar(10), c3 long  varchar);
insert into t5612 values (X'00680069', X'00680069', X'00680069');
select * from t5612;
values cast(X'00680069' as char(30)), cast(X'00680069' as varchar(30)), cast(X'00680069' as long varchar);
