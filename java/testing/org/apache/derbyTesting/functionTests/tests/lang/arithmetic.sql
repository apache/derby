--
-- Test the arithmetic operators
--

create table t (i int, j int);

insert into t values (null, null);
insert into t values (0, 100);
insert into t values (1, 101);
insert into t values (-2, -102);

select i + j from t;

select i, i + 10 + 20, j, j + 100 + 200 from t;

select i - j, j - i from t;

select i, i - 10 - 20, 20 - 10 - i, j, j - 100 - 200, 200 - 100 - j from t;

select i, j, i * j, j * i from t;

select i, j, i * 10 * -20, j * 100 * -200 from t;

-- try unary minus on some expressions
select -i, -j, -(i * 10 * -20), -(j * 100 * -200) from t;

-- unary plus doesn't do anything
select +i, +j, +(+i * +10 * -20), +(+j * +100 * -200) from t;

-- test null/null, constant/null, null/constant
select i, j, i / j, 10 / j, j / 10 from t;

-- test for divide by 0
select j / i from t;

select (j - 1) / (i + 4), 20 / 5 / 4, 20 / 4 / 5 from t;

-- test positive/negative, negative/positive and negative/negative
select j, j / (0 - j), (0 - j) / j, (0 - j) / (0 - j) from t;

-- test some "more complex" expressions
select i, i + 10, i - (10 - 20), i - 10, i - (20 - 10) from t;

select 'The next 2 columns should agree', 2 + 3 * 4 + 5, 2 + (3 * 4) + 5 from t;

select 'The next column should be 45', (2 + 3) * (4 + 5) from t;

-- test overflow
delete from t;

insert into t values (null, null);
insert into t values (0, 100);
insert into t values (1, 101);

select i + 2147483647 from t;

select i - 2147483647 - 1, 'This query should work' from t;
select i - 2147483647 - 2, 'This query should fail' from t;

select j * 2147483647 from t;
select j * -2147483647 from t;

insert into t values (-2147483648, 0);

select -i from t;

-- test the arithmetic operators on a type we know they don't work on
create table s (x char(10), y char(10));

select x + y from s;

select x - y from s;

select x * y from s;

select x / y from s;

select -x from s;

-- do the same thing with smallints
-- except that integer constants are ints!
create table smallint_t (i smallint, j smallint);
create table smallint_s (i smallint, j smallint);

insert into smallint_t values (null, null);
insert into smallint_t values (0, 100);
insert into smallint_t values (1, 101);
insert into smallint_t values (-2, -102);

select i + j from smallint_t;

select i, j, i + i + j, j + j + i from smallint_t;

select i - j, j - i from smallint_t;

select i, i - j - j, j - j - i, j, j - i - i, i - i - j from smallint_t;

select i, j, i * j, j * i from smallint_t;

select i, j, i * i * (i - j), j * i * (i - j) from smallint_t;

select -i, -j, -(i * i * (i - j)), -(j * i * (i - j)) from smallint_t;

-- test for divide by 0
select j / i from smallint_t;

-- test for overflow
insert into smallint_s values (1, 32767);
select i + j from smallint_s;
select i - j - j from smallint_s;
select j + j from smallint_s;
select j * j from smallint_s;

insert into smallint_s values (-32768, 0);

select -i from smallint_s;

-- test mixed types: int and smallint
create table smallint_r (y smallint);

insert into smallint_r values (2);

select 65535 + y from smallint_r;
select y + 65535 from smallint_r;
select 65535 - y from smallint_r;
select y - 65535 from smallint_r;
select 65535 * y from smallint_r;
select y * 65535 from smallint_r;
select 65535 / y from smallint_r;
select y / 65535 from smallint_r;


-- do the same thing with bigints
create table bigint_t (i bigint, j bigint);
create table bigint_s (i bigint, j bigint);

insert into bigint_t values (null, null);
insert into bigint_t values (0, 100);
insert into bigint_t values (1, 101);
insert into bigint_t values (-2, -102);

select i + j from bigint_t;

select i, j, i + i + j, j + j + i from bigint_t;

select i - j, j - i from bigint_t;

select i, i - j - j, j - j - i, j, j - i - i, i - i - j from bigint_t;

select i, j, i * j, j * i from bigint_t;

select i, j, i * i * (i - j), j * i * (i - j) from bigint_t;

select -i, -j, -(i * i * (i - j)), -(j * i * (i - j)) from bigint_t;

-- test for divide by 0
select j / i from bigint_t;

-- test for overflow
insert into bigint_s values (1, 9223372036854775807);
select i + j from bigint_s;
select i - j - j from bigint_s;
select j + j from bigint_s;
select j * j from bigint_s;

select 2 * (9223372036854775807 / 2 + 1) from bigint_s;
select -2 * (9223372036854775807 / 2 + 2) from bigint_s;
select 2 * (-9223372036854775808 / 2 - 1) from bigint_s;
select -2 * (-9223372036854775808 / 2 - 1) from bigint_s;

insert into bigint_s values (-9223372036854775808, 0);

select -i from bigint_s;
select -j from bigint_s;

select i / 2 * 2 + 1 from bigint_s;
select j / 2 * 2 from bigint_s;

-- test mixed types: int and bigint
create table bigint_r (y bigint);

insert into bigint_r values (2);

select 2147483647 + y from bigint_r;
select y + 2147483647 from bigint_r;
select 2147483647 - y from bigint_r;
select y - 2147483647 from bigint_r;
select 2147483647 * y from bigint_r;
select y * 2147483647 from bigint_r;
select 2147483647 / y from bigint_r;
select y / 2147483647 from bigint_r;

-- test precedence and associativity
create table r (x int);

insert into r values (1);

select 2 + 3 * 4 from r;
select (2 + 3) * 4 from r;
select 3 * 4 + 2 from r;
select 3 * (4 + 2) from r;
select 2 - 3 * 4 from r;
select (2 - 3) * 4 from r;
select 3 * 4 - 2 from r;
select 3 * (4 - 2) from r;
select 4 + 3 / 2 from r;
select (4 + 3) / 2 from r;
select 3 / 2 + 4 from r;
select 3 / (2 + 4) from r;
select 4 - 3 / 2 from r;
select (4 - 3) / 2 from r;

-- + and - are of equal precedence, so they should be evaluated left to right
-- The result is the same regardless of order of evaluation, so test it
-- by causing an overflow.  The first test should get an overflow, and the
-- second one shouldn't.

select 1 + 2147483647 - 2 from r;
select 1 + (2147483647 - 2) from r;

select 4 * 3 / 2 from r;
select 4 * (3 / 2) from r;

-- Test associativity of unary - versus the binary operators
select -1 + 2 from r;
select -(1 + 2) from r;
select -1 - 2 from r;
select -(1 - 2) from r;

-- The test the associativity of unary - with respect to binary *, we must
-- use a trick.  The value -1073741824 is the minimum integer divided by 2.
-- So, 1073741824 * 2 will overflow, but (-1073741824) * 2 will not (because
-- of two's complement arithmetic.

select -1073741824 * 2 from r;
select -(1073741824 * 2) from r;

-- This should not get an overflow
select -2147483648 / 2 from r;

-- arithmetic on a numeric data type
create table u (c1 int, c2 char(10));
insert into u (c2) values 'asdf';
insert into u (c1) values null;
insert into u (c1) values 1;
insert into u (c1) values null;
insert into u (c1) values 2;
select c1 + c1 from u;
select c1 / c1 from u;

-- arithmetic between a numeric and a string data type fails
select c1 + c2 from u;

-- clean up after ourselves
drop table t;
drop table s;
drop table r;
drop table u;
drop table smallint_t;
drop table smallint_s;
drop table smallint_r;
drop table bigint_t;
drop table bigint_s;
drop table bigint_r;
