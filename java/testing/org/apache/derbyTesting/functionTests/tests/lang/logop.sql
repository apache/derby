--
-- this test is for logical operators (AND, OR, etc.)
--

-- create a table. Logical operators work on the results of comparisons,
-- which are tested in a separate test, so the types of the columns being
-- compared are irrelevant here.

create table t (x int, y int);

-- insert some values, including nulls
insert into t values (1, 1);
insert into t values (1, 2);
insert into t values (2, 1);
insert into t values (2, 2);
insert into t values (null, 2);
insert into t values (1, null);
insert into t values (null, null);

-- basic AND test
select x, y from t where x = 1 and y = 2;
select x, y from t where y = 2 and x = 1;

select x, y from t where x = 1 and y = 3;
select x, y from t where y = 3 and x = 1;

create table s (x int);

insert into s values (1);

-- there is no short-circuiting with AND: ie when the first operand is FALSE,
-- the second operant still got evaluated for AND. Same behavior in DB2 as well.
select x from s where x = 5 and 2147483647 + 10 = 2;

-- Does not matter it in what order the 2 operands are. Both of them always gets evaluated.
select x from s where 2147483647 + 10 = 2 and x = 5;

-- Now try a chain of ANDs
select x, y from t where x = 1 and x + 0 = 1 and y = 2 and y + 0 = 2;

-- basic OR test
select x, y from t where x = 1 or y = 2;
select x, y from t where y = 2 or x = 1;

select x, y from t where x = 4 or y = 5;
select x, y from t where y = 5 or x = 4;

-- test short-circuiting: for OR, when the first operand is TRUE, the second
-- operand should not be evaluated.  We test this by deliberately causing an
-- error in one of the operands.
select x from s where x = 1 or 2147483647 + 10 = 2;

-- Now try it with the error on the left, just to be sure the error really
-- happens.
select x from s where 2147483647 + 10 = 2 or x = 1;

-- Now try a chain of ORs
select x, y from t where x = 1 or x + 0 = 1 or y = 2 or y + 0 = 2;
-- Test the precedence of AND versus OR.  AND is supposed to have a higher
-- precedence that OR, i.e. "a OR b AND c" is equivalent to "a OR (b AND c)"

-- First test TRUE OR TRUE AND FALSE.  This should evaluate to TRUE.  If
-- the precedence is wrong, it will evaluate to FALSE.
select x from s where (1 = 1) or (2 = 2) and (3 = 4);

-- Now test FALSE AND TRUE OR TRUE.  This should evaluate to to TRUE.  If
-- the precedence is wrong, it will evaluate to FALSE.
select x from s where (1 = 2) and (3 = 3) or (4 = 4);

-- Now test whether parenthesized expressions work.  Use the parentheses to
-- force the OR to be evaluated before the AND.

select x from s where ( (1 = 1) or (2 = 2) ) and (3 = 4);

select x from s where (1 = 2) and ( (3 = 3) or (4 = 4) );

-- More involved testing of expression normalization
-- Ands under ands under ands ...
select * from s where (	( ( ((1=1) and (1=1)) and ((1=1) and (1=1)) ) and
						  ( ((1=1) and (1=1)) and ((1=1) and (1=1)) ) ) and
						( ( ((1=1) and (1=1)) and ((1=1) and (1=1)) ) and
						  ( ((1=1) and (1=1)) and ((1=1) and (1=1)) ) ) );

-- Ors under ors under ors ...
select * from s where (	( ( ((1=1) or (1=1)) or ((1=1) or (1=1)) ) or
						  ( ((1=1) or (1=1)) or ((1=1) or (1=1)) ) ) or
						( ( ((1=1) or (1=1)) or ((1=1) or (1=1)) ) or
						  ( ((1=1) or (1=1)) or ((1=1) or (1=1)) ) ) );

-- Ands under ors under ors ...
select * from s where (	( ( ((1=1) and (1=1)) or ((1=1) and (1=1)) ) or
						  ( ((1=1) and (1=1)) or ((1=1) and (1=1)) ) ) or
						( ( ((1=1) and (1=1)) or ((1=1) and (1=1)) ) or
						  ( ((1=1) and (1=1)) or ((1=1) and (1=1)) ) ) );

-- Ors under ands under ands
select * from s where ( ( ( ((1=1) or (1=1)) and ((1=1) or (1=1)) ) and
						  ( ((1=1) or (1=1)) and ((1=1) or (1=1)) ) ) and
						( ( ((1=1) or (1=1)) and ((1=1) or (1=1)) ) and
						  ( ((1=1) or (1=1)) and ((1=1) or (1=1)) ) ) );

-- left deep with ands
select * from s where ( ( ( ( ( ((1=1) and (1=1)) and (1=1)) and (1=1)) and (1=1))
						and (1=1)) and (1=1));

-- left deep with ors
select * from s where ( ( ( ( ( ((1=1) or (1=1)) or (1=1)) or (1=1)) or (1=1))
						or (1=1)) or (1=1));

select * from s where ( ( ( ( ( ((1=1) or (1=2)) or (1=2)) or (1=2)) or (1=2))
						or (1=2)) or (1=2));

select * from s where ( ( ( ( ( ((1=2) or (1=2)) or (1=1)) or (1=2)) or (1=2))
						or (1=2)) or (1=2));

select * from s where ( ( ( ( ( ((1=2) or (1=2)) or (1=2)) or (1=1)) or (1=2))
						or (1=2)) or (1=2));

select * from s where ( ( ( ( ( ((1=2) or (1=2)) or (1=2)) or (1=2)) or (1=1))
						or (1=2)) or (1=2));

select * from s where ( ( ( ( ( ((1=2) or (1=2)) or (1=2)) or (1=2)) or (1=2))
						or (1=1)) or (1=2));

select * from s where ( ( ( ( ( ((1=2) or (1=2)) or (1=2)) or (1=2)) or (1=2))
						or (1=2)) or (1=1));

-- right deep with ors
select * from s where ( ( ( ( ( (1=1) or ((1=2) or (1=2)) ) or (1=2)) or (1=2))
						or (1=2)) or (1=2));

select * from s where ( ( ( ( ( (1=2) or ((1=1) or (1=2)) ) or (1=2)) or (1=2))
						or (1=2)) or (1=2));

select * from s where ( ( ( ( ( (1=2) or ((1=2) or (1=1)) ) or (1=2)) or (1=2))
						or (1=2)) or (1=2));

select * from s where ( ( ( ( ( (1=2) or ((1=2) or (1=2)) ) or (1=1)) or (1=2))
						or (1=2)) or (1=2));

select * from s where ( ( ( ( ( (1=2) or ((1=2) or (1=2)) ) or (1=2)) or (1=1))
						or (1=2)) or (1=2));

select * from s where ( ( ( ( ( (1=2) or ((1=2) or (1=2)) ) or (1=2)) or (1=2))
						or (1=1)) or (1=2));

select * from s where ( ( ( ( ( (1=2) or ((1=2) or (1=2)) ) or (1=2)) or (1=2))
						or (1=2)) or (1=1));

-- ... and false and ... should get resolved to false
select x from s where 2147483647 + 10 = 2 and (1=2);
select x from s where (1=2) and 2147483647 + 10 = 2;

-- nots
select x from s where not ( (1 = 1) or (2 = 2) ) and (3 = 4);
select x from s where not ( ( (1 = 1) or (2 = 2) ) and (3 = 4) );
select x from s where (1 = 2) and not ( (3 = 3) or (4 = 4) );
select x from s where not ( (1 = 2) and ( (3 = 3) or (4 = 4) ) );
-- following NOTs in select clause won't work because it results in a transient boolean datatype
select not ( (1 = 1) or (2 = 2) ) and (3 = 4) from s;
--
select not ( ( (1 = 1) or (2 = 2) ) and (3 = 4) ) from s;
--
select (1 = 2) and not ( (3 = 3) or (4 = 4) ) from s;
--
select not ( (1 = 2) and ( (3 = 3) or (4 = 4) ) ) from s;

-- Ands under ands under ands ...
select * from s where not (	( ( ((1=1) and (1=1)) and ((1=1) and (1=1)) ) and
						      ( ((1=1) and (1=1)) and ((1=1) and (1=1)) ) ) and
						    ( ( ((1=1) and (1=1)) and ((1=1) and (1=1)) ) and
						      ( ((1=1) and (1=1)) and ((1=1) and (1=2)) ) ) );

-- Ors under ors under ors ...
select * from s where not (	( ( ((1=1) or (1=1)) or ((1=1) or (1=1)) ) or
						      ( ((1=1) or (1=1)) or ((1=1) or (1=1)) ) ) or
						    ( ( ((1=1) or (1=1)) or ((1=1) or (1=1)) ) or
						      ( ((1=1) or (1=1)) or ((1=1) or (1=2)) ) ) );

-- Ands under ors under ors ...
select * from s where not (	( ( ((1=1) and (1=1)) or ((1=1) and (1=1)) ) or
						      ( ((1=1) and (1=1)) or ((1=1) and (1=1)) ) ) or
						    ( ( ((1=1) and (1=1)) or ((1=1) and (1=1)) ) or
						      ( ((1=1) and (1=1)) or ((1=1) and (1=2)) ) ) );

-- Ors under ands under ands
select * from s where not ( ( ( ((1=1) or (1=1)) and ((1=1) or (1=1)) ) and
						      ( ((1=1) or (1=1)) and ((1=1) or (1=1)) ) ) and
						    ( ( ((1=1) or (1=1)) and ((1=1) or (1=1)) ) and
						      ( ((1=1) or (1=1)) and ((1=1) or (1=2)) ) ) );

-- left deep with ands
select * from s where not ( ( ( ( ( ((1=1) and (1=2)) and (1=1)) and (1=1)) and (1=1))
						    and (1=1)) and (1=1));

-- left deep with ors
select * from s where not ( ( ( ( ( ((1=2) or (1=1)) or (1=1)) or (1=1)) or (1=1))
						    or (1=1)) or (1=1));

select * from s where not ( ( ( ( ( ((1=2) or (1=2)) or (1=2)) or (1=2)) or (1=2))
						    or (1=2)) or (1=2));

select * from s where not ( ( ( ( ( ((1=2) or (1=2)) or (1=1)) or (1=2)) or (1=2))
						    or (1=2)) or (1=2));

select * from s where not ( ( ( ( ( ((1=2) or (1=2)) or (1=2)) or (1=1)) or (1=2))
						    or (1=2)) or (1=2));

select * from s where not ( ( ( ( ( ((1=2) or (1=2)) or (1=2)) or (1=2)) or (1=1))
						    or (1=2)) or (1=2));

select * from s where not ( ( ( ( ( ((1=2) or (1=2)) or (1=2)) or (1=2)) or (1=2))
						    or (1=1)) or (1=2));

select * from s where not ( ( ( ( ( ((1=2) or (1=2)) or (1=2)) or (1=2)) or (1=2))
						    or (1=2)) or (1=1));

-- right deep with ors
select * from s where not ( ( ( ( ( (1=1) or ((1=2) or (1=2)) ) or (1=2)) or (1=2))
						    or (1=2)) or (1=2));

select * from s where not ( ( ( ( ( (1=2) or ((1=2) or (1=2)) ) or (1=2)) or (1=2))
						    or (1=2)) or (1=2));

select * from s where not ( ( ( ( ( (1=2) or ((1=2) or (1=1)) ) or (1=2)) or (1=2))
						    or (1=2)) or (1=2));

select * from s where not ( ( ( ( ( (1=2) or ((1=2) or (1=2)) ) or (1=1)) or (1=2))
						    or (1=2)) or (1=2));

select * from s where not ( ( ( ( ( (1=2) or ((1=2) or (1=2)) ) or (1=2)) or (1=1))
						    or (1=2)) or (1=2));

select * from s where not ( ( ( ( ( (1=2) or ((1=2) or (1=2)) ) or (1=2)) or (1=2))
						    or (1=1)) or (1=2));

select * from s where not ( ( ( ( ( (1=2) or ((1=2) or (1=2)) ) or (1=2)) or (1=2))
						    or (1=2)) or (1=1));

-- nots on nots
select * from s where not ( not ( not ((1=2) or (1=1))) or (not ((1=2) or (1=2)) ) );
-- following nots on nots won't work because they result in transient boolean datatype in the select clause
select not ( not ( not ((1=2) or (1=1))) or (not ((1=2) or (1=2)) ) ) from s;

-- negative testing
-- non boolean where clauses
select * from s where 1;
select * from s where 1 and (1=1);
select * from s where (1=1) and 1;
select * from s where 1 or (1=1);
select * from s where (1=1) or 1;
select * from s where not 1;

-- Clean up
drop table t;
drop table s;

