--
-- this tests derived column lists and derived tables
--

create table s (a int, b int, c int, d int, e int, f int);
create table t (aa int, bb int, cc int, dd int, ee int, ff int);

insert into s values (0,1,2,3,4,5);
insert into s values (10,11,12,13,14,15);

-- tests without a derived table

-- negative tests
-- # of columns does not match
select aa from s ss (aa);
select aa from s ss (aa, bb, cc, dd, ee, ff, gg);
-- duplicate names in derived column list
select aa from s ss (aa, ee, bb, cc, dd, aa);
-- test case insensitivity
select aa from s ss (aa, bb, cc, dd, ee, AA);
-- test uniqueness of names
select aa from s ss (aa, bb, cc, dd, ee, ff), t;
-- test uniqueness of names
insert into t select aa 
from s aa (aa, bb, cc, dd, ee, ff), s bb (aa, bb, cc, dd, ee, ff);
-- verify using "exposed" names
select a from s ss (aa, bb, cc, dd, ee, ff);

-- positive tests
-- rename the columns
select * from s ss (f, e, d, c, b, a) where f = 0;
-- delimited identifiers in list
select * from s ss ("a a", "b b", "c c", "d d", "e e", "f f") where "a a" = 0;
-- uniqueness of "exposed" names
select a, aa from s a, s b (aa, bb, cc, dd, ee, ff)	where f = ff and aa = 10;
select a.a, b.aa from s a, s b (aa, bb, cc, dd, ee, ff) where f = ff and b.aa = 10;

-- insert tests
insert into t select * from s ss (aa, bb, cc, dd, ee, ff);
select * from t;
delete from t;

insert into t (aa,bb) select ff, aa from s ss (aa, bb, cc, dd, ee, ff);
select * from t;
delete from t;

-- derived tables

-- negative tests
-- no correlation name
select * from (select * from s);
-- # of columns does not match
select aa from (select * from s) ss (aa);
select aa from (select * from s) ss (aa, bb, cc, dd, ee, ff, gg);
-- duplicate names in derived column list
select aa from (select * from s) ss (aa, ee, bb, cc, dd, aa);
-- test case insensitivity
select aa from (select * from s) ss (aa, bb, cc, dd, ee, AA);
-- test uniqueness of names
select aa from (select * from s) ss (aa, bb, cc, dd, ee, ff), t;
-- test uniqueness of names
insert into t select aa 
from (select * from s) aa (aa, bb, cc, dd, ee, ff), 
	 (select * from s) bb (aa, bb, cc, dd, ee, ff);
-- verify using "exposed" names
select a from (select * from s) ss (aa, bb, cc, dd, ee, ff);
-- ambiguous column reference
select a from (select * from s a, s b) ss;

-- positive tests

-- simple derived table
select a from (select a from s) a;
-- select * query's
select * from (select * from s) a;
select * from (select a, b, c, d, e, f from s) a;
select * from (select a, b, c from s) a;
select a, b, c, d, e, f from (select * from s) a;

-- simple derived table
insert into t (aa) select a from (select a from s) a;
select * from t;
delete from t;

-- select * query's
insert into t select * from (select * from s) a;
select * from t;
delete from t;

insert into t select * from (select a, b, c, d, e, f from s) a;
select * from t;
delete from t;

insert into t (aa, bb, cc) select * from (select a, b, c from s) a;
select * from t;
delete from t;

insert into t select a, b, c, d, e, f from (select * from s) a;
select * from t;
delete from t;

-- simple derived table with derived column list
select a from (select a from s) a (a);
-- select * query's	with derived column lists
select * from (select * from s) a (f, e, d, c, b, a);
select * from (select a, b, c, d, e, f from s) a (f, e, d, c, b, a);
select * from (select a, b, c from s) a (c, f, e);
select a, b, c, d, e, f from (select * from s) a (a, b, c, d, e, f);

-- simple derived table with derived column list
insert into t (aa) select a from (select a from s) a (a);
select * from t;
delete from t;

-- select * query's with derived column lists
insert into t select * from (select * from s) a (c, b, a, e, f, d);
select * from t;
delete from t;

insert into t select * from (select a, b, c, d, e, f from s) a (f, a, c, b, e, d);
select * from t;
delete from t;

insert into t (aa, bb, cc) select * from (select a, b, c from s) a (f, e, a);
select * from t;
delete from t;

insert into t select a, c, "a", "b", b, "c" from (select * from s) a (a, c, "a", "b", b, "c");
select * from t;
delete from t;

-- project and reorder derived column list
select a, f from (select * from s) a (b, c, d, e, f, a);

insert into t (aa, bb) select a, f from (select * from s) a (b, c, d, e, f, a);
select * from t;
delete from t;

-- outer where clause references columns from derived table 
select * from (select * from s) a (a, b, c, d, e, f) where a = 0;
select * from (select * from s) a (f, e, d, c, b, a) where f = 0;

insert into t select * from (select * from s) a (a, b, c, d, e, f) where a = 0;
select * from t;
delete from t;

insert into t select * from (select * from s) a (f, e, d, c, b, a) where f = 0;
select * from t;
delete from t;

-- join between 2 derived tables
select * from (select a from s) a, (select a from s) b;
select * from (select a from s) a, (select a from s) b where a.a = b.a;

insert into t (aa, bb) select * from (select a from s) a, (select a from s) b where a.a = b.a;
select * from t;
delete from t;

-- join within a derived table
select * from (select a.a, b.a from s a, s b) a (b, a) where b = a;
select * from (select a.a, b.a from s a, s b) a (b, a),
			  (select a.a, b.a from s a, s b) b (b, a) where a.b = b.b;
select * from (select (select 1 from s where 1 = 0), b.a from s a, s b) a (b, a),
			  (select (select 1 from s where 1 = 0), b.a from s a, s b) b (b, a) where a.b = b.b;

insert into t (aa, bb) select * from (select a.a, b.a from s a, s b) a (b, a) where b = a;
select * from t;
delete from t;

-- join within a derived table, 2 predicates can be pushed all the way down
select * from (select a.a, b.a from s a, s b) a (b, a) where b = a and a = 0 and b = 0;

insert into t (aa, bb) select * from (select a.a, b.a from s a, s b) a (b, a) where b = a and a = 0 and b = 0;
select * from t;
delete from t;

-- nested derived tables
select * from (select * from (select * from s) a ) a;
select * from 
	(select * from 
		(select * from 
			(select * from 
				(select * from 
					(select * from
						(select * from
							(select * from
								(select * from
									(select * from
										(select * from
											(select * from
												(select * from
													(select * from
														(select * from
															(select * from s) a
														) a
													) a
												) a
											) a
										) a
									) a
								) a
							) a
						) a
					) a
				) a
			) a
		) a
	) a;

-- test predicate push through
select * from
(select a.a as a1, b.a as a2 from s a, s b) a 
where a.a1 = 0 and a.a2 = 10;

-- push column = column through
select * from (select a, a from s) a (x, y) where x = y;
select * from (select a, a from s) a (x, y) where x + y = x * y;

-- return contants and expressions from derived table
select * from (select 1 from s) a;
select * from (select 1 from s) a (x) where x = 1;
select * from (select 1 from s a, s b where a.a = b.a) a (x);
select * from (select 1 from s a, s b where a.a = b.a) a (x) where x = 1;
select * from (select a + 1 from s) a;
select * from (select a + 1 from s) a (x) where x = 1;
select * from (select a.a + 1 from s a, s b where a.a = b.a) a (x) where x = 1;

-- Bug 2767, don't flatten derived table with join
create table tab1(tab1_c1 int, tab1_c2 int);
create table tab2(tab2_c1 int, tab2_c2 int);
insert into tab1 values (1, 1), (2, 2);
insert into tab2 values (1, 1), (2, 2);
select * from (select * from tab1, tab2) c where tab1_c1 in (1, 3);
drop table tab1;
drop table tab2;


drop table s;
drop table t;

