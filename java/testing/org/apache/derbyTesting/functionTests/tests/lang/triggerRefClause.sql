--
-- Test the REFERENCING clause for a trigger
-- as well as a general test that using the
-- OLD and NEW transition variables work ok
--
drop table x;
create table x (x int, y int, z int);

--
-- negative tests
--

-- syntax

-- mismatch: insert->old, delete->new
create trigger t1 after insert on x referencing old as oldrow for each row mode db2sql values 1;
create trigger t1 after insert on x referencing old_table as oldtab for each statement mode db2sql values 1;
create trigger t1 after insert on x referencing old_table as oldtab for each statement mode db2sql values 1;
create trigger t1 after delete on x referencing new as newrow for each row mode db2sql values 1;
create trigger t1 after delete on x referencing new_table as newtab for each statement mode db2sql values 1;
create trigger t1 after delete on x referencing new_table as newtab for each statement mode db2sql values 1;

-- same as above, bug using old/new
create trigger t1 after insert on x referencing old as old for each row mode db2sql values old.x;
create trigger t1 after insert on x referencing old_table as old for each statement mode db2sql select * from old;
create trigger t1 after insert on x referencing old_table as old for each statement mode db2sql select * from old;
create trigger t1 after delete on x referencing new as new for each row mode db2sql values new.x;
create trigger t1 after delete on x referencing new_table as new for each statement mode db2sql select * from new;
create trigger t1 after delete on x referencing new_table as new for each statement mode db2sql select * from new;

-- cannot reference columns that don't exist, not bound as normal stmts
create trigger t1 after delete on x referencing old as old for each row mode db2sql values old.badcol;
create trigger t1 after delete on x referencing old as old for each row mode db2sql values old;
create trigger t1 after delete on x referencing old as oldrow for each row mode db2sql values oldrow.badcol;
create trigger t1 after delete on x referencing old as oldrow for each row mode db2sql values oldrow;

-- lets try some basics with old/new table
create table y (x int);
insert into y values 1, 2, 666, 2, 2, 1;
insert into x values (1, null, null), (2, null, null);
create trigger t1 after delete on x referencing old as old for each row mode db2sql delete from y where x = old.x;
autocommit off;

delete from x;
select * from y;
rollback;

drop trigger t1;
commit;

create trigger t1 after delete on x referencing old_table as old for each statement mode db2sql delete from y where x in (select x from old);
delete from x;
select * from y;

drop trigger t1;
rollback;

delete from x;
select * from y;
rollback;

delete from x;
delete from y;

-- test all types and row triggers since they do explicit type mapping
create table allTypes1 (i int, tn smallint, s smallint, l bigint,
				c char(10), v varchar(50), lvc long varchar,
				d double precision, r real, f float,
				dt date, t time, ts timestamp,
				b CHAR(2) FOR BIT DATA, bv VARCHAR(2) FOR BIT DATA, lbv LONG VARCHAR FOR BIT DATA,
				dc decimal(5,2), n numeric(8,4));

create table allTypes2 (i int, tn smallint, s smallint, l bigint,
				c char(10), v varchar(50), lvc long varchar,
				d double precision, r real, f float,
				dt date, t time, ts timestamp,
				b  CHAR(2) FOR BIT DATA, bv VARCHAR(2) FOR BIT DATA, lbv LONG VARCHAR FOR BIT DATA,
				dc decimal(5,2), n numeric(8,4));
create trigger t1 after insert on allTypes1 referencing new as newrowtab for each row mode db2sql
	insert into allTypes2 
	values (newrowtab.i, newrowtab.tn, newrowtab.s, newrowtab.l,
		newrowtab.c, newrowtab.v, newrowtab.lvc,
		newrowtab.d, newrowtab.r, newrowtab.f,   newrowtab.dt,  
		newrowtab.t, newrowtab.ts, newrowtab.b, newrowtab.bv, 
		newrowtab.lbv, newrowtab.dc, newrowtab.n);

commit;
insert into allTypes1 values (0, 10, 100, 1000000,
					  'duplicate', 'this is duplicated', 'also duplicated',
					  200.0e0, 200.0e0, 200.0e0,
					  date('1992-01-01'), time('12:30:30'), timestamp('1992-01-01 12:30:30'),
					  X'12af', X'0F0F', X'1234', 111.11, 222.2);

select * from allTypes1;
select * from allTypes2;
commit;
drop trigger t1;

insert into allTypes1 values (0, 10, 100, 1000000,
					  'duplicate', 'this is duplicated', 'also duplicated',
					  200.0e0, 200.0e0, 200.0e0,
					  date('1992-01-01'), time('12:30:30'), timestamp('1992-01-01 12:30:30'),
					  X'12af', X'0F0F', X'1234', 111.11, 222.2); 

delete from alltypes1;
drop trigger t1;

insert into allTypes1 values (0, 10, 100, 1000000,
					  'duplicate', 'this is duplicated', 'also duplicated',
					  200.0e0, 200.0e0, 200.0e0,
					  date('1992-01-01'), time('12:30:30'), timestamp('1992-01-01 12:30:30'),
					  X'12af', X'0F0F', X'1234', 111.11, 222.2); 
drop table allTypes1;
drop table allTypes2;

-- do a join to find changed values just because i can
drop table x;
drop table y;
create table x (x int);
create table removed (x int);
-- create trigger t1 after update of x on x referencing old_table as old new_table as new
-- 	 for each statement mode db2sql
-- 	 insert into removed select * from old where x not in (select x from new where x < 10);
insert into x values 1,3,4,5,6,9,666,667,668;
update x set x = x+1;
select * from x;
select * from removed;
drop table x;
drop table removed;
commit;

create table x (x int, y int);
create table y (x int, y int);
create trigger t1 after insert on x referencing new_table as newtab for each statement mode db2sql
	insert into y select newtab.x, y+newtab.y from newtab;
insert into x values (1,1);
select * from y;
delete from y;
drop trigger t1;

-- how about a correlation of a transition variable
create trigger t1 after insert on x referencing new_table as newtab for each statement mode db2sql
	insert into y select newtab2.x, y+newtab2.y from newtab newtab2;
insert into x values (1,1);
select * from y;


-- lets prove that we are getting object types from row transition
-- variables.  this is only an issue with row triggers because
-- they are doing some funky stuff under the covers to make
-- a column appear just like a normal table column
drop table x;
drop table y;

create table val (x int);
create table x (b char(5) FOR BIT DATA);
create table y (b char(5) FOR BIT DATA);
create trigger t1 after insert on x referencing new as new for each row mode db2sql insert into y values (new.b || X'80');
insert into x values (X'E0');
select * from y;

drop trigger t1;
create trigger t1 after insert on x referencing new as new for each row mode db2sql insert into y values new.b;
insert into x values null;
select * from y;

drop trigger t1;
create trigger t1 after insert on x referencing new as new for each row mode db2sql insert into val values length(new.b);
insert into x values X'FFE0';
select * from val;

drop table x;
drop table y;
drop table val;

create table x (x dec(7,3));
create table y (x dec(8,4));
insert into x values 1234.1234, null, 1234.123;
select * from x;
select * from y;

create table t1 (col1 int not null primary key, col2 char(20));
create table s_t1(col1 int not null primary key, chgType char(20));

-- should work
create trigger trig_delete_2 after delete on t1 referencing OLD_TABLE as OLD for each statement mode db2sql 
	insert into s_t1 (select col1, 'D'
	from OLD  where OLD.col1 <> ALL
	(select col1 from s_t1 where  OLD.col1 = s_t1.col1));

drop trigger trig_delete_2;
-- should work
create trigger trig_delete_2 after delete on t1 referencing old_table as OLD for each statement mode db2sql 
	insert into s_t1 (select col1, 'D'
	from OLD where OLD.col1 <> ALL
	(select s_t1.col1 from s_t1, OLD where  OLD.col1 = s_t1.col1));

insert into t1 values (5, 'first row'), (3, 'second row'), (9, 'third row'),
			(4, 'forth row');
select * from s_t1;
delete from t1 where col1 = 3 or col1 = 9;
select * from s_t1;
insert into t1 values (9, 'third row'), (3, 'second row'), (7, 'fifth row');
delete from t1 where col1 = 3 or col1 = 7;
select * from s_t1;
delete from t1;
select * from s_t1;

rollback;
