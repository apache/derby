--
-- Small trigger stream test.  Make sure we can
-- read streams ok from the context of a row or
-- statement trigger.
--
create function getAsciiColumn( whichRS int, colNumber int, value varchar(128)) returns int
  PARAMETER STYLE JAVA LANGUAGE JAVA NO SQL
  EXTERNAL NAME 'org.apache.derbyTesting.functionTests.util.StreamUtil.getAsciiColumn';
create procedure insertAsciiColumn( stmtText varchar( 256), colNumber int, value varchar(128), length int)
  PARAMETER STYLE JAVA LANGUAGE JAVA MODIFIES SQL DATA
  EXTERNAL NAME 'org.apache.derbyTesting.functionTests.util.StreamUtil.insertAsciiColumn';
create function getBinaryColumn( whichRS int, colNumber int, value varchar(128)) returns int
  PARAMETER STYLE JAVA LANGUAGE JAVA NO SQL
  EXTERNAL NAME 'org.apache.derbyTesting.functionTests.util.StreamUtil.getBinaryColumn';
create procedure insertBinaryColumn( stmtText varchar( 256), colNumber int, value varchar(128), length int)
  PARAMETER STYLE JAVA LANGUAGE JAVA MODIFIES SQL DATA
  EXTERNAL NAME 'org.apache.derbyTesting.functionTests.util.StreamUtil.insertBinaryColumn';

drop table x;
create table x (x int, c1 long varchar, y int, slen int);

-- this getAsciiColumn() method reads in the stream
-- and verifies each byte and prints out the length
-- of the column
create trigger t1 NO CASCADE before update of x,y on x for each statement mode db2sql
	values getAsciiColumn( 0, 2, 'a');
create trigger t2 after update of x,y on x for each row  mode db2sql
	values getAsciiColumn( 1, 2, 'a');

create trigger t3 after insert on x for each statement  mode db2sql
	values getAsciiColumn( 1, 2, 'a');
create trigger t4 NO CASCADE before insert on x for each row  mode db2sql
	values getAsciiColumn( 1, 2, 'a');

create trigger t5 NO CASCADE before delete on x for each statement  mode db2sql
	values getAsciiColumn( 0, 2, 'a');
create trigger t6 after delete on x for each row  mode db2sql
	values getAsciiColumn( 0, 2, 'a');

call insertAsciiColumn('insert into x values (1, ?, 1, ?)', 1, 'a', 1);
call insertAsciiColumn('insert into x values (2, ?, 2, ?)', 1, 'a', 10);
call insertAsciiColumn('insert into x values (3, ?, 3, ?)', 1, 'a', 100);
call insertAsciiColumn('insert into x values (4, ?, 4, ?)', 1, 'a', 1000);
call insertAsciiColumn('insert into x values (5, ?, 5, ?)', 1, 'a', 5000);
call insertAsciiColumn('insert into x values (6, ?, 6, ?)', 1, 'a', 10000);
call insertAsciiColumn('insert into x values (7, ?, 7, ?)', 1, 'a', 16500);
call insertAsciiColumn('insert into x values (8, ?, 8, ?)', 1, 'a', 32500);
call insertAsciiColumn('insert into x values (9, ?, 9, ?)', 1, 'a', 0);
call insertAsciiColumn('insert into x values (10, ?, 10, ?)', 1, 'a', 666);

update x set x = x+1;
update x set x = null;

insert into x select * from x;

delete from x;

drop table x;



create table x (x int, c1 long varchar for bit data, y int, slen int);

-- this getBinaryColumn() method reads in the stream
-- and verifies each byte and prints out the length
-- of the column
create trigger t1 NO CASCADE before update of x,y on x for each statement  mode db2sql
	values getBinaryColumn( 0, 2, 'a');
create trigger t2 after update of x,y on x for each row  mode db2sql
	values getBinaryColumn( 1, 2, 'a');

create trigger t3 after insert on x for each statement  mode db2sql
	values getBinaryColumn( 1, 2, 'a');
create trigger t4 NO CASCADE before insert on x for each row  mode db2sql
	values getBinaryColumn( 1, 2, 'a');

create trigger t5 NO CASCADE before delete on x for each statement  mode db2sql
	values getBinaryColumn( 1, 2, 'a');
create trigger t6 after delete on x for each row  mode db2sql
	values getBinaryColumn( 0, 2, 'a');

call insertBinaryColumn('insert into x values (1, ?, 1, ?)', 1, 'a', 1);
call insertBinaryColumn('insert into x values (2, ?, 2, ?)', 1, 'a', 10);
call insertBinaryColumn('insert into x values (3, ?, 3, ?)', 1, 'a', 100);
call insertBinaryColumn('insert into x values (4, ?, 4, ?)', 1, 'a', 1000);
call insertBinaryColumn('insert into x values (5, ?, 5, ?)', 1, 'a', 10000);
call insertBinaryColumn('insert into x values (6, ?, 6, ?)', 1, 'a', 32700);
call insertBinaryColumn('insert into x values (7, ?, 7, ?)', 1, 'a', 32699);
call insertBinaryColumn('insert into x values (8, ?, 8, ?)', 1, 'a', 16384);
call insertBinaryColumn('insert into x values (9, ?, 9, ?)', 1, 'a', 16383);
call insertBinaryColumn('insert into x values (10, ?, 10, ?)', 1, 'a', 0);
call insertBinaryColumn('insert into x values (11, ?, 11, ?)', 1, 'a', 666);

select x, length(c1) from x order by 1;

update x set x = x+1;
select x, length(c1) from x order by 1;

update x set x = null;
select x, length(c1) from x order by 2;

insert into x select * from x;
select x, length(c1) from x order by 2;

delete from x;
