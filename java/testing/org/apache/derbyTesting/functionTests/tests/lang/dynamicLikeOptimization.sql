-- test the dynamic like optimization
-- NOTE: the metadata test does a bunch
-- of likes with parameters

autocommit off;

-- by default, holdability of ResultSet objects created using this Connection object is true. Following will set it to false for this connection.
-- NoHoldForConnection;

call SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1);
maximumdisplaywidth 2000;

-- language layer tests
create table t1(c11 int);
insert into t1 values(1);
prepare ll1 as 'select 1 from t1 where ''asdf'' like ?';
execute ll1 using 'values '''' ';	-- no match char(1) pads to ' '
execute ll1 using 'values ''%'' ';
execute ll1 using 'values ''%f'' ';
execute ll1 using 'values cast(''%f'' as varchar(2)) ';
execute ll1 using 'values ''%g'' ';
execute ll1 using 'values ''asd%'' ';
execute ll1 using 'values ''_%'' ';
execute ll1 using 'values ''%_'' ';
execute ll1 using 'values ''_asdf'' ';
execute ll1 using 'values ''%asdf'' ';
execute ll1 using 'values cast(null as char)';
execute ll1 using 'values '''' ';

-- Escape tests
prepare ll15 as 'select 1 from t1 where ''%foobar'' like ''Z%foobar'' escape ?';
execute ll15 using 'values ''Z''';		-- match: optimize to LIKE AND ==
execute ll15 using 'values cast(''Z'' as varchar(1)) ';

execute ll15 using 'values ''raZ''';	-- too many like chars
execute ll15 using 'values ''%''';		-- no match, wrong char

select 1 from t1  where '%foobar' like '%%foobar' escape '%';	-- match
select 1 from t1  where '_foobar' like '__foobar' escape '_';	-- match

select 1 from t1  where 'asdf' like 'a%' escape cast(null as char);	-- error NULL escape

prepare ll2 as 'select 1 from t1 where ''%foobar'' like ? escape ?'; 
execute ll2 using 'values (''Z%foobar'', ''Z'') '; 		-- match
execute ll2 using 'values (''Z%foobar'', '''') '; 		-- error empty string escape 
prepare ll2 as 'select 1 from t1 where ''%foobar'' like ? escape ''Z''';
execute ll2 using 'values ''x%foobar'' '; 		-- no match 
execute ll2 using 'values ''Z%foobar'' ';		-- match 
prepare ll2 as 'select 1 from t1 where ''%foobar'' like ? escape ''$''';
execute ll2 using 'values ''$%f%bar'' ';		-- match

prepare ll3 as 'select 1 from t1 where ''Z%foobar'' like ? escape ''Z''';
execute ll3 using 'values ''ZZZ%foo%a_'' ';		-- MATCH

CREATE FUNCTION GETMAXCHAR() RETURNS CHAR(1) EXTERNAL NAME 'org.apache.derbyTesting.functionTests.tests.lang.CharUTF8.getMaxDefinedCharAsString' LANGUAGE JAVA PARAMETER STYLE JAVA;
 
--\uFA2D - the highest valid character according to Character.isDefined() of JDK 1.4;
--prepare ll4 as 'select 1 from t1 where ''\uFA2D'' like ?';
prepare ll4 as 'select 1 from t1 where GETMAXCHAR() like ?';
execute ll4 using 'values ''%'' ';
execute ll4 using 'values '''' ';
execute ll4 using 'values ''_'' ';
execute ll4 using 'values GETMAXCHAR() ';

-- create and populate tables
create table test(id char(10), c10 char(10), vc10 varchar(10));
insert into test values ('V-NULL', null, null);
insert into test values ('asdf', 'asdf', 'asdf');
insert into test values ('asdg', 'asdg', 'asdg');
insert into test values ('aasdf', 'aasdf', 'aasdf');
insert into test values ('%foobar', '%foobar', '%foobar');
insert into test values ('foo%bar', 'foo%bar', 'foo%bar');
insert into test values ('foo_bar', 'foo_bar', 'foo_bar');
insert into test values ('MAX_CHAR', '\uFA2D', '\uFA2D');

-- pushing generated predicates down
prepare p1 as 'select id from test where c10 like ?';
prepare p2 as 'select id from test where vc10 like ?';

select vc10 from test where vc10 like 'values cast(null as varchar(1))';
-- return 0 rows
execute p1 using 'values cast(null as char)';
execute p2 using 'values cast(null as varchar(1))';

-- false
execute p1 using 'values 1';
execute p2 using 'values 1';

-- false
execute p1 using 'values '''' ';
execute p2 using 'values '''' ';

-- true
execute p1 using 'values ''%'' ';
execute p2 using 'values ''%'' ';

-- fail, no end blankd pad
execute p1 using 'values ''%f'' ';
execute p2 using 'values ''%f'' ';

execute p1 using 'values cast(''%f'' as varchar(2)) ';
execute p2 using 'values cast(''%f'' as varchar(2)) ';

execute p1 using 'values ''%g'' ';
execute p2 using 'values ''%g'' ';

execute p1 using 'values ''asd%'' ';
execute p2 using 'values ''asd%'' ';

execute p1 using 'values ''_%'' ';
execute p2 using 'values ''_%'' ';

execute p1 using 'values ''%_'' ';
execute p2 using 'values ''%_'' ';

-- one: aasdf
execute p1 using 'values ''_asdf'' ';		-- fail: char blank padding significant
execute p1 using 'values ''_asdf   %'' ';
execute p2 using 'values ''_asdf'' ';

execute p1 using 'values ''%asdf'' ';		-- fail
execute p2 using 'values ''%asdf'' ';

-- verify that like optimization being performed
execute p2 using 'values ''%'' ';
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
create index i1 on test(vc10);


create table likeable (match_me varchar(10), pattern varchar(10), esc varchar(1));

insert into likeable values ('foo%bar', 'fooZ%bar', 'Z');
insert into likeable values ('foo%bar', '%Z%ba_', 'Z');
insert into likeable values ('foo%bar', 'fooZ%baZ', 'Z');	-- error
select match_me from likeable where match_me like pattern escape esc;

delete from likeable;
insert into likeable values ('foo%bar', 'foo%bar', NULL);	-- should error
select match_me from likeable where match_me like pattern escape esc;
delete from likeable;
insert into likeable values ('foo%bar', 'foo%bar', '');		-- should error
select match_me from likeable where match_me like pattern escape esc;

-- Defect 6002/6039
create table cei(id int, name varchar(192) not null, source varchar(252) not null);
insert into cei values (1, 'Alarms', 'AlarmDisk999'), 
		(2, 'Alarms', 'AlarmFS-usr'),
		(3, 'Alarms', 'AlarmPower'),
		(4, 'Alert', 'AlertBattery'),
		(5, 'Alert', 'AlertUPS'),
		(6, 'Warning', 'WarnIntrusion'),
		(7, 'Warning', 'WarnUnlockDoor'),
		(8, 'Warning', 'Warn%Unlock%Door'),
		(9, 'Warning', 'W_Unlock_Door');
select * from cei;

prepare s as 'select id, name, source from cei where (name LIKE ? escape ''\'') and (source like ? escape ''\'') order by source asc, name asc';

execute s using 'values (''%'', ''%'')';
execute s using 'values (''Alarms'', ''AlarmDisk%'')';
execute s using 'values (''A%'', ''%'')';
execute s using 'values (''%'',	''___rm%'')';
execute s using 'values (''Warning'', ''%oor'')';
execute s using 'values (''Warning'', ''Warn\%Unlock\%Door'')';
execute s using 'values (''Warning'', ''%\%Unlo%'')';
execute s using 'values (''Warning'', ''W\_Unloc%'')';
execute s using 'values (''Warning'', ''_\_Unlock\_Door'')';
execute s using 'values (''W%'', ''Warn\%Unlock\%Door'')';
execute s using 'values (''%ing'', ''W\_Unlock\_%Door'')';
execute s using 'values (''Bogus'', ''Name'')';

-- clean up
drop table test;
drop table likeable;
drop table cei;
