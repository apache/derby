--
-- this test shows the current supported comparison operators
--

-- first, do comparisons on the int type

-- create a table with couple of int columns
create table inttab (c1 int, c2 int);

-- insert some values
insert into inttab values (0, 0);
insert into inttab values (null, 5);
insert into inttab values (1, 1);
insert into inttab values (2147483647, 2147483647);

-- select each one in turn
select c1 from inttab where c1 = 0;
select c1 from inttab where c1 = 1;
select c1 from inttab where c1 = 2147483647;

-- now look for a value that isn't in the table
select c1 from inttab where c1 = 2;

-- now test null = null semantics
select c1 from inttab where c1 = c1;

-- test is null semantics
select c1 from inttab where c1 is null;
select c1 from inttab where c1 is not null;
select c1 from inttab where not c1 is null;

-- now test <>
select c1 from inttab where c1 <> 0;
select c1 from inttab where c1 <> 1;
select c1 from inttab where c1 <> 2147483647;
select c1 from inttab where c1 <> 2;
select c1 from inttab where c1 <> c1;
select c1 from inttab where c1 <> c2;

-- now test !=
select c1 from inttab where c1 != 0;
select c1 from inttab where c1 != 1;
select c1 from inttab where c1 != 2147483647;
select c1 from inttab where c1 != 2;
select c1 from inttab where c1 != c1;
select c1 from inttab where c1 != c2;

-- now test <
select c1 from inttab where c1 < 0;
select c1 from inttab where c1 < 1;
select c1 from inttab where c1 < 2;
select c1 from inttab where c1 < 2147483647;
select c1 from inttab where c1 < c1;
select c1 from inttab where c1 < c2;

-- now test >
select c1 from inttab where c1 > 0;
select c1 from inttab where c1 > 1;
select c1 from inttab where c1 > 2;
select c1 from inttab where c1 > 2147483647;
select c1 from inttab where c1 > c1;
select c1 from inttab where c1 > c2;

-- now test <=
select c1 from inttab where c1 <= 0;
select c1 from inttab where c1 <= 1;
select c1 from inttab where c1 <= 2;
select c1 from inttab where c1 <= 2147483647;
select c1 from inttab where c1 <= c1;
select c1 from inttab where c1 <= c2;

-- now test >=
select c1 from inttab where c1 >= 0;
select c1 from inttab where c1 >= 1;
select c1 from inttab where c1 >= 2;
select c1 from inttab where c1 >= 2147483647;
select c1 from inttab where c1 >= c1;
select c1 from inttab where c1 >= c2;

-- now test not
select c1 from inttab where not (c1 = 0);
select c1 from inttab where not (c1 <> 0);
select c1 from inttab where not (c1 != 0);
select c1 from inttab where not (c1 < 0);
select c1 from inttab where not (c1 <= 0);
select c1 from inttab where not (c1 > 0);
select c1 from inttab where not (c1 >= 0);

-- create a table with a couple of smallint columns.  All smallint vs. smallint
-- comparisons must be done between columns, because there are no smallint
-- constants in the language

create table smallinttab (c1 smallint, c2 smallint);

-- insert some values
insert into smallinttab values (0, 0);
insert into smallinttab values (null, null);
insert into smallinttab values (1, 1);
insert into smallinttab values (32767, 32767);
insert into smallinttab values (0, 9);
insert into smallinttab values (null, 8);
insert into smallinttab values (1, 7);
insert into smallinttab values (32767, 6);

-- select the ones where the columns are equal
select c1, c2 from smallinttab where c1 = c2;

-- test smallint = int semantics
select c1 from smallinttab where c1 = 0;
select c1 from smallinttab where c1 = 1;
select c1 from smallinttab where c1 = 32767;

-- test that the smallint gets promoted to int, and not vice versa.  65537
-- when converted to short becomes 1
select c1 from smallinttab where c1 = 65537;

-- test int = smallint semantics
select c1 from smallinttab where 0 = c1;
select c1 from smallinttab where 1 = c1;
select c1 from smallinttab where 32767 = c1;

-- test that the smallint gets promoted to int, and not vice versa.  65537
-- when converted to short becomes 1
select c1 from smallinttab where 65537 = c1;

-- Now test <>
select c1, c2 from smallinttab where c1 <> c2;
select c1, c2 from smallinttab where c1 != c2;

-- test smallint <> int semantics
select c1 from smallinttab where c1 <> 0;
select c1 from smallinttab where c1 <> 1;
select c1 from smallinttab where c1 <> 32767;

select c1 from smallinttab where c1 != 0;
select c1 from smallinttab where c1 != 1;
select c1 from smallinttab where c1 != 32767;

-- test that the smallint gets promoted to int, and not vice versa.  65537
-- when converted to short becomes 1
select c1 from smallinttab where c1 <> 65537;
select c1 from smallinttab where c1 != 65537;

-- test int = smallint semantics
select c1 from smallinttab where 0 <> c1;
select c1 from smallinttab where 1 <> c1;
select c1 from smallinttab where 32767 <> c1;

select c1 from smallinttab where 0 != c1;
select c1 from smallinttab where 1 != c1;
select c1 from smallinttab where 32767 != c1;

-- test that the smallint gets promoted to int, and not vice versa.  65537
-- when converted to short becomes 1
select c1 from smallinttab where 65537 <> c1;
select c1 from smallinttab where 65537 != c1;

-- Now test <
select c1, c2 from smallinttab where c1 < c2;

-- test smallint < int semantics
select c1 from smallinttab where c1 < 0;
select c1 from smallinttab where c1 < 1;
select c1 from smallinttab where c1 < 32767;

-- test that the smallint gets promoted to int, and not vice versa.  65537
-- when converted to short becomes 1
select c1 from smallinttab where c1 < 65537;

-- test int < smallint semantics
select c1 from smallinttab where 0 < c1;
select c1 from smallinttab where 1 < c1;
select c1 from smallinttab where 32767 < c1;

-- test that the smallint gets promoted to int, and not vice versa.  65537
-- when converted to short becomes 1
select c1 from smallinttab where 65537 < c1;

-- Now test >
select c1, c2 from smallinttab where c1 > c2;

-- test smallint > int semantics
select c1 from smallinttab where c1 > 0;
select c1 from smallinttab where c1 > 1;
select c1 from smallinttab where c1 > 32767;

-- test that the smallint gets promoted to int, and not vice versa.  65537
-- when converted to short becomes 1
select c1 from smallinttab where c1 > 65537;

-- test int > smallint semantics
select c1 from smallinttab where 0 > c1;
select c1 from smallinttab where 1 > c1;
select c1 from smallinttab where 32767 > c1;

-- test that the smallint gets promoted to int, and not vice versa.  65537
-- when converted to short becomes 1
select c1 from smallinttab where 65537 > c1;

-- Now test <=
select c1, c2 from smallinttab where c1 <= c2;

-- test smallint <= int semantics
select c1 from smallinttab where c1 <= 0;
select c1 from smallinttab where c1 <= 1;
select c1 from smallinttab where c1 <= 32767;

-- test that the smallint gets promoted to int, and not vice versa.  65537
-- when converted to short becomes 1
select c1 from smallinttab where c1 <= 65537;

-- test int <= smallint semantics
select c1 from smallinttab where 0 <= c1;
select c1 from smallinttab where 1 <= c1;
select c1 from smallinttab where 32767 <= c1;

-- test that the smallint gets promoted to int, and not vice versa.  65537
-- when converted to short becomes 1
select c1 from smallinttab where 65537 <= c1;

-- Now test >=
select c1, c2 from smallinttab where c1 >= c2;

-- test smallint >= int semantics
select c1 from smallinttab where c1 >= 0;
select c1 from smallinttab where c1 >= 1;
select c1 from smallinttab where c1 >= 32767;

-- test that the smallint gets promoted to int, and not vice versa.  65537
-- when converted to short becomes 1
select c1 from smallinttab where c1 >= 65537;

-- test int >= smallint semantics
select c1 from smallinttab where 0 >= c1;
select c1 from smallinttab where 1 >= c1;
select c1 from smallinttab where 32767 >= c1;

-- test is null semantics
select c1 from smallinttab where c1 is null;
select c1 from smallinttab where c1 is not null;
select c1 from smallinttab where not c1 is null;

-- test that the smallint gets promoted to int, and not vice versa.  65537
-- when converted to short becomes 1
select c1 from smallinttab where 65537 >= c1;


-- create a table with a couple of bigint columns.  

create table biginttab (c1 bigint, c2 bigint, c3 int, c4 smallint);

-- insert some values
insert into biginttab values (0, 0, 0, 0 );
insert into biginttab values (null, null, null, null);
insert into biginttab values (9223372036854775807, 
							   9223372036854775807,
							   2147483647,
							   32767);
insert into biginttab values (-9223372036854775808, 
							   -9223372036854775808,
							   -2147483648,
							   -32768);

-- select the ones where the columns are equal
select c1, c2 from biginttab where c1 = c2;

-- test bigint = int semantics
select c1 from biginttab where c1 = 0;
select c1 from biginttab where c1 = c3;

-- test int = bigint semantics
select c1 from biginttab where 0 = c1;
select c1 from biginttab where c3 = c1;

-- test bigint = smallint semantics
select c1 from biginttab where c1 = c4;

-- test smallint = bigint semantics
select c1 from biginttab where c4 = c1;

-- Now test <>
select c1, c2 from biginttab where c1 <> c2;

-- test bigint <> int semantics
select c1 from biginttab where c1 <> 0;
select c1 from biginttab where c1 <> c3;

-- test int <> bigint semantics
select c1 from biginttab where 0 <> c1;
select c1 from biginttab where c3 <> c1;

-- test bigint <> smallint semantics
select c1 from biginttab where c1 <> c4;

-- test smallint <> bigint semantics
select c1 from biginttab where c4 <> c1;


-- Now test <
select c1, c2 from biginttab where c1 < c2;

-- test bigint < int semantics
select c1 from biginttab where c1 < 0;
select c1 from biginttab where c1 < c3;

-- test int < bigint semantics
select c1 from biginttab where 0 < c1;
select c1 from biginttab where c3 < c1;

-- test bigint < smallint semantics
select c1 from biginttab where c1 < c4;

-- test smallint < bigint semantics
select c1 from biginttab where c4 < c1;

-- Now test >
select c1, c2 from biginttab where c1 > c2;

-- test bigint > int semantics
select c1 from biginttab where c1 > 0;
select c1 from biginttab where c1 > c3;

-- test int > bigint semantics
select c1 from biginttab where 0 > c1;
select c1 from biginttab where c3 > c1;

-- test bigint > smallint semantics
select c1 from biginttab where c1 > c4;

-- test smallint > bigint semantics
select c1 from biginttab where c4 > c1;

-- Now test <=
select c1, c2 from biginttab where c1 <= c2;

-- test bigint <= int semantics
select c1 from biginttab where c1 <= 0;
select c1 from biginttab where c1 <= c3;

-- test int <= bigint semantics
select c1 from biginttab where 0 <= c1;
select c1 from biginttab where c3 <= c1;

-- test bigint <= smallint semantics
select c1 from biginttab where c1 <= c4;

-- test smallint <= bigint semantics
select c1 from biginttab where c4 <= c1;

-- Now test >=
select c1, c2 from biginttab where c1 >= c2;

-- test bigint >= int semantics
select c1 from biginttab where c1 >= 0;
select c1 from biginttab where c1 >= c3;

-- test int >= bigint semantics
select c1 from biginttab where 0 >= c1;
select c1 from biginttab where c3 >= c1;

-- test bigint >= smallint semantics
select c1 from biginttab where c1 >= c4;

-- test smallint >= bigint semantics
select c1 from biginttab where c4 >= c1;

-- test is null semantics
select c1 from biginttab where c1 is null;
select c1 from biginttab where c1 is not null;
select c1 from biginttab where not c1 is null;

-- create a table with char columns of different lengths

create table chartab (c1 char(1), c2 char(5));

-- insert some values

insert into chartab values (' ', '     ');
insert into chartab values ('a', 'a    ');
insert into chartab values ('b', 'bcdef');
insert into chartab values (null, null);

-- select each one in turn
select c1 from chartab where c1 = ' ';
select c2 from chartab where c2 = '     ';
select c1 from chartab where c1 = 'a';
select c2 from chartab where c2 = 'a    ';
select c1 from chartab where c1 = 'b';
select c2 from chartab where c2 = 'bcdef';

-- now check for end-of-string blank semantics
select c1 from chartab where c1 = '';
select c1 from chartab where c1 = '                      ';
select c2 from chartab where c2 = '';
select c2 from chartab where c2 = ' ';
select c2 from chartab where c2 = '                           ';
select c1 from chartab where c1 = 'a        ';
select c2 from chartab where c2 = 'a ';
select c1 from chartab where c1 = 'b             ';
select c2 from chartab where c2 = 'bcdef                ';
select c2 from chartab where c2 = 'bcde       ';

-- now check null = null semantics
select c1, c2 from chartab where c1 = c2;

-- test is null semantics
select c1 from chartab where c1 is null;
select c1 from chartab where c1 is not null;
select c1 from chartab where not c1 is null;

-- Now test <>
select c1 from chartab where c1 <> ' ';
select c2 from chartab where c2 <> '     ';
select c1 from chartab where c1 <> 'a';
select c2 from chartab where c2 <> 'a    ';
select c1 from chartab where c1 <> 'b';
select c2 from chartab where c2 <> 'bcdef';

select c1 from chartab where c1 != ' ';
select c2 from chartab where c2 != '     ';
select c1 from chartab where c1 != 'a';
select c2 from chartab where c2 != 'a    ';
select c1 from chartab where c1 != 'b';
select c2 from chartab where c2 != 'bcdef';

-- now check for end-of-string blank semantics
select c1 from chartab where c1 <> '';
select c1 from chartab where c1 <> '                      ';
select c2 from chartab where c2 <> '';
select c2 from chartab where c2 <> ' ';
select c2 from chartab where c2 <> '                           ';
select c1 from chartab where c1 <> 'a        ';
select c2 from chartab where c2 <> 'a ';
select c1 from chartab where c1 <> 'b             ';
select c2 from chartab where c2 <> 'bcdef                ';
select c2 from chartab where c2 <> 'bcde       ';

-- now check null <> null semantics
select c1, c2 from chartab where c1 <> c2;

-- Now test <
select c1 from chartab where c1 < ' ';
select c2 from chartab where c2 < '     ';
select c1 from chartab where c1 < 'a';
select c2 from chartab where c2 < 'a    ';
select c1 from chartab where c1 < 'b';
select c2 from chartab where c2 < 'bcdef';

-- now check for end-of-string blank semantics
select c1 from chartab where c1 < '';
select c1 from chartab where c1 < '                      ';
select c2 from chartab where c2 < '';
select c2 from chartab where c2 < ' ';
select c2 from chartab where c2 < '                           ';
select c1 from chartab where c1 < 'a        ';
select c2 from chartab where c2 < 'a ';
select c1 from chartab where c1 < 'b             ';
select c2 from chartab where c2 < 'bcdef                ';
select c2 from chartab where c2 < 'bcde       ';

-- now check null < null semantics
select c1, c2 from chartab where c1 < c2;

-- Now test >
select c1 from chartab where c1 > ' ';
select c2 from chartab where c2 > '     ';
select c1 from chartab where c1 > 'a';
select c2 from chartab where c2 > 'a    ';
select c1 from chartab where c1 > 'b';
select c2 from chartab where c2 > 'bcdef';

-- now check for end-of-string blank semantics
select c1 from chartab where c1 > '';
select c1 from chartab where c1 > '                      ';
select c2 from chartab where c2 > '';
select c2 from chartab where c2 > ' ';
select c2 from chartab where c2 > '                           ';
select c1 from chartab where c1 > 'a        ';
select c2 from chartab where c2 > 'a ';
select c1 from chartab where c1 > 'b             ';
select c2 from chartab where c2 > 'bcdef                ';
select c2 from chartab where c2 > 'bcde       ';

-- now check null > null semantics
select c1, c2 from chartab where c1 > c2;

-- Now test <=
select c1 from chartab where c1 <= ' ';
select c2 from chartab where c2 <= '     ';
select c1 from chartab where c1 <= 'a';
select c2 from chartab where c2 <= 'a    ';
select c1 from chartab where c1 <= 'b';
select c2 from chartab where c2 <= 'bcdef';

-- now check for end-of-string blank semantics
select c1 from chartab where c1 <= '';
select c1 from chartab where c1 <= '                      ';
select c2 from chartab where c2 <= '';
select c2 from chartab where c2 <= ' ';
select c2 from chartab where c2 <= '                           ';
select c1 from chartab where c1 <= 'a        ';
select c2 from chartab where c2 <= 'a ';
select c1 from chartab where c1 <= 'b             ';
select c2 from chartab where c2 <= 'bcdef                ';
select c2 from chartab where c2 <= 'bcde       ';

-- now check null <= null semantics
select c1, c2 from chartab where c1 <= c2;

-- Now test >=
select c1 from chartab where c1 >= ' ';
select c2 from chartab where c2 >= '     ';
select c1 from chartab where c1 >= 'a';
select c2 from chartab where c2 >= 'a    ';
select c1 from chartab where c1 >= 'b';
select c2 from chartab where c2 >= 'bcdef';

-- now check for end-of-string blank semantics
select c1 from chartab where c1 >= '';
select c1 from chartab where c1 >= '                      ';
select c2 from chartab where c2 >= '';
select c2 from chartab where c2 >= ' ';
select c2 from chartab where c2 >= '                           ';
select c1 from chartab where c1 >= 'a        ';
select c2 from chartab where c2 >= 'a ';
select c1 from chartab where c1 >= 'b             ';
select c2 from chartab where c2 >= 'bcdef                ';
select c2 from chartab where c2 >= 'bcde       ';

-- now check null >= null semantics
select c1, c2 from chartab where c1 >= c2;

-- create a table with a few varchar columns.  All varchar vs. varchar
-- comparisons must be done between columns, because there are no varchar
-- constants in the language

create table varchartab (c1 varchar(1), c2 varchar(1), c3 varchar(5),
			 c4 varchar(5));

-- insert some values

insert into varchartab values ('', '', '', '');
insert into varchartab values ('a', 'a', 'a', 'a');
insert into varchartab values ('b', 'b', 'bcdef', 'bcdef');
insert into varchartab values (null, null, null, null);
insert into varchartab values ('', null, '', null);
insert into varchartab values ('a', 'b', 'a', 'b');
insert into varchartab values ('b', '', 'b', 'bcdef');

-- select the ones where the columns are equal
select c1 from varchartab where c1 = c2;
select c3 from varchartab where c3 = c4;

-- test varchar = char semantics.  Test with trailing blanks.
select c1 from varchartab where c1 = '                 ';
select c1 from varchartab where c1 = '';
select c1 from varchartab where c1 = 'a ';
select c1 from varchartab where c1 = 'b                               ';
select c1 from varchartab where c1 = 'bb';
select c3 from varchartab where c3 = ' ';
select c3 from varchartab where c3 = '';
select c3 from varchartab where c3 = 'a    ';
select c3 from varchartab where c3 = 'bcdef                   ';
select c3 from varchartab where c3 = 'bbbb';

-- test char = varchar semantics.  Test with trailing blanks.
select c1 from varchartab where '                 ' = c1;
select c1 from varchartab where '' = c1;
select c1 from varchartab where 'a ' = c1;
select c1 from varchartab where 'b                               ' = c1;
select c1 from varchartab where 'bb' = c1;
select c3 from varchartab where ' ' = c3;
select c3 from varchartab where '' = c3;
select c3 from varchartab where 'a    ' = c3;
select c3 from varchartab where 'bcdef                   ' = c3;
select c3 from varchartab where 'bbbb' = c3;

-- Now test <>
select c1 from varchartab where c1 <> c2;
select c3 from varchartab where c3 <> c4;

-- test varchar <> char semantics.  Test with trailing blanks.
select c1 from varchartab where c1 <> '                 ';
select c1 from varchartab where c1 <> '';
select c1 from varchartab where c1 <> 'a ';
select c1 from varchartab where c1 <> 'b                               ';
select c1 from varchartab where c1 <> 'bb';
select c3 from varchartab where c3 <> ' ';
select c3 from varchartab where c3 <> '';
select c3 from varchartab where c3 <> 'a    ';
select c3 from varchartab where c3 <> 'bcdef                   ';
select c3 from varchartab where c3 <> 'bbbb';

select c1 from varchartab where c1 != '                 ';
select c1 from varchartab where c1 != '';
select c1 from varchartab where c1 != 'a ';
select c1 from varchartab where c1 != 'b                               ';
select c1 from varchartab where c1 != 'bb';
select c3 from varchartab where c3 != ' ';
select c3 from varchartab where c3 != '';
select c3 from varchartab where c3 != 'a    ';
select c3 from varchartab where c3 != 'bcdef                   ';
select c3 from varchartab where c3 != 'bbbb';

-- test char <> varchar semantics.  Test with trailing blanks.
select c1 from varchartab where '                 ' <> c1;
select c1 from varchartab where '' <> c1;
select c1 from varchartab where 'a ' <> c1;
select c1 from varchartab where 'b                               ' <> c1;
select c1 from varchartab where 'bb' <> c1;
select c3 from varchartab where ' ' <> c3;
select c3 from varchartab where '' <> c3;
select c3 from varchartab where 'a    ' <> c3;
select c3 from varchartab where 'bcdef                   ' <> c3;
select c3 from varchartab where 'bbbb' <> c3;

-- Now test <
select c1 from varchartab where c1 < c2;
select c3 from varchartab where c3 < c4;

-- test varchar < char semantics.  Test with trailing blanks.
select c1 from varchartab where c1 < '                 ';
select c1 from varchartab where c1 < '';
select c1 from varchartab where c1 < 'a ';
select c1 from varchartab where c1 < 'b                               ';
select c1 from varchartab where c1 < 'bb';
select c3 from varchartab where c3 < ' ';
select c3 from varchartab where c3 < '';
select c3 from varchartab where c3 < 'a    ';
select c3 from varchartab where c3 < 'bcdef                   ';
select c3 from varchartab where c3 < 'bbbb';

-- test char < varchar semantics.  Test with trailing blanks.
select c1 from varchartab where '                 ' < c1;
select c1 from varchartab where '' < c1;
select c1 from varchartab where 'a ' < c1;
select c1 from varchartab where 'b                               ' < c1;
select c1 from varchartab where 'bb' < c1;
select c3 from varchartab where ' ' < c3;
select c3 from varchartab where '' < c3;
select c3 from varchartab where 'a    ' < c3;
select c3 from varchartab where 'bcdef                   ' < c3;
select c3 from varchartab where 'bbbb' < c3;

-- Now test >
select c1 from varchartab where c1 > c2;
select c3 from varchartab where c3 > c4;

-- test varchar > char semantics.  Test with trailing blanks.
select c1 from varchartab where c1 > '                 ';
select c1 from varchartab where c1 > '';
select c1 from varchartab where c1 > 'a ';
select c1 from varchartab where c1 > 'b                               ';
select c1 from varchartab where c1 > 'bb';
select c3 from varchartab where c3 > ' ';
select c3 from varchartab where c3 > '';
select c3 from varchartab where c3 > 'a    ';
select c3 from varchartab where c3 > 'bcdef                   ';
select c3 from varchartab where c3 > 'bbbb';

-- test char > varchar semantics.  Test with trailing blanks.
select c1 from varchartab where '                 ' > c1;
select c1 from varchartab where '' > c1;
select c1 from varchartab where 'a ' > c1;
select c1 from varchartab where 'b                               ' > c1;
select c1 from varchartab where 'bb' > c1;
select c3 from varchartab where ' ' > c3;
select c3 from varchartab where '' > c3;
select c3 from varchartab where 'a    ' > c3;
select c3 from varchartab where 'bcdef                   ' > c3;
select c3 from varchartab where 'bbbb' > c3;

-- Now test <=
select c1 from varchartab where c1 <= c2;
select c3 from varchartab where c3 <= c4;

-- test varchar <= char semantics.  Test with trailing blanks.
select c1 from varchartab where c1 <= '                 ';
select c1 from varchartab where c1 <= '';
select c1 from varchartab where c1 <= 'a ';
select c1 from varchartab where c1 <= 'b                               ';
select c1 from varchartab where c1 <= 'bb';
select c3 from varchartab where c3 <= ' ';
select c3 from varchartab where c3 <= '';
select c3 from varchartab where c3 <= 'a    ';
select c3 from varchartab where c3 <= 'bcdef                   ';
select c3 from varchartab where c3 <= 'bbbb';

-- test char <= varchar semantics.  Test with trailing blanks.
select c1 from varchartab where '                 ' <= c1;
select c1 from varchartab where '' <= c1;
select c1 from varchartab where 'a ' <= c1;
select c1 from varchartab where 'b                               ' <= c1;
select c1 from varchartab where 'bb' <= c1;
select c3 from varchartab where ' ' <= c3;
select c3 from varchartab where '' <= c3;
select c3 from varchartab where 'a    ' <= c3;
select c3 from varchartab where 'bcdef                   ' <= c3;
select c3 from varchartab where 'bbbb' <= c3;

-- Now test >=
select c1 from varchartab where c1 >= c2;
select c3 from varchartab where c3 >= c4;

-- test varchar >= char semantics.  Test with trailing blanks.
select c1 from varchartab where c1 >= '                 ';
select c1 from varchartab where c1 >= '';
select c1 from varchartab where c1 >= 'a ';
select c1 from varchartab where c1 >= 'b                               ';
select c1 from varchartab where c1 >= 'bb';
select c3 from varchartab where c3 >= ' ';
select c3 from varchartab where c3 >= '';
select c3 from varchartab where c3 >= 'a    ';
select c3 from varchartab where c3 >= 'bcdef                   ';
select c3 from varchartab where c3 >= 'bbbb';

-- test char >= varchar semantics.  Test with trailing blanks.
select c1 from varchartab where '                 ' >= c1;
select c1 from varchartab where '' >= c1;
select c1 from varchartab where 'a ' >= c1;
select c1 from varchartab where 'b                               ' >= c1;
select c1 from varchartab where 'bb' >= c1;
select c3 from varchartab where ' ' >= c3;
select c3 from varchartab where '' >= c3;
select c3 from varchartab where 'a    ' >= c3;
select c3 from varchartab where 'bcdef                   ' >= c3;
select c3 from varchartab where 'bbbb' >= c3;

-- test is null semantics
select c1 from varchartab where c1 is null;
select c1 from varchartab where c1 is not null;
select c1 from varchartab where not c1 is null;

-- clean up
drop table inttab;
drop table smallinttab;
drop table biginttab;
drop table chartab;
drop table varchartab;
