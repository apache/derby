
-- This test EJBQL Absolute function. Resolve 3535
-- Begin of ABS/ABSVAL test.  For all valid types, un-escaped function.


-- Integer has a range of -2147483648 to 2147483647
-- Basic
create table myint( a int );
create table myinteger( a integer );
select abs(a) from myint;
select abs(a) from myinteger;
insert into myint values (null), (+0), (-0), (+1), (-1), (1000), (-1000), (null), (2147483647), (-2147483647);
insert into myinteger values (NULL), (+0), (-0), (+1), (-1), (1000), (-1000), (NULL), (2147483647), (-2147483647);
select a from myint;
select a from myinteger;
select abs(a) from myint;
select abs(a) from myinteger;
select -abs(a) from myint;
select -abs(a) from myinteger;
select abs(abs(-abs(-abs(a)))) from myint;
select abs(abs(-abs(-abs(a)))) from myinteger;
SELECT ABSVAL(ABSVAL(-ABSVAL(-ABSVAL(A)))) FROM MYINT;
SELECT ABSVAL(ABSVAL(-ABSVAL(-ABSVAL(A)))) FROM MYINTEGER;
insert into myint values (-2147483648);
insert into myinteger values (-2147483648);
select a from myint where a=-2147483648;
select a from myinteger where a=-2147483648;
-- Error
select -a from myint where a=-2147483648;
select -a from myinteger where a=-2147483648;
select abs(-a) from myint where a=-2147483648;
select abs(-a) from myinteger where a=-2147483648;
select abs(a) from myint where a=-2147483648;
select abs(a) from myinteger where a=-2147483648;
select abs(-abs(a)) from myint where a=-2147483648;
select abs(-abs(a)) from myinteger where a=-2147483648;
drop table myint;
drop table myinteger;
-- End of Integer test


-- Smallint has a range of -32768 to 32767
-- Basic
create table mysmallint( a smallint );
select abs(a) from mysmallint;
insert into mysmallint values (null), (+0), (-0), (+1), (-1), (1000), (-1000), (null), (32767), (-32767);
select a from mysmallint;
select abs(a) from mysmallint;
select -abs(a) from mysmallint;
select abs(abs(-abs(-abs(a)))) from mysmallint;
SELECT ABSVAL(ABSVAL(-ABSVAL(-ABSVAL(A)))) FROM MYSMALLINT;
insert into mysmallint values (-32768);
select a from mysmallint where a=-32768;
-- Error
select -a from mysmallint where a=-32768;
select abs(-a) from mysmallint where a=-32768;
select abs(a) from mysmallint where a=-32768;
select abs(-abs(a)) from mysmallint where a=-32768;
drop table mysmallint;
-- End of Smallint test


-- Bigint has a range of -9223372036854775808 to 9223372036854775807
-- Basic
create table mybigint( a bigint );
select abs(a) from mybigint;
insert into mybigint values (null), (+0), (-0), (+1), (-1), (1000), (-1000), (null), (9223372036854775807), (-9223372036854775807);
select a from mybigint;
select abs(a) from mybigint;
select -abs(a) from mybigint;
select abs(abs(-abs(-abs(a)))) from mybigint;
SELECT ABSVAL(ABSVAL(-ABSVAL(-ABSVAL(A)))) FROM MYBIGINT;
insert into mybigint values (-9223372036854775808);
select a from mybigint where a=-9223372036854775808;
-- Error
select -a from mybigint where a=-9223372036854775808;
select abs(-a) from mybigint where a=-9223372036854775808;
select abs(a) from mybigint where a=-9223372036854775808;
select abs(-abs(a)) from mybigint where a=-9223372036854775808;
drop table mybigint;
-- End of Bigint test


-- REAL has a range of +/-1.175E-37 to +/-3.402E+38 
-- Basic
create table myreal( a real );
select abs(a) from myreal;
insert into myreal values (null), (+0), (-0), (+1), (-1), (null), (100000000), (-100000000),
(3.402E+38), (-3.402E+38),
(1.175E-37), (-1.175E-37);
select a from myreal;
select -a from myreal;
select abs(a) from myreal;
select abs(-a) from myreal;
select -abs(a) from myreal;
select abs(abs(-abs(-abs(a)))) from myreal;
SELECT ABSVAL(ABSVAL(-ABSVAL(-ABSVAL(A)))) FROM MYREAL;
select distinct abs(a) from myreal;
---- There is nothing wrong with returning 1.0.  The float overflows and this is just the way it behaves.
-- this used to work on CS, not any more when adopted to DB2 style floats
-- since contant numbers are (parsed as) doubles
select abs(-abs(a)) + 1 from myreal where a=1.175E-37;
-- when casted to a real, it is found
select abs(-abs(a)) + 1 from myreal where a=cast(1.175E-37 as real);
-- Error
insert into myreal values ( 3.402E+38 *2);
insert into myreal values (-3.402E+38 *2);
drop table myreal;
-- End of Real test


-- Double Precision has a range of +/-2.225E-307 to +/-1.79769E+308 
-- Basic
create table mydoubleprecision( a double precision );
select abs(a) from mydoubleprecision;
insert into mydoubleprecision values (null), (+0), (-0), (+1), (-1), (100000000), (-100000000), (null),
(1.79769E+308), (-1.79769E+308),
(2.225E-307), (-2.225E-307);
select a from mydoubleprecision;
select -a from mydoubleprecision;
select abs(a) from mydoubleprecision;
select abs(-a) from mydoubleprecision;
select -abs(a) from mydoubleprecision;
select abs(abs(-abs(-abs(a)))) from mydoubleprecision;
SELECT ABSVAL(ABSVAL(-ABSVAL(-ABSVAL(A)))) FROM MYDOUBLEPRECISION;
select distinct abs(a) from mydoubleprecision;
-- There is nothing wrong with returning 1.0.  The double overflows and this is just the way it behaves.
select abs(-abs(a)) + 1 from mydoubleprecision where a=2.225E-307;
-- Error
insert into mydoubleprecision values ( 1.79769E+308 *2);
insert into mydoubleprecision values (-1.79769E+308 *2);
drop table mydoubleprecision;
-- End of Double Precision test


-- Float has a the range or a REAL or DOUBLE depending on
-- the precision you specify.  Below a is a double, b is a float
create table myfloat( a float, b float(23) );
select abs(a), abs(b) from myfloat;
select columnname, columndatatype
from sys.syscolumns c, sys.systables t where c.referenceid = t.tableid and t.tablename='MYFLOAT';
insert into myfloat values (null, null), (+0, +0), (-0, -0), (+1, +1), (-1, -1),
(100000000, 100000000), (-100000000, -100000000), (null, null),
(1.79769E+308, 3.402E+38),
(-1.79769E+308, -3.402E+38),
(2.225E-307, 1.175E-37),
(-2.225E-307, -1.175E-37);
select a, b from myfloat;
select -a, -b from myfloat;
select abs(a), abs(b) from myfloat;
select abs(-a), abs(-b) from myfloat;
select -abs(a), -abs(b) from myfloat;
select abs(abs(-abs(-abs(a)))), abs(abs(-abs(-abs(b)))) from myfloat;
SELECT ABSVAL(ABSVAL(-ABSVAL(-ABSVAL(A)))), ABSVAL(ABSVAL(-ABSVAL(-ABSVAL(B)))) FROM MYFLOAT;
select distinct abs(a) from myfloat;
-- -- There is nothing wrong with returning 1.0.  The float overflows and this is just the way it behaves.
-- this used to work in CS, but no more, = on floating point values isn't really useful
select abs(-abs(a)) + 1, abs(-abs(b)) + 1 from myfloat where a=2.225E-307 AND b=1.175E-37;
select abs(-abs(a)) + 1, abs(-abs(b)) + 1 from myfloat where b=3.402E+38;
-- 'real =' works on DB2 and DB2 Cloudscape
select abs(-abs(a)) + 1, abs(-abs(b)) + 1 from myfloat where b=cast(3.402E+38 as real);
select abs(-abs(a)) + 1, abs(-abs(b)) + 1 from myfloat where a=2.225E-307 AND b=cast(1.175E-37 as real);
select abs(-abs(a)) + 1, abs(-abs(b)) + 1 from myfloat where a=2.225E-307;
-- Error
insert into myfloat values ( 1.79769E+308 *2, 3.402E+38 *2);
insert into myfloat values (-1.79769E+308 *2, -3.402E+38 *2);
insert into myfloat values ( 2.225E-307, 3.402E+38 *2);
insert into myfloat values (-2.225E-307, -3.402E+38 *2);
drop table myfloat;
-- End of Float test


-- Decimal is java.math.BigDecimal
-- Basic
create table myDecimal( a decimal(31, 0), b decimal(31,31));
select abs(a) from myDecimal;
insert into myDecimal values (null,0), (+0,0), (-0,0), (+1,0), (-1,0), 
(100000000,.10000000), (-100000000,-.10000000), (null,null), 
(1.0e30, 1.0e-30), 
(-1.0e30, -1.0e-30);
select a from myDecimal;
select -a from myDecimal;
select b from myDecimal;
select -b from myDecimal;
select abs(a) from myDecimal;
select abs(-a) from myDecimal;
select -abs(a) from myDecimal;
select abs(abs(-abs(-abs(a)))) from myDecimal;
SELECT ABSVAL(ABSVAL(-ABSVAL(-ABSVAL(A)))) FROM MYDECIMAL;
select distinct abs(a) from myDecimal;
select abs(b) from myDecimal;
select abs(-b) from myDecimal;
select -abs(b) from myDecimal;
select abs(abs(-abs(-abs(b)))) from myDecimal;
SELECT ABSVAL(ABSVAL(-ABSVAL(-ABSVAL(B)))) FROM MYDECIMAL;
select distinct abs(b) from myDecimal;
-- There is nothing wrong with returning 1.0.  The decimal overflows and this is just the way it behaves.  Needs to make this compatible with jdk1.1.8(which had a bug).
select abs(-abs(a)) + 1 from myDecimal;
drop table myDecimal;
-- End of Decimal test


-- Numeric java.math.BigDecimal
-- Basic
create table myNumeric( a decimal(31,0), b decimal(31,31 ));
select abs(a) from myNumeric;
insert into myNumeric values (null), (+0), (-0), (+1), (-1),
(100000000), (-100000000), (null),
(1.0e31, ,1.0e-31),
(-1.0e31, -1.0e-31 ), 
select a from myNumeric;
select -a from myNumeric;
select b from myNumeric;
select -b from myNumeric;
select abs(a), abs(b)from myNumeric;
select abs(-a), abs(-b) from myNumeric;
select -abs(a), -abs(b)  from myNumeric;
select abs(abs(-abs(-abs(a)))) from myNumeric;
SELECT ABSVAL(ABSVAL(-ABSVAL(-ABSVAL(A)))) FROM MYNUMERIC; 
select distinct abs(a) from myNumeric;
-- There is nothing wrong with returning 1.0.  The numeric overflows and this is just the way it behaves.  Needs to make this compatible with jdk1.1.8(which had a bug).
select abs(-abs(a)) + 1 from myNumeric;
drop table myNumeric;
-- End of Numeric test

-- Test some different statements, just in case
create table foo( a int );
insert into foo values ( abs( 1) );
insert into foo values ( abs(-2) );
insert into foo values (-abs(-3) );
insert into foo values (-abs( 4) );
insert into foo values (          -5  );
insert into foo values (          -6  );
insert into foo values (          -7  );

autocommit off;
prepare p1 as 'select a from foo';
prepare p2 as 'select abs(a) from foo';
prepare p3 as 'insert into foo select a*(-1) from foo';
execute p1;
execute p2;
execute p3;
execute p1;
insert into foo values( abs( 8 ) );
insert into foo values( abs(-9 ) );
insert into foo values(-abs(-10) );
insert into foo values( abs( 11) );
insert into foo values(          -12  );
execute p1;
execute p2;
execute p3;
execute p1;
rollback;
commit;

autocommit on;
insert into foo values( abs( 13) );
insert into foo values( abs(-14) );
insert into foo values(-abs(-15) );
insert into foo values(-abs( 16) );
insert into foo values(          -17  );
execute p1;
execute p2;
execute p3;
execute p1;
select * from foo;
drop table foo;
-- End of ABS/ABSVAL test.  For all valid types.  Un-escaped function.

-- abs is not a reserved word
create table abs( a int );
drop table abs;

-- This test EJBQL Absolute function. Resolve 3535
-- Begin of ABS test.  For escape function.

-- Integer
-- Basic
-- beetle 5805 - support INT[EGER] built-in function
values{fn abs(INT(' 0')               )};
values{fn abs(INT('-0')               )};
values{fn abs(INT(' 1')               )};
values{fn abs(INT('-1')               )};
values{fn abs(INT(' 1000000')         )};
values{fn abs(INT('-1000000')         )};
values{fn abs(INT(' 2147483647')      )};
values{fn abs(INT('-2147483648') + 1  )};
-- Error
values{fn abs(INT('-2147483648')      )};
values{fn abs(INT(' 2147483647') + 1  )};

-- Smallint
-- Basic
-- beetle 5807 - support SMALLINT built-in function
values{fn abs( SMALLINT(' 0')         )};
values{fn abs( SMALLINT('-0')         )};
values{fn abs( SMALLINT(' 1')         )};
values{fn abs( SMALLINT('-1')         )};
values{fn abs( SMALLINT(' 10000')     )};
values{fn abs( SMALLINT('-10000')     )};
values{fn abs( SMALLINT(' 32767')     )};
values{fn abs( SMALLINT('-32768') + 1 )};
values{fn abs(-SMALLINT('-32768')     )};
-- Error
values{fn abs(-SMALLINT(' 32768')     )};
values{fn abs( SMALLINT('-32768')     )};

-- Bigint
-- Basic
-- beetle 5809 - support BIGINT built-in function
values{fn abs( BIGINT(' 0')                       )};
values{fn abs( BIGINT('-0')                       )};
values{fn abs( BIGINT(' 1')                       )};
values{fn abs( BIGINT('-1')                       )};
values{fn abs( BIGINT(' 100000000000')            )};
values{fn abs( BIGINT('-100000000000')            )};
values{fn abs( BIGINT(' 9223372036854775807')     )};
values{fn abs( BIGINT('-9223372036854775808') + 1 )};
-- Error
values{fn abs(-BIGINT('-9223372036854775808')     )};
values{fn abs( BIGINT('-9223372036854775808')     )};

-- Real
-- Basic
-- beetle 5806 - support REAL built-in function
values{fn abs( REAL( 0)                       )};
values{fn abs( REAL(-0)                       )};
values{fn abs( REAL( 1)                       )};
values{fn abs( REAL(-1)                       )};
values{fn abs( REAL( 1000000.001)             )};
values{fn abs( REAL(-1000000.001)             )};
values{fn abs( REAL( 3.402E+38)               )};
values{fn abs( REAL(-3.402E+38) + 1           )};
-- Error
values{fn abs( REAL( 3.402E+38 * 2)           )};
values{fn abs(-REAL( NaN)                     )};
values{fn abs( REAL( 1.40129846432481707e-45) )};
values{fn abs( REAL( 3.40282346638528860e+38) )};

-- Double Precision/Double
-- Basic
-- beetle 5803 - support DOUBLE_[PRECISION] built-in function
values{fn abs( DOUBLE( 0)                      )};
values{fn abs( DOUBLE(-0)                      )};
values{fn abs( DOUBLE( 1)                      )};
values{fn abs( DOUBLE(-1)                      )};
values{fn abs( DOUBLE( 1000000.001)            )};
values{fn abs( DOUBLE(-1000000.001)            )};
values{fn abs( DOUBLE(-1.79769E+308)           )};
values{fn abs( DOUBLE( 1.79769E+308) + 1       )};
values{fn abs( DOUBLE( 2.225E-307 + 1)         )};
-- Error
values{fn abs( DOUBLE( 1.79769E+308 * 2)       )};
values{fn abs(-DOUBLE( NaN)                    )};
values{fn abs( DOUBLE( 4.9E-324)               )};
values{fn abs( DOUBLE( 1.7976931348623157E308) )};

-- Decimal/Numeric
-- Basic
-- beetle 5802 - support DEC[IMAL] built-in function
values{ fn abs(DEC( 0)             )};
values{ fn abs(DEC(-0)             )};
values{ fn abs(DEC( 1)             )};
values{ fn abs(DEC(-1)             )};
values{ fn abs(DEC( 1000000000000) )};
values{ fn abs(DEC(-1000000000000) )};


-- More generic test
values{ fn abs( 0-1-.1 ) };
values{ fn abs( -0-1.000000001 ) };
VALUES{ FN ABS( 100-200-300 ) };

-- Error
values{ fn abs('null') };

-- End of ABS test.  For escaped function.
-- This test EJBQL Absolute function. Resolve 3535
-- Begin of ABSVAL test.  For all valid types, un-escaped function.


-- Integer has a range of -2147483648 to 2147483647
-- Basic

create table myint( a int );
select abs(a) from myint;
insert into myint values (null);
select abs(a) from myint;
autocommit off;

-- Prepare Statements, should pass and return 1
prepare p1 as 'select abs(?) from myint';
prepare p1 as 'select 1 from myint where ? <= 4';
execute p1 using 'values absval( 4 )';
execute p1 using 'values absval( -4 )';
execute p1 using 'values absval( 4.4 )';
execute p1 using 'values absval( -4.4 )';

-- Prepare Statements, should pass and return 1
prepare p2 as 'select {fn abs(?)} from myint';
prepare p2 as 'select 1 from myint where ? <= 4';
execute p2 using 'values {fn abs( 4 )}';
execute p2 using 'values {fn abs( -4 )}';
execute p2 using 'values {fn abs( 4.4 )}';
execute p2 using 'values {fn abs( -4.4 )}';
execute p2 using 'values {fn abs( -4.44444444444444444444444 )}';
autocommit on;

drop table myint;
-- Using Strings in escape function
create table myStr( a varchar(10) );
insert into myStr values ( '123' );
insert into myStr values ( '-123' );
insert into myStr values ( '-12 ' );
insert into myStr values ( ' -2 ' );
insert into myStr values ( '1a3' );
select * from myStr;
select abs(a) from myStr;
select {fn abs(a)} from myStr;
drop table myStr;
-- End of ABSVAL test

-- This test EJBQL function, CONCAT. Resolve 3535

-- Begin of CONCAT test
-- Basic
values{ fn concat( 'hello', ' world' ) };
VALUES{ FN CONCAT( 'HELLO', ' WORLD' ) };
values{ fn concat( '' , '' )};
values{ fn concat( CHAR(''), CHAR('') ) };
values{ fn concat( 45, 67 )};
values{ fn concat( '45', 67 )};
values{ fn concat( 45, '67' )};
values{ fn concat( CHAR('C'), CHAR('#') ) };
values{ fn concat( 'ABCDEFGHIJKLMNOPQRSTUVWXYZ`1234567890-=\    [];,./ \'' |',
                   'abcdefghijklmnopqrstuvwxyz~!@#$%^&*()_+|<>?:"{}     ''''''      ' ) };
create table concat ( a int );
insert into concat values (1);
select * from CONCAT;

create table myconcat( a varchar(10) default null, b varchar(10) default null, c int);
insert into myconcat (c) values( 1 );
insert into myconcat (c) values( 2 );
insert into myconcat (a) values( 'hello' );
insert into myconcat (b) values( 'world' );
insert into myconcat (a,b) values( 'hello', 'world' );
select * from myconcat;
select { fn concat( a, b ) } from myconcat;
drop table concat;
drop table myconcat;
-- End of CONCAT test

-- This test the EJBQL function, LOCATE. Resolve 3535

-- LOCATE( string1, string2[, start] ) --- string1, searching from the beginning
--   of string2 }; if start is specified, the search begins from position start.
--   0 is returned if string2 does not contain string1.  Position1 is the first
--   character in string2.

-- Begin of LOCATE test
-- Basic
-- 2 args
values{ fn locate( 'hello', 'hello' ) };
values{ fn locate( 'hello', 'hellohello' ) };
values{ fn locate( 'hello', 'helloworld' ) };
values{ fn locate( 'hello', 'h?hello' ) };
values{ fn locate( 'hello', 'match me, hello now!' ) };
values{ fn locate( '?', '?' ) };
values{ fn locate( '\', '\\') };
values{ fn locate( '/', '//') };
values{ fn locate( '\\', '\') };
values{ fn locate( '//', '/') };
values{ fn locate( '', 'test' ) };
values{ fn locate( '', ''     ) };
values{ fn locate( 'test', '' ) };

-- 3 args 
values{ fn locate( 'hello', 'hello',-1 ) };
values{ fn locate( 'hello', 'hello',-0 ) };
values{ fn locate( 'hello', 'hello', 0 ) };
values{ fn locate( 'hello', 'hello', 1 ) };
values{ fn locate( 'hello', 'hello', 2 ) };
values{ fn locate( 'hello', 'hello', 5 ) };
values{ fn locate( 'hello', 'hello', 9 ) };

values{ fn locate( 'hello', 'hellohello', 0 ) };
values{ fn locate( 'hello', 'hellohello', 1 ) };
values{ fn locate( 'hello', 'hellohello', 2 ) };
values{ fn locate( 'hello', 'hellohello', 5 ) };
values{ fn locate( 'hello', 'hellohello', 6 ) };
values{ fn locate( 'hello', 'hellohello', 7 ) };

values{ fn locate( 'hello', 'h?hello', 1 ) };
values{ fn locate( 'hello', 'h?hello', 2 ) };
values{ fn locate( 'hello', 'h?hello', 3 ) };
values{ fn locate( 'hello', 'h?hello', 4 ) };

values{ fn locate( 'hello', 'match me, hello now!',  7 ) };
values{ fn locate( 'hello', 'match me, hello now!', 15 ) };

values{ fn locate( '?', '?',-1 ) };
values{ fn locate( '?', '?',-0 ) };
values{ fn locate( '?', '?', 0 ) };
values{ fn locate( '?', '?', 1 ) };
values{ fn locate( '?', '?', 2 ) };

values{ fn locate( '\', '\\',0) };
values{ fn locate( '\', '\\',1) };
values{ fn locate( '\', '\\',2) };
values{ fn locate( '\', '\\',3) };

values{ fn locate( '/', '//',0) };
values{ fn locate( '/', '//',1) };
values{ fn locate( '/', '//',2) };
values{ fn locate( '/', '//',3) };

values{ fn locate( '\\', '\',1) };
values{ fn locate( '//', '/',1) };

values{ fn locate( '', 'test',1) };
values{ fn locate( '', 'test',2) };
values{ fn locate( '', 'test',3) };
values{ fn locate( '', 'test',4) };
values{ fn locate( '', 'test',5) };
values{ fn locate( '', ''    ,1) };
values{ fn locate( 'test', '',1) };
values{ fn locate( 'test', '',2) };
values{ fn locate( 'test', '',3) };
values{ fn locate( 'test', '',4) };

values{ fn locate( 'hello', 1 ) };
values{ fn locate( 1, 'hello' ) };
values{ fn locate( 'hello', 'hello', 'hello' ) };
values{ fn locate( 'hello', 'hello', 1.99999999999 ) };
values{ fn locate( 1, 'hel1lo' ) };
values{ fn locate( 1, 1 ) };
values{ fn locate( 1, 1, '1' ) };
values{ fn locate( '1', 1, 1 ) };
values{ fn locate( '1', '1', '1' ) };


-- End of EJBQL function test for LOCATE.
-- This test the EJBQL function, LOCATE. Resolve 3535

-- LOCATE( string1, string2[, start] ) --- string1, searching from the beginning
--   of string2; if start is specified, the search begins from position start.
--   0 is returned if string2 does not contain string1.  Position1 is the first
--   character in string2.

-- Begin of LOCATE test
-- Basic
create table locate( a varchar(20) );

-- create table myChar( a char(10), b char(20), c int default '1'  );
create table myChar( a char(10), b char(20), c int );
insert into myChar (a, b) values( '1234567890', 'abcde1234567890fghij' );
insert into myChar (a, b) values( 'abcdefghij', 'abcdefghij1234567890' );
insert into myChar (a, b) values( 'abcdefghij', '1234567890abcdefghij' );
insert into myChar (a, b) values( 'abcdefghij', '1234567890!@#$%^&*()' );
insert into myChar values( '1234567890', 'abcde1234567890fghij', 2 );
insert into myChar values( 'abcdefghij', 'abcdefghij1234567890', 1  );
insert into myChar values( 'abcdefghij', '1234567890abcdefghij', 15 );
insert into myChar (c) values( 0 );
insert into myChar (c) values( 1 );
insert into myChar (c) values( 2 );
insert into myChar (a) values( 'hello' );
insert into myChar (b) values( 'hello' );
insert into myChar values( 'abcdefghij', '1234567890!@#$%^&*()', 21 );
select a, b, c from myChar;
select locate(a, b) from myChar;
select locate(a, b, c) from myChar;

drop table myChar;
create table myLongVarChar( a long varchar, b long varchar, c int);
insert into myLongVarChar (a, b) values( '1234567890', 'abcde1234567890fghij' );
insert into myLongVarChar (a, b) values( 'abcdefghij', 'abcdefghij1234567890' );
insert into myLongVarChar (a, b) values( 'abcdefghij', '1234567890abcdefghij' );
insert into myLongVarChar (a, b) values( 'abcdefghij', '1234567890!@#$%^&*()' );
insert into myLongVarChar (a, b) values( 'abcde', 'abcde' );
insert into myLongVarChar (a, b) values( 'abcde', 'abcd' );
insert into myLongVarChar (a, b) values( '', 'abcde' );
insert into myLongVarChar (a, b) values( 'abcde', null );
insert into myLongVarChar (a, b) values( null, 'abcde' );
insert into myLongVarChar values( '1234567890', 'abcde1234567890fghij', 2 );
insert into myLongVarChar values( 'abcdefghij', 'abcdefghij1234567890', 1 );
insert into myLongVarChar values( 'abcdefghij', '1234567890abcdefghij', 15 );
insert into myLongVarChar values( 'abcde', 'abcde', 1 );
insert into myLongVarChar values( 'abcde', 'abcd', 1 );
insert into myLongVarChar values( '', 'abcde', 2 );
insert into myLongVarChar values( 'abcde', null, 1  );
insert into myLongVarChar values( null, 'abcde', 1 );
insert into myLongVarChar (c) values( 0 );
insert into myLongVarChar (c) values( 1 );
insert into myLongVarChar (c) values( 2 );
insert into myLongVarChar (a) values( 'hello' );
insert into myLongVarChar (b) values( 'hello' );
insert into myLongVarChar values( 'abcdefghij', '1234567890!@#$%^&*()', 21 );
select a, b, c from myLongVarChar;
select locate(a, b) from myLongVarChar;
select locate(a, b, c) from myLongVarChar;
drop table myLongVarChar;

create table myVarChar( a varchar(10), b varchar(20), c int );
insert into myVarChar (a, b) values( '1234567890', 'abcde1234567890fghij' );
insert into myVarChar (a, b) values( 'abcdefghij', 'abcdefghij1234567890' );
insert into myVarChar (a, b) values( 'abcdefghij', '1234567890abcdefghij' );
insert into myVarChar (a, b) values( 'abcdefghij', '1234567890!@#$%^&*()' );
insert into myVarChar (a, b) values( 'abcde', 'abcde' );
insert into myVarChar (a, b) values( 'abcde', 'abcd' );
insert into myVarChar (a, b) values( '', 'abcde' );
insert into myVarChar (a, b) values( 'abcde', null );
insert into myVarChar (a, b) values( null, 'abcde' );
insert into myVarChar values( '1234567890', 'abcde1234567890fghij', 2 );
insert into myVarChar values( 'abcdefghij', 'abcdefghij1234567890', 1 );
insert into myVarChar values( 'abcdefghij', '1234567890abcdefghij', 15 );
insert into myVarChar values( 'abcde', 'abcde', 1 );
insert into myVarChar values( 'abcde', 'abcd', 1 );
insert into myVarChar values( '', 'abcde', 2 );
insert into myVarChar values( 'abcde', null, 1  );
insert into myVarChar values( null, 'abcde', 1 );
insert into myVarChar (c) values( 0 );
insert into myVarChar (c) values( 1 );
insert into myVarChar (c) values( 2 );
insert into myVarChar (a) values( 'hello' );
insert into myVarChar (b) values( 'hello' );
insert into myVarChar values( 'abcdefghij', '1234567890!@#$%^&*()', 21 );
select a, b, c from myVarChar;
select locate(a, b) from myVarChar;
select locate(a, b, c) from myVarChar;
drop table myVarChar;

-- Negative cases. To match DB2 behaviour
create table t1 (dt date, tm time, ts timestamp);
insert into t1 values (current_date, current_time, current_timestamp);
select locate (dt, ts) from t1;
select locate (tm, ts) from t1;
select locate (ts, ts) from t1;
drop table t1;

values locate('abc', 'dkabc', 1.4);
values locate('c', 'abcdedf', cast(1 as decimal(2,0)));

-- =========================================================================
-- These test cases for national character types will fail until 
-- until a future work around is implemented
-- =========================================================================
create table mynChar( a nchar(10), b nchar(20), c int );
insert into mynChar values( '1234567890', 'abcde1234567890fghij' );
insert into mynChar values( 'abcdefghij', 'abcdefghij1234567890' );
insert into mynChar values( 'abcdefghij', '1234567890abcdefghij' );
insert into mynChar values( 'abcdefghij', '1234567890!@#$%^&*()' );
insert into mynChar values( '1234567890', 'abcde1234567890fghij', 2 );
insert into mynChar values( 'abcdefghij', 'abcdefghij1234567890', 1  );
insert into mynChar values( 'abcdefghij', '1234567890abcdefghij', 15 );
insert into mynChar (c) values( 0 );
insert into mynChar (c) values( 1 );
insert into mynChar (c) values( 2 );
insert into mynChar (a) values( 'hello' );
insert into mynChar (b) values( 'hello' );
insert into mynChar values( 'abcdefghij', '1234567890!@#$%^&*()', 21 );
select a, b, c from mynChar;
select locate(a, b) from mynChar;
select locate(a, b, c) from mynChar;
drop table mynChar;

create table myLongnVarChar( a long nvarchar, b long nvarchar, c int );
insert into myLongnVarChar values( '1234567890', 'abcde1234567890fghij' );
insert into myLongnVarChar values( 'abcdefghij', 'abcdefghij1234567890' );
insert into myLongnVarChar values( 'abcdefghij', '1234567890abcdefghij' );
insert into myLongnVarChar values( 'abcdefghij', '1234567890!@#$%^&*()' );
insert into myLongnVarChar values( 'abcde', 'abcde' );
insert into myLongnVarChar values( 'abcde', 'abcd' );
insert into myLongnVarChar values( '', 'abcde' );
insert into myLongnVarChar values( 'abcde', null );
insert into myLongnVarChar values( null, 'abcde' );
insert into myLongnVarChar values( '1234567890', 'abcde1234567890fghij', 2 );
insert into myLongnVarChar values( 'abcdefghij', 'abcdefghij1234567890', 1 );
insert into myLongnVarChar values( 'abcdefghij', '1234567890abcdefghij', 15 );
insert into myLongnVarChar values( 'abcde', 'abcde', 1 );
insert into myLongnVarChar values( 'abcde', 'abcd', 1 );
insert into myLongnVarChar values( '', 'abcde', 2 );
insert into myLongnVarChar values( 'abcde', null, 1  );
insert into myLongnVarChar values( null, 'abcde', 1 );
insert into myLongnVarChar (c) values( 0 );
insert into myLongnVarChar (c) values( 1 );
insert into myLongnVarChar (c) values( 2 );
insert into myLongnVarChar (a) values( 'hello' );
insert into myLongnVarChar (b) values( 'hello' );
insert into myLongnVarChar values( 'abcdefghij', '1234567890!@#$%^&*()', 21 );
select a, b, c from myLongnVarChar;
select locate(a, b) from myLongnVarChar;
select locate(a, b, c) from myLongnVarChar;
drop table myLongnVarChar;

create table mynVarChar( a nvarchar(10), b nvarchar(20), c int );
insert into mynVarChar values( '1234567890', 'abcde1234567890fghij' );
insert into mynVarChar values( 'abcdefghij', 'abcdefghij1234567890' );
insert into mynVarChar values( 'abcdefghij', '1234567890abcdefghij' );
insert into mynVarChar values( 'abcdefghij', '1234567890!@#$%^&*()' );
insert into mynVarChar values( 'abcde', 'abcde' );
insert into mynVarChar values( 'abcde', 'abcd' );
insert into mynVarChar values( '', 'abcde' );
insert into mynVarChar values( 'abcde', null );
insert into mynVarChar values( null, 'abcde' );
insert into mynVarChar values( '1234567890', 'abcde1234567890fghij', 2 );
insert into mynVarChar values( 'abcdefghij', 'abcdefghij1234567890', 1 );
insert into mynVarChar values( 'abcdefghij', '1234567890abcdefghij', 15 );
insert into mynVarChar values( 'abcde', 'abcde', 1 );
insert into mynVarChar values( 'abcde', 'abcd', 1 );
insert into mynVarChar values( '', 'abcde', 2 );
insert into mynVarChar values( 'abcde', null, 1  );
insert into mynVarChar values( null, 'abcde', 1 );
insert into mynVarChar (c) values( 0 );
insert into mynVarChar (c) values( 1 );
insert into mynVarChar (c) values( 2 );
insert into mynVarChar (a) values( 'hello' );
insert into mynVarChar (b) values( 'hello' );
insert into mynVarChar values( 'abcdefghij', '1234567890!@#$%^&*()', 21 );
select a, b, c from mynVarChar;
select locate(a, b) from mynVarChar;
select locate(a, b, c) from mynVarChar;

create table myMixed( a char(10), b long nvarchar, c int );
insert into myMixed values( '1234567890', 'abcde1234567890fghij' );
insert into myMixed values( 'abcdefghij', 'abcdefghij1234567890' );
insert into myMixed values( 'abcdefghij', '1234567890abcdefghij' );
insert into myMixed values( 'abcdefghij', '1234567890!@#$%^&*()' );
insert into myMixed values( '1234567890', 'abcde1234567890fghij', 2 );
insert into myMixed values( 'abcdefghij', 'abcdefghij1234567890', 1 );
insert into myMixed values( 'abcdefghij', '1234567890abcdefghij', 15 );
insert into myMixed (c) values( 0 );
insert into myMixed (c) values( 1 );
insert into myMixed (c) values( 2 );
insert into myMixed (a) values( 'hello' );
insert into myMixed (b) values( 'hello' );
insert into myMixed values( 'abcdefghij', '1234567890!@#$%^&*()', 21 );
select a, b, c from myMixed;
select locate(a, b) from myMixed;
select locate(a, b, c) from myMixed;
drop table myMixed;

create table foo( a int );
insert into foo select locate(a, b) from mynVarChar;
insert into foo values( {fn locate('hello', 'hello')} );
select * from foo;
drop table foo;
drop table mynVarChar;
-- =========================================================================

-- Other types
create table myBigInt( a bigint, b bigint );
insert into myBigInt values( 1234, 1234 );
insert into myBigInt values( 4321, 1234 );
select locate(a, b) from myBigInt;
drop table myBigInt;

create table myBit( a char for bit data, b char for bit data );
insert into myBit values( X'40', X'40' );
insert into myBit values( X'01', X'40' );
select locate(a, b) from myBit;
drop table myBit;

-- bug 5794 - LOCATE built-in function is not db2 udb compatible
create table myDate( a date, b date );
insert into myDate values( date('1970-01-08'), date('1970-01-08') );
insert into myDate values( date('1979-08-30'), date('1978-07-28') );
select locate(a, b) from myDate;
drop table myDate;

create table myDecimal( a decimal, b decimal );
insert into myDecimal values( 2.2, 2.2 );
insert into myDecimal values( 12.23, 3423 );
select locate(a, b) from myDecimal;
drop table myDecimal;

create table myDouble( a double precision, b double precision );
insert into myDouble values( 2.2, 2.2 );
insert into myDouble values( 12.23, 3423 );
select locate(a, b) from myDouble;
drop table myDouble;

create table myInteger(a integer, b integer );
insert into myInteger values( 2, 2 );
insert into myInteger values( 123, 3423 );
select locate(a, b) from myInteger;
drop table myInteger;

create table mylongvarbinary( a long varchar for bit data, b long varchar for bit data );
select locate(a, b) from mylongvarbinary;
drop table mylongvarbinary;

-- bug 5794 - LOCATE built-in function is not db2 udb compatible
create table mytime( a time, b time );
insert into mytime values( time('10:00:00'), time('10:00:00') );
insert into mytime values( time('10:00:00'), time('11:00:00') );
select locate(a, b) from mytime;
drop table mytime;

-- bug 5794 - LOCATE built-in function is not db2 udb compatible
create table mytimestamp( a timestamp, b timestamp );
insert into mytimestamp values( timestamp('1997-01-01 03:03:03'), timestamp('1997-01-01 03:03:03' ));
insert into mytimestamp values( timestamp('1997-01-01 03:03:03'), timestamp('1997-01-01 04:03:03' ));
select locate(a, b) from mytimestamp;
drop table mytimestamp;

-- End of ejbql_locate2.sql test


-- This test the EJBQL function, LOCATE. Resolve 3535

-- LOCATE( string1, string2[, start] ) --- string1, searching from the beginning
--   of string2; if start is specified, the search begins from position start.
--   0 is returned if string2 does not contain string1.  Position1 is the first
--   character in string2.

-- Begin of LOCATE test
-- Basic

-- AUTHOR'S NOTE: This test highlights the difference between Oracle8i,
--    IBM DB2, and Cloudscape.

create table foo( a varchar(10), b varchar(20) );
insert into foo values( 'abc', 'abcd' );
insert into foo (a,b) values ( 'ABC', NULL );
insert into foo (a,b) values ( NULL, 'DEF' );
insert into foo (a,b) values ( 'ABC', '') ;
insert into foo (a,b) values ( '', 'DEF' );
insert into foo (a,b) values ( '', '' );
insert into foo (a,b) values ( NULL, NULL );
insert into foo (a,b) values ( 'GHJK', 'GHJ' );
insert into foo (a,b) values ( 'QWE', 'QWERT' );
insert into foo (a,b) values ( 'TYUI', 'RTYUI' );
insert into foo (a,b) values ( 'IOP', 'UIOP[' );
insert into foo (a,b) values ( 'ZXCV', 'ZXCV' );
select * from foo;
select locate(a, b) from foo;
select locate(a, b, 0) from foo;
select locate(a, b, -1) from foo;
select locate(a, b, 1) from foo;
select locate(a, b, 2) from foo;
select locate(a, b, 200) from foo;

drop table foo;
-- End of ejbql_locate3.sql test
-- This test the EJBQL function, LOCATE. Resolve 3535

-- LOCATE( string1, string2[, start] ) --- string1, searching from the beginning
--   of string2 }; if start is specified, the search begins from position start.
--   0 is returned if string2 does not contain string1.  Position1 is the first
--   character in string2.

-- Begin of LOCATE test
-- Basic

create table loc( c varchar(20) default null, a int default null, b int default null);

insert into loc (c) values ('This world is crazy' );
insert into loc (c) values ('nada' );
insert into loc (b) values ( 3 );
select * from loc;
select c, locate( 'crazy', c ) from loc;
autocommit off;

-- Prepare Statements
prepare p1 as 'select locate( ''crazy'', c ) from loc';
execute p1;
-- first arg ?
prepare p2 as 'select locate( ?, c ) from loc';
execute p2 using 'values ( ''crazy'' )';
execute p2 using 'values ( ''hahah'' )';
-- second arg ?
prepare p3 as 'select locate( ''nada'', ? ) from loc';
execute p3 using 'values ( ''nada'' )';
execute p3 using 'values ( ''haha'' )';
-- both first and second arguments ? ?
prepare p4 as 'select locate( ?, ? ) from loc';
execute p4 using 'values ( ''dont'', ''match'' )';
execute p4 using 'values ( ''match'', ''me match me'' )';
-- thrid arg ?
prepare p5 as 'select locate( c, c, ? ) from loc';
execute p5 using 'values ( 1 )';
execute p5 using 'values ( 2 )';
-- all args ? ? ?
prepare p6 as 'select locate( ?, ?, ? ) from loc';
execute p6 using 'values ( ''hello'', ''no match'', 1 )';
execute p6 using 'values ( ''match'', ''me match me'', 2 )';

-- Prepare Statements
prepare p7 as 'select {fn locate( ''crazy'', c )} from loc';
execute p7;
-- first arg ?
prepare p7 as 'select {fn locate( ?, c )} from loc';
execute p7 using 'values ( ''crazy'' )';
execute p7 using 'values ( ''hahah'' )';
-- second arg ?
prepare p8 as 'select {fn locate( ''nada'', ? )} from loc';
execute p8 using 'values ( ''nada'' )';
execute p8 using 'values ( ''haha'' )';
-- both first and second arguments ? ?
prepare p9 as 'select {fn locate( ?, ? )} from loc';
execute p9 using 'values ( ''dont'', ''match'' )';
execute p9 using 'values ( ''match'', ''me match me'' )';
-- thrid arg ?
prepare p10 as 'select {fn locate( c, c, ? )} from loc';
execute p10 using 'values ( 1 )';
execute p10 using 'values ( 2 )';
-- all args ? ? ?
prepare p11 as 'select {fn locate( ?, ?, ? )} from loc';
execute p11 using 'values ( ''hello'', ''no match'', 1 )';
execute p11 using 'values ( ''match'', ''me match me'', 2 )';
autocommit on;

drop table loc;

-- End of LOCATE test
-- This test EJBQL Sqrt function. Resolve 3535
-- Begin of SQRT test.  For all valid types, un-escaped function.


-- Real has a range of +/-1.4E-45 to +/-3.4028235E+38
-- Basic
create table myreal( a real );
select sqrt(a) from myreal;
insert into myreal values (null), (+0), (-0), (+1), (null), (100000000),
(3.402E+38), (1.175E-37);
select a from myreal;
select sqrt(a) from myreal;
select -sqrt(a) from myreal;
select sqrt(sqrt(-sqrt(-sqrt(a)))) from myreal;
SELECT SQRT(SQRT(-SQRT(-SQRT(A)))) FROM MYREAL;
select sqrt(sqrt(sqrt(sqrt(a)))) from myreal;
select distinct sqrt(a) from myreal;
drop table myreal;
-- End of Real test


-- Double Precision has a range of +/-4.9E-324 to +/-1.7976931348623157E+308
-- Basic
create table mydoubleprecision( a double precision );
select sqrt(a) from mydoubleprecision;
insert into mydoubleprecision values (null), (+0), (-0), (+1), (100000000), (null),
(1.79769E+308), (2.225E-307);
select a from mydoubleprecision;
select sqrt(a) from mydoubleprecision;
select -sqrt(a) from mydoubleprecision;
select sqrt(sqrt(-sqrt(-sqrt(a)))) from mydoubleprecision;
SELECT SQRT(SQRT(-SQRT(-SQRT(A)))) FROM MYDOUBLEPRECISION;
select sqrt(sqrt(sqrt(sqrt(a)))) from mydoubleprecision;
select distinct sqrt(a) from mydoubleprecision;
drop table mydoubleprecision;
-- End of Double Precision test


-- Float has a the range or a java.lang.Float or java.lang.Double depending on
-- the precision you specify.  Below a is a double, b is a float
create table myfloat( a float, b float(23) );
select sqrt(a), sqrt(b) from myfloat;
select columnname, columndatatype
from sys.syscolumns c, sys.systables t where c.referenceid = t.tableid and t.tablename='MYFLOAT';
insert into myfloat values (null, null), (+0, +0), (-0, -0), (+1, +1), (100000000, 100000000), (null, null),
(1.79769E+308, 3.402E+38),
(2.225E-307, 1.175E-37);
select a, b from myfloat;
select sqrt(a), sqrt(b) from myfloat;
select -sqrt(a), -sqrt(b) from myfloat;
select sqrt(sqrt(-sqrt(-sqrt(a)))), sqrt(sqrt(-sqrt(-sqrt(b)))) from myfloat;
SELECT SQRT(SQRT(-SQRT(-SQRT(A)))), SQRT(SQRT(-SQRT(-SQRT(B)))) FROM MYFLOAT;
select sqrt(sqrt(sqrt(sqrt(a)))), sqrt(sqrt(sqrt(sqrt(b)))) from myfloat;
select distinct sqrt(a) from myfloat;
select distinct sqrt(b) from myfloat;
drop table myfloat;
-- End of Float test


-- Test some different statements, just in case
-- beetle 5804 - support FLOAT built-in function
create table foo( a float );
insert into foo values ( sqrt(FLOAT( 1)));
insert into foo values ( sqrt(FLOAT( 2)));
insert into foo values (-sqrt(FLOAT( 3)));
insert into foo values (-sqrt(FLOAT( 4)));
insert into foo values (     (FLOAT(-5)));
-- this insert should fail
insert into foo values ( sqrt(FLOAT(-3)));

autocommit off;
prepare p1 as 'select a from foo';
prepare p2 as 'insert into foo select a*(-1) from foo';
execute p1;
execute p2;
execute p1;
insert into foo values ( sqrt(FLOAT( 6)));
insert into foo values (-sqrt(FLOAT( 7)));
insert into foo values (-sqrt(FLOAT( 8)));
insert into foo values ( sqrt(FLOAT( 9)));
insert into foo values (     (FLOAT(10)));
-- few negative tests
-- insert should fail
insert into foo values ( sqrt(FLOAT(-7)));
prepare p3 as 'select sqrt(a) from foo';
-- executing p3 should fail
execute p3;

-- these should pass
execute p1;
execute p2;
execute p1;
rollback;
commit;

autocommit on;
insert into foo values ( sqrt(FLOAT( 11)));
insert into foo values (-sqrt(FLOAT( 12)));
insert into foo values (-sqrt(FLOAT( 13)));
insert into foo values (-sqrt(FLOAT( 14)));
insert into foo values (     (FLOAT( 15)));
-- these 2 inserts should fail
insert into foo values (-sqrt(FLOAT(-12)));
insert into foo values ( sqrt(FLOAT(-13)));

-- these should pass
autocommit off;
execute p1;
execute p3;
execute p1;
-- executing p2 should fail
execute p2;

autocommit on;
select * from foo;
drop table foo;
-- End of SQRT test.  For all valid types.  Un-escaped function.
-- This test EJBQL Sqrt function. Resolve 3535
-- Begin of SQRT test.


-- Integer, Smallint, Bigint, Decimal
create table myint( a int );
create table myinteger( a Integer );
select sqrt(a) from myint;
select sqrt(a) from myinteger;
drop table myint;
drop table myinteger;

create table mysmallint( a smallint );
select sqrt(a) from mysmallint;
drop table mysmallint;

create table mybigint( a bigint );
select sqrt(a) from mybigint;
drop table mybigint;

create table mydecimal( a decimal );
select sqrt(a) from mydecimal;
drop table mydecimal;


-- For escape function.


-- Integer
-- Basic
values{ fn sqrt(INT('0'))};

-- Smallint
-- Basic
-- beetle 5805 - support INT[EGER] built-in function
values{ fn sqrt(SMALLINT('0'))};

-- Bigint
-- Basic
-- beetle 5809 - support BIGINT built-in function
values{ fn sqrt(BIGINT('0'))};

-- Real
-- Basic
-- beetle 5806 - support REAL built-in function
values{fn sqrt( REAL( 0)                       )};
values{fn sqrt( REAL(-0)                       )};
values{fn sqrt( REAL( 1)                       )};
values{fn sqrt( REAL(-1)                       )};
values{fn sqrt( REAL( 1000000.001)             )};
values{fn sqrt( REAL(-1000000.001)             )};
values{fn sqrt( REAL( 3.402E+38)               )};
values{fn sqrt( REAL(-3.402E+38) + 1           )};
-- Error
values{fn sqrt( REAL( 3.402E+38 * 2)           )};
values{fn sqrt(-REAL( NaN)                     )};
values{fn sqrt( REAL( 1.40129846432481707e-45) )};
values{fn sqrt( REAL( 3.40282346638528860e+38) )};

-- Double Precision/Double
-- Basic
-- beetle 5803 - support DOUBLE_[PRECISION] built-in function
values{fn  sqrt( DOUBLE( 0)                      )};
values{fn  sqrt( DOUBLE(-0)                      )};
values{fn  sqrt( DOUBLE( 1)                      )};
values{fn -sqrt( DOUBLE(1)                       )};
values{fn  sqrt( DOUBLE( 1000000.001)            )};
values{fn -sqrt( DOUBLE(1000000.001)             )};
values{fn -sqrt( DOUBLE(1.79769E+308)            )};
values{fn  sqrt( DOUBLE( 1.79769E+308) + 1       )};
values{fn  sqrt( DOUBLE( 2.225E-307 + 1)         )};
-- Error
values{fn  sqrt( DOUBLE(-1)                      )};
values{fn  sqrt( DOUBLE(-1000000.001)            )};
values{fn  sqrt( DOUBLE(-1.79769E+308)           )};
values{fn  sqrt( DOUBLE( 1.79769E+308 * 2)       )};
values{fn  sqrt(-DOUBLE( NaN)                    )};
values{fn  sqrt( DOUBLE( 4.9E-324)               )};
values{fn  sqrt( DOUBLE( 1.7976931348623157E308) )};

-- Decimal/Numeric
-- Basic
-- beetle 5802 - support DEC[IMAL] built-in function
values{ fn sqrt(DEC('0'))};

-- More generic test
values{ fn sqrt( 0+1+.1 ) };
values{ fn sqrt( +0+1.000000001 ) };
VALUES{ FN sqrt( 100+200+300 ) };
values{ fn sqrt( 0-1-.1 ) };
values{ fn sqrt( -0-1.000000001 ) };
VALUES{ FN sqrt( 100-200-300 ) };

-- Error
values{ fn sqrt('null') };

-- sqrt as a keyword
create table sqrt( a int );

-- End of SQRT test.
-- This test EJBQL Sqrt function. Resolve 3535
-- Begin of SQRT test.  For all valid types, un-escaped function.

create table myreal( a real );
select sqrt(a) from myreal;
insert into myreal values ( 3.402E+38 );
select a from myreal;

-- Prepare Statements, should pass
-- beetle 5806 - support REAL built-in function
autocommit off;
prepare p1 as 'select a from myreal where ? <> 1';
execute p1 using 'values  sqrt(REAL( 0 ))';
execute p1 using 'values -sqrt(REAL( 20))';
execute p1 using 'values  sqrt(REAL( 20))';
-- this should fail
execute p1 using 'values  sqrt(REAL(-20))';

-- Prepare Statements, should pass
-- beetle 5806 - support REAL built-in function
prepare p2 as 'select a from myreal where ? <> 1';
execute p2 using 'values {fn sqrt (REAL( 0 ))}';
execute p2 using 'values {fn -sqrt(REAL( 20))}';
execute p2 using 'values {fn sqrt (REAL( 20))}';
-- this should fail
execute p2 using 'values {fn  sqrt(REAL(-20))}';
autocommit on;

-- mod function
create table modfn(s smallint, i int, b bigint, c char(10), d decimal(6,3), r real, dbl double);
insert into modfn values(0, 0, 0, '0', 0.0, 0.0, 0.0);
insert into modfn values(5, 5, 5, '5', 5.0, 5.0, 5.0);
insert into modfn values(null, null, null, null, null, null, null);
select { fn mod(s, 3) } from modfn;
select { fn mod(i, 3) } from modfn;
select { fn mod(b, 3) } from modfn;
select { fn mod(c, 3) } from modfn;
select { fn mod(d, 3) } from modfn;
select { fn mod(r, 3) } from modfn;
select { fn mod(dbl, 3) } from modfn;
select { fn mod(67, t) } from modfn where s <> 0;
select { fn mod(67, s) } from modfn where s <> 0;
select { fn mod(67, i) } from modfn where s <> 0;
select { fn mod(67, b) } from modfn where s <> 0;
select { fn mod(67, c) } from modfn where s <> 0;
select { fn mod(67, d) } from modfn where s <> 0;
select { fn mod(67, r) } from modfn where s <> 0;
select { fn mod(67, dbl) } from modfn where s <> 0;
select { fn mod(s, s) } from modfn where s = 0;
select { fn mod(i, i) } from modfn where s = 0;
select { fn mod(i, b) } from modfn where s = 0;
select { fn mod(s, s) } from modfn where s is null;
select { fn mod(i, i) } from modfn where s is null;
select { fn mod(i, b) } from modfn where s is null;

select { fn mod(67, i) } from modfn where i <> 0;
select { fn mod(67, b) } from modfn where b <> 0;
-- this query should fail because of incompatible arguments
select { fn mod('rrrr', b) } from modfn  where b <> 0;

values { fn mod(23, 9)};
values mod(23, 9);

create table mod(mod int);
insert into mod values(1);
select mod from mod;
select mod(mod,mod) from mod;
drop table mod;

drop table modfn;

-- Using Strings in escape function
create table myStr( a varchar(10) );
insert into myStr values ( '123' );
insert into myStr values ( ' 123' );
insert into myStr values ( ' 12 ' );
insert into myStr values ( '  2 ' );
insert into myStr values ( '1a3' );
select * from myStr;
select sqrt(a) from myStr;
select {fn sqrt(a)} from myStr;
select {fn sqrt( '-12' ) } from myStr;
select {fn sqrt( '-1a2' ) } from myStr;

drop table myreal;
drop table myStr;
-- End of SQRT test

-- CHAR AND VARCHAR 
--
-- create some tables
create table t1 (c15a char(15), c15b char(15), vc15a varchar(15),
	vc15b varchar(15), lvc long varchar);
create table t2 (c20 char(20), c30 char(30), c40 char(40),
				 vc20 varchar(20), vc30 varchar(30), vc40 varchar(40),
				lvc long varchar);

-- populate the tables
insert into t1 (c15a) values(null);
insert into t1 values('1', '2', '3', '4', '5');
insert into t1 values('111111', '222222222222222', 
					  '333333', '444444444444444',
					'555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555');
insert into t1 values('555555    ', '66          ', 
					  '777777    ', '88          ',
					'99999999999999999999999999999999999999999999999999999999999999999999999999999999999                                                                     ');

-- negative tests

-- mixing char and bit (illegal)
values X'11' || 'asdf';
values 'adsf' || X'11';

-- ? parameter on both sides
values ? || ?;

-- simple positive
values 'aaa' || 'bbb';
values X'aaaa' || X'bbbb';

-- non-blank truncation error on char result
insert into t2 (c20) select c15a || c15b from t1 where c15a = '111111';
insert into t2 (vc20) select vc15a || vc15b from t1 where c15a= '111111';
insert into t2 (c20) select lvc || lvc from t1 where c15a = '111111';

maximumdisplaywidth 512;

-- positive tests
-- blank truncation on varchar
insert into t2 (c20) select vc15a || vc15b from t1 where c15a = '555555    ';
select c20 from t2;
delete from t2;

-- no blank truncation on char
insert into t2 (c30) select c15a || c15b from t1 where c15a = '555555    ';
select c30 from t2;
delete from t2;

-- long varchar
insert into t2 (c30) select lvc || lvc from t1 where c15a = '1';
select c30 from t2;
delete from t2;

-- vc || c -> vc
insert into t2 (c30) select vc15a || c15a from t1 where c15a = '555555    ';
select c30 from t2;
delete from t2;

-- c || vc -> vc
insert into t2 (c30) select c15a || vc15a || '9' from t1 where c15a = '555555    ';
select c30 from t2;
delete from t2;

-- vc || c -> lvc
insert into t2 (lvc) select c15a || vc15a from t1 where c15a = '555555    ';
select lvc from t2;
select length(lvc) from t2;
delete from t2;

-- lvc || lvc - > lvc
insert into t2 (lvc) select lvc || lvc from t1;
select lvc from t2;
delete from t2;

-- Parameters can be used in DB2 UDB if one operand is either CHAR(n) or VARCHAR(n), 
-- where n is less than 128, then other is VARCHAR(254 - n). 
-- In all other cases the data type is VARCHAR(254).
autocommit off;
-- ? || c
prepare pc as 'select ? || c15a from t1';

execute pc using 'values (''left'')';

-- c || ?
prepare cp as 'select c15a || ? from t1';

execute cp using 'values (''right'')';

-- ? || v
prepare pv as 'select ? || vc15a from t1';

execute pv using 'values (''left'')';

-- v || ?
prepare vp as 'select vc15a || ? from t1';

execute vp using 'values (''right'')';

-- Parameters cannot be used in DB2 UDB 
-- if one operand is a long varchar [for bit data] data type. 
-- An invalid parameter marker error is thrown in DB2 UDB (SQLSTATE 42610).

-- lvc || ?
prepare lvp as 'select lvc || ? from t1';

execute lvp using 'values (''right'')';

-- ? || lvc
prepare plv as 'select ? || lvc from t1';

execute plv using 'values (''left'')';
autocommit on;

-- multiple concatenations
insert into t2 (c30, vc30) values ('111  ' || '222  ' || '333   ',
								   '444  ' || '555  ' || '666   ');
select c30, vc30 from t2;
delete from t2;

-- concatenation on a long varchar
create table t3 (c1 long varchar, c2 long varchar);
insert into t3 values ('c1   ', 'c2');
insert into t2 (c30, vc30) select t3.c1 || t3.c2, t3.c2 || t3.c1 from t3;
select c30, vc30 from t2;
delete from t2;

-- drop the tables
drop table t1;
drop table t2;
drop table t3;

------------------------------------------------------------------------------------
-- CHAR (n) FOR BIT DATA AND VARCHAR (n) FOR BIT DATA

-- try some cases zero length cases
values X''|| X'80';
values X'01'|| X'';

-- create some tables


create table t1 (b16a char(2) for bit data, b16b char(2) for bit data, vb16a varchar(2) for bit data, vb16b varchar(2) for bit data, lbv long varchar for bit data);
create table t2 (b20 char(3) for bit data, b60 char(8) for bit data, b80 char(10) for bit data,
				 vb20 varchar(3) for bit data, vb60 varchar(8) for bit data, vb80 varchar(10) for bit data, lbv long varchar for bit data);

-- populate the tables
insert into t1 (b16a) values(null);
insert into t1 values(X'11', X'22', X'33', X'44', X'55');
insert into t1 values(X'1111', X'2222', 
					  X'3333', X'4444',
					  X'5555');
insert into t1 values(X'5555', X'66', 
					  X'7777', X'88',
					  X'9999');

-- negative tests

-- non-blank truncation error on bit result
insert into t2 (b20) select b16a || b16b from t1 where b16a = X'1111';
insert into t2 (vb20) select vb16a || vb16b from t1 where b16a= X'1111';

-- positive tests
-- truncation on bit varying
insert into t2 (b20) select vb16a || vb16b from t1 where b16a = X'5555';
select b20 from t2;
delete from t2;

-- bc || b -> vb
insert into t2 (b80) select vb16a || b16a from t1 where b16a = X'5555';
select b80 from t2;
delete from t2;

-- b || vb -> vb
insert into t2 (b80) select b16a || vb16a || X'99' from t1 where b16a = X'5555';
select b80 from t2;
delete from t2;

-- b || lbv -> lbv
insert into t2 (lbv) select b16a || lbv from t1 where b16a = X'5555';
select lbv from t2;
delete from t2;

-- lbv || b -> lbv
insert into t2 (lbv) select lbv || b16a from t1 where b16a = X'5555';
select lbv from t2;
delete from t2;

-- vb || lbv -> lbv
insert into t2 (lbv) select vb16a || lbv from t1 where b16a = X'5555';
select lbv from t2;
delete from t2;

-- lbv || vb -> lbv
insert into t2 (lbv) select lbv || vb16a from t1 where b16a = X'5555';
select lbv from t2;
delete from t2;

-- Parameters can be used in DB2 UDB 
-- if one operand is either CHAR(n) [for bit data] or VARCHAR(n) [for bit data], 
-- where n is less than 128, then other is VARCHAR(254 - n). 
-- In all other cases the data type is VARCHAR(254).
autocommit off;

-- ? || b
prepare pb as 'select ? || b16a from t1';

execute pb using 'values (X''ABCD'')';

-- b || ?
prepare bp as 'select b16a || ? from t1';

execute bp using 'values (X''ABCD'')';

-- ? || vb
prepare pvb as 'select ? || vb16a from t1';

execute pvb using 'values (X''ABCD'')';

-- vb || ?
prepare vbp as 'select vb16a || ? from t1';

execute vbp using 'values (X''ABCD'')';

-- Parameters cannot be used in DB2 UDB 
-- if one operand is a long varchar [for bit data] data type. 
-- An invalid parameter marker error is thrown in DB2 UDB (SQLSTATE 42610).

-- ? || lbv
prepare plbv as 'select ? || lbv from t1';

execute plbv using 'values (X''ABCD'')';

-- lbv || ?
prepare lbvp as 'select lbv || ? from t1';

execute lbvp using 'values (X''ABCD'')';
autocommit on;

-- multiple concatenations
insert into t2 (b80, vb80, lbv) values (X'e0' || X'A0' || X'20',
								   X'10' || X'11' || X'e0',
								   X'1234' || X'A0' || X'20');
select b80, vb80, lbv from t2;
delete from t2;

-- concatenation on a byte
create table t3 (b1 char(1) for bit data, b2 char(1) for bit data);
insert into t3 values (X'11', X'22');
insert into t2 (b80, vb80) select t3.b1 || t3.b2, t3.b2 || t3.b1 from t3;
select b80, vb80 from t2;
delete from t2;

-- clean up the prepared statements
remove pc;
remove cp;
remove vp;
remove pv;
remove pb;
remove bp;
remove pvb;
remove vbp;
remove lvp;
remove plv;
remove plbv;
remove lbvp;

-- drop the tables
drop table t1;
drop table t2;
drop table t3;
-- reset maximumdisplaywidth
maximumdisplaywidth 128;
--
--
-- the like tests are all run through the unit test
-- mechanism that is fired off with this test's
-- properties file.  that test tests all the %, _ combinations
-- to exhaustion.
--
-- we show that the language level support works, here, which is:
-- the syntax
-- char and varchar columns
-- not can be applied and pushed around with it
-- parameters (would need to be .java to show completely...)
-- not other types of columns
--

create table t (c char(20), v varchar(20), lvc long varchar);
insert into t values('hello','world', 'nice day, huh?');
insert into t values('goodbye','planet', 'see you later');
insert into t values('aloha','orb', 'hang loose');

-- subquery on left side
select * from t where (select max(c) from t) like '%';

select * from t where c like 'h%';
select * from t where v like '%or%';
select * from t where lvc like '%y%';

-- these four should all have the same results:
select * from t where not v like '%or%';
select * from t where not (v like '%or%');
select * from t where 1=0 or not v like '%or%';
select * from t where not (1=0 or not v not like '%or%');

-- these two should have the same results:
select * from t where c like '%lo%' or v like '%o%';
select * from t where v like '%o%' or c like '%lo%';

-- these three should have the same results:
select * from t where c like '%lo%' and 0=0;
select * from t where c like '%lo%' and 1=1;
select * from t where 1=1 and c like '%lo%';

-- we can at least show the parameters compile...
autocommit off;
prepare s as 'select * from t where v like ?';
execute s;
prepare s as 'select * from t where ? like ?';
execute s;
prepare s as 'select * from t where c like ?';
execute s;
prepare s as 'select * from t where lvc like ?';
execute s;
prepare s as 'select * from t where lvc like ?';
execute s;
autocommit on;

create table n (i int, r real, d date, u char(10));

-- these should succeed
insert into n values (1, 1.1, date('1111-11-11'), '%');
insert into n values (2, 2.2, date('2222-2-2'), 'haha');
select * from n where u like 'haha______';

-- now, with an index
create table m (i int, r real, d date, u varchar(10));
insert into m select * from n;
select * from m where u like 'haha';
select * from m where u like 'haha______';
create index i1 on m(u);
select * from m where u like 'haha';
select * from m where u like 'haha______';

-- tests for column like constant optimization
create table u (c char(10), vc varchar(10));
insert into u values ('hello', 'hello');
select * from u where c like 'hello';
select * from u where vc like 'hello';
select * from u where c like 'hello     ';
select * from u where vc like 'hello     ';

-- cleanup
drop table t;
drop table n;
drop table m;
drop table u;

-- testing JDBC escaped length function
-- JDBC length is defined as the number of characters in a string without trailing blanks.
values {FN LENGTH('hello     ') };
values {FN LENGTH(rtrim('hello     ')) };

-- defect 5749. rtrim() over substr() used to raise ASSERT failure.
create table t1 (c1 char(10));
insert into t1 values ('testing');
select rtrim(substr(' asdf', 1, 3)) from t1;
