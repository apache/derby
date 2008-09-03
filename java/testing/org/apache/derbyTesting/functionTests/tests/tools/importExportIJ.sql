--
--   Licensed to the Apache Software Foundation (ASF) under one or more
--   contributor license agreements.  See the NOTICE file distributed with
--   this work for additional information regarding copyright ownership.
--   The ASF licenses this file to You under the Apache License, Version 2.0
--   (the "License"); you may not use this file except in compliance with
--   the License.  You may obtain a copy of the License at
--
--      http://www.apache.org/licenses/LICENSE-2.0
--
--   Unless required by applicable law or agreed to in writing, software
--   distributed under the License is distributed on an "AS IS" BASIS,
--   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
--   See the License for the specific language governing permissions and
--   limitations under the License.
--
-- test for import export thru command line interface

-- first test basic import functionality
-- ascii delimited default format
drop table T1;
create table T1 (	Account	int,
			Name    char(30),
			Jobdesc char(40),
			Company varchar(35),
			Address1 varchar(40),
			Address2 varchar(40),
			City	varchar(20),
			State	char(5),
			Zip	char(10),
			Country char(10),
			Phone1  char(20),
			Phone2  char(20),
			email   char(30),
			web     char(30),
			Fname	char(30),
			Lname	char(30),
			Comment	char(30),
			AccDate	char(30),
			Payment	decimal(8,2),
			Balance decimal(8,2));

create index T1_IndexBalance on T1 (Balance, Account, Company);
create index T1_IndexFname on T1 (Fname, Account);
create index T1_IndexLname on T1 (Lname, Account);

-- second line of data file is not complete - should give error message re EOF
call SYSCS_UTIL.SYSCS_IMPORT_TABLE (null, 'T1' , 'extin/TwoLineBadEOF.dat' , 
                                    null, null, null, 0) ;

-- should work, default format (i.e. field sep , column delimiter ".)
-- (last two lines have extra white space (tabs, spaces) which should not matter).
call SYSCS_UTIL.SYSCS_IMPORT_TABLE (null, 'T1' , 'extin/AccountData_defaultformat.dat' , 
                                    null, null, null, 0) ;

values (SYSCS_UTIL.SYSCS_CHECK_TABLE('APP', 'T1'));

-- Delimited with a different char but using default import format
drop table T2;

autocommit off;
create table T2 (	Account	int,
			Name    char(30),
			Jobdesc char(40),
			Company varchar(35),
			Address1 varchar(40),
			Address2 varchar(40),
			City	varchar(20),
			State	char(5),
			Zip	char(10),
			Country char(10),
			Phone1  char(20),
			Phone2  char(20),
			email   char(30),
			web     char(30),
			Fname	char(30),
			Lname	char(30),
			Comment	char(30),
			AccDate	char(30),
			Payment	decimal(8,2),
			Balance decimal(8,2));

create index T2_IndexBalance on T2 (Balance, Account, Company);
create index T2_IndexFname on T2 (Fname, Account);
create index T2_IndexLname on T2 (Lname, Account);
commit;
--this one should fail as we're still using the default import format
call SYSCS_UTIL.SYSCS_IMPORT_TABLE (null, 'T2' , 'extin/AccountData_format1.dat' , 
                                    null, null, null, 0) ;

values (SYSCS_UTIL.SYSCS_CHECK_TABLE('APP', 'T2'));
-- so following should only commit an empty table
commit;

-- But if we use correct specification?
call SYSCS_UTIL.SYSCS_IMPORT_TABLE (null, 'T2' , 'extin/AccountData_format1.dat' , 
				   'q', '"', 'ASCII', 0) ;

values (SYSCS_UTIL.SYSCS_CHECK_TABLE('APP', 'T2'));
select count(*) from T2;
rollback;

-- test remapping
drop table T3;
create table T3 (	Lname	char(30),
			Fname	char(30),
			Account	int not null primary key,
			Company varchar(35),
			Payment	decimal(8,2),
			Balance decimal(8,2));

create index T3_indexBalance on T3 (Balance, Company, Account);
create index T3_indexPayment on T3 (Payment, Company, Account);

--incorrect mapping of file to table ; should give error and rollback
call SYSCS_UTIL.SYSCS_IMPORT_DATA(null, 'T3' , 
                                  null , '16, 15, 1, 4, 19, 200' ,
				  'extin/AccountData_defaultformat.dat' , 
				  null, null, null, 0) ;
select count(*) from T3;

drop table T4;
create table T4 (	Lname	char(30),
			Fname	char(30),
			Account	int not null primary key,
			Company varchar(35),
			Payment	decimal(8,2),
			Balance decimal(8,2));

create index T4_indexBalance on T4 (Balance, Company, Account);
create index T4_indexPayment on T4 (Payment, Company, Account);
-- correctly remapped
call SYSCS_UTIL.SYSCS_IMPORT_DATA(null, 'T4' ,  
                                  null , '16, 15, 1, 4, 19, 20' ,
				  'extin/AccountData_defaultformat.dat' ,
				   null, null, null, 0) ;
values (SYSCS_UTIL.SYSCS_CHECK_TABLE('APP', 'T4'));
commit;

-- now check results
select count(*) from T1;
select count(*) from T2;
select count(*) from T3;
select count(*) from T4;

select * from T1 where State = 'CA';
select * from T2 where State = 'CA';
select * from T4 where Fname = 'Georgiana' or Fname = 'Michael';

select Balance, Account, Company from T1 order by Balance;
select Balance, Account, Company from T2 order by Balance;
select Balance, Account, Company from T4 order by Balance;

--- now check other input formats
-- this is sample data with RecordSeperator=',', FieldStartDelimiter=(, FieldEndDelimiter=),FieldSeperator=TAB
-- which the import can't handle (not the RecordSeparator, and there can be only one
-- fieldDelimitor character). 
-- The error XIE0R and 22018 are returned if the 1 line file is largish  
autocommit on;
drop table Alt1;
create table Alt1 (	Account	int,
			Name    char(30),
			Jobdesc char(40),
			Company varchar(35),
			Address1 varchar(40),
			City	varchar(20),
			State	char(5),
			Zip	char(10),
			Payment	decimal(8,2),
			Balance decimal(8,2));

call SYSCS_UTIL.SYSCS_IMPORT_TABLE (null, 'ALT1' , 'extin/UnsupportedFormat1.dat' , 
                                      null, null, null, 0) ;

select count(*) from Alt1;

-- But error 38000 and 42X04 are returned if the 1 line file is smaller  
drop table Alt2;
create table Alt2 (	Account	int,
			Name    char(30),
			Jobdesc char(40),
			Company varchar(35),
			Address1 varchar(40),
			City	varchar(20),
			State	char(5),
			Zip	char(10),
			Payment	decimal(8,2),
			Balance decimal(8,2));

call SYSCS_UTIL.SYSCS_IMPORT_TABLE (null, 'ALT2' , 'extin/UnsupportedFormat2.dat' , 
                                      null, null, null, 0) ;

select count(*) from Alt2;

-- this is sample data with some null (missing) fields

drop table Alt3;
create table Alt3 (	Account	int,
			Name    char(30),
			Jobdesc char(40),
			Company varchar(35),
			Address1 varchar(40),
			Address2 varchar(40),
			City	varchar(20),
			State	char(5),
			Zip	char(10),
			Country char(10),
			Phone1  char(20),
			Phone2  char(20),
			email   char(30),
			web     char(30),
			Fname	char(30),
			Lname	char(30),
			Comment	char(30),
			AccDate	char(30),
			Payment	decimal(8,2),
			Balance decimal(8,2));


call SYSCS_UTIL.SYSCS_IMPORT_TABLE (null, 'ALT3' , 'extin/AccountData_NullFields.dat' , 
                                      null, null, null, 0) ;

select count(*) from Alt3;

--test more remapping - size matters not
create table Alt4(column1 varchar(1000) , column3 varchar(1000) , column9 varchar(1000));

call SYSCS_UTIL.SYSCS_IMPORT_DATA (null, 'ALT4' , 
   				   null , '2,3,9',
				   'extin/AccountData_defaultformat.dat' , 
				   null, null, null, 0) ;
select * from Alt4;

--- Format with | as column separator and '' as delimiter. Also remapping
drop table Alt5;
create table Alt5 (
	Id	int,
	Name	varchar(40),
	Title	varchar(40),
	Company	varchar(50),
	Address	varchar(80),
	City	varchar(30),
	State	varchar(30),
	Zip	varchar(30),
	Country varchar(30),
	phone1	varchar(50),
	phone2	varchar(30),
	email	varchar(80),
	web	varchar(50));

call SYSCS_UTIL.SYSCS_IMPORT_DATA(null, 'ALT5' ,
                                  null , '1,2,3,4,5,7,8,9,10,11,12,13,14', 
	 		          'extin/AccountData_format2.dat' , 
				  '|', '''', 'ASCII', 0) ;

	
select Company, Country from Alt5 where country not like 'U%S%A%' and country is not null;

rollback;

-- test remapping out of sequence
drop table Alt6;
create table Alt6 (
	Fname varchar(30),
	Lname varchar(30),
	email varchar(40),
	phone varchar(30));

call SYSCS_UTIL.SYSCS_IMPORT_DATA(null, 'ALT6' , 
		                  null , '15, 16, 13, 11',
			          'extin/AccountData_format2.dat' , 
				  '|', '''', 'ASCII', 0) ;
select count(*) from Alt6;

rollback;

-- import fails if data has delimiter character within field. 
drop table Alt7;
create table Alt7 (	Account	int,
			Name    char(30),
			Jobdesc char(40),
			Company varchar(35),
			Address1 varchar(40),
			Address2 varchar(40),
			City	varchar(20),
			State	char(5),
			Zip	char(10),
			Country char(10),
			Phone1  char(20),
			Phone2  char(20),
			email   char(30),
			web     char(30),
			Fname	char(30),
			Lname	char(30),
			Comment	char(30),
			AccDate	char(30),
			Payment	decimal(8,2),
			Balance decimal(8,2));

call SYSCS_UTIL.SYSCS_IMPORT_TABLE (null, 'ALT7' ,
			          'extin/AccountData_format2oops.dat' , 
				  '|', '''', 'ASCII', 0) ;

select count(*) from Alt8;
rollback;

-- if there's no end of record (CR/LF) after 1st record, rest gets ignored
drop table Alt8;
create table Alt8 (	Account	int,
			Name    char(30),
			Jobdesc char(40),
			Company varchar(35),
			Address1 varchar(40),
			Address2 varchar(40),
			City	varchar(20),
			State	char(5),
			Zip	char(10),
			Country char(10),
			Phone1  char(20),
			Phone2  char(20),
			email   char(30),
			web     char(30),
			Fname	char(30),
			Lname	char(30),
			Comment	char(30),
			AccDate	char(30),
			Payment	decimal(8,2),
			Balance decimal(8,2));

call SYSCS_UTIL.SYSCS_IMPORT_TABLE (null, 'ALT8' , 'extin/NoEOR.dat' , 
                                      null, null, null, 0) ;

select count(*) from Alt8;
rollback;

autocommit off;

-- import fails if the table has more columns than named in import statement
drop table HouseHoldItem;
create table HouseHoldItem(
	Category	int,
	RoomId		int,
	Description	varchar(255),
	Model		varchar(50),
	ModelId		varchar(50),
	SerialNumber	varchar(50),
	DayPurchase	date,
	PurchasePrice	decimal(8,2),
	Insured		decimal(8,2),
	Note		varchar(512));
call SYSCS_UTIL.SYSCS_IMPORT_DATA(null, 'HOUSEHOLDITEM' , 
				  null , '2,3,4,5,6,7,8,11,12', 
				  'extin/Access1.txt' , 
				  null, null, null, 0) ;
select * from HouseHoldItem;
rollback;

-- import fails if datatype & format of data are not compatible
create table HouseHoldItem(
	Category	int,
	RoomId		int,
	Description	varchar(255),
	Model		varchar(50),
	ModelId		varchar(50),
	SerialNumber	varchar(50),
	DayPurchase	date,
	PurchasePrice	decimal(8,2),
	Insured		smallint,
	Note		varchar(512));
call SYSCS_UTIL.SYSCS_IMPORT_DATA(null, 'HOUSEHOLDITEM' , 
				  null , '2,3,4,5,6,7,8,11,12,10', 
				  'extin/Access1.txt' , 
				  null, null, null, 0) ;
select * from HouseHoldItem;
rollback;

-- import will succeed with default format settings whether int, date, 
-- or time fields have quotes in the import file or not
create table HouseHoldItem(
	Category	int,
	RoomId		int,
	Description	varchar(255),
	Model		varchar(50),
	ModelId		varchar(50),
	SerialNumber	varchar(50),
	DayPurchase	date,
	PurchasePrice	decimal(8,2),
	Insured		decimal(8,2),
	Note		varchar(512));
call SYSCS_UTIL.SYSCS_IMPORT_DATA(null, 'HOUSEHOLDITEM' , 
				  null , '2,3,4,5,6,7,8,11,12,10', 
				  'extin/Access1.txt' , 
				  null, null, null, 0) ;
select * from HouseHoldItem;
rollback;


----
---- test export 
----

call SYSCS_UTIL.SYSCS_EXPORT_TABLE (null, 'T1' , 'extinout/t1.dump' , 
                                    '|','''', 'ASCII') ;

create table  imp_temp(column2 varchar(200), 
                  column3 varchar(200), 
                  column4 varchar(200), 
                  column5 varchar(200),
                  column6 varchar(200));

call SYSCS_UTIL.SYSCS_IMPORT_DATA(null, 'IMP_TEMP' ,null, '2, 3, 4, 5, 6',
                                    'extinout/t1.dump', '|', '''', 'ASCII', 0) ;

select * from imp_temp ;
drop table imp_temp;

-- test case for derby-1854/derby-1641
-- perform import into a table that has same column 
-- as a primary key and a foreign key (ADMINS table).  

create table users (
 user_id int not null generated by default as identity,
 user_login varchar(255) not null,
 primary key (user_id));

create table admins (
 user_id int not null,
 primary key (user_id),
 constraint admin_uid_fk foreign key (user_id) references users (user_id));
 
insert into users (user_login) values('test1');
insert into users (user_login) values('test2');

call SYSCS_UTIL.SYSCS_EXPORT_QUERY('select user_id from users' , 
                    'extinout/users_id.dat', null , null , null ) ;
call syscs_util.syscs_import_table( null, 'ADMINS', 
                    'extinout/users_id.dat', null, null, null,1); 
select * from admins; 
select * from users;
-- do consistency check on the table.
values SYSCS_UTIL.SYSCS_CHECK_TABLE('APP', 'ADMINS');
drop table admins;
drop table users;
-- end derby-1854/derby-1641 test case. 

--
-- begin test case for derby-2193:
--
-- Field comprised of all blank space should become a null
--

create table derby_2193_tab
(
    a  varchar( 50 ),
    b  varchar( 50 )
);

CALL SYSCS_UTIL.SYSCS_IMPORT_TABLE 
( null, 'DERBY_2193_TAB', 'extin/derby-2193.txt', null, null, null, 0 );
select * from derby_2193_tab;
select b, length(b) from derby_2193_tab;

--
-- Errors should contain identifying line numbers
--
create table derby_2193_lineNumber
(
    a  int,
    b  int
);

CALL SYSCS_UTIL.SYSCS_IMPORT_TABLE 
( null, 'DERBY_2193_LINENUMBER', 'extin/derby-2193-linenumber.txt', null, null, null, 0 );
select * from derby_2193_lineNumber;

--
-- end test case for derby-2193:
--

--
-- begin test case for derby-2925:
--
-- Prevent export from overwriting existing files 
--

create table derby_2925_tab
(
    a  varchar( 50 ),
    b  varchar( 50 )
);

--
-- Testing SYSCS_UTIL.SYSCS_EXPORT_TABLE
--

CALL SYSCS_UTIL.SYSCS_EXPORT_TABLE
( null, 'DERBY_2925_TAB', 'extout/derby-2925.txt', null, null, null);
--
-- Errors should should happen in the second
-- call to SYSCS_UTIL.SYSCS_EXPORT_TABLE
-- since extout/derby-2925.txt already exists. 
--
CALL SYSCS_UTIL.SYSCS_EXPORT_TABLE
( null, 'DERBY_2925_TAB', 'extout/derby-2925.txt', null, null, null);

--
-- Testing SYSCS_UTIL.SYSCS_EXPORT_QUERY
--

CALL SYSCS_UTIL.SYSCS_EXPORT_QUERY
('select * from DERBY_2925_TAB', 'extout/derby-2925-query.dat', null , null , null ) ;
--
-- Errors should should happen in the second
-- call to SYSCS_UTIL.SYSCS_EXPORT_QUERY
-- since extout/derby-2925-query.dat already exists.
--
CALL SYSCS_UTIL.SYSCS_EXPORT_QUERY
('select * from DERBY_2925_TAB', 'extout/derby-2925-query.dat', null , null , null ) ;

--
-- Testing SYSCS_UTIL.SYSCS_EXPORT_QUERY_LOBS_TO_EXTFILE
--

create table derby_2925_lob
(
	id 	int,
        name 	varchar(30),
        content clob, 
        pic 	blob 
);

--
-- Testing SYSCS_UTIL.SYSCS_EXPORT_QUERY_LOBS_TO_EXTFILE
-- where data file exists.
--

CALL SYSCS_UTIL.SYSCS_EXPORT_QUERY_LOBS_TO_EXTFILE
('SELECT * FROM DERBY_2925_LOB','extout/derby-2925_data.dat', '\t' ,'|','UTF-16','extout/derby-2925_lobs.dat');
--
-- Errors should should happen in the second
-- call to SYSCS_UTIL.SYSCS_EXPORT_QUERY_LOBS_TO_EXTFILE
-- since extout/derby-2925_data.dat already exists.
--
CALL SYSCS_UTIL.SYSCS_EXPORT_QUERY_LOBS_TO_EXTFILE
('SELECT * FROM DERBY_2925_LOB','extout/derby-2925_data.dat', '\t' ,'|','UTF-16','extout/derby-2925_lobs.dat');

--
-- Testing SYSCS_UTIL.SYSCS_EXPORT_QUERY_LOBS_TO_EXTFILE
-- where lob file exists.
--
-- Errors should should happen in the 
-- call to SYSCS_UTIL.SYSCS_EXPORT_QUERY_LOBS_TO_EXTFILE
-- since extout/derby-2925_lobs.dat already exists.
--

CALL SYSCS_UTIL.SYSCS_EXPORT_QUERY_LOBS_TO_EXTFILE
('SELECT * FROM DERBY_2925_LOB','extout/derby-2925_data1.dat', '\t' ,'|','UTF-16','extout/derby-2925_lobs.dat');

--
-- end test case for derby-2925:
