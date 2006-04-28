-- test for import export thru command line interface

-- first test basic import functionality
-- ascii delimited default format
drop table T1;
create table T1 (	Account	int,
			Fname	char(30),
			Lname	char(30),
			Company varchar(35),
			Address varchar(40),
			City	varchar(20),
			State	char(5),
			Zip	char(10),
			Payment	decimal(8,2),
			Balance decimal(8,2));

create index T1_IndexBalance on T1 (Balance, Account, Company);
create index T1_IndexFname on T1 (Fname, Account);
create index T1_IndexLname on T1 (Lname, Account);


call SYSCS_UTIL.SYSCS_IMPORT_TABLE (null, 'T1' , 'extin/EndOfFile.txt' , 
                                    null, null, null, 0) ;

call SYSCS_UTIL.SYSCS_IMPORT_TABLE (null, 'T1' , 'extin/Tutor1.asc' , 
                                    null, null, null, 0) ;

values (SYSCS_UTIL.SYSCS_CHECK_TABLE('APP', 'T1'));

-- ascii Fixed
drop table T2;

autocommit off;
create table T2 (	Account	int,
			Fname	char(30),
			Lname	char(30),
			Company	varchar(35),
			Address	varchar(40),
			City	varchar(20),
			State	char(5),
			Zip	char(10),
			Payment	decimal(8,2),
			Balance	decimal(8,2));

create index T2_IndexBalance on T2 (Balance, Account, Company);
create index T2_IndexFname on T2 (Fname, Account);
create index T2_IndexLname on T2 (Lname, Account);
commit;
--this one should fail becuase this is not the right command to handle fixed formats
call SYSCS_UTIL.SYSCS_IMPORT_TABLE (null, 'T2' , 'extin/Tutor2.asc' , 
                                    null, null, null, 0) ;

values (SYSCS_UTIL.SYSCS_CHECK_TABLE('APP', 'T2'));
commit;

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

--icorrect mapping of file to table ; should give error and rollback
call SYSCS_UTIL.SYSCS_IMPORT_DATA(null, 'T3' , 
                                  null , '3, 2, 1, 4, 9, 200' ,
				  'extin/Tutor3.asc' , 
				  null, null, null, 0) ;
rollback;

-- table should not be there
select count(*) from T3;

create table T3 (	Lname	char(30),
			Fname	char(30),
			Account	int not null primary key,
			Company varchar(35),
			Payment	decimal(8,2),
			Balance decimal(8,2));

create index T3_indexBalance on T3 (Balance, Company, Account);
create index T3_indexPayment on T3 (Payment, Company, Account);

call SYSCS_UTIL.SYSCS_IMPORT_DATA(null, 'T3' ,  
                                  null , '3, 2, 1, 4, 9, 10' ,
				  'extin/Tutor3.asc' ,
				   null, null, null, 0) ;
commit;


values (SYSCS_UTIL.SYSCS_CHECK_TABLE('APP', 'T3'));


-- now check results
select count(*) from T1;
select count(*) from T2;
select count(*) from T3;

select * from T1 where State = 'CA';
select * from T2 where State = 'CA';
select * from T3 where Fname = 'Bugs' or Fname = 'Mickey';

select Balance, Account, Company from T1 order by Balance;
select Balance, Account, Company from T2 order by Balance;
select Balance, Account, Company from T3 order by Balance;
--- now check other input formats
--- this is Tutor1 with RecordSeperator=',', FieldStartDelimiter=(, FieldEndDelimiter=),FieldSeperator=TAB
autocommit on;
drop table Alt1;
create table Alt1 (	Account	int,
			Fname	char(30),
			Lname	char(30),
			Company varchar(35),
			Address varchar(40),
			City	varchar(20),
			State	char(5),
			Zip	char(10),
			Payment	decimal(8,2),
			Balance decimal(8,2));


call SYSCS_UTIL.SYSCS_IMPORT_TABLE (null, 'ALT1' , 'extin/Alt1.asc' , 
                                      null, null, null, 0) ;


select * from Alt1 where State = 'CA';
select Balance, Account, Company from Alt1 order by Balance;


-- this is Tutor1 with some null fields

drop table Alt3;
create table Alt3 (	Account	int,
			Fname	char(30),
			Lname	char(30),
			Company varchar(35),
			Address varchar(40),
			City	varchar(20),
			State	char(5),
			Zip	char(10),
			Payment	decimal(8,2),
			Balance decimal(8,2));



call SYSCS_UTIL.SYSCS_IMPORT_TABLE (null, 'ALT3' , 'extin/Alt3.asc' , 
                                      null, null, null, 0) ;


select * from Alt3 where State = 'CA' or State = 'TX';
select Balance, Account, Company from Alt3 order by Balance;

-- test remapping
-- test remapping
-- following case is commented because data has different seperator than the default one 
-- enable this case after rearranging the data (-suresht)
--create table tempAlt3(column1 varchar(1000) , column3 varchar(1000) , column9 varchar(1000));

--call SYSCS_UTIL.SYSCS_IMPORT_DATA (null, 'tempAlt3' , 
--    				   null , '2,3,9',
--				   'extin/Alt3.asc' , 
--				   null, null, null, 0) ;
--select * from tempAlt3;

---
---  SQL anywhere table
---
drop table sqlAnywhere1;
create table sqlAnywhere1 (
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

call SYSCS_UTIL.SYSCS_IMPORT_DATA(null, 'SQLANYWHERE1' ,
                                  null , '1,2,3,4,5,7,8,9,10,11,12,13,14', 
	 		          'extin/sqlAnywhere1.utf' , 
				  '|', '''', 'ASCII', 0) ;

	
select Company, Country from sqlAnywhere1 where country not like 'U%S%A%' and country is not null;

drop table sqlAnywhere2;
create table sqlAnywhere2 (
	Fname varchar(30),
	Lname varchar(30),
	email varchar(40),
	phone varchar(30));

call SYSCS_UTIL.SYSCS_IMPORT_DATA(null, 'SQLANYWHERE2' , 
		                  null , '3, 4, 7, 5',
			          'extin/sqlAnywhere2.utf' , 
				  '|', '''', 'ASCII', 0) ;
select count(*) from sqlAnywhere2;

---
---  MS access text file
---  default text format is compatible with ours except their record seperator
---  is CR-LF instead of LF
--   data does not seem to match above description , only problem I see
---  is delimiters inside the data , which gets fixed with double delimters
---  check the intended case -suresht

drop table HouseHoldItem;
create table HouseHoldItem (
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

-- import it first with just LF as record seperator, we should be seeing 
-- ^M's at the end of note field.

call SYSCS_UTIL.SYSCS_IMPORT_DATA(null, 'HOUSEHOLDITEM' , 
				  null , '2,3,4,5,6,7,8,11,14', 
				  'extin/Access1.txt' , 
				  null, null, null, 0) ;

select * from HouseHoldItem;



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





