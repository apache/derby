--
-- this test is for miscellaneous errors
--

-- lexical error
select @#^%*&! from swearwords;

--
-- try to create duplicate table
--


create table a (one int);

create table a (one int, two int);

create table a (one int);

drop table a ;

create table a (one int, two int, three int);

insert into a values (1,2,3);

select * from a;

drop table a;

-- set isolation to repeatable read
set isolation serializable;

-- see that statements that fail at parse or bind time
-- are not put in the statment cache;
values 1;
select SQL_TEXT from syscs_diag.statement_cache where SQL_TEXT LIKE '%932432%';

VALUES FRED932432;
SELECT * FROM BILL932432;
SELECT 932432;

select SQL_TEXT from syscs_diag.statement_cache where SQL_TEXT LIKE '%932432%';
