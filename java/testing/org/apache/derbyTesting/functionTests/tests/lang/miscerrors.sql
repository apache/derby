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
