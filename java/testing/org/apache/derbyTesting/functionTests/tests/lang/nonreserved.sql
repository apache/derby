-- This tests that SQL92 formally reserved words are now unreserved
--

-- INTERVAL
create table interval(interval int);
prepare interval as 'select * from interval';
execute interval;
create index interval on interval(interval);
drop table interval;
remove interval;
-- MODULE
create table module(module int);
prepare module as 'select * from module';
execute module;
create index module on module(module);
drop table module;
remove module;
-- NAMES
create table names(names int);
prepare names as 'select * from names';
execute names;
create index names on names(names);
drop table names;
remove names;
-- PRECISION
create table precision(precision int);
prepare precision as 'select * from precision';
execute precision;
create index precision on precision(precision);
drop table precision;
remove precision;
-- POSITION
create table position(position int);
prepare position as 'select * from position';
execute position;
create index position on position(position);
drop table position;
remove position;
-- SECTION
create table section(section int);
prepare section as 'select * from section';
execute section;
create index section on section(section);
drop table section;
remove section;
-- VALUE
create table value(value int);
prepare value as 'select * from value';
execute value;
create index value on value(value);
drop table value;
remove value;
-- DATE
create table date (date date);
insert into date(date) values (date('2001-01-01'));
select date from date;
select date( '2001-02-02'), date "2001-02-02" from date;
select date date from date;
select date as date from date;
select date.date as date from date date;
select date.date as date from date as date;
delete from date where date = date('2001-01-01');
create index date on date(date);
drop table date;
remove date;
-- TIME
create table time (time time);
insert into time(time) values (time('01:01:01'));
select time from time;
select time( '02:02:02'), time "02:02:02" from time;
select time time from time;
select time as time from time;
select time.time as time from time time;
select time.time as time from time as time;
delete from time where time = time('01:01:01');
create index time on time(time);
drop table time;
remove time;
-- TIMESTAMP
create table timestamp (timestamp timestamp);
insert into timestamp(timestamp) values (timestamp('2002-05-22 16:17:34.144'));
select timestamp from timestamp;
select timestamp( '2003-05-22 16:17:34.144'), timestamp "2003-05-22 16:17:34.144" from timestamp;
select timestamp timestamp from timestamp;
select timestamp as timestamp from timestamp;
select timestamp.timestamp as timestamp from timestamp timestamp;
select timestamp.timestamp as timestamp from timestamp as timestamp;
delete from timestamp where timestamp = timestamp('2002-05-22 16:17:34.144');
create index timestamp on timestamp(timestamp);
drop table timestamp;
remove timestamp;
-- 
create table DOMAIN (domain int);
insert into domain values (1);
select domain from domain where domain > 0;
select domain from domain domain where domain > 0;
select domain.domain from domain domain where domain.domain > 0;
prepare domain as 'select * from domain';
execute domain;
create index domain on domain(domain);
drop table DOMAIN;
remove domain;

create table CATALOG (catalog int);
insert into catalog values (1);
select catalog from catalog where catalog > 0;
select catalog from catalog catalog where catalog > 0;
prepare catalog as 'select * from catalog';
execute catalog;
create index catalog on catalog(catalog);
drop table CATALOG;
remove catalog;

create table TIME (time int);
insert into time values (1);
select time from time where time > 0;
select time from time time where time > 0;
prepare time as 'select * from time';
execute time;
create index time on time(time);
drop table TIME;
remove time;

create table ACTION (action int);
insert into action values (1);
select action from action where action > 0;
select action from action action where action > 0;
prepare action as 'select * from action';
create index action on action(action);
drop table ACTION;

create table DAY (day int);
insert into day values (1);
select day from day where day > 0;
select day from day day where day > 0;
prepare day as 'select * from day';
create index day on day(day);
drop table DAY;

create table MONTH (month int);
insert into month values (1);
select month from month where month > 0;
select month from month month where month > 0;
select month.month from month month where month.month > 0;
prepare month as 'select * from month';
execute month;
create index month on month(month);
drop table MONTH;
remove month;

create table USAGE (usage int);
insert into usage values (1);
select usage from usage where usage > 0;
select usage from usage usage where usage > 0;
select usage.usage from usage usage where usage.usage > 0;
prepare usage as 'select * from usage';
create index usage on usage(usage);
drop table USAGE;
remove usage;

create table LANGUAGE (language int);
insert into language values (1);
select language from language where language > 0;
select language from language language where language > 0;
select language.language from language language where language.language > 0;
prepare language as 'select * from language';
create index language on language(language);
drop table LANGUAGE;
remove language;

-- making LOCKS keyword nonreserved as fix for Derby-38
create table LOCKS (c11 int);
drop table LOCKS;
create table t1 (LOCKS int);
drop table t1;
create table LOCKS (locks int);
insert into locks values (1);
select locks from locks where locks > 0;
select locks from locks locks where locks > 0;
select locks.locks from locks locks where locks.locks > 0;
prepare locks as 'select * from locks';
create index locks on locks(locks);
drop table LOCKS;
remove locks;
