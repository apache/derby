drop table tab1;
create table tab1( c1 decimal(5,3), c2 date, c3 char(20) );
insert into tab1 values(12.345, date('2000-05-25'),  'test row 1');
insert into tab1 values(32.432, date('2000-01-14'),  'test row 2');
insert into tab1 values(54.846, date('2000-08-21'),  'test row 3');
insert into tab1 values(98.214, date('2000-12-08'),  'test row 4');
insert into tab1 values(77.406, date('2000-10-19'),  'test row 5');
insert into tab1 values(50.395, date('2000-11-29'),  'test row 6');

call SYSCS_UTIL.SYSCS_EXPORT_TABLE (null, 'tab1' , 'extinout/tab1_fr.unl' , 
                                    null, null, 'UTF8') ;

-- localized display is off
select * from tab1;

LOCALIZEDDISPLAY ON;
select * from tab1;

drop table tab1;
create table tab1( c1 decimal(5,3), c2 date, c3 char(20) );

call SYSCS_UTIL.SYSCS_IMPORT_TABLE (null, 'tab1' , 'extinout/tab1_fr.unl' , 
                                    null, null, 'UTF8', 0) ;


-- localized display is off
LOCALIZEDDISPLAY OFF;
select * from tab1;

LOCALIZEDDISPLAY ON;
select * from tab1;

