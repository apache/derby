disconnect;

connect 'wombat;user=U1' AS C1;
connect 'wombat;user=U2' AS C2;

set connection C1;

create table t1(c1 int, c2 int);
insert into t1 values (1, 2), (3, 4), (5, 6), (7, 8), (9, 10);
get scroll insensitive cursor c1 as 'select * from t1';

set connection C2;
-- see what happens to other user when we close our cursor
-- before they are done.
get scroll insensitive cursor c1 as 'select * from U1.t1';

set connection C1;
next c1;

set connection C2;
next c1;

set connection C1;
last c1;

set connection C2;
last c1;

set connection C1;
previous c1;

set connection C2;
close c1;

set connection C1;
first c1;
close c1;

drop table t1;
disconnect;
set connection C2;
disconnect;
