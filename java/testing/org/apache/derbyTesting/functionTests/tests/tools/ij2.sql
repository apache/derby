
-- this test shows some ij abilities against an active database

create table t (i int);
insert into t values (3), (4);

prepare s as 'select * from t';
execute s;

remove s;
-- now it won't find s
execute s;

prepare s as 'select * from t where i=?';
-- fails, needs parameter
execute s;

-- works, finds value
execute s using 'values 3';

prepare t as 'values 3';
-- same as last execute
execute s using t;

-- same as last execute
execute 'select * from t where i=?' using 'values 3';

-- same as last execute
execute 'select * from t where i=?' using t;

-- param that is not needed gets out of range message
execute 'select * from t where i=?' using 'values (3,4)';

-- ignores rows that are not needed
execute 'select * from t where i=?' using 'values 3,4';

-- with autocommit off, extra rows are processed and no warning results
autocommit off;
execute 'select * from t where i=?' using 'values 3,4';

execute 'select * from t where i=?' using 'values 3';

autocommit on;

-- will say params not set when no rows in using values
execute 'select * from t where i=?' using 'select * from t where i=9';

-- will say params not set when using values is not a query
execute 'select * from t where i=?' using 'create table s (i int)';

-- note that the using part was, however, executed...
drop table s;

-- bug 5926 - make sure the using clause result set got closed
drop table t;
create table t(c1 int);
insert into t values(1);
execute 'select * from t where c1=?' using 'select * from t where c1=1';
drop table t;
create table t(c1 int);
insert into t values(1);
insert into t values(2);
execute 'select * from t where c1=?' using 'select * from t where c1>=1';
drop table t;

-- show that long fields now don't take forever...

create table t ( c char(50));
insert into t values('hello');
select cast(c as varchar(20)) from t;
drop table t;

-- show multiconnect ability; db name is wombat, reuse it...
-- assumes ij.protocol is appropriately set...

connect 'wombat' as wombat;

show connections;

set connection connection0;

show connections;

set connection wombat;

disconnect;

show connections;

set connection connection0;

show connections;

