-- simple scroll cursor tests
create table t (a int);
insert into t values (1),(2),(3),(4),(5);
get scroll insensitive cursor c1 as 'select * from t';
-- should be 1
first c1; 
-- should be 2
next c1; 
-- should be 1
previous c1; 
-- should be 5
last c1;
-- should be 2
absolute 2 c1;
-- should be 4
relative 2 c1;
close c1;
-- since JCC gets 64 results and then scrolls within them
-- lets try each positioning command as the first command for the cursor
get scroll insensitive cursor c1 as 'select * from t';
-- should be 1
next c1;
close c1;
get scroll insensitive cursor c1 as 'select * from t';
-- should be 5
last c1;
close c1;
get scroll insensitive cursor c1 as 'select * from t';
-- should be 3
absolute 3 c1;
-- should be 4
next c1;
close c1;
-- let's try a table with more than 64 rows
create table t1 (a int);
insert into t1 values (1),(2),(3),(4),(5),(6),(7),(8),(9),(10);
insert into t1 values (11),(12),(13),(14),(15),(16),(17),(18),(19),(20);
insert into t1 values (21),(22),(23),(24),(25),(26),(27),(28),(29),(30);
insert into t1 values (31),(32),(33),(34),(35),(36),(37),(38),(39),(40);
insert into t1 values (41),(42),(43),(44),(45),(46),(47),(48),(49),(50);
insert into t1 values (51),(52),(53),(54),(55),(56),(57),(58),(59),(60);
insert into t1 values (61),(62),(63),(64),(65),(66),(67),(68),(69),(70);
get scroll insensitive cursor c1 as 'select * from t1';
-- should be 1
first c1;
-- should be 70
last c1;
-- should be 65
absolute 65 c1;
-- should be 70
absolute -1 c1;
close c1;
-- try sensitive scroll cursors bug 4677
get scroll sensitive cursor c1 as 'select * from t';
close c1;
get scroll sensitive cursor c1 as 'select * from t for update';
close c1;

drop table t1;

-- defect 5225, outer joins returning NULLs
create table t1 (i1 bigint not null, c1 varchar(64) not null);
create table t2 (i2 bigint not null, c2 varchar(64) not null);

insert into t1 values (1, 'String 1');
insert into t1 values (2, 'String 2');

insert into t2 values (1, 'String 1');
insert into t2 values (3, 'String 3');

-- Outer joins can return NULLs on the non-outer side of the join
select c1 from t1 right outer join t2 on (i1=i2);
select c2 from t1 right outer join t2 on (i1=i2);

-- Left outer join
select c1 from t1 left outer join t2 on (i1=i2);
select c2 from t1 left outer join t2 on (i1=i2);

drop table t1;
drop table t2;

