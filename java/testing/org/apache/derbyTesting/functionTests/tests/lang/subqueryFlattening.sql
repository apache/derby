-- test subquery flattening into outer query block

set isolation to rr;

-- tests for flattening a subquery based on a
-- uniqueness condition

-- by default, holdability of ResultSet objects created using this Connection object is true. Following will set it to false for this connection.
NoHoldForConnection;

-- create some tables
create table outer1 (c1 int, c2 int, c3 int);
create table outer2 (c1 int, c2 int, c3 int);
create table noidx (c1 int);
create table idx1 (c1 int);
create unique index idx1_1 on idx1(c1);
create table idx2 (c1 int, c2 int);
create unique index idx2_1 on idx2(c1, c2);
create table nonunique_idx1 (c1 int);
create index nonunique_idx1_1 on nonunique_idx1(c1);


insert into outer1 values (1, 2, 3);
insert into outer1 values (4, 5, 6);
insert into outer2 values (1, 2, 3);
insert into outer2 values (4, 5, 6);
insert into noidx values 1, 1;
insert into idx1 values 1, 2;
insert into idx2 values (1, 1), (1, 2);
insert into nonunique_idx1 values 1, 1;

-- cases where subqueries don't get flattened
-- (we would get incorrect results with 
-- incorrect flattening)
-- one of tables in subquery doesn't have index
select * from outer1 where c1 in (select idx1.c1 from noidx, idx1 where idx1.c1 = noidx.c1);
-- group by in subquery
select * from outer1 o where c1 <= (select c1 from idx1 i group by c1);
-- otherwise flattenable subquery under an or 
-- subquery returns no rows
select * from outer1 o where c1 + 0 = 1 or c1 in (select c1 from idx1 i where i.c1 = 0);
select * from outer1 o where c1 in (select c1 from idx1 i where i.c1 = 0) or c1 + 0 = 1;
-- empty subquery in select list which is otherwise flattenable
select (select c1 from idx1 where c1 = 0) from outer1;
-- multiple tables in subquery
-- no one table's equality condition based
-- solely on constants and correlation columns
select * from outer1 o where exists (select * from idx2 i, idx1 where o.c1 = i.c1 and i.c2 = idx1.c1);

-- subqueries that should get flattened
call SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1);
maximumdisplaywidth 2000;

-- simple IN
select * from outer1 o where o.c1 in (select c1 from idx1);
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
-- simple EXISTS
select * from outer1 o where exists (select * from idx1 i where o.c1 = i.c1);
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
-- simple ANY
select * from outer1 o where o.c1 = ANY (select c1 from idx1);
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
-- another simple ANY
select * from outer1 o where o.c2 > ANY (select c1 from idx1 i where o.c1 = i.c1);
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- comparisons with parameters
prepare p1 as 'select * from outer1 o where exists (select * from idx1 i where i.c1 = ?)';
execute p1 using 'values 1';
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
prepare p2 as 'select * from outer1 o where ? = ANY (select c1 from idx1)';
execute p2 using 'values 1';
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- mix constants with correlation columns
select * from outer1 o where exists (select * from idx2 i where o.c1 = i.c1 and i.c2 = 2);
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- multiple tables in subquery
select * from outer1 o where exists (select * from idx2 i, idx1 where o.c1 = i.c1 and i.c2 = idx1.c1 and i.c2 = 1);
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- comparisons with non-join expressions
select * from outer1 o where exists (select * from idx1 where idx1.c1 = 1 + 0);
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
select * from outer1 o where exists (select * from idx2 i, idx1 where o.c1 + 0 = i.c1 and i.c2 + 0 = idx1.c1 and i.c2 = 1 + 0);
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- multilevel subqueries
-- flatten all
select * from outer1 o where exists
    (select * from idx2 i where exists
        (select * from idx1 ii 
         where o.c1 = i.c1 and i.c2 = ii.c1 and i.c2 = 1));
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
-- only flatten bottom
select * from outer1 o where exists
    (select * from idx2 i where exists
        (select * from idx1 ii 
         where o.c1 = i.c1 and i.c2 = ii.c1));
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
-- flatten innermost into exists join, then flatten middle
-- into outer
select * from outer1 o where exists
    (select * from idx2 i 
     where  o.c1 = i.c1 and i.c2 = 1 and exists
        (select * from idx1 ii));
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- flatten a subquery that has a subquery in its select list
-- verify that subquery gets copied up to outer block
select * from outer1 o where c1 in
    (select (select c1 from idx1 where c1 = i.c1)
     from idx2 i where o.c1 = i.c1 and i.c2 = 1);
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- expression subqueries
-- simple =
select * from outer1 o where o.c1 = (select c1 from idx1 i where o.c1 = i.c1);
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
select * from outer1 o where o.c1 <= (select c1 from idx1 i where o.c1 = i.c1);
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
-- multiple tables in subquery
select * from outer1 o where c1 =  (select i.c1 from idx2 i, idx1 where o.c1 = i.c1 and i.c2 = idx1.c1 and i.c2 = 1);
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- flattening to an exists join
-- no index on subquery table
select * from outer1 where c1 in (select c1 from noidx);
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
-- no unique index on subquery table
select * from outer1 where c1 in (select c1 from nonunique_idx1);
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
-- columns in subquery are not superset of unique index
select * from outer1 where c1 in (select c1 from idx2);
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
-- single table subquery, self join on unique column
select * from outer1 where exists (select * from idx1 where c1 = c1);
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- flattening values subqueries
-- flatten unless contains a subquery
select * from outer1 where c1 in (values 1);
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
select * from outer1 where c1 in (values (select max(c1) from outer1));
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- beetle 4459 - problems with flattening to exist joins and then flattening to 
-- normal join
-- non correlated exists subquery with conditional join
maximumdisplaywidth 5000;
select o.c1 from outer1 o join outer2 o2 on (o.c1 = o2.c1) 
where exists (select c1 from idx1);
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- in predicate (will be flattened to exists)
select o.c1 from outer1 o join outer2 o2 on (o.c1 = o2.c1) 
where o.c1 in (select c1 from idx1);
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
-- flattened exists join in nested subquery
select c1 from (select t.c1 from (select o.c1 from outer1 o join outer2 o2 on (o.c1 = o2.c1) where exists (select c1 from idx1)) t, outer2 where t.c1 = outer2.c1) t2;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
-- original reported bug
create table business(businesskey int, name varchar(50), changedate int);
create table nameelement(parentkey int, parentelt varchar(50), seqnum int);
create table categorybag(cbparentkey int, cbparentelt varchar(50), 
	krtModelKey varchar(50), keyvalue varchar(50));
select businesskey, name, changedate 
from business as biz left outer join nameelement as nameElt 
	on (businesskey = parentkey and parentelt = 'businessEntity') 
where (nameElt.seqnum = 1) 
	and businesskey in 
		 (select cbparentkey 
			from categorybag 
			where (cbparentelt = 'businessEntity') and 
				(krtModelKey = 'UUID:CD153257-086A-4237-B336-6BDCBDCC6634' and keyvalue = '40.00.00.00.00'))  order by name asc , biz.changedate asc;

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
-- clean up
drop table outer1;
drop table outer2;
drop table noidx;
drop table idx1;
drop table idx2;
drop table nonunique_idx1;
drop table business;
drop table nameelement;
drop table categorybag;


-- --------------------------------------------------------------------
-- TEST CASES for different kinds of subquery flattening, Beetle 5173
-- --------------------------------------------------------------------

drop table colls;
drop table docs;

CREATE TABLE "APP"."COLLS" ("ID" VARCHAR(128) NOT NULL, "COLLID" SMALLINT NOT NULL);
CREATE INDEX "APP"."NEW_INDEX3" ON "APP"."COLLS" ("COLLID");
CREATE INDEX "APP"."NEW_INDEX2" ON "APP"."COLLS" ("ID");
ALTER TABLE "APP"."COLLS" ADD CONSTRAINT "NEW_KEY2" UNIQUE ("ID", "COLLID");

CREATE TABLE "APP"."DOCS" ("ID" VARCHAR(128) NOT NULL);
CREATE INDEX "APP"."NEW_INDEX1" ON "APP"."DOCS" ("ID");
ALTER TABLE "APP"."DOCS" ADD CONSTRAINT "NEW_KEY1" PRIMARY KEY ("ID");

insert into colls values ('123', 2);
insert into colls values ('124', -5);
insert into colls values ('24', 1);
insert into colls values ('26', -2);
insert into colls values ('36', 1);
insert into colls values ('37', 8);

insert into docs values '24', '25', '36', '27', '124', '567';

call SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1);
maximumdisplaywidth 10000;

-- NOT IN is flattened
SELECT COUNT(*) FROM
( SELECT ID FROM DOCS WHERE
        ( ID NOT IN (SELECT ID FROM COLLS WHERE COLLID IN (-2,1) ) )
) AS TAB;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- NOT EXISTS is flattened
SELECT COUNT(*) FROM
( SELECT ID FROM DOCS WHERE
        ( NOT EXISTS  (SELECT ID FROM COLLS WHERE DOCS.ID = COLLS.ID
AND COLLID IN (-2,1) ) )
) AS TAB;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- EXISTS is flattened
SELECT COUNT(*) FROM
( SELECT ID FROM DOCS WHERE
        ( EXISTS  (SELECT ID FROM COLLS WHERE DOCS.ID = COLLS.ID
AND COLLID IN (-2,1) ) )
) AS TAB;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- IN is flattened
SELECT count(ID) FROM DOCS WHERE ID IN (SELECT ID FROM COLLS WHERE COLLID IN (-2,1) );
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- ANY is flattened
SELECT count(ID) FROM DOCS WHERE ID > ANY (SELECT ID FROM COLLS WHERE COLLID IN (-2,1) );
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
-- ANY is flattened
SELECT count(ID) FROM DOCS WHERE ID <> ANY (SELECT ID FROM COLLS WHERE COLLID IN (-2,1) );
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- ALL is flattened, what's not?
SELECT count(ID) FROM DOCS WHERE ID = ALL (SELECT ID FROM COLLS WHERE COLLID IN (-2,1) );
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
-- ALL is flattened, what's not?
SELECT count(ID) FROM DOCS WHERE ID < ALL (SELECT ID FROM COLLS WHERE COLLID IN (-2,1) );
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
-- ALL is flattened, what's not?
SELECT count(ID) FROM DOCS WHERE ID <> ALL (SELECT ID FROM COLLS WHERE COLLID IN (-2,1) );
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- Now test nullable correlated columns
drop table colls;
-- the only change is ID is now nullable
CREATE TABLE "APP"."COLLS" ("ID" VARCHAR(128), "COLLID" SMALLINT NOT NULL);
CREATE INDEX "APP"."NEW_INDEX3" ON "APP"."COLLS" ("COLLID");
CREATE INDEX "APP"."NEW_INDEX2" ON "APP"."COLLS" ("ID");
ALTER TABLE "APP"."COLLS" ADD CONSTRAINT "NEW_KEY2" UNIQUE ("ID", "COLLID");
insert into colls values ('123', 2);
insert into colls values ('124', -5);
insert into colls values ('24', 1);
insert into colls values ('26', -2);
insert into colls values ('36', 1);
insert into colls values ('37', 8);
insert into colls values (null, -2);
insert into colls values (null, 1);
insert into colls values (null, 8);

-- NOT EXISTS should be flattened
SELECT COUNT(*) FROM
( SELECT ID FROM DOCS WHERE
        ( NOT EXISTS  (SELECT ID FROM COLLS WHERE DOCS.ID = COLLS.ID
AND COLLID IN (-2,1) ) )
) AS TAB;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- EXISTS should be flattened
SELECT COUNT(*) FROM
( SELECT ID FROM DOCS WHERE
        ( EXISTS  (SELECT ID FROM COLLS WHERE DOCS.ID = COLLS.ID
AND COLLID IN (-2,1) ) )
) AS TAB;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- IN should be flattened
SELECT count(ID) FROM DOCS WHERE ID IN (SELECT ID FROM COLLS WHERE COLLID IN (-2,1) );
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- ANY should be flattened
SELECT count(ID) FROM DOCS WHERE ID > ANY (SELECT ID FROM COLLS WHERE COLLID IN (-2,1) );
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- ALL should NOT be flattened, but subquery should be materialized
SELECT count(ID) FROM DOCS WHERE ID <> ALL (SELECT ID FROM COLLS WHERE COLLID IN (-2,1) );
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();


-- Now we make the other correlated column also nullable
drop table docs;
CREATE TABLE "APP"."DOCS" ("ID" VARCHAR(128));
CREATE INDEX "APP"."NEW_INDEX1" ON "APP"."DOCS" ("ID");

insert into docs values '24', '25', '36', '27', '124', '567';
insert into docs values null;

-- NOT EXISTS should be flattened
SELECT COUNT(*) FROM
( SELECT ID FROM DOCS WHERE
        ( NOT EXISTS  (SELECT ID FROM COLLS WHERE DOCS.ID = COLLS.ID
AND COLLID IN (-2,1) ) )
) AS TAB;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- EXISTS should be flattened
SELECT COUNT(*) FROM
( SELECT ID FROM DOCS WHERE
        ( EXISTS  (SELECT ID FROM COLLS WHERE DOCS.ID = COLLS.ID
AND COLLID IN (-2,1) ) )
) AS TAB;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- IN should be flattened
SELECT count(ID) FROM DOCS WHERE ID IN (SELECT ID FROM COLLS WHERE COLLID IN (-2,1) );
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- ANY should be flattened
SELECT count(ID) FROM DOCS WHERE ID > ANY (SELECT ID FROM COLLS WHERE COLLID IN (-2,1) );
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- ALL should NOT be flattened, but subquery should be materialized, watch out results
SELECT count(ID) FROM DOCS WHERE ID <> ALL (SELECT ID FROM COLLS WHERE COLLID IN (-2,1) );
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

drop table t1;
drop table t2;
drop table t3;
drop table t4;
create table t1 (c1 int not null);
create table t2 (c1 int not null);
create table t3 (c1 int not null);
create table t4 (c1 int);

insert into t1 values 1,2,3,4,5,1,2;
insert into t2 values 1,4,5,1,1,5,4;
insert into t3 values 4,4,3,3;
insert into t4 values 1,1,2,2,3,4,5,5;

-- should return 2,3,2
select * from t1 where not exists (select * from t2 where t1.c1=t2.c1);
-- should be flattened
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- watch out result, should return 2,3,4,2
select * from t1 where not exists (select * from t2 where t1.c1=t2.c1 and t2.c1
not in (select t3.c1 from t3, t4));
-- can not be flattened, should be materialized
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- should return 1,5,1
select * from t1 where exists (select * from t2 where t1.c1=t2.c1 and t2.c1 not
in (select t3.c1 from t3, t4));
-- can not be flattened, should be materialized
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

drop table colls;
drop table docs;
drop table t1;
drop table t2;
drop table t3;
drop table t4;
