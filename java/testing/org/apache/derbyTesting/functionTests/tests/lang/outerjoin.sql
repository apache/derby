-- test outer joins
-- (NO NATURAL JOIN)


autocommit off;

-- create some tables

create table t1(c1 int);
create table t2(c1 int);
create table t3(c1 int);
create table tt1(c1 int, c2 int, c3 int);
create table tt2(c1 int, c2 int, c3 int);
create table tt3(c1 int, c2 int, c3 int);
create table empty_table(c1 int);
create table insert_test(c1 int, c2 int, c3 int);
-- following is verifying that oj is not a keyword
create table oj(oj int);

-- populate the tables
insert into t1 values 1, 2, 2, 3, 4;
insert into t2 values 1, 3, 3, 5, 6;
insert into t3 values 2, 3, 5, 5, 7;
insert into tt1 select c1, c1, c1 from t1;
insert into tt2 select c1, c1, c1 from t2;
insert into tt3 select c1, c1, c1 from t3;
-- verifying that oj is not a keyword
insert into oj(oj) values (1);

-- negative tests

-- no outer join type
select * from t1 outer join t2;

-- no join clause
select * from t1 left outer join t2;
select * from t1 right outer join t2;


-- positive tests

select t1.c1 from t1 left outer join t2 on t1.c1 = t2.c1;
select t2.c1 from t1 right outer join t2 on t1.c1 = t2.c1;
select a.x from t1 a (x) left outer join t2 b (x) on a.x = b.x;

-- verify that selects from inner table work
select b.* from (values 9) a left outer join t2 b on 1=1;
select b.* from (values 9) a left outer join t2 b on 1=0;
select b.* from (values 9) a right outer join t2 b on 1=0;
select a.* from (values 9) a right outer join t2 b on 1=1;
select a.* from (values 9) a right outer join t2 b on 1=0;
select a.* from ((values ('a', 'b')) a inner join (values ('c', 'd')) b on 1=1) left outer join (values ('e', 'f')) c on 1=1;
select b.* from ((values ('a', 'b')) a inner join (values ('c', 'd')) b on 1=1) left outer join (values ('e', 'f')) c on 1=1;
select c.* from ((values ('a', 'b')) a inner join (values ('c', 'd')) b on 1=1) left outer join (values ('e', 'f')) c on 1=1;

-- verifying that oj is not a keyword
select * from oj where oj = 1;

--verifying both regular and {oj } in 
select * from t1 left outer join {oj t2 left outer join t3 on t2.c1=t3.c1} on t1.c1=t3.c1;

-- left and right outer join with an empty table
select t1.c1 from t1 left outer join empty_table et on t1.c1 = et.c1;
select t1.c1 from t1 right outer join empty_table et on t1.c1 = et.c1;
select t1.c1 from empty_table et right outer join t1 on et.c1 = t1.c1;

-- this query may make no sense at all, but it's just trying to show that parser works
-- fine with both regular tableexpression and tableexpression with {oj }
select * from t1, {oj t2 join t3 on t2.c1=t3.c1};

-- parameters and join clause
prepare asdf as 'select * from t1 left outer join t2 on 1=? and t1.c1 = t2.c1';
execute asdf using 'values 1';
remove asdf;

prepare asdf as 'select * from t1 left outer join t2 on t1.c1 = t2.c1 and t1.c1 = ?';
execute asdf using 'values 1';
remove asdf;

-- additional predicates outside of the join clause
-- egs of using {oj --} syntax
select * from t1 left outer join t2 on t1.c1 = t2.c1 where t1.c1 = 1;
select * from {oj t1 left outer join t2 on t1.c1 = t2.c1} where t1.c1 = 1;
select * from t1 right outer join t2 on t1.c1 = 1 where t2.c1 = t1.c1;
select * from {oj t1 right outer join t2 on t1.c1 = 1} where t2.c1 = t1.c1;

-- subquery in join clause. Not allowed in the DB2 compatibility mode. ERROR.
-- egs of using {oj --} syntax
select * from t1 a left outer join t2 b 
on a.c1 = b.c1 and a.c1 = (select c1 from t1 where a.c1 = t1.c1 and a.c1 = 1);
select * from {oj t1 a left outer join t2 b 
on a.c1 = b.c1 and a.c1 = (select c1 from t1 where a.c1 = t1.c1 and a.c1 = 1)};
select * from t1 a left outer join t2 b 
on a.c1 = b.c1 and a.c1 = (select c1 from t1 where a.c1 = t1.c1 and a.c1 <> 2);
select * from {oj t1 a left outer join t2 b 
on a.c1 = b.c1 and a.c1 = (select c1 from t1 where a.c1 = t1.c1 and a.c1 <> 2)};
select * from t1 a right outer join t2 b 
on a.c1 = b.c1 and a.c1 in (select c1 from t1 where a.c1 = t1.c1);

-- outer join in subquery
-- egs of using {oj --} syntax
select * from t1 a
where exists (select * from t1 left outer join t2 on t1.c1 = t2.c1);
select * from t1 a
where exists (select * from {oj t1 left outer join t2 on t1.c1 = t2.c1});
select * from t1 a
where exists (select * from t1 left outer join t2 on 1=0);

-- nested joins
-- egs of using {oj --} syntax
select * from t1 left outer join t2 on t1.c1 = t2.c1 left outer join t3 on t1.c1 = t3.c1;
select * from {oj t1 left outer join t2 on t1.c1 = t2.c1 left outer join t3 on t1.c1 = 
t3.c1};
select * from t1 left outer join t2 on t1.c1 = t2.c1 left outer join t3 on t2.c1 = t3.c1;
select * from t3 right outer join t2 on t3.c1 = t2.c1 right outer join t1 on t1.c1 = t2.c1;

-- parens
select * from (t1 left outer join t2 on t1.c1 = t2.c1) left outer join t3 on t1.c1 = t3.c1;
select * from t1 left outer join (t2 left outer join t3 on t2.c1 = t3.c1) on t1.c1 = t2.c1;

-- left/right outer join combinations
select * from t1 a right outer join t2 b on a.c1 = b.c1 left outer join t3 c on a.c1 = b.c1 and b.c1 = c.c1;
select * from (t1 a right outer join t2 b on a.c1 = b.c1) left outer join t3 c on a.c1 = b.c1 and b.c1 = c.c1;

select * from t1 a left outer join t2 b on a.c1 = b.c1 right outer join t3 c on c.c1 = a.c1 where a.c1 is not null;
select * from (t1 a left outer join t2 b on a.c1 = b.c1) right outer join t3 c on c.c1 = a.c1 where a.c1 is not null;
select * from t1 a left outer join (t2 b right outer join t3 c on c.c1 = b.c1) on a.c1 = c.c1 where c.c1=b.c1;

-- test insert/update/delete
insert into insert_test
select * from t1 a left outer join t2 b on a.c1 = b.c1 left outer join t3 c on a.c1 <> c.c1;
select * from insert_test;

update insert_test
set c1 = (select 9 from t1 a left outer join t1 b on a.c1 = b.c1 where a.c1 = 1)
where c1 = 1;
select * from insert_test;

delete from insert_test
where c1 = (select 9 from t1 a left outer join t1 b on a.c1 = b.c1 where a.c1 = 1);
select * from insert_test;

delete from insert_test;

insert into insert_test
select * from (select * from t1 a left outer join t2 b on a.c1 = b.c1 left outer join t3 c on a.c1 <> c.c1) d (c1, c2, c3);
select * from insert_test;
delete from insert_test;

-- verify that right outer join xforms don't get result columns
-- confused
create table a (c1 int);
create table b (c2 float);
create table c (c3 char(30));

insert into a values 1;
insert into b values 3.3;
insert into c values 'asdf';

select * from a left outer join b on 1=1 left outer join c on 1=1;
select * from a left outer join b on 1=1 left outer join c on 1=0;
select * from a left outer join b on 1=0 left outer join c on 1=1;
select * from a left outer join b on 1=0 left outer join c on 1=0;

select * from c right outer join b on 1=1 right outer join a on 1=1;
select * from c right outer join b on 1=1 right outer join a on 1=0;
select * from c right outer join b on 1=0 right outer join a on 1=1;
select * from c right outer join b on 1=0 right outer join a on 1=0;

-- multicolumn tests
-- c1, c2, and c3 all have the same values
select tt1.c1, tt1.c2, tt1.c3, tt2.c2, tt2.c3 from tt1 left outer join tt2 on tt1.c1 = tt2.c1;
select tt1.c1, tt1.c2, tt1.c3, tt2.c3 from tt1 left outer join tt2 on tt1.c1 = tt2.c1;
select tt1.c1, tt1.c2, tt1.c3 from tt1 left outer join tt2 on tt1.c1 = tt2.c1;

-- nested outer joins
select tt1.c2, tt1.c1, tt1.c3, tt2.c1, tt2.c3 from t1 left outer join tt1 on t1.c1 = tt1.c1 left outer join tt2 on tt1.c2 = tt2.c2;

-- make sure that column reordering is working correctly 
-- when there's an ON clause
create table x (c1 int, c2 int, c3 int);
create table y (c3 int, c4 int, c5 int);
insert into x values (1, 2, 3), (4, 5, 6);
insert into y values (3, 4, 5), (666, 7, 8);

-- qualfied * will return all of the columns of the qualified table
-- including join columns
select x.* from x join y on x.c3 = y.c3;
select x.* from x left outer join y on x.c3 = y.c3;
select x.* from x right outer join y on x.c3 = y.c3;
select y.* from x join y on x.c3 = y.c3;
select y.* from x left outer join y on x.c3 = y.c3;
select y.* from x right outer join y on x.c3 = y.c3;

-- * will return all of the columns of all joined tables
select * from x join y on x.c3 = y.c3;
select * from x left outer join y on x.c3 = y.c3;
select * from x right outer join y on x.c3 = y.c3;

commit;

-- test outer join -> inner join xform
delete from tt1;
delete from tt2;
delete from tt3;

insert into tt1 values (1, 2, 3), (2, 3, 4), (3, 4, 5);
insert into tt2 values (1, 2, 3), (2, 3, 4), (3, 4, 5);
insert into tt3 values (1, 2, 3), (2, 3, 4), (3, 4, 5);

call SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1);
maximumdisplaywidth 4500;

-- no xform, predicate on outer table
select * from tt1 left outer join tt2 on tt1.c1 = tt2.c2 where tt1.c1 = 3;

-- various predicates on inner table
select * from tt1 left outer join tt2 on tt1.c1 = tt2.c2 where tt2.c2 = 3;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
select * from tt1 left outer join tt2 on tt1.c1 = tt2.c2 where tt2.c1 + 1= tt2.c2;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
select * from tt1 left outer join tt2 on tt1.c1 = tt2.c2 where tt2.c1 + 1= 3;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
select * from tt2 right outer join tt1 on tt1.c1 = tt2.c2 where tt2.c1 + 1= 3;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
select * from tt1 left outer join tt2 on tt1.c1 = tt2.c2 left outer join tt3 on tt2.c2 = tt3.c3 where tt3.c3 = 3;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
select * from tt1 left outer join tt2 on tt1.c1 = tt2.c2 left outer join tt3 on tt2.c2 = tt3.c3 where tt2.c2 = 3;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- make sure predicates are null tolerant 
select * from tt1 left outer join tt2 on tt1.c1 = tt2.c2 
where char(tt2.c2) is null;
-- where java.lang.Integer::toString(tt2.c2) = '2';
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
commit;

-- bug 2924, cross join under an outer join
CREATE TABLE inventory(itemno INT NOT NULL PRIMARY KEY, capacity INT);

INSERT INTO inventory VALUES (1, 4);
INSERT INTO inventory VALUES (2, 2);
INSERT INTO inventory VALUES (3, 2);

CREATE TABLE timeslots (slotno INT NOT NULL PRIMARY KEY);

INSERT INTO timeslots VALUES(1);
INSERT INTO timeslots VALUES(2);

create table reservations(slotno INT CONSTRAINT timeslots_fk REFERENCES timeslots, 
itemno INT CONSTRAINT inventory_fk REFERENCES inventory, 
name VARCHAR(100), resdate DATE);

    
INSERT INTO reservations VALUES(1, 1, 'Joe', '2000-04-14');
INSERT INTO reservations VALUES(1, 1, 'Fred', '2000-04-13');

-- This query used to cause a null pointer exception
   
select name, resdate 
from reservations left outer join (inventory join timeslots on inventory.itemno = timeslots.slotno)
on inventory.itemno = reservations.itemno and timeslots.slotno = reservations.slotno
where resdate = '2000-04-14';
rollback;

-- bug 2923, cross join under an outer join
create table inventory(itemno INT NOT NULL PRIMARY KEY, capacity INT);
INSERT into inventory values (1, 4);
INSERT into inventory values (2, 2);
INSERT into inventory values (3, 2);

CREATE TABLE timeslots (slotno INT NOT NULL PRIMARY KEY);
INSERT INTO timeslots VALUES(1);
INSERT INTO timeslots VALUES(2);

create table reservations(slotno INT CONSTRAINT timeslots_fk REFERENCES timeslots,
itemno INT CONSTRAINT inventory_fk REFERENCES inventory,
name VARCHAR(100));
INSERT INTO reservations VALUES(1, 1, 'Joe');
INSERT INTO reservations VALUES(2, 2, 'Fred');

-- This query used to get incorrect results
-- when name is null was the 2nd predicate
-- due to a bug in OJ->IJ xform code.
select timeslots.slotno, inventory.itemno, capacity, name
from inventory left outer join timeslots
on inventory.capacity = timeslots.slotno
left outer join reservations
on timeslots.slotno = reservations.slotno
where capacity > 3 and name is null;
select timeslots.slotno, inventory.itemno, capacity, name
from inventory left outer join timeslots
on inventory.capacity = timeslots.slotno
left outer join reservations
on timeslots.slotno = reservations.slotno
where name is null and capacity > 3;

rollback;

-- bug 2930, cross join under outer join
CREATE TABLE properties (
	name VARCHAR(50),
	value VARCHAR(200));

INSERT INTO properties VALUES ('businessName', 'Cloud 9 Cafe');
INSERT INTO properties VALUES ('lastReservationDate', '2001-12-31');

CREATE TABLE inventory (
	itemno INT NOT NULL PRIMARY KEY,
	capacity INT
);
INSERT INTO inventory VALUES (1, 2);
INSERT INTO inventory VALUES (2, 2);
INSERT INTO inventory VALUES (3, 2);
INSERT INTO inventory VALUES (4, 2);
INSERT INTO inventory VALUES (5, 2);
INSERT INTO inventory VALUES (6, 4);
INSERT INTO inventory VALUES (7, 4);
INSERT INTO inventory VALUES (8, 4);
INSERT INTO inventory VALUES (9, 4);
INSERT INTO inventory VALUES (10, 4);

CREATE TABLE timeslots (
	slot TIME NOT NULL PRIMARY KEY
);

INSERT INTO timeslots VALUES('17:00:00');
INSERT INTO timeslots VALUES('17:30:00');
INSERT INTO timeslots VALUES('18:00:00');
INSERT INTO timeslots VALUES('18:30:00');
INSERT INTO timeslots VALUES('19:00:00');
INSERT INTO timeslots VALUES('19:30:00');
INSERT INTO timeslots VALUES('20:00:00');
INSERT INTO timeslots VALUES('20:30:00');
INSERT INTO timeslots VALUES('21:00:00');
INSERT INTO timeslots VALUES('21:30:00');
INSERT INTO timeslots VALUES('22:00:00');

CREATE TABLE reservations (
	itemno INT CONSTRAINT inventory_fk REFERENCES inventory,
	slot TIME CONSTRAINT timeslots_fk REFERENCES timeslots,
	resdate DATE NOT NULL,
	name VARCHAR(100) NOT NULL,
	quantity INT,
	CONSTRAINT reservations_u UNIQUE(name, resdate));

INSERT INTO reservations VALUES(6, '17:00:00', '2000-07-13', 'Williams', 4);
INSERT INTO reservations VALUES(7, '17:00:00', '2000-07-13', 'Johnson',  4);
INSERT INTO reservations VALUES(8, '17:00:00', '2000-07-13', 'Allen',    3);
INSERT INTO reservations VALUES(9, '17:00:00', '2000-07-13', 'Dexmier',  4);
INSERT INTO reservations VALUES(1, '17:30:00', '2000-07-13', 'Gates', 	 2);
INSERT INTO reservations VALUES(2, '17:30:00', '2000-07-13', 'McNealy',  2);
INSERT INTO reservations VALUES(3, '17:30:00', '2000-07-13', 'Hoffman',  1);
INSERT INTO reservations VALUES(4, '17:30:00', '2000-07-13', 'Sippl',    2);
INSERT INTO reservations VALUES(6, '17:30:00', '2000-07-13', 'Yang',     4);
INSERT INTO reservations VALUES(7, '17:30:00', '2000-07-13', 'Meyers',   4);
select max(name), max(resdate) from inventory join timeslots on inventory.capacity is not null
left outer join reservations on inventory.itemno = reservations.itemno and reservations.slot = timeslots.slot;
rollback;

-- bug 2931, cross join under outer join
CREATE TABLE properties (
	name VARCHAR(50),
	value VARCHAR(200));

INSERT INTO properties VALUES ('businessName', 'Cloud 9 Cafe');
INSERT INTO properties VALUES ('lastReservationDate', '2001-12-31');

CREATE TABLE inventory (
	itemno INT NOT NULL PRIMARY KEY,
	capacity INT
);
INSERT INTO inventory VALUES (1, 2);
INSERT INTO inventory VALUES (2, 2);
INSERT INTO inventory VALUES (3, 2);
INSERT INTO inventory VALUES (4, 2);
INSERT INTO inventory VALUES (5, 2);
INSERT INTO inventory VALUES (6, 4);
INSERT INTO inventory VALUES (7, 4);
INSERT INTO inventory VALUES (8, 4);
INSERT INTO inventory VALUES (9, 4);
INSERT INTO inventory VALUES (10, 4);

CREATE TABLE timeslots (
	slot TIME NOT NULL PRIMARY KEY
);

INSERT INTO timeslots VALUES('17:00:00');
INSERT INTO timeslots VALUES('17:30:00');
INSERT INTO timeslots VALUES('18:00:00');
INSERT INTO timeslots VALUES('18:30:00');
INSERT INTO timeslots VALUES('19:00:00');
INSERT INTO timeslots VALUES('19:30:00');
INSERT INTO timeslots VALUES('20:00:00');
INSERT INTO timeslots VALUES('20:30:00');
INSERT INTO timeslots VALUES('21:00:00');
INSERT INTO timeslots VALUES('21:30:00');
INSERT INTO timeslots VALUES('22:00:00');

CREATE TABLE reservations (
	itemno INT CONSTRAINT inventory_fk REFERENCES inventory,
	slot TIME CONSTRAINT timeslots_fk REFERENCES timeslots,
	resdate DATE NOT NULL,
	name VARCHAR(100) NOT NULL,
	quantity INT,
	CONSTRAINT reservations_u UNIQUE(name, resdate));

INSERT INTO reservations VALUES(6, '17:00:00', '2000-07-13', 'Williams', 4);
INSERT INTO reservations VALUES(7, '17:00:00', '2000-07-13', 'Johnson',  4);
INSERT INTO reservations VALUES(8, '17:00:00', '2000-07-13', 'Allen',    3);
INSERT INTO reservations VALUES(9, '17:00:00', '2000-07-13', 'Dexmier',  4);
INSERT INTO reservations VALUES(1, '17:30:00', '2000-07-13', 'Gates', 	 2);
INSERT INTO reservations VALUES(2, '17:30:00', '2000-07-13', 'McNealy',  2);
INSERT INTO reservations VALUES(3, '17:30:00', '2000-07-13', 'Hoffman',  1);
INSERT INTO reservations VALUES(4, '17:30:00', '2000-07-13', 'Sippl',    2);
INSERT INTO reservations VALUES(6, '17:30:00', '2000-07-13', 'Yang',     4);
INSERT INTO reservations VALUES(7, '17:30:00', '2000-07-13', 'Meyers',   4);


-- this query should return values from the 'slot' column (type date)
-- but it seems to be returning integers!
select max(timeslots.slot) from inventory inner join timeslots on inventory.capacity is not null
left outer join reservations on inventory.capacity = reservations.itemno and reservations.slot = timeslots.slot;
rollback;

-- bug 2897 Push join predicates from where clause
-- to right
select * from t1 inner join t2 on 1=1 left outer join t3 on t1.c1 = t3.c1
where t1.c1 = t2.c1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- Test fix for bug 5659
create table xxx (a int not null);
create table yyy (a int not null);

insert into xxx values (1);
select * from xxx left join yyy on (xxx.a=yyy.a);
insert into xxx values (null);
select * from xxx;

drop table xxx;
drop table yyy;

-- Defect 5658. Disable querries with ambiguous references.
create table ttab1 (a int, b int);
insert into ttab1 values (1,1),(2,2);
create table ttab2 (c int, d int);
insert into ttab2 values (1,1),(2,2);

-- this statement should raise an error because 
-- more than one object table includes column "b"
select cor1.*, cor2.* from ttab1 cor1 left outer join ttab2 on (b = d),
		ttab1 left outer join ttab2 cor2 on (b = d);
select cor1.*, cor2.* from ttab1 cor1 left outer join ttab2 on (b = d),
		ttab1 left outer join ttab2 cor2 on (b = cor2.d);

-- This should pass
select cor1.*, cor2.* from ttab1 left outer join ttab2 on (b = d), 
		ttab1 cor1 left outer join ttab2 cor2 on (cor1.b = cor2.d);

-- These should fail too
select * from ttab1, ttab1 left outer join ttab2 on (a=c);
select * from ttab1 cor1, ttab1 left outer join ttab2 on (cor1.a=c);

-- This should pass
select * from ttab1, ttab1 cor1 left outer join ttab2 on (cor1.a=c);

drop table ttab1;
drop table ttab2;

-- Test 5164

CREATE TABLE "APP"."GOVT_AGCY" ("GVA_ID" NUMERIC(20,0) NOT NULL, "GVA_ORL_ID" NUMERIC(20,0) NOT NULL, "GVA_GAC_ID" NUMERIC(20,0));

CREATE TABLE "APP"."GEO_STRC_ELMT" ("GSE_ID" NUMERIC(20,0) NOT NULL, "GSE_GSET_ID" NUMERIC(20,0) NOT NULL, "GSE_GA_ID_PRNT" NUMERIC(20,0) NOT NULL, "GSE_GA_ID_CHLD" NUMERIC(20,0) NOT NULL);

CREATE TABLE "APP"."GEO_AREA" ("GA_ID" NUMERIC(20,0) NOT NULL, "GA_GAT_ID" NUMERIC(20,0) NOT NULL, "GA_NM" VARCHAR(30) NOT NULL, "GA_ABRV_NM" VARCHAR(5));

CREATE TABLE "APP"."REG" ("REG_ID" NUMERIC(20,0) NOT NULL, "REG_NM" VARCHAR(60) NOT NULL, "REG_DESC" VARCHAR(240), "REG_ABRV_NM" VARCHAR(15), "REG_CD" NUMERIC(8,0) NOT NULL, "REG_STRT_DT" TIMESTAMP NOT NULL, "REG_END_DT" TIMESTAMP NOT NULL DEFAULT '4712-12-31 00:00:00', "REG_EMPR_LIAB_IND" CHAR(1) NOT NULL DEFAULT 'N', "REG_PAYR_TAX_SURG_CRTF_IND" CHAR(1) NOT NULL DEFAULT 'N', "REG_PYT_ID" NUMERIC(20,0), "REG_GA_ID" NUMERIC(20,0) NOT NULL, "REG_GVA_ID" NUMERIC(20,0) NOT NULL, "REG_REGT_ID" NUMERIC(20,0) NOT NULL, "REG_PRNT_ID" NUMERIC(20,0));

-- This should not get ArrayIndexOutofBound exception
SELECT 1
FROM reg
     JOIN geo_area jrsd ON (jrsd.ga_id = reg.reg_ga_id)
     LEFT OUTER
JOIN geo_strc_elmt gse ON (gse.gse_ga_id_chld =
reg.reg_ga_id)
     LEFT OUTER
JOIN geo_area prnt ON (prnt.ga_id =
reg.reg_ga_id)
     JOIN govt_agcy gva ON (reg.reg_gva_id = gva.gva_id);

DROP TABLE "APP"."GOVT_AGCY";
DROP TABLE "APP"."GEO_STRC_ELMT";
DROP TABLE "APP"."GEO_AREA";
DROP TABLE "APP"."REG";

-- reset autocommit
autocommit on;

-- drop the tables
drop table t1;
drop table t2;
drop table t3;
drop table tt1;
drop table tt2;
drop table tt3;
drop table insert_test;
drop table empty_table;
drop table a;
drop table b;
drop table c;
drop table oj;
drop table x;
drop table y;
