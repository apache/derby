-- This tests whether we are prevented from creating a container with more
-- than 40k of user data in it in the demo (limited) version of the product.
--
run resource 'createTestProcedures.subsql';

create table limits2 (x int, b varchar(4000));

insert into limits2 values (1, PADSTRING('a test',4000));
insert into limits2 (select limits2.x + 1,  limits2.b from limits2);
insert into limits2 (select limits2.x + 2,  limits2.b from limits2);
insert into limits2 (select limits2.x + 4,  limits2.b from limits2);
insert into limits2 (select limits2.x + 8,  limits2.b from limits2);
insert into limits2 (select limits2.x + 16, limits2.b from limits2);

-- this one should error.
insert into limits2 (select limits2.x + 32, limits2.b from limits2);

-- should still be able to see the data, and be able to add one row
select x from limits2;
insert into limits2 values (33, PADSTRING('a second test',4000));


-- now see if getting an error in the btree causes an error.
-- Make the btree index only allow 2 rows per 4k page, which will cause
-- the btree to grow quicker than the heap base table.
drop table limits2;
create table limits2 (x int, b varchar(1800));
create unique index t_idx on limits2 (x, b);

insert into limits2 values (1, PADSTRING('a test',1800));
insert into limits2 (select limits2.x + 1,  limits2.b from limits2);
insert into limits2 (select limits2.x + 2,  limits2.b from limits2);
insert into limits2 (select limits2.x + 4,  limits2.b from limits2);
insert into limits2 (select limits2.x + 8,  limits2.b from limits2);
insert into limits2 (select limits2.x + 16, limits2.b from limits2);
