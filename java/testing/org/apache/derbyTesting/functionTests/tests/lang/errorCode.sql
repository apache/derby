--
-- this test shows the error code functionality
--

-- specify an invalid driver
driver 'java.lang.Integer';

-- now a valid driver
driver 'org.apache.derby.jdbc.EmbeddedDriver';

-- specify an invalid database
connect 'asdfasdf';

-- now a valid database, but no create
connect 'jdbc:derby:wombat';

-- now a valid database
connect 'jdbc:derby:wombat;create=true';


-- create the table
create table t(i int, s smallint);

-- populate the table
insert into t values (1,2);
insert into t values (null,2);

-- parser error
-- bug 5701
create table t(i nt, s smallint);

-- non-boolean where clause
select * from t where i;

-- invalid correlation name for "*"
select asdf.* from t; 

-- execution time error
select i/0 from t;

-- test ErrorMessages VTI
select * from new org.apache.derby.diag.ErrorMessages() c where sql_state = '07000';
select * from new org.apache.derby.diag.ErrorMessages() c where sql_state = '22012';
select * from new org.apache.derby.diag.ErrorMessages() c where sql_state = '42X74';

-- cleanup
drop table t;
