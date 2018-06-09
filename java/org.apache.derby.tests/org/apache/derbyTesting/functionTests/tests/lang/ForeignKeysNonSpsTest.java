/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.lang.ForeignKeysNonSpsTest

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package org.apache.derbyTesting.functionTests.tests.lang;

import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import junit.framework.Test;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Test of foreign key constraints. Converted from the old harness test
 * lang/fk_nonSPS.sql
 */
public final class ForeignKeysNonSpsTest extends BaseJDBCTestCase {

    private static final int WAIT_TIMEOUT_DURATION = 4;

    /**
     * Public constructor required for running test as standalone JUnit.
     * @param name test name
     */
    public ForeignKeysNonSpsTest (String name) {
        super(name);
    }

    /**
     * JUnit handle
     * @return this JUnit test
     */
    public static Test suite() {
        return TestConfiguration.defaultSuite(ForeignKeysNonSpsTest.class);
    }

    public void testForeignKeys() throws Exception {
        ResultSet rs;
        PreparedStatement pSt;
        String [][] expRS;
        final Statement st = createStatement();
        final int initialCardSysDepends = numberOfRowsInSysdepends(st);

        st.executeUpdate(
            "CREATE PROCEDURE WAIT_FOR_POST_COMMIT() DYNAMIC "
            + "RESULT SETS 0 LANGUAGE JAVA EXTERNAL NAME "
            + "'org.apache.derbyTesting.functionTests.util.T_Access"
            + ".waitForPostCommitToFinish' PARAMETER STYLE JAVA");

        st.executeUpdate(
            " create table p (c1 char(1), y int not null, c2 "
            + "char(1), x int not null, constraint pk primary key (x,y))");

        st.executeUpdate(
            " create table f (x int not null, s smallint, y int "
            + "not null, constraint fk foreign key (x,y) references p)");

        st.executeUpdate("insert into p values ('1',1,'1',1)");

        // should pass, foreign key constraint satisfied
        st.executeUpdate(
            "insert into f "
            + "values "
            + "	(1,1,1),"
            + "	(1,1,1),"
            + "	(1,1,1),	"
            + "	(1,1,1),"
            + "	(1, 0, 1),"
            + "	(1,1,1),"
            + "	(1,0,1),"
            + "	(1, 0, 1)");

        // should FAIL, foreign key constraint violated
        assertUpdateCount(st, 8, "delete from f");

        assertStatementError("23503", st,
            " insert into f "
            + "values "
            + "	(1,1,1),"
            + "	(1,1,1),"
            + "	(1,1,1),	"
            + "	(1,1,1),"
            + "	(1, 1, 1),"
            + "	(2,1,666),"
            + "	(1,1,0),"
            + "	(0, 1, 0)");

        st.executeUpdate("drop table f");

        waitForPostCommit();

        // make sure boundary conditions are ok, null insert set
        st.executeUpdate(
            "create table f (c1 char(1), y int, c2 char(1), x "
            + "int, constraint fk foreign key (x,y) references p)");

        st.executeUpdate("insert into f select * from p where 1 = 2");

        st.executeUpdate("drop table f");
        st.executeUpdate("drop table p");

        waitForPostCommit();

        // self referencing
        st.executeUpdate(
            "create table s (x int not null primary key, y int "
            + "references s, z int references s)");

        // ok
        st.executeUpdate(
            "insert into s "
            + "values "
            + "	(1,1,1),"
            + "	(2,1,1),"
            + "	(10,2,1),	"
            + "	(11,1,2),"
            + "	(12,4,4),"
            + "	(4,1,1),"
            + "	(13,null,null),"
            + "	(14,1,2),"
            + "	(15,null, 1)");

        assertUpdateCount(st, 9, "delete from s");

        // bad
        assertStatementError("23503", st,
            "insert into s "
            + "values "
            + "	(1,1,1),"
            + "	(2,1,1),"
            + "	(10,2,1),	"
            + "	(11,1,2),"
            + "	(12,4,4),"
            + "	(4,1,1),"
            + "	(13,null,null),"
            + "	(14,1,2),"
            + "	(15,666, 1)");

        // now a test for depenencies. the insert will create new
        // index conglomerate numbers, so we want to test that a
        // statement with a constraint check that is dependent on
        // the conglomerate number that is being changed is invalidated
        st.executeUpdate(
            "create table x (x int not null, y int, constraint "
            + "pk primary key (x))");

        st.executeUpdate(
            " create table y (x int , y int, constraint fk "
            + "foreign key (x) references x)");

        final PreparedStatement pStIx = prepareStatement(
            "insert into x	values" +
            "(0,0)," +
            "(1,1)," +
            "(2,2)");

        final PreparedStatement pStIx2 = prepareStatement(
            "insert into x values" +
            "(3,3),"+
            "(4,4)");

        final PreparedStatement pStIy = prepareStatement(
            "insert into y values" +
            "(0,0)," +
            "(1,1)," +
            "(2,2)");

        final PreparedStatement dy = prepareStatement(
            "delete from y where x = 1");

        final PreparedStatement dx = prepareStatement(
            "delete from x where x = 1");

        assertUpdateCount(pStIx, 3);

        setAutoCommit(false);

        commit();

        // ok
        assertUpdateCount(dy, 0);
        assertUpdateCount(dx, 1);

        // will fail, no key 1 in x
        assertStatementError("23503", pStIy);

        rollback();

        assertUpdateCount(pStIy, 3);
        assertUpdateCount(dy, 1);
        assertUpdateCount(dx, 1);

        pStIx.close();
        pStIx2.close();
        pStIy.close();
        dy.close();
        dx.close();

        st.executeUpdate("drop table y");
        st.executeUpdate("drop table x");
        st.executeUpdate("drop table s");

        setAutoCommit(true);

        waitForPostCommit();

        // ** insert fkddl.sql simple syntax checks column constraint
        st.executeUpdate(
            "create table p1 (x int not null, constraint pk1 "
            + "primary key(x))");

        st.executeUpdate(
            " create table u1 (x int not null unique)");

        // table constraint
        st.executeUpdate(
            "create table p2 (x int not null, y dec(5,2) not "
            + "null, constraint pk2 primary key (x,y))");

        st.executeUpdate(
            " create table u2 (x int not null, y dec(5,2) not "
            + "null, constraint uk2 unique (x,y))");

        st.executeUpdate(
            " create table p3 (x char(10) not null, constraint "
            + "pk3 primary key (x))");

        // for future use
        st.executeUpdate("create schema otherschema");

        st.executeUpdate(
            " create table otherschema.p1 (x int not null primary key)");

        // Negative test cases for foreign key TABLE constraints
        // negative: fk table, no table
        assertStatementError("X0Y46", st,
            "create table f (x int, constraint fk foreign key "
            + "(x) references notthere)");

        // negative: fk table, bad column
        assertStatementError("X0Y44", st,
            "create table f (x int, constraint fk foreign key "
            + "(x) references p1(notthere))");

        // negative: fk table, no constraint
        assertStatementError("X0Y44", st,
            "create table f (x int, constraint fk foreign key "
            + "(x) references p2(y))");

        // negative: fk table, wrong type
        assertStatementError("X0Y44", st,
            "create table f (x smallint, constraint fk foreign "
            + "key (x) references p1(x))");

        // negative: cannot reference a system table
        assertStatementError("42Y08", st,
            "create table f (x char(36), constraint fk foreign "
            + "key (x) references sys.sysforeignkeys(constraintid))");

        // negative: bad schema
        assertStatementError("42Y07", st,
            "create table f (x char(36), constraint fk foreign "
            + "key (x) references badschema.x)");

        // negative: bad column list
        assertStatementError("42X93", st,
            "create table f (x dec(5,2), y int, constraint fk "
            + "foreign key (x,z) references p2(x,y))");

        // negative: wrong number of columns
        assertStatementError("X0Y44", st,
            "create table f (x dec(5,2), y int, constraint fk "
            + "foreign key (x) references p2(x,y))");

        assertStatementError("X0Y44", st,
            " create table f (x dec(5,2), y int, constraint fk "
            + "foreign key (x,y) references p2(x))");

        // Negative test cases for foreign key COLUMN constraints
        // negative: fk column, no table
        assertStatementError("X0Y46", st,
            "create table f (x int references notthere)");

        // negative: fk column, bad column
        assertStatementError("X0Y44", st,
            "create table f (x int references p1(notthere))");

        // negative: fk column, no constraint
        assertStatementError("X0Y44", st,
            "create table f (x int references p2(y))");

        // negative: fk column, wrong type
        assertStatementError("X0Y44", st,
            "create table f (x smallint references p1(x))");

        // negative: cannot reference a system table
        assertStatementError("42Y08", st,
            "create table f (x char(36) references "
            + "sys.sysforeignkeys(constraintid))");

        // negative: bad schema
        assertStatementError("42Y07", st,
            "create table f (x char(36) references badschema.x)");

        // Some type checks.  Types must match exactly ok
        st.executeUpdate(
            "create table f (d dec(5,2), i int, constraint fk "
            + "foreign key (i,d) references p2(x,y))");

        st.executeUpdate(
            " drop table f");

        waitForPostCommit();

        st.executeUpdate(
            " create table f (i int, d dec(5,2), constraint fk "
            + "foreign key (i,d) references p2(x,y))");

        st.executeUpdate("drop table f");

        waitForPostCommit();

        st.executeUpdate(
            " create table f (d dec(5,2), i int, constraint fk "
            + "foreign key (i,d) references u2(x,y))");

        st.executeUpdate("drop table f");

        waitForPostCommit();

        st.executeUpdate(
            " create table f (i int, d dec(5,2), constraint fk "
            + "foreign key (i,d) references u2(x,y))");

        st.executeUpdate("drop table f");

        waitForPostCommit();

        st.executeUpdate(
            " create table f (c char(10) references p3(x))");

        st.executeUpdate("drop table f");

        waitForPostCommit();

        // type mismatch
        assertStatementError("X0Y44", st,
            "create table f (i int, d dec(5,1), constraint fk "
            + "foreign key (i,d) references p2(x,y))");

        assertStatementError("X0Y44", st,
            " create table f (i int, d dec(4,2), constraint fk "
            + "foreign key (i,d) references p2(x,y))");

        assertStatementError("X0Y44", st,
            " create table f (i int, d dec(4,2), constraint fk "
            + "foreign key (i,d) references p2(x,y))");

        assertStatementError("X0Y44", st,
            " create table f (i int, d numeric(5,2), constraint "
            + "fk foreign key (i,d) references p2(x,y))");

        assertStatementError("X0Y44", st,
            " create table f (c char(11) references p3(x))");

        assertStatementError("X0Y44", st,
            " create table f (c varchar(10) references p3(x))");

        // wrong order
        assertStatementError("X0Y44", st,
            "create table f (d dec(5,2), i int, constraint fk "
            + "foreign key (d,i) references p2(x,y))");

        // check system tables
        st.executeUpdate(
            "create table f (x int, constraint fk foreign key "
            + "(x) references p1)");

        rs = st.executeQuery(
            " select constraintname, referencecount "
            + "	from sys.sysconstraints c, sys.sysforeignkeys fk"
            + "	where fk.keyconstraintid = c.constraintid order by "
            + "constraintname");

        expRS = new String [][]{{"PK1", "1"}};

        JDBC.assertFullResultSet(rs, expRS, true);

        st.executeUpdate(
            " create table f2 (x int, constraint fk2 foreign key "
            + "(x) references p1(x))");

        st.executeUpdate(
            " create table f3 (x int, constraint fk3 foreign key "
            + "(x) references p1(x))");

        st.executeUpdate(
            " create table f4 (x int, constraint fk4 foreign key "
            + "(x) references p1(x))");

        rs = st.executeQuery(
            " select distinct constraintname, referencecount "
            + "	from sys.sysconstraints c, sys.sysforeignkeys fk"
            + "	where fk.keyconstraintid = c.constraintid order by "
            + "constraintname");

        expRS = new String [][]{{"PK1", "4"}};

        JDBC.assertFullResultSet(rs, expRS, true);

        rs = st.executeQuery(
            " select constraintname "
            + "	from sys.sysconstraints c, sys.sysforeignkeys fk"
            + "	where fk.constraintid = c.constraintid"
            + "	order by 1");

        expRS = new String [][]
        {
            {"FK"},
            {"FK2"},
            {"FK3"},
            {"FK4"}
        };

        JDBC.assertFullResultSet(rs, expRS, true);

        // we should not be able to drop the primary key
        assertStatementError("X0Y25", st,
            "alter table p1 drop constraint pk1");

        assertStatementError("X0Y25", st, "drop table p1");

        waitForPostCommit();

        // now lets drop the foreign keys and try again
        st.executeUpdate("drop table f2");

        st.executeUpdate("drop table f3");

        st.executeUpdate("drop table f4");

        waitForPostCommit();

        rs = st.executeQuery(
            " select constraintname, referencecount "
            + "	from sys.sysconstraints c, sys.sysforeignkeys fk"
            + "	where fk.keyconstraintid = c.constraintid order by "
            + "constraintname");

        expRS = new String [][]{{"PK1", "1"}};

        JDBC.assertFullResultSet(rs, expRS, true);

        st.executeUpdate(" alter table f drop constraint fk");

        waitForPostCommit();

        // ok
        st.executeUpdate("alter table p1 drop constraint pk1");

        waitForPostCommit();

        // we shouldn't be able to add an fk on p1 now
        assertStatementError("X0Y41", st,
            "alter table f add constraint fk foreign key (x) "
            + "references p1");

        // add the constraint and try again
        st.executeUpdate(
            "alter table p1 add constraint pk1 primary key (x)");

        st.executeUpdate(
            " create table f2 (x int, constraint fk2 foreign key "
            + "(x) references p1(x))");

        st.executeUpdate(
            " create table f3 (x int, constraint fk3 foreign key "
            + "(x) references p1(x))");

        st.executeUpdate(
            " create table f4 (x int, constraint fk4 foreign key "
            + "(x) references p1(x))");

        // drop constraint
        st.executeUpdate("alter table f4 drop constraint fk4");
        st.executeUpdate("alter table f3 drop constraint fk3");
        st.executeUpdate("alter table f2 drop constraint fk2");
        st.executeUpdate("alter table p1 drop constraint pk1");

        waitForPostCommit();

        // all fks are gone, right?
        rs = st.executeQuery(
            "select constraintname "
            + "	from sys.sysconstraints c, sys.sysforeignkeys fk"
            + "	where fk.constraintid = c.constraintid order by "
            + "constraintname");

        JDBC.assertDrainResults(rs, 0);

        // cleanup what we have done so far
        st.executeUpdate("drop table p1");
        st.executeUpdate("drop table p2");
        st.executeUpdate("drop table u1");
        st.executeUpdate("drop table u2");
        st.executeUpdate("drop table otherschema.p1");
        st.executeUpdate("drop schema otherschema restrict");

        waitForPostCommit();

        // will return dependencies for SPS metadata queries now
        // created by default database is created.
        st.executeUpdate(
            "create table default_sysdepends_count(a int)");

        st.executeUpdate(
            " insert into default_sysdepends_count select "
            + "count(*) from sys.sysdepends");

        rs = st.executeQuery(
            " select * from default_sysdepends_count");

        expRS = new String [][]{{Integer.toString(initialCardSysDepends)}};

        JDBC.assertFullResultSet(rs, expRS, true);

        // now we are going to do some self referencing tests.
        st.executeUpdate(
            "create table selfref (p char(10) not null primary key, "
            + "		f char(10) references selfref)");

        st.executeUpdate("drop table selfref");

        waitForPostCommit();

        // ok
        st.executeUpdate(
            "create table selfref (p char(10) not null, "
            + "		f char(10) references selfref, "
            + "		constraint pk primary key (p))");

        st.executeUpdate("drop table selfref");

        waitForPostCommit();

        // ok
        st.executeUpdate(
            "create table selfref (p char(10) not null, f char(10), "
            + "		constraint f foreign key (f) references selfref(p), "
            + "		constraint pk primary key (p))");

        // should fail
        assertStatementError("X0Y25", st,
            "alter table selfref drop constraint pk");

        waitForPostCommit();

        // ok
        st.executeUpdate(
            "alter table selfref drop constraint f");

        st.executeUpdate(
            " alter table selfref drop constraint pk");

        st.executeUpdate("drop table selfref");

        waitForPostCommit();

        // what if a pk references another pk?  should just drop
        // the direct references (nothing special, really)
        st.executeUpdate(
            "create table pr1(x int not null, "
            + "		constraint pkr1 primary key (x))");

        st.executeUpdate(
            " create table pr2(x int not null, "
            + "		constraint pkr2 primary key(x), "
            + "		constraint fpkr2 foreign key (x) references pr1)");

        st.executeUpdate(
            " create table pr3(x int not null, "
            + "		constraint pkr3 primary key(x), "
            + "		constraint fpkr3 foreign key (x) references pr2)");

        rs = st.executeQuery(
            " select constraintname, referencecount from "
            + "sys.sysconstraints order by constraintname");

        expRS = new String [][]
        {
            {"FPKR2", "0"},
            {"FPKR3", "0"},
            {"PK3", "0"},
            {"PKR1", "1"},
            {"PKR2", "1"},
            {"PKR3", "0"}
        };

        JDBC.assertFullResultSet(rs, expRS, true);

        // now drop constraint pkr1
        st.executeUpdate(
            "alter table pr2 drop constraint fpkr2");

        st.executeUpdate(
            " alter table pr1 drop constraint pkr1");

        waitForPostCommit();

        // pkr1 and pfkr2 are gone
        rs = st.executeQuery(
            "select constraintname, referencecount from "
            + "sys.sysconstraints order by constraintname");

        expRS = new String [][]
        {
            {"FPKR3", "0"},
            {"PK3", "0"},
            {"PKR2", "1"},
            {"PKR3", "0"}
        };

        JDBC.assertFullResultSet(rs, expRS, true);

        // cleanup
        st.executeUpdate(
            "drop table pr3");

        st.executeUpdate("drop table pr2");
        st.executeUpdate("drop table pr1");

        waitForPostCommit();

        // should return 0, confirm no unexpected dependencies
        // verify that all rows in sys.sysdepends got dropped
        // apart from sps dependencies
        st.executeUpdate(
            "create table default_sysdepends_count2(a int)");

        st.executeUpdate(
            " insert into default_sysdepends_count2 select "
            + "count(*) from sys.sysdepends");

        rs = st.executeQuery(
            " select default_sysdepends_count2.a - "
            + "default_sysdepends_count.a"
            + "    from default_sysdepends_count2, default_sysdepends_count");

        expRS = new String [][]{{"0"}};

        JDBC.assertFullResultSet(rs, expRS, true);

        // dependencies and spses
        st.executeUpdate(
            "create table x (x int not null primary key, y int, "
            + "constraint xfk foreign key (y) references x)");

        st.executeUpdate(
            " create table y (x int, constraint yfk foreign key "
            + "(x) references x)");

        final PreparedStatement ss = prepareStatement(
            "select * from x");

        final PreparedStatement si = prepareStatement(
            "insert into x values (1,1)");

        final PreparedStatement su = prepareStatement(
            "update x set x = x+1, y=y+1");

        st.executeUpdate(
            " alter table x drop constraint xfk");

        waitForPostCommit();

        setAutoCommit(false);

        // drop the referenced fk, should force su to be
        // recompiled since it no longer has to check the foreign
        // key table
        st.executeUpdate(
            "alter table y drop constraint yfk");

        commit();
        waitForPostCommit();

        st.executeUpdate("drop table y");

        commit();

        waitForPostCommit();

        // ok
        st.executeUpdate("drop table x");

        ss.close();
        si.close();
        su.close();

        st.executeUpdate("drop table f3");
        st.executeUpdate("drop table f2");
        st.executeUpdate("drop table f");

        commit();
        waitForPostCommit();

        // verify that all rows in sys.sysdepends got dropped
        // apart from sps dependencies Since, with beetle 5352; we
        // create metadata SPS for network server at database
        // bootup time so the dependencies for SPS are there.
        st.executeUpdate(
            "create table default_sysdepends_count3(a int)");

        st.executeUpdate(
            " insert into default_sysdepends_count3 select "
            + "count(*) from sys.sysdepends");

        rs = st.executeQuery(
            " select default_sysdepends_count3.a - "
            + "default_sysdepends_count.a"
            + "    from default_sysdepends_count3, default_sysdepends_count");

        expRS = new String [][]{{"0"}};

        JDBC.assertFullResultSet(rs, expRS, true);

        // ** insert fkdml.sql
        setAutoCommit(true);

        // DML and foreign keys
        assertStatementError("42Y55", st, "drop table s");
        assertStatementError("42Y55", st, "drop table f3");
        assertStatementError("42Y55", st, "drop table f2");
        assertStatementError("42Y55", st, "drop table f");
        assertStatementError("42Y55", st, "drop table p");

        waitForPostCommit();

        st.executeUpdate(
            " create table p (x int not null, y int not null, "
            + "constraint pk primary key (x,y))");

        st.executeUpdate(
            " create table f (x int, y int, constraint fk "
            + "foreign key (x,y) references p)");

        st.executeUpdate(
            " insert into p values (1,1)");

        // ok
        st.executeUpdate("insert into f values (1,1)");

        // fail
        assertStatementError("23503", st, "insert into f values (2,1)");
        assertStatementError("23503", st, " insert into f values (1,2)");

        // nulls are ok
        st.executeUpdate("insert into f values (1,null)");
        st.executeUpdate("insert into f values (null,null)");
        st.executeUpdate("insert into f values (1,null)");

        // update on pk, fail
        assertStatementError("23503", st, "update p set x = 2");
        assertStatementError("23503", st, "update p set y = 2");
        assertStatementError("23503", st, "update p set x = 1, y = 2");
        assertStatementError("23503", st, "update p set x = 2, y = 1");
        assertStatementError("23503", st, "update p set x = 2, y = 2");

        // ok
        assertUpdateCount(st, 1, "update p set x = 1, y = 1");

        // delete pk, fail
        assertStatementError("23503", st, "delete from p");

        // delete fk, ok
        assertUpdateCount(st, 4, "delete from f");

        st.executeUpdate("insert into f values (1,1)");

        // update fk, fail
        assertStatementError("23503", st, "update f set x = 2");
        assertStatementError("23503", st, "update f set y = 2");
        assertStatementError("23503", st, "update f set x = 1, y = 2");
        assertStatementError("23503", st, "update f set x = 2, y = 1");

        // update fk, ok
        assertUpdateCount(st, 1, "update f set x = 1, y = 1");

        // nulls ok
        assertUpdateCount(st, 1, "update f set x = null, y = 1");
        assertUpdateCount(st, 1, "update f set x = 1, y = null");
        assertUpdateCount(st, 1, "update f set x = null, y = null");
        assertUpdateCount(st, 1, "delete from f");

        st.executeUpdate("insert into f values (1,1)");
        st.executeUpdate("insert into p values (2,2)");

        // ok
        assertUpdateCount(st, 1, "update f set x = x+1, y = y+1");

        rs = st.executeQuery("select * from f");

        expRS = new String [][]{{"2", "2"}};

        JDBC.assertFullResultSet(rs, expRS, true);

        rs = st.executeQuery("select * from p");

        expRS = new String [][]
        {
            {"1", "1"},
            {"2", "2"}
        };

        JDBC.assertFullResultSet(rs, expRS, true);

        // ok
        assertUpdateCount(st, 2, "update p set x = x+1, y = y+1");

        // fail
        assertStatementError("23503", st, "update p set x = x+1, y = y+1");

        // BOUNDARY CONDITIONS
        assertUpdateCount(st, 1, "delete from f");
        assertUpdateCount(st, 2, "delete from p");

        st.executeUpdate("insert into f select * from f");

        assertUpdateCount(st, 0, "delete from p where x = 9999");
        assertUpdateCount(st, 0, "update p set x = x+1, y=y+1 where x = 999");

        st.executeUpdate("insert into p values (1,1)");
        st.executeUpdate("insert into f values (1,1)");

        assertUpdateCount(st, 0, "update p set x = x+1, y=y+1 where x = 999");
        assertUpdateCount(st, 0, "delete from p where x = 9999");

        st.executeUpdate("insert into f select * from f");

        // test a CURSOR
        assertUpdateCount(st, 2, "delete from f");
        assertUpdateCount(st, 1, "delete from p");

        st.executeUpdate("insert into p values (1,1)");
        st.executeUpdate("insert into f values (1,1)");

        setAutoCommit(false);

        final Statement uS = createStatement(
            ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
        ResultSet r = uS.executeQuery("select * from p for update of x");
        r.next();
        assertEquals(r.getString(1), "1");
        assertEquals(r.getString(2), "1");

        try {
            // UPDATE on table 'P' caused a violation of foreign
            // key constraint 'FK' for key (1,1).
            r.updateInt("X", 666);
        } catch (SQLException e) {
            assertSQLState("23503", e);
        }

        r.close();

        r = uS.executeQuery("select * from f for update of x");
        r.next();
        assertEquals(r.getString(1), "1");
        assertEquals(r.getString(2), "1");

        try {
            // UPDATE on table 'F' caused a violation of foreign
            // key constraint 'FK' for key (666,1).
            r.updateInt("X", 666);
        } catch (SQLException e) {
            assertSQLState("23503", e);
        }

        r.close();

        commit();
        setAutoCommit(true);

        assertUpdateCount(st, 1, "delete from f");
        assertUpdateCount(st, 1, "delete from p");

        st.executeUpdate("insert into p values (0,0), (1,1), (2,2), (3,3), (4,4)");
        st.executeUpdate("insert into f values (1,1)");

        // lets add some additional foreign keys to the mix
        st.executeUpdate(
            "create table f2 (x int, y int, constraint fk2 "
            + "foreign key (x,y) references p)");

        st.executeUpdate("insert into f2 values (2,2)");

        st.executeUpdate(
            " create table f3 (x int, y int, constraint fk3 "
            + "foreign key (x,y) references p)");

        st.executeUpdate("insert into f3 values (3,3)");

        // ok
        assertUpdateCount(st, 5, "update p set x = x+1, y = y+1");

        // error, fk1
        assertStatementError("23503", st, "update p set x = x+1");
        assertStatementError("23503", st, "update p set y = y+1");
        assertStatementError("23503", st, "update p set x = x+1, y = y+1");

        // fail of fk3
        assertStatementError("23503", st, "update p set y = 666 where y = 3");

        // fail of fk2
        assertStatementError("23503", st, "update p set x = 666 where x = 2");

        // cleanup
        st.executeUpdate("drop table f");
        st.executeUpdate("drop table f2");
        st.executeUpdate("drop table f3");
        st.executeUpdate("drop table p");

        waitForPostCommit();

        // SELF REFERENCING
        st.executeUpdate(
            "create table s (x int not null primary key, y int "
            + "references s, z int references s)");

        // ok
        st.executeUpdate("insert into s values (1,null,null)");

        // ok
        assertUpdateCount(st, 1, "update s set y = 1");

        // fail
        assertStatementError("23503", st, "update s set z = 2");

        // ok
        assertUpdateCount(st, 1, "update s set z = 1");

        // ok
        st.executeUpdate("insert into s values (2, 1, 1)");

        // ok
        assertUpdateCount(st, 1, "update s set x = 666 where x = 2");

        // ok
        assertUpdateCount(st, 2, "update s set x = x+1, y = y+1, z = z+1");
        assertUpdateCount(st, 2, "delete from s");

        // ok
        st.executeUpdate("insert into s values (1,null,null)");
        st.executeUpdate("insert into s values (2,null,null)");
        assertUpdateCount(st, 1, "update s set y = 2 where x = 1");
        assertUpdateCount(st, 1, "update s set z = 1 where x = 2");

        rs = st.executeQuery("select * from s");

        expRS = new String [][]
        {
            {"1", "2", null},
            {"2", null, "1"}
        };

        JDBC.assertFullResultSet(rs, expRS, true);

        // fail
        assertStatementError("23503", st, "update s set x = 0 where x = 1");

        // Now we are going to do a short but sweet check to make
        // sure we are actually hitting the correct columns
        st.executeUpdate(
            "create table p (c1 char(1), y int not null, c2 "
            + "char(1), x int not null, constraint pk primary key (x,y))");

        st.executeUpdate(
            " create table f (x int, s smallint, y int, "
            + "constraint fk foreign key (x,y) references p)");

        st.executeUpdate(
            " insert into p values ('1',1,'1',1)");

        // ok
        st.executeUpdate("insert into f values (1,1,1)");
        st.executeUpdate("insert into p values ('0',0,'0',0)");

        // ok
        assertUpdateCount(st, 2, "update p set x = x+1, y=y+1");

        // fail
        assertStatementError("23503", st, "delete from p where y = 1");
        assertStatementError("23503", st, "insert into f values (1,1,4)");

        assertUpdateCount(st, 1, "delete from f");
        assertUpdateCount(st, 2, "delete from p");

        // Lets make sure we don't interact poorly with 'normal'
        // deferred dml
        st.executeUpdate("insert into p values ('1',1,'1',1)");
        st.executeUpdate("insert into f values (1,1,1)");
        st.executeUpdate("insert into p values ('0',0,'0',0)");

        // ok
        assertUpdateCount(st, 2,
            "update p set x = x+1, y=y+1 where x < (select "
            + "max(x)+10000 from p)");

        // fail
        assertStatementError("23503", st,
            "delete from p where y = 1 and y in (select y from p)");

        // inserts
        st.executeUpdate(
            "create table f2 (x int, t smallint, y int)");

        st.executeUpdate("insert into f2 values (1,1,4)");

        // fail
        assertStatementError("23503", st,"insert into f select * from f2");

        // ok
        st.executeUpdate("insert into f2 values (1,1,1)");
        st.executeUpdate("insert into f select * from f2 where y = 1");

        st.executeUpdate("drop table f2");
        st.executeUpdate("drop table f");
        st.executeUpdate("drop table p");

        waitForPostCommit();

        // PREPARED STATEMENTS
        assertStatementError("42Y55", st, "drop table f");
        assertStatementError("42Y55", st, "drop table p");

        //the reason for this wait call is to wait unitil system
        // tables row deletesare completed other wise we will get
        // different order fk checksthat will lead different error
        // messages depending on when post commit thread runs
        waitForPostCommit();

        pSt = prepareStatement(
            "create table p (w int not null primary key, x int "
            + "references p, y int not null, z int not null, "
            + "constraint uyz unique (y,z))");

        assertUpdateCount(pSt, 0);


        pSt = prepareStatement(
            "create table f (w int references p, x int, y int, z "
            + "int, constraint fk foreign key (y,z) references p (y,z))");

        assertUpdateCount(pSt, 0);


        pSt = prepareStatement(
            "alter table f drop constraint fk");

        assertUpdateCount(pSt, 0);


        //the reason for this wait call is to wait unitil system
        // tables row deletesare completed other wise we will get
        // different order fk checks
        waitForPostCommit();

        pSt = prepareStatement(
            "alter table f add constraint fk foreign key (y,z) "
            + "references p (y,z)");

        assertUpdateCount(pSt, 0);


        PreparedStatement sf = prepareStatement(
            "insert into f values (1,1,1,1)");

        PreparedStatement sp = prepareStatement(
            "insert into p values (1,1,1,1)");

        // fail
        assertStatementError("23503", sf);

        // ok
        assertUpdateCount(sp, 1);
        assertUpdateCount(sf, 1);

        st.executeUpdate(" insert into p values (2,2,2,2)");


        pSt = prepareStatement(
            "update f set w=w+1, x = x+1, y=y+1, z=z+1");

        // ok
        assertUpdateCount(pSt, 1);

        pSt = prepareStatement("update p set w=w+1, x = x+1, y=y+1, z=z+1");

        // ok
        assertUpdateCount(pSt, 2);

        pSt = prepareStatement("delete from p where x =1");

        // ok
        assertUpdateCount(pSt, 0);

        st.executeUpdate("drop table f");
        st.executeUpdate("drop table p");

        waitForPostCommit();

        st.executeUpdate("drop procedure WAIT_FOR_POST_COMMIT");

        rollback();
        st.close();
    }

    /**
     * Get a count of number of rows in SYS.SYSDEPENDS
     */
    private int numberOfRowsInSysdepends(Statement st) throws SQLException {
    	final ResultSet rs = 
            st.executeQuery("SELECT COUNT(*) FROM SYS.SYSDEPENDS");
    	rs.next();
        final int result = rs.getInt(1);
    	rs.close();
        return result;
    }

    private void waitForPostCommit() throws SQLException {
        final CallableStatement s = 
            prepareCall("CALL WAIT_FOR_POST_COMMIT()");
        assertUpdateCount(s, 0);
        s.close();
    }
}
