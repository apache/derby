/*
Derby - Class org.apache.derbyTesting.functionTests.tests.lang.AlterTableTest

Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
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

import java.io.InputStream;
import java.sql.Blob;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLWarning;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.Arrays;
import junit.framework.Test;
import junit.framework.TestSuite;
import org.apache.derbyTesting.functionTests.util.TestInputStream;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.TestConfiguration;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.DerbyConstants;

import junit.framework.Assert;

public final class AlterTableTest extends BaseJDBCTestCase {

    ResultSet rs = null;
    ResultSetMetaData rsmd;
    DatabaseMetaData dbmd;
    SQLWarning sqlWarn = null;
    PreparedStatement pSt;
    CallableStatement cSt;
    //Statement st;
    Connection conn;
    String[][] expRS;
    String[] expColNames;

    /**
     * Public constructor required for running test as standalone JUnit.
     */
    public AlterTableTest(String name) {
        super(name);
    }

    public static Test suite() {
        TestSuite suite = new TestSuite("AlterTableTest Test");
        suite.addTest(TestConfiguration.defaultSuite(AlterTableTest.class));
        return TestConfiguration.sqlAuthorizationDecorator(suite);
    }

    private void createTestObjects(Statement st) throws SQLException {
        conn = getConnection();
        conn.setAutoCommit(false);
        CleanDatabaseTestSetup.cleanDatabase(conn, false);

        st.executeUpdate(
                "create table t0(c1 int not null constraint p1 primary key)");

        st.executeUpdate("create table t0_1(c1 int)");
        st.executeUpdate("create table t0_2(c1 int)");
        st.executeUpdate("create table t0_3(c1 int)");
        st.executeUpdate("create table t1(c1 int)");
        st.executeUpdate("create table t1_1(c1 int)");
        st.executeUpdate("create table t2(c1 int)");
        st.executeUpdate("create table t3(c1 int)");
        st.executeUpdate("create table t4(c1 int not null)");
        st.executeUpdate("create view v1 as select * from t2");
        st.executeUpdate("create view v2 as select c1 from t2");
        st.executeUpdate("create index i0_1 on t0_1(c1)");
        st.executeUpdate("create index i0_2 on t0_2(c1)");

        // do some population

        st.executeUpdate("insert into t1 values 1");
        st.executeUpdate("insert into t1_1 values 1");
        st.executeUpdate("insert into t2 values 1");
        st.executeUpdate("insert into t2 values 2");
        st.executeUpdate("insert into t3 values 1");
        st.executeUpdate("insert into t3 values 2");
        st.executeUpdate("insert into t3 values 3");
        st.executeUpdate("insert into t4 values 1, 2, 3, 1");
        st.executeUpdate("create schema emptyschema");
    }

    private void checkWarning(Statement st, String expectedWarning)
            throws Exception {
        if ((sqlWarn == null) && (st != null)) {
            sqlWarn = st.getWarnings();
        }
        if (sqlWarn == null) {
            sqlWarn = getConnection().getWarnings();
        }
        assertNotNull("Expected warning but found none", sqlWarn);
        assertSQLState(expectedWarning, sqlWarn);
        sqlWarn = null;
    }

    public void testAddColumn() throws Exception {
        Statement st = createStatement();
        createTestObjects(st);
        conn.commit();

        // add column negative tests alter a non-existing table
        assertStatementError("42Y55", st,
                "alter table notexists add column c1 int");

        // add a column that already exists
        assertStatementError("X0Y32", st,
                "alter table t0 add column c1 int");

        // add a column without a datatype (DERBY-5160)
        assertStatementError("42XA9", st,
                "alter table t0 add column y");

        // alter a system table
        assertStatementError("42X62", st,
                "alter table sys.systables add column c1 int");

        // alter table on a view
        assertStatementError("42Y62", st,
                "alter table v2 add column c2 int");

        // add a primary key column to a table which already has 
        // one this will produce an error
        assertStatementError("X0Y58", st,
                "alter table t0 add column c2 int not null default 0 " +
                "primary key");

        // add a unique column constraint to a table with > 1 row
        assertStatementError("23505", st,
                "alter table t3 add column c2 int not null default 0 " +
                "unique");

        // cannot alter a table when there is an open cursor on it

        PreparedStatement ps_c1 = prepareStatement("select * from t1");

        ResultSet c1 = ps_c1.executeQuery();
        if (usingEmbedded()) // client/server doesn't keep cursor open.
        {
            assertStatementError("X0X95", st,
                    " alter table t1 add column c2 int");
        }
        c1.close();
        ps_c1.close();

        // positive tests add a non-nullable column to a non-empty table
        st.executeUpdate(
                "alter table t1 add column c2 int not null default 0");

        // add a primary key column to a non-empty table
        st.executeUpdate(
                "alter table t1 add column c3 int not null default 0 " +
                "primary key");

        // add a column with a check constraint to a non-empty column
        st.executeUpdate("alter table t1 add column c4 int check(c4 = 1)");

        // Newly-added column does not appear in existing view:
        rs = st.executeQuery("select * from v1");
        JDBC.assertColumnNames(rs, new String[]{"C1"});
        JDBC.assertFullResultSet(rs, new String[][]{{"1"}, {"2"}});

        pSt = prepareStatement("select * from t2");

        rs = pSt.executeQuery();
        JDBC.assertColumnNames(rs, new String[]{"C1"});
        JDBC.assertFullResultSet(rs, new String[][]{{"1"}, {"2"}});

        st.executeUpdate("alter table t2 add column c2 int");

        // select * views don't see added columns after alter table

        rs = st.executeQuery("select * from v1");
        JDBC.assertColumnNames(rs, new String[]{"C1"});
        JDBC.assertFullResultSet(rs, new String[][]{{"1"}, {"2"}});

        // select * prepared statements do see added columns after 
        // alter table

        if (usingEmbedded()) // client/server doesn't keep cursor open.
        {
            rs = pSt.executeQuery();
            JDBC.assertColumnNames(rs, new String[]{"C1", "C2"});
            JDBC.assertFullResultSet(rs, new String[][]{
                        {"1", null},
                        {"2", null}
                    });
        } else {
            rs = pSt.executeQuery();
            JDBC.assertColumnNames(rs, new String[]{"C1"});
            JDBC.assertFullResultSet(rs, new String[][]{{"1"}, {"2"}});
        }

        // add non-nullable column to 0 row table and verify
        st.executeUpdate("alter table t0 add column c2 int not null default 0");
        st.executeUpdate("insert into t0 values (1, default)");

        rs = st.executeQuery("select * from t0");
        JDBC.assertColumnNames(rs, new String[]{"C1", "C2"});
        JDBC.assertFullResultSet(rs, new String[][]{{"1", "0"}});

        st.executeUpdate("drop table t0");
        conn.rollback();
        rs = st.executeQuery(" select  * from t0");
        JDBC.assertColumnNames(rs, new String[]{"C1"});
        JDBC.assertDrainResults(rs, 0);

        // add primary key to 0 row table and verify

        st.executeUpdate(
                "alter table t0_1 add column c2 int not null primary " +
                "key default 0");

        st.executeUpdate("insert into t0_1 values (1, 1)");

        //duplicate key value in a unique or primary key 
        //constraint or unique index not allowed
        assertStatementError("23505", st, "insert into t0_1 values (1, 1)");

        rs = st.executeQuery("select * from t0_1");
        JDBC.assertColumnNames(rs, new String[]{"C1", "C2"});
        JDBC.assertFullResultSet(rs, new String[][]{{"1", "1"}});

        conn.rollback();

        // add unique constraint to 0 and 1 row tables and verify

        st.executeUpdate(
                "alter table t0_1 add column c2 int not null unique " +
                " default 0");

        st.executeUpdate(
                " insert into t0_1 values (1, default)");

        //duplicate key value in a unique or primary key 
        //constraint or unique index not allowed
        assertStatementError("23505", st,
                " insert into t0_1 values (2, default)");

        st.executeUpdate("insert into t0_1 values (3, 1)");

        st.executeUpdate("drop table t1");
        st.executeUpdate("create table t1(c1 int)");

        st.executeUpdate(
                " alter table t1 add column c2 int not null unique default 0");

        st.executeUpdate("insert into t1 values (2, 2)");
        st.executeUpdate("insert into t1 values (3, 1)");

        // verify the consistency of the indexes on the user tables

        rs = st.executeQuery(
                "select tablename, " +
                "SYSCS_UTIL.SYSCS_CHECK_TABLE('" + DerbyConstants.TEST_DBO +
                "', tablename) from " + "sys.systables where tabletype = 'T'");

        expRS = new String[][]{
                    {"T0", "1"},
                    {"T0_1", "1"},
                    {"T0_2", "1"},
                    {"T0_3", "1"},
                    {"T1", "1"},
                    {"T1_1", "1"},
                    {"T2", "1"},
                    {"T3", "1"},
                    {"T4", "1"}
                };

        JDBC.assertUnorderedResultSet(rs, expRS, true);

        conn.rollback();

        st.executeUpdate(
                " create function countopens() returns varchar(128) " +
                "language java parameter style java external name " +
                "'org.apache.derbyTesting.functionTests.util.T_ConsistencyChecker." +
                "countOpens'");

        conn.commit();
        // do consistency check on scans, etc.

        rs = st.executeQuery("values countopens()");
        JDBC.assertFullResultSet(rs,
                new String[][]{{"No open scans, etc."}});
    }

    public void testDropObjects() throws Exception {
        Statement st = createStatement();
        createTestObjects(st);
        // some typical data

        st.executeUpdate(
                "create table tab1 (c1 int, c2 int not null " +
                "constraint tab1pk primary key, c3 double, c4 int)");

        st.executeUpdate("create index i11 on tab1 (c1)");
        st.executeUpdate("create unique index i12 on tab1 (c1)");
        st.executeUpdate("create index i13 on tab1 (c3, c1, c4)");
        st.executeUpdate("create unique index i14 on tab1 (c3, c1)");
        st.executeUpdate("insert into tab1 values (6, 5, 4.5, 90)");
        st.executeUpdate("insert into tab1 values (10, 3, 8.9, -5)");
        st.executeUpdate("insert into tab1 values (100, 15, 4.5, 9)");
        st.executeUpdate("insert into tab1 values (2, 8, 4.4, 8)");
        st.executeUpdate("insert into tab1 values (11, 9, 2.5, 88)");
        st.executeUpdate("insert into tab1 values(null,10, 3.5, 99)");
        st.executeUpdate("create view vw1 (col_sum, col_diff) as select " +
                "c1+c4, c1-c4 from tab1");
        st.executeUpdate("create view vw2 (c1) as select c3 from tab1");
        st.executeUpdate("create table tab2 (c1 int not null unique, c2 " +
                "double, c3 int, c4 int not null constraint c4_PK " +
                "primary key, c5 int, constraint t2ck check (c2+c3<100.0))");
        st.executeUpdate("create table tab3 (c1 int, c2 int, c3 int, c4 int," +
                "constraint t3fk foreign key (c2) references " +
                "tab2(c1), constraint t3ck check (c2-c3<80))");
        st.executeUpdate("create view vw3 (c1, c2) as select c5, tab3.c4 " +
                "from tab2, tab3 where tab3.c1 > 0");
        st.executeUpdate(
                " create view vw4 (c1) as select c4 from tab3 where c2 > 8");
        st.executeUpdate("create table tab4 (c1 int, c2 int, c3 int, c4 int)");
        st.executeUpdate("create table tab5 (c1 int)");
        st.executeUpdate("insert into tab4 values (1,2,3,4)");
        st.executeUpdate("create trigger tr1 after update of c2, c3, c4 on " +
                "tab4 for each row insert into tab5 values (1)");
        st.executeUpdate("create trigger tr2 after update of c3, c4 on tab4 " +
                "for each row insert into tab5 values (2)");

        // tr1 is dropped, tr2 still OK
        st.executeUpdate("drop trigger tr1");
        rs = st.executeQuery("select * from tab5");
        JDBC.assertColumnNames(rs, new String[]{"C1"});
        JDBC.assertDrainResults(rs, 0);

        // fire tr2 only
        assertUpdateCount(st, 1, "update tab4 set c3 = 33");
        assertUpdateCount(st, 1, " update tab4 set c4 = 44");

        rs = st.executeQuery("select * from tab5");
        JDBC.assertColumnNames(rs, new String[]{"C1"});
        JDBC.assertFullResultSet(rs, new String[][]{{"2"}, {"2"}});

        // drop tr2

        st.executeUpdate("drop trigger tr2");

        assertUpdateCount(st, 1, "update tab4 set c4 = 444");

        rs = st.executeQuery("select * from tab2");

        expColNames = new String[]{"C1", "C2", "C3", "C4", "C5"};
        JDBC.assertColumnNames(rs, expColNames);
        JDBC.assertDrainResults(rs, 0);

        st.executeUpdate("drop view vw2");
        st.executeUpdate("create view vw2 (c1) as select c3 from tab1");

        // vw1 should be dropped

        st.executeUpdate("drop view vw1");

        //view vw1 does not exist
        assertStatementError("42X05", st, "select * from vw1");

        // do the indexes still exist? the create index statements 
        // should fail

        st.executeUpdate("create index i13 on tab1 (c3, c1, c4)");
        checkWarning(st, "01504");
        st.executeUpdate("create unique index i14 on tab1 (c3, c1)");
        checkWarning(st, "01504");
        st.executeUpdate("create unique index i12 on tab1 (c1)");
        checkWarning(st, "01504");

        rs = st.executeQuery("select c2, c3, c4 from tab1 order by c3");

        expColNames = new String[]{"C2", "C3", "C4"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String[][]{
                    {"9", "2.5", "88"},
                    {"10", "3.5", "99"},
                    {"8", "4.4", "8"},
                    {"15", "4.5", "9"},
                    {"5", "4.5", "90"},
                    {"3", "8.9", "-5"}
                };

        JDBC.assertFullResultSet(rs, expRS, true);

        st.executeUpdate("drop index i12");
        st.executeUpdate("drop index i13");
        st.executeUpdate("drop index i14");

        // more data
        st.executeUpdate("insert into tab1 (c2, c3, c4) values (22, 8.9, 5)");
        st.executeUpdate("insert into tab1 (c2, c3, c4) values (11, 4.5, 67)");

        rs = st.executeQuery("select c2 from tab1");

        expColNames = new String[]{"C2"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String[][]{
                    {"3"},
                    {"5"},
                    {"8"},
                    {"9"},
                    {"10"},
                    {"11"},
                    {"15"},
                    {"22"}
                };

        JDBC.assertFullResultSet(rs, expRS, true);

        // add a new column
        st.executeUpdate("alter table tab1 add column c5 double");

        // drop view vw2 so can create a new one, with where clause
        st.executeUpdate("drop view vw2");
        st.executeUpdate(
                " create view vw2 (c1) as select c5 from tab1 where c2 > 5");

        // drop vw2 as well

        st.executeUpdate("drop view vw2");
        st.executeUpdate("alter table tab1 drop constraint tab1pk");

        // any surviving index? creating the index should not fail

        rs = st.executeQuery("select c4 from tab1 order by 1");

        expRS = new String[][]{
                    {"-5"},
                    {"5"},
                    {"8"},
                    {"9"},
                    {"67"},
                    {"88"},
                    {"90"},
                    {"99"}
                };

        JDBC.assertFullResultSet(rs, expRS, true);

        st.executeUpdate("create index i13 on tab1 (c3, c1, c4)");

        // should drop t2ck

        st.executeUpdate("alter table tab2 drop constraint t2ck");

        // this should drop t3fk, unique constraint and backing index

        st.executeUpdate("alter table tab3 drop constraint t3fk");
        st.executeUpdate("alter table tab2 drop constraint c4_PK");
        st.executeUpdate("insert into tab3 values (1,2,3,4)");

        // drop view vw3
        st.executeUpdate("drop view vw3");

        // violates t3ck

        st.executeUpdate("insert into tab3 (c1, c2, c3) values (81, 1, 2)");
        st.executeUpdate("insert into tab3 (c1, c2, c3) values (81, 2, 2)");

        // this should drop t3ck, vw4

        st.executeUpdate("alter table tab3 drop constraint t3ck");
        st.executeUpdate("drop view vw4");
        st.executeUpdate("insert into tab3 (c2, c3) values (-82, 9)");
        st.executeUpdate(
                " create view vw4 (c1) as select c3 from tab3 where c3+5>c4");

        // drop view vw4

        st.executeUpdate("drop view vw4");

        conn.rollback();

        // check that dropping a column will drop backing index on 
        // referencing table

        st.executeUpdate(
                "create table tt1(a int, b int not null constraint " +
                "tt1uc unique)");

        st.executeUpdate(
                " create table reftt1(a int constraint reftt1rc " +
                "references tt1(b))");

        // count should be 2

        rs = st.executeQuery(
                "select count(*) from sys.sysconglomerates c, " +
                "sys.systables t where t.tableid = c.tableid and " +
                "t.tablename = 'REFTT1'");
        JDBC.assertSingleValueResultSet(rs, "2");

        st.executeUpdate("alter table reftt1 drop constraint reftt1rc");
        st.executeUpdate("alter table tt1 drop constraint tt1uc");

        // count should be 1

        rs = st.executeQuery(
                "select count(*) from sys.sysconglomerates c, " +
                "sys.systables t where t.tableid = c.tableid and " +
                "t.tablename = 'REFTT1'");
        JDBC.assertSingleValueResultSet(rs, "1");

        conn.rollback();
    }

    public void testAddConstraint() throws Exception {
        Statement st = createStatement();
        createTestObjects(st);
        // add constraint negative tests add primary key to table 
        // which already has one

        st.executeUpdate("alter table t0 add column c3 int");

        //column contain null values cannot be a primary key because
        //it can contain null value
        assertStatementError("42831", st,
                " alter table t0 add constraint cons1 primary key(c3)");

        assertStatementError("42831", st,
                " alter table t0 add primary key(c3)");

        // add constraint references non-existant column
        assertStatementError("42X14", st,
                "alter table t4 add constraint t4pk primary key(\"c1\")");

        assertStatementError("42X14", st,
                " alter table t4 add constraint t4uq unique(\"c1\")");

        assertStatementError("42X14", st,
                " alter table t4 add constraint t4fk foreign key " +
                "(\"c1\") references t0");

        assertStatementError("42X04", st,
                " alter table t4 add constraint t4ck check (\"c1\" <> 4)");

        // add primary key to non-empty table with duplicates

        assertStatementError("23505", st, "alter table t4 add primary key(c1)");

        // positive tests add primary key to 0 row table and verify

        st.executeUpdate(
                "alter table t0_1 add column c2 int not null " +
                "constraint p2 primary key default 0");

        st.executeUpdate("insert into t0_1 values (1, 1)");

        //duplicating a key value in a primary key not allowed
        assertStatementError("23505", st, "insert into t0_1 values (1, 1)");

        rs = st.executeQuery("select * from t0_1");
        JDBC.assertColumnNames(rs, new String[]{"C1", "C2"});
        JDBC.assertFullResultSet(rs, new String[][]{{"1", "1"}});

        // add check constraint to 0 row table and verify

        st.executeUpdate("alter table t0_1 add column c3 int check(c3 != 3)");
        st.executeUpdate("insert into t0_1 values (1, 2, 1)");

        assertStatementError("23513", st, "insert into t0_1 values (1, 3, 3)");

        st.executeUpdate("insert into t0_1 values (1, 4, 1)");

        rs = st.executeQuery("select c1,c3 from t0_1");

        JDBC.assertUnorderedResultSet(rs, new String[][]{
                    {"1", null},
                    {"1", "1"},
                    {"1", "1"}
                });

        // add check constraint to table with rows that are ok

        st.executeUpdate("alter table t0_1 add column c4 int");
        st.executeUpdate("delete from t0_1");
        st.executeUpdate("insert into t0_1 values (1, 5,1,1)");
        st.executeUpdate("insert into t0_1 values (2, 6,1,2)");

        st.executeUpdate(
                " alter table t0_1 add constraint ck1 check(c4 = c1)");

        rs = st.executeQuery("select c1,c4 from t0_1");

        JDBC.assertFullResultSet(rs, new String[][]{
                    {"1", "1"},
                    {"2", "2"}
                });

        // verify constraint has been added, the following should fail

        assertStatementError("23513", st,
                "insert into t0_1(c1,c4) values (1, 3)");


        // add check constraint to table with rows w/ 3 failures

        st.executeUpdate("drop table t0_1");
        st.executeUpdate("create table t0_1 (c1 int)");
        st.executeUpdate("alter table t0_1 add column c2 int");
        st.executeUpdate("insert into t0_1 values (1, 1)");
        st.executeUpdate("insert into t0_1 values (2, 2)");
        st.executeUpdate("insert into t0_1 values (2, 2)");
        st.executeUpdate("insert into t0_1 values (666, 2)");
        st.executeUpdate("insert into t0_1 values (2, 2)");
        st.executeUpdate("insert into t0_1 values (3, 3)");
        st.executeUpdate("insert into t0_1 values (666, 3)");
        st.executeUpdate("insert into t0_1 values (666, 3)");
        st.executeUpdate("insert into t0_1 values (3, 3)");
        assertStatementError("X0Y59", st,
                " alter table t0_1 add constraint ck1 check(c2 = c1)");

        // verify constraint has NOT been added, the following 
        // should succeed

        st.executeUpdate(
                "insert into t0_1 values (1, 3)");

        rs = st.executeQuery(
                " select * from t0_1");

        expColNames = new String[]{"C1", "C2"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String[][]{
                    {"1", "1"},
                    {"2", "2"},
                    {"2", "2"},
                    {"666", "2"},
                    {"2", "2"},
                    {"3", "3"},
                    {"666", "3"},
                    {"666", "3"},
                    {"3", "3"},
                    {"1", "3"}
                };

        JDBC.assertFullResultSet(rs, expRS, true);


        // check and primary key constraints on same table and enforced

        st.executeUpdate("create table t0_4(c1 int)");
        st.executeUpdate(
                "alter table t0_4 add column c2 int not null " +
                "constraint p2 primary key default 0");

        st.executeUpdate("alter table t0_4 add check(c2 = c1)");
        st.executeUpdate("insert into t0_4 values (1, 1)");

        //fail:check constraint was violated
        assertStatementError("23513", st, "insert into t0_4 values (1, 2)");
        //fail:duplicate primary key
        assertStatementError("23505", st, "insert into t0_4 values (1, 1)");
        //fail:check constraint was violated
        assertStatementError("23513", st, "insert into t0_4 values (2, 1)");

        st.executeUpdate("insert into t0_4 values (2, 2)");

        rs = st.executeQuery("select * from t0_4");

        JDBC.assertColumnNames(rs, new String[]{"C1", "C2"});

        JDBC.assertUnorderedResultSet(rs, new String[][]{
                    {"1", "1"},
                    {"2", "2"}
                });

        st.executeUpdate("drop table t0_4");


        // add primary key constraint to table with > 1 row
        st.executeUpdate("alter table t3 add column c3 int");
        st.executeUpdate("alter table t3 add unique(c3)");

        // add unique constraint to 0 and 1 row tables and verify

        st.executeUpdate(
                "alter table t0_2 add column c2 int not null unique default 0");

        st.executeUpdate("insert into t0_2 values (1, default)");
        st.executeUpdate("insert into t0_2 values (1, 1)");

        assertUpdateCount(st, 1, " delete from t1_1");

        st.executeUpdate("alter table t1_1 add column c2 int not null unique " +
                "default 0");

        st.executeUpdate("insert into t1_1 values (1, 2)");

        //fail:duplicate key value in "unique" coloumn 
        assertStatementError("23505", st, " insert into t1_1 values (1, 2)");

        st.executeUpdate("insert into t1_1 values (1, 1)");

        // add unique constraint to table with > 1 row

        st.executeUpdate("alter table t3 add unique(c1)");

        // verify prepared alter table dependent on underlying table

        assertCompileError("42Y55", "alter table xxx add check(c2 = 1)");
        st.executeUpdate("create table xxx(c1 int, c2 int)");
        pSt = prepareStatement("alter table xxx add check(c2 = 1)");
        assertUpdateCount(pSt, 0);
        st.executeUpdate("drop table xxx");
        st.executeUpdate("create table xxx(c1 int)");

        //add constraint to a coloumn not in the table
        assertStatementError("42X04", pSt);
        st.executeUpdate("alter table xxx add column c2 int");
        assertUpdateCount(pSt, 0);
        st.executeUpdate("drop table xxx");

        // verify the consistency of the indexes on the user tables

        rs = st.executeQuery(
                "select tablename, " +
                "SYSCS_UTIL.SYSCS_CHECK_TABLE('" + DerbyConstants.TEST_DBO +
                "', tablename) from " + "sys.systables where tabletype = 'T'");

        expColNames = new String[]{"TABLENAME", "2"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String[][]{
                    {"T0", "1"},
                    {"T0_1", "1"},
                    {"T0_2", "1"},
                    {"T0_3", "1"},
                    {"T1", "1"},
                    {"T1_1", "1"},
                    {"T2", "1"},
                    {"T3", "1"},
                    {"T4", "1"}
                };

        JDBC.assertUnorderedResultSet(rs, expRS, true);
    }

    public void testDropConstraint() throws Exception {
        Statement st = createStatement();
        createTestObjects(st);
        // drop constraint negative tests drop non-existent constraint

        assertStatementError("42X86", st,
                "alter table t0 drop constraint notexists");

        // constraint/table mismatch

        assertStatementError("42X86", st,
                "alter table t1 drop constraint p1");

        // In DB2 compatibility mode, we can't add a nullable 
        // primary key

        assertStatementError("42831", st,
                "alter table t0_1 add constraint p2 primary key(c1)");

        assertStatementError("42X86", st,
                " alter table t0_1 drop constraint p2");

        // positive tests verify that we can add/drop/add/drop/... 
        // constraints

        st.executeUpdate(
                "alter table t0_1 add column c2 int not null " +
                "constraint p2 primary key default 0");

        assertUpdateCount(st, 0, "delete from t0_1");
        st.executeUpdate("alter table t0_1 drop constraint p2");
        st.executeUpdate("alter table t0_1 add constraint p2 primary key(c2)");
        st.executeUpdate("alter table t0_1 drop constraint p2");
        st.executeUpdate("alter table t0_1 add constraint p2 primary key(c2)");

        // verify that constraint is still enforced

        st.executeUpdate("insert into t0_1 values (1,1)");
        assertStatementError("23505", st, "insert into t0_1 values (1,1)");

        // verify the consistency of the indexes on the user tables

        rs = st.executeQuery(
                "select tablename, " + "SYSCS_UTIL.SYSCS_CHECK_TABLE('" +
                DerbyConstants.TEST_DBO + "', tablename) from " +
                "sys.systables where tabletype = 'T' and tablename = 'T0_1'");

        expColNames = new String[]{"TABLENAME", "2"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String[][]{
                    {"T0_1", "1"}
                };

        JDBC.assertFullResultSet(rs, expRS, true);

        // verify that alter table works after drop/recreate of table

        pSt = prepareStatement("alter table t0_1 drop constraint p2");

        assertUpdateCount(pSt, 0);

        st.executeUpdate("drop table t0_1");

        st.executeUpdate(
                " create table t0_1 (c1 int, c2 int not null " +
                "constraint p2 primary key)");

        assertUpdateCount(pSt, 0);

        // do consistency check on scans, etc. values 
        // (org.apache.derbyTesting.functionTests.util.T_Consistency
        // Checker::countOpens()) verify the consistency of the 
        // indexes on the system catalogs

        rs = st.executeQuery(
                "select tablename, " +
                "SYSCS_UTIL.SYSCS_CHECK_TABLE('SYS', tablename) from " +
                "sys.systables where tabletype = 'S' and tablename " +
                "!= 'SYSDUMMY1'");

        expRS = new String[][]{
                    {"SYSCONGLOMERATES", "1"},
                    {"SYSTABLES", "1"},
                    {"SYSCOLUMNS", "1"},
                    {"SYSSCHEMAS", "1"},
                    {"SYSCONSTRAINTS", "1"},
                    {"SYSKEYS", "1"},
                    {"SYSDEPENDS", "1"},
                    {"SYSALIASES", "1"},
                    {"SYSVIEWS", "1"},
                    {"SYSCHECKS", "1"},
                    {"SYSFOREIGNKEYS", "1"},
                    {"SYSSTATEMENTS", "1"},
                    {"SYSFILES", "1"},
                    {"SYSTRIGGERS", "1"},
                    {"SYSSTATISTICS", "1"},
                    {"SYSTABLEPERMS", "1"},
                    {"SYSCOLPERMS", "1"},
                    {"SYSROUTINEPERMS", "1"},
                    {"SYSROLES", "1"},
                    {"SYSSEQUENCES", "1"},
                    {"SYSPERMS", "1"}
                };

        JDBC.assertFullResultSet(rs, expRS, true);

        // verify the consistency of the indexes on the user tables

        rs = st.executeQuery(
                "select tablename, " +
                "SYSCS_UTIL.SYSCS_CHECK_TABLE('" + DerbyConstants.TEST_DBO +
                "', tablename) from " + "sys.systables where tabletype = 'T'");

        expRS = new String[][]{
                    {"T0", "1"},
                    {"T0_2", "1"},
                    {"T0_3", "1"},
                    {"T1", "1"},
                    {"T1_1", "1"},
                    {"T2", "1"},
                    {"T3", "1"},
                    {"T4", "1"},
                    {"T0_1", "1"}
                };

        JDBC.assertUnorderedResultSet(rs, expRS, true);

        // bugs 793

        st.executeUpdate(
                "create table b793 (pn1 int not null constraint " +
                "named_primary primary key, pn2 int constraint " +
                "named_pn2 check (pn2 > 3))");

        st.executeUpdate("alter table b793 drop constraint named_primary");
        st.executeUpdate("drop table b793");

        // test that drop constraint removes backing indexes

        st.executeUpdate("drop table t1");

        st.executeUpdate(
                " create table t1(a int not null constraint t1_pri " +
                "primary key)");

        st.executeUpdate(
                " create table reft1(a int constraint t1_ref " +
                "references t1(a))");

        // count should be 2

        rs = st.executeQuery(
                "select count(*) from sys.sysconglomerates c, " +
                "sys.systables t where c.tableid = t.tableid and " +
                "t.tablename = 'REFT1'");
        JDBC.assertSingleValueResultSet(rs, "2");

        st.executeUpdate("alter table reft1 drop constraint t1_ref");
        st.executeUpdate("alter table t1 drop constraint t1_pri");

        // count should be 1

        rs = st.executeQuery(
                "select count(*) from sys.sysconglomerates c, " +
                "sys.systables t where c.tableid = t.tableid and " +
                "t.tablename = 'REFT1'");
        JDBC.assertSingleValueResultSet(rs, "1");

        st.executeUpdate("drop table reft1");

        // clean up

        st.executeUpdate("drop view v2");
        st.executeUpdate("drop view v1");
        st.executeUpdate("drop table t0");
        st.executeUpdate("drop table t0_1");
        st.executeUpdate("drop table t0_2");
        st.executeUpdate("drop table t0_3");
        st.executeUpdate("drop table t1");
        st.executeUpdate("drop table t1_1");
        st.executeUpdate("drop table t3");
        st.executeUpdate("drop table t4");
    }

    public void testWithSchema() throws Exception {
        Statement st = createStatement();
        createTestObjects(st);
        //----------------------------------------------------
        // special funky schema 
        // tests----------------------------------------------------

        st.executeUpdate("create schema newschema");

        //drop a table that does not exist
        assertStatementError("42Y55", st, "drop table x");
        st.executeUpdate("create table x (x int not null, y int not null)");
        st.executeUpdate(
                "alter table x add constraint NEWCONS primary key (x)");

        // schemaname should be DerbyConstants.TEST_DBO

        rs = st.executeQuery(
                "select schemaname, constraintname from " +
                "sys.sysconstraints c, sys.sysschemas s where " +
                "s.schemaid = c.schemaid order by 1");

        JDBC.assertFullResultSet(rs, new String[][]{
                    {DerbyConstants.TEST_DBO, "P1"},
                    {DerbyConstants.TEST_DBO, "NEWCONS"}
                });
        //duplicating values ina priary key column
        assertStatementError("23505", st,
                " insert into x values (1,1),(1,1)");

        st.executeUpdate(
                " alter table x drop constraint " +
                DerbyConstants.TEST_DBO + ".newcons");

        st.executeUpdate(
                " alter table x add constraint newcons primary key (x)");

        // schemaname should be DerbyConstants.TEST_DBO

        rs = st.executeQuery(
                "select schemaname, constraintname from " +
                "sys.sysconstraints c, sys.sysschemas s where " +
                "s.schemaid = c.schemaid order by 1");
        JDBC.assertFullResultSet(rs, new String[][]{
                    {DerbyConstants.TEST_DBO, "P1"},
                    {DerbyConstants.TEST_DBO, "NEWCONS"}
                });

        //schema does not exist
        assertStatementError("42Y07", st,
                "alter table x drop constraint badschema.newcons");
        //constriant does not exis in the schama
        assertStatementError("42X86", st,
                "alter table x drop constraint newschema.newcons");

        st.executeUpdate(
                "alter table x drop constraint " +
                DerbyConstants.TEST_DBO + ".newcons");

        // bad schema name(table x is not in the same schema of constraint)
        assertStatementError("42X85", st,
                "alter table x add constraint badschema.newcons " +
                "primary key (x)");

        // two constriants, same name, different schema (second will fail)

        st.executeUpdate("drop table x");
        st.executeUpdate("create table x (x int not null, y int not null)");
        st.executeUpdate("alter table x add constraint con check (x > 1)");

        assertStatementError("42X85", st,
                " alter table x add constraint newschema.con check (x > 1)");

        rs = st.executeQuery(
                " select schemaname, constraintname from " +
                "sys.sysconstraints c, sys.sysschemas s where " +
                "s.schemaid = c.schemaid order by 1");
        JDBC.assertFullResultSet(rs, new String[][]{
                    {DerbyConstants.TEST_DBO, "P1"},
                    {DerbyConstants.TEST_DBO, "CON"}
                });

        st.executeUpdate("set schema emptyschema");

        // fail, cannot find emptyschema.conn
        assertStatementError("42X86", st,
                "alter table " + DerbyConstants.TEST_DBO +
                ".x drop constraint emptyschema.con");

        rs = st.executeQuery(
                " select schemaname, constraintname from " +
                "sys.sysconstraints c, sys.sysschemas s where " +
                "s.schemaid = c.schemaid order by 1");
        JDBC.assertFullResultSet(rs, new String[][]{
                    {DerbyConstants.TEST_DBO, "P1"},
                    {DerbyConstants.TEST_DBO, "CON"}
                });

        st.executeUpdate(" set schema newschema");

        // add constraint, default to table schema

        st.executeUpdate(
                "alter table " + DerbyConstants.TEST_DBO +
                ".x add constraint con2 check (x > 1)");

        // added constraint in DerbyConstants.TEST_DBO
        //(defaults to table's schema)

        rs = st.executeQuery(
                "select schemaname, constraintname from " +
                "sys.sysconstraints c, sys.sysschemas s where " +
                "s.schemaid = c.schemaid order by 1,2");
        JDBC.assertFullResultSet(rs, new String[][]{
                    {DerbyConstants.TEST_DBO, "CON"},
                    {DerbyConstants.TEST_DBO, "CON2"},
                    {DerbyConstants.TEST_DBO, "P1"}
                });

        st.executeUpdate("drop table " + DerbyConstants.TEST_DBO + ".x");
        st.executeUpdate("drop schema newschema restrict");
    }

    public void testTemporaryTable() throws Exception {
        Statement st = createStatement();
        createTestObjects(st);
        // some temporary table tests declare temp table with no 
        // explicit on commit behavior.

        assertUpdateCount(st, 0,
                "declare global temporary table session.t1 (c11 int) " +
                "not logged");

        assertUpdateCount(st, 0,
                " declare global temporary table session.t2 (c21 " +
                "int) on commit delete rows not logged");

        assertUpdateCount(st, 0,
                " declare global temporary table session.t3 (c31 " +
                "int) on commit preserve rows not logged");

        st.executeUpdate("drop table session.t1");
        st.executeUpdate("drop table session.t2");
        st.executeUpdate("drop table session.t3");
        assertStatementError("42Y55", st, "drop table session.t1");
        st.executeUpdate("drop table t1");
        st.executeUpdate(
                "create table t1(c1 int, c2 int not null primary key)");
        st.executeUpdate("insert into t1 values (1, 1)");
        assertStatementError("23505", st, "insert into t1 values (1, 1)");
        st.executeUpdate("alter table t1 drop primary key");
        st.executeUpdate("insert into t1 values (1, 1)");

        rs = st.executeQuery("select * from t1");

        expColNames = new String[]{"C1", "C2"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String[][]{
                    {"1", "1"},
                    {"1", "1"}
                };

        JDBC.assertFullResultSet(rs, expRS, true);

        //fail, no primary key to remove
        assertStatementError("42X86", st,
                " alter table t1 drop primary key");
        //no constraint in the empty schema
        assertStatementError("42X86", st,
                " alter table t1 drop constraint emptyschema.C1");
        //schema does not exist
        assertStatementError("42Y07", st,
                " alter table t1 drop constraint nosuchschema.C2");
        //table and constriant not in the same schema
        assertStatementError("42X85", st,
                " alter table t1 add constraint " +
                "emptyschema.C1_PLUS_C2 check ((c1 + c2) < 100)");

        st.executeUpdate(
                " alter table t1 add constraint C1_PLUS_C2 check " +
                "((c1 + c2) < 100)");

        pSt = prepareStatement(
                "alter table t1 drop constraint C1_PLUS_C2");

        st.executeUpdate(
                " alter table " + DerbyConstants.TEST_DBO +
                ".t1 drop constraint " + DerbyConstants.TEST_DBO +
                ".C1_PLUS_C2");

        assertStatementError("42X86", pSt);


        st.executeUpdate(
                " drop table t1");

        // bug 5817 - make LOGGED non-reserved keyword. following 
        // test cases for that

        st.executeUpdate("create table LOGGED(c11 int)");
        st.executeUpdate("drop table LOGGED");
        st.executeUpdate("create table logged(logged int)");
        st.executeUpdate("drop table logged");

        assertUpdateCount(st, 0,
                " declare global temporary table " +
                "session.logged(logged int) on commit delete rows not logged");
    }

    /**
     * See DERBY-4693 for a case where this was broken.
     */
    public void testRenameAutoincrementColumn()
	throws Exception
    {
	// First, the repro from the Jira issue originally logged:
	Statement st = createStatement();
	st.executeUpdate("create table d4693" +
		"(a int generated always as identity, b int)");
        JDBC.assertFullResultSet(st.executeQuery(
                "select columnname,columnnumber,columndatatype," +
		"       autoincrementvalue," +
		"       autoincrementstart," +
		"       autoincrementinc" +
		" from sys.syscolumns where " +
		"      columnname = 'A' and " +
		"      referenceid in (select tableid " +
                "             from sys.systables where tablename = 'D4693')"),
                new String[][]{ {"A","1","INTEGER NOT NULL","1","1","1"} });
	st.executeUpdate("insert into d4693 (b) values (1)");
	st.executeUpdate("rename column d4693.a to a2");
        JDBC.assertFullResultSet(st.executeQuery(
                "select columnname,columnnumber,columndatatype," +
		"       autoincrementvalue," +
		"       autoincrementstart," +
		"       autoincrementinc" +
		" from sys.syscolumns where " +
		"      columnname = 'A2' and " +
		"      referenceid in (select tableid " +
                "             from sys.systables where tablename = 'D4693')"),
                new String[][]{ {"A2","1","INTEGER NOT NULL","2","1","1"} });
	st.executeUpdate("insert into d4693 (b) values (2)");
        JDBC.assertFullResultSet(st.executeQuery(
                "select a2, b from d4693"),
                new String[][]{ {"1", "1"}, {"2", "2"} });
        st.executeUpdate("drop table d4693");

	// Then, a few other arbitrary test cases:
	String colspecs[] = {
	    "autoinc int generated always as identity (start with 100)",
	    "autoinc1 int generated always as identity (increment by 100)",
	    "autoinc2 int generated always as identity (start with 101, increment by 100)",
	    "a11 int generated always as identity (start with  0, increment by -1)",
	    "a21 int generated always as identity (start with  +0, increment by -1)",
	    "a31 int generated always as identity (start with  -1, increment by -1)",
	    "a41 int generated always as identity (start with  -11, increment by +100)"
	};
	String cn[] = {
	    "AUTOINC", "AUTOINC1", "AUTOINC2", "A11", "A21", "A31", "A41" };
	String val[] = {
	    "100",     "1",        "101",      "0",   "0",   "-1",  "-11" };
	String start[] = {
	    "100",     "1",        "101",      "0",   "0",   "-1",  "-11" };
	String inc[] = {
	    "1",      "100",       "100",      "-1",  "-1",  "-1",  "100" };
	for (int i = 0; i < colspecs.length; i++)
	{
	    st.executeUpdate("create table d4693 (" + colspecs[i] + ")");
	    checkValStartInc(st, cn[i], val[i], start[i], inc[i]);
	    st.executeUpdate("rename column d4693."+cn[i]+" to "+cn[i]+"2");
	    checkValStartInc(st, cn[i]+"2", val[i], start[i], inc[i]);
	    st.executeUpdate("drop table d4693");
	}
    }
    private void checkValStartInc(Statement st, String nm, String v,
					String s, String inc)
	throws Exception
    {
        JDBC.assertFullResultSet(st.executeQuery(
            "select autoincrementvalue,autoincrementstart,autoincrementinc" +
		" from sys.syscolumns where columnname = '"+nm+"' and " +
		"      referenceid in (select tableid " +
                "             from sys.systables where tablename = 'D4693')"),
                new String[][]{ {v, s, inc} });
    }

    public void testAlterColumn() throws Exception {
        Statement st = createStatement();
        createTestObjects(st);
        // tests for ALTER TABLE ALTER COLUMN [NOT] NULL

        st.executeUpdate(
                "create table atmcn_1 (a integer, b integer not null)");

        // should fail because b cannot be null
        assertStatementError("23502", st,
                "insert into atmcn_1 (a) values (1)");

        st.executeUpdate("insert into atmcn_1 values (1,1)");

        rs = st.executeQuery("select * from atmcn_1");

        expColNames = new String[]{"A", "B"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String[][]{
                    {"1", "1"}
                };

        JDBC.assertFullResultSet(rs, expRS, true);

        st.executeUpdate("alter table atmcn_1 alter column a not null");

        // should fail because a cannot be null

        assertStatementError("23502", st,
                "insert into atmcn_1 (b) values (2)");

        st.executeUpdate("insert into atmcn_1 values (2,2)");

        rs = st.executeQuery("select * from atmcn_1");

        expColNames = new String[]{"A", "B"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String[][]{
                    {"1", "1"},
                    {"2", "2"}
                };

        JDBC.assertFullResultSet(rs, expRS, true);

        st.executeUpdate("alter table atmcn_1 alter column b null");
        st.executeUpdate("insert into atmcn_1 (a) values (1)");

        rs = st.executeQuery("select * from atmcn_1");

        expColNames = new String[]{"A", "B"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String[][]{
                    {"1", "1"},
                    {"2", "2"},
                    {"1", null}
                };

        JDBC.assertFullResultSet(rs, expRS, true);

        // Now that B has a null value, trying to modify it to NOT 
        // NULL should fail

        assertStatementError("X0Y80", st,
                "alter table atmcn_1 alter column b not null");

        // show that a column which is part of the PRIMARY KEY 
        // cannot be modified NULL

        st.executeUpdate(
                "create table atmcn_2 (a integer not null primary " +
                "key, b integer not null)");

        assertStatementError("42Z20", st,
                " alter table atmcn_2 alter column a null");

        st.executeUpdate(
                " create table atmcn_3 (a integer not null, b " +
                "integer not null)");

        st.executeUpdate(
                " alter table atmcn_3 add constraint atmcn_3_pk " +
                "primary key(a, b)");

        assertStatementError("42Z20", st,
                " alter table atmcn_3 alter column b null");

        // verify that the keyword "column" in the ALTER TABLE ... 
        // ALTER COLUMN ... statement is optional:

        st.executeUpdate(
                "create table atmcn_4 (a integer not null, b integer)");

        st.executeUpdate("alter table atmcn_4 alter a null");

        //set column, part of unique constraint, to null

        st.executeUpdate(
                "create table atmcn_5 (a integer not null, b integer " +
                "not null unique)");

        st.executeUpdate("alter table atmcn_5 alter column b null");

        // tests for ALTER TABLE ALTER COLUMN DEFAULT

        st.executeUpdate(
                "create table atmod_1 (a integer, b varchar(10))");

        st.executeUpdate("insert into atmod_1 values (1, 'one')");
        st.executeUpdate("alter table atmod_1 alter column a default -1");
        st.executeUpdate("insert into atmod_1 values (default, 'minus one')");
        st.executeUpdate("insert into atmod_1 (b) values ('b')");

        rs = st.executeQuery("select * from atmod_1");

        JDBC.assertColumnNames(rs, new String[]{"A", "B"});

        expRS = new String[][]{
                    {"1", "one"},
                    {"-1", "minus one"},
                    {"-1", "b"}
                };

        JDBC.assertFullResultSet(rs, expRS, true);

        st.executeUpdate("alter table atmod_1 alter a default 42");
        st.executeUpdate("insert into atmod_1 values(3, 'three')");
        st.executeUpdate("insert into atmod_1 values (default, 'forty two')");

        rs = st.executeQuery("select * from atmod_1");
        JDBC.assertColumnNames(rs, new String[]{"A", "B"});
        JDBC.assertFullResultSet(rs, new String[][]{
                    {"1", "one"},
                    {"-1", "minus one"},
                    {"-1", "b"},
                    {"3", "three"},
                    {"42", "forty two"}
                });

        // Tests for renaming a column. These tests are in 
        // AlterTableTest because renaming a column is closely 
        // linked, conseptually, to other table alterations. 
        // However, the actual syntax is:    RENAME COLUMN t.c1 TO c2

        st.executeUpdate(
                "create table renc_1 (a int, b varchar(10), c " +
                "timestamp, d double)");

        // table doesn't exist, should fail:

        assertStatementError("42Y55", st, "rename column renc_no_such.a to b");

        // table exists, but column doesn't exist

        assertStatementError("42X14", st, "rename column renc_1.no_such to e");

        // new column name already exists in table:

        assertStatementError("X0Y32", st, "rename column renc_1.a to c");

        // can't rename a column to itself:

        assertStatementError("X0Y32", st, "rename column renc_1.b to b");

        // new column name is a reserved word:

        assertStatementError("42X01", st,
                "rename column renc_1.a to select");

        //attempt to rename a column in a system table. Should fail
        assertStatementError("42X62", st,
                "rename column sys.sysconglomerates.isindex to is_an_index");

        // attempt to rename a column in a view, should fail:

        st.executeUpdate(
                "create view renc_vw_1 (v1, v2) as select b, d from renc_1");

        assertStatementError("42Y62", st,
                " rename column renc_vw_1.v2 to v3");


        // attempt to rename a column in an index, should fail:

        st.executeUpdate(
                "create index renc_idx_1 on renc_1 (c, d)");

        assertStatementError("42Y55", st,
                " rename column renc_idx_1.d to d_new");


        // A few syntax errors in the statement, to check for 
        // reasonable messages:

        assertStatementError("42Y55", st, "rename column renc_1 to b");
        assertStatementError("42X01", st, "rename column renc_1 rename a to b");
        assertStatementError("42X01", st, "rename column renc_1.a");
        assertStatementError("42X01", st, "rename column renc_1.a b");
        assertStatementError("42X01", st, "rename column renc_1.a to");
        assertStatementError("42X01", st, "rename column renc_1.a to b, c");
        assertStatementError("42X01", st,
                " rename column renc_1.a to b and c to d");

        //Rename a column which is the primary key of the table

        st.executeUpdate(
                "create table renc_2(c1 int not null constraint " +
                "renc_2_p1 primary key)");

        st.executeUpdate("rename column renc_2.c1 to c2");

        dbmd = conn.getMetaData();
        rs = dbmd.getColumns(null, null, "RENC_2", "C2");
        assertTrue(rs.next());
        assertEquals("C2", rs.getString("COLUMN_NAME"));
        assertEquals("INTEGER", rs.getString("TYPE_NAME"));
        assertEquals("0", rs.getString("DECIMAL_DIGITS"));
        assertEquals("10", rs.getString("NUM_PREC_RADIX"));
        assertEquals("10", rs.getString("COLUMN_SIZE"));
        assertEquals(null, rs.getString("COLUMN_DEF"));
        assertEquals(null, rs.getString("CHAR_OCTET_LENGTH"));
        assertEquals("NO", rs.getString("IS_NULLABLE"));
        assertFalse(rs.next());

        if (usingEmbedded()) {
            dbmd = conn.getMetaData();
            rs = dbmd.getIndexInfo(null, null, "RENC_2", false, false);
            assertTrue(rs.next());
            assertEquals("RENC_2", rs.getString("TABLE_NAME"));
            assertEquals("C2", rs.getString("COLUMN_NAME"));
            assertEquals("false", rs.getString("NON_UNIQUE"));
            assertEquals("3", rs.getString("TYPE"));
            assertEquals("A", rs.getString("ASC_OR_DESC"));
            assertEquals(null, rs.getString("CARDINALITY"));
            assertEquals(null, rs.getString("PAGES"));
            assertFalse(rs.next());
        }

        rs = st.executeQuery(
                " select c.constraintname, c.type from " +
                "sys.sysconstraints c, sys.systables t where " +
                "t.tableid = c.tableid and t.tablename = 'RENC_2'");

        JDBC.assertFullResultSet(rs, new String[][]{{"RENC_2_P1",
                        "P"
                    }});

        st.executeUpdate(
                " create table renc_3 (a integer not null, b integer " +
                "not null, c int, constraint renc_3_pk primary key(a, b))");

        st.executeUpdate(
                "rename column renc_3.b to newbie");

        dbmd = conn.getMetaData();
        rs = dbmd.getColumns(null, null, "RENC_3", "NEWBIE");
        assertTrue(rs.next());
        assertEquals("NEWBIE", rs.getString("COLUMN_NAME"));
        assertEquals("INTEGER", rs.getString("TYPE_NAME"));
        assertEquals("0", rs.getString("DECIMAL_DIGITS"));
        assertEquals("10", rs.getString("NUM_PREC_RADIX"));
        assertEquals("10", rs.getString("COLUMN_SIZE"));
        assertEquals(null, rs.getString("COLUMN_DEF"));
        assertEquals(null, rs.getString("CHAR_OCTET_LENGTH"));
        assertEquals("NO", rs.getString("IS_NULLABLE"));
        assertFalse(rs.next());

        if (usingEmbedded()) {
            dbmd = conn.getMetaData();
            rs = dbmd.getIndexInfo(null, null, "RENC_3", false, false);
            assertTrue(rs.next());
            assertEquals("RENC_3", rs.getString("TABLE_NAME"));
            assertEquals("A", rs.getString("COLUMN_NAME"));
            assertEquals("false", rs.getString("NON_UNIQUE"));
            assertEquals("3", rs.getString("TYPE"));
            assertEquals("A", rs.getString("ASC_OR_DESC"));
            assertEquals(null, rs.getString("CARDINALITY"));
            assertEquals(null, rs.getString("PAGES"));
            assertTrue(rs.next());
            assertEquals("RENC_3", rs.getString("TABLE_NAME"));
            assertEquals("NEWBIE", rs.getString("COLUMN_NAME"));
            assertEquals("false", rs.getString("NON_UNIQUE"));
            assertEquals("3", rs.getString("TYPE"));
            assertEquals("A", rs.getString("ASC_OR_DESC"));
            assertEquals(null, rs.getString("CARDINALITY"));
            assertEquals(null, rs.getString("PAGES"));
            assertFalse(rs.next());
        }
        rs = st.executeQuery(
                " select c.constraintname, c.type from " +
                "sys.sysconstraints c, sys.systables t where " +
                "t.tableid = c.tableid and t.tablename = 'RENC_3'");

        JDBC.assertFullResultSet(rs, new String[][]{{"RENC_3_PK",
                        "P"
                    }});

        st.executeUpdate(
                " create table renc_4 (c1 int not null unique, c2 " +
                "double, c3 int, c4 int not null constraint " +
                "renc_4_c4_PK primary key, c5 int, c6 int, " +
                "constraint renc_4_t2ck check (c2+c3<100.0))");

        st.executeUpdate(
                " create table renc_5 (c1 int, c2 int, c3 int, c4 " +
                "int, c5 int not null, c6 int, constraint " +
                "renc_5_t3fk foreign key (c2) references renc_4(c4), " +
                "constraint renc_5_unq unique(c5), constraint " +
                "renc_5_t3ck check (c2-c3<80))");

        // Attempt to rename a column referenced by a foreign key 
        // constraint should fail:

        assertStatementError(
                "X0Y25", st,
                "rename column renc_4.c4 to another_c4");

        // Rename a column with a unique constraint should work:
        st.executeUpdate(
                "rename column renc_4.c1 to unq_c1");
        
        if (usingEmbedded()) {
            dbmd = conn.getMetaData();
            rs = dbmd.getIndexInfo(null, null, "RENC_4", false, false);
            assertTrue(rs.next());
            assertEquals("RENC_4", rs.getString("TABLE_NAME"));
            assertEquals("UNQ_C1", rs.getString("COLUMN_NAME"));
            assertEquals("false", rs.getString("NON_UNIQUE"));
            assertEquals("3", rs.getString("TYPE"));
            assertEquals("A", rs.getString("ASC_OR_DESC"));
            assertEquals(null, rs.getString("CARDINALITY"));
            assertEquals(null, rs.getString("PAGES"));
            assertTrue(rs.next());
            assertEquals("RENC_4", rs.getString("TABLE_NAME"));
            assertEquals("C4", rs.getString("COLUMN_NAME"));
            assertEquals("false", rs.getString("NON_UNIQUE"));
            assertEquals("3", rs.getString("TYPE"));
            assertEquals("A", rs.getString("ASC_OR_DESC"));
            assertEquals(null, rs.getString("CARDINALITY"));
            assertEquals(null, rs.getString("PAGES"));
            assertFalse(rs.next());
        }
        
        st.executeUpdate(
                "rename column renc_5.c5 to unq_c5");
        
        if (usingEmbedded()) {
            dbmd = conn.getMetaData();
            rs = dbmd.getIndexInfo(null, null, "RENC_5", false, false);
            assertTrue(rs.next());
            assertEquals("RENC_5", rs.getString("TABLE_NAME"));
            assertEquals("UNQ_C5", rs.getString("COLUMN_NAME"));
            assertEquals("false", rs.getString("NON_UNIQUE"));
            assertEquals("3", rs.getString("TYPE"));
            assertEquals("A", rs.getString("ASC_OR_DESC"));
            assertEquals(null, rs.getString("CARDINALITY"));
            assertEquals(null, rs.getString("PAGES"));
            assertTrue(rs.next());
            assertEquals("RENC_5", rs.getString("TABLE_NAME"));
            assertEquals("C2", rs.getString("COLUMN_NAME"));
            assertEquals("true", rs.getString("NON_UNIQUE"));
            assertEquals("3", rs.getString("TYPE"));
            assertEquals("A", rs.getString("ASC_OR_DESC"));
            assertEquals(null, rs.getString("CARDINALITY"));
            assertEquals(null, rs.getString("PAGES"));
            assertFalse(rs.next());
        }

        // Attempt to rename a column used in a check constraint 
        // should fail:
        assertStatementError(
                "42Z97", st,
                "rename column renc_4.c2 to some_other_name");

        // Attempt to rename a column used in a trigger should fail:
        st.executeUpdate(
                "create trigger renc_5_tr1 after update of c2, c3, " +
                "c6 on renc_4 for each row mode db2sql insert into " +
                "renc_5 (c6) values (1)");

        // This fails, because the tigger is dependent on it:
        assertStatementError(
                "X0Y25", st,
                "rename column renc_4.c6 to some_name");

        // This succeeds, because the trigger is not dependent on 
        // renc_5.c6. DERBY-2041 requests that triggers should be 
        // marked as dependent on tables and columns in their body. 
        // If that improvement is made, this test will need to be 
        // changed, as the next rename would fail, and the insert 
        // after it would then succeed.

        st.executeUpdate(
                "rename column renc_5.c6 to new_name");

        // The update statement will fail, because column c6 no 
        // longer exists. See DERBY-2041 for a discussion of this 
        // topic.

        st.executeUpdate(
                "insert into renc_4 values(1, 2, 3, 4, 5, 6)");

        assertStatementError(
                "42X14", st, "update renc_4 set c6 = 92");

        rs = st.executeQuery("select * from renc_5");

        JDBC.assertColumnNames(rs,
                new String[]{"C1",
                    "C2", "C3", "C4", "UNQ_C5", "NEW_NAME"
                });
        JDBC.assertDrainResults(rs,
                0);

        // Rename a column which has a granted privilege, show 
        // that the grant is properly processed and now applies to 
        // the new column:

        st.executeUpdate(
                "create table renc_6 (a int, b int, c int)");
        st.executeUpdate(
                "grant select (a, b) on renc_6 to eranda");

        rs = st.executeQuery(
                " select p.grantee,p.type, p.columns from " +
                "sys.syscolperms p, sys.systables t where " +
                "t.tableid=p.tableid and t.tablename='RENC_6'");

        JDBC.assertFullResultSet(rs, new String[][]{
                    {"ERANDA",
                        "s", "{0, 1}"
                    }
                });

        st.executeUpdate(
                "rename column renc_6.b to bb_gun");

        rs = st.executeQuery(
                " select p.grantee,p.type, p.columns from " +
                "sys.syscolperms p, sys.systables t where " +
                "t.tableid=p.tableid and t.tablename='RENC_6'");

        JDBC.assertFullResultSet(rs, new String[][]{
                    {"ERANDA",
                        "s", "{0, 1}"
                    }
                });

        // Attempt to rename a column should fail when there is an 
        // open cursor on it:

        PreparedStatement ps_renc_c1 = prepareStatement("select * from renc_6");
        ResultSet renc_c1 = ps_renc_c1.executeQuery();
        if (usingEmbedded()) // client/server doesn't keep cursor open.
        {
            assertStatementError("X0X95", st,
                    " rename column renc_6.bb_gun to water_pistol");
        }

        renc_c1.close();

        ps_renc_c1.close();

        // Attempt to rename a column when there is an open 
        // prepared statement on it. The rename of the column will 
        // be successful; the open statement will get errors when 
        // it tries to re-execute.
        conn.setAutoCommit(false);
        pSt = prepareStatement("select * from renc_6 where a = ?");
        rs = st.executeQuery("values (30)");

        rs.next();
        rsmd = rs.getMetaData();
        for (int i = 1;
                i <= rsmd.getColumnCount(); i++) {
            pSt.setObject(i, rs.getObject(i));
        }
        rs = pSt.executeQuery();
        expColNames = new String[]{"A", "BB_GUN", "C"};

        JDBC.assertColumnNames(rs, expColNames);

        JDBC.assertDrainResults(rs, 0);

        st.executeUpdate(
                " rename column renc_6.a to abcdef");
        rs = st.executeQuery(
                "values (30)");

        rs.next();
        rsmd = rs.getMetaData();
        for (int i = 1;
                i <= rsmd.getColumnCount(); i++) {
            pSt.setObject(i, rs.getObject(i));
        }
        assertStatementError(
                "42X04", pSt);

        conn.setAutoCommit(
                true);

        // Demonstrate that you cannot rename a column in a 
        // synonym, and demonstrate that renaming a column in the 
        // underlying table correctly renames it in the synonym too

        st.executeUpdate(
                "create table renc_7 (c1 varchar(50), c2 int)");
        st.executeUpdate(
                "create synonym renc_7_syn for renc_7");
        st.executeUpdate(
                "insert into renc_7 values ('one', 1)");

        assertStatementError(
                "42Y55", st,
                " rename column renc_7_syn.c2 to c2_syn");

        st.executeUpdate(
                "rename column renc_7.c1 to c1_renamed");

        rs = st.executeQuery("select c1_renamed from renc_7_syn");
        expColNames = new String[]{"C1_RENAMED"};

        JDBC.assertColumnNames(rs, expColNames);
        expRS = new String[][]{
                    {"one"}
                };

        JDBC.assertFullResultSet(rs, expRS, true);

        // demonstrate that you can rename a column in a table in 
        // a different schema
        st.executeUpdate("create schema renc_schema_1");

        st.executeUpdate("create schema renc_schema_2");

        st.executeUpdate("set schema renc_schema_2");

        st.executeUpdate("create table renc_8 (a int, b int, c int)");

        st.executeUpdate("set schema renc_schema_1");

        // This should fail, as there is no table renc_8 in schema 1:
        assertStatementError("42Y55", st, "rename column renc_8.b to bbb");

        // But this should work, and should find the table in the 
        // other schema

        st.executeUpdate(
                "rename column renc_schema_2.renc_8.b to b2");
        
        //DERBY-3823 While a resulset is still open, network server allows
        // ALTER TABLE to change the length of the column in the resultset,
        // but that length is not reflected in resultset's metadata. This
        // most likely is happening because of the pre-fetching by the 
        // server. Related jiras are DERBY-3839 and DERBY-4373.
        //Once DERBY-3823 is fixed, we should see the change in metadata
        // reflected in resultset's metadata. A fix for DERBY-3823 will
        // cause the following test to fail. Right now, the following
        // test accepts the incorrect metadata length obtained through
        // the resultset's metadata after ALTER TABLE has been performed.
        conn.setAutoCommit(false);
        //Create table and load data
        st.executeUpdate(
                "create table derby_3823_t1 (c11 int, c12 varchar(5))");
        PreparedStatement ps = prepareStatement(
        		"insert into derby_3823_t1 values(?,'aaaaa')");
        for (int i = 0; i < 1000; i++) { 
        	ps.setInt(1, i); 
        	ps.executeUpdate(); 
    	} 
        conn.commit();
        //Open a resultset on the table which will be altered because
        // the resultset has been exhausted. The alter table will fail
        // in embedded mode because of the open resulset but will succeed
        // in network server because of the pre-fetching.
        rs = st.executeQuery("select * from derby_3823_t1");
        //Just get first 100 rows rather than going through all the rows
        //Next, we will attempt to change the column length of one of the
        // columns in the resultset and see what happens
        for (int i = 0; i < 100; i++) { 
        	rs.next(); 
    	}
        rsmd = rs.getMetaData();
        //The column c12's length at this point is 2
        assertEquals(5, rsmd.getColumnDisplaySize(2));
        Statement st1 = createStatement();
        // This should fail, as c12's column length at this point is 2 and
        //  data being inserted is 8 characters in length
        assertStatementError("22001", st1, "insert into derby_3823_t1 values(99,'12345678')");
        if (usingEmbedded()) 
        {
        	//ALTER TABLE will fail in embedded because of the open resulset
            assertStatementError("X0X95", st1,
                    "alter table derby_3823_t1 alter column c12 set data type varchar(8)");
        } else {
        	//ALTER TABLE does not fail in network server because of pre-fetching
            st1.execute("alter table derby_3823_t1 alter column c12 set data type varchar(8)"); 
            //BUG - but the following metadata of the resultset does not show
            //  the new column length for C12 which is 8 rather than 2
            rsmd = rs.getMetaData(); 
            //Following is incorrect. The column length should have been 8
            // rather than 5
            assertEquals(5, rsmd.getColumnDisplaySize(2));
            //Following shows that we are able to enter 8character string after
            // alter table alter column. It is the resulset metadata which does
            // not reflect the change in length
            st1.executeUpdate("insert into derby_3823_t1 values(99,'12345678')"); 
        }
    }
    
    // DERBY-5120 Make sure that sysdepends will catch trigger
    //  table changes and cause the triggers defined on that
    //  table to recompile when they fire next time
    public void testAlterTableAndSysdepends() throws Exception {
        Statement st = createStatement();
        createTableAndInsertData(st, "Derby5120_tab", "C11", "C12");
        createTableAndInsertData(st, "Derby5120_tab_bkup1", "C111", "C112");
        createTableAndInsertData(st, "Derby5120_tab_bkup2", "C211", "C212");
        
        int sysdependsRowCountBeforeTestStart;

        sysdependsRowCountBeforeTestStart = numberOfRowsInSysdepends(st);
        //Following trigger will add 5 rows to sysdepends. Trigger creation
        // will send CREATE TRIGGER invalidation to trigger table but there
        // are no other persistent dependents on trigger table at this point.
        st.executeUpdate(
                " create trigger Derby5120_tr1 " +
                "after update of c11 on Derby5120_tab referencing  " +
                "old_table as old for each statement insert into " +
                "Derby5120_tab_bkup1 select * from old");
        Assert.assertEquals("# of rows in SYS.SYSDEPENDS should not change",
        		numberOfRowsInSysdepends(st),sysdependsRowCountBeforeTestStart+5);

        //Following trigger will add 5 rows to sysdepends. Trigger creation
        // will send CREATE TRIGGER invalidation to trigger table which will
        // invalidate trigger created earlier (Derby5120_tr1). Because of
        // this, when Derby5120_tr1 trigger fires next, it will be recompiled.
        st.executeUpdate(
                " create trigger Derby5120_tr2 " +
                "after update of c11 on Derby5120_tab referencing  " +
                "old as oldrow for each row insert into  " +
                "Derby5120_tab_bkup2(c211) values (oldrow.c11)");
        Assert.assertEquals("# of rows in SYS.SYSDEPENDS should not change",
        		numberOfRowsInSysdepends(st),sysdependsRowCountBeforeTestStart+10);

        //Following will fire the 2 triggers created above. During the firing,
        // we will find that Derby5120_tr1 has been marked invalid. As a result
        // we will recompile it's trigger action.
        st.executeUpdate("update Derby5120_tab set c11=2");
        Assert.assertEquals("# of rows in SYS.SYSDEPENDS should not change",
        		numberOfRowsInSysdepends(st),sysdependsRowCountBeforeTestStart+10);

        //Following alter table on trigger table will mark the two triggers 
        // created above invalid. As a result, when they are fired next
        // time, their trigger action sps will be regenerated.
        st.executeUpdate("alter table Derby5120_tab add column c113 int");
        Assert.assertEquals("# of rows in SYS.SYSDEPENDS should not change",
        		numberOfRowsInSysdepends(st),sysdependsRowCountBeforeTestStart+10);

        //Following will cause the 2 triggers to fire because they were marked
        // invalid by alter table. During the trigger action sps regeneration
        // of Derby5120_tr1, we will find that the trigger action sql is not
        // valid anymore because trigger table now has 3 columns where as
        // Derby5120_tab_bkup1 has only 2 columns and hence trigger action
        // sps will not be able to do insert into Derby5120_tab_bkup1 select *
        // from trigger table
        assertStatementError("42802", st, " update Derby5120_tab set c11=2");

        //Drop the errorneous trigger
        st.executeUpdate("drop trigger Derby5120_tr1");
        Assert.assertEquals("# of rows in SYS.SYSDEPENDS will be less",
        		numberOfRowsInSysdepends(st),sysdependsRowCountBeforeTestStart+5);

        //Following update will succeed this time
        st.executeUpdate("update Derby5120_tab set c11=2");
        Assert.assertEquals("# of rows in SYS.SYSDEPENDS should not change",
        		numberOfRowsInSysdepends(st),sysdependsRowCountBeforeTestStart+5);
    }
    
    //A test for ALTER TABLE DROP COLUMN with synonyms and trigger combination.
    // Trigger uses synonym in it's trigger action and when a column is 
    // dropped(in cascade mode), the trigger gets dropped because the synonym 
    // in it's trigger action relied on that column.
    public void testTriggersAndSynonyms() throws Exception {
        Statement st = createStatement();
        
        st.executeUpdate("create table atdcSynonymTab_1 (c11 integer, c12 integer)");
        st.executeUpdate("create table atdcSynonymTab_2 (c21 integer, c22 integer)");
		st.executeUpdate("CREATE SYNONYM synonymTab2 FOR atdcSynonymTab_2");
        st.executeUpdate(
                "create trigger syn_tr1t1 after update of c11 on atdcSynonymTab_1 " +
                "for each row mode db2sql " +
                "insert into atdcSynonymTab_2(c21, c22) values(9,9)");

        //Verify there is no data in tables before the start of the test
        JDBC.assertEmpty(st.executeQuery(
                " select * from atdcSynonymTab_1"));
        JDBC.assertEmpty(st.executeQuery(
                " select * from synonymTab2"));
        st.executeUpdate(
                " insert into atdcSynonymTab_1 values(11,12)");
        //Followng will fire the trigger and insert a row in table on which
        // there is a synonym defined
        st.executeUpdate(
                " update atdcSynonymTab_1 set c11=99");
        //A new row in the table with synonym defined on it
        JDBC.assertFullResultSet(
        		st.executeQuery("select * from synonymTab2"),
                new String[][]{{"9","9"}});
        //delete data to get ready for next test which will attempt to do
        // ALTER TABLE DROP COLUMN RESTRICT and fail because there is a
        // trigger using the column being dropped
        st.executeUpdate(
                " delete from atdcSynonymTab_1");
        st.executeUpdate(
                " delete from synonymTab2");
        
        //Following will fail because there is a trigger using that 
        // column
        assertStatementError("X0Y25", st,
                " alter table atdcSynonymTab_2 drop column c21 restrict");
        //Run through the trigger firing test again to see that trigger is
        // still intact
        JDBC.assertEmpty(st.executeQuery(
                " select * from atdcSynonymTab_1"));
        JDBC.assertEmpty(st.executeQuery(
                " select * from synonymTab2"));
        st.executeUpdate(
                " insert into atdcSynonymTab_1 values(11,12)");
        //Followng will fire the trigger and insert a row in table on which
        // there is a synonym defined
        st.executeUpdate(
                " update atdcSynonymTab_1 set c11=99");
        //A new row in the table with synonym defined on it
        JDBC.assertFullResultSet(
        		st.executeQuery("select * from synonymTab2"),
                new String[][]{{"9","9"}});
        //delete data to get ready for next test which will attempt to do
        // ALTER TABLE DROP COLUMN and will dropped the trigger using the 
        // column being dropped
        st.executeUpdate(
                " delete from atdcSynonymTab_1");
        st.executeUpdate(
                " delete from synonymTab2");
        
        //Following will drop three triggers using the column being dropped
        st.executeUpdate(
                " alter table atdcSynonymTab_2 drop column c21");
        //Run through the trigger firing test again and we will see the trigger
        // is not there anymore since no new row gets inserted through the
        // trigger
        JDBC.assertEmpty(st.executeQuery(
                " select * from atdcSynonymTab_1"));
        JDBC.assertEmpty(st.executeQuery(
                " select * from synonymTab2"));
        st.executeUpdate(
                " insert into atdcSynonymTab_1 values(11,12)");
        st.executeUpdate(
                " update atdcSynonymTab_1 set c11=99");
        //Will still be empty because trigger which would have caused a row
        // insertion got dropped as a result of ALTER TABLE DROP COLUMN
        JDBC.assertEmpty(st.executeQuery(
                " select * from synonymTab2"));
    }

    // Column being dropped is getting used in two triggers. A trigger defined
    //  on the table whose column is getting dropped and a trigger defined on
    //  another table but using the table whose column is getting dropped in
    //  it's trigger action
    public void testDropColumnTriggerDependency() throws Exception {
        Statement st = createStatement();
        createTestObjects(st);

        st.executeUpdate("create table atdctd_1 (c11 integer, c12 integer)");
        st.executeUpdate("create table atdctd_2 (c21 integer, c22 integer)");
        st.executeUpdate("create table atdctd_3 (c31 integer, c32 integer)");

        st.executeUpdate(
                "create trigger tr1t1 after update of c11 on atdctd_1 " +
                "for each row mode db2sql " +
                "insert into atdctd_3(c31, c32) values(9,9)");

        st.executeUpdate(
                "create trigger tr1t2 after insert on atdctd_2 " +
                "for each row mode db2sql " +
                "insert into atdctd_3(c31, c32) " +
                "select c11, c12 from atdctd_1");
        st.executeUpdate(
                "create trigger tr2t2 after insert on atdctd_2 " +
                "for each row mode db2sql " +
                "insert into atdctd_3(c31) " +
                "select c11 from atdctd_1");

        JDBC.assertEmpty(st.executeQuery(
                " select * from atdctd_3"));
        st.executeUpdate(
                " insert into atdctd_1 values(11,12)");
        st.executeUpdate(
                " update atdctd_1 set c11=99");
        JDBC.assertFullResultSet(
        		st.executeQuery("select * from atdctd_3"),
                new String[][]{{"9","9"}});
        st.executeUpdate(
                " insert into atdctd_2 values(21,22)");
        JDBC.assertFullResultSet(
        		st.executeQuery("select * from atdctd_3 order by c32"),
                new String[][]{{"9","9"}, {"99","12"},{"99",null}});
        st.executeUpdate(
                " delete from atdctd_3");
        st.executeUpdate(
                " delete from atdctd_1");
        st.executeUpdate(
                " delete from atdctd_2");
        
        //Following will fail because there are three triggers using that 
        // column
        assertStatementError("X0Y25", st,
                " alter table atdctd_1 drop column c11 restrict");
        JDBC.assertEmpty(st.executeQuery(
        		" select * from atdctd_3"));
        st.executeUpdate(
        		" insert into atdctd_1 values(11,12)");
        st.executeUpdate(
        		" update atdctd_1 set c11=99");
        JDBC.assertFullResultSet(
        		st.executeQuery("select * from atdctd_3"),
                new String[][]{{"9","9"}});
        st.executeUpdate(
                " insert into atdctd_2 values(21,22)");
        JDBC.assertFullResultSet(
        		st.executeQuery("select * from atdctd_3 order by c32"),
                new String[][]{{"9","9"}, {"99","12"},{"99",null}});
        st.executeUpdate(
                " delete from atdctd_3");
        st.executeUpdate(
                " delete from atdctd_1");
        st.executeUpdate(
                " delete from atdctd_2");
        
        //Following will drop three triggers using the column being dropped
        st.executeUpdate(
                " alter table atdctd_1 drop column c11");
        JDBC.assertEmpty(st.executeQuery(
                " select * from atdctd_3"));
        st.executeUpdate(
                " insert into atdctd_1 values(12)");
        st.executeUpdate(
                " update atdctd_1 set c12=99");
        //Will still be empty because trigger which would have added a row into
        // atdctd_3 got dropped as a result of ALTER TABLE DROP COLUMN earlier
        JDBC.assertEmpty(st.executeQuery(
                " select * from atdctd_3"));
        st.executeUpdate(
                " insert into atdctd_2 values(21,22)");
        //Will still be empty because triggers which would have added a row 
        // each into atdctd_3 got dropped as a result of ALTER TABLE DROP 
        // COLUMN earlier
        JDBC.assertEmpty(st.executeQuery(
                " select * from atdctd_3"));
    }

    // alter table tests for ALTER TABLE DROP COLUMN. The 
    // overall syntax is:    ALTER TABLE tablename DROP [ 
    // COLUMN ] columnname [ CASCADE | RESTRICT ]
    public void testDropColumn() throws Exception {
        Statement st = createStatement();
        createTestObjects(st);
        int sysdependsRowCountBeforeCreateTrigger;
        int sysdependsRowCountAfterCreateTrigger; 
        int countAfter1Trigger;
        int countAfter2Triggers;
        int countAfter3Triggers;
        int countAfter4Triggers;

        st.executeUpdate("create table atdc_0 (a integer)");
        st.executeUpdate("create table atdc_1 (a integer, b integer)");
        st.executeUpdate("insert into atdc_1 values (1, 1)");

        JDBC.assertFullResultSet(st.executeQuery(" select * from atdc_1"),
                new String[][]{{"1", "1"}});

        rs =
                st.executeQuery(
                " select columnname,columnnumber,columndatatype from " +
                "sys.syscolumns where referenceid in (select tableid " +
                "from sys.systables where tablename = 'ATDC_1')");

        JDBC.assertFullResultSet(rs,
                new String[][]{
                    {"A", "1", "INTEGER"},
                    {"B", "2", "INTEGER"}
                });

        st.executeUpdate("alter table atdc_1 drop column b");

        rs =
                st.executeQuery("select * from atdc_1");
        JDBC.assertColumnNames(rs, new String[]{"A"});
        JDBC.assertSingleValueResultSet(rs, "1");

        rs =
                st.executeQuery(
                " select columnname,columnnumber,columndatatype from " +
                "sys.syscolumns where referenceid in (select tableid " +
                "from sys.systables where tablename = 'ATDC_1')");
        JDBC.assertFullResultSet(rs, new String[][]{{"A", "1", "INTEGER"}});

        st.executeUpdate("alter table atdc_1 add column b varchar (20)");
        st.executeUpdate("insert into atdc_1 values (1, 'new val')");
        st.executeUpdate("insert into atdc_1 (a, b) values (2, 'two val')");

        rs =
                st.executeQuery("select * from atdc_1");
        JDBC.assertColumnNames(rs, new String[]{"A", "B"});
        JDBC.assertFullResultSet(rs,
                new String[][]{
                    {"1", null},
                    {"1", "new val"},
                    {"2", "two val"}
                });

        rs =
                st.executeQuery(
                " select columnname,columnnumber,columndatatype from " +
                "sys.syscolumns where referenceid in (select tableid " +
                "from sys.systables where tablename = 'ATDC_1')");

        JDBC.assertFullResultSet(rs,
                new String[][]{
                    {"A", "1", "INTEGER"},
                    {"B", "2", "VARCHAR(20)"}
                });

        st.executeUpdate("alter table atdc_1 add column c integer");
        st.executeUpdate("insert into atdc_1 values (3, null, 3)");

        rs =
                st.executeQuery("select * from atdc_1");
        JDBC.assertColumnNames(rs, new String[]{"A", "B", "C"});
        JDBC.assertFullResultSet(rs,
                new String[][]{
                    {"1", null, null},
                    {"1", "new val", null},
                    {"2", "two val", null},
                    {"3", null, "3"}
                });

        st.executeUpdate("alter table atdc_1 drop b");

        rs =
                st.executeQuery("select * from atdc_1");
        JDBC.assertColumnNames(rs, new String[]{"A", "C"});
        JDBC.assertFullResultSet(rs,
                new String[][]{
                    {"1", null},
                    {"1", null},
                    {"2", null},
                    {"3", "3"}
                });

        rs =
                st.executeQuery(
                " select columnname,columnnumber,columndatatype from " +
                "sys.syscolumns where referenceid in (select tableid " +
                "from sys.systables where tablename = 'ATDC_1')");

        JDBC.assertFullResultSet(rs,
                new String[][]{
                    {"A", "1", "INTEGER"},
                    {"C", "2", "INTEGER"}
                });

        // Demonstrate that we can drop a column which is the 
        // primary key. Also demonstrate that when we drop a column 
        // which is the primary key, that cascade processing will 
        // drop the corresponding foreign key constraint

        st.executeUpdate(
                "create table atdc_1_01 (a int, b int, c int not " +
                "null primary key)");

        st.executeUpdate("alter table atdc_1_01 drop column c cascade");

        if (usingEmbedded()) {
            if ((sqlWarn == null) && (st != null)) {
                sqlWarn = st.getWarnings();
            }

            if (sqlWarn == null) {
                sqlWarn = getConnection().getWarnings();
            }

            assertNotNull("Expected warning but found none", sqlWarn);
            assertSQLState("01500", sqlWarn);
            sqlWarn =
                    null;
        }

        st.executeUpdate(
                " create table atdc_1_02 (a int not null primary key, b int)");

        st.executeUpdate(
                " create table atdc_1_03 (a03 int, constraint a03_fk " +
                "foreign key (a03) references atdc_1_02(a))");

        st.executeUpdate(
                " alter table atdc_1_02 drop column a cascade");

        if (usingEmbedded()) {
            if ((sqlWarn == null) && (st != null)) {
                sqlWarn = st.getWarnings();
            }

            if (sqlWarn == null) {
                sqlWarn = getConnection().getWarnings();
            }

            assertNotNull("Expected warning but found none", sqlWarn);
            assertSQLState("01500", sqlWarn);
            sqlWarn =
                    null;
        }

        if (usingEmbedded()) {
            if ((sqlWarn == null) && (st != null)) {
                sqlWarn = st.getWarnings();
            }

            if (sqlWarn == null) {
                sqlWarn = getConnection().getWarnings();
            }

            assertNotNull("Expected warning but found none", sqlWarn);
            assertSQLState("01500", sqlWarn);
            sqlWarn =
                    null;
        }

// drop column restrict should fail because column is used 
// in a constraint:
        st.executeUpdate(
                "alter table atdc_1 add constraint atdc_constraint_1 " +
                "check (a > 0)");

        rs =
                st.executeQuery(
                " select CONSTRAINTNAME,TYPE,STATE,REFERENCECOUNT " +
                "from sys.sysconstraints where tableid in " +
                "(select tableid from sys.systables where tablename " +
                "= 'ATDC_1')");
        JDBC.assertFullResultSet(rs, new String[][]{
                    {"ATDC_CONSTRAINT_1", "C", "E", "0"}
                });

        rs =
                st.executeQuery(
                " select sc.CHECKDEFINITION,sc.REFERENCEDCOLUMNS " +
                "from sys.syschecks sc,sys.sysconstraints con, " +
                " sys.systables st where " +
                "sc.constraintid = con.constraintid and con.tableid " +
                "= st.tableid and st.tablename = 'ATDC_1'");
        JDBC.assertFullResultSet(rs, new String[][]{
                    {"(a > 0)", "(1)"}
                });

        assertStatementError("X0Y25", st,
                " alter table atdc_1 drop column a restrict");

        // drop column cascade should also drop the check constraint:

        st.executeUpdate("alter table atdc_1 drop column a cascade");
        checkWarning(st, "01500");

        rs =
                st.executeQuery(
                " select * from sys.sysconstraints where tableid in " +
                "(select tableid from sys.systables where tablename " +
                "= 'ATDC_1')");
        JDBC.assertDrainResults(rs, 0);

        // Verify the behavior of the various constraint types: 
        // check, primary key, foreign key, unique, not null

        st.executeUpdate(
                "create table atdc_1_constraints (a int not null " +
                "primary key, b int not null, c int constraint " +
                "atdc_1_c_chk check (c is not null), d int not null " +
                "unique, e int, f int, constraint atdc_1_e_fk " +
                "foreign key (e) references atdc_1_constraints(a))");

        // In restrict mode, none of the columns a, c, d, or e 
        // should be droppable, but in cascade mode each of them 
        // should be droppable, and at the end we should have only 
        // column f column b is droppable because an unnamed NOT 
        // NULL constraint doesn't prevent DROP COLUMN, only an 
        // explicit CHECK constraint does.


        assertStatementError("X0Y25", st,
                " alter table atdc_1_constraints drop column a restrict");

        st.executeUpdate(
                " alter table atdc_1_constraints drop column b restrict");

        assertStatementError("X0Y25", st,
                " alter table atdc_1_constraints drop column c restrict");

        assertStatementError("X0Y25", st,
                " alter table atdc_1_constraints drop column d restrict");

        assertStatementError("X0Y25", st,
                " alter table atdc_1_constraints drop column e restrict");

        st.executeUpdate(
                "alter table atdc_1_constraints drop column a cascade");
        checkWarning(st, "01500");
        st.executeUpdate(
                " alter table atdc_1_constraints drop column c cascade");
        checkWarning(st, "01500");
        st.executeUpdate(
                " alter table atdc_1_constraints drop column d cascade");
        checkWarning(st, "01500");
        st.executeUpdate(
                " alter table atdc_1_constraints drop column e cascade");

        // Some negative testing of ALTER TABLE DROP COLUMN Table 
        // does not exist:

        assertStatementError("42Y55", st,
                "alter table atdc_nosuch drop column a");

        // Table exists, but column does not exist:

        st.executeUpdate("create table atdc_2 (a integer)");
        assertStatementError("42X14", st, "alter table atdc_2 drop column b");
        assertStatementError("42X14", st, "alter table atdc_2 drop b");

        // Column name is spelled incorrectly (wrong case)

        assertStatementError("42X01", st, "alter table atdc_2 drop column 'a'");

        //Some special reserved words to cause parser errors
        assertStatementError("42X01", st,
                "alter table atdc_2 drop column column");

        assertStatementError("42X01", st, "alter table atdc_2 drop column");

        assertStatementError("42X01", st,
                " alter table atdc_2 drop column constraint");

        assertStatementError("42X01", st,
                " alter table atdc_2 drop column primary");

        assertStatementError("42X01", st,
                " alter table atdc_2 drop column foreign");

        assertStatementError("42X01", st,
                " alter table atdc_2 drop column check");

        st.executeUpdate("create table atdc_3 (a integer)");
        st.executeUpdate("create index atdc_3_idx_1 on atdc_3 (a)");

        // This fails because a is the only column in the table.

        assertStatementError("X0Y25", st,
                "alter table atdc_3 drop column a restrict");

        st.executeUpdate("drop index atdc_3_idx_1");

        // cascade/restrict processing doesn't currently consider 
        // indexes. The column being dropped is automatically 
        // dropped from all indexes as well. If that was the only 
        // (last) column in the index, then the index is dropped, too.

        st.executeUpdate(
                "create table atdc_4 (a int, b int, c int, d int, e int)");

        st.executeUpdate("insert into atdc_4 values (1,2,3,4,5)");
        st.executeUpdate("create index atdc_4_idx_1 on atdc_4 (a)");
        st.executeUpdate("create index atdc_4_idx_2 on atdc_4 (b, c, d)");
        st.executeUpdate("create index atdc_4_idx_3 on atdc_4 (c, a)");

        rs =
                st.executeQuery(
                " select conglomeratename,isindex from " +
                "sys.sysconglomerates where tableid in (select " +
                "tableid from sys.systables where tablename = 'ATDC_4') " +
                "and isindex='true'");

        JDBC.assertFullResultSet(rs, new String[][]{
                    {"ATDC_4_IDX_1", "true"},
                    {"ATDC_4_IDX_2", "true"},
                    {"ATDC_4_IDX_3", "true"}
                });


        // This succeeds, because cascade/restrict doesn't matter 
        // for indexes. The effect of dropping column a is that:    
        // index atdc_4_idx_1 is entirely dropped    index 
        // atdc_4_idx_2 is left alone but the column positions are 
        // fixed up    index atdc_4_idx_3 is modified to refer only 
        // to column c

        st.executeUpdate("alter table atdc_4 drop column a restrict");

        rs =
                st.executeQuery(
                " select conglomeratename,isindex from " +
                "sys.sysconglomerates where tableid in (select " +
                "tableid from sys.systables where tablename = 'ATDC_4')" +
                " and isindex='true'");
        JDBC.assertFullResultSet(rs, new String[][]{
                    {"ATDC_4_IDX_2", "true"},
                    {"ATDC_4_IDX_3", "true"}
                });

        // The effect of dropping column c is that:    index 
        // atdc_4_idx_2 is modified to refer to columns b and d    
        // index atdc_4_idx_3 is entirely dropped

        st.executeUpdate("alter table atdc_4 drop column c restrict");

        assertStatementError("42X04", st, "select * from atdc_4 where c = 3");

        rs =
                st.executeQuery(
                " select count(*) from sys.sysconglomerates where " +
                "conglomeratename='ATDC_4_IDX_2'");
        JDBC.assertSingleValueResultSet(rs, "1");

        rs =
                st.executeQuery(
                " select conglomeratename, isindex from " +
                "sys.sysconglomerates where conglomeratename like 'ATDC_4%'");

        JDBC.assertFullResultSet(rs, new String[][]{
                    {"ATDC_4_IDX_2", "true"}
                });

        st.executeUpdate("drop index atdc_4_idx_2");

        // drop column restrict should fail becuase column is used in a view:

        st.executeUpdate("create table atdc_5 (a int, b int)");

        st.executeUpdate(
                " create view atdc_vw_1 (vw_b) as select b from atdc_5");

        assertStatementError("X0Y23", st,
                " alter table atdc_5 drop column b restrict");

        rs =
                st.executeQuery("select * from atdc_vw_1");

        expColNames =
                new String[]{"VW_B"};
        JDBC.assertColumnNames(rs, expColNames);
        JDBC.assertDrainResults(rs, 0);

        // drop column cascade drops the column, and also drops 
        // the dependent view:

        st.executeUpdate("alter table atdc_5 drop column b cascade");
        checkWarning(st, "01501");

        assertStatementError("42X05", st, "select * from atdc_vw_1");

        // cascade processing should transitively drop a view 
        // dependent on a view dependent in turn on the column 
        // being dropped:

        st.executeUpdate("create table atdc_5a (a int, b int, c int)");

        st.executeUpdate(
                " create view atdc_vw_5a_1 (vw_5a_b, vw_5a_c) as " +
                "select b,c from atdc_5a");

        st.executeUpdate(
                " create view atdc_vw_5a_2 (vw_5a_c_2) as select " +
                "vw_5a_c from atdc_vw_5a_1");

        st.executeUpdate("alter table atdc_5a drop column b cascade");
        checkWarning(st, "01501");

        assertStatementError("42X05", st, "select * from atdc_vw_5a_1");

        assertStatementError("42X05", st, "select * from atdc_vw_5a_2");

        // Another test
        // drop column restrict should fail because trigger is defined to 
        // fire on the update of the column. But cascade should succeed
        // and drop the dependent trigger
        createTableAndInsertData(st, "ATDC_6", "A", "B");
        sysdependsRowCountBeforeCreateTrigger = numberOfRowsInSysdepends(st);
        st.executeUpdate(
                " create trigger atdc_6_trigger_1 after update of b " +
                "on atdc_6 for each row values current_date");
        sysdependsRowCountAfterCreateTrigger = numberOfRowsInSysdepends(st);

        assertStatementError("X0Y25", st,
                " alter table atdc_6 drop column b restrict");
        triggersExist(st, new String[][]{{"ATDC_6_TRIGGER_1"}});
        Assert.assertEquals("# of rows in SYS.SYSDEPENDS should not change",
        		numberOfRowsInSysdepends(st),sysdependsRowCountAfterCreateTrigger);

        //CASCADE will drop the dependent trigger
        st.executeUpdate("alter table atdc_6 drop column b cascade");
        checkWarning(st, "01502");
        JDBC.assertEmpty(st.executeQuery(
                " select triggername from sys.systriggers where " +
                "triggername='ATDC_6_TRIGGER_1'"));
        Assert.assertEquals("# of rows in SYS.SYSDEPENDS should reduce",
        		numberOfRowsInSysdepends(st),sysdependsRowCountBeforeCreateTrigger);
        st.executeUpdate("drop table ATDC_6");

        // Another test
        // drop column restrict should fail because trigger is defined to 
        // fire on the update of the column and it is also used in trigger
        // action. But cascade should succeed and drop the dependent trigger
        createTableAndInsertData(st, "ATDC_11", "A", "B");
        sysdependsRowCountBeforeCreateTrigger = numberOfRowsInSysdepends(st);
        st.executeUpdate(
                " create trigger atdc_11_trigger_1 after update of b " +
                "on atdc_11 for each row select a,b from atdc_11");
        sysdependsRowCountAfterCreateTrigger = numberOfRowsInSysdepends(st);

        assertStatementError("X0Y25", st,
                " alter table atdc_11 drop column b restrict");
        triggersExist(st, new String[][]{{"ATDC_11_TRIGGER_1"}});
        Assert.assertEquals("# of rows in SYS.SYSDEPENDS should not change",
        		numberOfRowsInSysdepends(st),sysdependsRowCountAfterCreateTrigger);

        //CASCADE will drop the dependent trigger
        st.executeUpdate("alter table atdc_11 drop column b cascade");
        checkWarning(st, "01502");
        JDBC.assertEmpty(st.executeQuery(
                " select triggername from sys.systriggers where " +
                "triggername='ATDC_11_TRIGGER_1'"));
        Assert.assertEquals("# of rows in SYS.SYSDEPENDS should reduce",
        		numberOfRowsInSysdepends(st),sysdependsRowCountBeforeCreateTrigger);
        st.executeUpdate("drop table ATDC_11");

        // Another test
        // drop column restrict should fail because trigger uses the column 
        // inside the trigger action. 
        createTableAndInsertData(st, "ATDC_12", "A", "B");
        sysdependsRowCountBeforeCreateTrigger = numberOfRowsInSysdepends(st);
        st.executeUpdate(
                " create trigger atdc_12_trigger_1 after update of a " +
                "on atdc_12 for each row select a,b from atdc_12");
        st.executeUpdate(
                " create trigger atdc_12_trigger_2 " +
                " after update of a on atdc_12" +
                " REFERENCING NEW AS newt OLD AS oldt "+
                " for each row select oldt.b from atdc_12");
        sysdependsRowCountAfterCreateTrigger = numberOfRowsInSysdepends(st);

        // We got an error because Derby detected the dependency of 
        // the triggers
        assertStatementError("X0Y25", st,
        		"alter table atdc_12 drop column b restrict");
        triggersExist(st, new String[][]{{"ATDC_12_TRIGGER_1"},
        		{"ATDC_12_TRIGGER_2"}});
        Assert.assertEquals("# of rows in SYS.SYSDEPENDS should not change",
        		numberOfRowsInSysdepends(st),sysdependsRowCountAfterCreateTrigger);

        //Now try ALTER TABLE DROP COLUMN CASCADE where the column being
        //dropped is in trigger action but is not part of the trigger
        //column list
        st.executeUpdate("alter table atdc_12 drop column b");
        checkWarning(st, "01502");
        // the 2 triggers will get dropped as a result of cascade
        JDBC.assertEmpty(st.executeQuery(
        		" select triggername from sys.systriggers where " +
        		"triggername in ('ATDC_12_TRIGGER_1', 'ATDC_12_TRIGGER_2')"));
        Assert.assertEquals("# of rows in SYS.SYSDEPENDS should reduce",
        		numberOfRowsInSysdepends(st),sysdependsRowCountBeforeCreateTrigger);
        st.executeUpdate("drop table ATDC_12");

        // Another test
        // drop column restrict should fail because there is a table level
        // trigger defined with the column being dropped in it's trigger
        // action. 
        createTableAndInsertData(st, "ATDC_13", "A", "B");
        sysdependsRowCountBeforeCreateTrigger = numberOfRowsInSysdepends(st);
        st.executeUpdate(
                " create trigger atdc_13_trigger_1 after update " +
                "on atdc_13 for each row select a,b from atdc_13");
        st.executeUpdate(
                " create trigger atdc_13_trigger_2 after insert " +
                "on atdc_13 for each row select a,b from atdc_13");
        st.executeUpdate(
                " create trigger atdc_13_trigger_3 after delete " +
                "on atdc_13 for each row select a,b from atdc_13");
        st.executeUpdate(
                " create trigger atdc_13_trigger_4 after update on atdc_13 " +
                " REFERENCING NEW AS newt OLD AS oldt "+
                " for each row select oldt.b, newt.b from atdc_13");
        st.executeUpdate(
                " create trigger atdc_13_trigger_5 after insert on atdc_13 " +
                " REFERENCING NEW AS newt "+
                " for each row select newt.b from atdc_13");
        st.executeUpdate(
                " create trigger atdc_13_trigger_6 after delete on atdc_13 " +
                " REFERENCING OLD AS oldt "+
                " for each row select oldt.b from atdc_13");
        sysdependsRowCountAfterCreateTrigger = numberOfRowsInSysdepends(st);

        assertStatementError("X0Y25", st,
        		"alter table atdc_13 drop column b restrict");
        triggersExist(st, new String[][]{{"ATDC_13_TRIGGER_1"},
            	{"ATDC_13_TRIGGER_2"}, {"ATDC_13_TRIGGER_3"},
            	{"ATDC_13_TRIGGER_4"}, {"ATDC_13_TRIGGER_5"},
            	{"ATDC_13_TRIGGER_6"}});
        Assert.assertEquals("# of rows in SYS.SYSDEPENDS should not change",
        		numberOfRowsInSysdepends(st),sysdependsRowCountAfterCreateTrigger);
        
        // Derby will drop all the 6 triggers
        st.executeUpdate("alter table atdc_13 drop column b");
        checkWarning(st, "01502");
        JDBC.assertEmpty(st.executeQuery(
        		" select triggername from sys.systriggers where " +
        		"triggername in ('ATDC_13_TRIGGER_1', "+
        		"'ATDC_13_TRIGGER_2', 'ATDC_13_TRIGGER_3'," +
        		"'ATDC_13_TRIGGER_4', 'ATDC_13_TRIGGER_5'," +
        		"'ATDC_13_TRIGGER_6')"));        
        Assert.assertEquals("# of rows in SYS.SYSDEPENDS should reduce",
        		numberOfRowsInSysdepends(st),sysdependsRowCountBeforeCreateTrigger);
        st.executeUpdate("drop table ATDC_13");
        
        // Another test DERBY-5044
        // ALTER TABLE DROP COLUMN in following test case causes the column
        // position of trigger column to change. Derby detects that dependency
        // and fixes the trigger column position
        st.executeUpdate("create table atdc_16_tab1 (a1 integer, b1 integer, c1 integer)");
        st.executeUpdate("create table atdc_16_tab2 (a2 integer, b2 integer, c2 integer)");        
        st.executeUpdate("insert into atdc_16_tab1 values(1,11,111)");
        st.executeUpdate("insert into atdc_16_tab2 values(1,11,111)");
        rs =
            st.executeQuery(" select * from atdc_16_tab1");
        JDBC.assertFullResultSet(rs, new String[][]{{"1","11","111"}});
        rs =
            st.executeQuery(" select * from atdc_16_tab2");
        JDBC.assertFullResultSet(rs, new String[][]{{"1","11","111"}});

        sysdependsRowCountBeforeCreateTrigger = numberOfRowsInSysdepends(st);
        st.executeUpdate(
                " create trigger atdc_16_trigger_1 " +
                " after update of b1 on atdc_16_tab1" +
                " REFERENCING NEW AS newt"+
                " for each row " +
                " update atdc_16_tab2 set c2 = newt.c1");
        sysdependsRowCountAfterCreateTrigger = numberOfRowsInSysdepends(st);
        st.executeUpdate("update atdc_16_tab1 set b1=22,c1=222");
        rs =
            st.executeQuery(" select * from atdc_16_tab1");
        JDBC.assertFullResultSet(rs, new String[][]{{"1","22","222"}});
        rs =
            st.executeQuery(" select * from atdc_16_tab2");
        JDBC.assertFullResultSet(rs, new String[][]{{"1","11","222"}});
        st.executeUpdate("alter table atdc_16_tab1 drop column a1 restrict");
        st.executeUpdate("update atdc_16_tab1 set b1=33, c1=333");
        rs =
            st.executeQuery(" select * from atdc_16_tab1");
        JDBC.assertFullResultSet(rs, new String[][]{{"33","333"}});
        rs =
            st.executeQuery(" select * from atdc_16_tab2");
        JDBC.assertFullResultSet(rs, new String[][]{{"1","11","333"}});
        Assert.assertEquals("# of rows in SYS.SYSDEPENDS should reduce",
              		numberOfRowsInSysdepends(st),sysdependsRowCountAfterCreateTrigger);
        st.executeUpdate("drop table ATDC_16_TAB1");
        st.executeUpdate("drop table ATDC_16_TAB2");
        
        // Another test DERBY-5044
        //Following test case involves two tables. The trigger is defined 
        //on table 1 and it uses the column from table 2 in it's trigger  
    	//action. 
        createTableAndInsertData(st, "ATDC_14_TAB1", "A1", "B1");
        createTableAndInsertData(st, "ATDC_14_TAB2", "A2", "B2");
        sysdependsRowCountBeforeCreateTrigger = numberOfRowsInSysdepends(st);
        st.executeUpdate(
                " create trigger atdc_14_trigger_1 after update " +
                "on atdc_14_tab1 REFERENCING NEW AS newt " +
                "for each row " +
                "update atdc_14_tab2 set a2 = newt.a1");
        sysdependsRowCountAfterCreateTrigger = numberOfRowsInSysdepends(st);

        assertStatementError("X0Y25", st,
		"alter table atdc_14_tab2 drop column a2 restrict");
        triggersExist(st, new String[][]{{"ATDC_14_TRIGGER_1"}});

        //Now try ALTER TABLE DROP COLUMN CASCADE where the column being
        //dropped is in trigger action of trigger defined on a different table
        st.executeUpdate("alter table atdc_14_tab2 drop column a2");
        checkWarning(st, "01502");
        // the trigger will get dropped as a result of cascade
        JDBC.assertEmpty(st.executeQuery(
        		" select triggername from sys.systriggers where " +
        		"triggername in ('ATDC_14_TRIGGER_1')"));
        Assert.assertEquals("# of rows in SYS.SYSDEPENDS should not change",
        		numberOfRowsInSysdepends(st),sysdependsRowCountBeforeCreateTrigger);
        st.executeUpdate("drop table ATDC_14_TAB1");
        st.executeUpdate("drop table ATDC_14_TAB2");

        // Start of another test for DERBY-5044
        createTableAndInsertData(st, "ATDC_13_TAB1", "C11", "C12");
        createTableAndInsertData(st, "ATDC_13_TAB1_BACKUP", "C11", "C12");
        createTableAndInsertData(st, "ATDC_13_TAB2", "C21", "C22");
        createTableAndInsertData(st, "ATDC_13_TAB3", "C31", "C32");
        sysdependsRowCountBeforeCreateTrigger = numberOfRowsInSysdepends(st);
        st.executeUpdate(
                " create trigger ATDC_13_TAB1_trigger_1 after update " +
                "on ATDC_13_TAB1 for each row " +
                "INSERT INTO ATDC_13_TAB1_BACKUP " +
                " SELECT C31, C32 from ATDC_13_TAB3");
        st.executeUpdate(
                " create trigger ATDC_13_TAB1_trigger_2 after update " +
                "on ATDC_13_TAB1 for each row " +
                "INSERT INTO ATDC_13_TAB1_BACKUP " +
                " SELECT * from ATDC_13_TAB3");
        countAfter2Triggers = numberOfRowsInSysdepends(st);
        st.executeUpdate(
                " create trigger ATDC_13_TAB1_trigger_3 after update " +
                "on ATDC_13_TAB1 for each row " +
                "INSERT INTO ATDC_13_TAB1_BACKUP VALUES(1,1)");
        int countAfter3rdTrigger = numberOfRowsInSysdepends(st);
        st.executeUpdate(
                " create trigger ATDC_13_TAB1_trigger_4 after update " +
                "on ATDC_13_TAB1 for each row " +
                "INSERT INTO ATDC_13_TAB1_BACKUP(C11) " +
                " SELECT C21 from ATDC_13_TAB2");
        int countAfter4thTrigger = numberOfRowsInSysdepends(st);
        sysdependsRowCountAfterCreateTrigger = numberOfRowsInSysdepends(st);
        Assert.assertEquals("# of rows in SYS.SYSDEPENDS should not change",
        		numberOfRowsInSysdepends(st),sysdependsRowCountAfterCreateTrigger);
        st.executeUpdate("update ATDC_13_TAB1 set c12=11");
        Assert.assertEquals("# of rows in SYS.SYSDEPENDS should not change",
        		numberOfRowsInSysdepends(st),sysdependsRowCountAfterCreateTrigger);
        rs = st.executeQuery("select * from ATDC_13_TAB1_BACKUP ORDER BY C11, C12");
        JDBC.assertFullResultSet(rs, new String[][]{
        		{"1","1"}, {"1","11"}, {"1","11"}, {"1","11"}, {"1",null} });
        st.executeUpdate("delete from ATDC_13_TAB1_BACKUP");

        assertStatementError("X0Y25", st,
		"alter table ATDC_13_TAB2 drop column c21 restrict");
        triggersExist(st, new String[][]{{"ATDC_13_TAB1_TRIGGER_1"},
            	{"ATDC_13_TAB1_TRIGGER_2"}, {"ATDC_13_TAB1_TRIGGER_3"},
            	{"ATDC_13_TAB1_TRIGGER_4"}});
        Assert.assertEquals("# of rows in SYS.SYSDEPENDS should not change",
        		numberOfRowsInSysdepends(st),sysdependsRowCountAfterCreateTrigger);
        st.executeUpdate("drop table ATDC_13_TAB1_BACKUP");
        st.executeUpdate("drop table ATDC_13_TAB1");
        st.executeUpdate("drop table ATDC_13_TAB2");
        st.executeUpdate("drop table ATDC_13_TAB3");
        
        // Start of another test for DERBY-5044. Test INSERT/DELETE/UPDATE
        // inside the trigger action from base tables
        createTableAndInsertData(st, "ATDC_13_TAB1", "C11", "C12");
        createTableAndInsertData(st, "ATDC_13_TAB1_BACKUP", "C11", "C12");
        createTableAndInsertData(st, "ATDC_13_TAB2", "C21", "C22");
        createTableAndInsertData(st, "ATDC_13_TAB3", "C31", "C32");
        sysdependsRowCountBeforeCreateTrigger = numberOfRowsInSysdepends(st);
        
        //Test triggers with trigger action doing INSERT
        st.executeUpdate(
                " create trigger ATDC_13_TAB1_trigger_1 after update " +
                "on ATDC_13_TAB1 for each row " +
                "INSERT INTO ATDC_13_TAB1_BACKUP " +
                " SELECT C31, C32 from ATDC_13_TAB3");
        st.executeUpdate(
                " create trigger ATDC_13_TAB1_trigger_2 after update " +
                "on ATDC_13_TAB1 for each row " +
                "INSERT INTO ATDC_13_TAB1_BACKUP " +
                " SELECT * from ATDC_13_TAB3");
        countAfter2Triggers = numberOfRowsInSysdepends(st);
        st.executeUpdate(
                " create trigger ATDC_13_TAB1_trigger_3 after update " +
                "on ATDC_13_TAB1 for each row " +
                "INSERT INTO ATDC_13_TAB1_BACKUP VALUES(1,1)");
        countAfter3Triggers = numberOfRowsInSysdepends(st);
        st.executeUpdate(
                " create trigger ATDC_13_TAB1_trigger_4 after update " +
                "on ATDC_13_TAB1 for each row " +
                "INSERT INTO ATDC_13_TAB1_BACKUP(C11) " +
                " SELECT C21 from ATDC_13_TAB2");
        countAfter4Triggers = numberOfRowsInSysdepends(st);
        sysdependsRowCountAfterCreateTrigger = numberOfRowsInSysdepends(st);
        st.executeUpdate("update ATDC_13_TAB1 set c12=11");
        Assert.assertEquals("# of rows in SYS.SYSDEPENDS should not change",
        		numberOfRowsInSysdepends(st),sysdependsRowCountAfterCreateTrigger);
        rs = st.executeQuery("select * from ATDC_13_TAB1_BACKUP ORDER BY C11, C12");
        JDBC.assertFullResultSet(rs, new String[][]{
        		{"1","1"}, {"1","11"}, {"1","11"}, {"1","11"}, {"1",null} });
        st.executeUpdate("delete from ATDC_13_TAB1_BACKUP");
        //We will get an error because column being dropped is getting used 
        // in a trigger action 
        assertStatementError("X0Y25", st,
        		"alter table ATDC_13_TAB2 drop column c21 restrict");
        triggersExist(st, new String[][]{{"ATDC_13_TAB1_TRIGGER_1"},
            	{"ATDC_13_TAB1_TRIGGER_2"}, {"ATDC_13_TAB1_TRIGGER_3"},
            	{"ATDC_13_TAB1_TRIGGER_4"}});
        Assert.assertEquals("# of rows in SYS.SYSDEPENDS should not change",
        		numberOfRowsInSysdepends(st),sysdependsRowCountAfterCreateTrigger);
        
        // We will drop the dependent triggers  
        st.executeUpdate("alter table ATDC_13_TAB2 drop column c21");
        checkWarning(st, "01502");
        triggersExist(st, new String[][]{{"ATDC_13_TAB1_TRIGGER_1"},
            	{"ATDC_13_TAB1_TRIGGER_2"}, {"ATDC_13_TAB1_TRIGGER_3"}});
        Assert.assertEquals("# of rows in SYS.SYSDEPENDS should reduce",
        		numberOfRowsInSysdepends(st),countAfter3Triggers);
        st.executeUpdate("alter table ATDC_13_TAB2 add column c21 int");
        
        //We will get an error because column being dropped is getting used 
        // in a trigger action 
        assertStatementError("X0Y25", st,
        		"alter table ATDC_13_TAB1_BACKUP drop column c11 restrict");
        triggersExist(st, new String[][]{{"ATDC_13_TAB1_TRIGGER_1"},
            	{"ATDC_13_TAB1_TRIGGER_2"}, {"ATDC_13_TAB1_TRIGGER_3"}});
        Assert.assertEquals("# of rows in SYS.SYSDEPENDS should not change",
        		numberOfRowsInSysdepends(st),countAfter3Triggers);
        
        // We will drop the dependent triggers  
        st.executeUpdate("alter table ATDC_13_TAB1_BACKUP drop column c11");
        Assert.assertEquals("# of rows in SYS.SYSDEPENDS should not change",
        		numberOfRowsInSysdepends(st),sysdependsRowCountBeforeCreateTrigger);
        st.executeUpdate("alter table ATDC_13_TAB1_BACKUP add column c11 int");
        
        //Test triggers with trigger action doing UPDATE
        sysdependsRowCountBeforeCreateTrigger = numberOfRowsInSysdepends(st);
        st.executeUpdate(
                " create trigger ATDC_13_TAB1_trigger_1 after update " +
                "on ATDC_13_TAB1 for each row " +
                "UPDATE ATDC_13_TAB1_BACKUP SET C11=123 " +
                "WHERE C12>1");
        countAfter1Trigger = numberOfRowsInSysdepends(st);
        st.executeUpdate(
                " create trigger ATDC_13_TAB1_trigger_2 after update " +
                "on ATDC_13_TAB1 for each row " +
                "UPDATE ATDC_13_TAB2 SET C21=123");
        countAfter2Triggers = numberOfRowsInSysdepends(st);
        st.executeUpdate(
                " create trigger ATDC_13_TAB1_trigger_3 after update " +
                "on ATDC_13_TAB1 for each row " +
                "UPDATE ATDC_13_TAB3 SET C31=123 WHERE "+
                "C32 IN (values(1))");
        countAfter3Triggers = numberOfRowsInSysdepends(st);
        sysdependsRowCountAfterCreateTrigger = numberOfRowsInSysdepends(st);

        assertStatementError("X0Y25", st,
        		"alter table ATDC_13_TAB3 drop column c31 restrict");
        triggersExist(st, new String[][]{{"ATDC_13_TAB1_TRIGGER_1"},
            	{"ATDC_13_TAB1_TRIGGER_2"}, {"ATDC_13_TAB1_TRIGGER_3"}});
        Assert.assertEquals("# of rows in SYS.SYSDEPENDS should not change",
        		numberOfRowsInSysdepends(st),sysdependsRowCountAfterCreateTrigger);
        
        // We will drop the dependent trigger
        st.executeUpdate("alter table ATDC_13_TAB3 drop column c31");
        triggersExist(st, new String[][]{{"ATDC_13_TAB1_TRIGGER_1"},
            	{"ATDC_13_TAB1_TRIGGER_2"}});
        Assert.assertEquals("# of rows in SYS.SYSDEPENDS should reduce",
        		numberOfRowsInSysdepends(st),countAfter2Triggers);
        // After DERBY-5044 is fixed, following should be rewritten
        st.executeUpdate("alter table ATDC_13_TAB3 add column c31 int");

        assertStatementError("X0Y25", st,
        		"alter table ATDC_13_TAB2 drop column c21 restrict");
        triggersExist(st, new String[][]{{"ATDC_13_TAB1_TRIGGER_1"},
            	{"ATDC_13_TAB1_TRIGGER_2"}});
        Assert.assertEquals("# of rows in SYS.SYSDEPENDS should not change",
        		numberOfRowsInSysdepends(st),countAfter2Triggers);
        
        // We will drop the dependent trigger
        st.executeUpdate("alter table ATDC_13_TAB2 drop column c21");
        triggersExist(st, new String[][]{{"ATDC_13_TAB1_TRIGGER_1"}});
        Assert.assertEquals("# of rows in SYS.SYSDEPENDS should reduce",
        		numberOfRowsInSysdepends(st),countAfter1Trigger);

        assertStatementError("X0Y25", st,
        		"alter table ATDC_13_TAB1_BACKUP drop column c12 restrict");
        triggersExist(st, new String[][]{{"ATDC_13_TAB1_TRIGGER_1"}});
        Assert.assertEquals("# of rows in SYS.SYSDEPENDS should not change",
        		numberOfRowsInSysdepends(st),countAfter1Trigger);
        
        // We will drop the dependent trigger
        st.executeUpdate("alter table ATDC_13_TAB1_BACKUP drop column c12");
        Assert.assertEquals("# of rows in SYS.SYSDEPENDS should reduce",
        		numberOfRowsInSysdepends(st),sysdependsRowCountBeforeCreateTrigger);
        // After DERBY-5044 is fixed, following should be rewritten
        st.executeUpdate("alter table ATDC_13_TAB1_BACKUP add column c12 int");

        //Test triggers with trigger action doing DELETE
        sysdependsRowCountBeforeCreateTrigger = numberOfRowsInSysdepends(st);
        st.executeUpdate(
                " create trigger ATDC_13_TAB1_trigger_1 after update " +
                "on ATDC_13_TAB1 for each row " +
                "DELETE FROM ATDC_13_TAB1_BACKUP " +
                "WHERE C12>1");
        countAfter1Trigger = numberOfRowsInSysdepends(st);
        st.executeUpdate(
                " create trigger ATDC_13_TAB1_trigger_2 after update " +
                "on ATDC_13_TAB1 for each row " +
                "DELETE FROM ATDC_13_TAB3 WHERE "+
                "C32 IN (values(1))");
        countAfter2Triggers = numberOfRowsInSysdepends(st);
        sysdependsRowCountAfterCreateTrigger = numberOfRowsInSysdepends(st);

        assertStatementError("X0Y25", st,
        		"alter table ATDC_13_TAB3 drop column c32 restrict");
        triggersExist(st, new String[][]{{"ATDC_13_TAB1_TRIGGER_1"},
            	{"ATDC_13_TAB1_TRIGGER_2"}});
        Assert.assertEquals("# of rows in SYS.SYSDEPENDS should not change",
        		numberOfRowsInSysdepends(st),sysdependsRowCountAfterCreateTrigger);
        
        // We will drop the dependent trigger
        st.executeUpdate("alter table ATDC_13_TAB3 drop column c32");
        triggersExist(st, new String[][]{{"ATDC_13_TAB1_TRIGGER_1"}});
        Assert.assertEquals("# of rows in SYS.SYSDEPENDS should reduce",
        		numberOfRowsInSysdepends(st),countAfter1Trigger);

        assertStatementError("X0Y25", st,
        		"alter table ATDC_13_TAB1_BACKUP drop column c12 restrict");
        triggersExist(st, new String[][]{{"ATDC_13_TAB1_TRIGGER_1"}});
        Assert.assertEquals("# of rows in SYS.SYSDEPENDS should not change",
        		numberOfRowsInSysdepends(st),countAfter1Trigger);
        
        // We will drop the dependent trigger
        st.executeUpdate("alter table ATDC_13_TAB1_BACKUP drop column c12");
        Assert.assertEquals("# of rows in SYS.SYSDEPENDS should reduce",
        		numberOfRowsInSysdepends(st),sysdependsRowCountBeforeCreateTrigger);
        st.executeUpdate("drop table ATDC_13_TAB1");
        st.executeUpdate("drop table ATDC_13_TAB1_BACKUP");
        st.executeUpdate("drop table ATDC_13_TAB2");
        st.executeUpdate("drop table ATDC_13_TAB3");
        // End of that test
        
        // Start of another test for DERBY-5044. 
        // Test SELECT from views inside the trigger action. The drop column 
        // detects the view dependnecy and does not allow drop column restrict 
        // to work but cascade option only drops the view but not the trigger.
        createTableAndInsertData(st, "ATDC_13_TAB1", "C11", "C12");
        createTableAndInsertData(st, "ATDC_13_TAB2", "C11", "C12");
        createTableAndInsertData(st, "ATDC_13_TAB3", "C11", "C12");
        
        st.executeUpdate("create view ATDC_13_VIEW1 as " +
        		"select c11 from ATDC_13_TAB2");
        st.executeUpdate("create view ATDC_13_VIEW3 as " +
        		"select * from ATDC_13_TAB2");
        st.executeUpdate("create view ATDC_13_VIEW2 as " +
        		"select c12 from ATDC_13_TAB3 where c12>0");
        
        //Test triggers with trigger action using views
        sysdependsRowCountBeforeCreateTrigger = numberOfRowsInSysdepends(st);
        st.executeUpdate(
                " create trigger ATDC_13_TAB1_trigger_1 after update " +
                "on ATDC_13_TAB1 for each row " +
                "SELECT * from ATDC_13_VIEW1 WHERE C11>0");
        countAfter1Trigger = numberOfRowsInSysdepends(st);
        st.executeUpdate(
                " create trigger ATDC_13_TAB1_trigger_2 after update " +
                "on ATDC_13_TAB1 for each row " +
                "SELECT * from ATDC_13_VIEW3");
        countAfter2Triggers = numberOfRowsInSysdepends(st);
        st.executeUpdate(
                " create trigger ATDC_13_TAB1_trigger_3 after update " +
                "on ATDC_13_TAB1 for each row " +
                "SELECT * from ATDC_13_VIEW2 ");
        countAfter3Triggers = numberOfRowsInSysdepends(st);
        sysdependsRowCountAfterCreateTrigger = numberOfRowsInSysdepends(st);

        // DROP COLUMN RESTRICT fails because there is a view using the column
        assertStatementError("X0Y23", st,
        		"alter table ATDC_13_TAB3 drop column c12 restrict");
        triggersExist(st, new String[][]{{"ATDC_13_TAB1_TRIGGER_1"},
            	{"ATDC_13_TAB1_TRIGGER_2"}, {"ATDC_13_TAB1_TRIGGER_3"}});
        Assert.assertEquals("# of rows in SYS.SYSDEPENDS should not change",
        		numberOfRowsInSysdepends(st),sysdependsRowCountAfterCreateTrigger);
        
        st.executeUpdate("alter table ATDC_13_TAB3 drop column c12");
        triggersExist(st, new String[][]{{"ATDC_13_TAB1_TRIGGER_1"},
            	{"ATDC_13_TAB1_TRIGGER_2"}});
        // One row from sysdepends got dropped because of a view getting
        // dropped and that is why we are checking for countAfter2Triggers-1
        Assert.assertEquals("# of rows in SYS.SYSDEPENDS should reduce",
        		numberOfRowsInSysdepends(st),countAfter2Triggers-1);

        // DROP COLUMN RESTRICT fails as there are 2 views using the column
        assertStatementError("X0Y23", st,
		"alter table ATDC_13_TAB2 drop column c11 restrict");
        triggersExist(st, new String[][]{{"ATDC_13_TAB1_TRIGGER_1"},
            	{"ATDC_13_TAB1_TRIGGER_2"}});
        Assert.assertEquals("# of rows in SYS.SYSDEPENDS should not change",
        		numberOfRowsInSysdepends(st),countAfter2Triggers-1);
        
        // We have dropped dependent triggers while dropping dependent view
        st.executeUpdate("alter table ATDC_13_TAB2 drop column c11");
        // Two rows from sysdepends got dropped because of 2 views getting
        // dropped from the drop column c11 from ATDC_13_TAB2. Additionally,
        // another view was dropped from drop of c12 from ATDC_13_TAB3.
        // So 3 dependencies altogether got lost from sysdepends in
        // addition to the dependencies that triggers had required.
        Assert.assertEquals("# of rows in SYS.SYSDEPENDS should reduce",
        		numberOfRowsInSysdepends(st),sysdependsRowCountBeforeCreateTrigger-3);

        st.executeUpdate("drop table ATDC_13_TAB1");
        st.executeUpdate("drop table ATDC_13_TAB2");
        st.executeUpdate("drop table ATDC_13_TAB3");
        // End of that test

        
        // Another test
        // ALTER TABLE DROP COLUMN in following test case causes the column 
        // positions of trigger action columns to change. Derby detects 
        // that and regenerates the internal trigger action sql with correct
        // column positions. The trigger here is defined at the table level
        createTableAndInsertData(st, "ATDC_15_TAB1", "A1", "B1");
        createTableAndInsertData(st, "ATDC_15_TAB2", "A2", "B2");

        sysdependsRowCountBeforeCreateTrigger = numberOfRowsInSysdepends(st);
        st.executeUpdate(
                " create trigger atdc_15_trigger_1 after update " +
                "on atdc_15_tab1 REFERENCING NEW AS newt " +
                "for each row " +
                "update atdc_15_tab2 set b2 = newt.b1");
        sysdependsRowCountAfterCreateTrigger = numberOfRowsInSysdepends(st);
        st.executeUpdate("update atdc_15_tab1 set b1=22");
        rs =
            st.executeQuery(" select * from atdc_15_tab1");
        JDBC.assertFullResultSet(rs, new String[][]{{"1","22"}});
        rs =
            st.executeQuery(" select * from atdc_15_tab2");
        JDBC.assertFullResultSet(rs, new String[][]{{"1","22"}});
        st.executeUpdate("alter table atdc_15_tab1 drop column a1 restrict");
        Assert.assertEquals("# of rows in SYS.SYSDEPENDS should not change",
        		numberOfRowsInSysdepends(st),sysdependsRowCountAfterCreateTrigger);
        st.executeUpdate("update atdc_15_tab1 set b1=33");
        rs =
            st.executeQuery(" select * from atdc_15_tab1");
        JDBC.assertFullResultSet(rs, new String[][]{{"33"}});
        rs =
            st.executeQuery(" select * from atdc_15_tab2");
        JDBC.assertFullResultSet(rs, new String[][]{{"1","33"}});
        st.executeUpdate("drop table ATDC_15_TAB1");
        st.executeUpdate("drop table ATDC_15_TAB2");

        st.executeUpdate(
                " create table atdc_7 (a int, b int, c int, primary key (a))");

        assertStatementError("X0Y25", st,
                " alter table atdc_7 drop column a restrict");

        st.executeUpdate(
                " alter table atdc_7 drop column a cascade");
        checkWarning(st, "01500");

        st.executeUpdate(
                " create table atdc_8 (a int, b int, c int, primary " +
                "key (b, c))");

        assertStatementError("X0Y25", st,
                " alter table atdc_8 drop column c restrict");

        st.executeUpdate("alter table atdc_8 drop column c cascade");
        checkWarning(st, "01500");

        st.executeUpdate("create table atdc_9 (a int not null, b int)");
        st.executeUpdate("alter table atdc_9 drop column a restrict");

        // ALTER TABLE DROP COLUMN automatically drops any granted privilege,
        // regardless of whether RESTRICT or CASCADE was specified. Verify that
        // the privileges are dropped correctly and that the bitmap is updated:

        st.executeUpdate("create table atdc_10 (a int, b int, c int)");
        st.executeUpdate("grant select(a, b, c) on atdc_10 to bryan");

        rs =
                st.executeQuery(
                " select GRANTEE,GRANTOR,TYPE,COLUMNS from sys.syscolperms");
        JDBC.assertFullResultSet(rs, new String[][]{
                    {"BRYAN", DerbyConstants.TEST_DBO, "s", "{0, 1, 2}"}
                });

        st.executeUpdate("alter table atdc_10 drop column b restrict");

        rs =
                st.executeQuery(
                " select GRANTEE,GRANTOR,TYPE,COLUMNS from sys.syscolperms");
        JDBC.assertFullResultSet(rs, new String[][]{
                    {"BRYAN", DerbyConstants.TEST_DBO, "s", "{0, 1}"}
                });

        assertStatementError("42X14", st,
                " alter table atdc_10 drop column b cascade");

        rs =
                st.executeQuery(
                " select GRANTEE,GRANTOR,TYPE,COLUMNS from sys.syscolperms");
        JDBC.assertFullResultSet(rs, new String[][]{
                    {"BRYAN", DerbyConstants.TEST_DBO, "s", "{0, 1}"}
                });

        // Include the test from the DERBY-1909 report:

        //drop a table that does not exist should fail
        assertStatementError("42Y55", st, "drop table d1909");
        st.executeUpdate("create table d1909 (a int, b int, c int)");
        st.executeUpdate("grant select (a) on d1909 to user1");
        st.executeUpdate("grant select (a,b) on d1909 to user2");
        st.executeUpdate("grant update(c) on d1909 to super_user");

        rs =
                st.executeQuery(
                " select c.grantee, c.type, c.columns from " +
                "sys.syscolperms c, sys.systables t where c.tableid " +
                "= t.tableid and t.tablename='D1909'");
        JDBC.assertFullResultSet(rs, new String[][]{
                    {"USER1", "s", "{0}"},
                    {"USER2", "s", "{0, 1}"},
                    {"SUPER_USER", "u", "{2}"}
                });

        st.executeUpdate("alter table d1909 drop column a");

        rs =
                st.executeQuery(
                " select c.grantee, c.type, c.columns from " +
                "sys.syscolperms c, sys.systables t where c.tableid " +
                "= t.tableid and t.tablename='D1909'");
        JDBC.assertFullResultSet(rs, new String[][]{
                    {"USER1", "s", "{}"},
                    {"USER2", "s", "{0}"},
                    {"SUPER_USER", "u", "{1}"}
                });

        st.executeUpdate("grant update(b) on d1909 to user1");
        st.executeUpdate("grant select(c) on d1909 to user1");
        st.executeUpdate("grant select(c) on d1909 to user2");

        rs =
                st.executeQuery(
                " select c.grantee, c.type, c.columns from " +
                "sys.syscolperms c, sys.systables t where c.tableid " +
                "= t.tableid and t.tablename='D1909'");
        JDBC.assertFullResultSet(rs, new String[][]{
                    {"USER1", "s", "{1}"},
                    {"USER2", "s", "{0, 1}"},
                    {"SUPER_USER", "u", "{1}"},
                    {"USER1", "u", "{0}"}
                });
    }

    //Create table and insert data necessary for ALTER TABLE DROP COLUMN test
    private void createTableAndInsertData(Statement s, String tableName, 
    		String column1, String column2)
    throws SQLException {
        s.execute("CREATE TABLE " + tableName + " (" + 
        		column1 + " int, " + column2 + " int) ");
        s.execute("INSERT INTO " + tableName + " VALUES (1,11)");
    }

    //Get a count of number of rows in SYS.SYSDEPENDS
    private int numberOfRowsInSysdepends(Statement st)
    		throws SQLException {
    	ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM SYS.SYSDEPENDS");
    	rs.next();
    	return(rs.getInt(1));
    }

    //Make sure that the passed triggers exist in SYS.SYSTRIGGERS
    private void triggersExist(Statement st, String [][] expectedTriggers) 
    		throws SQLException {
        StringBuffer query = new StringBuffer("select triggername from sys.systriggers where triggername in (");
        
        for (int i=0; i < expectedTriggers.length; i++)
        {
        	query.append("'" + expectedTriggers[i][0] + "'");
        	if (i+1 < expectedTriggers.length)
            	query.append(", ");
        }
    	query.append(")");

        ResultSet rs = st.executeQuery(query.toString());
        JDBC.assertFullResultSet(rs, expectedTriggers);
    }

// JIRA 3175: Null Pointer Exception or SanityManager 
// ASSERT because autoincrement properties of generated 
// column are not maintained properly when a column before 
// it in the table is dropped:
    public void testJira3175()
            throws Exception {
        Statement st = createStatement();

        st.executeUpdate(
                "create table d3175 (x varchar(12), y varchar(12), " +
                "id int primary key generated by default as identity)");

        rs =
                st.executeQuery(
                " select COLUMNNAME, COLUMNNUMBER, COLUMNDATATYPE, " +
                " COLUMNDEFAULT, AUTOINCREMENTVALUE, AUTOINCREMENTSTART, " +
                " AUTOINCREMENTINC,  TABLENAME, TABLETYPE, LOCKGRANULARITY " +
                " from sys.syscolumns c,sys.systables t " +
                "where c.referenceid = t.tableid and t.tablename='D3175'");
        JDBC.assertUnorderedResultSet(rs, new String[][]{
                    {"X", "1", "VARCHAR(12)", null, null, null, null, "D3175", "T", "R"},
                    {"Y", "2", "VARCHAR(12)", null, null, null, null, "D3175", "T", "R"},
                    {"ID", "3", "INTEGER NOT NULL", "GENERATED_BY_DEFAULT", "1", "1", "1", "D3175", "T", "R"}
                });

        st.executeUpdate("insert into d3175(x) values 'b'");
        st.executeUpdate("alter table d3175 drop column y");
        st.executeUpdate("insert into d3175(x) values 'a'");

        rs =
                st.executeQuery(
                " select COLUMNNAME, COLUMNNUMBER, COLUMNDATATYPE, " +
                " COLUMNDEFAULT, AUTOINCREMENTVALUE, AUTOINCREMENTSTART, " +
                " AUTOINCREMENTINC,  TABLENAME, TABLETYPE, LOCKGRANULARITY " +
                " from sys.syscolumns c,sys.systables t " +
                "where c.referenceid = t.tableid and t.tablename='D3175'");
        JDBC.assertUnorderedResultSet(rs, new String[][]{
                    {"X", "1", "VARCHAR(12)", null, null, null, null, "D3175", "T", "R"},
                    {"ID", "2", "INTEGER NOT NULL", "GENERATED_BY_DEFAULT", "3", "1", "1", "D3175", "T", "R"}
                });
    }

// JIRA 3177 appears to be aduplicate of JIRA 3175, but 
// the reproduction test script is different. In the 
// interests of additional testing, we include the JIRA 
// 3177 test script, as it has a number of additional 
// examples of interesting ALTER TABLE statements In the 
// original JIRA 3177 bug, by the time we get to the end of 
// the ALTER TABLE processing, the select from 
// SYS.SYSCOLUMNS retrieves NULL for the autoinc columns, 
// instead of the correct value (1).
    public void testJira3177()
            throws Exception {
        Statement st = createStatement();
        st.executeUpdate(
                "create table d3177_SchemaVersion (version INTEGER NOT NULL)");

        st.executeUpdate(
                "insert into d3177_SchemaVersion (version) values (0)");

        st.executeUpdate(
                " create table d3177_BinaryData ( id INTEGER NOT " +
                "NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, " +
                "INCREMENT BY 1), CRC32 BIGINT NOT NULL , data BLOB " +
                "NOT NULL , CONSTRAINT d3177_BinaryData_id_pk " +
                "PRIMARY KEY(id) )");

        st.executeUpdate(
                " create table d3177_MailServers ( id INTEGER NOT " +
                "NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, " +
                "INCREMENT BY 1), port INTEGER NOT NULL , username " +
                "varchar(80) NOT NULL , protocol varchar(80) NOT " +
                "NULL , SSLProtocol varchar(10), emailAddress " +
                "varchar(80) NOT NULL , server varchar(80) NOT NULL " +
                ", password varchar(80) NOT NULL , CONSTRAINT " +
                "d3177_MailServers_id_pk PRIMARY KEY(id) )");

        st.executeUpdate(
                " create table d3177_Mailboxes ( id INTEGER NOT NULL " +
                "GENERATED ALWAYS AS IDENTITY (START WITH 1, " +
                "INCREMENT BY 1), port INTEGER NOT NULL , folder " +
                "varchar(80) NOT NULL , username varchar(80) NOT " +
                "NULL , SSLProtocol varchar(10), hostname " +
                "varchar(80) NOT NULL , storeType varchar(80) NOT " +
                "NULL , password varchar(80) NOT NULL , timeout " +
                "INTEGER NOT NULL , MailServerID INTEGER NOT NULL , " +
                "CONSTRAINT d3177_Mailboxes_id_pk PRIMARY KEY(id) )");

        st.executeUpdate(
                " create table d3177_MESSAGES ( Message_From " +
                "varchar(1000), Message_Cc varchar(1000), " +
                "Message_Subject varchar(1000), Message_ID " +
                "varchar(256) NOT NULL , Message_Bcc varchar(1000), " +
                "Message_Date TIMESTAMP, Content_Type varchar(256), " +
                "MailboxID INTEGER NOT NULL , Search_Text CLOB NOT " +
                "NULL , id INTEGER NOT NULL GENERATED ALWAYS AS " +
                "IDENTITY (START WITH 1, INCREMENT BY 1), Message_To " +
                "varchar(1000), Display_Text CLOB NOT NULL , " +
                "Message_Data_ID INTEGER NOT NULL , CONSTRAINT " +
                "d3177_MESSAGES_id_pk PRIMARY KEY(id) )");

        rs =
                st.executeQuery(
                " select COLUMNNAME, COLUMNNUMBER, COLUMNDATATYPE, " +
                " COLUMNDEFAULT, AUTOINCREMENTVALUE, AUTOINCREMENTSTART, " +
                " AUTOINCREMENTINC,  TABLENAME, TABLETYPE, LOCKGRANULARITY " +
                " from sys.syscolumns c,sys.systables t " +
                "where c.referenceid = t.tableid and " +
                "c.columnname='ID' and t.tablename='D3177_MESSAGES'");

        JDBC.assertFullResultSet(rs, new String[][]{
                    {"ID", "10", "INTEGER NOT NULL", null, "1", "1", "1", "D3177_MESSAGES", "T", "R"}
                });

        st.executeUpdate(
                " create table D3177_ATTACHMENTS ( id INTEGER NOT " +
                "NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, " +
                "INCREMENT BY 1), Inline INTEGER, CRC32 BIGINT NOT " +
                "NULL , Attachment_Name varchar(256) NOT NULL , " +
                "Attachment_File varchar(512) NOT NULL , Message_ID " +
                "INTEGER NOT NULL , Content_Type varchar(256) NOT " +
                "NULL , CONSTRAINT D3177_ATTACHMENTS_id_pk PRIMARY KEY(id) )");

        st.executeUpdate(
                " alter table D3177_ATTACHMENTS ADD CONSTRAINT " +
                "ATTACHMENTS_Message_ID_MESSAGES_ID FOREIGN KEY ( " +
                "Message_ID ) REFERENCES D3177_MESSAGES ( ID )");

        st.executeUpdate(
                " alter table D3177_MESSAGES ADD CONSTRAINT " +
                "MESSAGES_MailboxID_Mailboxes_ID FOREIGN KEY ( " +
                "MailboxID ) REFERENCES d3177_Mailboxes ( ID )");

        st.executeUpdate(
                " alter table D3177_MESSAGES ADD CONSTRAINT " +
                "MESSAGES_Message_Data_ID_d3177_BinaryData_ID " +
                "FOREIGN KEY ( Message_Data_ID ) REFERENCES " +
                "d3177_BinaryData ( ID )");

        st.executeUpdate(
                " alter table d3177_Mailboxes ADD CONSTRAINT " +
                "Mailboxes_MailServerID_MailServers_ID FOREIGN KEY ( " +
                "MailServerID ) REFERENCES d3177_MailServers ( ID )");

        assertUpdateCount(st, 1, "update d3177_SchemaVersion set version=1");

        st.executeUpdate(
                " alter table D3177_MESSAGES alter Message_To SET " +
                "DATA TYPE varchar(10000)");

        st.executeUpdate(
                " alter table D3177_MESSAGES alter Message_From SET " +
                "DATA TYPE varchar(10000)");

        st.executeUpdate(
                " alter table D3177_MESSAGES alter Message_Cc SET " +
                "DATA TYPE varchar(10000)");

        st.executeUpdate(
                " alter table D3177_MESSAGES alter Message_Bcc SET " +
                "DATA TYPE varchar(10000)");

        rs =
                st.executeQuery(
                " select COLUMNNAME, COLUMNNUMBER, COLUMNDATATYPE, " +
                " COLUMNDEFAULT, AUTOINCREMENTVALUE, AUTOINCREMENTSTART, " +
                " AUTOINCREMENTINC,  TABLENAME, TABLETYPE, LOCKGRANULARITY " +
                " from sys.syscolumns c,sys.systables t " +
                "where c.referenceid = t.tableid and " +
                "c.columnname='ID' and t.tablename='D3177_MESSAGES'");

        JDBC.assertFullResultSet(rs, new String[][]{
                    {"ID", "10", "INTEGER NOT NULL", null, "1", "1", "1", "D3177_MESSAGES", "T", "R"}
                });

        assertUpdateCount(st, 1, "update d3177_SchemaVersion set version=2");

        st.executeUpdate(
                " create table D3177_MailStatistics ( id INTEGER NOT " +
                "NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, " +
                "INCREMENT BY 1), ProcessedCount INTEGER DEFAULT 0 " +
                "NOT NULL , HourOfDay INTEGER NOT NULL , " +
                "LastModified TIMESTAMP NOT NULL , RejectedMailCount " +
                "INTEGER DEFAULT 0 NOT NULL , DayOfWeek INTEGER NOT " +
                "NULL , CONSTRAINT D3177_MailStatistics_id_pk " +
                "PRIMARY KEY(id) )");

        st.executeUpdate(
                " CREATE INDEX D3177_MailStatistics_HourOfDay_idx ON " +
                "D3177_MailStatistics(HourOfDay)");

        st.executeUpdate(
                " CREATE INDEX D3177_MailStatistics_DayOfWeek_idx ON " +
                "D3177_MailStatistics(DayOfWeek)");

        st.executeUpdate(
                " alter table D3177_MESSAGES alter CONTENT_TYPE SET " +
                "DATA TYPE varchar(256)");

        assertUpdateCount(st, 1, "update d3177_SchemaVersion set version=3");

        st.executeUpdate(
                " alter table D3177_messages alter column Message_ID NULL");

        st.executeUpdate(
                " CREATE INDEX D3177_MESSAGES_Message_ID_idx ON " +
                "D3177_MESSAGES(Message_ID)");

        assertUpdateCount(st, 1, "update d3177_SchemaVersion set version=4");

        st.executeUpdate(
                "alter table D3177_MESSAGES add filename varchar(256)");

        st.executeUpdate("alter table D3177_MESSAGES add CRC32 BIGINT");

        JDBC.assertEmpty(
                st.executeQuery("select id,crc32,data from d3177_BinaryData"));

        rs =
                st.executeQuery(
                " select COLUMNNAME, COLUMNNUMBER, COLUMNDATATYPE, " +
                " COLUMNDEFAULT, AUTOINCREMENTVALUE, AUTOINCREMENTSTART, " +
                " AUTOINCREMENTINC,  TABLENAME, TABLETYPE, LOCKGRANULARITY " +
                " from sys.syscolumns c,sys.systables t " +
                "where c.referenceid = t.tableid and " +
                "c.columnname='ID' and t.tablename='D3177_MESSAGES'");

        JDBC.assertFullResultSet(rs, new String[][]{
                    {"ID", "10", "INTEGER NOT NULL", null, "1", "1", "1", "D3177_MESSAGES", "T", "R"}
                });


        st.executeUpdate(
                " alter table D3177_messages alter column filename NOT NULL");

        st.executeUpdate(
                " alter table D3177_messages alter column crc32 NOT NULL");

        st.executeUpdate(
                " alter table D3177_messages alter column mailboxid NULL");

        assertStatementError("42X86", st,
                " ALTER TABLE D3177_MESSAGES DROP CONSTRAINT " +
                "MESSAGES_message_data_id_BinaryData_id");

        st.executeUpdate(
                " alter table D3177_messages drop column message_data_id");
        checkWarning(st, "01500");

        st.executeUpdate("drop table d3177_BinaryData");
        assertUpdateCount(st, 1, "update d3177_SchemaVersion set version=6");

        st.executeUpdate(
                " create table D3177_EmailAddresses ( id INTEGER NOT " +
                "NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, " +
                "INCREMENT BY 1), address varchar(256) NOT NULL , " +
                "CONSTRAINT D3177_EmailAddresses_id_pk PRIMARY " +
                "KEY(id), CONSTRAINT D3177_EmailAddresses_address_uq " +
                "UNIQUE(address) )");

        st.executeUpdate(
                " CREATE UNIQUE INDEX " +
                "D3177_EmailAddresses_address_idx ON " +
                "D3177_EmailAddresses(address)");
        checkWarning(st, "01504"); //new index is a duplicate of an existing index

        st.executeUpdate(
                " create table D3177_EmailAddressesToMessages ( " +
                "MessageID INTEGER NOT NULL , EmailAddressID INTEGER " +
                "NOT NULL )");

        st.executeUpdate(
                " alter table D3177_EmailAddressesToMessages ADD CONSTRAINT " +
                "EmailAddressesToMessages_MessageID_Messages_ID " +
                "FOREIGN KEY ( MessageID ) REFERENCES D3177_Messages ( ID )");

        st.executeUpdate(
                " alter table D3177_EmailAddressesToMessages ADD CONSTRAINT " +
                "EmailAddressesToMessages_EmailAddressID_EmailAddress" +
                "es_ID FOREIGN KEY ( EmailAddressID ) REFERENCES " +
                "D3177_EmailAddresses ( ID )");

        st.executeUpdate(
                " create table AuthenticationServers ( id INTEGER " +
                "NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH " +
                "1, INCREMENT BY 1), port INTEGER NOT NULL , " +
                "protocol varchar(20) NOT NULL , hostname " +
                "varchar(40) NOT NULL , CONSTRAINT " +
                "AuthenticationServers_id_pk PRIMARY KEY(id) )");

        st.executeUpdate(
                " alter table d3177_Mailboxes add " +
                "AuthenticationServerID INTEGER");

        JDBC.assertEmpty(
                st.executeQuery("select id,filename from D3177_messages"));

        st.executeUpdate("alter table D3177_MESSAGES drop column message_to");
        st.executeUpdate("alter table D3177_MESSAGES drop column message_cc");
        st.executeUpdate("alter table D3177_MESSAGES drop column message_from");

        rs =
                st.executeQuery(
                " select COLUMNNAME, COLUMNNUMBER, COLUMNDATATYPE, " +
                " COLUMNDEFAULT, AUTOINCREMENTVALUE, AUTOINCREMENTSTART, " +
                " AUTOINCREMENTINC,  TABLENAME, TABLETYPE, LOCKGRANULARITY " +
                " from sys.syscolumns c,sys.systables t " +
                "where c.referenceid = t.tableid and " +
                "c.columnname='ID' and t.tablename='D3177_MESSAGES'");

        JDBC.assertFullResultSet(rs, new String[][]{
                    {"ID", "8", "INTEGER NOT NULL", null, "1", "1", "1", "D3177_MESSAGES", "T", "R"}
                });

        assertUpdateCount(st, 1, "update d3177_SchemaVersion set version=7");
    }

// JIRA 2371: ensure that a non-numeric, non-autogenerated 
// column can have its default value modified:
    public void testJira2371() throws Exception {
        Statement st = createStatement();

        st.executeUpdate("create table t2371 ( a varchar(10))");
        st.executeUpdate("alter table t2371 alter column a default 'my val'");
        st.executeUpdate("insert into t2371 (a) values ('hi')");
        st.executeUpdate("insert into t2371 (a) values (default)");
        st.executeUpdate("alter table t2371 alter column a default 'another'");
        st.executeUpdate("insert into t2371 (a) values (default)");

        JDBC.assertFullResultSet(st.executeQuery("select A from t2371"),
                new String[][]{{"hi"}, {"my val"}, {"another"}});
    }

// DERBY-3355: Exercise ALTER TABLE ... NOT NULL with 
// table and column names which are in mixed case. This is 
// important because 
// AlterTableConstantAction.validateNotNullConstraint 
// generates and executes some SQL on-the-fly, and it's 
// important that it properly delimits the table and column 
// names in that SQL. We also include a few other "unusual" 
// table and column names.
    public void testJira3355() throws Exception {

        Statement st = createStatement();
        createTestObjects(st);
        st.executeUpdate(
                "create table d3355 ( c1 varchar(10), \"c2\" " +
                "varchar(10), c3 varchar(10))");

        st.executeUpdate(
                " create table \"d3355_a\" ( c1 varchar(10), \"c2\" " +
                "varchar(10), c3 varchar(10))");

        st.executeUpdate(
                " create table d3355_qt_col (\"\"\"c\"\"4\" int, " +
                "\"\"\"\"\"C5\" int, \"c 6\" int)");

        st.executeUpdate(
                " create table \"d3355_qt_\"\"tab\" ( c4 int, c5 int, c6 int)");

        st.executeUpdate("insert into d3355 values ('a', 'b', 'c')");
        st.executeUpdate("insert into \"d3355_a\" values ('d', 'e', 'f')");
        st.executeUpdate("insert into d3355_qt_col values (4, 5, 6)");
        st.executeUpdate("insert into \"d3355_qt_\"\"tab\" values (4, 5, 6)");

        // All of these ALTER TABLE statements should succeed.

        st.executeUpdate("alter table d3355 alter column c1 not null");
        st.executeUpdate("alter table d3355 alter column \"c2\" not null");
        st.executeUpdate("alter table d3355 alter column \"C3\" not null");
        st.executeUpdate("alter table \"d3355_a\" alter column c1 not null");
        st.executeUpdate(
                "alter table \"d3355_a\" alter column \"c2\" not null");
        st.executeUpdate(
                "alter table \"d3355_a\" alter column \"C3\" not null");
        st.executeUpdate(
                " alter table d3355_qt_col alter column " +
                "\"\"\"\"\"C5\" not null");
        st.executeUpdate(
                " alter table d3355_qt_col alter column \"c 6\" not null");
        st.executeUpdate(
                " alter table \"d3355_qt_\"\"tab\" alter column c5 not null");

        // These ALTER TABLE statements should fail, with 
        // no-such-column and/or no-such-table errors:

        assertStatementError("42X14", st,
                "alter table d3355 alter column \"c1\" not null");

        assertStatementError("42X14", st,
                " alter table d3355 alter column c2 not null");

        assertStatementError("42Y55", st,
                " alter table d3355_a alter column c1 not null");

        assertStatementError("42X14", st,
                " alter table \"d3355_a\" alter column \"c1\" not null");
    }
    
    public void testJira4256() throws SQLException{
        
        Statement st = createStatement();
        createTestObjects(st);
        
        //increase the maximum size of the clob 
        
        Clob clob = null;
        Blob blob=null;
        int val = 1;
        int size = 15 * 1024;
        InputStream stream;
               
        st.executeUpdate("create table clob_tab(c1 int,clob_col clob(10K))");
        conn.commit();
        
        pSt=conn.prepareStatement("INSERT INTO clob_tab values (?,?)");   
        stream = new TestInputStream(size, val);
        
        //this insert fails(size>10K) 
        pSt.setInt(1, val);
        pSt.setAsciiStream(2, stream, size);
        assertStatementError("XJ001", pSt);
        pSt.close();
        
        conn.rollback();
        
        st.executeUpdate("ALTER TABLE clob_tab ALTER COLUMN "
                +"clob_col SET DATA TYPE clob(20K)");
        
        pSt=conn.prepareStatement("INSERT INTO clob_tab values (?,?)");
        stream = new TestInputStream(size, val);
        
        //this insert succeed (maximum blob size not increased to 20K)
        pSt.setInt(1, val);
        pSt.setAsciiStream(2, stream, size);
        pSt.executeUpdate();
        pSt.close(); 
        
        
        //increase the maximum size of the blob        
        
        st.executeUpdate("CREATE TABLE blob_tab ( C1 INTEGER," +
                                "blob_col BLOB(10K) NOT NULL)");
        
        conn.commit();
        
        pSt=conn.prepareStatement("INSERT INTO blob_tab values (?,?)");
        stream = new TestInputStream(size, val);
        
        //this insert fails(size>10K) 
        pSt.setInt(1, val);
        pSt.setBinaryStream(2, stream, size);
        assertStatementError("22001", pSt);
        pSt.close();
        
        conn.rollback();
        
        st.executeUpdate("ALTER TABLE blob_tab ALTER COLUMN "
                +"blob_col SET DATA TYPE blob(20K)");  
        
        pSt=conn.prepareStatement("INSERT INTO blob_tab values (?,?)");
        stream = new TestInputStream(size, val);
        
        //this insert succeed (maximum blob size not increased to 20K)
        pSt.setInt(1, val);
        pSt.setBinaryStream(2, stream, size);
        pSt.executeUpdate();
        pSt.close();   
        
        conn.rollback();
    }

    /**
     * Test that an ALTER TABLE statement that adds a new column with a
     * default value, doesn't fail if the schema name, table name or column
     * name contains a double quote character.
     */
    public void testDerby5157_addColumnWithDefaultValue() throws SQLException {
        setAutoCommit(false);
        Statement s = createStatement();
        s.execute("create schema \"\"\"\"");
        s.execute("create table \"\"\"\".\"\"\"\" (x int)");

        // The following statement used to fail with a syntax error.
        s.execute("alter table \"\"\"\".\"\"\"\" " +
                  "add column \"\"\"\" int default 42");
    }

    /**
     * Test that an ALTER TABLE statement that changes the increment value of
     * an identity column, doesn't fail if the schema name, table name or
     * column name contains a double quote character.
     */
    public void testDerby5157_changeIncrement() throws SQLException {
        setAutoCommit(false);
        Statement s = createStatement();
        s.execute("create schema \"\"\"\"");
        s.execute("create table \"\"\"\".\"\"\"\"" +
                  "(\"\"\"\" int generated always as identity)");

        // The following statement used to fail with a syntax error.
        s.execute("alter table \"\"\"\".\"\"\"\" " +
                  "alter column \"\"\"\" set increment by 2");
    }
    
    /**
     * Verify that rollback works properly if a column with a null default
     * is added and then the table is updated. See DERBY-5679.
     */
    public void test_5679() throws Exception
    {
        Statement s = createStatement();
        ResultSet   rs;

        String[][]  rowBefore = new String[][]{ { "before", null, "before" }  };
        String[][]  rowAfter = new String[][]{ { "after", "after", "after" }  };

        // create a table, insert a row, add two columns, then update one of the columns
        s.execute( "create table t_5679(name1 varchar(10))" );
        s.execute( "insert into t_5679(name1) values('before')" );
        s.execute( "alter table t_5679 add column str1 varchar(10)" );
        s.execute( "alter table t_5679 add column str2 varchar(10)" );
        s.execute( "update t_5679 set str2 = 'before'" );

        rs = s.executeQuery( "select * from t_5679" );
        JDBC.assertFullResultSet( rs, rowBefore );

        // now update the row and rollback
        setAutoCommit( false );
        s.execute( "update t_5679 set name1='after', str1='after', str2='after'" );
        rs = s.executeQuery( "select * from t_5679" );
        JDBC.assertFullResultSet( rs, rowAfter );
        rollback();
        setAutoCommit( true );

        // all columns of the row should have reverted
        rs = s.executeQuery( "select * from t_5679" );
        JDBC.assertFullResultSet( rs, rowBefore );

        s.execute( "drop table t_5679" );
    }
    
    /**
     * More tests for DERBY-5679. Verify with a lot of columns.
     */
    public void test_5679_manyColumns() throws Exception
    {
        Statement s = createStatement();
        ResultSet   rs;

        // create a table, insert a row, add two columns, then update one of the columns
        s.execute( "create table t_5679_1( keyCol int )" );
        s.execute( "insert into t_5679_1( keyCol ) values( 1 )" );

        // now add a lot of columns
        for ( int i = 1; i < 100; i++ )
        {
            s.execute( "alter table t_5679_1 add column a_" + i + " int" );
        }
        s.execute( "update t_5679_1 set a_50 = 50" );

        String[]    rawBeforeRow = new String[ 100 ];
        rawBeforeRow[ 0 ] = "1";
        rawBeforeRow[ 50 ] = "50";
        String[][]  beforeRow = new String[][] { rawBeforeRow };

        String[]    rawAfterRow = new String[ 100 ];
        rawAfterRow[ 0 ] = "1";
        rawAfterRow[ 49 ] = "490";
        rawAfterRow[ 50 ] = "500";
        rawAfterRow[ 51 ] = "510";
        String[][]  afterRow = new String[][] { rawAfterRow };

        rs = s.executeQuery( "select * from t_5679_1" );
        JDBC.assertFullResultSet( rs, beforeRow );

        // now update the row and rollback
        setAutoCommit( false );
        s.execute( "update t_5679_1 set a_49 = 490, a_50 = 500, a_51 = 510" );
        rs = s.executeQuery( "select * from t_5679_1" );
        JDBC.assertFullResultSet( rs, afterRow );
        rollback();
        setAutoCommit( true );

        // all columns of the row should have reverted
        rs = s.executeQuery( "select * from t_5679_1" );
        JDBC.assertFullResultSet( rs, beforeRow );

        s.execute( "drop table t_5679_1" );
    }
    
    /**
     * More tests for DERBY-5679. Verify with long rows.
     */
    public void test_5679_longRows() throws Exception
    {
        Connection conn = getConnection();
        PreparedStatement ps;
        ResultSet   rs;

        // verify that the default page size of 4096 bytes is in effect
        ps = conn.prepareStatement( "values syscs_util.syscs_get_database_property( 'derby.storage.pageSize' )" );
        rs = ps.executeQuery();
        rs.next();
        assertNull( rs.getString( 1 ) );
        rs.close();
        ps.close();

        final   int LONG = 1050;
        final   int SHORT = 500;
        final   int PAGE_SIZE = 4096;

        byte[]  a_0 = makeBytes( 0, LONG );
        byte[]  a_1 = makeBytes( 1, LONG );
        byte[]  a_2 = makeBytes( 2, LONG );
        byte[]  a_4 = makeBytes( 4, LONG );

        // create a table, insert a row, add two columns, then update one of the columns
        conn.prepareStatement
            (
             "create table t_5679_2( a_0 varchar( " + LONG + " ) for bit data," +
             " a_1 varchar( " + LONG + " ) for bit data," +
             " a_2 varchar( " + LONG + " ) for bit data)" )
            .execute();
        ps = conn.prepareStatement( "insert into t_5679_2( a_0, a_1, a_2 ) values ( ?, ?, ? )" );
        ps.setBytes( 1, a_0 );
        ps.setBytes( 2, a_1 );
        ps.setBytes( 3, a_2 );
        ps.executeUpdate();
        ps.close();

        // now add 2 columns. the second column will spill onto the second page if it is
        // stuffed with a long value
        conn.prepareStatement( "alter table t_5679_2 add column a_3 varchar( " + SHORT + " ) for bit data" ).execute();
        conn.prepareStatement( "alter table t_5679_2 add column a_4 varchar( " + LONG + " ) for bit data" ).execute();

        assertTrue( LONG + LONG + LONG + SHORT < PAGE_SIZE );
        assertTrue( LONG + LONG + LONG + LONG > PAGE_SIZE );
        
        // now stuff the second newly added column with a large value which
        // spills onto the next page
        ps = conn.prepareStatement( "update t_5679_2 set a_4 = ?" );
        ps.setBytes( 1, a_4 );
        ps.executeUpdate();
        ps.close();


        byte[]  after_0 = makeBytes( 100, LONG );
        byte[]  after_1 = makeBytes( 101, LONG );
        byte[]  after_2 = makeBytes( 102, LONG );
        byte[]  after_3 = makeBytes( 103, SHORT );
        byte[]  after_4 = makeBytes( 104, LONG );
        
        byte[][]    beforeRow = new byte[][] { a_0, a_1, a_2, null, a_4 };
        byte[][]    afterRow = new byte[][] { after_0, after_1, after_2, after_3, after_4 };

        vetBytes_5679( conn, beforeRow );

        // now update the row and rollback. columns a_0 through a_3 should stay on
        // the first page and a_4 should stay on the second page
        conn.setAutoCommit( false );
        ps = conn.prepareStatement( "update t_5679_2 set a_0 = ?, a_1 = ?, a_2 = ?, a_3 = ?, a_4 = ?" );
        ps.setBytes( 1, after_0 );
        ps.setBytes( 2, after_1 );
        ps.setBytes( 3, after_2 );
        ps.setBytes( 4, after_3 );
        ps.setBytes( 5, after_4 );
        ps.executeUpdate();
        vetBytes_5679( conn, afterRow );
        rollback();
        conn.setAutoCommit( true );

        // all columns of the row should have reverted
        vetBytes_5679( conn, beforeRow );

        conn.prepareStatement( "drop table t_5679_2" ).execute();
    }
    private byte[]  makeBytes( int seed, int length )
    {
        byte[]  result = new byte[ length ];

        Arrays.fill( result, (byte) seed );

        return result;
    }
    private void    vetBytes_5679( Connection conn, byte[][] expected ) throws Exception
    {
        PreparedStatement   ps = conn.prepareStatement( "select * from t_5679_2" );
        ResultSet   rs = ps.executeQuery();

        rs.next();

        for ( int i = 0; i < expected.length; i++ )
        {
            assertBytes( expected[ i ] , rs.getBytes( i + 1 ) );
        }

        rs.close();
        ps.close();
    }
    private void    assertBytes( byte[] expected, byte[] actual ) throws Exception
    {
        if ( expected == null )
        {
            assertNull( actual );
            return;
        }
        else { assertNotNull( actual ); }

        assertEquals( expected.length, actual.length );

        for ( int i = 0; i < expected.length; i++ )
        {
            assertEquals( expected[ i ], actual[ i ] );
        }
    }
    
}
