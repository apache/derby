
/*
Derby - Class org.apache.derbyTesting.functionTests.tests.lang.CheckConstraintTest

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

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import junit.framework.Test;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.SupportFilesSetup;
import org.apache.derbyTesting.junit.TestConfiguration;

public final class CheckConstraintTest extends BaseJDBCTestCase {

    // poached from GeneratedColumnsTest
    private static  final   String  IMPORT_FILE_NAME = "t_bi_1.dat";

    /**
     * Public constructor required for running test as standalone JUnit.
     */
    public CheckConstraintTest(String name)
    {
        super(name);
    }

    public static Test suite()
    {
        BaseTestSuite suite = new BaseTestSuite("checkConstraint Test");
        suite.addTest(TestConfiguration.defaultSuite(CheckConstraintTest.class));

        return new SupportFilesSetup(
             suite,
             new String [] { "functionTests/tests/lang/" + IMPORT_FILE_NAME });
    }
    
    public void testNotAllowedInCheckConstraints() throws Exception
    {
        
        Statement st = createStatement();
        setAutoCommit(false);
        
        // negative The following are not allowed in check 
        // constraints:	?, subquery, datetime functions
        
        assertStatementError("42Y39", st,
            "create table neg1(c1 int check(?))");
        
        assertStatementError("42Y39", st,
            " create table neg1(c1 int check(c1 in (select c1 "
            + "from neg1)))");
        
        assertStatementError("42Y39", st,
            " create table neg1(c1 int check(CURRENT_DATE = "
            + "CURRENT_DATE))");
        
        assertStatementError("42Y39", st,
            " create table neg1(c1 int check(CURRENT_TIME = "
            + "CURRENT_TIME))");
        
        assertStatementError("42Y39", st,
            " create table neg1(c1 int check(CURRENT_TIMESTAMP = "
            + "CURRENT_TIMESTAMP))");
        
        // The check constraint definition must evaluate to a boolean
        
        assertStatementError("42X19", st,
            "create table neg1(c1 int check(c1))");
        
        assertStatementError("42X19", st,
            " create table neg1(c1 int check(1))");
        
        assertStatementError("42X19", st,
            " create table neg1(c1 int check(c1+c1))");
        
        // All column references are to target table
        
        assertStatementError("42X04", st,
            "create table neg1(c1 int check((c2 = 1)))");
        
        // verify that a check constraint can't be used as an 
        // optimizer override
        
        st.executeUpdate(
            "create table t1(c1 int constraint asdf check(c1 = 1))");
        
        assertStatementError("42Y48", st,
            " select * from t1 --derby-properties constraint = asdf ");
        
        // alter table t1 drop constraint asdf
        
        rollback();
        
        // alter table t1 drop constraint asdf forward references 
        // should fail
        
        assertStatementError("42621", st,
            "create table neg1(c1 int check(c2 = 1), c2 int)");
        
        assertStatementError("42621", st,
            " create table neg2(c1 int constraint asdf check(c2 "
            + "= 1), c2 int)");
        
        rollback();
    }
    public void testCheckConstraints() throws SQLException{
        
        Statement st = createStatement();
        setAutoCommit(false);
        
        // positive multiple check constraints on same table
        
        st.executeUpdate(
            "create table pos1(c1 int check(c1 > 0), constraint "
            + "asdf check(c1 < 10))");
        
        // verify both constraints are enforced
        
        assertStatementError("23513", st,
            "insert into pos1 values 0");
        
        st.executeUpdate(
            " insert into pos1 values 1");
        
        st.executeUpdate(
            " insert into pos1 values 9");
        
        assertStatementError("23513", st,
            " insert into pos1 values 10");
        
        ResultSet rs = st.executeQuery("select * from pos1");
        
        String[] expColNames = {"C1"};
        JDBC.assertColumnNames(rs, expColNames);
        
        String[][] expRS = {
            {"1"},
            {"9"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        // verify constraint violation rolls back entire statement
        
        assertStatementError("23513", st,
            "update pos1 set c1 = c1 + 1");
        
        rs = st.executeQuery(
            " select * from pos1");
        
        expColNames = new String [] {"C1"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1"},
            {"9"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        assertStatementError("23513", st,
            " update pos1 set c1 = c1 - 1");
        
        rs = st.executeQuery(
            " select * from pos1");
        
        expColNames = new String [] {"C1"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1"},
            {"9"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rollback();
        
        // conflicting constraints, should fail
        
        st.executeUpdate(
            "create table negcks(c1 int constraint ck1st "
            + "check(c1 > 4), c2 int constraint ck2nd check(c2 > "
            + "2), c3 int, constraint ckLast check(c2 > c1))");
        
        // constraint ck1st fails
        
        assertStatementError("23513", st,
            "insert into negcks values (1, 3, 3)");
        
        // constraint ckLast fails (ck2nd fails too)
        
        assertStatementError("23513", st,
            "insert into negcks values (5, 1, 3)");
        
        // constraint ck1st fails (ckLast fails too)
        
        assertStatementError("23513", st,
            "insert into negcks values (2, 3, 3)");
        
        rollback();
        
        // same source and target tables
        
        st.executeUpdate(
            "create table pos1(c1 int, c2 int, constraint ck1 "
            + "check (c1 < c2))");
        
        st.executeUpdate(
            " insert into pos1 values (1, 2), (2, 3), (3, 4)");
        
        commit();
        // these should work
        
        st.executeUpdate(
            "insert into pos1 select * from pos1");
        
        rs = st.executeQuery(
            " select count(*) from pos1");
        
        expColNames = new String [] {"1"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"6"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        assertUpdateCount(st, 6,
            " update pos1 set c2 = (select max(c1) from pos1), "
            + "c1 = (select min(c2) from pos1)");
        
        rs = st.executeQuery(
            " select * from pos1");
        
        expColNames = new String [] {"C1", "C2"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"2", "3"},
            {"2", "3"},
            {"2", "3"},
            {"2", "3"},
            {"2", "3"},
            {"2", "3"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rollback();
        
        // these should fail
        
        assertStatementError("23513", st,
            "insert into pos1 select c2, c1 from pos1");
        
        rs = st.executeQuery(
            " select count(*) from pos1");
        
        expColNames = new String [] {"1"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"3"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        assertStatementError("23513", st,
            " update pos1 set c2 = (select min(c1) from pos1), "
            + "c1 = (select max(c2) from pos1)");
        
        rs = st.executeQuery(
            " select * from pos1");
        
        expColNames = new String [] {"C1", "C2"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1", "2"},
            {"2", "3"},
            {"3", "4"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        st.executeUpdate(
            " drop table pos1");
        
        commit();
        // union under insert
        
        st.executeUpdate(
            "create table t1(c1 int, c2 int, constraint ck1 "
            + "check(c1 = c2))");
        
        assertStatementError("23513", st,
            " insert into t1 values (1, 1), (2, 1)");
        
        rs = st.executeQuery(
            " select * from t1");
        
        expColNames = new String [] {"C1", "C2"};
        JDBC.assertColumnNames(rs, expColNames);
        JDBC.assertDrainResults(rs, 0);
        
        // normalize result set under insert/update
        
        st.executeUpdate(
            "insert into t1 values (1.0, 1)");
        
        assertStatementError("23513", st,
            " insert into t1 values (2.0, 1)");
        
        rs = st.executeQuery(
            " select * from t1");
        
        expColNames = new String [] {"C1", "C2"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1", "1"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        assertUpdateCount(st, 1,
            " update t1 set c2 = 1.0");
        
        assertStatementError("23513", st,
            " update t1 set c2 = 2.0");
        
        rs = st.executeQuery(
            " select * from t1");
        
        expColNames = new String [] {"C1", "C2"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1", "1"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        assertUpdateCount(st, 1,
            " update t1 set c1 = 3.0, c2 = 3.0");
        
        rs = st.executeQuery(
            " select * from t1");
        
        expColNames = new String [] {"C1", "C2"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"3", "3"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rollback();
    }
    
    public void testPositionalUpdate() throws SQLException{
        
        Statement st = createStatement();
        setAutoCommit(false);
        
        // positioned update
        
        st.executeUpdate(
            "create table t1(c1 int, c2 int, constraint ck1 "
            + "check(c1 = c2), constraint ck2 check(c2=c1))");
        
        st.executeUpdate(
            " insert into t1 values (1, 1), (2, 2), (3, 3), (4, 4)");
        
        st.executeUpdate(
            " create index i1 on t1(c1)");
        
        Statement st1 = createStatement();
        st1.setCursorName("c1");
        ResultSet rs1 = st1.executeQuery(
                "select * from t1 where c2=2 for update of C1");
        rs1.next();   
        setAutoCommit(false);
        
        // this update should succeed
        assertUpdateCount(st,1,
                "update t1 set c1 = c1 where current of \"c1\"");
        
        // this update should fail
        assertStatementError("23513", st,
            "update t1 set c1 = c1 + 1 where current of \"c1\"");
        st1.close();
        rs1.close();
        
        Statement st2 = createStatement();
        st2.setCursorName("c2");
        ResultSet rs2 = st2.executeQuery(
                "select * from t1 where c1 = 2 for update of c2");
        rs2.next();   
        setAutoCommit(false);
        // this update should succeed
        assertUpdateCount(st,1,
                "update t1 set c2 = c2 where current of \"c2\"");
        
        // this update should fail
        assertStatementError("23513", st,
            "update t1 set c2 = c2 + 1 where current of \"c2\"");
        st2.close();
        rs2.close();
        
        Statement st3 = createStatement();
        st3.setCursorName("c3");
        ResultSet rs3 = st3.executeQuery(
                "select * from t1 where c1 = 2 for update of c1, c2");
        rs3.next();   
        setAutoCommit(false);
        
        // this update should succeed
        assertUpdateCount(st, 1,
            "update t1 set c2 = c1, c1 = c2 where current of \"c3\"");
        
        // this update should fail
        assertStatementError("23513", st,
            "update t1 set c2 = c2 + 1, c1 = c1 + 3 where current of \"c3\"");
        
        // this update should succeed
        assertUpdateCount(st, 1,
            "update t1 set c2 = c1 + 3, c1 = c2 + 3 where current of \"c3\"");
        st3.close();
        rs3.close();
        
        ResultSet rs = st.executeQuery("select * from t1");
        
        String[] expColNames = {"C1", "C2"};
        JDBC.assertColumnNames(rs, expColNames);
        
        String[][] expRS = {
            {"1", "1"},
            {"5", "5"},
            {"3", "3"},
            {"4", "4"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rollback();
        
        // complex expressions
        
        st.executeUpdate(
            "create table t1(c1 int check((c1 + c1) = (c1 * c1) "
            + "or (c1 + c1)/2 = (c1 * c1)), c2 int)");
        
        // this insert should succeed
        
        st.executeUpdate(
            "insert into t1 values (1, 9), (2, 10)");
        
        // these updates should succeed
        
        assertUpdateCount(st, 2,
            "update t1 set c2 = c2 * c2");
        
        assertUpdateCount(st, 1,
            " update t1 set c1 = 2 where c1 = 1");
        
        assertUpdateCount(st, 2,
            " update t1 set c1 = 1 where c1 = 2");
        
        // this update should fail
        
        assertStatementError("23513", st,
            "update t1 set c1 = c2");
        
        rs = st.executeQuery(
            " select * from t1");
        
        expColNames = new String [] {"C1", "C2"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1", "81"},
            {"1", "100"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rollback();
    }
    
    public void testBuiltInFunctions() throws SQLException{
        
        Statement st = createStatement();
        setAutoCommit(false);
        
        // built-in functions in a check constraint
        
        st.executeUpdate(
            "create table charTab (c1 char(4) check(CHAR(c1) = c1))");
        
        st.executeUpdate(
            " insert into charTab values 'asdf'");
        
        st.executeUpdate(
            " insert into charTab values 'fdsa'");
        
        // beetle 5805 - support built-in function INT should fail 
        // until beetle 5805 is implemented
        
        st.executeUpdate(
            "create table intTab (c1 int check(INT(1) = c1))");
        
        st.executeUpdate(
            " insert into intTab values 1");
        
        // this insert should fail, does not satisfy check constraint
        
        assertStatementError("23513", st,
            "insert into intTab values 2");
        
        st.executeUpdate(
            " create table maxIntTab (c1 int check(INT(2147483647) > c1))");
        
        st.executeUpdate(
            " insert into maxIntTab values 1");
        
        // this insert should fail, does not satisfy check constraint
        
        assertStatementError("23513", st,
            "insert into maxIntTab values 2147483647");
        
        rollback();
        
        // verify that inserts, updates and statements with forced 
        // constraints are indeed dependent on the constraints
        
        st.executeUpdate(
            "create table t1(c1 int not null constraint asdf primary key)");
        
        st.executeUpdate(
            " insert into t1 values 1, 2, 3, 4, 5");
        
        commit();
        
        PreparedStatement pSt1 = prepareStatement(
            "insert into t1 values 1");
        
        PreparedStatement pSt2 = prepareStatement(
            "update t1 set c1 = 3 where c1 = 4");
        
        PreparedStatement pSt3 = prepareStatement(
            "select * from t1");
        
        // the insert and update should fail, select should succeed
        
        assertStatementError("23505", pSt1);
        
        assertStatementError("23505", pSt2);

        
        ResultSet rs = pSt3.executeQuery();
        
        String[] expColNames = {"C1"};
        JDBC.assertColumnNames(rs, expColNames);
        
        String[][] expRS = {
            {"1"},
            {"2"},
            {"3"},
            {"4"},
            {"5"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        st.executeUpdate(
            " alter table t1 drop constraint asdf");
        
        // rollback and verify that constraints are enforced and 
        // select succeeds
        
        rollback();
        
        assertStatementError("23505", pSt1);
        
        assertStatementError("23505", pSt2);
        
        rs = pSt3.executeQuery();
        expColNames = new String [] {"C1"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1"},
            {"2"},
            {"3"},
            {"4"},
            {"5"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        
        
        
        st.executeUpdate(
            " drop table t1");
        
        // check constraints with parameters
        
        st.executeUpdate(
            "create table t1(c1 int constraint asdf check(c1 = 1))");
        
        PreparedStatement pSt = prepareStatement(
            "insert into t1 values (?)");
        
        rs = st.executeQuery(
            "values (1)");
        
        rs.next();
        ResultSetMetaData rsmd = rs.getMetaData();
        for (int i = 1; i <= rsmd.getColumnCount(); i++)
            pSt.setObject(i, rs.getObject(i));
        
        assertUpdateCount(pSt, 1);
        
        // clean up
        
        st.executeUpdate(
            "drop table t1");
        
        st.executeUpdate(
            " create table t1(active_flag char(2) "
            + "check(active_flag IN ('Y', 'N')), "
            + "araccount_active_flag char(2) "
            + "check(araccount_active_flag IN ('Y', 'N')), "
            + "automatic_refill_flag char(2) "
            + "check(automatic_refill_flag IN ('Y', 'N')), "
            + "call_when_ready_flag char(2) "
            + "check(call_when_ready_flag IN ('Y', 'N')), "
            + "compliance_flag char(2) check(compliance_flag IN "
            + "('Y', 'N')), delivery_flag char(2) "
            + "check(delivery_flag IN ('Y', 'N')), "
            + "double_count_flag char(2) check(double_count_flag "
            + "IN ('Y', 'N')), gender_ind char(2) check(gender_ind "
            + "IN ('M', 'F', 'U')), geriatric_flag char(2) "
            + "check(geriatric_flag IN ('Y', 'N')), "
            + "refuse_inquiry_flag char(2) "
            + "check(refuse_inquiry_flag IN ('Y', 'N')), "
            + "animal_flag char(2) check(animal_flag IN ('Y', "
            + "'N')), terminal_flag char(2) check(terminal_flag IN "
            + "('Y', 'N')), unit_flag char(2) check(unit_flag IN "
            + "('Y', 'N')), VIP_flag char(2) check(VIP_flag IN "
            + "('Y', 'N')), snap_cap_flag char(2) "
            + "check(snap_cap_flag IN ('Y', 'N')), "
            + "consent_on_file_flag char(2) "
            + "check(consent_on_file_flag IN ('Y', 'N')), "
            + "enlarged_SIG_flag char(2) check(enlarged_SIG_flag "
            + "IN ('Y', 'N')),aquired_patient_flag char(2) "
            + "check(aquired_patient_flag IN ('Y', 'N')))");
        
        // bug 5622 - internal generated constraint names are 
        // re-worked to match db2's naming convention.
        
        st.executeUpdate(
            "drop table t1");
        
        st.executeUpdate(
            " create table t1 (c1 int not null primary key, c2 "
            + "int not null unique, c3 int check (c3>=0))");
        
        st.executeUpdate(
            " alter table t1 add column c4 int not null default 1");
        
        st.executeUpdate(
            " alter table t1 add constraint c4_unique UNIQUE(c4)");
        
        st.executeUpdate(
            " alter table t1 add column c5 int check(c5 >= 0)");
        
        rs = st.executeQuery(
            " select  c.type from "
            + "sys.sysconstraints c, sys.systables t where "
            + "c.tableid = t.tableid and tablename='T1'");
        
        expColNames = new String [] {"TYPE"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"P"},
            {"U"},
            {"C"},
            {"U"},
            {"C"}
        };
        
        JDBC.assertUnorderedResultSet(rs, expRS, true);
        
        assertStatementError("42Y55", st,
            " drop table t2");
        
        st.executeUpdate(
            " create table t2 (c21 int references t1)");
        
        rs = st.executeQuery(
            " select c.type from "
            + "sys.sysconstraints c, sys.systables t where "
            + "c.tableid = t.tableid and tablename='T2'");
        
        expColNames = new String [] {"TYPE"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"F"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        assertStatementError("42Y55", st,
            " drop table t3");
        
        st.executeUpdate(
            " create table t3 (c1 int check (c1 >= 0), c2 int "
            + "check (c2 >= 0), c3 int check (c3 >= 0), c4 int "
            + "check (c4 >= 0), c5 int check (c5 >= 0), c6 int "
            + "check (c6 >= 0), c7 int check (c7 >= 0), c8 int "
            + "check (c8 >= 0), c9 int check (c9 >= 0), c10 int "
            + "check (c10 >= 0), c11 int check (c11 >= 0), c12 int "
            + "check (c12 >= 0), c13 int check (c13 >= 0))");
        
        rs = st.executeQuery(
            " select c.type from "
            + "sys.sysconstraints c, sys.systables t where "
            + "c.tableid = t.tableid and tablename='T3'");
        
        expColNames = new String [] {"TYPE"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"C"},
            {"C"},
            {"C"},
            {"C"},
            {"C"},
            {"C"},
            {"C"},
            {"C"},
            {"C"},
            {"C"},
            {"C"},
            {"C"},
            {"C"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        assertStatementError("42Y55", st,
            " drop table t4");
        
        st.executeUpdate(
            " create table t4(c11 int not null, c12 int not "
            + "null, primary key (c11, c12))");
        
        rs = st.executeQuery(
            " select c.type from "
            + "sys.sysconstraints c, sys.systables t where "
            + "c.tableid = t.tableid and tablename='T4'");
        
        expColNames = new String [] {"TYPE"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"P"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        // Cleanup:
        st.executeUpdate("drop table t4");
        st.executeUpdate("drop table t3");
        st.executeUpdate("drop table t2");
        st.executeUpdate("drop table t1");
        commit();
        
        // DERBY-2989
    }
    public void testJira2989() throws SQLException{
        
        Statement st = createStatement();
        setAutoCommit(false);
        
        st.executeUpdate(
            "CREATE TABLE \"indicator\" (c CHAR(1) DEFAULT 'N')");
        
        st.executeUpdate(
            " ALTER TABLE  \"indicator\" ADD CONSTRAINT "
            + "my_constraint CHECK ((c IN ('Y','N')))");
        
        st.executeUpdate(
            " INSERT INTO  \"indicator\" VALUES ('N')");
        
        st.executeUpdate(
            " ALTER TABLE  \"indicator\" DROP CONSTRAINT my_constraint");
        
        st.executeUpdate(
            " DROP TABLE   \"indicator\"");
             
        getConnection().rollback();
        st.close();
    }
    public void testJira4282() throws SQLException
    {
        // This test doesnt work properly in the embedded configuration.
        // The intent of the test is to expose the DERBY-4282 problem, and
        // this test case does do that in the client/server configuration, so
        // we only run the test in that configuration. In the embedded
        // configuration, the UPDATE statement unexpectedly gets a 
        // "no current row" exception.
        //
        if (usingEmbedded())
            return;

        Statement st = createStatement();

        st.executeUpdate(
            "create table t4282(c1 int, c2 int, constraint ck1 "
            + "check(c1 = c2), constraint ck2 check(c2=c1))");

        st.executeUpdate("insert into t4282 values (1,1),(2,2),(3,3),(4,4)");

        Statement st1 = createStatement();
        st1.setCursorName("c1");
        ResultSet rs = st1.executeQuery("select * from t4282 for update");
        assertTrue("Failed to retrieve row for update", rs.next());
        // DERBY-4282 causes the next statement to fail with:
        //
        // Column 'C2' is either not in any table in the FROM list or
        // appears within a join specification and is outside the scope
        // of the join specification or appears in a HAVING clause and
        // is not in the GROUP BY list. If this is a CREATE or ALTER TABLE
        // statement then 'C2' is not a column in the target table. 
        st.executeUpdate("update t4282 set c1 = c1 where current of \"c1\"");

        // If we get here, all is well, and DERBY-4282 did not occur.
        st1.close();
        st.close();

        dropTable("t4282");
    }
    // This test verifies that if the PRIMARY KEY constraint mentions a
    // column which is potentially large, then Derby will automatically
    // choose a large pagesize for the index's conglomerate (DERBY-3947)
    //
    public void testPrimaryKeyPageSizeDerby3947()
        throws SQLException
    {
        Statement st = createStatement();
        st.executeUpdate("create table d3947 (x varchar(1000) primary key)");
        char[] chars = new char[994];
        PreparedStatement ps = prepareStatement("insert into d3947 values (?)");
        ps.setString(1, new String(chars));
        ps.executeUpdate();
        ps.close();
        checkLargePageSize(st, "D3947");
        st.executeUpdate("drop table d3947");

        // A second variation is to add the PK constraint using ALTER TABLE;
        // A third variation is to add a FK constraint
        st.executeUpdate("create table d3947 (x varchar(1000) not null, " +
                " y varchar(1000))");
        st.executeUpdate("alter table d3947 add constraint " +
                "constraint1 primary key (x)");
        st.executeUpdate("alter table d3947 add constraint " +
                "constraint2 foreign key (y) references d3947(x)");
        checkLargePageSize(st, "D3947");
        // Ensure we still get the right error message when col doesn't exist:
        assertStatementError("42X14", st,
                "alter table d3947 add constraint " +
                "constraint3 foreign key (z) references d3947(x)");
        st.executeUpdate("drop table d3947");

        st.close();
    }
    private void checkLargePageSize(Statement st, String tblName)
        throws SQLException
    {
        ResultSet rs = st.executeQuery(
            "select * from TABLE(SYSCS_DIAG.SPACE_TABLE('"+tblName+"')) T");
        while (rs.next())
        {
            if ("1".equals(rs.getString("isindex")))
                assertEquals(32768, rs.getInt("pagesize"));
            else
                assertEquals(4096, rs.getInt("pagesize"));

            //System.out.println(rs.getString("conglomeratename") +
            //        ","+rs.getString("isindex")+
            //        ","+rs.getString("pagesize"));
        }
        rs.close();
    }

    /**
     * Test that CHECK constraint works if it contains unqualified names and
     * the current schema when the constraint is defined is different from the
     * schema in which the table lives. Regression test case for DERBY-6362.
     */
    public void testDerby6362() throws SQLException {
        setAutoCommit(false);
        Statement s = createStatement();
        s.execute("create schema d6362_s1");
        s.execute("create schema d6362_s2");

        s.execute("set schema d6362_s1");
        s.execute("create function f(x int) returns int deterministic "
                + "language java  parameter style java external name "
                + "'java.lang.Math.abs' no sql");
        s.execute("create type typ "
                + "external name 'java.util.ArrayList' language java");

        // Create the table with the constraints in a different schema than
        // the current schema. Before DERBY-6362, unqualified names would be
        // resolved to the current schema at definition time and to the
        // table's schema during execution, which made them behave unreliably
        // if the schemas differed.
        s.execute("create table d6362_s2.t(x int, "
                + "constraint c001 check(f(x) < 3))");
        s.execute("alter table d6362_s2.t "
                + "add constraint c002 check(f(x) >= 0)");
        s.execute("alter table d6362_s2.t "
                + "add constraint c003 check(cast(null as typ) is null)");

        // Use a function that lives in the SYSFUN schema.
        s.execute("alter table d6362_s2.t add constraint c004 "
                + "check(f(x) > cos(pi()))");

        // ABS is an operator, not a function, so it will not be qualified.
        s.execute("alter table d6362_s2.t add constraint c005 "
                + "check(abs(f(x)) < pi())");

        // Add some constraints that reference the table. See that table
        // names are qualified. Unqualified column names will not be qualified
        // with schema and table.
        s.execute("set schema d6362_s2");
        s.execute("alter table t add constraint c101 check(x < 3)");
        s.execute("alter table t add constraint c102 check(t.x < 4)");
        s.execute("alter table t add constraint c103 "
                + "check(x <= d6362_s1.f(t.x))");

        // Add some fully qualified names to see that they still work.
        s.execute("alter table t add constraint c201 check(d6362_s2.t.x < 5)");
        s.execute("alter table t add constraint c202 check(d6362_s1.f(x) < 5)");
        s.execute("alter table t add constraint c203 "
                + "check(cast(null as d6362_s1.typ) is null)");

        // Verify that the constraints were stored with fully qualified names.
        String[][] expectedConstraints = {
            {"C001", "(\"D6362_S1\".\"F\"(x) < 3)"},
            {"C002", "(\"D6362_S1\".\"F\"(x) >= 0)"},
            {"C003", "(cast(null as \"D6362_S1\".\"TYP\") is null)"},
            {"C004", "(\"D6362_S1\".\"F\"(x) > \"SYSFUN\".\"COS\"(\"SYSFUN\".\"PI\"()))"},
            {"C005", "(abs(\"D6362_S1\".\"F\"(x)) < \"SYSFUN\".\"PI\"())"},
            {"C101", "(x < 3)"},
            {"C102", "(\"D6362_S2\".\"T\".x < 4)"},
            {"C103", "(x <= \"D6362_S1\".\"F\"(\"D6362_S2\".\"T\".x))"},
            {"C201", "(\"D6362_S2\".\"T\".x < 5)"},
            {"C202", "(\"D6362_S1\".\"F\"(x) < 5)"},
            {"C203", "(cast(null as \"D6362_S1\".\"TYP\") is null)"},
        };

        JDBC.assertFullResultSet(
            s.executeQuery(
                "select constraintname, checkdefinition from sys.syschecks "
                + "natural join sys.sysconstraints natural join sys.sysschemas "
                + "where schemaname = 'D6362_S2' and type = 'C' "
                + "order by constraintname"),
            expectedConstraints);

        // Verify that constraints can be executed. Used to fail because
        // unqualified functions and types were resolved to the table's schema
        // instead of the current schema at the time the constraint was defined.
        s.execute("insert into t values 1,2");
        assertStatementError("23513", s, "insert into t values -10");
        assertStatementError("23513", s, "insert into t values 10");
    }

    private void setupForBulkInsert(Statement s, int limit)
            throws SQLException {
        s.executeUpdate(
            "create table t_bi_1( a int, b int check (b < " + limit + "))");

        s.executeUpdate(
            "create function func () returns int " + 
            "    language java parameter style java deterministic no sql " +
            "    external name '" + this.getClass().getName() + ".func'");
    }

    /**
     * DERBY-6453. Exercise hitherto untested code path in
     * InsertResultSet (call to evaluateCheckConstraints from
     * preprocessSourceRow used by bulkInsert) and verify IMPORT with
     * trigger (which would a priori mandate bulk insert, but is
     * changed due to normal inserts due to the presence of a
     * trigger).
     */
    public void testbulkInsert() throws SQLException {

        setAutoCommit(false);
        Statement s = createStatement();

        setupForBulkInsert(s, 0);

        assertStatementError(
            "23513", s,
            "call syscs_util.syscs_import_data( " + 
            "    null, " + 
            "    'T_BI_1', " + 
            "    'A, B', " + 
            "    '1, 2', " + 
            "    'extin/" + IMPORT_FILE_NAME + "', " + 
            "    null, null, null, 0 )");

        setupForBulkInsert(s, 10);

        s.executeUpdate(
            "create trigger dagstrigger no cascade before insert on t_bi_1 " +
            "    values func() ");

        funcWasCalled = false;

        s.executeUpdate(
            "call syscs_util.syscs_import_data( " + 
            "    null, " + 
            "    'T_BI_1', " + 
            "    'A, B', " + 
            "    '1, 2', " + 
            "    'extin/" + IMPORT_FILE_NAME + "', " + 
            "    null, null, null, 0 )");

        assertTrue(funcWasCalled);
    }

    static boolean funcWasCalled;
    
    public static int func()
    {
        funcWasCalled = true;
        return 0;
    }

}
