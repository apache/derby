/*
Derby - Class org.apache.derbyTesting.functionTests.tests.lang.ColumnDefaultsTest

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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import junit.framework.Test;
import org.apache.derbyTesting.functionTests.util.Formatters;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.TestConfiguration;

public final class ColumnDefaultsTest extends BaseJDBCTestCase {

    /**
     * Public constructor required for running test as standalone JUnit.
     */
    public ColumnDefaultsTest(String name)
    {
        super(name);
    }

    public static Test suite()
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-6590
        BaseTestSuite suite = new BaseTestSuite("ColumnDefaultsTest Test");
        suite.addTest(TestConfiguration.defaultSuite(ColumnDefaultsTest.class));
        return suite;
    }

    public void testNegativeTestsForColumnDefaults() throws Exception
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-5714
        Statement st = createStatement();
        
        //? in default
        assertStatementError("42X01", st,
            "create table neg(c1 int default ?)");
        
        // column reference in default
        
        assertStatementError("42X01", st,
            "create table neg(c1 int, c2 int default c1)");
        
        // subquery in default
        
        assertStatementError("42X01", st,
            "create table neg(c1 int default (values 1))");
        
        // type incompatibility at compile time
        
        assertStatementError("42821", st,
            "create table neg(c1 date default 1)");
        
        // type incompatibility at execution time bug 5585 - 
        // should fail at create table statement because the 
        // default value '1' is not valid
        
        st.executeUpdate(
            "create table neg(c1 int, c2 date default '1')");
        
        assertStatementError("22007", st, " insert into neg (c1) values 1");
        
        st.executeUpdate( " drop table neg");
        
        // bug 5203 - built-in functions are not be allowed in a 
        // constantExpression because DB2 UDB returns SQLSTATE 42894
        
        st.executeUpdate(
            "CREATE FUNCTION ASDF (DATA DOUBLE) RETURNS DOUBLE "
            + "EXTERNAL NAME 'java.lang.Math.sin' LANGUAGE JAVA "
            + "PARAMETER STYLE JAVA");
        
        //DEFAULT value or IDENTITY attribute value is not valid 
        
        assertStatementError("42894", st,
            " create table neg(c1 int default asdf(0))");
        
        //drop a table which does not exists fails
        assertStatementError("42Y55", st, " drop table neg");
        
        // DEFAULT only valid in VALUES within an insert
        
        assertStatementError("42Y85", st, "values default");
        
        assertStatementError("42Y85", st, " values 1, default");
        
        // alter table modify default
        
        st.executeUpdate( "create table neg(c1 date)");
        
        assertStatementError("42X01", st,
            " alter table neg modify x default null");
        
        assertStatementError("42821", st,
            " alter table neg add column x date default 1");
        
        // bug 5585 - should fail at alter table statement because 
        // the default value '1' is not valid
        
        st.executeUpdate( "alter table neg add column x date default '1'");
        
        assertStatementError("22007", st,
            " insert into neg (c1) values default");
        
        st.executeUpdate( " drop table neg");
        
        // too many values in values clause
        
        st.executeUpdate( "create table neg(c1 int default 10)");
        
        assertStatementError("42X06", st,
            " insert into neg values (1, default)");
        
        assertStatementError("42802", st,
            " insert into neg values (default, 1)");


        // DERBY-4426
//IC see: https://issues.apache.org/jira/browse/DERBY-4426
        assertStatementError("42Y85", st,
            " insert into neg values (default) union values (default)");

        assertStatementError("42Y85", st,
            " insert into neg values (default) except values (default)");

        // Make sure sub-queries are inspected for illegal DEFAULT also.
        assertStatementError("42Y85", st,
            " insert into neg select * from (values default) t");

        st.executeUpdate( " drop table neg");
        
        st.executeUpdate( "drop function asdf");
        
        rollback();
        st.close();
    }
    
    public void testPositiveTestsForColumnDefaults()
        throws SQLException
    {
        
        // positive 
//IC see: https://issues.apache.org/jira/browse/DERBY-5714
        Statement st = createStatement();
        
        //create tables
        
        st.executeUpdate(
            "create table t1(c1 int, c2 int with default 5, c3 "
            + "date default current_date, c4 int)");
        
        // verify that defaults work
        
        st.executeUpdate( "insert into t1 (c1) values 1");
        
        st.executeUpdate( " insert into t1 (c4) values 4");
        
//IC see: https://issues.apache.org/jira/browse/DERBY-5714
        ResultSet rs = st.executeQuery("select c1, c2, c4 from t1");
        
        String[] expColNames = {"C1", "C2", "C4"};
        JDBC.assertColumnNames(rs, expColNames);
        
        String[][] expRS = {
            {"1", "5", null},
            {null, "5", "4"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select c1, c2, c4 from t1 where c3 = current_date");
        
        expColNames = new String [] {"C1", "C2", "C4"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1", "5", null},
            {null, "5", "4"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        // update default for column whose default is null
        
        assertUpdateCount(st, 2, "update t1 set c1 = default");
        
        rs = st.executeQuery(
            " select c1, c2, c4 from t1 where c3 = current_date");
        
        expColNames = new String [] {"C1", "C2", "C4"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {null, "5", null},
            {null, "5", "4"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        // default for column that has explicit default
        
        assertUpdateCount(st, 2, "update t1 set c2 = 7");
        
        rs = st.executeQuery( " select c2 from t1");
        
        expColNames = new String [] {"C2"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"7"},
            {"7"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        assertUpdateCount(st, 2, " update t1 set c2 = default");
        
        rs = st.executeQuery( " select c2 from t1");
        
        expColNames = new String [] {"C2"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"5"},
            {"5"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        // insert default
        
        assertUpdateCount(st, 2, "delete from t1");
        
        st.executeUpdate(
            " insert into t1 values (5, default, '1999-09-09', default)");
        
        st.executeUpdate(
            " insert into t1 values (default, 6, default, 5)");
        
        st.executeUpdate(
            " insert into t1 values (default, 6, default, 5), "
            + "(7, default, '1997-07-07', 3)");
        
        rs = st.executeQuery(
            " select c1, c2, c4 from t1 where c3 = current_date");
        
        expColNames = new String [] {"C1", "C2", "C4"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {null, "6", "5"},
            {null, "6", "5"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select c1, c2, c4 from t1 where c3 <> current_date");
        
        expColNames = new String [] {"C1", "C2", "C4"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"5", "5", null},
            {"7", "5", "3"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        assertUpdateCount(st, 4, " delete from t1");
        
        st.executeUpdate(
            " insert into t1 (c1, c3, c4) values (5, "
            + "'1999-09-09', default)");
        
        st.executeUpdate(
            " insert into t1 (c1, c3, c4) values (default, default, 5)");
        
        st.executeUpdate(
            " insert into t1 (c1, c3, c4) values (default, "
            + "default, default)");
        
        st.executeUpdate(
            " insert into t1 (c1, c3, c4) values (default, "
            + "default, 5), (7, '1997-07-07', 3)");
        
        rs = st.executeQuery(
            " select c1, c2, c4 from t1 where c3 = current_date");
        
        expColNames = new String [] {"C1", "C2", "C4"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {null, "5", "5"},
            {null, "5", null},
            {null, "5", "5"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select c1, c2, c4 from t1 where c3 <> current_date");
        
        expColNames = new String [] {"C1", "C2", "C4"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"5", "5", null},
            {"7", "5", "3"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        // delimited identifiers this schema
        
        st.executeUpdate( "create table \"x1\" (\"c1\" int)");
        
        st.executeUpdate( " insert into \"x1\" values 1");
        
        st.executeUpdate(
            " alter table \"x1\" add column \"c2\" char(1) default 'x'");
        
        rs = st.executeQuery( " select * from \"x1\"");
        
        expColNames = new String [] {"c1", "c2"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1", "x"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        // another schema
        
        st.executeUpdate( "create schema \"otherschema\"");
        
        st.executeUpdate(
            " create table \"otherschema\".\"y1\" (\"c11\" int)");
        
        st.executeUpdate(
            " insert into \"otherschema\".\"y1\" values 2");
        
        st.executeUpdate(
            " alter table \"otherschema\".\"y1\" add column "
            + "\"c22\" char(1) default 'y'");
        
        rs = st.executeQuery(
            " select * from \"otherschema\".\"y1\"");
        
        expColNames = new String [] {"c11", "c22"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"2", "y"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        // bug 3433
        
        st.executeUpdate( "create table t7(c1 int default 10)");
        
        st.executeUpdate( " insert into t7 values (default)");
        
        rs = st.executeQuery( " select * from t7");
        
        expColNames = new String [] {"C1"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"10"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);

        // DERBY-4426: make sure we don't forbid inside a multi-value table
        // constructor since this is represented as a UnionNode
//IC see: https://issues.apache.org/jira/browse/DERBY-4426
        st.executeUpdate( "delete from t7");
        st.executeUpdate( "insert into t7 values 1, default");
        rs = st.executeQuery( " select * from t7");

        expRS = new String [][]
        {
            {"1"},
            {"10"}
        };

        JDBC.assertFullResultSet(rs, expRS, true);

        st.executeUpdate( " drop table t1");
        
        st.executeUpdate( " drop table t7");
        
        st.executeUpdate( " drop table \"x1\"");
        
        st.executeUpdate( " drop table \"otherschema\".\"y1\"");
        
        st.executeUpdate( " drop schema \"otherschema\" restrict");
        
        rollback();
        st.close();
        
    }
    public void testJira331()
        throws SQLException
    {
        // JIRA issue Derby-331
//IC see: https://issues.apache.org/jira/browse/DERBY-5714
        Statement st = createStatement();
        
        st.executeUpdate(
            "create table t_331 (a int not null, b int default "
            + "0, unique (a))");
        
        st.executeUpdate( " insert into t_331 values (4, default)");
        
        assertStatementError("23505", st,
            " insert into t_331 values (4, default)");
        
//IC see: https://issues.apache.org/jira/browse/DERBY-5714
        ResultSet rs = st.executeQuery( " select * from t_331");
        
        String[] expColNames = {"A", "B"};
        JDBC.assertColumnNames(rs, expColNames);
        
        String[][] expRS = {
            {"4", "0"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
         st.executeUpdate( " drop table t_331");
         
        rollback();
        st.close();
    }
    
    public void testJira3013()
        throws SQLException
    {
        // begin DERBY-3013
//IC see: https://issues.apache.org/jira/browse/DERBY-5714
        Statement st = createStatement();
        
        st.executeUpdate(
            "create table tabWithUserAndSchemaDefaults("
            + "             cUser           CHAR(8) default user,"
            + "             cCurrent_user   CHAR(8) default current_user,"
            + "             cSession_user   CHAR(8) default session_user,"
            + "             cCurrent_schema CHAR(128) default "
            + "current schema)");
        
        // Should work
        
        st.executeUpdate(
            "insert into tabWithUserAndSchemaDefaults values "
            + "(default, default, default, default)");
        
//IC see: https://issues.apache.org/jira/browse/DERBY-5714
        ResultSet rs = st.executeQuery(
            " select * from tabWithUserAndSchemaDefaults");
        
        String[] expColNames = {"CUSER", "CCURRENT_USER",
            "CSESSION_USER", "CCURRENT_SCHEMA"};
        JDBC.assertColumnNames(rs, expColNames);
        
        String[][] expRS = {
            {"APP", "APP", "APP", "APP"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        // Should fail:DEFAULT value or IDENTITY attribute value is not valid
        
        assertStatementError("42894", st,
            "create table tabWithUserDefaultTooNarrowColumn("
            + "       c1 CHAR(7) default user)");
        
        // Should fail:DEFAULT value or IDENTITY attribute value is not valid
        
        assertStatementError("42894", st,
            "create table tabWithSchemaDefaultTooNarrowColumn("
            + "       c1 CHAR(127) default current sqlid)");
        
        st.executeUpdate(
            " drop table tabWithUserAndSchemaDefaults");
               
//IC see: https://issues.apache.org/jira/browse/DERBY-5714
//IC see: https://issues.apache.org/jira/browse/DERBY-5714
//IC see: https://issues.apache.org/jira/browse/DERBY-5714
//IC see: https://issues.apache.org/jira/browse/DERBY-5714
        rollback();
        st.close();
    }

    public void testDerby118_and_5829 () throws SQLException {
        Statement st = createStatement();
//IC see: https://issues.apache.org/jira/browse/DERBY-5829

        /*
         * Lift restriction that even VARCHAR can max have 254 chars in
         * DEFAULT, another DB2ism.
         */
        String sb = Formatters.repeatChar("-", 400);

        st.executeUpdate(
            "create table tabLongVarchar("
            + "    c varchar(400) default " + "'" + sb + "')");
        st.executeUpdate(
            "insert into  tabLongVarchar" +
            "    values default");

        ResultSet rs = st.executeQuery("select c from tabLongVarchar");
        JDBC.assertSingleValueResultSet(rs, sb);

        // Negative test: Make sure we still flag too long string for CHAR
        // (length > 254).  Note that the check will now only happen at insert
        // time, as other truncation checks do.
        st.executeUpdate(
            "create table tabLongChar("
            + "    c char(254) default " + "'" + sb + "')");
        try {
            st.executeUpdate(
                "insert into tabLongChar values default");
            fail();
        } catch (SQLException e) {
            assertSQLState("22001", e); // truncate
        }

        // Interestingly, a float literal for SMALLINT is not disabled for
        // assignment to integer types.  This is asymmetric, as it is forbidden
        // for the other integer types, as per the standard, btw.
        // Remove this test if we forbid this, or add tests for the other
        // integer types if we decide to allow it, cf discussions on
        // DERBY-118. For now, we just document this weirdness in this test.
        st.executeUpdate(
            "create table tabSmallIntFloat(si smallint default 3.14)");
        st.executeUpdate(
            "insert into  tabSmallIntFloat values default");
        rs = st.executeQuery("select si from tabSmallIntFloat");
        JDBC.assertSingleValueResultSet(rs, "3");

        // Compare with INT, which fails:
        try {
            st.executeUpdate(
                "create table tabIntFloat(i int default 3.14)");
            fail();
        } catch (SQLException e) {
            assertSQLState("42894", e);
        }
    }
}
