/*
 *
 * Derby - Class org.apache.derbyTesting.functionTests.tests.lang.SubqueryTest
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, 
 * software distributed under the License is distributed on an 
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, 
 * either express or implied. See the License for the specific 
 * language governing permissions and limitations under the License.
 */
package org.apache.derbyTesting.functionTests.tests.lang;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.DatabasePropertyTestSetup;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.RuntimeStatisticsParser;
import org.apache.derbyTesting.junit.SystemPropertyTestSetup;

/**
 * Test case for subquery.sql.
 */
public class SubqueryTest extends BaseJDBCTestCase {

    public SubqueryTest(String name) {
        super(name);
    }

    public static Test suite() {

        Properties props = new Properties();

        props.setProperty("derby.language.statementCacheSize", "0");
        return new DatabasePropertyTestSetup(
            new SystemPropertyTestSetup(new CleanDatabaseTestSetup(
               new TestSuite(SubqueryTest.class, "SubqueryTest")) {

                    /**
                     * @see org.apache.derbyTesting.junit.CleanDatabaseTestSetup#decorateSQL(java.sql.Statement)
                     */
                    protected void decorateSQL(Statement s) throws SQLException {
                        s.execute(
                            "CREATE FUNCTION ConsistencyChecker() " +
                            "RETURNS VARCHAR(128) " +
                            "EXTERNAL NAME " +
                            "'org.apache.derbyTesting.functionTests." +
                            "util.T_ConsistencyChecker.runConsistencyChecker' " +
                            "LANGUAGE JAVA PARAMETER STYLE JAVA");
                        s.execute("create table s " +
                            "(i int, s smallint, c char(30), " +
                                "vc char(30), b bigint)");
                        s.execute("create table t " +
                            "(i int, s smallint, c char(30), " +
                                "vc char(30), b bigint)");
                        s.execute("create table tt " +
                            "(ii int, ss smallint, cc char(30), " +
                                "vcvc char(30), b bigint)");
                        s.execute("create table ttt " +
                            "(iii int, sss smallint, ccc char(30), " +
                                "vcvcvc char(30))");
                        // populate the tables
                        s.execute("insert into s values " +
                            "(null, null, null, null, null)");
                        s.execute("insert into s values (0, 0, '0', '0', 0)");
                        s.execute("insert into s values (1, 1, '1', '1', 1)");
                        s.execute("insert into t values " +
                            "(null, null, null, null, null)");
                        s.execute("insert into t values (0, 0, '0', '0', 0)");
                        s.execute("insert into t values (1, 1, '1', '1', 1)");
                        s.execute("insert into t values (1, 1, '1', '1', 1)");
                        s.execute("insert into t values (2, 2, '2', '2', 1)");
                        s.execute("insert into tt values " +
                            "(null, null, null, null, null)");
                        s.execute("insert into tt values (0, 0, '0', '0', 0)");
                        s.execute("insert into tt values (1, 1, '1', '1', 1)");
                        s.execute("insert into tt values (1, 1, '1', '1', 1)");
                        s.execute("insert into tt values (2, 2, '2', '2', 1)");
                        s.execute("insert into ttt values (null, null, null, null)");
                        s.execute("insert into ttt values (11, 11, '11', '11')");
                        s.execute("insert into ttt values (11, 11, '11', '11')");
                        s.execute("insert into ttt values (22, 22, '22', '22')");
                    }

                }, props), props, true);
    }

    /**
     * exists non-correlated negative tests "mis"qualified all
     * 
     * @throws Exception
     */
    public void testExistsNonCorrelated() throws Exception {
        Statement st = createStatement();
        ResultSet rs = null;
        String[][] expRS;
        String[] expColNames;
        assertStatementError("42X10", st,
        "select * from s where exists (select tt.* from t)");

        assertStatementError("42X10", st,
            "select * from s where exists (select t.* from t tt)");

        // invalid column reference in select list
        assertStatementError("42X04", st,
            "select * from s where exists (select nosuchcolumn from t)");

        // multiple matches at subquery level
        assertStatementError("42X03", st,
            "select * from s where exists (select i from s, t)");

        // ? parameter in select list of exists subquery
        assertStatementError("42X34", st,
            "select * from s where exists (select ? from s)");

        // positive tests
        // qualified *
        rs = st.executeQuery(
            "select * from s where exists (select s.* from t)");
        expColNames = new String[] { "I", "S", "C", "VC", "B" };
        JDBC.assertColumnNames(rs, expColNames);
        expRS = new String[][] { { null, null, null, null, null },
                { "0", "0", "0", "0", "0" }, { "1", "1", "1", "1", "1" } };
        JDBC.assertFullResultSet(rs, expRS, true);

        rs = st.executeQuery(
            "select * from s t where exists (select t.* from t)");
        expColNames = new String[] { "I", "S", "C", "VC", "B" };
        JDBC.assertColumnNames(rs, expColNames);
        expRS = new String[][] { { null, null, null, null, null },
                { "0", "0", "0", "0", "0" }, { "1", "1", "1", "1", "1" } };
        JDBC.assertFullResultSet(rs, expRS, true);

        rs = st.executeQuery(
            "select * from s u where exists (select u.* from t)");
        expColNames = new String[] { "I", "S", "C", "VC", "B" };
        JDBC.assertColumnNames(rs, expColNames);
        expRS = new String[][] { { null, null, null, null, null },
                { "0", "0", "0", "0", "0" }, { "1", "1", "1", "1", "1" } };
        JDBC.assertFullResultSet(rs, expRS, true);

        // column reference in select list
        rs = st.executeQuery("select * from s where exists (select i from t)");
        expColNames = new String[] { "I", "S", "C", "VC", "B" };
        JDBC.assertColumnNames(rs, expColNames);
        expRS = new String[][] { { null, null, null, null, null },
                { "0", "0", "0", "0", "0" }, { "1", "1", "1", "1", "1" } };
        JDBC.assertFullResultSet(rs, expRS, true);

        rs = st.executeQuery(
            "select * from s where exists (select t.i from t)");
        expColNames = new String[] { "I", "S", "C", "VC", "B" };
        JDBC.assertColumnNames(rs, expColNames);
        expRS = new String[][] { { null, null, null, null, null },
                { "0", "0", "0", "0", "0" }, { "1", "1", "1", "1", "1" } };
        JDBC.assertFullResultSet(rs, expRS, true);

        rs = st.executeQuery(
            "select * from s where exists (select i, s from t)");
        expColNames = new String[] { "I", "S", "C", "VC", "B" };
        JDBC.assertColumnNames(rs, expColNames);
        expRS = new String[][] { { null, null, null, null, null },
                { "0", "0", "0", "0", "0" }, { "1", "1", "1", "1", "1" } };
        JDBC.assertFullResultSet(rs, expRS, true);

        // subquery returns empty result set
        rs = st.executeQuery(
            "select * from s where exists (select * from t where i = -1)");
        expColNames = new String[] { "I", "S", "C", "VC", "B" };
        JDBC.assertColumnNames(rs, expColNames);
        JDBC.assertDrainResults(rs, 0);

        // test semantics of AnyResultSet
        rs = st.executeQuery(
            "select * from s where exists (select t.* from t)");
        expColNames = new String[] { "I", "S", "C", "VC", "B" };
        JDBC.assertColumnNames(rs, expColNames);
        expRS = new String[][] { { null, null, null, null, null },
                { "0", "0", "0", "0", "0" }, { "1", "1", "1", "1", "1" } };
        JDBC.assertFullResultSet(rs, expRS, true);

        rs = st.executeQuery("select * from s where exists (select 0 from t)");
        expColNames = new String[] { "I", "S", "C", "VC", "B" };
        JDBC.assertColumnNames(rs, expColNames);
        expRS = new String[][] { { null, null, null, null, null },
                { "0", "0", "0", "0", "0" }, { "1", "1", "1", "1", "1" } };
        JDBC.assertFullResultSet(rs, expRS, true);

        // subquery in derived table
        rs = st.executeQuery(
            "select * from (select * from s where exists " +
                "(select * from t) and i = 0) a");
        expColNames = new String[] { "I", "S", "C", "VC", "B" };
        JDBC.assertColumnNames(rs, expColNames);
        expRS = new String[][] { { "0", "0", "0", "0", "0" } };
        JDBC.assertFullResultSet(rs, expRS, true);

        // exists under an OR
        rs = st.executeQuery(
            "select * from s where 0=1 or exists (select * from t)");
        expColNames = new String[] { "I", "S", "C", "VC", "B" };
        JDBC.assertColumnNames(rs, expColNames);
        expRS = new String[][] { { null, null, null, null, null },
                { "0", "0", "0", "0", "0" }, { "1", "1", "1", "1", "1" } };
        JDBC.assertFullResultSet(rs, expRS, true);

        rs = st.executeQuery(
            "select * from s where 1=1 or exists " +
                "(select * from t where 0=1)");
        expColNames = new String[] { "I", "S", "C", "VC", "B" };
        JDBC.assertColumnNames(rs, expColNames);
        expRS = new String[][] { { null, null, null, null, null },
                { "0", "0", "0", "0", "0" }, { "1", "1", "1", "1", "1" } };
        JDBC.assertFullResultSet(rs, expRS, true);

        rs = st.executeQuery("select * from s where exists (select * from t "
                + "where 0=1) or exists (select * from t)");
        expColNames = new String[] { "I", "S", "C", "VC", "B" };
        JDBC.assertColumnNames(rs, expColNames);
        expRS = new String[][] { { null, null, null, null, null },
                { "0", "0", "0", "0", "0" }, { "1", "1", "1", "1", "1" } };
        JDBC.assertFullResultSet(rs, expRS, true);

        rs = st.executeQuery(" select * from s where exists (select * from t "
                + "where exists (select * from t where 0=1) or exists "
                + "(select * from t))");
        expColNames = new String[] { "I", "S", "C", "VC", "B" };
        JDBC.assertColumnNames(rs, expColNames);
        expRS = new String[][] { { null, null, null, null, null },
                { "0", "0", "0", "0", "0" }, { "1", "1", "1", "1", "1" } };
        JDBC.assertFullResultSet(rs, expRS, true);

        // (exists empty set) is null
        rs = st.executeQuery("select * from s where (exists (select * from t "
                + "where 0=1)) is null");
        expColNames = new String[] { "I", "S", "C", "VC", "B" };
        JDBC.assertColumnNames(rs, expColNames);
        expRS = new String[][] { { null, null, null, null, null },
                { "0", "0", "0", "0", "0" }, { "1", "1", "1", "1", "1" } };
        JDBC.assertFullResultSet(rs, expRS, true);

        // Not exists
        rs = st.executeQuery(
            "select * from s where not exists (select * from t)");
        expColNames = new String[] { "I", "S", "C", "VC", "B" };
        JDBC.assertColumnNames(rs, expColNames);
        JDBC.assertDrainResults(rs, 0);

        rs = st.executeQuery(
            "select * from s where not exists " +
                "(select * from t where i = -1)");
        expColNames = new String[] { "I", "S", "C", "VC", "B" };
        JDBC.assertColumnNames(rs, expColNames);
        expRS = new String[][] { { null, null, null, null, null },
                { "0", "0", "0", "0", "0" }, { "1", "1", "1", "1", "1" } };
        JDBC.assertFullResultSet(rs, expRS, true);

        rs.close();
        st.close();
    }

    /**
     * expression subqueries non-correlated negative tests all node
     * 
     * @throws Exception
     */
    public void testExpressionNonCorrelated() throws Exception {
        Statement st = createStatement();
        ResultSet rs = null;
        String[][] expRS;
        String[] expColNames;

        assertStatementError("42X38", st,
            "select * from s where i = (select * from t)");

        // too many columns in select list
        assertStatementError("42X39", st,
            "select * from s where i = (select i, s from t)");

        // no conversions
        assertStatementError("21000", st,
            "select * from s where i = (select 1 from t)");

        assertStatementError("21000", st,
            "select * from s where i = (select b from t)");

        // ? parameter in select list of expression subquery
        assertStatementError("42X34", st,
            "select * from s where i = (select ? from t)");

        // do consistency check on scans, etc.
        rs = st.executeQuery("values ConsistencyChecker()");

        expColNames = new String[] { "1" };
        JDBC.assertColumnNames(rs, expColNames);

        // cardinality violation
        assertStatementError("21000", st,
            "select * from s where i = (select i from t)");

        // do consistency check on scans, etc.
        rs = st.executeQuery("values ConsistencyChecker()");
        expColNames = new String[] { "1" };
        JDBC.assertColumnNames(rs, expColNames);

        assertStatementError("21000", st,
            "select * from s where s = (select s from t where s = 1)");

        // do consistency check on scans, etc.
        rs = st.executeQuery("values ConsistencyChecker()");
        expColNames = new String[] { "1" };
        JDBC.assertColumnNames(rs, expColNames);
        if (usingEmbedded()) {
            expRS = new String[][] 
                { { "No open scans, etc.\n16 dependencies found" } };
            JDBC.assertFullResultSet(rs, expRS, true);
        } else {
            expRS = new String[][] 
                { { "No open scans, etc.\n16 dependencies found" } };
            JDBC.assertFullResultSet(rs, expRS, true);
        }

        assertStatementError("21000", st,
            "update s set b = (select max(b) from t) where vc " +
                "<> (select vc from t where vc = '1')");

        // do consistency check on scans, etc.
        rs = st.executeQuery("values ConsistencyChecker()");
        expColNames = new String[] { "1" };
        JDBC.assertColumnNames(rs, expColNames);
        if (usingEmbedded()) {
            expRS = new String[][] 
                { { "No open scans, etc.\n16 dependencies found" } };
            JDBC.assertFullResultSet(rs, expRS, true);
        } else {
            expRS = new String[][] 
                {  { "No open scans, etc.\n16 dependencies found" } };
            JDBC.assertFullResultSet(rs, expRS, true);
        }

        assertStatementError("21000", st,
            "delete from s where c = (select c from t where c = '1')");

        // do consistency check on scans, etc.
        rs = st.executeQuery("values ConsistencyChecker()");
        expColNames = new String[] { "1" };
        JDBC.assertColumnNames(rs, expColNames);
        if (usingEmbedded()) {
            expRS = new String[][] 
                { { "No open scans, etc.\n16 dependencies found" } };
            JDBC.assertFullResultSet(rs, expRS, true);
        } else {
            expRS = new String[][] 
                { { "No open scans, etc.\n16 dependencies found" } };
            JDBC.assertFullResultSet(rs, expRS, true);
        }

        // positive tests

        rs = st.executeQuery("select * from s");
        expColNames = new String[] { "I", "S", "C", "VC", "B" };
        JDBC.assertColumnNames(rs, expColNames);
        expRS = new String[][] { { null, null, null, null, null },
                { "0", "0", "0", "0", "0" }, { "1", "1", "1", "1", "1" } };
        JDBC.assertFullResultSet(rs, expRS, true);

        rs = st.executeQuery(" select * from t");
        expColNames = new String[] { "I", "S", "C", "VC", "B" };
        JDBC.assertColumnNames(rs, expColNames);
        expRS = new String[][] { { null, null, null, null, null },
                { "0", "0", "0", "0", "0" }, { "1", "1", "1", "1", "1" },
                { "1", "1", "1", "1", "1" }, { "2", "2", "2", "2", "1" } };
        JDBC.assertFullResultSet(rs, expRS, true);
    }

    /**
     * Testing simple subquery for each data type
     * 
     * @throws Exception
     */
    public void testSimpleSubquery() throws Exception {
        Statement st = createStatement();
        ResultSet rs = null;
        String[][] expRS;
        String[] expColNames;

        rs = st.executeQuery(
            "select * from s where i = (select i from t where i = 0)");
        expColNames = new String[] { "I", "S", "C", "VC", "B" };
        JDBC.assertColumnNames(rs, expColNames);
        expRS = new String[][] { { "0", "0", "0", "0", "0" } };
        JDBC.assertFullResultSet(rs, expRS, true);

        rs = st.executeQuery(
            "select * from s where s = (select s from t where s = 0)");
        expColNames = new String[] { "I", "S", "C", "VC", "B" };
        JDBC.assertColumnNames(rs, expColNames);
        expRS = new String[][] { { "0", "0", "0", "0", "0" } };
        JDBC.assertFullResultSet(rs, expRS, true);

        rs = st.executeQuery(
            "select * from s where c = (select c from t where c = '0')");
        expColNames = new String[] { "I", "S", "C", "VC", "B" };
        JDBC.assertColumnNames(rs, expColNames);
        expRS = new String[][] { { "0", "0", "0", "0", "0" } };
        JDBC.assertFullResultSet(rs, expRS, true);

        rs = st.executeQuery(
            "select * from s where vc = " +
                "(select vc from t where vc = '0')");
        expColNames = new String[] { "I", "S", "C", "VC", "B" };
        JDBC.assertColumnNames(rs, expColNames);
        expRS = new String[][] { { "0", "0", "0", "0", "0" } };
        JDBC.assertFullResultSet(rs, expRS, true);

        rs = st.executeQuery(
            "select * from s where b = " +
                "(select max(b) from t where b = 0)");
        expColNames = new String[] { "I", "S", "C", "VC", "B" };
        JDBC.assertColumnNames(rs, expColNames);
        expRS = new String[][] { { "0", "0", "0", "0", "0" } };
        JDBC.assertFullResultSet(rs, expRS, true);

        rs = st.executeQuery(
            "select * from s where b = " +
                "(select max(b) from t where i = 2)");
        expColNames = new String[] { "I", "S", "C", "VC", "B" };
        JDBC.assertColumnNames(rs, expColNames);
        expRS = new String[][] { { "1", "1", "1", "1", "1" } };
        JDBC.assertFullResultSet(rs, expRS, true);
    }

    /**
     * ? parameter on left hand side of expression subquery
     * @throws Exception
     */
    public void testParameterOnLeft()throws Exception {
        Statement st = createStatement();
        PreparedStatement pSt;
        ResultSetMetaData rsmd;
        ResultSet rs = null;
        String[][] expRS;
        String[] expColNames;

        pSt = prepareStatement(
            "select * from s where ? = (select i from t where i = 0)");

        rs = st.executeQuery("values (0)");
        rs.next();
        rsmd = rs.getMetaData();
        for (int i = 1; i <= rsmd.getColumnCount(); i++)
            pSt.setObject(i, rs.getObject(i));

        rs = pSt.executeQuery();
        expColNames = new String [] {"I", "S", "C", "VC", "B"};
        JDBC.assertColumnNames(rs, expColNames);
        expRS = new String [][] {
                {null, null, null, null, null},
                {"0", "0", "0", "0", "0"},
                {"1", "1", "1", "1", "1"}
        };
        JDBC.assertFullResultSet(rs, expRS, true);
    }

    /**
     * Testing conversions
     * @throws Exception
     */
    public void testConversions()throws Exception {
        Statement st = createStatement();
        ResultSet rs = null;
        String[][] expRS;
        String[] expColNames;

        rs = st.executeQuery(
            "select * from s where i = (select s from t where s = 0)");
        expColNames = new String [] {"I", "S", "C", "VC", "B"};
        JDBC.assertColumnNames(rs, expColNames);
        expRS = new String [][] { {"0", "0", "0", "0", "0"} };
        JDBC.assertFullResultSet(rs, expRS, true);

        rs = st.executeQuery(
            "select * from s where s = (select i from t where i = 0)");
        expColNames = new String [] {"I", "S", "C", "VC", "B"};
        JDBC.assertColumnNames(rs, expColNames);
        expRS = new String [][] { {"0", "0", "0", "0", "0"} };
        JDBC.assertFullResultSet(rs, expRS, true);

        rs = st.executeQuery(
            "select * from s where c = (select vc from t where vc = '0')");
        expColNames = new String [] {"I", "S", "C", "VC", "B"};
        JDBC.assertColumnNames(rs, expColNames);
        expRS = new String [][] { {"0", "0", "0", "0", "0"} };
        JDBC.assertFullResultSet(rs, expRS, true);

        rs = st.executeQuery(
            "select * from s where vc = (select c from t where c = '0')");
        expColNames = new String [] {"I", "S", "C", "VC", "B"};
        JDBC.assertColumnNames(rs, expColNames);
        expRS = new String [][] { {"0", "0", "0", "0", "0"} };
        JDBC.assertFullResultSet(rs, expRS, true);
    }

    /**
     * (select nullable_column ...) is null On of each data 
     * type to test clone()
     * @throws Exception
     */
    public void testClone() throws Exception {
        Statement st = createStatement();
        ResultSet rs = null;
        String[][] expRS;
        String[] expColNames;

        rs = st.executeQuery(
                "select * from s where (select s from s where i is "
                + "null) is null");
        expColNames = new String [] {"I", "S", "C", "VC", "B"};
        JDBC.assertColumnNames(rs, expColNames);
        expRS = new String [][] {
                {null, null, null, null, null},
                {"0", "0", "0", "0", "0"},
                {"1", "1", "1", "1", "1"}
                                };
        JDBC.assertFullResultSet(rs, expRS, true);

        rs = st.executeQuery("select * from s where " +
                "(select i from s where i is null) is null");
        expColNames = new String [] {"I", "S", "C", "VC", "B"};
        JDBC.assertColumnNames(rs, expColNames);
        expRS = new String [][] {
                {null, null, null, null, null},
                {"0", "0", "0", "0", "0"},
                {"1", "1", "1", "1", "1"}
                                };
        JDBC.assertFullResultSet(rs, expRS, true);

        rs = st.executeQuery("select * from s where " +
            "(select c from s where i is null) is null");
        expColNames = new String [] {"I", "S", "C", "VC", "B"};
        JDBC.assertColumnNames(rs, expColNames);
        expRS = new String [][]
                              {
                {null, null, null, null, null},
                {"0", "0", "0", "0", "0"},
                {"1", "1", "1", "1", "1"}
                              };
        JDBC.assertFullResultSet(rs, expRS, true);

        rs = st.executeQuery("select * from s where " +
            "(select vc from s where i is null) is null");
        expColNames = new String [] {"I", "S", "C", "VC", "B"};
        JDBC.assertColumnNames(rs, expColNames);
        expRS = new String [][]
                              {
                {null, null, null, null, null},
                {"0", "0", "0", "0", "0"},
                {"1", "1", "1", "1", "1"}
                              };
        JDBC.assertFullResultSet(rs, expRS, true);

        rs = st.executeQuery("select * from s where " +
            "(select b from s where i is null) is null");
        expColNames = new String [] {"I", "S", "C", "VC", "B"};
        JDBC.assertColumnNames(rs, expColNames);
        expRS = new String [][]
                              {
                {null, null, null, null, null},
                {"0", "0", "0", "0", "0"},
                {"1", "1", "1", "1", "1"}
                              };
        JDBC.assertFullResultSet(rs, expRS, true);

        rs = st.executeQuery("select * from s where " +
                "(select 1 from t where exists " +
                    "(select * from t where 1 = 0) and s = -1) is null");
        expColNames = new String [] {"I", "S", "C", "VC", "B"};
        JDBC.assertColumnNames(rs, expColNames);
        expRS = new String [][]
                              {
                {null, null, null, null, null},
                {"0", "0", "0", "0", "0"},
                {"1", "1", "1", "1", "1"}
                              };
        JDBC.assertFullResultSet(rs, expRS, true);
    }

    /**
     * Test subquery in subqueries
     * @throws Exception
     */
    public void testSubqueryInSubquery() throws Exception {
        Statement st = createStatement();
        ResultSet rs = null;
        String[][] expRS;
        String[] expColNames;
        
        rs = st.executeQuery("select * from s where " +
            "(select i from t where i = 0) = (select s from t where s = 0)");
        expColNames = new String [] {"I", "S", "C", "VC", "B"};
        JDBC.assertColumnNames(rs, expColNames);
        expRS = new String [][]
                              {
                {null, null, null, null, null},
                {"0", "0", "0", "0", "0"},
                {"1", "1", "1", "1", "1"}
                              };
        JDBC.assertFullResultSet(rs, expRS, true);

        // multiple subqueries at the same level
        rs = st.executeQuery("select * from s where i = " +
            "(select s from t where s = 0) " +
            "and s = (select i from t where i = 2)");
        expColNames = new String [] {"I", "S", "C", "VC", "B"};
        JDBC.assertColumnNames(rs, expColNames);
        JDBC.assertDrainResults(rs, 0);

        rs = st.executeQuery("select * from s where i = " +
            "(select s from t where s = 0) " +
            "and s = (select i from t where i = 0)");
        expColNames = new String [] {"I", "S", "C", "VC", "B"};
        JDBC.assertColumnNames(rs, expColNames);
        expRS = new String [][] { {"0", "0", "0", "0", "0"} };
        JDBC.assertFullResultSet(rs, expRS, true);

        // nested subqueries

        rs = st.executeQuery(
            "select * from s where i = " +
                "(select i from t where s = " +
                "(select i from t where s = 2))");
        expColNames = new String [] {"I", "S", "C", "VC", "B"};
        JDBC.assertColumnNames(rs, expColNames);
        JDBC.assertDrainResults(rs, 0);

        rs = st.executeQuery(
            "select * from s where i = " +
                "(select i - 1 from t where s = " +
                "(select i from t where s = 2))");
        expColNames = new String [] {"I", "S", "C", "VC", "B"};
        JDBC.assertColumnNames(rs, expColNames);
        expRS = new String [][] { {"1", "1", "1", "1", "1"} };
        JDBC.assertFullResultSet(rs, expRS, true);
    }

    /**
     * Test expression subqueries in select list
     * @throws Exception
     */
    public void testSubqueriesInSelect() throws Exception {
        Statement st = createStatement();
        ResultSet rs = null;
        String[][] expRS;
        String[] expColNames;
        
        rs = st.executeQuery("select (select i from t where 0=1) from s");
        expColNames = new String [] {"1"};
        JDBC.assertColumnNames(rs, expColNames);
        expRS = new String [][] { {null}, {null}, {null} };
        JDBC.assertFullResultSet(rs, expRS, true);

        rs = st.executeQuery("select " +
            "(select i from t where i = 2) * " +
            "(select s from t where i = 2) from s " +
            "where i > " +
                "(select i from t where i = 0) - " +
                "(select i from t where i = 0)");
        expColNames = new String [] {"1"};
        JDBC.assertColumnNames(rs, expColNames);
        expRS = new String [][] { {"4"} };
        JDBC.assertFullResultSet(rs, expRS, true);

        // in subqueries negative tests select * subquery

        assertStatementError("42X38", st,
            "select * from s where s in (select * from s)");
        
        // incompatable types
        rs = st.executeQuery(
            "select * from s where s in (select b from t)");
        expColNames = new String [] {"I", "S", "C", "VC", "B"};
        JDBC.assertColumnNames(rs, expColNames);
        expRS = new String [][]
                              {
                {"0", "0", "0", "0", "0"},
                {"1", "1", "1", "1", "1"}
                              };
        JDBC.assertFullResultSet(rs, expRS, true);
    }

    /**
     * Test constants in left, right and both sides of the subquery
     * @throws Exception
     */
    public void testConstants() throws Exception {
        Statement st = createStatement();
        ResultSet rs = null;
        String[][] expRS;
        String[] expColNames;

        // positive tests constants on left side of subquery

        rs = st.executeQuery(
            "select * from s where 1 in (select s from t)");
        expColNames = new String [] {"I", "S", "C", "VC", "B"};
        JDBC.assertColumnNames(rs, expColNames);
        expRS = new String [][]
                              {
                {null, null, null, null, null},
                {"0", "0", "0", "0", "0"},
                {"1", "1", "1", "1", "1"}
                              };
        JDBC.assertFullResultSet(rs, expRS, true);

        rs = st.executeQuery(
            "select * from s where -1 in (select i from t)");
        expColNames = new String [] {"I", "S", "C", "VC", "B"};
        JDBC.assertColumnNames(rs, expColNames);
        JDBC.assertDrainResults(rs, 0);

        rs = st.executeQuery(
            "select * from s where '1' in (select vc from t)");
        expColNames = new String [] {"I", "S", "C", "VC", "B"};
        JDBC.assertColumnNames(rs, expColNames);
        expRS = new String [][]
                              {
                {null, null, null, null, null},
                {"0", "0", "0", "0", "0"},
                {"1", "1", "1", "1", "1"}
                              };
        JDBC.assertFullResultSet(rs, expRS, true);

        rs = st.executeQuery(
            "select * from s where 0 in (select b from t)");
        expColNames = new String [] {"I", "S", "C", "VC", "B"};
        JDBC.assertColumnNames(rs, expColNames);
        expRS = new String [][]
                              {
                {null, null, null, null, null},
                {"0", "0", "0", "0", "0"},
                {"1", "1", "1", "1", "1"}
                              };
        JDBC.assertFullResultSet(rs, expRS, true);

        // constants in subquery select list
        rs = st.executeQuery(
            "select * from s where i in (select 1 from t)");
        expColNames = new String [] {"I", "S", "C", "VC", "B"};
        JDBC.assertColumnNames(rs, expColNames);
        expRS = new String [][]
                              {
                {"1", "1", "1", "1", "1"}
                              };
        JDBC.assertFullResultSet(rs, expRS, true);

        rs = st.executeQuery(
            "select * from s where i in (select -1 from t)");
        expColNames = new String [] {"I", "S", "C", "VC", "B"};
        JDBC.assertColumnNames(rs, expColNames);
        JDBC.assertDrainResults(rs, 0);

        rs = st.executeQuery(
            "select * from s where c in (select '1' from t)");
        expColNames = new String [] {"I", "S", "C", "VC", "B"};
        JDBC.assertColumnNames(rs, expColNames);
        expRS = new String [][] { {"1", "1", "1", "1", "1"} };
        JDBC.assertFullResultSet(rs, expRS, true);

        rs = st.executeQuery(
            "select * from s where b in (select 0 from t)");
        expColNames = new String [] {"I", "S", "C", "VC", "B"};
        JDBC.assertColumnNames(rs, expColNames);
        expRS = new String [][] { {"0", "0", "0", "0", "0"} };
        JDBC.assertFullResultSet(rs, expRS, true);

        // constants on both sides
        rs = st.executeQuery(
            "select * from s where 0 in (select 0 from t)");
        expColNames = new String [] {"I", "S", "C", "VC", "B"};
        JDBC.assertColumnNames(rs, expColNames);
        expRS = new String [][]
                              {
                {null, null, null, null, null},
                {"0", "0", "0", "0", "0"},
                {"1", "1", "1", "1", "1"}
                              };
        JDBC.assertFullResultSet(rs, expRS, true);

        // compatable types

        rs = st.executeQuery(
            "select * from s where c in (select vc from t)");
        expColNames = new String [] {"I", "S", "C", "VC", "B"};
        JDBC.assertColumnNames(rs, expColNames);
        expRS = new String [][]
                              {
                {"0", "0", "0", "0", "0"},
                {"1", "1", "1", "1", "1"}
                              };
        JDBC.assertFullResultSet(rs, expRS, true);

        rs = st.executeQuery(
            "select * from s where vc in (select c from t)");
        expColNames = new String [] {"I", "S", "C", "VC", "B"};
        JDBC.assertColumnNames(rs, expColNames);
        expRS = new String [][]
                              {
                {"0", "0", "0", "0", "0"},
                {"1", "1", "1", "1", "1"}
                              };
        JDBC.assertFullResultSet(rs, expRS, true);

        rs = st.executeQuery(
            "select * from s where i in (select s from t)");
        expColNames = new String [] {"I", "S", "C", "VC", "B"};
        JDBC.assertColumnNames(rs, expColNames);
        expRS = new String [][]
                              {
                {"0", "0", "0", "0", "0"},
                {"1", "1", "1", "1", "1"}
                              };
        JDBC.assertFullResultSet(rs, expRS, true);

        rs = st.executeQuery(
            "select * from s where s in (select i from t)");
        expColNames = new String [] {"I", "S", "C", "VC", "B"};
        JDBC.assertColumnNames(rs, expColNames);
        expRS = new String [][]
                              {
                {"0", "0", "0", "0", "0"},
                {"1", "1", "1", "1", "1"}
                              };
        JDBC.assertFullResultSet(rs, expRS, true);
    }

    /**
     * empty subquery result set
     * @throws Exception
     */
    public void testEmptyResultSet() throws Exception {
        Statement st = createStatement();
        ResultSet rs = null;
        String[] expColNames;

        rs = st.executeQuery(
            "select * from s where i in (select i from t where 1 = 0)");
        expColNames = new String [] {"I", "S", "C", "VC", "B"};
        JDBC.assertColumnNames(rs, expColNames);
        JDBC.assertDrainResults(rs, 0);

        rs = st.executeQuery(
            "select * from s where (i in " +
                "(select i from t where i = 0)) is null");
        expColNames = new String [] {"I", "S", "C", "VC", "B"};
        JDBC.assertColumnNames(rs, expColNames);
        JDBC.assertDrainResults(rs, 0);
    }

    /**
     * Test subqueries in select list
     */
    public void testSubqueriesInSelectList() throws Exception {
        Statement st = createStatement();
        ResultSet rs = null;
        String[][] expRS;
        String[] expColNames;

        rs = st.executeQuery(
            "select ( i in (select i from t) ) a from s order by a");            
        expColNames = new String [] {"A"};
        JDBC.assertColumnNames(rs, expColNames);
        expRS = new String [][] {{"false"}, {"true"}, {"true"}};
        JDBC.assertFullResultSet(rs, expRS, true);

        rs = st.executeQuery("select " +
            "( i in (select i from t where 1 = 0) ) a from s order by a");            
        expColNames = new String [] {"A"};
        JDBC.assertColumnNames(rs, expColNames);
        expRS = new String [][] {{"false"}, {"false"}, {"false"}};
        JDBC.assertFullResultSet(rs, expRS, true);

        rs = st.executeQuery("select " +
            "( (i in " +
                "(select i from t where 1 = 0)) is null ) a " +
                "from s order by a");
        expColNames = new String [] {"A"};
        JDBC.assertColumnNames(rs, expColNames);
        expRS = new String [][] {{"false"}, {"false"}, {"false"}};
        JDBC.assertFullResultSet(rs, expRS, true);
    }

    /**
     * subquery under an or
     * @throws Exception
     */
    public void testSubqueryUnderOR() throws Exception {
        Statement st = createStatement();
        ResultSet rs = null;
        String[][] expRS;
        String[] expColNames;
        
        rs = st.executeQuery(
            "select i from s where i = -1 or i in (select i from t)");
        expColNames = new String [] {"I"};
        JDBC.assertColumnNames(rs, expColNames);
        expRS = new String [][] { {"0"}, {"1"} };
        JDBC.assertFullResultSet(rs, expRS, true);

        rs = st.executeQuery(
            "select i from s where i = 0 or i in " +
                "(select i from t where i = -1)");
        expColNames = new String [] {"I"};
        JDBC.assertColumnNames(rs, expColNames);
        expRS = new String [][] { {"0"} };
        JDBC.assertFullResultSet(rs, expRS, true);

        rs = st.executeQuery(
            "select i from s where i = -1 or i in " +
                "(select i from t where i = -1 or i = 1)");
        expColNames = new String [] {"I"};
        JDBC.assertColumnNames(rs, expColNames);
        expRS = new String [][] { {"1"} };
        JDBC.assertFullResultSet(rs, expRS, true);
    }

    /**
     * distinct elimination
     * @throws Exception
     */
    public void testDistinct() throws Exception {
        Statement st = createStatement();
        ResultSet rs = null;
        String[][] expRS;
        String[] expColNames;
        
        rs = st.executeQuery(
            "select i from s where i in (select i from s)");
        expColNames = new String [] {"I"};
        JDBC.assertColumnNames(rs, expColNames);
        expRS = new String [][] { {"0"}, {"1"} };
        JDBC.assertFullResultSet(rs, expRS, true);

        rs = st.executeQuery(
            "select i from s where i in (select distinct i from s)");
        expColNames = new String [] {"I"};
        JDBC.assertColumnNames(rs, expColNames);
        expRS = new String [][] { {"0"}, {"1"} };
        JDBC.assertFullResultSet(rs, expRS, true);

        rs = st.executeQuery(
            "select i from s ss where i in " +
                "(select i from s where s.i = ss.i)");
        expColNames = new String [] {"I"};
        JDBC.assertColumnNames(rs, expColNames);
        expRS = new String [][] { {"0"}, {"1"} };
        JDBC.assertFullResultSet(rs, expRS, true);

        rs = st.executeQuery(
            "select i from s ss where i in " +
                "(select distinct i from s where s.i = ss.i)");
        expColNames = new String [] {"I"};
        JDBC.assertColumnNames(rs, expColNames);
        expRS = new String [][] { {"0"}, {"1"} };
        JDBC.assertFullResultSet(rs, expRS, true);

        // do consistency check on scans, etc.
        rs = st.executeQuery("values ConsistencyChecker()");
        expColNames = new String [] {"1"};
        JDBC.assertColumnNames(rs, expColNames);
        // Utilities.showResultSet(rs);
        if (usingEmbedded()) {
            expRS = new String[][] 
                { { "No open scans, etc.\n16 dependencies found" } };
            JDBC.assertFullResultSet(rs, expRS, true);
        } else {
            expRS = new String[][] 
                { { "No open scans, etc.\n16 dependencies found" } };
            JDBC.assertFullResultSet(rs, expRS, true);
        }
    }

    /**
     * Test Matches
     * 
     */
    public void testMatches() throws Exception {
        Statement st = createStatement();
        // correlated subqueries negative tests multiple matches 
        // at parent level

        assertStatementError("42X03", st,
        "select * from s, t where exists (select i from tt)");

        // match is against base table, but not derived column list

        assertStatementError("42X04", st,
                "select * from s ss (c1, c2, c3, c4, c5) where "
                + "exists (select i from tt)");

        assertStatementError("42X04", st,
                " select * from s ss (c1, c2, c3, c4, c5) where "
                + "exists (select ss.i from tt)");

        // correlation name exists at both levels, but only column 
        // match is at parent level

        assertStatementError("42X04", st,
        "select * from s where exists (select s.i from tt s)");

        // only match is at peer level

        assertStatementError("42X04", st,
                "select * from s where exists (select * from tt) and "
                + "exists (select ii from t)");

        assertStatementError("42X04", st,
                " select * from s where exists (select * from tt) "
                + "and exists (select tt.ii from t)");

        // correlated column in a derived table

        assertStatementError("42X04", st,
        "select * from s, (select * from tt where i = ii) a");

        assertStatementError("42X04", st,
        " select * from s, (select * from tt where s.i = ii) a");
    }

    /**
     * Test Simple correlated subqueries
     */
    public void testSimpleCorrelated() throws Exception {
        Statement st = createStatement();
        ResultSet rs = null;
        String[][] expRS;
        String[] expColNames;
        
        // positive tests simple correlated subqueries

        rs = st.executeQuery(
            "select (select i from tt where ii = i and ii <> 1) from s");
        expColNames = new String [] {"1"};
        JDBC.assertColumnNames(rs, expColNames);
        expRS = new String [][] { {null}, {"0"}, {null} };
        JDBC.assertFullResultSet(rs, expRS, true);

        rs = st.executeQuery("select " +
            "(select s.i from tt where ii = s.i and ii <> 1) from s");
        expColNames = new String [] {"1"};
        JDBC.assertColumnNames(rs, expColNames);
        expRS = new String [][] { {null}, {"0"}, {null} };
        JDBC.assertFullResultSet(rs, expRS, true);

        rs = st.executeQuery(
            "select (select s.i from ttt where iii = i) from s");
        expColNames = new String [] {"1"};
        JDBC.assertColumnNames(rs, expColNames);
        expRS = new String [][] { {null}, {null}, {null} };
        JDBC.assertFullResultSet(rs, expRS, true);

        rs = st.executeQuery(
            "select * from s where exists " +
                "(select * from tt where i = ii and ii <> 1)");
        expColNames = new String [] {"I", "S", "C", "VC", "B"};
        JDBC.assertColumnNames(rs, expColNames);
        expRS = new String [][] { {"0", "0", "0", "0", "0"} };
        JDBC.assertFullResultSet(rs, expRS, true);

        rs = st.executeQuery(
            "select * from s where exists " +
                "(select * from tt where s.i = ii and ii <> 1)");
        expColNames = new String [] {"I", "S", "C", "VC", "B"};
        JDBC.assertColumnNames(rs, expColNames);
        expRS = new String [][] { {"0", "0", "0", "0", "0"} };
        JDBC.assertFullResultSet(rs, expRS, true);

        rs = st.executeQuery(
            "select * from s where exists " +
                "(select * from ttt where i = iii)");
        expColNames = new String [] {"I", "S", "C", "VC", "B"};
        JDBC.assertColumnNames(rs, expColNames);
        JDBC.assertDrainResults(rs, 0);

        // 1 case where we get a cardinality violation after a few 
        // rows
        try{
            rs = st.executeQuery(
            "select (select i from tt where ii = i) from s");
        }catch(SQLException sqle){
            BaseJDBCTestCase.assertSQLState(
                "Scalar subquery is only allowed to return a single row.","21000",sqle);
        }

        // skip levels to find match
        rs = st.executeQuery(
            "select * from s where exists (select * from ttt "
                + "where iii = (select 11 from tt where ii = i and ii <> 1))");
        expColNames = new String [] {"I", "S", "C", "VC", "B"};
        JDBC.assertColumnNames(rs, expColNames);
        expRS = new String [][] { {"0", "0", "0", "0", "0"} };
        JDBC.assertFullResultSet(rs, expRS, true);
    }

    /**
     * join in subquery
     * @throws Exception
     */
    public void testJoinInSubqueries() throws Exception {
        Statement st = createStatement();
        ResultSet rs = null;
        String[][] expRS;
        String[] expColNames;

        rs = st.executeQuery(
            "select * from s where i in " +
                "(select i from t, tt where s.i <> i and i = ii)");
        expColNames = new String [] {"I", "S", "C", "VC", "B"};
        JDBC.assertColumnNames(rs, expColNames);
        JDBC.assertDrainResults(rs, 0);

        rs = st.executeQuery(
            "select * from s where i in " +
                "(select i from t, ttt where s.i < iii and s.i = t.i)");
        expColNames = new String [] {"I", "S", "C", "VC", "B"};
        JDBC.assertColumnNames(rs, expColNames);
        expRS = new String [][]
                              {
                {"0", "0", "0", "0", "0"},
                {"1", "1", "1", "1", "1"}
                              };
        JDBC.assertFullResultSet(rs, expRS, true);

        // join in outer query block
        rs = st.executeQuery(
            "select s.i, t.i from s, t where exists " +
                "(select * from ttt where iii = 1)");
        expColNames = new String [] {"I", "I"};
        JDBC.assertColumnNames(rs, expColNames);
        JDBC.assertDrainResults(rs, 0);

        rs = st.executeQuery(
            "select s.i, t.i from s, t where exists " +
                "(select * from ttt where iii = 11)");
        expColNames = new String [] {"I", "I"};
        JDBC.assertColumnNames(rs, expColNames);
        expRS = new String [][]
                              {
                {null, null},
                {null, "0"},
                {null, "1"},
                {null, "1"},
                {null, "2"},
                {"0", null},
                {"0", "0"},
                {"0", "1"},
                {"0", "1"},
                {"0", "2"},
                {"1", null},
                {"1", "0"},
                {"1", "1"},
                {"1", "1"},
                {"1", "2"}
                              };
        JDBC.assertFullResultSet(rs, expRS, true);

        // joins in both query blocks
        rs = st.executeQuery(
            "select s.i, t.i from s, t where t.i = " +
                "(select iii from ttt, tt where iii = t.i)");
        expColNames = new String [] {"I", "I"};
        JDBC.assertColumnNames(rs, expColNames);
        JDBC.assertDrainResults(rs, 0);

        rs = st.executeQuery(
            "select s.i, t.i from s, t " +
                "where t.i = (select ii from ttt, tt " +
                    "where s.i = t.i and t.i = tt.ii " +
                    "and iii = 22 and ii <> 1)");
        expColNames = new String [] {"I", "I"};
        JDBC.assertColumnNames(rs, expColNames);
        expRS = new String [][] { {"0", "0"} };
        JDBC.assertFullResultSet(rs, expRS, true);
    }

    /**
     * Test proper caching of subqueries in prepared statements
     * This section (old Cloudscape reference 'Beetle 5382') tests the fix for
     * a problem where sub-queries were executed not once per execution of the
     * statement, but only once, when the statement was first executed. 
     * If the parameter changed between executions or if the data changed 
     * between executions then the top level select returned the wrong results.

     * @throws Exception
     */
    public void testSubqueriesInPS() throws Exception {
        Statement st = createStatement();
        PreparedStatement pSt;
        ResultSetMetaData rsmd;
        ResultSet rs = null;
        String[][] expRS;
        String[] expColNames;

        pSt = prepareStatement(
            "select s.i from s where s.i in " +
                "(select s.i from s, t where s.i = t.i and t.s = ?)");
        rs = st.executeQuery("values(0)");
        rs.next();
        rsmd = rs.getMetaData();
        for (int i = 1; i <= rsmd.getColumnCount(); i++)
            pSt.setObject(i, rs.getObject(i));

        rs = pSt.executeQuery();
        expColNames = new String [] {"I"};
        JDBC.assertColumnNames(rs, expColNames);
        expRS = new String [][] { {"0"} };
        JDBC.assertFullResultSet(rs, expRS, true);

        rs = st.executeQuery("values(1)");
        rs.next();
        rsmd = rs.getMetaData();
        for (int i = 1; i <= rsmd.getColumnCount(); i++)
            pSt.setObject(i, rs.getObject(i));

        rs = pSt.executeQuery();
        expColNames = new String [] {"I"};
        JDBC.assertColumnNames(rs, expColNames);
        expRS = new String [][] { {"1"} };
        JDBC.assertFullResultSet(rs, expRS, true);

        setAutoCommit(false);
        
        pSt = prepareStatement(
            "select s.i from s where s.i in " +
                "(select s.i from s, t where s.i = t.i and t.s = 3)");

        rs = pSt.executeQuery();
        expColNames = new String [] {"I"};
        JDBC.assertColumnNames(rs, expColNames);
        JDBC.assertDrainResults(rs, 0);

        setAutoCommit(false);
        
        st.executeUpdate("insert into t(i,s) values(1,3)");

        rs = pSt.executeQuery();
        expColNames = new String [] {"I"};
        JDBC.assertColumnNames(rs, expColNames);
        expRS = new String [][] { {"1"} };
        JDBC.assertFullResultSet(rs, expRS, true);

        rollback();
    }

    /**
     * correlated subquery in select list of a derived table
     * @throws Exception
     */
    public void testSubuqeryInSelectListOfDerivedTable() throws Exception {
        Statement st = createStatement();
        ResultSet rs = null;
        String[][] expRS;
        String[] expColNames;

        rs = st.executeQuery(
            "select * from " +
                "(select (select iii from ttt " +
                    "where  sss > i and " +
                    "sss = iii and iii <> 11) " +
                "from s) a");
        expColNames = new String [] {"1"};
        JDBC.assertColumnNames(rs, expColNames);
        expRS = new String [][] { {null}, {"22"}, {"22"} };
        JDBC.assertFullResultSet(rs, expRS, true);

        // bigint and subqueries

        st.executeUpdate("create table li(i int, s smallint, l bigint)");
        st.executeUpdate("insert into li values (null, null, null)");
        st.executeUpdate("insert into li values (1, 1, 1)");
        st.executeUpdate("insert into li values (2, 2, 2)");

        rs = st.executeQuery(
            "select l from li o where l = " +
                "(select i from li i where o.l = i.i)");
        expColNames = new String [] {"L"};
        JDBC.assertColumnNames(rs, expColNames);
        expRS = new String [][] { {"1"}, {"2"} };
        JDBC.assertFullResultSet(rs, expRS, true);

        rs = st.executeQuery(
            "select l from li o where l = " +
                "(select s from li i where o.l = i.s)");
        expColNames = new String [] {"L"};
        JDBC.assertColumnNames(rs, expColNames);
        expRS = new String [][] { {"1"}, {"2"} };
        JDBC.assertFullResultSet(rs, expRS, true);

        rs = st.executeQuery(
            "select l from li o where l = " +
                "(select l from li i where o.l = i.l)");
        expColNames = new String [] {"L"};
        JDBC.assertColumnNames(rs, expColNames);
        expRS = new String [][] { {"1"}, {"2"} };
        JDBC.assertFullResultSet(rs, expRS, true);

        rs = st.executeQuery(
            "select l from li where l in (select i from li)");
        expColNames = new String [] {"L"};
        JDBC.assertColumnNames(rs, expColNames);
        expRS = new String [][] { {"1"}, {"2"} };
        JDBC.assertFullResultSet(rs, expRS, true);

        rs = st.executeQuery(
            "select l from li where l in (select s from li)");
        expColNames = new String [] {"L"};
        JDBC.assertColumnNames(rs, expColNames);
        expRS = new String [][] { {"1"}, {"2"} };
        JDBC.assertFullResultSet(rs, expRS, true);

        rs = st.executeQuery(
            "select l from li where l in (select l from li)");
        expColNames = new String [] {"L"};
        JDBC.assertColumnNames(rs, expColNames);
        expRS = new String [][] { {"1"}, {"2"} };
        JDBC.assertFullResultSet(rs, expRS, true);
    }

    /**
     * Some extra tests for subquery flattening on table expressions 
     * (remapColumnReferencesToExpressions() binary list node
     * @throws Exception
     */
    public void testSubqueryFlattening() throws Exception {
        Statement st = createStatement();
        ResultSet rs = null;
        String[][] expRS;
        String[] expColNames;

        rs = st.executeQuery(
            "select i in (1,2) from (select i from s) as tmp(i)");
        expColNames = new String [] {"1"};
        JDBC.assertColumnNames(rs, expColNames);
        expRS = new String [][] {{null}, {"false"}, {"true"}};
        JDBC.assertFullResultSet(rs, expRS, true);

        // conditional expression
        assertStatementError("42X01", st,
            "select i = 1 ? 1 : i from (select i from s) as tmp(i)");

        // more tests for correlated column resolution

        rs = st.executeQuery(
            "select * from s where i = (values i)");
        expColNames = new String [] {"I", "S", "C", "VC", "B"};
        JDBC.assertColumnNames(rs, expColNames);
        expRS = new String [][]
                              {
                {"0", "0", "0", "0", "0"},
                {"1", "1", "1", "1", "1"}
                              };
        JDBC.assertFullResultSet(rs, expRS, true);

        rs = st.executeQuery(
            "select t.* from s, t where t.i = (values s.i)");
        expColNames = new String [] {"I", "S", "C", "VC", "B"};
        JDBC.assertColumnNames(rs, expColNames);
        expRS = new String [][]
                              {
                {"0", "0", "0", "0", "0"},
                {"1", "1", "1", "1", "1"},
                {"1", "1", "1", "1", "1"}
                              };
        JDBC.assertFullResultSet(rs, expRS, true);

        rs = st.executeQuery(
            "select * from s where i in (values i)");
        expColNames = new String [] {"I", "S", "C", "VC", "B"};
        JDBC.assertColumnNames(rs, expColNames);
        expRS = new String [][]
                              {
                {"0", "0", "0", "0", "0"},
                {"1", "1", "1", "1", "1"}
                              };
        JDBC.assertFullResultSet(rs, expRS, true);

        rs = st.executeQuery(
            "select t.* from s, t where t.i in (values s.i)");
        expColNames = new String [] {"I", "S", "C", "VC", "B"};
        JDBC.assertColumnNames(rs, expColNames);
        expRS = new String [][]
                              {
                {"0", "0", "0", "0", "0"},
                {"1", "1", "1", "1", "1"},
                {"1", "1", "1", "1", "1"}
                              };
        JDBC.assertFullResultSet(rs, expRS, true);
    }

    /**
     * tests for not needing to do cardinality check
     * @throws Exception
     */
    public void testNoNeedForCardinalityCheck() throws Exception {
        Statement st = createStatement();
        ResultSet rs = null;
        String[][] expRS;
        String[] expColNames;

        rs = st.executeQuery(
            "select * from s where i = " +
                "(select min(i) from s where i is not null)");
        expColNames = new String [] {"I", "S", "C", "VC", "B"};
        JDBC.assertColumnNames(rs, expColNames);
        expRS = new String [][] { {"0", "0", "0", "0", "0"} };
        JDBC.assertFullResultSet(rs, expRS, true);

        assertStatementError("21000", st,
            "select * from s where i = (select min(i) from s group by i)");

        // tests for distinct expression subquery

        st.executeUpdate("create table dist1 (c1 int)");
        st.executeUpdate("create table dist2 (c1 int)");
        st.executeUpdate("insert into dist1 values null, 1, 2");
        st.executeUpdate("insert into dist2 values null, null");

        // no match, no violation
        rs = st.executeQuery(
            "select * from dist1 where c1 = " +
                "(select distinct c1 from dist2)");
        expColNames = new String [] {"C1"};
        JDBC.assertColumnNames(rs, expColNames);
        JDBC.assertDrainResults(rs, 0);

        // violation

        st.executeUpdate("insert into dist2 values 1");

        assertStatementError("21000", st,
            "select * from dist1 where c1 = " +
                "(select distinct c1 from dist2)");

        // match, no violation

        assertUpdateCount(st, 3, "update dist2 set c1 = 2");

        rs = st.executeQuery(
            "select * from dist1 where c1 = " +
                "(select distinct c1 from dist2)");
        expColNames = new String [] {"C1"};
        JDBC.assertColumnNames(rs, expColNames);
        expRS = new String [][] { {"2"} };
        JDBC.assertFullResultSet(rs, expRS, true);

        st.executeUpdate("drop table dist1");
        st.executeUpdate("drop table dist2");

        // update

        st.executeUpdate("create table u " +
            "(i int, s smallint, c char(30), vc char(30), b bigint)");
        st.executeUpdate("insert into u select * from s");

        rs = st.executeQuery("select * from u");
        expColNames = new String [] {"I", "S", "C", "VC", "B"};
        JDBC.assertColumnNames(rs, expColNames);
        expRS = new String [][]
                              {
                {null, null, null, null, null},
                {"0", "0", "0", "0", "0"},
                {"1", "1", "1", "1", "1"}
                              };
        JDBC.assertFullResultSet(rs, expRS, true);

        assertStatementError("42821", st,
            "update u set b = exists " +
                "(select b from t) where " +
                    "vc <> (select vc from s where vc = '1')");

        rs = st.executeQuery("select * from u");
        expColNames = new String [] {"I", "S", "C", "VC", "B"};
        JDBC.assertColumnNames(rs, expColNames);
        expRS = new String [][]
                              {
                {null, null, null, null, null},
                {"0", "0", "0", "0", "0"},
                {"1", "1", "1", "1", "1"}
                              };
        JDBC.assertFullResultSet(rs, expRS, true);

        assertUpdateCount(st, 3,"delete from u");

        st.executeUpdate("insert into u select * from s");

        // delete

        assertUpdateCount(st, 2,
            "delete from u where c < (select c from t where c = '2')");

        rs = st.executeQuery("select * from u");
        expColNames = new String [] {"I", "S", "C", "VC", "B"};
        JDBC.assertColumnNames(rs, expColNames);
        expRS = new String [][]
                              {
                {null, null, null, null, null}
                              };
        JDBC.assertFullResultSet(rs, expRS, true);

        // restore u

        assertUpdateCount(st, 1, "delete from u");

        st.executeUpdate("insert into u select * from s");
    }

    /**
     * check clean up when errors occur in subqueries insert
     * @throws Exception
     */
    public void testErrorsInNestedSubqueries() throws Exception {
        Statement st = createStatement();
        ResultSet rs = null;
        String[][] expRS;
        String[] expColNames;

        assertStatementError("22012", st,
            "insert into u select * from s s_outer where i = " +
                "(select s_inner.i/(s_inner.i-1) from s s_inner " +
                    "where s_outer.i = s_inner.i)");

        rs = st.executeQuery("select * from u");
        expColNames = new String [] {"I", "S", "C", "VC", "B"};
        JDBC.assertColumnNames(rs, expColNames);
        expRS = new String [][]
                              {
                {null, null, null, null, null},
                {"0", "0", "0", "0", "0"},
                {"1", "1", "1", "1", "1"}
                              };
        JDBC.assertFullResultSet(rs, expRS, true);

        // delete

        assertStatementError("22012", st,
            "delete from u " +
                "where i = (select i/(i-1) from s where u.i = s.i)");
        
        rs = st.executeQuery("select * from u");
        expColNames = new String [] {"I", "S", "C", "VC", "B"};
        JDBC.assertColumnNames(rs, expColNames);
        expRS = new String [][]
                              {
                {null, null, null, null, null},
                {"0", "0", "0", "0", "0"},
                {"1", "1", "1", "1", "1"}
                              };
        JDBC.assertFullResultSet(rs, expRS, true);

        // update

        assertStatementError("22012", st,
            "update u  set i = (select i from s where u.i = s.i) " +
                "where i = (select i/(i-1) from s where u.i = s.i)");

        assertStatementError("22012", st,
            "update u  set i = (select i/i-1 from s where u.i = s.i) " +
                "where i = (select i from s where u.i = s.i)");

        rs = st.executeQuery("select * from u");
        expColNames = new String [] {"I", "S", "C", "VC", "B"};
        JDBC.assertColumnNames(rs, expColNames);
        expRS = new String [][]
                              {
                {null, null, null, null, null},
                {"0", "0", "0", "0", "0"},
                {"1", "1", "1", "1", "1"}
                              };
        JDBC.assertFullResultSet(rs, expRS, true);

        // error in nested subquery
        assertStatementError("21000", st,
            "select (select (select (select i from s) from s) from s) from s");

        // do consistency check on scans, etc.
        rs = st.executeQuery("values ConsistencyChecker()");
        expColNames = new String [] {"1"};
        JDBC.assertColumnNames(rs, expColNames);
        if (usingEmbedded()) {
            expRS = new String[][] 
                { { "No open scans, etc.\n16 dependencies found" } };
            JDBC.assertFullResultSet(rs, expRS, true);
        } else {
            expRS = new String[][]
                { { "No open scans, etc.\n16 dependencies found" } };
            JDBC.assertFullResultSet(rs, expRS, true);
        }

        // reset autocommit
        setAutoCommit(true);
    }

    /**
     * subquery with groupby and having clause
     */
    public void testSubqueryWithClause() throws Exception {
        Statement st = createStatement();
        ResultSet rs = null;
        String[][] expRS;
        String[] expColNames;

        rs = st.executeQuery(
            "select distinct vc, i from t as myt1 " +
                "where s <= (select max(myt1.s) from t as myt2 " +
                    "where myt1.vc = myt2.vc " +
                    "and myt1.s <= myt2.s group by s " +
                    "having count(distinct s) <= 3)");
        expColNames = new String [] {"VC", "I"};
        JDBC.assertColumnNames(rs, expColNames);
        expRS = new String [][]
                              {
                {"0", "0"},
                {"1", "1"},
                {"2", "2"}
                              };
        JDBC.assertFullResultSet(rs, expRS, true);

        // subquery with having clause but no groupby
        rs = st.executeQuery(
            "select distinct vc, i from t as myt1 " +
                "where s <= (select max(myt1.s) from t as myt2 " +
                    "where myt1.vc = myt2.vc and myt1.s <= myt2.s " +
                    "having count(distinct s) <= 3)");
        expColNames = new String [] {"VC", "I"};
        JDBC.assertColumnNames(rs, expColNames);
        expRS = new String [][]
                              {
                {"0", "0"},
                {"1", "1"},
                {"2", "2"}
                              };
        JDBC.assertFullResultSet(rs, expRS, true);
    }

    /**
     * DERBY-1007: Optimizer for subqueries can return 
     * incorrect cost estimates leading to sub-optimal join 
     * orders for the outer query.  Before the patch for that 
     * issue, the following query plan will show T3 first and 
     * then T1-- but that's determined by the optimizer to be 
     * the "bad" join order.  After the fix, the join order 
     * will show T1 first, then T3, which is correct (based on 
     * the optimizer's estimates).
     * @throws Exception
     */
    public void testDERBY1007() throws Exception {
        Statement st = createStatement();
        ResultSet rs = null;
        String[][] expRS;
        String[] expColNames;

        st.executeUpdate("create table t_1 (i int, j int)");
        st.executeUpdate
            ("insert into T_1 values (1,1), (2,2), (3,3), (4,4), (5,5)");
        st.executeUpdate("create table t_3 (a int, b int)");
        st.executeUpdate(
            "insert into T_3 values (1,1), (2,2), (3,3), (4,4)");
        st.executeUpdate("insert into t_3 values " +
            "(6, 24), (7, 28), (8, 32), (9, 36), (10, 40)");
        st.execute("CALL SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1)");

        rs = st.executeQuery(
            "select x1.j, x2.b from (select distinct i,j from t_1) x1, " +
                "(select distinct a,b from t_3) x2 " +
                "where x1.i = x2.a order by x1.j, x2.b");
        expColNames = new String [] {"J", "B"};
        JDBC.assertColumnNames(rs, expColNames);
        expRS = new String [][]
                              {
                {"1", "1"},
                {"2", "2"},
                {"3", "3"},
                {"4", "4"}
                              };
        JDBC.assertFullResultSet(rs, expRS, true);

        rs = st.executeQuery("values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS()");
        rs.next();
        String rts = rs.getString(1);

        // Now verify the correct runtimeStatistics output
        RuntimeStatisticsParser rtsp = new RuntimeStatisticsParser(rts);
        // print out the full stats if derby.tests.debug is true
        println("full stats: \n" + rtsp.toString());
        // Checking only on the sequence of T3 and T1 scans.
        // If further checking is needed, uncomment more lines.
        rtsp.assertSequence( 
             new String[] {
                        "Source result set:",
                        "_Project-Restrict ResultSet (5):",
                        "_Source result set:",
                        "__Hash Join ResultSet:",
                        "__Left result set:",
                        "___Distinct Scan ResultSet for T_1 at read committed isolation level using instantaneous share row locking:",
                        "____Bit set of columns fetched=All",
                        "____Scan type=heap",
                        "__Right result set:",
                        "___Hash Table ResultSet (4):",
                        "___Source result set:",
                        "____Distinct Scan ResultSet for T_3 at read committed isolation level using instantaneous share row locking:",
                        "_____Bit set of columns fetched=All",
                        "_____Scan type=heap"
                              });

        st.executeUpdate("drop table t_1");
        st.executeUpdate("drop table t_3");
    }

    /**
     * DERBY-781: Materialize subqueries where possible to avoid creating 
     * invariant result sets many times.  This test case executes a query that
     * that has subqueries twice: the first time the tables have only a few
     * rows in them; the second time they have hundreds of rows in them.
     */
    public void testDERBY781() throws Exception {
        Statement st = createStatement();
        ResultSet rs = null;
        String[][] expRS;
        String[] expColNames;

        st.executeUpdate("create table t1 (i int, j int)");
        st.executeUpdate("create table t2 (i int, j int)");
        st.executeUpdate
            ("insert into t1 values (1,1), (2,2), (3,3), (4,4), (5,5)");
        st.executeUpdate
            ("insert into t2 values (1,1), (2,2), (3,3), (4,4), (5,5)");
        st.executeUpdate("create table t3 (a int, b int)");
        st.executeUpdate("create table t4 (a int, b int)");
        st.executeUpdate
            ("insert into t3 values (2,2), (4,4), (5,5)");
        st.executeUpdate
            ("insert into t4 values (2,2), (4,4), (5,5)");
    
        // Use of the term "DISTINCT" makes it so that we don't flatten 
        // the subqueries.
        st.executeUpdate("create view V1 as " +
            "select distinct T1.i, T2.j from T1, T2 where T1.i = T2.i");
        st.executeUpdate("create view V2 as " +
            "select distinct T3.a, T4.b from T3, T4 where T3.a = T4.a");
        st.execute("CALL SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1)");


        /* Run the test query the first time, with only a small number
         * of rows in each table. Before the patch for DERBY-781
         * the optimizer would have chosen a nested loop join, which 
         * means that we would generate the result set for the inner 
         * view multiple times.  After DERBY-781 the optimizer will 
         * choose to do a hash join and thereby materialize the inner 
         * result set, thus improving performance.  Should see a Hash join 
         * as the top-level join with a HashTableResult as the right child 
         * of the outermost join.
         */  
        rs = st.executeQuery(
            "select * from V1, V2 where V1.j = V2.b and V1.i in (1,2,3,4,5)");
            expColNames = new String [] {"I", "J", "A", "B"};
            JDBC.assertColumnNames(rs, expColNames);
            expRS = new String [][]
                                  {
                    {"2", "2", "2", "2"},
                    {"4", "4", "4", "4"},
                    {"5", "5", "5", "5"}
                                  };
            JDBC.assertFullResultSet(rs, expRS, true);

            rs = st.executeQuery(
                "values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS()");
            rs.next();
            String rts = rs.getString(1);

            // Now verify the correct runtimeStatistics output
            RuntimeStatisticsParser rtsp = new RuntimeStatisticsParser(rts);
            // print out the full stats if derby.tests.debug is true
            println("full stats: \n" + rtsp.toString());
            // the essentials are getting checked as per the comments
            // above. If further checking is needed, uncomment more lines.
            rtsp.assertSequence( 
                    new String[] {
                        "Hash Join ResultSet:",
                        //"Left result set:",
                        //"_Sort ResultSet:",
                        //"_Source result set:",
                        //"__Project-Restrict ResultSet (7):",
                        //"__Source result set:",
                        //"___Nested Loop Join ResultSet:",
                        //"___Left result set:",
                        //"____Project-Restrict ResultSet (5):",
                        //"____Source result set:",
                        //"_____Table Scan ResultSet for T1 at read committed " +
                        //    "isolation level using share row locking chosen " +
                        //    "by the optimizer",
                        //"______Bit set of columns fetched={0}",
                        //"______Scan type=heap",
                        //"___Right result set:",
                        //"____Table Scan ResultSet for T2 at read committed " +
                        //    "isolation level using share row locking chosen " +
                        //    "by the optimizer",
                        //"_____Bit set of columns fetched=All",
                        //"_____Scan type=heap",
                        //"______Operator: =",                        
                        "Right result set:",
                        "_Hash Table ResultSet (13):"
                        //"_Source result set:",
                        //"__Sort ResultSet:",
                        //"__Source result set:",
                        //"___Project-Restrict ResultSet (12):",
                        //"___Source result set:",
                        //"____Hash Join ResultSet:",
                        //"____Left result set:",
                        //"_____Table Scan ResultSet for T3 at read committed " +
                        //    "isolation level using share row locking chosen " +
                        //    "by the optimizer",
                        //"______Bit set of columns fetched=All",
                        //"______Scan type=heap"
                        //,
           // after this, there's something peculiar with the 
           // 'Right result set' line output, and this RuntimeStatisticsParser
           // method cannot find any further matches...
                        //"___Right result set:",
                        //"_____Hash Scan ResultSet for T4 at read committed " +
                        //    "isolation level using instantaneous share row" + 
                        //    "locking: ",
                        //"______Bit set of columns fetched=All",
                        //"______Scan type=heap",
                        //"_______Operator: ="                        
                            });
            // ...so checking on the remaining output another way. 
            assertTrue(rtsp.findString("Right result set:",3));        
            assertTrue(rtsp.findString("Hash Scan ResultSet for T4 at read " +
                "committed isolation level using instantaneous share row " +
                "locking: ",1));        
            //assertTrue(rtsp.findString("Bit set of columns fetched=All",2));        
            //assertTrue(rtsp.findString("Scan type=heap",4));        
            
            // Now add more data to the tables.
            st.executeUpdate("insert into t1 select * from t2");
            st.executeUpdate("insert into t2 select * from t1");
            st.executeUpdate("insert into t2 select * from t1");
            st.executeUpdate("insert into t1 select * from t2");
            st.executeUpdate("insert into t2 select * from t1");
            st.executeUpdate("insert into t1 select * from t2");
            st.executeUpdate("insert into t2 select * from t1");
            st.executeUpdate("insert into t1 select * from t2");
            st.executeUpdate("insert into t2 select * from t1");
            st.executeUpdate("insert into t1 select * from t2");
            st.executeUpdate("insert into t3 select * from t4");
            st.executeUpdate("insert into t4 select * from t3");
            st.executeUpdate("insert into t3 select * from t4");
            st.executeUpdate("insert into t4 select * from t3");
            st.executeUpdate("insert into t3 select * from t4");
            st.executeUpdate("insert into t4 select * from t3");
            st.executeUpdate("insert into t3 select * from t4");
            st.executeUpdate("insert into t4 select * from t3");
            st.executeUpdate("insert into t3 select * from t4");
            st.executeUpdate("insert into t4 select * from t3");
            st.executeUpdate("insert into t3 select * from t4");

            /* Drop the views and recreate them with slightly different
             * names.  The reason we use different names is to ensure that
             * the query will be "different" from the last time and thus we'll 
             * we'll go through optimization again (instead of just using 
             * the cached plan from last time). 
             */

            st.executeUpdate("drop view v1");
            st.executeUpdate("drop view v2");

            // Use of the term "DISTINCT" makes it so that we don't flatten
            // the subqueries.
            st.executeUpdate("create view VV1 as " +
                "select distinct T1.i, T2.j from T1, T2 where T1.i = T2.i");
            st.executeUpdate("create view VV2 as " +
                "select distinct T3.a, T4.b from T3, T4 where T3.a = T4.a");
            // Now execute the query again using the larger tables.
            rs = st.executeQuery(
                "select * from VV1, VV2 " +
                "where VV1.j = VV2.b and VV1.i in (1,2,3,4,5)");
            expColNames = new String [] {"I", "J", "A", "B"};
            JDBC.assertColumnNames(rs, expColNames);
            expRS = new String [][]
                                  {
                    {"2", "2", "2", "2"},
                    {"4", "4", "4", "4"},
                    {"5", "5", "5", "5"}
                                  };
            JDBC.assertFullResultSet(rs, expRS, true);

            rs = st.executeQuery(
                "values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS()");
            rs.next();
            rts = rs.getString(1);

            // Now verify the correct runtimeStatistics output
            rtsp = new RuntimeStatisticsParser(rts);
            // print out the full stats if derby.tests.debug is true
            println("full stats: \n" + rtsp.toString());
            // the essentials are getting checked as per the comments
            // above. If more detailed checking is needed, uncomment lines.
            rtsp.assertSequence( 
                    new String[] {
                        "Hash Join ResultSet:",
                        //"Left result set:",
                        //"_Sort ResultSet:",
                        "_Rows input = 53055",
                        //"_Source result set:",
                        //"__Project-Restrict ResultSet (7):",
                        //"__Source result set:",
                        //"___Hash Join ResultSet:",
                        //"___Left result set:",
                        //"____Project-Restrict ResultSet (5):",
                        //"____Source result set:",
                        //"_____Table Scan ResultSet for T1 at read committed " +
                        //    "isolation level using share row locking chosen " +
                        //    "by the optimizer",
                        //"______Bit set of columns fetched={0}",
                        //"______Scan type=heap",
                        //"___Right result set:",
                        // with fewer roles, the optimizer chose a Table Scan 
                        //"____Hash Scan ResultSet for T2 at read committed " +
                        //    "isolation level using instantaneous share row " +
                        //    "locking: ",
                        //"_____Bit set of columns fetched=All",
                        //"_____Scan type=heap",
                        //"______Operator: =",                        
                        //"Right result set:",
                        //"_Hash Table ResultSet (13):",
                        //"_Source result set:",
                        //"__Sort ResultSet:",
                        //"__Source result set:",
                        //"___Project-Restrict ResultSet (12):",
                        //"___Source result set:",
                        //"____Hash Join ResultSet:",
                        //"____Left result set:",
                        // with 4 rows, the optimizer used a Table Scan on T3 
                        // for left node and a Hash Scan on T4 for the right.
                        //"_____Table Scan ResultSet for T4 at read committed " +
                        //    "isolation level using share row locking chosen " +
                        //    "by the optimizer",
                        //"______Bit set of columns fetched=All",
                        //"______Scan type=heap",
                        "____Right result set:",
                        "_____Hash Scan ResultSet for T3 at read committed " +
                            "isolation level using instantaneous share row " +
                            "locking: "
                            //,
                        //"______Bit set of columns fetched={0}",
                        //"______Scan type=heap"
                            });

        // clean up.
        st.execute("CALL SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(0)");

        st.executeUpdate("drop view vv1");
        st.executeUpdate("drop view vv2");
        st.executeUpdate("drop table t1");
        st.executeUpdate("drop table t2");
        st.executeUpdate("drop table t3");
        st.executeUpdate("drop table t4");
    }

    /**
     * DERBY-1574: Subquery in COALESCE gives NPE due to 
     * preprocess not implemented for that node type
     */
    public void testSubqueryInCOALESCE() throws Exception {
        Statement st = createStatement();
        ResultSet rs = null;
        String[][] expRS;
        String[] expColNames;

        st.executeUpdate("create table t1 (id int)");
        st.executeUpdate("create table t2 (i integer primary key, j int)");
        st.executeUpdate("insert into t1 values 1,2,3,4,5");
        st.executeUpdate("insert into t2 values (1,1),(2,4),(3,9),(4,16)");

        assertUpdateCount(st, 5,
            "update t1 set id = coalesce((select j from t2 " +
                "where t2.i=t1.id), 0)");

        rs = st.executeQuery("select * from t1");
        expColNames = new String [] {"ID"};
        JDBC.assertColumnNames(rs, expColNames);
        expRS = new String [][] { {"1"}, {"4"}, {"9"}, {"16"}, {"0"} };
        JDBC.assertFullResultSet(rs, expRS, true);

        st.executeUpdate("drop table t1");
        st.executeUpdate("drop table t2");
    }

    /**
     * Test the fix for DERBY-2218
     * @throws Exception
     */
    public void testDERBY_2218() throws Exception {
        Statement st = createStatement();
        ResultSet rs = null;
        String[] expColNames;
        
        st.executeUpdate("create table t1 (i int)");

        rs = st.executeQuery(
            "select * from t1 " +
                "where i in (1, 2, (values cast(null as integer)))");
        expColNames = new String [] {"I"};
        JDBC.assertColumnNames(rs, expColNames);
        JDBC.assertDrainResults(rs, 0);

        // expect error, this used to throw NPE
        assertStatementError("42X07", st,
            "select * from t1 where i in (1, 2, (values null))");

        assertStatementError("42X07", st,
            "select * from t1 where i in " +
                "(select i from t1 where i in (1, 2, (values null)))");

        // expect error
        assertStatementError("42X07", st,
            "select * from t1 where exists (values null)");

        assertStatementError("42X07", st,
            "select * from t1 where exists " +
                "(select * from t1 where exists(values null))");

        assertStatementError("42X07", st,
            "select i from t1 where exists " +
                "(select i from t1 where exists(values null))");

        assertStatementError("42X07", st,
            "select * from (values null) as t2");

        assertStatementError("42X07", st,
            "select * from t1 where exists " +
                "(select 1 from (values null) as t2)");

        assertStatementError("42X07", st,
            "select * from t1 where exists " +
                "(select * from (values null) as t2)");

        st.executeUpdate("drop table t1");
        st.close();
    }
    
    /**
     * DERBY-4549: NPE in JBitSet
     */
    public void testDERBY_4549() throws Exception {
        Statement st = createStatement();
        PreparedStatement pSt;
        ResultSet rs = null;
        String[][] expRS;
        String[] expColNames;

        st.executeUpdate("create table ABC (ID int)");
        st.executeUpdate("create table DEF (ID int)");

        //compilation of the statement used to fail with NPE
        pSt = prepareStatement(
            "select * from ABC t1 " +
                "where (select distinct t2.ID from DEF t2) in " +
                    "(select t3.ID from DEF t3)");
        
        // empty tables, should give empty result
        rs = pSt.executeQuery();
        expColNames = new String [] {"ID"};
        JDBC.assertColumnNames(rs, expColNames);
        JDBC.assertDrainResults(rs, 0);

        // now, test with data in the tables
        st.executeUpdate("insert into ABC values 1, 2");
        rs = pSt.executeQuery();
        expColNames = new String [] {"ID"};
        JDBC.assertColumnNames(rs, expColNames);
        JDBC.assertDrainResults(rs, 0);

        st.executeUpdate("insert into DEF values 2");
        rs = pSt.executeQuery();
        expColNames = new String [] {"ID"};
        JDBC.assertColumnNames(rs, expColNames);
        expRS = new String [][] { {"1"}, {"2"} };
        JDBC.assertFullResultSet(rs, expRS, true);

        st.executeUpdate("insert into DEF values 2");
        rs = pSt.executeQuery();
        expColNames = new String [] {"ID"};
        JDBC.assertColumnNames(rs, expColNames);
        expRS = new String [][] { {"1"}, {"2"} };
        JDBC.assertFullResultSet(rs, expRS, true);

        st.executeUpdate("insert into DEF values 3");
        // will fail because left operand of IN is no longer scalar
        // expect ERROR 21000: 
        //     Scalar subquery is only allowed to return a single row
        assertStatementError("21000", pSt);

        st.executeUpdate("drop table ABC");
        st.executeUpdate("drop table DEF");
    }
}
