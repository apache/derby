/*
Derby - Class org.apache.derbyTesting.functionTests.tests.lang.LojReorderTest

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

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLWarning;

import junit.framework.Test;
import junit.framework.TestSuite;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Semi-automated rewrite of the original test harness/ij based test:
 * lojreorder.sql
 */
public final class LojReorderTest extends BaseJDBCTestCase {

    /**
     * Public constructor required for running test as standalone JUnit.
     */
    public LojReorderTest(String name)
    {
        super(name);
    }

    public static Test suite()
    {
        TestSuite suite = new TestSuite("LojReorderTest Test");
        suite.addTest(TestConfiguration.defaultSuite(LojReorderTest.class));
        return suite;
    }


    public void test_LojReorderTest_a() throws Exception
    {
        ResultSet rs = null;
        CallableStatement cSt;
        Statement st = createStatement();

        String [][] expRS;
        String [] expColNames;

        setAutoCommit(false);

        // This test LOJ reordering. Many testcases are derived from
        // DB2 UDB FVT testcases. Cliff Leung. 10/2002
        st.executeUpdate(
            "CREATE TABLE T (A INT NOT NULL, B DECIMAL(10,3) NOT "
            + "NULL, C VARCHAR(5) NOT NULL)");

        st.executeUpdate(
            " INSERT INTO T VALUES (1, 1.0, '1'), (2, 2.0, '2'), "
            + "(3, 3.0, '3')");

        st.executeUpdate(
            " CREATE TABLE S (D INT NOT NULL, E DECIMAL(10,3) "
            + "NOT NULL, F VARCHAR(5) NOT NULL)");

        st.executeUpdate(
            " INSERT INTO S VALUES (2, 2.0, '2'), (3, 3.0, '3'), "
            + "(4, 4.0, '4')");

        st.executeUpdate(
            " CREATE TABLE R (G INT NOT NULL, H DECIMAL(10,3) "
            + "NOT NULL, I VARCHAR(5) NOT NULL)");

        st.executeUpdate(
            " INSERT INTO R VALUES (3, 3.0, '3'), (4, 4.0, '4'), "
            + "(5, 5.0, '5')");

        st.executeUpdate(
            " CREATE TABLE TT (J INT NOT NULL, K DECIMAL(10,3) "
            + "NOT NULL, L VARCHAR(5) NOT NULL)");

        st.executeUpdate(
            " INSERT INTO TT VALUES (2, 2.0, '2'), (3, 3.0, "
            + "'3'), (4, 4.0, '4')");

        rs = st.executeQuery(
            " select * from t left outer join s on (b = e)");


        expColNames = new String [] {"A", "B", "C", "D", "E", "F"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1", "1.000", "1", null, null, null},
            {"2", "2.000", "2", "2", "2.000", "2"},
            {"3", "3.000", "3", "3", "3.000", "3"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        rs = st.executeQuery(
            " select a, e, f, a+e as \"A+E\" from t left outer "
            + "join s on (b = e)");

        expColNames = new String [] {"A", "E", "F", "A+E"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1", null, null, null},
            {"2", "2.000", "2", "4.000"},
            {"3", "3.000", "3", "6.000"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        rs = st.executeQuery(
            " select * from t right outer join s on (b = e)");

        expColNames = new String [] {"A", "B", "C", "D", "E", "F"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"2", "2.000", "2", "2", "2.000", "2"},
            {"3", "3.000", "3", "3", "3.000", "3"},
            {null, null, null, "4", "4.000", "4"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        rs = st.executeQuery(
            " select a, e, f, a+e as \"A+E\" from t right outer "
            + "join s on (b = e)");

        expColNames = new String [] {"A", "E", "F", "A+E"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"2", "2.000", "2", "4.000"},
            {"3", "3.000", "3", "6.000"},
            {null, "4.000", "4", null}
        };

        JDBC.assertFullResultSet(rs, expRS);

        rs = st.executeQuery(
            " select a, e, f, a+e as \"A+E\" from t left outer "
            + "join s on (b = e) where d > 0");

        expColNames = new String [] {"A", "E", "F", "A+E"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"2", "2.000", "2", "4.000"},
            {"3", "3.000", "3", "6.000"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        rs = st.executeQuery(
            " select a, e, f, a+e as \"A+E\" from t right outer "
            + "join s on (b = e) where d > 0");

        expColNames = new String [] {"A", "E", "F", "A+E"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"2", "2.000", "2", "4.000"},
            {"3", "3.000", "3", "6.000"},
            {null, "4.000", "4", null}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // LOJ and LOJ

        rs = st.executeQuery(
            "select a, e, f, a+e as \"A+E\", g from (t left "
            + "outer join s on (b = e)) left outer join r on (f = i)");

        expColNames = new String [] {"A", "E", "F", "A+E", "G"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1", null, null, null, null},
            {"2", "2.000", "2", "4.000", null},
            {"3", "3.000", "3", "6.000", "3"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        rs = st.executeQuery(
            " select a, e, f, a+e as \"A+E\", g from (t left "
            + "outer join s on (b = e)) left outer join r on (f = "
            + "i) where a>1");

        expColNames = new String [] {"A", "E", "F", "A+E", "G"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"2", "2.000", "2", "4.000", null},
            {"3", "3.000", "3", "6.000", "3"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        rs = st.executeQuery(
            " select a, d, e, a+e as \"A+E\", g from (t left "
            + "outer join s on (b = e)) left outer join r on (f = "
            + "i) where d>1");

        expColNames = new String [] {"A", "D", "E", "A+E", "G"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"2", "2", "2.000", "4.000", null},
            {"3", "3", "3.000", "6.000", "3"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        rs = st.executeQuery(
            " select a, d, e, a+e as \"A+E\", g from (t left "
            + "outer join s on (b = e)) left outer join r on (f = "
            + "i) where h>1");

        expColNames = new String [] {"A", "D", "E", "A+E", "G"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"3", "3", "3.000", "6.000", "3"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // LOJ and ROJ

        rs = st.executeQuery(
            "select a, e, f, a+e as \"A+E\", g from (t left "
            + "outer join s on (b = e)) right outer join r on (f = i)");

        expColNames = new String [] {"A", "E", "F", "A+E", "G"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"3", "3.000", "3", "6.000", "3"},
            {null, null, null, null, "4"},
            {null, null, null, null, "5"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        rs = st.executeQuery(
            " select a, e, f, a+e as \"A+E\", g from (t left "
            + "outer join s on (b = e)) right outer join r on (f = "
            + "i) where a>1");

        expColNames = new String [] {"A", "E", "F", "A+E", "G"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"3", "3.000", "3", "6.000", "3"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        rs = st.executeQuery(
            " select a, e, f, a+e as \"A+E\", g from (t left "
            + "outer join s on (b = e)) right outer join r on (f = "
            + "i) where d>1");

        expColNames = new String [] {"A", "E", "F", "A+E", "G"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"3", "3.000", "3", "6.000", "3"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        rs = st.executeQuery(
            " select a, e, f, a+e as \"A+E\", g from (t left "
            + "outer join s on (b = e)) right outer join r on (f = "
            + "i) where h>1");

        expColNames = new String [] {"A", "E", "F", "A+E", "G"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"3", "3.000", "3", "6.000", "3"},
            {null, null, null, null, "4"},
            {null, null, null, null, "5"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // ROJ and ROJ

        rs = st.executeQuery(
            "select a, e, f, a+e as \"A+E\", g from (t right "
            + "outer join s on (b = e)) right outer join r on (f = i)");

        expColNames = new String [] {"A", "E", "F", "A+E", "G"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"3", "3.000", "3", "6.000", "3"},
            {null, "4.000", "4", null, "4"},
            {null, null, null, null, "5"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        rs = st.executeQuery(
            " select a, e, f, a+e as \"A+E\", g from (t right "
            + "outer join s on (b = e)) right outer join r on (f = "
            + "i) where a>1");

        expColNames = new String [] {"A", "E", "F", "A+E", "G"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"3", "3.000", "3", "6.000", "3"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        rs = st.executeQuery(
            " select a, e, f, a+e as \"A+E\", g from (t right "
            + "outer join s on (b = e)) right outer join r on (f = "
            + "i) where d>1");

        expColNames = new String [] {"A", "E", "F", "A+E", "G"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"3", "3.000", "3", "6.000", "3"},
            {null, "4.000", "4", null, "4"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        rs = st.executeQuery(
            " select a, e, f, a+e as \"A+E\", g from (t right "
            + "outer join s on (b = e)) right outer join r on (f = "
            + "i) where h>1");

        expColNames = new String [] {"A", "E", "F", "A+E", "G"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"3", "3.000", "3", "6.000", "3"},
            {null, "4.000", "4", null, "4"},
            {null, null, null, null, "5"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // ROJ and LOJ

        rs = st.executeQuery(
            "select a, e, f, a+e as \"A+E\", g from (t right "
            + "outer join s on (b = e)) left outer join r on (f = i)");

        expColNames = new String [] {"A", "E", "F", "A+E", "G"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"2", "2.000", "2", "4.000", null},
            {"3", "3.000", "3", "6.000", "3"},
            {null, "4.000", "4", null, "4"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        rs = st.executeQuery(
            " select a, e, f, a+e as \"A+E\", g from (t right "
            + "outer join s on (b = e)) left outer join r on (f = "
            + "i) where a>1");

        expColNames = new String [] {"A", "E", "F", "A+E", "G"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"2", "2.000", "2", "4.000", null},
            {"3", "3.000", "3", "6.000", "3"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        rs = st.executeQuery(
            " select a, e, f, a+e as \"A+E\", g from (t right "
            + "outer join s on (b = e)) left outer join r on (f = "
            + "i) where d>1");

        expColNames = new String [] {"A", "E", "F", "A+E", "G"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"2", "2.000", "2", "4.000", null},
            {"3", "3.000", "3", "6.000", "3"},
            {null, "4.000", "4", null, "4"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        rs = st.executeQuery(
            " select a, e, f, a+e as \"A+E\", g from (t right "
            + "outer join s on (b = e)) left outer join r on (f = "
            + "i) where h>1");

        expColNames = new String [] {"A", "E", "F", "A+E", "G"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"3", "3.000", "3", "6.000", "3"},
            {null, "4.000", "4", null, "4"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // NO LOJ reordering

        rs = st.executeQuery(
            "select * from (t left outer join s on (b = e)) left "
            + "outer join r on (f = i) where a > 0");

        expColNames = new String [] {"A", "B", "C", "D", "E", "F", "G", "H", "I"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1", "1.000", "1", null, null, null, null, null, null},
            {"2", "2.000", "2", "2", "2.000", "2", null, null, null},
            {"3", "3.000", "3", "3", "3.000", "3", "3", "3.000", "3"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        rs = st.executeQuery(
            " select * from (t left outer join s on (b = e)) "
            + "inner join r on (f = i)");

        expColNames = new String [] {"A", "B", "C", "D", "E", "F", "G", "H", "I"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"3", "3.000", "3", "3", "3.000", "3", "3", "3.000", "3"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        rs = st.executeQuery(
            " select * from (t left outer join s on (b = e)) "
            + "inner join r on (f = i) where a > 0");

        expColNames = new String [] {"A", "B", "C", "D", "E", "F", "G", "H", "I"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"3", "3.000", "3", "3", "3.000", "3", "3", "3.000", "3"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        rs = st.executeQuery(
            " select * from (t inner join s on (b = e)) inner "
            + "join r on (f = i) where a > 0");

        expColNames = new String [] {"A", "B", "C", "D", "E", "F", "G", "H", "I"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"3", "3.000", "3", "3", "3.000", "3", "3", "3.000", "3"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        rs = st.executeQuery(
            " select * from (t inner join s on (b = e)) left "
            + "outer join r on (f = i) where a > 0");

        expColNames = new String [] {"A", "B", "C", "D", "E", "F", "G", "H", "I"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"2", "2.000", "2", "2", "2.000", "2", null, null, null},
            {"3", "3.000", "3", "3", "3.000", "3", "3", "3.000", "3"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        rs = st.executeQuery(
            " select * from t, s left outer join r on (d = g) where a = e");

        expColNames = new String [] {"A", "B", "C", "D", "E", "F", "G", "H", "I"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"2", "2.000", "2", "2", "2.000", "2", null, null, null},
            {"3", "3.000", "3", "3", "3.000", "3", "3", "3.000", "3"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        rs = st.executeQuery(
            " select * from t left outer join s on (b = e), r where a = g");

        expColNames = new String [] {"A", "B", "C", "D", "E", "F", "G", "H", "I"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"3", "3.000", "3", "3", "3.000", "3", "3", "3.000", "3"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // bug 5658 In DB2 UDB, any column referenced in an
        // expression of the  join-condition must be a column of
        // one of the operand tables of  the associated join (in
        // the scope of the same joined-table clause). DERBY-4380:
        // This query used to fail because column B was seen as
        // ambiguous (could belong to both T1 and T2). However, the
        // scope of the ON clause makes it unabiguous; the first
        // occurrence of B must be T1.B, and the second one must be
        // T2.B.

        rs = st.executeQuery(
            "select t1.*, s2.* from t t1 left outer join s on (b "
            + "= e), t t2 left outer join s s2 on (b = e)");

        expColNames = new String [] {"A", "B", "C", "D", "E", "F"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1", "1.000", "1", null, null, null},
            {"1", "1.000", "1", "2", "2.000", "2"},
            {"1", "1.000", "1", "3", "3.000", "3"},
            {"2", "2.000", "2", null, null, null},
            {"2", "2.000", "2", "2", "2.000", "2"},
            {"2", "2.000", "2", "3", "3.000", "3"},
            {"3", "3.000", "3", null, null, null},
            {"3", "3.000", "3", "2", "2.000", "2"},
            {"3", "3.000", "3", "3", "3.000", "3"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // a view of a regular join

        st.executeUpdate(
            "create view jv (fv, ev, dv, cv, bv, av) as (select "
            + "f, e, d, c, b, a from t, s where b = e)");

        rs = st.executeQuery(
            " select * from jv left outer join r on (fv = i)");

        expColNames = new String [] {"FV", "EV", "DV", "CV", "BV", "AV", "G", "H", "I"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"2", "2.000", "2", "2", "2.000", "2", null, null, null},
            {"3", "3.000", "3", "3", "3.000", "3", "3", "3.000", "3"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        rs = st.executeQuery(
            " select * from r left outer join jv on (fv = i)");

        expColNames = new String [] {"G", "H", "I", "FV", "EV", "DV", "CV", "BV", "AV"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"3", "3.000", "3", "3", "3.000", "3", "3", "3.000", "3"},
            {"4", "4.000", "4", null, null, null, null, null, null},
            {"5", "5.000", "5", null, null, null, null, null, null}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // a view of LOJ

        st.executeUpdate(
            "create view lojv (fv, ev, dv, cv, bv, av) as "
            + "(select f, e, d, c, b, a from t left outer join s on b = e)");

        rs = st.executeQuery(
            " select * from r left outer join lojv on (fv = i)");

        expColNames = new String [] {"G", "H", "I", "FV", "EV", "DV", "CV", "BV", "AV"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"3", "3.000", "3", "3", "3.000", "3", "3", "3.000", "3"},
            {"4", "4.000", "4", null, null, null, null, null, null},
            {"5", "5.000", "5", null, null, null, null, null, null}
        };

        JDBC.assertFullResultSet(rs, expRS);

        rs = st.executeQuery(
            " select * from r right outer join lojv on (fv = i)");

        expColNames = new String [] {"G", "H", "I", "FV", "EV", "DV", "CV", "BV", "AV"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {null, null, null, null, null, null, "1", "1.000", "1"},
            {null, null, null, "2", "2.000", "2", "2", "2.000", "2"},
            {"3", "3.000", "3", "3", "3.000", "3", "3", "3.000", "3"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // a view on top of a LOJ view

        st.executeUpdate(
            "create view vv as (select * from lojv)");

        rs = st.executeQuery(
            " select * from r left outer join vv on (fv = i)");

        expColNames = new String [] {"G", "H", "I", "FV", "EV", "DV", "CV", "BV", "AV"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"3", "3.000", "3", "3", "3.000", "3", "3", "3.000", "3"},
            {"4", "4.000", "4", null, null, null, null, null, null},
            {"5", "5.000", "5", null, null, null, null, null, null}
        };

        JDBC.assertFullResultSet(rs, expRS);

        rs = st.executeQuery(
            " select * from r right outer join vv on (fv = i)");

        expColNames = new String [] {"G", "H", "I", "FV", "EV", "DV", "CV", "BV", "AV"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {null, null, null, null, null, null, "1", "1.000", "1"},
            {null, null, null, "2", "2.000", "2", "2", "2.000", "2"},
            {"3", "3.000", "3", "3", "3.000", "3", "3", "3.000", "3"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // check for ON predicate for LOJ reordering: no reordering

        // Update: With DERBY-4471, this query also gets reordered, cf plan
        // check below.

        cSt = prepareCall(
            " call SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1)");assertUpdateCount(cSt, 0);

        rs = st.executeQuery(
            "select * from t left outer join (s left outer join "
            + "r on (f = i)) on (b = e and a > b)");

        expColNames = new String [] {"A", "B", "C", "D", "E", "F", "G", "H", "I"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1", "1.000", "1", null, null, null, null, null, null},
            {"2", "2.000", "2", null, null, null, null, null, null},
            {"3", "3.000", "3", null, null, null, null, null, null}
        };

        JDBC.assertFullResultSet(rs, expRS);

        JDBC.checkPlan(st,
                  new String[] {
                      "Hash Left Outer Join ResultSet:",
                      "Left result set:",
                      "_Hash Left Outer Join ResultSet:",
                      "_Left result set:",
                      "__Table Scan ResultSet for T",
                      "_Right result set:",
                      "__Project-Restrict ResultSet (5):",
                      "__Source result set:",
                      "___Hash Scan ResultSet for S",
                      "Right result set:",
                      "_Hash Scan ResultSet for R"});

        // Update: With DERBY-4471, this query also gets reordered, cf plan
        // check below.
        rs = st.executeQuery(
            " select * from t left outer join (s left outer join "
            + "r on (f = i)) on (b = e and a = b)");

        expColNames = new String [] {"A", "B", "C", "D", "E", "F", "G", "H", "I"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1", "1.000", "1", null, null, null, null, null, null},
            {"2", "2.000", "2", "2", "2.000", "2", null, null, null},
            {"3", "3.000", "3", "3", "3.000", "3", "3", "3.000", "3"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        JDBC.checkPlan(st,
                  new String[] {
                      "Hash Left Outer Join ResultSet:",
                      "Left result set:",
                      "_Hash Left Outer Join ResultSet:",
                      "_Left result set:",
                      "__Table Scan ResultSet for T",
                      "_Right result set:",
                      "__Project-Restrict ResultSet (5):",
                      "__Source result set:",
                      "___Hash Scan ResultSet for S",
                      "Right result set:",
                      "_Hash Scan ResultSet for R"});

        rs = st.executeQuery(
            " select * from t left outer join (s left outer join "
            + "r on (f = i)) on (b = e and 1 = 1)");

        expColNames = new String [] {"A", "B", "C", "D", "E", "F", "G", "H", "I"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1", "1.000", "1", null, null, null, null, null, null},
            {"2", "2.000", "2", "2", "2.000", "2", null, null, null},
            {"3", "3.000", "3", "3", "3.000", "3", "3", "3.000", "3"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        JDBC.checkPlan(st,
                  new String[] {
                      "Hash Left Outer Join ResultSet:",
                      "Left result set:",
                      "_Hash Left Outer Join ResultSet:",
                      "_Left result set:",
                      "__Table Scan ResultSet for T",
                      "_Right result set:",
                      "__Project-Restrict ResultSet (5):",
                      "__Source result set:",
                      "___Hash Scan ResultSet for S",
                      "Right result set:",
                      "_Hash Scan ResultSet for R"});

        rs = st.executeQuery(
            " select * from t left outer join (s left outer join "
            + "r on (f = i)) on (b > e)");

        expColNames = new String [] {"A", "B", "C", "D", "E", "F", "G", "H", "I"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1", "1.000", "1", null, null, null, null, null, null},
            {"2", "2.000", "2", null, null, null, null, null, null},
            {"3", "3.000", "3", "2", "2.000", "2", null, null, null}
        };

        JDBC.assertFullResultSet(rs, expRS);

        JDBC.checkPlan(st,
                  new String[] {
                      "Hash Left Outer Join ResultSet:",
                      "Left result set:",
                      "_Nested Loop Left Outer Join ResultSet:",
                      "_Left result set:",
                      "__Table Scan ResultSet for T",
                      "_Right result set:",
                      "__Table Scan ResultSet for S",
                      "Right result set:",
                      "_Hash Scan ResultSet for R"});

        rs = st.executeQuery(
            " select * from t left outer join (s left outer join "
            + "r on (f = i)) on (a = a)");

        expColNames = new String [] {"A", "B", "C", "D", "E", "F", "G", "H", "I"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1", "1.000", "1", "2", "2.000", "2", null, null, null},
            {"1", "1.000", "1", "3", "3.000", "3", "3", "3.000", "3"},
            {"1", "1.000", "1", "4", "4.000", "4", "4", "4.000", "4"},
            {"2", "2.000", "2", "2", "2.000", "2", null, null, null},
            {"2", "2.000", "2", "3", "3.000", "3", "3", "3.000", "3"},
            {"2", "2.000", "2", "4", "4.000", "4", "4", "4.000", "4"},
            {"3", "3.000", "3", "2", "2.000", "2", null, null, null},
            {"3", "3.000", "3", "3", "3.000", "3", "3", "3.000", "3"},
            {"3", "3.000", "3", "4", "4.000", "4", "4", "4.000", "4"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        rs = st.executeQuery(
            " select * from t left outer join (s left outer join "
            + "r on (f = i)) on (a = 1)");

        expColNames = new String [] {"A", "B", "C", "D", "E", "F", "G", "H", "I"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1", "1.000", "1", "2", "2.000", "2", null, null, null},
            {"1", "1.000", "1", "3", "3.000", "3", "3", "3.000", "3"},
            {"1", "1.000", "1", "4", "4.000", "4", "4", "4.000", "4"},
            {"2", "2.000", "2", null, null, null, null, null, null},
            {"3", "3.000", "3", null, null, null, null, null, null}
        };

        JDBC.assertFullResultSet(rs, expRS);

        rs = st.executeQuery(
            " select * from t left outer join (s left outer join "
            + "r on (f = i)) on (1 = 1)");

        expColNames = new String [] {"A", "B", "C", "D", "E", "F", "G", "H", "I"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1", "1.000", "1", "2", "2.000", "2", null, null, null},
            {"1", "1.000", "1", "3", "3.000", "3", "3", "3.000", "3"},
            {"1", "1.000", "1", "4", "4.000", "4", "4", "4.000", "4"},
            {"2", "2.000", "2", "2", "2.000", "2", null, null, null},
            {"2", "2.000", "2", "3", "3.000", "3", "3", "3.000", "3"},
            {"2", "2.000", "2", "4", "4.000", "4", "4", "4.000", "4"},
            {"3", "3.000", "3", "2", "2.000", "2", null, null, null},
            {"3", "3.000", "3", "3", "3.000", "3", "3", "3.000", "3"},
            {"3", "3.000", "3", "4", "4.000", "4", "4", "4.000", "4"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // check for ON predicate for LOJ reordering: reordering
        // is done

        rs = st.executeQuery(
            "select * from t left outer join (s left outer join "
            + "r on (f = i)) on (a = d and b = e and c = f)");

        expColNames = new String [] {"A", "B", "C", "D", "E", "F", "G", "H", "I"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1", "1.000", "1", null, null, null, null, null, null},
            {"2", "2.000", "2", "2", "2.000", "2", null, null, null},
            {"3", "3.000", "3", "3", "3.000", "3", "3", "3.000", "3"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        rs = st.executeQuery(
            " select * from t left outer join (s left outer join "
            + "r on (f = i)) on ((a = d and b = e) and c = f)");

        expColNames = new String [] {"A", "B", "C", "D", "E", "F", "G", "H", "I"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1", "1.000", "1", null, null, null, null, null, null},
            {"2", "2.000", "2", "2", "2.000", "2", null, null, null},
            {"3", "3.000", "3", "3", "3.000", "3", "3", "3.000", "3"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        rs = st.executeQuery(
            " select * from t left outer join (s left outer join "
            + "r on (f = i)) on (a = d and (b = e and c = f))");

        expColNames = new String [] {"A", "B", "C", "D", "E", "F", "G", "H", "I"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1", "1.000", "1", null, null, null, null, null, null},
            {"2", "2.000", "2", "2", "2.000", "2", null, null, null},
            {"3", "3.000", "3", "3", "3.000", "3", "3", "3.000", "3"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        rs = st.executeQuery(
            " select * from t left outer join (s left outer join "
            + "r on (f = i)) on (not(a <> d or b <> e or c <> f))");

        expColNames = new String [] {"A", "B", "C", "D", "E", "F", "G", "H", "I"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1", "1.000", "1", null, null, null, null, null, null},
            {"2", "2.000", "2", "2", "2.000", "2", null, null, null},
            {"3", "3.000", "3", "3", "3.000", "3", "3", "3.000", "3"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // check for LOJ reorder with subquery

        rs = st.executeQuery(
            "select * from t left outer join (s left outer join "
            + "r on (f = i)) on (a = d) where a in (select j from tt)");

        expColNames = new String [] {"A", "B", "C", "D", "E", "F", "G", "H", "I"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"2", "2.000", "2", "2", "2.000", "2", null, null, null},
            {"3", "3.000", "3", "3", "3.000", "3", "3", "3.000", "3"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // LOJ involving the same table

        rs = st.executeQuery(
            "select * from t t1 left outer join (t t2 left outer "
            + "join t t3 on (t2.a=t3.a)) on (t1.a=t2.a)");

        expColNames = new String [] {"A", "B", "C", "A", "B", "C", "A", "B", "C"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1", "1.000", "1", "1", "1.000", "1", "1", "1.000", "1"},
            {"2", "2.000", "2", "2", "2.000", "2", "2", "2.000", "2"},
            {"3", "3.000", "3", "3", "3.000", "3", "3", "3.000", "3"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        rs = st.executeQuery(
            " select * from t t1 left outer join (t t2 left "
            + "outer join t t3 on (t2.a=t3.a)) on (t1.b=t2.b)");

        expColNames = new String [] {"A", "B", "C", "A", "B", "C", "A", "B", "C"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1", "1.000", "1", "1", "1.000", "1", "1", "1.000", "1"},
            {"2", "2.000", "2", "2", "2.000", "2", "2", "2.000", "2"},
            {"3", "3.000", "3", "3", "3.000", "3", "3", "3.000", "3"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        rs = st.executeQuery(
            " select * from t t1 left outer join (t t2 left "
            + "outer join t t3 on (t2.a=t3.b)) on (t1.a=t2.b)");

        expColNames = new String [] {"A", "B", "C", "A", "B", "C", "A", "B", "C"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1", "1.000", "1", "1", "1.000", "1", "1", "1.000", "1"},
            {"2", "2.000", "2", "2", "2.000", "2", "2", "2.000", "2"},
            {"3", "3.000", "3", "3", "3.000", "3", "3", "3.000", "3"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // comparing columns of different types.

        rs = st.executeQuery(
            "select t.a, s.d, r.g from t left outer join (s left "
            + "outer join r on (e=g)) on (b=d)");

        expColNames = new String [] {"A", "D", "G"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1", null, null},
            {"2", "2", null},
            {"3", "3", "3"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        rs = st.executeQuery(
            " select r.g from t left outer join (s left outer "
            + "join r on (e=g)) on (b=d)");

        expColNames = new String [] {"G"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {null},
            {null},
            {"3"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // these seem to work

        rs = st.executeQuery(
            "select * from t left outer join (s left outer join "
            + "r on (e=g)) on (b=d)");

        expColNames = new String [] {"A", "B", "C", "D", "E", "F", "G", "H", "I"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1", "1.000", "1", null, null, null, null, null, null},
            {"2", "2.000", "2", "2", "2.000", "2", null, null, null},
            {"3", "3.000", "3", "3", "3.000", "3", "3", "3.000", "3"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        rs = st.executeQuery(
            " select t.a from t left outer join (s left outer "
            + "join r on (e=g)) on (b=d)");

        expColNames = new String [] {"A"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1"},
            {"2"},
            {"3"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        rs = st.executeQuery(
            " select s.f, s.e, s.d, t.c, t.b, t.a from t left "
            + "outer join (s left outer join r on (e=g)) on (b=d)");

        expColNames = new String [] {"F", "E", "D", "C", "B", "A"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {null, null, null, "1", "1.000", "1"},
            {"2", "2.000", "2", "2", "2.000", "2"},
            {"3", "3.000", "3", "3", "3.000", "3"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // need to test more on changes deep inside...

        st.executeUpdate(
            "create view loj (a, b, c, d, e, f, g, h, i, ae) as "
            + "(select a, b, c, d, e, f, g, h, i, a+e as \"A+E\" "
            + "from t left outer join (s left outer join r on (f = "
            + "i)) on (b = e))");

        rs = st.executeQuery(
            " select * from loj");

        expColNames = new String [] {"A", "B", "C", "D", "E", "F", "G", "H", "I", "AE"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1", "1.000", "1", null, null, null, null, null, null, null},
            {"2", "2.000", "2", "2", "2.000", "2", null, null, null, "4.000"},
            {"3", "3.000", "3", "3", "3.000", "3", "3", "3.000", "3", "6.000"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        rs = st.executeQuery(
            " select * from t left outer join loj on (t.a=loj.a)");

        expColNames = new String [] {"A", "B", "C", "A", "B", "C", "D", "E", "F", "G", "H", "I", "AE"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1", "1.000", "1", "1", "1.000", "1", null, null, null, null, null, null, null},
            {"2", "2.000", "2", "2", "2.000", "2", "2", "2.000", "2", null, null, null, "4.000"},
            {"3", "3.000", "3", "3", "3.000", "3", "3", "3.000", "3", "3", "3.000", "3", "6.000"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        rs = st.executeQuery(
            " select * from t left outer join loj on (t.a=loj.d)");

        expColNames = new String [] {"A", "B", "C", "A", "B", "C", "D", "E", "F", "G", "H", "I", "AE"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1", "1.000", "1", null, null, null, null, null, null, null, null, null, null},
            {"2", "2.000", "2", "2", "2.000", "2", "2", "2.000", "2", null, null, null, "4.000"},
            {"3", "3.000", "3", "3", "3.000", "3", "3", "3.000", "3", "3", "3.000", "3", "6.000"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        rs = st.executeQuery(
            " select * from t right outer join loj on (t.a=loj.a)");

        expColNames = new String [] {"A", "B", "C", "A", "B", "C", "D", "E", "F", "G", "H", "I", "AE"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1", "1.000", "1", "1", "1.000", "1", null, null, null, null, null, null, null},
            {"2", "2.000", "2", "2", "2.000", "2", "2", "2.000", "2", null, null, null, "4.000"},
            {"3", "3.000", "3", "3", "3.000", "3", "3", "3.000", "3", "3", "3.000", "3", "6.000"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        rs = st.executeQuery(
            " select * from t inner join loj on (t.a=loj.a)");

        expColNames = new String [] {"A", "B", "C", "A", "B", "C", "D", "E", "F", "G", "H", "I", "AE"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1", "1.000", "1", "1", "1.000", "1", null, null, null, null, null, null, null},
            {"2", "2.000", "2", "2", "2.000", "2", "2", "2.000", "2", null, null, null, "4.000"},
            {"3", "3.000", "3", "3", "3.000", "3", "3", "3.000", "3", "3", "3.000", "3", "6.000"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 4 level deep b=e will be pushed first, one level down
        // j=a will then be pushed next, 2 levels down (after b=e
        // pushed)

        rs = st.executeQuery(
            "select * from tt left outer join (t left outer join "
            + "(s left outer join r on (f = i)) on (b = e)) on (j=a)");

        expColNames = new String [] {"J", "K", "L", "A", "B", "C", "D", "E", "F", "G", "H", "I"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"2", "2.000", "2", "2", "2.000", "2", "2", "2.000", "2", null, null, null},
            {"3", "3.000", "3", "3", "3.000", "3", "3", "3.000", "3", "3", "3.000", "3"},
            {"4", "4.000", "4", null, null, null, null, null, null, null, null, null}
        };

        JDBC.assertFullResultSet(rs, expRS);

        rs = st.executeQuery(
            " select * from tt left outer join (t left outer "
            + "join (s left outer join r on (f = i)) on (b = e)) "
            + " on (j=a) where j>0");

        expColNames = new String [] {"J", "K", "L", "A", "B", "C", "D", "E", "F", "G", "H", "I"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"2", "2.000", "2", "2", "2.000", "2", "2", "2.000", "2", null, null, null},
            {"3", "3.000", "3", "3", "3.000", "3", "3", "3.000", "3", "3", "3.000", "3"},
            {"4", "4.000", "4", null, null, null, null, null, null, null, null, null}
        };

        JDBC.assertFullResultSet(rs, expRS);

        rs = st.executeQuery(
            " select * from tt left outer join (t left outer "
            + "join (s left outer join r on (f = i)) on (b = e)) "
            + " on (j=a) where j>0");

        expColNames = new String [] {"J", "K", "L", "A", "B", "C", "D", "E", "F", "G", "H", "I"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"2", "2.000", "2", "2", "2.000", "2", "2", "2.000", "2", null, null, null},
            {"3", "3.000", "3", "3", "3.000", "3", "3", "3.000", "3", "3", "3.000", "3"},
            {"4", "4.000", "4", null, null, null, null, null, null, null, null, null}
        };

        JDBC.assertFullResultSet(rs, expRS);

        rs = st.executeQuery(
            " select * from tt left outer join (t left outer "
            + "join (s left outer join r on (f = i)) on (b = e)) on (j=d)");

        expColNames = new String [] {"J", "K", "L", "A", "B", "C", "D", "E", "F", "G", "H", "I"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"2", "2.000", "2", "2", "2.000", "2", "2", "2.000", "2", null, null, null},
            {"3", "3.000", "3", "3", "3.000", "3", "3", "3.000", "3", "3", "3.000", "3"},
            {"4", "4.000", "4", null, null, null, null, null, null, null, null, null}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // TT LOJ (T LOJ (S LOJ R))) becomes TT LOJ ((T LOJ S) LOJ R)

        rs = st.executeQuery(
            "select 1 from tt left outer join (t left outer join "
            + "(s left outer join r on (f = i)) on (b = e)) on (j=r.g)");

        expColNames = new String [] {"1"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1"},
            {"1"},
            {"1"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        rs = st.executeQuery(
            " select 1 from tt right outer join (t left outer "
            + "join (s left outer join r on (f = i)) on (b = e)) on (j=g)");

        expColNames = new String [] {"1"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1"},
            {"1"},
            {"1"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // okay

        rs = st.executeQuery(
            "select 1 from tt, (t left outer join (s left outer "
            + "join r on (f = i)) on (b = e)) where (j=g)");

        expColNames = new String [] {"1"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        rs = st.executeQuery(
            " select 1 from tt right outer join (t left outer "
            + "join (s inner join r on (f = i)) on (b = e)) on (j=g)");

        expColNames = new String [] {"1"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1"},
            {"1"},
            {"1"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        rs = st.executeQuery(
            " select 1 from tt inner join (t left outer join (s "
            + "left outer join r on (f = i)) on (b = e)) on (j=g)");

        expColNames = new String [] {"1"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // LOJ w/ left-deep LOJ: LOJ will be pushed twice

        rs = st.executeQuery(
            "select * from tt left outer join (t left outer join "
            + "s on (b=e) left outer join r on (f = i)) on (j=a)");

        expColNames = new String [] {"J", "K", "L", "A", "B", "C", "D", "E", "F", "G", "H", "I"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"2", "2.000", "2", "2", "2.000", "2", "2", "2.000", "2", null, null, null},
            {"3", "3.000", "3", "3", "3.000", "3", "3", "3.000", "3", "3", "3.000", "3"},
            {"4", "4.000", "4", null, null, null, null, null, null, null, null, null}
        };

        JDBC.assertFullResultSet(rs, expRS);

        rs = st.executeQuery(
            " select * from tt left outer join (t left outer "
            + "join s on (b=e) left outer join r on (f = i)) on "
            + "(j=a) where j > 0");

        expColNames = new String [] {"J", "K", "L", "A", "B", "C", "D", "E", "F", "G", "H", "I"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"2", "2.000", "2", "2", "2.000", "2", "2", "2.000", "2", null, null, null},
            {"3", "3.000", "3", "3", "3.000", "3", "3", "3.000", "3", "3", "3.000", "3"},
            {"4", "4.000", "4", null, null, null, null, null, null, null, null, null}
        };

        JDBC.assertFullResultSet(rs, expRS);

        rs = st.executeQuery(
            " select * from tt left outer join (t left outer "
            + "join s on (b=e) left outer join r on (f = i)) on "
            + "(j=a) where b > 0");

        expColNames = new String [] {"J", "K", "L", "A", "B", "C", "D", "E", "F", "G", "H", "I"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"2", "2.000", "2", "2", "2.000", "2", "2", "2.000", "2", null, null, null},
            {"3", "3.000", "3", "3", "3.000", "3", "3", "3.000", "3", "3", "3.000", "3"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        rs = st.executeQuery(
            " select * from tt left outer join (t left outer "
            + "join s on (b=e) left outer join r on (f = i)) on "
            + "(j=a) where e > 0");

        expColNames = new String [] {"J", "K", "L", "A", "B", "C", "D", "E", "F", "G", "H", "I"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"2", "2.000", "2", "2", "2.000", "2", "2", "2.000", "2", null, null, null},
            {"3", "3.000", "3", "3", "3.000", "3", "3", "3.000", "3", "3", "3.000", "3"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        rs = st.executeQuery(
            " select * from tt left outer join (t left outer "
            + "join s on (b=e) left outer join r on (f = i)) on "
            + "(j=a) where g > 0");

        expColNames = new String [] {"J", "K", "L", "A", "B", "C", "D", "E", "F", "G", "H", "I"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"3", "3.000", "3", "3", "3.000", "3", "3", "3.000", "3", "3", "3.000", "3"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        rs = st.executeQuery(
            " select j+1, k+2, L||'s', a+10, b+10, C||'t', d+20, "
            + "e+20, f||'u', g+30, h+30, i||'v' from tt left outer "
            + "join (t left outer join s on (b=e) left outer join "
            + "r on (f = i)) on (j=a)");

        expColNames = new String [] {"1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"3", "4.000", "2s", "12", "12.000", "2t", "22", "22.000", "2u", null, null, null},
            {"4", "5.000", "3s", "13", "13.000", "3t", "23", "23.000", "3u", "33", "33.000", "3v"},
            {"5", "6.000", "4s", null, null, null, null, null, null, null, null, null}
        };

        JDBC.assertFullResultSet(rs, expRS);

        rs = st.executeQuery(
            " select j, j+1, k, k+2, L, L||'s' from tt left "
            + "outer join (t left outer join s on (b=e) left outer "
            + "join r on (f = i)) on (j=a)");

        expColNames = new String [] {"J", "2", "K", "4", "L", "6"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"2", "3", "2.000", "4.000", "2", "2s"},
            {"3", "4", "3.000", "5.000", "3", "3s"},
            {"4", "5", "4.000", "6.000", "4", "4s"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        rs = st.executeQuery(
            " select a, a+10, b, b+10, C, C||'t' from tt left "
            + "outer join (t left outer join s on (b=e) left outer "
            + "join r on (f = i)) on (j=a)");

        expColNames = new String [] {"A", "2", "B", "4", "C", "6"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"2", "12", "2.000", "12.000", "2", "2t"},
            {"3", "13", "3.000", "13.000", "3", "3t"},
            {null, null, null, null, null, null}
        };

        JDBC.assertFullResultSet(rs, expRS);

        rs = st.executeQuery(
            " select d, d+20, e, e+20, f, f||'u' from tt left "
            + "outer join (t left outer join s on (b=e) left outer "
            + "join r on (f = i)) on (j=a)");

        expColNames = new String [] {"D", "2", "E", "4", "F", "6"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"2", "22", "2.000", "22.000", "2", "2u"},
            {"3", "23", "3.000", "23.000", "3", "3u"},
            {null, null, null, null, null, null}
        };

        JDBC.assertFullResultSet(rs, expRS);

        rs = st.executeQuery(
            " select g, g+30, h, h+30, i, i||'v' from tt left "
            + "outer join (t left outer join s on (b=e) left outer "
            + "join r on (f = i)) on (j=a)");

        expColNames = new String [] {"G", "2", "H", "4", "I", "6"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {null, null, null, null, null, null},
            {"3", "33", "3.000", "33.000", "3", "3v"},
            {null, null, null, null, null, null}
        };

        JDBC.assertFullResultSet(rs, expRS);

        rs = st.executeQuery(
            " select * from tt left outer join (t left outer "
            + "join s on (b=e) left outer join r on (f = i)) on (j=d)");

        expColNames = new String [] {"J", "K", "L", "A", "B", "C", "D", "E", "F", "G", "H", "I"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"2", "2.000", "2", "2", "2.000", "2", "2", "2.000", "2", null, null, null},
            {"3", "3.000", "3", "3", "3.000", "3", "3", "3.000", "3", "3", "3.000", "3"},
            {"4", "4.000", "4", null, null, null, null, null, null, null, null, null}
        };

        JDBC.assertFullResultSet(rs, expRS);

        rs = st.executeQuery(
            " select * from tt left outer join (t left outer "
            + "join s on (b=e) left outer join r on (f = i)) on "
            + "(j=d) where j > 0");

        expColNames = new String [] {"J", "K", "L", "A", "B", "C", "D", "E", "F", "G", "H", "I"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"2", "2.000", "2", "2", "2.000", "2", "2", "2.000", "2", null, null, null},
            {"3", "3.000", "3", "3", "3.000", "3", "3", "3.000", "3", "3", "3.000", "3"},
            {"4", "4.000", "4", null, null, null, null, null, null, null, null, null}
        };

        JDBC.assertFullResultSet(rs, expRS);

        rs = st.executeQuery(
            " select * from tt left outer join (t left outer "
            + "join s on (b=e) left outer join r on (f = i)) on "
            + "(j=d) where b > 0");

        expColNames = new String [] {"J", "K", "L", "A", "B", "C", "D", "E", "F", "G", "H", "I"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"2", "2.000", "2", "2", "2.000", "2", "2", "2.000", "2", null, null, null},
            {"3", "3.000", "3", "3", "3.000", "3", "3", "3.000", "3", "3", "3.000", "3"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        rs = st.executeQuery(
            " select * from tt left outer join (t left outer "
            + "join s on (b=e) left outer join r on (f = i)) on "
            + "(j=d) where e > 0");

        expColNames = new String [] {"J", "K", "L", "A", "B", "C", "D", "E", "F", "G", "H", "I"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"2", "2.000", "2", "2", "2.000", "2", "2", "2.000", "2", null, null, null},
            {"3", "3.000", "3", "3", "3.000", "3", "3", "3.000", "3", "3", "3.000", "3"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        rs = st.executeQuery(
            " select * from tt left outer join (t left outer "
            + "join s on (b=e) left outer join r on (f = i)) on "
            + "(j=d) where g > 0");

        expColNames = new String [] {"J", "K", "L", "A", "B", "C", "D", "E", "F", "G", "H", "I"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"3", "3.000", "3", "3", "3.000", "3", "3", "3.000", "3", "3", "3.000", "3"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        rs = st.executeQuery(
            " select * from tt left outer join (t left outer "
            + "join s on (b=e) left outer join r on (f = i)) on (j=g)");

        expColNames = new String [] {"J", "K", "L", "A", "B", "C", "D", "E", "F", "G", "H", "I"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"2", "2.000", "2", null, null, null, null, null, null, null, null, null},
            {"3", "3.000", "3", "3", "3.000", "3", "3", "3.000", "3", "3", "3.000", "3"},
            {"4", "4.000", "4", null, null, null, null, null, null, null, null, null}
        };

        JDBC.assertFullResultSet(rs, expRS);

        rs = st.executeQuery(
            " select * from tt left outer join (t left outer "
            + "join s on (b=e) left outer join r on (f = i)) on "
            + "(j=g) where j > 0");

        expColNames = new String [] {"J", "K", "L", "A", "B", "C", "D", "E", "F", "G", "H", "I"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"2", "2.000", "2", null, null, null, null, null, null, null, null, null},
            {"3", "3.000", "3", "3", "3.000", "3", "3", "3.000", "3", "3", "3.000", "3"},
            {"4", "4.000", "4", null, null, null, null, null, null, null, null, null}
        };

        JDBC.assertFullResultSet(rs, expRS);

        rs = st.executeQuery(
            " select * from tt left outer join (t left outer "
            + "join s on (b=e) left outer join r on (f = i)) on "
            + "(j=g) where b > 0");

        expColNames = new String [] {"J", "K", "L", "A", "B", "C", "D", "E", "F", "G", "H", "I"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"3", "3.000", "3", "3", "3.000", "3", "3", "3.000", "3", "3", "3.000", "3"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        rs = st.executeQuery(
            " select * from tt left outer join (t left outer "
            + "join s on (b=e) left outer join r on (f = i)) on "
            + "(j=g) where e > 0");

        expColNames = new String [] {"J", "K", "L", "A", "B", "C", "D", "E", "F", "G", "H", "I"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"3", "3.000", "3", "3", "3.000", "3", "3", "3.000", "3", "3", "3.000", "3"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        rs = st.executeQuery(
            " select * from tt left outer join (t left outer "
            + "join s on (b=e) left outer join r on (f = i)) on "
            + "(j=g) where g > 0");

        expColNames = new String [] {"J", "K", "L", "A", "B", "C", "D", "E", "F", "G", "H", "I"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"3", "3.000", "3", "3", "3.000", "3", "3", "3.000", "3", "3", "3.000", "3"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // Create views of union, regular joins, subquery, group
        // by, having clause, and use the view in the LOJ operands
        // and yet the LOJ reordering is applicable. We need to
        // test the join condition: LHS=RHS.

        st.executeUpdate(
            "create view v1 (fv, ev, dv, cv, bv, av) as (select "
            + "f, e, d, c, b, a from t left outer join s on b = e)");

        st.executeUpdate(
            " create view v2 (fv, ev, dv, cv, bv, av) as (select "
            + "f, e, d, c, b, a from t inner join s on b = e)");

        st.executeUpdate(
            " create view v3 (fv, ev, dv, cv, bv, av) as (select "
            + "f, e, d, c, b, a from t right join s on b = e)");

        st.executeUpdate(
            " create view v4 (fv, ev, dv, cv, bv, av) as (select "
            + "f, e, d, c, b, a from t, s where b = e)");

        st.executeUpdate(
            " create view v5 (cv, bv, cnt) as (select c, b, "
            + "count(*) from t group by c, b)");

        st.executeUpdate(
            " create view v6 (cv, bv, cnt) as (select c, b, "
            + "count(*) from t group by c, b having b > 0)");

        st.executeUpdate(
            " create view v7 (cv, bv, av) as (select c, b, a "
            + "from t where b in (select e from s))");

        st.executeUpdate(
            " create view v8 (cv, bv, av) as (select c, b, a "
            + "from t union select f, e, d from s)");

        // All these test cases need table references to determine
        // if the join condition is proper for LOJ reordering: LHS
        // = RHS

        rs = st.executeQuery(
            "select * from v1 left outer join (s left outer join "
            + "r on (f = i)) on (d=v1.av)");

        expColNames = new String [] {"FV", "EV", "DV", "CV", "BV", "AV", "D", "E", "F", "G", "H", "I"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {null, null, null, "1", "1.000", "1", null, null, null, null, null, null},
            {"2", "2.000", "2", "2", "2.000", "2", "2", "2.000", "2", null, null, null},
            {"3", "3.000", "3", "3", "3.000", "3", "3", "3.000", "3", "3", "3.000", "3"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        rs = st.executeQuery(
            " select * from v2 left outer join (s left outer "
            + "join r on (f = i)) on (d=v2.av)");

        expColNames = new String [] {"FV", "EV", "DV", "CV", "BV", "AV", "D", "E", "F", "G", "H", "I"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"2", "2.000", "2", "2", "2.000", "2", "2", "2.000", "2", null, null, null},
            {"3", "3.000", "3", "3", "3.000", "3", "3", "3.000", "3", "3", "3.000", "3"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        rs = st.executeQuery(
            " select * from v3 left outer join (s left outer "
            + "join r on (f = i)) on (d=v3.av)");

        expColNames = new String [] {"FV", "EV", "DV", "CV", "BV", "AV", "D", "E", "F", "G", "H", "I"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"2", "2.000", "2", "2", "2.000", "2", "2", "2.000", "2", null, null, null},
            {"3", "3.000", "3", "3", "3.000", "3", "3", "3.000", "3", "3", "3.000", "3"},
            {"4", "4.000", "4", null, null, null, null, null, null, null, null, null}
        };

        JDBC.assertFullResultSet(rs, expRS);

        rs = st.executeQuery(
            " select * from v4 left outer join (s left outer "
            + "join r on (f = i)) on (d=v4.av)");

        expColNames = new String [] {"FV", "EV", "DV", "CV", "BV", "AV", "D", "E", "F", "G", "H", "I"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"2", "2.000", "2", "2", "2.000", "2", "2", "2.000", "2", null, null, null},
            {"3", "3.000", "3", "3", "3.000", "3", "3", "3.000", "3", "3", "3.000", "3"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        rs = st.executeQuery(
            " select * from v5 left outer join (s left outer "
            + "join r on (f = i)) on (e=v5.bv)");

        expColNames = new String [] {"CV", "BV", "CNT", "D", "E", "F", "G", "H", "I"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1", "1.000", "1", null, null, null, null, null, null},
            {"2", "2.000", "1", "2", "2.000", "2", null, null, null},
            {"3", "3.000", "1", "3", "3.000", "3", "3", "3.000", "3"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        rs = st.executeQuery(
            " select * from v6 left outer join (s left outer "
            + "join r on (f = i)) on (e=v6.bv)");

        expColNames = new String [] {"CV", "BV", "CNT", "D", "E", "F", "G", "H", "I"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1", "1.000", "1", null, null, null, null, null, null},
            {"2", "2.000", "1", "2", "2.000", "2", null, null, null},
            {"3", "3.000", "1", "3", "3.000", "3", "3", "3.000", "3"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        rs = st.executeQuery(
            " select * from v7 left outer join (s left outer "
            + "join r on (f = i)) on (e=v7.bv)");

        expColNames = new String [] {"CV", "BV", "AV", "D", "E", "F", "G", "H", "I"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"2", "2.000", "2", "2", "2.000", "2", null, null, null},
            {"3", "3.000", "3", "3", "3.000", "3", "3", "3.000", "3"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        rs = st.executeQuery(
            " select * from v8 left outer join (s left outer "
            + "join r on (f = i)) on (e=v8.bv)");

        expColNames = new String [] {"CV", "BV", "AV", "D", "E", "F", "G", "H", "I"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1", "1.000", "1", null, null, null, null, null, null},
            {"2", "2.000", "2", "2", "2.000", "2", null, null, null},
            {"3", "3.000", "3", "3", "3.000", "3", "3", "3.000", "3"},
            {"4", "4.000", "4", "4", "4.000", "4", "4", "4.000", "4"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        rs = st.executeQuery(
            " select * from t left outer join (s left outer join "
            + "v1 on (f = cv)) on (d=a)");

        expColNames = new String [] {"A", "B", "C", "D", "E", "F", "FV", "EV", "DV", "CV", "BV", "AV"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1", "1.000", "1", null, null, null, "1", "1.000", "1", null, null, null},
            {"2", "2.000", "2", "2", "2.000", "2", "2", "2.000", "2", "2", "2.000", "2"},
            {"3", "3.000", "3", "3", "3.000", "3", "3", "3.000", "3", "3", "3.000", "3"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        rs = st.executeQuery(
            " select * from t left outer join (s left outer join "
            + "v8 on (f = cv)) on (d=a)");

        expColNames = new String [] {"A", "B", "C", "D", "E", "F", "CV", "BV", "AV"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1", "1.000", "1", null, null, null, "1", "1.000", "1"},
            {"2", "2.000", "2", "2", "2.000", "2", "2", "2.000", "2"},
            {"3", "3.000", "3", "3", "3.000", "3", "3", "3.000", "3"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        rs = st.executeQuery(
            " select * from t left outer join (v1 left outer "
            + "join s on (f = cv)) on (av=a)");

        expColNames = new String [] {"A", "B", "C", "FV", "EV", "DV", "CV", "BV", "AV", "D", "E", "F"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1", "1.000", "1", "1", "1.000", "1", null, null, null, null, null, null},
            {"2", "2.000", "2", "2", "2.000", "2", "2", "2.000", "2", "2", "2.000", "2"},
            {"3", "3.000", "3", "3", "3.000", "3", "3", "3.000", "3", "3", "3.000", "3"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        rs = st.executeQuery(
            " select * from t left outer join (v8 left outer "
            + "join s on (f = cv)) on (av=a)");

        expColNames = new String [] {"A", "B", "C", "CV", "BV", "AV", "D", "E", "F"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1", "1.000", "1", "1", "1.000", "1", null, null, null},
            {"2", "2.000", "2", "2", "2.000", "2", "2", "2.000", "2"},
            {"3", "3.000", "3", "3", "3.000", "3", "3", "3.000", "3"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // No LOJ reordering due to join conditions

        rs = st.executeQuery(
            "select * from v1 left outer join (s left outer join "
            + "r on (f = i)) on (g=v1.av)");

        expColNames = new String [] {"FV", "EV", "DV", "CV", "BV", "AV", "D", "E", "F", "G", "H", "I"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {null, null, null, "1", "1.000", "1", null, null, null, null, null, null},
            {"2", "2.000", "2", "2", "2.000", "2", null, null, null, null, null, null},
            {"3", "3.000", "3", "3", "3.000", "3", "3", "3.000", "3", "3", "3.000", "3"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        rs = st.executeQuery(
            " select * from v2 left outer join (s left outer "
            + "join r on (f = i)) on (g=v2.av)");

        expColNames = new String [] {"FV", "EV", "DV", "CV", "BV", "AV", "D", "E", "F", "G", "H", "I"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"2", "2.000", "2", "2", "2.000", "2", null, null, null, null, null, null},
            {"3", "3.000", "3", "3", "3.000", "3", "3", "3.000", "3", "3", "3.000", "3"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        rs = st.executeQuery(
            " select * from t left outer join (s left outer join "
            + "v1 on (f = cv)) on (av=a)");

        expColNames = new String [] {"A", "B", "C", "D", "E", "F", "FV", "EV", "DV", "CV", "BV", "AV"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1", "1.000", "1", null, null, null, null, null, null, null, null, null},
            {"2", "2.000", "2", "2", "2.000", "2", "2", "2.000", "2", "2", "2.000", "2"},
            {"3", "3.000", "3", "3", "3.000", "3", "3", "3.000", "3", "3", "3.000", "3"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        rs = st.executeQuery(
            " select * from t left outer join (s left outer join "
            + "v8 on (f = cv)) on (av=a)");

        expColNames = new String [] {"A", "B", "C", "D", "E", "F", "CV", "BV", "AV"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1", "1.000", "1", null, null, null, null, null, null},
            {"2", "2.000", "2", "2", "2.000", "2", "2", "2.000", "2"},
            {"3", "3.000", "3", "3", "3.000", "3", "3", "3.000", "3"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        rs = st.executeQuery(
            " select * from t left outer join (v1 left outer "
            + "join s on (f = cv)) on (d=a)");

        expColNames = new String [] {"A", "B", "C", "FV", "EV", "DV", "CV", "BV", "AV", "D", "E", "F"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1", "1.000", "1", null, null, null, null, null, null, null, null, null},
            {"2", "2.000", "2", "2", "2.000", "2", "2", "2.000", "2", "2", "2.000", "2"},
            {"3", "3.000", "3", "3", "3.000", "3", "3", "3.000", "3", "3", "3.000", "3"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        rs = st.executeQuery(
            " select * from t left outer join (v8 left outer "
            + "join s on (f = cv)) on (d=a)");

        expColNames = new String [] {"A", "B", "C", "CV", "BV", "AV", "D", "E", "F"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1", "1.000", "1", null, null, null, null, null, null},
            {"2", "2.000", "2", "2", "2.000", "2", "2", "2.000", "2"},
            {"3", "3.000", "3", "3", "3.000", "3", "3", "3.000", "3"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // values clause select * from table (values (1, 2, 3)) as
        // q(a, b, c)

        st.executeUpdate(
            "drop view v1");

        st.executeUpdate(
            " drop view v2");

        st.executeUpdate(
            " drop view v3");

        st.executeUpdate(
            " drop view v4");

        st.executeUpdate(
            " drop view v5");

        st.executeUpdate(
            " drop view v6");

        st.executeUpdate(
            " drop view v7");

        st.executeUpdate(
            " drop view v8");

        st.executeUpdate(
            " drop view loj");

        st.executeUpdate(
            " drop view vv");

        st.executeUpdate(
            " drop view lojv");

        st.executeUpdate(
            " drop view jv");

        st.executeUpdate(
            " DROP TABLE T");

        st.executeUpdate(
            " DROP TABLE S");

        st.executeUpdate(
            " DROP TABLE R");

        st.executeUpdate(
            " DROP TABLE TT");

        st.close();
        rollback();
    }


    public void test_LojReorderTest_b() throws Exception
    {
        ResultSet rs = null;
        CallableStatement cSt;
        Statement st = createStatement();

        String [][] expRS;
        String [] expColNames;

        setAutoCommit(false);

        // from the customer

        st.executeUpdate(
            "CREATE TABLE TNL1 (ID INTEGER NOT NULL UNIQUE, "
            + "col_char CHAR(20), col_decimal DECIMAL(12,5))");

        st.executeUpdate(
            " insert into TNL1 values (1, 'abc', 1.1)");

        st.executeUpdate(
            " insert into TNL1 values (2, 'bcd', 2.2)");

        st.executeUpdate(
            " insert into TNL1 values (3, 'cde', 3.3)");

        st.executeUpdate(
            " insert into TNL1 values (4, 'ABC', 1.1)");

        st.executeUpdate(
            " insert into TNL1 values (5, 'BCD', 2.2)");

        st.executeUpdate(
            " insert into TNL1 values (6, 'CDE', 3.3)");

        st.executeUpdate(
            " CREATE TABLE TNL1_1 (ID INTEGER NOT NULL UNIQUE, "
            + "col_char CHAR(20), col_decimal DECIMAL(12,5), ID2 INTEGER)");

        st.executeUpdate(
            " CREATE INDEX I_TNL11 ON TNL1_1 (ID2 ASC)");

        st.executeUpdate(
            " insert into TNL1_1 values (3, 'cde', 3.3, 30)");

        st.executeUpdate(
            " insert into TNL1_1 values (4, 'xyz', 4.4, 40)");

        st.executeUpdate(
            " insert into TNL1_1 values (5, 'lmn', 5.5, 50)");

        st.executeUpdate(
            " insert into TNL1_1 values (6, 'CDE', 3.3, 30)");

        st.executeUpdate(
            " insert into TNL1_1 values (7, 'XYZ', 4.4, 40)");

        st.executeUpdate(
            " insert into TNL1_1 values (8, 'LMN', 5.5, 50)");

        st.executeUpdate(
            " CREATE TABLE TNL1_1_1 (ID INTEGER NOT NULL UNIQUE, "
            + "col_char CHAR(20), col_decimal DECIMAL(12,5))");

        st.executeUpdate(
            " insert into TNL1_1_1 values (2, 'bcd', 2.2)");

        st.executeUpdate(
            " insert into TNL1_1_1 values (3, 'cde', 3.3)");

        st.executeUpdate(
            " insert into TNL1_1_1 values (4, 'xyz', 4.4)");

        st.executeUpdate(
            " insert into TNL1_1_1 values (5, 'BCD', 2.2)");

        st.executeUpdate(
            " insert into TNL1_1_1 values (6, 'CDE', 3.3)");

        st.executeUpdate(
            " insert into TNL1_1_1 values (7, 'XYZ', 4.4)");

        st.executeUpdate(
            " CREATE TABLE TNL1_2 (ID INTEGER NOT NULL UNIQUE, "
            + "col_char CHAR(20), col_decimal DECIMAL(12,5))");

        st.executeUpdate(
            " insert into TNL1_2 values (4, 'xyz', 4.4)");

        st.executeUpdate(
            " insert into TNL1_2 values (5, 'lmn', 5.5)");

        st.executeUpdate(
            " insert into TNL1_2 values (6, 'stu', 6.6)");

        st.executeUpdate(
            " insert into TNL1_2 values (7, 'XYZ', 4.4)");

        st.executeUpdate(
            " insert into TNL1_2 values (8, 'LMN', 5.5)");

        st.executeUpdate(
            " insert into TNL1_2 values (9, 'STU', 6.6)");

        cSt = prepareCall(
            " call SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1)");assertUpdateCount(cSt, 0);


        rs = st.executeQuery(
            " SELECT * FROM TNL1 A LEFT OUTER JOIN TNL1_1 B ON "
            + "A.ID = B.ID LEFT OUTER JOIN  TNL1_1_1 C ON "
            + "B.ID2=C.ID LEFT OUTER JOIN TNL1_2 D ON A.ID=D.ID ORDER BY 1");

        expColNames = new String [] {"ID", "COL_CHAR", "COL_DECIMAL", "ID", "COL_CHAR", "COL_DECIMAL", "ID2", "ID", "COL_CHAR", "COL_DECIMAL", "ID", "COL_CHAR", "COL_DECIMAL"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1", "abc", "1.10000", null, null, null, null, null, null, null, null, null, null},
            {"2", "bcd", "2.20000", null, null, null, null, null, null, null, null, null, null},
            {"3", "cde", "3.30000", "3", "cde", "3.30000", "30", null, null, null, null, null, null},
            {"4", "ABC", "1.10000", "4", "xyz", "4.40000", "40", null, null, null, "4", "xyz", "4.40000"},
            {"5", "BCD", "2.20000", "5", "lmn", "5.50000", "50", null, null, null, "5", "lmn", "5.50000"},
            {"6", "CDE", "3.30000", "6", "CDE", "3.30000", "30", null, null, null, "6", "stu", "6.60000"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        JDBC.checkPlan(st,
                  new String[] {
                  "Sort ResultSet:",
                  "In sorted order = false",
                  "Source result set:",
                  "_Hash Left Outer Join ResultSet:",
                  "_Left result set:",
                  "__Hash Left Outer Join ResultSet:",
                  "__Left result set:",
                  "___Hash Left Outer Join ResultSet:",
                  "___Left result set:",
                  "____Table Scan ResultSet for TNL1",
                  "___Right result set:",
                  "____Hash Scan ResultSet for TNL1_1",
                  "__Right result set:",
                  "___Hash Scan ResultSet for TNL1_1_1",
                  "_Right result set:",
                  "__Hash Scan ResultSet for TNL1_2"});

        rs = st.executeQuery(
            " SELECT * FROM TNL1 A LEFT OUTER JOIN (TNL1_1 B "
            + "LEFT OUTER JOIN TNL1_1_1 C ON B.ID2=C.ID) ON "
            + "A.ID=B.ID LEFT OUTER JOIN TNL1_2 D ON A.ID=D.ID ORDER BY 1");

        expColNames = new String [] {"ID", "COL_CHAR", "COL_DECIMAL", "ID", "COL_CHAR", "COL_DECIMAL", "ID2", "ID", "COL_CHAR", "COL_DECIMAL", "ID", "COL_CHAR", "COL_DECIMAL"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1", "abc", "1.10000", null, null, null, null, null, null, null, null, null, null},
            {"2", "bcd", "2.20000", null, null, null, null, null, null, null, null, null, null},
            {"3", "cde", "3.30000", "3", "cde", "3.30000", "30", null, null, null, null, null, null},
            {"4", "ABC", "1.10000", "4", "xyz", "4.40000", "40", null, null, null, "4", "xyz", "4.40000"},
            {"5", "BCD", "2.20000", "5", "lmn", "5.50000", "50", null, null, null, "5", "lmn", "5.50000"},
            {"6", "CDE", "3.30000", "6", "CDE", "3.30000", "30", null, null, null, "6", "stu", "6.60000"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        JDBC.checkPlan(
            st,
            new String[] {
                "Sort ResultSet:",
                "Number of opens = 1",
                "Rows input = 6",
                "Rows returned = 6",
                "Source result set:",
                "_Hash Left Outer Join ResultSet:",
                "_Left result set:",
                "__Hash Left Outer Join ResultSet:",
                "__Left result set:",
                "___Hash Left Outer Join ResultSet:",
                "___Left result set:",
                "____Table Scan ResultSet for TNL1",
                "___Right result set:",
                "____Hash Scan ResultSet for TNL1_1",
                "__Right result set:",
                "___Hash Scan ResultSet for TNL1_1_1",
                "_Right result set:",
                "__Hash Scan ResultSet for TNL1_2"});

        cSt = prepareCall(
            " call SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(0)");assertUpdateCount(cSt, 0);

        st.executeUpdate(
            " DROP TABLE TNL1");

        st.executeUpdate(
            " DROP TABLE TNL1_1");

        st.executeUpdate(
            " DROP TABLE TNL1_1_1");

        st.executeUpdate(
            " DROP TABLE TNL1_2");
        st.close();
        rollback();
    }


    public void test_LojReorderTest_c() throws SQLException
    {
        Statement st = createStatement();

        setAutoCommit(false);

        part_i(st);
        part_ii(st);
        part_iii(st);
        part_iv(st);
        part_v(st);

        st.close();
        rollback();

    }


    public void part_i(Statement st) throws SQLException
    {
        ResultSet rs = null;

        String [][] expRS;
        String [] expColNames;

        //- outerjn.db

        st.executeUpdate(
            "CREATE SCHEMA K55ADMIN");

        st.executeUpdate(
            " CREATE TABLE K55ADMIN.PARTS"
            + "      (PART     CHAR(10),"
            + "       NUM    SMALLINT,"
            + "       SUPPLIER CHAR(20))");

        st.executeUpdate(
            " CREATE TABLE K55ADMIN.PARTS_T"
            + "      (PART     CHAR(10),"
            + "       NUM1    SMALLINT,"
            + "       NUM2    SMALLINT,"
            + "       SUPPLIER CHAR(20))");

        st.executeUpdate(
            " CREATE TABLE K55ADMIN.PARTS_NOTNULL"
            + "      (PART     CHAR(10)  NOT NULL,"
            + "       NUM    SMALLINT  NOT NULL,"
            + "       SUPPLIER CHAR(20)  NOT NULL)");

        st.executeUpdate(
            " CREATE TABLE K55ADMIN.PARTS_ALLNULL"
            + "      (PART     CHAR(10),"
            + "       NUM    SMALLINT,"
            + "       SUPPLIER CHAR(20))");

        st.executeUpdate(
            " CREATE TABLE K55ADMIN.PARTS_EMPTY"
            + "      (PART     CHAR(10),"
            + "       NUM    SMALLINT,"
            + "       SUPPLIER CHAR(20))");

        st.executeUpdate(
            " CREATE TABLE K55ADMIN.PARTS_EMPTY_NN"
            + "      (PART     CHAR(10)  NOT NULL,"
            + "       NUM    SMALLINT  NOT NULL,"
            + "       SUPPLIER CHAR(20)  NOT NULL)");

        st.executeUpdate(
            " CREATE TABLE K55ADMIN.PRODUCTS"
            + "      (NUM    SMALLINT,"
            + "       PRODUCT  CHAR(15),"
            + "       PRICE    DECIMAL(7,2))");

        st.executeUpdate(
            " CREATE TABLE K55ADMIN.PRODUCTS_T"
            + "      (NUM1    SMALLINT,"
            + "       NUM2    SMALLINT,"
            + "       PRODUCT  CHAR(15),"
            + "       PRICE    DECIMAL(7,2))");

        st.executeUpdate(
            " CREATE TABLE K55ADMIN.PRODUCTS_NOTNULL"
            + "      (NUM    SMALLINT     NOT NULL,"
            + "       PRODUCT  CHAR(15)     NOT NULL,"
            + "       PRICE    DECIMAL(7,2) NOT NULL)");

        st.executeUpdate(
            " CREATE TABLE K55ADMIN.PRODUCTS_ALLNULL"
            + "      (NUM    SMALLINT,"
            + "       PRODUCT  CHAR(15),"
            + "       PRICE    DECIMAL(7,2))");

        st.executeUpdate(
            " CREATE TABLE K55ADMIN.PRODUCTS_EMPTY"
            + "      (NUM    SMALLINT,"
            + "       PRODUCT  CHAR(15),"
            + "       PRICE    DECIMAL(7,2))");

        st.executeUpdate(
            " CREATE TABLE K55ADMIN.PRODUCTS_EMPTY_NN"
            + "      (NUM    SMALLINT     NOT NULL,"
            + "       PRODUCT  CHAR(15)     NOT NULL,"
            + "       PRICE    DECIMAL(7,2) NOT NULL)");

        st.executeUpdate(
            " CREATE TABLE K55ADMIN.S90"
            + "      (DEPT     CHAR(3)      NOT NULL,"
            + "       SALES    SMALLINT)");

        st.executeUpdate(
            " CREATE TABLE K55ADMIN.S91"
            + "      (DEPT     CHAR(3),"
            + "       SALES    SMALLINT)");

        st.executeUpdate(
            " CREATE TABLE K55ADMIN.S92"
            + "      (DEPT     CHAR(3)      NOT NULL,"
            + "       SALES    SMALLINT)");

        st.executeUpdate(
            " CREATE TABLE K55ADMIN.EMPLOYEES"
            + "      (EMP_ID        CHAR(6)     NOT NULL,"
            + "       EMP_NAME      VARCHAR(25),"
            + "       SALARY        INTEGER,"
            + "       COMM          SMALLINT)");

        st.executeUpdate(
            " CREATE UNIQUE INDEX K55ADMIN.EMPLOYIX"
            + "  ON K55ADMIN.EMPLOYEES(EMP_ID)");

        st.executeUpdate(
            " CREATE TABLE K55ADMIN.OLD_OFFICES"
            + "      (OLD_OFFICE    CHAR(4)     NOT NULL,"
            + "       EMP_ID        CHAR(6))");

        st.executeUpdate(
            " CREATE UNIQUE INDEX k55ADMIN.OLD_OFFIX"
            + "  ON K55ADMIN.OLD_OFFICES(OLD_OFFICE)");

        st.executeUpdate(
            " CREATE TABLE K55ADMIN.NEW_OFFICES"
            + "      (NEW_OFFICE    CHAR(4)     NOT NULL,"
            + "       EMP_ID        CHAR(6))");

        st.executeUpdate(
            " CREATE UNIQUE INDEX k55ADMIN.NEW_OFFIX"
            + "  ON K55ADMIN.NEW_OFFICES(NEW_OFFICE)");

        //*********************************************************
        // ****************   This last group is for general usage
        // throughout the tests
        // *********************************************************
        // ****************

        st.executeUpdate(
            "CREATE TABLE K55ADMIN.MANYTYPES"
            + "      (INTCOL        INTEGER,"
            + "       SMINTCOL      SMALLINT,"
            + "       DEC62COL      DECIMAL(6,2),"
            + "       DEC72COL      DECIMAL(7,2),"
            + "       FLOATCOL      FLOAT,"
            + "       CHARCOL       CHAR(10),"
            + "       LCHARCOL      CHAR(250),"
            + "       VCHARCOL      VARCHAR(100))");

        st.executeUpdate(
            " CREATE TABLE K55ADMIN.MANYTYPES_NOTNULL"
            + "      (INTCOL        INTEGER       NOT NULL,"
            + "       SMINTCOL      SMALLINT      NOT NULL,"
            + "       DEC62COL      DECIMAL(6,2)  NOT NULL,"
            + "       DEC72COL      DECIMAL(7,2)  NOT NULL,"
            + "       FLOATCOL      FLOAT         NOT NULL,"
            + "       CHARCOL       CHAR(15)      NOT NULL,"
            + "       LCHARCOL      CHAR(250)     NOT NULL,"
            + "       VCHARCOL      VARCHAR(100)  NOT NULL)");

        //* CREATE TABLE K55ADMIN.MANYTYPES_CTRL LIKE
        // MANYTYPES_NOTNULL

        st.executeUpdate(
            "CREATE TABLE K55ADMIN.MANYTYPES_CTRL"
            + "      (INTCOL        INTEGER       NOT NULL,"
            + "       SMINTCOL      SMALLINT      NOT NULL,"
            + "       DEC62COL      DECIMAL(6,2)  NOT NULL,"
            + "       DEC72COL      DECIMAL(7,2)  NOT NULL,"
            + "       FLOATCOL      FLOAT         NOT NULL,"
            + "       CHARCOL       CHAR(15)      NOT NULL,"
            + "       LCHARCOL      CHAR(250)     NOT NULL,"
            + "       VCHARCOL      VARCHAR(100)  NOT NULL)");

        //*********************************************************
        // **************** Populate the tables created above
        //
        // *********************************************************
        // ****************

        st.executeUpdate(
            "INSERT INTO K55ADMIN.PARTS VALUES ('Wire',     10,'ACWF')");

        st.executeUpdate(
            " INSERT INTO K55ADMIN.PARTS VALUES ('Oil',     "
            + "160,'Western-Chem')");

        st.executeUpdate(
            " INSERT INTO K55ADMIN.PARTS VALUES ('Magnets',  "
            + "10,'Bateman')");

        st.executeUpdate(
            " INSERT INTO K55ADMIN.PARTS VALUES ('Plastic',  "
            + "30,'Plastik-Corp')");

        st.executeUpdate(
            " INSERT INTO K55ADMIN.PARTS VALUES ('Blades',  "
            + "205,'Ace-Steel')");

        st.executeUpdate(
            " INSERT INTO K55ADMIN.PARTS VALUES ('Paper',    "
            + "20,'Ace-Steel')");

        st.executeUpdate(
            " INSERT INTO K55ADMIN.PARTS VALUES ('Steel',    30,'ACWF')");

        //*

        st.executeUpdate(
            "INSERT INTO K55ADMIN.PARTS_ALLNULL VALUES (NULL,NULL,NULL)");

        st.executeUpdate(
            " INSERT INTO K55ADMIN.PARTS_ALLNULL VALUES (NULL,NULL,NULL)");

        st.executeUpdate(
            " INSERT INTO K55ADMIN.PARTS_ALLNULL VALUES (NULL,NULL,NULL)");

        //*

        st.executeUpdate(
            "INSERT INTO K55ADMIN.PARTS_NOTNULL"
            + "       SELECT * FROM K55ADMIN.PARTS");

        st.executeUpdate(
            " INSERT INTO K55ADMIN.PARTS_T"
            + "       SELECT PART, NUM, 10+NUM,SUPPLIER FROM "
            + "K55ADMIN.PARTS WHERE K55ADMIN.PARTS.NUM>10");

        st.executeUpdate(
            " INSERT INTO K55ADMIN.PARTS VALUES (NULL,    30,NULL)");

        st.executeUpdate(
            " INSERT INTO K55ADMIN.PARTS_T VALUES ('Unknown', "
            + "NULL, NULL, NULL)");

        //*

        st.executeUpdate(
            "INSERT INTO K55ADMIN.PRODUCTS VALUES (505, "
            + "'Screwdriver',  3.70)");

        st.executeUpdate(
            " INSERT INTO K55ADMIN.PRODUCTS VALUES ( 30, "
            + "'Relay',        7.55)");

        st.executeUpdate(
            " INSERT INTO K55ADMIN.PRODUCTS VALUES ( 50, "
            + "'Hammer',       5.75)");

        st.executeUpdate(
            " INSERT INTO K55ADMIN.PRODUCTS VALUES (205, 'Saw',  "
            + "       18.90)");

        st.executeUpdate(
            " INSERT INTO K55ADMIN.PRODUCTS VALUES ( 10, "
            + "'Generator',   45.75)");

        st.executeUpdate(
            " INSERT INTO K55ADMIN.PRODUCTS VALUES ( 20, "
            + "'Sander',      35.75)");

        st.executeUpdate(
            " INSERT INTO K55ADMIN.PRODUCTS VALUES ( 30, "
            + "'Ruler',        8.75)");

        //*

        st.executeUpdate(
            "INSERT INTO K55ADMIN.PRODUCTS_NOTNULL"
            + "       SELECT * FROM K55ADMIN.PRODUCTS");

        st.executeUpdate(
            " INSERT INTO K55ADMIN.PRODUCTS_T"
            + "       SELECT NUM, 10+NUM, PRODUCT, PRICE  FROM "
            + "K55ADMIN.PRODUCTS WHERE PRICE>7");

        st.executeUpdate(
            " INSERT INTO K55ADMIN.PRODUCTS VALUES ( 20, NULL, NULL)");

        st.executeUpdate(
            " INSERT INTO K55ADMIN.PRODUCTS_T VALUES ( NULL, "
            + "NULL, 'Unknown', NULL)");

        //*

        st.executeUpdate(
            "INSERT INTO K55ADMIN.PRODUCTS_ALLNULL VALUES (NULL, "
            + "NULL, NULL)");

        st.executeUpdate(
            " INSERT INTO K55ADMIN.PRODUCTS_ALLNULL VALUES "
            + "(NULL, NULL, NULL)");

        st.executeUpdate(
            " INSERT INTO K55ADMIN.PRODUCTS_ALLNULL VALUES "
            + "(NULL, NULL, NULL)");

        //*

        st.executeUpdate(
            "INSERT INTO K55ADMIN.S90 VALUES ('M62',10)");

        st.executeUpdate(
            " INSERT INTO K55ADMIN.S90 VALUES ('M09',99)");

        st.executeUpdate(
            " INSERT INTO K55ADMIN.S90 VALUES ('J64',64)");

        st.executeUpdate(
            " INSERT INTO K55ADMIN.S91 VALUES ('M62',100)");

        st.executeUpdate(
            " INSERT INTO K55ADMIN.S91 VALUES ('M09',10)");

        st.executeUpdate(
            " INSERT INTO K55ADMIN.S91 VALUES ('M03',500)");

        st.executeUpdate(
            " INSERT INTO K55ADMIN.S92 VALUES ('M62',50)");

        st.executeUpdate(
            " INSERT INTO K55ADMIN.S92 VALUES ('M03',10)");

        st.executeUpdate(
            " INSERT INTO K55ADMIN.S92 VALUES ('J64',50)");

        //*

        st.executeUpdate(
            "INSERT INTO K55ADMIN.EMPLOYEES VALUES ('711276','J. "
            + "Thomas',75000,1500)");

        st.executeUpdate(
            " INSERT INTO K55ADMIN.EMPLOYEES VALUES "
            + "('480923','C. Manthey',33500, 500)");

        st.executeUpdate(
            " INSERT INTO K55ADMIN.EMPLOYEES VALUES "
            + "('368521','B. Ward',46700,0)");

        st.executeUpdate(
            " INSERT INTO K55ADMIN.EMPLOYEES VALUES "
            + "('966641','K. Woods',41300,350)");

        st.executeUpdate(
            " INSERT INTO K55ADMIN.EMPLOYEES VALUES ('537260',NULL,0,0)");

        st.executeUpdate(
            " INSERT INTO K55ADMIN.EMPLOYEES VALUES "
            + "('216861','N. Baxter',52000,550)");

        //*

        st.executeUpdate(
            "INSERT INTO K55ADMIN.OLD_OFFICES VALUES ('X124','480923')");

        st.executeUpdate(
            " INSERT INTO K55ADMIN.OLD_OFFICES VALUES ('X125','711276')");

        st.executeUpdate(
            " INSERT INTO K55ADMIN.OLD_OFFICES VALUES ('X126','988870')");

        st.executeUpdate(
            " INSERT INTO K55ADMIN.OLD_OFFICES VALUES ('X127','368521')");

        st.executeUpdate(
            " INSERT INTO K55ADMIN.OLD_OFFICES VALUES ('X128','537260')");

        st.executeUpdate(
            " INSERT INTO K55ADMIN.OLD_OFFICES VALUES ('X129','622273')");

        st.executeUpdate(
            " INSERT INTO K55ADMIN.OLD_OFFICES VALUES ('X130',NULL    )");

        //*

        st.executeUpdate(
            "INSERT INTO K55ADMIN.NEW_OFFICES VALUES ('Y124','537260')");

        st.executeUpdate(
            " INSERT INTO K55ADMIN.NEW_OFFICES VALUES ('Y125','368521')");

        st.executeUpdate(
            " INSERT INTO K55ADMIN.NEW_OFFICES VALUES ('Y126','711276')");

        st.executeUpdate(
            " INSERT INTO K55ADMIN.NEW_OFFICES VALUES ('Y127',NULL    )");

        st.executeUpdate(
            " INSERT INTO K55ADMIN.NEW_OFFICES VALUES ('Y128','480923')");

        st.executeUpdate(
            " INSERT INTO K55ADMIN.NEW_OFFICES VALUES ('Y129','216861')");

        st.executeUpdate(
            " INSERT INTO K55ADMIN.NEW_OFFICES VALUES ('Y130','333666')");

        //*

        st.executeUpdate(
            "INSERT INTO K55ADMIN.MANYTYPES VALUES "
            + "(1,1,1.0,1.0,1E0,'One','One', 'One')");

        st.executeUpdate(
            " INSERT INTO K55ADMIN.MANYTYPES VALUES "
            + "(2,2,2.0,2.0,2E0,'Two','Two', 'Two')");

        st.executeUpdate(
            " INSERT INTO K55ADMIN.MANYTYPES VALUES "
            + "(3,3,3.0,3.0,3E0,'Three','Three', 'Three')");

        st.executeUpdate(
            " INSERT INTO K55ADMIN.MANYTYPES VALUES "
            + "(4,4,4.0,4.0,4E0,'Four','Four', 'Four')");

        st.executeUpdate(
            " INSERT INTO K55ADMIN.MANYTYPES VALUES "
            + "(5,5,5.0,5.0,5E0,'Five','Five', 'Five')");

        st.executeUpdate(
            " INSERT INTO K55ADMIN.MANYTYPES VALUES "
            + "(6,6,6.0,6.0,6E0,'Six','Six', 'Six')");

        st.executeUpdate(
            " INSERT INTO K55ADMIN.MANYTYPES VALUES "
            + "(7,7,7.0,7.0,7E0,'Seven','Seven', 'Seven')");

        st.executeUpdate(
            " INSERT INTO K55ADMIN.MANYTYPES VALUES "
            + "(8,8,8.0,8.0,8E0,'Eight','Eight', 'Eight')");

        st.executeUpdate(
            " INSERT INTO K55ADMIN.MANYTYPES VALUES "
            + "(9,9,9.0,9.0,9E0,'Nine','Nine', 'Nine')");

        st.executeUpdate(
            " INSERT INTO K55ADMIN.MANYTYPES VALUES "
            + "(10,10,10.0,10.0,1E1,'Ten','Ten', 'Ten')");

        st.executeUpdate(
            " INSERT INTO K55ADMIN.MANYTYPES VALUES "
            + "(11,11,11.0,11.0,1.1E1,'Eleven', 'Eleven','Eleven')");

        st.executeUpdate(
            " INSERT INTO K55ADMIN.MANYTYPES VALUES "
            + "(12,12,12.0,12.0,1.2E1,'Twelve', 'Twelve','Twelve')");

        st.executeUpdate(
            " INSERT INTO K55ADMIN.MANYTYPES VALUES "
            + "(13,13,13.0,13.0,1.3E1,'Thirteen', 'Thirteen','Thirteen')");

        st.executeUpdate(
            " INSERT INTO K55ADMIN.MANYTYPES VALUES "
            + "(14,14,14.0,14.0,1.4E1,'Fourteen', 'Fourteen','Fourteen')");

        st.executeUpdate(
            " INSERT INTO K55ADMIN.MANYTYPES VALUES "
            + "(15,15,15.0,15.0,1.5E1,'Fifteen', 'Fifteen','Fifteen')");

        st.executeUpdate(
            " INSERT INTO K55ADMIN.MANYTYPES VALUES "
            + "(16,16,16.0,16.0,1.6E1,'Sixteen', 'Sixteen','Sixteen')");

        st.executeUpdate(
            " INSERT INTO K55ADMIN.MANYTYPES VALUES "
            + "(17,17,17.0,17.0,1.7E1,'Seventeen', 'Seventeen','Seventeen')");

        st.executeUpdate(
            " INSERT INTO K55ADMIN.MANYTYPES VALUES "
            + "(18,18,18.0,18.0,1.8E1,'Eighteen', 'Eighteen','Eighteen')");

        st.executeUpdate(
            " INSERT INTO K55ADMIN.MANYTYPES VALUES "
            + "(19,19,19.0,19.0,1.9E1,'Nineteen', 'Nineteen','Nineteen')");

        st.executeUpdate(
            " INSERT INTO K55ADMIN.MANYTYPES VALUES "
            + "(20,20,20.0,20.0,2E1,'Twenty', 'Twenty','Twenty')");

        //*

        st.executeUpdate(
            "INSERT INTO K55ADMIN.MANYTYPES_NOTNULL VALUES"
            + "   (11,11,11.0,11.0,1.1E1,'Eleven','Eleven', 'Eleven')");

        st.executeUpdate(
            " INSERT INTO K55ADMIN.MANYTYPES_NOTNULL VALUES"
            + "   (12,12,12.0,12.0,1.2E1,'Twelve','Twelve', 'Twelve')");

        st.executeUpdate(
            " INSERT INTO K55ADMIN.MANYTYPES_NOTNULL VALUES"
            + "   (13,13,13.0,13.0,1.3E1,'Thirteen', 'Thirteen','Thirteen')");

        st.executeUpdate(
            " INSERT INTO K55ADMIN.MANYTYPES_NOTNULL VALUES"
            + "   (14,14,14.0,14.0,1.4E1,'Fourteen', 'Fourteen','Fourteen')");

        st.executeUpdate(
            " INSERT INTO K55ADMIN.MANYTYPES_NOTNULL VALUES"
            + "   (15,15,15.0,15.0,1.5E1,'Fifteen', 'Fifteen','Fifteen')");

        st.executeUpdate(
            " INSERT INTO K55ADMIN.MANYTYPES_NOTNULL VALUES"
            + "   (16,16,16.0,16.0,1.6E1,'Sixteen', 'Sixteen','Sixteen')");

        st.executeUpdate(
            " INSERT INTO K55ADMIN.MANYTYPES_NOTNULL VALUES"
            + "   (17,17,17.0,17.0,1.7E1,'Seventeen', "
            + "'Seventeen','Seventeen')");

        st.executeUpdate(
            " INSERT INTO K55ADMIN.MANYTYPES_NOTNULL VALUES"
            + "   (18,18,18.0,18.0,1.8E1,'Eighteen', 'Eighteen','Eighteen')");

        st.executeUpdate(
            " INSERT INTO K55ADMIN.MANYTYPES_NOTNULL VALUES"
            + "   (19,19,19.0,19.0,1.9E1,'Nineteen', 'Nineteen','Nineteen')");

        st.executeUpdate(
            " INSERT INTO K55ADMIN.MANYTYPES_NOTNULL VALUES"
            + "   (20,20,20.0,20.0,2E1,'Twenty','Twenty', 'Twenty')");

        st.executeUpdate(
            " INSERT INTO K55ADMIN.MANYTYPES_NOTNULL VALUES"
            + "   (21,21,21.0,21.0,2.1E1,'Twenty One','Twenty "
            + " one', 'Twenty One')");

        st.executeUpdate(
            " INSERT INTO K55ADMIN.MANYTYPES_NOTNULL VALUES"
            + "   (22,22,22.0,22.0,2.2E1,'Twenty Two','Twenty "
            + "Two', 'Twenty Two')");

        st.executeUpdate(
            " INSERT INTO K55ADMIN.MANYTYPES_NOTNULL VALUES"
            + "   (23,23,23.0,23.0,2.3E1,'Twenty Three','Twenty "
            + "Three', 'Twenty Three')");

        st.executeUpdate(
            " INSERT INTO K55ADMIN.MANYTYPES_NOTNULL VALUES"
            + "   (24,24,24.0,24.0,2.4E1,'Twenty Four','Twenty "
            + "Four', 'Twenty Four')");

        st.executeUpdate(
            " INSERT INTO K55ADMIN.MANYTYPES_NOTNULL VALUES"
            + "   (25,25,25.0,25.0,2.5E1,'Twenty Five','Twenty "
            + "Five', 'Twenty Five')");

        //*

        st.executeUpdate(
            "INSERT INTO K55ADMIN.MANYTYPES_CTRL VALUES"
            + "   (2,2,2.0,2.0,2E0,'Two','Two', 'Two')");

        st.executeUpdate(
            " INSERT INTO K55ADMIN.MANYTYPES_CTRL VALUES"
            + "   (4,4,4.0,4.0,4E0,'Four','Four', 'Four')");

        st.executeUpdate(
            " INSERT INTO K55ADMIN.MANYTYPES_CTRL VALUES"
            + "   (6,6,6.0,6.0,6E0,'Six','Six', 'Six')");

        st.executeUpdate(
            " INSERT INTO K55ADMIN.MANYTYPES_CTRL VALUES"
            + "   (8,8,8.0,8.0,8E0,'Eight','Eight', 'Eight')");

        st.executeUpdate(
            " INSERT INTO K55ADMIN.MANYTYPES_CTRL VALUES"
            + "   (10,10,10.0,10.0,1E1,'Ten','Ten', 'Ten')");

        st.executeUpdate(
            " INSERT INTO K55ADMIN.MANYTYPES_CTRL VALUES"
            + "   (12,12,12.0,12.0,1.2E1,'Twelve','Twelve', 'Twelve')");

        st.executeUpdate(
            " INSERT INTO K55ADMIN.MANYTYPES_CTRL VALUES"
            + "   (14,14,14.0,14.0,1.4E1,'Fourteen', 'Fourteen','Fourteen')");

        st.executeUpdate(
            " INSERT INTO K55ADMIN.MANYTYPES_CTRL VALUES"
            + "   (16,16,16.0,16.0,1.6E1,'Sixteen', 'Sixteen','Sixteen')");

        st.executeUpdate(
            " INSERT INTO K55ADMIN.MANYTYPES_CTRL VALUES"
            + "   (18,18,18.0,18.0,1.8E1,'Eighteen', 'Eighteen','Eighteen')");

        st.executeUpdate(
            " INSERT INTO K55ADMIN.MANYTYPES_CTRL VALUES"
            + "   (20,20,20.0,20.0,2E1,'Twenty','Twenty', 'Twenty')");

        st.executeUpdate(
            " INSERT INTO K55ADMIN.MANYTYPES_CTRL VALUES"
            + "   (22,22,22.0,22.0,2.2E1,'Twenty Two','Twenty "
            + "Two', 'Twenty Two')");

        st.executeUpdate(
            " INSERT INTO K55ADMIN.MANYTYPES_CTRL VALUES"
            + "   (24,24,24.0,24.0,2.4E1,'Twenty Four','Twenty "
            + "Four', 'Twenty Four')");

        // coj201.clp
        //------------------------------------------------------------
        // test unit 1. OUTER JOIN w/ correlated subquery;
        //------------------------------------------------------------
        // 101 - 'between' in LEFT JOIN ON condition;
        //       '>=all' subquery correlated to tuple-preserving
        //        operand in the WHERE clause

        rs = st.executeQuery(
            "select part, p.num, product, pt.NUM, price "
            + "from k55admin.parts p left join k55admin.products pt "
            + " on   (p.num between pt.num and pt.num + 5)"
            + " where pt.price+20 >= all"
            + "       (select a.price"
            + "        from k55admin.products a"
            + "        where a.num>=p.num)"
            + "order by 1,2,3,4");

        expColNames = new String [] {"PART", "NUM", "PRODUCT", "NUM", "PRICE"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Blades", "205", "Saw", "205", "18.90"},
            {"Plastic", "30", "Relay", "30", "7.55"},
            {"Plastic", "30", "Ruler", "30", "8.75"},
            {"Steel", "30", "Relay", "30", "7.55"},
            {"Steel", "30", "Ruler", "30", "8.75"},
            {null, "30", "Relay", "30", "7.55"},
            {null, "30", "Ruler", "30", "8.75"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 102 - 'between' in RIGHT JOIN ON condition;
        // '>=all' subquery correlated to tuple-preserving operand
        // in the WHERE clause

        rs = st.executeQuery(
            "select part, p.num, product, pt.NUM, price"
            + " from k55admin.parts p right join k55admin.products pt"
            + " on   (p.num between pt.num and pt.num + 5)"
            + " where pt.price+20 >= all"
            + "       (select a.price"
            + "        from k55admin.products a"
            + "        where a.num>=p.num)"
            + "order by 1,2,3,4");

        expColNames = new String [] {"PART", "NUM", "PRODUCT", "NUM", "PRICE"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Blades", "205", "Saw", "205", "18.90"},
            {"Plastic", "30", "Relay", "30", "7.55"},
            {"Plastic", "30", "Ruler", "30", "8.75"},
            {"Steel", "30", "Relay", "30", "7.55"},
            {"Steel", "30", "Ruler", "30", "8.75"},
            {null, "30", "Relay", "30", "7.55"},
            {null, "30", "Ruler", "30", "8.75"},
            {null, null, "Hammer", "50", "5.75"},
            {null, null, "Screwdriver", "505", "3.70"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 103 - '='/like/in in LEFT JOIN ON condition;
        // '>=all' subquery correlated to null-producing operand in
        // the WHERE clause

        rs = st.executeQuery(
            "select part, p.num, product, pt.NUM, price"
            + " from k55admin.parts p left join k55admin.products pt"
            + " on (pt.product in ('Bolt','Nuts') or"
            + "    pt.product like 'Power%' or"
            + "    p.num = pt.num)"
            + " where pt.price+20 >= all"
            + "       (select a.price"
            + "        from k55admin.products a"
            + "        where a.num>=pt.num)"
            + "order by 1,2,3,4");

        expColNames = new String [] {"PART", "NUM", "PRODUCT", "NUM", "PRICE"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Blades", "205", "Saw", "205", "18.90"},
            {"Oil", "160", null, null, null},
            {"Plastic", "30", "Relay", "30", "7.55"},
            {"Plastic", "30", "Ruler", "30", "8.75"},
            {"Steel", "30", "Relay", "30", "7.55"},
            {"Steel", "30", "Ruler", "30", "8.75"},
            {null, "30", "Relay", "30", "7.55"},
            {null, "30", "Ruler", "30", "8.75"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 104 - '='/like/in in RIGHT JOIN ON condition;
        // '>=all' subquery correlated to null-producing operand in
        // the WHERE clause

        rs = st.executeQuery(
            "select part, p.num, product, pt.NUM, price"
            + " from k55admin.parts p right join k55admin.products pt"
            + " on (pt.product in ('Bolt','Nuts') or"
            + "    pt.product like 'Power%' or"
            + "    p.num = pt.num)"
            + " where pt.price+20 >= all"
            + "       (select a.price"
            + "        from k55admin.products a"
            + "        where a.num>=pt.num)"
            + "order by 1,2,3,4");

        expColNames = new String [] {"PART", "NUM", "PRODUCT", "NUM", "PRICE"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Blades", "205", "Saw", "205", "18.90"},
            {"Plastic", "30", "Relay", "30", "7.55"},
            {"Plastic", "30", "Ruler", "30", "8.75"},
            {"Steel", "30", "Relay", "30", "7.55"},
            {"Steel", "30", "Ruler", "30", "8.75"},
            {null, "30", "Relay", "30", "7.55"},
            {null, "30", "Ruler", "30", "8.75"},
            {null, null, "Hammer", "50", "5.75"},
            {null, null, "Screwdriver", "505", "3.70"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 105 - 'between' in LEFT JOIN ON condition
        // IN subquery correlated to tuple-preserving operand in the
        // WHERE clause

        rs = st.executeQuery(
            "select part, p.num, product, pt.NUM, price"
            + " from k55admin.parts p left join k55admin.products pt"
            + " on   (p.num between pt.num and pt.num + 5)"
            + " where pt.price IN"
            + "       (select a.price"
            + "        from k55admin.products a"
            + "        where a.num>=p.num)"
            + "order by 1,2,3,4");

        expColNames = new String [] {"PART", "NUM", "PRODUCT", "NUM", "PRICE"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Blades", "205", "Saw", "205", "18.90"},
            {"Magnets", "10", "Generator", "10", "45.75"},
            {"Paper", "20", "Sander", "20", "35.75"},
            {"Plastic", "30", "Relay", "30", "7.55"},
            {"Plastic", "30", "Ruler", "30", "8.75"},
            {"Steel", "30", "Relay", "30", "7.55"},
            {"Steel", "30", "Ruler", "30", "8.75"},
            {"Wire", "10", "Generator", "10", "45.75"},
            {null, "30", "Relay", "30", "7.55"},
            {null, "30", "Ruler", "30", "8.75"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 106 - 'between' in RIGHT JOIN ON condition
        // IN subquery correlated to tuple-preserving operand in the
        // WHERE clause

        rs = st.executeQuery(
            "select part, p.num, product, pt.NUM, price"
            + " from k55admin.parts p right join k55admin.products pt"
            + " on   (p.num between pt.num and pt.num + 5)"
            + " where pt.price IN"
            + "       (select a.price"
            + "        from k55admin.products a"
            + "        where a.num>=p.num)"
            + "order by 1,2,3,4");

        expColNames = new String [] {"PART", "NUM", "PRODUCT", "NUM", "PRICE"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Blades", "205", "Saw", "205", "18.90"},
            {"Magnets", "10", "Generator", "10", "45.75"},
            {"Paper", "20", "Sander", "20", "35.75"},
            {"Plastic", "30", "Relay", "30", "7.55"},
            {"Plastic", "30", "Ruler", "30", "8.75"},
            {"Steel", "30", "Relay", "30", "7.55"},
            {"Steel", "30", "Ruler", "30", "8.75"},
            {"Wire", "10", "Generator", "10", "45.75"},
            {null, "30", "Relay", "30", "7.55"},
            {null, "30", "Ruler", "30", "8.75"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 107 - '='/like/in in LEFT JOIN ON condition
        // IN subquery correlated to null-producing operand in the
        // WHERE clause

        rs = st.executeQuery(
            "select part, p.num, product, pt.NUM, price"
            + " from k55admin.parts p left join k55admin.products pt"
            + " on (pt.product in ('Bolt','Nuts') or"
            + "    pt.product like 'Power%' or"
            + "    p.num = pt.num)"
            + " where pt.price IN"
            + "       (select a.price"
            + "        from k55admin.products a"
            + "        where a.num>=pt.num)"
            + "order by 1,2,3,4");

        expColNames = new String [] {"PART", "NUM", "PRODUCT", "NUM", "PRICE"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Blades", "205", "Saw", "205", "18.90"},
            {"Magnets", "10", "Generator", "10", "45.75"},
            {"Paper", "20", "Sander", "20", "35.75"},
            {"Plastic", "30", "Relay", "30", "7.55"},
            {"Plastic", "30", "Ruler", "30", "8.75"},
            {"Steel", "30", "Relay", "30", "7.55"},
            {"Steel", "30", "Ruler", "30", "8.75"},
            {"Wire", "10", "Generator", "10", "45.75"},
            {null, "30", "Relay", "30", "7.55"},
            {null, "30", "Ruler", "30", "8.75"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 108 - '='/like/in in RIGHT JOIN ON condition
        // IN subquery correlated to null-producing operand in the
        // WHERE clause

        rs = st.executeQuery(
            "select part, p.num, product, pt.NUM, price"
            + " from k55admin.parts p right join k55admin.products pt"
            + " on (pt.product in ('Bolt','Nuts') or"
            + "    pt.product like 'Power%' or"
            + "    p.num = pt.num)"
            + " where pt.price IN"
            + "       (select a.price"
            + "        from k55admin.products a"
            + "        where a.num>=pt.num)"
            + "order by 1,2,3,4");

        expColNames = new String [] {"PART", "NUM", "PRODUCT", "NUM", "PRICE"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Blades", "205", "Saw", "205", "18.90"},
            {"Magnets", "10", "Generator", "10", "45.75"},
            {"Paper", "20", "Sander", "20", "35.75"},
            {"Plastic", "30", "Relay", "30", "7.55"},
            {"Plastic", "30", "Ruler", "30", "8.75"},
            {"Steel", "30", "Relay", "30", "7.55"},
            {"Steel", "30", "Ruler", "30", "8.75"},
            {"Wire", "10", "Generator", "10", "45.75"},
            {null, "30", "Relay", "30", "7.55"},
            {null, "30", "Ruler", "30", "8.75"},
            {null, null, "Hammer", "50", "5.75"},
            {null, null, "Screwdriver", "505", "3.70"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        //------------------------------------------------------------
        // test unit 2. nested subquery: OUTER JOIN
        // w/ correlated OUTER JOIN subquery;
        //------------------------------------------------------------

        // 201 - 'between' in LEFT JOIN ON condition;
        //       '>=all' subquery correlated to tuple-preserving
        //        operand in the WHERE clause
        //        subquery: local pred on tuple-preserving operand,
        //        '=' join predicate, and, left join

        rs = st.executeQuery(
            "select part, p.num, product, pt.NUM, price"
            + " from k55admin.parts p left join k55admin.products pt"
            + " on   (p.num between pt.num and pt.num + 5)"
            + " where pt.price+20 >= all"
            + "       (select a.price"
            + "        from k55admin.parts b LEFT JOIN k55admin.products a"
            + "        on   b.num = a.num and b.part <> 'Wire'"
            + "        where a.num>=p.num)"
            + "order by 1,2,3,4");

        expColNames = new String [] {"PART", "NUM", "PRODUCT", "NUM", "PRICE"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Blades", "205", "Saw", "205", "18.90"},
            {"Plastic", "30", "Relay", "30", "7.55"},
            {"Plastic", "30", "Ruler", "30", "8.75"},
            {"Steel", "30", "Relay", "30", "7.55"},
            {"Steel", "30", "Ruler", "30", "8.75"},
            {null, "30", "Relay", "30", "7.55"},
            {null, "30", "Ruler", "30", "8.75"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 202 - 'between' in RIGHT JOIN ON condition;
        //       '>=all' subquery correlated to tuple-preserving operand in the
        //       WHERE clause
        //       subquery: local pred on both operands, '=' join predicate,
        //       and, left join

        rs = st.executeQuery(
            "select part, p.num, product, pt.NUM, price"
            + " from k55admin.parts p right join k55admin.products pt"
            + " on   (p.num between pt.num and pt.num + 5)"
            + " where pt.price+20 >= all"
            + "       (select a.price"
            + "        from k55admin.parts b left join k55admin.products a"
            + "        on   a.num = b.num and (a.price>8 or b.part "
            + "= 'Steel') and a.num > 20"
            + "        where a.num>=p.num)"
            + "order by 1,2,3,4");

        expColNames = new String [] {"PART", "NUM", "PRODUCT", "NUM", "PRICE"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Blades", "205", "Saw", "205", "18.90"},
            {"Magnets", "10", "Generator", "10", "45.75"},
            {"Paper", "20", "Sander", "20", "35.75"},
            {"Plastic", "30", "Relay", "30", "7.55"},
            {"Plastic", "30", "Ruler", "30", "8.75"},
            {"Steel", "30", "Relay", "30", "7.55"},
            {"Steel", "30", "Ruler", "30", "8.75"},
            {"Wire", "10", "Generator", "10", "45.75"},
            {null, "30", "Relay", "30", "7.55"},
            {null, "30", "Ruler", "30", "8.75"},
            {null, null, "Hammer", "50", "5.75"},
            {null, null, "Screwdriver", "505", "3.70"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 203 - =/like/in in LEFT JOIN ON condition;
        //        '>=all' subquery correlated to null-producing operand in
        //        the WHERE clause
        //        subquery: local '=' pred on null-producing operand OR '='
        //        join predicate, right join

        rs = st.executeQuery(
            "select part, p.num, product, pt.NUM, price"
            + " from k55admin.parts p left join k55admin.products pt"
            + " on (pt.product in ('Bolt','Nuts') or"
            + "    pt.product like 'Power%' or"
            + "    p.num = pt.num)"
            + " where pt.price+20 >= all"
            + "       (select a.price"
            + "        from k55admin.parts b right join k55admin.products a"
            + "        on   b.num = 10 OR a.num = b.num"
            + "        where a.num>=pt.num)"
            + "order by 1,2,3,4");

        expColNames = new String [] {"PART", "NUM", "PRODUCT", "NUM", "PRICE"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Blades", "205", "Saw", "205", "18.90"},
            {"Oil", "160", null, null, null},
            {"Plastic", "30", "Relay", "30", "7.55"},
            {"Plastic", "30", "Ruler", "30", "8.75"},
            {"Steel", "30", "Relay", "30", "7.55"},
            {"Steel", "30", "Ruler", "30", "8.75"},
            {null, "30", "Relay", "30", "7.55"},
            {null, "30", "Ruler", "30", "8.75"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 204 - '='/like/in in RIGHT JOIN ON condition;
        //       '>=all' subquery correlated to null-producing operand in
        //        the WHERE clause
        //        subquery: local pred on both operands, '=' join predicate,
        //        and/or, left join;

        rs = st.executeQuery(
            "select part, p.num, product, pt.NUM, price"
            + " from k55admin.parts p right join k55admin.products pt"
            + " on (pt.product in ('Bolt','Nuts') or"
            + "    pt.product like 'Power%' or"
            + "    p.num = pt.num)"
            + " where pt.price+20 >= all"
            + "       (select a.price"
            + "        from   k55admin.parts b left join "
            + "k55admin.products a"
            + "        on     a.num = b.num and (a.price>8 or "
            + "b.part = 'Steel' or b.num > 20)"
            + "        where a.num>=pt.num)"
            + "order by 1,2,3,4");

        expColNames = new String [] {"PART", "NUM", "PRODUCT", "NUM", "PRICE"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Blades", "205", "Saw", "205", "18.90"},
            {"Magnets", "10", "Generator", "10", "45.75"},
            {"Paper", "20", "Sander", "20", "35.75"},
            {"Plastic", "30", "Relay", "30", "7.55"},
            {"Plastic", "30", "Ruler", "30", "8.75"},
            {"Steel", "30", "Relay", "30", "7.55"},
            {"Steel", "30", "Ruler", "30", "8.75"},
            {"Wire", "10", "Generator", "10", "45.75"},
            {null, "30", "Relay", "30", "7.55"},
            {null, "30", "Ruler", "30", "8.75"},
            {null, null, "Hammer", "50", "5.75"},
            {null, null, "Screwdriver", "505", "3.70"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 205 - 'between' in LEFT JOIN ON condition
        //       IN subquery correlated to tuple-preserving operand in the
        //       WHERE clause
        //       subquery: local '=' pred on tuple-preserving operand OR '='
        //       join predicate, left join;

        rs = st.executeQuery(
            "select part, p.num, product, pt.NUM, price"
            + " from k55admin.parts p left join k55admin.products pt"
            + " on   (p.num between pt.num and pt.num + 5)"
            + " where pt.price IN"
            + "       (select a.price"
            + "        from   k55admin.parts b left join "
            + "k55admin.products a"
            + "        on     b.num = 10 OR a.num = b.num"
            + "        where a.num>=p.num)"
            + "order by 1,2,3,4");

        expColNames = new String [] {"PART", "NUM", "PRODUCT", "NUM", "PRICE"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Blades", "205", "Saw", "205", "18.90"},
            {"Magnets", "10", "Generator", "10", "45.75"},
            {"Paper", "20", "Sander", "20", "35.75"},
            {"Plastic", "30", "Relay", "30", "7.55"},
            {"Plastic", "30", "Ruler", "30", "8.75"},
            {"Steel", "30", "Relay", "30", "7.55"},
            {"Steel", "30", "Ruler", "30", "8.75"},
            {"Wire", "10", "Generator", "10", "45.75"},
            {null, "30", "Relay", "30", "7.55"},
            {null, "30", "Ruler", "30", "8.75"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 206 - 'between' in RIGHT JOIN ON condition
        //       IN subquery correlated to tuple-preserving operand in the
        //       WHERE clause
        //       subquery: local '=' pred on tuple-preserving operand AND '<>'
        //       join predicate, left join;

        rs = st.executeQuery(
            "select part, p.num, product, pt.NUM, price"
            + " from k55admin.parts p right join k55admin.products pt"
            + " on   (p.num between pt.num and pt.num + 5)"
            + " where pt.price IN"
            + "       (select a.price"
            + "        from   k55admin.parts b left join "
            + "k55admin.products a"
            + "        on     b.num = 1000 and a.num <> b.num"
            + "        where a.num>=p.num)"
            + "order by 1,2,3,4");

        expColNames = new String [] {"PART", "NUM", "PRODUCT", "NUM", "PRICE"};
        JDBC.assertColumnNames(rs, expColNames);
        JDBC.assertDrainResults(rs, 0);

        // 207 - '='/like/in in LEFT JOIN ON condition;       IN
        //       subquery correlated to null-producing operand in the
        //       WHERE clause
        //       subquery: '=' join predicate and '2>1', left join

        rs = st.executeQuery(
            "select part, p.num, product, pt.NUM, price"
            + " from k55admin.parts p left join k55admin.products pt"
            + " on (pt.product in ('Bolt','Nuts') or"
            + "    pt.product like 'Power%' or"
            + "    p.num = pt.num)"
            + " where pt.price IN"
            + "       (select a.price"
            + "        from k55admin.parts b left join k55admin.products a"
            + "        on   a.num = b.num and 2>1"
            + "        where a.num>=pt.num)"
            + "order by 1,2,3,4");

        expColNames = new String [] {"PART", "NUM", "PRODUCT", "NUM", "PRICE"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Blades", "205", "Saw", "205", "18.90"},
            {"Magnets", "10", "Generator", "10", "45.75"},
            {"Paper", "20", "Sander", "20", "35.75"},
            {"Plastic", "30", "Relay", "30", "7.55"},
            {"Plastic", "30", "Ruler", "30", "8.75"},
            {"Steel", "30", "Relay", "30", "7.55"},
            {"Steel", "30", "Ruler", "30", "8.75"},
            {"Wire", "10", "Generator", "10", "45.75"},
            {null, "30", "Relay", "30", "7.55"},
            {null, "30", "Ruler", "30", "8.75"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 208 - '='/like/in in RIGHT JOIN ON condition
        //       IN subquery correlated to null-producing operand in the
        //       WHERE clause
        //       subquery: '1=0' in ON condition, left join

        rs = st.executeQuery(
            "select part, p.num, product, pt.NUM, price"
            + " from k55admin.parts p right join k55admin.products pt"
            + " on (pt.product in ('Bolt','Nuts') or"
            + "    pt.product like 'Power%' or"
            + "    p.num = pt.num)"
            + " where pt.price IN"
            + "       (select a.price"
            + "        from k55admin.parts b left join k55admin.products a"
            + "        on 1=0"
            + "        where a.num>=pt.num)"
            + "order by 1,2,3,4");

        expColNames = new String [] {"PART", "NUM", "PRODUCT", "NUM", "PRICE"};
        JDBC.assertColumnNames(rs, expColNames);
        JDBC.assertDrainResults(rs, 0);

    }


    public void part_ii(Statement st) throws SQLException
    {
        ResultSet rs = null;

        String [][] expRS;
        String [] expColNames;

        //
        //------------------------------------------------------------
        // test unit 3. subquery in OUTER JOIN ON clause;
        //------------------------------------------------------------

        // 301 - '='/like/in in RIGHT JOIN ON condition with correlated IN
        //        subquery

        rs = st.executeQuery(
            "select part, p.num, product, pt.NUM, price"
            + " from k55admin.parts p right join k55admin.products pt"
            + " on (pt.product in ('Bolt','Nuts') or"
            + "    pt.product like 'Power%' or"
            + "    p.num = pt.num)"
            + "and pt.price IN"
            + "       (select a.price"
            + "        from k55admin.parts b left join k55admin.products a"
            + "        on 1=0"
            + "        where a.num>=pt.num)"
            + "order by 1,2,3,4");

        expColNames = new String [] {"PART", "NUM", "PRODUCT", "NUM", "PRICE"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {null, null, "Generator", "10", "45.75"},
            {null, null, "Hammer", "50", "5.75"},
            {null, null, "Relay", "30", "7.55"},
            {null, null, "Ruler", "30", "8.75"},
            {null, null, "Sander", "20", "35.75"},
            {null, null, "Saw", "205", "18.90"},
            {null, null, "Screwdriver", "505", "3.70"},
            {null, null, null, "20", null}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 302 - '='/like/in in LEFT JOIN ON condition with
        //       uncorrelated IN subquery

        rs = st.executeQuery(
            "select part, p.num, product, pt.NUM, price"
            + " from k55admin.parts p left join k55admin.products pt"
            + " on (pt.product in ('Bolt','Nuts') or"
            + "    pt.product like 'Power%' or"
            + "    p.num = pt.num)"
            + "and pt.price IN"
            + "       (select a.price"
            + "        from k55admin.parts b left join k55admin.products a"
            + "        on 1=0)"
            + "order by 1,2,3,4");

        expColNames = new String [] {"PART", "NUM", "PRODUCT", "NUM", "PRICE"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Blades", "205", null, null, null},
            {"Magnets", "10", null, null, null},
            {"Oil", "160", null, null, null},
            {"Paper", "20", null, null, null},
            {"Plastic", "30", null, null, null},
            {"Steel", "30", null, null, null},
            {"Wire", "10", null, null, null},
            {null, "30", null, null, null}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 303 - '=' and inlist in RIGHT JOIN ON condition with
        //       correlated exists subquery

        rs = st.executeQuery(
            "select part, p.num, product, pt.NUM, price"
            + " from k55admin.parts p right join k55admin.products pt"
            + " on pt.product in ('Bolt','Nuts') and p.num = pt.num"
            + " and exists"
            + "       (select a.price"
            + "        from k55admin.parts b left join k55admin.products a"
            + "        on 1=0"
            + "        where a.num>=pt.num)"
            + "order by 1,2,3,4");

        expColNames = new String [] {"PART", "NUM", "PRODUCT", "NUM", "PRICE"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {null, null, "Generator", "10", "45.75"},
            {null, null, "Hammer", "50", "5.75"},
            {null, null, "Relay", "30", "7.55"},
            {null, null, "Ruler", "30", "8.75"},
            {null, null, "Sander", "20", "35.75"},
            {null, null, "Saw", "205", "18.90"},
            {null, null, "Screwdriver", "505", "3.70"},
            {null, null, null, "20", null}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 304 - '=' and inlist in LEFT JOIN ON condition with
        //       uncorrelated exists subquery

        rs = st.executeQuery(
            "select part, p.num, product, pt.NUM, price"
            + " from k55admin.parts p left join k55admin.products pt"
            + " on pt.product in ('Bolt','Nuts') and p.num = pt.num"
            + " and exists"
            + "       (select a.price"
            + "        from k55admin.parts b left join k55admin.products a"
            + "        on 1=0)"
            + "order by 1,2,3,4");

        expColNames = new String [] {"PART", "NUM", "PRODUCT", "NUM", "PRICE"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Blades", "205", null, null, null},
            {"Magnets", "10", null, null, null},
            {"Oil", "160", null, null, null},
            {"Paper", "20", null, null, null},
            {"Plastic", "30", null, null, null},
            {"Steel", "30", null, null, null},
            {"Wire", "10", null, null, null},
            {null, "30", null, null, null}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 305 - '='/like/in in RIGHT JOIN ON condition with
        //       correlated '>=ALL' subquery

        rs = st.executeQuery(
            "select part, p.num, product, pt.NUM, price"
            + " from k55admin.parts p right join k55admin.products pt"
            + " on (pt.product in ('Bolt','Nuts') or"
            + "    pt.product like 'Power%' or"
            + "    p.num = pt.num)"
            + "and pt.price >=ALL"
            + "       (select a.price"
            + "        from k55admin.parts b left join k55admin.products a"
            + "        on 1=0"
            + "        where a.num>=pt.num)"
            + "order by 1,2,3,4");

        expColNames = new String [] {"PART", "NUM", "PRODUCT", "NUM", "PRICE"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Blades", "205", "Saw", "205", "18.90"},
            {"Magnets", "10", "Generator", "10", "45.75"},
            {"Paper", "20", "Sander", "20", "35.75"},
            {"Paper", "20", null, "20", null},
            {"Plastic", "30", "Relay", "30", "7.55"},
            {"Plastic", "30", "Ruler", "30", "8.75"},
            {"Steel", "30", "Relay", "30", "7.55"},
            {"Steel", "30", "Ruler", "30", "8.75"},
            {"Wire", "10", "Generator", "10", "45.75"},
            {null, "30", "Relay", "30", "7.55"},
            {null, "30", "Ruler", "30", "8.75"},
            {null, null, "Hammer", "50", "5.75"},
            {null, null, "Screwdriver", "505", "3.70"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 306 - '='/like/in in LEFT JOIN ON condition with
        //       uncorrelated scalar subquery

        rs = st.executeQuery(
            "select part, p.num, product, pt.NUM, price"
            + " from k55admin.parts p left join k55admin.products pt"
            + " on (pt.product in ('Bolt','Nuts') or"
            + "    pt.product like 'Power%' or"
            + "    p.num = pt.num)"
            + "and pt.price ="
            + "       (select max(a.price)"
            + "        from k55admin.parts b left join k55admin.products a"
            + "        on 1=0)"
            + "order by 1,2,3,4");

        expColNames = new String [] {"PART", "NUM", "PRODUCT", "NUM", "PRICE"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Blades", "205", null, null, null},
            {"Magnets", "10", null, null, null},
            {"Oil", "160", null, null, null},
            {"Paper", "20", null, null, null},
            {"Plastic", "30", null, null, null},
            {"Steel", "30", null, null, null},
            {"Wire", "10", null, null, null},
            {null, "30", null, null, null}
        };

        JDBC.assertFullResultSet(rs, expRS);

        //
        // coj202.clp
        //------------------------------------------------------------
        // --     inner join (105)
        //------------------------------------------------------------

        //------------------------------------------------------------
        // test unit 1. plain joins, different relops, conjunction;
        //------------------------------------------------------------

        // 101 - multiple '>=', '<=' in INNER JOIN ON condition

        rs = st.executeQuery(
            "SELECT PART, K55ADMIN.PARTS.NUM, PRODUCT, "
            + "K55ADMIN.PRODUCTS.NUM"
            + "  FROM K55ADMIN.PARTS JOIN K55ADMIN.PRODUCTS"
            + "    ON K55ADMIN.PARTS.NUM >= K55ADMIN.PRODUCTS.NUM "
            + "and K55ADMIN.PARTS.NUM <= K55ADMIN.PRODUCTS.NUM"
            + "    order by 1,2,3,4");

        expColNames = new String [] {"PART", "NUM", "PRODUCT", "NUM"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Blades", "205", "Saw", "205"},
            {"Magnets", "10", "Generator", "10"},
            {"Paper", "20", "Sander", "20"},
            {"Paper", "20", null, "20"},
            {"Plastic", "30", "Relay", "30"},
            {"Plastic", "30", "Ruler", "30"},
            {"Steel", "30", "Relay", "30"},
            {"Steel", "30", "Ruler", "30"},
            {"Wire", "10", "Generator", "10"},
            {null, "30", "Relay", "30"},
            {null, "30", "Ruler", "30"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 102 - 'between' in INNER JOIN ON condition

        rs = st.executeQuery(
            "SELECT PART, K55ADMIN.PARTS.NUM, PRODUCT, "
            + "K55ADMIN.PRODUCTS.NUM"
            + "  FROM K55ADMIN.PARTS JOIN K55ADMIN.PRODUCTS"
            + "    ON K55ADMIN.PARTS.NUM between "
            + "K55ADMIN.PRODUCTS.NUM and K55ADMIN.PRODUCTS.NUM + 50"
            + "    order by 1,2,3,4");

        expColNames = new String [] {"PART", "NUM", "PRODUCT", "NUM"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Blades", "205", "Saw", "205"},
            {"Magnets", "10", "Generator", "10"},
            {"Paper", "20", "Generator", "10"},
            {"Paper", "20", "Sander", "20"},
            {"Paper", "20", null, "20"},
            {"Plastic", "30", "Generator", "10"},
            {"Plastic", "30", "Relay", "30"},
            {"Plastic", "30", "Ruler", "30"},
            {"Plastic", "30", "Sander", "20"},
            {"Plastic", "30", null, "20"},
            {"Steel", "30", "Generator", "10"},
            {"Steel", "30", "Relay", "30"},
            {"Steel", "30", "Ruler", "30"},
            {"Steel", "30", "Sander", "20"},
            {"Steel", "30", null, "20"},
            {"Wire", "10", "Generator", "10"},
            {null, "30", "Generator", "10"},
            {null, "30", "Relay", "30"},
            {null, "30", "Ruler", "30"},
            {null, "30", "Sander", "20"},
            {null, "30", null, "20"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 103 - '>'/'<' in INNER JOIN on condition

        rs = st.executeQuery(
            "SELECT PART, K55ADMIN.PARTS.NUM, PRODUCT, "
            + "K55ADMIN.PRODUCTS.NUM"
            + "  FROM K55ADMIN.PARTS JOIN K55ADMIN.PRODUCTS"
            + "    ON K55ADMIN.PARTS.NUM > K55ADMIN.PRODUCTS.NUM "
            + "and K55ADMIN.PRODUCTS.NUM < K55ADMIN.PARTS.NUM"
            + "    order by 1,2,3,4");

        expColNames = new String [] {"PART", "NUM", "PRODUCT", "NUM"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Blades", "205", "Generator", "10"},
            {"Blades", "205", "Hammer", "50"},
            {"Blades", "205", "Relay", "30"},
            {"Blades", "205", "Ruler", "30"},
            {"Blades", "205", "Sander", "20"},
            {"Blades", "205", null, "20"},
            {"Oil", "160", "Generator", "10"},
            {"Oil", "160", "Hammer", "50"},
            {"Oil", "160", "Relay", "30"},
            {"Oil", "160", "Ruler", "30"},
            {"Oil", "160", "Sander", "20"},
            {"Oil", "160", null, "20"},
            {"Paper", "20", "Generator", "10"},
            {"Plastic", "30", "Generator", "10"},
            {"Plastic", "30", "Sander", "20"},
            {"Plastic", "30", null, "20"},
            {"Steel", "30", "Generator", "10"},
            {"Steel", "30", "Sander", "20"},
            {"Steel", "30", null, "20"},
            {null, "30", "Generator", "10"},
            {null, "30", "Sander", "20"},
            {null, "30", null, "20"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 104 - '<>' in INNER JOIN on condition

        rs = st.executeQuery(
            "SELECT PART, K55ADMIN.PARTS.NUM, PRODUCT, "
            + "K55ADMIN.PRODUCTS.NUM"
            + "  FROM K55ADMIN.PARTS JOIN K55ADMIN.PRODUCTS"
            + "    ON K55ADMIN.PARTS.NUM <> K55ADMIN.PRODUCTS.NUM"
            + "    order by 1,2,3,4");

        expColNames = new String [] {"PART", "NUM", "PRODUCT", "NUM"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Blades", "205", "Generator", "10"},
            {"Blades", "205", "Hammer", "50"},
            {"Blades", "205", "Relay", "30"},
            {"Blades", "205", "Ruler", "30"},
            {"Blades", "205", "Sander", "20"},
            {"Blades", "205", "Screwdriver", "505"},
            {"Blades", "205", null, "20"},
            {"Magnets", "10", "Hammer", "50"},
            {"Magnets", "10", "Relay", "30"},
            {"Magnets", "10", "Ruler", "30"},
            {"Magnets", "10", "Sander", "20"},
            {"Magnets", "10", "Saw", "205"},
            {"Magnets", "10", "Screwdriver", "505"},
            {"Magnets", "10", null, "20"},
            {"Oil", "160", "Generator", "10"},
            {"Oil", "160", "Hammer", "50"},
            {"Oil", "160", "Relay", "30"},
            {"Oil", "160", "Ruler", "30"},
            {"Oil", "160", "Sander", "20"},
            {"Oil", "160", "Saw", "205"},
            {"Oil", "160", "Screwdriver", "505"},
            {"Oil", "160", null, "20"},
            {"Paper", "20", "Generator", "10"},
            {"Paper", "20", "Hammer", "50"},
            {"Paper", "20", "Relay", "30"},
            {"Paper", "20", "Ruler", "30"},
            {"Paper", "20", "Saw", "205"},
            {"Paper", "20", "Screwdriver", "505"},
            {"Plastic", "30", "Generator", "10"},
            {"Plastic", "30", "Hammer", "50"},
            {"Plastic", "30", "Sander", "20"},
            {"Plastic", "30", "Saw", "205"},
            {"Plastic", "30", "Screwdriver", "505"},
            {"Plastic", "30", null, "20"},
            {"Steel", "30", "Generator", "10"},
            {"Steel", "30", "Hammer", "50"},
            {"Steel", "30", "Sander", "20"},
            {"Steel", "30", "Saw", "205"},
            {"Steel", "30", "Screwdriver", "505"},
            {"Steel", "30", null, "20"},
            {"Wire", "10", "Hammer", "50"},
            {"Wire", "10", "Relay", "30"},
            {"Wire", "10", "Ruler", "30"},
            {"Wire", "10", "Sander", "20"},
            {"Wire", "10", "Saw", "205"},
            {"Wire", "10", "Screwdriver", "505"},
            {"Wire", "10", null, "20"},
            {null, "30", "Generator", "10"},
            {null, "30", "Hammer", "50"},
            {null, "30", "Sander", "20"},
            {null, "30", "Saw", "205"},
            {null, "30", "Screwdriver", "505"},
            {null, "30", null, "20"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 105 - multiple '>=', '<=' in LEFT JOIN ON condition

        rs = st.executeQuery(
            "SELECT PART, K55ADMIN.PARTS.NUM, PRODUCT, "
            + "K55ADMIN.PRODUCTS.NUM"
            + "  FROM K55ADMIN.PARTS LEFT JOIN K55ADMIN.PRODUCTS"
            + "    ON K55ADMIN.PARTS.NUM >= K55ADMIN.PRODUCTS.NUM "
            + "and K55ADMIN.PARTS.NUM <= K55ADMIN.PRODUCTS.NUM"
            + "    order by 1,2,3,4");

        expColNames = new String [] {"PART", "NUM", "PRODUCT", "NUM"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Blades", "205", "Saw", "205"},
            {"Magnets", "10", "Generator", "10"},
            {"Oil", "160", null, null},
            {"Paper", "20", "Sander", "20"},
            {"Paper", "20", null, "20"},
            {"Plastic", "30", "Relay", "30"},
            {"Plastic", "30", "Ruler", "30"},
            {"Steel", "30", "Relay", "30"},
            {"Steel", "30", "Ruler", "30"},
            {"Wire", "10", "Generator", "10"},
            {null, "30", "Relay", "30"},
            {null, "30", "Ruler", "30"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 106 - 'between' in LEFT JOIN ON condition

        rs = st.executeQuery(
            "SELECT PART, K55ADMIN.PARTS.NUM, PRODUCT, "
            + "K55ADMIN.PRODUCTS.NUM"
            + "  FROM K55ADMIN.PARTS LEFT JOIN K55ADMIN.PRODUCTS"
            + "    ON K55ADMIN.PARTS.NUM between "
            + "K55ADMIN.PRODUCTS.NUM and K55ADMIN.PRODUCTS.NUM + 50"
            + "    order by 1,2,3,4");

        expColNames = new String [] {"PART", "NUM", "PRODUCT", "NUM"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Blades", "205", "Saw", "205"},
            {"Magnets", "10", "Generator", "10"},
            {"Oil", "160", null, null},
            {"Paper", "20", "Generator", "10"},
            {"Paper", "20", "Sander", "20"},
            {"Paper", "20", null, "20"},
            {"Plastic", "30", "Generator", "10"},
            {"Plastic", "30", "Relay", "30"},
            {"Plastic", "30", "Ruler", "30"},
            {"Plastic", "30", "Sander", "20"},
            {"Plastic", "30", null, "20"},
            {"Steel", "30", "Generator", "10"},
            {"Steel", "30", "Relay", "30"},
            {"Steel", "30", "Ruler", "30"},
            {"Steel", "30", "Sander", "20"},
            {"Steel", "30", null, "20"},
            {"Wire", "10", "Generator", "10"},
            {null, "30", "Generator", "10"},
            {null, "30", "Relay", "30"},
            {null, "30", "Ruler", "30"},
            {null, "30", "Sander", "20"},
            {null, "30", null, "20"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 107 - '>'/'<' in LEFT JOIN on condition

        rs = st.executeQuery(
            "SELECT PART, K55ADMIN.PARTS.NUM, PRODUCT, "
            + "K55ADMIN.PRODUCTS.NUM"
            + "  FROM K55ADMIN.PARTS LEFT JOIN K55ADMIN.PRODUCTS"
            + "    ON K55ADMIN.PARTS.NUM > K55ADMIN.PRODUCTS.NUM "
            + "and K55ADMIN.PRODUCTS.NUM < K55ADMIN.PARTS.NUM"
            + "    order by 1,2,3,4");

        expColNames = new String [] {"PART", "NUM", "PRODUCT", "NUM"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Blades", "205", "Generator", "10"},
            {"Blades", "205", "Hammer", "50"},
            {"Blades", "205", "Relay", "30"},
            {"Blades", "205", "Ruler", "30"},
            {"Blades", "205", "Sander", "20"},
            {"Blades", "205", null, "20"},
            {"Magnets", "10", null, null},
            {"Oil", "160", "Generator", "10"},
            {"Oil", "160", "Hammer", "50"},
            {"Oil", "160", "Relay", "30"},
            {"Oil", "160", "Ruler", "30"},
            {"Oil", "160", "Sander", "20"},
            {"Oil", "160", null, "20"},
            {"Paper", "20", "Generator", "10"},
            {"Plastic", "30", "Generator", "10"},
            {"Plastic", "30", "Sander", "20"},
            {"Plastic", "30", null, "20"},
            {"Steel", "30", "Generator", "10"},
            {"Steel", "30", "Sander", "20"},
            {"Steel", "30", null, "20"},
            {"Wire", "10", null, null},
            {null, "30", "Generator", "10"},
            {null, "30", "Sander", "20"},
            {null, "30", null, "20"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 108 - '<>' in LEFT JOIN on condition

        rs = st.executeQuery(
            "SELECT PART, K55ADMIN.PARTS.NUM, PRODUCT, "
            + "K55ADMIN.PRODUCTS.NUM"
            + "  FROM K55ADMIN.PARTS LEFT JOIN K55ADMIN.PRODUCTS"
            + "    ON K55ADMIN.PARTS.NUM <> K55ADMIN.PRODUCTS.NUM"
            + "    order by 1,2,3,4");

        expColNames = new String [] {"PART", "NUM", "PRODUCT", "NUM"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Blades", "205", "Generator", "10"},
            {"Blades", "205", "Hammer", "50"},
            {"Blades", "205", "Relay", "30"},
            {"Blades", "205", "Ruler", "30"},
            {"Blades", "205", "Sander", "20"},
            {"Blades", "205", "Screwdriver", "505"},
            {"Blades", "205", null, "20"},
            {"Magnets", "10", "Hammer", "50"},
            {"Magnets", "10", "Relay", "30"},
            {"Magnets", "10", "Ruler", "30"},
            {"Magnets", "10", "Sander", "20"},
            {"Magnets", "10", "Saw", "205"},
            {"Magnets", "10", "Screwdriver", "505"},
            {"Magnets", "10", null, "20"},
            {"Oil", "160", "Generator", "10"},
            {"Oil", "160", "Hammer", "50"},
            {"Oil", "160", "Relay", "30"},
            {"Oil", "160", "Ruler", "30"},
            {"Oil", "160", "Sander", "20"},
            {"Oil", "160", "Saw", "205"},
            {"Oil", "160", "Screwdriver", "505"},
            {"Oil", "160", null, "20"},
            {"Paper", "20", "Generator", "10"},
            {"Paper", "20", "Hammer", "50"},
            {"Paper", "20", "Relay", "30"},
            {"Paper", "20", "Ruler", "30"},
            {"Paper", "20", "Saw", "205"},
            {"Paper", "20", "Screwdriver", "505"},
            {"Plastic", "30", "Generator", "10"},
            {"Plastic", "30", "Hammer", "50"},
            {"Plastic", "30", "Sander", "20"},
            {"Plastic", "30", "Saw", "205"},
            {"Plastic", "30", "Screwdriver", "505"},
            {"Plastic", "30", null, "20"},
            {"Steel", "30", "Generator", "10"},
            {"Steel", "30", "Hammer", "50"},
            {"Steel", "30", "Sander", "20"},
            {"Steel", "30", "Saw", "205"},
            {"Steel", "30", "Screwdriver", "505"},
            {"Steel", "30", null, "20"},
            {"Wire", "10", "Hammer", "50"},
            {"Wire", "10", "Relay", "30"},
            {"Wire", "10", "Ruler", "30"},
            {"Wire", "10", "Sander", "20"},
            {"Wire", "10", "Saw", "205"},
            {"Wire", "10", "Screwdriver", "505"},
            {"Wire", "10", null, "20"},
            {null, "30", "Generator", "10"},
            {null, "30", "Hammer", "50"},
            {null, "30", "Sander", "20"},
            {null, "30", "Saw", "205"},
            {null, "30", "Screwdriver", "505"},
            {null, "30", null, "20"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 109 - multiple '>=', '<=' in RIGHT JOIN ON condition

        rs = st.executeQuery(
            "SELECT PART, K55ADMIN.PARTS.NUM, PRODUCT, "
            + "K55ADMIN.PRODUCTS.NUM"
            + "  FROM K55ADMIN.PARTS RIGHT JOIN K55ADMIN.PRODUCTS"
            + "    ON K55ADMIN.PARTS.NUM >= K55ADMIN.PRODUCTS.NUM "
            + "and K55ADMIN.PARTS.NUM <= K55ADMIN.PRODUCTS.NUM"
            + "    order by 1,2,3,4");

        expColNames = new String [] {"PART", "NUM", "PRODUCT", "NUM"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Blades", "205", "Saw", "205"},
            {"Magnets", "10", "Generator", "10"},
            {"Paper", "20", "Sander", "20"},
            {"Paper", "20", null, "20"},
            {"Plastic", "30", "Relay", "30"},
            {"Plastic", "30", "Ruler", "30"},
            {"Steel", "30", "Relay", "30"},
            {"Steel", "30", "Ruler", "30"},
            {"Wire", "10", "Generator", "10"},
            {null, "30", "Relay", "30"},
            {null, "30", "Ruler", "30"},
            {null, null, "Hammer", "50"},
            {null, null, "Screwdriver", "505"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 110 - 'between' in RIGHT JOIN ON condition

        rs = st.executeQuery(
            "SELECT PART, K55ADMIN.PARTS.NUM, PRODUCT, "
            + "K55ADMIN.PRODUCTS.NUM"
            + "  FROM K55ADMIN.PARTS RIGHT JOIN K55ADMIN.PRODUCTS"
            + "    ON K55ADMIN.PARTS.NUM between "
            + "K55ADMIN.PRODUCTS.NUM and K55ADMIN.PRODUCTS.NUM + 50"
            + "    order by 1,2,3,4");

        expColNames = new String [] {"PART", "NUM", "PRODUCT", "NUM"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Blades", "205", "Saw", "205"},
            {"Magnets", "10", "Generator", "10"},
            {"Paper", "20", "Generator", "10"},
            {"Paper", "20", "Sander", "20"},
            {"Paper", "20", null, "20"},
            {"Plastic", "30", "Generator", "10"},
            {"Plastic", "30", "Relay", "30"},
            {"Plastic", "30", "Ruler", "30"},
            {"Plastic", "30", "Sander", "20"},
            {"Plastic", "30", null, "20"},
            {"Steel", "30", "Generator", "10"},
            {"Steel", "30", "Relay", "30"},
            {"Steel", "30", "Ruler", "30"},
            {"Steel", "30", "Sander", "20"},
            {"Steel", "30", null, "20"},
            {"Wire", "10", "Generator", "10"},
            {null, "30", "Generator", "10"},
            {null, "30", "Relay", "30"},
            {null, "30", "Ruler", "30"},
            {null, "30", "Sander", "20"},
            {null, "30", null, "20"},
            {null, null, "Hammer", "50"},
            {null, null, "Screwdriver", "505"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 111 - '>'/'<' in RIGHT JOIN on condition

        rs = st.executeQuery(
            "SELECT PART, K55ADMIN.PARTS.NUM, PRODUCT, "
            + "K55ADMIN.PRODUCTS.NUM"
            + "  FROM K55ADMIN.PARTS RIGHT JOIN K55ADMIN.PRODUCTS"
            + "    ON K55ADMIN.PARTS.NUM > K55ADMIN.PRODUCTS.NUM "
            + "and K55ADMIN.PRODUCTS.NUM < K55ADMIN.PARTS.NUM"
            + "    order by 1,2,3,4");

        expColNames = new String [] {"PART", "NUM", "PRODUCT", "NUM"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Blades", "205", "Generator", "10"},
            {"Blades", "205", "Hammer", "50"},
            {"Blades", "205", "Relay", "30"},
            {"Blades", "205", "Ruler", "30"},
            {"Blades", "205", "Sander", "20"},
            {"Blades", "205", null, "20"},
            {"Oil", "160", "Generator", "10"},
            {"Oil", "160", "Hammer", "50"},
            {"Oil", "160", "Relay", "30"},
            {"Oil", "160", "Ruler", "30"},
            {"Oil", "160", "Sander", "20"},
            {"Oil", "160", null, "20"},
            {"Paper", "20", "Generator", "10"},
            {"Plastic", "30", "Generator", "10"},
            {"Plastic", "30", "Sander", "20"},
            {"Plastic", "30", null, "20"},
            {"Steel", "30", "Generator", "10"},
            {"Steel", "30", "Sander", "20"},
            {"Steel", "30", null, "20"},
            {null, "30", "Generator", "10"},
            {null, "30", "Sander", "20"},
            {null, "30", null, "20"},
            {null, null, "Saw", "205"},
            {null, null, "Screwdriver", "505"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 112 - '<>' in RIGHT JOIN on condition

        rs = st.executeQuery(
            "SELECT PART, K55ADMIN.PARTS.NUM, PRODUCT, "
            + "K55ADMIN.PRODUCTS.NUM"
            + "  FROM K55ADMIN.PARTS RIGHT JOIN K55ADMIN.PRODUCTS"
            + "    ON K55ADMIN.PARTS.NUM <> K55ADMIN.PRODUCTS.NUM"
            + "    order by 1,2,3,4");

        expColNames = new String [] {"PART", "NUM", "PRODUCT", "NUM"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Blades", "205", "Generator", "10"},
            {"Blades", "205", "Hammer", "50"},
            {"Blades", "205", "Relay", "30"},
            {"Blades", "205", "Ruler", "30"},
            {"Blades", "205", "Sander", "20"},
            {"Blades", "205", "Screwdriver", "505"},
            {"Blades", "205", null, "20"},
            {"Magnets", "10", "Hammer", "50"},
            {"Magnets", "10", "Relay", "30"},
            {"Magnets", "10", "Ruler", "30"},
            {"Magnets", "10", "Sander", "20"},
            {"Magnets", "10", "Saw", "205"},
            {"Magnets", "10", "Screwdriver", "505"},
            {"Magnets", "10", null, "20"},
            {"Oil", "160", "Generator", "10"},
            {"Oil", "160", "Hammer", "50"},
            {"Oil", "160", "Relay", "30"},
            {"Oil", "160", "Ruler", "30"},
            {"Oil", "160", "Sander", "20"},
            {"Oil", "160", "Saw", "205"},
            {"Oil", "160", "Screwdriver", "505"},
            {"Oil", "160", null, "20"},
            {"Paper", "20", "Generator", "10"},
            {"Paper", "20", "Hammer", "50"},
            {"Paper", "20", "Relay", "30"},
            {"Paper", "20", "Ruler", "30"},
            {"Paper", "20", "Saw", "205"},
            {"Paper", "20", "Screwdriver", "505"},
            {"Plastic", "30", "Generator", "10"},
            {"Plastic", "30", "Hammer", "50"},
            {"Plastic", "30", "Sander", "20"},
            {"Plastic", "30", "Saw", "205"},
            {"Plastic", "30", "Screwdriver", "505"},
            {"Plastic", "30", null, "20"},
            {"Steel", "30", "Generator", "10"},
            {"Steel", "30", "Hammer", "50"},
            {"Steel", "30", "Sander", "20"},
            {"Steel", "30", "Saw", "205"},
            {"Steel", "30", "Screwdriver", "505"},
            {"Steel", "30", null, "20"},
            {"Wire", "10", "Hammer", "50"},
            {"Wire", "10", "Relay", "30"},
            {"Wire", "10", "Ruler", "30"},
            {"Wire", "10", "Sander", "20"},
            {"Wire", "10", "Saw", "205"},
            {"Wire", "10", "Screwdriver", "505"},
            {"Wire", "10", null, "20"},
            {null, "30", "Generator", "10"},
            {null, "30", "Hammer", "50"},
            {null, "30", "Sander", "20"},
            {null, "30", "Saw", "205"},
            {null, "30", "Screwdriver", "505"},
            {null, "30", null, "20"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        //
        //------------------------------------------------------------
        // test unit 2. plain joins, different relops, and/or/not;
        //------------------------------------------------------------

        // 201 - '>='/'<='/OR in INNER JOIN ON condition

        rs = st.executeQuery(
            "SELECT PART, K55ADMIN.PARTS.NUM, PRODUCT, "
            + "K55ADMIN.PRODUCTS.NUM"
            + "  FROM K55ADMIN.PARTS JOIN K55ADMIN.PRODUCTS"
            + "    ON K55ADMIN.PARTS.NUM >= K55ADMIN.PRODUCTS.NUM "
            + "OR K55ADMIN.PARTS.NUM <= K55ADMIN.PRODUCTS.NUM"
            + "    order by 1,2,3,4");

        expColNames = new String [] {"PART", "NUM", "PRODUCT", "NUM"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Blades", "205", "Generator", "10"},
            {"Blades", "205", "Hammer", "50"},
            {"Blades", "205", "Relay", "30"},
            {"Blades", "205", "Ruler", "30"},
            {"Blades", "205", "Sander", "20"},
            {"Blades", "205", "Saw", "205"},
            {"Blades", "205", "Screwdriver", "505"},
            {"Blades", "205", null, "20"},
            {"Magnets", "10", "Generator", "10"},
            {"Magnets", "10", "Hammer", "50"},
            {"Magnets", "10", "Relay", "30"},
            {"Magnets", "10", "Ruler", "30"},
            {"Magnets", "10", "Sander", "20"},
            {"Magnets", "10", "Saw", "205"},
            {"Magnets", "10", "Screwdriver", "505"},
            {"Magnets", "10", null, "20"},
            {"Oil", "160", "Generator", "10"},
            {"Oil", "160", "Hammer", "50"},
            {"Oil", "160", "Relay", "30"},
            {"Oil", "160", "Ruler", "30"},
            {"Oil", "160", "Sander", "20"},
            {"Oil", "160", "Saw", "205"},
            {"Oil", "160", "Screwdriver", "505"},
            {"Oil", "160", null, "20"},
            {"Paper", "20", "Generator", "10"},
            {"Paper", "20", "Hammer", "50"},
            {"Paper", "20", "Relay", "30"},
            {"Paper", "20", "Ruler", "30"},
            {"Paper", "20", "Sander", "20"},
            {"Paper", "20", "Saw", "205"},
            {"Paper", "20", "Screwdriver", "505"},
            {"Paper", "20", null, "20"},
            {"Plastic", "30", "Generator", "10"},
            {"Plastic", "30", "Hammer", "50"},
            {"Plastic", "30", "Relay", "30"},
            {"Plastic", "30", "Ruler", "30"},
            {"Plastic", "30", "Sander", "20"},
            {"Plastic", "30", "Saw", "205"},
            {"Plastic", "30", "Screwdriver", "505"},
            {"Plastic", "30", null, "20"},
            {"Steel", "30", "Generator", "10"},
            {"Steel", "30", "Hammer", "50"},
            {"Steel", "30", "Relay", "30"},
            {"Steel", "30", "Ruler", "30"},
            {"Steel", "30", "Sander", "20"},
            {"Steel", "30", "Saw", "205"},
            {"Steel", "30", "Screwdriver", "505"},
            {"Steel", "30", null, "20"},
            {"Wire", "10", "Generator", "10"},
            {"Wire", "10", "Hammer", "50"},
            {"Wire", "10", "Relay", "30"},
            {"Wire", "10", "Ruler", "30"},
            {"Wire", "10", "Sander", "20"},
            {"Wire", "10", "Saw", "205"},
            {"Wire", "10", "Screwdriver", "505"},
            {"Wire", "10", null, "20"},
            {null, "30", "Generator", "10"},
            {null, "30", "Hammer", "50"},
            {null, "30", "Relay", "30"},
            {null, "30", "Ruler", "30"},
            {null, "30", "Sander", "20"},
            {null, "30", "Saw", "205"},
            {null, "30", "Screwdriver", "505"},
            {null, "30", null, "20"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 202 - 'not between' in INNER JOIN ON condition

        rs = st.executeQuery(
            "SELECT PART, K55ADMIN.PARTS.NUM, PRODUCT, "
            + "K55ADMIN.PRODUCTS.NUM"
            + "  FROM K55ADMIN.PARTS JOIN K55ADMIN.PRODUCTS"
            + "    ON NOT(K55ADMIN.PARTS.NUM between "
            + "K55ADMIN.PRODUCTS.NUM AND K55ADMIN.PRODUCTS.NUM + 50)"
            + "    order by 1,2,3,4");

        expColNames = new String [] {"PART", "NUM", "PRODUCT", "NUM"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Blades", "205", "Generator", "10"},
            {"Blades", "205", "Hammer", "50"},
            {"Blades", "205", "Relay", "30"},
            {"Blades", "205", "Ruler", "30"},
            {"Blades", "205", "Sander", "20"},
            {"Blades", "205", "Screwdriver", "505"},
            {"Blades", "205", null, "20"},
            {"Magnets", "10", "Hammer", "50"},
            {"Magnets", "10", "Relay", "30"},
            {"Magnets", "10", "Ruler", "30"},
            {"Magnets", "10", "Sander", "20"},
            {"Magnets", "10", "Saw", "205"},
            {"Magnets", "10", "Screwdriver", "505"},
            {"Magnets", "10", null, "20"},
            {"Oil", "160", "Generator", "10"},
            {"Oil", "160", "Hammer", "50"},
            {"Oil", "160", "Relay", "30"},
            {"Oil", "160", "Ruler", "30"},
            {"Oil", "160", "Sander", "20"},
            {"Oil", "160", "Saw", "205"},
            {"Oil", "160", "Screwdriver", "505"},
            {"Oil", "160", null, "20"},
            {"Paper", "20", "Hammer", "50"},
            {"Paper", "20", "Relay", "30"},
            {"Paper", "20", "Ruler", "30"},
            {"Paper", "20", "Saw", "205"},
            {"Paper", "20", "Screwdriver", "505"},
            {"Plastic", "30", "Hammer", "50"},
            {"Plastic", "30", "Saw", "205"},
            {"Plastic", "30", "Screwdriver", "505"},
            {"Steel", "30", "Hammer", "50"},
            {"Steel", "30", "Saw", "205"},
            {"Steel", "30", "Screwdriver", "505"},
            {"Wire", "10", "Hammer", "50"},
            {"Wire", "10", "Relay", "30"},
            {"Wire", "10", "Ruler", "30"},
            {"Wire", "10", "Sander", "20"},
            {"Wire", "10", "Saw", "205"},
            {"Wire", "10", "Screwdriver", "505"},
            {"Wire", "10", null, "20"},
            {null, "30", "Hammer", "50"},
            {null, "30", "Saw", "205"},
            {null, "30", "Screwdriver", "505"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 203 - '>'/'<'/OR in INNER JOIN on condition

        rs = st.executeQuery(
            "SELECT PART, K55ADMIN.PARTS.NUM, PRODUCT, "
            + "K55ADMIN.PRODUCTS.NUM"
            + "  FROM K55ADMIN.PARTS JOIN K55ADMIN.PRODUCTS"
            + "    ON K55ADMIN.PARTS.NUM > K55ADMIN.PRODUCTS.NUM "
            + "OR K55ADMIN.PRODUCTS.NUM < K55ADMIN.PARTS.NUM"
            + "    order by 1,2,3,4");

        expColNames = new String [] {"PART", "NUM", "PRODUCT", "NUM"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Blades", "205", "Generator", "10"},
            {"Blades", "205", "Hammer", "50"},
            {"Blades", "205", "Relay", "30"},
            {"Blades", "205", "Ruler", "30"},
            {"Blades", "205", "Sander", "20"},
            {"Blades", "205", null, "20"},
            {"Oil", "160", "Generator", "10"},
            {"Oil", "160", "Hammer", "50"},
            {"Oil", "160", "Relay", "30"},
            {"Oil", "160", "Ruler", "30"},
            {"Oil", "160", "Sander", "20"},
            {"Oil", "160", null, "20"},
            {"Paper", "20", "Generator", "10"},
            {"Plastic", "30", "Generator", "10"},
            {"Plastic", "30", "Sander", "20"},
            {"Plastic", "30", null, "20"},
            {"Steel", "30", "Generator", "10"},
            {"Steel", "30", "Sander", "20"},
            {"Steel", "30", null, "20"},
            {null, "30", "Generator", "10"},
            {null, "30", "Sander", "20"},
            {null, "30", null, "20"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 204 - not/'<>' in INNER JOIN on condition

        rs = st.executeQuery(
            "SELECT PART, K55ADMIN.PARTS.NUM, PRODUCT, "
            + "K55ADMIN.PRODUCTS.NUM"
            + "  FROM K55ADMIN.PARTS JOIN K55ADMIN.PRODUCTS"
            + "    ON NOT( K55ADMIN.PARTS.NUM <> K55ADMIN.PRODUCTS.NUM )"
            + "    order by 1,2,3,4");

        expColNames = new String [] {"PART", "NUM", "PRODUCT", "NUM"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Blades", "205", "Saw", "205"},
            {"Magnets", "10", "Generator", "10"},
            {"Paper", "20", "Sander", "20"},
            {"Paper", "20", null, "20"},
            {"Plastic", "30", "Relay", "30"},
            {"Plastic", "30", "Ruler", "30"},
            {"Steel", "30", "Relay", "30"},
            {"Steel", "30", "Ruler", "30"},
            {"Wire", "10", "Generator", "10"},
            {null, "30", "Relay", "30"},
            {null, "30", "Ruler", "30"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 205 - '>='/'<='/or in LEFT JOIN ON condition

        rs = st.executeQuery(
            "SELECT PART, K55ADMIN.PARTS.NUM, PRODUCT, "
            + "K55ADMIN.PRODUCTS.NUM"
            + "  FROM K55ADMIN.PARTS LEFT JOIN K55ADMIN.PRODUCTS"
            + "    ON K55ADMIN.PARTS.NUM >= K55ADMIN.PRODUCTS.NUM "
            + "OR K55ADMIN.PRODUCTS.NUM <= K55ADMIN.PARTS.NUM"
            + "    order by 1,2,3,4");

        expColNames = new String [] {"PART", "NUM", "PRODUCT", "NUM"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Blades", "205", "Generator", "10"},
            {"Blades", "205", "Hammer", "50"},
            {"Blades", "205", "Relay", "30"},
            {"Blades", "205", "Ruler", "30"},
            {"Blades", "205", "Sander", "20"},
            {"Blades", "205", "Saw", "205"},
            {"Blades", "205", null, "20"},
            {"Magnets", "10", "Generator", "10"},
            {"Oil", "160", "Generator", "10"},
            {"Oil", "160", "Hammer", "50"},
            {"Oil", "160", "Relay", "30"},
            {"Oil", "160", "Ruler", "30"},
            {"Oil", "160", "Sander", "20"},
            {"Oil", "160", null, "20"},
            {"Paper", "20", "Generator", "10"},
            {"Paper", "20", "Sander", "20"},
            {"Paper", "20", null, "20"},
            {"Plastic", "30", "Generator", "10"},
            {"Plastic", "30", "Relay", "30"},
            {"Plastic", "30", "Ruler", "30"},
            {"Plastic", "30", "Sander", "20"},
            {"Plastic", "30", null, "20"},
            {"Steel", "30", "Generator", "10"},
            {"Steel", "30", "Relay", "30"},
            {"Steel", "30", "Ruler", "30"},
            {"Steel", "30", "Sander", "20"},
            {"Steel", "30", null, "20"},
            {"Wire", "10", "Generator", "10"},
            {null, "30", "Generator", "10"},
            {null, "30", "Relay", "30"},
            {null, "30", "Ruler", "30"},
            {null, "30", "Sander", "20"},
            {null, "30", null, "20"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 206 - 'not between' in LEFT JOIN ON condition

        rs = st.executeQuery(
            "SELECT PART, K55ADMIN.PARTS.NUM, PRODUCT, "
            + "K55ADMIN.PRODUCTS.NUM"
            + "  FROM K55ADMIN.PARTS LEFT JOIN K55ADMIN.PRODUCTS"
            + "    ON not (K55ADMIN.PARTS.NUM between "
            + "K55ADMIN.PRODUCTS.NUM and K55ADMIN.PRODUCTS.NUM + 50)"
            + "    order by 1,2,3,4");

        expColNames = new String [] {"PART", "NUM", "PRODUCT", "NUM"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Blades", "205", "Generator", "10"},
            {"Blades", "205", "Hammer", "50"},
            {"Blades", "205", "Relay", "30"},
            {"Blades", "205", "Ruler", "30"},
            {"Blades", "205", "Sander", "20"},
            {"Blades", "205", "Screwdriver", "505"},
            {"Blades", "205", null, "20"},
            {"Magnets", "10", "Hammer", "50"},
            {"Magnets", "10", "Relay", "30"},
            {"Magnets", "10", "Ruler", "30"},
            {"Magnets", "10", "Sander", "20"},
            {"Magnets", "10", "Saw", "205"},
            {"Magnets", "10", "Screwdriver", "505"},
            {"Magnets", "10", null, "20"},
            {"Oil", "160", "Generator", "10"},
            {"Oil", "160", "Hammer", "50"},
            {"Oil", "160", "Relay", "30"},
            {"Oil", "160", "Ruler", "30"},
            {"Oil", "160", "Sander", "20"},
            {"Oil", "160", "Saw", "205"},
            {"Oil", "160", "Screwdriver", "505"},
            {"Oil", "160", null, "20"},
            {"Paper", "20", "Hammer", "50"},
            {"Paper", "20", "Relay", "30"},
            {"Paper", "20", "Ruler", "30"},
            {"Paper", "20", "Saw", "205"},
            {"Paper", "20", "Screwdriver", "505"},
            {"Plastic", "30", "Hammer", "50"},
            {"Plastic", "30", "Saw", "205"},
            {"Plastic", "30", "Screwdriver", "505"},
            {"Steel", "30", "Hammer", "50"},
            {"Steel", "30", "Saw", "205"},
            {"Steel", "30", "Screwdriver", "505"},
            {"Wire", "10", "Hammer", "50"},
            {"Wire", "10", "Relay", "30"},
            {"Wire", "10", "Ruler", "30"},
            {"Wire", "10", "Sander", "20"},
            {"Wire", "10", "Saw", "205"},
            {"Wire", "10", "Screwdriver", "505"},
            {"Wire", "10", null, "20"},
            {null, "30", "Hammer", "50"},
            {null, "30", "Saw", "205"},
            {null, "30", "Screwdriver", "505"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 207 - '>'/'<'/or in LEFT JOIN on condition

        rs = st.executeQuery(
            "SELECT PART, K55ADMIN.PARTS.NUM, PRODUCT, "
            + "K55ADMIN.PRODUCTS.NUM"
            + "  FROM K55ADMIN.PARTS LEFT JOIN K55ADMIN.PRODUCTS"
            + "    ON K55ADMIN.PARTS.NUM > K55ADMIN.PRODUCTS.NUM "
            + "OR K55ADMIN.PRODUCTS.NUM < K55ADMIN.PARTS.NUM"
            + "    order by 1,2,3,4");

        expColNames = new String [] {"PART", "NUM", "PRODUCT", "NUM"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Blades", "205", "Generator", "10"},
            {"Blades", "205", "Hammer", "50"},
            {"Blades", "205", "Relay", "30"},
            {"Blades", "205", "Ruler", "30"},
            {"Blades", "205", "Sander", "20"},
            {"Blades", "205", null, "20"},
            {"Magnets", "10", null, null},
            {"Oil", "160", "Generator", "10"},
            {"Oil", "160", "Hammer", "50"},
            {"Oil", "160", "Relay", "30"},
            {"Oil", "160", "Ruler", "30"},
            {"Oil", "160", "Sander", "20"},
            {"Oil", "160", null, "20"},
            {"Paper", "20", "Generator", "10"},
            {"Plastic", "30", "Generator", "10"},
            {"Plastic", "30", "Sander", "20"},
            {"Plastic", "30", null, "20"},
            {"Steel", "30", "Generator", "10"},
            {"Steel", "30", "Sander", "20"},
            {"Steel", "30", null, "20"},
            {"Wire", "10", null, null},
            {null, "30", "Generator", "10"},
            {null, "30", "Sander", "20"},
            {null, "30", null, "20"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 208 - not/'<>' in LEFT JOIN on condition

        rs = st.executeQuery(
            "SELECT PART, K55ADMIN.PARTS.NUM, PRODUCT, "
            + "K55ADMIN.PRODUCTS.NUM"
            + "  FROM K55ADMIN.PARTS LEFT JOIN K55ADMIN.PRODUCTS"
            + "    ON NOT(K55ADMIN.PARTS.NUM <> K55ADMIN.PRODUCTS.NUM)"
            + "    order by 1,2,3,4");

        expColNames = new String [] {"PART", "NUM", "PRODUCT", "NUM"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Blades", "205", "Saw", "205"},
            {"Magnets", "10", "Generator", "10"},
            {"Oil", "160", null, null},
            {"Paper", "20", "Sander", "20"},
            {"Paper", "20", null, "20"},
            {"Plastic", "30", "Relay", "30"},
            {"Plastic", "30", "Ruler", "30"},
            {"Steel", "30", "Relay", "30"},
            {"Steel", "30", "Ruler", "30"},
            {"Wire", "10", "Generator", "10"},
            {null, "30", "Relay", "30"},
            {null, "30", "Ruler", "30"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 209 - '>='/'<='/OR in RIGHT JOIN ON condition

        rs = st.executeQuery(
            "SELECT PART, K55ADMIN.PARTS.NUM, PRODUCT, "
            + "K55ADMIN.PRODUCTS.NUM"
            + "  FROM K55ADMIN.PARTS RIGHT JOIN K55ADMIN.PRODUCTS"
            + "    ON K55ADMIN.PARTS.NUM >= K55ADMIN.PRODUCTS.NUM "
            + "or K55ADMIN.PRODUCTS.NUM <= K55ADMIN.PARTS.NUM"
            + "    order by 1,2,3,4");

        expColNames = new String [] {"PART", "NUM", "PRODUCT", "NUM"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Blades", "205", "Generator", "10"},
            {"Blades", "205", "Hammer", "50"},
            {"Blades", "205", "Relay", "30"},
            {"Blades", "205", "Ruler", "30"},
            {"Blades", "205", "Sander", "20"},
            {"Blades", "205", "Saw", "205"},
            {"Blades", "205", null, "20"},
            {"Magnets", "10", "Generator", "10"},
            {"Oil", "160", "Generator", "10"},
            {"Oil", "160", "Hammer", "50"},
            {"Oil", "160", "Relay", "30"},
            {"Oil", "160", "Ruler", "30"},
            {"Oil", "160", "Sander", "20"},
            {"Oil", "160", null, "20"},
            {"Paper", "20", "Generator", "10"},
            {"Paper", "20", "Sander", "20"},
            {"Paper", "20", null, "20"},
            {"Plastic", "30", "Generator", "10"},
            {"Plastic", "30", "Relay", "30"},
            {"Plastic", "30", "Ruler", "30"},
            {"Plastic", "30", "Sander", "20"},
            {"Plastic", "30", null, "20"},
            {"Steel", "30", "Generator", "10"},
            {"Steel", "30", "Relay", "30"},
            {"Steel", "30", "Ruler", "30"},
            {"Steel", "30", "Sander", "20"},
            {"Steel", "30", null, "20"},
            {"Wire", "10", "Generator", "10"},
            {null, "30", "Generator", "10"},
            {null, "30", "Relay", "30"},
            {null, "30", "Ruler", "30"},
            {null, "30", "Sander", "20"},
            {null, "30", null, "20"},
            {null, null, "Screwdriver", "505"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 210 - 'not between' in RIGHT JOIN ON condition

        rs = st.executeQuery(
            "SELECT PART, K55ADMIN.PARTS.NUM, PRODUCT, "
            + "K55ADMIN.PRODUCTS.NUM"
            + "  FROM K55ADMIN.PARTS RIGHT JOIN K55ADMIN.PRODUCTS"
            + "    ON not(K55ADMIN.PARTS.NUM between "
            + "K55ADMIN.PRODUCTS.NUM and K55ADMIN.PRODUCTS.NUM + 50)"
            + "    order by 1,2,3,4");

        expColNames = new String [] {"PART", "NUM", "PRODUCT", "NUM"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Blades", "205", "Generator", "10"},
            {"Blades", "205", "Hammer", "50"},
            {"Blades", "205", "Relay", "30"},
            {"Blades", "205", "Ruler", "30"},
            {"Blades", "205", "Sander", "20"},
            {"Blades", "205", "Screwdriver", "505"},
            {"Blades", "205", null, "20"},
            {"Magnets", "10", "Hammer", "50"},
            {"Magnets", "10", "Relay", "30"},
            {"Magnets", "10", "Ruler", "30"},
            {"Magnets", "10", "Sander", "20"},
            {"Magnets", "10", "Saw", "205"},
            {"Magnets", "10", "Screwdriver", "505"},
            {"Magnets", "10", null, "20"},
            {"Oil", "160", "Generator", "10"},
            {"Oil", "160", "Hammer", "50"},
            {"Oil", "160", "Relay", "30"},
            {"Oil", "160", "Ruler", "30"},
            {"Oil", "160", "Sander", "20"},
            {"Oil", "160", "Saw", "205"},
            {"Oil", "160", "Screwdriver", "505"},
            {"Oil", "160", null, "20"},
            {"Paper", "20", "Hammer", "50"},
            {"Paper", "20", "Relay", "30"},
            {"Paper", "20", "Ruler", "30"},
            {"Paper", "20", "Saw", "205"},
            {"Paper", "20", "Screwdriver", "505"},
            {"Plastic", "30", "Hammer", "50"},
            {"Plastic", "30", "Saw", "205"},
            {"Plastic", "30", "Screwdriver", "505"},
            {"Steel", "30", "Hammer", "50"},
            {"Steel", "30", "Saw", "205"},
            {"Steel", "30", "Screwdriver", "505"},
            {"Wire", "10", "Hammer", "50"},
            {"Wire", "10", "Relay", "30"},
            {"Wire", "10", "Ruler", "30"},
            {"Wire", "10", "Sander", "20"},
            {"Wire", "10", "Saw", "205"},
            {"Wire", "10", "Screwdriver", "505"},
            {"Wire", "10", null, "20"},
            {null, "30", "Hammer", "50"},
            {null, "30", "Saw", "205"},
            {null, "30", "Screwdriver", "505"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 211 - '>'/'<'/or in RIGHT JOIN on condition

        rs = st.executeQuery(
            "SELECT PART, K55ADMIN.PARTS.NUM, PRODUCT, "
            + "K55ADMIN.PRODUCTS.NUM"
            + "  FROM K55ADMIN.PARTS RIGHT JOIN K55ADMIN.PRODUCTS"
            + "    ON K55ADMIN.PARTS.NUM > K55ADMIN.PRODUCTS.NUM "
            + "or K55ADMIN.PRODUCTS.NUM < K55ADMIN.PARTS.NUM"
            + "    order by 1,2,3,4");

        expColNames = new String [] {"PART", "NUM", "PRODUCT", "NUM"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Blades", "205", "Generator", "10"},
            {"Blades", "205", "Hammer", "50"},
            {"Blades", "205", "Relay", "30"},
            {"Blades", "205", "Ruler", "30"},
            {"Blades", "205", "Sander", "20"},
            {"Blades", "205", null, "20"},
            {"Oil", "160", "Generator", "10"},
            {"Oil", "160", "Hammer", "50"},
            {"Oil", "160", "Relay", "30"},
            {"Oil", "160", "Ruler", "30"},
            {"Oil", "160", "Sander", "20"},
            {"Oil", "160", null, "20"},
            {"Paper", "20", "Generator", "10"},
            {"Plastic", "30", "Generator", "10"},
            {"Plastic", "30", "Sander", "20"},
            {"Plastic", "30", null, "20"},
            {"Steel", "30", "Generator", "10"},
            {"Steel", "30", "Sander", "20"},
            {"Steel", "30", null, "20"},
            {null, "30", "Generator", "10"},
            {null, "30", "Sander", "20"},
            {null, "30", null, "20"},
            {null, null, "Saw", "205"},
            {null, null, "Screwdriver", "505"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 212 - not/'<>' in RIGHT JOIN on condition

        rs = st.executeQuery(
            "SELECT PART, K55ADMIN.PARTS.NUM, PRODUCT, "
            + "K55ADMIN.PRODUCTS.NUM"
            + "  FROM K55ADMIN.PARTS RIGHT JOIN K55ADMIN.PRODUCTS"
            + "    ON not(K55ADMIN.PARTS.NUM <> K55ADMIN.PRODUCTS.NUM)"
            + "    order by 1,2,3,4");

        expColNames = new String [] {"PART", "NUM", "PRODUCT", "NUM"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Blades", "205", "Saw", "205"},
            {"Magnets", "10", "Generator", "10"},
            {"Paper", "20", "Sander", "20"},
            {"Paper", "20", null, "20"},
            {"Plastic", "30", "Relay", "30"},
            {"Plastic", "30", "Ruler", "30"},
            {"Steel", "30", "Relay", "30"},
            {"Steel", "30", "Ruler", "30"},
            {"Wire", "10", "Generator", "10"},
            {null, "30", "Relay", "30"},
            {null, "30", "Ruler", "30"},
            {null, null, "Hammer", "50"},
            {null, null, "Screwdriver", "505"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        //
        //------------------------------------------------------------
        // test unit 3. plain joins, like/in/is-null/is-not-null;
        //------------------------------------------------------------

        // 301 - '<>'/'=' in LEFT JOIN ON condition

        rs = st.executeQuery(
            "SELECT PART, K55ADMIN.PARTS.NUM, PRODUCT, "
            + "K55ADMIN.PRODUCTS.NUM"
            + "  FROM K55ADMIN.PARTS LEFT JOIN K55ADMIN.PRODUCTS"
            + "    ON ( K55ADMIN.PARTS.NUM = K55ADMIN.PRODUCTS.NUM or"
            + "         K55ADMIN.PARTS.NUM <> K55ADMIN.PRODUCTS.NUM)"
            + "    order by 1,2,3,4");

        expColNames = new String [] {"PART", "NUM", "PRODUCT", "NUM"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Blades", "205", "Generator", "10"},
            {"Blades", "205", "Hammer", "50"},
            {"Blades", "205", "Relay", "30"},
            {"Blades", "205", "Ruler", "30"},
            {"Blades", "205", "Sander", "20"},
            {"Blades", "205", "Saw", "205"},
            {"Blades", "205", "Screwdriver", "505"},
            {"Blades", "205", null, "20"},
            {"Magnets", "10", "Generator", "10"},
            {"Magnets", "10", "Hammer", "50"},
            {"Magnets", "10", "Relay", "30"},
            {"Magnets", "10", "Ruler", "30"},
            {"Magnets", "10", "Sander", "20"},
            {"Magnets", "10", "Saw", "205"},
            {"Magnets", "10", "Screwdriver", "505"},
            {"Magnets", "10", null, "20"},
            {"Oil", "160", "Generator", "10"},
            {"Oil", "160", "Hammer", "50"},
            {"Oil", "160", "Relay", "30"},
            {"Oil", "160", "Ruler", "30"},
            {"Oil", "160", "Sander", "20"},
            {"Oil", "160", "Saw", "205"},
            {"Oil", "160", "Screwdriver", "505"},
            {"Oil", "160", null, "20"},
            {"Paper", "20", "Generator", "10"},
            {"Paper", "20", "Hammer", "50"},
            {"Paper", "20", "Relay", "30"},
            {"Paper", "20", "Ruler", "30"},
            {"Paper", "20", "Sander", "20"},
            {"Paper", "20", "Saw", "205"},
            {"Paper", "20", "Screwdriver", "505"},
            {"Paper", "20", null, "20"},
            {"Plastic", "30", "Generator", "10"},
            {"Plastic", "30", "Hammer", "50"},
            {"Plastic", "30", "Relay", "30"},
            {"Plastic", "30", "Ruler", "30"},
            {"Plastic", "30", "Sander", "20"},
            {"Plastic", "30", "Saw", "205"},
            {"Plastic", "30", "Screwdriver", "505"},
            {"Plastic", "30", null, "20"},
            {"Steel", "30", "Generator", "10"},
            {"Steel", "30", "Hammer", "50"},
            {"Steel", "30", "Relay", "30"},
            {"Steel", "30", "Ruler", "30"},
            {"Steel", "30", "Sander", "20"},
            {"Steel", "30", "Saw", "205"},
            {"Steel", "30", "Screwdriver", "505"},
            {"Steel", "30", null, "20"},
            {"Wire", "10", "Generator", "10"},
            {"Wire", "10", "Hammer", "50"},
            {"Wire", "10", "Relay", "30"},
            {"Wire", "10", "Ruler", "30"},
            {"Wire", "10", "Sander", "20"},
            {"Wire", "10", "Saw", "205"},
            {"Wire", "10", "Screwdriver", "505"},
            {"Wire", "10", null, "20"},
            {null, "30", "Generator", "10"},
            {null, "30", "Hammer", "50"},
            {null, "30", "Relay", "30"},
            {null, "30", "Ruler", "30"},
            {null, "30", "Sander", "20"},
            {null, "30", "Saw", "205"},
            {null, "30", "Screwdriver", "505"},
            {null, "30", null, "20"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 302 - '='/like in LEFT JOIN ON condition

        rs = st.executeQuery(
            "SELECT PART, K55ADMIN.PARTS.NUM, PRODUCT, "
            + "K55ADMIN.PRODUCTS.NUM"
            + "  FROM K55ADMIN.PARTS LEFT JOIN K55ADMIN.PRODUCTS"
            + "    ON ( K55ADMIN.PARTS.NUM = K55ADMIN.PRODUCTS.NUM "
            + "             or K55ADMIN.PRODUCTS.PRODUCT LIKE 'Screw%')"
            + "    order by 1,2,3,4");

        expColNames = new String [] {"PART", "NUM", "PRODUCT", "NUM"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Blades", "205", "Saw", "205"},
            {"Blades", "205", "Screwdriver", "505"},
            {"Magnets", "10", "Generator", "10"},
            {"Magnets", "10", "Screwdriver", "505"},
            {"Oil", "160", "Screwdriver", "505"},
            {"Paper", "20", "Sander", "20"},
            {"Paper", "20", "Screwdriver", "505"},
            {"Paper", "20", null, "20"},
            {"Plastic", "30", "Relay", "30"},
            {"Plastic", "30", "Ruler", "30"},
            {"Plastic", "30", "Screwdriver", "505"},
            {"Steel", "30", "Relay", "30"},
            {"Steel", "30", "Ruler", "30"},
            {"Steel", "30", "Screwdriver", "505"},
            {"Wire", "10", "Generator", "10"},
            {"Wire", "10", "Screwdriver", "505"},
            {null, "30", "Relay", "30"},
            {null, "30", "Ruler", "30"},
            {null, "30", "Screwdriver", "505"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 303 - '='/in in LEFT JOIN ON condition

        rs = st.executeQuery(
            "SELECT PART, K55ADMIN.PARTS.NUM, PRODUCT, "
            + "K55ADMIN.PRODUCTS.NUM"
            + "  FROM K55ADMIN.PARTS LEFT JOIN K55ADMIN.PRODUCTS"
            + "    ON ( K55ADMIN.PRODUCTS.PRODUCT in ('Screwdriver','Saw') "
            + "             or K55ADMIN.PARTS.NUM = K55ADMIN.PRODUCTS.NUM )"
            + "    order by 1,2,3,4");

        expColNames = new String [] {"PART", "NUM", "PRODUCT", "NUM"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Blades", "205", "Saw", "205"},
            {"Blades", "205", "Screwdriver", "505"},
            {"Magnets", "10", "Generator", "10"},
            {"Magnets", "10", "Saw", "205"},
            {"Magnets", "10", "Screwdriver", "505"},
            {"Oil", "160", "Saw", "205"},
            {"Oil", "160", "Screwdriver", "505"},
            {"Paper", "20", "Sander", "20"},
            {"Paper", "20", "Saw", "205"},
            {"Paper", "20", "Screwdriver", "505"},
            {"Paper", "20", null, "20"},
            {"Plastic", "30", "Relay", "30"},
            {"Plastic", "30", "Ruler", "30"},
            {"Plastic", "30", "Saw", "205"},
            {"Plastic", "30", "Screwdriver", "505"},
            {"Steel", "30", "Relay", "30"},
            {"Steel", "30", "Ruler", "30"},
            {"Steel", "30", "Saw", "205"},
            {"Steel", "30", "Screwdriver", "505"},
            {"Wire", "10", "Generator", "10"},
            {"Wire", "10", "Saw", "205"},
            {"Wire", "10", "Screwdriver", "505"},
            {null, "30", "Relay", "30"},
            {null, "30", "Ruler", "30"},
            {null, "30", "Saw", "205"},
            {null, "30", "Screwdriver", "505"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 304 - '='/is-null in LEFT JOIN ON condition

        rs = st.executeQuery(
            "SELECT PART, K55ADMIN.PARTS.NUM, PRODUCT, "
            + "K55ADMIN.PRODUCTS.NUM"
            + "  FROM K55ADMIN.PARTS LEFT JOIN K55ADMIN.PRODUCTS"
            + "    ON ( K55ADMIN.PRODUCTS.PRODUCT is null "
            + "             or K55ADMIN.PARTS.NUM = K55ADMIN.PRODUCTS.NUM )"
            + "    order by 1,2,3,4");

        expColNames = new String [] {"PART", "NUM", "PRODUCT", "NUM"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Blades", "205", "Saw", "205"},
            {"Blades", "205", null, "20"},
            {"Magnets", "10", "Generator", "10"},
            {"Magnets", "10", null, "20"},
            {"Oil", "160", null, "20"},
            {"Paper", "20", "Sander", "20"},
            {"Paper", "20", null, "20"},
            {"Plastic", "30", "Relay", "30"},
            {"Plastic", "30", "Ruler", "30"},
            {"Plastic", "30", null, "20"},
            {"Steel", "30", "Relay", "30"},
            {"Steel", "30", "Ruler", "30"},
            {"Steel", "30", null, "20"},
            {"Wire", "10", "Generator", "10"},
            {"Wire", "10", null, "20"},
            {null, "30", "Relay", "30"},
            {null, "30", "Ruler", "30"},
            {null, "30", null, "20"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 305 - '='/is-not-null in LEFT JOIN ON condition

        rs = st.executeQuery(
            "SELECT PART, K55ADMIN.PARTS.NUM, PRODUCT, "
            + "K55ADMIN.PRODUCTS.NUM"
            + "  FROM K55ADMIN.PARTS LEFT JOIN K55ADMIN.PRODUCTS"
            + "    ON ( K55ADMIN.PRODUCTS.PRODUCT is not null "
            + "             or K55ADMIN.PARTS.NUM = K55ADMIN.PRODUCTS.NUM )"
            + "    order by 1,2,3,4");

        expColNames = new String [] {"PART", "NUM", "PRODUCT", "NUM"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Blades", "205", "Generator", "10"},
            {"Blades", "205", "Hammer", "50"},
            {"Blades", "205", "Relay", "30"},
            {"Blades", "205", "Ruler", "30"},
            {"Blades", "205", "Sander", "20"},
            {"Blades", "205", "Saw", "205"},
            {"Blades", "205", "Screwdriver", "505"},
            {"Magnets", "10", "Generator", "10"},
            {"Magnets", "10", "Hammer", "50"},
            {"Magnets", "10", "Relay", "30"},
            {"Magnets", "10", "Ruler", "30"},
            {"Magnets", "10", "Sander", "20"},
            {"Magnets", "10", "Saw", "205"},
            {"Magnets", "10", "Screwdriver", "505"},
            {"Oil", "160", "Generator", "10"},
            {"Oil", "160", "Hammer", "50"},
            {"Oil", "160", "Relay", "30"},
            {"Oil", "160", "Ruler", "30"},
            {"Oil", "160", "Sander", "20"},
            {"Oil", "160", "Saw", "205"},
            {"Oil", "160", "Screwdriver", "505"},
            {"Paper", "20", "Generator", "10"},
            {"Paper", "20", "Hammer", "50"},
            {"Paper", "20", "Relay", "30"},
            {"Paper", "20", "Ruler", "30"},
            {"Paper", "20", "Sander", "20"},
            {"Paper", "20", "Saw", "205"},
            {"Paper", "20", "Screwdriver", "505"},
            {"Paper", "20", null, "20"},
            {"Plastic", "30", "Generator", "10"},
            {"Plastic", "30", "Hammer", "50"},
            {"Plastic", "30", "Relay", "30"},
            {"Plastic", "30", "Ruler", "30"},
            {"Plastic", "30", "Sander", "20"},
            {"Plastic", "30", "Saw", "205"},
            {"Plastic", "30", "Screwdriver", "505"},
            {"Steel", "30", "Generator", "10"},
            {"Steel", "30", "Hammer", "50"},
            {"Steel", "30", "Relay", "30"},
            {"Steel", "30", "Ruler", "30"},
            {"Steel", "30", "Sander", "20"},
            {"Steel", "30", "Saw", "205"},
            {"Steel", "30", "Screwdriver", "505"},
            {"Wire", "10", "Generator", "10"},
            {"Wire", "10", "Hammer", "50"},
            {"Wire", "10", "Relay", "30"},
            {"Wire", "10", "Ruler", "30"},
            {"Wire", "10", "Sander", "20"},
            {"Wire", "10", "Saw", "205"},
            {"Wire", "10", "Screwdriver", "505"},
            {null, "30", "Generator", "10"},
            {null, "30", "Hammer", "50"},
            {null, "30", "Relay", "30"},
            {null, "30", "Ruler", "30"},
            {null, "30", "Sander", "20"},
            {null, "30", "Saw", "205"},
            {null, "30", "Screwdriver", "505"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 306 - '='/'<' in LEFT JOIN ON condition

        rs = st.executeQuery(
            "SELECT PART, K55ADMIN.PARTS.NUM, PRODUCT, "
            + "K55ADMIN.PRODUCTS.NUM"
            + "  FROM K55ADMIN.PARTS LEFT JOIN K55ADMIN.PRODUCTS"
            + "    ON ( K55ADMIN.PARTS.NUM < K55ADMIN.PRODUCTS.NUM "
            + "             or K55ADMIN.PARTS.NUM = K55ADMIN.PRODUCTS.NUM )"
            + "    order by 1,2,3,4");

        expColNames = new String [] {"PART", "NUM", "PRODUCT", "NUM"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Blades", "205", "Saw", "205"},
            {"Blades", "205", "Screwdriver", "505"},
            {"Magnets", "10", "Generator", "10"},
            {"Magnets", "10", "Hammer", "50"},
            {"Magnets", "10", "Relay", "30"},
            {"Magnets", "10", "Ruler", "30"},
            {"Magnets", "10", "Sander", "20"},
            {"Magnets", "10", "Saw", "205"},
            {"Magnets", "10", "Screwdriver", "505"},
            {"Magnets", "10", null, "20"},
            {"Oil", "160", "Saw", "205"},
            {"Oil", "160", "Screwdriver", "505"},
            {"Paper", "20", "Hammer", "50"},
            {"Paper", "20", "Relay", "30"},
            {"Paper", "20", "Ruler", "30"},
            {"Paper", "20", "Sander", "20"},
            {"Paper", "20", "Saw", "205"},
            {"Paper", "20", "Screwdriver", "505"},
            {"Paper", "20", null, "20"},
            {"Plastic", "30", "Hammer", "50"},
            {"Plastic", "30", "Relay", "30"},
            {"Plastic", "30", "Ruler", "30"},
            {"Plastic", "30", "Saw", "205"},
            {"Plastic", "30", "Screwdriver", "505"},
            {"Steel", "30", "Hammer", "50"},
            {"Steel", "30", "Relay", "30"},
            {"Steel", "30", "Ruler", "30"},
            {"Steel", "30", "Saw", "205"},
            {"Steel", "30", "Screwdriver", "505"},
            {"Wire", "10", "Generator", "10"},
            {"Wire", "10", "Hammer", "50"},
            {"Wire", "10", "Relay", "30"},
            {"Wire", "10", "Ruler", "30"},
            {"Wire", "10", "Sander", "20"},
            {"Wire", "10", "Saw", "205"},
            {"Wire", "10", "Screwdriver", "505"},
            {"Wire", "10", null, "20"},
            {null, "30", "Hammer", "50"},
            {null, "30", "Relay", "30"},
            {null, "30", "Ruler", "30"},
            {null, "30", "Saw", "205"},
            {null, "30", "Screwdriver", "505"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 307 - '='/like in LEFT JOIN ON condition

        rs = st.executeQuery(
            "SELECT PART, K55ADMIN.PARTS.NUM, PRODUCT, "
            + "K55ADMIN.PRODUCTS.NUM"
            + "  FROM K55ADMIN.PARTS LEFT JOIN K55ADMIN.PRODUCTS"
            + "    ON ( K55ADMIN.PARTS.NUM = K55ADMIN.PRODUCTS.NUM "
            + "             or K55ADMIN.PRODUCTS.PRODUCT LIKE 'Nut%' )"
            + "    order by 1,2,3,4");

        expColNames = new String [] {"PART", "NUM", "PRODUCT", "NUM"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Blades", "205", "Saw", "205"},
            {"Magnets", "10", "Generator", "10"},
            {"Oil", "160", null, null},
            {"Paper", "20", "Sander", "20"},
            {"Paper", "20", null, "20"},
            {"Plastic", "30", "Relay", "30"},
            {"Plastic", "30", "Ruler", "30"},
            {"Steel", "30", "Relay", "30"},
            {"Steel", "30", "Ruler", "30"},
            {"Wire", "10", "Generator", "10"},
            {null, "30", "Relay", "30"},
            {null, "30", "Ruler", "30"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 308 - '='/like/in in RIGHT JOIN ON condition

        rs = st.executeQuery(
            "SELECT PART, K55ADMIN.PARTS.NUM, PRODUCT, "
            + "K55ADMIN.PRODUCTS.NUM"
            + "  FROM K55ADMIN.PARTS LEFT JOIN K55ADMIN.PRODUCTS"
            + "    ON ( K55ADMIN.PRODUCTS.PRODUCT in ('Bolt','Nuts') "
            + "             or K55ADMIN.PRODUCTS.PRODUCT LIKE 'Power%' "
            + "             or K55ADMIN.PARTS.NUM = K55ADMIN.PRODUCTS.NUM )"
            + "    order by 1,2,3,4");

        expColNames = new String [] {"PART", "NUM", "PRODUCT", "NUM"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Blades", "205", "Saw", "205"},
            {"Magnets", "10", "Generator", "10"},
            {"Oil", "160", null, null},
            {"Paper", "20", "Sander", "20"},
            {"Paper", "20", null, "20"},
            {"Plastic", "30", "Relay", "30"},
            {"Plastic", "30", "Ruler", "30"},
            {"Steel", "30", "Relay", "30"},
            {"Steel", "30", "Ruler", "30"},
            {"Wire", "10", "Generator", "10"},
            {null, "30", "Relay", "30"},
            {null, "30", "Ruler", "30"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        //
        //------------------------------------------------------------
        // test unit 4. plain joins, only local predicates, no join predicate
        //------------------------------------------------------------

        // 401 - local '=' pred on tuple-preserving operand

        rs = st.executeQuery(
            "SELECT PART, K55ADMIN.PARTS.NUM, PRODUCT, "
            + "K55ADMIN.PRODUCTS.NUM"
            + "  FROM K55ADMIN.PARTS LEFT JOIN K55ADMIN.PRODUCTS"
            + "    ON K55ADMIN.PARTS.NUM = 10"
            + "    order by 1,2,3,4");

        expColNames = new String [] {"PART", "NUM", "PRODUCT", "NUM"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Blades", "205", null, null},
            {"Magnets", "10", "Generator", "10"},
            {"Magnets", "10", "Hammer", "50"},
            {"Magnets", "10", "Relay", "30"},
            {"Magnets", "10", "Ruler", "30"},
            {"Magnets", "10", "Sander", "20"},
            {"Magnets", "10", "Saw", "205"},
            {"Magnets", "10", "Screwdriver", "505"},
            {"Magnets", "10", null, "20"},
            {"Oil", "160", null, null},
            {"Paper", "20", null, null},
            {"Plastic", "30", null, null},
            {"Steel", "30", null, null},
            {"Wire", "10", "Generator", "10"},
            {"Wire", "10", "Hammer", "50"},
            {"Wire", "10", "Relay", "30"},
            {"Wire", "10", "Ruler", "30"},
            {"Wire", "10", "Sander", "20"},
            {"Wire", "10", "Saw", "205"},
            {"Wire", "10", "Screwdriver", "505"},
            {"Wire", "10", null, "20"},
            {null, "30", null, null}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 402 - local IN pred on tuple-preserving operand

        rs = st.executeQuery(
            "SELECT PART, K55ADMIN.PARTS.NUM, PRODUCT, "
            + "K55ADMIN.PRODUCTS.NUM"
            + "  FROM K55ADMIN.PARTS LEFT JOIN K55ADMIN.PRODUCTS"
            + "    ON K55ADMIN.PARTS.NUM IN (160, 205)"
            + "    order by 1,2,3,4");

        expColNames = new String [] {"PART", "NUM", "PRODUCT", "NUM"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Blades", "205", "Generator", "10"},
            {"Blades", "205", "Hammer", "50"},
            {"Blades", "205", "Relay", "30"},
            {"Blades", "205", "Ruler", "30"},
            {"Blades", "205", "Sander", "20"},
            {"Blades", "205", "Saw", "205"},
            {"Blades", "205", "Screwdriver", "505"},
            {"Blades", "205", null, "20"},
            {"Magnets", "10", null, null},
            {"Oil", "160", "Generator", "10"},
            {"Oil", "160", "Hammer", "50"},
            {"Oil", "160", "Relay", "30"},
            {"Oil", "160", "Ruler", "30"},
            {"Oil", "160", "Sander", "20"},
            {"Oil", "160", "Saw", "205"},
            {"Oil", "160", "Screwdriver", "505"},
            {"Oil", "160", null, "20"},
            {"Paper", "20", null, null},
            {"Plastic", "30", null, null},
            {"Steel", "30", null, null},
            {"Wire", "10", null, null},
            {null, "30", null, null}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 403 - local between pred on tuple-preserving operand

        rs = st.executeQuery(
            "SELECT PART, K55ADMIN.PARTS.NUM, PRODUCT, "
            + "K55ADMIN.PRODUCTS.NUM"
            + "  FROM K55ADMIN.PARTS LEFT JOIN K55ADMIN.PRODUCTS"
            + "    ON K55ADMIN.PARTS.NUM between 150 and 250"
            + "    order by 1,2,3,4");

        expColNames = new String [] {"PART", "NUM", "PRODUCT", "NUM"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Blades", "205", "Generator", "10"},
            {"Blades", "205", "Hammer", "50"},
            {"Blades", "205", "Relay", "30"},
            {"Blades", "205", "Ruler", "30"},
            {"Blades", "205", "Sander", "20"},
            {"Blades", "205", "Saw", "205"},
            {"Blades", "205", "Screwdriver", "505"},
            {"Blades", "205", null, "20"},
            {"Magnets", "10", null, null},
            {"Oil", "160", "Generator", "10"},
            {"Oil", "160", "Hammer", "50"},
            {"Oil", "160", "Relay", "30"},
            {"Oil", "160", "Ruler", "30"},
            {"Oil", "160", "Sander", "20"},
            {"Oil", "160", "Saw", "205"},
            {"Oil", "160", "Screwdriver", "505"},
            {"Oil", "160", null, "20"},
            {"Paper", "20", null, null},
            {"Plastic", "30", null, null},
            {"Steel", "30", null, null},
            {"Wire", "10", null, null},
            {null, "30", null, null}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 404 - local '=' pred on null-producing operand

        rs = st.executeQuery(
            "SELECT PART, K55ADMIN.PARTS.NUM, PRODUCT, "
            + "K55ADMIN.PRODUCTS.NUM"
            + "  FROM K55ADMIN.PARTS RIGHT JOIN K55ADMIN.PRODUCTS"
            + "    ON K55ADMIN.PARTS.NUM = 10"
            + "    order by 1,2,3,4");

        expColNames = new String [] {"PART", "NUM", "PRODUCT", "NUM"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Magnets", "10", "Generator", "10"},
            {"Magnets", "10", "Hammer", "50"},
            {"Magnets", "10", "Relay", "30"},
            {"Magnets", "10", "Ruler", "30"},
            {"Magnets", "10", "Sander", "20"},
            {"Magnets", "10", "Saw", "205"},
            {"Magnets", "10", "Screwdriver", "505"},
            {"Magnets", "10", null, "20"},
            {"Wire", "10", "Generator", "10"},
            {"Wire", "10", "Hammer", "50"},
            {"Wire", "10", "Relay", "30"},
            {"Wire", "10", "Ruler", "30"},
            {"Wire", "10", "Sander", "20"},
            {"Wire", "10", "Saw", "205"},
            {"Wire", "10", "Screwdriver", "505"},
            {"Wire", "10", null, "20"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 405 - local IN pred on null-producing operand

        rs = st.executeQuery(
            "SELECT PART, K55ADMIN.PARTS.NUM, PRODUCT, "
            + "K55ADMIN.PRODUCTS.NUM"
            + "  FROM K55ADMIN.PARTS RIGHT JOIN K55ADMIN.PRODUCTS"
            + "    ON K55ADMIN.PARTS.NUM IN (10, 20)"
            + "    order by 1,2,3,4");

        expColNames = new String [] {"PART", "NUM", "PRODUCT", "NUM"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Magnets", "10", "Generator", "10"},
            {"Magnets", "10", "Hammer", "50"},
            {"Magnets", "10", "Relay", "30"},
            {"Magnets", "10", "Ruler", "30"},
            {"Magnets", "10", "Sander", "20"},
            {"Magnets", "10", "Saw", "205"},
            {"Magnets", "10", "Screwdriver", "505"},
            {"Magnets", "10", null, "20"},
            {"Paper", "20", "Generator", "10"},
            {"Paper", "20", "Hammer", "50"},
            {"Paper", "20", "Relay", "30"},
            {"Paper", "20", "Ruler", "30"},
            {"Paper", "20", "Sander", "20"},
            {"Paper", "20", "Saw", "205"},
            {"Paper", "20", "Screwdriver", "505"},
            {"Paper", "20", null, "20"},
            {"Wire", "10", "Generator", "10"},
            {"Wire", "10", "Hammer", "50"},
            {"Wire", "10", "Relay", "30"},
            {"Wire", "10", "Ruler", "30"},
            {"Wire", "10", "Sander", "20"},
            {"Wire", "10", "Saw", "205"},
            {"Wire", "10", "Screwdriver", "505"},
            {"Wire", "10", null, "20"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 406 - local between pred on null-producing operand

        rs = st.executeQuery(
            "SELECT PART, K55ADMIN.PARTS.NUM, PRODUCT, "
            + "K55ADMIN.PRODUCTS.NUM"
            + "  FROM K55ADMIN.PARTS RIGHT JOIN K55ADMIN.PRODUCTS"
            + "    ON K55ADMIN.PARTS.NUM between 10 and 50"
            + "    order by 1,2,3,4");

        expColNames = new String [] {"PART", "NUM", "PRODUCT", "NUM"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Magnets", "10", "Generator", "10"},
            {"Magnets", "10", "Hammer", "50"},
            {"Magnets", "10", "Relay", "30"},
            {"Magnets", "10", "Ruler", "30"},
            {"Magnets", "10", "Sander", "20"},
            {"Magnets", "10", "Saw", "205"},
            {"Magnets", "10", "Screwdriver", "505"},
            {"Magnets", "10", null, "20"},
            {"Paper", "20", "Generator", "10"},
            {"Paper", "20", "Hammer", "50"},
            {"Paper", "20", "Relay", "30"},
            {"Paper", "20", "Ruler", "30"},
            {"Paper", "20", "Sander", "20"},
            {"Paper", "20", "Saw", "205"},
            {"Paper", "20", "Screwdriver", "505"},
            {"Paper", "20", null, "20"},
            {"Plastic", "30", "Generator", "10"},
            {"Plastic", "30", "Hammer", "50"},
            {"Plastic", "30", "Relay", "30"},
            {"Plastic", "30", "Ruler", "30"},
            {"Plastic", "30", "Sander", "20"},
            {"Plastic", "30", "Saw", "205"},
            {"Plastic", "30", "Screwdriver", "505"},
            {"Plastic", "30", null, "20"},
            {"Steel", "30", "Generator", "10"},
            {"Steel", "30", "Hammer", "50"},
            {"Steel", "30", "Relay", "30"},
            {"Steel", "30", "Ruler", "30"},
            {"Steel", "30", "Sander", "20"},
            {"Steel", "30", "Saw", "205"},
            {"Steel", "30", "Screwdriver", "505"},
            {"Steel", "30", null, "20"},
            {"Wire", "10", "Generator", "10"},
            {"Wire", "10", "Hammer", "50"},
            {"Wire", "10", "Relay", "30"},
            {"Wire", "10", "Ruler", "30"},
            {"Wire", "10", "Sander", "20"},
            {"Wire", "10", "Saw", "205"},
            {"Wire", "10", "Screwdriver", "505"},
            {"Wire", "10", null, "20"},
            {null, "30", "Generator", "10"},
            {null, "30", "Hammer", "50"},
            {null, "30", "Relay", "30"},
            {null, "30", "Ruler", "30"},
            {null, "30", "Sander", "20"},
            {null, "30", "Saw", "205"},
            {null, "30", "Screwdriver", "505"},
            {null, "30", null, "20"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        //
        //------------------------------------------------------------
        // test unit 5. plain joins, local predicate and join predicate;
        //------------------------------------------------------------

        // 501 - local pred on tuple-preserving operand, '=' join predicate,
        //       and;

        rs = st.executeQuery(
            "SELECT PART, K55ADMIN.PARTS.NUM, PRODUCT, "
            + "K55ADMIN.PRODUCTS.NUM"
            + "  FROM K55ADMIN.PARTS LEFT JOIN K55ADMIN.PRODUCTS"
            + "    ON K55ADMIN.PARTS.NUM = K55ADMIN.PRODUCTS.NUM and"
            + "       K55ADMIN.PARTS.PART <> 'Wire'"
            + "    order by 1,2,3,4");

        expColNames = new String [] {"PART", "NUM", "PRODUCT", "NUM"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Blades", "205", "Saw", "205"},
            {"Magnets", "10", "Generator", "10"},
            {"Oil", "160", null, null},
            {"Paper", "20", "Sander", "20"},
            {"Paper", "20", null, "20"},
            {"Plastic", "30", "Relay", "30"},
            {"Plastic", "30", "Ruler", "30"},
            {"Steel", "30", "Relay", "30"},
            {"Steel", "30", "Ruler", "30"},
            {"Wire", "10", null, null},
            {null, "30", null, null}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 502 - local pred on both operands, '=' join predicate, and

        rs = st.executeQuery(
            "SELECT K55ADMIN.PARTS.*, K55ADMIN.PRODUCTS.*"
            + "  FROM K55ADMIN.PARTS LEFT JOIN K55ADMIN.PRODUCTS"
            + "    ON K55ADMIN.PARTS.NUM = K55ADMIN.PRODUCTS.NUM and"
            + "       (K55ADMIN.PRODUCTS.PRICE>8 or "
            + "K55ADMIN.PARTS.PART = 'Steel')"
            + "        and K55ADMIN.PRODUCTS.NUM > 20"
            + "    order by 1,2,3,4,5,6");

        expColNames = new String [] {"PART", "NUM", "SUPPLIER", "NUM", "PRODUCT", "PRICE"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Blades", "205", "Ace-Steel", "205", "Saw", "18.90"},
            {"Magnets", "10", "Bateman", null, null, null},
            {"Oil", "160", "Western-Chem", null, null, null},
            {"Paper", "20", "Ace-Steel", null, null, null},
            {"Plastic", "30", "Plastik-Corp", "30", "Ruler", "8.75"},
            {"Steel", "30", "ACWF", "30", "Relay", "7.55"},
            {"Steel", "30", "ACWF", "30", "Ruler", "8.75"},
            {"Wire", "10", "ACWF", null, null, null},
            {null, "30", null, "30", "Ruler", "8.75"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 503 - local pred on both operands, '=' join predicate,
        //       and/or

        rs = st.executeQuery(
            "SELECT K55ADMIN.PARTS.*, K55ADMIN.PRODUCTS.*"
            + "  FROM K55ADMIN.PARTS LEFT JOIN K55ADMIN.PRODUCTS"
            + "    ON K55ADMIN.PARTS.NUM = K55ADMIN.PRODUCTS.NUM and"
            + "       (K55ADMIN.PRODUCTS.PRICE>8 or "
            + "K55ADMIN.PARTS.PART = 'Steel'"
            + "        or K55ADMIN.PRODUCTS.NUM > 20)"
            + "    order by 1,2,3,4,5,6");

        expColNames = new String [] {"PART", "NUM", "SUPPLIER", "NUM", "PRODUCT", "PRICE"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Blades", "205", "Ace-Steel", "205", "Saw", "18.90"},
            {"Magnets", "10", "Bateman", "10", "Generator", "45.75"},
            {"Oil", "160", "Western-Chem", null, null, null},
            {"Paper", "20", "Ace-Steel", "20", "Sander", "35.75"},
            {"Plastic", "30", "Plastik-Corp", "30", "Relay", "7.55"},
            {"Plastic", "30", "Plastik-Corp", "30", "Ruler", "8.75"},
            {"Steel", "30", "ACWF", "30", "Relay", "7.55"},
            {"Steel", "30", "ACWF", "30", "Ruler", "8.75"},
            {"Wire", "10", "ACWF", "10", "Generator", "45.75"},
            {null, "30", null, "30", "Relay", "7.55"},
            {null, "30", null, "30", "Ruler", "8.75"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 504 - local '=' pred on null-producing operand OR '='
        //       join predicate

        rs = st.executeQuery(
            "SELECT PART, K55ADMIN.PARTS.NUM, PRODUCT, "
            + "K55ADMIN.PRODUCTS.NUM"
            + "  FROM K55ADMIN.PARTS RIGHT JOIN K55ADMIN.PRODUCTS"
            + "    ON K55ADMIN.PARTS.NUM = 10 OR"
            + "       K55ADMIN.PARTS.NUM = K55ADMIN.PRODUCTS.NUM"
            + "    order by 1,2,3,4");

        expColNames = new String [] {"PART", "NUM", "PRODUCT", "NUM"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Blades", "205", "Saw", "205"},
            {"Magnets", "10", "Generator", "10"},
            {"Magnets", "10", "Hammer", "50"},
            {"Magnets", "10", "Relay", "30"},
            {"Magnets", "10", "Ruler", "30"},
            {"Magnets", "10", "Sander", "20"},
            {"Magnets", "10", "Saw", "205"},
            {"Magnets", "10", "Screwdriver", "505"},
            {"Magnets", "10", null, "20"},
            {"Paper", "20", "Sander", "20"},
            {"Paper", "20", null, "20"},
            {"Plastic", "30", "Relay", "30"},
            {"Plastic", "30", "Ruler", "30"},
            {"Steel", "30", "Relay", "30"},
            {"Steel", "30", "Ruler", "30"},
            {"Wire", "10", "Generator", "10"},
            {"Wire", "10", "Hammer", "50"},
            {"Wire", "10", "Relay", "30"},
            {"Wire", "10", "Ruler", "30"},
            {"Wire", "10", "Sander", "20"},
            {"Wire", "10", "Saw", "205"},
            {"Wire", "10", "Screwdriver", "505"},
            {"Wire", "10", null, "20"},
            {null, "30", "Relay", "30"},
            {null, "30", "Ruler", "30"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 505 - local '=' pred on tuple-preserving operand OR '='
        //       join predicate

        rs = st.executeQuery(
            "SELECT PART, K55ADMIN.PARTS.NUM, PRODUCT, "
            + "K55ADMIN.PRODUCTS.NUM"
            + "  FROM K55ADMIN.PARTS LEFT JOIN K55ADMIN.PRODUCTS"
            + "    ON K55ADMIN.PARTS.NUM = 10 OR"
            + "       K55ADMIN.PARTS.NUM = K55ADMIN.PRODUCTS.NUM"
            + "    order by 1,2,3,4");

        expColNames = new String [] {"PART", "NUM", "PRODUCT", "NUM"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Blades", "205", "Saw", "205"},
            {"Magnets", "10", "Generator", "10"},
            {"Magnets", "10", "Hammer", "50"},
            {"Magnets", "10", "Relay", "30"},
            {"Magnets", "10", "Ruler", "30"},
            {"Magnets", "10", "Sander", "20"},
            {"Magnets", "10", "Saw", "205"},
            {"Magnets", "10", "Screwdriver", "505"},
            {"Magnets", "10", null, "20"},
            {"Oil", "160", null, null},
            {"Paper", "20", "Sander", "20"},
            {"Paper", "20", null, "20"},
            {"Plastic", "30", "Relay", "30"},
            {"Plastic", "30", "Ruler", "30"},
            {"Steel", "30", "Relay", "30"},
            {"Steel", "30", "Ruler", "30"},
            {"Wire", "10", "Generator", "10"},
            {"Wire", "10", "Hammer", "50"},
            {"Wire", "10", "Relay", "30"},
            {"Wire", "10", "Ruler", "30"},
            {"Wire", "10", "Sander", "20"},
            {"Wire", "10", "Saw", "205"},
            {"Wire", "10", "Screwdriver", "505"},
            {"Wire", "10", null, "20"},
            {null, "30", "Relay", "30"},
            {null, "30", "Ruler", "30"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 506 - local '=' pred on tuple-preserving operand AND
        //       '<>' join predicate

        rs = st.executeQuery(
            "SELECT PART, K55ADMIN.PARTS.NUM, PRODUCT, "
            + "K55ADMIN.PRODUCTS.NUM"
            + "  FROM K55ADMIN.PARTS LEFT JOIN K55ADMIN.PRODUCTS"
            + "    ON K55ADMIN.PARTS.NUM = 1000 AND"
            + "       K55ADMIN.PARTS.NUM <> K55ADMIN.PRODUCTS.NUM"
            + "    order by 1,2,3,4");

        expColNames = new String [] {"PART", "NUM", "PRODUCT", "NUM"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Blades", "205", null, null},
            {"Magnets", "10", null, null},
            {"Oil", "160", null, null},
            {"Paper", "20", null, null},
            {"Plastic", "30", null, null},
            {"Steel", "30", null, null},
            {"Wire", "10", null, null},
            {null, "30", null, null}
        };

        JDBC.assertFullResultSet(rs, expRS);

        //
        //------------------------------------------------------------
        // test unit 6. plain joins, '1=0' or '2>1' predicate, w/ or w/o join
        // predicate;
        //------------------------------------------------------------

        // 601 - '=' join predicate and '2>1'

        rs = st.executeQuery(
            "SELECT PART, K55ADMIN.PARTS.NUM, PRODUCT, "
            + "K55ADMIN.PRODUCTS.NUM"
            + "  FROM K55ADMIN.PARTS LEFT JOIN K55ADMIN.PRODUCTS"
            + "    ON K55ADMIN.PARTS.NUM = K55ADMIN.PRODUCTS.NUM and"
            + "       2>1"
            + "    order by 1,2,3,4");

        expColNames = new String [] {"PART", "NUM", "PRODUCT", "NUM"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Blades", "205", "Saw", "205"},
            {"Magnets", "10", "Generator", "10"},
            {"Oil", "160", null, null},
            {"Paper", "20", "Sander", "20"},
            {"Paper", "20", null, "20"},
            {"Plastic", "30", "Relay", "30"},
            {"Plastic", "30", "Ruler", "30"},
            {"Steel", "30", "Relay", "30"},
            {"Steel", "30", "Ruler", "30"},
            {"Wire", "10", "Generator", "10"},
            {null, "30", "Relay", "30"},
            {null, "30", "Ruler", "30"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 602 - '=' join predicate and '1=0'

        rs = st.executeQuery(
            "SELECT PART, K55ADMIN.PARTS.NUM, PRODUCT, "
            + "K55ADMIN.PRODUCTS.NUM"
            + "  FROM K55ADMIN.PARTS LEFT JOIN K55ADMIN.PRODUCTS"
            + "    ON K55ADMIN.PARTS.NUM = K55ADMIN.PRODUCTS.NUM and"
            + "       1=0"
            + "    order by 1,2,3,4");

        expColNames = new String [] {"PART", "NUM", "PRODUCT", "NUM"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Blades", "205", null, null},
            {"Magnets", "10", null, null},
            {"Oil", "160", null, null},
            {"Paper", "20", null, null},
            {"Plastic", "30", null, null},
            {"Steel", "30", null, null},
            {"Wire", "10", null, null},
            {null, "30", null, null}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 603 - '=' join predicate or '2>1'

        rs = st.executeQuery(
            "SELECT PART, K55ADMIN.PARTS.NUM, PRODUCT, "
            + "K55ADMIN.PRODUCTS.NUM"
            + "  FROM K55ADMIN.PARTS LEFT JOIN K55ADMIN.PRODUCTS"
            + "    ON K55ADMIN.PARTS.NUM = K55ADMIN.PRODUCTS.NUM or"
            + "       2>1"
            + "    order by 1,2,3,4");

        expColNames = new String [] {"PART", "NUM", "PRODUCT", "NUM"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Blades", "205", "Generator", "10"},
            {"Blades", "205", "Hammer", "50"},
            {"Blades", "205", "Relay", "30"},
            {"Blades", "205", "Ruler", "30"},
            {"Blades", "205", "Sander", "20"},
            {"Blades", "205", "Saw", "205"},
            {"Blades", "205", "Screwdriver", "505"},
            {"Blades", "205", null, "20"},
            {"Magnets", "10", "Generator", "10"},
            {"Magnets", "10", "Hammer", "50"},
            {"Magnets", "10", "Relay", "30"},
            {"Magnets", "10", "Ruler", "30"},
            {"Magnets", "10", "Sander", "20"},
            {"Magnets", "10", "Saw", "205"},
            {"Magnets", "10", "Screwdriver", "505"},
            {"Magnets", "10", null, "20"},
            {"Oil", "160", "Generator", "10"},
            {"Oil", "160", "Hammer", "50"},
            {"Oil", "160", "Relay", "30"},
            {"Oil", "160", "Ruler", "30"},
            {"Oil", "160", "Sander", "20"},
            {"Oil", "160", "Saw", "205"},
            {"Oil", "160", "Screwdriver", "505"},
            {"Oil", "160", null, "20"},
            {"Paper", "20", "Generator", "10"},
            {"Paper", "20", "Hammer", "50"},
            {"Paper", "20", "Relay", "30"},
            {"Paper", "20", "Ruler", "30"},
            {"Paper", "20", "Sander", "20"},
            {"Paper", "20", "Saw", "205"},
            {"Paper", "20", "Screwdriver", "505"},
            {"Paper", "20", null, "20"},
            {"Plastic", "30", "Generator", "10"},
            {"Plastic", "30", "Hammer", "50"},
            {"Plastic", "30", "Relay", "30"},
            {"Plastic", "30", "Ruler", "30"},
            {"Plastic", "30", "Sander", "20"},
            {"Plastic", "30", "Saw", "205"},
            {"Plastic", "30", "Screwdriver", "505"},
            {"Plastic", "30", null, "20"},
            {"Steel", "30", "Generator", "10"},
            {"Steel", "30", "Hammer", "50"},
            {"Steel", "30", "Relay", "30"},
            {"Steel", "30", "Ruler", "30"},
            {"Steel", "30", "Sander", "20"},
            {"Steel", "30", "Saw", "205"},
            {"Steel", "30", "Screwdriver", "505"},
            {"Steel", "30", null, "20"},
            {"Wire", "10", "Generator", "10"},
            {"Wire", "10", "Hammer", "50"},
            {"Wire", "10", "Relay", "30"},
            {"Wire", "10", "Ruler", "30"},
            {"Wire", "10", "Sander", "20"},
            {"Wire", "10", "Saw", "205"},
            {"Wire", "10", "Screwdriver", "505"},
            {"Wire", "10", null, "20"},
            {null, "30", "Generator", "10"},
            {null, "30", "Hammer", "50"},
            {null, "30", "Relay", "30"},
            {null, "30", "Ruler", "30"},
            {null, "30", "Sander", "20"},
            {null, "30", "Saw", "205"},
            {null, "30", "Screwdriver", "505"},
            {null, "30", null, "20"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 604 - '=' join predicate or '1=0'

        rs = st.executeQuery(
            "SELECT PART, K55ADMIN.PARTS.NUM, PRODUCT, "
            + "K55ADMIN.PRODUCTS.NUM"
            + "  FROM K55ADMIN.PARTS LEFT JOIN K55ADMIN.PRODUCTS"
            + "    ON K55ADMIN.PARTS.NUM = K55ADMIN.PRODUCTS.NUM or"
            + "       1=0"
            + "    order by 1,2,3,4");

        expColNames = new String [] {"PART", "NUM", "PRODUCT", "NUM"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Blades", "205", "Saw", "205"},
            {"Magnets", "10", "Generator", "10"},
            {"Oil", "160", null, null},
            {"Paper", "20", "Sander", "20"},
            {"Paper", "20", null, "20"},
            {"Plastic", "30", "Relay", "30"},
            {"Plastic", "30", "Ruler", "30"},
            {"Steel", "30", "Relay", "30"},
            {"Steel", "30", "Ruler", "30"},
            {"Wire", "10", "Generator", "10"},
            {null, "30", "Relay", "30"},
            {null, "30", "Ruler", "30"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 605 - '2>1' in ON condition

        rs = st.executeQuery(
            "SELECT PART, K55ADMIN.PARTS.NUM, PRODUCT, "
            + "K55ADMIN.PRODUCTS.NUM"
            + "  FROM K55ADMIN.PARTS LEFT JOIN K55ADMIN.PRODUCTS"
            + "    ON 2>1"
            + "    order by 1,2,3,4");

        expColNames = new String [] {"PART", "NUM", "PRODUCT", "NUM"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Blades", "205", "Generator", "10"},
            {"Blades", "205", "Hammer", "50"},
            {"Blades", "205", "Relay", "30"},
            {"Blades", "205", "Ruler", "30"},
            {"Blades", "205", "Sander", "20"},
            {"Blades", "205", "Saw", "205"},
            {"Blades", "205", "Screwdriver", "505"},
            {"Blades", "205", null, "20"},
            {"Magnets", "10", "Generator", "10"},
            {"Magnets", "10", "Hammer", "50"},
            {"Magnets", "10", "Relay", "30"},
            {"Magnets", "10", "Ruler", "30"},
            {"Magnets", "10", "Sander", "20"},
            {"Magnets", "10", "Saw", "205"},
            {"Magnets", "10", "Screwdriver", "505"},
            {"Magnets", "10", null, "20"},
            {"Oil", "160", "Generator", "10"},
            {"Oil", "160", "Hammer", "50"},
            {"Oil", "160", "Relay", "30"},
            {"Oil", "160", "Ruler", "30"},
            {"Oil", "160", "Sander", "20"},
            {"Oil", "160", "Saw", "205"},
            {"Oil", "160", "Screwdriver", "505"},
            {"Oil", "160", null, "20"},
            {"Paper", "20", "Generator", "10"},
            {"Paper", "20", "Hammer", "50"},
            {"Paper", "20", "Relay", "30"},
            {"Paper", "20", "Ruler", "30"},
            {"Paper", "20", "Sander", "20"},
            {"Paper", "20", "Saw", "205"},
            {"Paper", "20", "Screwdriver", "505"},
            {"Paper", "20", null, "20"},
            {"Plastic", "30", "Generator", "10"},
            {"Plastic", "30", "Hammer", "50"},
            {"Plastic", "30", "Relay", "30"},
            {"Plastic", "30", "Ruler", "30"},
            {"Plastic", "30", "Sander", "20"},
            {"Plastic", "30", "Saw", "205"},
            {"Plastic", "30", "Screwdriver", "505"},
            {"Plastic", "30", null, "20"},
            {"Steel", "30", "Generator", "10"},
            {"Steel", "30", "Hammer", "50"},
            {"Steel", "30", "Relay", "30"},
            {"Steel", "30", "Ruler", "30"},
            {"Steel", "30", "Sander", "20"},
            {"Steel", "30", "Saw", "205"},
            {"Steel", "30", "Screwdriver", "505"},
            {"Steel", "30", null, "20"},
            {"Wire", "10", "Generator", "10"},
            {"Wire", "10", "Hammer", "50"},
            {"Wire", "10", "Relay", "30"},
            {"Wire", "10", "Ruler", "30"},
            {"Wire", "10", "Sander", "20"},
            {"Wire", "10", "Saw", "205"},
            {"Wire", "10", "Screwdriver", "505"},
            {"Wire", "10", null, "20"},
            {null, "30", "Generator", "10"},
            {null, "30", "Hammer", "50"},
            {null, "30", "Relay", "30"},
            {null, "30", "Ruler", "30"},
            {null, "30", "Sander", "20"},
            {null, "30", "Saw", "205"},
            {null, "30", "Screwdriver", "505"},
            {null, "30", null, "20"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 606 - '1=0' in ON condition

        rs = st.executeQuery(
            "SELECT PART, K55ADMIN.PARTS.NUM, PRODUCT, "
            + "K55ADMIN.PRODUCTS.NUM"
            + "  FROM K55ADMIN.PARTS LEFT JOIN K55ADMIN.PRODUCTS"
            + "    ON 1=0"
            + "    order by 1,2,3,4");

        expColNames = new String [] {"PART", "NUM", "PRODUCT", "NUM"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Blades", "205", null, null},
            {"Magnets", "10", null, null},
            {"Oil", "160", null, null},
            {"Paper", "20", null, null},
            {"Plastic", "30", null, null},
            {"Steel", "30", null, null},
            {"Wire", "10", null, null},
            {null, "30", null, null}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 607 - '1=1' in ON condition

        rs = st.executeQuery(
            "SELECT PART, K55ADMIN.PARTS.NUM, PRODUCT, "
            + "K55ADMIN.PRODUCTS.NUM"
            + "  FROM K55ADMIN.PARTS LEFT JOIN K55ADMIN.PRODUCTS"
            + "    ON 1=1"
            + "    order by 1,2,3,4");

        expColNames = new String [] {"PART", "NUM", "PRODUCT", "NUM"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Blades", "205", "Generator", "10"},
            {"Blades", "205", "Hammer", "50"},
            {"Blades", "205", "Relay", "30"},
            {"Blades", "205", "Ruler", "30"},
            {"Blades", "205", "Sander", "20"},
            {"Blades", "205", "Saw", "205"},
            {"Blades", "205", "Screwdriver", "505"},
            {"Blades", "205", null, "20"},
            {"Magnets", "10", "Generator", "10"},
            {"Magnets", "10", "Hammer", "50"},
            {"Magnets", "10", "Relay", "30"},
            {"Magnets", "10", "Ruler", "30"},
            {"Magnets", "10", "Sander", "20"},
            {"Magnets", "10", "Saw", "205"},
            {"Magnets", "10", "Screwdriver", "505"},
            {"Magnets", "10", null, "20"},
            {"Oil", "160", "Generator", "10"},
            {"Oil", "160", "Hammer", "50"},
            {"Oil", "160", "Relay", "30"},
            {"Oil", "160", "Ruler", "30"},
            {"Oil", "160", "Sander", "20"},
            {"Oil", "160", "Saw", "205"},
            {"Oil", "160", "Screwdriver", "505"},
            {"Oil", "160", null, "20"},
            {"Paper", "20", "Generator", "10"},
            {"Paper", "20", "Hammer", "50"},
            {"Paper", "20", "Relay", "30"},
            {"Paper", "20", "Ruler", "30"},
            {"Paper", "20", "Sander", "20"},
            {"Paper", "20", "Saw", "205"},
            {"Paper", "20", "Screwdriver", "505"},
            {"Paper", "20", null, "20"},
            {"Plastic", "30", "Generator", "10"},
            {"Plastic", "30", "Hammer", "50"},
            {"Plastic", "30", "Relay", "30"},
            {"Plastic", "30", "Ruler", "30"},
            {"Plastic", "30", "Sander", "20"},
            {"Plastic", "30", "Saw", "205"},
            {"Plastic", "30", "Screwdriver", "505"},
            {"Plastic", "30", null, "20"},
            {"Steel", "30", "Generator", "10"},
            {"Steel", "30", "Hammer", "50"},
            {"Steel", "30", "Relay", "30"},
            {"Steel", "30", "Ruler", "30"},
            {"Steel", "30", "Sander", "20"},
            {"Steel", "30", "Saw", "205"},
            {"Steel", "30", "Screwdriver", "505"},
            {"Steel", "30", null, "20"},
            {"Wire", "10", "Generator", "10"},
            {"Wire", "10", "Hammer", "50"},
            {"Wire", "10", "Relay", "30"},
            {"Wire", "10", "Ruler", "30"},
            {"Wire", "10", "Sander", "20"},
            {"Wire", "10", "Saw", "205"},
            {"Wire", "10", "Screwdriver", "505"},
            {"Wire", "10", null, "20"},
            {null, "30", "Generator", "10"},
            {null, "30", "Hammer", "50"},
            {null, "30", "Relay", "30"},
            {null, "30", "Ruler", "30"},
            {null, "30", "Sander", "20"},
            {null, "30", "Saw", "205"},
            {null, "30", "Screwdriver", "505"},
            {null, "30", null, "20"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // coj204.clp
        // Creating a table
        //------------------------------------------------------------

        st.executeUpdate(
            "create table k55admin.stru ("
            + "             cd_plant           varchar(5) not null,"
            + "             in_reseq           varchar(1) not null,"
            + "             no_level           integer    not null,"
            + "             no_part_base_nxt_a varchar(9) not null,"
            + "             no_part_pref_nxt_a varchar(7) not null,"
            + "             no_part_suff_nxt_a varchar(8) not null,"
            + "             no_part_cntl_nxt_a varchar(3) not null,"
            + "             no_seq             integer    not null,"
            + "             no_part_base       varchar(9) not null,"
            + "             no_part_prefix     varchar(7) not null,"
            + "             no_part_suffix     varchar(8) not null,"
            + "             no_part_control    varchar(3) not null)");

        //------------------------------------------------------------
        // Start of loading data into table
        //------------------------------------------------------------

        st.executeUpdate(
            "insert into k55admin.stru "
            + "values('EE01A','N',0,'','','','',0,'BLPO','R','','')");

        st.executeUpdate(
            " insert into k55admin.stru "
            + "values('EE01A','N',1,'BLPO','R','','',30,'M10A63','R"
            + "','AH','')");

        st.executeUpdate(
            " insert into k55admin.stru "
            + "values('EE01A','N',1,'BLPO','R','','',40,'M10A55','R"
            + "','AH','')");

        st.executeUpdate(
            " insert into k55admin.stru "
            + "values('EE01A','N',1,'BLPO','R','','',51,'M10A61','R"
            + "','AH','')");

        st.executeUpdate(
            " insert into k55admin.stru "
            + "values('EE01A','N',1,'BLPO','R','','',55,'STH1008','"
            + "R','','')");

        st.executeUpdate(
            " insert into k55admin.stru "
            + "values('EE01A','N',1,'BLPO','R','','',61,'M99G124','"
            + "R','AH','')");

        st.executeUpdate(
            " insert into k55admin.stru "
            + "values('EE01A','N',1,'BLPO','R','','',71,'STH1050','"
            + "R','','')");

        st.executeUpdate(
            " insert into k55admin.stru "
            + "values('EE01A','N',1,'BLPO','R','','',80,'PMIC3','R','','')");

        st.executeUpdate(
            " insert into k55admin.stru "
            + "values('EE01A','N',1,'BLPO','R','','',90,'M99G24','R','','')");

        st.executeUpdate(
            " insert into k55admin.stru "
            + "values('EE01A','N',1,'BLPO','R','','',100,'M10A57','"
            + "R','BH','')");

        st.executeUpdate(
            " insert into k55admin.stru "
            + "values('EE01A','N',4,'9A937','D9AE','AA','',1,'9A301"
            + "','RC5OF','A','')");

        st.executeUpdate(
            " insert into k55admin.stru "
            + "values('EE01A','N',2,'9509','E2DE','UA','',130,'9J54"
            + "7','D9AE','CA','')");

        st.executeUpdate(
            " insert into k55admin.stru "
            + "values('EE01A','N',2,'9509','E2DE','UA','',140,'9J56"
            + "3','E0AE','BA','')");

        st.executeUpdate(
            " insert into k55admin.stru "
            + "values('EE01A','N',2,'9509','E2DE','UA','',141,'3853"
            + "38','','S2','')");

        st.executeUpdate(
            " insert into k55admin.stru "
            + "values('EE01A','N',2,'9509','E2DE','UA','',150,'3871"
            + "55','R','S','')");

        st.executeUpdate(
            " insert into k55admin.stru "
            + "values('EE01A','N',2,'9509','E2DE','UA','',160,'9E95"
            + "7','E2AE','P82','')");

        st.executeUpdate(
            " insert into k55admin.stru "
            + "values('EE01A','N',3,'9E957','E2AE','P82','',1,'9E95"
            + "7','E2AE','BB','')");

        st.executeUpdate(
            " insert into k55admin.stru "
            + "values('EE01A','N',3,'9E957','E2AE','P82','',2,'9S55"
            + "5','E37E','CA','')");

        st.executeUpdate(
            " insert into k55admin.stru "
            + "values('EE01A','N',4,'9S555','E37E','CA','',1,'9S555"
            + "','RE2AE','BA','')");

        st.executeUpdate(
            " insert into k55admin.stru "
            + "values('EE01A','N',3,'9E957','E2AE','P82','',3,'9S55"
            + "4','D2AF','DA','')");

        st.executeUpdate(
            " insert into k55admin.stru "
            + "values('EE01A','N',5,'10304','E9DF','AA','',20,'N806"
            + "017','','S40G','')");

        st.executeUpdate(
            " insert into k55admin.stru "
            + "values('EE01A','N',5,'10304','E9DF','AA','',35,'10A3"
            + "18','PE2AF','BA','')");

        st.executeUpdate(
            " insert into k55admin.stru "
            + "values('EE01A','N',6,'10A318','PE2AF','BA','',10,'10"
            + "A318','E2AF','BA','')");

        st.executeUpdate(
            " insert into k55admin.stru "
            + "values('EE01A','N',7,'10A318','E2AF','BA','',10,'10A"
            + "318','RE2HF','AA','')");

        st.executeUpdate(
            " insert into k55admin.stru "
            + "values('EE01A','N',7,'10A318','E2AF','BA','',20,'48Z"
            + "P42842','','','')");

        st.executeUpdate(
            " insert into k55admin.stru "
            + "values('EE01A','N',5,'10304','E9DF','AA','',40,'10A3"
            + "19','PD5VF','AA','')");

        st.executeUpdate(
            " insert into k55admin.stru "
            + "values('EE01A','N',6,'10A319','PD5VF','AA','',10,'10"
            + "A319','D5VF','AA','')");

        st.executeUpdate(
            " insert into k55admin.stru "
            + "values('EE01A','N',7,'10A319','D5VF','AA','',10,'10A"
            + "319','RD5VF','AA','')");

        st.executeUpdate(
            " insert into k55admin.stru "
            + "values('EE01A','N',5,'10304','E9DF','AA','',50,'10B3"
            + "01','D2OF','AA','')");

        st.executeUpdate(
            " insert into k55admin.stru "
            + "values('EE01A','N',5,'10304','E9DF','AA','',65,'3847"
            + "58','','S','')");

        st.executeUpdate(
            " insert into k55admin.stru "
            + "values('EE01A','N',4,'9E950','E6AE','AA','',1,'9E950"
            + "','RE3EE','BR','')");

        st.executeUpdate(
            " insert into k55admin.stru "
            + "values('EE01A','N',2,'9E926','E8DE','FAIP','',61,'80"
            + "4064','N','S2','')");

        st.executeUpdate(
            " insert into k55admin.stru "
            + "values('EE01A','N',2,'9E926','E8DE','FAIP','',70,'80"
            + "3853','N','S100','')");

        st.executeUpdate(
            " insert into k55admin.stru "
            + "values('EE01A','N',2,'9E926','E8DE','FAIP','',80,'9A"
            + "776','E6AE','DF','')");

        st.executeUpdate(
            " insert into k55admin.stru "
            + "values('EE01A','N',3,'9A776','E6AE','DF','',1,'9A794"
            + "','E6AE','BD1','')");

        st.executeUpdate(
            " insert into k55admin.stru "
            + "values('EE01A','N',4,'9A794','E6AE','BD1','',1,'9A79"
            + "4','PE6AE','BD','')");

        st.executeUpdate(
            " insert into k55admin.stru "
            + "values('EE01A','N',5,'9A794','PE6AE','BD','',1,'9A79"
            + "4','E6AE','BD','')");

        st.executeUpdate(
            " insert into k55admin.stru "
            + "values('EE01A','N',6,'9A794','E6AE','BD','',1,'9A794"
            + "','RE69E','BB','')");

        st.executeUpdate(
            " insert into k55admin.stru "
            + "values('EE01A','N',4,'9A794','E6AE','BD1','',2,'9E55"
            + "1','PE0ZE','AA','')");

        st.executeUpdate(
            " insert into k55admin.stru "
            + "values('EE01A','N',5,'9E551','PE0ZE','AA','',1,'9E55"
            + "1','HE0ZE','AA','')");

        st.executeUpdate(
            " insert into k55admin.stru "
            + "values('EE01B','N',7,'9533','HD7ZE','AA','',1,'9533'"
            + ",'D7ZE','AA','')");

        st.executeUpdate(
            " insert into k55admin.stru "
            + "values('EE01B','N',8,'9533','D7ZE','AA','',1,'9533',"
            + "'RD7ZE','AA','')");

        st.executeUpdate(
            " insert into k55admin.stru "
            + "values('EE01B','N',4,'9E509','D9ZE','BE1','',40,'9A5"
            + "74','D1AF','FA','')");

        st.executeUpdate(
            " insert into k55admin.stru "
            + "values('EE01B','N',5,'9A574','D1AF','FA','',1,'9A521"
            + "','RC1AE','B','')");

        st.executeUpdate(
            " insert into k55admin.stru "
            + "values('EE01B','N',4,'9E509','D9ZE','BE1','',50,'390"
            + "468','','S','')");

        st.executeUpdate(
            " insert into k55admin.stru "
            + "values('EE01B','N',4,'9E509','D9ZE','BE1','',60,'9A5"
            + "21','D0AF','AT','')");

        st.executeUpdate(
            " insert into k55admin.stru "
            + "values('EE01B','N',5,'9A521','D0AF','AT','',1,'9A521"
            + "','RC1AE','B','')");

        st.executeUpdate(
            " insert into k55admin.stru "
            + "values('EE01B','N',4,'9E509','D9ZE','BE1','',70,'390"
            + "468','','S','')");

        st.executeUpdate(
            " insert into k55admin.stru "
            + "values('EE01B','N',4,'9E509','D9ZE','BE1','',80,'9A5"
            + "21','C9AF','AA','')");

        st.executeUpdate(
            " insert into k55admin.stru "
            + "values('EE01B','N',5,'9A521','C9AF','AA','',1,'9A521"
            + "','RD0AF','CV','')");

        st.executeUpdate(
            " insert into k55admin.stru "
            + "values('EE01B','N',4,'9C511','E3ZE','AA','',1,'9C511"
            + "','RE3ZE','AA','')");

        st.executeUpdate(
            " insert into k55admin.stru "
            + "values('EE01B','N',3,'9B559','E3ZE','AA','',60,'9834"
            + "','E3TE','AA','')");

        st.executeUpdate(
            " insert into k55admin.stru "
            + "values('EE01B','N',3,'9B559','E3ZE','AA','',70,'9B55"
            + "1','E3ZE','AA','')");

        st.executeUpdate(
            " insert into k55admin.stru "
            + "values('EE01B','N',4,'9B551','E3ZE','AA','',1,'9B551"
            + "','RE3ZE','AA','')");

        st.executeUpdate(
            " insert into k55admin.stru "
            + "values('EE01B','N',3,'9B559','E3ZE','AA','',80,'9934"
            + "','E3ZE','AA','')");

        st.executeUpdate(
            " insert into k55admin.stru "
            + "values('EE01B','N',2,'9509','E4TE','AMA','',370,'9D5"
            + "87','E3TE','ABP1','')");

        st.executeUpdate(
            " insert into k55admin.stru "
            + "values('EE01B','N',3,'9D587','E3TE','ABP1','',10,'9D"
            + "587','E3TE','AB','')");

        st.executeUpdate(
            " insert into k55admin.stru "
            + "values('EE01B','N',4,'9D587','E3TE','AB','',1,'9D587"
            + "','','5','')");

        st.executeUpdate(
            " insert into k55admin.stru "
            + "values('EE01B','N',5,'9D587','','5','',1,'SAE303','R"
            + "','M','')");

        st.executeUpdate(
            " insert into k55admin.stru "
            + "values('EE01B','N',3,'9D587','E3TE','ABP1','',20,'95"
            + "29','E3AE','AA','')");

        st.executeUpdate(
            " insert into k55admin.stru "
            + "values('EE01B','N',5,'17B517','E59F','AA','',20,'17C"
            + "526','E0TF','AA','')");

        st.executeUpdate(
            " insert into k55admin.stru "
            + "values('EE01B','N',5,'17B517','E59F','AA','',30,'17A"
            + "530','E0TF','AA','')");

        st.executeUpdate(
            " insert into k55admin.stru "
            + "values('EE01B','N',5,'17B517','E59F','AA','',40,'17B"
            + "559','E59F','AA','')");

        st.executeUpdate(
            " insert into k55admin.stru "
            + "values('EE01B','N',4,'17B443','E59F','AA','',35,'17B"
            + "583','E2TF','AA','')");

        st.executeUpdate(
            " insert into k55admin.stru "
            + "values('EE01B','N',4,'17B443','E59F','AA','',40,'637"
            + "57','','S7','')");

        st.executeUpdate(
            " insert into k55admin.stru "
            + "values('EE01B','N',4,'17B443','E59F','AA','',50,'17A"
            + "482','E0TF','AA','')");

        st.executeUpdate(
            " insert into k55admin.stru "
            + "values('EE01B','N',2,'17508','E69F','AA','',30,'17A4"
            + "25','E59F','AA','')");

        st.executeUpdate(
            " insert into k55admin.stru "
            + "values('EE01B','N',2,'17508','E69F','AA','',50,'17B5"
            + "84','E59F','AB','')");

        st.executeUpdate(
            " insert into k55admin.stru "
            + "values('EE01B','N',3,'17B584','E59F','AB','',10,'17B"
            + "558','E5TF','AA','')");

        st.executeUpdate(
            " insert into k55admin.stru "
            + "values('EE01B','N',3,'17B584','E59F','AB','',20,'17C"
            + "449','E0TF','AA','')");

        st.executeUpdate(
            " insert into k55admin.stru "
            + "values('EE01D','N',3,'9512','D5TE','EA1','',60,'9996"
            + "','D5AE','CB','')");

        st.executeUpdate(
            " insert into k55admin.stru "
            + "values('EE01D','N',3,'9512','D5TE','EA1','',70,'6B60"
            + "8','D3AF','AA','')");

        st.executeUpdate(
            " insert into k55admin.stru "
            + "values('EE01D','N',2,'9509','D4PE','BGA','',20,'9A52"
            + "1','C9AF','AK','')");

        st.executeUpdate(
            " insert into k55admin.stru "
            + "values('EE01D','N',3,'9A521','C9AF','AK','',1,'9A521"
            + "','RD0AF','CV','')");

        st.executeUpdate(
            " insert into k55admin.stru "
            + "values('EE01D','N',2,'9509','D4PE','BGA','',30,'9A52"
            + "1','E3AE','BA','')");

        st.executeUpdate(
            " insert into k55admin.stru "
            + "values('EE01D','N',2,'9509','D4PE','BGA','',40,'9576"
            + "','E3ZE','AA','')");

        st.executeUpdate(
            " insert into k55admin.stru "
            + "values('EE01D','N',2,'9509','D4PE','BGA','',50,'9A56"
            + "5','D5DE','BA','')");

        st.executeUpdate(
            " insert into k55admin.stru "
            + "values('EE01D','N',3,'9A565','D5DE','BA','',10,'9E50"
            + "1','D5AE','AA','')");

        st.executeUpdate(
            " insert into k55admin.stru "
            + "values('EE01D','N',4,'9E501','D5AE','AA','',1,'9E501"
            + "','5','2V5','')");

        st.executeUpdate(
            " insert into k55admin.stru "
            + "values('EE01D','N',5,'9E501','5','2V5','',1,'SAE903'"
            + ",'R','','')");

        st.executeUpdate(
            " insert into k55admin.stru "
            + "values('EE01D','N',3,'9512','E2ZE','MA4','',10,'9513"
            + "','E2ZE','MAPE','')");

        st.executeUpdate(
            " insert into k55admin.stru "
            + "values('EE01D','N',4,'9513','E2ZE','MAPE','',1,'9513"
            + "','','2V136','')");

        st.executeUpdate(
            " insert into k55admin.stru "
            + "values('EE01D','N',5,'9513','','2V136','',1,'SAE303'"
            + ",'R','M','')");

        st.executeUpdate(
            " insert into k55admin.stru "
            + "values('EE01D','N',3,'9512','E2ZE','MA4','',11,'M4G2"
            + "38','RESH','A','')");

        st.executeUpdate(
            " insert into k55admin.stru "
            + "values('EE01D','N',3,'9512','E2ZE','MA4','',12,'35',"
            + "'R14','760','')");

        st.executeUpdate(
            " insert into k55admin.stru "
            + "values('EE01D','N',3,'9512','E2ZE','MA4','',20,'9A52"
            + "1','D0AF','FD','')");

        st.executeUpdate(
            " insert into k55admin.stru "
            + "values('EE01D','N',4,'9A521','D0AF','FD','',1,'9A521"
            + "','RD0AF','CV','')");

        st.executeUpdate(
            " insert into k55admin.stru "
            + "values('EE01D','N',3,'9512','E2ZE','MA4','',30,'9581"
            + "','E2ZE','CA','')");

        st.executeUpdate(
            " insert into k55admin.stru "
            + "values('EE01D','N',4,'9581','E2ZE','CA','',1,'9582',"
            + "'PD7AE','CA','')");

        st.executeUpdate(
            " insert into k55admin.stru "
            + "values('EE01D','N',5,'9582','PD7AE','CA','',1,'9582'"
            + ",'D7AE','CA','')");

        st.executeUpdate(
            " insert into k55admin.stru "
            + "values('EE01D','N',5,'10379','E7AF','AA1','',10,'103"
            + "79','E7AF','AA','')");

        st.executeUpdate(
            " insert into k55admin.stru "
            + "values('EE01D','N',3,'10335','E7HF','AA','',55,'1032"
            + "8','E5AF','AA','')");

        st.executeUpdate(
            " insert into k55admin.stru "
            + "values('EE01D','N',3,'10335','E7HF','AA','',60,'10B3"
            + "02','E7HF','AA','')");

        st.executeUpdate(
            " insert into k55admin.stru "
            + "values('EE01D','N',4,'10B302','E7HF','AA','',20,'10B"
            + "302','','2','')");

        st.executeUpdate(
            " insert into k55admin.stru "
            + "values('EE01D','N',5,'10B302','','2','',10,'SAE303',"
            + "'R','M','')");

        st.executeUpdate(
            " insert into k55admin.stru "
            + "values('EE01D','N',2,'10C335','E2HF','AA','',30,'351"
            + "124','','S36','')");

        st.executeUpdate(
            " insert into k55admin.stru "
            + "values('EE01D','N',2,'10C335','E2HF','AA','',35,'391"
            + "042','','S2','')");

        st.executeUpdate(
            " insert into k55admin.stru "
            + "values('EE01D','N',2,'10C335','E2HF','AA','',40,'389"
            + "767','','S36','')");

        st.executeUpdate(
            " insert into k55admin.stru "
            + "values('EE01D','N',2,'10C335','E2HF','AA','',50,'375"
            + "026','','S36','')");

        st.executeUpdate(
            " insert into k55admin.stru "
            + "values('EE01D','N',2,'10C335','E2HF','AA','',60,'103"
            + "43','D0AF','A','')");

        //
        //------------------------------------------------------------
        // test unit 1. OJ in a scalar subquery in WHERE clause
        //              inequality predicate in ON clause and a WHERE clause
        //------------------------------------------------------------

        // 101 - LEFT JOIN in a scalar subquery in WHERE clause

        rs = st.executeQuery(
            "select cd_plant, no_part_base_nxt_a"
            + " from k55admin.stru"
            + " where cd_plant = ( select  distinct b.cd_plant"
            + "                   from k55admin.stru a left join "
            + "k55admin.stru b"
            + "                   on a.no_level = b.no_level and "
            + "b.no_level>7"
            + "                   where a.cd_plant = 'EE01B' and "
            + "b.cd_plant is not null"
            + "                 )"
            + "      and no_level < 3");

        expColNames = new String [] {"CD_PLANT", "NO_PART_BASE_NXT_A"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"EE01B", "9509"},
            {"EE01B", "17508"},
            {"EE01B", "17508"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 102 - RIGHT JOIN in a scalar subquery in WHERE clause

        rs = st.executeQuery(
            "select cd_plant, no_part_base_nxt_a"
            + " from k55admin.stru"
            + " where cd_plant = ( select  distinct b.cd_plant"
            + "                   from k55admin.stru a right join "
            + "k55admin.stru b"
            + "                   on a.no_level = b.no_level and "
            + "b.no_level>7"
            + "                   where a.cd_plant = 'EE01B' and "
            + "b.cd_plant is not null"
            + "                 )"
            + "      and no_level < 3");

        expColNames = new String [] {"CD_PLANT", "NO_PART_BASE_NXT_A"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"EE01B", "9509"},
            {"EE01B", "17508"},
            {"EE01B", "17508"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        //
        //------------------------------------------------------------
        // test unit 3. OJ in a correlated scalar subquery in WHERE clause
        //              an equality predicate in ON clause and a WHERE clause;
        //------------------------------------------------------------

        // 301 - LEFT JOIN in a correlated scalar
        //       subquery in WHERE clause
        //       correlation to the WHERE clause of the outerjoin subquery

        rs = st.executeQuery(
            "select cd_plant, no_level, no_seq"
            + " from k55admin.stru t"
            + " where cd_plant = ( select  distinct a.cd_plant"
            + "                   from k55admin.stru a left join "
            + "k55admin.stru b"
            + "                   on a.no_level = b.no_level"
            + "                   where b.no_level = t.no_seq"
            + "                 )"
            + " and  no_part_base = 'BLPO'");

        expColNames = new String [] {"CD_PLANT", "NO_LEVEL", "NO_SEQ"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"EE01A", "0", "0"}
        };

        JDBC.assertFullResultSet(rs, expRS);
    }


    private void part_iii(Statement st) throws SQLException {
        ResultSet rs = null;

        String [][] expRS;
        String [] expColNames;

        // 302 - RIGHT JOIN in a correlated scalar subquery in
        //       WHERE clause
        //       correlation to the WHERE clause of the outerjoin subquery

        rs = st.executeQuery(
            "select cd_plant, no_level, no_seq"
            + " from k55admin.stru t"
            + " where cd_plant = ( select  distinct a.cd_plant"
            + "                   from k55admin.stru a right join "
            + "k55admin.stru b"
            + "                   on a.no_level = b.no_level"
            + "                   where b.no_level = t.no_seq"
            + "                 )"
            + " and  no_part_base = 'BLPO'");

        expColNames = new String [] {"CD_PLANT", "NO_LEVEL", "NO_SEQ"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"EE01A", "0", "0"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 303 - INNER JOIN in a correlated scalar subquery in
        //       WHERE clause
        //       correlation to the WHERE clause of the outerjoin subquery;

        rs = st.executeQuery(
            "select cd_plant, no_level, no_seq"
            + " from k55admin.stru t"
            + " where cd_plant = ( select  distinct a.cd_plant"
            + "                   from k55admin.stru a inner join "
            + "k55admin.stru b"
            + "                   on a.no_level = b.no_level"
            + "                   where b.no_level = t.no_seq"
            + "                 )"
            + " and  no_part_base = 'BLPO'");

        expColNames = new String [] {"CD_PLANT", "NO_LEVEL", "NO_SEQ"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"EE01A", "0", "0"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        //
        //------------------------------------------------------------
        // test unit 4. OJ in a scalar subquery in the SELECT list;
        //------------------------------------------------------------

        // 401 - LEFT JOIN in a scalar subquery in the SELECT list

        rs = st.executeQuery(
            "select no_level,"
            + "       ( select  distinct a.cd_plant"
            + "         from k55admin.stru a left join k55admin.stru b"
            + "         on a.no_level = b.no_level"
            + "         where a.no_level = 0"
            + "       )"
            + " from k55admin.stru"
            + " where no_seq <= 4 and no_level < 2");

        expColNames = new String [] {"NO_LEVEL", "2"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"0", "EE01A"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 402 - RIGHT JOIN in a scalar subquery in the SELECT list

        rs = st.executeQuery(
            "select no_level,"
            + "       ( select  distinct a.cd_plant"
            + "         from k55admin.stru a right join k55admin.stru b"
            + "         on a.no_level = b.no_level"
            + "         where a.no_level = 0"
            + "       )"
            + " from k55admin.stru"
            + " where no_seq <= 4 and no_level < 2");

        expColNames = new String [] {"NO_LEVEL", "2"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"0", "EE01A"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        //
        //------------------------------------------------------------
        // test unit 6. OJ in a nested scalar subquery, no correlation
        //              inequality predicate in the ON clause and WHERE clause
        //------------------------------------------------------------

        // 601 - LEFT JOIN in a nested scalar subquery, no correlation

        rs = st.executeQuery(
            "select cd_plant, no_part_base_nxt_a"
            + " from k55admin.stru"
            + " where cd_plant = ( select  distinct b.cd_plant"
            + "                   from k55admin.stru a left join "
            + "k55admin.stru b"
            + "                   on a.no_level = b.no_level and "
            + "b.no_level>7"
            + "                   where a.cd_plant = 'EE01B' and "
            + "b.cd_plant is not null and"
            + "                         b.cd_plant = ( select  "
            + "distinct b.cd_plant"
            + "                                        from "
            + "k55admin.stru a left join k55admin.stru b"
            + "                                        on "
            + "a.no_level = b.no_level and b.no_level>7"
            + "                                        where "
            + "a.cd_plant = 'EE01B' and b.cd_plant is not null)"
            + "                 )"
            + "      and no_level < 3");

        expColNames = new String [] {"CD_PLANT", "NO_PART_BASE_NXT_A"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"EE01B", "9509"},
            {"EE01B", "17508"},
            {"EE01B", "17508"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 602 - RIGHT JOIN in a nested scalar subquery, no
        // correlation

        rs = st.executeQuery(
            "select cd_plant, no_part_base_nxt_a"
            + " from k55admin.stru"
            + " where cd_plant = ( select  distinct b.cd_plant"
            + "                   from k55admin.stru a right join "
            + "k55admin.stru b"
            + "                   on a.no_level = b.no_level and "
            + "b.no_level>7"
            + "                   where a.cd_plant = 'EE01B' and "
            + "b.cd_plant is not null"
            + "                    and  b.cd_plant = ( select  "
            + "distinct b.cd_plant"
            + "                                        from "
            + "k55admin.stru a right join k55admin.stru b"
            + "                                        on "
            + "a.no_level = b.no_level and b.no_level>7"
            + "                                        where "
            + "a.cd_plant = 'EE01B' and b.cd_plant is not null)"
            + "                 )"
            + "      and no_level < 3");

        expColNames = new String [] {"CD_PLANT", "NO_PART_BASE_NXT_A"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"EE01B", "9509"},
            {"EE01B", "17508"},
            {"EE01B", "17508"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        //
        //------------------------------------------------------------
        // test unit 7. OJ in a scalar subquery in an INLIST, no correlation
        //              inequality predicate in the ON clause and WHERE clause
        //------------------------------------------------------------

        // 701 - LEFT JOIN in a scalar subquery in an INLIST, no correlation

        rs = st.executeQuery(
            "select *"
            + " from k55admin.stru"
            + " where cd_plant in ('EE01B', ( select distinct b.cd_plant"
            + "                              from k55admin.stru a "
            + "left join k55admin.stru b"
            + "                              on a.no_level = "
            + "b.no_level and b.no_level>7"
            + "                              where a.cd_plant = "
            + "'EE01B' and b.cd_plant is not null )"
            + "                            )"
            + "      and no_level = 7");

        expColNames = new String [] {"CD_PLANT", "IN_RESEQ", "NO_LEVEL", "NO_PART_BASE_NXT_A", "NO_PART_PREF_NXT_A", "NO_PART_SUFF_NXT_A", "NO_PART_CNTL_NXT_A", "NO_SEQ", "NO_PART_BASE", "NO_PART_PREFIX", "NO_PART_SUFFIX", "NO_PART_CONTROL"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"EE01B", "N", "7", "9533", "HD7ZE", "AA", "", "1", "9533", "D7ZE", "AA", ""}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 702 - RIGHT JOIN in a scalar subquery in an INLIST, no
        //       correlation

        rs = st.executeQuery(
            "select *"
            + " from k55admin.stru"
            + " where cd_plant in ('EE01B', ( select distinct b.cd_plant"
            + "                              from k55admin.stru a "
            + "right join k55admin.stru b"
            + "                              on a.no_level = "
            + "b.no_level and b.no_level>7"
            + "                              where a.cd_plant = "
            + "'EE01B' and b.cd_plant is not null )"
            + "                            )"
            + "      and no_level = 7");

        expColNames = new String [] {"CD_PLANT", "IN_RESEQ", "NO_LEVEL", "NO_PART_BASE_NXT_A", "NO_PART_PREF_NXT_A", "NO_PART_SUFF_NXT_A", "NO_PART_CNTL_NXT_A", "NO_SEQ", "NO_PART_BASE", "NO_PART_PREFIX", "NO_PART_SUFFIX", "NO_PART_CONTROL"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"EE01B", "N", "7", "9533", "HD7ZE", "AA", "", "1", "9533", "D7ZE", "AA", ""}
        };

        JDBC.assertFullResultSet(rs, expRS);

        //
        //------------------------------------------------------------
        // test unit 8. OJ in an EXISTS predicate subquery, no correlation
        //              inequality predicate in the ON clause and WHERE clause;
        //------------------------------------------------------------

        // 801 - LEFT JOIN in an EXISTS predicate subquery w/ distinct, no
        //       correlation;

        rs = st.executeQuery(
            "select distinct cd_plant, no_part_base_nxt_a"
            + " from k55admin.stru"
            + " where exists ( select distinct b.cd_plant"
            + "               from k55admin.stru a left join "
            + "k55admin.stru b"
            + "               on a.no_level = b.no_level and b.no_level>7"
            + "               where a.cd_plant = 'EE01B' and "
            + "b.cd_plant is not null )"
            + "      and no_level < 2");

        expColNames = new String [] {"CD_PLANT", "NO_PART_BASE_NXT_A"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"EE01A", ""},
            {"EE01A", "BLPO"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 802 - LEFT JOIN in an EXISTS predicate subquery, no
        //       correlation

        rs = st.executeQuery(
            "select distinct cd_plant, no_part_base_nxt_a"
            + " from k55admin.stru"
            + " where exists ( select b.cd_plant"
            + "               from k55admin.stru a left join "
            + "k55admin.stru b"
            + "               on a.no_level = b.no_level and b.no_level>7)"
            + "      and no_level < 2");

        expColNames = new String [] {"CD_PLANT", "NO_PART_BASE_NXT_A"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"EE01A", ""},
            {"EE01A", "BLPO"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 803 - RIGHT JOIN in an EXISTS predicate subquery w/
        //       distinct, no correlation

        rs = st.executeQuery(
            "select distinct cd_plant, no_part_base_nxt_a"
            + " from k55admin.stru"
            + " where exists ( select distinct b.cd_plant"
            + "               from k55admin.stru a right join "
            + "k55admin.stru b"
            + "               on a.no_level = b.no_level and b.no_level>7"
            + "               where a.cd_plant = 'EE01B' and "
            + "b.cd_plant is not null )"
            + "      and no_level < 2");

        expColNames = new String [] {"CD_PLANT", "NO_PART_BASE_NXT_A"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"EE01A", ""},
            {"EE01A", "BLPO"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 804 - RIGHT JOIN in an EXISTS predicate subquery, no
        //       correlation

        rs = st.executeQuery(
            "select distinct cd_plant, no_part_base_nxt_a"
            + " from k55admin.stru"
            + " where exists ( select b.cd_plant"
            + "               from k55admin.stru a RIGHT join "
            + "k55admin.stru b"
            + "               on a.no_level = b.no_level and b.no_level>7)"
            + "      and no_level < 2");

        expColNames = new String [] {"CD_PLANT", "NO_PART_BASE_NXT_A"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"EE01A", ""},
            {"EE01A", "BLPO"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 805 - LEFT JOIN in a correlated subquery in EXISTS predicate
        //       correlation to the WHERE clause of the outerjoin subquery

        rs = st.executeQuery(
            "select cd_plant, no_level, no_seq"
            + " from k55admin.stru t"
            + " where exists ( select  distinct a.cd_plant"
            + "                   from k55admin.stru a LEFT join "
            + "k55admin.stru b"
            + "                   on a.no_level = b.no_level"
            + "                   where b.no_level = t.no_seq"
            + "                 )"
            + " and  no_part_base = 'BLPO'");

        expColNames = new String [] {"CD_PLANT", "NO_LEVEL", "NO_SEQ"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"EE01A", "0", "0"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 806 - RIGHT JOIN in a correlated subquery in EXISTS predicate
        //       correlation to the WHERE clause of the outerjoin subquery;

        rs = st.executeQuery(
            "select cd_plant, no_level, no_seq"
            + " from k55admin.stru t"
            + " where exists ( select  distinct a.cd_plant"
            + "                   from k55admin.stru a right join "
            + "k55admin.stru b"
            + "                   on a.no_level = b.no_level"
            + "                   where b.no_level = t.no_seq"
            + "                 )"
            + " and  no_part_base = 'BLPO'");

        expColNames = new String [] {"CD_PLANT", "NO_LEVEL", "NO_SEQ"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"EE01A", "0", "0"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 807 - LEFT JOIN in an IN subquery, no correlation

        rs = st.executeQuery(
            "select cd_plant, no_part_base_nxt_a"
            + " from k55admin.stru"
            + " where cd_plant in ( select b.cd_plant"
            + "                   from k55admin.stru a left join "
            + "k55admin.stru b"
            + "                   on a.no_level = b.no_level and "
            + "b.no_level>7"
            + "                   where a.cd_plant = 'EE01B'"
            + "                 )"
            + "      and no_level < 3");

        expColNames = new String [] {"CD_PLANT", "NO_PART_BASE_NXT_A"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"EE01B", "9509"},
            {"EE01B", "17508"},
            {"EE01B", "17508"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 808 - RIGHT JOIN in an IN subquery, no correlation

        rs = st.executeQuery(
            "select cd_plant, no_part_base_nxt_a"
            + " from k55admin.stru"
            + " where cd_plant in ( select b.cd_plant"
            + "                   from k55admin.stru a right join "
            + "k55admin.stru b"
            + "                   on a.no_level = b.no_level and "
            + "b.no_level>7"
            + "                   where a.cd_plant = 'EE01B'"
            + "                 )"
            + "      and no_level < 3");

        expColNames = new String [] {"CD_PLANT", "NO_PART_BASE_NXT_A"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"EE01B", "9509"},
            {"EE01B", "17508"},
            {"EE01B", "17508"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 809 - INNER JOIN in an IN subquery, no correlation

        rs = st.executeQuery(
            "select cd_plant, no_part_base_nxt_a"
            + " from k55admin.stru"
            + " where cd_plant in ( select b.cd_plant"
            + "                   from k55admin.stru a join k55admin.stru b"
            + "                   on a.no_level = b.no_level and "
            + "b.no_level>7"
            + "                   where a.cd_plant = 'EE01B'"
            + "                 )"
            + "      and no_level < 3");

        expColNames = new String [] {"CD_PLANT", "NO_PART_BASE_NXT_A"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"EE01B", "9509"},
            {"EE01B", "17508"},
            {"EE01B", "17508"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 810 - LEFT JOIN in a correlated in subquery;
        //       correlation to the WHERE clause of the outerjoin subquery

        rs = st.executeQuery(
            "select cd_plant, no_level, no_seq"
            + " from k55admin.stru t"
            + " where cd_plant in ( select  distinct a.cd_plant"
            + "                    from k55admin.stru a LEFT join "
            + "k55admin.stru b"
            + "                    on a.no_level = b.no_level and "
            + "b.no_level>0"
            + "                    where b.no_level = t.no_seq"
            + "                 )"
            + "order by no_level");

        expColNames = new String [] {"CD_PLANT", "NO_LEVEL", "NO_SEQ"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"EE01A", "3", "1"},
            {"EE01A", "3", "3"},
            {"EE01A", "3", "2"},
            {"EE01A", "3", "1"},
            {"EE01A", "4", "2"},
            {"EE01A", "4", "1"},
            {"EE01A", "4", "1"},
            {"EE01A", "4", "1"},
            {"EE01A", "4", "1"},
            {"EE01A", "5", "1"},
            {"EE01A", "5", "1"},
            {"EE01A", "6", "1"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 811 - RIGHT JOIN in a correlated in subquery;
        //       correlation to the WHERE clause of the outerjoin subquery

        rs = st.executeQuery(
            "select cd_plant, no_level, no_seq"
            + " from k55admin.stru t"
            + " where cd_plant in  ( select  distinct a.cd_plant"
            + "                     from k55admin.stru a right "
            + "join k55admin.stru b"
            + "                     on a.no_level = b.no_level and "
            + "b.no_level>0"
            + "                     where b.no_level = t.no_seq"
            + "                 )");

        expColNames = new String [] {"CD_PLANT", "NO_LEVEL", "NO_SEQ"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"EE01A", "4", "1"},
            {"EE01A", "3", "1"},
            {"EE01A", "3", "2"},
            {"EE01A", "4", "1"},
            {"EE01A", "3", "3"},
            {"EE01A", "4", "1"},
            {"EE01A", "3", "1"},
            {"EE01A", "4", "1"},
            {"EE01A", "5", "1"},
            {"EE01A", "6", "1"},
            {"EE01A", "4", "2"},
            {"EE01A", "5", "1"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 812 - INNER JOIN in a correlated in subquery;
        //       correlation to the WHERE clause of the outerjoin subquery

        rs = st.executeQuery(
            "select cd_plant, no_level, no_seq"
            + " from k55admin.stru t"
            + " where cd_plant in  ( select  distinct a.cd_plant"
            + "                     from k55admin.stru a join "
            + "k55admin.stru b"
            + "                     on a.no_level = b.no_level and "
            + "b.no_level>0"
            + "                     where b.no_level = t.no_seq"
            + "                 )");

        expColNames = new String [] {"CD_PLANT", "NO_LEVEL", "NO_SEQ"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"EE01A", "4", "1"},
            {"EE01A", "3", "1"},
            {"EE01A", "3", "2"},
            {"EE01A", "4", "1"},
            {"EE01A", "3", "3"},
            {"EE01A", "4", "1"},
            {"EE01A", "3", "1"},
            {"EE01A", "4", "1"},
            {"EE01A", "5", "1"},
            {"EE01A", "6", "1"},
            {"EE01A", "4", "2"},
            {"EE01A", "5", "1"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        //
        //------------------------------------------------------------
        // test unit 9. OJ in a quantified predicate subquery, no correlation
        //              inequality predicate in the ON clause and WHERE clause;
        //------------------------------------------------------------

        // 901 - LEFT JOIN in '>=ALL' predicate subquery w/ distinct, no
        //        correlation;

        rs = st.executeQuery(
            "select distinct cd_plant, no_part_base_nxt_a"
            + " from k55admin.stru"
            + " where cd_plant >=all ( select distinct b.cd_plant"
            + "                       from k55admin.stru a left "
            + "join k55admin.stru b"
            + "                       on a.no_level = b.no_level "
            + "and b.no_level>7"
            + "                       where a.cd_plant = 'EE01B' "
            + "and b.cd_plant is not null )"
            + "      and no_level < 3");

        expColNames = new String [] {"CD_PLANT", "NO_PART_BASE_NXT_A"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"EE01B", "17508"},
            {"EE01B", "9509"},
            {"EE01D", "10C335"},
            {"EE01D", "9509"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 902 - LEFT JOIN in '>=ALL' predicate subquery, no correlation
        //       empty subquery results

        rs = st.executeQuery(
            "select distinct cd_plant, no_part_base_nxt_a"
            + " from k55admin.stru"
            + " where cd_plant >=all ( select b.cd_plant"
            + "                       from k55admin.stru a left "
            + "join k55admin.stru b"
            + "                       on a.no_level = b.no_level "
            + "and b.no_level>7"
            + "                       where a.cd_plant = 'EE01B' "
            + "and b.cd_plant is not null and 1>2)"
            + "      and no_level < 3");

        expColNames = new String [] {"CD_PLANT", "NO_PART_BASE_NXT_A"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"EE01A", ""},
            {"EE01A", "9509"},
            {"EE01A", "9E926"},
            {"EE01A", "BLPO"},
            {"EE01B", "17508"},
            {"EE01B", "9509"},
            {"EE01D", "10C335"},
            {"EE01D", "9509"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 903 - LEFT JOIN in '>=ALL' predicate subquery, no correlation
        //       null returned in the subquery

        rs = st.executeQuery(
            "select distinct cd_plant, no_part_base_nxt_a"
            + " from k55admin.stru"
            + " where cd_plant >=all ( select b.cd_plant"
            + "                       from k55admin.stru a left "
            + "join k55admin.stru b"
            + "                       on a.no_level = b.no_level "
            + "and b.no_level>7"
            + "                       where a.cd_plant = 'EE01B')"
            + "      and no_level < 3");

        expColNames = new String [] {"CD_PLANT", "NO_PART_BASE_NXT_A"};
        JDBC.assertColumnNames(rs, expColNames);
        JDBC.assertDrainResults(rs, 0);

        // 904 - RIGHT JOIN in '>=ALL' predicate subquery w/
        //       distinct, no correlation

        rs = st.executeQuery(
            "select distinct cd_plant, no_part_base_nxt_a"
            + " from k55admin.stru"
            + " where cd_plant >=all ( select distinct b.cd_plant"
            + "                       from k55admin.stru a right "
            + "join k55admin.stru b"
            + "                       on a.no_level = b.no_level "
            + "and b.no_level>7"
            + "                       where a.cd_plant = 'EE01B' "
            + "and b.cd_plant is not null )"
            + "      and no_level < 3");

        expColNames = new String [] {"CD_PLANT", "NO_PART_BASE_NXT_A"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"EE01B", "17508"},
            {"EE01B", "9509"},
            {"EE01D", "10C335"},
            {"EE01D", "9509"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 905 - RIGHT JOIN in '>=ALL' predicate subquery, no correlation
        //       empty subquery results

        rs = st.executeQuery(
            "select distinct cd_plant, no_part_base_nxt_a"
            + " from k55admin.stru"
            + " where cd_plant >=all ( select b.cd_plant"
            + "                       from k55admin.stru a right "
            + "join k55admin.stru b"
            + "                       on a.no_level = b.no_level "
            + "and b.no_level>7"
            + "                       where a.cd_plant = 'EE01B' "
            + "and b.cd_plant is not null and 1>2)"
            + "      and no_level < 3");

        expColNames = new String [] {"CD_PLANT", "NO_PART_BASE_NXT_A"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"EE01A", ""},
            {"EE01A", "9509"},
            {"EE01A", "9E926"},
            {"EE01A", "BLPO"},
            {"EE01B", "17508"},
            {"EE01B", "9509"},
            {"EE01D", "10C335"},
            {"EE01D", "9509"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 906 - RIGHT JOIN in '>=ALL' predicate subquery, no correlation
        //       null returned in the subquery

        rs = st.executeQuery(
            "select distinct cd_plant, no_part_base_nxt_a"
            + " from k55admin.stru"
            + " where cd_plant >=all ( select a.cd_plant"
            + "                       from k55admin.stru a right "
            + "join k55admin.stru b"
            + "                       on a.no_level = b.no_level "
            + "and b.no_level>7 )"
            + "      and no_level < 3");

        expColNames = new String [] {"CD_PLANT", "NO_PART_BASE_NXT_A"};
        JDBC.assertColumnNames(rs, expColNames);
        JDBC.assertDrainResults(rs, 0);

        //------------------------------------------------------------
        // Dropping the table
        //------------------------------------------------------------

        st.executeUpdate(
            "drop table k55admin.stru");

        //
        // coj205.clp
        //------------------------------------------------------------
        // --      inner join (105)
        //------------------------------------------------------------

        //------------------------------------------------------------
        // test unit 1. plain joins, different relops, conjunction;
        //------------------------------------------------------------

        // 101 - multiple '>=', '<=' in INNER JOIN ON  condition;

        rs = st.executeQuery(
            "SELECT PART, K55ADMIN.PARTS_NOTNULL.NUM, PRODUCT, "
            + "K55ADMIN.PRODUCTS_NOTNULL.NUM"
            + "  FROM K55ADMIN.PARTS_NOTNULL JOIN K55ADMIN.PRODUCTS_NOTNULL"
            + "    ON K55ADMIN.PARTS_NOTNULL.NUM >= "
            + "K55ADMIN.PRODUCTS_NOTNULL.NUM and"
            + "       K55ADMIN.PARTS_NOTNULL.NUM <= "
            + "K55ADMIN.PRODUCTS_NOTNULL.NUM"
            + "    order by 1,2,3,4");

        expColNames = new String [] {"PART", "NUM", "PRODUCT", "NUM"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Blades", "205", "Saw", "205"},
            {"Magnets", "10", "Generator", "10"},
            {"Paper", "20", "Sander", "20"},
            {"Plastic", "30", "Relay", "30"},
            {"Plastic", "30", "Ruler", "30"},
            {"Steel", "30", "Relay", "30"},
            {"Steel", "30", "Ruler", "30"},
            {"Wire", "10", "Generator", "10"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 102 - 'between' in INNER JOIN ON condition

        rs = st.executeQuery(
            "SELECT PART, K55ADMIN.PARTS_NOTNULL.NUM, PRODUCT, "
            + "K55ADMIN.PRODUCTS_NOTNULL.NUM"
            + "  FROM K55ADMIN.PARTS_NOTNULL JOIN K55ADMIN.PRODUCTS_NOTNULL"
            + "    ON K55ADMIN.PARTS_NOTNULL.NUM between "
            + "K55ADMIN.PRODUCTS_NOTNULL.NUM and"
            + "       K55ADMIN.PRODUCTS_NOTNULL.NUM + 50"
            + "    order by 1,2,3,4");

        expColNames = new String [] {"PART", "NUM", "PRODUCT", "NUM"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Blades", "205", "Saw", "205"},
            {"Magnets", "10", "Generator", "10"},
            {"Paper", "20", "Generator", "10"},
            {"Paper", "20", "Sander", "20"},
            {"Plastic", "30", "Generator", "10"},
            {"Plastic", "30", "Relay", "30"},
            {"Plastic", "30", "Ruler", "30"},
            {"Plastic", "30", "Sander", "20"},
            {"Steel", "30", "Generator", "10"},
            {"Steel", "30", "Relay", "30"},
            {"Steel", "30", "Ruler", "30"},
            {"Steel", "30", "Sander", "20"},
            {"Wire", "10", "Generator", "10"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 103 - '>'/'<' in INNER JOIN on condition

        rs = st.executeQuery(
            "SELECT PART, K55ADMIN.PARTS_NOTNULL.NUM, PRODUCT, "
            + "K55ADMIN.PRODUCTS_NOTNULL.NUM"
            + "  FROM K55ADMIN.PARTS_NOTNULL JOIN K55ADMIN.PRODUCTS_NOTNULL"
            + "    ON K55ADMIN.PARTS_NOTNULL.NUM > "
            + "K55ADMIN.PRODUCTS_NOTNULL.NUM and"
            + "       K55ADMIN.PRODUCTS_NOTNULL.NUM < "
            + "K55ADMIN.PARTS_NOTNULL.NUM"
            + "    order by 1,2,3,4");

        expColNames = new String [] {"PART", "NUM", "PRODUCT", "NUM"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Blades", "205", "Generator", "10"},
            {"Blades", "205", "Hammer", "50"},
            {"Blades", "205", "Relay", "30"},
            {"Blades", "205", "Ruler", "30"},
            {"Blades", "205", "Sander", "20"},
            {"Oil", "160", "Generator", "10"},
            {"Oil", "160", "Hammer", "50"},
            {"Oil", "160", "Relay", "30"},
            {"Oil", "160", "Ruler", "30"},
            {"Oil", "160", "Sander", "20"},
            {"Paper", "20", "Generator", "10"},
            {"Plastic", "30", "Generator", "10"},
            {"Plastic", "30", "Sander", "20"},
            {"Steel", "30", "Generator", "10"},
            {"Steel", "30", "Sander", "20"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 104 - '<>' in INNER JOIN on condition

        rs = st.executeQuery(
            "SELECT PART, K55ADMIN.PARTS_NOTNULL.NUM, PRODUCT, "
            + "K55ADMIN.PRODUCTS_NOTNULL.NUM"
            + "  FROM K55ADMIN.PARTS_NOTNULL JOIN K55ADMIN.PRODUCTS_NOTNULL"
            + "    ON K55ADMIN.PARTS_NOTNULL.NUM <> "
            + "K55ADMIN.PRODUCTS_NOTNULL.NUM"
            + "    order by 1,2,3,4");

        expColNames = new String [] {"PART", "NUM", "PRODUCT", "NUM"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Blades", "205", "Generator", "10"},
            {"Blades", "205", "Hammer", "50"},
            {"Blades", "205", "Relay", "30"},
            {"Blades", "205", "Ruler", "30"},
            {"Blades", "205", "Sander", "20"},
            {"Blades", "205", "Screwdriver", "505"},
            {"Magnets", "10", "Hammer", "50"},
            {"Magnets", "10", "Relay", "30"},
            {"Magnets", "10", "Ruler", "30"},
            {"Magnets", "10", "Sander", "20"},
            {"Magnets", "10", "Saw", "205"},
            {"Magnets", "10", "Screwdriver", "505"},
            {"Oil", "160", "Generator", "10"},
            {"Oil", "160", "Hammer", "50"},
            {"Oil", "160", "Relay", "30"},
            {"Oil", "160", "Ruler", "30"},
            {"Oil", "160", "Sander", "20"},
            {"Oil", "160", "Saw", "205"},
            {"Oil", "160", "Screwdriver", "505"},
            {"Paper", "20", "Generator", "10"},
            {"Paper", "20", "Hammer", "50"},
            {"Paper", "20", "Relay", "30"},
            {"Paper", "20", "Ruler", "30"},
            {"Paper", "20", "Saw", "205"},
            {"Paper", "20", "Screwdriver", "505"},
            {"Plastic", "30", "Generator", "10"},
            {"Plastic", "30", "Hammer", "50"},
            {"Plastic", "30", "Sander", "20"},
            {"Plastic", "30", "Saw", "205"},
            {"Plastic", "30", "Screwdriver", "505"},
            {"Steel", "30", "Generator", "10"},
            {"Steel", "30", "Hammer", "50"},
            {"Steel", "30", "Sander", "20"},
            {"Steel", "30", "Saw", "205"},
            {"Steel", "30", "Screwdriver", "505"},
            {"Wire", "10", "Hammer", "50"},
            {"Wire", "10", "Relay", "30"},
            {"Wire", "10", "Ruler", "30"},
            {"Wire", "10", "Sander", "20"},
            {"Wire", "10", "Saw", "205"},
            {"Wire", "10", "Screwdriver", "505"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 105 - multiple '>=', '<=' in LEFT JOIN ON condition

        rs = st.executeQuery(
            "SELECT PART, K55ADMIN.PARTS_NOTNULL.NUM, PRODUCT, "
            + "K55ADMIN.PRODUCTS_NOTNULL.NUM"
            + "  FROM K55ADMIN.PARTS_NOTNULL LEFT JOIN "
            + "K55ADMIN.PRODUCTS_NOTNULL"
            + "    ON K55ADMIN.PARTS_NOTNULL.NUM >= "
            + "K55ADMIN.PRODUCTS_NOTNULL.NUM and"
            + "       K55ADMIN.PARTS_NOTNULL.NUM <= "
            + "K55ADMIN.PRODUCTS_NOTNULL.NUM"
            + "    order by 1,2,3,4");

        expColNames = new String [] {"PART", "NUM", "PRODUCT", "NUM"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Blades", "205", "Saw", "205"},
            {"Magnets", "10", "Generator", "10"},
            {"Oil", "160", null, null},
            {"Paper", "20", "Sander", "20"},
            {"Plastic", "30", "Relay", "30"},
            {"Plastic", "30", "Ruler", "30"},
            {"Steel", "30", "Relay", "30"},
            {"Steel", "30", "Ruler", "30"},
            {"Wire", "10", "Generator", "10"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 106 - 'between' in LEFT JOIN ON condition

        rs = st.executeQuery(
            "SELECT PART, K55ADMIN.PARTS_NOTNULL.NUM, PRODUCT, "
            + "K55ADMIN.PRODUCTS_NOTNULL.NUM"
            + "  FROM K55ADMIN.PARTS_NOTNULL LEFT JOIN "
            + "K55ADMIN.PRODUCTS_NOTNULL"
            + "    ON K55ADMIN.PARTS_NOTNULL.NUM between "
            + "K55ADMIN.PRODUCTS_NOTNULL.NUM and"
            + "        K55ADMIN.PRODUCTS_NOTNULL.NUM + 50"
            + "    order by 1,2,3,4");

        expColNames = new String [] {"PART", "NUM", "PRODUCT", "NUM"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Blades", "205", "Saw", "205"},
            {"Magnets", "10", "Generator", "10"},
            {"Oil", "160", null, null},
            {"Paper", "20", "Generator", "10"},
            {"Paper", "20", "Sander", "20"},
            {"Plastic", "30", "Generator", "10"},
            {"Plastic", "30", "Relay", "30"},
            {"Plastic", "30", "Ruler", "30"},
            {"Plastic", "30", "Sander", "20"},
            {"Steel", "30", "Generator", "10"},
            {"Steel", "30", "Relay", "30"},
            {"Steel", "30", "Ruler", "30"},
            {"Steel", "30", "Sander", "20"},
            {"Wire", "10", "Generator", "10"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 107 - '>'/'<' in LEFT JOIN on condition

        rs = st.executeQuery(
            "SELECT PART, K55ADMIN.PARTS_NOTNULL.NUM, PRODUCT, "
            + "K55ADMIN.PRODUCTS_NOTNULL.NUM"
            + "  FROM K55ADMIN.PARTS_NOTNULL LEFT JOIN "
            + "K55ADMIN.PRODUCTS_NOTNULL"
            + "    ON K55ADMIN.PARTS_NOTNULL.NUM > "
            + "K55ADMIN.PRODUCTS_NOTNULL.NUM and"
            + "        K55ADMIN.PRODUCTS_NOTNULL.NUM < "
            + "K55ADMIN.PARTS_NOTNULL.NUM"
            + "    order by 1,2,3,4");

        expColNames = new String [] {"PART", "NUM", "PRODUCT", "NUM"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Blades", "205", "Generator", "10"},
            {"Blades", "205", "Hammer", "50"},
            {"Blades", "205", "Relay", "30"},
            {"Blades", "205", "Ruler", "30"},
            {"Blades", "205", "Sander", "20"},
            {"Magnets", "10", null, null},
            {"Oil", "160", "Generator", "10"},
            {"Oil", "160", "Hammer", "50"},
            {"Oil", "160", "Relay", "30"},
            {"Oil", "160", "Ruler", "30"},
            {"Oil", "160", "Sander", "20"},
            {"Paper", "20", "Generator", "10"},
            {"Plastic", "30", "Generator", "10"},
            {"Plastic", "30", "Sander", "20"},
            {"Steel", "30", "Generator", "10"},
            {"Steel", "30", "Sander", "20"},
            {"Wire", "10", null, null}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 108 - '<>' in LEFT JOIN on condition

        rs = st.executeQuery(
            "SELECT PART, K55ADMIN.PARTS_NOTNULL.NUM, PRODUCT, "
            + "K55ADMIN.PRODUCTS_NOTNULL.NUM"
            + "  FROM K55ADMIN.PARTS_NOTNULL LEFT JOIN "
            + "K55ADMIN.PRODUCTS_NOTNULL"
            + "    ON K55ADMIN.PARTS_NOTNULL.NUM <> "
            + "K55ADMIN.PRODUCTS_NOTNULL.NUM"
            + "    order by 1,2,3,4");

        expColNames = new String [] {"PART", "NUM", "PRODUCT", "NUM"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Blades", "205", "Generator", "10"},
            {"Blades", "205", "Hammer", "50"},
            {"Blades", "205", "Relay", "30"},
            {"Blades", "205", "Ruler", "30"},
            {"Blades", "205", "Sander", "20"},
            {"Blades", "205", "Screwdriver", "505"},
            {"Magnets", "10", "Hammer", "50"},
            {"Magnets", "10", "Relay", "30"},
            {"Magnets", "10", "Ruler", "30"},
            {"Magnets", "10", "Sander", "20"},
            {"Magnets", "10", "Saw", "205"},
            {"Magnets", "10", "Screwdriver", "505"},
            {"Oil", "160", "Generator", "10"},
            {"Oil", "160", "Hammer", "50"},
            {"Oil", "160", "Relay", "30"},
            {"Oil", "160", "Ruler", "30"},
            {"Oil", "160", "Sander", "20"},
            {"Oil", "160", "Saw", "205"},
            {"Oil", "160", "Screwdriver", "505"},
            {"Paper", "20", "Generator", "10"},
            {"Paper", "20", "Hammer", "50"},
            {"Paper", "20", "Relay", "30"},
            {"Paper", "20", "Ruler", "30"},
            {"Paper", "20", "Saw", "205"},
            {"Paper", "20", "Screwdriver", "505"},
            {"Plastic", "30", "Generator", "10"},
            {"Plastic", "30", "Hammer", "50"},
            {"Plastic", "30", "Sander", "20"},
            {"Plastic", "30", "Saw", "205"},
            {"Plastic", "30", "Screwdriver", "505"},
            {"Steel", "30", "Generator", "10"},
            {"Steel", "30", "Hammer", "50"},
            {"Steel", "30", "Sander", "20"},
            {"Steel", "30", "Saw", "205"},
            {"Steel", "30", "Screwdriver", "505"},
            {"Wire", "10", "Hammer", "50"},
            {"Wire", "10", "Relay", "30"},
            {"Wire", "10", "Ruler", "30"},
            {"Wire", "10", "Sander", "20"},
            {"Wire", "10", "Saw", "205"},
            {"Wire", "10", "Screwdriver", "505"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 109 - multiple '>=', '<=' in RIGHT JOIN ON condition

        rs = st.executeQuery(
            "SELECT PART, K55ADMIN.PARTS_NOTNULL.NUM, PRODUCT, "
            + "K55ADMIN.PRODUCTS_NOTNULL.NUM"
            + "  FROM K55ADMIN.PARTS_NOTNULL RIGHT JOIN "
            + "K55ADMIN.PRODUCTS_NOTNULL"
            + "    ON K55ADMIN.PARTS_NOTNULL.NUM >= "
            + "K55ADMIN.PRODUCTS_NOTNULL.NUM and"
            + "        K55ADMIN.PARTS_NOTNULL.NUM <= "
            + "K55ADMIN.PRODUCTS_NOTNULL.NUM"
            + "    order by 1,2,3,4");

        expColNames = new String [] {"PART", "NUM", "PRODUCT", "NUM"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Blades", "205", "Saw", "205"},
            {"Magnets", "10", "Generator", "10"},
            {"Paper", "20", "Sander", "20"},
            {"Plastic", "30", "Relay", "30"},
            {"Plastic", "30", "Ruler", "30"},
            {"Steel", "30", "Relay", "30"},
            {"Steel", "30", "Ruler", "30"},
            {"Wire", "10", "Generator", "10"},
            {null, null, "Hammer", "50"},
            {null, null, "Screwdriver", "505"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 110 - 'between' in RIGHT JOIN ON condition

        rs = st.executeQuery(
            "SELECT PART, K55ADMIN.PARTS_NOTNULL.NUM, PRODUCT, "
            + "K55ADMIN.PRODUCTS_NOTNULL.NUM"
            + "  FROM K55ADMIN.PARTS_NOTNULL RIGHT JOIN "
            + "K55ADMIN.PRODUCTS_NOTNULL"
            + "    ON K55ADMIN.PARTS_NOTNULL.NUM between "
            + "K55ADMIN.PRODUCTS_NOTNULL.NUM and"
            + "        K55ADMIN.PRODUCTS_NOTNULL.NUM + 50"
            + "    order by 1,2,3,4");

        expColNames = new String [] {"PART", "NUM", "PRODUCT", "NUM"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Blades", "205", "Saw", "205"},
            {"Magnets", "10", "Generator", "10"},
            {"Paper", "20", "Generator", "10"},
            {"Paper", "20", "Sander", "20"},
            {"Plastic", "30", "Generator", "10"},
            {"Plastic", "30", "Relay", "30"},
            {"Plastic", "30", "Ruler", "30"},
            {"Plastic", "30", "Sander", "20"},
            {"Steel", "30", "Generator", "10"},
            {"Steel", "30", "Relay", "30"},
            {"Steel", "30", "Ruler", "30"},
            {"Steel", "30", "Sander", "20"},
            {"Wire", "10", "Generator", "10"},
            {null, null, "Hammer", "50"},
            {null, null, "Screwdriver", "505"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 111 - '>'/'<' in RIGHT JOIN on condition

        rs = st.executeQuery(
            "SELECT PART, K55ADMIN.PARTS_NOTNULL.NUM, PRODUCT, "
            + "K55ADMIN.PRODUCTS_NOTNULL.NUM"
            + "  FROM K55ADMIN.PARTS_NOTNULL RIGHT JOIN "
            + "K55ADMIN.PRODUCTS_NOTNULL"
            + "    ON K55ADMIN.PARTS_NOTNULL.NUM > "
            + "K55ADMIN.PRODUCTS_NOTNULL.NUM and"
            + "        K55ADMIN.PRODUCTS_NOTNULL.NUM < "
            + "K55ADMIN.PARTS_NOTNULL.NUM"
            + "    order by 1,2,3,4");

        expColNames = new String [] {"PART", "NUM", "PRODUCT", "NUM"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Blades", "205", "Generator", "10"},
            {"Blades", "205", "Hammer", "50"},
            {"Blades", "205", "Relay", "30"},
            {"Blades", "205", "Ruler", "30"},
            {"Blades", "205", "Sander", "20"},
            {"Oil", "160", "Generator", "10"},
            {"Oil", "160", "Hammer", "50"},
            {"Oil", "160", "Relay", "30"},
            {"Oil", "160", "Ruler", "30"},
            {"Oil", "160", "Sander", "20"},
            {"Paper", "20", "Generator", "10"},
            {"Plastic", "30", "Generator", "10"},
            {"Plastic", "30", "Sander", "20"},
            {"Steel", "30", "Generator", "10"},
            {"Steel", "30", "Sander", "20"},
            {null, null, "Saw", "205"},
            {null, null, "Screwdriver", "505"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 112 - '<>' in RIGHT JOIN on condition

        rs = st.executeQuery(
            "SELECT PART, K55ADMIN.PARTS_NOTNULL.NUM, PRODUCT, "
            + "K55ADMIN.PRODUCTS_NOTNULL.NUM"
            + "  FROM K55ADMIN.PARTS_NOTNULL RIGHT JOIN "
            + "K55ADMIN.PRODUCTS_NOTNULL"
            + "    ON K55ADMIN.PARTS_NOTNULL.NUM <> "
            + "K55ADMIN.PRODUCTS_NOTNULL.NUM"
            + "    order by 1,2,3,4");

        expColNames = new String [] {"PART", "NUM", "PRODUCT", "NUM"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Blades", "205", "Generator", "10"},
            {"Blades", "205", "Hammer", "50"},
            {"Blades", "205", "Relay", "30"},
            {"Blades", "205", "Ruler", "30"},
            {"Blades", "205", "Sander", "20"},
            {"Blades", "205", "Screwdriver", "505"},
            {"Magnets", "10", "Hammer", "50"},
            {"Magnets", "10", "Relay", "30"},
            {"Magnets", "10", "Ruler", "30"},
            {"Magnets", "10", "Sander", "20"},
            {"Magnets", "10", "Saw", "205"},
            {"Magnets", "10", "Screwdriver", "505"},
            {"Oil", "160", "Generator", "10"},
            {"Oil", "160", "Hammer", "50"},
            {"Oil", "160", "Relay", "30"},
            {"Oil", "160", "Ruler", "30"},
            {"Oil", "160", "Sander", "20"},
            {"Oil", "160", "Saw", "205"},
            {"Oil", "160", "Screwdriver", "505"},
            {"Paper", "20", "Generator", "10"},
            {"Paper", "20", "Hammer", "50"},
            {"Paper", "20", "Relay", "30"},
            {"Paper", "20", "Ruler", "30"},
            {"Paper", "20", "Saw", "205"},
            {"Paper", "20", "Screwdriver", "505"},
            {"Plastic", "30", "Generator", "10"},
            {"Plastic", "30", "Hammer", "50"},
            {"Plastic", "30", "Sander", "20"},
            {"Plastic", "30", "Saw", "205"},
            {"Plastic", "30", "Screwdriver", "505"},
            {"Steel", "30", "Generator", "10"},
            {"Steel", "30", "Hammer", "50"},
            {"Steel", "30", "Sander", "20"},
            {"Steel", "30", "Saw", "205"},
            {"Steel", "30", "Screwdriver", "505"},
            {"Wire", "10", "Hammer", "50"},
            {"Wire", "10", "Relay", "30"},
            {"Wire", "10", "Ruler", "30"},
            {"Wire", "10", "Sander", "20"},
            {"Wire", "10", "Saw", "205"},
            {"Wire", "10", "Screwdriver", "505"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        //
        //------------------------------------------------------------
        // test unit 2. plain joins, different relops, and/or/not;
        //------------------------------------------------------------

        // 201 - '>='/'<='/OR in INNER JOIN ON condition;

        rs = st.executeQuery(
            "SELECT PART, K55ADMIN.PARTS_NOTNULL.NUM, PRODUCT, "
            + "K55ADMIN.PRODUCTS_NOTNULL.NUM"
            + "  FROM K55ADMIN.PARTS_NOTNULL JOIN K55ADMIN.PRODUCTS_NOTNULL"
            + "    ON K55ADMIN.PARTS_NOTNULL.NUM >= "
            + "K55ADMIN.PRODUCTS_NOTNULL.NUM OR"
            + "        K55ADMIN.PARTS_NOTNULL.NUM <= "
            + "K55ADMIN.PRODUCTS_NOTNULL.NUM"
            + "    order by 1,2,3,4");

        expColNames = new String [] {"PART", "NUM", "PRODUCT", "NUM"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Blades", "205", "Generator", "10"},
            {"Blades", "205", "Hammer", "50"},
            {"Blades", "205", "Relay", "30"},
            {"Blades", "205", "Ruler", "30"},
            {"Blades", "205", "Sander", "20"},
            {"Blades", "205", "Saw", "205"},
            {"Blades", "205", "Screwdriver", "505"},
            {"Magnets", "10", "Generator", "10"},
            {"Magnets", "10", "Hammer", "50"},
            {"Magnets", "10", "Relay", "30"},
            {"Magnets", "10", "Ruler", "30"},
            {"Magnets", "10", "Sander", "20"},
            {"Magnets", "10", "Saw", "205"},
            {"Magnets", "10", "Screwdriver", "505"},
            {"Oil", "160", "Generator", "10"},
            {"Oil", "160", "Hammer", "50"},
            {"Oil", "160", "Relay", "30"},
            {"Oil", "160", "Ruler", "30"},
            {"Oil", "160", "Sander", "20"},
            {"Oil", "160", "Saw", "205"},
            {"Oil", "160", "Screwdriver", "505"},
            {"Paper", "20", "Generator", "10"},
            {"Paper", "20", "Hammer", "50"},
            {"Paper", "20", "Relay", "30"},
            {"Paper", "20", "Ruler", "30"},
            {"Paper", "20", "Sander", "20"},
            {"Paper", "20", "Saw", "205"},
            {"Paper", "20", "Screwdriver", "505"},
            {"Plastic", "30", "Generator", "10"},
            {"Plastic", "30", "Hammer", "50"},
            {"Plastic", "30", "Relay", "30"},
            {"Plastic", "30", "Ruler", "30"},
            {"Plastic", "30", "Sander", "20"},
            {"Plastic", "30", "Saw", "205"},
            {"Plastic", "30", "Screwdriver", "505"},
            {"Steel", "30", "Generator", "10"},
            {"Steel", "30", "Hammer", "50"},
            {"Steel", "30", "Relay", "30"},
            {"Steel", "30", "Ruler", "30"},
            {"Steel", "30", "Sander", "20"},
            {"Steel", "30", "Saw", "205"},
            {"Steel", "30", "Screwdriver", "505"},
            {"Wire", "10", "Generator", "10"},
            {"Wire", "10", "Hammer", "50"},
            {"Wire", "10", "Relay", "30"},
            {"Wire", "10", "Ruler", "30"},
            {"Wire", "10", "Sander", "20"},
            {"Wire", "10", "Saw", "205"},
            {"Wire", "10", "Screwdriver", "505"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 202 - 'not between' in INNER JOIN ON condition

        rs = st.executeQuery(
            "SELECT PART, K55ADMIN.PARTS_NOTNULL.NUM, PRODUCT, "
            + "K55ADMIN.PRODUCTS_NOTNULL.NUM"
            + "  FROM K55ADMIN.PARTS_NOTNULL JOIN K55ADMIN.PRODUCTS_NOTNULL"
            + "    ON NOT(K55ADMIN.PARTS_NOTNULL.NUM between "
            + "K55ADMIN.PRODUCTS_NOTNULL.NUM AND"
            + "        K55ADMIN.PRODUCTS_NOTNULL.NUM + 50)"
            + "    order by 1,2,3,4");

        expColNames = new String [] {"PART", "NUM", "PRODUCT", "NUM"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Blades", "205", "Generator", "10"},
            {"Blades", "205", "Hammer", "50"},
            {"Blades", "205", "Relay", "30"},
            {"Blades", "205", "Ruler", "30"},
            {"Blades", "205", "Sander", "20"},
            {"Blades", "205", "Screwdriver", "505"},
            {"Magnets", "10", "Hammer", "50"},
            {"Magnets", "10", "Relay", "30"},
            {"Magnets", "10", "Ruler", "30"},
            {"Magnets", "10", "Sander", "20"},
            {"Magnets", "10", "Saw", "205"},
            {"Magnets", "10", "Screwdriver", "505"},
            {"Oil", "160", "Generator", "10"},
            {"Oil", "160", "Hammer", "50"},
            {"Oil", "160", "Relay", "30"},
            {"Oil", "160", "Ruler", "30"},
            {"Oil", "160", "Sander", "20"},
            {"Oil", "160", "Saw", "205"},
            {"Oil", "160", "Screwdriver", "505"},
            {"Paper", "20", "Hammer", "50"},
            {"Paper", "20", "Relay", "30"},
            {"Paper", "20", "Ruler", "30"},
            {"Paper", "20", "Saw", "205"},
            {"Paper", "20", "Screwdriver", "505"},
            {"Plastic", "30", "Hammer", "50"},
            {"Plastic", "30", "Saw", "205"},
            {"Plastic", "30", "Screwdriver", "505"},
            {"Steel", "30", "Hammer", "50"},
            {"Steel", "30", "Saw", "205"},
            {"Steel", "30", "Screwdriver", "505"},
            {"Wire", "10", "Hammer", "50"},
            {"Wire", "10", "Relay", "30"},
            {"Wire", "10", "Ruler", "30"},
            {"Wire", "10", "Sander", "20"},
            {"Wire", "10", "Saw", "205"},
            {"Wire", "10", "Screwdriver", "505"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 203 - '>'/'<'/OR in INNER JOIN on condition

        rs = st.executeQuery(
            "SELECT PART, K55ADMIN.PARTS_NOTNULL.NUM, PRODUCT, "
            + "K55ADMIN.PRODUCTS_NOTNULL.NUM"
            + "  FROM K55ADMIN.PARTS_NOTNULL JOIN K55ADMIN.PRODUCTS_NOTNULL"
            + "    ON K55ADMIN.PARTS_NOTNULL.NUM > "
            + "K55ADMIN.PRODUCTS_NOTNULL.NUM OR"
            + "        K55ADMIN.PRODUCTS_NOTNULL.NUM < "
            + "K55ADMIN.PARTS_NOTNULL.NUM"
            + "    order by 1,2,3,4");

        expColNames = new String [] {"PART", "NUM", "PRODUCT", "NUM"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Blades", "205", "Generator", "10"},
            {"Blades", "205", "Hammer", "50"},
            {"Blades", "205", "Relay", "30"},
            {"Blades", "205", "Ruler", "30"},
            {"Blades", "205", "Sander", "20"},
            {"Oil", "160", "Generator", "10"},
            {"Oil", "160", "Hammer", "50"},
            {"Oil", "160", "Relay", "30"},
            {"Oil", "160", "Ruler", "30"},
            {"Oil", "160", "Sander", "20"},
            {"Paper", "20", "Generator", "10"},
            {"Plastic", "30", "Generator", "10"},
            {"Plastic", "30", "Sander", "20"},
            {"Steel", "30", "Generator", "10"},
            {"Steel", "30", "Sander", "20"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 204 - not/'<>' in INNER JOIN on condition

        rs = st.executeQuery(
            "SELECT PART, K55ADMIN.PARTS_NOTNULL.NUM, PRODUCT, "
            + "K55ADMIN.PRODUCTS_NOTNULL.NUM"
            + "  FROM K55ADMIN.PARTS_NOTNULL JOIN K55ADMIN.PRODUCTS_NOTNULL"
            + "    ON NOT( K55ADMIN.PARTS_NOTNULL.NUM <> "
            + "K55ADMIN.PRODUCTS_NOTNULL.NUM )"
            + "    order by 1,2,3,4");

        expColNames = new String [] {"PART", "NUM", "PRODUCT", "NUM"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Blades", "205", "Saw", "205"},
            {"Magnets", "10", "Generator", "10"},
            {"Paper", "20", "Sander", "20"},
            {"Plastic", "30", "Relay", "30"},
            {"Plastic", "30", "Ruler", "30"},
            {"Steel", "30", "Relay", "30"},
            {"Steel", "30", "Ruler", "30"},
            {"Wire", "10", "Generator", "10"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 205 - '>='/'<='/or in LEFT JOIN ON condition

        rs = st.executeQuery(
            "SELECT PART, K55ADMIN.PARTS_NOTNULL.NUM, PRODUCT, "
            + "K55ADMIN.PRODUCTS_NOTNULL.NUM"
            + "  FROM K55ADMIN.PARTS_NOTNULL LEFT JOIN "
            + "K55ADMIN.PRODUCTS_NOTNULL"
            + "    ON K55ADMIN.PARTS_NOTNULL.NUM >= "
            + "K55ADMIN.PRODUCTS_NOTNULL.NUM OR "
            + "K55ADMIN.PRODUCTS_NOTNULL.NUM <= K55ADMIN.PARTS_NOTNULL.NUM"
            + "    order by 1,2,3,4");

        expColNames = new String [] {"PART", "NUM", "PRODUCT", "NUM"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Blades", "205", "Generator", "10"},
            {"Blades", "205", "Hammer", "50"},
            {"Blades", "205", "Relay", "30"},
            {"Blades", "205", "Ruler", "30"},
            {"Blades", "205", "Sander", "20"},
            {"Blades", "205", "Saw", "205"},
            {"Magnets", "10", "Generator", "10"},
            {"Oil", "160", "Generator", "10"},
            {"Oil", "160", "Hammer", "50"},
            {"Oil", "160", "Relay", "30"},
            {"Oil", "160", "Ruler", "30"},
            {"Oil", "160", "Sander", "20"},
            {"Paper", "20", "Generator", "10"},
            {"Paper", "20", "Sander", "20"},
            {"Plastic", "30", "Generator", "10"},
            {"Plastic", "30", "Relay", "30"},
            {"Plastic", "30", "Ruler", "30"},
            {"Plastic", "30", "Sander", "20"},
            {"Steel", "30", "Generator", "10"},
            {"Steel", "30", "Relay", "30"},
            {"Steel", "30", "Ruler", "30"},
            {"Steel", "30", "Sander", "20"},
            {"Wire", "10", "Generator", "10"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 206 - 'not between' in LEFT JOIN ON condition

        rs = st.executeQuery(
            "SELECT PART, K55ADMIN.PARTS_NOTNULL.NUM, PRODUCT, "
            + "K55ADMIN.PRODUCTS_NOTNULL.NUM"
            + "  FROM K55ADMIN.PARTS_NOTNULL LEFT JOIN "
            + "K55ADMIN.PRODUCTS_NOTNULL"
            + "    ON not (K55ADMIN.PARTS_NOTNULL.NUM between "
            + "K55ADMIN.PRODUCTS_NOTNULL.NUM and"
            + "        K55ADMIN.PRODUCTS_NOTNULL.NUM + 50)"
            + "    order by 1,2,3,4");

        expColNames = new String [] {"PART", "NUM", "PRODUCT", "NUM"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Blades", "205", "Generator", "10"},
            {"Blades", "205", "Hammer", "50"},
            {"Blades", "205", "Relay", "30"},
            {"Blades", "205", "Ruler", "30"},
            {"Blades", "205", "Sander", "20"},
            {"Blades", "205", "Screwdriver", "505"},
            {"Magnets", "10", "Hammer", "50"},
            {"Magnets", "10", "Relay", "30"},
            {"Magnets", "10", "Ruler", "30"},
            {"Magnets", "10", "Sander", "20"},
            {"Magnets", "10", "Saw", "205"},
            {"Magnets", "10", "Screwdriver", "505"},
            {"Oil", "160", "Generator", "10"},
            {"Oil", "160", "Hammer", "50"},
            {"Oil", "160", "Relay", "30"},
            {"Oil", "160", "Ruler", "30"},
            {"Oil", "160", "Sander", "20"},
            {"Oil", "160", "Saw", "205"},
            {"Oil", "160", "Screwdriver", "505"},
            {"Paper", "20", "Hammer", "50"},
            {"Paper", "20", "Relay", "30"},
            {"Paper", "20", "Ruler", "30"},
            {"Paper", "20", "Saw", "205"},
            {"Paper", "20", "Screwdriver", "505"},
            {"Plastic", "30", "Hammer", "50"},
            {"Plastic", "30", "Saw", "205"},
            {"Plastic", "30", "Screwdriver", "505"},
            {"Steel", "30", "Hammer", "50"},
            {"Steel", "30", "Saw", "205"},
            {"Steel", "30", "Screwdriver", "505"},
            {"Wire", "10", "Hammer", "50"},
            {"Wire", "10", "Relay", "30"},
            {"Wire", "10", "Ruler", "30"},
            {"Wire", "10", "Sander", "20"},
            {"Wire", "10", "Saw", "205"},
            {"Wire", "10", "Screwdriver", "505"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 207 - '>'/'<'/or in LEFT JOIN on condition

        rs = st.executeQuery(
            "SELECT PART, K55ADMIN.PARTS_NOTNULL.NUM, PRODUCT, "
            + "K55ADMIN.PRODUCTS_NOTNULL.NUM"
            + "  FROM K55ADMIN.PARTS_NOTNULL LEFT JOIN "
            + "K55ADMIN.PRODUCTS_NOTNULL"
            + "    ON K55ADMIN.PARTS_NOTNULL.NUM > "
            + "K55ADMIN.PRODUCTS_NOTNULL.NUM OR"
            + "        K55ADMIN.PRODUCTS_NOTNULL.NUM < "
            + "K55ADMIN.PARTS_NOTNULL.NUM"
            + "    order by 1,2,3,4");

        expColNames = new String [] {"PART", "NUM", "PRODUCT", "NUM"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Blades", "205", "Generator", "10"},
            {"Blades", "205", "Hammer", "50"},
            {"Blades", "205", "Relay", "30"},
            {"Blades", "205", "Ruler", "30"},
            {"Blades", "205", "Sander", "20"},
            {"Magnets", "10", null, null},
            {"Oil", "160", "Generator", "10"},
            {"Oil", "160", "Hammer", "50"},
            {"Oil", "160", "Relay", "30"},
            {"Oil", "160", "Ruler", "30"},
            {"Oil", "160", "Sander", "20"},
            {"Paper", "20", "Generator", "10"},
            {"Plastic", "30", "Generator", "10"},
            {"Plastic", "30", "Sander", "20"},
            {"Steel", "30", "Generator", "10"},
            {"Steel", "30", "Sander", "20"},
            {"Wire", "10", null, null}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 208 - not/'<>' in LEFT JOIN on condition

        rs = st.executeQuery(
            "SELECT PART, K55ADMIN.PARTS_NOTNULL.NUM, PRODUCT, "
            + "K55ADMIN.PRODUCTS_NOTNULL.NUM"
            + "  FROM K55ADMIN.PARTS_NOTNULL LEFT JOIN "
            + "K55ADMIN.PRODUCTS_NOTNULL"
            + "    ON NOT(K55ADMIN.PARTS_NOTNULL.NUM <> "
            + "K55ADMIN.PRODUCTS_NOTNULL.NUM)"
            + "    order by 1,2,3,4");

        expColNames = new String [] {"PART", "NUM", "PRODUCT", "NUM"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Blades", "205", "Saw", "205"},
            {"Magnets", "10", "Generator", "10"},
            {"Oil", "160", null, null},
            {"Paper", "20", "Sander", "20"},
            {"Plastic", "30", "Relay", "30"},
            {"Plastic", "30", "Ruler", "30"},
            {"Steel", "30", "Relay", "30"},
            {"Steel", "30", "Ruler", "30"},
            {"Wire", "10", "Generator", "10"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 209 - '>='/'<='/OR in RIGHT JOIN ON condition

        rs = st.executeQuery(
            "SELECT PART, K55ADMIN.PARTS_NOTNULL.NUM, PRODUCT, "
            + "K55ADMIN.PRODUCTS_NOTNULL.NUM"
            + "  FROM K55ADMIN.PARTS_NOTNULL RIGHT JOIN "
            + "K55ADMIN.PRODUCTS_NOTNULL"
            + "    ON K55ADMIN.PARTS_NOTNULL.NUM >= "
            + "K55ADMIN.PRODUCTS_NOTNULL.NUM or"
            + "        K55ADMIN.PRODUCTS_NOTNULL.NUM <= "
            + "K55ADMIN.PARTS_NOTNULL.NUM"
            + "    order by 1,2,3,4");

        expColNames = new String [] {"PART", "NUM", "PRODUCT", "NUM"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Blades", "205", "Generator", "10"},
            {"Blades", "205", "Hammer", "50"},
            {"Blades", "205", "Relay", "30"},
            {"Blades", "205", "Ruler", "30"},
            {"Blades", "205", "Sander", "20"},
            {"Blades", "205", "Saw", "205"},
            {"Magnets", "10", "Generator", "10"},
            {"Oil", "160", "Generator", "10"},
            {"Oil", "160", "Hammer", "50"},
            {"Oil", "160", "Relay", "30"},
            {"Oil", "160", "Ruler", "30"},
            {"Oil", "160", "Sander", "20"},
            {"Paper", "20", "Generator", "10"},
            {"Paper", "20", "Sander", "20"},
            {"Plastic", "30", "Generator", "10"},
            {"Plastic", "30", "Relay", "30"},
            {"Plastic", "30", "Ruler", "30"},
            {"Plastic", "30", "Sander", "20"},
            {"Steel", "30", "Generator", "10"},
            {"Steel", "30", "Relay", "30"},
            {"Steel", "30", "Ruler", "30"},
            {"Steel", "30", "Sander", "20"},
            {"Wire", "10", "Generator", "10"},
            {null, null, "Screwdriver", "505"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 210 - 'not between' in RIGHT JOIN ON condition

        rs = st.executeQuery(
            "SELECT PART, K55ADMIN.PARTS_NOTNULL.NUM, PRODUCT, "
            + "K55ADMIN.PRODUCTS_NOTNULL.NUM"
            + "  FROM K55ADMIN.PARTS_NOTNULL RIGHT JOIN "
            + "K55ADMIN.PRODUCTS_NOTNULL"
            + "    ON not(K55ADMIN.PARTS_NOTNULL.NUM between "
            + "K55ADMIN.PRODUCTS_NOTNULL.NUM and"
            + "        K55ADMIN.PRODUCTS_NOTNULL.NUM + 50)"
            + "    order by 1,2,3,4");

        expColNames = new String [] {"PART", "NUM", "PRODUCT", "NUM"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Blades", "205", "Generator", "10"},
            {"Blades", "205", "Hammer", "50"},
            {"Blades", "205", "Relay", "30"},
            {"Blades", "205", "Ruler", "30"},
            {"Blades", "205", "Sander", "20"},
            {"Blades", "205", "Screwdriver", "505"},
            {"Magnets", "10", "Hammer", "50"},
            {"Magnets", "10", "Relay", "30"},
            {"Magnets", "10", "Ruler", "30"},
            {"Magnets", "10", "Sander", "20"},
            {"Magnets", "10", "Saw", "205"},
            {"Magnets", "10", "Screwdriver", "505"},
            {"Oil", "160", "Generator", "10"},
            {"Oil", "160", "Hammer", "50"},
            {"Oil", "160", "Relay", "30"},
            {"Oil", "160", "Ruler", "30"},
            {"Oil", "160", "Sander", "20"},
            {"Oil", "160", "Saw", "205"},
            {"Oil", "160", "Screwdriver", "505"},
            {"Paper", "20", "Hammer", "50"},
            {"Paper", "20", "Relay", "30"},
            {"Paper", "20", "Ruler", "30"},
            {"Paper", "20", "Saw", "205"},
            {"Paper", "20", "Screwdriver", "505"},
            {"Plastic", "30", "Hammer", "50"},
            {"Plastic", "30", "Saw", "205"},
            {"Plastic", "30", "Screwdriver", "505"},
            {"Steel", "30", "Hammer", "50"},
            {"Steel", "30", "Saw", "205"},
            {"Steel", "30", "Screwdriver", "505"},
            {"Wire", "10", "Hammer", "50"},
            {"Wire", "10", "Relay", "30"},
            {"Wire", "10", "Ruler", "30"},
            {"Wire", "10", "Sander", "20"},
            {"Wire", "10", "Saw", "205"},
            {"Wire", "10", "Screwdriver", "505"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 211 - '>'/'<'/or in RIGHT JOIN on condition

        rs = st.executeQuery(
            "SELECT PART, K55ADMIN.PARTS_NOTNULL.NUM, PRODUCT, "
            + "K55ADMIN.PRODUCTS_NOTNULL.NUM"
            + "  FROM K55ADMIN.PARTS_NOTNULL RIGHT JOIN "
            + "K55ADMIN.PRODUCTS_NOTNULL"
            + "    ON K55ADMIN.PARTS_NOTNULL.NUM > "
            + "K55ADMIN.PRODUCTS_NOTNULL.NUM or"
            + "        K55ADMIN.PRODUCTS_NOTNULL.NUM < "
            + "K55ADMIN.PARTS_NOTNULL.NUM"
            + "    order by 1,2,3,4");

        expColNames = new String [] {"PART", "NUM", "PRODUCT", "NUM"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Blades", "205", "Generator", "10"},
            {"Blades", "205", "Hammer", "50"},
            {"Blades", "205", "Relay", "30"},
            {"Blades", "205", "Ruler", "30"},
            {"Blades", "205", "Sander", "20"},
            {"Oil", "160", "Generator", "10"},
            {"Oil", "160", "Hammer", "50"},
            {"Oil", "160", "Relay", "30"},
            {"Oil", "160", "Ruler", "30"},
            {"Oil", "160", "Sander", "20"},
            {"Paper", "20", "Generator", "10"},
            {"Plastic", "30", "Generator", "10"},
            {"Plastic", "30", "Sander", "20"},
            {"Steel", "30", "Generator", "10"},
            {"Steel", "30", "Sander", "20"},
            {null, null, "Saw", "205"},
            {null, null, "Screwdriver", "505"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 212 - not/'<>' in RIGHT JOIN on condition

        rs = st.executeQuery(
            "SELECT PART, K55ADMIN.PARTS_NOTNULL.NUM, PRODUCT, "
            + "K55ADMIN.PRODUCTS_NOTNULL.NUM"
            + "  FROM K55ADMIN.PARTS_NOTNULL RIGHT JOIN "
            + "K55ADMIN.PRODUCTS_NOTNULL"
            + "    ON not(K55ADMIN.PARTS_NOTNULL.NUM <> "
            + "K55ADMIN.PRODUCTS_NOTNULL.NUM)"
            + "    order by 1,2,3,4");

        expColNames = new String [] {"PART", "NUM", "PRODUCT", "NUM"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Blades", "205", "Saw", "205"},
            {"Magnets", "10", "Generator", "10"},
            {"Paper", "20", "Sander", "20"},
            {"Plastic", "30", "Relay", "30"},
            {"Plastic", "30", "Ruler", "30"},
            {"Steel", "30", "Relay", "30"},
            {"Steel", "30", "Ruler", "30"},
            {"Wire", "10", "Generator", "10"},
            {null, null, "Hammer", "50"},
            {null, null, "Screwdriver", "505"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        //
        //------------------------------------------------------------
        // test unit 3. plain joins, like/in/is-null/is-not-null;
        //------------------------------------------------------------

        // 301 - '<>'/'=' in LEFT JOIN ON condition;

        rs = st.executeQuery(
            "SELECT PART, K55ADMIN.PARTS_NOTNULL.NUM, PRODUCT, "
            + "K55ADMIN.PRODUCTS_NOTNULL.NUM"
            + "  FROM K55ADMIN.PARTS_NOTNULL LEFT JOIN "
            + "K55ADMIN.PRODUCTS_NOTNULL"
            + "    ON ( K55ADMIN.PARTS_NOTNULL.NUM = "
            + "K55ADMIN.PRODUCTS_NOTNULL.NUM)"
            + "    order by 1,2,3,4");

        expColNames = new String [] {"PART", "NUM", "PRODUCT", "NUM"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Blades", "205", "Saw", "205"},
            {"Magnets", "10", "Generator", "10"},
            {"Oil", "160", null, null},
            {"Paper", "20", "Sander", "20"},
            {"Plastic", "30", "Relay", "30"},
            {"Plastic", "30", "Ruler", "30"},
            {"Steel", "30", "Relay", "30"},
            {"Steel", "30", "Ruler", "30"},
            {"Wire", "10", "Generator", "10"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 302 - '='/like in LEFT JOIN ON condition

        rs = st.executeQuery(
            "SELECT PART, K55ADMIN.PARTS_NOTNULL.NUM, PRODUCT, "
            + "K55ADMIN.PRODUCTS_NOTNULL.NUM"
            + "  FROM K55ADMIN.PARTS_NOTNULL LEFT JOIN "
            + "K55ADMIN.PRODUCTS_NOTNULL"
            + "    ON ( K55ADMIN.PARTS_NOTNULL.NUM = "
            + "K55ADMIN.PRODUCTS_NOTNULL.NUM or"
            + "             K55ADMIN.PRODUCTS_NOTNULL.PRODUCT LIKE "
            + "'Screw%')"
            + "    order by 1,2,3,4");

        expColNames = new String [] {"PART", "NUM", "PRODUCT", "NUM"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Blades", "205", "Saw", "205"},
            {"Blades", "205", "Screwdriver", "505"},
            {"Magnets", "10", "Generator", "10"},
            {"Magnets", "10", "Screwdriver", "505"},
            {"Oil", "160", "Screwdriver", "505"},
            {"Paper", "20", "Sander", "20"},
            {"Paper", "20", "Screwdriver", "505"},
            {"Plastic", "30", "Relay", "30"},
            {"Plastic", "30", "Ruler", "30"},
            {"Plastic", "30", "Screwdriver", "505"},
            {"Steel", "30", "Relay", "30"},
            {"Steel", "30", "Ruler", "30"},
            {"Steel", "30", "Screwdriver", "505"},
            {"Wire", "10", "Generator", "10"},
            {"Wire", "10", "Screwdriver", "505"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 303 - '='/in in LEFT JOIN ON condition

        rs = st.executeQuery(
            "SELECT PART, K55ADMIN.PARTS_NOTNULL.NUM, PRODUCT, "
            + "K55ADMIN.PRODUCTS_NOTNULL.NUM"
            + "  FROM K55ADMIN.PARTS_NOTNULL LEFT JOIN "
            + "K55ADMIN.PRODUCTS_NOTNULL"
            + "    ON ( K55ADMIN.PRODUCTS_NOTNULL.PRODUCT in "
            + "('Screwdriver','Saw') or"
            + "          K55ADMIN.PARTS_NOTNULL.NUM = "
            + "K55ADMIN.PRODUCTS_NOTNULL.NUM)"
            + "    order by 1,2,3,4");

        expColNames = new String [] {"PART", "NUM", "PRODUCT", "NUM"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Blades", "205", "Saw", "205"},
            {"Blades", "205", "Screwdriver", "505"},
            {"Magnets", "10", "Generator", "10"},
            {"Magnets", "10", "Saw", "205"},
            {"Magnets", "10", "Screwdriver", "505"},
            {"Oil", "160", "Saw", "205"},
            {"Oil", "160", "Screwdriver", "505"},
            {"Paper", "20", "Sander", "20"},
            {"Paper", "20", "Saw", "205"},
            {"Paper", "20", "Screwdriver", "505"},
            {"Plastic", "30", "Relay", "30"},
            {"Plastic", "30", "Ruler", "30"},
            {"Plastic", "30", "Saw", "205"},
            {"Plastic", "30", "Screwdriver", "505"},
            {"Steel", "30", "Relay", "30"},
            {"Steel", "30", "Ruler", "30"},
            {"Steel", "30", "Saw", "205"},
            {"Steel", "30", "Screwdriver", "505"},
            {"Wire", "10", "Generator", "10"},
            {"Wire", "10", "Saw", "205"},
            {"Wire", "10", "Screwdriver", "505"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 304 - '='/is-null in LEFT JOIN ON condition

        rs = st.executeQuery(
            "SELECT PART, K55ADMIN.PARTS_NOTNULL.NUM, PRODUCT, "
            + "K55ADMIN.PRODUCTS_NOTNULL.NUM"
            + "  FROM K55ADMIN.PARTS_NOTNULL LEFT JOIN "
            + "K55ADMIN.PRODUCTS_NOTNULL"
            + "    ON ( K55ADMIN.PRODUCTS_NOTNULL.PRODUCT is null or"
            + "             K55ADMIN.PARTS_NOTNULL.NUM = "
            + "K55ADMIN.PRODUCTS_NOTNULL.NUM )"
            + "    order by 1,2,3,4");

        expColNames = new String [] {"PART", "NUM", "PRODUCT", "NUM"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Blades", "205", "Saw", "205"},
            {"Magnets", "10", "Generator", "10"},
            {"Oil", "160", null, null},
            {"Paper", "20", "Sander", "20"},
            {"Plastic", "30", "Relay", "30"},
            {"Plastic", "30", "Ruler", "30"},
            {"Steel", "30", "Relay", "30"},
            {"Steel", "30", "Ruler", "30"},
            {"Wire", "10", "Generator", "10"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 305 - '='/is-not-null in LEFT JOIN ON condition

        rs = st.executeQuery(
            "SELECT PART, K55ADMIN.PARTS_NOTNULL.NUM, PRODUCT, "
            + "K55ADMIN.PRODUCTS_NOTNULL.NUM"
            + "  FROM K55ADMIN.PARTS_NOTNULL LEFT JOIN "
            + "K55ADMIN.PRODUCTS_NOTNULL"
            + "    ON ( K55ADMIN.PRODUCTS_NOTNULL.PRODUCT is not null or"
            + "          K55ADMIN.PARTS_NOTNULL.NUM = "
            + "K55ADMIN.PRODUCTS_NOTNULL.NUM )"
            + "    order by 1,2,3,4");

        expColNames = new String [] {"PART", "NUM", "PRODUCT", "NUM"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Blades", "205", "Generator", "10"},
            {"Blades", "205", "Hammer", "50"},
            {"Blades", "205", "Relay", "30"},
            {"Blades", "205", "Ruler", "30"},
            {"Blades", "205", "Sander", "20"},
            {"Blades", "205", "Saw", "205"},
            {"Blades", "205", "Screwdriver", "505"},
            {"Magnets", "10", "Generator", "10"},
            {"Magnets", "10", "Hammer", "50"},
            {"Magnets", "10", "Relay", "30"},
            {"Magnets", "10", "Ruler", "30"},
            {"Magnets", "10", "Sander", "20"},
            {"Magnets", "10", "Saw", "205"},
            {"Magnets", "10", "Screwdriver", "505"},
            {"Oil", "160", "Generator", "10"},
            {"Oil", "160", "Hammer", "50"},
            {"Oil", "160", "Relay", "30"},
            {"Oil", "160", "Ruler", "30"},
            {"Oil", "160", "Sander", "20"},
            {"Oil", "160", "Saw", "205"},
            {"Oil", "160", "Screwdriver", "505"},
            {"Paper", "20", "Generator", "10"},
            {"Paper", "20", "Hammer", "50"},
            {"Paper", "20", "Relay", "30"},
            {"Paper", "20", "Ruler", "30"},
            {"Paper", "20", "Sander", "20"},
            {"Paper", "20", "Saw", "205"},
            {"Paper", "20", "Screwdriver", "505"},
            {"Plastic", "30", "Generator", "10"},
            {"Plastic", "30", "Hammer", "50"},
            {"Plastic", "30", "Relay", "30"},
            {"Plastic", "30", "Ruler", "30"},
            {"Plastic", "30", "Sander", "20"},
            {"Plastic", "30", "Saw", "205"},
            {"Plastic", "30", "Screwdriver", "505"},
            {"Steel", "30", "Generator", "10"},
            {"Steel", "30", "Hammer", "50"},
            {"Steel", "30", "Relay", "30"},
            {"Steel", "30", "Ruler", "30"},
            {"Steel", "30", "Sander", "20"},
            {"Steel", "30", "Saw", "205"},
            {"Steel", "30", "Screwdriver", "505"},
            {"Wire", "10", "Generator", "10"},
            {"Wire", "10", "Hammer", "50"},
            {"Wire", "10", "Relay", "30"},
            {"Wire", "10", "Ruler", "30"},
            {"Wire", "10", "Sander", "20"},
            {"Wire", "10", "Saw", "205"},
            {"Wire", "10", "Screwdriver", "505"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 306 - '='/'<' in LEFT JOIN ON condition

        rs = st.executeQuery(
            "SELECT PART, K55ADMIN.PARTS_NOTNULL.NUM, PRODUCT, "
            + "K55ADMIN.PRODUCTS_NOTNULL.NUM"
            + "  FROM K55ADMIN.PARTS_NOTNULL LEFT JOIN "
            + "K55ADMIN.PRODUCTS_NOTNULL"
            + "    ON ( K55ADMIN.PARTS_NOTNULL.NUM < "
            + "K55ADMIN.PRODUCTS_NOTNULL.NUM or"
            + "             K55ADMIN.PARTS_NOTNULL.NUM = "
            + "K55ADMIN.PRODUCTS_NOTNULL.NUM )"
            + "    order by 1,2,3,4");

        expColNames = new String [] {"PART", "NUM", "PRODUCT", "NUM"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Blades", "205", "Saw", "205"},
            {"Blades", "205", "Screwdriver", "505"},
            {"Magnets", "10", "Generator", "10"},
            {"Magnets", "10", "Hammer", "50"},
            {"Magnets", "10", "Relay", "30"},
            {"Magnets", "10", "Ruler", "30"},
            {"Magnets", "10", "Sander", "20"},
            {"Magnets", "10", "Saw", "205"},
            {"Magnets", "10", "Screwdriver", "505"},
            {"Oil", "160", "Saw", "205"},
            {"Oil", "160", "Screwdriver", "505"},
            {"Paper", "20", "Hammer", "50"},
            {"Paper", "20", "Relay", "30"},
            {"Paper", "20", "Ruler", "30"},
            {"Paper", "20", "Sander", "20"},
            {"Paper", "20", "Saw", "205"},
            {"Paper", "20", "Screwdriver", "505"},
            {"Plastic", "30", "Hammer", "50"},
            {"Plastic", "30", "Relay", "30"},
            {"Plastic", "30", "Ruler", "30"},
            {"Plastic", "30", "Saw", "205"},
            {"Plastic", "30", "Screwdriver", "505"},
            {"Steel", "30", "Hammer", "50"},
            {"Steel", "30", "Relay", "30"},
            {"Steel", "30", "Ruler", "30"},
            {"Steel", "30", "Saw", "205"},
            {"Steel", "30", "Screwdriver", "505"},
            {"Wire", "10", "Generator", "10"},
            {"Wire", "10", "Hammer", "50"},
            {"Wire", "10", "Relay", "30"},
            {"Wire", "10", "Ruler", "30"},
            {"Wire", "10", "Sander", "20"},
            {"Wire", "10", "Saw", "205"},
            {"Wire", "10", "Screwdriver", "505"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 307 - '='/like in LEFT JOIN ON condition

        rs = st.executeQuery(
            "SELECT PART, K55ADMIN.PARTS_NOTNULL.NUM, PRODUCT, "
            + "K55ADMIN.PRODUCTS_NOTNULL.NUM"
            + "  FROM K55ADMIN.PARTS_NOTNULL LEFT JOIN "
            + "K55ADMIN.PRODUCTS_NOTNULL"
            + "    ON ( K55ADMIN.PARTS_NOTNULL.NUM = "
            + "K55ADMIN.PRODUCTS_NOTNULL.NUM or"
            + "          K55ADMIN.PRODUCTS_NOTNULL.PRODUCT LIKE 'Nut%' )"
            + "    order by 1,2,3,4");

        expColNames = new String [] {"PART", "NUM", "PRODUCT", "NUM"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Blades", "205", "Saw", "205"},
            {"Magnets", "10", "Generator", "10"},
            {"Oil", "160", null, null},
            {"Paper", "20", "Sander", "20"},
            {"Plastic", "30", "Relay", "30"},
            {"Plastic", "30", "Ruler", "30"},
            {"Steel", "30", "Relay", "30"},
            {"Steel", "30", "Ruler", "30"},
            {"Wire", "10", "Generator", "10"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 308 - '='/like/in in RIGHT JOIN ON condition

        rs = st.executeQuery(
            "SELECT PART, K55ADMIN.PARTS_NOTNULL.NUM, PRODUCT, "
            + "K55ADMIN.PRODUCTS_NOTNULL.NUM"
            + "  FROM K55ADMIN.PARTS_NOTNULL LEFT JOIN "
            + "K55ADMIN.PRODUCTS_NOTNULL"
            + "    ON ( K55ADMIN.PRODUCTS_NOTNULL.PRODUCT in "
            + "('Bolt','Nuts') or"
            + "          K55ADMIN.PRODUCTS_NOTNULL.PRODUCT LIKE 'Power%' or"
            + "          K55ADMIN.PARTS_NOTNULL.NUM = "
            + "K55ADMIN.PRODUCTS_NOTNULL.NUM  )"
            + "    order by 1,2,3,4");

        expColNames = new String [] {"PART", "NUM", "PRODUCT", "NUM"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Blades", "205", "Saw", "205"},
            {"Magnets", "10", "Generator", "10"},
            {"Oil", "160", null, null},
            {"Paper", "20", "Sander", "20"},
            {"Plastic", "30", "Relay", "30"},
            {"Plastic", "30", "Ruler", "30"},
            {"Steel", "30", "Relay", "30"},
            {"Steel", "30", "Ruler", "30"},
            {"Wire", "10", "Generator", "10"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        //
        //------------------------------------------------------------
        // test unit 4. plain joins, only local predicates, no join predicate;
        //------------------------------------------------------------

        // 01 - local '=' pred on tuple-preserving operand

        rs = st.executeQuery(
            "SELECT PART, K55ADMIN.PARTS_NOTNULL.NUM, PRODUCT, "
            + "K55ADMIN.PRODUCTS_NOTNULL.NUM"
            + "  FROM K55ADMIN.PARTS_NOTNULL LEFT JOIN "
            + "K55ADMIN.PRODUCTS_NOTNULL"
            + "    ON K55ADMIN.PARTS_NOTNULL.NUM = 10"
            + "    order by 1,2,3,4");

        expColNames = new String [] {"PART", "NUM", "PRODUCT", "NUM"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Blades", "205", null, null},
            {"Magnets", "10", "Generator", "10"},
            {"Magnets", "10", "Hammer", "50"},
            {"Magnets", "10", "Relay", "30"},
            {"Magnets", "10", "Ruler", "30"},
            {"Magnets", "10", "Sander", "20"},
            {"Magnets", "10", "Saw", "205"},
            {"Magnets", "10", "Screwdriver", "505"},
            {"Oil", "160", null, null},
            {"Paper", "20", null, null},
            {"Plastic", "30", null, null},
            {"Steel", "30", null, null},
            {"Wire", "10", "Generator", "10"},
            {"Wire", "10", "Hammer", "50"},
            {"Wire", "10", "Relay", "30"},
            {"Wire", "10", "Ruler", "30"},
            {"Wire", "10", "Sander", "20"},
            {"Wire", "10", "Saw", "205"},
            {"Wire", "10", "Screwdriver", "505"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 402 - local IN pred on tuple-preserving operand

        rs = st.executeQuery(
            "SELECT PART, K55ADMIN.PARTS_NOTNULL.NUM, PRODUCT, "
            + "K55ADMIN.PRODUCTS_NOTNULL.NUM"
            + "  FROM K55ADMIN.PARTS_NOTNULL LEFT JOIN "
            + "K55ADMIN.PRODUCTS_NOTNULL"
            + "    ON K55ADMIN.PARTS_NOTNULL.NUM IN (160, 205)"
            + "    order by 1,2,3,4");

        expColNames = new String [] {"PART", "NUM", "PRODUCT", "NUM"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Blades", "205", "Generator", "10"},
            {"Blades", "205", "Hammer", "50"},
            {"Blades", "205", "Relay", "30"},
            {"Blades", "205", "Ruler", "30"},
            {"Blades", "205", "Sander", "20"},
            {"Blades", "205", "Saw", "205"},
            {"Blades", "205", "Screwdriver", "505"},
            {"Magnets", "10", null, null},
            {"Oil", "160", "Generator", "10"},
            {"Oil", "160", "Hammer", "50"},
            {"Oil", "160", "Relay", "30"},
            {"Oil", "160", "Ruler", "30"},
            {"Oil", "160", "Sander", "20"},
            {"Oil", "160", "Saw", "205"},
            {"Oil", "160", "Screwdriver", "505"},
            {"Paper", "20", null, null},
            {"Plastic", "30", null, null},
            {"Steel", "30", null, null},
            {"Wire", "10", null, null}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 403 - local between pred on tuple-preserving operand

        rs = st.executeQuery(
            "SELECT PART, K55ADMIN.PARTS_NOTNULL.NUM, PRODUCT, "
            + "K55ADMIN.PRODUCTS_NOTNULL.NUM"
            + "  FROM K55ADMIN.PARTS_NOTNULL LEFT JOIN "
            + "K55ADMIN.PRODUCTS_NOTNULL"
            + "    ON K55ADMIN.PARTS_NOTNULL.NUM between 150 and 250"
            + "    order by 1,2,3,4");

        expColNames = new String [] {"PART", "NUM", "PRODUCT", "NUM"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Blades", "205", "Generator", "10"},
            {"Blades", "205", "Hammer", "50"},
            {"Blades", "205", "Relay", "30"},
            {"Blades", "205", "Ruler", "30"},
            {"Blades", "205", "Sander", "20"},
            {"Blades", "205", "Saw", "205"},
            {"Blades", "205", "Screwdriver", "505"},
            {"Magnets", "10", null, null},
            {"Oil", "160", "Generator", "10"},
            {"Oil", "160", "Hammer", "50"},
            {"Oil", "160", "Relay", "30"},
            {"Oil", "160", "Ruler", "30"},
            {"Oil", "160", "Sander", "20"},
            {"Oil", "160", "Saw", "205"},
            {"Oil", "160", "Screwdriver", "505"},
            {"Paper", "20", null, null},
            {"Plastic", "30", null, null},
            {"Steel", "30", null, null},
            {"Wire", "10", null, null}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 404 - local '=' pred on null-producing operand

        rs = st.executeQuery(
            "SELECT PART, K55ADMIN.PARTS_NOTNULL.NUM, PRODUCT, "
            + "K55ADMIN.PRODUCTS_NOTNULL.NUM"
            + "  FROM K55ADMIN.PARTS_NOTNULL RIGHT JOIN "
            + "K55ADMIN.PRODUCTS_NOTNULL"
            + "    ON K55ADMIN.PARTS_NOTNULL.NUM = 10"
            + "    order by 1,2,3,4");

        expColNames = new String [] {"PART", "NUM", "PRODUCT", "NUM"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Magnets", "10", "Generator", "10"},
            {"Magnets", "10", "Hammer", "50"},
            {"Magnets", "10", "Relay", "30"},
            {"Magnets", "10", "Ruler", "30"},
            {"Magnets", "10", "Sander", "20"},
            {"Magnets", "10", "Saw", "205"},
            {"Magnets", "10", "Screwdriver", "505"},
            {"Wire", "10", "Generator", "10"},
            {"Wire", "10", "Hammer", "50"},
            {"Wire", "10", "Relay", "30"},
            {"Wire", "10", "Ruler", "30"},
            {"Wire", "10", "Sander", "20"},
            {"Wire", "10", "Saw", "205"},
            {"Wire", "10", "Screwdriver", "505"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 405 - local IN pred on null-producing operand

        rs = st.executeQuery(
            "SELECT PART, K55ADMIN.PARTS_NOTNULL.NUM, PRODUCT, "
            + "K55ADMIN.PRODUCTS_NOTNULL.NUM"
            + "  FROM K55ADMIN.PARTS_NOTNULL RIGHT JOIN "
            + "K55ADMIN.PRODUCTS_NOTNULL"
            + "    ON K55ADMIN.PARTS_NOTNULL.NUM IN (10, 20)"
            + "    order by 1,2,3,4");

        expColNames = new String [] {"PART", "NUM", "PRODUCT", "NUM"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Magnets", "10", "Generator", "10"},
            {"Magnets", "10", "Hammer", "50"},
            {"Magnets", "10", "Relay", "30"},
            {"Magnets", "10", "Ruler", "30"},
            {"Magnets", "10", "Sander", "20"},
            {"Magnets", "10", "Saw", "205"},
            {"Magnets", "10", "Screwdriver", "505"},
            {"Paper", "20", "Generator", "10"},
            {"Paper", "20", "Hammer", "50"},
            {"Paper", "20", "Relay", "30"},
            {"Paper", "20", "Ruler", "30"},
            {"Paper", "20", "Sander", "20"},
            {"Paper", "20", "Saw", "205"},
            {"Paper", "20", "Screwdriver", "505"},
            {"Wire", "10", "Generator", "10"},
            {"Wire", "10", "Hammer", "50"},
            {"Wire", "10", "Relay", "30"},
            {"Wire", "10", "Ruler", "30"},
            {"Wire", "10", "Sander", "20"},
            {"Wire", "10", "Saw", "205"},
            {"Wire", "10", "Screwdriver", "505"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 406 - local between pred on null-producing operand

        rs = st.executeQuery(
            "SELECT PART, K55ADMIN.PARTS_NOTNULL.NUM, PRODUCT, "
            + "K55ADMIN.PRODUCTS_NOTNULL.NUM"
            + "  FROM K55ADMIN.PARTS_NOTNULL RIGHT JOIN "
            + "K55ADMIN.PRODUCTS_NOTNULL"
            + "    ON K55ADMIN.PARTS_NOTNULL.NUM between 10 and 50"
            + "    order by 1,2,3,4");

        expColNames = new String [] {"PART", "NUM", "PRODUCT", "NUM"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Magnets", "10", "Generator", "10"},
            {"Magnets", "10", "Hammer", "50"},
            {"Magnets", "10", "Relay", "30"},
            {"Magnets", "10", "Ruler", "30"},
            {"Magnets", "10", "Sander", "20"},
            {"Magnets", "10", "Saw", "205"},
            {"Magnets", "10", "Screwdriver", "505"},
            {"Paper", "20", "Generator", "10"},
            {"Paper", "20", "Hammer", "50"},
            {"Paper", "20", "Relay", "30"},
            {"Paper", "20", "Ruler", "30"},
            {"Paper", "20", "Sander", "20"},
            {"Paper", "20", "Saw", "205"},
            {"Paper", "20", "Screwdriver", "505"},
            {"Plastic", "30", "Generator", "10"},
            {"Plastic", "30", "Hammer", "50"},
            {"Plastic", "30", "Relay", "30"},
            {"Plastic", "30", "Ruler", "30"},
            {"Plastic", "30", "Sander", "20"},
            {"Plastic", "30", "Saw", "205"},
            {"Plastic", "30", "Screwdriver", "505"},
            {"Steel", "30", "Generator", "10"},
            {"Steel", "30", "Hammer", "50"},
            {"Steel", "30", "Relay", "30"},
            {"Steel", "30", "Ruler", "30"},
            {"Steel", "30", "Sander", "20"},
            {"Steel", "30", "Saw", "205"},
            {"Steel", "30", "Screwdriver", "505"},
            {"Wire", "10", "Generator", "10"},
            {"Wire", "10", "Hammer", "50"},
            {"Wire", "10", "Relay", "30"},
            {"Wire", "10", "Ruler", "30"},
            {"Wire", "10", "Sander", "20"},
            {"Wire", "10", "Saw", "205"},
            {"Wire", "10", "Screwdriver", "505"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        //
        //------------------------------------------------------------
        // unit 5. plain joins, local predicate and join predicate;
        //------------------------------------------------------------

        // 501 - local pred on tuple-preserving operand, '=' join predicate,
        //       and;

        rs = st.executeQuery(
            "SELECT PART, K55ADMIN.PARTS_NOTNULL.NUM, PRODUCT, "
            + "K55ADMIN.PRODUCTS_NOTNULL.NUM"
            + "  FROM K55ADMIN.PARTS_NOTNULL LEFT JOIN "
            + "K55ADMIN.PRODUCTS_NOTNULL"
            + "    ON K55ADMIN.PARTS_NOTNULL.NUM = "
            + "K55ADMIN.PRODUCTS_NOTNULL.NUM and"
            + "       K55ADMIN.PARTS_NOTNULL.PART <> 'Wire'"
            + "    order by 1,2,3,4");

        expColNames = new String [] {"PART", "NUM", "PRODUCT", "NUM"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Blades", "205", "Saw", "205"},
            {"Magnets", "10", "Generator", "10"},
            {"Oil", "160", null, null},
            {"Paper", "20", "Sander", "20"},
            {"Plastic", "30", "Relay", "30"},
            {"Plastic", "30", "Ruler", "30"},
            {"Steel", "30", "Relay", "30"},
            {"Steel", "30", "Ruler", "30"},
            {"Wire", "10", null, null}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 502 - local pred on both operands, '=' join predicate, and

        rs = st.executeQuery(
            "SELECT K55ADMIN.PARTS_NOTNULL.*, K55ADMIN.PRODUCTS_NOTNULL.*"
            + "  FROM K55ADMIN.PARTS_NOTNULL LEFT JOIN "
            + "K55ADMIN.PRODUCTS_NOTNULL"
            + "    ON K55ADMIN.PARTS_NOTNULL.NUM = "
            + "K55ADMIN.PRODUCTS_NOTNULL.NUM and"
            + "       (K55ADMIN.PRODUCTS_NOTNULL.PRICE>8 or "
            + "K55ADMIN.PARTS_NOTNULL.PART = 'Steel')"
            + "        and K55ADMIN.PRODUCTS_NOTNULL.NUM > 20"
            + "    order by 1,2,3,4,5,6");

        expColNames = new String [] {"PART", "NUM", "SUPPLIER", "NUM", "PRODUCT", "PRICE"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Blades", "205", "Ace-Steel", "205", "Saw", "18.90"},
            {"Magnets", "10", "Bateman", null, null, null},
            {"Oil", "160", "Western-Chem", null, null, null},
            {"Paper", "20", "Ace-Steel", null, null, null},
            {"Plastic", "30", "Plastik-Corp", "30", "Ruler", "8.75"},
            {"Steel", "30", "ACWF", "30", "Relay", "7.55"},
            {"Steel", "30", "ACWF", "30", "Ruler", "8.75"},
            {"Wire", "10", "ACWF", null, null, null}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 503 - local pred on both operands, '=' join predicate,
        //       and/or

        rs = st.executeQuery(
            "SELECT K55ADMIN.PARTS_NOTNULL.*, K55ADMIN.PRODUCTS_NOTNULL.*"
            + "  FROM K55ADMIN.PARTS_NOTNULL LEFT JOIN "
            + "K55ADMIN.PRODUCTS_NOTNULL"
            + "    ON K55ADMIN.PARTS_NOTNULL.NUM = "
            + "K55ADMIN.PRODUCTS_NOTNULL.NUM and"
            + "       (K55ADMIN.PRODUCTS_NOTNULL.PRICE>8 or "
            + "K55ADMIN.PARTS_NOTNULL.PART = 'Steel'"
            + "        or K55ADMIN.PRODUCTS_NOTNULL.NUM > 20)"
            + "    order by 1,2,3,4,5,6");

        expColNames = new String [] {"PART", "NUM", "SUPPLIER", "NUM", "PRODUCT", "PRICE"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Blades", "205", "Ace-Steel", "205", "Saw", "18.90"},
            {"Magnets", "10", "Bateman", "10", "Generator", "45.75"},
            {"Oil", "160", "Western-Chem", null, null, null},
            {"Paper", "20", "Ace-Steel", "20", "Sander", "35.75"},
            {"Plastic", "30", "Plastik-Corp", "30", "Relay", "7.55"},
            {"Plastic", "30", "Plastik-Corp", "30", "Ruler", "8.75"},
            {"Steel", "30", "ACWF", "30", "Relay", "7.55"},
            {"Steel", "30", "ACWF", "30", "Ruler", "8.75"},
            {"Wire", "10", "ACWF", "10", "Generator", "45.75"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 504 - local '=' pred on null-producing operand OR '='
        //       join predicate

        rs = st.executeQuery(
            "SELECT PART, K55ADMIN.PARTS_NOTNULL.NUM, PRODUCT, "
            + "K55ADMIN.PRODUCTS_NOTNULL.NUM"
            + "  FROM K55ADMIN.PARTS_NOTNULL RIGHT JOIN "
            + "K55ADMIN.PRODUCTS_NOTNULL"
            + "    ON K55ADMIN.PARTS_NOTNULL.NUM = 10 OR"
            + "       K55ADMIN.PARTS_NOTNULL.NUM = "
            + "K55ADMIN.PRODUCTS_NOTNULL.NUM"
            + "    order by 1,2,3,4");

        expColNames = new String [] {"PART", "NUM", "PRODUCT", "NUM"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Blades", "205", "Saw", "205"},
            {"Magnets", "10", "Generator", "10"},
            {"Magnets", "10", "Hammer", "50"},
            {"Magnets", "10", "Relay", "30"},
            {"Magnets", "10", "Ruler", "30"},
            {"Magnets", "10", "Sander", "20"},
            {"Magnets", "10", "Saw", "205"},
            {"Magnets", "10", "Screwdriver", "505"},
            {"Paper", "20", "Sander", "20"},
            {"Plastic", "30", "Relay", "30"},
            {"Plastic", "30", "Ruler", "30"},
            {"Steel", "30", "Relay", "30"},
            {"Steel", "30", "Ruler", "30"},
            {"Wire", "10", "Generator", "10"},
            {"Wire", "10", "Hammer", "50"},
            {"Wire", "10", "Relay", "30"},
            {"Wire", "10", "Ruler", "30"},
            {"Wire", "10", "Sander", "20"},
            {"Wire", "10", "Saw", "205"},
            {"Wire", "10", "Screwdriver", "505"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 505 - local '=' pred on tuple-preserving operand OR '='
        //       join predicate

        rs = st.executeQuery(
            "SELECT PART, K55ADMIN.PARTS_NOTNULL.NUM, PRODUCT, "
            + "K55ADMIN.PRODUCTS_NOTNULL.NUM"
            + "  FROM K55ADMIN.PARTS_NOTNULL LEFT JOIN "
            + "K55ADMIN.PRODUCTS_NOTNULL"
            + "    ON K55ADMIN.PARTS_NOTNULL.NUM = 10 OR"
            + "       K55ADMIN.PARTS_NOTNULL.NUM = "
            + "K55ADMIN.PRODUCTS_NOTNULL.NUM"
            + "    order by 1,2,3,4");

        expColNames = new String [] {"PART", "NUM", "PRODUCT", "NUM"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Blades", "205", "Saw", "205"},
            {"Magnets", "10", "Generator", "10"},
            {"Magnets", "10", "Hammer", "50"},
            {"Magnets", "10", "Relay", "30"},
            {"Magnets", "10", "Ruler", "30"},
            {"Magnets", "10", "Sander", "20"},
            {"Magnets", "10", "Saw", "205"},
            {"Magnets", "10", "Screwdriver", "505"},
            {"Oil", "160", null, null},
            {"Paper", "20", "Sander", "20"},
            {"Plastic", "30", "Relay", "30"},
            {"Plastic", "30", "Ruler", "30"},
            {"Steel", "30", "Relay", "30"},
            {"Steel", "30", "Ruler", "30"},
            {"Wire", "10", "Generator", "10"},
            {"Wire", "10", "Hammer", "50"},
            {"Wire", "10", "Relay", "30"},
            {"Wire", "10", "Ruler", "30"},
            {"Wire", "10", "Sander", "20"},
            {"Wire", "10", "Saw", "205"},
            {"Wire", "10", "Screwdriver", "505"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 506 - local '=' pred on tuple-preserving operand AND
        //       '<>' join predicate

        rs = st.executeQuery(
            "SELECT PART, K55ADMIN.PARTS_NOTNULL.NUM, PRODUCT, "
            + "K55ADMIN.PRODUCTS_NOTNULL.NUM"
            + "  FROM K55ADMIN.PARTS_NOTNULL LEFT JOIN "
            + "K55ADMIN.PRODUCTS_NOTNULL"
            + "    ON K55ADMIN.PARTS_NOTNULL.NUM = 1000 AND"
            + "       K55ADMIN.PARTS_NOTNULL.NUM <> "
            + "K55ADMIN.PRODUCTS_NOTNULL.NUM"
            + "    order by 1,2,3,4");

        expColNames = new String [] {"PART", "NUM", "PRODUCT", "NUM"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Blades", "205", null, null},
            {"Magnets", "10", null, null},
            {"Oil", "160", null, null},
            {"Paper", "20", null, null},
            {"Plastic", "30", null, null},
            {"Steel", "30", null, null},
            {"Wire", "10", null, null}
        };

        JDBC.assertFullResultSet(rs, expRS);

        //
        //------------------------------------------------------------
        // test unit 6. plain joins, '1=0' or '2>1' predicate,
        //              w/ or w/o join predicate;
        //------------------------------------------------------------

        // 601 - '=' join predicate and '2>1'

        rs = st.executeQuery(
            "SELECT PART, K55ADMIN.PARTS_NOTNULL.NUM, PRODUCT, "
            + "K55ADMIN.PRODUCTS_NOTNULL.NUM"
            + "  FROM K55ADMIN.PARTS_NOTNULL LEFT JOIN "
            + "K55ADMIN.PRODUCTS_NOTNULL"
            + "    ON K55ADMIN.PARTS_NOTNULL.NUM = "
            + "K55ADMIN.PRODUCTS_NOTNULL.NUM and"
            + "       2>1"
            + "    order by 1,2,3,4");

        expColNames = new String [] {"PART", "NUM", "PRODUCT", "NUM"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Blades", "205", "Saw", "205"},
            {"Magnets", "10", "Generator", "10"},
            {"Oil", "160", null, null},
            {"Paper", "20", "Sander", "20"},
            {"Plastic", "30", "Relay", "30"},
            {"Plastic", "30", "Ruler", "30"},
            {"Steel", "30", "Relay", "30"},
            {"Steel", "30", "Ruler", "30"},
            {"Wire", "10", "Generator", "10"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 602 - '=' join predicate and '1=0'

        rs = st.executeQuery(
            "SELECT PART, K55ADMIN.PARTS_NOTNULL.NUM, PRODUCT, "
            + "K55ADMIN.PRODUCTS_NOTNULL.NUM"
            + "  FROM K55ADMIN.PARTS_NOTNULL LEFT JOIN "
            + "K55ADMIN.PRODUCTS_NOTNULL"
            + "    ON K55ADMIN.PARTS_NOTNULL.NUM = "
            + "K55ADMIN.PRODUCTS_NOTNULL.NUM and"
            + "       1=0"
            + "    order by 1,2,3,4");

        expColNames = new String [] {"PART", "NUM", "PRODUCT", "NUM"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Blades", "205", null, null},
            {"Magnets", "10", null, null},
            {"Oil", "160", null, null},
            {"Paper", "20", null, null},
            {"Plastic", "30", null, null},
            {"Steel", "30", null, null},
            {"Wire", "10", null, null}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 603 - '=' join predicate or '2>1'

        rs = st.executeQuery(
            "SELECT PART, K55ADMIN.PARTS_NOTNULL.NUM, PRODUCT, "
            + "K55ADMIN.PRODUCTS_NOTNULL.NUM"
            + "  FROM K55ADMIN.PARTS_NOTNULL LEFT JOIN "
            + "K55ADMIN.PRODUCTS_NOTNULL"
            + "    ON K55ADMIN.PARTS_NOTNULL.NUM = "
            + "K55ADMIN.PRODUCTS_NOTNULL.NUM or"
            + "       2>1"
            + "    order by 1,2,3,4");

        expColNames = new String [] {"PART", "NUM", "PRODUCT", "NUM"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Blades", "205", "Generator", "10"},
            {"Blades", "205", "Hammer", "50"},
            {"Blades", "205", "Relay", "30"},
            {"Blades", "205", "Ruler", "30"},
            {"Blades", "205", "Sander", "20"},
            {"Blades", "205", "Saw", "205"},
            {"Blades", "205", "Screwdriver", "505"},
            {"Magnets", "10", "Generator", "10"},
            {"Magnets", "10", "Hammer", "50"},
            {"Magnets", "10", "Relay", "30"},
            {"Magnets", "10", "Ruler", "30"},
            {"Magnets", "10", "Sander", "20"},
            {"Magnets", "10", "Saw", "205"},
            {"Magnets", "10", "Screwdriver", "505"},
            {"Oil", "160", "Generator", "10"},
            {"Oil", "160", "Hammer", "50"},
            {"Oil", "160", "Relay", "30"},
            {"Oil", "160", "Ruler", "30"},
            {"Oil", "160", "Sander", "20"},
            {"Oil", "160", "Saw", "205"},
            {"Oil", "160", "Screwdriver", "505"},
            {"Paper", "20", "Generator", "10"},
            {"Paper", "20", "Hammer", "50"},
            {"Paper", "20", "Relay", "30"},
            {"Paper", "20", "Ruler", "30"},
            {"Paper", "20", "Sander", "20"},
            {"Paper", "20", "Saw", "205"},
            {"Paper", "20", "Screwdriver", "505"},
            {"Plastic", "30", "Generator", "10"},
            {"Plastic", "30", "Hammer", "50"},
            {"Plastic", "30", "Relay", "30"},
            {"Plastic", "30", "Ruler", "30"},
            {"Plastic", "30", "Sander", "20"},
            {"Plastic", "30", "Saw", "205"},
            {"Plastic", "30", "Screwdriver", "505"},
            {"Steel", "30", "Generator", "10"},
            {"Steel", "30", "Hammer", "50"},
            {"Steel", "30", "Relay", "30"},
            {"Steel", "30", "Ruler", "30"},
            {"Steel", "30", "Sander", "20"},
            {"Steel", "30", "Saw", "205"},
            {"Steel", "30", "Screwdriver", "505"},
            {"Wire", "10", "Generator", "10"},
            {"Wire", "10", "Hammer", "50"},
            {"Wire", "10", "Relay", "30"},
            {"Wire", "10", "Ruler", "30"},
            {"Wire", "10", "Sander", "20"},
            {"Wire", "10", "Saw", "205"},
            {"Wire", "10", "Screwdriver", "505"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 604 - '=' join predicate or '1=0'

        rs = st.executeQuery(
            "SELECT PART, K55ADMIN.PARTS_NOTNULL.NUM, PRODUCT, "
            + "K55ADMIN.PRODUCTS_NOTNULL.NUM"
            + "  FROM K55ADMIN.PARTS_NOTNULL LEFT JOIN "
            + "K55ADMIN.PRODUCTS_NOTNULL"
            + "    ON K55ADMIN.PARTS_NOTNULL.NUM = "
            + "K55ADMIN.PRODUCTS_NOTNULL.NUM or"
            + "       1=0"
            + "    order by 1,2,3,4");

        expColNames = new String [] {"PART", "NUM", "PRODUCT", "NUM"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Blades", "205", "Saw", "205"},
            {"Magnets", "10", "Generator", "10"},
            {"Oil", "160", null, null},
            {"Paper", "20", "Sander", "20"},
            {"Plastic", "30", "Relay", "30"},
            {"Plastic", "30", "Ruler", "30"},
            {"Steel", "30", "Relay", "30"},
            {"Steel", "30", "Ruler", "30"},
            {"Wire", "10", "Generator", "10"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 605 - '2>1' in ON condition

        rs = st.executeQuery(
            "SELECT PART, K55ADMIN.PARTS_NOTNULL.NUM, PRODUCT, "
            + "K55ADMIN.PRODUCTS_NOTNULL.NUM"
            + "  FROM K55ADMIN.PARTS_NOTNULL LEFT JOIN "
            + "K55ADMIN.PRODUCTS_NOTNULL"
            + "    ON 2>1"
            + "    order by 1,2,3,4");

        expColNames = new String [] {"PART", "NUM", "PRODUCT", "NUM"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Blades", "205", "Generator", "10"},
            {"Blades", "205", "Hammer", "50"},
            {"Blades", "205", "Relay", "30"},
            {"Blades", "205", "Ruler", "30"},
            {"Blades", "205", "Sander", "20"},
            {"Blades", "205", "Saw", "205"},
            {"Blades", "205", "Screwdriver", "505"},
            {"Magnets", "10", "Generator", "10"},
            {"Magnets", "10", "Hammer", "50"},
            {"Magnets", "10", "Relay", "30"},
            {"Magnets", "10", "Ruler", "30"},
            {"Magnets", "10", "Sander", "20"},
            {"Magnets", "10", "Saw", "205"},
            {"Magnets", "10", "Screwdriver", "505"},
            {"Oil", "160", "Generator", "10"},
            {"Oil", "160", "Hammer", "50"},
            {"Oil", "160", "Relay", "30"},
            {"Oil", "160", "Ruler", "30"},
            {"Oil", "160", "Sander", "20"},
            {"Oil", "160", "Saw", "205"},
            {"Oil", "160", "Screwdriver", "505"},
            {"Paper", "20", "Generator", "10"},
            {"Paper", "20", "Hammer", "50"},
            {"Paper", "20", "Relay", "30"},
            {"Paper", "20", "Ruler", "30"},
            {"Paper", "20", "Sander", "20"},
            {"Paper", "20", "Saw", "205"},
            {"Paper", "20", "Screwdriver", "505"},
            {"Plastic", "30", "Generator", "10"},
            {"Plastic", "30", "Hammer", "50"},
            {"Plastic", "30", "Relay", "30"},
            {"Plastic", "30", "Ruler", "30"},
            {"Plastic", "30", "Sander", "20"},
            {"Plastic", "30", "Saw", "205"},
            {"Plastic", "30", "Screwdriver", "505"},
            {"Steel", "30", "Generator", "10"},
            {"Steel", "30", "Hammer", "50"},
            {"Steel", "30", "Relay", "30"},
            {"Steel", "30", "Ruler", "30"},
            {"Steel", "30", "Sander", "20"},
            {"Steel", "30", "Saw", "205"},
            {"Steel", "30", "Screwdriver", "505"},
            {"Wire", "10", "Generator", "10"},
            {"Wire", "10", "Hammer", "50"},
            {"Wire", "10", "Relay", "30"},
            {"Wire", "10", "Ruler", "30"},
            {"Wire", "10", "Sander", "20"},
            {"Wire", "10", "Saw", "205"},
            {"Wire", "10", "Screwdriver", "505"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 606 - '1=0' in ON condition

        rs = st.executeQuery(
            "SELECT PART, K55ADMIN.PARTS_NOTNULL.NUM, PRODUCT, "
            + "K55ADMIN.PRODUCTS_NOTNULL.NUM"
            + "  FROM K55ADMIN.PARTS_NOTNULL LEFT JOIN "
            + "K55ADMIN.PRODUCTS_NOTNULL"
            + "    ON 1=0"
            + "    order by 1,2,3,4");

        expColNames = new String [] {"PART", "NUM", "PRODUCT", "NUM"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Blades", "205", null, null},
            {"Magnets", "10", null, null},
            {"Oil", "160", null, null},
            {"Paper", "20", null, null},
            {"Plastic", "30", null, null},
            {"Steel", "30", null, null},
            {"Wire", "10", null, null}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 607 - '1=1' in ON condition

        rs = st.executeQuery(
            "SELECT PART, K55ADMIN.PARTS_NOTNULL.NUM, PRODUCT, "
            + "K55ADMIN.PRODUCTS_NOTNULL.NUM"
            + "  FROM K55ADMIN.PARTS_NOTNULL LEFT JOIN "
            + "K55ADMIN.PRODUCTS_NOTNULL"
            + "    ON 1=1"
            + "    order by 1,2,3,4");

        expColNames = new String [] {"PART", "NUM", "PRODUCT", "NUM"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Blades", "205", "Generator", "10"},
            {"Blades", "205", "Hammer", "50"},
            {"Blades", "205", "Relay", "30"},
            {"Blades", "205", "Ruler", "30"},
            {"Blades", "205", "Sander", "20"},
            {"Blades", "205", "Saw", "205"},
            {"Blades", "205", "Screwdriver", "505"},
            {"Magnets", "10", "Generator", "10"},
            {"Magnets", "10", "Hammer", "50"},
            {"Magnets", "10", "Relay", "30"},
            {"Magnets", "10", "Ruler", "30"},
            {"Magnets", "10", "Sander", "20"},
            {"Magnets", "10", "Saw", "205"},
            {"Magnets", "10", "Screwdriver", "505"},
            {"Oil", "160", "Generator", "10"},
            {"Oil", "160", "Hammer", "50"},
            {"Oil", "160", "Relay", "30"},
            {"Oil", "160", "Ruler", "30"},
            {"Oil", "160", "Sander", "20"},
            {"Oil", "160", "Saw", "205"},
            {"Oil", "160", "Screwdriver", "505"},
            {"Paper", "20", "Generator", "10"},
            {"Paper", "20", "Hammer", "50"},
            {"Paper", "20", "Relay", "30"},
            {"Paper", "20", "Ruler", "30"},
            {"Paper", "20", "Sander", "20"},
            {"Paper", "20", "Saw", "205"},
            {"Paper", "20", "Screwdriver", "505"},
            {"Plastic", "30", "Generator", "10"},
            {"Plastic", "30", "Hammer", "50"},
            {"Plastic", "30", "Relay", "30"},
            {"Plastic", "30", "Ruler", "30"},
            {"Plastic", "30", "Sander", "20"},
            {"Plastic", "30", "Saw", "205"},
            {"Plastic", "30", "Screwdriver", "505"},
            {"Steel", "30", "Generator", "10"},
            {"Steel", "30", "Hammer", "50"},
            {"Steel", "30", "Relay", "30"},
            {"Steel", "30", "Ruler", "30"},
            {"Steel", "30", "Sander", "20"},
            {"Steel", "30", "Saw", "205"},
            {"Steel", "30", "Screwdriver", "505"},
            {"Wire", "10", "Generator", "10"},
            {"Wire", "10", "Hammer", "50"},
            {"Wire", "10", "Relay", "30"},
            {"Wire", "10", "Ruler", "30"},
            {"Wire", "10", "Sander", "20"},
            {"Wire", "10", "Saw", "205"},
            {"Wire", "10", "Screwdriver", "505"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // coj206.clp
        //------------------------------------------------------------
        // test unit 2. Insert with OUTER JOIN subquery;
        //------------------------------------------------------------

        // create a table with nullable columns

        st.executeUpdate(
            "create table k55admin.tmp_products"
            + "      (num    smallint,"
            + "       product  char(15),"
            + "       price    decimal(7,2))");

        // create a table with non-nullable columns

        st.executeUpdate(
            "create table k55admin.tmp_nn_products"
            + "      (num    smallint not null,"
            + "       product  char(15) not null,"
            + "       price    decimal(7,2) not null)");

        // 101 - 'between' in LEFT JOIN ON condition
        //       insert into table with nullable columns

        st.executeUpdate(
            "insert into k55admin.tmp_products "
            + "select pt.num, product, price"
            + " from k55admin.parts p left join k55admin.products pt"
            + " on   (p.num between pt.num and pt.num + 5)");

        // 102 - select from the table with newly insert tuples

        rs = st.executeQuery(
            "select * from k55admin.tmp_products order by 1, 2, 3");

        expColNames = new String [] {"NUM", "PRODUCT", "PRICE"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"10", "Generator", "45.75"},
            {"10", "Generator", "45.75"},
            {"20", "Sander", "35.75"},
            {"20", null, null},
            {"30", "Relay", "7.55"},
            {"30", "Relay", "7.55"},
            {"30", "Relay", "7.55"},
            {"30", "Ruler", "8.75"},
            {"30", "Ruler", "8.75"},
            {"30", "Ruler", "8.75"},
            {"205", "Saw", "18.90"},
            {null, null, null}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 103 - delete the above inserted tuples

        assertUpdateCount(st, 12,
            "delete from k55admin.tmp_products");

        // 104 - 'between' in RIGHT JOIN ON condition;
        //       insert into table with nullable columns

        st.executeUpdate(
            "insert into k55admin.tmp_products "
            + "select pt.num, product, price"
            + " from k55admin.parts p right join k55admin.products pt"
            + " on   (p.num between pt.num and pt.num + 5)");

        // 105 - select from the table with newly insert tuples

        rs = st.executeQuery(
            "select * from k55admin.tmp_products order by 1, 2, 3");

        expColNames = new String [] {"NUM", "PRODUCT", "PRICE"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"10", "Generator", "45.75"},
            {"10", "Generator", "45.75"},
            {"20", "Sander", "35.75"},
            {"20", null, null},
            {"30", "Relay", "7.55"},
            {"30", "Relay", "7.55"},
            {"30", "Relay", "7.55"},
            {"30", "Ruler", "8.75"},
            {"30", "Ruler", "8.75"},
            {"30", "Ruler", "8.75"},
            {"50", "Hammer", "5.75"},
            {"205", "Saw", "18.90"},
            {"505", "Screwdriver", "3.70"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 106 - delete the above inserted tuples

        assertUpdateCount(st, 13,
            "delete from k55admin.tmp_products");

        // 107 - 'between' in INNER JOIN ON condition;
        //       insert into table with nullable columns

        st.executeUpdate(
            "insert into k55admin.tmp_products "
            + "select pt.num, product, price"
            + " from k55admin.parts p inner join k55admin.products pt"
            + " on   (p.num between pt.num and pt.num + 5)");

        // 108 - select from the table with newly insert tuples

        rs = st.executeQuery(
            "select * from k55admin.tmp_products order by 1, 2, 3");

        expColNames = new String [] {"NUM", "PRODUCT", "PRICE"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"10", "Generator", "45.75"},
            {"10", "Generator", "45.75"},
            {"20", "Sander", "35.75"},
            {"20", null, null},
            {"30", "Relay", "7.55"},
            {"30", "Relay", "7.55"},
            {"30", "Relay", "7.55"},
            {"30", "Ruler", "8.75"},
            {"30", "Ruler", "8.75"},
            {"30", "Ruler", "8.75"},
            {"205", "Saw", "18.90"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        //
        //------------------------------------------------------------
        // test unit 3. Update with OUTER JOIN subquery;
        //------------------------------------------------------------

        // populate the table

        assertUpdateCount(st, 11,
            "delete from k55admin.tmp_products");

        st.executeUpdate(
            " insert into k55admin.tmp_products "
            + "select * from k55admin.products");

        // 301 - 'between' in LEFT JOIN ON condition as a subquery
        //       update table with nullable columns

        assertUpdateCount(st, 6,
            "update k55admin.tmp_products "
            + "set    price = price *1.1"
            + " where  num in (select p.num"
            + "               from k55admin.parts p left join "
            + "k55admin.products pt"
            + "               on   (p.num between pt.num and pt.num + 5))");

        // 302 - select from the updated table

        rs = st.executeQuery(
            "select * from k55admin.tmp_products order by 1, 2, 3");

        expColNames = new String [] {"NUM", "PRODUCT", "PRICE"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"10", "Generator", "50.32"},
            {"20", "Sander", "39.32"},
            {"20", null, null},
            {"30", "Relay", "8.30"},
            {"30", "Ruler", "9.62"},
            {"50", "Hammer", "5.75"},
            {"205", "Saw", "20.79"},
            {"505", "Screwdriver", "3.70"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 303 - 'between' in RIGHT JOIN ON condition as a subquery
        //       update table with nullable columns

        assertUpdateCount(st, 6,
            "update k55admin.tmp_products "
            + "set    price = price /1.1"
            + " where  num in (select p.num"
            + "               from k55admin.parts p right join "
            + "k55admin.products pt"
            + "               on   (p.num between pt.num and pt.num + 5))");

        // 304 - select from the updated table

        rs = st.executeQuery(
            "select * from k55admin.tmp_products order by 1, 2, 3");

        expColNames = new String [] {"NUM", "PRODUCT", "PRICE"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"10", "Generator", "45.74"},
            {"20", "Sander", "35.74"},
            {"20", null, null},
            {"30", "Relay", "7.54"},
            {"30", "Ruler", "8.74"},
            {"50", "Hammer", "5.75"},
            {"205", "Saw", "18.90"},
            {"505", "Screwdriver", "3.70"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // populate the table with non-nullable columns

        assertUpdateCount(st, 0,
            "delete from k55admin.tmp_nn_products");

        st.executeUpdate(
            " insert into k55admin.tmp_nn_products "
            + "select * from k55admin.products_notnull");

        // 305 - 'between' in LEFT JOIN ON condition as a subquery
        //       update table with non-nullable columns

        assertUpdateCount(st, 5,
            "update k55admin.tmp_nn_products "
            + "set    price = price *1.1"
            + " where  num in (select p.num"
            + "               from k55admin.parts p left join "
            + "k55admin.products pt"
            + "               on   (p.num between pt.num and pt.num + 5))");

        // 306 - select from the updated table

        rs = st.executeQuery(
            "select * from k55admin.tmp_nn_products order by 1, 2, 3");

        expColNames = new String [] {"NUM", "PRODUCT", "PRICE"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"10", "Generator", "50.32"},
            {"20", "Sander", "39.32"},
            {"30", "Relay", "8.30"},
            {"30", "Ruler", "9.62"},
            {"50", "Hammer", "5.75"},
            {"205", "Saw", "20.79"},
            {"505", "Screwdriver", "3.70"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 307 - 'between' in RIGHT JOIN ON condition as a subquery
        //       update table with nullable columns

        assertUpdateCount(st, 5,
            "update k55admin.tmp_nn_products "
            + " set    price = price /1.1"
            + " where  num in (select p.num"
            + "               from k55admin.parts p right join "
            + "k55admin.products pt"
            + "               on   (p.num between pt.num and pt.num + 5))");

        // 308 - select from the updated table

        rs = st.executeQuery(
            "select * from k55admin.tmp_nn_products order by 1, 2, 3");

        expColNames = new String [] {"NUM", "PRODUCT", "PRICE"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"10", "Generator", "45.74"},
            {"20", "Sander", "35.74"},
            {"30", "Relay", "7.54"},
            {"30", "Ruler", "8.74"},
            {"50", "Hammer", "5.75"},
            {"205", "Saw", "18.90"},
            {"505", "Screwdriver", "3.70"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        //
        //------------------------------------------------------------
        // test unit 4. Update with OUTER JOIN subquery and with OUTER JOIN in
        //              set clause
        //------------------------------------------------------------

        // populate the table

        assertUpdateCount(st, 8,
            "delete from k55admin.tmp_products");

        st.executeUpdate(
            " insert into k55admin.tmp_products "
            + "select * from k55admin.products");

        // 401 - 'between' in LEFT JOIN ON condition as a subquery
        //       update table with nullable columns

        assertUpdateCount(st, 6,
            "update k55admin.tmp_products "
            + " set    price = price * (select min(pt.price)"
            + "                      from   k55admin.parts p left "
            + "join k55admin.products pt"
            + "                      on   (p.num between pt.num "
            + "and pt.num + 5))"
            + " where  num in (select p.num"
            + "               from k55admin.parts p left join "
            + "k55admin.products pt"
            + "               on   (p.num between pt.num and pt.num + 5))");

        // 402 - select from the updated table

        rs = st.executeQuery(
            "select * from k55admin.tmp_products order by 1, 2, 3");

        expColNames = new String [] {"NUM", "PRODUCT", "PRICE"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"10", "Generator", "345.41"},
            {"20", "Sander", "269.91"},
            {"20", null, null},
            {"30", "Relay", "57.00"},
            {"30", "Ruler", "66.06"},
            {"50", "Hammer", "5.75"},
            {"205", "Saw", "142.69"},
            {"505", "Screwdriver", "3.70"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 403 - 'between' in RIGHT JOIN ON condition as a subquery
        //       update table with nullable columns

        assertUpdateCount(st, 6,
            "update k55admin.tmp_products "
            + " set    price = price * (select min(pt.price)"
            + "                        from   k55admin.parts p "
            + "right join k55admin.products pt"
            + "                        on   (p.num between pt.num "
            + "and pt.num + 5))"
            + " where  num in (select p.num"
            + "               from k55admin.parts p right join "
            + "k55admin.products pt"
            + "               on   (p.num between pt.num and pt.num + 5))");

        // 404 - select from the updated table

        rs = st.executeQuery(
            "select * from k55admin.tmp_products order by 1, 2, 3");

        expColNames = new String [] {"NUM", "PRODUCT", "PRICE"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"10", "Generator", "1278.01"},
            {"20", "Sander", "998.66"},
            {"20", null, null},
            {"30", "Relay", "210.90"},
            {"30", "Ruler", "244.42"},
            {"50", "Hammer", "5.75"},
            {"205", "Saw", "527.95"},
            {"505", "Screwdriver", "3.70"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // populate the table

        assertUpdateCount(st, 7,
            "delete from k55admin.tmp_nn_products");

        st.executeUpdate(
            " insert into k55admin.tmp_nn_products "
            + "select * from k55admin.products_notnull");

        // 405 - 'between' in LEFT JOIN ON condition as a subquery
        //       update table with non-nullable columns

        assertUpdateCount(st, 5,
            "update k55admin.tmp_nn_products"
            + " set    price = price * (select min(pt.price)"
            + "                      from   k55admin.parts p left "
            + "join k55admin.products pt"
            + "                      on   (p.num between pt.num "
            + "and pt.num + 5))"
            + " where  num in (select p.num"
            + "               from k55admin.parts p left join "
            + "k55admin.products pt"
            + "               on   (p.num between pt.num and pt.num + 5))");

        // 406 - select from the updated table

        rs = st.executeQuery(
            "select * from k55admin.tmp_nn_products order by 1, 2, 3");

        expColNames = new String [] {"NUM", "PRODUCT", "PRICE"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"10", "Generator", "345.41"},
            {"20", "Sander", "269.91"},
            {"30", "Relay", "57.00"},
            {"30", "Ruler", "66.06"},
            {"50", "Hammer", "5.75"},
            {"205", "Saw", "142.69"},
            {"505", "Screwdriver", "3.70"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // drop the tables

        st.executeUpdate(
            "drop table k55admin.tmp_products");

        st.executeUpdate(
            " drop table k55admin.tmp_nn_products");

        //
        // coj207.clp
        //------------------------------------------------------------
        // --      left outer join (111)
        //------------------------------------------------------------

        //------------------------------------------------------------
        // test unit 1, plain simple join, different relational operators in ON
        //              condition;
        //------------------------------------------------------------

        // 101 - explicit LEFT JOIN keywords, '<>' ON predicate

        rs = st.executeQuery(
            "SELECT PART, A.NUM, PRODUCT, B.NUM"
            + "  FROM K55ADMIN.PARTS A LEFT JOIN K55ADMIN.PRODUCTS B"
            + "    ON A.NUM <> B.NUM"
            + "  order by 1,2");

        expColNames = new String [] {"PART", "NUM", "PRODUCT", "NUM"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Blades", "205", null, "20"},
            {"Blades", "205", "Ruler", "30"},
            {"Blades", "205", "Sander", "20"},
            {"Blades", "205", "Generator", "10"},
            {"Blades", "205", "Hammer", "50"},
            {"Blades", "205", "Relay", "30"},
            {"Blades", "205", "Screwdriver", "505"},
            {"Magnets", "10", null, "20"},
            {"Magnets", "10", "Ruler", "30"},
            {"Magnets", "10", "Sander", "20"},
            {"Magnets", "10", "Saw", "205"},
            {"Magnets", "10", "Hammer", "50"},
            {"Magnets", "10", "Relay", "30"},
            {"Magnets", "10", "Screwdriver", "505"},
            {"Oil", "160", null, "20"},
            {"Oil", "160", "Ruler", "30"},
            {"Oil", "160", "Sander", "20"},
            {"Oil", "160", "Generator", "10"},
            {"Oil", "160", "Saw", "205"},
            {"Oil", "160", "Hammer", "50"},
            {"Oil", "160", "Relay", "30"},
            {"Oil", "160", "Screwdriver", "505"},
            {"Paper", "20", "Ruler", "30"},
            {"Paper", "20", "Generator", "10"},
            {"Paper", "20", "Saw", "205"},
            {"Paper", "20", "Hammer", "50"},
            {"Paper", "20", "Relay", "30"},
            {"Paper", "20", "Screwdriver", "505"},
            {"Plastic", "30", null, "20"},
            {"Plastic", "30", "Sander", "20"},
            {"Plastic", "30", "Generator", "10"},
            {"Plastic", "30", "Saw", "205"},
            {"Plastic", "30", "Hammer", "50"},
            {"Plastic", "30", "Screwdriver", "505"},
            {"Steel", "30", null, "20"},
            {"Steel", "30", "Sander", "20"},
            {"Steel", "30", "Generator", "10"},
            {"Steel", "30", "Saw", "205"},
            {"Steel", "30", "Hammer", "50"},
            {"Steel", "30", "Screwdriver", "505"},
            {"Wire", "10", null, "20"},
            {"Wire", "10", "Ruler", "30"},
            {"Wire", "10", "Sander", "20"},
            {"Wire", "10", "Saw", "205"},
            {"Wire", "10", "Hammer", "50"},
            {"Wire", "10", "Relay", "30"},
            {"Wire", "10", "Screwdriver", "505"},
            {null, "30", null, "20"},
            {null, "30", "Sander", "20"},
            {null, "30", "Generator", "10"},
            {null, "30", "Saw", "205"},
            {null, "30", "Hammer", "50"},
            {null, "30", "Screwdriver", "505"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 102 - explicit OUTER keyword, '>' ON predicate

        rs = st.executeQuery(
            "SELECT PART, A.NUM, PRODUCT, B.NUM"
            + "  FROM K55ADMIN.PARTS A LEFT OUTER JOIN K55ADMIN.PRODUCTS B"
            + "    ON A.NUM > B.NUM"
            + "  order by 1,2");

        expColNames = new String [] {"PART", "NUM", "PRODUCT", "NUM"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Blades", "205", null, "20"},
            {"Blades", "205", "Ruler", "30"},
            {"Blades", "205", "Sander", "20"},
            {"Blades", "205", "Generator", "10"},
            {"Blades", "205", "Hammer", "50"},
            {"Blades", "205", "Relay", "30"},
            {"Magnets", "10", null, null},
            {"Oil", "160", null, "20"},
            {"Oil", "160", "Ruler", "30"},
            {"Oil", "160", "Sander", "20"},
            {"Oil", "160", "Generator", "10"},
            {"Oil", "160", "Hammer", "50"},
            {"Oil", "160", "Relay", "30"},
            {"Paper", "20", "Generator", "10"},
            {"Plastic", "30", null, "20"},
            {"Plastic", "30", "Sander", "20"},
            {"Plastic", "30", "Generator", "10"},
            {"Steel", "30", null, "20"},
            {"Steel", "30", "Sander", "20"},
            {"Steel", "30", "Generator", "10"},
            {"Wire", "10", null, null},
            {null, "30", null, "20"},
            {null, "30", "Sander", "20"},
            {null, "30", "Generator", "10"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 103 - AND operator in on condition, '<=' or '>=' ON
        //       predicate

        rs = st.executeQuery(
            "SELECT PART, A.NUM, PRODUCT, B.NUM"
            + "  FROM K55ADMIN.PARTS A LEFT JOIN K55ADMIN.PRODUCTS B"
            + "    ON A.NUM <= B.NUM and B.NUM >= A.NUM"
            + "  order by 1,2");

        expColNames = new String [] {"PART", "NUM", "PRODUCT", "NUM"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Blades", "205", "Saw", "205"},
            {"Blades", "205", "Screwdriver", "505"},
            {"Magnets", "10", null, "20"},
            {"Magnets", "10", "Ruler", "30"},
            {"Magnets", "10", "Sander", "20"},
            {"Magnets", "10", "Generator", "10"},
            {"Magnets", "10", "Saw", "205"},
            {"Magnets", "10", "Hammer", "50"},
            {"Magnets", "10", "Relay", "30"},
            {"Magnets", "10", "Screwdriver", "505"},
            {"Oil", "160", "Saw", "205"},
            {"Oil", "160", "Screwdriver", "505"},
            {"Paper", "20", null, "20"},
            {"Paper", "20", "Ruler", "30"},
            {"Paper", "20", "Sander", "20"},
            {"Paper", "20", "Saw", "205"},
            {"Paper", "20", "Hammer", "50"},
            {"Paper", "20", "Relay", "30"},
            {"Paper", "20", "Screwdriver", "505"},
            {"Plastic", "30", "Ruler", "30"},
            {"Plastic", "30", "Saw", "205"},
            {"Plastic", "30", "Hammer", "50"},
            {"Plastic", "30", "Relay", "30"},
            {"Plastic", "30", "Screwdriver", "505"},
            {"Steel", "30", "Ruler", "30"},
            {"Steel", "30", "Saw", "205"},
            {"Steel", "30", "Hammer", "50"},
            {"Steel", "30", "Relay", "30"},
            {"Steel", "30", "Screwdriver", "505"},
            {"Wire", "10", null, "20"},
            {"Wire", "10", "Ruler", "30"},
            {"Wire", "10", "Sander", "20"},
            {"Wire", "10", "Generator", "10"},
            {"Wire", "10", "Saw", "205"},
            {"Wire", "10", "Hammer", "50"},
            {"Wire", "10", "Relay", "30"},
            {"Wire", "10", "Screwdriver", "505"},
            {null, "30", "Ruler", "30"},
            {null, "30", "Saw", "205"},
            {null, "30", "Hammer", "50"},
            {null, "30", "Relay", "30"},
            {null, "30", "Screwdriver", "505"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 104.1 - Add arbitrary number of parenthesis, '<>' ON
        //         predicate

        rs = st.executeQuery(
            "SELECT PART, A.NUM, PRODUCT, B.NUM"
            + "  FROM ((((K55ADMIN.PARTS A LEFT JOIN K55ADMIN.PRODUCTS B"
            + "    ON A.NUM <> B.NUM))))"
            + "  order by 1,2");

        expColNames = new String [] {"PART", "NUM", "PRODUCT", "NUM"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Blades", "205", null, "20"},
            {"Blades", "205", "Ruler", "30"},
            {"Blades", "205", "Sander", "20"},
            {"Blades", "205", "Generator", "10"},
            {"Blades", "205", "Hammer", "50"},
            {"Blades", "205", "Relay", "30"},
            {"Blades", "205", "Screwdriver", "505"},
            {"Magnets", "10", null, "20"},
            {"Magnets", "10", "Ruler", "30"},
            {"Magnets", "10", "Sander", "20"},
            {"Magnets", "10", "Saw", "205"},
            {"Magnets", "10", "Hammer", "50"},
            {"Magnets", "10", "Relay", "30"},
            {"Magnets", "10", "Screwdriver", "505"},
            {"Oil", "160", null, "20"},
            {"Oil", "160", "Ruler", "30"},
            {"Oil", "160", "Sander", "20"},
            {"Oil", "160", "Generator", "10"},
            {"Oil", "160", "Saw", "205"},
            {"Oil", "160", "Hammer", "50"},
            {"Oil", "160", "Relay", "30"},
            {"Oil", "160", "Screwdriver", "505"},
            {"Paper", "20", "Ruler", "30"},
            {"Paper", "20", "Generator", "10"},
            {"Paper", "20", "Saw", "205"},
            {"Paper", "20", "Hammer", "50"},
            {"Paper", "20", "Relay", "30"},
            {"Paper", "20", "Screwdriver", "505"},
            {"Plastic", "30", null, "20"},
            {"Plastic", "30", "Sander", "20"},
            {"Plastic", "30", "Generator", "10"},
            {"Plastic", "30", "Saw", "205"},
            {"Plastic", "30", "Hammer", "50"},
            {"Plastic", "30", "Screwdriver", "505"},
            {"Steel", "30", null, "20"},
            {"Steel", "30", "Sander", "20"},
            {"Steel", "30", "Generator", "10"},
            {"Steel", "30", "Saw", "205"},
            {"Steel", "30", "Hammer", "50"},
            {"Steel", "30", "Screwdriver", "505"},
            {"Wire", "10", null, "20"},
            {"Wire", "10", "Ruler", "30"},
            {"Wire", "10", "Sander", "20"},
            {"Wire", "10", "Saw", "205"},
            {"Wire", "10", "Hammer", "50"},
            {"Wire", "10", "Relay", "30"},
            {"Wire", "10", "Screwdriver", "505"},
            {null, "30", null, "20"},
            {null, "30", "Sander", "20"},
            {null, "30", "Generator", "10"},
            {null, "30", "Saw", "205"},
            {null, "30", "Hammer", "50"},
            {null, "30", "Screwdriver", "505"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 104.2 - Add arbitrary number of parenthesis, '<>' ON
        //         predicate

        rs = st.executeQuery(
            "SELECT PART, A.NUM, PRODUCT, B.NUM"
            + "  FROM (K55ADMIN.PARTS A LEFT JOIN K55ADMIN.PRODUCTS B"
            + "    ON A.NUM <> B.NUM)"
            + "  order by 1,2");

        expColNames = new String [] {"PART", "NUM", "PRODUCT", "NUM"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Blades", "205", null, "20"},
            {"Blades", "205", "Ruler", "30"},
            {"Blades", "205", "Sander", "20"},
            {"Blades", "205", "Generator", "10"},
            {"Blades", "205", "Hammer", "50"},
            {"Blades", "205", "Relay", "30"},
            {"Blades", "205", "Screwdriver", "505"},
            {"Magnets", "10", null, "20"},
            {"Magnets", "10", "Ruler", "30"},
            {"Magnets", "10", "Sander", "20"},
            {"Magnets", "10", "Saw", "205"},
            {"Magnets", "10", "Hammer", "50"},
            {"Magnets", "10", "Relay", "30"},
            {"Magnets", "10", "Screwdriver", "505"},
            {"Oil", "160", null, "20"},
            {"Oil", "160", "Ruler", "30"},
            {"Oil", "160", "Sander", "20"},
            {"Oil", "160", "Generator", "10"},
            {"Oil", "160", "Saw", "205"},
            {"Oil", "160", "Hammer", "50"},
            {"Oil", "160", "Relay", "30"},
            {"Oil", "160", "Screwdriver", "505"},
            {"Paper", "20", "Ruler", "30"},
            {"Paper", "20", "Generator", "10"},
            {"Paper", "20", "Saw", "205"},
            {"Paper", "20", "Hammer", "50"},
            {"Paper", "20", "Relay", "30"},
            {"Paper", "20", "Screwdriver", "505"},
            {"Plastic", "30", null, "20"},
            {"Plastic", "30", "Sander", "20"},
            {"Plastic", "30", "Generator", "10"},
            {"Plastic", "30", "Saw", "205"},
            {"Plastic", "30", "Hammer", "50"},
            {"Plastic", "30", "Screwdriver", "505"},
            {"Steel", "30", null, "20"},
            {"Steel", "30", "Sander", "20"},
            {"Steel", "30", "Generator", "10"},
            {"Steel", "30", "Saw", "205"},
            {"Steel", "30", "Hammer", "50"},
            {"Steel", "30", "Screwdriver", "505"},
            {"Wire", "10", null, "20"},
            {"Wire", "10", "Ruler", "30"},
            {"Wire", "10", "Sander", "20"},
            {"Wire", "10", "Saw", "205"},
            {"Wire", "10", "Hammer", "50"},
            {"Wire", "10", "Relay", "30"},
            {"Wire", "10", "Screwdriver", "505"},
            {null, "30", null, "20"},
            {null, "30", "Sander", "20"},
            {null, "30", "Generator", "10"},
            {null, "30", "Saw", "205"},
            {null, "30", "Hammer", "50"},
            {null, "30", "Screwdriver", "505"}
        };

        JDBC.assertFullResultSet(rs, expRS);

    }


    private void part_iv(Statement st) throws SQLException {
        ResultSet rs = null;

        String [][] expRS;
        String [] expColNames;
        String [] wrnRS;

        // 104.3 - Add arbitrary number of parenthesis, '<>' ON
        //         predicate

        rs = st.executeQuery(
            "SELECT PART, A.NUM, PRODUCT, B.NUM"
            + "  FROM ((((((((((K55ADMIN.PARTS A LEFT JOIN "
            + "K55ADMIN.PRODUCTS B"
            + "    ON A.NUM <> B.NUM))))))))))"
            + "  order by 1,2");

        expColNames = new String [] {"PART", "NUM", "PRODUCT", "NUM"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Blades", "205", null, "20"},
            {"Blades", "205", "Ruler", "30"},
            {"Blades", "205", "Sander", "20"},
            {"Blades", "205", "Generator", "10"},
            {"Blades", "205", "Hammer", "50"},
            {"Blades", "205", "Relay", "30"},
            {"Blades", "205", "Screwdriver", "505"},
            {"Magnets", "10", null, "20"},
            {"Magnets", "10", "Ruler", "30"},
            {"Magnets", "10", "Sander", "20"},
            {"Magnets", "10", "Saw", "205"},
            {"Magnets", "10", "Hammer", "50"},
            {"Magnets", "10", "Relay", "30"},
            {"Magnets", "10", "Screwdriver", "505"},
            {"Oil", "160", null, "20"},
            {"Oil", "160", "Ruler", "30"},
            {"Oil", "160", "Sander", "20"},
            {"Oil", "160", "Generator", "10"},
            {"Oil", "160", "Saw", "205"},
            {"Oil", "160", "Hammer", "50"},
            {"Oil", "160", "Relay", "30"},
            {"Oil", "160", "Screwdriver", "505"},
            {"Paper", "20", "Ruler", "30"},
            {"Paper", "20", "Generator", "10"},
            {"Paper", "20", "Saw", "205"},
            {"Paper", "20", "Hammer", "50"},
            {"Paper", "20", "Relay", "30"},
            {"Paper", "20", "Screwdriver", "505"},
            {"Plastic", "30", null, "20"},
            {"Plastic", "30", "Sander", "20"},
            {"Plastic", "30", "Generator", "10"},
            {"Plastic", "30", "Saw", "205"},
            {"Plastic", "30", "Hammer", "50"},
            {"Plastic", "30", "Screwdriver", "505"},
            {"Steel", "30", null, "20"},
            {"Steel", "30", "Sander", "20"},
            {"Steel", "30", "Generator", "10"},
            {"Steel", "30", "Saw", "205"},
            {"Steel", "30", "Hammer", "50"},
            {"Steel", "30", "Screwdriver", "505"},
            {"Wire", "10", null, "20"},
            {"Wire", "10", "Ruler", "30"},
            {"Wire", "10", "Sander", "20"},
            {"Wire", "10", "Saw", "205"},
            {"Wire", "10", "Hammer", "50"},
            {"Wire", "10", "Relay", "30"},
            {"Wire", "10", "Screwdriver", "505"},
            {null, "30", null, "20"},
            {null, "30", "Sander", "20"},
            {null, "30", "Generator", "10"},
            {null, "30", "Saw", "205"},
            {null, "30", "Hammer", "50"},
            {null, "30", "Screwdriver", "505"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        //
        //------------------------------------------------------------
        // test unit 2, add where, aggregate, order by, group by, having
        //------------------------------------------------------------

        // 201 - Use where clause with LEFT join, '>' ON predicate

        rs = st.executeQuery(
            "SELECT PART, A.NUM, PRODUCT, B.NUM, PRICE"
            + "  FROM K55ADMIN.PRODUCTS B LEFT JOIN K55ADMIN.PARTS A"
            + "    ON A.NUM > B.NUM"
            + "  WHERE B.PRICE>15 or A.PART <> 'Wire'"
            + "        and B.NUM > 10"
            + "  order by 1,2");

        expColNames = new String [] {"PART", "NUM", "PRODUCT", "NUM", "PRICE"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Blades", "205", null, "20", null},
            {"Blades", "205", "Ruler", "30", "8.75"},
            {"Blades", "205", "Sander", "20", "35.75"},
            {"Blades", "205", "Generator", "10", "45.75"},
            {"Blades", "205", "Hammer", "50", "5.75"},
            {"Blades", "205", "Relay", "30", "7.55"},
            {"Oil", "160", null, "20", null},
            {"Oil", "160", "Ruler", "30", "8.75"},
            {"Oil", "160", "Sander", "20", "35.75"},
            {"Oil", "160", "Generator", "10", "45.75"},
            {"Oil", "160", "Hammer", "50", "5.75"},
            {"Oil", "160", "Relay", "30", "7.55"},
            {"Paper", "20", "Generator", "10", "45.75"},
            {"Plastic", "30", null, "20", null},
            {"Plastic", "30", "Sander", "20", "35.75"},
            {"Plastic", "30", "Generator", "10", "45.75"},
            {"Steel", "30", null, "20", null},
            {"Steel", "30", "Sander", "20", "35.75"},
            {"Steel", "30", "Generator", "10", "45.75"},
            {null, "30", "Sander", "20", "35.75"},
            {null, "30", "Generator", "10", "45.75"},
            {null, null, "Saw", "205", "18.90"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 202 - Use aggregate function in select list, '>' ON predicate

        rs = st.executeQuery(
            "SELECT AVG(PRICE)"
            + "  FROM K55ADMIN.PRODUCTS B LEFT JOIN K55ADMIN.PARTS A"
            + "    ON A.NUM > B.NUM"
            + "  WHERE B.NUM > 10 and A.PART <> 'Wire'");

        expColNames = new String [] {"1"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"18.7100"}
        };

        wrnRS = new String [] {"01003"};

        JDBC.assertFullResultSet(rs, expRS, wrnRS);

        // 203 - Use where clause and order by clause, '>' ON predicate

        rs = st.executeQuery(
            "SELECT PART, PRODUCT, PRICE"
            + "  FROM K55ADMIN.PRODUCTS B LEFT JOIN K55ADMIN.PARTS A"
            + "    ON A.NUM > B.NUM"
            + "  WHERE B.PRICE>5"
            + "  ORDER BY PART, PRICE DESC");

        expColNames = new String [] {"PART", "PRODUCT", "PRICE"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Blades", "Generator", "45.75"},
            {"Blades", "Sander", "35.75"},
            {"Blades", "Ruler", "8.75"},
            {"Blades", "Relay", "7.55"},
            {"Blades", "Hammer", "5.75"},
            {"Oil", "Generator", "45.75"},
            {"Oil", "Sander", "35.75"},
            {"Oil", "Ruler", "8.75"},
            {"Oil", "Relay", "7.55"},
            {"Oil", "Hammer", "5.75"},
            {"Paper", "Generator", "45.75"},
            {"Plastic", "Generator", "45.75"},
            {"Plastic", "Sander", "35.75"},
            {"Steel", "Generator", "45.75"},
            {"Steel", "Sander", "35.75"},
            {null, "Generator", "45.75"},
            {null, "Sander", "35.75"},
            {null, "Saw", "18.90"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 204 - Use where clause and order by multiple columns, '>' ON
        //        predicate;

        rs = st.executeQuery(
            "SELECT PART, PRODUCT, PRICE"
            + "  FROM K55ADMIN.PRODUCTS B LEFT JOIN K55ADMIN.PARTS A"
            + "    ON A.NUM > B.NUM"
            + "  ORDER BY 1 DESC, 3 ASC");

        expColNames = new String [] {"PART", "PRODUCT", "PRICE"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {null, "Screwdriver", "3.70"},
            {null, "Saw", "18.90"},
            {null, "Sander", "35.75"},
            {null, "Generator", "45.75"},
            {null, null, null},
            {"Steel", "Sander", "35.75"},
            {"Steel", "Generator", "45.75"},
            {"Steel", null, null},
            {"Plastic", "Sander", "35.75"},
            {"Plastic", "Generator", "45.75"},
            {"Plastic", null, null},
            {"Paper", "Generator", "45.75"},
            {"Oil", "Hammer", "5.75"},
            {"Oil", "Relay", "7.55"},
            {"Oil", "Ruler", "8.75"},
            {"Oil", "Sander", "35.75"},
            {"Oil", "Generator", "45.75"},
            {"Oil", null, null},
            {"Blades", "Hammer", "5.75"},
            {"Blades", "Relay", "7.55"},
            {"Blades", "Ruler", "8.75"},
            {"Blades", "Sander", "35.75"},
            {"Blades", "Generator", "45.75"},
            {"Blades", null, null}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 205 - Use where clause and group by clause, '>' ON predicate

        rs = st.executeQuery(
            "SELECT B.NUM, count(*)"
            + "  FROM K55ADMIN.PRODUCTS B LEFT JOIN K55ADMIN.PARTS A"
            + "    ON A.NUM > B.NUM"
            + "  WHERE B.PRICE>5"
            + "  GROUP BY B.NUM"
            + "  order by 1");

        expColNames = new String [] {"NUM", "2"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"10", "6"},
            {"20", "5"},
            {"30", "4"},
            {"50", "2"},
            {"205", "1"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 206 - Use where clause and having clause, '<>' ON predicate

        rs = st.executeQuery(
            "SELECT B.NUM, count(*)"
            + "  FROM K55ADMIN.PRODUCTS B LEFT JOIN K55ADMIN.PARTS A"
            + "    ON A.NUM <> B.NUM"
            + "  GROUP BY B.NUM"
            + "  HAVING B.NUM > 20"
            + "  order by 1");

        expColNames = new String [] {"NUM", "2"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"30", "10"},
            {"50", "8"},
            {"205", "7"},
            {"505", "8"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        //
        //------------------------------------------------------------
        // test unit 3, create view of join result, inequality ON pred, and try
        //              to modify it;
        //------------------------------------------------------------

        // 301 - Create view with LEFT join clause, '<' or '<>' ON predicate

        st.executeUpdate(
            "CREATE VIEW K55ADMIN.VW1 AS"
            + "  SELECT PART, SUPPLIER, A.NUM, PRODUCT"
            + "    FROM K55ADMIN.PARTS A LEFT JOIN K55ADMIN.PRODUCTS B"
            + "         ON A.NUM > B.NUM"
            + "    WHERE PRICE>5");

        st.executeUpdate(
            " CREATE VIEW K55ADMIN.VW2 AS"
            + "  SELECT PART, SUPPLIER, A.NUM, PRODUCT"
            + "    FROM K55ADMIN.PARTS A LEFT JOIN K55ADMIN.PRODUCTS B"
            + "         ON A.NUM <> B.NUM"
            + "    WHERE SUPPLIER <> 'ACWF'");

        // 302 - Select from view with where clause

        rs = st.executeQuery(
            "SELECT count(*) FROM K55ADMIN.VW1 WHERE PRODUCT <> 'Saw'");

        expColNames = new String [] {"1"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"17"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 303 - Inner Join on views

        rs = st.executeQuery(
            "SELECT K55ADMIN.VW1.PART, K55ADMIN.VW1.PRODUCT, "
            + "K55ADMIN.VW2.PART, K55ADMIN.VW2.PRODUCT"
            + "  FROM K55ADMIN.VW1 JOIN K55ADMIN.VW2 ON "
            + "K55ADMIN.VW1.NUM=K55ADMIN.VW2.NUM"
            + "  order by 1,2,3");

        expColNames = new String [] {"PART", "PRODUCT", "PART", "PRODUCT"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Blades", "Generator", "Blades", null},
            {"Blades", "Generator", "Blades", "Ruler"},
            {"Blades", "Generator", "Blades", "Sander"},
            {"Blades", "Generator", "Blades", "Generator"},
            {"Blades", "Generator", "Blades", "Hammer"},
            {"Blades", "Generator", "Blades", "Relay"},
            {"Blades", "Generator", "Blades", "Screwdriver"},
            {"Blades", "Hammer", "Blades", null},
            {"Blades", "Hammer", "Blades", "Ruler"},
            {"Blades", "Hammer", "Blades", "Sander"},
            {"Blades", "Hammer", "Blades", "Generator"},
            {"Blades", "Hammer", "Blades", "Hammer"},
            {"Blades", "Hammer", "Blades", "Relay"},
            {"Blades", "Hammer", "Blades", "Screwdriver"},
            {"Blades", "Relay", "Blades", null},
            {"Blades", "Relay", "Blades", "Ruler"},
            {"Blades", "Relay", "Blades", "Sander"},
            {"Blades", "Relay", "Blades", "Generator"},
            {"Blades", "Relay", "Blades", "Hammer"},
            {"Blades", "Relay", "Blades", "Relay"},
            {"Blades", "Relay", "Blades", "Screwdriver"},
            {"Blades", "Ruler", "Blades", null},
            {"Blades", "Ruler", "Blades", "Ruler"},
            {"Blades", "Ruler", "Blades", "Sander"},
            {"Blades", "Ruler", "Blades", "Generator"},
            {"Blades", "Ruler", "Blades", "Hammer"},
            {"Blades", "Ruler", "Blades", "Relay"},
            {"Blades", "Ruler", "Blades", "Screwdriver"},
            {"Blades", "Sander", "Blades", null},
            {"Blades", "Sander", "Blades", "Ruler"},
            {"Blades", "Sander", "Blades", "Sander"},
            {"Blades", "Sander", "Blades", "Generator"},
            {"Blades", "Sander", "Blades", "Hammer"},
            {"Blades", "Sander", "Blades", "Relay"},
            {"Blades", "Sander", "Blades", "Screwdriver"},
            {"Oil", "Generator", "Oil", null},
            {"Oil", "Generator", "Oil", "Ruler"},
            {"Oil", "Generator", "Oil", "Sander"},
            {"Oil", "Generator", "Oil", "Generator"},
            {"Oil", "Generator", "Oil", "Saw"},
            {"Oil", "Generator", "Oil", "Hammer"},
            {"Oil", "Generator", "Oil", "Relay"},
            {"Oil", "Generator", "Oil", "Screwdriver"},
            {"Oil", "Hammer", "Oil", null},
            {"Oil", "Hammer", "Oil", "Ruler"},
            {"Oil", "Hammer", "Oil", "Sander"},
            {"Oil", "Hammer", "Oil", "Generator"},
            {"Oil", "Hammer", "Oil", "Saw"},
            {"Oil", "Hammer", "Oil", "Hammer"},
            {"Oil", "Hammer", "Oil", "Relay"},
            {"Oil", "Hammer", "Oil", "Screwdriver"},
            {"Oil", "Relay", "Oil", null},
            {"Oil", "Relay", "Oil", "Ruler"},
            {"Oil", "Relay", "Oil", "Sander"},
            {"Oil", "Relay", "Oil", "Generator"},
            {"Oil", "Relay", "Oil", "Saw"},
            {"Oil", "Relay", "Oil", "Hammer"},
            {"Oil", "Relay", "Oil", "Relay"},
            {"Oil", "Relay", "Oil", "Screwdriver"},
            {"Oil", "Ruler", "Oil", null},
            {"Oil", "Ruler", "Oil", "Ruler"},
            {"Oil", "Ruler", "Oil", "Sander"},
            {"Oil", "Ruler", "Oil", "Generator"},
            {"Oil", "Ruler", "Oil", "Saw"},
            {"Oil", "Ruler", "Oil", "Hammer"},
            {"Oil", "Ruler", "Oil", "Relay"},
            {"Oil", "Ruler", "Oil", "Screwdriver"},
            {"Oil", "Sander", "Oil", null},
            {"Oil", "Sander", "Oil", "Ruler"},
            {"Oil", "Sander", "Oil", "Sander"},
            {"Oil", "Sander", "Oil", "Generator"},
            {"Oil", "Sander", "Oil", "Saw"},
            {"Oil", "Sander", "Oil", "Hammer"},
            {"Oil", "Sander", "Oil", "Relay"},
            {"Oil", "Sander", "Oil", "Screwdriver"},
            {"Paper", "Generator", "Paper", "Ruler"},
            {"Paper", "Generator", "Paper", "Generator"},
            {"Paper", "Generator", "Paper", "Saw"},
            {"Paper", "Generator", "Paper", "Hammer"},
            {"Paper", "Generator", "Paper", "Relay"},
            {"Paper", "Generator", "Paper", "Screwdriver"},
            {"Plastic", "Generator", "Plastic", null},
            {"Plastic", "Generator", "Plastic", "Sander"},
            {"Plastic", "Generator", "Plastic", "Generator"},
            {"Plastic", "Generator", "Plastic", "Saw"},
            {"Plastic", "Generator", "Plastic", "Hammer"},
            {"Plastic", "Generator", "Plastic", "Screwdriver"},
            {"Plastic", "Sander", "Plastic", null},
            {"Plastic", "Sander", "Plastic", "Sander"},
            {"Plastic", "Sander", "Plastic", "Generator"},
            {"Plastic", "Sander", "Plastic", "Saw"},
            {"Plastic", "Sander", "Plastic", "Hammer"},
            {"Plastic", "Sander", "Plastic", "Screwdriver"},
            {"Steel", "Generator", "Plastic", null},
            {"Steel", "Generator", "Plastic", "Sander"},
            {"Steel", "Generator", "Plastic", "Generator"},
            {"Steel", "Generator", "Plastic", "Saw"},
            {"Steel", "Generator", "Plastic", "Hammer"},
            {"Steel", "Generator", "Plastic", "Screwdriver"},
            {"Steel", "Sander", "Plastic", null},
            {"Steel", "Sander", "Plastic", "Sander"},
            {"Steel", "Sander", "Plastic", "Generator"},
            {"Steel", "Sander", "Plastic", "Saw"},
            {"Steel", "Sander", "Plastic", "Hammer"},
            {"Steel", "Sander", "Plastic", "Screwdriver"},
            {null, "Generator", "Plastic", null},
            {null, "Generator", "Plastic", "Sander"},
            {null, "Generator", "Plastic", "Generator"},
            {null, "Generator", "Plastic", "Saw"},
            {null, "Generator", "Plastic", "Hammer"},
            {null, "Generator", "Plastic", "Screwdriver"},
            {null, "Sander", "Plastic", null},
            {null, "Sander", "Plastic", "Sander"},
            {null, "Sander", "Plastic", "Generator"},
            {null, "Sander", "Plastic", "Saw"},
            {null, "Sander", "Plastic", "Hammer"},
            {null, "Sander", "Plastic", "Screwdriver"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 305 - Drop views created in this test unit

        st.executeUpdate(
            "DROP VIEW K55ADMIN.VW1");

        st.executeUpdate(
            " DROP VIEW K55ADMIN.VW2");

        //
        //------------------------------------------------------------
        // test unit 4, test various mix of table
        // references in FROM clause, inequality ON predicate;
        //------------------------------------------------------------

        // 401 - use derived table, '>' ON predicate

        rs = st.executeQuery(
            "SELECT PART, SUPPLIER, A.NUM, PRODUCT, PRICE"
            + "  FROM K55ADMIN.PARTS A LEFT JOIN"
            + "    (SELECT * FROM K55ADMIN.PRODUCTS B WHERE "
            + "PRICE>20) AS CHEAP_PRODUCTS"
            + "    ON A.NUM > CHEAP_PRODUCTS.NUM"
            + "  order by 1,3");

        expColNames = new String [] {"PART", "SUPPLIER", "NUM", "PRODUCT", "PRICE"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Blades", "Ace-Steel", "205", "Sander", "35.75"},
            {"Blades", "Ace-Steel", "205", "Generator", "45.75"},
            {"Magnets", "Bateman", "10", null, null},
            {"Oil", "Western-Chem", "160", "Sander", "35.75"},
            {"Oil", "Western-Chem", "160", "Generator", "45.75"},
            {"Paper", "Ace-Steel", "20", "Generator", "45.75"},
            {"Plastic", "Plastik-Corp", "30", "Sander", "35.75"},
            {"Plastic", "Plastik-Corp", "30", "Generator", "45.75"},
            {"Steel", "ACWF", "30", "Sander", "35.75"},
            {"Steel", "ACWF", "30", "Generator", "45.75"},
            {"Wire", "ACWF", "10", null, null},
            {null, null, "30", "Sander", "35.75"},
            {null, null, "30", "Generator", "45.75"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 402 - one join table ref and one non-join-table-ref and
        //       DISTINCT, '>' ON predicate

        rs = st.executeQuery(
            "SELECT DISTINCT A.PART, B.NUM, K55ADMIN.PRODUCTS.NUM"
            + "  FROM K55ADMIN.PARTS as A, "
            + "       K55ADMIN.PARTS as B LEFT JOIN "
            + "K55ADMIN.PRODUCTS ON B.NUM > K55ADMIN.PRODUCTS.NUM"
            + "   order by 1,2,3");

        expColNames = new String [] {"PART", "NUM", "NUM"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Blades", "10", null},
            {"Blades", "20", "10"},
            {"Blades", "30", "10"},
            {"Blades", "30", "20"},
            {"Blades", "160", "10"},
            {"Blades", "160", "20"},
            {"Blades", "160", "30"},
            {"Blades", "160", "50"},
            {"Blades", "205", "10"},
            {"Blades", "205", "20"},
            {"Blades", "205", "30"},
            {"Blades", "205", "50"},
            {"Magnets", "10", null},
            {"Magnets", "20", "10"},
            {"Magnets", "30", "10"},
            {"Magnets", "30", "20"},
            {"Magnets", "160", "10"},
            {"Magnets", "160", "20"},
            {"Magnets", "160", "30"},
            {"Magnets", "160", "50"},
            {"Magnets", "205", "10"},
            {"Magnets", "205", "20"},
            {"Magnets", "205", "30"},
            {"Magnets", "205", "50"},
            {"Oil", "10", null},
            {"Oil", "20", "10"},
            {"Oil", "30", "10"},
            {"Oil", "30", "20"},
            {"Oil", "160", "10"},
            {"Oil", "160", "20"},
            {"Oil", "160", "30"},
            {"Oil", "160", "50"},
            {"Oil", "205", "10"},
            {"Oil", "205", "20"},
            {"Oil", "205", "30"},
            {"Oil", "205", "50"},
            {"Paper", "10", null},
            {"Paper", "20", "10"},
            {"Paper", "30", "10"},
            {"Paper", "30", "20"},
            {"Paper", "160", "10"},
            {"Paper", "160", "20"},
            {"Paper", "160", "30"},
            {"Paper", "160", "50"},
            {"Paper", "205", "10"},
            {"Paper", "205", "20"},
            {"Paper", "205", "30"},
            {"Paper", "205", "50"},
            {"Plastic", "10", null},
            {"Plastic", "20", "10"},
            {"Plastic", "30", "10"},
            {"Plastic", "30", "20"},
            {"Plastic", "160", "10"},
            {"Plastic", "160", "20"},
            {"Plastic", "160", "30"},
            {"Plastic", "160", "50"},
            {"Plastic", "205", "10"},
            {"Plastic", "205", "20"},
            {"Plastic", "205", "30"},
            {"Plastic", "205", "50"},
            {"Steel", "10", null},
            {"Steel", "20", "10"},
            {"Steel", "30", "10"},
            {"Steel", "30", "20"},
            {"Steel", "160", "10"},
            {"Steel", "160", "20"},
            {"Steel", "160", "30"},
            {"Steel", "160", "50"},
            {"Steel", "205", "10"},
            {"Steel", "205", "20"},
            {"Steel", "205", "30"},
            {"Steel", "205", "50"},
            {"Wire", "10", null},
            {"Wire", "20", "10"},
            {"Wire", "30", "10"},
            {"Wire", "30", "20"},
            {"Wire", "160", "10"},
            {"Wire", "160", "20"},
            {"Wire", "160", "30"},
            {"Wire", "160", "50"},
            {"Wire", "205", "10"},
            {"Wire", "205", "20"},
            {"Wire", "205", "30"},
            {"Wire", "205", "50"},
            {null, "10", null},
            {null, "20", "10"},
            {null, "30", "10"},
            {null, "30", "20"},
            {null, "160", "10"},
            {null, "160", "20"},
            {null, "160", "30"},
            {null, "160", "50"},
            {null, "205", "10"},
            {null, "205", "20"},
            {null, "205", "30"},
            {null, "205", "50"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 403 - mixed implicit and explicit inner joins, '>' ON
        //       predicate

        rs = st.executeQuery(
            "SELECT DISTINCT P1.PART, K55ADMIN.PARTS.NUM, "
            + "K55ADMIN.PRODUCTS.PRICE, D1.PRODUCT, P1.NUM, D1.NUM"
            + " FROM K55ADMIN.PARTS P1 LEFT JOIN K55ADMIN.PRODUCTS "
            + "D1 ON P1.NUM>D1.NUM,"
            + "     K55ADMIN.PARTS, K55ADMIN.PRODUCTS WHERE "
            + "K55ADMIN.PARTS.NUM=K55ADMIN.PRODUCTS.NUM"
            + " order by 1,2,3,4");

        expColNames = new String [] {"PART", "NUM", "PRICE", "PRODUCT", "NUM", "NUM"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Blades", "10", "45.75", "Generator", "205", "10"},
            {"Blades", "10", "45.75", "Hammer", "205", "50"},
            {"Blades", "10", "45.75", "Relay", "205", "30"},
            {"Blades", "10", "45.75", "Ruler", "205", "30"},
            {"Blades", "10", "45.75", "Sander", "205", "20"},
            {"Blades", "10", "45.75", null, "205", "20"},
            {"Blades", "20", "35.75", "Generator", "205", "10"},
            {"Blades", "20", "35.75", "Hammer", "205", "50"},
            {"Blades", "20", "35.75", "Relay", "205", "30"},
            {"Blades", "20", "35.75", "Ruler", "205", "30"},
            {"Blades", "20", "35.75", "Sander", "205", "20"},
            {"Blades", "20", "35.75", null, "205", "20"},
            {"Blades", "20", null, "Generator", "205", "10"},
            {"Blades", "20", null, "Hammer", "205", "50"},
            {"Blades", "20", null, "Relay", "205", "30"},
            {"Blades", "20", null, "Ruler", "205", "30"},
            {"Blades", "20", null, "Sander", "205", "20"},
            {"Blades", "20", null, null, "205", "20"},
            {"Blades", "30", "7.55", "Generator", "205", "10"},
            {"Blades", "30", "7.55", "Hammer", "205", "50"},
            {"Blades", "30", "7.55", "Relay", "205", "30"},
            {"Blades", "30", "7.55", "Ruler", "205", "30"},
            {"Blades", "30", "7.55", "Sander", "205", "20"},
            {"Blades", "30", "7.55", null, "205", "20"},
            {"Blades", "30", "8.75", "Generator", "205", "10"},
            {"Blades", "30", "8.75", "Hammer", "205", "50"},
            {"Blades", "30", "8.75", "Relay", "205", "30"},
            {"Blades", "30", "8.75", "Ruler", "205", "30"},
            {"Blades", "30", "8.75", "Sander", "205", "20"},
            {"Blades", "30", "8.75", null, "205", "20"},
            {"Blades", "205", "18.90", "Generator", "205", "10"},
            {"Blades", "205", "18.90", "Hammer", "205", "50"},
            {"Blades", "205", "18.90", "Relay", "205", "30"},
            {"Blades", "205", "18.90", "Ruler", "205", "30"},
            {"Blades", "205", "18.90", "Sander", "205", "20"},
            {"Blades", "205", "18.90", null, "205", "20"},
            {"Magnets", "10", "45.75", null, "10", null},
            {"Magnets", "20", "35.75", null, "10", null},
            {"Magnets", "20", null, null, "10", null},
            {"Magnets", "30", "7.55", null, "10", null},
            {"Magnets", "30", "8.75", null, "10", null},
            {"Magnets", "205", "18.90", null, "10", null},
            {"Oil", "10", "45.75", "Generator", "160", "10"},
            {"Oil", "10", "45.75", "Hammer", "160", "50"},
            {"Oil", "10", "45.75", "Relay", "160", "30"},
            {"Oil", "10", "45.75", "Ruler", "160", "30"},
            {"Oil", "10", "45.75", "Sander", "160", "20"},
            {"Oil", "10", "45.75", null, "160", "20"},
            {"Oil", "20", "35.75", "Generator", "160", "10"},
            {"Oil", "20", "35.75", "Hammer", "160", "50"},
            {"Oil", "20", "35.75", "Relay", "160", "30"},
            {"Oil", "20", "35.75", "Ruler", "160", "30"},
            {"Oil", "20", "35.75", "Sander", "160", "20"},
            {"Oil", "20", "35.75", null, "160", "20"},
            {"Oil", "20", null, "Generator", "160", "10"},
            {"Oil", "20", null, "Hammer", "160", "50"},
            {"Oil", "20", null, "Relay", "160", "30"},
            {"Oil", "20", null, "Ruler", "160", "30"},
            {"Oil", "20", null, "Sander", "160", "20"},
            {"Oil", "20", null, null, "160", "20"},
            {"Oil", "30", "7.55", "Generator", "160", "10"},
            {"Oil", "30", "7.55", "Hammer", "160", "50"},
            {"Oil", "30", "7.55", "Relay", "160", "30"},
            {"Oil", "30", "7.55", "Ruler", "160", "30"},
            {"Oil", "30", "7.55", "Sander", "160", "20"},
            {"Oil", "30", "7.55", null, "160", "20"},
            {"Oil", "30", "8.75", "Generator", "160", "10"},
            {"Oil", "30", "8.75", "Hammer", "160", "50"},
            {"Oil", "30", "8.75", "Relay", "160", "30"},
            {"Oil", "30", "8.75", "Ruler", "160", "30"},
            {"Oil", "30", "8.75", "Sander", "160", "20"},
            {"Oil", "30", "8.75", null, "160", "20"},
            {"Oil", "205", "18.90", "Generator", "160", "10"},
            {"Oil", "205", "18.90", "Hammer", "160", "50"},
            {"Oil", "205", "18.90", "Relay", "160", "30"},
            {"Oil", "205", "18.90", "Ruler", "160", "30"},
            {"Oil", "205", "18.90", "Sander", "160", "20"},
            {"Oil", "205", "18.90", null, "160", "20"},
            {"Paper", "10", "45.75", "Generator", "20", "10"},
            {"Paper", "20", "35.75", "Generator", "20", "10"},
            {"Paper", "20", null, "Generator", "20", "10"},
            {"Paper", "30", "7.55", "Generator", "20", "10"},
            {"Paper", "30", "8.75", "Generator", "20", "10"},
            {"Paper", "205", "18.90", "Generator", "20", "10"},
            {"Plastic", "10", "45.75", "Generator", "30", "10"},
            {"Plastic", "10", "45.75", "Sander", "30", "20"},
            {"Plastic", "10", "45.75", null, "30", "20"},
            {"Plastic", "20", "35.75", "Generator", "30", "10"},
            {"Plastic", "20", "35.75", "Sander", "30", "20"},
            {"Plastic", "20", "35.75", null, "30", "20"},
            {"Plastic", "20", null, "Generator", "30", "10"},
            {"Plastic", "20", null, "Sander", "30", "20"},
            {"Plastic", "20", null, null, "30", "20"},
            {"Plastic", "30", "7.55", "Generator", "30", "10"},
            {"Plastic", "30", "7.55", "Sander", "30", "20"},
            {"Plastic", "30", "7.55", null, "30", "20"},
            {"Plastic", "30", "8.75", "Generator", "30", "10"},
            {"Plastic", "30", "8.75", "Sander", "30", "20"},
            {"Plastic", "30", "8.75", null, "30", "20"},
            {"Plastic", "205", "18.90", "Generator", "30", "10"},
            {"Plastic", "205", "18.90", "Sander", "30", "20"},
            {"Plastic", "205", "18.90", null, "30", "20"},
            {"Steel", "10", "45.75", "Generator", "30", "10"},
            {"Steel", "10", "45.75", "Sander", "30", "20"},
            {"Steel", "10", "45.75", null, "30", "20"},
            {"Steel", "20", "35.75", "Generator", "30", "10"},
            {"Steel", "20", "35.75", "Sander", "30", "20"},
            {"Steel", "20", "35.75", null, "30", "20"},
            {"Steel", "20", null, "Generator", "30", "10"},
            {"Steel", "20", null, "Sander", "30", "20"},
            {"Steel", "20", null, null, "30", "20"},
            {"Steel", "30", "7.55", "Generator", "30", "10"},
            {"Steel", "30", "7.55", "Sander", "30", "20"},
            {"Steel", "30", "7.55", null, "30", "20"},
            {"Steel", "30", "8.75", "Generator", "30", "10"},
            {"Steel", "30", "8.75", "Sander", "30", "20"},
            {"Steel", "30", "8.75", null, "30", "20"},
            {"Steel", "205", "18.90", "Generator", "30", "10"},
            {"Steel", "205", "18.90", "Sander", "30", "20"},
            {"Steel", "205", "18.90", null, "30", "20"},
            {"Wire", "10", "45.75", null, "10", null},
            {"Wire", "20", "35.75", null, "10", null},
            {"Wire", "20", null, null, "10", null},
            {"Wire", "30", "7.55", null, "10", null},
            {"Wire", "30", "8.75", null, "10", null},
            {"Wire", "205", "18.90", null, "10", null},
            {null, "10", "45.75", "Generator", "30", "10"},
            {null, "10", "45.75", "Sander", "30", "20"},
            {null, "10", "45.75", null, "30", "20"},
            {null, "20", "35.75", "Generator", "30", "10"},
            {null, "20", "35.75", "Sander", "30", "20"},
            {null, "20", "35.75", null, "30", "20"},
            {null, "20", null, "Generator", "30", "10"},
            {null, "20", null, "Sander", "30", "20"},
            {null, "20", null, null, "30", "20"},
            {null, "30", "7.55", "Generator", "30", "10"},
            {null, "30", "7.55", "Sander", "30", "20"},
            {null, "30", "7.55", null, "30", "20"},
            {null, "30", "8.75", "Generator", "30", "10"},
            {null, "30", "8.75", "Sander", "30", "20"},
            {null, "30", "8.75", null, "30", "20"},
            {null, "205", "18.90", "Generator", "30", "10"},
            {null, "205", "18.90", "Sander", "30", "20"},
            {null, "205", "18.90", null, "30", "20"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        //
        //------------------------------------------------------------
        // test unit 5, use of join result in various
        // places, inequality ON predicate;
        //------------------------------------------------------------

        // 501 - join result used in subquery, '>' ON
        //       predicate

        rs = st.executeQuery(
            "SELECT  PART, PRICE"
            + " FROM K55ADMIN.PRODUCTS, K55ADMIN.PARTS"
            + " WHERE PRICE > (SELECT MIN(PRICE) FROM"
            + "                 K55ADMIN.PARTS A LEFT JOIN "
            + "K55ADMIN.PRODUCTS B ON A.NUM>B.NUM)"
            + "order by 1,2");

        expColNames = new String [] {"PART", "PRICE"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Blades", "7.55"},
            {"Blades", "8.75"},
            {"Blades", "18.90"},
            {"Blades", "35.75"},
            {"Blades", "45.75"},
            {"Magnets", "7.55"},
            {"Magnets", "8.75"},
            {"Magnets", "18.90"},
            {"Magnets", "35.75"},
            {"Magnets", "45.75"},
            {"Oil", "7.55"},
            {"Oil", "8.75"},
            {"Oil", "18.90"},
            {"Oil", "35.75"},
            {"Oil", "45.75"},
            {"Paper", "7.55"},
            {"Paper", "8.75"},
            {"Paper", "18.90"},
            {"Paper", "35.75"},
            {"Paper", "45.75"},
            {"Plastic", "7.55"},
            {"Plastic", "8.75"},
            {"Plastic", "18.90"},
            {"Plastic", "35.75"},
            {"Plastic", "45.75"},
            {"Steel", "7.55"},
            {"Steel", "8.75"},
            {"Steel", "18.90"},
            {"Steel", "35.75"},
            {"Steel", "45.75"},
            {"Wire", "7.55"},
            {"Wire", "8.75"},
            {"Wire", "18.90"},
            {"Wire", "35.75"},
            {"Wire", "45.75"},
            {null, "7.55"},
            {null, "8.75"},
            {null, "18.90"},
            {null, "35.75"},
            {null, "45.75"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 502 - join result used in subquery, '<' ON predicate

        rs = st.executeQuery(
            "SELECT  PART, PRICE"
            + " FROM K55ADMIN.PRODUCTS, K55ADMIN.PARTS"
            + " WHERE PRICE NOT IN (SELECT PRICE+1 FROM"
            + "                      K55ADMIN.PARTS A LEFT JOIN "
            + "K55ADMIN.PRODUCTS B ON A.NUM<B.NUM)"
            + "order by 1,2");

        expColNames = new String [] {"PART", "PRICE"};
        JDBC.assertColumnNames(rs, expColNames);
        JDBC.assertDrainResults(rs, 0);

        // 503 - join appear in having clause, '>' ON predicate

        rs = st.executeQuery(
            "SELECT B.NUM, count(*)"
            + "  FROM K55ADMIN.PRODUCTS B LEFT JOIN K55ADMIN.PARTS A"
            + "    ON A.NUM = B.NUM"
            + "  WHERE B.PRICE>5"
            + "  GROUP BY B.NUM"
            + "  HAVING B.NUM > (SELECT AVG(K55ADMIN.PRODUCTS_T.NUM1)"
            + "                  FROM   K55ADMIN.PARTS_T LEFT JOIN "
            + "K55ADMIN.PRODUCTS_T"
            + "                  ON     "
            + "K55ADMIN.PARTS_T.NUM1>K55ADMIN.PRODUCTS_T.NUM2)"
            + " order by 1");

        expColNames = new String [] {"NUM", "2"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"30", "6"},
            {"50", "1"},
            {"205", "1"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 504 - join appear in insert/update/delete

        st.executeUpdate(
            "insert into k55admin.products"
            + " select p1.num, product, price "
            + " from k55admin.parts p1 LEFT JOIN K55ADMIN.products_t p2 "
            + " ON p1.num>p2.num2");

        assertUpdateCount(st, 10,
            " update k55admin.products"
            + " set price=0 "
            + " where price> (select avg(price) "
            + "              from k55admin.products_t left join "
            + "k55admin.parts a"
            + "              on K55ADMIN.products_t.num1>a.num)");

        assertUpdateCount(st, 3,
            " delete from k55admin.products"
            + " where price> any (select price"
            + "                  from k55admin.products_t left "
            + "join k55admin.parts a"
            + "                  on K55ADMIN.products_t.num1>a.num)");

        rs = st.executeQuery(
            " select * from k55admin.products b order by 1,2");

        expColNames = new String [] {"NUM", "PRODUCT", "PRICE"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"10", "Generator", "0.00"},
            {"10", null, null},
            {"10", null, null},
            {"20", "Sander", "0.00"},
            {"20", null, null},
            {"20", null, null},
            {"30", "Generator", "0.00"},
            {"30", "Generator", "0.00"},
            {"30", "Generator", "0.00"},
            {"30", "Relay", "7.55"},
            {"50", "Hammer", "5.75"},
            {"160", "Generator", "0.00"},
            {"160", "Relay", "7.55"},
            {"160", "Sander", "0.00"},
            {"205", "Generator", "0.00"},
            {"205", "Relay", "7.55"},
            {"205", "Sander", "0.00"},
            {"205", "Saw", "0.00"},
            {"505", "Screwdriver", "3.70"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        //
        //------------------------------------------------------------
        // test unit 6, test join of non-null, nulls,
        // and  empty tables;
        //------------------------------------------------------------

        // 601 - test not nulls, '>' ON predicate

        rs = st.executeQuery(
            "SELECT PART, K55ADMIN.PARTS_NOTNULL.NUM, "
            + "K55ADMIN.PRODUCTS_NOTNULL.NUM"
            + "  FROM K55ADMIN.PARTS_NOTNULL LEFT JOIN "
            + "K55ADMIN.PRODUCTS_NOTNULL"
            + "    ON K55ADMIN.PARTS_NOTNULL.NUM > "
            + "K55ADMIN.PRODUCTS_NOTNULL.NUM"
            + " order by 1,2");

        expColNames = new String [] {"PART", "NUM", "NUM"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Blades", "205", "30"},
            {"Blades", "205", "20"},
            {"Blades", "205", "10"},
            {"Blades", "205", "50"},
            {"Blades", "205", "30"},
            {"Magnets", "10", null},
            {"Oil", "160", "30"},
            {"Oil", "160", "20"},
            {"Oil", "160", "10"},
            {"Oil", "160", "50"},
            {"Oil", "160", "30"},
            {"Paper", "20", "10"},
            {"Plastic", "30", "20"},
            {"Plastic", "30", "10"},
            {"Steel", "30", "20"},
            {"Steel", "30", "10"},
            {"Wire", "10", null}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 602.1 - test nulls, both sides all nulls, '<' ON predicate

        rs = st.executeQuery(
            "SELECT PART, K55ADMIN.PARTS_ALLNULL.NUM, "
            + "K55ADMIN.PRODUCTS_ALLNULL.NUM"
            + "  FROM K55ADMIN.PARTS_ALLNULL LEFT JOIN "
            + "K55ADMIN.PRODUCTS_ALLNULL"
            + "    ON K55ADMIN.PARTS_ALLNULL.NUM > "
            + "K55ADMIN.PRODUCTS_ALLNULL.NUM"
            + " order by 1,2");

        expColNames = new String [] {"PART", "NUM", "NUM"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {null, null, null},
            {null, null, null},
            {null, null, null}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 602.2 - test allnull table, left side null, '<>' ON
        //         predicate

        rs = st.executeQuery(
            "SELECT PART, K55ADMIN.PARTS_ALLNULL.NUM, B.NUM"
            + "  FROM K55ADMIN.PARTS_ALLNULL LEFT JOIN K55ADMIN.PRODUCTS B"
            + "    ON K55ADMIN.PARTS_ALLNULL.NUM <> B.NUM"
            + " order by 1,2");

        expColNames = new String [] {"PART", "NUM", "NUM"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {null, null, null},
            {null, null, null},
            {null, null, null}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 602.3 - test allnull table, right side null, '>=' ON
        //         predicate

        rs = st.executeQuery(
            "SELECT PART, A.NUM, K55ADMIN.PRODUCTS_ALLNULL.NUM"
            + "  FROM K55ADMIN.PARTS A LEFT JOIN K55ADMIN.PRODUCTS_ALLNULL"
            + "    ON A.NUM >= K55ADMIN.PRODUCTS_ALLNULL.NUM"
            + " order by 1,2");

        expColNames = new String [] {"PART", "NUM", "NUM"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Blades", "205", null},
            {"Magnets", "10", null},
            {"Oil", "160", null},
            {"Paper", "20", null},
            {"Plastic", "30", null},
            {"Steel", "30", null},
            {"Wire", "10", null},
            {null, "30", null}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 603.1 - test empty table, both sides empty, '<=' ON
        //         predicate

        rs = st.executeQuery(
            "SELECT PART, K55ADMIN.PARTS_EMPTY.NUM, "
            + "K55ADMIN.PRODUCTS_EMPTY.NUM"
            + "  FROM K55ADMIN.PARTS_EMPTY LEFT JOIN "
            + "K55ADMIN.PRODUCTS_EMPTY"
            + "    ON K55ADMIN.PARTS_EMPTY.NUM <= "
            + "K55ADMIN.PRODUCTS_EMPTY.NUM"
            + "  order by 1,2");

        expColNames = new String [] {"PART", "NUM", "NUM"};
        JDBC.assertColumnNames(rs, expColNames);
        JDBC.assertDrainResults(rs, 0);

        // 603.2 - test empty table, left side empty, '<>' ON
        //         predicate

        rs = st.executeQuery(
            "SELECT PART, K55ADMIN.PARTS_EMPTY.NUM, B.NUM"
            + "  FROM K55ADMIN.PARTS_EMPTY LEFT JOIN K55ADMIN.PRODUCTS B"
            + "    ON K55ADMIN.PARTS_EMPTY.NUM <> B.NUM"
            + " order by 1,2");

        expColNames = new String [] {"PART", "NUM", "NUM"};
        JDBC.assertColumnNames(rs, expColNames);
        JDBC.assertDrainResults(rs, 0);

        // 603.3 - test empty table, right side empty, '>' ON
        //         predicate

        rs = st.executeQuery(
            "SELECT PART, A.NUM, K55ADMIN.PRODUCTS_EMPTY.NUM"
            + "  FROM K55ADMIN.PARTS A LEFT JOIN K55ADMIN.PRODUCTS_EMPTY"
            + "    ON A.NUM > K55ADMIN.PRODUCTS_EMPTY.NUM"
            + " order by 1,2");

        expColNames = new String [] {"PART", "NUM", "NUM"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Blades", "205", null},
            {"Magnets", "10", null},
            {"Oil", "160", null},
            {"Paper", "20", null},
            {"Plastic", "30", null},
            {"Steel", "30", null},
            {"Wire", "10", null},
            {null, "30", null}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // simulate left join
        //SELECT PART, SUPPLIER, A.NUM, B.NUM, PRODUCT
        //  FROM PARTS, PRODUCTS
        //  WHERE A.NUM = B.NUM
        //UNION ALL
        //SELECT PART, SUPPLIER, NUM,
        //       nullif(1,1),         // null
        //       nullif('1','1')      // null
        //  FROM PARTS
        //  WHERE NOT EXISTS(SELECT * FROM PRODUCTS
        //                     WHERE A.NUM = B.NUM)

        // 604 - null padding for all data types, '>' ON predicate

        rs = st.executeQuery(
            "SELECT K55ADMIN.MANYTYPES.intcol, "
            + "K55ADMIN.MANYTYPES_NOTNULL.intcol,"
            + "       K55ADMIN.MANYTYPES.DEC62COL, "
            + "K55ADMIN.MANYTYPES_NOTNULL.DEC72COL"
            + "  FROM K55ADMIN.MANYTYPES LEFT JOIN "
            + "K55ADMIN.MANYTYPES_NOTNULL"
            + "    ON K55ADMIN.MANYTYPES.DEC62COL > "
            + "K55ADMIN.MANYTYPES_NOTNULL.DEC72COL"
            + " ORDER BY 1,2");

        expColNames = new String [] {"INTCOL", "INTCOL", "DEC62COL", "DEC72COL"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1", null, "1.00", null},
            {"2", null, "2.00", null},
            {"3", null, "3.00", null},
            {"4", null, "4.00", null},
            {"5", null, "5.00", null},
            {"6", null, "6.00", null},
            {"7", null, "7.00", null},
            {"8", null, "8.00", null},
            {"9", null, "9.00", null},
            {"10", null, "10.00", null},
            {"11", null, "11.00", null},
            {"12", "11", "12.00", "11.00"},
            {"13", "11", "13.00", "11.00"},
            {"13", "12", "13.00", "12.00"},
            {"14", "11", "14.00", "11.00"},
            {"14", "12", "14.00", "12.00"},
            {"14", "13", "14.00", "13.00"},
            {"15", "11", "15.00", "11.00"},
            {"15", "12", "15.00", "12.00"},
            {"15", "13", "15.00", "13.00"},
            {"15", "14", "15.00", "14.00"},
            {"16", "11", "16.00", "11.00"},
            {"16", "12", "16.00", "12.00"},
            {"16", "13", "16.00", "13.00"},
            {"16", "14", "16.00", "14.00"},
            {"16", "15", "16.00", "15.00"},
            {"17", "11", "17.00", "11.00"},
            {"17", "12", "17.00", "12.00"},
            {"17", "13", "17.00", "13.00"},
            {"17", "14", "17.00", "14.00"},
            {"17", "15", "17.00", "15.00"},
            {"17", "16", "17.00", "16.00"},
            {"18", "11", "18.00", "11.00"},
            {"18", "12", "18.00", "12.00"},
            {"18", "13", "18.00", "13.00"},
            {"18", "14", "18.00", "14.00"},
            {"18", "15", "18.00", "15.00"},
            {"18", "16", "18.00", "16.00"},
            {"18", "17", "18.00", "17.00"},
            {"19", "11", "19.00", "11.00"},
            {"19", "12", "19.00", "12.00"},
            {"19", "13", "19.00", "13.00"},
            {"19", "14", "19.00", "14.00"},
            {"19", "15", "19.00", "15.00"},
            {"19", "16", "19.00", "16.00"},
            {"19", "17", "19.00", "17.00"},
            {"19", "18", "19.00", "18.00"},
            {"20", "11", "20.00", "11.00"},
            {"20", "12", "20.00", "12.00"},
            {"20", "13", "20.00", "13.00"},
            {"20", "14", "20.00", "14.00"},
            {"20", "15", "20.00", "15.00"},
            {"20", "16", "20.00", "16.00"},
            {"20", "17", "20.00", "17.00"},
            {"20", "18", "20.00", "18.00"},
            {"20", "19", "20.00", "19.00"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 605 - null padding for all data types, '<' ON predicate

        rs = st.executeQuery(
            "SELECT K55ADMIN.MANYTYPES.intcol, "
            + "K55ADMIN.MANYTYPES_NOTNULL.intcol,"
            + "       K55ADMIN.MANYTYPES.DEC62COL+15, "
            + "K55ADMIN.MANYTYPES_NOTNULL.DEC72COL"
            + "  FROM K55ADMIN.MANYTYPES LEFT JOIN "
            + "K55ADMIN.MANYTYPES_NOTNULL"
            + "    ON K55ADMIN.MANYTYPES.DEC62COL+15 < "
            + "K55ADMIN.MANYTYPES_NOTNULL.DEC72COL"
            + " WHERE K55ADMIN.MANYTYPES.INTCOL BETWEEN 5 AND 15"
            + " ORDER BY 1,2");

        expColNames = new String [] {"INTCOL", "INTCOL", "3", "DEC72COL"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"5", "21", "20.00", "21.00"},
            {"5", "22", "20.00", "22.00"},
            {"5", "23", "20.00", "23.00"},
            {"5", "24", "20.00", "24.00"},
            {"5", "25", "20.00", "25.00"},
            {"6", "22", "21.00", "22.00"},
            {"6", "23", "21.00", "23.00"},
            {"6", "24", "21.00", "24.00"},
            {"6", "25", "21.00", "25.00"},
            {"7", "23", "22.00", "23.00"},
            {"7", "24", "22.00", "24.00"},
            {"7", "25", "22.00", "25.00"},
            {"8", "24", "23.00", "24.00"},
            {"8", "25", "23.00", "25.00"},
            {"9", "25", "24.00", "25.00"},
            {"10", null, "25.00", null},
            {"11", null, "26.00", null},
            {"12", null, "27.00", null},
            {"13", null, "28.00", null},
            {"14", null, "29.00", null},
            {"15", null, "30.00", null}
        };

        JDBC.assertFullResultSet(rs, expRS);

        //
        // coj209.clp
        // ***********************************************
        // ** complex join tests
        // **********************************************

        //------------------------------------------------------------
        // test unit 1. plain joins; multiple joins (219);
        //------------------------------------------------------------

        // 101 - Nest INNER join and RIGHT join, with "or 1=0" predicate

        rs = st.executeQuery(
            "SELECT emp_name, old_office, new_office"
            + "  FROM K55ADMIN.employees INNER JOIN"
            + "       (k55admin.old_offices RIGHT JOIN K55ADMIN.new_offices"
            + "        ON k55admin.old_offices.emp_id = "
            + "k55admin.new_offices.emp_id or 1=0)"
            + "       ON k55admin.employees.emp_id = "
            + "k55admin.new_offices.emp_id or 1=0"
            + "  ORDER BY 3");

        expColNames = new String [] {"EMP_NAME", "OLD_OFFICE", "NEW_OFFICE"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {null, "X128", "Y124"},
            {"B. Ward", "X127", "Y125"},
            {"J. Thomas", "X125", "Y126"},
            {"C. Manthey", "X124", "Y128"},
            {"N. Baxter", null, "Y129"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 102 - Nest INNER join and LEFT join, with "or 1=0"
        //       predicate

        rs = st.executeQuery(
            "SELECT emp_name, old_office, new_office"
            + "  FROM K55ADMIN.employees INNER JOIN"
            + "       (k55admin.old_offices LEFT JOIN K55ADMIN.new_offices"
            + "        ON k55admin.old_offices.emp_id = "
            + "k55admin.new_offices.emp_id or 1=0)"
            + "       ON k55admin.employees.emp_id = "
            + "k55admin.new_offices.emp_id or 1=0"
            + "  ORDER BY 3");

        expColNames = new String [] {"EMP_NAME", "OLD_OFFICE", "NEW_OFFICE"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {null, "X128", "Y124"},
            {"B. Ward", "X127", "Y125"},
            {"J. Thomas", "X125", "Y126"},
            {"C. Manthey", "X124", "Y128"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 103 - Nest LEFT join and RIGHT join, with "or 1=0"
        //       predicate

        rs = st.executeQuery(
            "SELECT emp_name, old_office, new_office"
            + "  FROM K55ADMIN.employees LEFT JOIN"
            + "       (k55admin.old_offices RIGHT JOIN K55ADMIN.new_offices"
            + "        ON k55admin.old_offices.emp_id = "
            + "k55admin.new_offices.emp_id or 1=0)"
            + "       ON k55admin.employees.emp_id = "
            + "k55admin.new_offices.emp_id or 1=0"
            + "  ORDER BY 3, 1");

        expColNames = new String [] {"EMP_NAME", "OLD_OFFICE", "NEW_OFFICE"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {null, "X128", "Y124"},
            {"B. Ward", "X127", "Y125"},
            {"J. Thomas", "X125", "Y126"},
            {"C. Manthey", "X124", "Y128"},
            {"N. Baxter", null, "Y129"},
            {"K. Woods", null, null}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 104 - Nest RIGHT join and LEFT join, with "or 1=0"
        //       predicate

        rs = st.executeQuery(
            "SELECT emp_name, old_office, new_office"
            + "  FROM K55ADMIN.employees RIGHT JOIN"
            + "       (k55admin.old_offices LEFT JOIN K55ADMIN.new_offices"
            + "        ON k55admin.old_offices.emp_id = "
            + "k55admin.new_offices.emp_id or 1=0)"
            + "       ON k55admin.employees.emp_id = "
            + "k55admin.new_offices.emp_id or 1=0"
            + "  ORDER BY 2");

        expColNames = new String [] {"EMP_NAME", "OLD_OFFICE", "NEW_OFFICE"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"C. Manthey", "X124", "Y128"},
            {"J. Thomas", "X125", "Y126"},
            {null, "X126", null},
            {"B. Ward", "X127", "Y125"},
            {null, "X128", "Y124"},
            {null, "X129", null},
            {null, "X130", null}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 105 - Nest COMMA join and LEFT join, with "or 1=0"
        //       predicate

        rs = st.executeQuery(
            "SELECT emp_name, old_office, new_office"
            + "  FROM K55ADMIN.employees,"
            + "       (k55admin.old_offices LEFT JOIN K55ADMIN.new_offices"
            + "           ON k55admin.old_offices.emp_id = "
            + "k55admin.new_offices.emp_id or 1=0)"
            + "  WHERE k55admin.employees.emp_id = "
            + "k55admin.new_offices.emp_id or 1=0"
            + "  ORDER BY 3");

        expColNames = new String [] {"EMP_NAME", "OLD_OFFICE", "NEW_OFFICE"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {null, "X128", "Y124"},
            {"B. Ward", "X127", "Y125"},
            {"J. Thomas", "X125", "Y126"},
            {"C. Manthey", "X124", "Y128"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 106 - COMMA join 2 LEFT joins, with "or 1=0" predicate

        rs = st.executeQuery(
            "SELECT T1.emp_id, T2.emp_id, T3.emp_id, T4.emp_id"
            + "  FROM (K55ADMIN.old_offices T1 LEFT JOIN "
            + "K55ADMIN.new_offices T2"
            + "           ON T1.emp_id = T2.emp_id or 1=0),"
            + "       (k55admin.old_offices T3 LEFT JOIN "
            + "K55ADMIN.new_offices T4"
            + "           ON T3.emp_id = T4.emp_id or 1=0)"
            + "  WHERE T1.emp_id = T3.emp_id"
            + "  ORDER BY 1");

        expColNames = new String [] {"EMP_ID", "EMP_ID", "EMP_ID", "EMP_ID"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"368521", "368521", "368521", "368521"},
            {"480923", "480923", "480923", "480923"},
            {"537260", "537260", "537260", "537260"},
            {"622273", null, "622273", null},
            {"711276", "711276", "711276", "711276"},
            {"988870", null, "988870", null}
        };

        JDBC.assertFullResultSet(rs, expRS);

        //
        //------------------------------------------------------------
        // test unit 2. UNION of joins;
        //------------------------------------------------------------

        // 201 - UNION two LEFT joins, with "or 1=0" predicate
        //                        UNION
        //                       /      \
        //                     LJ        LJ
        //                    /  \      /  \
        //                  T1    T2  T2    T1

        rs = st.executeQuery(
            "SELECT T1.charcol, T1.intcol, T2.intcol, "
            + "T2.dec62col, T2.floatcol"
            + "          FROM K55ADMIN.manytypes T1 LEFT JOIN"
            + "               k55admin.manytypes_notnull T2"
            + "            ON T1.intcol = T2.dec62col or 1=0"
            + "UNION"
            + " SELECT T2.charcol, T2.intcol, T1.intcol, "
            + "T2.dec62col, T2.floatcol"
            + "          FROM K55ADMIN.manytypes_notnull T2 LEFT JOIN"
            + "               k55admin.manytypes T1"
            + "            ON T2.smintcol = T1.dec62col or 1=0"
            + "ORDER BY 2");

        expColNames = new String [] {"CHARCOL", "INTCOL", "INTCOL", "DEC62COL", "FLOATCOL"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"One", "1", null, null, null},
            {"Two", "2", null, null, null},
            {"Three", "3", null, null, null},
            {"Four", "4", null, null, null},
            {"Five", "5", null, null, null},
            {"Six", "6", null, null, null},
            {"Seven", "7", null, null, null},
            {"Eight", "8", null, null, null},
            {"Nine", "9", null, null, null},
            {"Ten", "10", null, null, null},
            {"Eleven", "11", "11", "11.00", "11.0"},
            {"Twelve", "12", "12", "12.00", "12.0"},
            {"Thirteen", "13", "13", "13.00", "13.0"},
            {"Fourteen", "14", "14", "14.00", "14.0"},
            {"Fifteen", "15", "15", "15.00", "15.0"},
            {"Sixteen", "16", "16", "16.00", "16.0"},
            {"Seventeen", "17", "17", "17.00", "17.0"},
            {"Eighteen", "18", "18", "18.00", "18.0"},
            {"Nineteen", "19", "19", "19.00", "19.0"},
            {"Twenty", "20", "20", "20.00", "20.0"},
            {"Twenty One", "21", null, "21.00", "21.0"},
            {"Twenty Two", "22", null, "22.00", "22.0"},
            {"Twenty Three", "23", null, "23.00", "23.0"},
            {"Twenty Four", "24", null, "24.00", "24.0"},
            {"Twenty Five", "25", null, "25.00", "25.0"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 202 - Again, with extended ON clauses to join on
        //       multiple columns, with "or 1=0" predicate

        rs = st.executeQuery(
            "SELECT T1.charcol, T1.intcol, T2.intcol, T2.floatcol"
            + "          FROM K55ADMIN.manytypes T1 LEFT JOIN"
            + "               k55admin.manytypes_notnull T2"
            + "            ON T1.floatcol = T2.dec62col"
            + "               AND T1.smintcol = T2.intcol or 1=0"
            + "UNION"
            + " SELECT T2.charcol, T2.intcol, T1.intcol, T2.floatcol"
            + "          FROM K55ADMIN.manytypes_notnull T2 LEFT JOIN"
            + "               k55admin.manytypes T1"
            + "            ON T2.smintcol = T1.dec62col"
            + "               AND T2.floatcol = T1.intcol"
            + "               AND T2.smintcol = T1.smintcol or 1=0"
            + "ORDER BY 1");

        expColNames = new String [] {"CHARCOL", "INTCOL", "INTCOL", "FLOATCOL"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Eight", "8", null, null},
            {"Eighteen", "18", "18", "18.0"},
            {"Eleven", "11", "11", "11.0"},
            {"Fifteen", "15", "15", "15.0"},
            {"Five", "5", null, null},
            {"Four", "4", null, null},
            {"Fourteen", "14", "14", "14.0"},
            {"Nine", "9", null, null},
            {"Nineteen", "19", "19", "19.0"},
            {"One", "1", null, null},
            {"Seven", "7", null, null},
            {"Seventeen", "17", "17", "17.0"},
            {"Six", "6", null, null},
            {"Sixteen", "16", "16", "16.0"},
            {"Ten", "10", null, null},
            {"Thirteen", "13", "13", "13.0"},
            {"Three", "3", null, null},
            {"Twelve", "12", "12", "12.0"},
            {"Twenty", "20", "20", "20.0"},
            {"Twenty Five", "25", null, "25.0"},
            {"Twenty Four", "24", null, "24.0"},
            {"Twenty One", "21", null, "21.0"},
            {"Twenty Three", "23", null, "23.0"},
            {"Twenty Two", "22", null, "22.0"},
            {"Two", "2", null, null}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 203 - UNION ALL two RIGHT joins, with "or 1=0" predicate

        rs = st.executeQuery(
            "SELECT T1.charcol, T1.intcol, T2.intcol, T2.floatcol"
            + "          FROM K55ADMIN.manytypes T1 RIGHT JOIN"
            + "               k55admin.manytypes_notnull T2"
            + "            ON T1.intcol = T2.dec62col"
            + "               AND T1.smintcol = T2.intcol or 1=0"
            + "UNION ALL"
            + " SELECT T2.charcol, T2.intcol, T1.intcol, T2.floatcol"
            + "          FROM K55ADMIN.manytypes_notnull T2 RIGHT JOIN"
            + "               k55admin.manytypes T1"
            + "            ON T2.smintcol = T1.dec62col"
            + "               AND T2.smintcol = T1.intcol"
            + "               AND T2.floatcol = T1.smintcol or 1=0"
            + "ORDER BY 3");

        expColNames = new String [] {"CHARCOL", "INTCOL", "INTCOL", "FLOATCOL"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {null, null, "1", null},
            {null, null, "2", null},
            {null, null, "3", null},
            {null, null, "4", null},
            {null, null, "5", null},
            {null, null, "6", null},
            {null, null, "7", null},
            {null, null, "8", null},
            {null, null, "9", null},
            {null, null, "10", null},
            {"Eleven", "11", "11", "11.0"},
            {"Eleven", "11", "11", "11.0"},
            {"Twelve", "12", "12", "12.0"},
            {"Twelve", "12", "12", "12.0"},
            {"Thirteen", "13", "13", "13.0"},
            {"Thirteen", "13", "13", "13.0"},
            {"Fourteen", "14", "14", "14.0"},
            {"Fourteen", "14", "14", "14.0"},
            {"Fifteen", "15", "15", "15.0"},
            {"Fifteen", "15", "15", "15.0"},
            {"Sixteen", "16", "16", "16.0"},
            {"Sixteen", "16", "16", "16.0"},
            {"Seventeen", "17", "17", "17.0"},
            {"Seventeen", "17", "17", "17.0"},
            {"Eighteen", "18", "18", "18.0"},
            {"Eighteen", "18", "18", "18.0"},
            {"Nineteen", "19", "19", "19.0"},
            {"Nineteen", "19", "19", "19.0"},
            {"Twenty", "20", "20", "20.0"},
            {"Twenty", "20", "20", "20.0"},
            {null, null, "21", "21.0"},
            {null, null, "22", "22.0"},
            {null, null, "23", "23.0"},
            {null, null, "24", "24.0"},
            {null, null, "25", "25.0"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 204 - UNION ALL a RIGHT join with an INNER join, with
        //       "or 1=0" predicate

        rs = st.executeQuery(
            "SELECT T1.charcol, T1.intcol, T2.intcol, T2.floatcol"
            + "          FROM K55ADMIN.manytypes T1 RIGHT JOIN"
            + "               k55admin.manytypes_notnull T2"
            + "            ON T1.floatcol = T2.dec62col"
            + "               AND T1.smintcol = T2.intcol or 1=0"
            + "UNION ALL"
            + " SELECT T2.charcol, T2.intcol, T1.intcol, T2.floatcol"
            + "          FROM K55ADMIN.manytypes_notnull T2 INNER JOIN"
            + "               k55admin.manytypes T1"
            + "            ON T2.smintcol = T1.dec62col"
            + "               AND T2.smintcol = T1.intcol"
            + "               AND T2.smintcol = T1.smintcol or 1=0"
            + "ORDER BY 2, 3");

        expColNames = new String [] {"CHARCOL", "INTCOL", "INTCOL", "FLOATCOL"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Eleven", "11", "11", "11.0"},
            {"Eleven", "11", "11", "11.0"},
            {"Twelve", "12", "12", "12.0"},
            {"Twelve", "12", "12", "12.0"},
            {"Thirteen", "13", "13", "13.0"},
            {"Thirteen", "13", "13", "13.0"},
            {"Fourteen", "14", "14", "14.0"},
            {"Fourteen", "14", "14", "14.0"},
            {"Fifteen", "15", "15", "15.0"},
            {"Fifteen", "15", "15", "15.0"},
            {"Sixteen", "16", "16", "16.0"},
            {"Sixteen", "16", "16", "16.0"},
            {"Seventeen", "17", "17", "17.0"},
            {"Seventeen", "17", "17", "17.0"},
            {"Eighteen", "18", "18", "18.0"},
            {"Eighteen", "18", "18", "18.0"},
            {"Nineteen", "19", "19", "19.0"},
            {"Nineteen", "19", "19", "19.0"},
            {"Twenty", "20", "20", "20.0"},
            {"Twenty", "20", "20", "20.0"},
            {null, null, "21", "21.0"},
            {null, null, "22", "22.0"},
            {null, null, "23", "23.0"},
            {null, null, "24", "24.0"},
            {null, null, "25", "25.0"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        //
        //------------------------------------------------------------
        // test unit 3. Nest joins;
        //------------------------------------------------------------

        // 301 - RIGHT join two LEFT joins, with "or 1=0" predicate
        //                          RJ
        //                       /      \
        //                     LJ        LJ
        //                    /  \      /  \
        //                  T1    T2  T2    T1

        rs = st.executeQuery(
            "SELECT t1.charcol, t1.intcol, t2.smintcol,"
            + "       t2.intcol, t2.smintcol, t3.dec62col, T4.floatcol"
            + "  FROM (K55ADMIN.manytypes T1 LEFT JOIN "
            + "K55ADMIN.manytypes_notnull T2"
            + "            ON T1.intcol = T2.floatcol or 1=0)"
            + "     RIGHT JOIN"
            + "       (k55admin.manytypes_notnull T4 LEFT JOIN "
            + "K55ADMIN.manytypes T3"
            + "            ON T4.smintcol = T3.dec62col or 1=0)"
            + "     ON T1.intcol = T4.smintcol or 1=0"
            + "ORDER BY 7");

        expColNames = new String [] {"CHARCOL", "INTCOL", "SMINTCOL", "INTCOL", "SMINTCOL", "DEC62COL", "FLOATCOL"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Eleven", "11", "11", "11", "11", "11.00", "11.0"},
            {"Twelve", "12", "12", "12", "12", "12.00", "12.0"},
            {"Thirteen", "13", "13", "13", "13", "13.00", "13.0"},
            {"Fourteen", "14", "14", "14", "14", "14.00", "14.0"},
            {"Fifteen", "15", "15", "15", "15", "15.00", "15.0"},
            {"Sixteen", "16", "16", "16", "16", "16.00", "16.0"},
            {"Seventeen", "17", "17", "17", "17", "17.00", "17.0"},
            {"Eighteen", "18", "18", "18", "18", "18.00", "18.0"},
            {"Nineteen", "19", "19", "19", "19", "19.00", "19.0"},
            {"Twenty", "20", "20", "20", "20", "20.00", "20.0"},
            {null, null, null, null, null, null, "21.0"},
            {null, null, null, null, null, null, "22.0"},
            {null, null, null, null, null, null, "23.0"},
            {null, null, null, null, null, null, "24.0"},
            {null, null, null, null, null, null, "25.0"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 302 - LEFT join two RIGHT joins, with "or 1=0" predicate
        //                          LJ
        //                       /      \
        //                     RJ        RJ
        //                    /  \      /  \
        //                  T1    T2  T2    T1

        rs = st.executeQuery(
            "SELECT t1.charcol, t1.intcol, t2.smintcol,"
            + "       t2.intcol, t2.smintcol, t3.dec62col, T4.floatcol"
            + "  FROM (K55ADMIN.manytypes T1 RIGHT JOIN "
            + "K55ADMIN.manytypes_notnull T2"
            + "            ON T1.intcol = T2.floatcol or 1=0)"
            + "     LEFT JOIN"
            + "       (k55admin.manytypes_notnull T4 RIGHT JOIN "
            + "K55ADMIN.manytypes T3"
            + "            ON T4.smintcol = T3.dec62col or 1=0)"
            + "     ON T1.intcol = T4.smintcol or 1=0"
            + "ORDER BY 3");

        expColNames = new String [] {"CHARCOL", "INTCOL", "SMINTCOL", "INTCOL", "SMINTCOL", "DEC62COL", "FLOATCOL"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Eleven", "11", "11", "11", "11", "11.00", "11.0"},
            {"Twelve", "12", "12", "12", "12", "12.00", "12.0"},
            {"Thirteen", "13", "13", "13", "13", "13.00", "13.0"},
            {"Fourteen", "14", "14", "14", "14", "14.00", "14.0"},
            {"Fifteen", "15", "15", "15", "15", "15.00", "15.0"},
            {"Sixteen", "16", "16", "16", "16", "16.00", "16.0"},
            {"Seventeen", "17", "17", "17", "17", "17.00", "17.0"},
            {"Eighteen", "18", "18", "18", "18", "18.00", "18.0"},
            {"Nineteen", "19", "19", "19", "19", "19.00", "19.0"},
            {"Twenty", "20", "20", "20", "20", "20.00", "20.0"},
            {null, null, "21", "21", "21", null, null},
            {null, null, "22", "22", "22", null, null},
            {null, null, "23", "23", "23", null, null},
            {null, null, "24", "24", "24", null, null},
            {null, null, "25", "25", "25", null, null}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 303 - LEFT join a RIGHT join and LEFT join, with "or 1=0" predicate
        //                                  LJ
        //                               /      \
        //                             RJ        LJ
        //                            /  \      /   \
        //                          T1    T2  T2    T1

        rs = st.executeQuery(
            "SELECT t1.charcol, t1.intcol, t2.smintcol,"
            + "       t2.intcol, t2.smintcol, t3.dec62col, T4.floatcol"
            + "  FROM (K55ADMIN.manytypes T1 RIGHT JOIN "
            + "K55ADMIN.manytypes_notnull T2"
            + "            ON T1.intcol = T2.dec62col or 1=0)"
            + "     LEFT JOIN"
            + "       (k55admin.manytypes_notnull T4 LEFT JOIN "
            + "K55ADMIN.manytypes T3"
            + "            ON T4.floatcol = T3.dec62col or 1=0)"
            + "     ON T1.intcol = T4.smintcol or 1=0"
            + "ORDER BY 4");

        expColNames = new String [] {"CHARCOL", "INTCOL", "SMINTCOL", "INTCOL", "SMINTCOL", "DEC62COL", "FLOATCOL"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Eleven", "11", "11", "11", "11", "11.00", "11.0"},
            {"Twelve", "12", "12", "12", "12", "12.00", "12.0"},
            {"Thirteen", "13", "13", "13", "13", "13.00", "13.0"},
            {"Fourteen", "14", "14", "14", "14", "14.00", "14.0"},
            {"Fifteen", "15", "15", "15", "15", "15.00", "15.0"},
            {"Sixteen", "16", "16", "16", "16", "16.00", "16.0"},
            {"Seventeen", "17", "17", "17", "17", "17.00", "17.0"},
            {"Eighteen", "18", "18", "18", "18", "18.00", "18.0"},
            {"Nineteen", "19", "19", "19", "19", "19.00", "19.0"},
            {"Twenty", "20", "20", "20", "20", "20.00", "20.0"},
            {null, null, "21", "21", "21", null, null},
            {null, null, "22", "22", "22", null, null},
            {null, null, "23", "23", "23", null, null},
            {null, null, "24", "24", "24", null, null},
            {null, null, "25", "25", "25", null, null}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 304 - LEFT join a RIGHT join and LEFT join - join on
        // character column, with "or 1=0" predicate
        //                          LJ
        //                       /      \
        //                     RJ        LJ
        //                    /  \      /  \
        //                  T1    T2  T2    T1


        rs = st.executeQuery(
            "SELECT t1.charcol, t1.intcol, t2.smintcol,"
            + "       t2.intcol, t2.smintcol, t3.dec62col, T4.floatcol"
            + "  FROM (K55ADMIN.manytypes T1 RIGHT JOIN "
            + "K55ADMIN.manytypes_notnull T2"
            + "            ON T1.charcol = T2.vcharcol or 1=0)"
            + "     LEFT JOIN"
            + "       (k55admin.manytypes_notnull T4 LEFT JOIN "
            + "K55ADMIN.manytypes T3"
            + "            ON T4.charcol = T3.vcharcol or 1=0)"
            + "     ON T1.charcol = T4.vcharcol or 1=0"
            + "ORDER BY 2");

        expColNames = new String [] {"CHARCOL", "INTCOL", "SMINTCOL", "INTCOL", "SMINTCOL", "DEC62COL", "FLOATCOL"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Eleven", "11", "11", "11", "11", "11.00", "11.0"},
            {"Twelve", "12", "12", "12", "12", "12.00", "12.0"},
            {"Thirteen", "13", "13", "13", "13", "13.00", "13.0"},
            {"Fourteen", "14", "14", "14", "14", "14.00", "14.0"},
            {"Fifteen", "15", "15", "15", "15", "15.00", "15.0"},
            {"Sixteen", "16", "16", "16", "16", "16.00", "16.0"},
            {"Seventeen", "17", "17", "17", "17", "17.00", "17.0"},
            {"Eighteen", "18", "18", "18", "18", "18.00", "18.0"},
            {"Nineteen", "19", "19", "19", "19", "19.00", "19.0"},
            {"Twenty", "20", "20", "20", "20", "20.00", "20.0"},
            {null, null, "25", "25", "25", null, null},
            {null, null, "24", "24", "24", null, null},
            {null, null, "23", "23", "23", null, null},
            {null, null, "22", "22", "22", null, null},
            {null, null, "21", "21", "21", null, null}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 305 - LEFT join a RIGHT join and LEFT join on multiple
        // columns, with "or 1=0" predicate;
        //                          RJ
        //                       /      \
        //                     RJ        LJ
        //                    /  \      /  \
        //                  T1    T2  T2    T1

        rs = st.executeQuery(
            "SELECT t1.charcol, t1.intcol, t2.smintcol,"
            + "       t2.intcol, t2.smintcol, t3.dec62col, T4.floatcol"
            + "  FROM (K55ADMIN.manytypes T1 RIGHT JOIN "
            + "K55ADMIN.manytypes_notnull T2"
            + "            ON T1.intcol = T2.dec62col AND "
            + "T1.charcol = T2.vcharcol or 1=0)"
            + "     RIGHT JOIN"
            + "       (k55admin.manytypes_notnull T4 LEFT JOIN "
            + "K55ADMIN.manytypes T3"
            + "            ON T4.floatcol = T3.dec62col AND "
            + "T4.charcol = T3.vcharcol or 1=0)"
            + "     ON T1.intcol = T4.smintcol AND T1.charcol = "
            + "T4.vcharcol or 1=0"
            + "ORDER BY 7");

        expColNames = new String [] {"CHARCOL", "INTCOL", "SMINTCOL", "INTCOL", "SMINTCOL", "DEC62COL", "FLOATCOL"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Eleven", "11", "11", "11", "11", "11.00", "11.0"},
            {"Twelve", "12", "12", "12", "12", "12.00", "12.0"},
            {"Thirteen", "13", "13", "13", "13", "13.00", "13.0"},
            {"Fourteen", "14", "14", "14", "14", "14.00", "14.0"},
            {"Fifteen", "15", "15", "15", "15", "15.00", "15.0"},
            {"Sixteen", "16", "16", "16", "16", "16.00", "16.0"},
            {"Seventeen", "17", "17", "17", "17", "17.00", "17.0"},
            {"Eighteen", "18", "18", "18", "18", "18.00", "18.0"},
            {"Nineteen", "19", "19", "19", "19", "19.00", "19.0"},
            {"Twenty", "20", "20", "20", "20", "20.00", "20.0"},
            {null, null, null, null, null, null, "21.0"},
            {null, null, null, null, null, null, "22.0"},
            {null, null, null, null, null, null, "23.0"},
            {null, null, null, null, null, null, "24.0"},
            {null, null, null, null, null, null, "25.0"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        //
        //------------------------------------------------------------
        // test unit 4. Join a Nest join with a
        // table;
        //------------------------------------------------------------

        // 401 - LEFT join a RIGHT join of two LEFT
        // joins into another table, with "or 1=0" predicate;
        //                                 LJ
        //                              /      \
        //                          RJ          T3
        //                       /      \
        //                     LJ        LJ
        //                    /  \      /  \
        //                  T1    T2  T2    T1

        rs = st.executeQuery(
            "SELECT t1.intcol, t1.smintcol, t5.charcol"
            + "  FROM (K55ADMIN.manytypes T1 LEFT JOIN "
            + "K55ADMIN.manytypes_notnull T2"
            + "            ON T1.intcol = T2.dec62col or 1=0)"
            + "     RIGHT JOIN"
            + "       (k55admin.manytypes_notnull T4 LEFT JOIN "
            + "K55ADMIN.manytypes T3"
            + "            ON T4.smintcol = T3.dec62col or 1=0)"
            + "     ON T1.intcol = T4.smintcol or 1=0"
            + "     LEFT JOIN"
            + "      k55admin.manytypes_ctrl T5"
            + "    ON T5.intcol = T1.smintcol or 1=0"
            + "ORDER BY 1");

        expColNames = new String [] {"INTCOL", "SMINTCOL", "CHARCOL"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"11", "11", null},
            {"12", "12", "Twelve"},
            {"13", "13", null},
            {"14", "14", "Fourteen"},
            {"15", "15", null},
            {"16", "16", "Sixteen"},
            {"17", "17", null},
            {"18", "18", "Eighteen"},
            {"19", "19", null},
            {"20", "20", "Twenty"},
            {null, null, null},
            {null, null, null},
            {null, null, null},
            {null, null, null},
            {null, null, null}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 402 - Add WHERE clause to previous query, with "or 1=0"
        // predicate

        rs = st.executeQuery(
            "SELECT t1.intcol, t1.smintcol, t5.charcol, t5.intcol"
            + "  FROM (K55ADMIN.manytypes T1 LEFT JOIN "
            + "K55ADMIN.manytypes_notnull T2"
            + "            ON T1.intcol = T2.dec62col or 1=0)"
            + "     RIGHT JOIN"
            + "       (k55admin.manytypes_notnull T4 LEFT JOIN "
            + "K55ADMIN.manytypes T3"
            + "            ON T4.smintcol = T3.dec62col or 1=0)"
            + "     ON T1.intcol = T4.smintcol or 1=0"
            + "     LEFT JOIN"
            + "      k55admin.manytypes_ctrl T5"
            + "    ON T5.intcol = T1.smintcol or 1=0"
            + "  WHERE t5.intcol < 15 or t1.intcol IS NULL or 1=0"
            + "ORDER BY 1");

        expColNames = new String [] {"INTCOL", "SMINTCOL", "CHARCOL", "INTCOL"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"12", "12", "Twelve", "12"},
            {"14", "14", "Fourteen", "14"},
            {null, null, null, null},
            {null, null, null, null},
            {null, null, null, null},
            {null, null, null, null},
            {null, null, null, null}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 403 - RIGHT join a RIGHT join of two LEFT joins into
        // another table, with "or 1=0" predicate;
        //                                 RJ
        //                              /      \
        //                          RJ          T3
        //                       /      \
        //                     LJ        LJ
        //                    /  \      /  \
        //                  T1    T2  T2    T1

        rs = st.executeQuery(
            "SELECT t1.intcol, t3.smintcol, t5.charcol"
            + "  FROM (K55ADMIN.manytypes T1 LEFT JOIN "
            + "K55ADMIN.manytypes_notnull T2"
            + "            ON T1.intcol = T2.dec62col or 1=0)"
            + "     RIGHT JOIN"
            + "       (k55admin.manytypes_notnull T4 LEFT JOIN "
            + "K55ADMIN.manytypes T3"
            + "            ON T4.smintcol = T3.dec62col or 1=0)"
            + "     ON T1.intcol = T4.smintcol or 1=0"
            + "     RIGHT JOIN"
            + "      k55admin.manytypes_ctrl T5"
            + "    ON T5.intcol = T1.smintcol or 1=0"
            + "ORDER BY 1, 3");

        expColNames = new String [] {"INTCOL", "SMINTCOL", "CHARCOL"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"12", "12", "Twelve"},
            {"14", "14", "Fourteen"},
            {"16", "16", "Sixteen"},
            {"18", "18", "Eighteen"},
            {"20", "20", "Twenty"},
            {null, null, "Eight"},
            {null, null, "Four"},
            {null, null, "Six"},
            {null, null, "Ten"},
            {null, null, "Twenty Four"},
            {null, null, "Twenty Two"},
            {null, null, "Two"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 404 - Add WHERE clause to previous query, with "or 1=0"
        //       predicate

        rs = st.executeQuery(
            "SELECT t1.intcol, t3.smintcol, t5.charcol"
            + "  FROM (K55ADMIN.manytypes T1 LEFT JOIN "
            + "K55ADMIN.manytypes_notnull T2"
            + "            ON T1.intcol = T2.dec62col or 1=0)"
            + "     RIGHT JOIN"
            + "       (k55admin.manytypes_notnull T4 LEFT JOIN "
            + "K55ADMIN.manytypes T3"
            + "            ON T4.smintcol = T3.dec62col or 1=0)"
            + "     ON T1.intcol = T4.smintcol or 1=0"
            + "     RIGHT JOIN"
            + "      k55admin.manytypes_ctrl T5"
            + "    ON T5.intcol = T1.smintcol or 1=0"
            + "  WHERE t1.intcol < 15 or t1.intcol IS NULL or 1=0"
            + "ORDER BY 1, 3");

        expColNames = new String [] {"INTCOL", "SMINTCOL", "CHARCOL"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"12", "12", "Twelve"},
            {"14", "14", "Fourteen"},
            {null, null, "Eight"},
            {null, null, "Four"},
            {null, null, "Six"},
            {null, null, "Ten"},
            {null, null, "Twenty Four"},
            {null, null, "Twenty Two"},
            {null, null, "Two"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 405 - RIGHT join a RIGHT join of an INNER joins and a
        //       LEFT join into another table, with "or 1=0" predicate;
        //                                 RJ
        //                              /      \
        //                          RJ          T3
        //                       /      \
        //                     IJ        LJ
        //                    /  \      /  \
        //                  T1    T2  T2    T1

        rs = st.executeQuery(
            "SELECT t1.intcol, t3.smintcol, t5.charcol"
            + "  FROM (K55ADMIN.manytypes T1 INNER JOIN "
            + "K55ADMIN.manytypes_notnull T2"
            + "            ON T1.intcol = T2.dec62col or 1=0)"
            + "     RIGHT JOIN"
            + "       (k55admin.manytypes_notnull T4 LEFT JOIN "
            + "K55ADMIN.manytypes T3"
            + "            ON T4.smintcol = T3.dec62col or 1=0)"
            + "     ON T1.intcol = T4.smintcol or 1=0"
            + "     RIGHT JOIN"
            + "      k55admin.manytypes_ctrl T5"
            + "    ON T5.intcol = T1.smintcol or 1=0"
            + "ORDER BY 1, 3");

        expColNames = new String [] {"INTCOL", "SMINTCOL", "CHARCOL"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"12", "12", "Twelve"},
            {"14", "14", "Fourteen"},
            {"16", "16", "Sixteen"},
            {"18", "18", "Eighteen"},
            {"20", "20", "Twenty"},
            {null, null, "Eight"},
            {null, null, "Four"},
            {null, null, "Six"},
            {null, null, "Ten"},
            {null, null, "Twenty Four"},
            {null, null, "Twenty Two"},
            {null, null, "Two"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 406 - Add WHERE clause to previous query, with "or 1=0"
        //       predicate

        rs = st.executeQuery(
            "SELECT t1.intcol, t3.smintcol, t5.charcol"
            + "  FROM (K55ADMIN.manytypes T1 INNER JOIN "
            + "K55ADMIN.manytypes_notnull T2"
            + "            ON T1.intcol = T2.dec62col or 1=0)"
            + "     RIGHT JOIN"
            + "       (k55admin.manytypes_notnull T4 LEFT JOIN "
            + "K55ADMIN.manytypes T3"
            + "            ON T4.smintcol = T3.dec62col or 1=0)"
            + "     ON T1.intcol = T4.smintcol or 1=0"
            + "     RIGHT JOIN"
            + "      k55admin.manytypes_ctrl T5"
            + "    ON T5.intcol = T1.smintcol or 1=0"
            + "  WHERE t1.intcol < 15 or t5.intcol IS NOT NULL or 1=0"
            + "ORDER BY 1, 3");

        expColNames = new String [] {"INTCOL", "SMINTCOL", "CHARCOL"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"12", "12", "Twelve"},
            {"14", "14", "Fourteen"},
            {"16", "16", "Sixteen"},
            {"18", "18", "Eighteen"},
            {"20", "20", "Twenty"},
            {null, null, "Eight"},
            {null, null, "Four"},
            {null, null, "Six"},
            {null, null, "Ten"},
            {null, null, "Twenty Four"},
            {null, null, "Twenty Two"},
            {null, null, "Two"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 407 - RIGHT join a RIGHT join of an INNER joins and a
        // LEFT join into another table - join on character column,
        // with "or 1=0" predicate;
        //                                 RJ
        //                              /      \
        //                          RJ          T3
        //                       /      \
        //                     IJ        LJ
        //                    /  \      /  \
        //                  T1    T2  T2    T1

        rs = st.executeQuery(
            "SELECT t1.intcol, t1.smintcol, t4.dec62col, t5.charcol"
            + "  FROM (K55ADMIN.manytypes T1 INNER JOIN "
            + "K55ADMIN.manytypes_notnull T2"
            + "            ON T1.intcol = T2.dec62col or 1=0)"
            + "     RIGHT JOIN"
            + "       (k55admin.manytypes_notnull T4 LEFT JOIN "
            + "K55ADMIN.manytypes T3"
            + "            ON T4.smintcol = T3.dec62col or 1=0)"
            + "     ON T1.intcol = T4.smintcol or 1=0"
            + "     RIGHT JOIN"
            + "      k55admin.manytypes_ctrl T5"
            + "    ON T5.charcol = T1.vcharcol or 1=0"
            + "ORDER BY 1, 4");

        expColNames = new String [] {"INTCOL", "SMINTCOL", "DEC62COL", "CHARCOL"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"12", "12", "12.00", "Twelve"},
            {"14", "14", "14.00", "Fourteen"},
            {"16", "16", "16.00", "Sixteen"},
            {"18", "18", "18.00", "Eighteen"},
            {"20", "20", "20.00", "Twenty"},
            {null, null, null, "Eight"},
            {null, null, null, "Four"},
            {null, null, null, "Six"},
            {null, null, null, "Ten"},
            {null, null, null, "Twenty Four"},
            {null, null, null, "Twenty Two"},
            {null, null, null, "Two"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 408 - Add WHERE clause to previous query, with "or 1=0"
        //       predicate

        rs = st.executeQuery(
            "SELECT t1.intcol, t3.smintcol, t4.dec62col, t5.charcol"
            + "  FROM (K55ADMIN.manytypes T1 INNER JOIN "
            + "K55ADMIN.manytypes_notnull T2"
            + "            ON T1.intcol = T2.dec62col or 1=0)"
            + "     RIGHT JOIN"
            + "       (k55admin.manytypes_notnull T4 LEFT JOIN "
            + "K55ADMIN.manytypes T3"
            + "            ON T4.smintcol = T3.dec62col or 1=0)"
            + "     ON T1.intcol = T4.smintcol or 1=0"
            + "     RIGHT JOIN"
            + "      k55admin.manytypes_ctrl T5"
            + "    ON T5.charcol = T1.vcharcol or 1=0"
            + "  WHERE t1.intcol between 12 and 18"
            + "ORDER BY 1");

        expColNames = new String [] {"INTCOL", "SMINTCOL", "DEC62COL", "CHARCOL"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"12", "12", "12.00", "Twelve"},
            {"14", "14", "14.00", "Fourteen"},
            {"16", "16", "16.00", "Sixteen"},
            {"18", "18", "18.00", "Eighteen"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 409 - RIGHT join a RIGHT join of an INNER joins and a
        // LEFT join into another table - join on multiple columns,
        // with "or 1=0" predicate;
        //                                 RJ
        //                              /      \
        //                          RJ          T3
        //                       /      \
        //                     IJ        LJ
        //                    /  \      /  \
        //                  T1    T2  T2    T1

        rs = st.executeQuery(
            "SELECT t1.intcol, t3.smintcol, t5.charcol"
            + "  FROM (K55ADMIN.manytypes T1 INNER JOIN "
            + "K55ADMIN.manytypes_notnull T2"
            + "            ON T1.intcol = T2.dec62col AND "
            + "T1.charcol = T2.vcharcol or 1=0)"
            + "     RIGHT JOIN"
            + "       (k55admin.manytypes_notnull T4 LEFT JOIN "
            + "K55ADMIN.manytypes T3"
            + "            ON T4.smintcol = T3.dec62col or 1=0)"
            + "     ON T1.intcol = T4.smintcol or 1=0"
            + "     RIGHT JOIN"
            + "       k55admin.manytypes_ctrl T5"
            + "    ON T5.charcol = T1.vcharcol AND T5.intcol = "
            + "T1.smintcol AND"
            + "       T5.intcol = T3.intcol or 1=0"
            + "ORDER BY 1, 3");

        expColNames = new String [] {"INTCOL", "SMINTCOL", "CHARCOL"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"12", "12", "Twelve"},
            {"14", "14", "Fourteen"},
            {"16", "16", "Sixteen"},
            {"18", "18", "Eighteen"},
            {"20", "20", "Twenty"},
            {null, null, "Eight"},
            {null, null, "Four"},
            {null, null, "Six"},
            {null, null, "Ten"},
            {null, null, "Twenty Four"},
            {null, null, "Twenty Two"},
            {null, null, "Two"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        //
        //------------------------------------------------------------
        // test unit 5. Join a table with a nest
        // join;
        //------------------------------------------------------------

        // 501 - LEFT join a table with a LEFT join
        // of a RIGHT join of two LEFT joins into that same table,
        // with "or 1=0" predicate
        //                          LJ
        //                       /      \
        //                     T3          LJ
        //                              /      \
        //                          RJ          T3
        //                       /      \
        //                     LJ        LJ
        //                    /  \      /  \
        //                  T1    T2  T2    T1

        rs = st.executeQuery(
            "SELECT t1.intcol, t5.smintcol, t6.charcol"
            + "  FROM K55ADMIN.manytypes_ctrl T6"
            + "     LEFT JOIN"
            + "      ((k55admin.manytypes T1 LEFT JOIN "
            + "K55ADMIN.manytypes_notnull T2"
            + "            ON T1.intcol = T2.dec62col or 1=0)"
            + "       RIGHT JOIN"
            + "         (k55admin.manytypes_notnull T4 LEFT JOIN "
            + "K55ADMIN.manytypes T3"
            + "              ON T4.smintcol = T3.dec62col or 1=0)"
            + "       ON T1.intcol = T4.smintcol or 1=0"
            + "       LEFT JOIN"
            + "        k55admin.manytypes_ctrl T5"
            + "      ON T5.intcol = T1.smintcol or 1=0)"
            + "    ON T6.intcol = T1.smintcol or 1=0"
            + "ORDER BY 1, 3");

        expColNames = new String [] {"INTCOL", "SMINTCOL", "CHARCOL"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"12", "12", "Twelve"},
            {"14", "14", "Fourteen"},
            {"16", "16", "Sixteen"},
            {"18", "18", "Eighteen"},
            {"20", "20", "Twenty"},
            {null, null, "Eight"},
            {null, null, "Four"},
            {null, null, "Six"},
            {null, null, "Ten"},
            {null, null, "Twenty Four"},
            {null, null, "Twenty Two"},
            {null, null, "Two"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 502 - Add WHERE clause to previous query, with "or 1=0"
        //       predicate

        rs = st.executeQuery(
            "SELECT t1.intcol, t5.smintcol, t6.charcol"
            + "  FROM K55ADMIN.manytypes_ctrl T6"
            + "     LEFT JOIN"
            + "      ((k55admin.manytypes T1 LEFT JOIN "
            + "K55ADMIN.manytypes_notnull T2"
            + "            ON T1.intcol = T2.dec62col or 1=0)"
            + "       RIGHT JOIN"
            + "         (k55admin.manytypes_notnull T4 LEFT JOIN "
            + "K55ADMIN.manytypes T3"
            + "              ON T4.smintcol = T3.dec62col or 1=0)"
            + "       ON T1.intcol = T4.smintcol or 1=0"
            + "       LEFT JOIN"
            + "        k55admin.manytypes_ctrl T5"
            + "      ON T5.intcol = T1.smintcol or 1=0)"
            + "    ON T6.intcol = T1.smintcol or 1=0"
            + "  WHERE t1.intcol IS NOT NULL"
            + " ORDER BY 1");

        expColNames = new String [] {"INTCOL", "SMINTCOL", "CHARCOL"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"12", "12", "Twelve"},
            {"14", "14", "Fourteen"},
            {"16", "16", "Sixteen"},
            {"18", "18", "Eighteen"},
            {"20", "20", "Twenty"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 503 - RIGHT join a table with a LEFT join of a RIGHT
        // join of two LEFT joins into that same table, with "or
        // 1=0" predicate
        //                          RJ
        //                       /      \
        //                     T3          LJ
        //                              /      \
        //                          RJ          T3
        //                       /      \
        //                     LJ        LJ
        //                    /  \      /  \
        //                  T1    T2  T2    T1

        rs = st.executeQuery(
            "SELECT t1.intcol, t5.smintcol, t6.charcol"
            + "  FROM K55ADMIN.manytypes_ctrl T6"
            + "     RIGHT JOIN"
            + "      ((k55admin.manytypes T1 LEFT JOIN "
            + "K55ADMIN.manytypes_notnull T2"
            + "            ON T1.intcol = T2.dec62col or 1=0)"
            + "       RIGHT JOIN"
            + "         (k55admin.manytypes_notnull T4 LEFT JOIN "
            + "K55ADMIN.manytypes T3"
            + "              ON T4.smintcol = T3.dec62col  or 1=0)"
            + "       ON T1.intcol = T4.smintcol or 1=0"
            + "       LEFT JOIN"
            + "        k55admin.manytypes_ctrl T5"
            + "      ON T5.intcol = T1.smintcol or 1=0)"
            + "    ON T6.intcol = T1.smintcol or 1=0"
            + " ORDER BY 1");

        expColNames = new String [] {"INTCOL", "SMINTCOL", "CHARCOL"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"11", null, null},
            {"12", "12", "Twelve"},
            {"13", null, null},
            {"14", "14", "Fourteen"},
            {"15", null, null},
            {"16", "16", "Sixteen"},
            {"17", null, null},
            {"18", "18", "Eighteen"},
            {"19", null, null},
            {"20", "20", "Twenty"},
            {null, null, null},
            {null, null, null},
            {null, null, null},
            {null, null, null},
            {null, null, null}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 504 - Add WHERE clause to previous query, with "or 1=0"
        //       predicate

        rs = st.executeQuery(
            "SELECT t1.intcol, t5.smintcol, t6.charcol"
            + "  FROM K55ADMIN.manytypes_ctrl T6"
            + "     RIGHT JOIN"
            + "      ((k55admin.manytypes T1 LEFT JOIN "
            + "K55ADMIN.manytypes_notnull T2"
            + "            ON T1.intcol = T2.dec62col or 1=0)"
            + "       RIGHT JOIN"
            + "         (k55admin.manytypes_notnull T4 LEFT JOIN "
            + "K55ADMIN.manytypes T3"
            + "              ON T4.smintcol = T3.dec62col or 1=0)"
            + "       ON T1.intcol = T4.smintcol or 1=0"
            + "       LEFT JOIN"
            + "        k55admin.manytypes_ctrl T5"
            + "      ON T5.intcol = T1.smintcol or 1=0)"
            + "    ON T6.intcol = T1.smintcol"
            + "  WHERE t1.intcol IS NOT NULL and t1.intcol > 12"
            + " ORDER BY 1");

        expColNames = new String [] {"INTCOL", "SMINTCOL", "CHARCOL"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"13", null, null},
            {"14", "14", "Fourteen"},
            {"15", null, null},
            {"16", "16", "Sixteen"},
            {"17", null, null},
            {"18", "18", "Eighteen"},
            {"19", null, null},
            {"20", "20", "Twenty"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 505 - INNER join a table with a LEFT join of a RIGHT
        // join of two LEFT joins into that same table, with "or
        // 1=0" predicate
        //                          IJ
        //                       /      \
        //                     T3          LJ
        //                              /      \
        //                          RJ          T3
        //                       /      \
        //                     LJ        LJ
        //                    /  \      /  \
        //                  T1    T2  T2    T1


        rs = st.executeQuery(
            "SELECT t1.intcol, t5.smintcol, t6.charcol"
            + "  FROM K55ADMIN.manytypes_ctrl T6"
            + "     INNER JOIN"
            + "      ((k55admin.manytypes T1 LEFT JOIN "
            + "K55ADMIN.manytypes_notnull T2"
            + "            ON T1.intcol = T2.dec62col or 1=0)"
            + "       RIGHT JOIN"
            + "         (k55admin.manytypes_notnull T4 LEFT JOIN "
            + "K55ADMIN.manytypes T3"
            + "              ON T4.smintcol = T3.dec62col or 1=0)"
            + "       ON T1.intcol = T4.smintcol or 1=0"
            + "       LEFT JOIN"
            + "        k55admin.manytypes_ctrl T5"
            + "      ON T5.intcol = T1.smintcol or 1=0)"
            + "    ON T6.intcol = T1.smintcol or 1=0"
            + " ORDER BY 1");

        expColNames = new String [] {"INTCOL", "SMINTCOL", "CHARCOL"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"12", "12", "Twelve"},
            {"14", "14", "Fourteen"},
            {"16", "16", "Sixteen"},
            {"18", "18", "Eighteen"},
            {"20", "20", "Twenty"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 506 - Add WHERE clause to previous query, with "or 1=0"
        //       predicate

        rs = st.executeQuery(
            "SELECT t1.intcol, t1.smintcol, t1.dec62col"
            + "  FROM K55ADMIN.manytypes_ctrl T6"
            + "     INNER JOIN"
            + "      ((k55admin.manytypes T1 LEFT JOIN "
            + "K55ADMIN.manytypes_notnull T2"
            + "            ON T1.intcol = T2.dec62col or 1=0)"
            + "       RIGHT JOIN"
            + "         (k55admin.manytypes_notnull T4 LEFT JOIN "
            + "K55ADMIN.manytypes T3"
            + "              ON T4.smintcol = T3.dec62col or 1=0)"
            + "       ON T1.intcol = T4.smintcol or 1=0"
            + "       LEFT JOIN"
            + "        k55admin.manytypes_ctrl T5"
            + "      ON T5.intcol = T1.smintcol or 1=0)"
            + "    ON T6.intcol = T1.smintcol or 1=0"
            + "  WHERE t1.intcol <= 20"
            + " ORDER BY 1");

        expColNames = new String [] {"INTCOL", "SMINTCOL", "DEC62COL"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"12", "12", "12.00"},
            {"14", "14", "14.00"},
            {"16", "16", "16.00"},
            {"18", "18", "18.00"},
            {"20", "20", "20.00"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 507 - INNER join a table with a LEFT join of a RIGHT
        // join of two LEFT joins into that same table - join on
        // character column, with "or 1=0" predicate;
        //                          IJ
        //                       /      \
        //                     T3          LJ
        //                              /      \
        //                          RJ          T3
        //                       /      \
        //                     LJ        LJ
        //                    /  \      /  \
        //                  T1    T2  T2    T1

        rs = st.executeQuery(
            "SELECT t1.intcol, t3.smintcol, t5.dec62col, t6.charcol"
            + "  FROM K55ADMIN.manytypes_ctrl T6"
            + "     INNER JOIN"
            + "      ((k55admin.manytypes T1 LEFT JOIN "
            + "K55ADMIN.manytypes_notnull T2"
            + "            ON T1.charcol = T2.vcharcol or 1=0)"
            + "       RIGHT JOIN"
            + "         (k55admin.manytypes_notnull T4 LEFT JOIN "
            + "K55ADMIN.manytypes T3"
            + "              ON T4.smintcol = T3.dec62col or 1=0)"
            + "       ON T1.intcol = T4.smintcol or 1=0"
            + "       LEFT JOIN"
            + "        k55admin.manytypes_ctrl T5"
            + "      ON T5.intcol = T1.smintcol or 1=0)"
            + "    ON T6.charcol = T1.vcharcol or 1=0"
            + " ORDER BY 1");

        expColNames = new String [] {"INTCOL", "SMINTCOL", "DEC62COL", "CHARCOL"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"12", "12", "12.00", "Twelve"},
            {"14", "14", "14.00", "Fourteen"},
            {"16", "16", "16.00", "Sixteen"},
            {"18", "18", "18.00", "Eighteen"},
            {"20", "20", "20.00", "Twenty"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 508 - INNER join a table with a LEFT join of a RIGHT
        // join of two LEFT joins into that same table - join on
        // multiple columns, with "or 1=0" predicate;
        //                          RJ
        //                       /      \
        //                     T3          LJ
        //                              /      \
        //                          RJ          T3
        //                       /      \
        //                     LJ        LJ
        //                    /  \      /  \
        //                  T1    T2  T2    T1

        rs = st.executeQuery(
            "SELECT t1.intcol, t3.smintcol, t5.dec62col, "
            + "t6.charcol, t1.vcharcol, t5.charcol"
            + "  FROM K55ADMIN.manytypes_ctrl T6"
            + "     RIGHT JOIN"
            + "      ((k55admin.manytypes T1 LEFT JOIN "
            + "K55ADMIN.manytypes_notnull T2"
            + "            ON T1.charcol = T2.vcharcol AND "
            + "T1.intcol = T2.dec62col or 1=0)"
            + "       RIGHT JOIN"
            + "         (k55admin.manytypes_notnull T4 LEFT JOIN "
            + "K55ADMIN.manytypes T3"
            + "              ON T4.smintcol = T3.dec62col or 1=0)"
            + "       ON T1.intcol = T4.smintcol or 1=0"
            + "       LEFT JOIN"
            + "        k55admin.manytypes_ctrl T5"
            + "      ON T5.intcol = T1.smintcol AND T5.charcol = "
            + "T1.vcharcol or 1=0)"
            + "    ON T6.charcol = T1.vcharcol AND T6.intcol = "
            + "T3.smintcol AND "
            + "       T6.intcol = T5.intcol or 1=0"
            + " ORDER BY 1");

        expColNames = new String [] {"INTCOL", "SMINTCOL", "DEC62COL", "CHARCOL", "VCHARCOL", "CHARCOL"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"11", "11", null, null, "Eleven", null},
            {"12", "12", "12.00", "Twelve", "Twelve", "Twelve"},
            {"13", "13", null, null, "Thirteen", null},
            {"14", "14", "14.00", "Fourteen", "Fourteen", "Fourteen"},
            {"15", "15", null, null, "Fifteen", null},
            {"16", "16", "16.00", "Sixteen", "Sixteen", "Sixteen"},
            {"17", "17", null, null, "Seventeen", null},
            {"18", "18", "18.00", "Eighteen", "Eighteen", "Eighteen"},
            {"19", "19", null, null, "Nineteen", null},
            {"20", "20", "20.00", "Twenty", "Twenty", "Twenty"},
            {null, null, null, null, null, null},
            {null, null, null, null, null, null},
            {null, null, null, null, null, null},
            {null, null, null, null, null, null},
            {null, null, null, null, null, null}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 509 - Add WHERE clause to previous query, with "or 1=0"
        //       predicate

        rs = st.executeQuery(
            "SELECT t1.intcol, t3.smintcol, t5.dec62col, "
            + "t6.charcol, t1.vcharcol, t5.charcol"
            + "  FROM K55ADMIN.manytypes_ctrl T6"
            + "     RIGHT JOIN"
            + "      ((k55admin.manytypes T1 LEFT JOIN "
            + "K55ADMIN.manytypes_notnull T2"
            + "            ON T1.charcol = T2.vcharcol AND "
            + "T1.intcol = T2.dec62col or 1=0)"
            + "       RIGHT JOIN"
            + "         (k55admin.manytypes_notnull T4 LEFT JOIN "
            + "K55ADMIN.manytypes T3"
            + "              ON T4.smintcol = T3.dec62col or 1=0)"
            + "       ON T1.intcol = T4.smintcol or 1=0"
            + "       LEFT JOIN"
            + "        k55admin.manytypes_ctrl T5"
            + "      ON T5.intcol = T1.smintcol AND T5.charcol = "
            + "T1.vcharcol or 1=0)"
            + "    ON T6.charcol = T1.vcharcol AND T6.intcol = "
            + "T3.smintcol AND "
            + "       T6.intcol = T5.intcol or 1=0"
            + "  WHERE t1.vcharcol like 'T%'"
            + " ORDER BY 1");

        expColNames = new String [] {"INTCOL", "SMINTCOL", "DEC62COL", "CHARCOL", "VCHARCOL", "CHARCOL"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"12", "12", "12.00", "Twelve", "Twelve", "Twelve"},
            {"13", "13", null, null, "Thirteen", null},
            {"20", "20", "20.00", "Twenty", "Twenty", "Twenty"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        //------------------------------------------------------------
        // -- multiple joins (224)
        //------------------------------------------------------------

        //------------------------------------------------------------
        // test unit 6. Create Views needed for other
        // testunit below;
        //------------------------------------------------------------

        // 602 - Create View on LEFT join, with "or 1=0" predicate;
        //                    View
        //                      |
        //                     LJ
        //                    /  \
        //                  T1    T2

        st.executeUpdate(
            "CREATE VIEW K55ADMIN.LJview1"
            + "  (charcol, smintcol, intcol, floatcol, vcharcol) AS"
            + "    SELECT T1.charcol, T1.smintcol,"
            + "           T2.intcol, T2.floatcol, T2.vcharcol"
            + "      FROM K55ADMIN.manytypes as t1 LEFT JOIN "
            + "K55ADMIN.manytypes_notnull as T2"
            + "        ON T1.smintcol = T2.intcol or 1=0");

        // 603 - Create View on 2 LEFT joins, with "or 1=0"
        // predicate
        //                         View
        //                          |
        //                          LJ
        //                       /      \
        //                     LJ        T3
        //                    /  \
        //                  T1    T2


        st.executeUpdate(
            "CREATE VIEW K55ADMIN.LJview2"
            + "  (charcol, smintcol, intcol, dec62col, floatcol, "
            + "vcharcol) AS"
            + "    SELECT T1.charcol, T1.smintcol, T2.intcol,"
            + "           T3.dec62col, T3.floatcol, T2.vcharcol"
            + "      FROM (K55ADMIN.manytypes as T1 LEFT JOIN "
            + "K55ADMIN.manytypes_notnull as T2"
            + "         ON T1.smintcol = T2.intcol or 1=0)"
            + "           LEFT JOIN"
            + "             k55admin.manytypes_ctrl as T3"
            + "           ON T1.smintcol = T3.intcol or 1=0");

        // 604 - Create View on RIGHT join, with "or 1=0"
        // predicate
        //                    View
        //                      |
        //                     RJ
        //                    /  \
        //                  T1    T2


        st.executeUpdate(
            "CREATE VIEW K55ADMIN.RJview1"
            + "  (charcol, smintcol, intcol, floatcol, vcharcol) AS"
            + "    SELECT T2.charcol, T2.smintcol,"
            + "           T1.intcol, T1.floatcol, T1.vcharcol"
            + "      FROM K55ADMIN.manytypes_notnull as T1 RIGHT "
            + "JOIN K55ADMIN.manytypes as T2"
            + "        ON T2.smintcol = T1.intcol or 1=0");

        // 605 - Create View on 2 RIGHT joins, with "or 1=0"
        // predicate
        //                         View
        //                          |
        //                          RJ
        //                       /      \
        //                     RJ        T3
        //                    /  \
        //                  T1    T2


        st.executeUpdate(
            "CREATE VIEW K55admin.RJview2"
            + "  (charcol, smintcol, intcol, dec62col, floatcol, "
            + "vcharcol) AS"
            + "    SELECT T1.charcol, T1.smintcol, T2.intcol,"
            + "           T3.dec62col, T3.floatcol, T2.vcharcol"
            + "      FROM (K55ADMIN.manytypes as T1 RIGHT JOIN "
            + "K55ADMIN.manytypes_notnull as T2"
            + "         ON T1.smintcol = T2.intcol or 1=0)"
            + "           RIGHT JOIN"
            + "             k55admin.manytypes_ctrl as T3"
            + "           ON T1.smintcol = T3.intcol or 1=0");

        // 606 - Create View on INNER join, with "or 1=0"
        // predicate
        //                    View
        //                      |
        //                     IJ
        //                    /  \
        //                  T1    T2

        st.executeUpdate(
            "CREATE VIEW K55ADMIN.IJview1"
            + "  (charcol, smintcol, intcol, floatcol, vcharcol) AS"
            + "    SELECT T1.charcol, T1.smintcol,"
            + "           T2.intcol, T2.floatcol, T2.vcharcol"
            + "      FROM K55ADMIN.manytypes as T1 INNER JOIN "
            + "K55ADMIN.manytypes_notnull as T2"
            + "        ON T1.smintcol = T2.intcol or 1=0");

        // 607 - Check data in the Views

        rs = st.executeQuery(
            "SELECT * FROM K55ADMIN.LJview1 ORDER BY smintcol");

        expColNames = new String [] {"CHARCOL", "SMINTCOL", "INTCOL", "FLOATCOL", "VCHARCOL"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"One", "1", null, null, null},
            {"Two", "2", null, null, null},
            {"Three", "3", null, null, null},
            {"Four", "4", null, null, null},
            {"Five", "5", null, null, null},
            {"Six", "6", null, null, null},
            {"Seven", "7", null, null, null},
            {"Eight", "8", null, null, null},
            {"Nine", "9", null, null, null},
            {"Ten", "10", null, null, null},
            {"Eleven", "11", "11", "11.0", "Eleven"},
            {"Twelve", "12", "12", "12.0", "Twelve"},
            {"Thirteen", "13", "13", "13.0", "Thirteen"},
            {"Fourteen", "14", "14", "14.0", "Fourteen"},
            {"Fifteen", "15", "15", "15.0", "Fifteen"},
            {"Sixteen", "16", "16", "16.0", "Sixteen"},
            {"Seventeen", "17", "17", "17.0", "Seventeen"},
            {"Eighteen", "18", "18", "18.0", "Eighteen"},
            {"Nineteen", "19", "19", "19.0", "Nineteen"},
            {"Twenty", "20", "20", "20.0", "Twenty"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        rs = st.executeQuery(
            " SELECT * FROM K55ADMIN.LJview2 ORDER BY smintcol");

        expColNames = new String [] {"CHARCOL", "SMINTCOL", "INTCOL", "DEC62COL", "FLOATCOL", "VCHARCOL"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"One", "1", null, null, null, null},
            {"Two", "2", null, "2.00", "2.0", null},
            {"Three", "3", null, null, null, null},
            {"Four", "4", null, "4.00", "4.0", null},
            {"Five", "5", null, null, null, null},
            {"Six", "6", null, "6.00", "6.0", null},
            {"Seven", "7", null, null, null, null},
            {"Eight", "8", null, "8.00", "8.0", null},
            {"Nine", "9", null, null, null, null},
            {"Ten", "10", null, "10.00", "10.0", null},
            {"Eleven", "11", "11", null, null, "Eleven"},
            {"Twelve", "12", "12", "12.00", "12.0", "Twelve"},
            {"Thirteen", "13", "13", null, null, "Thirteen"},
            {"Fourteen", "14", "14", "14.00", "14.0", "Fourteen"},
            {"Fifteen", "15", "15", null, null, "Fifteen"},
            {"Sixteen", "16", "16", "16.00", "16.0", "Sixteen"},
            {"Seventeen", "17", "17", null, null, "Seventeen"},
            {"Eighteen", "18", "18", "18.00", "18.0", "Eighteen"},
            {"Nineteen", "19", "19", null, null, "Nineteen"},
            {"Twenty", "20", "20", "20.00", "20.0", "Twenty"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        rs = st.executeQuery(
            " SELECT * FROM K55ADMIN.RJview1 ORDER BY smintcol");

        expColNames = new String [] {"CHARCOL", "SMINTCOL", "INTCOL", "FLOATCOL", "VCHARCOL"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"One", "1", null, null, null},
            {"Two", "2", null, null, null},
            {"Three", "3", null, null, null},
            {"Four", "4", null, null, null},
            {"Five", "5", null, null, null},
            {"Six", "6", null, null, null},
            {"Seven", "7", null, null, null},
            {"Eight", "8", null, null, null},
            {"Nine", "9", null, null, null},
            {"Ten", "10", null, null, null},
            {"Eleven", "11", "11", "11.0", "Eleven"},
            {"Twelve", "12", "12", "12.0", "Twelve"},
            {"Thirteen", "13", "13", "13.0", "Thirteen"},
            {"Fourteen", "14", "14", "14.0", "Fourteen"},
            {"Fifteen", "15", "15", "15.0", "Fifteen"},
            {"Sixteen", "16", "16", "16.0", "Sixteen"},
            {"Seventeen", "17", "17", "17.0", "Seventeen"},
            {"Eighteen", "18", "18", "18.0", "Eighteen"},
            {"Nineteen", "19", "19", "19.0", "Nineteen"},
            {"Twenty", "20", "20", "20.0", "Twenty"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        rs = st.executeQuery(
            " SELECT * FROM K55ADMIN.RJview2 ORDER BY floatcol");

        expColNames = new String [] {"CHARCOL", "SMINTCOL", "INTCOL", "DEC62COL", "FLOATCOL", "VCHARCOL"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {null, null, null, "2.00", "2.0", null},
            {null, null, null, "4.00", "4.0", null},
            {null, null, null, "6.00", "6.0", null},
            {null, null, null, "8.00", "8.0", null},
            {null, null, null, "10.00", "10.0", null},
            {"Twelve", "12", "12", "12.00", "12.0", "Twelve"},
            {"Fourteen", "14", "14", "14.00", "14.0", "Fourteen"},
            {"Sixteen", "16", "16", "16.00", "16.0", "Sixteen"},
            {"Eighteen", "18", "18", "18.00", "18.0", "Eighteen"},
            {"Twenty", "20", "20", "20.00", "20.0", "Twenty"},
            {null, null, null, "22.00", "22.0", null},
            {null, null, null, "24.00", "24.0", null}
        };

        JDBC.assertFullResultSet(rs, expRS);

        rs = st.executeQuery(
            " SELECT * FROM K55ADMIN.IJview1 ORDER BY intcol");

        expColNames = new String [] {"CHARCOL", "SMINTCOL", "INTCOL", "FLOATCOL", "VCHARCOL"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Eleven", "11", "11", "11.0", "Eleven"},
            {"Twelve", "12", "12", "12.0", "Twelve"},
            {"Thirteen", "13", "13", "13.0", "Thirteen"},
            {"Fourteen", "14", "14", "14.0", "Fourteen"},
            {"Fifteen", "15", "15", "15.0", "Fifteen"},
            {"Sixteen", "16", "16", "16.0", "Sixteen"},
            {"Seventeen", "17", "17", "17.0", "Seventeen"},
            {"Eighteen", "18", "18", "18.0", "Eighteen"},
            {"Nineteen", "19", "19", "19.0", "Nineteen"},
            {"Twenty", "20", "20", "20.0", "Twenty"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        //
        //------------------------------------------------------------
        // test unit 7. Joins on view;
        //------------------------------------------------------------

        // 701 - Select from LJ view and base table

        rs = st.executeQuery(
            "SELECT T1.smintcol, T2.smintcol, T1.intcol, T2.intcol"
            + "   FROM K55ADMIN.LJview2 T1, k55admin.manytypes_notnull T2"
            + "   WHERE T1.intcol IS NOT NULL"
            + "   ORDER BY 1, 2");

        expColNames = new String [] {"SMINTCOL", "SMINTCOL", "INTCOL", "INTCOL"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"11", "11", "11", "11"},
            {"11", "12", "11", "12"},
            {"11", "13", "11", "13"},
            {"11", "14", "11", "14"},
            {"11", "15", "11", "15"},
            {"11", "16", "11", "16"},
            {"11", "17", "11", "17"},
            {"11", "18", "11", "18"},
            {"11", "19", "11", "19"},
            {"11", "20", "11", "20"},
            {"11", "21", "11", "21"},
            {"11", "22", "11", "22"},
            {"11", "23", "11", "23"},
            {"11", "24", "11", "24"},
            {"11", "25", "11", "25"},
            {"12", "11", "12", "11"},
            {"12", "12", "12", "12"},
            {"12", "13", "12", "13"},
            {"12", "14", "12", "14"},
            {"12", "15", "12", "15"},
            {"12", "16", "12", "16"},
            {"12", "17", "12", "17"},
            {"12", "18", "12", "18"},
            {"12", "19", "12", "19"},
            {"12", "20", "12", "20"},
            {"12", "21", "12", "21"},
            {"12", "22", "12", "22"},
            {"12", "23", "12", "23"},
            {"12", "24", "12", "24"},
            {"12", "25", "12", "25"},
            {"13", "11", "13", "11"},
            {"13", "12", "13", "12"},
            {"13", "13", "13", "13"},
            {"13", "14", "13", "14"},
            {"13", "15", "13", "15"},
            {"13", "16", "13", "16"},
            {"13", "17", "13", "17"},
            {"13", "18", "13", "18"},
            {"13", "19", "13", "19"},
            {"13", "20", "13", "20"},
            {"13", "21", "13", "21"},
            {"13", "22", "13", "22"},
            {"13", "23", "13", "23"},
            {"13", "24", "13", "24"},
            {"13", "25", "13", "25"},
            {"14", "11", "14", "11"},
            {"14", "12", "14", "12"},
            {"14", "13", "14", "13"},
            {"14", "14", "14", "14"},
            {"14", "15", "14", "15"},
            {"14", "16", "14", "16"},
            {"14", "17", "14", "17"},
            {"14", "18", "14", "18"},
            {"14", "19", "14", "19"},
            {"14", "20", "14", "20"},
            {"14", "21", "14", "21"},
            {"14", "22", "14", "22"},
            {"14", "23", "14", "23"},
            {"14", "24", "14", "24"},
            {"14", "25", "14", "25"},
            {"15", "11", "15", "11"},
            {"15", "12", "15", "12"},
            {"15", "13", "15", "13"},
            {"15", "14", "15", "14"},
            {"15", "15", "15", "15"},
            {"15", "16", "15", "16"},
            {"15", "17", "15", "17"},
            {"15", "18", "15", "18"},
            {"15", "19", "15", "19"},
            {"15", "20", "15", "20"},
            {"15", "21", "15", "21"},
            {"15", "22", "15", "22"},
            {"15", "23", "15", "23"},
            {"15", "24", "15", "24"},
            {"15", "25", "15", "25"},
            {"16", "11", "16", "11"},
            {"16", "12", "16", "12"},
            {"16", "13", "16", "13"},
            {"16", "14", "16", "14"},
            {"16", "15", "16", "15"},
            {"16", "16", "16", "16"},
            {"16", "17", "16", "17"},
            {"16", "18", "16", "18"},
            {"16", "19", "16", "19"},
            {"16", "20", "16", "20"},
            {"16", "21", "16", "21"},
            {"16", "22", "16", "22"},
            {"16", "23", "16", "23"},
            {"16", "24", "16", "24"},
            {"16", "25", "16", "25"},
            {"17", "11", "17", "11"},
            {"17", "12", "17", "12"},
            {"17", "13", "17", "13"},
            {"17", "14", "17", "14"},
            {"17", "15", "17", "15"},
            {"17", "16", "17", "16"},
            {"17", "17", "17", "17"},
            {"17", "18", "17", "18"},
            {"17", "19", "17", "19"},
            {"17", "20", "17", "20"},
            {"17", "21", "17", "21"},
            {"17", "22", "17", "22"},
            {"17", "23", "17", "23"},
            {"17", "24", "17", "24"},
            {"17", "25", "17", "25"},
            {"18", "11", "18", "11"},
            {"18", "12", "18", "12"},
            {"18", "13", "18", "13"},
            {"18", "14", "18", "14"},
            {"18", "15", "18", "15"},
            {"18", "16", "18", "16"},
            {"18", "17", "18", "17"},
            {"18", "18", "18", "18"},
            {"18", "19", "18", "19"},
            {"18", "20", "18", "20"},
            {"18", "21", "18", "21"},
            {"18", "22", "18", "22"},
            {"18", "23", "18", "23"},
            {"18", "24", "18", "24"},
            {"18", "25", "18", "25"},
            {"19", "11", "19", "11"},
            {"19", "12", "19", "12"},
            {"19", "13", "19", "13"},
            {"19", "14", "19", "14"},
            {"19", "15", "19", "15"},
            {"19", "16", "19", "16"},
            {"19", "17", "19", "17"},
            {"19", "18", "19", "18"},
            {"19", "19", "19", "19"},
            {"19", "20", "19", "20"},
            {"19", "21", "19", "21"},
            {"19", "22", "19", "22"},
            {"19", "23", "19", "23"},
            {"19", "24", "19", "24"},
            {"19", "25", "19", "25"},
            {"20", "11", "20", "11"},
            {"20", "12", "20", "12"},
            {"20", "13", "20", "13"},
            {"20", "14", "20", "14"},
            {"20", "15", "20", "15"},
            {"20", "16", "20", "16"},
            {"20", "17", "20", "17"},
            {"20", "18", "20", "18"},
            {"20", "19", "20", "19"},
            {"20", "20", "20", "20"},
            {"20", "21", "20", "21"},
            {"20", "22", "20", "22"},
            {"20", "23", "20", "23"},
            {"20", "24", "20", "24"},
            {"20", "25", "20", "25"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 702 - LEFT JOIN a table with a LJ view, with "or 1=0"
        //       predicate

        rs = st.executeQuery(
            "SELECT T1.charcol, T2.charcol, T2.smintcol, T2.intcol"
            + "  FROM K55ADMIN.manytypes T1 LEFT JOIN K55ADMIN.LJview1 T2"
            + "    ON T1.charcol = T2.charcol or 1=0"
            + "  WHERE T2.smintcol BETWEEN 5 AND 15"
            + "  ORDER BY 3,4");

        expColNames = new String [] {"CHARCOL", "CHARCOL", "SMINTCOL", "INTCOL"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Five", "Five", "5", null},
            {"Six", "Six", "6", null},
            {"Seven", "Seven", "7", null},
            {"Eight", "Eight", "8", null},
            {"Nine", "Nine", "9", null},
            {"Ten", "Ten", "10", null},
            {"Eleven", "Eleven", "11", "11"},
            {"Twelve", "Twelve", "12", "12"},
            {"Thirteen", "Thirteen", "13", "13"},
            {"Fourteen", "Fourteen", "14", "14"},
            {"Fifteen", "Fifteen", "15", "15"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 703 - RIGHT JOIN K55ADMIN.a table with a LJ view, with
        //       "or 1=0" predicate

        rs = st.executeQuery(
            "SELECT T1.charcol, T2.charcol, T2.smintcol, T2.intcol"
            + "  FROM K55ADMIN.manytypes_ctrl T1 RIGHT JOIN "
            + "K55ADMIN.LJview1 T2"
            + "    ON T1.charcol = T2.charcol or 1=0"
            + "  WHERE T2.smintcol BETWEEN 5 AND 15 OR T2.smintcol IS NULL"
            + "  ORDER BY 3, 4, 1");

        expColNames = new String [] {"CHARCOL", "CHARCOL", "SMINTCOL", "INTCOL"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {null, "Five", "5", null},
            {"Six", "Six", "6", null},
            {null, "Seven", "7", null},
            {"Eight", "Eight", "8", null},
            {null, "Nine", "9", null},
            {"Ten", "Ten", "10", null},
            {null, "Eleven", "11", "11"},
            {"Twelve", "Twelve", "12", "12"},
            {null, "Thirteen", "13", "13"},
            {"Fourteen", "Fourteen", "14", "14"},
            {null, "Fifteen", "15", "15"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 704 - Apply Aggregate function on RJ view

        rs = st.executeQuery(
            "SELECT intcol, COUNT(*)"
            + "  FROM K55ADMIN.RJview1"
            + "  GROUP BY intcol"
            + "  ORDER BY intcol");

        expColNames = new String [] {"INTCOL", "2"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"11", "1"},
            {"12", "1"},
            {"13", "1"},
            {"14", "1"},
            {"15", "1"},
            {"16", "1"},
            {"17", "1"},
            {"18", "1"},
            {"19", "1"},
            {"20", "1"},
            {null, "10"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 705 - Apply Aggregate function on LJ view

        rs = st.executeQuery(
            "SELECT intcol, COUNT(*), COUNT(DISTINCT intcol)"
            + "  FROM K55ADMIN.LJview2"
            + "  GROUP BY intcol");

        expColNames = new String [] {"INTCOL", "2", "3"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"11", "1", "1"},
            {"12", "1", "1"},
            {"13", "1", "1"},
            {"14", "1", "1"},
            {"15", "1", "1"},
            {"16", "1", "1"},
            {"17", "1", "1"},
            {"18", "1", "1"},
            {"19", "1", "1"},
            {"20", "1", "1"},
            {null, "10", "0"}
        };

        wrnRS = new String []
        {
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            "01003",
        };

        JDBC.assertFullResultSet(rs, expRS, wrnRS);


        // 706 - RIGHT JOIN a LJ view with a table

        rs = st.executeQuery(
            "SELECT T1.smintcol, T2.smintcol, T1.intcol, T2.intcol"
            + "  FROM K55ADMIN.LJview2 T1 RIGHT JOIN "
            + "K55ADMIN.manytypes_notnull T2"
            + "    ON T1.smintcol = T2.smintcol"
            + "  WHERE T1.intcol IS NOT NULL"
            + "  ORDER BY 1, 3");

        expColNames = new String [] {"SMINTCOL", "SMINTCOL", "INTCOL", "INTCOL"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"11", "11", "11", "11"},
            {"12", "12", "12", "12"},
            {"13", "13", "13", "13"},
            {"14", "14", "14", "14"},
            {"15", "15", "15", "15"},
            {"16", "16", "16", "16"},
            {"17", "17", "17", "17"},
            {"18", "18", "18", "18"},
            {"19", "19", "19", "19"},
            {"20", "20", "20", "20"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 707 - RIGHT join a RJ view with a table

        rs = st.executeQuery(
            "SELECT T1.charcol, T2.charcol, T2.smintcol, T2.intcol"
            + "  FROM K55ADMIN.RJview1 T2 RIGHT JOIN K55ADMIN.manytypes T1"
            + "    ON T1.charcol = T2.charcol"
            + "  WHERE T2.smintcol BETWEEN 5 AND 15"
            + "  ORDER BY 3, 4");

        expColNames = new String [] {"CHARCOL", "CHARCOL", "SMINTCOL", "INTCOL"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Five", "Five", "5", null},
            {"Six", "Six", "6", null},
            {"Seven", "Seven", "7", null},
            {"Eight", "Eight", "8", null},
            {"Nine", "Nine", "9", null},
            {"Ten", "Ten", "10", null},
            {"Eleven", "Eleven", "11", "11"},
            {"Twelve", "Twelve", "12", "12"},
            {"Thirteen", "Thirteen", "13", "13"},
            {"Fourteen", "Fourteen", "14", "14"},
            {"Fifteen", "Fifteen", "15", "15"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 708 - LEFT join a table with a RJ view

        rs = st.executeQuery(
            "SELECT T1.charcol, T2.charcol, T2.smintcol, T2.intcol"
            + "  FROM K55ADMIN.manytypes_ctrl T1 LEFT JOIN "
            + "K55ADMIN.RJview1 T2"
            + "    ON T1.charcol = T2.charcol"
            + "  WHERE T2.smintcol BETWEEN 5 AND 15 OR T2.smintcol IS NULL"
            + "  ORDER BY 3, 4, 1");

        expColNames = new String [] {"CHARCOL", "CHARCOL", "SMINTCOL", "INTCOL"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Six", "Six", "6", null},
            {"Eight", "Eight", "8", null},
            {"Ten", "Ten", "10", null},
            {"Twelve", "Twelve", "12", "12"},
            {"Fourteen", "Fourteen", "14", "14"},
            {"Twenty Four", null, null, null},
            {"Twenty Two", null, null, null}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 709 - Apply Aggregate function on RJ view with GROUP BY
        // clause

        rs = st.executeQuery(
            "SELECT intcol, COUNT(*),"
            + "       MAX(intcol),"
            + "       MIN(intcol),"            + "       SUM(intcol),"
            + "       AVG(intcol)"
            + "  FROM K55ADMIN.RJview1"
            + "  GROUP BY intcol"
            + "  ORDER BY intcol");

        expColNames = new String [] {"INTCOL", "2", "3", "4", "5", "6"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"11", "1", "11", "11", "11", "11"},
            {"12", "1", "12", "12", "12", "12"},
            {"13", "1", "13", "13", "13", "13"},
            {"14", "1", "14", "14", "14", "14"},
            {"15", "1", "15", "15", "15", "15"},
            {"16", "1", "16", "16", "16", "16"},
            {"17", "1", "17", "17", "17", "17"},
            {"18", "1", "18", "18", "18", "18"},
            {"19", "1", "19", "19", "19", "19"},
            {"20", "1", "20", "20", "20", "20"},
            {null, "10", null, null, null, null}
        };

        wrnRS = new String []
        {
            "01003",
            "01003",
            "01003",
            "01003",
            "01003",
            "01003",
            "01003",
            "01003",
            "01003",
            "01003",
            "01003",
        };

        JDBC.assertFullResultSet(rs, expRS, wrnRS);


        // 710 - Apply Aggregate function on RJ view with WHERE
        // clause

        rs = st.executeQuery(
            "SELECT intcol, COUNT(*), COUNT(DISTINCT intcol)"
            + "  FROM K55ADMIN.RJview1"
            + "  WHERE (smintcol / 2) * 2 = smintcol"
            + "  GROUP BY intcol"
            + "  ORDER BY intcol");

        expColNames = new String [] {"INTCOL", "2", "3"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"12", "1", "1"},
            {"14", "1", "1"},
            {"16", "1", "1"},
            {"18", "1", "1"},
            {"20", "1", "1"},
            {null, "5", "0"}
        };


        wrnRS = new String []
        {
            "01003",
            "01003",
            "01003",
            "01003",
            "01003",
            "01003",
        };

        JDBC.assertFullResultSet(rs, expRS, wrnRS);


        // 711 - LEFT join a RJ view with a LJ view - join on
        // multiple columns

        rs = st.executeQuery(
            "SELECT T1.smintcol, T1.charcol, substr(T1.vcharcol,1,15),"
            + "       T2.smintcol, T2.charcol, substr(T2.vcharcol,1,15)"
            + "  FROM K55ADMIN.RJview2 T1 LEFT JOIN K55ADMIN.LJview2 T2"
            + "    ON T1.smintcol = T2.smintcol AND T1.charcol = T2.charcol"
            + "  WHERE LENGTH(T1.vcharcol) > 6"
            + "  ORDER BY 1, 4");

        expColNames = new String [] {"SMINTCOL", "CHARCOL", "3", "SMINTCOL", "CHARCOL", "6"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"14", "Fourteen", "Fourteen", "14", "Fourteen", "Fourteen"},
            {"16", "Sixteen", "Sixteen", "16", "Sixteen", "Sixteen"},
            {"18", "Eighteen", "Eighteen", "18", "Eighteen", "Eighteen"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 712 - RIGHT join a RJ view with a LJ view - join on
        // multiple columns

        rs = st.executeQuery(
            "SELECT T1.smintcol, T1.charcol, substr(T1.vcharcol,1,15),"
            + "       T2.smintcol, T2.charcol, substr(T2.vcharcol,1,15)"
            + "  FROM K55ADMIN.RJview2 T1 RIGHT JOIN K55ADMIN.LJview2 T2"
            + "    ON T1.smintcol = T2.smintcol AND T1.charcol = T2.charcol"
            + "  WHERE LENGTH(T1.vcharcol) = 6"
            + "  ORDER BY 1, 4");

        expColNames = new String [] {"SMINTCOL", "CHARCOL", "3", "SMINTCOL", "CHARCOL", "6"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"12", "Twelve", "Twelve", "12", "Twelve", "Twelve"},
            {"20", "Twenty", "Twenty", "20", "Twenty", "Twenty"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 713 - LEFT join a RJ view with a RJ view - join on
        // multiple columns

        rs = st.executeQuery(
            "SELECT T1.smintcol, T1.charcol, substr(T1.vcharcol,1,15),"
            + "       T2.smintcol, T2.charcol, substr(T2.vcharcol,1,15)"
            + "  FROM K55ADMIN.RJview2 T1 LEFT JOIN K55ADMIN.RJview2 T2"
            + "    ON T1.smintcol = T2.smintcol AND T1.charcol = T2.charcol"
            + "  WHERE LENGTH(T1.vcharcol) >= 6"
            + "  ORDER BY 1, 4");

        expColNames = new String [] {"SMINTCOL", "CHARCOL", "3", "SMINTCOL", "CHARCOL", "6"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"12", "Twelve", "Twelve", "12", "Twelve", "Twelve"},
            {"14", "Fourteen", "Fourteen", "14", "Fourteen", "Fourteen"},
            {"16", "Sixteen", "Sixteen", "16", "Sixteen", "Sixteen"},
            {"18", "Eighteen", "Eighteen", "18", "Eighteen", "Eighteen"},
            {"20", "Twenty", "Twenty", "20", "Twenty", "Twenty"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 714 - RIGHT join an IJ view with a RJ view  - join on
        // multiple columns

        rs = st.executeQuery(
            "SELECT T1.smintcol, T1.charcol, substr(T1.vcharcol,1,15),"
            + "       T2.smintcol, T2.charcol, substr(T2.vcharcol,1,15)"
            + "  FROM K55ADMIN.IJview1 T1 RIGHT JOIN K55ADMIN.RJview2 T2"
            + "    ON T1.smintcol = T2.smintcol AND T1.charcol = T2.charcol"
            + "  WHERE LENGTH(T1.vcharcol) > 6"
            + "  ORDER BY 1, 4");

        expColNames = new String [] {"SMINTCOL", "CHARCOL", "3", "SMINTCOL", "CHARCOL", "6"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"14", "Fourteen", "Fourteen", "14", "Fourteen", "Fourteen"},
            {"16", "Sixteen", "Sixteen", "16", "Sixteen", "Sixteen"},
            {"18", "Eighteen", "Eighteen", "18", "Eighteen", "Eighteen"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 715 - INNER join a RJ view with a LJ view - join on
        // multiple columns

        rs = st.executeQuery(
            "SELECT T1.smintcol, T1.charcol, substr(T1.vcharcol,1,15),"
            + "       T2.smintcol, T2.charcol, substr(T2.vcharcol,1,15)"
            + "  FROM K55ADMIN.RJview2 T1 INNER JOIN K55ADMIN.LJview2 T2"
            + "    ON T1.smintcol = T2.smintcol AND T1.charcol = T2.charcol"
            + "  WHERE LENGTH(T1.vcharcol) >= 6"
            + "  ORDER BY 1, 4");

        expColNames = new String [] {"SMINTCOL", "CHARCOL", "3", "SMINTCOL", "CHARCOL", "6"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"12", "Twelve", "Twelve", "12", "Twelve", "Twelve"},
            {"14", "Fourteen", "Fourteen", "14", "Fourteen", "Fourteen"},
            {"16", "Sixteen", "Sixteen", "16", "Sixteen", "Sixteen"},
            {"18", "Eighteen", "Eighteen", "18", "Eighteen", "Eighteen"},
            {"20", "Twenty", "Twenty", "20", "Twenty", "Twenty"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 716 - INNER join an IJ view with a RJ view - join on
        // multiple columns

        rs = st.executeQuery(
            "SELECT T1.smintcol, T1.charcol, substr(T1.vcharcol,1,15),"
            + "       T2.smintcol, T2.charcol, substr(T2.vcharcol,1,15)"
            + "  FROM K55ADMIN.IJview1 T1 INNER JOIN K55ADMIN.RJview2 T2"
            + "    ON T1.smintcol = T2.smintcol AND T1.charcol = T2.charcol"
            + "  WHERE LENGTH(T1.vcharcol) >= 6"
            + "  ORDER BY 1, 4");

        expColNames = new String [] {"SMINTCOL", "CHARCOL", "3", "SMINTCOL", "CHARCOL", "6"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"12", "Twelve", "Twelve", "12", "Twelve", "Twelve"},
            {"14", "Fourteen", "Fourteen", "14", "Fourteen", "Fourteen"},
            {"16", "Sixteen", "Sixteen", "16", "Sixteen", "Sixteen"},
            {"18", "Eighteen", "Eighteen", "18", "Eighteen", "Eighteen"},
            {"20", "Twenty", "Twenty", "20", "Twenty", "Twenty"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 722 - Apply Aggregate function on RJ view with GROUP BY
        // clause

        rs = st.executeQuery(
            "SELECT intcol, COUNT(*),"
            + "       MAX(intcol),"
            + "       MIN(intcol),"
            + "       SUM(intcol),"
            + "       AVG(intcol)"
            + "  FROM K55ADMIN.RJview1"
            + "  GROUP BY intcol"
            + "  ORDER BY intcol");

        expColNames = new String [] {"INTCOL", "2", "3", "4", "5", "6"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"11", "1", "11", "11", "11", "11"},
            {"12", "1", "12", "12", "12", "12"},
            {"13", "1", "13", "13", "13", "13"},
            {"14", "1", "14", "14", "14", "14"},
            {"15", "1", "15", "15", "15", "15"},
            {"16", "1", "16", "16", "16", "16"},
            {"17", "1", "17", "17", "17", "17"},
            {"18", "1", "18", "18", "18", "18"},
            {"19", "1", "19", "19", "19", "19"},
            {"20", "1", "20", "20", "20", "20"},
            {null, "10", null, null, null, null}
        };

        wrnRS = new String []
        {
            "01003",
            "01003",
            "01003",
            "01003",
            "01003",
            "01003",
            "01003",
            "01003",
            "01003",
            "01003",
            "01003",
        };

        JDBC.assertFullResultSet(rs, expRS, wrnRS);


        // 724 - Apply Aggregate function on RJ view with WHERE
        // clause

        rs = st.executeQuery(
            "SELECT intcol, COUNT(*), COUNT(DISTINCT intcol)"
            + "  FROM K55ADMIN.RJview1"
            + "  WHERE (smintcol / 2) * 2 = smintcol"
            + "  GROUP BY intcol"
            + "  ORDER BY intcol");

        expColNames = new String [] {"INTCOL", "2", "3"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"12", "1", "1"},
            {"14", "1", "1"},
            {"16", "1", "1"},
            {"18", "1", "1"},
            {"20", "1", "1"},
            {null, "5", "0"}
        };

        wrnRS = new String []
        {
            "01003",
            "01003",
            "01003",
            "01003",
            "01003",
            "01003",
        };

        JDBC.assertFullResultSet(rs, expRS, wrnRS);


        // 804 - LEFT JOIN a table with a LJ view, with "or 1=0"
        // predicate

        rs = st.executeQuery(
            "SELECT intcol, charcol FROM K55ADMIN.manytypes_ctrl"
            + "  WHERE charcol in ("
            + "    SELECT T1.charcol"
            + "      FROM K55ADMIN.manytypes T1 LEFT JOIN "
            + "K55ADMIN.LJview1 T2"
            + "        ON T1.charcol = T2.charcol or 1=0"
            + "      WHERE T2.smintcol BETWEEN 5 AND 15)"
            + "  ORDER BY 1");

        expColNames = new String [] {"INTCOL", "CHARCOL"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"6", "Six"},
            {"8", "Eight"},
            {"10", "Ten"},
            {"12", "Twelve"},
            {"14", "Fourteen"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 805 - RIGHT JOIN a LJ view with a table, with "or 1=0"
        // predicate

        rs = st.executeQuery(
            "SELECT intcol, charcol FROM K55ADMIN.manytypes_ctrl"
            + "  WHERE smintcol in ("
            + "    SELECT T1.smintcol"
            + "      FROM K55ADMIN.LJview2 T1 RIGHT JOIN "
            + "K55ADMIN.manytypes_notnull T2"
            + "        ON T1.smintcol = T2.smintcol or 1=0"
            + "      WHERE T1.intcol IS NOT NULL)"
            + "  ORDER BY 1");

        expColNames = new String [] {"INTCOL", "CHARCOL"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"12", "Twelve"},
            {"14", "Fourteen"},
            {"16", "Sixteen"},
            {"18", "Eighteen"},
            {"20", "Twenty"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 806 - LEFT join a RJ view with a LJ view - join on
        // multiple columns, with "or 1=0" predicate

        rs = st.executeQuery(
            "SELECT intcol, charcol FROM K55ADMIN.manytypes_ctrl"
            + "  WHERE smintcol > SOME ("
            + "    SELECT T1.smintcol"
            + "      FROM K55ADMIN.RJview2 T1 LEFT JOIN K55ADMIN.LJview2 T2"
            + "        ON T1.smintcol = T2.smintcol AND T1.charcol "
            + "= T2.charcol or 1=0"
            + "      WHERE LENGTH(T1.vcharcol) > 6)"
            + "  ORDER BY 1");

        expColNames = new String [] {"INTCOL", "CHARCOL"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"16", "Sixteen"},
            {"18", "Eighteen"},
            {"20", "Twenty"},
            {"22", "Twenty Two"},
            {"24", "Twenty Four"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 807 - RIGHT join a RJ view with a LJ view - join on
        // multiple columns, with "or 1=0" predicate

        rs = st.executeQuery(
            "SELECT intcol, charcol FROM K55ADMIN.manytypes_ctrl"
            + "  WHERE smintcol > ANY ("
            + "    SELECT T1.smintcol"
            + "      FROM K55ADMIN.RJview2 T1 RIGHT JOIN "
            + "K55ADMIN.LJview2 T2"
            + "        ON T1.smintcol = T2.smintcol AND T1.charcol "
            + "= T2.charcol or 1=0"
            + "      WHERE LENGTH(T1.vcharcol) = 6)"
            + "  ORDER BY 1");

        expColNames = new String [] {"INTCOL", "CHARCOL"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"14", "Fourteen"},
            {"16", "Sixteen"},
            {"18", "Eighteen"},
            {"20", "Twenty"},
            {"22", "Twenty Two"},
            {"24", "Twenty Four"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 808 - RIGHT join an IJ view with a RJ view - join on
        // multiple columns, with "or 1=0" predicate

        rs = st.executeQuery(
            "SELECT intcol, charcol FROM K55ADMIN.manytypes_ctrl"
            + "  WHERE charcol in ("
            + "    SELECT T1.charcol"
            + "      FROM K55ADMIN.IJview1 T1 RIGHT JOIN "
            + "K55ADMIN.RJview2 T2"
            + "        ON T1.smintcol = T2.smintcol AND T1.charcol "
            + "= T2.charcol or 1=0"
            + "      WHERE LENGTH(T1.vcharcol) > 6)"
            + "  ORDER BY 1");

        expColNames = new String [] {"INTCOL", "CHARCOL"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"14", "Fourteen"},
            {"16", "Sixteen"},
            {"18", "Eighteen"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 809 - INNER join a RJ view with a LJ view - join on
        // multiple columns, with "or 1=0" predicate

        rs = st.executeQuery(
            "SELECT intcol, charcol FROM K55ADMIN.manytypes_ctrl"
            + "  WHERE smintcol < ALL ("
            + "    SELECT T1.smintcol"
            + "      FROM K55ADMIN.RJview2 T1 INNER JOIN "
            + "K55ADMIN.LJview2 T2"
            + "        ON T1.smintcol = T2.smintcol AND T1.charcol "
            + "= T2.charcol or 1=0"
            + "      WHERE LENGTH(T1.vcharcol) >= 6)"
            + "  ORDER BY 1");

        expColNames = new String [] {"INTCOL", "CHARCOL"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"2", "Two"},
            {"4", "Four"},
            {"6", "Six"},
            {"8", "Eight"},
            {"10", "Ten"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 810 - INNER join an IJ view with a RJ view - join on
        // multiple columns, with "or 1=0" predicate

        rs = st.executeQuery(
            "SELECT intcol, charcol FROM K55ADMIN.manytypes_ctrl"
            + "  WHERE smintcol > ALL ("
            + "    SELECT T1.smintcol"
            + "      FROM K55ADMIN.IJview1 T1 INNER JOIN "
            + "K55ADMIN.RJview2 T2"
            + "        ON T1.smintcol = T2.smintcol AND T1.charcol "
            + "= T2.charcol or 1=0"
            + "      WHERE LENGTH(T1.vcharcol) >= 6)"
            + "  ORDER BY 1");

        expColNames = new String [] {"INTCOL", "CHARCOL"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"22", "Twenty Two"},
            {"24", "Twenty Four"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 815 - INNER join an IJ view with a RJ view - join on
        // multiple columns, with "or 1=0" predicate

        rs = st.executeQuery(
            "SELECT intcol, charcol FROM K55ADMIN.manytypes_ctrl"
            + "  WHERE smintcol = ANY ("
            + "    SELECT T1.smintcol"
            + "      FROM K55ADMIN.IJview1 T1 LEFT JOIN K55ADMIN.RJview2 T2"
            + "        ON T1.smintcol = T2.smintcol AND T1.charcol "
            + "= T2.charcol or 1=0"
            + "      WHERE LENGTH(T1.vcharcol) IN (4, 6, 8) )"
            + "  ORDER BY 1");

        expColNames = new String [] {"INTCOL", "CHARCOL"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"12", "Twelve"},
            {"14", "Fourteen"},
            {"18", "Eighteen"},
            {"20", "Twenty"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 817 - LEFT join 2 views - with 1-level correlated
        // subquery, with "or 1=0" predicate

        rs = st.executeQuery(
            "SELECT intcol, charcol FROM K55ADMIN.manytypes_ctrl T1"
            + "  WHERE smintcol = ("
            + "    SELECT T2.smintcol + 2"
            + "      FROM K55ADMIN.IJview1 T2 LEFT JOIN K55ADMIN.LJview2 T3"
            + "             ON T2.smintcol = T3.floatcol AND "
            + "T2.charcol = T3.charcol or 1=0"
            + "      WHERE T2.floatcol = T1.floatcol - 2 )"
            + "  ORDER BY 1");

        expColNames = new String [] {"INTCOL", "CHARCOL"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"14", "Fourteen"},
            {"16", "Sixteen"},
            {"18", "Eighteen"},
            {"20", "Twenty"},
            {"22", "Twenty Two"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        //
        //------------------------------------------------------------
        // test unit 9. Joins tables used in HAVING
        // clause;
        //------------------------------------------------------------

        // 904 - LEFT JOIN a table with a LJ view,
        // with "or 1=0" predicate

        rs = st.executeQuery(
            "SELECT intcol, charcol FROM K55ADMIN.manytypes_ctrl"
            + "  GROUP BY intcol, charcol"
            + "  HAVING charcol in ("
            + "    SELECT T1.charcol"
            + "      FROM K55ADMIN.manytypes T1 LEFT JOIN "
            + "K55ADMIN.LJview1 T2"
            + "        ON T1.charcol = T2.charcol or 1=0"
            + "      WHERE T2.smintcol BETWEEN 5 AND 15)"
            + "  ORDER BY 1");

        expColNames = new String [] {"INTCOL", "CHARCOL"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"6", "Six"},
            {"8", "Eight"},
            {"10", "Ten"},
            {"12", "Twelve"},
            {"14", "Fourteen"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 905 - RIGHT JOIN a LJ view with a table, with "or 1=0"
        // predicate

        rs = st.executeQuery(
            "SELECT smintcol, charcol FROM K55ADMIN.manytypes_ctrl"
            + "  GROUP BY smintcol, charcol"
            + "  HAVING smintcol in ("
            + "    SELECT T1.smintcol"
            + "      FROM K55ADMIN.LJview2 T1 RIGHT JOIN "
            + "K55ADMIN.manytypes_notnull T2"
            + "        ON T1.smintcol = T2.smintcol or 1=0"
            + "      WHERE T1.intcol IS NOT NULL)"
            + "  ORDER BY 1");

        expColNames = new String [] {"SMINTCOL", "CHARCOL"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"12", "Twelve"},
            {"14", "Fourteen"},
            {"16", "Sixteen"},
            {"18", "Eighteen"},
            {"20", "Twenty"}
        };

        JDBC.assertFullResultSet(rs, expRS);
    }

    public void part_v(Statement st) throws SQLException
    {
        ResultSet rs = null;

        String [][] expRS;
        String [] expColNames;
        String [] wrnRS;

        // 906 - LEFT join a RJ view with a LJ view - join on
        // multiple columns, with "or 1=0" predicate

        rs = st.executeQuery(
            "SELECT smintcol, charcol FROM K55ADMIN.manytypes_ctrl"
            + "  GROUP BY smintcol, charcol"
            + "  HAVING smintcol > SOME ("
            + "    SELECT T1.smintcol"
            + "      FROM K55ADMIN.RJview2 T1 LEFT JOIN K55ADMIN.LJview2 T2"
            + "        ON T1.smintcol = T2.smintcol AND T1.charcol "
            + "= T2.charcol or 1=0"
            + "      WHERE LENGTH(T1.vcharcol) > 6)"
            + "  ORDER BY 1");

        expColNames = new String [] {"SMINTCOL", "CHARCOL"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"16", "Sixteen"},
            {"18", "Eighteen"},
            {"20", "Twenty"},
            {"22", "Twenty Two"},
            {"24", "Twenty Four"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 907 - RIGHT join a RJ view with a LJ view - join on
        // multiple columns, with "or 1=0" predicate

        rs = st.executeQuery(
            "SELECT smintcol, charcol FROM K55ADMIN.manytypes_ctrl"
            + "  GROUP BY smintcol, charcol"
            + "  HAVING smintcol > ANY ("
            + "    SELECT T1.smintcol"
            + "      FROM K55ADMIN.RJview2 T1 RIGHT JOIN "
            + "K55ADMIN.LJview2 T2"
            + "        ON T1.smintcol = T2.smintcol AND T1.charcol "
            + "= T2.charcol or 1=0"
            + "      WHERE LENGTH(T1.vcharcol) = 6)"
            + "  ORDER BY 1");

        expColNames = new String [] {"SMINTCOL", "CHARCOL"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"14", "Fourteen"},
            {"16", "Sixteen"},
            {"18", "Eighteen"},
            {"20", "Twenty"},
            {"22", "Twenty Two"},
            {"24", "Twenty Four"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 908 - RIGHT join an IJ view with a RJ view -join on
        // multiple columns, with "or 1=0" predicate

        rs = st.executeQuery(
            "SELECT intcol, charcol FROM K55ADMIN.manytypes_ctrl"
            + "  GROUP BY intcol, charcol"
            + "  HAVING charcol in ("
            + "    SELECT T1.charcol"
            + "      FROM K55ADMIN.IJview1 T1 RIGHT JOIN "
            + "K55ADMIN.RJview2 T2"
            + "        ON T1.smintcol = T2.smintcol AND T1.charcol "
            + "= T2.charcol or 1=0"
            + "      WHERE LENGTH(T1.vcharcol) > 6)"
            + "  ORDER BY 1");

        expColNames = new String [] {"INTCOL", "CHARCOL"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"14", "Fourteen"},
            {"16", "Sixteen"},
            {"18", "Eighteen"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 909 - INNER join a RJ view with a LJ view - join on
        // multiple columns, with "or 1=0" predicate

        rs = st.executeQuery(
            "SELECT smintcol, charcol FROM K55ADMIN.manytypes_ctrl"
            + "  GROUP BY smintcol, charcol"
            + "  HAVING smintcol < ALL ("
            + "    SELECT T1.smintcol"
            + "      FROM K55ADMIN.RJview2 T1 INNER JOIN "
            + "K55ADMIN.LJview2 T2"
            + "        ON T1.smintcol = T2.smintcol AND T1.charcol "
            + "= T2.charcol or 1=0"
            + "      WHERE LENGTH(T1.vcharcol) >= 6)"
            + "  ORDER BY 1");

        expColNames = new String [] {"SMINTCOL", "CHARCOL"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"2", "Two"},
            {"4", "Four"},
            {"6", "Six"},
            {"8", "Eight"},
            {"10", "Ten"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 910 - INNER join an IJ view with a RJ view - join on
        // multiple columns, with "or 1=0" predicate

        rs = st.executeQuery(
            "SELECT smintcol, charcol FROM K55ADMIN.manytypes_ctrl"
            + "  GROUP BY smintcol, charcol"
            + "  HAVING smintcol > ALL ("
            + "    SELECT T1.smintcol"
            + "      FROM K55ADMIN.IJview1 T1 INNER JOIN "
            + "K55ADMIN.RJview2 T2"
            + "        ON T1.smintcol = T2.smintcol AND T1.charcol "
            + "= T2.charcol or 1=0"
            + "      WHERE LENGTH(T1.vcharcol) >= 6)"
            + "  ORDER BY 1");

        expColNames = new String [] {"SMINTCOL", "CHARCOL"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"22", "Twenty Two"},
            {"24", "Twenty Four"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 915 - LEFT join 2 views - with 1-level correlated
        // subquery, with "or 1=0" predicate

        rs = st.executeQuery(
            "SELECT smintcol, charcol FROM K55ADMIN.manytypes_ctrl T1"
            + "  GROUP BY smintcol, charcol"
            + "  HAVING smintcol = ("
            + "    SELECT T2.smintcol - 1"
            + "      FROM K55ADMIN.IJview1 T2 LEFT JOIN K55ADMIN.LJview2 T3"
            + "             ON T2.smintcol = T3.floatcol AND "
            + "T2.charcol = T3.charcol or 1=0"
            + "      WHERE T2.floatcol = T1.smintcol + 1 )"
            + "  ORDER BY 1");

        expColNames = new String [] {"SMINTCOL", "CHARCOL"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"10", "Ten"},
            {"12", "Twelve"},
            {"14", "Fourteen"},
            {"16", "Sixteen"},
            {"18", "Eighteen"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 919 - LEFT join 2 views - with 2-level correlated
        // subquery, with "or 1=0" predicate

        rs = st.executeQuery(
            "SELECT smintcol, dec62col, charcol FROM "
            + "K55ADMIN.manytypes_ctrl T1"
            + "  GROUP BY smintcol, dec62col, charcol"
            + "  HAVING smintcol = ("
            + "    SELECT T2.smintcol "
            + "      FROM K55ADMIN.IJview1 T2 INNER JOIN "
            + "K55ADMIN.LJview2 T3"
            + "             ON T2.smintcol = T3.floatcol AND "
            + "T2.charcol = T3.charcol or 1=0"
            + "      WHERE T3.charcol = T1.charcol AND"
            + "            T2.floatcol = ("
            + "              SELECT T4.smintcol"
            + "                FROM K55ADMIN.LJview2 T4 LEFT JOIN "
            + "K55ADMIN.RJview1 T5"
            + "                       ON T4.dec62col = T5.floatcol or 1=0"
            + "                WHERE T5.floatcol = T1.dec62col ) )"
            + "  ORDER BY 1");

        expColNames = new String [] {"SMINTCOL", "DEC62COL", "CHARCOL"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"12", "12.00", "Twelve"},
            {"14", "14.00", "Fourteen"},
            {"16", "16.00", "Sixteen"},
            {"18", "18.00", "Eighteen"},
            {"20", "20.00", "Twenty"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        //
        //------------------------------------------------------------
        // test unit 10. Joins tables used in INSERT,
        // UPDATE or DELETE subquery;
        //------------------------------------------------------------

        // 1001 - Create table needed for the test

        st.executeUpdate(
            "CREATE TABLE k55admin.iud_tbl (c1 int,"
            + "                      c2 char(15),"
            + "                      c3 char(15))");

        // 1011 - LEFT join a RJ view with a LJ view - join on
        // multiple columns, with "or 1=0" predicate

        st.executeUpdate(
            "INSERT INTO k55admin.iud_tbl"
            + "    SELECT t1.intcol, t1.charcol, t2.vcharcol"
            + "      FROM K55ADMIN.RJview2 T1 LEFT JOIN K55ADMIN.LJview2 T2"
            + "        ON T1.smintcol = T2.smintcol AND T1.charcol "
            + "= T2.charcol or 1=0"
            + "      WHERE LENGTH(T1.vcharcol) > 6");

        rs = st.executeQuery(
            " SELECT * FROM K55ADMIN.iud_tbl ORDER BY c1");

        expColNames = new String [] {"C1", "C2", "C3"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"14", "Fourteen", "Fourteen"},
            {"16", "Sixteen", "Sixteen"},
            {"18", "Eighteen", "Eighteen"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 1012 - UPDATE rows inserted in previous statement, with
        // "or 1=0" predicate

        assertUpdateCount(st, 2,
            "UPDATE k55admin.iud_tbl"
            + "  SET c2 = NULL"
            + "  WHERE C1 < ANY ("
            + "    SELECT T1.smintcol"
            + "      FROM K55ADMIN.RJview2 T1 LEFT JOIN K55ADMIN.LJview2 T2"
            + "        ON T1.smintcol = T2.smintcol AND T1.charcol "
            + "= T2.charcol or 1=0"
            + "      WHERE LENGTH(T1.vcharcol) > 6)");

        rs = st.executeQuery(
            " SELECT * FROM K55ADMIN.iud_tbl ORDER BY c1");

        expColNames = new String [] {"C1", "C2", "C3"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"14", null, "Fourteen"},
            {"16", null, "Sixteen"},
            {"18", "Eighteen", "Eighteen"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 1013 - DELETE rows inserted in previous statement, with
        // "or 1=0" predicate

        assertUpdateCount(st, 3,
            "DELETE FROM K55ADMIN.iud_tbl"
            + "  WHERE c1 = ANY ("
            + "    SELECT T1.smintcol"
            + "      FROM K55ADMIN.RJview2 T1 LEFT JOIN K55ADMIN.LJview2 T2"
            + "        ON T1.smintcol = T2.smintcol AND T1.charcol "
            + "= T2.charcol or 1=0"
            + "      WHERE LENGTH(T1.vcharcol) > 6)");

        rs = st.executeQuery(
            " SELECT * FROM K55ADMIN.iud_tbl ORDER BY c1");

        expColNames = new String [] {"C1", "C2", "C3"};
        JDBC.assertColumnNames(rs, expColNames);
        JDBC.assertDrainResults(rs, 0);

        // 1014 - RIGHT join an IJ view with a RJ view - join on
        // multiple columns, with "or 1=0" predicate

        st.executeUpdate(
            "INSERT INTO k55admin.iud_tbl"
            + "    SELECT t1.intcol, t1.charcol, t2.vcharcol"
            + "      FROM K55ADMIN.IJview1 T1 RIGHT JOIN "
            + "K55ADMIN.RJview2 T2"
            + "        ON T1.smintcol = T2.smintcol AND T1.charcol "
            + "= T2.charcol or 1=0"
            + "      WHERE LENGTH(T1.charcol) > 6");

        rs = st.executeQuery(
            " SELECT * FROM K55ADMIN.iud_tbl ORDER BY c1");

        expColNames = new String [] {"C1", "C2", "C3"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"12", "Twelve", "Twelve"},
            {"14", "Fourteen", "Fourteen"},
            {"16", "Sixteen", "Sixteen"},
            {"18", "Eighteen", "Eighteen"},
            {"20", "Twenty", "Twenty"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 1015 - UPDATE rows inserted in previous statement, with
        // "or 1=0" predicate

        assertUpdateCount(st, 3,
            "UPDATE k55admin.iud_tbl"
            + "  SET c2 = NULL"
            + "  WHERE C1 < ANY ("
            + "    SELECT T1.intcol"
            + "      FROM K55ADMIN.IJview1 T1 RIGHT JOIN "
            + "K55ADMIN.RJview2 T2"
            + "        ON T1.smintcol = T2.smintcol AND T1.charcol "
            + "= T2.charcol or 1=0"
            + "      WHERE LENGTH(T1.vcharcol) > 6)");

        rs = st.executeQuery(
            " SELECT * FROM K55ADMIN.iud_tbl ORDER BY c1");

        expColNames = new String [] {"C1", "C2", "C3"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"12", null, "Twelve"},
            {"14", null, "Fourteen"},
            {"16", null, "Sixteen"},
            {"18", "Eighteen", "Eighteen"},
            {"20", "Twenty", "Twenty"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 1016 - DELETE rows inserted in previous statement, with
        // "or 1=0" predicate

        assertUpdateCount(st, 3,
            "DELETE FROM K55ADMIN.iud_tbl"
            + "  WHERE c1 = ANY ("
            + "    SELECT T1.intcol"
            + "      FROM K55ADMIN.IJview1 T1 RIGHT JOIN "
            + "K55ADMIN.RJview2 T2"
            + "        ON T1.smintcol = T2.smintcol AND T1.charcol "
            + "= T2.charcol or 1=0"
            + "      WHERE LENGTH(T1.vcharcol) > 6)");

        rs = st.executeQuery(
            " SELECT * FROM K55ADMIN.iud_tbl ORDER BY c1");

        expColNames = new String [] {"C1", "C2", "C3"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"12", null, "Twelve"},
            {"20", "Twenty", "Twenty"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 1017 - Clean up table iud_tbl

        assertUpdateCount(st, 2,
            "DELETE FROM K55ADMIN.iud_tbl");

        rs = st.executeQuery(
            " SELECT * FROM K55ADMIN.iud_tbl ORDER BY c3");

        expColNames = new String [] {"C1", "C2", "C3"};
        JDBC.assertColumnNames(rs, expColNames);
        JDBC.assertDrainResults(rs, 0);

        // 1019 - INNER join an IJ view with a RJ view - join on
        // multiple columns, with "or 1=0" predicate

        st.executeUpdate(
            "INSERT INTO k55admin.iud_tbl"
            + "    SELECT t1.intcol, t1.charcol, t2.vcharcol"
            + "      FROM K55ADMIN.IJview1 T1 INNER JOIN "
            + "K55ADMIN.RJview2 T2"
            + "        ON T1.smintcol = T2.smintcol AND T1.charcol "
            + "= T2.charcol or 1=0"
            + "      WHERE LENGTH(T1.vcharcol) >= 6");

        rs = st.executeQuery(
            " SELECT * FROM K55ADMIN.iud_tbl ORDER BY c1, c2");

        expColNames = new String [] {"C1", "C2", "C3"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"12", "Twelve", "Twelve"},
            {"14", "Fourteen", "Fourteen"},
            {"16", "Sixteen", "Sixteen"},
            {"18", "Eighteen", "Eighteen"},
            {"20", "Twenty", "Twenty"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 1021 - UPDATE rows inserted in previous statement, with
        // "or 1=0" predicate

        assertUpdateCount(st, 4,
            "UPDATE k55admin.iud_tbl"
            + "  SET c2 = NULL"
            + "  WHERE C1 > ANY ("
            + "    SELECT T1.smintcol"
            + "      FROM K55ADMIN.IJview1 T1 INNER JOIN "
            + "K55ADMIN.RJview2 T2"
            + "        ON T1.smintcol = T2.smintcol AND T1.charcol "
            + "= T2.charcol or 1=0"
            + "      WHERE LENGTH(T1.vcharcol) >= 6)");

        rs = st.executeQuery(
            " SELECT * FROM K55ADMIN.iud_tbl ORDER BY c1, c2");

        expColNames = new String [] {"C1", "C2", "C3"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"12", "Twelve", "Twelve"},
            {"14", null, "Fourteen"},
            {"16", null, "Sixteen"},
            {"18", null, "Eighteen"},
            {"20", null, "Twenty"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 1023 - DELETE rows inserted in previous statement, with
        // "or 1=0" predicate

        assertUpdateCount(st, 5,
            "DELETE FROM K55ADMIN.iud_tbl"
            + "  WHERE c1 = ANY ("
            + "    SELECT T1.smintcol"
            + "      FROM K55ADMIN.IJview1 T1 INNER JOIN "
            + "K55ADMIN.RJview2 T2"
            + "        ON T1.smintcol = T2.smintcol AND T1.charcol "
            + "= T2.charcol or 1=0"
            + "      WHERE LENGTH(T1.vcharcol) >= 6)");

        rs = st.executeQuery(
            " SELECT * FROM K55ADMIN.iud_tbl ORDER BY c1");

        expColNames = new String [] {"C1", "C2", "C3"};
        JDBC.assertColumnNames(rs, expColNames);
        JDBC.assertDrainResults(rs, 0);

        //
        //------------------------------------------------------------
        // test unit 11. Joins tables used in INSERT,
        // UPDATE or DELETE subquery. tables have ;
        //------------------------------------------------------------

        // 1102 - Create tables and populate data
        // needed for the test

        st.executeUpdate(
            "CREATE TABLE k55admin.iud_tbl_P (c1 float,"
            + "                        c2 char(15),"
            + "                        c3 char(15)) ");

        st.executeUpdate(
            " CREATE TABLE k55admin.MANYTYPES_P"
            + "      (INTCOL        INTEGER,"
            + "       SMINTCOL      SMALLINT,"
            + "       DEC62COL      DECIMAL(6,2),"
            + "       DEC72COL      DECIMAL(7,2),"
            + "       FLOATCOL      FLOAT,"
            + "       CHARCOL       CHAR(10),"
            + "       LCHARCOL      CHAR(250),"
            + "       VCHARCOL      VARCHAR(100)) ");

        st.executeUpdate(
            " CREATE TABLE k55admin.MTYPES_NOTNULL_P"
            + "      (INTCOL        INTEGER       NOT NULL,"
            + "       SMINTCOL      SMALLINT      NOT NULL,"
            + "       DEC62COL      DECIMAL(6,2)  NOT NULL,"
            + "       DEC72COL      DECIMAL(7,2)  NOT NULL,"
            + "       FLOATCOL      FLOAT         NOT NULL,"
            + "       CHARCOL       CHAR(15)      NOT NULL,"
            + "       LCHARCOL      CHAR(250)     NOT NULL,"
            + "       VCHARCOL      VARCHAR(100)  NOT NULL) ");

        st.executeUpdate(
            " CREATE TABLE k55admin.MTYPES_CTRL_P"
            + "      (INTCOL        INTEGER       NOT NULL,"
            + "       SMINTCOL      SMALLINT      NOT NULL,"
            + "       DEC62COL      DECIMAL(6,2)  NOT NULL,"
            + "       DEC72COL      DECIMAL(7,2)  NOT NULL,"
            + "       FLOATCOL      FLOAT         NOT NULL,"
            + "       CHARCOL       CHAR(15)      NOT NULL,"
            + "       LCHARCOL      CHAR(250)     NOT NULL,"
            + "       VCHARCOL      VARCHAR(100)  NOT NULL) ");

        st.executeUpdate(
            " INSERT INTO K55ADMIN.manytypes_P SELECT * from "
            + "k55admin.manytypes");

        st.executeUpdate(
            " INSERT INTO K55ADMIN.mtypes_notnull_P SELECT * "
            + " from k55admin.manytypes_notnull");

        st.executeUpdate(
            " INSERT INTO K55ADMIN.mtypes_ctrl_P SELECT * from "
            + "k55admin.manytypes_ctrl");

        // 1104 - LEFT join a table with a LEFT join of a RIGHT
        // join of two LEFT joins into that same table, with "or
        // 1=0" predicate

        st.executeUpdate(
            "INSERT INTO K55ADMIN.iud_tbl_P"
            + "  SELECT t1.floatcol, t1.charcol, t6.vcharcol"
            + "    FROM K55ADMIN.mtypes_ctrl_P T6"
            + "       LEFT JOIN"
            + "        ((k55admin.manytypes_P T1 LEFT JOIN "
            + "K55ADMIN.mtypes_notnull_P T2"
            + "              ON T1.intcol = T2.dec62col)"
            + "         RIGHT JOIN"
            + "           (k55admin.mtypes_notnull_P T4 LEFT JOIN "
            + "K55ADMIN.manytypes_P T3"
            + "                ON T4.smintcol = T3.dec62col or 1=0)"
            + "         ON T1.intcol = T4.smintcol"
            + "         LEFT JOIN"
            + "          k55admin.mtypes_ctrl_P T5"
            + "        ON T5.intcol = T1.smintcol)"
            + "      ON T6.intcol = T1.smintcol"
            + "    WHERE t1.intcol IS NOT NULL");

        rs = st.executeQuery(
            " SELECT * FROM K55ADMIN.iud_tbl_P ORDER BY c1");

        expColNames = new String [] {"C1", "C2", "C3"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"12.0", "Twelve", "Twelve"},
            {"14.0", "Fourteen", "Fourteen"},
            {"16.0", "Sixteen", "Sixteen"},
            {"18.0", "Eighteen", "Eighteen"},
            {"20.0", "Twenty", "Twenty"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 1106 - UPDATE rows inserted in previous statement, with
        // "or 1=0" predicate

        assertUpdateCount(st, 4,
            "UPDATE K55ADMIN.iud_tbl_P"
            + "  SET c2 = NULL"
            + "  WHERE C1 > ANY ("
            + "    SELECT t1.floatcol"
            + "      FROM K55ADMIN.mtypes_ctrl_P T6"
            + "         LEFT JOIN"
            + "          (k55admin.manytypes_P T1 "
            + "           RIGHT JOIN"
            + "           k55admin.mtypes_notnull_P T4"
            + "           ON T1.floatcol = T4.smintcol"
            + "           LEFT JOIN"
            + "            k55admin.mtypes_ctrl_P T5"
            + "          ON T5.floatcol = T1.smintcol)"
            + "        ON T6.floatcol = T1.smintcol or 1=0"
            + "      WHERE t1.floatcol IS NOT NULL)");

        rs = st.executeQuery(
            " SELECT * FROM K55ADMIN.iud_tbl_P ORDER BY c1");

        expColNames = new String [] {"C1", "C2", "C3"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"12.0", "Twelve", "Twelve"},
            {"14.0", null, "Fourteen"},
            {"16.0", null, "Sixteen"},
            {"18.0", null, "Eighteen"},
            {"20.0", null, "Twenty"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 1108 - DELETE rows inserted in previous statement, with
        // "or 1=0" predicate

        assertUpdateCount(st, 5,
            "DELETE FROM K55ADMIN.iud_tbl_P"
            + "  WHERE c1 = ANY ("
            + "    SELECT t1.floatcol"
            + "      FROM K55ADMIN.mtypes_ctrl_P T6"
            + "         LEFT JOIN"
            + "          (k55admin.manytypes_P T1 "
            + "           RIGHT JOIN"
            + "           k55admin.mtypes_notnull_P T4"
            + "           ON T1.floatcol = T4.smintcol"
            + "           LEFT JOIN"
            + "            k55admin.mtypes_ctrl_P T5"
            + "          ON T5.floatcol = T1.smintcol)"
            + "        ON T6.floatcol = T1.smintcol or 1=0"
            + "      WHERE t1.floatcol IS NOT NULL)");

        rs = st.executeQuery(
            " SELECT * FROM K55ADMIN.iud_tbl_P ORDER BY c1");

        expColNames = new String [] {"C1", "C2", "C3"};
        JDBC.assertColumnNames(rs, expColNames);
        JDBC.assertDrainResults(rs, 0);

        // 1116 - Clean up table iud_tbl_P

        assertUpdateCount(st, 0,
            "DELETE FROM K55ADMIN.iud_tbl_P");

        rs = st.executeQuery(
            " SELECT * FROM K55ADMIN.iud_tbl_P ORDER BY c3");

        expColNames = new String [] {"C1", "C2", "C3"};
        JDBC.assertColumnNames(rs, expColNames);
        JDBC.assertDrainResults(rs, 0);

        // 1118 - LEFT join a RJ view with a LJ view - join on
        // multiple columns, with "or 1=0" predicate

        st.executeUpdate(
            "INSERT INTO k55admin.iud_tbl_P"
            + "    SELECT t1.floatcol, t1.charcol, t2.vcharcol"
            + "      FROM K55ADMIN.RJview2 T1 LEFT JOIN K55ADMIN.LJview2 T2"
            + "        ON T1.smintcol = T2.smintcol AND T1.charcol "
            + "= T2.charcol or 1=0"
            + "      WHERE LENGTH(T1.vcharcol) > 6");

        rs = st.executeQuery(
            " SELECT * FROM K55ADMIN.iud_tbl_P ORDER BY c1");

        expColNames = new String [] {"C1", "C2", "C3"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"14.0", "Fourteen", "Fourteen"},
            {"16.0", "Sixteen", "Sixteen"},
            {"18.0", "Eighteen", "Eighteen"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 1120 - UPDATE rows inserted in previous statement, with
        // "or 1=0" predicate

        assertUpdateCount(st, 2,
            "UPDATE K55ADMIN.iud_tbl_P"
            + "  SET c2 = NULL"
            + "  WHERE C1 < ANY ("
            + "    SELECT T1.smintcol"
            + "      FROM K55ADMIN.RJview2 T1 LEFT JOIN K55ADMIN.LJview2 T2"
            + "        ON T1.smintcol = T2.smintcol AND T1.charcol "
            + "= T2.charcol or 1=0"
            + "      WHERE LENGTH(T1.vcharcol) > 6)");

        rs = st.executeQuery(
            " SELECT * FROM K55ADMIN.iud_tbl_P ORDER BY c1");

        expColNames = new String [] {"C1", "C2", "C3"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"14.0", null, "Fourteen"},
            {"16.0", null, "Sixteen"},
            {"18.0", "Eighteen", "Eighteen"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 1122 - DELETE rows inserted in previous statement, with
        // "or 1=0" predicate

        assertUpdateCount(st, 3,
            "DELETE FROM K55ADMIN.iud_tbl_P"
            + "  WHERE c1 = ANY ("
            + "    SELECT T1.smintcol"
            + "      FROM K55ADMIN.RJview2 T1 LEFT JOIN K55ADMIN.LJview2 T2"
            + "        ON T1.smintcol = T2.smintcol AND T1.charcol "
            + "= T2.charcol or 1=0"
            + "      WHERE LENGTH(T1.vcharcol) > 6)");

        rs = st.executeQuery(
            " SELECT * FROM K55ADMIN.iud_tbl_P ORDER BY c1");

        expColNames = new String [] {"C1", "C2", "C3"};
        JDBC.assertColumnNames(rs, expColNames);
        JDBC.assertDrainResults(rs, 0);

        // 1124 - RIGHT join an IJ view with a RJ view - join on
        // multiple columns, with "or 1=0" predicate

        st.executeUpdate(
            "INSERT INTO K55ADMIN.iud_tbl_P"
            + "    SELECT t1.floatcol, t1.charcol, t2.vcharcol"
            + "      FROM K55ADMIN.IJview1 T1 RIGHT JOIN "
            + "K55ADMIN.RJview2 T2"
            + "        ON T1.smintcol = T2.smintcol AND T1.charcol "
            + "= T2.charcol or 1=0"
            + "      WHERE LENGTH(T1.vcharcol) > 6 OR T2.charcol LIKE 'T%'");

        rs = st.executeQuery(
            " SELECT * FROM K55ADMIN.iud_tbl_P ORDER BY c1");

        expColNames = new String [] {"C1", "C2", "C3"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"12.0", "Twelve", "Twelve"},
            {"14.0", "Fourteen", "Fourteen"},
            {"16.0", "Sixteen", "Sixteen"},
            {"18.0", "Eighteen", "Eighteen"},
            {"20.0", "Twenty", "Twenty"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 1126 - UPDATE rows inserted in previous statement, with
        // "or 1=0" predicate

        assertUpdateCount(st, 3,
            "UPDATE K55ADMIN.iud_tbl_P"
            + "  SET c2 = NULL"
            + "  WHERE C1 < ANY ("
            + "    SELECT T1.floatcol"
            + "      FROM K55ADMIN.IJview1 T1 RIGHT JOIN "
            + "K55ADMIN.RJview2 T2"
            + "        ON T1.smintcol = T2.smintcol AND T1.charcol "
            + "= T2.charcol or 1=0"
            + "      WHERE LENGTH(T1.vcharcol) > 6)");

        rs = st.executeQuery(
            " SELECT * FROM K55ADMIN.iud_tbl_P ORDER BY c1");

        expColNames = new String [] {"C1", "C2", "C3"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"12.0", null, "Twelve"},
            {"14.0", null, "Fourteen"},
            {"16.0", null, "Sixteen"},
            {"18.0", "Eighteen", "Eighteen"},
            {"20.0", "Twenty", "Twenty"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 1128 - DELETE rows inserted in previous statement, with
        // "or 1=0" predicate

        assertUpdateCount(st, 2,
            "DELETE FROM K55ADMIN.iud_tbl_P"
            + "  WHERE c1 IN ("
            + "    SELECT T1.floatcol"
            + "      FROM K55ADMIN.IJview1 T1 RIGHT JOIN "
            + "K55ADMIN.RJview2 T2"
            + "        ON T1.smintcol = T2.smintcol AND T1.charcol "
            + "= T2.charcol or 1=0"
            + "      WHERE substr(T2.vcharcol, 1, 1) = 'T')");

        rs = st.executeQuery(
            " SELECT * FROM K55ADMIN.iud_tbl_P ORDER BY c1");

        expColNames = new String [] {"C1", "C2", "C3"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"14.0", null, "Fourteen"},
            {"16.0", null, "Sixteen"},
            {"18.0", "Eighteen", "Eighteen"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 1130 - Clean up table iud_tbl_P

        assertUpdateCount(st, 3,
            "DELETE FROM K55ADMIN.iud_tbl_P");

        rs = st.executeQuery(
            " SELECT * FROM K55ADMIN.iud_tbl_P ORDER BY c1");

        expColNames = new String [] {"C1", "C2", "C3"};
        JDBC.assertColumnNames(rs, expColNames);
        JDBC.assertDrainResults(rs, 0);

        // 1132 - INNER join an IJ view with a RJ view - join on
        // multiple columns, with "or 1=0" predicate

        st.executeUpdate(
            "INSERT INTO K55ADMIN.iud_tbl_P"
            + "    SELECT t1.floatcol, t1.charcol, t2.vcharcol"
            + "      FROM K55ADMIN.IJview1 T1 INNER JOIN "
            + "K55ADMIN.RJview2 T2"
            + "        ON T1.smintcol = T2.smintcol AND T1.charcol "
            + "= T2.charcol or 1=0"
            + "      WHERE LENGTH(T1.vcharcol) >= 6");

        rs = st.executeQuery(
            " SELECT * FROM K55ADMIN.iud_tbl_P ORDER BY c1");

        expColNames = new String [] {"C1", "C2", "C3"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"12.0", "Twelve", "Twelve"},
            {"14.0", "Fourteen", "Fourteen"},
            {"16.0", "Sixteen", "Sixteen"},
            {"18.0", "Eighteen", "Eighteen"},
            {"20.0", "Twenty", "Twenty"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 1134 - UPDATE rows inserted in previous statement, with
        // "or 1=0" predicate

        assertUpdateCount(st, 2,
            "UPDATE K55ADMIN.iud_tbl_P"
            + "  SET c2 = NULL"
            + "  WHERE C1 IN ("
            + "    SELECT T1.floatcol"
            + "      FROM K55ADMIN.IJview1 T1 INNER JOIN "
            + "K55ADMIN.RJview2 T2"
            + "        ON T1.smintcol = T2.smintcol AND T1.charcol "
            + "= T2.charcol or 1=0"
            + "      WHERE T1.vcharcol LIKE 'T_e%')");

        rs = st.executeQuery(
            " SELECT * FROM K55ADMIN.iud_tbl_P ORDER BY c1");

        expColNames = new String [] {"C1", "C2", "C3"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"12.0", null, "Twelve"},
            {"14.0", "Fourteen", "Fourteen"},
            {"16.0", "Sixteen", "Sixteen"},
            {"18.0", "Eighteen", "Eighteen"},
            {"20.0", null, "Twenty"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 1136 - DELETE rows inserted in previous statement, with
        // "or 1=0" predicate

        assertUpdateCount(st, 5,
            "DELETE FROM K55ADMIN.iud_tbl_P"
            + "  WHERE C1 IN ("
            + "    SELECT T1.floatcol"
            + "      FROM K55ADMIN.IJview1 T1 INNER JOIN "
            + "K55ADMIN.RJview2 T2"
            + "        ON T1.smintcol = T2.smintcol AND T1.charcol "
            + "= T2.charcol or 1=0"
            + "      WHERE T1.vcharcol NOT LIKE 'Tw%' OR "
            + "substr(T2.vcharcol, 1, 1) = 'T')");

        rs = st.executeQuery(
            " SELECT * FROM K55ADMIN.iud_tbl_P ORDER BY c1");

        expColNames = new String [] {"C1", "C2", "C3"};
        JDBC.assertColumnNames(rs, expColNames);
        JDBC.assertDrainResults(rs, 0);

        // some bugs found during development

        rs = st.executeQuery(
            "SELECT t1.floatcol"
            + "  FROM K55ADMIN.mtypes_ctrl_P T6"
            + "     LEFT JOIN"
            + "      ((k55admin.manytypes_P T1 LEFT JOIN "
            + "K55ADMIN.mtypes_notnull_P T2"
            + "            ON T1.floatcol = T2.dec62col or 1=0)"
            + "       RIGHT JOIN"
            + "         (k55admin.mtypes_notnull_P T4 LEFT JOIN "
            + "K55ADMIN.manytypes_P T3"
            + "              ON T4.smintcol = T3.dec62col)"
            + "       ON T1.floatcol = T4.smintcol"
            + "       LEFT JOIN"
            + "        k55admin.mtypes_ctrl_P T5"
            + "      ON T5.floatcol = T1.smintcol)"
            + "    ON T6.floatcol = T1.smintcol or 1=0"
            + " WHERE t1.floatcol IS NOT NULL");

        expColNames = new String [] {"FLOATCOL"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"12.0"},
            {"14.0"},
            {"16.0"},
            {"18.0"},
            {"20.0"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        rs = st.executeQuery(
            " SELECT *"
            + " FROM K55ADMIN.mtypes_ctrl_P T6"
            + "     LEFT JOIN ((k55admin.manytypes_P T1 LEFT JOIN "
            + "K55ADMIN.mtypes_notnull_P T2 ON T1.floatcol = "
            + "T2.dec62col or 1=0)"
            + "                 RIGHT JOIN "
            + "k55admin.mtypes_notnull_P T4 ON T1.floatcol = T4.smintcol)"
            + "     ON T6.floatcol = T1.smintcol or 1=0");

        expColNames = new String [] {"INTCOL", "SMINTCOL", "DEC62COL", "DEC72COL", "FLOATCOL", "CHARCOL", "LCHARCOL", "VCHARCOL", "INTCOL", "SMINTCOL", "DEC62COL", "DEC72COL", "FLOATCOL", "CHARCOL", "LCHARCOL", "VCHARCOL", "INTCOL", "SMINTCOL", "DEC62COL", "DEC72COL", "FLOATCOL", "CHARCOL", "LCHARCOL", "VCHARCOL", "INTCOL", "SMINTCOL", "DEC62COL", "DEC72COL", "FLOATCOL", "CHARCOL", "LCHARCOL", "VCHARCOL"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"2", "2", "2.00", "2.00", "2.0", "Two", "Two", "Two", null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null},
            {"4", "4", "4.00", "4.00", "4.0", "Four", "Four", "Four", null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null},
            {"6", "6", "6.00", "6.00", "6.0", "Six", "Six", "Six", null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null},
            {"8", "8", "8.00", "8.00", "8.0", "Eight", "Eight", "Eight", null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null},
            {"10", "10", "10.00", "10.00", "10.0", "Ten", "Ten", "Ten", null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null},
            {"12", "12", "12.00", "12.00", "12.0", "Twelve", "Twelve", "Twelve", "12", "12", "12.00", "12.00", "12.0", "Twelve", "Twelve", "Twelve", "12", "12", "12.00", "12.00", "12.0", "Twelve", "Twelve", "Twelve", "12", "12", "12.00", "12.00", "12.0", "Twelve", "Twelve", "Twelve"},
            {"14", "14", "14.00", "14.00", "14.0", "Fourteen", "Fourteen", "Fourteen", "14", "14", "14.00", "14.00", "14.0", "Fourteen", "Fourteen", "Fourteen", "14", "14", "14.00", "14.00", "14.0", "Fourteen", "Fourteen", "Fourteen", "14", "14", "14.00", "14.00", "14.0", "Fourteen", "Fourteen", "Fourteen"},
            {"16", "16", "16.00", "16.00", "16.0", "Sixteen", "Sixteen", "Sixteen", "16", "16", "16.00", "16.00", "16.0", "Sixteen", "Sixteen", "Sixteen", "16", "16", "16.00", "16.00", "16.0", "Sixteen", "Sixteen", "Sixteen", "16", "16", "16.00", "16.00", "16.0", "Sixteen", "Sixteen", "Sixteen"},
            {"18", "18", "18.00", "18.00", "18.0", "Eighteen", "Eighteen", "Eighteen", "18", "18", "18.00", "18.00", "18.0", "Eighteen", "Eighteen", "Eighteen", "18", "18", "18.00", "18.00", "18.0", "Eighteen", "Eighteen", "Eighteen", "18", "18", "18.00", "18.00", "18.0", "Eighteen", "Eighteen", "Eighteen"},
            {"20", "20", "20.00", "20.00", "20.0", "Twenty", "Twenty", "Twenty", "20", "20", "20.00", "20.00", "20.0", "Twenty", "Twenty", "Twenty", "20", "20", "20.00", "20.00", "20.0", "Twenty", "Twenty", "Twenty", "20", "20", "20.00", "20.00", "20.0", "Twenty", "Twenty", "Twenty"},
            {"22", "22", "22.00", "22.00", "22.0", "Twenty Two", "Twenty Two", "Twenty Two", null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null},
            {"24", "24", "24.00", "24.00", "24.0", "Twenty Four", "Twenty Four", "Twenty Four", null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null}
        };

        JDBC.assertFullResultSet(rs, expRS);

        st.executeUpdate(
            " DROP VIEW K55ADMIN.LJview1");

        st.executeUpdate(
            " DROP VIEW K55ADMIN.LJview2");

        st.executeUpdate(
            " DROP VIEW K55ADMIN.RJview1");

        st.executeUpdate(
            " DROP VIEW K55admin.RJview2");

        st.executeUpdate(
            " DROP VIEW K55ADMIN.IJview1");

        st.executeUpdate(
            " DROP TABLE k55admin.iud_tbl");

        st.executeUpdate(
            " DROP TABLE k55admin.iud_tbl_P");

        st.executeUpdate(
            " DROP TABLE k55admin.MANYTYPES_P");

        st.executeUpdate(
            " DROP TABLE k55admin.MTYPES_NOTNULL_P");

        st.executeUpdate(
            " DROP TABLE k55admin.MTYPES_CTRL_P");

        //
        // coj211.clp
        // ***********************************************
        // ** complex join tests
        // ***********************************************

        //------------------------------------------------------------
        // test unit 1. plain joins; multiple joins (219);
        //------------------------------------------------------------

        // 101 - Nest INNER join and RIGHT join

        rs = st.executeQuery(
            "SELECT emp_name, old_office, new_office"
            + "  FROM K55ADMIN.employees INNER JOIN"
            + "       (k55admin.old_offices RIGHT JOIN K55ADMIN.new_offices"
            + "        ON k55admin.old_offices.emp_id = "
            + "k55admin.new_offices.emp_id)"
            + "       ON k55admin.employees.emp_id = "
            + "k55admin.new_offices.emp_id"
            + "  ORDER BY 3");

        expColNames = new String [] {"EMP_NAME", "OLD_OFFICE", "NEW_OFFICE"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {null, "X128", "Y124"},
            {"B. Ward", "X127", "Y125"},
            {"J. Thomas", "X125", "Y126"},
            {"C. Manthey", "X124", "Y128"},
            {"N. Baxter", null, "Y129"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 102 - Nest INNER join and LEFT join

        rs = st.executeQuery(
            "SELECT emp_name, old_office, new_office"
            + "  FROM K55ADMIN.employees INNER JOIN"
            + "       (k55admin.old_offices LEFT JOIN K55ADMIN.new_offices"
            + "        ON k55admin.old_offices.emp_id = "
            + "k55admin.new_offices.emp_id)"
            + "       ON k55admin.employees.emp_id = "
            + "k55admin.new_offices.emp_id"
            + "  ORDER BY 3");

        expColNames = new String [] {"EMP_NAME", "OLD_OFFICE", "NEW_OFFICE"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {null, "X128", "Y124"},
            {"B. Ward", "X127", "Y125"},
            {"J. Thomas", "X125", "Y126"},
            {"C. Manthey", "X124", "Y128"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 103 - Nest LEFT join and RIGHT join

        rs = st.executeQuery(
            "SELECT emp_name, old_office, new_office"
            + "  FROM K55ADMIN.employees LEFT JOIN"
            + "       (k55admin.old_offices RIGHT JOIN K55ADMIN.new_offices"
            + "        ON k55admin.old_offices.emp_id = "
            + "k55admin.new_offices.emp_id)"
            + "       ON k55admin.employees.emp_id = "
            + "k55admin.new_offices.emp_id"
            + "  ORDER BY 3, 1");

        expColNames = new String [] {"EMP_NAME", "OLD_OFFICE", "NEW_OFFICE"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {null, "X128", "Y124"},
            {"B. Ward", "X127", "Y125"},
            {"J. Thomas", "X125", "Y126"},
            {"C. Manthey", "X124", "Y128"},
            {"N. Baxter", null, "Y129"},
            {"K. Woods", null, null}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 104 - Nest RIGHT join and LEFT join

        rs = st.executeQuery(
            "SELECT emp_name, old_office, new_office"
            + "  FROM K55ADMIN.employees RIGHT JOIN"
            + "       (k55admin.old_offices LEFT JOIN K55ADMIN.new_offices"
            + "        ON k55admin.old_offices.emp_id = "
            + "k55admin.new_offices.emp_id)"
            + "       ON k55admin.employees.emp_id = "
            + "k55admin.new_offices.emp_id"
            + "  ORDER BY 2");

        expColNames = new String [] {"EMP_NAME", "OLD_OFFICE", "NEW_OFFICE"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"C. Manthey", "X124", "Y128"},
            {"J. Thomas", "X125", "Y126"},
            {null, "X126", null},
            {"B. Ward", "X127", "Y125"},
            {null, "X128", "Y124"},
            {null, "X129", null},
            {null, "X130", null}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 105 - Nest COMMA join and LEFT join

        rs = st.executeQuery(
            "SELECT emp_name, old_office, new_office"
            + "  FROM K55ADMIN.employees,"
            + "       (k55admin.old_offices LEFT JOIN K55ADMIN.new_offices"
            + "           ON k55admin.old_offices.emp_id = "
            + "k55admin.new_offices.emp_id)"
            + "  WHERE k55admin.employees.emp_id = "
            + "k55admin.new_offices.emp_id"
            + "  ORDER BY 3");

        expColNames = new String [] {"EMP_NAME", "OLD_OFFICE", "NEW_OFFICE"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {null, "X128", "Y124"},
            {"B. Ward", "X127", "Y125"},
            {"J. Thomas", "X125", "Y126"},
            {"C. Manthey", "X124", "Y128"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 106 - COMMA join 2 LEFT joins

        rs = st.executeQuery(
            "SELECT T1.emp_id, T2.emp_id, T3.emp_id, T4.emp_id"
            + "  FROM (K55ADMIN.old_offices T1 LEFT JOIN "
            + "K55ADMIN.new_offices T2"
            + "           ON T1.emp_id = T2.emp_id),"
            + "       (k55admin.old_offices T3 LEFT JOIN "
            + "K55ADMIN.new_offices T4"
            + "           ON T3.emp_id = T4.emp_id)"
            + "  WHERE T1.emp_id = T3.emp_id"
            + "  ORDER BY 1");

        expColNames = new String [] {"EMP_ID", "EMP_ID", "EMP_ID", "EMP_ID"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"368521", "368521", "368521", "368521"},
            {"480923", "480923", "480923", "480923"},
            {"537260", "537260", "537260", "537260"},
            {"622273", null, "622273", null},
            {"711276", "711276", "711276", "711276"},
            {"988870", null, "988870", null}
        };

        JDBC.assertFullResultSet(rs, expRS);

        //
        //------------------------------------------------------------
        // test unit 2. UNION of joins;
        //------------------------------------------------------------

        // 201 - UNION two LEFT joins;
        //                        UNION
        //                       /      \
        //                     LJ        LJ
        //                    /  \      /  \
        //                  T1    T2  T2    T1

        rs = st.executeQuery(
            "SELECT T1.charcol, T1.intcol, T2.intcol, "
            + "T2.dec62col, T2.floatcol"
            + "          FROM K55ADMIN.manytypes T1 LEFT JOIN"
            + "               k55admin.manytypes_notnull T2"
            + "            ON T1.intcol = T2.dec62col"
            + " UNION "
            + " SELECT T2.charcol, T2.intcol, T1.intcol, "
            + "T2.dec62col, T2.floatcol"
            + "          FROM K55ADMIN.manytypes_notnull T2 LEFT JOIN"
            + "               k55admin.manytypes T1"
            + "            ON T2.smintcol = T1.dec62col"
            + " ORDER BY 2");

        expColNames = new String [] {"CHARCOL", "INTCOL", "INTCOL", "DEC62COL", "FLOATCOL"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"One", "1", null, null, null},
            {"Two", "2", null, null, null},
            {"Three", "3", null, null, null},
            {"Four", "4", null, null, null},
            {"Five", "5", null, null, null},
            {"Six", "6", null, null, null},
            {"Seven", "7", null, null, null},
            {"Eight", "8", null, null, null},
            {"Nine", "9", null, null, null},
            {"Ten", "10", null, null, null},
            {"Eleven", "11", "11", "11.00", "11.0"},
            {"Twelve", "12", "12", "12.00", "12.0"},
            {"Thirteen", "13", "13", "13.00", "13.0"},
            {"Fourteen", "14", "14", "14.00", "14.0"},
            {"Fifteen", "15", "15", "15.00", "15.0"},
            {"Sixteen", "16", "16", "16.00", "16.0"},
            {"Seventeen", "17", "17", "17.00", "17.0"},
            {"Eighteen", "18", "18", "18.00", "18.0"},
            {"Nineteen", "19", "19", "19.00", "19.0"},
            {"Twenty", "20", "20", "20.00", "20.0"},
            {"Twenty One", "21", null, "21.00", "21.0"},
            {"Twenty Two", "22", null, "22.00", "22.0"},
            {"Twenty Three", "23", null, "23.00", "23.0"},
            {"Twenty Four", "24", null, "24.00", "24.0"},
            {"Twenty Five", "25", null, "25.00", "25.0"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 202 - Again, with extended ON clauses to join on
        // multiple columns

        rs = st.executeQuery(
            "SELECT T1.charcol, T1.intcol, T2.intcol, T2.floatcol"
            + "          FROM K55ADMIN.manytypes T1 LEFT JOIN"
            + "               k55admin.manytypes_notnull T2"
            + "            ON T1.floatcol = T2.dec62col"
            + "               AND T1.smintcol = T2.intcol"
            + " UNION "
            + " SELECT T2.charcol, T2.intcol, T1.intcol, T2.floatcol"
            + "          FROM K55ADMIN.manytypes_notnull T2 LEFT JOIN"
            + "               k55admin.manytypes T1"
            + "            ON T2.smintcol = T1.dec62col"
            + "               AND T2.floatcol = T1.intcol"
            + "               AND T2.smintcol = T1.smintcol"
            + " ORDER BY 1");

        expColNames = new String [] {"CHARCOL", "INTCOL", "INTCOL", "FLOATCOL"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Eight", "8", null, null},
            {"Eighteen", "18", "18", "18.0"},
            {"Eleven", "11", "11", "11.0"},
            {"Fifteen", "15", "15", "15.0"},
            {"Five", "5", null, null},
            {"Four", "4", null, null},
            {"Fourteen", "14", "14", "14.0"},
            {"Nine", "9", null, null},
            {"Nineteen", "19", "19", "19.0"},
            {"One", "1", null, null},
            {"Seven", "7", null, null},
            {"Seventeen", "17", "17", "17.0"},
            {"Six", "6", null, null},
            {"Sixteen", "16", "16", "16.0"},
            {"Ten", "10", null, null},
            {"Thirteen", "13", "13", "13.0"},
            {"Three", "3", null, null},
            {"Twelve", "12", "12", "12.0"},
            {"Twenty", "20", "20", "20.0"},
            {"Twenty Five", "25", null, "25.0"},
            {"Twenty Four", "24", null, "24.0"},
            {"Twenty One", "21", null, "21.0"},
            {"Twenty Three", "23", null, "23.0"},
            {"Twenty Two", "22", null, "22.0"},
            {"Two", "2", null, null}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 203 - UNION ALL two RIGHT joins

        rs = st.executeQuery(
            "SELECT T1.charcol, T1.intcol, T2.intcol, T2.floatcol"
            + "          FROM K55ADMIN.manytypes T1 RIGHT JOIN"
            + "               k55admin.manytypes_notnull T2"
            + "            ON T1.intcol = T2.dec62col"
            + "               AND T1.smintcol = T2.intcol"
            + " UNION ALL "
            + " SELECT T2.charcol, T2.intcol, T1.intcol, T2.floatcol"
            + "          FROM K55ADMIN.manytypes_notnull T2 RIGHT JOIN"
            + "               k55admin.manytypes T1"
            + "            ON T2.smintcol = T1.dec62col"
            + "               AND T2.smintcol = T1.intcol"
            + "               AND T2.floatcol = T1.smintcol"
            + " ORDER BY 3");

        expColNames = new String [] {"CHARCOL", "INTCOL", "INTCOL", "FLOATCOL"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {null, null, "1", null},
            {null, null, "2", null},
            {null, null, "3", null},
            {null, null, "4", null},
            {null, null, "5", null},
            {null, null, "6", null},
            {null, null, "7", null},
            {null, null, "8", null},
            {null, null, "9", null},
            {null, null, "10", null},
            {"Eleven", "11", "11", "11.0"},
            {"Eleven", "11", "11", "11.0"},
            {"Twelve", "12", "12", "12.0"},
            {"Twelve", "12", "12", "12.0"},
            {"Thirteen", "13", "13", "13.0"},
            {"Thirteen", "13", "13", "13.0"},
            {"Fourteen", "14", "14", "14.0"},
            {"Fourteen", "14", "14", "14.0"},
            {"Fifteen", "15", "15", "15.0"},
            {"Fifteen", "15", "15", "15.0"},
            {"Sixteen", "16", "16", "16.0"},
            {"Sixteen", "16", "16", "16.0"},
            {"Seventeen", "17", "17", "17.0"},
            {"Seventeen", "17", "17", "17.0"},
            {"Eighteen", "18", "18", "18.0"},
            {"Eighteen", "18", "18", "18.0"},
            {"Nineteen", "19", "19", "19.0"},
            {"Nineteen", "19", "19", "19.0"},
            {"Twenty", "20", "20", "20.0"},
            {"Twenty", "20", "20", "20.0"},
            {null, null, "21", "21.0"},
            {null, null, "22", "22.0"},
            {null, null, "23", "23.0"},
            {null, null, "24", "24.0"},
            {null, null, "25", "25.0"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 204 - UNION ALL a RIGHT join with an INNER join

        rs = st.executeQuery(
            "SELECT T1.charcol, T1.intcol, T2.intcol, T2.floatcol"
            + "          FROM K55ADMIN.manytypes T1 RIGHT JOIN"
            + "               k55admin.manytypes_notnull T2"
            + "            ON T1.floatcol = T2.dec62col"
            + "               AND T1.smintcol = T2.intcol"
            + " UNION ALL "
            + " SELECT T2.charcol, T2.intcol, T1.intcol, T2.floatcol"
            + "          FROM K55ADMIN.manytypes_notnull T2 INNER JOIN"
            + "               k55admin.manytypes T1"
            + "            ON T2.smintcol = T1.dec62col"
            + "               AND T2.smintcol = T1.intcol"
            + "               AND T2.smintcol = T1.smintcol"
            + " ORDER BY 2, 3");

        expColNames = new String [] {"CHARCOL", "INTCOL", "INTCOL", "FLOATCOL"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Eleven", "11", "11", "11.0"},
            {"Eleven", "11", "11", "11.0"},
            {"Twelve", "12", "12", "12.0"},
            {"Twelve", "12", "12", "12.0"},
            {"Thirteen", "13", "13", "13.0"},
            {"Thirteen", "13", "13", "13.0"},
            {"Fourteen", "14", "14", "14.0"},
            {"Fourteen", "14", "14", "14.0"},
            {"Fifteen", "15", "15", "15.0"},
            {"Fifteen", "15", "15", "15.0"},
            {"Sixteen", "16", "16", "16.0"},
            {"Sixteen", "16", "16", "16.0"},
            {"Seventeen", "17", "17", "17.0"},
            {"Seventeen", "17", "17", "17.0"},
            {"Eighteen", "18", "18", "18.0"},
            {"Eighteen", "18", "18", "18.0"},
            {"Nineteen", "19", "19", "19.0"},
            {"Nineteen", "19", "19", "19.0"},
            {"Twenty", "20", "20", "20.0"},
            {"Twenty", "20", "20", "20.0"},
            {null, null, "21", "21.0"},
            {null, null, "22", "22.0"},
            {null, null, "23", "23.0"},
            {null, null, "24", "24.0"},
            {null, null, "25", "25.0"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        //
        //------------------------------------------------------------
        // test unit 3. Nest joins;
        //------------------------------------------------------------

        // 301 - RIGHT join two LEFT joins;
        //                          RJ
        //                       /      \
        //                     LJ        LJ
        //                    /  \      /  \
        //                  T1    T2  T2    T1

        rs = st.executeQuery(
            "SELECT t1.charcol, t1.intcol, t2.smintcol,"
            + "       t2.intcol, t2.smintcol, t3.dec62col, T4.floatcol"
            + "  FROM (K55ADMIN.manytypes T1 LEFT JOIN "
            + "K55ADMIN.manytypes_notnull T2"
            + "            ON T1.intcol = T2.floatcol)"
            + "     RIGHT JOIN"
            + "       (k55admin.manytypes_notnull T4 LEFT JOIN "
            + "K55ADMIN.manytypes T3"
            + "            ON T4.smintcol = T3.dec62col)"
            + "     ON T1.intcol = T4.smintcol"
            + " ORDER BY 7");

        expColNames = new String [] {"CHARCOL", "INTCOL", "SMINTCOL", "INTCOL", "SMINTCOL", "DEC62COL", "FLOATCOL"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Eleven", "11", "11", "11", "11", "11.00", "11.0"},
            {"Twelve", "12", "12", "12", "12", "12.00", "12.0"},
            {"Thirteen", "13", "13", "13", "13", "13.00", "13.0"},
            {"Fourteen", "14", "14", "14", "14", "14.00", "14.0"},
            {"Fifteen", "15", "15", "15", "15", "15.00", "15.0"},
            {"Sixteen", "16", "16", "16", "16", "16.00", "16.0"},
            {"Seventeen", "17", "17", "17", "17", "17.00", "17.0"},
            {"Eighteen", "18", "18", "18", "18", "18.00", "18.0"},
            {"Nineteen", "19", "19", "19", "19", "19.00", "19.0"},
            {"Twenty", "20", "20", "20", "20", "20.00", "20.0"},
            {null, null, null, null, null, null, "21.0"},
            {null, null, null, null, null, null, "22.0"},
            {null, null, null, null, null, null, "23.0"},
            {null, null, null, null, null, null, "24.0"},
            {null, null, null, null, null, null, "25.0"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 302 - LEFT join two RIGHT joins;
        //                          LJ
        //                       /      \
        //                     RJ        RJ
        //                    /  \      /  \
        //                  T1    T2  T2    T1

        rs = st.executeQuery(
            "SELECT t1.charcol, t1.intcol, t2.smintcol,"
            + "       t2.intcol, t2.smintcol, t3.dec62col, T4.floatcol"
            + "  FROM (K55ADMIN.manytypes T1 RIGHT JOIN "
            + "K55ADMIN.manytypes_notnull T2"
            + "            ON T1.intcol = T2.floatcol)"
            + "     LEFT JOIN"
            + "       (k55admin.manytypes_notnull T4 RIGHT JOIN "
            + "K55ADMIN.manytypes T3"
            + "            ON T4.smintcol = T3.dec62col)"
            + "     ON T1.intcol = T4.smintcol"
            + " ORDER BY 3");

        expColNames = new String [] {"CHARCOL", "INTCOL", "SMINTCOL", "INTCOL", "SMINTCOL", "DEC62COL", "FLOATCOL"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Eleven", "11", "11", "11", "11", "11.00", "11.0"},
            {"Twelve", "12", "12", "12", "12", "12.00", "12.0"},
            {"Thirteen", "13", "13", "13", "13", "13.00", "13.0"},
            {"Fourteen", "14", "14", "14", "14", "14.00", "14.0"},
            {"Fifteen", "15", "15", "15", "15", "15.00", "15.0"},
            {"Sixteen", "16", "16", "16", "16", "16.00", "16.0"},
            {"Seventeen", "17", "17", "17", "17", "17.00", "17.0"},
            {"Eighteen", "18", "18", "18", "18", "18.00", "18.0"},
            {"Nineteen", "19", "19", "19", "19", "19.00", "19.0"},
            {"Twenty", "20", "20", "20", "20", "20.00", "20.0"},
            {null, null, "21", "21", "21", null, null},
            {null, null, "22", "22", "22", null, null},
            {null, null, "23", "23", "23", null, null},
            {null, null, "24", "24", "24", null, null},
            {null, null, "25", "25", "25", null, null}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 303 - LEFT join a RIGHT join and LEFT join;
        //                          LJ
        //                       /      \
        //                     RJ        LJ
        //                    /  \      /  \
        //                  T1    T2  T2    T1

        rs = st.executeQuery(
            "SELECT t1.charcol, t1.intcol, t2.smintcol,"
            + "       t2.intcol, t2.smintcol, t3.dec62col, T4.floatcol"
            + "  FROM (K55ADMIN.manytypes T1 RIGHT JOIN "
            + "K55ADMIN.manytypes_notnull T2"
            + "            ON T1.intcol = T2.dec62col)"
            + "     LEFT JOIN"
            + "       (k55admin.manytypes_notnull T4 LEFT JOIN "
            + "K55ADMIN.manytypes T3"
            + "            ON T4.floatcol = T3.dec62col)"
            + "     ON T1.intcol = T4.smintcol"
            + " ORDER BY 4");

        expColNames = new String [] {"CHARCOL", "INTCOL", "SMINTCOL", "INTCOL", "SMINTCOL", "DEC62COL", "FLOATCOL"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Eleven", "11", "11", "11", "11", "11.00", "11.0"},
            {"Twelve", "12", "12", "12", "12", "12.00", "12.0"},
            {"Thirteen", "13", "13", "13", "13", "13.00", "13.0"},
            {"Fourteen", "14", "14", "14", "14", "14.00", "14.0"},
            {"Fifteen", "15", "15", "15", "15", "15.00", "15.0"},
            {"Sixteen", "16", "16", "16", "16", "16.00", "16.0"},
            {"Seventeen", "17", "17", "17", "17", "17.00", "17.0"},
            {"Eighteen", "18", "18", "18", "18", "18.00", "18.0"},
            {"Nineteen", "19", "19", "19", "19", "19.00", "19.0"},
            {"Twenty", "20", "20", "20", "20", "20.00", "20.0"},
            {null, null, "21", "21", "21", null, null},
            {null, null, "22", "22", "22", null, null},
            {null, null, "23", "23", "23", null, null},
            {null, null, "24", "24", "24", null, null},
            {null, null, "25", "25", "25", null, null}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 304 - LEFT join a RIGHT join and LEFT join - join on
        // character column
        //                          LJ
        //                       /      \
        //                     RJ        LJ
        //                    /  \      /  \
        //                  T1    T2  T2    T1

        rs = st.executeQuery(
            "SELECT t1.charcol, t1.intcol, t2.smintcol,"
            + "       t2.intcol, t2.smintcol, t3.dec62col, T4.floatcol"
            + "  FROM (K55ADMIN.manytypes T1 RIGHT JOIN "
            + "K55ADMIN.manytypes_notnull T2"
            + "            ON T1.charcol = T2.vcharcol)"
            + "     LEFT JOIN"
            + "       (k55admin.manytypes_notnull T4 LEFT JOIN "
            + "K55ADMIN.manytypes T3"
            + "            ON T4.charcol = T3.vcharcol)"
            + "     ON T1.charcol = T4.vcharcol"
            + " ORDER BY 2");

        expColNames = new String [] {"CHARCOL", "INTCOL", "SMINTCOL", "INTCOL", "SMINTCOL", "DEC62COL", "FLOATCOL"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Eleven", "11", "11", "11", "11", "11.00", "11.0"},
            {"Twelve", "12", "12", "12", "12", "12.00", "12.0"},
            {"Thirteen", "13", "13", "13", "13", "13.00", "13.0"},
            {"Fourteen", "14", "14", "14", "14", "14.00", "14.0"},
            {"Fifteen", "15", "15", "15", "15", "15.00", "15.0"},
            {"Sixteen", "16", "16", "16", "16", "16.00", "16.0"},
            {"Seventeen", "17", "17", "17", "17", "17.00", "17.0"},
            {"Eighteen", "18", "18", "18", "18", "18.00", "18.0"},
            {"Nineteen", "19", "19", "19", "19", "19.00", "19.0"},
            {"Twenty", "20", "20", "20", "20", "20.00", "20.0"},
            {null, null, "25", "25", "25", null, null},
            {null, null, "24", "24", "24", null, null},
            {null, null, "23", "23", "23", null, null},
            {null, null, "22", "22", "22", null, null},
            {null, null, "21", "21", "21", null, null}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 305 - LEFT join a RIGHT join and LEFT join on multiple
        // columns;
        //                          RJ
        //                       /      \
        //                     RJ        LJ
        //                    /  \      /  \
        //                  T1    T2  T2    T1

        rs = st.executeQuery(
            "SELECT t1.charcol, t1.intcol, t2.smintcol,"
            + "       t2.intcol, t2.smintcol, t3.dec62col, T4.floatcol"
            + "  FROM (K55ADMIN.manytypes T1 RIGHT JOIN "
            + "K55ADMIN.manytypes_notnull T2"
            + "            ON T1.intcol = T2.dec62col AND "
            + "T1.charcol = T2.vcharcol)"
            + "     RIGHT JOIN"
            + "       (k55admin.manytypes_notnull T4 LEFT JOIN "
            + "K55ADMIN.manytypes T3"
            + "            ON T4.floatcol = T3.dec62col AND "
            + "T4.charcol = T3.vcharcol)"
            + "     ON T1.intcol = T4.smintcol AND T1.charcol = T4.vcharcol"
            + " ORDER BY 7");

        expColNames = new String [] {"CHARCOL", "INTCOL", "SMINTCOL", "INTCOL", "SMINTCOL", "DEC62COL", "FLOATCOL"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Eleven", "11", "11", "11", "11", "11.00", "11.0"},
            {"Twelve", "12", "12", "12", "12", "12.00", "12.0"},
            {"Thirteen", "13", "13", "13", "13", "13.00", "13.0"},
            {"Fourteen", "14", "14", "14", "14", "14.00", "14.0"},
            {"Fifteen", "15", "15", "15", "15", "15.00", "15.0"},
            {"Sixteen", "16", "16", "16", "16", "16.00", "16.0"},
            {"Seventeen", "17", "17", "17", "17", "17.00", "17.0"},
            {"Eighteen", "18", "18", "18", "18", "18.00", "18.0"},
            {"Nineteen", "19", "19", "19", "19", "19.00", "19.0"},
            {"Twenty", "20", "20", "20", "20", "20.00", "20.0"},
            {null, null, null, null, null, null, "21.0"},
            {null, null, null, null, null, null, "22.0"},
            {null, null, null, null, null, null, "23.0"},
            {null, null, null, null, null, null, "24.0"},
            {null, null, null, null, null, null, "25.0"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        //
        //------------------------------------------------------------
        // test unit 4. Join a Nest join with a
        // table;
        //------------------------------------------------------------

        // 401 - LEFT join a RIGHT join of two LEFT
        // joins into another table
        //                                 LJ
        //                              /      \
        //                          RJ          T3
        //                       /      \
        //                     LJ        LJ
        //                    /  \      /  \
        //                  T1    T2  T2    T1


        rs = st.executeQuery(
            "SELECT t1.intcol, t1.smintcol, t5.charcol"
            + "  FROM (K55ADMIN.manytypes T1 LEFT JOIN "
            + "K55ADMIN.manytypes_notnull T2"
            + "            ON T1.intcol = T2.dec62col)"
            + "     RIGHT JOIN"
            + "       (k55admin.manytypes_notnull T4 LEFT JOIN "
            + "K55ADMIN.manytypes T3"
            + "            ON T4.smintcol = T3.dec62col)"
            + "     ON T1.intcol = T4.smintcol"
            + "     LEFT JOIN"
            + "      k55admin.manytypes_ctrl T5"
            + "    ON T5.intcol = T1.smintcol"
            + " ORDER BY 1");

        expColNames = new String [] {"INTCOL", "SMINTCOL", "CHARCOL"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"11", "11", null},
            {"12", "12", "Twelve"},
            {"13", "13", null},
            {"14", "14", "Fourteen"},
            {"15", "15", null},
            {"16", "16", "Sixteen"},
            {"17", "17", null},
            {"18", "18", "Eighteen"},
            {"19", "19", null},
            {"20", "20", "Twenty"},
            {null, null, null},
            {null, null, null},
            {null, null, null},
            {null, null, null},
            {null, null, null}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 402 - Add WHERE clause to previous query

        rs = st.executeQuery(
            "SELECT t1.intcol, t1.smintcol, t5.charcol, t5.intcol"
            + "  FROM (K55ADMIN.manytypes T1 LEFT JOIN "
            + "K55ADMIN.manytypes_notnull T2"
            + "            ON T1.intcol = T2.dec62col)"
            + "     RIGHT JOIN"
            + "       (k55admin.manytypes_notnull T4 LEFT JOIN "
            + "K55ADMIN.manytypes T3"
            + "            ON T4.smintcol = T3.dec62col)"
            + "     ON T1.intcol = T4.smintcol"
            + "     LEFT JOIN"
            + "      k55admin.manytypes_ctrl T5"
            + "    ON T5.intcol = T1.smintcol"
            + "  WHERE t5.intcol < 15 or t1.intcol IS NULL"
            + " ORDER BY 1");

        expColNames = new String [] {"INTCOL", "SMINTCOL", "CHARCOL", "INTCOL"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"12", "12", "Twelve", "12"},
            {"14", "14", "Fourteen", "14"},
            {null, null, null, null},
            {null, null, null, null},
            {null, null, null, null},
            {null, null, null, null},
            {null, null, null, null}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 403 - RIGHT join a RIGHT join of two LEFT joins into
        // another table
        //                                 RJ
        //                              /      \
        //                          RJ          T3
        //                       /      \
        //                     LJ        LJ
        //                    /  \      /  \
        //                  T1    T2  T2    T1


        rs = st.executeQuery(
            "SELECT t1.intcol, t3.smintcol, t5.charcol"
            + "  FROM (K55ADMIN.manytypes T1 LEFT JOIN "
            + "K55ADMIN.manytypes_notnull T2"
            + "            ON T1.intcol = T2.dec62col)"
            + "     RIGHT JOIN"
            + "       (k55admin.manytypes_notnull T4 LEFT JOIN "
            + "K55ADMIN.manytypes T3"
            + "            ON T4.smintcol = T3.dec62col)"
            + "     ON T1.intcol = T4.smintcol"
            + "     RIGHT JOIN"
            + "      k55admin.manytypes_ctrl T5"
            + "    ON T5.intcol = T1.smintcol"
            + " ORDER BY 1, 3");

        expColNames = new String [] {"INTCOL", "SMINTCOL", "CHARCOL"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"12", "12", "Twelve"},
            {"14", "14", "Fourteen"},
            {"16", "16", "Sixteen"},
            {"18", "18", "Eighteen"},
            {"20", "20", "Twenty"},
            {null, null, "Eight"},
            {null, null, "Four"},
            {null, null, "Six"},
            {null, null, "Ten"},
            {null, null, "Twenty Four"},
            {null, null, "Twenty Two"},
            {null, null, "Two"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 404 - Add WHERE clause to previous query

        rs = st.executeQuery(
            "SELECT t1.intcol, t3.smintcol, t5.charcol"
            + "  FROM (K55ADMIN.manytypes T1 LEFT JOIN "
            + "K55ADMIN.manytypes_notnull T2"
            + "            ON T1.intcol = T2.dec62col)"
            + "     RIGHT JOIN"
            + "       (k55admin.manytypes_notnull T4 LEFT JOIN "
            + "K55ADMIN.manytypes T3"
            + "            ON T4.smintcol = T3.dec62col)"
            + "     ON T1.intcol = T4.smintcol"
            + "     RIGHT JOIN"
            + "      k55admin.manytypes_ctrl T5"
            + "    ON T5.intcol = T1.smintcol"
            + "  WHERE t1.intcol < 15 or t1.intcol IS NULL"
            + " ORDER BY 1, 3");

        expColNames = new String [] {"INTCOL", "SMINTCOL", "CHARCOL"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"12", "12", "Twelve"},
            {"14", "14", "Fourteen"},
            {null, null, "Eight"},
            {null, null, "Four"},
            {null, null, "Six"},
            {null, null, "Ten"},
            {null, null, "Twenty Four"},
            {null, null, "Twenty Two"},
            {null, null, "Two"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 405 - RIGHT join a RIGHT join of an INNER joins and a
        // LEFT join into another table;
        //                                 RJ
        //                              /      \
        //                          RJ          T3
        //                       /      \
        //                     IJ        LJ
        //                    /  \      /  \
        //                  T1    T2  T2    T1

        rs = st.executeQuery(
            "SELECT t1.intcol, t3.smintcol, t5.charcol"
            + "  FROM (K55ADMIN.manytypes T1 INNER JOIN "
            + "K55ADMIN.manytypes_notnull T2"
            + "            ON T1.intcol = T2.dec62col)"
            + "     RIGHT JOIN"
            + "       (k55admin.manytypes_notnull T4 LEFT JOIN "
            + "K55ADMIN.manytypes T3"
            + "            ON T4.smintcol = T3.dec62col)"
            + "     ON T1.intcol = T4.smintcol"
            + "     RIGHT JOIN"
            + "      k55admin.manytypes_ctrl T5"
            + "    ON T5.intcol = T1.smintcol"
            + " ORDER BY 1, 3");

        expColNames = new String [] {"INTCOL", "SMINTCOL", "CHARCOL"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"12", "12", "Twelve"},
            {"14", "14", "Fourteen"},
            {"16", "16", "Sixteen"},
            {"18", "18", "Eighteen"},
            {"20", "20", "Twenty"},
            {null, null, "Eight"},
            {null, null, "Four"},
            {null, null, "Six"},
            {null, null, "Ten"},
            {null, null, "Twenty Four"},
            {null, null, "Twenty Two"},
            {null, null, "Two"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 406 - Add WHERE clause to previous query

        rs = st.executeQuery(
            "SELECT t1.intcol, t3.smintcol, t5.charcol"
            + "  FROM (K55ADMIN.manytypes T1 INNER JOIN "
            + "K55ADMIN.manytypes_notnull T2"
            + "            ON T1.intcol = T2.dec62col)"
            + "     RIGHT JOIN"
            + "       (k55admin.manytypes_notnull T4 LEFT JOIN "
            + "K55ADMIN.manytypes T3"
            + "            ON T4.smintcol = T3.dec62col)"
            + "     ON T1.intcol = T4.smintcol"
            + "     RIGHT JOIN"
            + "      k55admin.manytypes_ctrl T5"
            + "    ON T5.intcol = T1.smintcol"
            + "  WHERE t1.intcol < 15 or t5.intcol IS NOT NULL"
            + " ORDER BY 1, 3");

        expColNames = new String [] {"INTCOL", "SMINTCOL", "CHARCOL"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"12", "12", "Twelve"},
            {"14", "14", "Fourteen"},
            {"16", "16", "Sixteen"},
            {"18", "18", "Eighteen"},
            {"20", "20", "Twenty"},
            {null, null, "Eight"},
            {null, null, "Four"},
            {null, null, "Six"},
            {null, null, "Ten"},
            {null, null, "Twenty Four"},
            {null, null, "Twenty Two"},
            {null, null, "Two"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 407 - RIGHT join a RIGHT join of an INNER joins and a
        // LEFT join into another table - join on character column;
        //                                 RJ
        //                              /      \
        //                          RJ          T3
        //                       /      \
        //                     IJ        LJ
        //                    /  \      /  \
        //                  T1    T2  T2    T1

        rs = st.executeQuery(
            "SELECT t1.intcol, t1.smintcol, t4.dec62col, t5.charcol"
            + "  FROM (K55ADMIN.manytypes T1 INNER JOIN "
            + "K55ADMIN.manytypes_notnull T2"
            + "            ON T1.intcol = T2.dec62col)"
            + "     RIGHT JOIN"
            + "       (k55admin.manytypes_notnull T4 LEFT JOIN "
            + "K55ADMIN.manytypes T3"
            + "            ON T4.smintcol = T3.dec62col)"
            + "     ON T1.intcol = T4.smintcol"
            + "     RIGHT JOIN"
            + "      k55admin.manytypes_ctrl T5"
            + "    ON T5.charcol = T1.vcharcol"
            + " ORDER BY 1, 4");

        expColNames = new String [] {"INTCOL", "SMINTCOL", "DEC62COL", "CHARCOL"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"12", "12", "12.00", "Twelve"},
            {"14", "14", "14.00", "Fourteen"},
            {"16", "16", "16.00", "Sixteen"},
            {"18", "18", "18.00", "Eighteen"},
            {"20", "20", "20.00", "Twenty"},
            {null, null, null, "Eight"},
            {null, null, null, "Four"},
            {null, null, null, "Six"},
            {null, null, null, "Ten"},
            {null, null, null, "Twenty Four"},
            {null, null, null, "Twenty Two"},
            {null, null, null, "Two"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 408 - Add WHERE clause to previous query

        rs = st.executeQuery(
            "SELECT t1.intcol, t3.smintcol, t4.dec62col, t5.charcol"
            + "  FROM (K55ADMIN.manytypes T1 INNER JOIN "
            + "K55ADMIN.manytypes_notnull T2"
            + "            ON T1.intcol = T2.dec62col)"
            + "     RIGHT JOIN"
            + "       (k55admin.manytypes_notnull T4 LEFT JOIN "
            + "K55ADMIN.manytypes T3"
            + "            ON T4.smintcol = T3.dec62col)"
            + "     ON T1.intcol = T4.smintcol"
            + "     RIGHT JOIN"
            + "      k55admin.manytypes_ctrl T5"
            + "    ON T5.charcol = T1.vcharcol"
            + "  WHERE t1.intcol between 12 and 18"
            + " ORDER BY 1");

        expColNames = new String [] {"INTCOL", "SMINTCOL", "DEC62COL", "CHARCOL"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"12", "12", "12.00", "Twelve"},
            {"14", "14", "14.00", "Fourteen"},
            {"16", "16", "16.00", "Sixteen"},
            {"18", "18", "18.00", "Eighteen"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 409 - RIGHT join a RIGHT join of an INNER joins and a
        // LEFT join into another table - join on multiple columns;
        //                                 RJ
        //                              /      \
        //                          RJ          T3
        //                       /      \
        //                     IJ        LJ
        //                    /  \      /  \
        //                  T1    T2  T2    T1

        rs = st.executeQuery(
            "SELECT t1.intcol, t3.smintcol, t5.charcol"
            + "  FROM (K55ADMIN.manytypes T1 INNER JOIN "
            + "K55ADMIN.manytypes_notnull T2"
            + "            ON T1.intcol = T2.dec62col AND "
            + "T1.charcol = T2.vcharcol)"
            + "     RIGHT JOIN"
            + "       (k55admin.manytypes_notnull T4 LEFT JOIN "
            + "K55ADMIN.manytypes T3"
            + "            ON T4.smintcol = T3.dec62col)"
            + "     ON T1.intcol = T4.smintcol"
            + "     RIGHT JOIN"
            + "       k55admin.manytypes_ctrl T5"
            + "    ON T5.charcol = T1.vcharcol AND T5.intcol = "
            + "T1.smintcol AND"
            + "       T5.intcol = T3.intcol"
            + " ORDER BY 1, 3");

        expColNames = new String [] {"INTCOL", "SMINTCOL", "CHARCOL"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"12", "12", "Twelve"},
            {"14", "14", "Fourteen"},
            {"16", "16", "Sixteen"},
            {"18", "18", "Eighteen"},
            {"20", "20", "Twenty"},
            {null, null, "Eight"},
            {null, null, "Four"},
            {null, null, "Six"},
            {null, null, "Ten"},
            {null, null, "Twenty Four"},
            {null, null, "Twenty Two"},
            {null, null, "Two"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        //
        //------------------------------------------------------------
        // test unit 5. Join a table with a nest
        // join;
        //------------------------------------------------------------

        // 501 - LEFT join a table with a LEFT join
        // of a RIGHT join of two LEFT joins into that same table;
        //                          LJ
        //                       /      \
        //                     T3          LJ
        //                              /      \
        //                          RJ          T3
        //                       /      \
        //                     LJ        LJ
        //                    /  \      /  \
        //                  T1    T2  T2    T1

        rs = st.executeQuery(
            "SELECT t1.intcol, t5.smintcol, t6.charcol"
            + "  FROM K55ADMIN.manytypes_ctrl T6"
            + "     LEFT JOIN"
            + "      ((k55admin.manytypes T1 LEFT JOIN "
            + "K55ADMIN.manytypes_notnull T2"
            + "            ON T1.intcol = T2.dec62col)"
            + "       RIGHT JOIN"
            + "         (k55admin.manytypes_notnull T4 LEFT JOIN "
            + "K55ADMIN.manytypes T3"
            + "              ON T4.smintcol = T3.dec62col)"
            + "       ON T1.intcol = T4.smintcol"
            + "       LEFT JOIN"
            + "        k55admin.manytypes_ctrl T5"
            + "      ON T5.intcol = T1.smintcol)"
            + "    ON T6.intcol = T1.smintcol"
            + " ORDER BY 1, 3");

        expColNames = new String [] {"INTCOL", "SMINTCOL", "CHARCOL"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"12", "12", "Twelve"},
            {"14", "14", "Fourteen"},
            {"16", "16", "Sixteen"},
            {"18", "18", "Eighteen"},
            {"20", "20", "Twenty"},
            {null, null, "Eight"},
            {null, null, "Four"},
            {null, null, "Six"},
            {null, null, "Ten"},
            {null, null, "Twenty Four"},
            {null, null, "Twenty Two"},
            {null, null, "Two"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 502 - Add WHERE clause to previous query

        rs = st.executeQuery(
            "SELECT t1.intcol, t5.smintcol, t6.charcol"
            + "  FROM K55ADMIN.manytypes_ctrl T6"
            + "     LEFT JOIN"
            + "      ((k55admin.manytypes T1 LEFT JOIN "
            + "K55ADMIN.manytypes_notnull T2"
            + "            ON T1.intcol = T2.dec62col)"
            + "       RIGHT JOIN"
            + "         (k55admin.manytypes_notnull T4 LEFT JOIN "
            + "K55ADMIN.manytypes T3"
            + "              ON T4.smintcol = T3.dec62col)"
            + "       ON T1.intcol = T4.smintcol"
            + "       LEFT JOIN"
            + "        k55admin.manytypes_ctrl T5"
            + "      ON T5.intcol = T1.smintcol)"
            + "    ON T6.intcol = T1.smintcol"
            + "  WHERE t1.intcol IS NOT NULL"
            + " ORDER BY 1");

        expColNames = new String [] {"INTCOL", "SMINTCOL", "CHARCOL"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"12", "12", "Twelve"},
            {"14", "14", "Fourteen"},
            {"16", "16", "Sixteen"},
            {"18", "18", "Eighteen"},
            {"20", "20", "Twenty"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 503 - RIGHT join a table with a LEFT join of a RIGHT
        // join of two LEFT joins into that same table;
        //                          RJ
        //                       /      \
        //                     T3          LJ
        //                              /      \
        //                          RJ          T3
        //                       /      \
        //                     LJ        LJ
        //                    /  \      /  \
        //                  T1    T2  T2    T1

        rs = st.executeQuery(
            "SELECT t1.intcol, t5.smintcol, t6.charcol"
            + "  FROM K55ADMIN.manytypes_ctrl T6"
            + "     RIGHT JOIN"
            + "      ((k55admin.manytypes T1 LEFT JOIN "
            + "K55ADMIN.manytypes_notnull T2"
            + "            ON T1.intcol = T2.dec62col)"
            + "       RIGHT JOIN"
            + "         (k55admin.manytypes_notnull T4 LEFT JOIN "
            + "K55ADMIN.manytypes T3"
            + "              ON T4.smintcol = T3.dec62col )"
            + "       ON T1.intcol = T4.smintcol"
            + "       LEFT JOIN"
            + "        k55admin.manytypes_ctrl T5"
            + "      ON T5.intcol = T1.smintcol)"
            + "    ON T6.intcol = T1.smintcol"
            + " ORDER BY 1");

        expColNames = new String [] {"INTCOL", "SMINTCOL", "CHARCOL"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"11", null, null},
            {"12", "12", "Twelve"},
            {"13", null, null},
            {"14", "14", "Fourteen"},
            {"15", null, null},
            {"16", "16", "Sixteen"},
            {"17", null, null},
            {"18", "18", "Eighteen"},
            {"19", null, null},
            {"20", "20", "Twenty"},
            {null, null, null},
            {null, null, null},
            {null, null, null},
            {null, null, null},
            {null, null, null}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 504 - Add WHERE clause to previous query

        rs = st.executeQuery(
            "SELECT t1.intcol, t5.smintcol, t6.charcol"
            + "  FROM K55ADMIN.manytypes_ctrl T6"
            + "     RIGHT JOIN"
            + "      ((k55admin.manytypes T1 LEFT JOIN "
            + "K55ADMIN.manytypes_notnull T2"
            + "            ON T1.intcol = T2.dec62col)"
            + "       RIGHT JOIN"
            + "         (k55admin.manytypes_notnull T4 LEFT JOIN "
            + "K55ADMIN.manytypes T3"
            + "              ON T4.smintcol = T3.dec62col)"
            + "       ON T1.intcol = T4.smintcol"
            + "       LEFT JOIN"
            + "        k55admin.manytypes_ctrl T5"
            + "      ON T5.intcol = T1.smintcol)"
            + "    ON T6.intcol = T1.smintcol"
            + "  WHERE t1.intcol IS NOT NULL and t1.intcol > 12"
            + " ORDER BY 1");

        expColNames = new String [] {"INTCOL", "SMINTCOL", "CHARCOL"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"13", null, null},
            {"14", "14", "Fourteen"},
            {"15", null, null},
            {"16", "16", "Sixteen"},
            {"17", null, null},
            {"18", "18", "Eighteen"},
            {"19", null, null},
            {"20", "20", "Twenty"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 505 - INNER join a table with a LEFT join of a RIGHT
        // join of two LEFT joins into that same table;
        //                          IJ
        //                       /      \
        //                     T3          LJ
        //                              /      \
        //                          RJ          T3
        //                       /      \
        //                     LJ        LJ
        //                    /  \      /  \
        //                  T1    T2  T2    T1

        rs = st.executeQuery(
            "SELECT t1.intcol, t5.smintcol, t6.charcol"
            + "  FROM K55ADMIN.manytypes_ctrl T6"
            + "     INNER JOIN"
            + "      ((k55admin.manytypes T1 LEFT JOIN "
            + "K55ADMIN.manytypes_notnull T2"
            + "            ON T1.intcol = T2.dec62col)"
            + "       RIGHT JOIN"
            + "         (k55admin.manytypes_notnull T4 LEFT JOIN "
            + "K55ADMIN.manytypes T3"
            + "              ON T4.smintcol = T3.dec62col)"
            + "       ON T1.intcol = T4.smintcol"
            + "       LEFT JOIN"
            + "        k55admin.manytypes_ctrl T5"
            + "      ON T5.intcol = T1.smintcol)"
            + "    ON T6.intcol = T1.smintcol"
            + " ORDER BY 1");

        expColNames = new String [] {"INTCOL", "SMINTCOL", "CHARCOL"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"12", "12", "Twelve"},
            {"14", "14", "Fourteen"},
            {"16", "16", "Sixteen"},
            {"18", "18", "Eighteen"},
            {"20", "20", "Twenty"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 506 - Add WHERE clause to previous query

        rs = st.executeQuery(
            "SELECT t1.intcol, t1.smintcol, t1.dec62col"
            + "  FROM K55ADMIN.manytypes_ctrl T6"
            + "     INNER JOIN"
            + "      ((k55admin.manytypes T1 LEFT JOIN "
            + "K55ADMIN.manytypes_notnull T2"
            + "            ON T1.intcol = T2.dec62col)"
            + "       RIGHT JOIN"
            + "         (k55admin.manytypes_notnull T4 LEFT JOIN "
            + "K55ADMIN.manytypes T3"
            + "              ON T4.smintcol = T3.dec62col)"
            + "       ON T1.intcol = T4.smintcol"
            + "       LEFT JOIN"
            + "        k55admin.manytypes_ctrl T5"
            + "      ON T5.intcol = T1.smintcol)"
            + "    ON T6.intcol = T1.smintcol"
            + "  WHERE t1.intcol <= 20"
            + " ORDER BY 1");

        expColNames = new String [] {"INTCOL", "SMINTCOL", "DEC62COL"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"12", "12", "12.00"},
            {"14", "14", "14.00"},
            {"16", "16", "16.00"},
            {"18", "18", "18.00"},
            {"20", "20", "20.00"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 507 - INNER join a table with a LEFT join of a RIGHT
        // join of two LEFT joins into that same table - join on
        // character column
        //                          IJ
        //                       /      \
        //                     T3          LJ
        //                              /      \
        //                          RJ          T3
        //                       /      \
        //                     LJ        LJ
        //                    /  \      /  \
        //                  T1    T2  T2    T1

        rs = st.executeQuery(
            "SELECT t1.intcol, t3.smintcol, t5.dec62col, t6.charcol"
            + "  FROM K55ADMIN.manytypes_ctrl T6"
            + "     INNER JOIN"
            + "      ((k55admin.manytypes T1 LEFT JOIN "
            + "K55ADMIN.manytypes_notnull T2"
            + "            ON T1.charcol = T2.vcharcol)"
            + "       RIGHT JOIN"
            + "         (k55admin.manytypes_notnull T4 LEFT JOIN "
            + "K55ADMIN.manytypes T3"
            + "              ON T4.smintcol = T3.dec62col)"
            + "       ON T1.intcol = T4.smintcol"
            + "       LEFT JOIN"
            + "        k55admin.manytypes_ctrl T5"
            + "      ON T5.intcol = T1.smintcol)"
            + "    ON T6.charcol = T1.vcharcol"
            + " ORDER BY 1");

        expColNames = new String [] {"INTCOL", "SMINTCOL", "DEC62COL", "CHARCOL"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"12", "12", "12.00", "Twelve"},
            {"14", "14", "14.00", "Fourteen"},
            {"16", "16", "16.00", "Sixteen"},
            {"18", "18", "18.00", "Eighteen"},
            {"20", "20", "20.00", "Twenty"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 508 - INNER join a table with a LEFT join of a RIGHT
        // join of two LEFT joins into that same table - join on
        // multiple columns
        //                          RJ
        //                       /      \
        //                     T3          LJ
        //                              /      \
        //                          RJ          T3
        //                       /      \
        //                     LJ        LJ
        //                    /  \      /  \
        //                  T1    T2  T2    T1


        rs = st.executeQuery(
            "SELECT t1.intcol, t3.smintcol, t5.dec62col, "
            + "t6.charcol, t1.vcharcol, t5.charcol"
            + "  FROM K55ADMIN.manytypes_ctrl T6"
            + "     RIGHT JOIN"
            + "      ((k55admin.manytypes T1 LEFT JOIN "
            + "K55ADMIN.manytypes_notnull T2"
            + "            ON T1.charcol = T2.vcharcol AND "
            + "T1.intcol = T2.dec62col)"
            + "       RIGHT JOIN"
            + "         (k55admin.manytypes_notnull T4 LEFT JOIN "
            + "K55ADMIN.manytypes T3"
            + "              ON T4.smintcol = T3.dec62col)"
            + "       ON T1.intcol = T4.smintcol"
            + "       LEFT JOIN"
            + "        k55admin.manytypes_ctrl T5"
            + "      ON T5.intcol = T1.smintcol AND T5.charcol = "
            + "T1.vcharcol)"
            + "    ON T6.charcol = T1.vcharcol AND T6.intcol = "
            + "T3.smintcol AND "
            + "       T6.intcol = T5.intcol"
            + " ORDER BY 1");

        expColNames = new String [] {"INTCOL", "SMINTCOL", "DEC62COL", "CHARCOL", "VCHARCOL", "CHARCOL"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"11", "11", null, null, "Eleven", null},
            {"12", "12", "12.00", "Twelve", "Twelve", "Twelve"},
            {"13", "13", null, null, "Thirteen", null},
            {"14", "14", "14.00", "Fourteen", "Fourteen", "Fourteen"},
            {"15", "15", null, null, "Fifteen", null},
            {"16", "16", "16.00", "Sixteen", "Sixteen", "Sixteen"},
            {"17", "17", null, null, "Seventeen", null},
            {"18", "18", "18.00", "Eighteen", "Eighteen", "Eighteen"},
            {"19", "19", null, null, "Nineteen", null},
            {"20", "20", "20.00", "Twenty", "Twenty", "Twenty"},
            {null, null, null, null, null, null},
            {null, null, null, null, null, null},
            {null, null, null, null, null, null},
            {null, null, null, null, null, null},
            {null, null, null, null, null, null}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 509 - Add WHERE clause to previous query

        rs = st.executeQuery(
            "SELECT t1.intcol, t3.smintcol, t5.dec62col, "
            + "t6.charcol, t1.vcharcol, t5.charcol"
            + "  FROM K55ADMIN.manytypes_ctrl T6"
            + "     RIGHT JOIN"
            + "      ((k55admin.manytypes T1 LEFT JOIN "
            + "K55ADMIN.manytypes_notnull T2"
            + "            ON T1.charcol = T2.vcharcol AND "
            + "T1.intcol = T2.dec62col)"
            + "       RIGHT JOIN"
            + "         (k55admin.manytypes_notnull T4 LEFT JOIN "
            + "K55ADMIN.manytypes T3"
            + "              ON T4.smintcol = T3.dec62col)"
            + "       ON T1.intcol = T4.smintcol"
            + "       LEFT JOIN"
            + "        k55admin.manytypes_ctrl T5"
            + "      ON T5.intcol = T1.smintcol AND T5.charcol = "
            + "T1.vcharcol)"
            + "    ON T6.charcol = T1.vcharcol AND T6.intcol = "
            + "T3.smintcol AND "
            + "       T6.intcol = T5.intcol"
            + "  WHERE t1.vcharcol like 'T%'"
            + " ORDER BY 1");

        expColNames = new String [] {"INTCOL", "SMINTCOL", "DEC62COL", "CHARCOL", "VCHARCOL", "CHARCOL"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"12", "12", "12.00", "Twelve", "Twelve", "Twelve"},
            {"13", "13", null, null, "Thirteen", null},
            {"20", "20", "20.00", "Twenty", "Twenty", "Twenty"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        //------------------------------------------------------------
        // -- multiple joins (224)
        //------------------------------------------------------------

        //------------------------------------------------------------
        // test unit 6. Create Views needed for other
        // testunit below;
        //------------------------------------------------------------

        // 602 - Create View on LEFT join;
        //                    View
        //                      |
        //                     LJ
        //                    /  \
        //                  T1    T2

        st.executeUpdate(
            "CREATE VIEW K55ADMIN.LJview1"
            + "  (charcol, smintcol, intcol, floatcol, vcharcol) AS"
            + "    SELECT T1.charcol, T1.smintcol,"
            + "           T2.intcol, T2.floatcol, T2.vcharcol"
            + "      FROM K55ADMIN.manytypes as t1 LEFT JOIN "
            + "K55ADMIN.manytypes_notnull as T2"
            + "        ON T1.smintcol = T2.intcol");

        // 603 - Create View on 2 LEFT joins;
        //                         View
        //                          |
        //                          LJ
        //                       /      \
        //                     LJ        T3
        //                    /  \
        //                  T1    T2

        st.executeUpdate(
            "CREATE VIEW K55ADMIN.LJview2"
            + "  (charcol, smintcol, intcol, dec62col, floatcol, "
            + "vcharcol) AS"
            + "    SELECT T1.charcol, T1.smintcol, T2.intcol,"
            + "           T3.dec62col, T3.floatcol, T2.vcharcol"
            + "      FROM (K55ADMIN.manytypes as T1 LEFT JOIN "
            + "K55ADMIN.manytypes_notnull as T2"
            + "         ON T1.smintcol = T2.intcol)"
            + "           LEFT JOIN"
            + "             k55admin.manytypes_ctrl as T3"
            + "           ON T1.smintcol = T3.intcol");

        // 604 - Create View on RIGHT join;
        //                    View
        //                      |
        //                     RJ
        //                    /  \
        //                  T1    T2

        st.executeUpdate(
            "CREATE VIEW K55ADMIN.RJview1"
            + "  (charcol, smintcol, intcol, floatcol, vcharcol) AS"
            + "    SELECT T2.charcol, T2.smintcol,"
            + "           T1.intcol, T1.floatcol, T1.vcharcol"
            + "      FROM K55ADMIN.manytypes_notnull as T1 RIGHT "
            + "JOIN K55ADMIN.manytypes as T2"
            + "        ON T2.smintcol = T1.intcol");

        // 605 - Create View on 2 RIGHT joins;
        //                         View
        //                          |
        //                          RJ
        //                       /      \
        //                     RJ        T3
        //                    /  \
        //                  T1    T2

        st.executeUpdate(
            "CREATE VIEW K55admin.RJview2"
            + "  (charcol, smintcol, intcol, dec62col, floatcol, "
            + "vcharcol) AS"
            + "    SELECT T1.charcol, T1.smintcol, T2.intcol,"
            + "           T3.dec62col, T3.floatcol, T2.vcharcol"
            + "      FROM (K55ADMIN.manytypes as T1 RIGHT JOIN "
            + "K55ADMIN.manytypes_notnull as T2"
            + "         ON T1.smintcol = T2.intcol)"
            + "           RIGHT JOIN"
            + "             k55admin.manytypes_ctrl as T3"
            + "           ON T1.smintcol = T3.intcol");

        // 606 - Create View on INNER join;
        //                    View
        //                      |
        //                     IJ
        //                    /  \
        //                  T1    T2

        st.executeUpdate(
            "CREATE VIEW K55ADMIN.IJview1"
            + "  (charcol, smintcol, intcol, floatcol, vcharcol) AS"
            + "    SELECT T1.charcol, T1.smintcol,"
            + "           T2.intcol, T2.floatcol, T2.vcharcol"
            + "      FROM K55ADMIN.manytypes as T1 INNER JOIN "
            + "K55ADMIN.manytypes_notnull as T2"
            + "        ON T1.smintcol = T2.intcol");

        // 607 - Check data in the Views

        rs = st.executeQuery(
            "SELECT * FROM K55ADMIN.LJview1 ORDER BY smintcol");

        expColNames = new String [] {"CHARCOL", "SMINTCOL", "INTCOL", "FLOATCOL", "VCHARCOL"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"One", "1", null, null, null},
            {"Two", "2", null, null, null},
            {"Three", "3", null, null, null},
            {"Four", "4", null, null, null},
            {"Five", "5", null, null, null},
            {"Six", "6", null, null, null},
            {"Seven", "7", null, null, null},
            {"Eight", "8", null, null, null},
            {"Nine", "9", null, null, null},
            {"Ten", "10", null, null, null},
            {"Eleven", "11", "11", "11.0", "Eleven"},
            {"Twelve", "12", "12", "12.0", "Twelve"},
            {"Thirteen", "13", "13", "13.0", "Thirteen"},
            {"Fourteen", "14", "14", "14.0", "Fourteen"},
            {"Fifteen", "15", "15", "15.0", "Fifteen"},
            {"Sixteen", "16", "16", "16.0", "Sixteen"},
            {"Seventeen", "17", "17", "17.0", "Seventeen"},
            {"Eighteen", "18", "18", "18.0", "Eighteen"},
            {"Nineteen", "19", "19", "19.0", "Nineteen"},
            {"Twenty", "20", "20", "20.0", "Twenty"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        rs = st.executeQuery(
            " SELECT * FROM K55ADMIN.LJview2 ORDER BY smintcol");

        expColNames = new String [] {"CHARCOL", "SMINTCOL", "INTCOL", "DEC62COL", "FLOATCOL", "VCHARCOL"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"One", "1", null, null, null, null},
            {"Two", "2", null, "2.00", "2.0", null},
            {"Three", "3", null, null, null, null},
            {"Four", "4", null, "4.00", "4.0", null},
            {"Five", "5", null, null, null, null},
            {"Six", "6", null, "6.00", "6.0", null},
            {"Seven", "7", null, null, null, null},
            {"Eight", "8", null, "8.00", "8.0", null},
            {"Nine", "9", null, null, null, null},
            {"Ten", "10", null, "10.00", "10.0", null},
            {"Eleven", "11", "11", null, null, "Eleven"},
            {"Twelve", "12", "12", "12.00", "12.0", "Twelve"},
            {"Thirteen", "13", "13", null, null, "Thirteen"},
            {"Fourteen", "14", "14", "14.00", "14.0", "Fourteen"},
            {"Fifteen", "15", "15", null, null, "Fifteen"},
            {"Sixteen", "16", "16", "16.00", "16.0", "Sixteen"},
            {"Seventeen", "17", "17", null, null, "Seventeen"},
            {"Eighteen", "18", "18", "18.00", "18.0", "Eighteen"},
            {"Nineteen", "19", "19", null, null, "Nineteen"},
            {"Twenty", "20", "20", "20.00", "20.0", "Twenty"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        rs = st.executeQuery(
            " SELECT * FROM K55ADMIN.RJview1 ORDER BY smintcol");

        expColNames = new String [] {"CHARCOL", "SMINTCOL", "INTCOL", "FLOATCOL", "VCHARCOL"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"One", "1", null, null, null},
            {"Two", "2", null, null, null},
            {"Three", "3", null, null, null},
            {"Four", "4", null, null, null},
            {"Five", "5", null, null, null},
            {"Six", "6", null, null, null},
            {"Seven", "7", null, null, null},
            {"Eight", "8", null, null, null},
            {"Nine", "9", null, null, null},
            {"Ten", "10", null, null, null},
            {"Eleven", "11", "11", "11.0", "Eleven"},
            {"Twelve", "12", "12", "12.0", "Twelve"},
            {"Thirteen", "13", "13", "13.0", "Thirteen"},
            {"Fourteen", "14", "14", "14.0", "Fourteen"},
            {"Fifteen", "15", "15", "15.0", "Fifteen"},
            {"Sixteen", "16", "16", "16.0", "Sixteen"},
            {"Seventeen", "17", "17", "17.0", "Seventeen"},
            {"Eighteen", "18", "18", "18.0", "Eighteen"},
            {"Nineteen", "19", "19", "19.0", "Nineteen"},
            {"Twenty", "20", "20", "20.0", "Twenty"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        rs = st.executeQuery(
            " SELECT * FROM K55ADMIN.RJview2 ORDER BY floatcol");

        expColNames = new String [] {"CHARCOL", "SMINTCOL", "INTCOL", "DEC62COL", "FLOATCOL", "VCHARCOL"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {null, null, null, "2.00", "2.0", null},
            {null, null, null, "4.00", "4.0", null},
            {null, null, null, "6.00", "6.0", null},
            {null, null, null, "8.00", "8.0", null},
            {null, null, null, "10.00", "10.0", null},
            {"Twelve", "12", "12", "12.00", "12.0", "Twelve"},
            {"Fourteen", "14", "14", "14.00", "14.0", "Fourteen"},
            {"Sixteen", "16", "16", "16.00", "16.0", "Sixteen"},
            {"Eighteen", "18", "18", "18.00", "18.0", "Eighteen"},
            {"Twenty", "20", "20", "20.00", "20.0", "Twenty"},
            {null, null, null, "22.00", "22.0", null},
            {null, null, null, "24.00", "24.0", null}
        };

        JDBC.assertFullResultSet(rs, expRS);

        rs = st.executeQuery(
            " SELECT * FROM K55ADMIN.IJview1 ORDER BY intcol");

        expColNames = new String [] {"CHARCOL", "SMINTCOL", "INTCOL", "FLOATCOL", "VCHARCOL"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Eleven", "11", "11", "11.0", "Eleven"},
            {"Twelve", "12", "12", "12.0", "Twelve"},
            {"Thirteen", "13", "13", "13.0", "Thirteen"},
            {"Fourteen", "14", "14", "14.0", "Fourteen"},
            {"Fifteen", "15", "15", "15.0", "Fifteen"},
            {"Sixteen", "16", "16", "16.0", "Sixteen"},
            {"Seventeen", "17", "17", "17.0", "Seventeen"},
            {"Eighteen", "18", "18", "18.0", "Eighteen"},
            {"Nineteen", "19", "19", "19.0", "Nineteen"},
            {"Twenty", "20", "20", "20.0", "Twenty"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        //
        //------------------------------------------------------------
        // test unit 7. Joins on view;
        //------------------------------------------------------------

        // 701 - Select from LJ view and base table

        rs = st.executeQuery(
            "SELECT T1.smintcol, T2.smintcol, T1.intcol, T2.intcol"
            + "   FROM K55ADMIN.LJview2 T1, k55admin.manytypes_notnull T2"
            + "   WHERE T1.intcol IS NOT NULL"
            + "   ORDER BY 1, 2");

        expColNames = new String [] {"SMINTCOL", "SMINTCOL", "INTCOL", "INTCOL"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"11", "11", "11", "11"},
            {"11", "12", "11", "12"},
            {"11", "13", "11", "13"},
            {"11", "14", "11", "14"},
            {"11", "15", "11", "15"},
            {"11", "16", "11", "16"},
            {"11", "17", "11", "17"},
            {"11", "18", "11", "18"},
            {"11", "19", "11", "19"},
            {"11", "20", "11", "20"},
            {"11", "21", "11", "21"},
            {"11", "22", "11", "22"},
            {"11", "23", "11", "23"},
            {"11", "24", "11", "24"},
            {"11", "25", "11", "25"},
            {"12", "11", "12", "11"},
            {"12", "12", "12", "12"},
            {"12", "13", "12", "13"},
            {"12", "14", "12", "14"},
            {"12", "15", "12", "15"},
            {"12", "16", "12", "16"},
            {"12", "17", "12", "17"},
            {"12", "18", "12", "18"},
            {"12", "19", "12", "19"},
            {"12", "20", "12", "20"},
            {"12", "21", "12", "21"},
            {"12", "22", "12", "22"},
            {"12", "23", "12", "23"},
            {"12", "24", "12", "24"},
            {"12", "25", "12", "25"},
            {"13", "11", "13", "11"},
            {"13", "12", "13", "12"},
            {"13", "13", "13", "13"},
            {"13", "14", "13", "14"},
            {"13", "15", "13", "15"},
            {"13", "16", "13", "16"},
            {"13", "17", "13", "17"},
            {"13", "18", "13", "18"},
            {"13", "19", "13", "19"},
            {"13", "20", "13", "20"},
            {"13", "21", "13", "21"},
            {"13", "22", "13", "22"},
            {"13", "23", "13", "23"},
            {"13", "24", "13", "24"},
            {"13", "25", "13", "25"},
            {"14", "11", "14", "11"},
            {"14", "12", "14", "12"},
            {"14", "13", "14", "13"},
            {"14", "14", "14", "14"},
            {"14", "15", "14", "15"},
            {"14", "16", "14", "16"},
            {"14", "17", "14", "17"},
            {"14", "18", "14", "18"},
            {"14", "19", "14", "19"},
            {"14", "20", "14", "20"},
            {"14", "21", "14", "21"},
            {"14", "22", "14", "22"},
            {"14", "23", "14", "23"},
            {"14", "24", "14", "24"},
            {"14", "25", "14", "25"},
            {"15", "11", "15", "11"},
            {"15", "12", "15", "12"},
            {"15", "13", "15", "13"},
            {"15", "14", "15", "14"},
            {"15", "15", "15", "15"},
            {"15", "16", "15", "16"},
            {"15", "17", "15", "17"},
            {"15", "18", "15", "18"},
            {"15", "19", "15", "19"},
            {"15", "20", "15", "20"},
            {"15", "21", "15", "21"},
            {"15", "22", "15", "22"},
            {"15", "23", "15", "23"},
            {"15", "24", "15", "24"},
            {"15", "25", "15", "25"},
            {"16", "11", "16", "11"},
            {"16", "12", "16", "12"},
            {"16", "13", "16", "13"},
            {"16", "14", "16", "14"},
            {"16", "15", "16", "15"},
            {"16", "16", "16", "16"},
            {"16", "17", "16", "17"},
            {"16", "18", "16", "18"},
            {"16", "19", "16", "19"},
            {"16", "20", "16", "20"},
            {"16", "21", "16", "21"},
            {"16", "22", "16", "22"},
            {"16", "23", "16", "23"},
            {"16", "24", "16", "24"},
            {"16", "25", "16", "25"},
            {"17", "11", "17", "11"},
            {"17", "12", "17", "12"},
            {"17", "13", "17", "13"},
            {"17", "14", "17", "14"},
            {"17", "15", "17", "15"},
            {"17", "16", "17", "16"},
            {"17", "17", "17", "17"},
            {"17", "18", "17", "18"},
            {"17", "19", "17", "19"},
            {"17", "20", "17", "20"},
            {"17", "21", "17", "21"},
            {"17", "22", "17", "22"},
            {"17", "23", "17", "23"},
            {"17", "24", "17", "24"},
            {"17", "25", "17", "25"},
            {"18", "11", "18", "11"},
            {"18", "12", "18", "12"},
            {"18", "13", "18", "13"},
            {"18", "14", "18", "14"},
            {"18", "15", "18", "15"},
            {"18", "16", "18", "16"},
            {"18", "17", "18", "17"},
            {"18", "18", "18", "18"},
            {"18", "19", "18", "19"},
            {"18", "20", "18", "20"},
            {"18", "21", "18", "21"},
            {"18", "22", "18", "22"},
            {"18", "23", "18", "23"},
            {"18", "24", "18", "24"},
            {"18", "25", "18", "25"},
            {"19", "11", "19", "11"},
            {"19", "12", "19", "12"},
            {"19", "13", "19", "13"},
            {"19", "14", "19", "14"},
            {"19", "15", "19", "15"},
            {"19", "16", "19", "16"},
            {"19", "17", "19", "17"},
            {"19", "18", "19", "18"},
            {"19", "19", "19", "19"},
            {"19", "20", "19", "20"},
            {"19", "21", "19", "21"},
            {"19", "22", "19", "22"},
            {"19", "23", "19", "23"},
            {"19", "24", "19", "24"},
            {"19", "25", "19", "25"},
            {"20", "11", "20", "11"},
            {"20", "12", "20", "12"},
            {"20", "13", "20", "13"},
            {"20", "14", "20", "14"},
            {"20", "15", "20", "15"},
            {"20", "16", "20", "16"},
            {"20", "17", "20", "17"},
            {"20", "18", "20", "18"},
            {"20", "19", "20", "19"},
            {"20", "20", "20", "20"},
            {"20", "21", "20", "21"},
            {"20", "22", "20", "22"},
            {"20", "23", "20", "23"},
            {"20", "24", "20", "24"},
            {"20", "25", "20", "25"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 702 - LEFT JOIN a table with a LJ view

        rs = st.executeQuery(
            "SELECT T1.charcol, T2.charcol, T2.smintcol, T2.intcol"
            + "  FROM K55ADMIN.manytypes T1 LEFT JOIN K55ADMIN.LJview1 T2"
            + "    ON T1.charcol = T2.charcol"
            + "  WHERE T2.smintcol BETWEEN 5 AND 15"
            + "  ORDER BY 3,4");

        expColNames = new String [] {"CHARCOL", "CHARCOL", "SMINTCOL", "INTCOL"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Five", "Five", "5", null},
            {"Six", "Six", "6", null},
            {"Seven", "Seven", "7", null},
            {"Eight", "Eight", "8", null},
            {"Nine", "Nine", "9", null},
            {"Ten", "Ten", "10", null},
            {"Eleven", "Eleven", "11", "11"},
            {"Twelve", "Twelve", "12", "12"},
            {"Thirteen", "Thirteen", "13", "13"},
            {"Fourteen", "Fourteen", "14", "14"},
            {"Fifteen", "Fifteen", "15", "15"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 703 - RIGHT JOIN K55ADMIN.a table with a LJ view

        rs = st.executeQuery(
            "SELECT T1.charcol, T2.charcol, T2.smintcol, T2.intcol"
            + "  FROM K55ADMIN.manytypes_ctrl T1 RIGHT JOIN "
            + "K55ADMIN.LJview1 T2"
            + "    ON T1.charcol = T2.charcol"
            + "  WHERE T2.smintcol BETWEEN 5 AND 15 OR T2.smintcol IS NULL"
            + "  ORDER BY 3, 4, 1");

        expColNames = new String [] {"CHARCOL", "CHARCOL", "SMINTCOL", "INTCOL"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {null, "Five", "5", null},
            {"Six", "Six", "6", null},
            {null, "Seven", "7", null},
            {"Eight", "Eight", "8", null},
            {null, "Nine", "9", null},
            {"Ten", "Ten", "10", null},
            {null, "Eleven", "11", "11"},
            {"Twelve", "Twelve", "12", "12"},
            {null, "Thirteen", "13", "13"},
            {"Fourteen", "Fourteen", "14", "14"},
            {null, "Fifteen", "15", "15"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 704 - Apply Aggregate function on RJ view

        rs = st.executeQuery(
            "SELECT intcol, COUNT(*)"
            + "  FROM K55ADMIN.RJview1"
            + "  GROUP BY intcol"
            + "  ORDER BY intcol");

        expColNames = new String [] {"INTCOL", "2"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"11", "1"},
            {"12", "1"},
            {"13", "1"},
            {"14", "1"},
            {"15", "1"},
            {"16", "1"},
            {"17", "1"},
            {"18", "1"},
            {"19", "1"},
            {"20", "1"},
            {null, "10"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 705 - Apply Aggregate function on LJ view

        rs = st.executeQuery(
            "SELECT intcol, COUNT(*), COUNT(DISTINCT intcol)"
            + "  FROM K55ADMIN.LJview2"
            + "  GROUP BY intcol");

        expColNames = new String [] {"INTCOL", "2", "3"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"11", "1", "1"},
            {"12", "1", "1"},
            {"13", "1", "1"},
            {"14", "1", "1"},
            {"15", "1", "1"},
            {"16", "1", "1"},
            {"17", "1", "1"},
            {"18", "1", "1"},
            {"19", "1", "1"},
            {"20", "1", "1"},
            {null, "10", "0"}
        };

        wrnRS = new String []
        {
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            "01003",
        };

        JDBC.assertFullResultSet(rs, expRS, wrnRS);


        // 706 - RIGHT JOIN a LJ view with a table

        rs = st.executeQuery(
            "SELECT T1.smintcol, T2.smintcol, T1.intcol, T2.intcol"
            + "  FROM K55ADMIN.LJview2 T1 RIGHT JOIN "
            + "K55ADMIN.manytypes_notnull T2"
            + "    ON T1.smintcol = T2.smintcol"
            + "  WHERE T1.intcol IS NOT NULL"
            + "  ORDER BY 1, 3");

        expColNames = new String [] {"SMINTCOL", "SMINTCOL", "INTCOL", "INTCOL"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"11", "11", "11", "11"},
            {"12", "12", "12", "12"},
            {"13", "13", "13", "13"},
            {"14", "14", "14", "14"},
            {"15", "15", "15", "15"},
            {"16", "16", "16", "16"},
            {"17", "17", "17", "17"},
            {"18", "18", "18", "18"},
            {"19", "19", "19", "19"},
            {"20", "20", "20", "20"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 707 - RIGHT join a RJ view with a table

        rs = st.executeQuery(
            "SELECT T1.charcol, T2.charcol, T2.smintcol, T2.intcol"
            + "  FROM K55ADMIN.RJview1 T2 RIGHT JOIN K55ADMIN.manytypes T1"
            + "    ON T1.charcol = T2.charcol"
            + "  WHERE T2.smintcol BETWEEN 5 AND 15"
            + "  ORDER BY 3, 4");

        expColNames = new String [] {"CHARCOL", "CHARCOL", "SMINTCOL", "INTCOL"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Five", "Five", "5", null},
            {"Six", "Six", "6", null},
            {"Seven", "Seven", "7", null},
            {"Eight", "Eight", "8", null},
            {"Nine", "Nine", "9", null},
            {"Ten", "Ten", "10", null},
            {"Eleven", "Eleven", "11", "11"},
            {"Twelve", "Twelve", "12", "12"},
            {"Thirteen", "Thirteen", "13", "13"},
            {"Fourteen", "Fourteen", "14", "14"},
            {"Fifteen", "Fifteen", "15", "15"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 708 - LEFT join a table with a RJ view

        rs = st.executeQuery(
            "SELECT T1.charcol, T2.charcol, T2.smintcol, T2.intcol"
            + "  FROM K55ADMIN.manytypes_ctrl T1 LEFT JOIN "
            + "K55ADMIN.RJview1 T2"
            + "    ON T1.charcol = T2.charcol"
            + "  WHERE T2.smintcol BETWEEN 5 AND 15 OR T2.smintcol IS NULL"
            + "  ORDER BY 3, 4, 1");

        expColNames = new String [] {"CHARCOL", "CHARCOL", "SMINTCOL", "INTCOL"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"Six", "Six", "6", null},
            {"Eight", "Eight", "8", null},
            {"Ten", "Ten", "10", null},
            {"Twelve", "Twelve", "12", "12"},
            {"Fourteen", "Fourteen", "14", "14"},
            {"Twenty Four", null, null, null},
            {"Twenty Two", null, null, null}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 709 - Apply Aggregate function on RJ view with GROUP BY
        // clause

        rs = st.executeQuery(
            "SELECT intcol, COUNT(*),"
            + "       MAX(intcol),"
            + "       MIN(intcol),"
            + "       SUM(intcol),"
            + "       AVG(intcol)"
            + "  FROM K55ADMIN.RJview1"
            + "  GROUP BY intcol"
            + "  ORDER BY intcol");

        expColNames = new String [] {"INTCOL", "2", "3", "4", "5", "6"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"11", "1", "11", "11", "11", "11"},
            {"12", "1", "12", "12", "12", "12"},
            {"13", "1", "13", "13", "13", "13"},
            {"14", "1", "14", "14", "14", "14"},
            {"15", "1", "15", "15", "15", "15"},
            {"16", "1", "16", "16", "16", "16"},
            {"17", "1", "17", "17", "17", "17"},
            {"18", "1", "18", "18", "18", "18"},
            {"19", "1", "19", "19", "19", "19"},
            {"20", "1", "20", "20", "20", "20"},
            {null, "10", null, null, null, null}
        };

        wrnRS = new String []
        {
            "01003",
            "01003",
            "01003",
            "01003",
            "01003",
            "01003",
            "01003",
            "01003",
            "01003",
            "01003",
            "01003",
        };

        JDBC.assertFullResultSet(rs, expRS, wrnRS);

        // 710 - Apply Aggregate function on RJ view with WHERE
        // clause

        rs = st.executeQuery(
            "SELECT intcol, COUNT(*), COUNT(DISTINCT intcol)"
            + "  FROM K55ADMIN.RJview1"
            + "  WHERE (smintcol / 2) * 2 = smintcol"
            + "  GROUP BY intcol"
            + "  ORDER BY intcol");

        expColNames = new String [] {"INTCOL", "2", "3"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"12", "1", "1"},
            {"14", "1", "1"},
            {"16", "1", "1"},
            {"18", "1", "1"},
            {"20", "1", "1"},
            {null, "5", "0"},
        };

        JDBC.assertFullResultSet(rs, expRS);


        // 711 - LEFT join a RJ view with a LJ view - join on
        // multiple columns

        rs = st.executeQuery(
            "SELECT T1.smintcol, T1.charcol, substr(T1.vcharcol,1,15),"
            + "       T2.smintcol, T2.charcol, substr(T2.vcharcol,1,15)"
            + "  FROM K55ADMIN.RJview2 T1 LEFT JOIN K55ADMIN.LJview2 T2"
            + "    ON T1.smintcol = T2.smintcol AND T1.charcol = T2.charcol"
            + "  WHERE LENGTH(T1.vcharcol) > 6"
            + "  ORDER BY 1, 4");

        expColNames = new String [] {"SMINTCOL", "CHARCOL", "3", "SMINTCOL", "CHARCOL", "6"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"14", "Fourteen", "Fourteen", "14", "Fourteen", "Fourteen"},
            {"16", "Sixteen", "Sixteen", "16", "Sixteen", "Sixteen"},
            {"18", "Eighteen", "Eighteen", "18", "Eighteen", "Eighteen"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 712 - RIGHT join a RJ view with a LJ view - join on
        // multiple columns

        rs = st.executeQuery(
            "SELECT T1.smintcol, T1.charcol, substr(T1.vcharcol,1,15),"
            + "       T2.smintcol, T2.charcol, substr(T2.vcharcol,1,15)"
            + "  FROM K55ADMIN.RJview2 T1 RIGHT JOIN K55ADMIN.LJview2 T2"
            + "    ON T1.smintcol = T2.smintcol AND T1.charcol = T2.charcol"
            + "  WHERE LENGTH(T1.vcharcol) = 6"
            + "  ORDER BY 1, 4");

        expColNames = new String [] {"SMINTCOL", "CHARCOL", "3", "SMINTCOL", "CHARCOL", "6"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"12", "Twelve", "Twelve", "12", "Twelve", "Twelve"},
            {"20", "Twenty", "Twenty", "20", "Twenty", "Twenty"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 713 - LEFT join a RJ view with a RJ view - join on
        // multiple columns

        rs = st.executeQuery(
            "SELECT T1.smintcol, T1.charcol, substr(T1.vcharcol,1,15),"
            + "       T2.smintcol, T2.charcol, substr(T2.vcharcol,1,15)"
            + "  FROM K55ADMIN.RJview2 T1 LEFT JOIN K55ADMIN.RJview2 T2"
            + "    ON T1.smintcol = T2.smintcol AND T1.charcol = T2.charcol"
            + "  WHERE LENGTH(T1.vcharcol) >= 6"
            + "  ORDER BY 1, 4");

        expColNames = new String [] {"SMINTCOL", "CHARCOL", "3", "SMINTCOL", "CHARCOL", "6"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"12", "Twelve", "Twelve", "12", "Twelve", "Twelve"},
            {"14", "Fourteen", "Fourteen", "14", "Fourteen", "Fourteen"},
            {"16", "Sixteen", "Sixteen", "16", "Sixteen", "Sixteen"},
            {"18", "Eighteen", "Eighteen", "18", "Eighteen", "Eighteen"},
            {"20", "Twenty", "Twenty", "20", "Twenty", "Twenty"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 714 - RIGHT join an IJ view with a RJ view  - join on
        // multiple columns

        rs = st.executeQuery(
            "SELECT T1.smintcol, T1.charcol, substr(T1.vcharcol,1,15),"
            + "       T2.smintcol, T2.charcol, substr(T2.vcharcol,1,15)"
            + "  FROM K55ADMIN.IJview1 T1 RIGHT JOIN K55ADMIN.RJview2 T2"
            + "    ON T1.smintcol = T2.smintcol AND T1.charcol = T2.charcol"
            + "  WHERE LENGTH(T1.vcharcol) > 6"
            + "  ORDER BY 1, 4");

        expColNames = new String [] {"SMINTCOL", "CHARCOL", "3", "SMINTCOL", "CHARCOL", "6"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"14", "Fourteen", "Fourteen", "14", "Fourteen", "Fourteen"},
            {"16", "Sixteen", "Sixteen", "16", "Sixteen", "Sixteen"},
            {"18", "Eighteen", "Eighteen", "18", "Eighteen", "Eighteen"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 715 - INNER join a RJ view with a LJ view - join on
        // multiple columns

        rs = st.executeQuery(
            "SELECT T1.smintcol, T1.charcol, substr(T1.vcharcol,1,15),"
            + "       T2.smintcol, T2.charcol, substr(T2.vcharcol,1,15)"
            + "  FROM K55ADMIN.RJview2 T1 INNER JOIN K55ADMIN.LJview2 T2"
            + "    ON T1.smintcol = T2.smintcol AND T1.charcol = T2.charcol"
            + "  WHERE LENGTH(T1.vcharcol) >= 6"
            + "  ORDER BY 1, 4");

        expColNames = new String [] {"SMINTCOL", "CHARCOL", "3", "SMINTCOL", "CHARCOL", "6"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"12", "Twelve", "Twelve", "12", "Twelve", "Twelve"},
            {"14", "Fourteen", "Fourteen", "14", "Fourteen", "Fourteen"},
            {"16", "Sixteen", "Sixteen", "16", "Sixteen", "Sixteen"},
            {"18", "Eighteen", "Eighteen", "18", "Eighteen", "Eighteen"},
            {"20", "Twenty", "Twenty", "20", "Twenty", "Twenty"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 716 - INNER join an IJ view with a RJ view - join on
        // multiple columns

        rs = st.executeQuery(
            "SELECT T1.smintcol, T1.charcol, substr(T1.vcharcol,1,15),"
            + "       T2.smintcol, T2.charcol, substr(T2.vcharcol,1,15)"
            + "  FROM K55ADMIN.IJview1 T1 INNER JOIN K55ADMIN.RJview2 T2"
            + "    ON T1.smintcol = T2.smintcol AND T1.charcol = T2.charcol"
            + "  WHERE LENGTH(T1.vcharcol) >= 6"
            + "  ORDER BY 1, 4");

        expColNames = new String [] {"SMINTCOL", "CHARCOL", "3", "SMINTCOL", "CHARCOL", "6"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"12", "Twelve", "Twelve", "12", "Twelve", "Twelve"},
            {"14", "Fourteen", "Fourteen", "14", "Fourteen", "Fourteen"},
            {"16", "Sixteen", "Sixteen", "16", "Sixteen", "Sixteen"},
            {"18", "Eighteen", "Eighteen", "18", "Eighteen", "Eighteen"},
            {"20", "Twenty", "Twenty", "20", "Twenty", "Twenty"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 722 - Apply Aggregate function on RJ view with GROUP BY
        // clause

        rs = st.executeQuery(
            "SELECT intcol, COUNT(*),"
            + "       MAX(intcol),"
            + "       MIN(intcol),"
            + "       SUM(intcol),"
            + "       AVG(intcol)"
            + "  FROM K55ADMIN.RJview1"
            + "  GROUP BY intcol"
            + "  ORDER BY intcol");

        expColNames = new String [] {"INTCOL", "2", "3", "4", "5", "6"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"11", "1", "11", "11", "11", "11"},
            {"12", "1", "12", "12", "12", "12"},
            {"13", "1", "13", "13", "13", "13"},
            {"14", "1", "14", "14", "14", "14"},
            {"15", "1", "15", "15", "15", "15"},
            {"16", "1", "16", "16", "16", "16"},
            {"17", "1", "17", "17", "17", "17"},
            {"18", "1", "18", "18", "18", "18"},
            {"19", "1", "19", "19", "19", "19"},
            {"20", "1", "20", "20", "20", "20"},
            {null, "10", null, null, null, null}
        };

        JDBC.assertFullResultSet(rs, expRS);


        // 724 - Apply Aggregate function on RJ view with WHERE
        // clause

        rs = st.executeQuery(
            "SELECT intcol, COUNT(*), COUNT(DISTINCT intcol)"
            + "  FROM K55ADMIN.RJview1"
            + "  WHERE (smintcol / 2) * 2 = smintcol"
            + "  GROUP BY intcol"
            + "  ORDER BY intcol");

        expColNames = new String [] {"INTCOL", "2", "3"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"12", "1", "1"},
            {"14", "1", "1"},
            {"16", "1", "1"},
            {"18", "1", "1"},
            {"20", "1", "1"},
            {null, "5", "0"}
        };

        JDBC.assertFullResultSet(rs, expRS);


        // 804 - LEFT JOIN a table with a LJ view

        rs = st.executeQuery(
            "SELECT intcol, charcol FROM K55ADMIN.manytypes_ctrl"
            + "  WHERE charcol in ("
            + "    SELECT T1.charcol"
            + "      FROM K55ADMIN.manytypes T1 LEFT JOIN "
            + "K55ADMIN.LJview1 T2"
            + "        ON T1.charcol = T2.charcol"
            + "      WHERE T2.smintcol BETWEEN 5 AND 15)"
            + "  ORDER BY 1");

        expColNames = new String [] {"INTCOL", "CHARCOL"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"6", "Six"},
            {"8", "Eight"},
            {"10", "Ten"},
            {"12", "Twelve"},
            {"14", "Fourteen"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 805 - RIGHT JOIN a LJ view with a table

        rs = st.executeQuery(
            "SELECT intcol, charcol FROM K55ADMIN.manytypes_ctrl"
            + "  WHERE smintcol in ("
            + "    SELECT T1.smintcol"
            + "      FROM K55ADMIN.LJview2 T1 RIGHT JOIN "
            + "K55ADMIN.manytypes_notnull T2"
            + "        ON T1.smintcol = T2.smintcol"
            + "      WHERE T1.intcol IS NOT NULL)"
            + "  ORDER BY 1");

        expColNames = new String [] {"INTCOL", "CHARCOL"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"12", "Twelve"},
            {"14", "Fourteen"},
            {"16", "Sixteen"},
            {"18", "Eighteen"},
            {"20", "Twenty"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 806 - LEFT join a RJ view with a LJ view - join on
        // multiple columns

        rs = st.executeQuery(
            "SELECT intcol, charcol FROM K55ADMIN.manytypes_ctrl"
            + "  WHERE smintcol > SOME ("
            + "    SELECT T1.smintcol"
            + "      FROM K55ADMIN.RJview2 T1 LEFT JOIN K55ADMIN.LJview2 T2"
            + "        ON T1.smintcol = T2.smintcol AND T1.charcol "
            + "= T2.charcol"
            + "      WHERE LENGTH(T1.vcharcol) > 6)"
            + "  ORDER BY 1");

        expColNames = new String [] {"INTCOL", "CHARCOL"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"16", "Sixteen"},
            {"18", "Eighteen"},
            {"20", "Twenty"},
            {"22", "Twenty Two"},
            {"24", "Twenty Four"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 807 - RIGHT join a RJ view with a LJ view - join on
        // multiple columns

        rs = st.executeQuery(
            "SELECT intcol, charcol FROM K55ADMIN.manytypes_ctrl"
            + "  WHERE smintcol > ANY ("
            + "    SELECT T1.smintcol"
            + "      FROM K55ADMIN.RJview2 T1 RIGHT JOIN "
            + "K55ADMIN.LJview2 T2"
            + "        ON T1.smintcol = T2.smintcol AND T1.charcol "
            + "= T2.charcol"
            + "      WHERE LENGTH(T1.vcharcol) = 6)"
            + "  ORDER BY 1");

        expColNames = new String [] {"INTCOL", "CHARCOL"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"14", "Fourteen"},
            {"16", "Sixteen"},
            {"18", "Eighteen"},
            {"20", "Twenty"},
            {"22", "Twenty Two"},
            {"24", "Twenty Four"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 808 - RIGHT join an IJ view with a RJ view - join on
        // multiple columns

        rs = st.executeQuery(
            "SELECT intcol, charcol FROM K55ADMIN.manytypes_ctrl"
            + "  WHERE charcol in ("
            + "    SELECT T1.charcol"
            + "      FROM K55ADMIN.IJview1 T1 RIGHT JOIN "
            + "K55ADMIN.RJview2 T2"
            + "        ON T1.smintcol = T2.smintcol AND T1.charcol "
            + "= T2.charcol"
            + "      WHERE LENGTH(T1.vcharcol) > 6)"
            + "  ORDER BY 1");

        expColNames = new String [] {"INTCOL", "CHARCOL"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"14", "Fourteen"},
            {"16", "Sixteen"},
            {"18", "Eighteen"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 809 - INNER join a RJ view with a LJ view - join on
        // multiple columns

        rs = st.executeQuery(
            "SELECT intcol, charcol FROM K55ADMIN.manytypes_ctrl"
            + "  WHERE smintcol < ALL ("
            + "    SELECT T1.smintcol"
            + "      FROM K55ADMIN.RJview2 T1 INNER JOIN "
            + "K55ADMIN.LJview2 T2"
            + "        ON T1.smintcol = T2.smintcol AND T1.charcol "
            + "= T2.charcol"
            + "      WHERE LENGTH(T1.vcharcol) >= 6)"
            + "  ORDER BY 1");

        expColNames = new String [] {"INTCOL", "CHARCOL"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"2", "Two"},
            {"4", "Four"},
            {"6", "Six"},
            {"8", "Eight"},
            {"10", "Ten"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 810 - INNER join an IJ view with a RJ view - join on
        // multiple columns

        rs = st.executeQuery(
            "SELECT intcol, charcol FROM K55ADMIN.manytypes_ctrl"
            + "  WHERE smintcol > ALL ("
            + "    SELECT T1.smintcol"
            + "      FROM K55ADMIN.IJview1 T1 INNER JOIN "
            + "K55ADMIN.RJview2 T2"
            + "        ON T1.smintcol = T2.smintcol AND T1.charcol "
            + "= T2.charcol"
            + "      WHERE LENGTH(T1.vcharcol) >= 6)"
            + "  ORDER BY 1");

        expColNames = new String [] {"INTCOL", "CHARCOL"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"22", "Twenty Two"},
            {"24", "Twenty Four"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 815 - INNER join an IJ view with a RJ view - join on
        // multiple columns

        rs = st.executeQuery(
            "SELECT intcol, charcol FROM K55ADMIN.manytypes_ctrl"
            + "  WHERE smintcol = ANY ("
            + "    SELECT T1.smintcol"
            + "      FROM K55ADMIN.IJview1 T1 LEFT JOIN K55ADMIN.RJview2 T2"
            + "        ON T1.smintcol = T2.smintcol AND T1.charcol "
            + "= T2.charcol"
            + "      WHERE LENGTH(T1.vcharcol) IN (4, 6, 8) )"
            + "  ORDER BY 1");

        expColNames = new String [] {"INTCOL", "CHARCOL"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"12", "Twelve"},
            {"14", "Fourteen"},
            {"18", "Eighteen"},
            {"20", "Twenty"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 817 - LEFT join 2 views - with 1-level correlated subquery

        rs = st.executeQuery(
            "SELECT intcol, charcol FROM K55ADMIN.manytypes_ctrl T1"
            + "  WHERE smintcol = ("
            + "    SELECT T2.smintcol + 2"
            + "      FROM K55ADMIN.IJview1 T2 LEFT JOIN K55ADMIN.LJview2 T3"
            + "             ON T2.smintcol = T3.floatcol AND "
            + "T2.charcol = T3.charcol"
            + "      WHERE T2.floatcol = T1.floatcol - 2 )"
            + "  ORDER BY 1");

        expColNames = new String [] {"INTCOL", "CHARCOL"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"14", "Fourteen"},
            {"16", "Sixteen"},
            {"18", "Eighteen"},
            {"20", "Twenty"},
            {"22", "Twenty Two"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        //
        //------------------------------------------------------------
        // test unit 9. Joins tables used in HAVING
        // clause;
        //------------------------------------------------------------

        // 904 - LEFT JOIN a table with a LJ view

        rs = st.executeQuery(
            "SELECT intcol, charcol FROM K55ADMIN.manytypes_ctrl"
            + "  GROUP BY intcol, charcol"
            + "  HAVING charcol in ("
            + "    SELECT T1.charcol"
            + "      FROM K55ADMIN.manytypes T1 LEFT JOIN "
            + "K55ADMIN.LJview1 T2"
            + "        ON T1.charcol = T2.charcol"
            + "      WHERE T2.smintcol BETWEEN 5 AND 15)"
            + "  ORDER BY 1");

        expColNames = new String [] {"INTCOL", "CHARCOL"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"6", "Six"},
            {"8", "Eight"},
            {"10", "Ten"},
            {"12", "Twelve"},
            {"14", "Fourteen"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 905 - RIGHT JOIN a LJ view with a table

        rs = st.executeQuery(
            "SELECT smintcol, charcol FROM K55ADMIN.manytypes_ctrl"
            + "  GROUP BY smintcol, charcol"
            + "  HAVING smintcol in ("
            + "    SELECT T1.smintcol"
            + "      FROM K55ADMIN.LJview2 T1 RIGHT JOIN "
            + "K55ADMIN.manytypes_notnull T2"
            + "        ON T1.smintcol = T2.smintcol"
            + "      WHERE T1.intcol IS NOT NULL)"
            + "  ORDER BY 1");

        expColNames = new String [] {"SMINTCOL", "CHARCOL"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"12", "Twelve"},
            {"14", "Fourteen"},
            {"16", "Sixteen"},
            {"18", "Eighteen"},
            {"20", "Twenty"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 906 - LEFT join a RJ view with a LJ view - join on
        // multiple columns

        rs = st.executeQuery(
            "SELECT smintcol, charcol FROM K55ADMIN.manytypes_ctrl"
            + "  GROUP BY smintcol, charcol"
            + "  HAVING smintcol > SOME ("
            + "    SELECT T1.smintcol"
            + "      FROM K55ADMIN.RJview2 T1 LEFT JOIN K55ADMIN.LJview2 T2"
            + "        ON T1.smintcol = T2.smintcol AND T1.charcol "
            + "= T2.charcol"
            + "      WHERE LENGTH(T1.vcharcol) > 6)"
            + "  ORDER BY 1");

        expColNames = new String [] {"SMINTCOL", "CHARCOL"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"16", "Sixteen"},
            {"18", "Eighteen"},
            {"20", "Twenty"},
            {"22", "Twenty Two"},
            {"24", "Twenty Four"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 907 - RIGHT join a RJ view with a LJ view - join on
        // multiple columns

        rs = st.executeQuery(
            "SELECT smintcol, charcol FROM K55ADMIN.manytypes_ctrl"
            + "  GROUP BY smintcol, charcol"
            + "  HAVING smintcol > ANY ("
            + "    SELECT T1.smintcol"
            + "      FROM K55ADMIN.RJview2 T1 RIGHT JOIN "
            + "K55ADMIN.LJview2 T2"
            + "        ON T1.smintcol = T2.smintcol AND T1.charcol "
            + "= T2.charcol"
            + "      WHERE LENGTH(T1.vcharcol) = 6)"
            + "  ORDER BY 1");

        expColNames = new String [] {"SMINTCOL", "CHARCOL"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"14", "Fourteen"},
            {"16", "Sixteen"},
            {"18", "Eighteen"},
            {"20", "Twenty"},
            {"22", "Twenty Two"},
            {"24", "Twenty Four"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 908 - RIGHT join an IJ view with a RJ view -join on
        // multiple columns

        rs = st.executeQuery(
            "SELECT intcol, charcol FROM K55ADMIN.manytypes_ctrl"
            + "  GROUP BY intcol, charcol"
            + "  HAVING charcol in ("
            + "    SELECT T1.charcol"
            + "      FROM K55ADMIN.IJview1 T1 RIGHT JOIN "
            + "K55ADMIN.RJview2 T2"
            + "        ON T1.smintcol = T2.smintcol AND T1.charcol "
            + "= T2.charcol"
            + "      WHERE LENGTH(T1.vcharcol) > 6)"
            + "  ORDER BY 1");

        expColNames = new String [] {"INTCOL", "CHARCOL"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"14", "Fourteen"},
            {"16", "Sixteen"},
            {"18", "Eighteen"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 909 - INNER join a RJ view with a LJ view - join on
        // multiple columns

        rs = st.executeQuery(
            "SELECT smintcol, charcol FROM K55ADMIN.manytypes_ctrl"
            + "  GROUP BY smintcol, charcol"
            + "  HAVING smintcol < ALL ("
            + "    SELECT T1.smintcol"
            + "      FROM K55ADMIN.RJview2 T1 INNER JOIN "
            + "K55ADMIN.LJview2 T2"
            + "        ON T1.smintcol = T2.smintcol AND T1.charcol "
            + "= T2.charcol"
            + "      WHERE LENGTH(T1.vcharcol) >= 6)"
            + "  ORDER BY 1");

        expColNames = new String [] {"SMINTCOL", "CHARCOL"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"2", "Two"},
            {"4", "Four"},
            {"6", "Six"},
            {"8", "Eight"},
            {"10", "Ten"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 910 - INNER join an IJ view with a RJ view - join on
        // multiple columns

        rs = st.executeQuery(
            "SELECT smintcol, charcol FROM K55ADMIN.manytypes_ctrl"
            + "  GROUP BY smintcol, charcol"
            + "  HAVING smintcol > ALL ("
            + "    SELECT T1.smintcol"
            + "      FROM K55ADMIN.IJview1 T1 INNER JOIN "
            + "K55ADMIN.RJview2 T2"
            + "        ON T1.smintcol = T2.smintcol AND T1.charcol "
            + "= T2.charcol"
            + "      WHERE LENGTH(T1.vcharcol) >= 6)"
            + "  ORDER BY 1");

        expColNames = new String [] {"SMINTCOL", "CHARCOL"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"22", "Twenty Two"},
            {"24", "Twenty Four"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 915 - LEFT join 2 views - with 1-level correlated subquery

        rs = st.executeQuery(
            "SELECT smintcol, charcol FROM K55ADMIN.manytypes_ctrl T1"
            + "  GROUP BY smintcol, charcol"
            + "  HAVING smintcol = ("
            + "    SELECT T2.smintcol - 1"
            + "      FROM K55ADMIN.IJview1 T2 LEFT JOIN K55ADMIN.LJview2 T3"
            + "             ON T2.smintcol = T3.floatcol AND "
            + "T2.charcol = T3.charcol"
            + "      WHERE T2.floatcol = T1.smintcol + 1 )"
            + "  ORDER BY 1");

        expColNames = new String [] {"SMINTCOL", "CHARCOL"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"10", "Ten"},
            {"12", "Twelve"},
            {"14", "Fourteen"},
            {"16", "Sixteen"},
            {"18", "Eighteen"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 919 - LEFT join 2 views - with 2-level correlated subquery

        rs = st.executeQuery(
            "SELECT smintcol, dec62col, charcol FROM "
            + "K55ADMIN.manytypes_ctrl T1"
            + "  GROUP BY smintcol, dec62col, charcol"
            + "  HAVING smintcol = ("
            + "    SELECT T2.smintcol "
            + "      FROM K55ADMIN.IJview1 T2 INNER JOIN "
            + "K55ADMIN.LJview2 T3"
            + "             ON T2.smintcol = T3.floatcol AND "
            + "T2.charcol = T3.charcol"
            + "      WHERE T3.charcol = T1.charcol AND"
            + "            T2.floatcol = ("
            + "              SELECT T4.smintcol"
            + "                FROM K55ADMIN.LJview2 T4 LEFT JOIN "
            + "K55ADMIN.RJview1 T5"
            + "                       ON T4.dec62col = T5.floatcol"
            + "                WHERE T5.floatcol = T1.dec62col ) )"
            + "  ORDER BY 1");

        expColNames = new String [] {"SMINTCOL", "DEC62COL", "CHARCOL"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"12", "12.00", "Twelve"},
            {"14", "14.00", "Fourteen"},
            {"16", "16.00", "Sixteen"},
            {"18", "18.00", "Eighteen"},
            {"20", "20.00", "Twenty"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        //
        //------------------------------------------------------------
        // test unit 10. Joins tables used in INSERT,
        // UPDATE or DELETE subquery;
        //------------------------------------------------------------

        // 1001 - Create table needed for the test

        st.executeUpdate(
            "CREATE TABLE k55admin.iud_tbl (c1 int,"
            + "                      c2 char(15),"
            + "                      c3 char(15))");

        // 1011 - LEFT join a RJ view with a LJ view - join on
        // multiple columns

        st.executeUpdate(
            "INSERT INTO k55admin.iud_tbl"
            + "    SELECT t1.intcol, t1.charcol, t2.vcharcol"
            + "      FROM K55ADMIN.RJview2 T1 LEFT JOIN K55ADMIN.LJview2 T2"
            + "        ON T1.smintcol = T2.smintcol AND T1.charcol "
            + "= T2.charcol"
            + "      WHERE LENGTH(T1.vcharcol) > 6");

        rs = st.executeQuery(
            " SELECT * FROM K55ADMIN.iud_tbl ORDER BY c1");

        expColNames = new String [] {"C1", "C2", "C3"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"14", "Fourteen", "Fourteen"},
            {"16", "Sixteen", "Sixteen"},
            {"18", "Eighteen", "Eighteen"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 1012 - UPDATE rows inserted in previous statement

        assertUpdateCount(st, 2,
            "UPDATE k55admin.iud_tbl"
            + "  SET c2 = NULL"
            + "  WHERE C1 < ANY ("
            + "    SELECT T1.smintcol"
            + "      FROM K55ADMIN.RJview2 T1 LEFT JOIN K55ADMIN.LJview2 T2"
            + "        ON T1.smintcol = T2.smintcol AND T1.charcol "
            + "= T2.charcol"
            + "      WHERE LENGTH(T1.vcharcol) > 6)");

        rs = st.executeQuery(
            " SELECT * FROM K55ADMIN.iud_tbl ORDER BY c1");

        expColNames = new String [] {"C1", "C2", "C3"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"14", null, "Fourteen"},
            {"16", null, "Sixteen"},
            {"18", "Eighteen", "Eighteen"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 1013 - DELETE rows inserted in previous statement

        assertUpdateCount(st, 3,
            "DELETE FROM K55ADMIN.iud_tbl"
            + "  WHERE c1 = ANY ("
            + "    SELECT T1.smintcol"
            + "      FROM K55ADMIN.RJview2 T1 LEFT JOIN K55ADMIN.LJview2 T2"
            + "        ON T1.smintcol = T2.smintcol AND T1.charcol "
            + "= T2.charcol"
            + "      WHERE LENGTH(T1.vcharcol) > 6)");

        rs = st.executeQuery(
            " SELECT * FROM K55ADMIN.iud_tbl ORDER BY c1");

        expColNames = new String [] {"C1", "C2", "C3"};
        JDBC.assertColumnNames(rs, expColNames);
        JDBC.assertDrainResults(rs, 0);

        // 1014 - RIGHT join an IJ view with a RJ view - join on
        // multiple columns

        st.executeUpdate(
            "INSERT INTO k55admin.iud_tbl"
            + "    SELECT t1.intcol, t1.charcol, t2.vcharcol"
            + "      FROM K55ADMIN.IJview1 T1 RIGHT JOIN "
            + "K55ADMIN.RJview2 T2"
            + "        ON T1.smintcol = T2.smintcol AND T1.charcol "
            + "= T2.charcol"
            + "      WHERE LENGTH(T1.charcol) > 6");

        rs = st.executeQuery(
            " SELECT * FROM K55ADMIN.iud_tbl ORDER BY c1");

        expColNames = new String [] {"C1", "C2", "C3"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"12", "Twelve", "Twelve"},
            {"14", "Fourteen", "Fourteen"},
            {"16", "Sixteen", "Sixteen"},
            {"18", "Eighteen", "Eighteen"},
            {"20", "Twenty", "Twenty"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 1015 - UPDATE rows inserted in previous statement

        assertUpdateCount(st, 3,
            "UPDATE k55admin.iud_tbl"
            + "  SET c2 = NULL"
            + "  WHERE C1 < ANY ("
            + "    SELECT T1.intcol"
            + "      FROM K55ADMIN.IJview1 T1 RIGHT JOIN "
            + "K55ADMIN.RJview2 T2"
            + "        ON T1.smintcol = T2.smintcol AND T1.charcol "
            + "= T2.charcol"
            + "      WHERE LENGTH(T1.vcharcol) > 6)");

        rs = st.executeQuery(
            " SELECT * FROM K55ADMIN.iud_tbl ORDER BY c1");

        expColNames = new String [] {"C1", "C2", "C3"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"12", null, "Twelve"},
            {"14", null, "Fourteen"},
            {"16", null, "Sixteen"},
            {"18", "Eighteen", "Eighteen"},
            {"20", "Twenty", "Twenty"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 1016 - DELETE rows inserted in previous statement

        assertUpdateCount(st, 3,
            "DELETE FROM K55ADMIN.iud_tbl"
            + "  WHERE c1 = ANY ("
            + "    SELECT T1.intcol"
            + "      FROM K55ADMIN.IJview1 T1 RIGHT JOIN "
            + "K55ADMIN.RJview2 T2"
            + "        ON T1.smintcol = T2.smintcol AND T1.charcol "
            + "= T2.charcol"
            + "      WHERE LENGTH(T1.vcharcol) > 6)");

        rs = st.executeQuery(
            " SELECT * FROM K55ADMIN.iud_tbl ORDER BY c1");

        expColNames = new String [] {"C1", "C2", "C3"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"12", null, "Twelve"},
            {"20", "Twenty", "Twenty"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 1017 - Clean up table iud_tbl

        assertUpdateCount(st, 2,
            "DELETE FROM K55ADMIN.iud_tbl");

        rs = st.executeQuery(
            " SELECT * FROM K55ADMIN.iud_tbl ORDER BY c3");

        expColNames = new String [] {"C1", "C2", "C3"};
        JDBC.assertColumnNames(rs, expColNames);
        JDBC.assertDrainResults(rs, 0);

        // 1019 - INNER join an IJ view with a RJ view - join on
        // multiple columns

        st.executeUpdate(
            "INSERT INTO k55admin.iud_tbl"
            + "    SELECT t1.intcol, t1.charcol, t2.vcharcol"
            + "      FROM K55ADMIN.IJview1 T1 INNER JOIN "
            + "K55ADMIN.RJview2 T2"
            + "        ON T1.smintcol = T2.smintcol AND T1.charcol "
            + "= T2.charcol"
            + "      WHERE LENGTH(T1.vcharcol) >= 6");

        rs = st.executeQuery(
            " SELECT * FROM K55ADMIN.iud_tbl ORDER BY c1, c2");

        expColNames = new String [] {"C1", "C2", "C3"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"12", "Twelve", "Twelve"},
            {"14", "Fourteen", "Fourteen"},
            {"16", "Sixteen", "Sixteen"},
            {"18", "Eighteen", "Eighteen"},
            {"20", "Twenty", "Twenty"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 1021 - UPDATE rows inserted in previous statement

        assertUpdateCount(st, 4,
            "UPDATE k55admin.iud_tbl"
            + "  SET c2 = NULL"
            + "  WHERE C1 > ANY ("
            + "    SELECT T1.smintcol"
            + "      FROM K55ADMIN.IJview1 T1 INNER JOIN "
            + "K55ADMIN.RJview2 T2"
            + "        ON T1.smintcol = T2.smintcol AND T1.charcol "
            + "= T2.charcol"
            + "      WHERE LENGTH(T1.vcharcol) >= 6)");

        rs = st.executeQuery(
            " SELECT * FROM K55ADMIN.iud_tbl ORDER BY c1, c2");

        expColNames = new String [] {"C1", "C2", "C3"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"12", "Twelve", "Twelve"},
            {"14", null, "Fourteen"},
            {"16", null, "Sixteen"},
            {"18", null, "Eighteen"},
            {"20", null, "Twenty"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 1023 - DELETE rows inserted in previous statement

        assertUpdateCount(st, 5,
            "DELETE FROM K55ADMIN.iud_tbl"
            + "  WHERE c1 = ANY ("
            + "    SELECT T1.smintcol"
            + "      FROM K55ADMIN.IJview1 T1 INNER JOIN "
            + "K55ADMIN.RJview2 T2"
            + "        ON T1.smintcol = T2.smintcol AND T1.charcol "
            + "= T2.charcol"
            + "      WHERE LENGTH(T1.vcharcol) >= 6)");

        rs = st.executeQuery(
            " SELECT * FROM K55ADMIN.iud_tbl ORDER BY c1");

        expColNames = new String [] {"C1", "C2", "C3"};
        JDBC.assertColumnNames(rs, expColNames);
        JDBC.assertDrainResults(rs, 0);

        //
        //------------------------------------------------------------
        // test unit 11. Joins tables used in INSERT,
        // UPDATE or DELETE subquery. tables have ;
        //------------------------------------------------------------

        // 1102 - Create tables and populate data
        // needed for the test

        st.executeUpdate(
            "CREATE TABLE k55admin.iud_tbl_P (c1 float,"
            + "                        c2 char(15),"
            + "                        c3 char(15)) ");

        st.executeUpdate(
            " CREATE TABLE k55admin.MANYTYPES_P"
            + "      (INTCOL        INTEGER,"
            + "       SMINTCOL      SMALLINT,"
            + "       DEC62COL      DECIMAL(6,2),"
            + "       DEC72COL      DECIMAL(7,2),"
            + "       FLOATCOL      FLOAT,"
            + "       CHARCOL       CHAR(10),"
            + "       LCHARCOL      CHAR(250),"
            + "       VCHARCOL      VARCHAR(100)) ");

        st.executeUpdate(
            " CREATE TABLE k55admin.MTYPES_NOTNULL_P"
            + "      (INTCOL        INTEGER       NOT NULL,"
            + "       SMINTCOL      SMALLINT      NOT NULL,"
            + "       DEC62COL      DECIMAL(6,2)  NOT NULL,"
            + "       DEC72COL      DECIMAL(7,2)  NOT NULL,"
            + "       FLOATCOL      FLOAT         NOT NULL,"
            + "       CHARCOL       CHAR(15)      NOT NULL,"
            + "       LCHARCOL      CHAR(250)     NOT NULL,"
            + "       VCHARCOL      VARCHAR(100)  NOT NULL) ");

        st.executeUpdate(
            " CREATE TABLE k55admin.MTYPES_CTRL_P"
            + "      (INTCOL        INTEGER       NOT NULL,"
            + "       SMINTCOL      SMALLINT      NOT NULL,"
            + "       DEC62COL      DECIMAL(6,2)  NOT NULL,"
            + "       DEC72COL      DECIMAL(7,2)  NOT NULL,"
            + "       FLOATCOL      FLOAT         NOT NULL,"
            + "       CHARCOL       CHAR(15)      NOT NULL,"
            + "       LCHARCOL      CHAR(250)     NOT NULL,"
            + "       VCHARCOL      VARCHAR(100)  NOT NULL) ");

        st.executeUpdate(
            " INSERT INTO K55ADMIN.manytypes_P SELECT * from "
            + "k55admin.manytypes");

        st.executeUpdate(
            " INSERT INTO K55ADMIN.mtypes_notnull_P SELECT * "
            + " from k55admin.manytypes_notnull");

        st.executeUpdate(
            " INSERT INTO K55ADMIN.mtypes_ctrl_P SELECT * from "
            + "k55admin.manytypes_ctrl");

        // 1104- LEFT join a table with a LEFT join of a RIGHT
        // join of two LEFT joins into that same table

        st.executeUpdate(
            "INSERT INTO K55ADMIN.iud_tbl_P"
            + "  SELECT t1.floatcol, t1.charcol, t6.vcharcol"
            + "    FROM K55ADMIN.mtypes_ctrl_P T6"
            + "       LEFT JOIN"
            + "        ((k55admin.manytypes_P T1 LEFT JOIN "
            + "K55ADMIN.mtypes_notnull_P T2"
            + "              ON T1.intcol = T2.dec62col)"
            + "         RIGHT JOIN"
            + "           (k55admin.mtypes_notnull_P T4 LEFT JOIN "
            + "K55ADMIN.manytypes_P T3"
            + "                ON T4.smintcol = T3.dec62col)"
            + "         ON T1.intcol = T4.smintcol"
            + "         LEFT JOIN"
            + "          k55admin.mtypes_ctrl_P T5"
            + "        ON T5.intcol = T1.smintcol)"
            + "      ON T6.intcol = T1.smintcol"
            + "    WHERE t1.intcol IS NOT NULL");

        rs = st.executeQuery(
            " SELECT * FROM K55ADMIN.iud_tbl_P ORDER BY c1");

        expColNames = new String [] {"C1", "C2", "C3"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"12.0", "Twelve", "Twelve"},
            {"14.0", "Fourteen", "Fourteen"},
            {"16.0", "Sixteen", "Sixteen"},
            {"18.0", "Eighteen", "Eighteen"},
            {"20.0", "Twenty", "Twenty"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 1106 - UPDATE rows inserted in previous statement

        assertUpdateCount(st, 4,
            "UPDATE K55ADMIN.iud_tbl_P"
            + "  SET c2 = NULL"
            + "  WHERE C1 > ANY ("
            + "    SELECT t1.floatcol"
            + "      FROM K55ADMIN.mtypes_ctrl_P T6"
            + "         LEFT JOIN"
            + "          (k55admin.manytypes_P T1 "
            + "           RIGHT JOIN"
            + "           k55admin.mtypes_notnull_P T4"
            + "           ON T1.floatcol = T4.smintcol"
            + "           LEFT JOIN"
            + "            k55admin.mtypes_ctrl_P T5"
            + "          ON T5.floatcol = T1.smintcol)"
            + "        ON T6.floatcol = T1.smintcol"
            + "      WHERE t1.floatcol IS NOT NULL)");

        rs = st.executeQuery(
            " SELECT * FROM K55ADMIN.iud_tbl_P ORDER BY c1");

        expColNames = new String [] {"C1", "C2", "C3"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"12.0", "Twelve", "Twelve"},
            {"14.0", null, "Fourteen"},
            {"16.0", null, "Sixteen"},
            {"18.0", null, "Eighteen"},
            {"20.0", null, "Twenty"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 1108 - DELETE rows inserted in previous statement

        assertUpdateCount(st, 5,
            "DELETE FROM K55ADMIN.iud_tbl_P"
            + "  WHERE c1 = ANY ("
            + "    SELECT t1.floatcol"
            + "      FROM K55ADMIN.mtypes_ctrl_P T6"
            + "         LEFT JOIN"
            + "          (k55admin.manytypes_P T1 "
            + "           RIGHT JOIN"
            + "           k55admin.mtypes_notnull_P T4"
            + "           ON T1.floatcol = T4.smintcol"
            + "           LEFT JOIN"
            + "            k55admin.mtypes_ctrl_P T5"
            + "          ON T5.floatcol = T1.smintcol)"
            + "        ON T6.floatcol = T1.smintcol"
            + "      WHERE t1.floatcol IS NOT NULL)");

        rs = st.executeQuery(
            " SELECT * FROM K55ADMIN.iud_tbl_P ORDER BY c1");

        expColNames = new String [] {"C1", "C2", "C3"};
        JDBC.assertColumnNames(rs, expColNames);
        JDBC.assertDrainResults(rs, 0);

        // 1116 - Clean up table iud_tbl_P

        assertUpdateCount(st, 0,
            "DELETE FROM K55ADMIN.iud_tbl_P");

        rs = st.executeQuery(
            " SELECT * FROM K55ADMIN.iud_tbl_P ORDER BY c3");

        expColNames = new String [] {"C1", "C2", "C3"};
        JDBC.assertColumnNames(rs, expColNames);
        JDBC.assertDrainResults(rs, 0);

        // 1118 - LEFT join a RJ view with a LJ view - join on
        // multiple columns

        st.executeUpdate(
            "INSERT INTO k55admin.iud_tbl_P"
            + "    SELECT t1.floatcol, t1.charcol, t2.vcharcol"
            + "      FROM K55ADMIN.RJview2 T1 LEFT JOIN K55ADMIN.LJview2 T2"
            + "        ON T1.smintcol = T2.smintcol AND T1.charcol "
            + "= T2.charcol"
            + "      WHERE LENGTH(T1.vcharcol) > 6");

        rs = st.executeQuery(
            " SELECT * FROM K55ADMIN.iud_tbl_P ORDER BY c1");

        expColNames = new String [] {"C1", "C2", "C3"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"14.0", "Fourteen", "Fourteen"},
            {"16.0", "Sixteen", "Sixteen"},
            {"18.0", "Eighteen", "Eighteen"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 1120 - UPDATE rows inserted in previous statement

        assertUpdateCount(st, 2,
            "UPDATE K55ADMIN.iud_tbl_P"
            + "  SET c2 = NULL"
            + "  WHERE C1 < ANY ("
            + "    SELECT T1.smintcol"
            + "      FROM K55ADMIN.RJview2 T1 LEFT JOIN K55ADMIN.LJview2 T2"
            + "        ON T1.smintcol = T2.smintcol AND T1.charcol "
            + "= T2.charcol"
            + "      WHERE LENGTH(T1.vcharcol) > 6)");

        rs = st.executeQuery(
            " SELECT * FROM K55ADMIN.iud_tbl_P ORDER BY c1");

        expColNames = new String [] {"C1", "C2", "C3"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"14.0", null, "Fourteen"},
            {"16.0", null, "Sixteen"},
            {"18.0", "Eighteen", "Eighteen"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 1122 - DELETE rows inserted in previous statement

        assertUpdateCount(st, 3,
            "DELETE FROM K55ADMIN.iud_tbl_P"
            + "  WHERE c1 = ANY ("
            + "    SELECT T1.smintcol"
            + "      FROM K55ADMIN.RJview2 T1 LEFT JOIN K55ADMIN.LJview2 T2"
            + "        ON T1.smintcol = T2.smintcol AND T1.charcol "
            + "= T2.charcol"
            + "      WHERE LENGTH(T1.vcharcol) > 6)");

        rs = st.executeQuery(
            " SELECT * FROM K55ADMIN.iud_tbl_P ORDER BY c1");

        expColNames = new String [] {"C1", "C2", "C3"};
        JDBC.assertColumnNames(rs, expColNames);
        JDBC.assertDrainResults(rs, 0);

        // 1124 - RIGHT join an IJ view with a RJ view - join on
        // multiple columns

        st.executeUpdate(
            "INSERT INTO K55ADMIN.iud_tbl_P"
            + "    SELECT t1.floatcol, t1.charcol, t2.vcharcol"
            + "      FROM K55ADMIN.IJview1 T1 RIGHT JOIN "
            + "K55ADMIN.RJview2 T2"
            + "        ON T1.smintcol = T2.smintcol AND T1.charcol "
            + "= T2.charcol"
            + "      WHERE LENGTH(T1.vcharcol) > 6 OR T2.charcol LIKE 'T%'");

        rs = st.executeQuery(
            " SELECT * FROM K55ADMIN.iud_tbl_P ORDER BY c1");

        expColNames = new String [] {"C1", "C2", "C3"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"12.0", "Twelve", "Twelve"},
            {"14.0", "Fourteen", "Fourteen"},
            {"16.0", "Sixteen", "Sixteen"},
            {"18.0", "Eighteen", "Eighteen"},
            {"20.0", "Twenty", "Twenty"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 1126 - UPDATE rows inserted in previous statement

        assertUpdateCount(st, 3,
            "UPDATE K55ADMIN.iud_tbl_P"
            + "  SET c2 = NULL"
            + "  WHERE C1 < ANY ("
            + "    SELECT T1.floatcol"
            + "      FROM K55ADMIN.IJview1 T1 RIGHT JOIN "
            + "K55ADMIN.RJview2 T2"
            + "        ON T1.smintcol = T2.smintcol AND T1.charcol "
            + "= T2.charcol"
            + "      WHERE LENGTH(T1.vcharcol) > 6)");

        rs = st.executeQuery(
            " SELECT * FROM K55ADMIN.iud_tbl_P ORDER BY c1");

        expColNames = new String [] {"C1", "C2", "C3"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"12.0", null, "Twelve"},
            {"14.0", null, "Fourteen"},
            {"16.0", null, "Sixteen"},
            {"18.0", "Eighteen", "Eighteen"},
            {"20.0", "Twenty", "Twenty"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 1128 - DELETE rows inserted in previous statement

        assertUpdateCount(st, 2,
            "DELETE FROM K55ADMIN.iud_tbl_P"
            + "  WHERE c1 IN ("
            + "    SELECT T1.floatcol"
            + "      FROM K55ADMIN.IJview1 T1 RIGHT JOIN "
            + "K55ADMIN.RJview2 T2"
            + "        ON T1.smintcol = T2.smintcol AND T1.charcol "
            + "= T2.charcol"
            + "      WHERE substr(T2.vcharcol, 1, 1) = 'T')");

        rs = st.executeQuery(
            " SELECT * FROM K55ADMIN.iud_tbl_P ORDER BY c1");

        expColNames = new String [] {"C1", "C2", "C3"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"14.0", null, "Fourteen"},
            {"16.0", null, "Sixteen"},
            {"18.0", "Eighteen", "Eighteen"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 1130 - Clean up table iud_tbl_P

        assertUpdateCount(st, 3,
            "DELETE FROM K55ADMIN.iud_tbl_P");

        rs = st.executeQuery(
            " SELECT * FROM K55ADMIN.iud_tbl_P ORDER BY c1");

        expColNames = new String [] {"C1", "C2", "C3"};
        JDBC.assertColumnNames(rs, expColNames);
        JDBC.assertDrainResults(rs, 0);

        // 1132 - INNER join an IJ view with a RJ view - join on
        // multiple columns

        st.executeUpdate(
            "INSERT INTO K55ADMIN.iud_tbl_P"
            + "    SELECT t1.floatcol, t1.charcol, t2.vcharcol"
            + "      FROM K55ADMIN.IJview1 T1 INNER JOIN "
            + "K55ADMIN.RJview2 T2"
            + "        ON T1.smintcol = T2.smintcol AND T1.charcol "
            + "= T2.charcol"
            + "      WHERE LENGTH(T1.vcharcol) >= 6");

        rs = st.executeQuery(
            " SELECT * FROM K55ADMIN.iud_tbl_P ORDER BY c1");

        expColNames = new String [] {"C1", "C2", "C3"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"12.0", "Twelve", "Twelve"},
            {"14.0", "Fourteen", "Fourteen"},
            {"16.0", "Sixteen", "Sixteen"},
            {"18.0", "Eighteen", "Eighteen"},
            {"20.0", "Twenty", "Twenty"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 1134 - UPDATE rows inserted in previous statement

        assertUpdateCount(st, 2,
            "UPDATE K55ADMIN.iud_tbl_P"
            + "  SET c2 = NULL"
            + "  WHERE C1 IN ("
            + "    SELECT T1.floatcol"
            + "      FROM K55ADMIN.IJview1 T1 INNER JOIN "
            + "K55ADMIN.RJview2 T2"
            + "        ON T1.smintcol = T2.smintcol AND T1.charcol "
            + "= T2.charcol"
            + "      WHERE T1.vcharcol LIKE 'T_e%')");

        rs = st.executeQuery(
            " SELECT * FROM K55ADMIN.iud_tbl_P ORDER BY c1");

        expColNames = new String [] {"C1", "C2", "C3"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"12.0", null, "Twelve"},
            {"14.0", "Fourteen", "Fourteen"},
            {"16.0", "Sixteen", "Sixteen"},
            {"18.0", "Eighteen", "Eighteen"},
            {"20.0", null, "Twenty"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // 1136 - DELETE rows inserted in previous statement

        assertUpdateCount(st, 5,
            "DELETE FROM K55ADMIN.iud_tbl_P"
            + "  WHERE C1 IN ("
            + "    SELECT T1.floatcol"
            + "      FROM K55ADMIN.IJview1 T1 INNER JOIN "
            + "K55ADMIN.RJview2 T2"
            + "        ON T1.smintcol = T2.smintcol AND T1.charcol "
            + "= T2.charcol"
            + "      WHERE T1.vcharcol NOT LIKE 'Tw%' OR "
            + "substr(T2.vcharcol, 1, 1) = 'T')");

        rs = st.executeQuery(
            " SELECT * FROM K55ADMIN.iud_tbl_P ORDER BY c1");

        expColNames = new String [] {"C1", "C2", "C3"};
        JDBC.assertColumnNames(rs, expColNames);
        JDBC.assertDrainResults(rs, 0);

        // some bugs found during development

        rs = st.executeQuery(
            "SELECT t1.floatcol"
            + "  FROM K55ADMIN.mtypes_ctrl_P T6"
            + "     LEFT JOIN"
            + "      ((k55admin.manytypes_P T1 LEFT JOIN "
            + "K55ADMIN.mtypes_notnull_P T2"
            + "            ON T1.floatcol = T2.dec62col)"
            + "       RIGHT JOIN"
            + "         (k55admin.mtypes_notnull_P T4 LEFT JOIN "
            + "K55ADMIN.manytypes_P T3"
            + "              ON T4.smintcol = T3.dec62col)"
            + "       ON T1.floatcol = T4.smintcol"
            + "       LEFT JOIN"
            + "        k55admin.mtypes_ctrl_P T5"
            + "      ON T5.floatcol = T1.smintcol)"
            + "    ON T6.floatcol = T1.smintcol"
            + " WHERE t1.floatcol IS NOT NULL");

        expColNames = new String [] {"FLOATCOL"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"12.0"},
            {"14.0"},
            {"16.0"},
            {"18.0"},
            {"20.0"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        rs = st.executeQuery(
            " SELECT *"
            + " FROM K55ADMIN.mtypes_ctrl_P T6"
            + "     LEFT JOIN ((k55admin.manytypes_P T1 LEFT JOIN "
            + "K55ADMIN.mtypes_notnull_P T2 ON T1.floatcol = T2.dec62col)"
            + "                 RIGHT JOIN "
            + "k55admin.mtypes_notnull_P T4 ON T1.floatcol = T4.smintcol)"
            + "     ON T6.floatcol = T1.smintcol");

        expColNames = new String [] {"INTCOL", "SMINTCOL", "DEC62COL", "DEC72COL", "FLOATCOL", "CHARCOL", "LCHARCOL", "VCHARCOL", "INTCOL", "SMINTCOL", "DEC62COL", "DEC72COL", "FLOATCOL", "CHARCOL", "LCHARCOL", "VCHARCOL", "INTCOL", "SMINTCOL", "DEC62COL", "DEC72COL", "FLOATCOL", "CHARCOL", "LCHARCOL", "VCHARCOL", "INTCOL", "SMINTCOL", "DEC62COL", "DEC72COL", "FLOATCOL", "CHARCOL", "LCHARCOL", "VCHARCOL"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"2", "2", "2.00", "2.00", "2.0", "Two", "Two", "Two", null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null},
            {"4", "4", "4.00", "4.00", "4.0", "Four", "Four", "Four", null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null},
            {"6", "6", "6.00", "6.00", "6.0", "Six", "Six", "Six", null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null},
            {"8", "8", "8.00", "8.00", "8.0", "Eight", "Eight", "Eight", null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null},
            {"10", "10", "10.00", "10.00", "10.0", "Ten", "Ten", "Ten", null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null},
            {"12", "12", "12.00", "12.00", "12.0", "Twelve", "Twelve", "Twelve", "12", "12", "12.00", "12.00", "12.0", "Twelve", "Twelve", "Twelve", "12", "12", "12.00", "12.00", "12.0", "Twelve", "Twelve", "Twelve", "12", "12", "12.00", "12.00", "12.0", "Twelve", "Twelve", "Twelve"},
            {"14", "14", "14.00", "14.00", "14.0", "Fourteen", "Fourteen", "Fourteen", "14", "14", "14.00", "14.00", "14.0", "Fourteen", "Fourteen", "Fourteen", "14", "14", "14.00", "14.00", "14.0", "Fourteen", "Fourteen", "Fourteen", "14", "14", "14.00", "14.00", "14.0", "Fourteen", "Fourteen", "Fourteen"},
            {"16", "16", "16.00", "16.00", "16.0", "Sixteen", "Sixteen", "Sixteen", "16", "16", "16.00", "16.00", "16.0", "Sixteen", "Sixteen", "Sixteen", "16", "16", "16.00", "16.00", "16.0", "Sixteen", "Sixteen", "Sixteen", "16", "16", "16.00", "16.00", "16.0", "Sixteen", "Sixteen", "Sixteen"},
            {"18", "18", "18.00", "18.00", "18.0", "Eighteen", "Eighteen", "Eighteen", "18", "18", "18.00", "18.00", "18.0", "Eighteen", "Eighteen", "Eighteen", "18", "18", "18.00", "18.00", "18.0", "Eighteen", "Eighteen", "Eighteen", "18", "18", "18.00", "18.00", "18.0", "Eighteen", "Eighteen", "Eighteen"},
            {"20", "20", "20.00", "20.00", "20.0", "Twenty", "Twenty", "Twenty", "20", "20", "20.00", "20.00", "20.0", "Twenty", "Twenty", "Twenty", "20", "20", "20.00", "20.00", "20.0", "Twenty", "Twenty", "Twenty", "20", "20", "20.00", "20.00", "20.0", "Twenty", "Twenty", "Twenty"},
            {"22", "22", "22.00", "22.00", "22.0", "Twenty Two", "Twenty Two", "Twenty Two", null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null},
            {"24", "24", "24.00", "24.00", "24.0", "Twenty Four", "Twenty Four", "Twenty Four", null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null}
        };

        JDBC.assertFullResultSet(rs, expRS);

        st.executeUpdate(
            " DROP VIEW K55ADMIN.LJview1");

        st.executeUpdate(
            " DROP VIEW K55ADMIN.LJview2");

        st.executeUpdate(
            " DROP VIEW K55ADMIN.RJview1");

        st.executeUpdate(
            " DROP VIEW K55admin.RJview2");

        st.executeUpdate(
            " DROP VIEW K55ADMIN.IJview1");

        st.executeUpdate(
            " DROP TABLE k55admin.iud_tbl");

        st.executeUpdate(
            " DROP TABLE k55admin.iud_tbl_P");

        st.executeUpdate(
            " DROP TABLE k55admin.MANYTYPES");

        st.executeUpdate(
            " DROP TABLE k55admin.MANYTYPES_P");

        st.executeUpdate(
            " DROP TABLE k55admin.MANYTYPES_CTRL");

        st.executeUpdate(
            " DROP TABLE k55admin.MANYTYPES_NOTNULL");

        st.executeUpdate(
            " DROP TABLE k55admin.MTYPES_NOTNULL_P");

        st.executeUpdate(
            " DROP TABLE k55admin.MTYPES_CTRL_P");

        st.executeUpdate(
            " DROP TABLE k55admin.NEW_OFFICES");

        st.executeUpdate(
            " DROP TABLE k55admin.OLD_OFFICES");

        st.executeUpdate(
            " DROP TABLE k55admin.EMPLOYEES");
    }


    public void test_LojReorderTest_d() throws Exception
    {
        ResultSet rs = null;
        CallableStatement cSt;
        Statement st = createStatement();

        String [][] expRS;
        String [] expColNames;

        setAutoCommit(false);

        // OJqrw.pre

        st.executeUpdate(
            "create table a (a1 int not null primary key, a2 "
            + "int, a3 int, a4 int, a5 int, a6 int)");

        st.executeUpdate(
            " create table b (b1 int not null primary key, b2 "
            + "int, b3 int, b4 int, b5 int, b6 int)");

        st.executeUpdate(
            " create table c (c1 int not null, c2 int, c3 int "
            + "not null, c4 int, c5 int, c6 int)");

        st.executeUpdate(
            " create table d (d1 int not null, d2 int, d3 int "
            + "not null, d4 int, d5 int, d6 int)");

        st.executeUpdate(
            " create table e (e1 int not null primary key, e2 "
            + "int, e3 int, e4 int, e5 int, e6 int)");

        st.executeUpdate(
            " create table f (f1 int not null, f2 int, f3 int, "
            + "f4 int, f5 int, f6 int)");

        st.executeUpdate(
            " create table g (g1 int not null, g2 int, g3 int "
            + "not null, g4 int, g5 int, g6 int)");

        st.executeUpdate(
            " create table h (h1 int not null, h2 int, h3 int "
            + "not null, h4 int, h5 int, h6 int)");

        st.executeUpdate(
            " alter table c add primary key (c1,c3)");

        st.executeUpdate(
            " alter table d add primary key (d1,d3)");

        st.executeUpdate(
            " alter table h add primary key (h3,h1)");

        st.executeUpdate(
            " create index f_id1 on f(f1)");

        st.executeUpdate(
            " create index f_id2 on f(f2)");

        st.executeUpdate(
            " create unique index f_uid1 on f(f4)");

        st.executeUpdate(
            " create unique index f_uid2 on f(f5)");

        st.executeUpdate(
            " create unique index g_uid1 on g(g2,g4)");

        st.executeUpdate(
            " create unique index g_uid2 on g(g3,g1)");

        st.executeUpdate(
            " insert into a values "
            + "(1,1,3,6,NULL,2),(2,3,2,4,2,2),(3,4,2,NULL,NULL,NULL),"
            + "                     "
            + "(4,NULL,4,2,5,2),(5,2,3,5,7,4),(7,1,4,2,3,4),"
            + "                     (8,8,8,8,8,8),(6,7,3,2,3,4)");

        st.executeUpdate(
            " insert into b values "
            + "(6,7,2,3,NULL,1),(4,5,9,6,3,2),(1,4,2,NULL,NULL,NULL),"
            + "                     "
            + "(5,NULL,2,2,5,2),(3,2,3,3,1,4),(7,3,3,3,3,3),(9,3,3,3,3,3)");

        st.executeUpdate(
            " insert into c values "
            + "(3,7,7,3,NULL,1),(8,3,9,1,3,2),(1,4,1,NULL,NULL,NULL),"
            + "                     "
            + "(3,NULL,1,2,4,2),(2,2,5,3,2,4),(1,7,2,3,1,1),(3,8,4,2,4,6)");

        st.executeUpdate(
            " insert into d values "
            + "(1,7,2,3,NULL,3),(2,3,9,1,1,2),(2,2,2,NULL,3,2),"
            + "                     "
            + "(1,NULL,3,2,2,1),(2,2,5,3,2,3),(2,5,6,3,7,2)");

        st.executeUpdate(
            " insert into e values "
            + "(1,1,3,6,NULL,2),(2,3,2,4,2,2),(3,4,2,NULL,NULL,NULL),"
            + "                     "
            + "(4,NULL,4,2,5,2),(5,2,3,5,7,4),(7,1,4,2,3,4),"
            + "                     (8,8,8,8,8,8),(6,7,3,2,3,4)");

        st.executeUpdate(
            " insert into f values "
            + "(6,7,2,3,NULL,1),(4,5,9,6,3,2),(1,4,2,NULL,9,NULL),"
            + "                     "
            + "(5,NULL,2,2,5,2),(3,2,3,7,1,4),(7,3,3,0,0,3)");

        st.executeUpdate(
            " insert into g values "
            + "(3,7,7,3,NULL,1),(8,3,9,1,3,2),(1,4,1,NULL,NULL,NULL),"
            + "                     "
            + "(3,NULL,1,2,4,2),(2,2,5,3,2,4),(1,7,2,0,1,1),(3,8,4,2,4,6)");

        st.executeUpdate(
            " insert into h values "
            + "(1,7,2,3,NULL,3),(2,3,9,1,1,2),(2,2,2,NULL,3,2),"
            + "                     (1,NULL,3,2,2,1),(2,2,5,3,2,3)");

        // OJqrw001.clp
        // *********************************************************
        // Group 1: LOJ linearization
        // *********************************************************

        //------------------------------------------------------------
        // Case 1.1 : Right Deep tree to Left Deep tree
        //            Double Linearization
        //------------------------------------------------------------
        //   Right Deep Tree  to  Left Deep Tree
        //
        //       LJ                        LJ
        //     / | \                     / | \
        //    A  LJ a3=b3              LJ  D  c1=d1
        //     / | \                 / | \
        //    B  LJ b2=c2          LJ  C  b2=c2
        //     / | \             / | \
        //    C  D  c1=d1       A  B  a3=b3

        rs = st.executeQuery(
            "select a1,b1,c1,c3,d1,d3 "
            + "  from A left join (B left join (C left join D on "
            + "c1=d1) on b2=c2) on a3=b3"
            + " order by 6,5,4,3,2,1");

        expColNames = new String [] {"A1", "B1", "C1", "C3", "D1", "D3"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"2", "1", "1", "1", "1", "2"},
            {"3", "1", "1", "1", "1", "2"},
            {"2", "6", "1", "2", "1", "2"},
            {"3", "6", "1", "2", "1", "2"},
            {"1", "3", "2", "5", "2", "2"},
            {"5", "3", "2", "5", "2", "2"},
            {"6", "3", "2", "5", "2", "2"},
            {"2", "1", "1", "1", "1", "3"},
            {"3", "1", "1", "1", "1", "3"},
            {"2", "6", "1", "2", "1", "3"},
            {"3", "6", "1", "2", "1", "3"},
            {"1", "3", "2", "5", "2", "5"},
            {"5", "3", "2", "5", "2", "5"},
            {"6", "3", "2", "5", "2", "5"},
            {"1", "3", "2", "5", "2", "6"},
            {"5", "3", "2", "5", "2", "6"},
            {"6", "3", "2", "5", "2", "6"},
            {"1", "3", "2", "5", "2", "9"},
            {"5", "3", "2", "5", "2", "9"},
            {"6", "3", "2", "5", "2", "9"},
            {"2", "6", "3", "7", null, null},
            {"3", "6", "3", "7", null, null},
            {"1", "7", "8", "9", null, null},
            {"5", "7", "8", "9", null, null},
            {"6", "7", "8", "9", null, null},
            {"1", "9", "8", "9", null, null},
            {"5", "9", "8", "9", null, null},
            {"6", "9", "8", "9", null, null},
            {"2", "5", null, null, null, null},
            {"3", "5", null, null, null, null},
            {"4", null, null, null, null, null},
            {"7", null, null, null, null, null},
            {"8", null, null, null, null, null}
        };

        JDBC.assertFullResultSet(rs, expRS);

        //
        //------------------------------------------------------------
        // Case 1.2 : Right Deep tree - No linearization
        //            for table A; because of b3=d3
        //------------------------------------------------------------
        //   Right Deep Tree
        //
        //       LJ                        LJ
        //     / | \                     / | \
        //    A  LJ b3=d3       to      A  IJ b3=d3  (LJ to IJ by OJ2SEL)
        //     / | \                     / | \
        //    B  LJ b2=c2               D  IJ c1=d1  (LJ to IJ by OJ2SEL)
        //     / | \                     / | \
        //    C  D  c1=d1               B  C  b2=c2
        //
        // we had a bug here...

        rs = st.executeQuery(
            "select a1,b1,c1,c3,d1,d3 "
            + "  from a left join (b left join (c left join d on "
            + "c1=d1) on b2=c2) on b3=d3"
            + " order by 6,5,4,3,2,1");

        expColNames = new String [] {"A1", "B1", "C1", "C3", "D1", "D3"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1", "1", "1", "1", "1", "2"},
            {"2", "1", "1", "1", "1", "2"},
            {"3", "1", "1", "1", "1", "2"},
            {"4", "1", "1", "1", "1", "2"},
            {"5", "1", "1", "1", "1", "2"},
            {"6", "1", "1", "1", "1", "2"},
            {"7", "1", "1", "1", "1", "2"},
            {"8", "1", "1", "1", "1", "2"},
            {"1", "6", "1", "2", "1", "2"},
            {"2", "6", "1", "2", "1", "2"},
            {"3", "6", "1", "2", "1", "2"},
            {"4", "6", "1", "2", "1", "2"},
            {"5", "6", "1", "2", "1", "2"},
            {"6", "6", "1", "2", "1", "2"},
            {"7", "6", "1", "2", "1", "2"},
            {"8", "6", "1", "2", "1", "2"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // new query: linearization for table A

        rs = st.executeQuery(
            "select a1,b1,c1,c3,d1,d3 "
            + "  from a left join (b left join (c left join d on "
            + "c1=d1) on b2=c2) on b3=a3"
            + " order by 6,5,4,3,2,1");

        expColNames = new String [] {"A1", "B1", "C1", "C3", "D1", "D3"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"2", "1", "1", "1", "1", "2"},
            {"3", "1", "1", "1", "1", "2"},
            {"2", "6", "1", "2", "1", "2"},
            {"3", "6", "1", "2", "1", "2"},
            {"1", "3", "2", "5", "2", "2"},
            {"5", "3", "2", "5", "2", "2"},
            {"6", "3", "2", "5", "2", "2"},
            {"2", "1", "1", "1", "1", "3"},
            {"3", "1", "1", "1", "1", "3"},
            {"2", "6", "1", "2", "1", "3"},
            {"3", "6", "1", "2", "1", "3"},
            {"1", "3", "2", "5", "2", "5"},
            {"5", "3", "2", "5", "2", "5"},
            {"6", "3", "2", "5", "2", "5"},
            {"1", "3", "2", "5", "2", "6"},
            {"5", "3", "2", "5", "2", "6"},
            {"6", "3", "2", "5", "2", "6"},
            {"1", "3", "2", "5", "2", "9"},
            {"5", "3", "2", "5", "2", "9"},
            {"6", "3", "2", "5", "2", "9"},
            {"2", "6", "3", "7", null, null},
            {"3", "6", "3", "7", null, null},
            {"1", "7", "8", "9", null, null},
            {"5", "7", "8", "9", null, null},
            {"6", "7", "8", "9", null, null},
            {"1", "9", "8", "9", null, null},
            {"5", "9", "8", "9", null, null},
            {"6", "9", "8", "9", null, null},
            {"2", "5", null, null, null, null},
            {"3", "5", null, null, null, null},
            {"4", null, null, null, null, null},
            {"7", null, null, null, null, null},
            {"8", null, null, null, null, null}
        };

        JDBC.assertFullResultSet(rs, expRS);

        //
        //------------------------------------------------------------
        // Case 1.3 : Right Deep tree - Partial linearization because of
        //            b2=d2
        //------------------------------------------------------------
        //   Right Deep Tree
        //
        //       LJ                        LJ
        //     / | \                     / | \
        //    A  LJ b3=a3       to      /  |  b2=d2
        //     / | \                 LJ    LJ
        //    B  LJ b2=d2          / | \ / | \
        //     / | \              A  B | C D  c1=d1
        //    C  D  c1=d1            b3=a3
        //
        rs = st.executeQuery(
            "select a1,b1,c1,c3,d1,d3 "
            + "  from a left join (b left join (c left join d on "
            + "c1=d1) on b2=d2) on b3=a3"
            + " order by 6,5,4,3,2,1");

        expColNames = new String [] {"A1", "B1", "C1", "C3", "D1", "D3"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"2", "6", "1", "1", "1", "2"},
            {"3", "6", "1", "1", "1", "2"},
            {"2", "6", "1", "2", "1", "2"},
            {"3", "6", "1", "2", "1", "2"},
            {"1", "3", "2", "5", "2", "2"},
            {"5", "3", "2", "5", "2", "2"},
            {"6", "3", "2", "5", "2", "2"},
            {"1", "3", "2", "5", "2", "5"},
            {"5", "3", "2", "5", "2", "5"},
            {"6", "3", "2", "5", "2", "5"},
            {"1", "7", "2", "5", "2", "9"},
            {"5", "7", "2", "5", "2", "9"},
            {"6", "7", "2", "5", "2", "9"},
            {"1", "9", "2", "5", "2", "9"},
            {"5", "9", "2", "5", "2", "9"},
            {"6", "9", "2", "5", "2", "9"},
            {"2", "1", null, null, null, null},
            {"3", "1", null, null, null, null},
            {"2", "5", null, null, null, null},
            {"3", "5", null, null, null, null},
            {"4", null, null, null, null, null},
            {"7", null, null, null, null, null},
            {"8", null, null, null, null, null}
        };

        JDBC.assertFullResultSet(rs, expRS);

        //
        //------------------------------------------------------------
        // Case 1.4 : Right Deep tree - Partial linearization because of
        //            no join predicate between A and B
        //------------------------------------------------------------
        //   Right Deep Tree
        //
        //       LJ                        LJ
        //     / | \                     / | \
        //    A  LJ 1=0         to      A  LJ 1=0
        //     / | \                     / | \
        //    B  LJ b2=c2              LJ  D  c1=d1
        //     / | \                 / | \
        //    C  D  c1=d1           B  C  b2=c2
        //

        rs = st.executeQuery(
            "select a1,b1,c1,c3,d1,d3 "
            + "  from A left join (B left join (C left join D on "
            + "c1=d1) on b2=c2) on 1=0"
            + " order by 6,5,4,3,2,1");

        expColNames = new String [] {"A1", "B1", "C1", "C3", "D1", "D3"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1", null, null, null, null, null},
            {"2", null, null, null, null, null},
            {"3", null, null, null, null, null},
            {"4", null, null, null, null, null},
            {"5", null, null, null, null, null},
            {"6", null, null, null, null, null},
            {"7", null, null, null, null, null},
            {"8", null, null, null, null, null}
        };

        JDBC.assertFullResultSet(rs, expRS);

        //
        //------------------------------------------------------------
        // Case 1.5 : Right Deep tree - Partial linearization because of
        //            self-join only
        //------------------------------------------------------------
        //   Right Deep Tree
        //
        //       LJ                        LJ
        //     / | \                     / | \
        //    A  LJ a1=a3       to      A  LJ a1=a3
        //     / | \                     / | \
        //    B  LJ b2=c2              LJ  D  c1=d1
        //     / | \                 / | \
        //    C  D  c1=d1           B  C  b2=c2
        //

        rs = st.executeQuery(
            "select a1,b1,c1,c3,d1,d3 "
            + "  from A left join (B left join (C left join D on "
            + "c1=d1) on b2=c2) on a1=a3"
            + " order by 6,5,4,3,2,1");

        expColNames = new String [] {"A1", "B1", "C1", "C3", "D1", "D3"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"2", "1", "1", "1", "1", "2"},
            {"4", "1", "1", "1", "1", "2"},
            {"8", "1", "1", "1", "1", "2"},
            {"2", "6", "1", "2", "1", "2"},
            {"4", "6", "1", "2", "1", "2"},
            {"8", "6", "1", "2", "1", "2"},
            {"2", "3", "2", "5", "2", "2"},
            {"4", "3", "2", "5", "2", "2"},
            {"8", "3", "2", "5", "2", "2"},
            {"2", "1", "1", "1", "1", "3"},
            {"4", "1", "1", "1", "1", "3"},
            {"8", "1", "1", "1", "1", "3"},
            {"2", "6", "1", "2", "1", "3"},
            {"4", "6", "1", "2", "1", "3"},
            {"8", "6", "1", "2", "1", "3"},
            {"2", "3", "2", "5", "2", "5"},
            {"4", "3", "2", "5", "2", "5"},
            {"8", "3", "2", "5", "2", "5"},
            {"2", "3", "2", "5", "2", "6"},
            {"4", "3", "2", "5", "2", "6"},
            {"8", "3", "2", "5", "2", "6"},
            {"2", "3", "2", "5", "2", "9"},
            {"4", "3", "2", "5", "2", "9"},
            {"8", "3", "2", "5", "2", "9"},
            {"2", "6", "3", "7", null, null},
            {"4", "6", "3", "7", null, null},
            {"8", "6", "3", "7", null, null},
            {"2", "7", "8", "9", null, null},
            {"4", "7", "8", "9", null, null},
            {"8", "7", "8", "9", null, null},
            {"2", "9", "8", "9", null, null},
            {"4", "9", "8", "9", null, null},
            {"8", "9", "8", "9", null, null},
            {"2", "4", null, null, null, null},
            {"4", "4", null, null, null, null},
            {"8", "4", null, null, null, null},
            {"2", "5", null, null, null, null},
            {"4", "5", null, null, null, null},
            {"8", "5", null, null, null, null},
            {"1", null, null, null, null, null},
            {"3", null, null, null, null, null},
            {"5", null, null, null, null, null},
            {"6", null, null, null, null, null},
            {"7", null, null, null, null, null}
        };

        JDBC.assertFullResultSet(rs, expRS);

        //
        //------------------------------------------------------------
        // Case 1.6 : Single linearization
        //            Balance Tree to Left Deep tree in one step
        //------------------------------------------------------------
        //   Balance Tree       to      Left Deep Tree
        //
        //         LJ                       LJ
        //       /  | \                    / | \
        //      /   |  a2=c2 ^ b3=c3     LJ  D  c1=d1
        //   LJ     LJ                 / | \
        // / | \  / | \              LJ  C  a2=c2 ^ b3=c3
        // A B |  C D c1=d1        / | \
        //   a1=b1                A  B  a1=b1
        //

        rs = st.executeQuery(
            "select a1,b1,c1,c3,d1,d3 "
            + "  from (a left join b on a1=b1) left join (c left "
            + "join d on c1=d1) on a2=c2 and b3=c3"
            + " order by 6,5,4,3,2,1");

        expColNames = new String [] {"A1", "B1", "C1", "C3", "D1", "D3"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"6", "6", "1", "2", "1", "2"},
            {"6", "6", "1", "2", "1", "3"},
            {"1", "1", null, null, null, null},
            {"3", "3", null, null, null, null},
            {"4", "4", null, null, null, null},
            {"5", "5", null, null, null, null},
            {"7", "7", null, null, null, null},
            {"2", null, null, null, null, null},
            {"8", null, null, null, null, null}
        };

        JDBC.assertFullResultSet(rs, expRS);

        //
        //------------------------------------------------------------
        // Case 1.7 : No Linearization because the ON clause only has a join
        //            between two tuple-producing tables
        //------------------------------------------------------------
        //   Balance Tree
        //
        //         LJ
        //       /  | \
        //      /   |  a2=b2    No transformation because of a2=b2
        //   LJ     LJ
        // / | \  / | \
        // A B |  C D c1=d1
        //   a1=b1
        //

        rs = st.executeQuery(
            "select a1,b1,c1,c3,d1,d3 "
            + "  from (a left join b on a1=b1) left join (c left "
            + "join d on c1=d1) on a2=b2"
            + " order by 6,5,4,3,2,1");

        expColNames = new String [] {"A1", "B1", "C1", "C3", "D1", "D3"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"6", "6", "1", "1", "1", "2"},
            {"6", "6", "1", "2", "1", "2"},
            {"6", "6", "2", "5", "2", "2"},
            {"6", "6", "1", "1", "1", "3"},
            {"6", "6", "1", "2", "1", "3"},
            {"6", "6", "2", "5", "2", "5"},
            {"6", "6", "2", "5", "2", "6"},
            {"6", "6", "2", "5", "2", "9"},
            {"6", "6", "3", "1", null, null},
            {"6", "6", "3", "4", null, null},
            {"6", "6", "3", "7", null, null},
            {"6", "6", "8", "9", null, null},
            {"1", "1", null, null, null, null},
            {"3", "3", null, null, null, null},
            {"4", "4", null, null, null, null},
            {"5", "5", null, null, null, null},
            {"7", "7", null, null, null, null},
            {"2", null, null, null, null, null},
            {"8", null, null, null, null, null}
        };

        JDBC.assertFullResultSet(rs, expRS);

        //
        //------------------------------------------------------------
        // Case 1.8 : No linearization because of missing join predicate
        //            between A and C or B and C
        //------------------------------------------------------------
        //   Balance Tree
        //
        //         LJ
        //       /  | \
        //      /   |  1=0      No transformation because of no join between
        //   LJ     LJ          A and C or join between between B and C
        // / | \  / | \
        // A B |  C D c1=d1
        //   a1=b1
        //

        rs = st.executeQuery(
            "select a1,b1,c1,c3,d1,d3 "
            + "  from (a left join b on a1=b1) left join (c left "
            + "join d on c1=d1) on 1=0"
            + " order by 6,5,4,3,2,1");

        expColNames = new String [] {"A1", "B1", "C1", "C3", "D1", "D3"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1", "1", null, null, null, null},
            {"3", "3", null, null, null, null},
            {"4", "4", null, null, null, null},
            {"5", "5", null, null, null, null},
            {"6", "6", null, null, null, null},
            {"7", "7", null, null, null, null},
            {"2", null, null, null, null, null},
            {"8", null, null, null, null, null}
        };

        JDBC.assertFullResultSet(rs, expRS);

        //
        //------------------------------------------------------------
        // Case 1.9 : No linearization because of d1=2
        //------------------------------------------------------------
        //   Balance Tree       No linearization because of d1=2
        //
        //         LJ                        LJ
        //       /  | \                    /  | \
        //      /   |  c2=b2 ^ d1=2       /   |  c2=b2 ^ d1=2
        //   LJ     LJ                 LJ     IJ     LJ to IJ because of OJ2SEL
        // / | \  / | \              / | \  / | \
        // A B |  C D c1=d1          A B |  D C c1=d1
        //   a1=b1                     a1=b1
        //

        rs = st.executeQuery(
            "select a1,b1,c1,c3,d1,d3 "
            + "  from (a left join b on a1=b1) left join (c left "
            + "join d on c1=d1) on c2=b2 and d1=2"
            + " order by 6,5,4,3,2,1");

        expColNames = new String [] {"A1", "B1", "C1", "C3", "D1", "D3"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"3", "3", "2", "5", "2", "2"},
            {"3", "3", "2", "5", "2", "5"},
            {"3", "3", "2", "5", "2", "6"},
            {"3", "3", "2", "5", "2", "9"},
            {"1", "1", null, null, null, null},
            {"4", "4", null, null, null, null},
            {"5", "5", null, null, null, null},
            {"6", "6", null, null, null, null},
            {"7", "7", null, null, null, null},
            {"2", null, null, null, null, null},
            {"8", null, null, null, null, null}
        };

        JDBC.assertFullResultSet(rs, expRS);

        //
        //------------------------------------------------------------
        // Case 1.10: No linearization because of d1=d4
        //------------------------------------------------------------
        //   Balance Tree       No linearization because of d1=d4
        //
        //         LJ                        LJ
        //       /  | \                    /  | \
        //      /   |  c2=b2 ^ d1=d4      /   |  c2=b2 ^ d1=d4
        //   LJ     LJ                 LJ     IJ     LJ to IJ because of OJ2SEL
        // / | \  / | \              / | \  / | \
        // A B |  C D c1=d1          A B |  C D c1=d1
        //   a1=b1                     a1=b1
        //

        rs = st.executeQuery(
            "select a1,b1,c1,c3,d1,d3 "
            + "  from (a left join b on a1=b1) left join (c left "
            + "join d on c1=d1) on c2=b2 and d1=d4"
            + " order by 6,5,4,3,2,1");

        expColNames = new String [] {"A1", "B1", "C1", "C3", "D1", "D3"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1", "1", null, null, null, null},
            {"3", "3", null, null, null, null},
            {"4", "4", null, null, null, null},
            {"5", "5", null, null, null, null},
            {"6", "6", null, null, null, null},
            {"7", "7", null, null, null, null},
            {"2", null, null, null, null, null},
            {"8", null, null, null, null, null}
        };

        JDBC.assertFullResultSet(rs, expRS);

        //
        //------------------------------------------------------------
        // Case 1.11: Same as case 1.10 except no d1=d4
        //            Linearization performed even with LJ
        //------------------------------------------------------------
        //   Balance Tree       to      Left Deep Tree
        //
        //         LJ                       LJ
        //       /  | \                    / | \
        //      /   |  c2=b2             LJ  D  c1=d1
        //   LJ     LJ                 / | \
        // / | \  / | \              LJ  C  c2=b2
        // A B |  C D c1=d1        / | \
        //   a1=b1                A  B  a1=b1
        //

        rs = st.executeQuery(
            "select a1,b1,c1,c3,d1,d3 "
            + "  from (a left join b on a1=b1) left join (c left "
            + "join d on c1=d1) on c2=b2"
            + " order by 6,5,4,3,2,1");

        expColNames = new String [] {"A1", "B1", "C1", "C3", "D1", "D3"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1", "1", "1", "1", "1", "2"},
            {"6", "6", "1", "2", "1", "2"},
            {"3", "3", "2", "5", "2", "2"},
            {"1", "1", "1", "1", "1", "3"},
            {"6", "6", "1", "2", "1", "3"},
            {"3", "3", "2", "5", "2", "5"},
            {"3", "3", "2", "5", "2", "6"},
            {"3", "3", "2", "5", "2", "9"},
            {"6", "6", "3", "7", null, null},
            {"7", "7", "8", "9", null, null},
            {"4", "4", null, null, null, null},
            {"5", "5", null, null, null, null},
            {"2", null, null, null, null, null},
            {"8", null, null, null, null, null}
        };

        JDBC.assertFullResultSet(rs, expRS);

        //
        //------------------------------------------------------------
        // Case 1.12: No linearization because of a3=d3
        //------------------------------------------------------------
        //   Balance Tree       No linearization because of a3=d3
        //
        //         LJ                        LJ
        //       /  | \                    /  | \
        //      /   |  c2=b2 ^ a3=d3      /   |  c2=b2 ^ a3=d3
        //   LJ     LJ                 LJ     IJ     LJ to IJ because of OJ2SEL
        // / | \  / | \              / | \  / | \
        // A B |  C D c1=d1          A B |  D C c1=d1
        //   a1=b1                     a1=b1
        //

        rs = st.executeQuery(
            "select a1,b1,c1,c3,d1,d3 "
            + "  from (A left join B on a1=b1) left join (C left "
            + "join D on c1=d1) on c2=b2 and a3=d3"
            + " order by 6,5,4,3,2,1");

        expColNames = new String [] {"A1", "B1", "C1", "C3", "D1", "D3"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"3", "3", "2", "5", "2", "2"},
            {"1", "1", "1", "1", "1", "3"},
            {"6", "6", "1", "2", "1", "3"},
            {"4", "4", null, null, null, null},
            {"5", "5", null, null, null, null},
            {"7", "7", null, null, null, null},
            {"2", null, null, null, null, null},
            {"8", null, null, null, null, null}
        };

        JDBC.assertFullResultSet(rs, expRS);

        //
        //------------------------------------------------------------
        // Case 1.13: Linearization with always true predicate
        //------------------------------------------------------------
        //   Balance Tree      to       Left Deep Tree
        //
        //         LJ                       LJ
        //       /  | \                   / | \
        //      /   |  c2=b2 ^ 1=1      LJ  D  c1=d1
        //   LJ     LJ                / | \
        // / | \  / | \             LJ  C c2=b2 ^ 1=1
        // A B |  C D c1=d1       / | \
        //   a1=b1                A B a1=b1
        //

        // Update: With DERBY-4471, this query *does* gets reordered, cf plan
        //        check below. Previously, it did not, in spite of what the
        //        above comment indicated.
        cSt = prepareCall(
            " call SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1)");assertUpdateCount(cSt, 0);

        rs = st.executeQuery(
            "select a1,b1,c1,c3,d1,d3 "
            + "  from A left join B on a1=b1 left join (C left "
            + "join d on c1=d1) on b2=c2 and 1=1"
            + " order by 6,5,4,3,2,1");

        expColNames = new String [] {"A1", "B1", "C1", "C3", "D1", "D3"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1", "1", "1", "1", "1", "2"},
            {"6", "6", "1", "2", "1", "2"},
            {"3", "3", "2", "5", "2", "2"},
            {"1", "1", "1", "1", "1", "3"},
            {"6", "6", "1", "2", "1", "3"},
            {"3", "3", "2", "5", "2", "5"},
            {"3", "3", "2", "5", "2", "6"},
            {"3", "3", "2", "5", "2", "9"},
            {"6", "6", "3", "7", null, null},
            {"7", "7", "8", "9", null, null},
            {"4", "4", null, null, null, null},
            {"5", "5", null, null, null, null},
            {"2", null, null, null, null, null},
            {"8", null, null, null, null, null}
        };

        JDBC.assertFullResultSet(rs, expRS);

        JDBC.checkPlan(st,
                  new String[] {
                      "Sort ResultSet:",
                      "Source result set:",
                      "_Project-Restrict ResultSet (9):",
                      "_Source result set:",
                      "__Hash Left Outer Join ResultSet:",
                      "__Left result set:",
                      "___Hash Left Outer Join ResultSet:",
                      "___Left result set:",
                      "____Hash Left Outer Join ResultSet:",
                      "____Left result set:",
                      "_____Index Scan ResultSet for A",
                      "____Right result set:",
                      "_____Hash Scan ResultSet for B",
                      "___Right result set:",
                      "____Project-Restrict ResultSet (7):",
                      "____Source result set:",
                      "_____Hash Scan ResultSet for C",
                      "__Right result set:",
                      "___Hash Scan ResultSet for D"});

        //
        //------------------------------------------------------------
        // Case 1.14: No linearization with true predicate alone
        //------------------------------------------------------------
        //   Balance Tree
        //
        //         LJ
        //       /  | \
        //      /   |  1=1
        //   LJ     LJ
        // / | \  / | \
        // A B |  C D c1=d1
        //   a1=b1
        //

        rs = st.executeQuery(
            "select a1,b1,c1,c3,d1,d3 "
            + "  from A left join B on a1=b1 left join (C left "
            + "join d on c1=d1) on 1=1"
            + " order by 6,5,4,3,2,1");

        expColNames = new String [] {"A1", "B1", "C1", "C3", "D1", "D3"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1", "1", "1", "1", "1", "2"},
            {"3", "3", "1", "1", "1", "2"},
            {"4", "4", "1", "1", "1", "2"},
            {"5", "5", "1", "1", "1", "2"},
            {"6", "6", "1", "1", "1", "2"},
            {"7", "7", "1", "1", "1", "2"},
            {"2", null, "1", "1", "1", "2"},
            {"8", null, "1", "1", "1", "2"},
            {"1", "1", "1", "2", "1", "2"},
            {"3", "3", "1", "2", "1", "2"},
            {"4", "4", "1", "2", "1", "2"},
            {"5", "5", "1", "2", "1", "2"},
            {"6", "6", "1", "2", "1", "2"},
            {"7", "7", "1", "2", "1", "2"},
            {"2", null, "1", "2", "1", "2"},
            {"8", null, "1", "2", "1", "2"},
            {"1", "1", "2", "5", "2", "2"},
            {"3", "3", "2", "5", "2", "2"},
            {"4", "4", "2", "5", "2", "2"},
            {"5", "5", "2", "5", "2", "2"},
            {"6", "6", "2", "5", "2", "2"},
            {"7", "7", "2", "5", "2", "2"},
            {"2", null, "2", "5", "2", "2"},
            {"8", null, "2", "5", "2", "2"},
            {"1", "1", "1", "1", "1", "3"},
            {"3", "3", "1", "1", "1", "3"},
            {"4", "4", "1", "1", "1", "3"},
            {"5", "5", "1", "1", "1", "3"},
            {"6", "6", "1", "1", "1", "3"},
            {"7", "7", "1", "1", "1", "3"},
            {"2", null, "1", "1", "1", "3"},
            {"8", null, "1", "1", "1", "3"},
            {"1", "1", "1", "2", "1", "3"},
            {"3", "3", "1", "2", "1", "3"},
            {"4", "4", "1", "2", "1", "3"},
            {"5", "5", "1", "2", "1", "3"},
            {"6", "6", "1", "2", "1", "3"},
            {"7", "7", "1", "2", "1", "3"},
            {"2", null, "1", "2", "1", "3"},
            {"8", null, "1", "2", "1", "3"},
            {"1", "1", "2", "5", "2", "5"},
            {"3", "3", "2", "5", "2", "5"},
            {"4", "4", "2", "5", "2", "5"},
            {"5", "5", "2", "5", "2", "5"},
            {"6", "6", "2", "5", "2", "5"},
            {"7", "7", "2", "5", "2", "5"},
            {"2", null, "2", "5", "2", "5"},
            {"8", null, "2", "5", "2", "5"},
            {"1", "1", "2", "5", "2", "6"},
            {"3", "3", "2", "5", "2", "6"},
            {"4", "4", "2", "5", "2", "6"},
            {"5", "5", "2", "5", "2", "6"},
            {"6", "6", "2", "5", "2", "6"},
            {"7", "7", "2", "5", "2", "6"},
            {"2", null, "2", "5", "2", "6"},
            {"8", null, "2", "5", "2", "6"},
            {"1", "1", "2", "5", "2", "9"},
            {"3", "3", "2", "5", "2", "9"},
            {"4", "4", "2", "5", "2", "9"},
            {"5", "5", "2", "5", "2", "9"},
            {"6", "6", "2", "5", "2", "9"},
            {"7", "7", "2", "5", "2", "9"},
            {"2", null, "2", "5", "2", "9"},
            {"8", null, "2", "5", "2", "9"},
            {"1", "1", "3", "1", null, null},
            {"3", "3", "3", "1", null, null},
            {"4", "4", "3", "1", null, null},
            {"5", "5", "3", "1", null, null},
            {"6", "6", "3", "1", null, null},
            {"7", "7", "3", "1", null, null},
            {"2", null, "3", "1", null, null},
            {"8", null, "3", "1", null, null},
            {"1", "1", "3", "4", null, null},
            {"3", "3", "3", "4", null, null},
            {"4", "4", "3", "4", null, null},
            {"5", "5", "3", "4", null, null},
            {"6", "6", "3", "4", null, null},
            {"7", "7", "3", "4", null, null},
            {"2", null, "3", "4", null, null},
            {"8", null, "3", "4", null, null},
            {"1", "1", "3", "7", null, null},
            {"3", "3", "3", "7", null, null},
            {"4", "4", "3", "7", null, null},
            {"5", "5", "3", "7", null, null},
            {"6", "6", "3", "7", null, null},
            {"7", "7", "3", "7", null, null},
            {"2", null, "3", "7", null, null},
            {"8", null, "3", "7", null, null},
            {"1", "1", "8", "9", null, null},
            {"3", "3", "8", "9", null, null},
            {"4", "4", "8", "9", null, null},
            {"5", "5", "8", "9", null, null},
            {"6", "6", "8", "9", null, null},
            {"7", "7", "8", "9", null, null},
            {"2", null, "8", "9", null, null},
            {"8", null, "8", "9", null, null}
        };

        JDBC.assertFullResultSet(rs, expRS);

        //
        //------------------------------------------------------------
        // Case 1.15: No linearization with non-colequiv class
        //------------------------------------------------------------
        //   Balance Tree
        //
        //         LJ
        //       /  | \
        //      /   |  b1>c1
        //   LJ     LJ
        // / | \  / | \
        // A B |  C D c1=d1
        //   a1=b1
        //

        // Update: With DERBY-4471, this query also gets reordered, cf plan
        // check below, to
        //             LJ
        //           /  | \
        //         LJ   d  c1=d1
        //       /  | \
        //      /   |  b1>c1
        //   LJ     C
        //  / | \
        //  A B  \
        //      a1=b1

        rs = st.executeQuery(
            "select a1,b1,c1,c3,d1,d3 "
            + "  from A left join B on a1=b1 left join (C left "
            + "join d on c1=d1) on b1>c1"
            + " order by 6,5,4,3,2,1");

        expColNames = new String [] {"A1", "B1", "C1", "C3", "D1", "D3"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"3", "3", "1", "1", "1", "2"},
            {"4", "4", "1", "1", "1", "2"},
            {"5", "5", "1", "1", "1", "2"},
            {"6", "6", "1", "1", "1", "2"},
            {"7", "7", "1", "1", "1", "2"},
            {"3", "3", "1", "2", "1", "2"},
            {"4", "4", "1", "2", "1", "2"},
            {"5", "5", "1", "2", "1", "2"},
            {"6", "6", "1", "2", "1", "2"},
            {"7", "7", "1", "2", "1", "2"},
            {"3", "3", "2", "5", "2", "2"},
            {"4", "4", "2", "5", "2", "2"},
            {"5", "5", "2", "5", "2", "2"},
            {"6", "6", "2", "5", "2", "2"},
            {"7", "7", "2", "5", "2", "2"},
            {"3", "3", "1", "1", "1", "3"},
            {"4", "4", "1", "1", "1", "3"},
            {"5", "5", "1", "1", "1", "3"},
            {"6", "6", "1", "1", "1", "3"},
            {"7", "7", "1", "1", "1", "3"},
            {"3", "3", "1", "2", "1", "3"},
            {"4", "4", "1", "2", "1", "3"},
            {"5", "5", "1", "2", "1", "3"},
            {"6", "6", "1", "2", "1", "3"},
            {"7", "7", "1", "2", "1", "3"},
            {"3", "3", "2", "5", "2", "5"},
            {"4", "4", "2", "5", "2", "5"},
            {"5", "5", "2", "5", "2", "5"},
            {"6", "6", "2", "5", "2", "5"},
            {"7", "7", "2", "5", "2", "5"},
            {"3", "3", "2", "5", "2", "6"},
            {"4", "4", "2", "5", "2", "6"},
            {"5", "5", "2", "5", "2", "6"},
            {"6", "6", "2", "5", "2", "6"},
            {"7", "7", "2", "5", "2", "6"},
            {"3", "3", "2", "5", "2", "9"},
            {"4", "4", "2", "5", "2", "9"},
            {"5", "5", "2", "5", "2", "9"},
            {"6", "6", "2", "5", "2", "9"},
            {"7", "7", "2", "5", "2", "9"},
            {"4", "4", "3", "1", null, null},
            {"5", "5", "3", "1", null, null},
            {"6", "6", "3", "1", null, null},
            {"7", "7", "3", "1", null, null},
            {"4", "4", "3", "4", null, null},
            {"5", "5", "3", "4", null, null},
            {"6", "6", "3", "4", null, null},
            {"7", "7", "3", "4", null, null},
            {"4", "4", "3", "7", null, null},
            {"5", "5", "3", "7", null, null},
            {"6", "6", "3", "7", null, null},
            {"7", "7", "3", "7", null, null},
            {"1", "1", null, null, null, null},
            {"2", null, null, null, null, null},
            {"8", null, null, null, null, null}
        };

        JDBC.assertFullResultSet(rs, expRS);

        JDBC.checkPlan(st,
                  new String[] {
                      "Sort ResultSet:",
                      "Source result set:",
                      "_Hash Left Outer Join ResultSet:",
                      "_Left result set:",
                      "__Nested Loop Left Outer Join ResultSet:",
                      "__Left result set:",
                      "___Nested Loop Left Outer Join ResultSet:",
                      "___Left result set:",
                      "____Index Scan ResultSet for A",
                      "___Right result set:",
                      "____Index Scan ResultSet for B",
                      "__Right result set:",
                      "___Index Scan ResultSet for C",
                      "_Right result set:",
                      "__Hash Scan ResultSet for D"});

        //
        //------------------------------------------------------------
        // Case 1.16: Partial Left Deep tree becomes Left Left Deep tree
        //------------------------------------------------------------
        //  Partial Left Deep Tree  to   Left Left Deep Tree
        //
        //       LJ                           LJ
        //     / | \                        /  | \
        //   A   LJ a3=b3                 LJ   D  c2=d2 & b3>0
        //     / | \                    / | \
        //   LJ  D  c2=d2 & b3>0      LJ  C c1=b1
        // / | \                    / | \
        // B C c1=b1               A  B a3=b3
        //

        rs = st.executeQuery(
            "select a1,b1,c1,c3,d1,d3 "
            + "  from a left join ((B left join C on c1=b1) left "
            + "join d on c2=d2 and b3>0) on a3=b3"
            + " order by 6,5,4,3,2,1");

        expColNames = new String [] {"A1", "B1", "C1", "C3", "D1", "D3"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"2", "1", "1", "2", "1", "2"},
            {"3", "1", "1", "2", "1", "2"},
            {"1", "3", "3", "7", "1", "2"},
            {"5", "3", "3", "7", "1", "2"},
            {"6", "3", "3", "7", "1", "2"},
            {"2", "1", "1", "1", null, null},
            {"3", "1", "1", "1", null, null},
            {"1", "3", "3", "1", null, null},
            {"5", "3", "3", "1", null, null},
            {"6", "3", "3", "1", null, null},
            {"1", "3", "3", "4", null, null},
            {"5", "3", "3", "4", null, null},
            {"6", "3", "3", "4", null, null},
            {"2", "5", null, null, null, null},
            {"3", "5", null, null, null, null},
            {"2", "6", null, null, null, null},
            {"3", "6", null, null, null, null},
            {"1", "7", null, null, null, null},
            {"5", "7", null, null, null, null},
            {"6", "7", null, null, null, null},
            {"1", "9", null, null, null, null},
            {"5", "9", null, null, null, null},
            {"6", "9", null, null, null, null},
            {"4", null, null, null, null, null},
            {"7", null, null, null, null, null},
            {"8", null, null, null, null, null}
        };

        JDBC.assertFullResultSet(rs, expRS);

        //
        //------------------------------------------------------------
        // Case 1.17: Complicated case
        //            Cannot lefty linearize because of a3=c3
        //------------------------------------------------------------
        //  Zig-Zag tree           to   Partial left Deep Tree
        //
        //         LJ                           LJ
        //       / | \                        /  | \
        //     A   LJ a3=c3                 LJ   E  d2=e2
        //       / | \                    / | \
        //     LJ  E  d2=e2             LJ  D  c1=d1
        //   / | \                    / | \
        //  B  LJ b2=c2              A  IJ a3=c3
        //   / | \                    / | \
        //  C  D  c1=d1              C  B  b2=c2
        //

        rs = st.executeQuery(
            "select a1,b1,c1,c3,d1,d3,e1"
            + "  from A left join ((B left join (C left join D on "
            + "c1=d1) on b2=c2) left join E on d2=e2) on a3=c3"
            + " order by 7,6,5,4,3,2,1");

        expColNames = new String [] {"A1", "B1", "C1", "C3", "D1", "D3", "E1"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"2", "6", "1", "2", "1", "2", "6"},
            {"3", "6", "1", "2", "1", "2", "6"},
            {"2", "6", "1", "2", "1", "3", null},
            {"3", "6", "1", "2", "1", "3", null},
            {"1", null, null, null, null, null, null},
            {"4", null, null, null, null, null, null},
            {"5", null, null, null, null, null, null},
            {"6", null, null, null, null, null, null},
            {"7", null, null, null, null, null, null},
            {"8", null, null, null, null, null, null}
        };

        JDBC.assertFullResultSet(rs, expRS);

        //
        //------------------------------------------------------------
        // Case 1.18: Double linearization.
        //            Same as case 1.15 but lefty linearize
        //------------------------------------------------------------
        //  Zig-Zag tree             to   left Deep Tree
        //
        //         LJ                           LJ
        //       / | \                        /  | \
        //     A   LJ a3=b3                 LJ   E  d2=e2
        //       / | \                    / | \
        //     LJ  E  d2=e2             LJ  D  c1=d1
        //   / | \                    / | \
        //  B  LJ b2=c2             LJ  C  b2=c2
        //   / | \                / | \
        //  C  D  c1=d1          A  B  a3=b3
        //

        rs = st.executeQuery(
            "select a1,b1,c1,c3,d1,d3,e1"
            + "  from a left join ((b left join (c left join d on "
            + "c1=d1) on b2=c2) left join e on d2=e2) on a3=b3"
            + " order by 7,6,5,4,3,2,1");

        expColNames = new String [] {"A1", "B1", "C1", "C3", "D1", "D3", "E1"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1", "3", "2", "5", "2", "9", "2"},
            {"5", "3", "2", "5", "2", "9", "2"},
            {"6", "3", "2", "5", "2", "9", "2"},
            {"1", "3", "2", "5", "2", "2", "5"},
            {"5", "3", "2", "5", "2", "2", "5"},
            {"6", "3", "2", "5", "2", "2", "5"},
            {"1", "3", "2", "5", "2", "5", "5"},
            {"5", "3", "2", "5", "2", "5", "5"},
            {"6", "3", "2", "5", "2", "5", "5"},
            {"2", "1", "1", "1", "1", "2", "6"},
            {"3", "1", "1", "1", "1", "2", "6"},
            {"2", "6", "1", "2", "1", "2", "6"},
            {"3", "6", "1", "2", "1", "2", "6"},
            {"2", "1", "1", "1", "1", "3", null},
            {"3", "1", "1", "1", "1", "3", null},
            {"2", "6", "1", "2", "1", "3", null},
            {"3", "6", "1", "2", "1", "3", null},
            {"1", "3", "2", "5", "2", "6", null},
            {"5", "3", "2", "5", "2", "6", null},
            {"6", "3", "2", "5", "2", "6", null},
            {"2", "6", "3", "7", null, null, null},
            {"3", "6", "3", "7", null, null, null},
            {"1", "7", "8", "9", null, null, null},
            {"5", "7", "8", "9", null, null, null},
            {"6", "7", "8", "9", null, null, null},
            {"1", "9", "8", "9", null, null, null},
            {"5", "9", "8", "9", null, null, null},
            {"6", "9", "8", "9", null, null, null},
            {"2", "5", null, null, null, null, null},
            {"3", "5", null, null, null, null, null},
            {"4", null, null, null, null, null, null},
            {"7", null, null, null, null, null, null},
            {"8", null, null, null, null, null, null}
        };

        JDBC.assertFullResultSet(rs, expRS);

        //
        //------------------------------------------------------------
        // Case 1.19: Linearization in two branches of LJ
        //------------------------------------------------------------
        //
        //                 LJ                             LJ -- d3=e3
        //               /  | \                        /    \
        //              /   LJ d3=e3                  /      LJ
        //             /  / | \        to            /      / | \
        //          LJ   E  LJ e2=f2              LJ       LJ G  f1=g1
        //        / | \   / | \                 / | \     / | \
        //       /  |  \ F  G  f1=g1         LJ   D c1=d1 E F e2=f2
        //    LJ    LJ b2=c2               / | \
        //  / | \ / | \                 LJ   C b2=c2
        // A  B | C D c1=d1           / | \
        //    a1=b1                  A  B a1=b1
        //

        rs = st.executeQuery(
            "select a1,b1,c1,c3,d1,d3,e1,f4,g1,g3"
            + "  from ((A left join B on a1=b1) left join (C left "
            + "join D on c1=d1) on b2=c2)"
            + "       left join"
            + "       (e left join (f left join g on f1=g1) on "
            + "e2=f2) on d3=e3"
            + " order by 10,9,8,7,6,5,4,3,2,1");

        expColNames = new String [] {"A1", "B1", "C1", "C3", "D1", "D3", "E1", "F4", "G1", "G3"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1", "1", "1", "1", "1", "2", "3", null, "1", "1"},
            {"6", "6", "1", "2", "1", "2", "3", null, "1", "1"},
            {"3", "3", "2", "5", "2", "2", "3", null, "1", "1"},
            {"1", "1", "1", "1", "1", "3", "5", "7", "3", "1"},
            {"6", "6", "1", "2", "1", "3", "5", "7", "3", "1"},
            {"1", "1", "1", "1", "1", "2", "3", null, "1", "2"},
            {"6", "6", "1", "2", "1", "2", "3", null, "1", "2"},
            {"3", "3", "2", "5", "2", "2", "3", null, "1", "2"},
            {"1", "1", "1", "1", "1", "3", "5", "7", "3", "4"},
            {"6", "6", "1", "2", "1", "3", "5", "7", "3", "4"},
            {"1", "1", "1", "1", "1", "3", "5", "7", "3", "7"},
            {"6", "6", "1", "2", "1", "3", "5", "7", "3", "7"},
            {"1", "1", "1", "1", "1", "2", "2", "0", null, null},
            {"6", "6", "1", "2", "1", "2", "2", "0", null, null},
            {"3", "3", "2", "5", "2", "2", "2", "0", null, null},
            {"1", "1", "1", "1", "1", "3", "6", "3", null, null},
            {"6", "6", "1", "2", "1", "3", "6", "3", null, null},
            {"1", "1", "1", "1", "1", "3", "1", null, null, null},
            {"6", "6", "1", "2", "1", "3", "1", null, null, null},
            {"3", "3", "2", "5", "2", "5", null, null, null, null},
            {"3", "3", "2", "5", "2", "6", null, null, null, null},
            {"3", "3", "2", "5", "2", "9", null, null, null, null},
            {"6", "6", "3", "7", null, null, null, null, null, null},
            {"7", "7", "8", "9", null, null, null, null, null, null},
            {"4", "4", null, null, null, null, null, null, null, null},
            {"5", "5", null, null, null, null, null, null, null, null},
            {"2", null, null, null, null, null, null, null, null, null},
            {"8", null, null, null, null, null, null, null, null, null}
        };

        JDBC.assertFullResultSet(rs, expRS);

        //
        //------------------------------------------------------------
        // Case 1.20: Linearization in two branches of inner join
        //------------------------------------------------------------
        //
        //                 IJ                             IJ -- a1=d2
        //               /  | \                        /    \
        //              /   LJ a1=d2                  /      LJ
        //             /  / | \        to            /      / | \
        //          LJ   A  LJ a2=b2              LJ       LJ C  b1=c1
        //        / | \   / | \                 / | \     / | \
        //       /  |  \ B  C  b1=c1         LJ   G f1=g1 A B a2=b2
        //    LJ    LJ e2=f2               / | \
        //  / | \ / | \                 LJ   F e2=f2
        // D  E | F G f1=g1           / | \
        //    d1=e1                  D  E d1=e1
        //

        rs = st.executeQuery(
            "select a1,b1,c1,c3,d1,d3,e1,f4,g1,g3"
            + "  from (A left join (b left join c on b1=c1) on a2=b2) "
            + "       inner join "
            + "       ((D left join E on d1=e1) left join (F left "
            + "join G on f1=g1) on e2=f2)"
            + "       on a1=d2"
            + " order by 10,9,8,7,6,5,4,3,2,1");

        expColNames = new String [] {"A1", "B1", "C1", "C3", "D1", "D3", "E1", "F4", "G1", "G3"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"2", "7", null, null, "2", "2", "2", "0", null, null},
            {"2", "9", null, null, "2", "2", "2", "0", null, null},
            {"2", "7", null, null, "2", "5", "2", "0", null, null},
            {"2", "9", null, null, "2", "5", "2", "0", null, null},
            {"5", "3", "3", "1", "2", "6", "2", "0", null, null},
            {"5", "3", "3", "4", "2", "6", "2", "0", null, null},
            {"5", "3", "3", "7", "2", "6", "2", "0", null, null},
            {"3", "1", "1", "1", "2", "9", "2", "0", null, null},
            {"3", "1", "1", "2", "2", "9", "2", "0", null, null},
            {"7", null, null, null, "1", "2", "1", null, null, null}
        };

        JDBC.assertFullResultSet(rs, expRS);

        //
        //------------------------------------------------------------
        // Case 1.21: Linearization in two branches of inner join
        //            Inner join ON predicate a1=e1 has no effect on
        //            linearization
        //------------------------------------------------------------
        //
        //                 IJ                             IJ -- a1=e3
        //               /  | \                        /    \
        //              /   LJ a1=e3                  /      LJ
        //             /  / | \        to            /      / | \
        //          LJ   A  LJ a2=b2              LJ       LJ C  b1=c1
        //        / | \   / | \                 / | \     / | \
        //       /  |  \ B  C  b1=c1         LJ   G f1=g1 A B a2=b2
        //    LJ    LJ e2=f2               / | \
        //  / | \ / | \                 LJ   F e2=f2
        // D  E | F G f1=g1           / | \
        //    d1=e1                  D  E d1=e1
        //

        rs = st.executeQuery(
            "select a1,b1,c1,c3,d1,d3,e1,f4,g1,g3"
            + "  from (A left join (b left join c on b1=c1) on a2=b2) "
            + "       inner join "
            + "       ((D left join E on d1=e1) left join (F left "
            + "join G on f1=g1) on e2=f2)"
            + "       on a1=e3"
            + " order by 10,9,8,7,6,5,4,3,2,1");

        expColNames = new String [] {"A1", "B1", "C1", "C3", "D1", "D3", "E1", "F4", "G1", "G3"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"2", "7", null, null, "2", "2", "2", "0", null, null},
            {"2", "9", null, null, "2", "2", "2", "0", null, null},
            {"2", "7", null, null, "2", "5", "2", "0", null, null},
            {"2", "9", null, null, "2", "5", "2", "0", null, null},
            {"2", "7", null, null, "2", "6", "2", "0", null, null},
            {"2", "9", null, null, "2", "6", "2", "0", null, null},
            {"2", "7", null, null, "2", "9", "2", "0", null, null},
            {"2", "9", null, null, "2", "9", "2", "0", null, null},
            {"3", "1", "1", "1", "1", "2", "1", null, null, null},
            {"3", "1", "1", "2", "1", "2", "1", null, null, null},
            {"3", "1", "1", "1", "1", "3", "1", null, null, null},
            {"3", "1", "1", "2", "1", "3", "1", null, null, null}
        };

        JDBC.assertFullResultSet(rs, expRS);

        //
        //------------------------------------------------------------
        // Case 1.22: Inner composite is a long left deep tree
        //            Lefty linearized
        //------------------------------------------------------------

        rs = st.executeQuery(
            "select a1,b1,c1,c3,d1,d3,e1,f4,g1,g3"
            + "  from A left join (b left join c on b1=c1 left "
            + "join D on c2=d2 "
            + "       left join E on d1=e1 left join F on f2=e2 "
            + "left join G on f1=g1)"
            + "       on a1=b2"
            + " order by 10,9,8,7,6,5,4,3,2,1");

        expColNames = new String [] {"A1", "B1", "C1", "C3", "D1", "D3", "E1", "F4", "G1", "G3"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"4", "1", "1", "2", "1", "2", "1", null, null, null},
            {"2", "3", "3", "7", "1", "2", "1", null, null, null},
            {"4", "1", "1", "1", null, null, null, null, null, null},
            {"2", "3", "3", "1", null, null, null, null, null, null},
            {"2", "3", "3", "4", null, null, null, null, null, null},
            {"5", "4", null, null, null, null, null, null, null, null},
            {"7", "6", null, null, null, null, null, null, null, null},
            {"3", "7", null, null, null, null, null, null, null, null},
            {"3", "9", null, null, null, null, null, null, null, null},
            {"1", null, null, null, null, null, null, null, null, null},
            {"6", null, null, null, null, null, null, null, null, null},
            {"8", null, null, null, null, null, null, null, null, null}
        };

        JDBC.assertFullResultSet(rs, expRS);

        //
        //------------------------------------------------------------
        // Case 1.23: Inner composite is a long left deep tree
        //            Partially linearized because of a1=c3
        //------------------------------------------------------------

        rs = st.executeQuery(
            "select a1,b1,c1,c3,d1,d3,e1,f4,g1,g3"
            + "  from A left join (b left join c on b1=c1 left "
            + "join D on c2=d2 "
            + "       left join E on d1=e1 left join F on f2=e2 "
            + "left join G on f1=g1)"
            + "       on a1=c3"
            + " order by 10,9,8,7,6,5,4,3,2,1");

        expColNames = new String [] {"A1", "B1", "C1", "C3", "D1", "D3", "E1", "F4", "G1", "G3"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"2", "1", "1", "2", "1", "2", "1", null, null, null},
            {"7", "3", "3", "7", "1", "2", "1", null, null, null},
            {"1", "1", "1", "1", null, null, null, null, null, null},
            {"1", "3", "3", "1", null, null, null, null, null, null},
            {"4", "3", "3", "4", null, null, null, null, null, null},
            {"3", null, null, null, null, null, null, null, null, null},
            {"5", null, null, null, null, null, null, null, null, null},
            {"6", null, null, null, null, null, null, null, null, null},
            {"8", null, null, null, null, null, null, null, null, null}
        };

        JDBC.assertFullResultSet(rs, expRS);

        //
        //------------------------------------------------------------
        // Case 1.24: Inner composite is a long left deep tree
        //            Partially linearized because of a1=d3
        //------------------------------------------------------------

        rs = st.executeQuery(
            "select a1,b1,c1,c3,d1,d3,e1,f4,g1,g3"
            + "  from A left join (b left join c on b1=c1 left "
            + "join D on c2=d2 "
            + "       left join E on d1=e1 left join F on f2=e2 "
            + "left join G on f1=g1)"
            + "       on a1=d3"
            + " order by 10,9,8,7,6,5,4,3,2,1");

        expColNames = new String [] {"A1", "B1", "C1", "C3", "D1", "D3", "E1", "F4", "G1", "G3"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"2", "1", "1", "2", "1", "2", "1", null, null, null},
            {"2", "3", "3", "7", "1", "2", "1", null, null, null},
            {"1", null, null, null, null, null, null, null, null, null},
            {"3", null, null, null, null, null, null, null, null, null},
            {"4", null, null, null, null, null, null, null, null, null},
            {"5", null, null, null, null, null, null, null, null, null},
            {"6", null, null, null, null, null, null, null, null, null},
            {"7", null, null, null, null, null, null, null, null, null},
            {"8", null, null, null, null, null, null, null, null, null}
        };

        JDBC.assertFullResultSet(rs, expRS);

        //
        //------------------------------------------------------------
        // Case 1.25: Left join in inner composite
        //            A case for future extension because the LJ is
        //            converted to LJ later
        //------------------------------------------------------------
        //
        // Maximum linearization:
        //
        //                 LJ                           LJ
        //               / | \                        / | \
        //              A  LJ a1=b3                 LJ  D c2=d2
        //               / | \        to          / | \
        //             LJ  D c2=d2              LJ  C b1=c1
        //           / | \                    / | \
        //          B  C b1=c1               A  B a1=b3
        //
        //  A <-- (B <-> C <-- D)         A <-- B <-- C <-- D
        //  =====================  ==>    ===================
        //  Y      Y     Y     Y          Y     Y     Y     Y
        //  Y      Y     Y     N          Y     Y     Y     N
        //         N     Y     Y          Y     Y     N     N
        //         N     Y     N          Y     N     N     N
        //  Y      Y     N     N
        //  Y      N     N     N
        //
        // Current optimization:
        //
        //         LJ                  LJ                         LJ
        //       / | \               / | \                       / | \
        //      A  LJ a1=b3        LJ  D c2=d2                 LJ  D c2=d2
        //       / | \      to   / | \         later LJ      / | \
        //     LJ  D c2=d2      A  LJ a1=b3    becomes LJ   A  LJ a1=b3
        //   / | \               / | \                       / | \
        //  B  C b1=c1          B  C b1=c1                  B  C b1=c1
        //

        rs = st.executeQuery(
            "select a1,b1,c1,c3,d1,d3"
            + "  from A left join (b left join c on b1=c1 left "
            + "join D on c2=d2) on a1=b3"
            + " order by 6,5,4,3,2,1");

        expColNames = new String [] {"A1", "B1", "C1", "C3", "D1", "D3"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"2", "1", "1", "2", "1", "2"},
            {"3", "3", "3", "7", "1", "2"},
            {"2", "1", "1", "1", null, null},
            {"3", "3", "3", "1", null, null},
            {"3", "3", "3", "4", null, null},
            {"2", "5", null, null, null, null},
            {"2", "6", null, null, null, null},
            {"3", "7", null, null, null, null},
            {"3", "9", null, null, null, null},
            {"1", null, null, null, null, null},
            {"4", null, null, null, null, null},
            {"5", null, null, null, null, null},
            {"6", null, null, null, null, null},
            {"7", null, null, null, null, null},
            {"8", null, null, null, null, null}
        };

        JDBC.assertFullResultSet(rs, expRS);

        //
        //------------------------------------------------------------
        // Case 1.26: Inner join in inner composite
        //            Partially linearized because of the inner
        //------------------------------------------------------------

        rs = st.executeQuery(
            "select a1,b1,c1,c3,d1,d3"
            + "  from A left join (B join C on b1=c1 left join D "
            + " on c2=d2) on a1=b3"
            + " order by 6,5,4,3,2,1");

        expColNames = new String [] {"A1", "B1", "C1", "C3", "D1", "D3"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"2", "1", "1", "2", "1", "2"},
            {"3", "3", "3", "7", "1", "2"},
            {"2", "1", "1", "1", null, null},
            {"3", "3", "3", "1", null, null},
            {"3", "3", "3", "4", null, null},
            {"1", null, null, null, null, null},
            {"4", null, null, null, null, null},
            {"5", null, null, null, null, null},
            {"6", null, null, null, null, null},
            {"7", null, null, null, null, null},
            {"8", null, null, null, null, null}
        };

        JDBC.assertFullResultSet(rs, expRS);

        //
        //------------------------------------------------------------
        // Case 1.27: Left join in inner composite null-producing side
        //            A case for future extension because the LJ is
        //            converted to LJ later
        //------------------------------------------------------------
        //
        // Maximum linearization:
        //
        //                 LJ                          LJ
        //               /  | \                      / | \
        //             A   LJ  a1=b3              LJ   E d1=e1
        //               /  | \       to        / | \
        //              /   |  c2=d2          LJ  D c2=d2
        //           LJ     LJ              / | \
        //         / | \   / | \           LJ C b1=c1
        //        B  C  | D  E d1=e1     / | \
        //            b1=c1             A  B a1=b3
        //
        //
        //  A <-- ((B <-- C) <-- (D <-> E))      A <-- B <-- C <-- D <-- E
        //  =============================  ==>  ==========================
        //  Y       Y     Y       Y     Y        Y     Y     Y     Y     Y
        //  Y       Y     Y       Y     N        Y     Y     Y     Y     N
        //  Y       Y     Y       N     N        Y     Y     Y     N     N
        //  Y       Y     N       N     N        Y     Y     N     N     N
        //  Y       N     N       N     N        Y     N     N     N     N
        //                        N     Y
        //
        // Current optimization:
        //
        //                                       LJ                                 LJ
        //          LJ                          / | \                             / | \
        //        /  | \                      /   |  c2=d2                      /   |  c2=d2
        //      A   LJ a1=b3              LJ     LJ                         LJ     LJ
        //        /  | \       to       / | \  / | \      Later LJ        / | \  / | \
        //       /  LJ c2=d2         LJ   C  | D E d1=e1  becomes LJ   LJ   C  | D E d1=e1  becomes LJ
        //    LJ   / | \           / | \    b1=c1                    / | \    b1=c1
        //   / | \ D E d1=e1      A  B a1=b3                        A  B a1=b3
        //  B  C  |
        //      b1=c1
        //

        rs = st.executeQuery(
            "select a1,b1,c1,c3,d1,d3,e1"
            + "  from A left join ((B left join C on b1=c1) left "
            + "join (D left join E on d1=e1) on c2=d2) on a1=b3"
            + " order by 7,6,5,4,3,2,1");

        expColNames = new String [] {"A1", "B1", "C1", "C3", "D1", "D3", "E1"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"2", "1", "1", "2", "1", "2", "1"},
            {"3", "3", "3", "7", "1", "2", "1"},
            {"2", "1", "1", "1", null, null, null},
            {"3", "3", "3", "1", null, null, null},
            {"3", "3", "3", "4", null, null, null},
            {"2", "5", null, null, null, null, null},
            {"2", "6", null, null, null, null, null},
            {"3", "7", null, null, null, null, null},
            {"3", "9", null, null, null, null, null},
            {"1", null, null, null, null, null, null},
            {"4", null, null, null, null, null, null},
            {"5", null, null, null, null, null, null},
            {"6", null, null, null, null, null, null},
            {"7", null, null, null, null, null, null},
            {"8", null, null, null, null, null, null}
        };

        JDBC.assertFullResultSet(rs, expRS);

        //
        //------------------------------------------------------------
        // Case 1.28: Inner join in inner composite null-producing side
        //------------------------------------------------------------

        rs = st.executeQuery(
            "select a1,b1,c1,c3,d1,d3,e1"
            + "  from A left join ((B left join C on b1=c1) left "
            + "join (D join E on d1=e1) on c2=d2) on a1=b3"
            + " order by 7,6,5,4,3,2,1");

        expColNames = new String [] {"A1", "B1", "C1", "C3", "D1", "D3", "E1"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"2", "1", "1", "2", "1", "2", "1"},
            {"3", "3", "3", "7", "1", "2", "1"},
            {"2", "1", "1", "1", null, null, null},
            {"3", "3", "3", "1", null, null, null},
            {"3", "3", "3", "4", null, null, null},
            {"2", "5", null, null, null, null, null},
            {"2", "6", null, null, null, null, null},
            {"3", "7", null, null, null, null, null},
            {"3", "9", null, null, null, null, null},
            {"1", null, null, null, null, null, null},
            {"4", null, null, null, null, null, null},
            {"5", null, null, null, null, null, null},
            {"6", null, null, null, null, null, null},
            {"7", null, null, null, null, null, null},
            {"8", null, null, null, null, null, null}
        };

        JDBC.assertFullResultSet(rs, expRS);

        //
        //------------------------------------------------------------
        // Case 1.29: No linearization because local predicate only
        //------------------------------------------------------------

        rs = st.executeQuery(
            "select a1,b1,c1,c3,d1,d3,e1"
            + "  from A left join ((B left join C on b1=c1) left "
            + "join (D join E on d1=e1) on c2=d2) on b2=2"
            + " order by 7,6,5,4,3,2,1");

        expColNames = new String [] {"A1", "B1", "C1", "C3", "D1", "D3", "E1"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1", "3", "3", "7", "1", "2", "1"},
            {"2", "3", "3", "7", "1", "2", "1"},
            {"3", "3", "3", "7", "1", "2", "1"},
            {"4", "3", "3", "7", "1", "2", "1"},
            {"5", "3", "3", "7", "1", "2", "1"},
            {"6", "3", "3", "7", "1", "2", "1"},
            {"7", "3", "3", "7", "1", "2", "1"},
            {"8", "3", "3", "7", "1", "2", "1"},
            {"1", "3", "3", "1", null, null, null},
            {"2", "3", "3", "1", null, null, null},
            {"3", "3", "3", "1", null, null, null},
            {"4", "3", "3", "1", null, null, null},
            {"5", "3", "3", "1", null, null, null},
            {"6", "3", "3", "1", null, null, null},
            {"7", "3", "3", "1", null, null, null},
            {"8", "3", "3", "1", null, null, null},
            {"1", "3", "3", "4", null, null, null},
            {"2", "3", "3", "4", null, null, null},
            {"3", "3", "3", "4", null, null, null},
            {"4", "3", "3", "4", null, null, null},
            {"5", "3", "3", "4", null, null, null},
            {"6", "3", "3", "4", null, null, null},
            {"7", "3", "3", "4", null, null, null},
            {"8", "3", "3", "4", null, null, null}
        };

        JDBC.assertFullResultSet(rs, expRS);

        //
        //------------------------------------------------------------
        // Case 1.30: No linearization because local predicate only
        //------------------------------------------------------------

        rs = st.executeQuery(
            "select a1,b1,c1,c3,d1,d3,e1"
            + "  from A left join ((B left join C on b1=c1) left "
            + "join (D join E on d1=e1) on c2=d2) on a1=1 and b2=2"
            + " order by 7,6,5,4,3,2,1");

        expColNames = new String [] {"A1", "B1", "C1", "C3", "D1", "D3", "E1"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1", "3", "3", "7", "1", "2", "1"},
            {"1", "3", "3", "1", null, null, null},
            {"1", "3", "3", "4", null, null, null},
            {"2", null, null, null, null, null, null},
            {"3", null, null, null, null, null, null},
            {"4", null, null, null, null, null, null},
            {"5", null, null, null, null, null, null},
            {"6", null, null, null, null, null, null},
            {"7", null, null, null, null, null, null},
            {"8", null, null, null, null, null, null}
        };

        JDBC.assertFullResultSet(rs, expRS);

        //
        //------------------------------------------------------------
        // Case 1.31: Linearization with local predicate
        //------------------------------------------------------------

        // Update: With DERBY-4471, this query also gets reordered, twice, cf plan
        // check below. Previously it did not get any reordering, due to
        // presence of b2=2, inspite of the comment above..

        rs = st.executeQuery(
            "select a1,b1,c1,c3,d1,d3,e1"
            + "  from A left join ((B left join C on b1=c1) left "
            + "join (D join E on d1=e1) on c2=d2) on a1=b1 and b2=2"
            + " order by 7,6,5,4,3,2,1");

        expColNames = new String [] {"A1", "B1", "C1", "C3", "D1", "D3", "E1"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"3", "3", "3", "7", "1", "2", "1"},
            {"3", "3", "3", "1", null, null, null},
            {"3", "3", "3", "4", null, null, null},
            {"1", null, null, null, null, null, null},
            {"2", null, null, null, null, null, null},
            {"4", null, null, null, null, null, null},
            {"5", null, null, null, null, null, null},
            {"6", null, null, null, null, null, null},
            {"7", null, null, null, null, null, null},
            {"8", null, null, null, null, null, null}
        };

        JDBC.assertFullResultSet(rs, expRS);

        JDBC.checkPlan(st,
                  new String[] {
                      "Sort ResultSet:",
                      "Source result set:",
                      "_Project-Restrict ResultSet (11):",
                      "_Source result set:",
                      "__Nested Loop Left Outer Join ResultSet:",
                      "__Left result set:",
                      "___Hash Left Outer Join ResultSet:",
                      "___Left result set:",
                      "____Hash Left Outer Join ResultSet:",
                      "____Left result set:",
                      "_____Index Scan ResultSet for A",
                      "____Right result set:",
                      "_____Hash Scan ResultSet for B",
                      "___Right result set:",
                      "____Hash Scan ResultSet for C",
                      "__Right result set:",
                      "___Project-Restrict ResultSet (10):",
                      "___Source result set:",
                      "____Hash Exists Join ResultSet:",
                      "____Left result set:",
                      "_____Table Scan ResultSet for D",
                      "____Right result set:",
                      "_____Hash Scan ResultSet for E"});

        //
        //------------------------------------------------------------
        // Case 1.32: No linearization because join is between the tables in
        //            inner row-preserving side
        //------------------------------------------------------------

        rs = st.executeQuery(
            "select a1,b1,c1,c3,d1,d3,e1"
            + "  from A left join ((B left join C on b1=c1) left "
            + "join (D join E on d1=e1) on c2=d2) on b2=c2"
            + " order by 7,6,5,4,3,2,1");

        expColNames = new String [] {"A1", "B1", "C1", "C3", "D1", "D3", "E1"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1", "1", "1", "1", null, null, null},
            {"2", "1", "1", "1", null, null, null},
            {"3", "1", "1", "1", null, null, null},
            {"4", "1", "1", "1", null, null, null},
            {"5", "1", "1", "1", null, null, null},
            {"6", "1", "1", "1", null, null, null},
            {"7", "1", "1", "1", null, null, null},
            {"8", "1", "1", "1", null, null, null}
        };

        JDBC.assertFullResultSet(rs, expRS);

        //
        // *********************************************************
        // ** Group 2: Inner Join Reordering
        // *********************************************************

        //------------------------------------------------------------
        // Case 2.1 : Simple case - Right branch has LJ
        //            Single unique key (a1)
        //            Reordered as ON predicate contains unique keypart
        //------------------------------------------------------------
        //   Unique keys: A(A1), B(B1), C(C1,C3), D(D1,D3)
        //   Right Deep Tree  to    Left Deep Tree
        //
        //       IJ                        LJ
        //     / | \                     / | \
        //    A  LJ a1=b1              IJ  C  b2=c2
        //     / | \                 / | \
        //    B  C  b2=c2           B  A  a1=b1
        //


        rs = st.executeQuery(
            "select a1,b1,c1,c3"
            + "  from A join (B left join C on b2=c2) on a1=b1"
            + " order by 4,3,2,1");

        expColNames = new String [] {"A1", "B1", "C1", "C3"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1", "1", "1", "1"},
            {"6", "6", "1", "2"},
            {"3", "3", "2", "5"},
            {"6", "6", "3", "7"},
            {"7", "7", "8", "9"},
            {"4", "4", null, null},
            {"5", "5", null, null}
        };

        JDBC.assertFullResultSet(rs, expRS);

        //
        //------------------------------------------------------------
        // Case 2.2 : Simple case - Right branch has LJ
        //            Single unique key (a1) with local predicate
        //------------------------------------------------------------
        //   Unique keys: A(A1), B(B1), C(C1,C3), D(D1,D3)
        //   Right Deep Tree  to    Left Deep Tree
        //
        //       IJ                        LJ
        //     / | \                     / | \
        //    A  LJ a1=2               IJ  C  b2=c2
        //     / | \                 / | \
        //    B  C  b2=c2           B  A  a1=2
        //

        rs = st.executeQuery(
            "select a1,b1,c1,c3"
            + "  from A join (B left join C on b2=c2) on a1=2"
            + " order by 4,3,2,1");

        expColNames = new String [] {"A1", "B1", "C1", "C3"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"2", "1", "1", "1"},
            {"2", "6", "1", "2"},
            {"2", "3", "2", "5"},
            {"2", "6", "3", "7"},
            {"2", "7", "8", "9"},
            {"2", "9", "8", "9"},
            {"2", "4", null, null},
            {"2", "5", null, null}
        };

        JDBC.assertFullResultSet(rs, expRS);

        //
        //------------------------------------------------------------
        // Case 2.3 : Simple case - right branch has LJ
        //            Single unique key (a1), ON clause has self-join only
        //            No reordering because of self-join only
        //------------------------------------------------------------
        //   Unique keys: A(A1), B(B1), C(C1,C3), D(D1,D3)
        //   Right Deep Tree
        //
        //       IJ
        //     / | \
        //    A  LJ a1=a3
        //     / | \
        //    B  C  b2=c2
        //

        rs = st.executeQuery(
            "select a1,b1,c1,c3"
            + "  from A join (B left join C on b2=c2) on a1=a3"
            + " order by 4,3,2,1");

        expColNames = new String [] {"A1", "B1", "C1", "C3"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"2", "1", "1", "1"},
            {"4", "1", "1", "1"},
            {"8", "1", "1", "1"},
            {"2", "6", "1", "2"},
            {"4", "6", "1", "2"},
            {"8", "6", "1", "2"},
            {"2", "3", "2", "5"},
            {"4", "3", "2", "5"},
            {"8", "3", "2", "5"},
            {"2", "6", "3", "7"},
            {"4", "6", "3", "7"},
            {"8", "6", "3", "7"},
            {"2", "7", "8", "9"},
            {"4", "7", "8", "9"},
            {"8", "7", "8", "9"},
            {"2", "9", "8", "9"},
            {"4", "9", "8", "9"},
            {"8", "9", "8", "9"},
            {"2", "4", null, null},
            {"4", "4", null, null},
            {"8", "4", null, null},
            {"2", "5", null, null},
            {"4", "5", null, null},
            {"8", "5", null, null}
        };

        JDBC.assertFullResultSet(rs, expRS);

        //
        //------------------------------------------------------------
        // Case 2.4 : Simple case - Right branch has LJ
        //            Single unique key (a1)
        //            No reordering because of a1=c1
        //            OJ2SEL will convert the B LJ C to B IJ C
        //------------------------------------------------------------
        //   Unique keys: A(A1), B(B1), C(C1,C3), D(D1,D3)
        //   Right Deep Tree
        //
        //       IJ
        //     / | \
        //    A  LJ a1=c1
        //     / | \
        //    B  C  b2=c2
        //

        rs = st.executeQuery(
            "select a1,b1,c1,c3"
            + "  from A join (B left join C on b2=c2) on a1=c1"
            + " order by 4,3,2,1");

        expColNames = new String [] {"A1", "B1", "C1", "C3"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1", "1", "1", "1"},
            {"1", "6", "1", "2"},
            {"2", "3", "2", "5"},
            {"3", "6", "3", "7"},
            {"8", "7", "8", "9"},
            {"8", "9", "8", "9"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        //
        //------------------------------------------------------------
        // Case 2.5 : Simple case - Right branch has LJ
        //            Composite unique key (d1,d3), ON clause has local prd
        //            Reordered as ON predicate contains all unique keyparts
        //------------------------------------------------------------
        //   Unique keys: A(A1), B(B1), C(C1,C3), D(D1,D3)
        //   Right Deep Tree  to    Left Deep Tree
        //
        //       IJ                        LJ
        //     / | \                     / | \
        //    D  LJ d1=1 & d3=b3       IJ  C  b2=c2
        //     / | \                 / | \
        //    B  C  b2=c2           B  D  d1=1 & d3=b3
        //

        rs = st.executeQuery(
            "select b1,c1,c3,d1,d3 "
            + "  from D join (B left join C on b2=c2) on d1=1 and d3=b3"
            + " order by 5,4,3,2,1");

        expColNames = new String [] {"B1", "C1", "C3", "D1", "D3"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1", "1", "1", "1", "2"},
            {"6", "1", "2", "1", "2"},
            {"6", "3", "7", "1", "2"},
            {"5", null, null, "1", "2"},
            {"3", "2", "5", "1", "3"},
            {"7", "8", "9", "1", "3"},
            {"9", "8", "9", "1", "3"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        //
        //------------------------------------------------------------
        // Case 2.6 : Simple case - Right branch has LJ
        //            Composite unique key (d1,d3), ON clause has self-join
        //            No reordering because of ON clause does not have non-
        //            selLJoin predicates that contain all unique keyparts
        //------------------------------------------------------------
        //   Unique keys: A(A1), B(B1), C(C1,C3), D(D1,D3)
        //
        //       IJ
        //     / | \
        //    D  LJ d1=b3 & d3=d2
        //     / | \
        //    B  C  b2=c2
        //

        rs = st.executeQuery(
            "select b1,c1,c3,d1,d3 "
            + "  from D join (B left join C on b2=c2) on d1=b3 and d3=d2"
            + " order by 5,4,3,2,1");

        expColNames = new String [] {"B1", "C1", "C3", "D1", "D3"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1", "1", "1", "2", "2"},
            {"6", "1", "2", "2", "2"},
            {"6", "3", "7", "2", "2"},
            {"5", null, null, "2", "2"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        //
        //------------------------------------------------------------
        // Case 2.7 : Simple case - Right branch has LJ
        //            Composite unique key (d1,d3), ON clause has self-join
        //            Reordered because all unique keyparts are involved in
        //            non-selLJoin predicates
        //------------------------------------------------------------
        //   Unique keys: A(A1), B(B1), C(C1,C3), D(D1,D3)
        //   Right Deep Tree  to    Left Deep Tree
        //
        //       IJ                        LJ
        //     / | \                     / | \
        //    D  LJ d1=b3 & d1=d6      IJ  C  b2=c2
        //     / | \      & d3=b3    / | \
        //    B  C  b2=c2           B  D  d1=b3 & d1=d6 & d3=b3
        //

        rs = st.executeQuery(
            "select b1,c1,c3,d1,d3 "
            + "  from D join (B left join C on b2=c2) on d1=b3 and "
            + "d1=d6 and d3=b3"
            + " order by 5,4,3,2,1");

        expColNames = new String [] {"B1", "C1", "C3", "D1", "D3"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1", "1", "1", "2", "2"},
            {"6", "1", "2", "2", "2"},
            {"6", "3", "7", "2", "2"},
            {"5", null, null, "2", "2"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        //
        //------------------------------------------------------------
        // Case 2.8 : Simple case - Left branch has LJ
        //            Single unique key (a1)
        //            Reordered as ON predicate contains unique keypart
        //------------------------------------------------------------
        //   Unique keys: A(A1), B(B1), C(C1,C3), D(D1,D3)
        //
        //       IJ                     LJ
        //     / | \                  / | \
        //    LJ A a1=b1   to       IJ  C  b2=c2
        //  / | \                 / | \
        // B  C  b2=c2           B  A  a1=b1
        //

        rs = st.executeQuery(
            "select a1,b1,c1,c3"
            + "  from B left join C on b2=c2 join A on a1=b1"
            + " order by 4,3,2,1");

        expColNames = new String [] {"A1", "B1", "C1", "C3"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1", "1", "1", "1"},
            {"6", "6", "1", "2"},
            {"3", "3", "2", "5"},
            {"6", "6", "3", "7"},
            {"7", "7", "8", "9"},
            {"4", "4", null, null},
            {"5", "5", null, null}
        };

        JDBC.assertFullResultSet(rs, expRS);

        //
        //------------------------------------------------------------
        // Case 2.9 : Simple case - Left branch has LJ
        //            Single unique key (a1) with local predicate
        //------------------------------------------------------------
        //   Unique keys: A(A1), B(B1), C(C1,C3), D(D1,D3)
        //
        //       IJ                     LJ
        //     / | \                  / | \
        //    LJ A a1=2    to       IJ  C  b2=c2
        //  / | \                 / | \
        // B  C  b2=c2           B  A  a1=2
        //

        rs = st.executeQuery(
            "select a1,b1,c1,c3"
            + "  from B left join C on b2=c2 join A on a1=2"
            + " order by 4,3,2,1");

        expColNames = new String [] {"A1", "B1", "C1", "C3"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"2", "1", "1", "1"},
            {"2", "6", "1", "2"},
            {"2", "3", "2", "5"},
            {"2", "6", "3", "7"},
            {"2", "7", "8", "9"},
            {"2", "9", "8", "9"},
            {"2", "4", null, null},
            {"2", "5", null, null}
        };

        JDBC.assertFullResultSet(rs, expRS);

        //
        //------------------------------------------------------------
        // Case 2.10: Simple case - Left branch has LJ
        //            Single unique key (a1), ON clause has self-join only
        //            No reordering because of self-join only
        //------------------------------------------------------------
        //   Unique keys: A(A1), B(B1), C(C1,C3), D(D1,D3)
        //
        //       IJ
        //     / | \
        //    LJ A a1=a3
        //  / | \
        // B  C  b2=c2
        //

        rs = st.executeQuery(
            "select a1,b1,c1,c3"
            + "  from B left join C on b2=c2 join A on a1=a3"
            + " order by 4,3,2,1");

        expColNames = new String [] {"A1", "B1", "C1", "C3"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"2", "1", "1", "1"},
            {"4", "1", "1", "1"},
            {"8", "1", "1", "1"},
            {"2", "6", "1", "2"},
            {"4", "6", "1", "2"},
            {"8", "6", "1", "2"},
            {"2", "3", "2", "5"},
            {"4", "3", "2", "5"},
            {"8", "3", "2", "5"},
            {"2", "6", "3", "7"},
            {"4", "6", "3", "7"},
            {"8", "6", "3", "7"},
            {"2", "7", "8", "9"},
            {"4", "7", "8", "9"},
            {"8", "7", "8", "9"},
            {"2", "9", "8", "9"},
            {"4", "9", "8", "9"},
            {"8", "9", "8", "9"},
            {"2", "4", null, null},
            {"4", "4", null, null},
            {"8", "4", null, null},
            {"2", "5", null, null},
            {"4", "5", null, null},
            {"8", "5", null, null}
        };

        JDBC.assertFullResultSet(rs, expRS);

        //
        //------------------------------------------------------------
        // Case 2.11: Simple case - Left branch has LJ
        //            Single unique key (a1)
        //            No reordering because of a1=c1
        //            OJ2SEL will convert the B LJ C to B IJ C
        //------------------------------------------------------------
        //   Unique keys: A(A1), B(B1), C(C1,C3), D(D1,D3)
        //
        //       IJ
        //     / | \
        //    LJ A a1=c1
        //  / | \
        // B  C  b2=c2
        //

        rs = st.executeQuery(
            "select a1,b1,c1,c3"
            + "  from B left join C on b2=c2 join A on a1=c1"
            + " order by 4,3,2,1");

        expColNames = new String [] {"A1", "B1", "C1", "C3"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1", "1", "1", "1"},
            {"1", "6", "1", "2"},
            {"2", "3", "2", "5"},
            {"3", "6", "3", "7"},
            {"8", "7", "8", "9"},
            {"8", "9", "8", "9"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        //
        //------------------------------------------------------------
        // Case 2.12: Simple case - Left branch has LJ
        //            Composite unique key (d1,d3), ON clause has local prd
        //            Reordered as ON predicate contains all unique keyparts
        //------------------------------------------------------------
        //   Unique keys: A(A1), B(B1), C(C1,C3), D(D1,D3)
        //
        //       IJ                        LJ
        //     / | \                     / | \
        //    LJ D d1=1 & d3=b3  to    IJ  C  b2=c2
        //  / | \                    / | \
        // B  C  b2=c2              B  D  d1=1 & d3=b3
        //

        rs = st.executeQuery(
            "select b1,c1,c3,d1,d3 "
            + "  from B left join C on b2=c2 join D on d1=1 and d3=b3"
            + " order by 5,4,3,2,1");

        expColNames = new String [] {"B1", "C1", "C3", "D1", "D3"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1", "1", "1", "1", "2"},
            {"6", "1", "2", "1", "2"},
            {"6", "3", "7", "1", "2"},
            {"5", null, null, "1", "2"},
            {"3", "2", "5", "1", "3"},
            {"7", "8", "9", "1", "3"},
            {"9", "8", "9", "1", "3"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        //
        //------------------------------------------------------------
        // Case 2.13: Simple case - Left branch has LJ
        //            Composite unique key (d1,d3), ON clause has self-join
        //            No reordering because of ON clause does not have non-
        //            selLJoin predicates that contain all unique keyparts
        //------------------------------------------------------------
        //   Unique keys: A(A1), B(B1), C(C1,C3), D(D1,D3)
        //
        //       IJ
        //     / | \
        //    LJ D d1=b3 & d3=d2
        //  / | \
        // B  C  b2=c2
        //

        rs = st.executeQuery(
            "select b1,c1,c3,d1,d3 "
            + "  from B left join C on b2=c2 join D on d1=b3 and d3=d2"
            + " order by 5,4,3,2,1");

        expColNames = new String [] {"B1", "C1", "C3", "D1", "D3"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1", "1", "1", "2", "2"},
            {"6", "1", "2", "2", "2"},
            {"6", "3", "7", "2", "2"},
            {"5", null, null, "2", "2"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        //
        //------------------------------------------------------------
        // Case 2.14: Simple case - Left branch has LJ
        //            Composite unique key (d1,d3), ON clause has self-join
        //            Reordered because all unique keyparts are involved in
        //            non-selLJoin predicates
        //------------------------------------------------------------
        //   Unique keys: A(A1), B(B1), C(C1,C3), D(D1,D3)
        //
        //       IJ                         LJ
        //     / | \                      / | \
        //    LJ D d1=b3 & d1=d6  to    IJ  C  b2=c2
        //  / | \        & d3=b3      / | \
        // B  C  b2=c2               B  D  d1=b3 & d1=d6 & d3=b3
        //

        rs = st.executeQuery(
            "select b1,c1,c3,d1,d3 "
            + "  from B left join C on b2=c2 join D on d1=b3 and "
            + "d1=d6 and d3=b3"
            + " order by 5,4,3,2,1");

        expColNames = new String [] {"B1", "C1", "C3", "D1", "D3"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1", "1", "1", "2", "2"},
            {"6", "1", "2", "2", "2"},
            {"6", "3", "7", "2", "2"},
            {"5", null, null, "2", "2"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        //
        //------------------------------------------------------------
        // Case 2.15: Simple case - Right branch has LJ
        //            Single unique key
        //            No reordering because of non-colequiv class
        //------------------------------------------------------------
        //   Unique keys: A(A1), B(B1), C(C1,C3), D(D1,D3)
        //
        //       IJ
        //     / | \
        //    A  LJ a1>b1
        //     / | \
        //    B  C  b2=c2
        //

        rs = st.executeQuery(
            "select a1,b1,c1,c3 "
            + "  from A join (B left join C on b2=c2) on a1>b1"
            + " order by 4,3,2,1");

        expColNames = new String [] {"A1", "B1", "C1", "C3"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"2", "1", "1", "1"},
            {"3", "1", "1", "1"},
            {"4", "1", "1", "1"},
            {"5", "1", "1", "1"},
            {"6", "1", "1", "1"},
            {"7", "1", "1", "1"},
            {"8", "1", "1", "1"},
            {"7", "6", "1", "2"},
            {"8", "6", "1", "2"},
            {"4", "3", "2", "5"},
            {"5", "3", "2", "5"},
            {"6", "3", "2", "5"},
            {"7", "3", "2", "5"},
            {"8", "3", "2", "5"},
            {"7", "6", "3", "7"},
            {"8", "6", "3", "7"},
            {"8", "7", "8", "9"},
            {"5", "4", null, null},
            {"6", "4", null, null},
            {"7", "4", null, null},
            {"8", "4", null, null},
            {"6", "5", null, null},
            {"7", "5", null, null},
            {"8", "5", null, null}
        };

        JDBC.assertFullResultSet(rs, expRS);

        //
        //------------------------------------------------------------
        // Case 2.16: Simple case - Right branch has LJ
        //            Single unique key
        //            No reordering because of OR predicates
        //------------------------------------------------------------
        //   Unique keys: A(A1), B(B1), C(C1,C3), D(D1,D3)
        //
        //       IJ
        //     / | \
        //    A  LJ a1=b1 or a1=3
        //     / | \
        //    B  C  b2=c2
        //

        rs = st.executeQuery(
            "select a1,b1,c1,c3 "
            + "  from A join (B left join C on b2=c2) on a1=b1 or a1=3"
            + " order by 4,3,2,1");

        expColNames = new String [] {"A1", "B1", "C1", "C3"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1", "1", "1", "1"},
            {"3", "1", "1", "1"},
            {"3", "6", "1", "2"},
            {"6", "6", "1", "2"},
            {"3", "3", "2", "5"},
            {"3", "6", "3", "7"},
            {"6", "6", "3", "7"},
            {"3", "7", "8", "9"},
            {"7", "7", "8", "9"},
            {"3", "9", "8", "9"},
            {"3", "4", null, null},
            {"4", "4", null, null},
            {"3", "5", null, null},
            {"5", "5", null, null}
        };

        JDBC.assertFullResultSet(rs, expRS);

        //
        //------------------------------------------------------------
        // Case 2.17: Simple case - Right branch has LJ
        //            Single unique key
        //            No reordering because of c1=c2
        //            Later, OJ2SEL convert B LJ C to B IJ C
        //------------------------------------------------------------
        //   Unique keys: A(A1), B(B1), C(C1,C3), D(D1,D3)
        //
        //       IJ
        //     / | \
        //    A  LJ a1=b1 and c1=c2
        //     / | \
        //    B  C  b3=c2
        //

        rs = st.executeQuery(
            "select a1,b1,c1,c3 "
            + "  from A join (B left join C on b3=c2) on a1=b1 and c1=c2"
            + " order by 4,3,2,1");

        expColNames = new String [] {"A1", "B1", "C1", "C3"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1", "1", "2", "5"},
            {"5", "5", "2", "5"},
            {"6", "6", "2", "5"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        //
        //------------------------------------------------------------
        // Case 2.18: Simple case - Right branch has LJ
        //            Single unique key
        //            No reordering because of b3=c3
        //            Later, OJ2SEL convert B LJ C to B IJ C
        //------------------------------------------------------------
        //   Unique keys: A(A1), B(B1), C(C1,C3), D(D1,D3)
        //
        //       IJ
        //     / | \
        //    A  LJ a1=b1 and b3=c3
        //     / | \
        //    B  C  b3=c3
        //

        rs = st.executeQuery(
            "select a1,b1,c1,c3 "
            + "  from A join (B left join C on b3=c3) on a1=b1 and b3=c3"
            + " order by 4,3,2,1");

        expColNames = new String [] {"A1", "B1", "C1", "C3"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1", "1", "1", "2"},
            {"5", "5", "1", "2"},
            {"6", "6", "1", "2"},
            {"4", "4", "8", "9"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        //
        //------------------------------------------------------------
        // Case 2.19: Double inner join reordering
        //            Simple case
        //------------------------------------------------------------
        //   Unique keys: A(A1), B(B1), C(C1,C3), D(D1,D3)
        //
        //       IJ                          LJ
        //     / | \                       / | \
        //    D  IJ d1=1 & d3=b3  to      IJ  C  b2=c2
        //     / | \                    / | \
        //    A  LJ a1=b1             IJ  A  a1=b1
        //     / | \                / | \
        //    B  C  b2=c2          B  D  d1=1 & d3=b3
        //

        rs = st.executeQuery(
            "select a1,b1,c1,c3,d1,d3 "
            + "  from D join (A join (B left join C on b2=c2) on "
            + "a1=b1) on d1=1 and d3=b3"
            + " order by 6,5,4,3,2,1");

        expColNames = new String [] {"A1", "B1", "C1", "C3", "D1", "D3"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1", "1", "1", "1", "1", "2"},
            {"6", "6", "1", "2", "1", "2"},
            {"6", "6", "3", "7", "1", "2"},
            {"5", "5", null, null, "1", "2"},
            {"3", "3", "2", "5", "1", "3"},
            {"7", "7", "8", "9", "1", "3"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        //
        //------------------------------------------------------------
        // Case 2.20: Double inner join reordering
        //            Upper IJ reference lower IJ column
        //------------------------------------------------------------
        //   Unique keys: A(A1), B(B1), C(C1,C3), D(D1,D3)
        //
        //       IJ                          LJ
        //     / | \                       / | \
        //    D  IJ d1=a4 & d3=b3 to      IJ  C  b2=c2
        //     / | \                    / | \
        //    A  LJ a1=b1             IJ  A  a1=b1
        //     / | \                / | \
        //    B  C  b2=c2          B  D  d3=b3 & d1=a4
        //

        rs = st.executeQuery(
            "select a1,b1,c1,c3,d1,d3 "
            + "  from D join (A join (B left join C on b2=c2) on "
            + "a1=b1) on d3=b3 and d1=a4"
            + " order by 6,5,4,3,2,1");

        expColNames = new String [] {"A1", "B1", "C1", "C3", "D1", "D3"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"6", "6", "1", "2", "2", "2"},
            {"6", "6", "3", "7", "2", "2"},
            {"4", "4", null, null, "2", "9"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        //
        //------------------------------------------------------------
        // Case 2.21: No reordering because the lower inner join
        //            failed to reorder and upper inner join has d1=a1
        //------------------------------------------------------------
        //   Unique keys: A(A1), B(B1), C(C1,C3), D(D1,D3)
        //
        //       IJ
        //     / | \
        //    D  IJ d1=a1 & d3=b2
        //     / | \
        //    A  LJ a3=b3
        //     / | \
        //    B  C  b1=c1
        //

        rs = st.executeQuery(
            "select a1,b1,c1,c3,d1,d3 "
            + "  from D join (A join (B left join C on b1=c1) on "
            + "a3=b3) on d3=b2 and d1=a1"
            + " order by 6,5,4,3,2,1");

        expColNames = new String [] {"A1", "B1", "C1", "C3", "D1", "D3"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1", "3", "3", "1", "1", "2"},
            {"1", "3", "3", "4", "1", "2"},
            {"1", "3", "3", "7", "1", "2"},
            {"1", "7", null, null, "1", "3"},
            {"1", "9", null, null, "1", "3"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        // the following query (similar to above) should only
        // return 5 rows, beetle 5055

        rs = st.executeQuery(
            "select a1,b1,c1,c3,d1,d3,a3,b3,b2 from D "
            + "join (A join (B left join C on b1=c1) on a3=b3) on d3=b2 "
            + "and d1=a1 order by 6,5,4,3,2,1");

        expColNames = new String [] {"A1", "B1", "C1", "C3", "D1", "D3", "A3", "B3", "B2"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1", "3", "3", "1", "1", "2", "3", "3", "2"},
            {"1", "3", "3", "4", "1", "2", "3", "3", "2"},
            {"1", "3", "3", "7", "1", "2", "3", "3", "2"},
            {"1", "7", null, null, "1", "3", "3", "3", "3"},
            {"1", "9", null, null, "1", "3", "3", "3", "3"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        //
        //------------------------------------------------------------
        // Case 2.22: Upper inner reordered but not the lower inner
        //------------------------------------------------------------
        //   Unique keys: A(A1), B(B1), C(C1,C3), D(D1,D3)
        //
        //       IJ                                IJ
        //     / | \                             / | \
        //    D  IJ d1=1 & d3=b2   to           A  LJ a3=b3
        //     / | \                             / | \
        //    A  LJ a3=b3                      IJ  C  b1=c1
        //     / | \                         / | \
        //    B  C  b1=c1                   B  D  d1=1 & d3=b2
        //

        rs = st.executeQuery(
            "select a1,b1,c1,c3,d1,d3 "
            + "  from D join (A join (B left join C on b1=c1) on "
            + "a3=b3) on d3=b2 and d1=1"
            + " order by 6,5,4,3,2,1");

        expColNames = new String [] {"A1", "B1", "C1", "C3", "D1", "D3"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1", "3", "3", "1", "1", "2"},
            {"5", "3", "3", "1", "1", "2"},
            {"6", "3", "3", "1", "1", "2"},
            {"1", "3", "3", "4", "1", "2"},
            {"5", "3", "3", "4", "1", "2"},
            {"6", "3", "3", "4", "1", "2"},
            {"1", "3", "3", "7", "1", "2"},
            {"5", "3", "3", "7", "1", "2"},
            {"6", "3", "3", "7", "1", "2"},
            {"1", "7", null, null, "1", "3"},
            {"5", "7", null, null, "1", "3"},
            {"6", "7", null, null, "1", "3"},
            {"1", "9", null, null, "1", "3"},
            {"5", "9", null, null, "1", "3"},
            {"6", "9", null, null, "1", "3"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        //
        //------------------------------------------------------------
        // Case 2.23: Left branch has LJ and IJ
        //            On predicates are d1=a1 & d3=a3
        //            Reordered D to join with A first
        //------------------------------------------------------------
        //   Unique keys: A(A1), B(B1), C(C1,C3), D(D1,D3)
        //
        //         IJ                             LJ
        //        / | \                        /  | \
        //      LJ  D d1=a1 & d3=a3 to      IJ    IJ a1=b1
        //     / | \                      / | \ / | \
        //    A  IJ a1=b1                A  D | B C  b2=c2
        //     / | \                         d1=a1
        //    B  C  b2=c2                  & d3=a3
        //

        rs = st.executeQuery(
            "select a1,b1,c1,c3,d1,d3 "
            + "  from D join (A left join (B join C on b2=c2) on "
            + "a1=b1) on d3=a3 and d1=a1"
            + " order by 6,5,4,3,2,1");

        expColNames = new String [] {"A1", "B1", "C1", "C3", "D1", "D3"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"2", null, null, null, "2", "2"},
            {"1", "1", "1", "1", "1", "3"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        //
        //------------------------------------------------------------
        // Case 2.24: Left branch has LJ and IJ
        //            no inner join reordering because of d3=b3 where B
        //            is not the leftmost table
        //            Later, OJ2SEL ccnverts A LJ comp to A IJ comp
        //------------------------------------------------------------
        //   Unique keys: A(A1), B(B1), C(C1,C3), D(D1,D3)
        //
        //         IJ      <- JUnit conversion: Hmm, query below is modified to OJ here
        //        / | \
        //      LJ  D d1=a2 & d3=b3
        //     / | \
        //    A  IJ a1=b1  <- and here... why?
        //     / | \
        //    B  C  b2=c2
        //
        // select a1,b1,c1,c3,d1,d3
        //   from D join (A left join (B join C on b2=c2) on a1=b1) on d3=b3 and d1=a2
        //  order by 6,5,4,3,2,1

        rs = st.executeQuery(
            "select a1,b1,c1,c3,d1,d3 "
            + "  from D left join (A left join (B left join C on "
            + "b2=c2) on a1=b1) on d3=b3 and d1=a2"
            + " order by 6,5,4,3,2,1");

        expColNames = new String [] {"A1", "B1", "C1", "C3", "D1", "D3"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1", "1", "1", "1", "1", "2"},
            {"5", "5", null, null, "2", "2"},
            {"7", "7", "8", "9", "1", "3"},
            {null, null, null, null, "2", "5"},
            {null, null, null, null, "2", "6"},
            {null, null, null, null, "2", "9"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        //
        //------------------------------------------------------------
        // Case 2.25: Left branch has LJ and IJ
        //            Single inner join reordering
        //            Upper IJ reordered as e1 and c3 belongs to lower IJ
        //            Later, OJ2SEL converts C LJ E to C IJ E
        //------------------------------------------------------------
        //   Unique keys: A(A1), B(B1), C(C1,C3), D(D1,D3)
        //
        //              IJ                               LJ
        //            / | \                            / | \
        //          LJ  D d1=e1 & d3=c3   to         IJ  A  a1=b1
        //        / | \                            / | \
        //       IJ A a1=b1                      IJ  D d1=e1 & d3=c3
        //     / | \                           / | \
        //    B  LJ b2=c2                     B  LJ b2=c2
        //     / | \                           / | \
        //    C  E c1=e1                      C  E  c1=e1
        //

        rs = st.executeQuery(
            "select a1,b1,c1,c3,d1,d3,e1,e3"
            + "  from B join (C left join E on c1=e1) on b2=c2 "
            + "left join A on a1=b1 join D on d1=e1 and d3=c3"
            + " order by 8,7,6,5,4,3,2,1");

        expColNames = new String [] {"A1", "B1", "C1", "C3", "D1", "D3", "E1", "E3"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"3", "3", "2", "5", "2", "5", "2", "2"},
            {"6", "6", "1", "2", "1", "2", "1", "3"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        //
        //------------------------------------------------------------
        // Case 2.26: Left branch has LJ and IJ - not optimum reordering
        //            lower IJ reordered
        //            upper IJ not reordered as lower IJ null-producing
        //            side is E
        //            Had the logic is top-down, the upper IJ would
        //            reordered below upper LJ
        //            Also unfortunately, OJ2SEL failed to convert C LJ E
        //            to C IJ E
        //            The is a case where OJ demotion logic will help
        //            achieving the best solution.
        //------------------------------------------------------------
        //   Unique keys: A(A1), B(B1), C(C1,C3), D(D1,D3)
        //
        //              IJ                               IJ
        //            / | \                            / | \
        //          LJ  D d3=e3 & d1=c1   to         LJ  D  d3=e3 & d1=c1
        //        / | \                            / | \
        //       IJ A a1=b1                      LJ  A a1=b1
        //     / | \                           / | \
        //    B  LJ b1=c2                    IJ  E  c1=e1
        //     / | \                       / | \
        //    C  E c1=e1                  C  B  b1=c2
        //
        //   The best result should be:
        //
        //              LJ
        //            / | \
        //          IJ  A a1=b1
        //        / | \
        //       IJ D d3=e3 & d1=c1
        //     / | \
        //    B  IJ b1=c2
        //     / | \
        //    C  E c1=e1
        //

        rs = st.executeQuery(
            "select a1,b1,c1,c3,d1,d3,e1,e3"
            + "  from B join (C left join E on c1=e1) on b1=c2 "
            + "left join A on a1=b1 join D on d3=e3 and d1=c1"
            + " order by 8,7,6,5,4,3,2,1");

        expColNames = new String [] {"A1", "B1", "C1", "C3", "D1", "D3", "E1", "E3"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"4", "4", "1", "1", "1", "3", "1", "3"},
            {"7", "7", "1", "2", "1", "3", "1", "3"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        //
        //------------------------------------------------------------
        // Case 2.27: Balance tree
        //            No reordering
        //------------------------------------------------------------
        //   Unique keys: A(A1), B(B1), C(C1,C3), D(D1,D3)
        //
        //                IJ
        //              / | \
        //          LJ    LJ a1=b1
        //        / | \  / | \
        //       A  C | B  D b2=d1
        //           a2=c1
        //

        rs = st.executeQuery(
            "select a1,b1,c1,c3,d1,d3"
            + "  from (A left join C on a2=c1) join (B left join D "
            + " on b2=d1) on a1=b1"
            + " order by 6,5,4,3,2,1");

        expColNames = new String [] {"A1", "B1", "C1", "C3", "D1", "D3"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"3", "3", null, null, "2", "2"},
            {"3", "3", null, null, "2", "5"},
            {"3", "3", null, null, "2", "6"},
            {"3", "3", null, null, "2", "9"},
            {"1", "1", "1", "1", null, null},
            {"7", "7", "1", "1", null, null},
            {"1", "1", "1", "2", null, null},
            {"7", "7", "1", "2", null, null},
            {"5", "5", "2", "5", null, null},
            {"4", "4", null, null, null, null},
            {"6", "6", null, null, null, null}
        };

        JDBC.assertFullResultSet(rs, expRS);

        //
        //------------------------------------------------------------
        // Case 2.28: Right Deep tree - No reordering for now
        //            Reordering will take place when OJ demotion is
        //            available
        //------------------------------------------------------------
        //   Unique keys: A(A1), B(B1), C(C1,C3), D(D1,D3)
        //
        //                          1st linearization        2nd Linearization
        //
        //       LJ                     LJ                            LJ
        //     / | \                  / | \                         / | \
        //    A  LJ a1=c2     to     A  LJ a1=c2     to           LJ  D  c1=d1
        //     / | \                  / | \                     / | \
        //    B  LJ b1=c2           LJ  D  c1=d1               A  LJ a1=c2
        //     / | \               / | \                        / | \
        //    C  D  c1=d1         B  C  b1=c2                  B  C  b1=c2
        //
        //   OJ2SEL
        //
        //         LJ
        //       / | \
        //     LJ  D  c1=d1
        //   / | \
        //  A  IJ a1=c2
        //   / | \
        //  B  C  b1=c2
        //

        rs = st.executeQuery(
            "select a1,b1,c1,c3,d1,d3 "
            + "  from a left join (b left join (c left join d on "
            + "c1=d1) on b1=c2) on a1=c2"
            + " order by 6,5,4,3,2,1");

        expColNames = new String [] {"A1", "B1", "C1", "C3", "D1", "D3"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"4", "4", "1", "1", "1", "2"},
            {"7", "7", "1", "2", "1", "2"},
            {"4", "4", "1", "1", "1", "3"},
            {"7", "7", "1", "2", "1", "3"},
            {"7", "7", "3", "7", null, null},
            {"3", "3", "8", "9", null, null},
            {"1", null, null, null, null, null},
            {"2", null, null, null, null, null},
            {"5", null, null, null, null, null},
            {"6", null, null, null, null, null},
            {"8", null, null, null, null, null}
        };

        JDBC.assertFullResultSet(rs, expRS);

        //
        //------------------------------------------------------------
        // Case 2.29: Balance tree - one branch has IJ only
        //            No reordering
        //------------------------------------------------------------
        //   Unique keys: A(A1), B(B1), C(C1,C3), D(D1,D3)
        //
        //                IJ
        //              / | \
        //          IJ    LJ a1=b1
        //        / | \  / | \
        //       A  C | B  D b2=d1
        //           a2=c1
        //

        rs = st.executeQuery(
            "select a1,b1,c1,c3,d1,d3"
            + "  from (A join C on a2=c1) join (B left join D on "
            + "b2=d1) on a1=b1"
            + " order by 6,5,4,3,2,1");

        expColNames = new String [] {"A1", "B1", "C1", "C3", "D1", "D3"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1", "1", "1", "1", null, null},
            {"7", "7", "1", "1", null, null},
            {"1", "1", "1", "2", null, null},
            {"7", "7", "1", "2", null, null},
            {"5", "5", "2", "5", null, null}
        };

        JDBC.assertFullResultSet(rs, expRS);

        //
        //------------------------------------------------------------
        // Case 2.30: 2 IJs
        //            Lower IJ not reordered because condition not satisfied
        //            Upper IJ reordered in 2 steps
        //            1st step reordered to above lower IJ
        //            2nd step reordered pass above lower IJ
        //------------------------------------------------------------
        //   Unique keys: A(A1), B(B1), C(C1,C3), D(D1,D3)
        //
        //                IJ1                            LJ1
        //              / | \                          / | \
        //             A  LJ1 c1=a1                  IJ1 E  d1=e1
        //              / | \         1st step     / | \
        //            IJ2 E  d1=e1    ========>  IJ2 A  c1=a1
        //          / | \                      / | \
        //         B  LJ2 b2=c1               B  LJ2 b2=c1
        //          / | \                      / | \
        //         C  D  c1=d1                C  D  c1=d1
        //
        //  2nd step:
        //                LJ1
        //              / | \
        //            IJ2 E  d1=e1
        //          / | \
        //         B  LJ2 b2=c1
        //          / | \
        //        IJ1 D  c1=d1
        //      / | \
        //     C  A  c1=a1
        //

        rs = st.executeQuery(
            "select a1,b1,c1,c3,d1,d3,e1"
            + "  from a join ((b join (c left join d on c1=d1) on "
            + "b2=c1) left join e on d1=e1) on c1=a1"
            + " order by 7,6,5,4,3,2,1");

        expColNames = new String [] {"A1", "B1", "C1", "C3", "D1", "D3", "E1"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"2", "3", "2", "5", "2", "2", "2"},
            {"2", "3", "2", "5", "2", "5", "2"},
            {"2", "3", "2", "5", "2", "6", "2"},
            {"2", "3", "2", "5", "2", "9", "2"},
            {"3", "7", "3", "1", null, null, null},
            {"3", "9", "3", "1", null, null, null},
            {"3", "7", "3", "4", null, null, null},
            {"3", "9", "3", "4", null, null, null},
            {"3", "7", "3", "7", null, null, null},
            {"3", "9", "3", "7", null, null, null}
        };

        JDBC.assertFullResultSet(rs, expRS);

        //
        //------------------------------------------------------------
        // Case 2.31: 2 IJs - same as 2,30 except LJ2 is left of IJ2
        //            Lower IJ not reordered because condition not satisfied
        //            Upper IJ reordered in 2 steps
        //            1st step reordered to above lower IJ
        //            2nd step reordered pass above lower IJ
        //------------------------------------------------------------
        //   Unique keys: A(A1), B(B1), C(C1,C3), D(D1,D3)
        //
        //                IJ1                            LJ1
        //              / | \                          / | \
        //             A  LJ1 c1=a1                  IJ1 E  d1=e1
        //              / | \         1st step     / | \
        //            IJ2 E  d1=e1    ========>  IJ2 A  c1=a1
        //          / | \                      / | \
        //       LJ2  B b2=c1               LJ2  B  b2=c1
        //      / | \                      / | \
        //     C  D  c1=d1                C  D  c1=d1
        //
        //  2nd step:
        //                LJ1
        //              / | \
        //            IJ2 E  d1=e1
        //          / | \
        //       LJ2  B  b2=c1
        //      / | \
        //   IJ1  D  c1=d1
        //  / | \
        // C  A  c1=a1
        //

        rs = st.executeQuery(
            "select a1,b1,c1,c3,d1,d3,e1"
            + "  from a join (((c left join d on c1=d1) join b on "
            + "b2=c1) left join e on d1=e1) on c1=a1"
            + " order by 7,6,5,4,3,2,1");

        expColNames = new String [] {"A1", "B1", "C1", "C3", "D1", "D3", "E1"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"2", "3", "2", "5", "2", "2", "2"},
            {"2", "3", "2", "5", "2", "5", "2"},
            {"2", "3", "2", "5", "2", "6", "2"},
            {"2", "3", "2", "5", "2", "9", "2"},
            {"3", "7", "3", "1", null, null, null},
            {"3", "9", "3", "1", null, null, null},
            {"3", "7", "3", "4", null, null, null},
            {"3", "9", "3", "4", null, null, null},
            {"3", "7", "3", "7", null, null, null},
            {"3", "9", "3", "7", null, null, null}
        };

        JDBC.assertFullResultSet(rs, expRS);

        //
        //------------------------------------------------------------
        // Case 2.32: 3 IJs in a row - Only the top IJ reordered in 1 step
        //------------------------------------------------------------
        //   Unique keys: A(A1), B(B1), C(C1,C3), D(D1,D3)
        //
        //               IJ1                     IJ2
        //              / | \                   / | \
        //             A IJ2 a1=d1             B IJ3 b1=c1
        //              / | \                   / | \
        //             B IJ3 b1=c1    ==>      C LJ1 c1=d1
        //              / | \                   / | \
        //             C LJ1 c1=d1           IJ1  E  d1=e1
        //              / | \               / | \
        //             D  E  d1=e1         D  A  a1=d1
        //

        rs = st.executeQuery(
            "select a1,b1,c1,c3,d1,d3,e1"
            + "  from a join (b join (c join (d left join e on "
            + "d1=e1) on c1=d1) on b1=c1) on a1=d1"
            + " order by 7,6,5,4,3,2,1");

        expColNames = new String [] {"A1", "B1", "C1", "C3", "D1", "D3", "E1"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1", "1", "1", "1", "1", "2", "1"},
            {"1", "1", "1", "2", "1", "2", "1"},
            {"1", "1", "1", "1", "1", "3", "1"},
            {"1", "1", "1", "2", "1", "3", "1"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        //
        //------------------------------------------------------------
        // Case 2.33: 3 IJs in a row - upper 2 IJs each reordered in 1 step
        //------------------------------------------------------------
        //   Unique keys: A(A1), B(B1), C(C1,C3), D(D1,D3)
        //
        //               IJ1                            IJ3
        //              / | \                          / | \
        //             A IJ2 a1=d1                    C LJ1 c1=d1
        //              / | \                          / | \
        //             B IJ3 b1=d1    ==>           IJ1  E  d1=e1
        //              / | \                      / | \
        //             C LJ1 c1=d1              IJ2  A  a1=d1
        //              / | \                  / | \
        //             D  E  d1=e1            D  B  b1=d1
        //

        rs = st.executeQuery(
            "select a1,b1,c1,c3,d1,d3,e1"
            + "  from a join (b join (c join (d left join e on "
            + "d1=e1) on c1=d1) on b1=d1) on a1=d1"
            + " order by 7,6,5,4,3,2,1");

        expColNames = new String [] {"A1", "B1", "C1", "C3", "D1", "D3", "E1"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1", "1", "1", "1", "1", "2", "1"},
            {"1", "1", "1", "2", "1", "2", "1"},
            {"1", "1", "1", "1", "1", "3", "1"},
            {"1", "1", "1", "2", "1", "3", "1"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        //
        //------------------------------------------------------------
        // Case 2.34: LJs under IJ
        //            Reorder allowed above LJ
        //            Later OJ2SEL change LJ1 to IJ3 and LJ2 to LJ1
        //------------------------------------------------------------
        //   Unique keys: A(A1), B(B1), C(C1,C3), D(D1,D3)
        //
        //               IJ1                            LJ1
        //              / | \                          / | \
        //             A LJ1 a1=b1                  IJ1  F  e1=f1
        //              / | \                      / | \
        //           IJ2  F  e1=f1    ==>       IJ2  A  a1=b1
        //         / |  \                    /  |  \
        //     LJ1   LJ2 c1=d1           LJ1   LJ2 c1=d1
        //    / | \ / | \               / | \ / | \
        //   B  C | D E  d1=e1         B  C | D E  d1=e1
        //      b1=c1                     b1=c1
        //
        // After OJ2SEL
        //                     LJ1
        //                    / | \
        //                 IJ1  F  e1=f1
        //                / | \
        //             IJ2  A  a1=b1
        //          /  |  \
        //       IJ3   LJ2 c1=d1
        //      / | \ / | \
        //     B  C | D E  d1=e1
        //        b1=c1
        //
        //
        //  A --- (((B <-> C) --- (D <-> E)) <-- F)      A --- B --- C --- D <-- E <-- F
        //  =======================================      ===============================
        //  Y        Y     Y       Y     Y       Y       Y     Y     Y     Y     Y     Y
        //  Y        Y     Y       Y     Y       N   ==> Y     Y     Y     Y     Y     N
        //  Y        Y     Y       Y     N       N       Y     Y     Y     Y     N     N
        //           N     Y       Y     Y       Y
        //           N     Y       Y     Y       N
        //           N     Y       Y     N       N
        //           Y     N
        //                         N     Y
        //
        // Noted at JUnit conversion time: original query has been modified. Why?
        //
        // select a1,b1,c1,c3,d1,d3,e1,f4
        //   from a join (((b left join c on b1=c1) join (d left join e on d1=e1) on c1=d1) left join f on e1=f1) on a1=b1
        // order by 8,7,6,5,4,3,2,1

        rs = st.executeQuery(
            "select a1,b1,c1,c3,d1,d3,e1,f4"
            + "  from a left join (((b left join c on b1=c1) left "
            + "join (d left join e on d1=e1) on c1=d1) left join f "
            + " on e1=f1) on a1=b1"
            + " order by 8,7,6,5,4,3,2,1");

        expColNames = new String [] {"A1", "B1", "C1", "C3", "D1", "D3", "E1", "F4"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1", "1", "1", "1", "1", "2", "1", null},
            {"1", "1", "1", "2", "1", "2", "1", null},
            {"1", "1", "1", "1", "1", "3", "1", null},
            {"1", "1", "1", "2", "1", "3", "1", null},
            {"3", "3", "3", "1", null, null, null, null},
            {"3", "3", "3", "4", null, null, null, null},
            {"3", "3", "3", "7", null, null, null, null},
            {"4", "4", null, null, null, null, null, null},
            {"5", "5", null, null, null, null, null, null},
            {"6", "6", null, null, null, null, null, null},
            {"7", "7", null, null, null, null, null, null},
            {"2", null, null, null, null, null, null, null},
            {"8", null, null, null, null, null, null, null}
        };

        JDBC.assertFullResultSet(rs, expRS);

        //
        //------------------------------------------------------------
        // Case 2.35: LJs under IJ
        //            Reordered
        //------------------------------------------------------------
        //   Unique keys: A(A1), B(B1), C(C1,C3), D(D1,D3)
        //
        //              IJ1                             LJ1
        //             / | \                          /  |  \
        //           LJ1 C a1=c1 and c3=1         IJ1    LJ2 b1=d1
        //         /  |  \                       / | \  / | \
        //      LJ1   LJ2  b1=d1     ==>       LJ1 C |  D E d1=e1
        //     / | \ / | \                    / | \ a1=c1 and c3=1
        //    A  B | D E  d1=e1              A  B |
        //       a1=b1                          a1=b1
        //

        rs = st.executeQuery(
            "select a1,b1,c1,c3,d1,d3,e1"
            + "  from ((A left join B on a1=b1) left join (D left "
            + "join E on d1=e1) on b1=d1) join C on a1=c1 and c3=1"
            + " order by 7,6,5,4,3,2,1");

        expColNames = new String [] {"A1", "B1", "C1", "C3", "D1", "D3", "E1"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1", "1", "1", "1", "1", "2", "1"},
            {"1", "1", "1", "1", "1", "3", "1"},
            {"3", "3", "3", "1", null, null, null}
        };

        JDBC.assertFullResultSet(rs, expRS);

        //
        //------------------------------------------------------------
        // Case 2.36: No reordering because of LJ
        //------------------------------------------------------------
        //
        //               IJ1
        //              / | \
        //             A LJ1 a1=b1
        //              / | \
        //             B  C  b1=c1
        //

        rs = st.executeQuery(
            "select a1,b1,c1,c3"
            + "  from a join (b left join c on b1=c1) on a1=b1"
            + " order by 4,3,2,1");

        expColNames = new String [] {"A1", "B1", "C1", "C3"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1", "1", "1", "1"},
            {"3", "3", "3", "1"},
            {"1", "1", "1", "2"},
            {"3", "3", "3", "4"},
            {"3", "3", "3", "7"},
            {"4", "4", null, null},
            {"5", "5", null, null},
            {"6", "6", null, null},
            {"7", "7", null, null}
        };

        JDBC.assertFullResultSet(rs, expRS);

        //
        //------------------------------------------------------------
        // Case 2.37: LJ under IJ - no reordering
        //------------------------------------------------------------
        //
        //              IJ1
        //             / | \
        //           A  LJ1 a1=b1
        //             / | \
        //           LJ1 D c1=d1
        //          / | \
        //         B  C b1=c1
        //

        rs = st.executeQuery(
            "select a1,b1,c1,c3,d1,d3"
            + "  from a join ((b left join c on b1=c1) left join d "
            + " on c1=d1) on a1=b1"
            + " order by 6,5,4,3,2,1");

        expColNames = new String [] {"A1", "B1", "C1", "C3", "D1", "D3"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1", "1", "1", "1", "1", "2"},
            {"1", "1", "1", "2", "1", "2"},
            {"1", "1", "1", "1", "1", "3"},
            {"1", "1", "1", "2", "1", "3"},
            {"3", "3", "3", "1", null, null},
            {"3", "3", "3", "4", null, null},
            {"3", "3", "3", "7", null, null},
            {"4", "4", null, null, null, null},
            {"5", "5", null, null, null, null},
            {"6", "6", null, null, null, null},
            {"7", "7", null, null, null, null}
        };

        JDBC.assertFullResultSet(rs, expRS);

        //
        //------------------------------------------------------------
        // Case 2.38: LJ over IJ
        //            Reordered OK
        //------------------------------------------------------------
        //
        //              LJ1                  LJ1
        //             / | \                / | \
        //           IJ1 D  c1=d1         LJ1 D  c1=d1
        //          / | \          ===>  / | \
        //        LJ1 C a1=c1 & c3=0   IJ1 B a1=b1
        //       / | \                / | \
        //      A  B  a1=b1          A  C a1=c1 & c3=0
        //

        rs = st.executeQuery(
            "select a1,b1,c1,c3,d1,d3"
            + "  from a left join b on a1=b1 join C on a1=c1 and "
            + "c3=0 left join d on c1=d1"
            + " order by 6,5,4,3,2,1");

        expColNames = new String [] {"A1", "B1", "C1", "C3", "D1", "D3"};
        JDBC.assertColumnNames(rs, expColNames);
        JDBC.assertDrainResults(rs, 0);

        //
        //------------------------------------------------------------
        // Case 2.39: Future extension
        //            IJ1 cannot be reordered.  However, if linearization
        //            first, then IJ1 can be reordered
        //------------------------------------------------------------
        //
        //            IJ1                     LJ2
        //           / | \       should      / | \
        //          A LJ1 a1=c1  be       LJ1  D  c1=d1
        //           / | \       ===>    / | \
        //          B LJ2 b1=c1       IJ1  B  a1=b1
        //           / | \           / | \
        //          C  D  c1=d1     A  C  a1=c1
        //

        rs = st.executeQuery(
            "select a1,b1,c1,c3,d1,d3"
            + "  from A join (B left join (C left join D on c1=d1) "
            + " on b1=c1) on a1=c1"
            + " order by 6,5,4,3,2,1");

        expColNames = new String [] {"A1", "B1", "C1", "C3", "D1", "D3"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1", "1", "1", "1", "1", "2"},
            {"1", "1", "1", "2", "1", "2"},
            {"1", "1", "1", "1", "1", "3"},
            {"1", "1", "1", "2", "1", "3"},
            {"3", "3", "3", "1", null, null},
            {"3", "3", "3", "4", null, null},
            {"3", "3", "3", "7", null, null}
        };

        JDBC.assertFullResultSet(rs, expRS);

        //
        //------------------------------------------------------------
        // Case 2.40: Reordered with skip IJ
        //------------------------------------------------------------
        //
        //              IJ1                        IJ2
        //             / | \                     /  |  \
        //            A IJ2 a1=c1            IJ3    IJ4 b1=e1
        //            /  |  \       ===>    / | \  /  | \
        //        IJ3    IJ4 b1=e1         B LJ1| E   LJ2 e2=f1
        //       / | \  /  | \              / |\b2=c1 / | \
        //      B LJ1|  E  LJ2 e2=f1      IJ1 D |    F  G f1=g1
        //       / |\b2=c1 / | \         / | \ c1=d1
        //      C  D |    F  G  f1=g1   C  A a1=c1
        //         c1=d1
        //
        // Noted at JUnit conversion time: original query has been modified. Why?
        //
        //
        // select a1,b1,c1,c3,d1,d3,e1,f4,g1,g3
        //  from A join ((B join (C left join D on c1=d1) on b2=c1) join
        //               (E join (F left join G on f1=g1) on e2=f1) on b1=e1) on a1=c1
        // order by 10,9,8,7,6,5,4,3,2,1

        rs = st.executeQuery(
            "select a1,b1,c1,c3,d1,d3,e1,f4,g1,g3"
            + "  from A left join ((B left join (C left join D on "
            + "c1=d1) on b2=c1) left join"
            + "               (E left join (F left join G on "
            + "f1=g1) on e2=f1) on b1=e1) on a1=c1"
            + " order by 10,9,8,7,6,5,4,3,2,1");

        expColNames = new String [] {"A1", "B1", "C1", "C3", "D1", "D3", "E1", "F4", "G1", "G3"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"3", "7", "3", "1", null, null, "7", null, "1", "1"},
            {"3", "7", "3", "4", null, null, "7", null, "1", "1"},
            {"3", "7", "3", "7", null, null, "7", null, "1", "1"},
            {"3", "7", "3", "1", null, null, "7", null, "1", "2"},
            {"3", "7", "3", "4", null, null, "7", null, "1", "2"},
            {"3", "7", "3", "7", null, null, "7", null, "1", "2"},
            {"2", "3", "2", "5", "2", "2", "3", "6", null, null},
            {"2", "3", "2", "5", "2", "5", "3", "6", null, null},
            {"2", "3", "2", "5", "2", "6", "3", "6", null, null},
            {"2", "3", "2", "5", "2", "9", "3", "6", null, null},
            {"3", "9", "3", "1", null, null, null, null, null, null},
            {"3", "9", "3", "4", null, null, null, null, null, null},
            {"3", "9", "3", "7", null, null, null, null, null, null},
            {"1", null, null, null, null, null, null, null, null, null},
            {"4", null, null, null, null, null, null, null, null, null},
            {"5", null, null, null, null, null, null, null, null, null},
            {"6", null, null, null, null, null, null, null, null, null},
            {"7", null, null, null, null, null, null, null, null, null},
            {"8", null, null, null, null, null, null, null, null, null}
        };

        JDBC.assertFullResultSet(rs, expRS);

        //
        //------------------------------------------------------------
        // Case 2.41: Opportunity missed
        //------------------------------------------------------------
        //
        //              IJ1
        //             / | \
        //            A IJ2 a1=f1
        //            /  |  \       IJ1 should be reordered to below LJ2
        //        IJ3    IJ4 b1=e1
        //       / | \  /  | \
        //      B LJ1|  E  LJ2 e2=f1
        //       / |\b2=c1 / | \
        //      C  D |    F  G  f1=g1
        //         c1=d1
        //
        // select a1,b1,c1,c3,d1,d3,e1,f4,g1,g3
        //  from A join ((B join (C left join D on c1=d1) on b2=c1) join
        //               (E join (F left join G on f1=g1) on e2=f1) on b1=e1) on a1=f1
        // order by 10,9,8,7,6,5,4,3,2,1

        st.executeUpdate(
            "drop table a");

        st.executeUpdate(
            " drop table b");

        st.executeUpdate(
            " drop table c");

        st.executeUpdate(
            " drop table d");

        st.executeUpdate(
            " drop table e");

        st.executeUpdate(
            " drop table f");

        st.executeUpdate(
            " drop table g");

        st.executeUpdate(
            " drop table h");

        // ojel001.clp

        st.executeUpdate(
            "create table a (c1 int, c2 int, c3 int)");

        st.executeUpdate(
            " create table b (c1 int, c2 int, c3 int)");

        st.executeUpdate(
            " create table c (c1 int, c2 int, c3 int)");

        st.executeUpdate(
            " create table d (c1 int, c2 int, c3 int)");

        st.executeUpdate(
            " create table e (c1 int, c2 int, c3 int)");

        st.executeUpdate(
            " create table f (c1 int, c2 int, c3 int)");

        st.executeUpdate(
            " create table g (c1 int, c2 int, c3 int)");

        st.executeUpdate(
            " create table h (c1 int, c2 int, c3 int)");

        st.executeUpdate(
            " create table i (c1 int, c2 int, c3 int)");

        st.executeUpdate(
            " create table j (c1 int, c2 int, c3 int)");

        st.executeUpdate(
            " create table aa (c1 int, c2 int, c3 int)");

        st.executeUpdate(
            " create table bb (c1 int, c2 int, c3 int)");

        st.executeUpdate(
            " create table cc (c1 int, c2 int, c3 int)");

        st.executeUpdate(
            " create table dd (c1 int, c2 int, c3 int)");

        st.executeUpdate(
            " create table ee (c1 int, c2 int, c3 int)");

        st.executeUpdate(
            " create table ff (c1 int, c2 int, c3 int)");

        st.executeUpdate(
            " create table gg (c1 int, c2 int, c3 int)");

        st.executeUpdate(
            " create table hh (c1 int, c2 int, c3 int)");

        st.executeUpdate(
            " create table ii (c1 int, c2 int, c3 int)");

        st.executeUpdate(
            " create table jj (c1 int, c2 int, c3 int)");

        st.executeUpdate(
            " create table kk (c1 int, c2 int, c3 int)");

        st.executeUpdate(
            " create table t1 (c1 int, c2 int, c3 int)");

        st.executeUpdate(
            " create table t2 (c1 int, c2 int, c3 int)");

        st.executeUpdate(
            " create view v1 (c1, c2, c3) as (select c1, c2, c3 from t1)");

        st.executeUpdate(
            " create view v2 (c1, c2, c3) as (select c1, c2, c3"
            + "                                from t2"
            + "                                group by c1, c2, c3)");

        st.executeUpdate(
            " create unique index aa_idx1 on aa (c1)");

        st.executeUpdate(
            " create unique index bb_idx1 on bb (c1)");

        st.executeUpdate(
            " create unique index cc_idx1 on cc (c2)");

        st.executeUpdate(
            " create unique index dd_idx1 on dd (c3)");

        st.executeUpdate(
            " create unique index ee_idx1 on ee (c1)");

        st.executeUpdate(
            " create unique index ff_idx1 on ff (c2)");

        st.executeUpdate(
            " create unique index gg_idx1 on gg (c3)");

        st.executeUpdate(
            " create unique index jj_idx1 on jj (c1, c2)");

        st.executeUpdate(
            " create unique index kk_idx1 on kk (c1, c2, c3)");

        st.executeUpdate(
            " insert into a values (1, 1, 1), (1, 2, 3), (2, 3, "
            + "4), (5, 5, 5)");

        st.executeUpdate(
            " insert into b values (1, 2, 3), (2, 2, 2), (2, 3, "
            + "4), (5, 5, 5)");

        st.executeUpdate(
            " insert into c values (1, 1, 1), (1, 2, 3), (2, 2, "
            + "2), (3, 3, 3)");

        st.executeUpdate(
            " insert into d values (1, 1, 1), (1, 2, 3), (4, 4, 4)");

        st.executeUpdate(
            " insert into e values (1, 2, 3), (2, 2, 2), (2, 3, "
            + "4), (5, 5, 5)");

        st.executeUpdate(
            " insert into f values (1, 1, 1), (2, 2, 2), (2, 3, "
            + "4), (5, 5, 5)");

        st.executeUpdate(
            " insert into g values (1, 1, 1), (1, 2, 3), (2, 3, "
            + "4), (5, 5, 5)");

        st.executeUpdate(
            " insert into h values (1, 2, 3), (2, 2, 2), (2, 3, "
            + "4), (5, 5, 5)");

        st.executeUpdate(
            " insert into i values (1, 2, 3), (2, 2, 2), (2, 3, "
            + "4), (5, 5, 5)");

        st.executeUpdate(
            " insert into j values (1, 1, 1), (1, 2, 3), (2, 3, "
            + "4), (3, 4, 5)");

        st.executeUpdate(
            " insert into aa values (1, 1, 1), (2, 3, 4), (5, 5, 5)");

        st.executeUpdate(
            " insert into bb values (1, 2, 3), (2, 2, 2), (3, 3, "
            + "4), (5, 5, 5)");

        st.executeUpdate(
            " insert into cc values (1, 1, 1), (1, 2, 3), (2, 3, "
            + "2), (3, 5, 3)");

        st.executeUpdate(
            " insert into dd values (1, 1, 1), (1, 2, 3), (4, 4, 4)");

        st.executeUpdate(
            " insert into ee values (1, 2, 3), (2, 2, 2), (4, 3, "
            + "4), (5, 5, 5)");

        st.executeUpdate(
            " insert into ff values (1, 1, 1), (2, 2, 2), (2, 3, "
            + "4), (5, 5, 5)");

        st.executeUpdate(
            " insert into gg values (1, 1, 1), (1, 2, 3), (2, 3, "
            + "4), (5, 5, 5)");

        st.executeUpdate(
            " insert into hh values (1, 1, 1), (1, 2, 3), (2, 3, "
            + "4), (5, 5, 5)");

        st.executeUpdate(
            " insert into ii values (1, 2, 3), (2, 2, 2), (2, 3, "
            + "4), (5, 5, 5)");

        st.executeUpdate(
            " insert into jj values (1, 1, 1), (1, 2, 3), (2, 2, "
            + "2), (3, 3, 3)");

        st.executeUpdate(
            " insert into kk values (1, 1, 1), (1, 2, 3), (4, 4, 4)");

        st.executeUpdate(
            " insert into t1 values (1, 1, 1), (1, 2, 3), (2, 2, "
            + "2), (3, 3, 3)");

        st.executeUpdate(
            " insert into t2 values (1, 1, 1), (1, 2, 3), (4, 4, 4)");

        //
        //                         LJ
        //                        /  \
        //                       /    \
        //                      A      B
        //
        //------------------------------------------------------------

        rs = st.executeQuery(
            "select distinct a.* from a left outer join b on a.c1 = b.c1");

        expColNames = new String [] {"C1", "C2", "C3"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1", "1", "1"},
            {"1", "2", "3"},
            {"2", "3", "4"},
            {"5", "5", "5"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        //
        //------------------------------------------------------------
        // Case 1.02: A simple case with only one right join
        //------------------------------------------------------------
        //
        //                         RJ
        //                        /  \
        //                       /    \
        //                      A      B
        //
        //------------------------------------------------------------

        rs = st.executeQuery(
            "select distinct b.* "
            + " from a right outer join b on a.c1 = b.c1");

        expColNames = new String [] {"C1", "C2", "C3"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1", "2", "3"},
            {"2", "2", "2"},
            {"2", "3", "4"},
            {"5", "5", "5"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        //
        //------------------------------------------------------------
        // Case 1.03.1: A complex case with left joins and inner joins
        //------------------------------------------------------------
        //
        //                         LJ
        //                        /  \
        //                       IJ   G
        //                      /  \
        //                     LJ   F
        //                    /  \
        //                   LJ   E
        //                  /  \
        //                 IJ   D
        //                /  \
        //               LJ   C
        //              /  \
        //             A    B
        //
        //------------------------------------------------------------

        rs = st.executeQuery(
            "select distinct a.* "
            + " from a left outer join b on a.c1 = b.c1 "
            + "       inner join c on a.c2 = c.c2 "
            + "       left outer join d on a.c3 = d.c3"
            + "       left outer join e on a.c1 = e.c1"
            + "       inner join f on a.c2 = f.c2"
            + "       left outer join g on a.c3 = g.c3");

        expColNames = new String [] {"C1", "C2", "C3"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1", "1", "1"},
            {"1", "2", "3"},
            {"2", "3", "4"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        //
        //------------------------------------------------------------
        // Case 1.03.2: A complex case with left joins and inner joins
        //------------------------------------------------------------
        //
        //                         LJ
        //                        /  \
        //                       IJ   G
        //                      /  \
        //                     LJ   F
        //                    /  \
        //                   LJ   E
        //                  /  \
        //                 IJ   D
        //                /  \
        //               LJ   C
        //              /  \
        //             A    B
        //
        //------------------------------------------------------------

        rs = st.executeQuery(
            "select distinct e.* "
            + " from a left outer join b on a.c1 = b.c1 "
            + "       inner join c on a.c2 = c.c2 "
            + "       left outer join d on a.c3 = d.c3"
            + "       left outer join e on a.c1 = e.c1"
            + "       inner join f on a.c2 = f.c2"
            + "       left outer join g on a.c3 = g.c3");

        expColNames = new String [] {"C1", "C2", "C3"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1", "2", "3"},
            {"2", "2", "2"},
            {"2", "3", "4"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        //
        //------------------------------------------------------------
        // Case 1.04.1: A complex case with right joins and inner joins
        //------------------------------------------------------------
        //
        //                         RJ
        //                        /  \
        //                       G   RJ
        //                          /  \
        //                         F   RJ
        //                            /  \
        //                           E   IJ
        //                              /  \
        //                             D   IJ
        //                                /  \
        //                               C   RJ
        //                                  /  \
        //                                 B    A
        //
        //------------------------------------------------------------

        rs = st.executeQuery(
            "select distinct a.* "
            + " from g right outer join "
            + "     (f right outer join "
            + "      (e right outer join "
            + "       (d inner join "
            + "        (c inner join "
            + "         (b right outer join a on b.c3 = a.c3) "
            + "                                on c.c2 = a.c2) "
            + "                                 on d.c1 = a.c1) "
            + "                                  on e.c3 = a.c3) "
            + "                                   on f.c2 = a.c2) "
            + "                                    on g.c1 = a.c1");

        expColNames = new String [] {"C1", "C2", "C3"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1", "1", "1"},
            {"1", "2", "3"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        //
        //------------------------------------------------------------
        // Case 1.04.2: A complex case with right joins and inner joins
        //------------------------------------------------------------
        //
        //                         RJ
        //                        /  \
        //                       G   RJ
        //                          /  \
        //                         F   RJ
        //                            /  \
        //                           E   IJ
        //                              /  \
        //                             D   IJ
        //                                /  \
        //                               C   RJ
        //                                  /  \
        //                                 B    A
        //
        //------------------------------------------------------------

        rs = st.executeQuery(
            "select distinct f.* "
            + " from g right outer join "
            + "     (f right outer join "
            + "      (e right outer join "
            + "       (d inner join "
            + "        (c inner join "
            + "         (b right outer join a on b.c3 = a.c3) "
            + "                                on c.c2 = a.c2) "
            + "                                 on d.c1 = a.c1) "
            + "                                  on e.c3 = a.c3) "
            + "                                   on f.c2 = a.c2) "
            + "                                    on g.c1 = a.c1");

        expColNames = new String [] {"C1", "C2", "C3"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1", "1", "1"},
            {"2", "2", "2"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        //
        //------------------------------------------------------------
        // Case 1.05.1: A complex case with right joins, left joins and inner joins
        //------------------------------------------------------------
        //
        //                           LJ
        //                         /    \
        //                       LF      RJ
        //                      /  \    /  \
        //                     IJ   C  D   LJ
        //                    /  \        /  \
        //                   A    B      E    F
        //
        //------------------------------------------------------------

        rs = st.executeQuery(
            "select distinct a.* "
            + " from a inner join b on a.c1 = b.c1"
            + "       left outer join c on a.c2 = c.c2 "
            + "       left outer join (d right outer join "
            + "                        (e left outer join f on "
            + "e.c1 = f.c1) "
            + "                                              on "
            + "d.c3 = e.c3) "
            + "                                               on "
            + "a.c3 = e.c3");

        expColNames = new String [] {"C1", "C2", "C3"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1", "1", "1"},
            {"1", "2", "3"},
            {"2", "3", "4"},
            {"5", "5", "5"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        //
        //------------------------------------------------------------
        // Case 1.05.2: A complex case with right joins, left joins and inner joins
        //------------------------------------------------------------
        //
        //                           LJ
        //                         /    \
        //                       LF      RJ
        //                      /  \    /  \
        //                     IJ   C  D   LJ
        //                    /  \        /  \
        //                   A    B      E    F
        //
        //------------------------------------------------------------

        rs = st.executeQuery(
            "select distinct e.* "
            + " from a inner join b on a.c1 = b.c1"
            + "       left outer join c on a.c2 = c.c2 "
            + "       left outer join (d right outer join "
            + "                        (e left outer join f on "
            + "e.c1 = f.c1) "
            + "                                              on "
            + "d.c3 = e.c3) "
            + "                                               on "
            + "a.c3 = e.c3");

        expColNames = new String [] {"C1", "C2", "C3"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1", "2", "3"},
            {"2", "3", "4"},
            {"5", "5", "5"},
            {null, null, null}
        };

        JDBC.assertFullResultSet(rs, expRS);

        //
        //------------------------------------------------------------
        // Case 1.06.1: A complex case with right joins, left joins and inner joins
        //------------------------------------------------------------
        //
        //                           LJ
        //                         /    \
        //                       LF      RJ
        //                      /  \    /  \
        //                     IJ   C  D   LJ
        //                    /  \        /  \
        //                   A    B      RJ   G
        //                              /  \
        //                             E    F
        //
        //------------------------------------------------------------

        rs = st.executeQuery(
            "select distinct a.*"
            + " from a inner join b on a.c1 = b.c1"
            + "       left outer join c on a.c2 = c.c2 "
            + "       left outer join (d right outer join "
            + "                        ((e right outer join f on "
            + "e.c1 = f.c1) left outer join g on f.c1 = g.c1) "
            + "                                                    "
            + "                              on d.c3 = f.c3) "
            + "                                                    "
            + "                               on a.c3 = f.c3");

        expColNames = new String [] {"C1", "C2", "C3"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1", "1", "1"},
            {"1", "2", "3"},
            {"2", "3", "4"},
            {"5", "5", "5"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        //
        //------------------------------------------------------------
        // Case 1.06.2: A complex case with right joins, left joins and inner joins
        //------------------------------------------------------------
        //
        //                           LJ
        //                         /    \
        //                       LF      RJ
        //                      /  \    /  \
        //                     IJ   C  D   LJ
        //                    /  \        /  \
        //                   A    B      RJ   G
        //                              /  \
        //                             E    F
        //
        //------------------------------------------------------------

        rs = st.executeQuery(
            "select distinct f.*"
            + " from a inner join b on a.c1 = b.c1"
            + "       left outer join c on a.c2 = c.c2 "
            + "       left outer join (d right outer join "
            + "                        ((e right outer join f on "
            + "e.c1 = f.c1) left outer join g on f.c1 = g.c1) "
            + "                                                    "
            + "                              on d.c3 = f.c3) "
            + "                                                    "
            + "                               on a.c3 = f.c3");

        expColNames = new String [] {"C1", "C2", "C3"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1", "1", "1"},
            {"2", "3", "4"},
            {"5", "5", "5"},
            {null, null, null}
        };

        JDBC.assertFullResultSet(rs, expRS);

        //--
        //------------------------------------------------------------
        //-- Case 1.07: A complex case with right joins, left joins and inner joins
        //------------------------------------------------------------
        //
        //                           LJ
        //                         /    \
        //                       LJ      RJ
        //                      /  \    /  \
        //                     IJ   C  D   RJ
        //                    /  \        /  \
        //                   A    B      RJ   G
        //                              /  \
        //                             E    F
        //
        //------------------------------------------------------------

        rs = st.executeQuery(
            "select distinct e.*"
            + " from a inner join b on a.c1 = b.c1"
            + "       left outer join c on a.c2 = c.c2 "
            + "       left outer join (d right outer join "
            + "                        ((e right outer join f on "
            + "e.c1 = f.c1) right outer join g on f.c1 = g.c1) "
            + "                                                    "
            + "                               on d.c3 = f.c3) "
            + "                                                    "
            + "                                on a.c3 = f.c3");

        expColNames = new String [] {"C1", "C2", "C3"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1", "2", "3"},
            {"2", "2", "2"},
            {"2", "3", "4"},
            {"5", "5", "5"},
            {null, null, null}
        };

        JDBC.assertFullResultSet(rs, expRS);

        //
        //------------------------------------------------------------
        // Case 1.08: A simple view
        //------------------------------------------------------------
        //
        //                       LJ
        //                      /  \
        //                     IJ  V1(T1)
        //                    /  \
        //                   H    I
        //
        //------------------------------------------------------------

        rs = st.executeQuery(
            "select distinct h.*"
            + " from h inner join i on h.c1 = i.c1"
            + "       left outer join v1 on h.c1 = v1.c1 and h.c2 = v1.c2");

        expColNames = new String [] {"C1", "C2", "C3"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1", "2", "3"},
            {"2", "2", "2"},
            {"2", "3", "4"},
            {"5", "5", "5"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        //
        //------------------------------------------------------------
        // Case 1.9.1: No OJEL because of group-by view
        //------------------------------------------------------------
        //
        //                       LJ
        //                      /  \
        //                     IJ  V2(T2)
        //                    /  \
        //                   H    I
        //
        //------------------------------------------------------------

        rs = st.executeQuery(
            "select distinct h.*"
            + " from h inner join i on h.c1 = i.c1"
            + "       left outer join v2 on h.c1 = v2.c1 and h.c2 = v2.c2");

        expColNames = new String [] {"C1", "C2", "C3"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1", "2", "3"},
            {"2", "2", "2"},
            {"2", "3", "4"},
            {"5", "5", "5"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        //
        //------------------------------------------------------------
        // Case 1.9.2: No OJEL because of hxp columns
        //------------------------------------------------------------
        //
        //                       LJ
        //                      /  \
        //                     IJ   J
        //                    /  \
        //                   H    I
        //
        //------------------------------------------------------------

        rs = st.executeQuery(
            "select distinct h.c1, j.c2"
            + " from h inner join i on h.c1 = i.c1"
            + "       left outer join j on h.c1 = j.c1 and h.c2 = j.c2");

        expColNames = new String [] {"C1", "C2"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1", "2"},
            {"2", "3"},
            {"2", null},
            {"5", null}
        };

        JDBC.assertFullResultSet(rs, expRS);

        //
        // =========================================================
        // Group 2: OJEL - UNIQUE KEY (INDEX)
        // =========================================================

        //------------------------------------------------------------
        // Case 2.01.1: A simple case with only one left join
        //------------------------------------------------------------
        //
        //                         LJ
        //                        /  \
        //                       /    \
        //                      AA    BB
        //
        //------------------------------------------------------------

        rs = st.executeQuery(
            "select aa.* from aa left outer join bb on aa.c1 = bb.c1");

        expColNames = new String [] {"C1", "C2", "C3"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1", "1", "1"},
            {"2", "3", "4"},
            {"5", "5", "5"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        //
        //------------------------------------------------------------
        // Case 2.01.2: A simple case with only one right join
        //------------------------------------------------------------
        //
        //                         RJ
        //                        /  \
        //                       /    \
        //                      AA    BB
        //
        //------------------------------------------------------------

        rs = st.executeQuery(
            "select bb.* from aa right outer join bb on aa.c1 = bb.c1");

        expColNames = new String [] {"C1", "C2", "C3"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1", "2", "3"},
            {"2", "2", "2"},
            {"3", "3", "4"},
            {"5", "5", "5"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        //
        //------------------------------------------------------------
        // Case 2.02.1: A complex case with left joins and inner joins
        //------------------------------------------------------------
        //
        //                         LJ
        //                        /  \
        //                       IJ   GG
        //                      /  \
        //                     LJ   FF
        //                    /  \
        //                   LJ   EE
        //                  /  \
        //                 IJ   DD
        //                /  \
        //               LJ   CC
        //              /  \
        //             AA   BB
        //
        //------------------------------------------------------------

        rs = st.executeQuery(
            "select aa.* "
            + " from aa left outer join bb on aa.c1 = bb.c1 "
            + "        inner join cc on aa.c2 = cc.c2 "
            + "        left outer join dd on aa.c3 = dd.c3"
            + "        left outer join ee on aa.c1 = ee.c1"
            + "        inner join ff on aa.c2 = ff.c2"
            + "        left outer join gg on aa.c3 = gg.c3");

        expColNames = new String [] {"C1", "C2", "C3"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1", "1", "1"},
            {"2", "3", "4"},
            {"5", "5", "5"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        //
        //------------------------------------------------------------
        // Case 2.02.2: A complex case with left joins and inner joins
        //------------------------------------------------------------
        //
        //                         LJ
        //                        /  \
        //                       IJ   GG
        //                      /  \
        //                     LJ   FF
        //                    /  \
        //                   LJ   EE
        //                  /  \
        //                 IJ   DD
        //                /  \
        //               LJ   CC
        //              /  \
        //             AA   BB
        //
        //------------------------------------------------------------

        rs = st.executeQuery(
            "select ee.* "
            + " from aa left outer join bb on aa.c1 = bb.c1 "
            + "        inner join cc on aa.c2 = cc.c2 "
            + "        left outer join dd on aa.c3 = dd.c3"
            + "        left outer join ee on aa.c1 = ee.c1"
            + "        inner join ff on aa.c2 = ff.c2"
            + "        left outer join gg on aa.c3 = gg.c3");

        expColNames = new String [] {"C1", "C2", "C3"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1", "2", "3"},
            {"2", "2", "2"},
            {"5", "5", "5"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        //
        //------------------------------------------------------------
        // Case 2.03.1: A complex case with right joins and inner joins
        //------------------------------------------------------------
        //
        //                         RJ
        //                        /  \
        //                       GG  RJ
        //                          /  \
        //                         FF  RJ
        //                            /  \
        //                           EE  IJ
        //                              /  \
        //                             DD  IJ
        //                                /  \
        //                               CC  RJ
        //                                  /  \
        //                                 BB   AA
        //
        //------------------------------------------------------------

        rs = st.executeQuery(
            "select aa.* "
            + " from gg right outer join "
            + "     (ff right outer join "
            + "      (ee right outer join "
            + "       (dd inner join "
            + "        (cc inner join "
            + "         (bb right outer join aa on bb.c1 = aa.c1)"
            + "                                  on cc.c2 = aa.c2)"
            + "                                   on dd.c3 = aa.c3)"
            + "                                    on ee.c1 = aa.c1)"
            + "                                     on ff.c2 = aa.c2)"
            + "                                      on gg.c3 = aa.c3");

        expColNames = new String [] {"C1", "C2", "C3"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1", "1", "1"},
            {"2", "3", "4"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        //
        //------------------------------------------------------------
        // Case 2.03.2: A complex case with right joins and inner joins
        //------------------------------------------------------------
        //
        //                         RJ
        //                        /  \
        //                       GG  RJ
        //                          /  \
        //                         FF  RJ
        //                            /  \
        //                           EE  IJ
        //                              /  \
        //                             DD  IJ
        //                                /  \
        //                               CC  RJ
        //                                  /  \
        //                                 BB   AA
        //
        //------------------------------------------------------------

        rs = st.executeQuery(
            "select ff.* "
            + " from gg right outer join "
            + "     (ff right outer join "
            + "      (ee right outer join "
            + "       (dd inner join "
            + "        (cc inner join "
            + "         (bb right outer join aa on bb.c1 = aa.c1)"
            + "                                  on cc.c2 = aa.c2)"
            + "                                   on dd.c3 = aa.c3)"
            + "                                    on ee.c1 = aa.c1)"
            + "                                     on ff.c2 = aa.c2)"
            + "                                      on gg.c3 = aa.c3");

        expColNames = new String [] {"C1", "C2", "C3"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1", "1", "1"},
            {"2", "3", "4"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        //
        //------------------------------------------------------------
        // Case 2.04.1: A complex case with right joins, left joins and inner joins
        //------------------------------------------------------------
        //
        //                           LJ
        //                         /    \
        //                       LF      RJ
        //                      /  \    /  \
        //                     IJ  CC  DD  LJ
        //                    /  \        /  \
        //                   AA  BB      EE  FF
        //
        //------------------------------------------------------------

        rs = st.executeQuery(
            "select aa.* "
            + " from aa inner join bb on aa.c1 = bb.c1"
            + "        left outer join cc on aa.c2 = cc.c2 "
            + "        left outer join (dd right outer join "
            + "                        (ee left outer join ff on "
            + "ee.c2 = ff.c2) "
            + "                                                on "
            + "dd.c3 = ee.c3) "
            + "                                                 on "
            + "aa.c1 = ee.c1");

        expColNames = new String [] {"C1", "C2", "C3"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1", "1", "1"},
            {"2", "3", "4"},
            {"5", "5", "5"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        //
        //------------------------------------------------------------
        // Case 2.04.2: A complex case with right joins, left joins and inner joins
        //------------------------------------------------------------
        //
        //                           LJ
        //                         /    \
        //                       LF      RJ
        //                      /  \    /  \
        //                     IJ  CC  DD  LJ
        //                    /  \        /  \
        //                   AA  BB      EE  FF
        //
        //------------------------------------------------------------

        rs = st.executeQuery(
            "select ee.* "
            + " from aa inner join bb on aa.c1 = bb.c1"
            + "        left outer join cc on aa.c2 = cc.c2 "
            + "        left outer join (dd right outer join "
            + "                        (ee left outer join ff on "
            + "ee.c2 = ff.c2) "
            + "                                                on "
            + "dd.c3 = ee.c3) "
            + "                                                 on "
            + "aa.c1 = ee.c1");

        expColNames = new String [] {"C1", "C2", "C3"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1", "2", "3"},
            {"2", "2", "2"},
            {"5", "5", "5"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        //
        //------------------------------------------------------------
        // Case 2.05.1: A complex case with right joins, left joins and inner joins
        //------------------------------------------------------------
        //
        //                           LJ
        //                         /    \
        //                       LF      RJ
        //                      /  \    /  \
        //                     IJ  CC  DD   LJ
        //                    /  \        /  \
        //                   AA  BB      RJ  GG
        //                              /  \
        //                             EE  FF
        //
        //------------------------------------------------------------

        rs = st.executeQuery(
            "select aa.*"
            + " from aa inner join bb on aa.c1 = bb.c1"
            + "        left outer join cc on aa.c2 = cc.c2 "
            + "        left outer join (dd right outer join "
            + "                        ((ee right outer join ff on "
            + "ee.c1 = ff.c1) left outer join gg on ff.c3 = gg.c3) "
            + "                                                    "
            + "                                   on dd.c3 = ff.c3) "
            + "                                                    "
            + "                                    on aa.c2 = ff.c2");

        expColNames = new String [] {"C1", "C2", "C3"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1", "1", "1"},
            {"2", "3", "4"},
            {"5", "5", "5"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        //
        //------------------------------------------------------------
        // Case 2.05.2: A complex case with right joins, left joins and inner joins
        //------------------------------------------------------------
        //
        //                           LJ
        //                         /    \
        //                       LF      RJ
        //                      /  \    /  \
        //                     IJ  CC  DD  LJ
        //                    /  \        /  \
        //                   AA  BB      RJ  GG
        //                              /  \
        //                             EE  FF
        //
        //------------------------------------------------------------

        rs = st.executeQuery(
            "select ff.*"
            + " from aa inner join bb on aa.c1 = bb.c1"
            + "        left outer join cc on aa.c2 = cc.c2 "
            + "        left outer join (dd right outer join "
            + "                        ((ee right outer join ff on "
            + "ee.c1 = ff.c1) left outer join gg on ff.c3 = gg.c3) "
            + "                                                    "
            + "                                   on dd.c3 = ff.c3) "
            + "                                                    "
            + "                                    on aa.c2 = ff.c2");

        expColNames = new String [] {"C1", "C2", "C3"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1", "1", "1"},
            {"2", "3", "4"},
            {"5", "5", "5"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        //
        //------------------------------------------------------------
        // Case 2.06: A complex case with right joins, left joins and inner joins
        //------------------------------------------------------------
        //
        //                           LJ
        //                         /    \
        //                       LJ      RJ
        //                      /  \    /  \
        //                     IJ  CC  DD  RJ
        //                    /  \        /  \
        //                   AA  BB      RJ  GG
        //                              /  \
        //                             EE  FF
        //
        //------------------------------------------------------------

        rs = st.executeQuery(
            "select ee.*"
            + " from aa inner join bb on aa.c1 = bb.c1"
            + "        left outer join cc on aa.c2 = cc.c2 "
            + "        left outer join (dd right outer join "
            + "                        ((ee right outer join ff on "
            + "ee.c2 = ff.c2) right outer join gg on ff.c3 = gg.c3) "
            + "                                                    "
            + "                                    on dd.c3 = ff.c3) "
            + "                                                    "
            + "                                     on aa.c2 = ff.c2");

        expColNames = new String [] {"C1", "C2", "C3"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {null, null, null},
            {"4", "3", "4"},
            {"5", "5", "5"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        //
        //------------------------------------------------------------
        // Case 2.07.1: A composite unique index
        //------------------------------------------------------------
        //
        //                         LJ
        //                        /  \
        //                       LJ   KK
        //                      /  \
        //                     IJ  JJ
        //                    /  \
        //                   HH  II
        //
        //------------------------------------------------------------

        rs = st.executeQuery(
            "select hh.*"
            + " from hh inner join ii on hh.c1 = ii.c1"
            + "        left outer join jj on hh.c1 = jj.c1 and "
            + "hh.c2 = jj.c2"
            + "        left outer join kk on hh.c1 = kk.c1 and "
            + "hh.c2 = kk.c2 and kk.c3 = 5");

        expColNames = new String [] {"C1", "C2", "C3"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1", "1", "1"},
            {"1", "2", "3"},
            {"2", "3", "4"},
            {"2", "3", "4"},
            {"5", "5", "5"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        //
        //------------------------------------------------------------
        // Case 2.07.2: A composite unique index
        //------------------------------------------------------------
        //
        //                         LJ
        //                        /  \
        //                       LJ   KK
        //                      /  \
        //                     IJ  JJ
        //                    /  \
        //                   HH  II
        //
        //------------------------------------------------------------

        rs = st.executeQuery(
            "select hh.*"
            + " from hh inner join ii on hh.c1 = ii.c1"
            + "        left outer join jj on jj.c1 = 0 and jj.c2 is null"
            + "        left outer join kk on hh.c1 = kk.c1 and "
            + "kk.c2 is null and kk.c3 = 5");

        expColNames = new String [] {"C1", "C2", "C3"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1", "1", "1"},
            {"1", "2", "3"},
            {"2", "3", "4"},
            {"2", "3", "4"},
            {"5", "5", "5"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        //
        //------------------------------------------------------------
        // Case 2.07.3: A composite unique index
        //------------------------------------------------------------
        //
        //                         LJ
        //                        /  \
        //                       LJ   KK
        //                      /  \
        //                     IJ  JJ
        //                    /  \
        //                   HH  II
        //
        //------------------------------------------------------------

        rs = st.executeQuery(
            "select hh.*"
            + " from hh inner join ii on hh.c1 = ii.c1"
            + "        left outer join jj on jj.c1 = 0"
            + "        left outer join kk on hh.c1 = kk.c1 and "
            + "hh.c2 = kk.c2 and kk.c3 = 5");

        expColNames = new String [] {"C1", "C2", "C3"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1", "1", "1"},
            {"1", "2", "3"},
            {"2", "3", "4"},
            {"2", "3", "4"},
            {"5", "5", "5"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        //
        //------------------------------------------------------------
        // Case 2.08.1: No OJEL because of wrong types of ON predicates
        //------------------------------------------------------------
        //
        //                         LJ
        //                        /  \
        //                       LJ   KK
        //                      /  \
        //                     IJ  JJ
        //                    /  \
        //                   HH  II
        //
        //------------------------------------------------------------

        rs = st.executeQuery(
            "select hh.*"
            + " from hh inner join ii on hh.c1 = ii.c1"
            + "        left outer join jj on jj.c1 = 0 and jj.c2 "
            + "is not null"
            + "        left outer join kk on hh.c1 = kk.c1 and "
            + "hh.c2 = kk.c2 and kk.c3 <> 5");

        expColNames = new String [] {"C1", "C2", "C3"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1", "1", "1"},
            {"1", "2", "3"},
            {"2", "3", "4"},
            {"2", "3", "4"},
            {"5", "5", "5"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        //
        //------------------------------------------------------------
        // Case 2.08.2: No OJEL because of wrong types of ON predicates
        //------------------------------------------------------------
        //
        //                         LJ
        //                        /  \
        //                       LJ   KK
        //                      /  \
        //                     IJ  JJ
        //                    /  \
        //                   HH  II
        //
        //------------------------------------------------------------

        rs = st.executeQuery(
            "select hh.*"
            + " from hh inner join ii on hh.c1 = ii.c1"
            + "        left outer join jj on hh.c1 = jj.c1 and "
            + "jj.c2 is not null"
            + "        left outer join kk on hh.c1 = kk.c1 and kk.c3 <= 5");

        expColNames = new String [] {"C1", "C2", "C3"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1", "1", "1"},
            {"1", "1", "1"},
            {"1", "1", "1"},
            {"1", "1", "1"},
            {"1", "2", "3"},
            {"1", "2", "3"},
            {"1", "2", "3"},
            {"1", "2", "3"},
            {"2", "3", "4"},
            {"2", "3", "4"},
            {"5", "5", "5"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        //
        //------------------------------------------------------------
        // Case 2.08.3: No OJEL because of wrong types of ON predicates
        //------------------------------------------------------------
        //
        //                         LJ
        //                        /  \
        //                       LJ   KK
        //                      /  \
        //                     IJ  JJ
        //                    /  \
        //                   HH  II
        //
        //------------------------------------------------------------

        rs = st.executeQuery(
            "select hh.*"
            + " from hh inner join ii on hh.c1 = ii.c1"
            + "        left outer join jj on hh.c1 = jj.c1 and "
            + "jj.c2 is not null"
            + "        left outer join kk on hh.c1 = kk.c1 and "
            + "kk.c2 = kk.c3");

        expColNames = new String [] {"C1", "C2", "C3"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1", "1", "1"},
            {"1", "1", "1"},
            {"1", "2", "3"},
            {"1", "2", "3"},
            {"2", "3", "4"},
            {"2", "3", "4"},
            {"5", "5", "5"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        //
        //------------------------------------------------------------
        // Case 2.08.4: No OJEL because of view
        //------------------------------------------------------------
        //
        //                       LJ
        //                      /  \
        //                     IJ  V1
        //                    /  \
        //                   HH  II
        //
        //------------------------------------------------------------

        rs = st.executeQuery(
            "select hh.*"
            + " from hh inner join ii on hh.c1 = ii.c1"
            + "        left outer join v1 on hh.c1 = v1.c1 and "
            + "hh.c2 = v1.c2");

        expColNames = new String [] {"C1", "C2", "C3"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1", "1", "1"},
            {"1", "2", "3"},
            {"2", "3", "4"},
            {"2", "3", "4"},
            {"5", "5", "5"}
        };

        JDBC.assertFullResultSet(rs, expRS);

        //
        //------------------------------------------------------------
        // Case 2.08.5: No OJEL because of hxp columns
        //------------------------------------------------------------
        //
        //                       LJ
        //                      /  \
        //                     IJ  JJ
        //                    /  \
        //                   HH  II
        //
        //------------------------------------------------------------

        rs = st.executeQuery(
            "select jj.*"
            + " from hh inner join ii on hh.c1 = ii.c1"
            + "        left outer join jj on hh.c1 = jj.c1 and "
            + "hh.c2 = jj.c2");

        expColNames = new String [] {"C1", "C2", "C3"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1", "1", "1"},
            {"1", "2", "3"},
            {null, null, null},
            {null, null, null},
            {null, null, null}
        };

        JDBC.assertFullResultSet(rs, expRS);

        st.close();
        rollback();
    }
}