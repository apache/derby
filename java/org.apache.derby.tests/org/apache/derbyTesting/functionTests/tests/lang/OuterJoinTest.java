/*
Derby - Class org.apache.derbyTesting.functionTests.tests.lang.OuterJoinTest

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

import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.sql.Timestamp;
import junit.framework.Test;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.RuntimeStatisticsParser;
import org.apache.derbyTesting.junit.TestConfiguration;

public final class OuterJoinTest extends BaseJDBCTestCase
{

    /**
     * Public constructor required for running test as standalone JUnit.
     */
    public OuterJoinTest(String name)
    {
        super(name);
    }

    public static Test suite()
    {
        //Add the test case into the test suite
//IC see: https://issues.apache.org/jira/browse/DERBY-6590
        BaseTestSuite suite = new BaseTestSuite("OuterJoinTest Test");
        return TestConfiguration.defaultSuite(OuterJoinTest.class);
    }

    private void createTestObjects(Statement st) throws Exception
    {
        setAutoCommit(false);

        //clean the database tests in previously created
        CleanDatabaseTestSetup.cleanDatabase(getConnection(), false);

        // create some tables
        
        st.executeUpdate("create table t1(c1 int)");
        st.executeUpdate("create table t2(c1 int)");
        st.executeUpdate("create table t3(c1 int)");
        st.executeUpdate("create table tt1(c1 int, c2 int, c3 int)");
        st.executeUpdate("create table tt2(c1 int, c2 int, c3 int)");
        st.executeUpdate("create table tt3(c1 int, c2 int, c3 int)");
        st.executeUpdate("create table empty_table(c1 int)");
        st.executeUpdate("create table insert_test(c1 int, c2 int, c3 int)");
        st.executeUpdate("create table x (c1 int, c2 int, c3 int)");
        st.executeUpdate("create table y (c3 int, c4 int, c5 int)");
        st.executeUpdate("create table a (c1 int)");
        st.executeUpdate("create table b (c2 float)");
        st.executeUpdate("create table c (c3 char(30))");
        // following is verifying that oj is not a keyword
        st.executeUpdate("create table oj(oj int)");
        
        // populate the tables
        
        st.executeUpdate("insert into t1 values 1, 2, 2, 3, 4");
        st.executeUpdate("insert into t2 values 1, 3, 3, 5, 6");
        st.executeUpdate("insert into t3 values 2, 3, 5, 5, 7");
        st.executeUpdate("insert into tt1 select c1, c1, c1 from t1");
        st.executeUpdate("insert into tt2 select c1, c1, c1 from t2");
        st.executeUpdate("insert into tt3 select c1, c1, c1 from t3");
        st.executeUpdate("insert into x values (1, 2, 3), (4, 5, 6)");
        st.executeUpdate("insert into y values (3, 4, 5), (666, 7, 8)");
        st.executeUpdate(
            "insert into insert_test"
            + "(select * from t1 a left outer join t2 b on a.c1 = "
            + "b.c1 left outer join t3 c on a.c1 <> c.c1)");

        st.executeUpdate("insert into a values 1");
        st.executeUpdate("insert into b values 3.3");
        st.executeUpdate("insert into c values 'asdf'");
        // verifying that oj is not a keyword
        st.executeUpdate("insert into oj(oj) values (1)");
    }

    // negative tests on outer join 
    public void testNegative() throws Exception
    {
        Statement st = createStatement();

        createTestObjects(st);
        
        assertStatementError("42X01", st,
            "select * from t1 outer join t2");
        
        // no join clause
        
        assertStatementError("42X01", st,
            "select * from t1 left outer join t2");
        
        assertStatementError("42X01", st,
            " select * from t1 right outer join t2");
        }

    // positive tests on normal forms of outer join
    public void testPositive() throws Exception
    {
        Statement st = createStatement();
        ResultSet rs = null;
        ResultSetMetaData rsmd;
        PreparedStatement pSt;
        String [][] expRS;
        String [] expColNames;

        createTestObjects(st);
                
        rs = st.executeQuery(
            "select t1.c1 from t1 left outer join t2 on t1.c1 = t2.c1");
        
        expColNames = new String [] {"C1"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1"},
            {"2"},
            {"2"},
            {"3"},
            {"3"},
            {"4"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select t2.c1 from t1 right outer join t2 on t1.c1 = t2.c1");
        
        expColNames = new String [] {"C1"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1"},
            {"3"},
            {"3"},
            {"5"},
            {"6"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select a.x from t1 a (x) left outer join t2 b (x) "
            + "on a.x = b.x");
        
        expColNames = new String [] {"X"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1"},
            {"2"},
            {"2"},
            {"3"},
            {"3"},
            {"4"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        // verify that selects from inner table work
        
        rs = st.executeQuery(
            "select b.* from (values 9) a left outer join t2 b on 1=1");
        
        expColNames = new String [] {"C1"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1"},
            {"3"},
            {"3"},
            {"5"},
            {"6"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select b.* from (values 9) a left outer join t2 b on 1=0");
        
        expColNames = new String [] {"C1"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {null}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select b.* from (values 9) a right outer join t2 b on 1=0");
        
        expColNames = new String [] {"C1"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1"},
            {"3"},
            {"3"},
            {"5"},
            {"6"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select a.* from (values 9) a right outer join t2 b on 1=1");
        
        expColNames = new String [] {"1"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"9"},
            {"9"},
            {"9"},
            {"9"},
            {"9"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select a.* from (values 9) a right outer join t2 b on 1=0");
        
        expColNames = new String [] {"1"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {null},
            {null},
            {null},
            {null},
            {null}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select a.* from ((values ('a', 'b')) a inner join "
            + "(values ('c', 'd')) b on 1=1) left outer join "
            + "(values ('e', 'f')) c on 1=1");
        
        expColNames = new String [] {"1", "2"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"a", "b"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select b.* from ((values ('a', 'b')) a inner join "
            + "(values ('c', 'd')) b on 1=1) left outer join "
            + "(values ('e', 'f')) c on 1=1");
        
        expColNames = new String [] {"1", "2"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"c", "d"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select c.* from ((values ('a', 'b')) a inner join "
            + "(values ('c', 'd')) b on 1=1) left outer join "
            + "(values ('e', 'f')) c on 1=1");
        
        expColNames = new String [] {"1", "2"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"e", "f"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        // verifying that oj is not a keyword
        
        rs = st.executeQuery(
            "select * from oj where oj = 1");
        
        expColNames = new String [] {"OJ"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        //verifying both regular and {oj } in
        
        rs = st.executeQuery(
            "select * from t1 left outer join {oj t2 left outer "
            + "join t3 on t2.c1=t3.c1} on t1.c1=t3.c1");
        
        expColNames = new String [] {"C1", "C1", "C1"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1", null, null},
            {"2", null, null},
            {"2", null, null},
            {"3", "3", "3"},
            {"3", "3", "3"},
            {"4", null, null}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);

        // left and right outer join with an empty table
        
        rs = st.executeQuery(
            "select t1.c1 from t1 left outer join empty_table et "
            + "on t1.c1 = et.c1");
        
        expColNames = new String [] {"C1"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1"},
            {"2"},
            {"2"},
            {"3"},
            {"4"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select t1.c1 from t1 right outer join empty_table "
            + "et on t1.c1 = et.c1");
        
        expColNames = new String [] {"C1"};
        JDBC.assertColumnNames(rs, expColNames);
        JDBC.assertDrainResults(rs, 0);
        
        rs = st.executeQuery(
            " select t1.c1 from empty_table et right outer join "
            + "t1 on et.c1 = t1.c1");
        
        expColNames = new String [] {"C1"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1"},
            {"2"},
            {"2"},
            {"3"},
            {"4"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);

        // this query may make no sense at all, but it's just 
        // trying to show that parser works fine with both regular 
        // tableexpression and tableexpression with {oj }
        
        rs = st.executeQuery(
            "select * from t1, {oj t2 join t3 on t2.c1=t3.c1}");
        
        expColNames = new String [] {"C1", "C1", "C1"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1", "3", "3"},
            {"1", "3", "3"},
            {"1", "5", "5"},
            {"1", "5", "5"},
            {"2", "3", "3"},
            {"2", "3", "3"},
            {"2", "5", "5"},
            {"2", "5", "5"},
            {"2", "3", "3"},
            {"2", "3", "3"},
            {"2", "5", "5"},
            {"2", "5", "5"},
            {"3", "3", "3"},
            {"3", "3", "3"},
            {"3", "5", "5"},
            {"3", "5", "5"},
            {"4", "3", "3"},
            {"4", "3", "3"},
            {"4", "5", "5"},
            {"4", "5", "5"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);

        // parameters and join clause
        
        pSt = prepareStatement(
            "select * from t1 left outer join t2 on 1=? and t1.c1 = t2.c1");
        
        rs = st.executeQuery("values 1");
        
        rs.next();
        rsmd = rs.getMetaData();
        for (int i = 1; i <= rsmd.getColumnCount(); i++)
            pSt.setObject(i, rs.getObject(i));
        
        rs = pSt.executeQuery();
        expColNames = new String [] {"C1", "C1"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1", "1"},
            {"2", null},
            {"2", null},
            {"3", "3"},
            {"3", "3"},
            {"4", null}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        pSt = prepareStatement(
            "select * from t1 left outer join t2 on t1.c1 = "
            + "t2.c1 and t1.c1 = ?");
        
        rs = st.executeQuery("values 1");
        
        rs.next();
        rsmd = rs.getMetaData();
        for (int i = 1; i <= rsmd.getColumnCount(); i++)
            pSt.setObject(i, rs.getObject(i));
        
        rs = pSt.executeQuery();
        expColNames = new String [] {"C1", "C1"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1", "1"},
            {"2", null},
            {"2", null},
            {"3", null},
            {"4", null}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);        
        
        // additional predicates outside of the join clause egs of 
        // using {oj --} syntax
        
        rs = st.executeQuery(
            "select * from t1 left outer join t2 on t1.c1 = "
            + "t2.c1 where t1.c1 = 1");
        
        expColNames = new String [] {"C1", "C1"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1", "1"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select * from {oj t1 left outer join t2 on t1.c1 = "
            + "t2.c1} where t1.c1 = 1");
        
        expColNames = new String [] {"C1", "C1"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1", "1"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select * from t1 right outer join t2 on t1.c1 = 1 "
            + "where t2.c1 = t1.c1");
        
        expColNames = new String [] {"C1", "C1"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1", "1"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select * from {oj t1 right outer join t2 on t1.c1 "
            + "= 1} where t2.c1 = t1.c1");
        
        expColNames = new String [] {"C1", "C1"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1", "1"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
    }
    
    public void testOuterJoinWithSubquery() throws Exception
    {
        // subquery in join clause. egs of using {oj --} syntax
        
        ResultSet rs = null;
        Statement st = createStatement();
        String [][] expRS;
        String [] expColNames;

        createTestObjects(st);


        rs = st.executeQuery(
            "select * from t1 a left outer join t2 b "
            + "on a.c1 = b.c1 and a.c1 = (select c1 from t1 where "
            + "a.c1 = t1.c1 and a.c1 = 1)");

        expColNames = new String [] {"C1", "C1"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1", "1"},
            {"2", null},
            {"2", null},
            {"3", null},
            {"4", null}
        };

        JDBC.assertFullResultSet(rs, expRS, true);

        rs = st.executeQuery(
            " select * from {oj t1 a left outer join t2 b "
            + "on a.c1 = b.c1 and a.c1 = (select c1 from t1 where "
            + "a.c1 = t1.c1 and a.c1 = 1)}");

        expColNames = new String [] {"C1", "C1"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1", "1"},
            {"2", null},
            {"2", null},
            {"3", null},
            {"4", null}
        };

        JDBC.assertFullResultSet(rs, expRS, true);

        rs = st.executeQuery(
            " select * from t1 a left outer join t2 b "
            + "on a.c1 = b.c1 and a.c1 = (select c1 from t1 where "
            + "a.c1 = t1.c1 and a.c1 <> 2)");

        expColNames = new String [] {"C1", "C1"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1", "1"},
            {"2", null},
            {"2", null},
            {"3", "3"},
            {"3", "3"},
            {"4", null}
        };

        JDBC.assertFullResultSet(rs, expRS, true);

        rs = st.executeQuery(
            " select * from {oj t1 a left outer join t2 b "
            + "on a.c1 = b.c1 and a.c1 = (select c1 from t1 where "
            + "a.c1 = t1.c1 and a.c1 <> 2)}");

        expColNames = new String [] {"C1", "C1"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1", "1"},
            {"2", null},
            {"2", null},
            {"3", "3"},
            {"3", "3"},
            {"4", null}
        };

        JDBC.assertFullResultSet(rs, expRS, true);

        rs = st.executeQuery(
            " select * from t1 a right outer join t2 b "
            + "on a.c1 = b.c1 and a.c1 in (select c1 from t1 where "
            + "a.c1 = t1.c1)");

        expColNames = new String [] {"C1", "C1"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1", "1"},
            {"3", "3"},
            {"3", "3"},
            {null, "5"},
            {null, "6"}
        };

        JDBC.assertFullResultSet(rs, expRS, true);
    }
    
    public void testOuterJoinWithinSubquery() throws Exception
    {
        //outer join in subquery egs of using {oj --} syntax
        
        ResultSet rs = null;
        Statement st = createStatement();
        String [][] expRS;
        String [] expColNames;

        createTestObjects(st);

        rs = st.executeQuery(
            "select * from (t1 a)"
            + "where exists (select * from t1 left outer join t2 "
            + "on t1.c1 = t2.c1)");

        expColNames = new String [] {"C1"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1"},
            {"2"},
            {"2"},
            {"3"},
            {"4"}
        };

        JDBC.assertFullResultSet(rs, expRS, true);

        rs = st.executeQuery(
            " select * from (t1 a)"
            + "where exists (select * from {oj t1 left outer join "
            + "t2 on t1.c1 = t2.c1})");

        expColNames = new String [] {"C1"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1"},
            {"2"},
            {"2"},
            {"3"},
            {"4"}
        };

        JDBC.assertFullResultSet(rs, expRS, true);

        rs = st.executeQuery(
            " select * from (t1 a)"
            + "where exists (select * from t1 left outer join t2 on 1=0)");

        expColNames = new String [] {"C1"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1"},
            {"2"},
            {"2"},
            {"3"},
            {"4"}
        };

        JDBC.assertFullResultSet(rs, expRS, true);
    }

    public void testNestedJoins() throws Exception
    {
        // nested joins egs of using {oj --} syntax

        Statement st = createStatement();
        ResultSet rs = null;
        String [][] expRS;
        String [] expColNames;

        createTestObjects(st);
        
        rs = st.executeQuery(
            "select * from t1 left outer join t2 on t1.c1 = "
            + "t2.c1 left outer join t3 on t1.c1 = t3.c1");
        
        expColNames = new String [] {"C1", "C1", "C1"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1", "1", null},
            {"2", null, "2"},
            {"2", null, "2"},
            {"3", "3", "3"},
            {"3", "3", "3"},
            {"4", null, null}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select * from {oj t1 left outer join t2 on t1.c1 = "
            + "t2.c1 left outer join t3 on t1.c1 = "
            + "t3.c1}");
        
        expColNames = new String [] {"C1", "C1", "C1"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1", "1", null},
            {"2", null, "2"},
            {"2", null, "2"},
            {"3", "3", "3"},
            {"3", "3", "3"},
            {"4", null, null}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select * from t1 left outer join t2 on t1.c1 = "
            + "t2.c1 left outer join t3 on t2.c1 = t3.c1");
        
        expColNames = new String [] {"C1", "C1", "C1"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1", "1", null},
            {"2", null, null},
            {"2", null, null},
            {"3", "3", "3"},
            {"3", "3", "3"},
            {"4", null, null}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select * from t3 right outer join t2 on t3.c1 = "
            + "t2.c1 right outer join t1 on t1.c1 = t2.c1");
        
        expColNames = new String [] {"C1", "C1", "C1"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {null, "1", "1"},
            {null, null, "2"},
            {null, null, "2"},
            {"3", "3", "3"},
            {"3", "3", "3"},
            {null, null, "4"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        // parents
        
        rs = st.executeQuery(
            "select * from (t1 left outer join t2 on t1.c1 = "
            + "t2.c1) left outer join t3 on t1.c1 = t3.c1");
        
        expColNames = new String [] {"C1", "C1", "C1"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1", "1", null},
            {"2", null, "2"},
            {"2", null, "2"},
            {"3", "3", "3"},
            {"3", "3", "3"},
            {"4", null, null}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select * from t1 left outer join (t2 left outer "
            + "join t3 on t2.c1 = t3.c1) on t1.c1 = t2.c1");
        
        expColNames = new String [] {"C1", "C1", "C1"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1", "1", null},
            {"2", null, null},
            {"2", null, null},
            {"3", "3", "3"},
            {"3", "3", "3"},
            {"4", null, null}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
    }

    public void testLeftRightOuterJoinCombination() throws Exception
    {
        // left/right outer join combinations

        ResultSet rs = null;
        Statement st = createStatement();
        String [][] expRS;
        String [] expColNames;

        createTestObjects(st);
             
        rs = st.executeQuery(
            "select * from t1 a right outer join t2 b on a.c1 = "
            + "b.c1 left outer join t3 c on a.c1 = b.c1 and b.c1 = c.c1");
        
        expColNames = new String [] {"C1", "C1", "C1"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1", "1", null},
            {"3", "3", "3"},
            {"3", "3", "3"},
            {null, "5", null},
            {null, "6", null}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select * from (t1 a right outer join t2 b on a.c1 "
            + "= b.c1) left outer join t3 c on a.c1 = b.c1 and b.c1 = c.c1");
        
        expColNames = new String [] {"C1", "C1", "C1"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1", "1", null},
            {"3", "3", "3"},
            {"3", "3", "3"},
            {null, "5", null},
            {null, "6", null}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select * from t1 a left outer join t2 b on a.c1 = "
            + "b.c1 right outer join t3 c on c.c1 = a.c1 where "
            + "a.c1 is not null");
        
        expColNames = new String [] {"C1", "C1", "C1"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"2", null, "2"},
            {"2", null, "2"},
            {"3", "3", "3"},
            {"3", "3", "3"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select * from (t1 a left outer join t2 b on a.c1 = "
            + "b.c1) right outer join t3 c on c.c1 = a.c1 where "
            + "a.c1 is not null");
        
        expColNames = new String [] {"C1", "C1", "C1"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"2", null, "2"},
            {"2", null, "2"},
            {"3", "3", "3"},
            {"3", "3", "3"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select * from t1 a left outer join (t2 b right "
            + "outer join t3 c on c.c1 = b.c1) on a.c1 = c.c1 "
            + "where c.c1=b.c1");
        
        expColNames = new String [] {"C1", "C1", "C1"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"3", "3", "3"},
            {"3", "3", "3"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);

    }

    public void testOuterJoinWithInsertUpdateDelete() throws Exception
    {
        // test insert/update/delete

        Statement st = createStatement();
        ResultSet rs = null;
        String [][] expRS;
        String [] expColNames;

        createTestObjects(st);       
        
        rs = st.executeQuery("select * from insert_test");
        
        expColNames = new String [] {"C1", "C2", "C3"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1", "1", "2"},
            {"1", "1", "3"},
            {"1", "1", "5"},
            {"1", "1", "5"},
            {"1", "1", "7"},
            {"2", null, "3"},
            {"2", null, "5"},
            {"2", null, "5"},
            {"2", null, "7"},
            {"2", null, "3"},
            {"2", null, "5"},
            {"2", null, "5"},
            {"2", null, "7"},
            {"3", "3", "2"},
            {"3", "3", "5"},
            {"3", "3", "5"},
            {"3", "3", "7"},
            {"3", "3", "2"},
            {"3", "3", "5"},
            {"3", "3", "5"},
            {"3", "3", "7"},
            {"4", null, "2"},
            {"4", null, "3"},
            {"4", null, "5"},
            {"4", null, "5"},
            {"4", null, "7"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);

        assertUpdateCount(st, 5,
            " update insert_test"
            + " set c1 = (select 9 from t1 a left outer join t1 b"
            + " on a.c1 = b.c1 where a.c1 = 1)"
            + " where c1 = 1");

        rs = st.executeQuery("select * from insert_test");

        expColNames = new String [] {"C1", "C2", "C3"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"9", "1", "2"},
            {"9", "1", "3"},
            {"9", "1", "5"},
            {"9", "1", "5"},
            {"9", "1", "7"},
            {"2", null, "3"},
            {"2", null, "5"},
            {"2", null, "5"},
            {"2", null, "7"},
            {"2", null, "3"},
            {"2", null, "5"},
            {"2", null, "5"},
            {"2", null, "7"},
            {"3", "3", "2"},
            {"3", "3", "5"},
            {"3", "3", "5"},
            {"3", "3", "7"},
            {"3", "3", "2"},
            {"3", "3", "5"},
            {"3", "3", "5"},
            {"3", "3", "7"},
            {"4", null, "2"},
            {"4", null, "3"},
            {"4", null, "5"},
            {"4", null, "5"},
            {"4", null, "7"}
        };

        JDBC.assertFullResultSet(rs, expRS, true);
        
        assertUpdateCount(st, 5,
            " delete from insert_test"
            + " where c1 = (select 9 from t1 a left outer join t1 b"
            + " on a.c1 = b.c1 where a.c1 = 1)");

        rs = st.executeQuery("select * from insert_test");

        expColNames = new String [] {"C1", "C2", "C3"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"2", null, "3"},
            {"2", null, "5"},
            {"2", null, "5"},
            {"2", null, "7"},
            {"2", null, "3"},
            {"2", null, "5"},
            {"2", null, "5"},
            {"2", null, "7"},
            {"3", "3", "2"},
            {"3", "3", "5"},
            {"3", "3", "5"},
            {"3", "3", "7"},
            {"3", "3", "2"},
            {"3", "3", "5"},
            {"3", "3", "5"},
            {"3", "3", "7"},
            {"4", null, "2"},
            {"4", null, "3"},
            {"4", null, "5"},
            {"4", null, "5"},
            {"4", null, "7"}
        };

        JDBC.assertFullResultSet(rs, expRS, true);

        assertUpdateCount(st, 21, "delete from insert_test");
        
        st.executeUpdate(
            " insert into insert_test"
            + " (select * from (select * from t1 a left outer join"
            + " t2 b on a.c1 = b.c1 left outer join t3 c on a.c1 <>"
            + " c.c1) d (c1, c2, c3))");
        
        rs = st.executeQuery("select * from insert_test");
        
        expColNames = new String [] {"C1", "C2", "C3"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1", "1", "2"},
            {"1", "1", "3"},
            {"1", "1", "5"},
            {"1", "1", "5"},
            {"1", "1", "7"},
            {"2", null, "3"},
            {"2", null, "5"},
            {"2", null, "5"},
            {"2", null, "7"},
            {"2", null, "3"},
            {"2", null, "5"},
            {"2", null, "5"},
            {"2", null, "7"},
            {"3", "3", "2"},
            {"3", "3", "5"},
            {"3", "3", "5"},
            {"3", "3", "7"},
            {"3", "3", "2"},
            {"3", "3", "5"},
            {"3", "3", "5"},
            {"3", "3", "7"},
            {"4", null, "2"},
            {"4", null, "3"},
            {"4", null, "5"},
            {"4", null, "5"},
            {"4", null, "7"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);

        assertUpdateCount(st, 26, "delete from insert_test");
    }

    public void testMulticolumn() throws Exception
    {
        // multicolumn tests c1, c2, and c3 all have the same values

        Statement st = createStatement();
        ResultSet rs = null;
        String [][] expRS;
        String [] expColNames;

        createTestObjects(st);

        rs = st.executeQuery(
            "select tt1.c1, tt1.c2, tt1.c3, tt2.c2, tt2.c3 from "
            + "tt1 left outer join tt2 on tt1.c1 = tt2.c1");
        
        expColNames = new String [] {"C1", "C2", "C3", "C2", "C3"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1", "1", "1", "1", "1"},
            {"2", "2", "2", null, null},
            {"2", "2", "2", null, null},
            {"3", "3", "3", "3", "3"},
            {"3", "3", "3", "3", "3"},
            {"4", "4", "4", null, null}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select tt1.c1, tt1.c2, tt1.c3, tt2.c3 from tt1 "
            + "left outer join tt2 on tt1.c1 = tt2.c1");
        
        expColNames = new String [] {"C1", "C2", "C3", "C3"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1", "1", "1", "1"},
            {"2", "2", "2", null},
            {"2", "2", "2", null},
            {"3", "3", "3", "3"},
            {"3", "3", "3", "3"},
            {"4", "4", "4", null}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select tt1.c1, tt1.c2, tt1.c3 from tt1 left outer "
            + "join tt2 on tt1.c1 = tt2.c1");
        
        expColNames = new String [] {"C1", "C2", "C3"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1", "1", "1"},
            {"2", "2", "2"},
            {"2", "2", "2"},
            {"3", "3", "3"},
            {"3", "3", "3"},
            {"4", "4", "4"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);

        // nested outer joins

        rs = st.executeQuery(
            "select tt1.c2, tt1.c1, tt1.c3, tt2.c1, tt2.c3 from "
            + "t1 left outer join tt1 on t1.c1 = tt1.c1 left outer "
            + "join tt2 on tt1.c2 = tt2.c2");

        expColNames = new String [] {"C2", "C1", "C3", "C1", "C3"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1", "1", "1", "1", "1"},
            {"2", "2", "2", null, null},
            {"2", "2", "2", null, null},
            {"2", "2", "2", null, null},
            {"2", "2", "2", null, null},
            {"3", "3", "3", "3", "3"},
            {"3", "3", "3", "3", "3"},
            {"4", "4", "4", null, null}
        };

        JDBC.assertFullResultSet(rs, expRS, true);
    }
    
    public void testColumnReordering() throws Exception
    {
        // make sure that column reordering is working correctly
        // when there's an ON clause

        Statement st = createStatement();
        ResultSet rs = null;
        String [][] expRS;
        String [] expColNames;

        createTestObjects(st);

        // qualfied * will return all of the columns of the 
        // qualified table including join columns
        
        rs = st.executeQuery(
            "select x.* from x join y on x.c3 = y.c3");
        
        expColNames = new String [] {"C1", "C2", "C3"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1", "2", "3"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select x.* from x left outer join y on x.c3 = y.c3");
        
        expColNames = new String [] {"C1", "C2", "C3"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1", "2", "3"},
            {"4", "5", "6"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select x.* from x right outer join y on x.c3 = y.c3");
        
        expColNames = new String [] {"C1", "C2", "C3"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1", "2", "3"},
            {null, null, null}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select y.* from x join y on x.c3 = y.c3");
        
        expColNames = new String [] {"C3", "C4", "C5"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"3", "4", "5"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select y.* from x left outer join y on x.c3 = y.c3");
        
        expColNames = new String [] {"C3", "C4", "C5"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"3", "4", "5"},
            {null, null, null}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select y.* from x right outer join y on x.c3 = y.c3");
        
        expColNames = new String [] {"C3", "C4", "C5"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"3", "4", "5"},
            {"666", "7", "8"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        // * will return all of the columns of all joined tables
        
        rs = st.executeQuery(
            "select * from x join y on x.c3 = y.c3");
        
        expColNames = new String [] {"C1", "C2", "C3", "C3", "C4", "C5"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1", "2", "3", "3", "4", "5"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select * from x left outer join y on x.c3 = y.c3");
        
        expColNames = new String [] {"C1", "C2", "C3", "C3", "C4", "C5"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1", "2", "3", "3", "4", "5"},
            {"4", "5", "6", null, null, null}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select * from x right outer join y on x.c3 = y.c3");
        
        expColNames = new String [] {"C1", "C2", "C3", "C3", "C4", "C5"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1", "2", "3", "3", "4", "5"},
            {null, null, null, "666", "7", "8"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
    }

    public void testRightOuterJoinXform() throws Exception
    {
        // verify that right outer join xforms don't get result
        // columns confused

        Statement st = createStatement();
        ResultSet rs = null;
        String [][] expRS;
        String [] expColNames;

        createTestObjects(st);

        rs = st.executeQuery(
            " select * from a left outer join b on 1=1 left "
            + "outer join c on 1=1");

        expColNames = new String [] {"C1", "C2", "C3"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1", "3.3", "asdf"}
        };

        JDBC.assertFullResultSet(rs, expRS, true);

        rs = st.executeQuery(
            " select * from a left outer join b on 1=1 left "
            + "outer join c on 1=0");

        expColNames = new String [] {"C1", "C2", "C3"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1", "3.3", null}
        };

        JDBC.assertFullResultSet(rs, expRS, true);

        rs = st.executeQuery(
            " select * from a left outer join b on 1=0 left "
            + "outer join c on 1=1");

        expColNames = new String [] {"C1", "C2", "C3"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1", null, "asdf"}
        };

        JDBC.assertFullResultSet(rs, expRS, true);

        rs = st.executeQuery(
            " select * from a left outer join b on 1=0 left "
            + "outer join c on 1=0");

        expColNames = new String [] {"C1", "C2", "C3"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1", null, null}
        };

        JDBC.assertFullResultSet(rs, expRS, true);

        rs = st.executeQuery(
            " select * from c right outer join b on 1=1 right "
            + "outer join a on 1=1");

        expColNames = new String [] {"C3", "C2", "C1"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"asdf", "3.3", "1"}
        };

        JDBC.assertFullResultSet(rs, expRS, true);

        rs = st.executeQuery(
            " select * from c right outer join b on 1=1 right "
            + "outer join a on 1=0");

        expColNames = new String [] {"C3", "C2", "C1"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {null, null, "1"}
        };

        JDBC.assertFullResultSet(rs, expRS, true);

        rs = st.executeQuery(
            " select * from c right outer join b on 1=0 right "
            + "outer join a on 1=1");

        expColNames = new String [] {"C3", "C2", "C1"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {null, "3.3", "1"}
        };

        JDBC.assertFullResultSet(rs, expRS, true);

        rs = st.executeQuery(
            " select * from c right outer join b on 1=0 right "
            + "outer join a on 1=0");

        expColNames = new String [] {"C3", "C2", "C1"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {null, null, "1"}
        };

        JDBC.assertFullResultSet(rs, expRS, true);
    }
    
    public void testInnerJoinXform() throws Exception
    {
        // test outer join -> inner join xform
        
        Statement st = createStatement();
        ResultSet rs = null;
        String [][] expRS;
        String [] expColNames;

        createTestObjects(st);

        assertUpdateCount(st, 5, "delete from tt1");
        assertUpdateCount(st, 5, " delete from tt2");
        assertUpdateCount(st, 5, " delete from tt3");
        
        st.executeUpdate(
            " insert into tt1 values (1, 2, 3), (2, 3, 4), (3, 4, 5)");
        
        st.executeUpdate(
            " insert into tt2 values (1, 2, 3), (2, 3, 4), (3, 4, 5)");
        
        st.executeUpdate(
            " insert into tt3 values (1, 2, 3), (2, 3, 4), (3, 4, 5)");

        CallableStatement cSt = prepareCall(
                " call SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1)");
        
        assertUpdateCount(cSt, 0);
        
        // no xform, predicate on outer table
        
        rs = st.executeQuery(
            "select * from tt1 left outer join tt2 on tt1.c1 = "
            + "tt2.c2 where tt1.c1 = 3");
        
        expColNames = new String [] {"C1", "C2", "C3", "C1", "C2", "C3"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"3", "4", "5", "2", "3", "4"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        // various predicates on inner table
        
        rs = st.executeQuery(
            "select * from tt1 left outer join tt2 on tt1.c1 = "
            + "tt2.c2 where tt2.c2 = 3");
        
        expColNames = new String [] {"C1", "C2", "C3", "C1", "C2", "C3"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"3", "4", "5", "2", "3", "4"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS()");

        rs.next();
        
        if(usingEmbedded()){
            RuntimeStatisticsParser rtsp = new RuntimeStatisticsParser(rs.getString(1));
            assertTrue(rtsp.usedTableScan());
            assertFalse(rtsp.usedDistinctScan());
        }
        rs.close();
        
        rs = st.executeQuery(
            " select * from tt1 left outer join tt2 on tt1.c1 = "
            + "tt2.c2 where tt2.c1 + 1= tt2.c2");
        
        expColNames = new String [] {"C1", "C2", "C3", "C1", "C2", "C3"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"2", "3", "4", "1", "2", "3"},
            {"3", "4", "5", "2", "3", "4"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS()");

        rs.next();
        
        if(usingEmbedded()){
            RuntimeStatisticsParser rtsp = new RuntimeStatisticsParser(rs.getString(1));
            assertTrue(rtsp.usedTableScan());
            assertFalse(rtsp.usedDistinctScan());
        }
        rs.close();

        rs = st.executeQuery(
            " select * from tt1 left outer join tt2 on tt1.c1 = "
            + "tt2.c2 where tt2.c1 + 1= 3");
        
        expColNames = new String [] {"C1", "C2", "C3", "C1", "C2", "C3"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"3", "4", "5", "2", "3", "4"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS()");
        
        rs.next();

        if(usingEmbedded()){
            RuntimeStatisticsParser rtsp = new RuntimeStatisticsParser(rs.getString(1));
            assertTrue(rtsp.usedTableScan());
            assertFalse(rtsp.usedDistinctScan());
        }
        rs.close();
        
        rs = st.executeQuery(
            " select * from tt2 right outer join tt1 on tt1.c1 = "
            + "tt2.c2 where tt2.c1 + 1= 3");
        
        expColNames = new String [] {"C1", "C2", "C3", "C1", "C2", "C3"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"2", "3", "4", "3", "4", "5"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS()");
        
        rs.next();

        if(usingEmbedded()){
            RuntimeStatisticsParser rtsp = new RuntimeStatisticsParser(rs.getString(1));
            assertTrue(rtsp.usedTableScan());
            assertFalse(rtsp.usedDistinctScan());
        }
        rs.close();
        
        rs = st.executeQuery(
            " select * from tt1 left outer join tt2 on tt1.c1 = "
            + "tt2.c2 left outer join tt3 on tt2.c2 = tt3.c3 where "
            + "tt3.c3 = 3");
        
        expColNames = new String [] {"C1", "C2", "C3", "C1", "C2", "C3", "C1", "C2", "C3"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"3", "4", "5", "2", "3", "4", "1", "2", "3"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS()");
        
        rs.next();

        if(usingEmbedded()){
            RuntimeStatisticsParser rtsp = new RuntimeStatisticsParser(rs.getString(1));
            assertTrue(rtsp.usedTableScan());
            assertFalse(rtsp.usedDistinctScan());
        }
        rs.close();
        
        rs = st.executeQuery(
            " select * from tt1 left outer join tt2 on tt1.c1 = "
            + "tt2.c2 left outer join tt3 on tt2.c2 = tt3.c3 where "
            + "tt2.c2 = 3");
        
        expColNames = new String [] {"C1", "C2", "C3", "C1", "C2", "C3", "C1", "C2", "C3"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"3", "4", "5", "2", "3", "4", "1", "2", "3"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS()");
        
        rs.next();

        if(usingEmbedded()){
            RuntimeStatisticsParser rtsp = new RuntimeStatisticsParser(rs.getString(1));
            assertTrue(rtsp.usedTableScan());
            assertFalse(rtsp.usedDistinctScan());
        }
        rs.close();
        
        // make sure predicates are null tolerant
        
        rs = st.executeQuery(
            "select * from tt1 left outer join tt2 on tt1.c1 = tt2.c2 "
            + "where char(tt2.c2) is null");
        
        expColNames = new String [] {"C1", "C2", "C3", "C1", "C2", "C3"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1", "2", "3", null, null, null}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        // where java.lang.Integer::toString(tt2.c2) = '2';
        
        rs = st.executeQuery(
            "values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS()");
        
        rs.next();

        if(usingEmbedded()){
            RuntimeStatisticsParser rtsp = new RuntimeStatisticsParser(rs.getString(1));
            assertTrue(rtsp.usedTableScan());
            assertFalse(rtsp.usedDistinctScan());
        }
        rs.close();     
    }

    public void testdDerby2924() throws Exception
    {
        // bug 2924, cross join under an outer join

        Statement st = createStatement();
        ResultSet rs = null;        
        String [][] expRS;
        String [] expColNames;

        createTestObjects(st);

        st.executeUpdate(
            "CREATE TABLE inventory(itemno INT NOT NULL PRIMARY "
            + "KEY, capacity INT)");
        
        st.executeUpdate("INSERT INTO inventory VALUES (1, 4)");
        st.executeUpdate("INSERT INTO inventory VALUES (2, 2)");
        st.executeUpdate("INSERT INTO inventory VALUES (3, 2)");
        
        st.executeUpdate(
            " CREATE TABLE timeslots (slotno INT NOT NULL PRIMARY KEY)");
        
        st.executeUpdate("INSERT INTO timeslots VALUES(1)");
        st.executeUpdate("INSERT INTO timeslots VALUES(2)");
        
        st.executeUpdate(
            " create table reservations(slotno INT CONSTRAINT "
            + "timeslots_fk REFERENCES timeslots, "
            + "itemno INT CONSTRAINT inventory_fk REFERENCES inventory, "
            + "name VARCHAR(100), resdate DATE)");
        
        st.executeUpdate(
            " INSERT INTO reservations VALUES(1, 1, 'Joe', '2000-04-14')");
        
        st.executeUpdate(
            " INSERT INTO reservations VALUES(1, 1, 'Fred', '2000-04-13')");
        
        // This query used to cause a null pointer exception
        
        rs = st.executeQuery(
            "select name, resdate "
            + "from (reservations left outer join (inventory join "
            + "timeslots on inventory.itemno = timeslots.slotno)"
            + "on inventory.itemno = reservations.itemno and "
            + "timeslots.slotno = reservations.slotno)"
            + "where resdate = '2000-04-14'");
        
        expColNames = new String [] {"NAME", "RESDATE"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"Joe", "2000-04-14"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
    }

    public void testdDerby2923() throws Exception
    {
        // bug 2923, cross join under an outer join

        Statement st = createStatement();
        ResultSet rs = null; 
        String [][] expRS;
        String [] expColNames;

        createTestObjects(st);

        st.executeUpdate(
            "create table inventory(itemno INT NOT NULL PRIMARY "
            + "KEY, capacity INT)");
        
        st.executeUpdate("INSERT into inventory values (1, 4)");
        st.executeUpdate("INSERT into inventory values (2, 2)");
        st.executeUpdate("INSERT into inventory values (3, 2)");
        
        st.executeUpdate(
            " CREATE TABLE timeslots (slotno INT NOT NULL PRIMARY KEY)");
        
        st.executeUpdate("INSERT INTO timeslots VALUES(1)");
        st.executeUpdate("INSERT INTO timeslots VALUES(2)");
        
        st.executeUpdate(
            " create table reservations(slotno INT CONSTRAINT "
            + "timeslots_fk REFERENCES timeslots,"
            + "itemno INT CONSTRAINT inventory_fk REFERENCES inventory,"
            + "name VARCHAR(100))");
        
        st.executeUpdate("INSERT INTO reservations VALUES(1, 1, 'Joe')");
        st.executeUpdate("INSERT INTO reservations VALUES(2, 2, 'Fred')");
        
        // This query used to get incorrect results when name is 
        // null was the 2nd predicate due to a bug in OJ->IJ xform 
        // code.
        
        rs = st.executeQuery(
            "select timeslots.slotno, inventory.itemno, capacity, name"
            + " from inventory left outer join timeslots"
            + " on inventory.capacity = timeslots.slotno"
            + " left outer join reservations"
            + " on timeslots.slotno = reservations.slotno"
            + " where capacity > 3 and name is null");
        
        expColNames = new String [] {"SLOTNO", "ITEMNO", "CAPACITY", "NAME"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {null, "1", "4", null}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select timeslots.slotno, inventory.itemno, capacity, name"
            + " from inventory left outer join timeslots"
            + " on inventory.capacity = timeslots.slotno"
            + " left outer join reservations"
            + " on timeslots.slotno = reservations.slotno"
            + " where name is null and capacity > 3");
        
        expColNames = new String [] {"SLOTNO", "ITEMNO", "CAPACITY", "NAME"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {null, "1", "4", null}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
    }

    public void testdDerby2930() throws Exception
    {

        // bug 2930, cross join under outer join

        Statement st = createStatement();
        ResultSet rs = null;
        SQLWarning sqlWarn = null;      
        String [][] expRS;
        String [] expColNames;

        createTestObjects(st);

        st.executeUpdate(
            "CREATE TABLE properties ("
            + "	name VARCHAR(50),"
            + "	value VARCHAR(200))");
        
        st.executeUpdate(
            " INSERT INTO properties VALUES ('businessName', "
            + "'Cloud 9 Cafe')");
        
        st.executeUpdate(
            " INSERT INTO properties VALUES "
            + "('lastReservationDate', '2001-12-31')");
        
        st.executeUpdate(
            " CREATE TABLE inventory ("
            + "	itemno INT NOT NULL PRIMARY KEY,"
            + "	capacity INT"
            + ")");
        
        st.executeUpdate("INSERT INTO inventory VALUES (1, 2)");
        st.executeUpdate("INSERT INTO inventory VALUES (2, 2)");
        st.executeUpdate("INSERT INTO inventory VALUES (3, 2)");
        st.executeUpdate("INSERT INTO inventory VALUES (4, 2)");
        st.executeUpdate("INSERT INTO inventory VALUES (5, 2)");
        st.executeUpdate("INSERT INTO inventory VALUES (6, 4)");
        st.executeUpdate("INSERT INTO inventory VALUES (7, 4)");
        st.executeUpdate("INSERT INTO inventory VALUES (8, 4)");
        st.executeUpdate("INSERT INTO inventory VALUES (9, 4)");
        st.executeUpdate("INSERT INTO inventory VALUES (10, 4)");
        
        st.executeUpdate(
            " CREATE TABLE timeslots (slot TIME NOT NULL PRIMARY KEY)");
        
        st.executeUpdate("INSERT INTO timeslots VALUES('17:00:00')");
        st.executeUpdate("INSERT INTO timeslots VALUES('17:30:00')");
        st.executeUpdate("INSERT INTO timeslots VALUES('18:00:00')");
        st.executeUpdate("INSERT INTO timeslots VALUES('18:30:00')");
        st.executeUpdate("INSERT INTO timeslots VALUES('19:00:00')");
        st.executeUpdate("INSERT INTO timeslots VALUES('19:30:00')");
        st.executeUpdate("INSERT INTO timeslots VALUES('20:00:00')");
        st.executeUpdate("INSERT INTO timeslots VALUES('20:30:00')");
        st.executeUpdate("INSERT INTO timeslots VALUES('21:00:00')");
        st.executeUpdate("INSERT INTO timeslots VALUES('21:30:00')");
        st.executeUpdate("INSERT INTO timeslots VALUES('22:00:00')");
        
        st.executeUpdate(
            " CREATE TABLE reservations ("
            + "	itemno INT CONSTRAINT inventory_fk REFERENCES inventory,"
            + "	slot TIME CONSTRAINT timeslots_fk REFERENCES timeslots,"
            + "	resdate DATE NOT NULL,"
            + "	name VARCHAR(100) NOT NULL,"
            + "	quantity INT,"
            + "	CONSTRAINT reservations_u UNIQUE(name, resdate))");
        
        st.executeUpdate(
            " INSERT INTO reservations VALUES(6, '17:00:00', "
            + "'2000-07-13', 'Williams', 4)");
        
        st.executeUpdate(
            " INSERT INTO reservations VALUES(7, '17:00:00', "
            + "'2000-07-13', 'Johnson',  4)");
        
        st.executeUpdate(
            " INSERT INTO reservations VALUES(8, '17:00:00', "
            + "'2000-07-13', 'Allen',    3)");
        
        st.executeUpdate(
            " INSERT INTO reservations VALUES(9, '17:00:00', "
            + "'2000-07-13', 'Dexmier',  4)");
        
        st.executeUpdate(
            " INSERT INTO reservations VALUES(1, '17:30:00', "
            + "'2000-07-13', 'Gates', 	 2)");
        
        st.executeUpdate(
            " INSERT INTO reservations VALUES(2, '17:30:00', "
            + "'2000-07-13', 'McNealy',  2)");
        
        st.executeUpdate(
            " INSERT INTO reservations VALUES(3, '17:30:00', "
            + "'2000-07-13', 'Hoffman',  1)");
        
        st.executeUpdate(
            " INSERT INTO reservations VALUES(4, '17:30:00', "
            + "'2000-07-13', 'Sippl',    2)");
        
        st.executeUpdate(
            " INSERT INTO reservations VALUES(6, '17:30:00', "
            + "'2000-07-13', 'Yang',     4)");
        
        st.executeUpdate(
            " INSERT INTO reservations VALUES(7, '17:30:00', "
            + "'2000-07-13', 'Meyers',   4)");
        
        rs = st.executeQuery(
            " select max(name), max(resdate) from inventory join "
            + "timeslots on inventory.capacity is not null "
            + "left outer join reservations on inventory.itemno = "
            + "reservations.itemno and reservations.slot = timeslots.slot");

        expColNames = new String [] {"1", "2"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"Yang", "2000-07-13"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);

        rs = st.executeQuery(
           " select max(name), max(resdate) from inventory join "
           + "timeslots on inventory.capacity is not null "
           + "left outer join reservations on inventory.itemno = "
           + "reservations.itemno and reservations.slot = timeslots.slot");

        rs.next();
        // This causes the warning to be generated now.
        if (usingEmbedded())
        {
            sqlWarn = rs.getWarnings();
            assertNotNull("Expected warning but found none", sqlWarn);
            //Warning 01003:Null values were eliminated from the
            //argument of a column function
            assertSQLState("01003", sqlWarn);
        }
        assertEquals("Yang", rs.getString(1));
        assertEquals("2000-07-13", rs.getString(2));

    }

    public void testdDerby2931() throws Exception
    {

        // bug 2931, cross join under outer join

        Statement st = createStatement();
        ResultSet rs = null;       
        String [][] expRS;
        String [] expColNames;

        createTestObjects(st);

        st.executeUpdate(
            "CREATE TABLE properties ("
            + "	name VARCHAR(50),"
            + "	value VARCHAR(200))");
        
        st.executeUpdate(
            " INSERT INTO properties VALUES ('businessName', "
            + "'Cloud 9 Cafe')");
        
        st.executeUpdate(
            " INSERT INTO properties VALUES "
            + "('lastReservationDate', '2001-12-31')");
        
        st.executeUpdate(
            " CREATE TABLE inventory ("
            + "	itemno INT NOT NULL PRIMARY KEY,"
            + "	capacity INT"
            + ")");
        
        st.executeUpdate("INSERT INTO inventory VALUES (1, 2)");
        st.executeUpdate("INSERT INTO inventory VALUES (2, 2)");
        st.executeUpdate("INSERT INTO inventory VALUES (3, 2)");
        st.executeUpdate("INSERT INTO inventory VALUES (4, 2)");
        st.executeUpdate("INSERT INTO inventory VALUES (5, 2)");
        st.executeUpdate("INSERT INTO inventory VALUES (6, 4)");
        st.executeUpdate("INSERT INTO inventory VALUES (7, 4)");
        st.executeUpdate("INSERT INTO inventory VALUES (8, 4)");
        st.executeUpdate("INSERT INTO inventory VALUES (9, 4)");
        st.executeUpdate("INSERT INTO inventory VALUES (10, 4)");
        
        st.executeUpdate(
            " CREATE TABLE timeslots (slot TIME NOT NULL PRIMARY KEY)");
        
        st.executeUpdate("INSERT INTO timeslots VALUES('17:00:00')");
        st.executeUpdate("INSERT INTO timeslots VALUES('17:30:00')");
        st.executeUpdate("INSERT INTO timeslots VALUES('18:00:00')");
        st.executeUpdate("INSERT INTO timeslots VALUES('18:30:00')");
        st.executeUpdate("INSERT INTO timeslots VALUES('19:00:00')");
        st.executeUpdate("INSERT INTO timeslots VALUES('19:30:00')");
        st.executeUpdate("INSERT INTO timeslots VALUES('20:00:00')");
        st.executeUpdate("INSERT INTO timeslots VALUES('20:30:00')");
        st.executeUpdate("INSERT INTO timeslots VALUES('21:00:00')");
        st.executeUpdate("INSERT INTO timeslots VALUES('21:30:00')");
        st.executeUpdate("INSERT INTO timeslots VALUES('22:00:00')");
        
        st.executeUpdate(
            " CREATE TABLE reservations ("
            + "	itemno INT CONSTRAINT inventory_fk REFERENCES inventory,"
            + "	slot TIME CONSTRAINT timeslots_fk REFERENCES timeslots,"
            + "	resdate DATE NOT NULL,"
            + "	name VARCHAR(100) NOT NULL,"
            + "	quantity INT,"
            + "	CONSTRAINT reservations_u UNIQUE(name, resdate))");
        
        st.executeUpdate(
            " INSERT INTO reservations VALUES(6, '17:00:00', "
            + "'2000-07-13', 'Williams', 4)");
        
        st.executeUpdate(
            " INSERT INTO reservations VALUES(7, '17:00:00', "
            + "'2000-07-13', 'Johnson',  4)");
        
        st.executeUpdate(
            " INSERT INTO reservations VALUES(8, '17:00:00', "
            + "'2000-07-13', 'Allen',    3)");
        
        st.executeUpdate(
            " INSERT INTO reservations VALUES(9, '17:00:00', "
            + "'2000-07-13', 'Dexmier',  4)");
        
        st.executeUpdate(
            " INSERT INTO reservations VALUES(1, '17:30:00', "
            + "'2000-07-13', 'Gates', 	 2)");
        
        st.executeUpdate(
            " INSERT INTO reservations VALUES(2, '17:30:00', "
            + "'2000-07-13', 'McNealy',  2)");
        
        st.executeUpdate(
            " INSERT INTO reservations VALUES(3, '17:30:00', "
            + "'2000-07-13', 'Hoffman',  1)");
        
        st.executeUpdate(
            " INSERT INTO reservations VALUES(4, '17:30:00', "
            + "'2000-07-13', 'Sippl',    2)");
        
        st.executeUpdate(
            " INSERT INTO reservations VALUES(6, '17:30:00', "
            + "'2000-07-13', 'Yang',     4)");
        
        st.executeUpdate(
            " INSERT INTO reservations VALUES(7, '17:30:00', "
            + "'2000-07-13', 'Meyers',   4)");
        
        // this query should return values from the 'slot' column 
        // (type date) but it seems to be returning integers!
        
        rs = st.executeQuery(
            "select max(timeslots.slot) from inventory inner "
            + "join timeslots on inventory.capacity is not null "
            + "left outer join reservations on inventory.capacity "
            + "= reservations.itemno and reservations.slot = timeslots.slot");
        
        expColNames = new String [] {"1"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"22:00:00"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
    }

    public void testdDerby2897() throws Exception
    {
        // bug 2897 Push join predicates from where clause to right

        Statement st = createStatement();
        ResultSet rs = null;      
        String [][] expRS;
        String [] expColNames;

        createTestObjects(st);

        st.executeUpdate("CALL SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1)");

        rs = st.executeQuery(
            "select * from t1 inner join t2 on 1=1 left outer "
            + "join t3 on t1.c1 = t3.c1 "
            + "where t1.c1 = t2.c1");
        
        expColNames = new String [] {"C1", "C1", "C1"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1", "1", null},
            {"3", "3", "3"},
            {"3", "3", "3"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);

        rs = st.executeQuery(
            " values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS()");

        rs.next();

        if(usingEmbedded()){
            RuntimeStatisticsParser rtsp = new RuntimeStatisticsParser(rs.getString(1));
            assertTrue(rtsp.usedTableScan());
            assertFalse(rtsp.usedDistinctScan());
        }
        rs.close();
    }

    public void testdDerby5659() throws SQLException
    {
        // Test fix for bug 5659

        ResultSet rs = null;
        Statement st = createStatement();
        String [][] expRS;
        String [] expColNames;
             
        st.executeUpdate(
            "create table xxx (a int not null)");
        
        st.executeUpdate(
            " create table yyy (a int not null)");
        
        st.executeUpdate(
            " insert into xxx values (1)");
        
        rs = st.executeQuery(
            " select * from xxx left join yyy on (xxx.a=yyy.a)");
        
        expColNames = new String [] {"A", "A"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1", null}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        assertStatementError("23502", st,
            " insert into xxx values (null)");
        
        rs = st.executeQuery(
            " select * from xxx");
        
        expColNames = new String [] {"A"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
    }

    public void testdDerby5658() throws Exception
    {
        // Defect 5658. Disable querries with ambiguous references.

        Statement st = createStatement();
        ResultSet rs = null;    
        String [][] expRS;
        String [] expColNames;

        createTestObjects(st);

        st.executeUpdate(
            "create table ttab1 (a int, b int)");

        st.executeUpdate(
            " insert into ttab1 values (1,1),(2,2)");

        st.executeUpdate(
            " create table ttab2 (c int, d int)");

        st.executeUpdate(
            " insert into ttab2 values (1,1),(2,2)");

        // DERBY-4380: These statements used to raise an error
        // because more than one object table includes column "b".
        // But the scope of the ON clauses makes it clear which
        // table they belong to in each case, so they should not fail.

        rs = st.executeQuery(
            "select cor1.*, cor2.* from ttab1 cor1 left outer "
            + "join ttab2 on (b = d),"
            + "		ttab1 left outer join ttab2 cor2 on (b = d)");

        expColNames = new String [] {"A", "B", "C", "D"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1", "1", "1", "1"},
            {"1", "1", "2", "2"},
            {"2", "2", "1", "1"},
            {"2", "2", "2", "2"}
        };

        JDBC.assertFullResultSet(rs, expRS, true);

        rs = st.executeQuery(
            " select cor1.*, cor2.* from ttab1 cor1 left outer "
            + "join ttab2 on (b = d),"
            + "		ttab1 left outer join ttab2 cor2 on (b = cor2.d)");

        expColNames = new String [] {"A", "B", "C", "D"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1", "1", "1", "1"},
            {"1", "1", "2", "2"},
            {"2", "2", "1", "1"},
            {"2", "2", "2", "2"}
        };

        JDBC.assertFullResultSet(rs, expRS, true);

        // This should pass

        rs = st.executeQuery(
            "select cor1.*, cor2.* from ttab1 left outer join "
            + "ttab2 on (b = d), "
            + "		ttab1 cor1 left outer join ttab2 cor2 on (cor1.b = cor2.d)");

        expColNames = new String [] {"A", "B", "C", "D"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1", "1", "1", "1"},
            {"2", "2", "2", "2"},
            {"1", "1", "1", "1"},
            {"2", "2", "2", "2"}
        };

        JDBC.assertFullResultSet(rs, expRS, true);

        // These should fail too

        assertStatementError("42X03", st,
            "select * from ttab1, ttab1 left outer join ttab2 on (a=c)");

        assertStatementError("42X04", st,
            " select * from ttab1 cor1, ttab1 left outer join "
            + "ttab2 on (cor1.a=c)");

        // This should pass

        rs = st.executeQuery(
            "select * from ttab1, ttab1 cor1 left outer join "
            + "ttab2 on (cor1.a=c)");

        expColNames = new String [] {"A", "B", "A", "B", "C", "D"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1", "1", "1", "1", "1", "1"},
            {"2", "2", "1", "1", "1", "1"},
            {"1", "1", "2", "2", "2", "2"},
            {"2", "2", "2", "2", "2", "2"}
        };

        JDBC.assertFullResultSet(rs, expRS, true);
    }

    public void testdDerby5164() throws Exception
    {
        // Test 5164

        Statement st = createStatement();
        createTestObjects(st);
        ResultSet rs=null;
        String [][] expRS;
        String [] expColNames;

        createTestObjects(st);
        
        st.executeUpdate(
            "CREATE TABLE \"APP\".\"GOVT_AGCY\" (\"GVA_ID\" "
            + "NUMERIC(20,0) NOT NULL, \"GVA_ORL_ID\" "
            + "NUMERIC(20,0) NOT NULL, \"GVA_GAC_ID\" NUMERIC(20,0))");
        
        st.executeUpdate(
            " CREATE TABLE \"APP\".\"GEO_STRC_ELMT\" (\"GSE_ID\" "
            + "NUMERIC(20,0) NOT NULL, \"GSE_GSET_ID\" "
            + "NUMERIC(20,0) NOT NULL, \"GSE_GA_ID_PRNT\" "
            + "NUMERIC(20,0) NOT NULL, \"GSE_GA_ID_CHLD\" "
            + "NUMERIC(20,0) NOT NULL)");
        
        st.executeUpdate(
            " CREATE TABLE \"APP\".\"GEO_AREA\" (\"GA_ID\" "
            + "NUMERIC(20,0) NOT NULL, \"GA_GAT_ID\" NUMERIC(20,0) "
            + "NOT NULL, \"GA_NM\" VARCHAR(30) NOT NULL, "
            + "\"GA_ABRV_NM\" VARCHAR(5))");

        st.executeUpdate("CREATE TABLE \"APP\".\"REG\" "
                +"(\"REG_ID\" NUMERIC(20,0) NOT NULL, \"REG_NM\" "
                +"VARCHAR(60) NOT NULL, \"REG_DESC\" VARCHAR(240), "
                +"\"REG_ABRV_NM\" VARCHAR(15), \"REG_CD\" "
                +"NUMERIC(8,0) NOT NULL, \"REG_STRT_DT\" TIMESTAMP NOT NULL, "
                +"\"REG_END_DT\" TIMESTAMP NOT NULL DEFAULT "
                + "'"+Timestamp.valueOf("2009-12-05 01:29:59")+"',"
                +"\"REG_EMPR_LIAB_IND\" CHAR(1) NOT NULL DEFAULT 'N', "
                +"\"REG_PAYR_TAX_SURG_CRTF_IND\" CHAR(1) NOT NULL DEFAULT 'N', "
                +"\"REG_PYT_ID\" NUMERIC(20,0), \"REG_GA_ID\" NUMERIC(20,0) NOT NULL, "
                +"\"REG_GVA_ID\" NUMERIC(20,0) NOT NULL, \"REG_REGT_ID\" NUMERIC(20,0) NOT NULL, "
                +"\"REG_PRNT_ID\" NUMERIC(20,0))");
        
        rs=st.executeQuery("SELECT 1 FROM reg "
            +"JOIN geo_area jrsd ON (jrsd.ga_id = reg.reg_ga_id) "
            +"LEFT OUTER JOIN geo_strc_elmt gse ON (gse.gse_ga_id_chld =reg.reg_ga_id) "
            +"LEFT OUTER JOIN geo_area prnt ON (prnt.ga_id =reg.reg_ga_id) "
            +"JOIN govt_agcy gva ON (reg.reg_gva_id = gva.gva_id)");

       expColNames=new String[]{"1"};
       JDBC.assertColumnNames(rs, expColNames);

       expRS=new String[][]{};
       JDBC.assertFullResultSet(rs, expRS,true);
    }


    /**
     * Test that left outer join reordering works as expected.  This fixture is
     * the first repro mentioned for this issue, DERBY-4471. It checks for
     * correct results, and should fail prior to applying issue patch due to
     * erroneous reordering.
     * <p/>
     * The results asserted in these tests for DERBY-4471 are the same as Derby
     * gives without the reordering enabled and the same as Postgres yields.
     * <p/>
     * Note that the patch also opens up for correct reorderings of some
     * queries in the seen in the original lojreorder.sql test (converted to
     * JUnit as LojReorderTest in this patch) that did <em>not</em> happen
     * earlier, in addition to denying the wrong ones documented in DERBY-4471
     * and tested below. These new reorderings are tested, cf the comments in
     * LojReorderTest. Look for the string 4471.

     */
    public void testDerby_4471a() throws SQLException {
//IC see: https://issues.apache.org/jira/browse/DERBY-4471
        setAutoCommit(false);
        Statement s = createStatement();

        s.executeUpdate("create table r(c1 char(1))");
        s.executeUpdate("create table s(c1 char(1), c2 char(1))");
        s.executeUpdate("create table t(c1 char(1))");

        s.executeUpdate("insert into r values 'a'");
        s.executeUpdate("insert into s values ('b', default)");
        s.executeUpdate("insert into t values ('c')");

        ResultSet rs = s.executeQuery(
            "select * from r left outer join (s left outer join t " +
            "                                 on s.c2=t.c1 or s.c2 is null)" +
            "                on r.c1=s.c1");
        JDBC.assertFullResultSet(rs, new String[][]{{"a", null, null, null}});
    }

    /**
     * Test that left outer join reordering works as expected.  The schema here
     * is taken from the another issue which also saw this error;
     * DERBY-4712/DERBY-4736. This fixture checks for correct results. The
     * first query should fail prior to applying issue patch due to erroneous
     * reordering, the second is OK to reorder (and would be reordered also
     * prior to the patch).
     */
    public void testDerby_4471b() throws SQLException {
        setAutoCommit(false);
        Statement s = createStatement();

        s.executeUpdate("create table t0(x int)");
        s.executeUpdate("create table t1(x int)");
        s.executeUpdate("create table t2(x int)");
        s.executeUpdate("create table t3(x int)");
        s.executeUpdate("create table t4(x int)");
        s.executeUpdate("insert into t4 values(0)");
        s.executeUpdate("insert into t4 values(1)");
        s.executeUpdate("insert into t4 values(2)");
        s.executeUpdate("insert into t4 values(3)");
        s.executeUpdate("create table t5(x int)");
        s.executeUpdate("insert into t5 values(0)");
        s.executeUpdate("insert into t5 values(1)");
        s.executeUpdate("insert into t5 values(2)");
        s.executeUpdate("insert into t5 values(3)");
        s.executeUpdate("insert into t5 values(4)");
        s.executeUpdate("create table t6(x int)");
        s.executeUpdate("insert into t6 values(0)");
        s.executeUpdate("insert into t6 values(1)");
        s.executeUpdate("insert into t6 values(2)");
        s.executeUpdate("insert into t6 values(3)");
        s.executeUpdate("insert into t6 values(4)");
        s.executeUpdate("insert into t6 values(5)");
        s.executeUpdate("create table t7(x int)");
        s.executeUpdate("insert into t7 values(0)");
        s.executeUpdate("insert into t7 values(1)");
        s.executeUpdate("insert into t7 values(2)");
        s.executeUpdate("insert into t7 values(3)");
        s.executeUpdate("insert into t7 values(4)");
        s.executeUpdate("insert into t7 values(5)");
        s.executeUpdate("insert into t7 values(6)");
        s.executeUpdate("create table t8(x int)");
        s.executeUpdate("insert into t8 values(0)");
        s.executeUpdate("insert into t8 values(1)");
        s.executeUpdate("insert into t8 values(2)");
        s.executeUpdate("insert into t8 values(3)");
        s.executeUpdate("insert into t8 values(4)");
        s.executeUpdate("insert into t8 values(5)");
        s.executeUpdate("insert into t8 values(6)");
        s.executeUpdate("insert into t8 values(7)");
        s.executeUpdate("create table t9(x int)");
        s.executeUpdate("insert into t9 values(0)");
        s.executeUpdate("insert into t9 values(1)");
        s.executeUpdate("insert into t9 values(2)");
        s.executeUpdate("insert into t9 values(3)");
        s.executeUpdate("insert into t9 values(4)");
        s.executeUpdate("insert into t9 values(5)");
        s.executeUpdate("insert into t9 values(6)");
        s.executeUpdate("insert into t9 values(7)");
        s.executeUpdate("insert into t9 values(8)");
        s.executeUpdate("insert into t0 values(1)");
        s.executeUpdate("insert into t1 values(2)");
        s.executeUpdate("insert into t0 values(3)");
        s.executeUpdate("insert into t1 values(3)");
        s.executeUpdate("insert into t2 values(4)");
        s.executeUpdate("insert into t0 values(5)");
        s.executeUpdate("insert into t2 values(5)");
        s.executeUpdate("insert into t1 values(6)");
        s.executeUpdate("insert into t2 values(6)");
        s.executeUpdate("insert into t0 values(7)");
        s.executeUpdate("insert into t1 values(7)");
        s.executeUpdate("insert into t2 values(7)");
        s.executeUpdate("insert into t3 values(8)");
        s.executeUpdate("insert into t0 values(9)");
        s.executeUpdate("insert into t3 values(9)");
        s.executeUpdate("insert into t1 values(10)");
        s.executeUpdate("insert into t3 values(10)");
        s.executeUpdate("insert into t0 values(11)");
        s.executeUpdate("insert into t1 values(11)");
        s.executeUpdate("insert into t3 values(11)");
        s.executeUpdate("insert into t2 values(12)");
        s.executeUpdate("insert into t3 values(12)");
        s.executeUpdate("insert into t0 values(13)");
        s.executeUpdate("insert into t2 values(13)");
        s.executeUpdate("insert into t3 values(13)");
        s.executeUpdate("insert into t1 values(14)");
        s.executeUpdate("insert into t2 values(14)");
        s.executeUpdate("insert into t3 values(14)");
        s.executeUpdate("insert into t0 values(15)");
        s.executeUpdate("insert into t1 values(15)");
        s.executeUpdate("insert into t2 values(15)");
        s.executeUpdate("insert into t3 values(15)");

        // The theory exposed in Galindo-Legaria, C. & Rosenthal, A.:
        // "Outerjoin simplification and reordering for query optimization",
        // ACM Transactions on Database Systems, Vol 22, No 1, March 1997 uses
        // two assumption for its general case which involves full outer joins
        // as well: no duplicate rows and no rows consisting of only nulls. We
        // cannot make that assumption, this being SQL, but for our restricted
        // OJ rewrites, this should work ok, so we throw in both into the test
        // mix:

        // Make duplicates
        s.executeUpdate("insert into t2 select * from t2");
        s.executeUpdate("insert into t3 select * from t3");
        s.executeUpdate("insert into t4 select * from t4");

        // Insert full NULL tuples
        s.executeUpdate("insert into t2 values cast(null as int)");
        s.executeUpdate("insert into t3 values cast(null as int)");
        s.executeUpdate("insert into t4 values cast(null as int)");

        // This query was wrong prior to DERBY-4471: 1=1 is not allowed, since
        // the inner join predicate does not reference T3 and T4 as required.
        ResultSet rs = s.executeQuery(
            "SELECT * FROM (T2 LEFT JOIN (T3 left outer JOIN T4 " +
            "                                    ON 1=1) " +
            "                      ON T2.X = T3.X)");

        JDBC.assertUnorderedResultSet(
            rs,
            new String[][] {
                {"4", null, null},
                {"5", null, null},
                {"6", null, null},
                {"7", null, null},
                {"12", "12", "0"},
                {"12", "12", "1"},
                {"12", "12", "2"},
                {"12", "12", "3"},
                {"12", "12", "0"},
                {"12", "12", "1"},
                {"12", "12", "2"},
                {"12", "12", "3"},
                {"12", "12", null},
                {"12", "12", "0"},
                {"12", "12", "1"},
                {"12", "12", "2"},
                {"12", "12", "3"},
                {"12", "12", "0"},
                {"12", "12", "1"},
                {"12", "12", "2"},
                {"12", "12", "3"},
                {"12", "12", null},
                {"13", "13", "0"},
                {"13", "13", "1"},
                {"13", "13", "2"},
                {"13", "13", "3"},
                {"13", "13", "0"},
                {"13", "13", "1"},
                {"13", "13", "2"},
                {"13", "13", "3"},
                {"13", "13", null},
                {"13", "13", "0"},
                {"13", "13", "1"},
                {"13", "13", "2"},
                {"13", "13", "3"},
                {"13", "13", "0"},
                {"13", "13", "1"},
                {"13", "13", "2"},
                {"13", "13", "3"},
                {"13", "13", null},
                {"14", "14", "0"},
                {"14", "14", "1"},
                {"14", "14", "2"},
                {"14", "14", "3"},
                {"14", "14", "0"},
                {"14", "14", "1"},
                {"14", "14", "2"},
                {"14", "14", "3"},
                {"14", "14", null},
                {"14", "14", "0"},
                {"14", "14", "1"},
                {"14", "14", "2"},
                {"14", "14", "3"},
                {"14", "14", "0"},
                {"14", "14", "1"},
                {"14", "14", "2"},
                {"14", "14", "3"},
                {"14", "14", null},
                {"15", "15", "0"},
                {"15", "15", "1"},
                {"15", "15", "2"},
                {"15", "15", "3"},
                {"15", "15", "0"},
                {"15", "15", "1"},
                {"15", "15", "2"},
                {"15", "15", "3"},
                {"15", "15", null},
                {"15", "15", "0"},
                {"15", "15", "1"},
                {"15", "15", "2"},
                {"15", "15", "3"},
                {"15", "15", "0"},
                {"15", "15", "1"},
                {"15", "15", "2"},
                {"15", "15", "3"},
                {"15", "15", null},
                {"4", null, null},
                {"5", null, null},
                {"6", null, null},
                {"7", null, null},
                {"12", "12", "0"},
                {"12", "12", "1"},
                {"12", "12", "2"},
                {"12", "12", "3"},
                {"12", "12", "0"},
                {"12", "12", "1"},
                {"12", "12", "2"},
                {"12", "12", "3"},
                {"12", "12", null},
                {"12", "12", "0"},
                {"12", "12", "1"},
                {"12", "12", "2"},
                {"12", "12", "3"},
                {"12", "12", "0"},
                {"12", "12", "1"},
                {"12", "12", "2"},
                {"12", "12", "3"},
                {"12", "12", null},
                {"13", "13", "0"},
                {"13", "13", "1"},
                {"13", "13", "2"},
                {"13", "13", "3"},
                {"13", "13", "0"},
                {"13", "13", "1"},
                {"13", "13", "2"},
                {"13", "13", "3"},
                {"13", "13", null},
                {"13", "13", "0"},
                {"13", "13", "1"},
                {"13", "13", "2"},
                {"13", "13", "3"},
                {"13", "13", "0"},
                {"13", "13", "1"},
                {"13", "13", "2"},
                {"13", "13", "3"},
                {"13", "13", null},
                {"14", "14", "0"},
                {"14", "14", "1"},
                {"14", "14", "2"},
                {"14", "14", "3"},
                {"14", "14", "0"},
                {"14", "14", "1"},
                {"14", "14", "2"},
                {"14", "14", "3"},
                {"14", "14", null},
                {"14", "14", "0"},
                {"14", "14", "1"},
                {"14", "14", "2"},
                {"14", "14", "3"},
                {"14", "14", "0"},
                {"14", "14", "1"},
                {"14", "14", "2"},
                {"14", "14", "3"},
                {"14", "14", null},
                {"15", "15", "0"},
                {"15", "15", "1"},
                {"15", "15", "2"},
                {"15", "15", "3"},
                {"15", "15", "0"},
                {"15", "15", "1"},
                {"15", "15", "2"},
                {"15", "15", "3"},
                {"15", "15", null},
                {"15", "15", "0"},
                {"15", "15", "1"},
                {"15", "15", "2"},
                {"15", "15", "3"},
                {"15", "15", "0"},
                {"15", "15", "1"},
                {"15", "15", "2"},
                {"15", "15", "3"},
                {"15", "15", null},
                {null, null, null}});

        s.execute("CALL SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1)");

        // This one *can* be reordered
        rs = s.executeQuery(
            "SELECT * FROM (T2 LEFT JOIN (T3 left outer JOIN T4 " +
            "                                    ON t3.x=t4.x) " +
            "                      ON T2.X = T3.X)");

        JDBC.assertUnorderedResultSet(
            rs,
            new String[][] {
                {"4", null, null},
                {"4", null, null},
                {"5", null, null},
                {"5", null, null},
                {"6", null, null},
                {"6", null, null},
                {"7", null, null},
                {"7", null, null},
                {"12", "12", null},
                {"12", "12", null},
                {"12", "12", null},
                {"12", "12", null},
                {"13", "13", null},
                {"13", "13", null},
                {"13", "13", null},
                {"13", "13", null},
                {"14", "14", null},
                {"14", "14", null},
                {"14", "14", null},
                {"14", "14", null},
                {"15", "15", null},
                {"15", "15", null},
                {"15", "15", null},
                {"15", "15", null},
                {null, null, null}});

        JDBC.checkPlan(s,
                  new String[] {
                      "Hash Left Outer Join ResultSet:",
                      "Left result set:",
                      "_Hash Left Outer Join ResultSet:"});
    }

    /**
     * Check that ordering actually takes place as well as checking that the
     * results are correct.
     */
    public void testDerby_4471c() throws SQLException {
        setAutoCommit(false);
        Statement s = createStatement();

        s.execute("create table t1(c1 int)");
        s.execute("create table t2(c1 int)");
        s.execute("create table t3(c1 int)");

        s.execute("insert into t1 values 1, 2, 2, 3, 4");
        s.execute("insert into t2 values 1, 3, 3, 5, 6");
        s.execute("insert into t3 values 2, 3, 5, 5, 7");

        s.execute("CALL SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1)");

        PreparedStatement ps = prepareStatement(
            "select * from t1 left outer join " +
            "     (t2 left outer join t3 on t2.c1 = t3.c1)" +
            "   on t1.c1 = t2.c1");

        ResultSet rs = ps.executeQuery();

        JDBC.assertUnorderedResultSet(
            rs,
            new String[][] {
                {"1", "1", null},
                {"2", null, null},
                {"2", null, null},
                {"3", "3", "3"},
                {"3", "3", "3"},
                {"4", null, null}});

        JDBC.checkPlan(s,
                  new String[] {
                      "Hash Left Outer Join ResultSet:",
                      "Left result set:",
                      "_Hash Left Outer Join ResultSet:"});
    }

    /**
     * Check that ordering actually takes place (more complex: nested
     * reordering) as well as checking that the results are correct.
     */
    public void testDerby_4471d() throws SQLException {
        setAutoCommit(false);
        Statement s = createStatement();

        s.execute("create table t1(c1 int)");
        s.execute("create table t2(c2 int)");
        s.execute("create table t3(c3 int)");

        s.execute("insert into t1 values 1, 2, 2, 3, 4");
        s.execute("insert into t2 values 1, 3, 3, 5, 6");
        s.execute("insert into t3 values 2, 3, 5, 5, 7");

        s.execute("CALL SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1)");



        PreparedStatement ps = prepareStatement(
            "select * from t1 t1_o left outer join " +
            "     ((t1 left outer join t2 on t1.c1 = t2.c2) left outer join " +
            "      t3 on t2.c2 = t3.c3)" +
            "   on t1_o.c1 = t2.c2");

        // Expect one reordering, cf. rtsp.assertSequence below:
        //
        //     LOJ1 [t1_o.c1 = t2.c2]               LOJ1 [t2.c2=t3.c3]
        //     /  \                                  /   \
        //    /    \                                /    t3
        //   /      \                             LOJ2 [t1_o.c1 = t2.c2]
        //  t1_o   LOJ2 [t2.c2=t3.c3]            /    \
        //          /   \                       /     LOJ3 [t2.c2=t3.c3]
        //         /     \             =>    t1_o     /  \
        //        /       t3                         /    \
        //       /                                  t1    t2
        //      LOJ3 [t1.c1=t2.c2]
        //      /  \
        //     /    \
        //   t1     t2
        //
        // The reason we don't get two reorderings here is that the predicate
        // on LOJ1, "t1_o.c1 = t2.c2" refrences LOJ3's null producing side,
        // t2. Contrast with next example below.

        ResultSet rs = ps.executeQuery();

        JDBC.assertUnorderedResultSet(
            rs,
            new String[][] {
                {"1", "1", "1", null},
                {"2", null, null, null},
                {"2", null, null, null},
                {"3", "3", "3", "3"},
                {"3", "3", "3", "3"},
                {"4", null, null, null}});

        JDBC.checkPlan(s,
                  new String[] {
                      "Hash Left Outer Join ResultSet:",
                      "Left result set:",
                      "_Nested Loop Left Outer Join ResultSet:",
                      "_Left result set:",
                      "_Right result set:",
                      "__Source result set:",
                      "___Hash Left Outer Join ResultSet:"});

        ps = prepareStatement(
            "select * from " +
            "    t1 t1_o left outer join " +
            "        ((t1 t1_i left outer join t2 " +
            "          on t1_i.c1 = t2.c2) left outer join t3 " +
            "         on t1_i.c1 = t3.c3)" +
            "    on t1_o.c1 = t1_i.c1");

        // Expect two reorderings, cf. rtsp.assertSequence below:
        //
        //      LOJ1 [t1_o.c1 = t1_i.c1]               LOJ1 [t2.c2=t3.c3]
        //      /  \                                  /   \
        //     /    \                                /    t3
        //    /      \                             LOJ2 [t1_o.c1 = t1_i.c1]
        //   t1_o   LOJ2 [t1_i.c1=t3.c3]          /    \
        //           /   \                       /     LOJ3 [t1.c1=t2.c2]
        //          /     \             =>    t1_o     /  \
        //         /       t3                         /    \
        //       LOJ3 [t1_i.c1=t2.c2]               t1_i   t2
        //       /  \
        //      /    \
        //    t1_i    t2                =>
        //                                          LOJ1 [t2.c2=t3.c3]
        //                                           /   \
        //                                          /    t3
        //                                       LOJ2 [t2.c2=t3.c3]
        //                                        /  \
        //                                       /    t2
        //                                     LOJ3  [t1_o.c1 = t1_i.c1]
        //                                     /   \
        //                                   t1_o  t1_i
        //
        // In this example, LOJ1's predicate references LOJ3's row preserving
        // side (t1_i), so we get two reorderings.

        rs = ps.executeQuery();

        JDBC.assertUnorderedResultSet(
            rs,
            new String[][] {
                {"1", "1", "1", null},
                {"2", "2", null, "2"},
                {"2", "2", null, "2"},
                {"2", "2", null, "2"},
                {"2", "2", null, "2"},
                {"3", "3", "3", "3"},
                {"3", "3", "3", "3"},
                {"4", "4", null, null}});

        JDBC.checkPlan(s,
                  new String[] {
                      "Hash Left Outer Join ResultSet:",
                      "Left result set:",
                      "_Hash Left Outer Join ResultSet:",
                      "_Left result set:",
                      "__Hash Left Outer Join ResultSet:"});
    }


   /**
    * This fixture would give:
    * <pre>
    *   ASSERT FAILED sourceResultSetNumber expected to be &gt;= 0 for T2.X
    * </pre>
    * error in sane mode prior to DERBY-4736 due to a missing rebinding
    * operation as a result a the LOJ reordering.  Schema and query originally
    * stems from DERBY-4712 (parent issue of DERBY-4736).
    */
    public void testDerby_4736() throws SQLException {
//IC see: https://issues.apache.org/jira/browse/DERBY-4736
        setAutoCommit(false);
        Statement s = createStatement();

        s.executeUpdate("create table t0(x int)");
        s.executeUpdate("create table t1(x int)");
        s.executeUpdate("create table t2(x int)");
        s.executeUpdate("create table t3(x int)");
        s.executeUpdate("create table t4(x int)");
        s.executeUpdate("insert into t4 values(0)");
        s.executeUpdate("insert into t4 values(1)");
        s.executeUpdate("insert into t4 values(2)");
        s.executeUpdate("insert into t4 values(3)");
        s.executeUpdate("create table t5(x int)");
        s.executeUpdate("insert into t5 values(0)");
        s.executeUpdate("insert into t5 values(1)");
        s.executeUpdate("insert into t5 values(2)");
        s.executeUpdate("insert into t5 values(3)");
        s.executeUpdate("insert into t5 values(4)");
        s.executeUpdate("create table t6(x int)");
        s.executeUpdate("insert into t6 values(0)");
        s.executeUpdate("insert into t6 values(1)");
        s.executeUpdate("insert into t6 values(2)");
        s.executeUpdate("insert into t6 values(3)");
        s.executeUpdate("insert into t6 values(4)");
        s.executeUpdate("insert into t6 values(5)");
        s.executeUpdate("create table t7(x int)");
        s.executeUpdate("insert into t7 values(0)");
        s.executeUpdate("insert into t7 values(1)");
        s.executeUpdate("insert into t7 values(2)");
        s.executeUpdate("insert into t7 values(3)");
        s.executeUpdate("insert into t7 values(4)");
        s.executeUpdate("insert into t7 values(5)");
        s.executeUpdate("insert into t7 values(6)");
        s.executeUpdate("create table t8(x int)");
        s.executeUpdate("insert into t8 values(0)");
        s.executeUpdate("insert into t8 values(1)");
        s.executeUpdate("insert into t8 values(2)");
        s.executeUpdate("insert into t8 values(3)");
        s.executeUpdate("insert into t8 values(4)");
        s.executeUpdate("insert into t8 values(5)");
        s.executeUpdate("insert into t8 values(6)");
        s.executeUpdate("insert into t8 values(7)");
        s.executeUpdate("create table t9(x int)");
        s.executeUpdate("insert into t9 values(0)");
        s.executeUpdate("insert into t9 values(1)");
        s.executeUpdate("insert into t9 values(2)");
        s.executeUpdate("insert into t9 values(3)");
        s.executeUpdate("insert into t9 values(4)");
        s.executeUpdate("insert into t9 values(5)");
        s.executeUpdate("insert into t9 values(6)");
        s.executeUpdate("insert into t9 values(7)");
        s.executeUpdate("insert into t9 values(8)");
        s.executeUpdate("insert into t0 values(1)");
        s.executeUpdate("insert into t1 values(2)");
        s.executeUpdate("insert into t0 values(3)");
        s.executeUpdate("insert into t1 values(3)");
        s.executeUpdate("insert into t2 values(4)");
        s.executeUpdate("insert into t0 values(5)");
        s.executeUpdate("insert into t2 values(5)");
        s.executeUpdate("insert into t1 values(6)");
        s.executeUpdate("insert into t2 values(6)");
        s.executeUpdate("insert into t0 values(7)");
        s.executeUpdate("insert into t1 values(7)");
        s.executeUpdate("insert into t2 values(7)");
        s.executeUpdate("insert into t3 values(8)");
        s.executeUpdate("insert into t0 values(9)");
        s.executeUpdate("insert into t3 values(9)");
        s.executeUpdate("insert into t1 values(10)");
        s.executeUpdate("insert into t3 values(10)");
        s.executeUpdate("insert into t0 values(11)");
        s.executeUpdate("insert into t1 values(11)");
        s.executeUpdate("insert into t3 values(11)");
        s.executeUpdate("insert into t2 values(12)");
        s.executeUpdate("insert into t3 values(12)");
        s.executeUpdate("insert into t0 values(13)");
        s.executeUpdate("insert into t2 values(13)");
        s.executeUpdate("insert into t3 values(13)");
        s.executeUpdate("insert into t1 values(14)");
        s.executeUpdate("insert into t2 values(14)");
        s.executeUpdate("insert into t3 values(14)");
        s.executeUpdate("insert into t0 values(15)");
        s.executeUpdate("insert into t1 values(15)");
        s.executeUpdate("insert into t2 values(15)");
        s.executeUpdate("insert into t3 values(15)");

        s.execute("CALL SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1)");

        ResultSet rs = s.executeQuery(
            "select t0.x , t1.x , t2.x , t3.x , t4.x , t5.x , t6.x from " +
            "    ((t0 right outer join " +
            "         (t1 right outer join " +
            //            t2 LOJ (t3 LOJ t4) will be reordered
            "             (t2 left outer join " +
            "                 (t3 left outer join t4 on t3.x = t4.x ) " +
            "              on t2.x = t3.x ) " +
            "          on t1.x = t3.x ) " +
            "      on t0.x = t1.x ) " +
            "     left outer join " +
            "      (t5 inner join t6 on t5.x = t6.x ) " +
            "     on t2.x = t5.x)" );

        // The expected result below has been verified to the one we get if we
        // don't reorder LOJ.
        JDBC.assertUnorderedResultSet(
            rs,
            new String[][] {
                {null, null, "4", null, null, "4", "4"},
                {null, null, "5", null, null, null, null},
                {null, null, "6", null, null, null, null},
                {null, null, "7", null, null, null, null},
                {null, null, "12", "12", null, null, null},
                {null, null, "13", "13", null, null, null},
                {null, "14", "14", "14", null, null, null},
                {"15", "15", "15", "15", null, null, null}});

        rs = s.executeQuery("values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS()");
        rs.next();
        String rts = rs.getString(1);

        // Now verify that we actually *did* reorder
        RuntimeStatisticsParser rtsp = new RuntimeStatisticsParser(rts);
        rtsp.assertSequence(
            new String[] {
                "_Nested Loop Left Outer Join ResultSet:",
                "_Left result set:",
                "__Hash Left Outer Join ResultSet:",
                "__Left result set:",
                "___Hash Left Outer Join ResultSet:",
                "___Left result set:",
                "____Hash Left Outer Join ResultSet:",
                "____Left result set:",
                "_____Hash Left Outer Join ResultSet:",
                // Note: T2 and T3 are in innermost LOJ as expected
                // whereas originally it was T3 and T4
                "_____Left result set:",
                "______Table Scan ResultSet for T2 ",
                "_____Right result set:",
                "______Hash Scan ResultSet for T3 ",
                "____Right result set:",
                "_____Hash Scan ResultSet for T4"});

        rs.close();
    }


    /**
     * Test for a follow-up patch for DERBY-4736: verify that nullability in
     * result set metadata is correct also for columns for the null-producing
     * side of the LOJ.
     */
    public void testDerby_4736_nullability() throws Exception
    {
        setAutoCommit(false);
//IC see: https://issues.apache.org/jira/browse/DERBY-4736

        Statement st = createStatement();
        ResultSet rs = null;
        String [][] expRS;
        String [] expColNames;

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
            "create view v1 (fv, ev, dv, cv, bv, av) as (select "
            + "f, e, d, c, b, a from t left outer join s on b = e)");

        rs = st.executeQuery(
            " select * from t left outer join (s left outer join "
            + "v1 on (f = cv)) on (d=a)");

        expColNames = new String [] {"A", "B", "C", "D", "E", "F",
                                     "FV", "EV", "DV", "CV", "BV", "AV"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            // Before the follow-up patch, the three first NULL column values
            // below would get NOT NULL metadata before the follow-up patch
            // (caught by JDBC.assertResultColumnNullable called from
            // JDBC.assertRowInResultSet if a null value is seen).
            //
            {"1", "1.000", "1", null, null, null,
             "1", "1.000", "1", null, null, null},

            {"2", "2.000", "2", "2", "2.000", "2",
             "2", "2.000", "2", "2", "2.000", "2"},

            {"3", "3.000", "3", "3", "3.000", "3",
             "3", "3.000", "3", "3", "3.000", "3"}
        };

        JDBC.assertFullResultSet(rs, expRS);
    }


    /**
     * Test the queries reported in DERBY-4712 as giving null pointer
     * exceptions. Should fail with NPE before the fix went in.  For bug
     * explanation, see the JIRA issue and {@code JoinNode#transformOuterJoins}.
     */
    public void testDerby_4712_NPEs() throws Exception
    {
        setAutoCommit(false);

        Statement st = createStatement();
        ResultSet rs = null;
        String [][] expRS;

        st.executeUpdate("create table t0(x0 int)");
        st.executeUpdate("create table t1(x1 int)");
        st.executeUpdate("create table t2(x2 int)");
        st.executeUpdate("create table t3(x3 int)");
        st.executeUpdate("create table t4(x4 int)");
        st.executeUpdate("insert into t4 values(0)");
        st.executeUpdate("insert into t4 values(1)");
        st.executeUpdate("insert into t4 values(2)");
        st.executeUpdate("insert into t4 values(3)");
        st.executeUpdate("create table t5(x5 int)");
        st.executeUpdate("insert into t5 values(0)");
        st.executeUpdate("insert into t5 values(1)");
        st.executeUpdate("insert into t5 values(2)");
        st.executeUpdate("insert into t5 values(3)");
        st.executeUpdate("insert into t5 values(4)");
        st.executeUpdate("create table t6(x6 int)");
        st.executeUpdate("insert into t6 values(0)");
        st.executeUpdate("insert into t6 values(1)");
        st.executeUpdate("insert into t6 values(2)");
        st.executeUpdate("insert into t6 values(3)");
        st.executeUpdate("insert into t6 values(4)");
        st.executeUpdate("insert into t6 values(5)");
        st.executeUpdate("create table t7(x7 int)");
        st.executeUpdate("insert into t7 values(0)");
        st.executeUpdate("insert into t7 values(1)");
        st.executeUpdate("insert into t7 values(2)");
        st.executeUpdate("insert into t7 values(3)");
        st.executeUpdate("insert into t7 values(4)");
        st.executeUpdate("insert into t7 values(5)");
        st.executeUpdate("insert into t7 values(6)");
        st.executeUpdate("create table t8(x8 int)");
        st.executeUpdate("insert into t8 values(0)");
        st.executeUpdate("insert into t8 values(1)");
        st.executeUpdate("insert into t8 values(2)");
        st.executeUpdate("insert into t8 values(3)");
        st.executeUpdate("insert into t8 values(4)");
        st.executeUpdate("insert into t8 values(5)");
        st.executeUpdate("insert into t8 values(6)");
        st.executeUpdate("insert into t8 values(7)");
        st.executeUpdate("create table t9(x9 int)");
        st.executeUpdate("insert into t9 values(0)");
        st.executeUpdate("insert into t9 values(1)");
        st.executeUpdate("insert into t9 values(2)");
        st.executeUpdate("insert into t9 values(3)");
        st.executeUpdate("insert into t9 values(4)");
        st.executeUpdate("insert into t9 values(5)");
        st.executeUpdate("insert into t9 values(6)");
        st.executeUpdate("insert into t9 values(7)");
        st.executeUpdate("insert into t9 values(8)");
        st.executeUpdate("insert into t0 values(1)");
        st.executeUpdate("insert into t1 values(2)");
        st.executeUpdate("insert into t0 values(3)");
        st.executeUpdate("insert into t1 values(3)");
        st.executeUpdate("insert into t2 values(4)");
        st.executeUpdate("insert into t0 values(5)");
        st.executeUpdate("insert into t2 values(5)");
        st.executeUpdate("insert into t1 values(6)");
        st.executeUpdate("insert into t2 values(6)");
        st.executeUpdate("insert into t0 values(7)");
        st.executeUpdate("insert into t1 values(7)");
        st.executeUpdate("insert into t2 values(7)");
        st.executeUpdate("insert into t3 values(8)");
        st.executeUpdate("insert into t0 values(9)");
        st.executeUpdate("insert into t3 values(9)");
        st.executeUpdate("insert into t1 values(10)");
        st.executeUpdate("insert into t3 values(10)");
        st.executeUpdate("insert into t0 values(11)");
        st.executeUpdate("insert into t1 values(11)");
        st.executeUpdate("insert into t3 values(11)");
        st.executeUpdate("insert into t2 values(12)");
        st.executeUpdate("insert into t3 values(12)");
        st.executeUpdate("insert into t0 values(13)");
        st.executeUpdate("insert into t2 values(13)");
        st.executeUpdate("insert into t3 values(13)");
        st.executeUpdate("insert into t1 values(14)");
        st.executeUpdate("insert into t2 values(14)");
        st.executeUpdate("insert into t3 values(14)");
        st.executeUpdate("insert into t0 values(15)");
        st.executeUpdate("insert into t1 values(15)");
        st.executeUpdate("insert into t2 values(15)");
        st.executeUpdate("insert into t3 values(15)");

        rs = st.executeQuery(
        "SELECT t0.x0,                                                  " +
        "       t1.x1,                                                  " +
        "       t2.x2,                                                  " +
        "       t3.x3,                                                  " +
        "       t4.x4,                                                  " +
        "       t5.x5,                                                  " +
        "       t6.x6,                                                  " +
        "       t7.x7,                                                  " +
        "       t8.x8                                                   " +
        "FROM   (((t0                                                   " +
        "          INNER JOIN ((t1                                      " +
        "                       RIGHT OUTER JOIN (t2                    " +
        "                                         INNER JOIN t3         " +
        "                                           ON t2.x2 = t3.x3 )  " +
        "                         ON t1.x1 = t2.x2 )                    " +
        "                      LEFT OUTER JOIN (t4                      " +
        "                                       INNER JOIN t5           " +
        "                                         ON t4.x4 = t5.x5 )    " +
        "                        ON t1.x1 = t4.x4 )                     " +
        "            ON t0.x0 = t2.x2 )                                 " +
        "         LEFT OUTER JOIN (t6                                   " +
        "                          INNER JOIN t7                        " +
        "                            ON t6.x6 = t7.x7 )                 " +
        "           ON t1.x1 = t6.x6 )                                  " +
        "        INNER JOIN t8                                          " +
        "          ON t5.x5 = t8.x8 )                                   ");

        JDBC.assertEmpty(rs);

        rs = st.executeQuery(
        "SELECT t0.x0,                                               " +
        "       t1.x1,                                               " +
        "       t2.x2,                                               " +
        "       t3.x3,                                               " +
        "       t4.x4,                                               " +
        "       t5.x5,                                               " +
        "       t6.x6,                                               " +
        "       t7.x7                                                " +
        "FROM   ((t0                                                 " +
        "         RIGHT OUTER JOIN t1                                " +
        "           ON t0.x0 = t1.x1 )                               " +
        "        INNER JOIN (((t2                                    " +
        "                      INNER JOIN (t3                        " +
        "                                  LEFT OUTER JOIN t4        " +
        "                                    ON t3.x3 = t4.x4 )      " +
        "                        ON t2.x2 = t3.x3 )                  " +
        "                     RIGHT OUTER JOIN t5                    " +
        "                       ON t2.x2 = t5.x5 )                   " +
        "                    LEFT OUTER JOIN (t6                     " +
        "                                     INNER JOIN t7          " +
        "                                       ON t6.x6 = t7.x7 )   " +
        "                      ON t4.x4 = t6.x6 )                    " +
        "          ON t0.x0 = t5.x5 )                                ");

        expRS = new String [][]
        {
            {"3", "3", null, null, null, "3", null, null}
        };

        JDBC.assertFullResultSet(rs, expRS);

        rs = st.executeQuery(
        "SELECT t0.x0,                                                " +
        "       t1.x1,                                                " +
        "       t2.x2,                                                " +
        "       t3.x3,                                                " +
        "       t4.x4,                                                " +
        "       t5.x5,                                                " +
        "       t6.x6,                                                " +
        "       t7.x7                                                 " +
        "FROM   ((((t0                                                " +
        "           LEFT OUTER JOIN t1                                " +
        "             ON t0.x0 = t1.x1 )                              " +
        "          RIGHT OUTER JOIN t2                                " +
        "            ON t0.x0 = t2.x2 )                               " +
        "         RIGHT OUTER JOIN t3                                 " +
        "           ON t0.x0 = t3.x3 )                                " +
        "        INNER JOIN ((t4                                      " +
        "                     INNER JOIN t5                           " +
        "                       ON t4.x4 = t5.x5 )                    " +
        "                    RIGHT OUTER JOIN (t6                     " +
        "                                      RIGHT OUTER JOIN t7    " +
        "                                        ON t6.x6 = t7.x7 )   " +
        "                      ON t4.x4 = t6.x6 )                     " +
        "          ON t1.x1 = t4.x4 )                                 ");

        JDBC.assertEmpty(rs);

        rs = st.executeQuery(
        "SELECT t0.x0,                                    " +
        "       t1.x1,                                    " +
        "       t2.x2,                                    " +
        "       t3.x3,                                    " +
        "       t4.x4,                                    " +
        "       t5.x5                                     " +
        "FROM   (((t0                                     " +
        "          INNER JOIN t1                          " +
        "            ON t0.x0 = t1.x1 )                   " +
        "         RIGHT OUTER JOIN (t2                    " +
        "                           RIGHT OUTER JOIN t3   " +
        "                             ON t2.x2 = t3.x3 )  " +
        "           ON t0.x0 = t2.x2 )                    " +
        "        INNER JOIN (t4                           " +
        "                    LEFT OUTER JOIN t5           " +
        "                      ON t4.x4 = t5.x5 )         " +
        "          ON t1.x1 = t4.x4 )                     ");

        JDBC.assertEmpty(rs);

        rs = st.executeQuery(
        "SELECT t0.x0,                                                    " +
        "       t1.x1,                                                    " +
        "       t2.x2,                                                    " +
        "       t3.x3,                                                    " +
        "       t4.x4,                                                    " +
        "       t5.x5,                                                    " +
        "       t6.x6                                                     " +
        "FROM   ((t0                                                      " +
        "         RIGHT OUTER JOIN                                        " +
        "                  (t1                                            " +
        "                   RIGHT OUTER JOIN (t2                          " +
        "                                     LEFT OUTER JOIN             " +
        "                                           (t3                   " +
        "                                            LEFT OUTER JOIN t4   " +
        "                                               ON t3.x3 = t4.x4  " +
        "                                            )                    " +
        "                                               ON t2.x2 = t3.x3 )" +
        "                      ON t1.x1 = t3.x3 )                         " +
        "           ON t0.x0 = t1.x1 )                                    " +
        "        LEFT OUTER JOIN (t5                                      " +
        "                         INNER JOIN t6                           " +
        "                           ON t5.x5 = t6.x6 )                    " +
        "          ON t2.x2 = t5.x5 )                                     ");

        expRS = new String [][]
        {
            {null, null, "4", null, null, "4", "4"},
            {null, null, "5", null, null, null, null},
            {null, null, "6", null, null, null, null},
            {null, null, "7", null, null, null, null},
            {null, null, "12", "12", null, null, null},
            {null, null, "13", "13", null, null, null},
            {null, "14", "14", "14", null, null, null},
            {"15", "15", "15", "15", null, null, null},
        };

        JDBC.assertFullResultSet(rs, expRS);
    }


    /**
     * Test the queries reported in DERBY-4798 as giving null pointer
     * exceptions. Should fail with NPE before the fix went in.
     */
    public void testDerby_4798_NPE() throws Exception
    {
        setAutoCommit(false);
//IC see: https://issues.apache.org/jira/browse/DERBY-4798
//IC see: https://issues.apache.org/jira/browse/DERBY-3097

        Statement st = createStatement();
        ResultSet rs = null;
        String [][] expRS;

        st.executeUpdate("create table t0(x0 int)");
        st.executeUpdate("create table t1(x1 int)");
        st.executeUpdate("create table t2(x2 int)");
        st.executeUpdate("create table t3(x3 int)");
        st.executeUpdate("create table t4(x4 int)");
        st.executeUpdate("insert into t4 values(0)");
        st.executeUpdate("insert into t4 values(1)");
        st.executeUpdate("insert into t4 values(2)");
        st.executeUpdate("insert into t4 values(3)");
        st.executeUpdate("create table t5(x5 int)");
        st.executeUpdate("insert into t5 values(0)");
        st.executeUpdate("insert into t5 values(1)");
        st.executeUpdate("insert into t5 values(2)");
        st.executeUpdate("insert into t5 values(3)");
        st.executeUpdate("insert into t5 values(4)");
        st.executeUpdate("create table t6(x6 int)");
        st.executeUpdate("insert into t6 values(0)");
        st.executeUpdate("insert into t6 values(1)");
        st.executeUpdate("insert into t6 values(2)");
        st.executeUpdate("insert into t6 values(3)");
        st.executeUpdate("insert into t6 values(4)");
        st.executeUpdate("insert into t6 values(5)");
        st.executeUpdate("create table t7(x7 int)");
        st.executeUpdate("insert into t7 values(0)");
        st.executeUpdate("insert into t7 values(1)");
        st.executeUpdate("insert into t7 values(2)");
        st.executeUpdate("insert into t7 values(3)");
        st.executeUpdate("insert into t7 values(4)");
        st.executeUpdate("insert into t7 values(5)");
        st.executeUpdate("insert into t7 values(6)");
        st.executeUpdate("insert into t0 values(1)");
        st.executeUpdate("insert into t1 values(2)");
        st.executeUpdate("insert into t0 values(3)");
        st.executeUpdate("insert into t1 values(3)");
        st.executeUpdate("insert into t2 values(4)");
        st.executeUpdate("insert into t0 values(5)");
        st.executeUpdate("insert into t2 values(5)");
        st.executeUpdate("insert into t1 values(6)");
        st.executeUpdate("insert into t2 values(6)");
        st.executeUpdate("insert into t0 values(7)");
        st.executeUpdate("insert into t1 values(7)");
        st.executeUpdate("insert into t2 values(7)");
        st.executeUpdate("insert into t3 values(8)");
        st.executeUpdate("insert into t0 values(9)");
        st.executeUpdate("insert into t3 values(9)");
        st.executeUpdate("insert into t1 values(10)");
        st.executeUpdate("insert into t3 values(10)");
        st.executeUpdate("insert into t0 values(11)");
        st.executeUpdate("insert into t1 values(11)");
        st.executeUpdate("insert into t3 values(11)");
        st.executeUpdate("insert into t2 values(12)");
        st.executeUpdate("insert into t3 values(12)");
        st.executeUpdate("insert into t0 values(13)");
        st.executeUpdate("insert into t2 values(13)");
        st.executeUpdate("insert into t3 values(13)");
        st.executeUpdate("insert into t1 values(14)");
        st.executeUpdate("insert into t2 values(14)");
        st.executeUpdate("insert into t3 values(14)");
        st.executeUpdate("insert into t0 values(15)");
        st.executeUpdate("insert into t1 values(15)");
        st.executeUpdate("insert into t2 values(15)");
        st.executeUpdate("insert into t3 values(15)");

        rs = st.executeQuery(
        "SELECT t0.x0, " +
        "       t1.x1," +
        "       t2.x2," +
        "       t3.x3," +
        "       t4.x4," +
        "       t5.x5," +
        "       t6.x6," +
        "       t7.x7 " +
        "FROM         " +
        " ((t0                                                               " +
        "   LEFT OUTER JOIN ((t1                                             " +
        "                     LEFT OUTER JOIN (t2                            " +
        "                                      LEFT OUTER JOIN t3            " +
        "                                        ON t2.x2 = t3.x3 )          " +
        "                       ON t1.x1 = t2.x2 )                           " +
        "                    LEFT OUTER JOIN (t4                             " +
        "                                     INNER JOIN (t5                 " +
        "                                                 LEFT OUTER JOIN t6 " +
        "                                                   ON t5.x5 = t6.x6)" +
        "                                       ON t4.x4 = t5.x5 )           " +
        "                      ON t1.x1 = t5.x5 )                            " +
        "     ON t0.x0 = t5.x5 )                                             " +
        "  LEFT OUTER JOIN t7                                                " +
        "    ON t3.x3 = t7.x7 )                                              ");

        expRS = new String [][]
        {
            {"1", "1", null, null, null, null, null, null},
            {"3", "3", "3", null, "3", "3", "3", null},
            {"5", "5", null, null, null, null, null, null},
            {"7", "7", null, null, null, null, null, null},
            {"9", "9", null, null, null, null, null, null},
            {"11", "11", null, null, null, null, null, null},
            {"13", "13", null, null, null, null, null, null},
            {"15", "15", null, null, null, null, null, null}
        };

        JDBC.assertFullResultSet(rs, expRS);
    }
}
