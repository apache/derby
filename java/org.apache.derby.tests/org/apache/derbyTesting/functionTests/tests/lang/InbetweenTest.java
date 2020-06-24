
/*
Derby - Class org.apache.derbyTesting.functionTests.tests.lang.InbetweenTest

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
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import junit.framework.Test;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.DatabasePropertyTestSetup;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.RuntimeStatisticsParser;
import org.apache.derbyTesting.junit.TestConfiguration;

public final class InbetweenTest extends BaseJDBCTestCase {

    /**
     * Public constructor required for running test as standalone JUnit.
     */
    public InbetweenTest(String name)
    {
        super(name);
    }

    public static Test suite()
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-6590
        BaseTestSuite suite = new BaseTestSuite("InbetweenTest Test");
        suite.addTest(DatabasePropertyTestSetup.singleProperty(
                TestConfiguration.defaultSuite(InbetweenTest.class),
                "derby.language.statementCacheSize", "0",true));
        return suite;
    }

    private void createTestObjects(Statement st) throws SQLException {
//IC see: https://issues.apache.org/jira/browse/DERBY-5715
        setAutoCommit(false);
        CleanDatabaseTestSetup.cleanDatabase(getConnection(), false);
        
        st.executeUpdate("set isolation to rr");
        
        st.executeUpdate(
            " CREATE FUNCTION ConsistencyChecker() RETURNS VARCHAR(2000)"
                + "EXTERNAL NAME "
                + "'org.apache.derbyTesting.functionTests.util.T_Consis"
                + "tencyChecker.runConsistencyChecker'"
                + "LANGUAGE JAVA PARAMETER STYLE JAVA");
        
        // Create the tables
       
        st.executeUpdate(
            "create table s (i int)");
        
        st.executeUpdate(
            " create table t (i int, s smallint, c char(10), v "
                + "varchar(50), "
                + "d double precision, r real, e date, t time, p timestamp)");
        
        st.executeUpdate(
                " create table test (i int, d double precision)");
        
        st.executeUpdate(
            "create table big(i int, c char(10))");
        
        st.executeUpdate(
            "create table bt1 (i int, c char(5), de decimal(4, 1))");
        
        st.executeUpdate(
            " create table bt2 (i int, d double, da date, t "
                + "time, tp timestamp, vc varchar(10))");
        
        // Populate the tables
        
        st.executeUpdate("insert into s values (1)");        
        st.executeUpdate(" insert into s values (1)");                    
        st.executeUpdate(" insert into s values (2)");
        
        st.executeUpdate(
            " insert into t values (null, null, null, null, "
                + "null, null, null, null, null)");
        
        st.executeUpdate(
            " insert into t values (0, 100, 'hello', 'everyone "
                + "is here', 200.0e0,"
                + "	300.0e0, '1992-01-01','12:30:30',"
                +"'"+Timestamp.valueOf("1992-01-01 12:30:30")+"')");
        
        st.executeUpdate(
            "insert into t values (-1, -100, 'goodbye', "
                + "'everyone is there', -200.0e0,"
                + "	-300.0e0, '1992-01-02', '12:30:59',"
                +"'"+Timestamp.valueOf("1992-01-02 12:30:59")+"')");
        
        st.executeUpdate(" insert into test values (2, 4.0)");                    
        st.executeUpdate(" insert into test values (3, 10.0)");            
        st.executeUpdate(" insert into test values (4, 12.0)");                    
        st.executeUpdate(" insert into test values (5, 25.0)");                   
        st.executeUpdate(" insert into test values (10, 100.0)");      
        st.executeUpdate(" insert into test values (-6, 36)");
        
        st.executeUpdate(
            " insert into big values "
                + "	(1, '1'), (2, '2'), (3, '3'), (4, '4'), (5, '5'), "
                + "(6, '6'), (7, '7'), (8, '8'), (9, '9'), (10, '10'), "
                + "	(11, '11'), (12, '12'), (13, '13'), (14, '14'), "
                + "(15, '15'), (16, '16'), (17, '17'), (18, '18'), "
                + "(19, '19'), (20, '20'), "
                + "	(21, '21'), (22, '22'), (23, '23'), (24, '24'), "
                + "(25, '25'), (26, '26'), (27, '27'), (28, '28'), "
                + "(29, '29'), (30, '30')");        
        
        st.executeUpdate(
            " insert into bt1 values (1, 'one', null), (2, "
            + "'two', 22.2), (3, 'three', null),"
            + "  (7, 'seven', null), (8, 'eight', 2.8), (9, "
            + "'nine', null), (3, 'trois', 21.2)");
        
        st.executeUpdate(
            " insert into bt1 (i) values 10, 11, 12, 13, 14, 15, "
            + "16, 17, 18, 19, 20");
        
        assertUpdateCount(st, 11,
            " update bt1 set c = cast (i as char(5)) where i >= 10");
        
        assertUpdateCount(st, 6,
            " update bt1 set de = cast (i/2.8 as decimal(4,1)) "
            + "where i >= 10 and 2 * (cast (i as double) / 2.0) - "
            + "(i / 2) = i / 2");
        
        st.executeUpdate(
            " insert into bt2 values (8, -800.0, '1992-03-22', "
            + "'03:22:28', '"+Timestamp.valueOf("1992-03-22 03:22:28.0")+"',"
            +"'2992-01-02')");
        
        st.executeUpdate(
            " insert into bt2 values (1, 200.0, '1998-03-22',"
            + "'13:22:28', '"+Timestamp.valueOf("1998-03-22 03:22:28.0")+"',"
            +"'3999-08-08')");
        
        st.executeUpdate(
            " insert into bt2 values (-8, 800, '3999-08-08', "
            + "'02:28:22', '"+Timestamp.valueOf("3999-08-08 02:28:22.0")+"',"
            +"'1992-01-02')");
        
        st.executeUpdate(
            " insert into bt2 values (18, 180.00, '2007-02-23', "
            + "'15:47:27', null, null)");
        
        st.executeUpdate(
            " insert into bt2 values (22, 202.010, '2007-02-23', "
            + "'15:47:27', null, null)");
        
        st.executeUpdate(
            " insert into bt2 values (23, 322.002, null, "
            + "'15:47:28', null, null)");
        
        st.executeUpdate(
//IC see: https://issues.apache.org/jira/browse/DERBY-4375
                " insert into bt2 values (28, 82, null, '15:47:28', "
                + "'"+Timestamp.valueOf("2007-02-23 15:47:27.544")+"', null)");
            
        //create indexes
        
        st.executeUpdate(" create index ix_big_i on big (i)");       
        st.executeUpdate(" create index bt1_ixi on bt1 (i)");       
        st.executeUpdate(" create index bt1_ixde on bt1 (de)");       
        st.executeUpdate(" create index bt1_ixic on bt1 (i, c)");       
        st.executeUpdate(" create index bt2_ixd on bt2 (d)");        
        st.executeUpdate(" create index bt2_ixda on bt2 (da)");        
        st.executeUpdate(" create index bt2_ixvc on bt2 (vc)");
             
    }
    
    public void testBetween() throws Exception {
        
//IC see: https://issues.apache.org/jira/browse/DERBY-5715
        Statement st = createStatement();
        createTestObjects(st);
                
        // BETWEEN negative tests type mismatches
        
        //Comparisons between 'INTEGER' and 'DATE' are not supported
        assertStatementError("42818", st,
            "select * from t where i between i and e");
        
        //Comparisons between 'INTEGER' and 'TIME' are not supported
        assertStatementError("42818", st,
            " select * from t where i between i and t");
        
        //Comparisons between 'INTEGER' and 'TIMESTAMP' are not supported
        assertStatementError("42818", st,
            " select * from t where i between i and p");
        
        //Comparisons between 'DATE' and 'TIMESTAMP' are not supported
        assertStatementError("42818", st,
            " select * from t where e between p and p");
        
        //Comparisons between 'INTEGER' and 'DATE' are not supported
        assertStatementError("42818", st,
            " select * from t where 1 between e and p");
        
        // between null and i
        
        assertStatementError("42X01", st,
            "select * from t where i between null and i");
        
        // cardinality violation on a subquery
        //Scalar subquery is only allowed to return a single row
        assertStatementError("21000", st,
            "select * from t where i between i and (select i from s)");
        
        // all parameters
        //It is not allowed for both operands of 'BETWEEN' to be ? parameters
        assertStatementError("42X35", st,
            "select * from t where ? between ? and ?");
        
        // positive tests type comparisons
        
//IC see: https://issues.apache.org/jira/browse/DERBY-5715
        ResultSet rs = st.executeQuery(
            "select i from t where i between s and r");
        
        String[] expColNames = {"I"};
        JDBC.assertColumnNames(rs, expColNames);
        JDBC.assertDrainResults(rs, 0);
        
        rs = st.executeQuery(
            " select i from t where i between r and d");
        
        expColNames = new String [] {"I"};
        JDBC.assertColumnNames(rs, expColNames);
        JDBC.assertDrainResults(rs, 0);
        
        rs = st.executeQuery(
            " select i from t where s between i and r");
        
        expColNames = new String [] {"I"};
        JDBC.assertColumnNames(rs, expColNames);
//IC see: https://issues.apache.org/jira/browse/DERBY-5715
        JDBC.assertSingleValueResultSet(rs, "0");
        
        rs = st.executeQuery(
            " select i from t where s between r and d");
        
        expColNames = new String [] {"I"};
        JDBC.assertColumnNames(rs, expColNames);
        JDBC.assertDrainResults(rs, 0);
        
        rs = st.executeQuery(
            " select i from t where r between s and i");
        
        expColNames = new String [] {"I"};
        JDBC.assertColumnNames(rs, expColNames);
        JDBC.assertDrainResults(rs, 0);
        
        rs = st.executeQuery(
            " select i from t where d between s and i");
        
        expColNames = new String [] {"I"};
        JDBC.assertColumnNames(rs, expColNames);
        JDBC.assertDrainResults(rs, 0);
        
        rs = st.executeQuery(
            " select i from t where i between 40e1 and 50e1");
        
        expColNames = new String [] {"I"};
        JDBC.assertColumnNames(rs, expColNames);
        JDBC.assertDrainResults(rs, 0);
        
        rs = st.executeQuery(
            " select i from t where s between 40e1 and 50e1");
        
        expColNames = new String [] {"I"};
        JDBC.assertColumnNames(rs, expColNames);
        JDBC.assertDrainResults(rs, 0);
        
        rs = st.executeQuery(
            " select i from t where c between c and v");
        
        expColNames = new String [] {"I"};
        JDBC.assertColumnNames(rs, expColNames);
        JDBC.assertDrainResults(rs, 0);
        
        rs = st.executeQuery(
            " select i from t where 40e1 between i and s");
        
        expColNames = new String [] {"I"};
        JDBC.assertColumnNames(rs, expColNames);
        JDBC.assertDrainResults(rs, 0);
        
        rs = st.executeQuery(
            " select i from t where 'goodbye' between c and c");
        
        expColNames = new String [] {"I"};
        JDBC.assertColumnNames(rs, expColNames);
//IC see: https://issues.apache.org/jira/browse/DERBY-5715
        JDBC.assertSingleValueResultSet(rs, "-1");
        
        rs = st.executeQuery(
            " select i from t where "
            + "'"+Timestamp.valueOf("1992-01-02 12:30:59")+"'"
            +"between p and p");
        
        expColNames = new String [] {"I"};
        JDBC.assertColumnNames(rs, expColNames);
//IC see: https://issues.apache.org/jira/browse/DERBY-5715
        JDBC.assertSingleValueResultSet(rs, "-1");
        
        //between 2 and 1
        
        rs = st.executeQuery(
            "select * from t where i between 2 and 1");
        
        expColNames = new String [] {"I","S","C","V","D","R","E","T","P"};
        JDBC.assertColumnNames(rs, expColNames);
//IC see: https://issues.apache.org/jira/browse/DERBY-5715
        JDBC.assertEmpty(rs);
        
        rs = st.executeQuery(
            "select * from t where not i not between 2 and 1");
        
        expColNames = new String [] {"I","S","C","V","D","R","E","T","P"};
        JDBC.assertColumnNames(rs, expColNames);
        JDBC.assertEmpty(rs);
        
        rs = st.executeQuery(
            "select * from t where not i between 2 and 1");
        
        expColNames = new String [] {"I","S","C","V","D","R","E","T","P"};
        JDBC.assertColumnNames(rs, expColNames);
        
//IC see: https://issues.apache.org/jira/browse/DERBY-5715
        String[][] expRS = {
            {"0","100","hello","everyone is here","200.0","300.0",
                     "1992-01-01","12:30:30","1992-01-01 12:30:30.0"},
            {"-1","-100","goodbye","everyone is there","-200.0","-300.0",
                     "1992-01-02","12:30:59","1992-01-02 12:30:59.0"},       
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            "select * from t where i not between 2 and 1");
        
        expColNames = new String [] {"I","S","C","V","D","R","E","T","P"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"0","100","hello","everyone is here","200.0","300.0",
                     "1992-01-01","12:30:30","1992-01-01 12:30:30.0"},
            {"-1","-100","goodbye","everyone is there","-200.0","-300.0",
                     "1992-01-02","12:30:59","1992-01-02 12:30:59.0"}      
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);

        //between arbitrary expressions
        
        rs = st.executeQuery(
            "select * from test where sqrt(d) between 5 and 10");
        
        expColNames = new String [] {"I","D"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"5","25.0"},
            {"10","100.0"},
            {"-6","36.0"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            "select * from test where (i+d) between 20 and 50");
        
        expColNames = new String [] {"I","D"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"5","25.0"},
            {"-6","36.0"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            "select * from test where {fn abs (i)} between 5 and 8");
        
        expColNames = new String [] {"I","D"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"5","25.0"},
            {"-6","36.0"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
   
        rs = st.executeQuery(
            "select * from test where (i+d) not between 20 and 50");
        
        expColNames = new String [] {"I","D"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"2","4.0"},
            {"3","10.0"},
            {"4","12.0"},
            {"10","100.0"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);

        rs = st.executeQuery(
            "select * from test where sqrt(d) not between 5 and 20");
        
        expColNames = new String [] {"I","D"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"2","4.0"},
            {"3","10.0"},
            {"4","12.0"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
  
        //not (test clone() once its implemented)
        
        rs = st.executeQuery(
            "select i from t where i not between i and i");
        
        expColNames = new String [] {"I"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]{};
        
        JDBC.assertFullResultSet(rs, expRS, true);

        rs = st.executeQuery(
            "select i from t where s not between s and s");
        
        expColNames = new String [] {"I"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]{};
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            "select i from t where c not between c and c");
        
        expColNames = new String [] {"I"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]{};
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            "select i from t where v not between v and v");
        
        expColNames = new String [] {"I"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]{};
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            "select i from t where d not between d and d");
        
        expColNames = new String [] {"I"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]{};
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            "select i from t where r not between r and r");
        
        expColNames = new String [] {"I"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]{};
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            "select i from t where e not between e and e");
        
        expColNames = new String [] {"I"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]{};
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            "select i from t where t not between t and t");
        
        expColNames = new String [] {"I"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]{};
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            "select i from t where p not between p and p");
        
        expColNames = new String [] {"I"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]{};
        
        JDBC.assertFullResultSet(rs, expRS, true);
         
        // between complex expressions
        
        rs = st.executeQuery(
            "select i from t where s between (select i from s where i = 2)"
            +"and (select 100 from s where i = 2)");
        
        expColNames = new String [] {"I"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"0"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            "select * from t where i between i and (select max(i) from s)");
        
        expColNames = new String [] {"I","S","C","V","D","R","E","T","P"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"0","100","hello","everyone is here","200.0","300.0",
                     "1992-01-01","12:30:30","1992-01-01 12:30:30.0"},
            {"-1","-100","goodbye","everyone is there","-200.0","-300.0",
                     "1992-01-02","12:30:59","1992-01-02 12:30:59.0"}      
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        

        //subquery between
        
        rs = st.executeQuery(
            "select i from t where (select i from s where i = 2) between 1 and 2");
        
        expColNames = new String [] {"I"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {null},
            {"0"},
            {"-1"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        //parameters
        
        PreparedStatement q1 = prepareStatement(
                "select i from t where ? between 2 and 3");
        PreparedStatement q2 = prepareStatement(
                "select i from t where ? between ? and 3");
        PreparedStatement q3 = prepareStatement(
                "select i from t where ? between 2 and ?");
        PreparedStatement q4 = prepareStatement(
                "select i from t where i between ? and ?");
        
        rs = st.executeQuery("values (2)");
        rs.next();
//IC see: https://issues.apache.org/jira/browse/DERBY-5715
        ResultSetMetaData rsmd = rs.getMetaData();
        for (int i = 1;
                i <= rsmd.getColumnCount(); i++) {
            q1.setObject(i, rs.getObject(i));
        }
        rs = q1.executeQuery();
        expColNames = new String[]{"I"};

        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {null},
            {"0"},
            {"-1"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);

        rs = st.executeQuery("values (2, 2)");
        rs.next();
        rsmd = rs.getMetaData();
        for (int i = 1;
                i <= rsmd.getColumnCount(); i++) {
            q2.setObject(i, rs.getObject(i));
        }
        rs = q2.executeQuery();
        expColNames = new String[]{"I"};

        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {null},
            {"0"},
            {"-1"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery("values (2, 3)");
        rs.next();
        rsmd = rs.getMetaData();
        for (int i = 1;
                i <= rsmd.getColumnCount(); i++) {
            q3.setObject(i, rs.getObject(i));
        }
        rs = q3.executeQuery();
        expColNames = new String[]{"I"};

        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {null},
            {"0"},
            {"-1"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery("values (0, 1)");
        rs.next();
        rsmd = rs.getMetaData();
        for (int i = 1;
                i <= rsmd.getColumnCount(); i++) {
            q4.setObject(i, rs.getObject(i));
        }
        rs = q4.executeQuery();
        expColNames = new String[]{"I"};

        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"0"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);

        q1.close();
        q2.close();
        q3.close();
        q4.close();
      
        assertUpdateCount(st, 1, "update s set i = 5 where i between 2 and 3");

        rs = st.executeQuery(
            "select * from s");
        
        expColNames = new String [] {"I"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1"},
            {"1"},
            {"5"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);

        //delete - where clause 
        
        assertUpdateCount(st, 1, "delete from s where i between 3 and 5");

        rs = st.executeQuery(
            "select * from s");
        
        expColNames = new String [] {"I"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1"},
            {"1"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        

        //check consistency of scans, etc
        
        if(usingEmbedded()){
            rs = st.executeQuery("values ConsistencyChecker()");

            assertTrue("Consistency checker returned no data", rs.next());
            String line1 = rs.getString(1);
            assertTrue("Expected 'No open scans, etc.', not: " + line1,
                    line1.startsWith( "No open scans, etc.") );
        } 
        
       assertUpdateCount(st, 0, "drop table s"); 
        
       st.close();
    }

    public void testInList() throws SQLException {

//IC see: https://issues.apache.org/jira/browse/DERBY-5715
        Statement st = createStatement();
        createTestObjects(st);     
        
        //recreate s as ss
        
        st.executeUpdate("create table ss (i int)");
        st.executeUpdate("insert into ss values (1)");
        st.executeUpdate("insert into ss values (1)");
        st.executeUpdate("insert into ss values (2)");
            
        //negative tests
        
        //empty list  
        assertStatementError("42X01",st,
                "select i from t where i in ()");    
        //null in list
        assertStatementError("42X01",st,
                "select i from t where i in (null)");
        //cardinality violation from subquery
        //subquery is only allowed to return a single row
        assertStatementError("21000",st,
                "select i from t where i in (1, 3, 5, 6, (select i from ss))");
        //type mismatches
        //Comparisons between 'INTEGER' and 'DATE' are not supported
        assertStatementError("42818",st,
                "select i from t where i in (i, i, e)");
        //Comparisons between 'INTEGER' and 'TIME' are not supported
        assertStatementError("42818",st,
                "select i from t where i in (i, i, t)");
        //Comparisons between 'INTEGER' and 'TIMESTAMP' are not supported
        assertStatementError("42818",st,
                "select i from t where i in (i, i, p)");
        //Comparisons between 'DATE' and 'TIMESTAMP' are not supported
        assertStatementError("42818",st,
                "select i from t where e in (e, p, e)");
        //Comparisons between 'INTEGER' and 'TIMESTAMP' are not supported
        assertStatementError("42818",st,
                "select i from t where 1 in (p, 2, 1)");

        //positive tests
        //type comparisons
        
//IC see: https://issues.apache.org/jira/browse/DERBY-5715
        ResultSet rs = st.executeQuery(
            "select i from t where i in (s, r, i, d, 40e1)");
        
        String[] expColNames = {"I"};
        JDBC.assertColumnNames(rs, expColNames);
        
        String[][] expRS = {
            {"0"},
            {"-1"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            "select s from t where s in (s, r, i, d, 40e1)");
        
        expColNames = new String [] {"S"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"100"},
            {"-100"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);

        rs = st.executeQuery(
            "select r from t where r in (s, r, i, d, 40e1)");
        
        expColNames = new String [] {"R"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"300.0"},
            {"-300.0"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            "select d from t where d in (s, r, i, d, 40e1)");
        
        expColNames = new String [] {"D"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"200.0"},
            {"-200.0"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            "select i from t where 40e1 in (s, r, i, d, 40e1)");
        
        expColNames = new String [] {"I"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {null},
            {"0"},
            {"-1"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            "select c from t where c in (c, v, 'goodbye')");
        
        expColNames = new String [] {"C"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"hello"},
            {"goodbye"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            "select v from t where v in (c, v, 'goodbye')");
        
        expColNames = new String [] {"V"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"everyone is here"},
            {"everyone is there"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
       
        rs = st.executeQuery(
            "select i from t where 'goodbye' in (c, v, 'goodbye')");
        
        expColNames = new String [] {"I"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {null},
            {"0"},
            {"-1"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            "select i from t where '"+Timestamp.valueOf("1992-01-01 12:30:30.0")+
            "'in (p,'"+Timestamp.valueOf("1992-01-01 12:30:30.0")+"')");
        
        expColNames = new String [] {"I"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {null},
            {"0"},
            {"-1"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            "select p from t where p in (p, "
            +"'"+Timestamp.valueOf("1992-01-02 12:30:59")+"')");
        
        expColNames = new String [] {"P"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1992-01-01 12:30:30.0"},
            {"1992-01-02 12:30:59.0"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        // not (test clone() once its implemented)
        
        rs = st.executeQuery(
            "select i from t where i not in (i, i)");
        
        expColNames = new String [] {"I"};
        JDBC.assertColumnNames(rs, expColNames);
        JDBC.assertDrainResults(rs, 0);
        
        rs = st.executeQuery(
            " select i from t where s not in (s, s)");
        
        expColNames = new String [] {"I"};
        JDBC.assertColumnNames(rs, expColNames);
        JDBC.assertDrainResults(rs, 0);
        
        rs = st.executeQuery(
            " select i from t where c not in (c, c)");
        
        expColNames = new String [] {"I"};
        JDBC.assertColumnNames(rs, expColNames);
        JDBC.assertDrainResults(rs, 0);
        
        rs = st.executeQuery(
            " select i from t where v not in (v, v)");
        
        expColNames = new String [] {"I"};
        JDBC.assertColumnNames(rs, expColNames);
        JDBC.assertDrainResults(rs, 0);
        
        rs = st.executeQuery(
            " select i from t where d not in (d, d)");
        
        expColNames = new String [] {"I"};
        JDBC.assertColumnNames(rs, expColNames);
        JDBC.assertDrainResults(rs, 0);
        
        rs = st.executeQuery(
            " select i from t where r not in (r, r)");
        
        expColNames = new String [] {"I"};
        JDBC.assertColumnNames(rs, expColNames);
        JDBC.assertDrainResults(rs, 0);
        
        rs = st.executeQuery(
            " select i from t where e not in (e, e)");
        
        expColNames = new String [] {"I"};
        JDBC.assertColumnNames(rs, expColNames);
        JDBC.assertDrainResults(rs, 0);
        
        rs = st.executeQuery(
            " select i from t where t not in (t, t)");
        
        expColNames = new String [] {"I"};
        JDBC.assertColumnNames(rs, expColNames);
        JDBC.assertDrainResults(rs, 0);
        
        rs = st.executeQuery(
            " select i from t where p not in (p, p)");
        
        expColNames = new String [] {"I"};
        JDBC.assertColumnNames(rs, expColNames);
        JDBC.assertDrainResults(rs, 0);
        
        // more nots
        
        rs = st.executeQuery(
            "select i from t where i not in (0, 9, 8, 2, 7)");
        
        expColNames = new String [] {"I"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"-1"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select i from t where not i not in (0, 9, 8, 2, 7)");
        
        expColNames = new String [] {"I"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"0"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        // 1 element list
        
        rs = st.executeQuery(
            "select s from t where s in (100)");
        
        expColNames = new String [] {"S"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"100"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        // left side of expression
        
        rs = st.executeQuery(
            "select s from t where (s in (100))");
        
        expColNames = new String [] {"S"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"100"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        // complex expressions
        
        rs = st.executeQuery(
            "select i from t where i in (1, 3, 5, 6, (select i "
            + "from ss where i = 2) - 2)");
        
        expColNames = new String [] {"I"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"0"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select * from test where i in (sqrt(d),{fn abs (i)}, -6)");
        
        expColNames = new String [] {"I", "D"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"2", "4.0"},
            {"3", "10.0"},
            {"4", "12.0"},
            {"5", "25.0"},
            {"10", "100.0"},
            {"-6", "36.0"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select * from test where sqrt(d) in (i, 4)");
        
        expColNames = new String [] {"I", "D"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"2", "4.0"},
            {"5", "25.0"},
            {"10", "100.0"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select * from test where (i+d) in (6, 30)");
        
        expColNames = new String [] {"I", "D"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"2", "4.0"},
            {"5", "25.0"},
            {"-6", "36.0"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select * from test where sqrt(d) in (i)");
        
        expColNames = new String [] {"I", "D"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"2", "4.0"},
            {"5", "25.0"},
            {"10", "100.0"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select * from test where {fn abs (i)} in (i)");
        
        expColNames = new String [] {"I", "D"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"2", "4.0"},
            {"3", "10.0"},
            {"4", "12.0"},
            {"5", "25.0"},
            {"10", "100.0"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select * from test where {fn abs (i)} not in (i)");
        
        expColNames = new String [] {"I", "D"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"-6", "36.0"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select * from test where (i+d) not in (6, 30)");
        
        expColNames = new String [] {"I", "D"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"3", "10.0"},
            {"4", "12.0"},
            {"10", "100.0"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select * from test where sqrt(d) not in (5, 10, 2)");
        
        expColNames = new String [] {"I", "D"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"3", "10.0"},
            {"4", "12.0"},
            {"-6", "36.0"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        // subquery in
        
        rs = st.executeQuery(
            "select i from t where (select i from ss where i = "
            + "2) in (1, 2)");
        
        expColNames = new String [] {"I"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {null},
            {"0"},
            {"-1"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        // derived table
        
        rs = st.executeQuery(
            "select * from (select * from t "
            + "where i in (1, 3, 5, 6, (select i from ss where i = "
            + "2) - 2)) a");
        
        expColNames = new String [] {"I", "S", "C", "V", "D", "R", "E", "T", "P"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"0", "100", "hello", "everyone is here", "200.0", "300.0", 
                     "1992-01-01", "12:30:30", "1992-01-01 12:30:30.0"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        assertUpdateCount(st, 1,
            " update ss set i = 5 where i in (2, 3, 40e1)");
        
        rs = st.executeQuery(
            " select * from ss");
        
        expColNames = new String [] {"I"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1"},
            {"1"},
            {"5"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        // delete - where clause
        
        assertUpdateCount(st, 2,
            "delete from ss where i not in (5, 3)");
        
        rs = st.executeQuery(
            " select * from ss");
        
        expColNames = new String [] {"I"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"5"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        st.close();
    }

    public void testInBetween() throws SQLException {

//IC see: https://issues.apache.org/jira/browse/DERBY-5715
        Statement st = createStatement();
        createTestObjects(st);
        
        st.executeUpdate("create table u (c1 integer)");      
        st.executeUpdate(" insert into u values null");       
        st.executeUpdate(" insert into u values 1");        
        st.executeUpdate(" insert into u values null");     
        st.executeUpdate(" insert into u values 2");
        
//IC see: https://issues.apache.org/jira/browse/DERBY-5715
        ResultSet rs = st.executeQuery(
            " select * from u where c1 between 2 and 3");
        
        String[] expColNames = {"C1"};
        JDBC.assertColumnNames(rs, expColNames);
        JDBC.assertSingleValueResultSet(rs, "2");
        
        rs = st.executeQuery(
            " select * from u where c1 in (2, 3, 0, 1)");
        
        expColNames = new String [] {"C1"};
        JDBC.assertColumnNames(rs, expColNames);
        
        String[][] expRS = {
            {"1"},
            {"2"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        // add some more rows before testing static in list xform
        
        st.executeUpdate(
            "insert into t values (20, 200, 'maybe', 'noone is "
            + "here', 800.0e0,"
            + "	1000.0e0, '1892-01-01', '07:30:30', "
            + "'"+Timestamp.valueOf("1892-01-01 07:30:30")+"')");
        
        st.executeUpdate(
            " insert into t values (-50, -200, 'never', 'noone "
            + "is there', -800.0e0,"
            + "	-10300.0e0, '2992-01-02', '19:30:59', "
            + "'"+Timestamp.valueOf("2992-01-02 19:30:59")+"')");
        
        // test the static in list xform for the various types
        
        rs = st.executeQuery(
            "select i from t");
        
        expColNames = new String [] {"I"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {null},
            {"0"},
            {"-1"},
            {"20"},
            {"-50"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select i from t where i in (80, 20, -60, -1)");
        
        expColNames = new String [] {"I"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"-1"},
            {"20"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select s from t");
        
        expColNames = new String [] {"S"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {null},
            {"100"},
            {"-100"},
            {"200"},
            {"-200"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select s from t where s in (100, -200, -400)");
        
        expColNames = new String [] {"S"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"100"},
            {"-200"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select c from t");
        
        expColNames = new String [] {"C"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {null},
            {"hello"},
            {"goodbye"},
            {"maybe"},
            {"never"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select c from t where c in ('a', 'goodbye', '')");
        
        expColNames = new String [] {"C"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"goodbye"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select v from t");
        
        expColNames = new String [] {"V"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {null},
            {"everyone is here"},
            {"everyone is there"},
            {"noone is here"},
            {"noone is there"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select v from t where v in ('noone is there', "
            + "'everyone is here', '')");
        
        expColNames = new String [] {"V"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"everyone is here"},
            {"noone is there"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select d from t");
        
        expColNames = new String [] {"D"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {null},
            {"200.0"},
            {"-200.0"},
            {"800.0"},
            {"-800.0"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select d from t where d in (200, -800)");
        
        expColNames = new String [] {"D"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"200.0"},
            {"-800.0"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select r from t");
        
        expColNames = new String [] {"R"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {null},
            {"300.0"},
            {"-300.0"},
            {"1000.0"},
            {"-10300.0"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select r from t where r in (300.0, -10300.0)");
        
        expColNames = new String [] {"R"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"300.0"},
            {"-10300.0"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select e from t");
        
        expColNames = new String [] {"E"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {null},
            {"1992-01-01"},
            {"1992-01-02"},
            {"1892-01-01"},
            {"2992-01-02"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select e from t where e in ('2992-01-02', "
            + "'3999-08-08', '1992-01-02')");
        
        expColNames = new String [] {"E"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1992-01-02"},
            {"2992-01-02"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select t from t");
        
        expColNames = new String [] {"T"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {null},
            {"12:30:30"},
            {"12:30:59"},
            {"07:30:30"},
            {"19:30:59"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select t from t where t in ('12:30:58', "
            + "'07:20:20', '07:30:30')");
        
        expColNames = new String [] {"T"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"07:30:30"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        // verify that added predicates getting pushed down
        
        rs = st.executeQuery(
            "select p from t");
        
        expColNames = new String [] {"P"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {null},
            {"1992-01-01 12:30:30.0"},
            {"1992-01-02 12:30:59.0"},
            {"1892-01-01 07:30:30.0"},
            {"2992-01-02 19:30:59.0"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select p from t where p in "
            + "('"+Timestamp.valueOf("1992-01-02 12:30:59")+"',"
            + "'"+Timestamp.valueOf("1992-01-02 12:35:59")+"',"
            + "'"+Timestamp.valueOf("1992-05-02 12:38:59")+"')");
        
        expColNames = new String [] {"P"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1992-01-02 12:30:59.0"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        CallableStatement cSt = prepareCall(
            " call SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1)");
        assertUpdateCount(cSt, 0);
        
        rs=st.executeQuery("SELECT R FROM t");
        while(rs.next()){}
        rs.close();
        
        rs = st.executeQuery(
            " values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS()");
        rs.next();

        if(usingEmbedded()){
            RuntimeStatisticsParser rtsp = new RuntimeStatisticsParser(rs.getString(1));
            rs.close();
            assertTrue(rtsp.usedTableScan());
            assertFalse(rtsp.usedDistinctScan());
        }
        
        st.close();
    }

    public void testBigInList() throws SQLException {
        // big in lists (test binary search)
        
//IC see: https://issues.apache.org/jira/browse/DERBY-5715
        Statement st = createStatement();
        createTestObjects(st);                
        
        ResultSet rs = st.executeQuery(
            " select * from big where i in (1, 3, 5, 7, 9, 11, "
            + "13, 15, 17, 19, 21, 23, 25, 27, 29, 31)");
        
        String[] expColNames = {"I", "C"};
        JDBC.assertColumnNames(rs, expColNames);
        
        String[][] expRS = {
            {"1", "1"},
            {"3", "3"},
            {"5", "5"},
            {"7", "7"},
            {"9", "9"},
            {"11", "11"},
            {"13", "13"},
            {"15", "15"},
            {"17", "17"},
            {"19", "19"},
            {"21", "21"},
            {"23", "23"},
            {"25", "25"},
            {"27", "27"},
            {"29", "29"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select * from big where i in (31, 32, 5, 7, 9, 11, "
            + "13, 15, 17, 19, 21, 23, 25, 27, 29, 1)");
        
        expColNames = new String [] {"I", "C"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1", "1"},
            {"5", "5"},
            {"7", "7"},
            {"9", "9"},
            {"11", "11"},
            {"13", "13"},
            {"15", "15"},
            {"17", "17"},
            {"19", "19"},
            {"21", "21"},
            {"23", "23"},
            {"25", "25"},
            {"27", "27"},
            {"29", "29"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select * from big where i in (1, 5, 7, 9, 11, 13, "
            + "15, 17, 19, 21, 23, 25, 27, 29, 31)");
        
        expColNames = new String [] {"I", "C"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1", "1"},
            {"5", "5"},
            {"7", "7"},
            {"9", "9"},
            {"11", "11"},
            {"13", "13"},
            {"15", "15"},
            {"17", "17"},
            {"19", "19"},
            {"21", "21"},
            {"23", "23"},
            {"25", "25"},
            {"27", "27"},
            {"29", "29"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select * from big where i in (1, 5, 7, 9, 13, 15, "
            + "17, 19, 21, 23, 25, 27, 29, 31)");
        
        expColNames = new String [] {"I", "C"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1", "1"},
            {"5", "5"},
            {"7", "7"},
            {"9", "9"},
            {"13", "13"},
            {"15", "15"},
            {"17", "17"},
            {"19", "19"},
            {"21", "21"},
            {"23", "23"},
            {"25", "25"},
            {"27", "27"},
            {"29", "29"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select * from big where i in (1, 5, 7, 9, 13, 15, "
            + "17, 19, 23, 25, 27, 29, 31)");
        
        expColNames = new String [] {"I", "C"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1", "1"},
            {"5", "5"},
            {"7", "7"},
            {"9", "9"},
            {"13", "13"},
            {"15", "15"},
            {"17", "17"},
            {"19", "19"},
            {"23", "23"},
            {"25", "25"},
            {"27", "27"},
            {"29", "29"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select * from big where i in (3, 3, 3, 3)");
        
        expColNames = new String [] {"I", "C"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"3", "3"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select * from big where i in (4, 4, 4, 4)");
        
        expColNames = new String [] {"I", "C"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"4", "4"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        //Comparisons between 'CHAR (UCS_BASIC)' and 'INTEGER' are not supported
        
        assertStatementError("42818", st,
            " select * from big where c in (1, 3, 5, 7, 9, 11, "
            + "13, 15, 17, 19, 21, 23, 25, 27, 29, 31)");
        
        assertStatementError("42818", st,
            " select * from big where c in (31, 32, 5, 7, 9, 11, "
            + "13, 15, 17, 19, 21, 23, 25, 27, 29, 1)");
        
        assertStatementError("42818", st,
            " select * from big where c in (1, 5, 7, 9, 11, 13, "
            + "15, 17, 19, 21, 23, 25, 27, 29, 31)");
        
        assertStatementError("42818", st,
            " select * from big where c in (1, 5, 7, 9, 13, 15, "
            + "17, 19, 21, 23, 25, 27, 29, 31)");
        
        rs = st.executeQuery(
            " select * from big where c in ('1', '5', '7', '9', "
            + "'13', '15', '17', '19', '21', '23', '25', '27', '29', '31')");
        
        expColNames = new String [] {"I", "C"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1", "1"},
            {"5", "5"},
            {"7", "7"},
            {"9", "9"},
            {"13", "13"},
            {"15", "15"},
            {"17", "17"},
            {"19", "19"},
            {"21", "21"},
            {"23", "23"},
            {"25", "25"},
            {"27", "27"},
            {"29", "29"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select * from big where i in (1, 5, 7, 9, 13, 15, "
            + "17, 19, 23, 25, 27, 29, 31)");
        
        expColNames = new String [] {"I", "C"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1", "1"},
            {"5", "5"},
            {"7", "7"},
            {"9", "9"},
            {"13", "13"},
            {"15", "15"},
            {"17", "17"},
            {"19", "19"},
            {"23", "23"},
            {"25", "25"},
            {"27", "27"},
            {"29", "29"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        // check consistency of scans, etc.
        if(usingEmbedded()){
            rs = st.executeQuery("values ConsistencyChecker()");

            assertTrue("Consistency checker returned no data", rs.next());
            String line1 = rs.getString(1);
            assertTrue("Expected 'No open scans, etc.', not: " + line1,
                    line1.startsWith( "No open scans, etc.") );
        }      
        
        st.close();
    }
     
    public void testCheckQueries() throws SQLException{
        // Check various queries for which left column is part of 
        // an index.
        
//IC see: https://issues.apache.org/jira/browse/DERBY-5715
        Statement st = createStatement();
        createTestObjects(st);      
                
        // Simple cases, small table with index on IN col.
        
        ResultSet rs = st.executeQuery(
            "select * from bt1 where i in (9, 2, 8)");
        
        String[] expColNames = {"I", "C", "DE"};
        JDBC.assertColumnNames(rs, expColNames);
        
//IC see: https://issues.apache.org/jira/browse/DERBY-5715
        String[][] expRS = {
            {"2", "two", "22.2"},
            {"8", "eight", "2.8"},
            {"9", "nine", null}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select i from bt1 where i in (9, 2, 8)");
        
        expColNames = new String [] {"I"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"2"},
            {"8"},
            {"9"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        // Simple cases, small table, IN col is part of index but 
        // is not a leading column.
        
        rs = st.executeQuery(
            "select * from bt1 where c in ('a', 'two', 'three')");
        
        expColNames = new String [] {"I", "C", "DE"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"2", "two", "22.2"},
            {"3", "three", null}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select c from bt1 where c in ('a', 'two', 'three')");
        
        expColNames = new String [] {"C"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"two"},
            {"three"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        // Multiple rows matching a single IN value; make sure we 
        // get two rows for "3".
        
        rs = st.executeQuery(
            "select * from bt1 where i in (1, 2, 3)");
        
        expColNames = new String [] {"I", "C", "DE"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1", "one", null},
            {"2", "two", "22.2"},
            {"3", "three", null},
            {"3", "trois", "21.2"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select * from bt1 where i in (8, 3)");
        
        expColNames = new String [] {"I", "C", "DE"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"3", "three", null},
            {"3", "trois", "21.2"},
            {"8", "eight", "2.8"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select i from bt1 where i in (8, 3) order by i");
        
        expColNames = new String [] {"I"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"3"},
            {"3"},
            {"8"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select * from bt1 where i in (8, 3) order by i");
        
        expColNames = new String [] {"I", "C", "DE"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"3", "three", null},
            {"3", "trois", "21.2"},
            {"8", "eight", "2.8"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        // No row for minimum value; make sure we still get the rest.
        
        rs = st.executeQuery(
            "select * from bt1 where i in (-1, 1, 2, 3)");
        
        expColNames = new String [] {"I", "C", "DE"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1", "one", null},
            {"2", "two", "22.2"},
            {"3", "three", null},
            {"3", "trois", "21.2"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select * from bt1 where i in (0, 1, 2, 3)");
        
        expColNames = new String [] {"I", "C", "DE"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1", "one", null},
            {"2", "two", "22.2"},
            {"3", "three", null},
            {"3", "trois", "21.2"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select * from bt1 where i in (1, 2, -1, 3)");
        
        expColNames = new String [] {"I", "C", "DE"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1", "one", null},
            {"2", "two", "22.2"},
            {"3", "three", null},
            {"3", "trois", "21.2"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        // Various examples with larger table and multiple IN 
        // lists on same column in single table.
        
        rs = st.executeQuery(
            "select * from big where i in (1, 2)");
        
        expColNames = new String [] {"I", "C"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1", "1"},
            {"2", "2"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select * from big where i in (1, 30)");
        
        expColNames = new String [] {"I", "C"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1", "1"},
            {"30", "30"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select * from big where i in (1, 30) and i = 1");
        
        expColNames = new String [] {"I", "C"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1", "1"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select * from big where i in (1, 30) or i in (2, 29)");
        
        expColNames = new String [] {"I", "C"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1", "1"},
            {"2", "2"},
            {"29", "29"},
            {"30", "30"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select * from big where i in (1, 30) and i in (1, 2, 29)");
        
        expColNames = new String [] {"I", "C"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1", "1"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select * from big where i in (1, 30) and i in (1, "
            + "2, 29, 30)");
        
        expColNames = new String [] {"I", "C"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1", "1"},
            {"30", "30"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select * from big where i in (1, 2, 29, 30) and i "
            + "in (1, 30)");
        
        expColNames = new String [] {"I", "C"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1", "1"},
            {"30", "30"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select * from big where i in (1, 30) and (i = 30 or i = 1)");
        
        expColNames = new String [] {"I", "C"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1", "1"},
            {"30", "30"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select * from big where i in (1, 30) and (i = 30 or i = 2)");
        
        expColNames = new String [] {"I", "C"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"30", "30"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        // Multiple IN lists on different tables, plus join predicate.
        
        rs = st.executeQuery(
            "select count(*) from big, bt1 where big.i in (1, 3, "
            + "30) or bt1.i in (-1, 2, 3) and big.i = bt1.i");
        
        expColNames = new String [] {"1"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"55"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select * from big, bt1 where (big.i in (1, 3, 30) "
            + "or bt1.i in (-1, 2, 3)) and big.i = bt1.i");
        
        expColNames = new String [] {"I", "C", "I", "C", "DE"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1", "1", "1", "one", null},
            {"2", "2", "2", "two", "22.2"},
            {"3", "3", "3", "three", null},
            {"3", "3", "3", "trois", "21.2"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select * from big, bt1 where big.i in (1, 3, 30) "
            + "and bt1.i in (-1, 2, 3) and big.i = bt1.i");
        
        expColNames = new String [] {"I", "C", "I", "C", "DE"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"3", "3", "3", "three", null},
            {"3", "3", "3", "trois", "21.2"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select * from big, bt1 where big.i in (1, 3, 30) "
            + "and bt1.i in (2, 3) and big.i = bt1.i");
        
        expColNames = new String [] {"I", "C", "I", "C", "DE"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"3", "3", "3", "three", null},
            {"3", "3", "3", "trois", "21.2"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        // Multiple IN lists for different cols in same table; 
        // we'll only use one as a "probe predicate"; the other 
        // ones should be enforced as regular restrictions.
        
        rs = st.executeQuery(
            "select * from bt1 where i in (2, 4, 6, 8) and de in "
            + "(22.3, 2.8) and c in ('seven', 'eight', 'nine')");
        
        expColNames = new String [] {"I", "C", "DE"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"8", "eight", "2.8"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        // Multiple IN lists on different tables, no join 
        // predicate, count only.
        
        rs = st.executeQuery(
            "select count(*) from big, bt1 where big.i in (1, 3, "
            + "30) or bt1.i in (-1, 2, 3)");
        
        expColNames = new String [] {"1"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"135"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select count(*) from big, bt1 where big.i in (1, "
            + "3, 30) and bt1.i in (-1, 2, 3)");
        
        expColNames = new String [] {"1"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"9"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select count(*) from big, bt1 where big.i in (1, "
            + "3, 30) and bt1.i in (2, 3)");
        
        expColNames = new String [] {"1"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"9"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select count(*) from big b1, big b2 where b1.i in "
            + "(1, 3, 30) and b2.i in (2, 3)");
        
        expColNames = new String [] {"1"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"6"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select count(*) from big b1, big b2 where b1.i in "
            + "(1, 3, 30) and b2.i in (-1,2, 3)");
        
        expColNames = new String [] {"1"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"6"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        // Multiple IN lists on different tables, no join 
        // predicate, show rows.
        
        rs = st.executeQuery(
            "select * from big, bt1 where big.i in (1, 3, 30) "
            + "and bt1.i in (-1, 2, 3) order by big.i, bt1.c");
        
        expColNames = new String [] {"I", "C", "I", "C", "DE"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1", "1", "3", "three", null},
            {"1", "1", "3", "trois", "21.2"},
            {"1", "1", "2", "two", "22.2"},
            {"3", "3", "3", "three", null},
            {"3", "3", "3", "trois", "21.2"},
            {"3", "3", "2", "two", "22.2"},
            {"30", "30", "3", "three", null},
            {"30", "30", "3", "trois", "21.2"},
            {"30", "30", "2", "two", "22.2"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select * from big, bt1 where big.i in (1, 3, 30) "
            + "and bt1.i in (2, 3) order by big.i, bt1.c");
        
        expColNames = new String [] {"I", "C", "I", "C", "DE"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1", "1", "3", "three", null},
            {"1", "1", "3", "trois", "21.2"},
            {"1", "1", "2", "two", "22.2"},
            {"3", "3", "3", "three", null},
            {"3", "3", "3", "trois", "21.2"},
            {"3", "3", "2", "two", "22.2"},
            {"30", "30", "3", "three", null},
            {"30", "30", "3", "trois", "21.2"},
            {"30", "30", "2", "two", "22.2"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select * from big b1, big b2 where b1.i in (1, 3, "
            + "30) and b2.i in (2, 3) order by b1.i, b2.i");
        
        expColNames = new String [] {"I", "C", "I", "C"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1", "1", "2", "2"},
            {"1", "1", "3", "3"},
            {"3", "3", "2", "2"},
            {"3", "3", "3", "3"},
            {"30", "30", "2", "2"},
            {"30", "30", "3", "3"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select * from big b1, big b2 where b1.i in (1, 3, "
            + "30) and b2.i in (-1,2, 3) order by b1.i, b2.i");
        
        expColNames = new String [] {"I", "C", "I", "C"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1", "1", "2", "2"},
            {"1", "1", "3", "3"},
            {"3", "3", "2", "2"},
            {"3", "3", "3", "3"},
            {"30", "30", "2", "2"},
            {"30", "30", "3", "3"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        // IN lists with ORDER BY.
        
        rs = st.executeQuery(
            "select * from bt1 where i in (1, 8, 3, 3) order by i");
        
        expColNames = new String [] {"I", "C", "DE"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1", "one", null},
            {"3", "three", null},
            {"3", "trois", "21.2"},
            {"8", "eight", "2.8"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select * from bt1 where i in (1, 8, 3, 3) order by i desc");
        
        expColNames = new String [] {"I", "C", "DE"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"8", "eight", "2.8"},
            {"3", "trois", "21.2"},
            {"3", "three", null},
            {"1", "one", null}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select i from bt1 where i in (1, 29, 8, 3, 3) order by i");
        
        expColNames = new String [] {"I"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1"},
            {"3"},
            {"3"},
            {"8"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select i from bt1 where i in (1, 29, 8, 3, 3) "
            + "order by i desc");
        
        expColNames = new String [] {"I"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"8"},
            {"3"},
            {"3"},
            {"1"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select i from bt1 where i in (1, 8, 3, 3, 4, 5, 6, "
            + "7, 8, 9, 0) order by i");
        
        expColNames = new String [] {"I"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1"},
            {"3"},
            {"3"},
            {"7"},
            {"8"},
            {"9"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select c from bt1 where c in ('abc', 'de', 'fg', "
            + "'two', 'or', 'not', 'one', 'thre', 'zour', 'three') "
            + "order by c");
        
        expColNames = new String [] {"C"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"one"},
            {"three"},
            {"two"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select i from big where i in (1, 29, 3, 8) order by i");
        
        expColNames = new String [] {"I"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1"},
            {"3"},
            {"8"},
            {"29"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select i from big where i in (1, 29, 3, 8) order by i desc");
        
        expColNames = new String [] {"I"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"29"},
            {"8"},
            {"3"},
            {"1"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        // Prepared statement checks. Mix of constants and params.
        
//IC see: https://issues.apache.org/jira/browse/DERBY-5715
        PreparedStatement pSt = prepareStatement(
            "select * from bt1 where i in (1, 8, 3, ?) order by i, c");
        
        rs = st.executeQuery(
            "values 3");
        
        rs.next();
        ResultSetMetaData rsmd = rs.getMetaData();
        for (int i = 1; i <= rsmd.getColumnCount(); i++)
            pSt.setObject(i, rs.getObject(i));
        
        rs = pSt.executeQuery();
        expColNames = new String [] {"I", "C", "DE"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1", "one", null},
            {"3", "three", null},
            {"3", "trois", "21.2"},
            {"8", "eight", "2.8"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        pSt.close();
        
        pSt = prepareStatement(
            "select * from big where i in (1, ?, 30)");
        
        rs = st.executeQuery(
            "values (2)");
        
        rs.next();
        rsmd = rs.getMetaData();
        for (int i = 1; i <= rsmd.getColumnCount(); i++)
            pSt.setObject(i, rs.getObject(i));
        
        rs = pSt.executeQuery();
        expColNames = new String [] {"I", "C"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1", "1"},
            {"2", "2"},
            {"30", "30"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        // Execute statement more than once to make sure params 
        // are correctly assigned in subsequent executions.
        
        pSt = prepareStatement(
            "select i from bt1 where i in (?, 9, ?) order by i desc");
        
        rs = st.executeQuery(
            "values (5, 2)");
        
        rs.next();
        rsmd = rs.getMetaData();
        for (int i = 1; i <= rsmd.getColumnCount(); i++)
            pSt.setObject(i, rs.getObject(i));
        
        rs = pSt.executeQuery();
        expColNames = new String [] {"I"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"9"},
            {"2"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            "values (3, 2)");
        
        rs.next();
        rsmd = rs.getMetaData();
        for (int i = 1; i <= rsmd.getColumnCount(); i++)
            pSt.setObject(i, rs.getObject(i));
        
        rs = pSt.executeQuery();
        expColNames = new String [] {"I"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"9"},
            {"3"},
            {"3"},
            {"2"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            "values (3, 3)");
        
        rs.next();
        rsmd = rs.getMetaData();
        for (int i = 1; i <= rsmd.getColumnCount(); i++)
            pSt.setObject(i, rs.getObject(i));
        
        rs = pSt.executeQuery();
        expColNames = new String [] {"I"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"9"},
            {"3"},
            {"3"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        pSt = prepareStatement(
            "select i from bt1 where i in (?, ?, 1)");
        
        rs = st.executeQuery(
            "values (4, 3)");
        
        rs.next();
        rsmd = rs.getMetaData();
        for (int i = 1; i <= rsmd.getColumnCount(); i++)
            pSt.setObject(i, rs.getObject(i));
        
        rs = pSt.executeQuery();
        expColNames = new String [] {"I"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1"},
            {"3"},
            {"3"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        pSt = prepareStatement(
            "select * from bt1 where i in (?, ?, 1)");
        
        rs = st.executeQuery(
            "values (4, 3)");
        
        rs.next();
        rsmd = rs.getMetaData();
        for (int i = 1; i <= rsmd.getColumnCount(); i++)
            pSt.setObject(i, rs.getObject(i));
        
        rs = pSt.executeQuery();
        expColNames = new String [] {"I", "C", "DE"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1", "one", null},
            {"3", "three", null},
            {"3", "trois", "21.2"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            "values (34, 39)");
        
        rs.next();
        rsmd = rs.getMetaData();
        for (int i = 1; i <= rsmd.getColumnCount(); i++)
            pSt.setObject(i, rs.getObject(i));
        
        rs = pSt.executeQuery();
        expColNames = new String [] {"I", "C", "DE"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1", "one", null}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        // Null as a parameter.
            
        rs = st.executeQuery("values (3, cast (null as int))");
        
        rs.next();
        rsmd = rs.getMetaData();
        for (int i = 1;i <= rsmd.getColumnCount(); i++) {
             pSt.setObject(i, rs.getObject(i), Types.INTEGER);
        }

        rs = pSt.executeQuery();
        expColNames = new String [] {"I", "C", "DE"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1", "one", null},
            {"3", "three", null},
            {"3", "trois", "21.2"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        // Multiple IN lists, one with constants, other with 
        // parameter.
        
        pSt = prepareStatement(
            "select * from big, bt1 where big.i in (1, 3, 30) "
            + "and bt1.i in (?, 2, 3) and big.i = bt1.i");
        
        rs = st.executeQuery(
            "values -1");
        
        rs.next();
        rsmd = rs.getMetaData();
        for (int i = 1; i <= rsmd.getColumnCount(); i++)
            pSt.setObject(i, rs.getObject(i));
        
        rs = pSt.executeQuery();
        expColNames = new String [] {"I", "C", "I", "C", "DE"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"3", "3", "3", "three", null},
            {"3", "3", "3", "trois", "21.2"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            "values 1");
        
        rs.next();
        rsmd = rs.getMetaData();
        for (int i = 1; i <= rsmd.getColumnCount(); i++)
            pSt.setObject(i, rs.getObject(i));
        
        rs = pSt.executeQuery();
        expColNames = new String [] {"I", "C", "I", "C", "DE"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1", "1", "1", "one", null},
            {"3", "3", "3", "three", null},
            {"3", "3", "3", "trois", "21.2"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        // Only parameter markers (no constants).
        
        pSt = prepareStatement(
            "select * from bt1 where i in (?, ?)");
        
        rs = st.executeQuery(
            "values (2, 4)");
        
        rs.next();
        rsmd = rs.getMetaData();
        for (int i = 1; i <= rsmd.getColumnCount(); i++)
            pSt.setObject(i, rs.getObject(i));
        
        rs = pSt.executeQuery();
        expColNames = new String [] {"I", "C", "DE"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"2", "two", "22.2"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            "values (-2, -4)");
        
        rs.next();
        rsmd = rs.getMetaData();
        for (int i = 1; i <= rsmd.getColumnCount(); i++)
            pSt.setObject(i, rs.getObject(i));
        
        rs = pSt.executeQuery();
        expColNames = new String [] {"I", "C", "DE"};
        JDBC.assertColumnNames(rs, expColNames);
        JDBC.assertDrainResults(rs, 0);
        
        pSt = prepareStatement(
            "select * from bt1 where c in (?, ?, ?)");
        
        rs = st.executeQuery(
            "values ('one', 'two', 'a')");
        
        rs.next();
        rsmd = rs.getMetaData();
        for (int i = 1; i <= rsmd.getColumnCount(); i++)
            pSt.setObject(i, rs.getObject(i));
        
        rs = pSt.executeQuery();
        expColNames = new String [] {"I", "C", "DE"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1", "one", null},
            {"2", "two", "22.2"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        // Should work with UPDATE statements as well.
        
        assertUpdateCount(st, 2,
            "update bt1 set i = 22 where i in (2, 9)");
        
        rs = st.executeQuery(
            " select * from bt1");
        
        expColNames = new String [] {"I", "C", "DE"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1", "one", null},
            {"22", "two", "22.2"},
            {"3", "three", null},
            {"7", "seven", null},
            {"8", "eight", "2.8"},
            {"22", "nine", null},
            {"3", "trois", "21.2"},
            {"10", "10", "3.5"},
            {"11", "11", null},
            {"12", "12", "4.2"},
            {"13", "13", null},
            {"14", "14", "5.0"},
            {"15", "15", null},
            {"16", "16", "5.7"},
            {"17", "17", null},
            {"18", "18", "6.4"},
            {"19", "19", null},
            {"20", "20", "7.1"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        assertUpdateCount(st, 1,
            " update bt1 set i = 2 where c in ('two')");
        
        assertUpdateCount(st, 1,
            " update bt1 set i = 9 where c  in ('nine')");
        
        rs = st.executeQuery(
            " select * from bt1");
        
        expColNames = new String [] {"I", "C", "DE"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1", "one", null},
            {"2", "two", "22.2"},
            {"3", "three", null},
            {"7", "seven", null},
            {"8", "eight", "2.8"},
            {"9", "nine", null},
            {"3", "trois", "21.2"},
            {"10", "10", "3.5"},
            {"11", "11", null},
            {"12", "12", "4.2"},
            {"13", "13", null},
            {"14", "14", "5.0"},
            {"15", "15", null},
            {"16", "16", "5.7"},
            {"17", "17", null},
            {"18", "18", "6.4"},
            {"19", "19", null},
            {"20", "20", "7.1"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        pSt = prepareStatement(
            "update bt1 set i = 22 where i in (?, ?, ?, ?, ?)");
        
        rs = st.executeQuery(
            "values (-1, 2, 9, 41, 3)");
        
        rs.next();
        rsmd = rs.getMetaData();
        for (int i = 1; i <= rsmd.getColumnCount(); i++)
            pSt.setObject(i, rs.getObject(i));
        
        assertUpdateCount(pSt, 4);
        
        rs = st.executeQuery(
            " select * from bt1");
        
        expColNames = new String [] {"I", "C", "DE"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1", "one", null},
            {"22", "two", "22.2"},
            {"22", "three", null},
            {"7", "seven", null},
            {"8", "eight", "2.8"},
            {"22", "nine", null},
            {"22", "trois", "21.2"},
            {"10", "10", "3.5"},
            {"11", "11", null},
            {"12", "12", "4.2"},
            {"13", "13", null},
            {"14", "14", "5.0"},
            {"15", "15", null},
            {"16", "16", "5.7"},
            {"17", "17", null},
            {"18", "18", "6.4"},
            {"19", "19", null},
            {"20", "20", "7.1"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        assertUpdateCount(st, 1,
            " update bt1 set i = 2 where c in ('two')");
        
        assertUpdateCount(st, 1,
            " update bt1 set i = 9 where c in ('nine')");
        
        assertUpdateCount(st, 1,
            " update bt1 set i = 3 where c in ('three')");
        
        assertUpdateCount(st, 1,
            " update bt1 set i = 3 where c in ('trois')");
        
        rs = st.executeQuery(
            " select * from bt1");
        
        expColNames = new String [] {"I", "C", "DE"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1", "one", null},
            {"2", "two", "22.2"},
            {"3", "three", null},
            {"7", "seven", null},
            {"8", "eight", "2.8"},
            {"9", "nine", null},
            {"3", "trois", "21.2"},
            {"10", "10", "3.5"},
            {"11", "11", null},
            {"12", "12", "4.2"},
            {"13", "13", null},
            {"14", "14", "5.0"},
            {"15", "15", null},
            {"16", "16", "5.7"},
            {"17", "17", null},
            {"18", "18", "6.4"},
            {"19", "19", null},
            {"20", "20", "7.1"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        // Different (but compatible) types within IN list.
        
        rs = st.executeQuery(
            "select * from bt1 where de in (2.8, 2000.32)");
        
        expColNames = new String [] {"I", "C", "DE"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"8", "eight", "2.8"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select * from bt1 where de in (28, 21892)");
        
        expColNames = new String [] {"I", "C", "DE"};
        JDBC.assertColumnNames(rs, expColNames);
        JDBC.assertDrainResults(rs, 0);
        
        rs = st.executeQuery(
            " select * from bt1 where de in (2.8, 1249102)");
        
        expColNames = new String [] {"I", "C", "DE"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"8", "eight", "2.8"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select * from bt1 where de in (cast (28 as "
            + "decimal(3,1)), 1249102)");
        
        expColNames = new String [] {"I", "C", "DE"};
        JDBC.assertColumnNames(rs, expColNames);
        JDBC.assertDrainResults(rs, 0);
        
        rs = st.executeQuery(
            " select * from bt1 where de in (values (cast (null "
            + "as double)), 2.8, 1249102)");
        
        expColNames = new String [] {"I", "C", "DE"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"8", "eight", "2.8"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        // Different (but compatible) types: leftOp vs IN list.
        
        rs = st.executeQuery(
            "select * from bt1 where i in (2.8, 4.23)");
        
        expColNames = new String [] {"I", "C", "DE"};
        JDBC.assertColumnNames(rs, expColNames);
        JDBC.assertDrainResults(rs, 0);
        
        rs = st.executeQuery(
            " select d from bt2 where d in (200, -800)");
        
        expColNames = new String [] {"D"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"-800.0"},
            {"200.0"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select da from bt2 where da in ('2992-01-02', "
            + "'3999-08-08', '1992-01-02')");
        
        expColNames = new String [] {"DA"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"3999-08-08"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select t, vc from bt2 where vc in (cast "
            + "('2992-01-02' as date), cast ('1997-03-22' as date))");
        
        expColNames = new String [] {"T", "VC"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"03:22:28", "2992-01-02"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select t, vc from bt2 where vc in "
            + "(date('2992-01-02'), date('1997-03-22'))");
        
        expColNames = new String [] {"T", "VC"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"03:22:28", "2992-01-02"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select t, vc from bt2 where vc in ('2992-01-02', "
            + "cast ('1997-03-22' as date))");
        
        expColNames = new String [] {"T", "VC"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"03:22:28", "2992-01-02"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select t, vc from bt2 where vc in (cast "
            + "('2992-01-02' as date), '1997-03-22')");
        
        expColNames = new String [] {"T", "VC"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"03:22:28", "2992-01-02"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        // Duplicate IN-list values.  Should *not* see duplicate rows.
        
        rs = st.executeQuery(
            "select * from bt1 where i in (2, 2, 2, 3)");
        
        expColNames = new String [] {"I", "C", "DE"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"2", "two", "22.2"},
            {"3", "three", null},
            {"3", "trois", "21.2"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select i from bt1 where i in (2, 2, 2, 3)");
        
        expColNames = new String [] {"I"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"2"},
            {"3"},
            {"3"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select * from bt1 where i in (1, 8, 3, 3)");
        
        expColNames = new String [] {"I", "C", "DE"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1", "one", null},
            {"3", "three", null},
            {"3", "trois", "21.2"},
            {"8", "eight", "2.8"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select i from bt1 where i in (1, 29, 8, 3, 3)");
        
        expColNames = new String [] {"I"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1"},
            {"3"},
            {"3"},
            {"8"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        pSt = prepareStatement(
            "select * from bt1 where i in (2, ?, ?, 2)");
        
        rs = st.executeQuery(
            "values (4, -1)");
        
        rs.next();
        rsmd = rs.getMetaData();
        for (int i = 1; i <= rsmd.getColumnCount(); i++)
            pSt.setObject(i, rs.getObject(i));
        
        rs = pSt.executeQuery();
        expColNames = new String [] {"I", "C", "DE"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"2", "two", "22.2"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            "values (4, 3)");
        
        rs.next();
        rsmd = rs.getMetaData();
        for (int i = 1; i <= rsmd.getColumnCount(); i++)
            pSt.setObject(i, rs.getObject(i));
        
        rs = pSt.executeQuery();
        expColNames = new String [] {"I", "C", "DE"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"2", "two", "22.2"},
            {"3", "three", null},
            {"3", "trois", "21.2"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        pSt = prepareStatement(
            "select i from bt1 where i in (2, 5, ?, 2, 0, ?, 2)");
        
        rs = st.executeQuery(
            "values (4, -1)");
        
        rs.next();
        rsmd = rs.getMetaData();
        for (int i = 1; i <= rsmd.getColumnCount(); i++)
            pSt.setObject(i, rs.getObject(i));
        
        rs = pSt.executeQuery();
        expColNames = new String [] {"I"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"2"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            "values (4, 3)");
        
        rs.next();
        rsmd = rs.getMetaData();
        for (int i = 1; i <= rsmd.getColumnCount(); i++)
            pSt.setObject(i, rs.getObject(i));
        
        rs = pSt.executeQuery();
        expColNames = new String [] {"I"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"2"},
            {"3"},
            {"3"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        // IN-list in a subquery ("distinct" here keeps the 
        // subquery from being flattened).
        
        rs = st.executeQuery(
            "select * from (select distinct * from big where i "
            + "in (1, 30)) x");
        
        expColNames = new String [] {"I", "C"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1", "1"},
            {"30", "30"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        st.close();      
    }
    
    public void testNestedQueries() throws SQLException{
        // Nested queries with unions and top-level IN list.
        
        Statement st = createStatement();
        createTestObjects(st);        
        
        st.executeUpdate(
            "create view v2 as select i from bt1 union select i from bt2");
        
        st.executeUpdate(
            " create view v3 as select de d from bt1 union "
            + "select d from bt2");
        
//IC see: https://issues.apache.org/jira/browse/DERBY-5715
        ResultSet rs = st.executeQuery(
            " select * from V2, V3 where V2.i in (2,4) and V3.d "
            + "in (4.3, 7.1, 22.2)");
        
        String[] expColNames = {"I", "D"};
        JDBC.assertColumnNames(rs, expColNames);
        
        String[][] expRS = {
            {"2", "7.1"},
            {"2", "22.2"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select * from V2, V3 where V2.i in (2,3,4) and "
            + "V3.d in (4.3, 7.1, 22.2)");
        
        expColNames = new String [] {"I", "D"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"2", "7.1"},
            {"2", "22.2"},
            {"3", "7.1"},
            {"3", "22.2"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select * from V2 where V2.i in (2, 3, 4)");
        
        expColNames = new String [] {"I"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"2"},
            {"3"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        // OR rewrites.
        
        rs = st.executeQuery(
            "select * from bt1, (select i from bt2 where d = 2.2 "
            + "or d = 8) x(j)");
        
        expColNames = new String [] {"I", "C", "DE", "J"};
        JDBC.assertColumnNames(rs, expColNames);
        JDBC.assertDrainResults(rs, 0);
        
        rs = st.executeQuery(
            " select * from bt1, (select i from bt2 where d = "
            + "2.8 or d = 800) x(j)");
        
        expColNames = new String [] {"I", "C", "DE", "J"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1", "one", null, "-8"},
            {"2", "two", "22.2", "-8"},
            {"3", "three", null, "-8"},
            {"7", "seven", null, "-8"},
            {"8", "eight", "2.8", "-8"},
            {"9", "nine", null, "-8"},
            {"3", "trois", "21.2", "-8"},
            {"10", "10", "3.5", "-8"},
            {"11", "11", null, "-8"},
            {"12", "12", "4.2", "-8"},
            {"13", "13", null, "-8"},
            {"14", "14", "5.0", "-8"},
            {"15", "15", null, "-8"},
            {"16", "16", "5.7", "-8"},
            {"17", "17", null, "-8"},
            {"18", "18", "6.4", "-8"},
            {"19", "19", null, "-8"},
            {"20", "20", "7.1", "-8"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select * from bt1 where (i = 2 or i = 4 or i = 6 "
            + "or i = 8) and (de = 22.3 or de = 2.8)");
        
        expColNames = new String [] {"I", "C", "DE"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"8", "eight", "2.8"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select * from bt1 where (i = 2 or i = 4 or i = 6 "
            + "or i = 8) and (de = 22.3 or de = 2.8) and (c = "
            + "'seven' or c = 'eight' or c = 'nine')");
        
        expColNames = new String [] {"I", "C", "DE"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"8", "eight", "2.8"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        st.close();      
    }
    
    public void testBeetle4316() throws SQLException{
        // beetle 4316, check "in" with self-reference and 
        // correlation, etc.
        
        Statement st = createStatement();
        createTestObjects(st);
        
        st.executeUpdate(
            "create table t1 (c1 real, c2 real)");
        
        st.executeUpdate(
            " create index i11 on t1 (c1)");
        
        st.executeUpdate(
            " create table t2 (c1 real, c2 real)");
        
        st.executeUpdate(
            " insert into t1 values (2, 1), (3, 9), (8, 63), (5, "
            + "25), (20, 5)");
        
        st.executeUpdate(
            " insert into t2 values (4, 8), (8, 8), (7, 6), (5, 6)");
        
//IC see: https://issues.apache.org/jira/browse/DERBY-5715
        ResultSet rs = st.executeQuery(
            " select c1 from t1 where c1 in (2, sqrt(c2))");
        
        String[] expColNames = {"C1"};
        JDBC.assertColumnNames(rs, expColNames);
        
        String[][] expRS = {
            {"2.0"},
            {"3.0"},
            {"5.0"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        //Comparisons between 'REAL' and 'CHAR (UCS_BASIC)' are not supported
        assertStatementError("42818", st,
            " select c1 from t1 where c1 in ('10', '5', '20') and c1 > 3"
            + "and c1 < 19");
        
//IC see: https://issues.apache.org/jira/browse/DERBY-5715
//IC see: https://issues.apache.org/jira/browse/DERBY-5715
        CallableStatement cSt = prepareCall(
            " call SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1)");
        assertUpdateCount(cSt, 0);
            
        // nested loop exists join, right side should be 
        // ProjectRestrict on index scan with start and stop keys
        
        rs = st.executeQuery(
            "select c1 from t2 where c1 in (select c1 from t1 "
            + "where c1 in (5, t2.c2))");
        
        expColNames = new String [] {"C1"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"8.0"},
            {"5.0"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        st.execute("call SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1)");
        
        rs=st.executeQuery("SELECT C1 FROM t2");
        while(rs.next()){}
        rs.close();

       rs = st.executeQuery(
           " values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS()");
       rs.next();

       if(usingEmbedded()){
           RuntimeStatisticsParser rtsp = new RuntimeStatisticsParser(rs.getString(1));
           rs.close();
           assertTrue(rtsp.usedTableScan());
           assertFalse(rtsp.usedDistinctScan());
       }
        
        // nested loop exists join, right side should be 
        // ProjectRestrict on index scan with start and stop keys
        
       
       //Comparisons between 'REAL' and 'CHAR (UCS_BASIC)' are not supported
        assertStatementError("42818", st,
            "select c1 from t2 where c1 in (select c1 from t1 "
            + "where c1 in (5, t2.c2) and c1 in ('5', '7'))");
        
        st.execute("call SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1)");
        
        rs=st.executeQuery("SELECT C1 FROM t2");
        while(rs.next()){}
        rs.close();

       rs = st.executeQuery(
           " values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS()");
       rs.next();

       if(usingEmbedded()){
           RuntimeStatisticsParser rtsp = new RuntimeStatisticsParser(rs.getString(1));
           rs.close();
           assertTrue(rtsp.usedTableScan());
           assertFalse(rtsp.usedDistinctScan());
       }
        
        // hash exists join, right side PR on hash index scan, no 
        // start/stop key, next qualifier "=".
        
        rs = st.executeQuery(
            "select c1 from t2 where c1 in (select c1 from t1 "
            + "where c1 in (5, t2.c2))");
        
        expColNames = new String [] {"C1"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"8.0"},
            {"5.0"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        st.execute("call SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1)");
        
        rs=st.executeQuery("SELECT C1 FROM t2");
        while(rs.next()){}
        rs.close();

       rs = st.executeQuery(
           " values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS()");
       rs.next();

       if(usingEmbedded()){
           RuntimeStatisticsParser rtsp = new RuntimeStatisticsParser(rs.getString(1));
           rs.close();
           assertTrue(rtsp.usedTableScan());
           assertFalse(rtsp.usedDistinctScan());
       }
        
        // hash exists join, right side PR on hash index scan, 
        // still no start/stop key, next qualifier "=". It still 
        // doesn't have start/stop key because c1 in ('5', '7') is 
        // blocked out by 2 others.
        
       
        //Comparisons between 'REAL' and 'CHAR (UCS_BASIC)' are not supported
        assertStatementError("42818", st,
            "select c1 from t2 where c1 in (select c1 from t1 "
            + "where c1 in (5, t2.c2) and c1 in ('5', '7'))");
        
        st.execute("call SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1)");
        
        rs=st.executeQuery("SELECT C1 FROM t2");
        while(rs.next()){}
        rs.close();

       rs = st.executeQuery(
           " values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS()");
       rs.next();

       if(usingEmbedded()){
           RuntimeStatisticsParser rtsp = new RuntimeStatisticsParser(rs.getString(1));
           rs.close();
           assertTrue(rtsp.usedTableScan());
           assertFalse(rtsp.usedDistinctScan());
       }
        
       st.executeUpdate(
            " create index i12 on t1 (c1, c2)");
        
        // at push "in" time, we determined that it is key and we 
        // can push; but at hash time we determined it's not key.  
        // Now the key is it should be filtered out, otherwise we 
        // get exception.
        
        rs = st.executeQuery(
            "select c1 from t2 where c1 in (select c1 from t1 "
            + "where c2 in (5, t2.c2))");
        
        expColNames = new String [] {"C1"};
        JDBC.assertColumnNames(rs, expColNames);
        JDBC.assertDrainResults(rs, 0);
        
        st.execute("call SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1)");
        
        rs=st.executeQuery("SELECT C1 FROM t2");
        while(rs.next()){}
        rs.close();

       rs = st.executeQuery(
           " values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS()");
       rs.next();

       if(usingEmbedded()){
           RuntimeStatisticsParser rtsp = new RuntimeStatisticsParser(rs.getString(1));
           rs.close();
           assertTrue(rtsp.usedTableScan());
           assertFalse(rtsp.usedDistinctScan());
       }
        
        // just some more tests in different situations, not for 
        // the bug 4316 many items
        
       //Comparisons between 'REAL' and 'CHAR (UCS_BASIC)' are not supported
        assertStatementError("42818", st,
            "select c1 from t1 where c1 in ('9', '4', '8.0', '7.7',"
            + "	5.2, 6, '7.7', '4.9', '6.1')");
        
//IC see: https://issues.apache.org/jira/browse/DERBY-5715
        PreparedStatement pSt = prepareStatement(
            "select c1 from t1 where c1 in (3, ?)");
        
        rs = st.executeQuery(
            "values 8");
        
        rs.next();
        ResultSetMetaData rsmd = rs.getMetaData();
        for (int i = 1; i <= rsmd.getColumnCount(); i++)
            pSt.setObject(i, rs.getObject(i));
        
        rs = pSt.executeQuery();
        expColNames = new String [] {"C1"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"3.0"},
            {"8.0"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            "values 9");
        
        rs.next();
        rsmd = rs.getMetaData();
        for (int i = 1; i <= rsmd.getColumnCount(); i++)
            pSt.setObject(i, rs.getObject(i));
        
        rs = pSt.executeQuery();
        expColNames = new String [] {"C1"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"3.0"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        
        // DERBY-2256: IN lists where left operand is not the 
        // dominant type. Should see *no* rows for either of these 
        // queries.
        
        rs = st.executeQuery(
            "select * from test where i in (4.23)");
        
        expColNames = new String [] {"I", "D"};
        JDBC.assertColumnNames(rs, expColNames);
        JDBC.assertDrainResults(rs, 0);
        
        rs = st.executeQuery(
            " select * from test where i in (2.8, 4.23)");
        
        expColNames = new String [] {"I", "D"};
        JDBC.assertColumnNames(rs, expColNames);
        JDBC.assertDrainResults(rs, 0);
        
        // Should not see any rows for this one, either.
        
        rs = st.executeQuery(
            "select * from test where i in (cast (2.8 as "
            + "decimal(4, 2)), 4.23)");
        
        expColNames = new String [] {"I", "D"};
        JDBC.assertColumnNames(rs, expColNames);
        JDBC.assertDrainResults(rs, 0);
        
        // Should get one row for each of these queries.
        
        rs = st.executeQuery(
            "select * from test where i in (4, 4.23)");
        
        expColNames = new String [] {"I", "D"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"4", "12.0"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select * from test where i in (4.23, 4)");
        
        expColNames = new String [] {"I", "D"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"4", "12.0"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        st.close();
    } 
    
    public void testReproductionBeetle5135() throws SQLException{
        //reproduction for beetle 5135 ( long list of constants in 
        // IN clause)
            
//IC see: https://issues.apache.org/jira/browse/DERBY-5715
//IC see: https://issues.apache.org/jira/browse/DERBY-5715
//IC see: https://issues.apache.org/jira/browse/DERBY-5715
        Statement st = createStatement();
        createTestObjects(st);
        
        st.executeUpdate("create table t1(id int)");
        
        st.executeUpdate(" insert into t1 values(2)");        
        st.executeUpdate(" insert into t1 values(5644)");        
        st.executeUpdate(" insert into t1 values(723)");        
        st.executeUpdate(" insert into t1 values(0)");        
        st.executeUpdate(" insert into t1 values(1827)");        
        st.executeUpdate(" insert into t1 values(4107)");        
        st.executeUpdate(" insert into t1 values(5095)");        
        st.executeUpdate(" insert into t1 values(6666)");        
        st.executeUpdate(" insert into t1 values(7777)");        
        st.executeUpdate(" insert into t1 values(15157)");        
        st.executeUpdate(" insert into t1 values(13037)");       
        st.executeUpdate(" insert into t1 values(9999)");
        
//IC see: https://issues.apache.org/jira/browse/DERBY-5715
        ResultSet rs = st.executeQuery(
            " SELECT id FROM t1 WHERE id IN "
            + "(2,3,5,7,6,8,11,13,14,15,16,18,19"
            + ",22,25,30,32,33,5712,34,39,42,43,46,51,54"
            + ",55,56,58,60,62,63,64,65,68,70,72,73,5663"
            + ",5743,74,5396,78,81,83,87,5267,89,91,92,93,94"
            + ",95,96,97,99,101,102,103,104,107,108,109,110,114"
            + ",115,116,118,121,122,124,126,128,129,130,131,132,134"
            + ",136,135,139,140,141,145,150,155,156,158,159,162,160"
            + ",164,165,166,168,169,170,171,172,173,174,175,176,178"
            + ",180,182,183,185,187,188,190,191,193,197,198,200,202"
            + ",203,208,5672,5221,5713,212,213,215,219,220,221,225,227"
            + ",229,5763,234,235,236,238,241,239,243,245,249,250,5716"
            + ",255,256,257,258,259,260,261,262,263,264,265,269,5644"
            + ",272,274,275,276,277,280,282,284,286,289,290,294,296"
            + ",293,299,301,303,305,5234,306,310,311,5473,313,314,315"
            + ",316,318,319,322,323,324,326,327,328,330,333,334,336"
            + ",337,338,340,341,342,343,344,345,346,347,348,350,351"
            + ",353,354,361,363,368,369,370,374,372,373,375,376,379"
            + ",380,384,388,389,390,392,394,396,397,398,400,403,404"
            + ",5775,406,407,408,409,410,412,413,414,416,420,422,423"
            + ",424,428,429,431,434,436,438,441,442,443,450,452,454"
            + ",456,457,458,462,467,466,468,469,5651,470,474,477,479"
            + ",481,482,483,484,488,486,493,494,495,496,498,500,501"
            + ",502,503,504,506,507,508,509,510,512,513,514,516,519"
            + ",520,522,523,524,527,528,530,532,534,535,538,539,542"
            + ",543,546,548,550,552,555,562,561,563,565,567,568,569"
            + ",571,574,572,5250,576,573,579,581,583,584,586,589,590"
            + ",5642,592,596,600,601,602,604,606,607,609,610,611,615"
            + ",616,617,618,619,620,621,623,624,625,626,627,629,630"
            + ",631,632,633,635,636,637,640,641,642,643,644,5246,647"
            + ",646,648,653,5324,654,655,656,658,660,662,663,665,668"
            + ",669,670,672,673,674,675,676,677,678,680,681,683,684"
            + ",686,689,691,2762,694,695,5464,696,697,698,700,701,705"
            + ",5635,5471,708,711,713,714,715,717,719,720,721,722,723"
            + ",724,726,728,729,730,731,733,735,740,741,746,747,748"
            + ",749,750,751,752,754,755,756,757,759,761,762,763,764"
            + ",766,768,769,772,774,776,775,779,780,781,783,788,790"
            + ",794,795,797,801,800,802,804,806,811,813,814,816,819"
            + ",822,823,824,825,826,827,829,5755,832,833,834,836,838"
            + ",840,841,843,844,846,847,848,850,851,852,855,856,857"
            + ",858,859,860,864,5602,865,869,871,872,873,874,876,878"
            + ",880,882,883,885,886,888,890,892,896,898,5528,900,901"
            + ",902,903,904,905,906,907,908,5334,911,912,913,914,915"
            + ",916,918,919,920,921,922,924,923,926,927,928,930,933"
            + ",934,937,938,939,941,942,943,947,945,948,949,951,955"
            + ",957,958,959,960,961,967,968,971,974,980,981,986,987"
            + ",988,991,989,993,995,996,997,999,1000,1001,1002,1003,1005"
            + ",1006,1007,1008,1009,1010,1012,1011,1014,1015,1016,1"
            + "017,1019,1021"
            + ",1025,1026,1028,1029,1030,1031,1034,1036,1037,1039,1"
            + "041,1042,1043"
            + ",1049,1047,1050,1051,1052,1053,1054,1056,1057,1058,1"
            + "061,1062,1063"
            + ",1066,1071,1070,1073,1075,1077,1078,5710,1084,1085,1"
            + "086,1088,1090"
            + ",1091,1093,1094,1095,1096,1099,1102,1104,1105,1107,1"
            + "108,1109,1110"
            + ",1114,1117,1119,1121,1123,1124,1126,1127,1128,1129,1"
            + "130,1131,1136"
            + ",1138,1141,1143,1144,1145,1147,1150,1151,1157,1146,1"
            + "158,1164,1166"
            + ",1171,1170,1176,1177,1189,5525,1202,1203,1173,1175,1"
            + "179,1181,1183"
            + ",1184,1186,1188,1193,1195,1196,1197,1198,1199,1200,1"
            + "205,1207,1225"
            + ",1226,1227,1228,1209,1210,1214,1212,1215,1217,1218,1"
            + "219,1220,5238"
            + ",1221,1223,5288,1230,5727,1232,1234,1235,1236,5795,5"
            + "816,1238,1240"
            + ",1241,1245,1246,1247,1250,1253,1254,1258,1261,1262,1"
            + "264,1265,1266"
            + ",1268,1270,1274,1275,1277,1278,1280,1281,1282,1283,1"
            + "284,1286,1285"
            + ",1287,1288,1290,1293,1294,1295,1297,1301,1302,1305,1"
            + "307,1308,1309"
            + ",1311,1313,1314,1316,1317,1318,1320,1321,1323,1327,1"
            + "329,1332,1334"
            + ",1336,1338,1339,1341,1343,1348,1346,1347,1349,1350,1"
            + "353,1357,1358"
            + ",1359,1361,1363,1366,1367,1368,1369,1370,1371,1374,5"
            + "689,1376,1377"
            + ",1379,1380,1381,1386,1387,5661,1389,1390,1392,1393,1"
            + "394,1395,1396"
            + ",1398,1400,1402,1408,1409,1410,1411,1412,1413,1414,1"
            + "415,1416,1419"
            + ",1421,1425,1427,1428,5216,1430,1431,1432,1433,1434,1"
            + "437,1438,1440"
            + ",1444,1446,1448,1449,1451,1453,1454,1456,1457,1458,1"
            + "459,1461,1463"
            + ",1464,1465,1466,1467,1468,1472,1474,1475,1477,1476,1"
            + "479,1480,1482"
            + ",1484,1485,1489,1490,1491,1492,1494,1495,1498,1496,1"
            + "502,1503,1504"
            + ",1506,1507,1508,1510,1511,1512,1517,1519,5686,1521,1"
            + "525,1528,1531"
            + ",1530,1529,1535,1537,1538,1539,1541,1542,1546,1549,1"
            + "552,1554,1555"
            + ",1557,1558,1561,1562,1563,1566,1568,1570,1574,1575,1"
            + "576,1580,1579"
            + ",1577,1581,1583,1584,1585,1586,1589,1588,1592,1590,1"
            + "594,1597,1598"
            + ",1600,1601,1605,1606,1607,1608,1610,1611,1612,1613,1"
            + "614,1615,1618"
            + ",1620,1624,1625,1626,1627,1628,1631,1633,1635,1639,1"
            + "640,1641,1642"
            + ",5653,1645,1647,1649,1650,1655,5633,1656,1657,5647,1"
            + "661,1662,1666"
            + ",1667,1668,1669,1671,1672,1673,1674,1675,1676,1677,1"
            + "678,1680,1682"
            + ",1686,1688,1690,1694,1695,1696,1697,1699,1700,1701,1"
            + "702,1703,1708"
            + ",1710,1714,1713,1716,1719,1722,1721,1723,1724,1726,1"
            + "727,1728,1729"
            + ",1732,1734,1735,5419,1736,1737,1739,1740,1743,1744,1"
            + "747,1748,1749"
            + ",1750,1751,1752,1754,1757,1758,1767,1759,1761,1762,1"
            + "764,1765,1766"
            + ",1768,1771,1774,1775,1776,1779,1777,1781,1783,1785,1"
            + "787,1789,1791"
            + ",1794,1795,1796,1797,1798,1802,1804,1805,1806,1808,1"
            + "809,1811,1812"
            + ",1813,1814,1815,1816,1817,1819,5372,1822,1823,1824,1"
            + "825,1827,1829"
            + ",5709,1830,1831,1832,1833,1834,1835,1837,1838,1839,1"
            + "841,1842,1847"
            + ",5337,1848,1850,1851,1852,1854,1855,1858,1856,1859,1"
            + "861,1862,1863"
            + ",1867,1866,1868,1870,1871,1873,1874,1878,1879,1880,1"
            + "881,1883,1884"
            + ",1886,1889,1891,1893,1894,1896,1901,1903,1905,1906,1"
            + "907,1908,1909"
            + ",1911,1915,1916,1918,1919,1921,1922,1924,1925,5468,5"
            + "671,1930,1931"
            + ",1932,1933,1935,1937,1942,1943,1944,1947,1949,1951,1"
            + "952,1955,1956"
            + ",1957,1961,1962,1963,5393,1965,1966,1968,1972,1971,1"
            + "976,1978,1980"
            + ",1982,1983,1986,1989,1991,1992,1994,1995,1996,1997,1"
            + "998,2000,2001"
            + ",2002,2003,2005,2006,2008,2009,2012,2013,2015,2016,2"
            + "018,2024,2026"
            + ",2027,2028,2029,2031,2038,2039,2044,2046,2049,2050,2"
            + "051,2052,2053"
            + ",2054,2056,2058,2055,2060,2061,2062,2063,2065,2069,2"
            + "070,2066,2076"
            + ",2074,2072,2077,2079,2080,2083,2085,2086,2088,2089,2"
            + "091,2092,2094"
            + ",2096,2095,2098,2097,2099,2100,2106,2107,2108,2111,2"
            + "112,2113,2114"
            + ",2116,2117,2118,2119,2121,2123,2124,2125,2126,2127,2"
            + "128,2129,2130"
            + ",2134,2138,2139,2144,2145,2147,2148,2150,2151,2152,2"
            + "153,2156,2157"
            + ",2158,2160,2161,2162,2163,2164,2165,2166,2167,2170,2"
            + "171,2172,2173"
            + ",2174,2175,2176,2178,2180,2181,2186,5408,2188,2189,2"
            + "190,2191,2192"
            + ",2195,2198,2199,2201,2203,2206,2207,2209,2211,2212,5"
            + "236,2213,2215"
            + ",2216,2217,2218,2219,5253,2224,2225,2226,2227,2229,2"
            + "231,2232,2233"
            + ",2235,2236,2237,2238,2240,2241,2242,2243,2245,2246,2"
            + "247,2248,2249"
            + ",2251,2257,2259,2260,2261,2262,2263,2264,2265,2266,2"
            + "267,2270,2272"
            + ",2273,2274,2275,2276,2277,2281,2282,2284,2285,2288,2"
            + "289,2290,2291"
            + ",2293,2294,2295,2296,2298,2299,2300,2301,2304,2306,2"
            + "308,2310,2309"
            + ",2312,2313,2316,2317,2322,2324,2320,2318,2330,2331,2"
            + "332,2334,5711"
            + ",2335,2337,2338,2339,2344,2345,2347,2348,2349,5740,2"
            + "350,2354,2356"
            + ",2357,2358,2359,2361,2362,2365,2367,2368,2370,2372,2"
            + "374,2378,2379"
            + ",2380,2381,2382,2385,2388,2389,2391,2392,2393,2395,2"
            + "396,2398,2400"
            + ",2402,2401,2403,2404,2406,2408,2409,2411,2413,2417,2"
            + "419,2421,2422"
            + ",2424,2425,2426,2427,2428,2430,2432,2433,2434,2435,2"
            + "436,2440,2439"
            + ",2441,2443,2445,2446,2450,2448,2449,2451,2453,2456,2"
            + "457,2458,5751"
            + ",2460,2462,2463,2465,5731,2468,2469,2471,2472,2474,2"
            + "479,2480,2481"
            + ",2482,2484,2485,2486,2487,2488,2489,2491,2492,2494,2"
            + "495,2496,2497"
            + ",2499,2500,2501,2503,2505,2506,2507,2508,2511,2513,2"
            + "515,2514,2516"
            + ",2522,2525,2523,2526,2527,2528,2529,2532,2531,2533,2"
            + "534,2535,2537"
            + ",2539,2541,2543,2544,2546,2548,2550,2551,5629,2553,2"
            + "555,2556,2557"
            + ",2558,2559,2560,2563,2565,2569,2571,2574,2575,5718,5"
            + "434,2577,2578"
            + ",5760,2580,2584,2585,2587,2589,2590,2591,2592,2593,2"
            + "594,2596,2598"
            + ",2600,2602,2603,2605,2606,2607,2608,2610,2612,2613,2"
            + "615,2616,2618"
            + ",2619,2623,2621,2624,2625,2630,2633,2634,2636,2638,2"
            + "640,2643,2644"
            + ",2649,2651,2653,2654,2655,2656,2657,2658,2659,2660,2"
            + "661,2662,2665"
            + ",2666,2667,2670,2671,2673,2674,2676,2680,2682,2683,2"
            + "684,2687,2685"
            + ",2690,2688,2694,2692,2695,2696,5448,2698,2699,2700,2"
            + "701,2703,2704"
            + ",2705,2706,2708,2709,2711,2714,2716,2717,2718,2719,2"
            + "720,2722,2724"
            + ",2725,2726,2728,2729,2733,2736,2734,2737,2738,2739,2"
            + "743,2744,2745"
            + ",2746,2747,2748,2754,2751,2753,2755,2757,2758,2761,2"
            + "763,2766,2768"
            + ",2769,2771,2773,2775,2774,2776,2778,2780,2781,2782,2"
            + "784,2785,2786"
            + ",2787,2788,2789,2790,2791,2795,2798,2801,2802,2803,2"
            + "804,2807,2808"
            + ",2809,2810,2812,2814,2815,2816,2819,2820,2822,2824,5"
            + "649,2828,5465"
            + ",2832,2833,5817,5809,5814,5815,2835,2838,2839,2845,2"
            + "846,2847,2850"
            + ",2851,2852,2854,2855,2857,2842,2858,2859,2861,2863,2"
            + "864,2865,2866"
            + ",2872,2873,2874,2875,2878,2881,2882,2883,2884,2885,2"
            + "886,2887,2888"
            + ",2891,2893,2894,2895,2896,2898,2897,2899,2900,2901,2"
            + "903,2905,2906"
            + ",2907,2908,2910,2914,2916,2917,2920,2918,2921,2925,2"
            + "927,2928,2929"
            + ",2932,2934,2936,2937,2938,2940,2939,2942,2943,2944,2"
            + "945,2947,2950"
            + ",2952,2953,2955,2957,2958,2959,2961,2962,2963,2964,2"
            + "966,2967,2968"
            + ",2972,2974,2976,2977,2978,2979,2980,2981,2982,2983,2"
            + "984,2986,2987"
            + ",2988,2989,2990,2991,2992,2993,2994,2996,2997,2998,2"
            + "999,3000,3001"
            + ",3003,3007,3008,3009,3011,3013,3014,3015,3016,3017,3"
            + "019,3021,5768"
            + ",3023,3026,3027,3028,3029,3032,3033,3035,3039,3040,3"
            + "041,3042,3043"
            + ",3044,3045,3046,3048,3050,3051,3055,3056,3057,3060,3"
            + "061,3062,3064"
            + ",3069,3068,3070,3071,3072,3074,3076,3079,3080,3082,3"
            + "083,3086,3099"
            + ",3088,3089,3090,3091,3092,3093,3094,3095,3096,5183,3"
            + "097,3101,3103"
            + ",3104,3107,3109,3111,3112,3114,3116,3120,3122,3123,3"
            + "126,3127,3128"
            + ",3129,3131,3132,3134,3135,3137,3139,5758,5724,3140,3"
            + "144,3145,3147"
            + ",3148,3149,3150,3152,3153,3154,3155,3158,3161,3162,3"
            + "163,3164,3165"
            + ",3166,3167,5776,3168,3170,3180,3181,3182,3186,3191,3"
            + "192,3196,3198"
            + ",3199,3200,3203,3205,3206,3207,3208,3210,3211,3212,3"
            + "213,3215,3216"
            + ",3217,3218,3219,3220,3221,3224,3226,3228,3230,3231,3"
            + "232,3233,3235"
            + ",3236,3237,3239,3241,3242,3243,5687,3245,3246,3248,3"
            + "249,3253,3254"
            + ",3259,3260,3261,3262,3264,3266,3267,3269,5811,3271,3"
            + "273,3275,3277"
            + ",5620,3278,3279,3280,3282,3284,3286,3287,3289,3293,3"
            + "294,3295,3297"
            + ",3299,3302,3301,3305,3307,3306,3308,3310,3311,3312,3"
            + "313,3315,3316"
            + ",5497,3318,3322,3324,3326,3328,3336,3337,3338,3339,3"
            + "341,5589,3344"
            + ",5742,3345,3346,3348,3350,3352,3354,3355,3356,3357,3"
            + "361,3363,3364"
            + ",3365,3367,3368,3369,3371,3370,3372,3375,3373,3377,3"
            + "378,3379,3381"
            + ",5638,3382,3384,3386,3387,3389,3390,3391,3392,3397,3"
            + "398,3400,3401"
            + ",3402,3404,3405,3406,3407,3408,3409,3410,3411,3414,3"
            + "415,3416,3417"
            + ",3418,3420,3421,3423,3424,3426,3428,3430,3431,3432,3"
            + "433,3435,3436"
            + ",3437,5391,3440,3441,3442,3443,3444,3446,3448,3450,3"
            + "452,3451,3453"
            + ",3455,3456,3457,3460,3461,3463,3464,3467,3466,3468,3"
            + "471,3472,3474"
            + ",3475,3477,3479,3481,3482,3484,3485,3486,3487,3488,3"
            + "489,3491,3493"
            + ",3494,3496,3497,3498,3500,3502,3504,3499,3505,3507,3"
            + "514,3515,3517"
            + ",3519,3520,3522,3524,3525,5256,3526,3527,3528,3529,3"
            + "531,5636,3532"
            + ",3533,3535,3536,3538,3539,3541,3544,3548,3550,3551,3"
            + "552,3554,3556"
            + ",3557,3559,3560,3563,3564,3565,3567,3568,3571,3572,3"
            + "573,3574,3577"
            + ",3583,3582,3580,3584,3586,3589,3587,3590,3591,3592,3"
            + "593,3596,3597"
            + ",3599,3602,3603,3604,3605,3606,3608,3609,5398,3612,3"
            + "614,3615,3616"
            + ",3617,3618,3619,3620,3621,3623,3624,3628,3630,3631,3"
            + "633,3635,3636"
            + ",3637,3638,3640,3641,3643,3645,3644,3648,3650,3649,3"
            + "651,3655,3662"
            + ",3664,3665,3667,3668,3672,3673,3676,3679,3681,3682,3"
            + "683,3685,3688"
            + ",3689,3690,3692,3695,3696,3697,3699,3700,3701,3704,5"
            + "349,3707,3708"
            + ",3710,3713,3715,3716,3717,3718,3720,3721,3724,3726,3"
            + "727,3728,3729"
            + ",3731,3732,3733,3735,3736,3741,3745,3747,3749,3751,3"
            + "752,3754,3756"
            + ",3758,3761,3762,3767,3769,3773,3775,5680,5181,3779,3"
            + "783,3784,3788"
            + ",5567,3792,3794,3797,3800,3801,3804,3805,3806,3807,3"
            + "808,3809,3810"
            + ",3811,3812,3813,3814,3819,3818,3820,3821,3822,3824,3"
            + "825,3826,3827"
            + ",3829,3830,3832,5242,3834,3835,3836,3838,3843,3802,3"
            + "849,3850,3855"
            + ",3857,5657,3858,3859,3862,5645,3863,3864,3865,5669,3"
            + "866,3867,3868"
            + ",3869,3872,5720,3873,3874,3877,3879,3880,3881,3882,3"
            + "884,3885,3886"
            + ",3887,3888,3889,3890,3892,3893,3898,3899,3900,3903,3"
            + "904,3905,3908"
            + ",3909,3910,3911,3916,3917,3918,3921,3924,3926,3930,3"
            + "931,3933,3934"
            + ",3936,3938,3939,3940,3941,3945,3949,3950,3954,3955,3"
            + "957,3958,3960"
            + ",3961,3964,3966,3968,3973,3979,3980,3981,3982,3983,3"
            + "985,3986,3987"
            + ",3989,3991,3990,3994,3992,3993,3995,3997,3998,3999,4"
            + "000,4001,4002"
            + ",4003,4004,4005,4006,4007,4008,4009,4011,4012,4013,4"
            + "015,4016,4020"
            + ",4022,4023,5536,4026,4027,4028,4030,4031,4034,5770,4"
            + "035,4037,4040"
            + ",4041,4043,4044,4045,4046,4047,4048,4051,4052,4053,4"
            + "055,4059,4061"
            + ",4062,4063,4064,4067,4070,4073,4074,4075,4076,4077,4"
            + "079,4081,4083"
            + ",4084,4085,4086,4093,5240,4090,4092,4094,4095,4097,4"
            + "098,4100,4102"
            + ",4103,4104,4105,4106,4107,4108,4109,4110,4112,4114,4"
            + "115,4118,5631"
            + ",4120,4124,4132,4135,4142,4144,4145,4147,4148,4149,4"
            + "150,4156,4159"
            + ",4160,4162,4163,4165,4166,4168,4167,4169,4171,4172,4"
            + "174,4175,4179"
            + ",4181,4182,4186,4190,4188,4194,4196,4198,5738,4200,4"
            + "202,4203,4205"
            + ",4206,4208,4211,4212,4213,4215,4217,4220,4223,4225,4"
            + "229,4230,4231"
            + ",4235,4236,4237,4238,4239,5826,4241,4242,4243,4244,4"
            + "246,5343,4250"
            + ",4251,4252,4253,4254,4255,4256,4257,4258,4262,5685,4"
            + "264,4268,4269"
            + ",4270,4271,4272,4273,4274,5659,4279,4281,4283,4284,4"
            + "285,4287,4291"
            + ",4292,4296,4298,4299,4300,4301,4302,4303,4304,4305,4"
            + "308,4310,5348"
            + ",4311,4312,4313,4315,5438,4317,4319,4321,4322,4324,4"
            + "326,4327,4328"
            + ",4329,4330,4331,4334,4336,4337,4341,4342,4343,4344,5"
            + "326,4346,4352"
            + ",4354,4356,4359,4362,4364,4366,4367,4371,4373,4375,4"
            + "379,4381,4384"
            + ",4386,4392,4390,5218,4397,4404,4406,4409,4410,4412,4"
            + "411,4413,4414"
            + ",4416,4418,4420,4424,4426,4427,4430,4431,4432,4433,4"
            + "437,4438,4439"
            + ",4440,4441,4442,4444,4445,4448,4446,5748,4451,4453,4"
            + "454,4455,4458"
            + ",5774,4461,4462,4464,4465,4466,4467,4468,4469,4470,4"
            + "472,4474,4475"
            + ",4476,4479,4480,4482,4483,4485,4487,4490,4492,4493,4"
            + "494,4500,4501"
            + ",4503,4504,4506,4507,4508,4509,4510,4511,4512,4513,4"
            + "516,4519,4520"
            + ",4521,4522,4524,4525,4527,4528,4533,4535,4536,4537,4"
            + "538,4539,4540"
            + ",4541,4542,4544,4547,4548,4550,4552,4553,4555,4556,4"
            + "557,4559,4561"
            + ",4562,4564,4565,4566,4567,4568,4569,5417,4570,4572,4"
            + "575,4582,4576"
            + ",4578,4581,4583,4584,4585,4586,4587,4588,4589,4593,4"
            + "594,4596,4603"
            + ",4604,4605,4610,4612,4614,5387,4619,4622,4624,4626,4"
            + "627,4628,4629"
            + ",4630,4632,4634,4636,4637,4640,4645,4646,4648,4650,4"
            + "651,4652,4653"
            + ",4654,4657,4659,4662,4660,4664,4665,4667,4668,4669,4"
            + "672,4674,4677"
            + ",4679,4681,4682,4683,4684,4686,4688,4689,4690,4692,4"
            + "693,4694,4695"
            + ",4698,4699,4700,4705,4701,4703,4708,4709,4711,4713,4"
            + "714,4717,4727"
            + ",4728,4732,4734,4736,4737,4739,4741,4744,4747,4748,4"
            + "750,4751,4754"
            + ",4755,4756,4758,4759,4761,4762,4764,4765,4767,4769,4"
            + "749,4770,4771"
            + ",4773,4774,4775,4776,4777,4778,4784,4785,4786,4787,4"
            + "788,4791,4793"
            + ",4794,5389,4798,4800,4801,4803,4805,4808,4806,4809,4"
            + "810,4811,4814"
            + ",4815,4816,4822,4826,4827,4829,4831,4824,4832,4835,4"
            + "836,4838,4839"
            + ",4840,4842,4844,4846,4848,4849,4850,4853,4854,4858,4"
            + "860,4861,4862"
            + ",4863,4864,4867,4868,4871,4873,4874,4875,4877,4878,4"
            + "879,4884,4886"
            + ",4888,4889,4890,4891,4892,4893,4894,4895,4896,4897,4"
            + "899,4902,4903"
            + ",4904,4908,4905,4906,4907,4910,4911,4912,4913,4915,4"
            + "914,4916,4917"
            + ",4918,4919,4920,4921,4923,4926,4927,4928,4929,4930,4"
            + "931,4932,4933"
            + ",4937,4942,4944,4945,4946,4948,4950,4951,4954,4956,4"
            + "958,4960,4961"
            + ",4963,4964,4965,4967,4970,4969,4971,4972,4974,4977,4"
            + "975,4979,4981"
            + ",5729,4982,4983,4984,4986,4989,4991,4992,4994,4995,4"
            + "996,4997,4998"
            + ",4999,5001,5003,5005,5006,5655,3969,5007,5622,5009,5"
            + "013,5015,5021"
            + ",5022,5024,5025,5026,5028,5029,5031,5033,5036,5037,5"
            + "038,5040,5041"
            + ",5042,5043,5047,5048,5050,5051,5053,5054,5056,5058,5"
            + "059,5061,5063"
            + ",5064,5065,5066,5068,5069,5070,5072,5073,5076,5080,5"
            + "081,5082,5083"
            + ",5084,5085,5087,5086,5088,5090,5092,5094,5095,5097,5"
            + "099,5101,5102"
            + ",5104,5105,5106,5107,5108,5110,5112,5114,5115,5116,5"
            + "117,5118,5119"
            + ",5120,5121,5123,5124,5125,5126,5127,5128,5130,5131,5"
            + "132,5134,5136"
            + ",5137,5138,5139,5140,5141,5143,5777,5812,5148,5154,5"
            + "155,5157,5159"
            + ",6022,6024,6025,6026,6028,6029,6031,6033,6036,6037,6"
            + "038,6040,6041"
            + ",6042,6043,6047,6048,6050,6051,6053,6054,6056,6058,6"
            + "059,6061,6063"
            + ",6064,6065,6066,6068,6069,6070,6072,6073,6076,6080,6"
            + "081,6082,6083"
            + ",6084,6085,6087,6086,6088,6090,6092,6094,6095,6097,6"
            + "099,6101,6102"
            + ",6104,6105,6106,6107,6108,6110,6112,6114,6115,6116,6"
            + "117,6118,6119"
            + ",6120,6121,6123,6124,6125,6126,6127,6128,6130,6131,6"
            + "132,6134,6136"
            + ",6137,6138,6139,6140,6141,6143,6777,6812,6148,6154,6"
            + "155,6157,6159"
            + ",7022,7024,7025,7026,7028,7029,7031,7033,7036,7037,7"
            + "038,7040,7041"
            + ",7042,7043,7047,7048,7050,7051,7053,7054,7056,7058,7"
            + "059,7061,7063"
            + ",7064,7065,7066,7068,7069,7070,7072,7073,7076,7080,7"
            + "081,7082,7083"
            + ",7084,7085,7087,7086,7088,7090,7092,7094,7095,7097,7"
            + "099,7101,7102"
            + ",7104,7105,7106,7107,7108,7110,7112,7114,7115,7116,7"
            + "117,7118,7119"
            + ",7120,7121,7123,7124,7125,7126,7127,7128,7130,7131,7"
            + "132,7134,7136"
            + ",7137,7138,7139,7140,7141,7143,7777,7812,7148,7154,7"
            + "155,7157,7159"
            + ",8022,8024,8025,8026,8028,8029,8031,8033,8036,8037,8"
            + "038,8040,8041"
            + ",8042,8043,8047,8048,8050,8051,8053,8054,8056,8058,8"
            + "059,8061,8063"
            + ",8064,8065,8066,8068,8069,8070,8072,8073,8076,8080,8"
            + "081,8082,8083"
            + ",8084,8085,8087,8086,8088,8090,8092,8094,8095,8097,8"
            + "099,8101,8102"
            + ",8104,8105,8106,8107,8108,8110,8112,8114,8115,8116,8"
            + "117,8118,8119"
            + ",8120,8121,8123,8124,8125,8126,8127,8128,8130,8131,8"
            + "132,8134,8136"
            + ",8137,8138,8139,8140,8141,8143,8777,8812,8148,8154,8"
            + "155,8157,8159"
            + ",9022,9024,9025,9026,9028,9029,9031,9033,9036,9037,9"
            + "038,9040,9041"
            + ",9042,9043,9047,9048,9050,9051,9053,9054,9056,9058,9"
            + "059,9061,9063"
            + ",9064,9065,9066,9068,9069,9070,9072,9073,9076,9080,9"
            + "081,9082,9083"
            + ",9084,9085,9087,9086,9088,9090,9092,9094,9095,9097,9"
            + "099,9101,9102"
            + ",9104,9105,9106,9107,9108,9110,9112,9114,9115,9116,9"
            + "117,9118,9119"
            + ",9120,9121,9123,9124,9125,9126,9127,9128,9130,9131,9"
            + "132,9134,9136"
            + ",9137,9138,9139,9140,9141,9143,9777,9812,9148,9154,9"
            + "155,9157,9159"
            + ",10022,10024,10025,10026,10028,10029,10031,10033,100"
            + "36,10037,10038,10040,10041"
            + ",10042,10043,10047,10048,10050,10051,10053,10054,100"
            + "56,10058,10059,10061,10063"
            + ",10064,10065,10066,10068,10069,10070,10072,10073,100"
            + "76,10080,10081,10082,10083"
            + ",10084,10085,10087,10086,10088,10090,10092,10094,100"
            + "95,10097,10099,10101,10102"
            + ",10104,10105,10106,10107,10108,10110,10112,10114,101"
            + "15,10116,10117,10118,10119"
            + ",10120,10121,10123,10124,10125,10126,10127,10128,101"
            + "30,10131,10132,10134,10136"
            + ",10137,10138,10139,10140,10141,10143,10777,10812,101"
            + "48,10154,10155,10157,10159"
            + ",11022,11024,11025,11026,11028,11029,11031,11033,110"
            + "36,11037,11038,11040,11041"
            + ",11042,11043,11047,11048,11050,11051,11053,11054,110"
            + "56,11058,11059,11061,11063"
            + ",11064,11065,11066,11068,11069,11070,11072,11073,110"
            + "76,11080,11081,11082,11083"
            + ",11084,11085,11087,11086,11088,11090,11092,11094,110"
            + "95,11097,11099,11101,11102"
            + ",11104,11105,11106,11107,11108,11110,11112,11114,111"
            + "15,11116,11117,11118,11119"
            + ",11120,11121,11123,11124,11125,11126,11127,11128,111"
            + "30,11131,11132,11134,11136"
            + ",11137,11138,11139,11140,11141,11143,11777,11812,111"
            + "48,11154,11155,11157,11159"
            + ",12022,12024,12025,12026,12028,12029,12031,12033,120"
            + "36,12037,12038,12040,12041"
            + ",12042,12043,12047,12048,12050,12051,12053,12054,120"
            + "56,12058,12059,12061,12063"
            + ",12064,12065,12066,12068,12069,12070,12072,12073,120"
            + "76,12080,12081,12082,12083"
            + ",12084,12085,12087,12086,12088,12090,12092,12094,120"
            + "95,12097,12099,12101,12102"
            + ",12104,12105,12106,12107,12108,12110,12112,12114,121"
            + "15,12116,12117,12118,12119"
            + ",12120,12121,12123,12124,12125,12126,12127,12128,121"
            + "30,12131,12132,12134,12136"
            + ",12137,12138,12139,12140,12141,12143,12777,12812,121"
            + "48,12154,12155,12157,12159"
            + ",13022,13024,13025,13026,13028,13029,13031,13033,130"
            + "36,13037,13038,13040,13041"
            + ",13042,13043,13047,13048,13050,13051,13053,13054,130"
            + "56,13058,13059,13061,13063"
            + ",13064,13065,13066,13068,13069,13070,13072,13073,130"
            + "76,13080,13081,13082,13083"
            + ",13084,13085,13087,13086,13088,13090,13092,13094,130"
            + "95,13097,13099,13101,13102"
            + ",13104,13105,13106,13107,13108,13110,13112,13114,131"
            + "15,13116,13117,13118,13119"
            + ",13120,13121,13123,13124,13125,13126,13127,13128,131"
            + "30,13131,13132,13134,13136"
            + ",13137,13138,13139,13140,13141,13143,13777,13812,131"
            + "48,13154,13155,13157,13159"
            + ",14022,14024,14025,14026,14028,14029,14031,14033,140"
            + "36,14037,14038,14040,14041"
            + ",14042,14043,14047,14048,14050,14051,14053,14054,140"
            + "56,14058,14059,14061,14063"
            + ",14064,14065,14066,14068,14069,14070,14072,14073,140"
            + "76,14080,14081,14082,14083"
            + ",14084,14085,14087,14086,14088,14090,14092,14094,140"
            + "95,14097,14099,14101,14102"
            + ",14104,14105,14106,14107,14108,14110,14112,14114,141"
            + "15,14116,14117,14118,14119"
            + ",14120,14121,14123,14124,14125,14126,14127,14128,141"
            + "30,14131,14132,14134,14136"
            + ",14137,14138,14139,14140,14141,14143,14777,14812,141"
            + "48,14154,14155,14157,14159"
            + ",15022,15024,15025,15026,15028,15029,15031,15033,150"
            + "36,15037,15038,15040,15041"
            + ",15042,15043,15047,15048,15050,15051,15053,15054,150"
            + "56,15058,15059,15061,15063"
            + ",15064,15065,15066,15068,15069,15070,15072,15073,150"
            + "76,15080,15081,15082,15083"
            + ",15084,15085,15087,15086,15088,15090,15092,15094,150"
            + "95,15097,15099,15101,15102"
            + ",15104,15105,15106,15107,15108,15110,15112,15114,151"
            + "15,15116,15117,15118,15119"
            + ",15120,15121,15123,15124,15125,15126,15127,15128,151"
            + "30,15131,15132,15134,15136"
            + ",15137,15138,15139,15140,15141,15143,15777,15812,151"
            + "48,15154,15155,15157,15159"
            + ",4436,5162,5165,5170,5171,5173,5345,5174,5765,5177,5"
            + "750,5793,0) ORDER BY id");
        
//IC see: https://issues.apache.org/jira/browse/DERBY-5715
        String[] expColNames = {"ID"};
        JDBC.assertColumnNames(rs, expColNames);
        
        String[][] expRS = {
            {"0"},
            {"2"},
            {"723"},
            {"1827"},
            {"4107"},
            {"5095"},
            {"5644"},
            {"7777"},
            {"13037"},
            {"15157"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        st.executeUpdate(
            " drop table t1");
        
        st.executeUpdate(
            " create table t1(c1 int )");
        
        st.executeUpdate(
            " create table t2(c2 int)");
        
        st.executeUpdate(
            " insert into t2 values(0)");
        
        st.executeUpdate(
            " create view v1(c1)"
            + "as"
            + "(select c1 from t1)"
            + "union all"
            + "(select c2 from t2)");
        
        //following statement fails with NPE before fix of 5469
        
        rs = st.executeQuery(
            "select c1 from v1 where c1 NOT IN (1, 2)");
        
        expColNames = new String [] {"C1"};
        JDBC.assertColumnNames(rs, expColNames);
        JDBC.assertSingleValueResultSet(rs, "0");
//IC see: https://issues.apache.org/jira/browse/DERBY-5715

        rollback();
        st.close();
    }

    /**
     * Regression test cases for DERBY-4388, where the not elimination in
     * BetweenOperatorNode could make column references point to the wrong
     * result sets after optimization, causing NullPointerExceptions.
     */
    public void testDerby4388NotElimination() throws SQLException {
        setAutoCommit(false); // for easy cleanup with rollback() in tearDown()
        Statement s = createStatement();
        s.execute("create table d4388_t1(a int)");
        s.execute("create table d4388_t2(b int)");
        s.execute("insert into d4388_t1 values 0,1,2,3,4,5,6");
        s.execute("insert into d4388_t2 values 0,1,2,3");
        // The queries below used to cause NullPointerException.
        JDBC.assertFullResultSet(
                s.executeQuery("select * from d4388_t1 left join d4388_t2 " +
                               "on a=b where b not between 1 and 5"),
                new String[][]{{"0", "0"}});
        JDBC.assertFullResultSet(
                s.executeQuery("select * from d4388_t2 right join d4388_t1 " +
                               "on a=b where b not between 1 and 5"),
                new String[][]{{"0", "0"}});
    }

    /** Regression test case for DERBY-6577. */
    public void testInBetweenQuantifiedComparison() throws SQLException {
        Statement s = createStatement();
        String[][] expectedRows = {
            { "Y", "true" },
            { "N", "false" },
        };

        // This query used to return wrong results.
        JDBC.assertUnorderedResultSet(s.executeQuery(
                "select c, true in ((c = all (values 'Y'))) "
                + "from (values 'Y', 'N') v(c)"),
            expectedRows);

        // This query used to return wrong results.
        JDBC.assertUnorderedResultSet(s.executeQuery(
                "select c, true between false and (c = all (values 'Y')) "
                + "from (values 'Y', 'N') v(c)"),
            expectedRows);
    }
}
