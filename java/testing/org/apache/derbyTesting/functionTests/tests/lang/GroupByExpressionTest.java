/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.lang.GroupByExpressionTest

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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

import junit.extensions.TestSetup;
import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.BaseJDBCTestSetup;

public class GroupByExpressionTest extends BaseJDBCTestCase
{

    private static String[][] TABLES = { 
        {"test", "create table test (c1 int, c2 int, c3 int, c4 int)"},
        {"coal", "create table coal (vc1 varchar(2), vc2 varchar(2))"},
        {"alltypes", 
            "create table alltypes (i int, s smallint, l bigint, " +
            "c char(10), v varchar(50), lvc long varchar, " +
            " d double precision, r real, " + 
            " dt date, t time, ts timestamp, " +
            " b char(2) for bit data, bv varchar(8) for bit data, " +
            " lbv long varchar for bit data, dc decimal(5,2))"}};
    private static String[][] FUNCTIONS = {
        {"r", "create function r() returns double external name " +
            "'java.lang.Math.random' language java parameter style java"}};
    
    /** 
     * Basic test case. Checks functionality with simple arithmetic expressions
     */
    public void testSimpleExpressions() throws Exception
    {
        verifyQueryResults(
                "Q1",
                "select c1,c2,sum(c3) from test group by c2,c1",
                new int[][] {
                        {1,10,100},
                        {2,10,100},
                        {1,11,100},
                        {2,11,202}});

        verifyQueryResults(
                "Q2",
                "select c1+c2, sum(c3) from test group by c1,c2",
                new int[][] {
                        {11, 100},
                        {12, 100}, 
                        {12, 100}, 
                        {13, 202}});
        verifyQueryResults(
                "Q3",
                "select c1+c2, sum(c3) from test group by c1+c2",
                new int[][] {
                        {11, 100}, 
                        {12, 200}, 
                        {13, 202}});
        verifyQueryResults(
                "Q4",
                "select (c1+c2)+1, sum(c3) from test group by c1+c2",
                new int[][] {
                        {12, 100}, 
                        {13, 200}, 
                        {14, 202}});
        verifyQueryResults(
                "Q5",
                "select (c1+c2), sum(c3)+(c1+c2) from test group by c1+c2",
                new int[][] {
                        {11,111},
                        {12,212},
                        {13,215}});
        verifyQueryResults(
                "Q6",
                "select c2-c1, c1+c2, count(*) from test group by c1+c2, c2-c1",
                new int[][] {
                        {9,11,1},
                        {8,12,1},
                        {10,12,1},
                        {9,13,2}});
    }
    
    
    public void testSubSelect() throws Exception
    {
        /* query with a group by on a subselect */
        verifyQueryResults(
                "Q1",
                "select a+1, sum(b) from (select c1+1  a , c2+1 b from test) t group by a",
                new int[][] {
                        {3,23}, {4,35}});
        
        verifyQueryResults(
                "Q2",
                "select a+1, sum(b) from (select c1+1  a , c2+1 b from test) t group by a+1",
                new int[][] {
                        {3,23}, {4,35}});
        
        verifyQueryResults(
                "Q3",
                "select b/2,sum(a) from " +
                "(select c1+1 a, max(c2) b from test group by c1+1) t " +
                "group by b/2",
                new int[][] {{5,5}});
    }
    

    public void testMiscExpressions() throws Exception
    {
        // cast
        verifyQueryResults(
                "cast",
                "select (cast (c1 as char(2))), count(*) from test " +
                " group by (cast (c1 as char(2)))",
                new Object[][] {
                        {"1 ", new Integer(2)}, 
                        {"2 ", new Integer(3)}});
        
        // coalesce
        verifyQueryResults(
                "coalesce",
                "select (coalesce(vc1,vc2)), count(*) from coal " +
                " group by (coalesce(vc1,vc2))",
                new Object[][] {{"1", new Integer(2)}, {"2", new Integer(1)}});
        // concat
        verifyQueryResults(
                "concat",
                "select c||v, count(*) from alltypes group by c||v",
                new Object[][] {
                        {"duplicate noone is here", new Integer(1)},
                        {"duplicate this is duplicated", new Integer(13)},
                        {"goodbye   this is duplicated", new Integer(1)}});
        // conditional.
        verifyQueryResults(
                "cond",
                "select (case when c1 = 1 then 2 else 1 end), sum(c2) from test " +
                " group by (case when c1 = 1 then 2 else 1 end)",
                new int[][] {{1,32}, {2, 21}});
        
        // length
        verifyQueryResults(
                "length",
                "select length(v), count(*) from alltypes group by length(v)",
                new int[][] {{13,1},{18,14}});
        
        // current time. ignore the value of current time. 
        // just make sure we can group by it and get the right results for
        // the aggregate.
        verifyQueryResults(
                "current_time",
                "select co from " +
                "(select current_time ct, count(*) co from test t1, test t2, test t3 group by current_time) t",
                new int[][] {{125}});
        // concat + substr
        verifyQueryResults(
                "concat+substr",
                "select substr(c||v, 1, 4), count(*) from alltypes group by substr(c||v, 1, 4)",
                new Object[][] {
                        {"dupl", new Integer(14)},
                        {"good", new Integer(1)}});

    }
    
    public void testExtractOperator() throws Exception
    {
        verifyQueryResults(
                "year",
                "select year(dt), count(*) from alltypes group by year(dt)",
                new int[][] {{1992, 15}});
        verifyQueryResults(
                "month",
                "select month(dt), count(*) from alltypes group by month(dt)",
                new int[][] {{1,5},{2,6},{3,4}});
        verifyQueryResults(
                "day",
                "select day(dt), count(*) from alltypes group by day(dt)",
                new int[][] {{1,3},{2,3},{3,3},{4,3},{5,2},{6,1}});
        verifyQueryResults(
                "hour",
                "select hour(t), count(*) from alltypes group by hour(t)",
                new int[][] {{12, 15}});
        verifyQueryResults(
                "hour2",
                "select hour(ts), count(*) from alltypes group by hour(ts)",
                new int[][] {{12,15}});
        verifyQueryResults(
                "minute",
                "select minute(ts), count(*) from alltypes group by minute(ts)",
                new int[][] {{30,14},{55,1}});
        verifyQueryResults(
                "second",
                "select second(t), count(*) from alltypes group by second(t)",
                new int[][]{
                        {30,2},{31,1},{32,1},{33,1},{34,1},{35,1},
                        {36,1},{37,1},{38,1},{39,1},{40,1},{41,1},
                        {42,1},{55,1}});
    }
    
    /**
     * Check that duplicate columns are now allowed in group by's. Earlier
     * (pre 883), derby would flag an error.
     *
     */
    public void testDuplicateColumns() throws Exception
    {
        verifyQueryResults(
                "Q1",
                "select c1, sum(c2) from test group by c1,c1",
                new int[][]{ {1,21}, {2,32}});
        
        verifyQueryResults(
                "Q2",
                "select c1, c1, sum(c2) from test group by c1,c1",
                new int[][]{ {1,1,21}, {2,2,32}});
    }
    /**
     * Negative tests. These queries should not compile at all.
     */
    public void testNegative()
    {
        // disallow java function 
        assertCompileError(
                "42Y30", "select r(), count(*) from test group by r()");

        // invalid grouping expression.
        assertCompileError(
                "42Y30", "select c1+1, count(*) from test group by c1+2");
        
        // again invalid grouping expression because cast type is different.
        assertCompileError(
                "42Y30", "select (cast (c as char(2))), count(*) " +
                " from alltypes group by (cast (c as char(3)))");

        // same column name, same table but different tablenumber in the query
        assertCompileError(
                "42Y30", 
                "select t1.c1, count(*) from test t1, test t2 " + 
                " group by t2.c1");
        // ternary operator, not equivalent test.
        assertCompileError(
                "42Y30",
                "select substr(c, 3, 4) from alltypes group by substr(v, 3, 4)");
        // aggregates in group by list.
        assertCompileError(
                "42Y26",
                "select 1, max(c1) from test group by max(c1)");
    }
    /* --------------------- begin helper methods -------------------- */
    
    private Object[] intRow(int[] expected)
    {
        Object[] arr = new Object[expected.length];
        for (int i = 0; i < expected.length; i++)
        {
            arr[i] = new Integer(expected[i]);
        }
        return arr;
    }
    
    private void verifyQueryResults(
            String assertString, String query, Object[][] golden)
        throws Exception
    {

        PreparedStatement ps = prepareStatement(query);
        ResultSet rs = ps.executeQuery();
        for (int i = 0; i < golden.length; i++)
        {
            assertTrue(
                    "Not enough rows. Expected " + golden.length + 
                    " but found " + i, 
                    rs.next());

            assertRow(assertString + ":Row:" + i, rs, golden[i]);
        }
        rs.close();
        ps.close();
    }
    
    private void verifyQueryResults(
            String assertString, String query, int[][] golden) 
        throws Exception
    {
        PreparedStatement ps = prepareStatement(query);
        ResultSet rs = ps.executeQuery();
        for (int i = 0; i < golden.length; i++)
        {
            assertTrue(
                "Not enough rows. Expected " + golden.length + 
                " but found " + i, 
                rs.next());
            assertRow(assertString + ":Row:" + i, rs, intRow(golden[i]));
        }
        assertFalse("more rows than expected", rs.next());
        rs.close();
        ps.close();
    }
    
    public void    assertRow(
            String assertString, ResultSet rs, Object[] expectedRow)
        throws Exception
    {
        int        count = expectedRow.length;
        
        for ( int i = 0; i < count; i++ )
        {
            int    columnNumber = i + 1;
            Object expected = expectedRow[i];
            Object actual = rs.getObject(columnNumber);
            assertEquals(assertString + ":Column number ", expected, actual);
        }
    }

    /* ------------------- end helper methods  -------------------------- */ 
    public GroupByExpressionTest(String name)
    {
        super(name);
    }
    
    public static Test suite()
    {
        TestSuite suite = new TestSuite("GROUP BY expression tests");
        suite.addTestSuite(GroupByExpressionTest.class);
        
        TestSetup wrapper = new BaseJDBCTestSetup(suite) { 
            public void setUp() throws Exception
            { 
                Connection c = getConnection();
                c.setAutoCommit(false);
                Statement s = c.createStatement();
                for (int i = 0; i < TABLES.length; i++) {
                    s.execute(TABLES[i][1]);
                }
                for (int i = 0; i < FUNCTIONS.length; i++) {
                    s.execute(FUNCTIONS[i][1]);
                }
                
                s.execute("insert into test values (1, 10, 100, 1000)");
                s.execute("insert into test values (1, 11, 100, 1001)");
                s.execute("insert into test values (2, 10, 100, 1000)");
                s.execute("insert into test values (2, 11, 101, 1001)");
                s.execute("insert into test values (2, 11, 101, 1000)");
                
                s.execute("insert into coal values ('1', '2')");
                s.execute("insert into coal values (null, '2')");
                s.execute("insert into coal values ('1', null)");
                
                s.execute(
                    "insert into alltypes values (0, 100, 1000000, " +
                    "'duplicate', 'this is duplicated', 'also duplicated', " +
                    "200.0e0, 200.0e0, " + 
                    " date('1992-01-01'), time('12:30:30'), " + 
                    " timestamp('1992-01-01 12:30:30'), " +
                    "X'12af', x'0000111100001111', X'1234', 111.11) ");
                s.execute(
                    "insert into alltypes values (0, 100, 1000000, " +
                    " 'duplicate', 'this is duplicated', 'also duplicated', " +
                    " 200.0e0, 200.0e0, "  +
                    " date('1992-01-02'), time('12:30:31'), " + 
                    "timestamp('1992-01-02 12:30:31'), " +
                    " X'12af', X'0000111100001111', X'1234', 111.11)");
                s.execute(
                    "insert into alltypes values (1, 100, 1000000, " +
                    "'duplicate', 'this is duplicated', 'also duplicated', " +
                    " 200.0e0, 200.0e0, " +  
                    " date('1992-01-03'), time('12:30:32'), " + 
                    " timestamp('1992-01-03 12:30:32'), " +
                    " X'12af', X'0000111100001111', X'1234', 111.11)");
                s.execute(
                    "insert into alltypes values (0, 200, 1000000, " +
                    " 'duplicate', 'this is duplicated', 'also duplicated', " +
                    " 200.0e0, 200.0e0, " +
                    " date('1992-01-04'), time('12:30:33'), " + 
                    " timestamp('1992-01-04 12:30:33'), " +
                    " X'12af', X'0000111100001111', X'1234', 222.22)");
                s.execute(
                    "insert into alltypes values (0, 100, 2000000, " +
                    " 'duplicate', 'this is duplicated', 'also duplicated', " +
                    " 200.0e0, 200.0e0,  " +
                    " date('1992-01-05'), time('12:30:34'), " + 
                    " timestamp('1992-01-05 12:30:34'), " +
                    " X'12af', X'0000111100001111', X'1234', 222.22)");
                s.execute("insert into alltypes values (0, 100, 1000000, " +
                    " 'goodbye', 'this is duplicated', 'also duplicated', "  +
                    " 200.0e0, 200.0e0, " +
                    " date('1992-02-01'), time('12:30:35'), " + 
                    " timestamp('1992-02-01 12:30:35'), " +
                    " X'12af', X'0000111100001111', X'1234', 111.11)");
                s.execute("insert into alltypes values (0, 100, 1000000, " +
                    " 'duplicate', 'noone is here', 'jimmie noone was here', " +
                    " 200.0e0, 200.0e0, " +
                    " date('1992-02-02'), time('12:30:36'), " + 
                    " timestamp('1992-02-02 12:30:36'), " +
                    " X'12af', X'0000111100001111', X'1234', 111.11) ");
                s.execute("insert into alltypes values (0, 100, 1000000, " +
                    " 'duplicate', 'this is duplicated', 'also duplicated', " +
                    " 200.0e0, 200.0e0, " +
                    " date('1992-02-03'), time('12:30:37'), " + 
                    " timestamp('1992-02-03 12:30:37'), " +
                    " X'12af', X'0000111100001111', X'1234', 111.11)");
                s.execute("insert into alltypes values (0, 100, 1000000, " +
                    " 'duplicate', 'this is duplicated', 'also duplicated', " +
                    " 100.0e0, 200.0e0, " +
                    " date('1992-02-04'), time('12:30:38'), " + 
                    " timestamp('1992-02-04 12:30:38'), " +
                    " X'12af', X'0000111100001111', X'1234', 111.11) ");
                s.execute("insert into alltypes values (0, 100, 1000000, " +
                    " 'duplicate', 'this is duplicated', 'also duplicated', " +
                    " 200.0e0, 100.0e0, " +
                    " date('1992-02-05'), time('12:30:39'), " + 
                    " timestamp('1992-02-05 12:30:39'), " +
                    " X'12af', X'0000111100001111', X'1234', 111.11) ");
                s.execute("insert into alltypes values (0, 100, 1000000, " +
                    " 'duplicate', 'this is duplicated', 'also duplicated', " +
                    " 200.0e0, 200.0e0, " +
                    " date('1992-02-06'), time('12:30:40'), " + 
                    " timestamp('1992-02-06 12:30:40'), " +
                    " X'12af', X'0000111100001111', X'1234', 111.11) ");
                s.execute("insert into alltypes values (0, 100, 1000000, " +
                    " 'duplicate', 'this is duplicated', 'also duplicated', " +
                    " 200.0e0, 200.0e0, " +
                    " date('1992-03-01'), time('12:55:55'), " + 
                    "timestamp('1992-03-01 12:30:30'), " +
                    " X'12af', X'0000111100001111', X'1234', 111.11) ");
                s.execute("insert into alltypes values (0, 100, 1000000, " +
                    " 'duplicate', 'this is duplicated', 'also duplicated', " +
                    " 200.0e0, 200.0e0, " +
                    " date('1992-03-02'), time('12:30:30'), " + 
                    "timestamp('1992-03-02 12:55:55'), " +
                    " X'12af', X'0000111100001111', X'1234', 111.11) ");
                s.execute("insert into alltypes values (0, 100, 1000000, " +
                    " 'duplicate', 'this is duplicated', 'also duplicated', " +
                    " 200.0e0, 200.0e0, " +
                    " date('1992-03-03'), time('12:30:41'), " + 
                    " timestamp('1992-03-03 12:30:41'), " +
                    " X'ffff', X'0000111100001111', X'1234', 111.11) ");
                s.execute("insert into alltypes values (0, 100, 1000000, " +
                    " 'duplicate', 'this is duplicated', 'also duplicated', " +
                    " 200.0e0, 200.0e0, " +
                    " date('1992-03-04'), time('12:30:42'), " + 
                    " timestamp('1992-03-04 12:30:42'), " +
                    " X'12af', X'1111111111111111', X'1234', 111.11) " );
                
                s.close();
                c.commit();
                c.close();
              }
            protected void tearDown() throws Exception 
            { 
                Connection c = getConnection();
                c.setAutoCommit(false);
                Statement s = c.createStatement();
                
                for (int i = 0; i < TABLES.length; i++) {
                    s.execute("drop table " + TABLES[i][0]);
                }
                for (int i = 0; i < FUNCTIONS.length; i++) {
                    s.execute("drop function " + FUNCTIONS[i][0]);
                }
                
                c.commit();
                super.tearDown();
            }
            }; 
        return wrapper;
    }
}
