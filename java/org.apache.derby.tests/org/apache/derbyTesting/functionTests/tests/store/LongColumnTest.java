/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.store.checkPoint

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
package org.apache.derbyTesting.functionTests.tests.store;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import junit.framework.Test;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.TestConfiguration;

public class LongColumnTest extends BaseJDBCTestCase {

    public LongColumnTest(String name) {
        super(name);
    }
    
    public static Test suite() {
//IC see: https://issues.apache.org/jira/browse/DERBY-6590
        BaseTestSuite suite = new BaseTestSuite("LongColumnTest");
        suite.addTest(TestConfiguration.defaultSuite(LongColumnTest.class));

        return suite;
    }

    protected void setUp() {
        try {
            Statement s = createStatement();
            s.execute("CREATE FUNCTION  PADSTRING (DATA VARCHAR(32000), "
                    + "LENGTH INTEGER) RETURNS VARCHAR(32000) EXTERNAL NAME "
                    + "'org.apache.derbyTesting.functionTests.util.Formatters"
                    + ".padString' LANGUAGE JAVA PARAMETER STYLE JAVA");
            s.execute("CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY"
                    + "('derby.storage.pageSize', '4096')");
            s.execute("CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY"
                    + "('derby.storage.pageCacheSize', '40')");
            s.close();
        } catch (SQLException se) {
            se.printStackTrace();
        }
        
        try {
            dropTable("testing");
        } catch (SQLException e) {
            //ignore
        }
    }

    public void tearDown() throws Exception {
        Statement st = createStatement();
        st.executeUpdate("DROP FUNCTION PADSTRING");
        st.close();
        dropTable("testing");
//IC see: https://issues.apache.org/jira/browse/DERBY-5723
        super.tearDown();
    }

    /**
     * test 1: one long column
     */
    public void testOneLongColumn() throws SQLException {
        Statement st = createStatement();
        
        st.execute("create table testing (a varchar(8096))");
        st.execute("insert into testing values (PADSTRING('1 2 3 4 5 6 7 8 9 0', 8096))");
        st.execute("insert into testing values (PADSTRING('a b c d e f g h i j', 8096))");
        st.execute("insert into testing values (PADSTRING('11 22 33 44 55 66 77', 8096))");
        st.execute("insert into testing values (PADSTRING('aa bb cc dd ee ff gg', 8096))");
        
        ResultSet rs = st.executeQuery("select a from testing");  
        JDBC.assertFullResultSet(rs, new String[][]{
                {"1 2 3 4 5 6 7 8 9 0"},
                {"a b c d e f g h i j"},
                {"11 22 33 44 55 66 77"},
                {"aa bb cc dd ee ff gg"}
        });
        
        st.close();
    }

    /**
     * test 2: testing two column (1 short, 1 long) table
     */
    public void testTwoColumnsShortAndLong() throws SQLException {
        Statement st = createStatement();
        
        st.execute("create table testing (a int, b varchar(32384))");
        st.execute("insert into testing values (1, PADSTRING('1 2 3 4 5 6 7 8 9 0', 32384))");
        st.execute("insert into testing values (2, PADSTRING('a b c d e f g h i j', 32384))");
        st.execute("insert into testing values (3, PADSTRING('11 22 33 44 55 66 77', 32384))");
        st.execute("insert into testing values (4, PADSTRING('aa bb cc dd ee ff gg', 32384))");
        
        ResultSet rs = st.executeQuery("select * from testing");
        JDBC.assertFullResultSet(rs, new String[][]{
                {"1", "1 2 3 4 5 6 7 8 9 0"},
                {"2", "a b c d e f g h i j"},
                {"3", "11 22 33 44 55 66 77"},
                {"4", "aa bb cc dd ee ff gg"}
        });
        
        rs = st.executeQuery("select a from testing");        
        JDBC.assertFullResultSet(rs, new String[][]{
                {"1"},
                {"2"},
                {"3"},
                {"4"}
        });
        
        rs = st.executeQuery("select b from testing");        
        JDBC.assertFullResultSet(rs, new String[][]{
                {"1 2 3 4 5 6 7 8 9 0"},
                {"a b c d e f g h i j"},
                {"11 22 33 44 55 66 77"},
                {"aa bb cc dd ee ff gg"}
        });
        
        rs = st.executeQuery("select b from testing where a = 1");        
        JDBC.assertFullResultSet(rs, new String[][]{
                {"1 2 3 4 5 6 7 8 9 0"},
        });
        
        st.close();        
    }
    
    /**
     * test 3: testing two column (1 long, 1 shor) table
     */
    public void testTwoColumnsLongAndShort() throws SQLException {
        Statement st = createStatement();
        
        st.execute("create table testing (a varchar(32384), b int)");
        st.execute("insert into testing values (PADSTRING('1 2 3 4 5 6 7 8 9 0', 32384), 1)");
        st.execute("insert into testing values (PADSTRING('a b c d e f g h i j', 32384), 2)");
        st.execute("insert into testing values (PADSTRING('11 22 33 44 55 66 77', 32384), 3)");
        st.execute("insert into testing values (PADSTRING('aa bb cc dd ee ff gg', 32384), 4)");
        
        ResultSet rs = st.executeQuery("select * from testing");        
        JDBC.assertFullResultSet(rs, new String[][]{
                {"1 2 3 4 5 6 7 8 9 0", "1"},
                {"a b c d e f g h i j", "2"},
                {"11 22 33 44 55 66 77", "3"},
                {"aa bb cc dd ee ff gg", "4"}
        });
        
        rs = st.executeQuery("select a from testing");        
        JDBC.assertFullResultSet(rs, new String[][]{
                {"1 2 3 4 5 6 7 8 9 0"},
                {"a b c d e f g h i j"},
                {"11 22 33 44 55 66 77"},
                {"aa bb cc dd ee ff gg"}
        });
        
        rs = st.executeQuery("select b from testing");        
        JDBC.assertFullResultSet(rs, new String[][]{
                {"1"},
                {"2"},
                {"3"},
                {"4"}
        });
        
        rs = st.executeQuery("select a from testing where b = 4");        
        JDBC.assertFullResultSet(rs, new String[][]{
                {"aa bb cc dd ee ff gg"}
        });
        
        st.close();        
    }

    /**
     * test 4: testing three column (1 short, 1 long, 1 short) table
     */
    public void testThreeColumnsShortAndLongAndShort() throws SQLException {
        Statement st = createStatement();
        
        st.execute("create table testing (z int, a varchar(32384), b int)");
        st.execute("insert into testing values (0, PADSTRING('1 2 3 4 5 6 7 8 9 0',32384), 1)");
        st.execute("insert into testing values (1, PADSTRING('a b c d e f g h i j',32384), 2)");
        st.execute("insert into testing values (2, PADSTRING('11 22 33 44 55 66 77',32384), 3)");
        st.execute("insert into testing values (4, PADSTRING('aa bb cc dd ee ff gg',32384), 4)");
        
        ResultSet rs = st.executeQuery("select * from testing");        
        JDBC.assertFullResultSet(rs, new String[][]{
                {"0", "1 2 3 4 5 6 7 8 9 0", "1"},
                {"1", "a b c d e f g h i j", "2"},
                {"2", "11 22 33 44 55 66 77", "3"},
                {"4", "aa bb cc dd ee ff gg", "4"}
        });
        
        rs = st.executeQuery("select a from testing");        
        JDBC.assertFullResultSet(rs, new String[][]{
                {"1 2 3 4 5 6 7 8 9 0"},
                {"a b c d e f g h i j"},
                {"11 22 33 44 55 66 77"},
                {"aa bb cc dd ee ff gg"}      
        });
        
        rs = st.executeQuery("select b from testing");        
        JDBC.assertFullResultSet(rs, new String[][]{
                {"1"},
                {"2"},
                {"3"},
                {"4"},
        });
        
        rs = st.executeQuery("select z from testing");        
        JDBC.assertFullResultSet(rs, new String[][]{
                {"0"},
                {"1"},
                {"2"},
                {"4"},
        });
        
        rs = st.executeQuery("select b from testing where z = b");        
        JDBC.assertFullResultSet(rs, new String[][]{
                {"4"}
        });
        
        
        try {
            st.executeUpdate("create index zz on testing (a)");
            fail("try creating btree index on long column, should fail");
        } catch (SQLException e) {
            assertSQLState("XSCB6", e);
        }
        
        st.execute("update testing set a = PADSTRING('update once', 32384)");
        st.execute("update testing set a = PADSTRING('update twice', 32384)");
        st.execute("update testing set a = PADSTRING('update three times', 32384)");
        st.execute("update testing set a = PADSTRING('update four times', 32384)");
        st.execute("update testing set a = PADSTRING('update five times', 32384)");
                
        rs = st.executeQuery("select a from testing");        
        JDBC.assertFullResultSet(rs, new String[][]{
                {"update five times"},
                {"update five times"},
                {"update five times"},
                {"update five times"}
        });
        
        st.close();        
    }
 
    /**
     * test 5: testing three columns (1 long, 1 short, 1 long) table
     */
    public void testThreeColumnsLongAndShortAndLong() throws SQLException {
        Statement st = createStatement();
        
        st.execute("create table testing (a varchar(32384), b int, c varchar(32084))");
        st.execute("insert into testing values (PADSTRING('1 2 3 4 5 6 7 8 9 0',32384),"
                + " 1, PADSTRING('1 2 3 4 5 6 7 8 9 0',32084))");
        st.execute("insert into testing values (PADSTRING('a b c d e f g h i j',32384),"
                + " 2, PADSTRING('a b c d e f g h i j',32084))");
        st.execute("insert into testing values (PADSTRING('11 22 33 44 55 66 77',32384),"
                + " 3, PADSTRING('11 22 33 44 55 66 77',32084))");
        st.execute("insert into testing values (PADSTRING('aa bb cc dd ee ff gg',32384),"
                + " 4, PADSTRING('aa bb cc dd ee ff gg',32084))");
        
        ResultSet rs = st.executeQuery("select * from testing");        
        JDBC.assertFullResultSet(rs, new String[][]{
                {"1 2 3 4 5 6 7 8 9 0", "1", "1 2 3 4 5 6 7 8 9 0"},
                {"a b c d e f g h i j", "2", "a b c d e f g h i j"},
                {"11 22 33 44 55 66 77", "3", "11 22 33 44 55 66 77"},
                {"aa bb cc dd ee ff gg", "4", "aa bb cc dd ee ff gg"}
        });
        
        rs = st.executeQuery("select a from testing");        
        JDBC.assertFullResultSet(rs, new String[][]{
                {"1 2 3 4 5 6 7 8 9 0",},
                {"a b c d e f g h i j",},
                {"11 22 33 44 55 66 77",},
                {"aa bb cc dd ee ff gg",}
        });
        
        rs = st.executeQuery("select b from testing");        
        JDBC.assertFullResultSet(rs, new String[][]{
                {"1"},
                {"2"},
                {"3"},
                {"4"},
        });
        
        rs = st.executeQuery("select c from testing");        
        JDBC.assertFullResultSet(rs, new String[][]{
                {"1 2 3 4 5 6 7 8 9 0"},
                {"a b c d e f g h i j"},
                {"11 22 33 44 55 66 77"},
                {"aa bb cc dd ee ff gg"}
        });
        
        rs = st.executeQuery("select * from testing where b = 4");        
        JDBC.assertFullResultSet(rs, new String[][]{
                {"aa bb cc dd ee ff gg", "4", "aa bb cc dd ee ff gg"}
        });
        
        try {
            st.executeUpdate("create index zz on testing (a)");
            fail("try creating btree index, should fail on long columns");
        } catch (SQLException e) {
            assertSQLState("XSCB6", e);
        }
        
        try {
            st.executeUpdate("create index zz on testing (c)");
            fail("try creating btree index, should fail on long columns");
        } catch (SQLException e) {
            assertSQLState("XSCB6", e);
        }
        
        st.executeUpdate("create index zz on testing (b)");
        
        
        st.execute("update testing set c = PADSTRING('update 0', 32084)");
        st.execute("update testing set c = PADSTRING('update 1', 32084)");
        st.execute("update testing set c = PADSTRING('update 2', 32084)");
        st.execute("update testing set c = PADSTRING('update 3', 32084)");
        st.execute("update testing set c = PADSTRING('update 4', 32084)");
        st.execute("update testing set c = PADSTRING('update 5', 32084)");
        st.execute("update testing set c = PADSTRING('update 6', 32084)");
        st.execute("update testing set c = PADSTRING('update 7', 32084)");
        st.execute("update testing set c = PADSTRING('update 8', 32084)");
        st.execute("update testing set c = PADSTRING('update 9', 32084)");
        
        rs = st.executeQuery("select * from testing");        
        JDBC.assertFullResultSet(rs, new String[][]{
                {"1 2 3 4 5 6 7 8 9 0", "1", "update 9"},
                {"a b c d e f g h i j", "2", "update 9"},
                {"11 22 33 44 55 66 77", "3", "update 9"},
                {"aa bb cc dd ee ff gg", "4", "update 9"}
        });
        
        st.close();        
    }
    
    /**
     * test 6: table with 5 columns (1 short, 1 long, 1 short, 1 long, 1 short) table
     */
    public void testFiveColumnsSLSLS() throws SQLException {
        Statement st = createStatement();
        
        st.execute("create table testing (a int, b clob(64768), c int, d varchar(32384), e int)");
        st.execute("insert into testing values (0, PADSTRING('1 2 3 4 5 6 7 8 9 0', 64768),"
                + "  1, PADSTRING('1 2 3 4 5 6 7 8 9 0', 32384),  2)");
        st.execute("insert into testing values (1, PADSTRING('a b c d e f g h i j', 64768),"
                + "  2, PADSTRING('a b c d e f g h i j', 32384),  3)");
        st.execute("insert into testing values (2, PADSTRING('11 22 33 44 55 66 77', 64768),"
                + " 3, PADSTRING('11 22 33 44 55 66 77', 32384), 4)");
        st.execute("insert into testing values (3, PADSTRING('aa bb cc dd ee ff gg', 64768),"
                + " 4, PADSTRING('aa bb cc dd ee ff gg',32384), 5)");
        st.execute("insert into testing values (4, PADSTRING('1 2 3 4 5 6 7 8 9 0', 64768),"
                + "  5, PADSTRING('aa bb cc dd ee ff gg',32384), 6)");
        st.execute("insert into testing values (5, PADSTRING('a b c d e f g h i j', 64768),"
                + "  6, PADSTRING('aa bb cc dd ee ff gg',32384), 7)");
        st.execute("insert into testing values (6, PADSTRING('11 22 33 44 55 66 77', 64768),"
                + " 7, PADSTRING('aa bb cc dd ee ff gg',32384), 8)");
        st.execute("insert into testing values (7, PADSTRING('aa bb cc dd ee ff gg', 64768),"
                + " 8, PADSTRING('aa bb cc dd ee ff gg',32384), 9)");
        
        ResultSet rs = st.executeQuery("select * from testing");        
        JDBC.assertFullResultSet(rs, new String[][]{
                {"0", "1 2 3 4 5 6 7 8 9 0", "1", "1 2 3 4 5 6 7 8 9 0", "2"},
                {"1", "a b c d e f g h i j", "2", "a b c d e f g h i j", "3"},
                {"2", "11 22 33 44 55 66 77", "3", "11 22 33 44 55 66 77", "4"},
                {"3", "aa bb cc dd ee ff gg", "4", "aa bb cc dd ee ff gg", "5"},
                {"4", "1 2 3 4 5 6 7 8 9 0", "5", "aa bb cc dd ee ff gg", "6"},
                {"5", "a b c d e f g h i j", "6", "aa bb cc dd ee ff gg", "7"},
                {"6", "11 22 33 44 55 66 77", "7", "aa bb cc dd ee ff gg", "8"},
                {"7", "aa bb cc dd ee ff gg", "8", "aa bb cc dd ee ff gg", "9"}
        });
        
        rs = st.executeQuery("select a from testing");        
        JDBC.assertFullResultSet(rs, new String[][]{
                {"0"},
                {"1"},
                {"2"},
                {"3"},
                {"4"},
                {"5"},
                {"6"},
                {"7"},
        });
        
        
        rs = st.executeQuery("select b from testing");        
        JDBC.assertFullResultSet(rs, new String[][]{
                {"1 2 3 4 5 6 7 8 9 0"},
                {"a b c d e f g h i j"},
                {"11 22 33 44 55 66 77"},
                {"aa bb cc dd ee ff gg"},
                {"1 2 3 4 5 6 7 8 9 0"},
                {"a b c d e f g h i j"},
                {"11 22 33 44 55 66 77"},
                {"aa bb cc dd ee ff gg"}
        });
        
        rs = st.executeQuery("select a, c, d from testing");        
        JDBC.assertFullResultSet(rs, new String[][]{
                {"0", "1", "1 2 3 4 5 6 7 8 9 0"},
                {"1", "2", "a b c d e f g h i j"},
                {"2", "3", "11 22 33 44 55 66 77"},
                {"3", "4", "aa bb cc dd ee ff gg"},
                {"4", "5", "aa bb cc dd ee ff gg"},
                {"5", "6", "aa bb cc dd ee ff gg"},
                {"6", "7", "aa bb cc dd ee ff gg"},
                {"7", "8", "aa bb cc dd ee ff gg"}
        });

        
        st.execute("update testing set b = PADSTRING('update 0', 64768)");
        st.execute("update testing set b = PADSTRING('update 1', 64768)");
        st.execute("update testing set b = PADSTRING('update 2', 64768)");
        st.execute("update testing set b = PADSTRING('update 3', 64768)");
        st.execute("update testing set b = PADSTRING('update 4', 64768)");
        st.execute("update testing set b = PADSTRING('update 5', 64768)");
        st.execute("update testing set b = PADSTRING('update 6', 64768)");
        st.execute("update testing set b = PADSTRING('update 7', 64768)");
        st.execute("update testing set b = PADSTRING('update 8', 64768)");
        st.execute("update testing set b = PADSTRING('update 9', 64768)");
        
        rs = st.executeQuery("select b from testing");
        JDBC.assertFullResultSet(rs, new String[][]{
                {"update 9"},
                {"update 9"},
                {"update 9"},
                {"update 9"},
                {"update 9"},
                {"update 9"},
                {"update 9"},
                {"update 9"},
        });
        
        rs = st.executeQuery("select a, b, e from testing");
        JDBC.assertFullResultSet(rs, new String[][]{
                {"0", "update 9", "2"},
                {"1", "update 9", "3"},
                {"2", "update 9", "4"},
                {"3", "update 9", "5"},
                {"4", "update 9", "6"},
                {"5", "update 9", "7"},
                {"6", "update 9", "8"},
                {"7", "update 9", "9"}
        });
        
        st.close();
    }

    /**
     * test 7: table with 5 columns, all long columns
     */
    public void testFiveColumnsAllLong() throws SQLException {
        Statement st = createStatement();
        
        st.execute("create table testing"
                + " (a clob(64768), b varchar(32384), c clob(64768), d varchar(32384), e clob(64768))");
        
        for (int i = 0; i < 10; i++) {
            st.execute("insert into testing values (PADSTRING('a a a a a a a a a a',64768),"
                    + " PADSTRING('b b b b b b b b b b',32384), PADSTRING('c c c c c c c c c c',64768),"
                    + " PADSTRING('d d d d d d d d d d', 32384), PADSTRING('e e e e e e e e',64768))");
        }
        
        ResultSet rs = st.executeQuery("select * from testing");        
        JDBC.assertFullResultSet(rs, new String[][]{
                {"a a a a a a a a a a", "b b b b b b b b b b", "c c c c c c c c c c", 
                    "d d d d d d d d d d", "e e e e e e e e"},
                {"a a a a a a a a a a", "b b b b b b b b b b", "c c c c c c c c c c", 
                    "d d d d d d d d d d", "e e e e e e e e"},
                {"a a a a a a a a a a", "b b b b b b b b b b", "c c c c c c c c c c", 
                    "d d d d d d d d d d", "e e e e e e e e"},
                {"a a a a a a a a a a", "b b b b b b b b b b", "c c c c c c c c c c", 
                    "d d d d d d d d d d", "e e e e e e e e"},
                {"a a a a a a a a a a", "b b b b b b b b b b", "c c c c c c c c c c", 
                    "d d d d d d d d d d", "e e e e e e e e"},
                {"a a a a a a a a a a", "b b b b b b b b b b", "c c c c c c c c c c", 
                    "d d d d d d d d d d", "e e e e e e e e"},
                {"a a a a a a a a a a", "b b b b b b b b b b", "c c c c c c c c c c", 
                    "d d d d d d d d d d", "e e e e e e e e"},
                {"a a a a a a a a a a", "b b b b b b b b b b", "c c c c c c c c c c", 
                    "d d d d d d d d d d", "e e e e e e e e"},
                {"a a a a a a a a a a", "b b b b b b b b b b", "c c c c c c c c c c", 
                    "d d d d d d d d d d", "e e e e e e e e"},
                {"a a a a a a a a a a", "b b b b b b b b b b", "c c c c c c c c c c", 
                    "d d d d d d d d d d", "e e e e e e e e"},                        
        });
        
        rs = st.executeQuery("select a from testing");        
        JDBC.assertFullResultSet(rs, new String[][]{
                {"a a a a a a a a a a",},
                {"a a a a a a a a a a",},
                {"a a a a a a a a a a",},
                {"a a a a a a a a a a",},
                {"a a a a a a a a a a",},
                {"a a a a a a a a a a",},
                {"a a a a a a a a a a",},
                {"a a a a a a a a a a",},
                {"a a a a a a a a a a",},
                {"a a a a a a a a a a",},                        
        });
        
        rs = st.executeQuery("select b from testing");        
        JDBC.assertFullResultSet(rs, new String[][]{
                {"b b b b b b b b b b",},
                {"b b b b b b b b b b",},
                {"b b b b b b b b b b",},
                {"b b b b b b b b b b",},
                {"b b b b b b b b b b",},
                {"b b b b b b b b b b",},
                {"b b b b b b b b b b",},
                {"b b b b b b b b b b",},
                {"b b b b b b b b b b",},
                {"b b b b b b b b b b",},                        
        });
        
        rs = st.executeQuery("select c from testing");        
        JDBC.assertFullResultSet(rs, new String[][]{
                {"c c c c c c c c c c",},
                {"c c c c c c c c c c",},
                {"c c c c c c c c c c",},
                {"c c c c c c c c c c",},
                {"c c c c c c c c c c",},
                {"c c c c c c c c c c",},
                {"c c c c c c c c c c",},
                {"c c c c c c c c c c",},
                {"c c c c c c c c c c",},
                {"c c c c c c c c c c",},                        
        });
        
        rs = st.executeQuery("select d from testing");        
        JDBC.assertFullResultSet(rs, new String[][]{
                {"d d d d d d d d d d",},
                {"d d d d d d d d d d",},
                {"d d d d d d d d d d",},
                {"d d d d d d d d d d",},
                {"d d d d d d d d d d",},
                {"d d d d d d d d d d",},
                {"d d d d d d d d d d",},
                {"d d d d d d d d d d",},
                {"d d d d d d d d d d",},
                {"d d d d d d d d d d",},                        
        });
        
        rs = st.executeQuery("select e from testing");        
        JDBC.assertFullResultSet(rs, new String[][]{
                {"e e e e e e e e",},
                {"e e e e e e e e",},
                {"e e e e e e e e",},
                {"e e e e e e e e",},
                {"e e e e e e e e",},
                {"e e e e e e e e",},
                {"e e e e e e e e",},
                {"e e e e e e e e",},
                {"e e e e e e e e",},
                {"e e e e e e e e",},                        
        });
        
        rs = st.executeQuery("select a, c, e from testing");        
        JDBC.assertFullResultSet(rs, new String[][]{
                {"a a a a a a a a a a", "c c c c c c c c c c", "e e e e e e e e"},
                {"a a a a a a a a a a", "c c c c c c c c c c", "e e e e e e e e"},
                {"a a a a a a a a a a", "c c c c c c c c c c", "e e e e e e e e"},
                {"a a a a a a a a a a", "c c c c c c c c c c", "e e e e e e e e"},
                {"a a a a a a a a a a", "c c c c c c c c c c", "e e e e e e e e"},
                {"a a a a a a a a a a", "c c c c c c c c c c", "e e e e e e e e"},
                {"a a a a a a a a a a", "c c c c c c c c c c", "e e e e e e e e"},
                {"a a a a a a a a a a", "c c c c c c c c c c", "e e e e e e e e"},
                {"a a a a a a a a a a", "c c c c c c c c c c", "e e e e e e e e"},
                {"a a a a a a a a a a", "c c c c c c c c c c", "e e e e e e e e"},                        
        });
        
        rs = st.executeQuery("select b, e from testing");        
        JDBC.assertFullResultSet(rs, new String[][]{
                {"b b b b b b b b b b", "e e e e e e e e"},
                {"b b b b b b b b b b", "e e e e e e e e"},
                {"b b b b b b b b b b", "e e e e e e e e"},
                {"b b b b b b b b b b", "e e e e e e e e"},
                {"b b b b b b b b b b", "e e e e e e e e"},
                {"b b b b b b b b b b", "e e e e e e e e"},
                {"b b b b b b b b b b", "e e e e e e e e"},
                {"b b b b b b b b b b", "e e e e e e e e"},
                {"b b b b b b b b b b", "e e e e e e e e"},
                {"b b b b b b b b b b", "e e e e e e e e"},                        
        });
        
        
        st.execute("update testing set a = PADSTRING('1 1 1 1 1 1 1 1 1 1', 64768)");
        st.execute("update testing set e = PADSTRING('9 9 9 9 9 9 9 9 9 9',64768)");
        
        rs = st.executeQuery("select a, e from testing");
        JDBC.assertFullResultSet(rs, new String[][]{
                {"1 1 1 1 1 1 1 1 1 1", "9 9 9 9 9 9 9 9 9 9"},
                {"1 1 1 1 1 1 1 1 1 1", "9 9 9 9 9 9 9 9 9 9"},
                {"1 1 1 1 1 1 1 1 1 1", "9 9 9 9 9 9 9 9 9 9"},
                {"1 1 1 1 1 1 1 1 1 1", "9 9 9 9 9 9 9 9 9 9"},
                {"1 1 1 1 1 1 1 1 1 1", "9 9 9 9 9 9 9 9 9 9"},
                {"1 1 1 1 1 1 1 1 1 1", "9 9 9 9 9 9 9 9 9 9"},
                {"1 1 1 1 1 1 1 1 1 1", "9 9 9 9 9 9 9 9 9 9"},
                {"1 1 1 1 1 1 1 1 1 1", "9 9 9 9 9 9 9 9 9 9"},
                {"1 1 1 1 1 1 1 1 1 1", "9 9 9 9 9 9 9 9 9 9"},
                {"1 1 1 1 1 1 1 1 1 1", "9 9 9 9 9 9 9 9 9 9"},                        
        });
        
        rs = st.executeQuery("select a, c, b, e from testing");
        JDBC.assertFullResultSet(rs, new String[][]{
                {"1 1 1 1 1 1 1 1 1 1", "c c c c c c c c c c", "b b b b b b b b b b",
                    "9 9 9 9 9 9 9 9 9 9"},
                {"1 1 1 1 1 1 1 1 1 1", "c c c c c c c c c c", "b b b b b b b b b b",
                    "9 9 9 9 9 9 9 9 9 9"},                    
                {"1 1 1 1 1 1 1 1 1 1", "c c c c c c c c c c", "b b b b b b b b b b",
                    "9 9 9 9 9 9 9 9 9 9"},
                {"1 1 1 1 1 1 1 1 1 1", "c c c c c c c c c c", "b b b b b b b b b b",
                    "9 9 9 9 9 9 9 9 9 9"},
                {"1 1 1 1 1 1 1 1 1 1", "c c c c c c c c c c", "b b b b b b b b b b",
                    "9 9 9 9 9 9 9 9 9 9"},                    
                {"1 1 1 1 1 1 1 1 1 1", "c c c c c c c c c c", "b b b b b b b b b b",
                    "9 9 9 9 9 9 9 9 9 9"},
                {"1 1 1 1 1 1 1 1 1 1", "c c c c c c c c c c", "b b b b b b b b b b",
                    "9 9 9 9 9 9 9 9 9 9"},
                {"1 1 1 1 1 1 1 1 1 1", "c c c c c c c c c c", "b b b b b b b b b b",
                    "9 9 9 9 9 9 9 9 9 9"},                    
                {"1 1 1 1 1 1 1 1 1 1", "c c c c c c c c c c", "b b b b b b b b b b",
                    "9 9 9 9 9 9 9 9 9 9"},
                {"1 1 1 1 1 1 1 1 1 1", "c c c c c c c c c c", "b b b b b b b b b b",
                    "9 9 9 9 9 9 9 9 9 9"},
        });

        rs = st.executeQuery("select e from testing");
        JDBC.assertFullResultSet(rs, new String[][]{
                {"9 9 9 9 9 9 9 9 9 9"},
                {"9 9 9 9 9 9 9 9 9 9"},
                {"9 9 9 9 9 9 9 9 9 9"},
                {"9 9 9 9 9 9 9 9 9 9"},
                {"9 9 9 9 9 9 9 9 9 9"},
                {"9 9 9 9 9 9 9 9 9 9"},
                {"9 9 9 9 9 9 9 9 9 9"},
                {"9 9 9 9 9 9 9 9 9 9"},
                {"9 9 9 9 9 9 9 9 9 9"},
                {"9 9 9 9 9 9 9 9 9 9"},                        
        });
        
        st.close();
    }
}
