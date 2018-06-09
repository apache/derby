/**
 *  Derby - Class org.apache.derbyTesting.functionTests.tests.lang.ScrollCursors1Test
 *  
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.derbyTesting.functionTests.tests.lang;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import junit.framework.Test;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.RuntimeStatisticsParser;
import org.apache.derbyTesting.junit.TestConfiguration;

public class ScrollCursors1Test extends BaseJDBCTestCase {

    public ScrollCursors1Test(String name) {
        super(name);

    }

    public void testForwardOnlyNegative() throws SQLException {
        Connection conn = getConnection();
        PreparedStatement ps_c1 = conn.prepareStatement("select i from t1");
        ResultSet rs = ps_c1.executeQuery();
        try {
            rs.getRow();
            // client and embedded differ on getRow().
            // spec says getRow is optional for forward only cursors.
            if (usingEmbedded())
                fail("getRow succeeded on forward only cursor");
        } catch (SQLException se) {
            assertSQLState("XJ061",se);
            
        }
        try {
            rs.first();
            fail("first() not allowed on forward only result set");
        } catch(SQLException se) {
            if (usingEmbedded())
                assertSQLState("XJ061",se);
            else
                assertSQLState("XJ125",se);
        }
        try {
            rs.last();
            fail("last() not allowed on forward only result set");
        } catch(SQLException se) {
            if (usingEmbedded())
                assertSQLState("XJ061",se);
            else
                assertSQLState("XJ125",se);
        }
        try {
            rs.previous();
            fail("previous() not allowed on forward only result set");
        } catch(SQLException se) {
            if (usingEmbedded())
                assertSQLState("XJ061",se);
            else
                assertSQLState("XJ125",se);
        }
        try {
            rs.beforeFirst();
            fail("beforeFirst() not allowed on forward only result set");
        } catch(SQLException se) {
            if (usingEmbedded())
                assertSQLState("XJ061",se);
            else
                assertSQLState("XJ125",se);
        }
        try {
            rs.afterLast();
            fail("afterLast() not allowed on forward only result set");
        } catch(SQLException se) {
            if (usingEmbedded())
                assertSQLState("XJ061",se);
            else
                assertSQLState("XJ125",se);
        }
        try {
            rs.absolute(1);
            fail("absolute() not allowed on forward only result set");
        } catch(SQLException se) {
            if (usingEmbedded())
                assertSQLState("XJ061",se);
            else
                assertSQLState("XJ125",se);
        }
        try {
            rs.relative(1);
            fail("relative() not allowed on forward only result set");
        } catch(SQLException se) {
            if (usingEmbedded())
                assertSQLState("XJ061",se);
            else
                assertSQLState("XJ125",se);
        }
        rs.close();
        ps_c1.close();
    }

        public void testScrollInsensitive() throws SQLException {
            Connection conn = getConnection();
            conn.setAutoCommit(false);
            PreparedStatement ps_c1 = conn.prepareStatement("select * from t1",
                        ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_READ_ONLY );
            ResultSet rs = ps_c1.executeQuery();
            rs.absolute(0);
            assertNoCurrentRow(rs);
            rs.close();
            
            rs = ps_c1.executeQuery();
            rs.relative(0);
            assertNoCurrentRow(rs);
            rs.close();
            rs = ps_c1.executeQuery();
            
            rs.relative(2);
            assertEquals("c",rs.getString(1).trim());
            assertEquals(3, rs.getInt(2));
            rs.close();
            rs = ps_c1.executeQuery();
            rs.first();
            assertEquals("b", rs.getString(1).trim());
            assertEquals(2, rs.getInt(2));
            assertEquals(1, rs.getRow());
            rs.next();
            assertEquals("c",rs.getString(1).trim());
            assertEquals(3, rs.getInt(2));
            assertEquals(2,rs.getRow());
            assertEquals(2,rs.getRow());
            rs.first();
            assertEquals("b", rs.getString(1).trim());
            assertEquals(2, rs.getInt(2));
            assertEquals(1, rs.getRow());
            
            rs.next();
            assertEquals("c",rs.getString(1).trim());
            assertEquals(3, rs.getInt(2));
            assertEquals(2,rs.getRow());
            
            rs.next();
            assertEquals("d",rs.getString(1).trim());
            assertEquals(4, rs.getInt(2));
            assertEquals(3,rs.getRow());
            
            rs.first();
            assertEquals("b", rs.getString(1).trim());
            assertEquals(2, rs.getInt(2));
            assertEquals(1, rs.getRow());
            
            rs.next();
            assertEquals("c",rs.getString(1).trim());
            assertEquals(3, rs.getInt(2));
            assertEquals(2,rs.getRow());
            
            rs.afterLast();
            assertNoCurrentRow(rs);
            
            assertEquals(0,rs.getRow());
            assertFalse(rs.next());
            assertNoCurrentRow(rs);
            
            assertEquals(0,rs.getRow());
            
            rs.previous();
            assertEquals("m",rs.getString(1).trim());
            assertEquals(13, rs.getInt(2));
            assertEquals(12, rs.getRow());
            rs.previous();
            assertEquals("l",rs.getString(1).trim());
            assertEquals(12, rs.getInt(2));
            assertEquals(11, rs.getRow());
            
            rs.last();
            assertEquals("m",rs.getString(1).trim());
            assertEquals(13, rs.getInt(2));
            assertEquals(12, rs.getRow());
            rs.beforeFirst();
            assertNoCurrentRow(rs);
            assertEquals(0,rs.getRow());
            rs.next();
            assertEquals("b", rs.getString(1).trim());
            assertEquals(2, rs.getInt(2));
            assertEquals(1, rs.getRow());
            
            rs.absolute(12);
            assertEquals("m",rs.getString(1).trim());
            assertEquals(13, rs.getInt(2));
            assertEquals(12, rs.getRow());
            
            rs.absolute(-11);
            assertEquals("c",rs.getString(1).trim());
            assertEquals(3, rs.getInt(2));
            assertEquals(2,rs.getRow());
            
            rs.absolute(13);
            assertNoCurrentRow(rs);
            assertEquals(0,rs.getRow());
            
            
            rs.absolute(-1);
            assertEquals("m",rs.getString(1).trim());
            assertEquals(13, rs.getInt(2));
            assertEquals(12, rs.getRow());
            rs.close();
   
            rs = ps_c1.executeQuery();
            // do last first
            rs.last();
            assertEquals("m",rs.getString(1).trim());
            assertEquals(13, rs.getInt(2));
            assertEquals(12, rs.getRow());
            
            assertFalse(rs.next());
            assertNoCurrentRow(rs);
           
            rs.last();
            assertEquals("m",rs.getString(1).trim());
            assertEquals(13, rs.getInt(2));
            assertEquals(12, rs.getRow());
            
            rs.previous();
            assertEquals("l",rs.getString(1).trim());
            assertEquals(12, rs.getInt(2));
            assertEquals(11, rs.getRow());
            
            rs.first();
            assertEquals("b", rs.getString(1).trim());
            assertEquals(2, rs.getInt(2));
            assertEquals(1, rs.getRow());

            rs.previous();
            assertNoCurrentRow(rs);
            rs.next();
            assertEquals("b", rs.getString(1).trim());
            assertEquals(2, rs.getInt(2));
            assertEquals(1, rs.getRow());
            rs.close();
            
            // afterLast first
            rs = ps_c1.executeQuery();
            rs.afterLast();
            assertNoCurrentRow(rs);
            rs.previous();
            
            assertEquals("m",rs.getString(1).trim());
            assertEquals(13, rs.getInt(2));
            assertEquals(12, rs.getRow());
            
            rs.previous();
            assertEquals("l",rs.getString(1).trim());
            assertEquals(12, rs.getInt(2));
            assertEquals(11, rs.getRow());
            
            rs.close();
            
            // go to next to last row and then do next
            ps_c1.close();
            ps_c1 = conn.prepareStatement("select * from t1 where i >=11",
                    ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_READ_ONLY);
            rs = ps_c1.executeQuery();
            rs.next();
            assertEquals("k",rs.getString(1).trim());
            assertEquals(11, rs.getInt(2));
            assertEquals(1, rs.getRow());
            
            rs.next();
            assertEquals("l",rs.getString(1).trim());
            assertEquals(12, rs.getInt(2));
            assertEquals(2, rs.getRow());
            
            rs.last();
            assertEquals("m",rs.getString(1).trim());
            assertEquals(13, rs.getInt(2));
            assertEquals(3, rs.getRow());
            
            rs.previous();
            assertEquals("l",rs.getString(1).trim());
            assertEquals(12, rs.getInt(2));
            assertEquals(2, rs.getRow());
            
            rs.afterLast();
            assertNoCurrentRow(rs);
            
            rs.previous();
            assertEquals("m",rs.getString(1).trim());
            assertEquals(13, rs.getInt(2));
            assertEquals(3, rs.getRow());
            rs.close();
            // start at after ;ast/
            rs = ps_c1.executeQuery();
            rs.afterLast();
            assertNoCurrentRow(rs);
            
            rs.previous();
            assertEquals("m",rs.getString(1).trim());
            assertEquals(13, rs.getInt(2));
            assertEquals(3, rs.getRow());
            rs.close();
            ps_c1.close();
            // use absolute to get rows before scan would get to them.
           ps_c1 = conn.prepareStatement("select i from t1", ResultSet.TYPE_SCROLL_INSENSITIVE,
                   ResultSet.CONCUR_READ_ONLY);
           rs = ps_c1.executeQuery();
           rs.absolute(5);
           assertEquals(6, rs.getInt(1));
           assertEquals(5, rs.getRow());
           
           rs.absolute(-5);
           assertEquals(9,rs.getInt(1));
           assertEquals(8, rs.getRow());

           rs.absolute(5);
           assertEquals(6,rs.getInt(1));
           assertEquals(5, rs.getRow());
           rs.close();
           rs = ps_c1.executeQuery();
           rs.absolute(13);
           assertNoCurrentRow(rs);
           
           rs.previous();
           assertEquals(13, rs.getInt(1));
           assertEquals(12, rs.getRow());
           rs.close();
           rs = ps_c1.executeQuery();
           rs.absolute(-13);
           assertNoCurrentRow(rs);
           rs.next();
           assertEquals(2, rs.getInt(1));
           assertEquals(1,rs.getRow());
           rs.close();
           rs = ps_c1.executeQuery();
           rs.first();
           assertEquals(2,rs.getInt(1)); 
           assertEquals(1,rs.getRow());
           
           rs.relative(11);
           assertEquals(13, rs.getInt(1));
           assertEquals(12, rs.getRow());
           
           rs.relative(1);
           assertNoCurrentRow(rs);
           rs.last();
           assertEquals(13, rs.getInt(1));
           assertEquals(12,rs.getRow());
           
           rs.relative(-11);
           assertEquals(2,rs.getInt(1));
           assertEquals(1,rs.getRow());
           rs.close();
           ps_c1.close();
           conn.commit();
           // scroll sensitive cursor becomes scroll insensitive.
           ps_c1 = conn.prepareStatement("Select i from t1", ResultSet.TYPE_SCROLL_SENSITIVE,
                   ResultSet.CONCUR_READ_ONLY);    
           
           rs = ps_c1.executeQuery();
           rs.first();
           assertEquals(2,rs.getInt(1));
           rs.next();
           assertEquals(3,rs.getInt(1));
           Statement s = conn.createStatement();
           s.executeUpdate("update t1 set i = 666 where i = 2");
           rs.first();
           conn.rollback();
           rs.close();
           // verify that statement cache works correctly with scroll and forward only cursors.
           // with the same query text.
           ps_c1 = conn.prepareStatement("select i from t1", ResultSet.TYPE_SCROLL_INSENSITIVE,
                   ResultSet.CONCUR_READ_ONLY);
           rs = ps_c1.executeQuery();
           PreparedStatement ps_c2 = conn.prepareStatement("select i from t1");
           ResultSet rs2 = ps_c2.executeQuery();
           rs.first();
           assertEquals(2,rs.getInt(1));
           rs2.next();
           assertEquals(2,rs2.getInt(1));
           try {
               rs2.first();
               fail("first() not allowed on forward only result set");
           } catch(SQLException se) {
               if (usingEmbedded())
                   assertSQLState("XJ061",se);
               else
                   assertSQLState("XJ125",se);
           }
            rs.close();
            rs2.close();
            ps_c1.close();
            ps_c1.close();
            // first, last, etc on empty result set
            ps_c1 = conn.prepareStatement("select i from t1 where 1=0",
                    ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_READ_ONLY);
            rs = ps_c1.executeQuery();
            rs.first();
            assertNoCurrentRow(rs);
            rs.previous();
            assertNoCurrentRow(rs);
            rs.next();
            assertNoCurrentRow(rs);
            rs.last();
            assertNoCurrentRow(rs);
            rs.previous();
            assertNoCurrentRow(rs);
            rs.absolute(1);
            assertNoCurrentRow(rs);
            rs.absolute(-1);
            assertNoCurrentRow(rs);
            assertEquals(0,rs.getRow());
            rs.close();
            rs = ps_c1.executeQuery();
            rs.afterLast();
            assertNoCurrentRow(rs);
            rs.previous();
            assertNoCurrentRow(rs);
            rs.beforeFirst();
            assertNoCurrentRow(rs);
            rs.next();
            assertNoCurrentRow(rs);
            rs.close();
            rs = ps_c1.executeQuery();
            rs.absolute(1);
            assertNoCurrentRow(rs);
            rs.absolute(-1);
            assertNoCurrentRow(rs);
            rs.close();
            rs = ps_c1.executeQuery();
            rs.absolute(-1);
            assertNoCurrentRow(rs);
            rs.absolute(1);
            assertNoCurrentRow(rs);
            rs.close();
            // with autocommit on
            conn.setAutoCommit(false);
            rs = ps_c1.executeQuery();
            rs = ps_c1.executeQuery();
            rs.first();
            assertNoCurrentRow(rs);
            rs.previous();
            assertNoCurrentRow(rs);
            rs.next();
            assertNoCurrentRow(rs);
            rs.last();
            assertNoCurrentRow(rs);
            rs.previous();
            assertNoCurrentRow(rs);
            rs.absolute(1);
            assertNoCurrentRow(rs);
            rs.absolute(-1);
            assertNoCurrentRow(rs);
            rs.next();
            assertNoCurrentRow(rs);
            rs.next();
            assertNoCurrentRow(rs);
            rs.close();
            ps_c1.close();
            
            // cursor on a sort
            ps_c1 = conn.prepareStatement("select * from t1 order by i desc",ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_READ_ONLY);
            rs = ps_c1.executeQuery();
            rs.last();
            assertEquals("b",rs.getString(1).trim());
            assertEquals(2,rs.getInt(2));
            rs.first();
            assertEquals("m",rs.getString(1).trim());
            assertEquals(13,rs.getInt(2));
            rs.relative(11);
            assertEquals("b",rs.getString(1).trim());
            assertEquals(2,rs.getInt(2));
            rs.previous();
            assertEquals("c",rs.getString(1).trim());
            assertEquals(3,rs.getInt(2));
            rs.close();
            ps_c1.close();
            ps_c1 = conn.prepareStatement("select * from t1",
                    ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_READ_ONLY);
            CallableStatement cs = conn.prepareCall("call SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1)");
            cs.execute();
            cs.close();
            rs = ps_c1.executeQuery();
            rs.last();
            assertEquals("m",rs.getString(1).trim());
            assertEquals(13,rs.getInt(2));
            rs.first();
            assertEquals("b",rs.getString(1).trim());
            assertEquals(2,rs.getInt(2));
            rs.next();
            assertEquals("c",rs.getString(1).trim());
            assertEquals(3,rs.getInt(2));
            rs.close();
            rs2 = s.executeQuery("values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS()");
            rs2.next();
                    
            if (usingEmbedded())
            {
                // not sure why I get a null rts with network server.
                RuntimeStatisticsParser rts = new RuntimeStatisticsParser(rs2.getString(1));
                rs2.close();
                assertEquals(Connection.TRANSACTION_READ_COMMITTED, rts.getIsolationLevel());
                assertTrue(rts.usedTableScan());
                assertTrue(rts.isScrollInsensitive());
            }
         
            rs.close();  
            ps_c1.close();
        }

        public void testNoHoldScrollableResults() throws SQLException{
            Connection conn = getConnection();
            conn.setHoldability(ResultSet.CLOSE_CURSORS_AT_COMMIT);
            // Beetle 4551 - insensitive cursor uses estimated row count which 
            // might be pessimistic and will get out of memory error
            Statement s = createStatement();
            s.executeUpdate("create table big(a int generated always as identity (start with 1, increment by 1))");
            
            for (int i = 0; i < 10; i++)
                s.executeUpdate("insert into big values(default)");
            
         PreparedStatement ps_c1 = conn.prepareStatement("select * from big b1 left outer join  big b2 on b1.a = b2.a left outer join  big b3 on b2.a = b3.a left outer join big b4 on b3.a = b4.a left outer join (big b5 left outer join (big b6 left outer join (big b7 left outer join big b8 on b7.a = b8.a) on b6.a=b7.a) on b5.a = b6.a) on b4.a = b5.a");
         ResultSet rs = ps_c1.executeQuery();
         s.executeUpdate("drop table big");
        }
        
        public void testSimpleScrollCursors() throws SQLException {
            Connection conn = getConnection();
            Statement s = conn.createStatement();
            s.executeUpdate("create table t (a int)");
            PreparedStatement ps = conn.prepareStatement("insert into t values (?)");
            for (int i = 1; i <=5; i++)
            {
                ps.setInt(1, i);
                ps.executeUpdate();
            }
            ps.close();
            PreparedStatement ps_c1 = conn.prepareStatement("select * from t", ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_READ_ONLY);
            ResultSet rs = ps_c1.executeQuery();
            rs.first();
            assertEquals(1,rs.getInt(1));
            rs.next();
            assertEquals(2,rs.getInt(1));
            rs.previous();
            assertEquals(1,rs.getInt(1));
            rs.last();
            assertEquals(5,rs.getInt(1));
            rs.absolute(2);
            assertEquals(2, rs.getInt(1));
            rs.relative(2);
            assertEquals(4,rs.getInt(1));
            rs.close();
            // since JCC gets 64 results and then scrolls within them
            // lets try each p ositioning command as the first command for the cursor.
            rs = ps_c1.executeQuery();
            rs.next();
            assertEquals(1,rs.getInt(1));
            rs.close();
            rs = ps_c1.executeQuery();
            rs.last();
            assertEquals(5,rs.getInt(1));
            rs.close();
            rs = ps_c1.executeQuery();
            rs.absolute(3);
            assertEquals(3,rs.getInt(1));
            rs.next();
            assertEquals(4,rs.getInt(1));
            rs.close();
            ps_c1.close();
            // lets try a table with more than 64 rows.
            s.executeUpdate("create table tab1 (a int)");
            PreparedStatement is = conn.prepareStatement("insert into tab1 values (?)");
            for (int i = 1; i <= 70; i++) {
                is.setInt(1, i);
                is.executeUpdate();
            }
            ps_c1 = conn.prepareStatement("select * from tab1",ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_READ_ONLY);
            rs = ps_c1.executeQuery();
            rs.first();
            assertEquals(1,rs.getInt(1));
            rs.last();
            assertEquals(70,rs.getInt(1));
            rs.absolute(65);
            assertEquals(65,rs.getInt(1));
            rs.absolute(-1);
            assertEquals(70,rs.getInt(1));
            rs.close();
            ps_c1.close();
            // try sensitive scroll cursors bug 4677
            ps_c1 = conn.prepareStatement("select * from t1",ResultSet.TYPE_SCROLL_SENSITIVE,
                        ResultSet.CONCUR_READ_ONLY);
            rs = ps_c1.executeQuery();
            rs.close();
            ps_c1.close();
            ps_c1 = conn.prepareStatement("select * from t1 for update",ResultSet.TYPE_SCROLL_SENSITIVE,
                    ResultSet.CONCUR_UPDATABLE);
            rs.close();
            s.executeUpdate("drop table tab1");
            // defect 5225, outer joins returning NULLS
            s.executeUpdate("create table tab1(i1 bigint not null, c1 varchar(64) not null)");
            s.executeUpdate("create table tab2 (i2 bigint not null, c2 varchar(64) not null)");
            s.executeUpdate("insert into tab1 values (1, 'String 1')");
            s.executeUpdate("insert into tab1 values (2, 'String 2')");
            s.executeUpdate("insert into tab2 values (1, 'String 1')");
            s.executeUpdate("insert into tab2 values (3, 'String 3')");
            rs = s.executeQuery("select c1 from tab1 right outer join tab2 on (i1=i2)");
            JDBC.assertFullResultSet(rs, new String[][] {{"String 1"},{null}});
            rs = s.executeQuery("select c2 from tab1 right outer join tab2 on (i1=i2)");
            JDBC.assertFullResultSet(rs, new String[][] {{"String 1"},{"String 3"}});
            // left outer join
            rs = s.executeQuery("select c1 from tab1 left outer join tab2 on (i1=i2)");
            JDBC.assertFullResultSet(rs, new String[][] {{"String 1"},{"String 2"}});
            rs = s.executeQuery("select c2 from tab1 left outer join tab2 on (i1=i2)");
            JDBC.assertFullResultSet(rs, new String[][] {{"String 1"},{null}});
            s.executeUpdate("drop table t");
            s.executeUpdate("drop table tab1");
            s.executeUpdate("drop table tab2");            
        }
        
        public void testScrollCursors3() throws SQLException {
            Connection conn = getConnection();
            Connection conn2 = openDefaultConnection();
            Statement s = conn.createStatement();
            s.executeUpdate("create table u1.t1(c1 int, c2 int)");
            s.executeUpdate("insert into u1.t1 values (1, 2), (3, 4), (5, 6), (7, 8), (9, 10)");
            PreparedStatement ps_c1 = conn.prepareStatement("select * from u1.t1", ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_READ_ONLY);
            ResultSet rs = ps_c1.executeQuery();
            
            // see what happens when other user when we close our cursor before
            // they are done.
            PreparedStatement ps_c2 = conn2.prepareStatement("select * from u1.t1", ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_READ_ONLY);
            ResultSet rs2 = ps_c2.executeQuery();
            rs.next();
            assertEquals(1, rs.getInt(1));
            assertEquals(2, rs.getInt(2));
            
            rs2.next();
            assertEquals(1, rs.getInt(1));
            assertEquals(2, rs.getInt(2));
            rs.last();
            assertEquals(9, rs.getInt(1));
            assertEquals(10, rs.getInt(2));
            rs2.last();
            assertEquals(9, rs.getInt(1));
            assertEquals(10, rs.getInt(2));
            rs.previous();
            assertEquals(7, rs.getInt(1));
            assertEquals(8, rs.getInt(2));
            rs2.close();
            rs.first();
            assertEquals(1, rs.getInt(1));
            assertEquals(2, rs.getInt(2));
            rs.close();
            s.executeUpdate("drop table u1.t1");
        }
        
        
        private void assertNoCurrentRow(ResultSet rs) throws SQLException {
            try {
                rs.getString(1);
                fail("getString not allowed after beforeFirst()");
            }catch (SQLException se ){
                if (usingEmbedded())
                    assertSQLState("24000",se);
                else
                    assertSQLState("XJ121",se);                
            }
            assertEquals(0,rs.getRow());
        }

        
        public static Test baseSuite(String name) {

        BaseTestSuite suite = new BaseTestSuite(name);
        suite.addTestSuite(ScrollCursors1Test.class);

        return new CleanDatabaseTestSetup(suite) {

            /**
             * Create and populate table
             * 
             * @see org.apache.derbyTesting.junit.CleanDatabaseTestSetup#decorateSQL(java.sql.Statement)
             */
            protected void decorateSQL(Statement s) throws SQLException {
                s.executeUpdate("create table t1(c50 char(50), i int)");

                s.executeUpdate(" create table t2(c50 char(50), i int)");

                // populate tables

                s.executeUpdate("insert into t1 values ('b', 2), ('c', 3), ('d', 4), "
                                + "('e', 5),"
                                + "                   ('f', 6), ('g', 7), ('h', 8), ('i', 9),"
                                + "                   ('j', 10), ('k', 11), ('l', 12), ('m', 13)");

            }
        };
    }

    public static Test suite() {
        BaseTestSuite suite = new BaseTestSuite("ScrollCursors1");
        suite.addTest(baseSuite("ScrollCursors1:embedded"));
        suite.addTest(TestConfiguration.clientServerDecorator(baseSuite("ScrollCursors1:client")));
        return suite;

    }
}
