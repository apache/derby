/*
 *
 * Derby - Class SURTest
 *
 * Copyright 2006 The Apache Software Foundation or its
 * licensors, as applicable.
 *
 * Licensed under the Apache License, Version 2.0 (the "License")
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 */
package org.apache.derbyTesting.functionTests.tests.jdbcapi;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import junit.extensions.TestSetup;
import junit.framework.Test;
import junit.framework.TestSuite;
import java.util.Iterator;
/**
 * Tests for variants of scrollable updatable resultsets.
 *
 * @author Andreas Korneliussen
 */
public class SURTest extends SURBaseTest {
    
    /** Creates a new instance of SURTest */
    public SURTest(String name) {
        super(name);
    }
    
    /**
     * Test that you get a warning when specifying a query which is not
     * updatable and concurrency mode CONCUR_UPDATABLE.
     * In this case, the query contains an "order by"
     */
    public void testConcurrencyModeWarning1()
        throws SQLException 
    {
        Statement s = con.createStatement(ResultSet.TYPE_FORWARD_ONLY,
                                          ResultSet.CONCUR_UPDATABLE);
        s.setCursorName(getNextCursorName());
        ResultSet rs = s.executeQuery("select * from t1 order by a");
        
        SQLWarning warn = rs.getWarnings();
        assertEquals("Expected resultset to be read only",
                     ResultSet.CONCUR_READ_ONLY,
                     rs.getConcurrency());
        assertNotNull("Expected to get a warning", warn);
        scrollForward(rs);
        rs.close();
    }
    
    /**
     * Test that you get a warning when specifying a query which is not
     * updatable and concurrency mode CONCUR_UPDATABLE.
     * In this case, the query contains a join.
     */
    public void testConcurrencyModeWarning2()
        throws SQLException 
    {
        Statement s = con.createStatement(ResultSet.TYPE_FORWARD_ONLY,
                                          ResultSet.CONCUR_UPDATABLE);
        s.setCursorName(getNextCursorName());
        ResultSet rs = s.executeQuery
            ("select * from t1 as table1,t1 as table2 where " +
             "table1.a=table2.a");
        
        SQLWarning warn = rs.getWarnings();
        assertEquals("Expected resultset to be read only",
                     ResultSet.CONCUR_READ_ONLY,
                     rs.getConcurrency());
        assertNotNull("Expected to get a warning", warn);
        scrollForward(rs);
        rs.close();
    }
    
    /**
     * Test that you get an exception when specifying update clause
     * "FOR UPDATE"
     * along with a query which is not updatable.
     * In this case, the query contains and order by.
     */
    public void testForUpdateException1()
        throws SQLException 
    {
        Statement s = con.createStatement(ResultSet.TYPE_FORWARD_ONLY,
                                          ResultSet.CONCUR_UPDATABLE);
        try {
            String queryString =
                "select * from t1 order by a for update";
            s.setCursorName(getNextCursorName());
            ResultSet rs = s.executeQuery(queryString);
            
            assertTrue("Expected query '" + queryString +
                       "' to fail", false);
        } catch (SQLException e) {
            assertEquals("Unexpected SQLState", 
                         FOR_UPDATE_NOT_PERMITTED_SQL_STATE,
                         e.getSQLState());
        }
        con.rollback();
    }
    
    /**
     * Test that you get an exception when specifying update clause
     * "FOR UPDATE" along with a query which is not updatable.
     * In this case, the query contains a join
     */
    public void testForUpdateException2()
        throws SQLException 
    {
        Statement s = con.createStatement(ResultSet.TYPE_FORWARD_ONLY,
                                          ResultSet.CONCUR_UPDATABLE);
        try {
            String queryString =
                "select * from t1 as table1,t1 as table2" +
                " where table1.a=table2.a for update";
            s.setCursorName(getNextCursorName());
            ResultSet rs = s.executeQuery(queryString);
            
            assertTrue("Expected query '" + queryString + "' to fail",
                       false);
        } catch (SQLException e) {
            assertEquals("Unexpected SQLState", 
                         FOR_UPDATE_NOT_PERMITTED_SQL_STATE,
                         e.getSQLState());
        }
        con.rollback();
    }
    
    /**
     * Test that you can scroll forward and read all records in the
     * ResultSet
     */
    public void testForwardOnlyReadOnly1()
        throws SQLException 
    {
        Statement s = con.createStatement(ResultSet.TYPE_FORWARD_ONLY,
                                          ResultSet.CONCUR_READ_ONLY);
        s.setCursorName(getNextCursorName());
        ResultSet rs = s.executeQuery("select * from t1");
        
        scrollForward(rs);
        rs.close();
    }
    
    
    /**
     * Test that you get an exception if you try to update a ResultSet
     * with concurrency mode CONCUR_READ_ONLY.
     */
    public void testFailOnUpdateOfReadOnlyResultSet1()
        throws SQLException 
    {
        Statement s = con.createStatement(ResultSet.TYPE_FORWARD_ONLY,
                                          ResultSet.CONCUR_READ_ONLY);
        s.setCursorName(getNextCursorName());
        ResultSet rs = s.executeQuery("select * from t1");
        
        rs.next();
        assertFailOnUpdate(rs);
    }
    
    /**
     * Test that you get an exception when attempting to update a
     * ResultSet which has been downgraded to a read only ResultSet.
     */
    public void testFailOnUpdateOfReadOnlyResultSet2()
        throws SQLException 
    {
        Statement s = con.createStatement(ResultSet.TYPE_FORWARD_ONLY,
                                          ResultSet.CONCUR_UPDATABLE);
        s.setCursorName(getNextCursorName());
        ResultSet rs = s.executeQuery("select * from t1 order by id");
        
        rs.next();
        assertFailOnUpdate(rs);
    }
    
    /**
     * Test that you get an exception when attempting to update a
     * ResultSet which has been downgraded to a read only ResultSet.
     */
    public void testFailOnUpdateOfReadOnlyResultSet3()
        throws SQLException 
    {
        Statement s = con.createStatement(ResultSet.TYPE_FORWARD_ONLY,
                                          ResultSet.CONCUR_UPDATABLE);
        s.setCursorName(getNextCursorName());
        ResultSet rs =
            s.executeQuery("select * from t1 for read only");
        
        rs.next();
        assertFailOnUpdate(rs);
    }
    
    /**
     * Test that you get an exception when attempting to update a
     * ResultSet which has been downgraded to a read only ResultSet.
     */
    public void testFailOnUpdateOfReadOnlyResultSet4()
        throws SQLException 
    {
        Statement s = con.createStatement(ResultSet.TYPE_FORWARD_ONLY,
                                          ResultSet.CONCUR_UPDATABLE);
        s.setCursorName(getNextCursorName());
        ResultSet rs = s.executeQuery
            ("select * from t1 where a=1 for read only");
        
        rs.next();
        verifyTuple(rs);
        assertFailOnUpdate(rs);
    }
    
    
    /**
     * Test that you get an exception if you try to update a ResultSet
     * with concurrency mode CONCUR_READ_ONLY.
     */
    public void testFailOnUpdateOfReadOnlyResultSet5()
        throws SQLException 
    {
        Statement s = con.createStatement(ResultSet.
                                          TYPE_SCROLL_INSENSITIVE,
                                          ResultSet.CONCUR_READ_ONLY);
        s.setCursorName(getNextCursorName());
        ResultSet rs = s.executeQuery
            ("select * from t1 where a=1 for read only");
        
        rs.next();
        verifyTuple(rs);
        assertFailOnUpdate(rs);
    }
    
    /**
     * Test that you can scroll forward and update indexed records in
     * the ResultSet (not using FOR UPDATE)
     */
    public void testIndexedUpdateCursor1()
        throws SQLException 
    {
        Statement s = con.createStatement(ResultSet.TYPE_FORWARD_ONLY,
                                          ResultSet.CONCUR_UPDATABLE);
        s.setCursorName(getNextCursorName());
        ResultSet rs = s.executeQuery("select * from t1 where a=1");
        
        assertTrue("Expected to get a tuple on rs.next()", rs.next());
        verifyTuple(rs);
        updateTuple(rs);
        
    }
    
    /**
     *  Test that you can scroll forward and update indexed records
     *  in the ResultSet (using FOR UPDATE).
     */
    public void testIndexedUpdateCursor2()
        throws SQLException 
    {
        Statement s = con.createStatement(ResultSet.TYPE_FORWARD_ONLY,
                                          ResultSet.CONCUR_UPDATABLE);
        s.setCursorName(getNextCursorName());
        ResultSet rs =
            s.executeQuery("select * from t1 where a=1 for update");
        
        assertTrue("Expected to get a tuple on rs.next()", rs.next());
        verifyTuple(rs);
        updateTuple(rs);
    }
    
    /**
     *  Test that you can scroll forward and update indexed records
     *  in the scrollable ResultSet (not using FOR UPDATE).
     */
    public void
        testIndexedScrollInsensitiveUpdateCursorWithoutForUpdate1()
        throws SQLException 
    {
        Statement s = con.createStatement
            (ResultSet.TYPE_SCROLL_INSENSITIVE,
             ResultSet.CONCUR_UPDATABLE);
        s.setCursorName(getNextCursorName());
        ResultSet rs =
            s.executeQuery("select * from t1 where a=1 or a=2");
        
        rs.next();
        rs.next();
        rs.previous();
        verifyTuple(rs);
        updateTuple(rs);
    }
    
    /**
     *  Test that you can scroll forward and update indexed records
     *  in the scrollable ResultSet (using FOR UPDATE).
     */
    public void
        testIndexedScrollInsensitiveUpdateCursorWithForUpdate1()
        throws SQLException 
    {
        Statement s = con.createStatement
            (ResultSet.TYPE_SCROLL_INSENSITIVE,
             ResultSet.CONCUR_UPDATABLE);
        s.setCursorName(getNextCursorName());
        ResultSet rs = s.executeQuery
            ("select * from t1 where a=1 or a=2 for update");
        
        rs.next();
        rs.next();
        rs.previous();
        verifyTuple(rs);
        updateTuple(rs);
        rs.close();
        s.close();
    }
   
    /**
     * Test update of a keyed record using scrollable updatable
     * resultset.
     */
    public void testPrimaryKeyUpdate1()
        throws SQLException 
    {
        Statement s = con.createStatement
            (ResultSet.TYPE_SCROLL_INSENSITIVE,
             ResultSet.CONCUR_UPDATABLE);
        s.setCursorName(getNextCursorName());
        ResultSet rs = s.executeQuery("select * from t1");
        
        rs.last();
        rs.next();
        while(rs.previous()) {
            // Update the key of every second row.
            int key = rs.getInt(1);
            if (key%2==0) {
                int newKey = -key;
                rs.updateInt(1, newKey);
                rs.updateRow();
            }
        }
        PreparedStatement ps = con.prepareStatement
            ("select * from t1 where id=?");
        for (int i=0; i<recordCount; i++) {
            int key = (i%2==0) ? -i : i;
            ps.setInt(1, key);
            ResultSet rs2 = ps.executeQuery();
            assertTrue("Expected query to have 1 row", rs2.next());
            println("T1: Read Tuple:(" + rs2.getInt(1) + "," +
                    rs2.getInt(2) + "," +
                    rs2.getInt(3) + ")");
            assertEquals("Unexpected value of id", key, rs2.getInt(1));
            assertTrue("Did not expect more than 1 row, " +
                       "however rs2.next returned another row",
                       !rs2.next());
        }
    }
        
    /**
     * Test update of a keyed record using other statement
     * object.
     */
    public void testOtherPrimaryKeyUpdate1()
        throws SQLException 
    {
        Statement s = con.createStatement
            (ResultSet.TYPE_SCROLL_INSENSITIVE,
             ResultSet.CONCUR_UPDATABLE);
        s.setCursorName(getNextCursorName());
        ResultSet rs = s.executeQuery("select * from t1");
        
        rs.last();
        int primaryKey = rs.getInt(1);
        PreparedStatement ps = con.prepareStatement
            ("update t1 set id = ? where id= ?");
        ps.setInt(1, -primaryKey);
        ps.setInt(2, primaryKey);
        assertEquals("Expected one row to be updated", 1,
                     ps.executeUpdate());
        
        rs.updateInt(2, -555);
        rs.updateInt(3, -777);
        rs.updateRow();
        
        PreparedStatement ps2 = con.prepareStatement
            ("select * from t1 where id=?");
        ps2.setInt(1, -primaryKey);
        ResultSet rs2 = ps2.executeQuery();
        assertTrue("Expected query to have 1 row", rs2.next());
        println("T1: Read Tuple:(" + rs2.getInt(1) + "," +
                rs2.getInt(2) + "," +
                rs2.getInt(3) + ")");
        assertEquals("Expected a=-555", -555, rs2.getInt(2));
        assertEquals("Expected b=-777", -777, rs2.getInt(3));
        assertTrue("Did not expect more than 1 row, however " +
                   "rs2.next() returned another row", !rs2.next());
    }
    
    /**
     * Test update of a keyed record using other both the
     * scrollable updatable resultset and using another statement
     * object.
     */
    public void testOtherAndOwnPrimaryKeyUpdate1()
        throws SQLException 
    {
        Statement s = con.createStatement
            (ResultSet.TYPE_SCROLL_INSENSITIVE,
             ResultSet.CONCUR_UPDATABLE);
        s.setCursorName(getNextCursorName());
        ResultSet rs = s.executeQuery("select * from t1");
        
        rs.last();
        int primaryKey = rs.getInt(1);
        PreparedStatement ps = con.prepareStatement
            ("update t1 set id = ? where id= ?");
        ps.setInt(1, -primaryKey);
        ps.setInt(2, primaryKey);
        assertEquals("Expected one row to be updated", 1,
                     ps.executeUpdate());
        rs.updateInt(1, primaryKey*10);
        rs.updateInt(2, -555);
        rs.updateInt(3, -777);
        rs.updateRow();
        
        PreparedStatement ps2 =
            con.prepareStatement("select * from t1 where id=?");
        ps2.setInt(1, primaryKey*10);
        ResultSet rs2 = ps2.executeQuery();
        assertTrue("Expected query to have 1 row", rs2.next());
        println("T1: Read Tuple:(" + rs2.getInt(1) + "," +
                rs2.getInt(2) + "," +
                rs2.getInt(3) + ")");
        assertEquals("Expected a=-555", -555, rs2.getInt(2));
        assertEquals("Expected b=-777", -777, rs2.getInt(3));
        assertTrue("Did not expect more than 1 row, however " +
                   "rs2.next() returned another row", !rs2.next());
    }
    
    /**
     * Update multiple keyed records using scrollable updatable resultset
     */
    public void testMultipleKeyUpdates()
        throws SQLException 
    {
        Statement s = con.createStatement
            (ResultSet.TYPE_SCROLL_INSENSITIVE,
             ResultSet.CONCUR_UPDATABLE);
        s.setCursorName(getNextCursorName());
        ResultSet rs = s.executeQuery("select * from t1");
        
        rs.last();
        int primaryKey = rs.getInt(1);
        PreparedStatement ps = con.prepareStatement
            ("update t1 set id = ? where id= ?");
        ps.setInt(1, -primaryKey);
        ps.setInt(2, primaryKey);
        assertEquals("Expected one row to be updated", 1,
                     ps.executeUpdate());
        rs.updateInt(1, primaryKey*10);
        rs.updateInt(2, -555);
        rs.updateInt(3, -777);
        rs.updateRow();
        rs.first();
        rs.last();
        for (int i=0; i<10; i++) {
            rs.first();
            rs.last();
            rs.next();
            rs.previous();
            rs.updateInt(1, primaryKey*10 +i);
            rs.updateInt(2, (-555 -i));
            rs.updateInt(3, (-777 -i));
            rs.updateRow();
        }
    }
    
    /**
     * Test update indexed records using scrollable updatable resultset 
     */
    public void testSecondaryIndexKeyUpdate1()
        throws SQLException 
    {
        
        Statement s = con.createStatement
            (ResultSet.TYPE_SCROLL_INSENSITIVE,
             ResultSet.CONCUR_UPDATABLE);
        s.setCursorName(getNextCursorName());
        ResultSet rs = s.executeQuery("select * from t1");
        
        rs.last();
        rs.next();
        int newKey = 0;
        while(rs.previous()) {
            // Update the secondary key of all rows
            rs.updateInt(2, newKey--);
            rs.updateRow();
        }
        PreparedStatement ps = con.prepareStatement
            ("select * from t1 where a=?");
        for (int i=0; i<recordCount; i++) {
            int key = -i;
            ps.setInt(1, key);
            ResultSet rs2 = ps.executeQuery();
            assertTrue("Expected query to have 1 row", rs2.next());
            println("T1: Read Tuple:(" + rs2.getInt(1) + "," +
                    rs2.getInt(2) + "," +
                    rs2.getInt(3) + ")");
            assertEquals("Unexpected value of id", key, rs2.getInt(2));
            assertTrue("Did not expect more than 1 row, " +
                       "however rs2.next returned another row",
                       !rs2.next());
        }
    }
    
    /**
     * Test update indexed records using other statement object
     * and using resultset.
     */
    public void testOtherSecondaryKeyUpdate1()
        throws SQLException 
    {
        Statement s = con.createStatement
            (ResultSet.TYPE_SCROLL_INSENSITIVE,
             ResultSet.CONCUR_UPDATABLE);
        s.setCursorName(getNextCursorName());
        ResultSet rs = s.executeQuery("select * from t1");
        
        rs.last();
        int indexedKey = rs.getInt(2);
        PreparedStatement ps =
            con.prepareStatement("update t1 set a = ? where a= ?");
        ps.setInt(1, -indexedKey);
        ps.setInt(2, indexedKey);
        assertEquals("Expected one row to be updated", 1,
                     ps.executeUpdate());
        
        rs.updateInt(1, -555);
        rs.updateInt(3, -777);
        rs.updateRow();
        
        PreparedStatement ps2 =
            con.prepareStatement("select * from t1 where a=?");
        ps2.setInt(1, -indexedKey);
        ResultSet rs2 = ps2.executeQuery();
        assertTrue("Expected query to have 1 row", rs2.next());
        println("T1: Read Tuple:(" + rs2.getInt(1) + "," +
                rs2.getInt(2) + "," +
                rs2.getInt(3) + ")");
        assertEquals("Expected id=-555", -555, rs2.getInt(1));
        assertEquals("Expected b=-777", -777, rs2.getInt(3));
        assertTrue("Did not expect more than 1 row, however " +
                   "rs2.next() returned another row", !rs2.next());
    }
    
    /**
     * Test scrolling in a read only resultset
     */
    public void testScrollInsensitiveReadOnly1()
        throws SQLException 
    {
        Statement s = con.createStatement
            (ResultSet.TYPE_SCROLL_INSENSITIVE,
             ResultSet.CONCUR_READ_ONLY);
        s.setCursorName(getNextCursorName());
        ResultSet rs = s.executeQuery("select * from t1");
        
        scrollForward(rs);
        scrollBackward(rs);
        rs.close();
    }
    
    /**
     * Test updating a forward only resultset (with FOR UPDATE)
     */
    public void testForwardOnlyConcurUpdatableWithForUpdate1()
        throws SQLException 
    {
        Statement s = con.createStatement
            (ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
        s.setCursorName(getNextCursorName());
        ResultSet rs = s.executeQuery("select * from t1 for update");
        
        scrollForwardAndUpdate(rs);
        rs.close();
    }
    
    /**
     * Test updating a forward only resultset (without FOR UPDATE)
     */
    public void testForwardOnlyConcurUpdatableWithoutForUpdate1()
        throws SQLException 
    {
        Statement s = con.createStatement
            (ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
        s.setCursorName(getNextCursorName());
        ResultSet rs = s.executeQuery("select * from t1");
        
        scrollForwardAndUpdate(rs);
        rs.close();
    }
    
    /**
     * Test updating a forward only resultset (without FOR UPDATE)
     * and using positioned update
     */
    public void testPositionedUpdateWithoutForUpdate1()
        throws SQLException 
    {
        Statement s = con.createStatement
            (ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
        s.setCursorName("MYCURSOR");
        ResultSet rs = s.executeQuery("select * from t1");
        
        scrollForwardAndUpdatePositioned(rs);
        rs.close();
    }
    
    /**
     * Test updating a forward only resultset (with FOR UPDATE)
     * and using positioned update
     */
    public void testPositionedUpdateWithForUpdate1()
        throws SQLException 
    {
        Statement s = con.createStatement();
        s.setCursorName(getNextCursorName());
        ResultSet rs = s.executeQuery("select * from t1 for update");
        
        scrollForwardAndUpdatePositioned(rs);
        rs.close();
    }
    
    /**
     * Test positioned update of a scrollable resultset (with FOR UPDATE) 
     */
    public void testScrollablePositionedUpdateWithForUpdate1()
        throws SQLException 
    {
        Statement s = con.createStatement
            (ResultSet.TYPE_SCROLL_INSENSITIVE,
             ResultSet.CONCUR_READ_ONLY);
        s.setCursorName("MYCURSOR");
        ResultSet rs = s.executeQuery("select * from t1 for update");
        
        rs.next();
        int pKey = rs.getInt(1);
        rs.previous();
        rs.next();
        assertEquals("Expecting to be on the same row after previous() " + 
                     "+ next() ", pKey, rs.getInt(1));
        rs.next();
        rs.previous();
        assertEquals("Expecting to be on the same row after next() + " + 
                     "previous()", pKey, rs.getInt(1));
        final int previousA = rs.getInt(2);
        final int previousB = rs.getInt(3);
        println(rs.getCursorName());
        PreparedStatement ps = con.prepareStatement
            ("update T1 set a=?,b=? where current of " + rs.getCursorName());
        ps.setInt(1, 666);
        ps.setInt(2, 777);
        ps.executeUpdate();
        rs.next();
        rs.previous();
        assertEquals("Expected to be on the same row after next() + previous()",
                     pKey, rs.getInt(1));
        assertEquals("Expected row to be updated by own change, " + 
                     " however did not get updated value for column a", 
                     666, rs.getInt(2));
        assertEquals("Expected row to be updated by own change, however did " +
                     "not get updated value for column b", 777, rs.getInt(3));
        rs.close();
        s.setCursorName(getNextCursorName());
        rs = s.executeQuery("select * from t1 order by b");
        
        while (rs.next()) {
            if (rs.getInt(1)==pKey) {
                assertEquals("Expected row with primary key = " + pKey + 
                             " to be updated", 666, rs.getInt(2));
                assertEquals("Expected row with primary key = " + pKey + 
                             " to be updated", 777, rs.getInt(3));
            } else {
                println("Got tuple (" + rs.getInt(1) + "," + rs.getInt(2) + 
                        "," + rs.getInt(3) + "," + rs.getString(4)+ ")");
            }
        }
        
    }
    
    /**
     * Test update of a scrollable resultset (with FOR UPDATE)
     * Only scrolling forward
     */
    public void testScrollInsensitiveConcurUpdatableWithForUpdate1()
        throws SQLException 
    {
        Statement s = con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, 
                                          ResultSet.CONCUR_UPDATABLE);
        s.setCursorName(getNextCursorName());
        ResultSet rs = s.executeQuery("select * from t1 for update");
        scrollForwardAndUpdate(rs);
        rs.close();
    }
    
    /**
     * Test update of a scrollable resultset (with FOR UPDATE) 
     * Scrolling forward and backward.
     */
    public void testScrollInsensitiveConcurUpdatableWithForUpdate2()
        throws SQLException 
    {
        Statement s = con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, 
                                          ResultSet.CONCUR_UPDATABLE);
        assertEquals("Invalid resultset concurrency on statement", 
                     ResultSet.CONCUR_UPDATABLE, s.getResultSetConcurrency());
        s.setCursorName(getNextCursorName());
        ResultSet rs = s.executeQuery("select * from t1 for update");
        
        assertEquals("Invalid resultset concurrency on resultset", 
                     ResultSet.CONCUR_UPDATABLE, rs.getConcurrency());
        scrollForward(rs);
        scrollBackwardAndUpdate(rs);
        rs.close();
    }
    
    /**
     * Test update of a scrollable resultset
     * Scrolling forward and backward. Then open another
     * resultset and verify the data.
     */
    private void testScrollInsensistiveConurUpdatable3(ResultSet rs) 
        throws SQLException 
    {
        while (rs.next()) {
        }
        while (rs.previous()) {
            int a = rs.getInt(1);
            int b = rs.getInt(2);
            int id = b - 17 - a;
            int newA = 1000;
            int newB = id + newA + 17;
            rs.updateInt(1, newA); // Set a to 1000
            rs.updateInt(2, newB); // Set b to checksum value
            rs.updateRow();
            
            assertEquals("Expected a to be 1000", 1000, rs.getInt(1));
        }
        int count = 0;
        while (rs.next()) {
            int a = rs.getInt(1);
            count++;
            assertEquals("Incorrect row updated for row " + count, 1000, a);
        }
        assertEquals("Expected count to be the same as number of records", 
                     recordCount, count);
        while (rs.previous()) {
            int a = rs.getInt(1);
            count--;
            assertEquals("Incorrect row updated for row " + count, 1000, a);
        }
        rs.close();
        Statement s = con.createStatement(ResultSet.TYPE_FORWARD_ONLY, 
                                          ResultSet.CONCUR_READ_ONLY);
        s.setCursorName(getNextCursorName());
        rs = s.executeQuery("select * from t1");
        
        while (rs.next()) {
            int id = rs.getInt(1);
            int a = rs.getInt(2);
            int b = rs.getInt(3);
            println("Updated tuple:" + id + "," + a + "," + b);
        }
    }
    
    /**
     * Test update of a scrollable resultset (with FOR UPDATE)
     * Scrolling forward and backward. Then open another
     * resultset and verify the data.
     */
    public void testScrollInsensitiveConcurUpdatableWithForUpdate3()
        throws SQLException 
    {
        Statement s = con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, 
                                          ResultSet.CONCUR_UPDATABLE);
        s.setCursorName(getNextCursorName());
        ResultSet rs = s.executeQuery("select a,b from t1 for update");
        
        testScrollInsensistiveConurUpdatable3(rs);
    }
    
    /**
     * Test update of a scrollable resultset (without FOR UPDATE) 
     * Scrolling forward only
     */
    public void testScrollInsensitiveConcurUpdatableWithoutForUpdate1()
        throws SQLException 
    {
        Statement s = con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, 
                                          ResultSet.CONCUR_UPDATABLE);
        s.setCursorName(getNextCursorName());
        ResultSet rs = s.executeQuery("select * from t1");
        
        scrollForwardAndUpdate(rs);
        rs.close();
    }
    
    /**
     * Test update of a scrollable resultset (without FOR UPDATE) 
     * Scrolling forward and backward.
     */
    public void testScrollInsensitiveConcurUpdatableWithoutForUpdate2()
        throws SQLException 
    {
        Statement s = con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, 
                                          ResultSet.CONCUR_UPDATABLE);
        s.setCursorName(getNextCursorName());
        ResultSet rs = s.executeQuery("select * from t1");
        
        scrollForward(rs);
        scrollBackwardAndUpdate(rs);
        rs.close();
    }
    
    /**
     * Test update of a scrollable resultset (without FOR UPDATE)
     * Scrolling forward and backward. Then open another
     * resultset and verify the data.
     */
    public void testScrollInsensitiveConcurUpdatableWithoutForUpdate3()
        throws SQLException 
    {
        Statement s = con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, 
                                          ResultSet.CONCUR_UPDATABLE);
        s.setCursorName(getNextCursorName());
        ResultSet rs = s.executeQuery("select a,b from t1");
        
        testScrollInsensistiveConurUpdatable3(rs);
    }
    
    /**
     * Get a new cursor name. WORKAROUND FOR DERBY-787, since
     * the cursors have side-effects..!
     * To reproduce DERBY-787, just return the same cursor name.
     */
    private final String getNextCursorName() {
        return "MYCURSOR" + cursorIndex++;
    }
    
    private static int cursorIndex = 0;

    
    /**
     * The suite contains all testcases in this class running on different 
     * data models
     */
    public static Test suite() {
        
        TestSuite mainSuite = new TestSuite();
        
        // Iterate over all data models and decorate the tests:
        for (Iterator i = SURDataModelSetup.SURDataModel.values().iterator();
             i.hasNext();) {
            
            SURDataModelSetup.SURDataModel model = 
                (SURDataModelSetup.SURDataModel) i.next();
            
            TestSuite suite = new TestSuite(SURTest.class);
            TestSetup decorator = new SURDataModelSetup
                (suite, model);
            
            mainSuite.addTest(decorator);    
        }
        
        return mainSuite;
    }
}
