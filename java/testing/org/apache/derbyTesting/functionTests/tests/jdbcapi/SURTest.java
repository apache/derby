/*
 *
 * Derby - Class SURTest
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
package org.apache.derbyTesting.functionTests.tests.jdbcapi;
import org.apache.derbyTesting.functionTests.util.SQLStateConstants;
import org.apache.derbyTesting.junit.TestConfiguration;

import java.sql.Connection;
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
        Statement s = createStatement(ResultSet.TYPE_FORWARD_ONLY,
                                          ResultSet.CONCUR_UPDATABLE);
        s.setCursorName(getNextCursorName());
        ResultSet rs = s.executeQuery("select * from t1 order by a");
        
        SQLWarning warn = rs.getWarnings();
        assertEquals("Expected resultset to be read only",
                     ResultSet.CONCUR_READ_ONLY,
                     rs.getConcurrency());
        assertWarning(warn, QUERY_NOT_QUALIFIED_FOR_UPDATABLE_RESULTSET);
        scrollForward(rs);
        rs.close();
        s.close();
    }
    
    /**
     * Test that you get a warning when specifying a query which is not
     * updatable and concurrency mode CONCUR_UPDATABLE.
     * In this case, the query contains a join.
     */
    public void testConcurrencyModeWarning2()
        throws SQLException 
    {
        Statement s = createStatement(ResultSet.TYPE_FORWARD_ONLY,
                                          ResultSet.CONCUR_UPDATABLE);
        s.setCursorName(getNextCursorName());
        ResultSet rs = s.executeQuery
            ("select * from t1 as table1,t1 as table2 where " +
             "table1.a=table2.a");
        
        SQLWarning warn = rs.getWarnings();
        assertEquals("Expected resultset to be read only",
                     ResultSet.CONCUR_READ_ONLY,
                     rs.getConcurrency());
        assertWarning(warn, QUERY_NOT_QUALIFIED_FOR_UPDATABLE_RESULTSET);
        scrollForward(rs);
        rs.close();
        s.close();
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
        Statement s = createStatement(ResultSet.TYPE_FORWARD_ONLY,
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
        rollback();
        s.close();
    }
    
    /**
     * Test that you get an exception when specifying update clause
     * "FOR UPDATE" along with a query which is not updatable.
     * In this case, the query contains a join
     */
    public void testForUpdateException2()
        throws SQLException 
    {
        Statement s = createStatement(ResultSet.TYPE_FORWARD_ONLY,
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
        rollback();
        s.close();
    }
    
    /**
     * Test that you can scroll forward and read all records in the
     * ResultSet
     */
    public void testForwardOnlyReadOnly1()
        throws SQLException 
    {
        Statement s = createStatement(ResultSet.TYPE_FORWARD_ONLY,
                                          ResultSet.CONCUR_READ_ONLY);
        s.setCursorName(getNextCursorName());
        ResultSet rs = s.executeQuery("select * from t1");
        
        scrollForward(rs);
        rs.close();
        s.close();
    }
    
    
    /**
     * Test that you get an exception if you try to update a ResultSet
     * with concurrency mode CONCUR_READ_ONLY.
     */
    public void testFailOnUpdateOfReadOnlyResultSet1()
        throws SQLException 
    {
        Statement s = createStatement(ResultSet.TYPE_FORWARD_ONLY,
                                          ResultSet.CONCUR_READ_ONLY);
        s.setCursorName(getNextCursorName());
        ResultSet rs = s.executeQuery("select * from t1");
        
        rs.next();
        assertFailOnUpdate(rs);
        s.close();
    }
    
    /**
     * Test that you get an exception when attempting to update a
     * ResultSet which has been downgraded to a read only ResultSet.
     */
    public void testFailOnUpdateOfReadOnlyResultSet2()
        throws SQLException 
    {
        Statement s = createStatement(ResultSet.TYPE_FORWARD_ONLY,
                                          ResultSet.CONCUR_UPDATABLE);
        s.setCursorName(getNextCursorName());
        ResultSet rs = s.executeQuery("select * from t1 order by id");
        
        rs.next();
        assertFailOnUpdate(rs);
        s.close();
    }
    
    /**
     * Test that you get an exception when attempting to update a
     * ResultSet which has been downgraded to a read only ResultSet.
     */
    public void testFailOnUpdateOfReadOnlyResultSet3()
        throws SQLException 
    {
        Statement s = createStatement(ResultSet.TYPE_FORWARD_ONLY,
                                          ResultSet.CONCUR_UPDATABLE);
        s.setCursorName(getNextCursorName());
        ResultSet rs =
            s.executeQuery("select * from t1 for read only");
        
        rs.next();
        assertFailOnUpdate(rs);
        s.close();
    }
    
    /**
     * Test that you get an exception when attempting to update a
     * ResultSet which has been downgraded to a read only ResultSet.
     */
    public void testFailOnUpdateOfReadOnlyResultSet4()
        throws SQLException 
    {
        Statement s = createStatement(ResultSet.TYPE_FORWARD_ONLY,
                                          ResultSet.CONCUR_UPDATABLE);
        s.setCursorName(getNextCursorName());
        ResultSet rs = s.executeQuery
            ("select * from t1 where a=1 for read only");
        
        rs.next();
        verifyTuple(rs);
        assertFailOnUpdate(rs);
        s.close();
    }
    
    
    /**
     * Test that you get an exception if you try to update a ResultSet
     * with concurrency mode CONCUR_READ_ONLY.
     */
    public void testFailOnUpdateOfReadOnlyResultSet5()
        throws SQLException 
    {
        Statement s = createStatement(ResultSet.
                                          TYPE_SCROLL_INSENSITIVE,
                                          ResultSet.CONCUR_READ_ONLY);
        s.setCursorName(getNextCursorName());
        ResultSet rs = s.executeQuery
            ("select * from t1 where a=1 for read only");
        
        rs.next();
        verifyTuple(rs);
        assertFailOnUpdate(rs);
        s.close();
    }

    /** 
     * Test that when doing an update immediately after
     * a commit, the update fails, because the cursor has been 
     * postioned between the current row and the next row.
     * The test uses a FORWARD_ONLY resultset and ResultSet update methods
     * when doing the update.
     */
    public void testCursorStateAfterCommit1() 
        throws SQLException
    {
        testCursorStateAfterCommit(false, ResultSet.TYPE_FORWARD_ONLY);
    }

    /** 
     * Test that when doing an update immediately after
     * a commit, the update fails, because the cursor has been 
     * postioned between the current row and the next row.
     * The test uses a SCROLL_INSENSITIVE resultset and ResultSet update methods
     * when doing the update.
     */
    public void testCursorStateAfterCommit2() 
        throws SQLException
    {
        testCursorStateAfterCommit(false, ResultSet.TYPE_SCROLL_INSENSITIVE);
    }
    
     /** 
     * Test that when doing an update immediately after
     * a commit, the update fails, because the cursor has been 
     * postioned between the current row and the next row.
     * The test uses a FORWARD_ONLY resultset and positioned updates.
     */
    public void testCursorStateAfterCommit3() 
        throws SQLException
    {
        testCursorStateAfterCommit(true, ResultSet.TYPE_FORWARD_ONLY);
    }

    /** 
     * Test that when doing an update immediately after
     * a commit, the update fails, because the cursor has been 
     * postioned between the current row and the next row.
     * The test uses a SCROLL_INSENSITIVE resultset and positioned updates.
     */
    public void testCursorStateAfterCommit4() 
        throws SQLException
    {
        testCursorStateAfterCommit(true, ResultSet.TYPE_SCROLL_INSENSITIVE);
    }
    
    /** 
     * Test that when doing an update immediately after
     * a commit, the update fails, because the cursor has been 
     * postioned between the current row and the next row.
     * If the cursor gets repositioned, it allows an update.
     * @param positioned true to use positioned update, otherwise use 
     *                   ResultSet.updateRow()
     * @param resultSetType type of result set (as in ResultSet.getType())
     */
    private void testCursorStateAfterCommit(final boolean positioned, 
                                            final int resultSetType) 
        throws SQLException
    {
        final Statement s = createStatement(resultSetType, 
                                                ResultSet.CONCUR_UPDATABLE);
        final String cursorName = getNextCursorName();
        s.setCursorName(cursorName);
        
        final ResultSet rs = s.executeQuery("select a from t1");
        final int recordToUpdate = 5;
        
        if (resultSetType==ResultSet.TYPE_FORWARD_ONLY) {
            for (int i = 0; i < recordToUpdate; i++) {
                rs.next();
            }
        } else {
            rs.absolute(recordToUpdate);
        }
        
        commit();
        
        PreparedStatement ps = 
            prepareStatement("update t1 set a=? where current of " +
                                 cursorName);
        // First: check that we get an exception on update without repositioning:
        try {
            if (positioned) {
                ps.setInt(1, -1);
                ps.executeUpdate();                
                fail("Expected exception to be thrown on positioned update " + 
                     "since cursor is not positioned");
            } else {
                rs.updateInt(1, -1);
                rs.updateRow();
                fail("Expected exception to be thrown on updateRow() since " +
                     "cursor is not positioned");
            }
        } catch (SQLException e) {
            assertSQLState("Unexpected SQLState when updating row after commit",
                           SQLStateConstants.INVALID_CURSOR_STATE_NO_SUBCLASS,
                           e);
        }
        
        // Check that we after a repositioning can update:
        if (resultSetType==ResultSet.TYPE_FORWARD_ONLY) {
            rs.next();
        } else {
            rs.relative(0);
        }
        if (positioned) {
            ps.setInt(1, -1);
            ps.executeUpdate();                
        } else {
            rs.updateInt(1, -1);
            rs.updateRow();
        }
        
        s.close();
        ps.close();
        
    }

    /**
     * Test that you can correctly run multiple updateXXX() + updateRow() 
     * combined with cancelRowUpdates().
     */
    public void testMultiUpdateRow1() 
        throws SQLException 
    {
        Statement s = createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                                          ResultSet.CONCUR_UPDATABLE);
        s.setCursorName(getNextCursorName());
        ResultSet rs = s.executeQuery("select * from t1");
        rs.absolute(5);
        final int oldCol2 = rs.getInt(2);
        final int newCol2 = -2222;
        final int oldCol3 = rs.getInt(3);
        final int newCol3 = -3333;
                
        rs.updateInt(2, newCol2);
        assertEquals("Expected the resultset to be updated after updateInt",
                     newCol2, rs.getInt(2));
        rs.cancelRowUpdates();
        assertEquals("Expected updateXXX to have no effect after cancelRowUpdated",
                     oldCol2, rs.getInt(2));
        rs.updateInt(2, newCol2);
        assertEquals("Expected the resultset to be updated after updateInt", 
                     newCol2, rs.getInt(2));
        assertTrue("Expected rs.rowUpdated() to be false before updateRow", 
                   !rs.rowUpdated());
        rs.updateRow();
        
        assertTrue("Expected rs.rowUpdated() to be true after updateRow", 
                   rs.rowUpdated());
        assertEquals("Expected the resultset detect the updates of previous " + 
                     "updateRow", newCol2, rs.getInt(2));
        
        rs.updateInt(3, newCol3);
        
        assertEquals("Expected the resultset to be updated after updateInt", 
                     newCol3, rs.getInt(3));
        assertEquals("Expected the resultset detect the updates of previous " + 
                     "updateRow", newCol2, rs.getInt(2));
        
        rs.cancelRowUpdates();
        
        assertEquals("Expected updateXXX to have no effect after " +
                     "cancelRowUpdated", oldCol3, rs.getInt(3));
        assertEquals("Expected the resultset detect the updates of previous " +
                     "updateRow after cancelRowUpdated", newCol2, rs.getInt(2));
        rs.updateInt(3, newCol3);
        rs.updateRow();
        assertEquals("Expected the resultset to be updated after updateInt", 
                     newCol3, rs.getInt(3));
        rs.cancelRowUpdates();
        
        assertEquals("Expected the resultset detect the updates of previous" + 
                     "updateRow after cancelRowUpdates", newCol2, rs.getInt(2));
        assertEquals("Expected the resultset detect the updates of previous" + 
                     "updateRow after cancelRowUpdates", newCol3, rs.getInt(3));
        assertTrue("Expected rs.rowUpdated() to be true after " + 
                   "updateRow and cancelRowUpdates", rs.rowUpdated());
        
        rs.close();
        s.close();
    }

    /**
     * Test that you can correctly run multiple updateNull() + updateRow() 
     * combined with cancelRowUpdates().
     */
    public void testMultiUpdateRow2() 
        throws SQLException 
    {
        Statement s = createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                                          ResultSet.CONCUR_UPDATABLE);
        s.setCursorName(getNextCursorName());
        ResultSet rs = s.executeQuery("select * from t1");
        rs.absolute(5);
        final int oldCol2 = rs.getInt(2);
        final int oldCol3 = rs.getInt(3);
        
        rs.updateNull(2);
        assertEquals("Expected the resultset to be updated after updateNull",
                     0, rs.getInt(2));
        assertTrue("Expected wasNull to be true after updateNull", rs.wasNull());
        rs.cancelRowUpdates();
        assertEquals("Expected updateXXX to have no effect after cancelRowUpdated",
                     oldCol2, rs.getInt(2));
        rs.updateNull(2);
        assertEquals("Expected the resultset to be updated after updateNull", 
                     0, rs.getInt(2));
        assertTrue("Expected wasNull to be true after updateNull", rs.wasNull());
        assertTrue("Expected rs.rowUpdated() to be false before updateRow", 
                   !rs.rowUpdated());
        rs.updateRow();
        
        assertTrue("Expected rs.rowUpdated() to be true after updateRow", 
                   rs.rowUpdated());
        assertEquals("Expected the resultset detect the updates of previous " + 
                     "updateRow", 0, rs.getInt(2));
        
        rs.updateNull(3);
        
        assertEquals("Expected the resultset to be updated after updateNull", 
                     0, rs.getInt(3));
        assertTrue("Expected wasNull to be true after updateNull", rs.wasNull());
        assertEquals("Expected the resultset detect the updates of previous " + 
                     "updateRow", 0, rs.getInt(2));
        
        rs.cancelRowUpdates();
        
        assertEquals("Expected updateXXX to have no effect after " +
                     "cancelRowUpdated", oldCol3, rs.getInt(3));
        assertEquals("Expected the resultset detect the updates of previous " +
                     "updateRow after cancelRowUpdated", 0, rs.getInt(2));
        rs.updateNull(3);
        rs.updateRow();
        assertEquals("Expected the resultset to be updated after updateNull", 
                     0, rs.getInt(3));
        rs.cancelRowUpdates();
        
        assertEquals("Expected the resultset detect the updates of previous" + 
                     "updateRow after cancelRowUpdates", 0, rs.getInt(2));
        assertEquals("Expected the resultset detect the updates of previous" + 
                     "updateRow after cancelRowUpdates", 0, rs.getInt(3));
        assertTrue("Expected rs.rowUpdated() to be true after " + 
                   "updateRow and cancelRowUpdates", rs.rowUpdated());
        
        rs.close();
        s.close();
    }

    /**
     * Test that you get cursor operation conflict warning if updating 
     * a row which has been deleted from the table.
     */
    public void testCursorOperationConflictWarning1() 
        throws SQLException 
    {
        Statement s = createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                                          ResultSet.CONCUR_UPDATABLE);
        s.setCursorName(getNextCursorName());
        ResultSet rs = s.executeQuery("select * from t1");
        rs.next();
        createStatement().executeUpdate("delete from t1 where id=" +
                                            rs.getString("ID"));
        final int newValue = -3333;
        final int oldValue = rs.getInt(2);
        rs.updateInt(2, newValue);
        rs.updateRow();
        
        SQLWarning warn = rs.getWarnings();
        assertWarning(warn, CURSOR_OPERATION_CONFLICT);
        assertEquals("Did not expect the resultset to be updated", oldValue, rs.getInt(2));
        assertTrue("Expected rs.rowDeleted() to be false", !rs.rowDeleted());
        assertTrue("Expected rs.rowUpdated() to be false", !rs.rowUpdated());
        
        rs.clearWarnings();
        rs.deleteRow();
        warn = rs.getWarnings();
        assertWarning(warn, CURSOR_OPERATION_CONFLICT);
        rs.relative(0);
        assertTrue("Expected rs.rowUpdated() to be false", !rs.rowUpdated());
        assertTrue("Expected rs.rowDeleted() to be false", !rs.rowDeleted());
        assertEquals("Did not expect the resultset to be updated", oldValue, rs.getInt(2));
        
        rs.close();
        s.close();
    }

    /**
     * Test that you get cursor operation conflict warning if updating 
     * a row which has been deleted from the table, now using 
     * positioned updates / deletes.
     */
    public void testCursorOperationConflictWarning2() 
        throws SQLException 
    {
        Statement s = createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                                          ResultSet.CONCUR_UPDATABLE);
        s.setCursorName(getNextCursorName());
        ResultSet rs = s.executeQuery("select * from t1");
        rs.next();
        createStatement().executeUpdate ("delete from t1 where id=" +
                                             rs.getString("ID"));
        
        final int newValue = -3333;
        final int oldValue = rs.getInt(2);
        
        Statement s3 = createStatement();
        int updateCount = s3.executeUpdate
            ("update t1 set A=" + newValue + 
             " where current of " + rs.getCursorName());
        
        rs.relative(0);
        SQLWarning warn = s3.getWarnings();
        assertWarning(warn, CURSOR_OPERATION_CONFLICT);
        assertTrue("Expected rs.rowUpdated() to be false", !rs.rowUpdated());
        assertTrue("Expected rs.rowDeleted() to be false", !rs.rowDeleted());
        assertEquals("Did not expect the resultset to be updated", oldValue, rs.getInt(2));
        assertEquals("Expected update count to be 0", 0, updateCount);
        
        Statement s4 = createStatement();
        updateCount = s4.executeUpdate("delete from t1 where current of " +
                                       rs.getCursorName());
        
        rs.relative(0);
        warn = s4.getWarnings();
        assertWarning(warn, CURSOR_OPERATION_CONFLICT);
        assertTrue("Expected rs.rowUpdated() to be false", !rs.rowUpdated());
        assertTrue("Expected rs.rowDeleted() to be false", !rs.rowDeleted());
        assertEquals("Did not expect the resultset to be updated", oldValue, rs.getInt(2));
        assertEquals("Expected update count to be 0", 0, updateCount);
        
        rs.close();
        s.close();
        s3.close();
        s4.close();
    }
    
    /**
     * Test that you can scroll forward and update indexed records in
     * the ResultSet (not using FOR UPDATE)
     */
    public void testIndexedUpdateCursor1()
        throws SQLException 
    {
        Statement s = createStatement(ResultSet.TYPE_FORWARD_ONLY,
                                          ResultSet.CONCUR_UPDATABLE);
        s.setCursorName(getNextCursorName());
        ResultSet rs = s.executeQuery("select * from t1 where a=1");
        
        assertTrue("Expected to get a tuple on rs.next()", rs.next());
        verifyTuple(rs);
        updateTuple(rs);
        s.close();
        
    }
    
    /**
     *  Test that you can scroll forward and update indexed records
     *  in the ResultSet (using FOR UPDATE).
     */
    public void testIndexedUpdateCursor2()
        throws SQLException 
    {
        Statement s = createStatement(ResultSet.TYPE_FORWARD_ONLY,
                                          ResultSet.CONCUR_UPDATABLE);
        s.setCursorName(getNextCursorName());
        ResultSet rs =
            s.executeQuery("select * from t1 where a=1 for update");
        
        assertTrue("Expected to get a tuple on rs.next()", rs.next());
        verifyTuple(rs);
        updateTuple(rs);
        s.close();
    }
    
    /**
     * Tests that it is possible to move using positioning methods after
     * moveToInsertRow and that it is possible to delete a row after 
     * positioning back from insertRow. Also tests that it is possible to 
     * insert a row when positioned on insert row, that it is not possible
     * to update or delete a row from insertRow and that it also is not possible
     * to insert a row without being on insert row.
     */
    public void testInsertRowWithScrollCursor() throws SQLException {
        Statement s = createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                                          ResultSet.CONCUR_UPDATABLE);
        
        int currentPosition, lastRow;
        
        s.setCursorName(getNextCursorName());
        ResultSet rs =
            s.executeQuery("select * from t1");
        
        rs.last();
        lastRow = rs.getRow();
        
        rs.beforeFirst();
        
        rs.next();
        
        // Test that it is possible to move to next row from insertRow
        currentPosition = rs.getRow();
        rs.moveToInsertRow();
        rs.updateInt(1, currentPosition + 1000);
        rs.next();
        assertEquals("CurrentPosition should be " + (currentPosition + 1), 
                rs.getRow(), currentPosition + 1);
        // should be able to delete the row
        rs.deleteRow();

        // Test that it is possible to move using relative from insertRow
        currentPosition = rs.getRow();
        rs.moveToInsertRow();
        rs.updateInt(1, currentPosition + 1000);
        rs.relative(2);
        assertEquals("CurrentPosition should be " + (currentPosition + 2), 
                rs.getRow(), currentPosition + 2);
        // should be able to delete the row
        rs.deleteRow();

        // Test that it is possible to move using absolute from insertRow
        currentPosition = rs.getRow();
        rs.moveToInsertRow();
        rs.updateInt(1, currentPosition + 1000);
        rs.absolute(6);
        assertEquals("CurrentPosition should be 6", rs.getRow(), 6);
        // should be able to delete the row
        rs.deleteRow();

        // Test that it is possible to move to previous row from insertRow
        currentPosition = rs.getRow();
        rs.moveToInsertRow();
        rs.updateInt(1, currentPosition + 1000);
        rs.previous();
        assertEquals("CurrentPosition should be " + (currentPosition - 1), 
                rs.getRow(), currentPosition - 1);
        // should be able to delete the row
        rs.deleteRow();

        // Test that it is possible to move to first row from insertRow
        currentPosition = rs.getRow();
        rs.moveToInsertRow();
        rs.updateInt(1, currentPosition + 1000);
        rs.first();
        assertEquals("CurrentPosition should be 1", rs.getRow(), 1);
        assertTrue("isFirst() should return true", rs.isFirst());
        // should be able to delete the row
        rs.deleteRow();

        // Test that it is possible to move to last row from insertRow
        currentPosition = rs.getRow();
        rs.moveToInsertRow();
        rs.updateInt(1, currentPosition + 1000);
        rs.last();
        assertEquals("CurrentPosition should be " + lastRow, 
                rs.getRow(), lastRow);
        assertTrue("isLast() should return true", rs.isLast());
        // should be able to delete the row
        rs.deleteRow();

        // Test that it is possible to move beforeFirst from insertRow
        currentPosition = rs.getRow();
        rs.moveToInsertRow();
        rs.updateInt(1, currentPosition + 1000);
        rs.beforeFirst();
        assertTrue("isBeforeFirst() should return true", rs.isBeforeFirst());
        rs.next();
        assertEquals("CurrentPosition should be 1", rs.getRow(), 1);
        assertTrue("isFirst() should return true", rs.isFirst());

        // Test that it is possible to move afterLast from insertRow
        currentPosition = rs.getRow();
        rs.moveToInsertRow();
        rs.updateInt(1, currentPosition + 1000);
        rs.afterLast();
        assertTrue("isAfterLast() should return true", rs.isAfterLast());
        rs.previous();
        assertEquals("CurrentPosition should be " + lastRow, 
                rs.getRow(), lastRow);
        assertTrue("isLast() should return true", rs.isLast());

        // Test that it is possible to insert a row and move back to current row
        rs.previous();
        currentPosition = rs.getRow();
        rs.moveToInsertRow();
        rs.updateInt(1, currentPosition + 1000);
        rs.insertRow();
        rs.moveToCurrentRow();
        assertEquals("CurrentPosition should be " + currentPosition, 
                rs.getRow(), currentPosition);

        
        try {
            rs.moveToInsertRow();
            rs.updateInt(1, currentPosition + 2000);
            rs.updateRow();
        } catch (SQLException se) {
            assertEquals("Expected exception", 
                    se.getSQLState().substring(0, 5), 
                    INVALID_CURSOR_STATE_NO_CURRENT_ROW);
        }
        
        try {
            rs.moveToInsertRow();
            rs.updateInt(1, currentPosition + 2000);
            rs.deleteRow();
        } catch (SQLException se) {
            assertEquals("Expected exception", 
                    se.getSQLState().substring(0, 5), 
                    INVALID_CURSOR_STATE_NO_CURRENT_ROW);
        }
        
        try {
            rs.moveToCurrentRow();
            rs.updateInt(1, currentPosition + 2000);
            rs.insertRow();
        } catch (SQLException se) {
            assertEquals("Expected exception", 
                    se.getSQLState().substring(0, 5), 
                    CURSOR_NOT_POSITIONED_ON_INSERT_ROW);
        }
        
        rs.close();
        
        s.close();
    }
    
    /**
     *  Test that you can scroll forward and update indexed records
     *  in the scrollable ResultSet (not using FOR UPDATE).
     */
    public void
        testIndexedScrollInsensitiveUpdateCursorWithoutForUpdate1()
        throws SQLException 
    {
        Statement s = createStatement
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
        s.close();
    }
    
    /**
     *  Test that you can scroll forward and update indexed records
     *  in the scrollable ResultSet (using FOR UPDATE).
     */
    public void
        testIndexedScrollInsensitiveUpdateCursorWithForUpdate1()
        throws SQLException 
    {
        Statement s = createStatement
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
        Statement s = createStatement
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
        PreparedStatement ps = prepareStatement
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
        s.close();
        ps.close();
    }
        
    /**
     * Test update of a keyed record using other statement
     * object.
     */
    public void testOtherPrimaryKeyUpdate1()
        throws SQLException 
    {
        Statement s = createStatement
            (ResultSet.TYPE_SCROLL_INSENSITIVE,
             ResultSet.CONCUR_UPDATABLE);
        s.setCursorName(getNextCursorName());
        ResultSet rs = s.executeQuery("select * from t1");
        
        rs.last();
        int primaryKey = rs.getInt(1);
        PreparedStatement ps = prepareStatement
            ("update t1 set id = ? where id= ?");
        ps.setInt(1, -primaryKey);
        ps.setInt(2, primaryKey);
        assertEquals("Expected one row to be updated", 1,
                     ps.executeUpdate());
        
        rs.updateInt(2, -555);
        rs.updateInt(3, -777);
        rs.updateRow();
        
        PreparedStatement ps2 = prepareStatement
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
        
        
        s.close();
        ps.close();
        ps2.close();
    }
    
    /**
     * Test update of a keyed record using other both the
     * scrollable updatable resultset and using another statement
     * object.
     */
    public void testOtherAndOwnPrimaryKeyUpdate1()
        throws SQLException 
    {
        Statement s = createStatement
            (ResultSet.TYPE_SCROLL_INSENSITIVE,
             ResultSet.CONCUR_UPDATABLE);
        s.setCursorName(getNextCursorName());
        ResultSet rs = s.executeQuery("select * from t1");
        
        rs.last();
        int primaryKey = rs.getInt(1);
        PreparedStatement ps = prepareStatement
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
            prepareStatement("select * from t1 where id=?");
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
        
        s.close();
        ps.close();
        ps2.close();
    }
    
    /**
     * Update multiple keyed records using scrollable updatable resultset
     */
    public void testMultipleKeyUpdates()
        throws SQLException 
    {
        Statement s = createStatement
            (ResultSet.TYPE_SCROLL_INSENSITIVE,
             ResultSet.CONCUR_UPDATABLE);
        s.setCursorName(getNextCursorName());
        ResultSet rs = s.executeQuery("select * from t1");
        
        rs.last();
        int primaryKey = rs.getInt(1);
        PreparedStatement ps = s.getConnection().prepareStatement
            ("update t1 set id = ? where id= ?");
        ps.setInt(1, -primaryKey);
        ps.setInt(2, primaryKey);
        assertEquals("Expected one row to be updated", 1,
                     ps.executeUpdate());
        ps.close();
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
        rs.close();
        s.close();
    }
    
    /**
     * Test update indexed records using scrollable updatable resultset 
     */
    public void testSecondaryIndexKeyUpdate1()
        throws SQLException 
    {
        
        Statement s = createStatement
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
        PreparedStatement ps = prepareStatement
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
        
        s.close();
        ps.close();
    }
    
    /**
     * Test update indexed records using other statement object
     * and using resultset.
     */
    public void testOtherSecondaryKeyUpdate1()
        throws SQLException 
    {
        Statement s = createStatement
            (ResultSet.TYPE_SCROLL_INSENSITIVE,
             ResultSet.CONCUR_UPDATABLE);
        s.setCursorName(getNextCursorName());
        ResultSet rs = s.executeQuery("select * from t1");
        
        rs.last();
        int indexedKey = rs.getInt(2);
        PreparedStatement ps =
            prepareStatement("update t1 set a = ? where a= ?");
        ps.setInt(1, -indexedKey);
        ps.setInt(2, indexedKey);
        assertEquals("Expected one row to be updated", 1,
                     ps.executeUpdate());
        
        rs.updateInt(1, -555);
        rs.updateInt(3, -777);
        rs.updateRow();
        
        PreparedStatement ps2 =
            prepareStatement("select * from t1 where a=?");
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
        
        s.close();
        ps.close();
        ps2.close();
    }
    
    /**
     * Test scrolling in a read only resultset
     */
    public void testScrollInsensitiveReadOnly1()
        throws SQLException 
    {
        Statement s = createStatement
            (ResultSet.TYPE_SCROLL_INSENSITIVE,
             ResultSet.CONCUR_READ_ONLY);
        s.setCursorName(getNextCursorName());
        ResultSet rs = s.executeQuery("select * from t1");
        
        scrollForward(rs);
        scrollBackward(rs);
        rs.close();
        s.close();
    }
    
    /**
     * Test updating a forward only resultset (with FOR UPDATE)
     */
    public void testForwardOnlyConcurUpdatableWithForUpdate1()
        throws SQLException 
    {
        Statement s = createStatement
            (ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
        s.setCursorName(getNextCursorName());
        ResultSet rs = s.executeQuery("select * from t1 for update");
        
        scrollForwardAndUpdate(rs);
        rs.close();
        s.close();
    }
    
    /**
     * Test updating a forward only resultset (without FOR UPDATE)
     */
    public void testForwardOnlyConcurUpdatableWithoutForUpdate1()
        throws SQLException 
    {
        Statement s = createStatement
            (ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
        s.setCursorName(getNextCursorName());
        ResultSet rs = s.executeQuery("select * from t1");
        
        scrollForwardAndUpdate(rs);
        rs.close();
        s.close();
    }
    
    /**
     * Test updating a forward only resultset (without FOR UPDATE)
     * and using positioned update
     */
    public void testPositionedUpdateWithoutForUpdate1()
        throws SQLException 
    {
        Statement s = createStatement
            (ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
        s.setCursorName("MYCURSOR");
        ResultSet rs = s.executeQuery("select * from t1");
        
        scrollForwardAndUpdatePositioned(rs);
        rs.close();
        s.close();
    }
    
    /**
     * Test updating a forward only resultset (with FOR UPDATE)
     * and using positioned update
     */
    public void testPositionedUpdateWithForUpdate1()
        throws SQLException 
    {
        Statement s = createStatement();
        s.setCursorName(getNextCursorName());
        ResultSet rs = s.executeQuery("select * from t1 for update");
        
        scrollForwardAndUpdatePositioned(rs);
        rs.close();
        s.close();
    }
    
    /**
     * Test positioned update of a scrollable resultset (with FOR UPDATE) 
     */
    public void testScrollablePositionedUpdateWithForUpdate1()
        throws SQLException 
    {
        Statement s = createStatement
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
        PreparedStatement ps = prepareStatement
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

        s.close();
        ps.close();
    }
    
    /**
     * Test update of a scrollable resultset (with FOR UPDATE)
     * Only scrolling forward
     */
    public void testScrollInsensitiveConcurUpdatableWithForUpdate1()
        throws SQLException 
    {
        Statement s = createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, 
                                          ResultSet.CONCUR_UPDATABLE);
        s.setCursorName(getNextCursorName());
        ResultSet rs = s.executeQuery("select * from t1 for update");
        scrollForwardAndUpdate(rs);
        rs.close();
        s.close();
    }
    
    /**
     * Test update of a scrollable resultset (with FOR UPDATE) 
     * Scrolling forward and backward.
     */
    public void testScrollInsensitiveConcurUpdatableWithForUpdate2()
        throws SQLException 
    {
        Statement s = createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, 
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
        s.close();
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
        Statement s = createStatement(ResultSet.TYPE_FORWARD_ONLY, 
                                          ResultSet.CONCUR_READ_ONLY);
        s.setCursorName(getNextCursorName());
        rs = s.executeQuery("select * from t1");
        
        while (rs.next()) {
            int id = rs.getInt(1);
            int a = rs.getInt(2);
            int b = rs.getInt(3);
            println("Updated tuple:" + id + "," + a + "," + b);
        }
        s.close();
    }
    
    /**
     * Test update of a scrollable resultset (with FOR UPDATE)
     * Scrolling forward and backward. Then open another
     * resultset and verify the data.
     */
    public void testScrollInsensitiveConcurUpdatableWithForUpdate3()
        throws SQLException 
    {
        Statement s = createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, 
                                          ResultSet.CONCUR_UPDATABLE);
        s.setCursorName(getNextCursorName());
        ResultSet rs = s.executeQuery("select a,b from t1 for update");
        
        testScrollInsensistiveConurUpdatable3(rs);
        s.close();
    }
    
    /**
     * Test update of a scrollable resultset (without FOR UPDATE) 
     * Scrolling forward only
     */
    public void testScrollInsensitiveConcurUpdatableWithoutForUpdate1()
        throws SQLException 
    {
        Statement s = createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, 
                                          ResultSet.CONCUR_UPDATABLE);
        s.setCursorName(getNextCursorName());
        ResultSet rs = s.executeQuery("select * from t1");
        
        scrollForwardAndUpdate(rs);
        rs.close();
        s.close();
    }
    
    /**
     * Test update of a scrollable resultset (without FOR UPDATE) 
     * Scrolling forward and backward.
     */
    public void testScrollInsensitiveConcurUpdatableWithoutForUpdate2()
        throws SQLException 
    {
        Statement s = createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, 
                                          ResultSet.CONCUR_UPDATABLE);
        s.setCursorName(getNextCursorName());
        ResultSet rs = s.executeQuery("select * from t1");
        
        scrollForward(rs);
        scrollBackwardAndUpdate(rs);
        rs.close();
        s.close();
    }
    
    /**
     * Test update of a scrollable resultset (without FOR UPDATE)
     * Scrolling forward and backward. Then open another
     * resultset and verify the data.
     */
    public void testScrollInsensitiveConcurUpdatableWithoutForUpdate3()
        throws SQLException 
    {
        Statement s = createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, 
                                          ResultSet.CONCUR_UPDATABLE);
        s.setCursorName(getNextCursorName());
        ResultSet rs = s.executeQuery("select a,b from t1");
        
        testScrollInsensistiveConurUpdatable3(rs);
        s.close();
    }
    
    /**
     * DERBY-4198 "When using the FOR UPDATE OF clause with SUR
     * (Scroll-insensive updatable result sets), the updateRow() method crashes"
     *
     * This bug revealed missing logic to handle the fact the the ExecRow
     * passed down to ScrollInsensitiveResultSet.updateRow does not always
     * contain all the rows of the basetable, cf. the logic of RowChangerImpl.
     * When an explicit list of columns is given as in FOR UPDATE OF
     * <column-list>, the ExecRow may contains a subset of the the base table
     * columns and ScrollInsensitiveResultSet was not ready to handle that.
     *
     * Test some of the cases which went wrong before the fix.
     *
     */
    public void testForUpdateWithColumnList() throws SQLException {
        Statement s = createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                                          ResultSet.CONCUR_UPDATABLE);

        // case a)
        ResultSet rs = s.executeQuery("select c from t1 for update of c");

        rs.next();
        rs.updateString(1,"foobar");
        rs.updateRow();
        rs.next();
        rs.previous();
        assertEquals("foobar", rs.getString(1));
        rs.close();

        // case b)
        rs = s.executeQuery("select id from t1 for update of id");
        rs.next();
        rs.updateInt(1,20);
        rs.updateRow();
        rs.next();
        rs.previous();
        assertEquals(20, rs.getInt(1));
        rs.close();

        // case c)
        rs = s.executeQuery("select * from t1 for update of id");
        rs.next();
        rs.updateInt(1,20);
        rs.updateRow();
        rs.next();
        rs.previous();
        assertEquals(20, rs.getInt(1));
        rs.close();

        // case d)
        rs = s.executeQuery("SELECT * from t1 for update of c");
        rs.next();
        int id = rs.getInt(1);
        int a =  rs.getInt(2);
        int b =  rs.getInt(3);
        rs.updateString(4,"foobar");
        rs.updateRow();
        rs.next();
        rs.previous();
        assertEquals(id, rs.getInt(1));
        assertEquals(a, rs.getInt(2));
        assertEquals(b, rs.getInt(3));
        assertEquals("foobar", rs.getString(4));
        rs.close();

        // case e)
        rs = s.executeQuery("SELECT * from t1 for update of id,a,b,c");
        rs.next();
        rs.updateInt(1, -20);
        rs.updateInt(2, 20);
        rs.updateInt(3, 21);
        rs.updateString(4,"foobar");
        rs.updateRow();
        rs.next();
        rs.previous();
        assertEquals(-20, rs.getInt(1));
        assertEquals(20, rs.getInt(2));
        assertEquals(21, rs.getInt(3));
        assertEquals("foobar", rs.getString(4));
        rs.close();

        // case f)
        rs = s.executeQuery("SELECT * from t1 for update of id, a,b,c");
        rs.next();
        rs.updateInt(1, 20);
        rs.updateRow();
        rs.next();
        rs.previous();
        assertEquals(20, rs.getInt(1));
        rs.close();

        // case h)
        rs = s.executeQuery("SELECT id from t1 for update of id, c");
           String cursorname = rs.getCursorName();
        rs.next();
           Statement s2 = createStatement();
        s2.executeUpdate("update t1 set c='foobar' where current of " +
                         cursorname);
        s2.close();
        rs.next();
        rs.previous();
        rs.getInt(1); // gives error state 22018 before fix
        rs.close();

        // case i)
        rs = s.executeQuery("SELECT id from t1 for update");
        cursorname = rs.getCursorName();
        rs.next();
        s2 = createStatement();
        s2.executeUpdate("update t1 set c='foobar' where current of " +
                         cursorname);
        s2.close();
        rs.next();
        rs.previous();
        rs.getInt(1); // ok before fix
        rs.close();

        // Odd cases: base row mentioned twice in rs, update 1st instance
        rs = s.executeQuery("SELECT id,a,id from t1");
        rs.next();
        rs.updateInt(1, 20);
        rs.updateRow();
        rs.next();
        rs.previous();
        assertEquals(20, rs.getInt(1));
        assertEquals(20, rs.getInt(3));
        rs.close();

        // Odd cases: base row mentioned twice in rs, update 2nd instance
        // with explicit column list; fails, see DERBY-4226.
        rs = s.executeQuery("SELECT id,a,id from t1 for update of id");
        rs.next();
        try {
            rs.updateInt(3, 20);
            fail("should fail");
        } catch (SQLException e) {
            String sqlState = usingEmbedded() ? "42X31" : "XJ124";
            assertSQLState(sqlState, e);
        }
        rs.close();

        // Odd cases: base row mentioned twice in rs, update 2nd instance
        // without explicit column list; works
        rs = s.executeQuery("SELECT id,a,id from t1 for update");
        rs.next();
        rs.updateInt(3, 20);
        rs.updateRow();
        assertEquals(20, rs.getInt(1));
        assertEquals(20, rs.getInt(3));
        rs.next();
        rs.previous();
        assertEquals(20, rs.getInt(1));
        assertEquals(20, rs.getInt(3));
        rs.close();

        s.close();
    }


    /**
     * Check that detectability methods throw the correct exception
     * when called in an illegal row state, that is, somehow not
     * positioned on a row. Minion of testDetectabilityExceptions.
     *
     * @param rs An open updatable result set.
     * @param state A string describing the illegal state.
     */
    private void checkDetectabilityCallsOutsideRow(ResultSet rs, 
                                                   String state)
    {
        boolean b;
        
        try {
            b = rs.rowUpdated();
            fail("rowUpdated while " + state + 
                 " did not throw exception: " + b);
        } catch (SQLException e) {
            assertEquals(e.getSQLState(),
                         INVALID_CURSOR_STATE_NO_CURRENT_ROW);
        }

        try {
            b = rs.rowDeleted();
            fail("rowdeleted while " + state + 
                 " did not throw exception: " + b);
        } catch (SQLException e) {
            assertEquals(e.getSQLState(),
                         INVALID_CURSOR_STATE_NO_CURRENT_ROW);
        }

        try {
            b = rs.rowInserted();
            fail("rowInserted while " + state + 
                 " did not throw exception: " + b);
        } catch (SQLException e) {
            assertEquals(e.getSQLState(),
                         INVALID_CURSOR_STATE_NO_CURRENT_ROW);
        }
    }


    /**
     * Test that rowUpdated() and rowDeleted() methods both return true when
     * the row has first been updated and then deleted using the updateRow()
     * and deleteRow() methods.
     */
    public void testRowUpdatedAndRowDeleted() throws SQLException {
        Statement s = createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, 
                                           ResultSet.CONCUR_UPDATABLE);
        s.setCursorName(getNextCursorName());
        ResultSet rs = s.executeQuery("select a,b from t1");
        rs.next();
        rs.updateInt(1, rs.getInt(1) + 2 * recordCount);
        rs.updateRow();
        assertTrue("Expected rowUpdated() to return true", rs.rowUpdated());
        rs.deleteRow();
        rs.next();
        rs.previous();
        assertTrue("Expected rowUpdated() to return true", rs.rowUpdated());
        assertTrue("Expected rowDeleted() to return true", rs.rowDeleted());
        rs.next();
        assertFalse("Expected rowUpdated() to return false", rs.rowUpdated());
        assertFalse("Expected rowDeleted() to return false", rs.rowDeleted());
        rs.previous();
        assertTrue("Expected rowUpdated() to return true", rs.rowUpdated());
        assertTrue("Expected rowDeleted() to return true", rs.rowDeleted());
        rs.close();
        s.close();
    }


    /**
     * Test that the JDBC detectability calls throw correct exceptions when
     * called in in wrong row states. 
     * This is done for both supported updatable result set types.
     */
    public void testDetectabilityExceptions() throws SQLException 
    {
        Statement s = createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, 
                                          ResultSet.CONCUR_UPDATABLE);
        ResultSet rs = s.executeQuery("select * from t1");
        
        checkDetectabilityCallsOutsideRow(rs, "before positioning");

        rs.moveToInsertRow();
        checkDetectabilityCallsOutsideRow(rs, 
                                          "on insertRow before positioning");

        rs.next();
        rs.moveToInsertRow();
        checkDetectabilityCallsOutsideRow(rs, "on insertRow");
        rs.moveToCurrentRow(); // needed until to DERBY-1322 is fixed

        rs.beforeFirst();
        checkDetectabilityCallsOutsideRow(rs, "on beforeFirst row");

        rs.afterLast();
        checkDetectabilityCallsOutsideRow(rs, "on afterLast row");

        rs.first();
        rs.deleteRow();
        checkDetectabilityCallsOutsideRow(rs, "after deleteRow");

        rs.last();
        rs.deleteRow();
        checkDetectabilityCallsOutsideRow(rs, "after deleteRow of last row");

        rs.close();
        s.close();

        // Not strictly SUR, but fixed in same patch, so we test it here.
        s = createStatement(ResultSet.TYPE_FORWARD_ONLY, 
                                ResultSet.CONCUR_UPDATABLE);
        rs = s.executeQuery("select * from t1");

        checkDetectabilityCallsOutsideRow(rs, "before FO positioning");

        rs.moveToInsertRow();
        checkDetectabilityCallsOutsideRow(rs, 
                                          "on insertRow before FO positioning");

        rs.next();
        rs.moveToInsertRow();
        checkDetectabilityCallsOutsideRow(rs, "on FO insertRow");

        rs.next();
        rs.updateInt(2, 666);
        rs.updateRow();
        checkDetectabilityCallsOutsideRow(rs, "after FO updateRow");

        rs.next();
        rs.deleteRow();
        checkDetectabilityCallsOutsideRow(rs, "after FO deleteRow");

        while (rs.next()) {};
        checkDetectabilityCallsOutsideRow(rs, "after FO emptied out");

        rs.close();
        s.close();
    }

    /**
     * DERBY-1481 - ResultSet.beforeFirst() gives protocol error on scrollable,
     * updatable result sets that are downgraded to read-only
     * 
     * Check that no exception is thrown when calling positioning methods on a
     * result set that has been downgraded to read-only.
     *
     */
    public void testDowngradeToScrollReadOnly() throws SQLException {
        Statement s = createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, 
                                          ResultSet.CONCUR_UPDATABLE);
        ResultSet rs = s.executeQuery("select * from t1 order by b");

        // check that the ResultSet was downgraded
        assertWarning(rs.getWarnings(), 
                QUERY_NOT_QUALIFIED_FOR_UPDATABLE_RESULTSET);
        
        // call positioning methods
        rs.next();
        rs.next();
        rs.previous();
        rs.relative(1);
        rs.absolute(3);
        rs.relative(-1);
        rs.first();
        rs.last();
        rs.beforeFirst();
        rs.afterLast();
        
        // close result set and statement
        rs.close();
        s.close();
    }


    /**
     * Get a cursor name. We use the same cursor name for all cursors.
     */
    private final String getNextCursorName() {
        return "MYCURSOR";
    }
    
    /**
     * Run the base suite in embedded and client mode.
     */
    public static Test suite() {
        TestSuite mainSuite = new TestSuite("SURTest");
        mainSuite.addTest(baseSuite("SURTest:embedded"));
        mainSuite.addTest(
                TestConfiguration.clientServerDecorator(baseSuite("SURTest:client")));
        return mainSuite;      
    }
    
    /**
     * The suite contains all testcases in this class running on different 
     * data models
     */
    private static Test baseSuite(String name) { 
        
        TestSuite mainSuite = new TestSuite(name);
        
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
