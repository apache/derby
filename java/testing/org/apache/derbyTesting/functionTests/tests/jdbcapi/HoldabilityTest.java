/*
 *
 * Derby - Class HoldabilityTest
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
import junit.framework.*;
import java.sql.*;

/**
 * Tests holdable resultsets.
 */
public class HoldabilityTest extends SURBaseTest {
    
    /** Creates a new instance of HoldabilityTest */
    public HoldabilityTest(String name) {
        super(name, 1000); // We will use 1000 records
    }

    /**
     * Sets up the connection, then create the data model
     */
    public void setUp() 
        throws Exception 
    {      
        try {
            super.setUp();
        } catch (SQLException e) {
            if (con!=null) tearDown();
            throw e;
        }
        // For the holdability tests, we recreate the model
        // for each testcase (since we do commits)
        
        // We also use more records to ensure that the disk
        // is being used.
        SURDataModelSetup.createDataModel
            (SURDataModelSetup.SURDataModel.MODEL_WITH_PK, con,
             recordCount);
        con.commit();
    }
    
    /**
     * Drop the data model, and close the connection
     */
    public void tearDown() 
    {
        try {            
            con.rollback();
            Statement dropStatement = con.createStatement();
            dropStatement.execute("drop table t1");
            con.commit();
            con.close();
        } catch (SQLException e) {
            printStackTrace(e); // Want to propagate the real exception.
        }
    }
    
    /**
     * Test that a forward only resultset can be held over commit while
     * it has not done any scanning
     */
    public void testHeldForwardOnlyResultSetScanInit() 
        throws SQLException
    {
        Statement s = con.createStatement();
        ResultSet rs = s.executeQuery(selectStatement);
        
        con.commit(); // scan initialized
        
        scrollForward(rs);
    }
    
    /**
     * Test that a forward only resultset can be held over commit while
     * it is in progress of scanning
     */
    public void testHeldForwardOnlyResultSetScanInProgress() 
        throws SQLException
    {
        Statement s = con.createStatement();
        ResultSet rs = s.executeQuery(selectStatement);

        for (int i=0; i<this.recordCount/2; i++) {
            rs.next();
            verifyTuple(rs);
        }
        con.commit(); // Scan is in progress
        
        while (rs.next()) {
            verifyTuple(rs);
        }
    }

    /**
     * Test that a forward only resultset can be held over commit while
     * it has not done any scanning, and be updatable
     */
    public void testHeldForwardOnlyUpdatableResultSetScanInit() 
        throws SQLException
    {
        Statement s = con.createStatement(ResultSet.TYPE_FORWARD_ONLY, 
                                          ResultSet.CONCUR_UPDATABLE);
        ResultSet rs = s.executeQuery(selectStatement);
        con.commit(); // scan initialized
        rs.next();    // naviagate to a new tuple
        updateTuple(rs); // Updatable
        scrollForward(rs);
    }
    
    
    /**
     * Test that a forward only resultset can be held over commit while
     * it is in progress of scanning, and that after a compress the
     * resultset is still updatable.
     */
    public void testCompressOnHeldForwardOnlyUpdatableResultSetScanInProgress()
        throws SQLException
    {
        Statement s = con.createStatement(ResultSet.TYPE_FORWARD_ONLY, 
                                          ResultSet.CONCUR_UPDATABLE);
        ResultSet rs = s.executeQuery(selectStatement);

        for (int i=0; i<this.recordCount/2; i++) {
            rs.next();
            verifyTuple(rs);
        }
        updateTuple(rs);
        con.commit(); // Scan is in progress
        
        // Verifies resultset can do updates after compress
        verifyResultSetUpdatableAfterCompress(rs);
        
    }

    /**
     * Test that a forward only resultset can be held over commit while
     * it has not done any scanning, and that after a compress it is
     * still updatable.
     */
    public void testCompressOnHeldForwardOnlyUpdatableResultSetScanInit() 
        throws SQLException
    {
        Statement s = con.createStatement(ResultSet.TYPE_FORWARD_ONLY, 
                                          ResultSet.CONCUR_UPDATABLE);
        ResultSet rs = s.executeQuery(selectStatement);
        con.commit(); // scan initialized
        
        // Verifies resultset can do updates after compress
        verifyResultSetUpdatableAfterCompress(rs);
    }
        
    /**
     * Test that a forward only resultset can be held over commit while
     * it is in progress of scanning
     */
    public void testHeldForwardOnlyUpdatableResultSetScanInProgress() 
        throws SQLException
    {
        Statement s = con.createStatement(ResultSet.TYPE_FORWARD_ONLY, 
                                          ResultSet.CONCUR_UPDATABLE);
        ResultSet rs = s.executeQuery(selectStatement);

        for (int i=0; i<this.recordCount/2; i++) {
            rs.next();
            verifyTuple(rs);
        }
        updateTuple(rs);
        con.commit(); // Scan is in progress
        rs.next();
        updateTuple(rs); // Still updatable
        while (rs.next()) {
            verifyTuple(rs); // complete the scan
        }
    }
    
    /**
     * Test that a scrollable resultset can be held over commit while
     * it has not done any scanning
     */
    public void testHeldScrollableResultSetScanInit() 
        throws SQLException
    {
        Statement s = con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, 
                                          ResultSet.CONCUR_READ_ONLY);
        ResultSet rs = s.executeQuery(selectStatement);
        
        con.commit(); // scan initialized
        
        scrollForward(rs);
        scrollBackward(rs);
    }
        
    /**
     * Test that a scrollable resultset can be held over commit while
     * it is in progress of scanning
     */
    public void testHeldScrollableResultSetScanInProgress() 
        throws SQLException
    {
        Statement s = con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, 
                                          ResultSet.CONCUR_READ_ONLY);
        ResultSet rs = s.executeQuery(selectStatement);

        for (int i=0; i<this.recordCount/2; i++) {
            rs.next();
            verifyTuple(rs);
        }
        con.commit(); // Scan is in progress
        
        while (rs.next()) {
            verifyTuple(rs);
        }
        scrollBackward(rs);
    }

    /**
     * Test that a scrollable resultset can be held over commit
     * after the resultset has been populated
     */
    public void testHeldScrollableResultSetScanDone() 
        throws SQLException
    {
        Statement s = con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, 
                                          ResultSet.CONCUR_READ_ONLY);
        ResultSet rs = s.executeQuery(selectStatement);
        
        scrollForward(rs); // Scan is done
        
        con.commit();
        
        scrollBackward(rs);
    }

    /**
     * Test that a scrollable updatable resultset can be held over commit 
     * while it has not done any scanning
     */
    public void testHeldScrollableUpdatableResultSetScanInit() 
        throws SQLException
    {
        Statement s = con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, 
                                          ResultSet.CONCUR_UPDATABLE);
        ResultSet rs = s.executeQuery(selectStatement);
        
        if (rs.getConcurrency()==ResultSet.CONCUR_READ_ONLY) {
            assertTrue("ResultSet concurrency downgraded to CONCUR_READ_ONLY",
                       false);
        }
        con.commit(); // scan initialized
        
        scrollForward(rs);
        scrollBackwardAndUpdate(rs);
    }    
    
    /**
     * Test that a scrollable updatable resultset can be held over commit while
     * it is in progress of scanning
     */
    public void testHeldScrollableUpdatableResultSetScanInProgress() 
        throws SQLException
    {
        Statement s = con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, 
                                          ResultSet.CONCUR_UPDATABLE);
        ResultSet rs = s.executeQuery(selectStatement);
        if (rs.getConcurrency()==ResultSet.CONCUR_READ_ONLY) {
            assertTrue("ResultSet concurrency downgraded to CONCUR_READ_ONLY",
                       false);
        }        
        for (int i=0; i<this.recordCount/2; i++) {
            rs.next();
            verifyTuple(rs);
        }
        con.commit(); // Scan is in progress
        
        while (rs.next()) {
            verifyTuple(rs);
        }
        scrollBackwardAndUpdate(rs);
    }

    /**
     * Test that a scrollable updatable resultset can be held over commit
     * after the resultset has been populated
     */
    public void testHeldScrollableUpdatableResultSetScanDone() 
        throws SQLException
    {
        Statement s = con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, 
                                          ResultSet.CONCUR_UPDATABLE);
        ResultSet rs = s.executeQuery(selectStatement);
        
        if (rs.getConcurrency()==ResultSet.CONCUR_READ_ONLY) {
            assertTrue("ResultSet concurrency downgraded to CONCUR_READ_ONLY",
                       false);
        }
      
        scrollForward(rs); // Scan is done
        
        con.commit();
        
        scrollBackwardAndUpdate(rs);
    }

    /**
     * Test that updateRow() after a commit requires a renavigation 
     * on a held forward only ResulTset.
     */
    public void testUpdateRowAfterCommitOnHeldForwardOnlyResultSet() 
        throws SQLException
    {
        Statement s = con.createStatement(ResultSet.TYPE_FORWARD_ONLY, 
                                          ResultSet.CONCUR_UPDATABLE);
        ResultSet rs = s.executeQuery(selectStatement);
        
        if (rs.getConcurrency()==ResultSet.CONCUR_READ_ONLY) {
            assertTrue("ResultSet concurrency downgraded to CONCUR_READ_ONLY",
                       false);
        }
        rs.next();
        con.commit();
        try {
            rs.updateInt(2, -100);
            rs.updateRow();
            assertTrue("Expected updateRow() to throw exception", false);
        } catch (SQLException e) {
            assertEquals("Unexpected SQLState",
                         INVALID_CURSOR_STATE_NO_CURRENT_ROW, e.getSQLState());
        }
    }

    /**
     * Test that updateRow() after a commit requires a renavigation 
     * on a held scrollinsensitve ResulTset.
     */
    public void testUpdateRowAfterCommitOnHeldScrollInsensitiveResultSet() 
        throws SQLException
    {
        Statement s = con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, 
                                          ResultSet.CONCUR_UPDATABLE);
        ResultSet rs = s.executeQuery(selectStatement);
        
        if (rs.getConcurrency()==ResultSet.CONCUR_READ_ONLY) {
            assertTrue("ResultSet concurrency downgraded to CONCUR_READ_ONLY",
                       false);
        }
        rs.next();
        con.commit();
        try {
            rs.updateInt(2, -100);
            rs.updateRow();
            assertTrue("Expected updateRow() to throw exception", false);
        } catch (SQLException e) {
            assertEquals("Unexpected SQLState",
                         INVALID_CURSOR_STATE_NO_CURRENT_ROW, e.getSQLState());
        }
    }

    /**
     * Test that running a compress on a holdable scrollable updatable 
     * resultset will not invalidate the ResultSet from doing updates,
     * if the scan is initialized
     */
    public void testCompressOnHeldScrollableUpdatableResultSetScanInit()
        throws SQLException
    {
        // First: Read all records in the table into the ResultSet:
        Statement s = con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, 
                                          ResultSet.CONCUR_UPDATABLE);
        
        ResultSet rs = s.executeQuery(selectStatement);
        if (rs.getConcurrency()==ResultSet.CONCUR_READ_ONLY) {
            assertTrue("ResultSet concurrency downgraded to CONCUR_READ_ONLY",
                       false);
        }
        con.commit(); // commit
        
        // Verifies resultset can do updates after compress
        verifyResultSetUpdatableAfterCompress(rs);
    }

    /**
     * Test that running a compress on a holdable scrollable updatable 
     * resultset will invalidate the Resultset from doing updates after 
     * a renavigate, if the scan is in progress.
     */
    public void testCompressOnHeldScrollableUpdatableResultSetScanInProgress()
        throws SQLException
    {
        // First: Read all records in the table into the ResultSet:
        Statement s = con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, 
                                          ResultSet.CONCUR_UPDATABLE);
        ResultSet rs = s.executeQuery(selectStatement);
        if (rs.getConcurrency()==ResultSet.CONCUR_READ_ONLY) {
            assertTrue("ResultSet concurrency downgraded to CONCUR_READ_ONLY",
                       false);
        }
        rs.next(); // Scan is in progress.
        
        con.commit(); // commit, releases the lock on the records
        
        verifyCompressInvalidation(rs);
    }
    
    /**
     * Test that running a compress on a holdable scrollable updatable 
     * resultset will invalidate the Resultset from doing updates after 
     * a renavigate.
     */
    public void testCompressOnHeldScrollableUpdatableResultSetScanDone()
        throws SQLException
    {
        // First: Read all records in the table into the ResultSet:
        Statement s = con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, 
                                          ResultSet.CONCUR_UPDATABLE);
        ResultSet rs = s.executeQuery(selectStatement);
        if (rs.getConcurrency()==ResultSet.CONCUR_READ_ONLY) {
            assertTrue("ResultSet concurrency downgraded to CONCUR_READ_ONLY",
                       false);
        }
        
        scrollForward(rs); // scan is done
        
        con.commit(); // commit, releases the lock on the records
        
        verifyCompressInvalidation(rs);
    }

    /**
     * Verifies that even after a compress, the ResultSet of this type and
     * state is updatable.
     */
    private void verifyResultSetUpdatableAfterCompress(ResultSet rs) 
        throws SQLException
    {
                // Delete all records except the first:
        Statement delStatement = con.createStatement();
        int deleted = delStatement.executeUpdate("delete from T1 where id>0");
        int expectedDeleted = recordCount-1;    
        
        assertEquals("Invalid number of records deleted", expectedDeleted, 
                     deleted);
        con.commit();
        
        // Execute online compress
        onlineCompress(true, true, true);
        
        // Now reinsert the tuples:
        PreparedStatement ps = con.
            prepareStatement("insert into t1 values (?,?,?,?)");
        
        for (int i=0; i<recordCount*2; i++) {
            int recordId = i + recordCount + 1000;
            ps.setInt(1, recordId);
            ps.setInt(2, recordId);
            ps.setInt(3, recordId *2 + 17);
            ps.setString(4, "m" + recordId);
            ps.addBatch();
        }
        ps.executeBatch();
        con.commit();

        rs.next();
        updateTuple(rs);
        
        SQLWarning warn = rs.getWarnings();
        assertNull("Expected no warning when updating this row", warn);
        
        // This part if only for scrollable resultsets
        if (rs.getType()!=ResultSet.TYPE_FORWARD_ONLY) {
            
            // Update last tuple
            rs.last();         
            updateTuple(rs);
            
            warn = rs.getWarnings();
            assertNull("Expected no warning when updating this row", warn);
            
            // Update first tuple
            rs.first();
            updateTuple(rs);
            warn = rs.getWarnings();
            assertNull("Expected no warning when updating this row", warn);
        }
        
        con.commit();
        
        // Verify data
        rs = con.createStatement().executeQuery(selectStatement);
        while (rs.next()) {            
            verifyTuple(rs);
        }
    }

    /**
     * Verifies that the ResultSet is invalidated from doing updates after
     * a compress.
     * @param rs ResultSet which we test is being invalidated
     */
    private void verifyCompressInvalidation(ResultSet rs) 
        throws SQLException 
    {
        
        // Delete all records except the first:
        Statement delStatement = con.createStatement();
        int deleted = delStatement.executeUpdate("delete from T1 where id>0");
        int expectedDeleted = recordCount-1;    
        
        assertEquals("Invalid number of records deleted", expectedDeleted, 
                     deleted);
        con.commit();
        
        // Execute online compress
        onlineCompress(true, true, true);
        
        // Now reinsert the tuples:
        PreparedStatement ps = con.
            prepareStatement("insert into t1 values (?,?,?,?)");
        
        for (int i=0; i<recordCount*2; i++) {
            int recordId = i + recordCount + 1000;
            ps.setInt(1, recordId);
            ps.setInt(2, recordId);
            ps.setInt(3, recordId *2 + 17);
            ps.setString(4, "m" + recordId);
            ps.addBatch();
        }
        ps.executeBatch();
        con.commit();
        
        // Update last tuple
        rs.last();         
        rs.updateInt(2, -100);
        rs.updateRow();
        SQLWarning warn = rs.getWarnings();
        assertWarning(warn, CURSOR_OPERATION_CONFLICT);
        rs.clearWarnings();
        
        // Update first tuple
        rs.first(); 
        rs.updateInt(2, -100);
        updateTuple(rs); 
        warn = rs.getWarnings();
        assertWarning(warn, CURSOR_OPERATION_CONFLICT);
        con.commit();
        
        // Verify data
        rs = con.createStatement().executeQuery(selectStatement);
        while (rs.next()) {            
            // This will fail if we managed to update reinserted tuple
            verifyTuple(rs); 
        }
    }

    /**
     * Executes online compress
     * @param purge set to true to purge rows
     * @param defragment set to true to defragment rows
     * @param truncate set to true to truncate pages
     */
    private void onlineCompress(boolean purge, 
                                boolean defragment, 
                                boolean truncate)
        throws SQLException
    {
               // Use a new connection to compress the table        
        final Connection con2 = getNewConnection();
        final String connId = con2.toString();
        con2.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
        
        final PreparedStatement ps2 = con2.prepareStatement
            ("call SYSCS_UTIL.SYSCS_INPLACE_COMPRESS_TABLE(?,?,?,?,?)");
        ps2.setString(1, "APP"); // schema
        ps2.setString(2, "T1");  // table name
        ps2.setBoolean(3, purge);
        ps2.setBoolean(4, defragment);
        ps2.setBoolean(5, truncate);
        
        try { 
            ps2.executeUpdate();
            con2.commit();
        } finally {
            con2.close();
        }
    }

    private final static String selectStatement = "select * from t1";
}
