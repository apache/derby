/*
 *
 * Derby - Class HoldabilityTest
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
import junit.framework.*;
import java.sql.*;

import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Tests holdable resultsets.
 */
public class HoldabilityTest extends SURBaseTest {
    
    /** Creates a new instance of HoldabilityTest */
    public HoldabilityTest(String name) {
        super(name, 1000); // We will use 1000 records
    }
    
    public static Test suite() {
               
        // DB2 client doesn't support this functionality
        if (usingDB2Client())
            return new TestSuite();
        
        return TestConfiguration.defaultSuite(HoldabilityTest.class);
    }

    /**
     * Sets up the connection, then create the data model
     */
    protected void setUp() 
        throws Exception 
    {      
       // For the holdability tests, we recreate the model
        // for each testcase (since we do commits)
        
        // We also use more records to ensure that the disk
        // is being used.
        SURDataModelSetup.createDataModel
            (SURDataModelSetup.SURDataModel.MODEL_WITH_PK, getConnection(),
             recordCount);
        commit();
    }
    
    protected void tearDown() throws Exception
    {
    	// Commit any changes, they will be dropped
    	// anyway by the next setUp to run or the
    	// outer database cleaners. It's faster to
    	// commit than rollback many changes.
    	commit();
    	super.tearDown();
    }
    
    /**
     * Test that a forward only resultset can be held over commit while
     * it has not done any scanning
     */
    public void testHeldForwardOnlyResultSetScanInit() 
        throws SQLException
    {
        Statement s = createStatement();
        ResultSet rs = s.executeQuery(selectStatement);
        
        commit(); // scan initialized
        
        scrollForward(rs);
        s.close();
    }
    
    /**
     * Test that a forward only resultset can be held over commit while
     * it is in progress of scanning
     */
    public void testHeldForwardOnlyResultSetScanInProgress() 
        throws SQLException
    {
        Statement s = createStatement();
        ResultSet rs = s.executeQuery(selectStatement);

        for (int i=0; i<this.recordCount/2; i++) {
            rs.next();
            verifyTuple(rs);
        }
        commit(); // Scan is in progress
        
        while (rs.next()) {
            verifyTuple(rs);
        }
        s.close();
    }

    /**
     * Test that a forward only resultset can be held over commit while
     * it has not done any scanning, and be updatable
     */
    public void testHeldForwardOnlyUpdatableResultSetScanInit() 
        throws SQLException
    {
        Statement s = createStatement(ResultSet.TYPE_FORWARD_ONLY, 
                                          ResultSet.CONCUR_UPDATABLE);
        ResultSet rs = s.executeQuery(selectStatement);
        commit(); // scan initialized
        rs.next();    // naviagate to a new tuple
        updateTuple(rs); // Updatable
        scrollForward(rs);
        s.close();
    }
    
    
    /**
     * Test that a forward only resultset can be held over commit while
     * it is in progress of scanning, and that after a compress the
     * resultset is still updatable.
     */
    public void testCompressOnHeldForwardOnlyUpdatableResultSetScanInProgress()
        throws SQLException
    {
        Statement s = createStatement(ResultSet.TYPE_FORWARD_ONLY, 
                                          ResultSet.CONCUR_UPDATABLE);
        ResultSet rs = s.executeQuery(selectStatement);

        for (int i=0; i<this.recordCount/2; i++) {
            rs.next();
            verifyTuple(rs);
        }
        updateTuple(rs);
        commit(); // Scan is in progress
        
        // Verifies resultset can do updates after compress
        verifyResultSetUpdatableAfterCompress(rs);
        s.close();
        
    }

    /**
     * Test that a forward only resultset can be held over commit while
     * it has not done any scanning, and that after a compress it is
     * still updatable.
     */
    public void testCompressOnHeldForwardOnlyUpdatableResultSetScanInit() 
        throws SQLException
    {
        Statement s = createStatement(ResultSet.TYPE_FORWARD_ONLY, 
                                          ResultSet.CONCUR_UPDATABLE);
        ResultSet rs = s.executeQuery(selectStatement);
        commit(); // scan initialized
        
        // Verifies resultset can do updates after compress
        verifyResultSetUpdatableAfterCompress(rs);
        s.close();
    }
        
    /**
     * Test that a forward only resultset can be held over commit while
     * it is in progress of scanning
     */
    public void testHeldForwardOnlyUpdatableResultSetScanInProgress() 
        throws SQLException
    {
        Statement s = createStatement(ResultSet.TYPE_FORWARD_ONLY, 
                                          ResultSet.CONCUR_UPDATABLE);
        ResultSet rs = s.executeQuery(selectStatement);

        for (int i=0; i<this.recordCount/2; i++) {
            rs.next();
            verifyTuple(rs);
        }
        updateTuple(rs);
        commit(); // Scan is in progress
        rs.next();
        updateTuple(rs); // Still updatable
        while (rs.next()) {
            verifyTuple(rs); // complete the scan
        }
        s.close();
    }
    
    /**
     * Test that a scrollable resultset can be held over commit while
     * it has not done any scanning
     */
    public void testHeldScrollableResultSetScanInit() 
        throws SQLException
    {
        Statement s = createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, 
                                          ResultSet.CONCUR_READ_ONLY);
        ResultSet rs = s.executeQuery(selectStatement);
        
        commit(); // scan initialized
        
        scrollForward(rs);
        scrollBackward(rs);
        
        s.close();
    }
        
    /**
     * Test that a scrollable resultset can be held over commit while
     * it is in progress of scanning
     */
    public void testHeldScrollableResultSetScanInProgress() 
        throws SQLException
    {
        Statement s = createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, 
                                          ResultSet.CONCUR_READ_ONLY);
        ResultSet rs = s.executeQuery(selectStatement);

        for (int i=0; i<this.recordCount/2; i++) {
            rs.next();
            verifyTuple(rs);
        }
        commit(); // Scan is in progress
        
        while (rs.next()) {
            verifyTuple(rs);
        }
        scrollBackward(rs);
        s.close();
    }

    /**
     * Test that a scrollable resultset can be held over commit
     * after the resultset has been populated
     */
    public void testHeldScrollableResultSetScanDone() 
        throws SQLException
    {
        Statement s = createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, 
                                          ResultSet.CONCUR_READ_ONLY);
        ResultSet rs = s.executeQuery(selectStatement);
        
        scrollForward(rs); // Scan is done
        
        commit();
        
        scrollBackward(rs);
        s.close();
    }

    /**
     * Test that a scrollable updatable resultset can be held over commit 
     * while it has not done any scanning
     */
    public void testHeldScrollableUpdatableResultSetScanInit() 
        throws SQLException
    {
        Statement s = createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, 
                                          ResultSet.CONCUR_UPDATABLE);
        ResultSet rs = s.executeQuery(selectStatement);
        
        if (rs.getConcurrency()==ResultSet.CONCUR_READ_ONLY) {
            fail("ResultSet concurrency downgraded to CONCUR_READ_ONLY");
        }
        commit(); // scan initialized
        
        scrollForward(rs);
        scrollBackwardAndUpdate(rs);
        
        s.close();
    }    
    
    /**
     * Test that a scrollable updatable resultset can be held over commit while
     * it is in progress of scanning
     */
    public void testHeldScrollableUpdatableResultSetScanInProgress() 
        throws SQLException
    {
        Statement s = createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, 
                                          ResultSet.CONCUR_UPDATABLE);
        ResultSet rs = s.executeQuery(selectStatement);
        if (rs.getConcurrency()==ResultSet.CONCUR_READ_ONLY) {
            fail("ResultSet concurrency downgraded to CONCUR_READ_ONLY");
        }        
        for (int i=0; i<this.recordCount/2; i++) {
            rs.next();
            verifyTuple(rs);
        }
        commit(); // Scan is in progress
        
        while (rs.next()) {
            verifyTuple(rs);
        }
        scrollBackwardAndUpdate(rs);
        
        s.close();
    }

    /**
     * Test that a scrollable updatable resultset can be held over commit
     * after the resultset has been populated
     */
    public void testHeldScrollableUpdatableResultSetScanDone() 
        throws SQLException
    {
        Statement s = createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, 
                                          ResultSet.CONCUR_UPDATABLE);
        ResultSet rs = s.executeQuery(selectStatement);
        
        if (rs.getConcurrency()==ResultSet.CONCUR_READ_ONLY) {
            fail("ResultSet concurrency downgraded to CONCUR_READ_ONLY");
        }
      
        scrollForward(rs); // Scan is done
        
        commit();
        
        scrollBackwardAndUpdate(rs);
        
        s.close();
    }

    /**
     * Test that updateRow() after a commit requires a renavigation 
     * on a held forward only ResulTset.
     */
    public void testUpdateRowAfterCommitOnHeldForwardOnlyResultSet() 
        throws SQLException
    {
        Statement s = createStatement(ResultSet.TYPE_FORWARD_ONLY, 
                                          ResultSet.CONCUR_UPDATABLE);
        ResultSet rs = s.executeQuery(selectStatement);
        
        if (rs.getConcurrency()==ResultSet.CONCUR_READ_ONLY) {
            fail("ResultSet concurrency downgraded to CONCUR_READ_ONLY");
        }
        rs.next();
        commit();
        try {
            rs.updateInt(2, -100);
            rs.updateRow();
            fail("Expected updateRow() to throw exception");
        } catch (SQLException e) {
            assertEquals("Unexpected SQLState",
                         INVALID_CURSOR_STATE_NO_CURRENT_ROW, e.getSQLState());
        }
        s.close();
    }

    /**
     * Test that updateRow() after a commit requires a renavigation 
     * on a held scrollinsensitve ResulTset.
     */
    public void testUpdateRowAfterCommitOnHeldScrollInsensitiveResultSet() 
        throws SQLException
    {
        Statement s = createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, 
                                          ResultSet.CONCUR_UPDATABLE);
        ResultSet rs = s.executeQuery(selectStatement);
        
        if (rs.getConcurrency()==ResultSet.CONCUR_READ_ONLY) {
            fail("ResultSet concurrency downgraded to CONCUR_READ_ONLY");
        }
        rs.next();
        commit();
        try {
            rs.updateInt(2, -100);
            rs.updateRow();
            fail("Expected updateRow() to throw exception");
        } catch (SQLException e) {
            assertEquals("Unexpected SQLState",
                         INVALID_CURSOR_STATE_NO_CURRENT_ROW, e.getSQLState());
        }
        s.close();
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
        Statement s = createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, 
                                          ResultSet.CONCUR_UPDATABLE);
        
        ResultSet rs = s.executeQuery(selectStatement);
        if (rs.getConcurrency()==ResultSet.CONCUR_READ_ONLY) {
            fail("ResultSet concurrency downgraded to CONCUR_READ_ONLY");
        }
        commit(); // commit
        
        // Verifies resultset can do updates after compress
        verifyResultSetUpdatableAfterCompress(rs);
        
        s.close();
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
        Statement s = createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, 
                                          ResultSet.CONCUR_UPDATABLE);
        ResultSet rs = s.executeQuery(selectStatement);
        if (rs.getConcurrency()==ResultSet.CONCUR_READ_ONLY) {
            fail("ResultSet concurrency downgraded to CONCUR_READ_ONLY");
        }
        rs.next(); // Scan is in progress.
        
        commit(); // commit, releases the lock on the records
        
        verifyCompressInvalidation(rs);
        
        s.close();
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
        Statement s = createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, 
                                          ResultSet.CONCUR_UPDATABLE);
        ResultSet rs = s.executeQuery(selectStatement);
        if (rs.getConcurrency()==ResultSet.CONCUR_READ_ONLY) {
            fail("ResultSet concurrency downgraded to CONCUR_READ_ONLY");
        }
        
        scrollForward(rs); // scan is done
        
        commit(); // commit, releases the lock on the records
        
        verifyCompressInvalidation(rs);
        s.close();
    }

    /**
     * Verifies that even after a compress, the ResultSet of this type and
     * state is updatable.
     */
    private void verifyResultSetUpdatableAfterCompress(ResultSet rs) 
        throws SQLException
    {
                // Delete all records except the first:
        Statement delStatement = createStatement();
        int deleted = delStatement.executeUpdate("delete from T1 where id>0");
        int expectedDeleted = recordCount-1;
        delStatement.close();
        
        assertEquals("Invalid number of records deleted", expectedDeleted, 
                     deleted);
        commit();
        
        // Execute online compress
        onlineCompress(true, true, true);
        
        // Now reinsert the tuples:
        PreparedStatement ps = 
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
        ps.close();
        commit();

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
        
        commit();
        
        // Verify data
        rs = createStatement().executeQuery(selectStatement);
        while (rs.next()) {            
            verifyTuple(rs);
        }
        rs.close();
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
        Statement delStatement = createStatement();
        int deleted = delStatement.executeUpdate("delete from T1 where id>0");
        int expectedDeleted = recordCount-1;    
        delStatement.close();
        
        assertEquals("Invalid number of records deleted", expectedDeleted, 
                     deleted);
        commit();
        
        // Execute online compress
        onlineCompress(true, true, true);
        
        // Now reinsert the tuples:
        PreparedStatement ps = 
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
        ps.close();
        commit();
        
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
        commit();
        
        // Verify data
        rs = createStatement().executeQuery(selectStatement);
        while (rs.next()) {            
            // This will fail if we managed to update reinserted tuple
            verifyTuple(rs); 
        }
        rs.close();
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
        final Connection con2 = openDefaultConnection();
        con2.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
        
        final PreparedStatement ps2 = con2.prepareStatement
            ("call SYSCS_UTIL.SYSCS_INPLACE_COMPRESS_TABLE(?,?,?,?,?)");
        ps2.setString(1, "APP"); // schema
        ps2.setString(2, "T1");  // table name
        ps2.setBoolean(3, purge);
        ps2.setBoolean(4, defragment);
        ps2.setBoolean(5, truncate);
        
        ps2.executeUpdate();
        ps2.close();
        con2.commit();
        con2.close();
    }

    private final static String selectStatement = "select * from t1";
}
