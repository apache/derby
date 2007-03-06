/*
 *
 * Derby - Class ConcurrencyTest
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
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.derbyTesting.junit.DatabasePropertyTestSetup;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Testing concurrency behaviour in derby when creating the resultsets with
 * different parameters.
 */
public class ConcurrencyTest extends SURBaseTest {
    
    /** Creates a new instance of ConcurrencyTest */
    public ConcurrencyTest(String name) {
        super(name);
    }

    /**
     * Sets up the connection, then create the data model
     */
    public void setUp() 
        throws Exception 
    {      
        // For the concurrency tests, we recreate the model
        // for each testcase (since we do commits)
        SURDataModelSetup.createDataModel
            (SURDataModelSetup.SURDataModel.MODEL_WITH_PK, getConnection());
        commit();
    }
    
    public void tearDown() throws Exception 
    {
        try {
            rollback();
            Statement dropStatement = createStatement();
            dropStatement.execute("drop table t1");
            dropStatement.close();
        } catch (SQLException e) {
            printStackTrace(e); // Want to propagate the real exception.
        }
        super.tearDown();
    }
    
    /**
     * Test that update locks are downgraded to shared locks
     * after repositioning.
     * This test fails with Derby
     */
    public void testUpdateLockDownGrade1()
        throws SQLException 
    {
        Statement s = createStatement(ResultSet.TYPE_FORWARD_ONLY, 
                                          ResultSet.CONCUR_UPDATABLE);
        ResultSet rs = s.executeQuery("select * from t1 for update");
        
        // After navigating through the resultset, 
        // presumably all rows are locked with shared locks
        while (rs.next());
        
        // Now open up a connection
        Connection con2 = openDefaultConnection();
        Statement s2 = con2.createStatement(ResultSet.TYPE_FORWARD_ONLY, 
                                            ResultSet.CONCUR_UPDATABLE);
        
        ResultSet rs2 = s2.executeQuery("select * from t1 for update");
        try {
            rs2.next(); // We should be able to get a update lock here.
        } catch (SQLException e) {
            assertEquals("Unexpected SQL state",  LOCK_TIMEOUT_SQL_STATE,
                         e.getSQLState());
            return;
        } finally {
            con2.rollback();
        }
        assertTrue("Expected Derby to hold updatelocks in RR mode", false);
        
        s2.close();
        con2.close();
        
        s.close();
    }
    
    /**
     * Test that we can aquire a update lock even if the row is locked with 
     * a shared lock.
     */
    public void testAquireUpdateLock1()
        throws SQLException 
    {
        Statement s = createStatement();
        ResultSet rs = s.executeQuery("select * from t1");
        
        // After navigating through the resultset, 
        // presumably all rows are locked with shared locks
        while (rs.next());
        
        // Now open up a connection
        Connection con2 = openDefaultConnection();
        Statement s2 = con2.createStatement(ResultSet.TYPE_FORWARD_ONLY, 
                                            ResultSet.CONCUR_UPDATABLE);
        
        ResultSet rs2 = s2.executeQuery("select * from t1 for update");
        try {
            rs2.next(); // We should be able to get a update lock here.
        } finally {
            con2.rollback();
        }
        
        s2.close();
        con2.close();
        s.close();
    }
    
    /*
     * Test that we do not get a concurrency problem when opening two cursors
     * as readonly.
     **/
    public void testSharedLocks1()
        throws SQLException 
    {
        Statement s = createStatement(ResultSet.TYPE_FORWARD_ONLY, 
                                          ResultSet.CONCUR_READ_ONLY);
        final ResultSet rs = s.executeQuery("select * from t1");
        scrollForward(rs);
        Connection con2 = openDefaultConnection();
        Statement s2 = con2.createStatement(ResultSet.TYPE_FORWARD_ONLY, 
                                            ResultSet.CONCUR_READ_ONLY);
        try {
            final ResultSet rs2 = s2.executeQuery("select * from t1");
            scrollForward(rs2);
        } finally {
            rs.close();
            con2.rollback();
            con2.close();
        }
        
        s.close();
    }
    
    /*
     * Test that we do not get a concurrency problem when opening two cursors 
     * reading the same data (no parameters specified to create statement).
     **/
    public void testSharedLocks2()
        throws SQLException 
    {
        Statement s = createStatement();
        ResultSet rs = s.executeQuery("select * from t1");
        scrollForward(rs);
        Connection con2 = openDefaultConnection();
        Statement s2 = con2.createStatement();
        try {
            final ResultSet rs2 = s2.executeQuery("select * from t1");
            scrollForward(rs2);
        } finally {
            rs.close();
            con2.rollback();
            con2.close();
        }
        s.close();
    }
    
    /*
     * Test that we do not get a concurrency problem when opening one cursor
     * as updatable (not using "for update"), and another cursor as read only
     **/
    public void testSharedAndUpdateLocks1()
        throws SQLException {
        Statement s = createStatement(ResultSet.TYPE_FORWARD_ONLY, 
                                          ResultSet.CONCUR_UPDATABLE);
        
        ResultSet rs = s.executeQuery("select * from t1");
        scrollForward(rs);
        Connection con2 = openDefaultConnection();
        Statement s2 = con2.createStatement(ResultSet.TYPE_FORWARD_ONLY, 
                                            ResultSet.CONCUR_READ_ONLY);
        try {
            final ResultSet rs2 = s2.executeQuery("select * from t1");
            scrollForward(rs2);
        } finally {
            rs.close();
            con2.rollback();
            con2.close();
        }
        s.close();
    }
    
    /*
     * Test that we do no get a concurrency problem when opening one cursor
     * as updatable (using "for update"), and another cursor as read only.
     *
     **/
    public void testSharedAndUpdateLocks2()
        throws SQLException 
    {
        Statement s = createStatement(ResultSet.TYPE_FORWARD_ONLY, 
                                          ResultSet.CONCUR_UPDATABLE);
        ResultSet rs = s.executeQuery("select * from t1 for update");
        scrollForward(rs);
        Connection con2 = openDefaultConnection();
        Statement s2 = con2.createStatement(ResultSet.TYPE_FORWARD_ONLY, 
                                            ResultSet.CONCUR_READ_ONLY);
        try {
            final ResultSet rs2 = s2.executeQuery("select * from t1");
            scrollForward(rs2);
        } finally {
            rs.close();
            con2.rollback();
            con2.close();
        }
        s.close();
    }
    
    /**
     * Test what happens if you update a deleted + purged tuple.
     * The transaction which deletes the tuple, will also
     * ensure that the tuple is purged from the table, not only marked
     * as deleted.
     **/
    public void testUpdatePurgedTuple1()
        throws SQLException
    {
        getConnection().setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
        Statement s = createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, 
                                          ResultSet.CONCUR_UPDATABLE);
        ResultSet rs = s.executeQuery("select * from t1");
        rs.next();
        int firstKey = rs.getInt(1);
        println("T1: Read next Tuple:(" + rs.getInt(1) + "," +
                rs.getInt(2) + "," +
                rs.getInt(3) + ")");
        int lastKey = firstKey;
        while (rs.next()) {
            lastKey = rs.getInt(1);
            println("T1: Read next Tuple:(" + rs.getInt(1) + "," +
                    rs.getInt(2) + "," +
                    rs.getInt(3) + ")");
        }
        
        Connection con2 = openDefaultConnection();
        con2.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
        try {
            PreparedStatement ps2 = con2.prepareStatement
                ("delete from t1 where id=? or id=?");
            ps2.setInt(1, firstKey);
            ps2.setInt(2, lastKey);
            assertEquals("Expected two records to be deleted", 
                         2, ps2.executeUpdate());
            println("T2: Deleted records with id=" + firstKey + " and id=" + 
                    lastKey);
            con2.commit();
            println("T2: commit");
            ps2 = con2.prepareStatement
                ("call SYSCS_UTIL.SYSCS_INPLACE_COMPRESS_TABLE(?,?,?,?,?)");
            ps2.setString(1, "APP"); // schema
            ps2.setString(2, "T1");  // table name
            ps2.setInt(3, 1); // purge
            ps2.setInt(4, 0); // defragment rows
            ps2.setInt(5, 0); // truncate end
            println("T3: call SYSCS_UTIL.SYSCS_INPLACE_COMPRESS_TABLE");
            println("T3: purges deleted records");
            ps2.executeUpdate();
            con2.commit();
            println("T3: commit");
        } catch (SQLException e) {
            con2.rollback();
            throw e;
        }
        rs.first(); // Go to first tuple
        println("T1: Read first Tuple:(" + rs.getInt(1) + "," +
                rs.getInt(2) + "," +
                rs.getInt(3) + ")");
        rs.updateInt(2, 3);
        println("T1: updateInt(2, 3);");
        rs.updateRow();
        println("T1: updateRow()");
        rs.last(); // Go to last tuple
        println("T1: Read last Tuple:(" + rs.getInt(1) + "," +
                rs.getInt(2) + "," +
                rs.getInt(3) + ")");
        rs.updateInt(2, 3);
        println("T1: updateInt(2, 3);");
        rs.updateRow();
        println("T1: updateRow()");
        commit();
        println("T1: commit");
        rs = s.executeQuery("select * from t1");
        println("T3: select * from table");
        while (rs.next()) {
            println("T3: Read next Tuple:(" + rs.getInt(1) + "," +
                    rs.getInt(2) + "," +
                    rs.getInt(3) + ")");
            
        }
        
        con2.close();
        s.close();
    }
    
    /**
     * Test what happens if you update a deleted tuple using positioned update
     * (same as testUpdatePurgedTuple1, except here we use positioned updates)
     **/
    public void testUpdatePurgedTuple2()
        throws SQLException 
    {
        getConnection().setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
        Statement s = createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, 
                                          ResultSet.CONCUR_UPDATABLE);
        ResultSet rs = s.executeQuery("select * from t1");
        rs.next(); // Point to first tuple
        println("T1: Read next Tuple:(" + rs.getInt(1) + "," +
                rs.getInt(2) + "," +
                rs.getInt(3) + ")");
        int firstKey = rs.getInt(1);
        rs.next(); // Go to next
        println("T1: Read next Tuple:(" + rs.getInt(1) + "," +
                rs.getInt(2) + "," +
                rs.getInt(3) + ")");
        Connection con2 = openDefaultConnection();
        con2.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
        try {
            PreparedStatement ps2 = con2.prepareStatement
                ("delete from t1 where id=?");
            ps2.setInt(1, firstKey);
            assertEquals("Expected one record to be deleted", 1, 
                         ps2.executeUpdate());
            println("T2: Deleted record with id=" + firstKey);
            con2.commit();
            println("T2: commit");
        } catch (SQLException e) {
            con2.rollback();
            throw e;
        }
        rs.previous(); // Go back to first tuple
        println("T1: Read previous Tuple:(" + rs.getInt(1) + "," +
                rs.getInt(2) + "," +
                rs.getInt(3) + ")");
        
        PreparedStatement ps = prepareStatement
            ("update T1 set a=? where current of " + rs.getCursorName());
        ps.setInt(1, 3);
        int updateCount = ps.executeUpdate();
        println("T1: update table, set a=3 where current of " + 
                rs.getCursorName());
        println("T1: commit");
        commit();
        rs = s.executeQuery("select * from t1");
        while (rs.next()) {
            println("T3: Tuple:(" + rs.getInt(1) + "," +
                    rs.getInt(2) + "," +
                    rs.getInt(3) + ")");
            
        }
        
        con2.close();
    }
    
    /**
     * Test what happens if you update a tuple which is deleted, purged and
     * reinserted
     **/
    public void testUpdatePurgedTuple3()
        throws SQLException 
    {
        getConnection().setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
        Statement s = createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, 
                                          ResultSet.CONCUR_UPDATABLE);
        ResultSet rs = s.executeQuery("select * from t1");
        rs.next(); // Point to first tuple
        int firstKey = rs.getInt(1);
        println("T1: read tuple with key " + firstKey);
        rs.next(); // Go to next
        println("T1: read next tuple");
        Connection con2 = openDefaultConnection();
        con2.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
        try {
            PreparedStatement ps2 = con2.prepareStatement
                ("delete from t1 where id=?");
            ps2.setInt(1, firstKey);
            assertEquals("Expected one record to be deleted", 1, 
                         ps2.executeUpdate());
            println("T2: Deleted record with id=" + firstKey);
            con2.commit();
            println("T2: commit");
            
            // Now purge the table
            ps2 = con2.prepareStatement
                ("call SYSCS_UTIL.SYSCS_INPLACE_COMPRESS_TABLE(?,?,?,?,?)");
            ps2.setString(1, "APP"); // schema
            ps2.setString(2, "T1");  // table name
            ps2.setInt(3, 1); // purge
            ps2.setInt(4, 0); // defragment rows
            ps2.setInt(5, 0); // truncate end
            println("T3: call SYSCS_UTIL.SYSCS_INPLACE_COMPRESS_TABLE");
            println("T3: purges deleted records");
            ps2.executeUpdate();
            con2.commit();
            
            ps2 = con2.prepareStatement("insert into t1 values(?,?,?,?)");
            ps2.setInt(1, firstKey);
            ps2.setInt(2, -1);
            ps2.setInt(3, -1);
            ps2.setString(4, "UPDATED TUPLE");
            assertEquals("Expected one record to be inserted", 1, 
                         ps2.executeUpdate());
            println("T4: Inserted record (" + firstKey + ",-1,-1)" );
            con2.commit();
            println("T4: commit");
        } catch (SQLException e) {
            con2.rollback();
            throw e;
        }
        println("T1: read previous tuple");
        rs.previous(); // Go back to first tuple
        println("T1: id=" + rs.getInt(1));
        rs.updateInt(2, 3);
        println("T1: updateInt(2, 3);");
        rs.updateRow();
        println("T1: updated column 2, to value=3");
        println("T1: commit");
        commit();
        rs = s.executeQuery("select * from t1");
        while (rs.next()) {
            println("T5: Read Tuple:(" + rs.getInt(1) + "," +
                    rs.getInt(2) + "," +
                    rs.getInt(3) + ")");
            
        }
        
        con2.close();
    }
    
    /**
     * Test what happens if you update a tuple which is deleted, purged and 
     * then reinserted with the exact same values
     **/
    public void testUpdatePurgedTuple4()
        throws SQLException 
    {
        getConnection().setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
        Statement s = createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, 
                                          ResultSet.CONCUR_UPDATABLE);
        ResultSet rs = s.executeQuery("select * from t1");
        rs.next(); // Point to first tuple
        int firstKey = rs.getInt(1);
        int valA = rs.getInt(2);
        int valB = rs.getInt(3);
        
        println("T1: Read next Tuple:(" + rs.getInt(1) + "," +
                rs.getInt(2) + "," +
                rs.getInt(3) + ")");
        
        rs.next(); // Go to next
        println("T1: Read next Tuple:(" + rs.getInt(1) + "," +
                rs.getInt(2) + "," +
                rs.getInt(3) + ")");
        Connection con2 = openDefaultConnection();
        con2.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
        try {
            PreparedStatement ps2 = con2.prepareStatement
                ("delete from t1 where id=?");
            ps2.setInt(1, firstKey);
            assertEquals("Expected one record to be deleted", 1, 
                         ps2.executeUpdate());
            println("T2: Deleted record with id=" + firstKey);
            con2.commit();
            println("T2: commit");
            
            // Now purge the table
            ps2 = con2.prepareStatement
                ("call SYSCS_UTIL.SYSCS_INPLACE_COMPRESS_TABLE(?,?,?,?,?)");
            ps2.setString(1, "APP"); // schema
            ps2.setString(2, "T1");  // table name
            ps2.setInt(3, 1); // purge
            ps2.setInt(4, 0); // defragment rows
            ps2.setInt(5, 0); // truncate end
            println("T3: call SYSCS_UTIL.SYSCS_INPLACE_COMPRESS_TABLE");
            println("T3: purges deleted records");
            ps2.executeUpdate();
            con2.commit();
            println("T3: commit");
            
            ps2 = con2.prepareStatement("insert into t1 values(?,?,?,?)");
            ps2.setInt(1, firstKey);
            ps2.setInt(2, valA);
            ps2.setInt(3, valB);
            ps2.setString(4, "UPDATE TUPLE " + firstKey);
            assertEquals("Expected one record to be inserted", 1, 
                         ps2.executeUpdate());
            println("T4: Inserted record (" + firstKey + "," + valA + "," + 
                    valB + ")" );
            con2.commit();
            println("T4: commit");
        } catch (SQLException e) {
            con2.rollback();
            throw e;
        }
        rs.previous(); // Go back to first tuple
        println("T1: Read previous Tuple:(" + rs.getInt(1) + "," +
                rs.getInt(2) + "," +
                rs.getInt(3) + ")");
        
        println("T1: id=" + rs.getInt(1));
        rs.updateInt(2, 3);
        rs.updateRow();
        println("T1: updated column 2, to value=3");
        println("T1: commit");
        commit();
        rs = s.executeQuery("select * from t1");
        while (rs.next()) {
            println("T4: Read next Tuple:(" + rs.getInt(1) + "," +
                    rs.getInt(2) + "," +
                    rs.getInt(3) + ")");
            
        }
        con2.close();
    }
    
    /**
     * Test what happens if you update a tuple which has been modified by 
     * another transaction.
     **/
    public void testUpdateModifiedTuple1()
        throws SQLException 
    {
        getConnection().setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
        Statement s = createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, 
                                          ResultSet.CONCUR_UPDATABLE);
        ResultSet rs = s.executeQuery("select * from t1");
        rs.next(); // Point to first tuple
        println("T1: Read next Tuple:(" + rs.getInt(1) + "," +
                rs.getInt(2) + "," +
                rs.getInt(3) + ")");
        int firstKey = rs.getInt(1);
        rs.next(); // Go to next
        println("T1: Read next Tuple:(" + rs.getInt(1) + "," +
                rs.getInt(2) + "," +
                rs.getInt(3) + ")");
        Connection con2 = openDefaultConnection();
        con2.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
        try {
            PreparedStatement ps2 = con2.prepareStatement
                ("update t1 set b=? where id=?");
            ps2.setInt(1, 999);
            ps2.setInt(2, firstKey);
            assertEquals("Expected one record to be updated", 1, 
                         ps2.executeUpdate());
            println("T2: Updated b=999 where id=" + firstKey);
            con2.commit();
            println("T2: commit");
        } catch (SQLException e) {
            con2.rollback();
            throw e;
        }
        rs.previous(); // Go back to first tuple
        println("T1: Read previous Tuple:(" + rs.getInt(1) + "," +
                rs.getInt(2) + "," +
                rs.getInt(3) + ")");
        rs.updateInt(2, 3);
        rs.updateRow();
        println("T1: updated column 2, to value=3");
        commit();
        println("T1: commit");
        rs = s.executeQuery("select * from t1");
        while (rs.next()) {
            println("T3: Read next Tuple:(" + rs.getInt(1) + "," +
                    rs.getInt(2) + "," +
                    rs.getInt(3) + ")");
            
        }
        con2.close();
    }
    
    /**
     * Test what happens if you update a tuple which has been modified by 
     * another transaction (in this case the same column)
     **/
    public void testUpdateModifiedTuple2()
        throws SQLException 
    {
        getConnection().setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
        Statement s = createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, 
                                          ResultSet.CONCUR_UPDATABLE);
        ResultSet rs = s.executeQuery("select * from t1");
        rs.next(); // Point to first tuple
        println("T1: Read next Tuple:(" + rs.getInt(1) + "," +
                rs.getInt(2) + "," +
                rs.getInt(3) + ")");
        int firstKey = rs.getInt(1);
        rs.next(); // Go to next
        println("T1: Read next Tuple:(" + rs.getInt(1) + "," +
                rs.getInt(2) + "," +
                rs.getInt(3) + ")");
        Connection con2 = openDefaultConnection();
        con2.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
        try {
            PreparedStatement ps2 = con2.prepareStatement
                ("update t1 set b=? where id=?");
            ps2.setInt(1, 999);
            ps2.setInt(2, firstKey);
            assertEquals("Expected one record to be updated", 1, 
                         ps2.executeUpdate());
            println("T2: Updated b=999 where id=" + firstKey);
            con2.commit();
            println("T2: commit");
        } catch (SQLException e) {
            con2.rollback();
            throw e;
        }
        rs.previous(); // Go back to first tuple
        println("T1: Read previous Tuple:(" + rs.getInt(1) + "," +
                rs.getInt(2) + "," +
                rs.getInt(3) + ")");
        rs.updateInt(3, 9999);
        rs.updateRow();
        println("T1: updated column 3, to value=9999");
        commit();
        println("T1: commit");
        rs = s.executeQuery("select * from t1");
        while (rs.next()) {
            println("T3: Read next Tuple:(" + rs.getInt(1) + "," +
                    rs.getInt(2) + "," +
                    rs.getInt(3) + ")");
            
        }
        con2.close();
    }
    
    /**
     * Tests that a ResultSet opened even in read uncommitted, gets a 
     * table intent lock, and that another transaction then cannot compress 
     * the table while the ResultSet is open.
     **/
    public void testTableIntentLock1()
        throws SQLException 
    {
        getConnection().setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
        Statement s = createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, 
                                          ResultSet.CONCUR_UPDATABLE);
        println("T1: select * from t1");
        ResultSet rs = s.executeQuery("select * from t1 for update");
        while (rs.next()) {
            println("T1: Read next Tuple:(" + rs.getInt(1) + "," +
                    rs.getInt(2) + "," +
                    rs.getInt(3) + ")");
        } // Now the cursor does not point to any tuples
        
        // Compressing the table in another transaction:
        Connection con2 = openDefaultConnection();
        
        con2.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
        PreparedStatement ps2 = con2.prepareStatement
            ("call SYSCS_UTIL.SYSCS_COMPRESS_TABLE(?, ?, ?)");
        ps2.setString(1, "APP");
        ps2.setString(2, "T1");
        ps2.setInt(3, 0);
        println("T2: call SYSCS_UTIL.SYSCS_COMPRESS_TABLE(APP, T1, 0)");
        try {
            ps2.executeUpdate(); // This will hang
            fail("Expected T2 to hang");
        } catch (SQLException e) {
            println("T2: Got exception:" + e.getMessage());
            
            assertSQLState(LOCK_TIMEOUT_EXPRESSION_SQL_STATE, e);

        }
        ps2.close();
        con2.rollback();
        con2.close();
        
        s.close();
    }
    
    /**
     * Test that Derby set updatelock on current row when using
     * read-uncommitted
     **/
    public void testUpdateLockInReadUncommitted()
        throws SQLException 
    {
        getConnection().setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
        Statement s = createStatement(ResultSet.TYPE_FORWARD_ONLY, 
                                          ResultSet.CONCUR_UPDATABLE);
        ResultSet rs = s.executeQuery("select * from t1");
        rs.next();
        int firstKey = rs.getInt(1);
        println("T1: Read next Tuple:(" + rs.getInt(1) + "," +
                rs.getInt(2) + "," +
                rs.getInt(3) + ")");
        Connection con2 = openDefaultConnection();
        con2.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);

        PreparedStatement ps2 = con2.prepareStatement
                ("delete from t1 where id=?");
        ps2.setInt(1, firstKey);
        try {
            ps2.executeUpdate();
            fail("expected record with id=" + firstKey + 
                       " to be locked");
        } catch (SQLException e) {
            assertSQLState(LOCK_TIMEOUT_SQL_STATE, e);
        }
        
        ps2.close();
        con2.rollback();
        con2.close();
        s.close();
    }
    
    /**
     * Test that the system cannot defragment any records
     * as long as an updatable result set is open against the table.
     **/
    public void testDefragmentDuringScan() 
        throws SQLException
    {
        testCompressDuringScan(true, false);
    }
    /**
     * Test that the system cannot truncate any records
     * as long as an updatable result set is open against the table.
     **/
    public void testTruncateDuringScan() 
        throws SQLException
    {
        testCompressDuringScan(false, true);
    }
    
    /**
     * Test that the system does not purge any records
     * as long as we do either a defragment, or truncate
     **/
    private void testCompressDuringScan(boolean testDefragment, 
                                        boolean testTruncate)
        throws SQLException 
    {
        getConnection().setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
        Statement delStatement = createStatement();
        // First delete all records except the last and first
        int deleted = delStatement.executeUpdate
            ("delete from T1 where id>0 and id<" + (recordCount-1));
        int expectedDeleted = recordCount-2;    
        println("T1: delete records");
        assertEquals("Invalid number of records deleted", expectedDeleted, 
                     deleted);
        delStatement.close();
        commit();
        println("T1: commit");
        
        Statement s = createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, 
                                          ResultSet.CONCUR_UPDATABLE);
        ResultSet rs = s.executeQuery("select * from t1");
        rs.next();
        int firstKey = rs.getInt(1);
        println("T2: Read next Tuple:(" + rs.getInt(1) + "," +
                rs.getInt(2) + "," +
                rs.getInt(3) + ")");
        int lastKey = firstKey;
        while (rs.next()) {
            lastKey = rs.getInt(1);
            println("T2: Read next Tuple:(" + rs.getInt(1) + "," +
                    rs.getInt(2) + "," +
                    rs.getInt(3) + ")");
        }
        
        final Connection con2 = openDefaultConnection();
        con2.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
        final PreparedStatement ps2 = con2.prepareStatement
            ("call SYSCS_UTIL.SYSCS_INPLACE_COMPRESS_TABLE(?,?,?,?,?)");
        ps2.setString(1, "APP"); // schema
        ps2.setString(2, "T1");  // table name
        ps2.setInt(3, 0); // purge
        int defragment = testDefragment ? 1 : 0;
        int truncate = testTruncate ? 1 : 0;
        ps2.setInt(4, defragment); // defragment rows
        ps2.setInt(5, truncate); // truncate end
        
        println("T3: call SYSCS_UTIL.SYSCS_INPLACE_COMPRESS_TABLE");
        println("T3: defragmenting rows");
        try { 
            ps2.executeUpdate();
            con2.commit();
            println("T3: commit");
            fail("Expected T3 to hang waiting for Table lock");
        } catch (SQLException e) {            
            println("T3: got expected exception");
            con2.rollback();            
        }
        ps2.close();
        rs.first(); // Go to first tuple
        println("T1: Read first Tuple:(" + rs.getInt(1) + "," +
                rs.getInt(2) + "," +
                rs.getInt(3) + ")");
        rs.updateInt(2, 3);
        println("T1: updateInt(2, 3);");
        rs.updateRow();        
        println("T1: updateRow()");
        rs.last(); // Go to last tuple
        println("T1: Read last Tuple:(" + rs.getInt(1) + "," +
                rs.getInt(2) + "," +
                rs.getInt(3) + ")");
        rs.updateInt(2, 3);
        println("T1: updateInt(2, 3);");
        rs.updateRow();
        println("T1: updateRow()");
        commit();
        println("T1: commit");
        rs = s.executeQuery("select * from t1");
        println("T4: select * from table");
        while (rs.next()) {
            println("T4: Read next Tuple:(" + rs.getInt(1) + "," +
                    rs.getInt(2) + "," +
                    rs.getInt(3) + ")");
        }
        con2.close();
        s.close();
    }
    
    // By providing a static suite(), you can customize which tests to run.
    // The default is to run all tests in the TestCase subclass.
    
    /**
     * Run in embedded and client.
     */
    public static Test suite()
    {
        final TestSuite suite = new TestSuite("ConcurrencyTest");
        suite.addTest(baseSuite("ConcurrencyTest:embedded", true));
        
        suite.addTest(
                TestConfiguration.clientServerDecorator(
                        baseSuite("ConcurrencyTest:client", false)));
        
        // Since this test relies on lock waiting, setting this property will
        // make it go a lot faster:
        return DatabasePropertyTestSetup.setLockTimeouts(suite, -1, 4);
    }
    
    private static Test baseSuite(String name, boolean embedded) {
        final TestSuite suite = new TestSuite(name);
        
        // This testcase does not require JDBC3/JSR169, since it does not
        // specify result set concurrency) in Connection.createStatement().
        suite.addTest(new ConcurrencyTest("testSharedLocks2"));
        
        // The following testcases requires JDBC3/JSR169:
        if ((JDBC.vmSupportsJDBC3() || JDBC.vmSupportsJSR169())) {
            
            // The following testcases do not use updatable result sets:
            suite.addTest(new ConcurrencyTest("testUpdateLockDownGrade1"));
            suite.addTest(new ConcurrencyTest("testAquireUpdateLock1"));
            suite.addTest(new ConcurrencyTest("testSharedLocks1"));
            suite.addTest(new ConcurrencyTest("testSharedAndUpdateLocks1"));
            suite.addTest(new ConcurrencyTest("testSharedAndUpdateLocks2"));
            
            // The following testcases do use updatable result sets.            
            if (!usingDerbyNet()) { // DB2 client does not support UR with Derby
                suite.addTest(new ConcurrencyTest ("testUpdatePurgedTuple2"));
                suite.addTest(new ConcurrencyTest("testUpdatePurgedTuple3"));
                suite.addTest(new ConcurrencyTest("testUpdatePurgedTuple4"));
                suite.addTest(new ConcurrencyTest("testUpdateModifiedTuple1"));
                suite.addTest(new ConcurrencyTest("testUpdateModifiedTuple2"));
                suite.addTest(new ConcurrencyTest("testTableIntentLock1"));
                suite.addTest
                    (new ConcurrencyTest("testUpdateLockInReadUncommitted"));
                suite.addTest(new ConcurrencyTest("testDefragmentDuringScan"));
                suite.addTest(new ConcurrencyTest("testTruncateDuringScan"));
                
                // This testcase fails in DerbyNetClient framework due to 
                // DERBY-1696
                if (embedded) {
                    suite.addTest
                        (new ConcurrencyTest("testUpdatePurgedTuple1"));
                }
                
            }         
        }
        
        return suite;
    }
    
}
