/**
 *
 * Derby - Class BLOBTest
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
import org.apache.derbyTesting.functionTests.util.BaseJDBCTestCase;
import org.apache.derbyTesting.functionTests.util.TestInputStream;
import junit.framework.Test;
import junit.framework.TestSuite;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.SQLException;
import java.io.IOException;
import java.io.InputStream;

/**
 * Tests reading and updating binary large objects (BLOBs).
 * @author Andreas Korneliussen
 */
final public class BLOBTest extends BaseJDBCTestCase
{
    /** 
     * Constructor
     * @param name name of test case (method).
     */
    public BLOBTest(String name) 
    {
        super(name);
        con = null;
    }

    
    /**
     * Tests updating a Blob from a scollable resultset, using
     * result set update methods.
     * @exception SQLException causes test to fail with error
     * @exception IOException causes test to fail with error
     */
    public void testUpdateBlobFromScrollableResultSetUsingResultSetMethods()
        throws SQLException, IOException
    {
        final Statement stmt = 
            con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                                ResultSet.CONCUR_UPDATABLE);
        final ResultSet rs = 
            stmt.executeQuery("SELECT * from " + 
                              BLOBDataModelSetup.getBlobTableName());
        println("Last");
        rs.last();
        
        final int newVal = rs.getInt(1) + 11;
        final int newSize = rs.getInt(2) / 2;
        testUpdateBlobWithResultSetMethods(rs, newVal, newSize);
        
        println("Verify updated blob using result set");
        verifyBlob(newVal, newSize, rs.getBlob(3));
        
        rs.close();
    }

    /**
     * Tests updating a Blob from a forward only resultset, using
     * result set update methods.
     * @exception SQLException causes test to fail with error
     * @exception IOException causes test to fail with error
     */
    public void testUpdateBlobFromForwardOnlyResultSetUsingResultSetMethods()
        throws SQLException, IOException
    {
        final Statement stmt = 
            con.createStatement(ResultSet.TYPE_FORWARD_ONLY,
                                ResultSet.CONCUR_UPDATABLE);
        final ResultSet rs = 
            stmt.executeQuery("SELECT * from " + 
                              BLOBDataModelSetup.getBlobTableName());
        
        while (rs.next()) {
            println("Next");
            final int val = rs.getInt(1);
            if (val == BLOBDataModelSetup.bigVal) break;
        }
        
        final int newVal = rs.getInt(1) + 11;
        final int newSize = rs.getInt(2) / 2;
        testUpdateBlobWithResultSetMethods(rs, newVal, newSize);
        
        rs.close();
    }

    /**
     * Tests updating a Blob from a scollable resultset, using
     * positioned updates.
     * @exception SQLException causes test to fail with error
     * @exception IOException causes test to fail with error
     */
    public void testUpdateBlobFromScrollableResultSetUsingPositionedUpdates()
        throws SQLException, IOException
    {
        final Statement stmt = 
            con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                                ResultSet.CONCUR_UPDATABLE);
        final ResultSet rs = 
            stmt.executeQuery("SELECT * from " + 
                              BLOBDataModelSetup.getBlobTableName());
        println("Last");
        rs.last();
        
        final int newVal = rs.getInt(1) + 11;
        final int newSize = rs.getInt(2) / 2;
        testUpdateBlobWithPositionedUpdate(rs, newVal, newSize);

        rs.relative(0); // Necessary after a positioned update
        
        println("Verify updated blob using result set");
        verifyBlob(newVal, newSize, rs.getBlob(3));
        
        rs.close();
    }

    /**
     * Tests updating a Blob from a forward only resultset, using
     * methods.
     * @exception SQLException causes test to fail with error
     * @exception IOException causes test to fail with error
     */
    public void testUpdateBlobFromForwardOnlyResultSetUsingPositionedUpdates()
        throws SQLException, IOException
    {
        final Statement stmt = 
            con.createStatement(ResultSet.TYPE_FORWARD_ONLY,
                                ResultSet.CONCUR_UPDATABLE);
        final ResultSet rs = 
            stmt.executeQuery("SELECT * from " + 
                              BLOBDataModelSetup.getBlobTableName());
        while (rs.next()) {
            println("Next");
            final int val = rs.getInt(1);
            if (val == BLOBDataModelSetup.bigVal) break;
        }
        
        final int newVal = rs.getInt(1) + 11;
        final int newSize = rs.getInt(2) / 2;
        testUpdateBlobWithPositionedUpdate(rs, newVal, newSize);
        
        rs.close();
    }

    /**
     * Tests updating a Blob from a scollable resultset produced by a
     * select query with projection. Updates are made using
     * result set update methods.
     * @exception SQLException causes test to fail with error
     * @exception IOException causes test to fail with error
     */
    public void testUpdateBlobFromScrollableResultSetWithProjectUsingResultSetMethods()
        throws SQLException, IOException
    {
        final Statement stmt = 
            con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                                ResultSet.CONCUR_UPDATABLE);
        final ResultSet rs = 
            stmt.executeQuery("SELECT data,val,length from " + 
                              BLOBDataModelSetup.getBlobTableName());
        println("Last");
        rs.last();
        
        final int newVal = rs.getInt(2) + 11;
        final int newSize = rs.getInt(3) / 2;
        testUpdateBlobWithResultSetMethods(rs, newVal, newSize);
        
        println("Verify updated blob using result set");
        verifyBlob(newVal, newSize, rs.getBlob(1));
        
        rs.close();
    }

    /**
     * Tests updating a Blob from a forward only resultset, produced by 
     * a select query with projection. Updates are made using
     * result set update methods.
     * @exception SQLException causes test to fail with error
     * @exception IOException causes test to fail with error
     */
    public void testUpdateBlobFromForwardOnlyResultSetWithProjectUsingResultSetMethods()
        throws SQLException, IOException
    {
        final Statement stmt = 
            con.createStatement(ResultSet.TYPE_FORWARD_ONLY,
                                ResultSet.CONCUR_UPDATABLE);
        final ResultSet rs = 
            stmt.executeQuery("SELECT data,val,length from " + 
                              BLOBDataModelSetup.getBlobTableName());
        
        while (rs.next()) {
            println("Next");
            final int val = rs.getInt("VAL");
            if (val == BLOBDataModelSetup.bigVal) break;
        }
        
        final int newVal = rs.getInt("VAL") + 11;
        final int newSize = BLOBDataModelSetup.bigSize / 2;
        testUpdateBlobWithResultSetMethods(rs, newVal, newSize);
        
        rs.close();
    }

    /**
     * Tests updating a Blob from a scollable resultset, produced by 
     * a select query with projection. Updates are made using
     * positioned updates
     * @exception SQLException causes test to fail with error
     * @exception IOException causes test to fail with error
     */
    public void testUpdateBlobFromScrollableResultSetWithProjectUsingPositionedUpdates()
        throws SQLException, IOException
    {
        final Statement stmt = 
            con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                                ResultSet.CONCUR_UPDATABLE);
        final ResultSet rs = 
            stmt.executeQuery("SELECT data from " + 
                              BLOBDataModelSetup.getBlobTableName() + 
                              " WHERE val= " + BLOBDataModelSetup.bigVal);
        println("Last");
        rs.last();
        
        final int newVal = BLOBDataModelSetup.bigVal * 2;
        final int newSize = BLOBDataModelSetup.bigSize / 2;
        testUpdateBlobWithPositionedUpdate(rs, newVal, newSize);

        rs.relative(0); // Necessary after a positioned update
        
        println("Verify updated blob using result set");
        verifyBlob(newVal, newSize, rs.getBlob("DATA"));
        
        rs.close();
    }

    /**
     * Tests updating a Blob from a forward only resultset, produced by 
     * a select query with projection. Updates are made using
     * positioned updates.
     * @exception SQLException causes test to fail with error
     * @exception IOException causes test to fail with error
     */
    public void testUpdateBlobFromForwardOnlyResultSetWithProjectUsingPositionedUpdates()
        throws SQLException, IOException
    {
        final Statement stmt = 
            con.createStatement(ResultSet.TYPE_FORWARD_ONLY,
                                ResultSet.CONCUR_UPDATABLE);
        final ResultSet rs = 
            stmt.executeQuery("SELECT data from " + 
                              BLOBDataModelSetup.getBlobTableName() + 
                              " WHERE val = " + BLOBDataModelSetup.bigVal);
        rs.next();
        
        final int newVal =  BLOBDataModelSetup.bigVal * 2;
        final int newSize = BLOBDataModelSetup.bigSize / 2;
        testUpdateBlobWithPositionedUpdate(rs, newVal, newSize);
        
        rs.close();
    }
    
    
    /**
     * Tests updating the Blob using result set update methods.
     * @param rs result set, currently positioned on row to be updated
     * @param newVal new value in val column and blob data
     * @param newSize new size of Blob
     * @exception SQLException causes test to fail with error
     * @exception IOException causes test to fail with error
     */
    private void testUpdateBlobWithResultSetMethods(final ResultSet rs,
                                                    final int newVal,
                                                    final int newSize) 
        throws SQLException, IOException
    {
        int val = rs.getInt("VAL");
        int size = rs.getInt("LENGTH");
        println("VerifyBlob");
        verifyBlob(val, size, rs.getBlob("DATA"));
        
        println("UpdateBlob");
        final TestInputStream newStream = new TestInputStream(newSize, newVal);
        
        rs.updateInt("VAL", newVal);
        rs.updateInt("LENGTH", newSize);
        rs.updateBinaryStream("DATA", newStream, newSize);
        rs.updateRow();
        
        println("Verify updated blob with another query");
        verifyNewValueInTable(newVal, newSize);
    }

    /**
     * Tests updating the Blob using positioned updates
     * @param rs result set, currently positioned on row to be updated
     * @param newVal new value in val column and blob data
     * @param newSize new size of Blob
     * @exception SQLException causes test to fail with error
     * @exception IOException causes test to fail with error
     */
    private void testUpdateBlobWithPositionedUpdate(final ResultSet rs,
                                                    final int newVal,
                                                    final int newSize) 
        throws SQLException, IOException
    {
        final PreparedStatement preparedStatement = con.prepareStatement
            ("UPDATE " + BLOBDataModelSetup.getBlobTableName() +
             " SET val=?, length = ?, data = ? WHERE CURRENT OF " +
             rs.getCursorName());
        
        println("UpdateBlob");
        
        final TestInputStream newStream = new TestInputStream(newSize, newVal);
        
        preparedStatement.setInt(1, newVal);
        preparedStatement.setInt(2, newSize);
        preparedStatement.setBinaryStream(3, newStream, newSize);
        preparedStatement.executeUpdate();
        
        println("Verify updated blob with another query");
        verifyNewValueInTable(newVal, newSize);
    }
    
    
    /**
     * Verifies that the table has row with column val=newVal
     * and that it its data and size columns are consistent.
     * @param newVal value expected to be found in the val column of a row
     * @param newSize expected size of size column and size of blob
     * @exception SQLException causes test to fail with error
     * @exception IOException causes test to fail with error
     */
    private void verifyNewValueInTable(final int newVal,
                                       final int newSize)
        throws IOException, SQLException
    {
        println("Verify new value in table: " + newVal);
        
        final Statement stmt = con.createStatement(ResultSet.TYPE_FORWARD_ONLY, 
                                                   ResultSet.CONCUR_READ_ONLY);
        
        final ResultSet rs = 
            stmt.executeQuery("SELECT * FROM " +  
                              BLOBDataModelSetup.getBlobTableName() +
                              " WHERE val = " + newVal);
        
        println("Query executed, calling next");
        
        boolean foundVal = false;
        
        while (rs.next()) {
            println("Next called, verifying row");
            
            assertEquals("Unexpected value in val column", 
                         newVal, rs.getInt(1));
            
            verifyBlob(newVal, newSize, rs.getBlob(3));
            foundVal = true;
        }
        assertTrue("No column with value= " + newVal + " found ", foundVal);
    }
                          
    /**
     * Verifies that the blob is consistent
     * @param expectedVal the InputStream for the Blob should return this value
     *                    for every byte
     * @param expecteSize the Blob should have this size
     * @exception SQLException causes test to fail with error
     * @exception IOException causes test to fail with error
     */
    private void verifyBlob(final int expectedVal, 
                            final int expectedSize, 
                            final Blob blob) 
        throws IOException, SQLException
    {
        final InputStream stream = blob.getBinaryStream();
        int blobSize = 0;
        for (int val = stream.read(); val!=-1; val = stream.read()) {
            blobSize++;
            
            // avoid doing a string-concat for every byte in blob
            if (expectedVal!=val) {
                assertEquals("Unexpected value in stream at position " + 
                             blobSize,
                             expectedVal, val);
            }
        }
        stream.close();
        assertEquals("Unexpected size of stream ", expectedSize, blobSize);
    }

    /**
     * The suite decorates the tests of this class with 
     * a setup which creates and populates the data model.
     */
    public static Test suite() 
    {
        TestSuite mainSuite = new TestSuite(BLOBTest.class);
        return new BLOBDataModelSetup(mainSuite);
    }

    /**
     * The setup creates a Connection to the database.
     * @exception Exception any exception will cause test to fail with error.
     */
    public final void setUp() 
        throws Exception
    {
        println("Setup of: " + getName());
        con = getConnection();
        con.setAutoCommit(false);
    }
    
    /**
     * Teardown test.
     * Rollback connection and close it.
     * @exception Exceptions causes the test to fail with error
     */
    public final void tearDown() 
        throws Exception
    {
        println("Teardown of: " + getName());
        try { 
            con.rollback();
            con.close();
        } catch (SQLException e) {
            printStackTrace(e);
        }      
    }
    
    /** JDBC Connection */        
    private Connection con;
}
