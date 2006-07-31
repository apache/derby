/*
 
   Derby - Class org.apache.derbyTesting.functionTests.tests.jdbc4.PreparedStatementTest
 
   Copyright 2006 The Apache Software Foundation or its licensors, as applicable.
 
   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at
 
      http://www.apache.org/licenses/LICENSE-2.0
 
   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 
 */

package org.apache.derbyTesting.functionTests.tests.jdbc4;

import junit.framework.*;

import org.apache.derbyTesting.functionTests.util.BaseJDBCTestCase;

import java.io.*;
import java.sql.*;
import javax.sql.*;

/**
 * This class is used to test JDBC4 specific methods in the PreparedStatement(s)
 * object.
 */
public class PreparedStatementTest extends BaseJDBCTestCase {

    /** Byte array passed in to the database. **/
    private static final byte[] BYTES = {
        0x65, 0x66, 0x67, 0x68, 0x69,
        0x69, 0x68, 0x67, 0x66, 0x65
    };

    /**
     * Default connection and prepared statements that are used by the tests.
     */
    //Connection object
    Connection conn      = null;
    //PreparedStatement object
    PreparedStatement ps = null;
    //Statement object
    Statement s = null;

    
    /**
     * Create a test with the given name.
     * 
     * @param name name of the test.
     */
    public PreparedStatementTest(String name) {
        super(name);
    }
    
    /**
     *
     * Obtain a "regular" connection and PreparedStatement that the tests 
     * can use.
     * 
     * @throws SQLException
     */
    public void setUp() 
        throws SQLException {
        conn = getConnection();
        //create the statement object
        s = conn.createStatement();
        //Create the PreparedStatement that will then be used as the basis 
        //throughout this test henceforth
        //This prepared statement will however NOT be used for testing
        //setClob and setBlob
        ps = conn.prepareStatement("select count(*) from sys.systables");
        
         // STEP1: create the tables
         // Structure of table
         // --------------------------
         // SNO            Clob Column
         // --------------------------

         s.execute("create table ClobTestTable (sno int, clobCol CLOB(1M))");
         s.execute("create table BlobTestTable (sno int, blobCol BLOB(1M))");
    }

    /**
     *
     * Release the resources that are used in this test
     *
     * @throws SQLException
     *
     */
    public void tearDown() 
        throws SQLException {
        
        s.execute("drop table ClobTestTable");
        s.execute("drop table BlobTestTable");
        s.close();
        
        if (conn != null && !conn.isClosed()) {
            conn.rollback();
            conn.close();
        }
        conn = null;
    }

    public static Test suite() {
        TestSuite suite = new TestSuite();
        suite.addTestSuite(PreparedStatementTest.class);
        suite.addTest(SetObjectUnsupportedTest.suite(false));
        return suite;
    }
    
    //--------------------------------------------------------------------------
    //BEGIN THE TEST OF THE METHODS THAT THROW AN UNIMPLEMENTED EXCEPTION IN
    //THIS CLASS
    
    /**
     * Tests the setRowId method of the PreparedStatement interface
     *
     * @throws SQLException upon any failure that occurs in the 
     *         call to the method.
     */
    public void testSetRowId() throws SQLException{
        try {
            RowId rowid = null;
            ps.setRowId(0,rowid);
            fail("setRowId should not be implemented");
        }
        catch(SQLFeatureNotSupportedException sqlfne) {
            //Do Nothing, This happens as expected
        }
    }
    
    /**
     * Tests the setNString method of the PreparedStatement interface
     *
     * @throws SQLException upon any failure that occurs in the 
     *         call to the method.
     */
    public void testSetNString() throws SQLException{
        try {
            String str = null;
            ps.setNString(0,str);
            fail("setNString should not be implemented");
        }
        catch(SQLFeatureNotSupportedException sqlfne) {
            //Do Nothing, This happens as expected
        }
    }
    
    /**
     * Tests the setNCharacterStream method of the PreparedStatement interface
     *
     * @throws SQLException upon any failure that occurs in the 
     *         call to the method.
     */
    public void testSetNCharacterStream() throws SQLException{
        try {
            Reader r  = null;
            ps.setNCharacterStream(0,r,0);
            fail("setNCharacterStream should not be implemented");
        }
        catch(SQLFeatureNotSupportedException sqlfne) {
            //Do Nothing, This happens as expected
        }
    }
    
    public void testSetNCharacterStreamLengthlessNotImplemented()
            throws SQLException {
        try {
            ps.setNCharacterStream(1, new StringReader("A string"));
            fail("setNCharacterStream(int,Reader) should not be implemented");
        } catch (SQLFeatureNotSupportedException sfnse) {
            // Do nothing, this is expected behavior.
        }
    }

    public void testSetNClobLengthlessNotImplemented()
            throws SQLException {
        try {
            ps.setNClob(1, new StringReader("A string"));
            fail("setNClob(int,Reader) should not be implemented");
        } catch (SQLFeatureNotSupportedException sfnse) {
            // Do nothing, this is expected behaviour.
        }
    }

    /**
     * Tests the setNClob method of the PreparedStatement interface
     *
     * @throws SQLException upon any failure that occurs in the 
     *         call to the method.
     */
    public void testSetNClob1() throws SQLException{
        try {
            NClob nclob = null;
            ps.setNClob(0,nclob);
            fail("setNClob should not be implemented");
        }
        catch(SQLFeatureNotSupportedException sqlfne) {
            //Do Nothing, This happens as expected
        }
    }
    
    /**
     * Tests the setNClob method of the PreparedStatement interface
     *
     * @throws SQLException upon any failure that occurs in the 
     *         call to the method.
     */
    public void testSetNClob2() throws SQLException{
        try {
            Reader reader = null;
            ps.setNClob(0,reader,0);
            fail("setNClob should not be implemented");
        }
        catch(SQLFeatureNotSupportedException sqlfne) {
            //Do Nothing, This happens as expected
        }
    }
    
    /**
     * Tests the setSQLXML method of the PreparedStatement interface
     *
     * @throws SQLException upon any failure that occurs in the 
     *         call to the method.
     */
    public void testSetSQLXML() throws SQLException{
        try {
            SQLXML sqlxml = null;
            ps.setSQLXML(0,sqlxml);
            fail("setNClob should not be implemented");
        }
        catch(SQLFeatureNotSupportedException sqlfne) {
            //Do Nothing, This happens as expected
        }
    }
    
    //--------------------------------------------------------------------------
    //Now test the methods that are implemented in the PreparedStatement 
    //interface

    public void testIsWrapperForStatement() throws SQLException {
        assertTrue(ps.isWrapperFor(Statement.class));
    }

    public void testIsWrapperForPreparedStatement() throws SQLException {
        assertTrue(ps.isWrapperFor(PreparedStatement.class));
    }

    public void testIsNotWrapperForCallableStatement() throws SQLException {
        assertFalse(ps.isWrapperFor(CallableStatement.class));
    }

    public void testIsNotWrapperForResultSet() throws SQLException {
        assertFalse(ps.isWrapperFor(ResultSet.class));
    }

    public void testUnwrapStatement() throws SQLException {
        Statement stmt = ps.unwrap(Statement.class);
        assertSame("Unwrap returned wrong object.", ps, stmt);
    }

    public void testUnwrapPreparedStatement() throws SQLException {
        PreparedStatement ps2 = ps.unwrap(PreparedStatement.class);
        assertSame("Unwrap returned wrong object.", ps, ps2);
    }

    public void testUnwrapCallableStatement() {
        try {
            CallableStatement cs = ps.unwrap(CallableStatement.class);
            fail("Unwrap didn't fail.");
        } catch (SQLException e) {
            assertSQLState("XJ128", e);
        }
    }

    public void testUnwrapResultSet() {
        try {
            ResultSet rs = ps.unwrap(ResultSet.class);
            fail("Unwrap didn't fail.");
        } catch (SQLException e) {
            assertSQLState("XJ128", e);
        }
    }

    //-----------------------------------------------------------------------
    // Begin test for setClob and setBlob
    
    /*
       we need a table in which a Clob or a Blob can be stored. We basically
       need to write tests for the setClob and the setBlob methods. 
       Proper process would be
       a) Do a createClob or createBlob
       b) Populate data in the LOB
       c) Store in Database

       But the createClob and createBlob implementations are not 
       available on the EmbeddedServer. So instead the workaround adopted
       is 

       a) store a Clob or Blob in Database. 
       b) Retrieve it from the database.
       c) store it back using setClob or setBlob

     */

    /**
     *
     * Test the setClob() method
     *
     * @throws SQLException if a failure occurs during the call to setClob
     *
     */
    public void testSetClob()
            throws IOException, SQLException {
        //insert default values into the table
        
        String str = "Test data for the Clob object";
        StringReader is = new StringReader("Test data for the Clob object");
        is.reset();
        
        PreparedStatement ps_sc = conn.prepareStatement("insert into ClobTestTable values(?,?)");
        
        //initially insert the data
        ps_sc.setInt(1,1);
        ps_sc.setClob(2,is,str.length());
        ps_sc.executeUpdate();
        
        //Now query to retrieve the Clob
        ResultSet rs = s.executeQuery("select * from ClobTestTable where sno = 1");
        rs.next();
        Clob clobToBeInserted = rs.getClob(2);
        rs.close();
        
        //Now use the setClob method
        ps_sc.setInt(1,2);
        ps_sc.setClob(2,clobToBeInserted);
        ps_sc.execute();
        
        ps_sc.close();
        
        //Now test to see that the Clob has been stored correctly
        rs = s.executeQuery("select * from ClobTestTable where sno = 2");
        rs.next();
        Clob clobRetrieved = rs.getClob(2);
        
        assertEquals(clobToBeInserted,clobRetrieved);
    }

    /**
     * Insert <code>Clob</code> without specifying length and read it back
     * for verification.
     *
     * Beacuse we don't yet support <code>Connection.createClob</code> in the
     * client driver, we must first insert data into the database and read back
     * a <code>Clob</code> object. This object is then inserted into the
     * database again.
     */
    public void testSetClobLengthless()
            throws IOException, SQLException {
        // Insert test data.
        String testString = "Test string for setCharacterStream\u1A00";
        Reader reader = new StringReader(testString);
        PreparedStatement psChar = conn.prepareStatement(
                "insert into ClobTestTable values (?,?)");
        psChar.setInt(1, 1);
        psChar.setCharacterStream(2, reader);
        psChar.execute();
        reader.close();
        // Must fetch Clob from database because we don't support
        // Connection.createClob on the client yet.
        ResultSet rs = s.executeQuery(
                "select clobCol from ClobTestTable where sno = 1");
        assertTrue("No results retrieved", rs.next());
        Clob insertClob = rs.getClob(1);
        psChar.setInt(1, 2);
        psChar.setClob(2, insertClob);
        psChar.execute();

        // Read back test data from database.
        rs = s.executeQuery(
                "select clobCol from ClobTestTable where sno = 2");
        assertTrue("No results retrieved", rs.next());
        Clob clobRetrieved = rs.getClob(1);

        // Verify test data.
        assertEquals(insertClob, clobRetrieved);
    }

    /**
     *
     * Test the setBlob() method
     *
     * @throws SQLException if a failure occurs during the call to setBlob
     *
     */
    public void testSetBlob()
            throws IOException, SQLException {
        //insert default values into the table
        
        InputStream is = new java.io.ByteArrayInputStream(BYTES);
        is.reset();
        
        PreparedStatement ps_sb = conn.prepareStatement("insert into BlobTestTable values(?,?)");
        
        //initially insert the data
        ps_sb.setInt(1,1);
        ps_sb.setBlob(2,is,BYTES.length);
        ps_sb.executeUpdate();
        
        //Now query to retrieve the Blob
        ResultSet rs = s.executeQuery("select * from BlobTestTable where sno = 1");
        rs.next();
        Blob blobToBeInserted = rs.getBlob(2);
        rs.close();
        
        //Now use the setBlob method
        ps_sb.setInt(1,2);
        ps_sb.setBlob(2,blobToBeInserted);
        ps_sb.execute();
        
        ps_sb.close();
        
        //Now test to see that the Blob has been stored correctly
        rs = s.executeQuery("select * from BlobTestTable where sno = 2");
        rs.next();
        Blob blobRetrieved = rs.getBlob(2);
        
        assertEquals(blobToBeInserted, blobRetrieved);
    }
    
    /**
     * Insert <code>Blob</code> without specifying length and read it back
     * for verification.
     *
     * Beacuse we don't yet support <code>Connection.createBlob</code> in the
     * client driver, we must first insert data into the database and read back
     * a <code>Blob</code> object. This object is then inserted into the
     * database again.
     */
    public void testSetBlobLengthless()
            throws IOException, SQLException {
        // Insert test data.
        InputStream is = new ByteArrayInputStream(BYTES);
        PreparedStatement psByte = conn.prepareStatement(
                "insert into BlobTestTable values (?,?)");
        psByte.setInt(1, 1);
        psByte.setBinaryStream(2, is);
        psByte.execute();
        is.close();
        // Must fetch Blob from database because we don't support
        // Connection.createBlob on the client yet.
        ResultSet rs = s.executeQuery(
                "select blobCol from BlobTestTable where sno = 1");
        assertTrue("No results retrieved", rs.next());
        Blob insertBlob = rs.getBlob(1);
        psByte.setInt(1, 2);
        psByte.setBlob(2, insertBlob);
        psByte.execute();

        // Read back test data from database.
        rs = s.executeQuery(
                "select blobCol from BlobTestTable where sno = 2");
        assertTrue("No results retrieved", rs.next());
        Blob blobRetrieved = rs.getBlob(1);

        // Verify test data.
        assertEquals(insertBlob, blobRetrieved);
    }

    //-------------------------------------------------
    //Test the methods used to test poolable statements
    
    /**
     *
     * Tests the PreparedStatement interface method setPoolable
     *
     * @throws SQLException
     */
    
    public void testSetPoolable() throws SQLException {
        try {
            // Set the poolable statement hint to false
            ps.setPoolable(false);
            if (ps.isPoolable())
                fail("Expected a non-poolable statement");
            // Set the poolable statement hint to true
            ps.setPoolable(true);
            if (!ps.isPoolable())
                fail("Expected a poolable statement");
        } catch(SQLException sqle) {
            // Check which SQLException state we've got and if it is
            // expected, do not print a stackTrace
            // Embedded uses XJ012, client uses XCL31.
            if (sqle.getSQLState().equals("XJ012") ||
                sqle.getSQLState().equals("XCL31")) {
                // All is good and is expected
            } else {
                fail("Unexpected SQLException " + sqle);
            }
        } catch(Exception e) {
            fail("Unexpected exception thrown in method " + e);
        }
    }
    
    /**
     *
     * Tests the PreparedStatement interface method isPoolable
     *
     * @throws SQLException
     *
     */
    
    public void testIsPoolable() throws SQLException {
        try {
            // By default a prepared statement is poolable
            if (!ps.isPoolable())
                fail("Expected a poolable statement");
        } catch(SQLException sqle) {
            // Check which SQLException state we've got and if it is
            // expected, do not print a stackTrace
            // Embedded uses XJ012, client uses XCL31.
            if (sqle.getSQLState().equals("XJ012") ||
                sqle.getSQLState().equals("XCL31")) {
                // All is good and is expected
            } else {
                fail("Unexpected SQLException " + sqle);
            }
        } catch(Exception e) {
            fail("Unexpected exception thrown in method " + e);
        }
    }
    
    
    /**
     *
     * Tests the PreparedStatement interface method setCharacterStream
     *
     * @throws SQLException
     *
     */
    public void testSetCharacterStream() throws Exception {
        String str = "Test data for the Clob object";
        StringReader is = new StringReader("Test data for the Clob object");
        
        is.reset();
        
        PreparedStatement ps_sc = conn.prepareStatement("insert into ClobTestTable values(?,?)");
        
        //initially insert the data
        ps_sc.setInt(1,1);
        ps_sc.setCharacterStream(2,is,str.length());
        ps_sc.executeUpdate();
        
        //Now query to retrieve the Clob
        ResultSet rs = s.executeQuery("select * from ClobTestTable where sno = 1");
        rs.next();
        Clob clobRetrieved = rs.getClob(2);
        rs.close();
        
        String str_out = clobRetrieved.getSubString(1L,(int)clobRetrieved.length());
        
        assertEquals("Error in inserting data into the Clob object",str,str_out);
        ps_sc.close();
    }

    public void testSetCharacterStreamLengthless()
            throws IOException, SQLException {
        // Insert test data.
        String testString = "Test string for setCharacterStream\u1A00";
        Reader reader = new StringReader(testString);
        PreparedStatement psChar = conn.prepareStatement(
                "insert into ClobTestTable values (?,?)");
        psChar.setInt(1, 1);
        psChar.setCharacterStream(2, reader);
        psChar.execute();
        reader.close();

        // Read back test data from database.
        ResultSet rs = s.executeQuery(
                "select clobCol from ClobTestTable where sno = 1");
        assertTrue("No results retrieved", rs.next());
        Clob clobRetrieved = rs.getClob(1);

        // Verify test data.
        assertEquals("Mismatch test data in/out", testString,
                     clobRetrieved.getSubString(1, testString.length()));
    }

     /**
      *
      * Tests the PreparedStatement interface method setAsciiStream
      *
      * @throws SQLException
      *
      */
    
    public void testSetAsciiStream() throws Exception {
        //insert default values into the table
        
        byte [] bytes1 = new byte[10];
        
        InputStream is = new java.io.ByteArrayInputStream(BYTES);
        
        is.reset();
        
        PreparedStatement ps_sb = conn.prepareStatement("insert into ClobTestTable values(?,?)");
        
        //initially insert the data
        ps_sb.setInt(1,1);
        ps_sb.setAsciiStream(2,is,BYTES.length);
        ps_sb.executeUpdate();
        
        //Now query to retrieve the Clob
        ResultSet rs = s.executeQuery("select * from ClobTestTable where sno = 1");
        rs.next();
        Clob ClobRetrieved = rs.getClob(2);
        rs.close();
        
        try {
            InputStream is_ret = ClobRetrieved.getAsciiStream();
            is_ret.read(bytes1);
        } catch(IOException ioe) {
            fail("IOException while reading the Clob from the database");
        }
        for(int i=0;i<BYTES.length;i++) {
            assertEquals("Error in inserting data into the Clob",BYTES[i],bytes1[i]);
        }
        ps_sb.close();
    }

    public void testSetAsciiStreamLengthless()
            throws IOException, SQLException {
        // Insert test data.
        InputStream is = new ByteArrayInputStream(BYTES);
        PreparedStatement psAscii = conn.prepareStatement(
                "insert into ClobTestTable values (?,?)");
        psAscii.setInt(1, 1);
        psAscii.setAsciiStream(2, is);
        psAscii.execute();
        is.close();

        // Read back test data from database.
        ResultSet rs = s.executeQuery(
                "select clobCol from ClobTestTable where sno = 1");
        assertTrue("No results retrieved", rs.next());
        Clob clobRetrieved = rs.getClob(1);

        // Verify read back data.
        byte[] dbBytes = new byte[10];
        InputStream isRetrieved = clobRetrieved.getAsciiStream();
        assertEquals("Unexpected number of bytes read", BYTES.length,
                isRetrieved.read(dbBytes));
        assertEquals("Stream should be exhausted", -1, isRetrieved.read());
        for (int i=0; i < BYTES.length; i++) {
            assertEquals("Byte mismatch in/out", BYTES[i], dbBytes[i]);
        }

        // Cleanup
        isRetrieved.close();
        psAscii.close();
    }

    /**
     *
     * Tests the PreparedStatement interface method setBinaryStream
     *
     * @throws SQLException
     *
     */
    
    public void testSetBinaryStream() throws Exception {
        //insert default values into the table
        
        byte [] bytes1 = new byte[10];
        
        InputStream is = new java.io.ByteArrayInputStream(BYTES);
        
        is.reset();
        
        PreparedStatement ps_sb = conn.prepareStatement("insert into BlobTestTable values(?,?)");
        
        //initially insert the data
        ps_sb.setInt(1,1);
        ps_sb.setBinaryStream(2,is,BYTES.length);
        ps_sb.executeUpdate();
        
        //Now query to retrieve the Clob
        ResultSet rs = s.executeQuery("select * from BlobTestTable where sno = 1");
        rs.next();
        Blob blobRetrieved = rs.getBlob(2);
        rs.close();
        
        try {
            InputStream is_ret = blobRetrieved.getBinaryStream();
            is_ret.read(bytes1);
        } catch(IOException ioe) {
            fail("IOException while reading the Clob from the database");
        }
        
        for(int i=0;i<BYTES.length;i++) {
            assertEquals("Error in inserting data into the Blob",BYTES[i],bytes1[i]);
        }
        ps_sb.close();
    }

    public void testSetBinaryStreamLengthless()
            throws IOException, SQLException {
        // Insert test data.
        InputStream is = new ByteArrayInputStream(BYTES);
        PreparedStatement psBinary = conn.prepareStatement(
                "insert into BlobTestTable values (?,?)");
        psBinary.setInt(1, 1);
        psBinary.setBinaryStream(2, is);
        psBinary.execute();
        is.close();

        // Read back test data from database.
        ResultSet rs = s.executeQuery(
                "select blobCol from BlobTestTable where sno = 1");
        assertTrue("No results retrieved", rs.next());
        Blob blobRetrieved = rs.getBlob(1);

        // Verify read back data.
        byte[] dbBytes = new byte[10];
        InputStream isRetrieved = blobRetrieved.getBinaryStream();
        assertEquals("Unexpected number of bytes read", BYTES.length,
                isRetrieved.read(dbBytes));
        assertEquals("Stream should be exhausted", -1, isRetrieved.read());
        for (int i=0; i < BYTES.length; i++) {
            assertEquals("Byte mismatch in/out", BYTES[i], dbBytes[i]);
        }

        // Cleanup
        isRetrieved.close();
        psBinary.close();
    }
}
