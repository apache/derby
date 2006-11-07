/*
 
   Derby - Class org.apache.derbyTesting.functionTests.tests.jdbc4.PreparedStatementTest
 
   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at
 
      http://www.apache.org/licenses/LICENSE-2.0
 
   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 
 */

package org.apache.derbyTesting.functionTests.tests.jdbc4;

import junit.framework.*;

import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.BaseJDBCTestSetup;
import org.apache.derbyTesting.functionTests.util.streams.LoopingAlphabetStream;
import org.apache.derbyTesting.junit.TestConfiguration;

import java.io.*;
import java.sql.*;
import javax.sql.*;

import org.apache.derby.iapi.services.io.DerbyIOException;
import org.apache.derby.impl.jdbc.EmbedSQLException;

/**
 * This class is used to test JDBC4 specific methods in the PreparedStatement(s)
 * object.
 *
 * A number of methods and variables are in place to aid the writing of tests:
 * <ul><li>setBinaryStreamOnBlob
 *     <li>setAsciiStream
 *     <li>key - an id. One is generated each time setUp is run.
 *     <li>reqeustKey() - generate a new unique id.
 *     <li>psInsertX - prepared statements for insert.
 *     <li>psFetchX - prepared statements for fetching values.
 * </ul>
 *
 * For table creation, see the <code>suite</code>-method.
 */
public class PreparedStatementTest extends BaseJDBCTestCase {

    private static final String BLOBTBL = "BlobTestTable";
    private static final String CLOBTBL = "ClobTestTable";
    private static final String LONGVARCHAR = "LongVarcharTestTable";

    /** Key used to id data inserted into the database. */
    private static int globalKey = 1;

    /** Byte array passed in to the database. **/
    private static final byte[] BYTES = {
        0x65, 0x66, 0x67, 0x68, 0x69,
        0x69, 0x68, 0x67, 0x66, 0x65
    };

    // Default connection and prepared statements that are used by the tests.
    /** 
     * Default key to use for insertions.
     * Is unique for each fixture. More keys can be fetched by calling
     * <link>requestKey</link>.
     */
    private int key;
    /** Default connection object. */
    /** PreparedStatement object with no positional arguments. */
    private PreparedStatement ps = null;
    /** PreparedStatement to fetch BLOB with specified id. */
    private PreparedStatement psFetchBlob = null;
    /** PreparedStatement to insert a BLOB with specified id. */
    private PreparedStatement psInsertBlob = null;
    /** PreparedStatement to fetch CLOB with specified id. */
    private PreparedStatement psFetchClob = null;
    /** PreparedStatement to insert a CLOB with specified id. */
    private PreparedStatement psInsertClob = null;
    /** PreparedStatement to insert a LONG VARCHAR with specified id. */
    private PreparedStatement psInsertLongVarchar = null;
    //Statement object
    private Statement s = null;


    
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
        key = requestKey();
        //create the statement object
        s = createStatement();
        //Create the PreparedStatement that will then be used as the basis 
        //throughout this test henceforth
        //This prepared statement will however NOT be used for testing
        //setClob and setBlob
        ps = prepareStatement("select count(*) from sys.systables");
        
        // Prepare misc statements.
        psFetchBlob = prepareStatement("SELECT dBlob FROM " +
                BLOBTBL + " WHERE sno = ?");
        psInsertBlob = prepareStatement("INSERT INTO " + BLOBTBL +
                " VALUES (?, ?)");
        psFetchClob = prepareStatement("SELECT dClob FROM " +
                CLOBTBL + " WHERE sno = ?");
        psInsertClob = prepareStatement("INSERT INTO " + CLOBTBL +
                " VALUES (?, ?)");
        psInsertLongVarchar = prepareStatement("INSERT INTO " + LONGVARCHAR +
                " VALUES (?, ?)");
    }

    /**
     *
     * Release the resources that are used in this test
     *
     * @throws SQLException
     *
     */
    public void tearDown() 
        throws Exception {
        
        psFetchBlob.close();
        psFetchClob.close();
        psInsertBlob.close();
        psInsertClob.close();
        psInsertLongVarchar.close();
        
        super.tearDown();
    }

    public static Test suite() {
        TestSuite suite = new TestSuite("PreparedStatementTest suite");
        suite.addTest(baseSuite("PreparedStatementTest:embedded"));
        suite.addTest(TestConfiguration.clientServerDecorator(
            baseSuite("PreparedStatementTest:client")));
        return suite;
    }

    private static Test baseSuite(String name) {
        TestSuite suite = new TestSuite(name);
        suite.addTestSuite(PreparedStatementTest.class);
        return new BaseJDBCTestSetup(suite) {
                public void setUp()
                        throws java.lang.Exception {
                        try {
                            create();
                        } catch (SQLException sqle) {
                            if (sqle.getSQLState().equals("X0Y32")) {
                                drop();
                                create();
                            } else {
                                throw sqle;
                            }
                        }
                }

                public void tearDown()
                        throws java.lang.Exception {
                    drop();
                    super.tearDown();
                }

                private void create()
                        throws SQLException {
                    Statement stmt = getConnection().createStatement();
                    stmt.execute("create table " + BLOBTBL +
                            " (sno int, dBlob BLOB(1M))");
                    stmt.execute("create table " + CLOBTBL +
                            " (sno int, dClob CLOB(1M))");
                    stmt.execute("create table " + LONGVARCHAR  +
                            " (sno int, dLongVarchar LONG VARCHAR)");
                    stmt.close();
                }

                private void drop()
                        throws SQLException {
                    Statement stmt = getConnection().createStatement();
                    stmt.execute("drop table " + BLOBTBL);
                    stmt.execute("drop table " + CLOBTBL);
                    stmt.execute("drop table " + LONGVARCHAR);
                    stmt.close();
                }
            };
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
        
        //initially insert the data
        psInsertClob.setInt(1, key);
        psInsertClob.setClob(2, is, str.length());
        psInsertClob.executeUpdate();
        
        //Now query to retrieve the Clob
        psFetchClob.setInt(1, key);
        ResultSet rs = psFetchClob.executeQuery();
        rs.next();
        Clob clobToBeInserted = rs.getClob(1);
        rs.close();
        
        //Now use the setClob method
        int secondKey = requestKey();
        psInsertClob.setInt(1, secondKey);
        psInsertClob.setClob(2, clobToBeInserted);
        psInsertClob.execute();
        
        psInsertClob.close();
        
        //Now test to see that the Clob has been stored correctly
        psFetchClob.setInt(1, secondKey);
        rs = psFetchClob.executeQuery();
        rs.next();
        Clob clobRetrieved = rs.getClob(1);
        
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
        psInsertClob.setInt(1, key);
        psInsertClob.setCharacterStream(2, reader);
        psInsertClob.execute();
        reader.close();
        // Must fetch Clob from database because we don't support
        // Connection.createClob on the client yet.
        psFetchClob.setInt(1, key);
        ResultSet rs = psFetchClob.executeQuery();
        assertTrue("No results retrieved", rs.next());
        int secondKey = requestKey();
        Clob insertClob = rs.getClob(1);
        psInsertClob.setInt(1, secondKey);
        psInsertClob.setClob(2, insertClob);
        psInsertClob.execute();

        // Read back test data from database.
        psFetchClob.setInt(1, secondKey);
        rs = psFetchClob.executeQuery();
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
        
        //initially insert the data
        psInsertBlob.setInt(1, key);
        psInsertBlob.setBlob(2, is, BYTES.length);
        psInsertBlob.executeUpdate();
        
        //Now query to retrieve the Blob
        psFetchBlob.setInt(1, key);
        ResultSet rs = psFetchBlob.executeQuery();
        rs.next();
        Blob blobToBeInserted = rs.getBlob(1);
        rs.close();
        
        //Now use the setBlob method
        int secondKey = requestKey();
        psInsertBlob.setInt(1, secondKey);
        psInsertBlob.setBlob(2, blobToBeInserted);
        psInsertBlob.execute();
        
        psInsertBlob.close();
        
        //Now test to see that the Blob has been stored correctly
        psFetchBlob.setInt(1, secondKey);
        rs = psFetchBlob.executeQuery();
        rs.next();
        Blob blobRetrieved = rs.getBlob(1);
        
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
        psInsertBlob.setInt(1, key);
        psInsertBlob.setBinaryStream(2, is);
        psInsertBlob.execute();
        is.close();
        // Must fetch Blob from database because we don't support
        // Connection.createBlob on the client yet.
        psFetchBlob.setInt(1, key);
        ResultSet rs = psFetchBlob.executeQuery();
        assertTrue("No results retrieved", rs.next());
        Blob insertBlob = rs.getBlob(1);
        int secondKey = requestKey();
        psInsertBlob.setInt(1, secondKey);
        psInsertBlob.setBlob(2, insertBlob);
        psInsertBlob.execute();

        // Read back test data from database.
        psFetchBlob.setInt(1, secondKey);
        rs = psFetchBlob.executeQuery();
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
        
        //initially insert the data
        psInsertClob.setInt(1, key);
        psInsertClob.setCharacterStream(2, is, str.length());
        psInsertClob.executeUpdate();
        
        //Now query to retrieve the Clob
        psFetchClob.setInt(1, key);
        ResultSet rs = psFetchClob.executeQuery();
        rs.next();
        Clob clobRetrieved = rs.getClob(1);
        rs.close();
        
        String str_out = clobRetrieved.getSubString(1L,(int)clobRetrieved.length());
        
        assertEquals("Error in inserting data into the Clob object",str,str_out);
        psInsertClob.close();
    }

    public void testSetCharacterStreamLengthless()
            throws IOException, SQLException {
        // Insert test data.
        String testString = "Test string for setCharacterStream\u1A00";
        Reader reader = new StringReader(testString);
        psInsertClob.setInt(1, key);
        psInsertClob.setCharacterStream(2, reader);
        psInsertClob.execute();
        reader.close();

        // Read back test data from database.
        psFetchClob.setInt(1, key);
        ResultSet rs = psFetchClob.executeQuery();
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
        
        //initially insert the data
        psInsertClob.setInt(1, key);
        psInsertClob.setAsciiStream(2, is, BYTES.length);
        psInsertClob.executeUpdate();
        
        //Now query to retrieve the Clob
        psFetchClob.setInt(1, key);
        ResultSet rs = psFetchClob.executeQuery();
        rs.next();
        Clob ClobRetrieved = rs.getClob(1);
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
        psInsertClob.close();
    }

    public void testSetAsciiStreamLengthless()
            throws IOException, SQLException {
        // Insert test data.
        InputStream is = new ByteArrayInputStream(BYTES);
        psInsertClob.setInt(1, key);
        psInsertClob.setAsciiStream(2, is);
        psInsertClob.execute();
        is.close();

        // Read back test data from database.
        psFetchClob.setInt(1, key);
        ResultSet rs = psFetchClob.executeQuery();
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
        psInsertClob.close();
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
        
        //initially insert the data
        psInsertBlob.setInt(1, key);
        psInsertBlob.setBinaryStream(2, is, BYTES.length);
        psInsertBlob.executeUpdate();
        
        //Now query to retrieve the Clob
        psFetchBlob.setInt(1, key);
        ResultSet rs = psFetchBlob.executeQuery();
        rs.next();
        Blob blobRetrieved = rs.getBlob(1);
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
        psInsertBlob.close();
    }

    public void testSetBinaryStreamLengthless()
            throws IOException, SQLException {
        // Insert test data.
        InputStream is = new ByteArrayInputStream(BYTES);
        psInsertBlob.setInt(1, key);
        psInsertBlob.setBinaryStream(2, is);
        psInsertBlob.execute();
        is.close();

        // Read back test data from database.
        psFetchBlob.setInt(1, key);
        ResultSet rs = psFetchBlob.executeQuery();
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
        psInsertBlob.close();
    }

    public void testSetBinaryStreamLengthLess1KOnBlob()
            throws IOException, SQLException {
        int length = 1*1024;
        setBinaryStreamOnBlob(key, length, -1, 0, true);
        psFetchBlob.setInt(1, key);
        ResultSet rs = psFetchBlob.executeQuery();
        assertTrue("Empty resultset", rs.next());
        assertEquals(new LoopingAlphabetStream(length),
                     rs.getBinaryStream(1));
        assertFalse("Resultset should have been exhausted", rs.next());
        rs.close();
    }

    public void testSetBinaryStreamLengthLess32KOnBlob()
            throws IOException, SQLException {
        int length = 32*1024;
        setBinaryStreamOnBlob(key, length, -1, 0, true);
        psFetchBlob.setInt(1, key);
        ResultSet rs = psFetchBlob.executeQuery();
        assertTrue("Empty resultset", rs.next());
        assertEquals(new LoopingAlphabetStream(length),
                     rs.getBinaryStream(1));
        assertFalse("Resultset should have been exhausted", rs.next());
        rs.close();
    }

    public void testSetBinaryStreamLengthLess65KOnBlob()
            throws IOException, SQLException {
        int length = 65*1024;
        setBinaryStreamOnBlob(key, length, -1, 0, true);
        psFetchBlob.setInt(1, key);
        ResultSet rs = psFetchBlob.executeQuery();
        assertTrue("Empty resultset", rs.next());
        LoopingAlphabetStream s1 = new LoopingAlphabetStream(length);
        assertEquals(new LoopingAlphabetStream(length),
                     rs.getBinaryStream(1));
        assertFalse("Resultset should have been exhausted", rs.next());
        rs.close();
    }

    public void testSetBinaryStreamLengthLessOnBlobTooLong() {
        int length = 1*1024*1024+512;
        try {
            setBinaryStreamOnBlob(key, length, -1, 0, true);
        } catch (SQLException sqle) {
            if (usingEmbedded()) {
                assertSQLState("XSDA4", sqle);
            } else {
                assertSQLState("22001", sqle);
            }
        }
    }

    public void testExceptionPathOnePage_bs()
            throws SQLException {
        int length = 11;
        try {
            setBinaryStreamOnBlob(key, length -1, length, 0, false);
            fail("Inserted a BLOB with fewer bytes than specified");
        } catch (SQLException sqle) {
            if (usingEmbedded()) {
                assertSQLState("XSDA4", sqle);
            } else {
                assertSQLState("XN017", sqle);
            }
        }
    }

    public void testExceptionPathMultiplePages_bs()
            throws SQLException {
        int length = 1*1024*1024;
        try {
            setBinaryStreamOnBlob(key, length -1, length, 0, false);
            fail("Inserted a BLOB with fewer bytes than specified");
        } catch (SQLException sqle) {
            if (usingEmbedded()) {
                assertSQLState("XSDA4", sqle);
            } else {
                assertSQLState("XN017", sqle);
            }
        }
    }

    public void testBlobExceptionDoesNotRollbackOtherStatements()
            throws IOException, SQLException {
        getConnection().setAutoCommit(false);
        int[] keys = {key, requestKey(), requestKey()};
        for (int i=0; i < keys.length; i++) {
            psInsertBlob.setInt(1, keys[i]);
            psInsertBlob.setNull(2, Types.BLOB);
            assertEquals(1, psInsertBlob.executeUpdate());
        }
        // Now insert a BLOB that fails because the stream is too short.
        int failedKey = requestKey();
        int length = 1*1024*1024;
        try {
            setBinaryStreamOnBlob(failedKey, length -1, length, 0, false);
            fail("Inserted a BLOB with less data than specified");
        } catch (SQLException sqle) {
            if (usingEmbedded()) {
                assertSQLState("XSDA4", sqle);
            } else {
                assertSQLState("XN017", sqle);
            }
        }
        // Now make sure the previous statements are there, and that the last
        // BLOB is not.
        ResultSet rs;
        for (int i=0; i < keys.length; i++) {
            psFetchBlob.setInt(1, keys[i]);
            rs = psFetchBlob.executeQuery();
            assertTrue(rs.next());
            assertFalse(rs.next());
            rs.close();
        }
        psFetchBlob.setInt(1, failedKey);
        rs = psFetchBlob.executeQuery();
        // When using the Derby client driver, the data seems to be padded
        // with 0s and inserted... Thus, the select returns a row.
        if (!usingEmbedded()) {
            assertTrue(rs.next());
            InputStream is = rs.getBinaryStream(1);
            int lastByte = -1;
            int b = 99; // Just a value > 0.
            while (b > -1) {
                lastByte = b;
                b = is.read();
            }
            assertEquals("Last padded byte is not 0", 0, lastByte);
        }
        assertFalse(rs.next());
        rs.close();
        rollback();
        // Make sure all data is gone after the rollback.
        for (int i=0; i < keys.length; i++) {
            psFetchBlob.setInt(1, keys[i]);
            rs = psFetchBlob.executeQuery();
            assertFalse(rs.next());
            rs.close();
        }
        // Make sure the failed insert has not "reappeared" somehow...
        psFetchBlob.setInt(1, failedKey);
        rs = psFetchBlob.executeQuery();
        assertFalse(rs.next());

    }

    public void testSetAsciiStreamLengthLess1KOnClob()
            throws IOException, SQLException {
        int length = 1*1024;
        setAsciiStream(psInsertClob, key, length, -1, 0, true);
        psFetchClob.setInt(1, key);
        ResultSet rs = psFetchClob.executeQuery();
        assertTrue("Empty resultset", rs.next());
        assertEquals(new LoopingAlphabetStream(length),
                     rs.getAsciiStream(1));
        assertFalse("Resultset should have been exhausted", rs.next());
        rs.close();
    }

    public void testSetAsciiStreamLengthLess32KOnClob()
            throws IOException, SQLException {
        int length = 32*1024;
        setAsciiStream(psInsertClob, key, length, -1, 0, true);
        psFetchClob.setInt(1, key);
        ResultSet rs = psFetchClob.executeQuery();
        assertTrue("Empty resultset", rs.next());
        assertEquals(new LoopingAlphabetStream(length),
                     rs.getAsciiStream(1));
        assertFalse("Resultset should have been exhausted", rs.next());
        rs.close();
    }

    public void testSetAsciiStreamLengthLess65KOnClob()
            throws IOException, SQLException {
        int length = 65*1024;
        setAsciiStream(psInsertClob, key, length, -1, 0, true);
        psFetchClob.setInt(1, key);
        ResultSet rs = psFetchClob.executeQuery();
        assertTrue("Empty resultset", rs.next());
        assertEquals(new LoopingAlphabetStream(length),
                     rs.getAsciiStream(1));
        assertFalse("Resultset should have been exhausted", rs.next());
        rs.close();
    }

    public void testSetAsciiStreamLengthLessOnClobTooLong() {
        int length = 1*1024*1024+512;
        try {
            setAsciiStream(psInsertClob, key, length, -1, 0, true);
        } catch (SQLException sqle) {
            if (usingEmbedded()) {
                assertSQLState("XSDA4", sqle);
            } else {
                assertSQLState("22001", sqle);
            }
        }
    }

    public void testSetAsciiStreamLengthLessOnClobTooLongTruncate()
            throws SQLException {
        int trailingBlanks = 512;
        int length = 1*1024*1024 + trailingBlanks;
        setAsciiStream(psInsertClob, key, length, -1, trailingBlanks, true);
    }

    public void testSetAsciiStreamLengthlessOnLongVarCharTooLong() {
        int length = 32700+512;
        try {
            setAsciiStream(psInsertLongVarchar, key, length, -1, 0, true);
            fail("Inserted a LONG VARCHAR that is too long");
        } catch (SQLException sqle) {
            if (usingEmbedded()) {
                assertInternalDerbyIOExceptionState("XCL30", "22001", sqle);
            } else {
                assertSQLState("22001", sqle);
            }
        }
    }

    public void testSetAsciiStreamLengthlessOnLongVarCharDontTruncate() {
        int trailingBlanks = 2000;
        int length = 32000 + trailingBlanks;
        try {
            setAsciiStream(psInsertLongVarchar, key, length, -1,
                    trailingBlanks, true);
            fail("Truncation is not allowed for LONG VARCHAR");
        } catch (SQLException sqle) {
            if (usingEmbedded()) {
                assertInternalDerbyIOExceptionState("XCL30", "22001", sqle);
            } else {
                assertSQLState("22001", sqle);
            }
        }
    }

    /************************************************************************
     *                 A U X I L I A R Y  M E T H O D S                     *
     ************************************************************************/

    /**
     * Insert data into a Blob column with setBinaryStream.
     *
     * @param id unique id for inserted row
     * @param actualLength the actual length of the stream
     * @param specifiedLength the specified length of the stream
     * @param trailingBlanks number of characters at the end that is blank
     * @param lengthLess whether to use the length less overloads or not
     */
    private void setBinaryStreamOnBlob(int id,
                                       int actualLength,
                                       int specifiedLength,
                                       int trailingBlanks,
                                       boolean lengthLess)
            throws SQLException {
        psInsertBlob.setInt(1, id);
        if (lengthLess) {
            psInsertBlob.setBinaryStream(2, new LoopingAlphabetStream(
                                                actualLength,
                                                trailingBlanks));
        } else {
            psInsertBlob.setBinaryStream(2,
                               new LoopingAlphabetStream(
                                        actualLength,
                                        trailingBlanks),
                               specifiedLength);
        }
        assertEquals("Insert with setBinaryStream failed",
                1, psInsertBlob.executeUpdate());
    }

    /**
     * Insert data into a column with setAsciiStream.
     * The prepared statement passed must have two positional parameters;
     * one int and one more. Depending on the last parameter, the execute
     * might succeed or it might fail. This is intended behavior, and should
     * be handled by the caller. For instance, calling this method on an
     * INT-column would fail, calling it on a CLOB-column would succeed.
     *
     * @param id unique id for inserted row
     * @param actualLength the actual length of the stream
     * @param specifiedLength the specified length of the stream
     * @param trailingBlanks number of characters at the end that is blank
     * @param lengthLess whether to use the length less overloads or not
     */
    private void setAsciiStream(PreparedStatement ps,
                                int id,
                                int actualLength,
                                int specifiedLength,
                                int trailingBlanks,
                                boolean lengthLess)
            throws SQLException {
        ps.setInt(1, id);
        if (lengthLess) {
            ps.setAsciiStream(2, 
                              new LoopingAlphabetStream(
                                                actualLength,
                                                trailingBlanks));
        } else {
            ps.setAsciiStream(2,
                              new LoopingAlphabetStream(
                                                actualLength,
                                                trailingBlanks),
                              specifiedLength);
        }
        assertEquals("Insert with setAsciiStream failed",
                1, ps.executeUpdate());
    }

    /**
     * Get next key to id inserted data with.
     */
    private static int requestKey() {
        return globalKey++;
    }

    /**
     * Return the last chained SQLException.
     * If there are no exceptions chained, the original one is returned.
     */
    private SQLException getLastSQLException(SQLException sqle) {
        SQLException last = sqle;
        SQLException next = sqle;
        while (next != null) {
            last = next;
            next = last.getNextException();
        }
        return last;
    }

    /**
     * This methods is not to be used, but sometimes you have to!
     *
     * @param preSQLState the expected outer SQL state
     * @param expectedInternal the expected internal SQL state
     * @param sqle the outer SQLException
     */
    private void assertInternalDerbyIOExceptionState(
                                        String preSQLState,
                                        String expectedInternal,
                                        SQLException sqle) {
        assertSQLState("Outer/public SQL state incorrect",
                       preSQLState, sqle);
        // We need to dig a little with the current way exceptions are
        // being reported. We can use getCause because we always run with
        // Mustang/Java SE 6.
        Throwable cause = getLastSQLException(sqle).getCause();
        assertTrue("Exception not an EmbedSQLException",
                   cause instanceof EmbedSQLException);
        cause = ((EmbedSQLException)cause).getJavaException();
        assertTrue("Exception not a DerbyIOException",
                   cause instanceof DerbyIOException);
        DerbyIOException dioe = (DerbyIOException)cause;
        assertEquals("Incorrect internal SQL state", expectedInternal,
                     dioe.getSQLState());
    }
}
