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
import org.apache.derbyTesting.functionTests.util.SQLStateConstants;

import java.io.*;
import java.sql.*;
import javax.sql.*;

/**
 * This class is used to test JDBC4 specific methods in the PreparedStatement(s)
 * object.
 */
public class PreparedStatementTest extends BaseJDBCTestCase {
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
    
    /**
     *
     * Tests the wrapper methods isWrapperFor and unwrap. Test
     * for the case when isWrapperFor returns true and we call unwrap
     * The test is right now being run in the embedded case only
     *
     * @throws SQLException upon any failure that occurs in the 
     *                      call to the method.
     *
     */
    public void testisWrapperReturnsTrue() throws SQLException {
        Class<PreparedStatement> wrap_class = PreparedStatement.class;
        
        //The if should return true enabling us  to call the unwrap method
        //without throwing  an exception
        if(ps.isWrapperFor(wrap_class)) {
            try {
                PreparedStatement stmt1 =
                        (PreparedStatement)ps.unwrap(wrap_class);
            }
            catch(SQLException sqle) {
                fail("Unwrap wrongly throws a SQLException");
            }
        } else {
            fail("isWrapperFor wrongly returns false");
        }
    }
    
    /**
     *
     * Tests the wrapper methods isWrapperFor and unwrap. Test
     * for the case when isWrapperFor returns false and we call unwrap
     * The test is right now being run in the embedded case only
     *
     * @throws SQLException upon any failure that occurs in the 
     *                      call to the method.
     *
     */
    public void testisWrapperReturnsFalse() throws SQLException {
        //test for the case when isWrapper returns false
        //using some class that will return false when
        //passed to isWrapperFor
        Class<ResultSet> wrap_class = ResultSet.class;
        
        //returning false is the correct behaviour in this case
        //Generate a message if it returns true
        if(ps.isWrapperFor(wrap_class)) {
            fail("isWrapperFor wrongly returns true");
        } else {
            try {
                ResultSet rs1 = (ResultSet)
                ps.unwrap(wrap_class);
                fail("unwrap does not throw the expected " +
                        "exception");
            } catch (SQLException sqle) {
                //calling unwrap in this case throws an SQLException
                //check that this SQLException has the correct SQLState
                if(!SQLStateConstants.UNABLE_TO_UNWRAP.equals(sqle.getSQLState())) {
                    throw sqle;
                }
            }
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
    public void testSetClob() throws SQLException {
        //insert default values into the table
        
        String str = "Test data for the Clob object";
        StringReader is = new StringReader("Test data for the Clob object");
        
        try {
            is.reset();
        } catch (IOException ioe) {
            fail("Failed to reset blob input stream: " + ioe.getMessage());
        }
        
        PreparedStatement ps_sc = conn.prepareStatement("insert into ClobTestTable values(?,?)");
        
        //initially insert the data
        ps_sc.setInt(1,1);
        ps_sc.setClob(2,is,str.length());
        ps_sc.executeUpdate();
        
        //Now query to retrieve the Clob
        ResultSet rs = s.executeQuery("select * from ClobTestTable where sno = 1");
        rs.next();
        Clob clobToBeInerted = rs.getClob(2);
        rs.close();
        
        //Now use the setClob method
        ps_sc.setInt(1,2);
        ps_sc.setClob(2,clobToBeInerted);
        ps_sc.execute();
        
        ps_sc.close();
        
        //Now test to see that the Clob has been stored correctly
        rs = s.executeQuery("select * from ClobTestTable where sno = 2");
        rs.next();
        Clob clobRetrieved = rs.getClob(2);
        
        if(!equalClob(clobToBeInerted,clobRetrieved)) 
            fail("Clob not inserted properly using setClob");
    }
    
    /*
     *
     * Compares the two clobs supplied to se if they are similar
     * returns true if they are similar and false otherwise.
     * 
     * @param clob1 Clob to be compared
     * @param clob1 Clob to be compared
     * @return true if they are equal
     *
     */
    boolean equalClob(Clob clob1,Clob clob2) {
        int c1,c2;
        InputStream is1=null,is2=null;
        try {
            is1 = clob1.getAsciiStream();
            is2 = clob2.getAsciiStream();
            if(clob1.length()!=clob2.length())
                return false;
        } catch(SQLException sqle){
            sqle.printStackTrace();
        }
        try {
            for(long i=0;i<clob1.length();i++) {
                c1=is1.read();
                c2=is2.read();
                if(c1!=c2)
                    return false;
            }
        } catch(IOException e) {
            e.printStackTrace();
        } catch(SQLException e) {
            e.printStackTrace();
        }
        return true;
    }
    
    /**
     *
     * Test the setBlob() method
     *
     * @throws SQLException if a failure occurs during the call to setBlob
     *
     */
    public void testSetBlob() throws SQLException {
        //insert default values into the table
        
        byte[] bytes = new byte[] {
            0x65, 0x66, 0x67, 0x68, 0x69,
            0x69, 0x68, 0x67, 0x66, 0x65
        };
        InputStream is = new java.io.ByteArrayInputStream(bytes);
        
        try {
            is.reset();
        } catch (IOException ioe) {
            fail("Failed to reset blob input stream: " + ioe.getMessage());
        }
        
        PreparedStatement ps_sb = conn.prepareStatement("insert into BlobTestTable values(?,?)");
        
        //initially insert the data
        ps_sb.setInt(1,1);
        ps_sb.setBlob(2,is,bytes.length);
        ps_sb.executeUpdate();
        
        //Now query to retrieve the Blob
        ResultSet rs = s.executeQuery("select * from BlobTestTable where sno = 1");
        rs.next();
        Blob blobToBeInerted = rs.getBlob(2);
        rs.close();
        
        //Now use the setBlob method
        ps_sb.setInt(1,2);
        ps_sb.setBlob(2,blobToBeInerted);
        ps_sb.execute();
        
        ps_sb.close();
        
        //Now test to see that the Blob has been stored correctly
        rs = s.executeQuery("select * from BlobTestTable where sno = 2");
        rs.next();
        Blob blobRetrieved = rs.getBlob(2);
        
        if(!equalBlob(blobToBeInerted,blobRetrieved)) 
            fail("Blob not inserted properly using setBlob");
    }
    
    /*
     * Compares the two blobs supplied to se if they are similar
     * returns true if they are similar and false otherwise.
     *
     * @param blob1 The first Blob that is passed as input
     * @param blob2 The second Blob that is passed as input
     *
     * @return true If the Blob values are equal
     */
    boolean equalBlob(Blob blob1,Blob blob2) {
        int c1,c2;
        InputStream is1=null,is2=null;
        try {
            is1 = blob1.getBinaryStream();
            is2 = blob2.getBinaryStream();
            if(blob1.length()!=blob2.length())
                return false;
        } catch(SQLException sqle){
            sqle.printStackTrace();
        }
        try {
            for(long i=0;i<blob1.length();i++) {
                c1=is1.read();
                c2=is2.read();
                if(c1!=c2)
                    return false;
            }
        } catch(IOException e) {
            e.printStackTrace();
        } catch(SQLException e) {
            e.printStackTrace();
        }
        return true;
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
    
     /**
      *
      * Tests the PreparedStatement interface method setAsciiStream
      *
      * @throws SQLException
      *
      */
    
    public void testSetAsciiStream() throws Exception {
        //insert default values into the table
        
        byte[] bytes = new byte[] {
            0x65, 0x66, 0x67, 0x68, 0x69,
            0x69, 0x68, 0x67, 0x66, 0x65
        };
        
        byte [] bytes1 = new byte[10];
        
        InputStream is = new java.io.ByteArrayInputStream(bytes);
        
        is.reset();
        
        PreparedStatement ps_sb = conn.prepareStatement("insert into ClobTestTable values(?,?)");
        
        //initially insert the data
        ps_sb.setInt(1,1);
        ps_sb.setAsciiStream(2,is,bytes.length);
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
        for(int i=0;i<bytes.length;i++) {
            assertEquals("Error in inserting data into the Clob",bytes[i],bytes1[i]);
        }
        ps_sb.close();
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
        
        byte[] bytes = new byte[] {
            0x65, 0x66, 0x67, 0x68, 0x69,
            0x69, 0x68, 0x67, 0x66, 0x65
        };
        
        byte [] bytes1 = new byte[10];
        
        InputStream is = new java.io.ByteArrayInputStream(bytes);
        
        is.reset();
        
        PreparedStatement ps_sb = conn.prepareStatement("insert into BlobTestTable values(?,?)");
        
        //initially insert the data
        ps_sb.setInt(1,1);
        ps_sb.setBinaryStream(2,is,bytes.length);
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
        
        for(int i=0;i<bytes.length;i++) {
            assertEquals("Error in inserting data into the Blob",bytes[i],bytes1[i]);
        }
        ps_sb.close();
    }
}
