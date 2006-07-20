/*
 
   Derby - Class CallableStatementTest
 
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

import java.io.IOException;
import java.io.Reader;
import java.sql.*;
import java.lang.reflect.Method;
import java.util.Vector;

/**
 * Tests of the <code>java.sql.CallableStatement</code> JDBC40 API.
 */
public class CallableStatementTest
    extends BaseJDBCTestCase {

    /** Default connection used by the tests. */
    private Connection con = null;
    /** Default callable statement used by the tests. */
    private CallableStatement cStmt = null;
    
    /**
     * Create a test with the given name.
     *
     * @param name name of the test.
     */
    public CallableStatementTest(String name) {
        super(name);
    }

    /**
     * Create a default callable statement and connection.
     *
     * @throws SQLException if creation of connection or callable statement
     *                      fail.
     */
    public void setUp() 
        throws SQLException {
        con = getConnection();
        cStmt = con.prepareCall("? = CALL FLOOR(?)");
        cStmt.registerOutParameter(1, Types.DOUBLE);
    }

    /**
     * Close default callable statement and connection.
     *
     * @throws SQLException if closing of the connection or the callable
     *                      statement fail.
     */
    public void tearDown()
        throws SQLException {
        if (cStmt != null && !cStmt.isClosed()) {
            cStmt.close();
        }
        if (con != null && !con.isClosed()) {
            con.rollback();
            con.close();
        }
        cStmt = null;
        con = null;
    }
   
    public void testNamedParametersAreNotSupported()
        throws SQLException {
        DatabaseMetaData met = con.getMetaData();
        assertFalse("Named parameters are not supported, but the metadata " +
                    "says they are", met.supportsNamedParameters());
        met = null;
    }
    
    public void testGetDoubleIntOnInParameter()
        throws SQLException {
        cStmt.setDouble(2, 3.3);
        cStmt.execute();
        try {
            cStmt.getDouble(2);
            fail("Calling getDouble on an IN parameter should throw " +
                 "an exception");
        } catch (SQLException sqle) {
            // SQLState differ between DerbyNetClient and embedded.
            String sqlState = usingDerbyNetClient() ? "XJ091" : "XCL26";
            assertSQLState("Unexpected SQLState", sqlState, sqle);
        }
    }
    
    public void testGetNClobIntNotImplemented()
        throws SQLException {
        try {
            cStmt.getNClob(1);
            fail("CallableStatement.getNClob(int) should not be implemented");
        } catch (SQLFeatureNotSupportedException sfnse) {
            // We are fine, do nothing.
        }
    }
    
    public void testGetNClobStringNotImplemented() 
        throws SQLException {
        try {
            cStmt.getNClob("some-parameter-name");
            fail("CallableStatement.getNClob(String) " +
                 "should not be implemented");
        } catch (SQLFeatureNotSupportedException sfnse) {
            // We are fine, do nothing.
        }
    }

    public void testGetNStringIntNotImplemented() 
        throws SQLException {
        try {
            cStmt.getNString(1);
            fail("CallableStatement.getNString(int) " +
                 "should not be implemented");
        } catch (SQLFeatureNotSupportedException sfnse) {
            // We are fine, do nothing.
        }
    }

    public void testGetNStringStringNotImplemented() 
        throws SQLException {
        try {
            cStmt.getNString("some-parameter-name");
            fail("CallableStatement.getNString(String) " +
                 "should not be implemented");
        } catch (SQLFeatureNotSupportedException sfnse) {
            // We are fine, do nothing.
        }
    }

    
    public void testGetCharacterStreamIntOnInvalidTypeDOUBLE() 
        throws SQLException {
        cStmt.setDouble(2, 3.3);
        cStmt.execute();
        try {
            cStmt.getCharacterStream(1);
            fail("An exception signalling invalid data type conversion " +
                 "should have been thrown");
        } catch (SQLDataException sqlde) {
            assertSQLState("Exception with invalid SQL state thrown on " +
                    "invalid data type conversion", "22005", sqlde);
        }
    }

    /**
     * Test which SQLState is thrown when getCharacterStream is called
     * on an IN parameter of an unsupported type.
     */
    public void testGetCharacterStreamIntOnInParameterOfInvalidType()
        throws SQLException {
        cStmt.setDouble(2, 3.3);
        cStmt.execute();
        try {
            cStmt.getCharacterStream(2);
            fail("Calling getCharacterStream on an IN parameter should " +
                 "throw an exception");
        } catch (SQLException sqle) {
            // SQLState differ between DerbyNetClient and embedded.
            String sqlState = usingDerbyNetClient() ? "XJ091" : "XCL26";
            assertSQLState("Exception with invalid SQL state thrown for " +
                           "getCharacterStream on IN parameter", 
                           sqlState, sqle);
        }
    }
    
    /**
     * Test which SQLState is thrown when getCharacterStream is called
     * on an IN parameter of a supported type.
     */
    public void testGetCharacterStreamIntOnInParameterOfValidType()
        throws SQLException {
        cStmt = CallableStatementTestSetup.getBinaryDirectProcedure(con);
        cStmt.setString(1, "A string");
        cStmt.execute();
        try {
            cStmt.getCharacterStream(1);
            fail("Calling getCharacterStream on an IN parameter should " +
                 "throw an exception");
        } catch (SQLException sqle) {
            // SQLState differ between DerbyNetClient and embedded.
            String sqlState = usingDerbyNetClient() ? "XJ091" : "XCL26";
            assertSQLState("Exception with invalid SQL state thrown for " +
                           "getCharacterStream on IN parameter", 
                           sqlState, sqle);
        }
    }
    
    /**
     * Test basic use of getCharacterStream on character data.
     * Create a CallableStatement that takes an integer as input and returns
     * the number as a string. The string is read as a stream, and the integer
     * is recreated from it and compared to the integer passed in.
     */
    public void testGetCharacterStreamIntVARCHAR()
        throws IOException, SQLException {
        cStmt = CallableStatementTestSetup.getIntToStringFunction(con);
        cStmt.setInt(2, 4509);
        assertFalse("No resultsets should be returned", cStmt.execute());
        assertEquals("Incorrect updatecount", -1, cStmt.getUpdateCount());
        // Get a character stream
        Reader cStream = cStmt.getCharacterStream(1);
        assertFalse("Stream should not be null", cStmt.wasNull());
        assertNotNull("Stream is null even though wasNull() returned false",
                cStream);
        char[] chars = new char[4];
        assertEquals("Wrong number of characters read",
                4, cStream.read(chars));
        // Make sure we have reached end of stream.
        assertEquals("Expected end of stream, but there were more data",
                -1, cStream.read());
        cStream.close();
        String result = new String(chars);
        assertEquals("Incorrect result obtained through java.io.Reader",
                "4509", result);
    }
    
    /**
     * Test basic use of getCharacterStream on binary data.
     * Create a CallableStatement that takes a string as input and returns
     * a byte representation, which is then read through a stream. The string
     * is recreated and compared to the one passed in. Note that strings must
     * be represented in UTF-16BE for this to work.
     */
    public void testGetCharacterStreamIntVARBINARYDirect()
        throws IOException, SQLException {
        String data = "This is the test string.";
        cStmt = CallableStatementTestSetup.getBinaryDirectProcedure(con);
        cStmt.setString(1, data);
        assertFalse("No resultsets should be returned", cStmt.execute());
        // Note that getUpdateCount behaves differently on client and embedded.
        assertEquals("Incorrect updatecount", 
                     usingEmbedded() ? 0 : -1, 
                     cStmt.getUpdateCount());
        Reader cStream = cStmt.getCharacterStream(2);
        assertFalse("Stream should not be null", cStmt.wasNull());
        assertNotNull("Stream is null even though wasNull() returned false",
                cStream);
        // Assume we don't know how many bytes the string will be represented 
        // by, just create enough space and read until stream is exhausted.
        // To be able to read the string back, getBytes must be called with
        // UTF-16BE charset, because Derby uses UTF-16BE encoding as default.
        // JDBC does not specify which charset to use for binary data, and 
        // UTF-16BE was apparently selected to match JCC.
        char[] tmpChars = new char[data.length() * 4];
        int curChar = cStream.read();
        int index = 0;
        while (curChar != -1) {
            tmpChars[index] = (char)curChar;
            index++;
            curChar = cStream.read();
        }
        cStream.close();
        char[] chars = new char[index];
        System.arraycopy(tmpChars, 0, chars, 0, index);
        String result = new String(chars);
        assertEquals("Incorrect result obtained through java.io.Reader",
                data, result);
    }

    /**
     * Fetch a string stored as bytes from the database through a reader,
     * then recreate the string.
     */
    public void testGetCharacterStreamIntVARBINARYFromDb()
        throws IOException, SQLException {
        cStmt = CallableStatementTestSetup.getBinaryFromDbFunction(con);
        cStmt.setInt(2, CallableStatementTestSetup.STRING_BYTES_ID);
        assertFalse("No resultsets should be returned", cStmt.execute());
        assertEquals("Incorrect updatecount", -1, cStmt.getUpdateCount());
        Reader cStream = cStmt.getCharacterStream(1);
        assertFalse("Stream should not be null", cStmt.wasNull());
        assertNotNull("Stream is null even though wasNull() returned false",
                cStream);
        char[] tmpChars = new char[32672];
        int curChar = cStream.read();
        int index = 0;
        while (curChar != -1) {
            tmpChars[index] = (char)curChar;
            index++;
            curChar = cStream.read();
        }
        char[] chars = new char[index];
        System.arraycopy(tmpChars, 0, chars, 0, index);
        tmpChars = null;
        cStream.close();
        String result = new String(chars);
        assertEquals("Strings not equal", 
                     CallableStatementTestSetup.STRING_BYTES, 
                     result);
    }

    /**
     * Read a SQL NULL value from a VARBINARY column through a reader.
     */
    public void testGetCharacterStreamIntOnVARBINARYWithNull()
        throws SQLException {
        cStmt = CallableStatementTestSetup.getBinaryFromDbFunction(con);
        cStmt.setInt(2, CallableStatementTestSetup.SQL_NULL_ID);
        assertFalse("No resultsets should be returned", cStmt.execute());
        assertEquals("Incorrect updatecount", -1, cStmt.getUpdateCount());
        Reader cStream = cStmt.getCharacterStream(1);
        assertTrue("Stream should be null", cStmt.wasNull());
        assertNull("Stream is not null even though wasNull() returned true",
                cStream);
    }
    
    /**
     * Read a SQL NULL value from a VARCHAR column through a reader.
     */
    public void testGetCharacterStreamIntOnVARCHARWithNull()
        throws SQLException {
        cStmt = CallableStatementTestSetup.getVarcharFromDbFunction(con);
        cStmt.setInt(2, CallableStatementTestSetup.SQL_NULL_ID);
        assertFalse("No resultsets should be returned", cStmt.execute());
        assertEquals("Incorrect updatecount", -1, cStmt.getUpdateCount());
        Reader cStream = cStmt.getCharacterStream(1);
        assertTrue("Stream should be null", cStmt.wasNull());
        assertNull("Stream is not null even though wasNull() returned true",
                cStream);
    }
    
    public void testGetCharacterStreamStringNotImplemented()
        throws SQLException {
        try {
            cStmt.getCharacterStream("some-parameter-name");
            fail("CallableStatement.getCharacterStream(String) " +
                 "should not be implemented");
        } catch (SQLFeatureNotSupportedException sfnse) {
            // We are fine, do nothing.
        }
    }

    public void testGetNCharacterStreamIntNotImplemented()
        throws SQLException {
        try {
            cStmt.getNCharacterStream(1);
            fail("CallableStatement.getNCharacterStream(int) " +
                 "should not be implemented");
        } catch (SQLFeatureNotSupportedException sfnse) {
            // We are fine, do nothing.
        }
    }
    
    public void testGetNCharacterStreamStringNotImplemented()
        throws SQLException {
        try {
            cStmt.getNCharacterStream("some-parameter-name");
            fail("CallableStatement.getNCharacterStream(String) " +
                 "should not be implemented");
        } catch (SQLFeatureNotSupportedException sfnse) {
            // We are fine, do nothing.
        }
    }

    public void testSetBlobNotImplemented()
        throws SQLException {
        try {
            cStmt.setBlob("some-parameter-name", (Blob)null);
            fail("CallableStatement.setBlob(String, Blob) " +
                 "should not be implemented");
        } catch (SQLFeatureNotSupportedException sfnse) {
            // We are fine, do nothing.
        }
    }
    
    public void testSetClobNotImplemented()
        throws SQLException {
        try {
            cStmt.setClob("some-parameter-name", (Clob)null);
            fail("CallableStatement.setClob(String, Clob) " +
                 "should not be implemented");
        } catch (SQLFeatureNotSupportedException sfnse) {
            // We are fine, do nothing.
        }
    }

    public void testSetNCharacterStreamNotImplemented()
        throws SQLException {
        try {
            cStmt.setNCharacterStream("some-parameter-name", null, 0l);
            fail("CallableStatement.setNCharacterStream(String,Reader,long) " +
                 "should not be implemented");
        } catch (SQLFeatureNotSupportedException sfnse) {
            // We are fine, do nothing.
        }
    }

    public void testSetNClobNClobNotImplemented()
        throws SQLException {
        try {
            cStmt.setNClob("some-parameter-name", (NClob)null);
            fail("CallableStatement.setNClob(String, NClob) " +
                 "should not be implemented");
        } catch (SQLFeatureNotSupportedException sfnse) {
            // We are fine, do nothing.
        }
    }

    public void testSetNClobReaderNotImplemented()
        throws SQLException {
        try {
            cStmt.setNClob("some-parameter-name", null, 0l);
            fail("CallableStatement.setNClob(String, Reader, long) " +
                 "should not be implemented");
        } catch (SQLFeatureNotSupportedException sfnse) {
            // We are fine, do nothing.
        }
    }

    public void testSetNStringNotImplemented()
        throws SQLException {
        try {
            cStmt.setNString("some-parameter-name", "some-value");
            fail("CallableStatement.setNString(String, String) " +
                 "should not be implemented");
        } catch (SQLFeatureNotSupportedException sfnse) {
            // We are fine, do nothing.
        }
    }
   
    public void testGetSQLXMLIntNotImplemented()
        throws SQLException {
        try {
            cStmt.getSQLXML(1);
            fail("CallableStatement.getSQLXML(int) " +
                 "should not be implemented");
        } catch (SQLFeatureNotSupportedException sfnse) {
            // We are fine, do nothing.
        }
    }
    
    public void testGetSQLXMLStringNotImplemented()
        throws SQLException {
        try {
            cStmt.getSQLXML("some-parameter-name");
            fail("CallableStatement.getSQLXML(String) " +
                 "should not be implemented");
        } catch (SQLFeatureNotSupportedException sfnse) {
            // We are fine, do nothing.
        }
    }

    public void testSetSQLXMLNotImplemented()
        throws SQLException {
        try {
            cStmt.setSQLXML("some-parameter-name", null);
            fail("CallableStatement.setSQLXML(String, SQLXML) " +
                 "should not be implemented");
        } catch (SQLFeatureNotSupportedException sfnse) {
            // We are fine, do nothing.
        }
    }

    /** Helper method for testIsWrapperFor*Statement test cases. */
    private void testIsWrapperForXXXStatement(Class klass) throws SQLException {
        assertTrue("The CallableStatement is not a wrapper for "
                       + klass.getName(),
                   cStmt.isWrapperFor(klass));
    }

    public void testIsWrapperForStatement() throws SQLException {
        testIsWrapperForXXXStatement(Statement.class);
    }

    public void testIsWrapperForPreparedStatement() throws SQLException {
        testIsWrapperForXXXStatement(PreparedStatement.class);
    }

    public void testIsWrapperForCallableStatement() throws SQLException {
        testIsWrapperForXXXStatement(CallableStatement.class);
    }

    public void testIsNotWrapperForResultSet() throws SQLException {
        assertFalse(cStmt.isWrapperFor(ResultSet.class));
    }

    public void testUnwrapStatement() throws SQLException {
        Statement stmt = cStmt.unwrap(Statement.class);
        assertSame("Unwrap returned wrong object.", cStmt, stmt);
    }

    public void testUnwrapPreparedStatement() throws SQLException {
        PreparedStatement ps = cStmt.unwrap(PreparedStatement.class);
        assertSame("Unwrap returned wrong object.", cStmt, ps);
    }

    public void testUnwrapCallableStatement() throws SQLException {
        Statement cs = cStmt.unwrap(CallableStatement.class);
        assertSame("Unwrap returned wrong object.", cStmt, cs);
    }

    public void testUnwrapResultSet() {
        try {
            ResultSet rs = cStmt.unwrap(ResultSet.class);
            fail("Unwrap didn't fail.");
        } catch (SQLException e) {
            assertSQLState("XJ128", e);
        }
    }

    /**
     *
     * Tests the setCharacterStream method that accepts length as a long
     * parameter in the Callable Statement interface
     *
     * @throws SQLException Upon any error that occurs while calling this
     *         method
     *
     */

    public void testSetCharacterStream() throws SQLException {
        try {
            cStmt.setCharacterStream("Some String",null,0L);
            fail("CallableStatement.setCharacterStream() " +
                 "should not be implemented");
        }
        catch(SQLFeatureNotSupportedException sqlfne) {
            //Do nothing as this is the expected behaviour

        }
    }

    /**
     *
     * Tests the setAsciiStream method that accepts length as a long
     * parameter in the Callable Statement interface
     *
     * @throws SQLException Upon any error that occurs while calling this
     *         method
     *
     */

    public void testSetAsciiStream() throws SQLException {
        try {
            cStmt.setAsciiStream("Some String",null,0L);
            fail("CallableStatement.setAsciiStream() " +
                 "should not be implemented");
        }
        catch(SQLFeatureNotSupportedException sqlfne) {
            //Do nothing as this is the expected behaviour

        }
    }

    /**
     *
     * Tests the setBinaryStream method that accepts length as a long
     * parameter in the Callable Statement interface
     *
     * @throws SQLException Upon any error that occurs while calling this
     *         method
     *
     */

    public void testSetBinaryStream() throws SQLException {
        try {
            cStmt.setBinaryStream("Some String",null,0L);
            fail("CallableStatement.setBinaryStream() " +
                 "should not be implemented");
        }
        catch(SQLFeatureNotSupportedException sqlfne) {
            //Do nothing as this is the expected behaviour

        }
    }

    /**
     * Return suite with all tests of the class.
     */
    public static Test suite() {
        TestSuite mainSuite = new TestSuite();
        TestSuite suite = new TestSuite(CallableStatementTest.class,
                                        "CallableStatementTest suite");
        mainSuite.addTest(new CallableStatementTestSetup(suite));
        mainSuite.addTest(SetObjectUnsupportedTest.suite(true));
        return mainSuite;
    }
    
} // End class CallableStatementTest
