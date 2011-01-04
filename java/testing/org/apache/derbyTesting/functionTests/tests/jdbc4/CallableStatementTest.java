/*
 
   Derby - Class org.apache.derbyTesting.functionTests.tests.jdbc4.CallableStatementTest
 
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

import org.apache.derby.iapi.types.HarmonySerialBlob;
import org.apache.derby.iapi.types.HarmonySerialClob;

import org.apache.derbyTesting.junit.TestConfiguration;

import java.io.IOException;
import java.io.Reader;
import java.math.BigDecimal;
import java.sql.*;

/**
 * Tests of the <code>java.sql.CallableStatement</code> JDBC40 API.
 */
public class CallableStatementTest  extends Wrapper41Test
{
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
    protected void setUp() 
        throws SQLException {
        cStmt = prepareCall("? = CALL FLOOR(?)");
        cStmt.registerOutParameter(1, Types.DOUBLE);
    }

    /**
     * Close default callable statement and connection.
     *
     * @throws SQLException if closing of the connection or the callable
     *                      statement fail.
     */
    protected void tearDown()
        throws Exception {

        cStmt.close();
        cStmt = null;

        super.tearDown();
    }
   
    public void testNamedParametersAreNotSupported()
        throws SQLException {
        DatabaseMetaData met = getConnection().getMetaData();
        assertFalse("Named parameters are not supported, but the metadata " +
                    "says they are", met.supportsNamedParameters());
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
        cStmt = CallableStatementTestSetup.getBinaryDirectProcedure(getConnection());
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
        cStmt = CallableStatementTestSetup.getIntToStringFunction(getConnection());
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
        cStmt = CallableStatementTestSetup.getBinaryDirectProcedure(getConnection());
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
        cStmt = CallableStatementTestSetup.getBinaryFromDbFunction(getConnection());
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
        cStmt = CallableStatementTestSetup.getBinaryFromDbFunction(getConnection());
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
        cStmt = CallableStatementTestSetup.getVarcharFromDbFunction(getConnection());
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
     * Test the JDBC 4.1 extensions.
     */
    public  void    testJDBC4_1() throws Exception
    {
        Connection  conn = getConnection();
        
        vetDataTypeCount( conn );

        PreparedStatement   ps = prepareStatement
            (
             conn,
             "create procedure allTypesProc\n" +
             "(\n" +
             "    out bigintCol bigint,\n" +
             "    out blobCol blob,\n" +
             "    out booleanCol boolean,\n" +
             "    out charCol char(1),\n" +
             "    out charForBitDataCol char(1) for bit data,\n" +
             "    out clobCol clob,\n" +
             "    out dateCol date,\n" +
             "    out doubleCol double,\n" +
             "    out floatCol float,\n" +
             "    out intCol int,\n" +
             "    out longVarcharCol long varchar,\n" +
             "    out longVarcharForBitDataCol long varchar for bit data,\n" +
             "    out numericCol numeric,\n" +
             "    out realCol real,\n" +
             "    out smallintCol smallint,\n" +
             "    out timeCol time,\n" +
             "    out timestampCol timestamp,\n" +
             "    out varcharCol varchar( 2 ),\n" +
             "    out varcharForBitDataCol varchar( 2 ) for bit data\n" +
             ")\n" +
             "language java\n" +
             "parameter style java\n" +
             "no sql\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.jdbc4.CallableStatementTest.allTypesProc'\n"
             );
        ps.execute();
        ps.close();

        CallableStatement cs = prepareCall
            (
             conn,
             "call allTypesProc(  ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? )"
             );
        int param = 1;
        cs.registerOutParameter( param++, Types.BIGINT );
        cs.registerOutParameter( param++, Types.BLOB );
        cs.registerOutParameter( param++, Types.BOOLEAN );
        cs.registerOutParameter( param++, Types.CHAR );
        cs.registerOutParameter( param++, Types.BINARY );
        cs.registerOutParameter( param++, Types.CLOB );
        cs.registerOutParameter( param++, Types.DATE );
        cs.registerOutParameter( param++, Types.DOUBLE );
        cs.registerOutParameter( param++, Types.FLOAT );
        cs.registerOutParameter( param++, Types.INTEGER );
        cs.registerOutParameter( param++, Types.LONGVARCHAR );
        cs.registerOutParameter( param++, Types.LONGVARBINARY );
        cs.registerOutParameter( param++, Types.NUMERIC );
        cs.registerOutParameter( param++, Types.REAL );
        cs.registerOutParameter( param++, Types.SMALLINT );
        cs.registerOutParameter( param++, Types.TIME );
        cs.registerOutParameter( param++, Types.TIMESTAMP );
        cs.registerOutParameter( param++, Types.VARCHAR );
        cs.registerOutParameter( param++, Types.VARBINARY );
        cs.execute();
        examineJDBC4_1extensions( new Wrapper41( cs ) );
        cs.close();

        ps = prepareStatement( conn, "drop procedure allTypesProc" );
        ps.execute();
        ps.close();
    }
    private void    vetDataTypeCount( Connection conn ) throws Exception
    {
        ResultSet rs = conn.getMetaData().getTypeInfo();
        int actualTypeCount = 0;
        while ( rs.next() ) { actualTypeCount++; }
        rs.close();

        //
        // If this assertion fails, that means that another data type has been added
        // to Derby. You need to add that datatype to the allTypesProc procedure created
        // by testJDBC4_1() and you need to add a verification case to examineJDBC4_1extensions().
        //
        assertEquals( 22, actualTypeCount );
    }
    
    /**
     * <p>
     * Procedure used by jdbc 4.1 tests.
     * </p>
     */
    public  static  void    allTypesProc
        (
         long[] bigintarg,
         Blob[] blobarg,
         boolean[] booleanarg,
         String[] chararg,
         byte[][] charforbitdataarg,
         Clob[] clobarg,
         Date[] datearg,
         double[] doublearg,
         double[] floatarg,
         int[] intarg,
         String[] longvarchararg,
         byte[][] longvarcharforbitdataarg,
         BigDecimal[] numericarg,
         float[] realarg,
         short[] smallintarg,
         Time[] timearg,
         Timestamp[] timestamparg,
         String[] varchararg,
         byte[][] varcharforbitdataarg
         )
        throws Exception
    {
        String  stringValue = "a";
        byte    intValue = (byte) 1;
        float   floatValue = 1.0F;
        String lobValue = "abc";
        
        bigintarg[0] = intValue;
        blobarg[0] = new HarmonySerialBlob( BINARY_VALUE );
        booleanarg[0] = true;
        chararg[0] = stringValue;
        charforbitdataarg[0] = BINARY_VALUE;
        clobarg[0] = new HarmonySerialClob( lobValue );
        datearg[0]= new Date( 761990400000L );
        doublearg[0] = floatValue;
        floatarg[0] = floatValue;
        intarg[0] = intValue;
        longvarchararg[0] = stringValue;
        longvarcharforbitdataarg[0] =  BINARY_VALUE;
        numericarg[0] = new BigDecimal( "1.0" );
        realarg[0] = floatValue;
        smallintarg[0] = intValue;
        timearg[0] = new Time(TIME_VALUE);
        timestamparg[0] = new Timestamp(TIMESTAMP_VALUE);
        varchararg[0] = stringValue;
        varcharforbitdataarg[0] = BINARY_VALUE;
    }

    /**
     * Return suite with all tests of the class.
     */
    public static Test suite() {
        TestSuite suite = new TestSuite("CallableStatementTest suite");
        suite.addTest(baseSuite("CallableStatementTest:embedded"));
        suite.addTest(TestConfiguration.clientServerDecorator(
            baseSuite("CallableStatementTest:client")));
        return suite;
    }

    private static Test baseSuite(String name) {
        TestSuite suite = new TestSuite(CallableStatementTest.class, name);
        return new CallableStatementTestSetup(suite);
    }
    
} // End class CallableStatementTest
