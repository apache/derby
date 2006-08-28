/*
 *
 * Derby - Class BaseJDBCTestCase
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
package org.apache.derbyTesting.junit;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.sql.*;

import junit.framework.AssertionFailedError;

import org.apache.derby.tools.ij;


/**
 * Base class for JDBC JUnit tests.
 * A method for getting a default connection is provided, along with methods
 * for telling if a specific JDBC client is used.
 */
public abstract class BaseJDBCTestCase
    extends BaseTestCase {

    /**
     * Maintain a single connection to the default
     * database, opened at the first call to getConnection.
     * Typical setup will just require a single connection.
     * @see BaseJDBCTestSetup#getConnection()
     */
    private Connection conn;
    
    /**
     * Create a test case with the given name.
     *
     * @param name of the test case.
     */
    public BaseJDBCTestCase(String name) {
        super(name);
    }
    
    /**
     * Obtain the connection to the default database.
     * This class maintains a single connection returned
     * by this class, it is opened on the first call to
     * this method. Subsequent calls will return the same
     * connection object unless it has been closed. In that
     * case a new connection object will be returned.
     * <P>
     * The tearDown method will close the connection if
     * it is open.
     * @see TestConfiguration#openDefaultConnection()
     */
    public Connection getConnection() throws SQLException
    {
        if (conn != null)
        {
            if (!conn.isClosed())
                return conn;
            conn = null;
        }
        return conn = openDefaultConnection();
    }
    
    /**
     * Allow a sub-class to initialize a connection to provide
     * consistent connection state for its tests. Called once
     * for each time these method calls open a connection:
     * <UL>
     * <LI> getConnection()
     * <LI> openDefaultConnection()
     * <LI> openConnection(database)
     * </UL>
     * when getConnection() opens a new connection. Default
     * action is to not modify the connection's state from
     * the initialization provided by the data source.
     * @param conn Connection to be intialized
     * @throws SQLException Error setting the initial state.
     */
    protected void initializeConnection(Connection conn) throws SQLException
    {
    }
    
    /**
     * Utility method to create a Statement using the connection
     * returned by getConnection.
     * @return Statement object from getConnection.createStatement()
     * @throws SQLException
     */
    public Statement createStatement() throws SQLException
    {
        return getConnection().createStatement();
    }

    /**
     * Utility method to create a Statement using the connection
     * returned by getConnection.
     * @return Statement object from
     * getConnection.createStatement(resultSetType, resultSetConcurrency)
     * @throws SQLException
     */
    public Statement createStatement(int resultSetType,
            int resultSetConcurrency) throws SQLException
    {
        return getConnection().createStatement(resultSetType, resultSetConcurrency);
    }
    /**
     * Utility method to create a PreparedStatement using the connection
     * returned by getConnection.
     * @return Statement object from
     * getConnection.prepareStatement(sql)
     * @throws SQLException
     */
    public PreparedStatement prepareStatement(String sql) throws SQLException
    {
        return getConnection().prepareStatement(sql);
    }    

    /**
     * Utility method to create a CallableStatement using the connection
     * returned by getConnection.
     * @return Statement object from
     * getConnection().prepareCall(sql)
     * @throws SQLException
     */
    public CallableStatement prepareCall(String sql) throws SQLException
    {
        return getConnection().prepareCall(sql);
    }
    
    /**
     * Utility method to commit using the connection
     * returned by getConnection.
     * @throws SQLException
     */
    public void commit() throws SQLException
    {
        getConnection().commit();
    }  
    /**
     * Utility method to rollback using the connection
     * returned by getConnection.
     * @throws SQLException
     */
    public void rollback() throws SQLException
    {
        getConnection().rollback();
    } 
    /**
     * Tear down this fixture, sub-classes should call
     * super.tearDown(). This cleanups & closes the connection
     * if it is open.
     */
    protected void tearDown()
    throws java.lang.Exception
    {
        JDBC.cleanup(conn);
        conn = null;
    }

    /**
     * Open a connection to the default database.
     * If the database does not exist, it will be created.
     * A default username and password will be used for the connection.
     *
     * @return connection to default database.
     * @see TestConfiguration#openDefaultConnection()
     */
    public Connection openDefaultConnection()
        throws SQLException {
        Connection conn =  getTestConfiguration().openDefaultConnection();
        initializeConnection(conn);
        return conn;
    }
    
    public Connection openConnection(String databaseName) throws SQLException
    {
        Connection conn = getTestConfiguration().openConnection(databaseName);
        initializeConnection(conn);
        return conn;        
    }
    
    /**
     * Run a SQL script through ij discarding the output
     * using this object's default connection. Intended for
     * setup scripts.
     * @throws UnsupportedEncodingException 
     * @throws SQLException 
     */
    public int runScript(InputStream script, String encoding)
        throws UnsupportedEncodingException, SQLException
    {
        // Sink output.
        OutputStream sink = new OutputStream() {
            public void write(byte[] b, int off, int len) {}
            public void write(int b) {}
        };
        
        // Use the same encoding as the input for the output.    
        return ij.runScript(getConnection(), script, encoding,
                sink, encoding);       
    }
    
    /**
     * Run a set of SQL commands from a String discarding the output.
     * Commands are separated by a semi-colon. Connection used
     * is this objects default connection.
     * @param sqlCommands
     * @return Number of errors executing the script.
     * @throws UnsupportedEncodingException
     * @throws SQLException
     */
    public int runSQLCommands(String sqlCommands)
        throws UnsupportedEncodingException, SQLException
    {
        byte[] raw = sqlCommands.getBytes("UTF-8");
        ByteArrayInputStream in = new ByteArrayInputStream(raw);
        
        return runScript(in, "UTF-8");
    }
    
    /**
     * Tell if the client is embedded.
     *
     * @return <code>true</code> if using the embedded client
     *         <code>false</code> otherwise.
     */
     public static boolean usingEmbedded() {
         return TestConfiguration.getCurrent().getJDBCClient().isEmbedded();
     }
    
    /**
    * Tell if the client is DerbyNetClient.
    *
    * @return <code>true</code> if using the DerbyNetClient client
    *         <code>false</code> otherwise.
    */
    public static boolean usingDerbyNetClient() {
        return TestConfiguration.getCurrent().getJDBCClient().isDerbyNetClient();
    }
    
    /**
    * Tell if the client is DerbyNet.
    *
    * @return <code>true</code> if using the DerbyNet client
    *         <code>false</code> otherwise.
    */
    public static boolean usingDerbyNet() {
        return TestConfiguration.getCurrent().getJDBCClient().isDB2Client();
    }

    /**
     * Assert equality between two <code>Blob</code> objects.
     * If both input references are <code>null</code>, they are considered
     * equal. The same is true if both blobs have <code>null</code>-streams.
     *
     * @param b1 first <code>Blob</code>.
     * @param b2 second <code>Blob</code>.
     * @throws AssertionFailedError if blobs are not equal.
     * @throws IOException if reading or closing a stream fails
     * @throws SQLException if obtaining a stream fails
     */
    public static void assertEquals(Blob b1, Blob b2)
            throws IOException, SQLException {
        if (b1 == null || b2 == null) {
            assertNull("Blob b2 is null, b1 is not", b1);
            assertNull("Blob b1 is null, b2 is not", b2);
            return;
        }
        assertEquals("Blobs have different lengths",
                     b1.length(), b2.length());
        InputStream is1 = b1.getBinaryStream();
        InputStream is2 = b2.getBinaryStream();
        if (is1 == null || is2 == null) {
            assertNull("Blob b2 has null-stream, blob b1 doesn't", is1);
            assertNull("Blob b1 has null-stream, blob b2 doesn't", is2);
            return;
        }
        long index = 1;
        int by1 = is1.read();
        int by2 = is2.read();
        do {
            // Avoid string concatenation for every byte in the stream.
            if (by1 != by2) {
                assertEquals("Blobs differ at index " + index,
                        by1, by2);
            }
            index++;
            by1 = is1.read();
            by2 = is2.read();
        } while ( by1 != -1 || by2 != -1);
        is1.close();
        is2.close();
    }

    /**
     * Assert equality between two <code>Clob</code> objects.
     * If both input references are <code>null</code>, they are considered
     * equal. The same is true if both clobs have <code>null</code>-streams.
     *
     * @param c1 first <code>Clob</code>.
     * @param c2 second <code>Clob</code>.
     * @throws AssertionFailedError if clobs are not equal.
     * @throws IOException if reading or closing a stream fails
     * @throws SQLException if obtaining a stream fails
     */
    public static void assertEquals(Clob c1, Clob c2)
            throws IOException, SQLException {
        if (c1 == null || c2 == null) {
            assertNull("Clob c2 is null, c1 is not", c1);
            assertNull("Clob c1 is null, c2 is not", c2);
            return;
        }
        assertEquals("Clobs have different lengths",
                     c1.length(), c2.length());
        Reader r1 = c1.getCharacterStream();
        Reader r2 = c2.getCharacterStream();
        if (r1 == null || r2 == null) {
            assertNull("Clob c2 has null-stream, clob c1 doesn't", r1);
            assertNull("Clob c1 has null-stream, clob c2 doesn't", r2);
            return;
        }
        long index = 1;
        int ch1 = r1.read();
        int ch2 = r2.read();
        do {
            // Avoid string concatenation for every char in the stream.
            if (ch1 != ch2) {
                assertEquals("Clobs differ at index " + index,
                        ch1, ch2);
            }
            index++;
            ch1 = r1.read();
            ch2 = r2.read();
        } while (ch1 != -1 || ch2 != -1);
        r1.close();
        r2.close();
    }

    /**
     * Assert that SQLState is as expected.
     *
     * @param message message to print on failure.
     * @param expected the expected SQLState.
     * @param exception the exception to check the SQLState of.
     */
    public static void assertSQLState(String message, 
                                      String expected, 
                                      SQLException exception) {
        // Make sure exception is not null. We want to separate between a
        // null-exception object, and a null-SQLState.
        assertNotNull("Exception cannot be null when asserting on SQLState", 
                      exception);
        
        try {
            String state = exception.getSQLState();
            
            if ( state != null )
                assertTrue("The exception's SQL state must be five characters long",
                        state.length() == 5);
            
            if ( expected != null )
                assertTrue("The expected SQL state must be five characters long",
                    expected.length() == 5);
            
            assertEquals(message, expected, state);
        } catch (AssertionFailedError e) {
            
            // Save the SQLException
            // e.initCause(exception);

            throw e;
        }
    }

    /**
     * Assert that SQLState is as expected.
     *
     * @param expected the expected SQLState.
     * @param exception the exception to check the SQLState of.
     */
    public static void assertSQLState(String expected, SQLException exception) {
        assertSQLState("Unexpected SQL state.", expected, exception);
    }
    /**
     * Assert that the query does not compile and throws
     * a SQLException with the expected state.
     * 
     * @param sqlstate expected sql state.
     * @param query the query to compile.
     */
    public void assertCompileError(String sqlState, String query) {

        try {
            prepareStatement(query).close();
            fail("expected compile error: " + sqlState);
        } catch (SQLException se) {
            assertSQLState(sqlState, se);
        }
    }

} // End class BaseJDBCTestCase


