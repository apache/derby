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
import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.PrintStream;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Method;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.net.URL;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import junit.framework.AssertionFailedError;
import junit.framework.Test;

import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.execute.RunTimeStatistics;
import org.apache.derby.impl.jdbc.EmbedConnection;
import org.apache.derby.tools.ij;
import org.apache.derbyTesting.functionTests.util.TestNullOutputStream;


/**
 * Base class for JDBC JUnit tests.
 * A method for getting a default connection is provided, along with methods
 * for telling if a specific JDBC client is used.
 */
public abstract class BaseJDBCTestCase
    extends BaseTestCase {

    private static final boolean ORDERED = true;
    private static final boolean UNORDERED = false;

    /**
     * Maintain a single connection to the default
     * database, opened at the first call to getConnection.
     * Typical setup will just require a single connection.
     * @see BaseJDBCTestCase#getConnection()
     */
    private Connection conn;
    
    /**
     * Maintain a list of statement objects that
     * were returned by utility methods and close
     * them at teardown.
     */
    private List<Statement> statements;

    /**
     * Maintain a list of connection objects that
     * were returned by utility methods and close
     * them at teardown.
     */
    private List<Connection> connections;
    
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
     * <BR>
     * The connection will be initialized by calling initializeConnection.
     * A sub-class may provide an implementation of initializeConnection
     * to ensure its connections are in a consistent state that is different
     * to the default.
     * @see #openDefaultConnection()
     */
    public Connection getConnection() throws SQLException
    {
        if (conn != null)
        {
            if (!conn.isClosed())
                return conn;
            conn = null;
        }
//IC see: https://issues.apache.org/jira/browse/DERBY-1555
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
     * <LI> getDefaultConnection(String connAttrs)
     * </UL>
     * Default action is to not modify the connection's state from
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
     * The returned statement object will be closed automatically
     * at tearDown() but may be closed earlier by the test if required.
     * @return Statement object from getConnection.createStatement()
     * @throws SQLException
     */
    public Statement createStatement() throws SQLException
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-2907
        Statement s = getConnection().createStatement();
        addStatement(s);
        return s;
    }
    
    /**
     * Add a statement into the list we will close
     * at tearDown.
     */
    private void addStatement(Statement s)
    {
        if (statements == null)
//IC see: https://issues.apache.org/jira/browse/DERBY-5840
            statements = new ArrayList<Statement>();
        statements.add(s);
    }
    
    /**
     * Add connection to the list. We will close at tearDown
     * @param c
     */
    private void addConnection(Connection c)
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-6150
        if (connections == null)
//IC see: https://issues.apache.org/jira/browse/DERBY-5840
            connections = new ArrayList<Connection>();
        connections.add(c);     
    }
    
    /**
     * Close a statement and remove it from the list of statements to close
     * at tearDown(). Useful for test cases that create a large number of
     * statements that are only used for a short time, as the memory footprint
     * may become big if all the statements are held until tearDown().
     *
     * @param s the statement to close and forget
     * @throws SQLException if closing the statement fails
     */
    public void closeStatement(Statement s) throws SQLException {
//IC see: https://issues.apache.org/jira/browse/DERBY-5683
        s.close();
        if (statements != null) {
            statements.remove(s);
        }
    }

    /**
     * Utility method to create a Statement using the connection
     * returned by getConnection.
     * The returned statement object will be closed automatically
     * at tearDown() but may be closed earlier by the test if required.
     * @return Statement object from
     * getConnection.createStatement(resultSetType, resultSetConcurrency)
     * @throws SQLException
     */
    public Statement createStatement(int resultSetType,
            int resultSetConcurrency) throws SQLException
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-2907
        Statement s =
            getConnection().createStatement(resultSetType, resultSetConcurrency);
        addStatement(s);
        return s;
    }

    /**
     * Utility method to create a Statement using the connection
     * returned by getConnection.
     * @return Statement object from
     * getConnection.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability)
     * @throws SQLException
     */
    public Statement createStatement(int resultSetType,
            			int resultSetConcurrency,
	    			int resultSetHoldability) throws SQLException
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-2854
        return getConnection().createStatement(resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    /**
     * Utility method to create a PreparedStatement using the connection
     * returned by getConnection.
     * The returned statement object will be closed automatically
     * at tearDown() but may be closed earlier by the test if required.
     * @return Statement object from
     * getConnection.prepareStatement(sql)
     * @throws SQLException
     */
    public PreparedStatement prepareStatement(String sql) throws SQLException
    {
        PreparedStatement ps = getConnection().prepareStatement(sql);
        addStatement(ps);
        return ps;
    }
    
    /**
     * Utility method to create a PreparedStatement using the connection
     * returned by getConnection with result set type and concurrency.
     * The returned statement object will be closed automatically
     * at tearDown() but may be closed earlier by the test if required.
     * @return Statement object from
     * getConnection.prepareStatement(sql)
     * @throws SQLException
     */
    public PreparedStatement prepareStatement(String sql,
            int resultSetType, int resultSetConcurrency) throws SQLException
    {
        PreparedStatement ps = getConnection().prepareStatement(sql,
                resultSetType, resultSetConcurrency);
        addStatement(ps);
        return ps;
    }
    /**
     * Utility method to create a PreparedStatement using the connection
     * returned by getConnection with result set type and concurrency.
     * The returned statement object will be closed automatically
     * at tearDown() but may be closed earlier by the test if required.
     * @return Statement object from
     * getConnection.prepareStatement(sql)
     * @throws SQLException
     */
    public PreparedStatement prepareStatement(String sql,
            int resultSetType, int resultSetConcurrency,
            int resultSetHoldability) throws SQLException
    {
        PreparedStatement ps = getConnection().prepareStatement(sql,
                resultSetType, resultSetConcurrency, resultSetHoldability);
        addStatement(ps);
        return ps;
    }
    /**
     * Utility method to create a PreparedStatement using the connection
     * returned by getConnection and a flag that signals the driver whether
     * the auto-generated keys produced by this Statement object should be
     * made available for retrieval.
     * The returned statement object will be closed automatically
     * at tearDown() but may be closed earlier by the test if required.
     * @return Statement object from
     * prepareStatement(sql, autoGeneratedKeys)
     * @throws SQLException
     */
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys)
//IC see: https://issues.apache.org/jira/browse/DERBY-2398
        throws SQLException
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-2907
        PreparedStatement ps =
            getConnection().prepareStatement(sql, autoGeneratedKeys);
        
        addStatement(ps);
        return ps;
    }    

    /**
     * Utility method to create a PreparedStatement using the connection
     * returned by getConnection and an array of column indexes that
     * indicates which auto-generated keys produced by this Statement
     * object should be made available for retrieval.
     * The returned statement object will be closed automatically
     * at tearDown() but may be closed earlier by the test if required.
     *
     * @return Statement object from:
     *     prepareStatement(sql, columnIndexes)
     *
     * @throws SQLException
     */
    public PreparedStatement prepareStatement(String sql,
        int [] columnIndexes) throws SQLException
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-2907
        PreparedStatement ps =
            getConnection().prepareStatement(sql, columnIndexes);
        addStatement(ps);
        return ps;
    }

    /**
     * Utility method to create a PreparedStatement using the connection
     * returned by getConnection and an array of column names that
     * indicates which auto-generated keys produced by this Statement
     * object should be made available for retrieval.
     * The returned statement object will be closed automatically
     * at tearDown() but may be closed earlier by the test if required.
     *
     * @return Statement object from:
     *     prepareStatement(sql, columnNames)
     *
     * @throws SQLException
     */
    public PreparedStatement prepareStatement(String sql,
        String [] columnNames) throws SQLException
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-2907
        PreparedStatement ps =
            getConnection().prepareStatement(sql, columnNames);
        addStatement(ps);
        return ps;
     }

    /**
     * Utility method to create a CallableStatement using the connection
     * returned by getConnection.
     * The returned statement object will be closed automatically
     * at tearDown() but may be closed earlier by the test if required.
     * @return Statement object from
     * getConnection().prepareCall(sql)
     * @throws SQLException
     */
    public CallableStatement prepareCall(String sql) throws SQLException
    {
        CallableStatement cs =
            getConnection().prepareCall(sql);
        addStatement(cs);
        return cs;
 
    }

    /**
     * Utility method to create a CallableStatement using the connection
     * returned by getConnection.
     * The returned statement object will be closed automatically
     * at tearDown() but may be closed earlier by the test if required.
     * @return Statement object from
     * getConnection().prepareCall(sql, resultSetType, resultSetConcurrency)
     * @throws SQLException
     */
    public CallableStatement prepareCall(String sql,
					int resultSetType, 
					int resultSetConcurrency) throws SQLException
    {
        CallableStatement cs = getConnection().prepareCall(sql, resultSetType,
                resultSetConcurrency);
        addStatement(cs);
        return cs;
    }

    /**
     * Utility method to create a CallableStatement using the connection
     * returned by getConnection.
     * The returned statement object will be closed automatically
     * at tearDown() but may be closed earlier by the test if required.
     * @return Statement object from
     * getConnection().prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability)
     * @throws SQLException
     */
    public CallableStatement prepareCall(String sql,
                                        int resultSetType,
                                        int resultSetConcurrency,
					 int resultSetHoldability) throws SQLException
    {
        CallableStatement cs = getConnection().prepareCall(sql,
                resultSetType, resultSetConcurrency, resultSetHoldability);
        addStatement(cs);
        return cs;
    }
    
    /**
     * Utility method to set auto commit behaviour.
     * @param commit false if autoCommit should be disabled.
     */
    public void setAutoCommit(boolean commit) throws SQLException {
//IC see: https://issues.apache.org/jira/browse/DERBY-3663
    	getConnection().setAutoCommit(commit);
    }
    /**
     * Utility method to commit using the connection
     * returned by getConnection.
     * @throws SQLException
     */
    public void commit() throws SQLException
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-1555
        getConnection().commit();
    }  
    
    /**
     * Utility method to rollback using the connection
     * returned by getConnection.
     * @throws SQLException
     */
    public void rollback() throws SQLException
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-1555
        getConnection().rollback();
    } 

    /**
     * <p>
     * Run the bare test, including {@code setUp()} and {@code tearDown()}.
     * </p>
     *
     * <p>
     * Subclasses that want to override {@code runBare()}, should override
     * this method instead. Overriding this method shouldn't be necessary
     * except in very special cases. Override {@code setUp()} and
     * {@code tearDown()} instead if possible.
     * </p>
     *
     * <p>
     * The overridden method would typically want to call
     * {@code super.runBareOverridable()} to actually run the test.
     * </p>
     */
    protected void runBareOverridable() throws Throwable {
//IC see: https://issues.apache.org/jira/browse/DERBY-5726
        super.runBare();
    }

    /**
     * <p>
     * Run the bare test, including {@code setUp()} and {@code tearDown()},
     * and finally verify that the cached connection has been released.
     * </p>
     *
     * <p>
     * This method is final to prevent subclasses from accidentally bypassing
     * the assert that checks if the cached connection has been released.
     * Subclasses that want to override the method, should override
     * {@link #runBareOverridable()} instead.
     * </p>
     */
    public final void runBare() throws Throwable {
        runBareOverridable();
        // It's quite common to forget to call super.tearDown() when
        // overriding tearDown() in sub-classes.
        assertNull(
            "Connection should be null by now. " +
            "Missing call to super.tearDown()?", conn);
    }

    /**
     * Tear down this fixture, sub-classes should call
     * super.tearDown(). This cleans up and closes the connection
     * if it is open and any statement objects returned through
     * the utility methods.
     */
    protected void tearDown()
    throws java.lang.Exception
    {
        if (statements != null) {
//IC see: https://issues.apache.org/jira/browse/DERBY-5840
            for (Statement s : statements) {
                s.close();
            }
            // Allow gc'ing of all those statements.
            statements = null;
        }
        if (connections != null) {
            for (Connection c : connections) {
                JDBC.cleanup(c);
            }
            // Allow gc'ing of all those connections.
            connections = null;
        }
        conn = null;
    }

    /**
     * Open a connection to the default database.
     * If the database does not exist, it will be created.
     * A default username and password will be used for the connection.
     * 
     * The connection will be initialized by calling initializeConnection.
     * A sub-class may provide an implementation of initializeConnection
     * to ensure its connections are in a consistent state that is different
     * to the default.
     *
     * @return connection to default database.
     * @see TestConfiguration#openDefaultConnection()
     * @see BaseJDBCTestCase#initializeConnection(Connection)
     */
    public Connection openDefaultConnection()
        throws SQLException {
//IC see: https://issues.apache.org/jira/browse/DERBY-1555
        Connection conn =  getTestConfiguration().openDefaultConnection();
        addConnection(conn);
        initializeConnection(conn);
        return conn;
    }


    /**
     * Open a connection to the default database for the given configuration.
     * If the database does not exist, it will be created.  A default username
     * and password will be used for the connection.
     *
     * The connection will be initialized by calling initializeConnection.
     * A sub-class may provide an implementation of initializeConnection
     * to ensure its connections are in a consistent state that is different
     * to the default.
     * @param tc test configuration to use
     * @return connection to default database for the configuration
     * @see TestConfiguration#openDefaultConnection()
     * @see BaseJDBCTestCase#initializeConnection(Connection)
     */
    public Connection openDefaultConnection(TestConfiguration tc)
//IC see: https://issues.apache.org/jira/browse/DERBY-4741
        throws SQLException {
        Connection conn =  tc.openDefaultConnection();
        addConnection(conn);
        initializeConnection(conn);
        return conn;
    }

    /**
     * Open a connection to the current default database using the
     * specified user name and password.
     * <BR>
     * This connection is not
     * automatically closed on tearDown, the test fixture must
     * ensure the connection is closed.
     * 
     * The connection will be initialized by calling initializeConnection.
     * A sub-class may provide an implementation of initializeConnection
     * to ensure its connections are in a consistent state that is different
     * to the default.
     * @see BaseJDBCTestCase#initializeConnection(Connection)
     */
    public Connection openDefaultConnection(String user, String password)
    throws SQLException
    {
        Connection conn =  getTestConfiguration().openDefaultConnection(user,
                password);
        addConnection(conn);
        initializeConnection(conn);
        return conn;        
    }
    
    /**
     * Open a connection to the current default database using the
     * specified user name. The password is a function of
     * the user name and the password token setup by the
     * builtin authentication decorators.
     * <BR>
     * If the fixture is not wrapped in one of the decorators
     * that setup BUILTIN authentication then the password
     * is a function of the user name and the empty string
     * as the password token. This mode is not recommended.
     * 
     * <BR>
     * This connection is not
     * automaticaly closed on tearDown, the test fixture must
     * ensure the connection is closed.
     * <BR>
     * The connection will be initialized by calling initializeConnection.
     * A sub-class may provide an implementation of initializeConnection
     * to ensure its connections are in a consistent state that is different
     * to the default.
     * 
     * @see DatabasePropertyTestSetup#builtinAuthentication(Test, String[], String)
     * @see TestConfiguration#sqlAuthorizationDecorator(Test, String[], String)
     * @see BaseJDBCTestCase#initializeConnection(Connection)
     */
    public Connection openUserConnection(String user) throws SQLException
    {
        return openDefaultConnection(user,
                getTestConfiguration().getPassword(user));
    }
    
    /**
     * Open a connection to the specified database.
     * If the database does not exist, it will be created.
     * A default username and password will be used for the connection.
     * Requires that the test has been decorated with a
     * additionalDatabaseDecorator with the matching name.
     * <BR>
     * The connection will be initialized by calling initializeConnection.
     * A sub-class may provide an implementation of initializeConnection
     * to ensure its connections are in a consistent state that is different
     * to the default.
     * @return connection to default database.
     * @see TestConfiguration#additionalDatabaseDecorator(Test, String)
     * @see BaseJDBCTestCase#initializeConnection(Connection)
     */
    public Connection openConnection(String databaseName)
//IC see: https://issues.apache.org/jira/browse/DERBY-2087
        throws SQLException {
        Connection conn =  getTestConfiguration().openConnection(databaseName);
//IC see: https://issues.apache.org/jira/browse/DERBY-6150
//IC see: https://issues.apache.org/jira/browse/DERBY-6150
//IC see: https://issues.apache.org/jira/browse/DERBY-6150
//IC see: https://issues.apache.org/jira/browse/DERBY-6150
        addConnection(conn);
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
//IC see: https://issues.apache.org/jira/browse/DERBY-1609
        throws UnsupportedEncodingException, SQLException
    {
        // Sink output.
//IC see: https://issues.apache.org/jira/browse/DERBY-5771
        OutputStream sink = new TestNullOutputStream();
        
        // Use the same encoding as the input for the output.    
//IC see: https://issues.apache.org/jira/browse/DERBY-1555
        return ij.runScript(getConnection(), script, encoding,
                sink, encoding);       
    }
    
    /**
     * Run a SQL script through ij discarding the output
     * using this object's default connection. Intended for
     * setup scripts.
     * @return Number of errors executing the script
     * @throws UnsupportedEncodingException 
     * @throws PrivilegedActionException
     * @throws SQLException 
     */
    public int runScript(String resource,String encoding)
//IC see: https://issues.apache.org/jira/browse/DERBY-2055
        throws UnsupportedEncodingException, SQLException,
        PrivilegedActionException,IOException
    {
        
        URL sql = getTestResource(resource);
        assertNotNull("SQL script missing: " + resource, sql);
        InputStream sqlIn = openTestResource(sql);
        Connection conn = getConnection();
        int numErrors = runScript(sqlIn,encoding);
        sqlIn.close();
        
        if (!conn.isClosed() && !conn.getAutoCommit())
            conn.commit();
        
        return numErrors;
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
//IC see: https://issues.apache.org/jira/browse/DERBY-1555
         return TestConfiguration.getCurrent().getJDBCClient().isEmbedded();
     }
    
    /**
    * Tell if the client is DerbyNetClient.
    *
    * @return <code>true</code> if using the DerbyNetClient client
    *         <code>false</code> otherwise.
    */
    public static boolean usingDerbyNetClient() {
//IC see: https://issues.apache.org/jira/browse/DERBY-1555
        return TestConfiguration.getCurrent().getJDBCClient().isDerbyNetClient();
    }
    
    /**
    * Tell if the client is DB2Client.
    *
    * @return <code>true</code> if using the DB2 client driver,
    *         <code>false</code> otherwise.
    */
    public static boolean usingDB2Client() {
//IC see: https://issues.apache.org/jira/browse/DERBY-1555
        return TestConfiguration.getCurrent().getJDBCClient().isDB2Client();
    }
    
    /**
     * Get the value of a database property using the default connection 
     * @param propertyName Property key
     * @return null if the property is not set at the database level,
     * otherwise the value of the property.
     * @throws SQLException
     */
    public String getDatabaseProperty(String propertyName) throws SQLException
    {
        PreparedStatement ps =  prepareStatement(
             "VALUES SYSCS_UTIL.SYSCS_GET_DATABASE_PROPERTY(?)");
        
        ps.setString(1, propertyName);
        ResultSet rs = ps.executeQuery();
        
        rs.next();

        String val = rs.getString(1);
//IC see: https://issues.apache.org/jira/browse/DERBY-5683

        rs.close();
        closeStatement(ps);

        return val;
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
//IC see: https://issues.apache.org/jira/browse/DERBY-1474
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
        
        // wrap buffered stream around the binary stream
//IC see: https://issues.apache.org/jira/browse/DERBY-378
        is1 = new BufferedInputStream(is1);
        is2 = new BufferedInputStream(is2);
 
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
//IC see: https://issues.apache.org/jira/browse/DERBY-1474
            throws IOException, SQLException {
        if (c1 == null || c2 == null) {
            assertNull("Clob c2 is null, c1 is not", c1);
            assertNull("Clob c1 is null, c2 is not", c2);
            return;
        }
        assertEquals("Clobs have different lengths",
                     c1.length(), c2.length());
        Reader r1 = c1.getCharacterStream();
        assertNotNull(r1); // java.sql.Blob object cannot represent NULL
        Reader r2 = c2.getCharacterStream();
        assertNotNull(r2); // java.sql.Blob object cannot represent NULL

        // wrap buffered reader around the character stream
//IC see: https://issues.apache.org/jira/browse/DERBY-378
        r1 = new BufferedReader(r1);
        r2 = new BufferedReader(r2);

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
     * Assert equality between two <code>java.sql.Time</code> objects.
     * If both input references are <code>null</code>, they are considered
     * equal.
     *
     * @param msg String with message to supply with AssertionFailedError
     * @param t1 first java.sql.Time object.
     * @param t2 second java.sql.Time object.
     * @throws AssertionFailedError if Time objects are not equal.
     */
    public static void assertEquals(String msg, Time t1, Time t2) {
//IC see: https://issues.apache.org/jira/browse/DERBY-3224
//IC see: https://issues.apache.org/jira/browse/DERBY-3176
        if(null == t1 && null == t2) {
            return;
        }
        assertNotNull(msg, t1);
        assertNotNull(msg, t2);
        assertEquals(msg, t1.toString(), t2.toString());
    }
    
    /**
     * Assert that SQLState is as expected.  If the SQLState for
     * the top-level exception doesn't match, look for nested
     * exceptions and, if there are any, see if they have the
     * desired SQLState.
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
//IC see: https://issues.apache.org/jira/browse/DERBY-1361
        assertNotNull("Exception cannot be null when asserting on SQLState", 
                      exception);
        
        try {
//IC see: https://issues.apache.org/jira/browse/DERBY-1149
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
            e.initCause(exception);
//IC see: https://issues.apache.org/jira/browse/DERBY-5540

//IC see: https://issues.apache.org/jira/browse/DERBY-3763
            if (usingDB2Client())
            {
                /* For JCC the error message is a series of tokens representing
                 * different things like SQLSTATE, SQLCODE, nested SQL error
                 * message, and nested SQL state.  Based on observation it
                 * appears that the last token in the message is the SQLSTATE
                 * of the nested exception, and it's preceded by a colon.
                 * So using that (hopefully consistent?) rule, try to find
                 * the target SQLSTATE.
                 */
                String msg = exception.getMessage();
                if (!msg.substring(msg.lastIndexOf(":")+1)
                    .trim().equals(expected))
                {
                    throw e;
                }
            }
            else
            {
                // Check nested exceptions to see if any of them is
                // the one we're looking for.
                exception = exception.getNextException();
                if (exception != null)
                    assertSQLState(message, expected, exception);
                else
                    throw e;
            }
        }
    }

    /**
     * Assert that a warning is chained to the connection.
     *
     * @param conn  the connection
     * @param expected the expected SQLState of the warning
     */
    public static void assertWarning( Connection conn, String expected )
        throws SQLException
    {
        SQLWarning  firstWarning = conn.getWarnings();
        assertNotNull( firstWarning );

        for ( SQLWarning warning = firstWarning; warning != null; warning = warning.getNextWarning() )
        {
            if ( expected.equals( warning.getSQLState() ) ) { return; }
        }

        fail( "Expected to see a SQLWarning with the SQLState " + expected );
    }


    /**
     * Assert that SQLState is as expected.
     *
     * @param expected the expected SQLState.
     * @param exception the exception to check the SQLState of.
     */
    public static void assertSQLState(String expected, SQLException exception) {
//IC see: https://issues.apache.org/jira/browse/DERBY-1534
        assertSQLState("Unexpected SQL state.", expected, exception);
    }

    /**
     * Assert that the error code is as expected.
     *
     * @param expected the expected error code
     * @param exception the exception to check
     * @throws AssertionFailedError if the error code is wrong
     */
    public static void assertErrorCode(int expected, SQLException exception) {
//IC see: https://issues.apache.org/jira/browse/DERBY-2601
        assertNotNull("Exception should not be null", exception);
        int actual = exception.getErrorCode();
        if (actual != expected) {
            fail("Expected error code " + expected + ", got " + actual,
                 exception);
        }
    }

    /**
     * Assert that the SQL statement does not compile and throws
     * a SQLException with the expected state.
     * 
     * @param sqlState expected sql state.
     * @param sql the SQL to compile.
     */
    public void assertCompileError(String sqlState, String sql) {

        try {
//IC see: https://issues.apache.org/jira/browse/DERBY-2907
            PreparedStatement pSt = prepareStatement(sql);
//IC see: https://issues.apache.org/jira/browse/DERBY-3763
            if (usingDB2Client())
            {
                /* For JCC the prepares are deferred until execution,
                 * so we have to actually execute in order to see the
                 * expected error.  Note that we don't need to worry
                 * about binding the parameters (if any); the compile
                 * error should occur before the execution-time error
                 * about unbound parameters.
                 */
                try {
//IC see: https://issues.apache.org/jira/browse/DERBY-1976
                    pSt.execute();
                } finally {
                    pSt.close();
                }
            }
            fail("expected compile error: " + sqlState);
        } catch (SQLException se) {
            assertSQLState(sqlState, se);
        }
    }
    
    /**
     * Check the table using SYSCS_UTIL.SYSCS_CHECK_TABLE.
     */
    public void assertCheckTable(String table) throws SQLException
    {
        PreparedStatement ps = prepareStatement(
                "VALUES SYSCS_UTIL.SYSCS_CHECK_TABLE(?, ?)");
        
        ps.setString(1, getTestConfiguration().getUserName());
        ps.setString(2, table);
        
        ResultSet rs = ps.executeQuery();
        JDBC.assertSingleValueResultSet(rs, "1");
        ps.close();
    }
    
    /**
     * Assert that the number of rows in a table is an expected value.
     * Query uses a SELECT COUNT(*) FROM "table".
     * 
     * @param table Name of table in current schema, will be quoted
     * @param rowCount Number of rows expected in the table
     * @throws SQLException Error accessing the database.
     */
    protected void assertTableRowCount(String table, int rowCount) throws SQLException
    {
        assertEscapedTableRowCount(JDBC.escape(table), rowCount);
    }

    /**
     * Assert that the number of rows in a table is an expected value.
     * Query uses a SELECT COUNT(*) FROM table.
     * 
     * @param escapedTableName Escaped name of table, will be used as-is.
     * @param rowCount Number of rows expected in the table
     * @throws SQLException Error accessing the database.
     */
    private void assertEscapedTableRowCount(String escapedTableName, int rowCount)
       throws SQLException
    {
    
        Statement s = createStatement();
        ResultSet rs = s.executeQuery(
                "SELECT COUNT(*) FROM " + escapedTableName);
        rs.next();
        assertEquals(escapedTableName + " row count:",
            rowCount, rs.getInt(1));
        rs.close();
        s.close();
    }

    /**
     * Clear the contents of the statement cache. Useful if a test case
     * needs to make sure that a statement is actually compiled and not
     * just fetched from the statement cache.
     *
     * @throws SQLException if a database error happens
     */
    protected final void emptyStatementCache() throws SQLException {
        Statement s = createStatement();
        s.execute("CALL SYSCS_UTIL.SYSCS_EMPTY_STATEMENT_CACHE()");
        closeStatement(s);
    }

    /**
     * Execute a DROP TABLE command using the passed in tableName as-is
     * and the default connection.
     * If the DROP TABLE fails because the table does not exist then
     * the exception is ignored.
     * @param tableName Table to be dropped.
     * @throws SQLException
     */
    public final void dropTable(String tableName) throws SQLException
    {
       dropTable(getConnection(), tableName);
    }
    
    /**
     * Execute a DROP TABLE command using the passed in tableName as-is.
     * If the DROP TABLE fails because the table does not exist then
     * the exception is ignored.
     * @param conn Connection to execute the DROP TABLE
     * @param tableName Table to be dropped.
     * @throws SQLException
     */
    public static void dropTable(Connection conn, String tableName) throws SQLException
    {
        Statement statement = conn.createStatement();
        String dropSQL = "DROP TABLE " + tableName;
        try { 
            
            statement.executeUpdate(dropSQL); 
        } catch (SQLException e) {
            assertSQLState("42Y55", e);
        }
        finally {
            statement.close();
        }
    }

    /**
     * Execute a DROP VIEW command using the passed in viewName as-is
     * and the default connection.
     * If the DROP VIEW fails because the view does not exist then
     * the exception is ignored.
     * @param viewName Table to be dropped.
     * @throws SQLException
     */
    public final void dropView(String viewName) throws SQLException
    {
       dropView(getConnection(), viewName);
    }
    
    /**
     * Execute a DROP VIEW command using the passed in viewName as-is.
     * If the DROP VIEW fails because the view does not exist then
     * the exception is ignored.
     * @param conn Connection to execute the DROP VIEW
     * @param viewName Table to be dropped.
     * @throws SQLException
     */
    public static void dropView(Connection conn, String viewName) throws SQLException
    {
        Statement statement = conn.createStatement();
        String dropSQL = "DROP VIEW " + viewName;
        try { 
            
            statement.executeUpdate(dropSQL); 
        } catch (SQLException e) {
            assertSQLState("42Y55", e);
        }
        finally {
            statement.close();
        }
    }

    /**
     * Assert that the query fails (either in compilation,
     * execution, or retrieval of results--doesn't matter)
     * and throws a SQLException with the expected states.
     *
     * Assumption is that 'query' does *not* have parameters
     * that need binding and thus can be executed using a
     * simple Statement.execute() call.
     *
     * If there are extra chained SQLExceptions that are 
     * not in sqlStates, this method will not fail.
     * 
     * @param sqlStates  expected sql states.
     * @param st Statement object on which to execute.
     * @param query the query to compile and execute.
     */
    public static void assertStatementError(String[] sqlStates,
//IC see: https://issues.apache.org/jira/browse/DERBY-4928
            Statement st, String query) {
        assertStatementErrorMinion(sqlStates, ORDERED, st, query);
    }

    /**
     * Assert that the query fails (either in compilation,
     * execution, or retrieval of results--doesn't matter)
     * and throws a SQLException with the expected states.
     *
     * Assumption is that 'query' does *not* have parameters
     * that need binding and thus can be executed using a
     * simple Statement.execute() call.
     *
     * If there are extra chained SQLExceptions that are
     * not in sqlStates, this method will not fail.
     *
     * @param sqlStates  expected sql states.
     * @param st Statement object on which to execute.
     * @param query the query to compile and execute.
     */
    public static void assertStatementErrorUnordered(String[] sqlStates,
            Statement st, String query) {
        assertStatementErrorMinion(sqlStates, UNORDERED, st, query);
    }

    /**
     * Asserts that the given statement fails (compilation, execution or
     * retrieval of results) and throws an {@code SQLException} with the
     * expected (chained) states.
     *
     * @param sqlStates the expected states
     * @param orderedStates whether or not the states are expected in the
     *      specified order or not
     * @param st the statement used to execute the query
     * @param query the query to execute
     */
    private static void assertStatementErrorMinion(
            String[] sqlStates, boolean orderedStates,
            Statement st, String query) {
//IC see: https://issues.apache.org/jira/browse/DERBY-5840
        ArrayList<String> statesBag = null;
        if (!orderedStates) {
            statesBag = new ArrayList<String>(Arrays.asList(sqlStates));
        }
        try {
            boolean haveRS = st.execute(query);
            fetchAndDiscardAllResults(st, haveRS);
//IC see: https://issues.apache.org/jira/browse/DERBY-3520
            String errorMsg = "Expected error(s) '" ;
            for (int i = 0; i < sqlStates.length;i++)
                errorMsg += " " + sqlStates[i];
            errorMsg += "' but no error was thrown.";            
            fail(errorMsg);            
        } catch (SQLException se) {
            int count = 0;
            do {
//IC see: https://issues.apache.org/jira/browse/DERBY-4928
                if (orderedStates) {
                    assertSQLState(sqlStates[count], se);
                } else {
                    String state = se.getSQLState();
                    assertTrue("Unexpected state: " + state,
                            statesBag.remove(state));
                    // Run through assertSQLStates too, to catch invalid states.
                    assertSQLState(state, se);
                }
                count++;
                se = se.getNextException();
            } while (se != null && count < sqlStates.length);
            // We must have at least as many exceptions as 
            // we expected.
            assertEquals("Got " +
                    count + " exceptions. Expected at least"+
                    sqlStates.length,count,sqlStates.length);
            
        }
    }

    /**
     * Assert that the query fails with a single error
     *  
     * @param sqlState  Expected SQLState of exception
     * @param st         
     * @param query
     * @see #assertStatementError(String[], Statement, String)
     */
    public static void assertStatementError(String sqlState, Statement st, String query) {
        assertStatementError(new String[] {sqlState},st,query);
    }
        
    /**
     * Assert that that a commit fails with the given error
     * @param sqlState state
     * @param c        the connection
     */
    public static void assertCommitError(String sqlState, Connection c) {
//IC see: https://issues.apache.org/jira/browse/DERBY-532
        try {
            c.commit();
            fail();
        } catch (SQLException e) {
            assertSQLState(sqlState, e);
        }
    }
   
    /**
     * Assert that the query fails (either in compilation,
     * execution, or retrieval of results--doesn't matter)
     * and throws a SQLException with the expected state
     * and error code
     *
     * Assumption is that 'query' does *not* have parameters
     * that need binding and thus can be executed using a
     * simple Statement.execute() call.
     * 
     * @param sqlState expected sql state.
     * @param errorCode expected error code.
     * @param st Statement object on which to execute.
     * @param query the query to compile and execute.
     */
    public static void assertStatementError(String sqlState, int errorCode, Statement st, String query) {
//IC see: https://issues.apache.org/jira/browse/DERBY-2567
        try {
            boolean haveRS = st.execute(query);
            fetchAndDiscardAllResults(st, haveRS);
            fail("Expected error '" + sqlState +
                "' but no error was thrown.");
        } catch (SQLException se) {
            assertSQLState(sqlState, se);
//IC see: https://issues.apache.org/jira/browse/DERBY-2601
            assertErrorCode(errorCode, se);
        }
        
    }

    /**
     * Assert that the query fails (either in execution, or retrieval of
     * results--doesn't matter) and throws a SQLException with the expected
     * state and error code
     *
     * Parameters must have been already bound, if any.
     *
     * @param sqlState expected sql state.
     * @param ps PreparedStatement query object to execute.
     */
    public static void assertPreparedStatementError(String sqlState,
//IC see: https://issues.apache.org/jira/browse/DERBY-4208
                                                    PreparedStatement ps) {
        try {
            boolean haveRS = ps.execute();
            fetchAndDiscardAllResults(ps, haveRS);
            fail("Expected error '" + sqlState +
                "' but no error was thrown.");
        } catch (SQLException se) {
            assertSQLState(sqlState, se);
        }
    }

    /**
     * Assert that execution of the received PreparedStatement
     * object fails (either in execution or when retrieving
     * results) and throws a SQLException with the expected
     * state.
     * 
     * Assumption is that "pSt" is either a PreparedStatement
     * or a CallableStatement that has already been prepared
     * and whose parameters (if any) have already been bound.
     * Thus the only thing left to do is to call "execute()"
     * and look for the expected SQLException.
     * 
     * @param sqlState expected sql state.
     * @param pSt A PreparedStatement or CallableStatement on
     *  which to call "execute()".
     */
    public static void assertStatementError(String sqlState,
        PreparedStatement pSt)
    {
        try {
            boolean haveRS = pSt.execute();
            fetchAndDiscardAllResults(pSt, haveRS);
            fail("Expected error '" + sqlState +
                "' but no error was thrown.");
        } catch (SQLException se) {
            assertSQLState(sqlState, se);
        }
    }


    /**
     * Executes the Callable statement that is expected to fail and verifies
     * that it throws the expected SQL exception.
     * @param expectedSE The expected SQL exception
     * @param callSQL The SQL to execute
     * @throws SQLException
     */
    public void assertCallError(String expectedSE, String callSQL)
    throws SQLException
    {
        try {
            CallableStatement cs = prepareCall(callSQL);
            cs.execute();
            fail("FAIL - SQL expected to throw exception");
        } catch (SQLException se) {
            assertSQLState(expectedSE, se);
        }
    }
    /**
     * Perform a fetch on the ResultSet with an expected failure
     * 
     * @param sqlState Expected SQLState
     * @param rs   ResultSet upon which next() will be called
     */
    public static void assertNextError(String sqlState,ResultSet rs)
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-2299
    	try {
    		rs.next();
    		fail("Expected error on next()");
    	}catch (SQLException se){
    		assertSQLState(sqlState,se);
    	}
    }
    
    /**
     * Perform getInt(position) with expected error
     * @param position  position argument to pass to getInt
     * @param sqlState  sqlState of expected error
     * @param rs ResultSet upon which to call getInt(position)
     */
    public static void assertGetIntError(int position, String sqlState, ResultSet rs)
    {
    	try {
    		rs.getInt(position);
    		fail("Expected exception " + sqlState);
    	} catch (SQLException se){
    		assertSQLState(sqlState,se);
    	}
    			
    	
    }
    /**
     * Take a Statement object and a SQL statement, execute it
     * via the "executeUpdate()" method, and assert that the
     * resultant row count matches the received row count.
     *
     * Assumption is that 'sql' does *not* have parameters
     * that need binding and that it can be executed using a
     * simple Statement.executeUpdate() call.
     * 
     * @param st Statement object on which to execute.
     * @param expectedRC Expected row count.
     * @param sql SQL to execute.
     */
    public static void assertUpdateCount(Statement st,
        int expectedRC, String sql) throws SQLException
    {
        assertEquals("Update count does not match:",
//IC see: https://issues.apache.org/jira/browse/DERBY-2907
            expectedRC, st.executeUpdate(sql));
    }

    /**
     * Assert that a call to "executeUpdate()" on the received
     * PreparedStatement object returns a row count that matches
     * the received row count.
     *
     * Assumption is that "pSt" is either a PreparedStatement
     * or a CallableStatement that has already been prepared
     * and whose parameters (if any) have already been bound.
     * Also assumes the statement's SQL is such that a call
     * executeUpdate() is allowed.  Thus the only thing left
     * to do is to call the "executeUpdate" method.
     * 
     * @param pSt The PreparedStatement on which to execute.
     * @param expectedRC The expected row count.
     */
    public static void assertUpdateCount(PreparedStatement pSt,
        int expectedRC) throws SQLException
    {
        assertEquals("Update count does not match:",
            expectedRC, pSt.executeUpdate());
    }

    /**
     * Get the last SQLException in chain.
     * @param sqle <code>SQLException</code>
     * @return the last exception in the chain.
     */
    public SQLException getLastSQLException(SQLException sqle) {
//IC see: https://issues.apache.org/jira/browse/DERBY-2567
        SQLException current = sqle;
        SQLException next = sqle.getNextException();
        while (next != null) {
            current = next;
            next = next.getNextException();
        }
        return current;
    }

    /**
     * Take the received Statement--on which a query has been
     * executed--and fetch all rows of all result sets (if any)
     * returned from execution.  The rows themselves are
     * discarded.  This is useful when we expect there to be
     * an error when processing the results but do not know
     * (or care) at what point the error occurs.
     *
     * @param st An already-executed statement from which
     *  we get the result set to process (if there is one).
     * @param haveRS Whether or not the the statement's
     *  first result is a result set (as opposed to an
     *  update count).
     */
    private static void fetchAndDiscardAllResults(Statement st,
        boolean haveRS) throws SQLException
    {
        ResultSet rs = null;
        while (haveRS || (st.getUpdateCount() != -1))
        {
            // If we have a result set, iterate through all
            // of the rows.
            if (haveRS)
                JDBC.assertDrainResults(st.getResultSet(), -1);
            haveRS = st.getMoreResults();
        }
    }

    /**
     * Assert that the two (2) passed-in SQLException's are equals and
     * not just '=='.
     *
     * @param se1 first SQLException to compare
     * @param se2 second SQLException to compare
     */
    public static void assertSQLExceptionEquals(SQLException se1,
                                                SQLException se2) {
        // Ensure non-null SQLException's are being passed.
        assertNotNull(
            "Passed-in SQLException se1 cannot be null",
            se1);
        assertNotNull(
            "Passed-in SQLException se2 cannot be null",
            se2);

        // Now verify that the passed-in SQLException's are of the same type
        assertEquals("SQLException class types are different",
                     se1.getClass().getName(), se2.getClass().getName());

        // Here we check that the detailed message of both
        // SQLException's is the same
        assertEquals(
                "Detailed messages of the SQLException's are different",
                 se1.getMessage(), se2.getMessage());

        // Check that getCause() returns the same value on the two exceptions.
//IC see: https://issues.apache.org/jira/browse/DERBY-5540
        Throwable se1Cause = se1.getCause();
        Throwable se2Cause = se2.getCause();
        if (se1Cause == null) {
            assertNull(se2Cause);
        } else {
            assertThrowableEquals(se1Cause, se2Cause);
        }

        // Check that the two exceptions have the same next exception.
        if (se1.getNextException() == null) {
            assertNull(se2.getNextException());
        } else {
            assertSQLExceptionEquals(se1.getNextException(),
                                     se2.getNextException());
        }
    }

    /**
     * Compares two JDBC types to see if they are equivalent.
     * DECIMAL and NUMERIC and DOUBLE and FLOAT are considered
     * equivalent.
     * @param expectedType Expected jdbctype from java.sql.Types
     * @param type         Actual type from metadata
     */
    public static void assertEquivalentDataType(int expectedType, int type)
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-2333
     if (expectedType == type)
         return;
     if (expectedType == java.sql.Types.DECIMAL && 
                 type == java.sql.Types.NUMERIC)
         return;
     if (expectedType == java.sql.Types.NUMERIC && 
             type == java.sql.Types.DECIMAL)
         return;
     if (expectedType == java.sql.Types.DOUBLE && 
                 type == java.sql.Types.FLOAT)
         return;
     if (expectedType == java.sql.Types.FLOAT && 
             type == java.sql.Types.DOUBLE)
     return;
     fail("types:" + expectedType + " and " + type + " are not equivalent");
     
    }
  
    /**
     * Attempts to obtain the client-side transaction counter from the given
     * connection, which is internal state information.
     * <p>
     * <em>NOTE:</em> Use with care, accesses internal state.
     *
     * @param conn the connection
     * @return Internal client transaction id.
     * @throws SQLException if the given connection is an embedded connection,
     *      or if invoking the required method fails
     **/
    public static int getClientTransactionID(Connection conn)
            throws SQLException {
        try {
            Method m = conn.getClass().getMethod(
                    "getTransactionID", new Class[] {});
            return ((Integer) m.invoke(conn, new Object[] {} )).intValue();
        } catch (Exception e) {
            SQLException se = new SQLException(e.getMessage());
            se.initCause(e);
            throw se;
        }
    }

    /**
     * Return estimated row count for runtime statistics.  
     * Requires caller first turned on RuntimeStatistics, executed a query and closed the ResultSet.
     * 
     * For client calls we just return as we can't find out this information.
     * @param conn
     * @param expectedCount
     * @throws SQLException
     */
    public static void checkEstimatedRowCount(Connection conn, double expectedCount) throws SQLException {
//IC see: https://issues.apache.org/jira/browse/DERBY-6751
        if (! (conn instanceof EmbedConnection))
	    { return; }
	
        EmbedConnection econn = (EmbedConnection) conn;
        LanguageConnectionContext   lcc = (LanguageConnectionContext) getLanguageConnectionContext( econn );
        RunTimeStatistics rts = lcc.getRunTimeStatisticsObject();
        assertNotNull(" RuntimeStatistics is null. Did you call SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1)?",rts);
        assertEquals((long) expectedCount, (long) rts.getEstimatedRowCount());
	}

    /**
     * Check consistency of all tables
     * 
     * @param conn
     * @throws SQLException
     */
    protected void  checkAllConsistency(
            Connection  conn)
    throws SQLException
    {
        Statement s = createStatement();

        ResultSet rs = 
            s.executeQuery(
                    "select schemaname, tablename, SYSCS_UTIL.SYSCS_CHECK_TABLE(schemaname, tablename) " + 
            "from sys.systables a,  sys.sysschemas b where a.schemaid = b.schemaid");

        int table_count = 0;

        while (rs.next())
        {
            table_count++;

            if (rs.getInt(3) != 1)
            {
                assertEquals("Bad return from consistency check of " +
                        rs.getString(1) + "." + rs.getString(2),1,rs.getInt(3));

            }
        }
        assertTrue("Something wrong with consistency check query, found only " +
                table_count + " tables.",table_count >= 5);

        rs.close();
        s.close();

        conn.commit();
    }

    protected static void dumpRs(ResultSet s, PrintStream out)
//IC see: https://issues.apache.org/jira/browse/DERBY-6378
            throws SQLException
    {
        if (s == null) {
            out.println("<NULL>");
            return;
        }

        ResultSetMetaData rsmd = s.getMetaData();

        // Get the number of columns in the result set
        int numCols = rsmd.getColumnCount();

        if (numCols <= 0) {
            out.println("(no columns!)");
            return;
        }

        StringBuilder heading = new StringBuilder("\t ");
        StringBuilder underline = new StringBuilder("\t ");

        int len;
        // Display column headings
        for (int i=1; i<=numCols; i++) {
            if (i > 1) {
                heading.append(",");
                underline.append(" ");
            }

            len = heading.length();
            heading.append(rsmd.getColumnLabel(i));
            len = heading.length() - len;

            for (int j = len; j > 0; j--) {
                underline.append("-");
            }
        }

        out.println(heading.toString());
        out.println(underline.toString());


        StringBuilder row = new StringBuilder();
        // Display data, fetching until end of the result set
        while (s.next()) {
            row.append("\t{");
            // Loop through each column, getting the
            // column data and displaying
            for (int i=1; i<=numCols; i++) {
                if (i > 1) row.append(",");
                row.append(s.getString(i));
            }

            row.append("}\n");
        }

        out.println(row.toString());
        s.close();
    }

    public static void dumpRs(ResultSet s) throws SQLException {
        dumpRs(s, System.out);
    }

    // helper methods moved from GeneratedColumnsHelper

    /**
     * Run good DDL.
     * @throws SQLException 
     */
    protected void    goodStatement( Connection conn, String command ) throws SQLException
    {
        PreparedStatement    ps = chattyPrepare( conn, command );
//IC see: https://issues.apache.org/jira/browse/DERBY-6429

        ps.execute();
        ps.close();
    }
    
    /**
     * Run a good update statement with an expected row count.
     * @throws SQLException 
     */
    protected void    goodUpdate( Connection conn, String update, int expectedRowCount ) throws SQLException
    {
        PreparedStatement    ps = chattyPrepare( conn, update );

        int actualRowCount = ps.executeUpdate();
        ps.close();

        println( "Expecting to touch " + expectedRowCount + " rows." );
        assertEquals( expectedRowCount, actualRowCount );
    }
    
    /**
     * Assert that the statement returns the correct results.
     */
    protected void assertResults( Connection conn, String query, String[][] rows, boolean trimResults )
//IC see: https://issues.apache.org/jira/browse/DERBY-532
        throws SQLException
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-6511
        PreparedStatement   ps = chattyPrepare( conn, query );
        ResultSet                   rs = ps.executeQuery();

        assertResults( rs, rows, trimResults );

        rs.close();
        ps.close();
    }
        
    /**
     * Assert that the ResultSet returns the desired rows.
     */
    protected void assertResults( ResultSet rs, String[][] rows, boolean trimResults )
//IC see: https://issues.apache.org/jira/browse/DERBY-532
        throws SQLException
    {
        int     rowCount = rows.length;

        for ( int i = 0; i < rowCount; i++ )
        {
            String[]    row = rows[ i ];
            int             columnCount = row.length;

            assertTrue( rs.next() );

            for ( int j = 0; j < columnCount; j++ )
            {
                String  expectedValue =  row[ j ];
                //println( "(row, column ) ( " + i + ", " +  j + " ) should be " + expectedValue );
                String  actualValue = null;
                int         column = j+1;

                actualValue = rs.getString( column );
                if ( rs.wasNull() ) { actualValue = null; }

                if ( (actualValue != null) && trimResults ) { actualValue = actualValue.trim(); }
                
                assertEquals( (expectedValue == null), rs.wasNull() );
                
                if ( expectedValue == null )    { assertNull( actualValue ); }
                else { assertEquals(expectedValue, actualValue); }
            }
        }

        assertFalse( rs.next() );
    }

	protected	static	ResultSet	executeQuery( Statement stmt, String text )
		throws SQLException
	{
		println( "Executing '" + text + "'" );

        return stmt.executeQuery( text );
	}

    /**
     * Prepare a statement and report its sql text.
     */
    protected PreparedStatement   chattyPrepare( Connection conn, String text )
        throws SQLException
    {
        println( "Preparing statement:\n\t" + text );
        
        return conn.prepareStatement( text );
    }

    /**
     * Prepare a callable statement and report its sql text.
     */
    protected CallableStatement   chattyPrepareCall( Connection conn, String text )
        throws SQLException
    {
        println( "Preparing callable statement:\n\t" + text );
        
        return conn.prepareCall( text );
    }

    /**
     * Assert that the statement text, when compiled, raises an exception
     */
    protected void    expectCompilationError( String sqlState, String query )
    {
        println( "\nExpecting " + sqlState + " when preparing:\n\t" + query );

        assertCompileError( sqlState, query );
    }

    /**
     * Assert that the statement text, when compiled, raises an exception
     */
    protected void    expectCompilationError( Connection conn, String sqlState, String query )
    {
        println( "\nExpecting " + sqlState + " when preparing:\n\t" + query );

        PreparedStatement ps = null;

        try {
            ps = conn.prepareStatement( query );
        } catch (SQLException se )
        {
            assertSQLState( sqlState, se );

            return;
        }

        fail( "Expected SQL state: " + sqlState );
    }

    /**
     * Assert that the statement text, when executed, raises an error.
     */
    protected void    expectExecutionError( Connection conn, String sqlState, String query )
        throws Exception
    {
        println( "\nExpecting " + sqlState + " when executing:\n\t"  );
        PreparedStatement   ps = chattyPrepare( conn, query );

        assertStatementError( sqlState, ps );
        ps.close();
    }
    
	/**
	  * Gets the LanguageConnectionContext for this connection. You might think that
      * this method could take an EmbedConnection as its argument and return a
      * LanguageConnectionContext. That, however, makes the compatibility tests blow up.
      * With those stronger types, the test lookup machinery in junit.framework.TestSuite
      * can't resolve the signature of this private method. That is because the engine jar is
      * not on the client-only classpath used by the compatibility tests. Now you know.
	  */
	private static Object	getLanguageConnectionContext( Connection conn )
	{
//IC see: https://issues.apache.org/jira/browse/DERBY-6751
        final EmbedConnection   econn = (EmbedConnection) conn;
        return AccessController.doPrivileged
            (
             new PrivilegedAction<LanguageConnectionContext>()
             {
                 public LanguageConnectionContext run()
                 {
                     return econn.getLanguageConnection();
                 }
             }
             );
	}


} // End class BaseJDBCTestCase

//IC see: https://issues.apache.org/jira/browse/DERBY-993

