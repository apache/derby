/*
 * Derby - org.apache.derbyTesting.functionTests.tests.jdbc4.ClosedObjectTest
 *
 * Copyright 2006 The Apache Software Foundation or its licensors, as
 * applicable.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 *
 */

package org.apache.derbyTesting.functionTests.tests.jdbc4;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.CallableStatement;
import java.sql.SQLClientInfoException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Properties;
import javax.sql.ConnectionPoolDataSource;
import javax.sql.DataSource;
import javax.sql.PooledConnection;
import javax.sql.XAConnection;
import javax.sql.XADataSource;
import junit.extensions.TestSetup;
import junit.framework.Test;
import junit.framework.TestSuite;
import org.apache.derbyTesting.functionTests.util.BaseJDBCTestCase;
import org.apache.derbyTesting.functionTests.util.TestDataSourceFactory;

/**
 * Test that all methods on <code>ResultSet</code>,
 * <code>Statement</code>, <code>PreparedStatement</code>,
 * <code>CallableStatement</code> and <code>Connection</code> objects
 * throw the appropriate exceptions when the objects are closed.
 */
public class ClosedObjectTest extends BaseJDBCTestCase {
    /** The method to test. */
    private final Method method_;
    /** Test decorator which provides a closed object to invoke a
     * method on. */
    private final ObjectDecorator decorator_;
    /** Name of the test. */
    private String name_;

    /**
     * Creates a new <code>ClosedObjectTest</code> instance.
     *
     * @param method the method to test
     * @param decorator a decorator which provides a closed object
     */
    public ClosedObjectTest(Method method, ObjectDecorator decorator) {
        super("testClosedObjects");
        method_ = method;
        decorator_ = decorator;
        // setting name temporarily, we don't know the real class name yet
        name_ = method.getDeclaringClass().getName() + "." + method.getName();
    }

    /**
     * Gets the name of the test.
     *
     * @return name of the test
     */
    public String getName() {
        return name_;
    }

    /**
     * Runs a test case. A method is called on a closed object, and it
     * is checked that the appropriate exception is thrown.
     *
     * @exception Throwable if an error occurs
     */
    public void testClosedObjects() throws Throwable {
        try {
            Object object = decorator_.getClosedObject();

            // update name of test with real class name
            name_ = object.getClass() + "." + method_.getName();

            method_.invoke(object,
                           getNullArguments(method_.getParameterTypes()));
            assertFalse("No exception was thrown",
                        decorator_.expectsException(method_));
        } catch (InvocationTargetException ite) {
            try {
                throw ite.getCause();
            } catch (SQLFeatureNotSupportedException fnse) {
                // if we don't support the method, it is OK that we
                // throw this exception
            } catch (SQLException sqle) {
                decorator_.checkException(method_, sqle);
            }
        }
    }

    /**
     * Creates the test suite and fills it with tests using
     * <code>DataSource</code>, <code>ConnectionPoolDataSource</code>
     * and <code>XADataSource</code> to obtain objects.
     *
     * @return a <code>Test</code> value
     * @exception Exception if an error occurs while building the test suite
     */
    public static Test suite() throws Exception {
        TestSuite topSuite = new TestSuite();

        TestSuite dsSuite = new TestSuite();
        DataSourceDecorator dsDecorator = new DataSourceDecorator(dsSuite);
        topSuite.addTest(dsDecorator);
        fillDataSourceSuite(dsSuite, dsDecorator);

        TestSuite poolSuite = new TestSuite();
        PoolDataSourceDecorator poolDecorator =
            new PoolDataSourceDecorator(poolSuite);
        topSuite.addTest(poolDecorator);
        fillDataSourceSuite(poolSuite, poolDecorator);

        TestSuite xaSuite = new TestSuite();
        XADataSourceDecorator xaDecorator = new XADataSourceDecorator(xaSuite);
        topSuite.addTest(xaDecorator);
        fillDataSourceSuite(xaSuite, xaDecorator);

        return topSuite;
    }

    /**
     * Fills a test suite which is contained in a
     * <code>DataSourceDecorator</code> with tests for
     * <code>ResultSet</code>, <code>Statement</code>,
     * <code>PreparedStatement</code>, <code>CallableStatement</code>
     * and <code>Connection</code>.
     *
     * @param suite the test suite to fill
     * @param dsDecorator the decorator for the test suite
     * @exception Exception if an error occurs while filling the suite
     */
    private static void fillDataSourceSuite(TestSuite suite,
                                            DataSourceDecorator dsDecorator)
        throws Exception
    {
        TestSuite rsSuite = new TestSuite();
        ResultSetObjectDecorator rsDecorator =
            new ResultSetObjectDecorator(rsSuite, dsDecorator);
        suite.addTest(rsDecorator);
        fillObjectSuite(rsSuite, rsDecorator, ResultSet.class);

        TestSuite stmtSuite = new TestSuite();
        StatementObjectDecorator stmtDecorator =
            new StatementObjectDecorator(stmtSuite, dsDecorator);
        suite.addTest(stmtDecorator);
        fillObjectSuite(stmtSuite, stmtDecorator, Statement.class);

        TestSuite psSuite = new TestSuite();
        PreparedStatementObjectDecorator psDecorator =
            new PreparedStatementObjectDecorator(psSuite, dsDecorator);
        suite.addTest(psDecorator);
        fillObjectSuite(psSuite, psDecorator, PreparedStatement.class);

        TestSuite csSuite = new TestSuite();
        CallableStatementObjectDecorator csDecorator =
            new CallableStatementObjectDecorator(csSuite, dsDecorator);
        suite.addTest(csDecorator);
        fillObjectSuite(csSuite, csDecorator, CallableStatement.class);

        TestSuite connSuite = new TestSuite();
        ConnectionObjectDecorator connDecorator =
            new ConnectionObjectDecorator(connSuite, dsDecorator);
        suite.addTest(connDecorator);
        fillObjectSuite(connSuite, connDecorator, Connection.class);
    }

    /**
     * Fills a suite with tests for all the methods of an interface.
     *
     * @param suite the suite to fill
     * @param decorator a decorator for the test (used for obtaining a
     * closed object to test the method on)
     * @param iface the interface which contains the methods to test
     * @exception Exception if an error occurs while filling the suite
     */
    private static void fillObjectSuite(TestSuite suite,
                                        ObjectDecorator decorator,
                                        Class iface)
        throws Exception
    {
        for (Method m : iface.getMethods()) {
            ClosedObjectTest cot = new ClosedObjectTest(m, decorator);
            suite.addTest(cot);
        }
    }

    /**
     * Takes an array of classes and returns an array of objects with
     * null values compatible with the classes. Helper method for
     * converting a parameter list to an argument list.
     *
     * @param params a <code>Class[]</code> value
     * @return an <code>Object[]</code> value
     */
    private static Object[] getNullArguments(Class[] params) {
        Object[] args = new Object[params.length];
        for (int i = 0; i < params.length; i++) {
            args[i] = getNullValueForType(params[i]);
        }
        return args;
    }

    /**
     * Returns a null value compatible with the class. For instance,
     * return <code>Boolean.FALSE</code> for primitive booleans, 0 for
     * primitive integers and <code>null</code> for non-primitive
     * types.
     *
     * @param type a <code>Class</code> value
     * @return a null value
     */
    private static Object getNullValueForType(Class type) {
        if (!type.isPrimitive()) {
            return null;
        }
        if (type == Boolean.TYPE) {
            return Boolean.FALSE;
        }
        if (type == Character.TYPE) {
            return new Character((char) 0);
        }
        if (type == Byte.TYPE) {
            return new Byte((byte) 0);
        }
        if (type == Short.TYPE) {
            return new Short((short) 0);
        }
        if (type == Integer.TYPE) {
            return new Integer(0);
        }
        if (type == Long.TYPE) {
            return new Long(0L);
        }
        if (type == Float.TYPE) {
            return new Float(0f);
        }
        if (type == Double.TYPE) {
            return new Double(0d);
        }
        fail("Don't know how to handle type " + type);
        return null;            // unreachable statement
    }

    /**
     * Abstract decorator class with functionality for obtaining a
     * closed object.
     */
    private static abstract class ObjectDecorator extends TestSetup {
        /** Decorator which provides a connection. */
        private final DataSourceDecorator decorator_;
        /** The closed object. Must be set by a sub-class. */
        protected Object object_;

        /**
         * Creates a new <code>ObjectDecorator</code> instance.
         *
         * @param test a test or suite to decorate
         * @param decorator a decorator which provides a connection
         */
        public ObjectDecorator(Test test, DataSourceDecorator decorator) {
            super(test);
            decorator_ = decorator;
        }

        /**
         * Returns the closed object.
         *
         * @return a closed object
         */
        public Object getClosedObject() {
            return object_;
        }

        /**
         * Checks whether a method expects an exception to be thrown
         * when the object is closed. Currently, only
         * <code>close()</code>, <code>isClosed()</code> and
         * <code>isValid()</code> don't expect exceptions.
         *
         * @param method a method
         * @return <code>true</code> if an exception is expected
         */
        public boolean expectsException(Method method) {
            final String[] exceptionLessMethods = {
                "close",
                "isClosed",
                "isValid",
            };
            for (String name : exceptionLessMethods) {
                if (name.equals(method.getName())) return false;
            }
            return true;
        }

        /**
         * Checks whether an exception is of the expected type for
         * that method.
         *
         * @param method a method
         * @param sqle an exception
         * @exception SQLException if the exception was not expected
         */
        public final void checkException(Method method, SQLException sqle)
            throws SQLException
        {
            if (!expectsException(method)) {
                throw sqle;
            }
            checkSQLState(method, sqle);
        }

        /**
         * Checks whether the SQL state is as expected.
         *
         * @param method a <code>Method</code> value
         * @param sqle a <code>SQLException</code> value
         * @exception SQLException if an error occurs
         */
        protected abstract void checkSQLState(Method method,
                                              SQLException sqle)
            throws SQLException;

        /**
         * Helper method for creating a connection.
         *
         * @return a connection
         * @exception SQLException if an error occurs
         */
        protected Connection createConnection() throws SQLException {
            return decorator_.newConnection();
        }

        /**
         * Helper method for creating a statement.
         *
         * @return a statement
         * @exception SQLException if an error occurs
         */
        protected Statement createStatement() throws SQLException {
            return decorator_.getConnection().createStatement();
        }

        /**
         * Helper method for creating a prepared statement.
         *
         * @param sql statement text
         * @return a prepared statement
         * @exception SQLException if an error occurs
         */
        protected PreparedStatement prepareStatement(String sql)
            throws SQLException
        {
            return decorator_.getConnection().prepareStatement(sql);
        }

        /**
         * Helper method for creating a callable statement.
         *
         * @param call statement text
         * @return a callable statement
         * @exception SQLException if an error occurs
         */
        protected CallableStatement prepareCall(String call)
            throws SQLException
        {
            return decorator_.getConnection().prepareCall(call);
        }
    }

    /**
     * Decorator class for testing methods on a closed result set.
     */
    private static class ResultSetObjectDecorator extends ObjectDecorator {
        /** Statement used for creating the result set to test. */
        private Statement stmt_;

        /**
         * Creates a new <code>ResultSetObjectDecorator</code> instance.
         *
         * @param test the test to decorate
         * @param decorator decorator used for obtaining a statement
         */
        public ResultSetObjectDecorator(Test test,
                                        DataSourceDecorator decorator) {
            super(test, decorator);
        }

        /**
         * Sets up the test. Creates a result set and closes it.
         *
         * @exception SQLException if an error occurs
         */
        public void setUp() throws SQLException {
            stmt_ = createStatement();
            ResultSet rs = stmt_.executeQuery("VALUES(1)");
            rs.close();
            object_ = rs;
        }

        /**
         * Tears down the test. Closes open resources.
         *
         * @exception SQLException if an error occurs
         */
        public void tearDown() throws SQLException {
            stmt_.close();
        }

        /**
         * Checks whether the exception has the expected SQL state
         * (XCL16 - result set is closed).
         *
         * @param method a <code>Method</code> value
         * @param sqle a <code>SQLException</code> value
         * @exception SQLException if an error occurs
         */
        protected void checkSQLState(Method method, SQLException sqle)
            throws SQLException
        {
            if (sqle.getSQLState().equals("XCL16")) {
                // everything is OK, do nothing
            } else {
                // unexpected exception
                throw sqle;
            }
        }
    }

    /**
     * Decorator class for testing methods on a closed statement.
     */
    private static class StatementObjectDecorator extends ObjectDecorator {
        /**
         * Creates a new <code>StatementObjectDecorator</code> instance.
         *
         * @param test the test to decorate
         * @param decorator decorator which provides a statement
         */
        public StatementObjectDecorator(Test test,
                                        DataSourceDecorator decorator) {
            super(test, decorator);
        }

        /**
         * Sets up the test. Creates a statement and closes it.
         *
         * @exception SQLException if an error occurs
         */
        public void setUp() throws SQLException {
            Statement stmt = createStatement();
            stmt.close();
            object_ = stmt;
        }

        /**
         * Checks whether the exception has the expected SQL state
         * (statement is closed). When using embedded, XJ012 is
         * expected. When using the client driver, XCL31 is expected.
         *
         * @param method a <code>Method</code> value
         * @param sqle a <code>SQLException</code> value
         * @exception SQLException if an error occurs
         */
        protected void checkSQLState(Method method, SQLException sqle)
            throws SQLException
        {
            String sqlState = sqle.getSQLState();
            if (sqlState.equals("XJ012")) {
                // expected, do nothing
            } else {
                // unexpected exception
                throw sqle;
            }
        }
    }

    /**
     * Decorator class for testing methods on a closed prepared statement.
     */
    private static class PreparedStatementObjectDecorator
        extends StatementObjectDecorator
    {
        /**
         * Creates a new <code>PreparedStatementObjectDecorator</code>
         * instance.
         *
         * @param test the test to decorate
         * @param decorator decorator which provides a prepared statement
         */
        public PreparedStatementObjectDecorator(Test test,
                                                DataSourceDecorator decorator)
        {
            super(test, decorator);
        }

        /**
         * Sets up the test. Prepares a statement and closes it.
         *
         * @exception SQLException if an error occurs
         */
        public void setUp() throws SQLException {
            PreparedStatement ps = prepareStatement("VALUES(1)");
            ps.close();
            object_ = ps;
        }

        /**
         * Checks whether the exception has the expected SQL state
         * (statement is closed), or XJ016 indicating it is a
         * Statement method not meant to be invoked on a
         * PreparedStatement.
         *
         * @param method a <code>Method</code> value
         * @param sqle a <code>SQLException</code> value
         * @exception SQLException if an error occurs
         */
        protected void checkSQLState(Method method, SQLException sqle)
            throws SQLException
        {
            if (method.getDeclaringClass() == Statement.class &&
                sqle.getSQLState().equals("XJ016")) {
                // XJ016 is "blah,blah not allowed on a prepared
                // statement", so it's OK to get this one
            } else {
                super.checkSQLState(method, sqle);
            }

        }
    }

    /**
     * Decorator class for testing methods on a closed callable statement.
     */
    private static class CallableStatementObjectDecorator
        extends PreparedStatementObjectDecorator
    {
        /**
         * Creates a new <code>CallableStatementObjectDecorator</code>
         * instance.
         *
         * @param test the test to decorate
         * @param decorator decorator which provides a callable statement
         */
        public CallableStatementObjectDecorator(Test test,
                                                DataSourceDecorator decorator)
        {
            super(test, decorator);
        }

        /**
         * Sets up the test. Prepares a call and closes the statement.
         *
         * @exception SQLException if an error occurs
         */
        public void setUp() throws SQLException {
            CallableStatement cs =
                prepareCall("CALL SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1)");
            cs.close();
            object_ = cs;
        }
    }

    /**
     * Decorator class for testing methods on a closed connection.
     */
    private static class ConnectionObjectDecorator extends ObjectDecorator {
        /**
         * Creates a new <code>ConnectionObjectDecorator</code> instance.
         *
         * @param test the test to decorate
         * @param decorator decorator which provides a connection
         */
        public ConnectionObjectDecorator(Test test,
                                         DataSourceDecorator decorator) {
            super(test, decorator);
        }

        /**
         * Sets up the test. Creates a connection and closes it.
         *
         * @exception SQLException if an error occurs
         */
        public void setUp() throws SQLException {
            Connection conn = createConnection();
            conn.rollback();    // cannot close active transactions
            conn.close();
            object_ = conn;
        }

        /**
         * Checks that the exception has an expected SQL state (08003
         * - no current connection). Also accept
         * <code>SQLClientInfoException</code>s from
         * <code>setClientInfo()</code>.
         *
         * @param method a <code>Method</code> value
         * @param sqle a <code>SQLException</code> value
         * @exception SQLException if an error occurs
         */
        protected void checkSQLState(Method method, SQLException sqle)
            throws SQLException
        {
            if (sqle instanceof SQLClientInfoException &&
                method.getName().equals("setClientInfo") &&
                Arrays.asList(method.getParameterTypes())
                .equals(Arrays.asList(new Class[] { Properties.class }))) {
                // setClientInfo(Properties) should throw
                // ClientInfoException, so this is OK
            } else if (sqle.getSQLState().equals("08003")) {
                // expected, connection closed
            } else {
                // unexpected exception
                throw sqle;
            }
        }
    }

    /**
     * Decorator class used for obtaining connections through a
     * <code>DataSource</code>.
     */
    private static class DataSourceDecorator extends TestSetup {
        /** Connection shared by many tests. */
        private Connection connection_;

        /**
         * Creates a new <code>DataSourceDecorator</code> instance.
         *
         * @param test the test to decorate
         */
        public DataSourceDecorator(Test test) {
            super(test);
        }

        /**
         * Sets up the test by creating a connection.
         *
         * @exception SQLException if an error occurs
         */
        public final void setUp() throws SQLException {
            connection_ = newConnection();
        }

        /**
         * Gets the connection created when the test was set up.
         *
         * @return a <code>Connection</code> value
         */
        public final Connection getConnection() {
            return connection_;
        }

        /**
         * Creates a new connection with auto-commit set to false.
         *
         * @return a <code>Connection</code> value
         * @exception SQLException if an error occurs
         */
        public final Connection newConnection() throws SQLException {
            Connection conn = newConnection_();
            conn.setAutoCommit(false);
            return conn;
        }

        /**
         * Tears down the test and closes the connection.
         *
         * @exception SQLException if an error occurs
         */
        public final void tearDown() throws SQLException {
            connection_.rollback();
            connection_.close();
        }

        /**
         * Creates a new connection using a <code>DataSource</code>.
         *
         * @return a <code>Connection</code> value
         * @exception SQLException if an error occurs
         */
        protected Connection newConnection_() throws SQLException {
            DataSource ds = TestDataSourceFactory.getDataSource();
            return ds.getConnection(CONFIG.getUserName(),
                                    CONFIG.getUserPassword());
        }
    }

    /**
     * Decorator class used for obtaining connections through a
     * <code>ConnectionPoolDataSource</code>.
     */
    private static class PoolDataSourceDecorator extends DataSourceDecorator {
        /**
         * Creates a new <code>PoolDataSourceDecorator</code> instance.
         *
         * @param test the test to decorate
         */
        public PoolDataSourceDecorator(Test test) {
            super(test);
        }

        /**
         * Creates a new connection using a
         * <code>ConnectionPoolDataSource</code>.
         *
         * @return a <code>Connection</code> value
         * @exception SQLException if an error occurs
         */
        protected Connection newConnection_() throws SQLException {
            ConnectionPoolDataSource ds = TestDataSourceFactory.getConnectionPoolDataSource();
            PooledConnection pc =
                ds.getPooledConnection(CONFIG.getUserName(),
                                       CONFIG.getUserPassword());
            return pc.getConnection();
        }
    }

    /**
     * Decorator class used for obtaining connections through an
     * <code>XADataSource</code>.
     */
    private static class XADataSourceDecorator extends DataSourceDecorator {
        /**
         * Creates a new <code>XADataSourceDecorator</code> instance.
         *
         * @param test the test to decorate
         */
        public XADataSourceDecorator(Test test) {
            super(test);
        }

        /**
         * Creates a new connection using an <code>XADataSource</code>.
         *
         * @return a <code>Connection</code> value
         * @exception SQLException if an error occurs
         */
        protected Connection newConnection_() throws SQLException {
            XADataSource ds = TestDataSourceFactory.getXADataSource();
            XAConnection xac = ds.getXAConnection(CONFIG.getUserName(),
                                                  CONFIG.getUserPassword());
            return xac.getConnection();
        }
    }
}
