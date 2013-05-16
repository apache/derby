/*
 * Derby - org.apache.derbyTesting.functionTests.tests.jdbcapi.ClosedObjectTest
 *
   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 *
 */

package org.apache.derbyTesting.functionTests.tests.jdbcapi;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import javax.sql.ConnectionPoolDataSource;
import javax.sql.DataSource;
import javax.sql.PooledConnection;
import javax.sql.XAConnection;
import javax.sql.XADataSource;
import junit.extensions.TestSetup;
import junit.framework.Test;
import junit.framework.TestSuite;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.J2EEDataSource;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.JDBCDataSource;
import org.apache.derbyTesting.junit.TestConfiguration;

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

            String implClassName = object.getClass().getName();

            // update name of test with real class name
            name_ = implClassName + "." + method_.getName();

            // DERBY-6147: If the test runs on a newer version of the JVM
            // than the JDBC driver supports, we should skip those methods
            // that are not implemented.
            //
            // Limit the check to platforms that support JDBC 4 or higher
            // since the isImplemented() method uses a method only available
            // in Java 5 and higher. We know that all JDBC 3 and JSR-169
            // methods are implemented, so no tests need to be skipped on
            // those older platforms anyway.
            if (JDBC.vmSupportsJDBC4() && !isImplemented()) {
                println("Skipping testing of " + method_ + " on " +
                        implClassName + " because it is not implemented");
                name_ += "_SKIPPED";
                return;
            }

            method_.invoke(object,
                           getNullArguments(method_.getParameterTypes()));

            // If we get here, and we expected an exception to be thrown,
            // report that as a failure.
            assertFalse("No exception was thrown for method " + method_,
                            decorator_.expectsException(method_));
        } catch (InvocationTargetException ite) {
            // An exception was thrown. Check if we expected that an exception
            // was thrown, and if it was the exception we expected.
            try {
                throw ite.getCause();
            } catch (SQLException sqle) {
                decorator_.checkException(method_, sqle);
            }
        }
    }

    /**
     * Check if the JDBC interface method tested by this test case is
     * actually implemented by the Derby object being tested.
     */
    private boolean isImplemented() throws NoSuchMethodException {
        // Check if the method is implemented in one of the Derby classes
        // that the JDBC object belongs to.
        for (Class<?> c = decorator_.getClosedObject().getClass();
                c != null; c = c.getSuperclass()) {
            if (c.getName().startsWith("org.apache.derby.")) {
                try {
                    Method m = c.getDeclaredMethod(
                        method_.getName(), method_.getParameterTypes());
                    if (!m.isSynthetic()) {
                        // Found a real implementation of the method.
                        return true;
                    }
                } catch (NoSuchMethodException e) {
                    // Method was not declared in this class. Try again in
                    // the superclass.
                }
            }
        }

        // No implementation was found.
        return false;
    }

    /** Creates a suite with all tests in the class. */
    public static Test suite() {
        TestSuite suite = new TestSuite("ClosedObjectTest suite");
        suite.addTest(baseSuite(false));
        suite.addTest(baseSuite(true));
        return suite;
    }

    /**
     * Creates the test suite and fills it with tests using
     * <code>DataSource</code>, <code>ConnectionPoolDataSource</code>
     * and <code>XADataSource</code> to obtain objects.
     *
     * @param network whether or not to run tests with the network client
     * @return a <code>Test</code> value
     * @exception Exception if an error occurs while building the test suite
     */
    private static Test baseSuite(boolean network) {
        TestSuite topSuite = new TestSuite(
            "ClosedObjectTest:" + (network ? "client" : "embedded"));

        TestSuite dsSuite = new TestSuite("ClosedObjectTest DataSource");
        DataSourceDecorator dsDecorator = new DataSourceDecorator(dsSuite);
        topSuite.addTest(dsDecorator);
        fillDataSourceSuite(dsSuite, dsDecorator);

        // JDBC 3 required for ConnectionPoolDataSource and XADataSource
        if (JDBC.vmSupportsJDBC3()) {

            // Plain connection pool test.
            topSuite.addTest(poolSuite(Collections.emptyMap()));

            // The client driver has a variant of connection pool that caches
            // and reuses JDBC statements. Test it here by setting the
            // maxStatements property.
            if (network) {
                topSuite.addTest(poolSuite(Collections.singletonMap(
                        "maxStatements", Integer.valueOf(5))));
            }

            TestSuite xaSuite = new TestSuite("ClosedObjectTest XA");
            XADataSourceDecorator xaDecorator = new XADataSourceDecorator(xaSuite);
            topSuite.addTest(xaDecorator);
            fillDataSourceSuite(xaSuite, xaDecorator);
        }

        return network ?
                TestConfiguration.clientServerDecorator(topSuite) :
                topSuite;
    }

    /**
     * Creates a suite that tests objects produced by a
     * ConnectionPoolDataSource.
     *
     * @param dsProps properties to set on the data source
     * @return a suite
     */
    private static Test poolSuite(Map dsProps) {
        TestSuite poolSuite = new TestSuite(
                "ClosedObjectTest ConnectionPoolDataSource");
        PoolDataSourceDecorator poolDecorator =
                new PoolDataSourceDecorator(poolSuite, dsProps);
        fillDataSourceSuite(poolSuite, poolDecorator);
        return poolDecorator;
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
     */
    private static void fillDataSourceSuite(TestSuite suite,
                                            DataSourceDecorator dsDecorator)
    {
        TestSuite rsSuite = new TestSuite("Closed ResultSet");
        ResultSetObjectDecorator rsDecorator =
            new ResultSetObjectDecorator(rsSuite, dsDecorator);
        suite.addTest(rsDecorator);
        fillObjectSuite(rsSuite, rsDecorator, ResultSet.class);

        TestSuite stmtSuite = new TestSuite("Closed Statement");
        StatementObjectDecorator stmtDecorator =
            new StatementObjectDecorator(stmtSuite, dsDecorator);
        suite.addTest(stmtDecorator);
        fillObjectSuite(stmtSuite, stmtDecorator, Statement.class);

        TestSuite psSuite = new TestSuite("Closed PreparedStatement");
        PreparedStatementObjectDecorator psDecorator =
            new PreparedStatementObjectDecorator(psSuite, dsDecorator);
        suite.addTest(psDecorator);
        fillObjectSuite(psSuite, psDecorator, PreparedStatement.class);

        TestSuite csSuite = new TestSuite("Closed CallableStatement");
        CallableStatementObjectDecorator csDecorator =
            new CallableStatementObjectDecorator(csSuite, dsDecorator);
        suite.addTest(csDecorator);
        fillObjectSuite(csSuite, csDecorator, CallableStatement.class);

        TestSuite connSuite = new TestSuite("Closed Connection");
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
     */
    private static void fillObjectSuite(TestSuite suite,
                                        ObjectDecorator decorator,
                                        Class iface)
    {
        Method[] methods = iface.getMethods();
        for (int i = 0; i < methods.length; i++) {
            ClosedObjectTest cot = new ClosedObjectTest(methods[i], decorator);
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

        /** Tears down the test environment. */
        protected void tearDown() throws Exception {
            object_ = null;
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
         * {@code close()}, {@code isClosed()}, {@code isValid()} and
         * {@code abort()} don't expect exceptions.
         *
         * @param method a method
         * @return <code>true</code> if an exception is expected
         */
        public boolean expectsException(Method method) {
            String name = method.getName();
            return !(name.equals("close") || name.equals("isClosed")
                    || name.equals("isValid") || name.equals("abort"));
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

            if (sqle.getSQLState().startsWith("0A")) {
                // method is not supported, so we don't expect closed object
                // exception
                return;
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
         * @exception Exception if an error occurs
         */
        public void tearDown() throws Exception {
            stmt_.close();
            stmt_ = null;
            super.tearDown();
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
                // DERBY-4767 - verification test for operation in XCL16 message.
                String methodString=method.getName();
                if (methodString.indexOf("(") > 1 )
                    methodString=methodString.substring(0, (methodString.length() -2));
                assertTrue("method = " + method.toString() + ", but message: " + sqle.getMessage(),
                           sqle.getMessage().indexOf(methodString) > 0); 
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
            if (method.getName().equals("setClientInfo") &&
                    method.getParameterTypes().length == 1 &&
                    method.getParameterTypes()[0] == Properties.class) {
                // setClientInfo(Properties) should throw SQLClientInfoException
                if (!sqle.getClass().getName().equals(
                            "java.sql.SQLClientInfoException")) {
                    throw sqle;
                }
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
            connection_ = null;
        }

        /**
         * Creates a new connection using a <code>DataSource</code>.
         *
         * @return a <code>Connection</code> value
         * @exception SQLException if an error occurs
         */
        protected Connection newConnection_() throws SQLException {
            DataSource ds = JDBCDataSource.getDataSource();
            // make sure the database is created when running the test
            // standalone
            JDBCDataSource.setBeanProperty(ds, "connectionAttributes",
                                           "create=true");
            return ds.getConnection();
        }
    }

    /**
     * Decorator class used for obtaining connections through a
     * <code>ConnectionPoolDataSource</code>.
     */
    private static class PoolDataSourceDecorator extends DataSourceDecorator {
        private final Map dsProps;

        /**
         * Creates a new <code>PoolDataSourceDecorator</code> instance.
         *
         * @param test the test to decorate
         * @param dsProps data source properties
         */
        public PoolDataSourceDecorator(Test test, Map dsProps) {
            super(test);
            this.dsProps = dsProps;
        }

        /**
         * Creates a new connection using a
         * <code>ConnectionPoolDataSource</code>.
         *
         * @return a <code>Connection</code> value
         * @exception SQLException if an error occurs
         */
        protected Connection newConnection_() throws SQLException {
            ConnectionPoolDataSource ds = J2EEDataSource.getConnectionPoolDataSource();
            for (Iterator it = dsProps.entrySet().iterator(); it.hasNext(); ) {
                Map.Entry e = (Map.Entry) it.next();
                J2EEDataSource.setBeanProperty(
                    ds, (String) e.getKey(), e.getValue());
            }
            PooledConnection pc =
                ds.getPooledConnection();
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
            XADataSource ds = J2EEDataSource.getXADataSource();
            XAConnection xac = ds.getXAConnection();
            return xac.getConnection();
        }
    }
}
