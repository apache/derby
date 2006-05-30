/*
 * Derby - org.apache.derbyTesting.functionTests.tests.jdbc4.VerifySignatures
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

import java.sql.*;
import javax.sql.*;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.apache.derbyTesting.functionTests.util.BaseJDBCTestCase;
import org.apache.derbyTesting.functionTests.util.TestUtil;
import org.apache.derbyTesting.functionTests.util.TestDataSourceFactory;

/**
 * JUnit test which checks that all methods specified by the
 * interfaces in JDBC 4.0 are implemented. The test requires JVM 1.6
 * to run.
 */
public class VerifySignatures extends BaseJDBCTestCase {

    /**
     * All the java.sql and javax.sql interfaces specified by JDBC 4.0.
     */
    private final static Class[] JDBC_INTERFACES = {
        java.sql.Array.class,
        java.sql.BaseQuery.class,
        java.sql.Blob.class,
        java.sql.CallableStatement.class,
        java.sql.Clob.class,
        java.sql.ConflictingRow.class,
        java.sql.Connection.class,
        java.sql.DatabaseMetaData.class,
        java.sql.DataSet.class,
        java.sql.DataSetResolver.class,
        java.sql.Driver.class,
        java.sql.NClob.class,
        java.sql.ParameterMetaData.class,
        java.sql.PreparedStatement.class,
        java.sql.QueryObjectGenerator.class,
        java.sql.Ref.class,
        java.sql.ResultSet.class,
        java.sql.ResultSetMetaData.class,
        java.sql.RowId.class,
        java.sql.Savepoint.class,
        java.sql.SQLData.class,
        java.sql.SQLInput.class,
        java.sql.SQLOutput.class,
        java.sql.SQLXML.class,
        java.sql.Statement.class,
        java.sql.Struct.class,
        java.sql.Wrapper.class,
        javax.sql.CommonDataSource.class,
        javax.sql.ConnectionEventListener.class,
        javax.sql.ConnectionPoolDataSource.class,
        javax.sql.DataSource.class,
        javax.sql.PooledConnection.class,
        javax.sql.RowSet.class,
        javax.sql.RowSetInternal.class,
        javax.sql.RowSetListener.class,
        javax.sql.RowSetMetaData.class,
        javax.sql.RowSetReader.class,
        javax.sql.RowSetWriter.class,
        javax.sql.StatementEventListener.class,
        javax.sql.XAConnection.class,
        javax.sql.XADataSource.class,
    };

    /**
     * Creates a new instance.
     */
    public VerifySignatures() {
        super("VerifySignatures");
    }

    /**
     * Build a suite of tests to be run.
     *
     * @return a test suite
     * @exception SQLException if a database error occurs
     */
    public static Test suite() throws SQLException {
        // set of all implementation/interface pairs found
        Set<ClassInfo> classes = new HashSet<ClassInfo>();

        collectClassesFromDataSource(classes);
        collectClassesFromConnectionPoolDataSource(classes);
        collectClassesFromXADataSource(classes);
        addClass(classes,
                 DriverManager.getDriver(CONFIG.getJDBCUrl()).getClass(),
                 java.sql.Driver.class);

        TestSuite suite = new TestSuite();

        // all interfaces for which tests have been generated
        Set<Class> interfaces = new HashSet<Class>();

        for (ClassInfo pair : classes) {
            // some methods are defined in many interfaces, so collect
            // them in a set first to avoid duplicates
            Set<Method> methods = new HashSet<Method>();
            for (Class iface : getAllInterfaces(pair.jdbcInterface)) {
                interfaces.add(iface);
                for (Method method : iface.getMethods()) {
                    methods.add(method);
                }
            }
            for (Method method : methods) {
                suite.addTest(new MethodTestCase(pair.derbyImplementation,
                                                 method));
            }
        }
        suite.addTest(new InterfaceCoverageTestCase(interfaces));
        return suite;
    }

    /**
     * Obtain a connection from a <code>DataSource</code> object and
     * perform JDBC operations on it. Collect the classes of all JDBC
     * objects that are found.
     *
     * @param classes set into which classes are collected
     * @exception SQLException if a database error occurs
     */
    private static void collectClassesFromDataSource(Set<ClassInfo> classes)
        throws SQLException
    {
        DataSource ds = TestDataSourceFactory.getDataSource();
        addClass(classes, ds.getClass(), javax.sql.DataSource.class);
        collectClassesFromConnection(ds.getConnection
                                     (CONFIG.getUserName(),
                                      CONFIG.getUserPassword()),
                                     classes);
    }

    /**
     * Obtain a connection from a <code>ConnectionPoolDataSource</code>
     * object and perform JDBC operations on it. Collect the classes
     * of all JDBC objects that are found.
     *
     * @param classes set into which classes are collected
     * @exception SQLException if a database error occurs
     */
    private static void
        collectClassesFromConnectionPoolDataSource(Set<ClassInfo> classes)
        throws SQLException
    {
        ConnectionPoolDataSource cpds = TestDataSourceFactory.getConnectionPoolDataSource();
        addClass(classes,
                 cpds.getClass(), javax.sql.ConnectionPoolDataSource.class);

        PooledConnection pc =
            cpds.getPooledConnection(CONFIG.getUserName(),
                                     CONFIG.getUserPassword());
        addClass(classes, pc.getClass(), javax.sql.PooledConnection.class);

        collectClassesFromConnection(pc.getConnection(), classes);

        pc.close();
    }

    /**
     * Obtain a connection from an <code>XADataSource</code> object
     * and perform JDBC operations on it. Collect the classes of all
     * JDBC objects that are found.
     *
     * @param classes set into which classes are collected
     * @exception SQLException if a database error occurs
     */
    private static void collectClassesFromXADataSource(Set<ClassInfo> classes)
        throws SQLException
    {
        XADataSource xads = TestDataSourceFactory.getXADataSource();
        addClass(classes, xads.getClass(), javax.sql.XADataSource.class);

        XAConnection xaconn = xads.getXAConnection(CONFIG.getUserName(),
                                                   CONFIG.getUserPassword());
        addClass(classes, xaconn.getClass(), javax.sql.XAConnection.class);

        collectClassesFromConnection(xaconn.getConnection(), classes);
    }

    /**
     * Perform JDBC operations on a <code>Connection</code>. Collect
     * the classes of all JDBC objects that are found.
     *
     * @param conn connection to a database
     * @param classes set into which classes are collected
     * @exception SQLException if a database error occurs
     */
    private static void collectClassesFromConnection(Connection conn,
                                                     Set<ClassInfo> classes)
        throws SQLException
    {
        conn.setAutoCommit(false);
        addClass(classes, conn.getClass(), java.sql.Connection.class);

        Savepoint sp = conn.setSavepoint();
        addClass(classes, sp.getClass(), java.sql.Savepoint.class);
        conn.releaseSavepoint(sp);

        DatabaseMetaData dmd = conn.getMetaData();
        addClass(classes, dmd.getClass(), java.sql.DatabaseMetaData.class);

        collectClassesFromStatement(conn, classes);
        collectClassesFromPreparedStatement(conn, classes);
        collectClassesFromCallableStatement(conn, classes);
        conn.rollback();
        conn.close();
    }

    /**
     * Perform JDBC operations on a <code>Statement</code>. Collect
     * the classes of all JDBC objects that are found.
     *
     * @param conn connection to a database
     * @param classes set into which classes are collected
     * @exception SQLException if a database error occurs
     */
    private static void
        collectClassesFromStatement(Connection conn, Set<ClassInfo> classes)
        throws SQLException
    {
        Statement stmt = conn.createStatement();
        addClass(classes, stmt.getClass(), java.sql.Statement.class);

        stmt.execute("CREATE TABLE t (id INT PRIMARY KEY, " +
                     "b BLOB(10), c CLOB(10))");
        stmt.execute("INSERT INTO t (id, b, c) VALUES (1, "+
                     "CAST (" + TestUtil.stringToHexLiteral("101010001101") +
                     "AS BLOB(10)), CAST ('hello' AS CLOB(10)))");

        ResultSet rs = stmt.executeQuery("SELECT id, b, c FROM t");
        addClass(classes, rs.getClass(), java.sql.ResultSet.class);
        rs.next();
        Blob b = rs.getBlob(2);
        addClass(classes, b.getClass(), java.sql.Blob.class);
        Clob c = rs.getClob(3);
        addClass(classes, c.getClass(), java.sql.Clob.class);
        ResultSetMetaData rsmd = rs.getMetaData();
        addClass(classes, rsmd.getClass(), java.sql.ResultSetMetaData.class);
        rs.close();

        stmt.close();
        conn.rollback();
    }

    /**
     * Perform JDBC operations on a <code>PreparedStatement</code>.
     * Collect the classes of all JDBC objects that are found.
     *
     * @param conn connection to a database
     * @param classes set into which classes are collected
     * @exception SQLException if a database error occurs
     */
    private static void
        collectClassesFromPreparedStatement(Connection conn,
                                            Set<ClassInfo> classes)
        throws SQLException
    {
        PreparedStatement ps = conn.prepareStatement("VALUES(1)");
        addClass(classes, ps.getClass(), java.sql.PreparedStatement.class);
        ResultSet rs = ps.executeQuery();
        addClass(classes, rs.getClass(), java.sql.ResultSet.class);
        rs.close();

        ParameterMetaData pmd = ps.getParameterMetaData();
        addClass(classes, pmd.getClass(), java.sql.ParameterMetaData.class);

        ps.close();
    }

    /**
     * Perform JDBC operations on a <code>CallableStatement</code>.
     * Collect the classes of all JDBC objects that are found.
     *
     * @param conn connection to a database
     * @param classes set into which classes are collected
     * @exception SQLException if a database error occurs
     */
    private static void
        collectClassesFromCallableStatement(Connection conn,
                                            Set<ClassInfo> classes)
        throws SQLException
    {
        CallableStatement cs =
            conn.prepareCall("CALL SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(0)");
        addClass(classes, cs.getClass(), java.sql.CallableStatement.class);

        ParameterMetaData pmd = cs.getParameterMetaData();
        addClass(classes, pmd.getClass(), java.sql.ParameterMetaData.class);

        cs.close();
    }

    /**
     * Adds a <code>ClassInfo</code> object to a set.
     *
     * @param classes set to which the class should be added
     * @param implementation Derby implementation class
     * @param iface JDBC interface supposed to be implemented
     */
    private static void addClass(Set<ClassInfo> classes,
                                 Class implementation, Class iface) {
        classes.add(new ClassInfo(implementation, iface));
    }

    /**
     * Get the set consisting of an interface and all its
     * super-interfaces.
     *
     * @param iface an interface
     * @return the set consisting of <code>iface</code> and all its
     * super-interfaces
     */
    private static Set<Class> getAllInterfaces(Class iface) {
        Set<Class> set = new HashSet<Class>();
        set.add(iface);
        for (Class superIface : iface.getInterfaces()) {
            set.add(superIface);
            set.addAll(getAllInterfaces(superIface));
        }
        return set;
    }

    /**
     * Test case which checks that a class implements a specific
     * method.
     */
    private static class MethodTestCase extends TestCase {
        /** The Derby implementation class which is tested. */
        private final Class derbyImplementation;
        /** The method that should be implemented. */
        private final Method ifaceMethod;

        /**
         * Creates a new <code>MethodTestCase</code> instance.
         *
         * @param imp the class to test
         * @param method the method to look for
         */
        private MethodTestCase(Class imp, Method method) {
            super("MethodTestCase{Class=" + imp.getName() +
                  ",Method=" + method + "}");
            derbyImplementation = imp;
            ifaceMethod = method;
        }

        /**
         * Run the test. Check that the method is implemented and that
         * its signature is correct.
         *
         * @exception NoSuchMethodException if the method is not found
         */
        public void runTest() throws NoSuchMethodException {
            assertFalse("Implementation class is interface",
                        derbyImplementation.isInterface());

            Method impMethod =
                derbyImplementation.getMethod(ifaceMethod.getName(),
                                              ifaceMethod.getParameterTypes());

            assertEquals("Incorrect return type",
                         ifaceMethod.getReturnType(),
                         impMethod.getReturnType());

            int modifiers = impMethod.getModifiers();
            assertTrue("Non-public method", Modifier.isPublic(modifiers));
            assertFalse("Abstract method", Modifier.isAbstract(modifiers));
            assertFalse("Static method", Modifier.isStatic(modifiers));

            Class[] declaredExceptions = ifaceMethod.getExceptionTypes();
            for (Class exception : impMethod.getExceptionTypes()) {
                if (RuntimeException.class.isAssignableFrom(exception)) {
                    continue;
                }
                assertNotNull("Incompatible throws clause",
                              findCompatibleClass(exception,
                                                  declaredExceptions));
            }
        }

        /**
         * Search an array of classes for a class that is identical to
         * or a super-class of the specified exception class.
         *
         * @param exception an exception class
         * @param declared an array of exception classes declared to
         * be thrown by a method
         * @return a class that is compatible with the specified
         * exception class, or <code>null</code> if no compatible
         * class is found
         */
        private Class findCompatibleClass(Class exception, Class[] declared)
        {
            for (Class<?> dec : declared) {
                if (dec.isAssignableFrom(exception)) {
                    return dec;
                }
            }
            return null;
        }
    }

    /**
     * Test case which checks that all relevant JDBC interfaces are
     * covered by the test.
     */
    private static class InterfaceCoverageTestCase extends TestCase {

        /** The interfaces that have been tested by
         * <code>MethodTestCase</code>. */
        private final Set<Class> checkedInterfaces;
        /** All JDBC interfaces whose implementations are relevant to
         * test. */
        private final Set<Class> jdbcInterfaces;

        /**
         * Creates a new <code>InterfaceCoverageTestCase</code> instance.
         *
         * @param interfaces the interfaces that have been tested
         */
        private InterfaceCoverageTestCase(Set<Class> interfaces) {
            super("InterfaceCoverageTestCase");
            checkedInterfaces = interfaces;
            jdbcInterfaces = new HashSet<Class>(Arrays.asList(JDBC_INTERFACES));

            // Remove the interfaces that we know we haven't checked.

            // Interfaces that Derby doesn't implement:
            jdbcInterfaces.remove(java.sql.Array.class);
            jdbcInterfaces.remove(java.sql.BaseQuery.class);
            jdbcInterfaces.remove(java.sql.ConflictingRow.class);
            jdbcInterfaces.remove(java.sql.DataSet.class);
            jdbcInterfaces.remove(java.sql.DataSetResolver.class);
            jdbcInterfaces.remove(java.sql.NClob.class);
            jdbcInterfaces.remove(java.sql.QueryObjectGenerator.class);
            jdbcInterfaces.remove(java.sql.Ref.class);
            jdbcInterfaces.remove(java.sql.SQLData.class);
            jdbcInterfaces.remove(java.sql.SQLInput.class);
            jdbcInterfaces.remove(java.sql.SQLOutput.class);
            jdbcInterfaces.remove(java.sql.SQLXML.class);
            jdbcInterfaces.remove(java.sql.Struct.class);
            jdbcInterfaces.remove(javax.sql.RowSet.class);
            jdbcInterfaces.remove(javax.sql.RowSetInternal.class);
            jdbcInterfaces.remove(javax.sql.RowSetListener.class);
            jdbcInterfaces.remove(javax.sql.RowSetMetaData.class);
            jdbcInterfaces.remove(javax.sql.RowSetReader.class);
            jdbcInterfaces.remove(javax.sql.RowSetWriter.class);

            // Derby implements RowId classes, but has no way to
            // obtain an object of that type.
            jdbcInterfaces.remove(java.sql.RowId.class);

            // The event listener interfaces are implemented in
            // application code, not in Derby code.
            jdbcInterfaces.remove(javax.sql.ConnectionEventListener.class);
            jdbcInterfaces.remove(javax.sql.StatementEventListener.class);
        }

        /**
         * Run the test. Check that all relevant interfaces have been
         * tested.
         */
        public void runTest() {
            jdbcInterfaces.removeAll(checkedInterfaces);
            assertTrue("Unchecked interfaces: " + jdbcInterfaces,
                       jdbcInterfaces.isEmpty());
        }
    }

    /**
     * Data structure holding a Derby implementation class and the
     * JDBC interface it is supposed to implement.
     */
    private static class ClassInfo {
        /** Derby implementation class. */
        Class derbyImplementation;
        /** JDBC interface which should be implemented. */
        Class jdbcInterface;

        /**
         * Creates a new <code>ClassInfo</code> instance.
         *
         * @param imp the Derby implementation class
         * @param iface the JDBC interface
         */
        ClassInfo(Class imp, Class iface) {
            derbyImplementation = imp;
            jdbcInterface = iface;
        }

        /**
         * Checks whether this object is equal to another object.
         *
         * @param x another object
         * @return <code>true</code> if the objects are equal,
         * <code>false</code> otherwise
         */
        public boolean equals(Object x) {
            if (x instanceof ClassInfo) {
                ClassInfo ci = (ClassInfo) x;
                return
                    derbyImplementation.equals(ci.derbyImplementation) &&
                    jdbcInterface.equals(ci.jdbcInterface);
            }
            return false;
        }

        /**
         * Calculate hash code.
         *
         * @return hash code
         */
        public int hashCode() {
            return derbyImplementation.hashCode() ^ jdbcInterface.hashCode();
        }
    }
}
