/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.derbyTesting.functionTests.tests.jdbcapi;

import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.SQLException;
import javax.sql.ConnectionPoolDataSource;
import javax.sql.DataSource;
import javax.sql.PooledConnection;
import javax.sql.XAConnection;
import javax.sql.XADataSource;
import junit.framework.Test;
import junit.framework.TestSuite;
import org.apache.derbyTesting.functionTests.util.TestUtil;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.J2EEDataSource;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.JDBCDataSource;

/**
 * This class tests that properties of data sources are handled correctly.
 */
public class DataSourcePropertiesTest extends BaseJDBCTestCase {

    /**
     * Creates a new test case.
     * @param name name of test method
     */
    public DataSourcePropertiesTest(String name) {
        super(name);
    }

    // SETUP

    /** Creates a test suite with all test cases
     * only running in embedded at the moment.
     */
    public static Test suite() {
        
        
        TestSuite suite = new TestSuite("DataSourcePropertiesTest");
        
        // TODO: Run fixtures in J2ME and JDBC2 (with extensions)
        // that can be supported there. This disabling matches
        // the original _app.properties file. Concern was over
        // XA support (which is supported in JDBC 2 with extensions).
        if (JDBC.vmSupportsJDBC3()) {
        
            // Add all methods starting with 'test'.
            //suite.addTestSuite(DataSourcePropertiesTest.class);
 
            Method[] methods = DataSourcePropertiesTest.class.getMethods();
            for (int i = 0; i < methods.length; i++) {
                Method m = methods[i];
                if (m.getParameterTypes().length > 0 ||
                        !m.getReturnType().equals(Void.TYPE)) {
                    continue;
                }
                String name = m.getName();
                if (name.startsWith("embedded")) {
                    suite.addTest(new DataSourcePropertiesTest(name));
                }
            }
        }
        return suite;
    }

    // HELPER METHODS

    /**
     * Sets a property of a data source object.
     *
     * @param dataSource the data source
     * @param name name of the property to set
     * @param value property value
     * @param type property type (useful for setting <code>null</code> or
     * primitive types)
     */
    private void setDataSourceProperty(Object dataSource, String name,
                                       Object value, Class type)
        throws Exception
    {
        Method setter = dataSource.getClass().
            getMethod(TestUtil.getSetterName(name), new Class[] { type });
        setter.invoke(dataSource, new Object[] { value });
    }

    /**
     * Sets a property of a data source object.
     *
     * @param dataSource the data source
     * @param name name of the property to set
     * @param value property value
     */
    private void setDataSourceProperty(Object dataSource, String name,
                                       Object value) throws Exception {
        setDataSourceProperty(dataSource, name, value, value.getClass());
    }

    // TEST METHODS

    /**
     * Tests that the default password is not sent as an attribute string when
     * <code>attributesAsPassword</code> is <code>true</code>. The test is run
     * with a <code>DataSource</code>.
     */
    public void embeddedTestAttributesAsPasswordWithoutPassword_ds()
        throws Exception
    {
        DataSource ds = JDBCDataSource.getDataSource();
        setDataSourceProperty(ds, "password", "mypassword");
        setDataSourceProperty(ds, "attributesAsPassword", Boolean.TRUE,
                              Boolean.TYPE);
        Connection c = ds.getConnection();
        c.close();
    }

    /**
     * Tests that the default password is not sent as an attribute string when
     * <code>attributesAsPassword</code> is <code>true</code>. The test is run
     * with a <code>ConnectionPoolDataSource</code>.
     */
    public void embeddedTestAttributesAsPasswordWithoutPassword_pooled()
        throws Exception
    {
        ConnectionPoolDataSource ds =
            J2EEDataSource.getConnectionPoolDataSource();
        setDataSourceProperty(ds, "password", "mypassword");
        setDataSourceProperty(ds, "attributesAsPassword", Boolean.TRUE,
                              Boolean.TYPE);
        // DERBY-1586 caused a malformed url error here
        PooledConnection pc = ds.getPooledConnection();
        Connection c = pc.getConnection();
        c.close();
    }

    /**
     * Tests that the default password is not sent as an attribute string when
     * <code>attributesAsPassword</code> is <code>true</code>. The test is run
     * with an <code>XADataSource</code>.
     */
    public void embeddedTestAttributesAsPasswordWithoutPassword_xa()
        throws Exception
    {
        XADataSource ds = J2EEDataSource.getXADataSource();
        setDataSourceProperty(ds, "password", "mypassword");
        setDataSourceProperty(ds, "attributesAsPassword", Boolean.TRUE,
                              Boolean.TYPE);
        XAConnection xa = ds.getXAConnection();
        Connection c = xa.getConnection();
        c.close();
    }

    /**
     * Tests that the <code>attributesAsPassword</code> property of a
     * <code>DataSource</code> causes an explicitly specified password to be
     * sent as a property string.
     */
    public void embeddedTestAttributesAsPasswordWithPassword_ds()
        throws Exception
    {
        DataSource ds = JDBCDataSource.getDataSource();
        setDataSourceProperty(ds, "attributesAsPassword", Boolean.TRUE,
                              Boolean.TYPE);
        try {
            Connection c = ds.getConnection("username", "mypassword");
            fail("Expected getConnection to fail.");
        } catch (SQLException e) {
            // expect error because of malformed url
            assertSQLState("XJ028", e);
        }
    }

    /**
     * Tests that the <code>attributesAsPassword</code> property of a
     * <code>ConnectionPoolDataSource</code> causes an explicitly specified
     * password to be sent as a property string.
     */
    public void embeddedTestAttributesAsPasswordWithPassword_pooled()
        throws Exception
    {
        ConnectionPoolDataSource ds =
            J2EEDataSource.getConnectionPoolDataSource();
        setDataSourceProperty(ds, "attributesAsPassword", Boolean.TRUE,
                              Boolean.TYPE);
        try {
            PooledConnection pc =
                ds.getPooledConnection("username", "mypassword");
            fail("Expected getPooledConnection to fail.");
        } catch (SQLException e) {
            // expect error because of malformed url
            assertSQLState("XJ028", e);
        }
    }

    /**
     * Tests that the <code>attributesAsPassword</code> property of an
     * <code>XADataSource</code> causes an explicitly specified password to be
     * sent as a property string.
     */
    public void embeddedTestAttributesAsPasswordWithPassword_xa()
        throws Exception
    {
        XADataSource ds = J2EEDataSource.getXADataSource();
        setDataSourceProperty(ds, "attributesAsPassword", Boolean.TRUE,
                              Boolean.TYPE);
        try {
            XAConnection xa = ds.getXAConnection("username", "mypassword");
            fail("Expected getXAConnection to fail.");
        } catch (SQLException e) {
            // expect error because of malformed url
            assertSQLState("XJ028", e);
        }
    }
}
