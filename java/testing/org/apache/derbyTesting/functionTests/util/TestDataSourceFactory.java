/*

   Derby - Class org.apache.derbyTesting.functionTests.util.TestUtil

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


package org.apache.derbyTesting.functionTests.util;

import java.util.Properties;
import javax.sql.DataSource;
import javax.sql.ConnectionPoolDataSource;
import javax.sql.XADataSource;

/**
 * Utility class for JDBC JUnit tests.
 * Contains methods to obtain the various datasources.
 */

public class TestDataSourceFactory {

    /**
     * Return a <code>DataSource</code> for the appropriate framework.
     *
     * @param attrs properties for the data source
     * @return a <code>DataSource</code> object
     * @see TestUtil#getDataSource(Properties)
     */
    public static DataSource getDataSource(Properties attrs) {
        return TestUtil.getDataSource(attrs);
    }

    /**
     * Return a <code>DataSource</code> which can establish a
     * connection to the default database.
     *
     * @return a <code>DataSource</code> object
     */
    public static DataSource getDataSource() {
        return getDataSource(TestConfiguration.getDefaultDataSourceProperties());
    }

    /**
     * Return a <code>ConnectionPoolDataSource</code> for the
     * appropriate framework.
     *
     * @param attrs properties for the data source
     * @return a <code>ConnectionPoolDataSource</code> object
     * @see TestUtil#getConnectionPoolDataSource(Properties)
     */
    public static ConnectionPoolDataSource
        getConnectionPoolDataSource(Properties attrs)
    {
        return TestUtil.getConnectionPoolDataSource(attrs);
    }

    /**
     * Return a <code>ConnectionPoolDataSource</code> which can
     * establish a connection to the default database.
     *
     * @return a <code>ConnectionPoolDataSource</code> object
     */
    public static ConnectionPoolDataSource getConnectionPoolDataSource() {
        return getConnectionPoolDataSource(TestConfiguration.getDefaultDataSourceProperties());
    }

    /**
     * Return an <code>XADataSource</code> for the appropriate
     * framework.
     *
     * @param attrs properties for the data source
     * @return an <code>XADataSource</code> object
     * @see TestUtil#getXADataSource(Properties)
     */
    public static XADataSource getXADataSource(Properties attrs) {
        return TestUtil.getXADataSource(attrs);
    }

    /**
     * Return an <code>XADataSource</code> which can establish a
     * connection to the default database.
     *
     * @return an <code>XADataSource</code> object
     */
    public static XADataSource getXADataSource() {
        return getXADataSource(TestConfiguration.getDefaultDataSourceProperties());
    }	

}

