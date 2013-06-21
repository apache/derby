/*
 
   Derby - Class org.apache.derbyTesting.functionTests.tests.jdbc4.JDBC4FromJDBC3DataSourceTest

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

import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.SQLException;
import javax.sql.ConnectionPoolDataSource;
import javax.sql.DataSource;
import javax.sql.PooledConnection;
import javax.sql.StatementEvent;
import javax.sql.StatementEventListener;
import javax.sql.XADataSource;
import junit.framework.*;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.J2EEDataSource;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * <p>
 * This test ensures that when a JDBC 4 application instantiates a JDBC 3
 * data source, that data source will return JDBC 4 connections even though
 * the data source itself is not a JDBC 4 object.
 * </p>
 *
 * <p>
 * Note that after DERBY-5868 and DERBY-5880, all the data sources implement all JDBC 4.0 methods.
 * </p>
 *
 * <p>
 * Neither the client nor the embedded variants of the JDBC 3 data sources
 * implement the full JDBC 4.1 interface.
 * </p>
 */
public class JDBC4FromJDBC3DataSourceTest extends BaseJDBCTestCase {
    
    /**
     * Create a test with the given name.
     * @param name name of the test.
     *
     */
    public JDBC4FromJDBC3DataSourceTest(String name)
    {
        super(name);
    }
    
    /**
     * Return suite with all tests of the class.
     */
    public static Test suite()
    {
        // Only run this test if we have a JDBC 4 JVM on full SE
        if (JDBC.vmSupportsJDBC4() && JDBC.vmSupportsJNDI())
        {
            return TestConfiguration.forceJDBC3Suite(
                JDBC4FromJDBC3DataSourceTest.class);
        }

        // Else return empty suite.
        return new TestSuite("JDBC 4 from JDBC 3 Data Sources");
    }

    /**
     * Test that a JDBC 3 data source returns a JDBC 4 PooledConnection
     * when running with a JDBC 4 JDK.
     */
    public void testPooledConnection() throws Exception
    {
        ConnectionPoolDataSource ds = J2EEDataSource.getConnectionPoolDataSource();

        checkJDBC4Interface(ds.getPooledConnection());
    }

    /**
     * Test that a JDBC 3 data source returns a JDBC 4 XAConnection
     * when running with a JDBC 4 JDK.
     */
    public void testXAConnection() throws Exception
    {
        XADataSource ds = J2EEDataSource.getXADataSource();

        checkJDBC4Interface(ds.getXAConnection());
    }


    /**
     * Make sure that the received PooledConnection, which we assume came
     * from a JDBC 3 data source, is nonetheless a JDBC 4 object.
     */
    private void checkJDBC4Interface(PooledConnection pc)
        throws Exception
    {
        // Create dummy event listener.
        StatementEventListener listener =
            new StatementEventListener()
            {
                public void statementClosed(StatementEvent event) {}
                public void statementErrorOccurred(StatementEvent event) {}
            };

        /* Assert that metadata reports JDBC 4 for the connection, which
         * it should even though the connection was created from a JDBC 3
         * datasource.
         */
        Connection conn = pc.getConnection();
        assertEquals(4, conn.getMetaData().getJDBCMajorVersion());
        conn.close();
        conn = null;

        /* The way we check to see if we actually have JDBC 4 objects is
         * to call two methods that only exist in JDBC 4.  These should
         * succeed.  Before DERBY-2488 they would fail with an Abstract
         * MethodError.
         */
        pc.addStatementEventListener(listener);
        pc.removeStatementEventListener(listener);
        pc.close();
    }

}
