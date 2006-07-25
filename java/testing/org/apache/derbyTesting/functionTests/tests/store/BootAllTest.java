/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.store.BootAllTest

   Copyright 1999, 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derbyTesting.functionTests.tests.store;

import org.apache.derbyTesting.functionTests.util.BaseTestCase;
import org.apache.derbyTesting.functionTests.util.TestUtil;

import junit.framework.*;
import java.sql.*;
import java.util.Properties;
import java.util.Arrays;

/**
 * Tests for the system property "derby.system.bootAll"
 *
 * DERBY-1296 - Setting property derby.system.bootAll causes an Exception
 * 
 */
public class BootAllTest  extends BaseTestCase {

    /** JDBC Connection */
    private Connection con;
    private Driver driver;
    private String databases[] = new String[] {"wombat1", "wombat2", "wombat3"};
    
    final static String DATABASE_SHUT_DOWN = "08006";
    final static String ALL_DATABASES_SHUT_DOWN = "XJ015";

    /**
     * Creates a new instance of BootAllTest
     */
    public BootAllTest(String name) {
        super(name);
    }

    /**
     * Create the databases
     */
    public void setUp() throws Exception {
        for (int i = 0; i < databases.length; i++) {
            con = CONFIG.getConnection(databases[i]);
            con.close();
            try {
                con = CONFIG.
                        getConnection(databases[i] + ";shutdown=true");
            } catch (SQLException se) {
                assertEquals("Expected exception on setUp " + se.getSQLState(), 
                        DATABASE_SHUT_DOWN, se.getSQLState());
            }
        }
        String url = CONFIG.getJDBCUrl("");
        driver = DriverManager.getDriver(url);
        DriverManager.deregisterDriver(driver);
        try {
            driver.connect(url + ";shutdown=true", null);
        } catch (SQLException se) {
            assertEquals("Expected exception on tearDown " + se.getSQLState(), 
                    ALL_DATABASES_SHUT_DOWN, se.getSQLState());
        }
        System.runFinalization();
        System.gc();
    }

    /**
     * Shutdown all databases
     */
    public void tearDown() throws Exception {
        String driverName = CONFIG.getJDBCClient().getJDBCDriverName();
        Class.forName(driverName);
        println("Teardown of: " + getName());
        try {
            con = CONFIG.
                    getConnection(";shutdown=true");
        } catch (SQLException se) {
            assertEquals("Expected exception on tearDown " + se.getSQLState(), 
                    ALL_DATABASES_SHUT_DOWN, se.getSQLState());
        }
    }

    /**
     * DERBY-1296 - Setting property derby.system.bootAll causes an Exception
     *
     * Check that setting the system property "derby.system.bootAll" will not 
     * cause an exception when used in combination with the system property
     * "derby.system.home".
     *
     * The property "derby.system.home" is set by default for all tests and does
     * not need to be explicitly set in this test.
     */
    public void testSettingBootAllPropertyWithHomePropertySet() 
            throws Exception 
    {
        String returnedDatabases[] = null;

        setSystemProperty("derby.system.bootAll", "true");

        String driverName = CONFIG.getJDBCClient().getJDBCDriverName();
        String url = CONFIG.getJDBCUrl("");

        Class.forName(driverName).newInstance();
        DriverManager.registerDriver(driver);

        Driver driver = DriverManager.getDriver(url);

        DriverPropertyInfo[] attributes = driver.getPropertyInfo(url, null);
        for (int i = 0; i < attributes.length; i++) {
            if (attributes[i].name.equalsIgnoreCase("databaseName")) {
                returnedDatabases = attributes[i].choices;
            }
        }

        Arrays.sort(returnedDatabases);

        assertEquals("The number of databases should be", 
                databases.length, 
                returnedDatabases.length);

        for (int i = 0; i < databases.length; i++) {
            assertEquals("Database names should be", 
                    databases[i], 
                    returnedDatabases[i]);
        }

    }
    
}
