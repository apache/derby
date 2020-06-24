/*
   Derby - Class org.apache.derbyTesting.functionTests.tests.store.BootAllTest

   Licensed to the Apache Software Foundation (ASF) under one
   or more contributor license agreements.  See the NOTICE file
   distributed with this work for additional information
   regarding copyright ownership.  The ASF licenses this file
   to you under the Apache License, Version 2.0 (the
   "License"); you may not use this file except in compliance
   with the License.  You may obtain a copy of the License at
   
   http://www.apache.org/licenses/LICENSE-2.0
   
   Unless required by applicable law or agreed to in writing,
   software distributed under the License is distributed on an
   "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
   KIND, either express or implied.  See the License for the
   specific language governing permissions and limitations
   under the License.
*/

package org.apache.derbyTesting.functionTests.tests.store;

import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Properties;
import junit.framework.Test;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.JDBCClient;
import org.apache.derbyTesting.junit.SystemPropertyTestSetup;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Tests for the system property "derby.system.bootAll"
 *
 * DERBY-1296 - Setting property derby.system.bootAll causes an Exception
 * 
 * create and shutdown three databases as well as the default
 * shutdown the engine
 * set "derby.system.bootAll"
 * check at least four databases are listed in the driver info
 * 
 * Test drops the three databases after their use as it uses
 * the singleUseDatabaseDecorator.
 * 
 * Test is written to be tolerant of other databases in the system.
 * 
 */
public class BootAllTest  extends BaseJDBCTestCase {
    
    public static Test suite() {
        
//IC see: https://issues.apache.org/jira/browse/DERBY-6590
        BaseTestSuite suite = new BaseTestSuite("BootAllTest");
        
        // Test uses driver manager so JDBC 2 required.
//IC see: https://issues.apache.org/jira/browse/DERBY-2024
        if (JDBC.vmSupportsJDBC3())
        {           
            // Suite to create the third (inner) database and
            // perform the actual test (will be run last)
            BaseTestSuite db3 = new BaseTestSuite("db3");
            db3.addTest(new BootAllTest("createShutdownDatabase"));
            db3.addTest(new BootAllTest("shutdownDerby"));
            
            Properties ba = new Properties();
            ba.setProperty("derby.system.bootAll", "true");
            
            db3.addTest(new SystemPropertyTestSetup(
                    new BootAllTest("testSettingBootAllPropertyWithHomePropertySet"),
                    ba));
            
            // Suite to create the second database (middle) and
            // embed in it the third database suite.
//IC see: https://issues.apache.org/jira/browse/DERBY-6590
            BaseTestSuite db2 = new BaseTestSuite("db2");
            db2.addTest(new BootAllTest("createShutdownDatabase"));
            db2.addTest(TestConfiguration.singleUseDatabaseDecorator(db3));
            
            // Suite to create the first database (outer) and
            // embed in it the second database suite.
            BaseTestSuite db1 = new BaseTestSuite("db1");
            db1.addTest(new BootAllTest("createShutdownDatabase"));
            db1.addTest(TestConfiguration.singleUseDatabaseDecorator(db2));
            
            // Add the default database in as well, this will ensure
            // that databases at the root level get booted as well
            // as those at sub-levels
            suite.addTest(new BootAllTest("createShutdownDatabase"));
            
            // add the first database into the actual suite.
            suite.addTest(TestConfiguration.singleUseDatabaseDecorator(db1)); 
        }
        
        return suite;
    }


    /**
     * Creates a new instance of BootAllTest
     */
    public BootAllTest(String name) {
        super(name);
    }
    
    public void createShutdownDatabase() throws SQLException
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-1952
//IC see: https://issues.apache.org/jira/browse/DERBY-2047
        getConnection().close();
        getTestConfiguration().shutdownDatabase();
    }
    
    public void shutdownDerby() {
        getTestConfiguration().shutdownEngine();
        System.runFinalization();
        System.gc();

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
        JDBCClient embedded = getTestConfiguration().getJDBCClient();
//IC see: https://issues.apache.org/jira/browse/DERBY-1952
//IC see: https://issues.apache.org/jira/browse/DERBY-2047

        String driverName = embedded.getJDBCDriverName();
        String url = embedded.getUrlBase();
        
        // Ensure the engine is not booted.
        try {
            DriverManager.getDriver(url);
            fail("Derby is booted!");
        } catch (SQLException e) {
       }

//IC see: https://issues.apache.org/jira/browse/DERBY-6856
        Class<?> clazz = Class.forName(driverName);
        clazz.getConstructor().newInstance();

        Driver driver = DriverManager.getDriver(url);

        DriverPropertyInfo[] attributes = driver.getPropertyInfo(url, null);
        
        String returnedDatabases[] = null;
        for (int i = 0; i < attributes.length; i++) {
            if (attributes[i].name.equalsIgnoreCase("databaseName")) {
                returnedDatabases = attributes[i].choices;
            }
        }
        
        // We expect at least four databases to be booted,
        // but it could be more if other tests have left databases
        // around.
        // DERBY-2069 the single use databases are not
        // booted automatically, once DERBY-2069 is fixed
        // the length can be compared to four.
        assertNotNull(returnedDatabases);
        assertTrue("Fewer databases booted than expected",
                returnedDatabases.length >= 1);
    }
    
}
