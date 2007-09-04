/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.jdbcapi.AutoloadTest

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

package org.apache.derbyTesting.functionTests.tests.jdbcapi;

import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Enumeration;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.derby.drda.NetworkServerControl;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.Derby;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.NetworkServerTestSetup;
import org.apache.derbyTesting.junit.SecurityManagerSetup;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * This JUnit test verifies the autoloading of the jdbc driver.
 * A driver may be autoloaded due to JDBC 4 autoloading from
 * jar files or the driver's name is listed in the system
 * property jdbc.drivers when DriverManager is first loaded.
 * 
 * This test must be run in its own VM because we want to verify that the
 * driver was not accidentally loaded by some other test.
 */
public class AutoloadTest extends BaseJDBCTestCase
{		
	public	AutoloadTest( String name ) { super( name ); }

    
    /**
     * Only run a test if the driver will be auto-loaded.
     * See class desciption for details.
     */
    public static Test suite() {
        if (!JDBC.vmSupportsJDBC2())
            return new TestSuite("empty: no java.sql.DriverManager");

        boolean embeddedAutoLoad = false;
        boolean clientAutoLoad = false;
        boolean jdbc4Autoload = false;
        
        if (JDBC.vmSupportsJDBC4() && TestConfiguration.loadingFromJars())
        {
            // test client & embedded,but the JDBC 4 auto boot is not
            // a full boot of the engine. Thus while there is no
            // need to explicitly load the driver, the embedded engine
            // does not start up. Unlike when the embedded driver is
            // put in jdbc.drivers.
            
            jdbc4Autoload = true;
        }


        // Simple test to see if the driver class is
        // in the value. Could get fancy and see if it is
        // correctly formatted but not worth it.

        try {
            String jdbcDrivers = getSystemProperty("jdbc.drivers");
            if (jdbcDrivers == null)
                jdbcDrivers = "";

            embeddedAutoLoad = jdbcDrivers
                    .indexOf("org.apache.derby.jdbc.EmbeddedDriver") != -1;

            clientAutoLoad = jdbcDrivers
                    .indexOf("org.apache.derby.jdbc.ClientDriver") != -1;

        } catch (SecurityException se) {
            // assume there is no autoloading if
            // we can't read the value of jdbc.drivers.
        }

        
        if (jdbc4Autoload || embeddedAutoLoad || clientAutoLoad)
        {
            TestSuite suite = new TestSuite("AutoloadTest");
            
            if (jdbc4Autoload && !embeddedAutoLoad)
            {
                suite.addTest(SecurityManagerSetup.noSecurityManager(
                        new AutoloadTest("testEmbeddedNotStarted")));
            }
            
            if (jdbc4Autoload || embeddedAutoLoad)
                suite.addTest(baseAutoLoadSuite("embedded"));
            if (jdbc4Autoload || clientAutoLoad)
                suite.addTest(
                  TestConfiguration.clientServerDecorator(
                          baseAutoLoadSuite("client")));
            
            if (jdbc4Autoload || embeddedAutoLoad)
            {
                // DERBY-2905 related testing.
                // Ensure that after a shutdown no Derby code is
                // left registered in the driver manager
                // and that after a shutdown, an explicit load
                // can restart the engine.
                suite.addTest(new AutoloadTest("testShutdownDeRegister"));
                suite.addTest(new AutoloadTest("testExplicitReload"));
            }
                
            return suite;
        }

        // Run a single test that ensures that the driver is
        // not loaded implicitly by some other means.
        TestSuite suite = new TestSuite("AutoloadTest: no autoloading expected");
        
        suite.addTest(SecurityManagerSetup.noSecurityManager(new AutoloadTest("testEmbeddedNotStarted")));
        suite.addTest(new AutoloadTest("noloadTestNodriverLoaded"));
        suite.addTest(TestConfiguration.clientServerDecorator(
                new AutoloadTest("noloadTestNodriverLoaded")));
        
        return suite;
    }
    
    /**
     * Return the ordered set of tests when autoloading is enabled.
     */
    private static Test baseAutoLoadSuite(String which)
    {
        TestSuite suite = new TestSuite("AutoloadTest: " + which);
        
        suite.addTest(new AutoloadTest("testRegisteredDriver"));
        if ("embedded".equals(which))
        {
            // Tests to see if the full engine is booted correctly
            // when the embedded driver is autoloading
            if (Derby.hasServer())
                suite.addTest(new AutoloadTest("testAutoNetworkServerBoot"));

        }
        
        suite.addTest(new AutoloadTest("testSuccessfulConnect"));
        
        if ("embedded".equals(which)) {
            suite.addTest(SecurityManagerSetup.noSecurityManager(
                new AutoloadTest("testEmbeddedStarted")));
        }

        suite.addTest(new AutoloadTest("testUnsuccessfulConnect"));
        suite.addTest(new AutoloadTest("testExplicitLoad"));
        return suite;
    }

	// ///////////////////////////////////////////////////////////
	//
	// TEST ENTRY POINTS
	//
	// ///////////////////////////////////////////////////////////
    
    /**
     * @throws SQLException
     * 
     */
    public void testRegisteredDriver() throws SQLException
    {
        String protocol =
            getTestConfiguration().getJDBCClient().getUrlBase();
                         
        Driver driver = DriverManager.getDriver(protocol);
        assertNotNull("Expected registered driver", driver);
    }
    
    /**
     * Test that after a shutdown that no Derby embedded driver
     * is left registered in the DriverManager. See DERBY-2905.
     * @throws SQLException failure
     */
    public void testShutdownDeRegister() throws SQLException
    {
        assertTrue(isEmbeddedDriverRegistered());
        TestConfiguration.getCurrent().shutdownEngine();
        
        // DERBY-2905 - Autoload driver is left around.
        // assertFalse(isEmbeddedDriverRegistered());   
    }
    
    /**
     * Return true if there appears to be a Derby embedded
     * driver registered with the DriverManager.
     * @return
     */
    private boolean isEmbeddedDriverRegistered()
    {
        for (Enumeration e = DriverManager.getDrivers();
                e.hasMoreElements(); )
        {
            Driver d = (Driver) e.nextElement();
            String driverClass = d.getClass().getName();
            if (!driverClass.startsWith("org.apache.derby."))
                continue;
            if (driverClass.equals("org.apache.derby.jdbc.ClientDriver"))
                continue;
            
            // Some form of Derby embedded driver seems to be registered.
            return true;
        }
        return false;
    }

	/**
     * Test we can connect successfully to a database.
	 */
	public void testSuccessfulConnect()
       throws SQLException
	{
		println( "We ARE autoloading..." );
       
        // Test we can connect successfully to a database!
        String url = getTestConfiguration().getJDBCUrl();
        url = url.concat(";create=true");
        String user = getTestConfiguration().getUserName();
        String password = getTestConfiguration().getUserPassword();
        DriverManager.getConnection(url, user, password).close();
	}
    /**
     * Test the error code on an unsuccessful connect
     * to ensure it is not one returned by DriverManager.
     */
    public void testUnsuccessfulConnect()
       throws SQLException
    {     
        // Test we can connect successfully to a database!
        String url = getTestConfiguration().getJDBCUrl("nonexistentDatabase");
        String user = getTestConfiguration().getUserName();
        String password = getTestConfiguration().getUserPassword();
        try {
            DriverManager.getConnection(url, user, password).close();
            fail("connected to nonexistentDatabase");
        } catch (SQLException e) {
            String expectedError = usingEmbedded() ? "XJ004" : "08004";
            
            assertSQLState(expectedError, e);
        }
    }
    
    /**
     * Test an explict load of the driver works as well
     * even though the drivers were loaded automatically.
     * @throws Exception 
     *
     */
    public void testExplicitLoad() throws Exception
    {
        explicitLoad(false);
    }
    
    /**
     * Test that an explicit reload of the driver works,
     * typically after a shutdown. Note that just loading
     * the driver class here cannot reload the driver
     * as the driver class is already loaded and thus
     * its static initializer will not be re-executed.
     * @throws Exception
     */
    public void testExplicitReload() throws Exception
    {
        explicitLoad(true);
    }
    
    private void explicitLoad(boolean instanceOnly) throws Exception
    {
        String driverClass =
            getTestConfiguration().getJDBCClient().getJDBCDriverName();
        
        
        // With and without a new instance
        if (!instanceOnly) {
            Class.forName(driverClass);
            testSuccessfulConnect();
            testUnsuccessfulConnect();
        }
        
        Class.forName(driverClass).newInstance();
        testSuccessfulConnect();
        testUnsuccessfulConnect();
    }
    
    /**
     * Simple test when auto-loading is not expected.
     * This is to basically test that the junit setup code
     * itself does not load the driver, thus defeating the
     * real auto-loading testing.
     */
    public void noloadTestNodriverLoaded() {
        try {
            testRegisteredDriver();
            fail("Derby junit setup code is loading driver!");
        } catch (SQLException e) {
        }
    }

    /**
     * Test that the auto-load of the network server is as expected.
     * <P>
     * derby.drda.startNetworkServer=false or not set
     * <BR>
     *     network server should not auto boot.
     * <P>
     * derby.drda.startNetworkServer=true
     * <BR>
     * If jdbc.drivers contains the name of the embedded driver
     * then the server must be booted.
     * <BR>
     * Otherwise even if auto-loading the embedded driver due to JDBC 4
     * auto-loading the network server must not boot. This is because
     * the auto-loaded driver for JDBC 4 is a proxy driver that registers
     * a driver but does not boot the complete embedded engine.
     * @throws Exception 
     * 
     *
     */
    public void testAutoNetworkServerBoot() throws Exception
    {
        boolean nsAutoBoot = "true".equalsIgnoreCase(
                getSystemProperty("derby.drda.startNetworkServer"));
        
        boolean serverShouldBeUp =
            nsAutoBoot && fullEngineAutoBoot();
        
        NetworkServerControl control = new NetworkServerControl();
        
        boolean isServerUp = NetworkServerTestSetup.pingForServerStart(control);
        
        assertEquals("Network Server state incorrect",
                serverShouldBeUp, isServerUp);
        
        if (isServerUp)
            control.shutdown();
    }
    
    /**
     * Return true if a full auto-boot of the engine is expected
     * due to jdbc.drivers containing the name of the embedded driver.
     */
    private boolean fullEngineAutoBoot()
    {
        String jdbcDrivers = getSystemProperty("jdbc.drivers");
        return jdbcDrivers.indexOf("org.apache.derby.jdbc.EmbeddedDriver") != -1;
    }
    
    /**
     * Test indirect artifiacts through public apis that
     * the embedded engine has not been started.
     */
    
    public void testEmbeddedNotStarted()
    {
        assertFalse(hasDerbyThreadGroup());
    }
    
    /**
     * Check the test(s) we use to determine if the embedded driver
     * is not up indicate the opposite once the driver has been
     * fully booted.
     *
     */
    public void testEmbeddedStarted()
    {
        assertTrue(hasDerbyThreadGroup());
    }
    
    /**
     * Return true if a ThreadGroup exists that has a name
     * starting with 'derby.'. This needs to run without a security
     * manager as it requires permissions to see all active
     * thread groups. Since this not testing Derby functionality
     * there's harm to not having a security manager, since
     * no code is executed against Derby.
     */
    private boolean hasDerbyThreadGroup() {
        ThreadGroup tg = Thread.currentThread().getThreadGroup();
        
        while (tg.getParent() != null)
        {
            tg = tg.getParent();
        }
        
        // estimate of groups        
        ThreadGroup[] allGroups = new ThreadGroup[tg.activeGroupCount()];
        int actual;
        for (;;)
        {
            actual = tg.enumerate(allGroups, true);
            if (actual < allGroups.length)
                break;
            // just double the size
            allGroups = new ThreadGroup[allGroups.length * 2];
        }

        for (int i = 0; i < actual; i++)
        {
            if (allGroups[i].getName().startsWith("derby."))
                return true;
        }
        return false;
    }
}

