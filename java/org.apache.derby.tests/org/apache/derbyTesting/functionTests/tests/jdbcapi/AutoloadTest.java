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

import java.net.InetAddress;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import javax.sql.DataSource;
import junit.extensions.TestSetup;
import junit.framework.Test;
import org.apache.derby.drda.NetworkServerControl;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.Derby;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.JDBCDataSource;
import org.apache.derbyTesting.junit.NetworkServerTestSetup;
import org.apache.derbyTesting.junit.SecurityManagerSetup;
import org.apache.derbyTesting.junit.SpawnedProcess;
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
    private Class<?> spawnedTestClass;

	public	AutoloadTest( String name ) { super( name ); }

    /**
     * Create a test case that runs this test in a separate JVM.
     *
     * @param wrapper a test class that decorates {@code AutoloadTest} with the
     * desired configuration
     */
    private AutoloadTest(Class<?> wrapper) {
        this("spawnProcess");
        spawnedTestClass = wrapper;
    }

    /**
     * Get the name of the test case.
     * @return the test name
     */
    @Override
    public String getName() {
        String name = super.getName();
        if (spawnedTestClass != null) {
            // Append the name of the class that decorates the test case to
            // make it easier to see which configuration it runs under.
            name += ":" + spawnedTestClass.getSimpleName();
        }
        return name;
    }
    
    /**
     * Only run a test if the driver will be auto-loaded.
     * See class desciption for details.
     * @return the test
     */
    public static Test suite() {
        if (!JDBC.vmSupportsJDBC3())
            return new BaseTestSuite("empty: no java.sql.DriverManager");


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
                    .contains("org.apache.derby.jdbc.EmbeddedDriver");

            clientAutoLoad = jdbcDrivers
                    .contains("org.apache.derby.jdbc.ClientDriver");

        } catch (SecurityException se) {
            // assume there is no autoloading if
            // we can't read the value of jdbc.drivers.
        }

        
        if (jdbc4Autoload || embeddedAutoLoad || clientAutoLoad)
        {
            BaseTestSuite suite = new BaseTestSuite("AutoloadTest");
            
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
                suite.addTest(new AutoloadTest("testAssertShutdownOK"));
                suite.addTest(new AutoloadTest("testShutdownDeRegister"));
                suite.addTest(new AutoloadTest("testExplicitReload"));
            }
                
            return suite;
        }

        // Run a single test that ensures that the driver is
        // not loaded implicitly by some other means.
        BaseTestSuite suite =
            new BaseTestSuite("AutoloadTest: no autoloading expected");
        
        suite.addTest(SecurityManagerSetup.noSecurityManager(new AutoloadTest("testEmbeddedNotStarted")));
        suite.addTest(new AutoloadTest("noloadTestNodriverLoaded"));
        suite.addTest(TestConfiguration.clientServerDecorator(
                new AutoloadTest("noloadTestNodriverLoaded")));
        
        return suite;
    }
    
    /**
     * Return the ordered set of tests when autoloading is enabled.
     *
     * @param which embedded or client
     * @return the constructed test suite
     */
    private static Test baseAutoLoadSuite(String which)
    {
        BaseTestSuite suite = new BaseTestSuite("AutoloadTest: " + which);
        
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

	 if ("embedded".equals(which)) {
            suite.addTest(new AutoloadTest("testAutoloadDriverUnregister"));
        }
        return suite;
    }

    /**
     * <p>
     * Generate the full suite of autoload tests. Each test will be started
     * in its own JVM so that we know that the driver hasn't been loaded
     * accidentally by another test.
     * </p>
     *
     * <p>
     * The test suite runs {@code AutoloadTest} in the following
     * configurations:
     * </p>
     *
     * <ul>
     * <li>No jdbc.drivers property</li>
     * <li>jdbc.drivers property specifying embedded driver</li>
     * <li>jdbc.drivers property specifying client driver</li>
     * <li>jdbc.drivers property specifying both drivers</li>
     * </ul>
     * @return the test constructed
     */
    static Test fullAutoloadSuite() {
        BaseTestSuite suite = new BaseTestSuite("AutoloadTest:All");
        suite.addTest(new AutoloadTest(AutoloadTest.class));
        suite.addTest(new AutoloadTest(JDBCDriversEmbeddedTest.class));
        suite.addTest(new AutoloadTest(JDBCDriversClientTest.class));
        suite.addTest(new AutoloadTest(JDBCDriversAllTest.class));
        suite.addTest(new AutoloadTest(ConcurrentAutoloadTest.class));

        // The forked test processes will access the default test database, so
        // stop the engine in the main test process to prevent attempts to
        // double-boot the database.
        return new TestSetup(suite) {
            @Override
            protected void setUp() {
                TestConfiguration.getCurrent().shutdownEngine();
            }
        };
    }

    /**
     * Run {@code AutoloadTest} in a separate JVM.
     *
     * @throws java.lang.Exception something went wrong
     */
    public void spawnProcess() throws Exception {
        final List<String> args = new ArrayList<String>();
        args.add("-Dderby.system.durability=" +
                getSystemProperty("derby.system.durability"));
        args.add("-Dderby.tests.trace=" +
                getSystemProperty("derby.tests.trace"));
        args.add("-Dderby.system.debug=" +
                getSystemProperty("derby.tests.debug"));

        if (!TestConfiguration.isDefaultBasePort()) {
            args.add("-Dderby.tests.basePort=" + TestConfiguration.getBasePort());
        }

        args.add("junit.textui.TestRunner");
        args.add(spawnedTestClass.getName());
        final String[] cmd = args.toArray(new String[0]);

        final SpawnedProcess proc = new SpawnedProcess
                            (execJavaCmd(cmd), spawnedTestClass.getName());
        // Close stdin of the process so that it stops
        // any waiting for it and exits (shoudln't matter for this test)
        proc.suppressOutputOnComplete(); // we want to read it ourselves

        final boolean completed = proc.waitForExit(120000L /* 2m */, 1000L);

        final StringBuilder jstackReport = new StringBuilder();
        if (!completed) {
            jstackReport.append("\n\n\n[Subprocess ");
            jstackReport.append(proc.getPid());
            jstackReport.append(" hanging, jstack result:");
            jstackReport.append(proc.jstack());
            jstackReport.append("End of jstack output]\n\n");
        }

        final int exitCode = proc.complete(0); // kill right away if not done

        final String output = proc.getFullServerOutput();
        final String err    = proc.getFullServerError();

        final String headerOut = "\n[ (stdout subprocess) ";
        final String headerErr = headerOut.replace("out", "err");
        final String contLineOut = headerOut.replace('[', ' ');
        final String contLineErr = contLineOut.replace("out", "err");

        if (exitCode != 0) {
            final StringBuilder errMsg = new StringBuilder();
            
            errMsg.append("subprocess run failed: exit code==");
            errMsg.append(exitCode);
            errMsg.append("\n");
            errMsg.append(headerOut);
            errMsg.append(output.replaceAll("\n", contLineOut));
            errMsg.append("]\n");
            errMsg.append(headerErr);
            errMsg.append(err.replaceAll("\n", contLineErr));
            errMsg.append("]\n");

            errMsg.append(jstackReport);

            fail(errMsg.toString());
        }


        // Print sub process' outputs if this test specifies any such
        if (Boolean.parseBoolean(
                getSystemProperty("derby.tests.trace")) ||
                Boolean.parseBoolean(
                        getSystemProperty("derby.tests.debug"))) {

            System.out.println(
                    headerOut + output.replace("\n", contLineOut) + "]\n");
            System.out.println(
                    headerErr + err.replace("\n", contLineErr) + "]\n");
        }
    }

	// ///////////////////////////////////////////////////////////
	//
	// TEST ENTRY POINTS
	//
	// ///////////////////////////////////////////////////////////

    /**
     * Test DERBY-2905:Shutting down embedded Derby does remove all code,
     * the AutoloadDriver is deregistered from DriverManager.
     * 
     * @throws Exception
     */
    public void testAutoloadDriverUnregister() throws Exception {
        if (usingEmbedded()) {
            String AutoloadedDriver = getAutoloadedDriverName();
            String Driver40 = "org.apache.derby.jdbc.Driver40";
            String Driver30 = "org.apache.derby.jdbc.Driver30";
            String Driver20 = "org.apache.derby.jdbc.Driver20";

            // Test whether the Autoload driver successfully unregister after
            // DB shutdown.
            String url = getTestConfiguration().getJDBCUrl();
            url = url.concat(";create=true");
            String user = getTestConfiguration().getUserName();
            String password = getTestConfiguration().getUserPassword();
            DriverManager.getConnection(url, user, password);

            assertTrue(getRegisteredDrivers(AutoloadedDriver));

            // shut down engine
            TestConfiguration.getCurrent().shutdownEngine();

            assertFalse(getRegisteredDrivers(AutoloadedDriver));

            // Test explicit loading of Embedded driver after Autoload driver
            // is un-registered.
            String driverClass = getTestConfiguration().getJDBCClient()
                    .getJDBCDriverName();

            //Derby should be able to get a connection if AutoloaderDriver is
            //not in DriverManager. Make a connection to test it. Derby-2905
            Class<?> clazz = Class.forName(driverClass);
            clazz.getConstructor().newInstance();
            url = getTestConfiguration().getJDBCUrl();
            user = getTestConfiguration().getUserName();
            password = getTestConfiguration().getUserPassword();
            DriverManager.getConnection(url, user, password);
            assertTrue(getRegisteredDrivers(AutoloadedDriver));

            // shut down engine
            TestConfiguration.getCurrent().shutdownEngine();

            assertFalse(getRegisteredDrivers(AutoloadedDriver));
            assertFalse(getRegisteredDrivers(Driver40));
            assertFalse(getRegisteredDrivers(Driver30));
            assertFalse(getRegisteredDrivers(Driver20));
        }
    }
    private String  getAutoloadedDriverName()
    {
        return "org.apache.derby.iapi.jdbc.AutoloadedDriver";
    }
    private String  getClientDriverName()
    {
        return "org.apache.derby.client.ClientAutoloadedDriver";
    }
    
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
        
        // DERBY-2905 - Autoload driver is [not] left around.
        assertFalse(isEmbeddedDriverRegistered());   
    }
    
    /**
     * Return true if there appears to be a Derby embedded
     * driver registered with the DriverManager.
     * @return true if there appears to be a Derby embedded driver registered
     */
    private boolean isEmbeddedDriverRegistered()
    {
        String  clientDriverName = getClientDriverName();
        
        for (Enumeration<Driver> e = DriverManager.getDrivers();
                e.hasMoreElements(); )
        {
            Driver d = e.nextElement();
            String driverClass = d.getClass().getName();
            if (!driverClass.startsWith("org.apache.derby."))
                continue;
            if (driverClass.equals( clientDriverName ))
                continue;

            println( "Found " + driverClass );
            
            // Some form of Derby embedded driver seems to be registered.
            return true;
        }
        return false;
    }

	/**
     * Test we can connect successfully to a database.
     * @throws SQLException test error
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
     * @throws SQLException test error
     */
    public void testUnsuccessfulConnect() throws SQLException
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
     * Test an explicit load of the driver works as well
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

        Class<?> clazz = Class.forName(driverClass);
        clazz.getConstructor().newInstance();
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
        
        String user = getTestConfiguration().getUserName();
        String pw = getTestConfiguration().getUserPassword();
        int port = TestConfiguration.getBasePort();
        final InetAddress host = InetAddress.getByName(TestConfiguration.getCurrent().getHostName());
        NetworkServerControl control = new NetworkServerControl(host, port, user, pw);

        if (!serverShouldBeUp) {
            // If we expect the server not to come up, wait a little before
            // checking if the server is up. If the server is (unexpectedly)
            // coming up and we ping before it has come up, we will conclude
            // (incorrectly) that it did not come up.
            Thread.sleep(5000L);
        }

        boolean isServerUp = NetworkServerTestSetup.pingForServerUp(
                control, null, serverShouldBeUp);
        
        assertEquals("Network Server state incorrect",
                serverShouldBeUp, isServerUp);
        
        if (isServerUp)
            control.shutdown();
    }
    
    /**
     * @return {@code true} if a full auto-boot of the engine is expected
     * due to jdbc.drivers containing the name of the embedded driver.
     */
    private boolean fullEngineAutoBoot()
    {
        String jdbcDrivers = getSystemProperty("jdbc.drivers");
        return jdbcDrivers.contains("org.apache.derby.jdbc.EmbeddedDriver");
    }
    
    /**
     * Test indirect artifacts through public apis that
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

    private boolean getRegisteredDrivers(String driver) {

    Enumeration<Driver> e = DriverManager.getDrivers();

        while(e.hasMoreElements())
        {
                Driver drv = e.nextElement();
                if(drv.getClass().getName().equals(driver))	
			return true;
        }

	return false;
    }

    public void testAssertShutdownOK() throws SQLException {
        String AutoloadedDriver = getAutoloadedDriverName();
        Connection conn = getConnection();

        if (usingEmbedded()) {
            DataSource ds = JDBCDataSource.getDataSource();
            JDBCDataSource.setBeanProperty(ds, "shutdownDatabase", "shutdown");
            try {
                ds.getConnection();
                fail("expected shutdown to fail");
            } catch (SQLException e) {
                // expect 08006 on successful shutdown
                assertSQLState("08006", e);
            }
            assertTrue(conn.isClosed());
        } else if (usingDerbyNetClient()) {
            DataSource ds = JDBCDataSource.getDataSource();
            //Case 1: Test the deregister attribute error
            JDBCDataSource.setBeanProperty(ds, "connectionAttributes",
                    "shutdown=true;deregiste=false");
            try {
                ds.getConnection();
                fail("expected shutdown to fail");
            } catch (SQLException e) {
                // expect 08006 on successful shutdown
                assertSQLState("08006", e);
            }
            //Case 2: Test with deregister=false, AutoloadedDriver should
            //still be in DriverManager
            JDBCDataSource.setBeanProperty(ds, "connectionAttributes",
                    "shutdown=true;deregister=false");
            try {
                ds.getConnection();
                fail("expected shutdown to fail");
            } catch (SQLException e) {
                // expect 08006 on successful shutdown
                assertSQLState("08006", e);
            }
            //DERBY-2905 deregister=false should keep AutoloadedDriver in
            //DriverManager
            assertTrue(getRegisteredDrivers(AutoloadedDriver));
            //Test getting a connection just right after the shutdown.
            String url = getTestConfiguration().getJDBCUrl();
            conn = DriverManager.getConnection(url);
            Statement stmt = conn.createStatement();
            stmt.executeUpdate("values 1");
            JDBCDataSource.setBeanProperty(ds, "connectonAttributes",
                    "shutdown=true;deregister=true");
            try {
                ds.getConnection();
                fail("expected shutdown to fail");
            } catch (SQLException e) {
                // expect 08006 on successful shutdown
                assertSQLState("08006", e);
            }
            //DERBY-2905 deregister=true should deregister AutoloadedDriver in
            //DriverManager
            assertFalse(getRegisteredDrivers(AutoloadedDriver));
        }
    }

    /**
     * Return true if a ThreadGroup exists that has a name
     * starting with 'derby.'. This needs to run without a security
     * manager as it requires permissions to see all active
     * thread groups. Since this not testing Derby functionality
     * there's harm to not having a security manager, since
     * no code is executed against Derby.
     * @return see above
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

