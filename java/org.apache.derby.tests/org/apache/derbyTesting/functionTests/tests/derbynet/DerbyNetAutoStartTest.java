/**
 *  Derby - Class org.apache.derbyTesting.functionTests.tests.derbynet.DerbyNetAutoStartTest
 *  
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

package org.apache.derbyTesting.functionTests.tests.derbynet;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Locale;
import junit.framework.Test;
import org.apache.derby.drda.NetworkServerControl;
import org.apache.derbyTesting.functionTests.util.PrivilegedFileOpsForTests;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.Derby;
import org.apache.derbyTesting.junit.LocaleTestSetup;
import org.apache.derbyTesting.junit.NetworkServerTestSetup;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Test the network server derby.drda.startNetworkServer property.
 * 
 * Test that:
 * <ul>

 * <li> 1 ) The network server is not started when the property value is false.
 * <li> 2 ) The network server is started when the property value is true, and
 * <li>  a) uses the default port when the port property is not specified.
 * <li>  b) uses a non-default port when a port number is specified
 * <li>  c) uses an invalid port number (-1)
 * <li> 3 ) A message is printed to derby.log when the server is 
 *          already started.
 * </ul>
 */

public class DerbyNetAutoStartTest extends BaseJDBCTestCase {

    private Locale oldLocale = Locale.getDefault();
    
    public DerbyNetAutoStartTest(String name) {
        super(name);

    }

    //args to helper method. With or without port
    private static boolean WITHOUTPORT  = false;
    private static boolean WITHPORT  = true;

    /**
     * Do some steps to prepare for the test.
     */
    public void setUp() {
        // make sure no network server is running
        TestConfiguration.getCurrent().shutdownEngine();
    }

    /**
     * Test case 1
     * Test that if derby.drda.startNetworkServer property is false
     * that server does not come up.
     * 
     * @throws Exception
     */
    public void testStartNetworkServerFalse() throws Exception {
        setSystemProperty("derby.drda.startNetworkServer", "false");         
        // Boot with an embedded connection
        // Should not start network server
        getConnection();
        NetworkServerControl ns = 
                NetworkServerTestSetup.getNetworkServerControl();

        // Verify the server is not up
        assertFalse(NetworkServerTestSetup.pingForServerUp(ns,null, false));
    }

    /**
     * Test case 2a.
     * Test setting derby.drda.startNetworkServer property without
     * specifying anything in the port number property.
     * Should start, using the default port.
     * 
     * To avoid possible conflict with other tests running concurrently,
     * this test may only run if baseport is not set and we are 
     * using the default 1527 port.  This is accomplished by naming the
     * test starting with "ttest" vs "test", and then code in
     * baseSuite explitly runs test if can.
     *
     * 
     * @throws Exception
     */
    public void ttestStartNetworkServerTrueNoPort() throws Exception {
        startNetworkServerTrueHelper(WITHOUTPORT);
    }

    /**
     * Test case 2b.
     * Test setting derby.drda.startNetworkServer property
     * and specifying a port number
     * 
     * @throws Exception
     */
    public void testStartNetworkServerTrueWithPort() throws Exception {
        startNetworkServerTrueHelper(WITHPORT);
    }

    /**
     * Test case 2c.
     * Test setting derby.drda.startNetworkServer property
     * and specifying an invalid port number
     * Should fail to start network server
     * 
     * @throws Exception
     */
    public void testStartNetworkServerTrueWithInvalidPort() throws Exception {
        setSystemProperty("derby.drda.startNetworkServer", "true");
        // Note that internally, portNumber -1 means 'no port number provided'
        setSystemProperty("derby.drda.portNumber", "-1");
        // Boot with an embedded connection
        // Should not start network server
        // But it still appears to find an embedded connection.

        // Check by attempting something on a connection
        // This will currently print an InvocationException to the console
        // Is it a bug that it will not print to derby.log?
        // But, for now, capture the output and throw it away
        final PrintStream realSystemOut = System.out;
        final PrintStream realSystemErr = System.err;
        ByteArrayOutputStream serverOutputBOS = new ByteArrayOutputStream();
        final PrintStream serverOutputOut = new PrintStream( serverOutputBOS);
//IC see: https://issues.apache.org/jira/browse/DERBY-5840
        setSystemOut(new PrintStream(serverOutputOut));
        setSystemErr(new PrintStream(serverOutputOut));

        try {
            try
            {
                // Network start fails, but we get an Embedded connection
                DatabaseMetaData dbmd = getConnection().getMetaData();
                ResultSet rs = dbmd.getSchemas();
                assertNotNull(rs);
                rs.close();
            }
            catch( SQLException e)
            {
                fail();
            }
        } finally {
            // Restore the original out streams
//IC see: https://issues.apache.org/jira/browse/DERBY-5840
            setSystemOut(realSystemOut);
            setSystemErr(realSystemErr);
        }

        // Verify the server - use default port - is not up
        NetworkServerControl ns = 
                NetworkServerTestSetup.getNetworkServerControl();
        assertFalse(NetworkServerTestSetup.pingForServerUp(ns,null, false));
    }

    /**
     * Helper method that actually starts the server.
     *
     * @param withport
     * @throws Exception
     */
    private void startNetworkServerTrueHelper(boolean withport) 
            throws Exception  {
        int theport = withport ? 
                TestConfiguration.getCurrent().getNextAvailablePort() :
                    TestConfiguration.getCurrent().getBasePort();
                
        setSystemProperty("derby.drda.startNetworkServer", "true");
        if (withport)
        {
            setSystemProperty("derby.drda.portNumber",
                    Integer.toString(theport));
        }
        // Boot with an embedded connection
        // Should start network server
        getConnection();

        // Check the server is up and then bring it back down
        NetworkServerControl ns = NetworkServerTestSetup
                .getNetworkServerControl(theport);
        NetworkServerTestSetup.waitForServerStart(ns);
        ns.shutdown();
        assertFalse
            (NetworkServerTestSetup.pingForServerUp(ns, null, false));
    }

    /**
     * Test case 3
     * Test that if a network server is already running on 
     * a certain port, starting the server after setting 
     * derby.drda.startNetworkServer reflects an error message
     * indicating the server is already in use.
     * 
     * To avoid possible conflict with other tests running concurrently,
     * this test will also set derby.drda.portNumber.
     * 
     * @throws Exception
     */
    public void testStartNetworkServerLogMessageOnDualStart()
            throws Exception {
        // first force English locale
//IC see: https://issues.apache.org/jira/browse/DERBY-5840
        LocaleTestSetup.setDefaultLocale(Locale.ENGLISH);
        int doubleport = TestConfiguration.getCurrent().getPort();
        // start a network server
        NetworkServerControl ns = 
                NetworkServerTestSetup.getNetworkServerControl(doubleport);
        ns.start(null);
        NetworkServerTestSetup.waitForServerStart(ns);
        // shutdown to ensure getConnection reads the properties
        TestConfiguration.getCurrent().shutdownEngine();

        setSystemProperty("derby.drda.startNetworkServer", "true");
        setSystemProperty("derby.drda.portNumber",
                Integer.toString(doubleport));
        // Boot with an embedded connection
        // Should attempt to start network server
        getConnection();

        // Check the server is still up
        assertTrue(NetworkServerTestSetup.pingForServerUp(ns, null, true));

        String logFileName = 
                getSystemProperty("derby.system.home") + 
                File.separator + "derby.log";
        // Give it a little time to write the message        
        // There should be a warning in the derby.log file.
        // With some JVMS there will be a java.net.BindException
        // But always there will be the more generic message.
        // Note that by checking on the generic message, we cannot
        // distinguish the expected from any other exception.
        String expectedString = 
                "An exception was thrown during network server startup";
        final long startTime = System.currentTimeMillis();
        final long waitTime = NetworkServerTestSetup.getWaitTime();
        while (true)
        {
            Thread.sleep(1000);
            if (checkLog( logFileName, new String[] {expectedString})){
                break;
            }
            long elapsed = System.currentTimeMillis() - startTime;
            if (elapsed > waitTime) {
                fail("did not find the expected string: " + expectedString
                        + " within the maximum wait time " + waitTime);
            }
        }

        assertTrue(checkLog( logFileName, new String[] {expectedString}));
        
        ns.shutdown();
    }

    static boolean checkLog( String logFileName, String[] expected)
            throws IOException
    {
        boolean allFound = true;
        boolean[] found = new boolean[ expected.length];
        FileInputStream is =
                PrivilegedFileOpsForTests
                .getFileInputStream(new File(logFileName));
        BufferedReader br = new BufferedReader(new InputStreamReader(is));
        String logLine; 
        while((logLine = br.readLine()) != null)
        {
            // to print out derby.log, uncomment this line:
            // System.out.println(logLine);
            for( int i = 0; i < expected.length; i++)
            {
//IC see: https://issues.apache.org/jira/browse/DERBY-6619
                if( (! found[i]) && logLine.contains( expected[i])) {
                    found[i] = true;
                }
            }
        }
        for( int i = 0; i < expected.length; i++)
        {
            if( ! found[i])
            {
                allFound = false;
            }
        }
//IC see: https://issues.apache.org/jira/browse/DERBY-6619
        br.close();
        return allFound;
    } // end of checkLog

    public static Test suite() {
//IC see: https://issues.apache.org/jira/browse/DERBY-6590
        BaseTestSuite suite = new BaseTestSuite("DerbyNetAutoStartTest");
        suite.addTest(baseSuite("DerbyNetAutoStartTest:embedded"));
        return suite;
    }

    private static Test baseSuite(String name) {

        BaseTestSuite suite = new BaseTestSuite(name);
        // Need derbynet.jar in the classpath, and cannot run with ME/JSR169/cdc profile
        if (!Derby.hasServer())
            return suite;
        // Adds all tests that can run with baseport set or not.
        suite.addTestSuite(DerbyNetAutoStartTest.class);
        if (getSystemProperty("derby.tests.basePort") != null )
        {
            return suite;
        }
        // We assume, that if baseport is set, then the intention is that
        // tests are run concurrently, so we cannot use the default port
        // 1527. Lists tests that rely on/test the usage of that port here:
        suite.addTest
            (new DerbyNetAutoStartTest("ttestStartNetworkServerTrueNoPort"));
        return suite;
    }

    protected void tearDown() throws Exception {
        // unset the system properties
        removeSystemProperty("derby.drda.startNetworkServer");
        removeSystemProperty("derby.drda.portNumber");
        // set the old locale back to the original
//IC see: https://issues.apache.org/jira/browse/DERBY-5840
        LocaleTestSetup.setDefaultLocale(oldLocale);
        oldLocale=null;
        super.tearDown();
    }
}
