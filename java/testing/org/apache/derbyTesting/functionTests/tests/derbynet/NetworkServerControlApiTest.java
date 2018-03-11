/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.derbynet.NetworkServerControlApiTest

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

 */

package org.apache.derbyTesting.functionTests.tests.derbynet;

import java.io.File;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import javax.net.SocketFactory;
import java.net.Socket;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import junit.framework.Test;
import org.apache.derby.drda.NetworkServerControl;
import org.apache.derbyTesting.functionTests.util.PrivilegedFileOpsForTests;
import org.apache.derbyTesting.functionTests.util.TestUtil;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.Derby;
import org.apache.derbyTesting.junit.NetworkServerTestSetup;
import org.apache.derbyTesting.junit.SecurityManagerSetup;
import org.apache.derbyTesting.junit.SystemPropertyTestSetup;
import org.apache.derbyTesting.junit.TestConfiguration;

public class NetworkServerControlApiTest extends BaseJDBCTestCase {

    private static final String NON_ASCII_USER = "bj\u00F8rn";
    private static final String NON_ASCII_PASSWORD = "l\u00F8yndom";

    private static final String POLICY_FILE_NAME =
            "org/apache/derbyTesting/functionTests/tests/derbynet/NetworkServerControlApiTest.policy";
    
    public NetworkServerControlApiTest(String name) {
        super(name);
       
    }

    /** Test NetworkServerControl API.
     *  Right now it tests only the trace command for DERBY-3110.
     *  TODO: Add tests for other API calls.
     */
    
    /**
     *   Test other commands. These should all give a helpful error and the
     *   usage message
     */
    public void test_01_WrongUsage() throws Exception
    {
        final String nsc = "org.apache.derby.drda.NetworkServerControl";
        // we'll assume that we get the full message if we get 'Usage'
        // because sometimes, the message gets returned with carriage return,
        // and sometimes it doesn't, checking for two different parts...
        final String usage = "Usage: ";

        // no arguments
        String[] cmd = new String[] {nsc};
        assertExecJavaCmdAsExpected(new String[] 
            {"No command given.", usage}, cmd, 1);

        // some option but no command
        cmd = new String[] {nsc, "-h", "localhost"};
        assertExecJavaCmdAsExpected(new String[] 
            {"No command given.", usage}, cmd, 1);

        // unknown command
        cmd = new String[] {nsc, "unknowncmd"};
        assertExecJavaCmdAsExpected(new String[] 
            {"Command unknowncmd is unknown.", usage}, cmd, 1);

        // unknown option
        cmd = new String[] {nsc, "-unknownarg"};
        assertExecJavaCmdAsExpected(new String[] 
            {"Argument -unknownarg is unknown.", usage}, cmd, 1);

        // wrong number of arguments
        cmd = new String[] {nsc, "ping", "arg1"};
        assertExecJavaCmdAsExpected(new String[] 
            {"Invalid number of arguments for command ping.", usage}, cmd, 1);
    }
    
     /** 
     * @throws Exception
     */
    public void test_02_TraceCommands() throws Exception
    {
        NetworkServerControl nsctrl = NetworkServerTestSetup.getNetworkServerControl();
        String derbySystemHome = getSystemProperty("derby.system.home");
        nsctrl.setTraceDirectory(derbySystemHome);
       
        nsctrl.trace(true);
        nsctrl.ping();
        assertTrue(fileExists(derbySystemHome+"/Server3.trace"));
        nsctrl.trace(false);
        
        // now try on a directory where we don't have permission
        // this won't actually cause a failure until we turn on tracing.
        // assume we don't have permission to write to root.
        nsctrl.setTraceDirectory("/");
        
        // attempt to turn on tracing to location where we don't have permisson
        try {
            nsctrl.trace(true);
            fail("Should have gotten an exception turning on tracing");
        } catch (Exception e) {
            // expected exception
        }
        // make sure we can still ping
        nsctrl.ping();
    
                        
    }
    
    /**
     * Test tracing with system properties if we have no permission
     * to write to the trace directory. Make sure we can still 
     * get a connection.  Trace directory set to "/" in test setup.
     * 
     */
    public void xtestTraceSystemPropertiesNoPermission() throws SQLException{
        // our connection should go through fine and there should be an
        // exception in the derby.log.
        //access denied (java.io.FilePermission \\ read). I verified 
        // this manually when creating this fixture but do not know 
        // how to check in the test.
        assertEquals(getSystemProperty("derby.drda.traceAll"),"true");
        assertEquals(getSystemProperty("derby.drda.traceDirectory"),"/");
        Connection conn = getConnection();
        assertFalse(conn.getMetaData().isReadOnly());
    }
    
    /**
     * Test tracing with system properties when we have permissions
     * to write to the trace directory. 
     * Check that the tracing file is there.
     * 
     */
    public void xtestTraceSystemPropertiesHasPermission() throws SQLException{
        String derbysystemhome = getSystemProperty("derby.system.home");
        assertEquals(getSystemProperty("derby.drda.traceAll"),"true");
        assertEquals(getSystemProperty("derby.drda.traceDirectory"),derbysystemhome + "/trace");
        Connection conn = getConnection();
        assertFalse(conn.getMetaData().isReadOnly());
        assertTrue(fileExists(derbysystemhome+"/trace/Server1.trace"));
    }

    /**
     * Run the shutdown command with credentials that contain non-ASCII
     * characters. Regression test case for DERBY-6457.
     */
    public void xtestShutdownWithNonASCIICredentials() throws Exception {
        NetworkServerControl control =
                NetworkServerTestSetup.getNetworkServerControl();

        // Verify that the server is up.
        NetworkServerTestSetup.pingForServerStart(control);

        // Shut down the server with the default credentials, which contain
        // non-ASCII characters. See NON_ASCII_USER and NON_ASCII_PASSWORD.
        // This call used to hang forever before DERBY-6457 was fixed.
        control.shutdown();

        // Verify that the server is down.
        NetworkServerTestSetup.pingForServerUp(control, null, false);
    }

    /**
     * Test NetworkServerControl ping command.
     * @throws Exception
     */
    public void test_03_Ping() throws Exception
    {
        String currentHost = TestConfiguration.getCurrent().getHostName();
        
        NetworkServerControl nsctrl = NetworkServerTestSetup.getNetworkServerControl();
        nsctrl.ping();
        
        // Note:Cannot test ping with unknown host because it fails in
        // InetAddress.getByName()
        
        nsctrl = new NetworkServerControl(privInetAddressGetByName(currentHost), 9393);
        try {        
        	nsctrl.ping();
        	fail("Should not have been able to ping on port 9393");
        }catch (Exception e){
        	// expected exception
        }
    }

    /*
     * CVE-2018-1313: Attempt to pass arguments to COMMAND_TESTCONNECTION
     */
    public void test_03_ping_args() throws Exception
    {
        String response = tryPingDbError("mydatabase", "myuser", "mypassword");
        //System.out.println(response);
        // This once said: XJ004:Database 'mydatabase' not found.
        assertEquals("Usage", response.substring(0,5));

        response = tryPingDbError("some/sorta/db","someone","somecredentials");
        //System.out.println(response);
        assertEquals("Usage", response.substring(0,5));

        response = tryPingDbError("\\\\192.168.1.2\\guest\\db1","tata","tata");
        //System.out.println(response);
        assertEquals("Usage", response.substring(0,5));

        response = tryPingDbError("my/nocred/db", "", "");
        //System.out.println(response);
        assertEquals("Usage", response.substring(0,5));

        response = tryPingDbOK("", "scarface", "evildoer");
        //System.out.println(response);
        assertEquals("OK", response.substring(0,2));
    }

    private Socket privilegedClientSocket(final String host, int port)
                        throws Exception
    {
        try {
            return AccessController.doPrivileged(
                    new PrivilegedExceptionAction<Socket>() {
                public Socket run() throws Exception {
                    return SocketFactory.getDefault().createSocket(
                                InetAddress.getByName(host), port);
                }
            });
        } catch (PrivilegedActionException pae) {
            throw (Exception)pae.getCause();
        }
    }

    private static String byteArrayToHex(byte[] ba, int l)
    {
        if (l < 0) return "STRING OF NEGATIVE LENGTH("+l+")";
        StringBuilder sb = new StringBuilder(l * 2);
        for (int i = 0; i < l; i++) sb.append(String.format("%02x", ba[i]));
        return sb.toString();
    }

    private String tryPingDbError(String d, String u, String p)
                    throws Exception
    {
        return tryPingDbTest(2, d, u, p); // Result 2: ERROR
    }

    private String tryPingDbOK(String d, String u, String p)
                    throws Exception
    {
        return tryPingDbTest(0, d, u, p); // Result 0: OK
    }

    private String tryPingDbTest(int rc, String d, String u, String p)
                    throws Exception
    {
        //System.out.println("database: '"+d+"' (len: "+d.length()+")");
        //System.out.println("    user: '"+u+"' (len: "+u.length()+")");
        //System.out.println("password: '"+p+"' (len: "+p.length()+")");

        Socket clientSocket = privilegedClientSocket(
                TestConfiguration.getCurrent().getHostName(),
                TestConfiguration.getCurrent().getPort());
        ByteArrayOutputStream byteArrayOs = new ByteArrayOutputStream();
        DataOutputStream commandOs = new DataOutputStream(byteArrayOs);

        byte[] msgBytes = "CMD:".getBytes("UTF8");
        commandOs.write(msgBytes,0,msgBytes.length);
        commandOs.writeByte((byte) 0); // default version: 02
        commandOs.writeByte((byte) 2); // default version: 02
        commandOs.writeByte((byte) 0); // default locale: 0
        commandOs.writeByte((byte) 0); // default codeset: 0
        commandOs.writeByte((byte) 4); // COMMAND_TESTCONNECTION

        msgBytes = d.getBytes("UTF8");
        commandOs.writeByte((byte)(msgBytes.length >> 8 ));
        commandOs.writeByte((byte) msgBytes.length);
        commandOs.write(msgBytes,0,msgBytes.length);

        msgBytes = u.getBytes("UTF8");
        commandOs.writeByte((byte)(msgBytes.length >> 8 ));
        commandOs.writeByte((byte) msgBytes.length);
        commandOs.write(msgBytes,0,msgBytes.length);

        msgBytes = p.getBytes("UTF8");
        commandOs.writeByte((byte)(msgBytes.length >> 8 ));
        commandOs.writeByte((byte) msgBytes.length);
        commandOs.write(msgBytes,0,msgBytes.length);

        byteArrayOs.writeTo(clientSocket.getOutputStream());
        commandOs.flush();
        byteArrayOs.reset();
        clientSocket.shutdownOutput();

        byte[]result = new byte[1024];
        int resultLen = clientSocket.getInputStream().read(result);

        clientSocket.close();
        
        //System.out.println( "Result was " + resultLen + " bytes long");
        //System.out.println( byteArrayToHex(result,resultLen) );
        
        if (resultLen < 0)
            return "DISCONNECT";

        String r = "RPY:";
        int rl   = r.length();
        assertTrue(resultLen > rl);
        String header = new String(result, 0, rl, "UTF8");
        assertEquals(r, header);
        assertEquals(rc, result[rl++]); // 0: OK, 2: ERROR, 3: SQLERROR, etc.

        if (rc == 0)
            return "OK";

        int l = ((result[rl++] & 0xff) << 8) + (result[rl++] & 0xff);
        String response = new String(result, rl, l, "UTF8");

        return response;
    }

    
    /**
     * Wraps InitAddress.getByName in privilege block.
     *
     * @param host host to resolve
     * @return InetAddress of host
     */
    private InetAddress privInetAddressGetByName(final String host)
            throws UnknownHostException {
        try {
            return AccessController.doPrivileged(
                    new PrivilegedExceptionAction<InetAddress>() {
                public InetAddress run() throws UnknownHostException {
                    return InetAddress.getByName(host);
                }
            });
        } catch (PrivilegedActionException pae) {
            throw (UnknownHostException) pae.getCause();
        }
    }

    private boolean fileExists(String filename) {
        return PrivilegedFileOpsForTests.exists(new File(filename));
    }
    
    /**
     * Add decorators to a test run. Context is established in the reverse order
     * that decorators are declared here. That is, decorators compose in reverse
     * order. The order of the setup methods is:
     *
     * <ul>
     * <li>Copy security policy to visible location.</li>
     * <li>Install a security manager.</li>
     * <li>Run the tests.</li>
     * </ul>
     */
    private static Test decorateTest()
    {
        Test test = TestConfiguration.clientServerDecorator(
                new BaseTestSuite(NetworkServerControlApiTest.class));
        //
        // Install a security manager using the initial policy file.
        //
        return new SecurityManagerSetup(test, POLICY_FILE_NAME);
    }
    
    public static Test suite()
    {
        
        BaseTestSuite suite =
            new BaseTestSuite("NetworkServerControlApiTest");
        
        // Need derbynet.jar in the classpath!
        if (!Derby.hasServer())
            return suite;
        suite.addTest(decorateTest());
        
        suite = decorateSystemPropertyTests(suite);

        suite.addTest(decorateShutdownTest(
                "xtestShutdownWithNonASCIICredentials",
                NON_ASCII_USER, NON_ASCII_PASSWORD));

        return suite;
    }

    private static BaseTestSuite decorateSystemPropertyTests(
        BaseTestSuite suite) {

        Properties traceProps = new Properties();
        traceProps.put("derby.drda.traceDirectory","/");
        traceProps.put("derby.drda.traceAll","true");
        suite.addTest(new SystemPropertyTestSetup(TestConfiguration.clientServerDecorator(
                new NetworkServerControlApiTest("xtestTraceSystemPropertiesNoPermission")),
                    traceProps));
        
        Properties traceProps2 = new Properties();
        
        traceProps2.put("derby.drda.traceDirectory",getSystemProperty("derby.system.home") + "/trace");
        traceProps2.put("derby.drda.traceAll","true");
        suite.addTest(new SystemPropertyTestSetup(TestConfiguration.clientServerDecorator(
                new NetworkServerControlApiTest("xtestTraceSystemPropertiesHasPermission")),
                    traceProps2));
        
        return suite;
    }

    /**
     * Decorate a test case that will attempt to shut down a network server
     * using the supplied credentials. The network server will run with
     * authentication enabled.
     *
     * @param testName name of the test case to decorate
     * @param user the user that should attempt to shut down the server
     * @param password the password to be used when shutting down the server
     * @return the decorated test case
     */
    private static Test decorateShutdownTest(String testName,
                                             String user, String password) {
        Properties props = new Properties();
        props.setProperty("derby.connection.requireAuthentication", "true");
        props.setProperty("derby.authentication.provider", "BUILTIN");
        props.setProperty("derby.user." + user, password);

        Test test = new NetworkServerControlApiTest(testName);
        test = TestConfiguration.clientServerDecorator(test);
        test = new SystemPropertyTestSetup(test, props, true);
        test = TestConfiguration.changeUserDecorator(test, user, password);
        return test;
    }

     // test fixtures from maxthreads
    public void test_04_MaxThreads_0() throws Exception {
        NetworkServerControl server = new NetworkServerControl(InetAddress.getLocalHost(),TestConfiguration.getCurrent().getPort());
        String[] maxthreadsCmd1 = new String[]{"org.apache.derby.drda.NetworkServerControl",
                "maxthreads", "0","-p", String.valueOf(TestConfiguration.getCurrent().getPort())};
        // test maxthreads 0
        assertExecJavaCmdAsExpected(new String[]
                {"Max threads changed to 0."}, maxthreadsCmd1, 0);
        int maxValue = server.getMaxThreads();
        assertEquals("Fail! Max threads value incorrect!", 0, maxValue);
    }

    public void test_05_MaxThreads_Neg1() throws Exception {
        NetworkServerControl server = new NetworkServerControl(InetAddress.getLocalHost(),TestConfiguration.getCurrent().getPort());
        String[] maxthreadsCmd2 = new String[]{"org.apache.derby.drda.NetworkServerControl",
                "maxthreads", "-1", "-h", "localhost", "-p", String.valueOf(TestConfiguration.getCurrent().getPort())};
        String host = TestUtil.getHostName();
        maxthreadsCmd2[4] = host;
        assertExecJavaCmdAsExpected(new String[]{"Max threads changed to 0."}, maxthreadsCmd2, 0);
        //test maxthreads -1
        int maxValue = server.getMaxThreads();
        assertEquals("Fail! Max threads value incorrect!", 0, maxValue);
    }

    /**
     * Calling with -12 should fail.
     * @throws Exception
     */
    public void test_06_MaxThreads_Neg12() throws Exception {
        NetworkServerControl server = new NetworkServerControl(InetAddress.getLocalHost(),
                    TestConfiguration.getCurrent().getPort());
        String[] maxthreadsCmd3 = new String[]{"org.apache.derby.drda.NetworkServerControl",
                "maxthreads", "-12","-p", String.valueOf(TestConfiguration.getCurrent().getPort())};
        //test maxthreads -12
        assertExecJavaCmdAsExpected(new String[]{
                "Invalid value, -12, for maxthreads.",
                "Usage: NetworkServerControl <commands>",
                "Commands:",
                "start [-h <host>] [-p <port number>] [-noSecurityManager] [-ssl <ssl mode>]",
                "shutdown [-h <host>][-p <port number>] [-ssl <ssl mode>] [-user <username>] [-password <password>]",
                "ping [-h <host>][-p <port number>] [-ssl <ssl mode>]",
                "sysinfo [-h <host>][-p <port number>] [-ssl <ssl mode>]",
                "runtimeinfo [-h <host>][-p <port number>] [-ssl <ssl mode>]",
                "logconnections { on|off } [-h <host>][-p <port number>] [-ssl <ssl mode>]",
                "maxthreads <max>[-h <host>][-p <port number>] [-ssl <ssl mode>]",
                "timeslice <milliseconds>[-h <host>][-p <port number>] [-ssl <ssl mode>]",
                "trace { on|off } [-s <session id>][-h <host>][-p <port number>] [-ssl <ssl mode>]",
                "tracedirectory <trace directory>[-h <host>][-p <port number>] [-ssl <ssl mode>]",
        }, maxthreadsCmd3, 1);
        int maxValue = server.getMaxThreads();
        assertEquals("Fail! Max threads value incorrect!", 0, maxValue);
    }

    public void test_07_MaxThreads_2147483647() throws Exception {
        NetworkServerControl server = new NetworkServerControl(InetAddress.getLocalHost(),TestConfiguration.getCurrent().getPort());
        String[] maxthreadsCmd4 = new String[]{"org.apache.derby.drda.NetworkServerControl",
                "maxthreads", "2147483647","-p", String.valueOf(TestConfiguration.getCurrent().getPort())};
        assertExecJavaCmdAsExpected(new String[]{"Max threads changed to 2147483647."}, maxthreadsCmd4, 0);
        int maxValue = server.getMaxThreads();
        assertEquals("Fail! Max threads value incorrect!", 2147483647, maxValue);
    }

    public void test_08_MaxThreads_9000() throws Exception {
        NetworkServerControl server = new NetworkServerControl(InetAddress.getLocalHost(),TestConfiguration.getCurrent().getPort());
        String[] maxthreadsCmd5 = new String[]{"org.apache.derby.drda.NetworkServerControl",
                "maxthreads", "9000","-p", String.valueOf(TestConfiguration.getCurrent().getPort())};
        assertExecJavaCmdAsExpected(new String[]{"Max threads changed to 9000."}, maxthreadsCmd5, 0);
        int maxValue = server.getMaxThreads();
        assertEquals("Fail! Max threads value incorrect!", 9000, maxValue);
    }

    /**
     * Calling with 'a' causes a NFE which results in an error.
     * @throws Exception
     */
    public void test_09_MaxThreads_Invalid() throws Exception {
        NetworkServerControl server = new NetworkServerControl(InetAddress.getLocalHost(),TestConfiguration.getCurrent().getPort());
        String[] maxthreadsCmd5 = new String[]{"org.apache.derby.drda.NetworkServerControl",
                "maxthreads", "10000","-p", String.valueOf(TestConfiguration.getCurrent().getPort())};
        assertExecJavaCmdAsExpected(new String[]{"Max threads changed to 10000."}, maxthreadsCmd5, 0);
        int maxValue = server.getMaxThreads();
        assertEquals("Fail! Max threads value incorrect!", 10000, maxValue);

        String[] maxthreadsCmd6 = new String[]{"org.apache.derby.drda.NetworkServerControl",
                "maxthreads", "a"};
        assertExecJavaCmdAsExpected(new String[]{"Invalid value, a, for maxthreads.",
                "Usage: NetworkServerControl <commands>",
                "Commands:",
                "start [-h <host>] [-p <port number>] [-noSecurityManager] [-ssl <ssl mode>]",
                "shutdown [-h <host>][-p <port number>] [-ssl <ssl mode>] [-user <username>] [-password <password>]",
                "ping [-h <host>][-p <port number>] [-ssl <ssl mode>]",
                "sysinfo [-h <host>][-p <port number>] [-ssl <ssl mode>]",
                "runtimeinfo [-h <host>][-p <port number>] [-ssl <ssl mode>]",
                "logconnections { on|off } [-h <host>][-p <port number>] [-ssl <ssl mode>]",
                "maxthreads <max>[-h <host>][-p <port number>] [-ssl <ssl mode>]",
                "timeslice <milliseconds>[-h <host>][-p <port number>] [-ssl <ssl mode>]",
                "trace { on|off } [-s <session id>][-h <host>][-p <port number>] [-ssl <ssl mode>]",
                "tracedirectory <trace directory>[-h <host>][-p <port number>] [-ssl <ssl mode>]",}, maxthreadsCmd6, 1);


        maxValue = server.getMaxThreads();
        assertEquals("Fail! Max threads value incorrect!", 10000, maxValue);
    }

    public void test_10_MaxThreadsCallable_0() throws Exception {
        NetworkServerControl server = new NetworkServerControl(InetAddress.getLocalHost(),TestConfiguration.getCurrent().getPort());
        server.setMaxThreads(0);
        int maxValue = server.getMaxThreads();
        assertEquals("Fail! Max threads value incorrect!", 0, maxValue);
    }

    public void test_11_MaxThreadsCallable_Neg1() throws Exception {
        NetworkServerControl server = new NetworkServerControl(InetAddress.getLocalHost(),TestConfiguration.getCurrent().getPort());
        server.setMaxThreads(-1);
        int maxValue = server.getMaxThreads();
        assertEquals("Fail! Max threads value incorrect!", 0, maxValue);
    }

    /**
     * Test should throw an exception.
     * @throws Exception
     */
    public void test_12_MaxThreadsCallable_Neg12() throws Exception {
        NetworkServerControl server = new NetworkServerControl(InetAddress.getLocalHost(),TestConfiguration.getCurrent().getPort());
        try {
            server.setMaxThreads(-2);
            fail("Should have thrown an exception with 'DRDA_InvalidValue.U:Invalid value, -2, for maxthreads.'");
        } catch (Exception e) {
            assertEquals("DRDA_InvalidValue.U:Invalid value, -2, for maxthreads.", e.getMessage());
        }
    }

    public void test_13_MaxThreadsCallable_2147483647() throws Exception {
        NetworkServerControl server = new NetworkServerControl(InetAddress.getLocalHost(),TestConfiguration.getCurrent().getPort());
        server.setMaxThreads(2147483647);
        int maxValue = server.getMaxThreads();
        assertEquals("Fail! Max threads value incorrect!", 2147483647, maxValue);
    }

    public void test_14_MaxThreadsCallable_9000() throws Exception {
        NetworkServerControl server = new NetworkServerControl(InetAddress.getLocalHost(),TestConfiguration.getCurrent().getPort());
        server.setMaxThreads(9000);
        int maxValue = server.getMaxThreads();
        assertEquals("Fail! Max threads value incorrect!", 9000, maxValue);
    }

      // timeslice test fixtures
    public void test_15_TimeSlice_0() throws Exception {
        int value = 0;
        NetworkServerControl server = new NetworkServerControl(InetAddress.getLocalHost(),TestConfiguration.getCurrent().getPort());
        String[] timesliceCmd1 = new String[]{"org.apache.derby.drda.NetworkServerControl",
                "timeslice", "0","-p", String.valueOf(TestConfiguration.getCurrent().getPort())};
        assertExecJavaCmdAsExpected(new String[]{"Time slice changed to 0."}, timesliceCmd1, 0);
        int timeSliceValue = server.getTimeSlice();
        assertEquals(value, timeSliceValue);
    }

    public void test_16_TimeSlice_Neg1() throws Exception {
        int value = 0;
        NetworkServerControl server = new NetworkServerControl(InetAddress.getLocalHost(),TestConfiguration.getCurrent().getPort());
        String[] timesliceCmd2 = new String[]{"org.apache.derby.drda.NetworkServerControl",
                "timeslice", "-1", "-h", "localhost", "-p", String.valueOf(TestConfiguration.getCurrent().getPort())};
        String host = TestUtil.getHostName();
        timesliceCmd2[4] = host;
        assertExecJavaCmdAsExpected(new String[]{"Time slice changed to 0."}, timesliceCmd2, 0);
        int timeSliceValue = server.getTimeSlice();
        assertEquals(value, timeSliceValue);
    }

    public void test_17_TimeSlice_Neg12() throws Exception {
        int value = 0;
        NetworkServerControl server = new NetworkServerControl(InetAddress.getLocalHost(),TestConfiguration.getCurrent().getPort());
        String[] timesliceCmd3 = new String[]{"org.apache.derby.drda.NetworkServerControl",
                "timeslice", "-12","-p", String.valueOf(TestConfiguration.getCurrent().getPort())};
        assertExecJavaCmdAsExpected(new String[]{"Invalid value, -12, for timeslice.",
                "Usage: NetworkServerControl <commands> ",
                "Commands:",
                "start [-h <host>] [-p <port number>] [-noSecurityManager] [-ssl <ssl mode>]",
                "shutdown [-h <host>][-p <port number>] [-ssl <ssl mode>] [-user <username>] [-password <password>]",
                "ping [-h <host>][-p <port number>] [-ssl <ssl mode>]",
                "sysinfo [-h <host>][-p <port number>] [-ssl <ssl mode>]",
                "runtimeinfo [-h <host>][-p <port number>] [-ssl <ssl mode>]",
                "logconnections { on|off } [-h <host>][-p <port number>] [-ssl <ssl mode>]",
                "maxthreads <max>[-h <host>][-p <port number>] [-ssl <ssl mode>]",
                "timeslice <milliseconds>[-h <host>][-p <port number>] [-ssl <ssl mode>]",
                "trace { on|off } [-s <session id>][-h <host>][-p <port number>] [-ssl <ssl mode>]",
                "tracedirectory <trace directory>[-h <host>][-p <port number>] [-ssl <ssl mode>]"}, timesliceCmd3, 1);
        int timeSliceValue = server.getTimeSlice();
        assertEquals(value, timeSliceValue);
    }

    public void test_18_TimeSlice_2147483647() throws Exception {
        int value = 2147483647;
        NetworkServerControl server = new NetworkServerControl(InetAddress.getLocalHost(),TestConfiguration.getCurrent().getPort());
        String[] timesliceCmd4 = new String[]{"org.apache.derby.drda.NetworkServerControl",
                "timeslice", "2147483647","-p", String.valueOf(TestConfiguration.getCurrent().getPort())};
        assertExecJavaCmdAsExpected(new String[]{"Time slice changed to 2147483647."}, timesliceCmd4, 0);
        int timeSliceValue = server.getTimeSlice();
        assertEquals(value, timeSliceValue);
    }

    public void test_19_TimeSlice_9000() throws Exception {
        int value = 9000;
        NetworkServerControl server = new NetworkServerControl(InetAddress.getLocalHost(),TestConfiguration.getCurrent().getPort());
        String[] timesliceCmd5 = new String[]{"org.apache.derby.drda.NetworkServerControl",
                "timeslice", "9000","-p", String.valueOf(TestConfiguration.getCurrent().getPort())};
        assertExecJavaCmdAsExpected(new String[]{"Time slice changed to 9000."}, timesliceCmd5, 0);
        int timeSliceValue = server.getTimeSlice();
        assertEquals(value, timeSliceValue);
    }

    public void test_20_TimeSlice_a() throws Exception {
        int value = 8000;
        NetworkServerControl server = new NetworkServerControl(InetAddress.getLocalHost(),TestConfiguration.getCurrent().getPort());
        String[] timesliceCmd5 = new String[]{"org.apache.derby.drda.NetworkServerControl",
                "timeslice", "8000","-p", String.valueOf(TestConfiguration.getCurrent().getPort())};
        assertExecJavaCmdAsExpected(new String[]{"Time slice changed to 8000."}, timesliceCmd5, 0);
        int timeSliceValue = server.getTimeSlice();
        assertEquals(value, timeSliceValue);
        String[] timesliceCmd6 = new String[]{"org.apache.derby.drda.NetworkServerControl",
                "timeslice", "a"};
        assertExecJavaCmdAsExpected(new String[]{"Invalid value, a, for timeslice.",
                "Usage: NetworkServerControl <commands> ",
                "Commands:",
                "start [-h <host>] [-p <port number>] [-noSecurityManager] [-ssl <ssl mode>]",
                "shutdown [-h <host>][-p <port number>] [-ssl <ssl mode>] [-user <username>] [-password <password>]",
                "ping [-h <host>][-p <port number>] [-ssl <ssl mode>]",
                "sysinfo [-h <host>][-p <port number>] [-ssl <ssl mode>]",
                "runtimeinfo [-h <host>][-p <port number>] [-ssl <ssl mode>]",
                "logconnections { on|off } [-h <host>][-p <port number>] [-ssl <ssl mode>]",
                "maxthreads <max>[-h <host>][-p <port number>] [-ssl <ssl mode>]",
                "timeslice <milliseconds>[-h <host>][-p <port number>] [-ssl <ssl mode>]",
                "trace { on|off } [-s <session id>][-h <host>][-p <port number>] [-ssl <ssl mode>]",
                "tracedirectory <trace directory>[-h <host>][-p <port number>] [-ssl <ssl mode>]"}, timesliceCmd6, 1);
        timeSliceValue = server.getTimeSlice();
        assertEquals(value, timeSliceValue);
    }

    public void test_21_TimeSliceCallable_0() throws Exception {
        NetworkServerControl server = new NetworkServerControl(InetAddress.getLocalHost(),TestConfiguration.getCurrent().getPort());
        int value = 0;
        server.setTimeSlice(0);
        int timeSliceValue = server.getTimeSlice();
        assertEquals(value, timeSliceValue);
    }

    public void test_22_TimeSliceCallable_Neg1() throws Exception {
        NetworkServerControl server = new NetworkServerControl(InetAddress.getLocalHost(),TestConfiguration.getCurrent().getPort());
        int value = 0;
        server.setTimeSlice(-1);
        int timeSliceValue = server.getTimeSlice();
        assertEquals(value, timeSliceValue);
    }

    public void test_23_TimeSliceCallable_Neg2() throws Exception {
        NetworkServerControl server = new NetworkServerControl(InetAddress.getLocalHost(),TestConfiguration.getCurrent().getPort());
        int value = 0;
        try {
            server.setTimeSlice(-2);
        } catch (Exception e) {
            assertTrue(e.getMessage().indexOf("Invalid value") != -1); 
        }
        int timeSliceValue = server.getTimeSlice();
        assertEquals(value, timeSliceValue);
    }

    public void test_24_TimeSliceCallable_2147483647() throws Exception {
        NetworkServerControl server = new NetworkServerControl(InetAddress.getLocalHost(),TestConfiguration.getCurrent().getPort());
        int value = 2147483647;
        server.setTimeSlice(2147483647);
        int timeSliceValue = server.getTimeSlice();
        assertEquals(value, timeSliceValue);
    }

    public void test_25_TimeSliceCallable_9000() throws Exception {
        NetworkServerControl server = new NetworkServerControl(InetAddress.getLocalHost(),TestConfiguration.getCurrent().getPort());
        int value = 9000;
        server.setTimeSlice(9000);
        int timeSliceValue = server.getTimeSlice();
        assertEquals(value, timeSliceValue);
    }
}
