/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.management.NetworkServerMBeanTest

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

package org.apache.derbyTesting.functionTests.tests.management;

import java.util.Hashtable;
import javax.management.ObjectName;
import junit.framework.Test;
import junit.framework.TestSuite;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * <p>
 * This JUnit test class is for testing the NetworkServerMBean that is available
 * with the Derby Network Server. Running these tests requires a JVM supporting 
 * J2SE 5.0 or better, due to the implementation's dependency of the platform 
 * management agent.</p>
 * <p>
 * This class currently tests the following:</p>
 * <ul>
 *   <li>That the attributes we expect to be available exist</li>
 *   <li>That these attributes are readable</li>
 *   <li>That these attributes have the correct type</li>
 *   <li>That these attributes have the correct value (with some exceptions)</li>
 * </ul>
 * <p>
 * The test fixtures will fail if an exception occurs (will be reported as an 
 * error in JUnit).</p>
 */
public class NetworkServerMBeanTest extends MBeanTest {
    
    public NetworkServerMBeanTest(String name) {
        super(name);
    }
    
    public static Test suite() {
        
        // assumes that the suite will be wrapped by a setup or decorator
        // starting the network server.
        return MBeanTest.suite(NetworkServerMBeanTest.class, 
                                        "NetworkServerMBeanTest");
        
    }
    
    /**
     * <p>
     * Creates an object name instance for the Derby MBean whose object name's 
     * textual representation includes the following key properties:</p>
     * <ul>
     *   <li>type=NetworkServer</li>
     * </ul>
     * <p>
     * The object name may also include other key properties such as a system
     * identifier (DERBY-3466).</p>
     * @return the object name representing the NetworkServerMBean for the 
     *         Derby Network Server instance associated with this test 
     *         configuration.
     * @throws MalformedObjectNameException if the object name is not valid
     */
    private ObjectName getNetworkServerMBeanObjectName() 
            throws Exception {
        
        // get a reference to the NetworkServerMBean instance
        Hashtable<String, String> keyProps = new Hashtable<String, String>();
        keyProps.put("type", "NetworkServer");
        return getDerbyMBeanName(keyProps);
    }
    
        
    //
    // ---------- TEST FIXTURES ------------
    
    public void testAttributeAccumulatedConnectionCount() throws Exception {
        // TODO - make a connection or two and verify that the number increases
        Integer count = (Integer) getAttribute(
                getNetworkServerMBeanObjectName(), 
                "AccumulatedConnectionCount");
        assertNotNull(count);
        // allowing the possibility that there has been some server activity
        assertTrue(count >= 0);
    }

    public void testAttributeActiveConnectionCount() throws Exception {
        // TODO - make a connection or two and verify that the number changes
        Integer count = (Integer) getAttribute(
                getNetworkServerMBeanObjectName(), 
                "ActiveConnectionCount");
        assertNotNull(count);
        // allowing the possibility that there are active connections
        assertTrue(count >= 0);
    }

    public void testAttributeBytesReceived() throws Exception {
        // TODO - do some DB work and verify that the number increases
        Long bytesReceived = (Long) getAttribute(
                getNetworkServerMBeanObjectName(), 
                "BytesReceived");
        assertNotNull(bytesReceived);
        // allowing the possibility that there has been some server activity
        assertTrue(bytesReceived >= 0);
    }

    public void testAttributeBytesReceivedPerSecond() throws Exception {
        // TODO - do some DB work and verify that the number changes
        Integer bytesPerSec = (Integer) getAttribute(
                getNetworkServerMBeanObjectName(), 
                "BytesReceivedPerSecond");
        assertNotNull(bytesPerSec);
        // allowing the possibility that there has been some server activity
        assertTrue(bytesPerSec >= 0);
    }

    public void testAttributeBytesSent() throws Exception {
        // TODO - do some DB work and verify that the number increases
        Long bytesSent = (Long) getAttribute(
                getNetworkServerMBeanObjectName(), 
                "BytesSent");
        assertNotNull(bytesSent);
        // allowing the possibility that there has been some server activity
        assertTrue(bytesSent >= 0);
    }

    public void testAttributeBytesSentPerSecond() throws Exception {
        // TODO - do some DB work and verify that the number changes
        Integer bytesPerSec = (Integer) getAttribute(
                getNetworkServerMBeanObjectName(), 
                "BytesSentPerSecond");
        assertNotNull(bytesPerSec);
        // allowing the possibility that there has been some server activity
        assertTrue(bytesPerSec >= 0);
    }

    public void testAttributeConnectionCount() throws Exception {
        // TODO - connect to and disconnect from a DB and verify that the number changes
        Integer count = (Integer) getAttribute(
                getNetworkServerMBeanObjectName(), 
                "ConnectionCount");
        assertNotNull(count);
        // allowing the possibility that there is or has been some server activity
        assertTrue(count >= 0);

    }
    
    public void testAttributeConnectionThreadPoolSize() throws Exception {
        // TODO - connect to and disconnect from a DB and verify that the number changes
        Integer size = (Integer) getAttribute(
                getNetworkServerMBeanObjectName(), 
                "ConnectionThreadPoolSize");
        assertNotNull(size);
        // allowing the possibility that there has been some server activity
        assertTrue(size >= 0);
    }
    
    public void testAttributeDrdaHost() throws Exception {
        // localhost may also be 127.0.0.1
        // serverHost = expected host
        String serverHost = TestConfiguration.getCurrent().getHostName();
        if (serverHost.equals("localhost") || serverHost.equals("127.0.0.1")) {
            String mbeanHost = (String) getAttribute(
                getNetworkServerMBeanObjectName(), 
                "DrdaHost");
            assertNotNull(mbeanHost);
            assertTrue("mbeanHost = " + mbeanHost + " (not localhost or 127.0.0.1)", 
            mbeanHost.equals("localhost") 
                    || mbeanHost.equals("127.0.0.1"));
        } else {
            assertStringAttribute(serverHost,
                    getNetworkServerMBeanObjectName(), 
                    "DrdaHost");
        }
    }
    
    public void testDrdaKeepAlive() throws Exception {
        // assumes that the default is kept and is true
        assertBooleanAttribute(true, 
                getNetworkServerMBeanObjectName(), 
                "DrdaKeepAlive");
    }
    
    public void testAttributeDrdaMaxThreads() throws Exception {
        // assuming the default, 0
        assertIntAttribute(0, 
                getNetworkServerMBeanObjectName(), 
                "DrdaMaxThreads");
    }
    
    public void testAttributeDrdaPortNumber() throws Exception {
        assertIntAttribute(TestConfiguration.getCurrent().getPort(), 
                getNetworkServerMBeanObjectName(), 
                "DrdaPortNumber");
    }
    
    public void testAttributeDrdaSecurityMechanism() throws Exception {
        // assuming no security mechanism
        assertStringAttribute("", 
                getNetworkServerMBeanObjectName(), 
                "DrdaSecurityMechanism");
    }

    public void testAttributeDrdaSslMode() throws Exception {
        // assuming that SSL is not enabled (off)
        assertStringAttribute("off", 
                getNetworkServerMBeanObjectName(), 
                "DrdaSslMode");
    }
    
    public void testAttributeDrdaStreamOutBufferSize() throws Exception {
        // assuming that the buffer size is 0 (default)
        assertIntAttribute(0,
                getNetworkServerMBeanObjectName(), 
                "DrdaStreamOutBufferSize");
    }
    
    public void testAttributeDrdaTimeSlice() throws Exception {
        // assuming 0 timeslice (default)
        assertIntAttribute(0, 
                getNetworkServerMBeanObjectName(), 
                "DrdaTimeSlice");
    }
    
    public void testAttributeDrdaTraceAll() throws Exception {
        // assuming that traceall is not set (default)
        assertBooleanAttribute(false, 
                getNetworkServerMBeanObjectName(), 
                "DrdaTraceAll");
    }
    
    public void testAttributeDrdaTraceDirectory() throws Exception {
        // assuming that the tracedirectory has not been set, meaning that it
        // is the value of derby.system.home, or user.dir (of the Network 
        // Server) if this has not been set.
        //
        // Temporary: NetworkServerTestSetup seems volatile in this area at the
        // moment (see derby-dev 2008-03-06); will defer value checking until
        // later.
          //assertStringAttribute("SomeDirectoryPath here", 
          //      getNetworkServerMBeanObjectName(), 
          //      "DrdaTraceDirectory");
        checkStringAttributeValue(getNetworkServerMBeanObjectName(), 
                "DrdaTraceDirectory");
    }
    
    public void testAttributeStartTime() throws Exception {
        // Haven't figured out how to test the actual value yet...
        // This will only check the attribute's existence, readability and 
        // return type.
        checkLongAttributeValue(getNetworkServerMBeanObjectName(), "StartTime");
    }
    
    public void testAttributeUptime() throws Exception {
        // Haven't figured out how to test the actual value yet...
        // This will only check the attribute's existence, readability and 
        // return type.
        checkLongAttributeValue(getNetworkServerMBeanObjectName(), "Uptime");
    }
    
    public void testAttributeWaitingConnectionCount() throws Exception {
        // assuming that no connections are waiting.
        assertIntAttribute(0, getNetworkServerMBeanObjectName(), 
                "WaitingConnectionCount");
    }
    
    /**
     * This method invokes the ping operation. Because this will currently
     * result in a security exception on the server side when running with Jars
     * (due to a lacking SocketPermission), the ping operation is actually
     * only invoked when running from the classes directory.
     * This is hopefully only a temporary solution...
     * 
     * @throws java.lang.Exception if the operation fails
     */
    public void testOperationPing() throws Exception {
        /* disabling the contents of this test fixture when running with 
         * jars, until the network server has the permission to connect to 
         * itself.
         * Otherwise we get a security exception (if the security manager has 
         * been enabled).
         * 
         *    java.net.SocketPermission <host>:<port> connect,resolve
         * 
         * Since the default server policy file doesn't work when running with
         * classes, the network server should in that case have been started 
         * with no security manager (see 
         * NetworkServerTestSetup#startSeparateProcess).
         */
        if (TestConfiguration.loadingFromJars()) {
            println("testOperationPing: Won't invoke the ping operation " +
                    "since the code has been loaded from the jars.");
            return;
        } 
        // if the server is not running, an exception will be thrown when
        // invoking the ping operation.
        // assumes noSecurityManager or that the required SocketPermission has
        // been given to the network server.
        invokeOperation(getNetworkServerMBeanObjectName(), "ping");
    }
    
}
