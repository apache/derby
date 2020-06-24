/*

 Derby - Class org.apache.derbyTesting.functionTests.tests.management.JMXConnectionGetter

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

import java.net.MalformedURLException;

import javax.management.remote.JMXServiceURL;

import junit.framework.Test;

import org.apache.derbyTesting.junit.BaseTestSetup;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Interface for MBeanTest to get a MBeanServerConnection connection
 * from. A decorator will setup mbeanServerConnector to point to
 * an implementation of this class to obtain JMX connections.
 */
class JMXConnectionDecorator extends BaseTestSetup {
    
    /**
     * Decorate a test so to use JMX connections from the passed in url. 
     */
    static Test remoteNoSecurity(Test test)
    {
        return new JMXConnectionDecorator(test, true);
    }
    
    /**
     * Decorate a test to use JMX connections directly from the platform
     * MBean Server.
     */
    static Test platformMBeanServer(Test test)
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-3385
        return new JMXConnectionDecorator(test, false);
    }
    
    // ignored for now
    private final boolean remote;
    private JMXConnectionGetter oldGetter;

    private JMXConnectionDecorator(Test test, boolean remote) {
        super(test);
        this.remote = remote;
    }
    
    @Override
    protected void setUp() throws Exception {
        super.setUp();
        oldGetter =
            JMXConnectionGetter.mbeanServerConnector.get();
        
//IC see: https://issues.apache.org/jira/browse/DERBY-3385
        JMXConnectionGetter getter = remote ?
                new RemoteConnectionGetter(getJmxUrl()) :
                new PlatformConnectionGetter();
                
        JMXConnectionGetter.mbeanServerConnector.set(getter);
    }
    
    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
        JMXConnectionGetter.mbeanServerConnector.set(oldGetter);
        oldGetter = null;
    }
    
    /**
     * Creates a URL for connecting to the platform MBean server on the host
     * specified by the network server hostname of this test configuration.
     * The JMX port number used is also retreived from the test configuration.
     * @return a service URL for connecting to the platform MBean server
     * @throws MalformedURLException if the URL is malformed
     */
    private JMXServiceURL getJmxUrl() throws MalformedURLException {
        
        // NOTE: This hostname is only valid in a client/server configuration
        String hostname = TestConfiguration.getCurrent().getHostName();
        //String hostname = TestConfiguration.DEFAULT_HOSTNAME; // for embedded?
        int jmxPort = TestConfiguration.getCurrent().getJmxPort();
                
        /* "jmxrmi" is the name of the RMI server connector of the platform
         * MBean server, which is used by Derby */
        JMXServiceURL url = new JMXServiceURL(
                "service:jmx:rmi:///jndi/rmi://" 
                    + hostname
                    + ":" + jmxPort + "/jmxrmi");
        
        return url;
    }
}
