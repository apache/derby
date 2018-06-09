/*

 Derby - Class org.apache.derbyTesting.functionTests.tests.management.RemoteConnectionGetter

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

import java.lang.management.ManagementFactory;
import java.security.AccessController;
import java.security.PrivilegedAction;

import javax.management.MBeanServerConnection;

/**
 * JMXConnectionGetter using the platform MBean server.
 *
 */
class PlatformConnectionGetter implements JMXConnectionGetter {

    PlatformConnectionGetter() {
    }

    /**
     * User name ignored, only applicable for remote connections.
     */
    public MBeanServerConnection getMBeanServerConnection(String user,
            String password) throws Exception {
        
        return AccessController.doPrivileged(new PrivilegedAction<MBeanServerConnection>() {

            public MBeanServerConnection run() {
                return ManagementFactory.getPlatformMBeanServer(); 
            }});     
    }

    public void close(MBeanServerConnection jmxConnection)  {
        // nothing to do.
    }
}
