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

import javax.management.MBeanServerConnection;

/**
 * Interface for MBeanTest to get a MBeanServerConnection connection
 * from. A decorator will setup mbeanServerConnector to point to
 * an implementation of this class to obtain JMX connections.
 */
interface JMXConnectionGetter {

    /**
     * Holds the implementation of JMXConnectionGetter for
     * an MBeanTest to use, set up by a decorator.
     */
    static final ThreadLocal<JMXConnectionGetter> mbeanServerConnector =
         new ThreadLocal<JMXConnectionGetter>();

    /**
     * Get a connection to the platform MBean Server.
     */
//IC see: https://issues.apache.org/jira/browse/DERBY-3506
    MBeanServerConnection getMBeanServerConnection(String user,
            String password) throws Exception;
    
//IC see: https://issues.apache.org/jira/browse/DERBY-3385
    void close(MBeanServerConnection jmxConnection) throws Exception;
}
