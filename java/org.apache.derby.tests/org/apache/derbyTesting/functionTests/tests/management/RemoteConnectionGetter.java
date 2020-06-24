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

import java.util.HashMap;
import java.util.Map;

import javax.management.MBeanServerConnection;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

/**
 * JMXConnectionGetter using a JMXServiceURL, currently
 * with no authentication and not using SSL.
 *
 */
class RemoteConnectionGetter implements JMXConnectionGetter {
    
    static final ThreadLocal<Map<MBeanServerConnection,JMXConnector>> connections =
        new ThreadLocal<Map<MBeanServerConnection,JMXConnector>>();
//IC see: https://issues.apache.org/jira/browse/DERBY-3385

    private final JMXServiceURL url;

    RemoteConnectionGetter(JMXServiceURL url) {
        this.url = url;
    }

    public MBeanServerConnection getMBeanServerConnection(String user,
//IC see: https://issues.apache.org/jira/browse/DERBY-3506
            String password) throws Exception {
        
        HashMap<String,String[]> env = new HashMap<String,String[]>();
        if (user != null) {
           String[] credentials = new String[] {
                   user, password };
           env.put("jmx.remote.credentials", credentials);
        }
        
        JMXConnector jmxc = JMXConnectorFactory.connect(url, env);
//IC see: https://issues.apache.org/jira/browse/DERBY-3385
        MBeanServerConnection jmxConn =  jmxc.getMBeanServerConnection();
        
        Map<MBeanServerConnection,JMXConnector> conns = connections.get();
        if (conns == null) {
            conns = new HashMap<MBeanServerConnection,JMXConnector>();
            connections.set(conns);
        }
        
        conns.put(jmxConn, jmxc);
        
        return jmxConn;
    }

    public void close(MBeanServerConnection jmxConnection) throws Exception {
        Map<MBeanServerConnection,JMXConnector> conns = connections.get();
        JMXConnector jmxc = conns.remove(jmxConnection);
        jmxc.close();
    }
}
