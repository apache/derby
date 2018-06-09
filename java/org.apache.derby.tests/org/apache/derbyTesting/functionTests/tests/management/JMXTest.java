/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.management.JMXTest

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

import java.io.IOException;
import java.security.AccessController;
import java.security.PrivilegedExceptionAction;
import java.util.Set;

import javax.management.InstanceNotFoundException;
import javax.management.IntrospectionException;
import javax.management.MBeanInfo;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.ReflectionException;

import junit.framework.Test;


/**
 * Tests against the general JMX management provided by Derby
 * instead of tests against a specific MBean,
 */
public class JMXTest extends MBeanTest {
    
    public JMXTest(String name) {
        super(name);
    }
    
    public static Test suite() {
        
        return MBeanTest.suite(JMXTest.class, 
                                        "JMXTest");
    }
    
    /**
     * Test that all MBeans registered by Derby have:
     * <UL>
     * <LI> A type key property correct set.
     * <LI> Expose a class name in org.apache.derby.mbeans.
     * </UL>
     * @throws Exception
     */
    public void testDerbyRegisteredMBeansSimpleInfo() throws Exception
    {        
        Set<ObjectName> derbyMBeans = getDerbyDomainMBeans();
        
        // We expect Derby to have registered MBeans
        // including a management MBean and the one registered
        // by our setUp method.
        assertTrue("Derby MBEan count:" + derbyMBeans.size(),
                derbyMBeans.size() >= 2);
        
        final MBeanServerConnection jmx = getMBeanServerConnection();
        for (final ObjectName name : derbyMBeans)
        {
            String type = name.getKeyProperty("type");
            // Every Derby MBean has a type.
            assertNotNull(type);
            
            MBeanInfo mbeanInfo = AccessController.doPrivileged(
                    new PrivilegedExceptionAction<MBeanInfo>() {
                        public MBeanInfo run() throws InstanceNotFoundException, IntrospectionException, ReflectionException, IOException {
                            return jmx.getMBeanInfo(name);
                       }   
                    }
                );
            
            String mbeanClassName = mbeanInfo.getClassName();
            // Is the class name in the public api
            assertTrue(mbeanClassName.startsWith("org.apache.derby.mbeans."));
            
            // See if it was the application created ManagementMBean
            // This will have the implementation class registered
            // as the class name since it is not registered by Derby.
            if ("Management".equals(type)
                    && "org.apache.derby.mbeans.Management".equals(mbeanClassName))
            {
                continue;
            }
                    
            // and is a Derby specific MBean.
            assertTrue(mbeanClassName.endsWith("MBean"));
            
            // Check the type is the class name of the MBean without
            // the MBean and the package.
            String scn = mbeanClassName.substring(mbeanClassName.lastIndexOf('.') + 1);         
            scn = scn.substring(0, scn.length() - "MBean".length());
            assertEquals(scn, type);
        }
    }
}
