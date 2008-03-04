/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.management.InactiveManagementMBeanTest

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

import java.util.Set;

import javax.management.ObjectName;

import org.apache.derbyTesting.junit.TestConfiguration;

import junit.framework.Test;


/**
 * Test the ManagementMBean interface provided by Derby
 * and installed by the application when Derby is not running.
 */
public class InactiveManagementMBeanTest extends MBeanTest {
    
    public InactiveManagementMBeanTest(String name) {
        super(name);
    }
    
    public static Test suite() {
        
        return MBeanTest.suite(InactiveManagementMBeanTest.class, 
                                        "InactiveManagementMBeanTest");
    }
    
    /**
     * Test that the MBean created by the application can
     * successfully start and stop Derby's JMX management.
     */
    public void testStartStopManagementFromApplication()
        throws Exception
    {
        ObjectName appMgmtBean = getApplicationManagementMBean();
        
        // Derby should be running.
        assertBooleanAttribute(true, appMgmtBean, "ManagementActive");
        
        assertNotNull(getAttribute(appMgmtBean, "SystemIdentifier"));
        
        // now shutdown Derby.
        if (isPlatformJMXClient())
        {
            // Derby is running embedded within the same virtual machine
            getTestConfiguration().shutdownEngine();
        }
        else
        {
            // TODO: Need to stop derby running on the remote
            // machine but leave the vm up. How to do that?
            return;
        }
        
        // Ensure that the state of Derby's management cannot change
        // since Derby is not running and that the application's MBean
        // continues to work.
        assertBooleanAttribute(false, appMgmtBean, "ManagementActive");
        assertNull(getAttribute(appMgmtBean, "SystemIdentifier"));
        
        invokeOperation(appMgmtBean, "startManagement");
        assertBooleanAttribute(false, appMgmtBean, "ManagementActive");
        assertNull(getAttribute(appMgmtBean, "SystemIdentifier"));
        
        invokeOperation(appMgmtBean, "stopManagement");
        assertBooleanAttribute(false, appMgmtBean, "ManagementActive");
        assertNull(getAttribute(appMgmtBean, "SystemIdentifier"));
    }
}
