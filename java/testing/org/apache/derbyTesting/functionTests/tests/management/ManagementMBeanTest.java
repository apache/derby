/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.management.ManagementMBeanTest

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

import javax.management.ObjectName;

import junit.framework.Test;


/**
 * Test the ManagementMBean interface provided by Derby
 * which has two implementations. A built in one and
 * one that can be created by a user.
 */
public class ManagementMBeanTest extends MBeanTest {
    
    public ManagementMBeanTest(String name) {
        super(name);
    }
    
    public static Test suite() {
        
        return MBeanTest.suite(ManagementMBeanTest.class, 
                                        "ManagementMBeanTest:client");
    }
    
    public void testStartStopManagementFromApplication()
        throws Exception
    {
        ObjectName appMgmtBean = getApplicationManagementMBean();
        startStopManagement(appMgmtBean);
    }
    
    private void startStopManagement(ObjectName mbean) throws Exception
    {
        // Test fixtures start off active
        assertBooleanAttribute(true, mbean, "ManagementActive");
        
        // Should be a no-op
        invokeOperation(mbean, "startManagement");
        assertBooleanAttribute(true, mbean, "ManagementActive");
        
        // now stop management
        invokeOperation(mbean, "stopManagement");
        assertBooleanAttribute(false, mbean, "ManagementActive");
        
        // now start management again
        invokeOperation(mbean, "startManagement");
        assertBooleanAttribute(true, mbean, "ManagementActive");

    }
}
