/*
 *
 * Derby - Class org.apache.derbyTesting.functionTests.tests.management.CustomMBeanServerBuilderTest
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, 
 * software distributed under the License is distributed on an 
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, 
 * either express or implied. See the License for the specific 
 * language governing permissions and limitations under the License.
 */

package org.apache.derbyTesting.functionTests.tests.management;
import java.sql.SQLException;
import java.util.Properties;

import junit.framework.Test;

import org.apache.derbyTesting.functionTests.tests.lang.SimpleTest;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.BaseTestCase;
import org.apache.derbyTesting.junit.DatabasePropertyTestSetup;
import org.apache.derbyTesting.junit.SystemPropertyTestSetup;
import org.apache.derbyTesting.junit.TestConfiguration;

public class CustomMBeanServerBuilderTest extends BaseJDBCTestCase {

    public CustomMBeanServerBuilderTest(String name) {
        super(name);
  
    }
    
    /**
     * Test that Derby will boot when user sets
     * javax.management.builder.initial
     * @throws SQLException
     */
    public void testDerbyBootWithCusomMBeanServerBuilderDerby3887() throws SQLException {
        getConnection();
   
    }
    
    public static Test suite() {
        Properties props = new Properties();
        props.setProperty("javax.management.builder.initial",
                "org.apache.derbyTesting.functionTests.tests.management.CustomMBeanServerBuilder");
        Test suite = TestConfiguration.embeddedSuite(CustomMBeanServerBuilderTest.class);   
    return new SystemPropertyTestSetup(suite,props);
    }

}
