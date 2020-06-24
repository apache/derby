/*
 *
 * Derby - Class org.apache.derbyTesting.system.oe.test.OETest
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
package org.apache.derbyTesting.system.oe.test;


import java.sql.Connection;
import java.sql.SQLException;
import junit.framework.Test;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.system.oe.direct.SimpleNonStandardOperations;
import org.apache.derbyTesting.system.oe.run.Checks;
import org.apache.derbyTesting.system.oe.run.Populate;
import org.apache.derbyTesting.system.oe.run.Schema;

/**
 * Test the basic functionality of the Order Entry test
 * using scale 1 as a functional test, to ensure that changes to the
 * database do not break the performance test.
 *
 */
public class OETest extends BaseJDBCTestCase {

   	
    public OETest(String name) {
        super(name);
    }
    
      
    public static Test suite() {
//IC see: https://issues.apache.org/jira/browse/DERBY-6590
        BaseTestSuite suite = new BaseTestSuite("Order Entry");
        
//IC see: https://issues.apache.org/jira/browse/DERBY-2095
        suite.addTest(Schema.suite());
        // Test load part
        suite.addTest(new Populate("testLoad"));
        // perform checks tests.
        suite.addTest(Checks.suite());

//IC see: https://issues.apache.org/jira/browse/DERBY-2094
        suite.addTestSuite(OperationsTester.class);
        suite.addTestSuite(OETest.class);
        
        // Ensure the transactions left the data in a consistent state
        suite.addTest(Checks.consistencyChecks());
                
        return new CleanDatabaseTestSetup(suite);
    }
    
    private SimpleNonStandardOperations getNSOps() throws SQLException
    {
        return new SimpleNonStandardOperations(
                getConnection(), Connection.TRANSACTION_SERIALIZABLE);
    }
    
    /**
     * Test the non-standard customer inquiry transaction
     */
    public void testCustomerInquiry() throws SQLException
    {
        SimpleNonStandardOperations nsops = getNSOps();
                   
        for (int i = 0; i < 20; i++)
            nsops.customerInquiry(1);
        
        nsops.close();
    }
    
    /**
     * Test the non-standard customer address change transaction
     */
    public void testCustomerAddressChange() throws SQLException
    {
        SimpleNonStandardOperations nsops = getNSOps();
                   
        for (int i = 0; i < 20; i++)
            nsops.customerAddressChange(1);
        
        nsops.close();
    }
}
