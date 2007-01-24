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

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.PrivilegedActionException;
import java.sql.SQLException;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.system.oe.direct.Standard;
import org.apache.derbyTesting.system.oe.client.Operations;
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
        TestSuite suite = new TestSuite("Order Entry");
        
        suite.addTest(Schema.suite());
        // Test load part
        suite.addTest(new Populate("testLoad"));
        // perform checks tests.
        suite.addTest(Checks.suite());

        suite.addTest(new OETest("testStandardOperations"));
        
        return new CleanDatabaseTestSetup(suite);
    }

    /**
     * Test the Standard implementations of the business transactions.
     */
    public void testStandardOperations() throws Exception
    {
        operationsTest(new Standard(getConnection()));
    }
    /**
     * Run a standard set of tests against an implementation
     * of Operations using the OperationsTester class.
     */
    private static void operationsTest(Operations ops) throws Exception
    {
        OperationsTester tester = new OperationsTester(ops);
        tester.test();
        ops.close();
    }
    


}
