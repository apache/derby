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
import org.apache.derbyTesting.system.oe.client.Operations;
import org.apache.derbyTesting.system.oe.direct.Standard;

/**
 * Test the basic functionality of the Order Entry test
 * as a functional test, to ensure that changes to the
 * database do not break the performance test.
 *
 */
public class OETest extends BaseJDBCTestCase {

    public OETest(String name) {
        super(name);
    }
    
    public static Test suite() {
        TestSuite suite = new TestSuite("Order Entry");
        
        suite.addTest(new OETest("testSchema"));
        suite.addTest(new OETest("testPrimaryKey"));
        suite.addTest(new OETest("testForeignKey"));
        suite.addTest(new OETest("testIndex"));
        
        suite.addTest(new OETest("testStandardOperations"));
        
        return new CleanDatabaseTestSetup(suite);
    }
    
    /**
     * Test setting up the base tables.
     */
    public void testSchema() throws UnsupportedEncodingException, SQLException, PrivilegedActionException, IOException
    {
        script("schema.sql");
    }
    /**
     * Test setting up the primary keys.
     */
    public void testPrimaryKey() throws UnsupportedEncodingException, SQLException, PrivilegedActionException, IOException
    {
        script("primarykey.sql");
    }
    /**
     * Test setting up the foreign keys.
     */
    public void testForeignKey() throws UnsupportedEncodingException, SQLException, PrivilegedActionException, IOException
    {
        script("foreignkey.sql");
    }
    /**
     * Test setting up the remaining indexes.
     */
    public void testIndex() throws UnsupportedEncodingException, SQLException, PrivilegedActionException, IOException
    {
        script("index.sql");
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
    }
    
    /**
     * Run a Order Entry script.
     */
    private void script(String name) throws UnsupportedEncodingException, SQLException, PrivilegedActionException, IOException {
        
        String script = "org/apache/derbyTesting/system/oe/schema/" + name;
        int errorCount = runScript(script,"US-ASCII");
        assertEquals("Errors in script ",0, errorCount);
    }
}
