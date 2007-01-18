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
import org.apache.derbyTesting.system.oe.load.SimpleInsert;
import org.apache.derbyTesting.system.oe.client.Load;

/**
 * Test the basic functionality of the Order Entry test
 * as a functional test, to ensure that changes to the
 * database do not break the performance test.
 *
 */
public class OETest extends BaseJDBCTestCase {

	short scale = 1;
   	LoadTester tester;
   	
    public OETest(String name) {
        super(name);
    }
    
    public OETest(String name, short scale)
    {
    	super(name);
    	this.scale=scale;
 
    }
    
    public static Test suite() {
        TestSuite suite = new TestSuite("Order Entry");
        
        suite.addTest(new OETest("testSchema"));
        suite.addTest(new OETest("testPrimaryKey"));
        suite.addTest(new OETest("testForeignKey"));
        suite.addTest(new OETest("testIndex"));
        
        // Test load part and the cardinality after the
        // population.
        suite.addTest(new OETest("testInsertStmtLoad",(short)1));
        suite.addTest(new OETest("testWarehouseRows"));
        suite.addTest(new OETest("testStockRows"));
        suite.addTest(new OETest("testItemRows"));
        suite.addTest(new OETest("testCustomerRows"));
        suite.addTest(new OETest("testDistrictRows"));
        suite.addTest(new OETest("testOrdersRows"));
        suite.addTest(new OETest("testNewOrdersRows"));
        suite.addTest(new OETest("testOrderLineRows"));
        suite.addTest(new OETest("testHistoryRows"));
        
        
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
     * Test the Simple Load 
     */
    public void testInsertStmtLoad() throws Exception
    {
       loadTest(new SimpleInsert(getConnection(),scale));
    }
    
    /**
     * Test load part. 
     * Includes 
     * 1) populating the tables
     * 2) Check if the cardinality of all tables is per the 
     * TPC-C requirement as expected.
     * @param p  - Implementation of the Load to use for populating the database
     * @throws Exception
     */
    public void loadTest(Load p) throws Exception
    {
    	LoadTester tester = new LoadTester(p);
    	tester.test();
    }
    
    /**
     * Test cardinality of WAREHOUSE table
     * 
     * @throws Exception
     */
    public void testWarehouseRows() throws Exception
    {
    	getLoadTester().testWarehouseRows();
    }

    /**
     * Test cardinality of CUSTOMER table
     * 
     * @throws Exception
     */
    public void testCustomerRows() throws Exception
    {
    	getLoadTester().testCustomerRows();
    }

    /**
     * Test cardinality of DISTRICT table
     * 
     * @throws Exception
     */
    public void testDistrictRows() throws Exception
    {
    	getLoadTester().testDistrictRows();
    }
    
    /**
     * Test cardinality of HISTORY table
     * 
     * @throws Exception
     */
    public void testHistoryRows() throws Exception
    {
    	getLoadTester().testHistoryRows();
    }

    /**
     * Test cardinality of ITEM table
     * 
     * @throws Exception
     */
    public void testItemRows() throws Exception
    {
    	getLoadTester().testItemRows();
    }
    
    /**
     * Test cardinality of NEWORDERS table
     * 
     * @throws Exception
     */
    public void testNewOrdersRows() throws Exception
    {
    	getLoadTester().testNewOrdersRows();
    }
    
    /**
     * Test cardinality of ORDERLINE table
     * 
     * @throws Exception
     */
    public void testOrderLineRows() throws Exception
    {
    	getLoadTester().testOrderLineRows();
    }
    
    /**
     * Test cardinality of STOCK table
     * 
     * @throws Exception
     */
    public void testStockRows() throws Exception
    {
    	getLoadTester().testStockRows();
    }

    /**
     * Test cardinality of ORDERS table
     * 
     * @throws Exception
     */
    public void testOrdersRows() throws Exception
    {
    	getLoadTester().testOrdersRows();
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
    
    /**
     * 
     * @return LoadTester to test the load part.
     * @throws SQLException
     */
    private LoadTester getLoadTester()
        throws SQLException
    {
    	if (tester != null)
    		return tester;
    	
    	tester = new LoadTester(new SimpleInsert(getConnection(),(short)1));
    	return tester;
    }
}
