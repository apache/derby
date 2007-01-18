/*
 * 
 * Derby - Class org.apache.derbyTesting.system.oe.run.Populate
 * 
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.derbyTesting.system.oe.run;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.PrivilegedActionException;
import java.sql.SQLException;

import junit.framework.Assert;
import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.derbyTesting.junit.JDBCPerfTestCase;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.system.oe.client.Load;
import org.apache.derbyTesting.system.oe.load.SimpleInsert;


/**
 * Driver to do the load phase for the Order Entry benchmark.
 * 
 * This class takes in following arguments currently:
 * Usage: java org.apache.derbyTesting.system.oe.run.Populate options
 * Options:
 * <OL>
 * <LI>-scale warehouse scaling factor. Takes a short value. If not specified defaults to 1
 * <LI>-createConstraintsBeforeLoad create constraints before initial load of data, takes a boolean value. If not specified, defaults to true
 * <LI>-doChecks check consistency of data, takes a boolean value. If not specified, defaults to true
 * <LI>-help prints usage
 * </OL>
 * 
 * To load database with scale of 2, to load constraints after the population, 
 * and to not do any checks, the command to run the test is as follows:
 * <BR>
 * java org.apache.derbyTesting.system.oe.run.Populate -scale 2 -doChecks false -createConstraintsBeforeLoad false
 * <BR>
 * This class uses the junit performance framework in Derby and 
 * the tests the performance of the following operations. 
 * 
 * <OL>
 * <LI> create schema with or without constraints (configurable)
 * <LI> populate the schema
 * <LI> Check the cardinality of the tables.
 * </OL>
 */
public class Populate extends JDBCPerfTestCase {

    /**
     * Warehouse scale factor
     */
    private static short scale = 1;

    /**
     * flag to indicate if we should create constraints before loading data
     */
    private static boolean createConstraintsBeforeLoad = true;
    
    /**
     * flag to indicate if we should perform consistency, cardinality checks
     * after the load
     */
    private static boolean doChecks = true;
    
    /**
     * Load implementation used to populate the database
     */
    private Load loader;

    /**
     * Create a test case with the given name.
     * 
     * @param name
     *            of the test case.
     */
    public Populate(String name) {
        super(name);
    }


    /**
     * Do the initial setup required Initialize the appropriate implementation
     * for the Load phase.
     */
    public void setUp() throws Exception {
        // Use simple insert statements to insert data.
        // currently only this form of load is present, once we have 
        // different implementations, the loading mechanism will need
        // to be configurable taking an option from the command line
        // arguments.
       loader = new SimpleInsert(getConnection(), scale);
    }

    /**
     * Run OE load
     * @param args supply arguments for benchmark.
     * @throws Exception
     */
    public static void main(String[] args) throws Exception
    {
        parseArgs(args);
        String[] tmp= {"org.apache.derbyTesting.system.oe.run.Populate"};
        
        // run the tests.
        junit.textui.TestRunner.main(tmp);
    }
    
    /**
     * parse arguments.
     * @param args arguments to parse
     */
    private static void parseArgs(String[] args) {
        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            if (arg.equals("-scale")) {
                scale = Short.parseShort(args[++i]);
            } else if (arg.equals("-createConstraintsBeforeLoad")) {
                createConstraintsBeforeLoad = (args[++i].equals("false")? false:true);
            } else if (arg.equals("-doChecks")) {
                doChecks = (args[++i].equals("false")? false:true);
            } else if (arg.equals("-help")) {
                printUsage();
                System.exit(0);
            } else {
                System.err.println("Invalid option: " + args[i]);
                System.exit(1);
            }
        }
        
    }

    private static void printUsage() {
        System.out.println("Usage: java org.apache.derbyTesting.system.oe.run.Populate options");
        System.out.println();
        System.out.println("Options:");
        System.out.println("  -scale warehouse scaling factor. Takes a short value. If not specified defaults to 1");
        System.out.println("  -createConstraintsBeforeLoad create constraints before initial load of data, takes a boolean value. If not specified, defaults to true)");
        System.out.println("  -doChecks check consistency of data, takes a boolean value. If not specified, defaults to true)");
        System.out.println("  -help prints usage");
        System.out.println();
    }

    /**
     * junit tests to do the OE load.
     * 
     * @return the tests to run
     */
    public static Test suite() throws Exception {
        TestSuite suite = new TestSuite("Order Entry");

        // Create Schema
        suite.addTest(new Populate("testSchema"));
        if (createConstraintsBeforeLoad)
            addConstraints(suite);
        // this will populate db
        suite.addTest(new Populate("testLoad"));

        if (!createConstraintsBeforeLoad)
            addConstraints(suite);

        if (doChecks)
        {
            //check if cardinality of rows are OK after
            //population phase.
            suite.addTest(new Populate("testWarehouseRows"));
            suite.addTest(new Populate("testStockRows"));
            suite.addTest(new Populate("testItemRows"));
            suite.addTest(new Populate("testCustomerRows"));
            suite.addTest(new Populate("testDistrictRows"));
            suite.addTest(new Populate("testOrdersRows"));
            suite.addTest(new Populate("testNewOrdersRows"));
            suite.addTest(new Populate("testOrderLineRows"));
            suite.addTest(new Populate("testHistoryRows"));
        }
        
        return suite;
    }

    /**
     * Add constraint tests to suite.
     * 
     * @param suite
     */
    private static void addConstraints(TestSuite suite) {
        suite.addTest(new Populate("testPrimaryKey"));
        suite.addTest(new Populate("testForeignKey"));
        suite.addTest(new Populate("testIndex"));

    }

    /**
     * Test setting up the base tables.
     */
    public void testSchema() throws UnsupportedEncodingException, SQLException,
    PrivilegedActionException, IOException {
        script("schema.sql");
    }

    /**
     * Test setting up the primary keys.
     */
    public void testPrimaryKey() throws UnsupportedEncodingException,
    SQLException, PrivilegedActionException, IOException {
        script("primarykey.sql");
    }

    /**
     * Test setting up the foreign keys.
     */
    public void testForeignKey() throws UnsupportedEncodingException,
    SQLException, PrivilegedActionException, IOException {
        script("foreignkey.sql");
    }

    /**
     * Test setting up the remaining indexes.
     */
    public void testIndex() throws UnsupportedEncodingException, SQLException,
    PrivilegedActionException, IOException {
        script("index.sql");
    }

    /**
     * test the initial database load
     * 
     * @throws Exception
     */
    public void testLoad() throws Exception {
        loader.populateAllTables();

        // Way to populate data is extensible. Any other implementation
        // of org.apache.derbyTesting.system.oe.client.Load can be used
        // to load data. configurable using the oe.load.insert property
        // that is defined in oe.properties
        // One extension would be to have an implementation that 
        // uses bulkinsert vti to load data.

    }

    /**
     * Test cardinality of WAREHOUSE table
     * 
     * @throws Exception
     */
    public void testWarehouseRows() throws Exception {
        checkCountStar("WAREHOUSE", loader.getScale());
    }

    /**
     * Test cardinality of STOCK table
     * 
     * @throws Exception
     */
    public void testStockRows() throws Exception {
        checkCountStar("STOCK", Load.STOCK_COUNT_W * loader.getScale());
    }

    /**
     * Test cardinality of ORDERS table
     * 
     * @throws Exception
     */
    public void testOrdersRows() throws Exception {
        checkCountStar("ORDERS", Load.ORDERS_COUNT_W * loader.getScale());
    }

    /**
     * Test cardinality of DISTRICT table
     * 
     * @throws Exception
     */
    public void testDistrictRows() throws Exception {
        checkCountStar("DISTRICT", Load.DISTRICT_COUNT_W * loader.getScale());
    }

    /**
     * Test cardinality of CUSTOMER table
     * 
     * @throws Exception
     */
    public void testCustomerRows() throws Exception {
        checkCountStar("CUSTOMER", Load.CUSTOMER_COUNT_W * loader.getScale());
    }

    /**
     * Test cardinality of ITEM table
     * 
     * @throws Exception
     */
    public void testItemRows() throws Exception {
        checkCountStar("ITEM", Load.ITEM_COUNT);
    }

    /**
     * Test cardinality of NEWORDERS table
     * 
     * @throws Exception
     */
    public void testNewOrdersRows() throws Exception {
        checkCountStar("NEWORDERS", Load.NEWORDERS_COUNT_W * loader.getScale());
    }

    /**
     * Test cardinality of HISTORY table
     * 
     * @throws Exception
     */
    public void testHistoryRows() throws Exception {
        checkCountStar("HISTORY", Load.HISTORY_COUNT_W * loader.getScale());
    }

    /**
     * Test cardinality of ORDERLINE table
     * 
     * @throws Exception
     */
    public void testOrderLineRows() throws Exception {
        checkWithinOnePercent("ORDERLINE", Load.ORDERLINE_COUNT_WV
                * loader.getScale());
    }

    /**
     * Check if number of rows in table is as expected
     * 
     * @param table -
     *            table on which to execute the query
     * @param expected -
     *            expected number of rows
     * @throws Exception
     */
    private void checkCountStar(String table, int expected) throws Exception {
        Assert.assertEquals("Number of rows loaded for " + table
                + " not correct", expected, loader.rowsInTable(table));
    }

    /**
     * Check if number of rows in table is within one percent of expected value
     * 
     * @param table -
     *            table on which to execute the query
     * @param expected -
     *            expected number of rows
     * @throws Exception
     */
    private void checkWithinOnePercent(String tableName, int expected)
    throws Exception {

        double count = loader.rowsInTable(tableName);

        double low = ((double) expected) * 0.99;
        double high = ((double) expected) * 1.01;

        Assert.assertEquals("Initial rows" + count + " in " + tableName
                + " is out of range.[" + low + "-" + high + "]", false,
                ((count < low) || (count > high)));

    }

    /**
     * Run a Order Entry script.
     */
    private void script(String name) throws UnsupportedEncodingException,
    SQLException, PrivilegedActionException, IOException {

        String script = "org/apache/derbyTesting/system/oe/schema/" + name;
        int errorCount = runScript(script, "US-ASCII");
        assertEquals("Errors in script ", 0, errorCount);
    }
}
