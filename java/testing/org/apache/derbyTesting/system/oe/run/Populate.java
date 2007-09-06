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

import java.sql.SQLException;

import javax.sql.DataSource;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.derbyTesting.junit.JDBCDataSource;
import org.apache.derbyTesting.junit.JDBCPerfTestCase;
import org.apache.derbyTesting.system.oe.client.Load;
import org.apache.derbyTesting.system.oe.load.ThreadInsert;

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
 * <LI>-loaderThreads Number of threads to populate tables, defaults to number of cores
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
    static short scale = 1;
    
    /**
     * Number of threads to load the data.
     */
    static int loaderThreads;

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
     * Create a test case with the given name.
     * 
     * @param name
     *            of the test case.
     */
    public Populate(String name) {
        super(name);
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
    static void parseArgs(String[] args) {
        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            if (arg.equals("-scale")) {
                scale = Short.parseShort(args[++i]);
            } else if (arg.equals("-createConstraintsBeforeLoad")) {
                createConstraintsBeforeLoad = (args[++i].equals("false")? false:true);
            } else if (arg.equals("-doChecks")) {
                doChecks = (args[++i].equals("false")? false:true);
            } else if (arg.equals("-loaderThreads")) {
                loaderThreads = Integer.parseInt(args[++i]);
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
        System.out.println("  -loaderThreads number of threads used to populate database, defaults to number of cpu cores)");
        System.out.println("  -help prints usage");
        System.out.println();
    }

    /**
     * junit tests to do the OE load.
     * 
     * @return the tests to run
     */
    public static Test suite() {

        TestSuite suite = new TestSuite("Order Entry");
        
        suite.addTest(new Populate("testCreateDB"));

        // Create Schema
        Schema.addBaseSchema(suite);
        if (createConstraintsBeforeLoad)
            Schema.addConstraints(suite);
        
        // this will populate db
        suite.addTest(new Populate("testLoad"));

        if (!createConstraintsBeforeLoad)
            Schema.addConstraints(suite);

        if (doChecks)
        {
            //check if cardinality of rows are OK after
            //population phase.
            suite.addTest(Checks.checkAllRowCounts(scale));
            // consistency checks.
            suite.addTest(Checks.consistencyChecks());
        }
        
        return suite;
    }

    public void testCreateDB() throws SQLException
    {
        DataSource ds = JDBCDataSource.getDataSource();
        
        JDBCDataSource.setBeanProperty(ds,
                "createDatabase", "create");
 
        ds.getConnection().close();

    }

    /**
     * test the initial database load
     * 
     * @throws Exception
     */
    public void testLoad() throws Exception {
        
        // Use simple insert statements to insert data.
        // currently only this form of load is present, once we have 
        // different implementations, the loading mechanism will need
        // to be configurable taking an option from the command line
        // arguments.
        DataSource ds = JDBCDataSource.getDataSource();
       
        Load loader = new ThreadInsert(ds);
        loader.setupLoad(getConnection(), scale);
        if (loaderThreads > 0)
            loader.setThreadCount(loaderThreads);
        
        loader.populateAllTables();

        // Way to populate data is extensible. Any other implementation
        // of org.apache.derbyTesting.system.oe.client.Load can be used
        // to load data. configurable using the oe.load.insert property
        // that is defined in oe.properties
        // One extension would be to have an implementation that 
        // uses bulkinsert vti to load data.
    }

}
