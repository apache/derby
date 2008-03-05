/*
 *
 * Derby - Class org.apache.derbyTesting.system.oe.run.DriverUtility
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
import java.sql.DriverManager;
import java.sql.Connection;
import org.apache.derbyTesting.system.oe.client.Load;
import org.apache.derbyTesting.system.oe.load.SimpleInsert;
import org.apache.derbyTesting.system.oe.util.HandleCheckError;
import org.apache.derbyTesting.system.oe.util.OEChecks;

/**
 * Driver to do the load phase for the Order Entry benchmark.
 *
 * This class takes in following arguments currently:
 * Usage: java org.apache.derbyTesting.system.oe.run.DriverUtility options
 * Options:
 * <OL>
 * <LI>-scale warehouse scaling factor. Takes a short value. If not specified 
 * defaults to 1
 * <LI>-doChecks check consistency of data, takes a boolean value. If not specified, defaults to true
 * <LI>-driver jdbc driver class to use
 * <LI>-dbUrl  database connection url 
 * <LI>-help prints usage
 * </OL>
 *
 * To load database with scale of 2
 * and to not do any checks, the command to run the test is as follows:
 * <BR>
 * java org.apache.derbyTesting.system.oe.run.DriverUtility -driver org.apache.derby.jdbc.ClientDriver -dbUrl 'jdbc:derby://localhost:1527/db' -scale 2 -doChecks false
 * <BR>
 */
public class DriverUtility {

    /**
     * Database connection
     */
    private Connection conn = null;
    /**
     * Warehouse scale factor
     */
    private static short scale = 1;

    /**
     * Database connection url
     */
    private static String dbUrl = "jdbc:derby:wombat;create=true";

    /**
     * JDBC Driver class
     */
    private static String driver = "org.apache.derby.jdbc.EmbeddedDriver";

    /**
     * flag to indicate if we should perform consistency, cardinality checks
     * after the load
     */
    private static boolean doChecks = true;

    /**
     * Create a test case with the given name.
     */
    public DriverUtility() {

        try {
            Class.forName(driver);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    /**
     * Run OE load
     * @param args supply arguments for benchmark.
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        parseArgs(args);
        DriverUtility oe = new DriverUtility();
        oe.populate();
        if ( doChecks )
            oe.allChecks();
        oe.cleanup();
    }

    /**
     * @return the connection
     * @throws SQLException
     */
    public Connection getConnection() throws SQLException {
        if ( conn == null)
        {
            System.out.println("dbUrl="+dbUrl);
            conn = DriverManager.getConnection(dbUrl);
        }
        return conn;
    }

    /**
     * Populate the OE database.
     * Assumption is that the schema is already loaded
     * in the database.
     */
    public void populate() throws Exception {
        // Use simple insert statements to insert data.
        // currently only this form of load is present, once we have
        // different implementations, the loading mechanism will need
        // to be configurable taking an option from the command line
        // arguments.
        Load loader = new SimpleInsert();
        loader.setupLoad(getConnection(), scale);
        long start = System.currentTimeMillis();
        loader.populateAllTables();
        long stop = System.currentTimeMillis();
        System.out.println("Time to load (ms)=" + (stop - start));
    }

    /**
     * Do the necessary checks to see if database is in consistent state
     */
    public void allChecks() throws Exception {
        OEChecks checks = new OEChecks();
        checks.initialize(new HandleCheckError(), getConnection(), scale);
        long start = System.currentTimeMillis();
        checks.checkAllRowCounts();
        long stop = System.currentTimeMillis();
        System.out.println("Time to do checks (ms)=" + (stop - start));
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
            } else if (arg.equals("-driver")) {
                driver = args[++i];
            } else if (arg.equals("-dbUrl")) {
                dbUrl = args[++i];
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

    /**
     * prints the usage
     */
    private static void printUsage() {
        System.out.println("Usage: java org.apache.derbyTesting.system.oe." +
                        "run.DriverUtility options");
        System.out.println();
        System.out.println("Options:");
        System.out.println("  -scale warehouse scaling factor. " +
                        "Takes a short value. If not specified defaults to 1");
        System.out.println("  -doChecks  should consistency checks be run" +
                        " on the database. Takes a boolean value");
        System.out.println("  -driver  the class of the jdbc driver");
        System.out.println("  -dbUrl  the database connection url");
        System.out.println("  -help prints usage");
        System.out.println();
    }

    /**
     * cleanup resources. 
     * @throws SQLException
     */
    public void cleanup() throws SQLException {
        if (conn != null)
            conn.close();
    }

}
