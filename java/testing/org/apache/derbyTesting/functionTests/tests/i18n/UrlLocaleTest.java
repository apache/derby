/*
 *
 * Derby - Class org.apache.derbyTesting.functionTests.tests.lang.UrlLocaleTest
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
 * i18n governing permissions and limitations under the License.
 */

package org.apache.derbyTesting.functionTests.tests.i18n;

import java.net.MalformedURLException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.SupportFilesSetup;
import org.apache.derbyTesting.junit.TestConfiguration;



public class UrlLocaleTest extends BaseJDBCTestCase {
    
    public UrlLocaleTest(String name) {
        super(name);
    }


    
    /**
     * Shutdown on tearDown to make sure all databases can be removed.
     * @throws Exception 
     * @see org.apache.derbyTesting.junit.BaseJDBCTestCase#tearDown()
     */
    public void tearDown() throws Exception {
        super.tearDown();
        TestConfiguration.getCurrent().shutdownEngine();
        // Reregister driver for any subsequent tests
        String driverClass =
                TestConfiguration.getCurrent().getJDBCClient().getJDBCDriverName();
        Class.forName(driverClass).newInstance();
    }
    
    /**
     * Test valid territory settings on URL
     * 
     */
    public void testURLLocale()  throws SQLException, MalformedURLException {
        // check this current database was created with the default locale
        Statement s = createStatement();
        s.executeUpdate("call checkRDefaultLoc()");
        
            // create a swiss database
        String url = getReadWriteJDBCURL("swissdb");
        url += ";create=true;territory=fr_CH";
        Connection locConn = DriverManager.getConnection(url);
        createLocaleProcedures(locConn);
        Statement locStatement = locConn.createStatement();
        locStatement.executeUpdate("call checkDatabaseLoc('fr_CH')");
        locStatement.close();
        locConn.close();
        
        //-- create a Hindi in India database (hi_IN)
        
        url = getReadWriteJDBCURL("hindi");
        url += ";create=true;territory=hi_IN";
        locConn = DriverManager.getConnection(url);
        createLocaleProcedures(locConn);
        locStatement = locConn.createStatement();
        locStatement.executeUpdate("call checkDatabaseLoc('hi_IN')");
        locStatement.close();
        locConn.close();
       //- now try one with a variant
       // -- create a English in Israel database for JavaOS en_IL_JavaOS
        url = getReadWriteJDBCURL("Israel");
        url += ";create=true;territory=en_IL_JavaOS";
        locConn = DriverManager.getConnection(url);
        createLocaleProcedures(locConn);
        locStatement = locConn.createStatement();
        locStatement.executeUpdate("call checkDatabaseLoc('en_IL_JavaOS')");
        locStatement.close();
        locConn.close();
        
        // now try with just a language - we support this
        // as some vms do.
        url = getReadWriteJDBCURL("bacon");
        url += ";create=true;territory=da";
        locConn = DriverManager.getConnection(url);
        createLocaleProcedures(locConn);
        locStatement = locConn.createStatement();
        locStatement.executeUpdate("call checkDatabaseLoc('da')");
        locStatement.close();
        locConn.close();
                
    }

    
    
    /**
     * Test invalid territory settings
     */
    public void testUrlLocaleNegative() throws SQLException {
        //Connection without territory specified in territory attribute        
        String url = TestConfiguration.getCurrent().getJDBCUrl("../extinout/fail1");
        url += ";create=true;territory=";
        testInvalidTerritoryFormat(url);
        //- database will not have been created so this connection will fail
        url = TestConfiguration.getCurrent().getJDBCUrl("../extinout/fail1");
        try {
            Connection locConn = DriverManager.getConnection(url);
            fail("Database connect " + url + " should fail because db does not exist");
        }    catch (SQLException se ) {   
            assertSQLState("XJ004", se);
          }
        //Invalid territory specification
        testInvalidTerritoryFormat("en_");
        testInvalidTerritoryFormat("en_d");
        testInvalidTerritoryFormat("en-US");
        
    }

    private void testInvalidTerritoryFormat(String territory) {
        try {
            String url = TestConfiguration.getCurrent().getJDBCUrl("../extinout/fail3");
            url += ";create=true;territory=" + territory;
            Connection locConn = DriverManager.getConnection(url);
            fail("connection without territory: " + url + "should have failed");
        } catch (SQLException se ) {
          assertSQLState("XJ041", se);
          assertSQLState("XBM0X", se.getNextException());
        }
    }
    
    /**
     * Get JDBC URL for database to be accessed in the read-write directory
     * @param dbname short database name to be created 
     * @return the JDBC URL for the database
     */
    private static String getReadWriteJDBCURL(String dbname)
   {
        return TestConfiguration.getCurrent().
        getJDBCUrl(SupportFilesSetup.getReadWriteFileName(dbname));
    }
    
    /**
     * Create procedures to test current territory value
     * 
     */
    private static void createLocaleProcedures(Connection conn) throws SQLException {
        Statement s = conn.createStatement();
        s.executeUpdate("create procedure checkDatabaseLoc(in locale " +
        "char(12)) parameter style java language java external name " +
                "'org.apache.derbyTesting.functionTests.tests.i18n." +
                "DefaultLocale.checkDatabaseLocale'");
        s.executeUpdate("create procedure checkRDefaultLoc() parameter " +
                    "style java language java external name " +
                    "'org.apache.derbyTesting.functionTests.tests.i18n." +
                    "DefaultLocale.checkRDefaultLocale'");

    }
    
    public static Test suite() {
        TestSuite suite = new TestSuite();
        suite.addTestSuite(UrlLocaleTest.class);
        Test tsuite =  new CleanDatabaseTestSetup(suite) {
            /**
             * Creates the table used in the test cases.
             *
             */
            protected void decorateSQL(Statement s) throws SQLException {
                createLocaleProcedures(s.getConnection());
            }
        };
        tsuite = new SupportFilesSetup(tsuite);
        return tsuite;
    }
 
}

