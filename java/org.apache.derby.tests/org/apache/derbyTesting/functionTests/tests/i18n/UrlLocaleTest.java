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
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.Locale;
import junit.framework.Test;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.LocaleTestSetup;
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
        Class<?> clazz = Class.forName(driverClass);
        clazz.getConstructor().newInstance();
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
        String url = getReadWriteJDBCURL("fail1");
        url += ";create=true;territory=";
        checkInvalidTerritoryFormat(url);
        //- database will not have been created so this connection will fail
        url = getReadWriteJDBCURL("fail1");
        try {
            DriverManager.getConnection(url);
            fail("Database connect " + url + " should fail because db does not exist");
        }    catch (SQLException se ) {   
            assertSQLState("XJ004", se);
          }
        //Invalid territory specification
        checkInvalidTerritoryFormat("en_");
        checkInvalidTerritoryFormat("en_d");
        checkInvalidTerritoryFormat("en-US");
        
    }

    /**
     * Test valid message resolution for an unknown Locale.
     * converted from i18n/messageLocale.sql
     *
     * This test case must run in a decorator that sets the locale to one
     * that is not recognized by Derby.
     */
    public void messageLocale_unknown() throws SQLException {
        String url = getReadWriteJDBCURL("rrTTdb");
        url += ";create=true";
        Connection locConn = DriverManager.getConnection(url);
        Statement s = locConn.createStatement();
        createLocaleProcedures(locConn);
        // check this current database was created with the default locale rr_TT
        s.executeUpdate("call checkDefaultLoc()");
        // check database Locale
        s.executeUpdate("call checkDatabaseLoc('rr_TT')");
        // Expect an error in English because rr_TT has no translated messages.
        // Language is determined by choosing a random word (that we hope 
        // won't change) in the current 
        try {
            s.executeUpdate("create table t1 oops (i int)");
        } catch (SQLException se) {
            assertSQLState("42X01", se);
            assertTrue("Expected English Message with \"Encountered\" " ,
                      (se.getMessage().indexOf("Encountered") != -1));
            
        }
        // Setup for warning
        s.executeUpdate("create table t2 (i int)");
        s.executeUpdate("create index i2_a on t2(i)");

        // Expect WARNING to also be English. Index is a duplicate
        s.executeUpdate("create index i2_b on t2(i)");
        SQLWarning sqlw = s.getWarnings();
        assertSQLState("01504", sqlw);
        assertTrue("Expected English warning", 
                sqlw.getMessage().indexOf("duplicate") != -1);
        
        s.close();
        locConn.close();
    }
        
    /**
     * Test valid message resolution for German Locale.
     * converted from i18n/messageLocale.sql
     *
     * This test case must run in a decorator that sets the default locale
     * to Locale.GERMANY.
     */
    public void messageLocale_Germany() throws SQLException {
        //create a database with a locale that has a small
        // number of messages. Missing ones will default to
        // the locale of the default locale: German;
        String url = getReadWriteJDBCURL("qqPPdb");
        url += ";create=true;territory=qq_PP_testOnly";
        Connection locConn = DriverManager.getConnection(url);
        Statement s = locConn.createStatement();
        s.executeUpdate("create table t2 (i int)");
        s.executeUpdate("create index i2_a on t2(i)");
        // Error that is in qq_PP messages
        try {
            s.executeUpdate("create table t1 oops (i int)");
        } catch (SQLException se) {
            assertSQLState("42X01", se);
            assertTrue("Expected qq_PP Message with \"Encountered\" " ,
                      (se.getMessage().indexOf("Encountered") != -1));
            
        }
        
        // Expect WARNING to be in German (default) because there is no 
        //qq_PP message. Index is a duplicate
        s.executeUpdate("create index i2_b on t2(i)");
        SQLWarning sqlw = s.getWarnings();
        assertSQLState("01504", sqlw);
        assertTrue("Expected German warning with Duplikat", 
                sqlw.getMessage().indexOf(" Duplikat") != -1);
        
        // Error from default German Locale as it does not exist in qq_PP
        // from default locale (German);
        try {
            s.executeUpdate("drop table t3");
        } catch (SQLException se) {
            assertSQLState("42Y55", se);
            assertTrue("Expected German Message with vorhanden"  ,
                      (se.getMessage().indexOf("vorhanden") != -1));
            
        }
        
        //Now all English messages
        url =  getReadWriteJDBCURL("enUSdb");
        url += ";create=true;territory=en_US";
        locConn = DriverManager.getConnection(url);
        s = locConn.createStatement();
        s.executeUpdate("create table t2 (i int)");
        s.executeUpdate("create index i2_a on t2(i)");

        try {
            s.executeUpdate("create table t1 oops (i int)");
        } catch (SQLException se) {
            assertSQLState("42X01", se);
            assertTrue("Expected English message with \"Encountered\" " ,
                      (se.getMessage().indexOf("Encountered") != -1));
            
        }
        
        // Expect WARNING to be in English because it is English db
        // Even though German default Locale still
        s.executeUpdate("create index i2_b on t2(i)");
         sqlw = s.getWarnings();
        assertSQLState("01504", sqlw);
        assertTrue("Expected English warning with duplicate", 
                sqlw.getMessage().indexOf("duplicate") != -1);
 
        try {
            s.executeUpdate("drop table t3");
        } catch (SQLException se) {
            assertSQLState("42Y55", se);
            assertTrue("Expected English Message with performed"  ,
                      (se.getMessage().indexOf("performed") != -1));
            
        }
        
    }
  
    private void checkInvalidTerritoryFormat(String territory) {
        try {
            String url = getReadWriteJDBCURL("fail3");
            url += ";create=true;territory=" + territory;
            DriverManager.getConnection(url);
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
        s.executeUpdate("create procedure checkDefaultLoc() parameter " +
                "style java language java external name " +
                "'org.apache.derbyTesting.functionTests.tests.i18n." +
                "DefaultLocale.checkDefaultLocale'");
    }
    
    public static Test suite() {
        BaseTestSuite suite = new BaseTestSuite();
        suite.addTestSuite(UrlLocaleTest.class);
        suite.addTest(new LocaleTestSetup(
                new UrlLocaleTest("messageLocale_unknown"),
                new Locale("rr", "TT")));
        suite.addTest(new LocaleTestSetup(
                new UrlLocaleTest("messageLocale_Germany"),
                Locale.GERMANY));

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

