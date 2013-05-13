/*
 *
 * Derby - Class org.apache.derbyTesting.functionTests.tests.demo.CheckToursDBTest
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file ecept in compliance with
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
package org.apache.derbyTesting.functionTests.tests.demo;

import java.sql.PreparedStatement;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import junit.framework.Test;
import junit.framework.TestSuite;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.TestConfiguration;
import org.apache.derbyTesting.junit.SupportFilesSetup;

import toursdb.insertMaps;

/**
 * This test is for testing the ToursDB database and functions
 */
public class CheckToursDBTest extends BaseJDBCTestCase {
    /**
     * Public constructor required for running test as standalone JUnit.
     * @param name
     */
    public CheckToursDBTest(String name) {
        super(name);
    }

   public static Test basesuite(String name) {
        TestSuite suite = new TestSuite(CheckToursDBTest.class, name);
        Test test = new SupportFilesSetup(suite, new String[] {
                "functionTests/tests/demo/cupisle.gif",
                "functionTests/tests/demo/smallisle.gif",
                "functionTests/tests/demo/witchisle.gif" });
        return test;
    }

    public static Test suite() {
        if ( JDBC.vmSupportsJSR169())
            // Test executes PreparedStatement.setBigDecimal, which
            // is not supported with JSR169
            return new TestSuite("empty CheckToursDBTest - *.setBigDecimal not supported with JSR169");
        
        TestSuite suite = new TestSuite("CheckToursDBTest");
        suite.addTest(basesuite("CheckToursDBTest:embedded"));
        suite.addTest(TestConfiguration
                .clientServerDecorator(basesuite("CheckToursDBTest:client")));
        return suite;

    }

   /**
    * Tear-down the fixture by removing the tables
    * @throws Exception
    */    protected void tearDown() throws Exception {
        Statement st = createStatement();
        st.execute("DROP TABLE AIRLINES");                
        st.execute("DROP TABLE CITIES");
        st.execute("DROP TABLE COUNTRIES");
        st.execute("DROP TABLE FLIGHTAVAILABILITY");
        st.execute("DROP TABLE FLIGHTS");
        st.execute("DROP TABLE MAPS");
        st.execute("DROP TABLE FLIGHTS_HISTORY");                      
        st.close();
        commit();
        super.tearDown();
    }

    /**
     * Test insert, update and delete on ToursDB tables 
     * @throws Exception
     */
    public void testToursDB() throws Exception {
        String[] dbfiles = { "ToursDB_schema.sql", "loadCOUNTRIES.sql",
                "loadCITIES.sql", "loadAIRLINES.sql", "loadFLIGHTS1.sql",
                "loadFLIGHTS2.sql", "loadFLIGHTAVAILABILITY1.sql",
                "loadFLIGHTAVAILABILITY2.sql" };

        for (int i = 0; i < dbfiles.length; i++) {
            runScript("org/apache/derbyTesting/functionTests/tests/demo/"
                    + dbfiles[i], "US-ASCII");
        }
        insertMapsPrivileged();
        doSelect();
        doUpdate();
        doDelete();

    }
/**
 * Method to delete rows from the ToursDB tables 
 * @throws Exception
 */
    private void doDelete() throws Exception {
        String tableName[] = { "AIRLINES", "CITIES", "COUNTRIES",
                "FLIGHTAVAILABILITY", "FLIGHTS", "MAPS" };
        int expectedRows[] = { 2, 87, 114, 518, 542, 3 };
        PreparedStatement ps = null;
        for (int i = 0; i < 6; i++) {
            ps = prepareStatement("delete from " + tableName[i]);
            assertEquals(ps.executeUpdate(), expectedRows[i]);
        }

        // now quickly checking FLIGHTS_HISTORY -
        // should now have a 2nd row because of trigger2
        ps = prepareStatement("select STATUS from FLIGHTS_HISTORY where FLIGHT_ID IS NULL and STATUS <> 'over'");
        // don't care if there are more than 1 rows...
        JDBC.assertSingleValueResultSet(ps.executeQuery(),
                "INSERTED FROM TRIG2");
        ps = prepareStatement("delete from FLIGHTS_HISTORY");
        assertEquals(ps.executeUpdate(), 2);

    }
/**
 * Method to update the rows in the ToursDB tables.
 * @throws SQLException
 */
    private void doUpdate() throws SQLException {
        PreparedStatement ps = null;
        ps = prepareStatement("select ECONOMY_SEATS from AIRLINES where AIRLINE = 'AA'");
        JDBC.assertSingleValueResultSet(ps.executeQuery(), "20");
        Statement stmt = createStatement();
        stmt.execute("update AIRLINES set ECONOMY_SEATS=108 where AIRLINE = 'AA'");
        JDBC.assertSingleValueResultSet(ps.executeQuery(), "108");
        ps = prepareStatement("select COUNTRY from COUNTRIES where COUNTRY_ISO_CODE = 'US'");
        JDBC.assertSingleValueResultSet(ps.executeQuery(), "United States");
        stmt.execute("update COUNTRIES set COUNTRY='United States of America' where COUNTRY_ISO_CODE = 'US'");
        JDBC.assertSingleValueResultSet(ps.executeQuery(),
                "United States of America");
        ps = prepareStatement("select COUNTRY from CITIES where CITY_ID = 52");
        JDBC.assertSingleValueResultSet(ps.executeQuery(), "United States");
        stmt.execute("update CITIES set COUNTRY='United States of America' where COUNTRY='United States'");
        JDBC.assertSingleValueResultSet(ps.executeQuery(),
                "United States of America");
        ps = prepareStatement("select ECONOMY_SEATS_TAKEN from FLIGHTAVAILABILITY where FLIGHT_ID = 'AA1134' and FLIGHT_DATE='2004-03-31'");
        JDBC.assertSingleValueResultSet(ps.executeQuery(), "2");
        stmt.execute("update FLIGHTAVAILABILITY set ECONOMY_SEATS_TAKEN=20 where FLIGHT_ID = 'AA1134' and FLIGHT_DATE='2004-03-31'");
        JDBC.assertSingleValueResultSet(ps.executeQuery(), "20");
        ps = prepareStatement("select AIRCRAFT from FLIGHTS where FLIGHT_ID = 'AA1183'");
        JDBC.assertSingleValueResultSet(ps.executeQuery(), "B747");
        stmt.execute("update FLIGHTS set AIRCRAFT='B777' where FLIGHT_ID = 'AA1134'");
        JDBC.assertSingleValueResultSet(ps.executeQuery(), "B747");
        ps = prepareStatement("select REGION from MAPS where MAP_NAME = 'North Ocean'");
        JDBC.assertSingleValueResultSet(ps.executeQuery(), "Cup Island");
        stmt.execute("update MAPS set REGION='Coffee Cup Island' where MAP_NAME = 'North Ocean'");
        JDBC.assertSingleValueResultSet(ps.executeQuery(),
                "Coffee Cup Island");
        // Flight_history is now has 1 row, because of TRIG1
        ps = prepareStatement("select STATUS from FLIGHTS_HISTORY where FLIGHT_ID = 'AA1134'");
        JDBC.assertSingleValueResultSet(ps.executeQuery(),
                "INSERTED FROM TRIG1");
        stmt.execute("update FLIGHTS_HISTORY set STATUS='over' where FLIGHT_ID='AA1134'");
        JDBC.assertSingleValueResultSet(ps.executeQuery(), "over");

    }
/**
 * Inserts rows in the Maps table. Calls insertMaps().
 * @throws Exception
 */
    public void insertMapsPrivileged() throws Exception {
        try {

            AccessController.doPrivileged(new PrivilegedExceptionAction<Object>() {
                public Object run() throws SQLException, FileNotFoundException,
                        IOException {
                    insertMaps();
                    return null;
                }
            });
        } catch (PrivilegedActionException e) {
            throw e.getException();
        }
    }
/**
 * Method to select rows from ToursDB tables
 * @throws SQLException
 */
    private void doSelect() throws SQLException {
        String expectedRows[] = { "2", "114", "87", "518", "542", "3", "0" };
        // now ensure we can select from all the tables
        PreparedStatement ps = null;
        String tableName[] = { "AIRLINES", "COUNTRIES", "CITIES",
                "FLIGHTAVAILABILITY", "FLIGHTS", "MAPS", "FLIGHTS_HISTORY" };
        for (int i = 0; i < 7; i++) {
            ps = prepareStatement("select count(*) from " + tableName[i]);
            JDBC.assertSingleValueResultSet(ps.executeQuery(), expectedRows[i]);
           }
       
    }
/**
 * Inserts 3 rows in the Maps table. 
 * @throws SQLException
 * @throws FileNotFoundException
 * @throws IOException
 */
    private void insertMaps() 
    throws SQLException, FileNotFoundException, IOException {
        Connection conn = getConnection();
        assertEquals(insertMaps.insertRows("extin", conn), 3);
    }

}
