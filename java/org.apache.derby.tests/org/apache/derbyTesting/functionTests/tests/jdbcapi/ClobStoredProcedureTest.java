/*
 *
 * Derby - Class org.apache.derbyTesting.functionTests.tests.lang.ClobStoredProcedureTest
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
package org.apache.derbyTesting.functionTests.tests.jdbcapi;


import java.sql.CallableStatement;
import java.sql.SQLException;
import junit.framework.Test;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Tests the stored procedures introduced as part of DERBY-208.
 * These stored procedures will used by the Clob methods on the client side.
 */
public class ClobStoredProcedureTest extends BaseJDBCTestCase {

    final String testStr = "I am a simple derby test case";
    final long testStrLength = testStr.length();
    /**
     * Public constructor required for running test as standalone JUnit.
     * @param name a string containing the name of the test.
     */
    public ClobStoredProcedureTest(String name) {
        super(name);
    }

    /**
     * Create a suite of tests.
     * @return the test suite created.
     */
    public static Test suite() {
        if (JDBC.vmSupportsJSR169()) {
            return new BaseTestSuite(
                "empty: client not supported on JSR169; procs use DriverMgr");
        }
        else {
            return TestConfiguration.defaultSuite(
                    ClobStoredProcedureTest.class);
        }
    }

    /**
     * Setup the test.
     * @throws a SQLException.
     */
    public void setUp() throws Exception {
        int locator = 0;
        getConnection().setAutoCommit(false);
        CallableStatement cs  = prepareCall
            ("? = CALL SYSIBM.CLOBCREATELOCATOR()");
        cs.registerOutParameter(1, java.sql.Types.INTEGER);
        cs.executeUpdate();
        locator = cs.getInt(1);
        cs.close();
        cs  = prepareCall("CALL SYSIBM.CLOBSETSTRING(?,?,?,?)");
        cs.setInt(1, locator);
        cs.setInt(2, 1);
        cs.setLong(3, testStrLength);
        cs.setString(4, testStr);
        cs.execute();
        cs.close();
    }
    /**
     * Cleanup the test.
     * @throws SQLException.
     */
    public void tearDown() throws Exception {
        commit();
        super.tearDown();
    }

    /**
     * Test the stored procedure SYSIBM.CLOBGETSUBSTRING
     *
     * @throws an SQLException.
     */
    public void testGetSubStringSP() throws SQLException {
        CallableStatement cs  = prepareCall("? = CALL " +
            "SYSIBM.CLOBGETSUBSTRING(?,?,?)");
        cs.registerOutParameter(1, java.sql.Types.VARCHAR);
        cs.setInt(2, 1);
        cs.setLong(3, 1);
        //get sub-string of length 10 from the clob.
        cs.setInt(4, 10);
        cs.executeUpdate();
        String retVal = cs.getString(1);
        //compare the string that is returned to the sub-string obtained directly
        //from the test string. If found to be equal the stored procedure
        //returns valid values.
        if (testStr.substring(0, 10).compareTo(retVal) != 0) {
            fail("Error SYSIBM.CLOBGETSUBSTRING returns the wrong string");
        }
        cs.close();
    }

    /**
     * Tests the locator value returned by the stored procedure
     * CLOBCREATELOCATOR.
     *
     * @throws SQLException.
     *
     */
    public void testClobCreateLocatorSP() throws SQLException {
        //initialize the locator to a default value.
        int locator = -1;
        //call the stored procedure to return the created locator.
        CallableStatement cs  = prepareCall
            ("? = CALL SYSIBM.CLOBCREATELOCATOR()");
        cs.registerOutParameter(1, java.sql.Types.INTEGER);
        cs.executeUpdate();
        locator = cs.getInt(1);
        //verify if the locator rturned and expected are equal.
        //remember in setup a locator is already created
        //hence expected value is 2
        assertEquals("The locator values returned by " +
            "SYSIBM.CLOBCREATELOCATOR() are incorrect", 2, locator);
        cs.close();
    }

    /**
     * Tests the SYSIBM.CLOBRELEASELOCATOR stored procedure.
     *
     * @throws SQLException
     */
    public void testClobReleaseLocatorSP() throws SQLException {
        CallableStatement cs  = prepareCall
            ("CALL SYSIBM.CLOBRELEASELOCATOR(?)");
        cs.setInt(1, 1);
        cs.execute();
        cs.close();

        //once the locator has been released the CLOBGETLENGTH on that
        //locator value will throw an SQLException. This assures that
        //the locator has been properly released.

        cs  = prepareCall
            ("? = CALL SYSIBM.CLOBGETLENGTH(?)");
        cs.registerOutParameter(1, java.sql.Types.BIGINT);
        cs.setInt(2, 1);
        try {
            cs.executeUpdate();
        } catch(SQLException sqle) {
            //on expected lines. The test was successful.
            return;
        }
        //The exception was not thrown. The test has failed here.
        fail("Error the locator was not released by SYSIBM.CLOBRELEASELOCATOR");
        cs.close();
    }

    /**
     * Tests the stored procedure SYSIBM.CLOBGETLENGTH.
     *
     * @throws SQLException
     */
    public void testClobGetLengthSP() throws SQLException {
        CallableStatement cs  = prepareCall
            ("? = CALL SYSIBM.CLOBGETLENGTH(?)");
        cs.registerOutParameter(1, java.sql.Types.BIGINT);
        cs.setInt(2, 1);
        cs.executeUpdate();
        //compare the actual length of the test string and the returned length.
        assertEquals("Error SYSIBM.CLOBGETLENGTH returns " +
            "the wrong value for the length of the Clob", testStrLength, cs.getLong(1));
        cs.close();
    }

    /**
     * Tests the stored procedure SYSIBM.CLOBGETPOSITIONFROMSTRING.
     *
     * @throws SQLException.
     */
    public void testClobGetPositionFromStringSP() throws SQLException {
        CallableStatement cs  = prepareCall
            ("? = CALL SYSIBM.CLOBGETPOSITIONFROMSTRING(?,?,?)");
        cs.registerOutParameter(1, java.sql.Types.BIGINT);
        cs.setInt(2, 1);
        cs.setString(3, new String("simple"));
        cs.setLong(4, 1L);
        cs.executeUpdate();
        //compare the substring position returned from the stored procedure and that
        //returned from the String class functions. If not found to be equal throw an
        //error.
        assertEquals("Error SYSIBM.CLOBGETPOSITIONFROMSTRING returns " +
            "the wrong value for the position of the SUBSTRING", testStr.indexOf("simple")+1, cs.getLong(1));
        cs.close();
    }

    /**
     * Tests the stored procedure SYSIBM.CLOBSETSTRING
     *
     * @throws SQLException.
     */
    public void testClobSetStringSP() throws SQLException {
        String newString = "123456789012345";
        //initialize the locator to a default value.
        int locator = -1;
        //call the stored procedure to return the created locator.
        CallableStatement cs  = prepareCall
            ("? = CALL SYSIBM.CLOBCREATELOCATOR()");
        cs.registerOutParameter(1, java.sql.Types.INTEGER);
        cs.executeUpdate();
        locator = cs.getInt(1);
        cs.close();

        //use this new locator to test the SETSUBSTRING function
        //by inserting a new sub string and testing whether it has
        //been inserted properly.

        //Insert the new substring.
        cs  = prepareCall("CALL SYSIBM.CLOBSETSTRING(?,?,?,?)");
        cs.setInt(1, locator);
        cs.setInt(2, 1);
        cs.setLong(3, newString.length());
        cs.setString(4, newString);
        cs.execute();
        cs.close();

        //check the new locator to see if the value has been inserted correctly.
        cs  = prepareCall("? = CALL " +
            "SYSIBM.CLOBGETSUBSTRING(?,?,?)");
        cs.registerOutParameter(1, java.sql.Types.VARCHAR);
        cs.setInt(2, locator);
        cs.setLong(3, 1);
        //get sub-string of length 10 from the clob.
        cs.setInt(4, newString.length());
        cs.executeUpdate();
        String retVal = cs.getString(1);
        //compare the new string and the string returned by the stored
        //procedure to see of they are the same.
        if (newString.compareTo(retVal) != 0)
            fail("SYSIBM.CLOBSETSTRING does not insert the right value");
        cs.close();
    }

    /**
     * Test the stored procedure SYSIBM.CLOBGETLENGTH
     *
     * @throws SQLException
     */
    public void testClobTruncateSP() throws SQLException {

       //----------TO BE ENABLED LATER------------------------------
       //This code needs to be enabled once the set methods on the
       //Clob interface are implemented. Until that time keep checking
       //for a not implemented exception being thrown.
       /*
        CallableStatement cs = prepareCall
            ("CALL SYSIBM.CLOBTRUNCATE(?,?)");
        cs.setInt(1, 1);
        cs.setLong(2, 10L);
        cs.execute();
        cs.close();

        cs  = prepareCall
            ("? = CALL SYSIBM.CLOBGETLENGTH(?)");
        cs.registerOutParameter(1, java.sql.Types.BIGINT);
        cs.setInt(2, 1);
        cs.executeUpdate();
        //compare the actual length of the test string and the returned length.
        assertEquals("Error SYSIBM.CLOBGETLENGTH returns " +
            "the wrong value for the length of the Clob", 10
            , cs.getLong(1));
        cs.close();
        */
        //----------TO BE ENABLED LATER------------------------------

        CallableStatement cs = prepareCall
            ("CALL SYSIBM.CLOBTRUNCATE(?,?)");
        cs.setInt(1, 1);
        cs.setLong(2, 10L);
        try {
            cs.execute();
        }
        catch(SQLException sqle) {
            //expected Unsupported SQLException
            //The CLOBTRUNCATE is not supported but contains
            //temporary code that shall be removed when
            //the method is enabled.
        }
        cs.close();
    }

    /**
     * Tests the SYSIBM.CLOBGETPOSITIONFROMLOCATOR stored procedure.
     *
     * @throws SQLException.
     */
    public void testClobGetPositionFromLocatorSP() throws SQLException {
        int locator = 0;

        String newStr = "simple";

        CallableStatement cs  = prepareCall
            ("? = CALL SYSIBM.CLOBCREATELOCATOR()");
        cs.registerOutParameter(1, java.sql.Types.INTEGER);
        cs.executeUpdate();
        locator = cs.getInt(1);
        cs.close();

        cs  = prepareCall("CALL SYSIBM.CLOBSETSTRING(?,?,?,?)");
        cs.setInt(1, locator);
        cs.setInt(2, 1);
        cs.setLong(3, newStr.length());
        cs.setString(4, newStr);
        cs.execute();

        cs.close();
        cs  = prepareCall
            ("? = CALL SYSIBM.CLOBGETPOSITIONFROMLOCATOR(?,?,?)");
        cs.registerOutParameter(1, java.sql.Types.BIGINT);
        cs.setInt(2, 1);
        //find the position of the bytes corresponding to
        //the String simple in the test string.
        cs.setInt(3, locator);
        cs.setLong(4, 1L);
        cs.executeUpdate();
        //check to see that the returned position and the expected position
        //of the substring simple in the string are matching.
        assertEquals("Error SYSIBM.CLOBGETPOSITIONFROMLOCATOR returns " +
            "the wrong value for the position of the Clob", 8, cs.getLong(1));
        cs.close();
    }
}
