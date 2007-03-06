/*
 *
 * Derby - Class org.apache.derbyTesting.functionTests.tests.lang.BlobStoredProcedureTest
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

import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import junit.framework.Test;
import junit.framework.TestSuite;
import junit.framework.Assert;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Tests the stored procedures introduced as part of DERBY-208. These stored procedures will
 * used by the Blob methods on the client side.
 */
public class BlobStoredProcedureTest extends BaseJDBCTestCase {

    //The test string that will be used in all the test runs.
    final String testStr = "I am a simple derby test case";

    //Byte array obatined from the string
    final byte [] strBytes = testStr.getBytes();

    //The length of the test string that will be used.
    final long testStrLength = testStr.length();

    /**
     * Public constructor required for running test as standalone JUnit.
     * @param name a string containing the name of the test.
     */
    public BlobStoredProcedureTest(String name) {
        super(name);
    }

    /**
     * Create a suite of tests.
     * @return the test suite created.
     */
    public static Test suite() {
        return TestConfiguration.defaultSuite(BlobStoredProcedureTest.class);
    }

    /**
     * Setup the test.
     * @throws a SQLException.
     */
    protected void setUp() throws SQLException {
        //initialize the locator to a default value.
        int locator = -1;
        //set auto commit to false for the connection
        getConnection().setAutoCommit(false);
        //call the stored procedure to return the created locator.
        CallableStatement cs  = prepareCall
            ("? = CALL SYSIBM.BLOBCREATELOCATOR()");
        cs.registerOutParameter(1, java.sql.Types.INTEGER);
        cs.executeUpdate();
        locator = cs.getInt(1);
        cs.close();
        //use this new locator to test the SETBYTES function
        //by inserting the new bytes and testing whether it has
        //been inserted properly.

        //Insert the new substring.
        cs  = prepareCall("CALL SYSIBM.BLOBSETBYTES(?,?,?,?)");
        cs.setInt(1, locator);
        cs.setLong(2, 1L);
        cs.setInt(3, (int)testStrLength);
        cs.setBytes(4, strBytes);
        cs.execute();
        cs.close();
    }

    /**
     * Cleanup the test.
     * @throws SQLException.
     */
    protected void tearDown() throws Exception {
        commit();
        super.tearDown();
    }

    /**
     * test the BLOBGETBYTES stored procedure which will
     * be used in the implementation of Blob.getBytes.
     *
     * @throws a SQLException.
     */
    public void testBlobGetBytesSP() throws SQLException {
        // This string represents the substring that is got from the
        // stored procedure
        String testSubStr = testStr.substring(0, 10);
        byte [] testSubBytes = testSubStr.getBytes();

        //create a callable statement and execute it to call the stored
        //procedure BLOBGETBYTES that will get the bytes
        //inserted into the Blob in the setup method.
        CallableStatement cs  = prepareCall
            ("? = CALL SYSIBM.BLOBGETBYTES(?,?,?)");
        cs.registerOutParameter(1, java.sql.Types.VARBINARY);
        cs.setInt(2, 1);
        cs.setLong(3, 1);
        //set the length of the bytes returned as 10.
        cs.setInt(4, 10);
        cs.executeUpdate();
        byte [] retVal = cs.getBytes(1);

        for (int i=0;i<10;i++){
            assertEquals
                ("The Stored procedure SYSIBM.BLOBGETBYTES " +
                "returns the wrong bytes"
                , testSubBytes[i], retVal[i]);
        }
        cs.close();
    }

    /**
     * Tests the locator value returned by the stored procedure
     * BLOBCREATELOCATOR.
     *
     * @throws SQLException.
     *
     */
    public void testBlobCreateLocatorSP() throws SQLException {
        //initialize the locator to a default value.
        int locator = -1;
        //call the stored procedure to return the created locator.
        CallableStatement cs  = prepareCall
            ("? = CALL SYSIBM.BLOBCREATELOCATOR()");
        cs.registerOutParameter(1, java.sql.Types.INTEGER);
        cs.executeUpdate();
        locator = cs.getInt(1);
        //verify if the locator rturned and expected are equal.
        //remember in setup a locator is already created
        //hence expected value is 2
        assertEquals("The locator values returned by " +
            "SYSIBM.BLOBCREATELOCATOR() are incorrect", 2, locator);
        cs.close();
    }

    /**
     * Tests the SYSIBM.BLOBRELEASELOCATOR stored procedure.
     *
     * @throws SQLException
     */
    public void testBlobReleaseLocatorSP() throws SQLException {
        CallableStatement cs  = prepareCall
            ("CALL SYSIBM.BLOBRELEASELOCATOR(?)");
        cs.setInt(1, 1);
        cs.execute();
        cs.close();

        //once the locator has been released the BLOBGETLENGTH on that
        //locator value will throw an SQLException. This assures that
        //the locator has been properly released.

        cs  = prepareCall("? = CALL SYSIBM.BLOBGETLENGTH(?)");
        cs.registerOutParameter(1, java.sql.Types.BIGINT);
        cs.setInt(2, 1);
        try {
            cs.executeUpdate();
        } catch(SQLException sqle) {
            //on expected lines. The test was successful.
            return;
        }
        //The exception was not thrown. The test has failed here.
        fail("Error the locator was not released by SYSIBM.BLOBRELEASELOCATOR");
        cs.close();
    }

    /**
     * Tests the SYSIBM.BLOBGETLENGTH stored procedure.
     *
     * @throws SQLException.
     */
    public void testBlobGetLengthSP() throws SQLException {
        CallableStatement cs  = prepareCall
            ("? = CALL SYSIBM.BLOBGETLENGTH(?)");
        cs.registerOutParameter(1, java.sql.Types.BIGINT);
        cs.setInt(2, 1);
        cs.executeUpdate();
        //compare the actual length of the test string and the returned length.
        assertEquals("Error SYSIBM.BLOBGETLENGTH returns " +
            "the wrong value for the length of the Blob", testStrLength, cs.getLong(1));
        cs.close();
    }

    /**
     * Tests the SYSIBM.BLOBGETPOSITIONFROMBYTES stored procedure.
     *
     * @throws SQLException.
     */
    public void testBlobGetPositionFromBytesSP() throws Exception {
        CallableStatement cs  = prepareCall
            ("? = CALL SYSIBM.BLOBGETPOSITIONFROMBYTES(?,?,?)");
        cs.registerOutParameter(1, java.sql.Types.BIGINT);
        cs.setInt(2, 1);
        //find the position of the bytes corresponding to
        //the String simple in the test string.
        cs.setBytes(3, (new String("simple")).getBytes());
        cs.setLong(4, 1L);
        cs.executeUpdate();
        //check to see that the returned position and the expected position
        //of the substring simple in the string are matching.
        assertEquals("Error SYSIBM.BLOBGETPOSITIONFROMBYTES returns " +
            "the wrong value for the position of the Blob", 8, cs.getLong(1));
        cs.close();
    }

    /**
     * Tests the stored procedure SYSIBM.BLOBSETBYTES
     *
     * @throws SQLException.
     */
    public void testBlobSetBytes() throws SQLException {
        String newString = "123456789012345";
        byte [] newBytes = newString.getBytes();
        //initialize the locator to a default value.
        int locator = -1;
        //call the stored procedure to return the created locator.
        CallableStatement cs  = prepareCall
            ("? = CALL SYSIBM.BLOBCREATELOCATOR()");
        cs.registerOutParameter(1, java.sql.Types.INTEGER);
        cs.executeUpdate();
        locator = cs.getInt(1);
        cs.close();

        //use this new locator to test the SETBYTES function
        //by inserting the new bytes and testing whether it has
        //been inserted properly.

        //Insert the new substring.
        cs  = prepareCall("CALL SYSIBM.BLOBSETBYTES(?,?,?,?)");
        cs.setInt(1, locator);
        cs.setLong(2, 1L);
        cs.setInt(3, newString.length());
        cs.setBytes(4, newBytes);
        cs.execute();
        cs.close();

        //check the new locator to see if the value has been inserted correctly.
        cs  = prepareCall("? = CALL " +
            "SYSIBM.BLOBGETBYTES(?,?,?)");
        cs.registerOutParameter(1, java.sql.Types.VARBINARY);
        cs.setInt(2, locator);
        cs.setLong(3, 1);
        cs.setInt(4, newString.length());
        cs.executeUpdate();
        byte [] retVal = cs.getBytes(1);
        //compare the new bytes and the bytes returned by the stored
        //procedure to see of they are the same.
        for (int i=0;i<newString.length();i++){
            assertEquals
                ("The Stored procedure SYSIBM.BLOBGETBYTES " +
                "returns the wrong bytes"
                , newBytes[i], retVal[i]);
        }
        cs.close();
    }

    /**
     * Test the stored procedure SYSIBM.BLOBGETLENGTH
     *
     * @throws SQLException
     */
    public void testBlobTruncateSP() throws SQLException {
        CallableStatement cs = prepareCall
            ("CALL SYSIBM.BLOBTRUNCATE(?,?)");
        cs.setInt(1, 1);
        cs.setLong(2, 10L);
        cs.execute();
        cs.close();

        cs  = prepareCall
            ("? = CALL SYSIBM.BLOBGETLENGTH(?)");
        cs.registerOutParameter(1, java.sql.Types.BIGINT);
        cs.setInt(2, 1);
        cs.executeUpdate();
        //compare the actual length of the test string and the returned length.
        assertEquals("Error SYSIBM.BLOBGETLENGTH returns " +
            "the wrong value for the length of the Blob", 10L
            , cs.getLong(1));
        cs.close();
     }

    /**
     * Tests the SYSIBM.BLOBGETPOSITIONFROMLOCATOR stored procedure.
     *
     * @throws SQLException.
     */
    public void testBlobGetPositionFromLocatorSP() throws SQLException {
        String newString = "simple";
        byte [] newBytes = newString.getBytes();
        //initialize the locator to a default value.
        int locator = -1;
        //call the stored procedure to return the created locator.
        CallableStatement cs  = prepareCall
            ("? = CALL SYSIBM.BLOBCREATELOCATOR()");
        cs.registerOutParameter(1, java.sql.Types.INTEGER);
        cs.executeUpdate();
        locator = cs.getInt(1);
        cs.close();

        //use this new locator to test the SETBYTES function
        //by inserting the new bytes and testing whether it has
        //been inserted properly.

        //Insert the new substring.
        cs  = prepareCall("CALL SYSIBM.BLOBSETBYTES(?,?,?,?)");
        cs.setInt(1, locator);
        cs.setLong(2, 1L);
        cs.setInt(3, newString.length());
        cs.setBytes(4, newBytes);
        cs.execute();
        cs.close();

        cs  = prepareCall
            ("? = CALL SYSIBM.BLOBGETPOSITIONFROMLOCATOR(?,?,?)");
        cs.registerOutParameter(1, java.sql.Types.BIGINT);
        cs.setInt(2, 1);
        //find the position of the bytes corresponding to
        //the String simple in the test string.
        cs.setInt(3, locator);
        cs.setLong(4, 1L);
        cs.executeUpdate();
        //check to see that the returned position and the expected position
        //of the substring simple in the string are matching.
        assertEquals("Error SYSIBM.BLOBGETPOSITIONFROMLOCATOR returns " +
            "the wrong value for the position of the Blob", 8, cs.getLong(1));
        cs.close();
    }
}
