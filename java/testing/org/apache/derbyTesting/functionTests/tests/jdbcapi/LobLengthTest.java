/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.jdbcapi.LobLengthTest

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package org.apache.derbyTesting.functionTests.tests.jdbcapi;

import java.sql.PreparedStatement;
import java.sql.Statement;
import junit.framework.Test;
import org.apache.derbyTesting.functionTests.util.streams.LoopingAlphabetStream;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * This tests a fix for a defect (DERBY-121) where the server and
 * client were processing lob lengths incorrectly.  For lob lengths
 * that are represented by 24 or more bits, the server and Derby
 * client were doing incorrect bit-shifting.  This test makes sure
 * that problem no longer occurs.
 */

public class LobLengthTest extends BaseJDBCTestCase {

    /**
     * Creates a new instance of LobLengthTest
     *
     * @param name name of the test.
     */
    public LobLengthTest(String name)
    {
        super(name);
    }

    
    public static Test suite() 
    {
        return TestConfiguration.defaultSuite(LobLengthTest.class);
    }


    /**
     * Create a JDBC connection using the arguments passed
     * in from the harness, and create the table to be used by the test.
     */
    public void setUp() throws Exception
    {
        getConnection().setAutoCommit(false);

        // Create a test table.
        Statement st = createStatement();
        st.execute("create table lobTable100M(bl blob(100M))");
        st.close();
    }


    /**
     * Cleanup: Drop table and close connection.
     */
    public void tearDown() throws Exception 
    {
        Statement st = createStatement();
        st.execute("drop table lobTable100M");
        st.close();

        commit();
        super.tearDown();
    }


    /**
     * There was a defect (DERBY-121) where the server and client
     * were processing lob lengths incorrectly.  For lob lengths
     * that are represented by 24 or more bits, the server and
     * Derby client were doing incorrect bit-shifting.  This
     * test makes sure that problem no longer occurs.
     */
    public void testLongLobLengths() throws Exception
    {
        PreparedStatement pSt = prepareStatement(
            "insert into lobTable100M(bl) values (?)");

        // The error we're testing occurs when the server
        // is shifting bits 24 and higher of the lob's
        // length (in bytes).  This means that, in order
        // to check for the error, we have to specify a
        // lob length (in bytes) that requires at least
        // 24 bits to represent.  Thus for a blob the
        // length of the test data must be specified as
        // at least 2^24 bytes (hence the '16800000' in
        // the next line).
        int lobSize = 16800000;
        pSt.setBinaryStream(1,
            new LoopingAlphabetStream(lobSize), lobSize);

        // Now try the insert; this is where the server processes
        // the lob length.
        pSt.execute();
        pSt.close();
    }
}
