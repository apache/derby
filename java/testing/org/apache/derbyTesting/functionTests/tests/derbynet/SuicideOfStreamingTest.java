/*

Derby - Class org.apache.derbyTesting.functionTests.tests.derbynet.SuicideOfStreamingTest

Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to you under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

*/

package org.apache.derbyTesting.functionTests.tests.derbynet;

import java.io.IOException;
import java.io.InputStream;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import junit.framework.Test;
import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derbyTesting.functionTests.util.streams.LoopingAlphabetStream;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Test that an exception is raised if the flow of data from the server to the
 * client is aborted.
 *
 * This test is somewhat special, and care should be taken if new tests are
 * added here. The requirements for this test are:<ol>
 *      <li>Must be run with the network client driver (DerbyNetClient)</li>
 *      <li>Derby must have been built in SANE mode</li>
 *      <li>System property <i>derby.debug.suicideOfLayerBStreaming</i> must be
 *          set to <i>true</i> in the server-side VM</li></ol>
 * 
 * Note that action must be taken if this test is to be run with a separate VM
 * for the network server (includes running the server on a remote host).
 */
public class SuicideOfStreamingTest
    extends BaseJDBCTestCase {

    /**
     * Create instance of the named test.
     */
    public SuicideOfStreamingTest(String name) {
        super(name);
    }

    /**
     * Create table, insert row and set debug property.
     */
    public void setUp()
            throws Exception {
        // Create the table.
        Statement createTableSt = createStatement();
        createTableSt.execute(
                "create table TEST_TABLE( TEST_COL blob( 65536 ))");
        createTableSt.close();
        // Insert a row.
        PreparedStatement insertLobSt = prepareStatement(
                "insert into TEST_TABLE (TEST_COL) values (?)");
        int lobLength = 65536;
        insertLobSt.setBinaryStream(1,
                new LoopingAlphabetStream(lobLength), lobLength);
        insertLobSt.executeUpdate();
        insertLobSt.close();
        setSystemProperty("derby.debug.suicideOfLayerBStreaming", "true");
    }

    /**
     * Unset the debug property.
     */
    public void tearDown()
            throws Exception {
        removeSystemProperty("derby.debug.suicideOfLayerBStreaming");
        super.tearDown();
    }

    /**
     * Test that the client throws an exception when an exception is thrown on
     * the server side when streaming from the database.
     */
    public void testInterruptedReadOfLob()
            throws IOException, SQLException {
        PreparedStatement fetchLobSt = prepareStatement(
                "select TEST_COL from TEST_TABLE");
        ResultSet rs = fetchLobSt.executeQuery();
        try {
            rs.next();
            InputStream is = rs.getBinaryStream(1);
            // Read the stream.
            int c;
            while ( (c = is.read() ) > -1) {}
            fail("Reading stream should have raised exception.");
        } catch (SQLException sqle) {
            assertSQLState("58009", sqle);
        }
        rs.close();
        fetchLobSt.close();
    }

    /**
     * Return a test suite.
     *
     * @return an empty suite if Derby is built with in INSANE mode,
     *      a suite with one or more tests otherwise.
     */
    public static Test suite() {
        if (SanityManager.DEBUG) {
            // [NOTE] Observe that the CleanDatabaseTestSetup is wrapping the
            //      client/server decorator. This is intentional, because the
            //      network server tend to enter an invalid state when setting
            //      the debug property used by this test. To avoid the error,
            //      we use an embedded connection to clean the database, while
            //      the test itself uses a network connection.
            //      This means this test will not run with a remote server.

            // [NOTE] To observe the protocol error that should not be seen,
            //      move the CleanDatabaseTestSetup inside the client/server
            //      decorator. The error is intermittent, so more than one run
            //      may be required.
            return new CleanDatabaseTestSetup(
                    TestConfiguration.clientServerDecorator(
                        new BaseTestSuite(SuicideOfStreamingTest.class,
                                      "SuicideOfStreamingTest")));
        }
        return new BaseTestSuite(
            "SuicideOfStreamingTest <DISABLED IN INSANE MODE>");
    }

} // End class SuicideOfStreamingTest
