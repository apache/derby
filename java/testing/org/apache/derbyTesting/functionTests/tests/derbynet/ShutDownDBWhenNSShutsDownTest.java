/*
 *
 * Derby - Class ShutDownDBWhenNSShutsDownTest
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

package org.apache.derbyTesting.functionTests.tests.derbynet;

import java.util.Properties;
import org.apache.derbyTesting.functionTests.util.TestUtil;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.BaseTestCase;
import org.apache.derby.drda.NetworkServerControl;

import junit.framework.*;
import java.sql.*;
import java.io.PrintWriter;
import java.io.File;
import java.security.AccessController;

/**
 * Derby-1274 - Network Server should shutdown the databases it has booted when
 * started from the command line.
 *
 * Tests that the network server will shutdown the databases it has booted when
 * started from the command line and that it will not shut down the databases
 * when started from the API.
 */
public class ShutDownDBWhenNSShutsDownTest extends BaseJDBCTestCase {


    NetworkServerControl server = null;


    /**
     * Creates a new instance of ShutDownDBWhenNSShutsDownTest
     */
    public ShutDownDBWhenNSShutsDownTest(String name) {
        super(name);
    }

    /**
     * Test that a shutdown of the engine does not take down the network
     * server. Before DERBY-1326 was fixed, shutting down the engine would
     * leave the network server in an inconsistent state which could make
     * clients hang infinitely.
     */
    public void testEngineShutdownDoesNotTakeDownNS() throws Exception {
        Connection[] conns = new Connection[20];

        // first make sure there are 20 active worker threads on the server
        for (int i = 0; i < conns.length; i++) {
            conns[i] = openDefaultConnection();
        }

        // then close them, leaving 20 free worker threads ready to pick up new
        // sessions
        for (int i = 0; i < conns.length; i++) {
            conns[i].close();
        }

        // Give the free threads a little time to close their sessions. This is
        // done to ensure that there are free threads waiting for new sessions,
        // which makes the DERBY-1326 hang more reliably reproducible.
        Thread.sleep(500);

        // shut down the engine
        try {
            getConnection("", "shutdown=true");
            fail("shutdown didn't raise exception");
        } catch (SQLException sqle) {
            assertSQLState("XJ015", sqle);
        }

        // see if it is still possible to connect to the server (before
        // DERBY-1326, this would hang)
        for (int i = 0; i < 20; i++) {
            openDefaultConnection().close();
        }
    }

    /**
     * Test that the NetworkServer shuts down the databases it has booted when
     * started from the command line, and that it does not shut down the
     * databases it has booted when started from the API.
     */
    public void testDatabasesShutDownWhenNSShutdown()
            throws Exception
    {
        server = new NetworkServerControl();
        // The server was started from the command line when the test was
        // started. Check that the database will be shut down when the server
        // is shut down.
        shutdownServerCheckDBShutDown(true);

        // Start the server form the API and test that the databases will not
        // be shutdown when the server is shutdown
        server.start(null);

        // wait until the server accepts connections
        int i = 0;
        while (!pingServer() && i < 10 ) {
            Thread.sleep(1000);
            i++;
        }

        // Check that the databases will not be shutdown when the server is
        // shut down.
        shutdownServerCheckDBShutDown(false);
    }

    /**
     * Checks whether the server shuts down causes the databases it has booted
     * to be shut down.
     *
     * Creates a database and shuts down the server. If the server was started
     * from the command line the database should be shut down. If the server
     * was started from the api the database should not be shut down.
     *
     * If the database has been shut down the db.lck file should not exist.
     *
     * @param dbShutDown Indicates whether the database should have been shut
     * down.
     */
    private void shutdownServerCheckDBShutDown(boolean dbShutDown)
            throws Exception
    {
        // connect to database
        createDatabase();

        // shut down the server
        shutdownServer();

        // check if db.lck exists
        String fileName = getSystemProperty("derby.system.home") +
                java.io.File.separator + "wombat" +
                java.io.File.separator + "db.lck";

        boolean fileNotFound = false;
        int i = 0;
        do {
            Thread.sleep(500);
            fileNotFound = !fileExists(fileName);
            i ++;
        } while (fileNotFound != dbShutDown && i < 120);

        assertEquals("Database is shut down", dbShutDown, fileNotFound);
    }

    private boolean fileExists (final String fileName) throws Exception {
        Boolean b = (Boolean) AccessController.doPrivileged
            (new java.security.PrivilegedAction(){
                public Object run(){
                    File file = new File(fileName);
                    return new Boolean(file.exists());
                }
        });

        return b.booleanValue();
    }

    private boolean pingServer() {
		try {
			server.ping();
		}
		catch (Exception e) {
			return false;
		}
		return true;
    }

    private void createDatabase() throws SQLException {
        Connection conn = getConnection();
        conn.setAutoCommit(false);
        Statement st = conn.createStatement();
        st.execute("CREATE TABLE T1 (a int)");
        st.execute("INSERT INTO T1 VALUES (1), (2), (3), (4), (5)");
        st.execute("DROP TABLE T1");
        conn.commit();
        conn.close();
    }

    private void shutdownServer() throws Exception {
        server.shutdown();
    }

}
