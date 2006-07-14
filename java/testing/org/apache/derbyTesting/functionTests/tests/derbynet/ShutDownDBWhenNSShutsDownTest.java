/*
 *
 * Derby - Class ShutDownDBWhenNSShutsDownTest
 *
 * Copyright 2006 The Apache Software Foundation or its
 * licensors, as applicable.
 *
 * Licensed under the Apache License, Version 2.0 (the "License")
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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
import org.apache.derbyTesting.functionTests.util.BaseTestCase;
import org.apache.derbyTesting.functionTests.util.TestUtil;
import org.apache.derbyTesting.functionTests.util.BaseJDBCTestCase;
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
public class ShutDownDBWhenNSShutsDownTest extends BaseTestCase {


    NetworkServerControl server = null;


    /**
     * Creates a new instance of ShutDownDBWhenNSShutsDownTest
     */
    public ShutDownDBWhenNSShutsDownTest(String name) {
        super(name);
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
        assertEquals("Database is shut down",
                dbShutDown,
                !fileExists(fileName));
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
        Connection conn = BaseJDBCTestCase.getConnection();
        conn.setAutoCommit(false);
        Statement st = conn.createStatement();
        st.execute("CREATE TABLE T1 (a int)");
        st.execute("INSERT INTO T1 VALUES (1), (2), (3), (4), (5)");
        st.execute("DROP TABLE T1");
        conn.commit();
    }

    private void shutdownServer() throws Exception {
        server.shutdown();
    }

}
