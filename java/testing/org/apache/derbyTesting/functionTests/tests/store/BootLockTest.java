/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.store.BootLockTest

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
package org.apache.derbyTesting.functionTests.tests.store;

import java.io.File;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.lang.Integer;
import java.sql.Connection;
import java.sql.SQLException;
import java.net.SocketTimeoutException;
import java.net.ServerSocket;
import java.net.Socket;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.derbyTesting.junit.BaseTestCase;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.SecurityManagerSetup;
import org.apache.derbyTesting.junit.TestConfiguration;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.DatabasePropertyTestSetup;

import org.apache.derby.iapi.store.raw.data.DataFactory;

/**
 * Testing for FileLocks that prevent Derby Double Boot.
 * <p/>
 * For Sun phoneME, test that the property {@code
 * derby.database.forceDatabaseLock} protects us.
 * <p/>
 * FIXME: If DERBY-4646 is fixed, the special handling for phoneME
 * should be removed.
 */

public class BootLockTest extends BaseJDBCTestCase {

    private final static String dbName = "BootLockTestDB";

    private static String[] cmd = new String[]{
        "org.apache.derbyTesting.functionTests.tests.store.BootLockMinion",
        DEFAULT_DB_DIR + File.separator + dbName,
        ""
    };

    private final static String DATA_MULTIPLE_JBMS_ON_DB = "XSDB6";
    private final static String DATA_MULTIPLE_JBMS_FORCE_LOCK = "XSDB8";

    /**
     * Constructor
     *
     * @param name
     */
    public BootLockTest(String name) {
        super(name);
    }

    /**
     * Creates a suite with two testcases, with and without some extra
     * system properties.
     *
     * @return The test suite with both English and German locales.
     */
    public static Test suite() {
        TestSuite suite = new TestSuite("BootLockTest");

        if (BaseTestCase.isJ9Platform()) {
            // forking currently not working, cf. DERBY-4179.
            return suite;
        }

        suite.addTest(decorateTest());
        return suite;
    }


    /**
     * Decorate test with singleUseDatabaseDecorator and noSecurityManager.
     *
     * @return the decorated test
     */
    private static Test decorateTest() {

        Test test = new TestSuite(BootLockTest.class);

        test = TestConfiguration.singleUseDatabaseDecorator(test, dbName);

        test = SecurityManagerSetup.noSecurityManager(test);

        return test;
    }

    String oldPropValue = null;

    protected void setUp() throws Exception {

        super.setUp();

        // For phoneMe, we won't see DATA_MULTIPLE_JBMS_ON_DB, but instead we
        // can check that we get DATA_MULTIPLE_JBMS_FORCE_LOCK if we set
        // derby.database.forceDatabaseLock.

        // FIXME: I tried to use the DatabasePropertyTestSetup.singleProperty,
        // but for some reason it failed to do the job, no matter where in the
        // decorator stack in decorateTest I placed it. Should be investigated
        // so this manual set/reset code of the property can be removed.

        if (JDBC.vmSupportsJSR169()) {
            oldPropValue =
                System.getProperty("derby.database.forceDatabaseLock");
            System.setProperty("derby.database.forceDatabaseLock", "true");
        }
    }


    protected void tearDown() throws Exception {

        if (JDBC.vmSupportsJSR169()) {

            if (oldPropValue != null) {
                System.setProperty("derby.database.forceDatabaseLock",
                                   oldPropValue);
            } else {
                // FIXME: Java 1.5 only: improve code when we move to that
                // source level.
                // System.clearProperty("derby.database.forceDatabaseLock");
                System.setProperty("derby.database.forceDatabaseLock",
                                   "false");
            }


        }

        super.tearDown();
    }


    public void testBootLock() throws Exception {

        ServerSocket parentService = null;
        Socket clientSocket = null;
        BufferedReader minionSysErr = null;
        Process p = null;

        try {
            int port = TestConfiguration.getCurrent().getPort();
            cmd[2] = (new Integer(port)).toString();

            p = execJavaCmd(cmd);

            // Attempt to catch any errors happening in minion for better test
            // diagnosis.
            minionSysErr = new BufferedReader(
                new InputStreamReader(p.getErrorStream()));

            // Create a socket so we know when subprogram has booted.  Since we
            // run this test only in embedded mode, (re)use derby server port.
            parentService = new ServerSocket(port);
            parentService.setSoTimeout(60000); // max we wait is 60s

            try {

                clientSocket = parentService.accept();

            } catch (SocketTimeoutException e) {
                p.destroy();
                p.waitFor();

                StringBuffer failmsg = new StringBuffer();
                failmsg.append(
                    "Minion did not start or boot db in 60 seconds.\n" +
                    "----Minion's stderr:\n");

                String minionErrLine= null ;
                do {
                    try {
                        minionErrLine = minionSysErr.readLine();
                    } catch (Exception ioe) {
                        // may not always work, so just bail out.
                        failmsg.append("could not read minion's stderr");
                    }

                    if (minionErrLine != null) {
                        failmsg.append(minionErrLine);
                    }
                } while (minionErrLine != null);

                failmsg.append("\n----Minion's stderr ended");

                fail(failmsg.toString());
            }

            // We now know minion has booted


            try {
                Connection c = getConnection();
                fail("Dual boot not detected: check BootLockMinion.log");
            } catch (SQLException e) {
                if (JDBC.vmSupportsJSR169()) {
                    assertSQLState(
                        "Dual boot not detected: check BootLockMinion.log",
                        DATA_MULTIPLE_JBMS_FORCE_LOCK,
                        e);
                } else {
                    assertSQLState(
                        "Dual boot not detected: check BootLockMinion.log",
                        DATA_MULTIPLE_JBMS_ON_DB,
                        e);
                }
            }

            p.destroy();
            p.waitFor();

            // Since all went OK, no need to keep the minion log file.
            File minionLog = new File("BootLockMinion.log");
            assertTrue(minionLog.delete());

        } finally {
            // Make sure we free up any socket resources
            if (clientSocket != null) {
                clientSocket.close();
            }

            if (parentService != null) {
                parentService.close();
            }

            if (minionSysErr != null) {
                minionSysErr.close();
            }

            // Get rid of minion in case test fails, otherwise redundant.
            if (p != null) {
                p.destroy();
                p.waitFor();
            }


            if (JDBC.vmSupportsJSR169()) {
                // Delete lock files so JUnit machinery can clean up the
                // one-off database without further warnings on System.err
                // (phoneMe).
                File db_lockfile_name = new File(
                    DEFAULT_DB_DIR + File.separator +
                    dbName + File.separator +
                    DataFactory.DB_LOCKFILE_NAME);

                File db_ex_lockfile_name = new File(
                    DEFAULT_DB_DIR + File.separator +
                    dbName + File.separator +
                    DataFactory.DB_EX_LOCKFILE_NAME);

                db_lockfile_name.delete();
                db_ex_lockfile_name.delete();
            }
        }
    }

}
