/*

Derby - Class org.apache.derbyTesting.functionTests.tests.replicationTests.Derby5937SlaveShutdownTest

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

package org.apache.derbyTesting.functionTests.tests.replicationTests;

import java.io.File;
import java.sql.Connection;
import java.sql.SQLException;
import javax.sql.DataSource;
import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.derbyTesting.functionTests.tests.store.BootLockTest;
import org.apache.derbyTesting.functionTests.util.PrivilegedFileOpsForTests;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.BaseTestCase;
import org.apache.derbyTesting.junit.JDBCDataSource;
import org.apache.derbyTesting.junit.NetworkServerTestSetup;
import org.apache.derbyTesting.junit.SecurityManagerSetup;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * <p>
 * Regression test case for DERBY-5937. After fail-over, the slave database
 * will leak a file handle for the active log file.
 * </p>
 *
 * <p>
 * The test case will set up replication between a master database and a
 * slave database, perform fail-over to the slave, and then shut down the
 * slave database. Finally, it attempts to delete the slave database. On
 * Windows, this fails if DERBY-5937 is not fixed, because one of the log
 * files is still held open, and Windows doesn't allow deletion of open
 * files.
 * </p>
 */
public class Derby5937SlaveShutdownTest extends BaseJDBCTestCase {

    private static final String MASTER_DB = "d5937-master-db";
    private static final String SLAVE_DB = "d5937-slave-db";

    private static final String FAILOVER_SUCCESS = "XRE20";
    private static final String DB_SHUTDOWN_SUCCESS = "08006";

    private static final long RETRY_INTERVAL = 50L;

    public Derby5937SlaveShutdownTest(String name) {
        super(name);
    }

    public static Test suite() {
        //DERBY-5975 test fails intermittently with weme causing a hang.
        // Likely a jvm issue, so don't run on that OS...
        if (BaseTestCase.isJ9Platform())
        {
            Test test = new TestSuite("Derby5937SlaveShutdownTest");
            return test;
        }
        Class klass = Derby5937SlaveShutdownTest.class;
        // The default security policy doesn't allow derby.jar to do
        // networking, which is needed for replication, so install a custom
        // policy for this test.
        return new SecurityManagerSetup(
            TestConfiguration.singleUseDatabaseDecorator(
                TestConfiguration.embeddedSuite(klass), MASTER_DB),
            klass.getName().replace('.', '/') + ".policy", true);
    }

    public void testSlaveFailoverLeak() throws Exception {
        // First establish a connection so that the database is created.
        getConnection().close();

        // Then shut down the database cleanly so that it can be used
        // to seed the replication slave.
        final TestConfiguration config = TestConfiguration.getCurrent();
        config.shutdownDatabase();

        // Copy the database to the slave.
        final String masterDb = config.getDatabasePath(MASTER_DB);
        final String slaveDb = config.getDatabasePath(SLAVE_DB);
        PrivilegedFileOpsForTests.copy(new File(masterDb), new File(slaveDb));

        // And start the slave.
        DataSource startSlaveDS = JDBCDataSource.getDataSource(SLAVE_DB);
        JDBCDataSource.setBeanProperty(startSlaveDS, "connectionAttributes",
                "startSlave=true;slaveHost=" + config.getHostName() +
                ";slavePort=" + config.getPort());
        SlaveThread slave = new SlaveThread(startSlaveDS);
        slave.start();

        // Start the master. This will fail until the slave is up, so do
        // it in a loop until successful or time runs out.
        DataSource startMasterDS = JDBCDataSource.getDataSource();
        JDBCDataSource.setBeanProperty(startMasterDS, "connectionAttributes",
                "startMaster=true;slaveHost=" + config.getHostName() +
                ";slavePort=" + config.getPort());
        long giveUp =
            System.currentTimeMillis() + NetworkServerTestSetup.getWaitTime();
        Connection c = null;
        while (c == null) {
            try {
                c = startMasterDS.getConnection();
            } catch (SQLException sqle) {
                slave.checkError(); // Exit early if the slave has failed
                if (System.currentTimeMillis() > giveUp) {
                    fail("Master won't start", sqle);
                } else {
                    println("Retrying after startMaster failed with: " + sqle);
                    Thread.sleep(RETRY_INTERVAL);
                }
            }
        }
        c.close();

        // Wait for the slave thread to complete, which it will do once
        // it's connected to the master.
        slave.join();
        slave.checkError();

        // Perform fail-over.
        DataSource failoverDS = JDBCDataSource.getDataSource();
        JDBCDataSource.setBeanProperty(
                failoverDS, "connectionAttributes", "failover=true");
        try {
            failoverDS.getConnection();
            fail("failover should receive exception");
        } catch (SQLException sqle) {
            assertSQLState(FAILOVER_SUCCESS, sqle);
        }

        // Shut down the slave database. This will fail until failover is
        // complete, so do it in a loop until successful or time runs out.
        giveUp =
            System.currentTimeMillis() + NetworkServerTestSetup.getWaitTime();
        DataSource slaveShutdownDS = JDBCDataSource.getDataSource(SLAVE_DB);
        JDBCDataSource.setBeanProperty(
                slaveShutdownDS, "shutdownDatabase", "shutdown");
        while (true) {
            try {
                slaveShutdownDS.getConnection();
                fail("Shutdown of slave database didn't throw an exception");
            } catch (SQLException sqle) {
                if (DB_SHUTDOWN_SUCCESS.equals(sqle.getSQLState())) {
                    // The expected shutdown exception was thrown. Break out
                    // of the loop.
                    break;
                } else if (System.currentTimeMillis() > giveUp) {
                    fail("Could not shut down slave database", sqle);
                } else {
                    println("Retrying after failover failed with: " + sqle);
                    Thread.sleep(RETRY_INTERVAL);
                }
            }
        }

        // This call used to fail on Windows because one of the log files
        // was still open.
        assertDirectoryDeleted(new File(slaveDb));
    }

    /**
     * Helper thread which starts a replication slave and blocks until the
     * slave is connected to a master database.
     */
    private class SlaveThread extends Thread {

        private final DataSource ds;
        private volatile Throwable error;

        SlaveThread(DataSource ds) {
            this.ds = ds;
        }

        public void run() {
            try {
                run_();
            } catch (Throwable t) {
                error = t;
            }
        }

        private void run_() throws Exception {
            println("Slave thread started.");

            try {
                ds.getConnection();
                fail("startSlave should throw exception");
            } catch (SQLException sqle) {
                assertSQLState("XRE08", sqle);
            }
        }

        void checkError() {
            if (error != null) {
                fail("Slave thread failed", error);
            }
        }
    }
}
