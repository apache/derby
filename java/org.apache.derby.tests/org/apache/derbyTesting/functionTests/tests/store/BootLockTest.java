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

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import junit.framework.Test;
import org.apache.derby.iapi.store.raw.data.DataFactory;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.SecurityManagerSetup;
import org.apache.derbyTesting.junit.SystemPropertyTestSetup;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Testing file locks that prevent Derby "double boot" a.k.a "dual boot",
 * i.e. two VMs booting a database concurrently, a disaster scenario.
 * <p/>
 * For phoneME, test that the property {@code
 * derby.database.forceDatabaseLock} protects us.
 * <p/>
 * FIXME: If DERBY-4646 is fixed, the special handling for phoneME
 * should be removed.
 */

public class BootLockTest extends BaseJDBCTestCase {

    private final static String dbName = "BootLockTestDB";
    private final static String dbDir = DEFAULT_DB_DIR + File.separator + dbName;
    public static String minionCompleteFileName = BootLockTest.dbDir + 
//IC see: https://issues.apache.org/jira/browse/DERBY-4985
        File.separator + "minionComplete";
    private final static String dbLockFile = dbDir + File.separator +
//IC see: https://issues.apache.org/jira/browse/DERBY-4667
    DataFactory.DB_LOCKFILE_NAME;
    private final static String dbExLockFile = dbDir + File.separator +
    DataFactory.DB_EX_LOCKFILE_NAME;
    private final static String servicePropertiesFileName = dbDir + File.separator + "service.properties";
    
    private static String[] cmd = new String[]{
        "org.apache.derbyTesting.functionTests.tests.store.BootLockMinion",
//IC see: https://issues.apache.org/jira/browse/DERBY-6651
        dbName,
    };

    private final static String DATA_MULTIPLE_JBMS_ON_DB = "XSDB6";
    private final static String DATA_MULTIPLE_JBMS_FORCE_LOCK = "XSDB8";
    // Ten minutes should hopefully be enough!
    public static final int MINION_WAIT_MAX_MILLIS = 600000;

    /**
     * Constructor
     *
     * @param name
     */
    public BootLockTest(String name) {
        super(name);
    }

    /**
     * Creates a suite.
     *
     * @return The test suite
     */
    public static Test suite() {
//IC see: https://issues.apache.org/jira/browse/DERBY-6590
        BaseTestSuite suite = new BaseTestSuite("BootLockTest");
        suite.addTest(decorateTest());
        return suite;
    }


    /**
     * Decorate test with singleUseDatabaseDecorator and noSecurityManager.
     *
     * @return the decorated test
     */
    private static Test decorateTest() {

        Test test = new BaseTestSuite(BootLockTest.class);
//IC see: https://issues.apache.org/jira/browse/DERBY-6590

        if (JDBC.vmSupportsJSR169() && !isJ9Platform()) {
            // PhoneME requires forceDatabaseLock
//IC see: https://issues.apache.org/jira/browse/DERBY-4179
            Properties props = new Properties();
            props.setProperty("derby.database.forceDatabaseLock", "true");
            test = new SystemPropertyTestSetup(test, props, true);
        }

        test = TestConfiguration.singleUseDatabaseDecorator(test, dbName);

        test = SecurityManagerSetup.noSecurityManager(test);

        return test;
    }


    public void testBootLock() throws Exception {

        Process p = null;

//IC see: https://issues.apache.org/jira/browse/DERBY-4667
        p = execJavaCmd(cmd);
        waitForMinionBoot(p,MINION_WAIT_MAX_MILLIS);
//IC see: https://issues.apache.org/jira/browse/DERBY-4987

        // We now know minion has booted

        try {
            Connection c = getConnection();
            fail("Dual boot not detected: check BootLockMinion.log");
        } catch (SQLException e) {
            if (JDBC.vmSupportsJSR169() && !isJ9Platform()) {
                // For PhoneME force database lock required
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
        finally {
            if (p!= null) {
                p.destroy();
                p.waitFor();
            }
        }
        // Since all went OK, no need to keep the minion log file.
//IC see: https://issues.apache.org/jira/browse/DERBY-6651
        File minionLog = new File(DEFAULT_DB_DIR, "BootLockMinion.log");
        assertTrue(minionLog.delete());

        if (JDBC.vmSupportsJSR169()) {
            // Delete lock files so JUnit machinery can clean up the
            // one-off database without further warnings on System.err
            // (phoneMe).
            File db_lockfile_name = new File(dbLockFile);                    

            File db_ex_lockfile_name = new File(dbExLockFile);                    

            db_lockfile_name.delete();
            db_ex_lockfile_name.delete();
        }
    }

    private void waitForMinionBoot(Process p, int waitmillis) throws InterruptedException {
        boolean minionComplete;
        int minionExitValue;
        StringBuffer failmsg = new StringBuffer();
        // boolean set to true once we find the  lock file
        File lockFile = new File(dbLockFile);
        File servicePropertiesFile = new File(servicePropertiesFileName);
        // Attempt to catch any errors happening in minion for better test
        // diagnosis.
        BufferedReader minionSysErr = new BufferedReader(
            new InputStreamReader(p.getErrorStream()));
        String minionErrLine= null ;
//IC see: https://issues.apache.org/jira/browse/DERBY-4985
        File checkFile = new File(minionCompleteFileName);
        do {
            if (checkFile.exists()) { 
                //The checkFile was created by BootLockMinion when we were
                //sure it was finished with creating the database and making 
                //the connection. It will get cleaned up with the database.
                return;
            }
            // otherwise sleep for a second and try again
            waitmillis -= 1000;
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                fail(e.getMessage());
            }
        } while (waitmillis > 0);
        
        // If we got here, the database did not boot. Try to print the error.
        failmsg.append(
//IC see: https://issues.apache.org/jira/browse/DERBY-4987
                "Minion did not start or boot db in " +
                (MINION_WAIT_MAX_MILLIS/1000) +
                " seconds.\n");                
        try {
            minionExitValue = p.exitValue();
            minionComplete =true;
            failmsg.append("exitValue = " + minionExitValue);
        }catch (IllegalThreadStateException e )
        {
            // got exception on exitValue.
            // still running ..
            minionComplete=false;
        }
        // If the process exited try to print why.
        if (minionComplete) {
            failmsg.append("----Process exited. Minion's stderr:\n");
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
        }
        
        p.destroy();
        p.waitFor();
        fail(failmsg.toString());
    }

}
