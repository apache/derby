/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.store.BackupRestoreTest

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
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.CallableStatement;
import java.sql.SQLException;
import javax.sql.DataSource;
import junit.framework.Test;
import org.apache.derbyTesting.functionTests.util.PrivilegedFileOpsForTests;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.JDBCDataSource;
import org.apache.derbyTesting.junit.SupportFilesSetup;
import org.apache.derbyTesting.junit.TestConfiguration;
import org.apache.derbyTesting.junit.Utilities;

public class BackupRestoreTest
    extends BaseJDBCTestCase {

    public BackupRestoreTest(String name) {
        super(name);
    }

    /**
     * See DERBY-3875.
     * <p>
     * Steps in the test:
     *  1) Create a database and perform a backup.
     *  2) Shutdown the Derby engine.
     *  3) Corrupt one of the database files.
     *  4) Boot corrupted database.
     *  5) Restore backup.
     * <p>
     * With the bug present, the test failed in step 5.
     * Note that the test did fail only on Windows platforms, which is probably
     * because of differences in the file system code.
     */
    public void testDerby3875()
            throws SQLException, IOException {
        // Create the database.
        println("Creating database");
        getConnection();
        // Backup the database.
        println("Backing up database");
        String dbBackup = SupportFilesSetup.getReadWrite("dbbackup").getPath();
        CallableStatement cs = prepareCall(
                "CALL SYSCS_UTIL.SYSCS_BACKUP_DATABASE(?)");
        cs.setString(1, dbBackup);
        cs.execute();
        cs.close();
        // Shutdown the database.
        getTestConfiguration().shutdownEngine();

        // Corrupt one of the database files.
        File dataDir = new File("system/" +
                getTestConfiguration().getDefaultDatabaseName(), "seg0");
        File df = new File(dataDir, "c10.dat");
        assertTrue("File to corrupt doesn't exist: " + df.getPath(),
                PrivilegedFileOpsForTests.exists(df));
        println("Corrupting data file");
        byte[] zeros = new byte[(int)PrivilegedFileOpsForTests.length(df)];
        FileOutputStream fout =
                PrivilegedFileOpsForTests.getFileOutputStream(df);
        fout.write(zeros);
        fout.flush();
        fout.close();

        // Reboot the database, which should fail.
        try {
            println("Rebooting corrupted database");
            getConnection();
            fail("Reboot of currupted database should have failed");
        } catch (SQLException sqle) {
            assertSQLState("XJ040", sqle);
        }

        // Now try to restore database.
        println("Restoring database");
        String tmp[] = Utilities.split(
                getTestConfiguration().getDefaultDatabaseName(), '/');
        final String dbName = tmp[tmp.length -1];
        DataSource ds = JDBCDataSource.getDataSource();
        JDBCDataSource.setBeanProperty(ds, "connectionAttributes", 
                ("restoreFrom=" + dbBackup + "/" + dbName ));
        assertNotNull(ds.getConnection());
    }

    /**
     * Returns a suite running with a single use database with the embedded
     * driver only.
     *
     * @return A test suite.
     */
    public static Test suite() {
        BaseTestSuite suite = new BaseTestSuite(BackupRestoreTest.class);
        return new SupportFilesSetup(
                TestConfiguration.singleUseDatabaseDecorator(suite));
    }
}
