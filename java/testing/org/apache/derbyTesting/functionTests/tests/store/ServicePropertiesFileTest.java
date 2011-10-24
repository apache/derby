/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.store.ServicePropertiesFileTest

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

package org.apache.derbyTesting.functionTests.tests.store;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import javax.sql.DataSource;

import junit.framework.Test;

import org.apache.derbyTesting.functionTests.util.PrivilegedFileOpsForTests;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.JDBCDataSource;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Tests Derby's ability to recover from various conditions related to the
 * service properties file.
 * <p>
 * The basic pattern of the tests is to start with a pristine database, modify
 * 'service.properties' and/or 'service.propertiesold', and then finally boot
 * the database and assert what happened with the two aforementioned files.
 */
public class ServicePropertiesFileTest
        extends BaseJDBCTestCase {
    
    private static final String LOG_A_MODE =
            "derby.storage.logArchiveMode";
    /**
     * End-of-file token used by Derby in 'service.properties'.
     */
    private static final String END_TOKEN =
            "#--- last line, don't put anything after this line ---";
    /** Logical name of the pristine database. */
    private static final String DB_NAME = "spfTestDb";

    /** Where the databases are living. */
    private static File databasesDir;
    /** Path to the pristine database. */
    private static File pristineDb;
    /** Whether the pristine database has been initialized or not. */
    private static boolean dbInitialized;

    /** Database that will be deleted during {@code shutDown}. */
    private File dbToDelete;
    /**
     * Path to 'service.properties' of the current database.
     * @see #copyDbAs
     */
    private File spf;
    /**
     * Path to 'service.propertiesold' of the current database.
     * @see #copyDbAs
     */
    private File spfOld;

    public ServicePropertiesFileTest(String name) {
        super(name);
    }

    /**
     * Initializes the pristine database if required.
     */
    public void setUp()
            throws SQLException {
        if (!dbInitialized) {
            DataSource ds = JDBCDataSource.getDataSourceLogical(DB_NAME);
            JDBCDataSource.setBeanProperty(ds, "createDatabase", "create");
            ds.getConnection();
            JDBCDataSource.shutdownDatabase(ds);
            File systemHome = new File(getSystemProperty("derby.system.home"));
            databasesDir = new File(systemHome, "singleUse");
            pristineDb = new File(
                    systemHome,
                    TestConfiguration.getCurrent().getPhysicalDatabaseName(
                        DB_NAME));
            dbInitialized = true;
        }        
    }

    /**
     * Deletes the last database copy (if one exists).
     */
    public void tearDown()
            throws Exception {
        if (dbToDelete != null) {
            assertDirectoryDeleted(dbToDelete);    
        }
        super.tearDown();
    }

    /**
     * Tests what happens when the service properties file is missing and there
     * is no backup available.
     */
    public void testMissingServicePropertiesFileNoBackup()
            throws IOException, SQLException {
        // Prepare
        String db = "spfTestMissingSPFNB";
        copyDbAs(db);
        PrivilegedFileOpsForTests.delete(spf);
        assertPresence(false, false);

        // This will currently fail with a message saying the database wasn't
        // found, even though everything is there except for the service
        // properties file.
        try {
            connectThenShutdown(db);
            fail("booted database without a service.properties file");
        } catch (SQLException sqle) {
            assertSQLState("error message has changed", "XJ004", sqle);
        }
    }

    /**
     * Tests handling of the situation where the service properties file is
     * missing, but a backup is available.
     * <p>
     * The expected behavior is to restore (by renaming) the service properties
     * file from the backup.
     */
    public void testMissingServicePropertiesFileWithBackup()
            throws IOException, SQLException {
        // Prepare
        String db = "spfTestMissingSPFWB";
        copyDbAs(db);
        createSPFBackup(false);
        assertPresence(false, true);

        // Recover and assert
        connectThenShutdown(db);
        assertNormalPresence();
    }

    /**
     * Tests handling of the situation where both the service properties file
     * and the backup are available.
     * <p>
     * Expected behavior here is to delete the backup (given that the original
     * service properties file contains the end-of-file token).
     */
    public void testSevicePropertiesFileWithBackup()
            throws IOException, SQLException {
        // Prepare
        String db = "spfTestSPFWB";
        copyDbAs(db);
        createSPFBackup(true);
        assertPresence(true, true);

        // Recover and assert
        connectThenShutdown(db);
        assertNormalPresence();
        assertEOFToken(spf);
    }

    /**
     * Tests the situation where both the service properties file and a backup
     * are available, but the service properties file is corrupted (see note
     * below).
     * <p>
     * The expected behavior is to delete the original service properties file
     * and then restore it from the backup (i.e. by renaming).
     * <p>
     * In this regard, a corrupt service properties file is one where the
     * end-of-file token is missing. No other error conditions are detected,
     * i.e. if properties are removed manually or the values are modified.
     */
    public void testSevicePropertiesFileCorruptedWithBackup()
            throws IOException, SQLException {
        // Prepare
        String db = "spfTestSPFCWB";
        copyDbAs(db);
        createSPFBackup(true);
        removeEOFToken(spf);
        assertPresence(true, true);

        // Recover and assert
        connectThenShutdown(db);
        assertNormalPresence();
        assertEOFToken(spf);
    }

    /**
     * Ensures that Derby can handle the case where the backup file already
     * exists when editing the service properties.
     */
    public void testBackupWithBackupExisting()
            throws IOException, SQLException {
        // Prepare
        String db = "spfTestBWBE";
        copyDbAs(db);
        assertPresence(true, false);
        // Make sure 'derby.storage.logArchiveMode' isn't present already.
        assertEquals(0, grepForToken(LOG_A_MODE, spf));

        // Connect, then enable log archive mode to trigger edit.
        DataSource ds = JDBCDataSource.getDataSource();
        JDBCDataSource.setBeanProperty(ds, "databaseName", "singleUse/" + db);
        Connection con = ds.getConnection();
        // Create the service properties file backup.
        createSPFBackup(true);

        // Trigger service properties file edit.
        Statement stmt = con.createStatement();
        stmt.execute("CALL SYSCS_UTIL.SYSCS_DISABLE_LOG_ARCHIVE_MODE(0)");
        con.close();
        // Shut down the database.
        JDBCDataSource.shutdownDatabase(ds);

        assertNormalPresence();
        assertEquals(1, grepForToken(LOG_A_MODE + "=false", spf));
    }

    /**
     * Asserts that the presence of the service properties file and the backup
     * is normal, that is that the former is present and the latter isn't.
     */
    private void assertNormalPresence() {
        assertPresence(true, false);
    }

    /**
     * Asserts the specified presence of the original and the backup service
     * properties files.
     *
     * @param spfPresence presence of the original file
     * @param spfOldPresence presence of the backup file
     */
    private void assertPresence(boolean spfPresence, boolean spfOldPresence) {
        assertEquals("incorrect '" + spf.getAbsolutePath() + "' presence,",
                spfPresence, PrivilegedFileOpsForTests.exists(spf));
        assertEquals("incorrect '" + spfOld.getPath() + "' presence,",
                spfOldPresence, PrivilegedFileOpsForTests.exists(spfOld));
    }

    /**
     * Asserts that the specified file ends with the end-of-file token.
     */
    private void assertEOFToken(File file)
            throws IOException {
        BufferedReader in = new BufferedReader(
                PrivilegedFileOpsForTests.getFileReader(file));
        String prev = null;
        String cur;
        while ((cur = in.readLine()) != null) {
            prev = cur;
        }
        in.close();
        assertNotNull("last line is null - empty file?", prev);
        assertTrue(prev.startsWith(END_TOKEN));
    }

    /**
     * Removes the end-of-file token from the specified file.
     */
    private void removeEOFToken(File original)
            throws IOException {
        // Move file, then rewrite by removing last line (the token).
        File renamed = new File(original.getAbsolutePath() + "-renamed");
        PrivilegedFileOpsForTests.copy(original, renamed);
        PrivilegedFileOpsForTests.delete(original);
        BufferedReader in = new BufferedReader(
                PrivilegedFileOpsForTests.getFileReader(renamed));
        // Default charset should be 8859_1.
        BufferedWriter out = new BufferedWriter(
                PrivilegedFileOpsForTests.getFileWriter(original));
        String prev = null;
        String line;
        while ((line = in.readLine()) != null) {
            if (prev != null) {
                out.write(prev);
                out.newLine();
            }
            prev = line;
        }
        assertEquals(END_TOKEN, prev);
        in.close();
        out.close();
        PrivilegedFileOpsForTests.delete(renamed);
    }

    /**
     * Looks for the specified token in the given file.
     *
     * @param token the search token
     * @param file the file to search
     * @return The number of matching lines.
     *
     * @throws IOException if accessing the specified file fails
     */
    private int grepForToken(String token, File file)
            throws IOException {
        int matchingLines = 0;
        BufferedReader in = new BufferedReader(
                PrivilegedFileOpsForTests.getFileReader(file));    
        String line;
        while ((line = in.readLine()) != null) {
            if (line.indexOf(token) != -1) {
                matchingLines++;
            }
        }
        in.close();
        return matchingLines;
    }
            
    /**
     * Copies the master/pristine database to a new database.
     *
     * @param name name of the database to copy to
     */
    private void copyDbAs(String name)
            throws IOException {
        File newDb = new File(databasesDir, name);
        dbToDelete = newDb;
        PrivilegedFileOpsForTests.copy(pristineDb, newDb);
        spf = new File(newDb, "service.properties");
        spfOld = new File(newDb, "service.propertiesold");
    }

    /** Dependent on state set by {@linkplain #copyDbAs}. */
    private void createSPFBackup(boolean keepOriginal)
            throws IOException {
        PrivilegedFileOpsForTests.copy(spf, spfOld);
        if (!keepOriginal) {
            PrivilegedFileOpsForTests.delete(spf);
        }
    }

    /**
     * Connects to the specified database, then shuts it down.
     * <p>
     * This method is used to trigger the recovery logic for the service
     * properties file.
     *
     * @param db database to connect to (expected to live in 'system/singleUse')
     */ 
    private void connectThenShutdown(String db)
            throws SQLException {
        DataSource ds = JDBCDataSource.getDataSource();
        JDBCDataSource.setBeanProperty(ds, "databaseName", "singleUse/" + db);
        ds.getConnection().close();
        JDBCDataSource.shutdownDatabase(ds);
    }

    public static Test suite() {
        return TestConfiguration.additionalDatabaseDecoratorNoShutdown(
            TestConfiguration.embeddedSuite(ServicePropertiesFileTest.class),
            DB_NAME);
    }
}
