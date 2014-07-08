/*

   Derby - Class org.apache.derbyTesting.functionTests.store.CryptoCrashRecoveryTest

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
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import javax.sql.DataSource;
import junit.framework.Test;
import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.JDBCDataSource;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * This class tests crash/recovery scenarios during cryptographic operations on
 * the database.
 * <p>
 * Debug flags are used to simulate crashes during the encryption of an
 * un-encrypted database, re-encryption of an encrypted database with new
 * password/key, and decryption of an encrypted database.
 * <p>
 * Unlike the other recovery tests which do a setup and recovery as different
 * tests, crash/recovery for cryptographic operations can be simulated in one
 * test itself because the cryptographic operation is performed at boot time.
 * When debug flags are set the database boot itself fails. To test the
 * recovery, it is just a matter of clearing up the debug flag and rebooting
 * the database.
 * <p>
 * In non-debug mode (compiled as "insane") this test does nothing.
 */
public class CryptoCrashRecoveryTest
    extends BaseJDBCTestCase {

    private static boolean USE_ENC_PWD = true;
    private static boolean USE_ENC_KEY = false;

    private static final int OP_ENCRYPT = 0;
    private static final int OP_DECRYPT = 1;
    private static final int OP_REENCRYPT = 2;

    /** Table name used by the test. */
    private static final String TEST_TABLE_NAME = "emp";

    private static final String OLD_PASSWORD = "xyz1234abc";
    private static final String NEW_PASSWORD = "new1234xyz";

    private static final String OLD_KEY = "6162636465666768";
    private static final String NEW_KEY = "5666768616263646";

	public CryptoCrashRecoveryTest(String name) {
        super(name);
	}

    public static Test suite() {
        Test suite;
        if (SanityManager.DEBUG) {
            suite = TestConfiguration.embeddedSuite(
                    CryptoCrashRecoveryTest.class);
        } else {
            suite = new BaseTestSuite(
                    "CryptoCrashRecovery disabled due to non-debug build");
            println("CryptoCrashRecoveryTest disabled due to non-debug build");
        }
        return suite;
    }

	public void testDecryptionWithBootPassword()
            throws Exception {
        String db = "wombat_pwd_de";
        // Crash recovery during decryption (with password mechanism).
        DataSource ds = JDBCDataSource.getDataSource(db);
        runCrashRecoveryTestCases(ds, OP_DECRYPT, USE_ENC_PWD);
        assertDirectoryDeleted(new File("system", db));
    }

    public void testDecryptionWithEncryptionKey()
            throws Exception {
        String db = "wombat_key_de";
        // Crash recovery during database decryption (with encryption key).
        DataSource ds = JDBCDataSource.getDataSource(db);
        runCrashRecoveryTestCases(ds, OP_DECRYPT, USE_ENC_KEY);
        assertDirectoryDeleted(new File("system", db));
    }

	public void testEncryptionWithBootPassword()
            throws Exception {
        String db = "wombat_pwd_en";
        // Crash recovery during encryption using the password mechanism.
        DataSource ds = JDBCDataSource.getDataSource(db);
        runCrashRecoveryTestCases(ds, OP_ENCRYPT, USE_ENC_PWD);
        assertDirectoryDeleted(new File("system", db));
    }

	public void testEncryptionWithEncryptionKey()
            throws Exception {
        String db = "wombat_key_en";
        // Crash recovery during database encryption using the encryption key.
        DataSource ds = JDBCDataSource.getDataSource(db);
        runCrashRecoveryTestCases(ds, OP_ENCRYPT, USE_ENC_KEY);
        assertDirectoryDeleted(new File("system", db));
    }

	public void testReEncryptionWithBootPassword()
            throws Exception {
        String db = "wombat_pwd_ren";
        // Crash recovery during re-encryption using the password mechanism.
        DataSource ds = JDBCDataSource.getDataSource(db);
        runCrashRecoveryTestCases(ds, OP_REENCRYPT, USE_ENC_PWD);
        assertDirectoryDeleted(new File("system", db));
    }

	public void testReEncryptionWithEncryptionKey()
            throws Exception {
        String db = "wombat_key_ren";
        // Crash recovery during re-encryption using an encryption key.
        DataSource ds = JDBCDataSource.getDataSource(db);
        runCrashRecoveryTestCases(ds, OP_REENCRYPT, USE_ENC_KEY);
        assertDirectoryDeleted(new File("system", db));
    }

    /**
     * Runs crash recovery test scenarios for the given cryptographic operation
     * using the debug flags.
     *
     * @param ds data source
     * @param operation the cryptographic operation to perform
     * @param useEncPwd whether to use encryption key or boot password (see
     *      {@linkplain #USE_ENC_KEY} and {@linkplain #USE_ENC_PWD})
     */
    private void runCrashRecoveryTestCases(DataSource ds, int operation,
                                           boolean useEncPwd)
            throws SQLException {
        Connection con = null; // silence the compiler
        switch (operation) {
            case OP_DECRYPT:
                // Fall through.
            case OP_REENCRYPT:
                con = createEncryptedDatabase(ds, useEncPwd);
                break;
            case OP_ENCRYPT:
                con = createDatabase(ds);
                break;
            default:
                fail("unsupported operation: " + operation);
        }

        createTable(con, TEST_TABLE_NAME);
        // Load some rows, used for verification later.
        insert(con, TEST_TABLE_NAME, 100);
        con.commit();
        con.close();
        JDBCDataSource.shutdownDatabase(ds);

        // Following cases of cryptographic operations should be rolled back.
        Boolean useNewCredential =
                (operation == OP_REENCRYPT ? Boolean.FALSE : null);

        crash(ds, operation, useEncPwd, TEST_REENCRYPT_CRASH_BEFORE_COMMT);

        crash(ds, operation, useEncPwd, TEST_REENCRYPT_CRASH_AFTER_COMMT);
        crashInRecovery(ds, useEncPwd, useNewCredential,
                     TEST_REENCRYPT_CRASH_AFTER_RECOVERY_UNDO_LOGFILE_DELETE);
        crashInRecovery(ds, useEncPwd, useNewCredential,
                     TEST_REENCRYPT_CRASH_AFTER_RECOVERY_UNDO_REVERTING_KEY);
        crashInRecovery(ds, useEncPwd, useNewCredential,
                     TEST_REENCRYPT_CRASH_BEFORE_RECOVERY_FINAL_CLEANUP);


        crash(ds, operation, useEncPwd, TEST_REENCRYPT_CRASH_AFTER_COMMT);
        crashInRecovery(ds, useEncPwd, useNewCredential,
                     TEST_REENCRYPT_CRASH_AFTER_RECOVERY_UNDO_LOGFILE_DELETE);
        // Retry operation and crash.
        crash(ds, operation, useEncPwd, TEST_REENCRYPT_CRASH_AFTER_COMMT);

        crash(ds, operation, useEncPwd,
                TEST_REENCRYPT_CRASH_AFTER_SWITCH_TO_NEWKEY);
        crashInRecovery(ds, useEncPwd, useNewCredential,
                     TEST_REENCRYPT_CRASH_AFTER_RECOVERY_UNDO_LOGFILE_DELETE);
        crashInRecovery(ds, useEncPwd, useNewCredential,
                     TEST_REENCRYPT_CRASH_AFTER_RECOVERY_UNDO_REVERTING_KEY);
        crashInRecovery(ds, useEncPwd, useNewCredential,
                     TEST_REENCRYPT_CRASH_BEFORE_RECOVERY_FINAL_CLEANUP);

        crash(ds, operation, useEncPwd,
                TEST_REENCRYPT_CRASH_AFTER_SWITCH_TO_NEWKEY);
        crashInRecovery(ds, useEncPwd, useNewCredential,
                     TEST_REENCRYPT_CRASH_AFTER_RECOVERY_UNDO_REVERTING_KEY);
        // Retry operation and crash.
        crash(ds, operation, useEncPwd,
                TEST_REENCRYPT_CRASH_AFTER_SWITCH_TO_NEWKEY);
        crashInRecovery(ds, useEncPwd, useNewCredential,
                     TEST_REENCRYPT_CRASH_BEFORE_RECOVERY_FINAL_CLEANUP);

        // Following cases should be successful, only cleanup is pending.
        // Crash after the cryptographic operation has been performed, but
        // before cleanup.
        // If re-encryption is complete, database should be bootable with the
        // new password. If decryption is complete, database should be bootable
        // without specifying a boot password / key.
        useNewCredential =
                (operation == OP_REENCRYPT ? Boolean.TRUE : Boolean.FALSE);
        crash(ds, operation, useEncPwd, TEST_REENCRYPT_CRASH_AFTER_CHECKPOINT);
        crashInRecovery(ds, useEncPwd, useNewCredential,
                     TEST_REENCRYPT_CRASH_BEFORE_RECOVERY_FINAL_CLEANUP);
        if (operation == OP_DECRYPT) {
            useNewCredential = null;
        }
        recover(ds, useEncPwd, useNewCredential);
        JDBCDataSource.shutdownDatabase(ds);
    }

    /**
     * Crashes the engine at the point specified by the debug flag while
     * performing the requested operation.
     *
     * @param ds database
     * @param operation cryptographic operation to perform
     * @param useEncPwd whether to use boot password or encryption key
     * @param debugFlag debug flag to enable to make the engine crash
     */
    private void crash(DataSource ds, int operation, boolean useEncPwd,
                       String debugFlag) {
        println("Testing crash at " + debugFlag);
        // Set the debug flag to crash.
        setDebugFlag(debugFlag);

        try {
            switch (operation) {
                case OP_REENCRYPT:
                    reEncryptDatabase(ds, useEncPwd);
                    break;
                case OP_ENCRYPT:
                    encryptDatabase(ds, useEncPwd);
                    break;
                case OP_DECRYPT:
                    decryptDatabase(ds, useEncPwd);
                    break;
                default:
                    fail("unsupported operation");
            }
            fail("crypto operation didn't crash as expected");
        } catch (SQLException sqle) {
            // check that database boot failed at the set debug flag.
            verifyBootException(sqle, debugFlag);
        } finally {
            clearDebugFlag(debugFlag);
        }
    }

    /**
     * Crashes the engine in recovery of the given database at the point
     * specified by the debug flag.
     *
     * @param ds database
     * @param useEncPwd whether to use boot password or encryption key
     * @param useNewCredential tri-state telling whether to use the old, the
     *      new, or no credential when booting the database
     * @param debugFlag debug flag to enable to make the engine crash
     */
    private void crashInRecovery(DataSource ds, boolean useEncPwd,
                                 Boolean useNewCredential, String debugFlag)
            throws SQLException {
        println("Recovery crash at " + debugFlag);

        // set the debug flag to crash.
        setDebugFlag(debugFlag);
        try {
            bootDatabase(ds, useEncPwd, useNewCredential);
            fail("database booted unexpectedly");
        } catch (SQLException sqle) {
            // check that database boot failed at the set debug flag.
            verifyBootException(sqle, debugFlag);
        } finally {
            clearDebugFlag(debugFlag);
        }
    }

    /*
     * Recover the database that failed during re-encryption and
     * perform some simple sanity checks on the database.
     */
    private void recover(DataSource ds, boolean useEncKey,
                         Boolean useNewCredential)
            throws SQLException {
        // starting recovery of database with failed Re-encryption
        // in debug mode;

        Connection con = bootDatabase(ds, useEncKey, useNewCredential);

        // Verify that the contents of the db are ok.
        runConsistencyChecker(con, TEST_TABLE_NAME);
        // Insert some rows, this might fail if anything is
        // wrong in the logging system setup.
        insert(con, TEST_TABLE_NAME, 100);
        con.commit();
        con.close();
    }

    // Debug flags are set to simulate a crash at different
    // points during a cryptographic operation on the database.
    // These flags should match the flags in the engine code, and they
    // are redefined here to avoid pulling the engine code into the tests.

	private static final String TEST_REENCRYPT_CRASH_BEFORE_COMMT =
            "TEST_REENCRYPT_CRASH_BEFORE_COMMT";
    private static final String TEST_REENCRYPT_CRASH_AFTER_COMMT =
            "TEST_REENCRYPT_CRASH_AFTER_COMMT";
    private static final String TEST_REENCRYPT_CRASH_AFTER_SWITCH_TO_NEWKEY =
        "TEST_REENCRYPT_CRASH_AFTER_SWITCH_TO_NEWKEY";
    private static final String TEST_REENCRYPT_CRASH_AFTER_CHECKPOINT =
        "TEST_REENCRYPT_CRASH_AFTER_CHECKPOINT";
    private static final String
        TEST_REENCRYPT_CRASH_AFTER_RECOVERY_UNDO_LOGFILE_DELETE =
            "TEST_REENCRYPT_CRASH_AFTER_RECOVERY_UNDO_LOGFILE_DELETE";
    private static final String
        TEST_REENCRYPT_CRASH_AFTER_RECOVERY_UNDO_REVERTING_KEY =
            "TEST_REENCRYPT_CRASH_AFTER_RECOVERY_UNDO_REVERTING_KEY";
    private static final String
        TEST_REENCRYPT_CRASH_BEFORE_RECOVERY_FINAL_CLEANUP =
            "TEST_REENCRYPT_CRASH_BEFORE_RECOVERY_FINAL_CLEANUP";

    private void setDebugFlag(String debugFlag) {
        if (SanityManager.DEBUG) {
            SanityManager.DEBUG_SET(debugFlag);
        }
    }

    private void clearDebugFlag(String debugFlag) {
        if (SanityManager.DEBUG) {
            SanityManager.DEBUG_CLEAR(debugFlag);
        }
    }

    /**
     * Verifies that database boot failed because of the right reasons.
     *
     * @param sqle the exception thrown when
     * @param debugFlag the debug flag that was set before booting
     * @throws junit.framework.AssertionFailedError if the boot failure is
     *      deemed invalid
     */
    private void verifyBootException(SQLException sqle, String debugFlag) {
        assertSQLState("XJ040", sqle);
        // Boot failed as expected triggered by a debug flag.
        // Now check if it failed because of the expected debug flag.
        SQLException ne = sqle.getNextException();
        while (ne != null) {
            String message = ne.getMessage();
            // Check if debug flag exists in the message.
            if (message.indexOf(debugFlag) != -1) {
                return;
            }
            ne = ne.getNextException();
        }
        fail("real error or wrong debug flag triggered crash", sqle);
    }

    /**
     * Creates the table that is used by this test.
     *
     * @param con connection to the database
     * @param tableName  name of the table to create
     * @exception SQLException if any database exception occurs.
     */
    private void createTable(Connection con, String tableName)
            throws SQLException {
        Statement s = con.createStatement();
        s.executeUpdate("CREATE TABLE " + tableName +
                        "(id INT," +
                        "name VARCHAR(200))");
        s.executeUpdate("create index " + tableName + "_id_idx on " +
                        tableName + "(id)");
        s.close();
    }

    /**
	 * Runs some consistency checks on the specified table.
     *
     * @param con connection to the database
	 * @param tableName target table
     * @exception SQLException if any database exception occurs
	 */
	private void runConsistencyChecker(Connection con, String tableName)
            throws SQLException {
		Statement stmt = con.createStatement();
		stmt.execute("values SYSCS_UTIL.SYSCS_CHECK_TABLE('APP',  'EMP')");
        // Check the data in the EMP table.
        verifyContents(con, tableName);
	}

    /**
     * Inserts rows into the specified table.
     *
     * @param con connection to the database
     * @param tableName target table
     * @param rowCount number of rows to insert
     * @exception SQLException if any database exception occurs
     */
    private void insert(Connection con, String tableName, int rowCount)
            throws SQLException {
        PreparedStatement ps = con.prepareStatement(
                "INSERT INTO " + tableName + " VALUES(?,?)");
        int startId = findMaxId(con, tableName);
        int endId = rowCount + startId;
        for (int i = startId; i < endId; i++) {
            ps.setInt(1, i); // ID
            ps.setString(2 , "skywalker" + i);
            ps.executeUpdate();
        }
        ps.close();
        con.commit();
    }

    /**
     * Returns the highest id in the given table.
     *
     * @param con connection to the database.
     * @param tableName name of the table
     * @return The highest id.
     * @exception SQLException if any database exception occurs.
     */
    private int findMaxId(Connection con, String tableName)
            throws SQLException {
        Statement s = con.createStatement();
        ResultSet rs = s.executeQuery("SELECT max(ID) from " + tableName);
        assertTrue(rs.next());
        int max = rs.getInt(1);
        rs.close();
        s.close();
        return max;
    }

    /**
     * Verifies the rows in the given table.
     *
     * @param con connection to the database
     * @param tableName table to select from
     * @exception SQLException if any database exception occurs
     */
    private void verifyContents(Connection con, String tableName)
            throws SQLException {
        Statement s = con.createStatement();
        ResultSet rs = s.executeQuery(
                "SELECT ID, name from " +  tableName + " order by id");
        int count = 0;
        int id = 0;
        while (rs.next()) {
            int tid = rs.getInt(1);
            String name = rs.getString(2);
            assertEquals("skywalker" + id, name);
            assertEquals(id, tid);
            id++;
            count++;
        }

        rs.close();
        s.close();
        con.commit();
    }

    /**
     * Creates an encrypted database.
     *
     * @param ds database
     * @param useEncPwd whether to use boot password or encryption key
     */
    private Connection createEncryptedDatabase(DataSource ds, boolean useEncPwd)
            throws SQLException {
        String connAttrs = "dataEncryption=true;";
        if (useEncPwd) {
            // create encrypted database.
            connAttrs += "bootPassword=" + OLD_PASSWORD;
        } else {
            // create an encrypted  database.
            connAttrs += "encryptionKey=" + OLD_KEY;
        }
        JDBCDataSource.setBeanProperty(ds, "createDatabase", "create");
        JDBCDataSource.setBeanProperty(ds, "connectionAttributes", connAttrs);
        try {
            return ds.getConnection();
        } finally {
            JDBCDataSource.clearStringBeanProperty(ds, "createDatabase");
            JDBCDataSource.clearStringBeanProperty(ds, "connectionAttributes");
        }
    }

    /**
     * Creates an un-encrypted database.
     *
     * @param ds database
     */
    private Connection createDatabase(DataSource ds)
            throws SQLException {
        JDBCDataSource.setBeanProperty(ds, "createDatabase", "create");
        try {
            Connection con = ds.getConnection();
            JDBC.assertNoWarnings(con.getWarnings());
            return con;
        } finally {
            JDBCDataSource.clearStringBeanProperty(ds, "createDatabase");
        }
    }

    /**
     * Re-encrypts the database.
     *
     * @param ds database
     * @param useEncPwd whether to use boot password or encryption key
     * @throws SQLException if any database exception occurs
     */
    private Connection reEncryptDatabase(DataSource ds, boolean useEncPwd)
            throws SQLException {
        String connAttrs;
        if (useEncPwd) {
            connAttrs = "bootPassword=" + OLD_PASSWORD +
                ";newBootPassword=" + NEW_PASSWORD;
        } else {
            connAttrs = "encryptionKey=" + OLD_KEY +
                ";newEncryptionKey=" + NEW_KEY;
        }

        JDBCDataSource.setBeanProperty(ds, "connectionAttributes", connAttrs);
        println("re-encrypting " + db(ds) + " with " + connAttrs);
        // Re-encrypt the database.
        try {
            return ds.getConnection();
        } finally {
            JDBCDataSource.clearStringBeanProperty(ds, "connectionAttributes");
        }
    }

    /**
     * Encrypts an un-encrypted database.
     *
     * @param ds database
     * @param useEncPwd whether to use boot password or encryption key
     * @exception SQLException if any database exception occurs.
     */
    private Connection encryptDatabase(DataSource ds, boolean useEncPwd)
        throws SQLException {
        String connAttrs = "dataEncryption=true;";
        if (useEncPwd) {
            connAttrs += "bootPassword=" + OLD_PASSWORD;
        } else {
            connAttrs += "encryptionKey=" + OLD_KEY;
        }

        JDBCDataSource.setBeanProperty(ds, "connectionAttributes", connAttrs);
        println("encrypting " + db(ds) + " with " + connAttrs);
        //Encrypt the existing database.
        try {
            return ds.getConnection();
        } finally {
            JDBCDataSource.clearStringBeanProperty(ds, "connectionAttributes");
        }
    }

    /**
     * Decrypts an encrypted database.
     *
     * @param ds database
     * @param useEncPwd whether to use boot password or encryption key
     * @throws SQLException if any database exception occurs
     */
    private Connection decryptDatabase(DataSource ds, boolean useEncPwd)
        throws SQLException {
        String connAttrs = "decryptDatabase=true;";
        if (useEncPwd) {
            connAttrs += "bootPassword=" + OLD_PASSWORD;
        } else {
            connAttrs += "encryptionKey=" + OLD_KEY;
        }

        JDBCDataSource.setBeanProperty(ds, "connectionAttributes", connAttrs);
        println("decrypting " + db(ds) + " with " + connAttrs);
        //Decrypt the existing database.
        try {
            return ds.getConnection();
        } finally {
            JDBCDataSource.clearStringBeanProperty(ds, "connectionAttributes");
        }
    }

    /**
     * Boots the database.
     *
     * @param ds database
     * @param useEncPwd whether to use boot password or encryption key
     * @param useNewCredential tri-state telling whether to use the old, the
     *      new, or no credential when booting the database
     * @exception SQLException if any database exception occurs.
     */
    private Connection bootDatabase(DataSource ds,
                                    boolean useEncPwd, Boolean useNewCredential)
            throws SQLException {
        String connAttrs = "";
        if (useEncPwd) {
            if (Boolean.TRUE.equals(useNewCredential)) {
                connAttrs = "bootPassword=" + NEW_PASSWORD;
            } else if (Boolean.FALSE.equals(useNewCredential)) {
                connAttrs = "bootPassword=" + OLD_PASSWORD;
            }
        } else {
            if (Boolean.TRUE.equals(useNewCredential)) {
                connAttrs = "encryptionKey=" + NEW_KEY;
            } else if (Boolean.FALSE.equals(useNewCredential)) {
                connAttrs = "encryptionKey=" + OLD_KEY;
            }
        }

        JDBCDataSource.setBeanProperty(ds, "connectionAttributes", connAttrs);
        println("booting " + db(ds) +
                (connAttrs.length() > 0 ? " with " + connAttrs : ""));
        try {
            return ds.getConnection();
        } finally {
            JDBCDataSource.clearStringBeanProperty(ds, "connectionAttributes");
        }
    }

    /** Extracts the database name from the data source. */
    private static String db(DataSource ds) {
        try {
            return (String)JDBCDataSource.getBeanProperty(ds, "databaseName");
        } catch (Exception e) {
            return "<unknown/error>";
        }
    }
}
