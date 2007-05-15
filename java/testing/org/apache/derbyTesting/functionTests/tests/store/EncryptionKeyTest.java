/*
 *
 * Derby - Class org.apache.derbyTesting.functionTests.tests.store.EncryptionKeyTest
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
package org.apache.derbyTesting.functionTests.tests.store;

import java.io.File;

import java.sql.Connection;
import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.SQLException;
import java.sql.SQLWarning;

import javax.sql.DataSource;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;

import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.JDBCDataSource;
import org.apache.derbyTesting.junit.SupportFilesSetup;

/**
 * Tests various connection sequences to encrypted databases.
 * Four kinds of external keys are used:
 *  <ol><li>the correct key
 *      <li>an incorrect key
 *      <li>a key with odd length (in hex representation)
 *      <li>a key containing invalid characters
 *  </ol>
 * <p>
 * The algorithms used for en-/decryption is determined by the subclasses,
 * where a single algorithm and a set of keys are associated with each
 * subclass.
 * <p>
 * Databases are created in the <tt>EXTINOUT</tt> directory. Backups are made
 * to <tt>EXTINOUT/backups</tt> and restored databases are put into
 * <tt>EXTINOUT/restored</tt> <b>if and only if</b> the databases need to be
 * both written and read. Otherwise backups are placed in <tt>EXTOUT</tt>.
 * Testsuites generated from this class must be wrapped in a
 * <code>SupportFileSetup</code> decorator.
 */
 //@NotThreadSafe
public abstract class EncryptionKeyTest
    extends BaseJDBCTestCase {

    /** Correct key constant. */
    protected static final int CORRECT_KEY = 0;
    /** Wrong key constant. */
    protected static final int WRONG_KEY = 1;
    /** Odd length key constant. */
    protected static final int ODD_LENGTH_KEY = 2;
    /** Invalid char key constant. */
    protected static final int INVALID_CHAR_KEY = 3;

    /** Table holding the test data. */
    private static final String TABLE = "encryptionkeytestdata";
    /** Test data inserted into database and used for verification. */
    private static final int[] DATA = {9,4,2,34,6543,3,123,434,5436,-123,0,123};

    /** The algorithm used by the fixture. */
    private final String algorithm;

    /** The correct key. */
    private final String keyCorrect;
    /** An incorrect key. */
    private final String keyWrong;
    /** A key with odd length. */
    private final String keyOddLength;
    /** A key with an invalid char in it. */
    private final String keyInvalidChar;

    /**
     * Variable to hold the various connections.
     * No guarantee is made about the state of this connection, but it is
     * closed at tear-down.
     */
    private Connection con = null;

    /**
     * Configures a new setup by specifying the encryption properties.
     *
     * @param name name of the fixture
     * @param algorithm encryption algorithm to use
     * @param correctKey the correct encryption key
     * @param wrongKey an incorrect encryption key
     * @param oddLengthKey a key of odd length
     * @param invalidCharKey a key with invalid characters
     */
    public EncryptionKeyTest(String name,
                             String algorithm,
                             String correctKey,
                             String wrongKey,
                             String oddLengthKey,
                             String invalidCharKey) {
        super(name);
        this.algorithm = algorithm;
        this.keyCorrect = correctKey;
        this.keyWrong = wrongKey;
        this.keyOddLength = oddLengthKey;
        this.keyInvalidChar = invalidCharKey;
    }

    /**
     * Clean up the connection maintained by this test.
     */
    protected void tearDown()
            throws java.lang.Exception {
        if (con != null && !con.isClosed()) {
            con.rollback();
            con.close();
            con = null;
        }
        super.tearDown();
    }

    /**
     * Test a sequence of connections and connection attempts.
     * Sequence: Create database, connect to database using correct key,
     * try to connect using incorrect key, connect using correct key.
     */
    public void testConnectionSequence1()
            throws SQLException {
        String dbName = "encryptedDB_ConnectionSequence1";
        // Create database.
        con = createAndPopulateDB(dbName);
        validateDBContents(con);
        // Shutdown the database.
        con.close();
        shutdown(dbName);
        // Connect using correct key.
        con = getConnection(dbName, CORRECT_KEY);
        validateDBContents(con);
        con.close();
        // Shutdown the database.
        shutdown(dbName);
        // Try to connect using wrong key.
        try {
            getConnection(dbName, WRONG_KEY);
            fail("Booting with an incorrect encryption key should fail.");
        } catch (SQLException sqle) {
            assertSQLState("XJ040", sqle);
            assertSQLState("XBCXK", getLastSQLException(sqle));
        }
        // Connect using correct key.
        con = getConnection(dbName, CORRECT_KEY);
        validateDBContents(con);
        con.close();
        // Shutdown the database.
        shutdown(dbName);
    }

    /**
     * Test a sequence of connections and connection attempts.
     * Sequence: Create database, connect to database using odd length key,
     * try to connect using incorrect key, connect using correct key.
     */
    public void testConnectionSequence2()
            throws SQLException {
        String dbName = "encryptedDB_ConnectionSequence2";
        // Create database.
        con = createAndPopulateDB(dbName);
        validateDBContents(con);
        // Shutdown the database.
        con.close();
        shutdown(dbName);
        // Connect using odd length key.
        try {
            con = getConnection(dbName, ODD_LENGTH_KEY);
            fail("Connected with an odd length key.");
        } catch (SQLException sqle) {
            assertSQLState("XJ040", sqle);
            SQLException lastSQLE = getLastSQLException(sqle);
            String sqlState = lastSQLE.getSQLState();
            // The state of this exception varies with the security provider
            // the test is run with.
            // Briefly stated, the deciding factor is whether the error is
            // caught by checks in the Derby code, or by the checks in the
            // security provider. For instance, the (current Sun JCE) DES
            // key implementation does not verify the key length, whereas the
            // AES key implementation does. For other providers, the situation
            // might be different.
            // XBCX0 : A general crypto exception, wraps the exception from the
            //         security provider.
            // XBCXM : A specific Derby exception for external keys of invalid
            //         lengths.
            if (!sqlState.equals("XBCX0") && !sqlState.equals("XBCXM")) {
                throw lastSQLE;
            }
        }
        confirmNonBootedDB(dbName);
        // Try to connect using wrong key.
        try {
            getConnection(dbName, WRONG_KEY);
            fail("Booting with an incorrect encryption key should fail.");
        } catch (SQLException sqle) {
            assertSQLState("XJ040", sqle);
            assertSQLState("XBCXK", getLastSQLException(sqle));
        }
        // Connect using correct key.
        con = getConnection(dbName, CORRECT_KEY);
        validateDBContents(con);
        con.close();
        // Shutdown the database.
        shutdown(dbName);
    }

    /**
     * Backup an encrypted database.
     */
    public void testBackupEncryptedDatabase()
            throws SQLException {
        String dbName = "encryptionKeyDBToBackup";
        // Create the database.
        con = createAndPopulateDB(dbName);
        validateDBContents(con);
        CallableStatement cs = con.prepareCall(
                "CALL SYSCS_UTIL.SYSCS_BACKUP_DATABASE(?)");
        cs.setString(1,
                new File(SupportFilesSetup.EXTINOUT, "backups").getPath());
        // Perform backup.
        cs.execute();
        cs.close();
        con.close();
        shutdown(dbName);
        // Connect to original database after backup.
        con = getConnection(dbName, CORRECT_KEY);
        validateDBContents(con);
        con.close();
        shutdown(dbName);
    }

    /**
     * Create a new database from a backup image.
     */
    public void testCreateDbFromBackup()
            throws SQLException {
        // No ordering imposed by JUnit, so we create our own db and backup.
        // Setup paths and names.
        final String dbName = "encryptionKeyDBToCreateFrom";
        final String backupDbLocation =
            SupportFilesSetup.getReadWrite(
                    new File("backups", "encryptionKeyDBToCreateFrom").getPath()
                ).getPath();
        // Create the database.
        con = createAndPopulateDB(dbName);
        validateDBContents(con);
        CallableStatement cs = con.prepareCall(
                "CALL SYSCS_UTIL.SYSCS_BACKUP_DATABASE(?)");
        cs.setString(1, 
                     new File(SupportFilesSetup.EXTINOUT, "backups").getPath());
        // Perform backup.
        cs.execute();
        cs.close();
        con.close();
        shutdown(dbName);
        // Create a new database from backup.
        String dbNameRestored = dbName + "Restored";
        con = getConnection(dbNameRestored, CORRECT_KEY,
                "createFrom=" + backupDbLocation);
        validateDBContents(con);
        con.close();
        shutdown(dbNameRestored, "restored");
        // Try to create a new database from backup with the wrong key.
        dbNameRestored = dbName + "RestoreAttemptedWrongKey";
        try {
            con = getConnection(dbNameRestored, WRONG_KEY,
                    "createFrom=" + backupDbLocation);
            fail("Created database from encrypted backup with wrong key.");
        } catch (SQLException sqle) {
            assertSQLState("XJ040", sqle);
            assertSQLState("XBCXK", getLastSQLException(sqle));
        }
        assertTrue(con.isClosed());
        // Try to create a new database from backup with an invalid key.
        dbNameRestored = dbName + "RestoreAttemptedInvalidKey";
        try {
            con = getConnection(dbNameRestored, INVALID_CHAR_KEY,
                    "createFrom=" + backupDbLocation);
            fail("Created database from encrypted backup with an invalid key.");
        } catch (SQLException sqle) {
            assertSQLState("XJ040", sqle);
            assertSQLState("XBCXN", getLastSQLException(sqle));
        }
        assertTrue(con.isClosed());
        // Try to create a new database from backup with an odd length key.
        dbNameRestored = dbName + "RestoreAttemptedOddLengthKey";
        try {
            con = getConnection(dbNameRestored, ODD_LENGTH_KEY,
                    "createFrom=" + backupDbLocation);
            fail("Created db from encrypted backup with an odd length key.");
        } catch (SQLException sqle) {
            assertSQLState("XJ040", sqle);
            SQLException lastSQLE = getLastSQLException(sqle);
            String sqlState = lastSQLE.getSQLState();
            // The state of this exception varies with the security provider
            // the test is run with.
            // Briefly stated, the deciding factor is whether the error is
            // caught by checks in the Derby code, or by the checks in the
            // security provider. For instance, the (current Sun JCE) DES
            // key implementation does not verify the key length, whereas the
            // AES key implementation does. For other providers, the situation
            // might be different.
            // XBCX0 : A general crypto exception, wraps the exception from the
            //         security provider.
            // XBCXM : A specific Derby exception for external keys of invalid
            //         lengths.
            if (!sqlState.equals("XBCX0") && !sqlState.equals("XBCXM")) {
                throw lastSQLE;
            }
        }
        assertTrue(con.isClosed());
        // Create a new database from backup again.
        dbNameRestored = dbName + "RestoredOnceMore";
        con = getConnection(dbNameRestored, CORRECT_KEY,
                "createFrom=" + backupDbLocation);
        validateDBContents(con);
        con.close();
        shutdown(dbNameRestored, "restored");
    }

    /**
     * Recover the database using <tt>restoreFrom</tt>.
     */
    public void testRestoreFrom()
            throws SQLException {
        // No ordering imposed by JUnit, so we create our own db and backup.
        String dbName = "encryptionKeyDBToRestoreFrom";
        String dbNameRestored = dbName + "Restored";
        createBackupRestore(dbName, dbNameRestored);
        shutdown(dbNameRestored, "restored");
    }

    /**
     * Try to recover database with an invalid key.
     * <p>
     * It should be noted that the existing database, which has been previously
     * recovered from the same backup image, is deleted/overwritten even though
     * Derby is unable to boot the backup image.
     */
    public void testInvalidRestoreFrom()
            throws SQLException {
        // No ordering imposed by JUnit, so we create our own db and backup.
        String dbName = "encryptionKeyDBToInvalidRestoreFrom";
        String dbNameRestored = dbName + "Restored";
        createBackupRestore(dbName, dbNameRestored);
        shutdown(dbNameRestored, "restored");
        // Check that the database is not booted.
        confirmNonBootedDB("restored/" + dbNameRestored);
        // Validate the existing database.
        con = getConnection("restored/" + dbNameRestored, CORRECT_KEY);
        validateDBContents(con);
        con.close();
        shutdown(dbNameRestored, "restored");
        // Confirm that trying a restore with an invalid key will overwrite
        // the existing database we are trying to restore to/into. This is
        // expected behavior currently, but should maybe change?
        try {
            con = getConnection(dbNameRestored, INVALID_CHAR_KEY,
                    ";restoreFrom=" + obtainDbName(dbName, null));
            fail("Restored database with an invalid key.");
        } catch (SQLException sqle) {
            assertSQLState("XBCXN", sqle);
        }
        // The database should no longer exist.
        try {
            // The "" is a hack to avoid using "create=true".
            con = getConnection("restored/" + dbNameRestored, CORRECT_KEY, "");
            fail("Expected connection to fail due to non-existent database.");
        } catch (SQLException sqle) {
            assertSQLState("XJ004", sqle);
        }
    }

    /**
     * Try to create database with a key of odd length.
     */
    public void testCreateWithOddEncryptionKeyLength()
            throws SQLException {
        try {
            getConnection("encryptedDB_oddKeyLength", ODD_LENGTH_KEY);
            fail("Database creation with odd key length should fail.");
        } catch (SQLException sqle) {
            assertSQLState("XJ041", sqle);
            SQLException lastSQLE = getLastSQLException(sqle);
            String sqlState = lastSQLE.getSQLState();
            // The state of this exception varies with the security provider
            // the test is run with. In general, it depends on whether it is
            // Derby code or the security provider code that detects the
            // problem with the encryption key.
            if (!sqlState.equals("XBCXM") && !sqlState.equals("XJ001")) {
                throw lastSQLE;
            }
        }
    }

    /**
     * Try to create database with a key containing one or more invalid chars.
     */
    public void testCreateWithInvalidEncryptionKey() {
        try {
            getConnection("encryptedDB_invkeyChar", INVALID_CHAR_KEY);
            fail("Database creation with invalid key should fail.");
        } catch (SQLException sqle)  {
            assertSQLState("XJ041", sqle);
            assertSQLState("XBCXN", getLastSQLException(sqle));
        }
    }

    /* ********************************************************************* *
     *                     H E L P E R  M E T H O D S                        *
     * ********************************************************************* */

    /**
     * Obtain absolute path for the specified database name.
     * <p>
     * This absolute path is the name of the database (specified) prefixed with
     * the absolute path to the EXTINOUT directory. The latter is determined by
     * consulting <code>SupportFilesSetup</code>.
     *
     * @param dbName name of the database
     * @param subdirectory directory to prefix the database name with (can be
     *      <code>null</code>). Note that the database name will be prefixed
     *      with the path to the EXTINOUT directory even if this parameter is
     *      <code>null</code>.
     * @return A string with the absolute path to the database.
     * @see SupportFilesSetup
     */
    private String obtainDbName(String dbName, String subdirectory) {
        File tmp = new File(dbName);
        if (subdirectory != null) {
            tmp = new File(subdirectory, dbName);
        }
        final File db = tmp;
        return (String)AccessController.doPrivileged(
                    new PrivilegedAction() {
                        public Object run() {
                            return new File(SupportFilesSetup.EXTINOUT,
                                            db.getPath()).getAbsolutePath();
                        }
                    }
                );
    }

    /**
     * Create encrypted database, validate it, backup, restore and validate
     * recovered database.
     * <p>
     * The source db is shutdown, the recovered db is left booted.
     *
     * @param sourceDb the original database to create
     * @param targetDb the database to recover to
     */
    private void createBackupRestore(String sourceDb, String targetDb)
            throws SQLException {
        // Create the database.
        con = createAndPopulateDB(sourceDb);
        validateDBContents(con);
        CallableStatement cs = con.prepareCall(
                "CALL SYSCS_UTIL.SYSCS_BACKUP_DATABASE(?)");
        cs.setString(1,
                     new File(SupportFilesSetup.EXTINOUT, "backups").getPath());
        // Perform backup.
        cs.execute();
        con.close();
        shutdown(sourceDb);
        confirmNonBootedDB(sourceDb);
        // Use the restoreFrom attribute.
        con = getConnection(targetDb, CORRECT_KEY,
                ";restoreFrom=" + obtainDbName(sourceDb, "backups"));
        validateDBContents(con);
        con.close();
    }

    /**
     * Confirm that the specified encrypted database has not been booted.
     *
     * @param dbName name of an encrypted database
     * @throws junit.framework.AssertionFailedError if the database has been
     *      booted (connection may or may not be established)
     */
    private void confirmNonBootedDB(String dbName) {
        DataSource ds = JDBCDataSource.getDataSource(obtainDbName(dbName, null));
        try {
            ds.getConnection();
        } catch (SQLException sqle) {
            assertSQLState("Database booted? <state:" + sqle.getSQLState() +
                    ", msg:" + sqle.getMessage() + ">", "XJ040", sqle);
        }
    }

    /**
     * Try to establish a connection to the named database with the
     * specified type of key.
     *
     * @param dbName name of the database
     * @param keyMode what kind of key to use (correct, wrong, invalid, odd)
     * @return A connection to the database.
     * @throws SQLException if connection fails
     */
    private Connection getConnection(String dbName, int keyMode)
            throws SQLException {
        return getConnection(dbName, keyMode, null);
    }

    /**
     * Create a new connection to the specified database, using the given
     * connection attributes.
     *
     * @param dbName name of the database
     * @param keyMode what kind of key to use (correct, wrong, invalid, odd)
     * @param recoveryAttribute attribute to recover a database from a backup,
     *      for instance <code>createFrom</code> or <code>restoreFrom</code>.
     *      Both the attribute and its value is expected.
     * @return A connection to the database.
     * @throws SQLException if connection fails
     */
    private Connection getConnection(String dbName,
                                     int keyMode,
                                     String recoveryAttribute)
            throws SQLException {
        DataSource ds = JDBCDataSource.getDataSource(
                obtainDbName(dbName, 
                             recoveryAttribute == null ? null : "restored"));
        StringBuffer str = new StringBuffer(75);
        if (recoveryAttribute == null) {
            // Enable data encryption is this database is being created.
            JDBCDataSource.setBeanProperty(ds, "CreateDatabase", "create");
            str.append("dataEncryption=true;");
        } else {
            str.append(recoveryAttribute);
            str.append(";");
        }
        // Add the encryption algorithm.
        str.append("encryptionAlgorithm=");
        str.append(algorithm);
        str.append(";");
        // Add the key.
        str.append("encryptionKey=");
        switch (keyMode) {
            case CORRECT_KEY:
                str.append(keyCorrect);
                break;
            case WRONG_KEY:
                str.append(keyWrong);
                break;
            case ODD_LENGTH_KEY:
                str.append(keyOddLength);
                break;
            case INVALID_CHAR_KEY:
                str.append(keyInvalidChar);
                break;
            default:
                throw new IllegalArgumentException(
                        "Invalid key mode specified: " + keyMode);
        }
        str.append(";");
        JDBCDataSource.setBeanProperty(
                ds, "connectionAttributes", str.toString());
        return ds.getConnection();
    }

    /**
     * Shutdown the specified database.
     *
     * @param databaseName the name of the database
     */
    protected void shutdown(String databaseName)
            throws SQLException {
        shutdown(databaseName, null);
    }

    /**
     * Shutdown the database, specified by the database name and prefix.
     *
     * @param databaseName the name of the database
     * @param dir sub-directory prefix for the database
     */
    protected void shutdown(String databaseName, String dir)
            throws SQLException {
        DataSource ds = JDBCDataSource.getDataSource(
                obtainDbName(databaseName, dir));
        JDBCDataSource.shutdownDatabase(ds);
    }

    /**
     * Create a new database and populate it.
     * <p>
     * The method fails with an exception if the database already exists.
     * This is because it is the creation process that is to be tested.
     *
     * @param dbName name of the database to create
     * @return A connection the to the newly created database.
     * @throws SQLException if the database already exist, or
     *      a general error happens during database interaction
     */
    protected Connection createAndPopulateDB(String dbName)
            throws SQLException {
        Connection con = getConnection(dbName, CORRECT_KEY);
        SQLWarning warning = con.getWarnings();
        // If the database already exists, fail the test.
        if (warning != null) {
            if ("01J01".equals(warning.getSQLState())) {
                fail("Refusing to continue, database already exists <" +
                        warning.getMessage() + ">");
            }
        }
        Statement stmt = con.createStatement();
        stmt.executeUpdate("CREATE TABLE " + TABLE + " (id int NOT NULL, " +
                "val int NOT NULL, PRIMARY KEY(id))");
        stmt.close();
        PreparedStatement ps = con.prepareStatement("INSERT INTO " + TABLE +
                " (id, val) VALUES (?,?)");
        for (int i=0; i < DATA.length; i++) {
            ps.setInt(1, i);
            ps.setInt(2, DATA[i]);
            ps.executeUpdate();
        }
        ps.close();
        return con;
    }

    /**
     * Validate the data in the database against the data model.
     *
     * @param con the database to validate the contents of
     * @throws junit.framework.AssertionFailedError if there is a mismatch
     *      between the data in the database and the model
     */
    protected void validateDBContents(Connection con)
            throws SQLException {
        Statement stmt = con.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT id, val FROM " + TABLE +
                                            " ORDER BY id");
        int id, val;
        while (rs.next()) {
            id = rs.getInt(1);
            val = rs.getInt(2);
            if (id >= DATA.length) {
                fail("Id in database out of bounds for data model; " +
                        id + " >= " + DATA.length);
            }
            if (val != DATA[id]) {
                fail("Mismatch between db and model for id " + id + ";" +
                        val + " != " + DATA[id]);
            }
        }
        rs.close();
        stmt.close();
    }
} // End EncryptionKeyTest
