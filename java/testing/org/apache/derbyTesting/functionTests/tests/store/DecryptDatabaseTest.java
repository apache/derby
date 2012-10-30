/*

   Derby - Class
       org.apache.derbyTesting.functionTests.tests.store.DecryptDatabaseTest

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

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import javax.sql.DataSource;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.Decorator;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.JDBCDataSource;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Tests that database decryption works, and that various error conditions
 * are detected and dealt with.
 * <p>
 * NOTE: Care must be taken to shut down a database before testing the
 * various connection attributes that apply to cryptographic operations, as
 * they are typically ignored if the database has already been booted.
 */
public class DecryptDatabaseTest
    extends BaseJDBCTestCase {

    private static final String TABLE = "DECRYPTTABLE";
    private static final String BOOTPW = "Thursday";
    /** Current encryption algorithm, used when re-encrypting during set up. */
    private static String encryptionAlgorithm;

    public DecryptDatabaseTest(String name) {
        super(name);
    }

    /** Makes sure that the database is encrypted. */
    public void setUp()
            throws Exception {
        super.setUp();

        // Connect.
        try {
            connect(false, BOOTPW, null).close();
        } catch (SQLException sqle) {
            assertSQLState("Did you change the boot password?", "XJ004", sqle);
            // Create the database and save the encryption algorithm.
            getConnection();
            saveEncryptionAlgorithm();
        }

        // Make sure the database is (still) encrypted.
        TestConfiguration tc = getTestConfiguration();
        tc.shutdownDatabase();
        try {
            connect(false, null, null);
            tc.shutdownDatabase();
            // Database has been decrypted. Encrypt it again.
            println("encrypting database (" + encryptionAlgorithm + ")");
            connect(false, BOOTPW, "dataEncryption=true;encryptionAlgorithm=" +
                    encryptionAlgorithm);
            tc.shutdownDatabase();
            connect(false, null, null);
            fail("database encryption failed");
        } catch (SQLException sqle) {
            assertSQLState("XBM06", sqle);
        }
    }

    /** Stashes away the encryption algorithm such that we can re-encrypt. */
    private void saveEncryptionAlgorithm()
            throws SQLException {
        Statement stmt = createStatement();
        ResultSet rs = stmt.executeQuery("values syscs_util." +
                "syscs_get_database_property('encryptionAlgorithm')");
        if (rs.next()) {
            String alg = rs.getString(1);
            if (alg != null && !alg.equals(encryptionAlgorithm)) {
                encryptionAlgorithm = alg;
            }
            assertFalse(rs.next());
        }
        rs.close();
        stmt.close();
    }

    /**
     * Tests that the encrypted database cannot be decrypted without the
     * boot password.
     */
    public void testDecryptDatabaseNegative()
            throws SQLException {
        // Boot with the wrong password, connection attempt should fail.
        try {
            connect(false, "verywrongpw", null);
            fail("connection succeeded with wrong password");
        } catch (SQLException sqle) {
            assertSQLState("XJ040", sqle);
        }
        // Boot without password, connection attempt should fail.
        try {
            connect(false, null, null);
            fail("connection succeeded without password");
        } catch (SQLException sqle) {
            assertSQLState("XBM06", sqle);
        }

        // Boot with the wrong password, connection attempt should fail.
        try {
            connect(true, "verywrongpw", null);
            fail("decryption succeeded with wrong password");
        } catch (SQLException sqle) {
            assertSQLState("XJ040", sqle);
        }
        // Boot without password, connection attempt should fail.
        try {
            connect(true, null, null);
            fail("decryption succeeded without password");
        } catch (SQLException sqle) {
            assertSQLState("XBM06", sqle);
        }
        try {
            connect(true, null, null);
        } catch (SQLException sqle) {
            assertSQLState("XBM06", sqle);
        }
        
        // Bad setting for decryptDatabase
        try {
            connect( false, BOOTPW, "decryptDatabase=fred" );
            fail( "bad decryptDatabase setting not detected" );
        } catch (SQLException sqle) {
            assertSQLState("XJ05B", sqle);
        }

        connect(false, BOOTPW, null);
    }

    /**
     * Tests that the encrypted database can be decrypted.
     * <p>
     * This is tested by first populating an encrypted database, and then
     * accessing the data in the end by booting the database without a boot
     * password. We verify that connection attempts with incorrect or missing
     * boot passwords before decryption fail.
     */
    public void testDecryptDatabase()
            throws SQLException {
        populateDatabase(true, 1000);
        getTestConfiguration().shutdownDatabase();

        // Connect to decrypt the database.
        Connection con = connect(true, BOOTPW, null);
        JDBC.assertNoWarnings(con.getWarnings());
        Statement stmt = con.createStatement();
        JDBC.assertDrainResults(
                stmt.executeQuery("select * from " + TABLE), 1000);
        stmt.close();
        con.close();
        getTestConfiguration().shutdownDatabase();

        // Boot again without boot password to verify that it works.
        con = connect(false, null, null);
        stmt = con.createStatement();
        JDBC.assertDrainResults(
                stmt.executeQuery("select * from " + TABLE), 1000);
        JDBC.assertFullResultSet(
                stmt.executeQuery("select * from " + TABLE +
                    " where id <= 6 order by id ASC"),
                new String[][] {{"1"},{"2"},{"3"},{"4"},{"5"},{"6"}});
        stmt.close();
        con.close();
    }

    /**
     * Tests that trying to decrypt an already booted database doesn't actually
     * decrypt the database.
     * <p>
     * The internal code isn't set up to deal with decryption/encryption while
     * other activities take place concurrently, so crypto operations are only
     * performed when booting a database.
     */
    public void testDecryptOnBootedDatabase()
            throws SQLException {
        getConnection();
        // Connect to decrypt the database. We expect this to fail since the
        // database is already booted. In this case fail means ignored...
        connect(true, BOOTPW, null).close();
        getTestConfiguration().shutdownDatabase();
        try {
            connect(false, null, null);
            fail("decrypted already booted database");
        } catch (SQLException sqle) {
            assertSQLState("XBM06", sqle);
        }
    }

    /**
     * Tests that asking to decrypt an un-encrypted doesn't fail.
     */
    public void testDecryptUnEncryptedDatabase()
            throws SQLException {
        // First decrypt the database.
        Connection con = connect(true, BOOTPW, null);
        JDBC.assertNoWarnings(con.getWarnings());
        con.close();

        // Shut down the database.
        getTestConfiguration().shutdownDatabase();

        // Specify the decrypt attribute again on the decrypted database.
        // We expect that this request is simply ignored.
        con = connect(true, null, null);
        con.close();
    }

    /**
     * Tests that conflicting connection attributes are detected and flagged.
     */
    public void testConflictingConnectionAttributes()
            throws SQLException {
        // Encryption attributes are typically ignored if the database has
        // already been booted.
        try {
            connect(true, BOOTPW, "newBootPassword=MondayMilk");
            fail("connected with conflicting attributes (newBootPassword)");
        } catch (SQLException sqle) {
            assertSQLState("XJ048", sqle);
        }
        try {
            connect(true, BOOTPW, "newEncryptionKey=6162636465666768");
            fail("connected with conflicting attributes (newEncryptionKey)");
        } catch (SQLException sqle) {
            assertSQLState("XJ048", sqle);
        }
        try {
            connect(true, BOOTPW, "createFrom=./nonexistdb");
            fail("connected with conflicting attributes (createFrom)");
        } catch (SQLException sqle) {
            assertSQLState("XJ081", sqle);
        }
        try {
            connect(true, BOOTPW, "restoreFrom=./nonexistdb");
            fail("connected with conflicting attributes (restoreFrom)");
        } catch (SQLException sqle) {
            assertSQLState("XJ081", sqle);
        }
        try {
            connect(true, BOOTPW, "rollForwardRecoveryFrom=./nonexistdb");
            fail("connected with conflicting attrs (rollForwardRecoveryFrom)");
        } catch (SQLException sqle) {
            assertSQLState("XJ081", sqle);
        }
        // Decrypt the database, then specify both encryption and decryption.
        connect(true, BOOTPW, null);
        getTestConfiguration().shutdownDatabase();
        try {
            connect(true, BOOTPW, "dataEncryption=true");
            fail("connected with conflicting attributes (dataEncryption)");
        } catch (SQLException sqle) {
            assertSQLState("XJ048", sqle);
        }
    }

    /**
     * Attempts to connect to the default database with the specified
     * attributes.
     *
     * @param decrypt whether or not to request database decryption
     * @param bootPassword boot password, may be {@code null}
     * @param otherAttrs additional boot attributes
     * @return A connection.
     * @throws SQLException if the connection cannot be established
     */
    private Connection connect(boolean decrypt,
                               String bootPassword,
                               String otherAttrs)
            throws SQLException {
        DataSource ds = JDBCDataSource.getDataSource();
        JDBCDataSource.clearStringBeanProperty(ds, "connectionAttributes");
        StringBuffer attrs = new StringBuffer();
        if (decrypt) {
            attrs.append("decryptDatabase=true").append(';');
        }
        if (bootPassword != null) {
            attrs.append("bootPassword=").append(bootPassword).append(';');
        }
        if (otherAttrs != null) {
            attrs.append(otherAttrs).append(';');
        }
        if (attrs.length() > 0) {
            JDBCDataSource.setBeanProperty(
                    ds, "connectionAttributes", attrs.toString());
        }
        println("connectionAttributes: " +
                (attrs.length() == 0 ? "<empty>" : attrs.toString()));
        return ds.getConnection();
    }

    /**
     * Populates the database (simple one-column table).
     *
     * @param init if {@code true} the table will be created or reset (the
     *      identity column will also be reset)
     * @param rows number of rows to insert
     */
    private void populateDatabase(boolean init, int rows)
            throws SQLException {
        setAutoCommit(false);
        DatabaseMetaData meta = getConnection().getMetaData();
        ResultSet rs = meta.getTables(null, null, TABLE, null);
        boolean hasTable = rs.next();
        assertFalse(rs.next());
        rs.close();
        if (init) {
            Statement stmt = createStatement();
            if (hasTable) {
                println("deleting rows from table " + TABLE);
                stmt.executeUpdate("delete from " + TABLE);
                println("resetting identity column");
                stmt.executeUpdate("ALTER TABLE " + TABLE + " ALTER COLUMN " +
                        "id RESTART WITH 1");
            } else {
                println("creating table " + TABLE);
                stmt.executeUpdate("create table " + TABLE + " (" +
                        "id int generated always as identity)");
            }
        }
        println("populating database");
        PreparedStatement ps = prepareStatement(
                "insert into " + TABLE + " values (DEFAULT)");
        for (int i=0; i < rows; i++) {
            ps.executeUpdate();
        }
        commit();
        setAutoCommit(true);
    }

    public static Test suite() {
        TestSuite suite = new TestSuite("DecryptDatabaseTest suite");
        suite.addTest(wrapTest());
        suite.addTest(wrapTest("AES/OFB/NoPadding"));
        return suite;
    }

    /** Wraps the default set of tests in the default encryption setup. */
    private static Test wrapTest() {
        return Decorator.encryptedDatabaseBpw(
                          TestConfiguration.embeddedSuite(
                              DecryptDatabaseTest.class),
                          BOOTPW);
    }

    /**
     * Wraps the default set of tests in the specified encryption setup.
     *
     * @param encryptionMethod encryption specification, for instance
     *      "AES/OFB/NoPadding"
     */
    private static Test wrapTest(String encryptionMethod) {
        return Decorator.encryptedDatabaseBpw(
                          TestConfiguration.embeddedSuite(
                              DecryptDatabaseTest.class),
                          encryptionMethod,
                          BOOTPW);
    }
}
