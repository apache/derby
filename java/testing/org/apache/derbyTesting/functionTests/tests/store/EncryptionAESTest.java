/*
 *
 * Derby - Class org.apache.derbyTesting.functionTests.tests.store.EncryptionAESTest
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
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.SQLException;
import java.sql.SQLWarning;

import javax.sql.DataSource;


import junit.framework.Test;
import junit.framework.TestSuite;
import org.apache.derbyTesting.functionTests.util.PrivilegedFileOpsForTests;

import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.JDBCDataSource;
import org.apache.derbyTesting.junit.SupportFilesSetup;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * <p>
 * Tests various connection sequences to further test AES encrypted databases.
 * 
 * Top level testcases grp.
 * <ol>
 * <li>Case 1.x	different feedback modes (valid - CBC,ECB,OFB,unsupported - ABC)
            2 cases for each - creating db and recovery mode
 * <li>Case 2.x	padding ( unsupported padding )
 * <li>Case 3.x	key lengths with bootpassword
       case of 128 bits, 192 bits and 256 bits and unsupported 512 bits
       mismatch keylengths (case of one keylength during creation and another 
       during connecting)
 * <li>Case 4.x	case of changing boot password 
 *     ( covered by old harness test - store/encryptDatabase.sql)
 *     Also see store/access.sql for other cases run with AES encryption
 * <li>Case 5.x	give external encryptionKey instead of bootpassword
 *     Not converted from original old harness test (aes.sql), for also 
 *     covered in junit test EncryptionKeyAESTest.
 * <p>
 */
//@NotThreadSafe
public class EncryptionAESTest
extends BaseJDBCTestCase {

    /** Table holding the test data. */
    private static final String TABLE = "encryptionkeytestdata";
    /** Test data inserted into database and used for verification. */
    private static final int[] DATA = {9,4,2,34,6543,3,123,434,5436,-123,0,123};

    /**
     * Variable to hold the various connections.
     * No guarantee is made about the state of this connection, but it is
     * closed at tear-down.
     */
//    private Connection con = null;

    /**
     * Configures a new setup by specifying the encryption properties.
     */
    public EncryptionAESTest(String name) {
        super(name);
    }

    /**
     * Clean up the connection maintained by this test.
     */
/*    protected void tearDown()
    throws java.lang.Exception {
        if (con != null && !con.isClosed()) {
            con.rollback();
            con.close();
            con = null;
        }
        super.tearDown();
    }
*/
    
    public static Test suite() {
        TestSuite suite = new TestSuite("Encryption AES suite");
        // we're using SupportFilesSetup so the created databases will get 
        // removed. Databases get created in subdirectory EXTINOUT.
        // However, this only happens after the test is finished, so we
        // can't run both embedded and networkserver for the database
        // wouldn't get created with networkserver.
        suite.addTest(
            TestConfiguration.embeddedSuite(EncryptionAESTest.class));
        return new SupportFilesSetup(suite);
    }

    /**
     * Case 1.x
     * Test connection attempts for 5 different feedback modes:
     * valid/supported: CBC(case 1.1), ECB(case 1.2), OFB(case 1.3), CFB(case 1.4)
     * unsupported:     ABC(case 1.5)
     */
    public void testFeedbackModes() throws SQLException {
        String[] feedbackModes = {"CBC", "ECB", "OFB", "CFB", "ABC"};
        for (int i=0 ; i<feedbackModes.length ; i++)
        {
            runTestFeedbackModes(feedbackModes[i]);
        }
    }

    /**
     * test connection attempts for a passed in feedback mode:
     * Connect-and-Create(test case 1.*.1, shutdown, reconnect(1.*.2).
     */
    protected void runTestFeedbackModes(String feedbackMode)
    throws SQLException {
        String dbName = "encryptedDB_Feedback" + feedbackMode;
        // Create database.
        String encryptionAlgorithm = "AES/" + feedbackMode + "/NoPadding";
        String[] bootPassword = {"bootPassword=Thursday"};
        if (feedbackMode=="ABC")
        {
            // expect unsupported feedbackMode error
            assertNoDBCreated("XBCXI", dbName, encryptionAlgorithm, bootPassword);
            return;
        }
        Connection con = createAndPopulateDB(dbName, encryptionAlgorithm, bootPassword );
        validateDBContents(con);
        // Shutdown the database.
        con.close();
        shutdown(dbName);
        // Reconnect using correct key.
        con = getConnection(dbName, encryptionAlgorithm, bootPassword);
        validateDBContents(con);
        con.close();
        // Shutdown the database.
        shutdown(dbName);
    }
    
    /**
     * Case 2.1 - Unsupported padding mode
     */
    public void testUnsupportedPadding() throws SQLException {
        assertNoDBCreated("XBCXB", "badPadDB", "AES/ECB/PKCS5Padding", 
            new String[] {"bootPassword=Thursday"});
    }
    
    /**
     * Case 3.x
     * Test connection attempts for 4 different keyLength values:
     * valid/supported: 128(case 3.1), 192(case 3.2), 256(case 3.3)
     * Create-and-connect is test case 3.*.1, reconnection 3.*.2.
     * unsupported:     512(case 3.5)
     * Also test creating the database with one length, then attempt
     * to reconnect with another encryptionKeyLength value (case 3.4.)
     * Connections with encryptionKeyLength 192 or 256 require an unrestricted
     * encryption policy, which may not be available on all machines, so
     * we need to handle that situation.
     */
    public void testEncryptionKeyLengths() throws SQLException {
        String[] encryptionKeyLengths = {"128", "192", "256"};
        for (int i=0 ; i<encryptionKeyLengths.length ; i++)
        {
            runTestEncryptionKeyLengths(encryptionKeyLengths[i]);
        }
        // case 3.5 - bad key length
        assertNoDBCreated("XJ001", "badKeyLengthDB", "AES/ECB/NoPadding", 
            new String[] {"encryptionKeyLength=512", "bootPassword=Thursday"});
    }

    /**
     * test connection attempts for a passed in feedback mode:
     * does most of the work for fixture testEncryptionKeyLength
     * Connect-and-Create(test case 1.*.1, shutdown, reconnect(1.*.2).
     */
    protected void runTestEncryptionKeyLengths(
            String encryptionKeyLength)
    throws SQLException {
        String dbName = "encrKeyLength" + encryptionKeyLength + "DB";
        // Create database.
        String encryptionAlgorithm = "AES/CBC/NoPadding";
        String[] attributes = 
            {("encryptionKeyLength=" + encryptionKeyLength),
             "bootPassword=Thursday"};
        Connection con = createAndPopulateDB(dbName, encryptionAlgorithm, attributes );
        // If we didn't get a connection and the test did not stop because 
        // of a failure, the policy jars in the jvm must be restrictive.
        // Pop a message to the console and only test encryptionKeyLength 128.
        if (con == null)
        {
            if (TestConfiguration.getCurrent().doTrace())
                System.out.println("no unrestricted policy jars; cannot test AES " +
                    "encryption with encryptionKeyLengths 192 nor 256");
            return;
        }
        validateDBContents(con);
        // Shutdown the database.
        con.close();
        shutdown(dbName);
        // Reconnect using correct key length.
        con = getConnection(dbName, encryptionAlgorithm, attributes);
        validateDBContents(con);
        con.close();
        // just for fun, try this with a DriverManager connection
        con = getDriverManagerConnection(dbName, encryptionAlgorithm, attributes);
        validateDBContents(con);
        con.close();
        shutdown(dbName);
        String[] keyLengths = {"128", "192", "256", "512"};
        for (int i=0 ; i < keyLengths.length ; i++) {
            if (!encryptionKeyLength.equals(keyLengths[i])){
                attributes = new String[] 
                   {("encryptionKeyLength=" + keyLengths[i]),
                     "bootPassword=Thursday"};
                // Reconnect using a valid, but different key length
                runMismatchKeyLength(dbName, encryptionAlgorithm,
                    encryptionKeyLength, attributes);
            }
        }

        // now try re-encrypting with a different boot password
        attributes = new String[]
            {
                ("encryptionKeyLength=" + encryptionKeyLength),
                "bootPassword=Thursday",
                "newBootPassword=Saturday"
            };
        con = getDriverManagerConnection(dbName, encryptionAlgorithm, attributes);
        validateDBContents(con);
        con.close();
        shutdown(dbName);

        // reconnect to make sure we don't have another variant of DERBY-3710
        attributes = new String[]
            {
                ("encryptionKeyLength=" + encryptionKeyLength),
                "bootPassword=Saturday"
            };
        con = getDriverManagerConnection(dbName, encryptionAlgorithm, attributes);
        validateDBContents(con);
        con.close();
        shutdown(dbName);
    }

    /**
     * attempt to connect and verify the SQLState if it's expected to fail
     * does the last bit of work for fixture testEncryptionKeyLength
     */
    public void runMismatchKeyLength(String dbName, String encryptionAlgorithm,
            String encryptionKeyLength, String[] attributes) throws SQLException {
        Connection con = null;
        // try connecting
        // all combinations work - (if unrestricted policy jars are
        // in place)
        try {
            con = getConnection(dbName, encryptionAlgorithm, attributes );
            validateDBContents(con);
            con.close();
            shutdown(dbName);
        } catch (SQLException e) {
            e.printStackTrace();
            con.close();
            shutdown(dbName);
            assertSQLState("XBM06", e);
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
     * Note that the database name will be prefixed
     *      with the path to the EXTINOUT directory
     * @return A string with the absolute path to the database.
     * @see SupportFilesSetup
     */
    private String obtainDbName(String dbName) {
        File tmp = new File(dbName);
        return PrivilegedFileOpsForTests.getAbsolutePath(
                new File(SupportFilesSetup.EXTINOUT, tmp.getPath()));
    }

    /**
     * Attempt to create a new database and expect a failure.
     * <p>
     * The method expects a failure
     * This is because it is the creation process that is to be tested.
     *
     * @param expectedSQLState SQLState for the expected error
     * @param dbName name of the database attempted to create
     * @param algorithm EncryptionAlgorithm
     * @param otherAttributes array for all other attributes 
     *        (Note: dataEncryption=true is already set in getConnection) 
     * @throws SQLException if the database already exist, or
     *      a general error happens during database interaction
     */
    protected void assertNoDBCreated(String expectedSQLState,
        String dbName, String algorithm, String[] otherAttributes)
    throws SQLException {
        try {
            getConnection(dbName, algorithm, otherAttributes );
            fail ("expected error message re unsupported functionality");
        } catch (SQLException e) {
            assertSQLState(expectedSQLState, e);
        }
    }
    
    /**
     * Create a new database and populate it.
     * <p>
     * The method fails with an exception if the database already exists.
     * This is because it is the creation process that is to be tested.
     *
     * @param dbName name of the database to create
     * @param algorithm EncryptionAlgorithm
     * @param otherAttributes array for all other attributes 
     *        (Note: dataEncryption=true is already set in getConnection) 
     * @return A connection the to the newly created database.
     * @throws SQLException if the database already exist, or
     *      a general error happens during database interaction
     */
    protected Connection createAndPopulateDB(
        String dbName, String algorithm, String[] otherAttributes)
    throws SQLException {
        try {
            Connection con = getConnection(dbName, algorithm, otherAttributes);
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
        catch (SQLException e) {
            // if it fails, it should only be because of non-existing
            // support for unrestricted encryption policy.
            assertSQLState("XJ001", e);
            return null;
        }
    }

    /**
     * Create a new connection to the specified database, using the given
     * connection attributes.
     *
     * @param dbName name of the database
     * @param algorithm EncryptionAlgorithm
     * @param otherAttributes array for all other attributes 
     *        (Note: dataEncryption=true is already set in this method) 
     * @return A connection to the database.
     * @throws SQLException if connection fails
     */
    private Connection getConnection(String dbName,
            String algorithm,
            String[] otherAttributes)
    throws SQLException {
        DataSource ds = JDBCDataSource.getDataSource(obtainDbName(dbName));
        StringBuffer str = new StringBuffer(75);
        // Enable data encryption and mark for creation
        // (will connect to existing db if already exists.)
        JDBCDataSource.setBeanProperty(ds, "CreateDatabase", "create");
        str.append("dataEncryption=true;");
        // Add the encryption algorithm.
        str.append("encryptionAlgorithm=");
        str.append(algorithm);
        str.append(";");
        // Add whatever else is being passed in.
        for (int i=0 ; i < otherAttributes.length ; i++) {
            str.append(otherAttributes[i]);
            str.append(";");
        }
        JDBCDataSource.setBeanProperty(
                ds, "connectionAttributes", str.toString());
        return ds.getConnection();
    }

    // does the same thing as getConnection, but uses DriverManager
    // temp method to see if this worked for encryptionKeyLength
    // test. But no...
    private Connection getDriverManagerConnection(String dbName,
            String algorithm,
            String[] otherAttributes)
    throws SQLException {
        String url = TestConfiguration.getCurrent().getJDBCUrl(obtainDbName(dbName));
        url = url + ";create=true;dataEncryption=true;encryptionAlgorithm=" +
            algorithm + ";";
        for (int i=0 ; i < otherAttributes.length ; i++) {
            url = url + otherAttributes[i] + ";";
        }
        // as we're only using SupportingFilesSetup, not default,
        // we need to explicitly load the driver.
        String driver =
            getTestConfiguration().getJDBCClient().getJDBCDriverName();
        try {
            Class.forName(driver).newInstance();
        } catch (ClassNotFoundException cnfe) {
            fail("\nUnable to load the JDBC driver " + driver);
        } catch (InstantiationException ie) {
            fail("\nUnable to instantiate the JDBC driver " + driver);
        } catch (IllegalAccessException iae) {
            fail("\nNot allowed to access the JDBC driver " + driver);
        }
        Connection conn = DriverManager.getConnection(url);
        return conn;
    }

    /**
     * Shutdown the specified database.
     *
     * @throws SQLException if fails
     * @param databaseName the name of the database
     */
    protected void shutdown(String databaseName)
    throws SQLException {
        DataSource ds = JDBCDataSource.getDataSource(obtainDbName(databaseName));
        JDBCDataSource.shutdownDatabase(ds);
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
        JDBC.assertFullResultSet(rs, new String[][] {
                {"0","9"},
                {"1","4"},
                {"2","2"},
                {"3","34"},
                {"4","6543"},
                {"5","3"},
                {"6","123"},
                {"7","434"},
                {"8","5436"},
                {"9","-123"},
                {"10","0"},
                {"11","123"}});
        //Utilities.showResultSet(rs);
        /* 
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
        */
        rs.close();
        stmt.close();
    }
} // End EncryptionAESTest
