/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.memorydb.BasicInMemoryDbTest

       Licensed to the Apache Software Foundation (ASF) under one
       or more contributor license agreements.  See the NOTICE file
       distributed with this work for additional information
       regarding copyright ownership.  The ASF licenses this file
       to you under the Apache License, Version 2.0 (the
       "License"); you may not use this file except in compliance
       with the License.  You may obtain a copy of the License at

         http://www.apache.org/licenses/LICENSE-2.0

       Unless required by applicable law or agreed to in writing,
       software distributed under the License is distributed on an
       "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
       KIND, either express or implied.  See the License for the
       specific language governing permissions and limitations
       under the License
*/
package org.apache.derbyTesting.functionTests.tests.memorydb;

import java.io.File;
import java.io.IOException;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.derbyTesting.functionTests.util.PrivilegedFileOpsForTests;
import org.apache.derbyTesting.functionTests.util.streams.CharAlphabet;
import org.apache.derbyTesting.functionTests.util.streams.LoopingAlphabetReader;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.SupportFilesSetup;

/**
 * Basic tests of the in-memory db storage back end.
 */
public class BasicInMemoryDbTest
        extends BaseJDBCTestCase {

    /**
     * Helper for dealing with memory databases. For now we use a single
     * instance for all test classes / cases, as the tests are run single
     * threaded.
     */
    private static final MemoryDbManager dbm =
            MemoryDbManager.getSharedInstance();

    public BasicInMemoryDbTest(String name) {
        super(name);
    }

    /**
     * Closes all opened statements and connections that are known, and also
     * deletes all known in-memory databases.
     *
     * @throws Exception if something goes wrong
     */
    public void tearDown()
            throws Exception {
        dbm.cleanUp();
        super.tearDown();
    }

    /**
     * Helper method to make sure the driver is loaded. May be needed when
     * calling DriverManager.getConnection() directly since the driver may
     * not already be loaded (in particular when running from classes so that
     * driver auto-loading doesn't come into play), or it may have been
     * unloaded by an engine shutdown in an earlier test case.
     */
    private void loadDriver() throws SQLException {
        // If we get a default connection through the framework, the driver
        // will be loaded implicitly.
        getConnection();
    }

    /**
     * Tries to connect to a non-existing database with the in-memory protocol,
     * expecting an error saying the database doesn't exist.
     */
    public void testFunctionalityPresent() {
        try {
            dbm.getConnection("nonExistingDb");
        } catch (SQLException e) {
            // Expect a database not found exception.
            assertSQLState("XJ004", e);
        }
    }

    /**
     * Performs a cycle to test that the in-memory db is compatible with the
     * deafult directory protocol.
     * <p>
     * <ol> <li>Create an in-memory db and add a table with a few rows.</li>
     *      <li>Backup to disk.</li>
     *      <li>Boot the database with the directory (default) protocol.</li>
     *      <li>Verify content, add a new row, shutdown.</li>
     *      <li>Use createFrom to restore database from disk into the in-memory
     *          representation.</li>
     *      <li>Verify new content.</li>
     * </ol>
     *
     * @throws IOException if something goes wrong
     * @throws SQLException if something goes wrong
     */
    public void testCreateBackupBootRestore()
            throws IOException, SQLException {
        // 1. Create the database with the in-memory protocol.
        Connection memCon = dbm.createDatabase("newMemDb");
        // Make sure the database is newly created.
        assertNull(memCon.getWarnings());
        Statement stmt = dbm.createStatement(memCon);
        stmt.executeUpdate("create table toverify(" +
                "id int, val1 varchar(10), val2 clob, primary key(id))");
        PreparedStatement ps = dbm.prepareStatement(memCon,
                "insert into toverify values (?,?,?)");
        // The content to insert into the table.
        String[][] firstContent = new String[][] {
            {"1", "one", getString(1000, CharAlphabet.modernLatinLowercase())},
            {"2", "two", getString(10000, CharAlphabet.tamil())},
            {"3", "three", getString(50000, CharAlphabet.cjkSubset())}
        };
        for (int i=0; i < firstContent.length; i++) {
            ps.setString(1, firstContent[i][0]);
            ps.setString(2, firstContent[i][1]);
            ps.setString(3, firstContent[i][2]);
            ps.executeUpdate();
        }
        ResultSet rs = stmt.executeQuery("select * from toverify");
        JDBC.assertFullResultSet(rs, firstContent);
        ps.close();
        stmt.close();

        // 2. Backup the database.
        String dbPath = SupportFilesSetup.getReadWrite("backedUpDb").getPath();
        CallableStatement cs = memCon.prepareCall(
                "CALL SYSCS_UTIL.SYSCS_BACKUP_DATABASE(?)");
        cs.setString(1, dbPath);
        cs.execute();
        memCon.close();

        // 3. Open the database with the default protocol.
        String dbPathBackedUp = PrivilegedFileOpsForTests.getAbsolutePath(
                new File(dbPath, "newMemDb"));
        Connection dirCon = DriverManager.getConnection(
                "jdbc:derby:" + dbPathBackedUp);
        // 4. Verify content, then add one more row.
        stmt = dirCon.createStatement();
        rs = stmt.executeQuery("select * from toverify");
        JDBC.assertFullResultSet(rs, firstContent);
        ps = dirCon.prepareStatement("insert into toverify values (?,?,?)");
        String[] rowToAdd = new String[] {
            "4", "four", getString(32*1024, CharAlphabet.tamil())};
        ps.setString(1, rowToAdd[0]);
        ps.setString(2, rowToAdd[1]);
        ps.setString(3, rowToAdd[2]);
        ps.executeUpdate();
        ps.close();
        dirCon.close();
        // Shutdown.
        try {
            DriverManager.getConnection(
                "jdbc:derby:" + dbPathBackedUp + ";shutdown=true");
        } catch (SQLException sqle) {
            assertSQLState("08006", sqle);
        }

        // 5. Restore modified backup into memory.
        memCon = dbm.getConnection("newMemDb2;createFrom=" + dbPathBackedUp);

        // 6. Verify the new content, where the original in-memory database was
        //    backed up and the directory protocol was used to add one more row
        //    to the backed up database. Now we have restored the on-disk
        //    modified backup, again representing it as an in-memory database.
        stmt = dbm.createStatement(memCon);
        rs = stmt.executeQuery("select * from toverify");
        String[][] secondContent = new String[4][3];
        System.arraycopy(firstContent, 0, secondContent, 0, 3);
        System.arraycopy(rowToAdd, 0, secondContent[3], 0, 3);
        JDBC.assertFullResultSet(rs, secondContent);

        // Delete the second in memory database.
        dbm.dropDatabase("newMemDb2");
    }

    /**
     * Makes sure shutting down an in-memory database works.
     *
     * @throws SQLException if something goes wrong
     */
    public void testShutdown()
            throws SQLException {
        dbm.createDatabase("/tmp/myDB");
        try {
            DriverManager.getConnection(
                    "jdbc:derby:memory:/tmp/myDB;shutdown=true");
            fail("Engine shutdown should have caused exception");
        } catch (SQLException sqle) {
            assertSQLState("08006", sqle);
        }
    }

    /**
     * Makes sure shutting down the Derby engine with an in-memory database
     * already booted works.
     * <p>
     * Related to DERBY-4093
     *
     * @throws SQLException if something goes wrong
     */
    public void testEnginehutdown()
            throws SQLException {
        dbm.createDatabase("/tmp/myDB");
        try {
            DriverManager.getConnection(
                    "jdbc:derby:;shutdown=true");
            fail("Engine shutdown should have caused exception");
        } catch (SQLException sqle) {
            assertSQLState("XJ015", sqle);
        }
        // Another hack, to make sure later tests in this class doesn't fail.
        // Get a connection to the default database to reload the engine.
        loadDriver();
    }

    /**
     * Verify that booting two databases with the same name but with different
     * subsubprotocols doesn't result in two connections to the same database.
     *
     * @throws SQLException if something goes wrong
     */
    public void testBootSameDbDifferentSubSubProtocol()
            throws SQLException {
        final String dbName = "BSDDSSP";
        // Connect to the in-memory database and create a table.
        Connection con1 = dbm.createDatabase(dbName);
        Statement stmt1 = dbm.createStatement(con1);
        stmt1.execute("create table t (text varchar(255))");
        stmt1.execute("insert into t values ('Inserted into in-memory db')");
        // Connect to the on-disk database. The table we created in the
        // in-memory database shouldn't exist in the on-disk database.
        Connection con2 = DriverManager.getConnection(
                "jdbc:derby:" + dbName + ";create=true");
        // Table t should not exist.
        Statement stmt2 = con2.createStatement();
        try {
            stmt2.executeQuery("select * from t");
            fail("Table 't' should not exist");
        } catch (SQLException sqle) {
            assertSQLState("42X05", sqle);
        }
        con2.close();
    }

    /**
     * Test deletion of an in-memory database:
     *  - create database
     *  - delete database
     *  - try to connection to database, should fail
     *  - recreate and delete again
     *
     * @throws SQLException if something else goes wrong
     */
    public void testDelete()
            throws SQLException {
            loadDriver();
            Connection conCreate = DriverManager.getConnection(
                    "jdbc:derby:memory:deleteDbSimple;create=true");
            Statement stmt = dbm.createStatement(conCreate);
            JDBC.assertDrainResults(stmt.executeQuery(
                    "select * from sys.systables"));
            // Delete the database.
            try {
                DriverManager.getConnection(
                    "jdbc:derby:memory:deleteDbSimple;drop=true");
                fail("Dropping database should have raised exception.");
            } catch (SQLException sqle) {
                assertSQLState("08006", sqle);
            }
            // Try to connect to the database again, without creation.
            try {
                DriverManager.getConnection(
                    "jdbc:derby:memory:deleteDbSimple;create=false");
                fail("Database should not exist after deletion.");
            } catch (SQLException sqle) {
                assertSQLState("XJ004", sqle);
            }

            // Recreate and delete again.
            conCreate = DriverManager.getConnection(
                    "jdbc:derby:memory:deleteDbSimple;create=true");
            stmt = dbm.createStatement(conCreate);
            JDBC.assertDrainResults(stmt.executeQuery(
                    "select * from sys.systables"));
            // Delete the database.
            try {
                DriverManager.getConnection(
                    "jdbc:derby:memory:deleteDbSimple;drop=true");
                fail("Dropping database should have raised exception.");
            } catch (SQLException sqle) {
                assertSQLState("08006", sqle);
            }
    }

    /**
     * Deletes the database when in use by a different connection.
     * <p>
     * The expected outcome is that the first connection will be closed when
     * the second one deletes the database.
     *
     * @throws IOException if something goes wrong
     * @throws SQLException if something goes wrong
     */
    public void testDeleteWhenInUse()
            throws IOException, SQLException {
        loadDriver();
        Connection con = DriverManager.getConnection(
                "jdbc:derby:memory:deleteDb;create=true");
        PreparedStatement ps = dbm.prepareStatement(con,
                "select * from sys.systables");
        JDBC.assertDrainResults(ps.executeQuery());
        // Delete the database.
        try {
            DriverManager.getConnection(
                "jdbc:derby:memory:deleteDb;drop=true");
            fail("Dropping database should have raised exception.");
        } catch (SQLException sqle) {
            assertSQLState("08006", sqle);
        }
        // Execute query from first connection again.
        assertTrue(con.isClosed());
        try {
            JDBC.assertDrainResults(ps.executeQuery());
            fail("Database has been dropped, query shouldn't work.");
        } catch (SQLException sqle) {
            // Expect no current connection.
            assertSQLState("08003", sqle);
        }
    }

    /**
     * Shuts down the database when in use by a different connection.
     * <p>
     * The expected outcome is that the first connection will be closed when
     * the second one shuts down the database.
     *
     * @throws IOException if something goes wrong
     * @throws SQLException if something goes wrong
     */
    public void testShutdownWhenInUse()
            throws IOException, SQLException {
        Connection con = dbm.createDatabase("deleteDb");
        PreparedStatement ps = dbm.prepareStatement(con,
                "select * from sys.systables");
        JDBC.assertDrainResults(ps.executeQuery());
        // Delete the database.
        try {
            DriverManager.getConnection(
                "jdbc:derby:memory:deleteDb;shutdown=true");
            fail("Database shutdown should have raised exception.");
        } catch (SQLException sqle) {
            assertSQLState("08006", sqle);
        }
        // Execute query from first connection again.
        assertTrue(con.isClosed());
        try {
            JDBC.assertDrainResults(ps.executeQuery());
            fail("Database has been shut down, query shouldn't work.");
        } catch (SQLException sqle) {
            // Expect no current connection.
            assertSQLState("08003", sqle);
        }
    }

    public static Test suite() {
        // Run only in embedded-mode for now.
        return new SupportFilesSetup(new TestSuite(BasicInMemoryDbTest.class));
    }

    /**
     * Generates a string.
     *
     * @param length length of the string
     * @param alphabet the alphabet to use for the content
     * @return A string.
     * @throws IOException if reading from the source stream fails
     */
    public static String getString(int length, CharAlphabet alphabet)
            throws IOException {
        LoopingAlphabetReader reader =
                new LoopingAlphabetReader(length, alphabet);
        char[] strChar = new char[length];
        int read = 0;
        while (read < length) {
            int readNow = reader.read(strChar, read, length - read);
            if (readNow < 1) {
                fail("Creating string failed, stream returned " + readNow);
            }
            read += readNow;
        }
        return String.copyValueOf(strChar);
    }
}
