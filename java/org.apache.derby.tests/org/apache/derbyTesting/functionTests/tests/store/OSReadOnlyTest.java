/* 

   Derby - Class org.apache.derbyTesting.functionTests.tests.store.OSReadOnlyTest

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
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import javax.sql.DataSource;
import junit.framework.Test;
import org.apache.derbyTesting.functionTests.util.PrivilegedFileOpsForTests;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.JDBCDataSource;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Simulates running Derby on a read-only media, and makes sure Derby gives a
 * reasonable error message when the user tries to insert data into the
 * read-only database.
 */
public class OSReadOnlyTest extends BaseJDBCTestCase{

    public OSReadOnlyTest(String name) {
        super(name);
    }
    
    private static Test newCleanDatabase(BaseTestSuite s) {
        return new CleanDatabaseTestSetup(s) {
        /**
         * Creates the database objects used in the test cases.
         *
         * @throws SQLException
         */
            /**
             * Creates the tables used in the test cases.
             * @exception SQLException if a database error occurs
             */
            protected void decorateSQL(Statement stmt) throws SQLException
            {
                getConnection();

                // create a table with some data
                stmt.executeUpdate(
                    "CREATE TABLE foo (a int, b char(100))");
                stmt.execute("insert into foo values (1, 'hello world')");
                stmt.execute("insert into foo values (2, 'happy world')");
                stmt.execute("insert into foo values (3, 'sad world')");
                stmt.execute("insert into foo values (4, 'crazy world')");
                for (int i=0 ; i<7 ; i++)
                    stmt.execute("insert into foo select * from foo");
                stmt.execute("create index fooi on foo(a, b)");
            }
        };
    }

    protected static Test baseSuite(String name) 
    {
        BaseTestSuite readonly = new BaseTestSuite("OSReadOnly");
        BaseTestSuite suite = new BaseTestSuite(name);
        readonly.addTestSuite(OSReadOnlyTest.class);
        suite.addTest(TestConfiguration.singleUseDatabaseDecorator(newCleanDatabase(readonly)));
        
        return suite;
    }

    public static Test suite() 
    {
        BaseTestSuite suite = new BaseTestSuite("OSReadOnlyTest");
        suite.addTest(baseSuite("OSReadOnlyTest:embedded"));
        suite.addTest(TestConfiguration
            .clientServerDecorator(baseSuite("OSReadOnlyTest:client")));
        return suite;
    }
    
    /**
     * Test that if we make the files comprising the database read-only
     * on OS level, the database reacts as if it's in 'ReadOnly' mode
     */
    public void testOSReadOnly() throws Exception {
        if (!supportsSetReadOnly()) {
            // If we can modify files after File.setReadOnly() has been
            // called on them, the test database will not actually be
            // read-only, and the test will fail. Skip the test in that
            // case.
            alarm("Read-only files can be modified. Skipping OSReadOnlyTest.");
            return;
        }

        // start with some simple checks
        setAutoCommit(false);
        Statement stmt = createStatement();
        JDBC.assertFullResultSet(stmt.executeQuery(
            "select count(*) from foo"), new String[][] {{"512"}});
        stmt.executeUpdate("delete from foo where a = 1");
        JDBC.assertFullResultSet(stmt.executeQuery(
            "select count(*) from foo"), new String[][] {{"384"}});
        rollback();
        JDBC.assertFullResultSet(stmt.executeQuery(
            "select count(*) from foo"), new String[][] {{"512"}});
        stmt.executeUpdate("insert into foo select * from foo where a = 1");
        JDBC.assertFullResultSet(stmt.executeQuery(
            "select count(*) from foo"), new String[][] {{"640"}});
        commit();
        stmt.executeUpdate("delete from foo where a = 1");
        JDBC.assertFullResultSet(stmt.executeQuery(
            "select count(*) from foo"), new String[][] {{"384"}});
        rollback();
        JDBC.assertFullResultSet(stmt.executeQuery(
            "select count(*) from foo"), new String[][] {{"640"}});
        setAutoCommit(false);
        
        TestConfiguration.getCurrent().shutdownDatabase();
        
        // so far, we were just playing. Now for the test.
        String phDbName = getPhysicalDbName();
        // copy the database to one called 'readOnly'
        moveDatabaseOnOS(phDbName, "readOnly");
        // change filePermissions on readOnly, to readonly.
        changeFilePermissions("readOnly");
        createDummyLockFile("readOnly");
        
        DataSource ds = JDBCDataSource.getDataSource();
        JDBCDataSource.setBeanProperty(ds, 
            "databaseName", "singleUse/readOnly");
        assertReadDB(ds);
        assertExpectedInsertBehaviour(ds, false, 10, "will fail");
        shutdownDB(ds);
        
        // copy the database to one called 'readWrite' 
        // this will have the default read/write permissions upon
        // copying
        moveDatabaseOnOS("readOnly", "readWrite");
        ds = JDBCDataSource.getDataSource();
        JDBCDataSource.setBeanProperty(ds, "databaseName", "singleUse/readWrite");
        assertReadDB(ds);
        assertExpectedInsertBehaviour(ds, true, 20, "will go in");
        shutdownDB(ds);
        
        // do it again...
        moveDatabaseOnOS("readWrite", "readOnly2");
        // change filePermissions on readOnly, to readonly.
        changeFilePermissions("readOnly2");
        createDummyLockFile("readOnly2");
        
        ds = JDBCDataSource.getDataSource();
        JDBCDataSource.setBeanProperty(ds, 
            "databaseName", "singleUse/readOnly2");
        assertReadDB(ds);
        assertExpectedInsertBehaviour(ds, false, 30, "will also fail");
        shutdownDB(ds);
        
        // testharness will try to remove the original db; put it back
        moveDatabaseOnOS("readOnly2", phDbName);
    }

    /**
     * Check if {@code File.setReadOnly()} has any effect in this environment.
     * For example, if the test runs as a privileged user, it may be able
     * to modify a file even if it has been made read-only. If so, it doesn't
     * make any sense to run this test.
     *
     * @return {@code true} if {@code File.setReadOnly()} prevents file
     *   modifications; otherwise, {@code false}
     * @throws IOException if an unexpected error happens
     */
    private boolean supportsSetReadOnly() throws IOException {
        File tmp = PrivilegedFileOpsForTests.createTempFile(
                "tmp", null, currentDirectory());
        PrivilegedFileOpsForTests.setReadOnly(tmp);
        FileOutputStream fs = null;
        try {
            fs = PrivilegedFileOpsForTests.getFileOutputStream(tmp);
            // Was able to open the file in read-write mode, so it's not
            // properly read-only.
            return false;
        } catch (FileNotFoundException fnf) {
            // Failed to open the file in read-write mode, so it seems like
            // it's read-only.
            return true;
        } finally {
            if (fs != null) {
                fs.close();
            }
            PrivilegedFileOpsForTests.delete(tmp);
        }
    }

    /*
     * figure out the physical database name, we want to manipulate
     * the actual files on the OS.
     */
    private String getPhysicalDbName() {
        String pdbName =TestConfiguration.getCurrent().getJDBCUrl();
        return pdbName.substring(pdbName.lastIndexOf("oneuse"));
    }
    
    private void shutdownDB(DataSource ds) throws SQLException {
        JDBCDataSource.setBeanProperty(
            ds, "ConnectionAttributes", "shutdown=true");
        try {
            ds.getConnection();
            fail("expected an sqlexception 08006");
        } catch (SQLException sqle) {
            assertSQLState("08006", sqle);
        }        
    }
    
    private void assertReadDB(DataSource ds) throws SQLException {
        Connection con = ds.getConnection();
        Statement stmt2 = con.createStatement();
        JDBC.assertFullResultSet(
            stmt2.executeQuery("select count(*) from foo where a=1"),
            new String [][] {{"256"}});
        JDBC.assertFullResultSet(
            stmt2.executeQuery("select count(*) from foo where a=2"),
            new String [][] {{"128"}});
        JDBC.assertFullResultSet(
            stmt2.executeQuery("select count(*) from foo where a=1 and b='hello world'"),
            new String [][] {{"256"}});
        stmt2.close();
        con.close();
    }
    
    private void assertExpectedInsertBehaviour(
            DataSource ds, boolean expectedSuccess, 
            int insertIntValue, String insertStringValue) 
    throws SQLException {
        Connection con = ds.getConnection();
        Statement stmt = con.createStatement();
        if (expectedSuccess)
        {
            stmt.executeUpdate("insert into foo values (" +
                insertIntValue + ", '" + insertStringValue + "')");
            assertTrue(stmt.getUpdateCount() == 1);
            JDBC.assertFullResultSet(
                stmt.executeQuery("select count(*) from foo where a=" +
                    insertIntValue), new String [][] {{"1"}});
        }
        else {
            try {
                stmt.executeUpdate("insert into foo values (" +
                    insertIntValue + ", '" + insertStringValue + "')");
                fail("expected an error indicating the db is readonly");
            } catch (SQLException sqle) {
                if (!(sqle.getSQLState().equals("25502") || 
                        // on iseries / OS400 machines, when file/os 
                        // permissions are off, we may get error 40XD1 instead
                        sqle.getSQLState().equals("40XD1")))
                    fail("unexpected sqlstate; expected 25502 or 40XD1, got: " + sqle.getSQLState());
            }
        }
        stmt.close();
        con.close();
    }

    /**
     * Moves the database from one location to another location.
     *
     * @param fromwhere source directory
     * @param todir destination directory
     * @throws IOException if the copy fails
     */
    private void moveDatabaseOnOS(String fromwhere, String todir)
            throws IOException {
        File from_dir = constructDbPath(fromwhere);
        File to_dir = constructDbPath(todir);

        PrivilegedFileOpsForTests.copy(from_dir, to_dir);
        assertDirectoryDeleted(from_dir);
    }

    /**
     * Creates a dummy database lock file if one doesn't exist, and sets the
     * lock file to read-only.
     * <p>
     * This method is a work-around for the problem that Java cannot make a file
     * writable before Java 6.
     *
     * @param dbDir the database directory where the lock file belongs
     */
    private void createDummyLockFile(String dbDir) {
        final File f = new File(constructDbPath(dbDir), "db.lck");
        AccessController.doPrivileged(new PrivilegedAction<Void>() {

            public Void run() {
                if (!f.exists()) {
                    try {
                        FileOutputStream fos = new FileOutputStream(f);
                        // Just write a dummy byte.
                        fos.write(12);
                        fos.close();
                    } catch (IOException fnfe) {
                        // Ignore
                    }
                }
                f.setReadOnly();
                return null;
            }
        });
    }

    public void changeFilePermissions(String dir) {
        File dir_to_change = constructDbPath(dir);
        assertTrue("Failed to change files in " + dir_to_change + " to ReadOnly",
            changeDirectoryToReadOnly(dir_to_change));
    }

    /**
     * Constructs the path to the database base directory.
     *
     * @param relDbDirName the database name (relative)
     * @return The path to the database.
     */
    private File constructDbPath(String relDbDirName) {
        // Example: "readOnly" -> "<user.dir>/system/singleUse/readOnly"
        File f = new File(getSystemProperty("user.dir"), "system");
        f = new File(f, "singleUse");
        return new File(f, relDbDirName);
    }

    /**
     * Change all of the files in a directory and its subdirectories
     * to read only.
     * @param directory the directory File handle to start recursing from.
     * @return <code>true</code> for success, <code>false</code> otherwise
     */
    public static boolean changeDirectoryToReadOnly( File directory )
    {
        if( null == directory )
            return false;
        final File sdirectory = directory;

        return AccessController.doPrivileged(
            new java.security.PrivilegedAction<Boolean>() {
                public Boolean run() {
                    // set fail to true to start with; unless it works, we
                    // want to specifically set the value.
                    boolean success = true;
                    if( !sdirectory.isDirectory() )
                        success = false;
                    String[] list = sdirectory.list();
                    // Some JVMs return null for File.list() when the directory is empty
                    if( list != null )
                    {
                        for( int i = 0; i < list.length; i++ )
                        {
                            File entry = new File( sdirectory, list[i] );
                            if( entry.isDirectory() )
                            {
                                if( !changeDirectoryToReadOnly(entry) )
                                    success = false;
                            }
                            else {
                                if( !entry.setReadOnly() )
                                    success = false;
                            }
                        }
                    }
                    // Before Java 6 we cannot make the directory writable
                    // again, which means we cannot delete the directory and
                    // its content...
                    //success &= sdirectory.setReadOnly();
                    return success;
                }
            });        
    }
}
