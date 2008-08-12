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
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.security.AccessController;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import javax.sql.DataSource;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.JDBCDataSource;
import org.apache.derbyTesting.junit.TestConfiguration;

public class OSReadOnlyTest extends BaseJDBCTestCase{

    public OSReadOnlyTest(String name) {
        super(name);
    }
    
    private static Test newCleanDatabase(TestSuite s) {
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
        TestSuite readonly = new TestSuite("OSReadOnly");
        TestSuite suite = new TestSuite(name);
        readonly.addTestSuite(OSReadOnlyTest.class);
        suite.addTest(TestConfiguration.singleUseDatabaseDecorator(newCleanDatabase(readonly)));
        
        return suite;
    }

    public static Test suite() 
    {
        TestSuite suite = new TestSuite("OSReadOnlyTest");
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
        copyDatabaseOnOS(phDbName, "readOnly");
        // change filePermissions on readOnly, to readonly.
        changeFilePermissions("readOnly");
        
        DataSource ds = JDBCDataSource.getDataSource();
        JDBCDataSource.setBeanProperty(ds, 
            "databaseName", "singleUse/readOnly");
        assertReadDB(ds);
        assertExpectedInsertBehaviour(ds, false, 10, "will fail");
        shutdownDB(ds);
        
        // copy the database to one called 'readWrite' 
        // this will have the default read/write permissions upon
        // copying
        copyDatabaseOnOS("readOnly", "readWrite");
        ds = JDBCDataSource.getDataSource();
        JDBCDataSource.setBeanProperty(ds, "databaseName", "singleUse/readWrite");
        assertReadDB(ds);
        assertExpectedInsertBehaviour(ds, true, 20, "will go in");
        shutdownDB(ds);
        
        // do it again...
        copyDatabaseOnOS("readWrite", "readOnly2");
        // change filePermissions on readOnly, to readonly.
        changeFilePermissions("readOnly2");
        
        ds = JDBCDataSource.getDataSource();
        JDBCDataSource.setBeanProperty(ds, 
            "databaseName", "singleUse/readOnly2");
        assertReadDB(ds);
        assertExpectedInsertBehaviour(ds, false, 30, "will also fail");
        shutdownDB(ds);
        
        // testharness will try to remove the original db; put it back
        copyDatabaseOnOS("readOnly2", phDbName);
    }
    
    /*
     * figure out the physical database name, we want to manipulate
     * the actual files on the OS.
     */
    private String getPhysicalDbName() {
        String pdbName =TestConfiguration.getCurrent().getJDBCUrl();
        if (pdbName != null)
            pdbName=pdbName.substring(pdbName.lastIndexOf("oneuse"),pdbName.length());
        else {
            // with JSR169, we don't *have* a protocol, and so, no url, and so
            // we'll have had a null.
            // But we know the name of the db is something like system/singleUse/oneuse#
            // So, let's see if we can look it up, if everything's been properly
            // cleaned, there should be just 1...
            pdbName = (String) AccessController.doPrivileged(new java.security.PrivilegedAction() {
                String filesep = getSystemProperty("file.separator");
                public Object run() {
                    File dbdir = new File("system" + filesep + "singleUse");
                    String[] list = dbdir.list();
                    // Some JVMs return null for File.list() when the directory is empty
                    if( list != null)
                    {
                        if(list.length > 1)
                        {
                            for( int i = 0; i < list.length; i++ )
                            {
                                if(list[i].indexOf("oneuse")<0)
                                    continue;
                                else
                                {
                                    return list[i];
                                }
                            }
                            // give up trying to be smart, assume it's 0
                            return "oneuse0";
                        }
                        else
                            return list[0];
                    }
                    return null;
                }
            });
            
        }
        return pdbName;
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
                assertSQLState("40XD1", sqle);
            }
        }
        stmt.close();
        con.close();
    }
    
    private void copyDatabaseOnOS(String fromwhere, String todir) {
        String from_dir;
        String to_dir;
        
        String filesep=getSystemProperty("file.separator");

        String testpath=new String( getSystemProperty("user.dir") + filesep +
            "system" + filesep + "singleUse" + filesep);

        from_dir = testpath + fromwhere;
        to_dir = testpath + todir;

        assertTrue("Failed to copy directory from " + from_dir + " to " + to_dir,
            (copyDirectory(from_dir, to_dir)));
        assertTrue("Failed to remove directory: " + from_dir,
            (removeTemporaryDirectory(from_dir)));
    }

    public void changeFilePermissions(String dir) {
        String filesep=getSystemProperty("file.separator");
        String dir_to_change = new String(getSystemProperty("user.dir") + filesep 
            + "system" + filesep + "singleUse" + filesep + dir);
        assertTrue("Failed to change files in " + dir_to_change + " to ReadOnly",
            changeDirectoryToReadOnly(dir_to_change));
    }
    
    /**
     * Change all of the files in a directory and its subdirectories
     * to read only (atleast not writeable, depending on system for execute
     * permission). 
     * @param directory the string representation of the directory
     * to start recursing from.
     * @return <code>true</code> for success, <code>false</code> otherwise
     */
    public static boolean changeDirectoryToReadOnly( String directory )
    {
        return changeDirectoryToReadOnly( new File(directory) );
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

        Boolean b = (Boolean)AccessController.doPrivileged(
            new java.security.PrivilegedAction() {
                public Object run() {
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
                    return new Boolean(success);
                }
            });        
        if (b.booleanValue())
        {
            return true;
        }
        else return false;
    }

    /**
        Remove a directory and all of its contents.

        The results of executing File.delete() on a File object
        that represents a directory seems to be platform
        dependent. This method removes the directory
        and all of its contents.

        @return true if the complete directory was removed, false if it could not be.
        If false is returned then some of the files in the directory may have been removed.
    */
    final private static boolean removeTemporaryDirectory(File directory) {
        //System.out.println("removeDirectory " + directory);

        if (directory == null)
            return false;
        final File sdirectory = directory;

        Boolean b = (Boolean)AccessController.doPrivileged(
            new java.security.PrivilegedAction() {
                public Object run() {
                    if (!sdirectory.exists())
                        return new Boolean(true);
                    if (!sdirectory.isDirectory())
                        return new Boolean(false);
                    String[] list = sdirectory.list();
                    // Some JVMs return null for File.list() when the
                    // directory is empty.
                    if (list != null) {
                        for (int i = 0; i < list.length; i++) {
                            File entry = new File(sdirectory, list[i]);
                            if (entry.isDirectory())
                            {
                                if (!removeTemporaryDirectory(entry))
                                    return new Boolean(false);
                            }
                            else
                            {
                                if (!entry.delete())
                                    return new Boolean(false);
                            }
                        }
                    }
                    return new Boolean(sdirectory.delete());
                }
            });
        if (b.booleanValue())
        {
            return true;
        }
        else return false;
    }

    public static boolean removeTemporaryDirectory(String directory)
    {
        return removeTemporaryDirectory(new File(directory));
    }

    /**
      Copy a directory and all of its contents.
      */
    private static boolean copyDirectory(File from, File to)
    {
        return copyDirectory(from, to, (byte[])null);
    }

    private static boolean copyDirectory(String from, String to)
    {
        return copyDirectory(new File(from), new File(to));
    }

    private static boolean copyDirectory(File from, File to, byte[] buffer)
    {
        if (from == null)
            return false;
        final File sfrom = from;
        final File sto = to;
        if (buffer == null)
            buffer = new byte[4*4096];
        final byte[] sbuffer = buffer;
        
        Boolean b = (Boolean)AccessController.doPrivileged(
            new java.security.PrivilegedAction() {
                public Object run() {
                    if (!sfrom.exists() || !sfrom.isDirectory() || sto.exists() || !sto.mkdirs())  
                    {
                        //can't do basic stuff, returning fail from copydir method
                        return new Boolean(false);
                    }
                    else {
                        //basic stuff succeeded, incl. makind dirs, going on...
                        boolean success=true;
                        String[] list = sfrom.list();

                        // Some JVMs return null for File.list() when the
                        // directory is empty.
                        if (list != null) {
                            for (int i = 0; i < list.length; i++) {
                                File entry = new File(sfrom, list[i]);
                                if (entry.isDirectory())
                                {
                                    success = copyDirectory(entry,new File(sto,list[i]),sbuffer);
                                }
                                else
                                {
                                    success = copyFile(entry,new File(sto,list[i]),sbuffer);
                                }
                            }
                        }
                        return new Boolean(success);
                    }
                }
            });
        if (b.booleanValue())
        {
            return true;
        }
        else return false;
    }       

    public static boolean copyFile(File from, File to)
    {
        return copyFile(from, to, (byte[])null);
    }

    public static boolean copyFile(File from, File to, byte[] buf)
    {
        if (buf == null)
            buf = new byte[4096*4];
        //
        //      System.out.println("Copy file ("+from+","+to+")");
        FileInputStream from_s = null;
        FileOutputStream to_s = null;

        try {
            from_s = new FileInputStream(from);
            to_s = new FileOutputStream(to);

            for (int bytesRead = from_s.read(buf);
                 bytesRead != -1;
                 bytesRead = from_s.read(buf))
                to_s.write(buf,0,bytesRead);

            from_s.close();
            from_s = null;

            to_s.getFD().sync();
            to_s.close();
            to_s = null;
        }
        catch (IOException ioe)
        {
            return false;
        }
        finally
        {
            if (from_s != null)
            {
                try { from_s.close(); }
                catch (IOException ioe) {}
            }
            if (to_s != null)
            {
                try { to_s.close(); }
                catch (IOException ioe) {}
            }
        }
        return true;
    }    
}