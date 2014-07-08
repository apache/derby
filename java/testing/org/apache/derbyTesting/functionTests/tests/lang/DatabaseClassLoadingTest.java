/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.lang.DatabaseClassLoadingTest

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
   under the License.

 */
package org.apache.derbyTesting.functionTests.tests.lang;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.security.AccessController;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import javax.sql.DataSource;
import junit.framework.Test;
import org.apache.derbyTesting.functionTests.util.PrivilegedFileOpsForTests;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.ClasspathSetup;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.JDBCDataSource;
import org.apache.derbyTesting.junit.LoginTimeoutTestSetup;
import org.apache.derbyTesting.junit.SecurityManagerSetup;
import org.apache.derbyTesting.junit.SupportFilesSetup;

/**
 * Test database class loading, executing routines from the
 * installed jars including accessing resources. Replacing
 * jars and handling signed jars is also tested.
 *
 */
public class DatabaseClassLoadingTest extends BaseJDBCTestCase {
    
    public DatabaseClassLoadingTest(String name)
    {
        super(name);
    }

    /**
     * Run the tests only in embedded since this is testing
     * server side behaviour. Due to DERBY-537 and DERBY-2040
     * most of the tests are run without a security manager.
     * Ordering is important here so the fixtures are added
     * explicitly.
     */
    public static Test suite() throws Exception
    {
        final BaseTestSuite suite =
            new BaseTestSuite("DatabaseClassLoadingTest");
        
        // Need DriverManager to execute the add contact procedure
        // as it uses server side jdbc.
        Test test = suite;
        if (JDBC.vmSupportsJDBC3()) {
            
            String[] orderedTests = {
                "testJarHandling",
                "testWithNoInstalledJars",
                "testWithNoClasspath",
                "testSetClasspath",
                "testAddContact",
                "testGetResource",          
                "testAlterTable",
                "testClassPathRollback",
                "testReplaceJar",      
                "testReplacedClass",
                "testSecondJar",
                "testSignedJar",
                "testCreateDatabaseJar",
                "testHackedJarReplacedClass",
                "testInvalidJar",
                "testRemoveJar",
                "testLoadJavaClassIndirectly",
                "testLoadJavaClassDirectly",
                "testLoadJavaClassDirectly2",
                "testLoadJavaClassDirectly3",
                "testLoadDerbyClassIndirectly",
                "testIndirectLoading",
                "testTableFunctionInJar",
                "testUDAInJar",
                "test_5352",
            };
            
            for (int i = 0; i < orderedTests.length; i++)
            {
                suite.addTest(new DatabaseClassLoadingTest(orderedTests[i]));
            }
       
            suite.addTest(new DatabaseClassLoadingTest("testDatabaseInJar"));

            // DERBY-2162: Only run this test case on platforms that support
            // the URLClassLoader.close() method. Otherwise, we won't be able
            // to delete the jar file afterwards.
            if (ClasspathSetup.supportsClose()) {
                suite.addTest(new ClasspathSetup(
                        new DatabaseClassLoadingTest("testDatabaseInClasspath"),
                        SupportFilesSetup.getReadOnlyURL("dclt.jar")));
            }
           
           // No security manager because the test uses getClass().getClassLoader()
           // in an installed jar to ensure that the class loader for
           // specific classes is correct. This operation is not allowed in general.
           suite.addTest(SecurityManagerSetup.noSecurityManager(
                   new DatabaseClassLoadingTest("testClassLoadOrdering")));

           // Add test cases accessing a classpath database when a login
           // timeout has been specified.
           suite.addTest(loginTimeoutSuite());

           test = new SupportFilesSetup(suite,
                   new String[] {
                   "functionTests/tests/lang/dcl_emc1.jar",
                   "functionTests/tests/lang/dcl_emcaddon.jar",
                   "functionTests/tests/lang/dcl_emc2.jar",
                   "functionTests/tests/lang/dcl_emc2s.jar",
                   "functionTests/tests/lang/dcl_emc2sm.jar",
                   "functionTests/tests/lang/dcl_emc2l.jar",
                   "functionTests/tests/lang/dcl_java.jar",
                   "functionTests/tests/lang/dcl_ot1.jar",
                   "functionTests/tests/lang/dcl_ot2.jar",
                   "functionTests/tests/lang/dcl_ot3.jar",
                   "functionTests/tests/lang/dcl_id.jar",
                   "functionTests/tests/lang/dummy_vti.jar",
                   "functionTests/tests/lang/median_uda.jar",
                   });
           
           }
        
        return new CleanDatabaseTestSetup(test) {
            protected void decorateSQL(Statement s) throws SQLException
            {
                s.executeUpdate("create schema emc");
                s.executeUpdate("create schema \"emcAddOn\"");
                s.executeUpdate("create table emc.contacts " +
                        "(id int primary key, e_mail varchar(30))");
                s.executeUpdate(
                  "create procedure EMC.ADDCONTACT(id INT, e_mail VARCHAR(30)) " +
                  "MODIFIES SQL DATA " +
                  "external name 'org.apache.derbyTesting.databaseclassloader.emc.addContact' " +
                  "language java parameter style java");

                s.executeUpdate(
                  "create function EMC.GETARTICLE(path VARCHAR(60)) " +
                  "RETURNS VARCHAR(256) " +
                  "NO SQL " +
                  "external name 'org.apache.derbyTesting.databaseclassloader.emc.getArticle' " +
                  "language java parameter style java");
                
                // function that gets the signers of the class (loaded from the jar)
                s.executeUpdate("CREATE FUNCTION EMC.GETSIGNERS(" +
                  "CLASS_NAME VARCHAR(256)) RETURNS VARCHAR(60) "+
                  "NO SQL LANGUAGE JAVA PARAMETER STYLE JAVA " +
                  "EXTERNAL NAME 'org.apache.derbyTesting.databaseclassloader.emc.getSigners'");
                
                s.executeUpdate("CREATE FUNCTION \"emcAddOn\".VALIDCONTACT(E_MAIL VARCHAR(30)) "+
                  "RETURNS SMALLINT "+
                  "READS SQL DATA LANGUAGE JAVA PARAMETER STYLE JAVA " +
                  "EXTERNAL NAME 'org.apache.derbyTesting.databaseclassloader.addon.vendor.util.valid'");
                }
        };
    }

    /**
     * Create a test suite that verifies the fix for DERBY-6107. Connection
     * attempts used to fail when trying to access a classpath database that
     * lived in the context class loader, if a login timeout was used and a
     * previous connection attempt had been made from a thread that did not
     * have the database in its context class loader.
     */
    private static Test loginTimeoutSuite() throws Exception {
        BaseTestSuite suite =
            new BaseTestSuite("Class loading with login timeout");

        // First run a test when the database is not in the classpath.
        // Expect the connection attempt to fail.
        suite.addTest(
            new DatabaseClassLoadingTest("testLoginTimeoutNotInClasspath"));

        // Then try again with the database in the classpath. Should succeed.
        // Failed before DERBY-6107.
        //
        // Only add this test case if we can close the URLClassLoader when
        // we're done. Otherwise, we won't be able to delete the jar file
        // afterwards. (DERBY-2162)
        if (ClasspathSetup.supportsClose()) {
            suite.addTest(
                new ClasspathSetup(
                    new DatabaseClassLoadingTest("testLoginTimeoutInClasspath"),
                    SupportFilesSetup.getReadOnlyURL("dclt.jar")));
        }

        // Finally, check that the database cannot be found anymore after
        // it has been removed from the classpath.
        suite.addTest(
            new DatabaseClassLoadingTest("testLoginTimeoutNotInClasspath"));

        // All of this should be done with a login timeout. Set the timeout
        // to a high value, so that the connection attempts don't actually
        // time out.
        return new LoginTimeoutTestSetup(suite, 100);
    }

    /**
     * Test the routines fail before the jars that contain their
     * code have been installed and/or set in the classpath.
     * @throws SQLException
     */
    public void testWithNoInstalledJars() throws SQLException {
        try {
            prepareCall("CALL EMC.ADDCONTACT(?, ?)");
            fail("prepareCall on procedure with path to class");
        } catch (SQLException e) {
            assertSQLState("42X51", e);
        }
        try {
            prepareStatement("VALUES EMC.GETARTICLE(?)");
            fail("prepareCall on function with path to class");
        } catch (SQLException e) {
            assertSQLState("42X51", e);
        }
    }
    
    /**
     * Test the sqlj procedures without setting any database
     * classpath. This allows testing with the security manager
     * without hitting the bugs that exist when the database class path
     * is set with the security manager.
     */
    public void testJarHandling() throws SQLException, MalformedURLException
    {       
        installJar("dcl_emc1.jar", "EMC.MAIL_APP_JHT");
        replaceJar("dcl_emc2.jar", "EMC.MAIL_APP_JHT");
        removeJar("EMC.MAIL_APP_JHT");
    }
    
    /**
     * Install the jar, but don't set the classpath.
     * @throws SQLException
     * @throws MalformedURLException 
     */
    public void testWithNoClasspath() throws SQLException, MalformedURLException
    {       
        installJar("dcl_emc1.jar", "EMC.MAIL_APP");
        testWithNoInstalledJars();
    }
    
    /**
     * Set the classpath to include the MAIL_APP jar.
     * @throws SQLException
     */
    public void testSetClasspath() throws SQLException
    {
        setDBClasspath("EMC.MAIL_APP");
        
        // Test we don't need a re-boot to see the new classes.
        CallableStatement cs = prepareCall("CALL EMC.ADDCONTACT(?, ?)");
        cs.setInt(1, 0);
        cs.setString(2, "now@classpathchange.com");
        cs.executeUpdate();
        cs.close();
        
        derby2035Workaround();
    }
    
    /**
     * Test that a new connection successfully sees the changes.
     * @throws SQLException
     */
    public void testAddContact() throws SQLException
    {
        CallableStatement cs = prepareCall("CALL EMC.ADDCONTACT(?, ?)");
        cs.setInt(1, 1);
        cs.setString(2, "bill@ruletheworld.com");
        cs.executeUpdate();
        
        cs.setInt(1, 2);
        cs.setString(2, "penguin@antartic.com");
        cs.executeUpdate();
        
        cs.close();
        
        Statement s = createStatement();
        ResultSet rs = s.executeQuery(
                "SELECT id, e_mail from EMC.CONTACTS ORDER BY 1");
        
        JDBC.assertFullResultSet(rs,
                new String[][] {
                {"0", "now@classpathchange.com"},
                {"1", "bill@ruletheworld.com"},
                {"2", "penguin@antartic.com"},
                });
        
        s.close();
    }
    
    public void testGetResource() throws SQLException
    {
        getResourceTests(getConnection());
    }
    
    private static void getResourceTests(Connection conn) throws SQLException
    {
        PreparedStatement ps = conn.prepareStatement("VALUES EMC.GETARTICLE(?)");
        
        // Simple path should be prepended with the package name
        // of the class executing the code to find
        // /org/apache/derbyTesting/databaseclassloader/graduate.txt
        ps.setString(1, "graduate.txt");
        JDBC.assertSingleValueResultSet(ps.executeQuery(),
                "The Apache Foundation has released the first version of " +
                "the open-source Derby database, which also gained support " +
                "from Sun Microsystems.");
        

        // absolute path within the jar.
        ps.setString(1, "/article/release.txt");
        JDBC.assertSingleValueResultSet(ps.executeQuery(),
                "The Apache Derby development community is pleased to announce " +
                "its first release after graduating from the Apache Incubator, " +
                "Apache Derby 10.1.1.0.");
        
        
        // Resources that don't exist, returns NULL.
        ps.setString(1, "barney.txt");
        JDBC.assertSingleValueResultSet(ps.executeQuery(), null);
        ps.setString(1, "/article/fred.txt");
        JDBC.assertSingleValueResultSet(ps.executeQuery(), null);
    
        // Accessing the class file is disallowed as well by
        // returning a NULL
        ps.setString(1, "emc.class");
        JDBC.assertSingleValueResultSet(ps.executeQuery(), null);
        ps.setString(1, "/org/apache/derbyTesting/databaseclassloader/emc.class");
        JDBC.assertSingleValueResultSet(ps.executeQuery(), null);
        
        ps.close();
    }
    
    /**
     * Alter the table to add a column, the add contact procedure
     * should still work.
     * @throws SQLException
     */
    public void testAlterTable() throws SQLException
    {
        Statement s = createStatement();
        s.executeUpdate("ALTER TABLE EMC.CONTACTS ADD COLUMN OK SMALLINT");
        JDBC.assertFullResultSet(
                s.executeQuery("SELECT id, e_mail, ok from EMC.CONTACTS ORDER BY 1"),
                new String[][] {
                    {"0", "now@classpathchange.com", null},
                    {"1", "bill@ruletheworld.com", null},
                    {"2", "penguin@antartic.com", null},
                    });
        
        // well written application, INSERT used explicit column names
        // ok defaults to NULL
        CallableStatement cs = prepareCall("CALL EMC.ADDCONTACT(?, ?)");
        cs.setInt(1, 3);
        cs.setString(2, "big@blue.com");
        cs.executeUpdate();
        cs.close();

        JDBC.assertFullResultSet(
                s.executeQuery("SELECT id, e_mail, ok from EMC.CONTACTS ORDER BY 1"),
                new String[][] {
                    {"0", "now@classpathchange.com", null},
                    {"1", "bill@ruletheworld.com", null},
                    {"2", "penguin@antartic.com", null},
                    {"3", "big@blue.com", null},
                    });
      
        s.close();
    }
    
    /**
     * check the roll back of class loading.
     * install a new jar in a transaction, see
     * that the new class is used and then rollback
     * the old class should be used after the rollback.
     * @throws SQLException
     * @throws MalformedURLException 
     */
    public void testClassPathRollback() throws SQLException, MalformedURLException
    {        
        getConnection().setAutoCommit(false);
        replaceJar("dcl_emc2.jar", "EMC.MAIL_APP");

        
        // This version checks the e-mail address.
        CallableStatement cs = prepareCall("CALL EMC.ADDCONTACT(?, ?)");
        cs.setInt(1, 99);
        cs.setString(2, "wormspam@soil.com");
        cs.executeUpdate();
        
        Statement s = createStatement();
        
        JDBC.assertFullResultSet(
                s.executeQuery("SELECT id, e_mail, ok from EMC.CONTACTS WHERE ID = 99"),
                new String[][] {
                    {"99", "wormspam@soil.com", "0"},
                    });
        
        rollback();
        getConnection().setAutoCommit(true);
        
        // execute again but reverted to the version that does not
        // check the email address.
        cs.executeUpdate();
        cs.close();

         JDBC.assertFullResultSet(
                s.executeQuery("SELECT id, e_mail, ok from EMC.CONTACTS WHERE ID = 99"),
                new String[][] {
                    {"99", "wormspam@soil.com", null},
                    });
         
         s.executeUpdate("DELETE FROM EMC.CONTACTS WHERE ID = 99");
         s.close();
    }
    
    /**
     * Replace the jar to later test the prepare from a different
     * connection picks up the new version.
     * @throws SQLException
     * @throws MalformedURLException 
     */
    public void testReplaceJar() throws SQLException, MalformedURLException
    {
        replaceJar("dcl_emc2.jar", "EMC.MAIL_APP");
    }
    
    /**
     * Change of class due to testReplaceJar that
     * changes the application to run checks on the e-mail
     * to ensure it is valid (in this case by seeing if
     *  it simply includes 'spam' in the title).
     * @throws SQLException
     */
    public void testReplacedClass() throws SQLException {
        // This version checks the e-mail address.
        CallableStatement cs = prepareCall("CALL EMC.ADDCONTACT(?, ?)");
        cs.setInt(1, 4);
        cs.setString(2, "spammer@ripoff.com");
        cs.executeUpdate();
        cs.setInt(1, 5);
        cs.setString(2, "open@source.org");
        cs.executeUpdate();
        
        Statement s = createStatement();
        JDBC.assertFullResultSet(
                s.executeQuery("SELECT id, e_mail, ok from EMC.CONTACTS ORDER BY 1"),
                new String[][] {
                    {"0", "now@classpathchange.com", null},
                    {"1", "bill@ruletheworld.com", null},
                    {"2", "penguin@antartic.com", null},
                    {"3", "big@blue.com", null},
                    {"4", "spammer@ripoff.com", "0"},
                    {"5", "open@source.org", "1"},
                    });
      
        s.close();
    }
    
    /**
     * now add another jar in to test two jars and
     * a quoted identifer for the jar names.
     * @throws MalformedURLException 
     */
    public void testSecondJar() throws SQLException, MalformedURLException {
        
        installJar("dcl_emcaddon.jar", "\"emcAddOn\".\"MailAddOn\"");

        setDBClasspath("EMC.MAIL_APP:\"emcAddOn\".\"MailAddOn\"");
        Statement s = createStatement();
        JDBC.assertFullResultSet(
                s.executeQuery("SELECT E_MAIL, \"emcAddOn\".VALIDCONTACT(E_MAIL) FROM EMC.CONTACTS ORDER BY 1"),
                new String[][] {
                    {"big@blue.com", "0"},
                    {"bill@ruletheworld.com", "0"},
                    {"now@classpathchange.com", "0"},
                    {"open@source.org", "1"},
                    {"penguin@antartic.com", "0"},
                    {"spammer@ripoff.com", "0"},
                    });
      
        s.close();
    }
    
    /**
     * Test to see if the jar signatures can be obtained from the jar file.
     * The jar was signed with a self signed certificate
     * <code>
        keytool -delete -alias emccto -keystore emcks -storepass ab987c
        keytool -genkey -dname "cn=EMC CTO, ou=EMC APP, o=Easy Mail Company, c=US" -alias emccto -keypass kpi135 -keystore emcks -storepass ab987c
        keytool -selfcert -alias emccto -keypass kpi135 -validity 36500 -keystore emcks -storepass ab987c
        keytool -keystore emcks -storepass ab987c -list -v
        jarsigner -keystore emcks -storepass ab987c -keypass kpi135 -signedjar dcl_emc2s.jar dcl_emc2.jar emccto
        keytool -delete -alias emccto -keystore emcks -storepass ab987c
        </code>
     * @throws SQLException
     * @throws MalformedURLException 
     */
    public void testSignedJar() throws SQLException, MalformedURLException
    {
        // Statement to get the signers for a class loaded from a jar file
        PreparedStatement ps = prepareStatement("VALUES EMC.GETSIGNERS(?)");
        
        // current jar is unsigned.
        ps.setString(1, "org.apache.derbyTesting.databaseclassloader.emc");      
        JDBC.assertSingleValueResultSet(ps.executeQuery(), null);
        
        // replace with a signed jar
        replaceJar("dcl_emc2s.jar", "EMC.MAIL_APP");
        
        ps.close();
        
        signersTests(getConnection());
        
    }
    
    private static void signersTests(Connection conn) throws SQLException
    {
        PreparedStatement ps = conn.prepareStatement("VALUES EMC.GETSIGNERS(?)");
        ps.setString(1, "org.apache.derbyTesting.databaseclassloader.emc");    
        
        // now class is signed
        JDBC.assertSingleValueResultSet(ps.executeQuery(),
                "CN=EMC CTO, OU=EMC APP, O=Easy Mail Company, C=US");
        
        // verify the other jar is still not signed
        ps.setString(1, "org.apache.derbyTesting.databaseclassloader.addon.vendor.util");
        JDBC.assertSingleValueResultSet(ps.executeQuery(), null);

        ps.close();
    }
    
    /**
     * Replace the signed jar with a hacked jar. emc.class modified to diable
     * valid e-mail address check but using same signatures within jar.
     * Class loader should reject.
     * 
     * rejects it.
     * @throws SQLException
     * @throws MalformedURLException 
     */
    public void testHackedJarReplacedClass() throws SQLException, MalformedURLException {

        replaceJar("dcl_emc2sm.jar", "EMC.MAIL_APP");
        
        try {
            CallableStatement cs = prepareCall("CALL EMC.ADDCONTACT(?, ?)");
            cs.setInt(1, 99);
            cs.setString(2, "spamking@cracker.org");
            cs.executeUpdate();
            cs.close();
            fail("procedure call worked on hacked jar");
        } catch (SQLException e) {
            assertSQLState("Class load should fail due to invalid signature", "42X51", e);
        }
    }
    
    /**
     * replace with a hacked jar file, emc.class modified to 
     be an invalid class (no signing on this jar).
     * @throws MalformedURLException 
     */
    public void testInvalidJar() throws SQLException, MalformedURLException
    {
        replaceJar("dcl_emc2l.jar", "EMC.MAIL_APP");
        
        try {
            CallableStatement cs = prepareCall("CALL EMC.ADDCONTACT(?, ?)");
            cs.setInt(1, 999);
            cs.setString(2, "spamking2@cracker.org");
            cs.executeUpdate();
            cs.close();
            fail("procedure call worked on invalid jar");
        } catch (SQLException e) {
            assertSQLState("Class load should fail due to invalid jar", "42X51", e);

        }        
    }
    
    public void testRemoveJar() throws SQLException
    {
        CallableStatement cs = prepareCall("CALL SQLJ.REMOVE_JAR(?, 0)");
        
        cs.setString(1, "EMC.MAIL_APP");
        
        // fail if jar is on classpath
        try {
            cs.executeUpdate();
            fail("REMOVE_JAR on jar in derby.database.classpath worked");
        } catch (SQLException e) {
            assertSQLState("X0X07", e);
        }
        
        // remove from classpath 
        setDBClasspath("\"emcAddOn\".\"MailAddOn\"");
        testWithNoInstalledJars();
        cs.executeUpdate();      
        testWithNoInstalledJars();
        
        // remove the second jar
        setDBClasspath(null);
        cs.setString(1, "\"emcAddOn\".\"MailAddOn\"");
        cs.executeUpdate();
        
        cs.close();
    }
    
    /**
     * Create a Jar of the current database.
     * @throws Exception 
     *
     */
    public void testCreateDatabaseJar() throws Exception
    {
        CallableStatement cs = prepareCall(
                "CALL SYSCS_UTIL.SYSCS_CHECKPOINT_DATABASE()");
        cs.executeUpdate();
        cs.close();
        
        cs = prepareCall(
                "CALL SYSCS_UTIL.SYSCS_BACKUP_DATABASE_NOWAIT(?)");
        
        final File backupDir = SupportFilesSetup.getReadWrite("dbreadonly");
        
        cs.setString(1, backupDir.getPath());
        cs.executeUpdate();

        cs.close();
        
        final String db = getTestConfiguration().getDefaultDatabaseName();
        AccessController.doPrivileged
        (new java.security.PrivilegedExceptionAction<Void>() {
            public Void run() throws Exception {
                createArchive("dclt.jar", new File(backupDir, db), "dbro");;
              return null;
            }
        });
        
    }
    
    /**
     * Test the jar'ed up database created by testCreateDatabaseJar
     * accessing the database using the jar(path to archive)db form
     * of database name.
     */
    public void testDatabaseInJar() throws SQLException
    {
        File jarFile = SupportFilesSetup.getReadOnly("dclt.jar");
        String dbName = "jar:(" +
                PrivilegedFileOpsForTests.getAbsolutePath(jarFile) + ")dbro";
        
        DataSource ds = JDBCDataSource.getDataSource(dbName);
        
        readOnlyTest(ds);
    }
    
    public void testDatabaseInClasspath() throws SQLException
    {
        String dbName = "classpath:dbro";
        DataSource ds = JDBCDataSource.getDataSource(dbName);
        readOnlyTest(ds);
    }
    
    /**
     * Load a java.sql class indirectly (ie. through a valid class
     * in the installed jar file) from the jar file.
     */
    public void testLoadJavaClassIndirectly() throws SQLException, MalformedURLException
    {
        loadJavaClass(
                "org.apache.derbyTesting.databaseclassloader.cracker.C1.simple",
                "38000");
    }
    
    /**
     * Load a java.sql class directly (ie. through a direct procedure call)
     * from the jar file.
     */    
    public void testLoadJavaClassDirectly() throws SQLException, MalformedURLException
    {
        loadJavaClass("java.sql.J1.simple", "XJ001");
    }
    
    /**
     * Load a java.derby99 class directly (ie. through a direct procedure call)
     * from the jar file. This is to see if additional non-standard java.* packages
     * can be added into the JVM
     */    
    public void testLoadJavaClassDirectly2() throws SQLException, MalformedURLException
    {
        loadJavaClass("java.derby99.J2.simple", "XJ001");
    }
    
    /**
     * Load a javax.derby99 class directly (ie. through a direct procedure call)
     * from the jar file. This is to see if additional non-standard javax.* packages
     * can be added into the JVM. As an implementation note this is blocked
     * by Derby's class loader, not the JVM's security mechanism.
     */    
    public void testLoadJavaClassDirectly3() throws SQLException, MalformedURLException
    {
        loadJavaClass("javax.derby99.J3.simple", "XJ001");
    }
    
    /**
     * Load a org.apache.derby class directly (ie. through a direct procedure call)
     * from the jar file. As an implementation note this is blocked
     * by Derby's class loader, not the JVM's security mechanism.
     */    
    public void testLoadDerbyClassIndirectly() throws SQLException, MalformedURLException
    {
        loadJavaClass(
                "org.apache.derbyTesting.databaseclassloader.cracker.C1.derby",
                "38000");
    }
    
    /**
     * Test loading classes in the java. and javax. namespaces
     * from a jar, it should be disallowed or be ignored. These tests
     * are run as separate fixtures to ensure the failed loading
     * does not affect subsequent attempts to load.
     * @throws MalformedURLException 
     */
    private void loadJavaClass(String method, String expectedSQLState)
        throws SQLException, MalformedURLException
    {
        String jarName = "EMC.MY_JAVA";
        
        installJar("dcl_java.jar", jarName);
        setDBClasspath(jarName);
        
        Statement s = createStatement();
        s.execute("CREATE PROCEDURE C1() LANGUAGE JAVA PARAMETER STYLE JAVA " +
                "NO SQL EXTERNAL NAME " +
                "'" + method + "'");
       
        try {
            s.execute("CALL C1()");
            fail("Call to procedure loading java class from installed jar");
        } catch (SQLException sqle)
        {
            assertSQLState(expectedSQLState, sqle);
        }
        
        s.execute("DROP PROCEDURE C1");
        s.close();
        setDBClasspath(null);
        removeJar(jarName);

    }
    
    /**
     * Run an number of statements against a jar'ed database to
     * ensure it is read-only and that class loading works from
     * jar files embedded in jar'ed up databases.
     */
    private static void readOnlyTest(DataSource ds) throws SQLException
    {
        try {
            Connection conn = ds.getConnection();
            Statement s = conn.createStatement();
            
            JDBC.assertFullResultSet(
                    s.executeQuery("SELECT id, e_mail, ok from EMC.CONTACTS ORDER BY 1"),
                    new String[][] {
                        {"0", "now@classpathchange.com", null},
                        {"1", "bill@ruletheworld.com", null},
                        {"2", "penguin@antartic.com", null},
                        {"3", "big@blue.com", null},
                        {"4", "spammer@ripoff.com", "0"},
                        {"5", "open@source.org", "1"},
                        });
            
            JDBC.assertFullResultSet(
                    s.executeQuery("SELECT id, e_mail, \"emcAddOn\".VALIDCONTACT(e_mail) from EMC.CONTACTS ORDER BY 1"),
                    new String[][] {
                        {"0", "now@classpathchange.com", "0"},
                        {"1", "bill@ruletheworld.com", "0"},
                        {"2", "penguin@antartic.com", "0"},
                        {"3", "big@blue.com", "0"},
                        {"4", "spammer@ripoff.com", "0"},
                        {"5", "open@source.org", "1"},
                        });
                       
            assertStatementError("25502", s,
                    "INSERT INTO EMC.CONTACTS values(3, 'no@is_read_only.gov', NULL)");
            assertStatementError("25502", s,
                    "CALL EMC.ADDCONTACT(3, 'really@is_read_only.gov')");

            getResourceTests(conn);
            
            // DERBY-553: Disabled on Java 5 due to JVM bug
            // http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=6284489
            if (!isJava5()) {
                signersTests(conn);
            }
            
            // ensure that a read-only database automatically gets table locking
            conn.setAutoCommit(false);
            JDBC.assertDrainResults(
                    s.executeQuery("select * from EMC.CONTACTS WITH RR"));
            
            JDBC.assertFullResultSet(
                    s.executeQuery(
                         "select TYPE, MODE, TABLENAME from syscs_diag.lock_table"),
                    new String[][] {
                        {"TABLE", "S", "CONTACTS"},
                        });
 
            s.close();
            conn.rollback();
            conn.setAutoCommit(true);
            conn.close();
        } finally {
            JDBCDataSource.shutdownDatabase(ds);
        }
    }
    
    /**
     * Test ordering of class loading.
     * @throws MalformedURLException 
     */
    public void testClassLoadOrdering() throws SQLException, MalformedURLException
    {
        Statement s = createStatement();
        
        s.executeUpdate("CREATE SCHEMA OT");
        
        // Functions to get the class loader of a specific class.
        // Thre variants that are loaded out of each installed jar
        // file to ensure that loading is delegated from one jar
        // to another correctly.
        // We use the added feature that the toString() of the
        // ClassLoader for installed jars returns the jar name
        // first. The RETURNS VARCHAR(10) trims the string to
        // the correct length for our compare purposes, ie. the
        // length of "OT"."OT{1,2,3}"
        s.execute("create function OT.WHICH_LOADER1(classname VARCHAR(256)) " +
        "RETURNS VARCHAR(10) " +
        "NO SQL " +
        "external name " +
        "'org.apache.derbyTesting.databaseclassloader.ot.OrderTest1.whichLoader' " +
        "language java parameter style java");
        
        s.execute("create function OT.WHICH_LOADER2(classname VARCHAR(256)) " +
                "RETURNS VARCHAR(10) " +
                "NO SQL " +
                "external name " +
                "'org.apache.derbyTesting.databaseclassloader.ot.OrderTest2.whichLoader' " +
                "language java parameter style java");

        s.execute("create function OT.WHICH_LOADER3(classname VARCHAR(256)) " +
                "RETURNS VARCHAR(10) " +
                "NO SQL " +
                "external name " +
                "'org.apache.derbyTesting.databaseclassloader.ot.OrderTest3.whichLoader' " +
                "language java parameter style java");


        installJar("dcl_ot1.jar", "OT.OT1");
        installJar("dcl_ot2.jar", "OT.OT2");
        installJar("dcl_ot3.jar", "OT.OT3");
              
        checkLoading("123");
        checkLoading("132");
        checkLoading("213");
        checkLoading("231");
        checkLoading("321");
        checkLoading("312");

        s.close();
        
    }
    
    /**
     * Run a number of tests to ensure classes are loaded
     * from the correct class loader. The order of loading
     * the entry point classes is set by order. 123 will
     * load the entry point classes in order OrderTest1, OrderTest2,
     * OrderTest3. Since loading these entry point classes and
     * the order of execution will determine the loading order
     * of the other classes, we change the order to ensure
     * that classes are loaded from the correct jar regardless
     * of which class loaded it. Ie. Loading OrderTest2 first, which
     * loads indirectly OrderObject1,2,3 ensures that even though
     * OrderTest2 is loaded from OT2 that the others are loaded
     * from their correct jar.
     * 
     * @param order Order the entry point classes will be loaded.
     * 
     */
    private void checkLoading(String order) throws SQLException
    {
        setDBClasspath("OT.OT1:OT.OT2:OT.OT3");
        
        PreparedStatement ps1 = prepareStatement(
            "VALUES OT.WHICH_LOADER" + order.charAt(0) +"(?)");
        PreparedStatement ps2 = prepareStatement(
                "VALUES OT.WHICH_LOADER" + order.charAt(1) +"(?)");
        PreparedStatement ps3 = prepareStatement(
                "VALUES OT.WHICH_LOADER" + order.charAt(2) +"(?)");
        
        // Tests the classes loaded as a direct entry point for a routine
        checkCorrectLoader("OrderTest1", ps1, ps2, ps3);
        checkCorrectLoader("OrderTest2", ps1, ps2, ps3);
        checkCorrectLoader("OrderTest3", ps1, ps2, ps3);
        
        // Tests the classes loaded directly (Class.forName()) by
        // code in an installed jar file.
        checkCorrectLoader("OrderLoad1", ps1, ps2, ps3);
        checkCorrectLoader("OrderLoad2", ps1, ps2, ps3);
        checkCorrectLoader("OrderLoad3", ps1, ps2, ps3);
        
        // Tests the classes loaded indirectly by
        // code in an installed jar file.
        checkCorrectLoader("OrderObject1", ps1, ps2, ps3);
        checkCorrectLoader("OrderObject2", ps1, ps2, ps3);
        checkCorrectLoader("OrderObject3", ps1, ps2, ps3);
                
        ps1.close();
        ps2.close();
        ps3.close();
        
        // reset the classpath to enforce the classes are loaded again
        // on the next attempt.
        setDBClasspath(null);
    }
    
    private void checkCorrectLoader(String className,
            PreparedStatement ps1,
            PreparedStatement ps2,
            PreparedStatement ps3)
       throws SQLException
    {
        className = "org.apache.derbyTesting.databaseclassloader.ot." + className;
        String expectedLoader = 
            "\"OT\".\"OT" + className.charAt(className.length() -1) + "\"";
        
        ps1.setString(1, className);
        JDBC.assertSingleValueResultSet(ps1.executeQuery(), expectedLoader);
        
        ps2.setString(1, className);
        JDBC.assertSingleValueResultSet(ps2.executeQuery(), expectedLoader);

        ps3.setString(1, className);
        JDBC.assertSingleValueResultSet(ps3.executeQuery(), expectedLoader);
    }
    
    /**
     * Test that loading of Derby's internal classes from
     * an installed jar file is disallowed.
     */
    public void testIndirectLoading() throws SQLException, MalformedURLException
    {
        Statement s = createStatement();
        
        s.executeUpdate("CREATE SCHEMA ID");
 /*       
        s.execute("create function OT.WHICH_LOADER1(classname VARCHAR(256)) " +
        "RETURNS VARCHAR(10) " +
        "NO SQL " +
        "external name " +
        "'org.apache.derbyTesting.databaseclassloader.ot.OrderTest1.whichLoader' " +
        "language java parameter style java");
*/
        installJar("dcl_id.jar", "ID.IDCODE");
        
        setDBClasspath("ID.IDCODE");
        
        // Create a procedure that is a method in an installed jar file
        // that calls the internal static method to set a database property.
        // If a user could do this then they bypass the grant/revoke on
        // the system procedure and instead are able to control the database
        // as they please.
        s.execute("CREATE PROCEDURE ID.SETDB(pkey VARCHAR(256), pvalue VARCHAR(256)) " +
                "NO SQL " +
                "external name " +
                "'org.apache.derbyTesting.databaseclassloader.id.IndirectLoad.setDB' " +
                "language java parameter style java");
        
        PreparedStatement ps = prepareCall("CALL ID.SETDB(?, ?)");

        ps.close();

        
        setDBClasspath(null);

              

        s.close();
        
    }

    /**
     * Test that table functions can be invoked from inside jar files stored in
     * the database.
     */
    public void testTableFunctionInJar() throws SQLException, MalformedURLException
    {
        String jarName = "EMC.DUMMY_VTI";

        installJar("dummy_vti.jar", jarName );

        setDBClasspath( jarName );

        Statement s = createStatement();

        // register a scalar function
        s.executeUpdate
            (
             "create function reciprocal( original double ) returns double\n" +
             "language java\n" +
             "parameter style java\n" +
             "no sql\n" +
             "external name 'DummyVTI.reciprocal'"
             );

        // register the table function
        s.executeUpdate
            (
             "create function dummyVTI()\n" +
             "returns table( tablename varchar( 128 ) )\n" +
             "language java\n" +
             "parameter style DERBY_JDBC_RESULT_SET\n" +
             "reads sql data\n" +
             "external name 'DummyVTI.dummyVTI'\n"
             );

        // register another table function in a class which doesn't exist
        s.executeUpdate
            (
             "create function dummyVTI2()\n" +
             "returns table( tablename varchar( 128 ) )\n" +
             "language java\n" +
             "parameter style DERBY_JDBC_RESULT_SET\n" +
             "reads sql data\n" +
             "external name 'MissingClass.dummyVTI'\n"
             );

        // invoke the scalar function
        JDBC.assertFullResultSet(
                s.executeQuery
                (
                 "values ( reciprocal( 2.0 ) )"
                 ),
                new String[][] {
                    {"0.5"},
                    });

        
        // invoke the table function
        JDBC.assertFullResultSet(
                s.executeQuery
                (
                 "select * from table( dummyVTI() ) s where tablename='SYSTABLES'"
                 ),
                new String[][] {
                    {"SYSTABLES"},
                    });

        // verify that a missing class raises an exception
        try {
            s.executeQuery
                (
                 "select * from table( dummyVTI2() ) s where tablename='SYSTABLES'"
                 );
            fail( "Should have seen a ClassNotFoundException." );
        } catch (SQLException e) {
            assertSQLState("XJ001", e);
        }

        // drop the useless function
        s.executeUpdate( "drop function dummyVTI2\n" );

        setDBClasspath(null);
        
        s.close();
    }
    
    /**
     * Test that user-defined aggregates can be invoked from inside jar files stored in
     * the database.
     */
    public void testUDAInJar() throws SQLException, MalformedURLException
    {
        String jarName = "EMC.MEDIAN_UDA";

        installJar( "median_uda.jar", jarName );

        setDBClasspath( jarName );

        Statement s = createStatement();

        // register the user-defined aggregate
        s.executeUpdate
            ( "create derby aggregate intMedian for int external name 'Median'\n" );

        // register another user-defined aggregate in a class which doesn't exist
        s.executeUpdate
            (
             "create derby aggregate missingAggregate for int external name 'MissingAggregate'\n"
             );

        // create a table with some values
        s.execute( "create table intValues( a int, b int )" );
        s.execute( "insert into intValues values ( 1, 1 ), ( 1, 10 ), ( 1, 100 ), ( 1, 1000 ), ( 2, 5 ), ( 2, 50 ), ( 2, 500 ), ( 2, 5000 )" );

        // invoke the user-defined aggregate
        JDBC.assertFullResultSet
            (
             s.executeQuery
             (
              "select intMedian( b ) from intValues"
              ),
             new String[][]
             {
                 { "100" },
             }
             );
        JDBC.assertFullResultSet
            (
             s.executeQuery
             (
              "select a, intMedian( b ) from intValues group by a"
              ),
             new String[][]
             {
                 { "1", "100" },
                 { "2", "500" },
             }
             );

        // verify that a missing class raises an exception
        try {
            s.executeQuery
                (
                 "select missingAggregate( b ) from intValues"
                 );
            fail( "Should have seen a ClassNotFoundException." );
        } catch (SQLException e) {
            assertSQLState("XJ001", e);
        }

        // drop the useless aggregate
        s.executeUpdate( "drop derby aggregate missingAggregate restrict" );

        setDBClasspath(null);
        
        s.close();
    }
    
    /**
     * Test that restricted table functions can be invoked from inside jar files stored in
     * the database.
     */
    public void test_5352() throws SQLException, MalformedURLException
    {
        String jarName = "EMC.DUMMY_VTI2";

        installJar("dummy_vti.jar", jarName );

        setDBClasspath( jarName );

        Statement s = createStatement();

        // register the table function
        s.executeUpdate
            (
             "create function dummyVTI2( allowsRestriction boolean )\n" +
             "returns table( a int )\n" +
             "language java\n" +
             "parameter style DERBY_JDBC_RESULT_SET\n" +
             "no sql\n" +
             "external name 'DummyVTI2.dummyVTI2'\n"
             );
        
        // invoke the table function
        JDBC.assertFullResultSet
            (
                s.executeQuery
                (
                 "select * from table( dummyVTI2( true ) ) s where a = 1"
                 ),
                new String[][]
                {
                    { "1" }
                }
             );

        // verify that the RestrictedVTI machinery is really invoked
        assertStatementError( "XYZZY", s, "select * from table( dummyVTI2( false ) ) s where a = 1" );
        
        s.executeUpdate( "drop function dummyVTI2\n" );

        setDBClasspath(null);
        
        s.close();
    }

    /**
     * Test that a classpath database is not found when it's not in the
     * classpath and there is a login timeout.
     * @see #loginTimeoutSuite()
     */
    public void testLoginTimeoutNotInClasspath() throws SQLException {
        checkConnectionToClasspathDB(false);
    }

    /**
     * Test that a classpath database is found when it's in the
     * classpath and there is a login timeout.
     * @see #loginTimeoutSuite()
     */
    public void testLoginTimeoutInClasspath() throws SQLException {
        checkConnectionToClasspathDB(true);
    }

    /**
     * Check if it is possible to connect to a classpath database.
     *
     * @param databaseInClasspath if {@code true}, expect that the database
     * can be connected to; otherwise, expect that the database cannot be
     * found.
     */
    private void checkConnectionToClasspathDB(boolean databaseInClasspath) {
        String dbName = "classpath:dbro";
        DataSource ds = JDBCDataSource.getDataSource(dbName);
        try {
            ds.getConnection().close();
            // We should only be able to get a connection if the database is
            // in the classpath.
            assertTrue(
                "Could connect to database when it was not in the classpath",
                databaseInClasspath);
        } catch (SQLException sqle) {
            // If the database is not in the classpath, we expect
            // ERROR XJ004: Database 'classpath:dbro' not found.
            if (databaseInClasspath) {
                fail("Could not connect to the database", sqle);
            } else {
                assertSQLState("XJ004", sqle);
            }
        }

        // If we managed to boot the database, shut it down again.
        if (databaseInClasspath) {
            JDBCDataSource.shutdownDatabase(ds);
        }
    }

    private void installJar(String resource, String jarName) throws SQLException, MalformedURLException
    {        
        URL jar = SupportFilesSetup.getReadOnlyURL(resource);
        
        assertNotNull(resource, jar);
        
        CallableStatement cs = prepareCall("CALL SQLJ.INSTALL_JAR(?, ?, 0)");
        cs.setString(1, jar.toExternalForm());
        cs.setString(2, jarName);
        cs.executeUpdate();
        cs.close();
    }
    
    private void replaceJar(String resource, String jarName) throws SQLException, MalformedURLException
    {        
        URL jar = SupportFilesSetup.getReadOnlyURL(resource);
        assertNotNull(resource, jar);
        
        CallableStatement cs = prepareCall("CALL SQLJ.REPLACE_JAR(?, ?)");
        cs.setString(1, jar.toExternalForm());
        cs.setString(2, jarName);
        cs.executeUpdate();
        cs.close();
    }
    
    private void removeJar(String jarName) throws SQLException
    {
        CallableStatement cs = prepareCall("CALL SQLJ.REMOVE_JAR(?, 0)");       
        cs.setString(1, jarName);       
        cs.executeUpdate();        
        cs.close();
    }
    
    private void setDBClasspath(String cp) throws SQLException
    {
        CallableStatement cs = prepareCall(
          "CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.database.classpath', ?)");

        cs.setString(1, cp);
        cs.executeUpdate();
        cs.close();
    }
    
    
    
    
    private void derby2035Workaround() throws SQLException
    {
        // DERBY-2035 Other connections, e.g. the next fixture
        // do not see the changes related to the new class path
        // until the database is shutdown. However, the connection
        // setting the change does see it!
        // 
        getConnection().close();
        getTestConfiguration().shutdownDatabase(); 
    }
    
    /**
     * jarname - jarname to use path - path to database dbname - database name
     * in archive
     */
    private static void createArchive(String jarName, File dbDir, String dbName)
            throws Exception {

        assertTrue(dbDir.isDirectory());

        // jar file paths in the JDBC URL are relative to the root
        // derby.system.home or user.dir, so need to create the jar there.
        File jarFile = SupportFilesSetup.getReadOnly(jarName);
        
        
        ZipOutputStream zos = new ZipOutputStream(
                new FileOutputStream(jarFile));

        addEntries(zos, dbDir, dbName, dbDir.getPath().length());

        zos.close();
    }

    static void addEntries(ZipOutputStream zos, File dir, String dbName, int old)
            throws Exception {

        String[] list = dir.list();

        for (int i = 0; i < list.length; i++) {

            File f = new File(dir, list[i]);
            if (f.isDirectory()) {
                addEntries(zos, f, dbName, old);
            } else {
                addFile(zos, f, dbName, old);
            }

        }
    }

    private static void addFile(ZipOutputStream zos, File f, String dbName, int old)
            throws IOException {

        String s = f.getPath().replace(File.separatorChar, '/');

        s = s.substring(old);

        s = dbName.concat(s);

        // jar has forward slashes!
        ZipEntry ze = new ZipEntry(s);
        ze.setTime(f.lastModified());

        zos.putNextEntry(ze);

        byte[] buf = new byte[4096];
        BufferedInputStream in = new BufferedInputStream(
                (new FileInputStream(f)));
        while (true) {
            int read = in.read(buf);
            if (read == -1) {
                break;
            }
            zos.write(buf, 0, read);
        }

        in.close();
        zos.closeEntry();
    }
}
