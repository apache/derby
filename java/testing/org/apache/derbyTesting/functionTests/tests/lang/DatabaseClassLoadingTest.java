package org.apache.derbyTesting.functionTests.tests.lang;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.security.AccessController;
import java.security.PrivilegedActionException;
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
import junit.framework.TestSuite;

import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.JDBCDataSource;
import org.apache.derbyTesting.junit.SupportFilesSetup;
import org.apache.derbyTesting.junit.SecurityManagerSetup;

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
    public static Test suite()
    {
        final TestSuite suite = new TestSuite("DatabaseClassLoadingTest");
        
        // Need DriverManager to execute the add contact procedure
        // as it uses server side jdbc.
        Test test = suite;
        if (JDBC.vmSupportsJDBC3()) {
        
        
          suite.addTest(new DatabaseClassLoadingTest("testWithNoInstalledJars"));
          suite.addTest(new DatabaseClassLoadingTest("testWithNoClasspath"));
          suite.addTest(
                SecurityManagerSetup.noSecurityManager(
                        new DatabaseClassLoadingTest("testSetClasspath")));
        
          
           suite.addTest(SecurityManagerSetup.noSecurityManager(
                new DatabaseClassLoadingTest("testAddContact")));
        
           suite.addTest(SecurityManagerSetup.noSecurityManager(
                new DatabaseClassLoadingTest("testGetResource")));
           
           suite.addTest(SecurityManagerSetup.noSecurityManager(
                   new DatabaseClassLoadingTest("testAlterTable")));

           suite.addTest(SecurityManagerSetup.noSecurityManager(
                   new DatabaseClassLoadingTest("testClassPathRollback")));
           
           suite.addTest(SecurityManagerSetup.noSecurityManager(
                   new DatabaseClassLoadingTest("testReplaceJar")));        
           
           suite.addTest(SecurityManagerSetup.noSecurityManager(
                   new DatabaseClassLoadingTest("testReplacedClass")));
           suite.addTest(SecurityManagerSetup.noSecurityManager(
                   new DatabaseClassLoadingTest("testSecondJar")));
           suite.addTest(SecurityManagerSetup.noSecurityManager(
                   new DatabaseClassLoadingTest("testSignedJar")));
           
           suite.addTest(new DatabaseClassLoadingTest("testCreateDatabaseJar"));
           
           suite.addTest(SecurityManagerSetup.noSecurityManager(
                   new DatabaseClassLoadingTest("testHackedJarReplacedClass")));
           suite.addTest(SecurityManagerSetup.noSecurityManager(
                   new DatabaseClassLoadingTest("testInvalidJar")));           
           suite.addTest(SecurityManagerSetup.noSecurityManager(
                   new DatabaseClassLoadingTest("testRemoveJar"))); 

/*  SKIP due to DERBY-2083         
           suite.addTest(SecurityManagerSetup.noSecurityManager(
                   new DatabaseClassLoadingTest("testDatabaseInJar"))); 

           suite.addTest(SecurityManagerSetup.noSecurityManager(
                   new DatabaseClassLoadingTest("testDatabaseInClasspath"))); 
*/

           test = new SupportFilesSetup(suite,
                   new String[] {
                   "functionTests/tests/lang/dcl_emc1.jar",
                   "functionTests/tests/lang/dcl_emcaddon.jar",
                   "functionTests/tests/lang/dcl_emc2.jar",
                   "functionTests/tests/lang/dcl_emc2s.jar",
                   "functionTests/tests/lang/dcl_emc2sm.jar",
                   "functionTests/tests/lang/dcl_emc2l.jar"
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
                  "create function EMC.GETARTICLE(path VARCHAR(40)) " +
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
        
        final String db = getTestConfiguration().getDatabaseName();
        AccessController.doPrivileged
        (new java.security.PrivilegedExceptionAction(){
            
            public Object run() throws Exception { 
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
        String dbName = "jar:(" + jarFile.getAbsolutePath() + ")dbro";
        
        DataSource ds = JDBCDataSource.getDataSource(dbName);
        
        readOnlyTest(ds);
    }
    
    public void testDatabaseInClasspath() throws SQLException, MalformedURLException
    {
        String dbName = "classpath:dbro";
        DataSource ds = JDBCDataSource.getDataSource(dbName);
        
        try {
            ds.getConnection();
            fail("opened database before it was on classpath");
        } catch (SQLException e)
        {
           assertSQLState("XJ004", e);
        }
        
        URL jarURL = SupportFilesSetup.getReadOnlyURL("dclt.jar");
        
        setContextClassLoader(jarURL);
        try {
            readOnlyTest(ds);
        } finally {
            setContextClassLoader(null);
        } 
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

            // Disabled due to DERBY-522
            // getResourceTests(conn);
            
            // Disabled due to DERBY-553
            // signersTests(conn);
            
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
    
    private static void setContextClassLoader(final URL url)
    {
        AccessController.doPrivileged
        (new java.security.PrivilegedAction(){
            
            public Object run()  { 
                URLClassLoader cl = 
                    url == null ? null : new URLClassLoader(new URL[] {url});
                java.lang.Thread.currentThread().setContextClassLoader(cl);
              return null;
            }
        });
    }
}
