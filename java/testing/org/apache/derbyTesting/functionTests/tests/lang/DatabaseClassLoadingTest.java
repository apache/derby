package org.apache.derbyTesting.functionTests.tests.lang;

import java.net.URL;
import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.SecurityManagerSetup;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Test database class loading, executing routines from the
 * installed jars including accessing resources.
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
     * most of the tests are run without a secuirty manager.
     * Ordering is important here so the fixtures are added
     * explicitly.
     */
    public static Test suite()
    {
        final TestSuite suite = new TestSuite("DatabaseClassLoadingTest");
        
        suite.addTest(new DatabaseClassLoadingTest("testWithNoInstalledJars"));
        suite.addTest(
                SecurityManagerSetup.noSecurityManager(
                new DatabaseClassLoadingTest("testWithNoClasspath")));
        suite.addTest(
                SecurityManagerSetup.noSecurityManager(
                        new DatabaseClassLoadingTest("testSetClasspath")));
        
        // Need DriverManager to execute the add contact procedure
        // as it uses server side jdbc.
        if (JDBC.vmSupportsJDBC3()) {
           suite.addTest(SecurityManagerSetup.noSecurityManager(
                new DatabaseClassLoadingTest("testAddContact")));
        }
        
        suite.addTest(SecurityManagerSetup.noSecurityManager(
                new DatabaseClassLoadingTest("testGetResource")));        
        
        return new CleanDatabaseTestSetup(suite) {
            protected void decorateSQL(Statement s) throws SQLException
            {
                s.executeUpdate("create schema emc");
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
     */
    public void testWithNoClasspath() throws SQLException
    {       
        URL jar =
            getTestResource("org/apache/derbyTesting/functionTests/tests/lang/dcl_emc1.jar");
        
        assertNotNull(jar);
        CallableStatement cs = prepareCall("CALL SQLJ.INSTALL_JAR(?, ?, 0)");
        
        cs.setString(1, jar.toExternalForm());
        cs.setString(2, "EMC.MAIL_APP");
        cs.executeUpdate();
        cs.close();

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
        PreparedStatement ps = prepareStatement("VALUES EMC.GETARTICLE(?)");
        
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
}
