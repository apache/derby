/*

   Derby - Class 
       org.apache.derbyTesting.functionTests.tests.jdbcapi.AuthenticationTest

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

package org.apache.derbyTesting.functionTests.tests.jdbcapi;

import java.security.AccessController;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Locale;
import java.util.Properties;

import javax.sql.DataSource;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.DatabasePropertyTestSetup;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.JDBCDataSource;
import org.apache.derbyTesting.junit.SystemPropertyTestSetup;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Tests authentication and connection level authorization.
 *
 */
public class AuthenticationTest extends BaseJDBCTestCase {

    private static final String PASSWORD_SUFFIX = "suf2ix";
    private static final String USERS[] = 
        {"APP","dan","kreg","jeff","ames","jerry","francois","jamie","howardR",
        "\"eVe\"","\"fred@derby.com\"", "\"123\"" };

    private static final String zeus = "\u0396\u0395\u03A5\u03A3";
    private static final String apollo = "\u0391\u09A0\u039F\u039B\u039B\u039A\u0390";
    
    /** Creates a new instance of the Test */
    public AuthenticationTest(String name) {
        super(name);
    }

    /**
     * Ensure all connections are not in auto commit mode.
     */
    protected void initializeConnection(Connection conn) throws SQLException {
        conn.setAutoCommit(false);
    }

    public static Test suite() {
        TestSuite suite = new TestSuite("AuthenticationTest");
        suite.addTest(baseSuite("AuthenticationTest:embedded"));
        suite.addTest(TestConfiguration.clientServerDecorator(
                baseSuite("AuthenticationTest:client")));
        return suite;
    }
    
    public static Test baseSuite(String name) {
        TestSuite suite = new TestSuite("AuthenticationTest");
        
        Test test = new AuthenticationTest(
            "testConnectShutdownAuthentication");
        setBaseProps(suite, test);
        
        test = new AuthenticationTest("testUserCasing");
        setBaseProps(suite, test);
        
        test = new AuthenticationTest("testUserFunctions");
        setBaseProps(suite, test);

        test = new AuthenticationTest("testNotFullAccessUsers");
        setBaseProps(suite, test);
        
        test = new AuthenticationTest("testUserAccessRoutines");
        setBaseProps(suite, test);
        
        test = new AuthenticationTest(
            "testChangePasswordAndDatabasePropertiesOnly");
        setBaseProps(suite, test);

        // only part of this fixture runs with network server / client
        test = new AuthenticationTest("testGreekCharacters");
        setBaseProps(suite, test);

        test = new AuthenticationTest("testSystemShutdown");
        setBaseProps(suite, test);
        
        // This test needs to run in a new single use database as we're setting
        // a number of properties
        return TestConfiguration.singleUseDatabaseDecorator(suite);
    }
    
    protected static void setBaseProps(TestSuite suite, Test test) 
    {
        // Use DatabasePropertyTestSetup.builtinAuthentication decorator
        // to set the user properties required by this test (and shutdown 
        // the database for the property to take effect).
        // DatabasePropertyTestSetup uses SYSCS_SET_DATABASE_PROPERTY
        // so that is database level setting.
        // Additionally use DatabasePropertyTestSetup to add some
        // possibly useful settings
        // Finally SystemPropertyTestSetup sets up system level users
        Properties props = new Properties();
        props.setProperty("derby.infolog.append", "true");
        props.setProperty("derby.debug.true", "AuthenticationTrace");
        Properties sysprops = new Properties();
        sysprops.put("derby.user.system", "admin");
        sysprops.put("derby.user.mickey", "mouse");
        test = DatabasePropertyTestSetup.builtinAuthentication(test,
            USERS, PASSWORD_SUFFIX);
        test = new DatabasePropertyTestSetup (test, props, true);
        suite.addTest(new SystemPropertyTestSetup (test, sysprops));
    }
    
    protected void setUp() throws Exception {
        
        setDatabaseProperty("derby.database.defaultConnectionMode",
                null, getConnection());
        setDatabaseProperty("derby.database.readOnlyAccessUsers",
                null, getConnection());
        setDatabaseProperty("derby.database.fullAccessUsers",
                null, getConnection());
        
        commit();
        
    }
    
    protected void tearDown() throws Exception {
        removeSystemProperty("derby.connection.requireAuthentication");
        removeSystemProperty("derby.user." +apollo);
        super.tearDown();
    }
    
    /**
     * Test how user names behave with casing.
     * @throws SQLException
     */
    public void testUserCasing() throws SQLException
    {
        for (int i = 0; i < USERS.length; i++)
        {          
            String jdbcUserName = USERS[i];
            boolean delimited = jdbcUserName.charAt(0) == '"';
            String normalUserName;
            if (delimited)
            {
                normalUserName = jdbcUserName.substring(1,
                        jdbcUserName.length() - 1);          
            }
            else
            {
                normalUserName = jdbcUserName.toUpperCase(Locale.ENGLISH);
            }
             
            String password = USERS[i] + PASSWORD_SUFFIX;
            
            userCasingTest(jdbcUserName, normalUserName, password);
            
            if (!delimited)
            {

                if (!normalUserName.equals(jdbcUserName))
                {
                    // Test connecting via the normalized name
                    // but only if it wasn't already tested.
                    // E.g. connect as "DAN" for user DAN as opposed
                    // to the user being defined as dan (regular identifier).
                    
                    // DERBY-3150 disable this test until bug is fixed.
                    //userCasingTest(normalUserName, normalUserName, password);
                }
                
                // Test with the normalized name quoted as a delimited identifer.
                // E.g. connect as "DAN" for user DAN
                
                // DERBY-3150 disable this test until bug is fixed.
                // userCasingTest("\"" + normalUserName + "\"",
                //        normalUserName, password);
            }
            
        }
        
        // Now test that setting the user connection authorizaton
        // with the various names works correctly. Use the first user
        // to set the access on others to avoid setting a user to read-only
        // and then not being able to reset it.
        
        PreparedStatement psGetAccess = prepareStatement(
            "VALUES SYSCS_UTIL.SYSCS_GET_USER_ACCESS(?)");
        CallableStatement csSetAccess = prepareCall(
            "CALL SYSCS_UTIL.SYSCS_SET_USER_ACCESS(?, ?)"); 
        
        setDatabaseProperty("derby.database.fullAccessUsers",
                USERS[0], getConnection());
        setDatabaseProperty("derby.database.readOnlyAccessUsers",
                null, getConnection());
        commit();
        
        // Yes - skip the first user, see above.
        for (int i = 1; i < USERS.length; i++)
        {          
            String jdbcUserName = USERS[i];
            
            boolean delimited = jdbcUserName.charAt(0) == '"';
            String normalUserName;
            if (delimited)
            {
                normalUserName = jdbcUserName.substring(1,
                        jdbcUserName.length() - 1);          
            }
            else
            {
                normalUserName = jdbcUserName.toUpperCase(Locale.ENGLISH);
            }
             
            String password = USERS[i] + PASSWORD_SUFFIX;
            
            // Set the access with the database property
            setDatabaseProperty("derby.database.readOnlyAccessUsers",
                    jdbcUserName, getConnection());
            commit();
            
            
            Connection connUser = openDefaultConnection(jdbcUserName, password);
            
            // DERBY-2738 (network client always returns false for isReadOnly)
            if (usingEmbedded())
                assertTrue(jdbcUserName + ":isReadOnly()",
                    connUser.isReadOnly());
            
            
            connUser.close();
            
            psGetAccess.setString(1, normalUserName);
            JDBC.assertSingleValueResultSet(psGetAccess.executeQuery(),
                    "READONLYACCESS");
            commit();
            
            // clear the property.
            setDatabaseProperty("derby.database.readOnlyAccessUsers",
                    null, getConnection());
            commit();
            
            // Test it was reset back
            connUser = openDefaultConnection(jdbcUserName, password);
            assertFalse(connUser.isReadOnly());
            connUser.close(); 
            
            psGetAccess.setString(1, normalUserName);
            JDBC.assertSingleValueResultSet(psGetAccess.executeQuery(),
                    "FULLACCESS");
            commit();
            
            
            // Set to be read-only via the procedure which uses
            // the normal user name.
            csSetAccess.setString(1, normalUserName);
            csSetAccess.setString(2, "READONLYACCESS");
            csSetAccess.executeUpdate();
            commit();
            
            connUser = openDefaultConnection(jdbcUserName, password);
            // DERBY-2738 (network client always returns false for isReadOnly)
            if (usingEmbedded())
                assertTrue(jdbcUserName + ":isReadOnly()",
                    connUser.isReadOnly());

            connUser.close();
            
            psGetAccess.setString(1, normalUserName);
            JDBC.assertSingleValueResultSet(psGetAccess.executeQuery(),
                    "READONLYACCESS");
            commit();           

        }
    }
    
    /**
     * Test the user casing obtaining connections a variety of ways.
     * @param jdbcUserName User name to be used to obtain the connection via JDBC
     * @param normalUserName Normalized form of the user connection.
     * @param password Password for the user.
     * @throws SQLException
     */
    private void userCasingTest(String jdbcUserName, String normalUserName,
            String password) throws SQLException
    {
        // Default test mechanism to get a connection.
        userCasingTest(jdbcUserName, normalUserName,
                openDefaultConnection(jdbcUserName, password));
        
        
        DataSource ds = JDBCDataSource.getDataSource();
        
        // DataSource using explict user
        userCasingTest(jdbcUserName, normalUserName,
                ds.getConnection(jdbcUserName, password));
        
        JDBCDataSource.setBeanProperty(ds, "user", jdbcUserName);
        JDBCDataSource.setBeanProperty(ds, "password", password);
        userCasingTest(jdbcUserName, normalUserName,
                ds.getConnection());        
    }
    
    /**
     * 
     * @param jdbcUserName User name as passed into the JDBC connection request.
     * @param normalUserName Normalized user name.
     * @param connUser Connection for the user, closed by this method.
     * @throws SQLException 
     */
    private void userCasingTest(String jdbcUserName, String normalUserName,
            Connection connUser) throws SQLException
    {        
        assertNormalUserName(normalUserName, connUser);
        
        // DatabaseMetaData.getUserName() returns the user name used
        // to make the request via JDBC.
        assertEquals("DatabaseMetaData.getUserName()",
                jdbcUserName, connUser.getMetaData().getUserName());
        
        
        Statement s = connUser.createStatement();
          
        s.executeUpdate("CALL SYSCS_UTIL.SYSCS_SET_USER_ACCESS(" +
                "CURRENT_USER, 'FULLACCESS')");
        
        s.close();
        
        JDBC.cleanup(connUser);
    }
    
    /**
     * Assert that the user name returned by various mechanisms
     * matches the normal user name.
     * @param normalUserName
     * @param conn
     * @throws SQLException
     */
    private void assertNormalUserName(String normalUserName, Connection connUser)
        throws SQLException
    {      
        Statement s = connUser.createStatement();
        
        JDBC.assertSingleValueResultSet(s.executeQuery("VALUES CURRENT_USER"),
                normalUserName);
        JDBC.assertSingleValueResultSet(s.executeQuery("VALUES SESSION_USER"),
                normalUserName);
        JDBC.assertSingleValueResultSet(s.executeQuery("VALUES {fn user()}"),
                normalUserName);
        s.close();
        
    }

    
    // roughly based on old functionTests test users.sql, except that
    // test used 2 databases. Possibly that was on the off-chance that
    // a second database would not work correctly - but that will not
    // be tested now.
    public void testConnectShutdownAuthentication() throws SQLException {
        
        String dbName = TestConfiguration.getCurrent().getDefaultDatabaseName();
        
        // check connections while fullAccess (default) is set
        // note that builtinAuthentication has been set, as well as
        // authentication=true.
        
        // first try connection without user password
        assertConnectionFail(dbName);
        assertConnectionOK(dbName, "system", ("admin"));
        assertConnectionWOUPOK(dbName, "system", ("admin"));
        assertConnectionOK(dbName, "dan", ("dan" + PASSWORD_SUFFIX));
        assertConnectionWOUPOK(dbName, "dan", ("dan" + PASSWORD_SUFFIX));
        // try shutdown as non-owner
        assertShutdownOK(dbName, "dan", ("dan" + PASSWORD_SUFFIX));
        assertConnectionOK(dbName, "system", ("admin"));
        assertShutdownWOUPOK(dbName, "dan", ("dan" + PASSWORD_SUFFIX));
        assertConnectionOK(dbName, "system", ("admin"));
        assertShutdownOK(dbName, "system", "admin");
        assertConnectionOK(dbName, "system", ("admin"));
        assertShutdownWOUPOK(dbName, "system", "admin");
        assertConnectionOK(dbName, "system", ("admin"));
        // try shutdown as owner
        assertShutdownUsingConnAttrsOK(dbName, "APP", ("APP" + PASSWORD_SUFFIX));
        
        // ensure that a password is encrypted
        Connection conn1 = openDefaultConnection(
            "dan", ("dan" + PASSWORD_SUFFIX));
        Statement stmt = conn1.createStatement();
        ResultSet rs = stmt.executeQuery(
            "values SYSCS_UTIL.SYSCS_GET_DATABASE_PROPERTY('derby.user.dan')");
        rs.next();
        assertNotSame(("dan"+PASSWORD_SUFFIX), rs.getString(1));
        conn1.commit();
        conn1.close();

        // specify full-access users.
        conn1 = openDefaultConnection("dan", ("dan" + PASSWORD_SUFFIX));
        setDatabaseProperty(
            "derby.database.fullAccessUsers", 
            "APP,system,nomen,francois,jeff", conn1);
        setDatabaseProperty(
            "derby.database.defaultConnectionMode","NoAccess", conn1);
        conn1.commit();
        conn1.close();

        // check the system wide user
        assertConnectionOK(dbName, "system", "admin"); 
        // check the non-existent, but allowed user
        assertConnectionFail("08004", dbName, "nomen", "nescio");
        assertConnectionWOUPFail("08004", dbName, "nomen", "nescio");
        // attempt to shutdown db as one of the allowed users, but not db owner
        assertShutdownOK(dbName, "francois", ("francois" + PASSWORD_SUFFIX));
        // attempt shutdown as db owner
        assertConnectionOK(dbName, "system", "admin");
        assertShutdownWOUPOK(dbName, "APP", ("APP" + PASSWORD_SUFFIX));
        // check simple connect ok as another allowed user, also revive db
        assertConnectionOK(dbName, "jeff", ("jeff" + PASSWORD_SUFFIX));
        // but dan wasn't on the list
        assertConnectionFail("08004", dbName, "dan", ("dan" + PASSWORD_SUFFIX));
        assertShutdownFail("08004", dbName, "dan", ("dan" + PASSWORD_SUFFIX));

        // now change fullAccessUsers & test again
        conn1 = 
            openDefaultConnection("francois", ("francois" + PASSWORD_SUFFIX));
        setDatabaseProperty("derby.database.fullAccessUsers", 
            "jeff,dan,francois,jamie", conn1);
        conn1.commit();
        conn1.close();
        assertConnectionOK(dbName, "dan", ("dan" + PASSWORD_SUFFIX));
        assertShutdownOK(dbName, "dan", ("dan" + PASSWORD_SUFFIX));
        assertConnectionOK(dbName, "dan", ("dan" + PASSWORD_SUFFIX)); 
         // but dbo was not on list...
        assertShutdownFail("08004", dbName, "APP", ("APP" + PASSWORD_SUFFIX));
        // now add dbo back in...
        conn1 = openDefaultConnection("francois", ("francois" + PASSWORD_SUFFIX));
        setDatabaseProperty(
            "derby.database.defaultConnectionMode","NoAccess", conn1);
        setDatabaseProperty(
            "derby.database.fullAccessUsers", 
            "APP,jeff,dan,francois,jamie", conn1);
        conn1.commit();
        conn1.close();

        // Invalid logins
        // bad user
        assertConnectionFail("08004", dbName, "badUser", "badPwd");
        // just checking that it's still not working if we try again
        assertConnectionFail("08004", dbName, "badUser", "badPwd");
        // system is not on the list...
        assertConnectionFail("08004", dbName, "system", "admin");
        // dan's on the list, but this isn't the pwd
        assertConnectionFail("08004", dbName, "dan", "badPwd");
        assertConnectionFail("08004", dbName, "jamie", ("dan" + PASSWORD_SUFFIX));
        // check some shutdowns
        assertShutdownFail("08004", dbName, "system", "admin");
        assertShutdownFail("08004", dbName, "badUser", "badPwd");
        assertShutdownFail("08004", dbName, "dan", "badPwd");
        assertShutdownFail("08004", dbName, "badUser", ("dan" + PASSWORD_SUFFIX));

        // try some system shutdowns. Note, that all these work, because
        // we have not set require authentication at system level.
        // try system shutdown with wrong user - should work
        assertSystemShutdownOK("", "badUser", ("dan" + PASSWORD_SUFFIX));
        openDefaultConnection("dan", ("dan" + PASSWORD_SUFFIX)).close(); // revive
        // with 'allowed' user but bad pwd - will succeed
        assertSystemShutdownOK("", "dan", ("jeff" + PASSWORD_SUFFIX));
        openDefaultConnection("dan", ("dan" + PASSWORD_SUFFIX)).close(); // revive
        // dbo, but bad pwd - will succeed
        assertSystemShutdownOK("", "APP", ("POO"));
        openDefaultConnection("dan", ("dan" + PASSWORD_SUFFIX)).close(); // revive
        // allowed user but not dbo - will also succeed
        assertSystemShutdownOK("", "dan", ("dan" + PASSWORD_SUFFIX));
        openDefaultConnection("dan", ("dan" + PASSWORD_SUFFIX)).close(); // revive
        // expect Derby system shutdown, which gives XJ015 error.
        assertSystemShutdownOK("", "APP", ("APP" + PASSWORD_SUFFIX));

        // so far so good. set back security properties
        conn1 = openDefaultConnection("dan", ("dan" + PASSWORD_SUFFIX));
        setDatabaseProperty(
            "derby.database.defaultConnectionMode","fullAccess", conn1);
        setDatabaseProperty(
            "derby.connection.requireAuthentication","false", conn1);
        conn1.commit();
        stmt.close();
        conn1.close();
    }

    // Experiment using USER, CURRENT_USER, etc.
    // also tests actual write activity
    public void testUserFunctions() throws SQLException
    {
        // use valid user/pwd to set the full accessusers.
        Connection conn1 = openDefaultConnection(
            "dan", ("dan" + PASSWORD_SUFFIX));
        setDatabaseProperty(
            "derby.database.fullAccessUsers", 
            "francois,jeff,ames,jerry,jamie,dan,system", conn1);
        setDatabaseProperty(
            "derby.database.defaultConnectionMode","NoAccess", conn1);
        conn1.commit();

        // we should still be connected as dan
        Statement stmt = conn1.createStatement();
        assertUpdateCount(stmt, 0, 
            "create table APP.t1(c1 varchar(30) check (UPPER(c1) <> 'JAMIE'))");
        assertUpdateCount(stmt, 1, "insert into APP.t1 values USER");
      
        conn1.commit();
        stmt.close();
        conn1.close();

        useUserValue(1, "jeff", "insert into APP.t1 values CURRENT_USER");
        useUserValue(1, "ames", "insert into APP.t1 values SESSION_USER");
        useUserValue(1, "jerry", "insert into APP.t1 values {fn user()}");
        assertUserValue(new String[] {"DAN","JEFF","AMES","JERRY"},
            "dan", "select * from APP.t1");
        // attempt some usage in where clause
        useUserValue(1,
            "dan", "update APP.t1 set c1 = 'edward' where c1 = USER");
        assertUserValue(new String[] {"JEFF"},"jeff",
            "select * from APP.t1 where c1 like CURRENT_USER");
        useUserValue(1, "ames", 
            "update APP.t1 set c1 = 'sema' where SESSION_USER = c1");
        useUserValue(1, "jerry", 
            "update APP.t1 set c1 = 'yrrej' where c1 like {fn user()}");
        assertUserValue(new String[] {"edward","JEFF","sema","yrrej"},
            "dan", "select * from APP.t1");
        useUserValue(4, "francois", "update APP.T1 set c1 = USER");
        assertUserValue(
            new String[] {"FRANCOIS","FRANCOIS","FRANCOIS","FRANCOIS"},
            "dan", "select * from APP.t1");

        // check that attempt to insert 'jamie' gives a check violation
        conn1 = openDefaultConnection("jamie", ("jamie" + PASSWORD_SUFFIX));
        stmt = conn1.createStatement();
        try {
            stmt.execute("insert into APP.t1 values CURRENT_USER");
        } catch (SQLException sqle) {
            assertSQLState("23513", sqle);
        }
        stmt.close();
        conn1.rollback();
        conn1.close();

        // Note: there is not much point in attempting to write with an invalid
        // user, that's already tested in the testConnectionShutdown fixture

        // reset
        conn1 = openDefaultConnection("dan", ("dan" + PASSWORD_SUFFIX));
        setDatabaseProperty(
            "derby.database.defaultConnectionMode","fullAccess", conn1);
        setDatabaseProperty(
            "derby.connection.requireAuthentication","false", conn1);
        stmt = conn1.createStatement();
        assertUpdateCount(stmt, 0, "drop table APP.t1");
        conn1.commit();
        stmt.close();
        conn1.close();
    }

    public void testChangePasswordAndDatabasePropertiesOnly() 
    throws SQLException
    {
        String dbName = TestConfiguration.getCurrent().getDefaultDatabaseName();

        // use valid user/pwd to set the full accessusers.
        Connection conn1 = openDefaultConnection(
            "dan", ("dan" + PASSWORD_SUFFIX));
        setDatabaseProperty("derby.database.fullAccessUsers", 
            "dan,jeff,system", conn1);
        setDatabaseProperty(
            "derby.database.defaultConnectionMode","NoAccess", conn1);
        setDatabaseProperty(
                "derby.database.requireAuthentication","true", conn1);
        
        conn1.commit();
        
        // check the system wide user
        assertConnectionOK(dbName, "system", "admin"); 
        assertConnectionFail("08004", dbName, "system", "otherSysPwd");
        assertConnectionOK(dbName, "jeff", ("jeff" + PASSWORD_SUFFIX));
        assertConnectionFail("08004", dbName, "jeff", "otherPwd");
        setDatabaseProperty("derby.user.jeff", "otherPwd", conn1);
        conn1.commit();
        // should have changed ok.
        assertConnectionOK(dbName, "jeff", "otherPwd");

        // note: if we do this:
        //  setDatabaseProperty("derby.user.system", "scndSysPwd", conn1);
        //  conn1.commit();
        // i.e. adding the same user (but different pwd) at database level,
        // then we cannot connect anymore using that user name, not with
        // either password.

        // force database props only
        setDatabaseProperty(
            "derby.database.propertiesOnly","true", conn1);
        conn1.commit();
        
        // now, should not be able to logon as system user
        assertConnectionFail("08004", dbName, "system", "admin");

        // reset propertiesOnly
        setDatabaseProperty(
            "derby.database.propertiesOnly","false", conn1);
        conn1.commit();
        conn1.close();
        assertConnectionOK(dbName, "system", "admin");
        
        // try changing system's pwd
        setSystemProperty("derby.user.system", "thrdSysPwd");

        // can we get in as system user with changed pwd
        assertConnectionOK(dbName, "system", "thrdSysPwd");
        
        // reset
        // first change system's pwd back
        setSystemProperty("derby.user.system", "admin");

        conn1 = openDefaultConnection("dan", ("dan" + PASSWORD_SUFFIX));
        setDatabaseProperty(
            "derby.database.defaultConnectionMode","fullAccess", conn1);
        setDatabaseProperty(
            "derby.connection.requireAuthentication","false", conn1);
        setDatabaseProperty(
                "derby.database.propertiesOnly","false", conn1);
        conn1.commit();
        conn1.close();
    }
    
    public void testNotFullAccessUsers() throws SQLException
    {
        // use valid user/pwd to set the full accessusers.
        Connection conn1 = openDefaultConnection(
            "dan", ("dan" + PASSWORD_SUFFIX));
        
        // Test duplicates on the list of users
        try {
            setDatabaseProperty("derby.database.fullAccessUsers", 
                    "dan,jamie,dan", conn1);
            fail("Duplicate allowed on derby.database.fullAccessUsers");
        } catch (SQLException e) {
            assertSQLState("4250D", e);
        }
        try {
            setDatabaseProperty("derby.database.fullAccessUsers", 
                    "dan,jamie,DaN", conn1);
            fail("Duplicate allowed on derby.database.fullAccessUsers");
        } catch (SQLException e) {
            assertSQLState("4250D", e);
        }
        try {
            setDatabaseProperty("derby.database.fullAccessUsers", 
                    "dan,jamie,\"DAN\"", conn1);
            fail("Duplicate allowed on derby.database.fullAccessUsers");
        } catch (SQLException e) {
            assertSQLState("4250D", e);
        }
        try {
            setDatabaseProperty("derby.database.fullAccessUsers", 
                    "\"dan\",jamie,\"dan\"", conn1);
            fail("Duplicate allowed on derby.database.fullAccessUsers");
        } catch (SQLException e) {
            assertSQLState("4250D", e);
        }
        
        try {
            setDatabaseProperty("derby.database.readOnlyAccessUsers", 
                    "dan,jamie,dan", conn1);
            fail("Duplicate allowed on derby.database.readOnlyAccessUsers");
        } catch (SQLException e) {
            assertSQLState("4250D", e);
        }
        try {
            setDatabaseProperty("derby.database.readOnlyAccessUsers", 
                    "dan,jamie,DaN", conn1);
            fail("Duplicate allowed on derby.database.readOnlyAccessUsers");
        } catch (SQLException e) {
            assertSQLState("4250D", e);
        }
        try {
            setDatabaseProperty("derby.database.readOnlyAccessUsers", 
                    "dan,jamie,\"DAN\"", conn1);
            fail("Duplicate allowed on derby.database.readOnlyAccessUsers");
        } catch (SQLException e) {
            assertSQLState("4250D", e);
        }
        try {
            setDatabaseProperty("derby.database.readOnlyAccessUsers", 
                    "\"dan\",jamie,\"dan\"", conn1);
            fail("Duplicate allowed on derby.database.readOnlyAccessUsers");
        } catch (SQLException e) {
            assertSQLState("4250D", e);
        }
        
        setDatabaseProperty("derby.database.fullAccessUsers", 
            "dan,jamie,system", conn1);
        // cannot set a user to both full and readonly access...
        assertFailSetDatabaseProperty(
                "derby.database.readOnlyAccessUsers", "jamie", conn1);
        setDatabaseProperty(
                "derby.database.readOnlyAccessUsers", "ames,mickey", conn1);
        setDatabaseProperty(
            "derby.database.defaultConnectionMode","NoAccess", conn1);
        setDatabaseProperty(
                "derby.database.requireAuthentication","true", conn1);
        conn1.commit();
        
        
        PreparedStatement psGetAccess = conn1.prepareStatement(
                "VALUES SYSCS_UTIL.SYSCS_GET_USER_ACCESS(?)");
        psGetAccess.setString(1, "JAMIE");
        JDBC.assertSingleValueResultSet(psGetAccess.executeQuery(), "FULLACCESS");
        
        psGetAccess.setString(1, "DAN");
        JDBC.assertSingleValueResultSet(psGetAccess.executeQuery(), "FULLACCESS");
        
        psGetAccess.setString(1, "SYSTEM");
        JDBC.assertSingleValueResultSet(psGetAccess.executeQuery(), "FULLACCESS");
        
        psGetAccess.setString(1, "AMES");
        JDBC.assertSingleValueResultSet(psGetAccess.executeQuery(), "READONLYACCESS");
        
        psGetAccess.setString(1, "MICKEY");
        JDBC.assertSingleValueResultSet(psGetAccess.executeQuery(), "READONLYACCESS");

        // unknown user
        psGetAccess.setString(1, "hagrid");
        JDBC.assertSingleValueResultSet(psGetAccess.executeQuery(), "NOACCESS");
        
        conn1.commit();
        
        // now add/switch some names using the utility method
        CallableStatement csSetAccess = conn1.prepareCall(
            "CALL SYSCS_UTIL.SYSCS_SET_USER_ACCESS(?, ?)");
        
        // Change AMES, everyone else is unchanged
        csSetAccess.setString(1, "AMES");
        csSetAccess.setString(2, "FULLACCESS");
        csSetAccess.execute();
        
        psGetAccess.setString(1, "AMES");
        JDBC.assertSingleValueResultSet(psGetAccess.executeQuery(), "FULLACCESS");
        
        psGetAccess.setString(1, "MICKEY");
        JDBC.assertSingleValueResultSet(psGetAccess.executeQuery(), "READONLYACCESS");
        psGetAccess.setString(1, "JAMIE");
        JDBC.assertSingleValueResultSet(psGetAccess.executeQuery(), "FULLACCESS");       
        psGetAccess.setString(1, "DAN");
        JDBC.assertSingleValueResultSet(psGetAccess.executeQuery(), "FULLACCESS");        
        psGetAccess.setString(1, "SYSTEM");
        JDBC.assertSingleValueResultSet(psGetAccess.executeQuery(), "FULLACCESS");

        // and change AMES back again
        csSetAccess.setString(1, "AMES");
        csSetAccess.setString(2, "READONLYACCESS");
        csSetAccess.execute();
        
        psGetAccess.setString(1, "AMES");
        JDBC.assertSingleValueResultSet(psGetAccess.executeQuery(), "READONLYACCESS");
        
        psGetAccess.setString(1, "MICKEY");
        JDBC.assertSingleValueResultSet(psGetAccess.executeQuery(), "READONLYACCESS");
        psGetAccess.setString(1, "JAMIE");
        JDBC.assertSingleValueResultSet(psGetAccess.executeQuery(), "FULLACCESS");       
        psGetAccess.setString(1, "DAN");
        JDBC.assertSingleValueResultSet(psGetAccess.executeQuery(), "FULLACCESS");        
        psGetAccess.setString(1, "SYSTEM");
        JDBC.assertSingleValueResultSet(psGetAccess.executeQuery(), "FULLACCESS");

        // add a new users in
        csSetAccess.setString(1, "BOND");
        csSetAccess.setString(2, "FULLACCESS");
        csSetAccess.execute(); 
        csSetAccess.setString(1, "JAMES");
        csSetAccess.setString(2, "READONLYACCESS");
        csSetAccess.execute();
        conn1.commit();
        
        psGetAccess.setString(1, "BOND");
        JDBC.assertSingleValueResultSet(psGetAccess.executeQuery(), "FULLACCESS");
        psGetAccess.setString(1, "JAMES");
        JDBC.assertSingleValueResultSet(psGetAccess.executeQuery(), "READONLYACCESS");
        conn1.commit();
        
        // and remove them
        csSetAccess.setString(1, "BOND");
        csSetAccess.setString(2, null);
        csSetAccess.execute(); 
        csSetAccess.setString(1, "JAMES");
        csSetAccess.setString(2, null);
        csSetAccess.execute(); 
        conn1.commit();
        
        psGetAccess.setString(1, "BOND");
        JDBC.assertSingleValueResultSet(psGetAccess.executeQuery(), "NOACCESS");
        psGetAccess.setString(1, "JAMES");
        JDBC.assertSingleValueResultSet(psGetAccess.executeQuery(), "NOACCESS");
        conn1.commit();
         
        
        psGetAccess.close();
        csSetAccess.close();
        
  
        // we should still be connected as dan
        Statement stmt = conn1.createStatement();
        assertUpdateCount(stmt, 0, 
            "create table APP.t1(c1 varchar(30) check (UPPER(c1) <> 'JAMIE'))");
        assertUpdateCount(stmt, 1, "insert into APP.t1 values USER");
      
        conn1.commit();
        stmt.close();
        conn1.close();

        // check full access system level user can update
        conn1 = openDefaultConnection("system", "admin");
        stmt = conn1.createStatement();
        assertUpdateCount(stmt, 1, "update APP.t1 set c1 = USER");
        conn1.commit();
        stmt.close();
        conn1.close();
        
        // read only users
        assertUserValue(new String[] {"SYSTEM"},"ames", 
            "select * from APP.t1"); // should succeed
        conn1 = openDefaultConnection("ames", ("ames"+PASSWORD_SUFFIX));
        
        // DERBY-2738 (network client always returns false for isReadOnly)
        if (usingEmbedded())
            assertTrue(conn1.isReadOnly());
        stmt = conn1.createStatement();
        assertStatementError(
            "25502", stmt, "delete from APP.t1 where c1 = 'SYSTEM'");
        assertStatementError("25502", stmt, "insert into APP.t1 values USER");
        assertStatementError(
            "25502", stmt, "update APP.t1 set c1 = USER where c1 = 'SYSTEM'");
        assertStatementError("25503", stmt, "create table APP.t2 (c1 int)");
        conn1.commit();
        stmt.close();
        conn1.close();
        
        // read-only system level user
        conn1 = openDefaultConnection("mickey", "mouse");
        // DERBY-2738 (network client always returns false for isReadOnly)
        if (usingEmbedded())
            assertTrue(conn1.isReadOnly());
        stmt = conn1.createStatement();
        assertStatementError(
            "25502", stmt, "delete from APP.t1 where c1 = 'SYSTEM'");
        conn1.rollback();
        conn1.close();

        // reset
        conn1 = openDefaultConnection("dan", ("dan" + PASSWORD_SUFFIX));
        setDatabaseProperty(
            "derby.database.defaultConnectionMode","fullAccess", conn1);
        setDatabaseProperty(
            "derby.connection.requireAuthentication","false", conn1);
        stmt = conn1.createStatement();
        assertUpdateCount(stmt, 0, "drop table APP.t1");
        conn1.commit();
        stmt.close();
        conn1.close();
    }
    
    /**
     * Test the procedure and function that provide short-cuts
     * to setting and getting connection level access.
     * @throws SQLException
     */
    public void testUserAccessRoutines() throws SQLException
    {
        // use valid user/pwd to set the full accessusers.
        Connection conn1 = openDefaultConnection(
            "dan", ("dan" + PASSWORD_SUFFIX));
        
        PreparedStatement psGetAccess = conn1.prepareStatement(
            "VALUES SYSCS_UTIL.SYSCS_GET_USER_ACCESS(?)");
        CallableStatement csSetAccess = conn1.prepareCall(
            "CALL SYSCS_UTIL.SYSCS_SET_USER_ACCESS(?, ?)"); 
        
        csSetAccess.setString(1, "DAN");
        csSetAccess.setString(2, "FULLACCESS");
        csSetAccess.execute();

        // Invalid users
        csSetAccess.setString(1, null);
        csSetAccess.setString(2, "FULLACCESS");
        assertStatementError("28502", csSetAccess);
        

        // Random user will now have only READONLYACCESS
        setDatabaseProperty(
                "derby.database.defaultConnectionMode","READONLYACCESS", conn1);       
        conn1.commit();             
        psGetAccess.setString(1, "TONYBLAIR");
        JDBC.assertSingleValueResultSet(psGetAccess.executeQuery(), "READONLYACCESS");
        conn1.commit();

        // Random user will now have FULLACCESS
        setDatabaseProperty(
                "derby.database.defaultConnectionMode","FULLACCESS", conn1);       
        conn1.commit();             
        psGetAccess.setString(1, "TONYBLAIR");
        JDBC.assertSingleValueResultSet(psGetAccess.executeQuery(), "FULLACCESS");
        conn1.commit();
        
        // and still full access
        setDatabaseProperty(
                "derby.database.defaultConnectionMode", null, conn1);       
        conn1.commit();             
        psGetAccess.setString(1, "TONYBLAIR");
        JDBC.assertSingleValueResultSet(psGetAccess.executeQuery(), "FULLACCESS");
        conn1.commit();
        
        conn1.close();
           
    }
    
    public void testGreekCharacters() throws SQLException {
        
        String dbName = TestConfiguration.getCurrent().getDefaultDatabaseName();
        
        setSystemProperty("derby.user." + apollo, zeus);

        Connection conn1 = openDefaultConnection(
                "dan", ("dan" + PASSWORD_SUFFIX));
        // add a database level user
        setDatabaseProperty(("derby.user." + zeus), apollo, conn1);
        setDatabaseProperty("derby.database.fullAccessUsers", 
                ("dan,system,APP" + zeus + "," + apollo) , conn1);
        conn1.commit();
        conn1.close();
        
        // with network server / derbynet client, non-ascii isn't supported,
        // (or not working) see also DERBY-728, and
        // org.apache.derby.client.net.EbcdicCcsidManager
        if (usingDerbyNetClient())
        {
            assertConnectionFail("22005", dbName, zeus, apollo);
        }
        else {
            assertConnectionOK(dbName, zeus, apollo);
            assertConnectionFail("08004", dbName, apollo, apollo);
            // shutdown as non-dbo
            assertShutdownOK(dbName, zeus, apollo);
            assertConnectionOK(dbName, apollo, zeus);
            // wrong credentials
            assertShutdownFail("08004", dbName, zeus, zeus);
             // shutdown as non-dbo
            assertShutdownOK(dbName, apollo, zeus);
            assertConnectionOK(dbName, apollo, zeus);
            // shutdown as dbo
            assertShutdownUsingSetShutdownOK(
                dbName, "APP", ("APP" + PASSWORD_SUFFIX));

            conn1 = openDefaultConnection(zeus, apollo);
            Statement stmt = conn1.createStatement();
            assertUpdateCount(stmt, 0, 
            "create table APP.t1(c1 varchar(30))");
            assertUpdateCount(stmt, 1, "insert into APP.t1 values USER");
            conn1.commit();
            assertUserValue(new String[] {zeus}, zeus, apollo,
            "select * from APP.t1 where c1 like CURRENT_USER");
            stmt.close();
            conn1.close();
        }
        
        // reset
        conn1 = openDefaultConnection("dan", ("dan" + PASSWORD_SUFFIX));
        setDatabaseProperty(
            "derby.database.defaultConnectionMode","fullAccess", conn1);
        setDatabaseProperty(
            "derby.connection.requireAuthentication","false", conn1);
        Statement stmt = conn1.createStatement();
        if (usingEmbedded())
            assertUpdateCount(stmt, 0, "drop table APP.t1");
        conn1.commit();
        stmt.close();
        conn1.close();
    }
    
    // tests system shutdown with setting required authentication at
    // system level
    public void testSystemShutdown() throws SQLException
    {
        String dbName = TestConfiguration.getCurrent().getDefaultDatabaseName();
        
        // just for the setting the stage, recheck connections while fullAccess
        // (default) is set at database level. 
        
        // first try connection with valid user/password
        assertConnectionOK(dbName, "system", ("admin"));
        assertConnectionOK(dbName, "dan", ("dan" + PASSWORD_SUFFIX));

        // try ensuring system level is set for authentication
        setSystemProperty("derby.connection.requireAuthentication", "true");

        // bring down the database
        assertShutdownUsingSetShutdownOK(
            dbName, "APP", "APP" + PASSWORD_SUFFIX);
        // recheck
        assertConnectionOK(dbName, "system", "admin");
        assertConnectionOK(dbName, "dan", ("dan" + PASSWORD_SUFFIX));
        // bring down server to ensure settings take effect 
        assertSystemShutdownOK("", "badUser", ("dan" + PASSWORD_SUFFIX));
        openDefaultConnection("dan", ("dan" + PASSWORD_SUFFIX)).close(); // revive

        // try system shutdown with wrong user
        assertSystemShutdownFail("08004", "", "badUser", ("dan" + PASSWORD_SUFFIX));
        // with 'allowed' user but bad pwd
        assertSystemShutdownFail("08004", "", "dan", ("jeff" + PASSWORD_SUFFIX));
        // APP, but bad pwd
        assertSystemShutdownFail("08004", "", "APP", ("POO"));
        // note: we don't have a database, so no point checking for dbo.
        // expect Derby system shutdown, which gives XJ015 error.
        assertSystemShutdownOK("", "system", "admin");
        
        // reset.
        Connection conn1 = openDefaultConnection("dan", ("dan" + PASSWORD_SUFFIX));
        setDatabaseProperty(
            "derby.database.defaultConnectionMode","fullAccess", conn1);
        setDatabaseProperty(
            "derby.connection.requireAuthentication","false", conn1);
        
        setSystemProperty("derby.connection.requireAuthentication", "false");

        conn1.commit();
        conn1.close();
        openDefaultConnection("system", "admin").close();
        assertShutdownUsingSetShutdownOK(
            dbName, "APP", "APP" + PASSWORD_SUFFIX);
        assertSystemShutdownOK("", "system", "admin");
        openDefaultConnection("system", "admin").close(); // just so teardown works.
    }
    
    protected void assertFailSetDatabaseProperty(
        String propertyName, String value, Connection conn) 
    throws SQLException {
        CallableStatement setDBP =  conn.prepareCall(
        "CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(?, ?)");
        setDBP.setString(1, propertyName);
        setDBP.setString(2, value);
        // user jamie cannot be both readOnly and fullAccess
        assertStatementError("4250C", setDBP);
        setDBP.close();
    }
    
    protected void setDatabaseProperty(
        String propertyName, String value, Connection conn) 
    throws SQLException {
        CallableStatement setDBP =  conn.prepareCall(
        "CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(?, ?)");
        setDBP.setString(1, propertyName);
        setDBP.setString(2, value);
        setDBP.execute();
        setDBP.close();
    }
    
    protected void useUserValue(int expectedUpdateCount, String user, String sql)
    throws SQLException
    {
        Connection conn1 = openDefaultConnection(user, user + PASSWORD_SUFFIX);
        Statement stmt = conn1.createStatement();
        assertUpdateCount(stmt, expectedUpdateCount, sql);
        conn1.commit();
        stmt.close();
        conn1.close();
    }
    
    // verify that the return value is the expected value, and 
    // we have the expected number of rows returning from the query
    // in this test, it will be one of the user names through
    // use of CURRENT_USER, SESSION_USER etc.
    protected void assertUserValue(
        String[] expected, String user, String password, String sql)
    throws SQLException
    {
        Connection conn1 = openDefaultConnection(user, password);
        Statement stmt = conn1.createStatement();
        ResultSet rs = stmt.executeQuery(sql);
        int i = 0; 
        while (rs.next())
        {
            assertEquals(expected[i],rs.getString(1));
            i++;
        }
        assertEquals(expected.length, i);
        conn1.commit();
        stmt.close();
        conn1.close();
    }
    
    // convenience method, password is often using PASSWORD_SUFFIX
    protected void assertUserValue(String[] expected, String user, String sql)
    throws SQLException {
        assertUserValue(expected, user, (user + PASSWORD_SUFFIX), sql);
    }
    
    // get a connection using ds.getConnection(user, password)
    protected void assertConnectionOK(
         String dbName, String user, String password)
    throws SQLException
    {
        DataSource ds = JDBCDataSource.getDataSource(dbName);
        Connection conn = ds.getConnection(user, password);
        assertNotNull(conn);
        conn.close();
    }
    
    // get a connection, using setUser / setPassword, and ds.getConnection()
    protected void assertConnectionWOUPOK(
        String dbName, String user, String password)
    throws SQLException
    {
        DataSource ds = JDBCDataSource.getDataSource(dbName);
        JDBCDataSource.setBeanProperty(ds, "user", user);
        JDBCDataSource.setBeanProperty(ds, "password", password);
        Connection conn = ds.getConnection();
        assertNotNull(conn);
        conn.close();
    }
    
    protected void assertConnectionFail(
        String expectedSqlState, String dbName, String user, String password)
    throws SQLException
    {
        DataSource ds = JDBCDataSource.getDataSource(dbName);
        try {
            ds.getConnection(user, password);
            fail("Connection should've been refused/failed");
        }
        catch (SQLException e) {
            assertSQLState(expectedSqlState, e);
        }
    }

    // same action as with assertConnectionFail, but using ds.getConnection()
    // instead of ds.getConnection(user, password). So, setting user and
    // password using appropriate ds.set method.
    protected void assertConnectionWOUPFail(
        String expectedError, String dbName, String user, String password) 
    throws SQLException 
    {
        DataSource ds = JDBCDataSource.getDataSource(dbName);
        JDBCDataSource.setBeanProperty(ds, "user", user);
        JDBCDataSource.setBeanProperty(ds, "password", password);
        try {
                ds.getConnection();
                fail("Connection should've been refused/failed");
        }
        catch (SQLException e) {
                assertSQLState(expectedError, e);
        }
    }

    protected void assertShutdownUsingSetShutdownOK(
            String dbName, String user, String password) throws SQLException {
        DataSource ds = JDBCDataSource.getDataSource(dbName);
        JDBCDataSource.setBeanProperty(ds, "shutdownDatabase", "shutdown");
        try {
            ds.getConnection(user, password);
            fail("expected shutdown to fail");
        } catch (SQLException e) {
            // expect 08006 on successful shutdown
            assertSQLState("08006", e);
        }
    }

    protected void assertShutdownUsingConnAttrsOK(
        String dbName, String user, String password) throws SQLException {

        DataSource ds = JDBCDataSource.getDataSource(dbName);
        JDBCDataSource.setBeanProperty(
            ds, "connectionAttributes", "shutdown=true");
        try {
            ds.getConnection(user, password);
            fail("expected shutdown to fail");
        } catch (SQLException e) {
            // expect 08006 on successful shutdown
            assertSQLState("08006", e);
        }
    }

    // same action as with assertShutdownOK, but using ds.getConnection()
    // instead of ds.getConnection(user, password). So, setting user and
    // password using appropriate ds.set method.
    protected void assertShutdownWOUPOK(
        String dbName, String user, String password)
    throws SQLException {

        DataSource ds = JDBCDataSource.getDataSource(dbName);
        JDBCDataSource.setBeanProperty(ds, "shutdownDatabase", "shutdown");
        JDBCDataSource.setBeanProperty(ds, "user", user);
        JDBCDataSource.setBeanProperty(ds, "password", password);
        try {
            ds.getConnection();
            fail("expected shutdown to fail");
        } catch (SQLException e) {
            // expect 08006 on successful shutdown
            assertSQLState("08006", e);
        }
    }
    
    protected void assertShutdownFail(
        String expectedSqlState, String dbName, String user, String password) 
    throws SQLException
    {
        DataSource ds = JDBCDataSource.getDataSource(dbName);
        JDBCDataSource.setBeanProperty(ds, "shutdownDatabase", "shutdown");
        try {
            ds.getConnection(user, password);
            fail("expected shutdown to fail");
        } catch (SQLException e) {
            assertSQLState(expectedSqlState, e);
        }
    }
    
    protected void assertShutdownOK(
        String dbName, String user, String password)
    throws SQLException
    {
        DataSource ds = JDBCDataSource.getDataSource(dbName);
        JDBCDataSource.setBeanProperty(ds, "shutdownDatabase", "shutdown");
        try {
            ds.getConnection(user, password);
            fail("expected shutdown to fail");
        } catch (SQLException e) {
            assertSQLState("08006", e);
        }
    }

    protected void assertShutdownWOUPFail(
        String expectedSqlState, String dbName, String user, String password) 
    throws SQLException
    {
        DataSource ds = JDBCDataSource.getDataSource(dbName);
        JDBCDataSource.setBeanProperty(ds, "shutdownDatabase", "shutdown");
        JDBCDataSource.setBeanProperty(ds, "user", user);
        JDBCDataSource.setBeanProperty(ds, "password", password);
        try {
            ds.getConnection();
            fail("expected shutdown to fail");
        } catch (SQLException e) {
            assertSQLState(expectedSqlState, e);
        }
    }
    
    protected void assertSystemShutdownOK(
        String dbName, String user, String password)
    throws SQLException {
        DataSource ds;
        if (usingEmbedded())
        {
            // we cannot use JDBCDataSource.getDataSource() (which uses the
            // default database name), unless we specifically clear the 
            // databaseName. Otherwise, only the database will be shutdown.
            // The alternative is to use jDBCDataSource.getDatasource(dbName),
            // where dbName is an empty string - this will in the current code
            // be interpreted as a system shutdown.
            
            ds = JDBCDataSource.getDataSource();
            JDBCDataSource.clearStringBeanProperty(ds, "databaseName");
        }
        else 
        {
            // With client, we cannot user clearStringBeanProperty on the  
            // databaseName, that will result in error 08001 - 
            // Required DataSource property databaseName not set.
            // So, we pass an empty string as databaseName, which the current
            // code interprets as a system shutdown.
            ds = JDBCDataSource.getDataSource(dbName);
        }
        
        JDBCDataSource.setBeanProperty(ds, "shutdownDatabase", "shutdown");
        try {
            ds.getConnection(user, password);
            fail("expected system shutdown resulting in XJ015 error");
        } catch (SQLException e) {
            // expect XJ015, system shutdown, on successful shutdown
            assertSQLState("XJ015", e);
        }
    }

    protected void assertSystemShutdownFail(
        String expectedError, String dbName, String user, String password)
    throws SQLException {
        DataSource ds;
        if (usingEmbedded())
        {
            ds = JDBCDataSource.getDataSource();
            JDBCDataSource.clearStringBeanProperty(ds, "databaseName");
        }
        else
        {
            // note: with network server/client, you can't set the databaseName
            // to null, that results in error 08001 - Required DataSource
            // property databaseName not set.
            // so, we rely on passing of an empty string for databaseName,
            // which in the current code is interpreted as system shutdown.
            ds = JDBCDataSource.getDataSource(dbName);
        }
        JDBCDataSource.setBeanProperty(ds, "shutdownDatabase", "shutdown");
        JDBCDataSource.setBeanProperty(ds, "user", user);
        JDBCDataSource.setBeanProperty(ds, "password", password);
        try {
            ds.getConnection();
            fail("expected shutdown to fail");
        } catch (SQLException e) {
            assertSQLState(expectedError, e);
        }
    }

    public void assertConnectionFail(String dbName) throws SQLException {
        
        // Get the default data source but clear the user and
        // password set by the configuration.
        DataSource ds = JDBCDataSource.getDataSource(dbName);
        
        // Reset to no user/password though client requires
        // a valid name, so reset to the default
        if (usingDerbyNetClient())
            JDBCDataSource.setBeanProperty(ds, "user", "APP");
        else
            JDBCDataSource.clearStringBeanProperty(ds, "user");
        JDBCDataSource.clearStringBeanProperty(ds, "password");
        
        try {
            ds.getConnection();
            fail("expected connection to fail");
        } catch (SQLException e) {
            assertSQLState("08004", e);
        }       
    }
}
