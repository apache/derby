/*

Derby - Class org.apache.derbyTesting.functionTests.tests.upgradeTests.Changes10_9

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
package org.apache.derbyTesting.functionTests.tests.upgradeTests;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import javax.sql.DataSource;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.derbyTesting.functionTests.tests.upgradeTests.helpers.DisposableIndexStatistics;
import org.apache.derbyTesting.junit.IndexStatsUtil;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.JDBCDataSource;
import org.apache.derbyTesting.junit.SupportFilesSetup;
import org.apache.derbyTesting.junit.TestConfiguration;


/**
 * Upgrade test cases for 10.9.
 */
public class Changes10_9 extends UpgradeChange
{
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTANTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    private static  final   String  UPGRADE_REQUIRED = "XCL47";
    private static  final   String  INVALID_PROVIDER_CHANGE = "XCY05";

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // STATE
    //
    ///////////////////////////////////////////////////////////////////////////////////

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTRUCTOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

    public Changes10_9(String name)
    {
        super(name);
        initPattern();
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // JUnit BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

    private static final String[] SUPPORT_FILES_SOURCE =
    {
        "functionTests/tests/lang/dcl_java.jar",
        "functionTests/tests/lang/dcl_emc1.jar",
        "functionTests/tests/lang/dcl_emc2.jar",
    };
    
    
    /**
     * Return the suite of tests to test the changes made in 10.7.
     * @param phase an integer that indicates the current phase in
     *              the upgrade test.
     * @return the test suite created.
     */
    public static Test suite(int phase) {
        TestSuite suite = new TestSuite("Upgrade test for 10.9");

        suite.addTestSuite(Changes10_9.class);
        
        return new SupportFilesSetup(
                (Test)suite, SUPPORT_FILES_SOURCE);
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // TESTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * Make sure that the drop statistics procedure only appears after 
     * hard-upgrade.
     */
    public  void    testDropStatisticsProc()  throws Exception
    {
        Statement s = createStatement();

        switch ( getPhase() )
        {
        case PH_CREATE: // create with old version
            s.execute("CREATE TABLE dropStatsT1 (c11 int, c12 int) ");
            vetProcs(s, "call syscs_util.syscs_drop_statistics( 'APP', 'DROPSTATST1', null )", false);
            break;
            
        case PH_SOFT_UPGRADE: // boot with new version and soft-upgrade
            vetProcs(s, "call syscs_util.syscs_drop_statistics( 'APP', 'DROPSTATST1', null )", false);
            break;
            
        case PH_POST_SOFT_UPGRADE: // soft-downgrade: boot with old version after soft-upgrade
            vetProcs(s, "call syscs_util.syscs_drop_statistics( 'APP', 'DROPSTATST1', null )", false);
            break;

        case PH_HARD_UPGRADE: // boot with new version and hard-upgrade
            vetProcs(s, "call syscs_util.syscs_drop_statistics( 'APP', 'DROPSTATST1', null )", true);
            s.execute("DROP TABLE dropStatsT1");
            break;
        }
        
        s.close();
    	
    }
    /**
     * Make sure that the catalogs and procedures for NATIVE authentication
     * only appear after hard-upgrade.
     */
    public  void    testNativeAuthentication()  throws Exception
    {
        Statement s = createStatement();

        switch ( getPhase() )
        {
        case PH_CREATE: // create with old version
            vetSYSUSERS( s, false );
            vetNativeProcs( s, false );
            break;
            
        case PH_SOFT_UPGRADE: // boot with new version and soft-upgrade
            vetSYSUSERS( s, false );
            vetNativeProcs( s, false );
            break;
            
        case PH_POST_SOFT_UPGRADE: // soft-downgrade: boot with old version after soft-upgrade
            vetSYSUSERS( s, false );
            vetNativeProcs( s, false );
            break;

        case PH_HARD_UPGRADE: // boot with new version and hard-upgrade
            vetSYSUSERS( s, true );
            vetNativeProcs( s, true );
            break;
        }
        
        s.close();
    }
    private void    vetProcs( Statement s, String procCall, boolean shouldExist ) throws Exception
    {
        try {
            s.execute( procCall );
            
            if ( !shouldExist )
            {
                fail( "syscs_util.syscs_create_user should not exist." );
            }
        } catch (SQLException se )
        {
            if ( shouldExist )
            {
                assertSQLState( "4251K", se );
            }
            else
            {
                assertSQLState( "42Y03", se );
            }
        }
    }
    private void    vetSYSUSERS( Statement s, boolean shouldExist ) throws Exception
    {
        ResultSet   rs = s.executeQuery( "select count(*) from sys.systables where tablename = 'SYSUSERS'" );
        rs.next();

        int expectedValue = shouldExist ? 1 : 0;

        assertEquals( expectedValue, rs.getInt( 1 ) );

        rs.close();
    }
    private void    vetNativeProcs( Statement s, boolean shouldExist ) throws Exception
    {
        // make sure that an authentication algorithm has been set
        String  defaultDigestAlgorithm = pushAuthenticationAlgorithm( s );
        vetProcs(s, "call syscs_util.syscs_create_user( 'FRED', 'fredpassword' )", shouldExist);
        // restore the authentication algorithm if we changed it
        popAuthenticationAlgorithm( s, defaultDigestAlgorithm );
    }
    private String    pushAuthenticationAlgorithm( Statement s ) throws Exception
    {
        // make sure that an authentication algorithm has been set.
        // otherwise, we won't be able to create NATIVE users.
        String  defaultDigestAlgorithm = getDatabaseProperty( s, "derby.authentication.builtin.algorithm" );
        if ( defaultDigestAlgorithm == null )
        {
            setDatabaseProperty( s, "derby.authentication.builtin.algorithm", "SHA-1" );
        }

        return defaultDigestAlgorithm;
    }
    private void    popAuthenticationAlgorithm( Statement s, String defaultDigestAlgorithm ) throws Exception
    {
        // restore the authentication algorithm if we changed it
        if ( defaultDigestAlgorithm == null )
        {
            setDatabaseProperty( s, "derby.authentication.builtin.algorithm", null );
        }
    }
    private void  setDatabaseProperty( Statement s, String key, String value )
        throws Exception
    {
        if ( value == null ) { value = "cast ( null as varchar( 32672 ) )"; }
        else { value = "'" + value + "'"; }
        String  command = "call syscs_util.syscs_set_database_property( '" + key + "', " + value + " )";

        s.execute( command );
    }
    private String  getDatabaseProperty( Statement s, String key )
        throws Exception
    {
        ResultSet   rs = s.executeQuery( "values( syscs_util.syscs_get_database_property( '" + key + "' ) )" );

        try {
            rs.next();
            return rs.getString( 1 );
        }
        finally
        {
            rs.close();
        }
    }
    
    /**
     * Make sure that NATIVE LOCAL authentication can't be turned on
     * before hard-upgrade.
     */
    public  void    testNativeLocalAuthentication()  throws Exception
    {
        Statement s = createStatement();

        switch ( getPhase() )
        {
        case PH_CREATE: // create with old version
        case PH_POST_SOFT_UPGRADE: // soft-downgrade: boot with old version after soft-upgrade

            //
            // It's possible (although very unlikely) that someone could set the
            // authentication provider to be NATIVE::LOCAL in an old database
            // just before upgrading. If they do this, they will get an error at
            // soft-upgrade time and they will have to back off to the old
            // derby version in order to unset the authentication provider.
            //
            setDatabaseProperty( s, "derby.authentication.provider", "NATIVE::LOCAL" );
            setDatabaseProperty( s, "derby.authentication.provider", null );
            break;
            
        case PH_SOFT_UPGRADE: // boot with new version and soft-upgrade
            setDatabaseProperty( s, "derby.authentication.provider", "com.acme.AcmeAuthenticator" );
            assertStatementError
                (
                 UPGRADE_REQUIRED, s,
                 "call syscs_util.syscs_set_database_property( 'derby.authentication.provider', 'NATIVE::LOCAL' )"
                 );
            setDatabaseProperty( s, "derby.authentication.provider", null );
            break;
            
        case PH_HARD_UPGRADE: // boot with new version and hard-upgrade
            //
            // Can't actually turn on NATIVE LOCAL authentication in the upgrade tests because, once turned on,
            // you can't turn it off and that would mess up later tests. 
            //
            break;
        }
        
        s.close();
    }

    /**
     * Make sure builtin authentication doesn't use a hash scheme that's not
     * supported by the old version until the database has been hard upgraded.
     * See DERBY-4483 and DERBY-5539.
     */
    public void testBuiltinAuthenticationWithConfigurableHash()
            throws SQLException {

        // This test needs to enable authentication, which is not supported
        // in the default database for the upgrade tests, so roll our own.
        DataSource ds = JDBCDataSource.getDataSourceLogical("BUILTIN_10_9");

        // Add create=true or upgrade=true, as appropriate, since we don't
        // get this for free when we don't use the default database.
        if (getPhase() == PH_CREATE) {
            JDBCDataSource.setBeanProperty(ds, "createDatabase", "create");
        } else if (getPhase() == PH_HARD_UPGRADE) {
            JDBCDataSource.setBeanProperty(
                    ds, "connectionAttributes", "upgrade=true");
        }

        // Connect as database owner, possibly creating or upgrading the
        // database.
        Connection c = ds.getConnection("dbo", "the boss");

        // Let's first verify that all the users can connect after the changes
        // in the previous phase. Would fail for instance in post soft upgrade
        // if soft upgrade saved passwords using the new scheme.
        verifyCanConnect(ds);

        CallableStatement setProp = c.prepareCall(
                "call syscs_util.syscs_set_database_property(?, ?)");

        if (getPhase() == PH_CREATE) {
            // The database is being created. Make sure that builtin
            // authentication is enabled.
            setProp.setString(1, "derby.connection.requireAuthentication");
            setProp.setString(2, "true");
            setProp.execute();

            setProp.setString(1, "derby.authentication.provider");
            setProp.setString(2, "BUILTIN");
            setProp.execute();

            // Set the length of the random salt to 0 to ensure that the
            // hashed token doesn't vary between test runs.
            setProp.setString(1, "derby.authentication.builtin.saltLength");
            setProp.setInt(2, 0);
            setProp.execute();
        }

        // Set (or reset) passwords for all users.
        setPasswords(setProp);
        setProp.close();

        // We should still be able to connect.
        verifyCanConnect(ds);

        // Check that the passwords are stored using the expected scheme (new
        // configurable hash scheme in hard upgrade, old scheme otherwise).
        verifyPasswords(c);

        c.close();

        // The framework doesn't know how to shutdown a database using
        // authentication, so do it manually as database owner here.
        JDBCDataSource.setBeanProperty(ds, "user", "dbo");
        JDBCDataSource.setBeanProperty(ds, "password", "the boss");
        JDBCDataSource.shutdownDatabase(ds);
    }

    /**
     * Information about users for the test of builtin authentication with
     * configurable hash algorithm. Two-dimensional array of strings where
     * each row contains (1) a user name, (2) a password, (3) the name of a
     * digest algorithm with which the password should be hashed, (4) the
     * hashed password when the old scheme is used, (5) the hashed password
     * when the new, configurable hash scheme is used in databases that
     * don't support the key-stretching extension (DERBY-5539), and (6) the
     * hashed password when configurable hash with key stretching is used.
     */
    private static final String[][] USERS = {
        { "dbo", "the boss", null,
                  "3b6071d99b1d48ab732e75a8de701b6c77632db65898",
                  "3b6071d99b1d48ab732e75a8de701b6c77632db65898",
                  "3b6071d99b1d48ab732e75a8de701b6c77632db65898",
        },
        { "pat", "postman", "MD5",
                  "3b609129e181a7f7527697235c8aead65c461a0257f3",
                  "3b61aaca567ed43d1ba2e6402cbf1a723407:MD5",
                  "3b624f4b0d7f3d2330c1db98a2000c62b5cd::1000:MD5",
        },
        { "sam", "fireman", "SHA-1",
                  "3b609e5173cfa03620061518adc92f2a58c7b15cf04f",
                  "3b6197160362c0122fcd7a63a9da58fd0781140901fb:SHA-1",
                  "3b62a2d88ffac5332219116ab53e29dd3b9e1222e990::1000:SHA-1",
        },
    };

    /**
     * Set the passwords for all users specified in {@code USERS}.
     *
     * @param cs a callable statement that sets database properties
     */
    private void setPasswords(CallableStatement cs) throws SQLException {
        for (int i = 0; i < USERS.length; i++) {
            // Use the specified algorithm, if possible. (Will be ignored if
            // the data dictionary doesn't support the new scheme.)
            cs.setString(1, Changes10_6.HASH_ALGORITHM_PROPERTY);
            cs.setString(2, USERS[i][2]);
            cs.execute();
            // Set the password.
            cs.setString(1, "derby.user." + USERS[i][0]);
            cs.setString(2, USERS[i][1]);
            cs.execute();
        }
    }

    /**
     * Verify that all passwords for the users in {@code USERS} are stored
     * as expected. Raise an assert failure on mismatch.
     *
     * @param c a connection to the database
     */
    private void verifyPasswords(Connection c)
            throws SQLException {
        int pwIdx;
        if (getPhase() == PH_HARD_UPGRADE) {
            // Expect configurable hash scheme with key stretching in fully
            // upgraded databases.
            pwIdx = 5;
        } else if (oldAtLeast(10, 6)) {
            // Databases whose dictionary is at least version 10.6 support
            // configurable hash without key stretching.
            pwIdx = 4;
        } else {
            // Older databases only support the old scheme based on SHA-1.
            pwIdx = 3;
        }
        PreparedStatement ps = c.prepareStatement(
                "values syscs_util.syscs_get_database_property(?)");
        for (int i = 0; i < USERS.length; i++) {
            String expectedToken = USERS[i][pwIdx];
            ps.setString(1, "derby.user." + USERS[i][0]);
            JDBC.assertSingleValueResultSet(ps.executeQuery(), expectedToken);
        }
        ps.close();
    }

    /**
     * Verify that all users specified in {@code USERS} can connect to the
     * database.
     *
     * @param ds a data source for connecting to the database
     * @throws SQLException if one of the users cannot connect to the database
     */
    private void verifyCanConnect(DataSource ds) throws SQLException {
        for (int i = 0; i < USERS.length; i++) {
            Connection c = ds.getConnection(USERS[i][0], USERS[i][1]);
            c.close();
        }
    }


    /**
     * For 10.9 and later storage of jar files changed. DERBY-5357.
     */
    public void testJarStorage()  throws Exception
    {
        Statement s = createStatement();

        switch (getPhase()) {
        case PH_CREATE: // create with old version
            createSchema("EMC");
            createSchema("FOO");

            s.executeUpdate(
                "create procedure EMC.ADDCONTACT(id INT, e_mail VARCHAR(30)) " +
                "MODIFIES SQL DATA " +
                "external name " +
                "'org.apache.derbyTesting.databaseclassloader.emc.addContact'" +
                " language java parameter style java");
            s.executeUpdate(
                "create table EMC.CONTACTS " +
                "    (id int, e_mail varchar(30))");

            installJar("dcl_emc1.jar", "EMC.MAIL_APP");
            installJar("dcl_java.jar", "EMC.MY_JAVA");
            installJar("dcl_emc2.jar", "FOO.BAR");

            setDBClasspath("EMC.MAIL_APP");
            tryCall();
            setDBClasspath(null);

            break;

        case PH_SOFT_UPGRADE:
            // boot with new version and soft-upgrade

            // DERBY-6505: Take a backup of the database. Backing up the
            // jar storage takes a different path in soft upgrade, so make
            // sure that path is exercised.
            String backupdir =
                SupportFilesSetup.getReadWriteFileName("d6505-backup");
            PreparedStatement backupStmt =
                prepareStatement("call syscs_util.syscs_backup_database(?)");
            backupStmt.setString(1, backupdir);
            backupStmt.execute();

            // fallthrough... run the same tests as in post soft upgrade.
        case PH_POST_SOFT_UPGRADE:
            // soft-downgrade: boot with old version after soft-upgrade

            setDBClasspath("EMC.MAIL_APP");
            tryCall();
            setDBClasspath(null);
            
            // if we can do this, it hasn't moved already:
            replaceJar("dcl_emc1.jar", "EMC.MAIL_APP");

            setDBClasspath("EMC.MAIL_APP");
            tryCall();
            setDBClasspath(null);

            break;
            
        case PH_HARD_UPGRADE: // boot with new version and hard-upgrade

            setDBClasspath("EMC.MAIL_APP");
            tryCall();
            setDBClasspath(null);

            installJar("dcl_emc1.jar", "FOO.\"BAR/..\\../\"");

            verifyNewLocations(4);
            
            removeJar("EMC.MAIL_APP");
            installJar("dcl_emc1.jar", "EMC.MAIL_APP");
            
            setDBClasspath("EMC.MAIL_APP");
            tryCall();
            setDBClasspath(null);
            
            // finally, check that all the rest are also here
            replaceJar("dcl_java.jar", "EMC.MY_JAVA");
            replaceJar("dcl_emc2.jar", "FOO.BAR");
            replaceJar("dcl_emc1.jar", "FOO.\"BAR/..\\../\"");

            // clean up
            removeJar("EMC.MY_JAVA");
            removeJar("FOO.BAR");
            removeJar("FOO.\"BAR/..\\../\"");
            removeJar("EMC.MAIL_APP");
            s.executeUpdate("drop table EMC.CONTACTS");
            s.executeUpdate("drop procedure EMC.ADDCONTACT");
            s.executeUpdate("drop schema FOO restrict");
            s.executeUpdate("drop schema EMC restrict");

            break;
        }
        
        s.close();
    }

    private void createSchema(String name) throws SQLException {
        Statement s = createStatement();
        s.executeUpdate("create schema " + name);
        s.close();
    }

    private void installJar(String resource, String jarName)
            throws SQLException, MalformedURLException {        

        URL jar = SupportFilesSetup.getReadOnlyURL(resource);
        
        CallableStatement cs = prepareCall("CALL SQLJ.INSTALL_JAR(?, ?, 0)");
        cs.setString(1, jar.toExternalForm());
        cs.setString(2, jarName);
        cs.executeUpdate();
        cs.close();
    }
    
    private void replaceJar(String resource, String jarName)
            throws SQLException, MalformedURLException {        

        URL jar = SupportFilesSetup.getReadOnlyURL(resource);
        CallableStatement cs = prepareCall("CALL SQLJ.REPLACE_JAR(?, ?)");
        cs.setString(1, jar.toExternalForm());
        cs.setString(2, jarName);
        cs.executeUpdate();
        cs.close();
    }
    
    private void removeJar(String jarName) throws SQLException {
        CallableStatement cs = prepareCall("CALL SQLJ.REMOVE_JAR(?, 0)");       
        cs.setString(1, jarName);       
        cs.executeUpdate();        
        cs.close();
    }

    private void setDBClasspath(String cp) throws SQLException {
        CallableStatement cs = prepareCall(
          "CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(" +
          "'derby.database.classpath', ?)");

        cs.setString(1, cp);
        cs.executeUpdate();
        cs.close();
    }

    private void tryCall() throws SQLException {
        if (JDBC.vmSupportsJSR169()) {
            return; // skip, EMC uses DriverManager
        }

        CallableStatement cs = prepareCall("CALL EMC.ADDCONTACT(?, ?)");
        cs.setInt(1, 0);
        cs.setString(2, "now@classpathchange.com");
        cs.executeUpdate();
        cs.close();
    }

    private void verifyNewLocations(int noOfObjects)
            throws SQLException {
        TestConfiguration tc = TestConfiguration.getCurrent();
        String dbPath = tc.getPhysicalDatabaseName(tc.getDefaultDatabaseName());
        String jarDirName =
            "system" + File.separator + dbPath + File.separator + "jar";
        File jarDir = new File(jarDirName);

        assertTrue(jarDir.isDirectory());

        File[] contents = jarDir.listFiles();
        
        // <db>/jar should now contain this no of files, none of which are
        // directories
        assertEquals(noOfObjects, contents.length);
        
        // assert that all the old style directories are gone
        for (int i=0; i < contents.length; i++) {
            File f = contents[i];
            assertTrue(f.isFile());
            assertFileNameShape(f.getName());
        }
    }


    /**
     * Regexp pattern to match the file name of a jar file stored in the
     * database (version >= 10.9).
     */
    private Goal[] pattern;
    
    /**
     * Initialize a pattern corresponding to:
     * <p/>
     * &lt;Derby uuid string&gt;[.]jar[.]G[0-9]+
     * <p/>
     * where:
     * <p/>
     * &lt;Derby uuid string&gt; has the form
     * hhhhhhhh-hhhh-hhhh-hhhh-hhhhhhhhhhhh
     * <p/>
     * where <em>h</em> id a lower case hex digit.
     */
    private void initPattern() {
        List<Goal> l = new ArrayList<Goal>(100);
        // The UUID format is determined by
        // org.apache.derby.impl.services.uuid.BasicUUID#toString

        for (int i=0; i < 8; i++) {
            l.add(new CharRange(new char[][]{{'0','9'},{'a','f'}}));
        }

        l.add(new SingleChar('-'));
        
        for (int j = 0; j < 3; j++) {
            for (int i=0; i < 4; i++) {
                l.add(new CharRange(new char[][]{{'0','9'},{'a','f'}}));
            }
            
            l.add(new SingleChar('-'));
        }
        
        for (int i=0; i < 12; i++) {
            l.add(new CharRange(new char[][]{{'0','9'},{'a','f'}}));
        }
        
        l.add(new SingleChar('.'));
        l.add(new SingleChar('j'));
        l.add(new SingleChar('a'));
        l.add(new SingleChar('r'));
        l.add(new SingleChar('.'));
        l.add(new SingleChar('G'));
        l.add(new CharRange(new char[][]{{'0','9'}}, Goal.REPEAT));

        this.pattern = l.toArray(new Goal[l.size()]);
    }

    /**
     * assert that fName has the expected shape of a jar file
     * in the database (version >= 10.9).
     */
    private void assertFileNameShape(String fName) {
        assertTrue(matches(fName, pattern));
    }
    
    /**
     * Poor man's regexp matcher: can match patterns of type below, where
     * start "^" and end "$" is implied: must match whole string.
     * <p/>
     * reg.exp: ( '[' &lt;fromchar&gt;-&lt;tochar&gt; ] '+'? ']' |
     *            &lt;char&gt; '+'? )*
     */
    private boolean matches(String fName, Goal[] pattern) {
        int patIdx = 0;
        for (int i = 0; i < fName.length(); i++) {
            Goal p = pattern[patIdx];
            char c = fName.charAt(i);

            if (p.matches(c)) {
                if (!p.isRepeatable()) {
                    patIdx++;
                } 
                p.setFoundOnce();
                continue;
            } 
                
            // Goal did not match: if we have a repeatable goal and we already
            // found one occurence it's ok, to step on to next goal in pattern
            // and see it that matches.
            patIdx++;
            if (p.matches(c)) {
                if (!p.isRepeatable()) {
                    patIdx++;
                } 
                p.setFoundOnce();
                continue;
            }

            return false;
            
        }
        
        return patIdx >= (pattern.length - 1); // exact match
    }
    
    abstract class Goal {
        public abstract boolean matches(char c);
        
        public final static int REPEAT = 0; // optional goal property
        int option = -1;
        boolean foundOnce = false;

        public boolean isRepeatable () {
            return option == REPEAT;
        }
        
        public void setFoundOnce() {
            this.foundOnce = true;
        }
        
        public boolean foundOnce () {
            return this.foundOnce;
        }
    }

    private class CharRange extends Goal {
        private char[][] ranges;
        
        public CharRange(char[][]ranges) {
            this.ranges = (char[][])ranges.clone();
        }
        
        public CharRange(char[][]ranges, int option) {
            this.ranges = (char[][])ranges.clone();
            this.option = option;
        }
        
        public boolean matches(char c) {
            for (int i = 0; i < ranges.length; i++) {
                if (c >= ranges[i][0] && c <= ranges[i][1]) {
                    return true;
                }
            }
            return false;
        }
    }

    private class SingleChar extends Goal {
        private char c;
        private int option = -1;
        private boolean foundOnce = false;
        
        public SingleChar(char c) {
            this.c = c;
        }
    
        public SingleChar(char c, int option) {
            this.c = c;
            this.option = option;
        }
        public boolean matches(char c) {
            return c == this.c;
        }
    }
    
    
    /**
     * Test the changes introduced to fix correctness problems with sequences.
     */
    public  void    test_5493()  throws Exception
    {
        Connection  conn = getConnection();
        Statement s = createStatement();

        switch ( getPhase() )
        {
        case PH_CREATE: // create with old version
            assertNull( getNewFunctionID( s ) );
            break;
            
        case PH_SOFT_UPGRADE: // boot with new version and soft-upgrade
            assertNull( getNewFunctionID( s ) );
            break;
            
        case PH_POST_SOFT_UPGRADE: // soft-downgrade: boot with old version after soft-upgrade
            assertNull( getNewFunctionID( s ) );
            break;

        case PH_HARD_UPGRADE: // boot with new version and hard-upgrade
            assertNotNull( getNewFunctionID( s ) );
            break;
        }
        
        s.close();
    }
    private String    getNewFunctionID( Statement s )
        throws Exception
    {
        ResultSet   rs = null;

        try {
            rs = s.executeQuery
            ( "select aliasid from sys.sysaliases where alias = 'SYSCS_PEEK_AT_SEQUENCE'" );
            if ( !rs.next() ) { return null; }
            else { return rs.getString( 1 ); }
        }
        finally
        {
            if ( rs != null ) { rs.close(); }
        }
    }

    /**
     * Verifies that an orphaned statistics entry can be dropped by running the
     * {@code SYSCS_DROP_STATISTICS} system procedure.
     * <p>
     * Relevant JIRAs:
     * <ul> <li>DERBY-4115: Provide a way to drop statistics information</li>
     *      <li>DERBY-5681: When a foreign key constraint on a table is dropped,
     *          the associated statistics row for the conglomerate</li>
     * </ul>
     * <p>
     *  DERBY-5702(Creating a foreign key constraint does not automatically
     *   create a statistics row if foreign key constraint will share a
     *   backing index created for a primay key) is causing a problem for
     *   us to test the hanging statistics row with 10.4 and prior releases.
     *   Following test relies on having hanging statistics rows which should
     *   have been dropped when the constraint owing it was dropped. The test
     *   then goes ahead and uses the new drop statisitcs procedure to drop
     *   the hanging statistics rows. But because of DERBY-5702, when a
     *   constraint is added which will reuse an existing backing index,
     *   no statistics row is created for that constraint unless a user were
     *   to say use an update statistics stored procedure to create the
     *   statistics for that constraint. And later when that constraint is
     *   dropped, we will find that because of DERBY-5681, the statistics
     *   row never gets dropped. But update statistics stored procedure was
     *   not introduced up until 10.5 and because of that, we can't really
     *   test for hanging index created through constraints sharing the same
     *   backing index prior to 10.5
     */
    public void testDropOrphanedStatistics()
            throws SQLException {
        // Update statistics procedure SYSCS_UPDATE_STATISTICS is not available
        //  prior to 10.5 and hence we can't cause the hanging statistics to 
        //  appear in order to test the drop statistics after hard upgrade
        if (!oldAtLeast(10, 5)) return;

        // Helper object to obtain information about index statistics.
        IndexStatsUtil stats = new IndexStatsUtil(openDefaultConnection());
        Statement s = createStatement();
        // The expected initial number of statistics entries in TEST_TAB_2.
        final int expected =
                DisposableIndexStatistics.hasDerby5681Bug(getOldVersion()) ?
                    2 : 1;
        
        switch (getPhase())
        {
        case PH_CREATE:
            s.executeUpdate("CREATE TABLE TEST_TAB_1 (c11 int not null,"+
                    "c12 int not null, c13 int)");
            s.executeUpdate("INSERT INTO TEST_TAB_1 VALUES(1,1,1),(2,2,2)");
            s.executeUpdate("ALTER TABLE TEST_TAB_1 "+
                    "ADD CONSTRAINT TEST_TAB_1_PK_1 "+
                    "PRIMARY KEY (c11)");
            //The statistics for primary key constraint has been added
            stats.assertTableStats("TEST_TAB_1",1);
            
            s.executeUpdate("CREATE TABLE TEST_TAB_2 (c21 int not null)");
            s.executeUpdate("INSERT INTO TEST_TAB_2 VALUES(1),(2)");
            s.executeUpdate("ALTER TABLE TEST_TAB_2 "+
                    "ADD CONSTRAINT TEST_TAB_2_PK_1 "+
                    "PRIMARY KEY (c21)");
            stats.assertTableStats("TEST_TAB_2",1);
            //DERBY-5702 Add a foreign key constraint and now we should find 2 rows
            // of statistics for TEST_TAB_2 - 1 for primary key and other for
            // foreign key constraint
            s.executeUpdate("ALTER TABLE TEST_TAB_2 "+
                    "ADD CONSTRAINT TEST_TAB_2_FK_1 "+
                    "FOREIGN KEY(c21) REFERENCES TEST_TAB_1(c11)");
            //DERBY-5702 Like primary key earlier, adding foreign key constraint
            // didn't automatically add a statistics row for it. Have to run update
            // statistics manually to get a row added for it's stat
            stats.assertTableStats("TEST_TAB_2",1);
            //Need to do a compress table to create the statistics for foreign
            // key constraint. Update statisitcs procedure is only available
            // in 10.5 and upwards and hence can't use that procedure here
            // since we are testing older releases too.
            s.execute("CALL SYSCS_UTIL.SYSCS_UPDATE_STATISTICS('APP','TEST_TAB_2', null)");
            //s.execute("CALL SYSCS_UTIL.SYSCS_COMPRESS_TABLE('APP','TEST_TAB_2',1)");
            stats.assertTableStats("TEST_TAB_2",2);
            s.executeUpdate("ALTER TABLE TEST_TAB_2 "+
                    "DROP CONSTRAINT TEST_TAB_2_FK_1");
            //Dropping the foreign key constraint does not remove it's 
            // statistics row because of DERBY-5681.
            stats.assertTableStats("TEST_TAB_2", expected);
            assertStatementError("42Y03", s,
            "CALL SYSCS_UTIL.SYSCS_DROP_STATISTICS('APP','TEST_TAB_2', null)");
            break;

        case PH_SOFT_UPGRADE:
        case PH_POST_SOFT_UPGRADE:
            assertStatementError("42Y03", s,
                       "CALL SYSCS_UTIL.SYSCS_DROP_STATISTICS('APP','TEST_TAB_2', null)");
            break;

        case PH_HARD_UPGRADE:
            stats.assertTableStats("TEST_TAB_2", expected);
            s.execute("CALL SYSCS_UTIL.SYSCS_DROP_STATISTICS('APP','TEST_TAB_2', null)");
            stats.assertNoStatsTable("TEST_TAB_2");
            s.execute("CALL SYSCS_UTIL.SYSCS_UPDATE_STATISTICS('APP','TEST_TAB_2', null)");
            stats.assertNoStatsTable("TEST_TAB_2");
            break;

        case PH_POST_HARD_UPGRADE:
            //Make sure that the new procedure is still available
            s.execute("CALL SYSCS_UTIL.SYSCS_DROP_STATISTICS('APP','TEST_TAB_2', null)");
            s.executeUpdate("DROP TABLE TEST_TAB_1");
            s.executeUpdate("DROP TABLE TEST_TAB_2");
            break;
        }
    }

    /**
     * Verifies the behavior of the update statistics code when faced with
     * "disposable statistics entries".
     * <p>
     * A disposable statistics entry is a row in SYS.SYSSTATISTICS that has
     * been orphaned (see DERBY-5681) or it is on longer needed by the
     * Derby optimizer (due to internal changes/improvements).
     * <p>
     * This test expects different things based on the phase:
     * <dl> <dt>create</dt>
     *      <dd>- run statements that will cause disposable statistics
     *          entries to be created</dd>
     *      <dt>soft upgrade</dt>
     *      <dd>- run the new update statistics code, expecting it to leave the
     *          disposable statistics intact</dd>
     *      <dt>downgrade</dt>
     *      <dd>- verify that the relevant statistics are present</dd>
     *      <dt>hard upgrade</dt>
     *      <dd>- run the new update statistics code, expecting it to get rid
     *          of the disposable statistics</dd>
     * </dl>
     */
    public void testDisposableStatisticsExplicit()
            throws SQLException {
        // Don't run this test with versions prior to 10.5, since the
        // required SYSCS_UPDATE_STATISTICS don't exist in older versions.
        if (!oldAtLeast(10, 5)) {
            return;
        }

        final String TBL = "ISTAT_DISPOSABLE_STATS";
        String updateStatsSQL = "call syscs_util.syscs_update_statistics(" +
                "'APP', ?, null)";
        DisposableIndexStatistics dis = new DisposableIndexStatistics(
                getOldVersion(), getConnection(), TBL);

        switch (getPhase()) {
            // create with old version
            case PH_CREATE:
            {
                dis.createAndPopulateTables();
                // The expected number of statistics entries depends on the
                // version of Derby used to create the database. Some older
                // versions of Derby contained a bug and lacked optimizations,
                // causing the number of statistics entries to increase.
                // Just after creation and before any update statistics expect
                // all stats to exist.
                dis.assertStatsCount(false, false);
                break;
            }
            // boot with new version and soft-upgrade
            case PH_SOFT_UPGRADE:
            {
                PreparedStatement ps = prepareStatement(updateStatsSQL);
                String[] tables = dis.getTableNames();
                // Update statistics on all relevant tables.
                for (int i=0; i < tables.length; i++) {
                    ps.setString(1, tables[i]);
                    ps.executeUpdate();
                }

                // After soft upgrade and update statistics expect the 
                // orphaned index entry to be deleted, but the "unneeded
                // disposable entries" are only deleted after hard upgrade.
                dis.assertStatsCount(true, false);
                break;
            }
            // soft-downgrade: boot with old version after soft-upgrade
            case PH_POST_SOFT_UPGRADE:
            {

                // expect no change in entries on downgrade, should be same
                // as they were in soft upgrade.
                dis.assertStatsCount(true, false);
                break;
            }
            // boot with new version and hard-upgrade
            case PH_HARD_UPGRADE:
            {
                // expect no change in entries on upgrade before update
                // statistics.
                dis.assertStatsCount(true, false);
                PreparedStatement ps = prepareStatement(updateStatsSQL);
                String[] tables = dis.getTableNames();
                for (int i=0; i < tables.length; i++) {
                    ps.setString(1, tables[i]);
                    ps.executeUpdate();
                }
                // Confirm that we disposed of the statistics that were added
                // due to a bug or simply not needed by Derby.
                try {
                    dis.assertStatsCount(true, true);
                } finally {
                    for (int i=0; i < tables.length; i++) {
                        dropTable(tables[i]);
                    }
                }
                commit();
                break;
            }
        }
    }
}
