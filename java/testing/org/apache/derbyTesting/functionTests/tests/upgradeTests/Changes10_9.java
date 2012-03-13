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

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import javax.sql.DataSource;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.JDBCDataSource;
import org.apache.derbyTesting.junit.SupportFilesSetup;


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
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // JUnit BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * Return the suite of tests to test the changes made in 10.7.
     * @param phase an integer that indicates the current phase in
     *              the upgrade test.
     * @return the test suite created.
     */
    public static Test suite(int phase) {
        TestSuite suite = new TestSuite("Upgrade test for 10.9");

        suite.addTestSuite(Changes10_9.class);
        return new SupportFilesSetup((Test) suite);
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // TESTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * Make sure that generator-based identity columns don't break upgrade/downgrade.
     */
    public void testIdentity() throws Exception
    {
        Statement s = createStatement();

        switch ( getPhase() )
        {
        case PH_CREATE: // create with old version
            s.execute( "create table t_identity1( a int, b int generated always as identity )" );
            s.execute( "insert into t_identity1( a ) values ( 100 )" );
            vetIdentityValues( s, "t_identity1", 1 );
            break;
            
        case PH_SOFT_UPGRADE: // boot with new version and soft-upgrade
            s.execute( "insert into t_identity1( a ) values ( 200 )" );
            vetIdentityValues( s, "t_identity1", 2 );

            s.execute( "create table t_identity2( a int, b int generated always as identity )" );
            s.execute( "insert into t_identity2( a ) values ( 100 )" );
            vetIdentityValues( s, "t_identity2", 1 );

            break;
            
        case PH_POST_SOFT_UPGRADE: // soft-downgrade: boot with old version after soft-upgrade
            s.execute( "insert into t_identity1( a ) values ( 300 )" );
            vetIdentityValues( s, "t_identity1", 3 );

            s.execute( "insert into t_identity2( a ) values ( 200 )" );
            vetIdentityValues( s, "t_identity2", 2 );

            break;

        case PH_HARD_UPGRADE: // boot with new version and hard-upgrade
            s.execute( "insert into t_identity1( a ) values ( 400 )" );
            vetIdentityValues( s, "t_identity1", 4 );

            s.execute( "insert into t_identity2( a ) values ( 300 )" );
            vetIdentityValues( s, "t_identity2", 3 );

            break;
        }
        
        s.close();
    }
    private void    vetIdentityValues( Statement s, String tableName, int expectedRowCount ) throws Exception
    {
        int     actualRowCount = 0;
        int     lastValue = 0;

        ResultSet   rs = s.executeQuery( "select * from " + tableName + " order by a" );

        while( rs.next() )
        {
            actualRowCount++;
            
            int currentValue = rs.getInt( 2 );
            if ( actualRowCount > 1 )
            {
                assertTrue( currentValue > lastValue );
            }
            lastValue = currentValue;
        }

        assertEquals( expectedRowCount, actualRowCount );
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

        try {
            s.execute( "call syscs_util.syscs_create_user( 'fred', 'fredpassword' )" );
            
            ResultSet   rs = s.executeQuery( "select username from sys.sysusers order by username" );
            rs.next();
            assertEquals( "fred", rs.getString( 1 ) );

            s.execute( "call syscs_util.syscs_reset_password( 'fred', 'fredpassword_rev2' )" );
            
            s.execute( "call syscs_util.syscs_drop_user( 'fred' )" );
            
            rs = s.executeQuery( "select username from sys.sysusers order by username" );
            assertFalse( rs.next() );

            rs.close();

            if ( !shouldExist )
            {
                fail( "syscs_util.syscs_create_user should not exist." );
            }
        } catch (SQLException se )
        {
            if ( shouldExist )
            {
                fail( "Saw unexpected error: " + se.getMessage() );
            }
            assertSQLState( "42Y03", se );
        }

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
}
