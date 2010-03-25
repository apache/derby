/*

Derby - Class org.apache.derbyTesting.functionTests.tests.upgradeTests.Changes10_6

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

import org.apache.derbyTesting.junit.SupportFilesSetup;

import org.apache.derbyTesting.junit.JDBCDataSource;
import java.lang.reflect.Method;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Connection;
import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import javax.sql.DataSource;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.derby.catalog.types.RoutineAliasInfo;
import org.apache.derby.catalog.TypeDescriptor;
import org.apache.derbyTesting.junit.JDBC;


/**
 * Upgrade test cases for 10.6.
 * If the old version is 10.6 or later then these tests
 * will not be run.
 * <BR>
    10.6 Upgrade issues

    <UL>
    <LI> testSetXplainSchemaProcedure - DERBY-2487
    Make sure that SYSCS_UTIL.SYSCS_SET_XPLAIN_SCHEMA can only be run in Derby
    10.5 and higher.
    </UL>

 */
public class Changes10_6 extends UpgradeChange {

    private static  final   String  BAD_SYNTAX = "42X01";
    private static  final   String  TABLE_DOES_NOT_EXIST = "42X05";
    private static  final   String  UPGRADE_REQUIRED = "XCL47";

    private static  final   String  QUERY_4215 =
        "select r.grantor\n" +
        "from sys.sysroutineperms r, sys.sysaliases a\n" +
        "where r.aliasid = a.aliasid\n" +
        "and a.alias = 'SYSCS_INPLACE_COMPRESS_TABLE'\n"
        ;

    private static final   String CREATE_TYPE_DDL = "create type fooType external name 'mypackage.foo' language java\n";
    private static final   String DROP_TYPE_DDL = "drop type fooType restrict\n";

    private static final String HASH_ALGORITHM_PROPERTY =
            "derby.authentication.builtin.algorithm";

    public Changes10_6(String name) {
        super(name);
    }

    /**
     * Return the suite of tests to test the changes made in 10.6.
     * @param phase an integer that indicates the current phase in
     *              the upgrade test.
     * @return the test suite created.
     */
    public static Test suite(int phase) {
        TestSuite suite = new TestSuite("Upgrade test for 10.6");

        suite.addTestSuite(Changes10_6.class);
        return new SupportFilesSetup((Test) suite);
    }


    /**
     * Make sure that SYSCS_UTIL.SYSCS_SET_XPLAIN_STYLE can only be run in 
     * Derby 10.5 and higher. 
     * DERBY-2487
     * Test added for 10.5.
     * @throws SQLException
     *
     */
    public void testSetXplainStyleProcedure() throws SQLException
    {
        String []xplainProcedures = {
            "call SYSCS_UTIL.SYSCS_SET_XPLAIN_SCHEMA('XPLAIN')",
            "call SYSCS_UTIL.SYSCS_SET_XPLAIN_SCHEMA('')",
            "call SYSCS_UTIL.SYSCS_SET_XPLAIN_MODE(1)",
            "call SYSCS_UTIL.SYSCS_SET_XPLAIN_MODE(0)",
            "values SYSCS_UTIL.SYSCS_GET_XPLAIN_SCHEMA()",
            "values SYSCS_UTIL.SYSCS_GET_XPLAIN_MODE()",
        };
    	Statement s;
        //ERROR 42Y03: 'SYSCS_UTIL.SYSCS_SET_XPLAIN_MODE' is not
        // recognized as a function or procedure.
        switch (getPhase())
        {
        case PH_SOFT_UPGRADE: // In soft-upgrade cases, XPLAIN should fail:
        case PH_POST_SOFT_UPGRADE:
            s = createStatement();
            for (int i = 0; i < xplainProcedures.length; i++)
                assertStatementError("42Y03", s, xplainProcedures[i]);
            s.close();
            break;

        case PH_HARD_UPGRADE: // After hard upgrade, XPLAIN should work:
            s = createStatement();
            for (int i = 0; i < xplainProcedures.length; i++)
                s.execute(xplainProcedures[i]);
            s.close();
            break;
        }
    }

    /**
     * Make sure that SYSIBM.CLOBGETSUBSTRING has the correct return value.
     * See https://issues.apache.org/jira/browse/DERBY-4214
     */
    public void testCLOBGETSUBSTRING() throws Exception
    {
        Version initialVersion = new Version( getOldMajor(), getOldMinor(), 0, 0 );
        Version firstVersionHavingThisFunction = new Version( 10, 3, 0, 0 );
        Version firstVersionHavingCorrectReturnType = new Version( 10, 5, 0, 0 );
        int     wrongLength = 32672;
        int     correctLength = 10890;
        int     actualJdbcType;
        int     actualLength;
        
        Object   returnType;

        boolean hasFunction = initialVersion.compareTo( firstVersionHavingThisFunction ) >= 0;
        boolean hasCorrectReturnType = initialVersion.compareTo( firstVersionHavingCorrectReturnType ) >= 0;
        
    	Statement s = createStatement();
        ResultSet rs = s.executeQuery
            (
             "select a.aliasinfo\n" +
             "from sys.sysschemas s, sys.sysaliases a\n" +
             "where s.schemaid = a.schemaid\n" +
             "and s.schemaname = 'SYSIBM'\n" +
             "and alias = 'CLOBGETSUBSTRING'\n"
             );
        rs.next();
        
        switch (getPhase())
        {
        case PH_CREATE:
        case PH_SOFT_UPGRADE:
        case PH_POST_SOFT_UPGRADE:
            
            if ( !hasFunction ) { break; }

            returnType = getTypeDescriptor( rs.getObject( 1 ) );
            actualJdbcType = getJDBCTypeId( returnType );
            actualLength = getMaximumWidth( returnType );
            int              expectedLength = hasCorrectReturnType ? correctLength : wrongLength;

            assertEquals( java.sql.Types.VARCHAR, actualJdbcType );
            assertEquals( expectedLength, actualLength );
            
            break;

        case PH_HARD_UPGRADE:

            RoutineAliasInfo rai = (RoutineAliasInfo) rs.getObject( 1 );
            TypeDescriptor   td = (TypeDescriptor) rai.getReturnType();

            assertEquals( java.sql.Types.VARCHAR, td.getJDBCTypeId() );
            assertEquals( correctLength, td.getMaximumWidth() );
            
            break;
        }

        rs.close();
        s.close();
    }

    /**
     * Make sure that SYSCS_UTIL.SYSCS_INPLACE_COMPRESS_TABLE  has the correct
     * permissons granted to it.
     * See https://issues.apache.org/jira/browse/DERBY-4215
     */
    public void testSYSCS_INPLACE_COMPRESS_TABLE() throws Exception
    {
        Version initialVersion = new Version( getOldMajor(), getOldMinor(), 0, 0 );
        Version firstVersionHavingPermissions = new Version( 10, 2, 0, 0 );
        boolean beforePermissionsWereAdded = ( initialVersion.compareTo( firstVersionHavingPermissions ) < 0 );
        
    	Statement s = createStatement();
        
        switch (getPhase())
        {
        case PH_CREATE:
        case PH_SOFT_UPGRADE:
        case PH_POST_SOFT_UPGRADE:
            
            if ( beforePermissionsWereAdded )
            {
                assertStatementError( TABLE_DOES_NOT_EXIST, s, QUERY_4215 );
            }
            else
            {
                vetDERBY_4215( s );
            }

            break;

        case PH_HARD_UPGRADE:

            vetDERBY_4215( s );
            
            break;
        }

        s.close();
    }
    /**
     * Vet the permissions on SYSCS_UTIL.SYSCS_INPLACE_COMPRESS_TABLE.
     * There should be only one permissions tuple for this system procedure and
     * the grantor should be APP.
     */
    private void vetDERBY_4215( Statement s ) throws Exception
    {
        String    expectedGrantor = "APP";
        ResultSet rs = s.executeQuery( QUERY_4215 );

        assertTrue( rs.next() );

        String actualGrantor = rs.getString( 1 );
        assertEquals( expectedGrantor, actualGrantor );

        assertFalse( rs.next() );

        rs.close();
    }

    /**
     * Make sure that you can only create UDTs in a hard-upgraded database.
     * See https://issues.apache.org/jira/browse/DERBY-651
     */
    public void testUDTs() throws Exception
    {        
    	Statement s = createStatement();

        int phase = getPhase();

        //println( "Phase = " + phase );
        
        switch ( phase )
        {
        case PH_CREATE:
        case PH_POST_SOFT_UPGRADE:
            
            assertStatementError( BAD_SYNTAX, s, CREATE_TYPE_DDL );
            assertStatementError( BAD_SYNTAX, s, DROP_TYPE_DDL );
            
            break;

        case PH_SOFT_UPGRADE:

            assertStatementError( UPGRADE_REQUIRED, s, CREATE_TYPE_DDL );
            assertStatementError( UPGRADE_REQUIRED, s, DROP_TYPE_DDL );
            
            break;

        case PH_HARD_UPGRADE:

            s.execute( CREATE_TYPE_DDL );
            s.execute( DROP_TYPE_DDL );
            
            break;
        }

        s.close();
    }

    
    /**
     * We would like to just cast the alias descriptor to
     * RoutineAliasDescriptor. However, this doesn't work if we are running on
     * an old version because the descriptor comes from a different class
     * loader. We use reflection to get the information we need.
     */
    private Object getTypeDescriptor( Object routineAliasDescriptor )
        throws Exception
    {
        Method  meth = routineAliasDescriptor.getClass().getMethod( "getReturnType", null );

        return meth.invoke( routineAliasDescriptor, null );
    }
    private int getJDBCTypeId( Object typeDescriptor )
        throws Exception
    {
        Method  meth = typeDescriptor.getClass().getMethod( "getJDBCTypeId", null );

        return ((Integer) meth.invoke( typeDescriptor, null )).intValue();
    }
    private int getMaximumWidth( Object typeDescriptor )
        throws Exception
    {
        Method  meth = typeDescriptor.getClass().getMethod( "getMaximumWidth", null );

        return ((Integer) meth.invoke( typeDescriptor, null )).intValue();
    }

    /**
     * Verify that we don't enable the configurable hash authentication
     * scheme when we upgrade a database. See DERBY-4483.
     */
    public void testBuiltinAuthenticationHashNotChangedOnUpgrade()
            throws SQLException {
        // We enable the configurable hash authentication scheme by setting
        // a property, so check that it's NULL in all phases to verify that
        // it's not enabled on upgrade.
        assertNull(getDatabaseProperty(HASH_ALGORITHM_PROPERTY));
    }

    /**
     * Make sure builtin authentication only uses the new configurable hash
     * scheme in hard-upgraded databases. See DERBY-4483.
     */
    public void testBuiltinAuthenticationWithConfigurableHash()
            throws SQLException {

        // This test needs to enable authentication, which is not supported
        // in the default database for the upgrade tests, so roll our own.
        DataSource ds = JDBCDataSource.getDataSourceLogical("BUILTIN_10_6");

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
        // in the previous phase. Would for instance fail in post soft upgrade
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
        }

        // Set (or reset) passwords for all users.
        setPasswords(setProp);
        setProp.close();

        // We should still be able to connect.
        verifyCanConnect(ds);

        // Check that the passwords are stored using the expected scheme (new
        // configurable hash scheme in hard upgrade, old scheme otherwise).
        verifyPasswords(c, getPhase() == PH_HARD_UPGRADE);

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
     * hashed password when the old scheme is used, and (5) the hashed
     * password when the new scheme is used.
     */
    private static final String[][] USERS = {
        { "dbo", "the boss", null,
                  "3b6071d99b1d48ab732e75a8de701b6c77632db65898",
                  "3b6071d99b1d48ab732e75a8de701b6c77632db65898"
        },
        { "pat", "postman", "MD5",
                  "3b609129e181a7f7527697235c8aead65c461a0257f3",
                  "3b61aaca567ed43d1ba2e6402cbf1a723407:MD5"
        },
        { "sam", "fireman", "SHA-256",
                  "3b609e5173cfa03620061518adc92f2a58c7b15cf04f",
                  "3b61aff1a3f161b6c0ce856c4ce99ce6d779bad9cc1" +
                  "44136099bc4b2b0742ed87899:SHA-256"
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
            cs.setString(1, HASH_ALGORITHM_PROPERTY);
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
     * @param newScheme if {@code true}, the passwords are expected to have
     * been hashed with the new scheme; otherwise, the passwords are expected
     * to have been hashed with the old scheme
     */
    private void verifyPasswords(Connection c, boolean newScheme)
            throws SQLException {
        PreparedStatement ps = c.prepareStatement(
                "values syscs_util.syscs_get_database_property(?)");
        for (int i = 0; i < USERS.length; i++) {
            String expectedToken = USERS[i][newScheme ? 4 : 3];
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
