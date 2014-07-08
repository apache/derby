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

import java.lang.reflect.Method;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import junit.framework.Test;
import org.apache.derby.catalog.TypeDescriptor;
import org.apache.derby.catalog.types.RoutineAliasInfo;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.SupportFilesSetup;

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

    static final String HASH_ALGORITHM_PROPERTY =
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
        BaseTestSuite suite = new BaseTestSuite("Upgrade test for 10.6");

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

}
