/*

Derby - Class org.apache.derbyTesting.functionTests.tests.upgradeTests.Changes10_7

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
 * Upgrade test cases for 10.7.
 * If the old version is 10.7 or later then these tests
 * will not be run.
 * <BR>
    10.7 Upgrade issues

    <UL>
    <LI>BOOLEAN data type support expanded.</LI>
    </UL>

 */
public class Changes10_7 extends UpgradeChange
{
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTANTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    private static final String SYNTAX_ERROR = "42X01";
    private static final String  UPGRADE_REQUIRED = "XCL47";
    private static final String  GRANT_REVOKE_WITH_LEGACY_ACCESS = "42Z60";

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

    public Changes10_7(String name)
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
        TestSuite suite = new TestSuite("Upgrade test for 10.7");

        suite.addTestSuite(Changes10_7.class);
        return new SupportFilesSetup((Test) suite);
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // TESTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * Make sure that that database is at level 10.7 in order to enjoy
     * extended support for the BOOLEAN datatype.
     */
    public void testBoolean() throws SQLException
    {
        String booleanValuedFunction =
            "create function f_4655( a varchar( 100 ) ) returns boolean\n" +
            "language java parameter style java no sql deterministic\n" +
            "external name 'Z.getBooleanValue'\n";

        Statement s = createStatement();

        switch ( getPhase() )
        {
        case PH_CREATE: // create with old version
        case PH_POST_SOFT_UPGRADE: // soft-downgrade: boot with old version after soft-upgrade
            assertStatementError(  SYNTAX_ERROR, s, booleanValuedFunction );
            break;

        case PH_SOFT_UPGRADE: // boot with new version and soft-upgrade
            assertStatementError( UPGRADE_REQUIRED, s, booleanValuedFunction );
            break;
            
        case PH_HARD_UPGRADE: // boot with new version and hard-upgrade
            s.execute( booleanValuedFunction );
            break;
        }
        
        s.close();
    }

    /**
     * Make sure that that database is at level 10.7 in order to enjoy
     * routines with specified EXTERNAL SECURITY INVOKER or DEFINER.
     */
    public void testExternalSecuritySpecification() throws SQLException
    {
        String functionWithDefinersRights =
            "create function f_4551( a varchar( 100 ) ) returns int\n" +
            "language java parameter style java reads sql data\n" +
            "external security definer\n" +
            "external name 'Z.getIntValue'\n";

        Statement s = createStatement();

        switch ( getPhase() )
        {
        case PH_CREATE: // create with old version
        case PH_POST_SOFT_UPGRADE:
            // soft-downgrade: boot with old version after soft-upgrade
            assertStatementError(
                SYNTAX_ERROR, s, functionWithDefinersRights );
            break;

        case PH_SOFT_UPGRADE: // boot with new version and soft-upgrade
            assertStatementError(
                UPGRADE_REQUIRED, s, functionWithDefinersRights );
            break;

        case PH_HARD_UPGRADE: // boot with new version and hard-upgrade.
            // Syntax now accepted and dictionary level ok, but
            // sqlAuthorization not enabled (a priori) - expected.
            assertStatementError(GRANT_REVOKE_WITH_LEGACY_ACCESS,
                                 s, functionWithDefinersRights );
            break;
        }

        s.close();
    }

}
