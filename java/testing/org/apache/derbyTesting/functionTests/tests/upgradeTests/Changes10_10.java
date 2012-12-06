/*

Derby - Class org.apache.derbyTesting.functionTests.tests.upgradeTests.Changes10_10

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
 * Upgrade test cases for 10.10.
 */
public class Changes10_10 extends UpgradeChange
{
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTANTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    private static  final   String  SYNTAX_ERROR = "42X01";
    private static  final   String  HARD_UPGRADE_REQUIRED = "XCL47";
    private static  final   String  NEEDS_JAVA_STYLE = "42ZCA";

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

    public Changes10_10(String name)
    {
        super(name);
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // JUnit BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////////////////
    
    /**
     * Return the suite of tests to test the changes made in 10.10.
     * @param phase an integer that indicates the current phase in
     *              the upgrade test.
     * @return the test suite created.
     */
    public static Test suite(int phase) {
        TestSuite suite = new TestSuite("Upgrade test for 10.9");

        suite.addTestSuite(Changes10_10.class);
        
        return new SupportFilesSetup((Test) suite);
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // TESTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * Make sure that the following procedure(s) which are new to 10.10 are 
     *  only available after hard-upgrade
     *  1)invalidate stored statements 
     *    SYCS_UTIL.SYSCS_INVALIDATE_STORED_STATEMENTS
     */
    public  void    testProcsNewTo10_10()  throws Exception
    {
        Statement s = createStatement();

        switch ( getPhase() )
        {
        case PH_CREATE: // create with old version
            vetProcs(s, "call syscs_util.SYSCS_INVALIDATE_STORED_STATEMENTS()", 
            		false, 
            		"syscs_util.SYSCS_INVALIDATE_STORED_STATEMENTS should not exist.");
            break;
            
        case PH_SOFT_UPGRADE: // boot with new version and soft-upgrade
            vetProcs(s, "call syscs_util.SYSCS_INVALIDATE_STORED_STATEMENTS()", 
            		false, 
            		"syscs_util.SYSCS_INVALIDATE_STORED_STATEMENTS should not exist.");
            break;
            
        case PH_POST_SOFT_UPGRADE: // soft-downgrade: boot with old version after soft-upgrade
            vetProcs(s, "call syscs_util.SYSCS_INVALIDATE_STORED_STATEMENTS()", 
            		false, 
            		"syscs_util.SYSCS_INVALIDATE_STORED_STATEMENTS should not exist.");
            break;

        case PH_HARD_UPGRADE: // boot with new version and hard-upgrade
            vetProcs(s, "call syscs_util.SYSCS_INVALIDATE_STORED_STATEMENTS()", 
            		true, 
            		null);
            break;
        }
        
        s.close();
    	
    }
    
    private void    vetProcs( Statement s, String procCall, 
    		boolean shouldExist,
    		String errorMessage) throws Exception
    {
        try {
            s.execute( procCall );
            
            if ( !shouldExist )
            {
                fail( errorMessage );
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

    /**
     * Verify upgrade behavior for user-defined aggregates.
     */
    public  void    testUDAs()  throws Exception
    {
        Statement st = createStatement();

        String  createUDA = "create derby aggregate mode for int external name 'foo.bar.Wibble'";
        String  dropUDA = "drop derby aggregate mode restrict";

        switch ( getPhase() )
        {
        case PH_CREATE: // create with old version
        case PH_POST_SOFT_UPGRADE: // soft-downgrade: boot with old version after soft-upgrade
            assertStatementError( SYNTAX_ERROR, st, createUDA );
            assertStatementError( SYNTAX_ERROR, st, dropUDA );
            break;
            
        case PH_SOFT_UPGRADE: // boot with new version and soft-upgrade
            assertStatementError( HARD_UPGRADE_REQUIRED, st, createUDA );
            assertStatementError( HARD_UPGRADE_REQUIRED, st, dropUDA );
            break;
            
        case PH_HARD_UPGRADE: // boot with new version and hard-upgrade
            st.execute( createUDA );
            st.execute( dropUDA );
            break;
        }
        
        st.close();
    }
    
    /**
     * Verify upgrade behavior for vararg routines.
     */
    public  void    testVarargss()  throws Exception
    {
        Statement st = createStatement();

        String  createVarargsProc = "create procedure vds ( a int ... ) language java parameter style derby no sql external name 'Foo.foo'";
        String  createVarargsFunc = "create function vds ( a int ... ) returns integer language java parameter style derby no sql external name 'Foo.foo'";
        String  createVarargsTableFunc = "create function vtf ( a int ... ) returns table ( b int ) language java parameter style derby_jdbc_result_set no sql external name 'Foo.foo'";
        String  createNonVarargsProcDerbyStyle = "create procedure nvds ( a int ) language java parameter style derby no sql external name 'Foo.foo'";
        String  createNonVarargsFuncDerbyStyle = "create function nvds ( a int ) returns integer language java parameter style derby no sql external name 'Foo.foo'";

        // table functions were introduced by 10.4
        boolean tableFunctionsOK = oldAtLeast( 10, 4 );       

        switch ( getPhase() )
        {
        case PH_CREATE: // create with old version
        case PH_POST_SOFT_UPGRADE: // soft-downgrade: boot with old version after soft-upgrade
            assertStatementError( SYNTAX_ERROR, st, createVarargsProc );
            assertStatementError( SYNTAX_ERROR, st, createVarargsFunc );
            if ( tableFunctionsOK ) { assertStatementError( SYNTAX_ERROR, st, createVarargsTableFunc ); }
            assertStatementError( SYNTAX_ERROR, st, createNonVarargsProcDerbyStyle );
            assertStatementError( SYNTAX_ERROR, st, createNonVarargsFuncDerbyStyle );
            break;
            
        case PH_SOFT_UPGRADE: // boot with new version and soft-upgrade
            assertStatementError( HARD_UPGRADE_REQUIRED, st, createVarargsProc );
            assertStatementError( HARD_UPGRADE_REQUIRED, st, createVarargsFunc );
            if ( tableFunctionsOK ) { assertStatementError( HARD_UPGRADE_REQUIRED, st, createVarargsTableFunc ); }
            assertStatementError( HARD_UPGRADE_REQUIRED, st, createNonVarargsProcDerbyStyle );
            assertStatementError( HARD_UPGRADE_REQUIRED, st, createNonVarargsFuncDerbyStyle );
            break;
            
        case PH_HARD_UPGRADE: // boot with new version and hard-upgrade
            st.execute( createVarargsProc );
            st.execute( createVarargsFunc );
            st.execute( createVarargsTableFunc );
            assertStatementError( NEEDS_JAVA_STYLE, st, createNonVarargsProcDerbyStyle );
            assertStatementError( NEEDS_JAVA_STYLE, st, createNonVarargsFuncDerbyStyle );
            break;
        }
        
        st.close();
    }
    
}
