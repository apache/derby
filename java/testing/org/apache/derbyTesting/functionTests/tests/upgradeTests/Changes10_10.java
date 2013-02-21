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
import java.io.IOException;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.derbyTesting.functionTests.util.PrivilegedFileOpsForTests;
import org.apache.derbyTesting.junit.JDBC;
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
    /**
    The readme file cautioning users against touching the files in
    the database directory 
    */
    private static final String DB_README_FILE_NAME = "README_DO_NOT_TOUCH_FILES.txt";

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
        String  iss = "call syscs_util.SYSCS_INVALIDATE_STORED_STATEMENTS()";
        String  rt = "call syscs_util.syscs_register_tool( 'foo', true )";
        String  rtGoodSQLState = "X0Y88";
        String  syntaxError = "42X01";
        boolean atLeastJava5 = JDBC.vmSupportsJDBC3();
        boolean oldSupportsBoolean = oldAtLeast( 10, 7 );

        switch ( getPhase() )
        {
        case PH_CREATE: // create with old version
        case PH_SOFT_UPGRADE: // boot with new version and soft-upgrade
        case PH_POST_SOFT_UPGRADE: // soft-downgrade: boot with old version after soft-upgrade
            vetProcs( s, iss, false, null );
            if ( atLeastJava5 )
            {
                vetProcs
                    ( s, rt, false,
                      oldSupportsBoolean || (getPhase() == PH_SOFT_UPGRADE ) ? null : syntaxError );
            }
            break;
            
        case PH_HARD_UPGRADE: // boot with new version and hard-upgrade
            vetProcs( s, iss, true, null );
            if ( atLeastJava5 ) { vetProcs( s, rt, true, rtGoodSQLState ); }
            break;
        }
        
        s.close();
    	
    }
    
    private void    vetProcs
        (
         Statement s,
         String procCall, 
         boolean shouldExist,
         String sqlState
         ) throws Exception
    {
        try {
            s.execute( procCall );
            
            if ( !shouldExist )
            {
                fail( "Procedure should not exist!"  );
            }
            if ( sqlState != null )
            {
                fail( "Expected to fail with SQLState " + sqlState );
            }
        } catch (SQLException se )
        {
            if ( sqlState == null ) { sqlState = "42Y03"; }
            assertSQLState( sqlState, se );
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
    
    /**
     * DERBY-5996(Create readme files (cautioning users against modifying 
     *  database files) at database hard upgrade time)
     * Simple test to make sure readme files are getting created
     */
    public void testReadMeFiles() throws SQLException, IOException
    {
        Statement s = createStatement();
        s.close();
        TestConfiguration currentConfig = TestConfiguration.getCurrent();
        String dbPath = currentConfig.getDatabasePath(currentConfig.getDefaultDatabaseName());
        switch (getPhase())
        {
        case PH_CREATE:
        case PH_SOFT_UPGRADE:
        case PH_POST_SOFT_UPGRADE:
            // DERBY-5995 Pre 10.10 databases would not have readme files
            lookForReadmeFile(dbPath, false);
            lookForReadmeFile(dbPath+File.separator+"seg0", false);
            lookForReadmeFile(dbPath+File.separator+"log", false);
            break;
        case PH_HARD_UPGRADE:
        case PH_POST_HARD_UPGRADE:
            // DERBY-5995 Hard upgrade to 10.10 will create readme files
            lookForReadmeFile(dbPath, true);
            lookForReadmeFile(dbPath+File.separator+"seg0", true);
            lookForReadmeFile(dbPath+File.separator+"log", true);
            break;
        }
    }

    /**
     * For pre-10.10 database, fileShouldExist will be false. For hard upgraded
     *  databases to 10.10, fileShouldExist will be true
     * @param path - this can be root database directory, log or seg0 directory
     * @param fileShouldExist
     * @throws IOException
     */
    private void lookForReadmeFile(String path, boolean fileShouldExist) throws IOException {
        File readmeFile = new File(path,
            DB_README_FILE_NAME);
        if (fileShouldExist)
        {
            assertTrue(readmeFile + "doesn't exist", 
                PrivilegedFileOpsForTests.exists(readmeFile));
        } else 
        {
            assertFalse(readmeFile + "exists", 
                PrivilegedFileOpsForTests.exists(readmeFile));
        
        }
    }

    // Old DB2 constants in Limits.java:
    // static final float DB2_SMALLEST_REAL = -3.402E+38f;
    // static final float DB2_LARGEST_REAL  = +3.402E+38f;
    // static final float DB2_SMALLEST_POSITIVE_REAL = +1.175E-37f;
    // static final float DB2_LARGEST_NEGATIVE_REAL  = -1.175E-37f;

    // static final double DB2_SMALLEST_DOUBLE = -1.79769E+308d;
    // static final double DB2_LARGEST_DOUBLE  = +1.79769E+308d;
    // static final double DB2_SMALLEST_POSITIVE_DOUBLE = +2.225E-307d;
    // static final double DB2_LARGEST_NEGATIVE_DOUBLE  = -2.225E-307d;

    static final float[] beyondDB2Real = new float[] {
        Float.MIN_VALUE,
        Float.MAX_VALUE,
        +1.174E-37f,
        -1.174E-37f,
        1.17549435E-38f, // Float.MIN_NORMAL
        -1.17549435E-38f // -Float.MIN_NORMAL
    };

    static final double[] beyondDB2Double = new double[] {
        Double.MIN_VALUE,
        Double.MAX_VALUE,
        +2.224E-307d,
        -2.224E-307d,
        2.2250738585072014E-308, // Double.MIN_NORMAL
        -2.2250738585072014E-308 // -Double.MIN_NORMAL
    };

    /**
     * Verify upgrade behavior DERBY-3398: removing DB2 float limits
     */
    public  void    testFloatLimits()  throws Exception
    {
        Statement st = createStatement();
        st.execute("create table d3398(r real, d double)");

        PreparedStatement psInsertReal =
            prepareStatement("insert into d3398(r) values (?)");
        PreparedStatement psInsertDouble =
            prepareStatement("insert into d3398(d) values (?)");
        PreparedStatement psSelect =
            prepareStatement("select * from d3398",
                ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_UPDATABLE);

        st.execute("insert into d3398 values (0.0, 0.0)");


        switch ( getPhase() )
        {
        case PH_CREATE: // create with old version
            verifyDB2Behavior(psSelect, psInsertReal, psInsertDouble, false);

            break;
        case PH_POST_SOFT_UPGRADE: // soft-downgrade: boot with old
                                   // version after soft-upgrade
            verifyDB2Behavior(psSelect, psInsertReal, psInsertDouble, false);

            break;
        case PH_SOFT_UPGRADE: // boot with new version and soft-upgrade

            verifyDB2Behavior(psSelect, psInsertReal, psInsertDouble, true);

            break;
        case PH_HARD_UPGRADE: // boot with new version and hard-upgrade

            for (int i = 0; i < beyondDB2Real.length; i++) {
                psInsertReal.setFloat(1, beyondDB2Real[i]);
                psInsertReal.execute();

                ResultSet rs = psSelect.executeQuery();
                rs.next();
                rs.updateFloat(1, beyondDB2Real[i]);
                rs.updateRow();
                rs.close();
            }

            for (int i = 0; i < beyondDB2Double.length; i++) {
                psInsertDouble.setDouble(1, beyondDB2Double[i]);
                psInsertDouble.execute();

                ResultSet rs = psSelect.executeQuery();
                rs.next();
                rs.updateDouble(2, beyondDB2Double[i]);
                rs.updateRow();
                rs.close();
            }

            break;
        }

        st.executeUpdate("drop table d3398");
        st.close();
    }

    private void assertSetError(PreparedStatement ps, float fv, boolean defer)
            throws SQLException {
        try {
            ps.setFloat(1, fv);

            if (!defer) {
                fail();
            }

            ps.executeUpdate();
            fail();
        } catch (SQLException e) {
            assertSQLState("22003", e);
        }
    }
    
    private void assertSetError(PreparedStatement ps, double dv, boolean defer)
            throws SQLException {
        try {
            ps.setDouble(1, dv);

            if (!defer) {
                fail();
            }

            ps.executeUpdate();
            fail();
        } catch (SQLException e) {
            assertSQLState("22003", e);
        }
    }


    private void assertUpdateError(
            PreparedStatement ps,
            float fv,
            boolean defer) throws SQLException {

        boolean supportsForwardUpdatableResultSet = oldAtLeast( 10, 2 );

        if (!supportsForwardUpdatableResultSet) {
            return;
        }

        ResultSet rs = ps.executeQuery();
        rs.next();

        try {
            rs.updateFloat(1, fv);

            if (!defer) {
                fail();
            }

            rs.updateRow();
            fail();
        } catch (SQLException e) {
            assertSQLState("22003", e);
        } finally {
            rs.close();
        }
    }

    private void assertUpdateError(
            PreparedStatement ps,
            double fv,
            boolean defer) throws SQLException {

        boolean supportsForwardUpdatableResultSet = oldAtLeast( 10, 2 );

        if (!supportsForwardUpdatableResultSet) {
            return;
        }

        ResultSet rs = ps.executeQuery();
        rs.next();

        try {
            rs.updateDouble(1, fv);

            if (!defer) {
                fail();
            }

            rs.updateRow();
            fail();
        } catch (SQLException e) {
            assertSQLState("22003", e);
        } finally {
            rs.close();
        }
    }

    /**
     * Check that the old DB2 limits are (still) enforced.
     *
     * @param defer In soft upgrade mode, the checking if deferred after
     * DERBY-3398, i.e. instead of throwing on the DB2 limits when calling
     * {@code ResultSet#updateXXX} or {@code PreparedStatement#setXXX}, the
     * check throws on {@code ResultSet#updateRow}, or {#insertRow}, and
     * similarly on {@code PreparedStatement#execute} or {@code #executeUpdate}.
     *
     * @throws SQLException if we see some expected error.
     */
    private void verifyDB2Behavior (
            PreparedStatement psSelect,
            PreparedStatement psInsertReal,
            PreparedStatement psInsertDouble,
            boolean defer) throws SQLException {

        for (int i = 0; i < beyondDB2Real.length; i++) {
            assertSetError(psInsertReal, beyondDB2Real[i], defer);
            assertUpdateError(psSelect, beyondDB2Real[i], defer);
        }

        for (int i = 0; i < beyondDB2Double.length; i++) {
            assertSetError(psInsertDouble, beyondDB2Double[i], defer);
            assertUpdateError(psSelect, beyondDB2Double[i], defer);
        }
    }
}
