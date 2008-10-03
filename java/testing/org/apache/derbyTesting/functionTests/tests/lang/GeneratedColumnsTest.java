/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.lang.GeneratedColumnsTest

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package org.apache.derbyTesting.functionTests.tests.lang;

import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.DriverManager;
import junit.framework.Test;
import junit.framework.TestSuite;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.DatabasePropertyTestSetup;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.TestConfiguration;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.JDBC;

import org.apache.derby.catalog.types.RoutineAliasInfo;

/**
 * <p>
 * Test generated columns. See DERBY-481.
 * </p>
 */
public class GeneratedColumnsTest extends BaseJDBCTestCase
{
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTANTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    private static  final   String  REDUNDANT_CLAUSE = "42613";

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


    /**
     * Create a new instance.
     */

    public GeneratedColumnsTest(String name)
    {
        super(name);
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // JUnit BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////////////////


    /**
     * Construct top level suite in this JUnit test
     */
    public static Test suite()
    {
        TestSuite suite = (TestSuite) TestConfiguration.embeddedSuite(GeneratedColumnsTest.class);

        return new CleanDatabaseTestSetup( suite );
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // TESTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Test that the stored system procedures and functions are non-deterministic. If you want
     * a particular procedure/function to be deterministic, add some logic here.
     * </p>
     *
     * <p>
     * Also test that, by default, user-defined routines are created as NOT DETERMINISTIC.
     * </p>
     */
    public  void    test_001_determinism_of_stored_system_routines()
        throws Exception
    {
        Connection  conn = getConnection();

        //
        // Create a user-defined function and procedure and verify
        // that they too are NOT DETERMINISTIC.
        // 
        PreparedStatement functionCreate = conn.prepareStatement
            (
             "create function f1()\n" +
             "returns int\n" +
             "language java\n" +
             "parameter style java\n" +
             "no sql\n" +
             "external name 'foo.bar.wibble'\n"
             );
        functionCreate.execute();
        functionCreate.close();

        PreparedStatement procedureCreate = conn.prepareStatement
            (
             "create procedure p1()\n" +
             "language java\n" +
             "parameter style java\n" +
             "modifies sql data\n" +
             "external name 'foo.bar.wibble'\n"
             );
        procedureCreate.execute();
        procedureCreate.close();

        //
        // OK, now verify that all routines in the catalogs are NOT DETERMINISTIC 
        //
        PreparedStatement   ps = conn.prepareStatement
            (
             "select s.schemaname, a.alias, a.aliastype, a.systemalias, a.aliasinfo\n" +
             "from sys.sysschemas s, sys.sysaliases a\n" +
             "where s.schemaid = a.schemaid\n" +
             "order by s.schemaname, a.alias\n"
             );
        ResultSet               rs = ps.executeQuery();

        while ( rs.next() )
        {
            String    aliasName = rs.getString( 2 );
            boolean isSystemAlias = rs.getBoolean( 4 );

            RoutineAliasInfo    rai = (RoutineAliasInfo) rs.getObject( 5 );

            if ( isSystemAlias ) { assertFalse( aliasName, rai.isDeterministic() ); }
        }

        rs.close();
        ps.close();
    }

    /**
     * <p>
     * Basic positive tests for DETERMINISTIC keyword.
     * </p>
     */
    public  void    test_002_determinism_positive()
        throws Exception
    {
        Connection  conn = getConnection();

        goodStatement
            (
             conn,
             "create function f11()\n" +
             "returns int\n" +
             "language java\n" +
             "parameter style java\n" +
             "no sql\n" +
             "external name 'foo.bar.wibble'\n"
             );
        assertDeterministic( conn, "F11", false );
        
        goodStatement
            (
             conn,
             "create function f12()\n" +
             "returns int\n" +
             "language java\n" +
             "parameter style java\n" +
             "deterministic\n" +
             "no sql\n" +
             "external name 'foo.bar.wibble'\n"
             );
        assertDeterministic( conn, "F12", true );
        
        goodStatement
            (
             conn,
             "create function f13()\n" +
             "returns int\n" +
             "language java\n" +
             "parameter style java\n" +
             "no sql\n" +
             "not deterministic\n" +
             "external name 'foo.bar.wibble'\n"
             );
        assertDeterministic( conn, "F13", false );
        
        goodStatement
            (
             conn,
             "create procedure p11()\n" +
             "language java\n" +
             "parameter style java\n" +
             "modifies sql data\n" +
             "external name 'foo.bar.wibble'\n"
             );
        assertDeterministic( conn, "P11", false );
        
        goodStatement
            (
             conn,
             "create procedure p12()\n" +
             "deterministic\n" +
             "language java\n" +
             "parameter style java\n" +
             "modifies sql data\n" +
             "external name 'foo.bar.wibble'\n"
             );
        assertDeterministic( conn, "P12", true );
        
        goodStatement
            (
             conn,
             "create procedure p13()\n" +
             "language java\n" +
             "not deterministic\n" +
             "parameter style java\n" +
             "modifies sql data\n" +
             "external name 'foo.bar.wibble'\n"
             );
        assertDeterministic( conn, "P13", false );
    }

    /**
     * <p>
     * Verify that we get errors when there is more than one determinism clause
     * in a routine declaration.
     * </p>
     */
    public  void    test_003_determinism_redundantClause()
        throws Exception
    {
        Connection  conn = getConnection();

        expectError
            (
             REDUNDANT_CLAUSE,
             "create function f_fail()\n" +
             "returns int\n" +
             "language java\n" +
             "parameter style java\n" +
             "deterministic\n" +
             "deterministic\n" +
             "no sql\n" +
             "external name 'foo.bar.wibble'\n"
             );
        expectError
            (
             REDUNDANT_CLAUSE,
             "create function f_fail()\n" +
             "returns int\n" +
             "language java\n" +
             "parameter style java\n" +
             "not deterministic\n" +
             "deterministic\n" +
             "no sql\n" +
             "external name 'foo.bar.wibble'\n"
             );
        expectError
            (
             REDUNDANT_CLAUSE,
             "create procedure p_fail()\n" +
             "language java\n" +
             "deterministic\n" +
             "parameter style java\n" +
             "modifies sql data\n" +
             "deterministic\n" +
             "external name 'foo.bar.wibble'\n"
             );
        expectError
            (
             REDUNDANT_CLAUSE,
             "create procedure p_fail()\n" +
             "language java\n" +
             "not deterministic\n" +
             "parameter style java\n" +
             "modifies sql data\n" +
             "not deterministic\n" +
             "external name 'foo.bar.wibble'\n"
             );
    }
    
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // MINIONS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * Run good DDL.
     * @throws SQLException 
     */
    private void    goodStatement( Connection conn, String ddl ) throws SQLException
    {
        PreparedStatement    ps = chattyPrepare( conn, ddl );

        ps.execute();
        ps.close();
    }
    
    /**
     * Prepare a statement and report its sql text.
     */
    private PreparedStatement   chattyPrepare( Connection conn, String text )
        throws SQLException
    {
        println( "Preparing statement:\n\t" + text );
        
        return conn.prepareStatement( text );
    }

    /**
     * Assert that the statement text, when compiled, raises an exception
     */
    private void    expectError( String sqlState, String query )
    {
        println( "\nExpecting " + sqlState + " when preparing:\n\t" + query );

        assertCompileError( sqlState, query );
    }

    /**
     * <p>
     * Assert whether a routine is expected to be DETERMINISTIC.
     * </p>
     */
    public  void    assertDeterministic( Connection conn, String routineName, boolean isDeterministic )
        throws Exception
    {
        PreparedStatement   ps = conn.prepareStatement
            (
             "select a.aliasinfo\n" +
             "from sys.sysaliases a\n" +
             "where alias =  ?"
             );
        ps.setString( 1, routineName );
        ResultSet               rs = ps.executeQuery();

        rs.next();
        RoutineAliasInfo    rai = (RoutineAliasInfo) rs.getObject( 1 );

        assertEquals( isDeterministic, rai.isDeterministic() );

        rs.close();
        ps.close();
    }

}