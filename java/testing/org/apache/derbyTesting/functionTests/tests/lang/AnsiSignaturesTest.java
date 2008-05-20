/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.lang.AnsiSignaturesTest

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

/**
 * <p>
 * Test that Derby resolves routines according to the ANSI method
 * resolution rules. Those rules are summarized in DERBY-3652.
 * </p>
 */
public class AnsiSignaturesTest extends BaseJDBCTestCase
{
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTANTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    public  static  final   String  MISSING_METHOD_SQLSTATE = "XJ001";
    public  static  final   String  TRIED_ALL_COMBINATIONS = "42X50";
    public  static  final   String  AMBIGUOUS = "42X73";
    
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

    public AnsiSignaturesTest(String name)
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
        TestSuite suite = (TestSuite) TestConfiguration.embeddedSuite(AnsiSignaturesTest.class);

        return new CleanDatabaseTestSetup( suite );
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // SUCCESSFUL RESOLUTIONS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    public  void    test_smallint_short_short()
        throws Exception
    {
        declareAndRunFunction
            ( "smallint_short_short", "smallint", new String[] { "smallint" }, "3", "3" );
    }
    public  void    test_smallint_short_Integer()
        throws Exception
    {
        // FIXME declareAndRunFunction
        // FIXME ( "smallint_short_Integer", "smallint", new String[] { "smallint" }, "3", "3" );
    }
    public  void    test_smallint_Integer_short()
        throws Exception
    {
        // FIXME declareAndRunFunction
        // FIXME ( "smallint_Integer_short", "smallint", new String[] { "smallint" }, "3", "3" );
    }
    public  void    test_smallint_Integer_Integer()
        throws Exception
    {
        // FIXME declareAndRunFunction
        // FIXME ( "smallint_Integer_Integer", "smallint", new String[] { "smallint" }, "3", "3" );
    }

    public  void    test_integer_int_int()
        throws Exception
    {
        declareAndRunFunction
            ( "integer_int_int", "int", new String[] { "int" }, "3", "3" );
    }
    public  void    test_integer_int_Integer()
        throws Exception
    {
        declareAndRunFunction
            ( "integer_int_Integer", "int", new String[] { "int" }, "3", "3" );
    }
    public  void    test_integer_Integer_int()
        throws Exception
    {
        declareAndRunFunction
            ( "integer_Integer_int", "int", new String[] { "int" }, "3", "3" );
    }
    public  void    test_integer_Integer_Integer()
        throws Exception
    {
        declareAndRunFunction
            ( "integer_Integer_Integer", "int", new String[] { "int" }, "3", "3" );
    }

    public  void    test_bigint_long_long()
        throws Exception
    {
        declareAndRunFunction
            ( "bigint_long_long", "bigint", new String[] { "bigint" }, "3", "3" );
    }
    public  void    test_bigint_long_Long()
        throws Exception
    {
        declareAndRunFunction
            ( "bigint_long_Long", "bigint", new String[] { "bigint" }, "3", "3" );
    }
    public  void    test_bigint_Long_long()
        throws Exception
    {
        declareAndRunFunction
            ( "bigint_Long_long", "bigint", new String[] { "bigint" }, "3", "3" );
    }
    public  void    test_bigint_Long_Long()
        throws Exception
    {
        declareAndRunFunction
            ( "bigint_Long_Long", "bigint", new String[] { "bigint" }, "3", "3" );
    }

    public  void    test_real_float_float()
        throws Exception
    {
        declareAndRunFunction
            ( "real_float_float", "real", new String[] { "real" }, "3.0", "3.0" );
    }
    public  void    test_real_float_Float()
        throws Exception
    {
        declareAndRunFunction
            ( "real_float_Float", "real", new String[] { "real" }, "3.0", "3.0" );
    }
    public  void    test_real_Float_float()
        throws Exception
    {
        declareAndRunFunction
            ( "real_Float_float", "real", new String[] { "real" }, "3.0", "3.0" );
    }
    public  void    test_real_Float_Float()
        throws Exception
    {
        declareAndRunFunction
            ( "real_Float_Float", "real", new String[] { "real" }, "3.0", "3.0" );
    }

    public  void    test_double_double_double()
        throws Exception
    {
        declareAndRunFunction
            ( "double_double_double", "double", new String[] { "double" }, "3.0", "3.0" );
    }
    public  void    test_double_double_Double()
        throws Exception
    {
        declareAndRunFunction
            ( "double_double_Double", "double", new String[] { "double" }, "3.0", "3.0" );
    }
    public  void    test_double_Double_double()
        throws Exception
    {
        declareAndRunFunction
            ( "double_Double_double", "double", new String[] { "double" }, "3.0", "3.0" );
    }
    public  void    test_double_Double_Double()
        throws Exception
    {
        declareAndRunFunction
            ( "double_Double_Double", "double", new String[] { "double" }, "3.0", "3.0" );
    }

    
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // SHOULD NOT RESOLVE
    //
    ///////////////////////////////////////////////////////////////////////////////////

    public  void    test_smallint_bad_short_Short()
        throws Exception
    {
        declareAndFailFunction
            ( "smallint_bad_short_Short", "smallint", new String[] { "smallint" }, "3", "3", MISSING_METHOD_SQLSTATE );
    }
    public  void    test_smallint_bad_Short_short()
        throws Exception
    {
        declareAndFailFunction
            ( "smallint_bad_Short_short", "smallint", new String[] { "smallint" }, "3", "3", TRIED_ALL_COMBINATIONS );
    }
    public  void    test_smallint_bad_Short_Short()
        throws Exception
    {
        declareAndFailFunction
            ( "smallint_bad_Short_Short", "smallint", new String[] { "smallint" }, "3", "3", TRIED_ALL_COMBINATIONS );
    }
    
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // AMBIGUOUS METHODS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    public  void    test_smallint_amb_short_short()
        throws Exception
    {
        // FIXME declareAndFailFunction
        // FIXME ( "smallint_amb_short_short", "smallint", new String[] { "smallint" }, "3", "3", AMBIGUOUS );
    }
    public  void    test_smallint_amb_Integer_short()
        throws Exception
    {
        // FIXME declareAndFailFunction
        // FIXME ( "smallint_amb_Integer_short", "smallint", new String[] { "smallint" }, "3", "3", AMBIGUOUS );
    }

    public  void    test_integer_amb_int_int()
        throws Exception
    {
        declareAndFailFunction
             ( "integer_amb_int_int", "int", new String[] { "int" }, "3", "3", AMBIGUOUS );
    }
    public  void    test_integer_amb_Integer_int()
        throws Exception
    {
        declareAndFailFunction
            ( "integer_amb_Integer_int", "int", new String[] { "int" }, "3", "3", AMBIGUOUS );
    }

    public  void    test_bigint_amb_long_long()
        throws Exception
    {
        declareAndFailFunction
            ( "bigint_amb_long_long", "bigint", new String[] { "bigint" }, "3", "3", AMBIGUOUS );
    }
    public  void    test_bigint_amb_Long_long()
        throws Exception
    {
        declareAndFailFunction
            ( "bigint_amb_Long_long", "bigint", new String[] { "bigint" }, "3", "3", AMBIGUOUS );
    }

    public  void    test_real_amb_float_float()
        throws Exception
    {
        declareAndFailFunction
            ( "real_amb_float_float", "real", new String[] { "real" }, "3.0", "3.0", AMBIGUOUS );
    }
    public  void    test_real_amb_Float_float()
        throws Exception
    {
        declareAndFailFunction
            ( "real_amb_Float_float", "real", new String[] { "real" }, "3.0", "3.0", AMBIGUOUS );
    }

    public  void    test_double_amb_double_double()
        throws Exception
    {
        declareAndFailFunction
            ( "double_amb_double_double", "double", new String[] { "double" }, "3.0", "3.0", AMBIGUOUS );
    }
    public  void    test_double_amb_Double_double()
        throws Exception
    {
        declareAndFailFunction
            ( "double_amb_Double_double", "double", new String[] { "double" }, "3.0", "3.0", AMBIGUOUS );
    }
    
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // UNRESOLVABLE METHODS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    public  void    test_smallint_unres_short()
        throws Exception
    {
        declareAndFailFunction
            ( "smallint_unres_short", "smallint", new String[] { "smallint" }, "3", "3", MISSING_METHOD_SQLSTATE );
    }
    public  void    test_smallint_unres_Short()
        throws Exception
    {
        //FIXME declareAndFailFunction
        //FIXME ( "smallint_unres_Short", "smallint", new String[] { "smallint" }, "3", "3", MISSING_METHOD_SQLSTATE );
    }

    public  void    test_integer_unres_int()
        throws Exception
    {
        declareAndFailFunction
            ( "integer_unres_int", "int", new String[] { "int" }, "3", "3", TRIED_ALL_COMBINATIONS );
    }
    public  void    test_integer_unres_Integer()
        throws Exception
    {
        declareAndFailFunction
            ( "integer_unres_Integer", "int", new String[] { "int" }, "3", "3", TRIED_ALL_COMBINATIONS );
    }
    
    public  void    test_bigint_unres_long()
        throws Exception
    {
        declareAndFailFunction
            ( "bigint_unres_long", "bigint", new String[] { "bigint" }, "3", "3", TRIED_ALL_COMBINATIONS );
    }
    public  void    test_bigint_unres_Long()
        throws Exception
    {
        declareAndFailFunction
            ( "bigint_unres_Long", "bigint", new String[] { "bigint" }, "3", "3", TRIED_ALL_COMBINATIONS );
    }
        
    public  void    test_real_unres_float()
        throws Exception
    {
        declareAndFailFunction
            ( "real_unres_float", "real", new String[] { "real" }, "3.0", "3.0", TRIED_ALL_COMBINATIONS );
    }
    public  void    test_real_unres_Float()
        throws Exception
    {
        declareAndFailFunction
            ( "real_unres_Float", "real", new String[] { "real" }, "3.0", "3.0", TRIED_ALL_COMBINATIONS );
    }
        
    public  void    test_double_unres_double()
        throws Exception
    {
        declareAndFailFunction
            ( "double_unres_double", "double", new String[] { "double" }, "3.0", "3.0", TRIED_ALL_COMBINATIONS );
    }
    public  void    test_double_unres_Double()
        throws Exception
    {
        declareAndFailFunction
            ( "double_unres_Double", "double", new String[] { "double" }, "3.0", "3.0", TRIED_ALL_COMBINATIONS );
    }

        
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // MINIONS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Declare and run a function.
     * </p>
     */
    private void declareAndRunFunction( String name, String returnType, String[] argTypes, String args, String result )
        throws Exception
    {
        Connection  conn = getConnection();

        declareFunction( conn, name, returnType, argTypes );
        runFunction( conn, name, args, result, null );
    }
    
    /**
     * <p>
     * Declare and run a function and expect the function to fail.
     * </p>
     */
    private void declareAndFailFunction( String name, String returnType, String[] argTypes, String args, String result, String sqlstate )
        throws Exception
    {
        Connection  conn = getConnection();

        declareFunction( conn, name, returnType, argTypes );
        runFunction( conn, name, args, result, sqlstate );
    }
    
    /**
     * <p>
     * Run a function. If sqlstate is not null, then we expect the run to fail.
     * </p>
     */
    private void runFunction( Connection conn, String name, String args, String result, String sqlstate )
        throws Exception
    {
        StringBuffer    buffer = new StringBuffer();

        buffer.append( "values ( " + doubleQuote( name ) + "( " + args + " ) )" );

        String          query = buffer.toString();

        println( query );

        PreparedStatement   ps = null;
        ResultSet               rs = null;

        try {
            ps = conn.prepareStatement( query );
            rs = ps.executeQuery();

            rs.next();

            assertEquals( rs.getString( 1 ), result );

            if ( sqlstate != null )
            {
                fail( "Should have failed with sqlstate: " + sqlstate );
            }
        }
        catch (SQLException se)
        {
            assertSQLState( sqlstate, se );
        }
        finally
        {
            if ( rs != null ) { rs.close(); }
            if ( ps != null ) { ps.close(); }
        }
    }
    
    /**
     * <p>
     * Declare a function with the given name, return type, and argument type.
     * </p>
     */
    private void declareFunction( Connection conn, String name, String returnType, String[] argTypes )
        throws Exception
    {
        StringBuffer    buffer = new StringBuffer();
        int                 count = argTypes.length;

        buffer.append( "create function " + doubleQuote( name ) );
        buffer.append( "\n(" );
        for ( int i = 0; i < count; i++ )
        {
            if ( i > 0 ) { buffer.append( "," ); }
            buffer.append( "\n\ta_" + i + " " + argTypes[ i ] );
        }
        buffer.append( "\n)\n" );
        buffer.append( "returns " + returnType );
        buffer.append( "\nlanguage java\nparameter style java\nno sql\n" );
        buffer.append( "external name '" + AnsiSignatures.class.getName() + "." + name + "'" );

        String  ddl = buffer.toString();

        println( ddl );

        PreparedStatement ps = conn.prepareStatement( ddl );

        ps.execute();
        ps.close();

        conn.commit();
    }

    private String  doubleQuote( String raw )
    {
        return '"' + raw + '"';
    }
    
}