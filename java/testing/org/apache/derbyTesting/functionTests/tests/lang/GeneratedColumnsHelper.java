/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.lang.GeneratedColumnsHelper

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
import java.util.ArrayList;
import junit.framework.Test;
import junit.framework.TestSuite;
import org.apache.derby.iapi.util.StringUtil;
import org.apache.derby.catalog.DefaultInfo;
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
 * Helper routines for testing generated columns. See DERBY-481.
 * </p>
 */
public class GeneratedColumnsHelper extends BaseJDBCTestCase
{
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTANTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    protected static final    String OBJECT_DOES_NOT_EXIST = "42X94";
    protected static final    String NONEXISTENT_OBJECT = "42Y55";
    protected static  final   String  REDUNDANT_CLAUSE = "42613";
    protected static  final   String  CANT_CONTAIN_NULLS = "42831";
    protected static  final   String  ILLEGAL_AGGREGATE = "42XA1";
    protected static  final   String  UNSTABLE_RESULTS = "42XA2";
    protected static  final   String  CANT_OVERRIDE_GENERATION_CLAUSE = "42XA3";
    protected static  final   String  CANT_REFERENCE_GENERATED_COLUMN = "42XA4";
    protected static  final   String  ROUTINE_CANT_ISSUE_SQL = "42XA5";
    protected static  final   String  BAD_FOREIGN_KEY_ACTION = "42XA6";
    protected static  final   String  ILLEGAL_ADD_DEFAULT = "42XA7";
    protected static  final   String  ILLEGAL_RENAME = "42XA8";
    protected static  final   String  NEED_EXPLICIT_DATATYPE = "42XA9";
    protected static  final   String  BAD_BEFORE_TRIGGER = "42XAA";
    protected static  final   String  NOT_NULL_NEEDS_DATATYPE = "42XAB";
    
    protected static  final   String  NOT_NULL_VIOLATION = "23502";
    protected static  final   String  CONSTRAINT_VIOLATION = "23513";
    protected static  final   String  FOREIGN_KEY_VIOLATION = "23503";
    protected static  final   String  ILLEGAL_DUPLICATE = "23505";
    protected static  final   String  SYNTAX_ERROR = "42X01";
    protected static  final   String  COLUMN_OUT_OF_SCOPE = "42X04";
    protected static  final   String  OPERATION_FORBIDDEN = "X0Y25";

    protected static  final   String  LACK_TABLE_PRIV = "42500";
    protected static  final   String  LACK_COLUMN_PRIV = "42502";
    protected static  final   String  LACK_EXECUTE_PRIV = "42504";
    protected static  final   String  LACK_USAGE_PRIV = "42504";
    protected static  final   String  CANT_ADD_IDENTITY = "42601";
    protected static  final   String  CANT_MODIFY_IDENTITY = "42Z23";
    
    protected static  final   String  CASCADED_COLUMN_DROP_WARNING = "01009";
    protected static  final   String  CONSTRAINT_DROPPED_WARNING = "01500";
    protected static  final   String  TRIGGER_DROPPED_WARNING = "01502";
    protected static  final   String  LANG_INVALID_USE_OF_DEFAULT = "42Y85";
    protected static  final   String  GRANT_REVOKE_NOT_ALLOWED = "42509";
    protected static  final   String  ROUTINE_DEPENDS_ON_TYPE = "X0Y30";
    protected static  final   String  TABLE_DEPENDS_ON_TYPE = "X0Y29";
    protected static  final   String  VIEW_DEPENDS_ON_PRIVILEGE = "X0Y23";
    protected static  final   String  NON_EMPTY_SCHEMA = "X0Y54";
    protected static  final   String  JAVA_EXCEPTION = "XJ001";

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

    public GeneratedColumnsHelper(String name)
    {
        super(name);
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // HELPER METHODS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * Run good DDL.
     * @throws SQLException 
     */
    protected void    goodStatement( Connection conn, String ddl ) throws SQLException
    {
        PreparedStatement    ps = chattyPrepare( conn, ddl );

        ps.execute();
        ps.close();
    }
    
	protected	static	ResultSet	executeQuery( Statement stmt, String text )
		throws SQLException
	{
		println( "Executing '" + text + "'" );

        return stmt.executeQuery( text );
	}

    /**
     * Prepare a statement and report its sql text.
     */
    protected PreparedStatement   chattyPrepare( Connection conn, String text )
        throws SQLException
    {
        println( "Preparing statement:\n\t" + text );
        
        return conn.prepareStatement( text );
    }

    /**
     * Assert that the statement text, when compiled, raises an exception
     */
    protected void    expectCompilationError( String sqlState, String query )
    {
        println( "\nExpecting " + sqlState + " when preparing:\n\t" + query );

        assertCompileError( sqlState, query );
    }

    /**
     * Assert that the statement text, when compiled, raises an exception
     */
    protected void    expectCompilationError( Connection conn, String sqlState, String query )
    {
        println( "\nExpecting " + sqlState + " when preparing:\n\t" + query );

        PreparedStatement ps = null;

        try {
            ps = conn.prepareStatement( query );
        } catch (SQLException se )
        {
            assertSQLState( sqlState, se );

            return;
        }

        fail( "Expected SQL state: " + sqlState );
    }

    /**
     * Assert that the statement text, when executed, raises an error.
     */
    protected void    expectExecutionError( Connection conn, String sqlState, String query )
        throws Exception
    {
        println( "\nExpecting " + sqlState + " when executing:\n\t"  );
        PreparedStatement   ps = chattyPrepare( conn, query );

        assertStatementError( sqlState, ps );
        ps.close();
    }

    /**
     * Assert that the in-place update raises the expected error.
     */
    protected void    expectUpdateRowError( ResultSet rs, String sqlState )
        throws Exception
    {
        println( "\nExpecting " + sqlState + " when updating row" );

        try {
            rs.updateRow();
            fail( "Expected error: " + sqlState );
        }
        catch (SQLException se)
        {
            assertSQLState( sqlState, se );
        }
    }

    /**
     * Assert that the in-place insert raises the expected error.
     */
    protected void    expectInsertRowError( ResultSet rs, String sqlState )
        throws Exception
    {
        println( "\nExpecting " + sqlState + " when inserting row" );

        try {
            rs.insertRow();
            fail( "Expected error: " + sqlState );
        }
        catch (SQLException se)
        {
            assertSQLState( sqlState, se );
        }
    }

    /**
     * Assert that the statement text, when executed, raises a warning.
     */
    protected void    expectExecutionWarning( Connection conn, String sqlState, String query )
        throws Exception
    {
        expectExecutionWarnings( conn, new String[] { sqlState }, query );
    }

    /**
     * Assert that the statement text, when executed, raises a warning.
     */
    protected void    expectExecutionWarnings( Connection conn, String[] sqlStates, String query )
        throws Exception
    {
        println( "\nExpecting warnings " + fill( sqlStates ).toString() + " when executing:\n\t"  );
        PreparedStatement   ps = chattyPrepare( conn, query );

        ps.execute();

        int idx = 0;

        for ( SQLWarning sqlWarning = ps.getWarnings(); sqlWarning != null; sqlWarning = sqlWarning.getNextWarning() )
        {
            String          actualSQLState = sqlWarning.getSQLState();

            if ( idx >= sqlStates.length )
            {
                fail( "Got more warnings than we expected." );
            }

            String  expectedSqlState = sqlStates[ idx++ ];

            assertEquals( expectedSqlState, actualSQLState );
        }

        assertEquals( idx, sqlStates.length );

        ps.close();
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

    /**
     * Assert that a table has the correct column types.
     */
    protected void assertColumnTypes( Connection conn, String tableName, String[][] columnTypes )
        throws Exception
    {
        PreparedStatement   ps = chattyPrepare
            (
             conn,
             "select c.columnname, c.columndatatype\n" +
             "from sys.syscolumns c, sys.systables t\n" +
             "where t.tablename = ?\n" +
             "and t.tableid = c.referenceid\n" +
             "order by c.columnname\n"
             );
        ps.setString( 1, tableName );
        ResultSet                   rs = ps.executeQuery();

        assertResults( rs, columnTypes, true );

        rs.close();
        ps.close();
    }
        
    /**
     * Assert that the statement returns the correct results.
     */
    protected void assertResults( Connection conn, String query, String[][] rows, boolean trimResults )
        throws Exception
    {
        PreparedStatement   ps = chattyPrepare( conn, query );
        ResultSet                   rs = ps.executeQuery();

        assertResults( rs, rows, trimResults );

        rs.close();
        ps.close();
    }
        
    /**
     * Assert that the ResultSet returns the desired rows.
     */
    protected void assertResults( ResultSet rs, String[][] rows, boolean trimResults )
        throws Exception
    {
        int     rowCount = rows.length;

        for ( int i = 0; i < rowCount; i++ )
        {
            String[]    row = rows[ i ];
            int             columnCount = row.length;

            assertTrue( rs.next() );

            for ( int j = 0; j < columnCount; j++ )
            {
                String  expectedValue =  row[ j ];
                //println( "(row, column ) ( " + i + ", " +  j + " ) should be " + expectedValue );
                String  actualValue = null;
                int         column = j+1;

                actualValue = rs.getString( column );
                if ( rs.wasNull() ) { actualValue = null; }

                if ( (actualValue != null) && trimResults ) { actualValue = actualValue.trim(); }
                
                assertEquals( (expectedValue == null), rs.wasNull() );
                
                if ( expectedValue == null )    { assertNull( actualValue ); }
                else { assertEquals(expectedValue, actualValue); }
            }
        }

        assertFalse( rs.next() );
    }

    /**
     * Test that a privilege can't be revoked if an object depends on it.
     */
    protected void verifyRevokePrivilege
        (
         Connection grantorConnection,
         Connection granteeConnection,
         String grantStatement,
         String revokeStatement,
         String createStatement,
         String dropStatement,
         String badRevokeSQLState
         ) throws Exception
    {
        expectExecutionError
            (
             granteeConnection,
             LACK_USAGE_PRIV,
             createStatement
             );
        goodStatement
            (
             grantorConnection,
             grantStatement
             );
        goodStatement
            (
             granteeConnection,
             createStatement
             );
        expectExecutionError
            (
             grantorConnection,
             badRevokeSQLState,
             revokeStatement
             );
        goodStatement
            (
             granteeConnection,
             dropStatement
             );
        goodStatement
            (
             grantorConnection,
             revokeStatement
             );
    }

    /**
     * <p>
     * Fill an ArrayList from an array.
     * </p>
     */
    protected ArrayList   fill( Object[] raw )
    {
        ArrayList   result = new ArrayList();
        int             count = raw.length;

        for ( int i = 0; i < count; i++ ) { result.add( raw[ i ] ); }

        return result;
    }
    

}

