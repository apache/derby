/*

Derby - Class org.apache.derbyTesting.functionTests.tests.lang.TableFunctionTest

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

package org.apache.derbyTesting.functionTests.tests.lang;

import java.sql.*;
import java.util.ArrayList;

import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.DatabasePropertyTestSetup;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.TestConfiguration;
import junit.framework.Test;
import junit.framework.TestSuite;

/**
 * Test Table Functions. See DERBY-716 for a description of
 * this feature.
 */
public class TableFunctionTest extends BaseJDBCTestCase
{
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTANTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    // functions to drop at teardown time
    private static  final   String[]    FUNCTION_NAMES =
    {
        "SIMPLEFUNCTIONTABLE",
        "invert",
        "returnsACoupleRows",
        "returnsAllLegalDatatypes",
    };
    
    private static  final   String[][]  SIMPLE_ROWS =
    {
        { "who", "put" },
        { "the", "bop" },
        { (String) null, "in" },
        { "the", (String) null },
    };
    
    private static  final   String[][]  ALL_TYPES_ROWS =
    {
        {
            null,   // BIGINT
            null,   // BLOB
            null,   // CHAR
            null,   // CHAR FOR BIT DATA
            null,   // CLOB
            null,   // DATE
            null,   // DECIMAL
            null,   // DOUBLE
            null,   // DOUBLE PRECISION
            null,   // FLOAT( 23 )
            null,   // FLOAT( 24 )
            null,   // INTEGER
            null,   // LONG VARCHAR
            null,   // LONG VARCHAR FOR BIT DATA
            null,   // NUMERIC
            null,   // REAL
            null,   // SMALLINT
            null,   // TIME
            null,   // TIMESTAMP
            null,   // VARCHAR
            null,   // VARCHAR FOR BIT DATA
        },
    };
    
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // INNER CLASSES
    //
    ///////////////////////////////////////////////////////////////////////////////////

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // STATE
    //
    ///////////////////////////////////////////////////////////////////////////////////

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTRUCTORS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    public TableFunctionTest
        (
         String name
        )
    {
         super( name );
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // JUnit MACHINERY
    //
    ///////////////////////////////////////////////////////////////////////////////////
    
    /**
     * Tests to run.
     */
    public static Test suite()
    {
        TestSuite       suite = new TestSuite( "TableFunctionTest" );

        suite.addTest( new TableFunctionTest( "testTableFunctions" ) );

        return suite;
    }
    
    protected void    setUp()
        throws Exception
    {
        super.setUp();

        dropSchema();
    }

    protected void    tearDown()
        throws Exception
    {
        dropSchema();

        super.tearDown();
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // JUnit TESTS
    //
    ///////////////////////////////////////////////////////////////////////////////////
    
    /**
     * Verify table functions.
     */
    public void testTableFunctions()
        throws Exception
    {
        badDDL();
        simpleDDL();

        notTableFunction();
        simpleVTIResults();
        allLegalDatatypesVTIResults();
    }
    
    /**
     * Verify bad DDL.
     */
    private void badDDL()
        throws Exception
    {
        //
        // Only table functions can have parameter style DERBY_JDBC_RESULT_SET
        //
        expectError
            (
             "42ZB1",
             "create function badParameterStyle()\n" +
             "returns varchar(10)\n" +
             "language java\n" +
             "parameter style DERBY_JDBC_RESULT_SET\n" +
             "no sql\n" +
             "external name 'com.scores.proc.Functions.weighQuestion'\n"
             );

        //
        // Procedures can not have parameter style DERBY_JDBC_RESULT_SET
        //
        expectError
            (
             "42ZB1",
             "create procedure badParameterStyle\n" +
             "( in takingID int )\n" +
             "language java\n" +
             "parameter style DERBY_JDBC_RESULT_SET\n" +
             "modifies sql data\n" +
             "external name 'com.scores.proc.Procedures.ScoreTestTaking'\n"
             );

        //
        // Table functions must have parameter style DERBY_JDBC_RESULT_SET
        //
        expectError
            (
             "42ZB2",
             "create function badParameterStyleForTableFunction()\n" +
             "returns TABLE\n" +
             "  (\n" +
             "     intCol int,\n" +
             "     varcharCol varchar( 10 )\n" +
             "  )\n" +
             "language java\n" +
             "parameter style java\n" +
             "no sql\n" +
             "external name 'com.scores.proc.Functions.weighQuestion'\n"
             );

        //
        // XML column types not allowed in table functions.
        //
        expectError
            (
             "42ZB3",
             "create function xmlForbiddenInReturnedColumns()\n" +
             "returns TABLE\n" +
             "  (\n" +
             "     intCol int,\n" +
             "     xmlCol xml\n" +
             "  )\n" +
             "language java\n" +
             "parameter style DERBY_JDBC_RESULT_SET\n" +
             "no sql\n" +
             "external name 'com.scores.proc.Functions.weighQuestion'\n"
             );
    }
    
    /**
     * Verify simple good DDL.
     */
    private void simpleDDL()
        throws Exception
    {
        goodDDL
            (
             "create function simpleFunctionTable()\n" +
             "returns TABLE\n" +
             "  (\n" +
             "     intCol int,\n" +
             "     varcharCol varchar( 10 )\n" +
             "  )\n" +
             "language java\n" +
             "parameter style DERBY_JDBC_RESULT_SET\n" +
             "no sql\n" +
             "external name 'com.scores.proc.Functions.weighQuestion'\n"
             );

        verifyReturnType
            (
             "SIMPLEFUNCTIONTABLE",
             "weighQuestion() " +
             "RETURNS ROW ( INTCOL INTEGER, VARCHARCOL VARCHAR(10) ) MULTISET " +
             "LANGUAGE JAVA PARAMETER STYLE DERBY_JDBC_RESULT_SET NO SQL CALLED ON NULL INPUT"
             );
    }
    
    /**
     * Verify that you can't invoke an ordinary function as a VTI.
     */
    private void notTableFunction()
        throws Exception
    {
        goodDDL
            (
             "create function invert( intValue int )\n" +
             "returns int\n" +
             "language java\n" +
             "parameter style java\n" +
             "no sql\n" +
             "external name '" + getClass().getName() + ".invert'\n"
             );

        //
        // Can't invoke a simple function as a table function.
        //
        expectError
            (
             "42ZB4",
             "select s.*\n" +
             "    from TABLE( invert( 1 ) ) s\n"
             );
    }
    
    /**
     * Verify that a simple VTI returns the correct results.
     */
    private void  simpleVTIResults()
        throws Exception
    {
        goodDDL
            (
             "create function returnsACoupleRows()\n" +
             "returns TABLE\n" +
             "  (\n" +
             "     column0 varchar( 10 ),\n" +
             "     column1 varchar( 10 )\n" +
             "  )\n" +
             "language java\n" +
             "parameter style DERBY_JDBC_RESULT_SET\n" +
             "no sql\n" +
             "external name '" + getClass().getName() + ".returnsACoupleRows'\n"
             );

        assertResults
            (
             "select s.*\n" +
             "    from TABLE( returnsACoupleRows() ) s\n",
             SIMPLE_ROWS,
             new int[] { Types.VARCHAR, Types.VARCHAR }
             );
    }
    
    /**
     * Verify that Derby handles VTI columns of all known datatypes.
     */
    private void  allLegalDatatypesVTIResults()
        throws Exception
    {
        goodDDL
            (
             "create function returnsAllLegalDatatypes()\n" +
             "returns TABLE\n" +
             "  (\n" +
             "column0 BIGINT,\n" +
             "column1 BLOB,\n" +
             "column2 CHAR( 10 ),\n" +
             "column3 CHAR( 10 ) FOR BIT DATA,\n" +
             "column4 CLOB,\n" +
             "column5 DATE,\n" +
             "column6 DECIMAL,\n" +
             "column7 DOUBLE,\n" +
             "column8 DOUBLE PRECISION,\n" +
             "column9 FLOAT( 23 ),\n" +
             "column10 FLOAT( 24 ),\n" +
             "column11 INTEGER,\n" +
             "column12 LONG VARCHAR,\n" +
             "column13 LONG VARCHAR FOR BIT DATA,\n" +
             "column14 NUMERIC,\n" +
             "column15 REAL,\n" +
             "column16 SMALLINT,\n" +
             "column17 TIME,\n" +
             "column18 TIMESTAMP,\n" +
             "column19 VARCHAR( 10 ),\n" +
             "column20 VARCHAR( 10 ) FOR BIT DATA\n" +
             "  )\n" +
             "language java\n" +
             "parameter style DERBY_JDBC_RESULT_SET\n" +
             "no sql\n" +
             "external name '" + getClass().getName() + ".returnsAllLegalDatatypes'\n"
             );

        assertResults
            (
             "select s.*\n" +
             "    from TABLE( returnsAllLegalDatatypes() ) s\n",
             ALL_TYPES_ROWS,
             new int[]
                {
                    Types.BIGINT,
                    Types.BLOB,
                    Types.CHAR,
                    Types.BINARY,
                    Types.CLOB,
                    Types.DATE,
                    Types.DECIMAL,
                    Types.DOUBLE,
                    Types.DOUBLE,
                    Types.REAL,
                    Types.DOUBLE,
                    Types.INTEGER,
                    Types.LONGVARCHAR,
                    Types.LONGVARBINARY,
                    Types.NUMERIC,
                    Types.REAL,
                    Types.SMALLINT,
                    Types.TIME,
                    Types.TIMESTAMP,
                    Types.VARCHAR,
                    Types.VARBINARY,
                }
             );
    }
    
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // Derby FUNCTIONS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * Just a simple function which is not a VTI.
     */
    public  static  int invert( int value )
    {
        return -value;
    }
    
    /**
     * A VTI which returns a couple rows.
     */
    public  static  ResultSet returnsACoupleRows()
    {
        return makeVTI( SIMPLE_ROWS );
    }

    /**
     * A VTI which returns rows having columns of all legal datatypes.
     */
    public  static  ResultSet returnsAllLegalDatatypes()
    {
        return makeVTI( ALL_TYPES_ROWS );
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // MINIONS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * Assert that the ResultSet returns the desired rows.
     */
    public void assertResults( String sql, String[][] rows, int[] expectedJdbcTypes )
        throws Exception
    {
        println( "\nExpecting good results from " + sql );

        String[]    columnNames = makeColumnNames( rows[ 0 ].length, "COLUMN" );

        try {
            PreparedStatement    ps = prepareStatement( sql );
            ResultSet                   rs = ps.executeQuery();

            assertResults( expectedJdbcTypes, columnNames, rs, rows );
            
            rs.close();
            ps.close();
        }
        catch (Exception e)
        {
            unexpectedThrowable( e );
        }
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
     * Run good DDL.
     */
    private void    goodDDL( String ddl )
    {
        println( "Running good DDL:\n\t" + ddl );
        
        try {
            PreparedStatement    ps = prepareStatement( ddl );

            ps.execute();
            ps.close();
        }
        catch (Exception e)
        {
            unexpectedThrowable( e );
        }
    }
    
    /**
     * Verify that the return type of function looks good.
     */
    private void    verifyReturnType( String functionName, String expectedReturnType )
    {
        println( functionName + " should have return type = " + expectedReturnType );
        
        try {
            String                          ddl = "select aliasinfo from sys.sysaliases where alias=?";
            PreparedStatement    ps = prepareStatement( ddl );

            ps.setString( 1, functionName );
            
            ResultSet                   rs = ps.executeQuery();

            rs.next();

            String                          actualReturnType = rs.getString( 1 );

            assertTrue( expectedReturnType.equals( actualReturnType ) );
            
            rs.close();
            ps.close();
        }
        catch (Exception e)
        {
            unexpectedThrowable( e );
        }
    }

    /**
     * Drop the schema that we are going to use so that we can recreate it.
     */
    private void    dropSchema()
        throws Exception
    {
        int count = FUNCTION_NAMES.length;

        for ( int i = 0; i < count; i++ ) { dropFunction( FUNCTION_NAMES[ i ] ); }
    }
    
    /**
     * Drop a function so that we can recreate it.
     */
    private void    dropFunction( String functionName )
        throws Exception
    {
        // swallow the "object doesn't exist" diagnostic
        try {
            PreparedStatement   ps = prepareStatement( "drop function " + functionName );

            ps.execute();
            ps.close();
        }
        catch( SQLException se) {}
    }

    /**
     * Assert that the ResultSet returns the desired rows.
     */
    private void assertResults( int[] expectedJdbcTypes, String[] columnNames, ResultSet rs, String[][] rows )
        throws Exception
    {
        int     rowCount = rows.length;
        int[]   actualJdbcTypes = getJdbcColumnTypes( rs );

        compareJdbcTypes( expectedJdbcTypes, actualJdbcTypes );
        compareColumnNames( columnNames, rs );

        for ( int i = 0; i < rowCount; i++ )
        {
            String[]    row = rows[ i ];
            int             columnCount = row.length;

            assertTrue( rs.next() );

            for ( int j = 0; j < columnCount; j++ )
            {
                String  columnName = columnNames[ j ];
                String  expectedValue =  row[ j ];
                String  actualValue = null;
                String  actualValueByName = null;
                int         column = j+1;
                int         actualJdbcType = actualJdbcTypes[ j ]; 

                switch( actualJdbcType )
                {
                case Types.BOOLEAN:
                    actualValue = new Boolean( rs.getBoolean( column ) ).toString();
                    actualValueByName = new Boolean( rs.getBoolean( columnName ) ).toString();
                    if ( rs.wasNull() ) { actualValue = actualValueByName = null; }
                    break;

                case Types.BIGINT:
                    actualValue = new Long( rs.getLong( column ) ).toString();
                    actualValueByName = new Long( rs.getLong( columnName ) ).toString();
                    if ( rs.wasNull() ) { actualValue = actualValueByName = null; }
                    break;
                case Types.INTEGER:
                    actualValue = new Integer( rs.getInt( column ) ).toString();
                    actualValueByName = new Integer( rs.getInt( columnName ) ).toString();
                    if ( rs.wasNull() ) { actualValue = actualValueByName = null; }
                    break;
                case Types.SMALLINT:
                    actualValue = new Short( rs.getShort( column ) ).toString();
                    actualValueByName = new Short( rs.getShort( columnName ) ).toString();
                    if ( rs.wasNull() ) { actualValue = actualValueByName = null; }
                    break;
                case Types.TINYINT:
                    actualValue = new Byte( rs.getByte( column ) ).toString();
                    actualValueByName = new Byte( rs.getByte( columnName ) ).toString();
                    if ( rs.wasNull() ) { actualValue = actualValueByName = null; }
                    break;
 
                case Types.DOUBLE:
                    actualValue = new Double( rs.getDouble( column ) ).toString();
                    actualValueByName = new Double( rs.getDouble( columnName ) ).toString();
                    if ( rs.wasNull() ) { actualValue = actualValueByName = null; }
                    break;
                case Types.REAL:
                case Types.FLOAT:
                    actualValue = new Float( rs.getFloat( column ) ).toString();
                    actualValueByName = new Float( rs.getFloat( columnName ) ).toString();
                    if ( rs.wasNull() ) { actualValue = actualValueByName = null; }
                    break;

                case Types.DECIMAL:
                case Types.NUMERIC:
                    actualValue = squeezeString(  rs.getBigDecimal( column ) );
                    actualValueByName = squeezeString(  rs.getBigDecimal( columnName ) );
                    break;

                case Types.DATE:
                    actualValue = squeezeString(  rs.getDate( column ) );
                    actualValueByName = squeezeString(  rs.getDate( columnName ) );
                    break;
                case Types.TIME:
                    actualValue = squeezeString(  rs.getTime( column ) );
                    actualValueByName = squeezeString(  rs.getTime( columnName ) );
                    break;
                case Types.TIMESTAMP:
                    actualValue = squeezeString(  rs.getTimestamp( column ) );
                    actualValueByName = squeezeString(  rs.getTimestamp( columnName ) );
                    break;

                case Types.BLOB:
                    actualValue = squeezeString(  rs.getBlob( column ) );
                    actualValueByName = squeezeString(  rs.getBlob( columnName ) );
                    break;
                case Types.CLOB:
                    actualValue = squeezeString(  rs.getClob( column ) );
                    actualValueByName = squeezeString(  rs.getClob( columnName ) );
                    break;

                case Types.BINARY:
                case Types.VARBINARY:
                case Types.LONGVARBINARY:
                    actualValue = squeezeString(  rs.getBytes( column ) );
                    actualValueByName = squeezeString(  rs.getBytes( columnName ) );
                    break;

                case Types.JAVA_OBJECT:
                    actualValue = squeezeString(  rs.getObject( column ) );
                    actualValueByName = squeezeString(  rs.getObject( columnName ) );
                    break;
                    
                case Types.CHAR:
                case Types.LONGVARCHAR:
                case Types.VARCHAR:
                    actualValue = rs.getString( column );
                    actualValueByName = rs.getString( columnName );
                    break;
                    
                default:
                    fail( "Can't handle jdbc type " + actualJdbcType );
                }

                println( "Comparing " + expectedValue + " to " + actualValue + " and " + actualValueByName );

                if ( actualValue == null ) { assertNull( actualValueByName ); }
                else { assertTrue( actualValue.equals( actualValueByName ) ); }
                
                assertEquals( (expectedValue == null), rs.wasNull() );
                
                if ( expectedValue == null )    { assertNull( actualValue ); }
                else { assertTrue( expectedValue.equals( actualValue ) ); }
            }
        }

        assertFalse( rs.next() );
    }

    /**
     * Verify that we saw the jdbc types that we expected.
     */
    private void   compareJdbcTypes( int[] expected, int[] actual )
        throws Exception
    {
        int     count = expected.length;

        assertEquals( count, actual.length );

        for ( int i = 0; i < count; i++ )
        {
            assertEquals( "Type at position " + i, expected[ i ], actual[ i ] );
        }
    }

    /**
     * Verify that we have the correct column names.
     */
    private void   compareColumnNames( String[] expectedNames, ResultSet rs )
        throws Exception
    {
        ResultSetMetaData   rsmd = rs.getMetaData();
        int                                 count = rsmd.getColumnCount();

        println( "Expecting " + expectedNames.length + " columns." );
        assertEquals( expectedNames.length, count );

        for ( int i = 0; i < count; i++ )
        {
            assertEquals( expectedNames[ i ], rsmd.getColumnName( i+1 ) );
        }
   }

    /**
     * Get the datatypes of returned columns
     */
    private int[]   getJdbcColumnTypes( ResultSet rs )
        throws Exception
    {
        ResultSetMetaData   rsmd = rs.getMetaData();
        int                                 count = rsmd.getColumnCount();
        int[]                           actualJdbcTypes = new int[ count ];

        for ( int i = 0; i < count; i++ ) { actualJdbcTypes[ i ] = rsmd.getColumnType( i + 1 ); }

        return actualJdbcTypes;
    }

    /**
     * Squeeze a string out of an object
     */
    private String  squeezeString( Object obj )
        throws Exception
    {
        if ( obj == null ) { return null; }
        else if ( obj instanceof Blob )
        {
            Blob    blob = (Blob) obj;

            return new String( blob.getBytes( (long) 0, (int) blob.length() ) );
        }
        else if ( obj instanceof Clob )
        {
            Clob    clob = (Clob) obj;

            return clob.getSubString( (long) 0, (int) clob.length() );
        }
        else if ( obj instanceof byte[] )
        {
            byte[]  bytes = (byte[]) obj;

            return new String( bytes );
        }
        else { return obj.toString(); }
    }

    /**
     * Fail the test for an unexpected exception
     */
    private void    unexpectedThrowable( Throwable t )
    {
        printStackTrace( t );
        fail( "Unexpected exception: " + t );
    }
    
    /**
     * Make a VTI given its rows.
     */
    private static  StringArrayVTI    makeVTI( String[][] rows )
    {
        int         columnCount = rows[ 0 ].length;

        return new StringArrayVTI( makeColumnNames( columnCount, "mycol" ), rows );
    }
    
    /**
     * Make column names.
     */
    private static  String[]    makeColumnNames( int columnCount, String stub )
    {
        String[]    names = new String[ columnCount ];

        for ( int i = 0; i < columnCount; i++ ) { names[ i ] = stub + i; }

        return names;
    }
    
}
