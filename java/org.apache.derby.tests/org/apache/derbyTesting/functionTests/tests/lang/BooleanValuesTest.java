/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.lang.BooleanValuesTest

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

import java.sql.Blob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import junit.framework.Test;
import org.apache.derby.iapi.types.HarmonySerialBlob;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.TestConfiguration;
import org.apache.derbyTesting.junit.XML;

/**
 * <p>
 * Test Derby's expanding support for BOOLEAN values.
 * </p>
 */
public class BooleanValuesTest  extends GeneratedColumnsHelper
{
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTANTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    private static final String ILLEGAL_GET = "22005";
    private static final String ILLEGAL_XML_SELECTION = "42Z71";
    private static final String BAD_CAST = "22018";
    private static final String NOT_UNION_COMPATIBLE = "42X61";
    private static final String BAD_CONVERSION = "42846";
    private static final String ILLEGAL_INSERT = "42821";
    private static final String BAD_DEFAULT = "42894";
    private static final String ILLEGAL_UPDATE = "XCL12";
    private static final String NON_BOOLEAN_OPERAND = "42Y94";

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // STATE
    //
    ///////////////////////////////////////////////////////////////////////////////////

    private boolean _supportsXML;

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTRUCTOR
    //
    ///////////////////////////////////////////////////////////////////////////////////


    /**
     * Create a new instance.
     */

    public BooleanValuesTest(String name)
    {
        super(name);

//IC see: https://issues.apache.org/jira/browse/DERBY-4674
        _supportsXML = XML.classpathMeetsXMLReqs();
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
//IC see: https://issues.apache.org/jira/browse/DERBY-6590
        Test result = (BaseTestSuite)TestConfiguration.defaultSuite(
            BooleanValuesTest.class);

        return result;
    }

    protected void    setUp()
        throws Exception
    {
        super.setUp();

        Connection conn = getConnection();

        if ( !routineExists( conn, "MAKESIMPLEBLOB" ) )
        {
            goodStatement
                (
                 conn,
                 "create function makeSimpleBlob( ) returns blob\n" +
                 "language java parameter style java no sql deterministic\n" +
                 "external name 'org.apache.derbyTesting.functionTests.tests.lang.BooleanValuesTest.makeSimpleBlob'\n"
                 );
        }
        if ( !tableExists( conn, "ALL_TYPES" ) )
        {
            StringBuffer buffer;
//IC see: https://issues.apache.org/jira/browse/DERBY-4674

            //
            // create table
            //
            buffer = new StringBuffer();
            buffer.append
                (
                 "create table all_types\n" +
                 "(\n" +
                 "    key_col int,\n" +
                 "    bigint_col  BIGINT,\n" +
                 "    blob_col  BLOB(2147483647),\n" +
                 "    char_col  CHAR(10),\n" +
                 "    char_for_bit_data_col  CHAR (10) FOR BIT DATA,\n" +
                 "    clob_col  CLOB(2147483647),\n" +
                 "    date_col  DATE,\n" +
                 "    decimal_col  DECIMAL(5,2),\n" +
                 "    real_col  REAL,\n" +
                 "    double_col  DOUBLE,\n" +
                 "    int_col  INTEGER,\n" +
                 "    long_varchar_col  LONG VARCHAR,\n" +
                 "    long_varchar_for_bit_data_col  LONG VARCHAR FOR BIT DATA,\n" +
                 "    numeric_col  NUMERIC(5,2), \n" +
                 "    smallint_col  SMALLINT,\n" +
                 "    time_col  TIME,\n" +
                 "    timestamp_col  TIMESTAMP,\n" +
                 "    varchar_col  VARCHAR(10),\n" +
//IC see: https://issues.apache.org/jira/browse/DERBY-4674
                "    varchar_for_bit_data_col  VARCHAR (10) FOR BIT DATA\n"
                 );
            if ( _supportsXML )
            {
//IC see: https://issues.apache.org/jira/browse/DERBY-4658
                buffer.append( "    ,xml_col  XML\n" );
            }
            buffer.append( ")\n" );
            goodStatement( conn, buffer.toString() );

            //
            // populate table
            //
            buffer = new StringBuffer();
            buffer.append
                (
                 "insert into all_types\n" +
                 "(\n" +
                 "    key_col,\n" +
                 "    bigint_col,\n" +
                 "    blob_col,\n" +
                 "    char_col,\n" +
                 "    char_for_bit_data_col,\n" +
                 "    clob_col,\n" +
                 "    date_col,\n" +
                 "    decimal_col,\n" +
                 "    real_col,\n" +
                 "    double_col,\n" +
                 "    int_col,\n" +
                 "    long_varchar_col,\n" +
                 "    long_varchar_for_bit_data_col,\n" +
                 "    numeric_col, \n" +
                 "    smallint_col,\n" +
                 "    time_col,\n" +
                 "    timestamp_col,\n" +
                 "    varchar_col,\n" +
//IC see: https://issues.apache.org/jira/browse/DERBY-4674
                 "    varchar_for_bit_data_col\n"
                 );
            if ( _supportsXML )
            {
                buffer.append( "    ,xml_col\n" );
            }
            buffer.append
                (
                 ")\n" +
                 "values\n" +
                 "(\n" +
                 "    0,\n" +
                 "    0,\n" +
                 "    makeSimpleBlob(),\n" +
                 "    '0',\n" +
                 "    X'DE',\n" +
                 "    '0',\n" +
                 "    date('1994-02-23'),\n" +
                 "    0.00,\n" +
                 "    0.0,\n" +
                 "    0.0,\n" +
                 "    0,\n" +
                 "    '0',\n" +
                 "    X'DE',\n" +
                 "    0.00, \n" +
                 "    0,\n" +
                 "    time('15:09:02'),\n" +
                 "    timestamp('1962-09-23 03:23:34.234'),\n" +
                 "    '0',\n" +
//IC see: https://issues.apache.org/jira/browse/DERBY-4674
                 "    X'DE'\n"
                 );
            if ( _supportsXML )
            {
                buffer.append( "    , xmlparse( document '<?xml version=\"1.0\" encoding=\"UTF-8\"?> <html/>' preserve whitespace )\n" );
            }
            buffer.append
                (
                 "),\n" +
                 "(\n" +
                 "    1,\n" +
                 "    1,\n" +
                 "    makeSimpleBlob(),\n" +
                 "    '1',\n" +
                 "    X'DE',\n" +
                 "    '1',\n" +
                 "    date('1994-02-23'),\n" +
                 "    1.00,\n" +
                 "    1.0,\n" +
                 "    1.0,\n" +
                 "    1,\n" +
                 "    '1',\n" +
                 "    X'DE',\n" +
                 "    1.00, \n" +
                 "    1,\n" +
                 "    time('15:09:02'),\n" +
                 "    timestamp('1962-09-23 03:23:34.234'),\n" +
                 "    '1',\n" +
//IC see: https://issues.apache.org/jira/browse/DERBY-4674
                 "    X'DE'\n"
                 );
            if ( _supportsXML )
            {
                buffer.append( "    , xmlparse( document '<?xml version=\"1.0\" encoding=\"UTF-8\"?> <html/>' preserve whitespace )\n"  );
            }
            buffer.append
                 (
                 "),\n" +
                 "(\n" +
                 "    2,\n" +
                 "    2,\n" +
                 "    makeSimpleBlob(),\n" +
                 "    '2',\n" +
                 "    X'DE',\n" +
                 "    '2',\n" +
                 "    date('1994-02-23'),\n" +
                 "    2.00,\n" +
                 "    2.0,\n" +
                 "    2.0,\n" +
                 "    2,\n" +
                 "    '2',\n" +
                 "    X'DE',\n" +
                 "    2.00, \n" +
                 "    2,\n" +
                 "    time('15:09:02'),\n" +
                 "    timestamp('1962-09-23 03:23:34.234'),\n" +
                 "    '2',\n" +
//IC see: https://issues.apache.org/jira/browse/DERBY-4674
                 "    X'DE'\n"
                  );
            if ( _supportsXML )
            {
                buffer.append( "    , xmlparse( document '<?xml version=\"1.0\" encoding=\"UTF-8\"?> <html/>' preserve whitespace )\n" );
            }
            buffer.append
                (
                 "),\n" +
                 "(\n" +
                 "    3,\n" +
                 "    3,\n" +
                 "    makeSimpleBlob(),\n" +
                 "    'baffle',\n" +
                 "    X'DE',\n" +
                 "    'baffle',\n" +
                 "    date('1994-02-23'),\n" +
                 "    3.00,\n" +
                 "    3.0,\n" +
                 "    3.0,\n" +
                 "    3,\n" +
                 "    'baffle',\n" +
                 "    X'DE',\n" +
                 "    3.00, \n" +
                 "    3,\n" +
                 "    time('15:09:02'),\n" +
                 "    timestamp('1962-09-23 03:23:34.234'),\n" +
                 "    'baffle',\n" +
//IC see: https://issues.apache.org/jira/browse/DERBY-4674
                 "    X'DE'\n"
                 );
            if ( _supportsXML )
            {
                buffer.append( "    , xmlparse( document '<?xml version=\"1.0\" encoding=\"UTF-8\"?> <html/>' preserve whitespace )\n" );
            }
            buffer.append
                (
                 ")\n"
                 );
            goodStatement( conn, buffer.toString() );
        }
        
//IC see: https://issues.apache.org/jira/browse/DERBY-4658
        if ( !tableExists( conn, "BOOLEANSTRING" ) )
        {
            //
            // create table
            //
            goodStatement( conn, "create table booleanString( keyCol int, stringCol varchar( 20 ) )" );

            //
            // populate it
            //
            goodStatement
                (
                 conn,
                 "insert into booleanString( keyCol, stringCol )\n" +
                 "values ( 0, 'false' ), ( 1, 'true' ), ( 2, null ), ( 3, 'unknown' ), ( 10, ' false ' ), ( 11, ' true ' ), ( 12, null ), ( 13, ' unknown ' )\n"
                 );
        }

        if ( !tableExists( conn, "T_4704" ) )
        {
            //
            // create table
            //
            goodStatement( conn, "create table t_4704( keyCol int, stringCol varchar( 20 ) not null )" );

            //
            // populate it
            //
            goodStatement
                (
                 conn,
                 "insert into t_4704( keyCol, stringCol )\n" +
                 "values ( 0, 'false' ), ( 1, 'true' ), ( 2, 'unknown' )\n"
                 );
        }

        if ( !tableExists( conn, "STRING_TYPES" ) )
        {
            //
            // create table
            //
            goodStatement
                (
                 conn,
                 "create table string_types\n" +
                 "(\n" +
                 "    key_col int,\n" +
                 "    char_col  CHAR(10),\n" +
                 "    clob_col  CLOB(2147483647),\n" +
                 "    long_varchar_col  LONG VARCHAR,\n" +
                 "    varchar_col  VARCHAR(10)\n" +
                 ")\n"
                 );

            //
            // populate it
            //
            goodStatement
                (
                 conn,
                 "insert into string_types\n" +
                 "( key_col, char_col, clob_col, long_varchar_col, varchar_col )\n" +
                 "values\n" +
                 "( 0, 'false', 'false', 'false', 'false' ),\n" +
                 "( 1, 'true', 'true', 'true', 'true' ),\n" +
                 "( 2, 'unknown', 'unknown', 'unknown', 'unknown' ),\n" +
                 "( 3, null, null, null, null ),\n" +
                 "( 4, ' false ', ' false ', ' false ', ' false ' ),\n" +
                 "( 5, ' true ', ' true ', ' true ', ' true ' ),\n" +
                 "( 6, ' unknown ', ' unknown ', ' unknown ', ' unknown ' ),\n" +
                 "( 7, null, null, null, null ),\n" +
                 "( 10, 'FALSE', 'FALSE', 'FALSE', 'FALSE' ),\n" +
                 "( 11, 'TRUE', 'TRUE', 'TRUE', 'TRUE' ),\n" +
                 "( 12, 'UNKNOWN', 'UNKNOWN', 'UNKNOWN', 'UNKNOWN' ),\n" +
                 "( 13, NULL, NULL, NULL, NULL ),\n" +
                 "( 14, ' FALSE ', ' FALSE ', ' FALSE ', ' FALSE ' ),\n" +
                 "( 15, ' TRUE ', ' TRUE ', ' TRUE ', ' TRUE ' ),\n" +
                 "( 16, ' UNKNOWN ', ' UNKNOWN ', ' UNKNOWN ', ' UNKNOWN ' ),\n" +
                 "( 17, NULL, NULL, NULL, NULL ),\n" +
                 "( 20, 'arg', 'arg', 'arg', 'arg' ),\n" +
                 "( 21, '0', '0', '0', '0' ),\n" +
                 "( 22, '1', '1', '1', '1' ),\n" +
                 "( 23, '2', '2', '2', '2' )\n"
                 );
        }
        
//IC see: https://issues.apache.org/jira/browse/DERBY-4659
        if ( !routineExists( conn, "BOOLEANVALUE" ) )
        {
            //
            // create function
            //
            goodStatement
                (
                 conn,
                 "create function booleanValue( b boolean )\n" +
                 "returns varchar( 100 ) language java parameter style java no sql\n" +
                 "external name 'org.apache.derbyTesting.functionTests.tests.lang.BooleanValuesTest.booleanValue'\n"
                 );
        }
        
//IC see: https://issues.apache.org/jira/browse/DERBY-4716
        if ( !tableExists( conn, "BOOLEAN_TABLE" ) )
        {
            //
            // create table
            //
            goodStatement
                (
                 conn,
                 "create table boolean_table\n" +
                 "(\n" +
                 "    key_col int,\n" +
                 "    boolean_col  boolean\n" +
                 ")\n"
                 );
        }
        
        if ( !tableExists( conn, "T_4889" ) )
        {
            //
            // create table
            //
            goodStatement
                (
                 conn,
                 "create table t_4889\n" +
                 "(\n" +
                 "    key_col int,\n" +
                 "    setter_col varchar( 20 ),\n" +
                 "    boolean_col  boolean\n" +
                 ")\n"
                 );
        }
    }


    ///////////////////////////////////////////////////////////////////////////////////
    //
    // TESTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Verify the number of datatypes supported by Derby. If this number changes,
     * then you need to add a new datatype to this test:
     * </p>
     *
     * <ul>
     * <li>Add a new column to ALL_TYPES and corresponding rows (see setUp())</li>
     * <li>Add the new datatype to one of the tests below</li>
     * <li>Add a new bad union case to test_06_unions()</li>
     * <li>Add a new bad explicit cast to test_09_explicitCasts()</li>
     * </ul>
     */
    public void test_01_datatypeCount() throws Exception
    {
        Connection conn = getConnection();

        ResultSet rs = conn.getMetaData().getTypeInfo();
        int actualTypeCount = 0;
        while ( rs.next() ) { actualTypeCount++; }
        rs.close();

//IC see: https://issues.apache.org/jira/browse/DERBY-4730
        assertEquals( 22, actualTypeCount );
    }

    /**
     * <p>
     * Test that ResultSet.getBoolean() behaves well for the datatypes for
     * which the JDBC spec defines correct values: CHAR, VARCHAR, TINYINT,
     * SMALLINT, INTEGER, BIGINT.
     * </p>
     */
    public void test_02_defined() throws Exception
    {
        Connection conn = getConnection();

        vet_getBoolean( conn, "BIGINT_COL" );
        vet_getBoolean( conn, "CHAR_COL" );
        vet_getBoolean( conn, "INT_COL" );
        vet_getBoolean( conn, "SMALLINT_COL" );
        vet_getBoolean( conn, "VARCHAR_COL" );
    }
    /**
     * <p>
     * Verifies Derby's behavior is the same for embedded and client
     * drivers on datatypes for which the JDBC spec does not define results
     * but which Derby handles.
     * </p>
     */
    public void test_03_undefinedButLegal() throws Exception
    {
        Connection conn = getConnection();

        vet_getBoolean( conn, "DECIMAL_COL" );
        vet_getBoolean( conn, "REAL_COL" );
        vet_getBoolean( conn, "DOUBLE_COL" );
        vet_getBoolean( conn, "LONG_VARCHAR_COL" );
        vet_getBoolean( conn, "NUMERIC_COL" );
    }
    /**
     * <p>
     * Verify Derby's behavior is the same for embedded and client
     * drivers on datatypes for which the JDBC spec does not define results
     * and which Derby does not handle.
     * </p>
     */
    public void test_04_undefinedAndIllegal() throws Exception
    {
        Connection conn = getConnection();

        vet_getBooleanIsIllegal( conn, "BLOB_COL" );
        vet_getBooleanIsIllegal( conn, "CHAR_FOR_BIT_DATA_COL" );
        vet_getBooleanIsIllegal( conn, "CLOB_COL" );
        vet_getBooleanIsIllegal( conn, "DATE_COL" );
        vet_getBooleanIsIllegal( conn, "LONG_VARCHAR_FOR_BIT_DATA_COL" );
        vet_getBooleanIsIllegal( conn, "TIME_COL" );
        vet_getBooleanIsIllegal( conn, "TIMESTAMP_COL" );
        vet_getBooleanIsIllegal( conn, "VARCHAR_FOR_BIT_DATA_COL" );
    }
    
    /**
     * <p>
     * Regression tests for outliers. If this behavior changes,
     * hopefully the new behavior will fit into one of the categories above.
     * </p>
     */
    public void test_05_undefinedIllegalOutliers() throws Exception
    {
        //
        // We don't test the XML datatype on JDK 1.4 because the Xalan
        // packages are in the wrong location there.
        //
//IC see: https://issues.apache.org/jira/browse/DERBY-4674
        if ( _supportsXML )
        {
            expectCompilationError
                (
                 ILLEGAL_XML_SELECTION,
                 makeQuery( "XML_COL" )
                 );
        }
    }

    /**
     * <p>
     * Test that ResultSet.getBoolean() returns  the correct value. Expects to
     * select a result set with the following rows:
     * </p>
     *
     * <pre>
     * 0       0
     * 1       1
     * 2       2
     * 3       for string columns, something other than a number
     * </pre>
     */
    private void vet_getBoolean( Connection conn, String columnName ) throws Exception
    {
        PreparedStatement ps = chattyPrepare( conn, makeQuery( columnName ) );
        ResultSet rs = ps.executeQuery();

        // 0 => getBoolean() = false
        rs.next();
        assertFalse( rs.getBoolean( columnName ) );

        // 1 => getBoolean() = true
        rs.next();
        assertTrue( rs.getBoolean( columnName ) );

        // 2 => getBoolean() = true
        //
        // If the value is not 0 or 1, the JDBC spec leaves it up to the implementation.
        // For Derby, the answer is  ( value != 0 )
        rs.next();
        assertTrue( rs.getBoolean( columnName ) );

        // non-number => getBoolean() = true
        //
        // If the value is not 0 or 1, the JDBC spec leaves it up to the implementation.
        // For Derby, the answer is  ( value != 0 )
        rs.next();
        assertTrue( rs.getBoolean( columnName ) );

        rs.close();
        ps.close();
    }
    private String makeQuery( String columnName )
    {
        return "select key_col, " + columnName + " from all_types order by key_col";
    }
    /**
     * <p>
     * Test that ResultSet.getBoolean() raises an error on datatypes which
     * Derby doesn't handle.
     * </p>
     */
    private void vet_getBooleanIsIllegal( Connection conn, String columnName ) throws Exception
    {
        vet_getBooleanException( conn, columnName, ILLEGAL_GET );
    }
    /**
     * <p>
     * Test that ResultSet.getBoolean() raises the expected error.
     * </p>
     */
    private void vet_getBooleanException( Connection conn, String columnName, String sqlstate ) throws Exception
    {
        PreparedStatement ps = chattyPrepare( conn, makeQuery( columnName ) );
        ResultSet rs = ps.executeQuery();

        rs.next();

        try {
            rs.getBoolean( columnName );
            fail( "getBoolean() on " + columnName + " should not have succeeded." );
        }
        catch (SQLException se)
        {
            assertSQLState( "getBoolean() on " + columnName, sqlstate, se );
        }

    }
    
    /**
     * <p>
     * Test that unions fail if one but not both sides of the union is BOOLEAN. The rules for union
     * compatibility are found in the SQL Standard, part 2, section 7.3 (<query expression>),
     * syntax rule 20.b.ii. That in turn, refers you to section 9.3 (Result of data type combinations).
     * See, for instance, <a href="https://issues.apache.org/jira/browse/DERBY-4692">DERBY-4692</a>.This enforces the rules found 
     * </p>
     */
    public void test_06_unions() throws Exception
    {
        Connection conn = getConnection();

        vetBadImplicitCasts( conn, "bigint_col" );
        vetBadImplicitCasts( conn, "blob_col" );
        vetBadImplicitCasts( conn, "char_col" );
        vetBadImplicitCasts( conn, "char_for_bit_data_col" );
        vetBadImplicitCasts( conn, "clob_col" );
        vetBadImplicitCasts( conn, "date_col" );
        vetBadImplicitCasts( conn, "decimal_col" );
        vetBadImplicitCasts( conn, "real_col" );
        vetBadImplicitCasts( conn, "double_col" );
        vetBadImplicitCasts( conn, "int_col" );
        vetBadImplicitCasts( conn, "long_varchar_col" );
        vetBadImplicitCasts( conn, "long_varchar_for_bit_data_col" );
        vetBadImplicitCasts( conn, "numeric_col" );
        vetBadImplicitCasts( conn, "smallint_col" );
        vetBadImplicitCasts( conn, "time_col" );
        vetBadImplicitCasts( conn, "timestamp_col" );
        vetBadImplicitCasts( conn, "varchar_col" );
        vetBadImplicitCasts( conn, "varchar_for_bit_data_col" );
        if ( _supportsXML ) { vetBadImplicitCasts( conn, "xml_col" ); }
    }
    private void vetBadImplicitCasts( Connection conn, String columnName ) throws Exception
    {
        vetBadImplicitCastToBoolean( conn, columnName );
        vetBadImplicitCastFromBoolean( conn, columnName );
    }
    private void vetBadImplicitCastToBoolean( Connection conn, String columnName ) throws Exception
    {
        String query =
            "select isindex from sys.sysconglomerates where conglomeratename = 'foo'\n" +
            "union\n" +
            "select " + columnName + " from all_types\n";
        
        expectCompilationError( NOT_UNION_COMPATIBLE, query );
    }
    private void vetBadImplicitCastFromBoolean( Connection conn, String columnName ) throws Exception
    {
        String query =
            "select " + columnName + " from all_types\n" +
            "union\n" +
            "select isindex from sys.sysconglomerates\n";
        
        expectCompilationError( NOT_UNION_COMPATIBLE, query );
    }

    /**
     * <p>
     * Verify that boolean literals work.
     * </p>
     */
    public void test_07_booleanLiterals() throws Exception
    {
        Connection conn = getConnection();
//IC see: https://issues.apache.org/jira/browse/DERBY-4583

        assertResults
            (
             conn,
             "values ( true )",
             new String[][]
             {
                 { "true" },
             },
             false
             );
        assertResults
            (
             conn,
             "values ( false )",
             new String[][]
             {
                 { "false" },
             },
             false
             );
    }
    
    /**
     * <p>
     * Verify that explicit casts to and from string work as expected.
     * </p>
     */
    public void test_08_stringCasts() throws Exception
    {
        Connection conn = getConnection();
//IC see: https://issues.apache.org/jira/browse/DERBY-4658

        assertResults( conn, "values ( cast( 'true' as boolean ) )", new String[][] { { "true" } }, false );
        assertResults( conn, "values ( cast( 'false' as boolean ) )", new String[][] { { "false" } }, false );
        assertResults( conn, "values ( cast( null as boolean ) )", new String[][] { { null } }, false );
        assertResults( conn, "values ( cast( 'unknown' as boolean ) )", new String[][] { { null } }, false );
        assertResults( conn, "values ( cast( ' true ' as boolean ) )", new String[][] { { "true" } }, false );
        assertResults( conn, "values ( cast( ' false ' as boolean ) )", new String[][] { { "false" } }, false );
        assertResults( conn, "values ( cast( ' unknown ' as boolean ) )", new String[][] { { null } }, false );

        expectCompilationError( BAD_CAST, "values ( cast( 'arglebargle' as boolean ) )" );
        expectCompilationError( BAD_CAST, "values ( cast( '1' as boolean ) )" );
        expectCompilationError( BAD_CAST, "values ( cast( '0' as boolean ) )" );
        expectCompilationError( BAD_CAST, "values ( cast( '2' as boolean ) )" );

        assertResults
            (
             conn,
             "select keyCol, cast( stringCol as boolean) from booleanString order by keyCol",
             new String[][]
             {
                 { "0", "false" },
                 { "1", "true" },
                 { "2", null },
                 { "3", null },
                 { "10", "false" },
                 { "11", "true" },
                 { "12", null },
                 { "13", null },
             },
             false
             );
        
        goodStatement( conn, "update booleanString set stringCol = 'arglebargle'" );
        expectExecutionError( conn, BAD_CAST, "select keyCol, cast( stringCol as boolean) from booleanString order by keyCol" );

        goodStatement( conn, "update booleanString set stringCol = '0'" );
        expectExecutionError( conn, BAD_CAST, "select keyCol, cast( stringCol as boolean) from booleanString order by keyCol" );

        goodStatement( conn, "update booleanString set stringCol = '1'" );
        expectExecutionError( conn, BAD_CAST, "select keyCol, cast( stringCol as boolean) from booleanString order by keyCol" );

        goodStatement( conn, "update booleanString set stringCol = '2'" );
        expectExecutionError( conn, BAD_CAST, "select keyCol, cast( stringCol as boolean) from booleanString order by keyCol" );

    }
    
    /**
     * <p>
     * Verify that most explicit casts to and from boolean are not allowed.
     * </p>
     */
    public void test_09_explicitCasts() throws Exception
    {
        Connection conn = getConnection();

        vetBadExplicitCasts( conn, "bigint_col", "BIGINT", "0" );
        vetBadExplicitCasts( conn, "blob_col", "BLOB(2147483647)", "makeSimpleBlob()" );
        // CHAR ok
        vetBadExplicitCasts( conn, "char_for_bit_data_col", "CHAR (10) FOR BIT DATA", "X'DE'" );
        // CLOB ok
        vetBadExplicitCasts( conn, "date_col", "DATE", "date('1994-02-23')" );
        vetBadExplicitCasts( conn, "decimal_col", "DECIMAL(5,2)", "0.00" );
        vetBadExplicitCasts( conn, "real_col", "REAL", "0.0" );
        vetBadExplicitCasts( conn, "double_col", "DOUBLE", "0.0" );
        vetBadExplicitCasts( conn, "int_col", "INTEGER", "0" );
        // LONG VARCHAR ok
        vetBadExplicitCasts( conn, "long_varchar_for_bit_data_col", "LONG VARCHAR FOR BIT DATA", "X'DE'" );
        vetBadExplicitCasts( conn, "numeric_col", "NUMERIC(5,2)", "0.00" );
        vetBadExplicitCasts( conn, "smallint_col", "SMALLINT", "0" );
        vetBadExplicitCasts( conn, "time_col", "TIME", "time('15:09:02')" );
        vetBadExplicitCasts( conn, "timestamp_col", "TIMESTAMP", "timestamp('1962-09-23 03:23:34.234')" );
        // VARCHAR ok
        vetBadExplicitCasts( conn, "varchar_for_bit_data_col", "VARCHAR (10) FOR BIT DATA", "X'DE'" );
        if ( _supportsXML ) { vetBadExplicitCasts( conn, "xml_col", "XML", "xmlparse( document '<?xml version=\"1.0\" encoding=\"UTF-8\"?> <html/>' preserve whitespace )" ); }

        assertResults
            (
             conn,
             "select key_col, cast( char_col as boolean ), cast( clob_col as boolean ), cast( long_varchar_col as boolean ), cast( varchar_col as boolean )\n" +
             "from string_types where key_col < 18 order by key_col\n",
             new String[][]
             {
                 { "0", "false", "false", "false", "false", },
                 { "1", "true", "true", "true", "true", },
                 { "2", null, null, null, null, },
                 { "3", null, null, null, null, },
                 { "4", "false", "false", "false", "false", },
                 { "5", "true", "true", "true", "true", },
                 { "6", null, null, null, null, },
                 { "7", null, null, null, null, },
                 { "10", "false", "false", "false", "false", },
                 { "11", "true", "true", "true", "true", },
                 { "12", null, null, null, null, },
                 { "13", null, null, null, null, },
                 { "14", "false", "false", "false", "false", },
                 { "15", "true", "true", "true", "true", },
                 { "16", null, null, null, null, },
                 { "17", null, null, null, null, },
             },
             false
             );

//IC see: https://issues.apache.org/jira/browse/DERBY-4730
        assertResults
            (
             conn,
             "values ( cast( true as boolean) )\n",
             new String[][]
             {
                 { "true" },
             },
             false
             );

        vetBadStringCast( conn, "char_col" );
        vetBadStringCast( conn, "clob_col" );
        vetBadStringCast( conn, "long_varchar_col" );
        vetBadStringCast( conn, "varchar_col" );

        vet4704();
    }
    private void vetBadStringCast( Connection conn, String columnName ) throws Exception
    {
        for ( int i = 20; i < 24; i++ )
        {
            expectExecutionError( conn, BAD_CAST, "select cast( " + columnName + " as boolean ) from string_types where key_col = " + i );
        }
    }
    private void vetBadExplicitCasts( Connection conn, String columnName, String dataType, String literal ) throws Exception
    {
        vetBadExplicitCastToBoolean( conn, columnName, literal );
        vetBadExplicitCastFromBoolean( conn, dataType );
    }
    private void vetBadExplicitCastToBoolean( Connection conn, String columnName, String literal ) throws Exception
    {
        expectCompilationError( BAD_CONVERSION, "values ( cast( " + literal + " as boolean ) )" );
        expectCompilationError( BAD_CONVERSION, "select cast( " + columnName + " as boolean ) from all_types\n" );
    }
    private void vetBadExplicitCastFromBoolean( Connection conn, String dataType ) throws Exception
    {
        expectCompilationError( BAD_CONVERSION, "values ( cast( true as " + dataType + " ) )" );
        expectCompilationError( BAD_CONVERSION, "select cast( isindex as " + dataType + " ) from sys.sysconglomerates" );
    }

    /**
     * Regression test case for DERBY-4704. When casting non-nullable VARCHAR
     * columns to BOOLEAN, the result column was marked as non-nullable, even
     * though the VARCHAR could contain the value 'UNKNOWN', in which case
     * the cast should return NULL.
     */
    public void vet4704()
            throws SQLException
    {
        Statement s = createStatement();

        ResultSet rs = s.executeQuery( "select keyCol, stringCol from t_4704 order by keyCol" );
        JDBC.assertNullability(rs, new boolean[] { true, false });

        rs = s.executeQuery( "select keyCol, cast(stringCol as boolean) from t_4704 order by keyCol" );
        JDBC.assertNullability(rs, new boolean[] { true, true });
        JDBC.assertFullResultSet
            (
             rs,
             new String[][]
             {
                 { "0", "false" },
                 { "1", "true" },
                 { "2", null },
             }
             );
    }

    public void test_10_nullabilityOfCastFromLiteral() throws SQLException {
        Statement s = createStatement();
//IC see: https://issues.apache.org/jira/browse/DERBY-4658

        ResultSet rs = s.executeQuery("values cast('true' as boolean)");
        JDBC.assertNullability(rs, new boolean[] { false });
        JDBC.assertSingleValueResultSet(rs, "true");

        rs = s.executeQuery("values cast('false' as boolean)");
        JDBC.assertNullability(rs, new boolean[] { false });
        JDBC.assertSingleValueResultSet(rs, "false");

        rs = s.executeQuery("values cast('unknown' as boolean)");
        JDBC.assertNullability(rs, new boolean[] { true });
        JDBC.assertSingleValueResultSet(rs, null);

        rs = s.executeQuery(
                "values (cast('true' as boolean),\n" +
                "        cast('false' as boolean),\n" +
                "        cast('unknown' as boolean))");
        JDBC.assertNullability(rs, new boolean[] { false, false, true });
        JDBC.assertFullResultSet(
                rs, new String[][] {{ "true", "false", null }});

        rs = s.executeQuery(
                "values (cast ('true' as boolean)),\n" +
                "       (cast ('false' as boolean)),\n" +
                "       (cast ('unknown' as boolean))");
        JDBC.assertNullability(rs, new boolean[] { true });
        JDBC.assertFullResultSet(
                rs, new String[][] { {"true"}, {"false"}, {null} });
    }

    /**
     * <p>
     * Verify that boolean function parameters behave well.
     * </p>
     */
    public void test_11_booleanFunctionParameters() throws Exception
    {
        Connection conn = getConnection();
//IC see: https://issues.apache.org/jira/browse/DERBY-4659

        assertResults
            (
             conn,
             "values ( booleanValue( true ) ), ( booleanValue( false ) ), ( booleanValue( null ) )",
             new String[][]
             {
                 { "True value", },
                 { "False value", },
                 { "Null value", },
             },
             false
             );

        PreparedStatement ps = chattyPrepare( conn, "values ( booleanValue( ? ) )" );

        ps.setBoolean( 1, true );
        assertScalarResult( ps, "True value" );
        
        ps.setBoolean( 1, false );
        assertScalarResult( ps, "False value" );
        
        ps.setNull( 1, Types.BOOLEAN );
        assertScalarResult( ps, "Null value" );

        ps.close();
    }
    private void assertScalarResult( PreparedStatement ps, String result ) throws Exception
    {
        ResultSet rs = ps.executeQuery();
        assertResults( rs, new String[][] {{ result } }, false );
        rs.close();        
    }
                                                                                
    /**
     * <p>
     * Verify that tables with boolean columns behave well.
     * </p>
     */
    public void test_12_booleanColumns() throws Exception
    {
        Connection conn = getConnection();
//IC see: https://issues.apache.org/jira/browse/DERBY-4716

        goodStatement( conn, "insert into boolean_table( key_col, boolean_col ) values ( 0, true ), ( 1, false ), ( 2, null )" );
        assertResults
            (
             conn,
             "select * from boolean_table order by boolean_col",
             new String[][]
             {
                 { "1", "false" },
                 { "0", "true" },
                 { "2", null },
             },
             false
             );
        goodStatement( conn, "delete from boolean_table" );

        vetBadInsert( "bigint_col" );
        vetBadInsert( "blob_col" );
        vetBadInsert( "char_for_bit_data_col" );
        vetBadInsert( "date_col" );
        vetBadInsert( "decimal_col" );
        vetBadInsert( "real_col" );
        vetBadInsert( "double_col" );
        vetBadInsert( "int_col" );
        vetBadInsert( "long_varchar_for_bit_data_col" );
        vetBadInsert( "numeric_col" );
        vetBadInsert( "smallint_col" );
        vetBadInsert( "time_col" );
        vetBadInsert( "timestamp_col" );
        vetBadInsert( "varchar_for_bit_data_col" );

        vetStringInsert( conn, "char_col" );
        vetStringInsert( conn, "clob_col" );
        vetStringInsert( conn, "long_varchar_col" );
        vetStringInsert( conn, "varchar_col" );
        
    }
    private void vetBadInsert( String columnName ) throws Exception
    {
        expectCompilationError
            (
             ILLEGAL_INSERT,
             "insert into boolean_table( key_col, boolean_col ) select key_col, " + columnName + " from all_types"
             );
    }
    private void vetStringInsert( Connection conn, String columnName ) throws Exception
    {
        goodStatement( conn, "insert into boolean_table( key_col, boolean_col ) select key_col, " + columnName + " from string_types where key_col < 18" );
        assertResults
            (
             conn,
             "select * from boolean_table order by key_col",
             new String[][]
             {
                 { "0", "false" },
                 { "1", "true" },
                 { "2", null },
                 { "3", null },
                 { "4", "false" },
                 { "5", "true" },
                 { "6", null },
                 { "7", null },
                 { "10", "false" },
                 { "11", "true" },
                 { "12", null },
                 { "13", null },
                 { "14", "false" },
                 { "15", "true" },
                 { "16", null },
                 { "17", null },
             },
             false
             );
        
        for ( int i = 20; i < 24; i++ )
        {
            expectExecutionError
                ( conn, BAD_CAST, "insert into boolean_table( key_col, boolean_col ) select key_col, " + columnName + " from string_types where key_col = " + i );
        }

        goodStatement( conn, "delete from boolean_table" );
    }
    
    /**
     * <p>
     * Verify that boolean nulls sort at the end with or without an index,
     * the behavior of other datatypes.
     * </p>
     */
    public void test_13_sortOrder() throws Exception
    {
        Connection conn = getConnection();
//IC see: https://issues.apache.org/jira/browse/DERBY-4716

        goodStatement( conn, "create table booleanUnindexed( a boolean )" );
        goodStatement( conn, "create table booleanIndexed( a boolean )" );
        goodStatement( conn, "create index bi on booleanIndexed( a )" );

        goodStatement( conn, "insert into booleanUnindexed( a ) values ( true ), ( null ), ( false )" );
        goodStatement( conn, "insert into booleanIndexed( a ) values ( true ), ( null ), ( false )" );

        String[][] expectedResults = new String[][]
        {
            { "false" },
            { "true" },
            { null },
        };
        
        assertResults
            (
             conn,
             "select * from booleanUnindexed order by a",
             expectedResults,
             false
             );
        assertResults
            (
             conn,
             "select * from booleanIndexed order by a",
             expectedResults,
             false
             );
        
        goodStatement( conn, "drop table booleanUnindexed" );
        goodStatement( conn, "drop table booleanIndexed" );
    }

    /**
     * <p>
     * Verify that you can declare literal defaults for boolean columns.
     * </p>
     */
    public void test_14_defaults() throws Exception
    {
        Connection conn = getConnection();
//IC see: https://issues.apache.org/jira/browse/DERBY-4716

        goodStatement( conn, "create table booleanDefaults( a int, b boolean default true, c boolean default false, d boolean default null )" );
        goodStatement( conn, "insert into booleanDefaults( a ) values ( 0 )" );
        assertResults
            (
             conn,
             "select * from booleanDefaults order by a",
             new String[][]
             {
                 { "0", "true", "false", null }
             },
             false
             );
        
        goodStatement( conn, "alter table booleanDefaults add column e boolean default true" );
        assertResults
            (
             conn,
             "select * from booleanDefaults order by a",
             new String[][]
             {
                 { "0", "true", "false", null, "true" }
             },
             false
             );

        goodStatement( conn, "drop table booleanDefaults" );

        expectCompilationError( BAD_DEFAULT, "create table badDefault( a int, b boolean default 0 )" );
        expectCompilationError( BAD_DEFAULT, "create table badDefault( a int, b boolean default 9.99 )" );
        expectCompilationError( BAD_DEFAULT, "create table badDefault( a int, b boolean default 5e1 )" );
        expectCompilationError( BAD_DEFAULT, "create table badDefault( a int, b boolean default 'false' )" );
        expectCompilationError( BAD_DEFAULT, "create table badDefault( a int, b boolean default X'DE' )" );
    }
    
    /**
     * <p>
     * Verify that you can have boolean expressions in the SELECT list.
     * </p>
     */
    public void test_15_selectList() throws Exception
    {
        Connection conn = getConnection();
//IC see: https://issues.apache.org/jira/browse/DERBY-4720

        assertResults
            (
             conn,
             "select columnnumber > 0 from sys.syscolumns where columnname = 'TABLENAME'",
             new String[][]
             {
                 { "true" }
             },
             false
             );
        
        assertResults
            (
             conn,
             "values 3 > 10",
             new String[][]
             {
                 { "false" }
             },
             false
             );
    }

    /**
     * <p>
     * Verify that you can alter a table and add boolean columns.
     * </p>
     */
    public void test_16_alterTable() throws Exception
    {
        Connection conn = getConnection();
//IC see: https://issues.apache.org/jira/browse/DERBY-4716

        goodStatement( conn, "create table booleanAlter( a int )" );
        goodStatement( conn, "insert into booleanAlter( a ) values ( 0 ), ( 1 ), ( 2 )" );
        goodStatement( conn, "alter table booleanAlter add column b boolean" );
        goodStatement( conn, "alter table booleanAlter add column c boolean default true" );
        goodStatement( conn, "alter table booleanAlter add column d boolean default false" );
        goodStatement( conn, "alter table booleanAlter add column e boolean default null" );        
        assertResults
            (
             conn,
             "select * from booleanAlter order by a",
             new String[][]
             {
                 { "0", null, "true", "false", null },
                 { "1", null, "true", "false", null },
                 { "2", null, "true", "false", null },
             },
             false
             );
        
        goodStatement( conn, "alter table booleanAlter drop column b" );        
        assertResults
            (
             conn,
             "select * from booleanAlter order by a",
             new String[][]
             {
                 { "0", "true", "false", null },
                 { "1", "true", "false", null },
                 { "2", "true", "false", null },
             },
             false
             );

        goodStatement( conn, "drop table booleanAlter" );
    }

    /**
     * <p>
     * Verify that you can create boolean-valued generated columns.
     * </p>
     */
    public void test_17_generatedColumns() throws Exception
    {
        Connection conn = getConnection();

        goodStatement
            (
             conn,
             "create table tree\n" +
             "(\n" +
             "    treeID int primary key generated always as identity,\n" +
             "    latinName varchar( 100 ),\n" +
             "    commonName varchar( 100 ),\n" +
             "    waterPerWeek int,\n" +
             "    minimumSunNeeded int,\n" +
             "    maximumSunTolerated int,\n" +
             "    windTolerated int,\n" +
             "    isHardy boolean generated always as( (waterPerWeek < 10) and (minimumSunNeeded < 5) and (maximumSunTolerated > 90) and (windTolerated > 100) )\n" +
             ")\n"
             );
        generatedMinion( conn );

        // repeat the experiment with an implicitly typed generated column
        goodStatement
            (
             conn,
             "create table tree\n" +
             "(\n" +
             "    treeID int primary key generated always as identity,\n" +
             "    latinName varchar( 100 ),\n" +
             "    commonName varchar( 100 ),\n" +
             "    waterPerWeek int,\n" +
             "    minimumSunNeeded int,\n" +
             "    maximumSunTolerated int,\n" +
             "    windTolerated int,\n" +
             "    isHardy generated always as( (waterPerWeek < 10) and (minimumSunNeeded < 5) and (maximumSunTolerated > 90) and (windTolerated > 100) )\n" +
             ")\n"
             );
        generatedMinion( conn );
    }
    private void generatedMinion( Connection conn ) throws Exception
    {
        goodStatement( conn, "create index hardyTrees on tree( treeID, isHardy )" );
        goodStatement
            (
             conn,
             "insert into tree( latinName, commonName, waterPerWeek, minimumSunNeeded, maximumSunTolerated, windTolerated )\n" +
             "values\n" +
             "( 'tristania conferta', 'brisbane boxwood', 7, 4, 95, 120 ),\n" +
             "( 'acer rubrum', 'red maple', 20, 20, 95, 120 )\n"
             );
        
        assertResults
            (
             conn,
             "select * from tree order by treeID",
             new String[][]
             {
                 { "1", "tristania conferta", "brisbane boxwood", "7", "4", "95", "120", "true" },
                 { "2", "acer rubrum", "red maple", "20", "20", "95", "120", "false" },
             },
             false
             );
        assertResults
            (
             conn,
             "select * from tree where isHardy order by treeID",
             new String[][]
             {
                 { "1", "tristania conferta", "brisbane boxwood", "7", "4", "95", "120", "true" },
             },
             false
             );
        
        goodStatement( conn, "drop table tree" );
    }

    /**
     * <p>
     * Verify views with boolean columns.
     * </p>
     */
    public void test_18_views() throws Exception
    {
        Connection conn = getConnection();

        goodStatement
            (
             conn,
             "create table item\n" +
             "(\n" +
             "    itemID int primary key generated always as identity,\n" +
             "    itemName varchar( 100 ),\n" +
             "    quantityOnHand int,\n" +
             "    quantityOrdered int\n" +
             ")\n"
             );
        goodStatement
            (
             conn,
             "create view orderView( itemID, needsRestocking )\n" +
             "    as select itemID, quantityOrdered > quantityOnHand from item\n"
             );
        goodStatement
            (
             conn,
             "insert into item( itemName, quantityOnHand, quantityOrdered )\n" +
             "values\n" +
             "( 'Bracelet', 100, 200 ),\n" +
             "( 'Glasses', 200, 100 )\n"
             );

        assertResults
            (
             conn,
             "select * from orderView order by itemID",
             new String[][]
             {
                 { "1", "true" },
                 { "2", "false" },
             },
             false
             );
        
        goodStatement( conn, "drop view orderView" );
        goodStatement( conn, "drop table item" );
    }
    
    /**
     * <p>
     * Verify booleans in foreign keys.
     * </p>
     */
    public void test_19_foreignKeys() throws Exception
    {
        Connection conn = getConnection();

        goodStatement
            (
             conn,
             "create table keys\n" +
             "(\n" +
             "    userName varchar( 100 ),\n" +
             "    publicVersion boolean,\n" +
             "    keyValue varchar( 1000 ),\n" +
             "    primary key( publicVersion, userName )\n" +
             ")\n"
            );
        goodStatement
            (
             conn,
             "insert into keys( userName, publicVersion, keyValue )\n" +
             "values\n" +
             "( 'Robert', true, 'abcdefghij' ),\n" +
             "( 'Robert', false, 'mnopqrstuv' )\n"
            );
        goodStatement
            (
             conn,
             "create table messages\n" +
             "(\n" +
             "    messageID int primary key generated always as identity,\n" +
             "    userName varchar( 100 ),\n" +
             "    publicVersion boolean,\n" +
             "    messageText clob,\n" +
             "    foreign key ( publicVersion, userName ) references keys( publicVersion, userName )\n" +
             ")\n"
             );
        goodStatement
            (
             conn,
             "insert into messages( userName, publicVersion, messageText )\n" +
             "values\n" +
             "( 'Robert', false, 'Twas brillig' )\n"
             );

        assertResults
            (
             conn,
             "select * from messages order by messageID",
             new String[][]
             {
                 { "1", "Robert", "false", "Twas brillig" },
             },
             false
             );
        
        goodStatement( conn, "drop table messages" );
        goodStatement( conn, "drop table keys" );
    }
    
    /**
     * <p>
     * Verify booleans in check constraints.
     * </p>
     */
    public void test_20_checkConstraints() throws Exception
    {
        Connection conn = getConnection();

        goodStatement
            (
             conn,
             "create table remes\n" +
             "(\n" +
             "    remesID int primary key generated always as identity,\n" +
             "    name varchar( 100 ),\n" +
             "    isInsect boolean,\n" +
             "    isArachnid boolean,\n" +
             "    check ( isInsect or isArachnid )\n" +
             ")\n"
            );
        goodStatement
            (
             conn,
             "insert into remes( name, isInsect, isArachnid )\n" +
             "values\n" +
             "( 'black widow', false, true ),\n" +
             "( 'house fly', true, false )\n"
            );
        
        assertResults
            (
             conn,
             "select * from remes order by remesID",
             new String[][]
             {
                 { "1", "black widow", "false", "true" },
                 { "2", "house fly", "true", "false" },
             },
             false
             );
        
        goodStatement( conn, "drop table remes" );
    }

    /**
     * <p>
     * Verify triggers on boolean columns.
     * </p>
     */
    public void test_21_triggers() throws Exception
    {
        Connection conn = getConnection();

        goodStatement
            (
             conn,
             "create table alumnus\n" +
             "(\n" +
             "    alumnusID int primary key generated always as identity,\n" +
             "    name varchar( 100 ),\n" +
             "    living boolean\n" +
             ")\n"
            );
        goodStatement
            (
             conn,
             "create table livenessChange\n" +
             "(\n" +
             "    eventID int primary key generated always as identity,\n" +
             "    alumnusID int, \n" +
             "    living boolean\n" +
             ")\n"
            );
        goodStatement
            (
             conn,
             "create trigger trig_after_update_row_trigger\n" +
             "after update of living\n" +
             "on alumnus\n" +
             "referencing new as newRow\n" +
             "for each row\n" +
             "insert into livenessChange( alumnusID, living ) values ( newRow.alumnusID, newRow.living )\n"
            );
        goodStatement( conn, "insert into alumnus( name, living ) values ( 'Fred', true ), ( 'Monica', true )" );
        goodStatement( conn, "update alumnus set living = false where name = 'Fred'" );

        
        assertResults
            (
             conn,
             "select * from livenessChange order by eventID",
             new String[][]
             {
                 { "1", "1", "false" },
             },
             false
             );

        goodStatement( conn, "drop table alumnus" );
        goodStatement( conn, "drop table livenessChange" );
    }

    /**
     * Test that the binary operators return the expected values for all
     * possible combinations of operands.
     */
    public void test_22_binary_operators() throws SQLException {
        setAutoCommit(false);
//IC see: https://issues.apache.org/jira/browse/DERBY-5971

        // Create a table with two columns containing all combinations of
        // legal boolean values.
        Statement s = createStatement();
        s.execute("create table combos(b1 boolean, b2 boolean)");
        assertUpdateCount(s, 9,
                "insert into combos select * from "
                + "(values true, false, cast(null as boolean)) v1, "
                + "(values true, false, cast(null as boolean)) v2");

        // Equals operator (=)
        JDBC.assertFullResultSet(
                s.executeQuery(
                    "select b1, b2, b1 = b2 from combos order by 1, 2"),
                new String[][] {
                    { "false", "false", "true"  },
                    { "false", "true" , "false" },
                    { "false",  null  ,  null   },
                    { "true" , "false", "false" },
                    { "true" , "true" , "true"  },
                    { "true" ,  null  ,  null   },
                    {  null  , "false",  null   },
                    {  null  , "true" ,  null   },
                    {  null  ,  null  ,  null   },
                });

        // Not equals operator (<>)
        JDBC.assertFullResultSet(
                s.executeQuery(
                    "select b1, b2, b1 <> b2 from combos order by 1, 2"),
                new String[][] {
                    { "false", "false", "false" },
                    { "false", "true" , "true"  },
                    { "false",  null  ,  null   },
                    { "true" , "false", "true"  },
                    { "true" , "true" , "false" },
                    { "true" ,  null  ,  null   },
                    {  null  , "false",  null   },
                    {  null  , "true" ,  null   },
                    {  null  ,  null  ,  null   },
                });

        // Less than operator (<)
        JDBC.assertFullResultSet(
                s.executeQuery(
                    "select b1, b2, b1 < b2 from combos order by 1, 2"),
                new String[][] {
                    { "false", "false", "false" },
                    { "false", "true" , "true"  },
                    { "false",  null  ,  null   },
                    { "true" , "false", "false" },
                    { "true" , "true" , "false" },
                    { "true" ,  null  ,  null   },
                    {  null  , "false",  null   },
                    {  null  , "true" ,  null   },
                    {  null  ,  null  ,  null   },
                });

        // Greater than operator (>)
        JDBC.assertFullResultSet(
                s.executeQuery(
                    "select b1, b2, b1 > b2 from combos order by 1, 2"),
                new String[][] {
                    { "false", "false", "false" },
                    { "false", "true" , "false" },
                    { "false",  null  ,  null   },
                    { "true" , "false", "true"  },
                    { "true" , "true" , "false" },
                    { "true" ,  null  ,  null   },
                    {  null  , "false",  null   },
                    {  null  , "true" ,  null   },
                    {  null  ,  null  ,  null   },
                });

        // Less than or equals operator (<=)
        JDBC.assertFullResultSet(
                s.executeQuery(
                    "select b1, b2, b1 <= b2 from combos order by 1, 2"),
                new String[][] {
                    { "false", "false", "true"  },
                    { "false", "true" , "true"  },
                    { "false",  null  ,  null   },
                    { "true" , "false", "false" },
                    { "true" , "true" , "true"  },
                    { "true" ,  null  ,  null   },
                    {  null  , "false",  null   },
                    {  null  , "true" ,  null   },
                    {  null  ,  null  ,  null   },
                });

        // Greater than or equals operator (>=)
        JDBC.assertFullResultSet(
                s.executeQuery(
                    "select b1, b2, b1 >= b2 from combos order by 1, 2"),
                new String[][] {
                    { "false", "false", "true"  },
                    { "false", "true" , "false" },
                    { "false",  null  ,  null   },
                    { "true" , "false", "true"  },
                    { "true" , "true" , "true"  },
                    { "true" ,  null  ,  null   },
                    {  null  , "false",  null   },
                    {  null  , "true" ,  null   },
                    {  null  ,  null  ,  null   },
                });

        // AND operator
        JDBC.assertFullResultSet(
                s.executeQuery(
                    "select b1, b2, b1 and b2 from combos order by 1, 2"),
                new String[][] {
                    { "false", "false", "false" },
                    { "false", "true" , "false" },
                    { "false",  null  , "false" },
                    { "true" , "false", "false" },
                    { "true" , "true" , "true"  },
                    { "true" ,  null  ,  null   },
                    {  null  , "false", "false" },
                    {  null  , "true" ,  null   },
                    {  null  ,  null  ,  null   },
                });

        // OR operator
        JDBC.assertFullResultSet(
                s.executeQuery(
                    "select b1, b2, (b1 or b2) from combos order by 1, 2"),
                new String[][] {
                    { "false", "false", "false" },
                    { "false", "true" , "true"  },
                    { "false",  null  ,  null   },
                    { "true" , "false", "true"  },
                    { "true" , "true" , "true"  },
                    { "true" ,  null  , "true"  },
                    {  null  , "false",  null   },
                    {  null  , "true" , "true"  },
                    {  null  ,  null  ,  null   },
                });
    }

    /**
     * Verify that SELECT DISTINCT works as expected on boolean columns.
     */
    public void test_23_select_distinct() throws SQLException {
        setAutoCommit(false);
//IC see: https://issues.apache.org/jira/browse/DERBY-5971

        Statement s = createStatement();
        assertUpdateCount(s, 6, "insert into boolean_table(boolean_col) "
                + "values true, true, false, false, null, null");

        JDBC.assertUnorderedResultSet(
            s.executeQuery("select distinct boolean_col from boolean_table"),
            new String[][] { {"true"}, {"false"}, {null} });
    }
    
    /**
     * <p>
     * Verify fix for DERBY-4890.
     * </p>
     */
    public void test_4890() throws Exception
    {
        Connection conn = getConnection();

        goodStatement( conn, "delete from boolean_table" );

        PreparedStatement ps = chattyPrepare( conn, "values ( cast ( ? as boolean ), cast ( ? as boolean ) )" );
        ps.setString( 1, "true" );
        ps.setString( 2, "false" );
        ResultSet rs = ps.executeQuery();

        rs.next();
        assertTrue( rs.getBoolean( 1 ) );
        assertFalse( rs.getBoolean( 2 ) );

        rs.close();
        ps.close();

        ps = chattyPrepare
            (
             conn,
             "insert into boolean_table( key_col, boolean_col ) values ( 1, ? ), ( 2, ? )"
             );
        ps.setString( 1, "true" );
        ps.setString( 2, "false" );
        ps.execute();

        assertResults
            (
             conn,
             "select * from boolean_table order by key_col",
             new String[][]
             {
                 { "1",  "true" },
                 { "2",  "false" },
             },
             false
             );

        goodStatement( conn, "delete from boolean_table" );
    }
    
    /**
     * <p>
     * Verify fix for DERBY-4890.
     * </p>
     */
    public void test_4889() throws Exception
    {
        Connection conn = getConnection();

        minion_4889( conn, 0, false );
        minion_4889( conn, 1, true );
        minion_4889( conn, 2, true );
    }
    private void minion_4889( Connection conn, int value, boolean expectedBooleanResult )
        throws Exception
    {
        goodStatement( conn, "delete from t_4889" );
        
        PreparedStatement ps = chattyPrepare
            (
             conn,
             "insert into t_4889( key_col, setter_col, boolean_col ) values ( ?, ?, ? )"
             );

        ps.setInt( 1, 1 );
        ps.setString( 2, "setByte" );
        ps.setByte( 3, (byte) value );
        ps.execute();

        ps.setInt( 1, 2 );
        ps.setString( 2, "setShort" );
        ps.setShort( 3, (short) value );
        ps.execute();

        ps.setInt( 1, 3 );
        ps.setString( 2, "setInt" );
        ps.setInt( 3, value );
        ps.execute();

        ps.setInt( 1, 4 );
        ps.setString( 2, "setLong" );
        ps.setLong( 3, (long) value );
        ps.execute();

        ps.setInt( 1, 5 );
        ps.setString( 2, "setObject( Byte )" );
//IC see: https://issues.apache.org/jira/browse/DERBY-6856
        ps.setObject( 3, (byte) value );
        ps.execute();

        ps.setInt( 1, 6 );
        ps.setString( 2, "setObject( Short )" );
        ps.setObject( 3, (short) value );
        ps.execute();

        ps.setInt( 1, 7 );
        ps.setString( 2, "setObject( Integer )" );
        ps.setObject( 3, value );
        ps.execute();

        ps.setInt( 1, 8 );
        ps.setString( 2, "setObject( Long )" );
        ps.setObject( 3, (long) value );
        ps.execute();

        String stringValue = Boolean.toString( (value != 0) );
        assertResults
            (
             conn,
             "select * from t_4889 order by key_col",
             new String[][]
             {
                 { "1",  "setByte", stringValue },
                 { "2",  "setShort", stringValue },
                 { "3",  "setInt", stringValue },
                 { "4",  "setLong", stringValue },
                 { "5",  "setObject( Byte )", stringValue },
                 { "6",  "setObject( Short )", stringValue },
                 { "7",  "setObject( Integer )", stringValue },
                 { "8",  "setObject( Long )", stringValue },
             },
             false
             );

    }

    /**
     * Verify fix for DERBY-4964 - failure to convert string to boolean with
     * the client driver.
     */
    public void test_4964() throws SQLException {
        PreparedStatement ps = prepareStatement("values cast(? as boolean)");

        // These should work
        test_4964_minion(ps, null, null);
        test_4964_minion(ps, "unknown", null);
        test_4964_minion(ps, "UNKNOWN", null);
        test_4964_minion(ps, "unKnoWn", null);
        test_4964_minion(ps, "  unknown  ", null);
        test_4964_minion(ps, "true", Boolean.TRUE);
        test_4964_minion(ps, "TRUE", Boolean.TRUE);
        test_4964_minion(ps, "TrUe", Boolean.TRUE);
        test_4964_minion(ps, "  true  ", Boolean.TRUE);
        test_4964_minion(ps, "false", Boolean.FALSE);
        test_4964_minion(ps, "FALSE", Boolean.FALSE);
        test_4964_minion(ps, "FaLsE", Boolean.FALSE);
        test_4964_minion(ps, "FaLsE", Boolean.FALSE);
        test_4964_minion(ps, "  false  ", Boolean.FALSE);

        // These should fail
        test_4964_minion(ps, "0", BAD_CAST);
        test_4964_minion(ps, "1", BAD_CAST);
        test_4964_minion(ps, "2", BAD_CAST);
        test_4964_minion(ps, "null", BAD_CAST);
        test_4964_minion(ps, "true true", BAD_CAST);
        test_4964_minion(ps, "false false", BAD_CAST);
    }

    /**
     * Set a boolean parameter using a string value and verify that we get
     * the expected result.
     *
     * @param ps a prepared statement that takes a boolean parameter
     * @param input input string for the parameter
     * @param expectedValue the expected result; either the expected Boolean return
     * value if the operation is expected to succeed, or the SQLState of the
     * exception if it is expected to fail
     */
    private void test_4964_minion(
            PreparedStatement ps, String input, Object expectedValue)
        throws SQLException
    {
        // If the expected value is a string, it denotes the SQLState of an
        // expected failure
        boolean shouldFail = expectedValue instanceof String;

        Object[][] rows = { { expectedValue } };

        // test setString(int, String)
        try {
            ps.setString(1, input);
            JDBC.assertFullResultSet(ps.executeQuery(), rows, false);
            assertFalse(shouldFail);
        } catch (SQLException sqle) {
            if (shouldFail) {
                assertSQLState((String) expectedValue, sqle);
            } else {
                throw sqle;
            }
        }

        // test setObject(int, Object)
        try {
            ps.setObject(1, input);
            JDBC.assertFullResultSet(ps.executeQuery(), rows, false);
            assertFalse(shouldFail);
        } catch (SQLException sqle) {
            if (shouldFail) {
                assertSQLState((String) expectedValue, sqle);
            } else {
                throw sqle;
            }
        }

        // test setObject(int, Object, int) with various target types
        int[] types = { Types.BIT, Types.BOOLEAN, Types.CHAR, Types.VARCHAR };
        for (int i = 0; i < types.length; i++) {
            try {
                ps.setObject(1, input, types[i]);
                JDBC.assertFullResultSet(ps.executeQuery(), rows, false);
                assertFalse(shouldFail);
            } catch (SQLException sqle) {
                if (shouldFail) {
                    assertSQLState((String) expectedValue, sqle);
                } else {
                    throw sqle;
                }
            }
        }
    }

    /**
     * Verify fix for DERBY-4965 - conversion from boolean to char results
     * in 1/0 instead of true/false.
     */
    public void test_4965() throws SQLException {
        String[] stringTypes = { "CHAR(10)", "VARCHAR(10)", "LONG VARCHAR" };
        for (int i = 0; i < stringTypes.length; i++) {
            PreparedStatement ps = prepareStatement(
                    "values cast(? as " + stringTypes[i] + ")");

            // Test setBoolean()
            ps.setBoolean(1, true);
            JDBC.assertSingleValueResultSet(ps.executeQuery(), "true");
            ps.setBoolean(1, false);
            JDBC.assertSingleValueResultSet(ps.executeQuery(), "false");

            // Test setObject(int, Object)
            ps.setObject(1, Boolean.TRUE);
            JDBC.assertSingleValueResultSet(ps.executeQuery(), "true");
            ps.setObject(1, Boolean.FALSE);
            JDBC.assertSingleValueResultSet(ps.executeQuery(), "false");

            // Test setObject(int, Object, int)
            int[] targetTypes = {
                Types.BIT, Types.BOOLEAN,
                Types.CHAR, Types.VARCHAR, Types.LONGVARCHAR,
            };
            for (int j = 0; j < targetTypes.length; j++) {
                ps.setObject(1, Boolean.TRUE, targetTypes[j]);
                JDBC.assertSingleValueResultSet(ps.executeQuery(), "true");
                ps.setObject(1, Boolean.FALSE, targetTypes[j]);
                JDBC.assertSingleValueResultSet(ps.executeQuery(), "false");
            }
        }

        String[] intTypes = { "SMALLINT", "INTEGER", "BIGINT" };
        for (int i = 0; i < intTypes.length; i++) {
            PreparedStatement ps = prepareStatement(
                    "values cast(? as " + intTypes[i] + ")");

            // Test setBoolean()
            ps.setBoolean(1, true);
            JDBC.assertSingleValueResultSet(ps.executeQuery(), "1");
            ps.setBoolean(1, false);
            JDBC.assertSingleValueResultSet(ps.executeQuery(), "0");

            // Test setObject(int, Object)
            ps.setObject(1, Boolean.TRUE);
            JDBC.assertSingleValueResultSet(ps.executeQuery(), "1");
            ps.setObject(1, Boolean.FALSE);
            JDBC.assertSingleValueResultSet(ps.executeQuery(), "0");

            // Test setObject(int, Object, int)
            int[] targetTypes = {
                Types.BIT, Types.BOOLEAN,
                Types.SMALLINT, Types.INTEGER, Types.BIGINT,
            };
            for (int j = 0; j < targetTypes.length; j++) {
                ps.setObject(1, Boolean.TRUE, targetTypes[j]);
                JDBC.assertSingleValueResultSet(ps.executeQuery(), "1");
                ps.setObject(1, Boolean.FALSE, targetTypes[j]);
                JDBC.assertSingleValueResultSet(ps.executeQuery(), "0");
            }
        }
    }

    /**
     * Verify fix for DERBY-5042, where updateBoolean() and updateObject()
     * would fail on a BOOLEAN column when using the client driver.
     */
    public void test_5042_updateBoolean() throws SQLException {
        setAutoCommit(false);

        Statement s = createStatement(ResultSet.TYPE_FORWARD_ONLY,
                                      ResultSet.CONCUR_UPDATABLE);
        s.execute("create table derby5042(b boolean, i int, c char(10))");

        ResultSet rs = s.executeQuery("select * from derby5042");

        // Test updateBoolean() on various column types
        rs.moveToInsertRow();
        rs.updateBoolean("B", true); // Used to fail with client driver
        rs.updateBoolean("I", true);
        rs.updateBoolean("C", true);
        rs.insertRow();

        // Test updateObject() with a java.lang.Boolean on various column types
        rs.moveToInsertRow();
        rs.updateObject("B", Boolean.FALSE); // Used to fail with client driver
        rs.updateObject("I", Boolean.FALSE);
        rs.updateObject("C", Boolean.FALSE);
        rs.insertRow();

        rs.close();

        JDBC.assertFullResultSet(
                s.executeQuery("select * from derby5042 order by 1,2,3"),
                new String[][]{
                    {"false", "0", "false"},
                    {"true", "1", "true"}});
    }

    /**
     * Verify fix for DERBY-5063 - updateBytes() should fail when invoked
     * on boolean columns.
     */
    public void test_5063_updateBytes() throws SQLException {
        setAutoCommit(false);

        Statement s = createStatement();
        s.execute("create table derby5063(b boolean)");

        PreparedStatement ps = prepareStatement("select b from derby5063",
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);

        byte[] bytes = "abc".getBytes();

        // Test updateBytes()
        ResultSet rs = ps.executeQuery();
        rs.moveToInsertRow();
        try {
            rs.updateBytes(1, bytes);
            fail("updateBytes should fail");
        } catch (SQLException sqle) {
            assertSQLState(ILLEGAL_UPDATE, sqle);
        }
        rs.close();

        // setObject() should also fail when the argument is a byte array
        rs = ps.executeQuery();
        rs.moveToInsertRow();
        try {
            rs.updateObject(1, bytes);
            fail("updateObject should fail");
        } catch (SQLException sqle) {
            assertSQLState(ILLEGAL_UPDATE, sqle);
        }
        rs.close();
    }

    /**
     * Verify that you can use CREATE TABLE AS SELECT to create
     * empty tables with BOOLEAN columns.
     */
    public void test_5918() throws Exception
    {
        setAutoCommit(false);
//IC see: https://issues.apache.org/jira/browse/DERBY-5918

        Statement s = createStatement();
        s.execute("create table derby5918_1(b boolean)");
        s.execute("create table derby5918_2 as select * from derby5918_1 with no data");
        s.executeUpdate("insert into derby5918_2 values ( true )");
        
        assertResults
            (
             getConnection(),
             "select * from derby5918_2",
             new String[][]
             {
                 { "true" },
             },
             false
             );
    }

    /**
     * Verify that you can use AND and OR expressions as BOOLEAN values.
     * Regression test case for DERBY-5972.
     */
    public void test_5972() throws SQLException {
        // Disable auto-commit so that tearDown() can roll back any changes.
        setAutoCommit(false);

        Statement s = createStatement();

        // Test boolean expressions in select list. Used to fail with syntax
        // errors with OR expressions.
        JDBC.assertSingleValueResultSet(
                s.executeQuery("select true and false from sysibm.sysdummy1"),
                "false");
        JDBC.assertSingleValueResultSet(
                s.executeQuery("select true or false from sysibm.sysdummy1"),
                "true");
        assertCompileError(NON_BOOLEAN_OPERAND,
                "select 1 and 2 from sysibm.sysdummy1");
        assertCompileError(NON_BOOLEAN_OPERAND,
                "select 1 or 2 from sysibm.sysdummy1");

        // Test boolean expressions in VALUES statements. Used to fail with
        // syntax errors with OR expressions.
        JDBC.assertSingleValueResultSet(
                s.executeQuery("values true and false"), "false");
        JDBC.assertSingleValueResultSet(
                s.executeQuery("values true or false"), "true");
        assertCompileError(NON_BOOLEAN_OPERAND, "values 1 and 2");
        assertCompileError(NON_BOOLEAN_OPERAND, "values 1 or 2");

        // Test boolean expressions as parameters in function calls. Used to
        // result in syntax errors with OR expressions.
        JDBC.assertSingleValueResultSet(s.executeQuery(
                "values booleanValue(true and false)"), "False value");
        JDBC.assertSingleValueResultSet(s.executeQuery(
                "values booleanValue(true or false)"), "True value");
        assertCompileError(NON_BOOLEAN_OPERAND, "values booleanValue(1 and 2)");
        assertCompileError(NON_BOOLEAN_OPERAND, "values booleanValue(1 or 2)");

        // Test boolean expressions in UPDATE statements. Used to fail with
        // syntax errors with OR expressions.
        s.execute("create table d5972(b boolean)");
        assertUpdateCount(s, 0, "update d5972 set b = true and false");
        assertUpdateCount(s, 0, "update d5972 set b = true or false");
        assertCompileError(NON_BOOLEAN_OPERAND, "update d5972 set b = 1 and 2");
        assertCompileError(NON_BOOLEAN_OPERAND, "update d5972 set b = 1 or 2");

        // Used to work correctly. Verify for completeness.
        JDBC.assertSingleValueResultSet(s.executeQuery(
                "values case when true and false then 1 else 0 end"), "0");
        JDBC.assertSingleValueResultSet(s.executeQuery(
                "values case when true or false then 1 else 0 end"), "1");
        assertCompileError(NON_BOOLEAN_OPERAND,
                "values case when 1 and 2 then 1 else 0 end");
        assertCompileError(NON_BOOLEAN_OPERAND,
                "values case when 1 or 2 then 1 else 0 end");
    }
    
    /**
     * Some BOOLEAN expressions used to be transformed to non-equivalent
     * IN lists. Verify that they now return the correct results.
     * Regression test case for DERBY-6363.
     */
    public void test_6363() throws SQLException {
        Statement s = createStatement();
        s.execute("create table d6363(a int, b char)");
        s.execute("insert into d6363 values (1, 'a'), (2, 'b'), (3, 'a'), "
                + "(4, 'b'), (5, 'a'), (6, 'b')");

        JDBC.assertFullResultSet(s.executeQuery(
            "select a, ((b = 'a' or b = 'b') and a < 4), "
                    + "((b = 'a' or b = 'c' or b = 'b') and a < 4), "
                    + "((b = 'a' or (b = 'c' or b = 'b')) and a < 4), "
                    + "((b = 'a' or b in ('c', 'b')) and a < 4), "
                    + "(a < 4 and (b = 'a' or b = 'b')) "
                    + "from d6363 order by a"),
            new String[][] {
                { "1", "true", "true", "true", "true", "true" },
                { "2", "true", "true", "true", "true", "true" },
                { "3", "true", "true", "true", "true", "true" },
                { "4", "false", "false", "false", "false", "false" },
                { "5", "false", "false", "false", "false", "false" },
                { "6", "false", "false", "false", "false", "false" },
            });

        JDBC.assertFullResultSet(s.executeQuery(
            "select a, b, "
            + "case when ((b = 'a' or b = 'b') and a < 4) "
            + "then 'x' else '-' end, "
            + "case when (a < 4 and (b = 'a' or b = 'b')) "
            + "then 'y' else '-' end "
            + "from d6363 order by a"),
            new String[][] {
                { "1", "a", "x", "y" },
                { "2", "b", "x", "y" },
                { "3", "a", "x", "y" },
                { "4", "b", "-", "-" },
                { "5", "a", "-", "-" },
                { "6", "b", "-", "-" },
            });
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // SQL ROUTINES
    //
    ///////////////////////////////////////////////////////////////////////////////////

    public static Blob makeSimpleBlob() throws SQLException
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-4932
        return new HarmonySerialBlob( new byte[] { 1 } );
    }
    
    public static String booleanValue( Boolean b )
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-4659
        if ( b == null ) { return "Null value"; }
        else if ( b.booleanValue() ) { return "True value"; }
        else { return "False value"; }
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // MINIONS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /** Return true if the SQL routine exists */
    private boolean routineExists( Connection conn, String functionName ) throws Exception
    {
        PreparedStatement ps = chattyPrepare( conn, "select count (*) from sys.sysaliases where alias = ?" );
        ps.setString( 1, functionName );

        ResultSet rs = ps.executeQuery();
        rs.next();

        boolean retval = rs.getInt( 1 ) > 0 ? true : false;

        rs.close();
        ps.close();

        return retval;
    }

    /** Return true if the table exists */
    private boolean tableExists( Connection conn, String tableName ) throws Exception
    {
        PreparedStatement ps = chattyPrepare( conn, "select count (*) from sys.systables where tablename = ?" );
        ps.setString( 1, tableName );

        ResultSet rs = ps.executeQuery();
        rs.next();

        boolean retval = rs.getInt( 1 ) > 0 ? true : false;

        rs.close();
        ps.close();

        return retval;
    }

}
