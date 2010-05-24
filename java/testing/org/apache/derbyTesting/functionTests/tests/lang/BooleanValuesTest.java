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

import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.derbyTesting.junit.TestConfiguration;
import org.apache.derbyTesting.junit.JDBC;
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
        Test result = (TestSuite) TestConfiguration.defaultSuite(BooleanValuesTest.class);

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
                "    varchar_for_bit_data_col  VARCHAR (10) FOR BIT DATA\n"
                 );
            if ( _supportsXML )
            {
                buffer.append( "    ,xml_col  xml\n" );
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
     * </ul>
     */
    public void test_01_datatypeCount() throws Exception
    {
        Connection conn = getConnection();

        ResultSet rs = conn.getMetaData().getTypeInfo();
        int actualTypeCount = 0;
        while ( rs.next() ) { actualTypeCount++; }
        rs.close();

        assertEquals( 21, actualTypeCount );
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
    
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // SQL ROUTINES
    //
    ///////////////////////////////////////////////////////////////////////////////////

    public static Blob makeSimpleBlob()
    {
        return new StringColumnVTI.SimpleBlob( new byte[] { 1 } );
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
