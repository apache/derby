/**
 *  Derby - Class org.apache.derbyTesting.functionTests.tests.lang.SimpleJsonTest
 *  
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.derbyTesting.functionTests.tests.lang;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;

import junit.framework.Test;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.SupportFilesSetup;
import org.apache.derbyTesting.junit.TestConfiguration;
import org.apache.derbyTesting.functionTests.util.PrivilegedFileOpsForTests;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

/**
 * <p>
 * Basic test of the optional tool which provides JSON support functions
 * which use the simple json library from https://code.google.com/p/json-simple/.
 * </p>
 */
public class SimpleJsonTest extends BaseJDBCTestCase
{
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTANTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    private static  final   String  TAB = "  ";
    private static  final   String  USER_ERROR = "38000";
    private static  final   String  OUT_OF_RANGE = "22003";

    private static  final   String  THERMOSTAT_READINGS =
//IC see: https://issues.apache.org/jira/browse/DERBY-6825
        "[\n" +
        " {\n" +
        "   \"id\": 1,\n" +
        "   \"temperature\": 70.3,\n" +
        "   \"fanOn\": true\n" +
        " },\n" +
        " {\n" +
        "   \"id\": 2,\n" +
        "   \"temperature\": 65.5,\n" +
        "   \"fanOn\": false\n" +
        " }\n" +
        "]";


    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTRUCTORS
    //
    ///////////////////////////////////////////////////////////////////////////////////

	public SimpleJsonTest(String name) {
		super(name);
	}
	
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // JUnit MACHINERY
    //
    ///////////////////////////////////////////////////////////////////////////////////
    
	public static Test suite()
    {
        BaseTestSuite suite = new BaseTestSuite("SimpleJsonTest");

        suite.addTest( TestConfiguration.defaultSuite( SimpleJsonTest.class ) );

//IC see: https://issues.apache.org/jira/browse/DERBY-6825
        return new SupportFilesSetup
            (
             suite,
             new String[]
             { 
                "functionTests/tests/lang/thermostatReadings.dat",
//IC see: https://issues.apache.org/jira/browse/DERBY-6825
                "functionTests/tests/lang/json.dat",
             }
            );
	}
	
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // JUnit TESTS
    //
    ///////////////////////////////////////////////////////////////////////////////////
    
	public void testBasic_001() throws Exception
    {
        Connection  conn = getConnection();

        // create the json support types and functions
        goodStatement( conn, "call syscs_util.syscs_register_tool( 'simpleJson', true )" );

        createSchema_001( conn );

        assertResults
            (
             conn,
             "values prettyPrint( toJSON( " +
             "'select * from thermostatReadings order by \"id\", \"sampleTime\"' ) )",
             new String[][]
             {
                 {
                     "[\n" +
                     "  {\n" +
                     "    \"fanOn\" : false, \n" +
                     "    \"id\" : 1, \n" +
                     "    \"sampleTime\" : \"2015-07-08 04:03:20.0\", \n" +
                     "    \"temperature\" : 65.5\n" +
                     "  }, \n" +
                     "  {\n" +
                     "    \"fanOn\" : true, \n" +
                     "    \"id\" : 1, \n" +
                     "    \"sampleTime\" : \"2015-07-08 13:03:20.0\", \n" +
                     "    \"temperature\" : 70.1\n" +
                     "  }, \n" +
                     "  {\n" +
                     "    \"fanOn\" : false, \n" +
                     "    \"id\" : 2, \n" +
                     "    \"sampleTime\" : \"2015-07-08 03:03:20.0\", \n" +
                     "    \"temperature\" : 64.5\n" +
                     "  }, \n" +
                     "  {\n" +
                     "    \"fanOn\" : true, \n" +
                     "    \"id\" : 2, \n" +
                     "    \"sampleTime\" : \"2015-07-08 16:03:20.0\", \n" +
                     "    \"temperature\" : 72.1\n" +
                     "  }\n" +
                     "]"
                 }
             },
             true
             );

        assertResults
            (
             conn,
             "values prettyPrint( toJSON( 'select \"id\", max( \"temperature\" ) \"maxTemp\" from thermostatReadings group by \"id\"' ) )",
             new String[][]
             {
                 {
                     "[\n" +
                     "  {\n" +
                     "    \"id\" : 1, \n" +
                     "    \"maxTemp\" : 70.1\n" +
                     "  }, \n" +
                     "  {\n" +
                     "    \"id\" : 2, \n" +
                     "    \"maxTemp\" : 72.1\n" +
                     "  }\n" +
                     "]"
                 }
             },
             true
             );

        assertResults
            (
             conn,
             "values prettyPrint( toJSON( 'select \"id\", max( \"temperature\" ) \"maxTemp\" from thermostatReadings where \"id\" = ? group by \"id\"', '2' ) )",
             new String[][]
             {
                 {
                     "[\n" +
                     "  {\n" +
                     "    \"id\" : 2, \n" +
                     "    \"maxTemp\" : 72.1\n" +
                     "  }\n" +
                     "]"
                 }
             },
             true
             );

        dropSchema_001( conn );
        
        // drop the json support types and functions
        goodStatement( conn, "call syscs_util.syscs_register_tool( 'simpleJson', false )" );
	}
    private void    createSchema_001( Connection conn ) throws Exception
    {
        goodStatement
            (
             conn,
             "create table thermostatReadings\n" +
             "(\n" +
             "\"id\" int,\n" +
             "\"temperature\" double,\n" +
             "\"fanOn\" boolean,\n" +
             "\"sampleTime\" timestamp,\n" +
             "primary key( \"id\", \"sampleTime\" )\n" +
             ")\n"
             );

        goodStatement
            (
             conn,
             "insert into thermostatReadings values\n" +
             "( 1, 65.5, false, timestamp( '2015-07-08 04:03:20') ),\n" +
             "( 1, 70.1, true, timestamp( '2015-07-08 13:03:20') ),\n" +
             "( 2, 64.5, false, timestamp( '2015-07-08 03:03:20') ),\n" +
             "( 2, 72.1, true, timestamp( '2015-07-08 16:03:20') )\n"
             );
        
        goodStatement
            (
             conn,
             "create function prettyPrint( doc JSONArray ) returns varchar( 32672 )\n" +
             "language java parameter style java no sql\n" +
             "external name '" + getClass().getName() + ".prettyPrint'\n"
             );
    }
    private void    dropSchema_001( Connection conn ) throws Exception
    {
        goodStatement( conn, "drop function prettyPrint" );
        goodStatement( conn, "drop table thermostatReadings" );
    }
    
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Test the jsonification of Derby's datatypes.
     * </p>
     */
	public void testDatatypes_002() throws Exception
    {
        Connection  conn = getConnection();

        // create the json support types and functions
        goodStatement( conn, "call syscs_util.syscs_register_tool( 'simpleJson', true )" );

        createSchema_002( conn );

        assertResults
            (
             conn,
             "values( prettyPrint( toJSON( 'select * from all_types order by key_col' ) ) )",
             new String[][]
             {
                 {
                     "[\n" +
                     "  {\n" +
                     "    \"BIGINT_COL\" : 0, \n" +
                     "    \"BLOB_COL\" : \"01\", \n" +
                     "    \"CHAR_COL\" : \"0         \", \n" +
                     "    \"CHAR_FOR_BIT_DATA_COL\" : \"de202020202020202020\", \n" +
                     "    \"CLOB_COL\" : \"0\", \n" +
                     "    \"DATE_COL\" : \"1994-02-23\", \n" +
                     "    \"DECIMAL_COL\" : 0.0, \n" +
                     "    \"DOUBLE_COL\" : 0.0, \n" +
                     "    \"INT_COL\" : 0, \n" +
                     "    \"JSON_ARRAY_COL\" : [\n" +
                     "      {\n" +
                     "        \"A\" : 1, \n" +
                     "        \"B\" : true\n" +
                     "      }, \n" +
                     "      {\n" +
                     "        \"A\" : 2, \n" +
                     "        \"B\" : false\n" +
                     "      }\n" +
                     "    ], \n" +
                     "    \"KEY_COL\" : 0, \n" +
                     "    \"LONG_VARCHAR_COL\" : \"0\", \n" +
                     "    \"LONG_VARCHAR_FOR_BIT_DATA_COL\" : \"de\", \n" +
                     "    \"NUMERIC_COL\" : 0.0, \n" +
                     "    \"PRICE_COL\" : \"Price( USD, 9.99000, 2009-10-16 14:24:43.0 )\", \n" +
                     "    \"REAL_COL\" : 0.0, \n" +
                     "    \"SMALLINT_COL\" : 0, \n" +
                     "    \"TIMESTAMP_COL\" : \"1962-09-23 03:23:34.234\", \n" +
                     "    \"TIME_COL\" : \"15:09:02\", \n" +
                     "    \"VARCHAR_COL\" : \"0\", \n" +
                     "    \"VARCHAR_FOR_BIT_DATA_COL\" : \"de\"\n" +
                     "  }, \n" +
                     "  {\n" +
                     "    \"BIGINT_COL\" : null, \n" +
                     "    \"BLOB_COL\" : null, \n" +
                     "    \"CHAR_COL\" : null, \n" +
                     "    \"CHAR_FOR_BIT_DATA_COL\" : null, \n" +
                     "    \"CLOB_COL\" : null, \n" +
                     "    \"DATE_COL\" : null, \n" +
                     "    \"DECIMAL_COL\" : null, \n" +
                     "    \"DOUBLE_COL\" : null, \n" +
                     "    \"INT_COL\" : null, \n" +
                     "    \"JSON_ARRAY_COL\" : null, \n" +
                     "    \"KEY_COL\" : 1, \n" +
                     "    \"LONG_VARCHAR_COL\" : null, \n" +
                     "    \"LONG_VARCHAR_FOR_BIT_DATA_COL\" : null, \n" +
                     "    \"NUMERIC_COL\" : null, \n" +
                     "    \"PRICE_COL\" : null, \n" +
                     "    \"REAL_COL\" : null, \n" +
                     "    \"SMALLINT_COL\" : null, \n" +
                     "    \"TIMESTAMP_COL\" : null, \n" +
                     "    \"TIME_COL\" : null, \n" +
                     "    \"VARCHAR_COL\" : null, \n" +
                     "    \"VARCHAR_FOR_BIT_DATA_COL\" : null\n" +
                     "  }\n" +
                     "]"                                                                                                     }
             },
             true
             );

        dropSchema_002( conn );

        // drop the json support types and functions
        goodStatement( conn, "call syscs_util.syscs_register_tool( 'simpleJson', false )" );
    }
    private void createSchema_002( Connection conn ) throws Exception
    {
        goodStatement
            (
             conn,
             "create type Price external name 'org.apache.derbyTesting.functionTests.tests.lang.Price' language java"
             );
        goodStatement
            (
             conn,
             "create function makePrice( currencyCode char( 3 ), amount decimal( 31, 5 ), timeInstant Timestamp )\n" +
             "returns Price language java parameter style java no sql\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.Price.makePrice'\n"
             );
        goodStatement
            (
             conn,
             "create function makeSimpleBlob( ) returns blob\n" +
             "language java parameter style java no sql deterministic\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.BooleanValuesTest.makeSimpleBlob'\n"
             );
        goodStatement
            (
             conn,
             "create table dummy( a int, b boolean )"
             );
        goodStatement
            (
             conn,
             "insert into dummy values ( 1, true ), ( 2, false )"
             );
        goodStatement
            (
             conn,
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
             "    numeric_col  NUMERIC(5,2),\n" +
             "    smallint_col  SMALLINT,\n" +
             "    time_col  TIME,\n" +
             "    timestamp_col  TIMESTAMP,\n" +
             "    varchar_col  VARCHAR(10),\n" +
             "    varchar_for_bit_data_col  VARCHAR (10) FOR BIT DATA,\n" +
             "    price_col price,\n" +
             "    json_array_col jsonArray\n" +
             ")\n"
             );
        goodStatement
            (
             conn,
             "insert into all_types( key_col ) values ( 1 )"
             );
        goodStatement
            (
             conn,
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
             "    numeric_col,\n" +
             "    smallint_col,\n" +
             "    time_col,\n" +
             "    timestamp_col,\n" +
             "    varchar_col,\n" +
             "    varchar_for_bit_data_col,\n" +
             "    price_col,\n" +
             "    json_array_col\n" +
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
             "    0.00,\n" +
             "    0,\n" +
             "    time('15:09:02'),\n" +
             "    timestamp('1962-09-23 03:23:34.234'),\n" +
             "    '0',\n" +
             "    X'DE',\n" +
             "    makePrice( 'USD', cast( 9.99 as decimal( 31, 5 ) ), timestamp('2009-10-16 14:24:43') ),\n" +
             "    toJSON( 'select * from dummy order by a' ) \n" +
             ")\n"
             );
        goodStatement
            (
             conn,
             "create function prettyPrint( doc JSONArray ) returns varchar( 32672 )\n" +
             "language java parameter style java no sql\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.SimpleJsonTest.prettyPrint'\n"
             );
    }
    private void dropSchema_002( Connection conn ) throws Exception
    {
        goodStatement
            (
             conn,
             "drop function makePrice"
             );
        goodStatement
            (
             conn,
             "drop function makeSimpleBlob"
             );
        goodStatement
            (
             conn,
             "drop table all_types"
             );
        goodStatement
            (
             conn,
             "drop type price restrict"
             );
        goodStatement
            (
             conn,
             "drop function prettyPrint"
             );

    }

    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Test errors.
     * </p>
     */
	public void testNegative_003() throws Exception
    {
        Connection  conn = getConnection();

        // can't load the tool redundantly
        goodStatement( conn, "call syscs_util.syscs_register_tool( 'simpleJson', true )" );

        expectExecutionError
            (
             conn,
             USER_ERROR,
             "call syscs_util.syscs_register_tool( 'simpleJson', true )"
             );
        
        goodStatement( conn, "call syscs_util.syscs_register_tool( 'simpleJson', false )" );

        // can't unload the tool redundantly
        expectExecutionError
            (
             conn,
             USER_ERROR,
             "call syscs_util.syscs_register_tool( 'simpleJson', false )"
             );

        // need to specify all ? parameters
        goodStatement( conn, "call syscs_util.syscs_register_tool( 'simpleJson', true )" );

        expectExecutionError
            (
             conn,
             USER_ERROR,
             "values( toJSON( 'select * from sys.systables where tablename = ?' ) )"
             );

        goodStatement( conn, "call syscs_util.syscs_register_tool( 'simpleJson', false )" );
    }

    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Test the SimpleJsonVTI.
     * </p>
     */
	public void testVTI_004() throws Exception
    {
        Connection  conn = getConnection();
//IC see: https://issues.apache.org/jira/browse/DERBY-6825

        goodStatement( conn, "call syscs_util.syscs_register_tool( 'simpleJson', true )" );

        // declare a table function for reading a string
        goodStatement
            (
             conn,
             "create function thermostatReadings( jsonDocument JSONArray )\n" +
             "returns table\n" +
             "(\n" +
             "\"id\" int,\n" +
             "\"temperature\" float,\n" +
             "\"fanOn\" boolean\n" +
             ")\n" +
             "language java parameter style derby_jdbc_result_set contains sql\n" +
             "external name 'org.apache.derby.optional.api.SimpleJsonVTI.readArray'\n"
             );

        PreparedStatement   ps;
        ResultSet           rs;

        // turn a JSON document string into a ResultSet
        ps = conn.prepareStatement
            (
             "select * from table\n" +
             "( thermostatReadings( readArrayFromString(?) ) ) t order by \"id\""
             );
        ps.setString( 1, THERMOSTAT_READINGS );
        rs = ps.executeQuery();
        assertResults
            (
             rs,
             new String[][]
             {
                 { "1", "70.3", "true" },
                 { "2", "65.5", "false" }
             },
             true
             );
        rs.close();
        ps.close();

        // make a ResultSet out of a file containing JSON text
        File    inputFile = SupportFilesSetup.getReadOnly( "thermostatReadings.dat" );
        String[][] fileReadings = new String[][]
            {
                { "1", "70.3", "true" },
                { "2", "65.5", "false" },
                { "3", "60.5", "false" },
            };
        ps = conn.prepareStatement
            (
             "select * from table\n" +
             "( thermostatReadings( readArrayFromFile( ?, 'UTF-8' ) ) ) t order by \"id\""
             );
        ps.setString( 1, PrivilegedFileOpsForTests.getAbsolutePath( inputFile ) );
        rs = ps.executeQuery();
        assertResults(rs, fileReadings, true );
        rs.close();
        ps.close();

        // make a ResultSet out of an URL which points to a file containing JSON text
        ps = conn.prepareStatement
            (
             "select * from table\n" +
             "( thermostatReadings( readArrayFromURL( ?, 'UTF-8' ) ) ) t order by \"id\""
             );
        String  inputFileURL = PrivilegedFileOpsForTests.toURI(inputFile ).toURL().toString();
        ps.setString( 1, inputFileURL);
        rs = ps.executeQuery();
        assertResults(rs, fileReadings, true );
        rs.close();
        ps.close();

        goodStatement( conn, "drop function thermostatReadings" );
        goodStatement( conn, "call syscs_util.syscs_register_tool( 'simpleJson', false )" );
    }

    /**
     * <p>
     * Test the datatypes understood by SimpleJsonVTI.
     * </p>
     */
	public void testVTIdatatypes005() throws Exception
    {
        Connection  conn = getConnection();
//IC see: https://issues.apache.org/jira/browse/DERBY-6825

        goodStatement( conn, "call syscs_util.syscs_register_tool( 'simpleJson', true )" );

        vetDatatype_005
            (
             conn,
             "smallint",
             new String[][]
             {
                 { "abc","true", "127" },
                 { "def", "false", "1" },
                 { "ghi", null, "345" },
                 { "lmn", "true", "-1" },    
             }
             );
        vetDatatype_005
            (
             conn,
             "int",
             new String[][]
             {
                 { "abc","true", "127" },
                 { "def", "false", "1" },
                 { "ghi", null, "345" },
                 { "lmn", "true", "-1" },    
             }
             );
        vetDatatype_005
            (
             conn,
             "bigint",
             new String[][]
             {
                 { "abc","true", "127" },
                 { "def", "false", "1" },
                 { "ghi", null, "345" },
                 { "lmn", "true", "9223372036854775807" },    
             }
             );
        vetDatatype_005
            (
             conn,
             "float",
             new String[][]
             {
                 { "abc","true", "127.0" },
                 { "def", "false", "1.2" },
                 { "ghi", null, "345.67" },
                 { "lmn", "true", "9.223372036854776E18" },    
             }
             );
        vetDatatype_005
            (
             conn,
             "double",
             new String[][]
             {
                 { "abc","true", "127.0" },
                 { "def", "false", "1.2" },
                 { "ghi", null, "345.67" },
                 { "lmn", "true", "9.223372036854776E18" },    
             }
             );

        goodStatement( conn, "call syscs_util.syscs_register_tool( 'simpleJson', false )" );
    }
    private void vetDatatype_005
        (
         Connection conn,
         String datatype,
         String[][] expectedResults
         )
        throws Exception
    {
        createFunction_005( conn, datatype );

        PreparedStatement   ps = conn.prepareStatement
            (
             "select * from table\n" +
             "( f_" + datatype + "( readArrayFromFile( ?, 'UTF-8' ) )) t\n"
             );
        File    inputFile = SupportFilesSetup.getReadOnly( "json.dat" );
        ps.setString( 1, PrivilegedFileOpsForTests.getAbsolutePath( inputFile ) );

        ResultSet           rs;

        rs = ps.executeQuery();
        assertResults(rs, expectedResults, true );
        rs.close();

        // the first two rows have numeric values which won't raise
        // truncation exceptions when fetched into tinyint
        rs = ps.executeQuery();
        rs.next();
        assertEquals( (byte) 127, rs.getByte( "NUM_COL" ) );
        rs.next();
        assertEquals( (byte) 1, rs.getByte( "NUM_COL" ) );
        rs.close();
        
        ps.close();

        dropFunction_005( conn, datatype );
    }
    private void createFunction_005( Connection conn, String datatype )
        throws Exception
    {
        goodStatement
            (
             conn,
             "create function f_" + datatype + "( jsonArray JSONArray )\n" +
             "returns table\n" +
             "(\n" +
             "  str_col varchar( 10 ),\n" +
             "  bool_col boolean,\n" +
             "  num_col " + datatype + "\n" +
             ")\n" +
             "language java parameter style derby_jdbc_result_set contains sql\n" +
             "external name 'org.apache.derby.optional.api.SimpleJsonVTI.readArray'\n"
             );
    }
    private void dropFunction_005( Connection conn, String datatype )
        throws Exception
    {
        goodStatement( conn, "drop function f_" + datatype );
    }

    /**
     * <p>
     * Test the arrayToClob() function.
     * </p>
     */
	public void testArrayToClob_006() throws Exception
    {
        Connection  conn = getConnection();
//IC see: https://issues.apache.org/jira/browse/DERBY-6825

        goodStatement( conn, "call syscs_util.syscs_register_tool( 'simpleJson', true )" );
        goodStatement( conn, "create table employee( fullName varchar( 100 ) )" );
        goodStatement( conn, "create table docs( stringDoc varchar( 32672 ) )" );
        goodStatement( conn, "insert into employee values ( 'Fred Flintstone' ), ( 'Barney Rubble' )" );
        goodStatement( conn, "insert into docs values( arrayToClob( toJSON( 'select * from employee' ) ) )" );

        assertResults
            (
             conn,
             "select * from docs",
             new String[][]
             {
                 {
                     "[{\"FULLNAME\":\"Fred Flintstone\"},{\"FULLNAME\":\"Barney Rubble\"}]"
                 }
             },
             true
             );

        goodStatement( conn, "drop table docs" );
        goodStatement( conn, "drop table employee" );
        goodStatement( conn, "call syscs_util.syscs_register_tool( 'simpleJson', false )" );
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // FUNCTIONS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Pretty-print a JSONArray.
     * </p>
     */
    public static String prettyPrint( JSONArray array )
    {
        StringBuilder   buffer = new StringBuilder();

        prettyPrintArray( buffer, 0, array );
        
        return buffer.toString();
    }

    private static  void    prettyPrintArray
        (
         StringBuilder buffer,
         int indentLevel,
         JSONArray array
         )
    {
        buffer.append( "[" );
        int cellCount = array.size();
        if ( cellCount > 0 )
        {
            for ( int i = 0; i < cellCount; i++ )
            {
                if ( i > 0 ) { buffer.append( ", " ); }
                indent( buffer, indentLevel + 1 );
                prettyPrint( buffer, indentLevel + 1, array.get( i ) );
            }
            indent( buffer, indentLevel );
        }
        buffer.append( "]" );
    }

    private static  void    prettyPrintObject
        (
         StringBuilder buffer,
         int indentLevel,
         JSONObject obj
         )
    {
        buffer.append( "{" );
        int keyCount = obj.size();
        if ( keyCount > 0 )
        {
            // alphabetize the keys
            Object[]    keys = obj.keySet().toArray();
            Arrays.sort( keys );
            for ( int i = 0; i < keyCount; i++ )
            {
                Object  key = keys[ i ];
                if ( i > 0 ) { buffer.append( ", " ); }
                indent( buffer, indentLevel + 1 );
                buffer.append( doubleQuote( (String) key ) );
                buffer.append( " : " );
                prettyPrint( buffer, indentLevel + 1, obj.get( key ) );
            }
            indent( buffer, indentLevel );
        }
        buffer.append( "}" );
    }

    private static void prettyPrint
        (
         StringBuilder buffer,
         int indentLevel,
         Object obj
         )
    {
        if ( obj == null ) { buffer.append( "null" ); }
        else if ( obj instanceof JSONArray )
        { prettyPrintArray( buffer, indentLevel, (JSONArray) obj ); }
        else if ( obj instanceof JSONObject )
        { prettyPrintObject( buffer, indentLevel, (JSONObject) obj ); }
        else if ( (obj instanceof Number) || (obj instanceof Boolean) )
        { buffer.append( obj.toString() ); }
        else { buffer.append( doubleQuote( obj.toString() ) ); }
    }

    private static  void indent( StringBuilder buffer, int indentLevel )
    {
        buffer.append( "\n" );
        for ( int i = 0; i < indentLevel; i++ ) { buffer.append( TAB ); }
    }

    private static  String  doubleQuote( String raw )
    {
        return "\"" + raw + "\"";
    }
}
