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

import java.lang.reflect.*;
import java.io.*;
import java.sql.*;
import java.text.NumberFormat;
import java.util.Arrays;
import java.util.ArrayList;

import org.apache.derby.shared.common.reference.JDBC40Translation;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.DatabasePropertyTestSetup;
import org.apache.derbyTesting.junit.Decorator;
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

    private static  final   String  UTF8 = "UTF-8";

    private static  final   int MAX_VARIABLE_DATA_TYPE_LENGTH = 32700;
    
    // functions to drop at teardown time
    private static  final   String[]    FUNCTION_NAMES =
    {
        "SIMPLEFUNCTIONTABLE",
        "invert",
        "returnsACoupleRows",
        "getXXXrecord",
        "returnsAllLegalDatatypes",
        "missingConstructor",
        "zeroArgConstructorNotPublic",
        "constructorException",
        "goodVTICosting",
        "allStringTypesFunction",
        "coercionFunction",
    };
    
    // tables to drop at teardown time
    private static  final   String[]    TABLE_NAMES =
    {
        "allStringTypesTable",
        "fooTestTable",
    };
    
    private static  final   String[][]  SIMPLE_ROWS =
    {
        { "who", "put" },
        { "the", "bop" },
        { (String) null, "in" },
        { "the", (String) null },
    };
    
    private static  final   String[][] SIMPLY_ROWS =
    {
        { "the       ", (String) null },
        { "the       ", "bop       " },
        { "who       ", "put       " }, 
        { (String) null, "in        " },
    };
    
    private static  final   String[][]  DOUBLY_SIMPLE_ROWS =
    {
        { "the       ", (String) null },
        { "the       ", "bop       " },
        { "the       ", (String) null },
        { "the       ", "bop       " },
        { "who       ", "put       " },
        { "who       ", "put       " },
        { (String) null, "in        " },
        { (String) null, "in        " },
    };
    
    private static  final   String[][]  BOOLEAN_ROWS =
    {
        { "tRuE", "true" },
        { "fAlSe", "false" },
    };
    
    private static  final   String[][]  BULK_INSERT_ROWS =
    {
        { "1", "red" },
        { "2", "blue" },
    };
    
    private static final    String[][] BULK_INSERT_SELF_JOIN_ROWS =
    {
        { "2", "blue" },
    };
    
    private static  final   String[][]  DOUBLY_INSERTED_ROWS =
    {
        { "1", "red" },
        { "1", "red" },
        { "2", "blue" },
        { "2", "blue" },
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
            null,   // BOOLEAN
        },
    };

    private static  final   String  EXPECTED_GET_XXX_CALLS =
        "getLong " +            // BIGINT
        "getBlob " +            // BLOB
        "getString " +          // CHAR
        "getBytes " +           // CHAR FOR BIT DATA
        "getString " +          // CLOB
        "getDate " +            // DATE
        "getBigDecimal " +      // DECIMAL
        "getDouble " +          // DOUBLE
        "getDouble " +          // DOUBLE PRECISION
        "getFloat " +           // FLOAT( 23 )
        "getDouble " +          // FLOAT( 24 )
        "getInt " +             // INTEGER
        "getString " +          // LONG VARCHAR
        "getBytes " +           // LONG VARCHAR FOR BIT DATA
        "getBigDecimal " +      // NUMERIC
        "getFloat " +           // REAL
        "getShort " +           // SMALLINT
        "getTime " +            // TIME
        "getTimestamp " +       // TIMESTAMP
        "getString " +          // VARCHAR
        "getBytes " +           // VARCHAR FOR BIT DATA
        "getBoolean ";            // BOOLEAN

    private static  final   String  EXPECTED_GET_XXX_CALLS_JSR169 =
        "getLong " +            // BIGINT
        "getBlob " +            // BLOB
        "getString " +          // CHAR
        "getBytes " +           // CHAR FOR BIT DATA
        "getString " +          // CLOB
        "getDate " +            // DATE
        "getString " +      // DECIMAL
        "getDouble " +          // DOUBLE
        "getDouble " +          // DOUBLE PRECISION
        "getFloat " +           // FLOAT( 23 )
        "getDouble " +          // FLOAT( 24 )
        "getInt " +             // INTEGER
        "getString " +          // LONG VARCHAR
        "getBytes " +           // LONG VARCHAR FOR BIT DATA
        "getString " +      // NUMERIC
        "getFloat " +           // REAL
        "getShort " +           // SMALLINT
        "getTime " +            // TIME
        "getTimestamp " +       // TIMESTAMP
        "getString " +          // VARCHAR
        "getBytes " +           // VARCHAR FOR BIT DATA
        "getBoolean ";            // BOOLEAN

    private static  final   String[]  STRING_TYPES =
    {
        "CHAR( 20 )",
        //"CLOB", long string types are not comparable
        //"LONG VARCHAR", long string types are not comparable
        "VARCHAR( 20 )",
    };
    
    private static  final   int[]  STRING_JDBC_TYPES =
    {
        Types.CHAR,
        //Types.CLOB, long string types are not comparable
        //Types.LONGVARCHAR, long string types are not comparable
        Types.VARCHAR,
    };
    
    private static  final   String[][]  ALL_STRING_TYPES_ROWS =
    {
        {
            "char col            ",   // CHAR
            //"clob col", long string types are not comparable
            //"long varchar col",   // LONG VARCHAR long string types are not comparable
            "varchar col",   // VARCHAR
        },
    };

    private static  final   String  SFT_RETURN_TYPE = "TABLE ( \"INTCOL\" INTEGER, \"VARCHARCOL\" VARCHAR(10) )";
    private static  final   String  RADT_RETURN_TYPE = "TABLE ( \"COLUMN0\" BIGINT, \"COLUMN1\" BLOB(2147483647), \"COLUMN2\" CHAR(10), \"COLUMN3\" CHAR (10) FOR BIT DATA, \"COLUMN4\" CLOB(2147483647), \"COLUMN5\" DATE, \"COLUMN6\" DECIMAL(5,0), \"COLUMN7\" DOUBLE, \"COLUMN8\" DOUBLE, \"COLUMN9\" REAL, \"COLUMN10\" DOUBLE, \"COLUMN11\" INTEGER, \"COLUMN12\" LONG VARCHAR, \"COLUMN13\" LONG VARCHAR FOR BIT DATA, \"COLUMN14\" NUMERIC(5,0), \"COLUMN15\" REAL, \"COLUMN16\" SMALLINT, \"COLUMN17\" TIME, \"COLUMN18\" TIMESTAMP, \"COLUMN19\" VARCHAR(10), \"COLUMN20\" VARCHAR (10) FOR BIT DATA, \"COLUMN21\" BOOLEAN )";
    
    private static  final   Integer FUNCTION_COLUMN_IN = new Integer( JDBC40Translation.FUNCTION_PARAMETER_IN );
    private static  final   Integer FUNCTION_RETURN_VALUE = new Integer( JDBC40Translation.FUNCTION_RETURN );
    private static  final   Integer FUNCTION_RESULT_COLUMN = new Integer( JDBC40Translation.FUNCTION_COLUMN_RESULT );

    private static  final   Integer FUNCTION_RETURNS_TABLE = new Integer( JDBC40Translation.FUNCTION_RETURNS_TABLE );


    private static  final   Integer JDBC_TYPE_OTHER = new Integer( Types.OTHER );
    private static  final   Integer JDBC_TYPE_INT = new Integer( Types.INTEGER );
    private static  final   Integer JDBC_TYPE_VARCHAR = new Integer( Types.VARCHAR );
    private static  final   Integer JDBC_TYPE_BIGINT = new Integer( Types.BIGINT );
    private static  final   Integer JDBC_TYPE_BLOB = new Integer( Types.BLOB );
    private static  final   Integer JDBC_TYPE_CHAR = new Integer( Types.CHAR );
    private static  final   Integer JDBC_TYPE_CLOB = new Integer( Types.CLOB );
    private static  final   Integer JDBC_TYPE_DATE = new Integer( Types.DATE );
    private static  final   Integer JDBC_TYPE_DECIMAL = new Integer( Types.DECIMAL );
    private static  final   Integer JDBC_TYPE_DOUBLE = new Integer( Types.DOUBLE );
    private static  final   Integer JDBC_TYPE_REAL = new Integer( Types.REAL );
    private static  final   Integer JDBC_TYPE_NUMERIC = new Integer( Types.NUMERIC );
    private static  final   Integer JDBC_TYPE_SMALLINT = new Integer( Types.SMALLINT );
    private static  final   Integer JDBC_TYPE_TIME = new Integer( Types.TIME );
    private static  final   Integer JDBC_TYPE_TIMESTAMP = new Integer( Types.TIMESTAMP );
    private static  final   Integer JDBC_TYPE_BINARY = new Integer( Types.BINARY );
    private static  final   Integer JDBC_TYPE_LONGVARBINARY = new Integer( Types.LONGVARBINARY );
    private static  final   Integer JDBC_TYPE_LONGVARCHAR = new Integer( Types.LONGVARCHAR );
    private static  final   Integer JDBC_TYPE_VARBINARY = new Integer( Types.VARBINARY );
    private static  final   Integer JDBC_TYPE_BOOLEAN = new Integer( Types.BOOLEAN );

    private static  final   Integer PRECISION_NONE = new Integer( 0 );
    private static  final   Integer PRECISION_INTEGER = new Integer( 10 );
    private static  final   Integer PRECISION_BIGINT = new Integer( 19 );
    private static  final   Integer PRECISION_MAX = new Integer( 2147483647 );

    private static  final   Integer LENGTH_UNDEFINED = new Integer( -1 );
    private static  final   Integer LENGTH_INTEGER = new Integer( 4 );
    private static  final   Integer LENGTH_BIGINT = new Integer( 40 );
    private static  final   Integer LENGTH_MAX = new Integer( 2147483647 );

    private static  final   Integer  SCALE_UNDEFINED = null;
    private static  final   Integer  SCALE_INTEGER = new Integer( 0 );

    private static  final   Integer  RADIX_UNDEFINED = null;
    private static  final   Integer  RADIX_INTEGER = new Integer( 10 );

    private static  final   Object  NO_CATALOG = null;
    private static  final   String  RETURN_VALUE_NAME = "";
    private static  final   Integer ALLOWS_NULLS = new Integer( JDBC40Translation.FUNCTION_NULLABLE );
    private static  final   Object  EMPTY_REMARKS = null;
    private static  final   Object  UNDEFINED_CHAR_OCTET_LENGTH = null;
    private static  final   String  IS_NULLABLE = "YES";
    
    private static  final   Integer ROW_ORDER_RETURN_VALUE = new Integer( -1 );
    private static  final   Integer ROW_ORDER_1 = new Integer( 0 );
    private static  final   Integer ROW_ORDER_2 = new Integer( 1 );
    
    private static  final   Integer POSITION_RETURN_VALUE = new Integer( 0 );
    private static  final   Integer POSITION_ARG_1 = new Integer( 1 );
    private static  final   Integer POSITION_ARG_2 = new Integer( 2 );

    private static  final   Integer ARG_COUNT_0 = new Integer( 0 );
    private static  final   Integer ARG_COUNT_1 = new Integer( 1 );
    private static  final   Integer ARG_COUNT_2 = new Integer( 2 );
    private static  final   Integer ARG_COUNT_3 = new Integer( 3 );

    private static final JDBC.GeneratedId GENERIC_NAME = new JDBC.GeneratedId();

    /** Expected rows from getFunctions() for  SIMPLEFUNCTIONTABLE */
    private static final Object[][]  GF_SFT= {
        { NO_CATALOG, "APP", "SIMPLEFUNCTIONTABLE", "com.scores.proc.Functions.weighQuestion", FUNCTION_RETURNS_TABLE,  GENERIC_NAME }
    };

    /** Expected rows from getFunctionColumns() for  SIMPLEFUNCTIONTABLE */
    private static final Object[][]  GFC_SFT= {
        {
            NO_CATALOG,
            "APP",
            "SIMPLEFUNCTIONTABLE",
            "INTCOL",
            FUNCTION_RESULT_COLUMN,
            JDBC_TYPE_INT,
            "INTEGER",
            PRECISION_INTEGER,
            LENGTH_INTEGER,
            SCALE_INTEGER,
            RADIX_INTEGER,
            ALLOWS_NULLS,
            EMPTY_REMARKS,
            UNDEFINED_CHAR_OCTET_LENGTH,
            new Integer( 1 ),           // ORDINAL_POSITION
            IS_NULLABLE,
            GENERIC_NAME,
            ARG_COUNT_0,
            ROW_ORDER_1
        },
        {
            NO_CATALOG,
            "APP",
            "SIMPLEFUNCTIONTABLE",
            "VARCHARCOL",
            FUNCTION_RESULT_COLUMN,
            JDBC_TYPE_VARCHAR,
            "VARCHAR",
            new Integer( 10 ),               // PRECISION
            new Integer( 20 ),              // LENGTH
            SCALE_UNDEFINED,
            RADIX_UNDEFINED,
            ALLOWS_NULLS,
            EMPTY_REMARKS,
            new Integer( 20 ),          // CHAR_OCTET_LENGTH
            new Integer( 2 ),           // ORDINAL_POSITION
            IS_NULLABLE,
            GENERIC_NAME,
            ARG_COUNT_0,
            ROW_ORDER_2
        },
    };

    /** Expected rows from getFunctions() for  returnsAllLegalDatatypes */
    private static final Object[][]  GF_RADT= {
        { NO_CATALOG, "APP", "RETURNSALLLEGALDATATYPES", "org.apache.derbyTesting.functionTests.tests.lang.TableFunctionTest.returnsAllLegalDatatypes", FUNCTION_RETURNS_TABLE,  GENERIC_NAME }
    };

    /** Expected rows from getFunctionColumns() for  returnsAllLegalDatatypes */
    private static final Object[][]  GFC_RADT= {
        {
            NO_CATALOG,
            "APP",
            "RETURNSALLLEGALDATATYPES",
            "INTARGUMENT",
            FUNCTION_COLUMN_IN,
            JDBC_TYPE_INT,
            "INTEGER",
            PRECISION_INTEGER,
            LENGTH_INTEGER,
            SCALE_INTEGER,
            RADIX_INTEGER,
            ALLOWS_NULLS,
            EMPTY_REMARKS,
            UNDEFINED_CHAR_OCTET_LENGTH,
            POSITION_ARG_1,
            IS_NULLABLE,
            GENERIC_NAME,
            ARG_COUNT_2,
            ROW_ORDER_1
        },
        {
            NO_CATALOG,
            "APP",
            "RETURNSALLLEGALDATATYPES",
            "VARCHARARGUMENT",
            FUNCTION_COLUMN_IN,
            JDBC_TYPE_VARCHAR,
            "VARCHAR",
            new Integer( 10 ),               // PRECISION
            new Integer( 20 ),              // LENGTH
            SCALE_UNDEFINED,
            RADIX_UNDEFINED,
            ALLOWS_NULLS,
            EMPTY_REMARKS,
            new Integer( 20 ),          // CHAR_OCTET_LENGTH
            POSITION_ARG_2,
            IS_NULLABLE,
            GENERIC_NAME,
            ARG_COUNT_2,
            ROW_ORDER_2
        },

        {
            NO_CATALOG,
            "APP",
            "RETURNSALLLEGALDATATYPES",
            "COLUMN0",
            FUNCTION_RESULT_COLUMN,
            JDBC_TYPE_BIGINT,
            "BIGINT",
            PRECISION_BIGINT,
            LENGTH_BIGINT,
            SCALE_INTEGER,
            RADIX_INTEGER,
            ALLOWS_NULLS,
            EMPTY_REMARKS,
            UNDEFINED_CHAR_OCTET_LENGTH,
            new Integer( 1 ),           // ORDINAL_POSITION
            IS_NULLABLE,
            GENERIC_NAME,
            ARG_COUNT_2,
            new Integer( 2 )
        },
        {
            NO_CATALOG,
            "APP",
            "RETURNSALLLEGALDATATYPES",
            "COLUMN1",
            FUNCTION_RESULT_COLUMN,
            JDBC_TYPE_BLOB,
            "BLOB",
            PRECISION_MAX ,
            LENGTH_MAX,
            SCALE_UNDEFINED,
            RADIX_UNDEFINED,
            ALLOWS_NULLS,
            EMPTY_REMARKS,
            UNDEFINED_CHAR_OCTET_LENGTH,
            new Integer( 2 ),           // ORDINAL_POSITION
            IS_NULLABLE,
            GENERIC_NAME,
            ARG_COUNT_2,
            new Integer( 3 )
        },
        {
            NO_CATALOG,
            "APP",
            "RETURNSALLLEGALDATATYPES",
            "COLUMN2",
            FUNCTION_RESULT_COLUMN,
            JDBC_TYPE_CHAR,
            "CHAR",
            new Integer( 10 ) ,     // PRECISION
            new Integer( 20 ),         // LENGTH
            SCALE_UNDEFINED,
            RADIX_UNDEFINED,
            ALLOWS_NULLS,
            EMPTY_REMARKS,
            new Integer( 20 ),    // CHAR_OCTET_LENGTH
            new Integer( 3 ),           // ORDINAL_POSITION
            IS_NULLABLE,
            GENERIC_NAME,
            ARG_COUNT_2,
            new Integer( 4 )
        },
        {
            NO_CATALOG,
            "APP",
            "RETURNSALLLEGALDATATYPES",
            "COLUMN3",
            FUNCTION_RESULT_COLUMN,
            JDBC_TYPE_BINARY,
            "CHAR () FOR BIT DATA",
            new Integer( 10 ) ,     // PRECISION
            new Integer( 10 ),         // LENGTH
            SCALE_UNDEFINED,
            RADIX_UNDEFINED,
            ALLOWS_NULLS,
            EMPTY_REMARKS,
            new Integer( 10 ),    // CHAR_OCTET_LENGTH
            new Integer( 4 ),           // ORDINAL_POSITION
            IS_NULLABLE,
            GENERIC_NAME,
            ARG_COUNT_2,
            new Integer( 5 )
        },
        {
            NO_CATALOG,
            "APP",
            "RETURNSALLLEGALDATATYPES",
            "COLUMN4",
            FUNCTION_RESULT_COLUMN,
            JDBC_TYPE_CLOB,
            "CLOB",
            PRECISION_MAX ,     // PRECISION
            LENGTH_MAX,         // LENGTH
            SCALE_UNDEFINED,
            RADIX_UNDEFINED,
            ALLOWS_NULLS,
            EMPTY_REMARKS,
            UNDEFINED_CHAR_OCTET_LENGTH,
            new Integer( 5 ),           // ORDINAL_POSITION
            IS_NULLABLE,
            GENERIC_NAME,
            ARG_COUNT_2,
            new Integer( 6 )
        },
        {
            NO_CATALOG,
            "APP",
            "RETURNSALLLEGALDATATYPES",
            "COLUMN5",
            FUNCTION_RESULT_COLUMN,
            JDBC_TYPE_DATE,
            "DATE",
            new Integer( 10 ) ,     // PRECISION
            new Integer( 6 ),         // LENGTH
            new Integer( 0 ),       // SCALE
            new Integer( 10 ),    // RADIX
            ALLOWS_NULLS,
            EMPTY_REMARKS,
            UNDEFINED_CHAR_OCTET_LENGTH,
            new Integer( 6 ),           // ORDINAL_POSITION
            IS_NULLABLE,
            GENERIC_NAME,
            ARG_COUNT_2,
            new Integer( 7 )
        },
        {
            NO_CATALOG,
            "APP",
            "RETURNSALLLEGALDATATYPES",
            "COLUMN6",
            FUNCTION_RESULT_COLUMN,
            JDBC_TYPE_DECIMAL,
            "DECIMAL",
            new Integer( 5 ) ,     // PRECISION
            new Integer( 14 ),         // LENGTH
            new Integer( 0 ),       // SCALE
            new Integer( 10 ),    // RADIX
            ALLOWS_NULLS,
            EMPTY_REMARKS,
            UNDEFINED_CHAR_OCTET_LENGTH,
            new Integer( 7 ),           // ORDINAL_POSITION
            IS_NULLABLE,
            GENERIC_NAME,
            ARG_COUNT_2,
            new Integer( 8 )
        },
        {
            NO_CATALOG,
            "APP",
            "RETURNSALLLEGALDATATYPES",
            "COLUMN7",
            FUNCTION_RESULT_COLUMN,
            JDBC_TYPE_DOUBLE,
            "DOUBLE",
            new Integer( 52 ) ,     // PRECISION
            new Integer( 8 ),         // LENGTH
            SCALE_UNDEFINED,       // SCALE
            new Integer( 2 ),    // RADIX
            ALLOWS_NULLS,
            EMPTY_REMARKS,
            UNDEFINED_CHAR_OCTET_LENGTH,
            new Integer( 8 ),           // ORDINAL_POSITION
            IS_NULLABLE,
            GENERIC_NAME,
            ARG_COUNT_2,
            new Integer( 9 )
        },
        {
            NO_CATALOG,
            "APP",
            "RETURNSALLLEGALDATATYPES",
            "COLUMN8",
            FUNCTION_RESULT_COLUMN,
            JDBC_TYPE_DOUBLE,
            "DOUBLE",
            new Integer( 52 ) ,     // PRECISION
            new Integer( 8 ),         // LENGTH
            SCALE_UNDEFINED,       // SCALE
            new Integer( 2 ),    // RADIX
            ALLOWS_NULLS,
            EMPTY_REMARKS,
            UNDEFINED_CHAR_OCTET_LENGTH,
            new Integer( 9 ),           // ORDINAL_POSITION
            IS_NULLABLE,
            GENERIC_NAME,
            ARG_COUNT_2,
            new Integer( 10 )
        },
        {
            NO_CATALOG,
            "APP",
            "RETURNSALLLEGALDATATYPES",
            "COLUMN9",
            FUNCTION_RESULT_COLUMN,
            JDBC_TYPE_REAL,
            "REAL",
            new Integer( 23 ) ,     // PRECISION
            new Integer( 4 ),         // LENGTH
            SCALE_UNDEFINED,       // SCALE
            new Integer( 2 ),    // RADIX
            ALLOWS_NULLS,
            EMPTY_REMARKS,
            UNDEFINED_CHAR_OCTET_LENGTH,
            new Integer( 10 ),           // ORDINAL_POSITION
            IS_NULLABLE,
            GENERIC_NAME,
            ARG_COUNT_2,
            new Integer( 11 )
        },
        {
            NO_CATALOG,
            "APP",
            "RETURNSALLLEGALDATATYPES",
            "COLUMN10",
            FUNCTION_RESULT_COLUMN,
            JDBC_TYPE_DOUBLE,
            "DOUBLE",
            new Integer( 52 ) ,     // PRECISION
            new Integer( 8 ),         // LENGTH
            SCALE_UNDEFINED,       // SCALE
            new Integer( 2 ),    // RADIX
            ALLOWS_NULLS,
            EMPTY_REMARKS,
            UNDEFINED_CHAR_OCTET_LENGTH,
            new Integer( 11 ),           // ORDINAL_POSITION
            IS_NULLABLE,
            GENERIC_NAME,
            ARG_COUNT_2,
            new Integer( 12 )
        },
        {
            NO_CATALOG,
            "APP",
            "RETURNSALLLEGALDATATYPES",
            "COLUMN11",
            FUNCTION_RESULT_COLUMN,
            JDBC_TYPE_INT,
            "INTEGER",
            PRECISION_INTEGER ,     // PRECISION
            LENGTH_INTEGER,         // LENGTH
            SCALE_INTEGER,       // SCALE
            RADIX_INTEGER,    // RADIX
            ALLOWS_NULLS,
            EMPTY_REMARKS,
            UNDEFINED_CHAR_OCTET_LENGTH,
            new Integer( 12 ),           // ORDINAL_POSITION
            IS_NULLABLE,
            GENERIC_NAME,
            ARG_COUNT_2,
            new Integer( 13 )
        },
        {
            NO_CATALOG,
            "APP",
            "RETURNSALLLEGALDATATYPES",
            "COLUMN12",
            FUNCTION_RESULT_COLUMN,
            JDBC_TYPE_LONGVARCHAR,
            "LONG VARCHAR",
            new Integer( 32700 ),     // PRECISION
            new Integer( 65400 ),         // LENGTH
            SCALE_UNDEFINED,       // SCALE
            RADIX_UNDEFINED,    // RADIX
            ALLOWS_NULLS,
            EMPTY_REMARKS,
            UNDEFINED_CHAR_OCTET_LENGTH,
            new Integer( 13 ),           // ORDINAL_POSITION
            IS_NULLABLE,
            GENERIC_NAME,
            ARG_COUNT_2,
            new Integer( 14 )
        },
        {
            NO_CATALOG,
            "APP",
            "RETURNSALLLEGALDATATYPES",
            "COLUMN13",
            FUNCTION_RESULT_COLUMN,
            JDBC_TYPE_LONGVARBINARY,
            "LONG VARCHAR FOR BIT DATA",
            new Integer( 32700 ),     // PRECISION
            new Integer( 32700 ),         // LENGTH
            SCALE_UNDEFINED,       // SCALE
            RADIX_UNDEFINED,    // RADIX
            ALLOWS_NULLS,
            EMPTY_REMARKS,
            UNDEFINED_CHAR_OCTET_LENGTH,
            new Integer( 14 ),           // ORDINAL_POSITION
            IS_NULLABLE,
            GENERIC_NAME,
            ARG_COUNT_2,
            new Integer( 15 )
        },
        {
            NO_CATALOG,
            "APP",
            "RETURNSALLLEGALDATATYPES",
            "COLUMN14",
            FUNCTION_RESULT_COLUMN,
            JDBC_TYPE_NUMERIC,
            "NUMERIC",
            new Integer( 5 ),     // PRECISION
            new Integer( 14 ),         // LENGTH
            new Integer( 0 ),       // SCALE
            new Integer( 10 ),    // RADIX
            ALLOWS_NULLS,
            EMPTY_REMARKS,
            UNDEFINED_CHAR_OCTET_LENGTH,
            new Integer( 15 ),           // ORDINAL_POSITION
            IS_NULLABLE,
            GENERIC_NAME,
            ARG_COUNT_2,
            new Integer( 16 )
        },
        {
            NO_CATALOG,
            "APP",
            "RETURNSALLLEGALDATATYPES",
            "COLUMN15",
            FUNCTION_RESULT_COLUMN,
            JDBC_TYPE_REAL,
            "REAL",
            new Integer( 23 ),     // PRECISION
            new Integer( 4 ),         // LENGTH
            SCALE_UNDEFINED,       // SCALE
            new Integer( 2 ),    // RADIX
            ALLOWS_NULLS,
            EMPTY_REMARKS,
            UNDEFINED_CHAR_OCTET_LENGTH,
            new Integer( 16 ),           // ORDINAL_POSITION
            IS_NULLABLE,
            GENERIC_NAME,
            ARG_COUNT_2,
            new Integer( 17 )
        },
        {
            NO_CATALOG,
            "APP",
            "RETURNSALLLEGALDATATYPES",
            "COLUMN16",
            FUNCTION_RESULT_COLUMN,
            JDBC_TYPE_SMALLINT,
            "SMALLINT",
            new Integer( 5 ),     // PRECISION
            new Integer( 2 ),         // LENGTH
            SCALE_INTEGER,       // SCALE
            RADIX_INTEGER,    // RADIX
            ALLOWS_NULLS,
            EMPTY_REMARKS,
            UNDEFINED_CHAR_OCTET_LENGTH,
            new Integer( 17 ),           // ORDINAL_POSITION
            IS_NULLABLE,
            GENERIC_NAME,
            ARG_COUNT_2,
            new Integer( 18 )
        },
        {
            NO_CATALOG,
            "APP",
            "RETURNSALLLEGALDATATYPES",
            "COLUMN17",
            FUNCTION_RESULT_COLUMN,
            JDBC_TYPE_TIME,
            "TIME",
            new Integer( 8 ),     // PRECISION
            new Integer( 6 ),         // LENGTH
            SCALE_INTEGER,       // SCALE
            RADIX_INTEGER,    // RADIX
            ALLOWS_NULLS,
            EMPTY_REMARKS,
            UNDEFINED_CHAR_OCTET_LENGTH,
            new Integer( 18 ),           // ORDINAL_POSITION
            IS_NULLABLE,
            GENERIC_NAME,
            ARG_COUNT_2,
            new Integer( 19 )
        },
        {
            NO_CATALOG,
            "APP",
            "RETURNSALLLEGALDATATYPES",
            "COLUMN18",
            FUNCTION_RESULT_COLUMN,
            JDBC_TYPE_TIMESTAMP,
            "TIMESTAMP",
            new Integer( 29 ),     // PRECISION
            new Integer( 16 ),         // LENGTH
            new Integer( 9 ),       // SCALE
            RADIX_INTEGER,    // RADIX
            ALLOWS_NULLS,
            EMPTY_REMARKS,
            UNDEFINED_CHAR_OCTET_LENGTH,
            new Integer( 19 ),           // ORDINAL_POSITION
            IS_NULLABLE,
            GENERIC_NAME,
            ARG_COUNT_2,
            new Integer( 20 )
        },
        {
            NO_CATALOG,
            "APP",
            "RETURNSALLLEGALDATATYPES",
            "COLUMN19",
            FUNCTION_RESULT_COLUMN,
            JDBC_TYPE_VARCHAR,
            "VARCHAR",
            new Integer( 10 ),     // PRECISION
            new Integer( 20 ),         // LENGTH
            SCALE_UNDEFINED,       // SCALE
            RADIX_UNDEFINED,    // RADIX
            ALLOWS_NULLS,
            EMPTY_REMARKS,
            new Integer( 20 ),    // CHAR_OCTET_LENGTH
            new Integer( 20 ),           // ORDINAL_POSITION
            IS_NULLABLE,
            GENERIC_NAME,
            ARG_COUNT_2,
            new Integer( 21 )
        },
        {
            NO_CATALOG,
            "APP",
            "RETURNSALLLEGALDATATYPES",
            "COLUMN20",
            FUNCTION_RESULT_COLUMN,
            JDBC_TYPE_VARBINARY,
            "VARCHAR () FOR BIT DATA",
            new Integer( 10 ),     // PRECISION
            new Integer( 10 ),         // LENGTH
            SCALE_UNDEFINED,       // SCALE
            RADIX_UNDEFINED,    // RADIX
            ALLOWS_NULLS,
            EMPTY_REMARKS,
            new Integer( 10 ),    // CHAR_OCTET_LENGTH
            new Integer( 21 ),           // ORDINAL_POSITION
            IS_NULLABLE,
            GENERIC_NAME,
            ARG_COUNT_2,
            new Integer( 22 )
        },
        {
            NO_CATALOG,
            "APP",
            "RETURNSALLLEGALDATATYPES",
            "COLUMN21",
            FUNCTION_RESULT_COLUMN,
            JDBC_TYPE_BOOLEAN,
            "BOOLEAN",
            new Integer( 1 ),     // PRECISION
            new Integer( 1 ),         // LENGTH
            SCALE_UNDEFINED,       // SCALE
            RADIX_UNDEFINED,    // RADIX
            ALLOWS_NULLS,
            EMPTY_REMARKS,
            UNDEFINED_CHAR_OCTET_LENGTH,    // CHAR_OCTET_LENGTH
            new Integer( 22 ),           // ORDINAL_POSITION
            IS_NULLABLE,
            GENERIC_NAME,
            ARG_COUNT_2,
            new Integer( 23 )
        },
    };

    private static  final   String  ESTIMATED_ROW_COUNT = "optimizer estimated row count:";
    private static  final   String  ESTIMATED_COST = "optimizer estimated cost:";
    
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

    private boolean             _usingLocaleSpecificCollation;
    private DatabaseMetaData    _databaseMetaData;

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

        suite.addTest( new TableFunctionTest( "noSpecialCollation" ) );
        suite.addTest( collatedSuite( "en", "specialCollation" ) );

        return suite;
    }
    
    /**
     * Return a suite that uses a single use database with
     * a primary fixture from this test plus potentially other
     * fixtures.
     * @param locale Locale to use for the database
     * @param baseFixture Base fixture from this test.
     * @return suite of tests to run for the given locale
     */
    private static Test collatedSuite(String locale, String baseFixture)
    {
        TestSuite suite = new TestSuite( "TableFunctionTest:territory=" + locale );
        suite.addTest( new TableFunctionTest( baseFixture ) );

        return Decorator.territoryCollatedDatabase( suite, locale );
    }

    protected void    setUp()
        throws Exception
    {
        super.setUp();

        _databaseMetaData = getConnection().getMetaData();

        dropSchema();
    }

    protected void    tearDown()
        throws Exception
    {
        dropSchema();

        _databaseMetaData = null;

        super.tearDown();
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // JUnit TESTS
    //
    ///////////////////////////////////////////////////////////////////////////////////
    
    /**
     * Verify table functions in a vanilla database without locale-specific collations.
     */
    public void noSpecialCollation()
        throws Exception
    {
        _usingLocaleSpecificCollation = false;
        tableFunctionTest();
    }
    
    /**
     * Verify table functions in a database with a special collation.
     */
    public void specialCollation()
        throws Exception
    {
        _usingLocaleSpecificCollation = true;
        tableFunctionTest();
    }
    
    /**
     * Verify table functions.
     */
    public void tableFunctionTest()
        throws Exception
    {
        badDDL();
        simpleDDL();

        notTableFunction();
        simpleVTIResults();
        allLegalDatatypesVTIResults();
        vtiCosting();
        
        collationTest();
        subqueryTest();

        coercionTest();

        bulkInsert();
        
        miscBugs();
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
        goodStatement
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
             "RETURNS " + SFT_RETURN_TYPE +
             " LANGUAGE JAVA PARAMETER STYLE DERBY_JDBC_RESULT_SET NO SQL CALLED ON NULL INPUT"
             );

        assertFunctionDBMD( "SIMPLEFUNCTIONTABLE", GF_SFT , GFC_SFT );
    }
    
    /**
     * Verify that you can't invoke an ordinary function as a VTI.
     */
    private void notTableFunction()
        throws Exception
    {
        goodStatement
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
        goodStatement
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
        
        goodStatement
        (
         "create function returnsACoupleRowsAsCHAR()\n" +
         "returns TABLE\n" +
         "  (\n" +
         "     column0 char( 10 ),\n" +
         "     column1 char( 10 )\n" +
         "  )\n" +
         "language java\n" +
         "parameter style DERBY_JDBC_RESULT_SET\n" +
         "no sql\n" +
         "external name '" + getClass().getName() + ".returnsACoupleRows'\n"
         );   
        
        String[][] CHAR_ROWS = new String[SIMPLE_ROWS.length][];
        for (int r = 0; r < CHAR_ROWS.length; r++)
        {
        	CHAR_ROWS[r] = new String[SIMPLE_ROWS[r].length];
        	for (int c = 0; c < CHAR_ROWS[r].length; c++)
        	{
        		String cv = SIMPLE_ROWS[r][c];
        		if (cv != null)
        		{
        			if (cv.length() < 10)
        			{
        				StringBuffer sb = new StringBuffer(cv);
        				for (int p = cv.length(); p < 10; p++)
        					sb.append(' ');
        				CHAR_ROWS[r][c] = sb.toString();
        			}	
        		}
        	}
        }
        assertResults
        (
         "select s.*\n" +
         "    from TABLE( returnsACoupleRowsAsCHAR() ) s\n",
         CHAR_ROWS,
         new int[] { Types.CHAR, Types.CHAR }
         );

        // boolean valued columns
        goodStatement
            (
             "create function returnsBooleans()\n" +
             "returns TABLE\n" +
             "  (\n" +
             "     column0 varchar( 10 ),\n" +
             "     column1 boolean\n" +
             "  )\n" +
             "language java\n" +
             "parameter style DERBY_JDBC_RESULT_SET\n" +
             "no sql\n" +
             "external name '" + getClass().getName() + ".returnsBooleans'\n"
             );
        assertResults
        (
         "select s.*\n" +
         "    from TABLE( returnsBooleans() ) s\n",
         BOOLEAN_ROWS,
         new int[] { Types.VARCHAR, Types.BOOLEAN }
         );


    }
    
    /**
     * Verify bulk insert using a VTI
     */
    private void  bulkInsert()
        throws Exception
    {
        Connection conn = getConnection();
        
        goodStatement
            (
             "create table bulkInsertTable\n" +
             "  (\n" +
             "     column0 int,\n" +
             "     column1 varchar( 10 )\n" +
             "  )\n"
             );
        goodStatement
            (
             "create table biSourceTable\n" +
             "  (\n" +
             "     column0 int,\n" +
             "     column1 varchar( 10 )\n" +
             "  )\n"
             );
        goodStatement
            (
             "create function bulkInsertVTI()\n" +
             "returns TABLE\n" +
             "  (\n" +
             "     column0 int,\n" +
             "     column1 varchar( 10 )\n" +
             "  )\n" +
             "language java\n" +
             "parameter style DERBY_JDBC_RESULT_SET\n" +
             "no sql\n" +
             "external name '" + getClass().getName() + ".bulkInsertVTI'\n"
             );
        goodStatement
            (
             "create view bulkInsertView( column0, column1 ) as select column0, column1\n" +
             "from table( bulkInsertVTI() ) s\n"
             );
        goodStatement
            (
             "insert into biSourceTable select * from bulkInsertView\n"
             );
        //Test table with null value on bulk insert
        goodStatement
        (
         "create table bulkInsertSimpleTable\n" +
         "  (\n" +
         "     column0 varchar( 10 ),\n" +
         "     column1 varchar( 10 )\n" +
         "  )\n"
         );

        //
        // Inserting from a table function into an empty table should trigger
        // the bulk-insert optimization, resulting in a new conglomerate for
        // the target table
        //
        // Inserting from a table function into a non-empty table should NOT trigger
        // the bulk-insert optimization. The conglomerate number of the target table
        // should not change.
        //

        vetBulkInsert
            (
             conn,
             "insert into bulkInsertTable select * from table( bulkInsertVTI() ) s",
             true
             );

        // You still get bulk-insert if you wrap the table function in a view
        vetBulkInsert
            (
             conn,
             "insert into bulkInsertTable select * from bulkInsertView",
             true
             );
        // You still get bulk-insert if it is a union that wrap a table
        // function
        vetBulkInsert
            (
             conn,
             "insert into bulkInsertTable select * from table( bulkInsertVTI() ) s union select * from table (bulkInsertVTI()) t",
             true
             );
        // You still get bulk-insert if it is a table function wrap subquery
        vetBulkInsert
            (
             conn,
             "insert into bulkInsertTable select * from table( bulkInsertVTI()) b where b.column0 in (select c.column0 from table( bulkInsertVTI()) c)",
             true
             );
        // You still get bulk-insert if it is a self join that wrap a table
        // function in a view
        goodStatement("delete from bulkInsertTable");
        vetBulkInsert
            (
             conn,
             "insert into bulkInsertTable select * from bulkInsertView b where 1 = (select count(*) from bulkInsertView bc where b.column0 > bc.column0)",
             true,
             BULK_INSERT_SELF_JOIN_ROWS
             );
        // You don't get bulk-insert if you insert from an ordinary table
        vetBulkInsert
            (
             conn,
             "insert into bulkInsertTable select * from biSourceTable",
             false
             );
        vetBulkInsert
        (
         conn,
         "insert into bulkInsertTable select * from table( bulkInsertVTI() ) s",
         true
         );
        // You still get bulk-insert if you wrap the table function in a view
        vetBulkInsertSimple
        (
         conn,
         "insert into bulkInsertSimpleTable select * from table(RETURNSACOUPLEROWSASCHAR()) r",
         true
         );
         // You still get bulk-insert if it is a union that wrap a table
         // function
        vetBulkInsertSimple
        (
         conn,
         "insert into bulkInsertSimpleTable select * from table( RETURNSACOUPLEROWSASCHAR() ) s union select * from table ( RETURNSACOUPLEROWSASCHAR() ) t",
         true
         );
         // You still get bulk-insert if it is a table function wrap subquery
        vetBulkInsertSimple
        (
         conn,
         "insert into bulkInsertSimpleTable select c.column0, c.column1 from table( RETURNSACOUPLEROWSASCHAR() ) c left outer join (select * from table( RETURNSACOUPLEROWSASCHAR() ) d) e on c.column0 = e.column0 and c.column1 = e.column1",
         true
         );
    }
    private void vetBulkInsert( Connection conn, String insert, boolean bulkInsertExpected )
        throws Exception
    {
        goodStatement( "delete from bulkInsertTable" );

        vetBulkInsert( conn, insert, bulkInsertExpected, BULK_INSERT_ROWS );

        //
        // Inserting from a table function into a non-empty table should NOT trigger
        // the bulk-insert optimization. The conglomerate number of the target table
        // should not change.
        //
        vetBulkInsert( conn, insert, false, DOUBLY_INSERTED_ROWS );
    }
    private void vetBulkInsert( Connection conn, String insert, boolean bulkInsertExpected, String[][] expectedRows )
        throws Exception
    {
        long originalConglomerateID = getConglomerateID( conn, "BULKINSERTTABLE" );
        goodStatement( insert );
        long conglomerateIDAfterInsert = getConglomerateID( conn, "BULKINSERTTABLE" );
        
        assertEquals( bulkInsertExpected, originalConglomerateID != conglomerateIDAfterInsert );
        assertResults
            (
             "select * from bulkInsertTable order by column0",
             expectedRows,
             new int[] { Types.INTEGER, Types.VARCHAR }
             );
    }
    private void vetBulkInsertSimple( Connection conn, String insert, boolean bulkInsertSimpleExpected )
    throws Exception
    {
    goodStatement( "delete from bulkInsertSimpleTable" );

    vetBulkInsertSimple( conn, insert, bulkInsertSimpleExpected, SIMPLY_ROWS );

    //
    // Inserting from a table function into a non-empty table should NOT triOgger
    // the bulk-insert optimization. The conglomerate number of the target table
    // should not change.
    //
    vetBulkInsertSimple( conn, insert, false, DOUBLY_SIMPLE_ROWS );
    }
    private void vetBulkInsertSimple( Connection conn, String insert, boolean bulkInsertSimpleExpected, String[][] expectedRows )
    throws Exception
    {
    long originalConglomerateID = getConglomerateID( conn, "BULKINSERTSIMPLETABLE" );
    goodStatement( insert );
    long conglomerateIDAfterInsert = getConglomerateID( conn, "BULKINSERTSIMPLETABLE" );
    
    assertEquals( bulkInsertSimpleExpected, originalConglomerateID != conglomerateIDAfterInsert );
    assertResults
        (
         "select * from bulkInsertSimpleTable order by column0",
         expectedRows,
         new int[] { Types.VARCHAR, Types.VARCHAR }
         );
     }
    /**
     * Verify that Derby handles VTI columns of all known datatypes.
     */
    private void  allLegalDatatypesVTIResults()
        throws Exception
    {
        goodStatement
            (
             "create function getXXXrecord()\n" +
             "returns varchar( 1000 )\n" +
             "language java\n" +
             "parameter style java\n" +
             "no sql\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.StringArrayVTI.getXXXrecord'\n"
             );

        goodStatement
            (
             "create function returnsAllLegalDatatypes( intArgument int, varcharArgument varchar( 10 ) )\n" +
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
             "column20 VARCHAR( 10 ) FOR BIT DATA,\n" +
             "column21 BOOLEAN\n" +
             "  )\n" +
             "language java\n" +
             "parameter style DERBY_JDBC_RESULT_SET\n" +
             "no sql\n" +
             "external name '" + getClass().getName() + ".returnsAllLegalDatatypes'\n"
             );

        assertResults
            (
             "select s.*\n" +
             "    from TABLE( returnsAllLegalDatatypes( 1, 'one' ) ) s\n",
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
                    Types.BOOLEAN,
                }
             );
        
        assertFunctionDBMD( "RETURNSALLLEGALDATATYPES", GF_RADT , GFC_RADT );

        checkGetXXXCalls();
    }
    
    /**
     * Verify that the correct getXXX() methods are called by Derby. If Derby
     * changes so that different getXXX() methods are called for these
     * datatypes, then the user documentation will have to be adjusted. These
     * are the methods which we tell users they must implement.
     */
    private void  checkGetXXXCalls()
        throws Exception
    {
        int             datatypeCount = ALL_TYPES_ROWS[ 0 ].length;
        StringBuffer    buffer = new StringBuffer();

        buffer.append( "select s.*\n" );
        buffer.append( "    from TABLE( returnsAllLegalDatatypes( 1, 'one' ) ) s\n" );
        buffer.append( "    where\n" );
        for ( int i = 0; i < datatypeCount; i++ )
        {
            String  rc = "s.column" + i;
            if ( i > 0 ) { buffer.append( "   and " ); }
            buffer.append( "( " + rc + " is null )\n" );
        }

        assertResults
            (
             buffer.toString(),
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
                    Types.BOOLEAN,
                }
             );

        PreparedStatement   ps = prepareStatement( "values getXXXrecord()" );
        ResultSet           rs = ps.executeQuery();

        rs.next();

        String  actualGetXXXCalls = rs.getString( 1 );

        rs.close();
        ps.close();
        
        println( actualGetXXXCalls );

        String  expectedGetXXXCalls;
        if ( JDBC.vmSupportsJSR169() )
        { expectedGetXXXCalls = EXPECTED_GET_XXX_CALLS_JSR169; }
        else { expectedGetXXXCalls = EXPECTED_GET_XXX_CALLS; }
        assertEquals( expectedGetXXXCalls, actualGetXXXCalls );
    }
    
    /**
     * Verify the VTICosting optimizer api.
     */
    private void vtiCosting()
        throws Exception
    {
        //
        // Doesn't have a public no-arg constructor.
        //
        goodStatement
            (
             "create function missingConstructor()\n" +
             "returns TABLE\n" +
             "  (\n" +
             "     varcharCol varchar( 10 )\n" +
             "  )\n" +
             "language java\n" +
             "parameter style DERBY_JDBC_RESULT_SET\n" +
             "no sql\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.StringArrayVTI$MissingConstructor.dummyVTI'\n"
             );
        expectError
            (
             "42ZB5",
             "select s.*\n" +
             "    from TABLE( missingConstructor() ) s\n"
             );

        //
        // Has a no-arg constructor but it isn't public.
        //
        goodStatement
            (
             "create function zeroArgConstructorNotPublic()\n" +
             "returns TABLE\n" +
             "  (\n" +
             "     varcharCol varchar( 10 )\n" +
             "  )\n" +
             "language java\n" +
             "parameter style DERBY_JDBC_RESULT_SET\n" +
             "no sql\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.StringArrayVTI$ZeroArgConstructorNotPublic.dummyVTI'\n"
             );
        expectError
            (
             "42ZB5",
             "select s.*\n" +
             "    from TABLE( missingConstructor() ) s\n"
             );

        //
        // Has a public, no-arg constructor but it raises an exception.
        //
        goodStatement
            (
             "create function constructorException()\n" +
             "returns TABLE\n" +
             "  (\n" +
             "     varcharCol varchar( 10 )\n" +
             "  )\n" +
             "language java\n" +
             "parameter style DERBY_JDBC_RESULT_SET\n" +
             "no sql\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.StringArrayVTI$ConstructorException.dummyVTI'\n"
             );
        expectError
            (
             "38000",
             "select s.*\n" +
             "    from TABLE( constructorException() ) s\n"
             );

        //
        // Good implementation of VTICosting. Verify that the optimizer costs
        // are overridden.
        //
        goodStatement
            (
             "create function goodVTICosting()\n" +
             "returns TABLE\n" +
             "  (\n" +
             "     varcharCol varchar( 10 )\n" +
             "  )\n" +
             "language java\n" +
             "parameter style DERBY_JDBC_RESULT_SET\n" +
             "no sql\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.StringArrayVTI$GoodVTICosting.dummyVTI'\n"
             );
        String      optimizerStats = getOptimizerStats
            (
             "select s.*\n" +
             "    from TABLE( goodVTICosting() ) s\n"
             );
        assertEquals( StringArrayVTI.FAKE_ROW_COUNT, readDoubleTag( optimizerStats, ESTIMATED_ROW_COUNT ), 0.0 );
        assertEquals( StringArrayVTI.FAKE_INSTANTIATION_COST, readDoubleTag( optimizerStats, ESTIMATED_COST ), 0.0 );
    }
    
    /**
     * Verify that Derby uses the same collation logic on columns in real Tables
     * and in Table Functions.
     */
    private void  collationTest()
        throws Exception
    {
        assertEquals( STRING_TYPES.length, ALL_STRING_TYPES_ROWS[ 0 ].length );
        
        StringBuffer    rowSet = new StringBuffer();
        int             stringTypeCount = STRING_TYPES.length;

        rowSet.append( "(\n" );
        for ( int i = 0; i < stringTypeCount; i++ )
        {
            rowSet.append( '\t' );
            if ( i > 0 ) { rowSet.append( ", " ); }
            rowSet.append( "column" + i + " " + STRING_TYPES[ i ] + "\n" );
        }
        rowSet.append( ")\n" );
        
        goodStatement
            (
             "create table allStringTypesTable\n" +
             rowSet.toString()
             );

        goodStatement
            (
             "create function allStringTypesFunction()\n" +
             "returns TABLE\n" +
             rowSet.toString() +
             "language java\n" +
             "parameter style DERBY_JDBC_RESULT_SET\n" +
             "no sql\n" +
             "external name '" + getClass().getName() + ".allStringTypesFunction'\n"
             );

        // populate table
        StringBuffer    insertSql = new StringBuffer();
        insertSql.append( "insert into allStringTypesTable values\n" );
        insertSql.append( "(\n" );
        for ( int i = 0; i < stringTypeCount; i++ )
        {
            if ( i > 0 ) { insertSql.append( ", " ); }
            insertSql.append( "?" );
        }
        insertSql.append( ")\n" );

        PreparedStatement   ps = chattyPrepare( insertSql.toString() );
        int                 rowCount = ALL_STRING_TYPES_ROWS.length;
        for ( int i = 0; i < rowCount; i++ )
        {
            for ( int j = 0; j < stringTypeCount; j++ )
            {
                ps.setString( j + 1, ALL_STRING_TYPES_ROWS[ i ][ j ] );
            }
            ps.execute();
        }
        ps.close();

        // now verify that the string columns in the table are comparable to the
        // string columns returned by the function. they would not be comparable
        // if they had different collations.
        StringBuffer    compareRows = new StringBuffer();
        compareRows.append
            (
             "select f.*\n" +
             "    from TABLE( allStringTypesFunction() ) f,\n" +
             "    allStringTypesTable t\n" +
             "where\n" );
        for ( int i = 0; i < stringTypeCount; i++ )
        {
            String  fcol = "f.column" + i;
            String  tcol = "t.column" + i;

            if ( i > 0 ) { compareRows.append( " and " ); }
            compareRows.append( fcol + " = " + tcol );
        }
        
        assertResults
            (
             compareRows.toString(),
             ALL_STRING_TYPES_ROWS,
             STRING_JDBC_TYPES
             );

        // now verify that with default collation, we can compare the function
        // columns to system identifiers. however, with locale-specific
        // collations, these comparisons should fail.
        compareRows = new StringBuffer();
        compareRows.append
            (
             "select f.*\n" +
             "    from TABLE( allStringTypesFunction() ) f,\n" +
             "    sys.systables t\n" +
             "where\n" );
        for ( int i = 0; i < stringTypeCount; i++ )
        {
            String  fcol = "f.column" + i;
            String  tcol = "t.tablename";

            if ( i > 0 ) { compareRows.append( " and " ); }
            compareRows.append( fcol + " = " + tcol );
        }

        if ( _usingLocaleSpecificCollation )
        {
            expectError
                (
                 "42818",
                 compareRows.toString()
                 );

        }
        else
        {
            assertResults
                (
                 compareRows.toString(),
                 new  String[][] {},
                 STRING_JDBC_TYPES
                 );
        }
    }
    
    /**
     * Verify that table functions work correctly when invoked in
     * a subuery with correlated references to outer query blocks.
     */
    private void  subqueryTest()
        throws Exception
    {
        goodStatement
            (
             "create table fooTestTable\n" +
             "(\n" +
             "    inputCol    varchar( 20 ),\n" +
             "    outputCol   varchar( 30 )\n" +
             ")\n"
             );
        goodStatement
            (
             "insert into fooTestTable\n" +
             "values\n" +
             "( 'succeed1', 'succeed1 foo' ),\n" +
             "( 'fail1', 'ladeedah' ),\n" +
             "( 'succeed2', 'succeed2 bar' ),\n" +
             "( 'fail2', 'hoopla' )\n"
             );
        goodStatement
            (
             "create function appendFooAndBar( inputArg varchar( 20 ) )\n" +
             "returns TABLE\n" +
             "  (\n" +
             "     inputText varchar( 20 ),\n" +
             "     outputText varchar( 30 )\n" +
             "  )\n" +
             "language java\n" +
             "parameter style DERBY_JDBC_RESULT_SET\n" +
             "no sql\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.TableFunctionTest.appendFooAndBar'\n"
             );

        assertResults
            (
             "select * from fooTestTable\n" +
             "where outputCol in\n" +
             "(\n" +
             "    select f.outputText\n" +
             "    from TABLE( appendFooAndBar( inputCol ) ) as f\n" +
             ")\n",
             new String[] { "INPUTCOL", "OUTPUTCOL" },
             new String[][]
             {
                 { "succeed1", "succeed1 foo" },
                 { "succeed2", "succeed2 bar" },
             },
             new int[] { Types.VARCHAR, Types.VARCHAR }
             );
    }
    
    /**
     * Verify that variable length data values are coerced to their
     * declared types, regardless of what actually is returned by the
     * user-coded ResultSet. See DERBY-3341.
     */
    private void  coercionTest()
        throws Exception
    {
        goodStatement
            (
             "create function coercionFunction( )\n" +
             "returns TABLE\n" +
             "  (\n" +
             "     keyCol int,\n" +
             "     charCol char( 5 ),\n" +
             "     varcharCol varchar( 5 ),\n" +
             "     charForBitDataCol char( 5 ) for bit data,\n" +
             "     varcharForBitDataCol varchar( 5 ) for bit data,\n" +
             "     decimalCol decimal( 5, 2 ),\n" +
             "     longvarcharCol long varchar,\n" +
             "     longvarcharForBitDataCol long varchar for bit data\n" +
             "  )\n" +
             "language java\n" +
             "parameter style DERBY_JDBC_RESULT_SET\n" +
             "no sql\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.TableFunctionTest.coercionFunction'\n"
             );
        
        assertResults
            (
             "select *\n" +
             "from TABLE( coercionFunction( ) ) as f order by keyCol\n",
             new String[]
             {
                 "KEYCOL",
                 "CHARCOL",
                 "VARCHARCOL",
                 "CHARFORBITDATACOL",
                 "VARCHARFORBITDATACOL",
                 "DECIMALCOL",
                 "LONGVARCHARCOL",
                 "LONGVARCHARFORBITDATACOL",
             },
             makeCoercionOutputs(),
             new int[]
             {
                 Types.INTEGER,
                 Types.CHAR,
                 Types.VARCHAR,
                 Types.BINARY,
                 Types.VARBINARY,
                 Types.DECIMAL,
                 Types.LONGVARCHAR,
                 Types.LONGVARBINARY,
             }
             );
    }
    
    /**
     * <p>
     * Miscellaneous bugs.
     * </p>
     */
    private void  miscBugs()
        throws Exception
    {
        derby_4092();
    }
    
    /**
     * <p>
     * Don't allow table functions to appear where scalar functions are expected.
     * </p>
     */
    private void  derby_4092()
        throws Exception
    {
        goodStatement
            (
             "create function derby_4092()\n" +
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

        expectError
            (
             "42ZB6",
             "values( derby_4092() )"
             );
        expectError
            (
             "42ZB6",
             "select derby_4092(), tablename from sys.systables"
             );
    }
    
    /**
     * <p>
     * Make the input rows for the coercion function.
     * </p>
     */
    private static  String[][]  makeCoercionInputs()
    {
        return new String[][]
        {
            { "1", "abc", "abc", "abc", "abc", "12.3", makeString( 5 ), makeByteString( 5 )  },    // too short
            { "2", "abcdef", "abcdef", "abcdef", "abcdef", "12.345", makeString( 32700 + 1 ), makeByteString( 32700 + 1 ) },   // too long
            { "3", "abcde", "abcde", "abcde", "abcde", "123.45", makeString( 5 ), makeByteString( 5 ) },  //  just right
        };
    }

    /**
     * <p>
     * Make the expected output rows which should come back from the coercion function.
     * </p>
     */
    private static  String[][]  makeCoercionOutputs()
    {
        return new String[][]
        {
            { "1", "abc  ", "abc", "abc  ", "abc", "12.30", makeString( 5 ), makeByteString( 5 ) },
            { "2", "abcde", "abcde", "abcde", "abcde", "12.34", makeString( 32700 ), makeByteString( 32700 ) },
            { "3", "abcde", "abcde", "abcde", "abcde", "123.45", makeString( 5 ), makeByteString( 5 ) },
        };
    }

    /**
     * <p>
     * Return a String of the specified length.
     * </p>
     */
    private static  String  makeString( int count )
    {
        char[]  raw = new char[ count ];

        Arrays.fill( raw, 'a' );

        return new String( raw );
    }
    
    /**
     * <p>
     * Return a String encoding a byte array of the specified length.
     * </p>
     */
    private static  String  makeByteString( int count )
    {
        try {
            byte[]  raw = new byte[ count ];
            byte    value = (byte) 1;

            Arrays.fill( raw, value );

            return new String( raw, UTF8 );
        }
        catch (Throwable t)
        {
            println( t.getMessage() );
            return null;
        }
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
     * A VTI which returns some boolean values
     */
    public  static  ResultSet returnsBooleans()
    {
        return makeVTI( BOOLEAN_ROWS );
    }

    /**
     * A VTI which returns a couple rows.
     */
    public  static  ResultSet returnsACoupleRows()
    {
        return makeVTI( SIMPLE_ROWS );
    }

    /**
     * A VTI for use in bulk insert
     */
    public  static  ResultSet bulkInsertVTI()
    {
        return makeVTI( BULK_INSERT_ROWS );
    }

    /**
     * A VTI which returns rows having columns of all legal datatypes.
     */
    public  static  ResultSet returnsAllLegalDatatypes( int intArg, String varcharArg )
    {
        return makeVTI( ALL_TYPES_ROWS );
    }

    /**
     * A VTI which returns rows having columns of all string datatypes.
     */
    public  static  ResultSet allStringTypesFunction()
    {
        return makeVTI( ALL_STRING_TYPES_ROWS );
    }

    /**
     * A VTI which returns rows based on a passed-in parameter
     */
    public  static  ResultSet appendFooAndBar( String text )
    {
        String[][]  kernel = new String[][]
        {
            { text, text + " foo" },
            { text, text + " bar" },
        };

        return makeVTI( kernel );
    }

    /**
     * A VTI which returns variable-length data typed columns.
     */
    public  static  ResultSet coercionFunction()
    {
        return makeVTI( makeCoercionInputs() );
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
        String[]    columnNames = makeColumnNames( expectedJdbcTypes.length, "COLUMN" );

        assertResults( sql, columnNames, rows, expectedJdbcTypes );
    }

    /**
     * Assert that the ResultSet returns the desired rows.
     */
    public void assertResults( String sql, String[] columnNames, String[][] rows, int[] expectedJdbcTypes )
        throws Exception
    {
        println( "\nExpecting good results from " + sql );

            PreparedStatement    ps = prepareStatement( sql );
            ResultSet                   rs = ps.executeQuery();

            assertResults( expectedJdbcTypes, columnNames, rs, rows );
            
            rs.close();
            ps.close();

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
     * @throws SQLException 
     */
    private void    goodStatement( String ddl ) throws SQLException
    {
            PreparedStatement    ps = chattyPrepare( ddl );

            ps.execute();
            ps.close();

    }
    
    /**
     * Prepare a statement and report its sql text.
     */
    private PreparedStatement   chattyPrepare( String text )
        throws SQLException
    {
        println( "Preparing statement:\n\t" + text );
        
        return prepareStatement( text );
    }
    
    /**
     * Verify that the return type of function looks good.
     * @throws SQLException 
     */
    private void    verifyReturnType( String functionName, String expectedReturnType ) throws SQLException
    {
        println( functionName + " should have return type = " + expectedReturnType );
        
        String ddl = "select aliasinfo from sys.sysaliases where alias=?";
        PreparedStatement ps = prepareStatement(ddl);
        ps.setString(1, functionName);

        JDBC.assertSingleValueResultSet(ps.executeQuery(), expectedReturnType);
    }

    /**
     * Assert that the function has the correct database metadata.
     */
    public void assertFunctionDBMD
        ( String functionName, Object[][] expectedGetFunctionsResult, Object[][] expectedGetFunctionColumnsResult )
        throws Exception
    {
        println( "\nExpecting correct function metadata from " + functionName );
        ResultSet                   rs = getFunctions(  null, "APP", functionName );
        JDBC.assertFullResultSet( rs, expectedGetFunctionsResult, false );
        rs.close();
        
        println( "\nExpecting correct function column metadata from " + functionName );
        rs = getFunctionColumns(  null, "APP", functionName, "%" );
        //prettyPrint( getConnection(), getFunctionColumns(  null, "APP", functionName, "%" ) );
        JDBC.assertFullResultSet( rs, expectedGetFunctionColumnsResult, false );
        rs.close();
    }

    /**
     * Call DatabaseMetaData.getFunctions(). We do this by reflection because
     * the calls exist in our JDBC3.0 implementations even though they don't
     * appear in the JDBC 3.0 java.sql.DatabaseMetaData api.
     */
    public ResultSet    getFunctions( String catalog, String schemaPattern, String functionNamePattern )
        throws Exception
    {
        Class       metadataClass = _databaseMetaData.getClass();
        Method  method = metadataClass.getMethod( "getFunctions", new Class[] { String.class, String.class, String.class } );
        ResultSet   result = (ResultSet) method.invoke( _databaseMetaData, new Object[] { catalog, schemaPattern, functionNamePattern } );

        return result;
    }

    /**
     * Call DatabaseMetaData.getFunctionColumnss(). We do this by reflection because
     * the calls exist in our JDBC3.0 implementations even though they don't
     * appear in the JDBC 3.0 java.sql.DatabaseMetaData api.
     */
    public ResultSet    getFunctionColumns
        ( String catalog, String schemaPattern, String functionNamePattern, String columnNamePattern )
        throws Exception
    {
        Class       metadataClass = _databaseMetaData.getClass();
        Method  method = metadataClass.getMethod( "getFunctionColumns", new Class[] { String.class, String.class, String.class, String.class } );
        ResultSet   result = (ResultSet) method.invoke
            ( _databaseMetaData, new Object[] { catalog, schemaPattern, functionNamePattern, columnNamePattern } );

        return result;
    }

    /**
     * Drop the schema that we are going to use so that we can recreate it.
     */
    private void    dropSchema()
        throws Exception
    {
        int functionCount = FUNCTION_NAMES.length;
        for ( int i = 0; i < functionCount; i++ ) { dropFunction( FUNCTION_NAMES[ i ] ); }

        int tableCount = TABLE_NAMES.length;
        for ( int i = 0; i < tableCount; i++ ) { dropTable( TABLE_NAMES[ i ] ); }
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
                    // with JSR169, we cannot execute resultSet.getBigDecimal...
                    if (JDBC.vmSupportsJDBC3()) {
                        actualValue = squeezeString(  rs.getBigDecimal( column ) );
                        actualValueByName = squeezeString(  rs.getBigDecimal( columnName ) );
                        break;
                    }
                    else {
                        actualValue = squeezeString(  rs.getString( column ) );
                        actualValueByName = squeezeString(  rs.getString( columnName ) );
                        break;
                    }
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
                    Blob blob = rs.getBlob(column);
                    actualValue = squeezeString(blob);
                    actualValueByName = squeezeString(blob);
                    break;
                case Types.CLOB:
                    Clob clob = rs.getClob(column);
                    actualValue = squeezeString(clob);
                    actualValueByName = squeezeString(clob);
                    break;

                case Types.BINARY:
                case Types.VARBINARY:
                case Types.LONGVARBINARY:
                    byte[]  bytes = rs.getBytes( column );

                    actualValue = squeezeString(  bytes );
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

                //println( "Comparing " + expectedValue + " to " + actualValue + " and " + actualValueByName );

                if ( actualValue == null ) { assertNull( actualValueByName ); }
                else { assertTrue( actualValue.equals( actualValueByName ) ); }
                
                assertEquals( (expectedValue == null), rs.wasNull() );
                
                if ( expectedValue == null )    { assertNull( actualValue ); }
                else { assertEquals(expectedValue, actualValue); }
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

            return new String( blob.getBytes( (long) 0, (int) blob.length() ), UTF8 );
        }
        else if ( obj instanceof Clob )
        {
            Clob    clob = (Clob) obj;

            return clob.getSubString( (long) 0, (int) clob.length() );
        }
        else if ( obj instanceof byte[] )
        {
            byte[]  bytes = (byte[]) obj;

            return new String( bytes, UTF8 );
        }
        else { return obj.toString(); }
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
    
    /**
     * <p>
     * Print a ResultSet, using Derby's pretty-printing tool.
     * </p>
     */
    public  static  void    prettyPrint( Connection conn, ResultSet rs )
        throws SQLException
    {
        org.apache.derby.tools.JDBCDisplayUtil.DisplayResults
            ( System.out, rs, conn );
    }

    //////////////////
    //
    // OPTIMIZER STATS
    //
    //////////////////

    /**
     * <p>
     * Get the optimizer stats for a query.
     * </p>
     */
    private String  getOptimizerStats( String query )
        throws Exception
    {
        goodStatement( "CALL SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1)" );
        goodStatement( "CALL SYSCS_UTIL.SYSCS_SET_STATISTICS_TIMING(1)" );
        goodStatement( query );

        PreparedStatement   ps = prepareStatement( "values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS()" );
        ResultSet           rs = ps.executeQuery();

        rs.next();

        String  retval = rs.getString( 1 );

        rs.close();
        ps.close();

        goodStatement( "CALL SYSCS_UTIL.SYSCS_SET_STATISTICS_TIMING(0)" );
        goodStatement( "CALL SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(0)" );

        return retval;
    }

    
    /**
     * <p>
     * Read the value of a tag in some optimizer output.
     * </p>
     */
    private	double  readDoubleTag( String optimizerOutput, String tag )
        throws Exception
    {
        StringReader        stringReader = new StringReader( optimizerOutput );
        LineNumberReader    lineNumberReader = new LineNumberReader( stringReader );

        while ( true )
        {
            String  line = lineNumberReader.readLine();
            if ( line == null ) { break; }

            int     idx = line.indexOf( tag );

            if ( idx < 0 ) { continue; }

            String remnant = line.substring(idx + tag.length()).trim();

            // Use NumberFormat.parse() instead of Double.parseDouble() to
            // avoid localization issues (DERBY-3100)
            Number result = NumberFormat.getInstance().parse(remnant);
            
            println( "Read " + result + " from optimizer output." );
            return result.doubleValue();
        }

        return 0.0;
    }

    /** Get the conglomerate id of a table */
    private long getConglomerateID( Connection conn, String tableName ) throws Exception
    {
        PreparedStatement ps = conn.prepareStatement
            (
             "select c.conglomeratenumber\n" +
             "from sys.sysconglomerates c, sys.systables t\n" +
             "where t.tablename = ? and t.tableid = c.tableid"
             );
        ps.setString( 1, tableName );

        long result = getScalarLong( ps );

        ps.close();

        return result;
    }

    /** Get a scalar long result from a query */
    private long getScalarLong( PreparedStatement ps ) throws Exception
    {
        ResultSet rs = ps.executeQuery();
        rs.next();
        long retval = rs.getLong( 1 );

        rs.close();
        ps.close();

        return retval;
    }

}
