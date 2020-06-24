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

import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import junit.framework.Test;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.TestConfiguration;

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
//IC see: https://issues.apache.org/jira/browse/DERBY-6590
        BaseTestSuite suite = (BaseTestSuite)TestConfiguration.defaultSuite(
            AnsiSignaturesTest.class);

        return new CleanDatabaseTestSetup( suite );
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // SUCCESSFUL RESOLUTIONS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    public  void    test_boolean_boolean_boolean()
        throws Exception
    {
        declareAndRunFunction
//IC see: https://issues.apache.org/jira/browse/DERBY-4659
            ( "boolean_boolean_boolean", "boolean", new String[] { "boolean" }, "true", "true" );
    }
    public  void    test_boolean_Boolean_boolean()
        throws Exception
    {
        declareAndRunFunction
            ( "boolean_Boolean_boolean", "boolean", new String[] { "boolean" }, "true", "true" );
    }
    public  void    test_boolean_boolean_Boolean()
        throws Exception
    {
        declareAndRunFunction
            ( "boolean_boolean_Boolean", "boolean", new String[] { "boolean" }, "true", "true" );
    }
    public  void    test_boolean_Boolean_Boolean()
        throws Exception
    {
        declareAndRunFunction
            ( "boolean_Boolean_Boolean", "boolean", new String[] { "boolean" }, "true", "true" );
    }
    
    public  void    test_smallint_short_short()
        throws Exception
    {
        declareAndRunFunction
            ( "smallint_short_short", "smallint", new String[] { "smallint" }, "3", "3" );
    }
    public  void    test_smallint_short_Integer()
        throws Exception
    {
        declareAndRunFunction
            ( "smallint_short_Integer", "smallint", new String[] { "smallint" }, "3", "3" );
    }
    public  void    test_smallint_Integer_short()
        throws Exception
    {
        declareAndRunFunction
            ( "smallint_Integer_short", "smallint", new String[] { "smallint" }, "3", "3" );
    }
    public  void    test_smallint_Integer_Integer()
        throws Exception
    {
        declareAndRunFunction
            ( "smallint_Integer_Integer", "smallint", new String[] { "smallint" }, "3", "3" );
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

    public  void    test_numeric_BigDecimal_BigDecimal()
        throws Exception
    {
        //
        // On small device platforms, this raises an exception in the byte-code
        // compiler. See DERBY-3697.
        //
        if ( JDBC.vmSupportsJSR169() ) { return; }
        
        declareAndRunFunction
            ( "numeric_BigDecimal_BigDecimal", "numeric( 7, 2 )", new String[] { "numeric( 7, 2 )" }, "12345.67", "12345.67" );
    }
    
    public  void    test_decimal_BigDecimal_BigDecimal()
        throws Exception
    {
        //
        // On small device platforms, this raises an exception in the byte-code
        // compiler. See DERBY-3697.
        //
        if ( JDBC.vmSupportsJSR169() ) { return; }
        
        declareAndRunFunction
            ( "decimal_BigDecimal_BigDecimal", "decimal( 7, 2 )", new String[] { "decimal( 7, 2 )" }, "12345.67", "12345.67" );
    }
    
    public  void    test_varchar_String_String()
        throws Exception
    {
        declareAndRunFunction
            ( "varchar_String_String", "varchar( 10 )", new String[] { "varchar( 10 )" }, "'3.0'", "3.0" );
    }

    public  void    test_char_String_String()
        throws Exception
    {
        declareAndRunFunction
            ( "char_String_String", "char( 10 )", new String[] { "char( 10 )" }, "'3.0'", "3.0       " );
    }

    public  void    test_longvarchar_String_String()
        throws Exception
    {
        // long varchar is not allowed as an argument type in a Derby routine
        declareAndRunFunction
            ( "longvarchar_String_String", "long varchar", new String[] { "varchar( 10 )" }, "'3.0'", "3.0" );
    }

    public  void    test_bigint__smallint_int_bigint_real_double()
        throws Exception
    {
        declareAndRunFunction
            (
             "bigint__smallint_int_bigint_real_double",
             "bigint",
              new String[] { "smallint", "int", "bigint", "real", "double" },
             "3, 3, 3, 3.0, 3.0",
             "3"
             );
    }

    public  void    test_flipped_bigint__smallint_int_bigint_real_double()
        throws Exception
    {
        declareAndRunFunction
            (
             "flipped_bigint__smallint_int_bigint_real_double",
             "bigint",
              new String[] { "smallint", "int", "bigint", "real", "double" },
             "3, 3, 3, 3.0, 3.0",
             "3"
             );
    }

    public  void    test_binary_bytes_bytes()
        throws Exception
    {
        declareAndRunFunction
            ( "binary_bytes_bytes", "char( 2 ) for bit data", new String[] { "char( 2 ) for bit data" }, "X'a1b2'", "a1b2" );
    }

    public  void    test_binary_bytes_int()
        throws Exception
    {
        declareAndRunFunction
            ( "binary_bytes_int", "char( 1 ) for bit data", new String[] { "integer" }, "3", "03" );
    }

    public  void    test_varbinary_bytes_bytes()
        throws Exception
    {
        declareAndRunFunction
            ( "varbinary_bytes_bytes", "varchar( 2 ) for bit data", new String[] { "varchar( 2 ) for bit data" }, "X'a1b2'", "a1b2" );
    }

    public  void    test_varbinary_bytes_int()
        throws Exception
    {
        declareAndRunFunction
            ( "varbinary_bytes_int", "char( 1 ) for bit data", new String[] { "integer" }, "3", "03" );
    }

    public  void    test_longvarbinary_bytes_bytes()
        throws Exception
    {
        declareAndRunFunction
            ( "longvarbinary_bytes_bytes", "long varchar for bit data", new String[] { "varchar(2) for bit data" }, "X'a1b2'", "a1b2" );
    }

    public  void    test_longvarbinary_bytes_int()
        throws Exception
    {
        declareAndRunFunction
            ( "longvarbinary_bytes_int", "long varchar for bit data", new String[] { "integer" }, "3", "03" );
    }

    public  void    test_date_Date_Date()
        throws Exception
    {
        declareAndRunFunction
            ( "date_Date_Date", "date", new String[] { "date" }, "date('1994-02-23')", "1994-02-23" );
    }

    public  void    test_time_Time_Time()
        throws Exception
    {
        declareAndRunFunction
            ( "time_Time_Time", "time", new String[] { "time" }, "time('15:09:02')", "15:09:02" );
    }

    public  void    test_timestamp_Timestamp_Timestamp()
        throws Exception
    {
        declareAndRunFunction
            ( "timestamp_Timestamp_Timestamp", "timestamp", new String[] { "timestamp" }, "timestamp('1962-09-23 03:23:34.234')", "1962-09-23 03:23:34.234" );
    }

    public  void    test_clob_Clob_String()
        throws Exception
    {
        Connection  conn = getConnection();
//IC see: https://issues.apache.org/jira/browse/DERBY-4066

        declareAndRunFunction
            ( "clob_Clob_String", "clob", new String[] { "varchar( 10 )" }, "'3'", "3" );

        // now test clob arguments
        declareFunction( conn, "varchar_Clob_Clob", "varchar( 10 )", new String[] { "clob" } );

        runFunction( conn, "varchar_Clob_Clob", " \"clob_Clob_String\"( 'abc' )", "abc", null );

        // make sure that you can set lob-typed ? parameters
        PreparedStatement ps = chattyPrepare( conn, "values ( \"varchar_Clob_Clob\"( ? ) )" );
        String expectedValue = "abcdef";
        Clob clob = AnsiSignatures.clob_Clob_String( expectedValue );
        ps.setClob( 1, clob );
        String actualValue = (String) getScalarString( ps );
        assertTrue( expectedValue.equals( actualValue ) );
    }

    public  void    test_blob_Blob_String()
        throws Exception
    {
        Connection  conn = getConnection();

        declareAndRunFunction
            ( "blob_Blob_String", "blob", new String[] { "varchar( 10 )" }, "'3'", "33" );

        // now test blob arguments
        declareFunction( conn, "varchar_Blob_Blob", "varchar( 10 )", new String[] { "blob" } );

        runFunction( conn, "varchar_Blob_Blob", " \"blob_Blob_String\"( 'abc' )", "abc", null );

        // make sure that you can set lob-typed ? parameters
        PreparedStatement ps = chattyPrepare( conn, "values ( \"varchar_Blob_Blob\"( ? ) )" );
        String expectedValue = "34";
        Blob blob = AnsiSignatures.blob_Blob_String( expectedValue );
        ps.setBlob( 1, blob );
        String actualValue = getScalarString( ps );
        assertTrue( expectedValue.equals( actualValue ) );
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
            ( "smallint_bad_short_Short", "smallint", new String[] { "smallint" }, "3", "3", TRIED_ALL_COMBINATIONS );
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
    // BAD RETURN TYPES
    //
    ///////////////////////////////////////////////////////////////////////////////////

    public  void    test_boolean_badreturn_byte_int()
//IC see: https://issues.apache.org/jira/browse/DERBY-4657
        throws Exception
    {
        declareAndFailFunction
            ( "boolean_badreturn_byte_int", "boolean", new String[] { "int" }, "3", "true", TRIED_ALL_COMBINATIONS );
    }

    public  void    test_smallint_badreturn_byte_short()
        throws Exception
    {
        declareAndFailFunction
            ( "smallint_badreturn_byte_short", "smallint", new String[] { "smallint" }, "3", "3", TRIED_ALL_COMBINATIONS );
    }

    public  void    test_integer_badreturn_byte_int()
        throws Exception
    {
        declareAndFailFunction
            ( "integer_badreturn_byte_int", "int", new String[] { "int" }, "3", "3", TRIED_ALL_COMBINATIONS );
    }

    public  void    test_bigint_badreturn_byte_long()
        throws Exception
    {
        declareAndFailFunction
            ( "bigint_badreturn_byte_long", "bigint", new String[] { "bigint" }, "3", "3",  TRIED_ALL_COMBINATIONS );
    }

    public  void    test_real_badreturn_byte_float()
        throws Exception
    {
        declareAndFailFunction
            ( "real_badreturn_byte_float", "real", new String[] { "real" }, "3.0", "3.0", TRIED_ALL_COMBINATIONS );
    }

     public  void    test_double_badreturn_byte_double()
        throws Exception
    {
        declareAndFailFunction
            ( "double_badreturn_byte_double", "double", new String[] { "double" }, "3.0", "3.0", TRIED_ALL_COMBINATIONS );
    }

     public  void    test_binary_badreturn_bytes_bytes()
        throws Exception
    {
        declareAndFailFunction
            ( "binary_badreturn_bytes_bytes", "char( 2 ) for bit data", new String[] { "char( 2 ) for bit data" }, "X'a1b2'", "a1b2", TRIED_ALL_COMBINATIONS );
    }

     public  void    test_varbinary_badreturn_bytes_bytes()
        throws Exception
    {
        declareAndFailFunction
            ( "varbinary_badreturn_bytes_bytes", "varchar( 2 ) for bit data", new String[] { "varchar( 2 ) for bit data" }, "X'a1b2'", "a1b2", TRIED_ALL_COMBINATIONS );
    }

     public  void    test_longvarbinary_badreturn_bytes_bytes()
        throws Exception
    {
        declareAndFailFunction
            ( "longvarbinary_badreturn_bytes_bytes", "long varchar for bit data", new String[] { "char( 2 ) for bit data" }, "X'a1b2'", "a1b2", TRIED_ALL_COMBINATIONS );
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // AMBIGUOUS METHODS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    public  void    test_boolean_amb_boolean_boolean()
//IC see: https://issues.apache.org/jira/browse/DERBY-4659
        throws Exception
    {
        declareAndFailFunction
             ( "boolean_amb_boolean_boolean", "boolean", new String[] { "boolean" }, "true", "true", AMBIGUOUS );
    }
    public  void    test_boolean_amb_Boolean_boolean()
        throws Exception
    {
        declareAndFailFunction
            ( "boolean_amb_Boolean_boolean", "boolean", new String[] { "boolean" }, "true", "true", AMBIGUOUS );
    }

    public  void    test_smallint_amb_short_short()
        throws Exception
    {
        declareAndFailFunction
            ( "smallint_amb_short_short", "smallint", new String[] { "smallint" }, "3", "3", AMBIGUOUS );
    }
    public  void    test_smallint_amb_Integer_short()
        throws Exception
    {
        declareAndFailFunction
            ( "smallint_amb_Integer_short", "smallint", new String[] { "smallint" }, "3", "3", AMBIGUOUS );
    }
    public  void    test_smallint_amb_byte_short()
        throws Exception
    {
        declareAndFailFunction
            ( "smallint_amb_byte_short", "smallint", new String[] { "smallint" }, "3", "3", AMBIGUOUS );
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
    public  void    test_integer_amb_byte_int()
        throws Exception
    {
        declareAndFailFunction
             ( "integer_amb_byte_int", "int", new String[] { "int" }, "3", "3", AMBIGUOUS );
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
    public  void    test_bigint_amb_byte_long()
        throws Exception
    {
        declareAndFailFunction
            ( "bigint_amb_byte_long", "bigint", new String[] { "bigint" }, "3", "3", AMBIGUOUS );
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
    public  void    test_real_amb_byte_float()
        throws Exception
    {
        declareAndFailFunction
            ( "real_amb_byte_float", "real", new String[] { "real" }, "3.0", "3.0", AMBIGUOUS );
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
    public  void    test_double_amb_byte_double()
        throws Exception
    {
        declareAndFailFunction
            ( "double_amb_byte_double", "double", new String[] { "double" }, "3.0", "3.0", AMBIGUOUS );
    }
    
    
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // UNRESOLVABLE METHODS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    public  void    test_boolean_unres_boolean()
//IC see: https://issues.apache.org/jira/browse/DERBY-4659
        throws Exception
    {
        declareAndFailFunction
            ( "boolean_unres_boolean", "boolean", new String[] { "boolean" }, "true", "true", TRIED_ALL_COMBINATIONS );
    }
    public  void    test_boolean_unres_Boolean()
        throws Exception
    {
        declareAndFailFunction
            ( "boolean_unres_Boolean", "boolean", new String[] { "boolean" }, "true", "true", TRIED_ALL_COMBINATIONS );
    }
    
    public  void    test_smallint_unres_short()
        throws Exception
    {
        declareAndFailFunction
            ( "smallint_unres_short", "smallint", new String[] { "smallint" }, "3", "3", TRIED_ALL_COMBINATIONS );
    }
    public  void    test_smallint_unres_Short()
        throws Exception
    {
        declareAndFailFunction
            ( "smallint_unres_Short", "smallint", new String[] { "smallint" }, "3", "3", TRIED_ALL_COMBINATIONS );
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
    // PRODEDURES
    //
    ///////////////////////////////////////////////////////////////////////////////////

    public  void    test_wrapperOutputArgs()
//IC see: https://issues.apache.org/jira/browse/DERBY-4958
        throws Exception
    {
        Connection  conn = getConnection();
        PreparedStatement   ps = conn.prepareStatement
            (
             "create procedure wrapperProc\n" +
             "(\n" +
             "    out bigintCol bigint,\n" +
             "    out booleanCol boolean,\n" +
             "    out doubleCol double,\n" +
             "    out floatCol float,\n" +
             "    out intCol int,\n" +
             "    out realCol real,\n" +
             "    out smallintCol smallint\n" +
             ")\n" +
             "language java\n" +
             "parameter style java\n" +
             "no sql\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.AnsiSignatures.wrapperProc'\n"
             );
        ps.execute();
        ps.close();

        CallableStatement cs = conn.prepareCall
            (
             "call wrapperProc(  ?, ?, ?, ?, ?, ?, ? )"
             );
        int param = 1;
        cs.registerOutParameter( param++, Types.BIGINT );
        cs.registerOutParameter( param++, Types.BOOLEAN );
        cs.registerOutParameter( param++, Types.DOUBLE );
        cs.registerOutParameter( param++, Types.FLOAT );
        cs.registerOutParameter( param++, Types.INTEGER );
        cs.registerOutParameter( param++, Types.REAL );
        cs.registerOutParameter( param++, Types.SMALLINT );

        cs.execute();
        param = 1;
        assertEquals( 1L, cs.getLong( param++ ) );
        assertEquals( true, cs.getBoolean( param++ ) );
        assertEquals( 1.0, cs.getDouble( param++ ), 0.0 );
        assertEquals( 1.0, cs.getDouble( param++ ), 0.0 );
        assertEquals( 1, cs.getInt( param++ ) );
        assertEquals( 1.0F, cs.getFloat( param++ ), 0.0F );
        assertEquals( (short) 1, cs.getShort( param++ ) );
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
    
    /**
     * Prepare a statement and report its sql text.
     */
    protected PreparedStatement   chattyPrepare( Connection conn, String text )
//IC see: https://issues.apache.org/jira/browse/DERBY-4066
        throws SQLException
    {
        println( "Preparing statement:\n\t" + text );
        
        return conn.prepareStatement( text );
    }

    /** Get a scalar result from a query */
    private String getScalarString( PreparedStatement ps ) throws Exception
    {
        ResultSet rs = ps.executeQuery();
        rs.next();
        String retval = rs.getString( 1 );

        rs.close();
        ps.close();

        return retval;
    }
  
}
