/**
 *  Derby - Class org.apache.derbyTesting.functionTests.tests.lang.CastingTest
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

import java.sql.Connection;
import java.sql.DataTruncation;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.sql.Types;
import java.util.Arrays;
import java.util.HashSet;

import junit.framework.Test;

import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.SQLUtilities;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 *
 */
public class CastingTest extends BaseJDBCTestCase {

    public static final class TypedColumn
    {
        public String columnName;
        public String typeName;
        public boolean comparable; // true except for long, non-indexable data types

            public TypedColumn( String columnName, String typeName, boolean comparable )
        {
            this.columnName = columnName;
            this.typeName = typeName;
            this.comparable = comparable;
        }
    }

    public CastingTest(String name) {
        super(name);

    }
    public static String VALID_DATE_STRING = "'2000-01-01'";
    public static String VALID_TIME_STRING = "'15:30:20'";
    public static String VALID_TIMESTAMP_STRING = "'2000-01-01 15:30:20'";
    public static String NULL_VALUE="NULL";

    public static String ILLEGAL_CAST_EXCEPTION_SQLSTATE = "42846";
    public static String LANG_NOT_STORABLE_SQLSTATE  = "42821";
    public static String LANG_NOT_COMPARABLE_SQLSTATE = "42818";
    public static String METHOD_NOT_FOUND_SQLSTATE = "42884";
    public static String LANG_FORMAT_EXCEPTION_SQLSTATE = "22018";

    public static int SQLTYPE_ARRAY_SIZE = 17 ;
    public static int SMALLINT_OFFSET = 0;
    public static int INTEGER_OFFSET = 1;
    public static int BIGINT_OFFSET = 2;
    public static int DECIMAL_OFFSET = 3;
    public static int REAL_OFFSET = 4;
    public static int DOUBLE_OFFSET = 5;
    public static int CHAR_OFFSET = 6;
    public static int VARCHAR_OFFSET = 7;
    public static int LONGVARCHAR_OFFSET = 8;
    public static int CHAR_FOR_BIT_OFFSET = 9;
    public static int VARCHAR_FOR_BIT_OFFSET = 10;
    public static int LONGVARCHAR_FOR_BIT_OFFSET = 11;
    public static int CLOB_OFFSET = 12;
    public static int DATE_OFFSET = 13;
    public static int TIME_OFFSET = 14;
    public static int TIMESTAMP_OFFSET = 15;
    public static int BLOB_OFFSET = 16;

    public static int[] jdbcTypes = {
        java.sql.Types.SMALLINT,
        java.sql.Types.INTEGER,
        java.sql.Types.BIGINT,
        java.sql.Types.DECIMAL,
        java.sql.Types.REAL,
        java.sql.Types.DOUBLE,
        java.sql.Types.CHAR,
        java.sql.Types.VARCHAR,
        java.sql.Types.LONGVARCHAR,
        java.sql.Types.BINARY,
        java.sql.Types.VARBINARY,
        java.sql.Types.LONGVARBINARY,
        java.sql.Types.CLOB,
        java.sql.Types.DATE,
        java.sql.Types.TIME,
        java.sql.Types.TIMESTAMP,
        java.sql.Types.BLOB
    };
    
    public static int NULL_DATA_OFFSET = 0;  // offset of NULL value
    public static int VALID_DATA_OFFSET = 1;  // offset of NULL value

    // rows are data types.
    // data is NULL_VALUE, VALID_VALUE
    // Should add Minimum, Maximum and out of range.
public static String[][]SQLData =
    {
            {NULL_VALUE, "0"},       // SMALLINT
            {NULL_VALUE,"11"},       // INTEGER
            {NULL_VALUE,"22"},       // BIGINT
            {NULL_VALUE,"3.3"},      // DECIMAL(10,5)
            {NULL_VALUE,"4.4"},      // REAL,
            {NULL_VALUE,"5.5"},      // DOUBLE
            {NULL_VALUE,"'7'"},      // CHAR(60)
            {NULL_VALUE,"'8'"},      //VARCHAR(60)",
            {NULL_VALUE,"'9'"},      // LONG VARCHAR
            {NULL_VALUE,"X'10aa'"},  // CHAR(60)  FOR BIT DATA
            {NULL_VALUE,"X'10bb'"},  // VARCHAR(60) FOR BIT DATA
            {NULL_VALUE,"X'10cc'"},  //LONG VARCHAR FOR BIT DATA
            {NULL_VALUE,"'13'"},     //CLOB(1k)
            {NULL_VALUE,VALID_DATE_STRING},        // DATE
            {NULL_VALUE,VALID_TIME_STRING},        // TIME
            {NULL_VALUE,VALID_TIMESTAMP_STRING},   // TIMESTAMP
            {NULL_VALUE,"X'01dd'"}                 // BLOB
    };




    public static final boolean n = false;
    public static final boolean X = true;

    /**
       Table 146 - Supported explicit casts between Built-in DataTypes

       This table has THE FOR BIT DATA TYPES broken out into separate columns
       for clarity and testing
    **/


    public static final boolean[][]  T_146 = {
            
//Types.                  S  I  B  D  R  D  C  V  L  C  V  L  C  D  T  T  B
//                        M  N  I  E  E  O  H  A  O  H  A  O  L  A  I  I  L
//                        A  T  G  C  A  U  A  R  N  A  R  N  O  T  M  M  O
//                        L  E  I  I  L  B  R  C  G  R  C  G  B  E  E  E  B
//                        L  G  N  M     L     H  V  .  H  V           S
//                        I  E  T  A     E     A  A  B  .  A           T
//                        N  R     L           R  R  I  B  R           A
//                        T                       C  T  I  .           M
//                                                H     T  B           P
//                                                A        I
//                                                R        T
/* 0 SMALLINT */        { X, X, X, X, X, X, X, n, n, n, n, n, n, n, n, n, n },
/* 1 INTEGER  */        { X, X, X, X, X, X, X, n, n, n, n, n, n, n, n, n, n },
/* 2 BIGINT   */        { X, X, X, X, X, X, X, n, n, n, n, n, n, n, n, n, n },
/* 3 DECIMAL  */        { X, X, X, X, X, X, X, n, n, n, n, n, n, n, n, n, n },
/* 4 REAL     */        { X, X, X, X, X, X, n, n, n, n, n, n, n, n, n, n, n },
/* 5 DOUBLE   */        { X, X, X, X, X, X, n, n, n, n, n, n, n, n, n, n, n },
/* 6 CHAR     */        { X, X, X, X, n, n, X, X, X, n, n, n, X, X, X, X, n },
/* 7 VARCHAR  */        { X, X, X, X, n, n, X, X, X, n, n, n, X, X, X, X, n },
/* 8 LONGVARCHAR */     { n, n, n, n, n, n, X, X, X, n, n, n, X, n, n, n, n },
/* 9 CHAR FOR BIT */    { n, n, n, n, n, n, n, n, n, X, X, X, n, n, n, n, X },
/* 10 VARCH. BIT   */   { n, n, n, n, n, n, n, n, n, X, X, X, n, n, n, n, X },
/* 11 LONGVAR. BIT */   { n, n, n, n, n, n, n, n, n, X, X, X, n, n, n, n, X },
/* 12 CLOB         */   { n, n, n, n, n, n, X, X, X, n, n, n, X, n, n, n, n },
/* 13 DATE         */   { n, n, n, n, n, n, X, X, n, n, n, n, n, X, n, X, n },
/* 14 TIME         */   { n, n, n, n, n, n, X, X, n, n, n, n, n, n, X, X, n },
/* 15 TIMESTAMP    */   { n, n, n, n, n, n, X, X, n, n, n, n, n, X, X, X, n },
/* 16 BLOB         */   { n, n, n, n, n, n, n, n, n, n, n, n, n, n, n, n, X },

    };

    /**
     * Table 147 describes  Data Type Compatibility for Assignments 
     *
     * The table 147a covers the assignments as they do differ somewhat 
     *  from comparisons which can be found in 147b
     *
     **/

    public static final boolean[][]  T_147a = {
            
//Types.                  S  I  B  D  R  D  C  V  L  C  V  L  C  D  T  T  B
//                        M  N  I  E  E  O  H  A  O  H  A  O  L  A  I  I  L
//                        A  T  G  C  A  U  A  R  N  A  R  N  O  T  M  M  O
//                        L  E  I  I  L  B  R  C  G  R  C  G  B  E  E  E  B
//                        L  G  N  M     L     H  V  .  H  V           S
//                        I  E  T  A     E     A  A  B  .  A           T
//                        N  R     L           R  R  I  B  R           A
//                        T                       C  T  I  .           M
//                                                H     T  B           P
//                                                A        I
//                                                R        T
/* 0 SMALLINT */        { X, X, X, X, X, X, n, n, n, n, n, n, n, n, n, n, n },
/* 1 INTEGER  */        { X, X, X, X, X, X, n, n, n, n, n, n, n, n, n, n, n },
/* 2 BIGINT   */        { X, X, X, X, X, X, n, n, n, n, n, n, n, n, n, n, n },
/* 3 DECIMAL  */        { X, X, X, X, X, X, n, n, n, n, n, n, n, n, n, n, n },
/* 4 REAL     */        { X, X, X, X, X, X, n, n, n, n, n, n, n, n, n, n, n },
/* 5 DOUBLE   */        { X, X, X, X, X, X, n, n, n, n, n, n, n, n, n, n, n },
/* 6 CHAR     */        { n, n, n, n, n, n, X, X, X, n, n, n, X, X, X, X, n },
/* 7 VARCHAR  */        { n, n, n, n, n, n, X, X, X, n, n, n, X, X, X, X, n },
/* 8 LONGVARCHAR */     { n, n, n, n, n, n, X, X, X, n, n, n, X, n, n, n, n },
/* 9 CHAR FOR BIT */    { n, n, n, n, n, n, n, n, n, X, X, X, n, n, n, n, n },
/* 10 VARCH. BIT   */   { n, n, n, n, n, n, n, n, n, X, X, X, n, n, n, n, n },
/* 11 LONGVAR. BIT */   { n, n, n, n, n, n, n, n, n, X, X, X, n, n, n, n, n },
/* 12 CLOB         */   { n, n, n, n, n, n, X, X, X, n, n, n, X, n, n, n, n },
/* 13 DATE         */   { n, n, n, n, n, n, X, X, n, n, n, n, n, X, n, n, n },
/* 14 TIME         */   { n, n, n, n, n, n, X, X, n, n, n, n, n, n, X, n, n },
/* 15 TIMESTAMP    */   { n, n, n, n, n, n, X, X, n, n, n, n, n, n, n, X, n },
/* 16 BLOB         */   { n, n, n, n, n, n, n, n, n, n, n, n, n, n, n, n, X },

    };


    // Comparisons table
    // Comparison's are different than assignments because
    // Long types cannot be compared.
    // Note: This table is referenced in NullIfTest.java
    public static final boolean[][]  T_147b = {
            
//Types.                  S  I  B  D  R  D  C  V  L  C  V  L  C  D  T  T  B
//                        M  N  I  E  E  O  H  A  O  H  A  O  L  A  I  I  L
//                        A  T  G  C  A  U  A  R  N  A  R  N  O  T  M  M  O
//                        L  E  I  I  L  B  R  C  G  R  C  G  B  E  E  E  B
//                        L  G  N  M     L     H  V  .  H  V           S
//                        I  E  T  A     E     A  A  B  .  A           T
//                        N  R     L           R  R  I  B  R           A
//                        T                       C  T  I  .           M
//                                                H     T  B           P
//                                                A        I
//                                                R        T
/* 0 SMALLINT */        { X, X, X, X, X, X, n, n, n, n, n, n, n, n, n, n, n },
/* 1 INTEGER  */        { X, X, X, X, X, X, n, n, n, n, n, n, n, n, n, n, n },
/* 2 BIGINT   */        { X, X, X, X, X, X, n, n, n, n, n, n, n, n, n, n, n },
/* 3 DECIMAL  */        { X, X, X, X, X, X, n, n, n, n, n, n, n, n, n, n, n },
/* 4 REAL     */        { X, X, X, X, X, X, n, n, n, n, n, n, n, n, n, n, n },
/* 5 DOUBLE   */        { X, X, X, X, X, X, n, n, n, n, n, n, n, n, n, n, n },
/* 6 CHAR     */        { n, n, n, n, n, n, X, X, n, n, n, n, n, X, X, X, n },
/* 7 VARCHAR  */        { n, n, n, n, n, n, X, X, n, n, n, n, n, X, X, X, n },
/* 8 LONGVARCHAR */     { n, n, n, n, n, n, n, n, n, n, n, n, n, n, n, n, n },
/* 9 CHAR FOR BIT */    { n, n, n, n, n, n, n, n, n, X, X, n, n, n, n, n, n },
/* 10 VARCH. BIT   */   { n, n, n, n, n, n, n, n, n, X, X, n, n, n, n, n, n },
/* 11 LONGVAR. BIT */   { n, n, n, n, n, n, n, n, n, n, n, n, n, n, n, n, n },
/* 12 CLOB         */   { n, n, n, n, n, n, n, n, n, n, n, n, n, n, n, n, n },
/* 13 DATE         */   { n, n, n, n, n, n, X, X, n, n, n, n, n, X, n, n, n },
/* 14 TIME         */   { n, n, n, n, n, n, X, X, n, n, n, n, n, n, X, n, n },
/* 15 TIMESTAMP    */   { n, n, n, n, n, n, X, X, n, n, n, n, n, n, n, X, n },
/* 16 BLOB         */   { n, n, n, n, n, n, n, n, n, n, n, n, n, n, n, n, n },


};
   /*
    * explicitCastValues is a table of expected values for the testExplicitCasts fixture
    * System.out.print statements in testExplicitCast were used to generate this table
    * and  remain in comments in testExplicitCasts in case it needs to be regenerated.
    */ 
    private static final String[][] explicitCastValues = {
    /*SMALLINT*/ {"0","0","0","0.00000","0.0","0.0","0                                                           ","Exception","Exception","Exception","Exception","Exception","Exception","Exception","Exception","Exception","Exception"},
    /*INTEGER*/ {"11","11","11","11.00000","11.0","11.0","11                                                          ","Exception","Exception","Exception","Exception","Exception","Exception","Exception","Exception","Exception","Exception"},
    /*BIGINT*/ {"22","22","22","22.00000","22.0","22.0","22                                                          ","Exception","Exception","Exception","Exception","Exception","Exception","Exception","Exception","Exception","Exception"},
    /*DECIMAL(10,5)*/ {"3","3","3","3.30000","3.3","3.3","3.30000                                                     ","Exception","Exception","Exception","Exception","Exception","Exception","Exception","Exception","Exception","Exception"},
    /*REAL*/ {"4","4","4","4.40000","4.4","4.400000095367432","Exception","Exception","Exception","Exception","Exception","Exception","Exception","Exception","Exception","Exception","Exception"},
    /*DOUBLE*/ {"5","5","5","5.50000","5.5","5.5","Exception","Exception","Exception","Exception","Exception","Exception","Exception","Exception","Exception","Exception","Exception"},
    /*CHAR(60)*/ {"0","11","22","3.30000","Exception","Exception","7                                                           ","8                                                           ","9                                                           ","Exception","Exception","Exception","13                                                          ","2000-01-01","15:30:20","2000-01-01 15:30:20.0","Exception"},
    /*VARCHAR(60)*/ {"0","11","22","3.30000","Exception","Exception","7                                                           ","8","9","Exception","Exception","Exception","13","2000-01-01","15:30:20","2000-01-01 15:30:20.0","Exception"},
    /*LONG VARCHAR*/ {"Exception","Exception","Exception","Exception","Exception","Exception","7                                                           ","8","9","Exception","Exception","Exception","13","Exception","Exception","Exception","Exception"},
    /*CHAR(60) FOR BIT DATA*/ {"Exception","Exception","Exception","Exception","Exception","Exception","Exception","Exception","Exception","10aa20202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020","10bb20202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020","10cc20202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020","Exception","Exception","Exception","Exception","01dd20202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020"},
    /*VARCHAR(60) FOR BIT DATA*/ {"Exception","Exception","Exception","Exception","Exception","Exception","Exception","Exception","Exception","10aa20202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020","10bb","10cc","Exception","Exception","Exception","Exception","01dd"},
    /*LONG VARCHAR FOR BIT DATA*/ {"Exception","Exception","Exception","Exception","Exception","Exception","Exception","Exception","Exception","10aa20202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020","10bb","10cc","Exception","Exception","Exception","Exception","01dd"},
    /*CLOB(1k)*/ {"Exception","Exception","Exception","Exception","Exception","Exception","13                                                          ","13","13","Exception","Exception","Exception","13","Exception","Exception","Exception","Exception"},
    /*DATE*/ {"Exception","Exception","Exception","Exception","Exception","Exception","2000-01-01                                                  ","2000-01-01","Exception","Exception","Exception","Exception","Exception","2000-01-01","Exception","2000-01-01 00:00:00.0","Exception"},
    /*TIME*/ {"Exception","Exception","Exception","Exception","Exception","Exception","15:30:20                                                    ","15:30:20","Exception","Exception","Exception","Exception","Exception","Exception","15:30:20","TODAY 15:30:20.0","Exception"},
    /*TIMESTAMP*/ {"Exception","Exception","Exception","Exception","Exception","Exception","2000-01-01 15:30:20.0                                       ","2000-01-01 15:30:20.0","Exception","Exception","Exception","Exception","Exception","2000-01-01","15:30:20","2000-01-01 15:30:20.0","Exception"},
    /*BLOB(1k)*/ {"Exception","Exception","Exception","Exception","Exception","Exception","Exception","Exception","Exception","Exception","Exception","Exception","Exception","Exception","Exception","Exception","01dd"}
    };

    private static final TypedColumn[] LEGAL_BOOLEAN_CASTS = new TypedColumn[]
    {
        new TypedColumn( "charCol", "char( 5 )", true ),
        new TypedColumn( "varcharCol", "varchar( 5 )", true ),
        new TypedColumn( "longVarcharCol", "long varchar", false ),
        new TypedColumn( "clobCol", "clob", false ),
        new TypedColumn( "booleanCol", "boolean", true ),
    };
    
    private static final TypedColumn[] ILLEGAL_BOOLEAN_CASTS = new TypedColumn[]
    {
        new TypedColumn( "bigintCol", "bigint", true ),
        new TypedColumn( "blobCol", "blob", false ),
        new TypedColumn( "charForBitDataCol", "char( 5 ) for bit data", true ),
        new TypedColumn( "dateCol", "date", true ),
        new TypedColumn( "decimalCol", "decimal", true ),
        new TypedColumn( "doubleCol", "double", true ),
        new TypedColumn( "floatCol", "float", true ),
        new TypedColumn( "integerCol", "integer", true ),
        new TypedColumn( "longVarcharForBitDataCol", "long varchar for bit data", false ),
        new TypedColumn( "numericCol", "numeric", true ),
        new TypedColumn( "realCol", "real", true ),
        new TypedColumn( "smallintCol", "smallint", true ),
        new TypedColumn( "timeCol", "time", true ),
        new TypedColumn( "timestampCol", "timestamp", true ),
        new TypedColumn( "varcharForBitDataCol", "varchar( 5 ) for bit data", true ),
        new TypedColumn( "xmlCol", "xml", false ),
    };
    
    protected void setUp() throws SQLException {
        Statement scb = createStatement();

        for (int type = 0; type < SQLUtilities.SQLTypes.length; type++) {
            String typeName = SQLUtilities.SQLTypes[type];
            String tableName = getTableName(type);

            String createSQL = "create table " + tableName + " (c " + typeName
                    + " )";

            scb.executeUpdate(createSQL);
            
        }
        // * testing literal inserts

        for (int dataOffset = 0; dataOffset < SQLData[0].length; dataOffset++)
            for (int type = 0; type < SQLUtilities.SQLTypes.length; type++) {
                try {
                    String tableName = getTableName(type);

                    String insertSQL = "insert into " + tableName + " values( "
                            + SQLData[type][dataOffset] + ")";
                    scb.executeUpdate(insertSQL);
                } catch (SQLException se) {
                    // literal inserts are ok for everything but BLOB
                    if (type != BLOB_OFFSET)
                        throw se;
                    
                }
            }
        scb.close();
        commit();
    }

    public void testAssignments() throws SQLException {

        Statement scb = createStatement();

        // Try to insert each sourceType into the targetType table
        for (int dataOffset = 0; dataOffset < SQLData[0].length; dataOffset++)
            for (int sourceType = 0; sourceType < SQLUtilities.SQLTypes.length; sourceType++) {
                String sourceTypeName = SQLUtilities.SQLTypes[sourceType];
                for (int targetType = 0; targetType < SQLUtilities.SQLTypes.length; targetType++) {
                    try {
                        String targetTableName = getTableName(targetType);

                        // For assignments Character types use strings that can
                        // be converted to the targetType.
                        String convertString = getCompatibleString(sourceType,
                                targetType, dataOffset);

                        String insertValuesString = " VALUES CAST("
                                + convertString + " AS " + sourceTypeName + ")";

                        String insertSQL = "INSERT INTO " + targetTableName
                                + insertValuesString;
                        // System.out.println(insertSQL);
                        scb.executeUpdate(insertSQL);
                        checkSupportedAssignment(sourceType, targetType);

                    } catch (SQLException se) {
                        String sqlState = se.getSQLState();
                        assertTrue(!isSupportedAssignment(sourceType, targetType)
                                && isNotStorableException(se)
                                || isCastException(se));
                    }
                }
            }

        scb.close();
        commit();
    }

    public void testExplicitCasts() throws SQLException {

        Statement s = createStatement();

        // Try Casts from each type to the
        for (int sourceType = 0; sourceType < SQLUtilities.SQLTypes.length; sourceType++) {

            String sourceTypeName = SQLUtilities.SQLTypes[sourceType];
            //System.out.print("/*" + sourceTypeName + "*/ {");
            for (int dataOffset = 0; dataOffset < SQLData[0].length; dataOffset++)
                for (int targetType = 0; targetType < SQLUtilities.SQLTypes.length; targetType++) {
                    try {
                        // Record the start time so that we can calculate
                        // the current date when checking TIME -> TIMESTAMP
                        // conversion.
                        final long startTime = System.currentTimeMillis();

                        String targetTypeName = SQLUtilities.SQLTypes[targetType];
                        // For casts from Character types use strings that can
                        // be converted to the targetType.

                        String convertString = getCompatibleString(sourceType,
                                targetType, dataOffset);

                        String query =
                            "VALUES CAST (CAST (" + convertString + " AS "
                                + SQLUtilities.SQLTypes[sourceType] + ") AS "
                                + SQLUtilities.SQLTypes[targetType] + " )";
                        ResultSet rs = s.executeQuery(query);
                        rs.next();
                        String val = rs.getString(1);
                        ResultSetMetaData rsmd = rs.getMetaData();
                        assertEquals(rsmd.getColumnType(1), jdbcTypes[targetType]);
                        rs.close();

                        // Record the time after finishing the data retrieval.
                        // Used for calculating the current date when checking
                        // TIME -> TIMESTAMP conversion.
                        final long finishTime = System.currentTimeMillis();

                        if (dataOffset == 0)
                            assertNull(val);
                        else
                        {
                            //System.out.print("\"" + val + "\"");
                            String expected =
                                explicitCastValues[sourceType][targetType];

                            if (isTime(sourceType) && isTimestamp(targetType)) {
                                // The expected value for a cast from TIME to
                                // TIMESTAMP includes the current date, so
                                // construct the expected value at run-time.
                                // We may have crossed midnight during query
                                // execution, in which case we cannot tell
                                // whether today or yesterday was used. Accept
                                // both.
                                String[] expectedValues = {
                                    expected.replace(
                                      "TODAY", new Date(startTime).toString()),
                                    expected.replace(
                                      "TODAY", new Date(finishTime).toString()),
                                };
                                HashSet<String> valid = new HashSet<String>(
                                        Arrays.asList(expectedValues));
                                if (!valid.contains(val)) {
                                    fail("Got " + val + ", expected one of "
                                         + valid);
                                }
                            } else {
                                // For all other types...
                                assertEquals(expected, val);
                            }
                        }
                        checkSupportedCast(sourceType, targetType);
                    } catch (SQLException se) {
                        if (dataOffset != 0)
                        {
                            //System.out.print("\"Exception\"");
                        }
                        String sqlState = se.getSQLState();
                        if (!isSupportedCast(sourceType, targetType)) {
                            assertTrue(isCastException(se));
                        } else
                            throw se;
                    }
                    /*
                    if (dataOffset > 0)
                        if (targetType == SQLTypes.length -1)
                            System.out.println("},");
                        else 
                            System.out.print(",");
                     */

                }
        }

        commit();

    }

    public void testComparisons() throws SQLException {

        Statement scb = createStatement();

        // Comparison's using literals

        for (int type = 0; type < SQLUtilities.SQLTypes.length; type++) {
            try {
                int dataOffset = 1; // don't use null values
                String tableName = getTableName(type);

                String compareSQL = "SELECT distinct c FROM " + tableName
                        + " WHERE c = " + SQLData[type][dataOffset];

                ResultSet rs = scb.executeQuery(compareSQL);
                //JDBC.assertDrainResults(rs);
                // should return 1 row
                assertTrue(rs.next());
                rs.close();
            } catch (SQLException se) {
                // literal comparisons are ok for everything but Lob and long
                assertTrue(isLongType(type));
            }
        }

        // Try to compare each sourceType with the targetType
        for (int dataOffset = 0; dataOffset < SQLData[0].length; dataOffset++)
            for (int sourceType = 0; sourceType < SQLUtilities.SQLTypes.length; sourceType++) {
                String sourceTypeName = SQLUtilities.SQLTypes[sourceType];
                for (int targetType = 0; targetType < SQLUtilities.SQLTypes.length; targetType++) {
                    try {
                        String targetTableName = getTableName(targetType);

                        // For assignments Character types use strings that can
                        // be converted to the targetType.
                        String convertString = getCompatibleString(sourceType,
                                targetType, dataOffset);

                        // Make sure table has just compatible data
                        scb.executeUpdate("DELETE FROM " + targetTableName);
                        String insertValuesString = " VALUES CAST("
                                + convertString + " AS " + sourceTypeName + ")";

                        String insertSQL = "INSERT INTO " + targetTableName
                                + insertValuesString;

                        String compareSQL = "select c from " + targetTableName
                                + " WHERE c = CAST(" + convertString + " AS "
                                + sourceTypeName + ")";

                    
                        ResultSet rs = scb.executeQuery(compareSQL);
                        JDBC.assertDrainResults(rs);
                        
                        checkSupportedComparison(sourceType, targetType);

                    } catch (SQLException se) {
                        String sqlState = se.getSQLState();
                        assertTrue(!isSupportedComparison(sourceType, targetType)
                                && isNotComparableException(se)
                                || isCastException(se));
                       
                    }
                }
            }
        scb.close();
        commit();

    }

    /**
     * Verify that DERBY-887 is fixed.
     */
    public void test_derby887() throws Exception
    {
        goodStatement
            (
             "create table t_887 (a int)\n"
             );

        expectError
            (
             LANG_NOT_COMPARABLE_SQLSTATE,
             "select * from t_887 where a=0<3\n"
             );
    }

    /**
     * <p>
     * Verify that the legal boolean casts work as expected. This
     * test helps verify that DERBY-887 is fixed. Verifies the following:
     * </p>
     *
     * <ul>
     * <li>Implicit casts of BOOLEAN to legal types.</li>
     * <li>Implicit casts of legal types to BOOLEAN.</li>
     * <li>Explicit casts of BOOLEAN to legal types.</li>
     * </ul>
     *
     * <p>
     * The following can't be tested until the BOOLEAN type is re-enabled:
     * </p>
     *
     * <ul>
     * <li>Explicit casts of legal types to BOOLEAN.</li>
     * </ul>
     */
    public void test_legalBooleanCasts() throws Exception
    {
        //
        // This assertion will fail if a new Derby data type is added. To
        // silence this assertion, you must add the new data type
        // to LEGAL_BOOLEAN_CASTS or ILLEGAL_BOOLEAN_CASTS.
        //
        assertAllTypesCovered();

        int  legalTypeCount = LEGAL_BOOLEAN_CASTS.length;
        String  tableName = "t_legal_boolean_casts";
        // create a table whose columns are all the legal datatypes
        makeTableForCasts( tableName, LEGAL_BOOLEAN_CASTS );

        // now test the implicit casting of boolean to all of the legal
        // types by inserting a boolean value into all of the columns
        // of the table
        goodStatement
            (
             "insert into " + tableName + "\n" +
             "( " + makeColumnList( LEGAL_BOOLEAN_CASTS ) + " )\n" +
             "select " + makeRepeatedColumnList( "c.isIndex", LEGAL_BOOLEAN_CASTS.length ) + "\n" +
             "from\n" +
             "  sys.sysconglomerates c,\n" +
             "  sys.systables t\n" +
             "where t.tablename='SYSTABLES'\n" +
             "and t.tableid = c.tableid\n" +
             "and not c.isIndex\n"
             );
        // test that all of the inserted values are false
        assertBooleanResults
            (
             "select * from " + tableName + "\n",
             false,
             1
             );

        // now try implicitly casting the legal types to boolean by
        // trying to compare the values in the table to a boolean value.
        // we only expect this to succeed for short, indexable data types.
        // the long data types cannot be compared
        for ( int i = 0; i < legalTypeCount; i++ )
        {
            TypedColumn tc = LEGAL_BOOLEAN_CASTS[ i ];

            String queryText =
                "select count(*)\n" +
                "from\n" +
                "  sys.sysconglomerates c,\n" +
                "  sys.systables t,\n" +
                "  " + tableName + " tt\n" +
                "where t.tablename='SYSTABLES'\n" +
                "and t.tableid = c.tableid\n" +
                "and not c.isIndex\n" +
                "and tt." + tc.columnName + " = c.isIndex\n";

            if ( tc.comparable ) { assertScalarResult( queryText, 1 ); }
            else { expectError( LANG_NOT_COMPARABLE_SQLSTATE, queryText ); }
        }

        // now try explicitly casting a boolean value to all of the legal types
        assertBooleanResults
            (
             "select\n" +
             makeCastedColumnList( "c.isIndex", LEGAL_BOOLEAN_CASTS ) +
             "\nfrom\n" +
             "  sys.sysconglomerates c,\n" +
             "  sys.systables t\n" +
             "where t.tablename='SYSTABLES'\n" +
             "and t.tableid = c.tableid\n" +
             "and not c.isIndex\n",
             false,
             1
             );
    }
    private void makeTableForCasts( String tableName, TypedColumn[] columns )
        throws Exception
    {
        StringBuffer buffer = new StringBuffer();
        int  count = columns.length;

        buffer.append( "create table " + tableName + "\n(\n" );
        for ( int i = 0; i < count; i++ )
        {
            buffer.append( "\t" );
            if ( i > 0 ) { buffer.append( ", " ); }

            TypedColumn tc = columns[ i ];

            buffer.append( tc.columnName + "\t" + tc.typeName + "\n"  );
        }
        buffer.append( ")\n" );
        
        goodStatement( buffer.toString() );
    }
    // make a comma-separated list of column names
    private String makeColumnList( TypedColumn[] columns )
    {
        StringBuffer buffer = new StringBuffer();
        int  count = columns.length;

        for ( int i = 0; i < count; i++ )
        {
            if ( i > 0 ) { buffer.append( ", " ); }
            buffer.append( columns[ i ].columnName  );
        }

        return buffer.toString();
    }
    // make a comma-separated list of a column casted to various target types
    private String makeCastedColumnList( String columnName, TypedColumn[] targetTypes )
    {
        StringBuffer buffer = new StringBuffer();
        int  count = targetTypes.length;

        for ( int i = 0; i < count; i++ )
        {
            if ( i > 0 ) { buffer.append( ", " ); }
            buffer.append( "cast ( " + columnName + " as " + targetTypes[ i ].typeName + " )" );
        }

        return buffer.toString();
    }
    // make a comma-separated list of N copies of a column
    private String makeRepeatedColumnList( String columnName, int N )
    {
        StringBuffer buffer = new StringBuffer();

        for ( int i = 0; i < N; i++ )
        {
            if ( i > 0 ) { buffer.append( ", " ); }
            buffer.append( columnName  );
        }

        return buffer.toString();
    }
    // assert that all result columns have the given boolean value
    private void assertBooleanResults( String queryText, boolean expectedValue, int expectedRowCount )
        throws Exception
    {
        PreparedStatement ps = chattyPrepare( queryText );
        ResultSet rs = ps.executeQuery();
        int actualRowCount = 0;
        int columnCount = rs.getMetaData().getColumnCount();
        String expectedStringValue = Boolean.toString( expectedValue );

        while ( rs.next() )
        {
            actualRowCount++;

            for ( int i = 0; i < columnCount; i++ )
            {
                assertEquals( "Column " + i, expectedStringValue, rs.getString( i + 1 ).trim() );
            }
        }

        rs.close();
        ps.close();

        assertEquals( expectedRowCount, actualRowCount );
    }
    // assert a scalar result
    private void assertScalarResult( String queryText, int expectedValue ) throws Exception
    {
        PreparedStatement ps = chattyPrepare( queryText );
        ResultSet rs = ps.executeQuery();

        rs.next();
        assertEquals( expectedValue, rs.getInt( 1 ) );

        rs.close();
        ps.close();
    }
    // assert that we are testing the casting behavior of BOOLEANs to and from
    // all Derby data types
    private void assertAllTypesCovered() throws Exception
    {
        println( "Verify that we are testing the casting behavior of BOOLEAN to/from all Derby data types." );
        
        Connection conn = getConnection();
        DatabaseMetaData dbmd = conn.getMetaData();
        ResultSet rs = dbmd.getTypeInfo();
        int count = 0;

        int expectedDataTypeCount = LEGAL_BOOLEAN_CASTS.length + ILLEGAL_BOOLEAN_CASTS.length;
        // getTypeInfo() also returns a row for the generic OBJECT data type
        expectedDataTypeCount++;

        while ( rs.next() ) { count++; }

        assertEquals( "You must add your new data type to LEGAL_BOOLEAN_CASTS or ILLEGAL_BOOLEAN_CASTS",
                      expectedDataTypeCount,
                      count );
        
        rs.close();
    }
    
    /**
     * <p>
     * Verify that the illegal boolean casts work as expected. This
     * test helps verify that DERBY-887 is fixed. Verifies the
     * following:
     * </p>
     *
     * <ul>
     * <li>Implicit casts of BOOLEAN to illegal types.</li>
     * <li>Implicit casts of illegal types to BOOLEAN.</li>
     * <li>Explicit casts of BOOLEAN to illegal types.</li>
     * </ul>
     *
     * <p>
     * The following can't be tested until the BOOLEAN type is re-enabled:
     * </p>
     *
     * <ul>
     * <li>Explicit casts of illegal types to BOOLEAN.</li>
     * </ul>
     */
    public void test_illegalBooleanCasts() throws Exception
    {
        //
        // This assertion will fail if a new Derby data type is added. To
        // silence this assertion, you must add the new data type
        // to LEGAL_BOOLEAN_CASTS or ILLEGAL_BOOLEAN_CASTS.
        //
        assertAllTypesCovered();
        
        int  illegalTypeCount = ILLEGAL_BOOLEAN_CASTS.length;
        String  tableName = "t_illegal_boolean_casts";
        // create a table whose columns are all the illegal datatypes
        makeTableForCasts( tableName, ILLEGAL_BOOLEAN_CASTS );

        // use inserts to test implicit casts of boolean to the illegal types
        for ( int i = 0; i < illegalTypeCount; i++ )
        {
            TypedColumn tc = ILLEGAL_BOOLEAN_CASTS[ i ];
            expectError
                (
                 LANG_NOT_STORABLE_SQLSTATE,
                 "insert into " + tableName + "( " + tc.columnName + " ) select c.isIndex from sys.sysconglomerates c\n"
                 );
        }

        // test implicit casts of illegal types to boolean
        for ( int i = 0; i < illegalTypeCount; i++ )
        {
            TypedColumn tc = ILLEGAL_BOOLEAN_CASTS[ i ];
            expectError
                (
                 LANG_NOT_COMPARABLE_SQLSTATE,
                 "select * from " + tableName + " t, sys.sysconglomerates c where t." + tc.columnName + " = c.isIndex\n"
                 );
        }
        
        // test explicit casts of boolean to illegal types
        for ( int i = 0; i < illegalTypeCount; i++ )
        {
            TypedColumn[] castedColumnList = new TypedColumn[] { ILLEGAL_BOOLEAN_CASTS[ i ] };
            expectError
                (
                 ILLEGAL_CAST_EXCEPTION_SQLSTATE,
                 "select " + makeCastedColumnList( "c.isIndex", castedColumnList ) + " from sys.sysconglomerates c\n"
                 );
        }
    }

    /**
     * Test that a java.sql.DataTruncation warning is created when a cast
     * results in truncation. DERBY-129.
     */
    public void testDataTruncationWarning() throws SQLException {
        Statement s = createStatement();

        // Test truncation of character data
        checkDataTruncationResult(s,
            "values (cast('abc' as char(2)), cast('de'   as char(2)))," +
            "       (cast('fg'  as char(2)), cast('hi'   as char(2)))," +
            "       (cast('jkl' as char(2)), cast('mnop' as char(2)))");
        checkDataTruncationResult(s,
            "values (cast('abc' as varchar(2)), cast('de'   as varchar(2)))," +
            "       (cast('fg'  as varchar(2)), cast('hi'   as varchar(2)))," +
            "       (cast('jkl' as varchar(2)), cast('mnop' as varchar(2)))");
        checkDataTruncationResult(s,
            "values (cast('abc' as clob(2)), cast('de'   as clob(2)))," +
            "       (cast('fg'  as clob(2)), cast('hi'   as clob(2)))," +
            "       (cast('jkl' as clob(2)), cast('mnop' as clob(2)))");

        // Exact same test as above for binary data
        checkDataTruncationResult(s,
            "values (cast(x'abcdef' as char(2) for bit data),"+
            "        cast(x'abcd' as char(2) for bit data))," +
            "       (cast(x'abcd' as char(2) for bit data)," +
            "        cast(x'cdef' as char(2) for bit data))," +
            "       (cast(x'012345' as char(2) for bit data)," +
            "        cast(x'6789ABCD' as char(2) for bit data))");
        checkDataTruncationResult(s,
            "values (cast(x'abcdef' as varchar(2) for bit data),"+
            "        cast(x'abcd' as varchar(2) for bit data))," +
            "       (cast(x'abcd' as varchar(2) for bit data)," +
            "        cast(x'cdef' as varchar(2) for bit data))," +
            "       (cast(x'012345' as varchar(2) for bit data)," +
            "        cast(x'6789ABCD' as varchar(2) for bit data))");
        checkDataTruncationResult(s,
            "values" +
            "    (cast(x'abcdef' as blob(2)), cast(x'abcd' as blob(2))), " +
            "    (cast(x'abcd' as blob(2)),   cast(x'cdef' as blob(2))), " +
            "    (cast(x'012345' as blob(2)), cast(x'6789ABCD' as blob(2)))");

        // DataTruncation's javadoc says that getDataSize() and
        // getTransferSize() should return number of bytes. Derby uses
        // UTF-8. Test with some characters outside the US-ASCII range to
        // verify that the returned values are in bytes and not in chars.
        ResultSet rs = s.executeQuery(
                "values cast('abc\u00E6\u00F8\u00E5' as varchar(4))");
        assertTrue(rs.next());
        assertEquals("abc\u00E6", rs.getString(1));
        // The warning should say the string is truncated from 9 bytes to
        // 5 bytes, not from 6 characters to 4 characters.
        assertDataTruncation(rs.getWarnings(), -1, true, false, 9, 5);
        assertFalse(rs.next());
        rs.close();

        // Test that there's a warning on the statement if truncation happens
        // in an operation that doesn't return a ResultSet.
        setAutoCommit(false);
        s.execute("create table t1_d129 (x8 char(8) for bit data)");
        s.execute("create table t2_d129 (x4 char(4) for bit data)");
        s.execute("insert into t1_d129(x8) values x'0123456789ABCDEF'");
        assertNull(s.getWarnings());
        s.execute("insert into t2_d129(x4) " +
                  "select cast(x8 as char(4) for bit data) from t1_d129");
        assertDataTruncation(s.getWarnings(), -1, true, false, 8, 4);
        rollback();
    }

    /**
     * <p>
     * Check the results for the queries in testDataTruncation().
     * </p>
     *
     * <p>
     * The method expects a query that returns three rows with columns of a
     * character string or binary string data type, where some of the values
     * are cast to a narrower data type.
     * </p>
     *
     * <p>
     * Expect the following truncations to have taken place:
     * </p>
     *
     * <ol>
     * <li>Row 1, column 1: truncated from 3 to 2 bytes</li>
     * <li>Row 3, column 1: truncated from 3 to 2 bytes</li>
     * <li>Row 3, column 2: truncated from 4 to 2 bytes</li>
     * </ol>
     */
    private void checkDataTruncationResult(Statement s, String sql)
            throws SQLException {
        ResultSet rs = s.executeQuery(sql);

        // First row should have one warning (column 1)
        assertTrue(rs.next());
        SQLWarning w = rs.getWarnings();
        assertDataTruncation(w, -1, true, false, 3, 2);
        w = w.getNextWarning();
        assertNull(w);
        rs.clearWarnings(); // workaround for DERBY-5765

        // Second row should have no warnings
        assertTrue(rs.next());
        assertNull(rs.getWarnings());

        // Third row should have two warnings (column 1 and 2)
        assertTrue(rs.next());
        w = rs.getWarnings();
        assertDataTruncation(w, -1, true, false, 3, 2);
        // Client driver doesn't support nested warnings
        if (usingEmbedded()) {
            w = w.getNextWarning();
            assertDataTruncation(w, -1, true, false, 4, 2);
        }
        w = w.getNextWarning();
        assertNull(w);
        rs.clearWarnings(); // workaround for DERBY-5765

        // No more rows
        assertFalse(rs.next());
        rs.close();

        // There should be no warnings on the statement or the connection
        assertNull(s.getWarnings());
        assertNull(getConnection().getWarnings());
    }

    private void assertDataTruncation(
            SQLWarning w, int index, boolean read, boolean parameter,
            int dataSize, int transferSize) throws SQLException {
        assertNotNull("No warning", w);
        if (!(w instanceof DataTruncation)) {
            fail("Not a DataTruncation warning", w);
        }

        DataTruncation dt = (DataTruncation) w;
        assertEquals("Column index", index, dt.getIndex());
        assertEquals("Read", read, dt.getRead());
        assertEquals("Parameter", parameter, dt.getParameter());
        assertEquals("Data size", dataSize, dt.getDataSize());
        assertEquals("Transfer size", transferSize, dt.getTransferSize());
    }

    /**
     * DERBY-896: Verify that casts from DATE and TIME to TIMESTAMP work.
     */
    public void testDateTimeToTimestamp() throws SQLException {
        Statement s = createStatement();

        ResultSet rs = s.executeQuery(
                "values (cast (current date as timestamp), "
                      + "current date, "
                      + "cast (current time as timestamp), "
                      + "current time)");

        // Verify correct types of casts.
        ResultSetMetaData rsmd = rs.getMetaData();
        assertEquals(Types.TIMESTAMP, rsmd.getColumnType(1));
        assertEquals(Types.TIMESTAMP, rsmd.getColumnType(3));

        rs.next();

        // CAST (CURRENT DATE AS TIMESTAMP) should match midnight of
        // current date.
        assertEquals(rs.getString(2) + " 00:00:00.0", rs.getString(1));

        // CAST (CURRENT TIME AS TIMESTAMP) should match current time of
        // current date.
        assertEquals(rs.getString(2) + ' ' + rs.getString(4) + ".0",
                     rs.getString(3));

        rs.close();

        // Don't allow casts between DATE and TIME.
        assertCompileError(ILLEGAL_CAST_EXCEPTION_SQLSTATE,
                           "values cast(current time as date)");
        assertCompileError(ILLEGAL_CAST_EXCEPTION_SQLSTATE,
                           "values cast(current date as time)");

        s.execute("create table derby896(id int generated always as identity, "
                + "d date, t time, ts timestamp)");

        // Only explicit casts are allowed.
        assertCompileError(LANG_NOT_STORABLE_SQLSTATE,
                           "insert into derby896(ts) values current time");
        assertCompileError(LANG_NOT_STORABLE_SQLSTATE,
                           "insert into derby896(ts) values current date");
        s.execute("insert into derby896(d,t,ts) values "
            + "(current date, current time, cast(current date as timestamp)), "
            + "(current date, current time, cast(current time as timestamp))");

        // Verify that the correct values were inserted.
        rs = s.executeQuery("select d, t, ts from derby896 order by id");
        rs.next();
        assertEquals(rs.getString(1) + " 00:00:00.0", rs.getString(3));
        rs.next();
        assertEquals(rs.getString(1) + ' ' + rs.getString(2) + ".0",
                     rs.getString(3));
        rs.close();

        // Insert some more values that we can use in casts later.
        s.execute("insert into derby896(d, t) values "
                + "({d'1999-12-31'}, {t'23:59:59'}), "
                + "({d'2000-01-01'}, {t'00:00:00'}), "
                + "({d'1970-01-01'}, {t'00:00:01'}), "
                + "({d'1969-12-31'}, {t'12:00:00'})");

        // Verify correct casts from DATE to TIMESTAMP in SELECT list.
        rs = s.executeQuery("select d, cast(d as timestamp) from derby896");
        while (rs.next()) {
            assertEquals(rs.getString(1) + " 00:00:00.0", rs.getString(2));
        }
        rs.close();

        // Verify correct casts from TIME to TIMESTAMP in SELECT list.
        rs = s.executeQuery("select t, cast(t as timestamp), current date "
                            + "from derby896");
        while (rs.next()) {
            assertEquals(rs.getString(3) + ' ' + rs.getString(1) + ".0",
                         rs.getString(2));
        }
        rs.close();
    }

    protected void tearDown() throws SQLException, Exception {
        Statement scb = createStatement();

        for (int type = 0; type < SQLUtilities.SQLTypes.length; type++) {
            String typeName = SQLUtilities.SQLTypes[type];
            String tableName = getTableName(type);

            String dropSQL = "drop table " + tableName;

            scb.executeUpdate(dropSQL);
        }

        scb.close();
        commit();
        super.tearDown();
    }

    /**
     * Build a unique table name from the type
     * 
     * @param type
     *            table offset
     * @return Table name in format <TYPE>_TAB. Replaces ' ' _;
     */
    private static String getTableName(int type) {
        return getShortTypeName(type).replace(' ', '_') + "_TAB";

    }

    /**
     * Truncates (*) from typename
     * 
     * @param type -
     *            Type offset
     * 
     * @return short name of type (e.g DECIMAL instead of DECIMAL(10,5)
     */

    private static String getShortTypeName(int type) {
        String typeName = SQLUtilities.SQLTypes[type];
        String shortName = typeName;
        int parenIndex = typeName.indexOf('(');
        if (parenIndex >= 0) {
            shortName = typeName.substring(0, parenIndex);
            int endParenIndex = typeName.indexOf(')');
            shortName = shortName
                    + typeName.substring(endParenIndex + 1, typeName.length());
        }
        return shortName;

    }

    private static String getCompatibleString(int sourceType, int targetType,
            int dataOffset) {
        String convertString = null;
        // for string and binary types use the target data string
        // so that the cast will work
        if ((isCharacterType(sourceType) || isBinaryType(sourceType))
                && !isLob(sourceType))
            convertString = formatString(SQLData[targetType][dataOffset]);
        else
            convertString = SQLData[sourceType][dataOffset];

        return convertString;
    }

    private static boolean isSupportedCast(int sourceType, int targetType) {
        return T_146[sourceType][targetType];
    }

    private static boolean isSupportedAssignment(int sourceType, int targetType) {
        return T_147a[sourceType][targetType];
    }

    private static boolean isSupportedComparison(int sourceType, int targetType) {
        return T_147b[sourceType][targetType];
    }

    private static boolean isCastException(SQLException se) {
        return sqlStateMatches(se, ILLEGAL_CAST_EXCEPTION_SQLSTATE);
    }

    private static boolean isMethodNotFoundException(SQLException se) {
        return sqlStateMatches(se, METHOD_NOT_FOUND_SQLSTATE);
    }

    private static boolean sqlStateMatches(SQLException se, String expectedValue) {
        String sqlState = se.getSQLState();
        if ((sqlState != null) && (sqlState.equals(expectedValue)))
            return true;
        return false;
    }

    private static boolean isNotStorableException(SQLException se) {
        String sqlState = se.getSQLState();
        if ((sqlState != null) && (sqlState.equals(LANG_NOT_STORABLE_SQLSTATE)))
            return true;
        return false;

    }

    private static boolean isNotComparableException(SQLException se) {
        String sqlState = se.getSQLState();
        if ((sqlState != null)
                && (sqlState.equals(LANG_NOT_COMPARABLE_SQLSTATE)))
            return true;
        return false;
    }

    private static void checkSupportedCast(int sourceType, int targetType) {
        String description = " Cast from " + SQLUtilities.SQLTypes[sourceType] + " to "
                + SQLUtilities.SQLTypes[targetType];

        if (!isSupportedCast(sourceType, targetType))
            fail(description + "should not succeed");
    }

    private static void checkSupportedAssignment(int sourceType, int targetType) {
        String description = " Assignment from " + SQLUtilities.SQLTypes[sourceType]
                + " to " + SQLUtilities.SQLTypes[targetType];

        if (!isSupportedAssignment(sourceType, targetType))
            fail(description + "should not succeed");

    }

    private static void checkSupportedComparison(int sourceType, int targetType) {
        String description = " Comparison of " + SQLUtilities.SQLTypes[sourceType] + " to "
                + SQLUtilities.SQLTypes[targetType];

        if (!isSupportedComparison(sourceType, targetType))
            fail("FAIL: unsupported comparison:" + description);
    }

    private static boolean isLongType(int typeOffset) {
        return ((typeOffset == LONGVARCHAR_OFFSET)
                || (typeOffset == LONGVARCHAR_FOR_BIT_OFFSET)
                || (typeOffset == CLOB_OFFSET) || (typeOffset == BLOB_OFFSET));
    }

    private static boolean isCharacterType(int typeOffset) {
        return ((typeOffset == CHAR_OFFSET) || (typeOffset == VARCHAR_OFFSET)
                || (typeOffset == LONGVARCHAR_OFFSET) || (typeOffset == CLOB_OFFSET));
    }

    private static boolean isBinaryType(int typeOffset) {
        return ((typeOffset == CHAR_FOR_BIT_OFFSET)
                || (typeOffset == VARCHAR_FOR_BIT_OFFSET)
                || (typeOffset == LONGVARCHAR_FOR_BIT_OFFSET) || (typeOffset == BLOB_OFFSET));
    }

    private static boolean isTime(int typeOffset) {
        return (typeOffset == TIME_OFFSET);
    }

    private static boolean isTimestamp(int typeOffset) {
        return (typeOffset == TIMESTAMP_OFFSET);
    }

    private static boolean isLob(int typeOffset) {
        return ((typeOffset == CLOB_OFFSET) || (typeOffset == BLOB_OFFSET));

    }

    // Data is already a string (starts with X, or a character string,
    // just return, otherwise bracket with ''s
    private static String formatString(String str) {
        if ((str != null)
                && (str.startsWith("X") || str.startsWith("'") || (str == NULL_VALUE)))
            return str;
        else
            return "'" + str + "'";
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
     * Assert that the statement text, when compiled, raises an exception
     */
    private void    expectError( String sqlState, String query )
    {
        println( "\nExpecting " + sqlState + " when preparing:\n\t" + query );

        assertCompileError( sqlState, query );
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
     * Create a test suite with all the tests in this class. Although we're
     * testing embedded functionality, also run the test in client/server
     * mode to ensure that warnings and errors travel across the wire.
     */
    public static Test suite() {
        return TestConfiguration.defaultSuite(CastingTest.class);
    }
}
