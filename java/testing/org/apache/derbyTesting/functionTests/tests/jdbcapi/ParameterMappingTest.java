/**
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

package org.apache.derbyTesting.functionTests.tests.jdbcapi;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.derbyTesting.junit.BigDecimalHandler;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.TestConfiguration;
import org.apache.derbyTesting.junit.Utilities;

import org.apache.derby.iapi.types.HarmonySerialBlob;
import org.apache.derby.iapi.types.HarmonySerialClob;

/**
 * 
 */
public class ParameterMappingTest extends BaseJDBCTestCase {
    private static boolean HAVE_BIG_DECIMAL;

    private static final String BAD_TYPE = "42962";
    private static final String UTF8 = "UTF-8";

    static {
        if (JDBC.vmSupportsJSR169())
            HAVE_BIG_DECIMAL = false;
        else
            HAVE_BIG_DECIMAL = true;
    }

    private static int[] jdbcTypes = { Types.TINYINT, Types.SMALLINT,
            Types.INTEGER, Types.BIGINT, Types.REAL, Types.FLOAT, Types.DOUBLE,
            Types.DECIMAL, Types.NUMERIC, Types.BIT, Types.BOOLEAN,
            Types.CHAR, Types.VARCHAR, Types.LONGVARCHAR, Types.NULL, // Types.BINARY,
            Types.VARBINARY, Types.NULL, // Types.LONGVARBINARY,
            Types.DATE, Types.TIME, Types.TIMESTAMP, Types.CLOB, Types.BLOB, };

    private static String[] SQLTypes = { null, "SMALLINT", "INTEGER", "BIGINT",
            "REAL", "FLOAT", "DOUBLE", "DECIMAL(10,5)", null, null, "BOOLEAN",
            "CHAR(60)", "VARCHAR(60)", "LONG VARCHAR", "CHAR(60) FOR BIT DATA",
            "VARCHAR(60) FOR BIT DATA", "LONG VARCHAR FOR BIT DATA", "DATE",
            "TIME", "TIMESTAMP", "CLOB(1k)", "BLOB(1k)",

    };

    private static String[] validString = {null,"98","98","98",
           "98","98", "98","98",null,null,"TRUE",
           "98","98","98","0x4",
           "0x4","0x4", "2004-02-14",
           "00:00:00","2004-02-14 00:00:00","98","0x4"};
    
    private static Class[] B3_GET_OBJECT;

    static {
        if (HAVE_BIG_DECIMAL) {
            B3_GET_OBJECT = new Class[] { java.lang.Integer.class, // Types.TINYINT,
                    java.lang.Integer.class, // Types.SMALLINT,
                    java.lang.Integer.class, // Types.INTEGER,
                    java.lang.Long.class, // Types.BIGINT,
                    java.lang.Float.class, // Types.REAL,
                    java.lang.Double.class, // Types.FLOAT,
                    java.lang.Double.class, // Types.DOUBLE,
                    java.math.BigDecimal.class, // Types.DECIMAL,
                    java.math.BigDecimal.class, // Types.NUMERIC,
                    java.lang.Boolean.class, // Types.BIT,
                    java.lang.Boolean.class, // Types.BOOLEAN
                    java.lang.String.class, // Types.CHAR,
                    java.lang.String.class, // Types.VARCHAR,
                    java.lang.String.class, // Types.LONGVARCHAR,
                    byte[].class, // Types.NULL, //Types.BINARY,
                    byte[].class, // Types.VARBINARY,
                    byte[].class, // Types.LONGVARBINARY,
                    java.sql.Date.class, // Types.DATE,
                    java.sql.Time.class, // Types.TIME,
                    java.sql.Timestamp.class, // Types.TIMESTAMP,
                    java.sql.Clob.class, // Types.CLOB,
                    java.sql.Blob.class, // Types.BLOB,
            };
        } else {
            B3_GET_OBJECT = new Class[] { java.lang.Integer.class, // Types.TINYINT,
                    java.lang.Integer.class, // Types.SMALLINT,
                    java.lang.Integer.class, // Types.INTEGER,
                    java.lang.Long.class, // Types.BIGINT,
                    java.lang.Float.class, // Types.REAL,
                    java.lang.Double.class, // Types.FLOAT,
                    java.lang.Double.class, // Types.DOUBLE,
                    java.lang.String.class, // Types.DECIMAL,
                    java.lang.String.class, // Types.NUMERIC,
                    java.lang.Boolean.class, // Types.BIT,
                    java.lang.Boolean.class, // Types.BOOLEAN
                    java.lang.String.class, // Types.CHAR,
                    java.lang.String.class, // Types.VARCHAR,
                    java.lang.String.class, // Types.LONGVARCHAR,
                    byte[].class, // Types.NULL, //Types.BINARY,
                    byte[].class, // Types.VARBINARY,
                    byte[].class, // Types.LONGVARBINARY,
                    java.sql.Date.class, // Types.DATE,
                    java.sql.Time.class, // Types.TIME,
                    java.sql.Timestamp.class, // Types.TIMESTAMP,
                    java.sql.Clob.class, // Types.CLOB,
                    java.sql.Blob.class, // Types.BLOB,
            };
        }
    }

    private static final boolean _ = false;

    private static final boolean X = true;

        /**
                JDBC 3.0 spec Table B6 - Use of ResultSet getter Methods to Retrieve JDBC Data Types
        */
        public static final boolean[][] B6 = {

// Types.             T  S  I  B  R  F  D  D  N  B  B  C  V  L  B  V  L  D  T  T  C  B
//                    I  M  N  I  E  L  O  E  U  I  O  H  A  O  I  A  O  A  I  I  L  L
//                    N  A  T  G  A  O  U  C  M  T  O  A  R  N  N  R  N  T  M  M  O  O
//                    Y  L  E  I  L  A  B  I  E     L  R  C  G  A  B  G  E  E  E  B  B
//                    I  L  G  N     T  L  M  R     E     H  V  R  I  V        S
//                    N  I  E  T        E  A  I     A     A  A  Y  N  A        T
//                    T  N  R              L  C     N     R  R     A  R        A
//                    T                                      C     R  B        M
//                                                           H     B  I        P
//                                                           A     I  N
//                                                           R     N  

/* 0 getByte*/          { X, X, X, X, X, X, X, X, X, X, X, X, X, X, _, _, _, _, _, _, _, _},
/* 1 getShort*/         { X, X, X, X, X, X, X, X, X, X, X, X, X, X, _, _, _, _, _, _, _, _},
/* 2 getInt*/           { X, X, X, X, X, X, X, X, X, X, X, X, X, X, _, _, _, _, _, _, _, _},
/* 3 getLong*/          { X, X, X, X, X, X, X, X, X, X, X, X, X, X, _, _, _, _, _, _, _, _},
/* 4 getFloat*/         { X, X, X, X, X, X, X, X, X, X, X, X, X, X, _, _, _, _, _, _, _, _},
/* 5 getDouble*/        { X, X, X, X, X, X, X, X, X, X, X, X, X, X, _, _, _, _, _, _, _, _},
/* 6 getBigDecimal*/    { X, X, X, X, X, X, X, X, X, X, X, X, X, X, _, _, _, _, _, _, _, _},
/* 7 getBoolean*/       { X, X, X, X, X, X, X, X, X, X, X, X, X, X, _, _, _, _, _, _, _, _},
/* 8 getString*/        { X, X, X, X, X, X, X, X, X, X, X, X, X, X, X, X, X, X, X, X, _, _},
/* 9 getBytes*/         { _, _, _, _, _, _, _, _, _, _, _, _, _, _, X, X, X, _, _, _, _, _},
/*10 getDate*/          { _, _, _, _, _, _, _, _, _, _, _, X, X, X, _, _, _, X, _, X, _, _},
/*11 getTime*/          { _, _, _, _, _, _, _, _, _, _, _, X, X, X, _, _, _, _, X, X, _, _},
/*12 getTimestamp*/     { _, _, _, _, _, _, _, _, _, _, _, X, X, X, _, _, _, X, X, X, _, _},
/*13 getAsciiStream*/   { _, _, _, _, _, _, _, _, _, _, _, X, X, X, X, X, X, _, _, _, _, _},
/*14 getBinaryStream*/  { _, _, _, _, _, _, _, _, _, _, _, _, _, _, X, X, X, _, _, _, _, _},
/*15 getCharStream*/    { _, _, _, _, _, _, _, _, _, _, _, X, X, X, X, X, X, _, _, _, _, _},
/*16 getClob */         { _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, X, _},
/*17 getBlob */         { _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, X},
                 
/*18 getUnicodeStream */{ _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _},
        };


        /**
                JDBC 3.0 Section 13.2.2.1 specifies that table B-2 is used to specify type mappings
                from the Java types (e.g. int as setInt) to the JDBC SQL Type (Types.INT).

                This table does not include stream methods and does not include conversions
                specified elsewhere in the text, Namely

                Section 16.3.2
                        setBinaryStream may be used to set a BLOB
                        setAsciiStream and setCharacterStream may be used to set a CLOB

                Thus this B2_MOD table is laid out like the B6 table and makes
                the assumptions that

                - Any Java numeric type can be used to set any SQL numeric type
                - Any Java numeric type can be used to set any SQL CHAR type
                - Numeric and date/time java types can be converted to SQL Char values.

                
        */

        // Types.             T  S  I  B  R  F  D  D  N  B  B  C  V  L  B  V  L  D  T  T  C  B
        //                    I  M  N  I  E  L  O  E  U  I  O  H  A  O  I  A  O  A  I  I  L  L
        //                    N  A  T  G  A  O  U  C  M  T  O  A  R  N  N  R  N  T  M  M  O  O
        //                    Y  L  E  I  L  A  B  I  E     L  R  C  G  A  B  G  E  E  E  B  B
        //                    I  L  G  N     T  L  M  R     E     H  V  R  I  V        S
        //                    N  I  E  T        E  A  I     A     A  A  Y  N  A        T
        //                    T  N  R              L  C     N     R  R     A  R        A
        //                       T                                   C     R  B        M
        //                                                           H     B  I        P
        //                                                           A     I  N
        //                                                           R     N  

        public static boolean[][] B2_MOD = {
/* 0 setByte*/          { X, X, X, X, X, X, X, X, X, X, X, X, X, X, _, _, _, _, _, _, _, _},
/* 1 setShort*/         { X, X, X, X, X, X, X, X, X, X, X, X, X, X, _, _, _, _, _, _, _, _},
/* 2 setInt*/           { X, X, X, X, X, X, X, X, X, X, X, X, X, X, _, _, _, _, _, _, _, _},
/* 3 setLong*/          { X, X, X, X, X, X, X, X, X, X, X, X, X, X, _, _, _, _, _, _, _, _},
/* 4 setFloat*/         { X, X, X, X, X, X, X, X, X, X, X, X, X, X, _, _, _, _, _, _, _, _},
/* 5 setDouble*/        { X, X, X, X, X, X, X, X, X, X, X, X, X, X, _, _, _, _, _, _, _, _},
/* 6 setBigDecimal*/    { X, X, X, X, X, X, X, X, X, X, X, X, X, X, _, _, _, _, _, _, _, _},
/* 7 setBoolean*/       { X, X, X, X, X, X, X, X, X, X, X, X, X, X, _, _, _, _, _, _, _, _},
/* 8 setString*/        { X, X, X, X, X, X, X, X, X, X, X, X, X, X, _, _, _, X, X, X, _, _},
/* 9 setBytes*/         { _, _, _, _, _, _, _, _, _, _, _, _, _, _, X, X, X, _, _, _, _, _},
/*10 setDate*/          { _, _, _, _, _, _, _, _, _, _, _, X, X, X, _, _, _, X, _, X, _, _},
/*11 setTime*/          { _, _, _, _, _, _, _, _, _, _, _, X, X, X, _, _, _, _, X, _, _, _},
/*12 setTimestamp*/     { _, _, _, _, _, _, _, _, _, _, _, X, X, X, _, _, _, X, X, X, _, _},
/*13 setAsciiStream*/   { _, _, _, _, _, _, _, _, _, _, _, X, X, X, _, _, _, _, _, _, X, _},
/*14 setBinaryStream*/  { _, _, _, _, _, _, _, _, _, _, _, _, _, _, X, X, X, _, _, _, _, X},
/*15 setCharStream*/    { _, _, _, _, _, _, _, _, _, _, _, X, X, X, _, _, _, _, _, _, X, _},
/*16 setClob */         { _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, X, _},
/*17 setBlob */         { _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, X},
                 
/*18 setUnicodeStream */{ _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _},
        };

        /** Table B5 conversion of Objects using setObject*/

// Types.             T  S  I  B  R  F  D  D  N  B  B  C  V  L  B  V  L  D  T  T  C  B
//                    I  M  N  I  E  L  O  E  U  I  O  H  A  O  I  A  O  A  I  I  L  L
//                    N  A  T  G  A  O  U  C  M  T  O  A  R  N  N  R  N  T  M  M  O  O
//                    Y  L  E  I  L  A  B  I  E     L  R  C  G  A  B  G  E  E  E  B  B
//                    I  L  G  N     T  L  M  R     E     H  V  R  I  V        S
//                    N  I  E  T        E  A  I     A     A  A  Y  N  A        T
//                    T  N  R              L  C     N     R  R     A  R        A
//                    T                                      C     R  B        M
//                                                           H     B  I        P
//                                                           A     I  N
//                                                           R     N  
        public static boolean[][] B5 = {
/* 0 String */          { X, X, X, X, X, X, X, X, X, X, X, X, X, _, _, _, X, X, X, X, _, _},
/* 1 BigDecimal */      { X, X, X, X, X, X, X, X, X, X, X, X, X, X, _, _, _, _, _, _, _, _},
/* 2 Boolean */         { X, X, X, X, X, X, X, X, X, X, X, X, X, X, _, _, _, _, _, _, _, _},
/* 3 Integer */         { X, X, X, X, X, X, X, X, X, X, X, X, X, X, _, _, _, _, _, _, _, _},
/* 4 Long */            { X, X, X, X, X, X, X, X, X, X, X, X, X, X, _, _, _, _, _, _, _, _},
/* 5 Float */           { X, X, X, X, X, X, X, X, X, X, X, X, X, X, _, _, _, _, _, _, _, _},
/* 6 Double */          { X, X, X, X, X, X, X, X, X, X, X, X, X, X, _, _, _, _, _, _, _, _},
/* 7 byte[] */          { _, _, _, _, _, _, _, _, _, _, _, _, _, _, X, X, X, _, _, _, _, _},
/* 8 Date */            { _, _, _, _, _, _, _, _, _, _, _, X, X, X, _, _, _, X, _, X, _, _},
/* 9 Time */            { _, _, _, _, _, _, _, _, _, _, _, X, X, X, _, _, _, _, X, _, _, _},
/*10 Timestamp */       { _, _, _, _, _, _, _, _, _, _, _, X, X, X, _, _, _, X, X, X, _, _},
/*11 Blob   */          { _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, X},
/*12 Clob */            { _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, X, _},

//Byte and Short were added to this table in JDBC 4.0. (See DERBY-1500.)

/*13 Byte */            { X, X, X, X, X, X, X, X, X, X, X, X, X, X, _, _, _, _, _, _, _, _},
/*14 Short */           { X, X, X, X, X, X, X, X, X, X, X, X, X, X, _, _, _, _, _, _, _, _},
        };

     
        public static final boolean[][]  allowRegisterOut = {
     

    //          Types.                T  S  I  B  R  F  D  D  N  B  B  C  V  L  B  V  L  D  T  T  C  B
//                                    I  M  N  I  E  L  O  E  U  I  O  H  A  O  I  A  O  A  I  I  L  L
//                                    N  A  T  G  A  O  U  C  M  T  O  A  R  N  N  R  N  T  M  M  O  O
//                                    Y  L  E  I  L  A  B  I  E     L  R  C  G  A  B  G  E  E  E  B  B
//                                    I  L  G  N     T  L  M  R     E     H  V  R  I  V        S
//                                    N  I  E  T        E  A  I     A     A  A  Y  N  A        T
//                                    T  N  R              L  C     N     R  R     A  R        A
//                                       T                                   C     R  B        M
//                                                                           H     B  I        P
//                                                                           A     I  N
//    param sqlType                                                          R     N  
            /* 0 null    */         { _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _},
            /* 1 SMALLINT*/         { X, X, X, X, X, X, X, X, X, X, X, X, X, X, _, _, _, _, _, _, _, _},
            /* 2 INTEGER*/          { X, X, X, X, X, X, X, X, X, X, X, X, X, X, _, _, _, _, _, _, _, _},
            /* 3 BIGINT */          { X, X, X, X, X, X, X, X, X, X, X, X, X, X, _, _, _, _, _, _, _, _},
            /* 4 REAL    */         { X, X, X, X, X, X, X, X, X, X, X, X, X, X, _, _, _, _, _, _, _, _},
            /* 5 FLOAT     */       { X, X, X, X, X, X, X, X, X, X, X, X, X, X, _, _, _, _, _, _, _, _},
            /* 6 DOUBLE    */       { X, X, X, X, X, X, X, X, X, X, X, X, X, X, _, _, _, _, _, _, _, _},
            /* 7 DECIMAL*/          { X, X, X, X, X, X, X, X, X, X, X, X, X, X, _, _, _, _, _, _, _, _},
            /* 8 null     */        { X, X, X, X, X, X, X, X, X, X, X, X, X, X, _, _, _, X, X, X, _, _},
            /* 9 null*/             { _, _, _, _, _, _, _, _, _, _, _, _, _, _, X, X, X, _, _, _, _, _},
            /*10 BOOLEAN   */       { X, X, X, X, X, X, X, X, X, X, X, X, X, X, _, _, _, _, _, _, _, _},
            /*11 CHAR(60) */        { _, _, _, _, _, _, _, _, _, _, _, X, X, X, _, _, _, _, X, _, _, _},
            /*12 VARCHAR(60) */     { _, _, _, _, _, _, _, _, _, _, _, X, X, X, _, _, _, X, X, X, _, _},
            /*13 LONG VARCHAR */    { _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _},
            /*14 CHAR FOR BIT   */  { _, _, _, _, _, _, _, _, _, _, _, _, _, _, X, X, X, _, _, _, _, _},
            /*15 VARCHAR FOR BIT*/  { _, _, _, _, _, _, _, _, _, _, _, _, _, _, X, X, X, _, _, _, _, _},
            /*16 LONGVARCHAR FOR B*/{ _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _},
            /*17 DATE */            { _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, X, _, _, _, _},
            /*18 TIME */            { _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, X, _, _, _},
            /*19 TIMESTAMP */       { _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, X, _, _},
            /*20 CLOB         */    { _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _},
            /*21 BLOB         */    { _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _},
                    };

        
            
        
    /**
     * @param arg0
     */
    public ParameterMappingTest(String arg0) {
        super(arg0);
    }

    /*
     * (non-Javadoc)
     * 
     * @see junit.framework.TestCase#setUp()
     */
    protected void setUp() throws Exception {
        Connection conn = getConnection();

        conn.setAutoCommit(false);

        // create simple a table with BLOB and CLOB thta
        // can be used to for setBlob/setClob testing.
        Statement scb = conn.createStatement();

        scb.execute("CREATE TABLE PM.LOB_GET(ID INT, B BLOB, C CLOB)");
        PreparedStatement pscb = conn
                .prepareStatement("INSERT INTO PM.LOB_GET VALUES (?, ?, ?)");
        pscb.setInt(1, 0);
        pscb.setNull(2, Types.BLOB);
        pscb.setNull(3, Types.CLOB);
        pscb.executeUpdate();

        pscb.setInt(1, 1);
        {
            byte[] data = new byte[6];
            data[0] = (byte) 0x4;
            data[1] = (byte) 0x3;
            data[2] = (byte) 0x72;
            data[3] = (byte) 0x43;
            data[4] = (byte) 0x00;
            data[5] = (byte) 0x37;

            pscb.setBinaryStream(2, new java.io.ByteArrayInputStream(data), 6);
        }
        pscb.setCharacterStream(3, new java.io.StringReader("72"), 2);
        pscb.executeUpdate();
        scb.close();
        pscb.close();
        conn.commit();

    }
    /**
     * Test setBigDecimal does not lose fractional digits
     * @throws Exception
     */
    public void testDerby2073() throws Exception
    {
        // Cannot use setBigDecimal with J2ME
        if (!JDBC.vmSupportsJDBC3())
            return;
        
        
        Statement s = createStatement();
        s.executeUpdate("CREATE TABLE DERBY_2073_TAB (dc DECIMAL(10,2), db double, r real, i int)");
        PreparedStatement ps = prepareStatement("INSERT INTO DERBY_2073_TAB VALUES(?,?,?,?)");
        BigDecimal value = new BigDecimal("123.45");
        ps.setBigDecimal(1, value);
        ps.setBigDecimal(2, value);
        ps.setBigDecimal(3, value);
        ps.setBigDecimal(4, value);
        ps.executeUpdate();
        // Test with null values as the change sets precision/scale for null values differently
        ps.setBigDecimal(1, null);
        ps.setBigDecimal(2, null);
        ps.setBigDecimal(3, null);
        ps.setBigDecimal(4, null);
        ps.executeUpdate();

        // Can't use negative scale on jdk1.4.2
        if (JDBC.vmSupportsJDBC4())
        {
            // Test with negative scale.
            value = new BigDecimal(new BigInteger("2"), -3);
        }
        else
        {
            value = new BigDecimal("2000");
        }
        ps.setBigDecimal(1,value);
        ps.setBigDecimal(2,value);
        ps.setBigDecimal(3,value);
        ps.setBigDecimal(4,value);
        ps.executeUpdate();        
        
        value = new BigDecimal("123.45");
        // Test with setObject and scale of 2
        ps.setObject(1, value,java.sql.Types.DECIMAL,2);
        ps.setObject(2, value,java.sql.Types.DECIMAL,2);
        ps.setObject(3, value,java.sql.Types.DECIMAL,2);
        ps.setObject(4, value,java.sql.Types.DECIMAL,2);
        ps.executeUpdate();
        
        // Test with setObject and scale of 0
        ps.setObject(1, value,java.sql.Types.DECIMAL,0);
        ps.setObject(2, value,java.sql.Types.DECIMAL,0);
        ps.setObject(3, value,java.sql.Types.DECIMAL,0);
        ps.setObject(4, value,java.sql.Types.DECIMAL,0);
        ps.executeUpdate();
        
        
        // Test with setObject and type with no scale.
        // should default to scale 0
        ps.setObject(1, value,java.sql.Types.DECIMAL);
        ps.setObject(2, value,java.sql.Types.DECIMAL);
        ps.setObject(3, value,java.sql.Types.DECIMAL);
        ps.setObject(4, value,java.sql.Types.DECIMAL);
        ps.executeUpdate();
        
        // Test with setObject and no type and no scale.
        // Keeps the fractional digits.
        ps.setObject(1, value);
        ps.setObject(2, value);
        ps.setObject(3, value);
        ps.setObject(4, value);
        ps.executeUpdate();
        
        // Can't use negative scale on jdk1.4.2
        if (JDBC.vmSupportsJDBC4())
        {
            // Test with setObject and negative scale.
            value = new BigDecimal(new BigInteger("2"), -3);
        } else
        {
            value = new BigDecimal("2000");
        }
        ps.setObject(1,value);
        ps.setObject(2,value);
        ps.setObject(3,value);
        ps.setObject(4,value);
        ps.executeUpdate();
        ResultSet rs = s.executeQuery("SELECT * FROM DERBY_2073_TAB");
        String [][] expectedResults = new String [][]
               {{"123.45","123.45","123.45","123"},
                {null,null,null,null},
                {"2000.00","2000.0","2000.0","2000"},
                {"123.45","123.45","123.45","123"},
                {"123.00","123.0","123.0","123"},
                {"123.00","123.0","123.0","123"},
                {"123.45","123.45","123.45","123"},
                {"2000.00","2000.0","2000.0","2000"}};
                                      
                                                    
       // Cannot run for embedded for now because embedded
       // does not adjust the scale if the  column is 
        // not BigDecimal (e.g. double or real or varchar.) (DERBY-3128)
        if (usingDerbyNetClient())
            JDBC.assertFullResultSet(rs, expectedResults);
           
        
        s.executeUpdate("DROP TABLE DERBY_2073_TAB");
        
    }
    
    public void testParameterMapping() throws Exception {
        Connection conn = getConnection();

        for (int type = 0; type < SQLTypes.length; type++) {

            String sqlType = SQLTypes[type];

            if (sqlType == null || jdbcTypes[type] == Types.NULL) {
                continue;
            }

            Statement s = conn.createStatement();
            try {
                s.execute("DROP TABLE PM.TYPE_AS");
            } catch (SQLException seq) {
            }
            s.execute("CREATE TABLE PM.TYPE_AS(VAL " + SQLTypes[type] + ")");

            PreparedStatement psi = conn
                    .prepareStatement("INSERT INTO PM.TYPE_AS(VAL) VALUES(?)");
            psi.setNull(1, jdbcTypes[type]);
            psi.executeUpdate();

            PreparedStatement psq = conn
                    .prepareStatement("SELECT VAL FROM PM.TYPE_AS");
            ResultSet rs = psq.executeQuery();
            ResultSetMetaData rsmd = rs.getMetaData();
            assertEquivalentDataType(jdbcTypes[type], rsmd.getColumnType(1));
            rs.close();
            // For this data type
            // Test inserting a NULL value and then performing all the getXXX()
            // calls on it.

            // System.out.println(" NULL VALUE");
            getXXX(psq, type, true);

            s.execute("DELETE FROM PM.TYPE_AS");

            // For this data type
            // Test inserting a valid value and then performing all the getXXX()
            // calls on it.
            if (setValidValue(psi, 1, jdbcTypes[type])) {
                psi.executeUpdate();
                getXXX(psq, type, false);
            }
            setXXX(s, psi, psq, type);

            psi.close();
            psq.close();
            s.execute("DROP TABLE PM.TYPE_AS");
            conn.commit();
            //          NOW PROCEDURE PARAMETERS
            try {
                s.execute("DROP PROCEDURE PMP.TYPE_AS");
            }catch (SQLException seq) {
            }
            String procSQL;
            procSQL = "CREATE PROCEDURE PMP.TYPE_AS(" +
                "IN P1 " + SQLTypes[type] + 
                ", INOUT P2 " + SQLTypes[type] +
                ", OUT P3 " + SQLTypes[type] +
                ") LANGUAGE JAVA PARAMETER STYLE JAVA NO SQL " +
                " EXTERNAL NAME 'org.apache.derbyTesting.functionTests.tests.jdbcapi.ParameterMappingTest.pmap'";
                        
            try {
                if (!HAVE_BIG_DECIMAL && SQLTypes[type].equals("DECIMAL(10,5)"))
                    continue;
                //System.out.println(procSQL);
                s.execute(procSQL);
            } catch (SQLException sqle) {
                // may get error that column is not allowed
                if (BAD_TYPE.equals(sqle.getSQLState()))
                    continue;
                else
                    fail(sqle.getSQLState() + ":" + sqle.getMessage());
                continue;
            }
            
            // For each JDBC type try to register the out parameters with that type.
                for (int opt = 0; opt < jdbcTypes.length; opt++) {
                    int jopt = jdbcTypes[opt];
                    if (jopt == Types.NULL)
                        continue;

                    CallableStatement csp = conn.prepareCall("CALL PMP.TYPE_AS(?, ?, ?)");
                    
                    boolean bothRegistered = true;
                    //System.out.print("INOUT " + sqlType + " registerOutParameter(" + JDBC.sqlNameFromJdbc(jopt) + ") ");
                    try {
                        csp.registerOutParameter(2, jopt);
                    } catch (SQLException sqle) {
                        assertFalse("INOUT " + sqlType + " registerOutParameter(" + JDBC.sqlNameFromJdbc(jopt) + 
                                ") failed",allowRegisterOut[type][opt]);
                        if (!"XCL25".equals(sqle.getSQLState()))
                            fail("-- " + sqle.getSQLState());
                        bothRegistered = false;
                      }       
                    //System.out.print("OUT " + sqlType + " registerOutParameter(" + TestUtil.getNameFromJdbcType(jopt) + ") ");
                    try {
                        csp.registerOutParameter(3, jopt);
                    } catch (SQLException sqle) {
                        if (!"XCL25".equals(sqle.getSQLState()))
                            fail("-- " + sqle.getSQLState());
                        assertFalse("OUT " + sqlType + " registerOutParameter(" + JDBC.sqlNameFromJdbc(jopt) + 
                                "failed",allowRegisterOut[type][opt]);
                        bothRegistered = false;
                    }

                    if (bothRegistered) {
                        
                        try {

                            // set the IN value with an accepted value according to its type
                            // set the INOUT value with an accepted value according to its registered type
                            if (setValidValue(csp, 1, jdbcTypes[type]) && setValidValue(csp, 2, jopt)) {

                                csp.execute();
                                
                               // now get the INOUT, OUT parameters according to their registered type.
                                getOutValue(csp, 2, jopt,type); 
                                
                                getOutValue(csp, 3, jopt,type);
                        }

                        } catch (SQLException sqle) {
                            boolean expectedConversionError = ("22018".equals(sqle.getSQLState())|| 
                                                               "22007".equals(sqle.getSQLState()) ||
                                                               "22005".equals(sqle.getSQLState()));
                            if ( !expectedConversionError)
                            {
                                printStackTrace( sqle );
                            }
                            assertTrue("FAIL: Unexpected exception" + sqle.getSQLState() + ":" + sqle.getMessage(),
                                    expectedConversionError);
                        }
                    }   

                    csp.close();
                    
                 }      

                
                s.execute("DROP PROCEDURE PMP.TYPE_AS");
                s.close();
                conn.commit();
         }      
    }

    /**
     * Verify correct mapping of clobs.
     */
    public void testClobMapping() throws Exception
    {
        Connection conn = getConnection();
        PreparedStatement ps;
        CallableStatement cs;
        Clob outVal;

        //
        // Clob input parameter
        //
        ps = chattyPrepare
            (
             conn,
             "create procedure clobIn\n" +
             "( in c clob, out result varchar( 100 ) )\n" +
             "language java\n" +
             "parameter style java\n" +
             "no sql\n" +
             "external name '" + getClass().getName() + ".clobIn'\n"
             );
        ps.execute();
        ps.close();

        cs = chattyPrepareCall( conn, "call clobIn( cast( 'def' as clob ), ? )" );
        cs.registerOutParameter( 1, Types.VARCHAR );
        cs.execute();
        assertEquals( "def", cs.getString( 1 ) );
        cs.close();

        cs = chattyPrepareCall( conn, "call clobIn( ?, ? )" );
        cs.setClob( 1, new HarmonySerialClob( "ghi" ) );
        cs.registerOutParameter( 2, Types.VARCHAR );
        cs.execute();
        assertEquals( "ghi", cs.getString( 2 ) );
        cs.close();

        //
        // Clob output parameter
        //
        ps = chattyPrepare
            (
             conn,
             "create procedure clobOut\n" +
             "( out c clob )\n" +
             "language java\n" +
             "parameter style java\n" +
             "no sql\n" +
             "external name '" + getClass().getName() + ".clobOut'\n"
             );
        ps.execute();
        ps.close();
        
        cs = chattyPrepareCall( conn, "call clobOut( ? )" );
        cs.registerOutParameter( 1, Types.CLOB );
        cs.execute();
        outVal = cs.getClob( 1 );
        assertEquals( "abc", outVal.getSubString( 1L, (int) outVal.length() ) );
        cs.close();

        //
        // Clob inout parameter
        //
        ps = chattyPrepare
            (
             conn,
             "create procedure clobInOut\n" +
             "( inout c clob )\n" +
             "language java\n" +
             "parameter style java\n" +
             "no sql\n" +
             "external name '" + getClass().getName() + ".clobInOut'\n"
             );
        ps.execute();
        ps.close();
        
        cs = chattyPrepareCall( conn, "call clobInOut( ? )" );
        cs.setClob( 1, new HarmonySerialClob( "ghi" ) );
        cs.registerOutParameter( 1, Types.CLOB );
        cs.execute();
        outVal = cs.getClob( 1 );
        assertEquals( "ihg", outVal.getSubString( 1L, (int) outVal.length() ) );

        Clob inValue = makeBigClob();
        cs.setClob( 1, inValue );
        cs.execute();
        Clob outValue = cs.getClob( 1 );
        compareClobs( inValue, outValue );
        
        cs.close();
    }
    private Clob makeBigClob() throws Exception
    {
        char[] template = "abcdefghijklmnopqrstuvwxyz".toCharArray();
        int templateLength = template.length;
        int multiplier = 50000;
        char[] value = new char[ templateLength * multiplier ];
        int idx = 0;
        for ( int i = 0; i < multiplier; i++ )
        {
            for ( int j = 0; j < templateLength; j++ )
            {
                value[ idx++ ] = template[ j ];
            }
                          
        }
        
        return new HarmonySerialClob( new String( value ) );
    }
    private void compareClobs( Clob left, Clob right ) throws Exception
    {
        long leftLength = left.length();
        long rightLength = right.length();

        println( "Left clob has " + leftLength + " characters and right clob has " + rightLength + " characters." );

        assertEquals( leftLength, rightLength );

        if ( leftLength == rightLength );
        {
            String leftString = left.getSubString( 1L, (int) leftLength );
            String rightString = right.getSubString( 1L, (int) rightLength );

            for ( int i = 0; i < leftLength; i++ )
            {
                int leftIdx = (int) i;
                int rightIdx = (int) ((leftLength - leftIdx) - 1);
                char leftC = leftString.charAt( leftIdx );
                char rightC = rightString.charAt( rightIdx );

                if ( leftC != rightC )
                {
                    println( "left[ " + leftIdx+ " ] = " + leftC + " but right[ " + rightIdx + " ] = " + rightC );
                    return;
                }

                assertEquals( leftC, rightC );
            }
        }

    }

    /**
     * Verify correct mapping of blobs.
     */
    public void testBlobMapping() throws Exception
    {
        Connection conn = getConnection();
        PreparedStatement ps;
        CallableStatement cs;
        Blob outVal;

        //
        // Blob input parameter
        //
        ps = chattyPrepare
            (
             conn,
             "create procedure blobIn\n" +
             "( in c blob, out result varchar( 100 ) )\n" +
             "language java\n" +
             "parameter style java\n" +
             "no sql\n" +
             "external name '" + getClass().getName() + ".blobIn'\n"
             );
        ps.execute();
        ps.close();

        cs = chattyPrepareCall( conn, "call blobIn( ?, ? )" );
        cs.setBlob( 1, new HarmonySerialBlob( "ghi".getBytes( UTF8 ) ) );
        cs.registerOutParameter( 2, Types.VARCHAR );
        cs.execute();
        assertEquals( "ghi", cs.getString( 2 ) );
        cs.close();

        //
        // Blob output parameter
        //
        ps = chattyPrepare
            (
             conn,
             "create procedure blobOut\n" +
             "( out c blob )\n" +
             "language java\n" +
             "parameter style java\n" +
             "no sql\n" +
             "external name '" + getClass().getName() + ".blobOut'\n"
             );
        ps.execute();
        ps.close();

        cs = chattyPrepareCall( conn, "call blobOut( ? )" );
        cs.registerOutParameter( 1, Types.BLOB );
        cs.execute();
        outVal = cs.getBlob( 1 );
        assertEquals( "abc", getBlobValue( outVal ) );
        cs.close();
        
        //
        // Blob inout parameter
        //
        ps = chattyPrepare
            (
             conn,
             "create procedure blobInOut\n" +
             "( inout c blob )\n" +
             "language java\n" +
             "parameter style java\n" +
             "no sql\n" +
             "external name '" + getClass().getName() + ".blobInOut'\n"
             );
        ps.execute();
        ps.close();

        cs = chattyPrepareCall( conn, "call blobInOut( ? )" );
        cs.setBlob( 1, new HarmonySerialBlob( "ghi".getBytes( UTF8 ) ) );
        cs.registerOutParameter( 1, Types.BLOB );
        cs.execute();
        outVal = cs.getBlob( 1 );
        assertEquals( "ihg", getBlobValue( outVal ) );
        
        Blob inValue = makeBigBlob();
        cs.setBlob( 1, inValue );
        cs.execute();
        Blob outValue = cs.getBlob( 1 );
        compareBlobs( inValue, outValue );

        cs.close();
    }
    private Blob makeBigBlob() throws Exception
    {
        byte[] template = new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
        int templateLength = template.length;
        int multiplier = 110000;
        byte[] value = new byte[ templateLength * multiplier ];
        int idx = 0;
        for ( int i = 0; i < multiplier; i++ )
        {
            for ( int j = 0; j < templateLength; j++ )
            {
                value[ idx++ ] = template[ j ];
            }
                          
        }
        
        return new HarmonySerialBlob( value );
    }
    private void compareBlobs( Blob left, Blob right ) throws Exception
    {
        long leftLength = left.length();
        long rightLength = right.length();

        println( "Left blob has " + leftLength + " bytes and right blob has " + rightLength + " bytes." );

        assertEquals( leftLength, rightLength );

        if ( leftLength == rightLength );
        {
            byte[] leftBytes = left.getBytes( 1L, (int) leftLength );
            byte[] rightBytes = right.getBytes( 1L, (int) rightLength );

            for ( int i = 0; i < leftLength; i++ )
            {
                int leftIdx = (int) i;
                int rightIdx = (int) ((leftLength - leftIdx) - 1);
                byte leftC = leftBytes[ leftIdx ];
                byte rightC = rightBytes[ rightIdx ];

                if ( leftC != rightC )
                {
                    println( "left[ " + leftIdx+ " ] = " + leftC + " but right[ " + rightIdx + " ] = " + rightC );
                    return;
                }

                assertEquals( leftC, rightC );
            }
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.derbyTesting.junit.BaseJDBCTestCase#tearDown()
     */
    protected void tearDown() throws Exception {
        Connection conn = getConnection();
        Statement scb = conn.createStatement();
        scb.execute("DROP TABLE PM.LOB_GET");
        scb.close();
        commit();

    }

    private static void getXXX(PreparedStatement ps, int type, boolean isNull)
            throws SQLException, java.io.IOException {

        // Expect the different getters to return some variant of the integer
        // 32. The exception is BOOLEAN columns, which don't have high enough
        // precision to do that. So expect 1 for BOOLEAN columns.
        final int expectedInt =
                (ps.getMetaData().getColumnType(1) == Types.BOOLEAN) ?
                    1 : 32;

        {
         
            // getByte();
            ResultSet rs = ps.executeQuery();
            rs.next();
            boolean worked;
            SQLException sqleResult = null;
            ;
            try {
                byte b = rs.getByte(1);
                boolean wn = rs.wasNull();
                if (isNull) {
                    assertTrue(wn);
                } else {
                    assertFalse(wn);
                    assertEquals(expectedInt, b);
                }
                worked = true;

            } catch (SQLException sqle) {
                sqleResult = sqle;
                worked = false;
            }
            rs.close();
            judge_getXXX(worked, sqleResult, 0, type);
        }

        {
            // getShort()
            ResultSet rs = ps.executeQuery();
            rs.next();
            boolean worked;
            SQLException sqleResult = null;
            try {
                short s = rs.getShort(1);
                boolean wn = rs.wasNull();
                if (isNull) {
                    assertTrue(wn);
                } else {
                    assertFalse(wn);
                    assertEquals(expectedInt, s);
                }
                worked = true;

            } catch (SQLException sqle) {
                sqleResult = sqle;
                worked = false;
            }
            rs.close();
            judge_getXXX(worked, sqleResult, 1, type);
        }

        {
            // getInt()
            ResultSet rs = ps.executeQuery();
            rs.next();
            boolean worked;
            SQLException sqleResult = null;
            ;
            try {
                int i = rs.getInt(1);
                boolean wn = rs.wasNull();
                if (isNull) {
                    assertTrue(wn);
                } else {
                    assertFalse(isNull);
                    assertEquals(expectedInt, i);
                }
                worked = true;
            } catch (SQLException sqle) {
                sqleResult = sqle;
                worked = false;
            }
            rs.close();
            judge_getXXX(worked, sqleResult, 2, type);
        }

        {
            // getLong();
            ResultSet rs = ps.executeQuery();
            rs.next();
            boolean worked;
            SQLException sqleResult = null;
            ;
            try {
                long l = rs.getLong(1);
                boolean wn = rs.wasNull();
                if (isNull) {
                    assertTrue(wn);
                } else {
                    assertEquals(expectedInt, l);
                    assertFalse(wn);
                }
                worked = true;
            } catch (SQLException sqle) {
                sqleResult = sqle;
                worked = false;
            }
            rs.close();
            judge_getXXX(worked, sqleResult, 3, type);
        }

        {
            // getFloat()

            ResultSet rs = ps.executeQuery();
            rs.next();
            boolean worked;
            SQLException sqleResult = null;
            try {
                float f = rs.getFloat(1);
                boolean wn = rs.wasNull();

                if (isNull) {
                    assertTrue(wn);
                } else {
                    assertFalse(wn);
                    assertEquals((float) expectedInt, f, .000001);
                }
                worked = true;

            } catch (SQLException sqle) {
                sqleResult = sqle;
                worked = false;
            }
            rs.close();
            judge_getXXX(worked, sqleResult, 4, type);
        }

        {
            // getDouble();
            ResultSet rs = ps.executeQuery();
            rs.next();
            SQLException sqleResult = null;
            boolean worked;
            try {
                double d = rs.getDouble(1);
                boolean wn = rs.wasNull();
                if (isNull) {
                    assertTrue(wn);
                } else {
                    assertFalse(wn);
                    assertEquals((double) expectedInt, d, .00001);
                }
                worked = true;

            } catch (SQLException sqle) {
                sqleResult = sqle;
                worked = false;
            }
            rs.close();
            judge_getXXX(worked, sqleResult, 5, type);
        }

        if (HAVE_BIG_DECIMAL) {
            // getBigDecimal()
            ResultSet rs = ps.executeQuery();
            rs.next();
            boolean worked;
            SQLException sqleResult = null;
            ;
            try {
                BigDecimal bd = rs.getBigDecimal(1);
                boolean wn = rs.wasNull();
                if (isNull) {
                    assertTrue(wn);
                    assertNull(bd);
                } else {
                    assertFalse(wn);
                    assertEquals("BigDecimal comparison failed", 0,
                            BigDecimal.valueOf(
                                    (long)expectedInt).compareTo(bd));
                }
                worked = true;

            } catch (SQLException sqle) {
                sqleResult = sqle;
                worked = false;
            }
            rs.close();
            judge_getXXX(worked, sqleResult, 6, type);
        }

        {
            // getBoolean()
            ResultSet rs = ps.executeQuery();
            rs.next();
            boolean worked;
            SQLException sqleResult = null;
            ;
            try {
                boolean b = rs.getBoolean(1);
                boolean wn = rs.wasNull();
                if (isNull) {
                    assertTrue(wn);
                } else {
                    assertFalse(wn);
                    assertTrue(b);
                }
                worked = true;

            } catch (SQLException sqle) {
                sqleResult = sqle;
                worked = false;
            }
            rs.close();
            judge_getXXX(worked, sqleResult, 7, type);
        }

        {
            // getString()
            ResultSet rs = ps.executeQuery();
            rs.next();
            boolean worked;
            SQLException sqleResult = null;
            ;
            try {
                String s = rs.getString(1);
                boolean wn = rs.wasNull();
                if (isNull) {
                    assertNull(s);
                    assertTrue(wn);
                } else {                    
                    s = s.trim();
                    int jdbcType = jdbcTypes[type];
                    switch(jdbcType) {
                    case java.sql.Types.SMALLINT:
                    case java.sql.Types.INTEGER:
                    case java.sql.Types.BIGINT:
                    case java.sql.Types.CHAR:
                    case java.sql.Types.VARCHAR:
                    case java.sql.Types.LONGVARCHAR:
                        assertEquals(Integer.toString(expectedInt),s);
                        break;
                    case java.sql.Types.REAL:
                    case java.sql.Types.FLOAT:
                    case java.sql.Types.DOUBLE:
                        assertEquals(expectedInt + ".0",s);
                        break;
                    case java.sql.Types.DECIMAL:
                    case java.sql.Types.NUMERIC:
                        assertEquals(expectedInt + ".00000",s);
                        break;
                    case java.sql.Types.VARBINARY:
                    case java.sql.Types.BINARY:
                        assertEquals("0403fdc373",s);
                        break;
                    case java.sql.Types.DATE:
                        assertEquals("2004-02-14",s);
                        break;
                    case java.sql.Types.TIME:
                        assertEquals("17:14:24",s);
                        break;
                    case java.sql.Types.TIMESTAMP:
                        assertEquals("2004-02-14 17:14:24.097625551",s);
                        break;
                    case java.sql.Types.CLOB:
                        assertEquals("67",s);
                        break;
                    case java.sql.Types.BLOB:
                        assertEquals("8243cafe0032",s);
                        break;
                    }
                    
                    assertFalse(wn);
                }
                worked = true;

            } catch (SQLException sqle) {
                sqleResult = sqle;
                worked = false;
            }
            rs.close();
            judge_getXXX(worked, sqleResult, 8, type);
        }

        {
            // getBytes()
            ResultSet rs = ps.executeQuery();
            rs.next();
            boolean worked;
            SQLException sqleResult = null;
            ;
            try {
                byte[] data = rs.getBytes(1);
                boolean wn = rs.wasNull();
                if (isNull) {
                    assertNull(data);
                    assertTrue(wn);
                } else {
                    int jdbcType = jdbcTypes[type];
                    switch (jdbcType) {
                    case java.sql.Types.BINARY:
                    case java.sql.Types.VARBINARY:
                    case java.sql.Types.LONGVARBINARY:
                        assertEquals("0x4,0x3", showFirstTwo(data));
                        break;
                    case java.sql.Types.BLOB:
                        assertEquals("0x82,0x43", showFirstTwo(data));
                    }
                    assertNotNull(data);
                    assertFalse(wn);
                }
                worked = true;

            } catch (SQLException sqle) {
                sqleResult = sqle;
                worked = false;
            }
            rs.close();
            judge_getXXX(worked, sqleResult, 9, type);
        }

        {
            // getDate()

            boolean worked;
            SQLException sqleResult = null;
            ;
            ResultSet rs = null;
            try {
                rs = ps.executeQuery();
                rs.next();
                Date d = rs.getDate(1);
                boolean wn = rs.wasNull();
                if (isNull) {
                    assertNull(d);
                    assertTrue(wn);
                } else {
                    assertEquals("2004-02-14", d.toString());
                    assertNotNull(d);
                    assertFalse(wn);
                }
                worked = true;

            } catch (SQLException sqle) {
                sqleResult = sqle;
                // 22007 invalid date time conversion
                worked = "22007".equals(sqle.getSQLState());
            } catch (Throwable t) {
                // System.out.print(t.toString());
                worked = false;
            }
            if (rs != null)
                rs.close();
            judge_getXXX(worked, sqleResult, 10, type);
        }

        {
            boolean worked;
            SQLException sqleResult = null;
            ;
            ResultSet rs = null;
            try {
                // getTime()
                rs = ps.executeQuery();
                rs.next();
                Time t = rs.getTime(1);
                boolean wn = rs.wasNull();
                if (isNull) {
                    assertNull(t);
                    assertTrue(wn);
                } else {
                    assertFalse(wn);
                    assertEquals("17:14:24", t.toString());
                }
                worked = true;

            } catch (SQLException sqle) {
                sqleResult = sqle;
                // 22007 invalid date time conversion
                worked = "22007".equals(sqle.getSQLState());
            } catch (Throwable t) {
                // System.out.println(t);
                worked = false;
            }
            if (rs != null)
                rs.close();
            judge_getXXX(worked, sqleResult, 11, type);
        }

        {
            boolean worked;
            SQLException sqleResult = null;
            ;
            ResultSet rs = null;
            try {
                // getTimestamp();
                rs = ps.executeQuery();
                rs.next();
                Timestamp ts = rs.getTimestamp(1);
                boolean wn = rs.wasNull();
                if (isNull) {
                    assertNull(ts);
                    assertTrue(wn);
                } else {
                    if (type == java.sql.Types.DATE
                            || type == java.sql.Types.TIMESTAMP)
                        assertEquals("2004-02-14 00:00:00.0", ts.toString());
                    assertFalse(rs.wasNull());
                }
                worked = true;

            } catch (SQLException sqle) {
                sqleResult = sqle;
                // 22007 invalid date time conversion
                worked = "22007".equals(sqle.getSQLState());
            } catch (Throwable t) {
                // System.out.println(t);
                worked = false;
            }
            if (rs != null)
                rs.close();
            judge_getXXX(worked, sqleResult, 12, type);
        }

        {
            // getAsciiStream()

            ResultSet rs = ps.executeQuery();
            rs.next();
            boolean worked;
            SQLException sqleResult = null;
            ;
            try {
                InputStream is = rs.getAsciiStream(1);
                boolean wn = rs.wasNull();
                if (isNull) {
                    assertTrue(wn);
                    assertNull(is);
                } else {
                    assertFalse(wn);
                    if (B6[13][type])
                        assertNotNull(showFirstTwo(is));
                }

                worked = true;

            } catch (SQLException sqle) {
                sqleResult = sqle;
                worked = false;
            }

            // getAsciiStream on a NULL value for an invalid conversion
            // is handled differently in JCC to Cloudscape. On a non-NULL
            // value an exception is correctly raised by both JCC and CS.
            // here we check this specific case to reduce canon differences
            // between CNS and CS.

            boolean judge = B6[13][type]
                    || specificCheck(rs, worked, sqleResult, isNull);
            rs.close();
            if (judge)
                judge_getXXX(worked, sqleResult, 13, type);
        }

        {
            // getBinaryStream()
            ResultSet rs = ps.executeQuery();
            rs.next();
            boolean worked;
            SQLException sqleResult = null;
            ;
            try {
                InputStream is = rs.getBinaryStream(1);
                if (isNull) {
                    assertTrue(rs.wasNull());
                    assertNull(is);

                } else if (B6[14][type]) {
                    assertNotNull(showFirstTwo(is));
                }
                worked = true;

            } catch (SQLException sqle) {
                sqleResult = sqle;
                worked = false;
            }
            boolean judge = B6[14][type]
                    || specificCheck(rs, worked, sqleResult, isNull);
            rs.close();
            if (judge)
                judge_getXXX(worked, sqleResult, 14, type);
        }

        {
            // getCharacterStream()
            ResultSet rs = ps.executeQuery();
            rs.next();
            boolean worked;
            SQLException sqleResult = null;
            ;
            try {

                Reader r = rs.getCharacterStream(1);
                boolean wn = rs.wasNull();
                if (isNull) {
                    assertNull(r);
                    assertTrue(wn);
                } else if (B6[15][type]) {
                    assertFalse(wn);
                    assertNotNull(showFirstTwo(r));
                }
                worked = true;

            } catch (SQLException sqle) {
                sqleResult = sqle;
                worked = false;
            }
            boolean judge = B6[15][type]
                    || specificCheck(rs, worked, sqleResult, isNull);
            rs.close();
            if (judge)
                judge_getXXX(worked, sqleResult, 15, type);
        }

        {
            // getClob();
            ResultSet rs = ps.executeQuery();
            rs.next();
            boolean worked;
            SQLException sqleResult = null;
            ;
            try {
                Clob clob = rs.getClob(1);
                boolean wn = rs.wasNull();
                if (isNull) {
                    assertNull(clob);
                    assertTrue(wn);
                } else if (B6[16][type]) {
                    assertFalse(wn);
                    assertNotNull(clob.getSubString(1, 10));
                }
                worked = true;

            } catch (SQLException sqle) {
                sqleResult = sqle;
                worked = false;
            }
            boolean judge = B6[16][type]
                    || specificCheck(rs, worked, sqleResult, isNull);
            rs.close();
            if (judge)
                judge_getXXX(worked, sqleResult, 16, type);
        }

        {
            // getBlob()

            ResultSet rs = ps.executeQuery();
            rs.next();
            boolean worked;
            SQLException sqleResult = null;
            ;
            try {
                Blob blob = rs.getBlob(1);
                boolean wn = rs.wasNull();
                if (isNull) {
                    assertTrue(wn);
                    assertNull(blob);
                } else if (B6[17][type]) {
                    assertNotNull(showFirstTwo(blob.getBinaryStream()));
                }
                worked = true;

            } catch (SQLException sqle) {
                sqleResult = sqle;
                worked = false;
            }
            boolean judge = B6[17][type]
                    || specificCheck(rs, worked, sqleResult, isNull);
            rs.close();
            if (judge)
                judge_getXXX(worked, sqleResult, 17, type);
        }

        {
            // getUnicodeStream()
            ResultSet rs = ps.executeQuery();
            rs.next();
            boolean worked;
            SQLException sqleResult = null;
            ;
            try {
                InputStream is = rs.getUnicodeStream(1);
                boolean wn = rs.wasNull();
                if (isNull) {
                    assertTrue(wn);
                    assertNull(is);

                } else {
                    assertFalse(wn);
                    assertNotNull(is);
                }
                worked = true;
            } catch (NoSuchMethodError e) {
                worked = true;

            } catch (SQLException sqle) {
                sqleResult = sqle;
                worked = false;
            }
            rs.close();
            if (JDBC.vmSupportsJDBC3())
                judge_getXXX(worked, sqleResult, 18, type);
        }

        // Check to see getObject returns the correct type
        {
            // getObject();
            ResultSet rs = ps.executeQuery();
            rs.next();
            SQLException sqleResult = null;
            ;
            try {

                boolean worked;
                if (!SQLTypes[type].equals("DECIMAL(10,5)") || HAVE_BIG_DECIMAL) {
                    Object o = rs.getObject(1);
                    boolean wn = rs.wasNull();
                    Class cgo = B3_GET_OBJECT[type];

                    String cname;
                    if (cgo.equals(byte[].class))
                        cname = "byte[]";
                    else
                        cname = cgo.getName();
                    if (isNull) {
                        assertTrue(wn);
                        assertNull(o);
                        worked = true;
                    } else if (cgo.isInstance(o)) {
                        worked = true;
                    } else {
                        worked = false;
                        fail("FAIL NOT :" + cgo.getName() + " is "
                                + o.getClass().getName());
                    }
                } else {
                    // "ResultSet.getObject not called for DECIMAL type for
                    // JSR169";
                    worked = true;
                }
                assertTrue(worked);

            } catch (SQLException sqle) {
                sqleResult = sqle;
            }
            rs.close();
        }

    }

    private static boolean specificCheck(ResultSet rs, boolean worked,
            SQLException sqleResult, boolean isNull) throws SQLException {
        boolean judge = true;
        if (worked && isNull && rs.wasNull()) {
            // JCC returns NULL
            if (usingDerbyNetClient())
                judge = false;
        } else if (!worked && isNull) {
            if (usingDerbyNetClient()
                    && "22005".equals(sqleResult.getSQLState()))
                judge = false;
        }

        return judge;
    }

    private static void judge_getXXX(boolean worked, SQLException sqleResult,
            int whichCall, int type) {
        boolean validSQLState = false;
        // verify valid conversion worked
        if (B6[whichCall][type] && !worked)
            fail(" JDBC FAIL " + SQLTypes[type] + " " + sqleResult);
        else if (!worked) {
            // make sure not implemented or conversion error was thrown if it
            // didn't work
            String sqlState = sqleResult.getSQLState();
            if ("0A000".equals(sqlState))
                validSQLState = true;
            if ("0A000".equals(sqlState))
                validSQLState = true;
            if ("22005".equals(sqlState))
                // embedded invalid conversion error
                validSQLState = true;
            else if (sqlState == null) {
                // client invalid conversion error
                if (sqleResult.getMessage().indexOf(
                        "Wrong result column type for requested conversion") != -1)
                    validSQLState = true;
            }
            assertTrue("FAIL: Expected conversion error but got " + sqleResult,
                    validSQLState);

        }

    }

    private static void judge_setXXX(boolean worked, SQLException sqleResult,
            int whichCall, int type) {
        String msg;
        boolean shouldWork = B2_MOD[whichCall][type];

        if (worked && shouldWork)
            msg = " JDBC MATCH(OK)";
        else if (worked)
            msg = " CLOUD EXT (OK)";
        else if (sqleResult != null && "0A000".equals(sqleResult.getSQLState()))
            msg = " Not Implemented (OK)";
        else if (shouldWork) {
            if (sqleResult != null)
                showException(sqleResult);
            msg = " JDBC FAIL " + SQLTypes[type];
        } else {
            msg = checkForInvalidConversion(sqleResult);
            if (msg == null)
                return;
        }
        if (msg.startsWith("JDBC FAIL"))
            fail(" JDBC FAIL " + SQLTypes[type]);
    }

    private static void judge_setObject(boolean worked,
            SQLException sqleResult, int b5o, int type) {
        String msg;
        boolean shouldWork = B5[b5o][type];

        if (worked && shouldWork)
            msg = " JDBC MATCH(OK)";
        else if (worked)
            msg = " CLOUD EXT (OK)";
        else if ("0A000".equals(sqleResult.getSQLState()))
            msg = " Not Implemented (OK)";
        else if (shouldWork) {
            if (sqleResult != null)
                showException(sqleResult);
            msg = " JDBC FAIL " + SQLTypes[type];
        } else {
            msg = checkForInvalidConversion(sqleResult);
            if (msg == null)
                return;
        }
        if (msg.startsWith("JDBC FAIL"))
            fail(" JDBC FAIL " + SQLTypes[type]);
    }

    /**
     * Look for an "Invalid Conversion" exception and format it for display.
     * 
     * Look for an "Invalid Conversion" exception. If one is found, print "IC".
     * If one is not found, dump the actual exception to the output instead.
     * 
     * Note that the actual invalid conversion exception may be wrapped inside a
     * BatchUpdateException, so we may need to hunt through the exception chain
     * to find it.
     */
    private static String checkForInvalidConversion(SQLException sqle) {
        if (sqle == null)
            return null;

        boolean unknownException = true;
        SQLException e = sqle;
        while (e != null && unknownException == true) {
            // XCL12 is temp
            if ("22005".equals(e.getSQLState())
                    || "XCL12".equals(e.getSQLState())
                    || e.getMessage().indexOf("Illegal Conv") != -1) {
                unknownException = false;
                if ("0A000".equals(e.getSQLState())
                        && e.getMessage().indexOf("setUnicodeStream") != -1)
                    unknownException = false;

                // System.out.print("IC");
                break;
            }
            e = e.getNextException();
        }
        if (unknownException)
            showException(sqle);

        return " JDBC MATCH (INVALID)";
    }

    private static void setXXX(Statement s, PreparedStatement psi,
            PreparedStatement psq, int type) throws SQLException,
            java.io.IOException {

        {
            s.execute("DELETE FROM PM.TYPE_AS");

            SQLException sqleResult = null;
            boolean worked;
            try {
                // setByte()
                psi.setByte(1, (byte) 98);
                psi.executeUpdate();

                getValidValue(psq, jdbcTypes[type], "setByte");

                worked = true;

            } catch (SQLException sqle) {
                sqleResult = sqle;
                worked = false;
            }
            judge_setXXX(worked, sqleResult, 0, type);
        }
        // and as a batch
        {
            s.execute("DELETE FROM PM.TYPE_AS");

            SQLException sqleResult = null;
            boolean worked;
            try {
                psi.setByte(1, (byte) 98);
                psi.addBatch();
                psi.executeBatch();

                getValidValue(psq, jdbcTypes[type], "setByte");

                worked = true;

            } catch (SQLException sqle) {
                sqleResult = sqle;
                worked = false;
            }
            judge_setXXX(worked, sqleResult, 0, type);
        }
        {
            s.execute("DELETE FROM PM.TYPE_AS");

            SQLException sqleResult = null;
            boolean worked;
            try {
                // setShort()
                psi.setShort(1, (short) 98);
                psi.executeUpdate();

                getValidValue(psq, jdbcTypes[type], "setShort");

                worked = true;

            } catch (SQLException sqle) {
                sqleResult = sqle;
                worked = false;
            }
            judge_setXXX(worked, sqleResult, 1, type);
        }
        // and as a batch
        {
            s.execute("DELETE FROM PM.TYPE_AS");

            SQLException sqleResult = null;
            boolean worked;
            try {
                // setShort() as batch
                psi.setShort(1, (short) 98);
                psi.addBatch();
                psi.executeBatch();

                getValidValue(psq, jdbcTypes[type], "setShort");

                worked = true;

            } catch (SQLException sqle) {
                sqleResult = sqle;
                worked = false;
            }
            judge_setXXX(worked, sqleResult, 1, type);
        }
        {
            s.execute("DELETE FROM PM.TYPE_AS");

            SQLException sqleResult = null;
            boolean worked;
            try {
                // setInt()
                psi.setInt(1, 98);
                psi.executeUpdate();

                getValidValue(psq, jdbcTypes[type], "setInt");

                worked = true;

            } catch (SQLException sqle) {
                sqleResult = sqle;
                worked = false;
            }
            judge_setXXX(worked, sqleResult, 2, type);
        }
        // and as a batch
        {
            s.execute("DELETE FROM PM.TYPE_AS");

            SQLException sqleResult = null;
            boolean worked;
            try {
                // setInt() as batch
                psi.setInt(1, 98);
                psi.addBatch();
                psi.executeBatch();

                getValidValue(psq, jdbcTypes[type], "setInt");

                worked = true;

            } catch (SQLException sqle) {
                sqleResult = sqle;
                worked = false;
            }
            judge_setXXX(worked, sqleResult, 2, type);
        }
        {
            s.execute("DELETE FROM PM.TYPE_AS");

            SQLException sqleResult = null;
            boolean worked;
            try {
                // setLong()
                psi.setLong(1, 98L);
                psi.executeUpdate();

                getValidValue(psq, jdbcTypes[type], "setLong");

                worked = true;

            } catch (SQLException sqle) {
                sqleResult = sqle;
                worked = false;
            }
            judge_setXXX(worked, sqleResult, 3, type);
        }
        // as a batch
        {
            s.execute("DELETE FROM PM.TYPE_AS");

            SQLException sqleResult = null;
            boolean worked;
            try {
                // setLong() as batch
                psi.setLong(1, 98L);
                psi.addBatch();
                psi.executeBatch();

                getValidValue(psq, jdbcTypes[type], "setLong");

                worked = true;

            } catch (SQLException sqle) {
                sqleResult = sqle;
                worked = false;
            }
            judge_setXXX(worked, sqleResult, 3, type);
        }

        {
            s.execute("DELETE FROM PM.TYPE_AS");

            SQLException sqleResult = null;
            boolean worked;
            try {
                // setFloat()
                psi.setFloat(1, 98.4f);
                psi.executeUpdate();

                getValidValue(psq, jdbcTypes[type], "setFloat");

                worked = true;

            } catch (SQLException sqle) {
                sqleResult = sqle;
                worked = false;
            }
            judge_setXXX(worked, sqleResult, 4, type);
        }

        // and as a batch
        {
            s.execute("DELETE FROM PM.TYPE_AS");

            SQLException sqleResult = null;
            boolean worked;
            try {
                // setFloat() as batch
                psi.setFloat(1, 98.4f);
                psi.addBatch();
                psi.executeBatch();

                getValidValue(psq, jdbcTypes[type], "setFloat");

                worked = true;

            } catch (SQLException sqle) {
                sqleResult = sqle;
                worked = false;
            }
            judge_setXXX(worked, sqleResult, 4, type);
        }

        {
            s.execute("DELETE FROM PM.TYPE_AS");

            SQLException sqleResult = null;
            boolean worked;
            try {
                // setDouble()
                psi.setDouble(1, 98.5);
                psi.executeUpdate();

                getValidValue(psq, jdbcTypes[type], "setDouble");

                worked = true;

            } catch (SQLException sqle) {
                sqleResult = sqle;
                worked = false;
            }
            judge_setXXX(worked, sqleResult, 5, type);
        }

        // as a batch
        {
            s.execute("DELETE FROM PM.TYPE_AS");

            SQLException sqleResult = null;
            boolean worked;
            try {
                // setDouble() as batch
                psi.setDouble(1, 98.5);
                psi.addBatch();
                psi.executeBatch();

                getValidValue(psq, jdbcTypes[type], "setDouble");

                worked = true;

            } catch (SQLException sqle) {
                sqleResult = sqle;
                worked = false;
            }
            judge_setXXX(worked, sqleResult, 5, type);
        }

        if (HAVE_BIG_DECIMAL) {
            {
                s.execute("DELETE FROM PM.TYPE_AS");

                SQLException sqleResult = null;
                boolean worked;
                try {
                    // setBigDecimal()
                    psi.setBigDecimal(1, new BigDecimal(98.0));
                    psi.executeUpdate();

                    getValidValue(psq, jdbcTypes[type], "setBigDecimal");

                    worked = true;

                } catch (SQLException sqle) {
                    sqleResult = sqle;
                    worked = false;
                }
                judge_setXXX(worked, sqleResult, 6, type);
            }
            // as a batch
            {
                s.execute("DELETE FROM PM.TYPE_AS");

                SQLException sqleResult = null;
                boolean worked;
                try {
                    // setBigDecimal() as batch
                    psi.setBigDecimal(1, new BigDecimal(98.0));
                    psi.addBatch();
                    psi.executeBatch();

                    getValidValue(psq, jdbcTypes[type], "setBigDecimal");

                    worked = true;

                } catch (SQLException sqle) {
                    sqleResult = sqle;
                    worked = false;
                }
                judge_setXXX(worked, sqleResult, 6, type);
            }
            // null BigDecimal
            {
                s.execute("DELETE FROM PM.TYPE_AS");

                SQLException sqleResult = null;
                boolean worked;
                try {
                    // setBigDecimal(null)
                    psi.setBigDecimal(1, null);
                    psi.executeUpdate();

                    getValidValue(psq, jdbcTypes[type], "setBigDecimal");

                    worked = true;

                } catch (SQLException sqle) {
                    sqleResult = sqle;
                    worked = false;
                }
                judge_setXXX(worked, sqleResult, 6, type);
            }

            // null BigDecimal
            {
                s.execute("DELETE FROM PM.TYPE_AS");

                SQLException sqleResult = null;
                boolean worked;
                try {
                    // setBigDecimal(null) as batch
                    psi.setBigDecimal(1, null);
                    psi.addBatch();
                    psi.executeBatch();

                    getValidValue(psq, jdbcTypes[type], "setBigDecimal");

                    worked = true;

                } catch (SQLException sqle) {
                    sqleResult = sqle;
                    worked = false;
                }
                judge_setXXX(worked, sqleResult, 6, type);
            }
        }

        {
            s.execute("DELETE FROM PM.TYPE_AS");

            SQLException sqleResult = null;
            boolean worked;
            try {
                // setBoolean()
                psi.setBoolean(1, true);
                psi.executeUpdate();

                getValidValue(psq, jdbcTypes[type], "setBoolean");

                worked = true;

            } catch (SQLException sqle) {
                sqleResult = sqle;
                worked = false;
            }
            judge_setXXX(worked, sqleResult, 7, type);
        }
        {
            s.execute("DELETE FROM PM.TYPE_AS");

            SQLException sqleResult = null;
            boolean worked;
            try {
                // setBoolean() as batch
                psi.setBoolean(1, true);
                psi.addBatch();
                psi.executeBatch();

                getValidValue(psq, jdbcTypes[type], "setBoolean");

                worked = true;

            } catch (SQLException sqle) {
                sqleResult = sqle;
                worked = false;
            }
            judge_setXXX(worked, sqleResult, 7, type);
        }
        {
            s.execute("DELETE FROM PM.TYPE_AS");

            SQLException sqleResult = null;
            boolean worked;
            try {
                
                psi.setString(1,validString[type]);
                psi.executeUpdate();

                getValidValue(psq, jdbcTypes[type], "setString");

                worked = true;

            } catch (SQLException sqle) {
                sqleResult = sqle;
                worked = false;
            } catch (Throwable t) {
                // JCC has some bugs
                // System.out.println(t.getMessage());
                worked = false;
                sqleResult = null;

            }
            judge_setXXX(worked, sqleResult, 8, type);
        }
        // as batch
        {
            s.execute("DELETE FROM PM.TYPE_AS");

            SQLException sqleResult = null;
            boolean worked;
            try {
                // setString() as batch
               
                psi.setString(1,validString[type]);
                psi.addBatch();
                psi.executeBatch();

                getValidValue(psq, jdbcTypes[type], "setString");

                worked = true;

            } catch (SQLException sqle) {
                sqleResult = sqle;
                worked = false;
            } catch (Throwable t) {
                // JCC has some bugs
                // System.out.println(t.getMessage());
                worked = false;
                sqleResult = null;

            }
            judge_setXXX(worked, sqleResult, 8, type);
        }

        // null String
        {
            s.execute("DELETE FROM PM.TYPE_AS");

            SQLException sqleResult = null;
            boolean worked;
            try {
                // setString(null)
                psi.setString(1, null);
                psi.executeUpdate();

                getValidValue(psq, jdbcTypes[type], "setString");

                worked = true;

            } catch (SQLException sqle) {
                sqleResult = sqle;
                worked = false;
            } catch (Throwable t) {
                // JCC has some bugs
                // System.out.println(t.getMessage());
                worked = false;
                sqleResult = null;

            }
            judge_setXXX(worked, sqleResult, 8, type);
        }
        // null String as batch
        {
            s.execute("DELETE FROM PM.TYPE_AS");

            SQLException sqleResult = null;
            boolean worked;
            try {
                // setString(null) as batch
                psi.setString(1, null);
                psi.addBatch();
                psi.executeBatch();

                getValidValue(psq, jdbcTypes[type], "setString");

                worked = true;

            } catch (SQLException sqle) {
                sqleResult = sqle;
                worked = false;
            } catch (Throwable t) {
                // JCC has some bugs
                // System.out.println(t.getMessage());
                worked = false;
                sqleResult = null;

            }
            judge_setXXX(worked, sqleResult, 8, type);
        }
        {
            s.execute("DELETE FROM PM.TYPE_AS");

            // Set Invalid String for nonString types (DERBY-149)
            testSetStringInvalidValue(type, psi);
        }
        {
            s.execute("DELETE FROM PM.TYPE_AS");

            SQLException sqleResult = null;
            boolean worked;
            try {
                // setBytes()
                byte[] data = { (byte) 0x04, (byte) 0x03, (byte) 0xfd,
                        (byte) 0xc3, (byte) 0x73 };
                psi.setBytes(1, data);
                psi.executeUpdate();

                getValidValue(psq, jdbcTypes[type], "setBytes");

                worked = true;

            } catch (SQLException sqle) {
                sqleResult = sqle;
                worked = false;
            }
            judge_setXXX(worked, sqleResult, 9, type);
        }
        {
            s.execute("DELETE FROM PM.TYPE_AS");

            SQLException sqleResult = null;
            boolean worked;
            try {
                // setBytes() as batch
                byte[] data = { (byte) 0x04, (byte) 0x03, (byte) 0xfd,
                        (byte) 0xc3, (byte) 0x73 };
                psi.setBytes(1, data);
                psi.addBatch();
                psi.executeBatch();

                getValidValue(psq, jdbcTypes[type], "setBytes");

                worked = true;

            } catch (SQLException sqle) {
                sqleResult = sqle;
                worked = false;
            }
            judge_setXXX(worked, sqleResult, 9, type);
        }
        // null byte[]
        {
            s.execute("DELETE FROM PM.TYPE_AS");

            SQLException sqleResult = null;
            boolean worked;
            try {
                // setBytes(null)
                psi.setBytes(1, null);
                psi.executeUpdate();

                getValidValue(psq, jdbcTypes[type], "setBytes");

                worked = true;

            } catch (SQLException sqle) {
                sqleResult = sqle;
                worked = false;
            }
            judge_setXXX(worked, sqleResult, 9, type);
        }

        {
            s.execute("DELETE FROM PM.TYPE_AS");

            SQLException sqleResult = null;
            boolean worked;
            try {
                // setBytes(null) as batch
                psi.setBytes(1, null);
                psi.addBatch();
                psi.executeBatch();

                getValidValue(psq, jdbcTypes[type], "setBytes");

                worked = true;

            } catch (SQLException sqle) {
                sqleResult = sqle;
                worked = false;
            }
            judge_setXXX(worked, sqleResult, 9, type);
        }
        {
            s.execute("DELETE FROM PM.TYPE_AS");

            SQLException sqleResult = null;
            boolean worked;
            try {
                // setDate()
                psi.setDate(1, java.sql.Date.valueOf("2004-02-14"));
                psi.executeUpdate();

                getValidValue(psq, jdbcTypes[type], "setDate");

                worked = true;

            } catch (SQLException sqle) {
                sqleResult = sqle;
                worked = false;
            }
            judge_setXXX(worked, sqleResult, 10, type);
        }
        {
            s.execute("DELETE FROM PM.TYPE_AS");

            SQLException sqleResult = null;
            boolean worked;
            try {
                // setDate() as batch
                psi.setDate(1, java.sql.Date.valueOf("2004-02-14"));
                psi.addBatch();
                psi.executeBatch();

                getValidValue(psq, jdbcTypes[type], "setDate");

                worked = true;

            } catch (SQLException sqle) {
                sqleResult = sqle;
                worked = false;
            }
            judge_setXXX(worked, sqleResult, 10, type);
        }
        // null Date
        {
            s.execute("DELETE FROM PM.TYPE_AS");

            SQLException sqleResult = null;
            boolean worked;
            try {
                // setDate(null)
                psi.setDate(1, null);
                psi.executeUpdate();

                getValidValue(psq, jdbcTypes[type], "setDate");

                worked = true;

            } catch (SQLException sqle) {
                sqleResult = sqle;
                worked = false;
            }
            judge_setXXX(worked, sqleResult, 10, type);
        }

        // null Date
        {
            s.execute("DELETE FROM PM.TYPE_AS");

            SQLException sqleResult = null;
            boolean worked;
            try {
                // setDate(null) as batch
                psi.setDate(1, null);
                psi.addBatch();
                psi.executeBatch();

                getValidValue(psq, jdbcTypes[type], "setDate");

                worked = true;

            } catch (SQLException sqle) {
                sqleResult = sqle;
                worked = false;
            }
            judge_setXXX(worked, sqleResult, 10, type);
        }

        {
            s.execute("DELETE FROM PM.TYPE_AS");

            SQLException sqleResult = null;
            boolean worked;
            try {
                // setTime()
                psi.setTime(1, java.sql.Time.valueOf("00:00:00"));
                psi.executeUpdate();

                getValidValue(psq, jdbcTypes[type], "setTime");

                worked = true;

            } catch (SQLException sqle) {
                sqleResult = sqle;
                worked = false;
            }
            judge_setXXX(worked, sqleResult, 11, type);
        }
        {
            s.execute("DELETE FROM PM.TYPE_AS");

            SQLException sqleResult = null;
            boolean worked;
            try {
                // setTime() as batch
                psi.setTime(1, java.sql.Time.valueOf("00:00:00"));
                psi.addBatch();
                psi.executeBatch();

                getValidValue(psq, jdbcTypes[type], "setTime");

                worked = true;

            } catch (SQLException sqle) {
                sqleResult = sqle;
                worked = false;
            }
            judge_setXXX(worked, sqleResult, 11, type);
        }

        {
            s.execute("DELETE FROM PM.TYPE_AS");

            SQLException sqleResult = null;
            boolean worked;
            try {
                // setTime(null)
                psi.setTime(1, null);
                psi.executeUpdate();

                getValidValue(psq, jdbcTypes[type], "setTime");

                worked = true;

            } catch (SQLException sqle) {
                sqleResult = sqle;
                worked = false;
            }
            judge_setXXX(worked, sqleResult, 11, type);
        }
        {
            s.execute("DELETE FROM PM.TYPE_AS");

            SQLException sqleResult = null;
            boolean worked;
            try {
                // setTime(null) as batch
                psi.setTime(1, null);
                psi.addBatch();
                psi.executeBatch();

                getValidValue(psq, jdbcTypes[type], "setTime");

                worked = true;

            } catch (SQLException sqle) {
                sqleResult = sqle;
                worked = false;
            }
            judge_setXXX(worked, sqleResult, 11, type);
        }
        {
            s.execute("DELETE FROM PM.TYPE_AS");

            SQLException sqleResult = null;
            boolean worked;
            try {
                // setTimestamp()
                psi.setTimestamp(1, java.sql.Timestamp
                        .valueOf("2004-02-14 00:00:00.0"));
                psi.executeUpdate();

                getValidValue(psq, jdbcTypes[type], "setTimestamp");

                worked = true;

            } catch (SQLException sqle) {
                sqleResult = sqle;
                worked = false;
            }
            judge_setXXX(worked, sqleResult, 12, type);
        }
        // as batch
        {
            s.execute("DELETE FROM PM.TYPE_AS");

            SQLException sqleResult = null;
            boolean worked;
            try {
                // setTimestamp() as batch
                psi.setTimestamp(1, java.sql.Timestamp
                        .valueOf("2004-02-14 00:00:00.0"));
                psi.addBatch();
                psi.executeBatch();

                getValidValue(psq, jdbcTypes[type], "setTimestamp");

                worked = true;

            } catch (SQLException sqle) {
                sqleResult = sqle;
                worked = false;
            }
            judge_setXXX(worked, sqleResult, 12, type);
        }

        {
            s.execute("DELETE FROM PM.TYPE_AS");

            SQLException sqleResult = null;
            boolean worked;
            try {
                // setTimestamp(null)
                psi.setTimestamp(1, null);
                psi.executeUpdate();

                getValidValue(psq, jdbcTypes[type], "setTimestamp");

                worked = true;

            } catch (SQLException sqle) {
                sqleResult = sqle;
                worked = false;
            }
            judge_setXXX(worked, sqleResult, 12, type);
        }
        // as batch
        {
            s.execute("DELETE FROM PM.TYPE_AS");

            SQLException sqleResult = null;
            boolean worked;
            try {
                // setTimestamp(null) as batch
                psi.setTimestamp(1, null);
                psi.addBatch();
                psi.executeBatch();

                getValidValue(psq, jdbcTypes[type], "setTimestamp");

                worked = true;

            } catch (SQLException sqle) {
                sqleResult = sqle;
                worked = false;
            }
            judge_setXXX(worked, sqleResult, 12, type);
        }

        {
            s.execute("DELETE FROM PM.TYPE_AS");

            SQLException sqleResult = null;
            boolean worked;
            try {
                // setAsciiStream()
                byte[] data = new byte[6];
                data[0] = (byte) 0x65;
                data[1] = (byte) 0x67;
                data[2] = (byte) 0x30;
                data[3] = (byte) 0x31;
                data[4] = (byte) 0x32;
                data[5] = (byte) 0x64;

                psi
                        .setAsciiStream(1, new java.io.ByteArrayInputStream(
                                data), 6);
                psi.executeUpdate();
                getValidValue(psq, jdbcTypes[type], "setAsciiStream");

                worked = true;

            } catch (SQLException sqle) {
                sqleResult = sqle;
                worked = false;
            }
            judge_setXXX(worked, sqleResult, 13, type);
        }
        {
            s.execute("DELETE FROM PM.TYPE_AS");

            SQLException sqleResult = null;
            boolean worked;
            try {
                // setAsciiStream() as batch
                byte[] data = new byte[6];
                data[0] = (byte) 0x65;
                data[1] = (byte) 0x67;
                data[2] = (byte) 0x30;
                data[3] = (byte) 0x31;
                data[4] = (byte) 0x32;
                data[5] = (byte) 0x64;

                psi
                        .setAsciiStream(1, new java.io.ByteArrayInputStream(
                                data), 6);
                psi.addBatch();
                psi.executeBatch();
                getValidValue(psq, jdbcTypes[type], "setAsciiStream");

                worked = true;

            } catch (SQLException sqle) {
                sqleResult = sqle;
                worked = false;
            }
            judge_setXXX(worked, sqleResult, 13, type);
        }

        {
            s.execute("DELETE FROM PM.TYPE_AS");

            SQLException sqleResult = null;
            boolean worked;
            try {
                // setAsciiStream(null)
                psi.setAsciiStream(1, null, 0);
                psi.executeUpdate();
                getValidValue(psq, jdbcTypes[type], "setAsciiStream");

                worked = true;

            } catch (SQLException sqle) {
                sqleResult = sqle;
                worked = false;
            }
            judge_setXXX(worked, sqleResult, 13, type);
        }
        {
            s.execute("DELETE FROM PM.TYPE_AS");

            SQLException sqleResult = null;
            boolean worked;
            try {
                // setAsciiStream(null) as batch
                psi.setAsciiStream(1, null, 0);
                psi.addBatch();
                psi.executeBatch();
                getValidValue(psq, jdbcTypes[type], "setAsciiStream");

                worked = true;

            } catch (SQLException sqle) {
                sqleResult = sqle;
                worked = false;
            }
            judge_setXXX(worked, sqleResult, 13, type);
        }
        {
            s.execute("DELETE FROM PM.TYPE_AS");

            SQLException sqleResult = null;
            boolean worked;
            try {
                // setBinaryStream()
                byte[] data = new byte[6];
                data[0] = (byte) 0x4;
                data[1] = (byte) 0x3;
                data[2] = (byte) 0xca;
                data[3] = (byte) 0xfe;
                data[4] = (byte) 0x00;
                data[5] = (byte) 0x32;

                psi.setBinaryStream(1, new java.io.ByteArrayInputStream(data),
                        6);
                psi.executeUpdate();
                getValidValue(psq, jdbcTypes[type], "setBinaryStream");

                worked = true;

            } catch (SQLException sqle) {
                sqleResult = sqle;
                worked = false;
            }
            judge_setXXX(worked, sqleResult, 14, type);
        }
        {
            s.execute("DELETE FROM PM.TYPE_AS");

            SQLException sqleResult = null;
            boolean worked;
            try {
                // setBinaryStream() as batch
                byte[] data = new byte[6];
                data[0] = (byte) 0x4;
                data[1] = (byte) 0x3;
                data[2] = (byte) 0xca;
                data[3] = (byte) 0xfe;
                data[4] = (byte) 0x00;
                data[5] = (byte) 0x32;

                psi.setBinaryStream(1, new java.io.ByteArrayInputStream(data),
                        6);
                psi.addBatch();
                psi.executeBatch();
                getValidValue(psq, jdbcTypes[type], "getBinaryStream");

                worked = true;

            } catch (SQLException sqle) {
                sqleResult = sqle;
                worked = false;
            }
            judge_setXXX(worked, sqleResult, 14, type);
        }

        {
            s.execute("DELETE FROM PM.TYPE_AS");

            SQLException sqleResult = null;
            boolean worked;
            try {
                // setBinaryStream(null)
                psi.setBinaryStream(1, null, 0);
                psi.executeUpdate();
                getValidValue(psq, jdbcTypes[type], "setBinaryStream");

                worked = true;

            } catch (SQLException sqle) {
                sqleResult = sqle;
                worked = false;
            }
            judge_setXXX(worked, sqleResult, 14, type);
        }
        {
            s.execute("DELETE FROM PM.TYPE_AS");

            SQLException sqleResult = null;
            boolean worked;
            try {
                // setBinaryStream(null) as batch
                psi.setBinaryStream(1, null, 0);
                psi.addBatch();
                psi.executeBatch();
                getValidValue(psq, jdbcTypes[type], "setBinaryStream");

                worked = true;

            } catch (SQLException sqle) {
                sqleResult = sqle;
                worked = false;
            }
            judge_setXXX(worked, sqleResult, 14, type);
        }

        {
            s.execute("DELETE FROM PM.TYPE_AS");

            SQLException sqleResult = null;
            boolean worked;
            try {
                // setCharacterStream()
                psi.setCharacterStream(1, new java.io.StringReader("89"), 2);
                psi.executeUpdate();
                getValidValue(psq, jdbcTypes[type], "setCharacterStream");

                worked = true;

            } catch (SQLException sqle) {
                sqleResult = sqle;
                worked = false;
            }
            judge_setXXX(worked, sqleResult, 15, type);
        }
        {
            s.execute("DELETE FROM PM.TYPE_AS");

            SQLException sqleResult = null;
            boolean worked;
            try {
                // setCharacterStream() as batch
                psi.setCharacterStream(1, new java.io.StringReader("89"), 2);
                psi.addBatch();
                psi.executeBatch();
                getValidValue(psq, jdbcTypes[type], "setCharacterStream");

                worked = true;

            } catch (SQLException sqle) {
                sqleResult = sqle;
                worked = false;
            }
            judge_setXXX(worked, sqleResult, 15, type);
        }

        {
            s.execute("DELETE FROM PM.TYPE_AS");

            SQLException sqleResult = null;
            boolean worked;
            try {
                // setCharacterStream(null)
                psi.setCharacterStream(1, null, 0);
                psi.executeUpdate();
                getValidValue(psq, jdbcTypes[type], "setCharacterStream");

                worked = true;

            } catch (SQLException sqle) {
                sqleResult = sqle;
                worked = false;
            }
            judge_setXXX(worked, sqleResult, 15, type);
        }
        {
            s.execute("DELETE FROM PM.TYPE_AS");

            SQLException sqleResult = null;
            boolean worked;
            try {
                // setCharacterStream(null) as batch
                psi.setCharacterStream(1, null, 0);
                psi.addBatch();
                psi.executeBatch();
                getValidValue(psq, jdbcTypes[type], "setCharacterStream");

                worked = true;

            } catch (SQLException sqle) {
                sqleResult = sqle;
                worked = false;
            }
            judge_setXXX(worked, sqleResult, 15, type);
        }

        {
            s.execute("DELETE FROM PM.TYPE_AS");

            SQLException sqleResult = null;
            boolean worked;
            try {
                // setClob()

                ResultSet rsc = s
                        .executeQuery("SELECT C FROM PM.LOB_GET WHERE ID = 1");
                rsc.next();
                Clob tester = rsc.getClob(1);
                rsc.close();

                psi.setClob(1, tester);
                psi.executeUpdate();
                getValidValue(psq, jdbcTypes[type], "setClob");

                worked = true;

            } catch (SQLException sqle) {
                sqleResult = sqle;
                worked = false;
            }
            judge_setXXX(worked, sqleResult, 16, type);
        }
        {
            s.execute("DELETE FROM PM.TYPE_AS");

            SQLException sqleResult = null;
            boolean worked;
            try {
                // setClob() as batch

                ResultSet rsc = s
                        .executeQuery("SELECT C FROM PM.LOB_GET WHERE ID = 1");
                rsc.next();
                Clob tester = rsc.getClob(1);
                rsc.close();

                psi.setClob(1, tester);
                psi.addBatch();
                psi.executeBatch();
                getValidValue(psq, jdbcTypes[type], "setClob");

                worked = true;

            } catch (SQLException sqle) {
                sqleResult = sqle;
                worked = false;
            }
            judge_setXXX(worked, sqleResult, 16, type);
        }

        {
            s.execute("DELETE FROM PM.TYPE_AS");

            SQLException sqleResult = null;
            boolean worked;
            try {
                // setClob(null)

                psi.setClob(1, null);
                psi.executeUpdate();
                getValidValue(psq, jdbcTypes[type], "setClob");

                worked = true;

            } catch (SQLException sqle) {
                sqleResult = sqle;
                worked = false;
            }
            judge_setXXX(worked, sqleResult, 16, type);
        }
        {
            s.execute("DELETE FROM PM.TYPE_AS");

            SQLException sqleResult = null;
            boolean worked;
            try {
                // setClob(null) as batch

                psi.setClob(1, null);
                psi.addBatch();
                psi.executeBatch();
                getValidValue(psq, jdbcTypes[type], "setClob");

                worked = true;

            } catch (SQLException sqle) {
                sqleResult = sqle;
                worked = false;
            }
            judge_setXXX(worked, sqleResult, 16, type);
        }
        {
            s.execute("DELETE FROM PM.TYPE_AS");
            SQLException sqleResult = null;
            boolean worked;
            try {
                // setBlob()

                ResultSet rsc = s
                        .executeQuery("SELECT B FROM PM.LOB_GET WHERE ID = 1");
                rsc.next();
                Blob tester = rsc.getBlob(1);
                rsc.close();

                psi.setBlob(1, tester);
                psi.executeUpdate();
                getValidValue(psq, jdbcTypes[type], "setBlob");

                worked = true;

            } catch (SQLException sqle) {
                sqleResult = sqle;
                worked = false;
            }
            judge_setXXX(worked, sqleResult, 17, type);
        }
        {
            s.execute("DELETE FROM PM.TYPE_AS");
            SQLException sqleResult = null;
            boolean worked;
            try {
                // setBlob() as batch

                ResultSet rsc = s
                        .executeQuery("SELECT B FROM PM.LOB_GET WHERE ID = 1");
                rsc.next();
                Blob tester = rsc.getBlob(1);
                rsc.close();

                psi.setBlob(1, tester);
                psi.addBatch();
                psi.executeBatch();
                getValidValue(psq, jdbcTypes[type], "setBlob");

                worked = true;

            } catch (SQLException sqle) {
                sqleResult = sqle;
                worked = false;
            }
            judge_setXXX(worked, sqleResult, 17, type);
        }
        {
            s.execute("DELETE FROM PM.TYPE_AS");
            SQLException sqleResult = null;
            boolean worked;
            try {
                // Blob(null)

                psi.setBlob(1, null);
                psi.executeUpdate();
                getValidValue(psq, jdbcTypes[type], "setBlob");

                worked = true;

            } catch (SQLException sqle) {
                sqleResult = sqle;
                worked = false;
            }
            judge_setXXX(worked, sqleResult, 17, type);
        }
        {
            s.execute("DELETE FROM PM.TYPE_AS");
            SQLException sqleResult = null;
            boolean worked;
            try {
                // setBlob(null) as batch

                psi.setBlob(1, null);
                psi.addBatch();
                psi.executeBatch();
                getValidValue(psq, jdbcTypes[type], "setBlob");

                worked = true;

            } catch (SQLException sqle) {
                sqleResult = sqle;
                worked = false;
            }
            judge_setXXX(worked, sqleResult, 17, type);
        }
        {
            s.execute("DELETE FROM PM.TYPE_AS");

            SQLException sqleResult = null;
            boolean worked;
            try {
                // setUnicodeStream()
                byte[] data = new byte[6];
                data[0] = (byte) 0x4;
                data[1] = (byte) 0x3;
                data[2] = (byte) 0xca;
                data[3] = (byte) 0xfe;
                data[4] = (byte) 0x00;
                data[5] = (byte) 0x32;

                try {
                    psi.setUnicodeStream(1, new java.io.ByteArrayInputStream(
                            data), 6);
                } catch (NoSuchMethodError e) {
                    // ResultSet.setUnicodeStream not present - correct for
                    // JSR169
                }

                if (JDBC.vmSupportsJDBC3()) {
                    psi.executeUpdate();
                    getValidValue(psq, jdbcTypes[type], "setUnicodeStream");
                }
                worked = true;

            } catch (SQLException sqle) {
                sqleResult = sqle;
                worked = false;
            }
            if (JDBC.vmSupportsJDBC3())
                judge_setXXX(worked, sqleResult, 14, type);
        }

        // DERBY-1938: Test setObject with null and no type specification.
        setXXX_setObjectNullNoTypeSpec(s, psi, psq, type);

        setXXX_setObject(s, psi, psq, type, validString[type], "java.lang.String", 0);
        
        if (HAVE_BIG_DECIMAL)
            setXXX_setObject(s, psi, psq, type, BigDecimal.valueOf(98L),
                    "java.math.BigDecimal", 1);
        setXXX_setObject(s, psi, psq, type, Boolean.TRUE, "java.lang.Boolean",
                2);

        // DERBY-1500: setObject() should work for Byte and Short too.
        setXXX_setObject(s, psi, psq, type, new Byte((byte) 98),
                "java.lang.Byte", 1);
        setXXX_setObject(s, psi, psq, type, new Short((short) 98),
                "java.lang.Short", 2);

        setXXX_setObject(s, psi, psq, type, new Integer(98),
                "java.lang.Integer", 3);
        setXXX_setObject(s, psi, psq, type, new Long(98), "java.lang.Long", 4);
        setXXX_setObject(s, psi, psq, type, new Float(98.0f),
                "java.lang.Float", 5);
        setXXX_setObject(s, psi, psq, type, new Double(98.0d),
                "java.lang.Double", 6);

        {
            byte[] data = { 0x4, 0x3 };
            setXXX_setObject(s, psi, psq, type, data, "byte[]", 7);
        }

        setXXX_setObject(s, psi, psq, type,
                java.sql.Date.valueOf("2004-02-14"), "java.sql.Date", 8);
        setXXX_setObject(s, psi, psq, type, java.sql.Time.valueOf("00:00:00"),
                "java.sql.Time", 9);
        setXXX_setObject(s, psi, psq, type, java.sql.Timestamp
                .valueOf("2004-02-14 00:00:00.0"), "java.sql.Timestamp", 10);
        s.getConnection().commit();

        // Test setObject with Blob
        {
            ResultSet rsc = s
                    .executeQuery("SELECT B FROM PM.LOB_GET WHERE ID = 1");
            rsc.next();
            Blob tester = rsc.getBlob(1);
            rsc.close();
            setXXX_setObject(s, psi, psq, type, tester, "java.sql.Blob", 11);
        }

        // Test setObject with Clob
        {
            ResultSet rsc = s
                    .executeQuery("SELECT C FROM PM.LOB_GET WHERE ID = 1");
            rsc.next();
            Clob tester = rsc.getClob(1);
            rsc.close();
            setXXX_setObject(s, psi, psq, type, tester, "java.sql.Clob", 12);
        }
    }

    /**
     * Do the {@code setObject()} tests for {@code setXXX()}. Test both for
     * the two-argument {@code setObject(int,Object)} method and the
     * three-argument {@code setObject(int,Object,int)} method.
     */
    private static void setXXX_setObject(Statement s, PreparedStatement psi,
            PreparedStatement psq, int type, Object value, String className,
            int b5o) throws SQLException, java.io.IOException {

        // Test setObject(int, Object)
        setXXX_setObject_doWork(
                s, psi, psq, type, value, className, b5o, false, false);

        // Test setObject(int, Object) with batch execution
        setXXX_setObject_doWork(
                s, psi, psq, type, value, className, b5o, false, true);

        // Test setObject(int, Object, int)
        setXXX_setObject_doWork(
                s, psi, psq, type, value, className, b5o, true, false);

        // Test setObject(int, Object, int) with batch execution
        setXXX_setObject_doWork(
                s, psi, psq, type, value, className, b5o, true, true);

    }

    /**
     * Helper method that does all the work for setXXX_setObject().
     *
     * @param withTypeFlag if true, use the setObject() method that takes a
     * type parameter; otherwise, use the two-argument type-less setObject()
     * method
     * @param batchExecution if true, do batch execution; otherwise, do
     * normal execution
     */
    private static void setXXX_setObject_doWork(
            Statement s, PreparedStatement psi, PreparedStatement psq,
            int type, Object value, String className, int b5o,
            boolean withTypeFlag, boolean batchExecution)
        throws SQLException, IOException
    {
        int jdbcType = jdbcTypes[type];
        String method = "setObject(" + className + ")";

        s.execute("DELETE FROM PM.TYPE_AS");

        SQLException sqleResult = null;
        boolean worked;
        try {
            // Set the parameter value, either with or without explicit type
            if (withTypeFlag) {
                psi.setObject(1, value, jdbcType);
            } else {
                psi.setObject(1, value);
            }

            // Execute the statement, either single execution or batch
            if (batchExecution) {
                psi.addBatch();
                psi.executeBatch();
            } else {
                psi.executeUpdate();
            }

            // Check if we got a valid value back
            getValidValue(psq, jdbcType, method);
            worked = true;
        } catch (SQLException sqle) {
            sqleResult = sqle;
            worked = false;
        }

        // Check if the we got the correct response
        judge_setObject(worked, sqleResult, b5o, type);
    }

    /**
     * Passes Java null to the setObject-call, expecting the driver to set the
     * column value to SQL NULL.
     * <p>
     * This behavior was allowed/introduced by DERBY-1938.
     *
     * @param s statement used for auxiliary tasks
     * @param psi statement used for insert
     * @param psq statement used for query (retrieving inserted value)
     * @param type the type of the column
     */
    private static void setXXX_setObjectNullNoTypeSpec(
            Statement s, PreparedStatement psi, PreparedStatement psq,
            int type) throws SQLException, IOException {
        // setObject(null) - see DERBY-1938
        s.execute("DELETE FROM PM.TYPE_AS");

        // setObject(null)
        psi.setObject(1, null);
        psi.executeUpdate();
        getValidValue(psq, jdbcTypes[type], "setObject");

        s.execute("DELETE FROM PM.TYPE_AS");

        // setObject(null) as batch
        psi.setObject(1, null);
        psi.addBatch();
        psi.executeBatch();
        getValidValue(psq, jdbcTypes[type], "setObject");
    }

    private static void unexpectedException(SQLException sqle) {

        fail("FAIL unexpected exception - ");
        showException(sqle);
        sqle.printStackTrace(System.out);
    }

    private static void showException(SQLException sqle) {
        do {
            String state = sqle.getSQLState();
            if (state == null)
                state = "?????";

            String msg = sqle.getMessage();
            if (msg == null)
                msg = "?? no message ??";
            sqle.printStackTrace();
            fail(" (" + state + "):" + msg);
            sqle = sqle.getNextException();
        } while (sqle != null);
    }

    private static boolean setValidValue(PreparedStatement ps, int param,
            int jdbcType) throws SQLException {

        switch (jdbcType) {
        case Types.BIT:
        case Types.BOOLEAN:
            ps.setBoolean(param, true);
            return true;
        case Types.TINYINT:
            ps.setByte(param, (byte) 32);
            return true;
        case Types.SMALLINT:
            ps.setShort(param, (short) 32);
            return true;
        case Types.INTEGER:
            ps.setInt(param, 32);
            return true;
        case Types.BIGINT:
            ps.setLong(param, 32L);
            return true;
        case Types.REAL:
            ps.setFloat(param, 32.0f);
            return true;
        case Types.FLOAT:
        case Types.DOUBLE:
            ps.setDouble(param, 32.0);
            return true;
        case Types.DECIMAL:
            BigDecimalHandler.setBigDecimalString(ps, param, "32.0");
            return true;
        case Types.CHAR:
        case Types.VARCHAR:
        case Types.LONGVARCHAR:
            ps.setString(param, "32");
            return true;
        case Types.BINARY:
        case Types.VARBINARY: {
            byte[] data = { (byte) 0x04, (byte) 0x03, (byte) 0xfd, (byte) 0xc3,
                    (byte) 0x73 };
            ps.setBytes(param, data);
            return true;
        }
            // Types.LONGVARBINARY:
        case Types.DATE:
            ps.setDate(param, java.sql.Date.valueOf("2004-02-14"));
            return true;
        case Types.TIME:
            ps.setTime(param, java.sql.Time.valueOf("17:14:24"));
            return true;
        case Types.TIMESTAMP:
            ps.setTimestamp(param, java.sql.Timestamp
                    .valueOf("2004-02-14 17:14:24.097625551"));
            return true;
        case Types.CLOB:
            // JDBC 3.0 spec section 16.3.2 explictly states setCharacterStream
            // is OK for setting a CLOB
            ps.setCharacterStream(param, new java.io.StringReader("67"), 2);
            return true;
        case Types.BLOB:
            // JDBC 3.0 spec section 16.3.2 explictly states setBinaryStream is
            // OK for setting a BLOB
        {
            byte[] data = new byte[6];
            data[0] = (byte) 0x82;
            data[1] = (byte) 0x43;
            data[2] = (byte) 0xca;
            data[3] = (byte) 0xfe;
            data[4] = (byte) 0x00;
            data[5] = (byte) 0x32;

            ps
                    .setBinaryStream(param, new java.io.ByteArrayInputStream(
                            data), 6);
            return true;
        }
        default:
            return false;
        }
    }

    private static boolean getValidValue(PreparedStatement ps, int jdbcType,
            String method) throws SQLException, IOException {

        ResultSet rs = ps.executeQuery();
        rs.next();

        switch (jdbcType) {
        case Types.SMALLINT: {
            short val = rs.getShort(1);
            boolean wn = rs.wasNull();

            if (wn)
                assertEquals(0, val);
            else if (isBooleanMethod(method))
                assertEquals(1, val);
            else
                assertEquals(98, val);
            return true;
        }
        case Types.INTEGER: {
            int val = rs.getInt(1);
            boolean wn = rs.wasNull();
            if (wn)
                assertEquals(0, val);
            else if (isBooleanMethod(method))
                assertEquals(1, val);
            else
                assertEquals(98, val);
            return true;
        }
        case Types.BIGINT: {
            long val = rs.getLong(1);
            boolean wn = rs.wasNull();
            if (wn)
                assertEquals(0, val);
            else if (isBooleanMethod(method))
                assertEquals(1, val);
            else
                assertEquals(98, val);
            return true;
        }
        case Types.REAL: {
            float val = rs.getFloat(1);
            boolean wn = rs.wasNull();
            if (wn)
                assertEquals(0.0, val, .001);
            else if (isBooleanMethod(method))
                assertEquals(1.0, val, .001);
            else if (method.equals("setFloat"))
                assertEquals(98.4, val, .001);
            else if (method.equals("setDouble"))
                assertEquals(98.5, val, .001);
            else
                assertEquals(98.0, val, .001);
            return true;
        }
        case Types.FLOAT:
        case Types.DOUBLE: {
            double val = rs.getDouble(1);
            boolean wn = rs.wasNull();
            if (wn)
                assertEquals(0.0, val, .001);
            else if (isBooleanMethod(method))
                assertEquals(1.0, val, .001);
            else if (method.equals("setFloat"))
                assertEquals(98.4, val, .001);
            else if (method.equals("setDouble"))
                assertEquals(98.5, val, .001);
            else
                assertEquals(98.0, val, .001);
            return true;
        }
        case Types.DECIMAL: {
            String val = BigDecimalHandler.getBigDecimalString(rs, 1);
            boolean wn = rs.wasNull();
            if (wn)
                assertNull(val);
            else if (isBooleanMethod(method))
                assertEquals("1.00000", val);
            else if (method.equals("setFloat"))
                assertEquals("98.40000", val);
            else if (method.equals("setDouble"))
                assertEquals("98.50000", val);
            else
                assertEquals("98.00000", val);
            return true;
        }
        case Types.CHAR:
        case Types.VARCHAR:
        case Types.LONGVARCHAR: {
            String s = rs.getString(1);
            boolean wn = rs.wasNull();
            if (wn)
                assertNull(s);
            else {
                // With IBM's DB2 universal driver.
                // Setting a java.sql.Clob value works with
                // a character column but sets the value to
                // be the object's toString. This is probably a bug with JCC.
                if (s.startsWith("com.ibm.db2.jcc.")
                        || s.startsWith("org.apache.derby.client"))
                    s = "<OBJECT.toString()>";

                boolean hasNonAscii = false;
                // check for any characters in the control range
                for (int si = 0; si < s.length(); si++) {
                    char c = s.charAt(si);
                    if (c < (char) 0x20 || c >= (char) 0x7f) {
                        hasNonAscii = true;
                        break;
                    }
                }

                if (hasNonAscii) {
                    StringBuffer sb = new StringBuffer();

                    sb.append("EncodedString: >");
                    for (int si = 0; si < s.length(); si++) {
                        sb.append(' ');
                        sb.append((int) s.charAt(si));
                    }
                    sb.append(" <");
                    s = sb.toString();

                }
                checkValidStringValue(method, s);
            }
            return true;
        }
        case Types.BINARY:
        case Types.VARBINARY: {
            byte[] data = rs.getBytes(1);
            boolean wn = rs.wasNull();
            if (wn)
                assertNull(data);
            else
                assertEquals("0x4,0x3", showFirstTwo(data));
            return true;
        }
        case Types.LONGVARBINARY: {
            InputStream is = rs.getBinaryStream(1);
            boolean wn = rs.wasNull();
            if (wn)
                assertNull(is);
            else
                assertEquals("0x4,0x3", showFirstTwo(is));
            return true;
        }
        case Types.DATE: {
            Date d = rs.getDate(1);
            boolean wn = rs.wasNull();
            if (wn)
                assertNull(d);
            else
                assertEquals(Date.valueOf("2004-02-14"), d);
            return true;
        }
        case Types.TIME: {
            Time t = rs.getTime(1);
            boolean wn = rs.wasNull();
            if (wn)
                assertNull(t);
            else
                assertEquals(Time.valueOf("00:00:00"), t);
            return true;

        }
        case Types.TIMESTAMP: {
            Timestamp ts = rs.getTimestamp(1);
            boolean wn = rs.wasNull();
            if (wn)
                assertNull(rs.getTimestamp(1));
            else
                assertEquals(Timestamp.valueOf("2004-02-14 00:00:00.0"), ts);
            return true;
        }
        case Types.CLOB: {
            Clob clob = rs.getClob(1);
            boolean wn = rs.wasNull();
            if (wn)
               assertNull(clob);
            else {
                char[] charray = new char[20];
                int numchar = clob.getCharacterStream().read(charray);
                String s = new String(charray,0,numchar);
                if ("setString".equals(method))
                    assertEquals("98",s);
                else if ("setAsciiStream".equals(method))
                    assertEquals("eg012d", s);
                else if ("setCharacterStream".equals(method))
                    assertEquals("89",s);
                else if ("setClob".equals(method))
                    assertEquals("72",s);
                else if ("setObject(java.lang.String)".equals(method))
                    assertEquals("98",s);
                else if ("setObject(java.lang.Clob)".equals(method))
                    assertEquals("72",s);
            }
            return true;
        }
        case Types.BLOB: {
            Blob blob = rs.getBlob(1);
            boolean wn = rs.wasNull();
            if (wn)
              assertNull(blob);
            else {
                assertEquals("0x4,0x3", showFirstTwo(blob.getBinaryStream())); 
            }
            return true;
        }
        case Types.BOOLEAN: {
            boolean b = rs.getBoolean(1);
            boolean wn = rs.wasNull();
            if (wn) {
                assertFalse(b);
            } else {
                assertTrue(b);
            }
            return true;
        }
        default:
            fail("FAIL JDBC TYPE IN getValidValue "
                    + JDBC.sqlNameFromJdbc(jdbcType));
            return false;
        }
    }

    private static void checkValidStringValue(String method, String s) {
        s = s.trim();
        if ("setBoolean".equals(method) ||
                "setObject(java.lang.Boolean)".equals(method) )
            assertEquals("1",s);
        else if ("setBytes".equals(method) ||
                ("setObject(byte[])".equals(method)))
            assertEquals("EncodedString: > 1027 ",s.substring(0,22));
        else if ("setFloat".equals(method))
            assertEquals("98.4", s);
        else if ("setDouble".equals(method))
            assertEquals("98.5",s);
        else if ("setDate".equals(method) ||
                "setObject(java.sql.Date)".equals(method))
            assertEquals("2004-02-14", s);
        else if ("setTime".equals(method) ||
                "setObject(java.sql.Time)".equals(method))
            assertEquals("00:00:00",s);
        else if ("setTimestamp".equals(method)||
                "setObject(java.sql.Timestamp)".equals(method))
            assertEquals("2004-02-14 00:00:00.0",s);
        else if ("setAsciiStream".equals(method))
            assertEquals("eg012d",s);
        else if ("setCharacterStream".equals(method))
            assertEquals("89",s);
        else if ("setObject(java.lang.Float)".equals(method) ||
                "setObject(java.lang.Double)".equals(method))
               assertEquals("98.0",s);
        else
            assertEquals("98",s.trim());
    }

    private static boolean isBooleanMethod(String method) {
        return method.equals("setBoolean")
                || method.equals("setObject(java.lang.Boolean)");
    }

    private static boolean getOutValue(CallableStatement cs, int param,
            int regJdbcType, int paramType) throws SQLException, IOException {
        
        int paramJdbcType= jdbcTypes[paramType];
        switch (regJdbcType) {
        case Types.BIT:
        case Types.BOOLEAN:
        {
            boolean val = cs.getBoolean(param);
            boolean wn = cs.wasNull();
            if (!wn)
                assertTrue(val);

            return true;
        }
        case Types.TINYINT: {
            // Check out and inout params for procedures
            byte val = cs.getByte(param);
            boolean wn = cs.wasNull();
            if (!wn)
                checkProcedureOutput(param, paramType, val);
            return true;
        }
            
        case Types.SMALLINT: {
            short val = cs.getShort(param);
            boolean wn = cs.wasNull();
            if (!wn)
                checkProcedureOutput(param, paramType, val);
            return true;
        }
    case Types.INTEGER: {
            int val = cs.getInt(param);
            boolean wn = cs.wasNull();
            if (!wn)
                checkProcedureOutput(param, paramType, val);
            return true;
    }
        case Types.BIGINT: {
            long val = cs.getLong(param);
            boolean wn = cs.wasNull();
            if(!wn)
                checkProcedureOutput(param, paramType, val);
            return true;
        }
        case Types.REAL: {
            float val = cs.getFloat(param);
            boolean wn = cs.wasNull();
            if(!wn)
                checkProcedureOutput(param, paramType, val);
            return true;
        }
        case Types.FLOAT:
        case Types.DOUBLE: {
            double val = cs.getDouble(param);
            boolean wn = cs.wasNull();
            if (!wn)
                checkProcedureOutput(param, paramType, val);
            return true;
        }       
        case Types.DECIMAL: {
           String val = BigDecimalHandler.getBigDecimalString(cs, param, regJdbcType);
           boolean wn = cs.wasNull();
           if (!wn)
               checkProcedureOutput(param,paramType,val);
           return true;
        }
        case Types.CHAR:
        case Types.VARCHAR:
        case Types.LONGVARCHAR: {
            String val = cs.getString(param);
            boolean wn = cs.wasNull();
            if (!wn)
                checkProcedureOutput(param,paramType,val.trim());
            return true;
        }
        case Types.BINARY:
        case Types.VARBINARY:
        case Types.LONGVARBINARY: {
            byte[] data = cs.getBytes(param);
            boolean wn = cs.wasNull();
            if (!wn)
                checkProcedureOutput(param,paramType,data);
            return true;
        }

        case Types.DATE: {
            Date val = cs.getDate(param);
            boolean wn = cs.wasNull();
            if (!wn)
                checkProcedureOutput(param,paramType,val);            
            return true;
        }
        case Types.TIME: {
            Time val = cs.getTime(param);
            boolean wn = cs.wasNull();
            if (!wn)
                checkProcedureOutput(param,paramType,val);
            return true;
        }
        case Types.TIMESTAMP: {
            Timestamp val = cs.getTimestamp(param);
            boolean wn = cs.wasNull();
            if (!wn)
                checkProcedureOutput(param,paramType,val);
            return true;
        }
        case Types.CLOB: {
            // clob not allowed for procedures
            Clob clob = cs.getClob(param);
            boolean wn = cs.wasNull();
            return true;
        }
        case Types.BLOB: {
            // blob not allowed for procedures
            Blob blob = cs.getBlob(param);
            boolean wn = cs.wasNull();
            return true;
        }
        default:
            fail("FAIL JDBC TYPE IN getOutValue "
                    + JDBC.sqlNameFromJdbc(regJdbcType));
            return false;
        }
    }

    private static void checkProcedureOutput(int param, int paramType, byte val)
    {
        checkProcedureOutput(param,paramType,(long) val);
    }
    
    private static void checkProcedureOutput(int param, int paramType, short val)
    {
        checkProcedureOutput(param,paramType,(long) val);
    }

    
    private static void checkProcedureOutput(int param, int paramType, int val)
    {
        checkProcedureOutput(param,paramType,(long) val);
    }

    
    private static void checkProcedureOutput(int param, int paramType, long val) {
        switch (jdbcTypes[paramType]) {
        case java.sql.Types.SMALLINT:
           if (param == 2)
               assertEquals(38,val);
           else if (param == 3)
               assertEquals(77,val);
           break;
        case java.sql.Types.INTEGER:
            if (param == 2)
                assertEquals(41,val);
            else if (param == 3)
                assertEquals(88,val);
        break;
        case java.sql.Types.BIGINT:
            if (param == 2)
                assertEquals(40,val);
            else if (param == 3)
                assertEquals(99,val);
            break;
        case java.sql.Types.FLOAT:
            if (param == 2)
                assertEquals(35,val);
            else if (param == 3)
                assertEquals(66,val);
            break;
        case java.sql.Types.REAL:
            if (param == 2)
                assertEquals(41,val);
            else if (param == 3)
                assertEquals(88,val);
            break;
        case java.sql.Types.DECIMAL:
            if (param == 2)
                assertEquals(34,val);
            else if (param == 3)
                assertEquals(84,val);
            break;
        case java.sql.Types.DOUBLE:
            if (param == 2)
                assertEquals(35,val);
            else if (param == 3)
                assertEquals(66,val);
            break;
        }
    }
    
    private static void checkProcedureOutput(int param, int paramType, float val) {
        checkProcedureOutput(param,paramType, (double) val);
    }
    
    private static void checkProcedureOutput(int param, int paramType, double val) {
        switch (jdbcTypes[paramType]) {
        case java.sql.Types.SMALLINT:
           if (param == 2)
               assertEquals(38.0,val,.00001);
           else if (param == 3)
               assertEquals(77.0,val,.00001);
           break;
        case java.sql.Types.INTEGER:
            if (param == 2)
                assertEquals(41.0,val,.00001);
            else if (param == 3)
                assertEquals(88.0,val, .00001);
        break;
        case java.sql.Types.BIGINT:
            if (param == 2)
                assertEquals(40.0,val,.00001);
            else if (param == 3)
                assertEquals(99.0,val,.00001);
            break;
        case java.sql.Types.FLOAT:
            if (param == 2)
                assertEquals(35.9,val,.00001);
            else if (param == 3)
                assertEquals(66.8,val,.00001);
            break;
        case java.sql.Types.REAL:
            if (param == 2)
                assertEquals(41.9,val,.00001);
            else if (param == 3)
                assertEquals(88.8,val,.00001);
            break;
        case java.sql.Types.DECIMAL:
            if (param == 2)
                assertEquals(34.29999,val,.0001);
            else if (param == 3)
                assertEquals(84.09999,val,.0001);
            break;
        case java.sql.Types.DOUBLE:
            if (param == 2)
                assertEquals(35.9,val,.00001);
            else if (param == 3)
                assertEquals(66.8,val,.00001);
            break;
        }
    }

    
    private static void checkProcedureOutput(int param, int paramType, String val) {
        switch (jdbcTypes[paramType]) {
        case java.sql.Types.SMALLINT:
           if (param == 2)
               assertEquals("38",val);
           
           else if (param == 3)
               assertEquals("77",val);
           break;
        case java.sql.Types.INTEGER:
            if (param == 2)
                assertEquals("41",val);
            else if (param == 3)
                assertEquals("88",val);
        break;
        case java.sql.Types.BIGINT:
            if (param == 2)
                assertEquals("40",val);
            else if (param == 3)
                assertEquals("99",val);
            break;
        case java.sql.Types.FLOAT:
            if (param == 2)
                assertEquals("35.9",val);
            else if (param == 3)
                assertEquals("66.8",val);
            break;
        case java.sql.Types.REAL:
            if (param == 2)
                assertEquals("41.9",val);
            else if (param == 3)
                assertEquals("88.8",val);
            break;
        case java.sql.Types.DECIMAL:
            if (param == 2)
                assertEquals("34.29999",val);
            else if (param == 3)
                assertEquals("84.09999",val);
            break;
        case java.sql.Types.DOUBLE:
            if (param == 2)
                assertEquals("35.9",val);
            else if (param == 3)
                assertEquals("66.8",val);
            break;
        }
    }

    private static void checkProcedureOutput(int param, int paramType, byte[] val) {
        boolean isBlob =  ( jdbcTypes[ paramType ] == Types.BLOB );
        if (param == 2)
        {
            if ( isBlob )
            {
                assertEquals("0x82,0x43",showFirstTwo(val));
            }
            else
            {
                assertEquals("0x4,0x3",showFirstTwo(val));
            }
        }
        else if (param == 3)
        {
            if ( isBlob )
            {
                assertEquals("0x1,0x2",showFirstTwo(val));
            }
            else
            {
                assertEquals("0x9,0xfe",showFirstTwo(val));
            }
        }
    }
    
    private static void checkProcedureOutput(int param, int paramType, Date val) {
        switch (jdbcTypes[paramType]) {
        case java.sql.Types.DATE:
            if (param == 2)
                assertEquals("2004-03-08", val.toString());
            else if (param == 3)
                assertEquals("2005-03-08", val.toString());
            break;
        case java.sql.Types.TIMESTAMP:
            if (param == 2)
                assertEquals("2004-03-12", val.toString());
            else if (param == 3)
                assertEquals("2004-04-12", val.toString());
            break;
        }
    }
    
    private static void checkProcedureOutput(int param, int paramType, Time val) {
        switch (jdbcTypes[paramType]) {
        case java.sql.Types.TIME:
            if (param == 2)
                assertEquals("19:44:42", val.toString());
            else if (param == 3)
                assertEquals("20:44:42", val.toString());
            break;
        case java.sql.Types.TIMESTAMP:
            if (param == 2)
                assertEquals("21:14:24", val.toString());
            else if (param == 3)
                assertEquals("04:25:26", val.toString());
            break;
        }
    }
    private static void checkProcedureOutput(int param, int paramType, Timestamp val) {
        switch (jdbcTypes[paramType]) {
        case java.sql.Types.DATE:
            if (param == 2)
                assertEquals("2004-03-08 00:00:00.0",val.toString());
            else if (param == 3)
                assertEquals("2005-03-08 00:00:00.0", val.toString());
            break;
        case java.sql.Types.TIME:
            // getTimestamp on time will use the current date, so can't check it explicitly
            // just check not null  
            assertNotNull(val);
            break;
        case java.sql.Types.TIMESTAMP:
            if (param == 2)
                assertEquals("2004-03-12 21:14:24.938222433", val.toString());
            else if (param == 3)
                assertEquals("2004-04-12 04:25:26.462983731", val.toString());
           break;
        }
    }

    static void dumpSQLExceptions(SQLException se) {

        while (se != null) {
            System.out.println("SQLSTATE(" + se.getSQLState() + "): "
                    + se.toString());
            se = se.getNextException();
        }
    }

    /**
     * Test for DERBY-149 fix Check that setString to an invalid value throws an
     * exception rather than causing a hang
     * 
     * @param type
     *            type for SQLTypes array
     * @param psi -
     *            insert prepared statement.
     * 
     */
    private static void testSetStringInvalidValue(int type,
            PreparedStatement psi) {
        // Do not perform this test for string types.
        // Only test for types wich will fail with setString("InvalidValue");
        switch (jdbcTypes[type]) {
        case Types.CHAR:
        case Types.VARCHAR:
        case Types.LONGVARCHAR:
        case Types.CLOB:
            return;
        }

        String sqlType = SQLTypes[type];
        try {
            psi.setString(1, "Invalid Value");
            psi.executeUpdate();
            // Should have gotten exception. Test fails
            String error = "FAIL - setString(1,\"Invalld Value\") for type "
                    + sqlType + " did not throw an exception as expected";
        } catch (SQLException sqle) {

            if ("22018".equals(sqle.getSQLState())
                    || "XCL12".equals(sqle.getSQLState())
                    || "22007".equals(sqle.getSQLState())
                    || "22005".equals(sqle.getSQLState())
                    || (sqle.getMessage().indexOf("Invalid data conversion") != -1)
                    || (sqle.getMessage().indexOf("Illegal Conversion") != -1))
                ;
            // System.out.println(" IC (Expected)");
            else
                fail("FAIL:" + sqle.getMessage());
        } catch (Exception e) {
            fail("FAIL: Unexpected Exception " + e.getMessage());
        }
    }

    private static String showFirstTwo(java.io.Reader in)
            throws java.io.IOException {

        int b1 = in.read();
        int b2 = in.read();
        in.close();

        return "0x" + Integer.toHexString(b1) + "," + "0x"
                + Integer.toHexString(b2);
    }

    private static String showFirstTwo(java.io.InputStream in)
            throws java.io.IOException {

        int b1 = in.read();
        int b2 = in.read();
        in.close();

        return "0x" + Integer.toHexString(b1) + "," + "0x"
                + Integer.toHexString(b2);
    }

    private static String showFirstTwo(byte[] data) {

        int b1 = data[0];
        int b2 = data[1];

        return "0x" + Integer.toHexString(((int) b1) & 0xff) + "," + "0x"
                + Integer.toHexString(((int) b2) & 0xff);
    }
    
    public static Test suite() {
        
        // Don't run for JSR169 until DERBY-2403 is resolved.
        if (JDBC.vmSupportsJDBC3())
            return TestConfiguration.defaultSuite(ParameterMappingTest.class);
        else
            return  new TestSuite("ParameterMapping");
    }
        /*
        ** Procedures for parameter mapping testing.
        */

        public static void pmap(short in, short[] inout, short[] out) {

                inout[0] += 6;
                out[0] = 77;
        }
        public static void pmap(int in, int[] inout, int[] out) {
                inout[0] += 9;
                out[0] = 88;

        }
        public static void pmap(boolean in, boolean[] inout, boolean[] out) {
            inout[0] = true;
            out[0] = true;
        }
        public static void pmap(long in, long[] inout, long[] out) {
                inout[0] += 8;
                out[0] = 99;
        }
        public static void pmap(float in, float[] inout, float[] out) {
                inout[0] += 9.9f;
                out[0] = 88.8f;
        }
        public static void pmap(double in, double[] inout, double[] out) {
                inout[0] += 3.9;
                out[0] = 66.8;
        }
        public static void pmap(byte[] in, byte[][] inout, byte[][] out) {

                inout[0][2] = 0x56;
                out[0] = new byte[4];
                out[0][0] = (byte) 0x09;
                out[0][1] = (byte) 0xfe;
                out[0][2] = (byte) 0xed;
                out[0][3] = (byte) 0x02;

        }
        public static void pmap(Date in, Date[] inout, Date[] out) {

                inout[0] = java.sql.Date.valueOf("2004-03-08");
                out[0] = java.sql.Date.valueOf("2005-03-08");

        }
        public static void pmap(Time in, Time[] inout, Time[] out) {
                inout[0] = java.sql.Time.valueOf("19:44:42");
                out[0] = java.sql.Time.valueOf("20:44:42");
        }
        public static void pmap(Timestamp in, Timestamp[] inout, Timestamp[] out) {

                inout[0] = java.sql.Timestamp.valueOf("2004-03-12 21:14:24.938222433");
                out[0] = java.sql.Timestamp.valueOf("2004-04-12 04:25:26.462983731");
        }
        public static void pmap(String in, String[] inout, String[] out) {
                inout[0] = inout[0].trim().concat("P2-PMAP");
                out[0] = "P3-PMAP";
        }

        /*
        ** Procedure which uses BigDecimal - for parameter mapping testing.
        */

        public static void pmap(BigDecimal in, BigDecimal[] inout, BigDecimal[] out) {
                inout[0] = inout[0].add(new BigDecimal(2.3));
                out[0] = new BigDecimal(84.1);
        }

    /*
    ** Procedures which use LOBs - for parameter mapping testing.
    */

    public static void pmap(Blob in, Blob[] inout, Blob[] out) throws SQLException {
        int    leftLength = (int) in.length();
        int    rightLength = (int) inout[0].length();
        byte[] left = in.getBytes( 1L, leftLength );
        byte[] right = inout[0].getBytes( 1L, rightLength );
        byte[] retval = new byte[ leftLength + rightLength ];
        System.arraycopy( left, 0, retval, 0, leftLength );
        System.arraycopy( right, 0, retval, leftLength, rightLength );
        inout[0] = new HarmonySerialBlob( retval );
        
        out[0] = new HarmonySerialBlob( new byte[] { (byte) 1, (byte) 2, (byte) 3 } );
    }

    public static void pmap(Clob in, Clob[] inout, Clob[] out) throws SQLException {
        inout[0] = new HarmonySerialClob( in.getSubString( 1L, (int) in.length() ) + inout[0].getSubString( 1L, (int) inout[0].length() ) );
        out[0] = new HarmonySerialClob( "abc" );
    }

    //
    // Clob procs
    //
    
    public static void clobIn( Clob c, String[] result ) throws SQLException
    {
        result[ 0 ] = getClobValue( c );
    }
    public static void clobOut( Clob[] c ) throws SQLException
    {
        c[ 0 ] = new HarmonySerialClob( "abc" );
    }
    public static void clobInOut( Clob[] c ) throws SQLException
    {
        String value = getClobValue( c[ 0 ] );
        
        char[] inValue = value.toCharArray();
        char[] outValue = reverse( inValue );

        c[ 0 ] = new HarmonySerialClob( new String( outValue ) );
    }

    private static String getClobValue( Clob c ) throws SQLException
    {
        return c.getSubString( 1L, (int) c.length() );
    }
    private static char[] reverse( char[] in )
    {
        int count = in.length;

        char[] retval = new char[ count ];
        for ( int i = 0; i < count; i++ ) { retval[ i ] = in[ (count - i) - 1 ]; }

        return retval;
    }
    
    //
    // Blob procs
    //
    
    public static void blobIn( Blob c, String[] result ) throws Exception
    {
        result[ 0 ] = getBlobValue( c );
    }
    public static void blobOut( Blob[] c ) throws Exception
    {
        c[ 0 ] = new HarmonySerialBlob( "abc".getBytes( UTF8 ) );
    }
    public static void blobInOut( Blob[] c ) throws Exception
    {
        String value = getBlobValue( c[ 0 ] );
        
        char[] inValue = value.toCharArray();
        char[] outValue = reverse( inValue );

        c[ 0 ] = new HarmonySerialBlob( (new String( outValue )).getBytes( UTF8 ) );
    }

    private static String getBlobValue( Blob c ) throws Exception
    {
        byte[] bytes = c.getBytes( 1L, (int) c.length() );

        return new String( bytes, UTF8 );
    }
    
    //
    // Debug helpers
    //
    
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
     * Prepare a call statement and report its sql text.
     */
    protected CallableStatement   chattyPrepareCall( Connection conn, String text )
        throws SQLException
    {
        println( "Preparing statement:\n\t" + text );
        
        return conn.prepareCall( text );
    }

    /**
     * Assert that the statement text, when compiled, raises an exception
     */
    protected void    expectCompilationError( String sqlState, String query )
    {
        println( "\nExpecting " + sqlState + " when preparing:\n\t" + query );

        assertCompileError( sqlState, query );
    }
}


