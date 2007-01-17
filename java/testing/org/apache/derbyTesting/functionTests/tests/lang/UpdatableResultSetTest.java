/*
 
   Derby - Class 
       org.apache.derbyTesting.functionTests.lang.UpdatableResultSetTest
 
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

import java.io.UnsupportedEncodingException;
import java.util.Arrays;

import org.apache.derbyTesting.functionTests.util.BigDecimalHandler;

import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.TestConfiguration;

import junit.framework.*;
import java.sql.*;


/**
 * This tests JDBC 2.0 updateable resultset - deleteRow, updateRow, and
 * insertRow API
 */
public class UpdatableResultSetTest  extends BaseJDBCTestCase {
        
    private static String[] allUpdateXXXNames =
    {
        "updateShort",
        "updateInt",
        "updateLong",
        "updateBigDecimal",
        "updateFloat",
        "updateDouble",
        "updateString",
        "updateAsciiStream",
        "updateCharacterStream",
        "updateByte",
        "updateBytes",
        "updateBinaryStream",
        "updateClob",
        "updateDate",
        "updateTime",
        "updateTimestamp",
        "updateBlob",
        "updateBoolean",
        "updateNull",
        "updateArray",
        "updateRef"
    };
    
    // test all the supported SQL datatypes using updateXXX methods
    private static String[] allSQLTypes =
    {
        "SMALLINT",
        "INTEGER",
        "BIGINT",
        "DECIMAL(10,5)",
        "REAL",
        "DOUBLE",
        "CHAR(60)",
        "VARCHAR(60)",
        "LONG VARCHAR",
        "CHAR(2) FOR BIT DATA",
        "VARCHAR(2) FOR BIT DATA",
        "LONG VARCHAR FOR BIT DATA",
        "CLOB(1k)",
        "DATE",
        "TIME",
        "TIMESTAMP",
        "BLOB(1k)",
    };
    
    // names for column names to test all the supported SQL datatypes using 
    // updateXXX methods
    private static String[] ColumnNames =
    {
        "SMALLINTCOL",
        "INTEGERCOL",
        "BIGINTCOL",
        "DECIMALCOL",
        "REALCOL",
        "DOUBLECOL",
        "CHARCOL",
        "VARCHARCOL",
        "LONGVARCHARCOL",
        "CHARFORBITCOL",
        "VARCHARFORBITCOL",
        "LVARCHARFORBITCOL",
        "CLOBCOL",
        "DATECOL",
        "TIMECOL",
        "TIMESTAMPCOL",
        "BLOBCOL",
    };
    
    // data to test all the supported SQL datatypes using updateXXX methods
    private static String[][]SQLData =
    {
        {"11","22"},                        // SMALLINT
        {"111","1111"},                     // INTEGER
        {"22","222"},                       // BIGINT
        {"3.3","3.33"},                     // DECIMAL(10,5)
        {"4.4","4.44"},                     // REAL,
        {"5.5","5.55"},                     // DOUBLE
        {"'1992-01-06'","'1992'"},          // CHAR(60)
        {"'1992-01-07'","'1992'"},          // VARCHAR(60),
        {"'1992-01-08'","'1992'"},          // LONG VARCHAR
        {"X'10'","X'10aa'"},                // CHAR(2)  FOR BIT DATA
        {"X'10'","X'10bb'"},                // VARCHAR(2) FOR BIT DATA
        {"X'10'","X'10cc'"},                // LONG VARCHAR FOR BIT DATA
        {"'13'","'14'"},                    // CLOB(1k)
        {"'2000-01-01'","'2000-01-01'"},    // DATE
        {"'15:30:20'","'15:30:20'"},        // TIME
        {"'2000-01-01 15:30:20'","'2000-01-01 15:30:20'"}, // TIMESTAMP
        {"X'1020'","X'10203040'"}           // BLOB
    };
    
    // This table contains the expected result of the combination of datatype
    // and updateXXX method on the embedded driver. If the call to the updateXXX
    // method fails the cell contains the expected SQLState and if it passes the 
    // cell contains PASS.
    public static final String[][]  updateXXXRulesTableForEmbedded = {
        
        // Types.             u  u  u  u  u  u  u  u  u  u  u  u  u  u  u  u  u  u  u  u  u
        //                    p  p  p  p  p  p  p  p  p  p  p  p  p  p  p  p  p  p  p  p  p
        //                    d  d  d  d  d  d  d  d  d  d  d  d  d  d  d  d  d  d  d  d  d
        //                    a  a  a  a  a  a  a  a  a  a  a  a  a  a  a  a  a  a  a  a  a
        //                    t  t  t  t  t  t  t  t  t  t  t  t  t  t  t  t  t  t  t  t  t
        //                    e  e  e  e  e  e  e  e  e  e  e  e  e  e  e  e  e  e  e  e  e
        //                    S  I  L  B  F  D  S  A  C  B  B  B  C  D  T  T  B  B  N  A  R
        //                    h  n  o  i  l  o  t  s  h  y  y  i  l  a  i  i  l  o  u  r  e
        //                    o  t  n  g  o  u  r  c  a  t  t  n  o  t  m  m  o  o  l  r  f
        //                    r     g  D  a  b  i  i  r  e  e  a  b  e  e  e  b  l  l  a
        //                    t        e  t  l  n  i  c     s  r           s     e     y
        //                             c     e  g  S  t        y           t     a
        //                             i           t  e        S           a     n
        //                             m           r  r        t           m
        //                             a           e  S        r           p
        //                             l           a  t        e
        //                                         m  r        a
        //                                            e        m
        //                                            a
        //                                            m
        /* 0 SMALLINT      */  {"PASS",  "PASS",  "PASS",  "PASS",  "PASS",  "PASS",  "PASS",  "22005", "22005", "PASS",  "XCL12", "22005", "22005", "XCL12", "XCL12", "XCL12", "22005", "PASS",  "PASS", "0A000", "0A000"},
        /* 1 INTEGER       */  {"PASS",  "PASS",  "PASS",  "PASS",  "PASS",  "PASS",  "PASS",  "22005", "22005", "PASS",  "XCL12", "22005", "22005", "XCL12", "XCL12", "XCL12", "22005", "PASS",  "PASS", "0A000", "0A000"},
        /* 2 BIGINT        */  {"PASS",  "PASS",  "PASS",  "PASS",  "PASS",  "PASS",  "PASS",  "22005", "22005", "PASS",  "XCL12", "22005", "22005", "XCL12", "XCL12", "XCL12", "22005", "PASS",  "PASS", "0A000", "0A000"},
        /* 3 DECIMAL       */  {"PASS",  "PASS",  "PASS",  "PASS",  "PASS",  "PASS",  "PASS",  "22005", "22005", "PASS",  "XCL12", "22005", "22005", "XCL12", "XCL12", "XCL12", "22005", "PASS",  "PASS", "0A000", "0A000"},
        /* 4 REAL          */  {"PASS",  "PASS",  "PASS",  "PASS",  "PASS",  "PASS",  "PASS",  "22005", "22005", "PASS",  "XCL12", "22005", "22005", "XCL12", "XCL12", "XCL12", "22005", "PASS",  "PASS", "0A000", "0A000"},
        /* 5 DOUBLE        */  {"PASS",  "PASS",  "PASS",  "PASS",  "PASS",  "PASS",  "PASS",  "22005", "22005", "PASS",  "XCL12", "22005", "22005", "XCL12", "XCL12", "XCL12", "22005", "PASS",  "PASS", "0A000", "0A000"},
        /* 6 CHAR          */  {"PASS",  "PASS",  "PASS",  "PASS",  "PASS",  "PASS",  "PASS",  "PASS",  "PASS",  "PASS",  "PASS",  "22005", "22005", "PASS",  "PASS",  "PASS",  "22005", "PASS",  "PASS", "0A000", "0A000"},
        /* 7 VARCHAR       */  {"PASS",  "PASS",  "PASS",  "PASS",  "PASS",  "PASS",  "PASS",  "PASS",  "PASS",  "PASS",  "PASS",  "22005", "22005", "PASS",  "PASS",  "PASS",  "22005", "PASS",  "PASS", "0A000", "0A000"},
        /* 8 LONGVARCHAR   */  {"PASS",  "PASS",  "PASS",  "PASS",  "PASS",  "PASS",  "PASS",  "PASS",  "PASS",  "PASS",  "PASS",  "22005", "22005", "PASS",  "PASS",  "PASS",  "22005", "PASS",  "PASS", "0A000", "0A000"},
        /* 9 CHAR FOR BIT  */  {"XCL12", "XCL12", "XCL12", "XCL12", "XCL12", "XCL12", "XCL12", "22005", "22005", "XCL12", "PASS",  "PASS",  "22005", "XCL12", "XCL12", "XCL12", "22005", "XCL12", "PASS", "0A000", "0A000"},
        /* 10 VARCH. BIT   */  {"XCL12", "XCL12", "XCL12", "XCL12", "XCL12", "XCL12", "XCL12", "22005", "22005", "XCL12", "PASS",  "PASS",  "22005", "XCL12", "XCL12", "XCL12", "22005", "XCL12", "PASS", "0A000", "0A000"},
        /* 11 LONGVAR. BIT */  {"XCL12", "XCL12", "XCL12", "XCL12", "XCL12", "XCL12", "XCL12", "22005", "22005", "XCL12", "PASS",  "PASS",  "22005", "XCL12", "XCL12", "XCL12", "22005", "XCL12", "PASS", "0A000", "0A000"},
        /* 12 CLOB         */  {"XCL12", "XCL12", "XCL12", "XCL12", "XCL12", "XCL12", "PASS",  "PASS",  "PASS",  "XCL12", "XCL12", "22005", "PASS",  "XCL12", "XCL12", "XCL12", "22005", "XCL12", "PASS", "0A000", "0A000"},
        /* 13 DATE         */  {"XCL12", "XCL12", "XCL12", "XCL12", "XCL12", "XCL12", "22007", "22005", "22005", "XCL12", "XCL12", "22005", "22005", "PASS",  "XCL12", "PASS",  "22005", "XCL12", "PASS", "0A000", "0A000"},
        /* 14 TIME         */  {"XCL12", "XCL12", "XCL12", "XCL12", "XCL12", "XCL12", "22007", "22005", "22005", "XCL12", "XCL12", "22005", "22005", "XCL12", "PASS",  "PASS",  "22005", "XCL12", "PASS", "0A000", "0A000"},
        /* 15 TIMESTAMP    */  {"XCL12", "XCL12", "XCL12", "XCL12", "XCL12", "XCL12", "22007", "22005", "22005", "XCL12", "XCL12", "22005", "22005", "PASS",  "XCL12", "PASS",  "22005", "XCL12", "PASS", "0A000", "0A000"},
        /* 16 BLOB         */  {"XCL12", "XCL12", "XCL12", "XCL12", "XCL12", "XCL12", "XCL12", "22005", "22005", "XCL12", "PASS",  "PASS",  "22005", "XCL12", "XCL12", "XCL12", "PASS",  "XCL12", "PASS", "0A000", "0A000"},
    };
    
    // This table contains the expected result of the combination of datatype
    // and updateXXX method on the network client driver. If the call to the 
    // updateXXX method fails the cell contains the expected SQLState and if it
    // passes the cell contains PASS.
    public static final String[][]  updateXXXRulesTableForNetworkClient = {
        
        // Types.             u  u  u  u  u  u  u  u  u  u  u  u  u  u  u  u  u  u  u  u  u
        //                    p  p  p  p  p  p  p  p  p  p  p  p  p  p  p  p  p  p  p  p  p
        //                    d  d  d  d  d  d  d  d  d  d  d  d  d  d  d  d  d  d  d  d  d
        //                    a  a  a  a  a  a  a  a  a  a  a  a  a  a  a  a  a  a  a  a  a
        //                    t  t  t  t  t  t  t  t  t  t  t  t  t  t  t  t  t  t  t  t  t
        //                    e  e  e  e  e  e  e  e  e  e  e  e  e  e  e  e  e  e  e  e  e
        //                    S  I  L  B  F  D  S  A  C  B  B  B  C  D  T  T  B  B  N  A  R
        //                    h  n  o  i  l  o  t  s  h  y  y  i  l  a  i  i  l  o  u  r  e
        //                    o  t  n  g  o  u  r  c  a  t  t  n  o  t  m  m  o  o  l  r  f
        //                    r     g  D  a  b  i  i  r  e  e  a  b  e  e  e  b  l  l  a
        //                    t        e  t  l  n  i  c     s  r           s     e     y
        //                             c     e  g  S  t        y           t     a
        //                             i           t  e        S           a     n
        //                             m           r  r        t           m
        //                             a           e  S        r           p
        //                             l           a  t        e
        //                                         m  r        a
        //                                            e        m
        //                                            a
        //                                            m
        /* 0 SMALLINT      */ {"PASS",  "PASS",  "PASS",  "PASS",  "PASS",  "PASS",  "22018", "XCL12", "XCL12", "PASS",  "XCL12", "XCL12", "0A000", "XCL12", "XCL12", "XCL12", "0A000", "PASS",  "PASS", "0A000", "0A000"},
        /* 1 INTEGER       */ {"PASS",  "PASS",  "PASS",  "PASS",  "PASS",  "PASS",  "22018", "XCL12", "XCL12", "PASS",  "XCL12", "XCL12", "0A000", "XCL12", "XCL12", "XCL12", "0A000", "PASS",  "PASS", "0A000", "0A000"},
        /* 2 BIGINT        */ {"PASS",  "PASS",  "PASS",  "PASS",  "PASS",  "PASS",  "22018", "XCL12", "XCL12", "PASS",  "XCL12", "XCL12", "0A000", "XCL12", "XCL12", "XCL12", "0A000", "PASS",  "PASS", "0A000", "0A000"},
        /* 3 DECIMAL       */ {"PASS",  "PASS",  "PASS",  "PASS",  "PASS",  "PASS",  "22018", "XCL12", "XCL12", "PASS",  "XCL12", "XCL12", "0A000", "XCL12", "XCL12", "XCL12", "0A000", "PASS",  "PASS", "0A000", "0A000"},
        /* 4 REAL          */ {"PASS",  "PASS",  "PASS",  "PASS",  "PASS",  "PASS",  "PASS",  "XCL12", "XCL12", "PASS",  "XCL12", "XCL12", "0A000", "XCL12", "XCL12", "XCL12", "0A000", "PASS",  "PASS", "0A000", "0A000"},
        /* 5 DOUBLE        */ {"PASS",  "PASS",  "PASS",  "PASS",  "PASS",  "PASS",  "PASS",  "XCL12", "XCL12", "PASS",  "XCL12", "XCL12", "0A000", "XCL12", "XCL12", "XCL12", "0A000", "PASS",  "PASS", "0A000", "0A000"},
        /* 6 CHAR          */ {"PASS",  "PASS",  "PASS",  "PASS",  "PASS",  "PASS",  "PASS",  "PASS",  "PASS",  "PASS",  "XCL12", "XCL12", "0A000", "PASS",  "PASS",  "PASS",  "0A000", "PASS",  "PASS", "0A000", "0A000"},
        /* 7 VARCHAR       */ {"PASS",  "PASS",  "PASS",  "PASS",  "PASS",  "PASS",  "PASS",  "PASS",  "PASS",  "PASS",  "XCL12", "XCL12", "0A000", "PASS",  "PASS",  "PASS",  "0A000", "PASS",  "PASS", "0A000", "0A000"},
        /* 8 LONGVARCHAR   */ {"PASS",  "PASS",  "PASS",  "PASS",  "PASS",  "PASS",  "PASS",  "PASS",  "PASS",  "PASS",  "XCL12", "XCL12", "0A000", "PASS",  "PASS",  "PASS",  "0A000", "PASS",  "PASS", "0A000", "0A000"},
        /* 9 CHAR FOR BIT  */ {"XCL12", "XCL12", "XCL12", "XCL12", "XCL12", "XCL12", "XCL12", "XCL12", "XCL12", "XCL12", "PASS",  "PASS",  "0A000", "XCL12", "XCL12", "XCL12", "0A000", "XCL12", "PASS", "0A000", "0A000"},
        /* 10 VARCH. BIT   */ {"XCL12", "XCL12", "XCL12", "XCL12", "XCL12", "XCL12", "XCL12", "XCL12", "XCL12", "XCL12", "PASS",  "PASS",  "0A000", "XCL12", "XCL12", "XCL12", "0A000", "XCL12", "PASS", "0A000", "0A000"},
        /* 11 LONGVAR. BIT */ {"XCL12", "XCL12", "XCL12", "XCL12", "XCL12", "XCL12", "XCL12", "XCL12", "XCL12", "XCL12", "PASS",  "PASS",  "0A000", "XCL12", "XCL12", "XCL12", "0A000", "XCL12", "PASS", "0A000", "0A000"},
        /* 12 CLOB         */ {"XCL12", "XCL12", "XCL12", "XCL12", "XCL12", "XCL12", "PASS",  "PASS",  "PASS",  "XCL12", "XCL12", "XCL12", "0A000", "XCL12", "XCL12", "XCL12", "0A000", "XCL12", "PASS", "0A000", "0A000"},
        /* 13 DATE         */ {"XCL12", "XCL12", "XCL12", "XCL12", "XCL12", "XCL12", "PASS",  "XCL12", "XCL12", "XCL12", "XCL12", "XCL12", "0A000", "PASS",  "XCL12", "PASS",  "0A000", "XCL12", "PASS", "0A000", "0A000"},
        /* 14 TIME         */ {"XCL12", "XCL12", "XCL12", "XCL12", "XCL12", "XCL12", "PASS",  "XCL12", "XCL12", "XCL12", "XCL12", "XCL12", "0A000", "XCL12", "PASS",  "PASS",  "0A000", "XCL12", "PASS", "0A000", "0A000"},
        /* 15 TIMESTAMP    */ {"XCL12", "XCL12", "XCL12", "XCL12", "XCL12", "XCL12", "PASS",  "XCL12", "XCL12", "XCL12", "XCL12", "XCL12", "0A000", "PASS",  "XCL12", "PASS",  "0A000", "XCL12", "PASS", "0A000", "0A000"},
        /* 16 BLOB         */ {"XCL12", "XCL12", "XCL12", "XCL12", "XCL12", "XCL12", "XCL12", "XCL12", "XCL12", "XCL12", "PASS",  "PASS",  "0A000", "XCL12", "XCL12", "XCL12", "0A000", "XCL12", "PASS", "0A000", "0A000"},
    };
    
    // This table contains the expected result of the combination of datatype
    // and updateObject method with a parameter of the type returned by the 
    // getXXX method on the network client driver. If the call to the 
    // updateObject method fails the cell contains the expected SQLState and if
    // it passes the cell contains PASS.
    public static final String[][]  updateObjectRulesTableForNetworkClient = {
        
        // Types.             g  g  g  g  g  g  g  g  g  g  g  g  g  g  g  g  g  g  g  g  g
        //                    e  e  e  e  e  e  e  e  e  e  e  e  e  e  e  e  e  e  e  e  e
        //                    t  t  t  t  t  t  t  t  t  t  t  t  t  t  t  t  t  t  t  t  t
        //                    S  I  L  B  F  D  S  A  C  B  B  B  C  D  T  T  B  B  N  A  R
        //                    h  n  o  i  l  o  t  s  h  y  y  i  l  a  i  i  l  o  u  r  e
        //                    o  t  n  g  o  u  r  c  a  t  t  n  o  t  m  m  o  o  l  r  f
        //                    r     g  D  a  b  i  i  r  e  e  a  b  e  e  e  b  l  l  a
        //                    t        e  t  l  n  i  c     s  r           s     e     y
        //                             c     e  g  S  t        y           t     a
        //                             i           t  e        S           a     n
        //                             m           r  r        t           m
        //                             a           e  S        r           p
        //                             l           a  t        e
        //                                         m  r        a
        //                                            e        m
        //                                            a
        //                                            m
        /* 0 SMALLINT      */ {"PASS",  "PASS",  "PASS",  "PASS",  "PASS",  "PASS",  "22018", "XCL12", "XCL12", "PASS",  "XCL12", "XCL12", "XCL12", "XCL12", "XCL12", "XCL12", "XCL12", "PASS",  "PASS", "XCL12", "XCL12"},
        /* 1 INTEGER       */ {"PASS",  "PASS",  "PASS",  "PASS",  "PASS",  "PASS",  "22018", "XCL12", "XCL12", "PASS",  "XCL12", "XCL12", "XCL12", "XCL12", "XCL12", "XCL12", "XCL12", "PASS",  "PASS", "XCL12", "XCL12"},
        /* 2 BIGINT        */ {"PASS",  "PASS",  "PASS",  "PASS",  "PASS",  "PASS",  "22018", "XCL12", "XCL12", "PASS",  "XCL12", "XCL12", "XCL12", "XCL12", "XCL12", "XCL12", "XCL12", "PASS",  "PASS", "XCL12", "XCL12"},
        /* 3 DECIMAL       */ {"PASS",  "PASS",  "PASS",  "PASS",  "PASS",  "PASS",  "22018", "XCL12", "XCL12", "PASS",  "XCL12", "XCL12", "XCL12", "XCL12", "XCL12", "XCL12", "XCL12", "PASS",  "PASS", "XCL12", "XCL12"},
        /* 4 REAL          */ {"PASS",  "PASS",  "PASS",  "PASS",  "PASS",  "PASS",  "PASS",  "XCL12", "XCL12", "PASS",  "XCL12", "XCL12", "XCL12", "XCL12", "XCL12", "XCL12", "XCL12", "PASS",  "PASS", "XCL12", "XCL12"},
        /* 5 DOUBLE        */ {"PASS",  "PASS",  "PASS",  "PASS",  "PASS",  "PASS",  "PASS",  "XCL12", "XCL12", "PASS",  "XCL12", "XCL12", "XCL12", "XCL12", "XCL12", "XCL12", "XCL12", "PASS",  "PASS", "XCL12", "XCL12"},
        /* 6 CHAR          */ {"PASS",  "PASS",  "PASS",  "PASS",  "PASS",  "PASS",  "PASS",  "PASS",  "PASS",  "PASS",  "XCL12", "XCL12", "PASS",  "PASS",  "PASS",  "PASS",  "XCL12", "PASS",  "PASS", "XCL12", "XCL12"},
        /* 7 VARCHAR       */ {"PASS",  "PASS",  "PASS",  "PASS",  "PASS",  "PASS",  "PASS",  "PASS",  "PASS",  "PASS",  "XCL12", "XCL12", "PASS",  "PASS",  "PASS",  "PASS",  "XCL12", "PASS",  "PASS", "XCL12", "XCL12"},
        /* 8 LONGVARCHAR   */ {"PASS",  "PASS",  "PASS",  "PASS",  "PASS",  "PASS",  "PASS",  "PASS",  "PASS",  "PASS",  "XCL12", "XCL12", "PASS",  "PASS",  "PASS",  "PASS",  "XCL12", "PASS",  "PASS", "XCL12", "XCL12"},
        /* 9 CHAR FOR BIT  */ {"XCL12", "XCL12", "XCL12", "XCL12", "XCL12", "XCL12", "XCL12", "XCL12", "XCL12", "XCL12", "PASS",  "PASS",  "XCL12", "XCL12", "XCL12", "XCL12", "22001", "XCL12", "PASS", "XCL12", "XCL12"},
        /* 10 VARCH. BIT   */ {"XCL12", "XCL12", "XCL12", "XCL12", "XCL12", "XCL12", "XCL12", "XCL12", "XCL12", "XCL12", "PASS",  "PASS",  "XCL12", "XCL12", "XCL12", "XCL12", "22001", "XCL12", "PASS", "XCL12", "XCL12"},
        /* 11 LONGVAR. BIT */ {"XCL12", "XCL12", "XCL12", "XCL12", "XCL12", "XCL12", "XCL12", "XCL12", "XCL12", "XCL12", "PASS",  "PASS",  "XCL12", "XCL12", "XCL12", "XCL12", "PASS",  "XCL12", "PASS", "XCL12", "XCL12"},
        /* 12 CLOB         */ {"XCL12", "XCL12", "XCL12", "XCL12", "XCL12", "XCL12", "PASS",  "PASS",  "PASS",  "XCL12", "XCL12", "XCL12", "PASS",  "XCL12", "XCL12", "XCL12", "XCL12", "XCL12", "PASS", "XCL12", "XCL12"},
        /* 13 DATE         */ {"XCL12", "XCL12", "XCL12", "XCL12", "XCL12", "XCL12", "PASS",  "XCL12", "XCL12", "XCL12", "XCL12", "XCL12", "XCL12", "PASS",  "XCL12", "PASS",  "XCL12", "XCL12", "PASS", "XCL12", "XCL12"},
        /* 14 TIME         */ {"XCL12", "XCL12", "XCL12", "XCL12", "XCL12", "XCL12", "PASS",  "XCL12", "XCL12", "XCL12", "XCL12", "XCL12", "XCL12", "XCL12", "PASS",  "PASS",  "XCL12", "XCL12", "PASS", "XCL12", "XCL12"},
        /* 15 TIMESTAMP    */ {"XCL12", "XCL12", "XCL12", "XCL12", "XCL12", "XCL12", "PASS",  "XCL12", "XCL12", "XCL12", "XCL12", "XCL12", "XCL12", "PASS",  "XCL12", "PASS",  "XCL12", "XCL12", "PASS", "XCL12", "XCL12"},
        /* 16 BLOB         */ {"XCL12", "XCL12", "XCL12", "XCL12", "XCL12", "XCL12", "XCL12", "XCL12", "XCL12", "XCL12", "PASS",  "PASS",  "XCL12", "XCL12", "XCL12", "XCL12", "PASS",  "XCL12", "PASS", "XCL12", "XCL12"},
    };
    
    /**
     * Creates a new instance of UpdatableResultSetTest
     */
    public UpdatableResultSetTest(String name) {
        super(name);
    }
    
    public static Test suite() {
        TestSuite suite = new TestSuite("UpdatableResultSetTest");
        
        TestSuite embeddedSuite = baseSuite("UpdatableResultSetTest:embedded");
        TestSuite clientSuite = baseSuite("UpdatableResultSetTest:client");
        
        if (JDBC.vmSupportsJDBC3()) {
            embeddedSuite.addTest(
                    new UpdatableResultSetTest("xTestInsertRowAfterCommit"));
            clientSuite.addTest(
                    new UpdatableResultSetTest("xTestInsertRowAfterCommit"));
        }
        
        suite.addTest(new CleanDatabaseTestSetup(embeddedSuite));
        suite.addTest(TestConfiguration.clientServerDecorator(
                new CleanDatabaseTestSetup(clientSuite)));
        
        return suite;
    }
    
    private static TestSuite baseSuite(String name) {
        TestSuite suite = new TestSuite(name);
        suite.addTestSuite(UpdatableResultSetTest.class);
        return suite;
    }
    
    protected void setUp() throws SQLException {
        getConnection().setAutoCommit(false);
    }
    
    /**
     * Negative test - request for scroll sensitive updatable resultset will
     * give an updatable scroll insensitive resultset
     */
    public void testScrollSensitiveResultSet() throws SQLException {
        getConnection().clearWarnings();
        Statement stmt = createStatement(
                ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
        String sqlState = usingEmbedded() ? "01J02" : "01J10";
        assertEquals("FAIL - Should get warning on Downgrade",
                sqlState, getConnection().getWarnings().getSQLState());
        assertEquals("FAIL - Result set type should be scroll insensitive",
                ResultSet.TYPE_SCROLL_INSENSITIVE, stmt.getResultSetType());
        assertEquals("FAIL - Result set concurrency should be updatable",
                ResultSet.CONCUR_UPDATABLE, stmt.getResultSetConcurrency());
    }
    
    /**
     * Negative test - request a read only resultset and attempt deleteRow and
     * updateRow on it
     */
    public void testUpdateDeleteRowOnReadOnlyResultSet() throws SQLException {
        createTableT1();
        Statement stmt = createStatement();
        ResultSet rs = stmt.executeQuery("select * from t1");
        assertEquals("FAIL - Result set concurrency should be read only",
                ResultSet.CONCUR_READ_ONLY, rs.getConcurrency());
        assertTrue("FAIL - row not found", rs.next());
        
        // attempt to send a deleteRow on a read only result set
        try {
            rs.deleteRow();
            fail("FAIL - deleteRow should have failed because this is a " +
                    "read only resultset");
        } catch (SQLException e) {
            assertSQLState("XJ083", e);
        }
        
        // attempt to send a updateRow on a read only result set
        try {
            rs.updateRow();
            fail("FAIL - updateRow should have failed because this is a " +
                    "read only resultset");
        } catch (SQLException e) {
            assertSQLState("XJ083", e);
        }
        rs.close();
        
        // verify that the data remains unchanged
        String expected[][] = {{"1", "aa"}, {"2", "bb"}, {"3", "cc"}};
        JDBC.assertFullResultSet(
                stmt.executeQuery("SELECT * FROM t1"), expected, true);
        stmt.close();
        
    }
    
    /**
     * Negative Test - request a read only resultset and send a sql with FOR
     * UPDATE clause and attempt deleteRow/updateRow on it
     */
    public void testUpdateDeleteRowOnReadOnlyResultSetWithForUpdate() 
            throws SQLException 
    {
        createTableT1();
        Statement stmt = createStatement();
        ResultSet rs = stmt.executeQuery("select * from t1 FOR UPDATE");
        assertEquals("FAIL - Result set concurrency should be read only",
                ResultSet.CONCUR_READ_ONLY, rs.getConcurrency());
        assertTrue("FAIL - row not found", rs.next());
        
        // attempt to send a deleteRow on a read only result set
        try {
            rs.deleteRow();
            fail("FAIL - deleteRow should have failed because this is a " +
                    "read only resultset");
        } catch (SQLException e) {
            assertSQLState("XJ083", e);
        }
        
        // attempt to send a updateRow on a read only result set
        try {
            rs.updateRow();
            fail("FAIL - updateRow should have failed because this is a " +
                    "read only resultset");
        } catch (SQLException e) {
            assertSQLState("XJ083", e);
        }
        rs.close();
        
        // verify that the data remains unchanged
        String expected[][] = {{"1", "aa"}, {"2", "bb"}, {"3", "cc"}};
        JDBC.assertFullResultSet(
                stmt.executeQuery("SELECT * FROM t1"), expected, true);
        stmt.close();
    }
    
    
    /**
     * Negative Test - request resultset with no FOR UPDATE clause and 
     * CONCUR_READ_ONLY
     */
    public void testUpdateDeleteRowOnReadOnlyResultSetWithoutForUpdate() 
            throws SQLException 
    {
        createTableT1();
        Statement stmt = createStatement(
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        ResultSet rs = stmt.executeQuery("select * from t1");
        assertEquals("FAIL - Result set concurrency should be read only",
                ResultSet.CONCUR_READ_ONLY, rs.getConcurrency());
        assertTrue("FAIL - row not found", rs.next());
        
        // attempt to send a deleteRow on a read only result set
        try {
            rs.deleteRow();
            fail("FAIL - deleteRow should have failed because this is a " +
                    "read only resultset");
        } catch (SQLException e) {
            assertSQLState("XJ083", e);
        }
        
        // attempt to send a deleteRow on a read only result set
        try {
            rs.updateRow();
            fail("FAIL - updateRow should have failed because this is a " +
                    "read only resultset");
        } catch (SQLException e) {
            assertSQLState("XJ083", e);
        }
        rs.close();
        
        // verify that the data remains unchanged
        String expected[][] = {{"1", "aa"}, {"2", "bb"}, {"3", "cc"}};
        JDBC.assertFullResultSet(
                stmt.executeQuery("SELECT * FROM t1"), expected, true);
        stmt.close();
    }
    
    /**
     * Negative Test - request updatable resultset for sql with FOR READ ONLY 
     * clause
     */
    public void testUpdateDeleteRowOnUpdatableResultSetWithForReadOnly() 
            throws SQLException 
    {
        createTableT1();
        getConnection().clearWarnings();
        Statement stmt = createStatement(
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
        ResultSet rs = stmt.executeQuery("select * from t1 FOR READ ONLY");
        assertEquals("FAIL - Result set concurrency should be read only",
                ResultSet.CONCUR_READ_ONLY, rs.getConcurrency());
        assertEquals("FAIL - FAIL - Should get warning on Downgrade",
                "01J06", rs.getWarnings().getSQLState());
        assertTrue("FAIL - row not found", rs.next());
        
        // Attempt to send a deleteRow on a read only result set"
        try {
            rs.deleteRow();
            fail("FAIL - deleteRow should have failed because this is a " +
                    "read only resultset");
        } catch (SQLException e) {
            assertSQLState("XJ083", e);
        }
        
        // Attempt to send a updateRow on a read only result set
        try {
            rs.updateRow();
            fail("FAIL - updateRow should have failed because this is a " +
                    "read only resultset");
        } catch (SQLException e) {
            assertSQLState("XJ083", e);
        }
        rs.close();
        
        // verify that the data remains unchanged
        String expected[][] = {{"1", "aa"}, {"2", "bb"}, {"3", "cc"}};
        JDBC.assertFullResultSet(
                stmt.executeQuery("SELECT * FROM t1"), expected, true);
        stmt.close();
    }
    
    /**
     * Negative test - attempt to deleteRow & updateRow on updatable resultset
     * when the resultset is not positioned on a row
     */
    public void testUpdateDeleteRowNotOnRow() throws SQLException {
        createTableT1();
        Statement stmt = createStatement(
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
        ResultSet rs = stmt.executeQuery("SELECT * FROM t1");
        assertEquals("FAIL - Result set concurrency should be updatable",
                ResultSet.CONCUR_UPDATABLE, rs.getConcurrency());
        
        // Attempt to send a deleteRow without being positioned on a row
        try {
            rs.deleteRow();
            fail("FAIL - deleteRow should have failed, not on a row");
        } catch (SQLException e) {
            assertSQLState("24000", e);
        }
        
        // Attempt to send a deleteRow without being positioned on a row
        try {
            rs.updateRow();
            fail("FAIL - updateRow should have failed, not on a row");
        } catch (SQLException e) {
            String sqlState = usingEmbedded() ? "24000" : "XJ121";
            assertSQLState(sqlState, e);
        }
        
        //read all the rows from the resultset and position after the last row
        while (rs.next());
        
        // attempt to send a deleteRow when positioned after the last row
        try {
            rs.deleteRow();
            fail("FAIL - deleteRow should have failed, positioned " +
                    "after last row");
        } catch (SQLException e) {
            String sqlState = usingEmbedded() ? "24000" : "XCL07";
            assertSQLState(sqlState, e);
        }
        
        // attempt to send a updateRow when positioned after the last row
        try {
            rs.updateRow();
            fail("FAIL - updateRow should have failed, positioned " +
                    "after last row");
        } catch (SQLException e) {
            String sqlState = usingEmbedded() ? "24000" : "XJ121";
            assertSQLState(sqlState, e);
        }
        rs.close();
        
        // verify that the data remains unchanged
        String expected[][] = {{"1", "aa"}, {"2", "bb"}, {"3", "cc"}};
        JDBC.assertFullResultSet(
                stmt.executeQuery("SELECT * FROM t1"), expected, true);
        stmt.close();
    }
    
    /**
     * Negative test - attempt deleteRow & updateRow on updatable resultset 
     * after closing the resultset
     */
    public void testUpdateDeleteRowOnClosedResultSet() throws SQLException {
        createTableT1();
        Statement stmt = createStatement(
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
        ResultSet rs = stmt.executeQuery("SELECT * FROM t1 FOR UPDATE");
        assertEquals("FAIL - Result set concurrency should be updatable",
                ResultSet.CONCUR_UPDATABLE, rs.getConcurrency());
        assertTrue("FAIL - row not found", rs.next());
        rs.close();
        
        // attempt to send a deleteRow on a closed result set
        try {
            rs.deleteRow();
            fail("FAIL - deleteRow should have failed because this " +
                    "result set is closed");
        } catch (SQLException e) {
            assertSQLState("XCL16", e);
        }
        
        // attempt to send a deleteRow on a closed result set
        try {
            rs.updateRow();
            fail("FAIL - updateRow should have failed because this " +
                    "result set is closed");
        } catch (SQLException e) {
            assertSQLState("XCL16", e);
        }
        rs.close();
        
        // verify that the data remains unchanged
        String expected[][] = {{"1", "aa"}, {"2", "bb"}, {"3", "cc"}};
        JDBC.assertFullResultSet(
                stmt.executeQuery("SELECT * FROM t1"), expected, true);
        stmt.close();
    }
    
    /**
     * Negative test - try updatable resultset on system table
     */
    public void testUpdatableResultSetOnSysTable() throws SQLException {
        try {
            ResultSet rs = createStatement().
                    executeQuery("SELECT * FROM sys.systables FOR UPDATE");
            fail("FAIL - trying to open an updatable resultset on a system " +
                    "table should have failed because system tables can't " +
                    "be updated by a user");
        } catch (SQLException e) {
            assertSQLState("42Y90", e);
        }
    }
    
    /**
     * Negative test - try updatable resultset on a view
     */
    public void testUpdatableResultSetOnView() throws SQLException {
        createTableT1();
        Statement stmt = createStatement();
        stmt.executeUpdate("create view v1 as select * from t1");
        try {
            ResultSet rs = stmt.executeQuery("SELECT * FROM v1 FOR UPDATE");
            fail("FAIL - trying to open an updatable resultset on a view " +
                    "should have failed because Derby does not support " +
                    "updates to views yet");
        } catch (SQLException e) {
            assertSQLState("42Y90", e);
        }
        stmt.executeUpdate("drop view v1");
        stmt.close();
    }
    
    /**
     * Negative test - attempt to open updatable resultset when there is
     * join in the select query should fail
     */
    public void testUpdatableResultSetOnJoin() throws SQLException {
        createTableT1();
        createTableT2();
        Statement stmt = createStatement(
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
        try {
            ResultSet rs = stmt.executeQuery(
                    "SELECT c1 FROM t1,t2 where t1.c1 = t2.c21 FOR UPDATE");
            fail("FAIL - trying to open an updatable resultset should have " +
                    "failed because updatable resultset donot support join " +
                    "in the select query");
        } catch (SQLException e) {
            assertSQLState("42Y90", e);
        }
    }
    
    /**
     * Negative test - With autocommit on, attempt to drop a table when there
     * is an open updatable resultset on it
     */
    public void testDropTableWithUpatableResultSet() throws SQLException {
        getConnection().setAutoCommit(true);
        createTableT1();
        Statement stmt = createStatement(
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
        ResultSet rs = stmt.executeQuery("SELECT c1 FROM t1 FOR UPDATE");
        assertTrue("FAIL - row not found", rs.next());
        rs.updateInt(1,123);
        
        Statement stmt1 = createStatement();
        try {
            stmt1.executeUpdate("drop table t1");
            fail("FAIL - drop table should have failed because the " +
                    "updatable resultset is still open");
        } catch (SQLException e) {
            assertSQLState("X0X95", e);
        }
        stmt1.close();
        
        // Since autocommit is on, the drop table exception resulted in a
        //runtime rollback causing updatable resultset object to close
        try {
            rs.updateRow();
            fail("FAIL - resultset should have been closed at this point and " +
                    "updateRow should have failed");
        } catch (SQLException e) {
            String sqlState = usingEmbedded()? "XCL16" : "24000";
            assertSQLState(sqlState, e);
        }
        
        try {
            rs.deleteRow();
            fail("FAIL - resultset should have been closed at this point and " +
                    "deleteRow should have failed");
        } catch (SQLException e) {
            String sqlState = usingEmbedded()? "XCL16" : "24000";
            assertSQLState(sqlState, e);
        }
        
        // verify that the data remains unchanged
        String expected[][] = {{"1", "aa"}, {"2", "bb"}, {"3", "cc"}};
        JDBC.assertFullResultSet(
                stmt.executeQuery("SELECT * FROM t1"), expected, true);
        stmt.executeUpdate("DROP TABLE T1");
        stmt.close();
        
    }
    
    /**
     * Negative test - foreign key constraint failure will cause deleteRow
     * to fail
     */
    public void testForeignKeyConstraintFailureOnDeleteRow() 
            throws SQLException 
    {
        getConnection().setAutoCommit(true);
        createTableWithPrimaryKey();
        Statement stmt = createStatement(
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
        ResultSet rs = stmt.executeQuery(
                "SELECT * FROM tableWithPrimaryKey FOR UPDATE");
        assertTrue("FAIL - row not found", rs.next());
        try {
            rs.deleteRow();
            fail("FAIL - deleteRow should have failed because it will cause " +
                    "foreign key constraint failure");
        } catch (SQLException e) {
            assertSQLState("23503", e);
        }
        // Since autocommit is on, the constraint exception resulted in a
        // runtime rollback causing updatable resultset object to close
        try {
            rs.next();
            // DERBY-160
            if (usingEmbedded())
                fail("FAIL - next should have failed because foreign key " +
                        "constraint failure resulted in a runtime rollback");
        } catch (SQLException e) {
            assertFalse("FAIL - Network client should not fail due to " +
                    "DERBY-160", !usingEmbedded());
            assertSQLState("XCL16", e);
        }
        
        // verify that the data is unchanged
        String[][] expected = {{"1", "1"}, {"2", "2"}, {"3", "3"}, {"4", "4"}};
        JDBC.assertFullResultSet(
                stmt.executeQuery("SELECT * FROM tableWithPrimaryKey"),
                expected, true);
        stmt.executeUpdate("DROP TABLE tableWithConstraint");
        stmt.executeUpdate("DROP TABLE tableWithPrimaryKey");
        stmt.close();
    }
    
    /**
     * Negative test - foreign key constraint failure will cause updateRow
     * to fail
     */
    public void testForeignKeyConstraintFailureOnUpdateRow() 
            throws SQLException 
    {
        getConnection().setAutoCommit(true);
        createTableWithPrimaryKey();
        Statement stmt = createStatement(
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
        ResultSet rs = stmt.executeQuery(
                "SELECT c1, c2 FROM tableWithPrimaryKey FOR UPDATE");
        assertTrue("FAIL - row not found", rs.next());
        rs.updateInt(1,11);
        rs.updateInt(2,22);
        try {
            rs.updateRow();
            fail("FAIL - updateRow should have failed because it will cause " +
                    "foreign key constraint failure");
        } catch (SQLException e) {
            assertSQLState("23503", e);
        }
        // Since autocommit is on, the constraint exception resulted in a
        // runtime rollback causing updatable resultset object to close
        try {
            rs.next();
            // DERBY-160
            if (usingEmbedded())
                fail("FAIL - next should have failed because foreign key " +
                        "constraint failure resulted in a runtime rollback");
        } catch (SQLException e) {
            assertFalse("FAIL - Network client should not fail due to " +
                    "DERBY-160", !usingEmbedded());
            assertSQLState("XCL16", e);
        }
        
        // verify that the data is unchanged
        String[][] expected = {{"1", "1"}, {"2", "2"}, {"3", "3"}, {"4", "4"}};
        JDBC.assertFullResultSet(
                stmt.executeQuery("SELECT * FROM tableWithPrimaryKey"),
                expected, true);
        stmt.executeUpdate("DROP TABLE tableWithConstraint");
        stmt.executeUpdate("DROP TABLE tableWithPrimaryKey");
        stmt.close();
    }
    
    /**
     * Negative test - Can't call updateXXX methods on columns that do not
     * correspond to a column in the table
     */
    public void testUpdateXXXOnColumnNotFromTable() throws SQLException {
        createTableT1();
        Statement stmt = createStatement(
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
        ResultSet rs = stmt.executeQuery("SELECT 1, 2 FROM t1 FOR UPDATE");
        assertTrue("FAIL - row not found", rs.next());
        try {
            rs.updateInt(1,22);
            fail("FAIL - updateInt should have failed because it is trying " +
                    "to update a column that does not correspond to column " +
                    "in base table");
        } catch (SQLException e) {
            String sqlState = (usingEmbedded()) ? "XJ084" : "XJ124";
            assertSQLState(sqlState, e);
        }
        rs.close();
        stmt.close();
    }
    
    /**
     * Negative test - Call updateXXX method on out of the range column
     */
    public void testUpdateXXXOnOutOfRangeColumn() throws SQLException {
        createTableT1();
        Statement stmt = createStatement(
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
        ResultSet rs = stmt.executeQuery("SELECT c1, c2 FROM t1 FOR UPDATE");
        assertTrue("FAIL - row not found", rs.next());
        try {
            println("There are only 2 columns in the select list and we are " +
                    "trying to send updateXXX on column position 3");
            rs.updateInt(3,22);
            fail("FAIL - updateInt should have failed because there are " +
                    "only 2 columns in the select list");
        } catch (SQLException e) {
            assertSQLState("XCL14", e);
        }
        rs.close();
        stmt.close();
    }
    
    /**
     * Positive test - request updatable resultset for forward only type 
     * resultset
     */
    public void testResultSetNotPositionedAfterDeleteRow() 
            throws SQLException 
    {
        createTableT1();
        getConnection().clearWarnings();
        Statement stmt = createStatement(
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
        assertNull("FAIL - should not get a warning",
                getConnection().getWarnings());
        assertEquals("FAIL - wrong result set type",
                ResultSet.TYPE_FORWARD_ONLY, stmt.getResultSetType());
        assertEquals("FAIL - wrong result set concurrency",
                ResultSet.CONCUR_UPDATABLE, stmt.getResultSetConcurrency());
        
        ResultSet rs = stmt.executeQuery("SELECT * FROM t1");
        assertTrue("FAIL - row not found", rs.next());
        int c1Before = rs.getInt(1);
        String c2Before = rs.getString(2);
        rs.deleteRow();
        // Calling getXXX will fail because the result set is positioned before
        //the next row.
        try {
            rs.getInt(1);
            fail("FAIL - result set not positioned on a row, rs.getInt(1) " +
                    "should have failed");
        } catch (SQLException e) {
            String sqlState = (usingEmbedded()) ? "24000" : "XJ121";
            assertSQLState(sqlState, e);
        }
        
        // Calling deleteRow again again w/o first positioning the ResultSet on
        // the next row will fail
        try {
            rs.deleteRow();
            fail("FAIL - deleteRow should have failed because ResultSet is " +
                    "not positioned on a row");
        } catch (SQLException e) {
            assertSQLState("24000", e);
        }
        
        // position the result set on the next row
        assertTrue("FAIL - row not found", rs.next());
        // calling delete row not will not fail because the result set is
        // positioned
        rs.deleteRow();
        rs.close();
        
        // verify that the table contains one row
        String[][] expected = {{"3", "cc"}};
        JDBC.assertFullResultSet(
                stmt.executeQuery("SELECT * FROM t1"), expected, true);
        stmt.close();
    }
    
    /**
     * Positive test - request updatable resultset for forward only type
     * resultset
     */
    public void testResultSetNotPositionedAfterUpdateRow() 
            throws SQLException 
    {
        createTableT1();
        getConnection().clearWarnings();
        Statement stmt = createStatement(
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
        assertNull("FAIL - should not get a warning",
                getConnection().getWarnings());
        ResultSet rs = stmt.executeQuery("SELECT * FROM t1 FOR UPDATE");
        assertTrue("FAIL - row not found", rs.next());
        
        int c1Before = rs.getInt(1);
        rs.updateInt(1,234);
        assertEquals("FAIL - column should have updated value",
                234, rs.getInt(1));
        assertEquals("FAIL - value of column 2 should not have changed",
                "aa", rs.getString(2).trim());
        println("now updateRow on the row");
        rs.updateRow();
        // Calling getXXX method will fail because after updateRow the result
        // set is positioned before the next row
        try {
            rs.getInt(1);
            fail("FAIL - result set not positioned on a row, rs.getInt(1) " +
                    "should have failed");
        } catch (SQLException e) {
            String sqlState = (usingEmbedded()) ? "24000" : "XJ121";
            assertSQLState(sqlState, e);
        }
        
        // calling updateRow again w/o first positioning the ResultSet on the
        // next row will fail
        try {
            rs.updateRow();
            fail("FAIL - updateRow should have failed because ResultSet is " +
                    "not positioned on a row");
        } catch (SQLException e) {
            String sqlState = (usingEmbedded()) ? "24000" : "XJ121";
            assertSQLState(sqlState, e);
        }
        
        // Position the ResultSet with next()
        assertTrue("FAIL - row not found", rs.next());
        //Should be able to updateRow() on the current row now
        rs.updateString(2,"234");
        rs.updateRow();
        rs.close();
        
        // Verify that the data was correctly updated
        String[][] expected = {{"234", "aa"}, {"2", "234"}, {"3", "cc"}};
        JDBC.assertFullResultSet(
                stmt.executeQuery("SELECT * FROM t1"), expected, true);
        stmt.close();
    }
    
    /**
     * Positive test - use updatable resultset to do postitioned delete
     */
    public void testPositionedDeleteOnUpdatableResultSet() 
            throws SQLException 
    {
        createTableT1();
        getConnection().clearWarnings();
        Statement stmt = createStatement(
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
        assertNull("FAIL - should not get a warning",
                getConnection().getWarnings());
        assertEquals("FAIL - wrong result set type",
                ResultSet.TYPE_FORWARD_ONLY, stmt.getResultSetType());
        assertEquals("FAIL - wrong result set concurrency",
                ResultSet.CONCUR_UPDATABLE, stmt.getResultSetConcurrency());
        
        ResultSet rs = stmt.executeQuery("SELECT * FROM t1");
        assertTrue("FAIL - row not found", rs.next());
        int c1Before = rs.getInt(1);
        String c2Before = rs.getString(2);
        PreparedStatement pStmt = prepareStatement(
                "DELETE FROM T1 WHERE CURRENT OF " + rs.getCursorName());
        pStmt.executeUpdate();
        assertEquals("FAIL - wrong value on deleted row",
                c1Before, rs.getInt(1));
        assertEquals("FAIL - wrong value on deleted row",
                c2Before, rs.getString(2));
        
        // doing positioned delete again w/o first positioning the ResultSet on
        // the next row will fail
        try {
            pStmt.executeUpdate();
            fail("FAIL - positioned delete should have failed because " +
                    "ResultSet is not positioned on a row");
        } catch (SQLException e) {
            assertSQLState("24000", e);
        }
        
        // Position the ResultSet with next()
        assertTrue("FAIL - row not found", rs.next());
        //Should be able to do positioned delete on the current row now
        pStmt.executeUpdate();
        
        rs.close();
        pStmt.close();
        
        
        // Verify that the data was correctly updated
        String[][] expected = {{"3", "cc"}};
        JDBC.assertFullResultSet(
                stmt.executeQuery("SELECT * FROM t1"), expected, true);
        stmt.close();
    }
    
    /**
     * Positive test - updatable resultset to do positioned update
     */
    public void testPositionedUpdateOnUpdatableResultSet() 
            throws SQLException 
    {
        createTableT1();
        getConnection().clearWarnings();
        Statement stmt = createStatement(
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
        assertNull("FAIL - should not get a warning",
                getConnection().getWarnings());
        ResultSet rs = stmt.executeQuery("SELECT * FROM t1");
        assertTrue("FAIL - row not found", rs.next());
        
        int c1Before = rs.getInt(1);
        String c2Before = rs.getString(2);
        PreparedStatement pStmt = prepareStatement(
                "UPDATE T1 SET C1=?, C2=? WHERE CURRENT OF " +
                rs.getCursorName());
        final int c1 = 2345;
        final String c2 = "UUU";
        pStmt.setInt(1, c1);
        pStmt.setString(2, c2); // current value
        pStmt.executeUpdate();
        
        assertEquals("FAIL - column 1 should have the original value",
                c1Before, rs.getInt(1));
        assertEquals("FAIL - column 2 should have the original value",
                c2Before, rs.getString(2));
        
        // refreshRow will fail, not implemented for this type of result set
        try {
            rs.refreshRow();
            fail("FAIL - refreshRow not implemented for this type of " +
                    "result set");
        } catch (SQLException e) {
            String sqlState = usingEmbedded() ? "0A000" : "XJ125";
            assertSQLState(sqlState, e);
        }
        
        // a sencond positioned update will succed because the cursor is still
        // positioned
        pStmt.setInt(1, c1);
        pStmt.setString(2, c2); // current value
        pStmt.executeUpdate();
        
        // Position the ResultSet with next()
        assertTrue("FAIL - row not found", rs.next());
        // Should still be able to do positioned update
        pStmt.setInt(1, rs.getInt(1)); // current value
        pStmt.setString(2, "abc");
        pStmt.executeUpdate();
        
        rs.close();
        
        // Verify that the data was correctly updated
        String[][] expected = {{"2345", "UUU"}, {"2", "abc"}, {"3", "cc"}};
        JDBC.assertFullResultSet(
                stmt.executeQuery("SELECT * FROM t1"), expected, true);
        stmt.close();
        pStmt.close();
    }
    
    /**
     * Positive Test2 - even if no columns from table specified in the column
     * list, we should be able to get updatable resultset
     */
    public void testUpdatableResultsetNoColumnInColumnList() 
            throws SQLException 
    {
        // Will work in embedded mode because target table is not derived from
        // the columns in the select list
        // Will not work in network server mode because it derives the target
        // table from the columns in the select list");
        getConnection().setAutoCommit(true);
        createTableT1();
        
        // Get row count
        Statement stmt = createStatement(
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
        ResultSet rs = stmt.executeQuery("select count(*) from t1");
        assertTrue("FAIL - statement should return a row", rs.next());
        int origCount = rs.getInt(1);
        assertEquals("FAIL - wrong row count", 3, origCount);
        
        rs = stmt.executeQuery("SELECT 1, 2 FROM t1 FOR UPDATE");
        assertTrue("FAIL - row not found", rs.next());
        assertEquals("FAIL - wrong value for column 1", 1, rs.getInt(1));
        try {
            rs.deleteRow();
            assertTrue("FAIL - should have failed in network server",
                    usingEmbedded());
        } catch (SQLException e) {
            assertTrue("FAIL - should not fail on embedded", !usingEmbedded());
            assertSQLState("42X01", e);
        }
        rs.close();
        
        rs = stmt.executeQuery("select count(*) from t1");
        assertTrue("FAIL - statement should return a row", rs.next());
        int count = rs.getInt(1);
        if (usingEmbedded()) {
            assertEquals("FAIL - wrong row count", (origCount - 1), count);
        } else {
            assertEquals("FAIL - wrong row count", (origCount), count);
        }
        rs.close();
        stmt.execute("DROP TABLE T1");
        stmt.close();
    }
    
    /**
     * Positive test - use prepared statement with concur updatable status to
     * test deleteRow
     */
    public void testDeleteRowWithPreparedStatement() throws SQLException {
        createTableT1();
        PreparedStatement pStmt = getConnection().prepareStatement(
                "select * from t1 where c1 > ?",
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
        assertEquals("FAIL - wrong result set type",
                ResultSet.TYPE_FORWARD_ONLY, pStmt.getResultSetType());
        assertEquals("FAIL - wrong result set concurrency",
                ResultSet.CONCUR_UPDATABLE, pStmt.getResultSetConcurrency());
        pStmt.setInt(1,0);
        ResultSet rs = pStmt.executeQuery();
        assertTrue("FAIL - statement should return a row", rs.next());
        int c1Before = rs.getInt(1);
        rs.deleteRow();
        
        // Since after deleteRow(), ResultSet is positioned before the next
        // row, getXXX will fail
        try {
            rs.getInt(1);
            fail("FAIL - not on a row, can not get column");
        } catch (SQLException e) {
            String sqlState = usingEmbedded() ? "24000" : "XJ121";
            assertSQLState(sqlState, e);
        }
        
        // calling deleteRow again w/o first positioning the ResultSet on the
        // next row will fail
        try {
            rs.deleteRow();
            fail("FAIL - deleteRow should have failed because it can't be " +
                    "called more than once on the same row");
        } catch (SQLException e) {
            String sqlState = "24000";
            assertSQLState(sqlState, e);
        }
        
        // Position the ResultSet with next()
        assertTrue("FAIL - statement should return a row", rs.next());
        //Derby-718 check that column values are not null after next()
        assertFalse("FAIL - first column should not be 0", rs.getInt(1) == 0);
        // Derby-718
        // Should be able to deletRow() on the current row now
        rs.deleteRow();
        
        rs.close();
        pStmt.close();
        
        // Verify that the data was correctly updated
        String[][] expected = {{"3", "cc"}};
        JDBC.assertFullResultSet(
                createStatement().executeQuery("SELECT * FROM t1"),
                expected, true);
    }
    
    /**
     * Positive test - use prepared statement with concur updatable status to
     * test updateXXX
     */
    public void testUpdateXXXWithPreparedStatement() throws SQLException {
        createTableT1();
        PreparedStatement pStmt = getConnection().prepareStatement(
                "select * from t1 where c1>? for update",
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
        assertEquals("FAIL - wrong result set type",
                ResultSet.TYPE_FORWARD_ONLY, pStmt.getResultSetType());
        assertEquals("FAIL - wrong result set concurrency",
                ResultSet.CONCUR_UPDATABLE, pStmt.getResultSetConcurrency());
        pStmt.setInt(1,0);
        ResultSet rs = pStmt.executeQuery();
        
        assertTrue("FAIL - statement should return a row", rs.next());
        assertEquals("FAIL - wrong value for column 1", 1, rs.getInt(1));
        rs.updateInt(1,5);
        assertEquals("FAIL - wrong value for column 5", 5, rs.getInt(1));
        rs.updateRow();
        
        // Since after updateRow(), ResultSet is positioned before the next row,
        // getXXX will fail
        try {
            rs.getInt(1);
            fail("FAIL - not on a row, can not get column");
        } catch (SQLException e) {
            String sqlState = usingEmbedded() ? "24000" : "XJ121";
            assertSQLState(sqlState, e);
        }
        
        // calling updateXXX again w/o first positioning the ResultSet on the
        //next row will fail
        try {
            rs.updateInt(1,0);
            fail("FAIL - updateXXX should have failed because resultset is " +
                    "not positioned on a row");
        }catch (SQLException e) {
            String sqlState = usingEmbedded() ? "24000" : "XJ121";
            assertSQLState(sqlState, e);
        }
        
        // calling updateRow again w/o first positioning the ResultSet on the
        // next row will fail
        try {
            rs.updateRow();
            fail("FAIL - updateRow should have failed because resultset is " +
                    "not positioned on a row");
        }catch (SQLException e) {
            String sqlState = usingEmbedded() ? "24000" : "XJ121";
            assertSQLState(sqlState, e);
        }
        
        // calling cancelRowUpdates will fail because the result set is not
        // positioned
        try {
            rs.cancelRowUpdates();
            fail("FAIL - cancelRowUpdates should have failed because the " +
                    "resultset is not positioned on a row");
        }catch (SQLException e) {
            String sqlState = usingEmbedded() ? "24000" : "XJ121";
            assertSQLState(sqlState, e);
        }
        
        // Position the ResultSet with next()
        assertTrue("FAIL - statement should return a row", rs.next());
        // Should be able to cancelRowUpdates() on the current row now"
        rs.cancelRowUpdates();
        rs.close();
        pStmt.close();
        
        // Verify that the data was correctly updated
        String[][] expected = {{"5", "aa"}, {"2", "bb"}, {"3", "cc"}};
        JDBC.assertFullResultSet(
                createStatement().executeQuery("SELECT * FROM t1"),
                expected, true);
    }
    
    /**
     * Positive test - use callable statement with concur updatable status
     */
    public void testCallableStatementWithUpdatableResultSet() 
            throws SQLException 
    {
        createTableT1();
        CallableStatement callStmt = getConnection().prepareCall(
                "select * from t1",
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
        ResultSet rs = callStmt.executeQuery();
        assertEquals("FAIL - wrong result set type",
                ResultSet.TYPE_FORWARD_ONLY, callStmt.getResultSetType());
        assertEquals("FAIL - wrong result set concurrency",
                ResultSet.CONCUR_UPDATABLE, callStmt.getResultSetConcurrency());
        assertTrue("FAIL - statement should return a row", rs.next());
        assertEquals("FAIL - wrong value for column 1", 1, rs.getInt(1));
        rs.deleteRow();
        
        // Since after deleteRow(), ResultSet is positioned before the next row,
        // getXXX will fail
        try {
            rs.getInt(1);
            fail("FAIL - not on row, can not get value");
        } catch (SQLException e) {
            String sqlState = usingEmbedded() ? "24000" : "XJ121";
            assertSQLState(sqlState, e);
        }
        
        // calling deleteRow again w/o first positioning the ResultSet on the
        // next row will fail
        try {
            rs.deleteRow();
            fail("FAIL - deleteRow should have failed because it can't be " +
                    "called more than once on the same row");
        } catch (SQLException e) {
            String sqlState = "24000";
            assertSQLState(sqlState, e);
        }
        
        // Position the ResultSet with next()
        assertTrue("FAIL - statement should return a row", rs.next());
        // Should be able to deletRow() on the current row now
        rs.deleteRow();
        //have to close the resultset because by default, resultsets are held
        // open over commit
        rs.close();
        callStmt.close();
        
        // Verify that the data was correctly updated
        String[][] expected = {{"3", "cc"}};
        JDBC.assertFullResultSet(
                createStatement().executeQuery("SELECT * FROM t1"),
                expected, true);
    }
    
    /**
     * Positive test - donot have to select primary key to get an updatable
     * resultset
     */
    public void testUpdatableResultSetWithoutSelectingPrimaryKey()
    throws SQLException {
        createTableT3();
        Statement stmt = createStatement(
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
        ResultSet rs = stmt.executeQuery("SELECT c32 FROM t3");
        assertTrue("FAIL - statement should return a row", rs.next());
        assertEquals("FAIL - wrong value for column 1", 1, rs.getInt(1));
        // now try to delete row when primary key is not selected for that row
        rs.deleteRow();
        assertTrue("FAIL - statement should return a row", rs.next());
        rs.updateLong(1,123);
        rs.updateRow();
        rs.close();
        
        // verify that the table was correctly update
        String[][] expected = {{"2", "123"}, {"3", "3"}, {"4", "4"}};
        JDBC.assertFullResultSet(stmt.executeQuery("select * from t3"),
                expected, true);
        stmt.close();
    }
    
    /**
     * Positive test - For Forward Only resultsets, DatabaseMetaData will
     * return false for ownDeletesAreVisible and deletesAreDetected
     * This is because, after deleteRow, we position the ResultSet before the
     * next row. We don't make a hole for the deleted row and then stay on that
     * deleted hole
     */
    public void testRowDeleted() throws SQLException {
        DatabaseMetaData dbmt = getConnection().getMetaData();
        assertEquals("FAIL - wrong values for ownDeletesAreVisible(" +
                "ResultSet.TYPE_FORWARD_ONLY)", false,
                dbmt.ownDeletesAreVisible(ResultSet.TYPE_FORWARD_ONLY));
        assertEquals("FAIL - wrong values for othersDeletesAreVisible(" +
                "ResultSet.TYPE_FORWARD_ONLY)", true,
                dbmt.othersDeletesAreVisible(ResultSet.TYPE_FORWARD_ONLY));
        assertEquals("FAIL - wrong value for deletesAreDetected(" +
                "ResultSet.TYPE_FORWARD_ONLY)", false,
                dbmt.deletesAreDetected(ResultSet.TYPE_FORWARD_ONLY));
        createTableT1();
        Statement stmt = createStatement(
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
        ResultSet rs = stmt.executeQuery(
                "SELECT * FROM t1 FOR UPDATE of c1");
        assertTrue("FAIL - statement should return a row", rs.next());
        assertFalse("FAIL - rs.rowDeleted() should always return false for " +
                "this type of result set", rs.rowDeleted());
        rs.deleteRow();
        rs.close();
        stmt.close();
    }
    
    /**
     * Positive test - For Forward Only resultsets, DatabaseMetaData will return
     * false for ownUpdatesAreVisible and updatesAreDetected
     * This is because, after updateRow, we position the ResultSet before the
     * next row
     */
    public void testRowUpdated() throws SQLException {
        DatabaseMetaData dbmt = getConnection().getMetaData();
        assertEquals("FAIL - wrong values for ownUpdatesAreVisible(" +
                "ResultSet.TYPE_FORWARD_ONLY)", false,
                dbmt.ownUpdatesAreVisible(ResultSet.TYPE_FORWARD_ONLY));
        assertEquals("FAIL - wrong values for othersUpdatesAreVisible(" +
                "ResultSet.TYPE_FORWARD_ONLY)", true,
                dbmt.othersUpdatesAreVisible(ResultSet.TYPE_FORWARD_ONLY));
        assertEquals("FAIL - wrong values for updatesAreDetected(" +
                "ResultSet.TYPE_FORWARD_ONLY)", false,
                dbmt.updatesAreDetected(ResultSet.TYPE_FORWARD_ONLY));
        createTableT1();
        Statement stmt = createStatement(
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
        ResultSet rs = stmt.executeQuery("SELECT * FROM t1 FOR UPDATE of c1");
        assertTrue("FAIL - statement should return a row", rs.next());
        assertFalse("FAIL - rs.rowUpdated() should always return false for " +
                "this type of result set", rs.rowUpdated());
        rs.updateLong(1,123);
        rs.updateRow();
        rs.close();
        stmt.close();
    }
    
    /**
     * Positive test - delete using updatable resultset api from a temporary
     * table
     */
    public void testDeleteRowOnTempTable() throws SQLException {
        Statement stmt = createStatement(
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
        stmt.executeUpdate("DECLARE GLOBAL TEMPORARY TABLE " +
                "SESSION.t2(c21 int, c22 int) " +
                "on commit preserve rows not logged");
        stmt.executeUpdate("insert into SESSION.t2 values(21, 1)");
        stmt.executeUpdate("insert into SESSION.t2 values(22, 1)");
        
        println("following rows in temp table before deleteRow");
        ResultSet rs = stmt.executeQuery("select * from SESSION.t2");
        assertTrue("FAIL - row not found", rs.next());
        assertEquals("FAIL - wrong value for column c21", 21, rs.getInt(1));
        assertEquals("FAIL - wrong value for column c21", 1, rs.getInt(2));
        assertTrue("FAIL - row not found", rs.next());
        assertEquals("FAIL - wrong value for column c21", 22, rs.getInt(1));
        assertEquals("FAIL - wrong value for column c21", 1, rs.getInt(2));
        rs.close();
        
        rs = stmt.executeQuery("select c21 from session.t2 for update");
        assertTrue("FAIL - row not found", rs.next());
        rs.deleteRow();
        assertTrue("FAIL - row not found", rs.next());
        assertFalse("FAIL - Column c21 should not be 0", rs.getInt(1) == 0);
        rs.deleteRow();
        println("As expected, no rows in temp table after deleteRow");
        rs.close();
        
        rs = stmt.executeQuery("select * from SESSION.t2");
        assertFalse("FAIL - all rows were deleted, rs.next() should " +
                "return false", rs.next());
        rs.close();
        
        stmt.executeUpdate("DROP TABLE SESSION.t2");
        stmt.close();
    }
    
    /**
     * Positive test - update using updatable resultset api from a temporary
     * table
     */
    public void testUpdateRowOnTempTable() throws SQLException {
        Statement stmt = createStatement(
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
        stmt.executeUpdate("DECLARE GLOBAL TEMPORARY TABLE " +
                "SESSION.t3(c31 int, c32 int) " +
                "on commit preserve rows not logged");
        stmt.executeUpdate("insert into SESSION.t3 values(21, 1)");
        stmt.executeUpdate("insert into SESSION.t3 values(22, 1)");
        
        println("following rows in temp table before deleteRow");
        ResultSet rs = stmt.executeQuery("select * from SESSION.t3");
        assertTrue("FAIL - row not found", rs.next());
        assertEquals("FAIL - wrong value for column c21", 21, rs.getInt(1));
        assertEquals("FAIL - wrong value for column c21", 1, rs.getInt(2));
        assertTrue("FAIL - row not found", rs.next());
        assertEquals("FAIL - wrong value for column c21", 22, rs.getInt(1));
        assertEquals("FAIL - wrong value for column c21", 1, rs.getInt(2));
        rs.close();
        
        rs = stmt.executeQuery("select c31 from session.t3");
        assertTrue("FAIL - row not found", rs.next());
        rs.updateLong(1,123);
        rs.updateRow();
        assertTrue("FAIL - row not found", rs.next());
        rs.updateLong(1,123);
        rs.updateRow();
        rs.close();
        
        int countRows = 0;
        rs = stmt.executeQuery("select * from SESSION.t3");
        while (rs.next()) {
            countRows++;
            assertEquals("FAIL - wrong value for column c21",
                    123, rs.getInt(1));
            assertEquals("FAIL - wrong value for column c21",
                    1, rs.getInt(2));
        }
        assertEquals("FAIL - wrong row count", 2, countRows);
        rs.close();
        
        stmt.executeUpdate("DROP TABLE SESSION.t3");
        stmt.close();
    }
    
    /**
     * Positive test - change the name of the statement when the resultset is
     * open and see if deleteRow still works
     * This test works in embedded mode since Derby can handle the change in the
     * name of the statement with an open resultset
     * But it fails under Network Server mode because JCC and Derby Net Client
     * do not allow statement name change when there an open resultset against
     * it
     */
    public void testDeleteRowWithSetCursorName() throws SQLException {
        createTableT1();
        Statement stmt = createStatement(
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
        // change the cursor name(case sensitive name) with setCursorName and
        // then try to deleteRow
        stmt.setCursorName("CURSORNOUPDATe");
        ResultSet rs = stmt.executeQuery("SELECT * FROM t1 FOR UPDATE of c1");
        assertTrue("FAIL - row not found", rs.next());
        rs.deleteRow();
        // change the cursor name one more time with setCursorName and then try
        // to deleteRow
        try {
            stmt.setCursorName("CURSORNOUPDATE1");
            assertTrue("FAIL - expected exception in network client",
                    usingEmbedded());
        } catch (SQLException e) {
            if (!usingEmbedded()) {
                assertSQLState("X0X95", e);
            } else {
                // throw unexpected exception
                throw e;
            }            
        }
        assertTrue("FAIL - row not found", rs.next());
        rs.deleteRow();
        rs.close();
        
        // verify that the table was correctly update
        String[][] expected = {{"3", "cc"}};
        JDBC.assertFullResultSet(stmt.executeQuery("SELECT * FROM t1"),
                expected, true);
        stmt.close();
    }
    
    /**
     * Positive test - change the name of the statement when the resultset is
     * open and see if updateRow still works
     * This test works in embedded mode since Derby can handle the change in the
     * name of the statement with an open resultset
     * But it fails under Network Server mode because JCC and Derby Net Client
     * do not allow statement name change when there an open resultset against
     * it
     */
    public void testUpdateRowWithSetCursorName() throws SQLException {
        createTableT1();
        Statement stmt = createStatement(
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
        // change the cursor name(case sensitive name) with setCursorName and
        // then try to updateRow
        stmt.setCursorName("CURSORNOUPDATe");
        ResultSet rs = stmt.executeQuery("SELECT * FROM t1 FOR UPDATE of c1");
        assertTrue("FAIL - row not found", rs.next());
        rs.updateLong(1, 123);
        try {
            stmt.setCursorName("CURSORNOUPDATE1");
            assertTrue("FAIL - expected exception in network client",
                    usingEmbedded());
        } catch (SQLException e) {
            if (!usingEmbedded()) {
                assertSQLState("X0X95", e);
            } else {
                // throw unexpected exception
                throw e;
            }
        }
        rs.updateRow();
        rs.close();
        
        // verify that the table was correctly update
        String[][] expected = {{"123", "aa"}, {"2", "bb"}, {"3", "cc"}};
        JDBC.assertFullResultSet(stmt.executeQuery("SELECT * FROM t1"),
                expected, true);
        stmt.close();
    }
    
    /**
     * Positive test - using correlation name for the table in the select sql
     */
    public void testDeleteRowWithCorrelationForTableName() 
            throws SQLException 
    {
        createTableT1();
        Statement stmt = createStatement(
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
        ResultSet rs = stmt.executeQuery(
                "SELECT * FROM t1 abcde FOR UPDATE of c1");
        assertTrue("FAIL - row not found", rs.next());
        assertEquals("FAIL - wrong value for column 1", 1, rs.getInt(1));
        // now try to deleteRow
        rs.deleteRow();
        rs.close();
        
        // verify that the table was correctly update
        String[][] expected = {{"2", "bb"}, {"3", "cc"}};
        JDBC.assertFullResultSet(stmt.executeQuery("SELECT * FROM t1"), 
                expected, true);
        stmt.close();
    }
    
    
    /**
     * Positive Test9b - using correlation name for updatable columns is not
     * allowed.
     */
    public void testDeleteRowWithCorrelationForColumnName() 
            throws SQLException 
    {
        createTableT1();
        Statement stmt = createStatement(
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
        // attempt to get an updatable resultset using correlation name for an
        // updatable column
        try {
            ResultSet rs = stmt.executeQuery(
                    "SELECT c1 as col1, c2 as col2 FROM t1 abcde " +
                    "FOR UPDATE of c1");
            fail("FAIL - executeQuery should have failed");
        } catch (SQLException e) {
            assertSQLState("42X42", e);
        }
        // attempt to get an updatable resultset using correlation name for an
        // readonly column. It should work
        ResultSet rs = stmt.executeQuery(
                "SELECT c1, c2 as col2 FROM t1 abcde FOR UPDATE of c1");
        assertTrue("FAIL - row not found", rs.next());
        rs.updateInt(1,11);
        rs.updateRow();
        rs.close();
        
        // verify that the table was correctly update
        String[][] expected = {{"11", "aa"}, {"2", "bb"}, {"3", "cc"}};
        JDBC.assertFullResultSet(stmt.executeQuery("SELECT * FROM t1"),
                expected, true);
        stmt.close();
        
    }
    
    /**
     * Positive test - try to updateXXX on a readonly column. Should get error
     */
    public void testUpdateXXXOnReadOnlyColumn() throws SQLException {
        createTableT1();
        Statement stmt = createStatement(
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
        ResultSet rs = stmt.executeQuery(
                "SELECT c1, c2 FROM t1 abcde FOR UPDATE of c1");
        assertTrue("FAIL - row not found", rs.next());
        // attempt to update a read only column
        try {
            rs.updateString(2,"bbbb");
            fail("FAIL - updateString on readonly column should have failed");
        } catch (SQLException e) {
            String sqlState = usingEmbedded() ? "42X31" : "XJ124";
            assertSQLState(sqlState, e);
        }
        rs.close();
        
        // verify that the table remains unchanged
        String expected[][] = {{"1", "aa"}, {"2", "bb"}, {"3", "cc"}};
        JDBC.assertFullResultSet(stmt.executeQuery("SELECT * FROM t1"),
                expected, true);
        stmt.close();
    }
    
    /**
     * Positive test - try to get an updatable resultset using correlation name
     * for a readonly column
     */
    public void testUpdateRowWithCorrelationOnTableAndColumn()
    throws SQLException {
        createTableT1();
        Statement stmt = createStatement(
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
        // attempt to get an updatable resultset using correlation name for a
        // readonly column. It should work
        ResultSet rs = stmt.executeQuery(
                "SELECT c1, c2 as col2 FROM t1 abcde FOR UPDATE of c1");
        assertTrue("FAIL - row not found", rs.next());
        rs.updateInt(1,11);
        rs.updateRow();
        rs.close();
        
        // verify that the table was correctly update
        String[][] expected = {{"11", "aa"}, {"2", "bb"}, {"3", "cc"}};
        JDBC.assertFullResultSet(stmt.executeQuery("SELECT * FROM t1"),
                expected, true);
        stmt.close();
    }
    
    /**
     * Positive test - try to updateXXX on a readonly column with correlation
     * name. Should get error
     */
    public void testUpdateXXXOnReadOnlyColumnWithCorrelationName()
    throws SQLException {
        createTableT1();
        Statement stmt = createStatement(
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
        ResultSet rs = stmt.executeQuery(
                "SELECT c1, c2 as col2 FROM t1 abcde FOR UPDATE of c1");
        assertTrue("FAIL - row not found", rs.next());
        try {
            rs.updateString(2,"bbbb");
            fail("FAIL - updateString on readonly column should have failed");
        } catch (SQLException e) {
            String sqlState = usingEmbedded() ? "42X31" : "XJ124";
            assertSQLState(sqlState, e);
        }
        rs.close();
        
        // verify that the table remains unchanged
        String expected[][] = {{"1", "aa"}, {"2", "bb"}, {"3", "cc"}};
        JDBC.assertFullResultSet(stmt.executeQuery("SELECT * FROM t1"),
                expected, true);
        stmt.close();
    }
    
    /**
     * Positive test - 2 updatable resultsets going against the same table
     */
    public void testTwoResultSetsDeletingSameRow() throws SQLException {
        createTableT1();
        Statement stmt = createStatement(
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
        Statement stmt1 = createStatement(
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
        ResultSet rs = stmt.executeQuery("SELECT * FROM t1");
        assertTrue("FAIL - row not found", rs.next());
        ResultSet rs1 = stmt1.executeQuery("SELECT * FROM t1 FOR UPDATE");
        assertTrue("FAIL - row not found", rs1.next());
        println("delete using first resultset");
        rs.deleteRow();
        try {
            // row already deleted by other result set
            rs1.deleteRow();
            fail("FAIL - delete using second resultset succedded? ");
        } catch (SQLException e) {
            assertSQLState("24000", e);
        }
        // Move to next row in the 2nd resultset and then delete using the
        // second resultset
        assertTrue("FAIL - row not found", rs1.next());
        rs1.deleteRow();
        rs.close();
        rs1.close();
        
        // verify that the table was correctly update
        String[][] expected = {{"3", "cc"}};
        JDBC.assertFullResultSet(stmt.executeQuery("SELECT * FROM t1"),
                expected, true);
        stmt.close();
        stmt1.close();
    }
    
    /**
     * Positive test - setting the fetch size to > 1 will be ignored by
     * updatable resultset. Same as updatable cursors
     */
    public void testSetFetchSizeOnUpdatableResultSet() throws SQLException {
        createTableT1();
        Statement stmt = createStatement(
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
        stmt.executeUpdate("call SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1)");
        stmt.setFetchSize(200);
        ResultSet rs = stmt.executeQuery("SELECT * FROM t1 FOR UPDATE of c1");
        // Check the Fetch Size in run time statistics output
        Statement stmt2 = createStatement();
        ResultSet rs2 = stmt2.executeQuery(
                "values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS()");
        while (rs2.next()) {
            if (rs2.getString(1).startsWith("Fetch Size")) {
                assertEquals("FAIL - wrong fetch size", "Fetch Size = 1",
                        rs2.getString(1));
            }
        }
        assertEquals("FAIL - wrong fetch size for updatable cursor",
                200, stmt.getFetchSize());
        rs.close();
        stmt.executeUpdate("call SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(0)");
        stmt.close();
    }
    
    /**
     * Positive test - make sure delete trigger gets fired when deleteRow is
     * issued
     */
    public void testDeleteRowWithDeleteTrigger() throws SQLException {
        createTable0WithTrigger();
        Statement stmt = createStatement(
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
        
        // Verify that before delete trigger got fired, row count is 0 in
        // deleteTriggerInsertIntoThisTable
        ResultSet rs = stmt.executeQuery(
                "select count(*) from deleteTriggerInsertIntoThisTable");
        assertTrue("FAIL - row not found", rs.next());
        assertEquals("FAIL - table shoud contain no rows", 0, rs.getInt(1));
        rs.close();
        
        rs = stmt.executeQuery("SELECT * FROM table0WithTriggers FOR UPDATE");
        assertTrue("FAIL - row not found", rs.next());
        assertEquals("FAIL - wrong value for column c1", 1, rs.getInt(1));
        // now try to delete row and make sure that trigger got fired
        rs.deleteRow();
        rs.close();
        
        // Verify that delete trigger got fired by verifying the row count to
        // be 1 in deleteTriggerInsertIntoThisTable
        rs = stmt.executeQuery(
                "select count(*) from deleteTriggerInsertIntoThisTable");
        assertTrue("FAIL - row not found", rs.next());
        assertEquals("FAIL - table shoud contain one row", 1, rs.getInt(1));
        rs.close();
        stmt.close();
    }
    
    /**
     * Positive test - make sure that update trigger gets fired when updateRow
     * is issue
     */
    public void testUpdateRowWithUpdateTrigger() throws SQLException {
        createTable0WithTrigger();
        Statement stmt = createStatement(
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
        // Verify that before update trigger got fired, row count is 0 in
        // updateTriggerInsertIntoThisTable
        ResultSet rs = stmt.executeQuery(
                "select count(*) from updateTriggerInsertIntoThisTable");
        assertTrue("FAIL - row not found", rs.next());
        assertEquals("FAIL - table shoud contain no rows", 0, rs.getInt(1));
        rs.close();
        
        rs = stmt.executeQuery("SELECT * FROM table0WithTriggers");
        assertTrue("FAIL - row not found", rs.next());
        assertEquals("FAIL - wrong value for column c1", 1, rs.getInt(1));
        // now try to update row and make sure that trigger got fired
        rs.updateLong(1,123);
        rs.updateRow();
        rs.close();
        
        // Verify that update trigger got fired by verifying the row count to
        // be 1 in updateTriggerInsertIntoThisTable
        rs = stmt.executeQuery(
                "select count(*) from updateTriggerInsertIntoThisTable");
        assertTrue("FAIL - row not found", rs.next());
        assertEquals("FAIL - table shoud contain one row", 1, rs.getInt(1));
        rs.close();
        stmt.close();
    }
    
    /**
     * Positive test - Another test case for delete trigger
     */
    public void testDeleteRowWithTriggerChangingRS() throws SQLException {
        createTable1WithTrigger();
        Statement stmt = createStatement(
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
        ResultSet rs = stmt.executeQuery(
                "SELECT * FROM table1WithTriggers FOR UPDATE");
        assertTrue("FAIL - row not found", rs.next());
        assertEquals("FAIL - wrong value for column c1", 1, rs.getInt(1));
        
        // this delete row will fire the delete trigger which will delete all
        // the rows from the table and from the resultset
        rs.deleteRow();
        try {
            assertFalse("FAIL - row not found", rs.next());
            rs.deleteRow();
            fail("FAIL - there should have be no more rows in the resultset " +
                    "at this point because delete trigger deleted all the " +
                    "rows");
        } catch (SQLException e) {
            String sqlState = usingEmbedded() ? "24000" : "XCL07";
            assertSQLState(sqlState, e);
        }
        rs.close();
        
        // Verify that delete trigger got fired by verifying the row count to
        // be 0 in table1WithTriggers
        rs = stmt.executeQuery("select count(*) from table1WithTriggers");
        assertTrue("FAIL - row not found", rs.next());
        assertEquals("FAIL - wrong row count", 0, rs.getInt(1));
        rs.close();
        stmt.close();
    }
    
    /**
     * Positive test - Another test case for update trigger
     */
    public void testUpdateRowWithTriggerChangingRS() throws SQLException {
        createTable1WithTrigger();
        Statement stmt = createStatement(
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
        
        // Look at the current contents of table2WithTriggers
        String[][] original = {{"1", "1"}, {"2", "2"}, {"3", "3"}, {"4", "4"}};
        JDBC.assertFullResultSet(
                stmt.executeQuery("select * from table2WithTriggers"),
                original, true);
        
        ResultSet rs = stmt.executeQuery(
                "SELECT * FROM table2WithTriggers where c1>1 FOR UPDATE");
        assertTrue("FAIL - row not found", rs.next());
        assertEquals("FAIL - wrong value for column c1", 2, rs.getInt(1));
        // this update row will fire the update trigger which will update all
        // the rows in the table to have c1=1 and hence no more rows will
        // qualify for the resultset
        rs.updateLong(2,2);
        rs.updateRow();
        try {
            assertFalse("FAIL - row not found", rs.next());
            rs.updateRow();
            fail("FAIL - there should have be no more rows in the resultset " +
                    "at this point because update trigger made all the rows " +
                    "not qualify for the resultset");
        } catch (SQLException e) {
            String sqlState = usingEmbedded() ? "24000" : "XJ121";
            assertSQLState(sqlState, e);
        }
        rs.close();
        
        // Verify that update trigger got fired by verifying that all column
        // c1s have value 1 in table2WithTriggers
        rs = stmt.executeQuery("SELECT * FROM table2WithTriggers");
        String[][] expected = {{"1", "1"}, {"1", "2"}, {"1", "3"}, {"1", "4"}};
        JDBC.assertFullResultSet(rs, expected, true);
        stmt.close();
    }
    
    /**
     * Positive test - make sure self referential delete cascade works when
     * deleteRow is issued
     */
    public void testDeleteRowSelfReferential() throws SQLException {
        createSelfReferencingTable();
        Statement stmt = createStatement(
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
        ResultSet rs = stmt.executeQuery("select * from selfReferencingT1");
        String[][] expected =
        {{"e1", null}, {"e2", "e1"}, {"e3", "e2"}, {"e4", "e3"}};
        JDBC.assertFullResultSet(rs, expected, true);
        rs.close();
        
        rs = stmt.executeQuery("SELECT * FROM selfReferencingT1");
        assertTrue("FAIL - row not found", rs.next());
        assertEquals("FAIL - wrong value for column1", "e1", rs.getString(1));
        // this delete row will cause the delete cascade constraint to delete
        // all the rows from the table and from the resultset
        rs.deleteRow();
        try {
            assertFalse("FAIL - row not found", rs.next());
            rs.deleteRow();
            fail("FAIL - there should have be no more rows in the resultset " +
                    "at this point because of the delete cascade");
        } catch (SQLException e) {
            String sqlState = usingEmbedded() ? "24000" : "XCL07";
            assertSQLState(sqlState, e);
        }
        rs.close();
        
        // Verify that delete trigger got fired by verifying the row count to
        // be 0 in selfReferencingT1
        rs = stmt.executeQuery("select count(*) from selfReferencingT1");
        assertTrue("FAIL - row not found", rs.next());
        assertEquals("FAIL - wrong row count", 0, rs.getInt(1));
        rs.close();
        stmt.close();
    }
    
    /**
     * Positive test - make sure self referential update restrict works when
     * updateRow is issued
     */
    public void testUpdateRowSelfReferential() throws SQLException {
        createSelfReferencingTable();
        Statement stmt = createStatement(
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
        ResultSet rs = stmt.executeQuery("select * from selfReferencingT2");
        String[][] expected =
        {{"e1", null}, {"e2", "e1"}, {"e3", "e2"}, {"e4", "e3"}};
        JDBC.assertFullResultSet(rs, expected, true);
        rs.close();
        
        rs = stmt.executeQuery("SELECT * FROM selfReferencingT2 FOR UPDATE");
        assertTrue("FAIL - row not found", rs.next());
        assertEquals("FAIL - wrong value for column1", "e1", rs.getString(1));
        // update row should fail because cascade constraint is update restrict
        rs.updateString(1,"e2");
        try {
            rs.updateRow();
            fail("FAIL - this update should have caused violation of foreign " +
                    "key constraint");
        } catch (SQLException e) {
            assertSQLState("23503", e);
        }
        rs.close();
        stmt.close();
    }
    
    /**
     * Positive test - With autocommit off, attempt to drop a table when there
     * is an open updatable resultset on it
     */
    public void testDropTableWithUpdatableCursorOnIt() throws SQLException {
        createTableT1();
        Statement stmt = createStatement(
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
        ResultSet rs = stmt.executeQuery("SELECT * FROM t1");
        assertTrue("FAIL - row not found", rs.next());
        // Opened an updatable resultset. Now trying to drop that table through
        // another Statement
        Statement stmt1 = createStatement();
        try {
            stmt1.executeUpdate("drop table t1");
            fail("FAIL - drop table should have failed because the updatable " +
                    "resultset is still open");
        } catch (SQLException e) {
            assertSQLState("X0X95", e);
        }
        stmt1.close();
        
        // Since autocommit is off, the drop table exception will NOT result
        // in a runtime rollback and hence updatable resultset object is still
        // open
        rs.deleteRow();
        rs.close();
        
        // verify that the data was correctly update
        rs = stmt.executeQuery("SELECT * FROM t1");
        String[][] expected = {{"2", "bb"}, {"3", "cc"}};
        JDBC.assertFullResultSet(rs, expected, true);
        rs.close();
        stmt.close();
    }
    
    /**
     * Positive test - Do deleteRow within a transaction and then rollback the
     * transaction
     */
    public void testDeleteRowAndRollbackWithTriggers() throws SQLException {
        createTable0WithTrigger();
        commit();
        Statement stmt = createStatement(
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
        
        // Verify that before delete trigger got fired, row count is 0 in
        // deleteTriggerInsertIntoThisTable
        ResultSet rs = stmt.executeQuery(
                "select count(*) from deleteTriggerInsertIntoThisTable");
        assertTrue("FAIL - row not found", rs.next());
        assertEquals("FAIL - wrong row count", 0, rs.getInt(1));
        rs.close();
        
        // Verify that before deleteRow, row count is 4 in table0WithTriggers
        rs = stmt.executeQuery("select count(*) from table0WithTriggers");
        assertTrue("FAIL - row not found", rs.next());
        assertEquals("FAIL - wrong row count", 4, rs.getInt(1));
        rs.close();
        
        rs = stmt.executeQuery("SELECT * FROM table0WithTriggers FOR UPDATE");
        assertTrue("FAIL - row not found", rs.next());
        assertEquals("FAIL - wrong value for column 1", 1, rs.getInt(1));
        println("now try to delete row and make sure that trigger got fired");
        rs.deleteRow();
        rs.close();
        
        // Verify that delete trigger got fired by verifying the row count to
        // be 1 in deleteTriggerInsertIntoThisTable
        rs = stmt.executeQuery(
                "select count(*) from deleteTriggerInsertIntoThisTable");
        assertTrue("FAIL - row not found", rs.next());
        assertEquals("FAIL - wrong row count", 1, rs.getInt(1));
        rs.close();
        
        
        // Verify that deleteRow in transaction, row count is 3 in
        // table0WithTriggers
        rs = stmt.executeQuery("select count(*) from table0WithTriggers");
        assertTrue("FAIL - row not found", rs.next());
        assertEquals("FAIL - wrong row count", 3, rs.getInt(1));
        rs.close();
        
        rollback();
        
        // Verify that after rollback, row count is back to 0 in
        // deleteTriggerInsertIntoThisTable
        rs = stmt.executeQuery(
                "select count(*) from deleteTriggerInsertIntoThisTable");
        assertTrue("FAIL - row not found", rs.next());
        assertEquals("FAIL - wrong row count", 0, rs.getInt(1));
        rs.close();
        
        // Verify that after rollback, row count is back to 4 in
        // table0WithTriggers
        rs = stmt.executeQuery("select count(*) from table0WithTriggers");
        assertTrue("FAIL - row not found", rs.next());
        assertEquals("FAIL - wrong row count", 4, rs.getInt(1));
        rs.close();
        
        // drop tables
        stmt.executeUpdate("drop table table0WithTriggers");
        stmt.executeUpdate("drop table deleteTriggerInsertIntoThisTable");
        stmt.executeUpdate("drop table updateTriggerInsertIntoThisTable");
        stmt.close();
        commit();
    }
    
    /**
     * Positive test - Do updateRow within a transaction and then rollback the
     * transaction
     */
    public void testUpdateRowAndRollbackWithTriggers() throws SQLException {
        createTable0WithTrigger();
        commit();
        Statement stmt = createStatement(
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
        
        // Verify that before update trigger got fired, row count is 0 in
        // updateTriggerInsertIntoThisTable
        ResultSet rs = stmt.executeQuery(
                "select count(*) from updateTriggerInsertIntoThisTable");
        assertTrue("FAIL - row not found", rs.next());
        assertEquals("FAIL - wrong row count", 0, rs.getInt(1));
        rs.close();
        
        rs = stmt.executeQuery("SELECT * FROM table0WithTriggers");
        assertTrue("FAIL - row not found", rs.next());
        assertEquals("FAIL - wrong value for column 1", 1, rs.getInt(1));
        println("now try to update row and make sure that trigger got fired");
        rs.updateLong(1,123);
        rs.updateRow();
        rs.close();
        
        // Verify that update trigger got fired by verifying the row count to be
        // 1 in updateTriggerInsertIntoThisTable
        rs = stmt.executeQuery(
                "select count(*) from updateTriggerInsertIntoThisTable");
        assertTrue("FAIL - row not found", rs.next());
        assertEquals("FAIL - wrong row count", 1, rs.getInt(1));
        rs.close();
        
        // Verify that new data in table0WithTriggers
        String[][] expected =
        {{"123", "1"}, {"2", "2"}, {"3", "3"}, {"4", "4"}};
        rs = stmt.executeQuery("select * from table0WithTriggers");
        JDBC.assertFullResultSet(rs, expected, true);
        rs.close();
        
        rollback();
        
        // Verify that after rollback, row count is back to 0 in
        // updateTriggerInsertIntoThisTable
        rs = stmt.executeQuery(
                "select count(*) from updateTriggerInsertIntoThisTable");
        assertTrue("FAIL - row not found", rs.next());
        assertEquals("FAIL - wrong row count", 0, rs.getInt(1));
        rs.close();
        
        String[][] original = {{"1", "1"}, {"2", "2"}, {"3", "3"}, {"4", "4"}};
        rs = stmt.executeQuery("select * from table0WithTriggers");
        JDBC.assertFullResultSet(rs, original, true);
        rs.close();
        
        // drop tables
        stmt.executeUpdate("drop table table0WithTriggers");
        stmt.executeUpdate("drop table deleteTriggerInsertIntoThisTable");
        stmt.executeUpdate("drop table updateTriggerInsertIntoThisTable");
        stmt.close();
        commit();
    }
    
    /**
     * Positive test - After deleteRow, resultset is positioned before the
     * next row
     */
    public void testResultSetPositionedBeforeNextAfterDeleteRow()
    throws SQLException {
        createTableT1();
        Statement stmt = createStatement(
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
        ResultSet rs = stmt.executeQuery("SELECT * FROM t1 FOR UPDATE");
        assertTrue("FAIL - row not found", rs.next());
        rs.deleteRow();
        // getXXX right after deleteRow will fail because resultset is not
        // positioned on a row, instead it is right before the next row
        try {
            rs.getString(1);
            fail("FAIL - getString should have failed, result set not " +
                    "positioned");
        } catch (SQLException e) {
            String sqlState = usingEmbedded() ? "24000" : "XJ121";
            assertSQLState(sqlState, e);
        }
        rs.close();
        stmt.close();
    }
    
    /**
     * Positive test - Test cancelRowUpdates method as the first updatable
     * ResultSet api on a read-only resultset
     */
    public void testCancelRowUpdatesOnReadOnlyRS() throws SQLException {
        createTableT1();
        Statement stmt = createStatement();
        ResultSet rs = stmt.executeQuery("SELECT * FROM t1");
        try {
            rs.cancelRowUpdates();
            fail("FAIL - should not have reached here because " +
                    "cancelRowUpdates is being called on a " +
                    "read-only resultset");
        } catch (SQLException e) {
            assertSQLState("XJ083", e);
        }
        rs.close();
        stmt.close();
    }
    
    /**
     * Positive test - Test updateRow method as the first updatable ResultSet
     * api on a read-only resultset
     */
    public void testUpdateRowOnReadOnlyRS() throws SQLException {
        createTableT1();
        Statement stmt = createStatement();
        ResultSet rs = stmt.executeQuery("SELECT * FROM t1");
        assertTrue("FAIL - row not found", rs.next());
        try {
            rs.updateRow();
            fail("FAIL - should not have reached here because updateRow is " +
                    "being called on a read-only resultset");
        } catch (SQLException e) {
            assertSQLState("XJ083", e);
        }
        rs.close();
        stmt.close();
    }
    
    /**
     * Positive test - Test updateXXX methods as the first updatable ResultSet
     * api on a read-only resultset
     */
    public void testUpdateXXXOnReadOnlyRS() throws SQLException {
        createAllDatatypesTable();
        Statement stmt = createStatement();
        Statement stmt1 = createStatement();
        for (int updateXXXName = 1;  
                updateXXXName <= allUpdateXXXNames.length; updateXXXName++) 
        {
            println("\nTest " + allUpdateXXXNames[updateXXXName-1] + 
                    " on a readonly resultset");
            for (int indexOrName = 1; indexOrName <= 2; indexOrName++) {
                ResultSet rs = stmt.executeQuery(
                        "SELECT * FROM AllDataTypesForTestingTable");
                rs.next();
                ResultSet rs1 = stmt1.executeQuery(
                        "SELECT * FROM AllDataTypesNewValuesData");
                rs1.next();
                if (indexOrName == 1) { //test by passing column position
                    println("Using column position as first parameter to " + 
                            allUpdateXXXNames[updateXXXName-1]);
                } else {
                    println("Using column name as first parameter to " + 
                            allUpdateXXXNames[updateXXXName-1]);
                }
                try {
                    if (updateXXXName == 1) {
                        //update column with updateShort methods
                        if (indexOrName == 1) //test by passing column position
                            rs.updateShort(1, rs1.getShort(updateXXXName));
                        else //test by passing column name
                            rs.updateShort(ColumnNames[0], 
                                    rs1.getShort(updateXXXName));
                    } else if (updateXXXName == 2) { 
                        //update column with updateInt methods
                        if (indexOrName == 1) //test by passing column position
                            rs.updateInt(1, rs1.getInt(updateXXXName));
                        else //test by passing column name
                            rs.updateInt(ColumnNames[0], 
                                    rs1.getInt(updateXXXName));
                    } else if (updateXXXName ==  3) { 
                        //update column with updateLong methods
                        if (indexOrName == 1) //test by passing column position
                            rs.updateLong(1, rs1.getLong(updateXXXName));
                        else //test by passing column name
                            rs.updateLong(ColumnNames[0], 
                                    rs1.getLong(updateXXXName));
                    } else if (updateXXXName == 4) { 
                        //update column with updateBigDecimal methods
                        if (indexOrName == 1) //test by passing column position
                            BigDecimalHandler.updateBigDecimalString(rs, 1,
                                    BigDecimalHandler.getBigDecimalString(
                                    rs1, updateXXXName));
                        else //test by passing column name
                            BigDecimalHandler.updateBigDecimalString(
                                    rs, ColumnNames[0], BigDecimalHandler.
                                    getBigDecimalString(rs1, updateXXXName));
                    } else if (updateXXXName == 5) { 
                        //update column with updateFloat methods
                        if (indexOrName == 1) //test by passing column position
                            rs.updateFloat(1, rs1.getFloat(updateXXXName));
                        else //test by passing column name
                            rs.updateFloat(ColumnNames[0], 
                                    rs1.getFloat(updateXXXName));
                    } else if (updateXXXName == 6) { 
                        //update column with updateDouble methods
                        if (indexOrName == 1) //test by passing column position
                            rs.updateDouble(1, rs1.getDouble(updateXXXName));
                        else //test by passing column name
                            rs.updateDouble(ColumnNames[0], 
                                    rs1.getDouble(updateXXXName));
                    } else if (updateXXXName == 7) { 
                        //update column with updateString methods
                        if (indexOrName == 1) //test by passing column position
                            rs.updateString(1, rs1.getString(updateXXXName));
                        else //test by passing column name
                            rs.updateString(ColumnNames[0], 
                                    rs1.getString(updateXXXName));
                    } else if (updateXXXName == 8) { 
                        //update column with updateAsciiStream methods
                        if (indexOrName == 1) //test by passing column position
                            rs.updateAsciiStream(1,
                                    rs1.getAsciiStream(updateXXXName), 4);
                        else //test by passing column name
                            rs.updateAsciiStream(ColumnNames[0],
                                    rs1.getAsciiStream(updateXXXName), 4);
                    } else if (updateXXXName == 9) { 
                        //update column with updateCharacterStream methods
                        if (indexOrName == 1) //test by passing column position
                            rs.updateCharacterStream(1,
                                    rs1.getCharacterStream(updateXXXName), 4);
                        else //test by passing column name
                            rs.updateCharacterStream(ColumnNames[0],
                                    rs1.getCharacterStream(updateXXXName), 4);
                    } else if (updateXXXName == 10) { 
                        //update column with updateByte methods
                        if (indexOrName == 1) //test by passing column position
                            rs.updateByte(1,rs1.getByte(1));
                        else //test by passing column name
                            rs.updateByte(ColumnNames[0],rs1.getByte(1));
                    } else if (updateXXXName == 11) { 
                        //update column with updateBytes methods
                        if (indexOrName == 1) //test by passing column position
                            rs.updateBytes(1,rs1.getBytes(updateXXXName));
                        else //test by passing column name
                            rs.updateBytes(ColumnNames[0],
                                    rs1.getBytes(updateXXXName));
                    } else if (updateXXXName == 12) { 
                        //update column with updateBinaryStream methods
                        if (indexOrName == 1) //test by passing column position
                            rs.updateBinaryStream(1, 
                                    rs1.getBinaryStream(updateXXXName), 2);
                        else //test by passing column name
                            rs.updateBinaryStream(ColumnNames[0],
                                    rs1.getBinaryStream(updateXXXName), 2);
                    } else if (updateXXXName == 13) { 
                        //update column with updateClob methods
                        //Don't test this method because running JDK1.3 and this
                        //jvm does not support the method
                        if (JDBC.vmSupportsJDBC3()) { 
                            if (indexOrName == 1) 
                                //test by passing column position
                                rs.updateClob(1,rs1.getClob(updateXXXName));
                            else //test by passing column name
                                rs.updateClob(ColumnNames[0],
                                        rs1.getClob(updateXXXName));
                        } else {
                            continue;
                        }
                    } else if (updateXXXName == 14) { 
                        //update column with updateDate methods
                        if (indexOrName == 1) //test by passing column position
                            rs.updateDate(1,rs1.getDate(updateXXXName));
                        else //test by passing column name
                            rs.updateDate(ColumnNames[0],
                                    rs1.getDate(updateXXXName));
                    } else if (updateXXXName == 15) { 
                        //update column with updateTime methods
                        if (indexOrName == 1) //test by passing column position
                            rs.updateTime(1,rs1.getTime(updateXXXName));
                        else //test by passing column name
                            rs.updateTime(ColumnNames[0],
                                    rs1.getTime(updateXXXName));
                    } else if (updateXXXName == 16) { 
                        //update column with updateTimestamp methods
                        if (indexOrName == 1) //test by passing column position
                            rs.updateTimestamp(1, 
                                    rs1.getTimestamp(updateXXXName));
                        else //test by passing column name
                            rs.updateTimestamp(ColumnNames[0],
                                    rs1.getTimestamp(updateXXXName));
                    } else if (updateXXXName == 17) { 
                        //update column with updateBlob methods
                        //Don't test this method because running JDK1.3 and this
                        //jvm does not support the method
                        if (JDBC.vmSupportsJDBC3()) { 
                            if (indexOrName == 1) 
                                //test by passing column position
                                rs.updateBlob(1,rs1.getBlob(updateXXXName));
                            else //test by passing column name
                                rs.updateBlob(ColumnNames[0],
                                        rs1.getBlob(updateXXXName));
                        } else {
                            continue;
                        }
                    } else if (updateXXXName == 18) { 
                        //update column with getBoolean methods
                        //use SHORT sql type column's value for testing boolean 
                        //since Derby don't support boolean datatype
                        //Since Derby does not support Boolean datatype, this 
                        //method is going to fail with the syntax error
                        if (indexOrName == 1) //test by passing column position
                            rs.updateBoolean(1, rs1.getBoolean(1));
                        else //test by passing column name
                            rs.updateBoolean(ColumnNames[0], rs1.getBoolean(1));
                    } else if (updateXXXName == 19) { 
                        //update column with updateNull methods
                        if (indexOrName == 1) //test by passing column position
                            rs.updateNull(1);
                        else //test by passing column name
                            rs.updateNull(ColumnNames[0]);
                    } else if (updateXXXName == 20) { 
                        //update column with updateArray methods - should get 
                        //not implemented exception
                        //Don't test this method because running JDK1.3 and this
                        //jvm does not support the method
                        if (JDBC.vmSupportsJDBC3()) { 
                            if (indexOrName == 1) 
                                //test by passing column position
                                rs.updateArray(1, null);
                            else //test by passing column name
                                rs.updateArray(ColumnNames[0], null);
                        } else {
                            continue;
                        }
                    } else if (updateXXXName == 21) { 
                        //update column with updateRef methods - should get not 
                        //implemented exception
                        //Don't test this method because running JDK1.3 and this
                        //jvm does not support the method
                        if (JDBC.vmSupportsJDBC3()) { 
                            if (indexOrName == 1) 
                                //test by passing column position
                                rs.updateRef(1, null);
                            else //test by passing column name
                                rs.updateRef(ColumnNames[0], null);
                        } else {
                            continue;
                        }
                    }
                    fail("FAIL - should not have reached here because " +
                            "updateXXX is being called on a read-only " +
                            "resultset");
                    return;
                } catch (SQLException e) {
                    // updateArray and updateRef are not implemented on both 
                    // drivers
                    // updateClob is not implemented in the client
                    // updateBlob is not implemented in the client
                    if (
                            (updateXXXName == 20) || (updateXXXName == 21) || 
                            (usingDerbyNetClient() && updateXXXName == 13) || 
                            (usingDerbyNetClient() && updateXXXName == 17))   
                    {
                        assertSQLState("FAIL - unexpected exception on " + 
                                allUpdateXXXNames[updateXXXName-1], "0A000", e);
                    } else {
                        assertSQLState("FAIL - unexpected exception on " + 
                                allUpdateXXXNames[updateXXXName-1], "XJ083", e);
                    }
                }
                rs.close();
                rs1.close();
            }
        }
        stmt.close();
        stmt1.close();
    }
    
    /**
     * Positive test - Test all updateXXX(excluding updateObject) methods on
     * all the supported sql datatypes
     */
    public void testUpdateXXXWithAllDatatypes() 
            throws SQLException, 
                    java.lang.IllegalArgumentException, 
                    UnsupportedEncodingException 
    {
        createAllDatatypesTable();
        commit();
        PreparedStatement pstmt = getConnection().prepareStatement(
                "SELECT * FROM AllDataTypesForTestingTable FOR UPDATE", 
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
        PreparedStatement pstmt1 = prepareStatement(
                "SELECT * FROM AllDataTypesNewValuesData");
        for (int sqlType = 1; sqlType <= allSQLTypes.length; sqlType++ ) {
            rollback();
            println("Next datatype to test is " + allSQLTypes[sqlType-1]);
            for (int updateXXXName = 1;  
                    updateXXXName <= allUpdateXXXNames.length; 
                    updateXXXName++) 
            {
                if(JDBC.vmSupportsJSR169() && (updateXXXName == 4))
                    continue;
                println("Testing " + allUpdateXXXNames[updateXXXName-1] + 
                        " on SQL type " + allSQLTypes[sqlType-1]);
                runTestUpdateXXXWithAllDatatypes(pstmt, pstmt1, sqlType, 
                        updateXXXName);
            }
        }

        rollback();
        createStatement().executeUpdate(
                "DROP TABLE AllDataTypesForTestingTable");
        createStatement().executeUpdate(
                "DROP TABLE AllDataTypesNewValuesData");
        commit();
    }
    
    private void runTestUpdateXXXWithAllDatatypes(
            PreparedStatement pstmt,
            PreparedStatement pstmt1,
            int sqlType,
            int updateXXXName) 
            throws SQLException, 
                    java.lang.IllegalArgumentException, 
                    UnsupportedEncodingException 
    {
        int checkAgainstColumn = updateXXXName;
        for (int indexOrName = 1; indexOrName <= 2; indexOrName++) {
            if (indexOrName == 1) //test by passing column position
                println("Using column position as first parameter to " + 
                        allUpdateXXXNames[updateXXXName-1]);
            else
                println("Using column name as first parameter to " + 
                        allUpdateXXXNames[updateXXXName-1]);
            ResultSet rs = pstmt.executeQuery();
            rs.next();
            ResultSet rs1 = pstmt1.executeQuery();
            rs1.next();
            try {
                if (updateXXXName == 1) {
                    //update column with updateShort methods
                    if (indexOrName == 1) //test by passing column position
                        rs.updateShort(sqlType, rs1.getShort(updateXXXName));
                    else //test by passing column name
                        rs.updateShort(ColumnNames[sqlType-1], 
                                rs1.getShort(updateXXXName));
                } else if (updateXXXName == 2) { 
                    //update column with updateInt methods
                    if (indexOrName == 1) //test by passing column position
                        rs.updateInt(sqlType, rs1.getInt(updateXXXName));
                    else //test by passing column name
                        rs.updateInt(ColumnNames[sqlType-1], 
                                rs1.getInt(updateXXXName));
                } else if (updateXXXName ==  3) { 
                    //update column with updateLong methods
                    if (indexOrName == 1) //test by passing column position
                        rs.updateLong(sqlType, rs1.getLong(updateXXXName));
                    else //test by passing column name
                        rs.updateLong(ColumnNames[sqlType-1], 
                                rs1.getLong(updateXXXName));
                } else if (updateXXXName == 4) { 
                    //update column with updateBigDecimal methods
                    if(!JDBC.vmSupportsJSR169()) {
                        if (indexOrName == 1) //test by passing column position
                            rs.updateBigDecimal(sqlType, 
                                    rs1.getBigDecimal(updateXXXName));
                        else //test by passing column name
                            rs.updateBigDecimal(ColumnNames[sqlType-1], 
                                    rs1.getBigDecimal(updateXXXName));
                    }
                } else if (updateXXXName == 5) { 
                    //update column with updateFloat methods
                    if (indexOrName == 1) //test by passing column position
                        rs.updateFloat(sqlType, rs1.getFloat(updateXXXName));
                    else //test by passing column name
                        rs.updateFloat(ColumnNames[sqlType-1], 
                                rs1.getFloat(updateXXXName));
                } else if (updateXXXName == 6) { 
                    //update column with updateDouble methods
                    if (indexOrName == 1) //test by passing column position
                        rs.updateDouble(sqlType, rs1.getDouble(updateXXXName));
                    else //test by passing column name
                        rs.updateDouble(ColumnNames[sqlType-1], 
                                rs1.getDouble(updateXXXName));
                } else if (updateXXXName == 7) { 
                    //update column with updateString methods
                    if (indexOrName == 1) //test by passing column position
                        rs.updateString(sqlType, rs1.getString(updateXXXName));
                    else //test by passing column name
                        rs.updateString(ColumnNames[sqlType-1], 
                                rs1.getString(updateXXXName));
                } else if (updateXXXName == 8) { 
                    //update column with updateAsciiStream methods
                    if (indexOrName == 1) //test by passing column position
                        rs.updateAsciiStream(sqlType,
                                rs1.getAsciiStream(updateXXXName), 4);
                    else //test by passing column name
                        rs.updateAsciiStream(ColumnNames[sqlType-1],
                                rs1.getAsciiStream(updateXXXName), 4);
                } else if (updateXXXName == 9) { 
                    //update column with updateCharacterStream methods
                    if (indexOrName == 1) //test by passing column position
                        rs.updateCharacterStream(sqlType,
                                rs1.getCharacterStream(updateXXXName), 4);
                    else //test by passing column name
                        rs.updateCharacterStream(ColumnNames[sqlType-1],
                                rs1.getCharacterStream(updateXXXName), 4);
                } else if (updateXXXName == 10) { 
                    //update column with updateByte methods
                    checkAgainstColumn = 1;
                    if (indexOrName == 1) //test by passing column position
                        rs.updateByte(sqlType,rs1.getByte(checkAgainstColumn));
                    else //test by passing column name
                        rs.updateByte(ColumnNames[sqlType-1],
                                rs1.getByte(checkAgainstColumn));
                } else if (updateXXXName == 11) { 
                    //update column with updateBytes methods
                    if (indexOrName == 1) //test by passing column position
                        rs.updateBytes(sqlType,rs1.getBytes(updateXXXName));
                    else //test by passing column name
                        rs.updateBytes(ColumnNames[sqlType-1],
                                rs1.getBytes(updateXXXName));
                } else if (updateXXXName == 12) { 
                    //update column with updateBinaryStream methods
                    if (indexOrName == 1) //test by passing column position
                        rs.updateBinaryStream(sqlType,
                                rs1.getBinaryStream(updateXXXName), 2);
                    else //test by passing column name
                        rs.updateBinaryStream(ColumnNames[sqlType-1],
                                rs1.getBinaryStream(updateXXXName), 2);
                } else if (updateXXXName == 13) { 
                    //update column with updateClob methods
                    //Don't test this method because running JDK1.3 and this jvm
                    //does not support the method
                    if (JDBC.vmSupportsJDBC3()) { 
                        if (indexOrName == 1) //test by passing column position
                            rs.updateClob(sqlType,rs1.getClob(updateXXXName));
                        else //test by passing column name
                            rs.updateClob(ColumnNames[sqlType-1],
                                    rs1.getClob(updateXXXName));
                    } else {
                        continue;
                    }
                } else if (updateXXXName == 14) { 
                    //update column with updateDate methods
                    if (indexOrName == 1) //test by passing column position
                        rs.updateDate(sqlType,rs1.getDate(updateXXXName));
                    else //test by passing column name
                        rs.updateDate(ColumnNames[sqlType-1],
                                rs1.getDate(updateXXXName));
                } else if (updateXXXName == 15) { 
                    //update column with updateTime methods
                    if (indexOrName == 1) //test by passing column position
                        rs.updateTime(sqlType,rs1.getTime(updateXXXName));
                    else //test by passing column name
                        rs.updateTime(ColumnNames[sqlType-1],
                                rs1.getTime(updateXXXName));
                } else if (updateXXXName == 16) { 
                    //update column with updateTimestamp methods
                    if (indexOrName == 1) //test by passing column position
                        rs.updateTimestamp(sqlType,
                                rs1.getTimestamp(updateXXXName));
                    else //test by passing column name
                        rs.updateTimestamp(ColumnNames[sqlType-1],
                                rs1.getTimestamp(updateXXXName));
                } else if (updateXXXName == 17) { 
                    //update column with updateBlob methods
                    //Don't test this method because running JDK1.3 and this jvm
                    //does not support the method
                    if (JDBC.vmSupportsJDBC3()) { 
                        if (indexOrName == 1) //test by passing column position
                            rs.updateBlob(sqlType,rs1.getBlob(updateXXXName));
                        else //test by passing column name
                            rs.updateBlob(ColumnNames[sqlType-1],
                                    rs1.getBlob(updateXXXName));
                    } else {
                        continue;
                    }
                } else if (updateXXXName == 18) { 
                    //update column with getBoolean methods
                    //use SHORT sql type column's value for testing boolean 
                    //since Derby don't support boolean datatype
                    //Since Derby does not support Boolean datatype, this method
                    //is going to fail with the syntax error
                    if (indexOrName == 1) //test by passing column position
                        rs.updateBoolean(sqlType, rs1.getBoolean(1));
                    else //test by passing column name
                        rs.updateBoolean(ColumnNames[sqlType-1], 
                                rs1.getBoolean(1));
                } else if (updateXXXName == 19) { 
                    //update column with updateNull methods
                    if (indexOrName == 1) //test by passing column position
                        rs.updateNull(sqlType);
                    else //test by passing column name
                        rs.updateNull(ColumnNames[sqlType-1]);
                } else if (updateXXXName == 20) { 
                    //update column with updateArray methods - should get not 
                    //implemented exception
                    //Don't test this method because running JDK1.3 and this jvm
                    //does not support the method
                    if (JDBC.vmSupportsJDBC3()) { 
                        if (indexOrName == 1) //test by passing column position
                            rs.updateArray(sqlType, null);
                        else //test by passing column name
                            rs.updateArray(ColumnNames[sqlType-1], null);
                    } else {
                        continue;
                    }
                } else if (updateXXXName == 21) { 
                    //update column with updateRef methods - should get not 
                    //implemented exception
                    //Don't test this method because running JDK1.3 and this jvm
                    //does not support the method
                    if (JDBC.vmSupportsJDBC3()) { 
                        if (indexOrName == 1) //test by passing column position
                            rs.updateRef(sqlType, null);
                        else //test by passing column name
                            rs.updateRef(ColumnNames[sqlType-1], null);
                    } else {
                        continue;
                    }
                }
                rs.updateRow();
                if ((usingDerbyNetClient() && 
                        !updateXXXRulesTableForNetworkClient[sqlType-1][updateXXXName-1].equals("PASS")) ||
                    (usingEmbedded() && 
                        !updateXXXRulesTableForEmbedded[sqlType-1][updateXXXName-1].equals("PASS"))) 
                {
                    fail("FAILURE : We shouldn't reach here. The test should " +
                            "have failed earlier on updateXXX or updateRow " +
                            "call");
                    return;
                }
                verifyData(sqlType, checkAgainstColumn);
                resetData();
            } catch (SQLException e) {
                if (usingDerbyNetClient()) {
                    assertSQLState("Error using " + 
                            allUpdateXXXNames[updateXXXName-1] +
                            " on column type " + allSQLTypes[sqlType-1], 
                            updateXXXRulesTableForNetworkClient[sqlType-1][updateXXXName-1], e);
                } else {
                    assertSQLState("Error using " + 
                            allUpdateXXXNames[updateXXXName-1] +
                            " on column type " + allSQLTypes[sqlType-1], 
                            updateXXXRulesTableForEmbedded[sqlType-1][updateXXXName-1], e);
                }
            } catch (java.lang.IllegalArgumentException ie) {
                //we are dealing with DATE/TIME/TIMESTAMP column types
                //we are dealing with updateString. The failure is because 
                //string does not represent a valid datetime value
                if ((sqlType == 14 || sqlType == 15 || sqlType == 16)) {
                    assertEquals("Should be updateString", 7, 
                            checkAgainstColumn);
                } else {
                    throw ie;
                }
            }
            rs.close();
            rs1.close();
        }
    }
    
    /**
     * Positive test - Test updateObject method
     */
    public void testUpdateObjectWithAllDatatypes() 
            throws SQLException, 
                    java.lang.IllegalArgumentException, 
                    UnsupportedEncodingException 
    {
        createAllDatatypesTable();
        commit();
        PreparedStatement pstmt = getConnection().prepareStatement(
                "SELECT * FROM AllDataTypesForTestingTable FOR UPDATE", 
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
        PreparedStatement pstmt1 = prepareStatement(
                "SELECT * FROM AllDataTypesNewValuesData");
        for (int sqlType = 1; sqlType <= allSQLTypes.length; sqlType++ ) {
            rollback();
            println("Next datatype to test is " + allSQLTypes[sqlType-1]);
            for (int updateXXXName = 1;  
                    updateXXXName <= allUpdateXXXNames.length; 
                    updateXXXName++) 
            {
                if(JDBC.vmSupportsJSR169() && (updateXXXName == 4))
                    continue;
                println("  Testing " + allUpdateXXXNames[updateXXXName-1] + 
                        " on SQL type " + allSQLTypes[sqlType-1]);
                runTestUpdateObjectWithAllDatatypes(pstmt, pstmt1, 
                        sqlType, updateXXXName);
            }
        }

        rollback();
        createStatement().executeUpdate(
                "DROP TABLE AllDataTypesForTestingTable");
        createStatement().executeUpdate(
                "DROP TABLE AllDataTypesNewValuesData");
        commit();
    }
    
    private void runTestUpdateObjectWithAllDatatypes(
            PreparedStatement pstmt,
            PreparedStatement pstmt1,
            int sqlType,
            int updateXXXName) 
            throws SQLException, 
                    java.lang.IllegalArgumentException, 
                    UnsupportedEncodingException 
    {
        String displayString;
        for (int indexOrName = 1; indexOrName <= 2; indexOrName++) {
            if (indexOrName == 1) //test by passing column position
                displayString = "  updateObject with column position &";
            else
                displayString = "  updateObject with column name &";
            ResultSet rs = pstmt.executeQuery();
            rs.next();
            ResultSet rs1 = pstmt1.executeQuery();
            rs1.next();
            try {
                if (updateXXXName == 1) { 
                    //updateObject using Short object
                    println(displayString + " Short object as parameters");
                    if (indexOrName == 1) //test by passing column position
                        rs.updateObject(sqlType, 
                                new Short(rs1.getShort(updateXXXName)));
                    else //test by passing column name
                        rs.updateObject(ColumnNames[sqlType-1], 
                                new Short(rs1.getShort(updateXXXName)));
                } else if (updateXXXName == 2) { 
                    //updateObject using Integer object
                    println(displayString + " Integer object as parameters");
                    if (indexOrName == 1) //test by passing column position
                        rs.updateObject(sqlType, 
                                new Integer(rs1.getInt(updateXXXName)));
                    else //test by passing column name
                        rs.updateObject(ColumnNames[sqlType-1], 
                                new Integer(rs1.getInt(updateXXXName)));
                } else if (updateXXXName ==  3) { 
                    //updateObject using Long object
                    println(displayString + " Long object as parameters");
                    if (indexOrName == 1) //test by passing column position
                        rs.updateObject(sqlType, 
                                new Long(rs1.getLong(updateXXXName)));
                    else //test by passing column name
                        rs.updateObject(ColumnNames[sqlType-1], 
                                new Long(rs1.getLong(updateXXXName)));
                } else if (updateXXXName == 4) { 
                    //updateObject using BigDecimal object
                    if(!JDBC.vmSupportsJSR169()) {
                        println(displayString + 
                                " BigDecimal object as parameters");
                        if (indexOrName == 1) //test by passing column position
                            rs.updateObject(sqlType, 
                                    rs1.getBigDecimal(updateXXXName));
                        else //test by passing column name
                            rs.updateObject(ColumnNames[sqlType-1],
                                    rs1.getBigDecimal(updateXXXName));
                    }
                } else if (updateXXXName == 5) { 
                    //updateObject using Float object
                    println(displayString + " Float object as parameters");
                    if (indexOrName == 1) //test by passing column position
                        rs.updateObject(sqlType, 
                                new Float(rs1.getFloat(updateXXXName)));
                    else //test by passing column name
                        rs.updateObject(ColumnNames[sqlType-1], 
                                new Float(rs1.getFloat(updateXXXName)));
                } else if (updateXXXName == 6) { 
                    //updateObject using Double object
                    println(displayString + " Double object as parameters");
                    if (indexOrName == 1) //test by passing column position
                        rs.updateObject(sqlType, 
                                new Double(rs1.getDouble(updateXXXName)));
                    else //test by passing column name
                        rs.updateObject(ColumnNames[sqlType-1], 
                                new Double(rs1.getDouble(updateXXXName)));
                } else if (updateXXXName == 7) { 
                    //updateObject using String object
                    println(displayString + " String object as parameters");
                    if (indexOrName == 1) //test by passing column position
                        rs.updateObject(sqlType,rs1.getString(updateXXXName));
                    else //test by passing column name
                        rs.updateObject(ColumnNames[sqlType-1],
                                rs1.getString(updateXXXName));
                } else if (updateXXXName == 8 || updateXXXName == 12) 
                    //updateObject does not accept InputStream and hence 
                    //this is a no-op
                    continue;
                else if (updateXXXName == 9) 
                    //updateObject does not accept Reader and hence this 
                    //is a no-op
                    continue;
                else if (updateXXXName == 10) 
                    //update column with updateByte methods
                    //non-Object parameter(which is byte in this cas) can't 
                    //be passed to updateObject mthod
                    continue;
                else if (updateXXXName == 11) { 
                    //update column with updateBytes methods
                    println(displayString + " bytes[] array as parameters");
                    if (indexOrName == 1) //test by passing column position
                        rs.updateObject(sqlType, rs1.getBytes(updateXXXName));
                    else //test by passing column name
                        rs.updateObject(ColumnNames[sqlType-1], 
                                rs1.getBytes(updateXXXName));
                } else if (updateXXXName == 13) { 
                    //update column with updateClob methods
                    //Don't test this method because running JDK1.3 and this jvm
                    //does not support the method
                    if (JDBC.vmSupportsJDBC3()) { 
                        println(displayString + " Clob object as parameters");
                        if (indexOrName == 1) //test by passing column position
                            rs.updateObject(sqlType, 
                                    rs1.getClob(updateXXXName));
                        else //test by passing column name
                            rs.updateObject(ColumnNames[sqlType-1], 
                                    rs1.getClob(updateXXXName));
                    } else {
                        continue;
                    }
                } else if (updateXXXName == 14) { 
                    //update column with updateDate methods
                    println(displayString + " Date object as parameters");
                    if (indexOrName == 1) //test by passing column position
                        rs.updateObject(sqlType, rs1.getDate(updateXXXName));
                    else //test by passing column name
                        rs.updateObject(ColumnNames[sqlType-1], 
                                rs1.getDate(updateXXXName));
                } else if (updateXXXName == 15) { 
                    //update column with updateTime methods
                    println(displayString + " Time object as parameters");
                    if (indexOrName == 1) //test by passing column position
                        rs.updateObject(sqlType, rs1.getTime(updateXXXName));
                    else //test by passing column name
                        rs.updateObject(ColumnNames[sqlType-1], 
                                rs1.getTime(updateXXXName));
                } else if (updateXXXName == 16) { 
                    //update column with updateTimestamp methods
                    println(displayString + " TimeStamp object as parameters");
                    if (indexOrName == 1) //test by passing column position
                        rs.updateObject(sqlType, 
                                rs1.getTimestamp(updateXXXName));
                    else //test by passing column name
                        rs.updateObject(ColumnNames[sqlType-1], 
                                rs1.getTimestamp(updateXXXName));
                } else if (updateXXXName == 17) { 
                    //update column with updateBlob methods
                    //Don't test this method because running JDK1.3 and this jvm
                    //does not support the method
                    if (JDBC.vmSupportsJDBC3()) { 
                        println(displayString + " Blob object as parameters");
                        if (indexOrName == 1) //test by passing column position
                            rs.updateObject(sqlType, 
                                    rs1.getBlob(updateXXXName));
                        else //test by passing column name
                            rs.updateObject(ColumnNames[sqlType-1], 
                                    rs1.getBlob(updateXXXName));
                    } else {
                        continue;
                    }
                } else if (updateXXXName == 18) {
                    //update column with getBoolean methods
                    println(displayString + " Boolean object as parameters");
                    //use SHORT sql type column's value for testing boolean 
                    //since Derby don't support boolean datatype
                    if (indexOrName == 1) //test by passing column position
                        rs.updateObject(sqlType, 
                                new Boolean(rs1.getBoolean(1)));
                    else //test by passing column name
                        rs.updateObject(ColumnNames[sqlType-1], 
                                new Boolean(rs1.getBoolean(1)));
                } else if (updateXXXName == 19) { 
                    //update column with updateNull methods
                    println(displayString + " null as parameters");
                    if (indexOrName == 1) //test by passing column position
                        rs.updateObject(sqlType, null);
                    else //test by passing column name
                        rs.updateObject(ColumnNames[sqlType-1], null);
                } else if (updateXXXName == 20 || updateXXXName == 21) 
                    //since Derby does not support Array, Ref datatype, 
                    //this is a no-op
                    continue;

                rs.updateRow();

                if ((usingDerbyNetClient() && 
                        !updateObjectRulesTableForNetworkClient[sqlType-1][updateXXXName-1].equals("PASS")) ||
                    (usingEmbedded() && 
                        !updateXXXRulesTableForEmbedded[sqlType-1][updateXXXName-1].equals("PASS"))) 
                {
                        fail("FAIL - We shouldn't reach here. The test " +
                                "should have failed earlier on updateXXX or " +
                                "updateRow call.");
                }

                // updateObject does not work with getClob on a column of type 
                // CHAR / VARCHAR / LONG VARCHAR
                // remove this check when DERBY-2105 if fixed
                if (!((sqlType == 7 || sqlType == 8 || sqlType == 9) && 
                        updateXXXName == 13)) 
                {
                    verifyData(sqlType, updateXXXName);
                }
                resetData();
            } catch (SQLException e) {
                if (usingEmbedded()) {
                    assertSQLState(updateXXXRulesTableForEmbedded[sqlType-1][updateXXXName-1], e);
                } else {
                    assertSQLState(updateObjectRulesTableForNetworkClient[sqlType-1][updateXXXName-1], e);
                }
            } catch (java.lang.IllegalArgumentException iae) {
                //we are dealing with DATE/TIME/TIMESTAMP column types
                //we are dealing with updateString. The failure is because 
                //string does not represent a valid datetime value
                if (sqlType == 14 || sqlType == 15 || sqlType == 16) {
                    assertEquals("FAIL - wrong updateXXX function", 7, updateXXXName);
                } else {
                    throw iae;
                }
            }
            rs.close();
            rs1.close();
        }
    }
    
    /**
     * Positive test - Test cancelRowUpdates after updateXXX methods on all
     * the supported sql datatypes
     */
    public void testUpdateXXXWithCancelRowUpdates() throws SQLException {
        createAllDatatypesTable();
        Statement stmt = createStatement(
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
        Statement stmt1 = createStatement();
        ResultSet rs = stmt.executeQuery(
                "SELECT * FROM AllDataTypesForTestingTable FOR UPDATE");
        rs.next();
        ResultSet rs1 = stmt1.executeQuery(
                "SELECT * FROM AllDataTypesNewValuesData");
        rs1.next();
        
        // updateShort and then cancelRowUpdates
        short s = rs.getShort(1);
        rs.updateShort(1, rs1.getShort(1));
        assertEquals("FAIL - wrong value returned by getXXX method",
                rs1.getShort(1), rs.getShort(1));
        rs.cancelRowUpdates();
        assertEquals("FAIL - wrong value returned by getXXX method",
                s, rs.getShort(1));
        
        // updateInt and then cancelRowUpdates
        int i = rs.getInt(2);
        rs.updateInt(2, rs1.getInt(2));
        assertEquals("FAIL - wrong value returned by getXXX method",
                rs1.getInt(2), rs.getInt(2));
        rs.cancelRowUpdates();
        assertEquals("FAIL - wrong value returned by getXXX method",
                i, rs.getInt(2));
        
        // updateLong and then cancelRowUpdates
        long l = rs.getLong(3);
        rs.updateLong(3, rs1.getLong(3));
        assertEquals("FAIL - wrong value returned by getXXX method",
                rs1.getLong(3), rs.getLong(3));
        rs.cancelRowUpdates();
        assertEquals("FAIL - wrong value returned by getXXX method",
                l, rs.getLong(3));
        
        // updateBigDecimal and then cancelRowUpdates
        String bdString = BigDecimalHandler.getBigDecimalString(rs, 4);
        BigDecimalHandler.updateBigDecimalString(rs, 4,
                BigDecimalHandler.getBigDecimalString(rs1, 4));
        assertEquals("FAIL - wrong value returned by getXXX method",
                BigDecimalHandler.getBigDecimalString(rs1, 4),
                BigDecimalHandler.getBigDecimalString(rs, 4));
        rs.cancelRowUpdates();
        assertEquals("FAIL - wrong value returned by getXXX method",
                bdString, BigDecimalHandler.getBigDecimalString(rs, 4));
        
        // updateFloat and then cancelRowUpdates
        float f = rs.getFloat(5);
        rs.updateFloat(5, rs1.getFloat(5));
        assertTrue("FAIL - wrong value returned by getXXX method expected " +
                rs1.getFloat(5) + " but was " + rs.getFloat(5),
                rs1.getFloat(5) == rs.getFloat(5));
        rs.cancelRowUpdates();
        assertTrue("FAIL - wrong value returned by getXXX method expected " +
                f + " but was " + rs.getFloat(5),
                f == rs.getFloat(5));
        
        println("  updateDouble and then cancelRowUpdates");
        double db = rs.getDouble(6);
        rs.updateDouble(6, rs1.getDouble(6));
        assertTrue("FAIL - wrong value returned by getXXX method expected " +
                rs1.getDouble(6) + " but was " + rs.getDouble(6),
                rs1.getDouble(6) == rs.getDouble(6));
        rs.cancelRowUpdates();
        assertTrue("FAIL - wrong value returned by getXXX method expected " +
                db + " but was " + rs.getDouble(6),
                db == rs.getDouble(6));
        
        // updateString and then cancelRowUpdates
        String str = rs.getString(7);
        rs.updateString(7, rs1.getString(7));
        assertEquals("FAIL - wrong value returned by getXXX method",
                rs1.getString(7), rs.getString(7));
        rs.cancelRowUpdates();
        assertEquals("FAIL - wrong value returned by getXXX method",
                str, rs.getString(7));
        
        // updateAsciiStream and then cancelRowUpdates
        str = rs.getString(8);
        rs.updateAsciiStream(8,rs1.getAsciiStream(8), 4);
        assertTrue("FAIL - wrong value returned by getXXX method",
                rs.getString(8).equals(rs1.getString(8)));
        rs.cancelRowUpdates();
        assertTrue("FAIL - wrong value returned by getXXX method",
                rs.getString(8).equals(str));
        
        // updateCharacterStream and then cancelRowUpdates
        str = rs.getString(9);
        rs.updateCharacterStream(9,rs1.getCharacterStream(9), 4);
        assertTrue("FAIL - wrong value returned by getXXX method",
                rs.getString(9).equals(rs1.getString(9)));
        rs.cancelRowUpdates();
        assertTrue("FAIL - wrong value returned by getXXX method",
                rs.getString(9).equals(str));
        
        // updateByte and then cancelRowUpdates");
        s = rs.getShort(1);
        rs.updateByte(1,rs1.getByte(1));
        assertEquals("FAIL - wrong value returned by getXXX method",
                rs1.getShort(1), rs.getShort(1));
        rs.cancelRowUpdates();
        assertEquals("FAIL - wrong value returned by getXXX method",
                s, rs.getShort(1));
        
        
        // updateBytes and then cancelRowUpdates
        byte[] bts = rs.getBytes(11);
        rs.updateBytes(11,rs1.getBytes(11));
        assertTrue("FAIL - wrong value returned by getXXX method",
                java.util.Arrays.equals(rs.getBytes(11),rs1.getBytes(11)));
        rs.cancelRowUpdates();
        assertTrue("FAIL - wrong value returned by getXXX method",
                java.util.Arrays.equals(rs.getBytes(11),bts));
        
        // updateBinaryStream and then cancelRowUpdates
        bts = rs.getBytes(12);
        rs.updateBinaryStream(12,rs1.getBinaryStream(12), 2);
        assertTrue("FAIL - wrong value returned by getXXX method",
                java.util.Arrays.equals(rs.getBytes(12),rs1.getBytes(12)));
        rs.cancelRowUpdates();
        assertTrue("FAIL - wrong value returned by getXXX method",
                java.util.Arrays.equals(rs.getBytes(12),bts));
        
        // updateDate and then cancelRowUpdates
        Date date = rs.getDate(14);
        rs.updateDate(14,rs1.getDate(14));
        assertTrue("FAIL - wrong value returned by getXXX method",
                rs.getDate(14).compareTo(rs1.getDate(14)) == 0);
        rs.cancelRowUpdates();
        assertTrue("FAIL - wrong value returned by getXXX method",
                rs.getDate(14).compareTo(date) == 0);
        
        // updateTime and then cancelRowUpdates
        Time time = rs.getTime(15);
        rs.updateTime(15,rs1.getTime(15));
        assertTrue("FAIL - wrong value returned by getXXX method",
                rs.getTime(15).compareTo(rs1.getTime(15)) == 0);
        rs.cancelRowUpdates();
        assertTrue("FAIL - wrong value returned by getXXX method",
                rs.getTime(15).compareTo(time) == 0);
        
        // updateTimestamp and then cancelRowUpdates
        Timestamp timeStamp = rs.getTimestamp(16);
        rs.updateTimestamp(16,rs1.getTimestamp(16));
        assertEquals("FAIL - wrong value returned by getXXX method",
                rs1.getTimestamp(16).toString(),
                rs.getTimestamp(16).toString());
        rs.cancelRowUpdates();
        assertEquals("FAIL - wrong value returned by getXXX method",
                timeStamp.toString(), rs.getTimestamp(16).toString());
        
        //Don't test this when running JDK1.3/in Network Server because they
        //both do not support updateClob and updateBlob
        if (usingEmbedded() && JDBC.vmSupportsJDBC3()) {
            println("  updateClob and then cancelRowUpdates");
            String clb1 = rs.getString(13);
            rs.updateClob(13, rs1.getClob(13));
            assertEquals("FAIL - wrong value returned by getXXX method",
                    rs1.getString(13), rs.getString(13));
            rs.cancelRowUpdates();
            assertEquals("FAIL - wrong value returned by getXXX method",
                    clb1, rs.getString(13));
            
            println("  updateBlob and then cancelRowUpdates");
            bts = rs.getBytes(17);
            rs.updateBlob(17,rs1.getBlob(17));
            assertTrue("FAIL - wrong value returned by getXXX method",
                    java.util.Arrays.equals(rs.getBytes(17),rs1.getBytes(17)));
            rs.cancelRowUpdates();
            assertTrue("FAIL - wrong value returned by getXXX method",
                    java.util.Arrays.equals(rs.getBytes(17),bts));
        }
        
        rs.close();
        rs1.close();
        stmt.close();
        stmt1.close();
    }
    
    /**
     * Positive test - after updateXXX, try cancelRowUpdates and then
     * deleteRow
     */
    public void testCancelRowUpdatesAndDeleteRow() throws SQLException {
        createTableT1();
        Statement stmt = createStatement(
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
        ResultSet rs = stmt.executeQuery("SELECT * FROM t1 FOR UPDATE");
        assertTrue("FAIL - row not found", rs.next());
        assertEquals("FAIL - wrong value for column 1 before updateInt",
                1, rs.getInt(1));
        rs.updateInt(1,234);
        assertEquals("FAIL - wrong value for column 1 before updateInt",
                234, rs.getInt(1));
        println("now cancelRowUpdates on the row");
        rs.cancelRowUpdates();
        assertEquals("FAIL - wrong value for column 1 after cancelRowUpdates",
                1, rs.getInt(1));
        rs.deleteRow();
        
        // calling updateRow after deleteRow w/o first positioning the ResultSet
        // on the next row will fail");
        try {
            rs.updateRow();
            fail("FAIL - updateRow should have failed because ResultSet is " +
                    "not positioned on a row");
        } catch (SQLException e) {
            String sqlState = usingEmbedded() ? "24000" : "XJ121";
            assertSQLState(sqlState, e);
        }
        println("Position the ResultSet with next()");
        assertTrue("FAIL - row not found", rs.next());
        println("Should be able to updateRow() on the current row now");
        rs.updateString(2,"234");
        rs.updateRow();
        rs.close();
        
        String[][] expected = {{"2", "234"}, {"3", "cc"}};
        rs = stmt.executeQuery("select * from t1");
        JDBC.assertFullResultSet(rs, expected, true);
        rs.close();
        
        stmt.close();
    }
    
    /**
     * Positive test - issue cancelRowUpdates without any updateXXX
     */
    public void testCancelRowUpdatesWithoutUpdateXXX() throws SQLException {
        createTableT1();
        Statement stmt = createStatement(
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
        ResultSet rs = stmt.executeQuery("SELECT * FROM t1");
        assertTrue("FAIL - row not found", rs.next());
        rs.cancelRowUpdates();
        rs.close();
        stmt.close();
    }
    
    /**
     * Positive test - issue updateRow without any updateXXX will not move
     * the resultset position
     */
    public void testUpdateRowWithoutUpdateXXX() throws SQLException {
        createTableT1();
        Statement stmt = createStatement(
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
        ResultSet rs = stmt.executeQuery("SELECT * FROM t1 FOR UPDATE");
        assertTrue("FAIL - row not found", rs.next());
        // this will not move the resultset to right before the next row because
        // there were no updateXXX issued before updateRow
        rs.updateRow();
        rs.updateRow();
        rs.close();
        
        // verify that the table is unchanged
        String[][] original = {{"1", "aa"}, {"2", "bb"}, {"3", "cc"}};
        rs = stmt.executeQuery("select * from t1");
        JDBC.assertFullResultSet(rs, original, true);
        rs.close();
        
        stmt.close();
    }
    
    /**
     * Positive test - issue updateXXX and then deleteRow
     */
    public void testUpdateXXXAndDeleteRow() throws SQLException {
        createTableT1();
        Statement stmt = createStatement(
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
        ResultSet rs = stmt.executeQuery("SELECT * FROM t1");
        assertTrue("FAIL - row not found", rs.next());
        rs.updateInt(1,1234);
        rs.updateString(2,"aaaaa");
        rs.deleteRow();
        try {
            rs.updateRow();
            fail("FAIL - deleteRow should have moved the ResultSet to right" +
                    " before the next row");
        } catch (SQLException e) {
            String sqlState = usingEmbedded() ? "24000" : "XJ121";
            assertSQLState(sqlState, e);
        }
        try {
            rs.updateInt(1,2345);
            fail("FAIL - deleteRow should have moved the ResultSet to right" +
                    " before the next row");
        } catch (SQLException e) {
            String sqlState = usingEmbedded() ? "24000" : "XJ121";
            assertSQLState(sqlState, e);;
        }
        try {
            rs.getInt(1);
            fail("FAIL - deleteRow should have moved the ResultSet to right" +
                    " before the next row");
        } catch (SQLException e) {
            String sqlState = usingEmbedded() ? "24000" : "XJ121";
            assertSQLState(sqlState, e);
        }
        rs.close();
        
        // verify that the table was correctly update
        String[][] expected = {{"2", "bb"}, {"3", "cc"}};
        rs = stmt.executeQuery("select * from t1");
        JDBC.assertFullResultSet(rs, expected, true);
        rs.close();
        
        stmt.close();
    }
    
    /**
     * Positive test - issue updateXXXs and then move off the row, the changes
     * should be ignored
     */
    public void testUpdateXXXAndMoveNext() throws SQLException {
        createTableT1();
        Statement stmt = createStatement(
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
        ResultSet rs = stmt.executeQuery("SELECT * FROM t1 FOR UPDATE");
        assertTrue("FAIL - row not found", rs.next());
        assertEquals("FAIL - wrong value for column 1", 1, rs.getInt(1));
        println("  Issue updateInt to change the column's value to 2345");
        rs.updateInt(1,2345);
        // Move to next row w/o issuing updateRow
        // the changes made on the earlier row should have be ignored because
        // we moved off that row without issuing updateRow
        rs.next();
        rs.close();
        
        // Make sure that changes didn't make it to the database
        String[][] original = {{"1", "aa"}, {"2", "bb"}, {"3", "cc"}};
        rs = stmt.executeQuery("select * from t1");
        JDBC.assertFullResultSet(rs, original, true);
        rs.close();
        stmt.close();
    }
    
    /**
     * Positive test - issue multiple updateXXXs and then a updateRow
     */
    public void testMultipleUpdateXXXAndUpdateRow() throws SQLException {
        createTableT1();
        Statement stmt = createStatement(
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
        ResultSet rs = stmt.executeQuery("SELECT * FROM t1 FOR UPDATE");
        assertTrue("FAIL - row not found", rs.next());
        assertEquals("FAIL - wrong value for column 1", 1, rs.getInt(1));
        rs.updateInt(1,2345);
        // Issue another updateInt on the same row and column to change the
        // column's value to 9999
        rs.updateInt(1,9999);
        // Issue updateString to change the column's value to 'xxxxxxx'
        rs.updateString(2,"xxxxxxx");
        println("  Now issue updateRow");
        rs.updateRow();
        rs.close();
        
        // Make sure that changes made it to the database correctly
        String[][] expected = {{"9999", "xxxxxxx"}, {"2", "bb"}, {"3", "cc"}};
        rs = stmt.executeQuery("SELECT * FROM t1");
        JDBC.assertFullResultSet(rs, expected, true);
        rs.close();
        stmt.close();
    }
    
    /**
     * Positive test - call updateXXX methods on only columns that correspond
     * to a column in the table
     */
    public void testUpdateXXXOnTableColumn() throws SQLException {
        createTableT1();
        Statement stmt = createStatement(
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
        ResultSet rs = stmt.executeQuery("SELECT 1, 2, c1, c2 FROM t1");
        assertTrue("FAIL - row not found", rs.next());
        rs.updateInt(3,22);
        rs.updateRow();
        rs.close();
        
        // Make sure that changes made it to the database correctly
        String[][] expected = {{"22", "aa"}, {"2", "bb"}, {"3", "cc"}};
        rs = stmt.executeQuery("SELECT * FROM t1");
        JDBC.assertFullResultSet(rs, expected, true);
        rs.close();
        stmt.close();
    }
    
    /**
     * Positive test - case sensitive table and column names
     */
    public void testCaseSensitiveTableAndColumnName() throws SQLException {
        Statement stmt = createStatement(
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
        stmt.executeUpdate("create table \"t1\" (\"c11\" int, c12 int)");
        stmt.executeUpdate("insert into \"t1\" values(1, 2), (2,3)");
        
        ResultSet rs = stmt.executeQuery(
                "SELECT \"c11\", \"C12\" FROM \"t1\" FOR UPDATE");
        assertTrue("FAIL - row not found", rs.next());
        rs.updateInt(1,11);
        rs.updateInt(2,22);
        rs.updateRow();
        assertTrue("FAIL - row not found", rs.next());
        rs.deleteRow();
        rs.close();
        
        // Make sure that changes made it to the database correctly
        rs = stmt.executeQuery(
                "SELECT \"c11\", \"C12\" FROM \"t1\" FOR UPDATE");
        String[][] expected = {{"11", "22"}};
        JDBC.assertFullResultSet(rs, expected, true);
        rs.close();
        stmt.close();
    }
    
    /**
     * Positive test - table and column names with spaces in middle and end
     */
    public void testTableAndColumnNameWithSpaces() throws SQLException {
        Statement stmt = createStatement(
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
        stmt.executeUpdate(
                "create table \" t 11 \" (\" c 111 \" int, c112 int)");
        stmt.executeUpdate("insert into \" t 11 \" values(1, 2), (2,3)");
        
        ResultSet rs = stmt.executeQuery(
                "SELECT \" c 111 \", \"C112\" FROM \" t 11 \" ");
        assertTrue("FAIL - row not found", rs.next());
        rs.updateInt(1,11);
        rs.updateInt(2,22);
        rs.updateRow();
        assertTrue("FAIL - row not found", rs.next());
        rs.deleteRow();
        rs.close();
        
        // Make sure for table \" t 11 \" that changes made it to the database
        // correctly
        rs = stmt.executeQuery("SELECT \" c 111 \", \"C112\" FROM \" t 11 \" ");
        String[][] expected = {{"11", "22"}};
        JDBC.assertFullResultSet(rs, expected, true);
        rs.close();
        stmt.close();
    }
    
    /**
     * Positive test - call updateXXX methods on column that is not in for
     * update columns list
     */
    public void testUpdateXXXNotForUpdateColumns() throws SQLException {
        createTableT1();
        Statement stmt = createStatement(
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
        ResultSet rs = stmt.executeQuery(
                "SELECT c1, c2 FROM t1 FOR UPDATE of c1");
        assertTrue("FAIL - row not found", rs.next());
        try {
            rs.updateInt(2,22);
            fail("FAIL - updateXXX methods should fail when the column is " +
                    "not in the FOR UPDATE clause");
        } catch (SQLException e) {
            String sqlState = usingEmbedded() ? "42X31" : "XJ124";
            assertSQLState(sqlState, e);
        }
        // updateRow should pass
        rs.updateRow();
        rs.close();
        
        // Make sure the contents of table are unchanged
        String[][] expected = {{"1", "aa"}, {"2", "bb"}, {"3", "cc"}};
        rs = stmt.executeQuery("SELECT * FROM t1");
        JDBC.assertFullResultSet(rs, expected, true);
        rs.close();
        stmt.close();
    }
    
    /**
     * Positive test - try to update a table from another schema
     */
    public void testUpdateTableDifferentSchema() throws SQLException {
        Statement stmt = createStatement(
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
        createTableT1();
        stmt.executeUpdate("create schema s2");
        stmt.executeUpdate("create table s2.t1 " +
                "(c1s2t1 int, c2s2t1 smallint, c3s2t2 double)");
        stmt.executeUpdate("insert into s2.t1 values(1,2,2.2),(1,3,3.3)");
        
        // contents of table t1
        String[][] expected_t1 = {{"1", "aa"}, {"2", "bb"}, {"3", "cc"}};
        ResultSet rs = stmt.executeQuery("SELECT * FROM t1");
        JDBC.assertFullResultSet(rs, expected_t1, true);
        rs.close();
        
        // contents of table t1 from schema s2
        String[][] original_s2_t1 = {{"1", "2", "2.2"}, {"1", "3", "3.3"}};
        rs = stmt.executeQuery("select * from s2.t1");
        JDBC.assertFullResultSet(rs, original_s2_t1, true);
        rs.close();
        
        // Try to change contents of 2nd column of s2.t1 using updateRow
        rs = stmt.executeQuery("SELECT * FROM s2.t1 FOR UPDATE");
        rs.next();
        rs.updateInt(2,1);
        rs.updateRow();
        rs.next();
        rs.updateInt(2,1);
        rs.updateRow();
        rs.close();
        
        // Make sure that changes made to the right table t1
        // contents of table t1 from current schema should have remained
        // unchanged
        rs = stmt.executeQuery("SELECT * FROM t1");
        JDBC.assertFullResultSet(rs, expected_t1, true);
        rs.close();
        
        // contents of table t1 from schema s2 should have changed
        String[][] expected_s2_t1 = {{"1", "1", "2.2"}, {"1", "1", "3.3"}};
        rs = stmt.executeQuery("select * from s2.t1");
        JDBC.assertFullResultSet(rs, expected_s2_t1, true);
        rs.close();
        stmt.close();
    }
    
    /**
     * Positive test - in autocommit mode, check that updateRow and deleteRow
     * does not commit
     */
    public void testUpdateRowDeleteRowDoNotCommit() throws SQLException {
        getConnection().setAutoCommit(true);
        createTableT1();
        commit();
        String expected[][] = {{"1", "aa"}, {"2", "bb"}, {"3", "cc"}};
        
        // First try deleteRow and updateRow on *first* row of result set
        Statement stmt = createStatement(
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
        ResultSet rs = stmt.executeQuery("SELECT * FROM t1 FOR UPDATE");
        assertTrue("FAIL - row not found", rs.next());
        rs.deleteRow();
        rollback();
        rs.close();
        JDBC.assertFullResultSet(stmt.executeQuery("SELECT * FROM t1"),
                expected, true);
        
        rs = stmt.executeQuery("SELECT * FROM t1 FOR UPDATE");
        assertTrue("FAIL - row not found", rs.next());
        rs.updateInt(1,-rs.getInt(1));
        rs.updateRow();
        rollback();
        rs.close();
        JDBC.assertFullResultSet(stmt.executeQuery("SELECT * FROM t1"),
                expected, true);
        
        // Now try the same on the *last* row in the result set
        rs = stmt.executeQuery("SELECT COUNT(*) FROM t1");
        assertTrue("FAIL - row not found", rs.next());
        int count = rs.getInt(1);
        rs.close();
        
        rs = stmt.executeQuery("SELECT * FROM t1 FOR UPDATE");
        for (int j = 0; j < count; j++) {
            assertTrue("FAIL - row not found", rs.next());
        }
        rs.deleteRow();
        rollback();
        rs.close();
        JDBC.assertFullResultSet(stmt.executeQuery("SELECT * FROM t1"),
                expected, true);
        
        rs = stmt.executeQuery("SELECT * FROM t1 FOR UPDATE");
        for (int j = 0; j < count; j++) {
            assertTrue("FAIL - row not found", rs.next());
        }
        rs.updateInt(1,-rs.getInt(1));
        rs.updateRow();
        rollback();
        rs.close();
        JDBC.assertFullResultSet(stmt.executeQuery("SELECT * FROM t1"),
                expected, true);
        
        stmt.executeUpdate("DROP TABLE t1");
        stmt.close();
        commit();
    }
    
    /**
     * Positive test - moveToInsertRow, insertRow, getXXX and moveToCurrentRow
     */
    public void testInsertRow() throws SQLException {
        createTableT4();
        int c41, c42, c41old, c42old;
        Statement stmt = createStatement(
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
        
        ResultSet rs = stmt.executeQuery("SELECT * FROM t4");
        assertTrue("FAIL - row not found", rs.next());
        c41old = rs.getInt(1);
        c42old = rs.getInt(2);
        
        // Test moveToInsertRow + insertRow
        rs.moveToInsertRow();
        rs.updateInt(1, 5);
        rs.updateInt(2, 5);
        rs.insertRow();
        
        // Test getXXX on insertRow
        c41 = rs.getInt(1);
        c42 = rs.getInt(2);
        assertEquals("FAIL - wrong value for column c41", 5, c41);
        assertEquals("FAIL - wrong value for column c42", 5, c42);
        
        // Test moveToCurrentRow
        rs.moveToCurrentRow();
        assertEquals("FAIL - wrong value for column c41", c41old, rs.getInt(1));
        assertEquals("FAIL - wrong value for column c42", c42old, rs.getInt(2));
        
        // Test calling moveToCurrentRow from currentRow
        rs.moveToCurrentRow();
        assertEquals("FAIL - wrong value for column c41", c41old, rs.getInt(1));
        assertEquals("FAIL - wrong value for column c42", c42old, rs.getInt(2));
        
        // Test getXXX from insertRow
        rs.moveToInsertRow();
        rs.updateInt(1, 6);
        rs.updateInt(2, 4);
        c41 = rs.getInt(1);
        c42 = rs.getInt(2);
        assertEquals("FAIL - wrong value for column c41", 6, c41);
        assertEquals("FAIL - wrong value for column c42", 4, c42);
        
        // Test that value for columns are undefined when moving to insertRow
        rs.moveToInsertRow();
        c41 = rs.getInt(1);
        assertEquals("FAIL - wrong value for column c41", 0, c41);
        assertTrue("FAIL - value should be undefined when moving to insertRow",
                rs.wasNull());
        c42 = rs.getInt(2);
        assertEquals("FAIL - wrong value for column c42", 0, c42);
        assertTrue("FAIL - value should be undefined when moving to insertRow",
                rs.wasNull());
        
        // Test insertRow without setting value for NOT NULL column
        rs.moveToInsertRow();
        rs.updateInt(2, 7);
        try {
            rs.insertRow();
            fail("FAIL - should have failed can not insert NULL into " +
                    "not null column");
        } catch (SQLException se) {
            assertSQLState("23502", se);
        }
        
        rs.close();
        
        // Make sure the contents of table are unchanged
        String[][] expected =
        {{"1", "1"}, {"2", "2"}, {"3", "3"}, {"4", "4"}, {"5", "5"}};
        rs = stmt.executeQuery("SELECT * FROM t4");
        JDBC.assertFullResultSet(rs, expected, true);
        rs.close();
        stmt.close();
    }
    
    /**
     * Negative test - run updateRow and deleterow when positioned at insertRow
     */
    public void testUpdateRowDeleteRowFromInsertRow() throws SQLException {
        createTableT4();
        Statement stmt = createStatement(
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
        ResultSet rs = stmt.executeQuery("SELECT * FROM t4");
        assertTrue("FAIL - row not found", rs.next());
        
        rs.moveToInsertRow();
        rs.updateInt(1, 6);
        rs.updateInt(2, 6);
        // Test updateRow from insertRow
        try {
            rs.updateRow();
            fail("FAIL - can not call updateRow from insertRow");
        } catch (SQLException se) {
            assertSQLState("24000", se);
        }
        // Test deleteRow from insertRow
        try {
            rs.deleteRow();
            fail("FAIL - can not call deleteRow from insertRow");
        } catch (SQLException se) {
            assertSQLState("24000", se);
        }
        rs.close();
        
        // Make sure the contents of table are unchanged
        String[][] expected = {{"1", "1"}, {"2", "2"}, {"3", "3"}, {"4", "4"}};
        rs = stmt.executeQuery("SELECT * FROM t4");
        JDBC.assertFullResultSet(rs, expected, true);
        rs.close();
        stmt.close();
    }
    
    /**
     * Negative test - Try to insertRow from current row
     */
    public void testInsertRowFromCurrentRow() throws SQLException {
        createTableT4();
        Statement stmt = createStatement(
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
        ResultSet rs = stmt.executeQuery("SELECT * FROM t4");
        assertTrue("FAIL - row not found", rs.next());
        
        rs.moveToCurrentRow();
        try {
            rs.insertRow();
            fail("FAIL - insert row not allowed from current row");
        } catch (SQLException se) {
            assertSQLState("XJ086", se);
        }
        rs.close();
        stmt.close();
    }
    
    /**
     * Positive test - try insertRow from different positions
     */
    public void testInsertRowFromDifferentPositions() throws SQLException {
        createTableT4();
        int c41, c42;
        Statement stmt = createStatement(
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
        ResultSet rs = stmt.executeQuery("SELECT * FROM t4");
        assertTrue("FAIL - row not found", rs.next());
        
        rs = stmt.executeQuery("SELECT * FROM t4 WHERE c41 <= 5");
        rs.moveToInsertRow();
        rs.updateInt(1, 1000);
        rs.updateInt(2, 1000);
        rs.insertRow();
        while (rs.next()) {
            c41 = rs.getInt(1);
            c42 = rs.getInt(2);
            rs.moveToInsertRow();
            rs.updateInt(1, c41 + 100);
            rs.updateInt(2, c42 + 100);
            rs.insertRow();
        }
        rs.moveToInsertRow();
        rs.updateInt(1, 2000);
        rs.updateInt(2, 2000);
        rs.insertRow();
        rs.close();
        
        String[][] expected = {
            {"1", "1"},
            {"2", "2"},
            {"3", "3"},
            {"4", "4"},
            {"1000", "1000"},
            {"101", "101"},
            {"102", "102"},
            {"103", "103"},
            {"104", "104"},
            {"2000", "2000"}
        };
        rs = stmt.executeQuery("SELECT * FROM t4");
        JDBC.assertFullResultSet(rs, expected, true);
        rs.close();
        
        stmt.close();
    }
    
    /**
     * Positive test - InsertRow leaving a nullable columns = NULL
     */
    public void testInsertRowWithNullColumn() throws SQLException {
        createTableT4();
        Statement stmt = createStatement(
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
        ResultSet rs = stmt.executeQuery("SELECT * FROM t4");
        assertTrue("FAIL - row not found", rs.next());
        rs.moveToInsertRow();
        rs.updateInt(1, 7);
        rs.insertRow();
        rs.close();
        
        String[][] expected =
        {{"1", "1"}, {"2", "2"}, {"3", "3"}, {"4", "4"}, {"7", null}};
        rs = stmt.executeQuery("SELECT * FROM t4");
        JDBC.assertFullResultSet(rs, expected, true);
        rs.close();
        stmt.close();
        
    }
    
    /**
     * Positive and negative tests - Commit while on insertRow
     */
    public void xTestInsertRowAfterCommit() throws SQLException {
        createTableT4();
        getConnection().setHoldability(ResultSet.HOLD_CURSORS_OVER_COMMIT);
        Statement stmt = createStatement(
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
        ResultSet rs = stmt.executeQuery("SELECT * FROM t4");
        assertTrue("FAIL - row not found", rs.next());
        rs.moveToInsertRow();
        rs.updateInt(1, 8);
        rs.updateInt(2, 8);
        commit();
        rs.insertRow();
        rs.close();
        stmt.close();
        
        getConnection().setHoldability(ResultSet.CLOSE_CURSORS_AT_COMMIT);
        stmt = createStatement(
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
        rs = stmt.executeQuery("SELECT * FROM t4");
        assertTrue("FAIL - row not found", rs.next());
        rs.moveToInsertRow();
        rs.updateInt(1, 82);
        rs.updateInt(2, 82);
        commit();
        try {
            rs.insertRow();
            fail("FAIL - result set is not holdable and should be closed " +
                    "after commit");
        } catch (SQLException se) {
            assertSQLState("XCL16", se);
        }
        rs.close();
        
        String[][] expected =
        {{"1", "1"}, {"2", "2"}, {"3", "3"}, {"4", "4"}, {"8", "8"}};
        rs = stmt.executeQuery("SELECT * FROM t4");
        JDBC.assertFullResultSet(rs, expected, true);
        rs.close();
        
        stmt.executeUpdate("DROP TABLE t4");
        stmt.close();
        commit();
    }
    
    /**
     * Negative test - test insertRow on closed resultset
     */
    public void testInsertRowAfterClose() throws SQLException {
        createTableT4();
        Statement stmt = createStatement(
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
        ResultSet rs = stmt.executeQuery("SELECT * FROM t4");
        assertTrue("FAIL - row not found", rs.next());
        rs.moveToInsertRow();
        rs.updateInt(1, 9);
        rs.updateInt(2, 9);
        rs.close();
        try {
            rs.insertRow();
            fail("FAIL - insertRow can not be called on closed RS");
        } catch (SQLException se) {
            assertSQLState("XCL16", se);
        }
        
        try {
            rs.moveToCurrentRow();
            fail("FAIL - moveToCurrentRow can not be called on closed RS");
        } catch (SQLException se) {
            assertSQLState("XCL16", se);
        }
        
        try {
            rs.moveToInsertRow();
            fail("FAIL: moveToInsertRow can not be called on closed RS");
        } catch (SQLException se) {
            assertSQLState("XCL16", se);
        }
        
        String[][] expected = {{"1", "1"}, {"2", "2"}, {"3", "3"}, {"4", "4"}};
        rs = stmt.executeQuery("SELECT * FROM t4");
        JDBC.assertFullResultSet(rs, expected, true);
        rs.close();
        
        stmt.close();
    }
    
    /**
     * Positive test - try to insert without updating all columns. All
     * columns allow nulls or have a default value
     */
    public void testInsertRowWithDefaultValue() throws SQLException {
        createTableT5();
        Statement stmt = createStatement(
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
        ResultSet rs = stmt.executeQuery("SELECT * FROM t5");
        assertTrue("FAIL - row not found", rs.next());
        rs.moveToInsertRow();
        // Should insert a row with NULLS and DEFAULT VALUES
        rs.insertRow();
        rs.close();
        
        String[][] expected =
        {{"1", "1"}, {"2", "2"}, {"3", "3"}, {"4", "4"}, {"0", null}};
        rs = stmt.executeQuery("SELECT * FROM t5");
        JDBC.assertFullResultSet(rs, expected, true);
        rs.close();
        
        stmt.close();
    }
    
    /**
     * Positive test - Rollback with AutoCommit on
     */
    public void testRollbackWithAutoCommit() throws SQLException {
        createTableT4();
        getConnection().setAutoCommit(true);
        Statement stmt = createStatement(
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
        ResultSet rs = stmt.executeQuery("SELECT * FROM t4");
        assertTrue("FAIL - row not found", rs.next());
        rs.moveToInsertRow();
        rs.updateInt(1, 4000);
        rs.updateInt(2, 4000);
        rs.insertRow();
        rollback();
        rs.close();
        
        String[][] expected = {{"1", "1"}, {"2", "2"}, {"3", "3"}, {"4", "4"}};
        rs = stmt.executeQuery("SELECT * FROM t4");
        JDBC.assertFullResultSet(rs, expected, true);
        rs.close();
        
        stmt.executeUpdate("DROP TABLE t4");
        stmt.close();
        commit();
    }
    
    /**
     * Negative test - insertRow and read-only RS
     */
    
    public void testInsertRowReadOnlyRS() throws SQLException {
        createTableT4();
        Statement stmt = createStatement(
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        ResultSet rs = stmt.executeQuery("SELECT * FROM t4");
        
        // test moveToInsertRow on read-only result set
        try {
            rs.moveToInsertRow();
            fail("FAIL - moveToInsertRow can not be called on read-only RS");
        } catch (SQLException se) {
            assertSQLState("XJ083", se);
        }
        
        // test updateXXX on read-only result set
        try {
            rs.updateInt(1, 5000);
            fail("FAIL - updateXXX not allowed on read-only RS");
        } catch (SQLException se) {
            assertSQLState("XJ083", se);
        }
        
        // test insertRow on read-only result set
        try {
            rs.insertRow();
            fail("FAIL - insertRow not allowed on read-only RS");
        } catch (SQLException se) {
            assertSQLState("XJ083", se);
        }
        
        // test moveToCurrentRow on read-only result set
        try {
            rs.moveToCurrentRow();
            fail("FAIL - moveToCurrentRow can not be called on read-only RS");
        } catch (SQLException se) {
            assertSQLState("XJ083", se);
        }
        rs.close();
        stmt.close();
    }
    
    /**
     * Positive test - Test all updateXXX methods on all the supported sql
     * datatypes
     */
    public void testUpdateXXXAllDataTypesInsertRow() 
            throws SQLException, UnsupportedEncodingException 
    {
        createAllDatatypesTable();
        Statement stmt = createStatement();
        stmt.executeUpdate("DELETE FROM AllDataTypesForTestingTable");
        
        PreparedStatement pstmti = getConnection().prepareStatement(
                "SELECT * FROM AllDataTypesForTestingTable FOR UPDATE", 
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
        PreparedStatement pstmt1i = prepareStatement(
                "SELECT * FROM AllDataTypesNewValuesData");
        
        for (int sqlType = 1; sqlType <= allSQLTypes.length; sqlType++ ) {
            println("Next datatype to test is " + allSQLTypes[sqlType-1]);
            for (int updateXXXName = 1;  
                    updateXXXName <= allUpdateXXXNames.length; updateXXXName++) 
            {
                println("  Testing " + allUpdateXXXNames[updateXXXName-1] + 
                        " on SQL type " + allSQLTypes[sqlType-1]);
                runTestUpdateXXXAllDataTypesInsertRow(pstmti, pstmt1i, 
                        sqlType, updateXXXName);
            }
        }
    }
    
    private void runTestUpdateXXXAllDataTypesInsertRow(
            PreparedStatement pstmt,
            PreparedStatement pstmt1,
            int sqlType,
            int updateXXXName) throws SQLException, UnsupportedEncodingException 
    {
        ResultSet rs, rs1;
        int checkAgainstColumn = updateXXXName;
        for (int indexOrName = 1; indexOrName <= 2; indexOrName++) {
            if (indexOrName == 1) //test by passing column position
                println("Using column position as first parameter to " + 
                        allUpdateXXXNames[updateXXXName-1]);
            else
                println("Using column name as first parameter to " + 
                        allUpdateXXXNames[updateXXXName-1]);
            rs = pstmt.executeQuery();
            rs.moveToInsertRow();
            rs1 = pstmt1.executeQuery();
            rs1.next();
            try {
                if (updateXXXName == 1) {
                    //update column with updateShort methods
                    if (indexOrName == 1) //test by passing column position
                        rs.updateShort(sqlType, rs1.getShort(updateXXXName));
                    else //test by passing column name
                        rs.updateShort(ColumnNames[sqlType-1], 
                                rs1.getShort(updateXXXName));
                } else if (updateXXXName == 2) { 
                    //update column with updateInt methods
                    if (indexOrName == 1) //test by passing column position
                        rs.updateInt(sqlType, rs1.getInt(updateXXXName));
                    else //test by passing column name
                        rs.updateInt(ColumnNames[sqlType-1], 
                                rs1.getInt(updateXXXName));
                } else if (updateXXXName ==  3) { 
                    //update column with updateLong methods
                    if (indexOrName == 1) //test by passing column position
                        rs.updateLong(sqlType, rs1.getLong(updateXXXName));
                    else //test by passing column name
                        rs.updateLong(ColumnNames[sqlType-1], 
                                rs1.getLong(updateXXXName));
                } else if (updateXXXName == 4) { 
                    if (!JDBC.vmSupportsJSR169())
                    {
                        //update column with updateBigDecimal methods
                        if (indexOrName == 1) //test by passing column position
                            rs.updateBigDecimal(sqlType, 
                                rs1.getBigDecimal(updateXXXName));
                        else //test by passing column name
                            rs.updateBigDecimal(ColumnNames[sqlType-1], 
                                rs1.getBigDecimal(updateXXXName));
                    } else {
                        continue;
                    }
                } else if (updateXXXName == 5) { 
                    //update column with updateFloat methods
                    if (indexOrName == 1) //test by passing column position
                        rs.updateFloat(sqlType, rs1.getFloat(updateXXXName));
                    else //test by passing column name
                        rs.updateFloat(ColumnNames[sqlType-1], 
                                rs1.getFloat(updateXXXName));
                } else if (updateXXXName == 6) { 
                    //update column with updateDouble methods
                    if (indexOrName == 1) //test by passing column position
                        rs.updateDouble(sqlType, rs1.getDouble(updateXXXName));
                    else //test by passing column name
                        rs.updateDouble(ColumnNames[sqlType-1], 
                                rs1.getDouble(updateXXXName));
                } else if (updateXXXName == 7) { 
                    //update column with updateString methods
                    if (indexOrName == 1) //test by passing column position
                        rs.updateString(sqlType, rs1.getString(updateXXXName));
                    else //test by passing column name
                        rs.updateString(ColumnNames[sqlType-1], 
                                rs1.getString(updateXXXName));
                } else if (updateXXXName == 8) { 
                    //update column with updateAsciiStream methods
                    if (indexOrName == 1) //test by passing column position
                        rs.updateAsciiStream(sqlType,
                                rs1.getAsciiStream(updateXXXName), 4);
                    else //test by passing column name
                        rs.updateAsciiStream(ColumnNames[sqlType-1],
                                rs1.getAsciiStream(updateXXXName), 4);
                } else if (updateXXXName == 9) { 
                    //update column with updateCharacterStream methods
                    if (indexOrName == 1) //test by passing column position
                        rs.updateCharacterStream(sqlType,
                                rs1.getCharacterStream(updateXXXName), 4);
                    else //test by passing column name
                        rs.updateCharacterStream(ColumnNames[sqlType-1],
                                rs1.getCharacterStream(updateXXXName), 4);
                } else if (updateXXXName == 10) { 
                    //update column with updateByte methods
                    checkAgainstColumn = 1;
                    if (indexOrName == 1) //test by passing column position
                        rs.updateByte(sqlType,rs1.getByte(checkAgainstColumn));
                    else //test by passing column name
                        rs.updateByte(ColumnNames[sqlType-1],
                                rs1.getByte(checkAgainstColumn));
                } else if (updateXXXName == 11) { 
                    //update column with updateBytes methods
                    if (indexOrName == 1) //test by passing column position
                        rs.updateBytes(sqlType,rs1.getBytes(updateXXXName));
                    else //test by passing column name
                        rs.updateBytes(ColumnNames[sqlType-1],
                                rs1.getBytes(updateXXXName));
                } else if (updateXXXName == 12) { 
                    //update column with updateBinaryStream methods
                    if (indexOrName == 1) //test by passing column position
                        rs.updateBinaryStream(sqlType,
                                rs1.getBinaryStream(updateXXXName), 2);
                    else //test by passing column name
                        rs.updateBinaryStream(ColumnNames[sqlType-1],
                                rs1.getBinaryStream(updateXXXName), 2);
                } else if (updateXXXName == 13) { 
                    //update column with updateClob methods
                    //Don't test this method because running JDK1.3 and this jvm
                    //does not support the method
                    if (JDBC.vmSupportsJDBC3()) { 
                        if (indexOrName == 1) //test by passing column position
                            rs.updateClob(sqlType, rs1.getClob(updateXXXName));
                        else //test by passing column name
                            rs.updateClob(ColumnNames[sqlType-1],
                                    rs1.getClob(updateXXXName));
                    } else {
                        continue;
                    }
                } else if (updateXXXName == 14) { 
                    //update column with updateDate methods
                    if (indexOrName == 1) //test by passing column position
                        rs.updateDate(sqlType,rs1.getDate(updateXXXName));
                    else //test by passing column name
                        rs.updateDate(ColumnNames[sqlType-1],
                                rs1.getDate(updateXXXName));
                } else if (updateXXXName == 15) { 
                    //update column with updateTime methods
                    if (indexOrName == 1) //test by passing column position
                        rs.updateTime(sqlType, rs1.getTime(updateXXXName));
                    else //test by passing column name
                        rs.updateTime(ColumnNames[sqlType-1],
                                rs1.getTime(updateXXXName));
                } else if (updateXXXName == 16) { 
                    //update column with updateTimestamp methods
                    if (indexOrName == 1) //test by passing column position
                        rs.updateTimestamp(sqlType, 
                                rs1.getTimestamp(updateXXXName));
                    else //test by passing column name
                        rs.updateTimestamp(ColumnNames[sqlType-1],
                                rs1.getTimestamp(updateXXXName));
                } else if (updateXXXName == 17) { 
                    //update column with updateBlob methods
                    //Don't test this method because running JDK1.3 and this jvm
                    //does not support the method
                    if (JDBC.vmSupportsJDBC3()) { 
                        if (indexOrName == 1) //test by passing column position
                            rs.updateBlob(sqlType,rs1.getBlob(updateXXXName));
                        else //test by passing column name
                            rs.updateBlob(ColumnNames[sqlType-1],
                                    rs1.getBlob(updateXXXName));
                    } else {
                        continue;
                    }
                } else if (updateXXXName == 18) { 
                    //update column with getBoolean methods
                    //use SHORT sql type column's value for testing boolean 
                    //since Derby don't support boolean datatype
                    //Since Derby does not support Boolean datatype, this method
                    //is going to fail with the syntax error
                    if (indexOrName == 1) //test by passing column position
                        rs.updateBoolean(sqlType, rs1.getBoolean(1));
                    else //test by passing column name
                        rs.updateBoolean(ColumnNames[sqlType-1], 
                                rs1.getBoolean(1));
                } else if (updateXXXName == 19) { 
                    //update column with updateNull methods
                    if (indexOrName == 1) //test by passing column position
                        rs.updateNull(sqlType);
                    else //test by passing column name
                        rs.updateNull(ColumnNames[sqlType-1]);
                } else if (updateXXXName == 20) { 
                    //update column with updateArray methods - should get not 
                    //implemented exception
                    //Don't test this method because running JDK1.3 and this jvm
                    //does not support the method
                    if (JDBC.vmSupportsJDBC3()) { 
                        if (indexOrName == 1) //test by passing column position
                            rs.updateArray(sqlType, null);
                        else //test by passing column name
                            rs.updateArray(ColumnNames[sqlType-1], null);
                    } else {
                        continue;
                    }
                } else if (updateXXXName == 21) { 
                    //update column with updateRef methods - should get not 
                    //implemented exception
                    //Don't test this method because running JDK1.3 and this jvm
                    //does not support the method
                    if (JDBC.vmSupportsJDBC3()) { 
                        if (indexOrName == 1) //test by passing column position
                            rs.updateRef(sqlType, null);
                        else //test by passing column name
                            rs.updateRef(ColumnNames[sqlType-1], null);
                    } else {
                        continue;
                    }
                }
                
                rs.insertRow();
                
                if ((usingDerbyNetClient() && 
                        !updateXXXRulesTableForNetworkClient[sqlType-1][updateXXXName-1].equals("PASS")) ||
                    (usingEmbedded() && 
                        !updateXXXRulesTableForEmbedded[sqlType-1][updateXXXName-1].equals("PASS"))) 
                {
                    fail("FAIL - We shouldn't reach here. The test should " +
                            "have failed earlier on updateXXX or " +
                            "insertRow call");
                    return;
                }
                verifyData(sqlType, checkAgainstColumn);
                
                createStatement().executeUpdate(
                        "DELETE FROM AllDataTypesForTestingTable");
                
            } catch (SQLException se) {
                if (usingEmbedded()) {
                    assertSQLState(updateXXXRulesTableForEmbedded[sqlType-1][updateXXXName-1], se);
                } else {
                    assertSQLState(updateXXXRulesTableForNetworkClient[sqlType-1][updateXXXName-1], se);
                }
            } catch (java.lang.IllegalArgumentException iae) {
                //we are dealing with DATE/TIME/TIMESTAMP column types
                //we are dealing with updateString. The failure is because 
                //string does not represent a valid datetime value
                if (sqlType == 14 || sqlType == 15 || sqlType == 16)
                    assertEquals("FAIL - exception expected for updateString", 
                            7, checkAgainstColumn);
                else
                    throw iae;
            }
            rs.close();
            rs1.close();       
         }
    }
    
    private void createTableT1() throws SQLException {
        Statement stmt = createStatement();
        stmt.executeUpdate("create table t1 (c1 int, c2 char(20))");
        stmt.executeUpdate("insert into t1 " +
                "values (1,'aa'), (2,'bb'), (3,'cc')");
        stmt.close();
    }
    
    private void createTableT2() throws SQLException {
        Statement stmt = createStatement();
        stmt.executeUpdate("create table t2 (c21 int, c22 int)");
        stmt.executeUpdate("insert into t2 " +
                "values (1,1), (2, 2), (3, 3), (4, 4)");
        stmt.close();
    }
    
    private void createTableT3() throws SQLException {
        Statement stmt = createStatement();
        stmt.executeUpdate("create table t3 " +
                "(c31 int not null primary key, c32 smallint)");
        stmt.executeUpdate("insert into t3 " +
                "values (1,1), (2, 2), (3, 3), (4, 4)");
        stmt.close();
    }
    
    private void createTableT4() throws SQLException {
        Statement stmt = createStatement();
        stmt.executeUpdate("create table t4 " +
                "(c41 int not null primary key, c42 int)");
        stmt.executeUpdate("insert into t4 " +
                "values (1,1), (2,2), (3,3), (4, 4)");
        stmt.close();
    }
    
    private void createTableT5() throws SQLException {
        Statement stmt = createStatement();
        stmt.executeUpdate("create table t5 " +
                "(c51 int not null default 0, c52 int)");
        stmt.executeUpdate("insert into t5 " +
                "values (1,1), (2,2), (3,3), (4, 4)");
        stmt.close();
    }
    
    private void createTableWithPrimaryKey() throws SQLException {
        Statement stmt = createStatement();
        stmt.executeUpdate("create table tableWithPrimaryKey " +
                "(c1 int not null, c2 int not null, " +
                "constraint pk primary key(c1,c2))");
        stmt.executeUpdate("create table tableWithConstraint " +
                "(c1 int, c2 int, constraint fk foreign key(c1,c2) " +
                "references tableWithPrimaryKey)");
        stmt.executeUpdate("insert into tableWithPrimaryKey " +
                "values (1, 1), (2, 2), (3, 3), (4, 4)");
        stmt.executeUpdate("insert into tableWithConstraint " +
                "values (1, 1), (2, 2), (3, 3), (4, 4)");
        stmt.close();
    }
    
    private void createTable0WithTrigger() throws SQLException {
        Statement stmt = createStatement();
        stmt.executeUpdate("create table table0WithTriggers " +
                "(c1 int, c2 bigint)");
        stmt.executeUpdate("create table deleteTriggerInsertIntoThisTable " +
                "(c1 int)");
        stmt.executeUpdate("create table updateTriggerInsertIntoThisTable " +
                "(c1 int)");
        stmt.executeUpdate("create trigger tr1 " +
                "after delete on table0WithTriggers for each statement " +
                "insert into deleteTriggerInsertIntoThisTable values (1)");
        stmt.executeUpdate("create trigger tr2 " +
                "after update on table0WithTriggers for each statement " +
                "insert into updateTriggerInsertIntoThisTable values (1)");
        stmt.executeUpdate("insert into table0WithTriggers " +
                "values (1, 1), (2, 2), (3, 3), (4, 4)");
        stmt.close();
    }
    
    private void createTable1WithTrigger() throws SQLException {
        Statement stmt = createStatement();
        stmt.executeUpdate("create table table1WithTriggers " +
                "(c1 int, c2 bigint)");
        stmt.executeUpdate("create trigger tr3 " +
                "after delete on table1WithTriggers referencing old as old " +
                "for each row delete from table1WithTriggers " +
                "where c1=old.c1+1 or c1=old.c1-1");
        stmt.executeUpdate("create table table2WithTriggers " +
                "(c1 int, c2 bigint)");
        stmt.executeUpdate("create trigger tr4 after update of c2 " +
                "on table2WithTriggers for each statement " +
                "update table2WithTriggers set c1=1");
        stmt.executeUpdate("insert into table1WithTriggers values " +
                "(1, 1), (2, 2), (3, 3), (4, 4)");
        stmt.executeUpdate("insert into table2WithTriggers values " +
                "(1, 1), (2, 2), (3, 3), (4, 4)");
        stmt.close();
    }
    
    private void createSelfReferencingTable() throws SQLException {
        Statement stmt = createStatement();
        stmt.executeUpdate("create table selfReferencingT1 " +
                "(c1 char(2) not null, c2 char(2), " +
                "constraint selfReferencingT1 primary key(c1), " +
                "constraint manages1 foreign key(c2) " +
                "references selfReferencingT1(c1) on delete cascade)");
        stmt.executeUpdate("create table selfReferencingT2 " +
                "(c1 char(2) not null, c2 char(2), " +
                "constraint selfReferencingT2 primary key(c1), " +
                "constraint manages2 foreign key(c2) " +
                "references selfReferencingT2(c1) on update restrict)");
        stmt.executeUpdate("insert into selfReferencingT1 values " +
                "('e1', null), ('e2', 'e1'), ('e3', 'e2'), ('e4', 'e3')");
        stmt.executeUpdate("insert into selfReferencingT2 values " +
                "('e1', null), ('e2', 'e1'), ('e3', 'e2'), ('e4', 'e3')");
        stmt.close();
    }
    
    private void createAllDatatypesTable() throws SQLException {
        Statement stmt = createStatement();
        StringBuffer createSQL =
                new StringBuffer("create table AllDataTypesForTestingTable (");
        StringBuffer createTestDataSQL =
                new StringBuffer("create table AllDataTypesNewValuesData (");
        for (int type = 0; type < allSQLTypes.length - 1; type++) {
            createSQL.append(ColumnNames[type] + " " + allSQLTypes[type] + ",");
            createTestDataSQL.
                    append(ColumnNames[type] + " " + allSQLTypes[type] + ",");
        }
        createSQL.append(ColumnNames[allSQLTypes.length - 1] + " " +
                allSQLTypes[allSQLTypes.length - 1] + ")");
        createTestDataSQL.append(ColumnNames[allSQLTypes.length - 1] + " " +
                allSQLTypes[allSQLTypes.length - 1] + ")");
        stmt.executeUpdate(createSQL.toString());
        stmt.executeUpdate(createTestDataSQL.toString());
        
        createSQL = new StringBuffer(
                "insert into AllDataTypesForTestingTable values(");
        createTestDataSQL = new StringBuffer(
                "insert into AllDataTypesNewValuesData values(");
        for (int type = 0; type < allSQLTypes.length - 1; type++) {
            createSQL.append(SQLData[type][0] + ",");
            createTestDataSQL.append(SQLData[type][1] + ",");
        }
        createSQL.append("cast("+SQLData[allSQLTypes.length - 1][0]
                + " as BLOB(1K)))");
        createTestDataSQL.append("cast("+SQLData[allSQLTypes.length - 1][1]
                + " as BLOB(1K)))");
        stmt.executeUpdate(createSQL.toString());
        stmt.executeUpdate(createTestDataSQL.toString());
        stmt.close();
    }
  
    private void verifyData(int sqlType, int updateXXXName) 
            throws SQLException, UnsupportedEncodingException
    {
        PreparedStatement pstmt1 = prepareStatement(
                "select * from AllDataTypesNewValuesData");
        ResultSet rs1 = pstmt1.executeQuery();
        rs1.next();
        PreparedStatement pstmt = prepareStatement(
                "select * from AllDataTypesForTestingTable");
        ResultSet rs = pstmt.executeQuery();
        rs.next();
        
        if (updateXXXName == 18) { //verifying updateBoolean
            assertEquals("FAIL - wrong value on " + allSQLTypes[sqlType - 1] + 
                    " using " + allUpdateXXXNames[updateXXXName - 1], 
                    rs.getBoolean(sqlType), rs1.getBoolean(1));
            return;
        }
        if (updateXXXName == 19) { //verifying updateNull
            assertNull("FAIL - wrong value on " + allSQLTypes[sqlType - 1] + 
                    " using " + allUpdateXXXNames[updateXXXName - 1], 
                    rs.getObject(sqlType));
            assertTrue("FAIL - wrong value on " + allSQLTypes[sqlType - 1] + 
                    " using " + allUpdateXXXNames[updateXXXName - 1], 
                    rs.wasNull());
            return;
        }
        
        if (sqlType == 1) {
            // verify update made to SMALLINT column with updateXXX methods
            assertEquals("FAIL - wrong value on " + allSQLTypes[sqlType - 1] + 
                    " using " + allUpdateXXXNames[updateXXXName - 1], 
                    rs1.getShort(updateXXXName), rs.getShort(sqlType));
        } else if (sqlType == 2) {
            // verify update made to INTEGER column with updateXXX methods
            assertEquals("FAIL - wrong value on " + allSQLTypes[sqlType - 1] + 
                    " using " + allUpdateXXXNames[updateXXXName - 1], 
                    rs1.getInt(updateXXXName), rs.getInt(sqlType));
        } else if (sqlType ==  3) {
            // verify update made to BIGINT column with updateXXX methods
            assertEquals("FAIL - wrong value on " + allSQLTypes[sqlType - 1] + 
                    " using " + allUpdateXXXNames[updateXXXName - 1],
                    rs1.getLong(updateXXXName), rs.getLong(sqlType));
        } else if (sqlType == 4) {
            if (!JDBC.vmSupportsJSR169()) {
                // verify update made to DECIMAL column with updateXXX methods
                assertTrue("FAIL - wrong value on " + allSQLTypes[sqlType - 1] + 
                    rs.getBigDecimal(sqlType),
                    rs.getBigDecimal(sqlType).doubleValue() == 
                            rs1.getBigDecimal(updateXXXName).doubleValue());
            }
        } else if (sqlType == 5) {
            // verify update made to REAL column with updateXXX methods
            assertTrue("FAIL - wrong value on " + allSQLTypes[sqlType - 1] + 
                    " using " + allUpdateXXXNames[updateXXXName - 1], 
                    rs.getFloat(sqlType) == rs1.getFloat(updateXXXName));
        } else if (sqlType == 6) {
            // verify update made to DOUBLE column with updateXXX methods
            Double d1, d2;
            d1 = new Double(rs.getDouble(sqlType));
            d2 = new Double(rs1.getDouble(updateXXXName));
            // can have precision problems with updateFloat
            if (updateXXXName == 5) {
                assertTrue("FAIL - wrong value on " + allSQLTypes[sqlType - 1] + 
                        " using " + allUpdateXXXNames[updateXXXName - 1], 
                        d1.floatValue() == d2.floatValue());
            } else {
                assertTrue("FAIL - wrong value on " + allSQLTypes[sqlType - 1] + 
                       " using " + allUpdateXXXNames[updateXXXName - 1],
                       d1.doubleValue() == d2.doubleValue());
            }
        } else if (sqlType == 7 || sqlType == 8 || sqlType == 9) {
            if (updateXXXName == 11) {
                // verify update made to CHAR column with updateBytes methods
                String expected = new String(
                        rs1.getBytes(updateXXXName), "UTF-16BE").trim();
                assertEquals("FAIL - wrong value on " + allSQLTypes[sqlType - 1]
                        + " using " + allUpdateXXXNames[updateXXXName - 1],
                        expected, rs.getString(sqlType).trim());
            } else {
                // verify update made to CHAR column with updateXXX methods
                assertEquals("FAIL - wrong value on " + allSQLTypes[sqlType - 1]
                        + " using " + allUpdateXXXNames[updateXXXName - 1],
                        rs1.getString(updateXXXName).trim(),
                        rs.getString(sqlType).trim());
            }
        } else if (sqlType == 10 || sqlType == 11 || sqlType == 12) {
            // verify update made to CHAR/VARCHAR/LONG VARCHAR FOR BIT DATA
            // column with updateXXX methods
            assertTrue("FAIL - wrong value on " + allSQLTypes[sqlType - 1]
                    + " using " + allUpdateXXXNames[updateXXXName - 1],
                    Arrays.equals(rs.getBytes(sqlType), 
                            rs1.getBytes(updateXXXName)));
        } else if (sqlType == 13 && JDBC.vmSupportsJDBC3()) {
            // verify update made to CLOB column with updateXXX methods
            int len = (int)rs.getClob(sqlType).length();
            assertEquals("FAIL - wrong value on " + allSQLTypes[sqlType - 1] + 
                    " using " + allUpdateXXXNames[updateXXXName - 1],
                    rs1.getString(updateXXXName).trim(), 
                    rs.getClob(sqlType).getSubString(1, len).trim());
        } else if (sqlType == 14) {
            // verify update made to DATE column with updateXXX methods
            assertEquals("FAIL - wrong value on " + allSQLTypes[sqlType - 1] + 
                    " using " + allUpdateXXXNames[updateXXXName - 1],
                    rs1.getDate(updateXXXName), rs.getDate(sqlType));
        } else if (sqlType == 15) {
            // verify update made to TIME column with updateXXX methods
            assertEquals("FAIL - wrong value on " + allSQLTypes[sqlType - 1] + 
                    " using " + allUpdateXXXNames[updateXXXName - 1],
                    rs1.getTime(updateXXXName), rs.getTime(sqlType));
        } else if (sqlType == 16) {
            // verify update made to TIMESTAMP column with updateXXX methods
            assertEquals("FAIL - wrong value on " + allSQLTypes[sqlType - 1] + 
                    " using " + allUpdateXXXNames[updateXXXName - 1],
                    rs1.getTimestamp(updateXXXName), rs.getTimestamp(sqlType));
        } else if (sqlType == 17 && JDBC.vmSupportsJDBC3()) {
            // verify update made to BLOB column with updateXXX methods
            long len = rs.getBlob(sqlType).length();
            assertTrue("FAIL - wrong value on " + allSQLTypes[sqlType - 1] + 
                    " using " + allUpdateXXXNames[updateXXXName - 1],
                    Arrays.equals(rs.getBlob(sqlType).getBytes(1, (int)len), 
                    rs1.getBytes(updateXXXName)));
        }
        
        rs.close();
        rs1.close();
        pstmt.close();
        pstmt1.close();
    }
    
    private void resetData() throws SQLException {
        Statement stmt = createStatement();
        stmt.executeUpdate("delete from AllDataTypesForTestingTable");
        StringBuffer insertSQL = new StringBuffer(
                "insert into AllDataTypesForTestingTable values(");
        for (int type = 0; type < allSQLTypes.length - 1; type++) {
            insertSQL.append(SQLData[type][0] + ",");
        }
        insertSQL.append("cast("+SQLData[allSQLTypes.length - 1][0]
                + " as BLOB(1K)))");
        stmt.executeUpdate(insertSQL.toString());
    }
}

