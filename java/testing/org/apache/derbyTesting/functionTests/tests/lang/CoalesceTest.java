/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.lang.CoalesceTest

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

import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Types;
import java.sql.SQLException;
import java.io.UnsupportedEncodingException;

import junit.framework.Test;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.TestConfiguration;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.JDBC;


/**
 * Coalesce/Value tests for various datatypes
 * coalesce/value function takes arguments and returns the first argument that is not null.
 * The arguments are evaluated in the order in which they are specified, and the result of the
 * function is the first argument that is not null. The result can be null only if all the arguments
 * can be null. The selected argument is converted, if necessary, to the attributes of the result.
 */
public class CoalesceTest extends BaseJDBCTestCase
{
    public Statement s = null;
    public PreparedStatement ps = null;

    private static String VALID_DATE_STRING = "'2000-01-01'";
    private static String VALID_TIME_STRING = "'15:30:20'";
    private static String VALID_TIMESTAMP_STRING = "'2000-01-01 15:30:20'";
    private static String NULL_VALUE="NULL";

    private static String[] SQLTypes =
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
        "CHAR(60) FOR BIT DATA",
        "VARCHAR(60) FOR BIT DATA",
        "LONG VARCHAR FOR BIT DATA",
        "CLOB(1k)",
        "DATE",
        "TIME",
        "TIMESTAMP",
        "BLOB(1k)",

    };

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

    private static String[][]SQLData =
    {
        {NULL_VALUE, "0","1","2"},       // SMALLINT
        {NULL_VALUE,"11","111",NULL_VALUE},       // INTEGER
        {NULL_VALUE,"22","222","3333"},       // BIGINT
        {NULL_VALUE,"3.3","3.33",NULL_VALUE},      // DECIMAL(10,5)
        {NULL_VALUE,"4.4","4.44","4.444"},      // REAL,
        {NULL_VALUE,"5.5","5.55",NULL_VALUE},      // DOUBLE
        {NULL_VALUE,"'1992-01-06'","'1992-01-16'",NULL_VALUE},      // CHAR(60)
        {NULL_VALUE,"'1992-01-07'","'1992-01-17'",VALID_TIME_STRING},      //VARCHAR(60)",
        {NULL_VALUE,"'1992-01-08'","'1992-01-18'",VALID_TIMESTAMP_STRING},      // LONG VARCHAR
        {NULL_VALUE,"X'10aa'",NULL_VALUE,"X'10aaaa'"},  // CHAR(60)  FOR BIT DATA
        {NULL_VALUE,"X'10bb'",NULL_VALUE,"X'10bbbb'"},  // VARCHAR(60) FOR BIT DATA
        {NULL_VALUE,"X'10cc'",NULL_VALUE,"X'10cccc'"},  //LONG VARCHAR FOR BIT DATA
        {NULL_VALUE,"'13'","'14'",NULL_VALUE},     //CLOB(1k)
        {NULL_VALUE,VALID_DATE_STRING,VALID_DATE_STRING,NULL_VALUE},        // DATE
        {NULL_VALUE,VALID_TIME_STRING,VALID_TIME_STRING,NULL_VALUE},        // TIME
        {NULL_VALUE,VALID_TIMESTAMP_STRING,VALID_TIMESTAMP_STRING,NULL_VALUE},   // TIMESTAMP
        {NULL_VALUE,NULL_VALUE,NULL_VALUE,NULL_VALUE}                 // BLOB
    };

    /**
	   SQL Reference Guide for DB2 has section titled "Rules for result data types" at the following url
	   http://publib.boulder.ibm.com/infocenter/db2help/index.jsp?topic=/com.ibm.db2.udb.doc/admin/r0008480.htm

	   I have constructed following table based on various tables and information under "Rules for result data types"
	   This table has FOR BIT DATA TYPES broken out into separate columns for clarity and testing
     **/
    public static final String[][]  resultDataTypeRulesTable = {

        // Types.             S  I  B  D  R  D  C  V  L  C  V  L  C  D  T  T  B
        //                    M  N  I  E  E  O  H  A  O  H  A  O  L  A  I  I  L
        //                    A  T  G  C  A  U  A  R  N  A  R  N  O  T  M  M  O
        //                    L  E  I  I  L  B  R  C  G  R  C  G  B  E  E  E  B
        //                    L  G  N  M     L     H  V  .  H  V           S
        //                    I  E  T  A     E     A  A  B  A  A           T
        //                    N  R     L           R  R  I  R  R           A
        //                    T                       C  T  .  .           M
        //                                            H     B  B           P
        //                                            A     I  I
        //                                            R     T   T
        /* 0 SMALLINT */        { "SMALLINT", "INTEGER", "BIGINT", "DECIMAL", "DOUBLE", "DOUBLE", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR" },
        /* 1 INTEGER  */        { "INTEGER", "INTEGER", "BIGINT", "DECIMAL", "DOUBLE", "DOUBLE", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR" },
        /* 2 BIGINT   */        { "BIGINT", "BIGINT", "BIGINT", "DECIMAL", "DOUBLE", "DOUBLE", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR" },
        /* 3 DECIMAL  */        { "DECIMAL", "DECIMAL", "DECIMAL", "DECIMAL", "DOUBLE", "DOUBLE", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR" },
        /* 4 REAL     */        { "DOUBLE", "DOUBLE", "DOUBLE", "DOUBLE", "REAL", "DOUBLE", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR" },
        /* 5 DOUBLE   */        { "DOUBLE", "DOUBLE", "DOUBLE", "DOUBLE", "DOUBLE", "DOUBLE", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR" },
        /* 6 CHAR     */        { "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "CHAR", "VARCHAR", "LONG VARCHAR", "ERROR", "ERROR", "ERROR", "CLOB", "DATE", "TIME", "TIMESTAMP", "ERROR" },
        /* 7 VARCHAR  */        { "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "VARCHAR", "VARCHAR","LONG VARCHAR", "ERROR", "ERROR", "ERROR", "CLOB", "DATE", "TIME", "TIMESTAMP", "ERROR" },
        /* 8 LONGVARCHAR */     { "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "LONG VARCHAR", "LONG VARCHAR", "LONG VARCHAR", "ERROR", "ERROR", "ERROR", "CLOB", "ERROR", "ERROR", "ERROR", "ERROR" },
        /* 9 CHAR FOR BIT */    { "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "CHAR () FOR BIT DATA", "VARCHAR () FOR BIT DATA", "LONG VARCHAR FOR BIT DATA", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR" },
        /* 10 VARCH. BIT   */   { "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "VARCHAR () FOR BIT DATA", "VARCHAR () FOR BIT DATA", "LONG VARCHAR FOR BIT DATA", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR" },
        /* 11 LONGVAR. BIT */   { "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "LONG VARCHAR FOR BIT DATA", "LONG VARCHAR FOR BIT DATA", "LONG VARCHAR FOR BIT DATA", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR" },
        /* 12 CLOB         */   { "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "CLOB", "CLOB", "CLOB", "ERROR", "ERROR", "ERROR", "CLOB", "ERROR", "ERROR", "ERROR", "ERROR" },
        /* 13 DATE         */   { "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "DATE", "DATE", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "DATE", "ERROR", "ERROR", "ERROR" },
        /* 14 TIME         */   { "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "TIME", "TIME", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "TIME", "ERROR", "ERROR" },
        /* 15 TIMESTAMP    */   { "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "TIMESTAMP", "TIMESTAMP", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "TIMESTAMP", "ERROR" },
        /* 16 BLOB         */   { "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "BLOB" },

    }; 

    /* list of all the tables used in this test */
    private static String[] TABLES = { 
        "create table tA (c1 int, c2 char(254))",
        "create table coalesce (coalesce int, c12 int)",
        "create table value (value int, c12 int)",
        "create table tF (dateCol date, charCol char(10), varcharCol varchar(50))",
        "create table tG (timeCol time, charCol char(10), varcharCol varchar(50))",
        "create table tH (timestampCol timestamp, charCol char(19), varcharCol varchar(50))",
        "create table tE (smallintCol smallint, intCol integer, bigintCol bigint, decimalCol1 decimal(22,2), decimalCol2 decimal(8,6), decimalCol3 decimal(31,28), realCol real, doubleCol double)",
        "create table tD (c1 int, c2 char(254))",
        "create table tB (c1 char(254), c2 char(40), vc1 varchar(253), vc2 varchar(2000), lvc1 long varchar, lvc2 long varchar, clob1 CLOB(200), clob2 CLOB(33K))",
        "create table tC (cbd1 char(254) for bit data, cbd2 char(40) for bit data, vcbd1 varchar(253) for bit data, vcbd2 varchar(2000) for bit data, lvcbd1 long varchar for bit data, lvcbd2 long varchar for bit data, blob1 BLOB(200), blob2 BLOB(33K))",	
        "create table tAggr (i int)"
    };

    /* Public constructor required for running test as standalone JUnit. */    
    public CoalesceTest(String name) {
        super(name);
    }

    /* Set up fixture */ 
    protected void setUp() throws SQLException {
        s = createStatement();
        ps = null;
    }

    /* Tear down the fixture */
    protected void tearDown() throws Exception {
        s.close();
        if ( ps != null ) ps.close();
        super.tearDown();
    }

    /**
     * Create a suite of tests, run only in embedded since
     * this is testing server-side behaviour.
     **/
    public static Test suite() {
        Test suite = TestConfiguration.embeddedSuite(CoalesceTest.class);
        
        return new CleanDatabaseTestSetup(suite) 
        {
            protected void decorateSQL(Statement stmt) throws SQLException
            {
                for (int i = 0; i < TABLES.length; i++) {
                    stmt.execute(TABLES[i]);
                }

                StringBuffer createSQL = new StringBuffer("create table AllDataTypesTable (");
                for (int type = 0; type < SQLTypes.length - 1; type++)
                {
                    createSQL.append(ColumnNames[type] + " " + SQLTypes[type] + ",");
                }
                createSQL.append(ColumnNames[SQLTypes.length - 1] + " " + SQLTypes[SQLTypes.length - 1] + ")");
                stmt.executeUpdate(createSQL.toString());

                for (int row = 0; row < SQLData[0].length; row++)
                {
                    createSQL = new StringBuffer("insert into AllDataTypesTable values(");
                    for (int type = 0; type < SQLTypes.length - 1; type++)
                    {
                        createSQL.append(SQLData[type][row] + ",");
                    }
                    createSQL.append(SQLData[SQLTypes.length - 1][row]+")");
                    stmt.executeUpdate(createSQL.toString());
                }
            }
        };

    } 

    public void testCoalesceSyntax() throws Throwable
    {
        s.executeUpdate("insert into tA (c1) values(1)");

        assertStatementError("42X04", s, "select coalesce from tA");
        assertStatementError("42X04", s, "select value from tA");
        assertStatementError("42X01", s, "select coalesce() from tA");
        assertStatementError("42X01", s, "select value() from tA");
        assertStatementError("42605", s, "select coalesce(c1) from tA");
        assertStatementError("42605", s, "select value(c1) from tA");
        assertStatementError("42X04", s, "select coalesce(c111) from tA");
        assertStatementError("42X04", s, "select value(c111) from tA");

        s.executeUpdate("insert into coalesce(coalesce) values(null)");
        s.executeUpdate("insert into coalesce values(null,1)");                
        dumpRS(s.executeQuery("select coalesce(coalesce,c12) from coalesce"), "COL1(datatype : INTEGER, precision : 10, scale : 0) null 1 " );

        s.executeUpdate("insert into value(value) values(null)");
        s.executeUpdate("insert into value values(null,1)");
        dumpRS(s.executeQuery("select coalesce(value,c12) from value"), "COL1(datatype : INTEGER, precision : 10, scale : 0) null 1 ");  


        try {
            ps = prepareStatement("select coalesce(?,?) from tA");
        }
        catch (SQLException sqle) {
            assertSQLState("42610", sqle);				
        }


        try {
            ps = prepareStatement("select value(?,?) from tA");	
        }
        catch (SQLException sqle) {
            assertSQLState("42610", sqle);	

        }                   		
    }

    public void testCompatibleDatatypesCombinations() throws Throwable
    {
        String[] expectedValues = {
                "COL1(datatype : SMALLINT, precision : 5, scale : 0) null 0 1 2 ",
                "COL1(datatype : INTEGER, precision : 10, scale : 0) null 0 1 2 ",
                "COL1(datatype : BIGINT, precision : 19, scale : 0) null 0 1 2 ",
                "COL1(datatype : DECIMAL, precision : 24, scale : 5) null 0.00000 1.00000 2.00000 ",
                "COL1(datatype : DOUBLE, precision : 15, scale : 0) null 0.0 1.0 2.0 ", 
                "COL1(datatype : DOUBLE, precision : 15, scale : 0) null 0.0 1.0 2.0 ",
                "COL1(datatype : INTEGER, precision : 10, scale : 0) null 11 111 2 ", 
                "COL1(datatype : INTEGER, precision : 10, scale : 0) null 11 111 2 ",
                "COL1(datatype : BIGINT, precision : 19, scale : 0) null 11 111 2 ",
                "COL1(datatype : DECIMAL, precision : 24, scale : 5) null 11.00000 111.00000 2.00000 ",
                "COL1(datatype : DOUBLE, precision : 15, scale : 0) null 11.0 111.0 2.0 ",
                "COL1(datatype : DOUBLE, precision : 15, scale : 0) null 11.0 111.0 2.0 ",
                "COL1(datatype : BIGINT, precision : 19, scale : 0) null 22 222 3333 ",
                "COL1(datatype : BIGINT, precision : 19, scale : 0) null 22 222 3333 ",
                "COL1(datatype : BIGINT, precision : 19, scale : 0) null 22 222 3333 ",
                "COL1(datatype : DECIMAL, precision : 24, scale : 5) null 22.00000 222.00000 3333.00000 ",
                "COL1(datatype : DOUBLE, precision : 15, scale : 0) null 22.0 222.0 3333.0 ",
                "COL1(datatype : DOUBLE, precision : 15, scale : 0) null 22.0 222.0 3333.0 ",
                "COL1(datatype : DECIMAL, precision : 10, scale : 5) null 3.30000 3.33000 2.00000 ",
                "COL1(datatype : DECIMAL, precision : 15, scale : 5) null 3.30000 3.33000 2.00000 ",
                "COL1(datatype : DECIMAL, precision : 24, scale : 5) null 3.30000 3.33000 2.00000 ",
                "COL1(datatype : DECIMAL, precision : 24, scale : 5) null 3.30000 3.33000 2.00000 ",
                "COL1(datatype : DOUBLE, precision : 15, scale : 0) null 3.3 3.33 2.0 ",
                "COL1(datatype : DOUBLE, precision : 15, scale : 0) null 3.3 3.33 2.0 ",
                "COL1(datatype : DOUBLE, precision : 15, scale : 0) null 4.400000095367432 4.440000057220459 4.443999767303467 ",
                "COL1(datatype : DOUBLE, precision : 15, scale : 0) null 4.400000095367432 4.440000057220459 4.443999767303467 ",
                "COL1(datatype : DOUBLE, precision : 15, scale : 0) null 4.400000095367432 4.440000057220459 4.443999767303467 ",
                "COL1(datatype : DOUBLE, precision : 15, scale : 0) null 4.400000095367432 4.440000057220459 4.443999767303467 ",
                "COL1(datatype : DOUBLE, precision : 15, scale : 0) null 4.400000095367432 4.440000057220459 4.443999767303467 ",
                "COL1(datatype : DOUBLE, precision : 15, scale : 0) null 4.400000095367432 4.440000057220459 4.443999767303467 ",
                "COL1(datatype : DOUBLE, precision : 15, scale : 0) null 5.5 5.55 2.0 ",
                "COL1(datatype : DOUBLE, precision : 15, scale : 0) null 5.5 5.55 2.0 ",
                "COL1(datatype : DOUBLE, precision : 15, scale : 0) null 5.5 5.55 2.0 ",
                "COL1(datatype : DOUBLE, precision : 15, scale : 0) null 5.5 5.55 2.0 ",
                "COL1(datatype : DOUBLE, precision : 15, scale : 0) null 5.5 5.55 2.0 ",
                "COL1(datatype : DOUBLE, precision : 15, scale : 0) null 5.5 5.55 2.0 ",
                "COL1(datatype : CHAR, precision : 60, scale : 0) null 1992-01-06                                                   1992-01-16                                                   null ",
                "COL1(datatype : VARCHAR, precision : 60, scale : 0) null 1992-01-06                                                   1992-01-16                                                   15:30:20 ",
                "COL1(datatype : LONG VARCHAR, precision : 32700, scale : 0) null 1992-01-06                                                   1992-01-16                                                   15:30:20 ",
                "COL1(datatype : CLOB, precision : 32700, scale : 0) null 1992-01-06                                                   1992-01-16                                                   15:30:20 ",
                "", "", "",
                "COL1(datatype : VARCHAR, precision : 60, scale : 0) null 1992-01-07 1992-01-17 15:30:20 ",
                "COL1(datatype : VARCHAR, precision : 60, scale : 0) null 1992-01-07 1992-01-17 15:30:20 ",
                "COL1(datatype : LONG VARCHAR, precision : 32700, scale : 0) null 1992-01-07 1992-01-17 15:30:20 ",
                "COL1(datatype : CLOB, precision : 32700, scale : 0) null 1992-01-07 1992-01-17 15:30:20 ",
                "", "", "",
                "COL1(datatype : LONG VARCHAR, precision : 32700, scale : 0) null 1992-01-08 1992-01-18 2000-01-01 15:30:20 ",
                "COL1(datatype : LONG VARCHAR, precision : 32700, scale : 0) null 1992-01-08 1992-01-18 2000-01-01 15:30:20 ",
                "COL1(datatype : LONG VARCHAR, precision : 32700, scale : 0) null 1992-01-08 1992-01-18 2000-01-01 15:30:20 ",
                "COL1(datatype : CLOB, precision : 32700, scale : 0) null 1992-01-08 1992-01-18 2000-01-01 15:30:20 ",
                "COL1(datatype : CHAR () FOR BIT DATA, precision : 60, scale : 0) null 10aa20202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020 null 10aaaa202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020 ",
                "COL1(datatype : VARCHAR () FOR BIT DATA, precision : 60, scale : 0) null 10aa20202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020 null 10aaaa202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020 ",
                "COL1(datatype : LONG VARCHAR FOR BIT DATA, precision : 32700, scale : 0) null 10aa20202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020 null 10aaaa202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020 ",	
                "COL1(datatype : VARCHAR () FOR BIT DATA, precision : 60, scale : 0) null 10bb null 10bbbb ",
                "COL1(datatype : VARCHAR () FOR BIT DATA, precision : 60, scale : 0) null 10bb null 10bbbb ",
                "COL1(datatype : LONG VARCHAR FOR BIT DATA, precision : 32700, scale : 0) null 10bb null 10bbbb ",
                "COL1(datatype : LONG VARCHAR FOR BIT DATA, precision : 32700, scale : 0) null 10cc null 10cccc ",
                "COL1(datatype : LONG VARCHAR FOR BIT DATA, precision : 32700, scale : 0) null 10cc null 10cccc ",
                "COL1(datatype : LONG VARCHAR FOR BIT DATA, precision : 32700, scale : 0) null 10cc null 10cccc ",
                "COL1(datatype : CLOB, precision : 1024, scale : 0) null 13 14 null ",
                "COL1(datatype : CLOB, precision : 1024, scale : 0) null 13 14 15:30:20 ",
                "COL1(datatype : CLOB, precision : 32700, scale : 0) null 13 14 15:30:20 ",
                "COL1(datatype : CLOB, precision : 32700, scale : 0) null 13 14 15:30:20 ",
                "COL1(datatype : DATE, precision : 10, scale : 0) null 2000-01-01 2000-01-01 null ",
                "", "",
                "COL1(datatype : TIME, precision : 8, scale : 0) null 15:30:20 15:30:20 null ",
                "COL1(datatype : TIME, precision : 8, scale : 0) null 15:30:20 15:30:20 15:30:20 ",
                "COL1(datatype : TIME, precision : 8, scale : 0) null 15:30:20 15:30:20 15:30:20 ",
                "COL1(datatype : TIMESTAMP, precision : 26, scale : 6) null 2000-01-01 15:30:20.0 2000-01-01 15:30:20.0 null ",
                "", "",
                "COL1(datatype : BLOB, precision : 1024, scale : 0) null null null null "
        };

        int index = 0;
        for (int firstColumnType = 0; firstColumnType < SQLTypes.length; firstColumnType++) {
            StringBuffer coalesceString = new StringBuffer("SELECT COALESCE(" + ColumnNames[firstColumnType]);
            for (int secondColumnType = 0; secondColumnType < SQLTypes.length; secondColumnType++) {
                try {
                    if (resultDataTypeRulesTable[firstColumnType][secondColumnType].equals("ERROR"))
                        continue; //the datatypes are incompatible, don't try them in COALESCE/VALUE
                    coalesceString.append("," + ColumnNames[secondColumnType]);
                    dumpRS(s.executeQuery(coalesceString + ") from AllDataTypesTable"),expectedValues[index]);
                } catch (SQLException sqle) {
                    if (sqle.getSQLState().equals("22007"))
                        assertSQLState("22007", sqle);
                    else if (isClobWithCharAndDateTypeArguments(coalesceString.toString())  && sqle.getSQLState().equals("42815"))
                        assertSQLState("42815", sqle);
                    else if (!isSupportedCoalesce(firstColumnType,secondColumnType)  && sqle.getSQLState().equals("42815"))
                        assertSQLState("42815", sqle);
                    else
                        fail("CoalesceTest: expected SQLException was not thrown.");				
                }
                index++;
            }
        }
    }

    public void testAllDatatypesCombinations() throws  Throwable
    {
        String[] expectedValues = {
                "COL1(datatype : SMALLINT, precision : 5, scale : 0) null 0 1 2 ",
                "COL1(datatype : SMALLINT, precision : 5, scale : 0) null 0 1 2 ",
                "COL1(datatype : INTEGER, precision : 10, scale : 0) null 0 1 2 ",
                "COL1(datatype : INTEGER, precision : 10, scale : 0) null 0 1 2 ",
                "COL1(datatype : BIGINT, precision : 19, scale : 0) null 0 1 2 ",
                "COL1(datatype : BIGINT, precision : 19, scale : 0) null 0 1 2 ",
                "COL1(datatype : DECIMAL, precision : 10, scale : 5) null 0.00000 1.00000 2.00000 ",
                "COL1(datatype : DECIMAL, precision : 10, scale : 5) null 0.00000 1.00000 2.00000 ",
                "COL1(datatype : DOUBLE, precision : 15, scale : 0) null 0.0 1.0 2.0 ",
                "COL1(datatype : DOUBLE, precision : 15, scale : 0) null 0.0 1.0 2.0 ",
                "COL1(datatype : DOUBLE, precision : 15, scale : 0) null 0.0 1.0 2.0 ",
                "COL1(datatype : DOUBLE, precision : 15, scale : 0) null 0.0 1.0 2.0 ",
                "","","","","","","","","","","","","","","","","","","","","","",
                "COL1(datatype : INTEGER, precision : 10, scale : 0) null 11 111 2 ",
                "COL1(datatype : INTEGER, precision : 10, scale : 0) null 11 111 2 ",
                "COL1(datatype : INTEGER, precision : 10, scale : 0) null 11 111 null ",
                "COL1(datatype : INTEGER, precision : 10, scale : 0) null 11 111 null ",
                "COL1(datatype : BIGINT, precision : 19, scale : 0) null 11 111 3333 ",
                "COL1(datatype : BIGINT, precision : 19, scale : 0) null 11 111 3333 ",
                "COL1(datatype : DECIMAL, precision : 15, scale : 5) null 11.00000 111.00000 null ",
                "COL1(datatype : DECIMAL, precision : 15, scale : 5) null 11.00000 111.00000 null ",
                "COL1(datatype : DOUBLE, precision : 15, scale : 0) null 11.0 111.0 4.443999767303467 ",
                "COL1(datatype : DOUBLE, precision : 15, scale : 0) null 11.0 111.0 4.443999767303467 ",
                "COL1(datatype : DOUBLE, precision : 15, scale : 0) null 11.0 111.0 null ",
                "COL1(datatype : DOUBLE, precision : 15, scale : 0) null 11.0 111.0 null ",
                "","","","","","","","","","","","","","","","","","","","","","",
                "COL1(datatype : BIGINT, precision : 19, scale : 0) null 22 222 3333 ",
                "COL1(datatype : BIGINT, precision : 19, scale : 0) null 22 222 3333 ",
                "COL1(datatype : BIGINT, precision : 19, scale : 0) null 22 222 3333 ",
                "COL1(datatype : BIGINT, precision : 19, scale : 0) null 22 222 3333 ",
                "COL1(datatype : BIGINT, precision : 19, scale : 0) null 22 222 3333 ",
                "COL1(datatype : BIGINT, precision : 19, scale : 0) null 22 222 3333 ",
                "COL1(datatype : DECIMAL, precision : 24, scale : 5) null 22.00000 222.00000 3333.00000 ",
                "COL1(datatype : DECIMAL, precision : 24, scale : 5) null 22.00000 222.00000 3333.00000 ",
                "COL1(datatype : DOUBLE, precision : 15, scale : 0) null 22.0 222.0 3333.0 ",
                "COL1(datatype : DOUBLE, precision : 15, scale : 0) null 22.0 222.0 3333.0 ",
                "COL1(datatype : DOUBLE, precision : 15, scale : 0) null 22.0 222.0 3333.0 ",
                "COL1(datatype : DOUBLE, precision : 15, scale : 0) null 22.0 222.0 3333.0 ",
                "","","","","","","","","","","","","","","","","","","","","","",
                "COL1(datatype : DECIMAL, precision : 10, scale : 5) null 3.30000 3.33000 2.00000 ",
                "COL1(datatype : DECIMAL, precision : 10, scale : 5) null 3.30000 3.33000 2.00000 ",
                "COL1(datatype : DECIMAL, precision : 15, scale : 5) null 3.30000 3.33000 null ",
                "COL1(datatype : DECIMAL, precision : 15, scale : 5) null 3.30000 3.33000 null ",
                "COL1(datatype : DECIMAL, precision : 24, scale : 5) null 3.30000 3.33000 3333.00000 ",
                "COL1(datatype : DECIMAL, precision : 24, scale : 5) null 3.30000 3.33000 3333.00000 ",
                "COL1(datatype : DECIMAL, precision : 10, scale : 5) null 3.30000 3.33000 null ",
                "COL1(datatype : DECIMAL, precision : 10, scale : 5) null 3.30000 3.33000 null ",
                "COL1(datatype : DOUBLE, precision : 15, scale : 0) null 3.3 3.33 4.443999767303467 ",
                "COL1(datatype : DOUBLE, precision : 15, scale : 0) null 3.3 3.33 4.443999767303467 ",
                "COL1(datatype : DOUBLE, precision : 15, scale : 0) null 3.3 3.33 null ",
                "COL1(datatype : DOUBLE, precision : 15, scale : 0) null 3.3 3.33 null ",
                "","","","","","","","","","","","","","","","","","","","","","",
                "COL1(datatype : DOUBLE, precision : 15, scale : 0) null 4.400000095367432 4.440000057220459 4.443999767303467 ",
                "COL1(datatype : DOUBLE, precision : 15, scale : 0) null 4.400000095367432 4.440000057220459 4.443999767303467 ",
                "COL1(datatype : DOUBLE, precision : 15, scale : 0) null 4.400000095367432 4.440000057220459 4.443999767303467 ",
                "COL1(datatype : DOUBLE, precision : 15, scale : 0) null 4.400000095367432 4.440000057220459 4.443999767303467 ",
                "COL1(datatype : DOUBLE, precision : 15, scale : 0) null 4.400000095367432 4.440000057220459 4.443999767303467 ",
                "COL1(datatype : DOUBLE, precision : 15, scale : 0) null 4.400000095367432 4.440000057220459 4.443999767303467 ",
                "COL1(datatype : DOUBLE, precision : 15, scale : 0) null 4.400000095367432 4.440000057220459 4.443999767303467 ",
                "COL1(datatype : DOUBLE, precision : 15, scale : 0) null 4.400000095367432 4.440000057220459 4.443999767303467 ",
                "COL1(datatype : REAL, precision : 7, scale : 0) null 4.4 4.44 4.444 ",
                "COL1(datatype : REAL, precision : 7, scale : 0) null 4.4 4.44 4.444 ",
                "COL1(datatype : DOUBLE, precision : 15, scale : 0) null 4.400000095367432 4.440000057220459 4.443999767303467 ",
                "COL1(datatype : DOUBLE, precision : 15, scale : 0) null 4.400000095367432 4.440000057220459 4.443999767303467 ",
                "","","","","","","","","","","","","","","","","","","","","","",
                "COL1(datatype : DOUBLE, precision : 15, scale : 0) null 5.5 5.55 2.0 ",
                "COL1(datatype : DOUBLE, precision : 15, scale : 0) null 5.5 5.55 2.0 ",
                "COL1(datatype : DOUBLE, precision : 15, scale : 0) null 5.5 5.55 null ",
                "COL1(datatype : DOUBLE, precision : 15, scale : 0) null 5.5 5.55 null ",
                "COL1(datatype : DOUBLE, precision : 15, scale : 0) null 5.5 5.55 3333.0 ",
                "COL1(datatype : DOUBLE, precision : 15, scale : 0) null 5.5 5.55 3333.0 ",
                "COL1(datatype : DOUBLE, precision : 15, scale : 0) null 5.5 5.55 null ",
                "COL1(datatype : DOUBLE, precision : 15, scale : 0) null 5.5 5.55 null ",
                "COL1(datatype : DOUBLE, precision : 15, scale : 0) null 5.5 5.55 4.443999767303467 ",
                "COL1(datatype : DOUBLE, precision : 15, scale : 0) null 5.5 5.55 4.443999767303467 ",
                "COL1(datatype : DOUBLE, precision : 15, scale : 0) null 5.5 5.55 null ",
                "COL1(datatype : DOUBLE, precision : 15, scale : 0) null 5.5 5.55 null ",
                "","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","",
                "COL1(datatype : CHAR, precision : 60, scale : 0) null 1992-01-06                                                   1992-01-16                                                   null ",
                "COL1(datatype : CHAR, precision : 60, scale : 0) null 1992-01-06                                                   1992-01-16                                                   null ",
                "COL1(datatype : VARCHAR, precision : 60, scale : 0) null 1992-01-06                                                   1992-01-16                                                   15:30:20 ",
                "COL1(datatype : VARCHAR, precision : 60, scale : 0) null 1992-01-06                                                   1992-01-16                                                   15:30:20 ",
                "COL1(datatype : LONG VARCHAR, precision : 32700, scale : 0) null 1992-01-06                                                   1992-01-16                                                   2000-01-01 15:30:20 ",
                "COL1(datatype : LONG VARCHAR, precision : 32700, scale : 0) null 1992-01-06                                                   1992-01-16                                                   2000-01-01 15:30:20 ",
                "","","","","","",
                "COL1(datatype : CLOB, precision : 1024, scale : 0) null 1992-01-06                                                   1992-01-16                                                   null ",
                "COL1(datatype : CLOB, precision : 1024, scale : 0) null 1992-01-06                                                   1992-01-16                                                   null ",
                "COL1(datatype : DATE, precision : 10, scale : 0) null 1992-01-06 1992-01-16 null ",
                "COL1(datatype : DATE, precision : 10, scale : 0) null 1992-01-06 1992-01-16 null ",
                "","","","","","","","","","","","","","","","","","",
                "COL1(datatype : VARCHAR, precision : 60, scale : 0) null 1992-01-07 1992-01-17 15:30:20 ",
                "COL1(datatype : VARCHAR, precision : 60, scale : 0) null 1992-01-07 1992-01-17 15:30:20 ",
                "COL1(datatype : VARCHAR, precision : 60, scale : 0) null 1992-01-07 1992-01-17 15:30:20 ",
                "COL1(datatype : VARCHAR, precision : 60, scale : 0) null 1992-01-07 1992-01-17 15:30:20 ",
                "COL1(datatype : LONG VARCHAR, precision : 32700, scale : 0) null 1992-01-07 1992-01-17 15:30:20 ",
                "COL1(datatype : LONG VARCHAR, precision : 32700, scale : 0) null 1992-01-07 1992-01-17 15:30:20 ",
                "","","","","","",
                "COL1(datatype : CLOB, precision : 1024, scale : 0) null 1992-01-07 1992-01-17 15:30:20 ",
                "COL1(datatype : CLOB, precision : 1024, scale : 0) null 1992-01-07 1992-01-17 15:30:20 ",
                "","","","","","","","","","","","","","","","","","","","",
                "COL1(datatype : LONG VARCHAR, precision : 32700, scale : 0) null 1992-01-08 1992-01-18 2000-01-01 15:30:20 ",
                "COL1(datatype : LONG VARCHAR, precision : 32700, scale : 0) null 1992-01-08 1992-01-18 2000-01-01 15:30:20 ",
                "COL1(datatype : LONG VARCHAR, precision : 32700, scale : 0) null 1992-01-08 1992-01-18 2000-01-01 15:30:20 ",
                "COL1(datatype : LONG VARCHAR, precision : 32700, scale : 0) null 1992-01-08 1992-01-18 2000-01-01 15:30:20 ",
                "COL1(datatype : LONG VARCHAR, precision : 32700, scale : 0) null 1992-01-08 1992-01-18 2000-01-01 15:30:20 ",
                "COL1(datatype : LONG VARCHAR, precision : 32700, scale : 0) null 1992-01-08 1992-01-18 2000-01-01 15:30:20 ",
                "","","","","","",
                "COL1(datatype : CLOB, precision : 32700, scale : 0) null 1992-01-08 1992-01-18 2000-01-01 15:30:20 ",
                "COL1(datatype : CLOB, precision : 32700, scale : 0) null 1992-01-08 1992-01-18 2000-01-01 15:30:20 ",
                "","","","","","","","","","","","","","","","","","","","","","","","","","",
                "COL1(datatype : CHAR () FOR BIT DATA, precision : 60, scale : 0) null 10aa20202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020 null 10aaaa202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020 ",
                "COL1(datatype : CHAR () FOR BIT DATA, precision : 60, scale : 0) null 10aa20202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020 null 10aaaa202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020 ",
                "COL1(datatype : VARCHAR () FOR BIT DATA, precision : 60, scale : 0) null 10aa20202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020 null 10aaaa202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020 ",
                "COL1(datatype : VARCHAR () FOR BIT DATA, precision : 60, scale : 0) null 10aa20202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020 null 10aaaa202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020 ",
                "COL1(datatype : LONG VARCHAR FOR BIT DATA, precision : 32700, scale : 0) null 10aa20202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020 null 10aaaa202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020 ",
                "COL1(datatype : LONG VARCHAR FOR BIT DATA, precision : 32700, scale : 0) null 10aa20202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020 null 10aaaa202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020 ",
                "","","","","","","","","","","","","","","","","","","","","","","","","","","","",
                "COL1(datatype : VARCHAR () FOR BIT DATA, precision : 60, scale : 0) null 10bb null 10bbbb ",
                "COL1(datatype : VARCHAR () FOR BIT DATA, precision : 60, scale : 0) null 10bb null 10bbbb ",
                "COL1(datatype : VARCHAR () FOR BIT DATA, precision : 60, scale : 0) null 10bb null 10bbbb ",
                "COL1(datatype : VARCHAR () FOR BIT DATA, precision : 60, scale : 0) null 10bb null 10bbbb ",
                "COL1(datatype : LONG VARCHAR FOR BIT DATA, precision : 32700, scale : 0) null 10bb null 10bbbb ",
                "COL1(datatype : LONG VARCHAR FOR BIT DATA, precision : 32700, scale : 0) null 10bb null 10bbbb ",
                "","","","","","","","","","","","","","","","","","","","","","","","","","","","",
                "COL1(datatype : LONG VARCHAR FOR BIT DATA, precision : 32700, scale : 0) null 10cc null 10cccc ",
                "COL1(datatype : LONG VARCHAR FOR BIT DATA, precision : 32700, scale : 0) null 10cc null 10cccc ",
                "COL1(datatype : LONG VARCHAR FOR BIT DATA, precision : 32700, scale : 0) null 10cc null 10cccc ",
                "COL1(datatype : LONG VARCHAR FOR BIT DATA, precision : 32700, scale : 0) null 10cc null 10cccc ",
                "COL1(datatype : LONG VARCHAR FOR BIT DATA, precision : 32700, scale : 0) null 10cc null 10cccc ",
                "COL1(datatype : LONG VARCHAR FOR BIT DATA, precision : 32700, scale : 0) null 10cc null 10cccc ",
                "","","","","","","","","","","","","","","","","","","","","","",
                "COL1(datatype : CLOB, precision : 1024, scale : 0) null 13 14 null ",
                "COL1(datatype : CLOB, precision : 1024, scale : 0) null 13 14 null ",
                "COL1(datatype : CLOB, precision : 1024, scale : 0) null 13 14 15:30:20 ",
                "COL1(datatype : CLOB, precision : 1024, scale : 0) null 13 14 15:30:20 ",
                "COL1(datatype : CLOB, precision : 32700, scale : 0) null 13 14 2000-01-01 15:30:20 ",
                "COL1(datatype : CLOB, precision : 32700, scale : 0) null 13 14 2000-01-01 15:30:20 ",
                "","","","","","","COL1(datatype : CLOB, precision : 1024, scale : 0) null 13 14 null ",
                "COL1(datatype : CLOB, precision : 1024, scale : 0) null 13 14 null ",
                "","","","","","","","","","","","","","","","","","","","",
                "COL1(datatype : DATE, precision : 10, scale : 0) null 2000-01-01 2000-01-01 null ",
                "COL1(datatype : DATE, precision : 10, scale : 0) null 2000-01-01 2000-01-01 null ",
                "","","","","","","","","","","","",
                "COL1(datatype : DATE, precision : 10, scale : 0) null 2000-01-01 2000-01-01 null ",
                "COL1(datatype : DATE, precision : 10, scale : 0) null 2000-01-01 2000-01-01 null ",
                "","","","","","","","","","","","","","","","","","",
                "COL1(datatype : TIME, precision : 8, scale : 0) null 15:30:20 15:30:20 null ",
                "COL1(datatype : TIME, precision : 8, scale : 0) null 15:30:20 15:30:20 null ",
                "COL1(datatype : TIME, precision : 8, scale : 0) null 15:30:20 15:30:20 15:30:20 ",
                "COL1(datatype : TIME, precision : 8, scale : 0) null 15:30:20 15:30:20 15:30:20 ",
                "","","","","","","","","","","","",
                "COL1(datatype : TIME, precision : 8, scale : 0) null 15:30:20 15:30:20 null ",
                "COL1(datatype : TIME, precision : 8, scale : 0) null 15:30:20 15:30:20 null ",
                "","","","","","","","","","","","","","","","",
                "COL1(datatype : TIMESTAMP, precision : 26, scale : 6) null 2000-01-01 15:30:20.0 2000-01-01 15:30:20.0 null ",
                "COL1(datatype : TIMESTAMP, precision : 26, scale : 6) null 2000-01-01 15:30:20.0 2000-01-01 15:30:20.0 null ",
                "","","","","","","","","","","","","","","","",
                "COL1(datatype : TIMESTAMP, precision : 26, scale : 6) null 2000-01-01 15:30:20.0 2000-01-01 15:30:20.0 null ",
                "COL1(datatype : TIMESTAMP, precision : 26, scale : 6) null 2000-01-01 15:30:20.0 2000-01-01 15:30:20.0 null ",
                "","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","",
                "COL1(datatype : BLOB, precision : 1024, scale : 0) null null null null ",
                "COL1(datatype : BLOB, precision : 1024, scale : 0) null null null null "
        };

        int index = 0;

        for (int firstColumnType = 0; firstColumnType < SQLTypes.length; firstColumnType++ ) {
            for (int secondColumnType = 0; secondColumnType < SQLTypes.length; secondColumnType++) {
                try {
                    String coalesceString = "SELECT COALESCE(" + ColumnNames[firstColumnType] + "," + ColumnNames[secondColumnType] + ") from AllDataTypesTable";
                    dumpRS(s.executeQuery(coalesceString), expectedValues[index]);
                } catch (SQLException sqle) {     
                    if (sqle.getSQLState().equals("22007"))
                        assertSQLState("22007", sqle);
                    else if (!isSupportedCoalesce(firstColumnType,secondColumnType)  && sqle.getSQLState().equals("42815"))
                        assertSQLState("42815", sqle);
                    else
                        fail("CoalesceTest: expected SQLException was not thrown.");
                } 
                index++;          
                try {
                    String valueString = "SELECT VALUE(" + ColumnNames[firstColumnType] + "," + ColumnNames[secondColumnType] + ") from AllDataTypesTable";
                    dumpRS(s.executeQuery(valueString), expectedValues[index]);
                } catch (SQLException sqle) {
                    if (sqle.getSQLState().equals("22007"))
                        assertSQLState("22007", sqle);
                    else if (!isSupportedCoalesce(firstColumnType,secondColumnType)  && sqle.getSQLState().equals("42815"))
                        assertSQLState("42815", sqle);
                    else
                        fail("CoalesceTest: expected SQLException was not thrown.");
                }
                index++;
            }
        }
    }

    public void testDateCoalesce() throws Throwable
    {	
        s.executeUpdate("insert into tF values(null, null, null)");
        s.executeUpdate("insert into tF values(date('1992-01-02'), '1992-01-03', '1992-01-04')");

        String expectedValue = "COL1(datatype : DATE, precision : 10, scale : 0) null 1992-01-02 ";
        String expectedValue1 = "COL1(datatype : DATE, precision : 10, scale : 0) null 1992-01-03 ";
        String expectedValue2 = "COL1(datatype : DATE, precision : 10, scale : 0) null 1992-01-04 ";

        dumpRS(s.executeQuery("select coalesce(dateCol,dateCol) from tF"), expectedValue);
        dumpRS(s.executeQuery("select value(dateCol,dateCol) from tF"), expectedValue);

        dumpRS(s.executeQuery("select coalesce(dateCol,charCol) from tF"), expectedValue);
        dumpRS(s.executeQuery("select value(dateCol,charCol) from tF"), expectedValue);

        dumpRS(s.executeQuery("select coalesce(charCol,dateCol) from tF"), expectedValue1);
        dumpRS(s.executeQuery("select value(charCol,dateCol) from tF"), expectedValue1);

        dumpRS(s.executeQuery("select coalesce(dateCol,varcharCol) from tF"), expectedValue);
        dumpRS(s.executeQuery("select value(dateCol,varcharCol) from tF"), expectedValue);

        dumpRS(s.executeQuery("select coalesce(varcharCol,dateCol) from tF"), expectedValue2);
        dumpRS(s.executeQuery("select value(varcharCol,dateCol) from tF"), expectedValue2);

        s.executeUpdate("insert into tF values(date('1992-01-01'), 'I am char', 'I am varchar')");

        try {
            dumpRS(s.executeQuery("select coalesce(charCol,dateCol) from tF"), "");
        } catch (SQLException sqle) {
            assertSQLState("22007", sqle);
        }

        try {
            dumpRS(s.executeQuery("select value(charCol,dateCol) from tF"), "");	
        } catch (SQLException sqle) {
            assertSQLState("22007", sqle);
        }

        try {
            dumpRS(s.executeQuery("select coalesce(varcharCol,dateCol) from tF"), "");
        } catch (SQLException sqle) {
            assertSQLState("22007", sqle);
        }

        try {
            dumpRS(s.executeQuery("select value(varcharCol,dateCol) from tF"), "");
        } catch (SQLException sqle) {
            assertSQLState("22007", sqle);
        }

    }

    public void testTimeCoalesce() throws Throwable
    {
        s.executeUpdate("insert into tG values(null, null, null)");
        s.executeUpdate("insert into tG values(time('12:30:30'), '12:30:31', '12:30:32')");

        String expectedValue = "COL1(datatype : TIME, precision : 8, scale : 0) null 12:30:30 ";
        String expectedValue1 = "COL1(datatype : TIME, precision : 8, scale : 0) null 12:30:31 ";
        String expectedValue2 = "COL1(datatype : TIME, precision : 8, scale : 0) null 12:30:32 ";

        dumpRS(s.executeQuery("select coalesce(timeCol,timeCol) from tG"), expectedValue);
        dumpRS(s.executeQuery("select value(timeCol,timeCol) from tG"), expectedValue);

        dumpRS(s.executeQuery("select coalesce(timeCol,charCol) from tG"),expectedValue);
        dumpRS(s.executeQuery("select value(timeCol,charCol) from tG"), expectedValue);

        dumpRS(s.executeQuery("select coalesce(charCol,timeCol) from tG"), expectedValue1);
        dumpRS(s.executeQuery("select value(charCol,timeCol) from tG"), expectedValue1);

        dumpRS(s.executeQuery("select coalesce(timeCol,varcharCol) from tG"), expectedValue);
        dumpRS(s.executeQuery("select value(timeCol,varcharCol) from tG"), expectedValue);

        dumpRS(s.executeQuery("select coalesce(varcharCol,timeCol) from tG"), expectedValue2);
        dumpRS(s.executeQuery("select value(varcharCol,timeCol) from tG"), expectedValue2);

        s.executeUpdate("insert into tG values(time('12:30:33'), 'I am char', 'I am varchar')");

        try {
            dumpRS(s.executeQuery("select coalesce(charCol,timeCol) from tG"), "");
        } catch (SQLException sqle) {
            assertSQLState("22007", sqle);	
        }

        try {
            dumpRS(s.executeQuery("select value(charCol,timeCol) from tG"), "");
        } catch (SQLException sqle) {
            assertSQLState("22007", sqle);			
        }

        try {
            dumpRS(s.executeQuery("select coalesce(charCol,timeCol) from tG"), "");
        } catch (SQLException sqle) {
            assertSQLState("22007", sqle);		
        }

        try {
            dumpRS(s.executeQuery("select value(charCol,timeCol) from tG"), "");
        } catch (SQLException sqle) {
            assertSQLState("22007", sqle);	
        }

        expectedValue = "COL1(datatype : TIME, precision : 8, scale : 0) null 12:30:30 12:30:33 ";
        dumpRS(s.executeQuery("select coalesce(timeCol,charCol) from tG"), expectedValue);			
        dumpRS(s.executeQuery("select value(timeCol,charCol) from tG"), expectedValue);
        dumpRS(s.executeQuery("select coalesce(timeCol,varcharCol) from tG"), expectedValue);			
        dumpRS(s.executeQuery("select value(timeCol,varcharCol) from tG"), expectedValue);
    }

    public void testTimeStampCoalesce() throws Throwable
    {
        s.executeUpdate("insert into tH values(null, null, null)");
        s.executeUpdate("insert into tH values(timestamp('1992-01-01 12:30:30'), '1992-01-01 12:30:31', '1992-01-01 12:30:32')");

        String expectedValue = "COL1(datatype : TIMESTAMP, precision : 26, scale : 6) null 1992-01-01 12:30:30.0 ";
        String expectedValue1 = "COL1(datatype : TIMESTAMP, precision : 26, scale : 6) null 1992-01-01 12:30:31.0 ";
        String expectedValue2 = "COL1(datatype : TIMESTAMP, precision : 26, scale : 6) null 1992-01-01 12:30:32.0 ";

        dumpRS(s.executeQuery("select coalesce(timestampCol,timestampCol) from tH"), expectedValue);
        dumpRS(s.executeQuery("select value(timestampCol,timestampCol) from tH"), expectedValue);

        dumpRS(s.executeQuery("select coalesce(timestampCol,charCol) from tH"), expectedValue);
        dumpRS(s.executeQuery("select value(timestampCol,charCol) from tH"), expectedValue);

        dumpRS(s.executeQuery("select coalesce(charCol,timestampCol) from tH"), expectedValue1);
        dumpRS(s.executeQuery("select value(charCol,timestampCol) from tH"), expectedValue1);

        dumpRS(s.executeQuery("select coalesce(timestampCol,varcharCol) from tH"), expectedValue);
        dumpRS(s.executeQuery("select value(timestampCol,varcharCol) from tH"), expectedValue);

        dumpRS(s.executeQuery("select coalesce(varcharCol,timestampCol) from tH"), expectedValue2);
        dumpRS(s.executeQuery("select value(varcharCol,timestampCol) from tH"), expectedValue2);

        s.executeUpdate("insert into tH values(timestamp('1992-01-01 12:30:33'), 'I am char', 'I am varchar')");

        try {
            dumpRS(s.executeQuery("select coalesce(charCol,timestampCol) from tH"), "");
        } catch (SQLException sqle) {
            assertSQLState("22007", sqle);
        }

        try {
            dumpRS(s.executeQuery("select value(charCol,timestampCol) from tH"), "");
        } catch (SQLException sqle) {
            assertSQLState("22007", sqle);
        }

        try {
            dumpRS(s.executeQuery("select coalesce(charCol,timestampCol) from tH"), "");
        } catch (SQLException sqle) {
            assertSQLState("22007", sqle);
        }

        try {
            dumpRS(s.executeQuery("select value(charCol,timestampCol) from tH"), "");
        } catch (SQLException sqle) {
            assertSQLState("22007", sqle);
        }
    }

    public void testNumericCoalesce() throws Throwable
    {
        s.executeUpdate("insert into tE values(1, 2, 3, 4, 5.5, 6.6, 7.7, 3.4028235E38)");
        s.executeUpdate("insert into tE values(null,null,null,null,null,null,null,null)");

        String[] expectedValues = {
                "COL1(datatype : SMALLINT, precision : 5, scale : 0) 1 null ",
                "COL1(datatype : INTEGER, precision : 10, scale : 0) 1 null ",
                "COL1(datatype : BIGINT, precision : 19, scale : 0) 1 null ",
                "COL1(datatype : DECIMAL, precision : 22, scale : 2) 1.00 null ",
                "COL1(datatype : DECIMAL, precision : 11, scale : 6) 1.000000 null ",
                "COL1(datatype : DECIMAL, precision : 31, scale : 28) 1.0000000000000000000000000000 null ",
                "COL1(datatype : DOUBLE, precision : 15, scale : 0) 1.0 null ",
                "COL1(datatype : INTEGER, precision : 10, scale : 0) 2 null ",
                "COL1(datatype : BIGINT, precision : 19, scale : 0) 2 null ",
                "COL1(datatype : DECIMAL, precision : 22, scale : 2) 2.00 null ",
                "COL1(datatype : DECIMAL, precision : 16, scale : 6) 2.000000 null ",
                "COL1(datatype : DECIMAL, precision : 31, scale : 28) 2.0000000000000000000000000000 null ",
                "COL1(datatype : DOUBLE, precision : 15, scale : 0) 2.0 null ",
                "COL1(datatype : BIGINT, precision : 19, scale : 0) 3 null ",
                "COL1(datatype : DECIMAL, precision : 22, scale : 2) 3.00 null ",
                "COL1(datatype : DECIMAL, precision : 25, scale : 6) 3.000000 null ",
                "COL1(datatype : DECIMAL, precision : 31, scale : 28) 3.0000000000000000000000000000 null ",
                "COL1(datatype : DOUBLE, precision : 15, scale : 0) 3.0 null ",			
                "COL1(datatype : DECIMAL, precision : 22, scale : 2) 4.00 null ",
                "COL1(datatype : DECIMAL, precision : 26, scale : 6) 4.000000 null ",
                "COL1(datatype : DECIMAL, precision : 31, scale : 28) 4.0000000000000000000000000000 null ",
                "COL1(datatype : DECIMAL, precision : 22, scale : 2) 4.00 null ",
                "COL1(datatype : DOUBLE, precision : 15, scale : 0) 4.0 null ",
                "COL1(datatype : REAL, precision : 7, scale : 0) 7.7 null ",
                "COL1(datatype : DOUBLE, precision : 15, scale : 0) 7.699999809265137 null ",
                "COL1(datatype : DOUBLE, precision : 15, scale : 0) 3.4028235E38 null "				 
        };

        dumpRS(s.executeQuery("select coalesce(smallintCol,smallintCol) from tE"), expectedValues[0]);
        dumpRS(s.executeQuery("select coalesce(smallintCol,intCol) from tE"), expectedValues[1]);
        dumpRS(s.executeQuery("select coalesce(smallintCol,bigintCol) from tE"), expectedValues[2]);
        dumpRS(s.executeQuery("select coalesce(smallintCol,decimalCol1) from tE"), expectedValues[3]);
        dumpRS(s.executeQuery("select coalesce(smallintCol,decimalCol2) from tE"), expectedValues[4]);
        dumpRS(s.executeQuery("select coalesce(smallintCol,decimalCol3) from tE"), expectedValues[5]);
        dumpRS(s.executeQuery("select coalesce(smallintCol,realCol) from tE"), expectedValues[6]);
        dumpRS(s.executeQuery("select coalesce(smallintCol,doubleCol) from tE"), expectedValues[6]);
        dumpRS(s.executeQuery("select coalesce(intCol,intCol) from tE"), expectedValues[7]);
        dumpRS(s.executeQuery("select coalesce(intCol,smallintCol) from tE"), expectedValues[7]);
        dumpRS(s.executeQuery("select coalesce(intCol,bigintCol) from tE"), expectedValues[8]);

        dumpRS(s.executeQuery("select coalesce(intCol,decimalCol1) from tE"), expectedValues[9]);
        dumpRS(s.executeQuery("select coalesce(intCol,decimalCol2) from tE"), expectedValues[10]);
        dumpRS(s.executeQuery("select coalesce(intCol,decimalCol3) from tE"), expectedValues[11]);
        dumpRS(s.executeQuery("select coalesce(intCol,realCol) from tE"), expectedValues[12]);
        dumpRS(s.executeQuery("select coalesce(intCol,doubleCol) from tE"), expectedValues[12]);
        dumpRS(s.executeQuery("select coalesce(bigintCol,bigintCol) from tE"), expectedValues[13]);
        dumpRS(s.executeQuery("select coalesce(bigintCol,smallintCol) from tE"), expectedValues[13]);
        dumpRS(s.executeQuery("select coalesce(bigintCol,intCol) from tE"), expectedValues[13]);
        dumpRS(s.executeQuery("select coalesce(bigintCol,decimalCol1) from tE"), expectedValues[14]);

        dumpRS(s.executeQuery("select coalesce(bigintCol,decimalCol2) from tE"), expectedValues[15]);
        dumpRS(s.executeQuery("select coalesce(bigintCol,decimalCol3) from tE"), expectedValues[16]);

        dumpRS(s.executeQuery("select coalesce(bigintCol,realCol) from tE"), expectedValues[17]);
        dumpRS(s.executeQuery("select coalesce(bigintCol,doubleCol) from tE"), expectedValues[17]);
        dumpRS(s.executeQuery("select coalesce(decimalCol1,decimalCol1) from tE"), expectedValues[18]);
        dumpRS(s.executeQuery("select coalesce(decimalCol1,decimalCol2) from tE"), expectedValues[19]);
        dumpRS(s.executeQuery("select coalesce(decimalCol1,decimalCol3) from tE"), expectedValues[20]);

        dumpRS(s.executeQuery("select coalesce(decimalCol1,smallintCol) from tE"), expectedValues[21]);
        dumpRS(s.executeQuery("select coalesce(decimalCol1,intCol) from tE"), expectedValues[21]);
        dumpRS(s.executeQuery("select coalesce(decimalCol1,bigintCol) from tE"), expectedValues[21]);

        dumpRS(s.executeQuery("select coalesce(decimalCol1,realCol) from tE"), expectedValues[22]);
        dumpRS(s.executeQuery("select coalesce(decimalCol1,doubleCol) from tE"), expectedValues[22]);

        dumpRS(s.executeQuery("select coalesce(realCol,realCol) from tE"), expectedValues[23]);

        dumpRS(s.executeQuery("select coalesce(realCol,smallintCol) from tE"), expectedValues[24]);
        dumpRS(s.executeQuery("select coalesce(realCol,intCol) from tE"), expectedValues[24]);
        dumpRS(s.executeQuery("select coalesce(realCol,bigintCol) from tE"), expectedValues[24]);
        dumpRS(s.executeQuery("select coalesce(realCol,decimalCol1) from tE"), expectedValues[24]);
        dumpRS(s.executeQuery("select coalesce(realCol,doubleCol) from tE"), expectedValues[24]);

        dumpRS(s.executeQuery("select coalesce(doubleCol,doubleCol) from tE"), expectedValues[25]);
        dumpRS(s.executeQuery("select coalesce(doubleCol,smallintCol) from tE"), expectedValues[25]);
        dumpRS(s.executeQuery("select coalesce(doubleCol,intCol) from tE"), expectedValues[25]);
        dumpRS(s.executeQuery("select coalesce(doubleCol,bigintCol) from tE"), expectedValues[25]);
        dumpRS(s.executeQuery("select coalesce(doubleCol,decimalCol1) from tE"), expectedValues[25]);
        dumpRS(s.executeQuery("select coalesce(doubleCol,realCol) from tE"), expectedValues[25]);

    }

    public void testMiscellaneousCoalesce() throws SQLException
    {
        String[] expectedValues = {
                "COL1(datatype : CHAR, precision : 50, scale : 0) asdfghj                                            ",
                "COL1(datatype : CHAR, precision : 50, scale : 0) asdfghj                                            ",
                "COL1(datatype : CHAR, precision : 254, scale : 0) first argument to coalesce                                                                                                                                                                                                                                     first argument to coalesce                                                                                                                                                                                                                                     ",
                "COL1(datatype : CHAR, precision : 254, scale : 0) first argument to value                                                                                                                                                                                                                                        first argument to value                                                                                                                                                                                                                                        ",
                "COL1(datatype : CHAR, precision : 254, scale : 0) abcdefgh                                                                                                                                                                                                                                                       null ",
                "COL1(datatype : CHAR, precision : 254, scale : 0) abcdefgh                                                                                                                                                                                                                                                       null ",

        };

        s.executeUpdate("insert into tD (c1,c2) values(1,'abcdefgh')");
        s.executeUpdate("insert into tD (c1) values(2)");

        int index = 0;

        dumpRS(s.executeQuery("values coalesce(cast('asdfghj' as char(30)),cast('asdf' as char(50)))"), expectedValues[index++]);
        dumpRS(s.executeQuery("values value(cast('asdfghj' as char(30)),cast('asdf' as char(50)))"), expectedValues[index++]);

        ps = prepareStatement("select coalesce(?,c2) from tD");
        ps.setString(1,"first argument to coalesce");
        dumpRS(ps.executeQuery(), expectedValues[index++]);

        ps = prepareStatement("select value(?,c2) from tD");
        ps.setString(1,"first argument to value");
        dumpRS(ps.executeQuery(), expectedValues[index++]);

        ps = prepareStatement("select coalesce(?,c2) from tD");
        ps.setNull(1,Types.CHAR);
        dumpRS(ps.executeQuery(), expectedValues[index++]);

        ps = prepareStatement("select value(?,c2) from tD");
        ps.setNull(1,Types.BIGINT);
        dumpRS(ps.executeQuery(), expectedValues[index++]);

        ps = prepareStatement("select coalesce(c1,?) from tD");	
        try {
            ps.setString(1,"abc");
            dumpRS(ps.executeQuery(), "");
        }
        catch (SQLException sqle) {
            assertSQLState("22018", sqle);
        }

    }

    public void testCharCoalesce() throws Throwable
    {
        String[] expectedValues = {
                "COL1(datatype : CHAR, precision : 254, scale : 0) c1 not null                                                                                                                                                                                                                                                    c1 not null but c2 is                                                                                                                                                                                                                                          c2 not null but c1 is                                                                                                                                                                                                                                          null ",
                "COL1(datatype : CHAR, precision : 254, scale : 0) c1 not null                                                                                                                                                                                                                                                    c1 not null but c2 is                                                                                                                                                                                                                                          c2 not null but c1 is                                                                                                                                                                                                                                          null ",
                "COL1(datatype : CHAR, precision : 254, scale : 0) c2 not null                                                                                                                                                                                                                                                    c1 not null but c2 is                                                                                                                                                                                                                                          c2 not null but c1 is                                                                                                                                                                                                                                          null ",
                "COL1(datatype : CHAR, precision : 254, scale : 0) c2 not null                                                                                                                                                                                                                                                    c1 not null but c2 is                                                                                                                                                                                                                                          c2 not null but c1 is                                                                                                                                                                                                                                          null ",
                "COL1(datatype : VARCHAR, precision : 254, scale : 0) c1 not null                                                                                                                                                                                                                                                    c1 not null but c2 is                                                                                                                                                                                                                                          null null ",
                "COL1(datatype : VARCHAR, precision : 254, scale : 0) c1 not null                                                                                                                                                                                                                                                    c1 not null but c2 is                                                                                                                                                                                                                                          null null ",
                "COL1(datatype : VARCHAR, precision : 254, scale : 0) vc1 not null vc1 is not null but vc2 is null null ",
                "COL1(datatype : VARCHAR, precision : 254, scale : 0) vc1 not null vc1 is not null but vc2 is null null ",
                "COL1(datatype : VARCHAR, precision : 2000, scale : 0) vc1 not null vc1 is not null but vc2 is vc2 is not null but vc1 is null ",
                "COL1(datatype : VARCHAR, precision : 2000, scale : 0) vc1 not null vc1 is not null but vc2 is vc2 is not null but vc1 is null ",
                "COL1(datatype : VARCHAR, precision : 2000, scale : 0) vc2 not null vc1 is not null but vc2 is vc2 is not null but vc1 is null ",
                "COL1(datatype : VARCHAR, precision : 2000, scale : 0) vc2 not null vc1 is not null but vc2 is vc2 is not null but vc1 is null ",
                "COL1(datatype : LONG VARCHAR, precision : 32700, scale : 0) c1 not null                                                                                                                                                                                                                                                    c1 not null but c2 is                                                                                                                                                                                                                                          lvc1 not null again null ",
                "COL1(datatype : LONG VARCHAR, precision : 32700, scale : 0) c1 not null                                                                                                                                                                                                                                                    c1 not null but c2 is                                                                                                                                                                                                                                          lvc1 not null again null ",
                "COL1(datatype : LONG VARCHAR, precision : 32700, scale : 0) lvc1 not null c1 not null but c2 is                                                                                                                                                                                                                                          lvc1 not null again null ",
                "COL1(datatype : LONG VARCHAR, precision : 32700, scale : 0) lvc1 not null c1 not null but c2 is                                                                                                                                                                                                                                          lvc1 not null again null ",
                "COL1(datatype : LONG VARCHAR, precision : 32700, scale : 0) vc1 not null vc1 is not null but vc2 is lvc1 not null again null ",
                "COL1(datatype : LONG VARCHAR, precision : 32700, scale : 0) vc1 not null vc1 is not null but vc2 is lvc1 not null again null ",
                "COL1(datatype : LONG VARCHAR, precision : 32700, scale : 0) lvc1 not null vc1 is not null but vc2 is lvc1 not null again null ",
                "COL1(datatype : LONG VARCHAR, precision : 32700, scale : 0) lvc1 not null vc1 is not null but vc2 is lvc1 not null again null ",
                "COL1(datatype : LONG VARCHAR, precision : 32700, scale : 0) lvc1 not null null lvc1 not null again null ",
                "COL1(datatype : LONG VARCHAR, precision : 32700, scale : 0) lvc1 not null null lvc1 not null again null ",
                "COL1(datatype : CLOB, precision : 254, scale : 0) clob1 not null c1 not null but c2 is                                                                                                                                                                                                                                          clob1 not null again null ",
                "COL1(datatype : CLOB, precision : 254, scale : 0) clob1 not null c1 not null but c2 is                                                                                                                                                                                                                                          clob1 not null again null ",
                "COL1(datatype : CLOB, precision : 33792, scale : 0) c1 not null                                                                                                                                                                                                                                                    c1 not null but c2 is                                                                                                                                                                                                                                          clob2 not null again null ",
                "COL1(datatype : CLOB, precision : 33792, scale : 0) c1 not null                                                                                                                                                                                                                                                    c1 not null but c2 is                                                                                                                                                                                                                                          clob2 not null again null ",
                "COL1(datatype : CLOB, precision : 253, scale : 0) clob1 not null vc1 is not null but vc2 is clob1 not null again null ",
                "COL1(datatype : CLOB, precision : 253, scale : 0) clob1 not null vc1 is not null but vc2 is clob1 not null again null ",
                "COL1(datatype : CLOB, precision : 33792, scale : 0) vc2 not null null vc2 is not null but vc1 is null ",
                "COL1(datatype : CLOB, precision : 33792, scale : 0) vc2 not null null vc2 is not null but vc1 is null ",
                "COL1(datatype : CLOB, precision : 32700, scale : 0) clob1 not null null clob1 not null again null ",
                "COL1(datatype : CLOB, precision : 32700, scale : 0) clob1 not null null clob1 not null again null ",
                "COL1(datatype : CLOB, precision : 33792, scale : 0) lvc2 not null null lvc2 not null again null ",
                "COL1(datatype : CLOB, precision : 33792, scale : 0) lvc2 not null null lvc2 not null again null ",
                "COL1(datatype : CLOB, precision : 33792, scale : 0) clob1 not null null clob1 not null again null ",
                "COL1(datatype : CLOB, precision : 33792, scale : 0) clob1 not null null clob1 not null again null ",

        };

        s.executeUpdate("insert into tB values('c1 not null', 'c2 not null', 'vc1 not null', 'vc2 not null', 'lvc1 not null', 'lvc2 not null', 'clob1 not null', 'clob2 not null')");
        s.executeUpdate("insert into tB values('c1 not null but c2 is', null, 'vc1 is not null but vc2 is', null, null, null,null,null)");
        s.executeUpdate("insert into tB values(null,'c2 not null but c1 is', null, 'vc2 is not null but vc1 is', 'lvc1 not null again', 'lvc2 not null again', 'clob1 not null again', 'clob2 not null again')");
        s.executeUpdate("insert into tB values(null,null, null, null, null, null, null, null)");


        int index = 0;

        dumpRS(s.executeQuery("select coalesce(c1,c2) from tB"), expectedValues[index++]);			
        dumpRS(s.executeQuery("select value(c1,c2) from tB"), expectedValues[index++]);			
        dumpRS(s.executeQuery("select coalesce(c2,c1) from tB"), expectedValues[index++]);
        dumpRS(s.executeQuery("select value(c2,c1) from tB"), expectedValues[index++]);
        dumpRS(s.executeQuery("select coalesce(c1,vc1) from tB"), expectedValues[index++]);
        dumpRS(s.executeQuery("select value(c1,vc1) from tB"), expectedValues[index++]);
        dumpRS(s.executeQuery("select coalesce(vc1,c1) from tB"), expectedValues[index++]);
        dumpRS(s.executeQuery("select value(vc1,c1) from tB"), expectedValues[index++]);		
        dumpRS(s.executeQuery("select coalesce(vc1,vc2) from tB"), expectedValues[index++]);
        dumpRS(s.executeQuery("select value(vc1,vc2) from tB"), expectedValues[index++]);
        dumpRS(s.executeQuery("select coalesce(vc2,vc1) from tB"), expectedValues[index++]);
        dumpRS(s.executeQuery("select value(vc2,vc1) from tB"), expectedValues[index++]);
        dumpRS(s.executeQuery("select coalesce(c1,lvc1) from tB"), expectedValues[index++]);
        dumpRS(s.executeQuery("select value(c1,lvc1) from tB"), expectedValues[index++]);
        dumpRS(s.executeQuery("select coalesce(lvc1,c1) from tB"), expectedValues[index++]);
        dumpRS(s.executeQuery("select value(lvc1,c1) from tB"), expectedValues[index++]);
        dumpRS(s.executeQuery("select coalesce(vc1,lvc1) from tB"), expectedValues[index++]);
        dumpRS(s.executeQuery("select value(vc1,lvc1) from tB"), expectedValues[index++]);
        dumpRS(s.executeQuery("select coalesce(lvc1,vc1) from tB"), expectedValues[index++]);
        dumpRS(s.executeQuery("select value(lvc1,vc1) from tB"), expectedValues[index++]);
        dumpRS(s.executeQuery("select coalesce(lvc1,lvc2) from tB"), expectedValues[index++]);
        dumpRS(s.executeQuery("select value(lvc1,lvc2) from tB"), expectedValues[index++]);
        dumpRS(s.executeQuery("select coalesce(clob1,c1) from tB"), expectedValues[index++]);
        dumpRS(s.executeQuery("select value(clob1,c1) from tB"), expectedValues[index++]);
        dumpRS(s.executeQuery("select coalesce(c1,clob2) from tB"), expectedValues[index++]);
        dumpRS(s.executeQuery("select value(c1,clob2) from tB"), expectedValues[index++]);
        dumpRS(s.executeQuery("select coalesce(clob1,vc1) from tB"), expectedValues[index++]);
        dumpRS(s.executeQuery("select value(clob1,vc1) from tB"), expectedValues[index++]);
        dumpRS(s.executeQuery("select coalesce(vc2,clob2) from tB"), expectedValues[index++]);
        dumpRS(s.executeQuery("select value(vc2,clob2) from tB"), expectedValues[index++]);
        dumpRS(s.executeQuery("select coalesce(clob1,lvc1) from tB"), expectedValues[index++]);
        dumpRS(s.executeQuery("select value(clob1,lvc1) from tB"), expectedValues[index++]);
        dumpRS(s.executeQuery("select coalesce(lvc2,clob2) from tB"), expectedValues[index++]);
        dumpRS(s.executeQuery("select value(lvc2,clob2) from tB"), expectedValues[index++]);
        dumpRS(s.executeQuery("select coalesce(clob1,clob2) from tB"), expectedValues[index++]);
        dumpRS(s.executeQuery("select value(clob1,clob2) from tB"), expectedValues[index++]);

    }

    public void testCharForBitDataCoalesce() throws SQLException, UnsupportedEncodingException
    {

        String[] expectedValues = {
                "COL1(datatype : CHAR () FOR BIT DATA, precision : 254, scale : 0) 63626431206e6f74206e756c6c20202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020 63626431206e6f74206e756c6c20627574206362643220697320202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020 63626432206e6f74206e756c6c20627574206362643120697320202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020 null ",
                "COL1(datatype : CHAR () FOR BIT DATA, precision : 254, scale : 0) 63626431206e6f74206e756c6c20202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020 63626431206e6f74206e756c6c20627574206362643220697320202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020 63626432206e6f74206e756c6c20627574206362643120697320202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020 null ",
                "COL1(datatype : CHAR () FOR BIT DATA, precision : 254, scale : 0) 63626432206e6f74206e756c6c20202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020 63626431206e6f74206e756c6c20627574206362643220697320202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020 63626432206e6f74206e756c6c20627574206362643120697320202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020 null ",
                "COL1(datatype : CHAR () FOR BIT DATA, precision : 254, scale : 0) 63626432206e6f74206e756c6c20202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020 63626431206e6f74206e756c6c20627574206362643220697320202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020 63626432206e6f74206e756c6c20627574206362643120697320202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020 null ",
                "COL1(datatype : VARCHAR () FOR BIT DATA, precision : 254, scale : 0) 63626431206e6f74206e756c6c20202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020 63626431206e6f74206e756c6c20627574206362643220697320202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020 null null ",
                "COL1(datatype : VARCHAR () FOR BIT DATA, precision : 254, scale : 0) 63626431206e6f74206e756c6c20202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020 63626431206e6f74206e756c6c20627574206362643220697320202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020 null null ",
                "COL1(datatype : VARCHAR () FOR BIT DATA, precision : 254, scale : 0) 7663626431206e6f74206e756c6c 7663626431206e6f74206e756c6c20627574207663626432206973 null null ",
                "COL1(datatype : VARCHAR () FOR BIT DATA, precision : 254, scale : 0) 7663626431206e6f74206e756c6c 7663626431206e6f74206e756c6c20627574207663626432206973 null null ",
                "COL1(datatype : VARCHAR () FOR BIT DATA, precision : 2000, scale : 0) 7663626431206e6f74206e756c6c 7663626431206e6f74206e756c6c20627574207663626432206973 7663626432206e6f74206e756c6c20627574207663626431206973 null ",
                "COL1(datatype : VARCHAR () FOR BIT DATA, precision : 2000, scale : 0) 7663626431206e6f74206e756c6c 7663626431206e6f74206e756c6c20627574207663626432206973 7663626432206e6f74206e756c6c20627574207663626431206973 null ",
                "COL1(datatype : VARCHAR () FOR BIT DATA, precision : 2000, scale : 0) 7663626432206e6f74206e756c6c 7663626431206e6f74206e756c6c20627574207663626432206973 7663626432206e6f74206e756c6c20627574207663626431206973 null ",
                "COL1(datatype : VARCHAR () FOR BIT DATA, precision : 2000, scale : 0) 7663626432206e6f74206e756c6c 7663626431206e6f74206e756c6c20627574207663626432206973 7663626432206e6f74206e756c6c20627574207663626431206973 null ",
                "COL1(datatype : LONG VARCHAR FOR BIT DATA, precision : 32700, scale : 0) 63626431206e6f74206e756c6c20202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020 63626431206e6f74206e756c6c20627574206362643220697320202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020 6c7663626431206e6f74206e756c6c20616761696e null ",
                "COL1(datatype : LONG VARCHAR FOR BIT DATA, precision : 32700, scale : 0) 63626431206e6f74206e756c6c20202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020 63626431206e6f74206e756c6c20627574206362643220697320202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020 6c7663626431206e6f74206e756c6c20616761696e null ",
                "COL1(datatype : LONG VARCHAR FOR BIT DATA, precision : 32700, scale : 0) 6c7663626431206e6f74206e756c6c 63626431206e6f74206e756c6c20627574206362643220697320202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020 6c7663626431206e6f74206e756c6c20616761696e null ",
                "COL1(datatype : LONG VARCHAR FOR BIT DATA, precision : 32700, scale : 0) 6c7663626431206e6f74206e756c6c 63626431206e6f74206e756c6c20627574206362643220697320202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020 6c7663626431206e6f74206e756c6c20616761696e null ",
                "COL1(datatype : LONG VARCHAR FOR BIT DATA, precision : 32700, scale : 0) 7663626431206e6f74206e756c6c 7663626431206e6f74206e756c6c20627574207663626432206973 6c7663626431206e6f74206e756c6c20616761696e null ",
                "COL1(datatype : LONG VARCHAR FOR BIT DATA, precision : 32700, scale : 0) 7663626431206e6f74206e756c6c 7663626431206e6f74206e756c6c20627574207663626432206973 6c7663626431206e6f74206e756c6c20616761696e null ",
                "COL1(datatype : LONG VARCHAR FOR BIT DATA, precision : 32700, scale : 0) 6c7663626431206e6f74206e756c6c 7663626431206e6f74206e756c6c20627574207663626432206973 6c7663626431206e6f74206e756c6c20616761696e null ",
                "COL1(datatype : LONG VARCHAR FOR BIT DATA, precision : 32700, scale : 0) 6c7663626431206e6f74206e756c6c 7663626431206e6f74206e756c6c20627574207663626432206973 6c7663626431206e6f74206e756c6c20616761696e null ",
                "COL1(datatype : LONG VARCHAR FOR BIT DATA, precision : 32700, scale : 0) 6c7663626431206e6f74206e756c6c null 6c7663626431206e6f74206e756c6c20616761696e null ",
                "COL1(datatype : LONG VARCHAR FOR BIT DATA, precision : 32700, scale : 0) 6c7663626431206e6f74206e756c6c null 6c7663626431206e6f74206e756c6c20616761696e null ",
                "","","","","","","","","","","","",
                "COL1(datatype : BLOB, precision : 33792, scale : 0) 626c6f6231206e6f74206e756c6c null 626c6f6231206e6f74206e756c6c20616761696e null ",
                "COL1(datatype : BLOB, precision : 33792, scale : 0) 626c6f6231206e6f74206e756c6c null 626c6f6231206e6f74206e756c6c20616761696e null "
        };

        ps = prepareStatement("insert into tC values (?,?,?,?,?,?,?,?)");
        ps.setBytes(1, "cbd1 not null".getBytes("US-ASCII"));
        ps.setBytes(2, "cbd2 not null".getBytes("US-ASCII"));
        ps.setBytes(3, "vcbd1 not null".getBytes("US-ASCII"));
        ps.setBytes(4, "vcbd2 not null".getBytes("US-ASCII"));
        ps.setBytes(5, "lvcbd1 not null".getBytes("US-ASCII"));
        ps.setBytes(6, "lvcbd2 not null".getBytes("US-ASCII"));
        ps.setBytes(7, "blob1 not null".getBytes("US-ASCII"));
        ps.setBytes(8, "blob2 not null".getBytes("US-ASCII"));
        ps.executeUpdate();
        ps.setBytes(1, "cbd1 not null but cbd2 is".getBytes("US-ASCII"));
        ps.setBytes(2, null);
        ps.setBytes(3, "vcbd1 not null but vcbd2 is".getBytes("US-ASCII"));
        ps.setBytes(4, null);
        ps.setBytes(5, null);
        ps.setBytes(6, null);
        ps.setBytes(7, null);
        ps.setBytes(8, null);
        ps.executeUpdate();
        ps.setBytes(1, null);
        ps.setBytes(2, "cbd2 not null but cbd1 is".getBytes("US-ASCII"));
        ps.setBytes(3, null);
        ps.setBytes(4, "vcbd2 not null but vcbd1 is".getBytes("US-ASCII"));
        ps.setBytes(5, "lvcbd1 not null again".getBytes("US-ASCII"));
        ps.setBytes(6, "lvcbd2 not null again".getBytes("US-ASCII"));
        ps.setBytes(7, "blob1 not null again".getBytes("US-ASCII"));
        ps.setBytes(8, "blob2 not null again".getBytes("US-ASCII"));
        ps.executeUpdate();
        ps.setBytes(1, null);
        ps.setBytes(2, null);
        ps.setBytes(3, null);
        ps.setBytes(4, null);
        ps.setBytes(5, null);
        ps.setBytes(6, null);
        ps.setBytes(7, null);
        ps.setBytes(8, null);
        ps.executeUpdate();

        int index = 0;

        dumpRS(s.executeQuery("select coalesce(cbd1,cbd2) from tC"), expectedValues[index++]);
        dumpRS(s.executeQuery("select value(cbd1,cbd2) from tC"), expectedValues[index++]);
        dumpRS(s.executeQuery("select coalesce(cbd2,cbd1) from tC"), expectedValues[index++]);	
        dumpRS(s.executeQuery("select value(cbd2,cbd1) from tC"), expectedValues[index++]);
        dumpRS(s.executeQuery("select coalesce(cbd1,vcbd1) from tC"), expectedValues[index++]);
        dumpRS(s.executeQuery("select value(cbd1,vcbd1) from tC"), expectedValues[index++]);
        dumpRS(s.executeQuery("select coalesce(vcbd1,cbd1) from tC"), expectedValues[index++]);
        dumpRS(s.executeQuery("select value(vcbd1,cbd1) from tC"), expectedValues[index++]);
        dumpRS(s.executeQuery("select coalesce(vcbd1,vcbd2) from tC"), expectedValues[index++]);
        dumpRS(s.executeQuery("select value(vcbd1,vcbd2) from tC"), expectedValues[index++]);
        dumpRS(s.executeQuery("select coalesce(vcbd2,vcbd1) from tC"), expectedValues[index++]);
        dumpRS(s.executeQuery("select value(vcbd2,vcbd1) from tC"), expectedValues[index++]);
        dumpRS(s.executeQuery("select coalesce(cbd1,lvcbd1) from tC"), expectedValues[index++]);
        dumpRS(s.executeQuery("select value(cbd1,lvcbd1) from tC"), expectedValues[index++]);
        dumpRS(s.executeQuery("select coalesce(lvcbd1,cbd1) from tC"), expectedValues[index++]);
        dumpRS(s.executeQuery("select value(lvcbd1,cbd1) from tC"), expectedValues[index++]);
        dumpRS(s.executeQuery("select coalesce(vcbd1,lvcbd1) from tC"), expectedValues[index++]);
        dumpRS(s.executeQuery("select value(vcbd1,lvcbd1) from tC"), expectedValues[index++]);
        dumpRS(s.executeQuery("select coalesce(lvcbd1,vcbd1) from tC"), expectedValues[index++]);
        dumpRS(s.executeQuery("select value(lvcbd1,vcbd1) from tC"), expectedValues[index++]);
        dumpRS(s.executeQuery("select coalesce(lvcbd1,lvcbd2) from tC"), expectedValues[index++]);
        dumpRS(s.executeQuery("select value(lvcbd1,lvcbd2) from tC"), expectedValues[index++]);		

        try {		
            dumpRS(s.executeQuery("select coalesce(blob1,cbd1) from tC"), expectedValues[index]);
        } catch (SQLException sqle) {		
            assertSQLState("42815", sqle);
        }
        index++;

        try {
            dumpRS(s.executeQuery("select value(blob1,cbd1) from tC"), expectedValues[index]);
        } catch (SQLException sqle) {	
            assertSQLState("42815", sqle);
        }
        index++;

        try {
            dumpRS(s.executeQuery("select coalesce(cbd1,blob2) from tC"), expectedValues[index]);
        } catch (SQLException sqle) {	
            assertSQLState("42815", sqle);
        }
        index++;

        try {
            dumpRS(s.executeQuery("select value(cbd1,blob2) from tC"), expectedValues[index]);
        } catch (SQLException sqle) {	
            assertSQLState("42815", sqle);
        }
        index++;

        try {
            dumpRS(s.executeQuery("select coalesce(blob1,vcbd1) from tC"), expectedValues[index]);
        } catch (SQLException sqle) {	
            assertSQLState("42815", sqle);
        }
        index++;

        try {
            dumpRS(s.executeQuery("select value(blob1,vcbd1) from tC"), expectedValues[index]);
        } catch (SQLException sqle) {	
            assertSQLState("42815", sqle);
        }
        index++;

        try {
            dumpRS(s.executeQuery("select coalesce(vcbd2,blob2) from tC"), expectedValues[index]);
        } catch (SQLException sqle) {	
            assertSQLState("42815", sqle);
        }
        index++;

        try {
            dumpRS(s.executeQuery("select value(vcbd2,blob2) from tC"), expectedValues[index]);
        } catch (SQLException sqle) {	
            assertSQLState("42815", sqle);
        }
        index++;

        try {
            dumpRS(s.executeQuery("select coalesce(blob1,lvcbd1) from tC"), expectedValues[index]);
        } catch (SQLException sqle) {	
            assertSQLState("42815", sqle);
        }
        index++;

        try {
            dumpRS(s.executeQuery("select value(blob1,lvcbd1) from tC"), expectedValues[index]);
        } catch (SQLException sqle) {	
            assertSQLState("42815", sqle);
        }
        index++;

        try {
            dumpRS(s.executeQuery("select coalesce(lvcbd2,blob2) from tC"), expectedValues[index]);
        } catch (SQLException sqle) {	
            assertSQLState("42815", sqle);
        }
        index++;

        try {
            dumpRS(s.executeQuery("select value(lvcbd2,blob2) from tC"), expectedValues[index]);
        } catch (SQLException sqle) {	
            assertSQLState("42815", sqle);
        }
        index++;

        dumpRS(s.executeQuery("select coalesce(blob1,blob2) from tC"), expectedValues[index++]);
        dumpRS(s.executeQuery("select value(blob1,blob2) from tC"), expectedValues[index++]);	
    }


    public void testAggregateDerby2016() throws SQLException
    {
        String[] expectedValues = {
            "COL1(datatype : INTEGER, precision : 10, scale : 0) 2 ",
            "COL1(datatype : INTEGER, precision : 10, scale : 0) 55 ",
            "COL1(datatype : INTEGER, precision : 10, scale : 0) 1 ",
        };

        int index = 0;

        // let aggregate max return a non-null: should give 2
        ps = prepareStatement("insert into tAggr values ?");
        for (int i=0; i<3; i++) {
            ps.setInt(1, i);
            ps.executeUpdate();
        }

        dumpRS(s.executeQuery("select coalesce(max(i), 55) from tAggr"),
               expectedValues[index++]);

        s.executeUpdate("delete from tAggr");

        // let aggregate max return a null
        ps.setNull(1, Types.INTEGER);
        ps.executeUpdate();

        dumpRS(s.executeQuery("select coalesce(max(i), 55) from tAggr"),
               expectedValues[index++]);

        // two aggregates
        dumpRS(s.executeQuery(
                   "select coalesce(max(i), count(*), 55) from tAggr"),
               expectedValues[index++]);
    }

    /**
     * Regression test for DERBY-4342. A self-join with COALESCE in the WHERE
     * clause used to fail with a NullPointerException because
     * CoalesceFunctionNode didn't remap column references correctly.
     */
    public void testColumnRemappingDerby4342() throws SQLException {
        JDBC.assertSingleValueResultSet(s.executeQuery(
                "select t1.smallintcol from " +
                "AllDataTypesTable t1 join AllDataTypesTable t2 " +
                "on t1.smallintcol=t2.smallintcol where " +
                "coalesce(t1.smallintcol, t1.integercol) = 1"),
                "1");
    }

    /**************supporting methods *******************/
    private void dumpRS(ResultSet rs, String expectedValue) throws SQLException
    {
        if (rs == null) return;

        ResultSetMetaData rsmd = rs.getMetaData();
        int numCols = rsmd.getColumnCount();
        if (numCols <= 0) return;

        StringBuffer heading = new StringBuffer();	
        for (int i=1; i<=numCols; i++)
        {
            if (i > 1) heading.append(",");
            heading.append("COL"+i);
            heading.append("(datatype : " + rsmd.getColumnTypeName(i));
            heading.append(", precision : " + rsmd.getPrecision(i));
            heading.append(", scale : " + rsmd.getScale(i) + ")");

        }

        StringBuffer row = new StringBuffer();
        while (rs.next())
        {
            for (int i=1; i<=numCols; i++)
            {
                if (i > 1) row.append(",");
                row.append(rs.getString(i));
                row.append(" ");	
            }

        }

        String actualValue = heading.toString() + " " + row.toString();

        if ( usingDerbyNetClient() ) {
            if ( expectedValue.indexOf("()") != -1 ) {
                String actualValue2  = actualValue.replaceAll("CHAR", "CHAR ()");   
                actualValue = actualValue2;
            } 

        }
        assertEquals(expectedValue, actualValue);	
        rs.close();

    }

    public static boolean isClobWithCharAndDateTypeArguments(String coalesceString) throws Throwable
    {
        if(coalesceString.indexOf("CLOB") != -1)
        {
            if(coalesceString.indexOf("CHAR") != -1 && (coalesceString.indexOf("DATE") != -1 || coalesceString.indexOf("TIME") != -1))
                return true;
        }
        return false;
    }

    private static boolean isSupportedCoalesce(int oneType, int anotherType)
    {
        return (!(resultDataTypeRulesTable[oneType][anotherType].equals("ERROR")));
    }
}
