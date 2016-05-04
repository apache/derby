/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.compatibility.JDBCDriverTest

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
package org.apache.derbyTesting.functionTests.tests.compatibility;

import java.io.ByteArrayInputStream;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.HashMap;
import java.util.regex.Pattern;
import junit.framework.Test;
import org.apache.derbyTesting.functionTests.tests.compatibility.helpers.DummyBlob;
import org.apache.derbyTesting.functionTests.tests.compatibility.helpers.DummyClob;
import org.apache.derbyTesting.junit.BaseJDBCTestSetup;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.DerbyVersion;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * A set of client driver compatibility tests.
 */
public class JDBCDriverTest
    extends AbstractCompatibilityTest {
    /////////////////////////////////////////////////////////////
    //
    //    CONSTANTS
    //
    /////////////////////////////////////////////////////////////

    private    static    final        String    ALL_TYPES_TABLE = "allTypesTable";
    private    static    final        String    KEY_COLUMN = "keyCol";

    //
    // Data values to be stuffed into columns of ALL_TYPES_TABLE.
    //
    private    static    final        byte[]    SAMPLE_BYTES =
        new byte[] { (byte) 1, (byte) 2, (byte) 3, (byte) 4, (byte) 5 };
    private    static    final        String    SAMPLE_STRING = "hello";

    //
    // These funny constants are defined this way to make the salient
    // facts of the COERCIONS table leap out at you.
    //
    private    static    final        boolean    Y = true;
    private    static    final        boolean    n = false;

    //
    // This table declares the datatypes supported by Derby and the earliest
    // versions of the Derby client which support these datatypes.
    //
    // If you add a type to this table, make sure you add a corresponding
    // column to the following row table. Also add a corresponding row to the
    // COERCIONS table.
    //
    private    static    final    TypeDescriptor[]    ALL_TYPES =
    {
        // 10.0 types

        new TypeDescriptor
        ( Types.BIGINT,         "bigint",                   DerbyVersion._10_0),
        new TypeDescriptor
        ( Types.BLOB,           "blob",                     DerbyVersion._10_0),
        new TypeDescriptor
        ( Types.CHAR,           "char(5)",                  DerbyVersion._10_0),
        new TypeDescriptor
        ( Types.BINARY,         "char(5) for bit data",     DerbyVersion._10_0),
        new TypeDescriptor
        ( Types.CLOB,           "clob",                     DerbyVersion._10_0),
        new TypeDescriptor
        ( Types.DATE,           "date",                     DerbyVersion._10_0),
        new TypeDescriptor
        ( Types.DECIMAL,        "decimal",                  DerbyVersion._10_0),
        new TypeDescriptor
        ( Types.DOUBLE,         "double",                   DerbyVersion._10_0),
        new TypeDescriptor
        ( Types.DOUBLE,         "double precision",         DerbyVersion._10_0),
        new TypeDescriptor
        ( Types.REAL,           "float(23)",                DerbyVersion._10_0),
        new TypeDescriptor
        ( Types.DOUBLE,         "float",                    DerbyVersion._10_0),
        new TypeDescriptor
        ( Types.INTEGER,        "integer",                  DerbyVersion._10_0),
        new TypeDescriptor
        ( Types.LONGVARCHAR,    "long varchar",             DerbyVersion._10_0),
        new TypeDescriptor
        ( Types.LONGVARBINARY,  "long varchar for bit data",DerbyVersion._10_0),
        new TypeDescriptor
        ( Types.NUMERIC,        "numeric",                  DerbyVersion._10_0),
        new TypeDescriptor
        ( Types.REAL,           "real",                     DerbyVersion._10_0),
        new TypeDescriptor
        ( Types.SMALLINT,       "smallint",                 DerbyVersion._10_0),
        new TypeDescriptor
        ( Types.TIME,           "time",                     DerbyVersion._10_0),
        new TypeDescriptor
        ( Types.TIMESTAMP,      "timestamp",                DerbyVersion._10_0),
        new TypeDescriptor
        ( Types.VARCHAR,        "varchar(5)",               DerbyVersion._10_0),
        new TypeDescriptor
        ( Types.VARBINARY,      "varchar(5) for bit data",  DerbyVersion._10_0),
    };

    //
    // This table needs to have the same number of entries as ALL_TYPES.
    // The testSanity() test case enforces this at run time.
    //
    private    static    final    Object[]    ROW_1 =
    {
        // 10.0 columns

        1L,
        new DummyBlob( SAMPLE_BYTES ),
        SAMPLE_STRING,
        SAMPLE_BYTES,
        new DummyClob( SAMPLE_STRING ),
        new java.sql.Date( 1L ),
        new BigDecimal( 1.0 ),
        1.0,
        1.0,
        (float) 1.0,
        1.0,
        1,
        SAMPLE_STRING,
        SAMPLE_BYTES,
        new BigDecimal( 1.0 ),
        (float) 1.0,
        (short) 1,
        new Time( 1L ),
        new Timestamp( 1L ),
        SAMPLE_STRING,
        SAMPLE_BYTES,
    };

    //
    // This table needs to have the same number of rows as ALL_TYPES.
    // Each row in this table needs to have the same number of columns as
    // rows in ALL_TYPES. The testSanity() test case enforces this at run time.
    // Note how the funny synonyms for true and false
    // make the salient facts of this table leap out at you.
    //
    // The ugly class name T_CN is an abbreviation which makes it possible to
    // squeeze this table onto a readable screen.
    //
    // Please read the introductory comment top-to-bottom. 'Y' means a coercion
    // is legal; '_' means it isn't.
    //
    private    static    final    T_CN[]    COERCIONS =
    {
        //                                                  B|B|C|B|C|D|D|D|R|I|L|L|N|R|S|T|T|V|V
        //                                                  I|L|H|I|L|A|E|O|E|N|O|O|U|E|M|I|I|A|A
        //                                                  G|O|A|N|O|T|C|U|A|T|N|N|M|A|A|M|M|R|R
        //                                                  I|B|R|A|B|E|I|B|L|E|G|G|E|L|L|E|E|C|B
        //                                                  N|-|-|R|-|-|M|L|-|G|V|V|R|-|L|-|S|H|I
        //                                                  T|-|-|Y|-|-|A|E|-|E|A|A|I|-|I|-|T|A|N
        //                                                  -|-|-|-|-|-|L|-|-|R|R|R|C|-|N|-|A|R|A
        //                                                  -|-|-|-|-|-|-|-|-|-|C|B|-|-|T|-|M|-|R
        //                                                  -|-|-|-|-|-|-|-|-|-|H|I|-|-|-|-|P|-|Y
        //                                                  -|-|-|-|-|-|-|-|-|-|A|N|-|-|-|-|-|-|-
        //                                                  -|-|-|-|-|-|-|-|-|-|R|A|-|-|-|-|-|-|-
        //                                                  -|-|-|-|-|-|-|-|-|-|-|R|-|-|-|-|-|-|-
        //                                                  -|-|-|-|-|-|-|-|-|-|-|Y|-|-|-|-|-|-|-
        new T_CN( Types.BIGINT, new boolean[]             { Y,n,Y,n,n,n,n,Y,Y,Y,Y,n,Y,Y,Y,n,n,Y,n } ),
        new T_CN( Types.BLOB, new boolean[]               { n,Y,n,n,n,n,n,n,n,n,n,n,n,n,n,n,n,n,n } ),
        new T_CN( Types.CHAR, new boolean[]               { n,n,Y,n,n,n,n,n,n,n,Y,n,n,n,n,n,n,Y,n } ),
        new T_CN( Types.BINARY, new boolean[]             { n,n,n,Y,n,n,n,n,n,n,n,Y,n,n,n,n,n,n,Y } ),
        new T_CN( Types.CLOB, new boolean[]               { n,n,n,n,Y,n,n,n,n,n,n,n,n,n,n,n,n,n,n } ),
        new T_CN( Types.DATE, new boolean[]               { n,n,n,n,n,Y,n,n,n,n,n,n,n,n,n,n,n,n,n } ),
        new T_CN( Types.DECIMAL, new boolean[]            { Y,n,n,n,n,n,Y,Y,Y,Y,Y,n,Y,Y,Y,n,n,Y,n } ),
        new T_CN( Types.DOUBLE, new boolean[]             { Y,n,n,n,n,n,Y,Y,Y,Y,Y,n,Y,Y,Y,n,n,Y,n } ),
        new T_CN( Types.REAL, new boolean[]               { Y,n,Y,n,n,n,Y,Y,Y,Y,Y,n,Y,Y,Y,n,n,Y,n } ),
        new T_CN( Types.INTEGER, new boolean[]            { Y,n,Y,n,n,n,Y,Y,Y,Y,Y,n,Y,Y,Y,n,n,Y,n } ),
        new T_CN( Types.LONGVARCHAR, new boolean[]        { n,n,Y,n,n,n,n,n,n,n,Y,n,n,n,n,n,n,Y,n } ),
        new T_CN( Types.LONGVARBINARY, new boolean[]      { n,n,n,n,n,n,n,n,n,n,n,Y,n,n,n,n,n,n,Y } ),
        new T_CN( Types.NUMERIC, new boolean[]            { Y,n,Y,n,n,n,Y,Y,Y,Y,Y,n,Y,Y,Y,n,n,Y,n } ),
        new T_CN( Types.REAL, new boolean[]               { Y,n,Y,n,n,n,Y,Y,Y,Y,Y,n,Y,Y,Y,n,n,Y,n } ),
        new T_CN( Types.SMALLINT, new boolean[]           { Y,n,Y,n,n,n,Y,Y,Y,Y,Y,n,Y,Y,Y,n,n,Y,n } ),
        new T_CN( Types.TIME, new boolean[]               { n,n,n,n,n,n,n,n,n,n,n,n,n,n,n,Y,n,n,n } ),
        new T_CN( Types.TIMESTAMP, new boolean[]          { n,n,n,n,n,n,n,n,n,n,n,n,n,n,n,n,Y,n,n } ),
        new T_CN( Types.VARCHAR, new boolean[]            { n,n,Y,n,n,n,n,n,n,n,Y,n,n,n,n,n,n,Y,n } ),
        new T_CN( Types.VARBINARY, new boolean[]          { n,n,n,n,n,n,n,n,n,n,n,Y,n,n,n,n,n,n,Y } ),
    };

    /////////////////////////////////////////////////////////////
    //
    //    STATE
    //
    /////////////////////////////////////////////////////////////

    // map derby type name to type descriptor
    private    static    HashMap<String,TypeDescriptor>        _types = new HashMap<String,TypeDescriptor>();

    // map jdbc type to index into COERCIONS
    private    static    HashMap<Integer,Integer>        _coercionIndex = new HashMap<Integer,Integer>();

    /////////////////////////////////////////////////////////////
    //
    //    CONSTRUCTOR
    //
    /////////////////////////////////////////////////////////////

    public JDBCDriverTest(String name) {
        super(name);
    }

    /////////////////////////////////////////////////////////////
    //
    //    TEST ENTRY POINTS
    //
    /////////////////////////////////////////////////////////////

    /**
     * Sanity check the integrity of this test suite.
     */
    public void testSanity()
    {
        assertEquals("ALL_TYPES.length == ROW_1.length",
                ALL_TYPES.length, ROW_1.length );

        // make sure we completely describe the coercibility of every jdbc type
        int coercionCount = COERCIONS.length;
        for ( int i = 0; i < coercionCount; i++ ) {
            assertEquals("Coercion " + i,
                    coercionCount, COERCIONS[ i ].getCoercions().length );
        }
    }

    public void testVerifyVersions()
            throws SQLException {
        DerbyVersion server = getServerVersion();
        DerbyVersion client = getDriverVersion();
        println("server=" + server.toString() + " <-> client=" +
                client.toString());
        String expS = getSystemProperty("derby.tests.compat.expectedServer");
        String expC = getSystemProperty("derby.tests.compat.expectedClient");
        assertNotNull("expected server property missing", expS);
        assertNotNull("expected client property missing", expC);
        DerbyVersion expectedServer = DerbyVersion.parseVersionString(expS);
        DerbyVersion expectedClient = DerbyVersion.parseVersionString(expC);
        assertEquals("server version mismatch", expectedServer, server);
        assertEquals("client version mismatch", expectedClient, client);
    }

    /**
     * Tests compatibility for the available data types.
     */
    public void testDataTypesCompatibility() throws SQLException {
        datatypesTest();
    }

    /////////////////////////////////////////////////////////////
    //
    //    DERBY-4613
    //
    // Make sure embedded and network clients treat BOOLEAN values identically.
    //
    /////////////////////////////////////////////////////////////

    /**
     * Verify that embedded and network clients handle BOOLEAN values the
     * same way from release 10.7 onward.
     */
    public void testDerby4613(Connection conn)
        throws Exception
    {
        boolean correctBehavior =
                getServerVersion().atLeast( DerbyVersion._10_7 ) &&
                getDriverVersion().atLeast( DerbyVersion._10_7 );
        println( "derby_4613_test correctBehavior = " + correctBehavior );

        vet_isindex_column(correctBehavior, "SYSTABLES_HEAP", false);
        vet_isindex_column(correctBehavior, "SYSTABLES_INDEX1", true);
    }

    /**
     * Vet boolean results.
     */
    private void vet_isindex_column(boolean correctBehavior,
                                    String conglomerateName,
                                    boolean expectedValue)
            throws Exception
    {
        PreparedStatement ps = prepareStatement(
                "select isindex from sys.sysconglomerates " +
                "where conglomeratename = ?");
        ps.setString( 1, conglomerateName );
        ResultSet rs = ps.executeQuery();
        ResultSetMetaData rsmd = rs.getMetaData();

        int jdbcType = correctBehavior ? Types.BOOLEAN : Types.SMALLINT;
        String typeName = correctBehavior ? "BOOLEAN" : "SMALLINT";
        int precision = correctBehavior ? 1 : 5;
        int scale = 0;
        int columnDisplaySize = correctBehavior ? 5 : 6;
        String columnClassName = correctBehavior ? "java.lang.Boolean"
                                                 : "java.lang.Integer";

        Object objectValue;
        if ( correctBehavior )
        {
            objectValue = Boolean.valueOf(expectedValue);
        }
        else
        {
            objectValue = expectedValue ? 1 : 0;
        }
        String stringValue = objectValue.toString();

        assertEquals( jdbcType, rsmd.getColumnType( 1 ) );
        assertEquals( typeName, rsmd.getColumnTypeName( 1 ) );
        assertEquals( precision, rsmd.getPrecision( 1 ) );
        assertEquals( scale, rsmd.getScale( 1 ) );
        assertEquals( columnDisplaySize, rsmd.getColumnDisplaySize( 1 ) );
        assertEquals( columnClassName, rsmd.getColumnClassName( 1 ) );

        assertEquals( true, rs.next() );

        assertEquals( expectedValue, rs.getBoolean( 1 ) );
        assertEquals( objectValue, rs.getObject( 1 ) );
        assertEquals( stringValue, rs.getString( 1 ) );

        rs.close();
        ps.close();
    }

    /////////////////////////////////////////////////////////////
    //
    //    DERBY-2602
    //
    /////////////////////////////////////////////////////////////

    /**
     * Verifies that timestamps retain their nanosecond-precision
     * across the network from release 10.6 onward.
     */
    public void testDerby2602() throws SQLException {
        //
        // We must expect truncation of timestamps in a network configuration
        // unless both the client and the server are at 10.6 or higher.
        // See DERBY-2602.
        //
        boolean correctBehavior =
                getServerVersion().atLeast( DerbyVersion._10_6 ) &&
                getDriverVersion().atLeast( DerbyVersion._10_6 );

        Timestamp ts = Timestamp.valueOf("2004-02-14 17:14:24.976255123");
        PreparedStatement    insert = prepareStatement(
                "insert into t_2602( a ) values ( ? )");
        insert.setTimestamp(1,ts);
        insert.executeUpdate();
        insert.close();

        PreparedStatement    select = prepareStatement("select a from t_2602" );
        ResultSet selectRS = select.executeQuery();
        selectRS.next();
        Timestamp resultTS = selectRS.getTimestamp( 1 );
        int resultNanos = resultTS.getNanos();

        int expectedResult = correctBehavior ? 976255123  : 976255000;
        assertEquals( expectedResult, resultNanos );
    }

    /**
     * Test case for DERBY-4888. Check that we can call DatabaseMetaData
     * methods returning a boolean without errors.
     */
    public void testDerby4888() throws SQLException {
        // Used to get a ClassCastException here in some combinations.
        assertFalse(getConnection().getMetaData().storesLowerCaseIdentifiers());
    }

    /**
     * A pattern that matches the value returned by
     * DatabaseMetaData.getDriverVersion() for the versions that suffer
     * from DERBY-5449. That is, all version on the 10.8 branch up to 10.8.2.2.
     */
    private static Pattern DERBY_5449_PATTERN = Pattern.compile(
            "^10\\.8\\.([01]\\.|2\\.[012] ).*");

    /**
     * Test case for DERBY-5449. Verify that PreparedStatement.setBoolean()
     * works across different versions. Used to fail with a ClassCastException
     * when talking to servers at version 10.7 and earlier.
     */
    public void testDerby5449() throws SQLException {
        if (getServerVersion().compareTo(DerbyVersion._10_7) <= 0) {
            // Derby's client drivers on the 10.8 branch up to 10.8.2.2
            // suffered from DERBY-5449 and the test case will fail when
            // talking to older servers. Skip the test case in such cases.
            String driverVersion =
                    getConnection().getMetaData().getDriverVersion();
            if (DERBY_5449_PATTERN.matcher(driverVersion).matches()) {
                return;
            }
        }

        PreparedStatement ps = prepareStatement("VALUES CAST(? AS INTEGER)");
        ps.setBoolean(1, true);
        JDBC.assertSingleValueResultSet(ps.executeQuery(), "1");
        ps.setBoolean(1, false);
        JDBC.assertSingleValueResultSet(ps.executeQuery(), "0");
    }

    /////////////////////////////////////////////////////////////
    //
    //    TEST UDTs
    //
    /////////////////////////////////////////////////////////////

    /**
     * Verify the metadata for user defined types.
     */
    public void testUDT() throws Exception
    {
        //
        // We must expect the wrong metadata in a network configuration
        // unless both the client and the server are at 10.6 or higher.
        // See DERBY-4491.
        //
        boolean correctBehavior =
             getServerVersion().atLeast( DerbyVersion._10_6 ) &&
             getDriverVersion().atLeast( DerbyVersion._10_6 );

        String query = "select aliasinfo from sys.sysaliases";

        if ( correctBehavior )
        {
            String aliasInfoClassName = "org.apache.derby.catalog.AliasInfo";

            checkRSMD
                (
                 query,
                 aliasInfoClassName,
                 15,
                 java.sql.Types.JAVA_OBJECT,
                 aliasInfoClassName,
                 0,
                 0
                 );
        }
        else
        {
            checkRSMD
                (
                 query,
                 "byte[]",
                 65400,
                 java.sql.Types.LONGVARBINARY,
                 "LONG VARCHAR FOR BIT DATA",
                 32700,
                 0
                 );
        }


        if ( serverSupportsUDTs() )
        {
            query = "select a from t_price";
            PreparedStatement ps = prepareStatement( query );
            ResultSet rs = ps.executeQuery();
            rs.next();
            Object price = rs.getObject( 1 );
            String actualClassName = price.getClass().getName();
            rs.close();
            ps.close();

            if ( correctBehavior )
            {
                String priceClassName = "org.apache.derbyTesting.functionTests.tests.lang.Price";
                checkRSMD
                    (
                     query,
                     priceClassName,
                     15,
                     java.sql.Types.JAVA_OBJECT,
                     "\"APP\".\"PRICE\"",
                     0,
                     0
                     );

                assertEquals( priceClassName, actualClassName );
            }
            else
            {
                checkRSMD
                    (
                     query,
                     "byte[]",
                     65400,
                     java.sql.Types.LONGVARBINARY,
                     "LONG VARCHAR FOR BIT DATA",
                     32700,
                     0
                     );

                assertEquals( "java.lang.String", actualClassName );
            }

            query = "insert into t_price( a ) values ( ? )";

            if ( correctBehavior )
            {
                checkPMD
                    (
                     query,
                     "org.apache.derbyTesting.functionTests.tests.lang.Price",
                     java.sql.Types.JAVA_OBJECT,
                     "\"APP\".\"PRICE\"",
                     0,
                     0
                     );
            }
            else
            {
                checkPMD
                    (
                     query,
                     "byte[]",
                     java.sql.Types.LONGVARBINARY,
                     "LONG VARCHAR FOR BIT DATA",
                     32700,
                     0
                     );
            }

            //
            // Should only be able to stuff an object into the column if both
            // the client and server are at 10.6 or higher.
            //
            ps = prepareStatement( query );
            byte[] someBytes = new byte[] { (byte) 1, (byte) 2, (byte) 3 };
            ByteArrayInputStream bais = new ByteArrayInputStream( someBytes );

            try {
                ps.setObject( 1, org.apache.derbyTesting.functionTests.tests.lang.Price.makePrice() );
                ps.executeUpdate();

                if ( !correctBehavior ) {
                    fail( "setObject( Price ) unexpectedly worked." );
                }
            }
            catch (SQLException se)
            {
                if ( correctBehavior ) {
                    fail( "setObject( Price ) unexpectedly failed." );
                }
            }
            try {
                ps.setObject( 1, someBytes );
                ps.executeUpdate();

                fail( "setObject( byte[] ) unexpectedly worked." );
            }
            catch (SQLException se) {}
            try {
                ps.setBytes( 1, someBytes );
                ps.executeUpdate();

                fail( "setBytes( byte[] ) unexpectedly worked." );
            }
            catch (SQLException se) {}
            try {
                ps.setBinaryStream( 1, bais, 3 );
                ps.executeUpdate();

                fail( "setBinaryStream( InputStream ) unexpectedly worked." );
            }
            catch (SQLException se) {}

            ps.close();
        }
    }

    /**
     * Check the ResultSetMetaData for a query whose first column is a UDT.
     */
    private void checkRSMD
        (
         String query,
         String expectedClassName,
         int expectedDisplaySize,
         int expectedJDBCType,
         String expectedSQLTypeName,
         int expectedPrecision,
         int expectedScale
         ) throws SQLException
    {
        PreparedStatement ps = prepareStatement( query );
        ResultSet rs = ps.executeQuery();
        ResultSetMetaData rsmd = rs.getMetaData();

        assertEquals( expectedClassName, rsmd.getColumnClassName( 1 ) );
        assertEquals( expectedDisplaySize , rsmd.getColumnDisplaySize( 1 ));
        assertEquals( expectedJDBCType, rsmd.getColumnType( 1 ) );
        assertEquals( expectedSQLTypeName, rsmd.getColumnTypeName( 1 ) );
        assertEquals( expectedPrecision, rsmd.getPrecision( 1 ) );
        assertEquals( expectedScale, rsmd.getScale( 1 ) );

        rs.close();
        ps.close();
    }

    /**
     * Check the ParameterMetaData for a statement whose first parameter is a UDT.
     */
    private void checkPMD
        (
         String query,
         String expectedClassName,
         int expectedJDBCType,
         String expectedSQLTypeName,
         int expectedPrecision,
         int expectedScale
         ) throws SQLException
    {
        PreparedStatement ps = prepareStatement( query );
        ParameterMetaData pmd = ps.getParameterMetaData();

        assertEquals( pmd.getParameterClassName( 1 ), expectedClassName );
        assertEquals( pmd.getParameterType( 1 ), expectedJDBCType );
        assertEquals( pmd.getParameterTypeName( 1 ), expectedSQLTypeName );
        assertEquals( pmd.getPrecision( 1 ), expectedPrecision );
        assertEquals( pmd.getScale( 1 ), expectedScale );

        ps.close();
    }

    /////////////////////////////////////////////////////////////
    //
    //    TEST DATATYPES
    //
    /////////////////////////////////////////////////////////////

    //
    // Test that we can declare, insert, and select all datatypes that
    // are legal on the server. Test the metadata for these datatypes.
    //
    private void datatypesTest()
            throws SQLException {
        TypeDescriptor[]    types = ALL_TYPES;
        String                tableName = ALL_TYPES_TABLE;
        Object[][]            rows = new Object[][] { makeNullRow( types.length ), ROW_1 };

        checkDBMetadata(tableName);
        stuffTable(tableName, types, rows);
        readTable(tableName, types, rows);
    }

    //
    // Verify that we get the correct DatabaseMetaData for a table.
    //
    private void checkDBMetadata(String tableName)
        throws SQLException {
        String defaultUser = TestConfiguration.getCurrent().getUserName();
        String                normalizedSchema = defaultUser.toUpperCase();
        String                normalizedTable = tableName.toUpperCase();
        DatabaseMetaData    dbmd = getConnection().getMetaData();

        ResultSet            rs = dbmd.getColumns
            ( null, normalizedSchema, normalizedTable, "%" );

        println( "Pawing through database metadata for " + normalizedSchema + '.' + normalizedTable );

        while( rs.next() )
        {
            String            columnName = rs.getString( "COLUMN_NAME" );
            int                actualJdbcType = rs.getInt( "DATA_TYPE" );
            TypeDescriptor    typeDesc = getType( columnName );

            if ( columnName.equals( KEY_COLUMN ) ) { continue; }

            StringBuilder builder = new StringBuilder();

            builder.append( "[ " ).
                   append( rs.getString( "COLUMN_NAME" ) ).
                   append( ",\t" ).
                   append( "type( " ).
                   append(rs.getInt( "DATA_TYPE" )).
                   append( " ),\t" ).
                   append( rs.getString( "TYPE_NAME" ) ).
                   append( " ]" );

            println( builder.toString() );

            assertEquals( columnName, typeDesc.getJdbcType(), actualJdbcType );
        }

        rs.close();
    }

    //
    // Stuff a table with rows
    //
    private void stuffTable(String tableName, TypeDescriptor[] types,
                            Object[][] rows)
            throws SQLException {
        PreparedStatement    ps = makeInsert(tableName, types);
        int                    rowCount = rows.length;

        for ( int i = 0; i < rowCount; i++ )
        {
            setRow( ps, i + 1, types, rows[ i ] );
        }

        ps.close();
    }

    private    PreparedStatement    makeInsert(String tableName,
                                           TypeDescriptor[] types )
            throws SQLException {
        StringBuilder      masterBuilder = new StringBuilder();
        StringBuilder      columnBuilder = new StringBuilder();
        StringBuilder      valuesBuilder = new StringBuilder();
        int                columnNumber = 0;
        int                valuesNumber = 0;
        int                typeCount = types.length;

        beginColumnList( columnBuilder );
        beginColumnList( valuesBuilder );

        addColumn( columnBuilder, columnNumber++, doubleQuote( KEY_COLUMN ) );
        addColumn( valuesBuilder, valuesNumber++, "?" );

        for ( int i = 0; i < typeCount; i++ )
        {
            TypeDescriptor    type = types[ i ];

            if ( getServerVersion().atLeast( type.getDerbyVersion() ) )
            {
                String    typeName = type.getDerbyTypeName();
                String    columnDesc = doubleQuote( typeName );

                addColumn( columnBuilder, columnNumber++, columnDesc );
                addColumn( valuesBuilder, valuesNumber++, "?" );
            }
        }

        endColumnList( columnBuilder );
        endColumnList( valuesBuilder );

        masterBuilder.append( "insert into " ).
                     append( tableName ).
                     append( "\n" ).
                     append( columnBuilder.toString() ).
                     append( "values\n" ).
                     append( valuesBuilder.toString() );

        return prepareStatement(masterBuilder.toString());
    }

    //
    // Verify that we can select all legal datatypes in a table.
    //
    private void readTable(String tableName, TypeDescriptor[] types,
                           Object[][] rows)
            throws SQLException {
        PreparedStatement    ps = readTableQuery(tableName, types);
        ResultSet            rs = ps.executeQuery();

        checkRSMD( rs );
        rs.close();
        // Execute the statement again for each cast / coercion we check.
        checkRows( ps, types, rows );

        ps.close();
    }

    //
    // Make the select query
    //
    private PreparedStatement readTableQuery(String tableName,
                                             TypeDescriptor[] types)
            throws SQLException {
        StringBuilder    builder = new StringBuilder();
        int                columnNumber = 0;
        int                typeCount = types.length;

        builder.append( "select \n" );

        addColumn( builder, columnNumber++, doubleQuote( KEY_COLUMN ) );

        for ( int i = 0; i < typeCount; i++ )
        {
            TypeDescriptor    type = types[ i ];

            if ( getServerVersion().atLeast( type.getDerbyVersion() ) )
            {
                String    typeName = type.getDerbyTypeName();
                String    columnDesc = doubleQuote( typeName );

                addColumn( builder, columnNumber++, columnDesc );
            }
        }

        builder.append( "\nfrom " ).
               append( tableName ).
               append( "\n" ).
               append( "order by " ).
               append(doubleQuote( KEY_COLUMN ) );

        return prepareStatement(builder.toString());
    }

    //
    // Verify that we get the correct ResultSetMetaData for all datatypes
    // which are legal on the server.
    //
    private    void    checkRSMD( ResultSet rs )
        throws SQLException
    {
        ResultSetMetaData    rsmd = rs.getMetaData();
        int                    columnCount = rsmd.getColumnCount();
        int                    firstTastyColumn = 0;

        println( "ResultSetMetaData:\n" );

        firstTastyColumn++;                // skip uninteresting key column

        for ( int i = firstTastyColumn; i < columnCount; i++ )
        {
            StringBuilder        builder = new StringBuilder();
            int                columnID = i + 1;
            String            columnName = rsmd.getColumnName( columnID );
            TypeDescriptor    typeDesc = getType( columnName );
            int                expectedType = rsmdTypeKludge( typeDesc.getJdbcType() );
            int                actualType = rsmd.getColumnType( columnID );

            builder.append( "[ " );
            builder.append( columnName );
            builder.append( ", type( " );
            builder.append( actualType );
            builder.append( " ), " );
            builder.append( rsmd.getColumnTypeName( columnID ) );
            builder.append( " ]\n" );

            println( builder.toString() );

            assertEquals( columnName, expectedType, actualType );
        }

    }

    /**
     * Verify that we select the values we originally inserted into a table,
     * and that the valid coercions succeeds.
     *
     *
     * @param ps the query used to obtain the results
     * @param types the type descriptions of the columns
     * @param rows the values expected to be returned
     * @throws Exception
     */
    private void checkRows(PreparedStatement ps, TypeDescriptor[] types,
                           Object[][] rows)
            throws SQLException {
        int typeCount = types.length;

        // Iterate over all the types we have defined.
        // Note that we don't iterate over the rows, as restrictions in
        // Derby stop us from getting the values of certain column types more
        // than once (see comments / patch for DERBY-3844).
        //We execute the query to obtain the rows many times.
        for (int colIndex=0; colIndex < typeCount; colIndex++ ) {
            TypeDescriptor type = types[colIndex];

            if (getServerVersion().atLeast(type.getDerbyVersion())) {
                // Make sure we're using the correct type descriptor.
                assertEquals(types[colIndex], type);
                checkPlainGet(ps, colIndex, type, rows);
                checkCoercions(ps, type);
            }
        }
    }

    /**
     * Checks that fetching the specified column as the declared data type
     * works, i.e doing rs.getString() on a VARCHAR column or rs.getInt() on
     * a SMALLINT column.
     *
     * @param ps the query used to obtain the results
     * @param columnIndex the index of the column to check
     * @param type the type description of the column
     * @param rows the values expected to be returned
     * @throws Exception
     */
    private void checkPlainGet(PreparedStatement ps, int columnIndex,
                               TypeDescriptor type, Object[][] rows)
            throws SQLException {
        String columnName = type.getDerbyTypeName();
        ResultSet rs = ps.executeQuery();
        for (int rowId=0; rowId < rows.length; rowId++) {
            assertTrue("Not enough rows in the result", rs.next());
            Object expectedValue = rows[rowId][columnIndex];
            Object actualValue = getColumn(rs, columnName, type);

            println("Comparing column " + columnName + ": " + expectedValue +
                    " to " + actualValue );
            compareObjects(columnName, expectedValue, actualValue);
        }
        // Make sure we drained the result set.
        assertFalse("Remaining rows in result", rs.next());
        rs.close();
    }

    /**
     * Verify all legal JDBC coercions of a data value.
     *
     * @param ps the query used to obtain the rows
     * @param type the type description of the column
     */
    private void checkCoercions(PreparedStatement ps, TypeDescriptor type)
            throws SQLException {
        String columnName = type.getDerbyTypeName();
        T_CN coercionDesc = COERCIONS[ getCoercionIndex(type.getJdbcType()) ];
        boolean[] coercions = coercionDesc.getCoercions();
        int count = coercions.length;
        int legalCoercions = 0;

        println( "Checking coercions for " + columnName );

        for ( int i=0; i < count; i++ ) {
            if (coercions[i]) {
                legalCoercions++;
                ResultSet rs = ps.executeQuery();

                while (rs.next()) {
                    int jdbcType = COERCIONS[i].getJdbcType();
                    Object retval = getColumn( rs, columnName, jdbcType );
                    println( "\t" + jdbcType + ":\t" + retval );
                }
                rs.close();
            }
        }
        println(legalCoercions + " legal coercions for " + columnName + " (" +
                "type=" + type.getDerbyTypeName() + ")");

        // finally, try getObject()
        ResultSet rs = ps.executeQuery();

        while (rs.next()) {
            Object objval = rs.getObject( columnName );
            if (objval == null) {
                println("\tgetObject() = null");
            } else {
                StringBuilder builder = new StringBuilder();
                builder.append("\tgetObject() = ");
                builder.append(objval.getClass().getName());
                builder.append("( ");
                builder.append(objval);
                builder.append(" )");
                println(builder.toString());
            }
        }
        rs.close();
    }

    //
    // This kludge compensates for the fact that the DRDA clients report
    // that NUMERIC columns are DECIMAL. See bug 584.
    //
    // In addition, booleans are handled oddly by down-rev clients.
    //
    private    int    rsmdTypeKludge( int originalJDbcType )
    {
        // The embedded client does the right thing.
        if ( usingEmbedded()) { return originalJDbcType; }

        switch( originalJDbcType )
        {
            //This kludge compensates for the fact that the DRDA clients report
            // that NUMERIC columns are DECIMAL. See bug 584.
            case Types.NUMERIC:
                return Types.DECIMAL;

            default:            return originalJDbcType;
        }
    }

    //
    // Insert a row into the ALL_TYPES table. The row contains all datatypes
    // that are legal on the server.
    //
    private    void    setRow( PreparedStatement ps, int keyValue, TypeDescriptor[] types, Object[] row )
        throws SQLException
    {
        int                param = 1;
        int                typeCount = types.length;

        ps.setInt( param++, keyValue );

        for ( int i = 0; i < typeCount; i++ )
        {
            TypeDescriptor    type = types[ i ];
            Object            value = row[ i ];

            if ( getServerVersion().atLeast( type.getDerbyVersion() ) )
            {
                setParameter( ps, param++, type, value );
            }
        }

        ps.execute();
    }

    private    Object[]    makeNullRow( int rowLength )
    {
        return new Object[ rowLength ];
    }

    //
    // Index the TypeDescriptors by Derby type name.
    //
    private    void    buildTypeMap()
    {
        int                typeCount = ALL_TYPES.length;

        for ( int i = 0; i < typeCount; i++ ) { putType( ALL_TYPES[ i ] ); }
    }
    private    void    putType( TypeDescriptor type )
    {
        _types.put( type.getDerbyTypeName(), type );
    }

    //
    // Lookup TypeDescriptors by Derby type name.
    //
    private    TypeDescriptor    getType( String typeName )
    {
        if ( _types.isEmpty() ) { buildTypeMap(); }

        return (TypeDescriptor) _types.get( typeName );
    }

    //
    // Index legal coercions by jdbc type.
    //
    private    void    buildCoercionMap()
    {
        int                count = COERCIONS.length;

        for ( int i = 0; i < count; i++ ) { putCoercionIndex( i ); }
    }
    private    void    putCoercionIndex( int index )
    {
        _coercionIndex.put( COERCIONS[ index ].getJdbcType(), index );
    }

    //
    // Lookup the legal coercions for a given jdbc type.
    //
    private    int    getCoercionIndex( int jdbcType )
    {
        if ( _coercionIndex.isEmpty() ) { buildCoercionMap(); }

        return ((Integer) _coercionIndex.get( jdbcType )).intValue();
    }

    /////////////////////////////////////////////////////////////
    //
    //    MINIONS
    //
    /////////////////////////////////////////////////////////////

    ///////////////////
    //
    //    TYPE MANAGEMENT
    //
    ///////////////////

    //////////////////
    //
    //    SCHEMA MINIONS
    //
    //////////////////

    //
    // Create a table modelling an array of datatypes.
    //
    private static void createTable(Connection con, String tableName,
                                    TypeDescriptor[] types)
            throws SQLException {
        StringBuilder    builder = new StringBuilder();
        int                columnNumber = 0;
        int                typeCount = types.length;

        builder.append("create table ").append(tableName).append('\n');
        beginColumnList( builder );

        addColumn( builder, columnNumber++, doubleQuote( KEY_COLUMN ) + "\tint" );

        for ( int i = 0; i < typeCount; i++ )
        {
            TypeDescriptor    type = types[ i ];

            if ( getServerVersion(con).atLeast( type.getDerbyVersion() ) )
            {
                String    typeName = type.getDerbyTypeName();
                String    columnDesc = doubleQuote( typeName ) + '\t' + typeName;

                addColumn( builder, columnNumber++, columnDesc );
            }
        }

        endColumnList( builder );

        PreparedStatement ps = con.prepareStatement(builder.toString());
        ps.execute();
        ps.close();
    }

    //
    // Create an ANSI UDT and a table with that type of column--
    // if the server is at 10.6 or higher.
    //
    private static void createUDTObjects(Connection con)
            throws SQLException {
        if ( !serverSupportsUDTs(con) ) { return; }

        PreparedStatement ps;

        ps = con.prepareStatement("create type price external name " +
                "'org.apache.derbyTesting.functionTests.tests.lang.Price' " +
                "language java\n");
        ps.execute();
        ps.close();

        ps = con.prepareStatement("create function makePrice( ) returns price " +
                "language java parameter style java no sql external name " +
                "'org.apache.derbyTesting.functionTests.tests.lang.Price.makePrice'\n");
        ps.execute();
        ps.close();

        ps = con.prepareStatement("create table t_price( a price )\n");
        ps.execute();
        ps.close();

        ps = con.prepareStatement("insert into t_price( a ) " +
                "values ( makePrice() )\n" );
        ps.execute();
        ps.close();
    }

    //
    // Create a table with a timestamp column.
    //
    private static void create_derby_2602_objects(Connection con)
            throws SQLException {
        PreparedStatement ps = con.prepareStatement(
                "create table t_2602( a timestamp )\n");
        ps.execute();
        ps.close();
    }


    //
    // Helper methods for declaring a table.
    //
    private static void beginColumnList( StringBuilder builder )
    {
        builder.append( "(\n" );
    }

    private static void endColumnList( StringBuilder builder )
    {
        builder.append( "\n)\n" );
    }

    private static void addColumn( StringBuilder builder, int columnNumber, String text  )
    {
        if ( columnNumber > 0 ) { builder.append( "," ); }

        builder.append( "\n\t" );
        builder.append( text );
    }

    //
    // Drop the tables used by our test cases.
    //
    private void dropSchema()
            throws SQLException {
        dropTable(ALL_TYPES_TABLE);
        dropUDTObjects();
        drop_derby_2602_objects();
    }

    //
    // Drop objects needed by UDT tests. We only do this if the server
    // is at 10.6 or higher.
    //
    private void dropUDTObjects()
            throws SQLException {
        if (serverSupportsUDTs()) {
            dropFunction("MAKEPRICE");
            dropTable("T_PRICE");
            dropUDT("PRICE");
        }
    }

    //
    // Drop objects needed by DERBY-2602 tests.
    //
    private void drop_derby_2602_objects()
            throws SQLException {
        dropTable("T_2602");
    }

    //
    // Logic for stuffing a data value into a column, given its type.
    //
    private    void    setParameter( PreparedStatement ps, int param, TypeDescriptor type, Object value )
        throws SQLException
    {
        int        jdbcType = type.getJdbcType();

        if ( value != null )
        {
            setParameter( ps, param, jdbcType, value );
            return;
        }
        else if ( clientSupports( type ) )
        {
            ps.setNull( param, jdbcType );

            return;
        }

        // client does not support nulls of this type.

        fail( "Unsupported Derby type: " + type.getDerbyTypeName() );
    }

    // return true if the client supports this datatype
    private    boolean    clientSupports( TypeDescriptor type )
            throws SQLException {
        DerbyVersion firstSupportedVersion = type.getDerbyVersion();

        if ( firstSupportedVersion == null ) { return false; }
        else { return getDriverVersion().atLeast( firstSupportedVersion ); }
    }

    //
    // Get a data value from a column, given its type.
    //
    private    Object    getColumn( ResultSet rs, String columnName, TypeDescriptor type )
        throws SQLException
    {
        int            jdbcType = type.getJdbcType();

        return getColumn( rs, columnName, jdbcType );
    }

    //
    // SQL code generation minions
    //
    private static String doubleQuote( String text )
    {
        return '"' + text + '"';
    }

    /////////////////////////////////////////////////////////////
    //
    //    INNER CLASSES
    //
    /////////////////////////////////////////////////////////////

    /**
     * Description of a legal datatype and the version of Derby
     * where the datatype first appears.
     */
    public    static    final    class    TypeDescriptor
    {
        private int          _jdbcType;
        private String       _derbyTypeName;
        /** The first Derby version which supports this type. */
        private DerbyVersion _derbyVersion;

        public    TypeDescriptor (
            int          jdbcType,
            String       derbyTypeName,
            DerbyVersion derbyVersion
        )
        {
            _jdbcType = jdbcType;
            _derbyTypeName = derbyTypeName;
            _derbyVersion = derbyVersion;
        }

        public int        getJdbcType()               { return _jdbcType; }
        public String    getDerbyTypeName()           { return _derbyTypeName; }
        public DerbyVersion    getDerbyVersion()      { return _derbyVersion; }
    }

    /**
     * Helper class capturing TypeCoercion logic.
     * <p>
     * I have abbreviated it to this ugly class name so that the COERCIONS
     * table will fit on a readable screen.
     */
    public    static    final    class    T_CN
    {
        private    int            _jdbcType;
        private    boolean[]    _coercions;

        public    T_CN( int jdbcType, boolean[] coercions )
        {
            _jdbcType = jdbcType;
            _coercions = coercions;
        }

        public    int          getJdbcType()             { return _jdbcType; }
        public    boolean[]    getCoercions()            { return _coercions; }
    }

    /**
     * Returns a suite with all the available JDBC client driver compatibility
     * tests.
     * <p>
     * JUnit boilerplate which adds as test cases all public methods
     * whose names start with the string "test" in the named classes.
     */
    public static Test suite() {
        BaseTestSuite testSuite = new BaseTestSuite("JDBCDriverTest suite");
        testSuite.addTestSuite(JDBCDriverTest.class);
        return TestConfiguration.defaultExistingServerDecorator(
                new BaseJDBCTestSetup(testSuite) {

                    protected void setUp() throws Exception {
                        super.setUp();
                        Connection con = getConnection();
                        Statement s = con.createStatement();
                        // We can't use Connection.getSchema yet.
                        ResultSet rs = s.executeQuery("values CURRENT SCHEMA");
                        rs.next();
                        String schema = rs.getString(1);
                        rs.close();
                        s.close();
                        con.setAutoCommit(false);
                        // Drop the current schema to clean up. Hopefully this
                        // is enough to start "reset" the database for each
                        // client run (running the newest CleanDatabaseTestSetup
                        // fails on older versions).
                        JDBC.dropSchema(con.getMetaData(), schema);
                        con.commit();

                        // Initialize the database for the tests.
                        con.setAutoCommit(true);
                        createTable(con, ALL_TYPES_TABLE, ALL_TYPES);
                        createUDTObjects(con);
                        create_derby_2602_objects(con);
                    }
                });
    }
}
