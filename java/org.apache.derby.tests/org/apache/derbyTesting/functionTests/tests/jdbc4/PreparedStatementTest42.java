/*
 *
 * Derby - Class org.apache.derbyTesting.functionTests.tests.jdbc4.PreparedStatementTest42
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, 
 * software distributed under the License is distributed on an 
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, 
 * either express or implied. See the License for the specific 
 * language governing permissions and limitations under the License.
 */

package org.apache.derbyTesting.functionTests.tests.jdbc4;

import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import junit.framework.Test;
import org.apache.derby.iapi.types.HarmonySerialBlob;
import org.apache.derby.iapi.types.HarmonySerialClob;
import org.apache.derbyTesting.functionTests.tests.lang.Price;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Tests for new methods added for PreparedStatement in JDBC 4.2.
 */
public class PreparedStatementTest42 extends BaseJDBCTestCase
{
    //////////////////////////////////////////////////////////
    //
    // CONSTANTS
    //
    //////////////////////////////////////////////////////////

    private static  final   String  UNIMPLEMENTED_FEATURE = "0A000";

    //
    // If any of these becomes a legal Derby type, remove it from this table and put a corresponding line
    // into _columnDescs.
    //
    private static  final   JDBCType[]  ILLEGAL_JDBC_TYPES = new JDBCType[]
    {
        JDBCType.ARRAY,
        JDBCType.DATALINK,
        JDBCType.DISTINCT,
        JDBCType.LONGNVARCHAR,
        JDBCType.NCHAR,
        JDBCType.NCLOB,
        JDBCType.NULL,
        JDBCType.NVARCHAR,
        JDBCType.OTHER,
        JDBCType.REF,
        JDBCType.REF_CURSOR,
        JDBCType.ROWID,
        JDBCType.SQLXML,
        JDBCType.STRUCT,
    };

    private static  final   int[]  ILLEGAL_SQL_TYPES = new int[]
    {
        Types.ARRAY,
        Types.DATALINK,
        Types.DISTINCT,
        Types.LONGNVARCHAR,
        Types.NCHAR,
        Types.NCLOB,
        Types.NVARCHAR,
        Types.OTHER,
        Types.REF,
        Types.REF_CURSOR,
        Types.ROWID,
        Types.SQLXML,
        Types.STRUCT,
    };

    //////////////////////////////////////////////////////////
    //
    // STATE
    //
    //////////////////////////////////////////////////////////

    private static  ColumnDesc[]    _columnDescs =
    {
        new ColumnDesc( JDBCType.BIGINT, "bigint", 0L, 1L, null ),
        new ColumnDesc( JDBCType.BLOB, "blob", makeBlob( "01234" ), makeBlob( "56789" ), null ),
        new ColumnDesc( JDBCType.BOOLEAN, "boolean", Boolean.FALSE, Boolean.TRUE, null ),
        new ColumnDesc( JDBCType.CHAR, "char( 5 )", "01234", "56789", null ),
        new ColumnDesc( JDBCType.BINARY, "char( 5 ) for bit data", makeBinary( "01234" ), makeBinary( "56789" ), null ),
        new ColumnDesc( JDBCType.CLOB, "clob", makeClob( "01234" ), makeClob( "56789" ), null ),
        new ColumnDesc( JDBCType.DATE, "date", new Date( 0L ), new Date( 1L ), null ),
        new ColumnDesc( JDBCType.DECIMAL, "decimal", new BigDecimal( 0 ), new BigDecimal( 1 ), null ),
        new ColumnDesc( JDBCType.DOUBLE, "double", 0.0, 1.0, null ),
        new ColumnDesc( JDBCType.FLOAT, "float", 0.0, 1.0, null ),
        new ColumnDesc( JDBCType.INTEGER, "int", 0, 1, null ),
        new ColumnDesc( JDBCType.LONGVARCHAR, "long varchar", "01234", "56789", null ),
        new ColumnDesc( JDBCType.LONGVARBINARY, "long varchar for bit data", makeBinary( "01234" ), makeBinary( "56789" ), null ),
        new ColumnDesc( JDBCType.NUMERIC, "numeric", new BigDecimal( 0 ), new BigDecimal( 1 ), null ),
        new ColumnDesc( JDBCType.REAL, "real", 0.0F, 1F, null ),
        new ColumnDesc( JDBCType.SMALLINT, "smallint", 0, 1, null ),
        new ColumnDesc( JDBCType.TIME, "time", new Time( 0L ), new Time( 1L ), null ),
        new ColumnDesc( JDBCType.TIMESTAMP, "timestamp", new Timestamp( 0L ), new Timestamp( 1L ), null ),
        new ColumnDesc( JDBCType.JAVA_OBJECT, "Price", makePrice( 0L ), makePrice( 1L ), null ),
        new ColumnDesc( JDBCType.VARCHAR, "varchar( 5 )", "01234", "56789", null ),
        new ColumnDesc( JDBCType.VARBINARY, "varchar( 5 ) for bit data", makeBinary( "01234" ), makeBinary( "56789" ), null ),
        // get/setObject on XML not supported because Derby does not support SQLXML yet
    };

    //////////////////////////////////////////////////////////
    //
    // NESTED CLASSES
    //
    //////////////////////////////////////////////////////////

    public  static  final   class   ColumnDesc
    {
        public  static  final   int VALUE_COUNT = 3;
        
        public  final   JDBCType    jdbcType;
        public  final   String          sqlType;
        public  final   Object[]    values;

        public  ColumnDesc
            (
             JDBCType    jdbcType,
             String          sqlType,
             Object...  values
             )
        {
            this.jdbcType = jdbcType;
            this.sqlType = sqlType;
            this.values = values;

            if ( values.length != VALUE_COUNT )
            {
                throw new IllegalArgumentException( "Expected " + VALUE_COUNT + " values but saw " + values.length );
            }
        }
    }

    //////////////////////////////////////////////////////////
    //
    // CONSTRUCTOR
    //
    //////////////////////////////////////////////////////////

    /**
     * Create a new test with the given name.
     */
    public PreparedStatementTest42( String name ) { super(name); }

    //////////////////////////////////////////////////////////
    //
    // JUnit MACHINERY
    //
    //////////////////////////////////////////////////////////

    public static Test suite()
    {
        BaseTestSuite suite = new BaseTestSuite("PreparedStatementTest42");

        suite.addTest( TestConfiguration.defaultSuite( PreparedStatementTest42.class ) );

        return suite;
    }
    
    //////////////////////////////////////////////////////////
    //
    // TESTS
    //
    //////////////////////////////////////////////////////////

    /**
     * <p>
     * Test the setObject() overloads added by JDBC 4.2.
     * </p>
     */
    public  void    test_01_setObject() throws Exception
    {
        Connection conn = getConnection();

        setupPrice( conn );
        makeTable( conn );
        populateTable( conn );
        vetTableContents( conn );
        updateColumns( conn );
    }
    private void    makeTable( Connection conn ) throws Exception
    {        
        StringBuilder   buffer = new StringBuilder();

        buffer.append( "create table allTypes\n(\n" );
        buffer.append( "\tcol0\tint generated always as identity" );
        for ( int i = 0; i < _columnDescs.length; i++ )
        {
            ColumnDesc  cd = _columnDescs[ i ];
            String  columnName = "col" + (i+1);
            String  columnType = cd.sqlType;
            buffer.append( "\n\t, " + columnName + "\t" + columnType );
        }
        buffer.append( "\n)" );

        conn.prepareStatement( buffer.toString() ).execute();
    }
    private void    populateTable( Connection conn ) throws Exception
    {
        PreparedStatement   insert = prepareInsert( conn );

        for ( int rowIdx = 0; rowIdx < ColumnDesc.VALUE_COUNT; rowIdx++ )
        {
            insertRow( insert, rowIdx );
        }

        for ( int rowIdx = 0; rowIdx < ColumnDesc.VALUE_COUNT; rowIdx++ )
        {
            for ( int colIdx = 0; colIdx < _columnDescs.length; colIdx++ )
            {
                ColumnDesc  cd = _columnDescs[ colIdx ];
                insert.setObject( colIdx + 1, cd.values[ rowIdx ], cd.jdbcType, 0 );
            }
            insert.executeUpdate();
        }


        // verify that certain SQLTypes are illegal
        for ( int i = 0; i < ILLEGAL_JDBC_TYPES.length; i++ )
        {
            try {
                insert.setObject( 1, null, ILLEGAL_JDBC_TYPES[ i ] );
                fail( "setObject() should have failed." );
            }
            catch (SQLException se) { assertUnimplemented( se ); }
        }

        insert.close();
    }
    private static void    assertUnimplemented( SQLException se ) throws Exception
    {
        assertSQLState( UNIMPLEMENTED_FEATURE, se );
        assertTrue( se instanceof SQLFeatureNotSupportedException );

    }
    private PreparedStatement   prepareInsert( Connection conn ) throws Exception
    {
        StringBuilder   columnBuffer = new StringBuilder();
        StringBuilder   valuesBuffer = new StringBuilder();

        columnBuffer.append( "( " );
        valuesBuffer.append( "( " );
        for ( int i = 0; i < _columnDescs.length; i++ )
        {
            String  columnName = "col" + (i+1);
            if ( i > 0 )
            {
                columnBuffer.append( ", " );
                valuesBuffer.append( ", " );
            }
            columnBuffer.append( columnName );
            valuesBuffer.append( "?" );
        }
        columnBuffer.append( " )" );
        valuesBuffer.append( " )" );

        PreparedStatement   insert = conn.prepareStatement
            ( "insert into allTypes " + columnBuffer.toString() + " values " + valuesBuffer.toString() );

        return insert;
    }
    private void    insertRow( PreparedStatement insert, int rowIdx ) throws Exception
    {
        for ( int colIdx = 0; colIdx < _columnDescs.length; colIdx++ )
        {
            ColumnDesc  cd = _columnDescs[ colIdx ];
            insert.setObject( colIdx + 1, cd.values[ rowIdx ], cd.jdbcType );
        }
        insert.executeUpdate();
    }
    private void    vetTableContents( Connection conn ) throws Exception
    {
        PreparedStatement   selectPS = conn.prepareStatement( "select * from allTypes order by col0" );
        ResultSet               selectRS = selectPS.executeQuery();
        int                     rowCount = 0;

        while( selectRS.next() )
        {
            int     rowIdx = rowCount % ColumnDesc.VALUE_COUNT;

            for ( int colIdx = 0; colIdx < _columnDescs.length; colIdx++ )
            {
                Object          expected = _columnDescs[ colIdx ].values[ rowIdx ];

                // skip the first column, the primary key
                assertObjectEquals( expected, selectRS.getObject( colIdx + 2 ) );
            }

            rowCount++;
        }
        
        selectRS.close();
        selectPS.close();
    }
    // test the behavior of the new ResultSet methods added by JDBC 4.2
    private void    updateColumns( Connection conn ) throws Exception
    {
        PreparedStatement forUpdatePS = conn.prepareStatement
            ( "select * from allTypes for update", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE );
        ResultSet   updateRS = null;

        // ResultSet.updateObject( int, Object, SQLType )
        prepTable( conn, 0 );
        updateRS = forUpdatePS.executeQuery();
        updateRS.next();
        for ( int colIdx = 0; colIdx < _columnDescs.length; colIdx++ )
        {
            ColumnDesc  cd = _columnDescs[ colIdx ];
            updateRS.updateObject( colIdx + 2, cd.values[ 1 ], cd.jdbcType );
        }
        updateRS.updateRow();
        updateRS.close();
        vetTable( conn, 1, 1 );

        // ResultSet.updateObject( int, Object, SQLType, int )
        prepTable( conn, 0 );
        updateRS = forUpdatePS.executeQuery();
        updateRS.next();
        for ( int colIdx = 0; colIdx < _columnDescs.length; colIdx++ )
        {
            ColumnDesc  cd = _columnDescs[ colIdx ];
            updateRS.updateObject( colIdx + 2, cd.values[ 1 ], cd.jdbcType, 0 );
        }
        updateRS.updateRow();
        updateRS.close();
        vetTable( conn, 1, 1 );

        // ResultSet.updateObject( String, Object, SQLType )
        prepTable( conn, 0 );
        updateRS = forUpdatePS.executeQuery();
        updateRS.next();
        for ( int colIdx = 0; colIdx < _columnDescs.length; colIdx++ )
        {
            ColumnDesc  cd = _columnDescs[ colIdx ];
            updateRS.updateObject( "col" + (colIdx+1), cd.values[ 1 ], cd.jdbcType );
        }
        updateRS.updateRow();
        updateRS.close();
        vetTable( conn, 1, 1 );

        // ResultSet.updateObject( String, Object, SQLType, int )
        prepTable( conn, 0 );
        updateRS = forUpdatePS.executeQuery();
        updateRS.next();
        for ( int colIdx = 0; colIdx < _columnDescs.length; colIdx++ )
        {
            ColumnDesc  cd = _columnDescs[ colIdx ];
            updateRS.updateObject( "col" + (colIdx+1), cd.values[ 1 ], cd.jdbcType, 0 );
        }
        updateRS.updateRow();
        updateRS.close();
        vetTable( conn, 1, 1 );

        // verify that ResultSet.updateObject() fails on bad SQLTypes
        prepTable( conn, 0 );
        updateRS = forUpdatePS.executeQuery();
        updateRS.next();
        println( "Testing ResultSet.updateObject() on illegal types." );
        for ( int i = 0; i < ILLEGAL_JDBC_TYPES.length; i++ )
        {
            try {
                updateRS.updateObject( 2, _columnDescs[ 0 ].values[ 1 ], ILLEGAL_JDBC_TYPES[ i ] );
                fail( "updateObject() should have failed." );
            }
            catch (SQLException se) { assertUnimplemented( se ); }
            try {
                updateRS.updateObject( 2, _columnDescs[ 0 ].values[ 1 ], ILLEGAL_JDBC_TYPES[ i ], 0 );
                fail( "updateObject() should have failed." );
            }
            catch (SQLException se) { assertUnimplemented( se ); }
            try {
                updateRS.updateObject( "col2", _columnDescs[ 0 ].values[ 1 ], ILLEGAL_JDBC_TYPES[ i ] );
                fail( "updateObject() should have failed." );
            }
            catch (SQLException se) { assertUnimplemented( se ); }
            try {
                updateRS.updateObject( "col2", _columnDescs[ 0 ].values[ 1 ], ILLEGAL_JDBC_TYPES[ i ], 0 );
                fail( "updateObject() should have failed." );
            }
            catch (SQLException se) { assertUnimplemented( se ); }
        }
        updateRS.close();
        vetTable( conn, 0, 1 );
    }
    private void    prepTable( Connection conn, int rowIdx ) throws Exception
    {
        conn.prepareStatement( "truncate table allTypes" ).execute();

        PreparedStatement   insert = prepareInsert( conn );

        insertRow( insert, rowIdx );
        vetTable( conn,rowIdx, 1 );
    }
    private void    vetTable( Connection conn, int rowIdx, int expectedRowCount ) throws Exception
    {
        PreparedStatement   selectPS = conn.prepareStatement( "select * from allTypes order by col0" );
        ResultSet               selectRS = selectPS.executeQuery();
        int                     actualRowCount = 0;

        while( selectRS.next() )
        {
            for ( int colIdx = 0; colIdx < _columnDescs.length; colIdx++ )
            {
                Object          expected = _columnDescs[ colIdx ].values[ rowIdx ];

                // skip the first column, the primary key
                assertObjectEquals( expected, selectRS.getObject( colIdx + 2 ) );
            }

            actualRowCount++;
        }

        assertEquals( expectedRowCount, actualRowCount );
        
        selectRS.close();
        selectPS.close();
    }

    /**
     * <p>
     * Test the CallableStatement.registerObject() overloads added by JDBC 4.2.
     * </p>
     */
    public  void    test_02_registerObject() throws Exception
    {
        Connection conn = getConnection();

        registerObjectTest( conn );
    }
    public  static  void    registerObjectTest( Connection conn ) throws Exception
    {
        createSchemaObjects( conn );
        vetProc( conn );
    }
    private static void    createSchemaObjects( Connection conn ) throws Exception
    {
        setupPrice( conn );
        createProc( conn );
    }
    private static void    createProc( Connection conn ) throws Exception
    {
        StringBuilder   buffer = new StringBuilder();

        buffer.append( "create procedure unpackAllTypes( in valueIdx int" );
        
        for ( int i = 0; i < _columnDescs.length; i++ )
        {
            ColumnDesc  cd = _columnDescs[ i ];
            String  parameterName = "param" + (i+1);
            String  parameterType = cd.sqlType;
            buffer.append( ", out " + parameterName + " " + parameterType );
        }
        
        buffer.append( " ) language java parameter style java no sql\n" );
        buffer.append( "external name 'org.apache.derbyTesting.functionTests.tests.jdbc4.PreparedStatementTest42.unpackAllTypes'" );

        String  sqlText = buffer.toString();
        println( sqlText );

        conn.prepareStatement( sqlText ).execute();
    }
    private static void    vetProc( Connection conn ) throws Exception
    {
        StringBuilder   buffer = new StringBuilder();
        buffer.append( "call unpackAllTypes( ?" );
        for ( int i = 0; i < _columnDescs.length; i++ ) { buffer.append( ", ?" ); }
        buffer.append( " )" );
        String  sqlText = buffer.toString();
        println( sqlText );

        CallableStatement   cs = conn.prepareCall( sqlText );
        int     valueIdx;
        int     param;

        // registerOutParameter( int, SQLType )
        valueIdx = 0;
        param = 1;
        cs.setInt( param++, valueIdx );
        for ( int i = 0; i < _columnDescs.length; i++ )
        {
            cs.registerOutParameter( param++, _columnDescs[ i ].jdbcType );
        }
        cs.execute();
        vetCS( cs, valueIdx );

        // registerOutParameter( int, SQLType, int )
        valueIdx = 1;
        param = 1;
        cs.setInt( param++, valueIdx );
        for ( int i = 0; i < _columnDescs.length; i++ )
        {
            cs.registerOutParameter( param++, _columnDescs[ i ].jdbcType, 0 );
        }
        cs.execute();
        vetCS( cs, valueIdx );

        // registerOutParameter( int, SQLType, String )
        valueIdx = 0;
        param = 1;
        cs.setInt( param++, valueIdx );
        for ( int i = 0; i < _columnDescs.length; i++ )
        {
            cs.registerOutParameter( param++, _columnDescs[ i ].jdbcType, "foo" );
        }
        cs.execute();
        vetCS( cs, valueIdx );

        // Negative test
        valueIdx = 1;
        param = 1;
        cs.setInt( param++, valueIdx );
        for ( int i = 0; i < ILLEGAL_JDBC_TYPES.length; i++ )
        {
            try {
                cs.registerOutParameter( param++, ILLEGAL_JDBC_TYPES[ i ], 0 );
                fail();
            }
            catch (SQLException se) { assertUnimplemented( se ); }
        }

        // registerOutParameter( String, SQLType )
        try {
            cs.registerOutParameter( "param1", _columnDescs[ 0 ].jdbcType );
            fail( "Expected unimplemented feature." );
        }
        catch (SQLException se) { assertUnimplemented( se ); }

        // registerOutParameter( String, SQLType, int )
        try {
            cs.registerOutParameter( "param1", _columnDescs[ 0 ].jdbcType, 0 );
            fail( "Expected unimplemented feature." );
        }
        catch (SQLException se) { assertUnimplemented( se ); }

        // registerOutParameter( String, SQLType, String )
        try {
            cs.registerOutParameter( "param1", _columnDescs[ 0 ].jdbcType, "foo" );
            fail( "Expected unimplemented feature." );
        }
        catch (SQLException se) { assertUnimplemented( se ); }
 
        // Make sure that the pre-JDBC4.2 overloads throw the correct exception too
        valueIdx = 1;
        param = 1;
        cs.setInt( param++, valueIdx );
        for ( int i = 0; i < ILLEGAL_SQL_TYPES.length; i++ )
        {
            int     type = ILLEGAL_SQL_TYPES[ i ];
            try {
                cs.registerOutParameter( param++, type, 0 );
                fail( "Should not have been able to register parameter type " + type );
            } catch (SQLException se) { assertUnimplemented( se ); }
        }
    }
    private  static void    vetCS( CallableStatement cs, int valueIdx )
        throws Exception
    {
        int     idx = 0;
        int     colIdx = 2;

        assertObjectEquals( _columnDescs[ idx++ ].values[ valueIdx ], cs.getObject( colIdx++ ) );
        assertObjectEquals( _columnDescs[ idx++ ].values[ valueIdx ], cs.getObject( colIdx++ ) );
        assertObjectEquals( _columnDescs[ idx++ ].values[ valueIdx ], cs.getObject( colIdx++ ) );
        assertObjectEquals( _columnDescs[ idx++ ].values[ valueIdx ], cs.getObject( colIdx++ ) );
        assertObjectEquals( _columnDescs[ idx++ ].values[ valueIdx ], cs.getObject( colIdx++ ) );
        assertObjectEquals( _columnDescs[ idx++ ].values[ valueIdx ], cs.getObject( colIdx++ ) );
        assertObjectEquals( _columnDescs[ idx++ ].values[ valueIdx ], cs.getObject( colIdx++ ) );
        assertObjectEquals( _columnDescs[ idx++ ].values[ valueIdx ], cs.getObject( colIdx++ ) );
        assertObjectEquals( _columnDescs[ idx++ ].values[ valueIdx ], cs.getObject( colIdx++ ) );
        assertObjectEquals( _columnDescs[ idx++ ].values[ valueIdx ], cs.getObject( colIdx++ ) );
        assertObjectEquals( _columnDescs[ idx++ ].values[ valueIdx ], cs.getObject( colIdx++ ) );
        assertObjectEquals( _columnDescs[ idx++ ].values[ valueIdx ], cs.getObject( colIdx++ ) );
        assertObjectEquals( _columnDescs[ idx++ ].values[ valueIdx ], cs.getObject( colIdx++ ) );
        assertObjectEquals( _columnDescs[ idx++ ].values[ valueIdx ], cs.getObject( colIdx++ ) );
        assertObjectEquals( _columnDescs[ idx++ ].values[ valueIdx ], cs.getObject( colIdx++ ) );
        assertObjectEquals( _columnDescs[ idx++ ].values[ valueIdx ], cs.getObject( colIdx++ ) );
        assertObjectEquals( _columnDescs[ idx++ ].values[ valueIdx ], cs.getObject( colIdx++ ) );
        assertObjectEquals( _columnDescs[ idx++ ].values[ valueIdx ], cs.getObject( colIdx++ ) );
        assertObjectEquals( _columnDescs[ idx++ ].values[ valueIdx ], cs.getObject( colIdx++ ) );
        assertObjectEquals( _columnDescs[ idx++ ].values[ valueIdx ], cs.getObject( colIdx++ ) );
        assertObjectEquals( _columnDescs[ idx++ ].values[ valueIdx ], cs.getObject( colIdx++ ) );
    }

    /**
     * <p>
     * Test the CallableStatement.setObject() overloads added by JDBC 4.2.
     * </p>
     */
    public  void    test_03_setObject() throws Exception
    {
        Connection conn = getConnection();

        callableStatementSetObjectTest( conn );
    }
    public  static  void    callableStatementSetObjectTest( Connection conn ) throws Exception
    {
        createSetObjectSchemaObjects( conn );
        vetSetObjectProc( conn );
    }
    private static void    createSetObjectSchemaObjects( Connection conn ) throws Exception
    {
        setupPrice( conn );
        createSetObjectProc( conn );
    }
    private static void    createSetObjectProc( Connection conn ) throws Exception
    {
        StringBuilder   buffer = new StringBuilder();

        buffer.append( "create procedure packAllTypes( in valueIdx int" );
        
        for ( int i = 0; i < _columnDescs.length; i++ )
        {
            ColumnDesc  cd = _columnDescs[ i ];
            String  parameterName = "param" + (i+1);
            String  parameterType = cd.sqlType;
            buffer.append( ", in " + parameterName + " " + parameterType );
        }
        
        buffer.append( " ) language java parameter style java no sql\n" );
        buffer.append( "external name 'org.apache.derbyTesting.functionTests.tests.jdbc4.PreparedStatementTest42.packAllTypes'" );

        String  sqlText = buffer.toString();
        println( sqlText );

        conn.prepareStatement( sqlText ).execute();
    }
    private static void    vetSetObjectProc( Connection conn ) throws Exception
    {
        StringBuilder   buffer = new StringBuilder();
        buffer.append( "call packAllTypes( ?" );
        for ( int i = 0; i < _columnDescs.length; i++ ) { buffer.append( ", ?" ); }
        buffer.append( " )" );
        String  sqlText = buffer.toString();
        println( sqlText );

        CallableStatement   cs = conn.prepareCall( sqlText );
        int     valueIdx;
        int     param;

        // setObject( int, Object, SQLType )
        valueIdx = 0;
        param = 1;
        cs.setInt( param++, valueIdx );
        for ( int i = 0; i < _columnDescs.length; i++ )
        {
            ColumnDesc  cd = _columnDescs[ i ];
            cs.setObject( param++, cd.values[ valueIdx ], cd.jdbcType );
        }
        cs.execute();

        // setObject( int, Object, SQLType, int )
        valueIdx = 1;
        param = 1;
        cs.setInt( param++, valueIdx );
        for ( int i = 0; i < _columnDescs.length; i++ )
        {
            ColumnDesc  cd = _columnDescs[ i ];
            cs.setObject( param++, cd.values[ valueIdx ], cd.jdbcType, 0 );
        }
        cs.execute();

        // setObject( String, Object, SQLType )
        try {
            ColumnDesc  cd = _columnDescs[ 0 ];
            cs.setObject( "param1", cd.values[ 0 ], cd.jdbcType );
            fail( "Expected unimplemented feature." );
        }
        catch (SQLException se) { assertUnimplemented( se ); }

        // setObject( String, Object, SQLType, int )
        try {
            ColumnDesc  cd = _columnDescs[ 0 ];
            cs.setObject( "param1", cd.values[ 0 ], cd.jdbcType, 0 );
            fail( "Expected unimplemented feature." );
        }
        catch (SQLException se) { assertUnimplemented( se ); }
    }

    /**
     * DERBY-6081: Verify that an SQLException is raised if the supplied
     * SQLType argument is null. It used to fail with a NullPointerException.
     */
    public void test_04_targetTypeIsNull() throws Exception
    {
        setAutoCommit(false);

        // Test PreparedStatement.setObject() with targetType == null.

        PreparedStatement ps = prepareStatement("values cast(? as int)");

        try {
            ps.setObject(1, 1, null);
            fail("setObject should fail when type is null");
        } catch (SQLException se) { assertUnimplemented( se ); }

        try {
            ps.setObject(1, 1, null, 1);
            fail("setObject should fail when type is null");
        } catch (SQLException se) { assertUnimplemented( se ); }

        // Test ResultSet.updateObject() with targetType == null.

        Statement s = createStatement(
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
        s.execute("create table t(x int)");
        s.execute("insert into t values 1");

        ResultSet rs = s.executeQuery("select * from t");
        assertTrue(rs.next());

        try {
            rs.updateObject("x", 1, null);
            fail("updateObject should fail when type is null");
        } catch (SQLException se) { assertUnimplemented( se ); }

        try {
            rs.updateObject(1, 1, null);
            fail("updateObject should fail when type is null");
        } catch (SQLException se) { assertUnimplemented( se ); }

        try {
            rs.updateObject("x", 1, null, 1);
            fail("updateObject should fail when type is null");
        } catch (SQLException se) { assertUnimplemented( se ); }

        try {
            rs.updateObject(1, 1, null, 1);
            fail("updateObject should fail when type is null");
        } catch (SQLException se) { assertUnimplemented( se ); }

        // There should be no more rows.
        JDBC.assertEmpty(rs);
    }

    //////////////////////////////////////////////////////////
    //
    // SQL ROUTINES
    //
    //////////////////////////////////////////////////////////

    public  static  void    unpackAllTypes
        (
         int valueIdx,
         Long[]    bigintValue,
         Blob[]   blobValue,
         Boolean[] booleanValue,
         String[]  charValue,
         byte[][]  binaryValue,
         Clob[]    clobValue,
         Date[]    dateValue,
         BigDecimal[]  decimalValue,
         Double[]  doubleValue,
         Double[]  floatValue,
         Integer[] intValue,
         String[]  longVarcharValue,
         byte[][]  longVarbinaryValue,
         BigDecimal[]  numericValue,
         Float[]   realValue,
         Integer[]   smallintValue,
         Time[]    timeValue,
         Timestamp[]  timestampValue,
         Price[]   priceValue,
         String[]  varcharValue,
         byte[][]  varbinaryValue
         )
    {
        int     colIdx = 0;
        
        bigintValue[ 0 ] = (Long) _columnDescs[ colIdx++ ].values[ valueIdx ];
        blobValue[ 0 ] = (Blob) _columnDescs[ colIdx++ ].values[ valueIdx ];
        booleanValue[ 0 ] = (Boolean) _columnDescs[ colIdx++ ].values[ valueIdx ];
        charValue[ 0 ] = (String) _columnDescs[ colIdx++ ].values[ valueIdx ];
        binaryValue[ 0 ] = (byte[]) _columnDescs[ colIdx++ ].values[ valueIdx ];
        clobValue[ 0 ] = (Clob) _columnDescs[ colIdx++ ].values[ valueIdx ];
        dateValue[ 0 ] = (Date) _columnDescs[ colIdx++ ].values[ valueIdx ];
        decimalValue[ 0 ] = (BigDecimal) _columnDescs[ colIdx++ ].values[ valueIdx ];
        doubleValue[ 0 ] = (Double) _columnDescs[ colIdx++ ].values[ valueIdx ];
        floatValue[ 0 ] = (Double) _columnDescs[ colIdx++ ].values[ valueIdx ];
        intValue[ 0 ] = (Integer) _columnDescs[ colIdx++ ].values[ valueIdx ];
        longVarcharValue[ 0 ] = (String) _columnDescs[ colIdx++ ].values[ valueIdx ];
        longVarbinaryValue[ 0 ] = (byte[]) _columnDescs[ colIdx++ ].values[ valueIdx ];
        numericValue[ 0 ] = (BigDecimal) _columnDescs[ colIdx++ ].values[ valueIdx ];
        realValue[ 0 ] = (Float) _columnDescs[ colIdx++ ].values[ valueIdx ];
        smallintValue[ 0 ] = (Integer) _columnDescs[ colIdx++ ].values[ valueIdx ];
        timeValue[ 0 ] = (Time) _columnDescs[ colIdx++ ].values[ valueIdx ];
        timestampValue[ 0 ] = (Timestamp) _columnDescs[ colIdx++ ].values[ valueIdx ];
        priceValue[ 0 ] = (Price) _columnDescs[ colIdx++ ].values[ valueIdx ];
        varcharValue[ 0 ] = (String) _columnDescs[ colIdx++ ].values[ valueIdx ];
        varbinaryValue[ 0 ] = (byte[]) _columnDescs[ colIdx++ ].values[ valueIdx ];
    }

    public  static  void    packAllTypes
        (
         int valueIdx,
         Long    bigintValue,
         Blob   blobValue,
         Boolean booleanValue,
         String  charValue,
         byte[]  binaryValue,
         Clob    clobValue,
         Date    dateValue,
         BigDecimal  decimalValue,
         Double  doubleValue,
         Double  floatValue,
         Integer intValue,
         String  longVarcharValue,
         byte[]  longVarbinaryValue,
         BigDecimal  numericValue,
         Float   realValue,
         Integer   smallintValue,
         Time    timeValue,
         Timestamp  timestampValue,
         Price   priceValue,
         String  varcharValue,
         byte[]  varbinaryValue
         )
        throws Exception
    {
        int     colIdx = 0;
        
        assertObjectEquals( _columnDescs[ colIdx++ ].values[ valueIdx ], bigintValue );
        assertObjectEquals( _columnDescs[ colIdx++ ].values[ valueIdx ], blobValue );
        assertObjectEquals( _columnDescs[ colIdx++ ].values[ valueIdx ], booleanValue );
        assertObjectEquals( _columnDescs[ colIdx++ ].values[ valueIdx ], charValue );
        assertObjectEquals( _columnDescs[ colIdx++ ].values[ valueIdx ], binaryValue );
        assertObjectEquals( _columnDescs[ colIdx++ ].values[ valueIdx ], clobValue );
        assertObjectEquals( _columnDescs[ colIdx++ ].values[ valueIdx ], dateValue );
        assertObjectEquals( _columnDescs[ colIdx++ ].values[ valueIdx ], decimalValue );
        assertObjectEquals( _columnDescs[ colIdx++ ].values[ valueIdx ], doubleValue );
        assertObjectEquals( _columnDescs[ colIdx++ ].values[ valueIdx ], floatValue );
        assertObjectEquals( _columnDescs[ colIdx++ ].values[ valueIdx ], intValue );
        assertObjectEquals( _columnDescs[ colIdx++ ].values[ valueIdx ], longVarcharValue );
        assertObjectEquals( _columnDescs[ colIdx++ ].values[ valueIdx ], longVarbinaryValue );
        assertObjectEquals( _columnDescs[ colIdx++ ].values[ valueIdx ], numericValue );
        assertObjectEquals( _columnDescs[ colIdx++ ].values[ valueIdx ], realValue );
        assertObjectEquals( _columnDescs[ colIdx++ ].values[ valueIdx ], smallintValue );
        assertObjectEquals( _columnDescs[ colIdx++ ].values[ valueIdx ], timeValue );
        assertObjectEquals( _columnDescs[ colIdx++ ].values[ valueIdx ], timestampValue );
        assertObjectEquals( _columnDescs[ colIdx++ ].values[ valueIdx ], priceValue );
        assertObjectEquals( _columnDescs[ colIdx++ ].values[ valueIdx ], varcharValue );
        assertObjectEquals( _columnDescs[ colIdx++ ].values[ valueIdx ], varbinaryValue );
    }

    //////////////////////////////////////////////////////////
    //
    // MINIONS
    //
    //////////////////////////////////////////////////////////

    private static void setupPrice( Connection conn ) throws Exception
    {
        if ( !aliasExists( conn, "PRICE" ) )
        {
            conn.prepareStatement
                (
                 "create type Price external name 'org.apache.derbyTesting.functionTests.tests.lang.Price' language java"
                 ).execute();
        }
    }
    private static  boolean aliasExists( Connection conn, String aliasName ) throws Exception
    {
        PreparedStatement   ps = conn.prepareStatement( "select count(*) from sys.sysaliases where alias = ?" );
        ps.setString( 1, aliasName );
        ResultSet   rs = ps.executeQuery();
        rs.next();

        int retval = rs.getInt( 1 );

        rs.close();
        ps.close();

        return (retval > 0);
    }
    
    private  static  Blob    makeBlob( String contents )
    {
        return new HarmonySerialBlob( makeBinary( contents ) );
    }

    private  static  Clob    makeClob( String contents )
    {
        return new HarmonySerialClob( contents );
    }

    private  static  byte[]    makeBinary( String contents )
    {
        try {
            return contents.getBytes( "UTF-8" );
        }
        catch (Exception e)
        {
            e.printStackTrace();
            return null;
        }
    }

    private static  Price   makePrice( long raw )
    {
        return Price.makePrice( new BigDecimal( raw ) );
    }

    public static void    assertObjectEquals( Object expected, Object actual ) throws Exception
    {
        if ( expected == null )
        {
            assertNull( actual );
            return;
        }
        else if ( actual == null )
        {
            assertNull( expected );
            return;
        }
        else if ( expected instanceof Blob ) { assertEquals( (Blob) expected, (Blob) actual ); }
        else if ( expected instanceof Clob ) { assertEquals( (Clob) expected, (Clob) actual ); }
        else if ( expected instanceof byte[] ) { compareBytes( (byte[]) expected, (byte[]) actual ); }
        else { assertEquals( expected.toString(), actual.toString() ); }
    }
    private static void  compareBytes( byte[] left, byte[] right )
        throws Exception
    {
        int count = left.length;
        
        if ( count != right.length )
        {
            fail("left count = " + count + " but right count = " + right.length );
        }
        for ( int i = 0; i < count; i++ )
        {
            if ( left[ i ] != right[ i ] )
            {
                fail( "left[ " + i + " ] = " + left[ i ] + " but right[ " + i + " ] = " + right[ i ] );
            }
        }
    }
}
