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
import java.sql.Clob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;

import junit.framework.*;

import org.apache.derby.iapi.types.HarmonySerialBlob;
import org.apache.derby.iapi.types.HarmonySerialClob;

import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.TestConfiguration;
import org.apache.derbyTesting.functionTests.tests.lang.Price;

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

    //
    // If any of these becomes a legal Derby type, remove it from this table and put a corresponding line
    // into _columnDescs.
    //
    private static  final   JDBCType[]  ILLEGAL_JDBC_TYPES = new JDBCType[]
    {
        JDBCType.ARRAY,
        JDBCType.DATALINK,
        JDBCType.NCHAR,
        JDBCType.NCLOB,
        JDBCType.NVARCHAR,
        JDBCType.REF,
        JDBCType.REF_CURSOR,
        JDBCType.ROWID,
        JDBCType.SQLXML,
        JDBCType.STRUCT,
        JDBCType.TINYINT,
    };

    //////////////////////////////////////////////////////////
    //
    // STATE
    //
    //////////////////////////////////////////////////////////

    private static  ColumnDesc[]    _columnDescs =
    {
        new ColumnDesc( JDBCType.BIGINT, "bigint", new Long( 0L ), new Long( 1L ), null ),
        new ColumnDesc( JDBCType.BLOB, "blob", makeBlob( "01234" ), makeBlob( "56789" ), null ),
        new ColumnDesc( JDBCType.BOOLEAN, "boolean", Boolean.FALSE, Boolean.TRUE, null ),
        new ColumnDesc( JDBCType.CHAR, "char( 5 )", "01234", "56789", null ),
        new ColumnDesc( JDBCType.BINARY, "char( 5 ) for bit data", makeBinary( "01234" ), makeBinary( "56789" ), null ),
        new ColumnDesc( JDBCType.CLOB, "clob", makeClob( "01234" ), makeClob( "56789" ), null ),
        new ColumnDesc( JDBCType.DATE, "date", new Date( 0L ), new Date( 1L ), null ),
        new ColumnDesc( JDBCType.DECIMAL, "decimal", new BigDecimal( 0 ), new BigDecimal( 1 ), null ),
        new ColumnDesc( JDBCType.DOUBLE, "double", new Double( 0.0 ), new Double( 1.0 ), null ),
        new ColumnDesc( JDBCType.FLOAT, "float", new Double( 0.0 ), new Double( 1.0 ), null ),
        new ColumnDesc( JDBCType.INTEGER, "int", new Integer( 0 ), new Integer( 1 ), null ),
        new ColumnDesc( JDBCType.LONGVARCHAR, "long varchar", "01234", "56789", null ),
        new ColumnDesc( JDBCType.LONGVARBINARY, "long varchar for bit data", makeBinary( "01234" ), makeBinary( "56789" ), null ),
        new ColumnDesc( JDBCType.NUMERIC, "numeric", new BigDecimal( 0 ), new BigDecimal( 1 ), null ),
        new ColumnDesc( JDBCType.REAL, "float", new Float( 0.0F ), new Float( 1F ), null ),
        new ColumnDesc( JDBCType.SMALLINT, "smallint", new Short( (short) 0 ), new Short( (short) 1 ), null ),
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
        TestSuite suite = new TestSuite("PreparedStatementTest42");

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

        makeTable( conn );
        populateTable( conn );
        vetTableContents( conn );
    }
    private void    makeTable( Connection conn ) throws Exception
    {
        conn.prepareStatement
            (
             "create type Price external name 'org.apache.derbyTesting.functionTests.tests.lang.Price' language java"
             ).execute();
        
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

        for ( int rowIdx = 0; rowIdx < ColumnDesc.VALUE_COUNT; rowIdx++ )
        {
            for ( int colIdx = 0; colIdx < _columnDescs.length; colIdx++ )
            {
                ColumnDesc  cd = _columnDescs[ colIdx ];
                insert.setObject( colIdx + 1, cd.values[ rowIdx ], cd.jdbcType );
            }
            insert.executeUpdate();
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
            }
            catch (SQLException se)
            {
                assertSQLState( "0A000", se );
            }
        }

        insert.close();
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

    //////////////////////////////////////////////////////////
    //
    // MINIONS
    //
    //////////////////////////////////////////////////////////

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

    private void    assertObjectEquals( Object expected, Object actual ) throws Exception
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
    private void  compareBytes( byte[] left, byte[] right )
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
