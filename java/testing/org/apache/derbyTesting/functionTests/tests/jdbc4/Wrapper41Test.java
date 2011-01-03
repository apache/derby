/*
 
   Derby - Class org.apache.derbyTesting.functionTests.tests.jdbc4.Wrapper41Test
 
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

package org.apache.derbyTesting.functionTests.tests.jdbc4;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.CallableStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import junit.framework.*;

import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * <p>
 * Machinery shared by the JDBC 4.1 tests for ResultSets and CallableStatements.
 * </p>
 */
public  class   Wrapper41Test   extends BaseJDBCTestCase
{
    ///////////////////////////////////////////////////////////////////////
    //
    // CONSTANTS
    //
    ///////////////////////////////////////////////////////////////////////

    private static  final   String  UNSUPPORTED_COERCION = "22005";
    private static  final   String  BAD_FORMAT = "22018";
    private static  final   String  BAD_DATETIME = "22007";

    private static  final   String  VARIABLE_STRING = "XXXXX";

    ///////////////////////////////////////////////////////////////////////
    //
    // STATE
    //
    ///////////////////////////////////////////////////////////////////////

    private Class   byteArrayClass;

    ///////////////////////////////////////////////////////////////////////
    //
    // CONSTRUCTOR
    //
    ///////////////////////////////////////////////////////////////////////

    /**
     * Create test with given name.
     *
     * @param name name of the test.
     */
    public Wrapper41Test(String name) { super(name); }

    ///////////////////////////////////////////////////////////////////////
    //
    // BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////

    protected void examineJDBC4_1extensions( Wrapper41 wrapper ) throws Exception
    {
        println( "Vetting a " + wrapper.getWrappedObject().getClass().getName() );

        byteArrayClass = Class.forName( "[B" );

        vetWrappedNull( wrapper );
        vetWrappedInteger( wrapper, 1, "BIGINTCOL" );
        vetWrappedBlob( wrapper );
        vetWrappedBoolean( wrapper );
        vetWrappedString( wrapper, 4, "CHARCOL" );
        vetWrappedBinary( wrapper, 5, "CHARFORBITDATACOL" );
        vetWrappedClob( wrapper );
        vetWrappedDate( wrapper );
        vetWrappedFloatingPoint( wrapper, 8, "DOUBLECOL" );
        vetWrappedFloatingPoint( wrapper, 9, "FLOATCOL" );
        vetWrappedInteger( wrapper, 10, "INTCOL" );
        vetWrappedString( wrapper, 11, "LONGVARCHARCOL" );
        vetWrappedBinary( wrapper, 12, "LONGVARCHARFORBITDATACOL" );
        vetWrappedInteger( wrapper, 13, "NUMERICCOL" );
        vetWrappedFloatingPoint( wrapper, 14, "REALCOL" );
        vetWrappedInteger( wrapper, 15, "SMALLINTCOL" );
        vetWrappedTime( wrapper );
        vetWrappedTimestamp( wrapper );
        vetWrappedString( wrapper, 18, "VARCHARCOL" );
        vetWrappedBinary( wrapper, 19, "VARCHARFORBITDATACOL" );
    }
    @SuppressWarnings("unchecked")
    private void    vetWrappedNull( Wrapper41 wrapper ) throws Exception
    {
            try {
                wrapper.getObject( 1, (Class) null );
                fail( "Did not expect to get a result for a null class type." );
            }
            catch (SQLException e)
            {
                assertSQLState( "Null type", UNSUPPORTED_COERCION, e );
            }

            // String overloads not implemented for CallableStatements
            if ( wrapper.getWrappedObject() instanceof CallableStatement ) { return; }
            
            try {
                wrapper.getObject( "BIGINTCOL", (Class) null );
                fail( "Did not expect to get a result for a null class type." );
            }
            catch (SQLException e)
            {
                assertSQLState( "Null type", UNSUPPORTED_COERCION, e );
            }
    }
    private void    vetWrappedInteger( Wrapper41 wrapper, int colID, String colName ) throws Exception
    {
        vetWrapperOK
            (
             wrapper,
             colID,
             colName,
             "1",
             new Class[] { String.class, BigDecimal.class, Byte.class, Short.class, Integer.class, Long.class, Number.class, Object.class }
             );
        vetWrapperOK
            (
             wrapper,
             colID,
             colName,
             "1.0",
             new Class[] { Float.class, Double.class }
             );
        vetWrapperOK
            (
             wrapper,
             colID,
             colName,
             "true",
             new Class[] { Boolean.class }
             );
        
        vetNoWrapper
            (
             wrapper,
             colID,
             colName,
             new Class[] { Date.class, Time.class, Timestamp.class, Blob.class, Clob.class, byteArrayClass, getClass() }
             );
    }
    private void    vetWrappedBlob( Wrapper41 wrapper ) throws Exception
    {
        vetWrapperOK
            (
             wrapper,
             2,
             "BLOBCOL",
             "abc",
             new Class[] { Blob.class, Object.class, byteArrayClass, String.class,  }
             );
        
        vetNoWrapper
            (
             wrapper,
             2,
             "BLOBCOL",
             new Class[]
             {
                 BigDecimal.class, Boolean.class,
                 Byte.class, Short.class, Integer.class, Long.class, Float.class, Double.class,
                 Date.class, Time.class, Timestamp.class
             }
             );

        //
        // We don't try to get a Clob value because we have already gotten a LOB value.
        // Trying to open another LOB stream raises an error. Using a random class type
        // also takes us down that code path, so we don't verify against getClass() either.
        //
    }
    private void    vetWrappedBoolean( Wrapper41 wrapper ) throws Exception
    {
        vetWrapperOK
            (
             wrapper,
             3,
             "BOOLEANCOL",
             "true",
             new Class[] { String.class, Boolean.class, Object.class }
             );
        vetWrapperOK
            (
             wrapper,
             3,
             "BOOLEANCOL",
             "1",
             new Class[] { BigDecimal.class, Byte.class, Short.class, Integer.class, Long.class }
             );
        vetWrapperOK
            (
             wrapper,
             3,
             "BOOLEANCOL",
             "1.0",
             new Class[] { Float.class, Double.class }
             );
        
        vetNoWrapper
            (
             wrapper,
             3,
             "BOOLEANCOL",
             new Class[] { Date.class, Time.class, Timestamp.class, Blob.class, Clob.class, byteArrayClass, getClass() }
             );
    }
    private void    vetWrappedString( Wrapper41 wrapper, int colID, String colName ) throws Exception
    {
        vetWrapperOK
            (
             wrapper,
             colID,
             colName,
             "a",
             new Class[] { String.class, Object.class }
             );

        vetWrapperOK
            (
             wrapper,
             colID,
             colName,
             "true",
             new Class[] { Boolean.class }
             );

        vetCoercionError
            (
             wrapper,
             colID,
             colName,
             new Class[]
             {
                 BigDecimal.class, Byte.class, Short.class, Integer.class, Long.class,
                 Float.class, Double.class,
             },
             BAD_FORMAT
             );

        vetCoercionError
            (
             wrapper,
             colID,
             colName,
             new Class[]
             {
                 Date.class, Time.class, Timestamp.class
             },
             BAD_DATETIME
             );

        vetNoWrapper
            (
             wrapper,
             colID,
             colName,
             new Class[]
             {
                 Blob.class, Clob.class, byteArrayClass, getClass()
             }
             );
    }
    private void    vetWrappedBinary( Wrapper41 wrapper, int colID, String colName ) throws Exception
    {
        vetWrapperOK
            (
             wrapper,
             colID,
             colName,
             "de",
             new Class[] { String.class }
             );
        vetWrapperOK
            (
             wrapper,
             colID,
             colName,
             "\ufffd",
             new Class[] { byteArrayClass, Object.class }
             );
        
        vetNoWrapper
            (
             wrapper,
             colID,
             colName,
             new Class[]
             {
                 Boolean.class, BigDecimal.class, Byte.class, Short.class, Integer.class, Long.class,
                 Float.class, Double.class,
                 Date.class, Time.class, Timestamp.class, Blob.class, Clob.class, getClass()
             }
             );
    }
    private void    vetWrappedClob( Wrapper41 wrapper ) throws Exception
    {
        vetWrapperOK
            (
             wrapper,
             6,
             "CLOBCOL",
             "abc",
             new Class[] { String.class, Clob.class, Object.class }
             );

        vetNoWrapper
            (
             wrapper,
             6,
             "CLOBCOL",
             new Class[]
             {
                 Boolean.class,
                 BigDecimal.class, Byte.class, Short.class, Integer.class, Long.class,
                 Float.class, Double.class,
                 Date.class, Time.class, Timestamp.class,
                 byteArrayClass
             }
             );

        //
        // We don't test getting a BLOB because we are only allowed one attempt
        // to get a LOB from the column. Using a random class type
        // also takes us down that code path, so we don't verify against getClass() either.
        //
    }
    private void    vetWrappedDate( Wrapper41 wrapper ) throws Exception
    {
        vetWrapperOK
            (
             wrapper,
             7,
             "DATECOL",
             "1994-02-23",
             new Class[] { String.class, Date.class, Object.class }
             );
        vetWrapperOK
            (
             wrapper,
             7,
             "DATECOL",
             "1994-02-23 00:00:00.0",
             new Class[] { Timestamp.class }
             );
        
        vetNoWrapper
            (
             wrapper,
             7,
             "DATECOL",
             new Class[]
             {
                 Boolean.class,
                 BigDecimal.class, Byte.class, Short.class, Integer.class, Long.class,
                 Float.class, Double.class,
                 Time.class,
                 Blob.class, Clob.class, byteArrayClass, getClass()
             }
             );
    }
    private void    vetWrappedFloatingPoint( Wrapper41 wrapper, int colID, String colName ) throws Exception
    {
        vetWrapperOK
            (
             wrapper,
             colID,
             colName,
             "1.0",
             new Class[] { String.class, Float.class, Double.class, BigDecimal.class, Number.class, Object.class }
             );
        vetWrapperOK
            (
             wrapper,
             colID,
             colName,
             "1",
             new Class[] { Byte.class, Short.class, Integer.class, Long.class }
             );
        vetWrapperOK
            (
             wrapper,
             colID,
             colName,
             "true",
             new Class[] { Boolean.class }
             );
        
        vetNoWrapper
            (
             wrapper,
             colID,
             colName,
             new Class[] { Date.class, Time.class, Timestamp.class, Blob.class, Clob.class, byteArrayClass, getClass() }
             );
    }
    private void    vetWrappedTime( Wrapper41 wrapper ) throws Exception
    {
        vetWrapperOK
            (
             wrapper,
             16,
             "TIMECOL",
             "15:09:02",
             new Class[] { String.class, Time.class, Object.class }
             );
        vetWrapperOK
            (
             wrapper,
             16,
             "TIMECOL",
             VARIABLE_STRING,
             new Class[] { Timestamp.class }
             );
        
        vetNoWrapper
            (
             wrapper,
             16,
             "TIMECOL",
             new Class[]
             {
                 Boolean.class,
                 BigDecimal.class, Byte.class, Short.class, Integer.class, Long.class,
                 Float.class, Double.class,
                 Date.class,
                 Blob.class, Clob.class, byteArrayClass, getClass()
             }
             );
    }
    private void    vetWrappedTimestamp( Wrapper41 wrapper ) throws Exception
    {
        vetWrapperOK
            (
             wrapper,
             17,
             "TIMESTAMPCOL",
             "1962-09-23 03:23:34.234",
             new Class[] { String.class, Timestamp.class, Object.class }
             );
        vetWrapperOK
            (
             wrapper,
             17,
             "TIMESTAMPCOL",
             "03:23:34",
             new Class[] { Time.class }
             );
        vetWrapperOK
            (
             wrapper,
             17,
             "TIMESTAMPCOL",
             "1962-09-23",
             new Class[] { Date.class }
             );
        
        vetNoWrapper
            (
             wrapper,
             17,
             "TIMESTAMPCOL",
             new Class[]
             {
                 Boolean.class,
                 BigDecimal.class, Byte.class, Short.class, Integer.class, Long.class,
                 Float.class, Double.class,
                 Blob.class, Clob.class, byteArrayClass, getClass()
             }
             );
    }

    @SuppressWarnings("unchecked")
    private void    vetWrapperOK
        ( Wrapper41 wrapper, int colID, String colName, String expectedValue, Class[] supportedCoercions )
        throws Exception
    {
        int coercionCount = supportedCoercions.length;
        for ( int i = 0; i < coercionCount; i++ )
        {
            Class   candidate = supportedCoercions[ i ];
            vetCandidate( candidate, expectedValue, wrapper.getObject( colID, candidate ) );
            
            // you can only retrieve a LOB once
            if ( (candidate == Blob.class) || (candidate == Clob.class) ) { return; }

            // String overloads not implemented for CallableStatement
            if ( !(wrapper.getWrappedObject() instanceof CallableStatement) )
            { vetCandidate( candidate, expectedValue, wrapper.getObject( colName, candidate ) ); }
        }
    }
    @SuppressWarnings("unchecked")
    private void    vetCandidate( Class candidate, String expectedValue, Object actualValue )
        throws Exception
    {
        assertTrue( candidate.getName(), candidate.isAssignableFrom( actualValue.getClass( ) ) );

        if ( VARIABLE_STRING.equals( expectedValue ) ) { return; }
        
        String  actualString;
        if ( actualValue instanceof Blob )
        {
            Blob    blob = (Blob) actualValue;
            actualString = squeezeString( blob.getBytes( 1L, (int) blob.length() ) );
        }
        else if ( actualValue instanceof byte[] )
        {
            actualString = squeezeString( (byte[]) actualValue );
        }
        else if ( actualValue instanceof Clob )
        {
            Clob    clob = (Clob) actualValue;
            actualString = clob.getSubString( 1L, (int) clob.length() );
        }
        else { actualString = actualValue.toString(); }
        
        assertEquals( candidate.getName(), expectedValue, actualString );
    }
    private String  squeezeString( byte[] bytes ) throws Exception
    {
        
        String result = new String( bytes, "UTF-8" );

        return result;
    }
    private void    vetNoWrapper
        ( Wrapper41 wrapper, int colID, String colName, Class[] unsupportedCoercions )
        throws Exception
    {
        vetCoercionError( wrapper, colID, colName, unsupportedCoercions, UNSUPPORTED_COERCION );
    }
    @SuppressWarnings("unchecked")
    private void    vetCoercionError
        ( Wrapper41 wrapper, int colID, String colName, Class[] unsupportedCoercions, String expectedSQLState )
        throws Exception
    {
        int coercionCount = unsupportedCoercions.length;
        for ( int i = 0; i < coercionCount; i++ )
        {
            Class   candidate = unsupportedCoercions[ i ];

            try {
                wrapper.getObject( colID, candidate );
                fail( "Did not expect to get a " + candidate.getName() );
            }
            catch (SQLException e)
            {
                assertSQLState( candidate.getName(), expectedSQLState, e );
            }

            // you can only retrieve a LOB once
            if ( (candidate == Blob.class) || (candidate == Clob.class) ) { return; }

            // String overloads not implemented for CallableStatements
            if ( wrapper.getWrappedObject() instanceof CallableStatement ) { return; }
            
            try {
                wrapper.getObject( colName, candidate );
                fail( "Did not expect to get a " + candidate.getName() );
            }
            catch (SQLException e)
            {
                assertSQLState( candidate.getName(), expectedSQLState, e );
            }
        }
    }
    
    ///////////////////////////////////////////////////////////////////////
    //
    // MINIONS
    //
    ///////////////////////////////////////////////////////////////////////

    protected PreparedStatement    prepareStatement( Connection conn, String text )
        throws Exception
    {
        println( text );
        
        PreparedStatement   ps = conn.prepareStatement( text );

        return ps;
    }

    protected CallableStatement    prepareCall( Connection conn, String text )
        throws Exception
    {
        println( text );
        
        CallableStatement cs = conn.prepareCall( text );

        return cs;
    }

}
