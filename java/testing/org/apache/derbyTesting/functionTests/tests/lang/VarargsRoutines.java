/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.lang.VarargsRoutines

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

import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.SQLException;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.derby.iapi.types.HarmonySerialBlob;
import org.apache.derby.iapi.types.HarmonySerialClob;

/**
 * <p>
 * Varargs routines used by VarargsTest.
 * </p>
 */
public  class   VarargsRoutines
{
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTANTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // STATE
    //
    ///////////////////////////////////////////////////////////////////////////////////

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // SQL ROUTINES
    //
    ///////////////////////////////////////////////////////////////////////////////////

    //////////////////
    //
    // SIMPLE ROUTINES
    //
    //////////////////

    /** Compute the maximum of a series of ints */
    public  static  Integer max( Integer... values )
    {
        if ( values == null ) { return null; }
        if ( values.length == 0 ) { return null; }

        int     result = Integer.MIN_VALUE;

        for ( Integer value : values )
        {
            if ( value ==  null ) { return null; }
            result = Math.max( result, value.intValue() );
        }

        return result;
    }

    /** Format a message */
    public  static  String  formatMessage( String message, String... args )
    {
        return MessageFormat.format( message, args );
    }

    //////////////////////////
    //
    // PRIMITIVES VS OBJECTS
    //
    //////////////////////////

    public  static  String  ambiguousTypes( int... a )    { return "primitive";}
    public  static  String  ambiguousTypes( Integer... a )    { return "wrapper";}

    public  static  Integer maxInts( int... values )
    {
        if ( values == null ) { return null; }
        if ( values.length == 0 ) { return null; }

        int     result = Integer.MIN_VALUE;

        for ( int value : values )  { result = Math.max( result, value ); }

        return result;
    }

    //////////////////////////
    //
    // NON-VARARGS METHODS
    //
    //////////////////////////

    public  static  String  nonVarargsMethod( int a )    { return "shouldn't be resolved";}
    public  static  String  nonVarargsMethod( int[] a )    { return "shouldn't be resolved";}

    //////////////////////////
    //
    // VARARGS & NON-VARARGS RESOLUTIONS
    //
    //////////////////////////

    public  static  Integer vnvr( int a )    { return -a;}
    public  static  Integer vnvr( int... a )    { return maxInts( a );}

    //////////////////////////
    //
    // LEADING NON-VARARGS
    //
    //////////////////////////

    public  static  Integer lnv( int... a ) { return maxInts( a );}
    public  static  Integer lnv( int first, int...a ) { return maxInts( a );}
    public  static  Integer lnv( int first, int second, int...a ) { return maxInts( a );}

    //////////////////////////
    //
    // IN, OUT, IN/OUT PARAMETERS
    //
    //////////////////////////

    public  static  void    inVarargs( String[] result, int... values )
    {
        String  retval;
        if ( values == null ) { retval = null; }
        else if ( values.length == 0 ) { retval = null; }
        else
        {
            StringBuilder   buffer = new StringBuilder();

            buffer.append( "RESULT: " );
            
            for ( int value : values )
            {
                buffer.append( " " + Integer.toString( value ) );
            }

            retval = buffer.toString();
        }

        result[ 0 ] = retval;
    }

    public  static  void    outVarargs( int seed, int[]... values )
        throws Exception
    {
        String  retval;
        if ( values == null ) { return; }
        else
        {
            for ( int i = 0; i < values.length; i++ )
            {
                values[ i ][ 0 ] = seed + i;
            }
        }
    }

    public  static  void    inoutVarargs( int seed, int[]... values )
        throws Exception
    {
        String  retval;
        if ( values == null ) { return; }
        else
        {
            for ( int i = 0; i < values.length; i++ )
            {
                values[ i ][ 0 ] += seed;
            }
        }
    }

    ////////////////////////
    //
    // DATATYPE COVERAGE
    //
    ////////////////////////

    public  static  String  add( String seed, int... values )
    {
        if ( values == null ) { return null; }
        if ( values.length == 0 ) { return null; }

        int     result = 0;

        for ( int value : values ) { result += value; }

        return "int " + seed + " " + result;
    }
    
    public  static  String  add( String seed, long... values )
    {
        if ( values == null ) { return null; }
        if ( values.length == 0 ) { return null; }

        long     result = 0;

        for ( long value : values ) { result += value; }

        return "long " + seed + " " + result;
    }
    
    public  static  String  add( String seed, short... values )
    {
        if ( values == null ) { return null; }
        if ( values.length == 0 ) { return null; }

        short     result = 0;

        for ( short value : values ) { result += value; }

        return "short " + seed + " " + result;
    }
    
    public  static  String  add( String seed, Blob... values )
        throws SQLException
    {
        if ( values == null ) { return null; }
        if ( values.length == 0 ) { return null; }

        long     result = 0;

        for ( Blob value : values ) { result += addBytes( value.getBytes( 1L, (int) value.length() ) ); }

        return "blob " + seed + " " + result;
    }

    public  static  String  add( String seed, boolean... values )
        throws SQLException
    {
        if ( values == null ) { return null; }
        if ( values.length == 0 ) { return null; }

        int     result = 0;

        for ( boolean value : values )  { if ( value ) {  result++; } }

        return "boolean " + seed + " " + result;
    }

    public  static  String  add( String seed, String... values )
        throws SQLException
    {
        if ( values == null ) { return null; }
        if ( values.length == 0 ) { return null; }

        String  result = "";

        for ( String value : values )  { result += value; }

        return "String " + seed + " " + result;
    }

    public  static  String  add( String seed, byte[]... values )
        throws SQLException
    {
        if ( values == null ) { return null; }
        if ( values.length == 0 ) { return null; }

        int result = 0;

        for ( byte[] value : values ) { result += addBytes( value ); }

        return "byte[] " + seed + " " + result;
    }

    public  static  String  add( String seed, Clob... values )
        throws SQLException
    {
        if ( values == null ) { return null; }
        if ( values.length == 0 ) { return null; }

        String  result = "";

        for ( Clob value : values ) { result += value.getSubString( 1L, (int) value.length() ); }

        return "clob " + seed + " " + result;
    }

    public  static  String  add( String seed, Date... values )
        throws SQLException
    {
        if ( values == null ) { return null; }
        if ( values.length == 0 ) { return null; }

        long     result = 0;

        for ( Object value : values ) { result ++; }

        return "date " + seed + " " + result;
    }

    public  static  String  add( String seed, Time... values )
        throws SQLException
    {
        if ( values == null ) { return null; }
        if ( values.length == 0 ) { return null; }

        long     result = 0;

        for ( Object value : values ) { result ++; }

        return "time " + seed + " " + result;
    }

    public  static  String  add( String seed, Timestamp... values )
        throws SQLException
    {
        if ( values == null ) { return null; }
        if ( values.length == 0 ) { return null; }

        long     result = 0;

        for ( Object value : values ) { result ++; }

        return "timestamp " + seed + " " + result;
    }

    public  static  String  add( String seed, BigDecimal... values )
    {
        if ( values == null ) { return null; }
        if ( values.length == 0 ) { return null; }

        long     result = 0;

        for ( BigDecimal value : values ) { result += value.longValue(); }

        return "bigdecimal " + seed + " " + result;
    }
    
    public  static  String  add( String seed, double... values )
    {
        if ( values == null ) { return null; }
        if ( values.length == 0 ) { return null; }

        double     result = 0.0;

        for ( double value : values ) { result += value; }

        return "double " + seed + " " + result;
    }
    
    public  static  String  add( String seed, float... values )
    {
        if ( values == null ) { return null; }
        if ( values.length == 0 ) { return null; }

        float     result = 0.0F;

        for ( float value : values ) { result += value; }

        return "float " + seed + " " + result;
    }
    
    public  static  String  add( String seed, Price... values )
        throws SQLException
    {
        if ( values == null ) { return null; }
        if ( values.length == 0 ) { return null; }

        long     result = 0;

        for ( Object value : values ) { result ++; }

        return "Price " + seed + " " + result;
    }

    private static  int addChars( String value )
    {
        int result = 0;

        for ( int i = 0; i < value.length(); i++ ) { result += value.charAt( i ); }

        return result;
    }

    public  static  Blob    makeBlob( String contents )
        throws SQLException
    {
        return new HarmonySerialBlob( makeBytes( contents ) );
    }
    public  static  byte[]    makeBytes( String contents )
        throws SQLException
    {
        int count = contents.length();
        byte[]  bytes = new byte[ count ];

        for ( int i = 0; i < count; i++ ) { bytes[ i ] = (byte) (contents.charAt( i ) - '0'); }

        return bytes;
    }
    public  static  Clob    makeClob( String contents )
        throws SQLException
    {
        return new HarmonySerialClob( contents );
    }

    private static  int addBytes( byte[] value )
    {
        int result = 0;

        for ( byte b : value ) { result += b; }

        return result;
    }
    
    //////////////////////////
    //
    // TABLE FUNCTIONS
    //
    //////////////////////////

    public  static  ResultSet   oneColumnRows( String... values )
    {
        String[]    columnNames = new String[] { "COL1" };
        String[][]  rows;

        if ( (values == null) || (values.length == 0) ) { rows = new String[0][]; }
        else
        {
            int count = values.length;

            rows = new String[ count ][];
            for ( int i = 0; i < count; i++ ) { rows[ i ] = new String[] { values[ i ] }; }
        }

        return new StringArrayVTI( columnNames, rows );
    }

    /**
     * <p>
     * This is a table function which creates a StringArrayVTI out of
     * a space separated list of column names, and a varargs of rows.
     * Each row is a space separated list of column values. Here is
     * a sample usage:
     * </p>
     *
     * <pre>
     * connect 'jdbc:derby:memory:db;create=true';
     * 
     * create function leftTable
     * (
     *     columnNames varchar( 32672 ),
     *     rowContents varchar( 32672 ) ...
     * )
     * returns table
     * (
     *     a   varchar( 5 ),
     *     b   varchar( 5 )
     * )
     * language java parameter style derby_jdbc_result_set no sql
     * external name 'org.apache.derbyTesting.functionTests.tests.lang.VarargsRoutines.stringArrayTable';
     * 
     * select * from table( leftTable( 'A B', 'APP T', 'APP S' ) ) l;
     * </pre>
     */
    public  static  ResultSet   stringArrayTable
        (
         String columnNames,
         String... rows
         )
    {
        ArrayList<String>   columnList = new ArrayList<String>();
        StringTokenizer colToks = new StringTokenizer( columnNames );
        while( colToks.hasMoreTokens() ) { columnList.add( colToks.nextToken()  ); }
        String[]    colNameArg = new String[ columnList.size() ];
        columnList.toArray( colNameArg );

        ArrayList<String[]> rowList = new ArrayList<String[]>();
        for ( String row : rows )
        {
            ArrayList<String>   valueList = new ArrayList<String>();
            StringTokenizer valueToks = new StringTokenizer( row );
            while( valueToks.hasMoreTokens() ) { valueList.add( valueToks.nextToken() ); }
            String[]    valueRow = new String[ valueList.size() ];
            valueList.toArray( valueRow );
            rowList.add( valueRow );
        }

        String[][]  rowsArg = new String[ rowList.size() ][];
        rowList.toArray( rowsArg );

        return new StringArrayVTI( colNameArg, rowsArg );
    }
    
}
