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

import java.text.MessageFormat;

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


}
