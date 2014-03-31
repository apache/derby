/*

 Derby - Class org.apache.derbyTesting.system.nstest.NsTestPrintStream

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
package org.apache.derbyTesting.system.nstest;

import java.io.PrintStream;
import java.util.Locale;

/**
 * <p>
 * A stream whose output can be throttled.
 * </p>
 */
public  class   NsTestPrintStream   extends PrintStream
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

    private boolean _chatty;

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTRUCTOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /** Construct a quiet or chatty print stream */
    public  NsTestPrintStream( PrintStream wrappedStream, boolean chatty )
    {
        super( wrappedStream );
        _chatty = chatty;
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // OVERRIDDEN BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

    public PrintStream append( char c )
    {
        if ( _chatty ) { super.append( c ); }
        return this;
    }
    public PrintStream append( CharSequence csq )
    {
        if ( _chatty ) { super.append( csq ); }
        return this;
    }
    public PrintStream append( CharSequence csq, int start, int end )
    {
        if ( _chatty ) { super.append( csq, start, end ); }
        return this;
    }
    public boolean checkError() { return super.checkError(); }
    protected void clearError() { super.clearError(); }
    public void close() { super.close(); }
    public void flush() { super.flush(); }
    public void print( boolean b )   { if ( _chatty ) { super.print( b ); } }
    public void print( char c )   { if ( _chatty ) { super.print( c ); } }
    public void print( int i )   { if ( _chatty ) { super.print( i ); } }
    public void print( long l )   { if ( _chatty ) { super.print( l ); } }
    public void print( float f )   { if ( _chatty ) { super.print( f ); } }
    public void print( double d )   { if ( _chatty ) { super.print( d ); } }
    public void print( char[] s )   { if ( _chatty ) { super.print( s ); } }
    public void print( String s )   { if ( _chatty ) { super.print( s ); } }
    public void print( Object obj )   { if ( _chatty ) { super.print( obj ); } }
    public void println()   { if ( _chatty ) { super.println(); } }
    public void println( boolean x )   { if ( _chatty ) { super.println( x ); } }
    public void println( char x )   { if ( _chatty ) { super.println( x ); } }
    public void println( int x )   { if ( _chatty ) { super.println( x ); } }
    public void println( long x )   { if ( _chatty ) { super.println( x ); } }
    public void println( float x )   { if ( _chatty ) { super.println( x ); } }
    public void println( double x )   { if ( _chatty ) { super.println( x ); } }
    public void println( char[] x )   { if ( _chatty ) { super.println( x ); } }
    public void println( String x )   { if ( _chatty ) { super.println( x ); } }
    public void println( Object x )   { if ( _chatty ) { super.println( x ); } }
    public PrintStream printf( String format, Object... args )
    {
        if ( _chatty ) { super.printf( format, args ); }
        return this;
    }
    public PrintStream printf( Locale l, String format, Object... args )
    {
        if ( _chatty ) { super.printf( l, format, args ); }
        return this;
    }
    public PrintStream format( String format, Object... args )
    {
        if ( _chatty ) { super.format( format, args ); }
        return this;
    }
    public PrintStream format( Locale l, String format, Object... args )
    {
        if ( _chatty ) { super.format( l, format, args ); }
        return this;
    }
    public void write( byte[] buf, int off, int len )   { if ( _chatty ) { super.write( buf, off, len ); } }
    public void write( int b )  { if ( _chatty ) { super.write( b ); } }

}

