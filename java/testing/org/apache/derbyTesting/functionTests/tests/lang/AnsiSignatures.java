/*

Derby - Class org.apache.derbyTesting.functionTests.tests.lang.AnsiSignatures

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

import java.math.BigDecimal;

/**
 * <p>
 * These are methods for testing ANSI routine resolution. The resolution rules
 * are described in DERBY-3652. Methods which return -1 are methods which we
 * expect will never be matched.
 * </p>
 */
public  class   AnsiSignatures
{
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTANTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    ///////////////////////////////////////////////////////////////////
    //
    // This block of methods is used to test whether Derby matches
    // primitives and wrapper objects according to the ANSI rules.
    // The ANSI rules are described in DERBY-3652.
    //
    ///////////////////////////////////////////////////////////////////

    //
    // SMALLINT
    //
    
    // legal resolutions
    public  static  short   smallint_short_short( short a ) { return a; }
    public  static  short   smallint_short_short( byte a ) { return (short) -1; }
    public  static  short   smallint_short_short( int a ) { return (short) -1; }
    public  static  short   smallint_short_short( long a ) { return (short) -1; }
    public  static  short   smallint_short_short( float a ) { return (short) -1; }
    public  static  short   smallint_short_short( double a ) { return (short) -1; }
    public  static  short   smallint_short_short( Byte a ) { return (short) -1; }
    public  static  short   smallint_short_short( Short a ) { return (short) -1; }
    public  static  short   smallint_short_short( Long a ) { return (short) -1; }
    public  static  short   smallint_short_short( Float a ) { return (short) -1; }
    public  static  short   smallint_short_short( Double a ) { return (short) -1; }
    public  static  short   smallint_short_short( BigDecimal a ) { return (short) -1; }
    public  static  short   smallint_short_short( String a ) { return (short) -1; }
    public  static  short   smallint_short_short( Object a ) { return (short) -1; }
    
    public  static  short   smallint_short_Integer( Integer a ) { return a.shortValue(); }
    public  static  short   smallint_short_Integer( byte a ) { return (short) -1; }
    public  static  short   smallint_short_Integer( int a ) { return (short) -1; }
    public  static  short   smallint_short_Integer( long a ) { return (short) -1; }
    public  static  short   smallint_short_Integer( float a ) { return (short) -1; }
    public  static  short   smallint_short_Integer( double a ) { return (short) -1; }
    public  static  short   smallint_short_Integer( Byte a ) { return (short) -1; }
    public  static  short   smallint_short_Integer( Short a ) { return (short) -1; }
    public  static  short   smallint_short_Integer( Long a ) { return (short) -1; }
    public  static  short   smallint_short_Integer( Float a ) { return (short) -1; }
    public  static  short   smallint_short_Integer( Double a ) { return (short) -1; }
    public  static  short   smallint_short_Integer( BigDecimal a ) { return (short) -1; }
    public  static  short   smallint_short_Integer( String a ) { return (short) -1; }
    public  static  short   smallint_short_Integer( Object a ) { return (short) -1; }

    public  static  Integer smallint_Integer_short( short a ) { return new Integer( a ); }
    public  static  Integer smallint_Integer_short( byte a ) { return new Integer( -1 ); }
    public  static  Integer smallint_Integer_short( int a ) { return new Integer( -1 ); }
    public  static  Integer smallint_Integer_short( long a ) { return new Integer( -1 ); }
    public  static  Integer smallint_Integer_short( float a ) { return new Integer( -1 ); }
    public  static  Integer smallint_Integer_short( double a ) { return new Integer( -1 ); }
    public  static  Integer smallint_Integer_short( Byte a ) { return new Integer( -1 ); }
    public  static  Integer smallint_Integer_short( Short a ) { return new Integer( -1 ); }
    public  static  Integer smallint_Integer_short( Long a ) { return new Integer( -1 ); }
    public  static  Integer smallint_Integer_short( Float a ) { return new Integer( -1 ); }
    public  static  Integer smallint_Integer_short( Double a ) { return new Integer( -1 ); }
    public  static  Integer smallint_Integer_short( BigDecimal a ) { return new Integer( -1 ); }
    public  static  Integer smallint_Integer_short( String a ) { return new Integer( -1 ); }
    public  static  Integer smallint_Integer_short( Object a ) { return new Integer( -1 ); }

    public  static  Integer smallint_Integer_Integer( Integer a ) { return a; }
    public  static  Integer smallint_Integer_Integer( byte a ) { return new Integer( -1 ); }
    public  static  Integer smallint_Integer_Integer( int a ) { return new Integer( -1 ); }
    public  static  Integer smallint_Integer_Integer( long a ) { return new Integer( -1 ); }
    public  static  Integer smallint_Integer_Integer( float a ) { return new Integer( -1 ); }
    public  static  Integer smallint_Integer_Integer( double a ) { return new Integer( -1 ); }
    public  static  Integer smallint_Integer_Integer( Byte a ) { return new Integer( -1 ); }
    public  static  Integer smallint_Integer_Integer( Short a ) { return new Integer( -1 ); }
    public  static  Integer smallint_Integer_Integer( Long a ) { return new Integer( -1 ); }
    public  static  Integer smallint_Integer_Integer( Float a ) { return new Integer( -1 ); }
    public  static  Integer smallint_Integer_Integer( Double a ) { return new Integer( -1 ); }
    public  static  Integer smallint_Integer_Integer( BigDecimal a ) { return new Integer( -1 ); }
    public  static  Integer smallint_Integer_Integer( String a ) { return new Integer( -1 ); }
    public  static  Integer smallint_Integer_Integer( Object a ) { return new Integer( -1 ); }
    
    // outside the spec. these should not resolve.
    public  static  short   smallint_bad_short_Short( Short a ) { return (short) -1; }
    public  static  Short   smallint_bad_Short_short( short a ) { return new Short( (short) -1 ); }
    public  static  Short   smallint_bad_Short_Short( Short a ) { return new Short( (short) -1 ); }

    // bad return type
    public  static  byte   smallint_badreturn_byte_short( short a ) { return (byte) -1; }
    
    // illegal ambiguity
    public  static  short   smallint_amb_short_short( short a ) { return (short) -1; }
    public  static  short   smallint_amb_short_short( Integer a ) { return (short) -1; }
    public  static  Integer smallint_amb_Integer_short( short a ) { return new Integer( -1 ); }
    public  static  Integer smallint_amb_Integer_short( Integer a ) { return new Integer( -1 ); }
    public  static  byte   smallint_amb_byte_short( short a ) { return (byte) -1; }
    public  static  byte   smallint_amb_byte_short( Integer a ) { return (byte) -1; }
    
    // unresolvable
    public  static  short   smallint_unres_short( byte a ) { return (short) -1; }
    public  static  short   smallint_unres_short( int a ) { return (short) -1; }
    public  static  short   smallint_unres_short( long a ) { return (short) -1; }
    public  static  short   smallint_unres_short( float a ) { return (short) -1; }
    public  static  short   smallint_unres_short( double a ) { return (short) -1; }
    public  static  short   smallint_unres_short( Byte a ) { return (short) -1; }
    public  static  short   smallint_unres_short( Short a ) { return (short) -1; }
    public  static  short   smallint_unres_short( Long a ) { return (short) -1; }
    public  static  short   smallint_unres_short( Float a ) { return (short) -1; }
    public  static  short   smallint_unres_short( Double a ) { return (short) -1; }
    public  static  short   smallint_unres_short( BigDecimal a ) { return (short) -1; }
    public  static  short   smallint_unres_short( String a ) { return (short) -1; }
    public  static  short   smallint_unres_short( Object a ) { return (short) -1; }

    public  static  Short   smallint_unres_Short( byte a ) { return new Short( (short) -1 ); }
    public  static  Short   smallint_unres_Short( short a ) { return new Short( (short) -1 ); }
    public  static  Short   smallint_unres_Short( int a ) { return new Short( (short) -1 ); }
    public  static  Short   smallint_unres_Short( long a ) { return new Short( (short) -1 ); }
    public  static  Short   smallint_unres_Short( float a ) { return new Short( (short) -1 ); }
    public  static  Short   smallint_unres_Short( double a ) { return new Short( (short) -1 ); }
    public  static  Short   smallint_unres_Short( Byte a ) { return new Short( (short) -1 ); }
    public  static  Short   smallint_unres_Short( Short a ) { return new Short( (short) -1 ); }
    public  static  Short   smallint_unres_Short( Long a ) { return new Short( (short) -1 ); }
    public  static  Short   smallint_unres_Short( Float a ) { return new Short( (short) -1 ); }
    public  static  Short   smallint_unres_Short( Double a ) { return new Short( (short) -1 ); }
    public  static  Short   smallint_unres_Short( BigDecimal a ) { return new Short( (short) -1 ); }
    public  static  Short   smallint_unres_Short( String a ) { return new Short( (short) -1 ); }
    public  static  Short   smallint_unres_Short( Object a ) { return new Short( (short) -1 ); }



    //
    // INTEGER
    //
    
    // legal resolutions
    public  static  int   integer_int_int( int a ) { return a; }
    public  static  int   integer_int_int( byte a ) { return -1; }
    public  static  int   integer_int_int( short a ) { return -1; }
    public  static  int   integer_int_int( long a ) { return -1; }
    public  static  int   integer_int_int( float a ) { return -1; }
    public  static  int   integer_int_int( double a ) { return -1; }
    public  static  int   integer_int_int( Byte a ) { return -1; }
    public  static  int   integer_int_int( Short a ) { return -1; }
    public  static  int   integer_int_int( Long a ) { return -1; }
    public  static  int   integer_int_int( Float a ) { return -1; }
    public  static  int   integer_int_int( Double a ) { return -1; }
    public  static  int   integer_int_int( BigDecimal a ) { return -1; }
    public  static  int   integer_int_int( String a ) { return -1; }
    public  static  int   integer_int_int( Object a ) { return -1; }

    public  static  int   integer_int_Integer( Integer a ) { return a.intValue(); }
    public  static  int   integer_int_Integer( byte a ) { return -1; }
    public  static  int   integer_int_Integer( short a ) { return -1; }
    public  static  int   integer_int_Integer( long a ) { return -1; }
    public  static  int   integer_int_Integer( float a ) { return -1; }
    public  static  int   integer_int_Integer( double a ) { return -1; }
    public  static  int   integer_int_Integer( Byte a ) { return -1; }
    public  static  int   integer_int_Integer( Short a ) { return -1; }
    public  static  int   integer_int_Integer( Long a ) { return -1; }
    public  static  int   integer_int_Integer( Float a ) { return -1; }
    public  static  int   integer_int_Integer( Double a ) { return -1; }
    public  static  int   integer_int_Integer( BigDecimal a ) { return -1; }
    public  static  int   integer_int_Integer( String a ) { return -1; }
    public  static  int   integer_int_Integer( Object a ) { return -1; }

    public  static  Integer integer_Integer_int( int a ) { return new Integer( a ); }
    public  static  Integer integer_Integer_int( byte a ) { return new Integer( -1 ); }
    public  static  Integer integer_Integer_int( short a ) { return new Integer( -1 ); }
    public  static  Integer integer_Integer_int( long a ) { return new Integer( -1 ); }
    public  static  Integer integer_Integer_int( float a ) { return new Integer( -1 ); }
    public  static  Integer integer_Integer_int( double a ) { return new Integer( -1 ); }
    public  static  Integer integer_Integer_int( Byte a ) { return new Integer( -1 ); }
    public  static  Integer integer_Integer_int( Short a ) { return new Integer( -1 ); }
    public  static  Integer integer_Integer_int( Long a ) { return new Integer( -1 ); }
    public  static  Integer integer_Integer_int( Float a ) { return new Integer( -1 ); }
    public  static  Integer integer_Integer_int( Double a ) { return new Integer( -1 ); }
    public  static  Integer integer_Integer_int( BigDecimal a ) { return new Integer( -1 ); }
    public  static  Integer integer_Integer_int( String a ) { return new Integer( -1 ); }
    public  static  Integer integer_Integer_int( Object a ) { return new Integer( -1 ); }

    public  static  Integer integer_Integer_Integer( Integer a ) { return a; }
    public  static  Integer integer_Integer_Integer( byte a ) { return new Integer( -1 ); }
    public  static  Integer integer_Integer_Integer( short a ) { return new Integer( -1 ); }
    public  static  Integer integer_Integer_Integer( long a ) { return new Integer( -1 ); }
    public  static  Integer integer_Integer_Integer( float a ) { return new Integer( -1 ); }
    public  static  Integer integer_Integer_Integer( double a ) { return new Integer( -1 ); }
    public  static  Integer integer_Integer_Integer( Byte a ) { return new Integer( -1 ); }
    public  static  Integer integer_Integer_Integer( Short a ) { return new Integer( -1 ); }
    public  static  Integer integer_Integer_Integer( Long a ) { return new Integer( -1 ); }
    public  static  Integer integer_Integer_Integer( Float a ) { return new Integer( -1 ); }
    public  static  Integer integer_Integer_Integer( Double a ) { return new Integer( -1 ); }
    public  static  Integer integer_Integer_Integer( BigDecimal a ) { return new Integer( -1 ); }
    public  static  Integer integer_Integer_Integer( String a ) { return new Integer( -1 ); }
    public  static  Integer integer_Integer_Integer( Object a ) { return new Integer( -1 ); }
    
    // bad return type
    public  static  byte   integer_badreturn_byte_int( int a ) { return (byte) -1; }

    // illegal ambiguity
    public  static  int   integer_amb_int_int( int a ) { return -1; }
    public  static  int   integer_amb_int_int( Integer a ) { return -1; }
    public  static  Integer integer_amb_Integer_int( int a ) { return new Integer( -1 ); }
    public  static  Integer integer_amb_Integer_int( Integer a ) { return new Integer( -1 ); }
    public  static  byte   integer_amb_byte_int( int a ) { return (byte) -1; }
    public  static  byte   integer_amb_byte_int( Integer a ) { return (byte) -1; }
    
    // unresolvable
    public  static  int   integer_unres_int( byte a ) { return -1; }
    public  static  int   integer_unres_int( short a ) { return -1; }
    public  static  int   integer_unres_int( long a ) { return -1; }
    public  static  int   integer_unres_int( float a ) { return -1; }
    public  static  int   integer_unres_int( double a ) { return -1; }
    public  static  int   integer_unres_int( Byte a ) { return -1; }
    public  static  int   integer_unres_int( Short a ) { return -1; }
    public  static  int   integer_unres_int( Long a ) { return -1; }
    public  static  int   integer_unres_int( Float a ) { return -1; }
    public  static  int   integer_unres_int( Double a ) { return -1; }
    public  static  int   integer_unres_int( BigDecimal a ) { return -1; }
    public  static  int   integer_unres_int( String a ) { return -1; }
    public  static  int   integer_unres_int( Object a ) { return -1; }

    public  static  Integer   integer_unres_Integer( byte a ) { return new Integer( -1 ); }
    public  static  Integer   integer_unres_Integer( short a ) { return new Integer( -1 ); }
    public  static  Integer   integer_unres_Integer( long a ) { return new Integer( -1 ); }
    public  static  Integer   integer_unres_Integer( float a ) { return new Integer( -1 ); }
    public  static  Integer   integer_unres_Integer( double a ) { return new Integer( -1 ); }
    public  static  Integer   integer_unres_Integer( Byte a ) { return new Integer( -1 ); }
    public  static  Integer   integer_unres_Integer( Short a ) { return new Integer( -1 ); }
    public  static  Integer   integer_unres_Integer( Long a ) { return new Integer( -1 ); }
    public  static  Integer   integer_unres_Integer( Float a ) { return new Integer( -1 ); }
    public  static  Integer   integer_unres_Integer( Double a ) { return new Integer( -1 ); }
    public  static  Integer   integer_unres_Integer( BigDecimal a ) { return new Integer( -1 ); }
    public  static  Integer   integer_unres_Integer( String a ) { return new Integer( -1 ); }
    public  static  Integer   integer_unres_Integer( Object a ) { return new Integer( -1 ); }

    //
    // BIGINT
    //
    
    // legal resolutions
    public  static  long   bigint_long_long( long a ) { return a; }
    public  static  long   bigint_long_long( byte a ) { return -1L; }
    public  static  long   bigint_long_long( short a ) { return -1L; }
    public  static  long   bigint_long_long( int a ) { return -1L; }
    public  static  long   bigint_long_long( float a ) { return -1L; }
    public  static  long   bigint_long_long( double a ) { return -1L; }
    public  static  long   bigint_long_long( Byte a ) { return -1L; }
    public  static  long   bigint_long_long( Short a ) { return -1L; }
    public  static  long   bigint_long_long( Integer a ) { return -1L; }
    public  static  long   bigint_long_long( Float a ) { return -1L; }
    public  static  long   bigint_long_long( Double a ) { return -1L; }
    public  static  long   bigint_long_long( BigDecimal a ) { return -1L; }
    public  static  long   bigint_long_long( String a ) { return -1L; }
    public  static  long   bigint_long_long( Object a ) { return -1L; }

    public  static  long   bigint_long_Long( Long a ) { return a.longValue(); }
    public  static  long   bigint_long_Long( byte a ) { return -1L; }
    public  static  long   bigint_long_Long( short a ) { return -1L; }
    public  static  long   bigint_long_Long( int a ) { return -1L; }
    public  static  long   bigint_long_Long( float a ) { return -1L; }
    public  static  long   bigint_long_Long( double a ) { return -1L; }
    public  static  long   bigint_long_Long( Byte a ) { return -1L; }
    public  static  long   bigint_long_Long( Short a ) { return -1L; }
    public  static  long   bigint_long_Long( Integer a ) { return -1L; }
    public  static  long   bigint_long_Long( Float a ) { return -1L; }
    public  static  long   bigint_long_Long( Double a ) { return -1L; }
    public  static  long   bigint_long_Long( BigDecimal a ) { return -1L; }
    public  static  long   bigint_long_Long( String a ) { return -1L; }
    public  static  long   bigint_long_Long( Object a ) { return -1L; }

    public  static  Long bigint_Long_long( long a ) { return new Long( a ); }
    public  static  Long bigint_Long_long( byte a ) { return new Long( -1L ); }
    public  static  Long bigint_Long_long( short a ) { return new Long( -1L ); }
    public  static  Long bigint_Long_long( int a ) { return new Long( -1L ); }
    public  static  Long bigint_Long_long( float a ) { return new Long( -1L ); }
    public  static  Long bigint_Long_long( double a ) { return new Long( -1L ); }
    public  static  Long bigint_Long_long( Byte a ) { return new Long( -1L ); }
    public  static  Long bigint_Long_long( Short a ) { return new Long( -1L ); }
    public  static  Long bigint_Long_long( Integer a ) { return new Long( -1L ); }
    public  static  Long bigint_Long_long( Float a ) { return new Long( -1L ); }
    public  static  Long bigint_Long_long( Double a ) { return new Long( -1L ); }
    public  static  Long bigint_Long_long( BigDecimal a ) { return new Long( -1L ); }
    public  static  Long bigint_Long_long( String a ) { return new Long( -1L ); }
    public  static  Long bigint_Long_long( Object a ) { return new Long( -1L ); }

    public  static  Long bigint_Long_Long( Long a ) { return a; }
    public  static  Long bigint_Long_Long( byte a ) { return new Long( -1L ); }
    public  static  Long bigint_Long_Long( short a ) { return new Long( -1L ); }
    public  static  Long bigint_Long_Long( int a ) { return new Long( -1L ); }
    public  static  Long bigint_Long_Long( float a ) { return new Long( -1L ); }
    public  static  Long bigint_Long_Long( double a ) { return new Long( -1L ); }
    public  static  Long bigint_Long_Long( Byte a ) { return new Long( -1L ); }
    public  static  Long bigint_Long_Long( Short a ) { return new Long( -1L ); }
    public  static  Long bigint_Long_Long( Integer a ) { return new Long( -1L ); }
    public  static  Long bigint_Long_Long( Float a ) { return new Long( -1L ); }
    public  static  Long bigint_Long_Long( Double a ) { return new Long( -1L ); }
    public  static  Long bigint_Long_Long( BigDecimal a ) { return new Long( -1L ); }
    public  static  Long bigint_Long_Long( String a ) { return new Long( -1L ); }
    public  static  Long bigint_Long_Long( Object a ) { return new Long( -1L ); }
    
    // bad return type
    public  static  byte   bigint_badreturn_byte_long( long a ) { return (byte) -1; }

    // illegal ambiguity
    public  static  long   bigint_amb_long_long( long a ) { return -1L; }
    public  static  long   bigint_amb_long_long( Long a ) { return -1L; }
    public  static  Long bigint_amb_Long_long( long a ) { return new Long( -1L ); }
    public  static  Long bigint_amb_Long_long( Long a ) { return new Long( -1L ); }
    public  static  byte   bigint_amb_byte_long( long a ) { return (byte) -1; }
    public  static  byte   bigint_amb_byte_long( Long a ) { return (byte) -1; }
    
    // unresolvable
    public  static  long   bigint_unres_long( byte a ) { return -1L; }
    public  static  long   bigint_unres_long( short a ) { return -1L; }
    public  static  long   bigint_unres_long( int a ) { return -1L; }
    public  static  long   bigint_unres_long( float a ) { return -1L; }
    public  static  long   bigint_unres_long( double a ) { return -1L; }
    public  static  long   bigint_unres_long( Byte a ) { return -1L; }
    public  static  long   bigint_unres_long( Short a ) { return -1L; }
    public  static  long   bigint_unres_long( Integer a ) { return -1L; }
    public  static  long   bigint_unres_long( Float a ) { return -1L; }
    public  static  long   bigint_unres_long( Double a ) { return -1L; }
    public  static  long   bigint_unres_long( BigDecimal a ) { return -1L; }
    public  static  long   bigint_unres_long( String a ) { return -1L; }
    public  static  long   bigint_unres_long( Object a ) { return -1L; }

    public  static  Long   bigint_unres_Long( byte a ) { return new Long( -1L ); }
    public  static  Long   bigint_unres_Long( short a ) { return new Long( -1L ); }
    public  static  Long   bigint_unres_Long( int a ) { return new Long( -1L ); }
    public  static  Long   bigint_unres_Long( float a ) { return new Long( -1L ); }
    public  static  Long   bigint_unres_Long( double a ) { return new Long( -1L ); }
    public  static  Long   bigint_unres_Long( Byte a ) { return new Long( -1L ); }
    public  static  Long   bigint_unres_Long( Short a ) { return new Long( -1L ); }
    public  static  Long   bigint_unres_Long( Integer a ) { return new Long( -1L ); }
    public  static  Long   bigint_unres_Long( Float a ) { return new Long( -1L ); }
    public  static  Long   bigint_unres_Long( Double a ) { return new Long( -1L ); }
    public  static  Long   bigint_unres_Long( BigDecimal a ) { return new Long( -1L ); }
    public  static  Long   bigint_unres_Long( String a ) { return new Long( -1L ); }
    public  static  Long   bigint_unres_Long( Object a ) { return new Long( -1L ); }
    
    //
    // REAL
    //
    
    // legal resolutions
    public  static  float   real_float_float( float a ) { return a; }
    public  static  float   real_float_float( byte a ) { return -1.0F; }
    public  static  float   real_float_float( short a ) { return -1.0F; }
    public  static  float   real_float_float( int a ) { return -1.0F; }
    public  static  float   real_float_float( long a ) { return -1.0F; }
    public  static  float   real_float_float( double a ) { return -1.0F; }
    public  static  float   real_float_float( Byte a ) { return -1.0F; }
    public  static  float   real_float_float( Short a ) { return -1.0F; }
    public  static  float   real_float_float( Integer a ) { return -1.0F; }
    public  static  float   real_float_float( Long a ) { return -1.0F; }
    public  static  float   real_float_float( Double a ) { return -1.0F; }
    public  static  float   real_float_float( BigDecimal a ) { return -1.0F; }
    public  static  float   real_float_float( String a ) { return -1.0F; }
    public  static  float   real_float_float( Object a ) { return -1.0F; }

    public  static  float   real_float_Float( Float a ) { return a.floatValue(); }
    public  static  float   real_float_Float( byte a ) { return -1.0F; }
    public  static  float   real_float_Float( short a ) { return -1.0F; }
    public  static  float   real_float_Float( int a ) { return -1.0F; }
    public  static  float   real_float_Float( long a ) { return -1.0F; }
    public  static  float   real_float_Float( double a ) { return -1.0F; }
    public  static  float   real_float_Float( Byte a ) { return -1.0F; }
    public  static  float   real_float_Float( Short a ) { return -1.0F; }
    public  static  float   real_float_Float( Integer a ) { return -1.0F; }
    public  static  float   real_float_Float( Long a ) { return -1.0F; }
    public  static  float   real_float_Float( Double a ) { return -1.0F; }
    public  static  float   real_float_Float( BigDecimal a ) { return -1.0F; }
    public  static  float   real_float_Float( String a ) { return -1.0F; }
    public  static  float   real_float_Float( Object a ) { return -1.0F; }

    public  static  Float real_Float_float( float a ) { return new Float( a ); }
    public  static  Float real_Float_float( byte a ) { return new Float( -1.0F ); }
    public  static  Float real_Float_float( short a ) { return new Float( -1.0F ); }
    public  static  Float real_Float_float( int a ) { return new Float( -1.0F ); }
    public  static  Float real_Float_float( long a ) { return new Float( -1.0F ); }
    public  static  Float real_Float_float( double a ) { return new Float( -1.0F ); }
    public  static  Float real_Float_float( Byte a ) { return new Float( -1.0F ); }
    public  static  Float real_Float_float( Short a ) { return new Float( -1.0F ); }
    public  static  Float real_Float_float( Integer a ) { return new Float( -1.0F ); }
    public  static  Float real_Float_float( Long a ) { return new Float( -1.0F ); }
    public  static  Float real_Float_float( Double a ) { return new Float( -1.0F ); }
    public  static  Float real_Float_float( BigDecimal a ) { return new Float( -1.0F ); }
    public  static  Float real_Float_float( String a ) { return new Float( -1.0F ); }
    public  static  Float real_Float_float( Object a ) { return new Float( -1.0F ); }

    public  static  Float real_Float_Float( Float a ) { return a; }
    public  static  Float real_Float_Float( byte  a ) { return new Float( -1.0F ); }
    public  static  Float real_Float_Float( short a ) { return new Float( -1.0F ); }
    public  static  Float real_Float_Float( int a ) { return new Float( -1.0F ); }
    public  static  Float real_Float_Float( long a ) { return new Float( -1.0F ); }
    public  static  Float real_Float_Float( double a ) { return new Float( -1.0F ); }
    public  static  Float real_Float_Float( Byte a ) { return new Float( -1.0F ); }
    public  static  Float real_Float_Float( Short a ) { return new Float( -1.0F ); }
    public  static  Float real_Float_Float( Integer a ) { return new Float( -1.0F ); }
    public  static  Float real_Float_Float( Long a ) { return new Float( -1.0F ); }
    public  static  Float real_Float_Float( Double a ) { return new Float( -1.0F ); }
    public  static  Float real_Float_Float( BigDecimal a ) { return new Float( -1.0F ); }
    public  static  Float real_Float_Float( String a ) { return new Float( -1.0F ); }
    public  static  Float real_Float_Float( Object a ) { return new Float( -1.0F ); }
    
    // bad return type
    public  static  byte   real_badreturn_byte_float( float a ) { return (byte) -1; }

    // illegal ambiguity
    public  static  float   real_amb_float_float( float a ) { return -1.0F; }
    public  static  float   real_amb_float_float( Float a ) { return -1.0F; }
    public  static  Float real_amb_Float_float( float a ) { return new Float( -1.0F ); }
    public  static  Float real_amb_Float_float( Float a ) { return new Float( -1.0F ); }
    public  static  byte   real_amb_byte_float( float a ) { return (byte) -1; }
    public  static  byte   real_amb_byte_float( Float a ) { return (byte) -1; }
    
    // unresolvable
    public  static  float   real_unres_float( byte a ) { return -1.0F; }
    public  static  float   real_unres_float( short a ) { return -1.0F; }
    public  static  float   real_unres_float( int a ) { return -1.0F; }
    public  static  float   real_unres_float( long a ) { return -1.0F; }
    public  static  float   real_unres_float( double a ) { return -1.0F; }
    public  static  float   real_unres_float( Byte a ) { return -1.0F; }
    public  static  float   real_unres_float( Short a ) { return -1.0F; }
    public  static  float   real_unres_float( Integer a ) { return -1.0F; }
    public  static  float   real_unres_float( Long a ) { return -1.0F; }
    public  static  float   real_unres_float( Double a ) { return -1.0F; }
    public  static  float   real_unres_float( BigDecimal a ) { return -1.0F; }
    public  static  float   real_unres_float( String a ) { return -1.0F; }
    public  static  float   real_unres_float( Object a ) { return -1.0F; }

    public  static  Float   real_unres_Float( byte a ) { return new Float( -1.0F ); }
    public  static  Float   real_unres_Float( short a ) { return new Float( -1.0F ); }
    public  static  Float   real_unres_Float( int a ) { return new Float( -1.0F ); }
    public  static  Float   real_unres_Float( long a ) { return new Float( -1.0F ); }
    public  static  Float   real_unres_Float( double a ) { return new Float( -1.0F ); }
    public  static  Float   real_unres_Float( Byte a ) { return new Float( -1.0F ); }
    public  static  Float   real_unres_Float( Short a ) { return new Float( -1.0F ); }
    public  static  Float   real_unres_Float( Integer a ) { return new Float( -1.0F ); }
    public  static  Float   real_unres_Float( Long a ) { return new Float( -1.0F ); }
    public  static  Float   real_unres_Float( Double a ) { return new Float( -1.0F ); }
    public  static  Float   real_unres_Float( BigDecimal a ) { return new Float( -1.0F ); }
    public  static  Float   real_unres_Float( String a ) { return new Float( -1.0F ); }
    public  static  Float   real_unres_Float( Object a ) { return new Float( -1.0F ); }

    //
    // DOUBLE
    //
    
    // legal resolutions
    public  static  double   double_double_double( double a ) { return a; }
    public  static  double   double_double_double( byte a ) { return -1.0; }
    public  static  double   double_double_double( short a ) { return -1.0; }
    public  static  double   double_double_double( int a ) { return -1.0; }
    public  static  double   double_double_double( long a ) { return -1.0; }
    public  static  double   double_double_double( float a ) { return -1.0; }
    public  static  double   double_double_double( Byte a ) { return -1.0; }
    public  static  double   double_double_double( Short a ) { return -1.0; }
    public  static  double   double_double_double( Integer a ) { return -1.0; }
    public  static  double   double_double_double( Long a ) { return -1.0; }
    public  static  double   double_double_double( Float a ) { return -1.0; }
    public  static  double   double_double_double( BigDecimal a ) { return -1.0; }
    public  static  double   double_double_double( String a ) { return -1.0; }
    public  static  double   double_double_double( Object a ) { return -1.0; }

    public  static  double   double_double_Double( Double a ) { return a.doubleValue(); }
    public  static  double   double_double_Double( byte a ) { return -1.0; }
    public  static  double   double_double_Double( short a ) { return -1.0; }
    public  static  double   double_double_Double( int a ) { return -1.0; }
    public  static  double   double_double_Double( long a ) { return -1.0; }
    public  static  double   double_double_Double( float a ) { return -1.0; }
    public  static  double   double_double_Double( Byte a ) { return -1.0; }
    public  static  double   double_double_Double( Short a ) { return -1.0; }
    public  static  double   double_double_Double( Integer a ) { return -1.0; }
    public  static  double   double_double_Double( Long a ) { return -1.0; }
    public  static  double   double_double_Double( Float a ) { return -1.0; }
    public  static  double   double_double_Double( BigDecimal a ) { return -1.0; }
    public  static  double   double_double_Double( String a ) { return -1.0; }
    public  static  double   double_double_Double( Object a ) { return -1.0; }

    public  static  Double double_Double_double( double a ) { return new Double( a ); }
    public  static  Double double_Double_double( byte a ) { return new Double( -1.0 ); }
    public  static  Double double_Double_double( short a ) { return new Double( -1.0 ); }
    public  static  Double double_Double_double( int a ) { return new Double( -1.0 ); }
    public  static  Double double_Double_double( long a ) { return new Double( -1.0 ); }
    public  static  Double double_Double_double( float a ) { return new Double( -1.0 ); }
    public  static  Double double_Double_double( Byte a ) { return new Double( -1.0 ); }
    public  static  Double double_Double_double( Short a ) { return new Double( -1.0 ); }
    public  static  Double double_Double_double( Integer a ) { return new Double( -1.0 ); }
    public  static  Double double_Double_double( Long a ) { return new Double( -1.0 ); }
    public  static  Double double_Double_double( Float a ) { return new Double( -1.0 ); }
    public  static  Double double_Double_double( BigDecimal a ) { return new Double( -1.0 ); }
    public  static  Double double_Double_double( String a ) { return new Double( -1.0 ); }
    public  static  Double double_Double_double( Object a ) { return new Double( -1.0 ); }

    public  static  Double double_Double_Double( Double a ) { return a; }
    public  static  Double double_Double_Double( byte a ) { return new Double( -1.0 ); }
    public  static  Double double_Double_Double( short a ) { return new Double( -1.0 ); }
    public  static  Double double_Double_Double( int a ) { return new Double( -1.0 ); }
    public  static  Double double_Double_Double( long a ) { return new Double( -1.0 ); }
    public  static  Double double_Double_Double( float a ) { return new Double( -1.0 ); }
    public  static  Double double_Double_Double( Byte a ) { return new Double( -1.0 ); }
    public  static  Double double_Double_Double( Short a ) { return new Double( -1.0 ); }
    public  static  Double double_Double_Double( Integer a ) { return new Double( -1.0 ); }
    public  static  Double double_Double_Double( Long a ) { return new Double( -1.0 ); }
    public  static  Double double_Double_Double( Float a ) { return new Double( -1.0 ); }
    public  static  Double double_Double_Double( BigDecimal a ) { return new Double( -1.0 ); }
    public  static  Double double_Double_Double( String a ) { return new Double( -1.0 ); }
    public  static  Double double_Double_Double( Object a ) { return new Double( -1.0 ); }
    
    // bad return type
    public  static  byte   double_badreturn_byte_double( double a ) { return (byte) -1; }

    // illegal ambiguity
    public  static  double   double_amb_double_double( double a ) { return -1.0; }
    public  static  double   double_amb_double_double( Double a ) { return -1.0; }
    public  static  Double double_amb_Double_double( double a ) { return new Double( -1.0 ); }
    public  static  Double double_amb_Double_double( Double a ) { return new Double( -1.0 ); }
    public  static  byte   double_amb_byte_double( double a ) { return (byte) -1; }
    public  static  byte   double_amb_byte_double( Double a ) { return (byte) -1; }
    
    // unresolvable
    public  static  double   double_unres_double( byte a ) { return -1.0; }
    public  static  double   double_unres_double( short a ) { return -1.0; }
    public  static  double   double_unres_double( int a ) { return -1.0; }
    public  static  double   double_unres_double( long a ) { return -1.0; }
    public  static  double   double_unres_double( float a ) { return -1.0; }
    public  static  double   double_unres_double( Byte a ) { return -1.0; }
    public  static  double   double_unres_double( Short a ) { return -1.0; }
    public  static  double   double_unres_double( Integer a ) { return -1.0; }
    public  static  double   double_unres_double( Long a ) { return -1.0; }
    public  static  double   double_unres_double( Float a ) { return -1.0; }
    public  static  double   double_unres_double( BigDecimal a ) { return -1.0; }
    public  static  double   double_unres_double( String a ) { return -1.0; }
    public  static  double   double_unres_double( Object a ) { return -1.0; }
 
    public  static  Double   double_unres_Double( byte a ) { return new Double( -1.0 ); }
    public  static  Double   double_unres_Double( short a ) { return new Double( -1.0 ); }
    public  static  Double   double_unres_Double( int a ) { return new Double( -1.0 ); }
    public  static  Double   double_unres_Double( long a ) { return new Double( -1.0 ); }
    public  static  Double   double_unres_Double( float a ) { return new Double( -1.0 ); }
    public  static  Double   double_unres_Double( Byte a ) { return new Double( -1.0 ); }
    public  static  Double   double_unres_Double( Short a ) { return new Double( -1.0 ); }
    public  static  Double   double_unres_Double( Integer a ) { return new Double( -1.0 ); }
    public  static  Double   double_unres_Double( Long a ) { return new Double( -1.0 ); }
    public  static  Double   double_unres_Double( Float a ) { return new Double( -1.0 ); } 
    public  static  Double   double_unres_Double( BigDecimal a ) { return new Double( -1.0 ); }
    public  static  Double   double_unres_Double( String a ) { return new Double( -1.0 ); }
    public  static  Double   double_unres_Double( Object a ) { return new Double( -1.0 ); }

    //
    // NUMERIC
    //
    
    // legal resolutions
    public  static  BigDecimal   numeric_BigDecimal_BigDecimal( BigDecimal a ) { return a; }
    public  static  BigDecimal   numeric_BigDecimal_BigDecimal( byte a ) { return new BigDecimal( -1.0 ); }
    public  static  BigDecimal   numeric_BigDecimal_BigDecimal( int a ) { return new BigDecimal( -1.0 ); }
    public  static  BigDecimal   numeric_BigDecimal_BigDecimal( short a ) { return new BigDecimal( -1.0 ); }
    public  static  BigDecimal   numeric_BigDecimal_BigDecimal( long a ) { return new BigDecimal( -1.0 ); }
    public  static  BigDecimal   numeric_BigDecimal_BigDecimal( float a ) { return new BigDecimal( -1.0 ); }
    public  static  BigDecimal   numeric_BigDecimal_BigDecimal( double a ) { return new BigDecimal( -1.0 ); }
    public  static  BigDecimal   numeric_BigDecimal_BigDecimal( Byte a ) { return new BigDecimal( -1.0 ); }
    public  static  BigDecimal   numeric_BigDecimal_BigDecimal( Short a ) { return new BigDecimal( -1.0 ); }
    public  static  BigDecimal   numeric_BigDecimal_BigDecimal( Integer a ) { return new BigDecimal( -1.0 ); }
    public  static  BigDecimal   numeric_BigDecimal_BigDecimal( Long a ) { return new BigDecimal( -1.0 ); }
    public  static  BigDecimal   numeric_BigDecimal_BigDecimal( Float a ) { return new BigDecimal( -1.0 ); }
    public  static  BigDecimal   numeric_BigDecimal_BigDecimal( Double a ) { return new BigDecimal( -1.0 ); }
    public  static  BigDecimal   numeric_BigDecimal_BigDecimal( String a ) { return new BigDecimal( -1.0 ); }
    public  static  BigDecimal   numeric_BigDecimal_BigDecimal( Object a ) { return new BigDecimal( -1.0 ); }

    //
    // VARCHAR
    //
    
    // legal resolutions
    public  static  String   varchar_String_String( String a ) { return a; }
    public  static  String   varchar_String_String( byte a ) { return "-1"; }
    public  static  String   varchar_String_String( short a ) { return "-1"; }
    public  static  String   varchar_String_String( int a ) { return "-1"; }
    public  static  String   varchar_String_String( long a ) { return "-1"; }
    public  static  String   varchar_String_String( float a ) { return "-1"; }
    public  static  String   varchar_String_String( double a ) { return "-1"; }
    public  static  String   varchar_String_String( Byte a ) { return "-1"; }
    public  static  String   varchar_String_String( Short a ) { return "-1"; }
    public  static  String   varchar_String_String( Integer a ) { return "-1"; }
    public  static  String   varchar_String_String( Long a ) { return "-1"; }
    public  static  String   varchar_String_String( Float a ) { return "-1"; }
    public  static  String   varchar_String_String( Double a ) { return "-1"; }
    public  static  String   varchar_String_String( Object a ) { return "-1"; }
    
    //
    // Mixed types
    //
    public static long bigint__smallint_int_bigint_real_double
        (
         short a,
         Integer b,
         long c,
         Float d,
         double e
         )
    { return c; }

    public static Long flipped_bigint__smallint_int_bigint_real_double
        (
         Integer a,
         int b,
         Long c,
         float d,
         Double e
         )
    { return c; }

    //
    // BINARY
    //
    
    // legal resolutions

    public  static  byte[]  binary_bytes_bytes( byte[] a ) { return a; }
    public  static  byte[]  binary_bytes_bytes( byte a ) { return new byte[] { (byte) -1 }; }
    public  static  byte[]  binary_bytes_bytes( int a ) { return new byte[] { (byte) -1 }; }
    public  static  byte[]  binary_bytes_bytes( long a ) { return new byte[] { (byte) -1 }; }
    public  static  byte[]  binary_bytes_bytes( float a ) { return new byte[] { (byte) -1 }; }
    public  static  byte[]  binary_bytes_bytes( double a ) { return new byte[] { (byte) -1 }; }
    public  static  byte[]  binary_bytes_bytes( Byte a ) { return new byte[] { (byte) -1 }; }
    public  static  byte[]  binary_bytes_bytes( Short a ) { return new byte[] { (byte) -1 }; }
    public  static  byte[]  binary_bytes_bytes( Long a ) { return new byte[] { (byte) -1 }; }
    public  static  byte[]  binary_bytes_bytes( Float a ) { return new byte[] { (byte) -1 }; }
    public  static  byte[]  binary_bytes_bytes( Double a ) { return new byte[] { (byte) -1 }; }
    public  static  byte[]  binary_bytes_bytes( BigDecimal a ) { return new byte[] { (byte) -1 }; }
    public  static  byte[]  binary_bytes_bytes( String a ) { return new byte[] { (byte) -1 }; }
    public  static  byte[]  binary_bytes_bytes( Object a ) { return new byte[] { (byte) -1 }; }

    public  static  byte[]  binary_bytes_int( int a ) { return new byte[] { (byte) a }; }

    // bad return type
    public  static  int  binary_badreturn_bytes_bytes( byte[] a ) { return -1; }
    
    //
    // VARBINARY
    //
    
    // legal resolutions

    public  static  byte[]  varbinary_bytes_bytes( byte[] a ) { return a; }
    public  static  byte[]  varbinary_bytes_bytes( byte a ) { return new byte[] { (byte) -1 }; }
    public  static  byte[]  varbinary_bytes_bytes( int a ) { return new byte[] { (byte) -1 }; }
    public  static  byte[]  varbinary_bytes_bytes( long a ) { return new byte[] { (byte) -1 }; }
    public  static  byte[]  varbinary_bytes_bytes( float a ) { return new byte[] { (byte) -1 }; }
    public  static  byte[]  varbinary_bytes_bytes( double a ) { return new byte[] { (byte) -1 }; }
    public  static  byte[]  varbinary_bytes_bytes( Byte a ) { return new byte[] { (byte) -1 }; }
    public  static  byte[]  varbinary_bytes_bytes( Short a ) { return new byte[] { (byte) -1 }; }
    public  static  byte[]  varbinary_bytes_bytes( Long a ) { return new byte[] { (byte) -1 }; }
    public  static  byte[]  varbinary_bytes_bytes( Float a ) { return new byte[] { (byte) -1 }; }
    public  static  byte[]  varbinary_bytes_bytes( Double a ) { return new byte[] { (byte) -1 }; }
    public  static  byte[]  varbinary_bytes_bytes( BigDecimal a ) { return new byte[] { (byte) -1 }; }
    public  static  byte[]  varbinary_bytes_bytes( String a ) { return new byte[] { (byte) -1 }; }
    public  static  byte[]  varbinary_bytes_bytes( Object a ) { return new byte[] { (byte) -1 }; }

    public  static  byte[]  varbinary_bytes_int( int a ) { return new byte[] { (byte) a }; }

    // bad return type
    public  static  int  varbinary_badreturn_bytes_bytes( byte[] a ) { return -1; }
    

    //
    // LONGVARBINARY
    //
    
    // legal resolutions

    public  static  byte[]  longvarbinary_bytes_bytes( byte[] a ) { return a; }
    public  static  byte[]  longvarbinary_bytes_bytes( byte a ) { return new byte[] { (byte) -1 }; }
    public  static  byte[]  longvarbinary_bytes_bytes( int a ) { return new byte[] { (byte) -1 }; }
    public  static  byte[]  longvarbinary_bytes_bytes( long a ) { return new byte[] { (byte) -1 }; }
    public  static  byte[]  longvarbinary_bytes_bytes( float a ) { return new byte[] { (byte) -1 }; }
    public  static  byte[]  longvarbinary_bytes_bytes( double a ) { return new byte[] { (byte) -1 }; }
    public  static  byte[]  longvarbinary_bytes_bytes( Byte a ) { return new byte[] { (byte) -1 }; }
    public  static  byte[]  longvarbinary_bytes_bytes( Short a ) { return new byte[] { (byte) -1 }; }
    public  static  byte[]  longvarbinary_bytes_bytes( Long a ) { return new byte[] { (byte) -1 }; }
    public  static  byte[]  longvarbinary_bytes_bytes( Float a ) { return new byte[] { (byte) -1 }; }
    public  static  byte[]  longvarbinary_bytes_bytes( Double a ) { return new byte[] { (byte) -1 }; }
    public  static  byte[]  longvarbinary_bytes_bytes( BigDecimal a ) { return new byte[] { (byte) -1 }; }
    public  static  byte[]  longvarbinary_bytes_bytes( String a ) { return new byte[] { (byte) -1 }; }
    public  static  byte[]  longvarbinary_bytes_bytes( Object a ) { return new byte[] { (byte) -1 }; }

    public  static  byte[]  longvarbinary_bytes_int( int a ) { return new byte[] { (byte) a }; }

    // bad return type
    public  static  int  longvarbinary_badreturn_bytes_bytes( byte[] a ) { return -1; }
}
