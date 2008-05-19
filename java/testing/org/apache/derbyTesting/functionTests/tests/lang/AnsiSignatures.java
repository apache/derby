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


/**
 * <p>
 * These are methods for testing ANSI routine resolution. The resolution rules
 * are described in DERBY-3652.
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
    public  static  short   smallint_short_Integer( Integer a ) { return a.shortValue(); }
    public  static  Integer smallint_Integer_short( short a ) { return new Integer( a ); }
    public  static  Integer smallint_Integer_Integer( Integer a ) { return a; }
    
    // outside the spec. these should not resolve.
    public  static  short   smallint_bad_short_Short( Short a ) { return a.shortValue(); }
    public  static  Short   smallint_bad_Short_short( short a ) { return new Short( a ); }
    public  static  Short   smallint_bad_Short_Short( Short a ) { return a; }

    
    // illegal ambiguity
    public  static  short   smallint_amb_short_short( short a ) { return a; }
    public  static  short   smallint_amb_short_short( Integer a ) { return a.shortValue(); }
    public  static  Integer smallint_amb_Integer_short( short a ) { return new Integer( a ); }
    public  static  Integer smallint_amb_Integer_short( Integer a ) { return a; }
    
    // unresolvable
    public  static  short   smallint_unres_short( byte a ) { return (short) a; }
    public  static  short   smallint_unres_short( int a ) { return (short) a; }
    public  static  short   smallint_unres_short( long a ) { return (short) a; }
    public  static  short   smallint_unres_short( float a ) { return (short) a; }
    public  static  short   smallint_unres_short( double a ) { return (short) a; }
    public  static  short   smallint_unres_short( Byte a ) { return a.shortValue(); }
    public  static  short   smallint_unres_short( Short a ) { return a.shortValue(); }
    public  static  short   smallint_unres_short( Long a ) { return a.shortValue(); }
    public  static  short   smallint_unres_short( Float a ) { return a.shortValue(); }
    public  static  short   smallint_unres_short( Double a ) { return a.shortValue(); }

    public  static  Short   smallint_unres_Short( byte a ) { return new Short( (short) a ); }
    public  static  Short   smallint_unres_Short( short a ) { return new Short( (short) a ); }
    public  static  Short   smallint_unres_Short( int a ) { return new Short( (short) a ); }
    public  static  Short   smallint_unres_Short( long a ) { return new Short( (short) a ); }
    public  static  Short   smallint_unres_Short( float a ) { return new Short( (short) a ); }
    public  static  Short   smallint_unres_Short( double a ) { return new Short( (short) a ); }
    public  static  Short   smallint_unres_Short( Byte a ) { return new Short( a.shortValue() ); }
    public  static  Short   smallint_unres_Short( Short a ) { return new Short( a.shortValue() ); }
    public  static  Short   smallint_unres_Short( Integer a ) { return new Short( a.shortValue() ); }
    public  static  Short   smallint_unres_Short( Long a ) { return new Short( a.shortValue() ); }
    public  static  Short   smallint_unres_Short( Float a ) { return new Short( a.shortValue() ); }
    public  static  Short   smallint_unres_Short( Double a ) { return new Short( a.shortValue() ); }



    //
    // INTEGER
    //
    
    // legal resolutions
    public  static  int   integer_int_int( int a ) { return a; }
    public  static  int   integer_int_Integer( Integer a ) { return a.intValue(); }
    public  static  Integer integer_Integer_int( int a ) { return new Integer( a ); }
    public  static  Integer integer_Integer_Integer( Integer a ) { return a; }
    
    // illegal ambiguity
    public  static  int   integer_amb_int_int( int a ) { return a; }
    public  static  int   integer_amb_int_int( Integer a ) { return a.intValue(); }
    public  static  Integer integer_amb_Integer_int( int a ) { return new Integer( a ); }
    public  static  Integer integer_amb_Integer_int( Integer a ) { return a; }
    
    // unresolvable
    public  static  int   integer_unres_int( byte a ) { return (int) a; }
    public  static  int   integer_unres_int( short a ) { return (int) a; }
    public  static  int   integer_unres_int( long a ) { return (int) a; }
    public  static  int   integer_unres_int( float a ) { return (int) a; }
    public  static  int   integer_unres_int( double a ) { return (int) a; }
    public  static  int   integer_unres_int( Byte a ) { return a.intValue(); }
    public  static  int   integer_unres_int( Short a ) { return a.intValue(); }
    public  static  int   integer_unres_int( Long a ) { return a.intValue(); }
    public  static  int   integer_unres_int( Float a ) { return a.intValue(); }
    public  static  int   integer_unres_int( Double a ) { return a.intValue(); }

    public  static  Integer   integer_unres_Integer( byte a ) { return new Integer( (int) a ); }
    public  static  Integer   integer_unres_Integer( short a ) { return new Integer( (int) a ); }
    public  static  Integer   integer_unres_Integer( long a ) { return new Integer( (int) a ); }
    public  static  Integer   integer_unres_Integer( float a ) { return new Integer( (int) a ); }
    public  static  Integer   integer_unres_Integer( double a ) { return new Integer( (int) a ); }
    public  static  Integer   integer_unres_Integer( Byte a ) { return new Integer( a.intValue() ); }
    public  static  Integer   integer_unres_Integer( Short a ) { return new Integer( a.intValue() ); }
    public  static  Integer   integer_unres_Integer( Long a ) { return new Integer( a.intValue() ); }
    public  static  Integer   integer_unres_Integer( Float a ) { return new Integer( a.intValue() ); }
    public  static  Integer   integer_unres_Integer( Double a ) { return new Integer( a.intValue() ); }

    //
    // BIGINT
    //
    
    // legal resolutions
    public  static  long   bigint_long_long( long a ) { return a; }
    public  static  long   bigint_long_Long( Long a ) { return a.longValue(); }
    public  static  Long bigint_Long_long( long a ) { return new Long( a ); }
    public  static  Long bigint_Long_Long( Long a ) { return a; }
    
    // illegal ambiguity
    public  static  long   bigint_amb_long_long( long a ) { return a; }
    public  static  long   bigint_amb_long_long( Long a ) { return a.longValue(); }
    public  static  Long bigint_amb_Long_long( long a ) { return new Long( a ); }
    public  static  Long bigint_amb_Long_long( Long a ) { return a; }
    
    // unresolvable
    public  static  long   bigint_unres_long( byte a ) { return (long) a; }
    public  static  long   bigint_unres_long( short a ) { return (long) a; }
    public  static  long   bigint_unres_long( int a ) { return (long) a; }
    public  static  long   bigint_unres_long( float a ) { return (long) a; }
    public  static  long   bigint_unres_long( double a ) { return (long) a; }
    public  static  long   bigint_unres_long( Byte a ) { return a.longValue(); }
    public  static  long   bigint_unres_long( Short a ) { return a.longValue(); }
    public  static  long   bigint_unres_long( Integer a ) { return a.longValue(); }
    public  static  long   bigint_unres_long( Float a ) { return a.longValue(); }
    public  static  long   bigint_unres_long( Double a ) { return a.longValue(); }

    public  static  Long   bigint_unres_Long( byte a ) { return new Long( (long) a ); }
    public  static  Long   bigint_unres_Long( short a ) { return new Long( (long) a ); }
    public  static  Long   bigint_unres_Long( int a ) { return new Long( (long) a ); }
    public  static  Long   bigint_unres_Long( float a ) { return new Long( (long) a ); }
    public  static  Long   bigint_unres_Long( double a ) { return new Long( (long) a ); }
    public  static  Long   bigint_unres_Long( Byte a ) { return new Long( a.longValue() ); }
    public  static  Long   bigint_unres_Long( Short a ) { return new Long( a.longValue() ); }
    public  static  Long   bigint_unres_Long( Integer a ) { return new Long( a.longValue() ); }
    public  static  Long   bigint_unres_Long( Float a ) { return new Long( a.longValue() ); }
    public  static  Long   bigint_unres_Long( Double a ) { return new Long( a.longValue() ); }
    
    //
    // REAL
    //
    
    // legal resolutions
    public  static  float   real_float_float( float a ) { return a; }
    public  static  float   real_float_Float( Float a ) { return a.floatValue(); }
    public  static  Float real_Float_float( float a ) { return new Float( a ); }
    public  static  Float real_Float_Float( Float a ) { return a; }
    
    // illegal ambiguity
    public  static  float   real_amb_float_float( float a ) { return a; }
    public  static  float   real_amb_float_float( Float a ) { return a.floatValue(); }
    public  static  Float real_amb_Float_float( float a ) { return new Float( a ); }
    public  static  Float real_amb_Float_float( Float a ) { return a; }
    
    // unresolvable
    public  static  float   real_unres_float( byte a ) { return (float) a; }
    public  static  float   real_unres_float( short a ) { return (float) a; }
    public  static  float   real_unres_float( int a ) { return (float) a; }
    public  static  float   real_unres_float( long a ) { return (float) a; }
    public  static  float   real_unres_float( double a ) { return (float) a; }
    public  static  float   real_unres_float( Byte a ) { return a.floatValue(); }
    public  static  float   real_unres_float( Short a ) { return a.floatValue(); }
    public  static  float   real_unres_float( Integer a ) { return a.floatValue(); }
    public  static  float   real_unres_float( Long a ) { return a.floatValue(); }
    public  static  float   real_unres_float( Double a ) { return a.floatValue(); }

    public  static  Float   real_unres_Float( byte a ) { return new Float( (float) a ); }
    public  static  Float   real_unres_Float( short a ) { return new Float( (float) a ); }
    public  static  Float   real_unres_Float( int a ) { return new Float( (float) a ); }
    public  static  Float   real_unres_Float( long a ) { return new Float( (float) a ); }
    public  static  Float   real_unres_Float( double a ) { return new Float( (float) a ); }
    public  static  Float   real_unres_Float( Byte a ) { return new Float( a.floatValue() ); }
    public  static  Float   real_unres_Float( Short a ) { return new Float( a.floatValue() ); }
    public  static  Float   real_unres_Float( Integer a ) { return new Float( a.floatValue() ); }
    public  static  Float   real_unres_Float( Long a ) { return new Float( a.floatValue() ); }
    public  static  Float   real_unres_Float( Double a ) { return new Float( a.floatValue() ); }

    //
    // DOUBLE
    //
    
    // legal resolutions
    public  static  double   double_double_double( double a ) { return a; }
    public  static  double   double_double_Double( Double a ) { return a.doubleValue(); }
    public  static  Double double_Double_double( double a ) { return new Double( a ); }
    public  static  Double double_Double_Double( Double a ) { return a; }
    
    // illegal ambiguity
    public  static  double   double_amb_double_double( double a ) { return a; }
    public  static  double   double_amb_double_double( Double a ) { return a.doubleValue(); }
    public  static  Double double_amb_Double_double( double a ) { return new Double( a ); }
    public  static  Double double_amb_Double_double( Double a ) { return a; }
    
    // unresolvable
    public  static  double   double_unres_double( byte a ) { return (double) a; }
    public  static  double   double_unres_double( short a ) { return (double) a; }
    public  static  double   double_unres_double( int a ) { return (double) a; }
    public  static  double   double_unres_double( long a ) { return (double) a; }
    public  static  double   double_unres_double( float a ) { return (double) a; }
    public  static  double   double_unres_double( Byte a ) { return a.doubleValue(); }
    public  static  double   double_unres_double( Short a ) { return a.doubleValue(); }
    public  static  double   double_unres_double( Integer a ) { return a.doubleValue(); }
    public  static  double   double_unres_double( Long a ) { return a.doubleValue(); }
    public  static  double   double_unres_double( Float a ) { return a.doubleValue(); }
 
    public  static  Double   double_unres_Double( byte a ) { return new Double( (double) a ); }
    public  static  Double   double_unres_Double( short a ) { return new Double( (double) a ); }
    public  static  Double   double_unres_Double( int a ) { return new Double( (double) a ); }
    public  static  Double   double_unres_Double( long a ) { return new Double( (double) a ); }
    public  static  Double   double_unres_Double( float a ) { return new Double( (double) a ); }
    public  static  Double   double_unres_Double( Byte a ) { return new Double( a.doubleValue() ); }
    public  static  Double   double_unres_Double( Short a ) { return new Double( a.doubleValue() ); }
    public  static  Double   double_unres_Double( Integer a ) { return new Double( a.doubleValue() ); }
    public  static  Double   double_unres_Double( Long a ) { return new Double( a.doubleValue() ); }
    public  static  Double   double_unres_Double( Float a ) { return new Double( a.doubleValue() ); }
 
    
}
