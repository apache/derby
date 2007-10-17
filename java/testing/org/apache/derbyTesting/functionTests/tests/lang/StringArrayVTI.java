/*

Derby - Class org.apache.derbyTesting.functionTests.tests.lang.StringArrayVTI

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

import  java.sql.*;

import org.apache.derby.vti.VTICosting;
import org.apache.derby.vti.VTIEnvironment;

/**
 * <p>
 * This is a concrete VTI which is prepopulated with rows which are just
 * arrays of string columns.
 * </p>
 */
public    class   StringArrayVTI  extends StringColumnVTI
{
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTANTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    public  static  final   double  FAKE_ROW_COUNT = 13.0;
    public  static  final   double  FAKE_INSTANTIATION_COST = 3149.0;
    
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // INNER CLASSES
    //
    ///////////////////////////////////////////////////////////////////////////////////

    //
    // Inner classes for testing VTICosting api.
    //
    public  static  class   MissingConstructor  extends StringArrayVTI  implements VTICosting
    {
        private  MissingConstructor( String[] columnNames, String[][] rows ) { super( columnNames, rows ); }

        public  static  ResultSet   dummyVTI()
        {
            return new StringArrayVTI( new String[] { "foo" }, new String[][] { { "bar" } } );
        }
        
        public  double  getEstimatedRowCount( VTIEnvironment env ) throws SQLException
        {
            return FAKE_ROW_COUNT;
        }
        
        public  double  getEstimatedCostPerInstantiation( VTIEnvironment env ) throws SQLException
        {
            return FAKE_INSTANTIATION_COST;
        }
        
        public  boolean supportsMultipleInstantiations( VTIEnvironment env ) throws SQLException
        {
            return false;
        }        
    }
    
    public  static  class   ZeroArgConstructorNotPublic    extends MissingConstructor
    {
        ZeroArgConstructorNotPublic()
        { super( new String[] { "foo" }, new String[][] { { "bar" } } ); }
    }
    
    public  static  class   ConstructorException    extends ZeroArgConstructorNotPublic
    {
        public  ConstructorException()
        {
            super();

            Object      shameOnYou = null;

            // trip over a null pointer exception
            shameOnYou.hashCode();
        }
    }
    
    public  static  class   GoodVTICosting    extends ZeroArgConstructorNotPublic
    {
        public  GoodVTICosting()
        {
            super();
        }
    }
    
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // STATE
    //
    ///////////////////////////////////////////////////////////////////////////////////

    private int             _rowIdx = -1;
    private String[][]      _rows;

    private static  StringBuffer    _callers;
    
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTRUCTORS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    public  StringArrayVTI( String[] columnNames, String[][] rows )
    {
        super( columnNames );

        _rows = rows;
    }
    
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // FUNCTIONS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * This SQL function returns the list of getXXX() calls made to the last
     * StringArrayVTI.
     * </p>
     */
    public  static  String  getXXXrecord()
    {
        if ( _callers == null ) { return null; }
        else { return _callers.toString(); }
    }
    
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // ABSTRACT StringColumn BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

    protected String  getRawColumn( int columnNumber ) throws SQLException
    {
        String                  callersCallerMethod = deduceGetXXXCaller();

        _callers.append( callersCallerMethod );
        _callers.append( ' ' );

        return  _rows[ _rowIdx ][ columnNumber - 1 ];
    }

    // The stack looks like this:
    //
    // getXXX()
    // getString()
    // getRawColumn()
    // deduceGetXXXCaller()
    //
    // Except if the actual getXXX() method is getString()
    //
    private String  deduceGetXXXCaller() throws SQLException
    {
        try {
            StackTraceElement[]     stack = (new Throwable()).getStackTrace();
            StackTraceElement       callersCaller = stack[ 3 ];
            String                  callersCallerMethod = callersCaller.getMethodName();

            if ( !callersCallerMethod.startsWith( "get" ) ) { callersCallerMethod = "getString"; }

            return  callersCallerMethod;
        } catch (Throwable t) { throw new SQLException( t.getMessage() ); }
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // ResultSet BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

    public  boolean next() throws SQLException
    {
        if ( (++_rowIdx) >= _rows.length ) { return false; }
        else
        {
            _callers = new StringBuffer();
            return true;
        }
    }

    public  void close() throws SQLException
    {}

    public  ResultSetMetaData   getMetaData() throws SQLException
    {
        throw new SQLException( "Not implemented." );
    }
    
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // MINIONS
    //
    ///////////////////////////////////////////////////////////////////////////////////



}

