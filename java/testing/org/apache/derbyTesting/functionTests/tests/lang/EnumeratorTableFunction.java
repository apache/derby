/*

Derby - Class org.apache.derbyTesting.functionTests.tests.lang.EnumeratorTableFunction

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

import java.sql.SQLException;
import java.lang.reflect.Array;
import java.util.Enumeration;
import java.util.Iterator;

import org.apache.derby.vti.StringColumnVTI;

/**
 * <p>
 * Abstract Table Function which lists out all of the objects in an
 * Enumeration, Iterator, Iterable, or array. To extend this class, you must
 * implement the following method, along with a constructor and a public
 * static method which returns an instance of your class:
 * </p>
 *
 * <ul>
 * <li>{@link #makeRow(java.lang.Object) makeRow()}</li>
 * </ul>
 */
public abstract class EnumeratorTableFunction extends StringColumnVTI
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

    // iterator for looping through the collection
    private Enumeration  _enumeration;
    
    // the columns
    protected String[]    _row;

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTRUCTORS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Construct from a collection and the column names for the rows
     * in the collection. The collection could be an Enumeration, an Iterator,
     * an Iterable, or an array.
     * </p>
     */
    public  EnumeratorTableFunction( String[] columnNames, Object collection )
        throws SQLException
    {
        super( columnNames );

        setEnumeration( collection );
    }
    
    /**
     * <p>
     * This constructor is called by subclasses which have custom logic for
     * creating an Enumeration. Those constructors are responsible for calling
     * setEnumeration() themselves.
     * </p>
     */
    protected   EnumeratorTableFunction( String[] columnNames )
    {
        super( columnNames );

        // here you would call setEnumeration()
    }
    
    ///////////////////////////////////////////////////////////////////////////////////
    //
    //  ACCESSORS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Set the Enumeration which we loop through. This method should be called
     * by constructors. As a side-effect, this method also creates the empty row
     * which buffers result columns.
     * </p>
     */
    protected   void    setEnumeration( Object collection )
        throws SQLException
    {
        if ( collection == null )
        {
            throw new SQLException( "Bad argument. Null collections not allowed." );
        }

        if ( collection instanceof Enumeration ) { _enumeration = (Enumeration) collection; }
        else if ( collection instanceof Iterator ) { _enumeration = new Enumerator( (Iterator) collection ); }
        else if ( collection instanceof Iterable ) { _enumeration = new Enumerator( ((Iterable) collection).iterator() ); }
        else if ( collection.getClass().isArray() ) { _enumeration = new ArrayEnumerator( collection ); }
        else
        {
            throw new SQLException( "Bad argument. Argument must be an Enumeration, Iterator, Iterable, or array." );
        }
        
        _row = new String[ getColumnCount() ];
    }
    
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // ABSTRACT BEHAVIOR TO BE IMPLEMENTED BY CHILDREN
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * This turns an object in the Enumeration into a row. Each cell in the
     * returned array is a column in the row.
     * </p>
     */
    public  abstract    String[]    makeRow( Object obj ) throws SQLException;
    
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // StringColumnTableFunction BEHAVIOR TO BE IMPLEMENTED BY SUBCLASSES
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Get the string value of the column in the current row identified by the 1-based columnNumber.
     * </p>
     */
    protected  String  getRawColumn( int columnNumber ) throws SQLException
    {
        return _row[ columnNumber - 1 ];
    }
    
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // ResultSet BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * This method advances to the next Object in the collection. This method
     * calls {@link #makeRow(java.lang.Object) makeRow()} in order to turn the Object into a
     * row. Subsequent calls to the <i>getXXX</i>() methods will return
     * individual columns in that row.
     * </p>
     */
    public  boolean next()
        throws SQLException
    {
        if ( _enumeration == null ) { return false; }
        if ( !_enumeration.hasMoreElements() ) { return false; }

        Object  obj = _enumeration.nextElement();

        _row = makeRow( obj );

        return true;
    }

    public  void    close()
    {
        _enumeration = null;
        _row = null;
    }
    
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // MINIONS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // INNER CLASSES
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Enumeration which wraps an Iterator.
     * </p>
     */
    public  final   class   Enumerator  implements  Enumeration
    {
        private Iterator    _iterator;

        public  Enumerator( Iterator iterator ) { _iterator = iterator; }

        public  boolean hasMoreElements()   { return _iterator.hasNext(); }
        public  Object  nextElement() { return _iterator.next(); }
    }
    
    /**
     * <p>
     * Enumeration which wraps an array and throws away null cells.
     * </p>
     */
    public  final   class   ArrayEnumerator   implements  Enumeration
    {
        Object      _array;
        int             _length;
        int             _idx;
        Object      _nextObject;

        public  ArrayEnumerator( Object array )
        {
            _array = array;
            _length = Array.getLength( array );
            _idx = 0;

            advance();
        }

        public  boolean hasMoreElements()
        {
            return ( _nextObject != null );
        }

        public  Object  nextElement()
        {
            Object  result = _nextObject;

            advance();
            
            return result;
        }

        // flush nulls
        private void    advance()
        {
            while( true )
            {
                if ( _idx >= _length )
                {
                    _nextObject = null;
                    break;
                }
                
                _nextObject = Array.get( _array, _idx++ );

                if ( _nextObject != null ) { break; }
            }
        }
        
    }

}
