/*

Derby - Class org.apache.derbyDemo.vtis.core.StringColumnVTI

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

package org.apache.derbyDemo.vtis.core;

import java.io.*;
import java.math.BigDecimal;
import java.sql.*;
import java.text.DateFormat;
import java.text.ParseException;

/**
 * <p>
 * This is an abstract VTI which assumes that all columns are strings and which
 * coerces the strings to reasonable values for various getXXX()
 * methods. Subclasses must implement the following ResultSet methods:
 * </p>
 *
 * <ul>
 * <li>next( )</li>
 * <li>close()</li>
 * <li>getMetaData()</li>
 * </ul>
 *
 * <p>
 * and the following protected methods introduced by this class:
 * </p>
 *
 * <ul>
 * <li>getRawColumn( int columnNumber )</li>
 * </ul>
 */
public  abstract    class   StringColumnVTI extends VTITemplate
{
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTANTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // INNER CLASSES
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * A crude Blob implementation.
     * </p>
     */
    public	static	final	class	SimpleBlob	implements	Blob
    {
        private	byte[]	_bytes;
        
        public	SimpleBlob( byte[] bytes )
        {
            _bytes = bytes;
        }
        
        public	InputStream	getBinaryStream()
        {
            return new ByteArrayInputStream( _bytes );
        }
        
        public	byte[]	getBytes( long position, int length ) { return _bytes; }
        
        public	long	length()
        {
            if ( _bytes == null ) { return 0L; }
            return (long) _bytes.length;
        }
        
        public	long	position( Blob pattern, long start ) { return 0L; }
        public	long	position( byte[] pattern, long start ) { return 0L; }
        
        public	boolean	equals( Object other )
        {
            if ( other == null ) { return false; }
            if ( !( other instanceof Blob ) ) { return false; }
            
            Blob	that = (Blob) other;
            
            try {
                if ( this.length() != that.length() ) { return false; }
                
                InputStream	thisStream = this.getBinaryStream();
                InputStream	thatStream = that.getBinaryStream();
                
                while( true )
                {
                    int		nextByte = thisStream.read();
                    
                    if ( nextByte < 0 ) { break; }
                    if ( nextByte != thatStream.read() ) { return false; }
                }
            }
            catch (Exception e)
            {
                System.err.println( e.getMessage() );
                e.printStackTrace();
                return false;
            }
            
            return true;
        }
        
        public int setBytes(long arg0, byte[] arg1) throws SQLException {
            throw new SQLException("not implemented");
        }
        
        public int setBytes(long arg0, byte[] arg1, int arg2, int arg3) throws SQLException {
            throw new SQLException("not implemented");
        }

        public OutputStream setBinaryStream(long arg0) throws SQLException {
            throw new SQLException("not implemented");
        }

        public void truncate(long arg0) throws SQLException {
            throw new SQLException("not implemented");
        }
    }
    
    /**
     * <p>
     * A crude Clob implementation.
     * </p>
     */
    public	static	final	class	SimpleClob	implements	Clob
    {
        private	String	_contents;

        public	SimpleClob( String contents )
        {
            _contents = contents;
        }
        
        public	InputStream	getAsciiStream()
        {
            try {
                return new ByteArrayInputStream( _contents.getBytes( "UTF-8" ) );
            }
            catch (Exception e) { return null; }
        }
        
        public	Reader	getCharacterStream()
        {
            return new CharArrayReader( _contents.toCharArray() );
        }
        
        public	String	getSubString( long position, int length )
        {
            return _contents.substring( (int) position, length );
        }
		
        public	long	length()
        {
            if ( _contents == null ) { return 0L; }
            return (long) _contents.length();
        }
        
        public	long	position( Clob searchstr, long start ) { return 0L; }
        public	long	position( String searchstr, long start ) { return 0L; }
        
        public	boolean	equals( Object other )
        {
            if ( other == null ) { return false; }
            if ( !( other instanceof Clob ) ) { return false; }
            
            Clob	that = (Clob) other;
            
            try {
                if ( this.length() != that.length() ) { return false; }
                
                InputStream	thisStream = this.getAsciiStream();
                InputStream	thatStream = that.getAsciiStream();
                
                while( true )
                {
                    int		nextByte = thisStream.read();
                    
                    if ( nextByte < 0 ) { break; }
                    if ( nextByte != thatStream.read() ) { return false; }
                }
            }
            catch (Exception e)
            {
                System.err.println( e.getMessage() );
                e.printStackTrace();
                return false;
            }
            
            return true;
        }
        
        public int setString(long arg0, String arg1) throws SQLException {
            throw new SQLException("not implemented");
        }
        
        public int setString(long arg0, String arg1, int arg2, int arg3) throws SQLException {
            throw new SQLException("not implemented");
        }

        public OutputStream setAsciiStream(long arg0) throws SQLException {
            throw new SQLException("not implemented");
        }

        public Writer setCharacterStream(long arg0) throws SQLException {
            throw new SQLException("not implemented");
        }

        public void truncate(long arg0) throws SQLException {
            throw new SQLException("not implemented");    
        }

    }
    
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // STATE
    //
    ///////////////////////////////////////////////////////////////////////////////////

    private String[]      _columnNames;
    private boolean _lastColumnWasNull;
    
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // StringColumnVTI BEHAVIOR TO BE IMPLEMENTED BY SUBCLASSES
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Get the string value of the column in the current row identified by the 1-based columnNumber.
     * </p>
     */
    protected  abstract    String  getRawColumn( int columnNumber ) throws SQLException;
    
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTRUCTORS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Build a StringColumnVTI with the given column names
     * </p>
     */
    public  StringColumnVTI( String[] columnNames )
    {
        _columnNames = columnNames;
    }
    
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // ResultSet BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

    public boolean wasNull() throws SQLException { return _lastColumnWasNull; }
    
    public int findColumn( String columnName ) throws SQLException
    {
        int     count = _columnNames.length;

        for ( int i = 0; i < count; i++ ) { if ( _columnNames[ i ].equals( columnName ) ) { return i+1; } }

        throw new SQLException( "Unknown column name." );
    }
    
    public String getString(int columnIndex) throws SQLException
    {
        String  columnValue = getRawColumn( columnIndex );

        checkNull( columnValue );

        return columnValue;
    }
    
    public boolean getBoolean(int columnIndex) throws SQLException
    {
        String  columnValue = getString( columnIndex );

        if ( columnValue == null ) { return false; }
        else { return Boolean.valueOf( columnValue ).booleanValue(); }
    }

    public byte getByte(int columnIndex) throws SQLException
    {
        String  columnValue = getString( columnIndex );

        if ( columnValue == null ) { return (byte) 0; }
        else
        {
            try {
                return Byte.valueOf( columnValue ).byteValue();
            } catch (NumberFormatException e) { throw wrap( e ); }
        }
    }

    public short getShort(int columnIndex) throws SQLException
    {
        String  columnValue = getString( columnIndex );

        if ( columnValue == null ) { return (short) 0; }
        else
        {
            try {
                return Short.valueOf( columnValue ).shortValue();
            } catch (NumberFormatException e) { throw wrap( e ); }
        }
    }

    public int getInt(int columnIndex) throws SQLException
    {
        String  columnValue = getString( columnIndex );

        if ( columnValue == null ) { return 0; }
        else
        {
            try {
                return Integer.valueOf( columnValue ).intValue();
            } catch (NumberFormatException e) { throw wrap( e ); }
        }
    }

    public long getLong(int columnIndex) throws SQLException
    {
        String  columnValue = getString( columnIndex );

        if ( columnValue == null ) { return (long) 0; }
        else
        {
            try {
                return Long.valueOf( columnValue ).longValue();
            } catch (NumberFormatException e) { throw wrap( e ); }
        }
    }

   public float getFloat(int columnIndex) throws SQLException
   {
        String  columnValue = getString( columnIndex );

        if ( columnValue == null ) { return (float) 0; }
        else
        {
            try {
                return Float.valueOf( columnValue ).floatValue();
            } catch (NumberFormatException e) { throw wrap( e ); }
        }
    }

    public double getDouble(int columnIndex) throws SQLException
    {
        String  columnValue = getString( columnIndex );

        if ( columnValue == null ) { return (double) 0; }
        else
        {
            try {
                return Double.valueOf( columnValue ).doubleValue();
            } catch (NumberFormatException e) { throw wrap( e ); }
        }
    }

    public BigDecimal getBigDecimal(int columnIndex) throws SQLException
    {
        String  columnValue = getString( columnIndex );

        if ( columnValue == null ) { return null; }
        else
        {
            try {
                return new BigDecimal( columnValue );
            } catch (NumberFormatException e) { throw wrap( e ); }
        }
    }

    public byte[] getBytes(int columnIndex) throws SQLException
    {
        String  columnValue = getString( columnIndex );

        if ( columnValue == null ) { return null; }
        else
        {
            return columnValue.getBytes();
        }
    }

    public java.sql.Date getDate(int columnIndex) throws SQLException
    {
        String  columnValue = getString( columnIndex );

        if ( columnValue == null ) { return null; }
        else
        {
            return new Date( parseDateTime( columnValue ) );
        }
    }

    public java.sql.Time getTime(int columnIndex) throws SQLException
    {
        String  columnValue = getString( columnIndex );

        if ( columnValue == null ) { return null; }
        else
        {
            return new Time( parseDateTime( columnValue ) );
        }
    }

    public java.sql.Timestamp getTimestamp(int columnIndex) throws SQLException
    {
        String  columnValue = getString( columnIndex );

        if ( columnValue == null ) { return null; }
        else
        {
            return new Timestamp( parseDateTime( columnValue ) );
        }
    }

    public InputStream getAsciiStream(int columnIndex) throws SQLException
    {
        String  columnValue = getString( columnIndex );

        return getEncodedStream( columnValue, "US-ASCII" );
    }

    public java.io.InputStream getBinaryStream(int columnIndex)
        throws SQLException
    {
        String  columnValue = getString( columnIndex );

        if ( columnValue == null ) { return null; }
        else { return new ByteArrayInputStream( getBytes( columnIndex ) ); }
    }

    public Blob getBlob(int columnIndex)
        throws SQLException
    {
        String  columnValue = getString( columnIndex );

        if ( columnValue == null ) { return null; }
        else { return new SimpleBlob( getBytes( columnIndex ) ); }
    }
    
    public Clob getClob(int columnIndex)
        throws SQLException
    {
        String  columnValue = getString( columnIndex );

        if ( columnValue == null ) { return null; }
        { return new SimpleClob( getString( columnIndex ) ); }
    }
    
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // PROTECTED BEHAVIOR USED BY SUBCLASSES
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Return the names of the columns.
     * </p>
     */
    protected   String[]    getColumnNames( )
    {
        return _columnNames;
    }
    
    /**
     * <p>
     * Set the wasNull flag.
     * </p>
     */
    protected   void setWasNull()
    {
        _lastColumnWasNull = true;
    }
    
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // PRIVATE MINIONS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Set the wasNull flag based on whether this column value turned out to be null.
     * </p>
     */
    private void checkNull( String columnValue )
    {
        _lastColumnWasNull = ( columnValue == null );
    }
    
    /**
     * <p>
     * Wrap an exception in a SQLException.
     * </p>
     */
    private SQLException wrap( Throwable t )
    {
        return new SQLException( t.getMessage() );
    }
    
    /**
     * <p>
     * Translate a date/time expression into the corresponding long number of
     * milliseconds.
     * </p>
     */
    private long parseDateTime( String columnValue  )
        throws SQLException
    {
        try {
            DateFormat      df = DateFormat.getDateTimeInstance();
                
            java.util.Date  rawDate = df.parse( columnValue );

            return rawDate.getTime();
        } catch (ParseException e) { throw wrap( e ); }
    }
    
    /**
     * <p>
     * Turn a string into an appropriately encoded ByteArrayInputStream.
     * </p>
     */
    private InputStream getEncodedStream( String columnValue, String encoding  )
        throws SQLException
    {
        if ( columnValue == null ) { return null; }
        else
        {
            try {
                byte[]      rawBytes = columnValue.getBytes( encoding );
            
                return new ByteArrayInputStream( rawBytes );
            } catch (UnsupportedEncodingException e) { throw wrap( e ); }
        }
    }

}
