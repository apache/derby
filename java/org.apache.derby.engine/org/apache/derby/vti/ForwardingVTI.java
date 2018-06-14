/*

   Derby - Class org.apache.derby.vti.ForwardingVTI

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

package org.apache.derby.vti;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

import org.apache.derby.shared.common.util.ArrayUtil;
import org.apache.derby.iapi.util.IdUtil;

/**
 * <p>
 * This class contains a table function which forwards its behavior to
 * another ResultSet wrapped inside it.
 * </p>
 */
public	class   ForwardingVTI extends VTITemplate
{
    ////////////////////////////////////////////////////////////////////////
    //
    //	CONSTANTS
    //
    ////////////////////////////////////////////////////////////////////////

    ////////////////////////////////////////////////////////////////////////
    //
    //	STATE
    //
    ////////////////////////////////////////////////////////////////////////

    private ResultSet           _wrappedResultSet;

    ////////////////////////////////////////////////////////////////////////
    //
    //	CONSTRUCTOR
    //
    ////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Construct from another ResultSet.
     * </p>
     */
    public  ForwardingVTI() { super(); }

    ////////////////////////////////////////////////////////////////////////
    //
    //	SUPPORT FUNCTIONS
    //
    ////////////////////////////////////////////////////////////////////////

    /**
     * Poke in another ResultSet to which we forward method calls.
     *
     * @param wrappedResultSet ResultSet to which to forward method calls
     */
    public  final   void    wrapResultSet( ResultSet wrappedResultSet ) { _wrappedResultSet = wrappedResultSet; }

    /**
     * Get the wrapped ResultSet.
     *
     * @return the wrapped ResultSet
     */
    public  final   ResultSet   getWrappedResultSet() { return _wrappedResultSet; }

    /**
     * This overridable method maps the ForwardVTI's column numbers to those of the wrapped ResultSet
     *
     * @param ourColumnNumber Number of column in this VTI
     *
     * @return the corresponding number of the column in the wrapped ResultSet
     */
    protected int mapColumnNumber( int ourColumnNumber )    { return ourColumnNumber; }

    ////////////////////////////////////////////////////////////////////////
    //
    //	ResultSet BEHAVIOR
    //
    ////////////////////////////////////////////////////////////////////////

    public  void    close() throws SQLException { _wrappedResultSet.close(); }

    public  boolean next()  throws SQLException { return _wrappedResultSet.next(); }

    public boolean isClosed() throws SQLException { return _wrappedResultSet.isClosed(); }

    public  boolean wasNull()   throws SQLException
    { return _wrappedResultSet.wasNull(); }

    public  ResultSetMetaData   getMetaData()   throws SQLException
    { return _wrappedResultSet.getMetaData(); }

    public  InputStream 	getAsciiStream(int i) throws SQLException
    { return _wrappedResultSet.getAsciiStream( mapColumnNumber( i ) ); }
    
    public  BigDecimal 	getBigDecimal(int i) throws SQLException
    { return _wrappedResultSet.getBigDecimal( mapColumnNumber( i ) ); }

    @Deprecated
    public  BigDecimal 	getBigDecimal(int i, int scale) throws SQLException
    { return _wrappedResultSet.getBigDecimal( mapColumnNumber( i ), scale ); }
    
    public  InputStream 	getBinaryStream(int i)  throws SQLException
    { return _wrappedResultSet.getBinaryStream( mapColumnNumber( i ) ); }
    
    public  Blob 	getBlob(int i)  throws SQLException
    { return _wrappedResultSet.getBlob( mapColumnNumber( i ) ); }
    
    public  boolean 	getBoolean(int i) throws SQLException
    { return _wrappedResultSet.getBoolean( mapColumnNumber( i ) ); }
    
    public  byte 	getByte(int i)    throws SQLException
    { return _wrappedResultSet.getByte( mapColumnNumber( i ) ); }
    
    public  byte[] 	getBytes(int i) throws SQLException
    { return _wrappedResultSet.getBytes( mapColumnNumber( i ) ); }
    
    public  Reader 	getCharacterStream(int i) throws SQLException
    { return _wrappedResultSet.getCharacterStream( mapColumnNumber( i ) ); }

    public  Clob 	getClob(int i)  throws SQLException
    { return _wrappedResultSet.getClob( mapColumnNumber( i ) ); }

    public  Date 	getDate(int i)  throws SQLException
    { return _wrappedResultSet.getDate( mapColumnNumber( i ) ); }

    public  Date 	getDate(int i, Calendar cal)    throws SQLException
    { return _wrappedResultSet.getDate( mapColumnNumber( i ), cal ); }

    public  double 	getDouble(int i)    throws SQLException
    { return _wrappedResultSet.getDouble( mapColumnNumber( i ) ); }

    public  float 	getFloat(int i) throws SQLException
    { return _wrappedResultSet.getFloat( mapColumnNumber( i ) ); }

    public  int 	getInt(int i)   throws SQLException
    { return _wrappedResultSet.getInt( mapColumnNumber( i ) ); }

    public  long 	getLong(int i)  throws SQLException
    { return _wrappedResultSet.getLong( mapColumnNumber( i ) ); }

    public  Object 	getObject(int i)    throws SQLException
    { return _wrappedResultSet.getObject( mapColumnNumber( i ) ); }

    public  short 	getShort(int i) throws SQLException
    { return _wrappedResultSet.getShort( mapColumnNumber( i ) ); }

    public  String 	getString(int i)    throws SQLException
    { return _wrappedResultSet.getString( mapColumnNumber( i ) ); }

    public  Time 	getTime(int i)  throws SQLException
    { return _wrappedResultSet.getTime( mapColumnNumber( i ) ); }

    public  Time 	getTime(int i, Calendar cal)    throws SQLException
    { return _wrappedResultSet.getTime( mapColumnNumber( i ), cal ); }

    public  Timestamp 	getTimestamp(int i) throws SQLException
    { return _wrappedResultSet.getTimestamp( mapColumnNumber( i ) ); }

    public  Timestamp 	getTimestamp(int i, Calendar cal)   throws SQLException
    { return _wrappedResultSet.getTimestamp( mapColumnNumber( i ), cal ); }

}
