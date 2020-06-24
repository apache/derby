/*

   Derby - Class org.apache.derby.vti.VTITemplate

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
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLInput;
import java.sql.SQLOutput;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Struct;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Map;

/**
	An abstract implementation of ResultSet that is useful
//IC see: https://issues.apache.org/jira/browse/DERBY-4932
	when writing table functions, read-only VTIs (virtual table interface), and
	the ResultSets returned by executeQuery in read-write VTI classes.
	
	This class implements most of the methods of the JDBC 4.0 interface java.sql.ResultSet,
	each one throwing a  SQLException with the name of the method. 
	A concrete subclass can then just implement the methods not implemented here 
	and override any methods it needs to implement for correct functionality.
	<P>
	The methods not implemented here are
	<UL>
	<LI>next()
	<LI>close()
	</UL>
	<P>

	For table functions and virtual tables, the database engine only calls methods defined
	in the JDBC 2.0 definition of java.sql.ResultSet.
 */
public abstract class VTITemplate   implements ResultSet, AwareVTI
{
    private VTIContext  _vtiContext;
    
    public  boolean 	isWrapperFor(Class<?> iface) throws SQLException { throw notImplemented( "isWrapperFor" ); }
    public  <T> T unwrap(Class<T> iface) throws SQLException { throw notImplemented( "unwrap" ); }

    public  ResultSetMetaData   getMetaData() throws SQLException { throw notImplemented( "getMetaData" ); }
    
    // If you implement findColumn() yourself, then the following overrides
    // mean that you only have to implement the getXXX(int) methods. You
    // don't have to also implement the getXXX(String) methods.
    public String getString(String columnName) throws SQLException { return getString(findColumn(columnName)); }
    public boolean getBoolean(String columnName) throws SQLException { return getBoolean(findColumn(columnName)); }
    public byte getByte(String columnName) throws SQLException { return getByte(findColumn(columnName)); }
    public short getShort(String columnName) throws SQLException { return getShort(findColumn(columnName)); }
    public int getInt(String columnName) throws SQLException { return getInt(findColumn(columnName)); }
    public long getLong(String columnName) throws SQLException { return getLong(findColumn(columnName)); }
    public float getFloat(String columnName) throws SQLException { return getFloat(findColumn(columnName)); }
    public double getDouble(String columnName) throws SQLException { return getDouble(findColumn(columnName)); }
    @Deprecated
    public BigDecimal getBigDecimal(String columnName, int scale) throws SQLException { return getBigDecimal(findColumn(columnName), scale); }
    public byte[] getBytes(String columnName) throws SQLException { return getBytes(findColumn(columnName)); }
    public java.sql.Date getDate(String columnName) throws SQLException { return getDate(findColumn(columnName)); }
    public java.sql.Time getTime(String columnName) throws SQLException { return getTime(findColumn(columnName)); }
    public java.sql.Timestamp getTimestamp(String columnName) throws SQLException { return getTimestamp(findColumn(columnName)); }
    public Object getObject(String columnName) throws SQLException { return getObject(findColumn(columnName)); }
    public <T> T getObject(String columnName, Class<T> type) throws SQLException { return getObject(findColumn(columnName), type); }
	public BigDecimal getBigDecimal(String columnName) throws SQLException { return getBigDecimal(findColumn(columnName)); }

    //
    // java.sql.ResultSet calls, passed through to our result set.
    //

    public boolean wasNull() throws SQLException { throw notImplemented( "wasNull" ); }
    public String getString(int columnIndex) throws SQLException { throw notImplemented( "getString" ); }
    public boolean getBoolean(int columnIndex) throws SQLException { throw notImplemented( "getBoolean" ); }
    public byte getByte(int columnIndex) throws SQLException { throw notImplemented( "getByte" ); }
    public short getShort(int columnIndex) throws SQLException { throw notImplemented( "getShort" ); }
    public int getInt(int columnIndex) throws SQLException { throw notImplemented( "getInt" ); }
    public long getLong(int columnIndex) throws SQLException { throw notImplemented( "getLong" ); }
    public float getFloat(int columnIndex) throws SQLException { throw notImplemented( "getFloat" ); }
    public double getDouble(int columnIndex) throws SQLException { throw notImplemented( "getDouble" ); }
    @Deprecated
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException { throw notImplemented( "getBigDecimal" ); }
    public byte[] getBytes(int columnIndex) throws SQLException { throw notImplemented( "] getBytes" ); }
    public java.sql.Date getDate(int columnIndex) throws SQLException { throw notImplemented( "sql.Date getDate" ); }
    public java.sql.Time getTime(int columnIndex) throws SQLException { throw notImplemented( "sql.Time getTime" ); }
    public java.sql.Timestamp getTimestamp(int columnIndex) throws SQLException { throw notImplemented( "sql.Timestamp getTimestamp" ); }
    public java.io.InputStream getAsciiStream(int columnIndex) throws SQLException { throw notImplemented( "io.InputStream getAsciiStream" ); }
    @Deprecated
    public java.io.InputStream getUnicodeStream(int columnIndex) throws SQLException { throw notImplemented( "io.InputStream getUnicodeStream" ); }
    public java.io.InputStream getBinaryStream(int columnIndex) throws SQLException { throw notImplemented( "io.InputStream getBinaryStream" ); }
    public java.io.InputStream getAsciiStream(String columnName) throws SQLException { throw notImplemented( "io.InputStream getAsciiStream" ); }
    @Deprecated
    public java.io.InputStream getUnicodeStream(String columnName) throws SQLException { throw notImplemented( "io.InputStream getUnicodeStream" ); }
    public java.io.InputStream getBinaryStream(String columnName) throws SQLException { throw notImplemented( "io.InputStream getBinaryStream" ); }
    public SQLWarning getWarnings() throws SQLException { return null; }
    public void clearWarnings() throws SQLException { throw notImplemented( "clearWarnings" ); }
    public String getCursorName() throws SQLException { throw notImplemented( "getCursorName" ); }
    public Object getObject(int columnIndex) throws SQLException { throw notImplemented( "getObject" ); }
    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException { throw notImplemented( "getObject" ); }
    public int findColumn(String columnName) throws SQLException { throw notImplemented( "findColumn" ); }
    public java.io.Reader getCharacterStream(int columnIndex) throws SQLException { throw notImplemented( "io.Reader getCharacterStream" ); }
    public java.io.Reader getCharacterStream(String columnName) throws SQLException { throw notImplemented( "io.Reader getCharacterStream" ); }
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException { throw notImplemented( "getBigDecimal" ); }
    public boolean isBeforeFirst() throws SQLException { throw notImplemented( "isBeforeFirst" ); }
    public boolean isAfterLast() throws SQLException { throw notImplemented( "isAfterLast" ); }
    public boolean isFirst() throws SQLException { throw notImplemented( "isFirst" ); }
    public boolean isLast() throws SQLException { throw notImplemented( "isLast" ); }
    public void beforeFirst() throws SQLException { throw notImplemented( "beforeFirst" ); }
    public void afterLast() throws SQLException { throw notImplemented( "afterLast" ); }
    public boolean first() throws SQLException { throw notImplemented( "first" ); }
    public boolean last() throws SQLException { throw notImplemented( "last" ); }
    public boolean isClosed() throws SQLException { throw notImplemented( "isClosed" ); }
    public int getHoldability() throws SQLException { throw notImplemented( "getHoldability" ); }
    public int getRow() throws SQLException { throw notImplemented( "getRow" ); }
    public boolean absolute(int row) throws SQLException { throw notImplemented( "absolute" ); }
    public boolean relative(int rows) throws SQLException { throw notImplemented( "relative" ); }
    public boolean previous() throws SQLException { throw notImplemented( "previous" ); }
    public void setFetchDirection(int direction) throws SQLException { throw notImplemented( "setFetchDirection" ); }
    public int getFetchDirection() throws SQLException { throw notImplemented( "getFetchDirection" ); }
    public void setFetchSize(int rows) throws SQLException { throw notImplemented( "setFetchSize" ); }
    public int getFetchSize() throws SQLException { throw notImplemented( "getFetchSize" ); }
    public int getType() throws SQLException { throw notImplemented( "getType" ); }
    public int getConcurrency() throws SQLException { throw notImplemented( "getConcurrency" ); }
    public boolean rowUpdated() throws SQLException { throw notImplemented( "rowUpdated" ); }
    public boolean rowInserted() throws SQLException { throw notImplemented( "rowInserted" ); }
    public boolean rowDeleted() throws SQLException { throw notImplemented( "rowDeleted" ); }
    public void updateNull(int columnIndex) throws SQLException { throw notImplemented( "updateNull" ); }
    public void updateBoolean(int columnIndex, boolean x) throws SQLException { throw notImplemented( "updateBoolean" ); }
    public void updateByte(int columnIndex, byte x) throws SQLException { throw notImplemented( "updateByte" ); }
    public void updateBytes(int columnIndex, byte[] x) throws SQLException { throw notImplemented( "updateBytes" ); }
    public void updateShort(int columnIndex, short x) throws SQLException { throw notImplemented( "updateShort" ); }
    public void updateInt(int columnIndex, int x) throws SQLException { throw notImplemented( "updateInt" ); }
    public void updateLong(int columnIndex, long x) throws SQLException { throw notImplemented( "updateLong" ); }
    public void updateFloat(int columnIndex, float x) throws SQLException { throw notImplemented( "updateFloat" ); }
    public void updateDouble(int columnIndex, double x) throws SQLException { throw notImplemented( "updateDouble" ); }
    public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException { throw notImplemented( "updateBigDecimal" ); }
    public void updateString(int columnIndex, String x) throws SQLException { throw notImplemented( "updateString" ); }
    public void updateDate(int columnIndex, java.sql.Date x) throws SQLException { throw notImplemented( "updateDate" ); }
    public void updateTime(int columnIndex, java.sql.Time x) throws SQLException { throw notImplemented( "updateTime" ); }
    public void updateTimestamp(int columnIndex, java.sql.Timestamp x) throws SQLException { throw notImplemented( "updateTimestamp" ); }
    public void updateAsciiStream(int columnIndex, InputStream x ) throws SQLException { throw notImplemented( "updateAsciiStream" ); }
    public void updateAsciiStream(int columnIndex, InputStream x, int length ) throws SQLException { throw notImplemented( "updateAsciiStream" ); }
    public void updateAsciiStream(int columnIndex, InputStream x, long length ) throws SQLException { throw notImplemented( "updateAsciiStream" ); }
	public void updateBinaryStream(int columnIndex, InputStream x)  throws SQLException { throw notImplemented( "updateBinaryStream" ); }
	public void updateBinaryStream(int columnIndex, InputStream x, int length)  throws SQLException { throw notImplemented( "updateBinaryStream" ); }
	public void updateBinaryStream(int columnIndex, InputStream x, long length)  throws SQLException { throw notImplemented( "updateBinaryStream" ); }
	public void updateCharacterStream(int columnIndex, java.io.Reader x ) throws SQLException { throw notImplemented( "updateCharacterStream" ); }
	public void updateCharacterStream(int columnIndex, java.io.Reader x, long length ) throws SQLException { throw notImplemented( "updateCharacterStream" ); }
	public void updateCharacterStream(int columnIndex, java.io.Reader x, int length ) throws SQLException { throw notImplemented( "updateCharacterStream" ); }
	public void updateObject(int columnIndex, Object x, int scale) throws SQLException { throw notImplemented( "updateObject" ); }
	public void updateObject(int columnIndex, Object x) throws SQLException { throw notImplemented( "updateObject" ); }
	public void updateNull(String columnName) throws SQLException { throw notImplemented( "updateNull" ); }
	public void updateBoolean(String columnName, boolean x) throws SQLException { throw notImplemented( "updateBoolean" ); }
	public void updateByte(String columnName, byte x) throws SQLException { throw notImplemented( "updateByte" ); }
	public void updateShort(String columnName, short x) throws SQLException { throw notImplemented( "updateShort" ); }
	public void updateInt(String columnName, int x) throws SQLException { throw notImplemented( "updateInt" ); }
	public void updateLong(String columnName, long x) throws SQLException { throw notImplemented( "updateLong" ); }
	public void updateFloat(String columnName, float x) throws SQLException { throw notImplemented( "updateFloat" ); }
	public void updateDouble(String columnName, double x) throws SQLException { throw notImplemented( "updateDouble" ); }
	public void updateBigDecimal(String columnName, BigDecimal x) throws SQLException { throw notImplemented( "updateBigDecimal" ); }
	public void updateString(String columnName, String x) throws SQLException { throw notImplemented( "updateString" ); }
	public void updateBytes(String columnName, byte[] x) throws SQLException { throw notImplemented( "updateBytes" ); }
	public void updateDate(String columnName, java.sql.Date x) throws SQLException { throw notImplemented( "updateDate" ); }
	public void updateTime(String columnName, java.sql.Time x) throws SQLException { throw notImplemented( "updateTime" ); }
	public void updateTimestamp(String columnName, java.sql.Timestamp x) throws SQLException { throw notImplemented( "updateTimestamp" ); }
	public void updateAsciiStream(String columnName, java.io.InputStream x) throws SQLException { throw notImplemented( "updateAsciiStream" ); }
	public void updateAsciiStream(String columnName, java.io.InputStream x, int length) throws SQLException { throw notImplemented( "updateAsciiStream" ); }
	public void updateAsciiStream(String columnName, java.io.InputStream x, long length) throws SQLException { throw notImplemented( "updateAsciiStream" ); }
	public void updateBinaryStream(String columnName, java.io.InputStream x) throws SQLException { throw notImplemented( "updateBinaryStream" ); }
	public void updateBinaryStream(String columnName, java.io.InputStream x, int length) throws SQLException { throw notImplemented( "updateBinaryStream" ); }
	public void updateBinaryStream(String columnName, java.io.InputStream x, long length) throws SQLException { throw notImplemented( "updateBinaryStream" ); }
	public void updateCharacterStream(String columnName, java.io.Reader x) throws SQLException { throw notImplemented( "updateCharacterStream" ); }
	public void updateCharacterStream(String columnName, java.io.Reader x, long length) throws SQLException { throw notImplemented( "updateCharacterStream" ); }
	public void updateCharacterStream(String columnName, java.io.Reader x, int length) throws SQLException { throw notImplemented( "updateCharacterStream" ); }
	public void updateObject(String columnName, Object x, int scale) throws SQLException { throw notImplemented( "updateObject" ); }
	public void updateObject(String columnName, Object x) throws SQLException { throw notImplemented( "updateObject" ); }
	public void insertRow() throws SQLException { throw notImplemented( "insertRow" ); }
	public void updateRow() throws SQLException { throw notImplemented( "updateRow" ); }
	public void deleteRow() throws SQLException { throw notImplemented( "deleteRow" ); }
	public void refreshRow() throws SQLException { throw notImplemented( "refreshRow" ); }
	public void cancelRowUpdates() throws SQLException { throw notImplemented( "cancelRowUpdates" ); }
	public void moveToInsertRow() throws SQLException { throw notImplemented( "moveToInsertRow" ); }
	public void moveToCurrentRow() throws SQLException { throw notImplemented( "moveToCurrentRow" ); }
	public Statement getStatement() throws SQLException { throw notImplemented( "getStatement" ); }
	public java.sql.Date getDate(int columnIndex, Calendar cal) throws SQLException { throw notImplemented( "sql.Date getDate" ); }
	public java.sql.Date getDate(String columnName, Calendar cal) throws SQLException { throw notImplemented( "sql.Date getDate" ); }
	public java.sql.Time getTime(int columnIndex, Calendar cal) throws SQLException { throw notImplemented( "sql.Time getTime" ); }
	public java.sql.Time getTime(String columnName, Calendar cal) throws SQLException { throw notImplemented( "sql.Time getTime" ); }
	public java.sql.Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException { throw notImplemented( "sql.Timestamp getTimestamp" ); }
	public java.sql.Timestamp getTimestamp(String columnName, Calendar cal) throws SQLException { throw notImplemented( "sql.Timestamp getTimestamp" ); }
	public URL getURL(int columnIndex) throws SQLException { throw notImplemented( "getURL" ); }
	public URL getURL(String columnName) throws SQLException { throw notImplemented( "getURL" ); }
	public Object getObject(int i, java.util.Map map) throws SQLException { throw notImplemented( "getObject" ); }
	public Ref getRef(int i) throws SQLException { throw notImplemented( "getRef" ); }
	public Blob getBlob(int i) throws SQLException { throw notImplemented( "getBlob" ); }
	public Clob getClob(int i) throws SQLException { throw notImplemented( "getClob" ); }
	public Array getArray(int i) throws SQLException { throw notImplemented( "getArray" ); }
	public Object getObject(String colName, java.util.Map map) throws SQLException { throw notImplemented( "getObject" ); }
	public Ref getRef(String colName) throws SQLException { throw notImplemented( "getRef" ); }
	public Blob getBlob(String colName) throws SQLException { throw notImplemented( "getBlob" ); }
	public Clob getClob(String colName) throws SQLException { throw notImplemented( "getClob" ); }
	public Array getArray(String colName) throws SQLException { throw notImplemented( "getArray" ); }
    public SQLXML getSQLXML(int columnIndex) throws SQLException { throw notImplemented( "getSQLXML" ); }
    public SQLXML getSQLXML(String columnLabel) throws SQLException { throw notImplemented( "getSQLXML" ); }
	public void updateRef(int columnIndex, Ref x) throws SQLException { throw notImplemented( "updateRef" ); }
	public void updateRef(String columnName, Ref x) throws SQLException { throw notImplemented( "updateRef" ); }
	public void updateBlob(int columnIndex, Blob x) throws SQLException { throw notImplemented( "updateBlob" ); }
	public void updateBlob(String columnName, Blob x) throws SQLException { throw notImplemented( "updateBlob" ); }
	public void updateBlob(int columnIndex, InputStream x) throws SQLException { throw notImplemented( "updateBlob" ); }
	public void updateBlob(int columnIndex, InputStream x, long pos) throws SQLException { throw notImplemented( "updateBlob" ); }
	public void updateBlob(String columnName, InputStream x) throws SQLException { throw notImplemented( "updateBlob" ); }
	public void updateBlob(String columnName, InputStream x, long pos) throws SQLException { throw notImplemented( "updateBlob" ); }
	public void updateClob(int columnIndex, Clob x) throws SQLException { throw notImplemented( "updateClob" ); }
	public void updateClob(String columnName, Clob x) throws SQLException { throw notImplemented( "updateClob" ); }
	public void updateClob(int columnIndex, Reader x) throws SQLException { throw notImplemented( "updateClob" ); }
	public void updateClob(int columnIndex, Reader x, long pos) throws SQLException { throw notImplemented( "updateClob" ); }
	public void updateClob(String columnName, Reader x) throws SQLException { throw notImplemented( "updateClob" ); }
	public void updateClob(String columnName, Reader x, long pos) throws SQLException { throw notImplemented( "updateClob" ); }
	public void updateArray(int columnIndex, Array x) throws SQLException { throw notImplemented( "updateArray" ); }
	public void updateArray(String columnName, Array x) throws SQLException { throw notImplemented( "updateArray" ); }

    public  Reader 	getNCharacterStream(int columnIndex) throws SQLException { throw notImplemented( "getNCharacterStream" ); }
    public  Reader 	getNCharacterStream(String columnLabel) throws SQLException { throw notImplemented( "getNCharacterStream" ); }
    public  NClob 	getNClob(int columnIndex) throws SQLException { throw notImplemented( "getNClob" ); }
    public  NClob 	getNClob(String columnLabel) throws SQLException { throw notImplemented( "getNClob" ); }
    public  String 	getNString(int columnIndex) throws SQLException { throw notImplemented( "getNString" ); }
    public  String 	getNString(String columnLabel) throws SQLException { throw notImplemented( "getNString" ); }
    public  RowId 	getRowId(int columnIndex) throws SQLException { throw notImplemented( "getRowId" ); }
    public  RowId 	getRowId(String columnLabel) throws SQLException { throw notImplemented( "getRowId" ); }
    
    public  void 	updateNCharacterStream(int columnIndex, Reader x) throws SQLException { throw notImplemented( "updateNCharacterStream" ); }
    public  void 	updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException { throw notImplemented( "updateNCharacterStream" ); }
    public  void 	updateNCharacterStream(String columnLabel, Reader reader) throws SQLException { throw notImplemented( "updateNCharacterStream" ); }
    public  void 	updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException { throw notImplemented( "updateNCharacterStream" ); }
    public  void 	updateNClob(int columnIndex, NClob nClob) throws SQLException { throw notImplemented( "updateNClob" ); }
    public  void 	updateNClob(int columnIndex, Reader reader) throws SQLException { throw notImplemented( "updateNClob" ); }
    public  void 	updateNClob(int columnIndex, Reader reader, long length) throws SQLException { throw notImplemented( "updateNClob" ); }
    public  void 	updateNClob(String columnLabel, NClob nClob) throws SQLException { throw notImplemented( "updateNClob" ); }
    public  void 	updateNClob(String columnLabel, Reader reader) throws SQLException { throw notImplemented( "updateNClob" ); }
    public  void 	updateNClob(String columnLabel, Reader reader, long length) throws SQLException { throw notImplemented( "updateNClob" ); }
    public  void 	updateNString(int columnIndex, String nString) throws SQLException { throw notImplemented( "updateNString" ); }
    public  void 	updateNString(String columnLabel, String nString) throws SQLException { throw notImplemented( "updateNString" ); }
    public  void 	updateRowId(int columnIndex, RowId x) throws SQLException { throw notImplemented( "updateRowId" ); }
    public  void 	updateRowId(String columnLabel, RowId x) throws SQLException { throw notImplemented( "updateRowId" ); }
    public  void 	updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException { throw notImplemented( "updateSQLXML" ); }
    public  void 	updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException { throw notImplemented( "updateSQLXML" ); }
    
    /////////////////////////////////////////////////////////////////////////
    //
    //  AwareVTI BEHAVIOR
    //
    /////////////////////////////////////////////////////////////////////////

    public  VTIContext  getContext() { return _vtiContext; }
    public  void    setContext( VTIContext context )    { _vtiContext = context; }

    /////////////////////////////////////////////////////////////////////////
    //
    //  MINIONS
    //
    /////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Create a SQLException saying that the calling method is not implemented.
     * </p>
     *
     * @param methodName Name of method
     *
     * @return a SQLFeatureNotSupportedException
     */
    protected SQLException    notImplemented( String methodName )
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-6213
        return new SQLFeatureNotSupportedException( "Unimplemented method: " + methodName );
    }
    
    /**
     * <p>
     * Get an array of descriptors for the return table shape declared for this
     * AwareVTI by its CREATE FUNCTION statement.
     * </p>
     *
     * @param currentConnection The current connection to Derby
     *
     * @return an array of descriptors of the columns returned by this VTI.
     * @throws SQLException on error
     */
    public  ColumnDescriptor[]  getReturnTableSignature( Connection currentConnection )
//IC see: https://issues.apache.org/jira/browse/DERBY-590
        throws SQLException
    {
        ArrayList<ColumnDescriptor> columns = new ArrayList<ColumnDescriptor>();
        VTIContext  context = getContext();
        String      schema = context.vtiSchema();
        String      function = context.vtiTable();
        ResultSet   rs = currentConnection.getMetaData().getFunctionColumns( null, schema, function, "%" );

        try {
            while ( rs.next() )
            {
                if ( rs.getInt( "COLUMN_TYPE" ) == DatabaseMetaData.functionColumnResult )
                {
                    ColumnDescriptor    cd = new ColumnDescriptor
                        (
                         rs.getString( "COLUMN_NAME" ),
                         rs.getInt( "DATA_TYPE" ),
                         rs.getInt( "PRECISION" ),
                         rs.getInt( "SCALE" ),
                         rs.getString( "TYPE_NAME" ),
                         rs.getInt( "ORDINAL_POSITION" )
                         );
                    columns.add( cd );
                }
            }
        }
        finally { rs.close(); }

        ColumnDescriptor[]  result = new ColumnDescriptor[ columns.size() ];
        columns.toArray( result );
        Arrays.sort( result );

        return result;
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // NESTED CLASSES
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * A struct class which is useful for describing columns and parameters.
     * </p>
     */
    public  static  final   class   ColumnDescriptor   implements  Comparable<ColumnDescriptor>
    {
        public  final   String  columnName;
        public  final   int jdbcType;
        public  final   int precision;
        public  final   int scale;
        public  final   String  typeName;
        public  final   int ordinalPosition;

        public  ColumnDescriptor
            (
             String columnName,
             int    jdbcType,
             int    precision,
             int    scale,
             String typeName,
             int    ordinalPosition
             )
        {
            this.columnName = columnName;
            this.jdbcType = jdbcType;
            this.precision = precision;
            this.scale = scale;
            this.typeName =typeName;
            this.ordinalPosition = ordinalPosition;
        }

        /** Sort on ordinalPosition */
        public  int compareTo( ColumnDescriptor that ) { return this.ordinalPosition - that.ordinalPosition; }
        public  boolean equals( Object other )
        {
            if ( other == null ) { return false; }
            else if ( !(other instanceof ColumnDescriptor) ) { return false; }
            else { return (compareTo( (ColumnDescriptor) other ) == 0); }
        }
        public  int hashCode()  { return columnName.hashCode(); }
        public  String  toString() { return columnName; }
    }

}
