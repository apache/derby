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

import java.sql.SQLException;
import java.math.BigDecimal;

/**
	An abstract implementation of ResultSet that is useful
	when writing table functions, read-only VTIs (virtual table interface), and
	the ResultSets returned by executeQuery in read-write VTI classes.
	
	This class implements most of the methods of the JDBC 3.0 interface java.sql.ResultSet,
	each one throwing a  SQLException with the name of the method. 
	A concrete subclass can then just implement the methods not implemented here 
	and override any methods it needs to implement for correct functionality.
	<P>
	The methods not implemented here are
	<UL>
	<LI>next()
	<LI>close()
	<LI>getMetaData()
	</UL>
	<P>

	For table functions and virtual tables, the database engine only calls methods defined
	in the JDBC 2.0 definition of java.sql.ResultSet.
	<BR>
	Classes that implement a JDBC 2.0 conformant java.sql.ResultSet can be used
	as table functions and virtual tables.
 */
public abstract class VTITemplate extends VTITemplateBase
{
    // Together with our superclass, the following overrides are a trick
    // to allow subclasses to compile on both Java 5 and Java 6
    public abstract boolean next() throws SQLException;
    public abstract void close() throws SQLException;

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
    public BigDecimal getBigDecimal(String columnName, int scale) throws SQLException { return getBigDecimal(findColumn(columnName), scale); }
    public byte[] getBytes(String columnName) throws SQLException { return getBytes(findColumn(columnName)); }
    public java.sql.Date getDate(String columnName) throws SQLException { return getDate(findColumn(columnName)); }
    public java.sql.Time getTime(String columnName) throws SQLException { return getTime(findColumn(columnName)); }
    public java.sql.Timestamp getTimestamp(String columnName) throws SQLException { return getTimestamp(findColumn(columnName)); }
    public Object getObject(String columnName) throws SQLException { return getObject(findColumn(columnName)); }
	public BigDecimal getBigDecimal(String columnName) throws SQLException { return getBigDecimal(findColumn(columnName)); }

}
