/*

   Derby - Class org.apache.derby.iapi.types.SQLClob

   Copyright 2003, 2004 The Apache Software Foundation or its licensors, as applicable.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package org.apache.derby.iapi.types;

import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.TypeId;
import org.apache.derby.iapi.types.StringDataValue;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.services.io.FormatIdUtil;
import org.apache.derby.iapi.services.io.StoredFormatIds;

import org.apache.derby.iapi.services.sanity.SanityManager;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.math.BigDecimal;

import java.util.Calendar;


/**
 * SQLClob uses SQLVarchar by inheritance.
 * It satisfies the DataValueDescriptor interfaces (i.e., OrderableDataType). It implements a String
 * holder, e.g. for storing a column value; it can be specified
 * when constructed to not allow nulls. Nullability cannot be changed
 * after construction.
 * <p>
 * Because OrderableDataType is a subclass of DataType,
 * SQLLongvarchar can play a role in either a DataType/ValueRow
 * or a OrderableDataType/KeyRow, interchangeably.
 */
public class SQLClob
	extends SQLVarchar
{
	/*
	 * DataValueDescriptor interface.
	 *
	 * These are actually all implemented in the super-class, but we need
	 * to duplicate some of them here so they can be called by byte-code
	 * generation, which needs to know the class the method appears in.
	 */

	public String getTypeName()
	{
		return TypeId.CLOB_NAME;
	}

	/*
	 * DataValueDescriptor interface
	 */

	/** @see DataValueDescriptor#getClone */
	public DataValueDescriptor getClone()
	{
		try
		{
			return new SQLClob(getString());
		}
		catch (StandardException se)
		{
			if (SanityManager.DEBUG)
				SanityManager.THROWASSERT("Unexpected exception " + se);
			return null;
		}
	}

	/**
	 * @see DataValueDescriptor#getNewNull
	 *
	 */
	public DataValueDescriptor getNewNull()
	{
		return new SQLClob();
	}

	/*
	 * Storable interface, implies Externalizable, TypedFormat
	 */

	/**
		Return my format identifier.

		@see org.apache.derby.iapi.services.io.TypedFormat#getTypeFormatId
	*/
	public int getTypeFormatId() {
		return StoredFormatIds.SQL_CLOB_ID;
	}

	/*
	 * constructors
	 */

	public SQLClob()
	{
	}

	public SQLClob(String val)
	{
		super(val);
	}

	/*
	 * DataValueDescriptor interface
	 */

	/* @see DataValueDescriptor#typePrecedence */
	public int typePrecedence()
	{
		return TypeId.CLOB_PRECEDENCE;
	}

	/*
	** disable conversions to/from most types for CLOB.
	** TEMP - real fix is to re-work class hierachy so
	** that CLOB is towards the root, not at the leaf.
	*/

	public Object	getObject() throws StandardException
	{
		throw dataTypeConversion("java.lang.Object");
	}

	public boolean	getBoolean() throws StandardException
	{
		throw dataTypeConversion("boolean");
	}

	public byte	getByte() throws StandardException
	{
		throw dataTypeConversion("byte");
	}

	public short	getShort() throws StandardException
	{
		throw dataTypeConversion("short");
	}

	public int	getInt() throws StandardException
	{
		throw dataTypeConversion("int");
	}

	public long	getLong() throws StandardException
	{
		throw dataTypeConversion("long");
	}

	public float	getFloat() throws StandardException
	{
		throw dataTypeConversion("float");
	}

	public double	getDouble() throws StandardException
	{
		throw dataTypeConversion("double");
	}
	public BigDecimal	getBigDecimal() throws StandardException
	{
		throw dataTypeConversion("java.math.BigDecimal");
	}
	public byte[]	getBytes() throws StandardException
	{
		throw dataTypeConversion("byte[]");
	}

	public Date	getDate(java.util.Calendar cal) throws StandardException
	{
		throw dataTypeConversion("java.sql.Date");
	}

	public Time	getTime(java.util.Calendar cal) throws StandardException
	{
		throw dataTypeConversion("java.sql.Time");
	}

	public Timestamp	getTimestamp(java.util.Calendar cal) throws StandardException
	{
		throw dataTypeConversion("java.sql.Timestamp");
	}

	public void setValue(Time theValue, Calendar cal) throws StandardException
	{
		throwLangSetMismatch("java.sql.Time");
	}
	
	public void setValue(Timestamp theValue, Calendar cal) throws StandardException
	{
		throwLangSetMismatch("java.sql.Timestamp");
	}
	
	public void setValue(Date theValue, Calendar cal) throws StandardException
	{
		throwLangSetMismatch("java.sql.Date");
	}
	
	public void setValue(BigDecimal theValue) throws StandardException
	{
		throwLangSetMismatch("java.math.BigDecimal");
	}

	public void setValue(int theValue) throws StandardException
	{
		throwLangSetMismatch("int");
	}

	public void setValue(double theValue) throws StandardException
	{
		throwLangSetMismatch("double");
	}

	public void setValue(float theValue) throws StandardException
	{
		throwLangSetMismatch("float");
	}
 
	public void setValue(short theValue) throws StandardException
	{
		throwLangSetMismatch("short");
	}

	public void setValue(long theValue) throws StandardException
	{
		throwLangSetMismatch("long");
	}


	public void setValue(byte theValue) throws StandardException
	{
		new Throwable("FRED").printStackTrace(System.out);
		throwLangSetMismatch("byte");
	}

	public void setValue(boolean theValue) throws StandardException
	{
		throwLangSetMismatch("boolean");
	}

	public void setValue(byte[] theValue) throws StandardException
	{
		throwLangSetMismatch("byte[]");
	}

	public void setValue(Object theValue)
		throws StandardException
	{
		if (theValue == null)
			setToNull();
		else
			throwLangSetMismatch(theValue);
	}

}
