/*

   Derby - Class org.apache.derby.iapi.types.SQLRef

   Copyright 1999, 2004 The Apache Software Foundation or its licensors, as applicable.

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

import org.apache.derby.iapi.services.io.ArrayInputStream;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.TypeId;
import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.iapi.types.DataValueDescriptor;

import org.apache.derby.iapi.types.RowLocation;
import org.apache.derby.iapi.types.Orderable;

import org.apache.derby.iapi.services.io.StoredFormatIds;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.catalog.TypeDescriptor;

import org.apache.derby.iapi.types.DataType;
import org.apache.derby.iapi.types.RefDataValue;
import org.apache.derby.iapi.services.cache.ClassSize;

import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.IOException;

import java.sql.ResultSet;
import java.sql.PreparedStatement;

public class SQLRef extends DataType implements RefDataValue
{
	protected RowLocation	value;

    private static final int BASE_MEMORY_USAGE = ClassSize.estimateBaseFromCatalog( SQLRef.class);

    public int estimateMemoryUsage()
    {
        int sz = BASE_MEMORY_USAGE;
        if( null != value)
            sz += value.estimateMemoryUsage();
        return sz;
    } // end of estimateMemoryUsage

	/*
	** DataValueDescriptor interface
	** (mostly implemented in DataType)
	*/

	public String getString()
	{
		if (value != null)
		{
			return value.toString();
		}
		else
		{
			return null;
		}
	}

	public Object getObject()
	{
		return value;
	}

	/**
	 * @see DataValueDescriptor#setValue
	 */
	public void setValue(Object theValue)
		throws StandardException
	{
		if ((theValue instanceof RowLocation) ||
			(theValue == null))
		{
			this.setValue((RowLocation)theValue);
		}
		else
		{
			throwLangSetMismatch(theValue);
		}
	}
	protected void setFrom(DataValueDescriptor theValue) throws StandardException {

		setValue(theValue.getObject());
	}

	public int getLength()
	{
		return TypeDescriptor.MAXIMUM_WIDTH_UNKNOWN;
	}

	/* this is for DataType's error generator */
	public String getTypeName()
	{
		return TypeId.REF_NAME;
	}

	/*
	 * Storable interface, implies Externalizable, TypedFormat
	 */


	/**
		Return my format identifier.

		@see org.apache.derby.iapi.services.io.TypedFormat#getTypeFormatId
	*/
	public int getTypeFormatId() {
		return StoredFormatIds.SQL_REF_ID;
	}  

	public boolean isNull()
	{
		return (value == null);
	}

	public void writeExternal(ObjectOutput out) throws IOException {

		if (SanityManager.DEBUG)
			SanityManager.ASSERT(value != null, "writeExternal() is not supposed to be called for null values.");

		out.writeObject(value);
	}

	/**
	 * @see java.io.Externalizable#readExternal
	 *
	 * @exception IOException	Thrown on error reading the object
	 * @exception ClassNotFoundException	Thrown if the class of the object
	 *										read from the stream can't be found
	 *										(not likely, since it's supposed to
	 *										be SQLRef).
	 */
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException
	{
		value = (RowLocation) in.readObject();
	}
	public void readExternalFromArray(ArrayInputStream in) throws IOException, ClassNotFoundException
	{
		value = (RowLocation) in.readObject();
	}

	/**
	 * @see org.apache.derby.iapi.services.io.Storable#restoreToNull
	 */

	public void restoreToNull()
	{
		value = null;
	}

	/*
	** Orderable interface
	*/

	/** @exception StandardException	Thrown on error */
	public boolean compare(int op,
						   DataValueDescriptor other,
						   boolean orderedNulls,
						   boolean unknownRV)
					throws StandardException
	{
		return value.compare(op,
							((SQLRef) other).value,
							orderedNulls,
							unknownRV);
	}

	/** @exception StandardException	Thrown on error */
	public int compare(DataValueDescriptor other) throws StandardException
	{
		return value.compare(((SQLRef) other).value);
	}

	/*
	 * DataValueDescriptor interface
	 */

	/** @see DataValueDescriptor#getClone */
	public DataValueDescriptor getClone()
	{
		/* In order to avoid a throws clause nightmare, we only call
		 * the constructors which do not have a throws clause.
		 *
		 * Clone the underlying RowLocation, if possible, so that we
		 * don't clobber the value in the clone.
		 */
		if (value == null)
			return new SQLRef();
		else
			return new SQLRef((RowLocation) value.cloneObject());
	}

	/**
	 * @see DataValueDescriptor#getNewNull
	 */
	public DataValueDescriptor getNewNull()
	{
		return new SQLRef();
	}

	/** 
	 * @see DataValueDescriptor#setValueFromResultSet 
	 *
	 */
	public void setValueFromResultSet(ResultSet resultSet, int colNumber,
									  boolean isNullable)
	{
		if (SanityManager.DEBUG)
			SanityManager.THROWASSERT(
				"setValueFromResultSet() is not supposed to be called for SQLRef.");
	}
	public void setInto(PreparedStatement ps, int position)  {
		if (SanityManager.DEBUG)
			SanityManager.THROWASSERT(
				"setValueInto(PreparedStatement) is not supposed to be called for SQLRef.");
	}

	/*
	** Class interface
	*/

	/*
	** Constructors
	*/

	public SQLRef()
	{
	}

	public SQLRef(RowLocation rowLocation)
	{
		value = rowLocation;
	}

	public void setValue(RowLocation rowLocation)
	{
		value = rowLocation;
	}

	/*
	** String display of value
	*/

	public String toString()
	{
		if (value == null)
			return "NULL";
		else
			return value.toString();
	}
}
