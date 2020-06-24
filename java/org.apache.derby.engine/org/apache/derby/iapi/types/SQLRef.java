/*

   Derby - Class org.apache.derby.iapi.types.SQLRef

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package org.apache.derby.iapi.types;

import org.apache.derby.shared.common.error.StandardException;

import org.apache.derby.iapi.services.io.StoredFormatIds;

import org.apache.derby.shared.common.sanity.SanityManager;

import org.apache.derby.catalog.TypeDescriptor;

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

	protected void setFrom(DataValueDescriptor theValue) throws StandardException {

//IC see: https://issues.apache.org/jira/browse/DERBY-776
		if (theValue.isNull())
			setToNull();
		else
			value = (RowLocation) theValue.getObject();
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

    /** @see DataValueDescriptor#cloneValue */
    public DataValueDescriptor cloneValue(boolean forceMaterialization)
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
//IC see: https://issues.apache.org/jira/browse/DERBY-4520
           return new SQLRef((RowLocation) value.cloneValue(false));
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

    /**
     * Adding this overload makes it possible to use SQLRefs as keys in HashMaps.
     */
    public  int hashCode()
    {
        if ( value == null ) { return 0; }
        else { return value.hashCode(); }
    }
}
