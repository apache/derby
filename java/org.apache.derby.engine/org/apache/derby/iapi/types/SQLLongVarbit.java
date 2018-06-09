/*

   Derby - Class org.apache.derby.iapi.types.SQLLongVarbit

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

import org.apache.derby.shared.common.reference.Limits;

import org.apache.derby.iapi.services.io.StoredFormatIds;

/**
 * SQLLongVarbit represents the SQL type LONG VARCHAR FOR BIT DATA
 * It is an extension of SQLVarbit and is virtually indistinguishable
 * other than normalization.
 */
public class SQLLongVarbit extends SQLVarbit
{

	public String getTypeName()
	{
		return TypeId.LONGVARBIT_NAME;
	}

	/**
	 * Return max memory usage for a SQL LongVarbit
	 */
	int getMaxMemoryUsage()
	{
		return Limits.DB2_LONGVARCHAR_MAXWIDTH;
	}

	/**
	 * @see DataValueDescriptor#getNewNull
	 */
	public DataValueDescriptor getNewNull()
	{
		return new SQLLongVarbit();
	}

	/**
		Return my format identifier.

		@see org.apache.derby.iapi.services.io.TypedFormat#getTypeFormatId
	*/
	public int getTypeFormatId()
	{
		return StoredFormatIds.SQL_LONGVARBIT_ID;
	}


	/*
	 * Orderable interface
	 */


	/*
	 * Column interface
	 */


	/*
	 * class interface
	 */

	/*
	 * constructors
	 */
	public SQLLongVarbit()
	{
	}

	public SQLLongVarbit(byte[] val)
	{
		super(val);
	}

	/**
	 * Normalization method - this method may be called when putting
	 * a value into a SQLVarbit, for example, when inserting into a SQLVarbit
	 * column.  See NormalizeResultSet in execution.
	 *
	 * This overrides SQLBit -- the difference is that we don't
	 * expand SQLVarbits to fit the target.
	 * 
	 * @param desiredType	The type to normalize the source column to
	 * @param source		The value to normalize
	 *
	 *
	 * @exception StandardException				Thrown for null into
	 *											non-nullable column, and for
	 *											truncation error
	 */

	public void normalize(
				DataTypeDescriptor desiredType,
				DataValueDescriptor source)
					throws StandardException
	{
		if (source instanceof SQLLongVarbit) {
			// avoid creating an object in memory if a matching type.
			// this may be a stream.
			SQLLongVarbit other = (SQLLongVarbit) source;
			this.stream = other.stream;
			this.dataValue = other.dataValue;
		}
		else
			setValue(source.getBytes());
	}

	/*
	 * DataValueDescriptor interface
	 */

	/** @see DataValueDescriptor#typePrecedence */
	public int typePrecedence()
	{
		return TypeId.LONGVARBIT_PRECEDENCE;
	}
}
