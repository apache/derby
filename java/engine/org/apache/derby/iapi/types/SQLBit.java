/*

   Derby - Class org.apache.derby.iapi.types.SQLBit

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

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.reference.Limits;

import org.apache.derby.iapi.services.io.StoredFormatIds;

import org.apache.derby.iapi.util.StringUtil;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * SQLBit represents the SQL type CHAR FOR BIT DATA
 */
public class SQLBit
	extends SQLBinary
{

	/**
	 *
	 * @exception StandardException		Thrown on error
	 */
	public Object	getObject() throws StandardException
	{
		return getBytes();
	}


	public String getTypeName()
	{
		return TypeId.BIT_NAME;
	}

	/**
	 * Return max memory usage for a SQL Bit
	 */
	int getMaxMemoryUsage()
	{
		return Limits.DB2_CHAR_MAXWIDTH;
	}

	/*
	 * Storable interface, implies Externalizable, TypedFormat
	 */

	/**
		Return my format identifier.

		@see org.apache.derby.iapi.services.io.TypedFormat#getTypeFormatId
	*/
	public int getTypeFormatId()
	{
		return StoredFormatIds.SQL_BIT_ID;
	}


	/** @see DataValueDescriptor#getNewNull */
	public DataValueDescriptor getNewNull()
	{
		return new SQLBit();
	}

	/** 
	 * Obtain the value using getBytes. This works for all FOR BIT DATA types.
	 * Getting a stream is problematic as any other getXXX() call on the ResultSet
	 * will close the stream we fetched. Therefore we have to create the value in-memory
	 * as a byte array.
	 * @see DataValueDescriptor#setValueFromResultSet 
	 *
	 * @exception SQLException		Thrown on error
	 */
	public final void setValueFromResultSet(ResultSet resultSet, int colNumber,
									  boolean isNullable)
		throws SQLException
	{
			setValue(resultSet.getBytes(colNumber));
	}

	/*
	 * DataValueDescriptor interface
	 */

	/** @see DataValueDescriptor#typePrecedence */
	public int typePrecedence()
	{
		return TypeId.BIT_PRECEDENCE;
	}
	
	/**
	 * Set the value from an non-null object.
	 */
	final void setObject(Object theValue)
		throws StandardException
	{
		setValue((byte[]) theValue);
	}

	/*
	 * constructors
	 */

	/**
		no-arg constructor, required by Formattable.
	*/
	public SQLBit()
	{
	}

	public SQLBit(byte[] val)
	{
		dataValue = val;
	}

	/**
	 * Normalization method - this method may be called when putting
	 * a value into a SQLBit, for example, when inserting into a SQLBit
	 * column.  See NormalizeResultSet in execution.
	 *
	 * @param desiredType	The type to normalize the source column to
	 * @param source		The value to normalize
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
		int		desiredWidth = desiredType.getMaximumWidth();

		((SQLBinary) this).setValue(source.getBytes());
		setWidth(desiredWidth, 0, true);
	}

	/**
	 * Set the width of the to the desired value.  Used
	 * when CASTing.  Ideally we'd recycle normalize(), but
	 * the behavior is different (we issue a warning instead
	 * of an error, and we aren't interested in nullability).
	 *
	 * @param desiredWidth	the desired length	
	 * @param desiredScale	the desired scale (ignored)	
	 * @param errorOnTrunc	throw error on truncation
	 *
	 * @exception StandardException		Thrown on non-zero truncation
	 *		if errorOnTrunc is true	
	 */
	public void setWidth(int desiredWidth, 
			int desiredScale,	// Ignored 
			boolean errorOnTrunc)
			throws StandardException
	{
		/*
		** If the input is NULL, nothing to do.
		*/
		if (getValue() == null)
		{
			return;
		}

		int sourceWidth = dataValue.length;

		/*
		** If the input is shorter than the desired type,
		** then pad with blanks to the right length.
		*/
		if (sourceWidth < desiredWidth)
		{
			byte[] actualData = new byte[desiredWidth];
			System.arraycopy(dataValue, 0, actualData, 0, dataValue.length);
			java.util.Arrays.fill(actualData, dataValue.length, actualData.length, SQLBinary.PAD);
			dataValue = actualData;
		}
		/*
		** Truncation?
		*/
		else if (sourceWidth > desiredWidth)
		{
			if (errorOnTrunc)
			{
				// error if truncating non pad characters.
				for (int i = desiredWidth; i < dataValue.length; i++) {

					if (dataValue[i] != SQLBinary.PAD)
						throw StandardException.newException(SQLState.LANG_STRING_TRUNCATION, getTypeName(), 
									StringUtil.formatForPrint(this.toString()),
									String.valueOf(desiredWidth));
				}
			}
			//else
			//{
			// RESOLVE: when we have warnings, issue a warning if
			// truncation of non-zero bits will occur
			//}
	
			/*
			** Truncate to the desired width.
			*/
			byte[] shrunkData = new byte[desiredWidth];
			System.arraycopy(dataValue, 0, shrunkData, 0, desiredWidth);
			dataValue = shrunkData;

		}
	}





}
