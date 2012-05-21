/*

   Derby - Class org.apache.derby.iapi.types.SQLVarbit

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

import org.apache.derby.iapi.reference.Limits;
import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.iapi.services.io.StoredFormatIds;

import org.apache.derby.iapi.util.StringUtil;


/**
 * SQLVarbit represents the SQL type VARCHAR FOR BIT DATA
 * It is an extension of SQLBit and is virtually indistinguishable
 * other than normalization.
 */
public class SQLVarbit extends SQLBit
{


	public String getTypeName()
	{
		return TypeId.VARBIT_NAME;
	}

	/**
	 * Return max memory usage for a SQL Varbit
	 */
	int getMaxMemoryUsage()
	{
		return Limits.DB2_VARCHAR_MAXWIDTH;
	}

	/**
	 * @see DataValueDescriptor#getNewNull
	 */
	public DataValueDescriptor getNewNull()
	{
		return new SQLVarbit();
	}

	/**
		Return my format identifier.

		@see org.apache.derby.iapi.services.io.TypedFormat#getTypeFormatId
	*/
	public int getTypeFormatId()
	{
		return StoredFormatIds.SQL_VARBIT_ID;
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

		byte[] sourceData = source.getBytes();
		setValue(sourceData);
		if (sourceData.length > desiredWidth)
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

		if (sourceWidth > desiredWidth)
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
	
			/*
			** Truncate to the desired width.
			*/
            truncate(sourceWidth, desiredWidth, !errorOnTrunc);
		}
	}


	/*
	 * Column interface
	 */


	/*
	 * class interface
	 */

	/*
	 * constructors
	 */
	public SQLVarbit()
	{
	}

	public SQLVarbit(byte[] val)
	{
		super(val);
	}

	/*
	 * DataValueDescriptor interface
	 */

	/** @see DataValueDescriptor#typePrecedence */
	public int typePrecedence()
	{
		return TypeId.VARBIT_PRECEDENCE;
	}
}
