/*

   Derby - Class org.apache.derby.iapi.types.SQLVarchar

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

import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.TypeId;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.StringDataValue;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.services.io.FormatIdUtil;
import org.apache.derby.iapi.services.io.StoredFormatIds;

import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.util.StringUtil;

/**
 * SQLVarchar satisfies the DataValueDescriptor
 * interfaces (i.e., OrderableDataType). It implements a String holder, 
 * e.g. for storing a column value; it can be specified
 * when constructed to not allow nulls. Nullability cannot be changed
 * after construction.
 * <p>
 * Because OrderableDataType is a subclass of DataType,
 * SQLVarchar can play a role in either a DataType/ValueRow
 * or a OrderableDataType/KeyRow, interchangeably.
 *
 * SQLVarchar is mostly the same as SQLChar, so it is implemented as a
 * subclass of SQLChar.  Only those methods with different behavior are
 * implemented here.
 */
public class SQLVarchar
	extends SQLChar
{

	/*
	 * DataValueDescriptor interface.
	 *
	 */

	public String getTypeName()
	{
		return TypeId.VARCHAR_NAME;
	}

	/*
	 * DataValueDescriptor interface
	 */

	/** @see DataValueDescriptor#getClone */
	public DataValueDescriptor getClone()
	{
		try
		{
			return new SQLVarchar(getString());
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
		return new SQLVarchar();
	}


	/*
	 * Storable interface, implies Externalizable, TypedFormat
	 */

	/**
		Return my format identifier.

		@see org.apache.derby.iapi.services.io.TypedFormat#getTypeFormatId
	*/
	public int getTypeFormatId() {
		return StoredFormatIds.SQL_VARCHAR_ID;
	}

	/*
	 * constructors
	 */

	public SQLVarchar()
	{
	}

	public SQLVarchar(String val)
	{
		super(val);
	}

	/**
	 * Normalization method - this method may be called when putting
	 * a value into a SQLVarchar, for example, when inserting into a SQLVarchar
	 * column.  See NormalizeResultSet in execution.
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
		normalize(desiredType, source.getString());
	}

	protected void normalize(DataTypeDescriptor desiredType, String sourceValue)
		throws StandardException
	{

		int			desiredWidth = desiredType.getMaximumWidth();

		int sourceWidth = sourceValue.length();

		/*
		** If the input is already the right length, no normalization is
		** necessary.
		**
		** It's OK for a Varchar value to be shorter than the desired width.
		** This can happen, for example, if you insert a 3-character Varchar
		** value into a 10-character Varchar column.  Just return the value
		** in this case.
		*/

		if (sourceWidth > desiredWidth) {

			hasNonBlankChars(sourceValue, desiredWidth, sourceWidth);

			/*
			** No non-blank characters will be truncated.  Truncate the blanks
			** to the desired width.
			*/
			sourceValue = sourceValue.substring(0, desiredWidth);
		}

		setValue(sourceValue);
	}


	/*
	 * DataValueDescriptor interface
	 */

	/* @see DataValueDescriptor#typePrecedence */
	public int typePrecedence()
	{
		return TypeId.VARCHAR_PRECEDENCE;
	}
}
