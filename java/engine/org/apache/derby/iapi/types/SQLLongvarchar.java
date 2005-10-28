/*

   Derby - Class org.apache.derby.iapi.types.SQLLongvarchar

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
import org.apache.derby.iapi.types.StringDataValue;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.services.io.FormatIdUtil;
import org.apache.derby.iapi.services.io.StoredFormatIds;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.util.StringUtil;

/**
 * SQLLongvarchar satisfies the DataValueDescriptor interfaces (i.e., OrderableDataType). It implements a String
 * holder, e.g. for storing a column value; it can be specified
 * when constructed to not allow nulls. Nullability cannot be changed
 * after construction.
 * <p>
 * Because OrderableDataType is a subclass of DataType,
 * SQLLongvarchar can play a role in either a DataType/ValueRow
 * or a OrderableDataType/KeyRow, interchangeably.
 *
 * SQLLongvarchar is mostly the same as SQLVarchar, so it is implemented as a
 * subclass of SQLVarchar.  Only those methods with different behavior are
 * implemented here.
 */
public class SQLLongvarchar
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
		return TypeId.LONGVARCHAR_NAME;
	}

	/*
	 * DataValueDescriptor interface
	 */

	/** @see DataValueDescriptor#getClone */
	public DataValueDescriptor getClone()
	{
		try
		{
			return new SQLLongvarchar(getString());
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
		return new SQLLongvarchar();
	}

	/*
	 * Storable interface, implies Externalizable, TypedFormat
	 */

	/**
		Return my format identifier.

		@see org.apache.derby.iapi.services.io.TypedFormat#getTypeFormatId
	*/
	public int getTypeFormatId() {
		return StoredFormatIds.SQL_LONGVARCHAR_ID;
	}

	/*
	 * constructors
	 */

	public SQLLongvarchar()
	{
	}

	public SQLLongvarchar(String val)
	{
		super(val);
	}

	protected void normalize(DataTypeDescriptor desiredType, String sourceValue)
		throws StandardException
	{
		//bug 5592 - for sql long varchar, any truncation is disallowed ie even the trailing blanks can't be truncated
		if (sourceValue.length() > desiredType.getMaximumWidth())
			throw StandardException.newException(SQLState.LANG_STRING_TRUNCATION, getTypeName(), StringUtil.formatForPrint(sourceValue), String.valueOf(desiredType.getMaximumWidth()));

		setValue(sourceValue);
	}

	/**
	 * @see StringDataValue#concatenate
	 *
	 * @exception StandardException		Thrown on error
	 */
	public StringDataValue concatenate(
				StringDataValue leftOperand,
				StringDataValue rightOperand,
				StringDataValue result)
		throws StandardException
	{
		super.concatenate(leftOperand, rightOperand, result);

		//bug 5600 - according to db2 concatenation documentation, for compatibility with previous versions, there is no automatic
		//escalation of results involving LONG data types to LOB data types. For eg, concatenation of a CHAR(200) value and a
		//completely full LONG VARCHAR value would result in an error rather than in a promotion to a CLOB data type

		//need to check for concatenated string for null value
		if ((result.getString() != null) && (result.getString().length() > TypeId.LONGVARCHAR_MAXWIDTH))
			throw StandardException.newException(SQLState.LANG_CONCAT_STRING_OVERFLOW, "CONCAT", String.valueOf(TypeId.LONGVARCHAR_MAXWIDTH));

		return result;
	}

	/*
	 * DataValueDescriptor interface
	 */

	/* @see DataValueDescriptor#typePrecedence */
	public int typePrecedence()
	{
		return TypeId.LONGVARCHAR_PRECEDENCE;
	}
}
