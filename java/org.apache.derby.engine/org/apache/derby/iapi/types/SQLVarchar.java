/*

   Derby - Class org.apache.derby.iapi.types.SQLVarchar

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

import java.sql.Clob;
import java.text.RuleBasedCollator;

import org.apache.derby.shared.common.error.StandardException;

import org.apache.derby.iapi.services.io.StoredFormatIds;

import org.apache.derby.shared.common.sanity.SanityManager;

/**
 * SQLVarchar represents a VARCHAR value with UCS_BASIC collation.
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

    /** @see DataValueDescriptor#cloneValue */
    public DataValueDescriptor cloneValue(boolean forceMaterialization)
	{
		try
		{
			return new SQLVarchar(getString());
		}
		catch (StandardException se)
		{
			if (SanityManager.DEBUG)
//IC see: https://issues.apache.org/jira/browse/DERBY-2581
				SanityManager.THROWASSERT("Unexpected exception", se);
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

	/** @see StringDataValue#getValue(RuleBasedCollator) */
	public StringDataValue getValue(RuleBasedCollator collatorForComparison)
	{
//IC see: https://issues.apache.org/jira/browse/DERBY-2534
		if (collatorForComparison == null)
		{//null collatorForComparison means use UCS_BASIC for collation
		    return this;			
		} else {
			//non-null collatorForComparison means use collator sensitive
			//implementation of SQLVarchar
		     CollatorSQLVarchar s = new CollatorSQLVarchar(collatorForComparison);
		     s.copyState(this);
		     return s;
		}
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

	public SQLVarchar(Clob val)
	{
		super(val);
	}

    /**
     * <p>
     * This is a special constructor used when we need to represent a password
     * as a VARCHAR (see DERBY-866). If you need a general-purpose constructor
     * for char[] values and you want to re-use this constructor, make sure to
     * read the comment on the SQLChar( char[] ) constructor.
     * </p>
     */
    public SQLVarchar( char[] val ) { super( val ); }

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
    
    /**
     * returns the reasonable minimum amount by 
     * which the array can grow . See readExternal. 
     * when we know that the array needs to grow by at least
     * one byte, it is not performant to grow by just one byte
     * instead this amount is used to provide a resonable growby size.
     * @return minimum reasonable growby size
     */
    protected final int growBy()
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-302
        return RETURN_SPACE_THRESHOLD;  //seems reasonable for a varchar or clob 
    }
}
