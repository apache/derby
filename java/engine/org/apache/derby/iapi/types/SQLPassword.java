/*

   Derby - Class org.apache.derby.iapi.types.SQLPassword

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
import java.util.Arrays;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.services.io.StoredFormatIds;

import org.apache.derby.iapi.services.sanity.SanityManager;

/**
 * SQLPassword represents a VARCHAR value with UCS_BASIC collation
 * which can only be used to wrap a char[]. See DERBY-866. This is a special
 * internal type which should never leak outside Derby into application code.
 */
public class SQLPassword extends SQLVarchar
{

	/*
	 * DataValueDescriptor interface.
	 *
	 */

	public String getTypeName()
	{
		return TypeId.PASSWORD_NAME;
	}

	/*
	 * DataValueDescriptor interface
	 */

    /** @see DataValueDescriptor#cloneValue */
    public DataValueDescriptor cloneValue(boolean forceMaterialization)
        throws StandardException
	{
        return new SQLPassword( getRawDataAndZeroIt() );
	}

	/**
	 * @see DataValueDescriptor#getNewNull
	 *
	 */
	public DataValueDescriptor getNewNull()
	{
		return new SQLPassword();
	}

	/** @see StringDataValue#getValue(RuleBasedCollator) */
	public StringDataValue getValue(RuleBasedCollator collatorForComparison)
	{
        // passwords are never search/sorted or ever used in a collation-sensitive context
        return this;
	}


	/*
	 * Storable interface, implies Externalizable, TypedFormat
	 */

	/**
		Return my format identifier.

		@see org.apache.derby.iapi.services.io.TypedFormat#getTypeFormatId
	*/
	public int getTypeFormatId() {
		return StoredFormatIds.SQL_PASSWORD_ID;
	}

	/*
	 * constructors
	 */

	public SQLPassword()
	{
	}

    /**
     * <p>
     * This is a special constructor used when we need to represent a password
     * as a VARCHAR (see DERBY-866). If you need a general-purpose constructor
     * for char[] values and you want to re-use this constructor, make sure to
     * read the comment on the SQLChar( char[] ) constructor.
     * </p>
     */
    public SQLPassword( char[] val ) { super( val ); }

	/**
	 * Normalization method - this method may be called when putting
	 * a value into a SQLPassword, for example, when inserting into a SQLPassword
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
        if ( source == null )
        {
            throwLangSetMismatch("null");
        }
        else if ( !(source instanceof SQLChar) )
        {
            throwLangSetMismatch( source.getClass().getName() );
        }
        else
        {
            normalize(desiredType, ((SQLChar) source).getRawDataAndZeroIt() );
        }
	}

    /** The passed-in sourceValue may be zeroed out */
	protected void normalize(DataTypeDescriptor desiredType, char[] sourceValue)
		throws StandardException
	{

		int			desiredWidth = desiredType.getMaximumWidth();

		int sourceWidth = sourceValue.length;

		/*
		** If the input is already the right length or shorter, no normalization is
		** necessary.
		*/

        char[]  result = sourceValue;
        
		if (sourceWidth > desiredWidth)
        {
            result = new char[ desiredWidth ];
            System.arraycopy( sourceValue, 0, result, 0, desiredWidth );

            // we can't count on our caller to zero out the old array
            Arrays.fill( sourceValue, (char) 0 );
		}

		setAndZeroOldValue( result );
	}

    protected void setFrom(DataValueDescriptor theValue) 
        throws StandardException 
    {
        if ( !(theValue instanceof SQLChar ) )
        {
            throwLangSetMismatch( theValue.getClass().getName() );
        }
        else
        {
            setAndZeroOldValue(  ((SQLChar) theValue).getRawDataAndZeroIt() );
        }
    }

	/*
	 * DataValueDescriptor interface
	 */

	/* @see DataValueDescriptor#typePrecedence */
	public int typePrecedence()
	{
		return TypeId.PASSWORD_PRECEDENCE;
	}
    
}
