/*

   Derby - Class org.apache.derby.iapi.types.SQLBit

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

import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.iapi.services.io.ArrayInputStream;

import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.TypeId;
import org.apache.derby.iapi.types.BitDataValue;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.ConcatableDataValue;
import org.apache.derby.iapi.types.VariableSizeDataValue;
import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.services.io.FormatIdUtil;
import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.iapi.services.io.StreamStorable;
import org.apache.derby.iapi.services.io.FormatIdInputStream;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.types.BooleanDataValue;
import org.apache.derby.iapi.types.StringDataValue;
import org.apache.derby.iapi.types.NumberDataValue;

import org.apache.derby.iapi.services.cache.ClassSize;
import org.apache.derby.iapi.util.StringUtil;

import org.apache.derby.iapi.types.SQLInteger;

import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.IOException;
import java.io.InputStream;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * SQLBit satisfies the DataValueDescriptor
 * interfaces (i.e., DataType). It implements a String holder,
 * e.g. for storing a column value; it can be specified
 * when constructed to not allow nulls. Nullability cannot be changed
 * after construction.
 * <p>
 * Because DataType is a subclass of DataType,
 * SQLBit can play a role in either a DataType/Value
 * or a DataType/KeyRow, interchangeably.

  <P>
  Format : <encoded length><raw data>
  <BR>
  Length is encoded to support 5.x databases where the length was stored as the number of bits.
  The first bit of the first byte indicates if the format is an old (5.x) style or a new 8.1 style.
  8.1 then uses the next two bits to indicate how the length is encoded.
  <BR>
  <encoded length> is one of N styles.
  <UL>
  <LI> (5.x format) 4 byte Java format integer value 0 - either <raw data> is 0 bytes/bits  or an unknown number of bytes.
  <LI> (5.x format) 4 byte Java format integer value >0 (positive) - number of bits in <raw data>, number of bytes in <raw data>
  is the minimum number of bytes required to store the number of bits.
  <LI> (8.1 format) 1 byte encoded length (0 <= L <= 31) - number of bytes of <raw data> - encoded = 0x80 & L
  <LI> (8.1 format) 3 byte encoded length (32 <= L < 64k) - number of bytes of <raw data> - encoded = 0xA0 <L as Java format unsigned short>
  <LI> (8.1 format) 5 byte encoded length (64k <= L < 2G) - number of bytes of <raw data> - encoded = 0xC0 <L as Java format integer>
  <LI> (future) to be determined L >= 2G - encoded 0xE0 <encoding of L to be determined>
  (0xE0 is an esacape to allow any number of arbitary encodings in the future).
  </UL>
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
	 * @see DataValueDescriptor#setValueFromResultSet 
	 *
	 * @exception SQLException		Thrown on error
	 */
	public void setValueFromResultSet(ResultSet resultSet, int colNumber,
									  boolean isNullable)
		throws SQLException
	{
			dataValue = resultSet.getBytes(colNumber);

			if (isNullable && resultSet.wasNull())
			{
				setToNull();
			}
	}

	/*
	 * DataValueDescriptor interface
	 */

	/** @see DataValueDescriptor#typePrecedence */
	public int typePrecedence()
	{
		return TypeId.BIT_PRECEDENCE;
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
	 * @see DataValueDescriptor#setValue
	 *
	 */	
	public final void setValue(Object theValue)
		throws StandardException
	{
		if (theValue == null)
		{
			setToNull();
		}
		else if (theValue instanceof byte[])
		{
			((SQLBinary) this).setValue((byte[])theValue);
		}
		else
		{
			throwLangSetMismatch(theValue);
		}
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
	 * @return this with the target width
	 *
	 * @exception StandardException		Thrown on non-zero truncation
	 *		if errorOnTrunc is true	
	 */
	public DataValueDescriptor setWidth(int desiredWidth, 
			int desiredScale,	// Ignored 
			boolean errorOnTrunc)
			throws StandardException
	{
		/*
		** If the input is NULL, nothing to do.
		*/
		if (getValue() == null)
		{
			return this;
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
		return this;
	}





}
