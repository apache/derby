/*

   Derby - Class org.apache.derby.iapi.types.SQLBinary

   Copyright 2004 The Apache Software Foundation or its licensors, as applicable.

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
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.services.io.NewByteArrayInputStream;

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
 * SQLBinary satisfies the DataValueDescriptor
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
public abstract class SQLBinary
	extends DataType implements BitDataValue
{

	static final byte PAD = (byte) 0x20;

    private static final int BASE_MEMORY_USAGE = ClassSize.estimateBaseFromCatalog( SQLBinary.class);

    public int estimateMemoryUsage()
    {
        int sz = BASE_MEMORY_USAGE;
        if( null != dataValue)
            sz += dataValue.length;
        return sz;
    } // end of estimateMemoryUsage

	  
	  
	 /*
	 * object state
	 */
	byte[] dataValue;

	/*
	 * stream state
	 */
	InputStream stream;

	/**
		Length of the stream in units relevant to the type,
		in this case bytes.
	*/
	int streamLength;

	/**
		no-arg constructor, required by Formattable.
	*/
	SQLBinary()
	{
	}

	SQLBinary(byte[] val)
	{
		dataValue = val;
	}


	public final void setValue(byte[] theValue)
	{
		dataValue = theValue;
		stream = null;
		streamLength = -1;
	}

	/**
	 * Used by JDBC -- string should not contain
	 * SQL92 formatting.
	 *
	 * @exception StandardException		Thrown on error
	 */
	public final String	getString() throws StandardException
	{
		if (getValue() == null)
			return null;
		else if (dataValue.length * 2 < 0)  //if converted to hex, length exceeds max int
		{
			throw StandardException.newException(SQLState.LANG_STRING_TRUNCATION, getTypeName(),
									"",
									String.valueOf(Integer.MAX_VALUE));
		}
		else 
		{
			return org.apache.derby.iapi.util.StringUtil.toHexString(dataValue, 0, dataValue.length);
		}
	}


	/**
	 * @exception StandardException		Thrown on error
	 */
	public final InputStream	getStream()
	{
		return (stream);
	}

	/**
	 *
	 * @exception StandardException		Thrown on error
	 */
	public final byte[]	getBytes() throws StandardException
	{
		return getValue();
	}

	byte[] getValue() throws StandardException
	{
		try
		{
			if ((dataValue == null) && (stream != null)) {

				if (stream instanceof FormatIdInputStream) {
					readExternal((FormatIdInputStream) stream);
				} else if ( stream instanceof NewByteArrayInputStream )
				{
					// this piece of code handles the case that a stream has been
					// opened on the bit value. the stream will have already called
					// readExternal() on the underlying FormatableBitSet. we just need to
					// retrieve the byte array from that stream.
					NewByteArrayInputStream	nbais = (NewByteArrayInputStream) stream;
					dataValue = nbais.getData();
				}
				else {
					readExternal(new FormatIdInputStream(stream));
				}
				stream = null;
				streamLength = -1;

			}
		}
		catch (IOException ioe)
		{
			throw StandardException.newException(SQLState.LANG_STREAMING_COLUMN_I_O_EXCEPTION, ioe, getTypeName());
		}
		return dataValue;
	}
	
	/**
	 * length in bytes
	 *
	 * @exception StandardException		Thrown on error
	 */
	public final int	getLength() throws StandardException
	{
		if (stream != null) {

			if (streamLength != -1)
				return streamLength;
		}

		return getBytes().length;

	}

	/*
	 * Storable interface, implies Externalizable, TypedFormat
	 */


	/**
	 * see if the Bit value is null.
	 * @see org.apache.derby.iapi.services.io.Storable#isNull
	 */
	public final boolean isNull()
	{
		return (dataValue == null) && (stream == null);
	}

	/** 
		Write the value out from the byte array (not called if null)
		using the 8.1 encoding.

	 * @exception IOException		io exception
	 */
	public final void writeExternal(ObjectOutput out) throws IOException
	{

		int len = dataValue.length;
		if (len <= 31)
		{
			out.write((byte) (0x80 | (len & 0xff)));
		}
		else if (len <= 0xFFFF)
		{
			out.write((byte) 0xA0);
			out.writeShort((short) len);
		}
		else
		{
			out.write((byte) 0xC0);
			out.writeInt(len);

		}
		out.write(dataValue, 0, dataValue.length);
	}

	/** 
	 * delegated to bit 
	 *
	 * @exception IOException			io exception
	 * @exception ClassNotFoundException	class not found
	*/
	public final void readExternal(ObjectInput in) throws IOException
	{
		// need to clear stream first, in case this object is reused, and
		// stream is set by previous use.  Track 3794.
		stream = null;
		streamLength = -1;


		int len = SQLBinary.readBinaryLength(in);

		if (len != 0)
		{
			dataValue = new byte[len];
			in.readFully(dataValue);
		}
		else
		{
			readFromStream((InputStream) in);
		}
	}
	public final void readExternalFromArray(ArrayInputStream in) throws IOException
	{
		// need to clear stream first, in case this object is reused, and
		// stream is set by previous use.  Track 3794.
		stream = null;
		streamLength = -1;

		int len = SQLBinary.readBinaryLength(in);

		if (len != 0)
		{
			dataValue = new byte[len];
			in.readFully(dataValue);
		}
		else
		{
			readFromStream(in);
		}
	}

	private static int readBinaryLength(ObjectInput in) throws IOException {
		int len = 0;
		int bl = in.read();
		if (len < 0)
			throw new java.io.EOFException();

		if ((bl & 0x80) != 0)
		{
			if (bl == 0xC0)
			{
				len = in.readInt();
			}
			else if (bl == 0xA0)
			{
				len = in.readUnsignedShort();
			}
			else
			{
				len = bl & 0x1F;
			}
		}
		else
		{
			// old length in bits
			int v2 = in.read();
			int v3 = in.read();
			int v4 = in.read();
			if (v2 < 0 || v3 < 0 || v4 < 0)
				throw new java.io.EOFException();
            int lenInBits = (((bl & 0xff) << 24) | ((v2 & 0xff) << 16) | ((v3 & 0xff) << 8) | (v4 & 0xff));

			len = lenInBits / 8;
			if ((lenInBits % 8) != 0)
				len++;
		}
		return len;
	}

	private void readFromStream(InputStream in) throws IOException {

		dataValue = null;	// allow gc of the old value before the new.
		byte[] tmpData = new byte[32 * 1024];

		int off = 0;
		for (;;) {

			int len = in.read(tmpData, off, tmpData.length - off);
			if (len == -1)
				break;
			off += len;

			int available = in.available();
			int extraSpace = available - (tmpData.length - off);
			if (extraSpace > 0)
			{
				// need to grow the array
				int size = tmpData.length * 2;
				if (extraSpace > tmpData.length)
					size += extraSpace;

				byte[] grow = new byte[size];
				System.arraycopy(tmpData, 0, grow, 0, off);
				tmpData = grow;
			}
		}

		dataValue = new byte[off];
		System.arraycopy(tmpData, 0, dataValue, 0, off);
	}

	/**
	 * @see org.apache.derby.iapi.services.io.Storable#restoreToNull
	 */
	public final void restoreToNull()
	{
		dataValue = null;
		stream = null;
		streamLength = -1;
	}

	/**
		@exception StandardException thrown on error
	 */
	public final boolean compare(int op,
						   DataValueDescriptor other,
						   boolean orderedNulls,
						   boolean unknownRV)
		throws StandardException
	{
		if (!orderedNulls)		// nulls are unordered
		{
			if (SanityManager.DEBUG)
			{
                int otherTypeFormatId = other.getTypeFormatId();
				if (!((StoredFormatIds.SQL_BIT_ID == otherTypeFormatId)
                      || (StoredFormatIds.SQL_VARBIT_ID == otherTypeFormatId)
                      || (StoredFormatIds.SQL_LONGVARBIT_ID == otherTypeFormatId)

                      || (StoredFormatIds.SQL_CHAR_ID == otherTypeFormatId)
                      || (StoredFormatIds.SQL_VARCHAR_ID == otherTypeFormatId)
                      || (StoredFormatIds.SQL_LONGVARCHAR_ID == otherTypeFormatId)

                      || ((StoredFormatIds.SQL_BLOB_ID == otherTypeFormatId)
                          && (StoredFormatIds.SQL_BLOB_ID == getTypeFormatId()))
                        ))
				SanityManager.THROWASSERT(
									"Some fool passed in a "+ other.getClass().getName() + ", "
                                    + otherTypeFormatId  + " to SQLBinary.compare()");
			}
			String otherString = other.getString();
			if (this.getString() == null  || otherString == null)
				return unknownRV;
		}
		/* Do the comparison */
		return super.compare(op, other, orderedNulls, unknownRV);
	}

	/**
		@exception StandardException thrown on error
	 */
	public final int compare(DataValueDescriptor other) throws StandardException
	{

		/* Use compare method from dominant type, negating result
		 * to reflect flipping of sides.
		 */
		if (typePrecedence() < other.typePrecedence())
		{
			return - (other.compare(this));
		}

		/*
		** By convention, nulls sort High, and null == null
		*/
		if (this.isNull() || other.isNull())
		{
			if (!isNull())
				return -1;
			if (!other.isNull())
				return 1;
			return 0;							// both null
		}

		return SQLBinary.compare(getBytes(), other.getBytes());
	}

	/*
	 * CloneableObject interface
	 */

	/** From CloneableObject
	 *	Shallow clone a StreamStorable without objectifying.  This is used to avoid
	 *	unnecessary objectifying of a stream object.  The only difference of this method
	 *  from getClone is this method does not objectify a stream.  beetle 4896
	 */
	public final Object cloneObject()
	{
		if (stream == null)
			return getClone();
		SQLBinary self = (SQLBinary) getNewNull();
		self.setStream(stream);
		return self;
	}

	/*
	 * DataValueDescriptor interface
	 */

	/** @see DataValueDescriptor#getClone */
	public final DataValueDescriptor getClone()
	{
		try
		{
			DataValueDescriptor cloneDVD = getNewNull();
			cloneDVD.setValue(getValue());
			return cloneDVD;
		}
		catch (StandardException se)
		{
			if (SanityManager.DEBUG)
				SanityManager.THROWASSERT("Unexpected exception " + se);
			return null;
		}
	}

	/*
	 * DataValueDescriptor interface
	 */

	/*
	 * StreamStorable interface : 
	 */
	public final InputStream returnStream()
	{
		return stream;
	}

	public final void setStream(InputStream newStream)
	{
		this.dataValue = null;
		this.stream = newStream;
		streamLength = -1;
	}

	public final void loadStream() throws StandardException
	{
		getValue();
	}

	/*
	 * class interface
	 */

    boolean objectNull(Object o) 
	{
		if (o == null) 
		{
			setToNull();
			return true;
		}
		return false;
	}

	/**
	 * @see SQLBit#setValue
	 *
	 */
	public final void setValue(InputStream theStream, int streamLength)
	{
		dataValue = null;
		stream = theStream;
		this.streamLength = streamLength;
	}

	protected final void setFrom(DataValueDescriptor theValue) throws StandardException {

		if (theValue instanceof SQLBinary)
		{
			SQLBinary theValueBinary = (SQLBinary) theValue;
			dataValue = theValueBinary.dataValue;
			stream = theValueBinary.stream;
			streamLength = theValueBinary.streamLength;
		}
		else
		{
			setValue(theValue.getBytes());
		}
	}

	/*
	** SQL Operators
	*/

	/**
	 * The = operator as called from the language module, as opposed to
	 * the storage module.
	 *
	 * @param left			The value on the left side of the =
	 * @param right			The value on the right side of the =
	 *						is not.
	 * @return	A SQL boolean value telling whether the two parameters are equal
	 *
	 * @exception StandardException		Thrown on error
	 */

	public final BooleanDataValue equals(DataValueDescriptor left,
							 DataValueDescriptor right)
								throws StandardException
	{
		boolean isEqual;

		if (left.isNull() || right.isNull())
		{
			isEqual = false;
		}
		else
		{	
			isEqual = SQLBinary.compare(left.getBytes(), right.getBytes()) == 0;
		}

		return SQLBoolean.truthValue(left,
									 right,
									 isEqual);
	}

	/**
	 * The <> operator as called from the language module, as opposed to
	 * the storage module.
	 *
	 * @param left			The value on the left side of the <>
	 * @param right			The value on the right side of the <>
	 *
	 * @return	A SQL boolean value telling whether the two parameters
	 * are not equal
	 *
	 * @exception StandardException		Thrown on error
	 */

	public final BooleanDataValue notEquals(DataValueDescriptor left,
							 DataValueDescriptor right)
								throws StandardException
	{
		boolean isNotEqual;

		if (left.isNull() || right.isNull())
		{
			isNotEqual = false;
		}
		else
		{	
			isNotEqual = SQLBinary.compare(left.getBytes(), right.getBytes()) != 0;
		}

		return SQLBoolean.truthValue(left,
									 right,
									 isNotEqual);
	}

	/**
	 * The < operator as called from the language module, as opposed to
	 * the storage module.
	 *
	 * @param left			The value on the left side of the <
	 * @param right			The value on the right side of the <
	 * @param result	The result of a previous call to this method, null
	 *					if not called yet.  NOTE: This is unused in this
	 *					method, because comparison operators always return
	 *					pre-allocated values.
	 *
	 * @return	A SQL boolean value telling whether the first operand is
	 *			less than the second operand
	 *
	 * @exception StandardException		Thrown on error
	 */

	public final BooleanDataValue lessThan(DataValueDescriptor left,
							 DataValueDescriptor right)
								throws StandardException
	{
		boolean isLessThan;

		if (left.isNull() || right.isNull())
		{
			isLessThan = false;
		}
		else
		{	
			isLessThan = SQLBinary.compare(left.getBytes(), right.getBytes()) < 0;
		}

		return SQLBoolean.truthValue(left,
									 right,
									 isLessThan);
	}

	/**
	 * The > operator as called from the language module, as opposed to
	 * the storage module.
	 *
	 * @param left			The value on the left side of the >
	 * @param right			The value on the right side of the >
	 *
	 * @return	A SQL boolean value telling whether the first operand is
	 *			greater than the second operand
	 *
	 * @exception StandardException		Thrown on error
	 */

	public final BooleanDataValue greaterThan(DataValueDescriptor left,
							 DataValueDescriptor right)
								throws StandardException
	{
		boolean isGreaterThan = false;

		if (left.isNull() || right.isNull())
		{
			isGreaterThan = false;
		}
		else
		{	
			isGreaterThan = SQLBinary.compare(left.getBytes(), right.getBytes()) > 0;
		}

		return SQLBoolean.truthValue(left,
									 right,
									 isGreaterThan);
	}

	/**
	 * The <= operator as called from the language module, as opposed to
	 * the storage module.
	 *
	 * @param left			The value on the left side of the <=
	 * @param right			The value on the right side of the <=
	 *
	 * @return	A SQL boolean value telling whether the first operand is
	 *			less than or equal to the second operand
	 *
	 * @exception StandardException		Thrown on error
	 */

	public final BooleanDataValue lessOrEquals(DataValueDescriptor left,
							 DataValueDescriptor right)
								throws StandardException
	{
		boolean isLessEquals = false;

		if (left.isNull() || right.isNull())
		{
			isLessEquals = false;
		}
		else
		{	
			isLessEquals = SQLBinary.compare(left.getBytes(), right.getBytes()) <= 0;
		}

		return SQLBoolean.truthValue(left,
									 right,
									 isLessEquals);
	}

	/**
	 * The >= operator as called from the language module, as opposed to
	 * the storage module.
	 *
	 * @param left			The value on the left side of the >=
	 * @param right			The value on the right side of the >=
	 *
	 * @return	A SQL boolean value telling whether the first operand is
	 *			greater than or equal to the second operand
	 *
	 * @exception StandardException		Thrown on error
	 */

	public final BooleanDataValue greaterOrEquals(DataValueDescriptor left,
							 DataValueDescriptor right)
								throws StandardException
	{
		boolean isGreaterEquals = false;

		if (left.isNull() || right.isNull())
		{
			isGreaterEquals = false;
		}
		else
		{	
			isGreaterEquals = SQLBinary.compare(left.getBytes(), right.getBytes()) >= 0;
		}

		return SQLBoolean.truthValue(left,
									 right,
									 isGreaterEquals);
	}


	/**
	 *
	 * This method implements the char_length function for bit.
	 *
	 * @param result	The result of a previous call to this method, null
	 *					if not called yet
	 *
	 * @return	A SQLInteger containing the length of the char value
	 *
	 * @exception StandardException		Thrown on error
	 *
	 * @see ConcatableDataValue#charLength
	 */

	public final NumberDataValue charLength(NumberDataValue result)
							throws StandardException
	{
		if (result == null)
		{
			result = new SQLInteger();
		}

		if (this.isNull())
		{
			result.setToNull();
			return result;
		}


		result.setValue(getValue().length);
		return result;
	}

	/**
	 * @see BitDataValue#concatenate
	 *
	 * @exception StandardException		Thrown on error
	 */
	public final BitDataValue concatenate(
				BitDataValue left,
				BitDataValue right,
				BitDataValue result)
		throws StandardException
	{
		if (left.isNull() || right.isNull())
		{
			result.setToNull();
			return result;
		}

		byte[] leftData = left.getBytes();
		byte[] rightData = right.getBytes();

		byte[] concatData = new byte[leftData.length + rightData.length];

		System.arraycopy(leftData, 0, concatData, 0, leftData.length);
		System.arraycopy(rightData, 0, concatData, leftData.length, rightData.length);


		result.setValue(concatData);
		return result;
	}

  
	/**
	 * The SQL substr() function.
	 *
	 * @param start		Start of substr
	 * @param length	Length of substr
	 * @param result	The result of a previous call to this method,
	 *					null if not called yet.
	 * @param maxLen	Maximum length of the result
	 *
	 * @return	A ConcatableDataValue containing the result of the substr()
	 *
	 * @exception StandardException		Thrown on error
	 */
	public final ConcatableDataValue substring(
				NumberDataValue start,
				NumberDataValue length,
				ConcatableDataValue result,
				int maxLen)
		throws StandardException
	{
		int startInt;
		int lengthInt;
		BitDataValue varbitResult;

		if (result == null)
		{
			result = new SQLVarbit();
		}

		varbitResult = (BitDataValue) result;

		/* The result is null if the receiver (this) is null or if the length is negative.
		 * Oracle docs don't say what happens if the start position or the length is a usernull.
		 * We will return null, which is the only sensible thing to do.
		 * (If the user did not specify a length then length is not a user null.)
		 */
		if (this.isNull() || start.isNull() || (length != null && length.isNull()))
		{
			varbitResult.setToNull();
			return varbitResult;
		}

		startInt = start.getInt();

		// If length is not specified, make it till end of the string
		if (length != null)
		{
			lengthInt = length.getInt();
		}
		else lengthInt = getLength() - startInt + 1;

		/* DB2 Compatibility: Added these checks to match DB2. We currently enforce these
		 * limits in both modes. We could do these checks in DB2 mode only, if needed, so
		 * leaving earlier code for out of range in for now, though will not be exercised
		 */
		if ((startInt <= 0 || lengthInt < 0 || startInt > getLength() ||
				lengthInt > getLength() - startInt + 1))
			throw StandardException.newException(SQLState.LANG_SUBSTR_START_OR_LEN_OUT_OF_RANGE);
			
		// Return null if length is non-positive
		if (lengthInt < 0)
		{
			varbitResult.setToNull();
			return varbitResult;
		}

		/* If startInt < 0 then we count from the right of the string */
		if (startInt < 0)
		{
			startInt += getLength();
			if (startInt < 0)
			{
				lengthInt += startInt;
				startInt = 0;
			}
			if (lengthInt + startInt > 0)
			{
				lengthInt += startInt;
			}
			else
			{
				lengthInt = 0;
			}
		}
		else if (startInt > 0)
		{
			/* java substr() is 0 based */
			startInt--;
		}

		/* Oracle docs don't say what happens if the window is to the
		 * left of the string.  Return "" if the window
		 * is to the left or right or if the length is 0.
		 */
		if (lengthInt == 0 ||
			lengthInt <= 0 - startInt ||
			startInt > getLength())
		{
			varbitResult.setValue(new byte[0]);
			return varbitResult;
		}

		if (lengthInt >= getLength() - startInt)
		{
			byte[] substring = new byte[dataValue.length - startInt];
			System.arraycopy(dataValue, startInt, substring, 0, substring.length);
			varbitResult.setValue(substring);
		}
		else
		{
			byte[] substring = new byte[lengthInt];
			System.arraycopy(dataValue, startInt, substring, 0, substring.length);
			varbitResult.setValue(substring);
		}

		return varbitResult;
	}

	/**
		Host variables are rejected if their length is
		bigger than the declared length, regardless of
		if the trailing bytes are the pad character.

		@exception StandardException Variable is too big.
	*/
	public final void checkHostVariable(int declaredLength) throws StandardException
	{
		// stream length checking occurs at the JDBC layer
		int variableLength = -1;
		if (stream == null)
		{
			if (dataValue != null)
				variableLength = dataValue.length;
		}
		else
		{
			variableLength = streamLength;
		}

		if (variableLength != -1 && variableLength > declaredLength)
				throw StandardException.newException(SQLState.LANG_STRING_TRUNCATION, getTypeName(), 
							"XX-RESOLVE-XX",
							String.valueOf(declaredLength));
	}

	/*
	 * String display of value
	 */

	public final String toString()
	{
		if (dataValue == null)
		{
			if (stream == null)
			{
				return "NULL";
			}
			else
			{
				if (SanityManager.DEBUG)
					SanityManager.THROWASSERT(
						"value is null, stream is not null");
				return "";
			}
		}
		else
		{
			return org.apache.derby.iapi.util.StringUtil.toHexString(dataValue, 0, dataValue.length);
		}
	}

	/*
	 * Hash code
	 */
	public final int hashCode()
	{
		try {
			if (getValue() == null)
				{
					return 0;
				}
		}
		catch (StandardException se)
		{
			if (SanityManager.DEBUG)
				SanityManager.THROWASSERT("Unexpected exception " + se);
			return 0;
		}

		/* Hash code is simply the sum of all of the bytes */
		byte[] bytes = dataValue;
		int hashcode = 0;

		// Build the hash code
		for (int index = 0 ; index < bytes.length; index++)
		{
			byte bv = bytes[index];
			if (bv != SQLBinary.PAD)
				hashcode += bytes[index];
		}

		return hashcode;
	}
	private static int compare(byte[] left, byte[] right) {

		int minLen = left.length;
		byte[] longer = right;
		if (right.length < minLen) {
			minLen = right.length;
			longer = left;
		}

		for (int i = 0; i < minLen; i++) {

			int lb = left[i] & 0xff;
			int rb = right[i] & 0xff;

			if (lb == rb)
				continue;

			return lb - rb;
		}

		// complete match on all the bytes for the smallest value.

		// if the longer value is all pad characters
		// then the values are equal.
		for (int i = minLen; i < longer.length; i++) {
			byte nb = longer[i];
			if (nb == SQLBinary.PAD)
				continue;

			// longer value is bigger.
			if (left == longer)
				return 1;
			return -1;
		}

		return 0;

	}
}
