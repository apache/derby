/*

   Derby - Class org.apache.derby.iapi.types.BinaryDecimal

   Copyright 2005 The Apache Software Foundation or its licensors, as applicable.

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

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.services.io.ArrayInputStream;
import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.iapi.services.sanity.SanityManager;

/**
 * SQL DECIMAL using raw data. Provides the basis for the
 * CDCDecimal implementation.
 * <P>
 * The on-disk format must match the SQLDecimal format so that
 * databases are portable across J2ME and J2SE environments.
 * <P>
 * The format of the byte array is defined by the return of the
 * java.math.BigInteger.toByteArray:, extracted here.
 * 
 * Returns a byte array containing the two's-complement representation of this BigInteger.
 * The byte array will be in big-endian byte-order: the most significant byte is in the zeroth element.
 *
 * This is the format for DECIMAL even if BigINteger is not available, e.g. OSGi ee.minimum.
 */

abstract class BinaryDecimal extends NumberDataType
	implements VariableSizeDataValue
{
	/**
	 * An unscaled value of 1 in two's complement
	 */
	private static final byte[] ONE_2C = {(byte) 0x01};
	
	/**
	 * The unscaled value as a binary two's complement array.
	*/
	protected byte[]		data2c;

	/**
	 * The SQL scale, zero or positive, of the value
	 */
	protected int			sqlScale;
	
	
	BinaryDecimal() {
	}
	
	/*
	** Methods about the DECIMAL type itself.
	*/

	/**
	 * DECIMAL implementation.
	 * Use DECIMAL to indicate to self that another
	 * passed in value is an instance of this type.
	 */
	public final int typeToBigDecimal()
	{
		return java.sql.Types.DECIMAL;
	}

	/** @see DataValueDescriptor#typePrecedence */
	public final int typePrecedence()
	{
		return TypeId.DECIMAL_PRECEDENCE;
	}
	
	/* Return DECIMAL as the type name.
	 * @see org.apache.derby.iapi.types.DataValueDescriptor#getTypeName()
	 */
	public final String getTypeName() {
		return TypeId.DECIMAL_NAME;
	}

	/**
	 * Return my format identifier.
	 * 
	 * @see org.apache.derby.iapi.services.io.TypedFormat#getTypeFormatId
	 */
	public final int getTypeFormatId() {
		return StoredFormatIds.SQL_DECIMAL_ID;
	}	
	
	/*
	** NULL handling.
	*/

	/**
	 * see if the decimal value is null.
	 */
	public boolean isNull()
	{
		return data2c == null;
	}	

	public void restoreToNull()
	{
		data2c = null;
	}

	/* Check the leftmost bit, if set the value is negative.
	 * NULL values return false.
	 * @see org.apache.derby.iapi.types.NumberDataType#isNegative()
	 */
	protected boolean isNegative() {
		return !isNull() && ((data2c[0] & 0x80) != 0);
	}
	
	
	/*
	** Methods to convert values into this DECIMAL
	*/
	
	/**
	 * Set the value from a long.
	 */
	public void setValue(long theValue)
	{
		byte[] rd = data2c;
		if (rd == null || rd.length < 8)
			rd = new byte[8];
		
		rd[0] = (byte)(theValue >>> 56);
		rd[1] = (byte)(theValue >>> 48);
		rd[2] = (byte)(theValue >>> 40);
		rd[3] = (byte)(theValue >>> 32);
		rd[4] = (byte)(theValue >>> 24);
		rd[5] = (byte)(theValue >>> 16);
		rd[6] = (byte)(theValue >>> 8);
		rd[7] = (byte) theValue;
		
		if (SanityManager.DEBUG)
		{
			data2c = rd;
			sqlScale = 0;
			try {
			if (theValue != getLong())
				SanityManager.THROWASSERT("BinaryDecimal invalid long conversion before reduce in "
						+ theValue + " out " + getLong());
			}
			catch (StandardException se)
			{
				SanityManager.THROWASSERT(se.toString());
			}
		}
		
		data2c = BinaryDecimal.reduceBytes2c(rd, 0, 8);
		sqlScale = 0;
		
		if (SanityManager.DEBUG)
		{
			try {
			if (theValue != getLong())
				SanityManager.THROWASSERT("BinaryDecimal invalid long conversion after reduce in "
						+ theValue + " out " + getLong());
			}
			catch (StandardException se)
			{
				SanityManager.THROWASSERT(se.toString());
			}
		}	
	}

	/**
	 * Set the value from an int, just copy 'byte-by-byte'
	 * from the int to a four byte array. Then reduce.
	 * @see NumberDataValue#setValue
	 */
	public final void setValue(int theValue)
	{
		byte[] rd = data2c;
		if (rd == null || rd.length < 4)
			rd = new byte[4];
		
		rd[0] = (byte)(theValue >>> 24);
		rd[1] = (byte)(theValue >>> 16);
		rd[2] = (byte)(theValue >>> 8);
		rd[3] = (byte) theValue;
			
		data2c = BinaryDecimal.reduceBytes2c(rd, 0, 4);
		sqlScale = 0;
	}
	
	/**
	 * Convert from a double, normalize and then convert as a String.
	 *
	 * @exception StandardException		Thrown on error
	 */
	public final void setValue(double theValue) throws StandardException
	{
		setCoreValue(NumberDataType.normalizeDOUBLE(theValue));
	}

	/**
	 * Convert from a float, normalize and then convert as a String.
	 *
	 */
	public final void setValue(float theValue)
		throws StandardException
	{
		setCoreValue((double)NumberDataType.normalizeREAL(theValue));
	}
	
	private void setCoreValue(double theValue) throws StandardException {
		setValue(Double.toString(theValue));
	}
	
	/**
	Called when setting a DECIMAL value internally or from
	through a procedure or function.
	Handles long in addition to BigDecimal to handle
	identity being stored as a long but returned as a DECIMAL.
	*/
	public void setValue(Number theValue) throws StandardException
	{
		if (SanityManager.ASSERT)
		{
			if (theValue != null &&
				!(theValue instanceof java.lang.Long))
				SanityManager.THROWASSERT("BinaryDecimal.setValue(Number) passed a " + theValue.getClass());
		}
	
		if (theValue == null)
			setToNull();
		else
			setValue(theValue.longValue());
	}
	
	/**
	 * Set this DECIMAL value from another DataValueDescriptor
	 */
	protected void setFrom(DataValueDescriptor dvd) throws StandardException
	{
	
		switch (dvd.typeToBigDecimal())
		{
			case Types.CHAR:
			case Types.DECIMAL: // TODO : direct copy
				
				setValue(dvd.getString());
				break;
			case Types.BIGINT:
				setValue(dvd.getLong());
			    break;
			default:
				super.setFrom(dvd);
		}
	}
	/*
	** Methods to get a value from this DECIMAL
	*/

	/**
	 * Return a int from this value.
	 * 
	 * @exception StandardException
	 *                this value is out of range for an int
	 */
	public final int getInt() throws StandardException
	{
		if (isNull())
			return 0;

		try {
			long lv = getLong();

			if ((lv >= Integer.MIN_VALUE) && (lv <= Integer.MAX_VALUE))
				return (int) lv;

		} catch (StandardException se) {
			// out of range error but with incorrect messgae (BIGINT)
			// fall through to correct message
		}

		throw StandardException.newException(SQLState.LANG_OUTSIDE_RANGE_FOR_DATATYPE, "INTEGER");
	}

	/**
	 * Return a byte from this value.
	 * 
	 * @exception StandardException
	 *                this value is out of range for a short
	 */
	public final byte getByte() throws StandardException {
		if (isNull())
			return (byte) 0;

		try {
			long lv = getLong();

			if ((lv >= Byte.MIN_VALUE) && (lv <= Byte.MAX_VALUE))
				return (byte) lv;

		} catch (StandardException se) {
			// out of range error but with incorrect messgae (BIGINT)
			// fall through to correct message
		}

		throw StandardException.newException(
				SQLState.LANG_OUTSIDE_RANGE_FOR_DATATYPE, "TINYINT");
	}

	/**
	 * Return a short from this value.
	 * @exception StandardException this value is out of range for a short
	 */
	public final short getShort() throws StandardException	
	{
		if (isNull())
			return (short)0;

		try {
			long lv = getLong();

			if ((lv >= Short.MIN_VALUE) && (lv <= Short.MAX_VALUE))
				return (short) lv;

		} catch (StandardException se) {
			// out of range error but with incorrect messgae (BIGINT)
			// fall through to correct message
		}

		throw StandardException.newException(SQLState.LANG_OUTSIDE_RANGE_FOR_DATATYPE, "SMALLINT");
	}
	
	
	/*
	** DECIMAL arithmetic methods.
	*/
	
	/**
	 * This method implements the + operator for DECIMAL.
	 *
	 * @param addend1	One of the addends
	 * @param addend2	The other addend
	 * @param result	The result of a previous call to this method, null
	 *					if not called yet
	 *
	 * @return	A SQLDecimal containing the result of the addition
	 *
	 * @exception StandardException		Thrown on error
	 */

	public final NumberDataValue plus(NumberDataValue addend1,
							NumberDataValue addend2,
							NumberDataValue result)
				throws StandardException
	{
		if (result == null)
		{
			result = (NumberDataValue) getNewNull();
		}

		if (addend1.isNull() || addend2.isNull())
		{
			result.setToNull();
			return result;
		}

		return plusNN(addend1, addend2, result);
	}
	
	/* (non-Javadoc)
	 * @see org.apache.derby.iapi.types.NumberDataValue#times(org.apache.derby.iapi.types.NumberDataValue, org.apache.derby.iapi.types.NumberDataValue, org.apache.derby.iapi.types.NumberDataValue)
	 */
	public final NumberDataValue times(NumberDataValue left, NumberDataValue right, NumberDataValue result)
	throws StandardException
	{
		if (result == null)
		{
			result = (NumberDataValue) getNewNull();
		}

		if (left.isNull() || right.isNull())
		{
			result.setToNull();
			return result;
		}		
		return timesNN(left, right, result);
	}
	public NumberDataValue divide(NumberDataValue dividend,
			 NumberDataValue divisor,
			 NumberDataValue result)
throws StandardException
{
return divide(dividend, divisor, result, -1);
}

	/**
	 * This method implements the / operator for BigDecimal/BigDecimal
	 *
	 * @param dividend	The numerator
	 * @param divisor	The denominator
	 * @param result	The result of a previous call to this method, null
	 *					if not called yet
	 * @param scale		The result scale, if < 0, calculate the scale according
	 *					to the actual values' sizes
	 *
	 * @return	A SQLDecimal containing the result of the division
	 *
	 * @exception StandardException		Thrown on error
	 */

	public final NumberDataValue divide(NumberDataValue dividend,
							 NumberDataValue divisor,
							 NumberDataValue result,
							 int scale)
				throws StandardException
	{
		if (result == null)
		{
			result = (NumberDataValue) getNewNull();
		}

		if (dividend.isNull() || divisor.isNull())
		{
			result.setToNull();
			return result;
		}
		
		return divideNN(dividend, divisor, result, scale);
	}	
	public final NumberDataValue minus(NumberDataValue left, NumberDataValue right, NumberDataValue result)
	throws StandardException
	{
		if (result == null)
		{
			result = (NumberDataValue) getNewNull();
		}

		if (left.isNull() || right.isNull())
		{
			result.setToNull();
			return result;
		}
		
		return minusNN(left, right, result);
	}
	
	/**
	 * Implement subtraction using addition and negation of the right value.
	 */
	public NumberDataValue minusNN(NumberDataValue left, NumberDataValue right, NumberDataValue result)
		throws StandardException
	{
		// Requires plusNN() correctly handles that its right argument and
		// result can be references to the same object.
		return plusNN(left, right.minus(result), result);
	}
		
	/*
	** Abstract methods for handling non-null arithmetic.
	** Eventually move these methods into NumberDataType
	** and directly compile to them when arguments cannot
	** be null. A performance optimization.
	*/
	
	
	/**
	 * Multiple two non-nullable values using DECIMAL arithmetic.
	 */
	public abstract NumberDataValue timesNN(NumberDataValue left,
			NumberDataValue right, NumberDataValue result)
			throws StandardException;

	/**
	 * Add two non-nullable values using DECIMAL arithmetic.
	 * For subclasses of BinaryDecimal, any implementation
	 * must handle the result and addend2 (right) being references
	 * to the same object.
	 */
	public abstract NumberDataValue plusNN(NumberDataValue addend1,
			NumberDataValue addend2, NumberDataValue result)
			throws StandardException;

	/**
	 * Divide two non-nullable values using DECIMAL arithmetic.
	 */
	public abstract NumberDataValue divideNN(NumberDataValue dividend,
			NumberDataValue divisor, NumberDataValue result, int scale)
			throws StandardException;
	
	/*
	** Methods that act directly on twos complement byte arrays.
	*/

	/**
	 * Compress the passed in byte array so that leading
	 * 0x00 and 0xff are removed when possible.
	 * E.g.
	 * 0x00000453 ->>> 0x0453
	 * 0xfffffff2 ->>> 0xf2
	 * 0xff192312 ->>> 0xff192312 (unchanged)
	 * 0xffff8039 ->>> 0x8039
	 * data2c is set to the compressed value.
	 * @param dataLength Valid length of data in data2c.
	 */
	private static byte[] reduceBytes2c(byte[] rd, int offset, int dataLength)
	{
		// look for leading zeros, if the value
		// is dataLength bytes long then look
		// at up to the first (dataLength - 1) bytes
		// to see if leading 0x00 can be removed.

		int leading;
		for (leading = 0; leading < (dataLength - 1); leading++)
		{
			if (rd[offset + leading] != (byte) 0)
				break;
			
			// if the hi bit of the next byte is set
			// then we cannot strip this 0x00 otherwise
			// the number will turn negative.
			if ((rd[offset + leading + 1] & 0x80) != 0)
				break;
		}

		if (leading == 0)
		{
			// now a similar trick with 0xff, but a slight
			// complication.
			for (; leading < (dataLength - 1); leading++)
			{
				// Need to check the highest byte of the
				// would-be remaining significant byte is
				// set to indicate this is still a negative number
				
				if ((rd[offset + leading] == (byte) 0xff) && ((rd[offset + leading+1] & (byte) 0x80) != 0))
					continue;
				break;
			}
		}
		
		if ((leading != 0) || (rd.length != dataLength))
		{
			byte[] reduced = new byte[dataLength - leading];
			System.arraycopy(rd, offset + leading, reduced, 0, reduced.length);
			return reduced;
		}
		
		return rd;
	}

	/**
	 * Return the SQL scale of this value, number of digits after the
	 * decimal point, or zero for a whole number.
	 */
	public int getDecimalValueScale()
	{
		if (isNull())
			return 0;
		
		return sqlScale;
	}	
	
	/*
	** I/O handling
	*/

	/** 
	 * Distill the Decimal to a byte array and
	 * Write out: <UL>
	 *	<LI> scale (unsigned byte) </LI>
	 *	<LI> length of byte array </LI>
	 *	<LI> the byte array </LI> </UL>
	 *
	 */
	public void writeExternal(ObjectOutput out) throws IOException 
	{
		// never called when value is null
		if (SanityManager.DEBUG)
			SanityManager.ASSERT(! isNull());

		out.writeByte(sqlScale);
		out.writeByte(data2c.length);
		out.write(data2c);
	}
	
	/** 
	 * Note the use of data2c: we reuse the array if the
	 * incoming array is the same length or smaller than
	 * the array length.  
	 * 
	 * @see java.io.Externalizable#readExternal 
	 */
	public void readExternal(ObjectInput in) throws IOException 
	{
		sqlScale = in.readUnsignedByte();
		int size = in.readUnsignedByte();

		/*
		** Allocate a new array if the data to read
		** is larger than the existing array, or if
		** we don't have an array yet.

        Need to use readFully below and NOT just read because read does not
        guarantee getting size bytes back, whereas readFully does (unless EOF).
        */
		if ((data2c == null) || size != data2c.length)
		{
			data2c = new byte[size];
		}
		in.readFully(data2c);

	}
	public void readExternalFromArray(ArrayInputStream in) throws IOException 
	{
		sqlScale = in.readUnsignedByte();
		int size = in.readUnsignedByte();

		/*
		** Allocate a new array if the data to read
		** is larger than the existing array, or if
		** we don't have an array yet.

        Need to use readFully below and NOT just read because read does not
        guarantee getting size bytes back, whereas readFully does (unless EOF).
        */
		if ((data2c == null) || size != data2c.length)
		{
			data2c = new byte[size];
		}
		in.readFully(data2c);
	}



	public final int getLength()
	{
		return getDecimalValuePrecision();
	}

	/* (non-Javadoc)
	 * @see org.apache.derby.iapi.types.DataValueDescriptor#getClone()
	 */
	public DataValueDescriptor getClone() {
		BinaryDecimal dvd = (BinaryDecimal) getNewNull();
		
		if (this.data2c != null)
		{
			dvd.data2c = new byte[data2c.length];
			System.arraycopy(data2c, 0, dvd.data2c, 0, data2c.length);
			dvd.sqlScale = sqlScale;
		}
		
		return dvd;
	}

	/* (non-Javadoc)
	 * @see org.apache.derby.iapi.types.DataValueDescriptor#setValueFromResultSet(java.sql.ResultSet, int, boolean)
	 */
	public void setValueFromResultSet(ResultSet resultSet, int colNumber, boolean isNullable) throws StandardException, SQLException {
		// TODO Auto-generated method stub
		throw StandardException.newException(SQLState.NOT_IMPLEMENTED);
		
	}

	/* (non-Javadoc)
	 * @see org.apache.derby.iapi.types.DataValueDescriptor#estimateMemoryUsage()
	 */
	public int estimateMemoryUsage() {
		// TODO Auto-generated method stub
		return 0;
	}
	
	public int hashCode()
	{
		if (isNull())
			return 0;

		try {
			return (int) Double.doubleToLongBits(getDouble());
		} catch (StandardException se)
		{
			return 0;
		}
	}
}
