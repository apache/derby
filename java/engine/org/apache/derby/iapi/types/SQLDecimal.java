/*

   Derby - Class org.apache.derby.iapi.types.SQLDecimal

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

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.iapi.services.io.Storable;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.services.cache.ClassSize;
import org.apache.derby.iapi.services.info.JVMInfo;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.lang.Math;
import java.lang.reflect.Method;
import java.lang.reflect.InvocationTargetException;
import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.IOException;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * SQLDecimal satisfies the DataValueDescriptor
 * interfaces (i.e., OrderableDataType). It implements a numeric/decimal column, 
 * e.g. for * storing a column value; it can be specified
 * when constructed to not allow nulls. Nullability cannot be changed
 * after construction, as it affects the storage size and mechanism.
 * <p>
 * Because OrderableDataType is a subtype of DataType,
 * SQLDecimal can play a role in either a DataType/Row
 * or a OrderableDataType/Row, interchangeably.
 * <p>
 * We assume the store has a flag for nullness of the value,
 * and simply return a 0-length array for the stored form
 * when the value is null.
 *
 * @author jamie
 */
public final class SQLDecimal extends NumberDataType implements VariableSizeDataValue
{
	public static final BigDecimal ZERO = BigDecimal.valueOf(0L);
	public static final BigDecimal ONE = BigDecimal.valueOf(1L);
	public static final BigDecimal MAXLONG_PLUS_ONE = BigDecimal.valueOf(Long.MAX_VALUE).add(ONE);
	public static final BigDecimal MINLONG_MINUS_ONE = BigDecimal.valueOf(Long.MIN_VALUE).subtract(ONE);



	/**
	 * object state.  Note that scale and precision are 
	 * always determined dynamically from value when
	 * it is not null.

       The field value can be null without the data value being null.
	   In this case the value is stored in rawData and rawScale. This
	   is to allow the minimal amount of work to read a SQLDecimal from disk.
	   Creating the BigDecimal is expensive as it requires allocating
	   three objects, the last two are a waste in the case the row does
	   not qualify or the row will be written out by the sorter before being
	   returned to the application.
		<P>
		This means that this field must be accessed for read indirectly through
		the getBigDecimal() method, and when setting it the rawData field must
		be set to null.

	 */
	private BigDecimal	value;

	/**
		See comments for value
	*/
	private byte[]		rawData;

	/**
		See comments for value
	*/
	private int			rawScale;

    private static final int BASE_MEMORY_USAGE = ClassSize.estimateBaseFromCatalog( SQLDecimal.class);
    private static final int BIG_DECIMAL_MEMORY_USAGE = ClassSize.estimateBaseFromCatalog( BigDecimal.class);

    public int estimateMemoryUsage()
    {
        int sz = BASE_MEMORY_USAGE;
        if( null != value)
            sz += BIG_DECIMAL_MEMORY_USAGE + (value.unscaledValue().bitLength() + 8)/8;
        if( null != rawData)
            sz += rawData.length;
        return sz;
    }


	////////////////////////////////////////////////////////////////////
	//
	// CLASS INTERFACE
	//
	////////////////////////////////////////////////////////////////////
	/** no-arg constructor, required by Formattable */
	public SQLDecimal() 
	{
	}

	public SQLDecimal(BigDecimal val)
	{
		value = val;
	}

	public SQLDecimal(BigDecimal val, int nprecision, int scale)
			throws StandardException
	{
		
		value = val;
		if ((value != null) && (scale >= 0))
		{
			value = value.setScale(scale, 
							BigDecimal.ROUND_DOWN);
		}
	}

	public SQLDecimal(String val) 
	{
		value = new BigDecimal(val);
	}

	/*
	 * DataValueDescriptor interface
	 * (mostly implemented in DataType)
	 *
	 */


	/**
	 * @exception StandardException thrown on failure to convert
	 */
	public int	getInt() throws StandardException
	{
		BigDecimal localValue = getBigDecimal();
		if (localValue == null)
			return 0;

		try {
			long lv = getLong();

			if ((lv >= Integer.MIN_VALUE) && (lv <= Integer.MAX_VALUE))
				return (int) lv;

		} catch (StandardException se) {
		}

		throw StandardException.newException(SQLState.LANG_OUTSIDE_RANGE_FOR_DATATYPE, "INTEGER");
	}

	/**
	 * @exception StandardException thrown on failure to convert
	 */
	public byte	getByte() throws StandardException
	{
		BigDecimal localValue = getBigDecimal();

		if (localValue == null)
			return (byte)0;

		try {
			long lv = getLong();

			if ((lv >= Byte.MIN_VALUE) && (lv <= Byte.MAX_VALUE))
				return (byte) lv;

		} catch (StandardException se) {
		}

		throw StandardException.newException(SQLState.LANG_OUTSIDE_RANGE_FOR_DATATYPE, "TINYINT");
	}

	/**
	 * @exception StandardException thrown on failure to convert
	 */
	public short	getShort() throws StandardException
	{
		BigDecimal localValue = getBigDecimal();
		if (localValue == null)
			return (short)0;

		try {
			long lv = getLong();

			if ((lv >= Short.MIN_VALUE) && (lv <= Short.MAX_VALUE))
				return (short) lv;

		} catch (StandardException se) {
		}

		throw StandardException.newException(SQLState.LANG_OUTSIDE_RANGE_FOR_DATATYPE, "SMALLINT");
	}

	/**
	 * @exception StandardException thrown on failure to convert
	 */
	public long	getLong() throws StandardException
	{
		BigDecimal localValue = getBigDecimal();
		if (localValue == null)
			return (long)0;

		// Valid range for long is
		//   greater than Long.MIN_VALUE - 1
		// *and*
		//   less than Long.MAX_VALUE + 1
		//
		// This ensures that DECIMAL values with an integral value
		// equal to the Long.MIN/MAX_VALUE round correctly to those values.
		// e.g. 9223372036854775807.1  converts to 9223372036854775807
		// this matches DB2 UDB behaviour

		if (   (localValue.compareTo(SQLDecimal.MINLONG_MINUS_ONE) == 1)
			&& (localValue.compareTo(SQLDecimal.MAXLONG_PLUS_ONE) == -1)) {

			return localValue.longValue();
		}

		throw StandardException.newException(SQLState.LANG_OUTSIDE_RANGE_FOR_DATATYPE, "BIGINT");
	}

	/**
	 * @exception StandardException thrown on failure to convert
	 */
	public float getFloat() throws StandardException
	{
		BigDecimal localValue = getBigDecimal();
		if (localValue == null)
			return (float)0;

		// If the BigDecimal is out of range for the float
		// then positive or negative infinity is returned.
		float value = NumberDataType.normalizeREAL(localValue.floatValue());

		return value;
	}

	/**
	 * 
	 * If we have a value that is greater than the maximum double,
	 * exception is thrown.  Otherwise, ok.  If the value is less
	 * than can be represented by a double, ti will get set to
	 * the smallest double value.
	 *
	 * @exception StandardException thrown on failure to convert
	 */
	public double getDouble() throws StandardException
	{
		BigDecimal localValue = getBigDecimal();
		if (localValue == null)
			return (double)0;

		// If the BigDecimal is out of range for double
		// then positive or negative infinity is returned.
		double value = NumberDataType.normalizeDOUBLE(localValue.doubleValue());
		return value;
	}

	public BigDecimal	getBigDecimal()
	{
		if ((value == null) && (rawData != null)) 
		{
			value = new BigDecimal(new BigInteger(rawData), rawScale);
		}

		return value;
	}

    // 0 or null is false, all else is true
	public boolean	getBoolean()
	{

		BigDecimal localValue = getBigDecimal();
		if (localValue == null)
			return false;

		return localValue.compareTo(ZERO) != 0;
	}

	public String	getString()
	{
		BigDecimal localValue = getBigDecimal();
		if (localValue == null)
			return null;
		else if (JVMInfo.JDK_ID < 6)
			return localValue.toString();
        else
        {
            // use reflection so we can still compile using JDK1.4
            // if we are prepared to require 1.5 to compile then this can be a direct call
            try {
                return (String) toPlainString.invoke(localValue, null);
            } catch (IllegalAccessException e) {
                // can't happen based on the JDK spec
                throw new IllegalAccessError("toPlainString");
            } catch (InvocationTargetException e) {
                Throwable t = e.getTargetException();
                if (t instanceof RuntimeException) {
                    throw (RuntimeException) t;
                } else if (t instanceof Error) {
                    throw (Error) t;
                } else {
                    // can't happen
                    throw new IncompatibleClassChangeError("toPlainString");
                }
            }
        }
	}

    private static final Method toPlainString;
    static {
        Method m;
        try {
            m = BigDecimal.class.getMethod("toPlainString", null);
        } catch (NoSuchMethodException e) {
            m = null;
        }
        toPlainString = m;
    }

	public Object	getObject()
	{
		/*
		** BigDecimal is immutable
		*/
		return getBigDecimal();
	}

	/**
	 * @see DataValueDescriptor#setValue
	 *
	 */	
	public void setValue(Object theValue)
		throws StandardException
	{
		rawData = null;
		if ((theValue instanceof BigDecimal) ||
			(theValue == null))
		{
			setValue((BigDecimal)theValue);
		}
		else if (theValue instanceof Number)
		{
			value = new BigDecimal(((Number)theValue).doubleValue());
		}
		else
		{
			genericSetObject(theValue);
		}
	}
	protected void setFrom(DataValueDescriptor theValue) throws StandardException {

		setValue(theValue.getBigDecimal());
	}

	public int	getLength()
	{
		return getPrecision();
	}

	// this is for DataType's error generator
	public String getTypeName()
	{
		return TypeId.DECIMAL_NAME;
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
		return StoredFormatIds.SQL_DECIMAL_ID;
	}

	/*
	 * see if the decimal value is null.
	 */
	/** @see Storable#isNull */
	public boolean isNull()
	{
		return (value == null) && (rawData == null);
	}

	/** 
	 * Distill the BigDecimal to a byte array and
	 * write out: <UL>
	 *	<LI> scale (int) </LI>
	 *	<LI> length of byte array </LI>
	 *	<LI> the byte array </LI> </UL>
	 *
	 */
	public void writeExternal(ObjectOutput out) throws IOException 
	{
		// never called when value is null
		if (SanityManager.DEBUG)
			SanityManager.ASSERT(! isNull());

		int scale;
		byte[] byteArray;

		if (value != null) {
			scale = value.scale();
			BigInteger bi = value.unscaledValue();
			byteArray = bi.toByteArray();
		} else {
			scale = rawScale;
			byteArray = rawData;
		}

		out.writeByte(scale);
		out.writeByte(byteArray.length);
		out.write(byteArray);
	}

	/** 
	 * Note the use of rawData: we reuse the array if the
	 * incoming array is the same length or smaller than
	 * the array length.  
	 * 
	 * @see java.io.Externalizable#readExternal 
	 */
	public void readExternal(ObjectInput in) throws IOException 
	{
		// clear the previous value to ensure that the
		// rawData value will be used
		value = null;

		rawScale = in.readUnsignedByte();
		int size = in.readUnsignedByte();

		/*
		** Allocate a new array if the data to read
		** is larger than the existing array, or if
		** we don't have an array yet.

        Need to use readFully below and NOT just read because read does not
        guarantee getting size bytes back, whereas readFully does (unless EOF).
        */
		if ((rawData == null) || size != rawData.length)
		{
			rawData = new byte[size];
		}
		in.readFully(rawData);

	}
	public void readExternalFromArray(ArrayInputStream in) throws IOException 
	{
		// clear the previous value to ensure that the
		// rawData value will be used
		value = null;

		rawScale = in.readUnsignedByte();
		int size = in.readUnsignedByte();

		/*
		** Allocate a new array if the data to read
		** is larger than the existing array, or if
		** we don't have an array yet.

        Need to use readFully below and NOT just read because read does not
        guarantee getting size bytes back, whereas readFully does (unless EOF).
        */
		if ((rawData == null) || size != rawData.length)
		{
			rawData = new byte[size];
		}
		in.readFully(rawData);
	}

	/**
	 * @see Storable#restoreToNull
	 *
	 */
	public void restoreToNull()
	{
		value = null;
		rawData = null;
	}


	/** @exception StandardException		Thrown on error */
	protected int typeCompare(DataValueDescriptor arg) throws StandardException
	{
		BigDecimal otherValue = arg.getBigDecimal();

		return getBigDecimal().compareTo(otherValue);
	}

	/*
	 * DataValueDescriptor interface
	 */

	/**
	 * <B> WARNING </B> clone is a shallow copy
 	 * @see DataValueDescriptor#getClone 
	 */
	public DataValueDescriptor getClone()
	{
		return new SQLDecimal(getBigDecimal());
	}

	/**
	 * @see DataValueDescriptor#getNewNull
	 */
	public DataValueDescriptor getNewNull()
	{
		return new SQLDecimal();
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
			ResultSetMetaData rsmd = resultSet.getMetaData();
			value = resultSet.getBigDecimal(colNumber,
											rsmd.getScale(colNumber));
			rawData = null;
	}
	/**
		Set the value into a PreparedStatement.

		@exception SQLException Error setting value in PreparedStatement
	*/
	public final void setInto(PreparedStatement ps, int position) throws SQLException {

		if (isNull()) {
			ps.setNull(position, java.sql.Types.DECIMAL);
			return;
		}

		ps.setBigDecimal(position, getBigDecimal());
	}
	
	/**
	 *
	 * <B> WARNING </B> there is no checking to make sure
	 * that theValue doesn't exceed the precision/scale of
	 * the current SQLDecimal.  It is just assumed that the
	 * SQLDecimal is supposed to take the precision/scale of
	 * the BigDecimalized String.
	 *
	 * @exception StandardException throws NumberFormatException
	 *		when the String format is not recognized.
	 */
	public void setValue(String theValue) throws StandardException
	{
		rawData = null;

		if (theValue == null)
		{
			value = null;
		}
		else
		{
		    try 
			{
				theValue = theValue.trim();
		        value = new BigDecimal(theValue);
				rawData = null;
			} catch (NumberFormatException nfe) 
			{
			    throw invalidFormat();
			}
		}
	}

	/**
	 * @see NumberDataValue#setValue
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void setValue(double theValue) throws StandardException
	{
		setCoreValue(NumberDataType.normalizeDOUBLE(theValue));
	}

	/**
	 * @see NumberDataValue#setValue
	 *
	 */
	public void setValue(float theValue)
		throws StandardException
	{
		setCoreValue((double)NumberDataType.normalizeREAL(theValue));
	}

	/**
	 * @see NumberDataValue#setValue
	 *
	 */
	public void setValue(long theValue)
	{
		value = BigDecimal.valueOf(theValue);
		rawData = null;
	}

	/**
	 * @see NumberDataValue#setValue
	 *
	 */
	public void setValue(int theValue)
	{
		setValue((long)theValue);
	}

	/**
	 * @see NumberDataValue#setValue
	 *
	 */
	public void setValue(short theValue)
	{
		setValue((long)theValue);
	}

	/**
	 * @see NumberDataValue#setValue
	 *
	 */
	public void setValue(byte theValue)
	{
		setValue((long)theValue);
	}

	/**
	 * @see NumberDataValue#setValue
	 *
	 */
	public void setValue(BigDecimal theValue)
	{
		setCoreValue(theValue);
	}

	/**
	 * @see NumberDataValue#setValue
	 *
	 */
	public void setValue(boolean theValue)
	{
		setCoreValue(theValue ? ONE : ZERO);
	}

	/*
	 * DataValueDescriptor interface
	 */

	/** @see DataValueDescriptor#typePrecedence */
	public int typePrecedence()
	{
		return TypeId.DECIMAL_PRECEDENCE;
	}
    // END DataValueDescriptor interface

	private void setCoreValue(BigDecimal theValue)
	{
		value = theValue;
		rawData = null;
	}

	private void setCoreValue(double theValue) {
		value = new BigDecimal(theValue);
		rawData = null;
	}

	/**
	 * Normalization method - this method may be called when putting
	 * a value into a SQLDecimal, for example, when inserting into a SQLDecimal
	 * column.  See NormalizeResultSet in execution.
	 * <p>
	 * Note that truncation is allowed on the decimal portion
	 * of a numeric only.	
	 *
	 * @param desiredType	The type to normalize the source column to
	 * @param source		The value to normalize
	 *
	 * @throws StandardException				Thrown for null into
	 *											non-nullable column, and for
	 *											truncation error
	 */
	public void normalize(
				DataTypeDescriptor desiredType,
				DataValueDescriptor source)
						throws StandardException
	{
		int desiredScale = desiredType.getScale();
		int desiredPrecision = desiredType.getPrecision();

		setValue(source.getBigDecimal());
		setWidth(desiredPrecision, desiredScale, true);
	}


	/*
	** SQL Operators
	*/


	/**
	 * This method implements the + operator for "double + double".
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

	public NumberDataValue plus(NumberDataValue addend1,
							NumberDataValue addend2,
							NumberDataValue result)
				throws StandardException
	{
		if (result == null)
		{
			result = new SQLDecimal();
		}

		if (addend1.isNull() || addend2.isNull())
		{
			result.setToNull();
			return result;
		}

		result.setValue(addend1.getBigDecimal().add(addend2.getBigDecimal()));
		return result;
	}

	/**
	 * This method implements the - operator for "decimal - decimal".
	 *
	 * @param left	The value to be subtracted from
	 * @param right	The value to be subtracted
	 * @param result	The result of a previous call to this method, null
	 *					if not called yet
	 *
	 * @return	A SQLDecimal containing the result of the subtraction
	 *
	 * @exception StandardException		Thrown on error
	 */

	public NumberDataValue minus(NumberDataValue left,
							NumberDataValue right,
							NumberDataValue result)
				throws StandardException
	{
		if (result == null)
		{
			result = new SQLDecimal();
		}

		if (left.isNull() || right.isNull())
		{
			result.setToNull();
			return result;
		}

		result.setValue(left.getBigDecimal().subtract(right.getBigDecimal()));
		return result;
	}

	/**
	 * This method implements the * operator for "double * double".
	 *
	 * @param left	The first value to be multiplied
	 * @param right	The second value to be multiplied
	 * @param result	The result of a previous call to this method, null
	 *					if not called yet
	 *
	 * @return	A SQLDecimal containing the result of the multiplication
	 *
	 * @exception StandardException		Thrown on error
	 */

	public NumberDataValue times(NumberDataValue left,
							NumberDataValue right,
							NumberDataValue result)
				throws StandardException
	{
		if (result == null)
		{
			result = new SQLDecimal();
		}

		if (left.isNull() || right.isNull())
		{
			result.setToNull();
			return result;
		}

		result.setValue(left.getBigDecimal().multiply(right.getBigDecimal()));
		return result;
	}

	/**
	 * This method implements the / operator for BigDecimal/BigDecimal
	 *
	 * @param dividend	The numerator
	 * @param divisor	The denominator
	 * @param result	The result of a previous call to this method, null
	 *					if not called yet
	 *
	 * @return	A SQLDecimal containing the result of the division
	 *
	 * @exception StandardException		Thrown on error
	 */

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

	public NumberDataValue divide(NumberDataValue dividend,
							 NumberDataValue divisor,
							 NumberDataValue result,
							 int scale)
				throws StandardException
	{
		if (result == null)
		{
			result = new SQLDecimal();
		}

		if (dividend.isNull() || divisor.isNull())
		{
			result.setToNull();
			return result;
		}

		BigDecimal divisorBigDecimal = divisor.getBigDecimal();

		if (divisorBigDecimal.compareTo(ZERO) == 0)
		{
			throw  StandardException.newException(SQLState.LANG_DIVIDE_BY_ZERO);
		}
		BigDecimal dividendBigDecimal = dividend.getBigDecimal();

		/*
		** Set the result scale to be either the passed in scale, whcih was
		** calculated at bind time to be max(ls+rp-rs+1, 4), where ls,rp,rs
		** are static data types' sizes, which are predictable and stable
		** (for the whole result set column, eg.); otherwise dynamically
		** calculates the scale according to actual values.  Beetle 3901
		*/
		result.setValue(dividendBigDecimal.divide(
									divisorBigDecimal,
									scale > -1 ? scale :
									Math.max((dividendBigDecimal.scale() + 
											getWholeDigits(divisorBigDecimal) +
											1), 
										NumberDataValue.MIN_DECIMAL_DIVIDE_SCALE),
									BigDecimal.ROUND_DOWN));
		return result;
	}

	/**
	 * This method implements the unary minus operator for double.
	 *
	 * @param result	The result of a previous call to this method, null
	 *					if not called yet
	 *
	 * @return	A SQLDecimal containing the result of the division
	 *
	 * @exception StandardException		Thrown on error
	 */

	public NumberDataValue minus(NumberDataValue result)
									throws StandardException
	{
		if (result == null)
		{
			result = new SQLDecimal();
		}

		if (this.isNull())
		{
			result.setToNull();
			return result;
		}

		result.setValue(getBigDecimal().negate());
		return result;
	}

    /**
     * This method implements the isNegative method.
     * 
     * @return  A boolean.  If this.value is negative, return true.
     *          For positive values or null, return false.
     */

    protected boolean isNegative()
    {
        return !isNull() && (getBigDecimal().compareTo(ZERO) == -1);
    }
    
	/*
	 * String display of value
	 */
	public String toString()
	{
		if (isNull())
			return "NULL";
		else
			return getBigDecimal().toString();
	}

	/*
	 * Hash code
	 */
	public int hashCode()
	{
		long longVal;
		BigDecimal localValue = getBigDecimal();

		double doubleVal = (localValue != null) ? localValue.doubleValue() : 0;

		if (Double.isInfinite(doubleVal))
		{
			/*
			 ** This loses the fractional part, but it probably doesn't
			 ** matter for numbers that are big enough to overflow a double -
			 ** it's probably rare for numbers this big to be different only in
			 ** their fractional parts.
			 */
			longVal = localValue.longValue();
		}
		else
		{
			longVal = (long) doubleVal;
			if (longVal != doubleVal)
			{
				longVal = Double.doubleToLongBits(doubleVal);
			}
		}

		return (int) (longVal ^ (longVal >> 32));
	}

	///////////////////////////////////////////////////////////////////////////////
	//
	// VariableSizeDataValue interface
	//
	///////////////////////////////////////////////////////////////////////////////

	/**
	 * Set the precision/scale of the to the desired values. 
	 * Used when CASTing.  Ideally we'd recycle normalize(), but
	 * the use is different.  
	 *
	 * @param desiredPrecision	the desired precision -- IGNORE_PREICISION
	 *					if it is to be ignored.
	 * @param desiredScale	the desired scale 
	 * @param errorOnTrunc	throw error on truncation (ignored -- 
	 *		always thrown if we truncate the non-decimal part of
	 *		the value)
	 * @return this with the target width
	 *
	 * @exception StandardException		Thrown on non-zero truncation
	 *		if errorOnTrunc is true	
	 */
	public DataValueDescriptor setWidth(int desiredPrecision, 
			int desiredScale,
			boolean errorOnTrunc)
			throws StandardException
	{
		if (isNull())
			return this;

		// the getWholeDigits() call will ensure via getBigDecimal()
		// that the rawData is translated into the BigDecimal in value.
		if (desiredPrecision != IGNORE_PRECISION &&
			((desiredPrecision - desiredScale) <  getWholeDigits()))
		{
			throw StandardException.newException(SQLState.LANG_OUTSIDE_RANGE_FOR_DATATYPE, 
									("DECIMAL/NUMERIC("+desiredPrecision+","+desiredScale+")"));
		}
		value = value.setScale(desiredScale, BigDecimal.ROUND_DOWN);
		rawData = null;
		return this;
	}

	public int getPrecision()
	{
		return getPrecision(getBigDecimal());
	}
	/**
	 *
	 * @param decimalValue the big decimal
	 *
	 * @return the precision
	 */	
	public static int getPrecision(BigDecimal decimalValue)
	{
		if ((decimalValue == null) ||
			 decimalValue.equals(ZERO))
		{
			return 0;
		}	

		return getWholeDigits(decimalValue) + decimalValue.scale();
	}

	public int getScale()
	{
		BigDecimal localValue = getBigDecimal();
		return (localValue == null) ? 0 : localValue.scale();
	}

	public int getWholeDigits()
	{
		return getWholeDigits(getBigDecimal());
	}

	public static int getWholeDigits(BigDecimal decimalValue)
	{
		if ((decimalValue == null) ||
			 decimalValue.equals(ZERO))
		{
			return 0;
		}

        /**
         * if ONE > abs(value) then the number of whole digits is 0
         */
        decimalValue = decimalValue.abs();
        if (ONE.compareTo(decimalValue) == 1)
        {
            return 0;
        }

		String s = decimalValue.toString();
        return (decimalValue.scale() == 0) ? s.length() : s.indexOf('.');
	}

	/**
	 * Return the value field
	 *
	 * @return BigDecimal
	 */
	public BigDecimal getValue()
	{
		return getBigDecimal();
	}
}
