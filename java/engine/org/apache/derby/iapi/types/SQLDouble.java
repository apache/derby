/*

   Derby - Class org.apache.derby.iapi.types.SQLDouble

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

import org.apache.derby.iapi.services.io.FormatIdUtil;
import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.iapi.services.io.Storable;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.types.BooleanDataValue;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.NumberDataValue;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.TypeId;

import org.apache.derby.iapi.services.cache.ClassSize;

import org.apache.derby.iapi.types.NumberDataType;
import org.apache.derby.iapi.types.SQLBoolean;

import java.math.BigDecimal;

import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.IOException;

import java.sql.ResultSet;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * SQLDouble satisfies the DataValueDescriptor
 * interfaces (i.e., OrderableDataType). It implements a double column, 
 * e.g. for * storing a column value; it can be specified
 * when constructed to not allow nulls. Nullability cannot be changed
 * after construction, as it affects the storage size and mechanism.
 * <p>
 * Because OrderableDataType is a subtype of DataType,
 * SQLDouble can play a role in either a DataType/Row
 * or a OrderableDataType/Row, interchangeably.
 * <p>
 * We assume the store has a flag for nullness of the value,
 * and simply return a 0-length array for the stored form
 * when the value is null.
 * <p>
 * PERFORMANCE: There are likely alot of performance improvements
 * possible for this implementation -- it new's Double
 * more than it probably wants to.
 * <p>
 * This is modeled after SQLInteger.
 * <p>
 * We don't let doubles get constructed with NaN or Infinity values, and
 * check for those values where they can occur on operations, so the
 * set* operations do not check for them coming in.
 *
 * @author ames
 */
public final class SQLDouble extends NumberDataType
{

	/*
	 * DataValueDescriptor interface
	 * (mostly implemented in DataType)
	 */


    // JDBC is lax in what it permits and what it
	// returns, so we are similarly lax
	// @see DataValueDescriptor
	/**
	 * @exception StandardException thrown on failure to convert
	 */
	public int	getInt() throws StandardException
	{
	    // REMIND: do we want to check for truncation?
		if ((value > (((double) Integer.MAX_VALUE) + 1.0d)) || (value < (((double) Integer.MIN_VALUE) - 1.0d)))
			throw StandardException.newException(SQLState.LANG_OUTSIDE_RANGE_FOR_DATATYPE, "INTEGER");
		return (int)value;
	}

	/**
	 * @exception StandardException thrown on failure to convert
	 */
	public byte	getByte() throws StandardException
	{
		if ((value > (((double) Byte.MAX_VALUE) + 1.0d)) || (value < (((double) Byte.MIN_VALUE) - 1.0d)))
			throw StandardException.newException(SQLState.LANG_OUTSIDE_RANGE_FOR_DATATYPE, "TINYINT");
		return (byte) value;
	}

	/**
	 * @exception StandardException thrown on failure to convert
	 */
	public short	getShort() throws StandardException
	{
		if ((value > (((double) Short.MAX_VALUE) + 1.0d)) || (value < (((double) Short.MIN_VALUE) - 1.0d)))
			throw StandardException.newException(SQLState.LANG_OUTSIDE_RANGE_FOR_DATATYPE, "SMALLINT");
		return (short) value;
	}

	/**
	 * @exception StandardException thrown on failure to convert
	 */
	public long	getLong() throws StandardException
	{
		if ((value > (((double) Long.MAX_VALUE) + 1.0d)) || (value < (((double) Long.MIN_VALUE) - 1.0d)))
			throw StandardException.newException(SQLState.LANG_OUTSIDE_RANGE_FOR_DATATYPE, "BIGINT");
		return (long) value;
	}

	/**
	 * @exception StandardException thrown on failure to convert
	 */
	public float	getFloat() throws StandardException
	{
		if (Math.abs(value) > Float.MAX_VALUE)
			throw StandardException.newException(SQLState.LANG_OUTSIDE_RANGE_FOR_DATATYPE, TypeId.REAL_NAME);
		return (float) value;
	}

	public double	getDouble()
	{
		/* This value is bogus if the SQLDouble is null */
		return value;
	}

	public BigDecimal	getBigDecimal()
	{
		if (isNull()) return null;
		return new BigDecimal(value);
	}

    // for lack of a specification: getDouble()==0 gives true
    // independent of the NULL flag
	public boolean	getBoolean()
	{
		return (value != 0);
	}

	public String	getString()
	{
		if (isNull())
			return null;
		else
			return Double.toString(value);
	}

	public Object	getObject()
	{
		// REMIND: could create one Double and reuse it?
		if (isNull())
			return null;
		else
			return new Double(value);
	}

	/**
	 * @see DataValueDescriptor#setValue
	 *
	 * @exception StandardException on error
	 */	
	public void setValue(Object theValue)
		throws StandardException
	{
		if (theValue == null)
		{
			setToNull();
		}
		else if (theValue instanceof Number)
		{
			this.setValue(((Number)theValue).doubleValue());
		}
		else
		{
			genericSetObject(theValue);
		}
	}
	protected void setFrom(DataValueDescriptor theValue) throws StandardException {
		setValue(theValue.getDouble());
	}

	public int	getLength()
	{
		return DOUBLE_LENGTH;
	}

	// this is for DataType's error generator
	public String getTypeName()
	{
		return TypeId.DOUBLE_NAME;
	}

	/*
	 * Storable interface, implies Externalizable, TypedFormat
	 */

	/**
		Return my format identifier.

		@see org.apache.derby.iapi.services.io.TypedFormat#getTypeFormatId
	*/
	public int getTypeFormatId() {
		return StoredFormatIds.SQL_DOUBLE_ID;
	}

	/*
	 * see if the double value is null.
	 */
	/** @see Storable#isNull */
	public boolean isNull()
	{
		return isnull;
	}

	public void writeExternal(ObjectOutput out) throws IOException {


		// never called when value is null
		if (SanityManager.DEBUG)
			SanityManager.ASSERT(! isNull());

		out.writeDouble(value);
	}

	/** @see java.io.Externalizable#readExternal */
	public void readExternal(ObjectInput in) throws IOException {

		value = in.readDouble();
		isnull = false;
	}

	/** @see java.io.Externalizable#readExternal */
	public void readExternalFromArray(ArrayInputStream in) throws IOException {

		value = in.readDouble();
		isnull = false;
	}

	/**
	 * @see Storable#restoreToNull
	 *
	 */

	public void restoreToNull()
	{
		value = 0;
		isnull = true;
	}


	/** @exception StandardException		Thrown on error */
	protected int typeCompare(DataValueDescriptor arg) throws StandardException
	{
		/* neither are null, get the value */

		double thisValue = this.getDouble();

		double otherValue = arg.getDouble();

		if (thisValue == otherValue)
			return 0;
		else if (thisValue > otherValue)
			return 1;
		else
			return -1;
	}

	/*
	 * DataValueDescriptor interface
	 */

	/** @see DataValueDescriptor#getClone */
	public DataValueDescriptor getClone()
	{
		try 
		{
			return new SQLDouble(value, isnull);
		} catch (StandardException se) 
		{
			if (SanityManager.DEBUG)
				SanityManager.THROWASSERT(
					"error on clone, se = " + se +
					" value = " + value +
					" isnull = " + isnull);
			return null;
		}
	}

	/**
	 * @see DataValueDescriptor#getNewNull
	 */
	public DataValueDescriptor getNewNull()
	{
		return new SQLDouble();
	}

	/** 
	 * @see DataValueDescriptor#setValueFromResultSet 
	 *
	 * @exception StandardException		Thrown on error
	 * @exception SQLException		Thrown on error
	 */
	public void setValueFromResultSet(ResultSet resultSet, int colNumber,
									  boolean isNullable)
		throws StandardException, SQLException
	{
			double dv = resultSet.getDouble(colNumber);
			isnull = (isNullable && resultSet.wasNull());
            if (isnull)
                value = 0;
            else 
                value = NumberDataType.normalizeDOUBLE(dv);
	}
	/**
		Set the value into a PreparedStatement.

		@exception SQLException Error setting value in PreparedStatement
	*/
	public final void setInto(PreparedStatement ps, int position) throws SQLException {

		if (isNull()) {
			ps.setNull(position, java.sql.Types.DOUBLE);
			return;
		}

		ps.setDouble(position, value);
	}
	/**
		Set this value into a ResultSet for a subsequent ResultSet.insertRow
		or ResultSet.updateRow. This method will only be called for non-null values.

		@exception SQLException thrown by the ResultSet object
		@exception StandardException thrown by me accessing my value.
	*/
	public final void setInto(ResultSet rs, int position) throws SQLException, StandardException {
		rs.updateDouble(position, value);
	}
	/*
	 * class interface
	 */

	/*
	 * constructors
	 */

	/** no-arg constructor, required by Formattable */
    // This constructor also gets used when we are
    // allocating space for a double.
	public SQLDouble() {
		isnull = true;
	}

	public SQLDouble(double val) throws StandardException
	{
		value = NumberDataType.normalizeDOUBLE(val);
	}

	public SQLDouble(Double obj) throws StandardException {
		if (isnull = (obj == null))
            ;
		else
			value = NumberDataType.normalizeDOUBLE(obj.doubleValue());
	}

	private SQLDouble(double val, boolean startsnull) throws StandardException
	{
		value = NumberDataType.normalizeDOUBLE(val); // maybe only do if !startsnull
		isnull = startsnull;
	}

	/**
		@exception StandardException throws NumberFormatException
			when the String format is not recognized.
	 */
	public void setValue(String theValue) throws StandardException
	{
		if (theValue == null)
		{
			value = 0;
			isnull = true;
		}
		else
		{
			double doubleValue = 0;
		    try {
                // ??? jsk: rounding???
		        doubleValue = Double.valueOf(theValue.trim()).doubleValue();
			} catch (NumberFormatException nfe) {
			    throw invalidFormat();
			}
			value = NumberDataType.normalizeDOUBLE(doubleValue);
			isnull = false;
		}
	}

	/**
	 * @exception StandardException on NaN or Infinite double
	 */
	public void setValue(double theValue) throws StandardException
	{
		value = NumberDataType.normalizeDOUBLE(theValue);
		isnull = false;
	}

	/**
	 * @exception StandardException on NaN or Infinite float
	 */
	public void setValue(float theValue) throws StandardException
	{
		value = NumberDataType.normalizeDOUBLE(theValue);
		isnull = false;
	}

	public void setValue(long theValue)
	{
		value = theValue; // no check needed
		isnull = false;
	}

	public void setValue(int theValue)
	{
		value = theValue; // no check needed
		isnull = false;
	}

	public void setValue(short theValue)
	{
		value = theValue; // no check needed
		isnull = false;
	}

	public void setValue(byte theValue)
	{
		value = theValue; // no check needed
		isnull = false;
	}

	public  void setValue(BigDecimal theValue) throws StandardException
	{
		if (objectNull(theValue)) 
			return;

		setValue(theValue.doubleValue());
	}

	/**
	 * @see NumberDataValue#setValue
	 *
	 */
	public void setValue(boolean theValue)
	{
		value = theValue?1:0;
		isnull = false;
	}


	/*
	 * DataValueDescriptor interface
	 */

	/** @see DataValueDescriptor#typePrecedence */
	public int typePrecedence()
	{
		return TypeId.DOUBLE_PRECEDENCE;
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
	 *
	 * @return	A SQL boolean value telling whether the two parameters are equal
	 *
	 * @exception StandardException		Thrown on error
	 */

	public BooleanDataValue equals(DataValueDescriptor left,
							DataValueDescriptor right)
			throws StandardException
	{
		return SQLBoolean.truthValue(left,
									 right,
									 left.getDouble() == right.getDouble());
	}

	/**
	 * The <> operator as called from the language module, as opposed to
	 * the storage module.
	 *
	 * @param left			The value on the left side of the <>
	 * @param right			The value on the right side of the <>
	 *						is not.
	 *
	 * @return	A SQL boolean value telling whether the two parameters
	 *			are not equal
	 *
	 * @exception StandardException		Thrown on error
	 */

	public BooleanDataValue notEquals(DataValueDescriptor left,
							DataValueDescriptor right)
			throws StandardException
	{
		return SQLBoolean.truthValue(left,
									 right,
									 left.getDouble() != right.getDouble());
	}

	/**
	 * The < operator as called from the language module, as opposed to
	 * the storage module.
	 *
	 * @param left			The value on the left side of the <
	 * @param right			The value on the right side of the <
	 *
	 * @return	A SQL boolean value telling whether the first operand is less
	 *			than the second operand
	 *
	 * @exception StandardException		Thrown on error
	 */

	public BooleanDataValue lessThan(DataValueDescriptor left,
							DataValueDescriptor right)
			throws StandardException
	{
		return SQLBoolean.truthValue(left,
									 right,
									 left.getDouble() < right.getDouble());
	}

	/**
	 * The > operator as called from the language module, as opposed to
	 * the storage module.
	 *
	 * @param left			The value on the left side of the >
	 * @param right			The value on the right side of the >
	 *
	 * @return	A SQL boolean value telling whether the first operand is greater
	 *			than the second operand
	 *
	 * @exception StandardException		Thrown on error
	 */

	public BooleanDataValue greaterThan(DataValueDescriptor left,
							DataValueDescriptor right)
			throws StandardException
	{
		return SQLBoolean.truthValue(left,
									 right,
									 left.getDouble() > right.getDouble());
	}

	/**
	 * The <= operator as called from the language module, as opposed to
	 * the storage module.
	 *
	 * @param left			The value on the left side of the <=
	 * @param right			The value on the right side of the <=
	 *
	 * @return	A SQL boolean value telling whether the first operand is less
	 *			than or equal to the second operand
	 *
	 * @exception StandardException		Thrown on error
	 */

	public BooleanDataValue lessOrEquals(DataValueDescriptor left,
							DataValueDescriptor right)
			throws StandardException
	{
		return SQLBoolean.truthValue(left,
									 right,
									 left.getDouble() <= right.getDouble());
	}

	/**
	 * The >= operator as called from the language module, as opposed to
	 * the storage module.
	 *
	 * @param left			The value on the left side of the >=
	 * @param right			The value on the right side of the >=
	 *
	 * @return	A SQL boolean value telling whether the first operand is greater
	 *			than or equal to the second operand
	 *
	 * @exception StandardException		Thrown on error
	 */

	public BooleanDataValue greaterOrEquals(DataValueDescriptor left,
							DataValueDescriptor right)
			throws StandardException
	{
		return SQLBoolean.truthValue(left,
									 right,
									 left.getDouble() >= right.getDouble());
	}

	/**
	 * This method implements the + operator for "double + double".
	 *
	 * @param addend1	One of the addends
	 * @param addend2	The other addend
	 * @param result	The result of a previous call to this method, null
	 *					if not called yet
	 *
	 * @return	A SQLDouble containing the result of the addition
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
			result = new SQLDouble();
		}

		if (addend1.isNull() || addend2.isNull())
		{
			result.setToNull();
			return result;
		}

		double tmpresult = addend1.getDouble() + addend2.getDouble();
        // No need to check underflow (result rounded to 0.0),
        // since the difference between two non-equal valid DB2 DOUBLE values is always non-zero in java.lang.Double precision.
		result.setValue(tmpresult);

		return result;
	}

	/**
	 * This method implements the - operator for "double - double".
	 *
	 * @param left	The value to be subtracted from
	 * @param right	The value to be subtracted
	 * @param result	The result of a previous call to this method, null
	 *					if not called yet
	 *
	 * @return	A SQLDouble containing the result of the subtraction
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
			result = new SQLDouble();
		}

		if (left.isNull() || right.isNull())
		{
			result.setToNull();
			return result;
		}

		double tmpresult = left.getDouble() - right.getDouble();
        // No need to check underflow (result rounded to 0.0),
        // since no difference between two valid DB2 DOUBLE values can be rounded off to 0.0 in java.lang.Double
		result.setValue(tmpresult);
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
	 * @return	A SQLDouble containing the result of the multiplication
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
			result = new SQLDouble();
		}

		if (left.isNull() || right.isNull())
		{
			result.setToNull();
			return result;
		}

        double leftValue = left.getDouble();
        double rightValue = right.getDouble();
		double tempResult = leftValue * rightValue;
        // check underflow (result rounded to 0.0)
        if ( (tempResult == 0.0) && ( (leftValue != 0.0) && (rightValue != 0.0) ) ) {
			throw StandardException.newException(SQLState.LANG_OUTSIDE_RANGE_FOR_DATATYPE, TypeId.DOUBLE_NAME);
        }

		result.setValue(tempResult);
		return result;
	}

	/**
	 * This method implements the / operator for "double / double".
	 *
	 * @param dividend	The numerator
	 * @param divisor	The denominator
	 * @param result	The result of a previous call to this method, null
	 *					if not called yet
	 *
	 * @return	A SQLDouble containing the result of the division
	 *
	 * @exception StandardException		Thrown on error
	 */

	public NumberDataValue divide(NumberDataValue dividend,
							 NumberDataValue divisor,
							 NumberDataValue result)
				throws StandardException
	{
		if (result == null)
		{
			result = new SQLDouble();
		}

		if (dividend.isNull() || divisor.isNull())
		{
			result.setToNull();
			return result;
		}

		/*
		** For double division, we can't catch divide by zero with Double.NaN;
		** So we check the divisor before the division.
		*/

		double divisorValue = divisor.getDouble();

		if (divisorValue == 0.0e0D)
		{
			throw StandardException.newException(SQLState.LANG_DIVIDE_BY_ZERO);
		}

        double dividendValue = dividend.getDouble();
		double divideResult =  dividendValue / divisorValue;

		if (Double.isNaN(divideResult))
		{
			throw StandardException.newException(SQLState.LANG_DIVIDE_BY_ZERO);
		}

        // check underflow (result rounded to 0.0d)
        if ((divideResult == 0.0d) && (dividendValue != 0.0d)) {
			throw StandardException.newException(SQLState.LANG_OUTSIDE_RANGE_FOR_DATATYPE, TypeId.DOUBLE_NAME);
        }

		result.setValue(divideResult);
		return result;
	}

	/**
	 * This method implements the unary minus operator for double.
	 *
	 * @param result	The result of a previous call to this method, null
	 *					if not called yet
	 *
	 * @return	A SQLDouble containing the result of the division
	 *
	 * @exception StandardException		Thrown on error
	 */

	public NumberDataValue minus(NumberDataValue result)
									throws StandardException
	{
		double		minusResult;

		if (result == null)
		{
			result = new SQLDouble();
		}

		if (this.isNull())
		{
			result.setToNull();
			return result;
		}

		/*
		** Doubles are assumed to be symmetric -- that is, their
		** smallest negative value is representable as a positive
		** value, and vice-versa.
		*/
		minusResult = -(this.getDouble());

		result.setValue(minusResult);
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
        return !isNull() && (value < 0.0d);
    }

	/*
	 * String display of value
	 */

	public String toString()
	{
		if (isNull())
			return "NULL";
		else
			return Double.toString(value);
	}

	/*
	 * Hash code
	 */
	public int hashCode()
	{
		long longVal = (long) value;
		double doubleLongVal = (double) longVal;

		/*
		** NOTE: This is coded to work around a bug in Visual Cafe 3.0.
		** If longVal is compared directly to value on that platform
		** with the JIT enabled, the values will not always compare
		** as equal even when they should be equal. This happens with
		** the value Long.MAX_VALUE, for example.
		**
		** Assigning the long value back to a double and then doing
		** the comparison works around the bug.
		**
		** This fixes Cloudscape bug number 1757.
		**
		**		-	Jeff Lichtman
		*/
		if (doubleLongVal != value)
        {
			longVal = Double.doubleToLongBits(value);
		}

		return (int) (longVal ^ (longVal >> 32));	
	}

	/*
	 * useful constants...
	 */
	static final int DOUBLE_LENGTH		= 32; // must match the number of bytes written by DataOutput.writeDouble()

    private static final int BASE_MEMORY_USAGE = ClassSize.estimateBaseFromCatalog( SQLDouble.class);

    public int estimateMemoryUsage()
    {
        return BASE_MEMORY_USAGE;
    }

	/*
	 * object state
	 */
	private double	value;
	private boolean	isnull;
}
