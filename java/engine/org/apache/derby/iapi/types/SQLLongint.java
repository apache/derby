/*

   Derby - Class org.apache.derby.iapi.types.SQLLongint

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

import org.apache.derby.iapi.services.io.ArrayInputStream;

import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.TypeId;
import org.apache.derby.iapi.types.TypeId;
import org.apache.derby.iapi.types.NumberDataValue;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.BooleanDataValue;
import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.iapi.services.io.FormatIdUtil;
import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.iapi.services.io.Storable;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.sanity.SanityManager;

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
 * SQLLongint satisfies the DataValueDescriptor
 * interfaces (i.e., OrderableDataType). It implements a bigint column, 
 * e.g. for * storing a column value; it can be specified
 * when constructed to not allow nulls. Nullability cannot be changed
 * after construction, as it affects the storage size and mechanism.
 * <p>
 * Because OrderableDataType is a subtype of DataType,
 * SQLLongint can play a role in either a DataType/Row
 * or a OrderableDataType/Row, interchangeably.
 * <p>
 * We assume the store has a flag for nullness of the value,
 * and simply return a 0-length array for the stored form
 * when the value is null.
 * <p>
 * PERFORMANCE: There are likely alot of performance improvements
 * possible for this implementation -- it new's Long
 * more than it probably wants to.
 */
public final class SQLLongint
	extends NumberDataType
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
		/* This value is bogus if the SQLLongint is null */

		if (value > Integer.MAX_VALUE || value < Integer.MIN_VALUE)
			throw StandardException.newException(SQLState.LANG_OUTSIDE_RANGE_FOR_DATATYPE, "INTEGER");
		return (int) value;
	}

	/**
	 * @exception StandardException thrown on failure to convert
	 */
	public byte	getByte() throws StandardException
	{
		if (value > Byte.MAX_VALUE || value < Byte.MIN_VALUE)
			throw StandardException.newException(SQLState.LANG_OUTSIDE_RANGE_FOR_DATATYPE, "TINYINT");
		return (byte) value;
	}

	/**
	 * @exception StandardException thrown on failure to convert
	 */
	public short	getShort() throws StandardException
	{
		if (value > Short.MAX_VALUE || value < Short.MIN_VALUE)
			throw StandardException.newException(SQLState.LANG_OUTSIDE_RANGE_FOR_DATATYPE, "SMALLINT");
		return (short) value;
	}

	public long	getLong()
	{
		return value;
	}

	public float	getFloat()
	{
		return (float) value;
	}

	public double	getDouble()
	{
		return (double) value;
	}

	public BigDecimal	getBigDecimal()
	{
		if (isNull()) return null;
		return BigDecimal.valueOf(value);
	}

    // for lack of a specification: 0 or null is false,
    // all else is true
	public boolean	getBoolean()
	{
		return (value != 0);
	}

	public String	getString()
	{
		if (isNull())
			return null;
		else
			return Long.toString(value);
	}

	public Object	getObject()
	{
		if (isNull())
			return null;
		else
			return new Long(value);
	}

	public int	getLength()
	{
		return TypeId.LONGINT_MAXWIDTH;
	}

	// this is for DataType's error generator
	public String getTypeName()
	{
		return TypeId.LONGINT_NAME;
	}

	/*
	 * Storable interface, implies Externalizable, TypedFormat
	 */


	/**
		Return my format identifier.

		@see org.apache.derby.iapi.services.io.TypedFormat#getTypeFormatId
	*/
	public int getTypeFormatId() {
		return StoredFormatIds.SQL_LONGINT_ID;
	}

	/*
	 * see if the integer value is null.
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

		out.writeLong(value);
	}

	/** @see java.io.Externalizable#readExternal */
	public void readExternal(ObjectInput in) throws IOException {

		value = in.readLong();
		isnull = false;
	}
	public void readExternalFromArray(ArrayInputStream in) throws IOException {

		value = in.readLong();
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

		long thisValue = this.getLong();

		long otherValue = arg.getLong();

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
		return new SQLLongint(value, isnull);
	}

	/**
	 * @see DataValueDescriptor#getNewNull
	 */
	public DataValueDescriptor getNewNull()
	{
		return new SQLLongint();
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
			if ((value = resultSet.getLong(colNumber)) == 0L)
				isnull = (isNullable && resultSet.wasNull());
			else
				isnull = false;
	}
	/**
		Set the value into a PreparedStatement.

		@exception SQLException Error setting value in PreparedStatement
	*/
	public final void setInto(PreparedStatement ps, int position) throws SQLException {

		if (isNull()) {
			ps.setNull(position, java.sql.Types.BIGINT);
			return;
		}

		ps.setLong(position, value);
	}
	/**
		Set this value into a ResultSet for a subsequent ResultSet.insertRow
		or ResultSet.updateRow. This method will only be called for non-null values.

		@exception SQLException thrown by the ResultSet object
	*/
	public final void setInto(ResultSet rs, int position) throws SQLException {
		rs.updateLong(position, value);
	}

	/*
	 * class interface
	 */

	/*
	 * constructors
	 */

	/** no-arg constructor, required by Formattable */
    // This constructor also gets used when we are
    // allocating space for a long.
	public SQLLongint() 
	{
		isnull = true;
	}

	public SQLLongint(long val)
	{
		value = val;
	}

	/* This constructor gets used for the getClone() method */
	private SQLLongint(long val, boolean isnull)
	{
		value = val;
		this.isnull = isnull;
	}
	public SQLLongint(Long obj) {
		if (isnull = (obj == null))
			;
		else
			value = obj.longValue();
	}

	/**
		@exception StandardException thrown if string not accepted
	 */
	public void setValue(String theValue)
		throws StandardException
	{
		if (theValue == null)
		{
			value = 0;
			isnull = true;
		}
		else
		{
		    try {
		        value = Long.valueOf(theValue.trim()).longValue();
			} catch (NumberFormatException nfe) {
			    throw invalidFormat();
			}
			isnull = false;
		}
	}

	public void setValue(long theValue)
	{
		value = theValue;
		isnull = false;
	}

	public void setValue(int theValue)
	{
		value = theValue;
		isnull = false;
	}

	public void setValue(short theValue)
	{
		value = theValue;
		isnull = false;
	}

	public void setValue(byte theValue)
	{
		value = theValue;
		isnull = false;
	}

	/**
	 * @see NumberDataValue#setValue
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void setValue(float theValue) throws StandardException
	{
		theValue = NumberDataType.normalizeREAL(theValue);

		if (theValue > Long.MAX_VALUE
			|| theValue < Long.MIN_VALUE)
			throw StandardException.newException(SQLState.LANG_OUTSIDE_RANGE_FOR_DATATYPE, "BIGINT");

		float floorValue = (float)Math.floor(theValue);

		value = (long)floorValue;
		isnull = false;
	}

	/**
	 * @see NumberDataValue#setValue
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void setValue(double theValue) throws StandardException
	{
		theValue = NumberDataType.normalizeDOUBLE(theValue);

		if (theValue > Long.MAX_VALUE
			|| theValue < Long.MIN_VALUE)
			throw StandardException.newException(SQLState.LANG_OUTSIDE_RANGE_FOR_DATATYPE, "BIGINT");

		double floorValue = Math.floor(theValue);

		value = (long)floorValue;
		isnull = false;

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

	/**
	 * @see DataValueDescriptor#setValue
	 *
	 * @exception StandardException		Thrown on error
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
			this.setValue(((Number)theValue).longValue());
		}
		else
		{
			genericSetObject(theValue);
		}
	}
	protected void setFrom(DataValueDescriptor theValue) throws StandardException {

		setValue(theValue.getLong());
	}

	/*
	 * DataValueDescriptor interface
	 */

	/** @see DataValueDescriptor#typePrecedence */
	public int typePrecedence()
	{
		return TypeId.LONGINT_PRECEDENCE;
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
									 left.getLong() == right.getLong());
	}

	/**
	 * The <> operator as called from the language module, as opposed to
	 * the storage module.
	 *
	 * @param left			The value on the left side of the <>
	 * @param right			The value on the right side of the <>
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
									 left.getLong() != right.getLong());
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
									 left.getLong() < right.getLong());
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
									 left.getLong() > right.getLong());
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
									 left.getLong() <= right.getLong());
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
									 left.getLong() >= right.getLong());
	}

	/**
	 * This method implements the + operator for "bigint + bigint".
	 *
	 * @param addend1	One of the addends
	 * @param addend2	The other addend
	 * @param result	The result of a previous call to this method, null
	 *					if not called yet
	 *
	 * @return	A SQLLongint containing the result of the addition
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
			result = new SQLLongint();
		}

		if (addend1.isNull() || addend2.isNull())
		{
			result.setToNull();
			return result;
		}
		long	addend1Long = addend1.getLong();
		long	addend2Long = addend2.getLong();

		long resultValue = addend1Long + addend2Long;

		/*
		** Java does not check for overflow with integral types. We have to
		** check the result ourselves.
		**
		** Overflow is possible only if the two addends have the same sign.
		** Do they?  (This method of checking is approved by "The Java
		** Programming Language" by Arnold and Gosling.)
		*/
		if ((addend1Long < 0) == (addend2Long < 0))
		{
			/*
			** Addends have the same sign.  The result should have the same
			** sign as the addends.  If not, an overflow has occurred.
			*/
			if ((addend1Long < 0) != (resultValue < 0))
			{
				throw StandardException.newException(SQLState.LANG_OUTSIDE_RANGE_FOR_DATATYPE, "BIGINT");
			}
		}
		result.setValue(resultValue);

		return result;
	}

	/**
	 * This method implements the - operator for "bigint - bigint".
	 *
	 * @param left	The value to be subtracted from
	 * @param right	The value to be subtracted
	 * @param result	The result of a previous call to this method, null
	 *					if not called yet
	 *
	 * @return	A SQLLongint containing the result of the subtraction
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
			result = new SQLLongint();
		}

		if (left.isNull() || right.isNull())
		{
			result.setToNull();
			return result;
		}

		long diff = left.getLong() - right.getLong();

		/*
		** Java does not check for overflow with integral types. We have to
		** check the result ourselves.
		**
		** Overflow is possible only if the left and the right side have opposite signs.
		** Do they?  (This method of checking is approved by "The Java
		** Programming Language" by Arnold and Gosling.)
		*/
		if ((left.getLong() < 0) != (right.getLong() < 0))
		{
			/*
			** Left and right have opposite signs.  The result should have the same
			** sign as the left (this).  If not, an overflow has occurred.
			*/
			if ((left.getLong() < 0) != (diff < 0))
			{
				throw StandardException.newException(SQLState.LANG_OUTSIDE_RANGE_FOR_DATATYPE, "BIGINT");
			}
		}

		result.setValue(diff);
		return result;
	}

	/**
	 * This method implements the * operator for "bigint * bigint".
	 *
	 * @param left	The first value to be multiplied
	 * @param right	The second value to be multiplied
	 * @param result	The result of a previous call to this method, null
	 *					if not called yet
	 *
	 * @return	A SQLLongint containing the result of the multiplication
	 *
	 * @exception StandardException		Thrown on error
	 */

	public NumberDataValue times(NumberDataValue left,
							NumberDataValue right,
							NumberDataValue result)
				throws StandardException
	{
		long		tempResult;

		if (result == null)
		{
			result = new SQLLongint();
		}

		if (left.isNull() || right.isNull())
		{
			result.setToNull();
			return result;
		}

		/*
		** Java does not check for overflow with integral types. We have to
		** check the result ourselves.
		**
		** We can't use sign checking tricks like we do for '+' and '-' since
		** the product of 2 integers can wrap around multiple times.  So, we
		** apply the principle that a * b = c => a = c / b.  If b != 0 and
		** a != c / b, then overflow occurred.
		*/
		tempResult = left.getLong() * right.getLong();
		if ((right.getLong() != 0) && (left.getLong() != tempResult / right.getLong()))
		{
			throw StandardException.newException(SQLState.LANG_OUTSIDE_RANGE_FOR_DATATYPE, "BIGINT");
		}

		result.setValue(tempResult);
		return result;
	}

	/**
	 * This method implements the / operator for "bigint / bigint".
	 *
	 * @param dividend	The numerator
	 * @param divisor	The denominator
	 * @param result	The result of a previous call to this method, null
	 *					if not called yet
	 *
	 * @return	A SQLLongint containing the result of the division
	 *
	 * @exception StandardException		Thrown on error
	 */

	public NumberDataValue divide(NumberDataValue dividend,
							 NumberDataValue divisor,
							 NumberDataValue result)
				throws StandardException
	{
		long	longDivisor;

		if (result == null)
		{
			result = new SQLLongint();
		}

		if (dividend.isNull() || divisor.isNull())
		{
			result.setToNull();
			return result;
		}

		/* Catch divide by 0 */
		longDivisor = divisor.getLong();
		if (longDivisor == 0)
		{
			throw StandardException.newException(SQLState.LANG_DIVIDE_BY_ZERO);
		}

		result.setValue(dividend.getLong() / longDivisor);
		return result;
	}
	/**
		mod(bigint, bigint)
	*/
	public NumberDataValue mod(NumberDataValue dividend,
							 NumberDataValue divisor,
							 NumberDataValue result)
				throws StandardException
	{
		if (result == null)
		{
			result = new SQLLongint();
		}

		if (dividend.isNull() || divisor.isNull())
		{
			result.setToNull();
			return result;
		}

		/* Catch divide by 0 */
		long longDivisor = divisor.getLong();
		if (longDivisor == 0)
		{
			throw StandardException.newException(SQLState.LANG_DIVIDE_BY_ZERO);
		}

		result.setValue(dividend.getLong() % longDivisor);
		return result;
	}
	/**
	 * This method implements the unary minus operator for bigint.
	 *
	 * @param result	The result of a previous call to this method, null
	 *					if not called yet
	 *
	 * @return	A SQLLongint containing the result of the negation
	 *
	 * @exception StandardException		Thrown on error
	 */

	public NumberDataValue minus(NumberDataValue result)
									throws StandardException
	{
		long		operandValue;

		if (result == null)
		{
			result = new SQLLongint();
		}

		if (this.isNull())
		{
			result.setToNull();
			return result;
		}

		operandValue = this.getLong();

		/*
		** In two's complement arithmetic, the minimum value for a number
		** can't be negated, since there is no representation for its
		** positive value.
		*/
		if (operandValue == Long.MIN_VALUE)
		{
			throw StandardException.newException(SQLState.LANG_OUTSIDE_RANGE_FOR_DATATYPE, "BIGINT");
		}

		result.setValue(-operandValue);
		return result;
	}

    /**
     * This method implements the isNegative method.
     *
     * @return  A boolean.  if this.value is negative, return true.
     *
     * @exception StandException       Thrown on error
     */
    
    protected boolean isNegative()
    {
        return !isNull() && value < 0L;
    }

	/*
	 * String display of value
	 */

	public String toString()
	{
		if (isNull())
			return "NULL";
		else
			return Long.toString(value);
	}


	/*
	 * Hash code
	 */
	public int hashCode()
	{
		return (int) (value ^ (value >> 32));
	}

    private static final int BASE_MEMORY_USAGE = ClassSize.estimateBaseFromCatalog( SQLLongint.class);

    public int estimateMemoryUsage()
    {
        return BASE_MEMORY_USAGE;
    }

	/*
	 * object state
	 */
	private long		value;
	private boolean	isnull;
}
