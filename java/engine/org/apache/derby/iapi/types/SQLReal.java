/*

   Derby - Class org.apache.derby.iapi.types.SQLReal

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

import org.apache.derby.iapi.types.BooleanDataValue;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.NumberDataValue;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.StringDataValue;
import org.apache.derby.iapi.types.TypeId;

import org.apache.derby.iapi.services.io.FormatIdUtil;
import org.apache.derby.iapi.services.io.Storable;
import org.apache.derby.iapi.services.io.StoredFormatIds;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.types.NumberDataType;
import org.apache.derby.iapi.types.SQLBoolean;

import org.apache.derby.iapi.services.cache.ClassSize;

import java.math.BigDecimal;

import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.IOException;

import java.sql.ResultSet;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * SQLReal satisfies the DataValueDescriptor
 * interfaces (i.e., OrderableDataType). It implements a real column, 
 * e.g. for storing a column value; it can be specified
 * when constructed to not allow nulls. Nullability cannot be changed
 * after construction, as it affects the storage size and mechanism.
 * <p>
 * Because OrderableDataType is a subtype of ValueColumn,
 * SQLReal can play a role in either a ValueColumn/Row
 * or a OrderableDataType/Row, interchangeably.
 * <p>
 * We assume the store has a flag for nullness of the value,
 * and simply return a 0-length array for the stored form
 * when the value is null.
 * <p>
 * PERFORMANCE: There are likely alot of performance improvements
 * possible for this implementation -- it new's Float
 * more than it probably wants to.
 * <p>
 * This is called SQLReal even though it maps to the Java float type,
 * to avoid confusion with whether it maps to the SQL float type or not.
 * It doesn't, it maps to the SQL real type.
 * <p>
 * This is modeled after SQLSmallint.
 * @see SQLSmallint
 *
 * @author ames
 */
public final class SQLReal
	extends NumberDataType
{

	/*
	 * DataValueDescriptor interface
	 * (mostly implemented in DataType)
	 */


    // JDBC is lax in what it permits and what it
	// returns, so we are similarly lax

	/** 
	 * @see DataValueDescriptor#getInt 
	 * @exception StandardException thrown on failure to convert
	 */
	public int	getInt() throws StandardException
	{
		if ((value > (((double) Integer.MAX_VALUE + 1.0d))) || (value < (((double) Integer.MIN_VALUE) - 1.0d)))
			throw StandardException.newException(SQLState.LANG_OUTSIDE_RANGE_FOR_DATATYPE, "INTEGER");
		return (int) value;
	}

	/** 
	 * @see DataValueDescriptor#getByte 
	 * @exception StandardException thrown on failure to convert
	 */
	public byte	getByte() throws StandardException
	{
		if ((value > (((double) Byte.MAX_VALUE + 1.0d))) || (value < (((double) Byte.MIN_VALUE) - 1.0d)))
			throw StandardException.newException(SQLState.LANG_OUTSIDE_RANGE_FOR_DATATYPE, "TINYINT");
		return (byte) value;
	}

	/** 
	 * @exception StandardException thrown on failure to convert
	 * @see DataValueDescriptor#getShort 
	 */
	public short	getShort() throws StandardException
	{
		if ((value > (((double) Short.MAX_VALUE + 1.0d))) || (value < (((double) Short.MIN_VALUE) - 1.0d)))
			throw StandardException.newException(SQLState.LANG_OUTSIDE_RANGE_FOR_DATATYPE, "SMALLINT");
		return (short) value;
	}

	/** 
	 * @see DataValueDescriptor#getLong 
	 * @exception StandardException thrown on failure to convert
	 */
	public long	getLong() throws StandardException
	{
		if ((value > (((double) Long.MAX_VALUE + 1.0d))) || (value < (((double) Long.MIN_VALUE) - 1.0d)))
			throw StandardException.newException(SQLState.LANG_OUTSIDE_RANGE_FOR_DATATYPE, "BIGINT");
		return (long) value;
	}

	/** 
	 * @see DataValueDescriptor#getFloat 
	 */
	public float	getFloat()
	{
		return value;
	}

	/** 
	 * @see DataValueDescriptor#getDouble 
	 */
	public double	getDouble()
	{
		return (double) value;
	}

	/** 
	 * @see DataValueDescriptor#getBigDecimal 
	 */
	public BigDecimal	getBigDecimal()
	{
		if (isNull()) return null;
		return new BigDecimal(value);
	}

    // for lack of a specification: 0 or null is false,
    // all else is true
	/** 
	 * @see DataValueDescriptor#getBoolean 
	 */
	public boolean	getBoolean()
	{
		return (value != 0);
	}

	/** 
	 * @see DataValueDescriptor#getString 
	 */
	public String	getString()
	{
		if (isNull())
			return null;
		else
			return Float.toString(value);
	}

	/** 
	 * @see DataValueDescriptor#getLength 
	 */
	public int	getLength()
	{
		return REAL_LENGTH;
	}

	/** 
	 * @see DataValueDescriptor#getObject 
	 */
	public Object	getObject()
	{
		if (isNull())
			return null;
		else
			return new Float(value);
	}

	// this is for DataType's error generator
	public String getTypeName()
	{
		return TypeId.REAL_NAME;
	}

	/*
	 * Storable interface, implies Externalizable, TypedFormat
	 */


	/**
		Return my format identifier.

		@see org.apache.derby.iapi.services.io.TypedFormat#getTypeFormatId
	*/
	public int getTypeFormatId() {
		return StoredFormatIds.SQL_REAL_ID;
	}

	/*
	 * see if the real value is null.
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

		out.writeFloat(value);
	}

	/** @see java.io.Externalizable#readExternal */
	public void readExternal(ObjectInput in) throws IOException {
        // setValue(in.readFloat()); // can throw StandardException which we can't pass on
        // assume we wrote the value, so we can read it without problem, for now.
        value = in.readFloat();
        isnull = false;
	}
	public void readExternalFromArray(ArrayInputStream in) throws IOException {
        // setValue(in.readFloat()); // can throw StandardException which we can't pass on
        // assume we wrote the value, so we can read it without problem, for now.
        value = in.readFloat();
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

        // jsk: should use double? depends on DB2
		float thisValue = this.getFloat();
		float otherValue = NumberDataType.normalizeREAL(arg.getFloat()); // could gotten from "any type", may not be a float

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
		SQLReal ret = new SQLReal();
		ret.value = this.value;
		ret.isnull = this.isnull;
		return ret;
	}

	/**
	 * @see DataValueDescriptor#getNewNull
	 */
	public DataValueDescriptor getNewNull()
	{
		return new SQLReal();
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
			float fv = resultSet.getFloat(colNumber);
            if (isNullable && resultSet.wasNull())
                restoreToNull();
            else
                setValue(fv);
	}
	/**
		Set the value into a PreparedStatement.

		@exception SQLException Error setting value in PreparedStatement
	*/
	public final void setInto(PreparedStatement ps, int position) throws SQLException {

		if (isNull()) {
			ps.setNull(position, java.sql.Types.REAL);
			return;
		}

		ps.setFloat(position, value);
	}
	/**
		Set this value into a ResultSet for a subsequent ResultSet.insertRow
		or ResultSet.updateRow. This method will only be called for non-null values.

		@exception SQLException thrown by the ResultSet object
		@exception StandardException thrown by me accessing my value.
	*/
	public final void setInto(ResultSet rs, int position) throws SQLException, StandardException {
		rs.updateFloat(position, value);
	}

	/*
	 * class interface
	 */

	/*
	 * constructors
	 */

	/** no-arg constructor, required by Formattable. */
    // This constructor also gets used when we are
    // allocating space for a float.
	public SQLReal() 
	{
		isnull = true;
	}

	public SQLReal(float val)
		throws StandardException
	{
		value = NumberDataType.normalizeREAL(val);
	}
	public SQLReal(Float obj) throws StandardException {
		if (isnull = (obj == null))
			;
		else {
			value = NumberDataType.normalizeREAL(obj.floatValue());
		}
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
            // what if String is rouned to zero?
            //System.out.println("SQLReal.setValue(String) - rounding issue?"+theValue);
		    try {
		        setValue(Double.valueOf(theValue.trim()).doubleValue());
			} catch (NumberFormatException nfe) {
			    throw invalidFormat();
			}
		}
	}

	public  void setValue(BigDecimal theValue) throws StandardException
	{
		if (objectNull(theValue)) 
			return;

		float value = theValue.floatValue();
		setValue(value);
	}

	public void setValue(float theValue)
		throws StandardException
	{
//        new Throwable().printStackTrace();
//        System.out.println("setValue(float "+theValue+")");
		value = NumberDataType.normalizeREAL(theValue);
//        System.out.println("value = "+value);
		isnull = false;
	}

	public void setValue(short theValue)
	{
		value = theValue;
		isnull = false;

	}

	public void setValue(int theValue)
	{
		value = theValue;
		isnull = false;

	}

	public void setValue(byte theValue)
	{
		value = theValue;
		isnull = false;

	}

	public void setValue(long theValue)
	{
		value = theValue;
		isnull = false;

	}

	/**
		@exception StandardException if outsideRangeForReal
	 */
	public void setValue(double theValue) throws StandardException
	{
        // jsk: where does this theValue come from? if some caller is rounding parsing from string
        // we might have rounding error (different than DB2 behaviour)
		float fv = (float) theValue;
        // detect rounding taking place at cast time
        if (fv == 0.0f && theValue != 0.0d) {
			throw StandardException.newException(SQLState.LANG_OUTSIDE_RANGE_FOR_DATATYPE, TypeId.REAL_NAME);
        }
        setValue(fv);
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
            // rounding issue to solve!!!/jsk, INF, 0.0f
			this.setValue(((Number)theValue).floatValue());
		}
		else
		{
            // will most likely call .setValue(String)
			genericSetObject(theValue);
		}
	}

	protected void setFrom(DataValueDescriptor theValue) throws StandardException {

            // rounding issue to solve!!!/jsk
            //DOUBLE.getFloat() would make rounding problem if it got called here!!!
            // need to check where it is called from!
            if (theValue instanceof StringDataValue) {
                //System.out.println("\tcalling setValue(string)");
                setValue(theValue.getString());
            } else if (theValue instanceof SQLDouble) {
                //System.out.println("\tcalling setValue(double)");
                setValue(theValue.getDouble());
            } else {
                setValue(theValue.getFloat());
            }
	}


	/*
	 * DataValueDescriptor interface
	 */

	/** @see DataValueDescriptor#typePrecedence */
	public int typePrecedence()
	{
		return TypeId.REAL_PRECEDENCE;
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
									 left.getFloat() == right.getFloat());
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
									 left.getFloat() != right.getFloat());
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
									 left.getFloat() < right.getFloat());
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
									 left.getFloat() > right.getFloat());
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
									 left.getFloat() <= right.getFloat());
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
									 left.getFloat() >= right.getFloat());
	}


	/**
	 * This method implements the + operator for "real + real".
     * The operator uses DOUBLE aritmetic as DB2 does.
	 *
	 * @param addend1	One of the addends
	 * @param addend2	The other addend
	 * @param result	The result of a previous call to this method, null
	 *					if not called yet
	 *
	 * @return	A SQLReal containing the result of the addition
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
			result = new SQLReal();
		}

		if (addend1.isNull() || addend2.isNull())
		{
			result.setToNull();
			return result;
		}

        double dsum = addend1.getDouble() + addend2.getDouble();
        // No need to check underflow (result rounded to 0.0),
        // since the difference between two non-equal valid DB2 DOUBLE values is always non-zero in java.lang.Double precision.
        result.setValue(dsum);

		return result;
	}

	/**
	 * This method implements the - operator for "real - real".
     * The operator uses DOUBLE aritmetic as DB2 does.
	 *
	 * @param left	The value to be subtracted from
	 * @param right	The value to be subtracted
	 * @param result	The result of a previous call to this method, null
	 *					if not called yet
	 *
	 * @return	A SQLReal containing the result of the subtraction
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
			result = new SQLReal();
		}

		if (left.isNull() || right.isNull())
		{
			result.setToNull();
			return result;
		}

		double ddifference = left.getDouble() - right.getDouble();
        // No need to check underflow (result rounded to 0.0),
        // since no difference between two valid DB2 DOUBLE values can be rounded off to 0.0 in java.lang.Double
		result.setValue(ddifference);
		return result;
	}

	/**
	 * This method implements the * operator for "real * real".
     * The operator uses DOUBLE aritmetic as DB2 does.
	 *
	 * @param left	The first value to be multiplied
	 * @param right	The second value to be multiplied
	 * @param result	The result of a previous call to this method, null
	 *					if not called yet
	 *
	 * @return	A SQLReal containing the result of the multiplication
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
			result = new SQLReal();
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
			throw StandardException.newException(SQLState.LANG_OUTSIDE_RANGE_FOR_DATATYPE, TypeId.REAL_NAME);
        }

		result.setValue(tempResult);
		return result;
	}

	/**
	 * This method implements the / operator for "real / real".
     * The operator uses DOUBLE aritmetic as DB2 does.
	 *
	 * @param dividend	The numerator
	 * @param divisor	The denominator
	 * @param result	The result of a previous call to this method, null
	 *					if not called yet
	 *
	 * @return	A SQLReal containing the result of the division
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
			result = new SQLReal();
		}

		if (dividend.isNull() || divisor.isNull())
		{
			result.setToNull();
			return result;
		}

		double divisorValue = divisor.getDouble();
		if (divisorValue == 0.0e0f)
		{
			throw StandardException.newException(SQLState.LANG_DIVIDE_BY_ZERO);
		}

        double dividendValue = dividend.getDouble();
		double resultValue = dividendValue / divisorValue;
		if (Double.isNaN(resultValue))
		{
			throw StandardException.newException(SQLState.LANG_DIVIDE_BY_ZERO);
		}

        // check underflow (result rounded to 0.0)
        if ((resultValue == 0.0e0d) && (dividendValue != 0.0e0d)) {
			throw StandardException.newException(SQLState.LANG_OUTSIDE_RANGE_FOR_DATATYPE, TypeId.REAL_NAME);
        }

		result.setValue(resultValue);
		return result;
	}

	/**
	 * This method implements the unary minus operator for real.
	 *
	 * @param result	The result of a previous call to this method, null
	 *					if not called yet
	 *
	 * @return	A SQLSmalllint containing the result of the division
	 *
	 * @exception StandardException		Thrown on error
	 */

	public NumberDataValue minus(NumberDataValue result)
									throws StandardException
	{
		float		minusResult;

		if (result == null)
		{
			result = new SQLReal();
		}

		if (this.isNull())
		{
			result.setToNull();
			return result;
		}

		minusResult = -(this.getFloat());
		result.setValue(minusResult);
		return result;
	}

    /**
     * This method implements the isNegative method.
     * Note: This method will return true for -0.0f.
     *
     * @return  A boolean.  If this.value is negative, return true.
     *          For positive values or null, return false.
     */

    protected boolean isNegative()
    {
        return !isNull() && (value < 0.0f);
    }

	/*
	 * String display of value
	 */

	public String toString()
	{
		if (isNull())
			return "NULL";
		else
			return Float.toString(value);
	}

	/*
	 * Hash code
	 */
	public int hashCode()
	{
		long longVal = (long) value;

		if (longVal != value)
		{
			longVal = Double.doubleToLongBits(value);
		}

		return (int) (longVal ^ (longVal >> 32));	
	}

	static final int REAL_LENGTH = 16;

    private static final int BASE_MEMORY_USAGE = ClassSize.estimateBaseFromCatalog( SQLReal.class);

    public int estimateMemoryUsage()
    {
        return BASE_MEMORY_USAGE;
    }

	/*
	 * object state
	 */
	private float value;
	private boolean isnull;

}
