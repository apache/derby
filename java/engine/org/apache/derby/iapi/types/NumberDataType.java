/*

   Derby - Class org.apache.derby.iapi.types.NumberDataType

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

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.types.NumberDataValue;
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.services.io.Storable;
import org.apache.derby.iapi.types.Orderable;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.TypeId;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.reference.DB2Limit;

import org.apache.derby.iapi.types.*;
import java.math.BigDecimal;

/**
 * NumberDataType is the superclass for all exact and approximate 
 * numeric data types. It exists for the purpose of allowing classification
 * of types for supported implicit conversions among them.
 *
 * @see DataType
 * @author ames
 */
public abstract class NumberDataType extends DataType 
									 implements NumberDataValue
{


    /**
     * Numbers check for isNegative first and negate it if negative.
     * 
     * @return this object's absolute value.  Null if object is null.
     * @exception StandardException thrown on error.
     */
    public NumberDataValue absolute(NumberDataValue result) 
                        throws StandardException
    {
        if(isNegative())
            return minus(result);

        if(result == null)
            result = (NumberDataType)getNewNull();

        result.setValue(this.getObject());
        return result;
    }

    /**
     * This is the sqrt method.
     * 
     * @return this object's sqrt value.  Null if object is null.
     * Note: -0.0f and  -0.0d returns 0.0f and 0.0d.
     *
     * @exception StandardException thrown on a negative number.
     */

    public NumberDataValue sqrt(NumberDataValue result)
                        throws StandardException
    {
        if(result == null)
        {
            result = (NumberDataValue)getNewNull();
        }

        if(this.isNull())
        {
            result.setToNull();
            return result;
        }

        double doubleValue = getDouble();

        if( this.isNegative() )
        {
            if( (new Double(doubleValue)).equals(new Double(-0.0d)) )
            {
                doubleValue = 0.0d;
            }
            else
            {
                throw StandardException.newException( SQLState.LANG_SQRT_OF_NEG_NUMBER, this);
            }
        }

        result.setValue( Math.sqrt(doubleValue) );
        return result;
    }

	/**
	 * This is dummy parent divide method.  Put it here for all the children
	 * that don't need this.  @see NumberDataValue#divide
	 */
	public NumberDataValue divide(NumberDataValue dividend,
								  NumberDataValue divisor,
								  NumberDataValue result,
								  int scale)
				throws StandardException
	{
		if (SanityManager.DEBUG)
			SanityManager.NOTREACHED();
		return null;
	}

	public NumberDataValue mod(NumberDataValue dividend,
								NumberDataValue divisor,
								NumberDataValue result)
								throws StandardException {
		if (SanityManager.DEBUG)
			SanityManager.NOTREACHED();
		return null;
	}


	/** @exception StandardException		Thrown on error */
	public final int compare(DataValueDescriptor arg) throws StandardException
	{
		/* Use compare method from dominant type, negating result
		 * to reflect flipping of sides.
		 */
		if (typePrecedence() < arg.typePrecedence())
		{
			return - (arg.compare(this));
		}


		boolean thisNull, otherNull;

		thisNull = this.isNull();
		otherNull = arg.isNull();

		/*
		 * thisNull otherNull	return
		 *	T		T		 	0	(this == other)
		 *	F		T		 	-1 	(this > other)
		 *	T		F		 	1	(this < other)
		 */
		if (thisNull || otherNull)
		{
			if (!thisNull)		// otherNull must be true
				return -1;
			if (!otherNull)		// thisNull must be true
				return 1;
			return 0;
		}

		return typeCompare(arg);

	}
	/**
		Compare this (not null) to a non-null value.
	
	@exception StandardException		Thrown on error
	*/
	protected abstract int typeCompare(DataValueDescriptor arg) throws StandardException;

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
			if (this.isNull() || other.isNull())
				return unknownRV;
		}

		/* Do the comparison */
		return super.compare(op, other, orderedNulls, unknownRV);
	}
    
	/**
     * The isNegative abstract method.  Checks to see if this.value is negative.
     * To be implemented by each NumberDataType.
     *
     * @return  A boolean.  If this.value is negative, return true.
     *          For positive values or null, return false.
     */
    protected abstract boolean isNegative();

	/**
	 * @see NumberDataValue#setValue
	 *
	 */
	public final void setValue(Byte theValue) throws StandardException
	{
		if (!objectNull(theValue))
		{
			setValue(theValue.byteValue());
		}
	}
	
	/**
	 * @see NumberDataValue#setValue
	 *
	 * @exception StandardException		Thrown on error
	 */
	public final void setValue(Short theValue) throws StandardException
	{
		if (!objectNull(theValue))
			setValue(theValue.shortValue());
	}
	
	/**
	 * @see NumberDataValue#setValue
	 *
	 * @exception StandardException		Thrown on error
	 */
	public final void setValue(Integer theValue) throws StandardException
	{
		if (!objectNull(theValue))
			setValue(theValue.intValue());
	}
	
	/**
	 * @see NumberDataValue#setValue
	 *
	 * @exception StandardException		Thrown on error
	 */
	public final void setValue(Long theValue) throws StandardException
	{
		if (!objectNull(theValue))
			setValue(theValue.longValue());
	}
	/**
	 * @see NumberDataValue#setValue
	 *
	 * @exception StandardException		Thrown on error
	 */
	public final void setValue(Double theValue) throws StandardException
	{
		if (!objectNull(theValue))
			setValue(theValue.doubleValue());
	}

	/**
		setValue for integral exact numerics. Converts the BigDecimal
		to a long to preserve precision
	*/
	public  void setValue(BigDecimal theValue) throws StandardException
	{
		if (objectNull(theValue))
			return;

		// See comment in SQLDecimal.getLong()

		if (   (theValue.compareTo(SQLDecimal.MINLONG_MINUS_ONE) == 1)
			&& (theValue.compareTo(SQLDecimal.MAXLONG_PLUS_ONE) == -1)) {

			setValue(theValue.longValue());
		} else {

			throw StandardException.newException(SQLState.LANG_OUTSIDE_RANGE_FOR_DATATYPE, getTypeName());
		}
	}

	/**
	 * @see NumberDataValue#setValue
	 *
	 * @exception StandardException		Thrown on error
	 */
	public final void setValue(Float theValue) throws StandardException
	{
		if (!objectNull(theValue))
			setValue(theValue.floatValue());
	}
	
	/**
	 * @see NumberDataValue#setValue
	 *
	 */
	public final void setValue(Boolean theValue) throws StandardException
	{
		if (!objectNull(theValue))
			setValue(theValue.booleanValue());
	}

    
	protected final boolean objectNull(Object o) 
	{
		if (o == null) 
		{
			restoreToNull();
			return true;
		}
		return false;
	}

	/**
       normalizeREAL checks the validity of the given java float that
       it fits within the range of DB2 REALs. In addition it
       normalizes the value, so that negative zero (-0.0) becomes positive.
	*/
    public static float normalizeREAL(float v) throws StandardException
	{
        if ( (Float.isNaN(v) || Float.isInfinite(v)) ||
             ((v < DB2Limit.DB2_SMALLEST_REAL) || (v > DB2Limit.DB2_LARGEST_REAL)) ||
             ((v > 0) && (v < DB2Limit.DB2_SMALLEST_POSITIVE_REAL)) ||
             ((v < 0) && (v > DB2Limit.DB2_LARGEST_NEGATIVE_REAL)) )
        {
			throw StandardException.newException(SQLState.LANG_OUTSIDE_RANGE_FOR_DATATYPE, TypeId.REAL_NAME);
        }
        // Normalize negative floats to be "positive" (can't detect easily without using Float object because -0.0f = 0.0f)
        if (v == 0.0f) v = 0.0f;

        return v;
	}

	/**
       normalizeREAL checks the validity of the given java double that
       it fits within the range of DB2 REALs. In addition it
       normalizes the value, so that negative zero (-0.0) becomes positive.

       The reason for having normalizeREAL with two signatures is to
       avoid that normalizeREAL is called with a casted (float)doublevalue,
       since this invokes an unwanted rounding (of underflow values to 0.0),
       in contradiction to DB2s casting semantics.
	*/
    public static float normalizeREAL(double v) throws StandardException
    {
        // can't just cast it to float and call normalizeFloat(float) since casting can round down to 0.0
        if ( (Double.isNaN(v) || Double.isInfinite(v)) ||
             ((v < DB2Limit.DB2_SMALLEST_REAL) || (v > DB2Limit.DB2_LARGEST_REAL)) ||
             ((v > 0) && (v < DB2Limit.DB2_SMALLEST_POSITIVE_REAL)) ||
             ((v < 0) && (v > DB2Limit.DB2_LARGEST_NEGATIVE_REAL)) )
        {
			throw StandardException.newException(SQLState.LANG_OUTSIDE_RANGE_FOR_DATATYPE, TypeId.REAL_NAME);
        }
        // Normalize negative floats to be "positive" (can't detect easily without using Float object because -0.0f = 0.0f)
        if (v == 0.0d) v = 0.0d;

        return (float)v;
    }

	/**
       normalizeDOUBLE checks the validity of the given java double that
       it fits within the range of DB2 DOUBLEs. In addition it
       normalizes the value, so that negative zero (-0.0) becomes positive.
	*/
    public static double normalizeDOUBLE(double v) throws StandardException
	{
        if ( (Double.isNaN(v) || Double.isInfinite(v)) ||
             ((v < DB2Limit.DB2_SMALLEST_DOUBLE) || (v > DB2Limit.DB2_LARGEST_DOUBLE)) ||
             ((v > 0) && (v < DB2Limit.DB2_SMALLEST_POSITIVE_DOUBLE)) ||
             ((v < 0) && (v > DB2Limit.DB2_LARGEST_NEGATIVE_DOUBLE)) )
        {
			throw StandardException.newException(SQLState.LANG_OUTSIDE_RANGE_FOR_DATATYPE, TypeId.DOUBLE_NAME);
        }
        // Normalize negative doubles to be "positive" (can't detect easily without using Double object because -0.0f = 0.0f)
        if (v == 0.0d) v = 0.0d;

        return v;
	}


}

