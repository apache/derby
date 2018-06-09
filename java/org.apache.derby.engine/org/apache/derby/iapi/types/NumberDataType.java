/*

   Derby - Class org.apache.derby.iapi.types.NumberDataType

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

import java.math.BigDecimal;
import java.security.AccessController;
import java.security.PrivilegedAction;

import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.iapi.services.context.Context;
import org.apache.derby.iapi.services.context.ContextService;
import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derby.shared.common.reference.SQLState;
import org.apache.derby.shared.common.reference.Limits;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;


/**
 * NumberDataType is the superclass for all exact and approximate 
 * numeric data types. It exists for the purpose of allowing classification
 * of types for supported implicit conversions among them.
 *
 * @see DataType
 */
public abstract class NumberDataType extends DataType 
									 implements NumberDataValue
{
    static final BigDecimal MAXLONG_PLUS_ONE =
            BigDecimal.valueOf(Long.MAX_VALUE).add(BigDecimal.ONE);
    static final BigDecimal MINLONG_MINUS_ONE =
            BigDecimal.valueOf(Long.MIN_VALUE).subtract(BigDecimal.ONE);

    /**
     * Numbers check for isNegative first and negate it if negative.
     * 
     * @return this object's absolute value.  Null if object is null.
     * @exception StandardException thrown on error.
     */
    public final NumberDataValue absolute(NumberDataValue result) 
                        throws StandardException
    {   		
        if(isNegative())
            return minus(result);

        if(result == null)
            result = (NumberDataValue)getNewNull();
        
        result.setValue(this);
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
            if( doubleValue == -0.0d )
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
	 * This method implements the + operator for TINYINT,SMALLINT,INT.
	 *
	 * @param addend1	One of the addends
	 * @param addend2	The other addend
	 * @param result	The result of a previous call to this method, null
	 *					if not called yet
	 *
	 * @return	A NumberDataValue containing the result of the addition
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
			result = (NumberDataValue) getNewNull();
		}

		if (addend1.isNull() || addend2.isNull())
		{
			result.setToNull();
			return result;
		}
		int addend1Int = addend1.getInt();
		int addend2Int = addend2.getInt();

		int resultValue = addend1Int + addend2Int;

		/*
		** Java does not check for overflow with integral types. We have to
		** check the result ourselves.
		**
		** Overflow is possible only if the two addends have the same sign.
		** Do they?  (This method of checking is approved by "The Java
		** Programming Language" by Arnold and Gosling.)
		*/
		if ((addend1Int < 0) == (addend2Int < 0))
		{
			/*
			** Addends have the same sign.  The result should have the same
			** sign as the addends.  If not, an overflow has occurred.
			*/
			if ((addend1Int < 0) != (resultValue < 0))
			{
				throw outOfRange();
			}
		}

		result.setValue(resultValue);

		return result;
	}
	/**
	 * This method implements the - operator for TINYINT, SMALLINT and INTEGER.
	 *
	 * @param left	The value to be subtracted from
	 * @param right	The value to be subtracted
	 * @param result	The result of a previous call to this method, null
	 *					if not called yet
	 *
	 * @return	A SQLInteger containing the result of the subtraction
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
			result = (NumberDataValue) getNewNull();
		}

		if (left.isNull() || right.isNull())
		{
			result.setToNull();
			return result;
		}

		int diff = left.getInt() - right.getInt();

		/*
		** Java does not check for overflow with integral types. We have to
		** check the result ourselves.
		**
		** Overflow is possible only if the left and the right side have opposite signs.
		** Do they?  (This method of checking is approved by "The Java
		** Programming Language" by Arnold and Gosling.)
		*/
		if ((left.getInt() < 0) != (right.getInt() < 0))
		{
			/*
			** Left and right have opposite signs.  The result should have the same
			** sign as the left (this).  If not, an overflow has occurred.
			*/
			if ((left.getInt() < 0) != (diff < 0))
			{
				throw outOfRange();
			}
		}

		result.setValue(diff);

		return result;
	}
	
	/**
	 * This method implements the / operator for TINYINT, SMALLINT and INTEGER.
	 * Specialized methods are not required for TINYINT and SMALLINT as the Java
	 * virtual machine always executes byte and int division as integer.
	 *
	 * @param dividend	The numerator
	 * @param divisor	The denominator
	 * @param result	The result of a previous call to this method, null
	 *					if not called yet
	 *
	 * @return	A SQLInteger containing the result of the division
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
			result = (NumberDataValue) getNewNull();
		}

		if (dividend.isNull() || divisor.isNull())
		{
			result.setToNull();
			return result;
		}

		/* Catch divide by 0 */
		int intDivisor = divisor.getInt();
		if (intDivisor == 0)
		{
			throw StandardException.newException(SQLState.LANG_DIVIDE_BY_ZERO);
		}

		result.setValue(dividend.getInt() / intDivisor);
		return result;
	}

	/**
	 	Suitable for integral types that ignore scale.
	 */
	public NumberDataValue divide(NumberDataValue dividend,
								  NumberDataValue divisor,
								  NumberDataValue result,
								  int scale)
				throws StandardException
	{
		return divide(dividend, divisor, result);
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
     * Common code to handle converting a short to this value
     * by using the int to this value conversion.
     * Simply calls setValue(int).
     * 
     */
	public void setValue(short theValue)
		throws StandardException
	{
		setValue((int) theValue);
	}

    /**
     * Common code to handle converting a byte to this value
     * by using the int to this value conversion.
     * Simply calls setValue(int).
     * 
     */
	public void setValue(byte theValue)
		throws StandardException
	{
		setValue((int) theValue);
	}		
	/**
	   Common code to handle java.lang.Integer as a Number,
	   used for TINYINT, SMALLINT, INTEGER
	 * @see NumberDataValue#setValue
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void setValue(Number theValue) throws StandardException
	{
		if (objectNull(theValue))
			return;
		
		if (SanityManager.ASSERT)
		{
			if (!(theValue instanceof java.lang.Integer))
				SanityManager.THROWASSERT("NumberDataType.setValue(Number) passed a " + theValue.getClass());
		}
		
		setValue(theValue.intValue());
	}
	
	/**
	 * Set the value from a correctly typed Integer object.
	 * Used for TINYINT, SMALLINT, INTEGER.
	 * @throws StandardException 
	 */
	void setObject(Object theValue) throws StandardException
	{
		setValue(((Integer) theValue).intValue());
	}

	/**
		setValue for integral exact numerics. Converts the BigDecimal
		to a long to preserve precision
	*/
	public void setBigDecimal(BigDecimal bigDecimal) throws StandardException
	{
		if (objectNull(bigDecimal))
			return;

		// See comment in SQLDecimal.getLong()

		if (   (bigDecimal.compareTo(NumberDataType.MINLONG_MINUS_ONE) == 1)
			&& (bigDecimal.compareTo(NumberDataType.MAXLONG_PLUS_ONE) == -1)) {

			setValue(bigDecimal.longValue());
		} else {

			throw StandardException.newException(SQLState.LANG_OUTSIDE_RANGE_FOR_DATATYPE, getTypeName());
		}
	}
	
	/**
	 * Implementation for integral types. Convert to a BigDecimal using long
	 */
	public int typeToBigDecimal()
	{
		return java.sql.Types.BIGINT;
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
      * normalizeREAL normalizes the value, so that negative zero (-0.0) becomes
      * positive.
      * @throws StandardException if the value is not a number (NaN) or is
      * infinite.
      */
    public static float normalizeREAL(float v) throws StandardException
	{
        boolean invalid = Float.isNaN(v) || Float.isInfinite(v);

        if (v < Limits.DB2_SMALLEST_REAL ||
            v > Limits.DB2_LARGEST_REAL ||
            (v > 0 && v < Limits.DB2_SMALLEST_POSITIVE_REAL) ||
            (v < 0 && v > Limits.DB2_LARGEST_NEGATIVE_REAL)) {

            if (useDB2Limits()) {
                invalid = true;
            }
        }

        if (invalid) {
            throw StandardException.newException(
                SQLState.LANG_OUTSIDE_RANGE_FOR_DATATYPE, TypeId.REAL_NAME);
        }

        // Normalize negative floats to be "positive" (can't detect easily without using Float object because -0.0f = 0.0f)
        // DERBY-2447: It shouldn't matter whether we compare to 0.0f or -0.0f,
        // both should match negative zero, but comparing to 0.0f triggered
        // this JVM bug: http://bugs.sun.com/view_bug.do?bug_id=6833879
        if (v == -0.0f) v = 0.0f;

        return v;
	}

	/**
     * normalizeREAL normalizes the value, so that negative zero (-0.0)
     * becomes positive.
     * <p>
     * The reason for having normalizeREAL with two signatures is to
     * avoid that normalizeREAL is accidentally called with a casted
     * {@code (float)<double value>} since this can introduce an undetected
     * underflow values to 0.0f.
     * @throws StandardException if the value is not a number (NaN) or is
     * infinite or on underflow
     * (the value has magnitude too small to be represented as a float).
     */
    public static float normalizeREAL(final double v) throws StandardException
    {
        // Can't just cast it to float and call normalizeFloat(float) since
        // casting can round down to 0.0
        float fv = (float)v;

        boolean invalid =
            Double.isNaN(v) ||
            Double.isInfinite(v) ||
            (fv == 0.0f && v != 0.0d); // too small to represent as REAL

        if (v < Limits.DB2_SMALLEST_REAL ||
            v > Limits.DB2_LARGEST_REAL ||
            (v > 0 && v < Limits.DB2_SMALLEST_POSITIVE_REAL) ||
            (v < 0 && v > Limits.DB2_LARGEST_NEGATIVE_REAL)) {

            if (useDB2Limits()) {
                invalid = true;
            }
        }

        if (invalid) {
            throw StandardException.newException(
                SQLState.LANG_OUTSIDE_RANGE_FOR_DATATYPE, TypeId.REAL_NAME);
        }

        // Normalize negative floats to be "positive" (can't detect easily without using Float object because -0.0f = 0.0f)
        // DERBY-2447: It shouldn't matter whether we compare to 0.0d or -0.0d,
        // both should match negative zero, but comparing to 0.0d triggered
        // this JVM bug: http://bugs.sun.com/view_bug.do?bug_id=6833879
        if (fv == -0.0f) {
            fv = 0.0f;
        }

        return fv;
    }

	/**
     * normalizeDOUBLE normalizes the value, so that negative zero (-0.0)
     * becomes positive.
     * @throws StandardException if v is not a number (NaN) or is infinite.
     */
    public static double normalizeDOUBLE(double v) throws StandardException
	{
        boolean invalid = Double.isNaN(v) || Double.isInfinite(v);

        if (v < Limits.DB2_SMALLEST_DOUBLE ||
            v > Limits.DB2_LARGEST_DOUBLE ||
            (v > 0 && v < Limits.DB2_SMALLEST_POSITIVE_DOUBLE) ||
            (v < 0 && v > Limits.DB2_LARGEST_NEGATIVE_DOUBLE)) {

            if (useDB2Limits()) {
                invalid = true;
            }
        }

        if (invalid) {
            throw StandardException.newException(
                SQLState.LANG_OUTSIDE_RANGE_FOR_DATATYPE, TypeId.DOUBLE_NAME);
        }
        // Normalize negative doubles to be "positive" (can't detect easily without using Double object because -0.0f = 0.0f)
        // DERBY-2447: It shouldn't matter whether we compare to 0.0d or -0.0d,
        // both should match negative zero, but comparing to 0.0d triggered
        // this JVM bug: http://bugs.sun.com/view_bug.do?bug_id=6833879
        if (v == -0.0d) v = 0.0d;

        return v;
	}


   /**
     * Controls use of old DB2 limits (DERBY-3398).
     * @return false if dictionary is new enough, see DD_Version.
     */
     private static boolean useDB2Limits() throws StandardException {
         LanguageConnectionContext lcc =
             (LanguageConnectionContext)getContextOrNull(
                 LanguageConnectionContext.CONTEXT_ID);
         if (lcc != null) {
             return !lcc.getDataDictionary().checkVersion(
                     DataDictionary.DD_VERSION_DERBY_10_10, null);
         } else {
             // In PreparedStatement#setXXX and ResultSet#updateXXX contexts we
             // do not have LanguageConnectionContext so check is deferred to
             // the PreparedStatement execute time or ResultSet#updateRow time
             // as the case may be.
             return false;
         }
    }
    
    /**
     * Privileged lookup of a Context. Must be private so that user code
     * can't call this entry point.
     */
    private  static  Context    getContextOrNull( final String contextID )
    {
        if ( System.getSecurityManager() == null )
        {
            return ContextService.getContextOrNull( contextID );
        }
        else
        {
            return AccessController.doPrivileged
                (
                 new PrivilegedAction<Context>()
                 {
                     public Context run()
                     {
                         return ContextService.getContextOrNull( contextID );
                     }
                 }
                 );
        }
    }

}

