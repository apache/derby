/*

   Derby - Class org.apache.derby.iapi.types.NumberDataValue

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

public interface NumberDataValue extends DataValueDescriptor
{
	/**
	 * The minimum scale when dividing Decimals
	 */
	public static final int MIN_DECIMAL_DIVIDE_SCALE = 4;
	public static final int MAX_DECIMAL_PRECISION_SCALE = 31;

	/**
	 * The SQL + operator.
	 *
	 * @param addend1	One of the addends
	 * @param addend2	The other addend
	 * @param result	The result of the previous call to this method, null
	 *					if not called yet.
	 *
	 * @return	The sum of the two addends
	 *
	 * @exception StandardException		Thrown on error, if result is non-null then its value will be unchanged.
	 */
	public NumberDataValue plus(NumberDataValue addend1,
								NumberDataValue addend2,
								NumberDataValue result)
							throws StandardException;

	/**
	 * The SQL - operator.
	 *
	 * @param left		The left operand
	 * @param right		The right operand
	 * @param result	The result of the previous call to this method, null
	 *					if not called yet.
	 *
	 * @return	left - right
	 *
	 * @exception StandardException		Thrown on error, if result is non-null then its value will be unchanged.
	 */
	public NumberDataValue minus(NumberDataValue left,
								 NumberDataValue right,
								NumberDataValue result)
							throws StandardException;

	/**
	 * The SQL * operator.
	 *
	 * @param left		The left operand
	 * @param right		The right operand
	 * @param result	The result of the previous call to this method, null
	 *					if not called yet.
	 *
	 * @return	left * right
	 *
	 * @exception StandardException		Thrown on error, if result is non-null then its value will be unchanged.
	 */
	public NumberDataValue times(NumberDataValue left,
								NumberDataValue right,
								NumberDataValue result)
							throws StandardException;

	/**
	 * The SQL / operator.
	 *
	 * @param dividend		The numerator
	 * @param divisor		The denominator
	 * @param result		The result of the previous call to this method, null
	 *						if not called yet.
	 *
	 * @return	dividend / divisor
	 *
	 * @exception StandardException		Thrown on error, if result is non-null then its value will be unchanged.
	 */
	public NumberDataValue divide(NumberDataValue dividend,
								NumberDataValue divisor,
								NumberDataValue result)
							throws StandardException;

	/**
	 * The SQL / operator.
	 *
	 * @param dividend		The numerator
	 * @param divisor		The denominator
	 * @param result		The result of the previous call to this method, null
	 *						if not called yet.
	 * @param scale			The scale of the result, for decimal type.  If pass
	 *						in value < 0, can calculate it dynamically.
	 *
	 * @return	dividend / divisor
	 *
	 * @exception StandardException		Thrown on error, if result is non-null then its value will be unchanged.
	 */
	public NumberDataValue divide(NumberDataValue dividend,
								NumberDataValue divisor,
								NumberDataValue result,
								int scale)
							throws StandardException;


	/**
	 * The SQL mod operator.
	 *
	 * @param dividend		The numerator
	 * @param divisor		The denominator
	 * @param result		The result of the previous call to this method, null
	 *						if not called yet.
	 *
	 * @return	dividend / divisor
	 *
	 * @exception StandardException		Thrown on error, if result is non-null then its value will be unchanged.
	 */
	public NumberDataValue mod(NumberDataValue dividend,
								NumberDataValue divisor,
								NumberDataValue result)
							throws StandardException;

	/**
	 * The SQL unary - operator.  Negates this NumberDataValue.
	 *
	 * @param result	The result of the previous call to this method, null
	 *					if not called yet.
	 *
	 * @return	- operand
	 *
	 * @exception StandardException		Thrown on error, if result is non-null then its value will be unchanged.
	 */
	public NumberDataValue minus(NumberDataValue result)
							throws StandardException;

    /**
     * The SQL ABSOLUTE operator.  Absolute value of this NumberDataValue.
     *
     * @param result    The result of the previous call to this method, null
     *                  if not called yet.
     *
     * @exception StandardException     Thrown on error, if result is non-null then its value will be unchanged.
     */
    public NumberDataValue absolute(NumberDataValue result)
                            throws StandardException;

    /**
     * The SQL SQRT operator.  Sqrt value of this NumberDataValue.
     *
     * @param result    The result of the previous call to this method, null
     *                  if not call yet.
     * 
     * @exception StandardException     Thrown on error (a negative number), if result is non-null then its value will be unchanged.
     */
    public NumberDataValue sqrt(NumberDataValue result)
                            throws StandardException;

	/**
	 * Set the value of this NumberDataValue to the given int value
	 *
	 * @param theValue	An Integer containing the value to set this
	 *					NumberDataValue to.  Null means set the value
	 *					to SQL null.
	 *
	 * @return	This NumberDataValue
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void setValue(Integer theValue) throws StandardException;

	/**
	 * Set the value of this NumberDataValue to the given double value
	 *
	 * @param theValue	A Double containing the value to set this
	 *					NumberDataValue to.  Null means set the value
	 *					to SQL null.
	 *
	 * @return	This NumberDataValue
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void setValue(Double theValue) throws StandardException;

	/**
	 * Set the value of this NumberDataValue to the given float value
	 *
	 * @param theValue	A Float containing the value to set this
	 *					NumberDataValue to.  Null means set the value
	 *					to SQL null.
	 *
	 * @return	This NumberDataValue
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void setValue(Float theValue) throws StandardException;

	/**
	 * Set the value of this NumberDataValue to the given short value
	 *
	 * @param theValue	A Short containing the value to set this
	 *					NumberDataValue to.  Null means set the value
	 *					to SQL null.
	 *
	 * @return	This NumberDataValue
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void setValue(Short theValue) throws StandardException;


	/**
	 * Set the value of this NumberDataValue to the given long value
	 *
	 * @param theValue	A Long containing the value to set this
	 *					NumberDataValue to.  Null means set the value
	 *					to SQL null.
	 *
	 * @return	This NumberDataValue
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void setValue(Long theValue) throws StandardException;

	/**
	 * Set the value of this NumberDataValue to the given byte value
	 *
	 * @param theValue	A Byte containing the value to set this
	 *					NumberDataValue to.  Null means set the value
	 *					to SQL null.
	 *
	 * @return	This NumberDataValue
	 *
	 */
	public void setValue(Byte theValue) throws StandardException;

	/**
	 * Set the value.
	 *
	 * @param theValue	Contains the boolean value to set this to
	 *
	 * @return	This value
	 *
	 */
	public void setValue(Boolean theValue) throws StandardException;
}









