/*

   Derby - Class org.apache.derby.iapi.types.BigIntegerDecimal

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

import java.math.BigInteger;
import java.sql.Types;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.services.sanity.SanityManager;

/**
 * DECIMAL support using the immutable java.math.BigInteger to perform arithmetic
 * and conversions. Extends BinaryDecimal to use the base
 * support of that class. J2ME/CDC/Foundation includes BigInteger.
 * 
 * A BigInteger is used in calculations etc. to represent the integral unscaled value.
 * It is simply created from new BigInteger(data2c). No additional instance fields
 * are used by this class, a possible enhancement would be to keep the BigInteger around
 * but would require calls from the parent class to reset state etc.
 */

public final class BigIntegerDecimal extends BinaryDecimal
{
	private static final BigInteger TEN = BigInteger.valueOf(10L);
	private static final BigInteger MAXLONG_PLUS_ONE = BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE);
	private static final BigInteger MINLONG_MINUS_ONE = BigInteger.valueOf(Long.MIN_VALUE).subtract(BigInteger.ONE);
	
	public BigIntegerDecimal() {
	}

	public DataValueDescriptor getNewNull()
	{
		return new BigIntegerDecimal();
	}	
	
	public long getLong() throws StandardException
	{
		if (isNull())
			return 0L;
		
		BigInteger bi = new BigInteger(data2c);
		
		// If at any time we see that the value to be scaled down
		// is within the range for a long, then we are guaranteed
		// that the scaled down value is within the range for long.
		boolean rangeOk = false;
		if ((bi.compareTo(BigIntegerDecimal.MAXLONG_PLUS_ONE) < 0)
			&& (bi.compareTo(BigIntegerDecimal.MINLONG_MINUS_ONE) > 0))
			rangeOk = true;
			
		for (int i = 0; i < sqlScale; i++)
		{
			bi = bi.divide(BigIntegerDecimal.TEN);
			if (rangeOk)
				continue;
			
			if ((bi.compareTo(BigIntegerDecimal.MAXLONG_PLUS_ONE) < 0)
					&& (bi.compareTo(BigIntegerDecimal.MINLONG_MINUS_ONE) > 0))
					rangeOk = true;
			}
		
		// TODO Range checking
		if (!rangeOk)
			throw StandardException.newException(SQLState.LANG_OUTSIDE_RANGE_FOR_DATATYPE, "BIGINT");
		
		return bi.longValue();
	}

	public float getFloat() throws StandardException
    {
		if (isNull())
			return 0.0f;
		return NumberDataType.normalizeREAL(Float.parseFloat(getString()));
	}
	public double getDouble() throws StandardException
    {
		if (isNull())
			return 0.0;
		return NumberDataType.normalizeDOUBLE(Double.parseDouble(getString()));
	}	
	

	/**
	 * Set the value from a String, the format is
	 * nnnn
	 * 
	 * Scale always set to zero.
	 */
	public void setValue(String theValue) throws StandardException
	{
		if (theValue == null)
		{
			restoreToNull();
			return;
		}
		
		theValue = theValue.trim();
		
		int dot = theValue.indexOf('.');
		int ePosition = theValue.indexOf('e');
		if (ePosition == -1)
			ePosition = theValue.indexOf('E');
		
		
		int scale = 0;
		try
		{
			// handle the exponent
			if (ePosition != -1)
			{
				if (dot > ePosition)
					throw invalidFormat();
				
				// Integer.parseInt does not handle a + sign in
				// front of the number, while the format for the
				// exponent allows it. Need to strip it off.
				
				int expOffset = ePosition + 1;

				if (expOffset >= theValue.length())
					throw invalidFormat();
				
				if (theValue.charAt(expOffset) == '+')
				{
					// strip the plus but must ensure the next character
					// is not a - sign. Any other invalid sign will be handled
					// by Integer.parseInt.
					expOffset++;
					if (expOffset >= theValue.length())
						throw invalidFormat();
					if (theValue.charAt(expOffset) == '-')
						throw invalidFormat();
				}
				
				String exponent = theValue.substring(expOffset);
				
				scale = -1 * Integer.parseInt(exponent);
				theValue = theValue.substring(0, ePosition);
			}
			
			if (dot != -1)
			{
				// remove the dot from the string
				String leading = theValue.substring(0, dot);
				
				scale += (theValue.length() - (dot + 1));
				
				theValue = leading.concat(theValue.substring(dot + 1, theValue.length()));
			}
			
			if (scale < 0)
			{
				for (int i = scale; i < 0; i++)
					theValue = theValue.concat("0");
				scale = 0;
			}
			
			BigInteger bi = new BigInteger(theValue);		
			data2c = bi.toByteArray();
			sqlScale = scale;
			
		} catch (NumberFormatException nfe) 
		{
		    throw invalidFormat();
		}		
	}
	
	
	/* (non-Javadoc)
	 * @see org.apache.derby.iapi.types.DataValueDescriptor#getString()
	 */
	public String getString() {
		
		if (isNull())
			return null;
		
		String unscaled = new BigInteger(data2c).toString();
				
		if (sqlScale == 0)
			return unscaled;

		boolean isNegative = this.isNegative();
		
		if (sqlScale >= (unscaled.length() - (isNegative ? 1 : 0)))
		{	
			if (isNegative)
				unscaled = unscaled.substring(1);

			String val = isNegative ? "-0." : "0.";
			for (int i = sqlScale - unscaled.length(); i > 0; i--)
				val = val.concat("0");
			return val.concat(unscaled);
		}
		
		String val = unscaled.substring(0, unscaled.length() - sqlScale);
		val = val.concat(".");
		return val.concat(unscaled.substring(unscaled.length() - sqlScale, unscaled.length()));
	}
	
	/**
	 * Return the SQL precision of this value.
	 */
	public int getDecimalValuePrecision()
	{
		if (isNull())
			return 0;
		
		BigInteger bi = new BigInteger(data2c);
		
		if (BigInteger.ZERO.equals(bi))
			return 0;
		
		int precision = bi.toString().length();
		
		if (this.isNegative())
			precision--;
		
		if (precision < sqlScale)
			return sqlScale;
		
		return precision;
	}
	

	/**
	 * Compare two non-null NumberDataValues using DECIMAL arithmetic.
	 */
	protected int typeCompare(DataValueDescriptor arg) throws StandardException {
		
		BigIntegerDecimal obid = getBID(arg);
		
		// need to align scales to perform comparisions
		int tscale = getDecimalValueScale();
		int oscale = obid.getDecimalValueScale();
	
		BigInteger tbi = new BigInteger(data2c);
		BigInteger obi = new BigInteger(obid.data2c);
		
		if (tscale < oscale)
			tbi = BigIntegerDecimal.rescale(tbi, oscale - tscale);
		else if (oscale < tscale)
			obi = BigIntegerDecimal.rescale(obi, tscale - oscale);
	
		return tbi.compareTo(obi);
	}

	/**
	 * Add two non-null NumberDataValues using DECIMAL arithmetic.
	 * Uses add() to perform the calculation.
	 */
	public NumberDataValue plusNN(NumberDataValue left, NumberDataValue right, NumberDataValue result)
	throws StandardException {
		
		BinaryDecimal resultBid = (BinaryDecimal) result;
		if (resultBid == null)
			resultBid = new BigIntegerDecimal();
				
		BigIntegerDecimal lbid = getBID(left);
		BigIntegerDecimal rbid = getBID(right);
		
		// need to align scales to perform plus
		int lscale = lbid.getDecimalValueScale();
		int rscale = rbid.getDecimalValueScale();
		
		BigInteger bi1 = new BigInteger(lbid.data2c);
		BigInteger bi2 = new BigInteger(rbid.data2c);
		
		int tscale = lscale;
		if (lscale < rscale)
		{
			bi1 = BigIntegerDecimal.rescale(bi1, rscale - lscale);
			tscale = rscale;
		}
		else if (rscale < lscale)
		{
			bi2 = BigIntegerDecimal.rescale(bi2, lscale - rscale);
		}
			
		

		bi1 = bi1.add(bi2);
		
		resultBid.data2c = bi1.toByteArray();
		resultBid.sqlScale = tscale;
		
		return resultBid;
	}

	/**
	 * Negate the number.
	 * @see org.apache.derby.iapi.types.NumberDataValue#minus(org.apache.derby.iapi.types.NumberDataValue)
	 */
	public NumberDataValue minus(NumberDataValue result) throws StandardException {
		
		if (result == null)
			result = (NumberDataValue) getNewNull();

		if (isNull())
			result.setToNull();
		else
		{
			BinaryDecimal rbd = (BinaryDecimal) result;
			
			BigInteger bi = new BigInteger(data2c);
			// scale remains unchanged.
			rbd.data2c = bi.negate().toByteArray();
			rbd.sqlScale = sqlScale;
		
		}
			
		return result;
	}


	/**
	 * Multiple two non-null NumberDataValues using DECIMAL arithmetic.
	 * Uses BigInteger.multipy() to perform the calculation.
	 * Simply multiply the unscaled values and add the scales, proof:
	 
	 <code>
	 left * right
	 = (left_unscaled * 10^-left_scale) * (right_unscaled * 10^-right_scale)
	 = (left_unscaled * 10^-left_scale) * (right_unscaled * 10^-right_scale)
	 = (left_unscaled * right_unscaled) * 10^-(left_scale + right_scale)
	 </code>
	 */
	public NumberDataValue timesNN(NumberDataValue left, NumberDataValue right, NumberDataValue result)
	throws StandardException
	{
		BigIntegerDecimal resultBid = (BigIntegerDecimal) result;
		if (resultBid == null)
			resultBid = new BigIntegerDecimal();
				
		BigIntegerDecimal lbid = getBID(left);
		BigIntegerDecimal rbid = getBID(right);

		BigInteger lbi = new BigInteger(lbid.data2c);
		BigInteger rbi = new BigInteger(rbid.data2c);
		
		rbi = lbi.multiply(rbi);	
		resultBid.data2c = rbi.toByteArray();
		resultBid.sqlScale = lbid.getDecimalValueScale() + rbid.getDecimalValueScale();
		
		return resultBid;
	}

	/**
	 * Divide two non-null NumberDataValues using DECIMAL arithmetic.
	 * Uses divide() to perform the calculation.
	 * Simply multiply the unscaled values and subtract the scales, proof:
	 
	 <code>
		left / right
		= (left_unscaled * 10^-left_scale) / (right_unscaled * 10^-right_scale)
		= (left_unscaled / right_unscaled) * (10^-left_scale / 10^-right_scale)
		= (left_unscaled / right_unscaled) * (10^-(left_scale-right_scale))
	</code>
	 */
	public NumberDataValue divideNN(NumberDataValue left, NumberDataValue right,
			NumberDataValue result, int scale)
	throws StandardException
	{
		BinaryDecimal resultBid = (BinaryDecimal) result;
		if (resultBid == null)
			resultBid = new BigIntegerDecimal();
				
		BigIntegerDecimal lbid = getBID(left);
		BigIntegerDecimal rbid = getBID(right);

		BigInteger lbi = new BigInteger(lbid.data2c);
		BigInteger rbi = new BigInteger(rbid.data2c);
		
		if (BigInteger.ZERO.equals(rbi))
			throw  StandardException.newException(SQLState.LANG_DIVIDE_BY_ZERO);
				
		int lscale = lbid.getDecimalValueScale();
		int rscale = rbid.getDecimalValueScale();
				
		if (scale >= 0)
		{
			if (lscale < (scale + rscale)) {
				lbi = BigIntegerDecimal.rescale(lbi, scale + rscale - lscale);
				lscale = scale + rscale;
			}
		}
		
		rbi = lbi.divide(rbi);
		resultBid.sqlScale = lscale - rscale;
		
		if (resultBid.sqlScale < 0)
		{
			rbi = BigIntegerDecimal.rescale(rbi, -resultBid.sqlScale);
			resultBid.sqlScale = 0;
		}
			
		resultBid.data2c = rbi.toByteArray();
		
		return resultBid;
	}	

	public void normalize(
			DataTypeDescriptor desiredType,
			DataValueDescriptor source)
					throws StandardException
	{
		int desiredScale = desiredType.getScale();
		int desiredPrecision = desiredType.getPrecision();

		setFrom(source);
		setWidth(desiredPrecision, desiredScale, true);
	}
	
	/* (non-Javadoc)
	 * @see org.apache.derby.iapi.types.VariableSizeDataValue#setWidth(int, int, boolean)
	 */
	public DataValueDescriptor setWidth(int desiredPrecision, int desiredScale,
			boolean errorOnTrunc) throws StandardException
	{
		if (isNull())
			return this;
			
		int deltaScale = desiredScale - sqlScale;
		if (desiredPrecision != IGNORE_PRECISION)
		{
			int currentPrecision = getDecimalValuePrecision();
	
			int futurePrecision = currentPrecision + deltaScale;
			
			if (futurePrecision > desiredPrecision)
				throw StandardException.newException(SQLState.LANG_OUTSIDE_RANGE_FOR_DATATYPE, 
						("DECIMAL/NUMERIC("+desiredPrecision+","+desiredScale+")"));
		}
		
		if (deltaScale == 0)
			return this;
		
		BigInteger bi = new BigInteger(data2c);
		
		bi = BigIntegerDecimal.rescale(bi, deltaScale);

		data2c = bi.toByteArray();
		sqlScale = desiredScale;		
     	return this;
	}
	
	/**
	 * Obtain a BinaryDecimal that represents the passed in value.
	 */
	private BigIntegerDecimal getBID(DataValueDescriptor value)
	throws StandardException
	 {
		switch (value.typeToBigDecimal()) {
		case Types.DECIMAL:
			return (BigIntegerDecimal) value;
		case Types.CHAR: {
			BigIntegerDecimal bid = new BigIntegerDecimal();
			bid.setValue(value.getString());
			return bid;
		}

		case Types.BIGINT: {
			BigIntegerDecimal bid = new BigIntegerDecimal();
			bid.setValue(value.getLong());
			return bid;
		}
		default:
			if (SanityManager.DEBUG)
				SanityManager.THROWASSERT("invalid return from "
						+ value.getClass() + ".typeToBigDecimal() "
						+ value.typeToBigDecimal());
			return null;
		}
	}	
	/**
	 * Rescale a BigInteger, a positive delta means the scale is increased, zero
	 * no change and negative decrease of scale. It is up to the caller to
	 * manage the actual scale of the value, e.g. don't allow the scale to go
	 * negative.
	 * 
	 * @param bi
	 *            value to be rescaled
	 * @param deltaScale
	 *            change of scale
	 * @return rescaled value
	 */
	private static BigInteger rescale(BigInteger bi, int deltaScale) {
		if (deltaScale == 0)
			return bi;
		if (deltaScale > 0) {
			// scale increasing, e.g. 10.23 to 10.2300		
			for (int i = 0; i < deltaScale; i++)
				bi = bi.multiply(BigIntegerDecimal.TEN);

		} else if (deltaScale < 0) {
			// scale decreasing, e.g. 10.2345 to 10.23
			for (int i = deltaScale; i < 0; i++)
				bi = bi.divide(BigIntegerDecimal.TEN);
		}
		return bi;
	}
	/*
	 * String display of value
	 */
	public String toString()
	{
		if (isNull())
			return "NULL";
		else
			return getString();
	}
}
