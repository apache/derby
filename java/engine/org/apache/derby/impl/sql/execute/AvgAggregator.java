/*

   Derby - Class org.apache.derby.impl.sql.execute.AvgAggregator

   Copyright 1998, 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derby.impl.sql.execute;

import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.types.NumberDataValue;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecAggregator;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.TypeId;

import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.iapi.reference.SQLState;

import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.IOException;
import java.math.BigDecimal;

/**
	Aggregator for AVG(). Extends the SumAggregator and
	implements a count. Result is then sum()/count().
	To handle overflow we catch the exception for
	value out of range, then we swap the holder for
	the current sum to one that can handle a larger
	range. Eventually a sum may end up in a SQLDecimal
	which can handle an infinite range. Once this
	type promotion has happened, it will not revert back
	to the original type, even if the sum would fit in
	a lesser type.

 */
public final class AvgAggregator extends SumAggregator
{
	private long count;
	private int scale;

	protected void accumulate(DataValueDescriptor addend) 
		throws StandardException
	{

		if (count == 0) {

			String typeName = addend.getTypeName();
			if (   typeName.equals(TypeId.TINYINT_NAME)
				|| typeName.equals(TypeId.SMALLINT_NAME)
				|| typeName.equals(TypeId.INTEGER_NAME)
				|| typeName.equals(TypeId.LONGINT_NAME)) {
				scale = 0;
			} else if (   typeName.equals(TypeId.REAL_NAME)
				|| typeName.equals(TypeId.DOUBLE_NAME)) {
				scale = TypeId.DECIMAL_SCALE;
			} else {
				// DECIMAL
				scale = addend.getBigDecimal().scale();
				if (scale < NumberDataValue.MIN_DECIMAL_DIVIDE_SCALE)
					scale = NumberDataValue.MIN_DECIMAL_DIVIDE_SCALE;
			}
		}

		try {

			super.accumulate(addend);
			count++;
			return;

		} catch (StandardException se) {

			if (!se.getMessageId().equals(SQLState.LANG_OUTSIDE_RANGE_FOR_DATATYPE))
				throw se;
		}


		/*
			Sum is out of range so promote

			TINYINT,SMALLINT -->> INTEGER

			INTEGER -->> BIGINT

			REAL -->> DOUBLE PRECISION

			others -->> DECIMAL
		*/

		// this code creates data type objects directly, it is anticipating
		// the time they move into the defined api of the type system. (djd).
		String typeName = value.getTypeName();

		if (typeName.equals(TypeId.INTEGER_NAME)) {
			value = new org.apache.derby.iapi.types.SQLLongint(value.getLong());
		} else if (typeName.equals(TypeId.TINYINT_NAME) || 
				   typeName.equals(TypeId.SMALLINT_NAME)) {
			value = new org.apache.derby.iapi.types.SQLInteger(value.getInt());
		} else if (typeName.equals(TypeId.REAL_NAME)) {
			value = new org.apache.derby.iapi.types.SQLDouble(value.getDouble());
		} else {
			value = new org.apache.derby.iapi.types.SQLDecimal(value.getBigDecimal());
		}
		accumulate(addend);
	}

	public void merge(ExecAggregator addend)
		throws StandardException
	{
		AvgAggregator otherAvg = (AvgAggregator) addend;

		// if I haven't been used take the other.
		if (count == 0) {
			count = otherAvg.count;
			value = otherAvg.value;
			scale = otherAvg.scale;
			return;
		}

		// Don't bother merging if the other is a NULL value aggregate.
		/* Note:Beetle:5346 fix change the sort to be High, that makes
		 * the neccessary for the NULL check because after the change 
		 * addend could have a  NULL value even on distincts unlike when 
		 * NULLs were sort order  Low, because by  sorting NULLs Low  
		 * they  happens to be always first row which makes it as 
		 * aggreagte result object instead of addends.
		 * Query that will fail without the following check:
		 * select avg(a) , count(distinct a) from t1;
		*/
		if(otherAvg.value != null)
		{
			// subtract one here as the accumulate will add one back in
			count += (otherAvg.count - 1);
			accumulate(otherAvg.value);
		}
	}

	/**
	 * Return the result of the aggregation.  If the count
	 * is zero, then we haven't averaged anything yet, so
	 * we return null.  Otherwise, return the running
	 * average as a double.
	 *
	 * @return null or the average as Double
	 */
	public Object getResult()
	{
		if (count == 0)
		{
			return null;
		}

		// note we cannot use the Datatype's divide method as it only supports
		// dividing by the same type, where we need the divisor to be a long
		// regardless of the sum type.

		BigDecimal avg = null;
		try {
			 avg = value.getBigDecimal().divide(BigDecimal.valueOf(count), scale, BigDecimal.ROUND_DOWN);
		} catch (StandardException se) {
			// get BigDecimal for a numeric type cannot throw an exception.
		}
		return avg;
	}

	/**
	 */
	public ExecAggregator newAggregator()
	{
		return new AvgAggregator();
	}

	/////////////////////////////////////////////////////////////
	// 
	// EXTERNALIZABLE INTERFACE
	// 
	/////////////////////////////////////////////////////////////
	/** 
	 *
	 * @exception IOException on error
	 */
	public void writeExternal(ObjectOutput out) throws IOException
	{
		super.writeExternal(out);
		out.writeLong(count);
		out.writeInt(scale);
	}

	/** 
	 * @see java.io.Externalizable#readExternal 
	 *
	 * @exception IOException on error
	 */
	public void readExternal(ObjectInput in) 
		throws IOException, ClassNotFoundException
	{
		super.readExternal(in);
		count = in.readLong();
		scale = in.readInt();
	}

	/////////////////////////////////////////////////////////////
	// 
	// FORMATABLE INTERFACE
	// 
	/////////////////////////////////////////////////////////////
	/**
	 * Get the formatID which corresponds to this class.
	 *
	 *	@return	the formatID of this class
	 */
	public	int	getTypeFormatId()	{ return StoredFormatIds.AGG_AVG_V01_ID; }
}
