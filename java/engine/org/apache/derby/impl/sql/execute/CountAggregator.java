/*

   Derby - Class org.apache.derby.impl.sql.execute.CountAggregator

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

import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.sql.execute.ExecAggregator;
import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.iapi.services.io.Formatable;
import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.IOException;

/**
 * Aggregator for COUNT()/COUNT(*).  
 * @author jamie
 */
public final class CountAggregator 
	extends SystemAggregator
{
	private long value;
	private boolean isCountStar;

	/**
	 */
	public void setup(String aggregateName)
	{
		isCountStar = aggregateName.equals("COUNT(*)");
	}

	/**
	 * @see ExecAggregator#merge
	 *
	 * @exception	StandardException	on error
	 */
	public void merge(ExecAggregator addend)
		throws StandardException
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(addend instanceof CountAggregator,
				"addend is supposed to be the same type of aggregator for the merge operator");
		}

		value += ((CountAggregator)addend).value;
	}

	/**
	 * Return the result of the aggregation.  Just
	 * spit out the running count.
	 *
	 * @return the value as a Long 
	 */
	public Object getResult()
	{
		return new Long(value);
	}


	/**
	 * Accumulate for count().  Toss out all nulls in this kind of count.
	 * Increment the count for count(*). Count even the null values.
	 *
	 * @param addend	value to be added in
	 * @param ga		the generic aggregator that is calling me
	 *
	 * @see ExecAggregator#accumulate
	 */
	public void accumulate(DataValueDescriptor addend, Object ga)
		throws StandardException
	{
		if (isCountStar)
			value++;
		else
			super.accumulate(addend, ga);
	}

	protected final void accumulate(DataValueDescriptor addend) {
			value++;
	}

	/**
	 * @return ExecAggregator the new aggregator
	 */
	public ExecAggregator newAggregator()
	{
		CountAggregator ca = new CountAggregator();
		ca.isCountStar = isCountStar;
		return ca;
	}

	public boolean isCountStar()
	{
		return isCountStar;
	}

	/////////////////////////////////////////////////////////////
	// 
	// EXTERNALIZABLE INTERFACE
	// 
	/////////////////////////////////////////////////////////////
	/** 
	 * Although we are not expected to be persistent per se,
	 * we may be written out by the sorter temporarily.  So
	 * we need to be able to write ourselves out and read
	 * ourselves back in.  
	 * 
	 * @exception IOException thrown on error
	 */
	public final void writeExternal(ObjectOutput out) throws IOException
	{
		super.writeExternal(out);
		out.writeBoolean(isCountStar);
		out.writeLong(value);
	}

	/** 
	* @see java.io.Externalizable#readExternal 
	*
	* @exception IOException io exception
	* @exception ClassNotFoundException on error
	*/
	public final void readExternal(ObjectInput in) 
		throws IOException, ClassNotFoundException
	{
		super.readExternal(in);
		isCountStar = in.readBoolean();
		value = in.readLong();
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
	public	int	getTypeFormatId() { return StoredFormatIds.AGG_COUNT_V01_ID; }
}
