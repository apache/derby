/*

   Derby - Class org.apache.derby.impl.sql.execute.OrderableAggregator

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
import org.apache.derby.iapi.sql.execute.ExecAggregator;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.Formatable;


import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.IOException;

/**
 * Abstract aggregator for Orderable aggregates (max/min).
 *
 * @author jamie
 */
abstract class OrderableAggregator extends SystemAggregator
{
	protected DataValueDescriptor value;

	/**
	 */
	public void setup(String aggregateName)
	{
	}

	/**
	 * @see ExecAggregator#merge
	 *
	 * @exception StandardException on error
	 */
	public void merge(ExecAggregator addend)
		throws StandardException
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(addend instanceof OrderableAggregator,
				"addend is supposed to be the same type of aggregator for the merge operator");
		}

		// Don't bother merging if the other has never been used.
		DataValueDescriptor bv = ((OrderableAggregator)addend).value;
		if (bv != null)
			this.accumulate(bv);
	}

	/**
	 * Return the result of the operations that we
	 * have been performing.  Returns a DataValueDescriptor.
	 *
	 * @return the result as a DataValueDescriptor 
	 */
	public Object getResult()
	{
		return value;
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
	 * ourselves back in.  We rely on formatable to handle
	 * situations where <I>value</I> is null.
	 * <p>
	 * Why would we be called to write ourselves out if we
	 * are null?  For scalar aggregates, we don't bother
	 * setting up the aggregator since we only need a single
	 * row.  So for a scalar aggregate that needs to go to
	 * disk, the aggregator might be null.
	 * 
	 * @exception IOException on error
	 *
	 * @see java.io.Externalizable#writeExternal
	 */
	public void writeExternal(ObjectOutput out) throws IOException
	{
		super.writeExternal(out);
		out.writeObject(value);
	}

	/** 
	 * @see java.io.Externalizable#readExternal 
	 *
	 * @exception IOException on error
	 * @exception ClassNotFoundException on error
	 */
	public void readExternal(ObjectInput in) 
		throws IOException, ClassNotFoundException
	{
		super.readExternal(in);
		value = (DataValueDescriptor) in.readObject();
	}
}
