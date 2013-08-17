/*

   Derby - Class org.apache.derby.impl.sql.execute.SumAggregator

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

package org.apache.derby.impl.sql.execute;

import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derby.iapi.types.NumberDataValue;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecAggregator;
import org.apache.derby.iapi.types.DataValueDescriptor;

import org.apache.derby.iapi.services.io.StoredFormatIds;

/**
 * Aggregator for SUM().  Defers most of its work
 * to OrderableAggregator.
 *
 */
public  class SumAggregator 
	extends OrderableAggregator
{
	/**
	 * Accumulate
 	 *
	 * @param addend	value to be added in
	 *
	 * @exception StandardException on error
	 *
	 * @see ExecAggregator#accumulate
	 */
	protected void accumulate(DataValueDescriptor addend) 
		throws StandardException
	{

		/*
		** If we don't have any value yet, just clone
		** the addend.
		*/
		if (value == null)
		{ 
			/* NOTE: We need to call cloneValue since value gets
			 * reused underneath us
			 */
			value = addend.cloneValue(false);
		}
		else
		{
			NumberDataValue	input = (NumberDataValue)addend;
			NumberDataValue nv = (NumberDataValue) value;

			value = nv.plus(
						input,						// addend 1
						nv,		// addend 2
						nv);	// result
		}
	}

	/**
 	 * @return ExecAggregator the new aggregator
	 */
	public ExecAggregator newAggregator()
	{
		return new SumAggregator();
	}

	////////////////////////////////////////////////////////////
	// 
	// FORMATABLE INTERFACE
	// 
	/////////////////////////////////////////////////////////////
	/**
	 * Get the formatID which corresponds to this class.
	 *
	 *	@return	the formatID of this class
	 */
	public	int	getTypeFormatId()	{ return StoredFormatIds.AGG_SUM_V01_ID; }
        public String toString()
        {
            try {
            return "SumAggregator: " + value.getString();
            }
            catch (StandardException e)
            {
                return super.toString() + ":" + e.getMessage();
            }
        }
}
