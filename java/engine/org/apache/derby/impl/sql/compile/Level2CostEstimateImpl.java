/*

   Derby - Class org.apache.derby.impl.sql.compile.Level2CostEstimateImpl

   Copyright 2000, 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derby.impl.sql.compile;

import org.apache.derby.iapi.sql.compile.CostEstimate;

import org.apache.derby.iapi.store.access.StoreCostResult;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.impl.sql.compile.CostEstimateImpl;

public class Level2CostEstimateImpl extends CostEstimateImpl 
{
	public Level2CostEstimateImpl() 
	{
	}

	public Level2CostEstimateImpl(double theCost,
							double theRowCount,
							double theSingleScanRowCount) 
	{
		super(theCost, theRowCount, theSingleScanRowCount);
	}

	/** @see CostEstimate#cloneMe */
	public CostEstimate cloneMe() 
	{
		return new Level2CostEstimateImpl(cost,
									rowCount,
									singleScanRowCount);
	}

	public String toString() 
	{
		return "Level2CostEstimateImpl: at " + hashCode() + ", cost == " + cost +
				", rowCount == " + rowCount + 
				", singleScanRowCount == " + singleScanRowCount;
	}

	public CostEstimateImpl setState(double theCost,
										double theRowCount,
										CostEstimateImpl retval) 
	{
		if (retval == null) 
		{
			retval = new Level2CostEstimateImpl();
		}

		return super.setState(theCost, theRowCount, retval);
	}
}
