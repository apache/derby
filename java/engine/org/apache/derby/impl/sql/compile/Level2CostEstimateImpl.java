/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.sql.compile
   (C) Copyright IBM Corp. 2000, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.impl.sql.compile;

import org.apache.derby.iapi.sql.compile.CostEstimate;

import org.apache.derby.iapi.store.access.StoreCostResult;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.impl.sql.compile.CostEstimateImpl;

public class Level2CostEstimateImpl extends CostEstimateImpl 
{
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_2000_2004;
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
