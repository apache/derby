/*

   Derby - Class org.apache.derby.impl.sql.compile.CostEstimateImpl

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

package org.apache.derby.impl.sql.compile;

import org.apache.derby.iapi.sql.compile.CostEstimate;

import org.apache.derby.iapi.store.access.StoreCostResult;

import org.apache.derby.iapi.services.sanity.SanityManager;

public class CostEstimateImpl implements CostEstimate {
	public double	cost;
	public double	rowCount;
	public double	singleScanRowCount;

	public CostEstimateImpl() {
	}

	public CostEstimateImpl(double theCost,
							double theRowCount,
							double theSingleScanRowCount) {
		if (SanityManager.DEBUG)
		{
			if (theCost < 0.0 || 
			    theRowCount < 0.0 || 
				theSingleScanRowCount < 0.0)
			{
				SanityManager.THROWASSERT(
					"All parameters expected to be < 0.0, " +
					"\n\ttheCost = " + theCost +
					"\n\ttheRowCount = " + theRowCount +
					"\n\ttheSingleScanRowCount = " + theSingleScanRowCount 
					);
			}
		}
		this.cost = theCost;
		this.rowCount = theRowCount;
		this.singleScanRowCount = theSingleScanRowCount;
	}

	/** @see CostEstimate#setCost */
	public void setCost(double cost, double rowCount,
						double singleScanRowCount) {
		if (SanityManager.DEBUG)
		{
			if (cost < 0.0 || 
			    rowCount < 0.0 || 
				singleScanRowCount < 0.0)
			{
				SanityManager.THROWASSERT(
					"All parameters expected to be < 0.0, " +
					"\n\tcost = " + cost +
					"\n\trowCount = " + rowCount +
					"\n\tsingleScanRowCount = " + singleScanRowCount 
					);
			}
		}
		this.cost = cost;
		this.rowCount = rowCount;
		this.singleScanRowCount = singleScanRowCount;
	}

	/** @see CostEstimate#setCost */
	public void setCost(CostEstimate other) {
		cost = other.getEstimatedCost();
		rowCount = other.rowCount();
		singleScanRowCount = other.singleScanRowCount();
	}

	/** @see CostEstimate#setSingleScanRowCount */
	public void setSingleScanRowCount(double singleScanRowCount)
	{
		if (SanityManager.DEBUG)
		{
			if (singleScanRowCount < 0.0)
			{
				SanityManager.THROWASSERT(
					"All parameters expected to be < 0.0, " +
					"\n\tsingleScanRowCount = " + singleScanRowCount 
					);
			}
		}
		this.singleScanRowCount = singleScanRowCount;
	}

	/** @see CostEstimate#compare */
	public double compare(CostEstimate other) {
		if (SanityManager.DEBUG) {
			if (other == null) {
				SanityManager.THROWASSERT("Comparing with null CostEstimate");
			}

			if ( ! (other instanceof CostEstimateImpl)) {
				SanityManager.THROWASSERT(other.getClass().getName());
			}
		}

		return this.cost - ((CostEstimateImpl) other).cost;
	}

	/** @see CostEstimate#add */
	public CostEstimate add(CostEstimate other, CostEstimate retval) {
		if (SanityManager.DEBUG) {
			SanityManager.ASSERT(other instanceof CostEstimateImpl);
			SanityManager.ASSERT(retval == null ||
								retval instanceof CostEstimateImpl);
		}

		CostEstimateImpl	addend = (CostEstimateImpl) other;

		double sumCost = this.cost + addend.cost;
		double sumRowCount = this.rowCount + addend.rowCount;
		if (SanityManager.DEBUG)
		{
			if (sumCost < 0.0 || 
			    sumRowCount < 0.0)
			{
				SanityManager.THROWASSERT(
					"All sums expected to be < 0.0, " +
					"\n\tthis.cost = " + this.cost +
					"\n\taddend.cost = " + addend.cost +
					"\n\tsumCost = " + sumCost +
					"\n\tthis.rowCount = " + this.rowCount +
					"\n\taddend.rowCount = " + addend.rowCount +
					"\n\tsumRowCount = " + sumRowCount
					);
			}
		}

		/* Presume that ordering is not maintained */
		return setState(sumCost,
						sumRowCount,
						(CostEstimateImpl) retval);
	}

	/** @see CostEstimate#multiply */
	public CostEstimate multiply(double multiplicand, CostEstimate retval) {
		if (SanityManager.DEBUG) {
			SanityManager.ASSERT(retval == null ||
								retval instanceof CostEstimateImpl);
		}

		double multCost = this.cost * multiplicand;
		double multRowCount = this.rowCount * multiplicand;

		if (SanityManager.DEBUG)
		{
			if (multCost < 0.0 || 
			    multRowCount < 0.0)
			{
				SanityManager.THROWASSERT(
					"All products expected to be < 0.0, " +
					"\n\tthis.cost = " + this.cost +
					"\n\tmultiplicand = " + multiplicand +
					"\n\tmultCost = " + multCost +
					"\n\tthis.rowCount = " + this.rowCount +
					"\n\tmultRowCount = " + multRowCount
					);
			}
		}

		/* Presume that ordering is not maintained */
		return setState(multCost,
						multRowCount,
						(CostEstimateImpl) retval);
	}

	/** @see CostEstimate#divide */
	public CostEstimate divide(double divisor, CostEstimate retval) {
		if (SanityManager.DEBUG) {
			SanityManager.ASSERT(retval == null ||
								retval instanceof CostEstimateImpl);
		}

		double divCost = this.cost / divisor;
		double divRowCount = this.rowCount / divisor;

		if (SanityManager.DEBUG)
		{
			if (divCost < 0.0 || 
			    divRowCount < 0.0)
			{
				SanityManager.THROWASSERT(
					"All products expected to be < 0.0, " +
					"\n\tthis.cost = " + this.cost +
					"\n\tdivisor = " + divisor +
					"\n\tdivCost = " + divCost +
					"\n\tthis.rowCount = " + this.rowCount +
					"\n\tdivRowCount = " + divRowCount
					);
			}
		}

		/* Presume that ordering is not maintained */
		return setState(divCost,
						divRowCount,
						(CostEstimateImpl) retval);
	}

	/** @see CostEstimate#rowCount */
	public double rowCount() {
		return rowCount;
	}

	/** @see CostEstimate#singleScanRowCount */
	public double singleScanRowCount() {
		return singleScanRowCount;
	}

	/** @see CostEstimate#cloneMe */
	public CostEstimate cloneMe() {
		return new CostEstimateImpl(cost,
									rowCount,
									singleScanRowCount);
	}

	/** @see CostEstimate#isUninitialized */
	public boolean isUninitialized()
	{
		return (cost == Double.MAX_VALUE &&
			    rowCount == Double.MAX_VALUE &&
				singleScanRowCount == Double.MAX_VALUE);
	}

	/** @see StoreCostResult#getEstimatedCost */
	public double getEstimatedCost() {
		return cost;
	}

	/** @see StoreCostResult#setEstimatedCost */
	public void setEstimatedCost(double cost) {
		this.cost = cost;
	}

	/** @see StoreCostResult#getEstimatedRowCount */
	public long getEstimatedRowCount() {
		return (long) rowCount;
	}

	/** @see StoreCostResult#setEstimatedRowCount */
	public void setEstimatedRowCount(long count) {
		/* This method is called by the store to
		 * give us the estimate for the # of rows
		 * returned in a scan.  So, we set both
		 * rowCount and singleScanRowCount here.
		 */
		rowCount = (double) count;
		singleScanRowCount = (double) count;
	}

	public CostEstimateImpl setState(double theCost,
										double theRowCount,
										CostEstimateImpl retval) {
		if (retval == null) {
			retval = new CostEstimateImpl();
		}

		retval.cost = theCost;
		retval.rowCount = theRowCount;

		return retval;
	}
}
