/*

   Derby - Class org.apache.derby.iapi.sql.compile.CostEstimate

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

package org.apache.derby.iapi.sql.compile;

import org.apache.derby.iapi.store.access.StoreCostResult;

/**
 * A CostEstimate represents the cost of getting a ResultSet, along with the
 * ordering of rows in the ResultSet, and the estimated number of rows in
 * this ResultSet.
 *
 * @author Jeff Lichtman
 */

public interface CostEstimate extends StoreCostResult
{
	/**
	 * Set the cost for this cost estimate.
	 */
	void setCost(double cost, double rowCount, double singleScanRowCount);

	/**
	 * Copy the values from the given cost estimate into this one.
	 */
	void setCost(CostEstimate other);

	/**
	 * Set the single scan row count.
	 */
	void setSingleScanRowCount(double singleRowScanCount);

	/**
	 * Compare this cost estimate with the given cost estimate.
	 *
	 * @param other		The cost estimate to compare this one with
	 *
	 * @return	< 0 if this < other, 0 if this == other, > 0 if this > other
	 */
	double compare(CostEstimate other);

	/**
	 * Add this cost estimate to another one.  This presumes that any row
	 * ordering is destroyed.
	 *
	 * @param addend	This cost estimate to add this one to.
	 * @param retval	If non-null, put the result here.
	 * 
	 * @return  this + other.
	 */
	CostEstimate add(CostEstimate addend, CostEstimate retval);

	/**
	 * Multiply this cost estimate by a scalar, non-dimensional number.  This
	 * presumes that any row ordering is destroyed.
	 *
	 * @param multiplicand	The value to multiply this CostEstimate by.
	 * @param retval	If non-null, put the result here.
	 * 
	 * @return	this * multiplicand
	 */
	CostEstimate multiply(double multiplicand, CostEstimate retval);

	/**
	 * Divide this cost estimate by a scalar, non-dimensional number.
	 *
	 * @param divisor	The value to divide this CostEstimate by.
	 * @param retval	If non-null, put the result here.
	 *
	 * @return	this / divisor
	 */
	CostEstimate divide(double divisor, CostEstimate retval);

	/**
	 * Get the estimated number of rows returned by the ResultSet that this
	 * CostEstimate models.
	 */
	double rowCount();

	/**
	 * Get the estimated number of rows returned by a single scan of
	 * the ResultSet that this CostEstimate models.
	 */
	double singleScanRowCount();

	/** Get a copy of this CostEstimate */
	CostEstimate cloneMe();

	/**
	 * Return whether or not this CostEstimate is uninitialized.
	 *
	 * @return Whether or not this CostEstimate is uninitialized.
	 */
	public boolean isUninitialized();
}
