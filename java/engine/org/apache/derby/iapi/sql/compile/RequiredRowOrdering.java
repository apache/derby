/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.sql.compile
   (C) Copyright IBM Corp. 1999, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.iapi.sql.compile;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.util.JBitSet;

/**
 * This interface provides a representation of the required ordering of rows
 * from a ResultSet.  Different operations can require ordering: ORDER BY,
 * DISTINCT, GROUP BY.  Some operations, like ORDER BY, require that the
 * columns be ordered a particular way, while others, like DISTINCT and
 * GROUP BY, reuire only that there be no duplicates in the result.
 */
public interface RequiredRowOrdering
{
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1999_2004;
	static final int SORT_REQUIRED = 1;
	static final int ELIMINATE_DUPS = 2;
	static final int NOTHING_REQUIRED = 3;

	/**
	 * Tell whether sorting is required for this RequiredRowOrdering,
	 * given a RowOrdering.
	 *
	 * @param rowOrdering	The order of rows in question
	 *
	 * @return	SORT_REQUIRED if sorting is required,
	 *			ELIMINATE_DUPS if no sorting is required but duplicates
	 *							must be eliminated (i.e. the rows are in
	 *							the right order but there may be duplicates),
	 *			NOTHING_REQUIRED is no operation is required
	 *
	 * @exception StandardException		Thrown on error
	 */
	int sortRequired(RowOrdering rowOrdering) throws StandardException;

	/**
	 * Tell whether sorting is required for this RequiredRowOrdering,
	 * given a RowOrdering representing a partial join order, and
	 * a bit map telling what tables are represented in the join order.
	 * This is useful for reducing the number of cases the optimizer
	 * has to consider.
	 *
	 * @param rowOrdering	The order of rows in the partial join order
	 * @param tableMap		A bit map of the tables in the partial join order
	 *
	 * @return	SORT_REQUIRED if sorting is required,
	 *			ELIMINATE_DUPS if no sorting is required by duplicates
	 *							must be eliminated (i.e. the rows are in
	 *							the right order but there may be duplicates),
	 *			NOTHING_REQUIRED is no operation is required
	 *
	 * @exception StandardException		Thrown on error
	 */
	int sortRequired(RowOrdering rowOrdering, JBitSet tableMap)
			throws StandardException;

	/**
	 * Estimate the cost of doing a sort for this row ordering, given
	 * the number of rows to be sorted.  This does not take into account
	 * whether the sort is really needed.  It also estimates the number of
	 * result rows.
	 *
	 * @param estimatedInputRows	The estimated number of rows to sort
	 * @param rowOrdering			The ordering of the input rows
	 * @param resultCost			A place to store the resulting cost
	 *
	 * @exception StandardException		Thrown on error
	 */
	void estimateCost(double estimatedInputRows,
						RowOrdering rowOrdering,
						CostEstimate resultCost)
					throws StandardException;

	/**
	 * Indicate that a sort is necessary to fulfill this required ordering.
	 * This method may be called many times during a single optimization.
	 */
	void sortNeeded();

	/**
	 * Indicate that a sort is *NOT* necessary to fulfill this required
	 * ordering.  This method may be called many times during a single
	 * optimization.
	 */
	void sortNotNeeded();

	boolean getSortNeeded();
}
