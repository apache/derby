/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.sql.compile
   (C) Copyright IBM Corp. 1997, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.iapi.sql.compile;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.sql.dictionary.DataDictionary;

/**
 * OptimizableList provides services for optimizing a list of
 * Optimizables (tables) in a query.
 */

public interface OptimizableList {
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1997_2004;

	/**
	 *  Return the number of Optimizables in the list.
	 *
	 *  @return integer		The number of Optimizables in the list.
	 */
	public int size();

	/**
	 *  Return the nth Optimizable in the list.
	 *
	 *  @param n				"index" (0 based) into the list.
	 *
	 *  @return Optimizable		The nth Optimizables in the list.
	 */
	public Optimizable getOptimizable(int n);

	/**
	 * Set the nth Optimizable to the specified Optimizable.
	 *
	 *  @param n				"index" (0 based) into the list.
	 *  @param optimizable		New nth Optimizable.
	 *
	 *  @return Nothing.
	 */
	public void setOptimizable(int n, Optimizable optimizable);

	/** 
	 * Verify that the Properties list with optimizer overrides, if specified, is valid
	 *
	 * @param dDictionary	The DataDictionary to use.
	 *
	 * @return Nothing.
	 * @exception StandardException		Thrown on error
	 */
	public void verifyProperties(DataDictionary dDictionary) throws StandardException;

	/**
	 * Set the join order for this list of optimizables.  The join order is
	 * represented as an array of integers - each entry in the array stands
	 * for the order of the corresponding element in the list.  For example,
	 * a joinOrder of {2, 0, 1} means that the 3rd Optimizable in the list
	 * (element 2, since we are zero-based) is the first one in the join
	 * order, followed by the 1st element in the list, and finally by the
	 * 2nd element in the list.
	 *
	 * This method shuffles this OptimizableList to match the join order.
	 *
	 * Obviously, the size of the array must equal the number of elements in
	 * the array, and the values in the array must be between 0 and the
	 * number of elements in the array minus 1, and the values in the array
	 * must be unique.
	 */
	public void reOrder(int[] joinOrder);

	/**
	 * user can specify that s/he doesn't want statistics to be considered when
	 * optimizing the query.
	 */
	public boolean useStatistics();

	/**
	 * Tell whether the join order should be optimized.
	 */
	public boolean optimizeJoinOrder();

	/**
	 * Tell whether the join order is legal.
	 */
	public boolean legalJoinOrder(int numTablesInQuery);

	/**
	 * Init the access paths for these optimizables.
	 *
	 * @param optimizer The optimizer being used.
	 *
	 * @return Nothing.
	 */
	public void initAccessPaths(Optimizer optimizer);
}
