/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.sql.compile
   (C) Copyright IBM Corp. 1997, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.iapi.sql.compile;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.types.DataValueDescriptor;

import org.apache.derby.iapi.util.JBitSet;

/**
 * OptimizablePredicate provides services for optimizing predicates in a query.
 */

public interface OptimizablePredicate
{
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1997_2004;
	/**
	 * Get the map of referenced tables for this OptimizablePredicate.
	 *
	 * @return JBitSet	Referenced table map.
	 */
	JBitSet getReferencedMap();

	/**
	 * Return whether or not an OptimizablePredicate contains a subquery.
	 *
	 * @return boolean	Whether or not an OptimizablePredicate includes a subquery.
	 */
	boolean hasSubquery();

	/**
	 * Return whether or not an OptimizablePredicate contains a method call.
	 *
	 * @return boolean	Whether or not an OptimizablePredicate includes a method call.
	 */
	boolean hasMethodCall();

	/**
	 * Tell the predicate that it is to be used as a column in the start key
	 * value for an index scan.
	 */
	void markStartKey();

	/** Is this predicate a start key? */
	boolean isStartKey();

	/**
	 * Tell the predicate that it is to be used as a column in the stop key
	 * value for an index scan.
	 */
	void markStopKey();

	/** Is this predicate a stop key? */
	boolean isStopKey();

	/**
	 * Tell the predicate that it is to be used as a qualifier in an index
	 * scan.
	 */
	void markQualifier();

	/** Is this predicate a qualifier? */
	boolean isQualifier();

	/**
	 * Is this predicate a comparison with a known constant value?
	 *
	 * @param optTable	The Optimizable that we want to know whether we
	 *					are comparing to a known constant.
	 * @param considerParameters	Whether or not to consider parameters with defaults
	 *								as known constants.
	 */
	boolean compareWithKnownConstant(Optimizable optTable, boolean considerParameters);

	/**
	 * Get an Object representing the known constant value that the given
	 * Optimizable is being compared to.
	 *
	 * @exception StandardException		Thrown on error
	 */
	DataValueDescriptor getCompareValue(Optimizable optTable) 
        throws StandardException;

	/**
	 * Is this predicate an equality comparison with a constant expression?
	 * (IS NULL is considered to be an = comparison with a constant expression).
	 *
	 * @param optTable	The Optimizable for which we want to know whether
	 *					it is being equality-compared to a constant expression.
	 */
	boolean equalsComparisonWithConstantExpression(Optimizable optTable);

	
	/**
	 * Returns if the predicate involves an equal operator on one of the
	 * columns specified in the baseColumnPositions.
	 *
	 * @param 	baseColumnPositions	the column numbers on which the user wants
	 * to check if the equality condition exists.
	 * @param 	optTable the table for which baseColumnPositions are given.

		@return returns the index into baseColumnPositions of the column that has the
		equality operator.
	 */
	int hasEqualOnColumnList(int[] baseColumnPositions,
								 Optimizable optTable)
		throws StandardException;

	/**
	 * Get a (crude) estimate of the selectivity of this predicate.
	 * This is to be used when no better technique is available for
	 * estimating the selectivity - this method's estimate is a hard-
	 * wired number based on the type of predicate and the datatype
	 * (the selectivity of boolean is always 50%).
	 *
	 * @param optTable	The Optimizable that this predicate restricts
	 */
	double selectivity(Optimizable optTable);

	/**
	 * Get the position of the index column that this predicate restricts.
	 * NOTE: This assumes that this predicate is part of an
	 * OptimizablePredicateList, and that classify() has been called
	 * on the OptimizablePredicateList.
	 *
	 * @return The index position that this predicate restricts (zero-based)
	 */
	int getIndexPosition();
}
