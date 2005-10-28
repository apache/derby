/*

   Derby - Class org.apache.derby.iapi.sql.compile.OptimizablePredicate

   Copyright 1997, 2004 The Apache Software Foundation or its licensors, as applicable.

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

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.types.DataValueDescriptor;

import org.apache.derby.iapi.util.JBitSet;

/**
 * OptimizablePredicate provides services for optimizing predicates in a query.
 */

public interface OptimizablePredicate
{
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
