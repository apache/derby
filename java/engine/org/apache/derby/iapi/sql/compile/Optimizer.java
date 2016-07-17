/*

   Derby - Class org.apache.derby.iapi.sql.compile.Optimizer

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

package org.apache.derby.iapi.sql.compile;

import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;
import org.apache.derby.iapi.sql.dictionary.ConglomerateDescriptor;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.util.JBitSet;

/**
 * Optimizer provides services for optimizing a query.
 * RESOLVE:
 *	o  Need to figure out what to do about subqueries, figuring out
 *	   their attachment points and how to communicate them back to the
 *	   caller.
 */

public interface Optimizer
{
	/**
		Module name for the monitor's module locating system.
	 */
	String MODULE = "org.apache.derby.iapi.sql.compile.Optimizer";

	/**
		Property name for controlling whether to do join order optimization.
	 */
	String JOIN_ORDER_OPTIMIZATION = "derby.optimizer.optimizeJoinOrder";

	/**
		Property name for controlling whether to do rule-based optimization,
		as opposed to cost-based optimization.
	 */
	String RULE_BASED_OPTIMIZATION =
						"derby.optimizer.ruleBasedOptimization";

	/**
		Property name for controlling whether the optimizer ever times out
		while optimizing a query and goes with the best plan so far.
	 */
	String NO_TIMEOUT = "derby.optimizer.noTimeout";

	/**
		Property name for controlling the maximum size of memory (in KB)
		the optimizer can use for each table.  If an access path takes
		memory larger than that size for a table, the access path is skipped.
		Default is 1024 (KB).
	 */
	String MAX_MEMORY_PER_TABLE = "derby.language.maxMemoryPerTable";

	/**
		Maximum size of dynamically created materialized rows. Caching large results
		use lot of memory and can cause stack overflow. See DERBY-634
	*/
	int MAX_DYNAMIC_MATERIALIZED_ROWS = 512;

	/**
	   Property name for disabling statistics use for all queries.
	*/
	String USE_STATISTICS = "derby.language.useStatistics";

	/** Indicates a "normal" plan that is not optimized to do sort avoidance */
	int NORMAL_PLAN = 1;

	/** Indicates a sort-avoidance plan */
	int SORT_AVOIDANCE_PLAN = 2;

	/**
	 * Iterate through the permutations, returning false when the permutations
	 * are exhausted.
	 * NOTE - Implementers are responsible for hiding tree pruning of permutations
	 * behind this method call.
	 *
	 * @return boolean	True - An optimizable permutation remains.
	 *					False - Permutations are exhausted.
	 *
	 * @exception StandardException		Thrown on error
	 */
	public boolean getNextPermutation() throws StandardException;

	/**
	 * Iterate through the "decorated permutations", returning false when they
	 * are exhausted.
	 * NOTE - Implementers are responsible for hiding tree pruning of access
	 * methods behind this method call.
	 *
	 * @return boolean	True - An optimizable decorated permutation remains.
	 *					False - Decorated permutations are exhausted.
	 *
	 * @exception StandardException		Thrown on error
	 */
	public boolean getNextDecoratedPermutation() throws StandardException;

	/**
	 * Cost the current permutation.
	 * Caller is responsible for pushing all predicates which can be evaluated 
	 * prior to costing.
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void costPermutation() throws StandardException;

	/**
	 * Cost the current Optimizable with the specified OPL.
	 * Caller is responsible for pushing all predicates which can be evaluated 
	 * prior to costing.
	 *
	 * @param optimizable	The Optimizable
	 * @param td			TableDescriptor of the Optimizable
	 * @param cd			The ConglomerateDescriptor for the conglom to cost
	 *						(This should change to an object to represent
	 *						access paths, but for now this is OK).
	 * @param predList		The OptimizablePredicateList to apply
	 * @param outerCost		The cost of the tables outer to the one being
	 *						optimizer - tells how many outer rows there are.
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void	costOptimizable(Optimizable optimizable,
								TableDescriptor td, 
								ConglomerateDescriptor cd,
								OptimizablePredicateList predList,
								CostEstimate outerCost)
			throws StandardException;

	/**
	 * Consider the cost of the given optimizable.  This method is like
	 * costOptimizable, above, but it is used when the Optimizable does
	 * not need help from the optimizer in costing the Optimizable (in practice,
	 * all Optimizables except FromBaseTable use this method.
	 *
	 * Caller is responsible for pushing all predicates which can be evaluated 
	 * prior to costing.
	 *
	 * @param optimizable	The Optimizable
	 * @param predList		The OptimizablePredicateList to apply
	 * @param estimatedCost	The estimated cost of the given optimizable
	 * @param outerCost		The cost of the tables outer to the one being
	 *						optimizer - tells how many outer rows there are.
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void	considerCost(Optimizable optimizable,
							OptimizablePredicateList predList,
							CostEstimate estimatedCost,
							CostEstimate outerCost)
			throws StandardException;

	/**
	 * Return the DataDictionary that the Optimizer is using.
	 * This is useful when an Optimizable needs to call optimize() on
	 * a child ResultSetNode.
	 * 
	 * @return DataDictionary	DataDictionary that the Optimizer is using.
	 */
	public DataDictionary getDataDictionary();

	/**
	 * Modify the access path for each Optimizable, as necessary.  This includes
	 * things like adding result sets to translate from index rows to base rows.
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void modifyAccessPaths() throws StandardException;

	/** Get the estimated cost of the optimized query */
	public CostEstimate getOptimizedCost();

	/**
	 * Get the final estimated cost of the optimized query.  This
	 * should be the cost that corresponds to the best overall join
	 * order chosen by the optimizer, and thus this method should
	 * only be called after optimization is complete (i.e. when
	 * modifying access paths).
	 */
	public CostEstimate getFinalCost();

	/**
	 * Prepare for another round of optimization.
	 *
	 * This method is called before every "round" of optimization, where
	 * we define a "round" to be the period between the last time a call to
	 * getOptimizer() (on either a ResultSetNode or an OptimizerFactory)
	 * returned _this_ Optimizer and the time a call to this Optimizer's
	 * getNextPermutation() method returns FALSE.  Any re-initialization
	 * of state that is required before each round should be done in this
	 * method.
	 */
	public void prepForNextRound();

	/**
	 * Set the estimated number of outer rows - good for optimizing nested
	 * optimizables like subqueries and join nodes.
	 */
	public void setOuterRows(double outerRowCount);

	/**
	 * Get the number of join strategies supported by this optimizer.
	 */
	public int getNumberOfJoinStrategies();

	/**
	 * Get the maximum number of estimated rows touched in a table before
	 * we decide to open the table with table locking (as opposed to row
	 * locking.
	 */
	public int tableLockThreshold();

	/**
	 * Gets a join strategy by number (zero-based).
	 */
	JoinStrategy getJoinStrategy(int whichStrategy);

	/**
	 * Gets a join strategy by name.  Returns null if not found.
	 * The look-up is case-insensitive.
	 */
	JoinStrategy getJoinStrategy(String whichStrategy);

	/**
	 * Get the level of this optimizer.
	 *
	 * @return The level of this optimizer.
	 */
	public int getLevel();

	/**
	 * Tells whether any of the tables outer to the current one
	 * has a uniqueness condition on the given predicate list,
	 * and if so, how many times each unique key can be seen by
	 * the current table.
	 *
	 * @param predList		The predicate list to check
	 *
	 * @return	&lt;= 0 means there is no uniqueness condition
	 *			&gt; 0 means there is a uniqueness condition on an
	 *			outer table, and the return value is the reciprocal of
	 *			the maximum number of times the optimizer estimates that each
	 *			unique key will be returned. For example, 0.5 means the
	 *			optimizer thinks each distinct join key will be returned
	 *			at most twice.
	 *
	 * @exception StandardException		Thrown on error
	 */
	double uniqueJoinWithOuterTable(OptimizablePredicateList predList)
			throws StandardException;
	
	/** 
	 * If statistics should be considered by the optimizer while optimizing 
	 * a query. The user may disable the use of statistics by setting the
	 * property derby.optimizer.useStatistics or by using the property
	 * useStatistics in a query.
	 *
	 * @see #USE_STATISTICS
	 */
	public boolean useStatistics();

    /**
     * @return the maximum number of bytes to be used per table.
     */
    public int getMaxMemoryPerTable();

    /**
     * Get the number of optimizables being considered by this Optimizer.
     */
    public  int getOptimizableCount();

    /**
     * Get the ith (0-based) Optimizable being considered by this Optimizer.
     */
    public  Optimizable getOptimizable( int idx );

	/**
	 * Process (i.e. add, load, or remove) current best join order as the
	 * best one for some outer query or ancestor node, represented by another
	 * Optimizer or an instance of FromTable, respectively. Then
	 * iterate through our optimizableList and tell each Optimizable
	 * to do the same.  See Optimizable.updateBestPlan() for more on why
	 * this is necessary.
	 *
	 * @param action Indicates whether to add, load, or remove the plan
	 * @param planKey Object to use as the map key when adding/looking up
	 *  a plan.  If this is an instance of Optimizer then it corresponds
	 *  to an outer query; otherwise it's some Optimizable above this
	 *  Optimizer that could potentially reject plans chosen by this
	 *  Optimizer.
	 */
	public void updateBestPlanMaps(short action, Object planKey)
        throws StandardException;
}
