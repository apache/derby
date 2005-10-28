/*

   Derby - Class org.apache.derby.iapi.sql.compile.OptimizerFactory

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

import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;

import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.iapi.services.context.ContextManager;

import org.apache.derby.iapi.error.StandardException;

/**
	This is simply the factory for creating an optimizer.
	<p>
	There is expected to be only one of these configured per database.
 */

public interface OptimizerFactory {
	/**
		Module name for the monitor's module locating system.
	 */
	String MODULE = "org.apache.derby.iapi.sql.compile.OptimizerFactory";

	/**
	 * Only one optimizer level should exist in the database, however, the
	 * connection may have multiple instances of that optimizer
	 * at a given time.
	 *
	 * @param ofc the optimizer context for the current connection
	 * @param optimizable	The list of Optimizables to optimize.
	 * @param predicateList	The list of unassigned OptimizablePredicates.
	 * @param dDictionary	The DataDictionary to use.
	 * @param requiredRowOrdering	The required ordering of the rows to
	 *								come out of the optimized result set
	 * @param numTablesInQuery	The number of tables in the current query
	 * @param lcc			The LanguageConnectionContext
	 *
	 * RESOLVE - We probably want to pass a subquery list, once we define a
	 * new interface for them, so that the Optimizer can out where to attach
	 * the subqueries.
	 *
	 * @exception StandardException		Thrown on error
	 */
	public Optimizer getOptimizer( OptimizableList optimizableList,
								  OptimizablePredicateList predicateList,
								  DataDictionary dDictionary,
								  RequiredRowOrdering requiredRowOrdering,
								  int numTablesInQuery,
								  LanguageConnectionContext lcc)
			throws StandardException;


	/**
	 * Return a new CostEstimate.
	 *
	 * @exception StandardException		Thrown on error
	 */
	public CostEstimate getCostEstimate()
		throws StandardException;

	/**
	 * Return whether or not the optimizer associated with
	 * this factory supports optimizer trace.
	 *
	 * @return Whether or not the optimizer associated with
	 * this factory supports optimizer trace.
	 */
	public boolean supportsOptimizerTrace();

	/**
	 * Return the maxMemoryPerTable setting, this is used in
	 * optimizer, as well as subquery materialization at run time.
	 *
	 * @return	maxMemoryPerTable value
	 */
	public int getMaxMemoryPerTable();
}
