/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.sql.compile
   (C) Copyright IBM Corp. 1997, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

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
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1997_2004;
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
