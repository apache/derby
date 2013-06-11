/*

   Derby - Class org.apache.derby.impl.sql.compile.Level2OptimizerImpl

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

package org.apache.derby.impl.sql.compile;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.sql.compile.JoinStrategy;
import org.apache.derby.iapi.sql.compile.Optimizable;
import org.apache.derby.iapi.sql.compile.OptimizableList;
import org.apache.derby.iapi.sql.compile.OptimizablePredicateList;
import org.apache.derby.iapi.sql.compile.Optimizer;
import org.apache.derby.iapi.sql.compile.CostEstimate;
import org.apache.derby.iapi.sql.compile.RequiredRowOrdering;

import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;

import org.apache.derby.iapi.sql.dictionary.ConglomerateDescriptor;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;

/**
 * This is the Level 2 Optimizer.
 */

public class Level2OptimizerImpl extends OptimizerImpl
{
	Level2OptimizerImpl(OptimizableList optimizableList, 
				  OptimizablePredicateList predicateList,
				  DataDictionary dDictionary,
				  boolean ruleBasedOptimization,
				  boolean noTimeout,
				  boolean useStatistics,
				  int maxMemoryPerTable,
				  JoinStrategy[] joinStrategies,
				  int tableLockThreshold,
				  RequiredRowOrdering requiredRowOrdering,
				  int numTablesInQuery,
				  LanguageConnectionContext lcc)
		throws StandardException
	{
		super(optimizableList, predicateList, dDictionary,
			  ruleBasedOptimization, noTimeout, useStatistics, maxMemoryPerTable,
			  joinStrategies, tableLockThreshold, requiredRowOrdering,
			  numTablesInQuery);

		this.lcc = lcc;

		// Optimization started
		if (tracingIsOn()) { tracer().traceStart( timeOptimizationStarted, hashCode(), optimizableList ); }
	}

	/** @see Optimizer#getLevel */
	public int getLevel()
	{
		return 2;
	}

	/** @see Optimizer#newCostEstimate */
	public CostEstimate newCostEstimate()
	{
		return new Level2CostEstimateImpl();
	}

	public CostEstimateImpl getNewCostEstimate(double theCost,
							double theRowCount,
							double theSingleScanRowCount)
	{
		return new Level2CostEstimateImpl(theCost, theRowCount, theSingleScanRowCount);
	}

	private String bestCost()
	{
		return "Best cost = " + bestCost + "\n";
	}

	private String lockModeThreshold(
						String lockMode, String relop,
						double rowCount, int threshold)
	{
		return
			"Lock mode set to " + lockMode + 
			" because estimated row count of " + rowCount +
			" " + relop + " than threshold of " + threshold;
	}


}
