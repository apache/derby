/*

   Derby - Class org.apache.derby.impl.sql.compile.Level2OptimizerFactoryImpl

   Copyright 2000, 2004 The Apache Software Foundation or its licensors, as applicable.

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
import org.apache.derby.iapi.sql.compile.JoinStrategy;
import org.apache.derby.iapi.sql.compile.OptimizableList;
import org.apache.derby.iapi.sql.compile.OptimizablePredicateList;
import org.apache.derby.iapi.sql.compile.Optimizer;
import org.apache.derby.iapi.sql.compile.OptimizerFactory;
import org.apache.derby.iapi.sql.compile.RequiredRowOrdering;

import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;

import org.apache.derby.iapi.store.access.TransactionController;

import org.apache.derby.iapi.sql.dictionary.DataDictionary;

import org.apache.derby.iapi.services.monitor.ModuleControl;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.services.property.PropertyUtil;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.reference.Property;

import org.apache.derby.impl.sql.compile.OptimizerFactoryImpl;

import java.util.Properties;

/**
	This is simply the factory for creating an optimizer.
 */

public class Level2OptimizerFactoryImpl
	extends OptimizerFactoryImpl 
{

	//
	// ModuleControl interface
	//

	public void boot(boolean create, Properties startParams)
			throws StandardException 
	{
		super.boot(create, startParams);
	}

	//
	// OptimizerFactory interface
	//

	/**
	 * @see OptimizerFactory#supportsOptimizerTrace
	 */
	public boolean supportsOptimizerTrace()
	{
		return true;
	}

	//
	// class interface
	//
	public Level2OptimizerFactoryImpl() 
	{
	}

	protected Optimizer getOptimizerImpl(
								  OptimizableList optimizableList,
								  OptimizablePredicateList predList,
								  DataDictionary dDictionary,
								  RequiredRowOrdering requiredRowOrdering,
								  int numTablesInQuery,
								  LanguageConnectionContext lcc)
				throws StandardException
	{

		return new Level2OptimizerImpl(
							optimizableList,
							predList,
							dDictionary,
							ruleBasedOptimization,
							noTimeout,
							useStatistics,
							maxMemoryPerTable,
							joinStrategySet,
							lcc.getLockEscalationThreshold(),
							requiredRowOrdering,
							numTablesInQuery,
							lcc);
	}

	/**
	 * @see OptimizerFactory#getCostEstimate
	 *
	 * @exception StandardException		Thrown on error
	 */
	public CostEstimate getCostEstimate()
		throws StandardException
	{
		return new Level2CostEstimateImpl();
	}
}

