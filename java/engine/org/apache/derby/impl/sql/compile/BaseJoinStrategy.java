/*

   Derby - Class org.apache.derby.impl.sql.compile.BaseJoinStrategy

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

package org.apache.derby.impl.sql.compile;

import org.apache.derby.iapi.services.loader.GeneratedMethod;

import org.apache.derby.iapi.services.compiler.MethodBuilder;

import org.apache.derby.iapi.sql.compile.ExpressionClassBuilderInterface;
import org.apache.derby.iapi.sql.compile.JoinStrategy;
import org.apache.derby.iapi.sql.compile.Optimizable;
import org.apache.derby.iapi.sql.compile.OptimizablePredicateList;

import org.apache.derby.iapi.sql.Activation;

import org.apache.derby.iapi.sql.dictionary.ConglomerateDescriptor;
import org.apache.derby.iapi.sql.dictionary.ConstraintDescriptor;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;

import org.apache.derby.iapi.store.access.Qualifier;
import org.apache.derby.iapi.store.access.StaticCompiledOpenConglomInfo;
import org.apache.derby.iapi.store.access.TransactionController;

import org.apache.derby.impl.sql.compile.ExpressionClassBuilder;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.reference.ClassName;

abstract public class BaseJoinStrategy implements JoinStrategy {
	public BaseJoinStrategy() {
	}

	/** @see JoinStrategy#bulkFetchOK */
	public boolean bulkFetchOK() {
		return true;
	}

	/** @see JoinStrategy#ignoreBulkFetch */
	public boolean ignoreBulkFetch() {
		return false;
	}

	protected void fillInScanArgs1(
								TransactionController tc,
								MethodBuilder mb,
								Optimizable innerTable,
								OptimizablePredicateList predList,
								ExpressionClassBuilderInterface acbi,
								MethodBuilder resultRowAllocator
								)
					throws StandardException {
		boolean				   sameStartStopPosition = predList.sameStartStopPosition();
		ExpressionClassBuilder acb = (ExpressionClassBuilder) acbi;
		long				   conglomNumber = 
								innerTable.getTrulyTheBestAccessPath().
									getConglomerateDescriptor().
										getConglomerateNumber();
		StaticCompiledOpenConglomInfo scoci = tc.getStaticCompiledConglomInfo(conglomNumber);
		
		mb.push(conglomNumber);
		mb.push(acb.addItem(scoci));
		acb.pushThisAsActivation(mb);

		acb.pushMethodReference(mb, resultRowAllocator);
		mb.push(innerTable.getResultSetNumber());

		predList.generateStartKey(acb, mb, innerTable);
		mb.push(predList.startOperator(innerTable));

		if (! sameStartStopPosition) {
			predList.generateStopKey(acb, mb, innerTable);
		} else {
			mb.pushNull(ClassName.GeneratedMethod);
		}

		mb.push(predList.stopOperator(innerTable));
		mb.push(sameStartStopPosition);

		predList.generateQualifiers(acb, mb, innerTable, true);
		mb.upCast(ClassName.Qualifier + "[][]");
	}

	protected void fillInScanArgs2(MethodBuilder mb,
								Optimizable innerTable,
								int bulkFetch,
								int colRefItem,
								int indexColItem,
								int lockMode,
								boolean tableLocked,
								int isolationLevel) 
		throws StandardException
	{
		mb.push(innerTable.getBaseTableName());

		ConglomerateDescriptor cd =
			innerTable.getTrulyTheBestAccessPath().getConglomerateDescriptor();
		if (cd.isConstraint())
		{
			DataDictionary dd = innerTable.getDataDictionary();
			TableDescriptor td = innerTable.getTableDescriptor();
			ConstraintDescriptor constraintDesc = dd.getConstraintDescriptor(
														td, cd.getUUID());
			mb.push(constraintDesc.getConstraintName());
		} else if (cd.isIndex())  {
			mb.push(cd.getConglomerateName());
		} else {
			mb.pushNull("java.lang.String");
		}

		// Whether or not the conglomerate is the backing index for a constraint
		mb.push(cd.isConstraint());

		// tell it whether it's to open for update, which we should do if
		// it's an update or delete statement, or if it's the target
		// table of an updatable cursor.
		mb.push(innerTable.forUpdate());

		mb.push(colRefItem);

		mb.push(indexColItem);

		mb.push(lockMode);

		mb.push(tableLocked);

		mb.push(isolationLevel);

		if (bulkFetch > 0) {
			mb.push(bulkFetch);
		}

		/* 1 row scans (avoiding 2nd next()) are
 		 * only meaningful for some join strategies.
		 * (Only an issue for outer table, which currently
		 * can only be nested loop, as avoidance of 2nd next
		 * on inner table already factored in to join node.)
		 */
		if (validForOutermostTable())
		{
			mb.push(innerTable.isOneRowScan());
		}

		mb.push(
				innerTable.getTrulyTheBestAccessPath().
												getCostEstimate().rowCount());

		mb.push(
						innerTable.getTrulyTheBestAccessPath().
										getCostEstimate().getEstimatedCost());
	}

	/**
	 * @see JoinStrategy#isHashJoin
	 */
	public boolean isHashJoin()
	{
		return false;
	}

	/**
	 * Can this join strategy be used on the
	 * outermost table of a join.
	 *
	 * @return Whether or not this join strategy
	 * can be used on the outermose table of a join.
	 */
	protected boolean validForOutermostTable()
	{
		return false;
	}
}
