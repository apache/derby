/*

   Derby - Class org.apache.derby.impl.sql.compile.DistinctNode

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

package	org.apache.derby.impl.sql.compile;

import java.util.List;
import java.util.Properties;
import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.shared.common.reference.ClassName;
import org.apache.derby.iapi.services.classfile.VMOpcode;
import org.apache.derby.iapi.services.compiler.MethodBuilder;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derby.iapi.sql.compile.CostEstimate;
import org.apache.derby.iapi.sql.compile.Optimizable;
import org.apache.derby.iapi.sql.compile.OptimizablePredicate;
import org.apache.derby.iapi.sql.compile.OptimizablePredicateList;
import org.apache.derby.iapi.sql.compile.Optimizer;
import org.apache.derby.iapi.sql.compile.RequiredRowOrdering;
import org.apache.derby.iapi.sql.compile.RowOrdering;
import org.apache.derby.iapi.sql.dictionary.ConglomerateDescriptor;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;

/**
 * A DistinctNode represents a result set for a distinct operation
 * on a select.  It has the same description as its input result set.
 *
 * For the most part, it simply delegates operations to its childResultSet,
 * which is currently expected to be a ProjectRestrictResultSet generated
 * for a SelectNode.
 *
 * NOTE: A DistinctNode extends FromTable since it can exist in a FromList.
 *
 */
class DistinctNode extends SingleChildResultSetNode
{
	boolean inSortedOrder;

	/**
     * Constructor for a DistinctNode.
	 *
	 * @param childResult	The child ResultSetNode
	 * @param inSortedOrder	Whether or not the child ResultSetNode returns its
	 *						output in sorted order.
	 * @param tableProperties	Properties list associated with the table
     * @param cm            The context manager
	 *
	 * @exception StandardException		Thrown on error
	 */
    DistinctNode(ResultSetNode childResult,
                 boolean inSortedOrder,
                 Properties tableProperties,
                 ContextManager cm) throws StandardException
	{
        super(childResult, tableProperties, cm);

		if (SanityManager.DEBUG)
		{
			if (!(childResult instanceof Optimizable))
			{
				SanityManager.THROWASSERT("childResult, " + childResult.getClass().getName() +
					", expected to be instanceof Optimizable");
			}
			if (!(childResult instanceof FromTable))
			{
				SanityManager.THROWASSERT("childResult, " + childResult.getClass().getName() +
					", expected to be instanceof FromTable");
			}
		}

		/*
			We want our own resultColumns, which are virtual columns
			pointing to the child result's columns.

			We have to have the original object in the distinct node,
			and give the underlying project the copy.
		 */

		/* We get a shallow copy of the ResultColumnList and its 
		 * ResultColumns.  (Copy maintains ResultColumn.expression for now.)
		 */
        final ResultColumnList prRCList =
                this.childResult.getResultColumns().copyListAndObjects();
        setResultColumns( this.childResult.getResultColumns() );
		this.childResult.setResultColumns(prRCList);

		/* Replace ResultColumn.expression with new VirtualColumnNodes
		 * in the DistinctNode's RCL.  (VirtualColumnNodes include
		 * pointers to source ResultSetNode, this, and source ResultColumn.)
		 */
        getResultColumns().genVirtualColumnNodes(this, prRCList);

		/* Verify that we can perform a DISTINCT on the
		 * underlying tree.
		 */
        getResultColumns().verifyAllOrderable();
        this.inSortedOrder = inSortedOrder;
	}

	/*
	 *  Optimizable interface
	 */

	/**
	 * @see Optimizable#optimizeIt
	 *
	 * @exception StandardException		Thrown on error
	 */
    @Override
	public CostEstimate optimizeIt(Optimizer optimizer,
									OptimizablePredicateList predList,
									CostEstimate outerCost,
									RowOrdering rowOrdering)
			throws StandardException
	{
        ((Optimizable) childResult).optimizeIt(optimizer,
                predList,
                outerCost,
                rowOrdering);

		return super.optimizeIt(optimizer, predList, outerCost, rowOrdering);
	}

	/**
	 * @see Optimizable#estimateCost
	 *
	 * @exception StandardException		Thrown on error
	 */
    @Override
	public CostEstimate estimateCost(OptimizablePredicateList predList,
									ConglomerateDescriptor cd,
									CostEstimate outerCost,
									Optimizer optimizer,
									RowOrdering rowOrdering)
			throws StandardException
	{
		// RESOLVE: WE NEED TO ADD IN THE COST OF SORTING HERE, AND FIGURE
		// OUT HOW MANY ROWS WILL BE ELIMINATED.
		CostEstimate childCost =
			((Optimizable) childResult).estimateCost(predList,
									cd,
									outerCost,
									optimizer,
									rowOrdering);

		setCostEstimate( getCostEstimate(optimizer) );
		getCostEstimate().setCost(childCost.getEstimatedCost(),
							 childCost.rowCount(),
							 childCost.singleScanRowCount());


		/*
		** No need to use estimateCost on join strategy - that has already
		** been done on the child.
		*/
		return getCostEstimate();
	}

	/**
	 * @see org.apache.derby.iapi.sql.compile.Optimizable#pushOptPredicate
	 *
	 * @exception StandardException		Thrown on error
	 */
    @Override
	public boolean pushOptPredicate(OptimizablePredicate optimizablePredicate)
			throws StandardException
	{
		return false;
		// return ((Optimizable) childResult).pushOptPredicate(optimizablePredicate);
	}


	/**
	 * Optimize this DistinctNode.  
	 *
	 * @param dataDictionary	The DataDictionary to use for optimization
	 * @param predicates		The PredicateList to optimize.  This should
	 *							be a join predicate.
	 * @param outerRows			The number of outer joining rows
	 *
	 * @return	ResultSetNode	The top of the optimized subtree
	 *
	 * @exception StandardException		Thrown on error
	 */
    @Override
    ResultSetNode optimize(DataDictionary dataDictionary,
								  PredicateList predicates,
								  double outerRows) 
					throws StandardException
	{
		/* We need to implement this method since a PRN can appear above a
		 * SelectNode in a query tree.
		 */
        childResult = childResult.optimize(dataDictionary,
                                           predicates,
                                           outerRows);

		// RESOLVE: NEED TO FACTOR IN COST OF SORTING AND FIGURE OUT HOW
		// MANY ROWS HAVE BEEN ELIMINATED.
        setCostEstimate( getOptimizerFactory().getCostEstimate() );

		getCostEstimate().setCost(childResult.getCostEstimate().getEstimatedCost(),
							 childResult.getCostEstimate().rowCount(),
							 childResult.getCostEstimate().singleScanRowCount());

		return this;
	}

	/**
	 * Return whether or not the underlying ResultSet tree
	 * is ordered on the specified columns.
	 * RESOLVE - This method currently only considers the outermost table 
	 * of the query block.
	 *
	 * @param	crs					The specified ColumnReference[]
	 * @param	permuteOrdering		Whether or not the order of the CRs in the array can be permuted
     * @param   fbtHolder           List that is to be filled with the FromBaseTable
	 *
	 * @return	Whether the underlying ResultSet tree
	 * is ordered on the specified column.
	 */
    @Override
    boolean isOrderedOn(ColumnReference[] crs,
                        boolean permuteOrdering,
                        List<FromBaseTable> fbtHolder)
	{
		/* RESOLVE - DistinctNodes are ordered on their RCLs.
		 * Walk RCL to see if cr is 1st non-constant column in the
		 * ordered result.
		 */
		return false;
	}

    /**
     * generate the distinct result set operating over the source
     * result set.
     *
	 * @exception StandardException		Thrown on error
     */
    @Override
    void generate(ActivationClassBuilder acb, MethodBuilder mb)
							throws StandardException
	{
		/* Get the next ResultSet#, so we can number this ResultSetNode, its
		 * ResultColumnList and ResultSet.
		 */
		assignResultSetNumber();

		// Get the final cost estimate based on the child's cost.
		setCostEstimate( childResult.getFinalCostEstimate() );

		/*
			create the orderItem and stuff it in.
		 */
		int orderItem = acb.addItem(acb.getColumnOrdering(getResultColumns()));

		/* Generate the SortResultSet:
		 *	arg1: childExpress - Expression for childResultSet
		 *  arg2: distinct - true, of course
		 *  arg3: isInSortedOrder - is the source result set in sorted order
		 *  arg4: orderItem - entry in saved objects for the ordering
		 *  arg5: rowAllocator - method to construct rows for fetching
		 *			from the sort
		 *  arg6: row size
		 *  arg7: resultSetNumber
		 */

		acb.pushGetResultSetFactoryExpression(mb);

		childResult.generate(acb, mb);
		mb.push(true);
		mb.push(inSortedOrder);
		mb.push(orderItem);
        mb.push(acb.addItem(getResultColumns().buildRowTemplate()));
		mb.push(getResultColumns().getTotalColumnSize());
		mb.push(getResultSetNumber());
		mb.push(getCostEstimate().rowCount());
		mb.push(getCostEstimate().getEstimatedCost());

		mb.callMethod(VMOpcode.INVOKEINTERFACE, (String) null, "getSortResultSet",
                ClassName.NoPutResultSet, 9);
	}
}
