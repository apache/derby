/*

   Derby - Class org.apache.derby.impl.sql.compile.IntersectNode

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

import java.sql.Types;
import java.util.BitSet;
import java.util.Properties;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.ClassName;
import org.apache.derby.iapi.services.classfile.VMOpcode;
import org.apache.derby.iapi.services.compiler.MethodBuilder;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derby.iapi.sql.compile.CostEstimate;
import org.apache.derby.iapi.sql.compile.Optimizable;
import org.apache.derby.iapi.sql.compile.OptimizablePredicateList;
import org.apache.derby.iapi.sql.compile.Optimizer;
import org.apache.derby.iapi.sql.compile.RowOrdering;
import org.apache.derby.iapi.sql.dictionary.ConglomerateDescriptor;
import org.apache.derby.iapi.types.TypeId;
import org.apache.derby.iapi.util.JBitSet;
import org.apache.derby.iapi.util.ReuseFactory;

/**
 * A IntersectOrExceptNode represents an INTERSECT or EXCEPT DML statement.
 *
 */

public class IntersectOrExceptNode extends SetOperatorNode
{
    /* Currently we implement INTERSECT and EXCEPT by rewriting
     *   t1 (INTERSECT|EXCEPT) [ALL] t2
     * as (roughly)
     *   setOpResultSet( opType, all, (select * from t1 order by 1,2,...n), (select * from t2 ORDER BY 1,2,...,n))
     * where n is the number of columns in t1 (which must be the same as the number of columns in t2),
     * and opType is INTERSECT, or EXCEPT.
     *
     * The setOpResultSet result set simultaneously scans through its two ordered inputs and
     * performs the intersect or except.
     *
     * There are other query plans that may be more efficient, depending on the sizes. One plan is
     * to make a hash table from one of the input tables and then look up each row of the other input
     * table in the hash table.  However, we have not yet implemented spilling to disk in the
     * BackingStoreHashtable class: currently the whole hash table is in RAM. If we were to use it
     * we would blow up on large input tables.
     */

    private int opType;
    public static final int INTERSECT_OP = 1;
    public static final int EXCEPT_OP = 2;

	/* Only optimize it once */
	/* Only call addNewNodes() once */
	private boolean addNewNodesCalled;

    private int[] intermediateOrderByColumns; // The input result sets will be ordered on these columns. 0 indexed
    private int[] intermediateOrderByDirection; // ascending = 1, descending = -1
    private boolean[] intermediateOrderByNullsLow; // TRUE means NULL values should be ordered lower than non-NULL values

	/**
     * Constructor for a SetOperatorNode.
	 *
     * @param opType            The operator type: one of {@link #EXCEPT_OP} or
     *                          {@link #INTERSECT_OP}.
	 * @param leftResult		The ResultSetNode on the left side of this union
	 * @param rightResult		The ResultSetNode on the right side of this union
     * @param all               {@code true} if this is an ALL set operation.
	 * @param tableProperties	Properties list associated with the table
     * @param cm                The context manager
	 * @exception StandardException		Thrown on error
	 */
    IntersectOrExceptNode(int            opType,
                          ResultSetNode  leftResult,
                          ResultSetNode  rightResult,
                          boolean        all,
                          Properties     tableProperties,
                          ContextManager cm) throws StandardException {

        super(leftResult, rightResult, all, tableProperties, cm);
        this.opType = opType;
    }

    private int getOpType()
    {
        return opType;
    }
    
    /**
     * Push order by lists down to the children so that we can implement the intersect/except
     * by scan of the two sorted inputs.
	 *
	 * @param numTables			Number of tables in the DML Statement
	 * @param gbl				The group by list, if any
	 * @param fromList			The from list, if any
	 *
	 * @return The preprocessed ResultSetNode that can be optimized
	 *
	 * @exception StandardException		Thrown on error
	 */

    @Override
    ResultSetNode preprocess(int numTables,
									GroupByList gbl,
									FromList fromList)
								throws StandardException
	{
        // RESOLVE: We are in a quandary as to when and how we should generate order by lists. SelectNode processing
        // requires order by lists at the start of preprocess. That is why we are doing it here. However we can
        // pick any column ordering. Depending on the child expressions the optimizer may be able to avoid a
        // sort if we pick the right column ordering. For instance if one of the child expressions is
        // "select <key columns>, <other expressions> from T" where there is a unique index on <key columns>
        // then we can just generate an order by on the key columns and the optimizer should use the unique index
        // to produce the sorted result set. However the ResultSetNode class does not make it easy to
        // find the structure of the query expression. Furthermore we most want to avoid a sort on the larger
        // input, but the size estimate is not available at preprocess time.

        intermediateOrderByColumns = new int[ getResultColumns().size()];
        intermediateOrderByDirection = new int[ intermediateOrderByColumns.length];
        intermediateOrderByNullsLow = new boolean[ intermediateOrderByColumns.length];

        /* If there is an order by on the result of the intersect then use
         * that because we know that doing so will avoid a sort.  If the
         * output of the intersect/except is small relative to its inputs then
         * in some cases it would be better to sort the inputs on a different
         * sequence of columns, but it is hard to analyze the input query
         * expressions to see if a sort can be avoided.
         */
        final OrderByList obl = qec.getOrderByList(0);

        if( obl != null)
        {
            BitSet colsOrdered = new BitSet( intermediateOrderByColumns.length);
            int orderByListSize = obl.size();
            int intermediateOrderByIdx = 0;
            for( int i = 0; i < orderByListSize; i++)
            {
                if( colsOrdered.get(i))
                    continue;
                OrderByColumn orderByColumn =
                    obl.getOrderByColumn(i);
                intermediateOrderByDirection[intermediateOrderByIdx] = orderByColumn.isAscending() ? 1 : -1;
                intermediateOrderByNullsLow[intermediateOrderByIdx] = orderByColumn.isNullsOrderedLow();
                int columnIdx = orderByColumn.getResultColumn().getColumnPosition() - 1;
                intermediateOrderByColumns[intermediateOrderByIdx] = columnIdx;
                colsOrdered.set( columnIdx);
                intermediateOrderByIdx++;
            }
            for( int i = 0; i < intermediateOrderByColumns.length; i++)
            {
                if( ! colsOrdered.get(i))
                {
                    intermediateOrderByDirection[intermediateOrderByIdx] = 1;
                    intermediateOrderByNullsLow[intermediateOrderByIdx] = false;
                    intermediateOrderByColumns[intermediateOrderByIdx] = i;
                    intermediateOrderByIdx++;
                }
            }
            qec.setOrderByList(0, null); // It will be pushed down.
        }
        else // The output of the intersect/except does not have to be ordered
        {
            // Pick an intermediate ordering that minimizes the cost.
            // RESOLVE: how do you do that?
            for( int i = 0; i < intermediateOrderByColumns.length; i++)
            {
                intermediateOrderByDirection[i] = 1;
                intermediateOrderByNullsLow[i] = false;
                intermediateOrderByColumns[i] = i;
            }
        }
        pushOrderingDown( leftResultSet);
        pushOrderingDown( rightResultSet);

        return super.preprocess( numTables, gbl, fromList);
    } // end of preprocess

    private void pushOrderingDown( ResultSetNode rsn)
        throws StandardException
    {
        ContextManager cm = getContextManager();
        OrderByList orderByList = new OrderByList(null, cm);

        for( int i = 0; i < intermediateOrderByColumns.length; i++)
        {
            OrderByColumn orderByColumn =
                new OrderByColumn(
                    new NumericConstantNode(
                        TypeId.getBuiltInTypeId(Types.INTEGER),
                        ReuseFactory.getInteger(
                            intermediateOrderByColumns[i] + 1),
                        cm),
                    cm);

            if( intermediateOrderByDirection[i] < 0) {
                orderByColumn.setDescending();
            }

            if( intermediateOrderByNullsLow[i]) {
                orderByColumn.setNullsOrderedLow();
            }

            orderByList.addOrderByColumn( orderByColumn);
        }
        orderByList.bindOrderByColumns( rsn);
        rsn.pushQueryExpressionSuffix();
        rsn.pushOrderByList( orderByList);
    } // end of pushOrderingDown
                                                            
    /**
     * @see org.apache.derby.iapi.sql.compile.Optimizable#estimateCost
     */
    @Override
    public CostEstimate estimateCost( OptimizablePredicateList predList,
                                      ConglomerateDescriptor cd,
                                      CostEstimate outerCost,
                                      Optimizer optimizer,
                                      RowOrdering rowOrdering)
                          throws StandardException
    {
		leftResultSet = optimizeSource(
							optimizer,
							leftResultSet,
							(PredicateList) null,
							outerCost);

		rightResultSet = optimizeSource(
							optimizer,
							rightResultSet,
							(PredicateList) null,
							outerCost);

        CostEstimate costEst = getCostEstimate(optimizer);
        CostEstimate leftCostEstimate = leftResultSet.getCostEstimate();
        CostEstimate rightCostEstimate = rightResultSet.getCostEstimate();

        // The cost is the sum of the two child costs plus the cost of
        // sorting the union.
        costEst.setCost(
            leftCostEstimate.getEstimatedCost() +
                rightCostEstimate.getEstimatedCost(),
            getRowCountEstimate(
                leftCostEstimate.rowCount(),
                rightCostEstimate.rowCount()),
            getSingleScanRowCountEstimate(
                leftCostEstimate.singleScanRowCount(),
                rightCostEstimate.singleScanRowCount()));

        return costEst;
    } // End of estimateCost

	/**
	 * @see Optimizable#modifyAccessPath
	 *
	 * @exception StandardException		Thrown on error
	 */
    @Override
	public Optimizable modifyAccessPath(JBitSet outerTables) throws StandardException
	{
		Optimizable retOptimizable;
		retOptimizable = super.modifyAccessPath(outerTables);

		/* We only want call addNewNodes() once */
		if (addNewNodesCalled)
		{
			return retOptimizable;
		}
		return (Optimizable) addNewNodes();
	}

	/**
	 * @see ResultSetNode#modifyAccessPaths
	 *
	 * @exception StandardException		Thrown on error
	 */
    @Override
    ResultSetNode modifyAccessPaths() throws StandardException
	{
		ResultSetNode retRSN;
		retRSN = super.modifyAccessPaths();

		/* We only want call addNewNodes() once */
		if (addNewNodesCalled)
		{
			return retRSN;
		}
		return addNewNodes();
	}

	/**
	 * Add any new ResultSetNodes that are necessary to the tree.
	 * We wait until after optimization to do this in order to
	 * make it easier on the optimizer.
	 *
	 * @return (Potentially new) head of the ResultSetNode tree.
	 *
	 * @exception StandardException		Thrown on error
	 */
	private ResultSetNode addNewNodes()
		throws StandardException
	{
		/* Only call addNewNodes() once */
		if (addNewNodesCalled)
		{
			return this;
		}

		addNewNodesCalled = true;

        ResultSetNode treeTop = this;

        for (int i = 0; i < qec.size(); i++) {
            final OrderByList obl = qec.getOrderByList(i);

            if(obl != null) {
                // Generate an order by node on top of the intersect/except
                treeTop = new OrderByNode(
                        treeTop,
                        obl,
                        tableProperties,
                        getContextManager());
            }

            final ValueNode offset = qec.getOffset(i);
            final ValueNode fetchFirst = qec.getFetchFirst(i);

            if (offset != null || fetchFirst != null) {
                ResultColumnList newRcl =
                    treeTop.getResultColumns().copyListAndObjects();
                newRcl.genVirtualColumnNodes(
                    treeTop, treeTop.getResultColumns());

                treeTop = new RowCountNode(
                        treeTop,
                        newRcl,
                        offset,
                        fetchFirst,
                        qec.getHasJDBCLimitClause()[i].booleanValue(),
                        getContextManager());
            }
        }

        return treeTop;

    } // end of addNewNodes

    /**
	 * Generate the code.
	 *
	 * @exception StandardException		Thrown on error
     */
    @Override
    void generate( ActivationClassBuilder acb, MethodBuilder mb)
        throws StandardException
	{

		/* Get the next ResultSet #, so that we can number this ResultSetNode, its
		 * ResultColumnList and ResultSet.
		 */
		assignResultSetNumber();

		// Get our final cost estimate based on the child estimates.
		setCostEstimate( getFinalCostEstimate() );

		// build up the tree.

        /* Generate the SetOpResultSet. Arguments:
         *  1) expression for left child ResultSet
         *  2) expression for right child ResultSet
         *  3) activation
         *  4) resultSetNumber
         *  5) estimated row count
         *  6) estimated cost
         *  7) opType
         *  8) all
         *  9) intermediateOrderByColumns saved object index
         *  10) intermediateOrderByDirection saved object index
         *  11) intermediateOrderByNullsLow saved object index
         */

		acb.pushGetResultSetFactoryExpression(mb); // instance for getSetOpResultSet

		getLeftResultSet().generate( acb, mb);
		getRightResultSet().generate( acb, mb);

		acb.pushThisAsActivation(mb);
		mb.push(getResultSetNumber());
        mb.push( getCostEstimate().getEstimatedRowCount());
        mb.push( getCostEstimate().getEstimatedCost());
        mb.push( getOpType());
        mb.push( all);
        mb.push( getCompilerContext().addSavedObject( intermediateOrderByColumns));
        mb.push( getCompilerContext().addSavedObject( intermediateOrderByDirection));
        mb.push( getCompilerContext().addSavedObject( intermediateOrderByNullsLow));

		mb.callMethod(VMOpcode.INVOKEINTERFACE,
                      (String) null,
                      "getSetOpResultSet",
                      ClassName.NoPutResultSet, 11);
	} // end of generate

	/**
	 * @see ResultSetNode#getFinalCostEstimate
	 *
	 * Get the final CostEstimate for this IntersectOrExceptNode.
	 *
	 * @return	The final CostEstimate for this IntersectOrExceptNode,
	 *  which is the sum of the two child costs.  The final number of
	 *  rows depends on whether this is an INTERSECT or EXCEPT (see
	 *  getRowCountEstimate() in this class for more).
	 */
    @Override
    CostEstimate getFinalCostEstimate()
		throws StandardException
	{
		if (getCandidateFinalCostEstimate() != null)
        {
			return getCandidateFinalCostEstimate();
        }

		CostEstimate leftCE = leftResultSet.getFinalCostEstimate();
		CostEstimate rightCE = rightResultSet.getFinalCostEstimate();

		setCandidateFinalCostEstimate( getNewCostEstimate() );
		getCandidateFinalCostEstimate().setCost(
			leftCE.getEstimatedCost() + rightCE.getEstimatedCost(),
			getRowCountEstimate(leftCE.rowCount(), rightCE.rowCount()),
			getSingleScanRowCountEstimate(leftCE.singleScanRowCount(),
				rightCE.singleScanRowCount()));

		return getCandidateFinalCostEstimate();
	}

    String getOperatorName()
    {
        switch( opType)
        {
        case INTERSECT_OP:
            return "INTERSECT";

        case EXCEPT_OP:
            return "EXCEPT";
        }
        if( SanityManager.DEBUG)
            SanityManager.THROWASSERT( "Invalid intersectOrExcept opType: " + opType);
        return "?";
    }
    
    double getRowCountEstimate( double leftRowCount, double rightRowCount)
    {
        switch( opType)
        {
        case INTERSECT_OP:
            // The result has at most min( leftRowCount, rightRowCount). Estimate the actual row count at
            // half that.
            return Math.min( leftRowCount, rightRowCount)/2;

        case EXCEPT_OP:
            // The result has at most leftRowCount rows and at least
            // max(0, leftRowCount - rightRowCount) rows.  Use the mean
            // of those two as the estimate.
            return (leftRowCount + Math.max(0, leftRowCount - rightRowCount))/2;
        }
        if( SanityManager.DEBUG)
            SanityManager.THROWASSERT( "Invalid intersectOrExcept opType: " + opType);
        return 1.0;
    } // end of getRowCountEstimate
    
    double getSingleScanRowCountEstimate( double leftSingleScanRowCount, double rightSingleScanRowCount)
    {
        return getRowCountEstimate( leftSingleScanRowCount, rightSingleScanRowCount);
    }
}
