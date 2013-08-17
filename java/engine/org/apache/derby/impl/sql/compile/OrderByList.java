/*

   Derby - Class org.apache.derby.impl.sql.compile.OrderByList

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

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.ClassName;
import org.apache.derby.iapi.reference.Limits;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.services.classfile.VMOpcode;
import org.apache.derby.iapi.services.compiler.MethodBuilder;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derby.iapi.sql.compile.CompilerContext;
import org.apache.derby.iapi.sql.compile.CostEstimate;
import org.apache.derby.iapi.sql.compile.Optimizable;
import org.apache.derby.iapi.sql.compile.OptimizableList;
import org.apache.derby.iapi.sql.compile.RequiredRowOrdering;
import org.apache.derby.iapi.sql.compile.RowOrdering;
import org.apache.derby.iapi.store.access.ColumnOrdering;
import org.apache.derby.iapi.store.access.SortCostController;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.util.JBitSet;

/**
 * An OrderByList is an ordered list of columns in the ORDER BY clause.
 * That is, the order of columns in this list is significant - the
 * first column in the list is the most significant in the ordering,
 * and the last column in the list is the least significant.
 *
 */
class OrderByList extends OrderedColumnList<OrderByColumn>
						implements RequiredRowOrdering {

	private boolean allAscending = true;
	private boolean alwaysSort;
	private ResultSetNode resultToSort;
	private SortCostController scc;
	private Object[] resultRow;
	private ColumnOrdering[] columnOrdering;
	private int estimatedRowSize;
	private boolean sortNeeded = true;
	private int resultSetNumber = -1;

    /**
     * {@code true} if this instance orders a
     * {@literal <table value constructor>}.
     * See {@link #isTableValueCtorOrdering}.
     */
    private boolean isTableValueCtorOrdering;

    /**
     * Constructor.
     * Initialize with the type of the result set this {@code OrderByList} is
     * attached to, e.g. {@code SELECT}, {@code VALUES} or a set operation.
     * @param rs The result set this {@code OrderByList} is ordering. May be
     *           null
     * @param cm The context manager
    */
   OrderByList(ResultSetNode rs, ContextManager cm) {
       super(OrderByColumn.class, cm);
       this.isTableValueCtorOrdering =
                (rs instanceof UnionNode &&
                ((UnionNode)rs).tableConstructor()) ||
                rs instanceof RowResultSetNode;
    }

	/**
		Add a column to the list
	
		@param column	The column to add to the list
	 */
    void addOrderByColumn(OrderByColumn column)
	{
		addElement(column);

		if (! column.isAscending())
			allAscending = false;
	}

	/**
	 * Are all columns in the list ascending.
	 *
	 * @return	Whether or not all columns in the list ascending.
	 */
	boolean allAscending()
	{
		return allAscending;
	}

	/**
		Get a column from the list
	
		@param position	The column to get from the list
	 */
    OrderByColumn getOrderByColumn(int position) {
        if (SanityManager.DEBUG) {
            SanityManager.ASSERT(position >=0 && position < size());
        }

        return elementAt(position);
	}

	/**
	 *	Bind the update columns by their names to the target resultset of the
     * cursor specification.
	 *
	 * 	@param target	The underlying result set
	 *	@exception StandardException		Thrown on error
	 */
    void bindOrderByColumns(ResultSetNode target)
					throws StandardException {

		/* Remember the target for use in optimization */
		resultToSort = target;

		/* Only 1012 columns allowed in ORDER BY clause */
        if (size() > Limits.DB2_MAX_ELEMENTS_IN_ORDER_BY)
		{
			throw StandardException.newException(SQLState.LANG_TOO_MANY_ELEMENTS);
		}

        for (OrderByColumn obc : this)
		{
			obc.bindOrderByColumn(target, this);

			/*
			** Always sort if we are ordering on an expression, and not
			** just a column.
			*/
			if ( !
			 (obc.getResultColumn().getExpression() instanceof ColumnReference))
			{
				alwaysSort = true;
			}
		}
	}

	/**
	 * Adjust addedColumnOffset values due to removal of a duplicate column
	 *
	 * This routine is called by bind processing when it identifies and
	 * removes a column from the result column list which was pulled up due
	 * to its presence in the ORDER BY clause, but which was later found to
	 * be a duplicate. The OrderByColumn instance for the removed column
	 * has been adjusted to point to the true column in the result column
	 * list and its addedColumnOffset has been reset to -1. This routine
	 * finds any other OrderByColumn instances which had an offset greater
	 * than that of the column that has been deleted, and decrements their
	 * addedColumOffset to account for the deleted column's removal.
	 *
	 * @param gap   column which has been removed from the result column list
	 */
	void closeGap(int gap)
	{
        for (OrderByColumn obc : this)
		{
			obc.collapseAddedColumnGap(gap);
		}
	}

	/**
		Pull up Order By columns by their names to the target resultset
		of the cursor specification.

		@param target	The underlying result set
	
	 */
    void pullUpOrderByColumns(ResultSetNode target)
					throws StandardException {

		/* Remember the target for use in optimization */
		resultToSort = target;

        for (OrderByColumn obc : this)
		{
			obc.pullUpOrderByColumn(target);
		}

	}

	/**
	 * Is this order by list an in order prefix of the specified RCL.
	 * This is useful when deciding if an order by list can be eliminated
	 * due to a sort from an underlying distinct or union.
	 *
	 * @param sourceRCL	The source RCL.
	 *
	 * @return Whether or not this order by list an in order prefix of the specified RCL.
	 */
	boolean isInOrderPrefix(ResultColumnList sourceRCL)
	{
		if (SanityManager.DEBUG)
		{
            int rclSize = sourceRCL.size();
            if (size() > rclSize)
			{
				SanityManager.THROWASSERT(
					"size() (" + size() + 
					") expected to be <= sourceRCL.size() (" +
					sourceRCL.size() + ")");
			}
		}

		int size = size();
		for (int index = 0; index < size; index++)
		{
            if (elementAt(index).getResultColumn() !=
                sourceRCL.elementAt(index))
			{
				return false;
			}
		}
		return true;
	}

	/**
	 * Order by columns now point to the PRN above the node of interest.
	 * We need them to point to the RCL under that one.  This is useful
	 * when combining sorts where we need to reorder the sorting
	 * columns.
	 */
	void resetToSourceRCs()
	{
        for (OrderByColumn obc : this)
		{
			obc.resetToSourceRC();
		}
	}

	/**
	 * Build a new RCL with the same RCs as the passed in RCL
	 * but in an order that matches the ordering columns.
	 *
	 * @param resultColumns	The RCL to reorder.
	 *	
	 *	@exception StandardException		Thrown on error
	 */
	ResultColumnList reorderRCL(ResultColumnList resultColumns)
		throws StandardException
	{
        ResultColumnList newRCL = new ResultColumnList(getContextManager());

		/* The new RCL starts with the ordering columns */
        for (OrderByColumn obc : this)
		{
			newRCL.addElement(obc.getResultColumn());
			resultColumns.removeElement(obc.getResultColumn());
		}

		/* And ends with the non-ordering columns */
		newRCL.destructiveAppend(resultColumns);
		newRCL.resetVirtualColumnIds();
		newRCL.copyOrderBySelect(resultColumns);
		return newRCL;
	}

	/**
		Remove any constant columns from this order by list.
		Constant columns are ones where all of the column references
		are equal to constant expressions according to the given
		predicate list.
	 */
	void removeConstantColumns(PredicateList whereClause)
	{
		/* Walk the list backwards so we can remove elements safely */
		for (int loc = size() - 1;
			 loc >= 0;
			 loc--)
		{
            if (elementAt(loc).constantColumn(whereClause))
			{
				removeElementAt(loc);
			}
		}
	}

	/**
		Remove any duplicate columns from this order by list.
		For example, one may "ORDER BY 1, 1, 2" can be reduced
		to "ORDER BY 1, 2".
		Beetle 5401.
	 */
	void removeDupColumns()
	{
		/* Walk the list backwards so we can remove elements safely */
		for (int loc = size() - 1; loc > 0; loc--)
		{
            OrderByColumn obc = elementAt(loc);
			int           colPosition = obc.getColumnPosition();

			for (int inner = 0; inner < loc; inner++)
			{
                OrderByColumn prev_obc = elementAt(inner);
				if (colPosition == prev_obc.getColumnPosition())
				{
					removeElementAt(loc);
					break;
				}
			}
		}
	}

	/**
    	generate the sort result set operating over the source
		expression.

		@param acb the tool for building the class
		@param mb	the method the generated code is to go into
		@exception StandardException thrown on failure
	 */
    void generate(ActivationClassBuilder acb,
								MethodBuilder mb,
								ResultSetNode child)
							throws StandardException 
	{
		/*
		** If sorting is not required, don't generate a sort result set -
		** just return the child result set.
		*/
		if ( ! sortNeeded) {
			child.generate(acb, mb);
			return;
		}

		/* Get the next ResultSet#, so we can number this ResultSetNode, its
		 * ResultColumnList and ResultSet.
		 *
		 * REMIND: to do this properly (if order bys can live throughout
		 * the tree) there ought to be an OrderByNode that holds its own
		 * ResultColumnList that is a lsit of virtual column nodes pointing
		 * to the source's result columns.  But since we know it is outermost,
		 * we just gloss over that and get ourselves a resultSetNumber
		 * directly.
		 */
		CompilerContext cc = getCompilerContext();


		/*
			create the orderItem and stuff it in.
		 */
		int orderItem = acb.addItem(acb.getColumnOrdering(this));


		/* Generate the SortResultSet:
		 *	arg1: childExpress - Expression for childResultSet
		 *  arg2: distinct - always false, we have a separate node
		 *				for distincts
		 *  arg3: isInSortedOrder - is the source result set in sorted order
		 *  arg4: orderItem - entry in saved objects for the ordering
		 *  arg5: rowAllocator - method to construct rows for fetching
		 *			from the sort
		 *  arg6: row size
		 *  arg7: resultSetNumber
		 *  arg8: estimated row count
		 *  arg9: estimated cost
		 */

		acb.pushGetResultSetFactoryExpression(mb);

		child.generate(acb, mb);

		resultSetNumber = cc.getNextResultSetNumber();

		// is a distinct query
		mb.push(false);

		// not in sorted order
		mb.push(false);

		mb.push(orderItem);

		// row allocator
        mb.push(acb.addItem(child.getResultColumns().buildRowTemplate()));

		mb.push(child.getResultColumns().getTotalColumnSize());

		mb.push(resultSetNumber);

		// Get the cost estimate for the child
		// RESOLVE - we will eventually include the cost of the sort
		CostEstimate costEstimate = child.getFinalCostEstimate(); 

		mb.push(costEstimate.rowCount());
		mb.push(costEstimate.getEstimatedCost());

		mb.callMethod(VMOpcode.INVOKEINTERFACE, (String) null, "getSortResultSet",
							ClassName.NoPutResultSet, 9);

	}

	/* RequiredRowOrdering interface */

    /**
     * @see RequiredRowOrdering#sortRequired(RowOrdering, OptimizableList, int[])
     *
     * @exception StandardException     Thrown on error
     */
    public int sortRequired(
        RowOrdering rowOrdering,
        OptimizableList optimizableList,
        int[] proposedJoinOrder) throws StandardException
    {
        return sortRequired(rowOrdering,
                            (JBitSet)null,
                            optimizableList,
                            proposedJoinOrder);
    }

    /**
     * @see RequiredRowOrdering#sortRequired(RowOrdering, JBitSet, OptimizableList, int[])
     *
     * @exception StandardException     Thrown on error
     */
    public int sortRequired(
        RowOrdering rowOrdering,
        JBitSet tableMap,
        OptimizableList optimizableList,
        int[] proposedJoinOrder) throws StandardException
    {
		/*
		** Currently, all indexes are ordered ascending, so a descending
		** ORDER BY always requires a sort.
		*/
		if (alwaysSort)
		{
			return RequiredRowOrdering.SORT_REQUIRED;
		}

		/*
		** Step through the columns in this list, and ask the
		** row ordering whether it is ordered on each column.
		*/
		int position = 0;
		int size = size();
		for (int loc = 0; loc < size; loc++)
		{
			OrderByColumn obc = getOrderByColumn(loc);

            // If the user specified NULLS FIRST or NULLS LAST in such a way
            // as to require NULL values to be re-sorted to be lower than
            // non-NULL values, then a sort is required, as the index holds
            // NULL values unconditionally higher than non-NULL values
            //
            if (obc.isNullsOrderedLow())
				return RequiredRowOrdering.SORT_REQUIRED;

			// ResultColumn rc = obc.getResultColumn();

			/*
			** This presumes that the OrderByColumn refers directly to
			** the base column, i.e. there is no intervening VirtualColumnNode.
			*/
			// ValueNode expr = obc.getNonRedundantExpression();
			ValueNode expr = obc.getResultColumn().getExpression();

			if ( ! (expr instanceof ColumnReference))
			{
				return RequiredRowOrdering.SORT_REQUIRED;
			}

			ColumnReference cr = (ColumnReference) expr;

			/*
			** Check whether the table referred to is in the table map (if any).
			** If it isn't, we may have an ordering that does not require
			** sorting for the tables in a partial join order.  Look for
			** columns beyond this column to see whether a referenced table
			** is found - if so, sorting is required (for example, in a
			** case like ORDER BY S.A, T.B, S.C, sorting is required).
			*/
			if (tableMap != null)
			{
				if ( ! tableMap.get(cr.getTableNumber()))
				{
					/* Table not in partial join order */
					for (int remainingPosition = loc + 1;
						 remainingPosition < size();
						 remainingPosition++)
					{
						OrderByColumn remainingobc = getOrderByColumn(loc);

						ResultColumn remainingrc =
												remainingobc.getResultColumn();

						ValueNode remainingexpr = remainingrc.getExpression();

						if (remainingexpr instanceof ColumnReference)
						{
							ColumnReference remainingcr =
											(ColumnReference) remainingexpr;
							if (tableMap.get(remainingcr.getTableNumber()))
							{
								return RequiredRowOrdering.SORT_REQUIRED;
							}
						}
					}

					return RequiredRowOrdering.NOTHING_REQUIRED;
				}
			}
			/*
			 * Does this order by column belong to the outermost optimizable in
			 * the current join order?
			 * 
			 * If yes, then we do not need worry about the ordering of the rows
			 * feeding into it. Because the order by column is associated with 
			 * the outermost optimizable, optimizer will not have to deal with 
			 * the order of any rows coming in from the previous optimizables. 
			 * 
			 * But if the current order by column belongs to an inner 
			 * optimizable in the join order, then go through the following
			 * if condition logic.
			 */

			/* If the following boolean is true, then it means that the join 
			 * order being considered has more than one table 
			 */
			boolean moreThanOneTableInJoinOrder = tableMap!=null?
					(!tableMap.hasSingleBitSet()) : false;
			if (moreThanOneTableInJoinOrder) 
			{
				/*
				 * First check if the order by column has a constant comparison
				 * predicate on it or it belongs to an optimizable which is 
				 * always ordered(that means it is a single row table) or the 
				 * column is involved in an equijoin with an optimizable which 
				 * is always ordered on the column on which the equijoin is 
				 * happening. If yes, then we know that the rows will always be 
				 * sorted and hence we do not need to worry if (any) prior 
				 * optimizables in join order are one-row resultsets or not. 
				 */
				if ((!rowOrdering.alwaysOrdered(cr.getTableNumber())) &&
						(!rowOrdering.isColumnAlwaysOrdered(
								cr.getTableNumber(), cr.getColumnNumber())))
				{
					/*
					 * The current order by column is not always ordered which 
					 * means that the rows from it will not necessarily be in 
					 * the sorted order on that column. Because of this, we 
					 * need to make sure that the outer optimizables (outer to 
					 * the order by columns's optimizable) in the join order 
					 * are all one row optimizables, meaning that they can at 
					 * the most return only one row. If they return more than 
					 * one row, then it will require multiple scans of the 
					 * order by column's optimizable and the rows returned 
					 * from those multiple scans may not be ordered correctly.
					 */

                   for (int i=0;
                        i < proposedJoinOrder.length &&
                            proposedJoinOrder[i] != -1; // -1: partial order
                        i++)
					{
                       // Get one outer optimizable at a time from the join
                       // order
                        Optimizable considerOptimizable = optimizableList.
                                getOptimizable(proposedJoinOrder[i]);

                       // If we have come across the optimizable for the order
                       // by column in the join order, then we do not need to
                       // look at the inner optimizables in the join order. As
                       // long as the outer optimizables are one row
                       // resultset, or is ordered on the order by column (see
                       // below check), we are fine to consider sort
                       // avoidance.
						if (considerOptimizable.getTableNumber() == 
							cr.getTableNumber())
							break;
						/*
						 * The following if condition is checking if the
						 * outer optimizable to the order by column's 
						 * optimizable is one row resultset or not. 
						 * 
						 * If the outer optimizable is one row resultset, 
						 * then move on to the next optimizable in the join 
						 * order and do the same check on that optimizable. 
						 * Continue this  until we are done checking that all 
						 * the outer optimizables in the join order are single 
						 * row resultsets. If we run into an outer optimizable 
						 * which is not one row resultset, then we can not 
						 * consider sort avoidance for the query.
						 */
						if (rowOrdering.alwaysOrdered(
								considerOptimizable.getTableNumber()))
							continue;
						else
							//This outer optimizable can return more than 
							//one row. Because of this, we can't avoid the
							//sorting for this query.
							return RequiredRowOrdering.SORT_REQUIRED;
					}
				}
			}
			if ( ! rowOrdering.alwaysOrdered(cr.getTableNumber()))
			{
				/*
				** Check whether the ordering is ordered on this column in
				** this position.
				*/
				if ( ! rowOrdering.orderedOnColumn(
			  		obc.isAscending() ?
								RowOrdering.ASCENDING : RowOrdering.DESCENDING,
			  		position,
			  		cr.getTableNumber(),
			  		cr.getColumnNumber()
			  		))
				{
					return RequiredRowOrdering.SORT_REQUIRED;
				}

				/*
				** The position to ask about is for the columns in tables
				** that are *not* always ordered.  The always-ordered tables
				** are not counted as part of the list of ordered columns
				*/
				position++;
			}
		}

		return RequiredRowOrdering.NOTHING_REQUIRED;
	}

	/**
	 * @see RequiredRowOrdering#estimateCost
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void estimateCost(double estimatedInputRows,
								RowOrdering rowOrdering,
								CostEstimate resultCost)
					throws StandardException
	{
		/*
		** Do a bunch of set-up the first time: get the SortCostController,
		** the template row, the ColumnOrdering array, and the estimated
		** row size.
		*/
		if (scc == null)
		{
			scc = getCompilerContext().getSortCostController();

			resultRow =
				resultToSort.getResultColumns().buildEmptyRow().getRowArray();
			columnOrdering = getColumnOrdering();
			estimatedRowSize =
						resultToSort.getResultColumns().getTotalColumnSize();
		}

		long inputRows = (long) estimatedInputRows;
		long exportRows = inputRows;
		double sortCost;

		sortCost = scc.getSortCost(
									(DataValueDescriptor[]) resultRow,
									columnOrdering,
									false,
									inputRows,
									exportRows,
									estimatedRowSize
									);

		resultCost.setCost(sortCost, estimatedInputRows, estimatedInputRows);
	}

	/** @see RequiredRowOrdering#sortNeeded */
	public void sortNeeded()
	{
		sortNeeded = true;
	}

	/** @see RequiredRowOrdering#sortNotNeeded */
	public void sortNotNeeded()
	{
		sortNeeded = false;
	}

	/**
	 * Remap all ColumnReferences in this tree to be clones of the
	 * underlying expression.
	 *
	 * @exception StandardException		Thrown on error
	 */
	void remapColumnReferencesToExpressions() throws StandardException
	{
	}

	/**
	 * Get whether or not a sort is needed.
	 *
	 * @return Whether or not a sort is needed.
	 */
	public boolean getSortNeeded()
	{
		return sortNeeded;
	}

	/**
	 * Determine whether or not this RequiredRowOrdering has a
	 * DESCENDING requirement for the column referenced by the
	 * received ColumnReference.
	 */
	boolean requiresDescending(ColumnReference cRef, int numOptimizables)
		throws StandardException
	{
		int size = size();

		/* Start by getting the table number and column position for
		 * the table to which the ColumnReference points.
		 */
		JBitSet tNum = new JBitSet(numOptimizables);
		BaseTableNumbersVisitor btnVis = new BaseTableNumbersVisitor(tNum);

		cRef.accept(btnVis);
		int crTableNumber = tNum.getFirstSetBit();
		int crColPosition = btnVis.getColumnNumber();

		if (SanityManager.DEBUG)
		{
			/* We assume that we only ever get here if the column
			 * reference points to a specific column in a specific
			 * table...
			 */
			if ((crTableNumber < 0) || (crColPosition < 0))
			{
				SanityManager.THROWASSERT(
					"Failed to find table/column number for column '" +
					cRef.getColumnName() + "' when checking for an " +
					"ORDER BY requirement.");
			}

			/* Since we started with a single ColumnReference there
			 * should be exactly one table number.
			 */
			if (!tNum.hasSingleBitSet())
			{
				SanityManager.THROWASSERT(
					"Expected ColumnReference '" + cRef.getColumnName() +
					"' to reference exactly one table, but tables found " +
					"were: " + tNum);
			}
		}

		/* Walk through the various ORDER BY elements to see if
		 * any of them point to the same table and column that
		 * we found above.
		 */
		for (int loc = 0; loc < size; loc++)
		{
			OrderByColumn obc = getOrderByColumn(loc);
			ResultColumn rcOrderBy = obc.getResultColumn();

			btnVis.reset();
			rcOrderBy.accept(btnVis);
			int obTableNumber = tNum.getFirstSetBit();
			int obColPosition = btnVis.getColumnNumber();

			/* ORDER BY target should always have a table number and
			 * a column position.  It may not necessarily be a base
			 * table, but there should be some FromTable for which
			 * we have a ResultColumnList, and the ORDER BY should
			 * reference one of the columns in that list (otherwise
			 * we shouldn't have made it this far).
			 */
			if (SanityManager.DEBUG)
			{
				/* Since we started with a single ResultColumn there
				 * should exactly one table number.
				 */
				if (!tNum.hasSingleBitSet())
				{
					SanityManager.THROWASSERT("Expected ResultColumn '" +
						rcOrderBy.getColumnName() + "' to reference " +
						"exactly one table, but found: " + tNum);
				}

				if (obColPosition < 0)
				{
					SanityManager.THROWASSERT(
						"Failed to find orderBy column number " +
						"for ORDER BY check on column '" + 
						cRef.getColumnName() + "'.");
				}
			}

			if (crTableNumber != obTableNumber)
				continue;

			/* They point to the same base table, so check the
			 * column positions.
			 */

			if (crColPosition == obColPosition)
			{
				/* This ORDER BY element points to the same table
				 * and column as the received ColumnReference.  So
				 * return whether or not this ORDER BY element is
				 * descending.
				 */
				return !obc.isAscending();
			}
		}

		/* None of the ORDER BY elements referenced the same table
		 * and column as the received ColumnReference, so there
		 * is no descending requirement for the ColumnReference's
		 * source (at least not from this OrderByList).
		 */
		return false;
	}

    @Override
	public String toString() {

        StringBuilder buff = new StringBuilder();

		if (columnOrdering != null) {
			for (int i = 0; i < columnOrdering.length; i++) {
				buff.append("[" + i + "] " + columnOrdering[i] + "\n");
			}
		}

		return
			"allAscending: " + allAscending + "\n" +
			"alwaysSort:" + allAscending + "\n" +
			"sortNeeded: " + sortNeeded  + "\n" +
			"columnOrdering: " + "\n" +
			buff.toString() + "\n" +
			super.toString();
	}

	public int getResultSetNumber() {
		return resultSetNumber;
	}

    /**
     * @return {@code true} if the {@code ORDER BY} is attached to a
     * {@literal <table value constructor>}, i.e. a {@code VALUES} clause.
     */
    public boolean isTableValueCtorOrdering() {
        return isTableValueCtorOrdering;
    }
}
