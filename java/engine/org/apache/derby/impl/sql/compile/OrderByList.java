/*

   Derby - Class org.apache.derby.impl.sql.compile.OrderByList

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

package	org.apache.derby.impl.sql.compile;

import org.apache.derby.iapi.sql.compile.CompilerContext;
import org.apache.derby.iapi.sql.compile.CostEstimate;
import org.apache.derby.iapi.sql.compile.RequiredRowOrdering;
import org.apache.derby.iapi.sql.compile.RowOrdering;
import org.apache.derby.iapi.sql.compile.C_NodeTypes;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.impl.sql.compile.ActivationClassBuilder;

import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.ResultSet;

import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;

import org.apache.derby.iapi.services.compiler.MethodBuilder;

import org.apache.derby.iapi.services.loader.GeneratedMethod;

import org.apache.derby.iapi.store.access.ColumnOrdering;
import org.apache.derby.iapi.store.access.SortCostController;
import org.apache.derby.iapi.store.access.TransactionController;

import org.apache.derby.iapi.types.DataValueDescriptor;

import org.apache.derby.iapi.reference.ClassName;
import org.apache.derby.iapi.reference.DB2Limit;
import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.iapi.util.JBitSet;
import org.apache.derby.iapi.services.classfile.VMOpcode;

import java.util.Properties;

/**
 * An OrderByList is an ordered list of columns in the ORDER BY clause.
 * That is, the order of columns in this list is significant - the
 * first column in the list is the most significant in the ordering,
 * and the last column in the list is the least significant.
 *
	@author ames
 */
public class OrderByList extends OrderedColumnList
						implements RequiredRowOrdering {

	private boolean allAscending = true;
	private boolean alwaysSort;
	private ResultSetNode resultToSort;
	private SortCostController scc;
	private Object[] resultRow;
	private ColumnOrdering[] columnOrdering;
	private int estimatedRowSize;
	private boolean sortNeeded = true;

	/**
		Add a column to the list
	
		@param column	The column to add to the list
	 */
	public void addOrderByColumn(OrderByColumn column) 
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
	public OrderByColumn getOrderByColumn(int position) {
		if (SanityManager.DEBUG)
		SanityManager.ASSERT(position >=0 && position < size());
		return (OrderByColumn) elementAt(position);
	}

	/**
		Print the list.
	
		@param depth		The depth at which to indent the sub-nodes
	 */
	public void printSubNodes(int depth) {

		if (SanityManager.DEBUG) 
		{
			for (int index = 0; index < size(); index++)
			{
				( (OrderByColumn) (elementAt(index)) ).treePrint(depth);
			}
		}
	}

	/**
		Bind the update columns by their names to the target resultset
		of the cursor specification.

		@param target	The underlying result set
	
		@exception StandardException		Thrown on error
	 */
	public void bindOrderByColumns(ResultSetNode target)
					throws StandardException {

		/* Remember the target for use in optimization */
		resultToSort = target;

		int size = size();

		/* Only 1012 columns allowed in ORDER BY clause */
		if (size > DB2Limit.DB2_MAX_ELEMENTS_IN_ORDER_BY)
		{
			throw StandardException.newException(SQLState.LANG_TOO_MANY_ELEMENTS);
		}

		for (int index = 0; index < size; index++)
		{
			OrderByColumn obc = (OrderByColumn) elementAt(index);
			obc.bindOrderByColumn(target);

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
		Pull up Order By columns by their names to the target resultset
		of the cursor specification.

		@param target	The underlying result set
	
	 */
	public void pullUpOrderByColumns(ResultSetNode target)
					throws StandardException {

		/* Remember the target for use in optimization */
		resultToSort = target;

		int size = size();
		for (int index = 0; index < size; index++)
		{
			OrderByColumn obc = (OrderByColumn) elementAt(index);
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
		boolean inOrderPrefix = true;
		int rclSize = sourceRCL.size();

		if (SanityManager.DEBUG)
		{
			if (size() > sourceRCL.size())
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
			if (((OrderByColumn) elementAt(index)).getResultColumn() !=
				(ResultColumn) sourceRCL.elementAt(index))
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
	 *
	 * @return Nothing.
	 */
	void resetToSourceRCs()
	{
		int size = size();
		for (int index = 0; index < size; index++)
		{
			OrderByColumn obc = (OrderByColumn) elementAt(index);
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
		ResultColumnList newRCL = (ResultColumnList) getNodeFactory().getNode(
												C_NodeTypes.RESULT_COLUMN_LIST,
												getContextManager());

		/* The new RCL starts with the ordering columns */
		int size = size();
		for (int index = 0; index < size; index++)
		{
			OrderByColumn obc = (OrderByColumn) elementAt(index);
			newRCL.addElement(obc.getResultColumn());
			resultColumns.removeElement(obc.getResultColumn());
		}

		/* And ends with the non-ordering columns */
		newRCL.destructiveAppend(resultColumns);
		newRCL.resetVirtualColumnIds();
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
			OrderByColumn obc = (OrderByColumn) elementAt(loc);

			if (obc.constantColumn(whereClause))
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
			OrderByColumn obc = (OrderByColumn) elementAt(loc);
			int           colPosition = obc.getColumnPosition();

			for (int inner = 0; inner < loc; inner++)
			{
				OrderByColumn prev_obc = (OrderByColumn) elementAt(inner);
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
	public void generate(ActivationClassBuilder acb, 
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
		 *  arg5: Activation
		 *  arg6: rowAllocator - method to construct rows for fetching
		 *			from the sort
		 *  arg7: row size
		 *  arg8: resultSetNumber
		 *  arg9: estimated row count
		 *  arg10: estimated cost
		 *  arg11: closeCleanup
		 */

		acb.pushGetResultSetFactoryExpression(mb);

		child.generate(acb, mb);

		int resultSetNumber = cc.getNextResultSetNumber();

		// is a distinct query
		mb.push(false);

		// not in sorted order
		mb.push(false);

		mb.push(orderItem);

		acb.pushThisAsActivation(mb);

		// row allocator
		child.getResultColumns().generateHolder(acb, mb);

		mb.push(child.getResultColumns().getTotalColumnSize());

		mb.push(resultSetNumber);

		// Get the cost estimate for the child
		// RESOLVE - we will eventually include the cost of the sort
		CostEstimate costEstimate = child.getCostEstimate(); 

		mb.push(costEstimate.rowCount());
		mb.push(costEstimate.getEstimatedCost());

		/**
			if this is the statement result set (today, it always is), 
			and there is a current
			date/time request, then a method will have been generated.
			Otherwise, a simple null is passed in to the result set method.
		 */
		acb.pushResultSetClosedMethodFieldAccess(mb);

		mb.callMethod(VMOpcode.INVOKEINTERFACE, (String) null, "getSortResultSet",
							ClassName.NoPutResultSet, 11);

	}

	/* RequiredRowOrdering interface */

	/**
	 * @see RequiredRowOrdering#sortRequired
	 *
	 * @exception StandardException		Thrown on error
	 */
	public int sortRequired(RowOrdering rowOrdering) throws StandardException
	{
		return sortRequired(rowOrdering, (JBitSet) null);
	}

	/**
	 * @see RequiredRowOrdering#sortRequired
	 *
	 * @exception StandardException		Thrown on error
	 */
	public int sortRequired(RowOrdering rowOrdering, JBitSet tableMap)
				throws StandardException
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
	 * @return Nothing.
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
}
