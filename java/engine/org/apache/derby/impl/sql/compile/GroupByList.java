/*

   Derby - Class org.apache.derby.impl.sql.compile.GroupByList

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

import org.apache.derby.iapi.sql.compile.C_NodeTypes;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.sql.dictionary.DataDictionary;

import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.reference.DB2Limit;

import org.apache.derby.impl.sql.compile.ActivationClassBuilder;

import org.apache.derby.iapi.util.ReuseFactory;

import java.util.Vector;

/**
 * A GroupByList represents the list of expressions in a GROUP BY clause in
 * a SELECT statement.
 *
 * @author Jeff Lichtman
 */

public class GroupByList extends OrderedColumnList
{
	int		numGroupingColsAdded = 0;

	/**
		Add a column to the list

		@param column	The column to add to the list
	 */
	public void addGroupByColumn(GroupByColumn column)
	{
		addElement(column);
	}

	/**
		Get a column from the list

		@param position	The column to get from the list
	 */
	public GroupByColumn getGroupByColumn(int position)
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(position >=0 && position < size(),
					"position (" + position +
					") expected to be between 0 and " + size());
		}
		return (GroupByColumn) elementAt(position);
	}

	/**
		Print the list.

		@param depth		The depth at which to indent the sub-nodes
	 */
	public void printSubNodes(int depth)
	{
		if (SanityManager.DEBUG)
		{
			for (int index = 0; index < size(); index++)
			{
				( (GroupByColumn) elementAt(index) ).treePrint(depth);
			}
		}
	}

	/**
	 * Get the number of grouping columns that need to be added to the SELECT list.
	 *
	 * @return int	The number of grouping columns that need to be added to
	 *				the SELECT list.
	 */
	public int getNumNeedToAddGroupingCols()
	{
		return numGroupingColsAdded;
	}

	/**
	 *  Bind the group by list.  Verify:
	 *		o  Number of grouping columns matches number of non-aggregates in
	 *		   SELECT's RCL.
	 *		o  Names in the group by list are unique
	 *		o  Names of grouping columns match names of non-aggregate
	 *		   expressions in SELECT's RCL.
	 *
	 * @param select		The SelectNode
	 * @param aggregateVector	The aggregate vector being built as we find AggregateNodes
	 *
	 * @return Nothing.
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void bindGroupByColumns(SelectNode select,
								   Vector	aggregateVector)
					throws StandardException
	{
		FromList		 fromList = select.getFromList();
		ResultColumnList selectRCL = select.getResultColumns();
		SubqueryList	 dummySubqueryList =
									(SubqueryList) getNodeFactory().getNode(
													C_NodeTypes.SUBQUERY_LIST,
													getContextManager());
		int				 numColsAddedHere = 0;
		int				 size = size();

		/* Only 32677 columns allowed in GROUP BY clause */
		if (size > DB2Limit.DB2_MAX_ELEMENTS_IN_GROUP_BY)
		{
			throw StandardException.newException(SQLState.LANG_TOO_MANY_ELEMENTS);
		}

		/* Bind the grouping column */
		for (int index = 0; index < size; index++)
		{
			GroupByColumn groupByCol = (GroupByColumn) elementAt(index);
			groupByCol.bindExpression(fromList,
									  dummySubqueryList, aggregateVector);
		}

		/* Verify that the columns in the GROUP BY list are unique.
		 * (Unique on table number and source ResultColumn.)
		 */
		verifyUniqueGroupingColumns();

		for (int index = 0; index < size; index++)
		{
			boolean				matchFound = false;
			GroupByColumn		groupingCol = (GroupByColumn) elementAt(index);
			String				curName = groupingCol.getColumnName();

			/* Verify that this entry in the GROUP BY list matches a
			 * grouping column in the select list.
			 */
			int				groupingColTableNum = groupingCol.getTableNumber();
			ResultColumn	groupingSource = groupingCol.getSource();
			int				rclSize = selectRCL.size();
			for (int inner = 0; inner < rclSize; inner++)
			{
				ColumnReference selectListCR;
				ResultColumn selectListRC = (ResultColumn) selectRCL.elementAt(inner);

				if (! (selectListRC.getExpression() instanceof ColumnReference))
				{
					continue;
				}
				selectListCR = (ColumnReference) selectListRC.getExpression();

				if (groupingColTableNum == selectListCR.getTableNumber() &&
					groupingSource == selectListCR.getSource())
				{
					/* Column positions for grouping columns are 0-based */
					groupingCol.setColumnPosition(inner + 1);

					/* Mark the RC in the SELECT list as a grouping column */
					selectListRC.markAsGroupingColumn();
					matchFound = true;
					break;
				}
			}

			/* If no match found in the SELECT list, then add a matching
			 * ResultColumn/ColumnReference pair to the SelectNode's RCL.
			 */
			if (! matchFound)
			{
				ResultColumn newRC;

				/* Get a new ResultColumn */
				newRC = (ResultColumn) getNodeFactory().getNode(
								C_NodeTypes.RESULT_COLUMN,
								groupingCol.getColumnName(),
								groupingCol.getColumnReference().getClone(),
								getContextManager());
				newRC.setVirtualColumnId(selectRCL.size() + 1);
				newRC.markGenerated();
				newRC.markAsGroupingColumn();

				/* Add the new RC/CR to the RCL */
				selectRCL.addElement(newRC);

				/* Set the columnPosition in the GroupByColumn, now that it
				* has a matching entry in the SELECT list.
				*/
				groupingCol.setColumnPosition(selectRCL.size());

				/*
				** Track the number of columns that we have added
				** in this routine.  We track this separately
				** than the total number of columns added by this
				** object (numGroupingColsAdded) because we
				** might be bound (though not gagged) more than
				** once (in which case numGroupingColsAdded will
				** already be set).
				*/
				numColsAddedHere++;
			}
		}

		/* Verify that no subqueries got added to the dummy list */
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(dummySubqueryList.size() == 0,
				"dummySubqueryList.size() is expected to be 0");
		}

		numGroupingColsAdded+= numColsAddedHere;
	}


	/**
	 * Check the uniqueness of the column names within a GROUP BY list.
	 *
	 * @return Nothing.
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void verifyUniqueGroupingColumns() throws StandardException
	{
		int				size = size();
		String			colName;

		for (int outer = 0; outer < size; outer++)
		{
			GroupByColumn		groupingCol = (GroupByColumn) elementAt(outer);
			int					outerTabNum = groupingCol.getTableNumber();
			ResultColumn		outerRC = groupingCol.getSource();
			String				curName = groupingCol.getColumnName();
			/* Verify that this column's name is unique within the list */
			colName = groupingCol.getColumnName();

			for (int inner = outer + 1; inner < size; inner++)
			{
				GroupByColumn		innerGBC = (GroupByColumn) elementAt(inner);
				int					innerTabNum = innerGBC.getTableNumber();
				ResultColumn		innerRC = innerGBC.getSource();
				if (outerTabNum == innerTabNum &&
					outerRC == innerRC)
				{
					throw StandardException.newException(SQLState.LANG_AMBIGUOUS_GROUPING_COLUMN, colName);
				}
			}
		}

		/* No duplicate column names */
	}

	/**
	 * Add any grouping columns which are not already in the appropriate RCL
	 * to the RCL.
	 * NOTE: The RC/VCNs in the SELECT list will point to the same pool of
	 *		 ResultColumns as the GroupByColumns in this list.
	 *
	 * @param selectNode	The SelectNode whose RCL we add to.
	 *
	 * @return Nothing.
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void addNewGroupingColumnsToRCL(SelectNode selectNode)
		throws StandardException
	{
		FromList			fromList = selectNode.getFromList();
		int					size = size();
		ResultColumnList	rcl = selectNode.getResultColumns();

		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(selectNode.getGroupByList() == this,
				"selectNode.getGroupByList() expected to equal this");
		}

		for (int index = 0; index < size; index++)
		{
			GroupByColumn		gbc = (GroupByColumn) elementAt(index);
			ResultColumn		newRC;
			VirtualColumnNode	newVCN;

			/* Skip over GBCs which have a match */
			if (gbc.getColumnPosition() != GroupByColumn.UNMATCHEDPOSITION)
			{
				continue;
			}

			/* Get and bind a new VCN */
			newVCN = (VirtualColumnNode) getNodeFactory().getNode(
							C_NodeTypes.VIRTUAL_COLUMN_NODE,
							fromList.getFromTableByResultColumn(gbc.getSource()),
							gbc.getSource(),
							ReuseFactory.getInteger(rcl.size() + 2),
							getContextManager());
			newVCN.setType(gbc.getColumnReference().getTypeServices());

			/* Get and bind a new ResultColumn */
			newRC = (ResultColumn) getNodeFactory().getNode(
							C_NodeTypes.RESULT_COLUMN,
							gbc.getColumnName(),
							newVCN,
							getContextManager());
			newRC.setType(newVCN.getTypeServices());
			newRC.setVirtualColumnId(rcl.size() + 2);

			/* Add the new RC/VCN to the RCL */
			rcl.addElement(newRC);

			/* Set the columnPosition in the GroupByColumn, now that it
			 * has a matching entry in the SELECT list.
			 */
			gbc.setColumnPosition(rcl.size());
		}
	}

	/**
	 *
	 */
	public GroupByColumn containsColumnReference(ColumnReference cr)
	{
		int size = size();
		for (int index = 0; index < size; index++)
		{
			GroupByColumn		groupingCol = (GroupByColumn) elementAt(index);

			if (groupingCol.getSource() == cr.getSource() &&
				groupingCol.getTableNumber() == cr.getTableNumber())
			{
				return groupingCol;
			}
		}

		return null;
	}

	/**
	 * Remap all ColumnReferences in this tree to be clones of the
	 * underlying expression.
	 *
	 * @return Nothing.
	 *
	 * @exception StandardException			Thrown on error
	 */
	public void remapColumnReferencesToExpressions() throws StandardException
	{
		GroupByColumn	gbc;
		int				size = size();

		/* This method is called when flattening a FromTable.  We should
		 * not be flattening a FromTable if the underlying expression that
		 * will get returned out, after chopping out the redundant ResultColumns,
		 * is not a ColumnReference.  (See ASSERT below.)
		 */
		for (int index = 0; index < size; index++)
		{
			ValueNode	retVN;
			gbc = (GroupByColumn) elementAt(index);

			retVN = gbc.getColumnReference().remapColumnReferencesToExpressions();

			if (SanityManager.DEBUG)
			{
				SanityManager.ASSERT(retVN instanceof ColumnReference,
					"retVN expected to be instanceof ColumnReference, not " +
					retVN.getClass().getName());
			}

			gbc.setColumnReference((ColumnReference) retVN);
		}
	}

	/**
	 * Print it out, baby
	 */
	public String toString()
	{
		if (SanityManager.DEBUG)
		{
			StringBuffer	buf = new StringBuffer();

			for (int index = 0; index < size(); index++)
			{
				GroupByColumn	groupingCol = (GroupByColumn) elementAt(index);

				buf.append(groupingCol.toString());
			}
			return buf.toString();
		}
		else
		{
			return "";
		}
	}
}
