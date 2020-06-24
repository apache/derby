/*

   Derby - Class org.apache.derby.impl.sql.compile.GroupByList

//IC see: https://issues.apache.org/jira/browse/DERBY-1377
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
import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.shared.common.reference.Limits;
import org.apache.derby.shared.common.reference.SQLState;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.shared.common.sanity.SanityManager;

/**
 * A GroupByList represents the list of expressions in a GROUP BY clause in
 * a SELECT statement.
 *
 */

//IC see: https://issues.apache.org/jira/browse/DERBY-673
class GroupByList extends OrderedColumnList<GroupByColumn>
{
	int		numGroupingColsAdded = 0;
	boolean         rollup = false;

    public GroupByList(ContextManager cm) {
        super(GroupByColumn.class, cm);
    }


	/**
		Add a column to the list

		@param column	The column to add to the list
	 */
    void addGroupByColumn(GroupByColumn column)
	{
		addElement(column);
	}

	/**
		Get a column from the list

		@param position	The column to get from the list
	 */
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
    GroupByColumn getGroupByColumn(int position)
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(position >=0 && position < size(),
					"position (" + position +
					") expected to be between 0 and " + size());
		}
//IC see: https://issues.apache.org/jira/browse/DERBY-673
        return elementAt(position);
	}


//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
    void setRollup()
	{
		rollup = true;
	}
    boolean isRollup()
	{
		return rollup;
	}
                        

	/**
	 * Get the number of grouping columns that need to be added to the SELECT list.
	 *
	 * @return int	The number of grouping columns that need to be added to
	 *				the SELECT list.
	 */
    int getNumNeedToAddGroupingCols()
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
     * @param aggregates    The aggregate list being built as we find AggregateNodes
	 *
	 * @exception StandardException		Thrown on error
	 */
//IC see: https://issues.apache.org/jira/browse/DERBY-6213
    void bindGroupByColumns(SelectNode select, List<AggregateNode> aggregates)
					throws StandardException
	{
		FromList		 fromList = select.getFromList();
		ResultColumnList selectRCL = select.getResultColumns();
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
        SubqueryList dummySubqueryList = new SubqueryList(getContextManager());
		int				 numColsAddedHere = 0;

		/* Only 32677 columns allowed in GROUP BY clause */
//IC see: https://issues.apache.org/jira/browse/DERBY-673
        if (size() > Limits.DB2_MAX_ELEMENTS_IN_GROUP_BY)
		{
			throw StandardException.newException(SQLState.LANG_TOO_MANY_ELEMENTS);
		}

		/* Bind the grouping column */
        for (GroupByColumn groupByCol : this)
		{
			groupByCol.bindExpression(fromList,
//IC see: https://issues.apache.org/jira/browse/DERBY-6075
                                      dummySubqueryList, aggregates);
		}

		
		int				rclSize = selectRCL.size();
        for (GroupByColumn groupingCol : this)
		{
			boolean				matchFound = false;

			/* Verify that this entry in the GROUP BY list matches a
			 * grouping column in the select list.
			 */
			for (int inner = 0; inner < rclSize; inner++)
			{
                ResultColumn selectListRC = selectRCL.elementAt(inner);
				if (!(selectListRC.getExpression() instanceof ColumnReference)) {
					continue;
				}
				
				ColumnReference selectListCR = (ColumnReference) selectListRC.getExpression();

				if (selectListCR.isEquivalent(groupingCol.getColumnExpression())) { 
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
			 * However, don't add additional result columns if the query
			 * specified DISTINCT, because distinct processing considers
			 * the entire RCL and including extra columns could change the
			 * results: e.g. select distinct a,b from t group by a,b,c
			 * should not consider column c in distinct processing (DERBY-3613)
			 */
			if (! matchFound && !select.hasDistinct() &&
			    groupingCol.getColumnExpression() instanceof ColumnReference) 
			{
			    	// only add matching columns for column references not 
			    	// expressions yet. See DERBY-883 for details. 
				ResultColumn newRC;

				/* Get a new ResultColumn */
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
                newRC = new ResultColumn(
                        groupingCol.getColumnName(),
                        groupingCol.getColumnExpression().getClone(),
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
				
				// a new hidden or generated column is added to this RCL
				// i.e. that the size() of the RCL != visibleSize(). 
				// Error checking done later should be aware of this 
				// special case.
				selectRCL.setCountMismatchAllowed(true);

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
			if (groupingCol.getColumnExpression() instanceof JavaToSQLValueNode) 
			{
				// disallow any expression which involves native java computation. 
				// Not possible to consider java expressions for equivalence.
				throw StandardException.newException(					
						SQLState.LANG_INVALID_GROUPED_SELECT_LIST);
			}
		}

		/* Verify that no subqueries got added to the dummy list */
        if (dummySubqueryList.size() != 0) {
            throw StandardException.newException(
                    SQLState.LANG_SUBQUERY_IN_GROUPBY_LIST);
        }

		numGroupingColsAdded+= numColsAddedHere;
	}

	

	/**
	 * Find the matching grouping column if any for the given expression
	 * 
	 * @param node an expression for which we are trying to find a match
	 * in the group by list.
	 * 
	 * @return the matching GroupByColumn if one exists, null otherwise.
	 * 
	 * @throws StandardException
	 */
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
    GroupByColumn findGroupingColumn(ValueNode node)
	        throws StandardException
	{
//IC see: https://issues.apache.org/jira/browse/DERBY-673
        for (GroupByColumn gbc : this)
		{
			if (gbc.getColumnExpression().isEquivalent(node))
			{
				return gbc;
			}
		}
		return null;
	}
	
	/**
	 * Remap all ColumnReferences in this tree to be clones of the
	 * underlying expression.
	 *
	 * @exception StandardException			Thrown on error
	 */
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
    void remapColumnReferencesToExpressions() throws StandardException
	{
        /* This method is called when flattening a FromTable. */
        for (GroupByColumn gbc : this)
		{
            gbc.setColumnExpression(
                gbc.getColumnExpression().remapColumnReferencesToExpressions());
		}
	}


	/**
	 * Convert this object to a String.  See comments in QueryTreeNode.java
	 * for how this should be done for tree printing.
	 *
	 * @return	This object as a String
	 */
    @Override
	public String toString()
	{
//IC see: https://issues.apache.org/jira/browse/DERBY-4087
		if (SanityManager.DEBUG) {
			return "numGroupingColsAdded: " + numGroupingColsAdded + "\n" +
				super.toString();
		} else {
			return "";
		}
	}


//IC see: https://issues.apache.org/jira/browse/DERBY-673
    void preprocess(int numTables,
                    FromList fromList,
                    SubqueryList whereSubquerys,
                    PredicateList wherePredicates) throws StandardException
	{
        for (GroupByColumn gbc : this)
		{
            gbc.setColumnExpression(
                    gbc.getColumnExpression().preprocess(
							numTables, fromList, whereSubquerys, wherePredicates));
		}		
	}
}
