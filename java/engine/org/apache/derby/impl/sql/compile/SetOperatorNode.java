/*

   Derby - Class org.apache.derby.impl.sql.compile.SetOperatorNode

   Copyright 2004 The Apache Software Foundation or its licensors, as applicable.

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

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.sql.compile.C_NodeTypes;

import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;

import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.types.DataTypeDescriptor;

import org.apache.derby.iapi.util.JBitSet;

/**
 * A SetOperatorNode represents a UNION, INTERSECT, or EXCEPT in a DML statement. Binding and optimization
 * preprocessing is the same for all of these operations, so they share bind methods in this abstract class.
 *
 * The class contains a boolean telling whether the operation should eliminate
 * duplicate rows.
 *
 * @author Jeff Lichtman
 */

public abstract class SetOperatorNode extends TableOperatorNode
{
	/**
	** Tells whether to eliminate duplicate rows.  all == TRUE means do
	** not eliminate duplicates, all == FALSE means eliminate duplicates.
	*/
	boolean			all;

	OrderByList orderByList;


	/**
	 * Initializer for a SetOperatorNode.
	 *
	 * @param leftResult		The ResultSetNode on the left side of this union
	 * @param rightResult		The ResultSetNode on the right side of this union
	 * @param all				Whether or not this is an ALL.
	 * @param tableProperties	Properties list associated with the table
	 *
	 * @exception StandardException		Thrown on error
	 */

	public void init(
					Object leftResult,
					Object rightResult,
					Object all,
					Object tableProperties)
			throws StandardException
	{
		super.init(leftResult, rightResult, tableProperties);

		this.all = ((Boolean) all).booleanValue();

		/* resultColumns cannot be null, so we make a copy of the left RCL
		 * for now.  At bind() time, we need to recopy the list because there
		 * may have been a "*" in the list.  (We will set the names and
		 * column types at that time, as expected.)
		 */
		resultColumns = leftResultSet.getResultColumns().copyListAndObjects();
	}

	/**
	 * Convert this object to a String.  See comments in QueryTreeNode.java
	 * for how this should be done for tree printing.
	 *
	 * @return	This object as a String
	 */

	public String toString()
	{
		if (SanityManager.DEBUG)
		{
			return 	"all: " + all + "\n" +
				"orderByList: " + 
				(orderByList != null ? orderByList.toString() : "null") + "\n" +
				super.toString();
		}
		else
		{
			return "";
		}
	}

	/**
	 * Bind the result columns of this ResultSetNode when there is no
	 * base table to bind them to.  This is useful for SELECT statements,
	 * where the result columns get their types from the expressions that
	 * live under them.
	 *
	 * @param fromListParam		FromList to use/append to.
	 *
	 * @return	Nothing
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void bindResultColumns(FromList fromListParam)
					throws StandardException
	{
		super.bindResultColumns(fromListParam);

		/* Now we build our RCL */
		buildRCL();
	}

	/**
	 * Bind the result columns for this ResultSetNode to a base table.
	 * This is useful for INSERT and UPDATE statements, where the
	 * result columns get their types from the table being updated or
	 * inserted into.
	 * If a result column list is specified, then the verification that the 
	 * result column list does not contain any duplicates will be done when
	 * binding them by name.
	 *
	 * @param targetTableDescriptor	The TableDescriptor for the table being
	 *				updated or inserted into
	 * @param targetColumnList	For INSERT statements, the user
	 *					does not have to supply column
	 *					names (for example, "insert into t
	 *					values (1,2,3)".  When this
	 *					parameter is null, it means that
	 *					the user did not supply column
	 *					names, and so the binding should
	 *					be done based on order.  When it
	 *					is not null, it means do the binding
	 *					by name, not position.
	 * @param statement			Calling DMLStatementNode (Insert or Update)
	 * @param fromListParam		FromList to use/append to.
	 *
	 * @return	Nothing
	 *
	 * @exception StandardException		Thrown on error
	 */

	public void bindResultColumns(TableDescriptor targetTableDescriptor,
					FromVTI targetVTI,
					ResultColumnList targetColumnList,
					DMLStatementNode statement,
					FromList fromListParam)
				throws StandardException
	{
		super.bindResultColumns(targetTableDescriptor,
								targetVTI,
								targetColumnList, statement,
								fromListParam);

		/* Now we build our RCL */
		buildRCL();
	}

	/**
	 * Build the RCL for this node.  We propagate the RCL up from the
	 * left child to form this node's RCL.
	 *
	 * @return	Nothing
	 *
	 * @exception StandardException		Thrown on error
	 */

	private void buildRCL() throws StandardException
	{
		/* Verify that both sides of the union have the same # of columns in their
		 * RCL.
		 */
		if (leftResultSet.getResultColumns().size() !=
			rightResultSet.getResultColumns().size())
		{
			throw StandardException.newException(SQLState.LANG_UNION_UNMATCHED_COLUMNS,
                                                 getOperatorName());
		}

		/* We need to recreate resultColumns for this node, since there
		 * may have been 1 or more *'s in the left's SELECT list.
		 */
		resultColumns = leftResultSet.getResultColumns().copyListAndObjects();

		/* Create new expressions with the dominant types after verifying
		 * union compatibility between left and right sides.
		 */
		resultColumns.setUnionResultExpression(rightResultSet.getResultColumns(), tableNumber, level, getOperatorName());
	}

	/**
	 * Bind the result columns of a table constructor to the types in the
	 * given ResultColumnList.  Use when inserting from a table constructor,
	 * and there are nulls in the values clauses.
	 *
	 * @param rcl	The ResultColumnList with the types to bind to
	 *
	 * @exception StandardException		Thrown on error.
	 */
	public void bindUntypedNullsToResultColumns(ResultColumnList rcl)
				throws StandardException
	{
		/*
		** If the RCL from the parent is null, then
		** the types are coming from the union itself.
		** So we have to cross check the two child
		** rcls.
		*/
		if (rcl == null)
		{
			ResultColumnList lrcl = rightResultSet.getResultColumns();
			ResultColumnList rrcl = leftResultSet.getResultColumns();

			leftResultSet.bindUntypedNullsToResultColumns(rrcl);
			rightResultSet.bindUntypedNullsToResultColumns(lrcl);
		}
		else	
		{
			leftResultSet.bindUntypedNullsToResultColumns(rcl);
			rightResultSet.bindUntypedNullsToResultColumns(rcl);
		}			
	}

	/**
	 * Get the parameter types from the given RowResultSetNode into the
	 * given array of types.  If an array position is already filled in,
	 * don't clobber it.
	 *
	 * @param types	The array of types to fill in
	 * @param rrsn	The RowResultSetNode from which to take the param types
	 *
	 * @return	The number of new types found in the RowResultSetNode
	 */
	int getParamColumnTypes(DataTypeDescriptor[] types, RowResultSetNode rrsn)
	{
		int	numTypes = 0;

		/* Look for columns where we have not found a non-? yet. */
		for (int i = 0; i < types.length; i++)
		{
			if (types[i] == null)
			{
				ResultColumn rc =
					(ResultColumn) rrsn.getResultColumns().elementAt(i);
				if ( ! (rc.getExpression().isParameterNode()))
				{
					types[i] = rc.getExpressionType();
					numTypes++;
				}
			}
		}

		return numTypes;
	}

	/**
	 * Set the type of each ? parameter in the given RowResultSetNode
	 * according to its ordinal position in the given array of types.
	 *
	 * @param types	An array of types containing the proper type for each
	 *				? parameter, by ordinal position.
	 * @param rrsn	A RowResultSetNode that could contain ? parameters whose
	 *				types need to be set.
	 *
	 * @exception StandardException		Thrown on error
	 */
	void setParamColumnTypes(DataTypeDescriptor[] types, RowResultSetNode rrsn)
					throws StandardException
	{
		/*
		** Look for ? parameters in the result column list
		** of each RowResultSetNode
		*/
		ResultColumnList rrcl = rrsn.getResultColumns();
		int rrclSize = rrcl.size();
		for (int index = 0; index < rrclSize; index++)
		{
			ResultColumn	rc = (ResultColumn) rrcl.elementAt(index);

			if (rc.getExpression().isParameterNode())
			{
				/*
				** We found a ? - set its type to the type from the
				** type array.
				*/
				((ParameterNode) rc.getExpression()).setDescriptor(
											types[index]);
			}
		}
	}

	/**
	 * Bind the expressions in the target list.  This means binding the
	 * sub-expressions, as well as figuring out what the return type is
	 * for each expression.  This is useful for EXISTS subqueries, where we
	 * need to validate the target list before blowing it away and replacing
	 * it with a SELECT true.
	 *
	 * @return	Nothing
	 *
	 * @exception StandardException		Thrown on error
	 */

	public void bindTargetExpressions(FromList fromListParam)
					throws StandardException
	{
		leftResultSet.bindTargetExpressions(fromListParam);
		rightResultSet.bindTargetExpressions(fromListParam);
	}

	/**
	 * Push the order by list down from the cursor node
	 * into its child result set so that the optimizer
	 * has all of the information that it needs to 
	 * consider sort avoidance.
	 *
	 * @param orderByList	The order by list
	 *
	 * @return Nothing.
	 */
	void pushOrderByList(OrderByList orderByList)
	{
		this.orderByList = orderByList;
	}

	/** 
	 * Put a ProjectRestrictNode on top of each FromTable in the FromList.
	 * ColumnReferences must continue to point to the same ResultColumn, so
	 * that ResultColumn must percolate up to the new PRN.  However,
	 * that ResultColumn will point to a new expression, a VirtualColumnNode, 
	 * which points to the FromTable and the ResultColumn that is the source for
	 * the ColumnReference.  
	 * (The new PRN will have the original of the ResultColumnList and
	 * the ResultColumns from that list.  The FromTable will get shallow copies
	 * of the ResultColumnList and its ResultColumns.  ResultColumn.expression
	 * will remain at the FromTable, with the PRN getting a new 
	 * VirtualColumnNode for each ResultColumn.expression.)
	 * We then project out the non-referenced columns.  If there are no referenced
	 * columns, then the PRN's ResultColumnList will consist of a single ResultColumn
	 * whose expression is 1.
	 *
	 * @param numTables			Number of tables in the DML Statement
	 * @param gbl				The group by list, if any
	 * @param fromList			The from list, if any
	 *
	 * @return The preprocessed ResultSetNode that can be optimized
	 *
	 * @exception StandardException		Thrown on error
	 */

	public ResultSetNode preprocess(int numTables,
									GroupByList gbl,
									FromList fromList)
								throws StandardException
	{
		ResultSetNode newTop = this;

		/* RESOLVE - what does numTables and referencedTableMap mean here? */
		leftResultSet = leftResultSet.preprocess(numTables, gbl, fromList);
		rightResultSet = rightResultSet.preprocess(numTables, gbl, fromList);

		/* Build the referenced table map (left || right) */
		referencedTableMap = (JBitSet) leftResultSet.getReferencedTableMap().clone();
		referencedTableMap.or((JBitSet) rightResultSet.getReferencedTableMap());

		/* If this is a UNION without an all and we have
		 * an order by then we can consider eliminating the sort for the
		 * order by.  All of the columns in the order by list must
		 * be ascending in order to do this.  There are 2 cases:
		 *	o	The order by list is an in order prefix of the columns
		 *		in the select list.  In this case the output of the
		 *		sort from the distinct will be in the right order
		 *		so we simply eliminate the order by list.
		 *	o	The order by list is a subset of the columns in the
		 *		the select list.  In this case we need to reorder the
		 *		columns in the select list so that the ordering columns
		 *		are an in order prefix of the select list and put a PRN
		 *		above the select so that the shape of the result set
		 *		is as expected.
		 */
		if ((! all) && orderByList != null && orderByList.allAscending())
		{
			/* Order by list currently restricted to columns in select
			 * list, so we will always eliminate the order by here.
			 */
			if (orderByList.isInOrderPrefix(resultColumns))
			{
				orderByList = null;
			}
			/* RESOLVE - We currently only eliminate the order by if it is
			 * a prefix of the select list.  We do not currently do the 
			 * elimination if the order by is not a prefix because the code
			 * doesn't work.  The problem has something to do with the
			 * fact that we generate additional nodes between the union
			 * and the PRN (for reordering that we would generate here)
			 * when modifying the access paths.  VCNs under the PRN can be
			 * seen as correlated since their source resultset is the Union
			 * which is no longer the result set directly under them.  This
			 * causes the wrong code to get generated. (jerry - 11/3/98)
			 * (bug 59)
			 */
		}

		return newTop;
	}
	
	/**
	 * Ensure that the top of the RSN tree has a PredicateList.
	 *
	 * @param numTables			The number of tables in the query.
	 * @return ResultSetNode	A RSN tree with a node which has a PredicateList on top.
	 *
	 * @exception StandardException		Thrown on error
	 */
	public ResultSetNode ensurePredicateList(int numTables) 
		throws StandardException
	{
		return genProjectRestrict(numTables);
	}

	/**
	 * Verify that a SELECT * is valid for this type of subquery.
	 *
	 * @param outerFromList	The FromList from the outer query block(s)
	 * @param subqueryType	The subquery type
	 *
	 * @return	None
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void verifySelectStarSubquery(FromList outerFromList, int subqueryType) 
					throws StandardException
	{
		/* Check both sides - SELECT * is not valid on either side */
		leftResultSet.verifySelectStarSubquery(outerFromList, subqueryType);
		rightResultSet.verifySelectStarSubquery(outerFromList, subqueryType);
	}

	/** 
	 * Determine whether or not the specified name is an exposed name in
	 * the current query block.
	 *
	 * @param name	The specified name to search for as an exposed name.
	 * @param schemaName	Schema name, if non-null.
	 * @param exactMatch	Whether or not we need an exact match on specified schema and table
	 *						names or match on table id.
	 *
	 * @return The FromTable, if any, with the exposed name.
	 *
	 * @exception StandardException		Thrown on error
	 */
	protected FromTable getFromTableByName(String name, String schemaName, boolean exactMatch)
		throws StandardException
	{
		/* We search both sides for a TableOperatorNode (join nodes)
		 * but only the left side for a UnionNode.
		 */
		return leftResultSet.getFromTableByName(name, schemaName, exactMatch);
	}

	/**
	 * Set the result column for the subquery to a boolean true,
	 * Useful for transformations such as
	 * changing:
	 *		where exists (select ... from ...) 
	 * to:
	 *		where (select true from ...)
	 *
	 * NOTE: No transformation is performed if the ResultColumn.expression is
	 * already the correct boolean constant.
	 * 
	 * @param onlyConvertAlls	Boolean, whether or not to just convert *'s
	 *
	 * @return Nothing.
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void setResultToBooleanTrueNode(boolean onlyConvertAlls)
				throws StandardException
	{
		super.setResultToBooleanTrueNode(onlyConvertAlls);
		leftResultSet.setResultToBooleanTrueNode(onlyConvertAlls);
		rightResultSet.setResultToBooleanTrueNode(onlyConvertAlls);
	}

	/**
	 * This ResultSet is the source for an Insert.  The target RCL
	 * is in a different order and/or a superset of this RCL.  In most cases
	 * we will reorder and/or add defaults to the current RCL so that is
	 * matches the target RCL.  Those RSNs whose generate() method does
	 * not handle projects will insert a PRN, with a new RCL which matches
	 * the target RCL, above the current RSN.
	 * NOTE - The new or enhanced RCL will be fully bound.
	 *
	 * @param numTargetColumns	# of columns in target RCL
	 * @param colMap[]			int array representation of correspondence between
	 *							RCLs - colmap[i] = -1 -> missing in current RCL
	 *								   colmap[i] = j -> targetRCL(i) <-> thisRCL(j+1)
	 * @param dataDictionary	DataDictionary to use
	 * @param targetTD			TableDescriptor for target if the target is not a VTI, null if a VTI
     * @param targetVTI         Target description if it is a VTI, null if not a VTI
	 *
	 * @return ResultSetNode	The new top of the tree
	 *
	 * @exception StandardException		Thrown on error
	 */
	public ResultSetNode enhanceRCLForInsert(int numTargetColumns, int[] colMap, 
											 DataDictionary dataDictionary,
											 TableDescriptor targetTD,
                                             FromVTI targetVTI)
			throws StandardException
	{
		// our newResultCols are put into the bound form straight away.
		ResultColumnList newResultCols =
								(ResultColumnList) getNodeFactory().getNode(
												C_NodeTypes.RESULT_COLUMN_LIST,
												getContextManager());
		int numResultSetColumns = resultColumns.size();

		/* Create a massaged version of the source RCL.
		 * (Much simpler to build new list and then assign to source,
		 * rather than massage the source list in place.)
		 */
		for (int index = 0; index < numTargetColumns; index++)
		{
			ResultColumn	newResultColumn;
			ResultColumn	oldResultColumn;
			ColumnReference newColumnReference;

			if (colMap[index] != -1)
			{
				// getResultColumn uses 1-based positioning, so offset the colMap entry appropriately
				oldResultColumn = resultColumns.getResultColumn(colMap[index]+1);

				newColumnReference = (ColumnReference) getNodeFactory().getNode(
												C_NodeTypes.COLUMN_REFERENCE,
												oldResultColumn.getName(),
												null,
												getContextManager());
				/* The ColumnReference points to the source of the value */
				newColumnReference.setSource(oldResultColumn);
				// colMap entry is 0-based, columnId is 1-based.
				newColumnReference.setType(oldResultColumn.getExpressionType());

				// Source of an insert, so nesting levels must be 0
				newColumnReference.setNestingLevel(0);
				newColumnReference.setSourceLevel(0);

				// because the insert already copied the target table's
				// column descriptors into the result, we grab it from there.
				// alternatively, we could do what the else clause does,
				// and look it up in the DD again.
				newResultColumn = (ResultColumn) getNodeFactory().getNode(
						C_NodeTypes.RESULT_COLUMN,
						oldResultColumn.getType(),
						newColumnReference,
						getContextManager());
			}
			else
			{
				newResultColumn = genNewRCForInsert(targetTD, targetVTI, index + 1, dataDictionary);
			}

			newResultCols.addResultColumn(newResultColumn);
		}

		/* The generated ProjectRestrictNode now has the ResultColumnList
		 * in the order that the InsertNode expects.
		 * NOTE: This code here is an exception to several "rules":
		 *		o  This is the only ProjectRestrictNode that is currently
		 *		   generated outside of preprocess().
		 *	    o  The UnionNode is the only node which is not at the
		 *		   top of the query tree which has ColumnReferences under
		 *		   its ResultColumnList prior to expression push down.
		 */
		return (ResultSetNode) getNodeFactory().getNode(
									C_NodeTypes.PROJECT_RESTRICT_NODE,
									this,
									newResultCols,
									null,
									null,
									null,
									null,
									tableProperties,
									getContextManager());
	}

	/**
	 * Evaluate whether or not the subquery in a FromSubquery is flattenable.  
	 * Currently, a FSqry is flattenable if all of the following are true:
	 *		o  Subquery is a SelectNode. (ie, not a RowResultSetNode or a UnionNode)
	 *		o  It contains no top level subqueries.  (RESOLVE - we can relax this)
	 *		o  It does not contain a group by or having clause
	 *		o  It does not contain aggregates.
	 *
	 * @param fromList	The outer from list
	 *
	 * @return boolean	Whether or not the FromSubquery is flattenable.
	 */
	public boolean flattenableInFromSubquery(FromList fromList)
	{
		/* Unions in FromSubquerys are not flattenable.	 */
		return false;
	}

	/**
	 * Return whether or not to materialize this ResultSet tree.
	 *
	 * @return Whether or not to materialize this ResultSet tree.
	 *			would return valid results.
	 *
	 * @exception StandardException		Thrown on error
	 */
	public boolean performMaterialization(JBitSet outerTables)
		throws StandardException
	{
		// RESOLVE - just say no to materialization right now - should be a cost based decision
		return false;

		/* Actual materialization, if appropriate, will be placed by our parent PRN.
		 * This is because PRN might have a join condition to apply.  (Materialization
		 * can only occur before that.
		 */
		//return true;
	}

    /**
     * @return the operator name: "UNION", "INTERSECT", or "EXCEPT"
     */
    abstract String getOperatorName();
}
