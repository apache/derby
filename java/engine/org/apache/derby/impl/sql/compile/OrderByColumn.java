/*

   Derby - Class org.apache.derby.impl.sql.compile.OrderByColumn

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

import org.apache.derby.iapi.types.TypeId;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.sql.compile.NodeFactory;
import org.apache.derby.iapi.sql.compile.C_NodeTypes;

/**
 * An OrderByColumn is a column in the ORDER BY clause.  An OrderByColumn
 * can be ordered ascending or descending.
 *
 * We need to make sure that the named columns are
 * columns in that query, and that positions are within range.
 *
 * @author ames
 */
public class OrderByColumn extends OrderedColumn {

	private ResultColumn	resultCol;
	private String			columnName;
	private	String			correlationName;
	private String	schemaName;
	private boolean			ascending = true;

	/**
	 * Initializer.
	 *
	 * @param columnName		The name of the column being referenced
	 * @param correlationName	The correlation name, if any
	 */
	public void init(
						Object columnName, 
						Object correlationName,
						Object schemaName) 
	{
		this.columnName = (String) columnName;
		this.correlationName = (String) correlationName;
		this.schemaName = (String) schemaName;
	}

	/**
	 * Initializer.
	 *
	 * @param columnPosition	The position of the column being referenced
	 */
	public void init(Object columnPosition) {
		this.columnPosition = ((Integer) columnPosition).intValue();
	}

	/**
	 * Convert this object to a String.  See comments in QueryTreeNode.java
	 * for how this should be done for tree printing.
	 *
	 * @return	This object as a String
	 */
	public String toString() {
		if (SanityManager.DEBUG) {
			return "columnName: " + columnName + "\n" +
				"correlationName: " + correlationName + "\n" +
				"schemaName: " + schemaName + "\n" +
				super.toString();
		} else {
			return "";
		}
	}

	/**
	 * Get the name of this column
	 *
	 * @return	The name of this column
	 */
	public String getColumnName() {
		return columnName;
	}

	/**
	 * Mark the column as descending order
	 */
	public void setDescending() {
		ascending = false;
	}

	/**
	 * Get the column order.  Overrides 
	 * OrderedColumn.isAscending.
	 *
	 * @return true if ascending, false if descending
	 */
	public boolean isAscending() {
		return ascending;
	}

	/**
	 * Get the underlying ResultColumn.
	 *
	 * @return The underlying ResultColumn.
	 */
	ResultColumn getResultColumn()
	{
		return resultCol;
	}

	/**
	 * Get the underlying expression, skipping over ResultColumns that
	 * are marked redundant.
	 */
	ValueNode getNonRedundantExpression()
	{
		ResultColumn	rc;
		ValueNode		value;
		ColumnReference	colref = null;

		for (rc = resultCol; rc.isRedundant(); rc = colref.getSource())
		{
			value = rc.getExpression();

			if (value instanceof ColumnReference)
			{
				colref = (ColumnReference) value;
			}
			else
			{
				if (SanityManager.DEBUG)
				{
					SanityManager.THROWASSERT(
						"value should be a ColumnReference, but is a " +
						value.getClass().getName());
				}
			}
		}

		return rc.getExpression();
	}

	/**
	 * Bind this column.
	 *
	 * @param target	The result set being selected from
	 *
	 * @exception StandardException		Thrown on error
	 * @exception StandardException		Thrown when column not found
	 */
	public void bindOrderByColumn(ResultSetNode target)
				throws StandardException 
	{
		int					sourceTableNumber = -1;
		ResultColumnList	targetCols = target.getResultColumns();

		//bug 5716 - for db2 compatibility - no qualified names allowed in order by clause when union/union all operator is used 
		if (target instanceof UnionNode && correlationName != null)
		{
			String fullName = (schemaName != null) ?
				(schemaName + "." + correlationName + "." + columnName) :
				(correlationName + "." + columnName);
			throw StandardException.newException(SQLState.LANG_QUALIFIED_COLUMN_NAME_NOT_ALLOWED, fullName);
		}
		/* If the correlation name is non-null then we need to verify that it
		 * is a valid exposed name.  
		 */
		if (correlationName != null)
		{
			/* Find the matching FromTable visible in the current scope.
			 * We first try a full match on both schema and table name.  If no
			 * match, then we go for same table id.
			 */
			FromTable fromTable = target.getFromTableByName(correlationName, schemaName, true);
			if (fromTable == null)
			{
				fromTable = target.getFromTableByName(correlationName, schemaName, false);
				if (fromTable == null)
				{
					String fullName = (schemaName != null) ?
										(schemaName + "." + correlationName) : 
										correlationName;
					// correlation name is not an exposed name in the current scope
					throw StandardException.newException(SQLState.LANG_EXPOSED_NAME_NOT_FOUND, fullName);
				}
			}
			
			/* HACK - if the target is a UnionNode, then we have to
			 * have special code to get the sourceTableNumber.  This is
			 * because of the gyrations we go to with building the RCLs
			 * for a UnionNode.
			 */
			if (target instanceof UnionNode)
			{
				sourceTableNumber = ((FromTable) target).getTableNumber();
			}
			else
			{
				sourceTableNumber = fromTable.getTableNumber();
			}
		}

		if (columnName != null) 
		{
			/* If correlation name is not null, then we look an RC whose expression is a
			 * ColumnReference with the same table number as the FromTable with 
			 * correlationName as its exposed name.
			 * If correlation name is null, then we simply look for an RC whose name matches
			 * columnName.
			 */
			resultCol = targetCols.getOrderByColumn(columnName, correlationName, sourceTableNumber);

			/* DB2 doesn't allow ordering using generated column name */
			if ((resultCol == null) || resultCol.isNameGenerated())
			{
				String errString = (correlationName == null) ?
									columnName :
									correlationName + "." + columnName;
				throw StandardException.newException(SQLState.LANG_ORDER_BY_COLUMN_NOT_FOUND, errString);
			}
			columnPosition = resultCol.getColumnPosition();
		}
		else {
			resultCol = targetCols.getOrderByColumn(columnPosition);
			if (resultCol == null) {
				throw StandardException.newException(SQLState.LANG_COLUMN_OUT_OF_RANGE, String.valueOf(columnPosition));
			}
		}

		// Verify that the column is orderable
		resultCol.verifyOrderable();
	}

	/**
	 * Pull up this orderby column if it doesn't appear in the resultset
	 *
	 * @param target	The result set being selected from
	 *
	 */
	public void pullUpOrderByColumn(ResultSetNode target)
				throws StandardException 
	{
		if (columnName != null) 
		{
			/* If correlation name is not null, then we look an RC whose expression is a
			 * ColumnReference with the same table number as the FromTable with 
			 * correlationName as its exposed name.
			 * If correlation name is null, then we simply look for an RC whose name matches
			 * columnName.
			 */
			ResultColumnList	targetCols = target.getResultColumns();
			resultCol = targetCols.getOrderByColumn(columnName, correlationName);
			if (resultCol == null) 
			{// add this order by column to the result set

				TableName tabName = null;
				if (schemaName != null || correlationName != null)
				{
					tabName = (TableName) getNodeFactory().getNode(
																   C_NodeTypes.TABLE_NAME,
																   schemaName,
																   correlationName,
																   getContextManager());
				}

				ColumnReference cr = (ColumnReference) getNodeFactory().getNode(
																		   C_NodeTypes.COLUMN_REFERENCE,
																		   columnName,
																		   tabName,
																		   getContextManager());
				
				resultCol = (ResultColumn) getNodeFactory().getNode(
															   C_NodeTypes.RESULT_COLUMN,
															   columnName,
															   cr, // column reference
															   getContextManager());

				targetCols.addResultColumn(resultCol);
				targetCols.incOrderBySelect();
			}
		}
	}

	/**
	 * Order by columns now point to the PRN above the node of interest.
	 * We need them to point to the RCL under that one.  This is useful
	 * when combining sorts where we need to reorder the sorting
	 * columns.
	 *
	 * @return Nothing.
	 */
	void resetToSourceRC()
	{
		if (SanityManager.DEBUG)
		{
			if (! (resultCol.getExpression() instanceof VirtualColumnNode))
			{
				SanityManager.THROWASSERT(
					"resultCol.getExpression() expected to be instanceof VirtualColumnNode " +
					", not " + resultCol.getExpression().getClass().getName());
			}
		}

		VirtualColumnNode vcn = (VirtualColumnNode) resultCol.getExpression();
		resultCol = vcn.getSourceResultColumn();
	}

	/**
	 * Is this OrderByColumn constant, according to the given predicate list?
	 * A constant column is one where all the column references it uses are
	 * compared equal to constants.
	 */
	boolean constantColumn(PredicateList whereClause)
	{
		ValueNode sourceExpr = resultCol.getExpression();

		return sourceExpr.constantExpression(whereClause);
	}

	/**
	 * Remap all the column references under this OrderByColumn to their
	 * expressions.
	 *
	 * @exception StandardException		Thrown on error
	 */
	void remapColumnReferencesToExpressions() throws StandardException
	{
		resultCol.setExpression(
			resultCol.getExpression().remapColumnReferencesToExpressions());
	}
}
