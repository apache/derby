/*

   Derby - Class org.apache.derby.impl.sql.compile.FromSubquery

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

import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.iapi.services.context.ContextManager;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.compile.CompilerContext;
import org.apache.derby.iapi.sql.compile.C_NodeTypes;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.ColumnDescriptor;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.util.JBitSet;

import java.util.Properties;

/**
 * A FromSubquery represents a subquery in the FROM list of a DML statement.
 *
 * The current implementation of this class is only
 * sufficient for Insert's need to push a new
 * select on top of the one the user specified,
 * to make the selected structure match that
 * of the insert target table.
 *
 * @author Jeff Lichtman
 */
public class FromSubquery extends FromTable
{
	boolean			generatedForGroupByClause;
	boolean			generatedForHavingClause;
	ResultSetNode	subquery;

	/**
	 * Intializer for a table in a FROM list.
	 *
	 * @param tableName		The name of the table
	 * @param correlationName	The correlation name
	 * @param derivedRCL		The derived column list
	 * @param tableProperties	Properties list associated with the table
	 */
	public void init(
					Object subquery,
					Object correlationName,
				 	Object derivedRCL,
					Object tableProperties)
	{
		super.init(correlationName, tableProperties);
		this.subquery = (ResultSetNode) subquery;
		resultColumns = (ResultColumnList) derivedRCL;
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
			return
			  "generatedForGroupByClause: " + generatedForGroupByClause + "\n" +
			  "generatedForHavingClause: " + generatedForHavingClause + "\n" +
			  super.toString();
		}
		else
		{
			return "";
		}
	}

	/**
	 * Prints the sub-nodes of this object.  See QueryTreeNode.java for
	 * how tree printing is supposed to work.
	 *
	 * @param depth		The depth of this node in the tree
	 *
	 * @return	Nothing
	 */

	public void printSubNodes(int depth)
	{
		if (SanityManager.DEBUG) {
			super.printSubNodes(depth);

			if (subquery != null)
			{
				printLabel(depth, "subquery: ");
				subquery.treePrint(depth + 1);
			}
		}
	}

	/** 
	 * Return the "subquery" from this node.
	 *
	 * @return ResultSetNode	The "subquery" from this node.
	 */
	public ResultSetNode getSubquery()
	{
		return subquery;
	}

	/**
	 * Mark this FromSubquery as being generated for a GROUP BY clause.
	 * (This node represents the SELECT thru GROUP BY clauses.  We
	 * appear in the FromList of a SelectNode generated to represent
	 * the result of the GROUP BY.  This allows us to add ResultColumns
	 * to the SelectNode for the user's query.
	 *
	 * @return Nothing.
	 */
	public void markAsForGroupByClause()
	{
		generatedForGroupByClause = true;
	}

	/**
	 * Mark this FromSubquery as being generated for a HAVING clause.
	 * (This node represents the SELECT thru GROUP BY clauses.  We
	 * appear in the FromList of a SelectNode generated to represent
	 * the actual HAVING clause.
	 *
	 * @return Nothing.
	 */
	public void markAsForHavingClause()
	{
		generatedForHavingClause = true;
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
		if (generatedForGroupByClause || generatedForHavingClause)
		{
			return subquery.getFromTableByName(name, schemaName, exactMatch);
		}
		else 
		{
			return super.getFromTableByName(name, schemaName, exactMatch);
		}
	}

	/**
	 * Bind this subquery that appears in the FROM list.
	 *
	 * @param dataDictionary	The DataDictionary to use for binding
	 * @param fromListParam		FromList to use/append to.
	 *
	 * @return	ResultSetNode		The bound FromSubquery.
	 *
	 * @exception StandardException		Thrown on error
	 */

	public ResultSetNode bindNonVTITables(DataDictionary dataDictionary, 
						  FromList fromListParam) 
							throws StandardException
	{
		/* Assign the tableNumber */
		if (tableNumber == -1)  // allow re-bind, in which case use old number
			tableNumber = getCompilerContext().getNextTableNumber();

		subquery = subquery.bindNonVTITables(dataDictionary, fromListParam);

		return this;
	}

	/**
	 * Bind this subquery that appears in the FROM list.
	 *
	 * @param fromListParam		FromList to use/append to.
	 *
	 * @return	ResultSetNode		The bound FromSubquery.
	 *
	 * @exception StandardException		Thrown on error
	 */

	public ResultSetNode bindVTITables(FromList fromListParam) 
							throws StandardException
	{
		subquery = subquery.bindVTITables(fromListParam);

		return this;
	}

	/**
	 * Check for (and reject) ? parameters directly under the ResultColumns.
	 * This is done for SELECT statements.  For FromSubquery, we
	 * simply pass the check through to the subquery.
	 *
	 * @return	Nothing
	 *
	 * @exception StandardException		Thrown if a ? parameter found
	 *									directly under a ResultColumn
	 */

	public void rejectParameters() throws StandardException
	{
		subquery.rejectParameters();
	}

	/**
	 * Bind the expressions in this FromSubquery.  This means 
	 * binding the sub-expressions, as well as figuring out what the return 
	 * type is for each expression.
	 *
	 * @return	Nothing
	 *
	 * @exception StandardException		Thrown on error
	 */

	public void bindExpressions(FromList fromListParam)
					throws StandardException
	{
		FromList			emptyFromList =
								(FromList) getNodeFactory().getNode(
									C_NodeTypes.FROM_LIST,
									getNodeFactory().doJoinOrderOptimization(),
									getContextManager());
		ResultColumnList	derivedRCL = resultColumns;
		ResultColumnList	subqueryRCL;

		/* From subqueries cannot be correlated, so we pass an empty FromList
		 * to subquery.bindExpressions() and .bindResultColumns().
		 */
		subquery.bindExpressions(emptyFromList);
		subquery.bindResultColumns(emptyFromList);

		/* Now that we've bound the expressions in the subquery, we 
		 * can propagate the subquery's RCL up to the FromSubquery.
		 * Get the subquery's RCL, assign shallow copy back to
		 * it and create new VirtualColumnNodes for the original's
		 * ResultColumn.expressions.
		 * NOTE: If the size of the derived column list is less than
		 * the size of the subquery's RCL and the derived column list is marked
		 * for allowing a size mismatch, then we have a select * view
		 * on top of a table that has had columns added to it via alter table.
		 * In this case, we trim out the columns that have been added to
		 * the table since the view was created.
		 */
		subqueryRCL = subquery.getResultColumns();
		if (resultColumns != null && resultColumns.getCountMismatchAllowed() &&
			resultColumns.size() < subqueryRCL.size())
		{
			for (int index = subqueryRCL.size() - 1; 
				 index >= resultColumns.size(); 
				 index--)
			{
				subqueryRCL.removeElementAt(index);
			}
		}

		subquery.setResultColumns(subqueryRCL.copyListAndObjects());
		subqueryRCL.genVirtualColumnNodes(subquery, subquery.getResultColumns());
		resultColumns = subqueryRCL;

		/* Propagate the name info from the derived column list */
		if (derivedRCL != null)
		{
			 resultColumns.propagateDCLInfo(derivedRCL, correlationName);
		}
	}

	/**
	 * Try to find a ResultColumn in the table represented by this FromBaseTable
	 * that matches the name in the given ColumnReference.
	 *
	 * @param columnReference	The columnReference whose name we're looking
	 *				for in the given table.
	 *
	 * @return	A ResultColumn whose expression is the ColumnNode
	 *			that matches the ColumnReference.
	 *		Returns null if there is no match.
	 *
	 * @exception StandardException		Thrown on error
	 */

	public ResultColumn getMatchingColumn(ColumnReference columnReference) throws StandardException
	{
		ResultColumn	resultColumn = null;
		String			columnsTableName;

		/*
		** RESOLVE: When we add support for schemas, check to see if
		** the column name specifies a schema, and if so, if this
		** table is in that schema.
		*/

		columnsTableName = columnReference.getTableName();

		/* We have 5 cases here:
		 *  1.  ColumnReference was generated to replace an aggregate.
		 *		(We are the wrapper for a HAVING clause and the ColumnReference
		 *		was generated to reference the aggregate which was pushed down into
		 *		the SELECT list in the user's query.)  
		 *		Just do what you would expect.  Try to resolve the
		 *		ColumnReference against our RCL if the ColumnReference is unqualified
		 *		or if it is qualified with our exposed name.
		 *	2.	We are the wrapper for a GROUP BY and a HAVING clause and
		 *		either the ColumnReference is qualified or it is in
		 *		the HAVING clause.  For example:
		 *			select a from t1 group by a having t1.a = 1
		 *			select a as asdf from t1 group by a having a = 1
		 *		We need to match against the underlying FromList and then find
		 *		the grandparent ResultColumn in our RCL so that we return a
		 *		ResultColumn from the correct ResultSetNode.  It is okay not to
		 *		find a matching grandparent node.  In fact, this is how we ensure
		 *		the correct semantics for ColumnReferences in the HAVING clause
		 *		(which must be bound against the GROUP BY list.)
		 *  3.	We are the wrapper for a HAVING clause without a GROUP BY and
		 *		the ColumnReference is from the HAVING clause.  ColumnReferences
		 *		are invalid in this case, so we return null.
		 *  4.  We are the wrapper for a GROUP BY with no HAVING.  This has
		 *		to be a separate case because of #5 and the following query:
		 *			select * from (select c1 from t1) t, (select c1 from t1) tt
		 *			group by t1.c1, tt.c1
		 *		(The correlation names are lost in the generated FromSuquery.)
		 *  5.  Everything else - do what you would expect.  Try to resolve the
		 *		ColumnReference against our RCL if the ColumnReference is unqualified
		 *		or if it is qualified with our exposed name.
		 */
		if (columnReference.getGeneratedToReplaceAggregate()) // 1
		{
			resultColumn = resultColumns.getResultColumn(columnReference.getColumnName());
		}
		else if (generatedForGroupByClause && generatedForHavingClause &&
			     (columnsTableName != null || 
			      columnReference.getClause() != ValueNode.IN_SELECT_LIST)) // 2
		{
			if (SanityManager.DEBUG)
			{
				SanityManager.ASSERT(correlationName == null,
					"correlationName expected to be null");
				SanityManager.ASSERT(subquery instanceof SelectNode,
					"subquery expected to be instanceof SelectNode, not " +
					subquery.getClass().getName());
			}

			SelectNode		select = (SelectNode) subquery;

			resultColumn = select.getFromList().bindColumnReference(columnReference);

			/* Find and return the matching RC from our RCL.
			 * (Not an error if no match found.  Let ColumnReference deal with it.
			 */
			if (resultColumn != null)
			{
				/* Is there a matching resultColumn in the subquery's RCL? */
				resultColumn = subquery.getResultColumns().findParentResultColumn(
												resultColumn);
				if (resultColumn != null)
				{
					/* Is there a matching resultColumn in our RCL? */
					resultColumn = resultColumns.findParentResultColumn(
												resultColumn);
				}
			}
		}
		else if ((generatedForHavingClause && ! generatedForGroupByClause) // 3
			 && (columnReference.getClause() != ValueNode.IN_SELECT_LIST) )
		{
		    resultColumn = null;
		}
		else if (generatedForGroupByClause) // 4
		{
		        resultColumn = resultColumns.getResultColumn(
								     columnsTableName,
								     columnReference.getColumnName());
		}
		else if (columnsTableName == null || columnsTableName.equals(correlationName)) // 5?
		{
		    resultColumn = resultColumns.getAtMostOneResultColumn(columnReference, correlationName);
		}
		    

		if (resultColumn != null)
		{
			columnReference.setTableNumber(tableNumber);
		}

		return resultColumn;
	}

	/**
	 * Preprocess a ResultSetNode - this currently means:
	 *	o  Generating a referenced table map for each ResultSetNode.
	 *  o  Putting the WHERE and HAVING clauses in conjunctive normal form (CNF).
	 *  o  Converting the WHERE and HAVING clauses into PredicateLists and
	 *	   classifying them.
	 *  o  Ensuring that a ProjectRestrictNode is generated on top of every 
	 *     FromBaseTable and generated in place of every FromSubquery.  
	 *  o  Pushing single table predicates down to the new ProjectRestrictNodes.
	 *
	 * @param numTables			The number of tables in the DML Statement
	 * @param gbl				The group by list, if any
	 * @param fromList			The from list, if any
	 *
	 * @return ResultSetNode at top of preprocessed tree.
	 *
	 * @exception StandardException		Thrown on error
	 */

	public ResultSetNode preprocess(int numTables,
									GroupByList gbl,
									FromList fromList)
								throws StandardException
	{
		/* We want to chop out the FromSubquery from the tree and replace it 
		 * with a ProjectRestrictNode.  One complication is that there may be 
		 * ColumnReferences above us which point to the FromSubquery's RCL.
		 * What we want to return is a tree with a PRN with the
		 * FromSubquery's RCL on top.  (In addition, we don't want to be
		 * introducing any redundant ProjectRestrictNodes.)
		 * Another complication is that we want to be able to only only push
		 * projections and restrictions down to this ProjectRestrict, but
		 * we want to be able to push them through as well.
		 * So, we:
		 *		o call subquery.preprocess() which returns a tree with
		 *		  a SelectNode or a RowResultSetNode on top.  
		 *		o If the FSqry is flattenable(), then we return (so that the
		 *		  caller can then call flatten()), otherwise we:
		 *		o generate a PRN, whose RCL is the FSqry's RCL, on top of the result.
		 *		o create a referencedTableMap for the PRN which represents 
		 *		  the FSqry's tableNumber, since ColumnReferences in the outer
		 *		  query block would be referring to that one.  
		 *		  (This will allow us to push restrictions down to the PRN.)
		 */

		subquery = subquery.preprocess(numTables, gbl, fromList);

		/* Return if the FSqry is flattenable() 
		 * NOTE: We can't flatten a FromSubquery if there is a group by list
		 * because the group by list must be ColumnReferences.  For:
		 *	select c1 from v1 group by c1,
		 *	where v1 is select 1 from t1
		 * The expression under the last redundant ResultColumn is an IntConstantNode,
		 * not a ColumnReference.
		 * We also do not flatten a subquery if tableProperties is non-null,
		 * as the user is specifying 1 or more properties for the derived table,
		 * which could potentially be lost on the flattening.
		 * RESOLVE - this is too restrictive.
		 */
		if ((gbl == null || gbl.size() == 0) &&
			tableProperties == null &&
		    subquery.flattenableInFromSubquery(fromList))
		{
			/* Set our table map to the subquery's table map. */
			setReferencedTableMap(subquery.getReferencedTableMap());
			return this;
		}

		return extractSubquery(numTables);
	}

	/**
	 * Extract out and return the subquery, with a PRN on top.
	 * (See FromSubquery.preprocess() for more details.)
	 *
	 * @param numTables			The number of tables in the DML Statement
	 *
	 * @return ResultSetNode at top of extracted tree.
	 *
	 * @exception StandardException		Thrown on error
	 */

	public ResultSetNode extractSubquery(int numTables)
		throws StandardException
	{
		JBitSet		  newJBS;
		ResultSetNode newPRN;

		newPRN = (ResultSetNode) getNodeFactory().getNode(
								C_NodeTypes.PROJECT_RESTRICT_NODE,
								subquery,		/* Child ResultSet */
								resultColumns,	/* Projection */
								null,			/* Restriction */
								null,			/* Restriction as PredicateList */
								null,			/* Subquerys in Projection */
								null,			/* Subquerys in Restriction */
								tableProperties,
								getContextManager()	 );

		/* Set up the PRN's referencedTableMap */
		newJBS = new JBitSet(numTables);
		newJBS.set(tableNumber);
		newPRN.setReferencedTableMap(newJBS);
		((FromTable) newPRN).setTableNumber(tableNumber);

		return newPRN;
	}

	/**
	 * Flatten this FSqry into the outer query block. The steps in
	 * flattening are:
	 *	o  Mark all ResultColumns as redundant, so that they are "skipped over"
	 *	   at generate().
	 *	o  Append the wherePredicates to the outer list.
	 *	o  Return the fromList so that the caller will merge the 2 lists 
	 *  RESOLVE - FSqrys with subqueries are currently not flattenable.  Some of
	 *  them can be flattened, however.  We need to merge the subquery list when
	 *  we relax this restriction.
	 *
	 * NOTE: This method returns NULL when flattening RowResultSetNodes
	 * (the node for a VALUES clause).  The reason is that no reference
	 * is left to the RowResultSetNode after flattening is done - the
	 * expressions point directly to the ValueNodes in the RowResultSetNode's
	 * ResultColumnList.
	 *
	 * @param rcl				The RCL from the outer query
	 * @param outerPList	PredicateList to append wherePredicates to.
	 * @param sql				The SubqueryList from the outer query
	 * @param gbl				The group by list, if any
	 *
	 * @return FromList		The fromList from the underlying SelectNode.
	 *
	 * @exception StandardException		Thrown on error
	 */
	public FromList flatten(ResultColumnList rcl,
							PredicateList outerPList,
							SubqueryList sql,
							GroupByList gbl)

			throws StandardException
	{
		FromList	fromList = null;
		SelectNode	selectNode;

		resultColumns.setRedundant();

		subquery.getResultColumns().setRedundant();

		/*
		** RESOLVE: Each type of result set should know how to remap itself.
		*/
		if (subquery instanceof SelectNode)
		{
			selectNode = (SelectNode) subquery;
			fromList = selectNode.getFromList();

			// selectNode.getResultColumns().setRedundant();

			if (selectNode.getWherePredicates().size() > 0)
			{
				outerPList.destructiveAppend(selectNode.getWherePredicates());
			}

			if (selectNode.getWhereSubquerys().size() > 0)
			{
				sql.destructiveAppend(selectNode.getWhereSubquerys());
			}
		}
		else if ( ! (subquery instanceof RowResultSetNode))
		{
			if (SanityManager.DEBUG)
			{
				SanityManager.THROWASSERT("subquery expected to be either a SelectNode or a RowResultSetNode, but is a " + subquery.getClass().getName());
			}
		}

		/* Remap all ColumnReferences from the outer query to this node.
		 * (We replace those ColumnReferences with clones of the matching
		 * expression in the SELECT's RCL.
		 */
		rcl.remapColumnReferencesToExpressions();
		outerPList.remapColumnReferencesToExpressions();
		if (gbl != null)
		{
			gbl.remapColumnReferencesToExpressions();
		}

		return fromList;
	}

	/**
	 * Get the exposed name for this table, which is the name that can
	 * be used to refer to it in the rest of the query.
	 *
	 * @return	The exposed name for this table.
	 */

	public String getExposedName()
	{
		return correlationName;
	}

	/**
	 * Expand a "*" into a ResultColumnList with all of the
	 * result columns from the subquery.
	 * @exception StandardException		Thrown on error
	 */
	public ResultColumnList getAllResultColumns(String allTableName)
			throws StandardException
	{
		ResultColumnList rcList = null;
		TableName		 exposedName;


		if (allTableName != null && ! allTableName.equals(getExposedName()))
		{
			return null;
		}

		/* Cache exposed name for this table.
		 * The exposed name becomes the qualifier for each column
		 * in the expanded list.
		 */
		exposedName = makeTableName(null, correlationName);

		rcList = (ResultColumnList) getNodeFactory().getNode(
										C_NodeTypes.RESULT_COLUMN_LIST,
										getContextManager());

		/* Build a new result column list based off of resultColumns.
		 * NOTE: This method will capture any column renaming due to 
		 * a derived column list.
		 */
		int rclSize = resultColumns.size();
		for (int index = 0; index < rclSize; index++)
		{
			ResultColumn resultColumn = (ResultColumn) resultColumns.elementAt(index);
			ValueNode		 valueNode;
			String			 columnName;

			if (resultColumn.isGenerated())
			{
				continue;
			}

			// Build a ResultColumn/ColumnReference pair for the column //
			columnName = resultColumn.getName();
			boolean isNameGenerated = resultColumn.isNameGenerated();

			/* If this node was generated for a GROUP BY, then tablename for the CR, if any,
			 * comes from the source RC.
			 */
			TableName tableName;

			if (correlationName == null && generatedForGroupByClause)
			{
				tableName = makeTableName(null, resultColumn.getTableName());
			}
			else
			{
				tableName = exposedName;
			}
			valueNode = (ValueNode) getNodeFactory().getNode(
											C_NodeTypes.COLUMN_REFERENCE,
											columnName,
											tableName,
											getContextManager());
			resultColumn = (ResultColumn) getNodeFactory().getNode(
											C_NodeTypes.RESULT_COLUMN,
											columnName,
											valueNode,
											getContextManager());

			resultColumn.setNameGenerated(isNameGenerated);
			// Build the ResultColumnList to return //
			rcList.addResultColumn(resultColumn);
		}
		return rcList;
	}

	/**
	 * Search to see if a query references the specifed table name.
	 *
	 * @param name		Table name (String) to search for.
	 * @param baseTable	Whether or not name is for a base table
	 *
	 * @return	true if found, else false
	 *
	 * @exception StandardException		Thrown on error
	 */
	public boolean referencesTarget(String name, boolean baseTable)
		throws StandardException
	{
		return subquery.referencesTarget(name, baseTable);
	}

	/**
	 * Return true if the node references SESSION schema tables (temporary or permanent)
	 *
	 * @return	true if references SESSION schema tables, else false
	 *
	 * @exception StandardException		Thrown on error
	 */
	public boolean referencesSessionSchema()
		throws StandardException
	{
		return subquery.referencesSessionSchema();
	}

	/**
	 * Bind any untyped null nodes to the types in the given ResultColumnList.
	 *
	 * @param bindingRCL	The ResultColumnList with the types to bind to.
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void bindUntypedNullsToResultColumns(ResultColumnList bindingRCL)
				throws StandardException
	{
		subquery.bindUntypedNullsToResultColumns(bindingRCL);
	}

	/**
	 * Decrement (query block) level (0-based) for this FromTable.
	 * This is useful when flattening a subquery.
	 *
	 * @param decrement	The amount to decrement by.
	 */
	void decrementLevel(int decrement)
	{
		super.decrementLevel(decrement);
		subquery.decrementLevel(decrement);
	}
}
