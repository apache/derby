/*

   Derby - Class org.apache.derby.impl.sql.compile.FromList

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

import org.apache.derby.iapi.services.context.ContextManager;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.sql.compile.CompilerContext;
import org.apache.derby.iapi.sql.compile.Optimizable;
import org.apache.derby.iapi.sql.compile.OptimizableList;
import org.apache.derby.iapi.sql.compile.Optimizer;
import org.apache.derby.iapi.sql.compile.Visitable;
import org.apache.derby.iapi.sql.compile.Visitor;
import org.apache.derby.iapi.sql.compile.C_NodeTypes;

import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.ColumnDescriptor;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.iapi.util.JBitSet;
import org.apache.derby.iapi.util.StringUtil;

import java.util.Properties;
import java.util.Enumeration;
import java.util.Vector;

/**
 * A FromList represents the list of tables in a FROM clause in a DML
 * statement.  It extends QueryTreeNodeVector.
 *
 * @author Jeff Lichtman
 */

public class FromList extends QueryTreeNodeVector implements OptimizableList
{
	Properties	properties;
	// RESOLVE: The default should be false
	boolean		fixedJoinOrder = true;
	// true by default.
	boolean 	useStatistics = true;

	/** Initializer for a FromList */

	public void init(Object optimizeJoinOrder)
	{
		fixedJoinOrder = ! (((Boolean) optimizeJoinOrder).booleanValue());
	}

	/**
	 * Initializer for a FromList
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void init(Object optimizeJoinOrder, Object fromTable)
				throws StandardException
	{
		init(optimizeJoinOrder);

		addFromTable((FromTable) fromTable);
	}

	/*
	 * OptimizableList interface
	 */

	/**
	 * @see org.apache.derby.iapi.sql.compile.OptimizableList#getOptimizable
	 */
	public Optimizable getOptimizable(int index)
	{
		return (Optimizable) elementAt(index);
	}

	/**
	 * @see org.apache.derby.iapi.sql.compile.OptimizableList#setOptimizable
	 */
	public void setOptimizable(int index, Optimizable optimizable)
	{
		setElementAt((FromTable) optimizable, index);
	}

	/** 
	 * @see OptimizableList#verifyProperties
	 * @exception StandardException		Thrown on error
	 */
	public void verifyProperties(DataDictionary dDictionary) throws StandardException
	{
		int size = size();
		for (int index = 0; index < size; index++)
		{
			((Optimizable) elementAt(index)).verifyProperties(dDictionary);
		}
	}


	/**
	 * Add a table to the FROM list.
	 *
	 * @param fromTable	A FromTable to add to the list
	 *
	 * @return	Nothing
	 *
	 * @exception StandardException		Thrown on error
	 */

	public void addFromTable(FromTable fromTable) throws StandardException
	{
		/* Don't worry about checking TableOperatorNodes since
		 * they don't have exposed names.  This will potentially
		 * allow duplicate exposed names in some degenerate cases,
		 * but the binding of the ColumnReferences will catch those
		 * cases with a different error.  If the query does not have
		 * any ColumnReferences from the duplicate exposed name, the
		 * user is executing a really dumb query and we won't throw
		 * and exception - consider it an ANSI extension.
		 */
		if (! (fromTable instanceof TableOperatorNode))
		{
			/* Check for duplicate table name in FROM list */
			int size = size();
			for (int index = 0; index < size; index++)
			{
				if (fromTable.getExposedName().equals
					(((FromTable) elementAt(index)).getExposedName()) )
				{
					throw StandardException.newException(SQLState.LANG_FROM_LIST_DUPLICATE_TABLE_NAME, fromTable.getExposedName());
				}
			}
		}

		addElement(fromTable);
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
		FromTable		fromTable;
		boolean			found = false;

		/* Check for table or VTI name in FROM list */
		int size = size();
		for (int index = 0; index < size; index++)
		{
			fromTable = (FromTable) elementAt(index);

			if (fromTable.referencesTarget(name, baseTable)) 
			{
				found = true;
				break;
			}
		}

		return found;
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
		FromTable		fromTable;
		boolean			found = false;

		/* Check for table or VTI name in FROM list */
		int size = size();
		for (int index = 0; index < size; index++)
		{
			fromTable = (FromTable) elementAt(index);

			if (fromTable.referencesSessionSchema())
			{
				found = true;
				break;
			}
		}

		return found;
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
		boolean			found = false;
		FromTable		fromTable;
		FromTable		result = null;

		int size = size();
		for (int index = 0; index < size; index++)
		{
			fromTable = (FromTable) elementAt(index);

			result = fromTable.getFromTableByName(name, schemaName, exactMatch);

			if (result != null)
			{
				return result;
			}
		}
		return result;
	}

	/**
	 * Bind the tables in this FromList.  This includes looking them up in
	 * the DataDictionary, getting their TableDescriptors and assigning the
	 * table numbers.
	 *
	 * @param dataDictionary	The DataDictionary to use for binding
	 * @param fromListParam		FromList to use/append to.
	 *
	 * @return	Nothing.
	 *
	 * @exception StandardException		Thrown on error
	 */

	public void bindTables(DataDictionary dataDictionary, 
							FromList fromListParam) 
			throws StandardException
	{
		FromTable	fromTable;

		/* Now we bind the tables - this is a 2 step process.
		 * We first bind all of the non-VTIs, then we bind the VTIs.
		 * This enables us to handle the passing of correlation
		 * columns in VTI parameters.
		 * NOTE: We set the table numbers for all of the VTIs in the
		 * first step, when we find them, in order to avoid an ordering
		 * problem with join columns in parameters.
		 */
		int size = size();
		for (int index = 0; index < size; index++)
		{
			fromTable = (FromTable) elementAt(index);
			setElementAt(fromTable.bindNonVTITables(dataDictionary, fromListParam), index);
		}
		for (int index = 0; index < size; index++)
		{
			fromTable = (FromTable) elementAt(index);
			setElementAt(fromTable.bindVTITables(fromListParam), index);
		}
	}

	/**
	 * Bind the expressions in this FromList.  This means 
	 * binding the sub-expressions, as well as figuring out what the return 
	 * type is for each expression.
	 *
	 * @return	Nothing
	 *
	 * @exception StandardException		Thrown on error
	 */

	public void bindExpressions()
					throws StandardException
	{
		FromTable	fromTable;

		int size = size();
		for (int index = 0; index < size; index++)
		{
			fromTable = (FromTable) elementAt(index);
			fromTable.bindExpressions(this);
		}
	}

	/**
	 * Bind the result columns of the ResultSetNodes in this FromList when there is no
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
		FromTable	fromTable;

		int origList = fromListParam.size();
		int size = size();
		for (int index = 0; index < size; index++)
		{
			fromTable = (FromTable) elementAt(index);
			if (fromTable.needsSpecialRCLBinding())
				fromTable.bindResultColumns(fromListParam);

			fromListParam.insertElementAt(fromTable, 0);
		}

		/* Remove all references added here */
		while (fromListParam.size() > origList)
			fromListParam.removeElementAt(0);
	}

	/**
	 * Returns true if any Outer joins present. Used to set Nullability
	 *
	 * @return	True if has any outer joins. False otherwise.
	 */
	public boolean hasOuterJoins()
				throws StandardException
	{
		FromTable	fromTable;

		int size = size();
		for (int index = 0; index < size; index++)
		{
			fromTable = (FromTable) elementAt(index);
			if (fromTable instanceof HalfOuterJoinNode)
				return true;
		}

		return false;
	}

	/**
	 * Expand a "*" into the appropriate ResultColumnList. If the "*"
	 * is unqualified it will expand into a list of all columns in all
	 * of the base tables in the from list, otherwise it will expand
	 * into a list of all of the columns in the base table that matches
	 * the qualification.
	 *
	 * @param allTableName		The qualification on the "*" as a String.
	 *
	 * @return ResultColumnList representing expansion
	 *
	 * @exception StandardException		Thrown on error
	 */
	public ResultColumnList expandAll(String allTableName)
			throws StandardException
	{
		ResultColumnList resultColumnList = null;
		ResultColumnList tempRCList = null;
		boolean			 matchfound = false;
		FromTable	 fromTable;
 
		/* Expand the "*" for the table that matches, if it is qualified 
		 * (allTableName is not null) or for all tables in the list if the 
		 * "*" is not qualified (allTableName is null).
		 */
		int size = size();
		for (int index = 0; index < size; index++)
		{
			fromTable = (FromTable) elementAt(index);
			fromTable.setAllColumnsProjected(true);

			/* We let the FromTable decide if there is a match on
			 * the exposed name.  (A JoinNode will not have an
			 * exposed name, so it will need to pass the info to its
			 * left and right children.)
			 */
			tempRCList = fromTable.getAllResultColumns(allTableName);

			if (tempRCList == null)
			{
				continue;
			}

			/* Expand the column list and append to the list that
			 * we will return.
			 */
			if (resultColumnList == null)
			{
				resultColumnList = tempRCList;
			}
			else
			{
				resultColumnList.nondestructiveAppend(tempRCList);
			}

			/* If the "*" is qualified, then we can stop the
			 * expansion as soon as we find the matching table.
			 */
			if (allTableName != null)
			{
				matchfound = true;
			}
		}

		/* Give an error if the qualification name did not match 
		 * an exposed name 
		 */
		if (resultColumnList == null)
		{
			throw StandardException.newException(SQLState.LANG_EXPOSED_NAME_NOT_FOUND, allTableName);
		}

		return resultColumnList;
	}

	/**
	 * Bind a column reference to one of the tables in this FromList.  The column name
	 * must be unique within the tables in the FromList.  An exception is thrown
	 * if a column name is not unique.
	 *
	 * NOTE: Callers are responsible for ordering the FromList by nesting level,
	 * with tables at the deepest (current) nesting level first.  We will try to 
	 * match against all FromTables at a given nesting level.  If no match is
	 * found at a nesting level, then we proceed to the next level.  We stop
	 * walking the list when the nesting level changes and we have found a match.
	 *
	 * NOTE: If the ColumnReference is qualified, then we will stop the search
	 * at the first nesting level where there is a match on the exposed table name.
	 * For example, s (a, b, c), t (d, e, f)
	 *		select * from s where exists (select * from t s where s.c = a)
	 * will not find a match for s.c, which is the expected ANSI behavior.
	 *
	 * bindTables() must have already been called on this FromList before
	 * calling this method.
	 *
	 * @param columnReference	The ColumnReference describing the column to bind
	 *
	 * @return	ResultColumn	The matching ResultColumn
	 *
	 * @exception StandardException		Thrown on error
	 */

	public ResultColumn bindColumnReference(ColumnReference columnReference)
				throws StandardException
	{
		boolean			columnNameMatch = false;
		boolean			tableNameMatch = false;
		FromTable		fromTable;
		int				currentLevel = -1;
		int				previousLevel = -1;
		ResultColumn	matchingRC = null;
		ResultColumn	resultColumn;
		String			crTableName = columnReference.getTableName();

		/*
		** Find the first table with matching column name.  If there
		** is more than one table with a matching column name at the same
		** nesting level, give an error.
		*/
		int size = size();
		for (int index = 0; index < size; index++)
		{
			fromTable = (FromTable) elementAt(index);

			/* We can stop if we've found a matching column or table name 
			 * at the previous nesting level.
			 */
			currentLevel = fromTable.getLevel();
			if (previousLevel != currentLevel)
			{
				if (columnNameMatch)
				{
					break;
				}

				if (tableNameMatch)
				{
					break;
				}
			}
			/* Simpler to always set previousLevel then to test and set */
			previousLevel = currentLevel;

			resultColumn = fromTable.getMatchingColumn(columnReference);
			if (resultColumn != null)
			{
				if (! columnNameMatch)
				{
					/* TableNumbers are set in the CR in the underlying
					 * FromTable.  This ensures that they get the table
					 * number from the underlying table, not the join node.
					 * This is important for beging able to push predicates 
					 * down through join nodes.
					 */
					matchingRC = resultColumn;
					columnReference.setSource(resultColumn);
					columnReference.setType(resultColumn.getTypeServices());
					/* Set the nesting level at which the CR appears and the nesting level
					 * of its source RC.
					 */
					columnReference.setNestingLevel(((FromTable) elementAt(0)).getLevel());
					columnReference.setSourceLevel(currentLevel);
					columnNameMatch = true;
				}
				else
				{
					throw StandardException.newException(SQLState.LANG_AMBIGUOUS_COLUMN_NAME, 
							 columnReference.getFullColumnName());
				}
			}

			/* Remember if we get a match on the exposed table name, so that
			 * we can stop at the beginning of the next level.
			 */
			tableNameMatch = tableNameMatch || 
						(crTableName != null &&
						 crTableName.equals(fromTable.getExposedName()) );
		}

		return matchingRC;
	}

	/**
	 * Check for (and reject) all ? parameters directly under the ResultColumns.
	 * This is done for SELECT statements.
	 *
	 * @return	Nothing
	 *
	 * @exception StandardException		Thrown if a ? parameter found
	 *									directly under a ResultColumn
	 */

	public void rejectParameters() throws StandardException
	{
		FromTable	fromTable;

		int size = size();
		for (int index = 0; index < size; index++)
		{
			fromTable = (FromTable) elementAt(index);
			fromTable.rejectParameters();
		}
	}

	// This method reorders LOJs in the FROM clause.
	// For now, we process only a LOJ.  For example, "... from LOJ_1, LOJ2 ..."
	// will not be processed. 
	public boolean LOJ_reorderable(int numTables) throws StandardException
	{
		boolean anyChange = false;

		if (size() > 1) return anyChange;

		FromTable ft = (FromTable) elementAt(0);

		anyChange = ft.LOJ_reorderable(numTables);

		return anyChange;
	}

	/**
	 * Preprocess the query tree - this currently means:
	 *	o  Generating a referenced table map for each ResultSetNode.
	 *  o  Putting the WHERE and HAVING clauses in conjunctive normal form (CNF).
	 *  o  Converting the WHERE and HAVING clauses into PredicateLists and
	 *	   classifying them.
	 *  o  Flatten those FromSubqueries which can be flattened.
	 *  o  Ensuring that a ProjectRestrictNode is generated on top of every 
	 *     FromBaseTable and generated in place of every FromSubquery which
	 *	   could not be flattened.  
	 *  o  Pushing single table predicates down to the new ProjectRestrictNodes.
	 *
	 * @param numTables			The number of tables in the DML Statement
	 * @param gbl				The group by list, if any
	 *
	 * @return Nothing.
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void preprocess(int numTables,
						   GroupByList gbl,
						   ValueNode predicateTree)
								throws StandardException
	{
		int size = size();

		/* Preprocess each FromTable in the list */
		for (int index = 0; index < size; index++)
		{
			FromTable ft = (FromTable) elementAt(index);

			/* Transform any outer joins to inner joins where appropriate */
			ft = ft.transformOuterJoins(predicateTree, numTables);
			/* Preprocess this FromTable */
			setElementAt(ft.preprocess(numTables, gbl, this), index);
		}
	}

	/**
	 * Flatten all the FromTables that are flattenable.
	 * RESOLVE - right now we just flatten FromSubqueries.  We
	 * should also flatten flattenable JoinNodes here.
	 *
	 * @param rcl				The RCL from the outer query
	 * @param predicateList		The PredicateList from the outer query
	 * @param sql				The SubqueryList from the outer query
	 * @param gbl				The group by list, if any
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void flattenFromTables(ResultColumnList rcl,
								  PredicateList predicateList,
								  SubqueryList sql,
								  GroupByList gbl)
									throws StandardException
	{
		boolean			flattened = true;
		Vector 			flattenedTableNumbers = new Vector();

		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(rcl != null,
							 "rcl is expected to be non-null");
			SanityManager.ASSERT(predicateList != null,
							 "predicateList is expected to be non-null");
			SanityManager.ASSERT(sql != null,
							 "sql is expected to be non-null");
		}

		/* Loop until all flattenable entries are flattened.
		 * We restart the inner loop after flattening an in place
		 * to simplify the logic and so that we don't have to worry
		 * about walking a list while we are modifying it.
		 */
		while (flattened)
		{
			flattened = false;

			for (int index = 0; index < size() && ! flattened; index++)
			{
				FromTable ft = (FromTable) elementAt(index);

				/* Flatten FromSubquerys and flattenable JoinNodes */
				if ((ft instanceof FromSubquery) ||
					ft.isFlattenableJoinNode())
				{
					//save the table number of the node to be flattened
					flattenedTableNumbers.addElement(new Integer(ft.getTableNumber()));

					/* Remove the node from the list and insert its
					 * FromList here.
					 */
					FromList	 flatteningFL = ft.flatten(
														rcl,
														predicateList,
														sql,
														gbl);
					if (SanityManager.DEBUG)
					{
						SanityManager.ASSERT(flatteningFL == null ||
											 flatteningFL.size() > 0,
							"flatteningFL expected to be null or size > 0");
					}

					if (flatteningFL != null)
					{
						setElementAt(flatteningFL.elementAt(0), index);

						int innerSize = flatteningFL.size();
						for (int inner = 1; inner < innerSize; inner++)
						{
							insertElementAt(flatteningFL.elementAt(inner), index + inner);
						}
					}
					else
					{
						/*
						** If flatten returns null, that means it wants to
						** be removed from the FromList.
						*/
						removeElementAt(index);
					}
					flattened = true;
				}
			}
		}
		
		/* fix up dependency maps for exists base tables since they might have a
		 * dependency on this join node
		 */
		if (flattenedTableNumbers.size() > 0)
		{
			for (int i = 0; i < size(); i++)
			{
				FromTable ft = (FromTable) elementAt(i);
				if (ft instanceof ProjectRestrictNode)
				{
					ResultSetNode rst = ((ProjectRestrictNode)ft).getChildResult();
					if (rst instanceof FromBaseTable)
					{
						((FromBaseTable)rst).clearDependency(flattenedTableNumbers);
					}
				}
			}
		}
	}

	/**
	 * Categorize and push the predicates that are pushable.
	 *
	 * @param predicateList		The query's PredicateList
	 *
	 * @exception StandardException		Thrown on error
	 */
	void pushPredicates(PredicateList predicateList)
			throws StandardException
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(predicateList != null,
							 "predicateList is expected to be non-null");
		}

		/* We can finally categorize each Predicate and try to push them down.
		 * NOTE: The PredicateList may be empty, but that's okay, we still
		 * call pushExpressions() for each entry in the FromList because that's
		 * where any outer join conditions will get pushed down.
		 */
		predicateList.categorize();

		int size = size();
		for (int index = 0; index < size; index++)
		{
			FromTable fromTable = (FromTable) elementAt(index);
			fromTable.pushExpressions(predicateList);
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
		if (SanityManager.DEBUG)
		{
			FromTable	fromTable;

			super.printSubNodes(depth);

			int size = size();
			for (int index = 0; index < size; index++)
			{
				fromTable = (FromTable) elementAt(index);
				fromTable.treePrint(depth + 1);
			}
		}
	}

	/**
	 * Set the (query block) level (0-based) for the FromTables in this
	 * FromList.
	 *
	 * @param level		The query block level for this table.
	 *
	 * @return Nothing
	 */
	public void setLevel(int level)
	{
		int size = size();
		for (int index = 0; index < size; index++)
		{
			FromTable fromTable = (FromTable) elementAt(index);
			fromTable.setLevel(level);
		}
	}

	/**
		Move the mark for result set being the statement's outermost
		result set down into the first table of the from list.
		Generally expect there is only one table at this point.
	 */
	void markStatementResultSet()
	{
		((FromTable) elementAt(0)).markStatementResultSet();
	}

	/**
	 * Get the FromTable from this list which has the specified ResultColumn in
	 * its RCL.
	 *
	 * @param rc	The ResultColumn match on.
	 *
	 * @return FromTable	The matching FromTable.
	 */
	public FromTable getFromTableByResultColumn(ResultColumn rc)
	{
		FromTable	fromTable = null;

		int size = size();
		for (int index = 0; index < size; index++)
		{
			fromTable = (FromTable) elementAt(index);

			if (fromTable.getResultColumns().indexOf(rc) != -1)
			{
				break;
			}
		}

		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(fromTable != null,
				"No matching FromTable found");
		}
		return fromTable;
	}

	/**
	 * Set the Properties list for this FromList.
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void setProperties(Properties props) throws StandardException
	{
		properties = props;

		/*
		** Validate the properties list now.  This is possible because
		** there is nothing in this properties list that relies on binding
		** or optimization to validate.
		*/
		Enumeration enum = properties.keys();
		while (enum.hasMoreElements())
		{
			String key = (String) enum.nextElement();
			String value = (String) properties.get(key);

			if (key.equals("joinOrder"))
			{
				if (StringUtil.SQLEqualsIgnoreCase(value,"fixed"))
				{
					fixedJoinOrder = true;
				}
				else if (StringUtil.SQLEqualsIgnoreCase(value,"unfixed"))
				{
					fixedJoinOrder = false;
				}
				else
				{
					throw StandardException.newException(SQLState.LANG_INVALID_JOIN_ORDER_SPEC, value);
				}
			}
			else if (key.equals("useStatistics"))
			{
				if (StringUtil.SQLEqualsIgnoreCase(value,"true"))
				{
					useStatistics = true;
				}
				else if (StringUtil.SQLEqualsIgnoreCase(value,"false"))
				{
					useStatistics = false;
				}
				else
				{
					throw StandardException.newException(SQLState.LANG_INVALID_STATISTICS_SPEC, value);
				}
			}
			else
			{
				throw StandardException.newException(SQLState.LANG_INVALID_FROM_LIST_PROPERTY, key, value);
			}
		}
	}

	/** @see OptimizableList#reOrder */
	public void reOrder(int[] joinOrder)
	{
		int	posn;

		if (SanityManager.DEBUG)
		{
			if (joinOrder.length != size())
			{
				SanityManager.THROWASSERT("In reOrder(), size of FromList is " + size() + " while size of joinOrder array is " + joinOrder.length);
			}

			/*
			** Determine that the values in the list are unique and in range.
			** The easiest way to determine that they are unique is to add
			** them all up and see whether the result is what's expected
			** for that array size.
			*/
			int sum = 0;
			for (int i = 0; i < joinOrder.length; i++)
			{
				if (joinOrder[i] < 0 || joinOrder[i] > (joinOrder.length - 1))
				{
					SanityManager.THROWASSERT("joinOrder[" + i + "] == " +
											joinOrder[i] +
											" is out of range - must be between 0 and " + 
											(joinOrder.length - 1) +
											" inclusive.");
				}

				sum += joinOrder[i];
			}

			/*
			** The sum of all integers from 0 through n is (n * (n - 1)) / 2.
			*/
			if (sum != ( ( joinOrder.length * (joinOrder.length - 1) ) / 2) )
			{
				String arrayVals = "";
				for (int i = 0; i < joinOrder.length; i++)
					arrayVals = arrayVals + joinOrder[i] + " ";
				SanityManager.THROWASSERT("joinOrder array has some duplicate value: " + arrayVals);
			}
		}

		/* Form a list that's in the order we want */
		QueryTreeNode[] orderedFL = new FromTable[joinOrder.length];
		for (posn = 0; posn < joinOrder.length; posn++)
		{
			/*
			** Get the element at the i'th join order position from the
			** current list and make it the next element of orderedList.
			*/
			orderedFL[posn] = elementAt(joinOrder[posn]);
		}

		/* Now orderedList has been built, so set this list to the same order */
		for (posn = 0; posn < joinOrder.length; posn++)
		{
			setElementAt(orderedFL[posn], posn);
		}
	}

	/** @see OptimizableList#useStatistics */
	public boolean useStatistics()
	{
		return useStatistics;
	}

	/** @see OptimizableList#optimizeJoinOrder */
	public boolean optimizeJoinOrder()
	{
		return ! fixedJoinOrder;
	}

	/** @see OptimizableList#legalJoinOrder */
	public boolean legalJoinOrder(int numTablesInQuery)
	{
		JBitSet			assignedTableMap = new JBitSet(numTablesInQuery);

		int size = size();
		for (int index = 0; index < size; index++)
		{
			FromTable ft = (FromTable) elementAt(index);
			assignedTableMap.or(ft.getReferencedTableMap());
			if ( ! ft.legalJoinOrder(assignedTableMap))
			{
				return false;
			}
		}
		return true;
	}

	/** @see OptimizableList#initAccessPaths */
	public void initAccessPaths(Optimizer optimizer)
	{
		int size = size();
		for (int index = 0; index < size; index++)
		{
			FromTable ft = (FromTable) elementAt(index);
			ft.initAccessPaths(optimizer);
		}
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
		int size = size();
		for (int index = 0; index < size; index++)
		{
			FromTable fromTable = (FromTable) elementAt(index);
			fromTable.bindUntypedNullsToResultColumns(bindingRCL);
		}
	}

	/**
	 * Decrement (query block) level (0-based) for 
	 * all of the tables in this from list.
	 * This is useful when flattening a subquery.
	 *
	 * @param decrement	The amount to decrement by.
	 *
	 * @return Nothing;
	 */
	void decrementLevel(int decrement)
	{
		int size = size();
		for (int index = 0; index < size; index++)
		{
			FromTable fromTable = (FromTable) elementAt(index);
			fromTable.decrementLevel(decrement);

			/* Decrement the level of any CRs in single table
			 * predicates that are interesting to transitive
			 * closure.
			 */
			ProjectRestrictNode prn = (ProjectRestrictNode) fromTable;
			PredicateList pl = prn.getRestrictionList();
			if (pl != null)
			{
				pl.decrementLevel(this, decrement);
			}
		}
	}

	
	/**
	 * This method is used for both subquery flattening and distinct
	 * elimination based on a uniqueness condition.  For subquery
	 * flattening we want to make sure that the query block
	 * will return at most 1 row.  For distinct elimination we
	 * want to make sure that the query block will not return
	 * any duplicates.
	 * This is true if every table in the from list is
	 * (a base table and the set of columns from the table that
	 * are in equality comparisons with expressions that do not include columns 
	 * from the same table is a superset of any unique index
	 * on the table) or an EXISTS FBT.  In addition, at least 1 of the tables
	 * in the list has a set of columns in equality comparisons with expressions
	 * that do not include column references from the same query block
	 * is a superset of a unique index
	 * on that table.  (This ensures that the query block will onlyr
	 * return a single row.)
	 * This method is expected to be called after normalization and
	 * after the from list has been preprocessed.
	 * It can be called both before and after the predicates have
	 * been pulled from the where clause.
	 * The algorithm for this is as follows
	 *
	 *	If any table in the query block is not a base table, give up.
	 * 	For each table in the query
	 *		Ignore exists table since they can only produce one row
	 *
	 *		create a matrix of tables and columns from the table (tableColMap)
	 *		(this is used to keep track of the join columns and constants
	 *		that can be used to figure out whether the rows from a join
	 *		or in a select list are distinct based on unique indexes)
	 *
	 *		create an array of columns from the table(eqOuterCol)
	 *		(this is used to determine that only one row will be returned
	 *		from a join)
	 *			
	 *		if the current table is the table for the result columns
	 *			set the result columns in the eqOuterCol and tableColMap
	 *			(if these columns are a superset of a unique index and
	 *			all joining tables result in only one row, the
	 *			results will be distinct)
	 *		go through all the predicates and update tableColMap  and
	 *		eqOuterCol with join columns and correlation variables, 
	 *		parameters and constants
	 *		since setting constants, correlation variables and parameters,
	 * 		reduces the number of columns required for uniqueness in a 
	 *		multi-column index, they are set for all the tables (if the
	 *		table is not the result table, in this case only the column of the
     *		result table is set)
	 *		join columns are just updated for the column in the row of the
	 *		joining table.
	 *		
	 *		check if the marked columns in tableColMap are a superset of a unique 
	 *			index		
	 *			(This means that the join will only produce 1 row when joined
	 *			with 1 row of another table)
	 *		check that there is a least one table for which the columns in 
	 *			eqOuterCol(i.e. constant values) are a superset of a unique index
	 *			(This quarantees that there will be only one row selected
	 *			from this table).
	 *
	 *	Once all tables have been evaluated, check that all the tables can be
	 * 	joined by unique index or will have only one row
	 *
	 *	
	 *
	 * @param rcl				If non-null, the RCL from the query block.
	 *							If non-null for subqueries, then entry can
	 *							be considered as part of an = comparison.
	 * @param whereClause		The WHERE clause to consider.
	 * @param wherePredicates	The predicates that have already been
	 *							pulled from the WHERE clause.
	 * @param dd				The DataDictionary to use.
	 *
	 * @return	Whether or not query block will return
	 *			at most 1 row for a subquery, no duplicates
	 *			for a distinct.
	 *
	 * @exception StandardException		Thrown on error
	 */
	boolean returnsAtMostSingleRow(ResultColumnList rcl, 
								   ValueNode whereClause, 
								   PredicateList wherePredicates,
								   DataDictionary dd)
		throws StandardException
	{
		boolean			satisfiesOuter = false;
		int[]			tableNumbers;
		ColumnReference	additionalCR = null;

		/* When considering subquery flattening, we are interested
		 * in the 1st (and only) entry in the RCL.  (The RCL will be
		 * null if result column is not of interest for subquery flattening.)
		 * We are interested in all entries in the RCL for distinct
		 * elimination.
		 */
		if (rcl != null)
		{
			ResultColumn rc = (ResultColumn) rcl.elementAt(0);
			if (rc.getExpression() instanceof ColumnReference)
			{
				additionalCR = (ColumnReference) rc.getExpression();
			}
		}

		/* First see if all entries are FromBaseTables.  No point
		 * in continuing if not.
		 */
		int size = size();
		for (int index = 0; index < size; index++)
		{
			FromTable fromTable = (FromTable) elementAt(index);
			if (! (fromTable instanceof ProjectRestrictNode))
			{
				return false;
			}

			ProjectRestrictNode prn = (ProjectRestrictNode) fromTable;

			if (! (prn.getChildResult() instanceof FromBaseTable))
			{
				return false;
			}
		}

		/* Build an array of tableNumbers from this query block.
		 * We will use that array to find out if we have at least
		 * one table with a uniqueness condition based only on
		 * constants, parameters and correlation columns.
		 */
		tableNumbers = getTableNumbers();
		JBitSet[][] tableColMap = new JBitSet[size][size];
		boolean[] oneRow = new boolean[size];
		boolean oneRowResult = false;

		/* See if each table has a uniqueness condition */
		for (int index = 0; index < size; index++)
		{
			ProjectRestrictNode prn = (ProjectRestrictNode) elementAt(index);
			FromBaseTable fbt = (FromBaseTable) prn.getChildResult();

			// Skip over EXISTS FBT since they cannot introduce duplicates
			if (fbt.getExistsBaseTable())
			{
				oneRow[index] = true;
				continue;
			}

			int numColumns = fbt.getTableDescriptor().getNumberOfColumns();
			boolean[] eqOuterCols = new boolean[numColumns + 1];
			int tableNumber = fbt.getTableNumber();
			boolean resultColTable = false;
			for (int i = 0; i < size; i++)
				tableColMap[index][i] = new JBitSet(numColumns + 1);

			if (additionalCR != null &&
				additionalCR.getTableNumber() == tableNumber)
			{
				rcl.recordColumnReferences(eqOuterCols, tableColMap[index], index);
				resultColTable = true;
			}

			/* Now see if there are any equality conditions
			 * of interest in the where clause.
			 */
			if (whereClause != null)
			{
				whereClause.checkTopPredicatesForEqualsConditions(
								tableNumber, eqOuterCols, tableNumbers,
								tableColMap[index], resultColTable);
			}

			/* Now see if there are any equality conditions
			 * of interest in the where predicates.
			 */
			wherePredicates.checkTopPredicatesForEqualsConditions(
								tableNumber, eqOuterCols, tableNumbers,
								tableColMap[index], resultColTable);

			/* Now see if there are any equality conditions
			 * of interest that were already pushed down to the
			 * PRN above the FBT. (Single table predicates.)
			 */
			if (prn.getRestrictionList() != null)
			{
				prn.getRestrictionList().checkTopPredicatesForEqualsConditions(
								tableNumber, eqOuterCols, tableNumbers,
								tableColMap[index], resultColTable);
			}

			/* We can finally check to see if the marked columns
			 * are a superset of any unique index.
			 */
			if (! fbt.supersetOfUniqueIndex(tableColMap[index]))
			{
				return false;
			}
			
			/* Do we have at least 1 table whose equality condition
			 * is based solely on constants, parameters and correlation columns.
			 */
			oneRowResult = fbt.supersetOfUniqueIndex(eqOuterCols);
			if (oneRowResult)
			{
				oneRow[index] = true;
				satisfiesOuter = true;
			}
		}

		/* Have we met all of the criteria */
		if (satisfiesOuter)
		{
			/* check that all the tables are joined by unique indexes 
			 * or only produce 1 row
			 */
			boolean foundOneRow = true;
			while (foundOneRow)
			{
				foundOneRow = false;
				for (int index = 0; index < size; index++)
				{
					if (oneRow[index])
					{
						for (int i = 0; i < size; i++)
						{
							/* unique key join - exists tables already marked as 
							 * 1 row - so don't need to look at them
							 */
							if (!oneRow[i] && tableColMap[i][index].get(0))	
							{
								oneRow[i] = true;
								foundOneRow = true;
							}
						}
					}
				}
			}
			/* does any table produce more than one row */
			for (int index = 0; index < size; index++)
			{
				if (!oneRow[index])
				{
					satisfiesOuter = false;
					break;
				}
			}
		}
		return satisfiesOuter;
	}

	int[] getTableNumbers()
	{
		int size = size();
		int[] tableNumbers = new int[size];
		for (int index = 0; index < size; index++)
		{
			ProjectRestrictNode prn = (ProjectRestrictNode) elementAt(index);
			if (! (prn.getChildResult() instanceof FromTable))
			{
				continue;
			}
			FromTable ft = (FromTable) prn.getChildResult();
			tableNumbers[index] = ft.getTableNumber();
		}

		return tableNumbers;
	}

	/**
	 * Mark all of the FromBaseTables in the list as EXISTS FBTs.
	 * Each EBT has the same dependency list - those tables that are referenced
	 * minus the tables in the from list.
	 *
	 * @param referencedTableMap	The referenced table map.
	 * @param outerFromList			FromList from outer query block
	 * @param isNotExists			Whether or not for NOT EXISTS
	 *
	 * @return Nothing.
	 *
	 * @exception StandardException		Thrown on error
	 */
	void genExistsBaseTables(JBitSet referencedTableMap, FromList outerFromList,
							 boolean isNotExists)
		throws StandardException
	{
		JBitSet			dependencyMap = (JBitSet) referencedTableMap.clone();

		// We currently only flatten single table from lists
		if (SanityManager.DEBUG)
		{
			if (size() != 1)
			{
				SanityManager.THROWASSERT(
					"size() expected to be 1, not " + size());
			}
		}

		/* Create the dependency map */
		int size = size();
		for (int index = 0; index < size; index++)
		{
			ResultSetNode ft = ((ProjectRestrictNode) elementAt(index)).getChildResult();
			if (ft instanceof FromTable)
			{
				dependencyMap.clear(((FromTable) ft).getTableNumber());
			}
		}

		/* Degenerate case - If flattening a non-correlated EXISTS subquery
		 * then we need to make the table that is getting flattened dependendent on
		 * all of the tables in the outer query block.  Gross but true.  Otherwise
		 * that table can get chosen as an outer table and introduce duplicates.
		 * The reason that duplicates can be introduced is that we do special processing
		 * in the join to make sure only one qualified row from the right side is
		 * returned.  If the exists table is on the left, we can return all the
		 * qualified rows. 
		 */
		if (dependencyMap.getFirstSetBit() == -1)
		{
			int outerSize = outerFromList.size();
			for (int outer = 0; outer < outerSize; outer++)
				dependencyMap.or(((FromTable) outerFromList.elementAt(outer)).getReferencedTableMap());
		}

		/* Do the marking */
		for (int index = 0; index < size; index++)
		{
			FromTable fromTable = (FromTable) elementAt(index);
			if (fromTable instanceof ProjectRestrictNode)
			{
				ProjectRestrictNode prn = (ProjectRestrictNode) fromTable;
				if (prn.getChildResult() instanceof FromBaseTable)
				{
					FromBaseTable fbt = (FromBaseTable) prn.getChildResult();
					fbt.setExistsBaseTable(true, (JBitSet) dependencyMap.clone(), isNotExists);
				}
			}
		}
	}

	/**
	 * Get the lock mode for the target of an update statement
	 * (a delete or update).  The update mode will always be row for
	 * CurrentOfNodes.  It will be table if there is no where clause.
	 *
	 * @return	The lock mode
	 */
	public int updateTargetLockMode()
	{
		if (SanityManager.DEBUG)
		{
			if (size() != 1)
			{
				SanityManager.THROWASSERT(
					"size() expected to be 1");
			}
		}
		return ((ResultSetNode) elementAt(0)).updateTargetLockMode();
	}

	/**
	 * Return whether or not the user specified a hash join for any of the 
	 * tables in this list.
	 *
	 * @return	Whether or not the user specified a hash join for any of the 
	 *			tables in this list.
	 */
	boolean hashJoinSpecified()
	{
		int size = size();
		for (int index = 0; index < size; index++)
		{
			FromTable ft = (FromTable) elementAt(index);
			String joinStrategy = ft.getUserSpecifiedJoinStrategy();

			if (joinStrategy != null && StringUtil.SQLToUpperCase(joinStrategy).equals("HASH"))
			{
				return true;
			}
		}

		return false;
	}

	/**
	 * Accept a visitor, and call v.visit()
	 * on child nodes as necessary.  
	 * 
	 * @param v the visitor
	 *
	 * @exception StandardException on error
	 */
	public Visitable accept(Visitor v) 
		throws StandardException
	{
		int size = size();
		for (int index = 0; index < size; index++)
		{
			FromTable fromTable = (FromTable) elementAt(index);
			setElementAt((QueryTreeNode) fromTable.accept(v), index);
		}

		return this;
	}
}
