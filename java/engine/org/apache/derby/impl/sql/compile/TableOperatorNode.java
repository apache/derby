/*

   Derby - Class org.apache.derby.impl.sql.compile.TableOperatorNode

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

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.compile.CompilerContext;
import org.apache.derby.iapi.sql.compile.Optimizable;
import org.apache.derby.iapi.sql.compile.Visitable;
import org.apache.derby.iapi.sql.compile.Visitor;
import org.apache.derby.iapi.sql.compile.Optimizer;
import org.apache.derby.iapi.sql.compile.OptimizableList;
import org.apache.derby.iapi.sql.compile.CostEstimate;
import org.apache.derby.iapi.sql.compile.OptimizerFactory;
import org.apache.derby.iapi.sql.compile.RequiredRowOrdering;
import org.apache.derby.iapi.sql.compile.C_NodeTypes;

import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;

import org.apache.derby.iapi.util.JBitSet;

import java.util.Properties;

/**
 * A TableOperatorNode represents a relational operator like UNION, INTERSECT,
 * JOIN, etc. that takes two tables as parameters and returns a table.  The
 * parameters it takes are represented as ResultSetNodes.
 *
 * Currently, all known table operators are binary operators, so there are no
 * subclasses of this node type called "BinaryTableOperatorNode" and
 * "UnaryTableOperatorNode".
 *
 * @author Jeff Lichtman
 */

public abstract class TableOperatorNode extends FromTable
{
	boolean			nestedInParens;
	ResultSetNode	leftResultSet;
	ResultSetNode	rightResultSet;
	Optimizer		leftOptimizer;
	Optimizer		rightOptimizer;
	private boolean 	leftModifyAccessPathsDone;
	private boolean 	rightModifyAccessPathsDone;

	/**
	 * Initializer for a TableOperatorNode.
	 *
	 * @param leftResultSet		The ResultSetNode on the left side of this node
	 * @param rightResultSet	The ResultSetNode on the right side of this node
	 * @param tableProperties	Properties list associated with the table
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void init(Object leftResultSet,
							 Object rightResultSet,
							 Object tableProperties)
				throws StandardException
	{
		/* correlationName is always null */
		init(null, tableProperties);
		this.leftResultSet = (ResultSetNode) leftResultSet;
		this.rightResultSet = (ResultSetNode) rightResultSet;
	}

	/**
	 * @see Optimizable#modifyAccessPath
	 *
	 * @exception StandardException		Thrown on error
	 */
	public Optimizable modifyAccessPath(JBitSet outerTables) throws StandardException
	{
		boolean callModifyAccessPaths = false;

		if (leftResultSet instanceof FromTable)
		{
			if (leftOptimizer != null)
				leftOptimizer.modifyAccessPaths();
			else
			{
				leftResultSet = 
					(ResultSetNode)
						((FromTable) leftResultSet).modifyAccessPath(outerTables);
			}
			leftModifyAccessPathsDone = true;
		}
		else
		{
			callModifyAccessPaths = true;
		}

		if (rightResultSet instanceof FromTable)
		{
			if (rightOptimizer != null)
				rightOptimizer.modifyAccessPaths();
			else
			{
				rightResultSet = 
					(ResultSetNode)
						((FromTable) rightResultSet).modifyAccessPath(outerTables);
			}
			rightModifyAccessPathsDone = true;
		}
		else
		{
			callModifyAccessPaths = true;
		}

		if (callModifyAccessPaths)
		{
			return (Optimizable) modifyAccessPaths();
		}
		return this;
	}

	/** @see Optimizable#verifyProperties 
	 * @exception StandardException		Thrown on error
	 */
	public void verifyProperties(DataDictionary dDictionary)
		throws StandardException
	{
		if (leftResultSet instanceof Optimizable)
		{
			((Optimizable) leftResultSet).verifyProperties(dDictionary);
		}
		if (rightResultSet instanceof Optimizable)
		{
			((Optimizable) rightResultSet).verifyProperties(dDictionary);
		}

		super.verifyProperties(dDictionary);
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
			return "nestedInParens: " + nestedInParens + "\n" +
				leftResultSet.toString() + "\n" +
				rightResultSet.toString() + "\n" + 
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
		if (SanityManager.DEBUG)
		{
			super.printSubNodes(depth);

			if (leftResultSet != null)
			{
				printLabel(depth, "leftResultSet: ");
				leftResultSet.printSubNodes(depth + 1);
			}

			if (rightResultSet != null)
			{
				printLabel(depth, "rightResultSet: ");
				rightResultSet.printSubNodes(depth + 1);
			}
		}
	}

	/**
	 * Get the leftResultSet from this node.
	 *
	 * @return ResultSetNode	The leftResultSet from this node.
	 */
	public ResultSetNode getLeftResultSet()
	{
		return leftResultSet;
	}

	/**
	 * Get the rightResultSet from this node.
	 *
	 * @return ResultSetNode	The rightResultSet from this node.
	 */
	public ResultSetNode getRightResultSet()
	{
		return rightResultSet;
	}

	public ResultSetNode getLeftmostResultSet()
	{
		if (leftResultSet instanceof TableOperatorNode)
		{
			return ((TableOperatorNode) leftResultSet).getLeftmostResultSet();
		}
		else
		{
			return leftResultSet;
		}
	}

	public void setLeftmostResultSet(ResultSetNode newLeftResultSet)
	{
		if (leftResultSet instanceof TableOperatorNode)
		{
			((TableOperatorNode) leftResultSet).setLeftmostResultSet(newLeftResultSet);
		}
		else
		{
			this.leftResultSet = newLeftResultSet;
		}
	}

	/**
	 * Set the (query block) level (0-based) for this FromTable.
	 *
	 * @param level		The query block level for this FromTable.
	 *
	 * @return Nothing
	 */
	public void setLevel(int level)
	{
		super.setLevel(level);
		if (leftResultSet instanceof FromTable)
		{
			((FromTable) leftResultSet).setLevel(level);
		}
		if (rightResultSet instanceof FromTable)
		{
			((FromTable) rightResultSet).setLevel(level);
		}
	}

	/**
	 * Return the exposed name for this table, which is the name that
	 * can be used to refer to this table in the rest of the query.
	 *
	 * @return	The exposed name for this table.
	 */

	public String getExposedName()
	{
		return null;
	}

	/**
	 * Mark whether or not this node is nested in parens.  (Useful to parser
	 * since some trees get created left deep and others right deep.)
	 *
	 * @param nestedInParens	Whether or not this node is nested in parens.
	 *
	 * @return Nothing.
	 */
	public void setNestedInParens(boolean nestedInParens)
	{
		this.nestedInParens = nestedInParens;
	}

	/**
	 * Return whether or not the table operator for this node was
	 * nested in parens in the query.  (Useful to parser
	 * since some trees get created left deep and others right deep.)
	 *
	 * @return boolean		Whether or not this node was nested in parens.
	 */
	public boolean getNestedInParens()
	{
		return nestedInParens;
	}


	/**
	 * Bind the non VTI tables in this TableOperatorNode. This means getting
	 * their TableDescriptors from the DataDictionary.
	 * We will build an unbound RCL for this node.  This RCL must be
	 * "bound by hand" after the underlying left and right RCLs
	 * are bound.
	 *
	 * @param dataDictionary	The DataDictionary to use for binding
	 * @param fromListParam		FromList to use/append to.
	 *
	 * @return	ResultSetNode		Returns this.
	 *
	 * @exception StandardException		Thrown on error
	 */

	public ResultSetNode bindNonVTITables(DataDictionary dataDictionary, 
						  FromList fromListParam) 
							throws StandardException
	{
		leftResultSet = leftResultSet.bindNonVTITables(dataDictionary, fromListParam);
		rightResultSet = rightResultSet.bindNonVTITables(dataDictionary, fromListParam);
		/* Assign the tableNumber */
		if (tableNumber == -1)  // allow re-bind, in which case use old number
			tableNumber = getCompilerContext().getNextTableNumber();

		return this;
	}

	/**
	 * Bind the VTI tables in this TableOperatorNode. This means getting
	 * their TableDescriptors from the DataDictionary.
	 * We will build an unbound RCL for this node.  This RCL must be
	 * "bound by hand" after the underlying left and right RCLs
	 * are bound.
	 *
	 * @param fromListParam		FromList to use/append to.
	 *
	 * @return	ResultSetNode		Returns this.
	 *
	 * @exception StandardException		Thrown on error
	 */

	public ResultSetNode bindVTITables(FromList fromListParam) 
							throws StandardException
	{
		leftResultSet = leftResultSet.bindVTITables(fromListParam);
		rightResultSet = rightResultSet.bindVTITables(fromListParam);

		return this;
	}

	/**
	 * Bind the expressions under this TableOperatorNode.  This means
	 * binding the sub-expressions, as well as figuring out what the
	 * return type is for each expression.
	 *
	 * @return	Nothing
	 *
	 * @exception StandardException		Thrown on error
	 */

	public void bindExpressions(FromList fromListParam)
				throws StandardException
	{
		/*
		** Parameters not allowed in select list of either side of union,
		** except when the union is for a table constructor.
		*/
		if ( ! (this instanceof UnionNode) ||
			 ! ((UnionNode) this).tableConstructor())
		{
			leftResultSet.rejectParameters();
			rightResultSet.rejectParameters();
		}

		leftResultSet.bindExpressions(fromListParam);
		rightResultSet.bindExpressions(fromListParam);
	}

	/**
	 * Check for (and reject) ? parameters directly under the ResultColumns.
	 * This is done for SELECT statements.  For TableOperatorNodes, we
	 * simply pass the check through to the left and right children.
	 *
	 * @return	Nothing
	 *
	 * @exception StandardException		Thrown if a ? parameter found
	 *									directly under a ResultColumn
	 */

	public void rejectParameters() throws StandardException
	{
		leftResultSet.rejectParameters();
		rightResultSet.rejectParameters();
	}

	/**
	 * Bind the expressions in this ResultSetNode if it has tables.  This means binding the
	 * sub-expressions, as well as figuring out what the return type is for
	 * each expression.
	 *
	 * @param fromListParam		FromList to use/append to.
	 *
	 * @return	Nothing
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void bindExpressionsWithTables(FromList fromListParam)
					throws StandardException
	{
		/*
		** Parameters not allowed in select list of either side of union,
		** except when the union is for a table constructor.
		*/
		if ( ! (this instanceof UnionNode) ||
			 ! ((UnionNode) this).tableConstructor())
		{
			leftResultSet.rejectParameters();
			rightResultSet.rejectParameters();
		}

		leftResultSet.bindExpressionsWithTables(fromListParam);
		rightResultSet.bindExpressionsWithTables(fromListParam);
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
		leftResultSet.bindResultColumns(fromListParam);
		rightResultSet.bindResultColumns(fromListParam);
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
		leftResultSet.bindResultColumns(targetTableDescriptor,
										targetVTI,
										targetColumnList,
										statement, fromListParam);
		rightResultSet.bindResultColumns(targetTableDescriptor,
										targetVTI,
										targetColumnList,
										statement, fromListParam);
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
		FromTable result = leftResultSet.getFromTableByName(name, schemaName, exactMatch);

		/* We search both sides for a TableOperatorNode (join nodes)
		 * but only the left side for a UnionNode.
		 */
		if (result == null)
		{
			result = rightResultSet.getFromTableByName(name, schemaName, exactMatch);
		}
		return result;
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
	 * @return The generated ProjectRestrictNode atop the original FromTable.
	 *
	 * @exception StandardException		Thrown on error
	 */

	public ResultSetNode preprocess(int numTables,
									GroupByList gbl,
									FromList fromList)
								throws StandardException
	{
		leftResultSet = leftResultSet.preprocess(numTables, gbl, fromList);
		/* If leftResultSet is a FromSubquery, then we must explicitly extract
		 * out the subquery (flatten it).  (SelectNodes have their own
		 * method of flattening them.
		 */
		if (leftResultSet instanceof FromSubquery)
		{
			leftResultSet = ((FromSubquery) leftResultSet).extractSubquery(numTables);
		}
		rightResultSet = rightResultSet.preprocess(numTables, gbl, fromList);
		/* If rightResultSet is a FromSubquery, then we must explicitly extract
		 * out the subquery (flatten it).  (SelectNodes have their own
		 * method of flattening them.
		 */
		if (rightResultSet instanceof FromSubquery)
		{
			rightResultSet = ((FromSubquery) rightResultSet).extractSubquery(numTables);
		}

		/* Build the referenced table map (left || right) */
		referencedTableMap = (JBitSet) leftResultSet.getReferencedTableMap().clone();
		referencedTableMap.or((JBitSet) rightResultSet.getReferencedTableMap());
		referencedTableMap.set(tableNumber);

		/* Only generate a PRN if this node is not a flattenable join node. */
		if (isFlattenableJoinNode())
		{
			return this;
		}
		else
		{
			/* Project out any unreferenced RCs before we generate the PRN.
			 * NOTE: We have to do this at the end of preprocess since it has to 
			 * be from the bottom up.  We can't do it until the join expression is 
			 * bound, since the join expression may contain column references that
			 * are not referenced anywhere else above us.
			 */
            projectResultColumns();
			return genProjectRestrict(numTables);
		}
	}

    /**
     * Find the unreferenced result columns and project them out. This is used in pre-processing joins
     * that are not flattened into the where clause.
     */
    void projectResultColumns() throws StandardException
    {
        resultColumns.doProjection();
    }
    
    /**
     * Set the referenced columns in the column list if it may not be correct.
     */
    void setReferencedColumns()
    {}
    
	/**
	 * Optimize a TableOperatorNode. 
	 *
	 * @param dataDictionary	The DataDictionary to use for optimization
	 * @param predicateList		The PredicateList to apply.
	 *
	 * @return	ResultSetNode	The top of the optimized query tree
	 *
	 * @exception StandardException		Thrown on error
	 */
	public ResultSetNode optimize(DataDictionary dataDictionary,
								  PredicateList predicateList,
								  double outerRows)
				throws StandardException
	{
		/* Get an optimizer, so we can get a cost structure */
		Optimizer optimizer =
							getOptimizer(
								(FromList) getNodeFactory().getNode(
									C_NodeTypes.FROM_LIST,
									getNodeFactory().doJoinOrderOptimization(),
									this,
									getContextManager()),
									predicateList,
									dataDictionary,
									(RequiredRowOrdering) null);

		costEstimate = optimizer.newCostEstimate();

		/* RESOLVE: This is just a stub for now */
		leftResultSet = leftResultSet.optimize(
											dataDictionary,
											predicateList,
											outerRows);
		rightResultSet = rightResultSet.optimize(
											dataDictionary,
											predicateList,
											outerRows);

		/* The cost is the sum of the two child costs */
		costEstimate.setCost(leftResultSet.getCostEstimate().getEstimatedCost(),
							 leftResultSet.getCostEstimate().rowCount(),
							 leftResultSet.getCostEstimate().singleScanRowCount() +
							 rightResultSet.getCostEstimate().singleScanRowCount());

		costEstimate.add(rightResultSet.costEstimate, costEstimate);

		return this;
	}

	/**
	 * @see ResultSetNode#modifyAccessPaths
	 *
	 * @exception StandardException		Thrown on error
	 */
	public ResultSetNode modifyAccessPaths() throws StandardException
	{
		/* Beetle 4454 - union all with another union all would modify access
		 * paths twice causing NullPointerException, make sure we don't
		 * do this again, if we have already done it in modifyAccessPaths(outertables)
		 */
		if (!leftModifyAccessPathsDone)
		{
			if (leftOptimizer != null)
				leftOptimizer.modifyAccessPaths();
			else
				leftResultSet = leftResultSet.modifyAccessPaths();
		}
		if (!rightModifyAccessPathsDone)
		{
			if (rightOptimizer != null)
				rightOptimizer.modifyAccessPaths();
			else
				rightResultSet = rightResultSet.modifyAccessPaths();
		}
		return this;
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
		return leftResultSet.referencesTarget(name, baseTable) ||
			   rightResultSet.referencesTarget(name, baseTable);
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
		return leftResultSet.referencesSessionSchema() ||
			   rightResultSet.referencesSessionSchema();
	}

	/** 
	 * Optimize a source result set to this table operator.
	 *
	 * @exception StandardException		Thrown on error
	 */
	protected ResultSetNode optimizeSource(
							Optimizer optimizer,
							ResultSetNode sourceResultSet,
							PredicateList predList,
							CostEstimate outerCost)
			throws StandardException
	{
		ResultSetNode	retval;

		if (sourceResultSet instanceof FromTable)
		{
			FromList optList = (FromList) getNodeFactory().getNode(
									C_NodeTypes.FROM_LIST,
									getNodeFactory().doJoinOrderOptimization(),
									sourceResultSet,
									getContextManager());

			/* If there is no predicate list, create an empty one */
			if (predList == null)
				predList = (PredicateList) getNodeFactory().getNode(
												C_NodeTypes.PREDICATE_LIST,
												getContextManager());

			LanguageConnectionContext lcc = getLanguageConnectionContext();
			OptimizerFactory optimizerFactory = lcc.getOptimizerFactory();
			optimizer = optimizerFactory.getOptimizer(optList,
													predList,
													getDataDictionary(),
													(RequiredRowOrdering) null,
													getCompilerContext().getNumTables(),
													  lcc);

			if (sourceResultSet == leftResultSet)
			{
				leftOptimizer = optimizer;
			}
			else if (sourceResultSet == rightResultSet)
			{
				rightOptimizer = optimizer;
			}
			else
			{
				if (SanityManager.DEBUG)
					SanityManager.THROWASSERT("Result set being optimized is neither left nor right");
			}
			
			/*
			** Set the estimated number of outer rows from the outer part of
			** the plan.
			*/
			optimizer.setOuterRows(outerCost.rowCount());

			/* Optimize the underlying result set */
			while (optimizer.getNextPermutation())
			{
				while (optimizer.getNextDecoratedPermutation())
				{
					optimizer.costPermutation();
				}
			}

			retval = sourceResultSet;
		}
		else
		{
			retval = sourceResultSet.optimize(
										optimizer.getDataDictionary(),
										predList,
										outerCost.rowCount());
		}

		return retval;
	}

	/**
	 * Decrement (query block) level (0-based) for 
	 * all of the tables in this ResultSet tree.
	 * This is useful when flattening a subquery.
	 *
	 * @param decrement	The amount to decrement by.
	 *
	 * @return Nothing;
	 */
	void decrementLevel(int decrement)
	{
		leftResultSet.decrementLevel(decrement);
		rightResultSet.decrementLevel(decrement);
	}

	/**
	 * Replace any DEFAULTs with the associated tree for the default.
	 *
	 * @param ttd	The TableDescriptor for the target table.
	 * @param tcl	The RCL for the target table.
	 *
	 * @exception StandardException		Thrown on error
	 */
	void replaceDefaults(TableDescriptor ttd, ResultColumnList tcl) 
		throws StandardException
	{
		leftResultSet.replaceDefaults(ttd, tcl);
		rightResultSet.replaceDefaults(ttd, tcl);
	}

	/**
	 * Notify the underlying result set tree that the result is
	 * ordering dependent.  (For example, no bulk fetch on an index
	 * if under an IndexRowToBaseRow.)
	 *
	 * @return Nothing.
	 */
	void markOrderingDependent()
	{
		leftResultSet.markOrderingDependent();
		rightResultSet.markOrderingDependent();
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
		if (v.skipChildren(this))
		{
			return v.visit(this);
		}

		Visitable returnNode = super.accept(v);

		if (leftResultSet != null && !v.stopTraversal())
		{
			leftResultSet = (ResultSetNode)leftResultSet.accept(v);
		}
		if (rightResultSet != null && !v.stopTraversal())
		{
			rightResultSet = (ResultSetNode)rightResultSet.accept(v);
		}
		return returnNode;
	}

	/** 
	 * apparently something special needs to be done for me....
	 */
	public boolean needsSpecialRCLBinding()
	{
		return true;
	}
}
