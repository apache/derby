/*

   Derby - Class org.apache.derby.impl.sql.compile.UnionNode

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

import org.apache.derby.iapi.services.compiler.MethodBuilder;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.sql.compile.CompilerContext;
import org.apache.derby.iapi.sql.compile.Optimizable;
import org.apache.derby.iapi.sql.compile.OptimizablePredicate;
import org.apache.derby.iapi.sql.compile.OptimizablePredicateList;
import org.apache.derby.iapi.sql.compile.Optimizer;
import org.apache.derby.iapi.sql.compile.CostEstimate;
import org.apache.derby.iapi.sql.compile.RowOrdering;
import org.apache.derby.iapi.sql.compile.C_NodeTypes;

import org.apache.derby.iapi.sql.dictionary.ColumnDescriptor;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.DefaultDescriptor;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;
import org.apache.derby.iapi.sql.dictionary.ConglomerateDescriptor;

import org.apache.derby.iapi.types.DataTypeDescriptor;

import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.reference.ClassName;

import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.sql.ResultSet;
import org.apache.derby.iapi.sql.Row;

import org.apache.derby.iapi.types.TypeId;

import org.apache.derby.impl.sql.compile.ActivationClassBuilder;

import org.apache.derby.iapi.util.JBitSet;
import org.apache.derby.iapi.services.classfile.VMOpcode;

import org.apache.derby.catalog.types.DefaultInfoImpl;

import java.util.Properties;

/**
 * A UnionNode represents a UNION in a DML statement.  It contains a boolean
 * telling whether the union operation should eliminate duplicate rows.
 *
 * @author Jeff Lichtman
 */

public class UnionNode extends TableOperatorNode
{
	/**
	** Tells whether to eliminate duplicate rows.  all == TRUE means do
	** not eliminate duplicates, all == FALSE means eliminate duplicates.
	*/
	boolean			all;

	/* Is this a UNION ALL generated for a table constructor. */
	boolean			tableConstructor;

	/* True if this is the top node of a table constructor */
	boolean			topTableConstructor;

	/* Only optimize a UNION once */
	/* Only call addNewNodes() once */
	private boolean addNewNodesCalled;

	private OrderByList orderByList;

	/**
	 * Initializer for a UnionNode.
	 *
	 * @param leftResult		The ResultSetNode on the left side of this union
	 * @param rightResult		The ResultSetNode on the right side of this union
	 * @param all				Whether or not this is a UNION ALL.
	 * @param tableConstructor	Whether or not this is from a table constructor.
	 * @param tableProperties	Properties list associated with the table
	 *
	 * @exception StandardException		Thrown on error
	 */

	public void init(
					Object leftResult,
					Object rightResult,
					Object all,
					Object tableConstructor,
					Object tableProperties)
			throws StandardException
	{
		super.init(leftResult, rightResult, tableProperties);

		this.all = ((Boolean) all).booleanValue();

		/* Is this a UNION ALL for a table constructor? */
		this.tableConstructor = ((Boolean) tableConstructor).booleanValue();

		/* resultColumns cannot be null, so we make a copy of the left RCL
		 * for now.  At bind() time, we need to recopy the list because there
		 * may have been a "*" in the list.  (We will set the names and
		 * column types at that time, as expected.)
		 */
		resultColumns = leftResultSet.getResultColumns().copyListAndObjects();
	}

	/**
	 * Mark this as the top node of a table constructor.
	 */
	public void markTopTableConstructor()
	{
		topTableConstructor = true;
	}

	/**
	 * Tell whether this is a UNION for a table constructor.
	 */
	boolean tableConstructor()
	{
		return tableConstructor;
	}

	/**
	 * Check for (and reject) ? parameters directly under the ResultColumns.
	 * This is done for SELECT statements.  Don't reject parameters that
	 * are in a table constructor - these are allowed, as long as the
	 * table constructor is in an INSERT statement or each column of the
	 * table constructor has at least one non-? column.  The latter case
	 * is checked below, in bindExpressions().
	 *
	 * @return	Nothing
	 *
	 * @exception StandardException		Thrown if a ? parameter found
	 *									directly under a ResultColumn
	 */
	public void rejectParameters() throws StandardException
	{
		if ( ! tableConstructor())
			super.rejectParameters();
	}

	/**
	 * Set the type of column in the result column lists of each
	 * source of this union tree to the type in the given result column list
	 * (which represents the result columns for an insert).
	 * This is only for table constructors that appear in insert statements.
	 *
	 * @param typeColumns	The ResultColumnList containing the desired result
	 *						types.
	 *
	 * @exception StandardException		Thrown on error
	 */
	void setTableConstructorTypes(ResultColumnList typeColumns)
			throws StandardException
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(resultColumns.size() <= typeColumns.size(),
				"More columns in ResultColumnList than in base table.");
		}

		ResultSetNode	rsn;

		/*
		** Should only set types of ? parameters to types of result columns
		** if it's a table constructor.
		*/
		if (tableConstructor())
		{
			/* By looping through the union nodes, we avoid recursion */
			for (rsn = this; rsn instanceof UnionNode; )
			{
				UnionNode union = (UnionNode) rsn;

				/*
				** Assume that table constructors are left-deep trees of UnionNodes
				** with RowResultSet nodes on the right.
				*/
				if (SanityManager.DEBUG)
					SanityManager.ASSERT(
						union.rightResultSet instanceof RowResultSetNode,
						"A " + union.rightResultSet.getClass().getName() +
						" is on the right of a union in a table constructor");

				((RowResultSetNode) union.rightResultSet).setTableConstructorTypes(
																typeColumns);

				rsn = union.leftResultSet;
			}

			/* The last node on the left should be a result set node */
			if (SanityManager.DEBUG)
				SanityManager.ASSERT(rsn instanceof RowResultSetNode,
					"A " + rsn.getClass().getName() +
					" is at the left end of a table constructor");

			((RowResultSetNode) rsn).setTableConstructorTypes(typeColumns);
		}
	}

	/*
	 *  Optimizable interface
	 */

	/**
	 * @see org.apache.derby.iapi.sql.compile.Optimizable#optimizeIt
	 *
	 * @exception StandardException		Thrown on error
	 */
	public CostEstimate optimizeIt(Optimizer optimizer,
							OptimizablePredicateList predList,
							CostEstimate outerCost,
							RowOrdering rowOrdering)
			throws StandardException
	{
		/*
		** RESOLVE: Most types of Optimizables only implement estimateCost(),
		** and leave it up to optimizeIt() in FromTable to figure out the
		** total cost of the join.  For unions, though, we want to figure out
		** the best plan for the sources knowing how many outer rows there are -
		** it could affect their strategies significantly.  So we implement
		** optimizeIt() here, which overrides the optimizeIt() in FromTable.
		** This assumes that the join strategy for which this union node is
		** the inner table is a nested loop join, which will not be a valid
		** assumption when we implement other strategies like materialization
		** (hash join can work only on base tables).
		*/

		/* optimize() both resultSets */
		/* RESOLVE - don't try to push predicates through for now */
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

		CostEstimate costEstimate = getCostEstimate(optimizer);

		/* The cost is the sum of the two child costs */
		costEstimate.setCost(leftResultSet.getCostEstimate().getEstimatedCost(),
							 leftResultSet.getCostEstimate().rowCount(),
							 leftResultSet.getCostEstimate().singleScanRowCount() +
							 rightResultSet.getCostEstimate().singleScanRowCount());

		costEstimate.add(rightResultSet.costEstimate, costEstimate);

		/*
		** Get the cost of this result set in the context of the whole plan.
		*/
		getCurrentAccessPath().
			getJoinStrategy().
				estimateCost(
							this,
							predList,
							(ConglomerateDescriptor) null,
							outerCost,
							optimizer,
							costEstimate
							);

		optimizer.considerCost(this, predList, costEstimate, outerCost);

		return costEstimate;
	}

	/**
	 * @see Optimizable#modifyAccessPath
	 *
	 * @exception StandardException		Thrown on error
	 */
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
	public ResultSetNode modifyAccessPaths() throws StandardException
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
		ResultSetNode treeTop = this;

		/* Only call addNewNodes() once */
		if (addNewNodesCalled)
		{
			return this;
		}

		addNewNodesCalled = true;

		/* RESOLVE - We'd like to generate any necessary NormalizeResultSets
		 * above our children here, in the tree.  However, doing so causes
		 * the following query to fail because the where clause goes against
		 * the NRS instead of the Union:
		 *		SELECT TABLE_TYPE
		 *		FROM SYS.SYSTABLES, 
		 *			(VALUES ('T','TABLE') ,
		 *				('S','SYSTEM TABLE') , ('V', 'VIEW')) T(TTABBREV,TABLE_TYPE) 
		 *		WHERE TTABBREV=TABLETYPE;
		 * Thus, we are forced to skip over generating the nodes in the tree
		 * and directly generate the execution time code in generate() instead.
		 * This solves the problem for some unknown reason.
		 */

		/* Simple solution (for now) to eliminating duplicates - 
		 * generate a distinct above the union.
		 */
		if (! all)
		{
			/* We need to generate a NormalizeResultSetNode above us if the column
			 * types and lengths don't match.  (We need to do it here, since they
			 * will end up agreeing in the PRN, which will be the immediate
			 * child of the DistinctNode, which means that the NormalizeResultSet
			 * won't get generated above the PRN.)
			 */
			if (! columnTypesAndLengthsMatch())
			{
				treeTop = genNormalizeResultSetNode(this, false);	
			}

			treeTop = (ResultSetNode) getNodeFactory().getNode(
							C_NodeTypes.DISTINCT_NODE,
							treeTop.genProjectRestrict(),
							Boolean.FALSE,
							tableProperties,
							getContextManager());
			/* HACK - propagate our table number up to the new DistinctNode
			 * so that arbitrary hash join will work correctly.  (Otherwise it
			 * could have a problem dividing up the predicate list at the end
			 * of modifyAccessPath() because the new child of the PRN above
			 * us would have a tableNumber of -1 instead of our tableNumber.)
			 */
			((FromTable)treeTop).setTableNumber(tableNumber);
			treeTop.setReferencedTableMap((JBitSet) referencedTableMap.clone());
			all = true;
		}

		/* Generate the OrderByNode if a sort is still required for
		 * the order by.
		 */
		if (orderByList != null)
		{
			treeTop = (ResultSetNode) getNodeFactory().getNode(
											C_NodeTypes.ORDER_BY_NODE,
											treeTop,
											orderByList,
											tableProperties,
											getContextManager());
		}
		return treeTop;
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
			 	"tableConstructor: " + tableConstructor + "\n" +
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
		super.bindExpressions(fromListParam);

		/*
		** Each ? parameter in a table constructor that is not in an insert
		** statement takes its type from the first non-? in its column
		** of the table constructor.  It's an error to have a column that
		** has all ?s.  Do this only for the top of the table constructor
		** list - we don't want to do this for every level of union node
		** in the table constructor.  Also, don't do this for an INSERT -
		** the types of the ? parameters come from the columns being inserted
		** into in that case.
		*/
		if (topTableConstructor && ( ! insertSource) )
		{
			/*
			** Step through all the rows in the table constructor to
			** get the type of the first non-? in each column.
			*/
			DataTypeDescriptor[]	types =
				new DataTypeDescriptor[leftResultSet.getResultColumns().size()];
			
			ResultSetNode	rsn;
			int				numTypes = 0;

			/* By looping through the union nodes, we avoid recursion */
			for (rsn = this; rsn instanceof UnionNode; )
			{
				UnionNode		union = (UnionNode) rsn;

				/*
				** Assume that table constructors are left-deep trees of
				** UnionNodes with RowResultSet nodes on the right.
				*/
				if (SanityManager.DEBUG)
					SanityManager.ASSERT(
					 union.rightResultSet instanceof RowResultSetNode,
					 "A " + union.rightResultSet.getClass().getName() +
					 " is on the right side of a union in a table constructor");

				RowResultSetNode	rrsn =
										(RowResultSetNode) union.rightResultSet;

				numTypes += getParamColumnTypes(types, rrsn);

				rsn = union.leftResultSet;
			}

			/* The last node on the left should be a result set node */
			if (SanityManager.DEBUG)
				SanityManager.ASSERT(rsn instanceof RowResultSetNode);

			numTypes += getParamColumnTypes(types, (RowResultSetNode) rsn);

			/* Are there any columns that are all ? parameters? */
			if (numTypes < types.length)
			{
			  throw StandardException.newException(SQLState.LANG_TABLE_CONSTRUCTOR_ALL_PARAM_COLUMN);
			}

			/*
			** Loop through the nodes again. This time, look for parameter
			** nodes, and give them the type from the type array we just
			** constructed.
			*/
			for (rsn = this; rsn instanceof UnionNode; )
			{
				UnionNode		union = (UnionNode) rsn;
				RowResultSetNode	rrsn =
										(RowResultSetNode) union.rightResultSet;

				setParamColumnTypes(types, rrsn);

				rsn = union.leftResultSet;
			}

			setParamColumnTypes(types, (RowResultSetNode) rsn);
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
			throw StandardException.newException(SQLState.LANG_UNION_UNMATCHED_COLUMNS);
		}

		/* We need to recreate resultColumns for this node, since there
		 * may have been 1 or more *'s in the left's SELECT list.
		 */
		resultColumns = leftResultSet.getResultColumns().copyListAndObjects();

		/* Create new expressions with the dominant types after verifying
		 * union compatibility between left and right sides.
		 */
		resultColumns.setUnionResultExpression(rightResultSet.getResultColumns(), tableNumber, level);
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
	 * @return The generated ProjectRestrictNode atop the original FromTable.
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
	 * Generate the code for this UnionNode.
	 *
	 * @exception StandardException		Thrown on error
     */
	public void generate(ActivationClassBuilder acb,
								MethodBuilder mb)
							throws StandardException
	{
		/*  By the time we get here we should be a union all.
		 *  (We created a DistinctNode above us, if needed,
		 *  to eliminate the duplicates earlier.)
		 */
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(all,
				"all expected to be true");
		}

		/* Get the next ResultSet #, so that we can number this ResultSetNode, its
		 * ResultColumnList and ResultSet.
		 */
		assignResultSetNumber();

		// build up the tree.

		acb.pushGetResultSetFactoryExpression(mb); // instance for getUnionResultSet


		/* Generate the left and right ResultSets */
		leftResultSet.generate(acb, mb);

		/* Do we need a NormalizeResultSet above the left ResultSet? */
		if (! resultColumns.isExactTypeAndLengthMatch(leftResultSet.getResultColumns()))
		{
			acb.pushGetResultSetFactoryExpression(mb);
			mb.swap();
			generateNormalizationResultSet(acb, mb, 
													getCompilerContext().getNextResultSetNumber(),
													makeResultDescription()
													);
		}

		rightResultSet.generate(acb, mb);

		/* Do we need a NormalizeResultSet above the right ResultSet? */
		if (! resultColumns.isExactTypeAndLengthMatch(rightResultSet.getResultColumns()))
		{
			acb.pushGetResultSetFactoryExpression(mb);
			mb.swap();
			generateNormalizationResultSet(acb, mb,
													getCompilerContext().getNextResultSetNumber(),
													makeResultDescription()
													);
		}

		/* Generate the UnionResultSet:
		 *	arg1: leftExpression - Expression for leftResultSet
		 *	arg2: rightExpression - Expression for rightResultSet
		 *  arg3: Activation
		 *  arg4: resultSetNumber
		 *  arg5: estimated row count
		 *  arg6: estimated cost
		 *  arg7: close method
		 */


		acb.pushThisAsActivation(mb);
		mb.push(resultSetNumber);
		mb.push(costEstimate.rowCount());
		mb.push(costEstimate.getEstimatedCost());
		closeMethodArgument(acb, mb);

		mb.callMethod(VMOpcode.INVOKEINTERFACE, (String) null, "getUnionResultSet", ClassName.NoPutResultSet, 7);
	}
}
