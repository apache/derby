/*

   Derby - Class org.apache.derby.impl.sql.compile.RowResultSetNode

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

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.shared.common.reference.ClassName;
import org.apache.derby.shared.common.reference.SQLState;
import org.apache.derby.iapi.services.classfile.VMOpcode;
import org.apache.derby.iapi.services.compiler.MethodBuilder;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derby.iapi.sql.compile.CostEstimate;
import org.apache.derby.iapi.sql.compile.Optimizable;
import org.apache.derby.iapi.sql.compile.OptimizablePredicateList;
import org.apache.derby.iapi.sql.compile.Optimizer;
import org.apache.derby.iapi.sql.compile.RequiredRowOrdering;
import org.apache.derby.iapi.sql.compile.RowOrdering;
import org.apache.derby.iapi.sql.dictionary.ConglomerateDescriptor;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;
import org.apache.derby.iapi.store.access.Qualifier;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.TypeId;
import org.apache.derby.iapi.util.JBitSet;

/**
 * A RowResultSetNode represents the result set for a VALUES clause.
 *
 */

class RowResultSetNode extends FromTable
{
	SubqueryList subquerys;
    private List<AggregateNode> aggregates;

    QueryExpressionClauses qec = new QueryExpressionClauses();
//IC see: https://issues.apache.org/jira/browse/DERBY-6378

	/**
     * Constructor for a RowResultSetNode.
	 *
	 * @param valuesClause	The result column list for the VALUES clause.
	 * @param tableProperties	Properties list associated with the table
     * @param cm            The context manager
	 */
    RowResultSetNode(ResultColumnList valuesClause,
                     Properties tableProperties,
                     ContextManager cm) {
        super(null /* correlationName */, tableProperties, cm);
        setResultColumns( valuesClause );
//IC see: https://issues.apache.org/jira/browse/DERBY-6464

        if (getResultColumns() != null) {
			getResultColumns().markInitialSize();
        }
	}

//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
    String statementToString()
	{
		return "VALUES";
	}

	/**
	 * Prints the sub-nodes of this object.  See QueryTreeNode.java for
	 * how tree printing is supposed to work.
	 *
	 * @param depth		The depth of this node in the tree
	 */
    @Override
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
    void printSubNodes(int depth)
	{
		if (SanityManager.DEBUG)
		{
			super.printSubNodes(depth);

			if (subquerys != null)
			{
				printLabel(depth, "subquerys: ");
				subquerys.treePrint(depth + 1);
			}

//IC see: https://issues.apache.org/jira/browse/DERBY-6378
            printQueryExpressionSuffixClauses(depth, qec);
		}
	}

    @Override
    public boolean referencesSessionSchema() throws StandardException {
        return subquerys != null && subquerys.referencesSessionSchema();
    }

	/**
	 * Modify the RCL of this node to match the target of the insert.
	 */
    @Override
//IC see: https://issues.apache.org/jira/browse/DERBY-4442
//IC see: https://issues.apache.org/jira/browse/DERBY-3
//IC see: https://issues.apache.org/jira/browse/DERBY-4433
//IC see: https://issues.apache.org/jira/browse/DERBY-1644
	ResultSetNode enhanceRCLForInsert(
			InsertNode target, boolean inOrder, int[] colMap)
		throws StandardException
	{
//IC see: https://issues.apache.org/jira/browse/DERBY-6464
		if (!inOrder || getResultColumns().size() < target.resultColumnList.size()) {
			setResultColumns( getRCLForInsert(target, colMap) );
		}
		return this;
	}

	/*
	 *  Optimizable interface
	 */

	/**
	 * @see Optimizable#estimateCost
	 *
	 * @exception StandardException		Thrown on error
	 */
    @Override
	public CostEstimate estimateCost(OptimizablePredicateList predList,
										ConglomerateDescriptor cd,
										CostEstimate outerCost,
										Optimizer optimizer,
										RowOrdering rowOrdering)
								throws StandardException
	{
		/*
		** Assume for now that the cost of a VALUES clause is zero, with one row
		** fetched.  Is this true, and if not, does it make a difference?
		** There's nothing to optimize in this case.
		*/
//IC see: https://issues.apache.org/jira/browse/DERBY-6464
		if (getCostEstimate() == null)
		{
			setCostEstimate( getOptimizerFactory().getCostEstimate() );
		}

		getCostEstimate().setCost(0.0d, 1.0d, 1.0d);

		/* A single row is always ordered */
		rowOrdering.optimizableAlwaysOrdered(this);

		return getCostEstimate();
	}

	/**
	 * Bind the non VTI tables in this ResultSetNode.  This includes getting their
	 * descriptors from the data dictionary and numbering them.
	 *
	 * @param dataDictionary	The DataDictionary to use for binding
	 * @param fromListParam		FromList to use/append to.
	 *
	 * @return	ResultSetNode
	 *
	 * @exception StandardException		Thrown on error
	 */
    @Override
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
    ResultSetNode bindNonVTITables(DataDictionary dataDictionary,
							FromList fromListParam) 
					throws StandardException
	{
		/* Assign the tableNumber */
		if (tableNumber == -1)  // allow re-bind, in which case use old number
			tableNumber = getCompilerContext().getNextTableNumber();

		/* VALUES clause has no tables, so nothing to do */
		return this;
	}

	/**
	 * Bind the expressions in this RowResultSetNode.  This means binding the
	 * sub-expressions, as well as figuring out what the return type is
	 * for each expression.
	 *
	 * @exception StandardException		Thrown on error
	 */
    @Override
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
    void bindExpressions(FromList fromListParam)
					throws StandardException
	{
		int nestingLevel;

        subquerys = new SubqueryList(getContextManager());

        aggregates = new ArrayList<AggregateNode>();
//IC see: https://issues.apache.org/jira/browse/DERBY-6213

		/* Verify that there are no DEFAULTs in the RCL.
		 * DEFAULT is only valid for an insert, and it has
		 * already been coverted into the tree by the time we get here.
		 * The grammar allows:
		 *		VALUES DEFAULT;
		 * so we need to check for that here and throw an exception if found.
		 */
		getResultColumns().checkForInvalidDefaults();
//IC see: https://issues.apache.org/jira/browse/DERBY-6464

		/* Believe it or not, a values clause can contain correlated column references
		 * and subqueries.  In order to get correlated column resolution working 
		 * correctly, we need to set our nesting level to be 1 deeper than the current
		 * level and push ourselves into the FROM list.  
		 */

		/* Set the nesting level in this node */
		if (fromListParam.size() == 0)
		{
			nestingLevel = 0;
		}
		else
		{
			nestingLevel = ((FromTable) fromListParam.elementAt(0)).getLevel() + 1;
		}
		setLevel(nestingLevel);
		fromListParam.insertElementAt(this, 0);
//IC see: https://issues.apache.org/jira/browse/DERBY-6464
		getResultColumns().bindExpressions(fromListParam, subquerys,
//IC see: https://issues.apache.org/jira/browse/DERBY-6075
                                      aggregates);
		// Pop ourselves back out of the FROM list
		fromListParam.removeElementAt(0);

        if (!aggregates.isEmpty())
		{
			throw StandardException.newException(SQLState.LANG_NO_AGGREGATES_IN_WHERE_CLAUSE);
		}

		SelectNode.checkNoWindowFunctions(getResultColumns(), "VALUES");
//IC see: https://issues.apache.org/jira/browse/DERBY-6464

//IC see: https://issues.apache.org/jira/browse/DERBY-6378
        for (int i = 0; i < qec.size(); i++) {
            final OrderByList obl = qec.getOrderByList(i);

            if (obl != null) {
                obl.pullUpOrderByColumns(this);
                obl.bindOrderByColumns(this);
            }

            bindOffsetFetch(qec.getOffset(i), qec.getFetchFirst(i));
        }
    }

	/**
	 * Bind the expressions in this ResultSetNode if it has tables.  This means binding the
	 * sub-expressions, as well as figuring out what the return type is for
	 * each expression.
	 *
	 * @param fromListParam		FromList to use/append to.
	 *
	 * @exception StandardException		Thrown on error
	 */
    @Override
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
    void bindExpressionsWithTables(FromList fromListParam)
					throws StandardException
	{
		/* We don't have any tables, so just return */
	}

	/**
	 * Bind the expressions in the target list.  This means binding the
	 * sub-expressions, as well as figuring out what the return type is
	 * for each expression.  This is useful for EXISTS subqueries, where we
	 * need to validate the target list before blowing it away and replacing
	 * it with a SELECT true.
	 *
	 * @exception StandardException		Thrown on error
	 */
    @Override
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
    void bindTargetExpressions(FromList fromListParam)
					throws StandardException
	{
		bindExpressions(fromListParam);
	}

	/**
	 * Bind any untyped null nodes to the types in the given ResultColumnList.
	 *
	 * @param bindingRCL	The ResultColumnList with the types to bind to.
	 *
	 * @exception StandardException		Thrown on error
	 */
    @Override
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
    void bindUntypedNullsToResultColumns(ResultColumnList bindingRCL)
				throws StandardException
	{
		/*
		** If bindingRCL is null, then we are
		** under a cursor node that is inferring
		** its RCL from us.  It passes null to
		** get union to use both sides of the union
		** for the check.  Anyway, since there is
		** nothing under us but an RCL, just pass
		** in our RCL.
		*/
		if (bindingRCL == null)
			bindingRCL = getResultColumns();
//IC see: https://issues.apache.org/jira/browse/DERBY-6464

		getResultColumns().bindUntypedNullsToResultColumns(bindingRCL);
	}

	/**
	 * Try to find a ResultColumn in the table represented by this FromTable
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
    @Override
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
    ResultColumn getMatchingColumn(
						ColumnReference columnReference)
						throws StandardException
	{
		return null;
	}

	/**
	 * Get the exposed name for this table, which is the name that can
	 * be used to refer to it in the rest of the query.
	 *
	 * @return	The exposed name of this table.
	 *
	 * @exception StandardException		Thrown on error
	 */
    @Override
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
    String getExposedName() throws StandardException
	{
		return null;
	}

	/**
	 * Verify that a SELECT * is valid for this type of subquery.
	 *
	 * @param outerFromList	The FromList from the outer query block(s)
	 * @param subqueryType	The subquery type
	 *
	 * @exception StandardException		Thrown on error
	 */
    @Override
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
    void verifySelectStarSubquery(FromList outerFromList, int subqueryType)
					throws StandardException
	{
	}

    @Override
    public void pushQueryExpressionSuffix() {
//IC see: https://issues.apache.org/jira/browse/DERBY-6378
        qec.push();
    }

	/**
	 * Push the order by list down from the cursor node
	 * into its child result set so that the optimizer
	 * has all of the information that it needs to 
	 * consider sort avoidance.
	 *
	 * @param orderByList	The order by list
	 */
    @Override
	void pushOrderByList(OrderByList orderByList)
	{
//IC see: https://issues.apache.org/jira/browse/DERBY-6378
        qec.setOrderByList(orderByList);
	}

    /**
     * Push down the offset and fetch first parameters, if any, to this node.
     *
     * @param offset    the OFFSET, if any
     * @param fetchFirst the OFFSET FIRST, if any
     * @param hasJDBClimitClause true if the clauses were added by (and have the semantics of) a JDBC limit clause
     */
    @Override
    void pushOffsetFetchFirst( ValueNode offset, ValueNode fetchFirst, boolean hasJDBClimitClause )
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-6378
        qec.setOffset(offset);
        qec.setFetchFirst(fetchFirst);
        qec.setHasJDBCLimitClause(hasJDBClimitClause);
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
    @Override
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
    ResultSetNode preprocess(int numTables,
									GroupByList gbl,
									FromList fromList)
								throws StandardException
	{
        getResultColumns().preprocess(
                numTables,
                fromList,
                subquerys,
                new PredicateList(getContextManager()));

		/* Allocate a dummy referenced table map */ 
//IC see: https://issues.apache.org/jira/browse/DERBY-6464
		setReferencedTableMap( new JBitSet(numTables) );
		getReferencedTableMap().set(tableNumber);

        // If we have more than 1 ORDERBY columns, we may be able to
        // remove duplicate columns, e.g., "ORDER BY 1, 1, 2".
        // Well, not very likely here, since we're here:
        //     VALUES x followed by ORDER BY 1,1,2
        // but for completeness...
//IC see: https://issues.apache.org/jira/browse/DERBY-6378
        for (int i = 0; i < qec.size(); i++) {
            final OrderByList obl = qec.getOrderByList(i);


            if (obl != null && obl.size() > 1) {
                obl.removeDupColumns();
            }
        }

        return this;
	}
	
	/**
	 * Ensure that the top of the RSN tree has a PredicateList.
	 *
	 * @param numTables			The number of tables in the query.
	 * @return ResultSetNode	A RSN tree with a node which has a PredicateList on top.
	 *
	 * @exception StandardException		Thrown on error
	 */
    @Override
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
    ResultSetNode ensurePredicateList(int numTables)
		throws StandardException
	{
		return genProjectRestrict(numTables);
	}

	/**
	 * Add a new predicate to the list.  This is useful when doing subquery
	 * transformations, when we build a new predicate with the left side of
	 * the subquery operator and the subquery's result column.
	 *
	 * @param predicate		The predicate to add
	 *
	 * @return ResultSetNode	The new top of the tree.
	 *
	 * @exception StandardException		Thrown on error
	 */
    @Override
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
    ResultSetNode addNewPredicate(Predicate predicate)
			throws StandardException
	{
		PredicateList		predList;
		ResultColumnList	prRCList;
		ResultSetNode		newPRN;
		
		/* We are the body of a quantified predicate subquery.  We
		 * need to generate (and return) a PRN above us so that there will be
		 * a place to attach the new predicate.
		 */

		/* We get a shallow copy of the ResultColumnList and its 
		 * ResultColumns.  (Copy maintains ResultColumn.expression for now.)
		 */
//IC see: https://issues.apache.org/jira/browse/DERBY-6464
		prRCList = getResultColumns();
		setResultColumns( getResultColumns().copyListAndObjects() );

		/* Replace ResultColumn.expression with new VirtualColumnNodes
		 * in the ProjectRestrictNode's ResultColumnList.  (VirtualColumnNodes include
		 * pointers to source ResultSetNode, this, and source ResultColumn.)
		 */
		prRCList.genVirtualColumnNodes(this, getResultColumns());

		/* Put the new predicate in a list */
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
        predList = new PredicateList(getContextManager());
		predList.addPredicate(predicate);

        return new ProjectRestrictNode(
								this,
								prRCList,
								null,	/* Restriction */
								predList,   /* Restriction as PredicateList */
								null,	/* Project subquery list */
								null,	/* Restrict subquery list */
								tableProperties,
								getContextManager()				 );
	}

	/**
	 * Evaluate whether or not the subquery in a FromSubquery is flattenable.  
	 * Currently, a FSqry is flattenable if all of the following are true:
	 *		o  Subquery is a SelectNode or a RowResultSetNode (not a UnionNode)
	 *		o  It contains no top level subqueries.  (RESOLVE - we can relax this)
	 *		o  It does not contain a group by or having clause
	 *		o  It does not contain aggregates.
	 *		o  There is at least one result set in the from list that is
	 *		   not a RowResultSetNode (the reason is to avoid having
	 *		   an outer SelectNode with an empty FromList.
	 *
	 * @param fromList	The outer from list
	 *
	 * @return boolean	Whether or not the FromSubquery is flattenable.
	 */
    @Override
    boolean flattenableInFromSubquery(FromList fromList)
	{
		if ((subquerys != null) &&
			(subquerys.size() > 0))
		{
			return false;
		}

//IC see: https://issues.apache.org/jira/browse/DERBY-6075
        if ((aggregates != null) && !aggregates.isEmpty())
		{
			return false;
		}

		/*
		** Don't flatten if select list contains something
		** that isn't clonable
		*/
//IC see: https://issues.apache.org/jira/browse/DERBY-6464
		if ( ! getResultColumns().isCloneable())
		{
			return false;
		}

		boolean nonRowResultSetFound = false;
		int flSize = fromList.size();
		for (int index = 0; index < flSize; index++)
		{
			FromTable ft = (FromTable) fromList.elementAt(index);

			if (ft instanceof FromSubquery)
			{
				ResultSetNode subq = ((FromSubquery) ft).getSubquery();
				if ( ! (subq instanceof RowResultSetNode))
				{
					nonRowResultSetFound = true;
					break;
				}
			}
			else
			{
				nonRowResultSetFound = true;
				break;
			}
		}

		return nonRowResultSetFound;
	}

	/**
	 * Optimize this SelectNode.  This means choosing the best access path
	 * for each table, among other things.
	 *
	 * @param dataDictionary	The DataDictionary to use for optimization
	 * @param predicateList		The predicate list to optimize against
	 * @param outerRows			The number of outer joining rows
	 *
	 * @return	ResultSetNode	The top of the optimized tree
	 *
	 * @exception StandardException		Thrown on error
	 */
    @Override
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
    ResultSetNode optimize(DataDictionary dataDictionary,
								  PredicateList	predicateList,
								  double outerRows) 
			throws StandardException
	{
		/*
		** Get an optimizer.  The only reason we need one is to get a
		** CostEstimate object, so we can represent the cost of this node.
		** This seems like overkill, but it's just an object allocation...
		*/
        setCostEstimate( getOptimizerFactory().getCostEstimate() );
//IC see: https://issues.apache.org/jira/browse/DERBY-6464

		// RESOLVE: THE COST SHOULD TAKE SUBQUERIES INTO ACCOUNT
		getCostEstimate().setCost(0.0d, outerRows, outerRows);

		subquerys.optimize(dataDictionary, outerRows);
//IC see: https://issues.apache.org/jira/browse/DERBY-6008
		return this;
	}

	/**
	 * @see Optimizable#modifyAccessPath
	 *
	 * @exception StandardException		Thrown on error
	 */
    @Override
	public Optimizable modifyAccessPath(JBitSet outerTables) throws StandardException
	{
		/* For most types of Optimizable, do nothing */
		return (Optimizable) modifyAccessPaths();
	}

	/**
	 * @see ResultSetNode#modifyAccessPaths
	 *
	 * @exception StandardException		Thrown on error
	 */
    @Override
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
    ResultSetNode modifyAccessPaths() throws StandardException
	{
		ResultSetNode treeTop = this;

		subquerys.modifyAccessPaths();

		/* Generate the OrderByNode if a sort is still required for
		 * the order by.
		 */
//IC see: https://issues.apache.org/jira/browse/DERBY-6378
        for (int i = 0; i < qec.size(); i++) {
            final OrderByList obl = qec.getOrderByList(i);
            if (obl != null) {
                treeTop = new OrderByNode(treeTop,
                        obl,
                        tableProperties,
                        getContextManager());
            }

            final ValueNode offset = qec.getOffset(i);
            final ValueNode fetchFirst = qec.getFetchFirst(i);
            Boolean hasJDBClimitClause = qec.getHasJDBCLimitClause()[i];

//IC see: https://issues.apache.org/jira/browse/DERBY-4398
            if (offset != null || fetchFirst != null) {
                ResultColumnList newRcl =
                    treeTop.getResultColumns().copyListAndObjects();
                newRcl.genVirtualColumnNodes(
                    treeTop, treeTop.getResultColumns());

                treeTop = new RowCountNode(
                        treeTop,
                        newRcl,
                        offset,
                        fetchFirst,
                        hasJDBClimitClause.booleanValue(),
                        getContextManager());
            }
        }

		return treeTop;
	}

	/**
	 * Return whether or not this ResultSet tree is guaranteed to return
	 * at most 1 row based on heuristics.  (A RowResultSetNode and a
	 * SELECT with a non-grouped aggregate will return at most 1 row.)
	 *
	 * @return Whether or not this ResultSet tree is guaranteed to return
	 * at most 1 row based on heuristics.
	 */
    @Override
	boolean returnsAtMostOneRow()
	{
		return true;
	}

	/**
	 * Set the type of each parameter in the result column list for this table constructor.
	 *
	 * @param typeColumns	The ResultColumnList containing the desired result
	 *						types.
	 *
	 * @exception StandardException		Thrown on error
	 */
    @Override
	void setTableConstructorTypes(ResultColumnList typeColumns)
			throws StandardException
	{
		if (SanityManager.DEBUG)
//IC see: https://issues.apache.org/jira/browse/DERBY-6464
			SanityManager.ASSERT(getResultColumns().visibleSize() <= typeColumns.size(),
				"More columns in ResultColumnList than in base table");

		/* Look for ? parameters in the result column list */
		int rclSize = getResultColumns().size();
		for (int index = 0; index < rclSize; index++)
		{
            ResultColumn rc = getResultColumns().elementAt(index);
			ValueNode re = rc.getExpression();

			if (re.requiresTypeFromContext())
			{
                ResultColumn typeCol = typeColumns.elementAt(index);
				/*
				** We found a ? - set its type to the type of the
				** corresponding column of the target table.
				*/
				re.setType(typeCol.getTypeServices());
			}
			else if (re instanceof CharConstantNode)
			{
				// Character constants are of type CHAR (fixed length string).
				// This causes a problem (beetle 5160) when multiple row values are provided
				// as constants for insertion into a variable length string column.
				//
				// This issue is the query expression
				// VALUES 'abc', 'defghi'
				// has type of CHAR(6), ie. the length of largest row value for that column.
				// This is from the UNION defined behaviour.
				// This causes strings with less than the maximum length to be blank padded
				// to that length (CHAR semantics). Thus if this VALUES clause is used to
				// insert into a variable length string column, then these blank padded values
				// are inserted, which is not what is required ...
				//
				// BECAUSE, when the VALUES is used as a table constructor SQL standard says the
				// types of the table constructor's columns are set by the table's column types.
				// Thus, in this case, each of those string constants should be of type VARCHAR
				// (or the matching string type for the table).
				//
				//
				// This is only an issue for fixed length character (CHAR, BIT) string or
				// binary consraints being inserted into variable length types.
				// This is because any other type's fundemental literal value is not affected
				// by its data type. E.g. Numeric types such as INT, REAL, BIGINT, DECIMAL etc.
				// do not have their value modifed by the union since even if the type is promoted
				// to a higher type, its fundemental value remains unchanged.
				// values (1.2, 34.4567, 234.47) will be promoted to
				// values (1.2000, 34.4567, 234.4700)
				// but their numeric value remains the same.
				//
				//
				//
				// The fix is to change the base type of the table constructor's value to
				// match the column type. Its length can be left as-is, because there is
				// still a normailzation step when the value is inserted into the table.
				// That will set the correct length and perform truncation checks etc.

                ResultColumn typeCol = typeColumns.elementAt(index);
//IC see: https://issues.apache.org/jira/browse/DERBY-673

				TypeId colTypeId = typeCol.getTypeId();

				if (colTypeId.isStringTypeId()) {

					if (colTypeId.getJDBCTypeId() != java.sql.Types.CHAR) {

						int maxWidth = re.getTypeServices().getMaximumWidth();

						re.setType(new DataTypeDescriptor(colTypeId, true, maxWidth));
					}
				}
				else if (colTypeId.isBitTypeId()) {
					if (colTypeId.getJDBCTypeId() == java.sql.Types.VARBINARY) {
					// then we're trying to cast a char literal into a
					// variable bit column.  We can't change the base
					// type of the table constructor's value from char
					// to bit, so instead, we just change the base type
					// of that value from char to varchar--that way,
					// no padding will be added when we convert to
					// bits later on (Beetle 5306).
						TypeId tId = TypeId.getBuiltInTypeId(java.sql.Types.VARCHAR);
						re.setType(new DataTypeDescriptor(tId, true));
						typeColumns.setElementAt(typeCol, index);
					}
					else if (colTypeId.getJDBCTypeId() == java.sql.Types.LONGVARBINARY) {
						TypeId tId = TypeId.getBuiltInTypeId(java.sql.Types.LONGVARCHAR);
						re.setType(new DataTypeDescriptor(tId, true));
						typeColumns.setElementAt(typeCol, index);
					}
				}

			}
			else if (re instanceof BitConstantNode)
			{
//IC see: https://issues.apache.org/jira/browse/DERBY-673
                ResultColumn typeCol   = typeColumns.elementAt(index);
                TypeId       colTypeId = typeCol.getTypeId();

				if (colTypeId.isBitTypeId()) {

					// NOTE: Don't bother doing this if the column type is BLOB,
					// as we don't allow bit literals to be inserted into BLOB
					// columns (they have to be explicitly casted first); beetle 5266.
					if ((colTypeId.getJDBCTypeId() != java.sql.Types.BINARY) &&
						(colTypeId.getJDBCTypeId() != java.sql.Types.BLOB)) {

						int maxWidth = re.getTypeServices().getMaximumWidth();

						re.setType(new DataTypeDescriptor(colTypeId, true, maxWidth));
					}
				}
				else if (colTypeId.isStringTypeId()) {
					if (colTypeId.getJDBCTypeId() == java.sql.Types.VARCHAR) {
					// then we're trying to cast a bit literal into a
					// variable char column.  We can't change the base
					// type of the table constructor's value from bit
					// to char, so instead, we just change the base
					// type of that value from bit to varbit--that way,
					// no padding will be added when we convert to
					// char later on.
						TypeId tId = TypeId.getBuiltInTypeId(java.sql.Types.VARBINARY);
						re.setType(new DataTypeDescriptor(tId, true));
						typeColumns.setElementAt(typeCol, index);
					}
					else if (colTypeId.getJDBCTypeId() == java.sql.Types.LONGVARCHAR) {
						TypeId tId = TypeId.getBuiltInTypeId(java.sql.Types.LONGVARBINARY);
						re.setType(new DataTypeDescriptor(tId, true));
						typeColumns.setElementAt(typeCol, index);
					}
				}
			}
		}
	}

    /**
     * The generated ResultSet will be:
     *
     *      RowResultSet -- for the VALUES clause
     *
	 *
	 * @exception StandardException		Thrown on error
     */
    @Override
    void generate(ActivationClassBuilder acb, MethodBuilder mb)
							throws StandardException
	{
		if (SanityManager.DEBUG)
            SanityManager.ASSERT(getResultColumns() != null, "Tree structure bad");
//IC see: https://issues.apache.org/jira/browse/DERBY-6464

		// Get our final cost estimate.
		setCostEstimate( getFinalCostEstimate() );

		/*
		** Check and see if everything below us is a constant or not.
		** If so, we'll let execution know that it can do some caching.
		** Before we do the check, we are going to temporarily set
		*/
		boolean canCache = canWeCacheResults();

		/* Get the next ResultSet #, so that we can number this ResultSetNode, its
		 * ResultColumnList and ResultSet.
		 */
		assignResultSetNumber();

	    // we are dealing with
		// VALUES(value1, value2, value3)
	    // so we generate a RowResultSet to return the values listed.

		// we can reduce the tree to one RowResultSet
    	// since there is nothing but the resultColumns

    	// RowResultSet takes the row-generating function
    	// so we generate one and get back the expression
    	// pointing to it.
	    //
		// generate the expression to return, which is:
		// ResultSetFactory.getRowResultSet(this, planX.exprN)
		// [planX is the name of the class being generated,
    	// exprN is the name of the function being generated.]

		acb.pushGetResultSetFactoryExpression(mb);

		acb.pushThisAsActivation(mb);
//IC see: https://issues.apache.org/jira/browse/DERBY-6464
		getResultColumns().generate(acb, mb);
		mb.push(canCache);
		mb.push(getResultSetNumber());
		mb.push(getCostEstimate().rowCount());
		mb.push(getCostEstimate().getEstimatedCost());
//IC see: https://issues.apache.org/jira/browse/DERBY-1700
		mb.callMethod(VMOpcode.INVOKEINTERFACE, (String) null, "getRowResultSet",
                ClassName.NoPutResultSet, 6);
	}

	/**
     * {@inheritDoc}
	 */
    @Override
//IC see: https://issues.apache.org/jira/browse/DERBY-4426
	void replaceOrForbidDefaults(TableDescriptor ttd,
                                 ResultColumnList tcl,
                                 boolean allowDefaults)
		throws StandardException
	{
//IC see: https://issues.apache.org/jira/browse/DERBY-6464
		getResultColumns().replaceOrForbidDefaults(ttd, tcl, allowDefaults);
	}

	/**
	 * Optimize any subqueries that haven't been optimized any where
	 * else.  This is useful for a RowResultSetNode as a derived table
	 * because it doesn't get optimized otherwise.
	 *
	 * @exception StandardException		Thrown on error
	 */
    @Override
	void optimizeSubqueries(DataDictionary dd, double rowCount)
		throws StandardException
	{
		subquerys.optimize(dd, rowCount);
	}

	/**
	 * @see ResultSetNode#adjustForSortElimination
	 */
    @Override
	void adjustForSortElimination()
	{
	}

	/*
	** Check and see if everything below us is a constant or not.
	** If so, we'll let execution know that it can do some caching.
	** Before we do the check, we are going to temporarily set
	** ParameterNodes to CONSTANT.  We do this because we know
	** that we can cache a row with a parameter value and get
	** the param column reset by the user setting a param, so
	** we can skip over parameter nodes.  We are doing this
	** extra work to optimize inserts of the form:
	**
	** prepare: insert into mytab values (?,?);
	** setParam
	** execute()
	** setParam
	** execute()
	*/
	private boolean canWeCacheResults() throws StandardException
	{

		/*
		** Check the tree below us
		*/
		HasVariantValueNodeVisitor visitor = 
			new HasVariantValueNodeVisitor(Qualifier.QUERY_INVARIANT, true);

		super.accept(visitor);
		boolean canCache = !visitor.hasVariant();

		return canCache;
	}
}
