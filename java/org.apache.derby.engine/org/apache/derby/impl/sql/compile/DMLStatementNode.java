/*

   Derby - Class org.apache.derby.impl.sql.compile.DMLStatementNode

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
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derby.iapi.sql.ResultColumnDescriptor;
import org.apache.derby.iapi.sql.ResultDescription;
import org.apache.derby.iapi.sql.compile.Visitor;
import org.apache.derby.iapi.sql.conn.Authorizer;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.util.JBitSet;

/**
 * A DMLStatementNode represents any type of DML statement: a cursor declaration,
 * an INSERT statement, and UPDATE statement, or a DELETE statement.  All DML
 * statements have result sets, but they do different things with them.  A
 * SELECT statement sends its result set to the client, an INSERT statement
 * inserts its result set into a table, a DELETE statement deletes from a
 * table the rows corresponding to the rows in its result set, and an UPDATE
 * statement updates the rows in a base table corresponding to the rows in its
 * result set.
 *
 */

abstract class DMLStatementNode extends StatementNode
{

	/**
	 * The result set is the rows that result from running the
	 * statement.  What this means for SELECT statements is fairly obvious.
	 * For a DELETE, there is one result column representing the
	 * key of the row to be deleted (most likely, the location of the
	 * row in the underlying heap).  For an UPDATE, the row consists of
	 * the key of the row to be updated plus the updated columns.  For
	 * an INSERT, the row consists of the new column values to be
	 * inserted, with no key (the system generates a key).
	 *
	 * The parser doesn't know anything about keys, so the columns
	 * representing the keys will be added after parsing (perhaps in
	 * the binding phase?).
	 *
	 */
	ResultSetNode	resultSet;

//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
    DMLStatementNode(ResultSetNode resultSet, ContextManager cm) {
        super(cm);
        this.resultSet = resultSet;
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
			if (resultSet != null)
			{
				printLabel(depth, "resultSet: ");
				resultSet.treePrint(depth + 1);
			}
		}
	}

	/**
	 * Get the ResultSetNode from this DML Statement.
	 * (Useful for view resolution after parsing the view definition.)
	 *
	 * @return ResultSetNode	The ResultSetNode from this DMLStatementNode.
	 */
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
    ResultSetNode getResultSetNode()
	{
		return resultSet;
	}

	/**
	 * Bind this DMLStatementNode.  This means looking up tables and columns and
	 * getting their types, and figuring out the result types of all
	 * expressions, as well as doing view resolution, permissions checking,
	 * etc.
	 *
	 * @param dataDictionary	The DataDictionary to use to look up
	 *				columns, tables, etc.
	 *
	 * @return	The bound query tree
	 *
	 * @exception StandardException		Thrown on error
	 */

//IC see: https://issues.apache.org/jira/browse/DERBY-2096
	QueryTreeNode bind(DataDictionary dataDictionary)
					 throws StandardException
	{
		// We just need select privilege on most columns and tables
//IC see: https://issues.apache.org/jira/browse/DERBY-464
		getCompilerContext().pushCurrentPrivType(getPrivType());
		try {
			/*
			** Bind the tables before binding the expressions, so we can
			** use the results of table binding to look up columns.
			*/
			bindTables(dataDictionary);

			/* Bind the expressions */
			bindExpressions();
		}
		finally
		{
			getCompilerContext().popCurrentPrivType();
		}

		return this;
	}

	/**
	 * Bind only the underlying ResultSets with tables.  This is necessary for
	 * INSERT, where the binding order depends on the underlying ResultSets.
	 * This means looking up tables and columns and
	 * getting their types, and figuring out the result types of all
	 * expressions, as well as doing view resolution, permissions checking,
	 * etc.
	 *
	 * @param dataDictionary	The DataDictionary to use to look up
	 *				columns, tables, etc.
	 *
	 * @return	The bound query tree
	 *
	 * @exception StandardException		Thrown on error
	 */

//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
    QueryTreeNode bindResultSetsWithTables(DataDictionary dataDictionary)
					 throws StandardException
	{
		/* Okay to bindly bind the tables, since ResultSets without tables
		 * know to handle the call.
		 */
		bindTables(dataDictionary);

		/* Bind the expressions in the underlying ResultSets with tables */
		bindExpressionsWithTables();

		return this;
	}

	/**
	 * Bind the tables in this DML statement.
	 *
	 * @param dataDictionary	The data dictionary to use to look up the tables
	 *
	 * @exception StandardException		Thrown on error
	 */

	protected void bindTables(DataDictionary dataDictionary)
			throws StandardException
	{
		/* Bind the tables in the resultSet 
		 * (DMLStatementNode is above all ResultSetNodes, so table numbering
		 * will begin at 0.)
		 * In case of referential action on delete , the table numbers can be
		 * > 0 because the nodes are create for dependent tables also in the 
		 * the same context.
		 */
        boolean doJOO = getOptimizerFactory().doJoinOrderOptimization();
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
        ContextManager cm = getContextManager();
        resultSet = resultSet.bindNonVTITables(dataDictionary,
                                               new FromList(doJOO, cm));
        resultSet = resultSet.bindVTITables(new FromList(doJOO, cm));
	}

	/**
	 * Bind the expressions in this DML statement.
	 *
	 * @exception StandardException		Thrown on error
	 */

	protected void bindExpressions()
			throws StandardException
	{
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
        FromList fromList =
                new FromList(getOptimizerFactory().doJoinOrderOptimization(),
                             getContextManager());

		/* Bind the expressions under the resultSet */
		resultSet.bindExpressions(fromList);

		/* Verify that all underlying ResultSets reclaimed their FromList */
		if (SanityManager.DEBUG)
		SanityManager.ASSERT(fromList.size() == 0,
			"fromList.size() is expected to be 0, not " + fromList.size() +
			" on return from RS.bindExpressions()");
	}

	/**
	 * Bind the expressions in the underlying ResultSets with tables.
	 *
	 * @exception StandardException		Thrown on error
	 */

	protected void bindExpressionsWithTables()
			throws StandardException
	{
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
        FromList fromList =
                new FromList(getOptimizerFactory().doJoinOrderOptimization(),
                             getContextManager());

		/* Bind the expressions under the resultSet */
		resultSet.bindExpressionsWithTables(fromList);

		/* Verify that all underlying ResultSets reclaimed their FromList */
		if (SanityManager.DEBUG)
		SanityManager.ASSERT(fromList.size() == 0,
			"fromList.size() is expected to be 0, not " + fromList.size() +
			" on return from RS.bindExpressions()");
	}

	/**
	 * Returns the type of activation this class
	 * generates.
	 * 
	 * @return either (NEED_ROW_ACTIVATION | NEED_PARAM_ACTIVATION) or
	 *			(NEED_ROW_ACTIVATION) depending on params
	 *
	 */
	int activationKind()
	{
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
        List<ParameterNode> parameterList =
            getCompilerContext().getParameterList();
		/*
		** We need rows for all types of DML activations.  We need parameters
		** only for those that have parameters.
		*/
		if (parameterList != null && !parameterList.isEmpty())
		{
			return StatementNode.NEED_PARAM_ACTIVATION;
		}
		else
		{
			return StatementNode.NEED_ROW_ACTIVATION;
		}
	}

	/**
	 * Optimize a DML statement (which is the only type of statement that
	 * should need optimizing, I think). This method over-rides the one
	 * in QueryTreeNode.
	 *
	 * This method takes a bound tree, and returns an optimized tree.
	 * It annotates the bound tree rather than creating an entirely
	 * new tree.
	 *
	 * Throws an exception if the tree is not bound, or if the binding
	 * is out of date.
	 *
	 *
	 * @exception StandardException		Thrown on error
	 */
    @Override
	public void optimizeStatement() throws StandardException
	{
		resultSet = resultSet.preprocess(getCompilerContext().getNumTables(),
										 null,
										 (FromList) null);

		// Evaluate expressions with constant operands here to simplify the
		// query tree and to reduce the runtime cost. Do it before optimize()
		// since the simpler tree may have more accurate information for
		// the optimizer. (Example: The selectivity for 1=1 is estimated to
		// 0.1, whereas the actual selectivity is 1.0. In this step, 1=1 will
		// be rewritten to TRUE, which is known by the optimizer to have
		// selectivity 1.0.)
		accept(new ConstantExpressionVisitor());
//IC see: https://issues.apache.org/jira/browse/DERBY-4416

		resultSet = resultSet.optimize(getDataDictionary(), null, 1.0d);

		resultSet = resultSet.modifyAccessPaths();

		/* If this is a cursor, then we
		 * need to generate a new ResultSetNode to enable the scrolling
		 * on top of the tree before modifying the access paths.
		 */
		if (this instanceof CursorNode)
		{
			ResultColumnList				siRCList;
			ResultColumnList				childRCList;
			ResultSetNode					siChild = resultSet;

			/* We get a shallow copy of the ResultColumnList and its 
			 * ResultColumns.  (Copy maintains ResultColumn.expression for now.)
			 */
			siRCList = resultSet.getResultColumns();
			childRCList = siRCList.copyListAndObjects();
			resultSet.setResultColumns(childRCList);

			/* Replace ResultColumn.expression with new VirtualColumnNodes
			 * in the ScrollInsensitiveResultSetNode's ResultColumnList.  (VirtualColumnNodes include
			 * pointers to source ResultSetNode, this, and source ResultColumn.)
			 */
			siRCList.genVirtualColumnNodes(resultSet, childRCList);

			/* Finally, we create the new ScrollInsensitiveResultSetNode */
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
            resultSet = new ScrollInsensitiveResultSetNode(
                    resultSet, siRCList, null, getContextManager());

			// Propagate the referenced table map if it's already been created
			if (siChild.getReferencedTableMap() != null)
			{
				resultSet.setReferencedTableMap((JBitSet) siChild.getReferencedTableMap().clone());
			}
		}

	}


	/**
	 * Make a ResultDescription for use in a PreparedStatement.
	 *
	 * ResultDescriptions are visible to JDBC only for cursor statements.
	 * For other types of statements, they are only used internally to
	 * get descriptions of the base tables being affected.  For example,
	 * for an INSERT statement, the ResultDescription describes the
	 * rows in the table being inserted into, which is useful when
	 * the values being inserted are of a different type or length
	 * than the columns in the base table.
	 *
	 * @return	A ResultDescription for this DML statement
	 */
    @Override
	public ResultDescription makeResultDescription()
	{
//IC see: https://issues.apache.org/jira/browse/DERBY-2661
	    ResultColumnDescriptor[] colDescs = resultSet.makeResultDescriptors();
		String statementType = statementToString();

	    return getExecutionFactory().getResultDescription(
                           colDescs, statementType );
	}

	/**
	 * Generate the code to create the ParameterValueSet, if necessary,
	 * when constructing the activation.  Also generate the code to call
	 * a method that will throw an exception if we try to execute without
	 * all the parameters being set.
	 * 
	 * @param acb	The ActivationClassBuilder for the class we're building
	 */

	void generateParameterValueSet(ActivationClassBuilder acb)
		throws StandardException
	{
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
        List<ParameterNode> parameterList =
            getCompilerContext().getParameterList();
		int	numberOfParameters = (parameterList == null) ? 0 : parameterList.size();

		if (numberOfParameters <= 0)
			return;

        ParameterNode.generateParameterValueSet
				( acb, numberOfParameters, parameterList);
	}

	/**
	 * A read statement is atomic (DMLMod overrides us) if there
	 * are no work units, and no SELECT nodes, or if its SELECT nodes 
	 * are all arguments to a function.  This is admittedly
	 * a bit simplistic, what if someone has: <pre>
	 * 	VALUES myfunc(SELECT max(c.commitFunc()) FROM T) 
	 * </pre>
	 * but we aren't going too far out of our way to
	 * catch every possible wierd case.  We basically
	 * want to be permissive w/o allowing someone to partially
	 * commit a write. 
	 * 
	 * @return true if the statement is atomic
	 *
	 * @exception StandardException on error
	 */	
    @Override
	public boolean isAtomic() throws StandardException
	{
		/*
		** If we have a FromBaseTable then we have
		** a SELECT, so we want to consider ourselves
		** atomic.  Don't drill below StaticMethodCallNodes
		** to allow a SELECT in an argument to a method
		** call that can be atomic.
		*/
		HasNodeVisitor visitor = new HasNodeVisitor(FromBaseTable.class, StaticMethodCallNode.class);
													
		this.accept(visitor);
		if (visitor.hasNode())
		{
			return true;
		}

		return false;
	}

	/**
	 * Accept the visitor for all visitable children of this node.
	 * 
	 * @param v the visitor
	 *
	 * @exception StandardException on error
	 */
    @Override
//IC see: https://issues.apache.org/jira/browse/DERBY-4421
	void acceptChildren(Visitor v)
		throws StandardException
	{
		super.acceptChildren(v);

//IC see: https://issues.apache.org/jira/browse/DERBY-4421
		if (resultSet != null)
		{
			resultSet = (ResultSetNode)resultSet.accept(v);
		}
	}

	/**
	 * Return default privilege needed for this node. Other DML nodes can override
	 * this method to set their own default privilege.
	 *
	 * @return true if the statement is atomic
	 */
	int getPrivType()
	{
//IC see: https://issues.apache.org/jira/browse/DERBY-464
		return Authorizer.SELECT_PRIV;
	}
}
