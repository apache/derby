/*

   Derby - Class org.apache.derby.impl.sql.compile.DMLStatementNode

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

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.compile.CompilerContext;
import org.apache.derby.iapi.sql.compile.Visitable;
import org.apache.derby.iapi.sql.compile.Visitor;
import org.apache.derby.iapi.sql.compile.C_NodeTypes;

import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.execute.ExecutionContext;
import org.apache.derby.iapi.sql.ResultColumnDescriptor;
import org.apache.derby.iapi.sql.ParameterValueSet;
import org.apache.derby.iapi.sql.ResultDescription;

import org.apache.derby.iapi.sql.dictionary.DataDictionaryContext;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;
import org.apache.derby.iapi.sql.dictionary.ConglomerateDescriptor;
import org.apache.derby.iapi.sql.dictionary.IndexRowGenerator;

import org.apache.derby.iapi.services.monitor.Monitor;

import org.apache.derby.iapi.services.compiler.MethodBuilder;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.util.JBitSet;

import org.apache.derby.impl.sql.compile.ActivationClassBuilder;

import java.util.Enumeration;
import java.util.Properties;
import java.util.Vector;

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
 * @author Jeff Lichtman
 */

public abstract class DMLStatementNode extends StatementNode
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
	 * RESOLVE: This is public so RepDeleteNode can see it. Perhaps it should
	 * hava a public accessor function.
	 */
	public ResultSetNode	resultSet;

	/**
	 * Initializer for a DMLStatementNode
	 *
	 * @param resultSet	A ResultSetNode for the result set of the
	 *			DML statement
	 */

	public void init(Object resultSet)
	{
		this.resultSet = (ResultSetNode) resultSet;
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
	public ResultSetNode getResultSetNode()
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

	public QueryTreeNode bind(DataDictionary dataDictionary)
					 throws StandardException
	{
		/*
		** Bind the tables before binding the expressions, so we can
		** use the results of table binding to look up columns.
		*/
		bindTables(dataDictionary);

		/* Bind the expressions */
		bindExpressions();

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

	public QueryTreeNode bindResultSetsWithTables(DataDictionary dataDictionary)
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
	 * @return	Nothing
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

		resultSet = resultSet.bindNonVTITables(
						dataDictionary,
						(FromList) getNodeFactory().getNode(
							C_NodeTypes.FROM_LIST,
							getNodeFactory().doJoinOrderOptimization(),
							getContextManager()));
		resultSet = resultSet.bindVTITables(
						(FromList) getNodeFactory().getNode(
							C_NodeTypes.FROM_LIST,
							getNodeFactory().doJoinOrderOptimization(),
							getContextManager()));
	}

	/**
	 * Bind the expressions in this DML statement.
	 *
	 * @return	Nothing
	 *
	 * @exception StandardException		Thrown on error
	 */

	protected void bindExpressions()
			throws StandardException
	{
		FromList fromList = (FromList) getNodeFactory().getNode(
								C_NodeTypes.FROM_LIST,
								getNodeFactory().doJoinOrderOptimization(),
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
	 *
	 * @return	Nothing
	 *
	 * @exception StandardException		Thrown on error
	 */

	protected void bindExpressionsWithTables()
			throws StandardException
	{
		FromList fromList = (FromList) getNodeFactory().getNode(
								C_NodeTypes.FROM_LIST,
								getNodeFactory().doJoinOrderOptimization(),
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
		Vector parameterList = getCompilerContext().getParameterList();
		/*
		** We need rows for all types of DML activations.  We need parameters
		** only for those that have parameters.
		*/
		if (parameterList != null && parameterList.size() > 0)
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
	 * @return	An optimized QueryTree
	 *
	 * @exception StandardException		Thrown on error
	 */

	public QueryTreeNode optimize() throws StandardException
	{
		resultSet = resultSet.preprocess(getCompilerContext().getNumTables(),
										 null,
										 (FromList) null);
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
			resultSet = (ResultSetNode) getNodeFactory().
							getNode(
								C_NodeTypes.SCROLL_INSENSITIVE_RESULT_SET_NODE,
								resultSet, 
								siRCList,
								null,
								getContextManager());
			// Propagate the referenced table map if it's already been created
			if (siChild.getReferencedTableMap() != null)
			{
				resultSet.setReferencedTableMap((JBitSet) siChild.getReferencedTableMap().clone());
			}
		}

		return this;
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
	 * @param name	The name of the cursor, if any
	 *
	 * @return	A ResultDescription for this DML statement
	 */

	public ResultDescription makeResultDescription()
	{
		ExecutionContext ec = (ExecutionContext) getContextManager().getContext(
			ExecutionContext.CONTEXT_ID);
	    ResultColumnDescriptor[] colDescs = resultSet.makeResultDescriptors(ec);
		String statementType = statementToString();

	    return ec.getExecutionFactory().getResultDescription(colDescs, statementType );
	}

	/**
	 * Generate the code to create the ParameterValueSet, if necessary,
	 * when constructing the activation.  Also generate the code to call
	 * a method that will throw an exception if we try to execute without
	 * all the parameters being set.
	 * 
	 * @param acb	The ActivationClassBuilder for the class we're building
	 *
	 * @return	Nothing
	 */

	void generateParameterValueSet(ActivationClassBuilder acb)
		throws StandardException
	{
		Vector parameterList = getCompilerContext().getParameterList();
		int	numberOfParameters = (parameterList == null) ? 0 : parameterList.size();

		if (numberOfParameters <= 0)
			return;

			ParameterNode.generateParameterValueSet
				( acb, numberOfParameters, parameterList);
	}

	/*
	** When all other generation is done for the statement,
	** we need to ensure all of the parameters have been touched.
	*
	*	@param	acb				ActivationClassBuilder
	*
	*/
	void generateParameterHolders(ActivationClassBuilder acb) 
		throws StandardException
	{
		Vector pList = getCompilerContext().getParameterList();
		if (pList.size() <= 0)
			return;

		ParameterNode.generateParameterHolders( acb,  pList);
			
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

		if (resultSet != null && !v.stopTraversal())
		{
			resultSet = (ResultSetNode)resultSet.accept(v);
		}

		return this;
	}
}
