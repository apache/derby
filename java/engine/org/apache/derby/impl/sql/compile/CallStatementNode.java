/*

   Derby - Class org.apache.derby.impl.sql.compile.CallStatementNode

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

import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.DataDictionaryContext;
import org.apache.derby.iapi.sql.dictionary.ColumnDescriptor;

import org.apache.derby.iapi.sql.ResultSet;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.ResultDescription;

import org.apache.derby.iapi.sql.compile.CompilerContext;
import org.apache.derby.iapi.sql.compile.Visitable;
import org.apache.derby.iapi.sql.compile.Visitor;
import org.apache.derby.iapi.sql.compile.C_NodeTypes;

import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;

import org.apache.derby.iapi.services.loader.GeneratedMethod;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.services.compiler.MethodBuilder;

import org.apache.derby.impl.sql.compile.ActivationClassBuilder;
import org.apache.derby.iapi.reference.ClassName;
import org.apache.derby.iapi.services.classfile.VMOpcode;


import java.lang.reflect.Modifier;

import java.util.Vector;

/**
 * An CallStatementNode represents a CALL statement.  It is the top node of the
 * query tree for that statement. There are 2 flavors, class and object, of call
 * statements.  A class call statement is a static method call off of a class
 * expression (class classnameandpath), while an object call statement is a
 * method call off of an object expression.  The return value, if any,
 * from the underlying method call is ignored.
 *
 * @author Jerry Brenner
 */
public class CallStatementNode extends DMLStatementNode
{
	private String		methodName;
	private ValueNode	methodCall;

	/* Need to track any subqueries under the methodCall */
	private SubqueryList subqueries;

	/* JRESOLVE?? Need to track any aggregates under the methodCall */
	private Vector aggregateVector;

	/**
	 * Initializer for a CallStatementNode.
	 *
	 * @param methodName		The method name
	 * @param methodCall		The expression to "call"
	 * @exception StandardException		Thrown on error
	 */

	public void init(Object methodName, Object methodCall)
		throws StandardException
	{
		init(null);
		this.methodName = (String) methodName;
		this.methodCall = (ValueNode) methodCall;
		((JavaToSQLValueNode)methodCall).getJavaValueNode().markForCallStatement();
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
			return "methodName: " + methodName + "\n" +
				super.toString();
		}
		else
		{
			return "";
		}
	}

	public String statementToString()
	{
		return "CALL";
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

			if (methodCall != null)
			{
				printLabel(depth, "methodCall: ");
				methodCall.treePrint(depth + 1);
			}

			if (subqueries != null && subqueries.size() >= 1)
			{
				printLabel(depth, "subqueries: ");
				subqueries.treePrint(depth + 1);
			}
		}
	}

	/**
	 * Bind this UpdateNode.  This means looking up tables and columns and
	 * getting their types, and figuring out the result types of all
	 * expressions, as well as doing view resolution, permissions checking,
	 * etc.
	 * <p>
	 * Binding an update will also massage the tree so that
	 * the ResultSetNode has a single column, the RID.
	 *
	 * @return	The bound query tree
	 *
	 * @exception StandardException		Thrown on error
	 */

	public QueryTreeNode bind() throws StandardException
	{
		DataDictionary				dd;

		subqueries = (SubqueryList) getNodeFactory().getNode(
											C_NodeTypes.SUBQUERY_LIST,
											getContextManager());
		aggregateVector = new Vector();

		dd = getDataDictionary();

		if (SanityManager.DEBUG)
		SanityManager.ASSERT((dd != null), "Failed to get data dictionary");

		methodCall = methodCall.bindExpression(
							(FromList) getNodeFactory().getNode(
								C_NodeTypes.FROM_LIST,
								getNodeFactory().doJoinOrderOptimization(),
								getContextManager()), 
							subqueries,
							aggregateVector);

		return this;
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
		DataDictionary				dd;

		dd = getDataDictionary();

		if (SanityManager.DEBUG)
		SanityManager.ASSERT((dd != null), "Failed to get data dictionary");

		/* Preprocess the method call tree */
		methodCall = methodCall.preprocess(
								getCompilerContext().getNumTables(),
								(FromList) getNodeFactory().getNode(
									C_NodeTypes.FROM_LIST,
									getNodeFactory().doJoinOrderOptimization(),
									getContextManager()),
								(SubqueryList) getNodeFactory().getNode(
													C_NodeTypes.SUBQUERY_LIST,
													getContextManager()),
								(PredicateList) getNodeFactory().getNode(
													C_NodeTypes.PREDICATE_LIST,
													getContextManager()));

		/* Optimize any subqueries in the method call tree */
		if (subqueries.size() >= 1)
		{
			subqueries.optimize(dd, 1.0);
			subqueries.modifyAccessPaths();
		}

		// JRESOLVE: no need to optimize aggregates, right?
		return this;
	}

	/**
	 * Code generation for CallStatementNode.
	 * The generated code will contain:
	 *		o  A generated void method for the user's method call.
	 *
	 * @param acb	The ActivationClassBuilder for the class being built
	 * @param mb	The method for the execute() method to be built
	 *
	 * @return		A compiled Expression returning a ResultSet for the call statement.
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void generate(ActivationClassBuilder acb,
								MethodBuilder mb)
							throws StandardException
	{
		JavaValueNode		methodCallBody;

		/* generate the parameters */
		generateParameterValueSet(acb);

		/* 
		 * Skip over the JavaToSQLValueNode and call generate() for the JavaValueNode.
		 * (This skips over generated code which is unnecessary since we are throwing
		 * away any return value and which won't work with void methods.)
		 * generates:
		 *     <methodCall.generate(acb)>;
		 * and adds it to userExprFun
		 */
		if (SanityManager.DEBUG)
		SanityManager.ASSERT(methodCall instanceof JavaToSQLValueNode,
					"methodCall is expected to be instanceof JavaToSQLValueNode");
		methodCallBody = ((JavaToSQLValueNode)methodCall).getJavaValueNode();

		/*
		** Tell the method call that its return value (if any) will be
		** discarded.  This is so it doesn't generate the ?: operator
		** that would return null if the receiver is null.  This is
		** important because the ?: operator cannot be made into a statement.
		*/
		methodCallBody.markReturnValueDiscarded();

		// this sets up the method
		// generates:
		// 	void userExprFun { }
		MethodBuilder userExprFun = acb.newGeneratedFun("void", Modifier.PUBLIC);
		userExprFun.addThrownException("java.lang.Exception");
		methodCallBody.generate(acb, userExprFun);
		userExprFun.endStatement();
		userExprFun.methodReturn();

		// we are done modifying userExprFun, complete it.
		userExprFun.complete();

		acb.pushGetResultSetFactoryExpression(mb);
		acb.pushMethodReference(mb, userExprFun); // first arg
		acb.pushThisAsActivation(mb); // arg 2
		mb.callMethod(VMOpcode.INVOKEINTERFACE, (String) null, "getCallStatementResultSet", ClassName.ResultSet, 2);

		/*
		** ensure all parameters have been generated
		*/
		generateParameterHolders(acb);
	}

	public ResultDescription makeResultDescription()
	{
		return null;
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

		if (methodCall != null && !v.stopTraversal())
		{
			methodCall = (ValueNode)methodCall.accept(v);
		}

		return returnNode;
	}

	/////////////////////////////////////////////////////////////////////
	//
	//	ACCESSORS
	//
	/////////////////////////////////////////////////////////////////////

	/**
	  *	Get the method call node.
	  *
	  *	@return	the method call node.
	  */
	public	MethodCallNode	getMethodCallNode()
	{
		JavaToSQLValueNode	jnode = (JavaToSQLValueNode) methodCall;

		MethodCallNode		mnode = (MethodCallNode) jnode.getJavaValueNode();

		return mnode;
	}
}

