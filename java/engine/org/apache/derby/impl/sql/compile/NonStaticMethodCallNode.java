/*

   Derby - Class org.apache.derby.impl.sql.compile.NonStaticMethodCallNode

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

import org.apache.derby.iapi.sql.compile.CompilerContext;

import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.store.access.Qualifier;


import org.apache.derby.iapi.sql.compile.Visitable;
import org.apache.derby.iapi.sql.compile.Visitor;
import org.apache.derby.iapi.sql.compile.C_NodeTypes;

import org.apache.derby.iapi.services.loader.ClassInspector;

import org.apache.derby.iapi.services.compiler.MethodBuilder;
import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.types.JSQLType;

import org.apache.derby.impl.sql.compile.ExpressionClassBuilder;
import org.apache.derby.impl.sql.compile.ActivationClassBuilder;
import org.apache.derby.iapi.error.ExceptionSeverity;
import org.apache.derby.iapi.util.JBitSet;
import org.apache.derby.iapi.services.classfile.VMOpcode;

import org.apache.derby.catalog.AliasInfo;

import java.lang.reflect.Modifier;

import java.util.Vector;

/**
 * A NonStaticMethodCallNode is really a node to represent a (static or non-static)
 * method call from an object (as opposed to a static method call from a class.
 */
public class NonStaticMethodCallNode extends MethodCallNode
{
	/*
	** The receiver for a non-static method call is an object, represented
	** by a ValueNode.
	*/
	JavaValueNode	receiver;	

	/* Is this a static method call? Assume non-static call */
	private boolean isStatic;

	/**
	 * Initializer for a NonStaticMethodCallNode
	 *
	 * @param methodName	The name of the method to call
	 * @param receiver		A JavaValueNode representing the receiving object
	 * @exception StandardException		Thrown on error
	 */
	public void init(
							Object methodName,
							Object receiver)
			throws StandardException
	{
		super.init(methodName);

		/*
		** If the receiver is a Java value that has been converted to a
		** SQL value, get rid of the conversion and just use the Java value
		** as-is.  If the receiver is a "normal" SQL value, then convert
		** it to a Java value to use as the receiver.
		*/
		if (receiver instanceof JavaToSQLValueNode)
		{
			this.receiver = ((JavaToSQLValueNode) receiver).
										getJavaValueNode();
		}
		else
		{
			this.receiver = (JavaValueNode) getNodeFactory().
								getNode(
									C_NodeTypes.SQL_TO_JAVA_VALUE_NODE,
									receiver,
									getContextManager());
//            System.out.println("NonStaticMethodCallNode.init() receiver = "+receiver);
// get nulpointer because not .bind?
//            System.out.println("\ttypecompiler = "+((ValueNode)receiver).getTypeCompiler());
//            System.out.println("\tdtd = "+((ValueNode)receiver).getTypeServices());
//            System.out.println("\ttypeid = "+((ValueNode)receiver).getTypeServices().getTypeId());
		}
	}

	/**
	 * Bind this expression.  This means binding the sub-expressions,
	 * as well as figuring out what the return type is for this expression.
	 *
	 * @param fromList		The FROM list for the query this
	 *				expression is in, for binding columns.
	 * @param subqueryList		The subquery list being built as we find SubqueryNodes
	 * @param aggregateVector	The aggregate vector being built as we find AggregateNodes
	 *
	 * @return	this
	 *
	 * @exception StandardException		Thrown on error
	 */

	public JavaValueNode bindExpression(
		FromList fromList, SubqueryList subqueryList,
		Vector	aggregateVector) 
			throws StandardException
	{
		boolean		nullParameter = false;
		String[]	parmTypeNames;

		/* NULL and ? not allowed as receivers */
		if (receiver instanceof SQLToJavaValueNode)
		{
			ValueNode	SQLValue =
							((SQLToJavaValueNode) receiver).getSQLValueNode();

			if (SanityManager.DEBUG)
			SanityManager.ASSERT(!(SQLValue instanceof UntypedNullConstantNode),
				"UntypedNullConstantNode found as a receiver of a non-static method call");

			//
			//	We don't allow methods off of naked unnamed "?" parameters.
			//	This is because we have no way of knowing the data type of
			//	a naked "?" parameter.
			//
			//	However, if this "?" has actually been associated with a
			//	named "?paramName" parameter in a COPY PUBLICATION statement,
			//	then we have a type for it. Binding can continue.
			//

			if (SQLValue.isParameterNode())
			{
				ParameterNode	unnamedParameter = (ParameterNode) SQLValue;

				if ( unnamedParameter.getTypeServices() == null )
				{ throw StandardException.newException(SQLState.LANG_PARAMETER_RECEIVER, methodName); }
			}
		}

		bindParameters(fromList, subqueryList, aggregateVector);

		/* Now we don't allow an alias static method call here (that has to
		 * use :: sign for any static call).  If it gets here, it can't be
		 * alias static method call.
		 */
		receiver = receiver.bindExpression(fromList, subqueryList, aggregateVector);

        // Don't allow LOB types to be used as a method receiver
        String type = receiver.getJSQLType().getSQLType().getTypeId().getSQLTypeName();
        if ( type.equals("BLOB") || type.equals("CLOB") || type.equals("NCLOB") ) {
            throw StandardException.newException(SQLState.LOB_AS_METHOD_ARGUMENT_OR_RECEIVER);
        }

		javaClassName = receiver.getJavaTypeName();

		/* Not allowed to use a primitive type as a method receiver */
		if (ClassInspector.primitiveType(javaClassName))
		{
			throw StandardException.newException(SQLState.LANG_PRIMITIVE_RECEIVER, methodName, javaClassName);
		}

		/* Resolve the method call */
		resolveMethodCall(javaClassName, false);

		/* Remember if method is static */
		isStatic = Modifier.isStatic(method.getModifiers());

		return this;
	}

	/**
	 * Categorize this predicate.  Initially, this means
	 * building a bit map of the referenced tables for each predicate.
	 * If the source of this ColumnReference (at the next underlying level) 
	 * is not a ColumnReference or a VirtualColumnNode then this predicate
	 * will not be pushed down.
	 *
	 * For example, in:
	 *		select * from (select 1 from s) a (x) where x = 1
	 * we will not push down x = 1.
	 * NOTE: It would be easy to handle the case of a constant, but if the
	 * inner SELECT returns an arbitrary expression, then we would have to copy
	 * that tree into the pushed predicate, and that tree could contain
	 * subqueries and method calls.
	 * RESOLVE - revisit this issue once we have views.
	 *
	 * @param referencedTabs	JBitSet with bit map of referenced FromTables
	 * @param simplePredsOnly	Whether or not to consider method
	 *							calls, field references and conditional nodes
	 *							when building bit map
	 *
	 * @return boolean		Whether or not source.expression is a ColumnReference
	 *						or a VirtualColumnNode.
	 * @exception StandardException			Thrown on error
	 */
	public boolean categorize(JBitSet referencedTabs, boolean simplePredsOnly)
		throws StandardException
	{
		/* We stop here when only considering simple predicates
		 *  as we don't consider method calls when looking
		 * for null invariant predicates.
		 */
		if (simplePredsOnly)
		{
			return false;
		}

		boolean pushable = true;

		pushable = pushable && super.categorize(referencedTabs, simplePredsOnly);

		if (receiver != null)
		{
			pushable = pushable && receiver.categorize(referencedTabs, simplePredsOnly);
		}

		return pushable;
	}

	/**
	 * Return the variant type for the underlying expression.
	 * The variant type can be:
	 *		VARIANT				- variant within a scan
	 *							  (non-static field access)
	 *		SCAN_INVARIANT		- invariant within a scan
	 *							  (column references from outer tables)
	 *		QUERY_INVARIANT		- invariant within the life of a query
	 *							  (constant expressions)
	 *
	 * @return	The variant type for the underlying expression.
	 */
	protected int getOrderableVariantType() throws StandardException
	{
		int receiverVariant = receiver.getOrderableVariantType();

		if (receiverVariant > Qualifier.SCAN_INVARIANT) {
			
			// If the method call is related to a trigger then the return
			// values are SCAN_INVARIANT even though their calls look QUERY_INVARIANT
			// because they take no parameters.
			if (receiver.getJavaTypeName().equals("org.apache.derby.iapi.db.TriggerExecutionContext"))
				receiverVariant = Qualifier.SCAN_INVARIANT;
		}


		int thisVariant = super.getOrderableVariantType();
		if (receiverVariant < thisVariant)	//return the more variant one
			return receiverVariant;
		return thisVariant;
	}

	/**
	 * Remap all ColumnReferences in this tree to be clones of the
	 * underlying expression.
	 *
	 * @return JavaValueNode			The remapped expression tree.
	 *
	 * @exception StandardException			Thrown on error
	 */
	public JavaValueNode remapColumnReferencesToExpressions()
		throws StandardException
	{
		if (receiver != null)
		{
			receiver.remapColumnReferencesToExpressions();
		}

		return super.remapColumnReferencesToExpressions();
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
			int	parm;

			super.printSubNodes(depth);
			if (receiver != null)
			{
				printLabel(depth, "receiver :");
				receiver.treePrint(depth + 1);
			}
		}
	}

	/**
	 * Preprocess an expression tree.  We do a number of transformations
	 * here (including subqueries, IN lists, LIKE and BETWEEN) plus
	 * subquery flattening.
	 * NOTE: This is done before the outer ResultSetNode is preprocessed.
	 *
	 * @param	numTables			Number of tables in the DML Statement
	 * @param	outerFromList		FromList from outer query block
	 * @param	outerSubqueryList	SubqueryList from outer query block
	 * @param	outerPredicateList	PredicateList from outer query block
	 *
	 * @return	Nothing.
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void preprocess(int numTables,
							FromList outerFromList,
							SubqueryList outerSubqueryList,
							PredicateList outerPredicateList) 
							throws StandardException
	{
		super.preprocess(numTables,
						 outerFromList, outerSubqueryList,
						 outerPredicateList);
		receiver.preprocess(numTables,
							outerFromList, outerSubqueryList,
							outerPredicateList);

	}

	/**
	 * Do code generation for this method call
	 *
	 * @param acb	The ExpressionClassBuilder for the class we're generating
	 * @param mb	The method the expression will go into
	 *
	 *
	 * @exception StandardException		Thrown on error
	 */

	public void generateExpression(ExpressionClassBuilder acb,
											MethodBuilder mb)
									throws StandardException
	{
		boolean inConditional = false;
		/*
		** If this method returns its value to the Java domain,
		** generate the receiver and put the value in a field (only if
		** this method does not return a primitive type).  If it
		** returns its value to the SQL domain, it's up to the JavaToSQLNode
		** to call generateReceiver().
		**
		** Also, don't do this if the return value from this method
		** call will be discarded.  This is the case for a CALL statement.
		** One reason we don't want to do this for a CALL statement
		** is that the ?: operator cannot be made into a statement.
		*/
		if ( ( ! valueReturnedToSQLDomain()) && ( ! returnValueDiscarded()))
		{
			if (generateReceiver(acb, mb, receiver))
			{
				/*
				** If the above did generate the expression, let's test it for
				** a null value.
				*/
				/*
				** Generate the following to test for null:
				**
				**		(receiverExpression == null) ?
				*/

				inConditional = true;
				mb.conditionalIfNull();
				mb.pushNull(getJavaTypeName());
				mb.startElseCode();
			}
		}

		/*
		** Generate the following:
		**
		** <receiver>.<method name>(<param> (, <param> )* )
		**
		** for non-static calls.
		**
		** Refer to the field holding the receiver, if there is any.
		*/

		Class declaringClass = method.getDeclaringClass();
		
		/*
		** If it's an interface, generate an interface method call, if it's a static,
		** generate a static method call, otherwise generate a regular method call.
		*/

		short methodType;

		if (declaringClass.isInterface())
			methodType = VMOpcode.INVOKEINTERFACE;
		else if (isStatic)
			methodType = VMOpcode.INVOKESTATIC;
		else
			methodType = VMOpcode.INVOKEVIRTUAL;

		getReceiverExpression(acb, mb, receiver);
		if (isStatic)
			mb.endStatement(); // PUSHCOMPILER ???

		int nargs = generateParameters(acb, mb);

		mb.callMethod(methodType, declaringClass.getName(), methodName, getJavaTypeName(), nargs);

		if (inConditional)
			mb.completeConditional();
	}

	/**
	 * Generate the expression that evaluates to the receiver. This is
	 * for the case where a java expression is being returned to the SQL
	 * domain, and we need to check whether the receiver is null (if so,
	 * the SQL value should be set to null, and this Java expression
	 * not evaluated). Instance method calls and field references have
	 * receivers, while class method calls and calls to constructors do
	 * not. If this Java expression does not have a receiver, this method
	 * returns null.
	 *
	 * Only generate the receiver once and cache it in a field. This is
	 * because there will be two references to the receiver, and we want
	 * to evaluate it only once.
	 *
	 *
	 * @param acb	The ExpressionClassBuilder for the class being built
	 * @param mb	The method the expression will go into
	 *
	 * @return		true if compiled receiver, false otherwise.
	 *
	 * @exception StandardException		Thrown on error
	 */
	protected boolean generateReceiver(ExpressionClassBuilder acb,
											MethodBuilder mb)
									throws StandardException
	{
		/*
		** Let's pretend that a call to a static method doesn't have a
		** receiver, since the method call is actually to the class,
		** and can be made even if the receiver is null (that is, we
		** always want to call a static method, even if the receiver is null).
		*/
		if (isStatic)
			return false;
		
		return generateReceiver(acb, mb, receiver);
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

		if (receiver != null && !v.stopTraversal())
		{
			receiver = (JavaValueNode)receiver.accept(v);
		}

		return returnNode;
	}
}
