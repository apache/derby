/*

   Derby - Class org.apache.derby.impl.sql.compile.ConditionalNode

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

import org.apache.derby.iapi.services.compiler.MethodBuilder;

import org.apache.derby.iapi.services.monitor.Monitor;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.sql.dictionary.DataDictionary;

import org.apache.derby.iapi.types.TypeId;

import org.apache.derby.iapi.types.BooleanDataValue;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.DataValueFactory;

import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.TypeId;

import org.apache.derby.iapi.services.loader.ClassInspector;

import org.apache.derby.impl.sql.compile.ExpressionClassBuilder;

import org.apache.derby.iapi.sql.compile.Visitable;
import org.apache.derby.iapi.sql.compile.Visitor;
import org.apache.derby.iapi.sql.compile.C_NodeTypes;
import org.apache.derby.iapi.reference.ClassName;


import org.apache.derby.iapi.util.JBitSet;
import org.apache.derby.iapi.services.classfile.VMOpcode;

import java.util.Vector;

/**
 * A ConditionalNode represents an if/then/else operator with a single
 * boolean expression on the "left" of the operator and a list of expressions on 
 * the "right". This is used to represent the java conditional (aka immediate if).
 *
 * @author Jerry Brenner
 */

public class ConditionalNode extends ValueNode
{
	ValueNode		testCondition;
	ValueNodeList	thenElseList;

	/**
	 * Initializer for a ConditionalNode
	 *
	 * @param testCondition		The boolean test condition
	 * @param thenElseList		ValueNodeList with then and else expressions
	 */

	public void init(Object testCondition, Object thenElseList)
	{
		this.testCondition = (ValueNode) testCondition;
		this.thenElseList = (ValueNodeList) thenElseList;
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

			if (testCondition != null)
			{
				printLabel(depth, "testCondition: ");
				testCondition.treePrint(depth + 1);
			}

			if (thenElseList != null)
			{
				printLabel(depth, "thenElseList: ");
				thenElseList.treePrint(depth + 1);
			}
		}
	}

	/**
	 * Set the clause that this node appears in.
	 *
	 * @param clause	The clause that this node appears in.
	 *
	 * @return Nothing.
	 */
	public void setClause(int clause)
	{
		super.setClause(clause);
		testCondition.setClause(clause);
		thenElseList.setClause(clause);
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
	 * @return	The new top of the expression tree.
	 *
	 * @exception StandardException		Thrown on error
	 */

	public ValueNode bindExpression(FromList fromList, SubqueryList subqueryList,
		Vector	aggregateVector) 
			throws StandardException
	{
		testCondition = testCondition.bindExpression(fromList, 
			subqueryList,
			aggregateVector);
		thenElseList.bindExpression(fromList, 
			subqueryList,
			aggregateVector);

		// Can't get the then and else expressions until after they've been bound
		ValueNode thenExpression = (ValueNode) thenElseList.elementAt(0);
		ValueNode elseExpression = (ValueNode) thenElseList.elementAt(1);

		/* testCondition must be a boolean expression.
		 * If it is a ? parameter on the left, then set type to boolean,
		 * otherwise verify that the result type is boolean.
		 */
		if (testCondition.isParameterNode())
		{
			((ParameterNode) testCondition).setDescriptor(
							new DataTypeDescriptor(
										TypeId.BOOLEAN_ID,
										true));
		}
		else
		{
			if ( ! testCondition.getTypeServices().getTypeId().equals(
														TypeId.BOOLEAN_ID))
			{
				throw StandardException.newException(SQLState.LANG_CONDITIONAL_NON_BOOLEAN);
			}
		}

		/* We can't determine the type for the result expression if
		 * all result expressions are ?s.
		 */
		if (thenElseList.containsAllParameterNodes())
		{
			throw StandardException.newException(SQLState.LANG_ALL_RESULT_EXPRESSIONS_PARAMS, "conditional");
		}
		else if (thenElseList.containsParameterNode())
		{
			/* Set the parameter's type to be the same as the other element in
			 * the list
			 */

			DataTypeDescriptor dts;
			ValueNode typeExpression;

			if (thenExpression.isParameterNode())
			{
				dts = elseExpression.getTypeServices();
			}
			else
			{
				dts = thenExpression.getTypeServices();
			}

			thenElseList.setParameterDescriptor(dts);
		}

		/* The then and else expressions must be type compatible */
		ClassInspector cu = getClassFactory().getClassInspector();

		/*
		** If it is comparable, then we are ok.  Note that we
		** could in fact allow any expressions that are convertible()
		** since we are going to generate a cast node, but that might
		** be confusing to users...
		*/

		// RESOLVE DJDOI - this looks wrong, why should the then expression
		// be comparable to the then expression ??
		if (! thenExpression.getTypeCompiler().
			 comparable(elseExpression.getTypeId(), false, getClassFactory()) &&
			! cu.assignableTo(thenExpression.getTypeId().getCorrespondingJavaTypeName(),
							  elseExpression.getTypeId().getCorrespondingJavaTypeName()) &&
			! cu.assignableTo(elseExpression.getTypeId().getCorrespondingJavaTypeName(),
							  thenExpression.getTypeId().getCorrespondingJavaTypeName()))
		{
			throw StandardException.newException(SQLState.LANG_NOT_TYPE_COMPATIBLE, 
						thenExpression.getTypeId().getSQLTypeName(),
						elseExpression.getTypeId().getSQLTypeName()
						);
		}

		/*
		** Set the result type of this conditional to be the dominant type
		** of the result expressions.
		*/
		setType(thenElseList.getDominantTypeServices());

		/*
		** Generate a CastNode if necessary and
		** stick it over the original expression
		*/
		TypeId condTypeId = getTypeId();
		TypeId thenTypeId = ((ValueNode) thenElseList.elementAt(0)).getTypeId();
		TypeId elseTypeId = ((ValueNode) thenElseList.elementAt(1)).getTypeId();

		/* Need to generate conversion if thenExpr or elseExpr is not of 
		 * dominant type.  (At least 1 of them must be of the dominant type.)
		 */
		if (thenTypeId.typePrecedence() != condTypeId.typePrecedence())
		{
			ValueNode cast = (ValueNode) getNodeFactory().getNode(
								C_NodeTypes.CAST_NODE,
								thenElseList.elementAt(0), 
								dataTypeServices,	// cast to dominant type
								getContextManager());
			cast = cast.bindExpression(fromList, 
											subqueryList,
											aggregateVector);
			
			thenElseList.setElementAt(cast, 0);
		}

		else if (elseTypeId.typePrecedence() != condTypeId.typePrecedence())
		{
			ValueNode cast = (ValueNode) getNodeFactory().getNode(
								C_NodeTypes.CAST_NODE,
								thenElseList.elementAt(1), 
								dataTypeServices,	// cast to dominant type
								getContextManager());
			cast = cast.bindExpression(fromList, 
											subqueryList,
											aggregateVector);
			
			thenElseList.setElementAt(cast, 1);
		}

		return this;
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
	 * @return		The modified expression
	 *
	 * @exception StandardException		Thrown on error
	 */
	public ValueNode preprocess(int numTables,
								FromList outerFromList,
								SubqueryList outerSubqueryList,
								PredicateList outerPredicateList) 
					throws StandardException
	{
		testCondition = testCondition.preprocess(numTables,
												 outerFromList, outerSubqueryList,
												 outerPredicateList);
 		thenElseList.preprocess(numTables,
								outerFromList, outerSubqueryList,
								outerPredicateList);
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
		 *  as we don't consider conditional operators when looking
		 * for null invariant predicates.
		 */
		if (simplePredsOnly)
		{
			return false;
		}

		boolean pushable;

		pushable = testCondition.categorize(referencedTabs, simplePredsOnly);
		pushable = (thenElseList.categorize(referencedTabs, simplePredsOnly) && pushable);
		return pushable;
	}

	/**
	 * Remap all ColumnReferences in this tree to be clones of the
	 * underlying expression.
	 *
	 * @return ValueNode			The remapped expression tree.
	 *
	 * @exception StandardException			Thrown on error
	 */
	public ValueNode remapColumnReferencesToExpressions()
		throws StandardException
	{
		testCondition = testCondition.remapColumnReferencesToExpressions();
		thenElseList = thenElseList.remapColumnReferencesToExpressions();
		return this;
	}

	/**
	 * Return whether or not this expression tree represents a constant expression.
	 *
	 * @return	Whether or not this expression tree represents a constant expression.
	 */
	public boolean isConstantExpression()
	{
		return (testCondition.isConstantExpression() &&
			    thenElseList.isConstantExpression());
	}

	/** @see ValueNode#constantExpression */
	public boolean constantExpression(PredicateList whereClause)
	{
		return (testCondition.constantExpression(whereClause) &&
			    thenElseList.constantExpression(whereClause));
	}

	/**
	 * Eliminate NotNodes in the current query block.  We traverse the tree, 
	 * inverting ANDs and ORs and eliminating NOTs as we go.  We stop at 
	 * ComparisonOperators and boolean expressions.  We invert 
	 * ComparisonOperators and replace boolean expressions with 
	 * boolean expression = false.
	 * NOTE: Since we do not recurse under ComparisonOperators, there
	 * still could be NotNodes left in the tree.
	 *
	 * @param	underNotNode		Whether or not we are under a NotNode.
	 *							
	 *
	 * @return		The modified expression
	 *
	 * @exception StandardException		Thrown on error
	 */
	ValueNode eliminateNots(boolean underNotNode) 
					throws StandardException
	{
		ValueNode thenExpression;
		ValueNode elseExpression;

		if (! underNotNode)
		{
			return this;
		}

		/* Simply swap the then and else expressions */
		thenExpression = (ValueNode) thenElseList.elementAt(0);
		elseExpression = (ValueNode) thenElseList.elementAt(1);
		thenElseList.setElementAt(elseExpression, 0);
		thenElseList.setElementAt(thenExpression, 1);

		return this;
	}

	/**
	 * Do code generation for this conditional expression.
	 *
	 * @param acb	The ExpressionClassBuilder for the class we're generating
	 * @param mb	The method the expression will go into
	 *
	 * @return	An expression to evaluate this operator
	 *
	 * @exception StandardException		Thrown on error
	 */

	public void generateExpression(ExpressionClassBuilder acb,
											MethodBuilder mb)
									throws StandardException
	{
		testCondition.generateExpression(acb, mb);
		mb.cast(ClassName.BooleanDataValue);
		mb.push(true);
		mb.callMethod(VMOpcode.INVOKEINTERFACE, (String) null, "equals", "boolean", 1);

		mb.conditionalIf();
		  ((ValueNode) thenElseList.elementAt(0)).generateExpression(acb, mb);
		mb.startElseCode();
		  ((ValueNode) thenElseList.elementAt(1)).generateExpression(acb, mb);
		mb.completeConditional();
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
		Visitable returnNode = v.visit(this);
	
		if (v.skipChildren(this))
		{
			return returnNode;
		}

		if (testCondition != null && !v.stopTraversal())
		{
			testCondition = (ValueNode)testCondition.accept(v);
		}

		if (thenElseList != null && !v.stopTraversal())
		{
			thenElseList = (ValueNodeList)thenElseList.accept(v);
		}
		
		return returnNode;
	}
}
