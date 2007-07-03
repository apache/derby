/*

   Derby - Class org.apache.derby.impl.sql.compile.ConditionalNode

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
 */

public class ConditionalNode extends ValueNode
{
	ValueNode		testCondition;
	ValueNodeList	thenElseList;
	//true means we are here for NULLIF(V1,V2), false means we are here for following
	//CASE WHEN BooleanExpression THEN thenExpression ELSE elseExpression END
	boolean	thisIsNullIfNode;

	/**
	 * Initializer for a ConditionalNode
	 *
	 * @param testCondition		The boolean test condition
	 * @param thenElseList		ValueNodeList with then and else expressions
	 */

	public void init(Object testCondition, Object thenElseList, Object thisIsNullIfNode)
	{
		this.testCondition = (ValueNode) testCondition;
		this.thenElseList = (ValueNodeList) thenElseList;
		this.thisIsNullIfNode = ((Boolean) thisIsNullIfNode).booleanValue();
	}

	/**
	 * Prints the sub-nodes of this object.  See QueryTreeNode.java for
	 * how tree printing is supposed to work.
	 *
	 * @param depth		The depth of this node in the tree
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
	 * Checks if the provided node is a CastNode.
	 *
	 * @param node	The node to check.
	 * @return 		True if this node is a CastNode, false otherwise.
	 */
	private boolean isCastNode(ValueNode node) {
		if (node.getNodeType() == C_NodeTypes.CAST_NODE)
			return true;
		else
			return false;
	}

	/**
	 * Checks if the provided CastNode is cast to a SQL CHAR type.
	 *
	 * @param node	The CastNode to check.
	 * @return		True if this CastNode's target type is CHAR,
	 *              false otherwise.
	 * @throws StandardException 
	 */
	private boolean isCastToChar(ValueNode node) throws StandardException {
		if (node.getTypeServices().getTypeName().equals(TypeId.CHAR_NAME))
			return true;
		else
			return false;
	}

	/**
	 * Checks to see if the provided node represents
	 * a parsing of an SQL NULL.
	 *
	 * @param node  The node to check.
	 * @return      True if this node represents a SQL NULL, false otherwise.
	 */
	private boolean isNullNode(ValueNode node) {
		if (isCastNode(node) &&
			(((CastNode)node).castOperand instanceof UntypedNullConstantNode))
			return true;
		else
			return false;
	}

 	/**
	 * Checks to see if the provided node represents
	 * a ConditionalNode.
	 *
	 * @param node    The node to check.
	 * @return        True if this node is a CondtionalNode, false otherwise.
	 */
	private boolean isConditionalNode(ValueNode node) {
		if (node.getNodeType() == C_NodeTypes.CONDITIONAL_NODE)
			return true;
		else
			return false;
	}

	/**
	 * Checks to see if oldType should be casted to the newType.
	 * Returns TRUE if the two DataTypeDescriptors have different
	 * TypeID's or if the oldType is NULL.  Returns FALSE if the newType is
	 * NULL or if the two Types are identical.
	 *
	 * @param newType    The type to cast oldType to if they're different.
	 * @param oldType    The type that should be casted to the newType if
	 *                   they're different.
	 * @return           False if the newType is null or they have the same
	 *                   TypeId, true otherwise.
	 */
	private boolean shouldCast(DataTypeDescriptor newType,
		DataTypeDescriptor oldType) throws StandardException
	{
		if ((newType != null) &&
			((oldType == null) ||
			 (!oldType.getTypeId().equals(newType.getTypeId()))))
			return true;
		else
			return false;
	}

	/**
	 * This method is a 'prebind.'  We need to determine what the types of
	 * the nodes are going to be before we can set all the SQLParsed NULL's
	 * to the appropriate type.  After we bind, however, we want to ignore
	 * the SQLParsed NULL's which will be bound to CHAR.  Also, we might
	 * have to delve into the CASE Expression tree.
	 *
	 * @param thenElseList    The thenElseList (recursive method)
	 * @param fromList        The fromList (required for Column References).
	 *
	 * @exception             StandardException Thrown on error.
	 */
	private DataTypeDescriptor findType(ValueNodeList thenElseList,
		FromList fromList) throws StandardException
	{
		/* We need to "prebind" because we want the Types.  Provide
		 * dummy SubqueryList and AggreateList (we don't care)
		 */

		ValueNode thenNode =
			((ValueNode)thenElseList.elementAt(0)).bindExpression(
				fromList, new SubqueryList(), new Vector());

		ValueNode elseNode =
			((ValueNode)thenElseList.elementAt(1)).bindExpression(
				fromList, new SubqueryList(), new Vector());

		DataTypeDescriptor thenType = thenNode.getTypeServices();
		DataTypeDescriptor elseType = elseNode.getTypeServices();
		DataTypeDescriptor theType = null;

		/* If it's not a Cast Node or a Conditional Node, then we'll
		 * use this type.
		 */
		if ((thenType != null) && !isCastNode(thenNode)
			&& !isConditionalNode(thenNode))
		{
			return thenType;
		}

		/* If it's not cast to CHAR it isn't a SQL parsed NULL, so
		 * we can use it.
		 */
		if (isCastNode(thenNode) && !isCastToChar(thenNode))
			return thenNode.getTypeServices();

		/* If we get here, we can't use the THEN node type, so we'll
		 * use the ELSE node type
		 */
		if ((elseType != null) && !isCastNode(elseNode)
			&& !isConditionalNode(elseNode))
		{
			return elseType;
		}

		if (isCastNode(elseNode) && !isCastToChar(elseNode))
			return elseNode.getTypeServices();

		/* If we get here, it means that we've got a conditional and a
		 * SQL parsed NULL or two conditionals.
		 */
		if (isConditionalNode(thenNode))
		{
			theType =
				findType(((ConditionalNode)thenNode).thenElseList, fromList);
		}

		if (theType != null) return theType;

		// Two conditionals and the first one was all SQL parsed NULLS.
		if (isConditionalNode(elseNode))
		{
			theType =
				findType(((ConditionalNode)elseNode).thenElseList, fromList);
		}

		if (theType != null) return theType;
		return null;
	}
	/**
	 * This recursive method will hunt through the ValueNodeList thenElseList
	 * looking for SQL NULL's.  If it finds any, it casts them to the provided
	 * castType.
	 *
	 * @param thenElseList    The thenElseList to update.
	 * @param castType        The type to cast SQL parsed NULL's too.
	 *
	 * @exception             StandardException Thrown on error.
	 */
	private void recastNullNodes(ValueNodeList thenElseList,
	                           DataTypeDescriptor castType)
	 throws StandardException {

		// Don't do anything if we couldn't find a castType.
		if (castType == null) return;

		ValueNode thenNode = (ValueNode)thenElseList.elementAt(0);
		ValueNode elseNode = (ValueNode)thenElseList.elementAt(1);

		// first check if the "then" node is NULL
		if (isNullNode(thenNode) &&
		    shouldCast(castType, thenNode.getTypeServices()))
		{
			thenElseList.setElementAt(recastNullNode(thenNode, castType), 0);
		// otherwise recurse on thenNode, but only if it's a conditional
		} else if (isConditionalNode(thenNode)) {
			recastNullNodes(((ConditionalNode)thenNode).thenElseList,
			                castType);
		}

		// lastly, check if the "else" node is NULL
		if (isNullNode(elseNode) &&
		    shouldCast(castType, elseNode.getTypeServices()))
		{
			thenElseList.setElementAt(recastNullNode(elseNode, castType), 1);
		// otherwise recurse on elseNode, but only if it's a conditional
		} else if (isConditionalNode(elseNode)) {
			recastNullNodes(((ConditionalNode)elseNode).thenElseList,
			                castType);
		}
	}

	/**
	 * recastNullNode casts the nodeToCast node to the typeToUse.
	 *
	 * recastNullNode is called by recastNullNodes.  It is called when the
	 * nodeToCast is an UntypedNullConstantNode that's been cast by the
	 * SQLParser to a CHAR.  The node needs to be recasted to the same type
	 * of the other nodes in order to prevent the type compatibility error
	 * 42X89 from occuring.  SQL Standard requires that:
	 *
	 *  VALUES CASE WHEN 1=2 THEN 3 ELSE NULL END
	 *
	 * returns NULL and not an error message.
	 *
	 * @param nodeToCast    The node that represents a SQL NULL value.
	 * @param typeToUse     The type which the nodeToCast should be
	 *                      recasted too.
	 *
	 * @exception StandardException Thrown on error.
	 */
	private QueryTreeNode recastNullNode(ValueNode nodeToCast,
		DataTypeDescriptor typeToUse) throws StandardException
	{
		QueryTreeNode cast = getNodeFactory().getNode(
					C_NodeTypes.CAST_NODE,
					((CastNode)nodeToCast).castOperand,
					typeToUse,
					getContextManager());
		return cast;
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

		if (thisIsNullIfNode) {
			//for NULLIF(V1,V2), parser binds thenElseList.elementAt(0) to untyped NULL
			//At bind phase, we should bind it to the type of V1 since now we know the
			//type of V1  
			BinaryComparisonOperatorNode bcon = (BinaryComparisonOperatorNode)testCondition;
			
			/* 
			 * NULLIF(V1,V2) is equivalent to: 
			 * 
			 *    CASE WHEN V1=V2 THEN NULL ELSE V1 END
			 * 
			 * The untyped NULL should have a data type descriptor
			 * that allows its value to be nullable.
			 */
			QueryTreeNode cast = getNodeFactory().getNode(
						C_NodeTypes.CAST_NODE,
						thenElseList.elementAt(0), 
						bcon.getLeftOperand().getTypeServices().getNullabilityType(true),
						getContextManager());
			thenElseList.setElementAt(cast,0);
		} else {
			recastNullNodes(thenElseList, findType(thenElseList, fromList));
 		}

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
		if (testCondition.requiresTypeFromContext())
		{
			testCondition.setType(
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

			if (thenExpression.requiresTypeFromContext())
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
		if (! thenExpression.getTypeServices().
			 comparable(elseExpression.getTypeServices(), false, getClassFactory()) &&
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
                                getTypeServices(),	// cast to dominant type
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
                                getTypeServices(),	// cast to dominant type
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
        
	/**
	 * {@inheritDoc}
	 */
	protected boolean isEquivalent(ValueNode o) throws StandardException
	{
		if (isSameNodeType(o)) 
		{
			ConditionalNode other = (ConditionalNode)o;
			if (thenElseList.size() == other.thenElseList.size()
					&& (testCondition.isEquivalent(other.testCondition))) 
			{
				int sz = thenElseList.size();
				for (int i = 0; i < sz; i++)
				{
					ValueNode v1 = (ValueNode)thenElseList.elementAt(i);
					ValueNode v2 = (ValueNode)other.thenElseList.elementAt(i);
					if (!v1.isEquivalent(v2)) 
					{
						return false;
					}
					
				}
				return true;
			}
		}
		return false;
	}
}
