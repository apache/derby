/*

   Derby - Class org.apache.derby.impl.sql.compile.AndNode

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

import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.sql.compile.NodeFactory;
import org.apache.derby.iapi.sql.compile.C_NodeTypes;

import java.util.Vector;

public class AndNode extends BinaryLogicalOperatorNode
{

	/**
	 * Initializer for an AndNode
	 *
	 * @param leftOperand	The left operand of the AND
	 * @param rightOperand	The right operand of the AND
	 */

	public void init(Object leftOperand, Object rightOperand)
	{
		super.init(leftOperand, rightOperand, Boolean.FALSE, "and");
	}

	/**
	 * Bind this logical operator.  All that has to be done for binding
	 * a logical operator is to bind the operands, check that both operands
	 * are BooleanDataValue, and set the result type to BooleanDataValue.
	 *
	 * @param fromList			The query's FROM list
	 * @param subqueryList		The subquery list being built as we find SubqueryNodes
	 * @param aggregateVector	The aggregate vector being built as we find AggregateNodes
	 *
	 * @return	The new top of the expression tree.
	 *
	 * @exception StandardException		Thrown on error
	 */

	public ValueNode bindExpression(
		FromList fromList, SubqueryList subqueryList,
		Vector aggregateVector)
			throws StandardException
	{
		super.bindExpression(fromList, subqueryList, aggregateVector);

		postBindFixup();
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
		/* If the left child is an OR, then mark it as the 1st OR in
		 * the list.  That will allow us to consider converting the OR
		 * to an IN list when we preprocess the 1st OR in the list.
		 */
		if (leftOperand instanceof OrNode)
		{
			((OrNode) leftOperand).setFirstOr();
		}
		leftOperand = leftOperand.preprocess(numTables,
											 outerFromList, outerSubqueryList,
											 outerPredicateList);
		/* We need to rerun the changeToCNF() phase if our left operand
		 * is an AndNode.  This can happen due to a predicate transformation,
		 * such as the ones for LIKE and BETWEEN, underneath us.
		 */
		if (leftOperand instanceof AndNode)
		{
			changeToCNF(false);
		}
		rightOperand = rightOperand.preprocess(numTables,
											   outerFromList, outerSubqueryList,
											   outerPredicateList);
		return this;
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
		leftOperand = leftOperand.eliminateNots(underNotNode);
		rightOperand = rightOperand.eliminateNots(underNotNode);
		if (! underNotNode)
		{
			return this;
		}

		/* Convert the AndNode to an OrNode */
		ValueNode	orNode;

		orNode = (ValueNode) getNodeFactory().getNode(
												C_NodeTypes.OR_NODE,
												leftOperand,
												rightOperand,
												getContextManager());
		orNode.setType(dataTypeServices);
		return orNode;
	}

	/**
	 * Do the 1st step in putting an expression into conjunctive normal
	 * form.  This step ensures that the top level of the expression is
	 * a chain of AndNodes terminated by a true BooleanConstantNode.
	 *
	 * @return		The modified expression
	 *
	 * @exception StandardException		Thrown on error
	 */
	public ValueNode putAndsOnTop() 
					throws StandardException
	{
		if (SanityManager.DEBUG)
		SanityManager.ASSERT(rightOperand != null,
			"rightOperand is expected to be non-null");
		rightOperand = rightOperand.putAndsOnTop();

		return this;
	}

	/**
	 * Verify that putAndsOnTop() did its job correctly.  Verify that the top level 
	 * of the expression is a chain of AndNodes terminated by a true BooleanConstantNode.
	 *
	 * @return		Boolean which reflects validity of the tree.
	 */
	public boolean verifyPutAndsOnTop()
	{
		boolean isValid = true;

		if (SanityManager.ASSERT)
		{
			isValid = ((rightOperand instanceof AndNode) ||
					   (rightOperand.isBooleanTrue()));

			if (rightOperand instanceof AndNode)
			{
				isValid = rightOperand.verifyPutAndsOnTop();
			}
		}

		return isValid;
	}

	/**
	 * Finish putting an expression into conjunctive normal
	 * form.  An expression tree in conjunctive normal form meets
	 * the following criteria:
	 *		o  If the expression tree is not null,
	 *		   the top level will be a chain of AndNodes terminating
	 *		   in a true BooleanConstantNode.
	 *		o  The left child of an AndNode will never be an AndNode.
	 *		o  Any right-linked chain that includes an AndNode will
	 *		   be entirely composed of AndNodes terminated by a true BooleanConstantNode.
	 *		o  The left child of an OrNode will never be an OrNode.
	 *		o  Any right-linked chain that includes an OrNode will
	 *		   be entirely composed of OrNodes terminated by a false BooleanConstantNode.
	 *		o  ValueNodes other than AndNodes and OrNodes are considered
	 *		   leaf nodes for purposes of expression normalization.
	 *		   In other words, we won't do any normalization under
	 *		   those nodes.
	 *
	 * In addition, we track whether or not we are under a top level AndNode.  
	 * SubqueryNodes need to know this for subquery flattening.
	 *
	 * @param	underTopAndNode		Whether or not we are under a top level AndNode.
	 *							
	 *
	 * @return		The modified expression
	 *
	 * @exception StandardException		Thrown on error
	 */
	public ValueNode changeToCNF(boolean underTopAndNode) 
					throws StandardException
	{
		AndNode curAnd = this;

		/* Top chain will be a chain of Ands terminated by a non-AndNode.
		 * (putAndsOnTop() has taken care of this. If the last node in
		 * the chain is not a true BooleanConstantNode then we need to do the
		 * transformation to make it so.
		 */

		/* Add the true BooleanConstantNode if not there yet */
		if (!(rightOperand instanceof AndNode) &&
			!(rightOperand.isBooleanTrue()))
		{
			BooleanConstantNode	trueNode;

			trueNode = (BooleanConstantNode) getNodeFactory().getNode(
											C_NodeTypes.BOOLEAN_CONSTANT_NODE,
											Boolean.TRUE,
											getContextManager());
			curAnd.setRightOperand(
					(ValueNode) getNodeFactory().getNode(
											C_NodeTypes.AND_NODE,
											curAnd.getRightOperand(),
											trueNode,
											getContextManager()));
			((AndNode) curAnd.getRightOperand()).postBindFixup();
		}

		/* If leftOperand is an AndNode, then we modify the tree from:
		 *
		 *				this
		 *			   /	\
		 *			And2	Nodex
		 *		   /	\		...
		 *		left2	right2
		 *
		 *	to:
		 *
		 *						this
		 *					   /	\
		 *	left2.changeToCNF()		 And2
		 *							/	\
		 *		right2.changeToCNF()	  Nodex.changeToCNF()
		 *
		 *	NOTE: We could easily switch places between left2.changeToCNF() and 
		 *  right2.changeToCNF().
		 */

		/* Pull up the AndNode chain to our left */
		while (leftOperand instanceof AndNode)
		{
			ValueNode newLeft;
			AndNode	  oldLeft;
			AndNode	  newRight;
			ValueNode oldRight;

			/* For "clarity", we first get the new and old operands */
			newLeft = ((AndNode) leftOperand).getLeftOperand();
			oldLeft = (AndNode) leftOperand;
			newRight = (AndNode) leftOperand;
			oldRight = rightOperand;

			/* We then twiddle the tree to match the above diagram */
			leftOperand = newLeft;
			rightOperand = newRight;
			newRight.setLeftOperand(oldLeft.getRightOperand());
			newRight.setRightOperand(oldRight);
		}

		/* Finally, we continue to normalize the left and right subtrees. */
		leftOperand = leftOperand.changeToCNF(underTopAndNode);
		rightOperand = rightOperand.changeToCNF(underTopAndNode);

		return this;
	}

	/**
	 * Verify that changeToCNF() did its job correctly.  Verify that:
	 *		o  AndNode  - rightOperand is not instanceof OrNode
	 *				      leftOperand is not instanceof AndNode
	 *		o  OrNode	- rightOperand is not instanceof AndNode
	 *					  leftOperand is not instanceof OrNode
	 *
	 * @return		Boolean which reflects validity of the tree.
	 */
	public boolean verifyChangeToCNF()
	{
		boolean isValid = true;

		if (SanityManager.ASSERT)
		{
			isValid = ((rightOperand instanceof AndNode) ||
					   (rightOperand.isBooleanTrue()));
			if (rightOperand instanceof AndNode)
			{
				isValid = rightOperand.verifyChangeToCNF();
			}
			if (leftOperand instanceof AndNode)
			{
				isValid = false;
			}
			else
			{
				isValid = isValid && leftOperand.verifyChangeToCNF();
			}
		}

		return isValid;
	}

	/**
	 * Do bind() by hand for an AndNode that was generated after bind(),
	 * eg by putAndsOnTop(). (Set the data type and nullability info.)
	 *
	 * @return	None.
	 *
	 * @exception StandardException		Thrown on error
	 */
	void postBindFixup()
					throws StandardException
	{
		setType(resolveLogicalBinaryOperator(
							leftOperand.getTypeServices(),
							rightOperand.getTypeServices()
											)
				);
	}
}
