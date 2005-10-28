/*

   Derby - Class org.apache.derby.impl.sql.compile.QuantifiedBinaryOperatorNode

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

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.util.JBitSet;

import java.util.Vector;

/**
 * A QuantifiedBinaryOperatorNode represents a binary quantified predicate
 * that is used with a subquery, such as IN, NOT IN, < ALL, etc.  Quantified
 * predicates all return Boolean values.  All quantified operators will be
 * removed from the tree by the time we get to code generation - they will
 * be replaced by other constructs that can be compiled. For example,
 * an IN node may be converted to a type of join.
 *
 * @author Jeff Lichtman
 */

public class QuantifiedBinaryOperatorNode extends BinaryOperatorNode
{
	int	operator;

	public final static int IN	= 1;
	public final static int NOT_IN	= 2;
	public final static int EQ_ANY	= 3;
	public final static int EQ_ALL	= 4;
	public final static int NE_ANY	= 5;
	public final static int NE_ALL	= 6;
	public final static int GT_ANY	= 7;
	public final static int GT_ALL	= 8;
	public final static int GE_ANY	= 9;
	public final static int GE_ALL	= 10;
	public final static int LT_ANY = 11;
	public final static int LT_ALL	= 12;
	public final static int LE_ANY	= 13;
	public final static int LE_ALL	= 14;

	ValueNode	leftOperand;
	SubqueryNode	rightOperand;

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

			if (leftOperand != null)
			{
				printLabel(depth, "leftOperand: ");
				leftOperand.treePrint(depth + 1);
			}

			if (rightOperand != null)
			{
				printLabel(depth, "rightOperand: ");
				rightOperand.treePrint(depth + 1);
			}
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
	 * @return	The new top of the expression tree.
	 *
	 * @exception StandardException		Thrown on error
	 */

	public ValueNode bindExpression(
		FromList fromList, SubqueryList subqueryList,
		Vector	aggregateVector) 
			throws StandardException
	{
		leftOperand = leftOperand.bindExpression(fromList, subqueryList,
									aggregateVector);
		rightOperand = (SubqueryNode) rightOperand.bindExpression(fromList, subqueryList,
									aggregateVector);

		/* RESOLVE: Need to bind this node */
		/* RESOLVE - set the subqueryOperator in the SubqueryNode */

		return this;
	}
}
