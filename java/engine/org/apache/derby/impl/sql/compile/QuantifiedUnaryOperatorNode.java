/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.sql.compile
   (C) Copyright IBM Corp. 1997, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package	org.apache.derby.impl.sql.compile;

import org.apache.derby.iapi.sql.dictionary.DataDictionary;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.util.JBitSet;

import java.util.Vector;

/**
 * A QuantifiedUnaryOperatorNode represents a unary quantified predicate
 * that is used with a subquery, such as EXISTS and NOT EXISTS.  Quantified
 * predicates all return Boolean values.  All quantified operators will be
 * removed from the tree by the time we get to code generation - they will
 * be replaced by other constructs that can be compiled. For example,
 * an EXISTS node may be converted to a type of join.
 *
 * @author Jeff Lichtman
 */

public class QuantifiedUnaryOperatorNode extends UnaryOperatorNode
{
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1997_2004;
	/*
	** For NOT EXISTS (SELECT * ...), the parser will generate a NOT
	** node and an EXISTS node.  Later, normalization will change this
	** to a NOT_EXISTS node.
	*/
	public final static int EXISTS		= 1;
	public final static int NOT_EXISTS	= 2;

	SubqueryNode	operand;

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
			if (operand != null)
			{
				printLabel(depth, "operand: ");
				operand.treePrint(depth + 1);
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

	public ValueNode bindExpression(FromList fromList, SubqueryList subqueryList,
					Vector	aggregateVector)
				throws StandardException
	{
		operand.bind();

		/* RESOLVE: Need to bind this node */
		/* RESOLVE: Need to set the subqueryOperator in the SubqueryNode */

		return this;
	}
}
