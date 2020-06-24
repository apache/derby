/*

   Derby - Class org.apache.derby.impl.sql.compile.OrNode

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

class OrNode extends BinaryLogicalOperatorNode
{
	/* Is this the 1st OR in the OR chain? */
	private boolean firstOr;

	/**
     * Constructor for an OrNode
	 *
	 * @param leftOperand	The left operand of the OR
	 * @param rightOperand	The right operand of the OR
     * @param cm            The context manager
	 */

    OrNode(ValueNode leftOperand, ValueNode rightOperand, ContextManager cm)
	{
        super(leftOperand, rightOperand, "or", cm);
		this.shortCircuitValue = true;
	}

	/**
	 * Mark this OrNode as the 1st OR in the OR chain.
	 * We will consider converting the chain to an IN list
	 * during preprocess() if all entries are of the form:
	 *		ColumnReference = expression
	 */
	void setFirstOr()
	{
		firstOr = true;
	}

	/**
	 * Bind this logical operator.  All that has to be done for binding
	 * a logical operator is to bind the operands, check that both operands
	 * are BooleanDataValue, and set the result type to BooleanDataValue.
	 *
	 * @param fromList			The query's FROM list
	 * @param subqueryList		The subquery list being built as we find SubqueryNodes
     * @param aggregates        The aggregate list being built as we find AggregateNodes
	 *
	 * @return	The new top of the expression tree.
	 *
	 * @exception StandardException		Thrown on error
	 */
    @Override
    ValueNode bindExpression(
//IC see: https://issues.apache.org/jira/browse/DERBY-6213
        FromList fromList, SubqueryList subqueryList, List<AggregateNode> aggregates)
			throws StandardException
	{
        super.bindExpression(fromList, subqueryList, aggregates);
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
    @Override
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
    ValueNode preprocess(int numTables,
								FromList outerFromList,
								SubqueryList outerSubqueryList,
								PredicateList outerPredicateList) 
					throws StandardException
	{
		super.preprocess(numTables,
						 outerFromList, outerSubqueryList, 
						 outerPredicateList);

		/* If this is the first OR in the OR chain then we will
		 * consider converting it to an IN list and then performing
		 * whatever IN list conversions/optimizations are available.
		 * An OR can be converted to an IN list if all of the entries
		 * in the chain are of the form:
		 *		ColumnReference = x
		 *	or:
		 *		x = ColumnReference
		 * where all ColumnReferences are from the same table.
         *
         * We only convert the OR chain to an IN list if it has been
         * normalized to conjunctive normal form (CNF) first. That is, the
         * shape of the chain must be something like this:
         *
         *               OR
         *              /  \
         *             =    OR
         *                 /  \
         *                =   OR
         *                    / \
         *                   =   FALSE
         *
         * Predicates in WHERE, HAVING and ON clauses will have been
         * normalized by the time we get here. Boolean expressions other
         * places in the query are not necessarily normalized, but they
         * won't benefit from IN list conversion anyway, since they cannot
         * be used as qualifiers in a multi-probe scan, so simply skip the
         * conversion in those cases.
		 */
		if (firstOr)
		{
			boolean			convert = true;
			ColumnReference	cr = null;
			int				columnNumber = -1;
			int				tableNumber = -1;
            ValueNode       vn;

            for (vn = this;
                    vn instanceof OrNode;
                    vn = ((OrNode) vn).getRightOperand())
			{
				OrNode on = (OrNode) vn;
				ValueNode left = on.getLeftOperand();

				// Is the operator an =
				if (!left.isRelationalOperator())
				{
					/* If the operator is an IN-list disguised as a relational
					 * operator then we can still convert it--we'll just
					 * combine the existing IN-list ("left") with the new IN-
					 * list values.  So check for that case now.
					 */ 

					if (SanityManager.DEBUG)
					{
						/* At the time of writing the only way a call to
						 * left.isRelationalOperator() would return false for
						 * a BinaryRelationalOperatorNode was if that node
						 * was for an IN-list probe predicate.  That's why we
						 * we can get by with the simple "instanceof" check
						 * below.  But if we're running in SANE mode, do a
						 * quick check to make sure that's still valid.
					 	 */
						if (left instanceof BinaryRelationalOperatorNode)
						{
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
                            BinaryRelationalOperatorNode bron =
                                    (BinaryRelationalOperatorNode)left;
							if (!bron.isInListProbeNode())
							{
								SanityManager.THROWASSERT(
								"isRelationalOperator() unexpectedly returned "
								+ "false for a BinaryRelationalOperatorNode.");
							}
						}
					}

					convert = (left instanceof BinaryRelationalOperatorNode);
					if (!convert)
						break;
				}

				if (!(((RelationalOperator)left).getOperator() == RelationalOperator.EQUALS_RELOP))
				{
					convert = false;
					break;
				}

				BinaryRelationalOperatorNode bron = (BinaryRelationalOperatorNode)left;

				if (bron.getLeftOperand() instanceof ColumnReference)
				{
					cr = (ColumnReference) bron.getLeftOperand();
					if (tableNumber == -1)
					{
						tableNumber = cr.getTableNumber();
						columnNumber = cr.getColumnNumber();
					}
					else if (tableNumber != cr.getTableNumber() ||
							 columnNumber != cr.getColumnNumber())
					{
						convert = false;
						break;
					}
				}
				else if (bron.getRightOperand() instanceof ColumnReference)
				{
					cr = (ColumnReference) bron.getRightOperand();
					if (tableNumber == -1)
					{
						tableNumber = cr.getTableNumber();
						columnNumber = cr.getColumnNumber();
					}
					else if (tableNumber != cr.getTableNumber() ||
							 columnNumber != cr.getColumnNumber())
					{
						convert = false;
						break;
					}
				}
				else
				{
					convert = false;
					break;
				}
			}

            // DERBY-6363: An OR chain on conjunctive normal form should be
            // terminated by a false BooleanConstantNode. If it is terminated
            // by some other kind of node, it is not on CNF, and it should
            // not be converted to an IN list.
            convert = convert && vn.isBooleanFalse();

			/* So, can we convert the OR chain? */
			if (convert)
			{
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
                ValueNodeList vnl = new ValueNodeList(getContextManager());
				// Build the IN list 
                for (vn = this;
                        vn instanceof OrNode;
                        vn = ((OrNode) vn).getRightOperand())
				{
					OrNode on = (OrNode) vn;
					BinaryRelationalOperatorNode bron =
						(BinaryRelationalOperatorNode) on.getLeftOperand();
					if (bron.isInListProbeNode())
					{
						/* If we have an OR between multiple IN-lists on the same
						 * column then just combine them into a single IN-list.
						 * Ex.
						 *
						 *   select ... from T1 where i in (2, 3) or i in (7, 10)
						 *
						 * effectively becomes:
						 *
						 *   select ... from T1 where i in (2, 3, 7, 10).
						 */
						vnl.destructiveAppend(
							bron.getInListOp().getRightOperandList());
					}
					else if (bron.getLeftOperand() instanceof ColumnReference)
					{
						vnl.addValueNode(bron.getRightOperand());
					}
					else
					{
						vnl.addValueNode(bron.getLeftOperand());
					}
				}

//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
                InListOperatorNode ilon =
                        new InListOperatorNode(cr, vnl, getContextManager());

				// Transfer the result type info to the IN list
				ilon.setType(getTypeServices());

				/* We return the result of preprocess() on the
				 * IN list so that any compilation time transformations
				 * will be done.
				 */
				return ilon.preprocess(numTables,
						 outerFromList, outerSubqueryList, 
						 outerPredicateList);
			}
		}

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
    @Override
	ValueNode eliminateNots(boolean underNotNode) 
					throws StandardException
	{
		leftOperand = leftOperand.eliminateNots(underNotNode);
		rightOperand = rightOperand.eliminateNots(underNotNode);
		if (! underNotNode)
		{
			return this;
		}

		/* Convert the OrNode to an AndNode */
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
       AndNode andNode =
                new AndNode(leftOperand, rightOperand, getContextManager());
		andNode.setType(getTypeServices());
		return andNode;
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
    @Override
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
    ValueNode changeToCNF(boolean underTopAndNode)
					throws StandardException
	{
		OrNode curOr = this;

		/* If rightOperand is an AndNode, then we must generate an 
		 * OrNode above it.
		 */
		if (rightOperand instanceof AndNode)
		{
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
           BooleanConstantNode falseNode =
                    new BooleanConstantNode(false, getContextManager());
            rightOperand =
                    new OrNode(rightOperand, falseNode, getContextManager());
			((OrNode) rightOperand).postBindFixup();
		}

		/* We need to ensure that the right chain is terminated by
		 * a false BooleanConstantNode.
		 */
		while (curOr.getRightOperand() instanceof OrNode)
		{
			curOr = (OrNode) curOr.getRightOperand();
		}

		/* Add the false BooleanConstantNode if not there yet */
		if (!(curOr.getRightOperand().isBooleanFalse()))
		{
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
           BooleanConstantNode falseNode =
                    new BooleanConstantNode(false, getContextManager());
            curOr.setRightOperand(new OrNode(
                    curOr.getRightOperand(), falseNode, getContextManager()));
			((OrNode) curOr.getRightOperand()).postBindFixup();
		}

		/* If leftOperand is an OrNode, then we modify the tree from:
		 *
		 *				this
		 *			   /	\
		 *			Or2		Nodex
		 *		   /	\		...
		 *		left2	right2
		 *
		 *	to:
		 *
		 *						this
		 *					   /	\
		 *	left2.changeToCNF()		 Or2
		 *							/	\
		 *		right2.changeToCNF()	 Nodex.changeToCNF()
		 *
		 *	NOTE: We could easily switch places between left2.changeToCNF() and 
		 *  right2.changeToCNF().
		 */

		while (leftOperand instanceof OrNode)
		{
			ValueNode newLeft;
			OrNode	  oldLeft;
			OrNode	  newRight;
			ValueNode oldRight;

			/* For "clarity", we first get the new and old operands */
			newLeft = ((OrNode) leftOperand).getLeftOperand();
			oldLeft = (OrNode) leftOperand;
			newRight = (OrNode) leftOperand;
			oldRight = rightOperand;

			/* We then twiddle the tree to match the above diagram */
			leftOperand = newLeft;
			rightOperand = newRight;
			newRight.setLeftOperand(oldLeft.getRightOperand());
			newRight.setRightOperand(oldRight);
		}

		/* Finally, we continue to normalize the left and right subtrees. */
		leftOperand = leftOperand.changeToCNF(false);
		rightOperand = rightOperand.changeToCNF(false);

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
    @Override
    boolean verifyChangeToCNF()
	{
		boolean isValid = true;

		if (SanityManager.ASSERT)
		{
			isValid = ((rightOperand instanceof OrNode) ||
					   (rightOperand.isBooleanFalse()));
			if (rightOperand instanceof OrNode)
			{
				isValid = rightOperand.verifyChangeToCNF();
			}
			if (leftOperand instanceof OrNode)
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
