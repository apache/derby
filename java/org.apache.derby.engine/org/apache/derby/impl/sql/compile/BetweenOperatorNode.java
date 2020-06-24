/*

   Derby - Class org.apache.derby.impl.sql.compile.BetweenOperatorNode

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

import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.iapi.services.compiler.MethodBuilder;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.shared.common.sanity.SanityManager;

/**
 * A BetweenOperatorNode represents a BETWEEN clause. The between values are
 * represented as a 2 element list in order to take advantage of code reuse.
 *
 */

//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
class BetweenOperatorNode extends BinaryListOperatorNode
{
    /**
     * @param leftOperand The left operand of the node
     * @param betweenValues The between values in list form
     * @param cm
     * @throws StandardException
     */
    BetweenOperatorNode(ValueNode leftOperand,
            ValueNodeList betweenValues,
            ContextManager cm) throws StandardException {
        super(leftOperand, vetValues(betweenValues), "BETWEEN", null, cm);
    }

    private static ValueNodeList vetValues(ValueNodeList betweenValues) {
        if (SanityManager.DEBUG)
		{
            ValueNodeList betweenVals = betweenValues;

			SanityManager.ASSERT(betweenVals.size() == 2,
				"betweenValues.size() (" +
				betweenVals.size()	+
				") is expected to be 2");
		}
        return betweenValues;
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
		BinaryComparisonOperatorNode leftBCO;
		BinaryComparisonOperatorNode rightBCO;
		OrNode						 newOr;

		if (SanityManager.DEBUG)
		SanityManager.ASSERT(rightOperandList.size() == 2,
			"rightOperandList.size() (" +
			rightOperandList.size()	+
			") is expected to be 2");

		if (! underNotNode)
		{
			return this;
		}

		/* we want to convert the BETWEEN  * into < OR > 
		   as described below.
		*/		

		/* Convert:
		 *		leftO between rightOList.elementAt(0) and rightOList.elementAt(1)
		 * to:
		 *		leftO < rightOList.elementAt(0) or leftO > rightOList.elementAt(1)
		 * NOTE - We do the conversion here since ORs will eventually be
		 * optimizable and there's no benefit for the optimizer to see NOT BETWEEN
		 */

		ContextManager cm = getContextManager();

		/* leftO < rightOList.elementAt(0) */
        leftBCO = new BinaryRelationalOperatorNode(
//IC see: https://issues.apache.org/jira/browse/DERBY-673
                BinaryRelationalOperatorNode.K_LESS_THAN,
                leftOperand,
                rightOperandList.elementAt(0),
                false,
                cm);
		/* Set type info for the operator node */
		leftBCO.bindComparisonOperator();

        // DERBY-4388: If leftOperand is a ColumnReference, it may be remapped
        // during optimization, and that requires the less-than node and the
        // greater-than node to have separate objects.
        ValueNode leftClone = (leftOperand instanceof ColumnReference) ?
            leftOperand.getClone() : leftOperand;

		/* leftO > rightOList.elementAt(1) */
        rightBCO = new BinaryRelationalOperatorNode(
//IC see: https://issues.apache.org/jira/browse/DERBY-673
                BinaryRelationalOperatorNode.K_GREATER_THAN,
                leftClone,
                rightOperandList.elementAt(1),
                false,
                cm);
		/* Set type info for the operator node */
		rightBCO.bindComparisonOperator();

		/* Create and return the OR */
        newOr = new OrNode(leftBCO, rightBCO, cm);
		newOr.postBindFixup();

		/* Tell optimizer to use the between selectivity instead of >= * <= selectivities */
		leftBCO.setBetweenSelectivity();
		rightBCO.setBetweenSelectivity();

		return newOr;
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
		ValueNode	leftClone1;
		ValueNode	rightOperand;

		/* We must 1st preprocess the component parts */
		super.preprocess(numTables,
						 outerFromList, outerSubqueryList,
						 outerPredicateList);

		/* This is where we do the transformation for BETWEEN to make it optimizable.
		 * c1 BETWEEN value1 AND value2 -> c1 >= value1 AND c1 <= value2
		 * This transformation is only done if the leftOperand is a ColumnReference.
		 */
		if (!(leftOperand instanceof ColumnReference))
		{
			return this;
		}

		/* For some unknown reason we need to clone the leftOperand if it is
		 * a ColumnReference because reusing them in Qualifiers for a scan
		 * does not work.  
		 */
		leftClone1 = leftOperand.getClone();

		/* The transformed tree has to be normalized:
		 *				AND
		 *			   /   \
		 *			  >=    AND
		 *				   /   \
		 *				  <=    TRUE
		 */

		ContextManager cm = getContextManager();

        BooleanConstantNode trueNode = new BooleanConstantNode(true, cm);

		/* Create the AND <= */
		BinaryComparisonOperatorNode lessEqual = 
            new BinaryRelationalOperatorNode(
//IC see: https://issues.apache.org/jira/browse/DERBY-673
                BinaryRelationalOperatorNode.K_LESS_EQUALS,
                leftClone1,
                rightOperandList.elementAt(1),
                false,
                cm);

		/* Set type info for the operator node */
		lessEqual.bindComparisonOperator();

		/* Create the AND */
       AndNode newAnd = new AndNode(lessEqual, trueNode, cm);
		newAnd.postBindFixup();

		/* Create the AND >= */
		BinaryComparisonOperatorNode greaterEqual = 
            new BinaryRelationalOperatorNode(
//IC see: https://issues.apache.org/jira/browse/DERBY-673
                BinaryRelationalOperatorNode.K_GREATER_EQUALS,
                leftOperand,
                rightOperandList.elementAt(0),
                false,
                cm);

		/* Set type info for the operator node */
		greaterEqual.bindComparisonOperator();

		/* Create the AND */
       newAnd = new AndNode(greaterEqual, newAnd, cm);
		newAnd.postBindFixup();

		/* Tell optimizer to use the between selectivity instead of >= * <= selectivities */
		lessEqual.setBetweenSelectivity();
		greaterEqual.setBetweenSelectivity();

		return newAnd;
	}
 
	/**
	 * Do code generation for this BETWEEN operator.
	 *
	 * @param acb	The ExpressionClassBuilder for the class we're generating
	 * @param mb	The method the code to place the code
	 *
	 *
	 * @exception StandardException		Thrown on error
	 */
    @Override
    void generateExpression(ExpressionClassBuilder acb, MethodBuilder mb)
		throws StandardException
	{
		AndNode						 newAnd;
		BinaryComparisonOperatorNode leftBCO;
		BinaryComparisonOperatorNode rightBCO;

		if (SanityManager.DEBUG)
		SanityManager.ASSERT(rightOperandList.size() == 2,
			"rightOperandList.size() (" +
			rightOperandList.size()	+
			") is expected to be 2");

		/* Convert:
		 *		leftO between rightOList.elementAt(0) and rightOList.elementAt(1)
		 * to:
		 *		leftO >= rightOList.elementAt(0) and leftO <= rightOList.elementAt(1) 
		 */

		ContextManager cm = getContextManager();

		/* leftO >= rightOList.elementAt(0) */
        leftBCO = new BinaryRelationalOperatorNode(
//IC see: https://issues.apache.org/jira/browse/DERBY-673
                BinaryRelationalOperatorNode.K_GREATER_EQUALS,
                leftOperand,
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-673
                rightOperandList.elementAt(0),
                false,
                cm);
		/* Set type info for the operator node */
		leftBCO.bindComparisonOperator();

		/* leftO <= rightOList.elementAt(1) */
        rightBCO = new BinaryRelationalOperatorNode(
                BinaryRelationalOperatorNode.K_LESS_EQUALS,
                leftOperand,
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-673
                rightOperandList.elementAt(1),
                false,
                cm);
		/* Set type info for the operator node */
		rightBCO.bindComparisonOperator();

		/* Create and return the AND */
        newAnd = new AndNode(leftBCO, rightBCO, cm);
		newAnd.postBindFixup();
		newAnd.generateExpression(acb, mb);
	}
}
