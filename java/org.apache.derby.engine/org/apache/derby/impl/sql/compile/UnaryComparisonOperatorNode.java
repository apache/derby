/*

   Derby - Class org.apache.derby.impl.sql.compile.UnaryComparisonOperatorNode

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
import org.apache.derby.iapi.services.compiler.MethodBuilder;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derby.iapi.sql.compile.ExpressionClassBuilderInterface;
import org.apache.derby.iapi.sql.compile.Optimizable;
import org.apache.derby.iapi.sql.dictionary.ConglomerateDescriptor;
import org.apache.derby.iapi.store.access.ScanController;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.TypeId;
import org.apache.derby.iapi.util.JBitSet;

/**
 * This node is the superclass  for all unary comparison operators, such as is null
 * and is not null.
 *
 */

public abstract class UnaryComparisonOperatorNode extends UnaryOperatorNode
{
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
    UnaryComparisonOperatorNode(ValueNode operator, ContextManager cm)
            throws StandardException {
        super(operator, cm);
    }

	/**
	 * Bind this comparison operator.  All that has to be done for binding
	 * a comparison operator is to bind the operand and set the result type 
	 * to SQLBoolean.
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
        bindOperand(fromList, subqueryList, aggregates);

		/* Set type info for this node */
		bindComparisonOperator();

		return this;
	}

	/**
	 * Set the type info for this node.  This method is useful both during 
	 * binding and when we generate nodes within the language module outside 
	 * of the parser.
	 *
	 * @exception StandardException		Thrown on error
	 */
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
    void bindComparisonOperator()
			throws StandardException
	{
		/*
		** Set the result type of this comparison operator based on the
		** operand.  The result type is always SQLBoolean and always
		** non-nullable.
		*/
		setType(new DataTypeDescriptor(TypeId.BOOLEAN_ID, false));
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
		if (! underNotNode)
		{
			return this;
		}

		/* Convert the BinaryComparison operator to its negation */
		return getNegation(operand);
	}

	/**
	 * Negate the comparison.
	 *
	 * @param operand	The operand of the comparison operator
	 *
	 * @return BinaryOperatorNode	The negated expression
	 *
	 * @exception StandardException		Thrown on error
	 */
//IC see: https://issues.apache.org/jira/browse/DERBY-4412
	abstract UnaryOperatorNode getNegation(ValueNode operand)
				throws StandardException;

	/* RelationalOperator interface */

	/** @see RelationalOperator#getColumnOperand */
	public ColumnReference getColumnOperand(
								Optimizable optTable,
								int columnPosition)
	{
		FromBaseTable	ft;

		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(optTable instanceof FromBaseTable);
		}

		ft = (FromBaseTable) optTable;
		ColumnReference	cr;
		if (operand instanceof ColumnReference)
		{
			/*
			** The operand is a column reference.
			** Is it the correct column?
			*/
			cr = (ColumnReference) operand;
			if (cr.getTableNumber() == ft.getTableNumber())
			{
				/* The table is correct, how about the column position? */
				if (cr.getSource().getColumnPosition() == columnPosition)
				{
					/* We've found the correct column - return it */
					return cr;
				}
			}
		}

		/* Neither side is the column we're looking for */
		return null;
	}

	/** @see RelationalOperator#getColumnOperand */
	public ColumnReference getColumnOperand(Optimizable optTable)
	{
		ColumnReference	cr;

		if (operand instanceof ColumnReference)
		{
			/*
			** The operand is a column reference.
			** Is it the correct column?
			*/
			cr = (ColumnReference) operand;
			if (cr.getTableNumber() == optTable.getTableNumber())
			{
				/* We've found the correct column - return it */
				return cr;
			}
		}

		/* Not the column we're looking for */
		return null;
	}

	/** @see RelationalOperator#getOperand */
	public ValueNode getOperand(ColumnReference cRef, int refSetSize,
		boolean otherSide)
	{
		if (otherSide)
		// there is no "other" side for Unary, so just return null.
			return null;

		ColumnReference	cr;
		if (operand instanceof ColumnReference)
		{
			/*
			** The operand is a column reference.
			** Is it the correct column?
			*/
			JBitSet cRefTables = new JBitSet(refSetSize);
			JBitSet crTables = new JBitSet(refSetSize);
			BaseTableNumbersVisitor btnVis =
				new BaseTableNumbersVisitor(crTables);

			cr = (ColumnReference) operand;
			try {
				cr.accept(btnVis);
				btnVis.setTableMap(cRefTables);
				cRef.accept(btnVis);
			} catch (StandardException se) {
            	if (SanityManager.DEBUG)
            	{
            	    SanityManager.THROWASSERT("Failed when trying to " +
//IC see: https://issues.apache.org/jira/browse/DERBY-2581
            	        "find base table number for column reference check:",
						se);
            	}
			}
			crTables.and(cRefTables);
			if (crTables.getFirstSetBit() != -1)
			{
				/*
				** The table is correct, how about the column position?
				*/
				if (cr.getSource().getColumnPosition() ==
					cRef.getColumnNumber())
				{
					/* We've found the correct column - return it. */
					return operand;
				}
			}
		}

		/* Not the column we're looking for */
		return null;
	}

	/** @see RelationalOperator#selfComparison */
	public boolean selfComparison(ColumnReference cr)
	{
		ValueNode	otherSide;
		JBitSet		tablesReferenced;

		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(cr == operand,
				"ColumnReference not found in IsNullNode.");
		}

		/* An IsNullNode is not a comparison with any other column */
		return false;
	}

	/**
	 * @see RelationalOperator#getExpressionOperand
	 */
	public ValueNode getExpressionOperand(int tableNumber,
										  int columnNumber,
										  Optimizable ft)
	{
		return null;
	}

	/**
	 * @see RelationalOperator#generateExpressionOperand
	 *
	 * @exception StandardException		Thrown on error
	 */
    public void generateExpressionOperand(Optimizable optTable,
                                          int columnPosition,
                                          ExpressionClassBuilderInterface acbi,
                                          MethodBuilder mb)
						throws StandardException
	{
        ExpressionClassBuilder acb = (ExpressionClassBuilder) acbi;
//IC see: https://issues.apache.org/jira/browse/DERBY-2583
		acb.generateNull(mb, operand.getTypeCompiler(), 
				operand.getTypeServices().getCollationType());
	}

	/** @see RelationalOperator#getStartOperator */
	public int getStartOperator(Optimizable optTable)
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.THROWASSERT(
							"getStartOperator not expected to be called for " +
							this.getClass().getName());
		}

		return ScanController.GE;
	}

	/** @see RelationalOperator#getStopOperator */
	public int getStopOperator(Optimizable optTable)
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.THROWASSERT(
							"getStopOperator not expected to be called for " +
							this.getClass().getName());
		}

		return ScanController.GT;
	}

	/** @see RelationalOperator#generateOrderedNulls */
	public void generateOrderedNulls(MethodBuilder mb)
	{
		mb.push(true);
	}

	/**
	 * @see RelationalOperator#generateQualMethod
	 *
	 * @exception StandardException		Thrown on error
	 */
    public void generateQualMethod(ExpressionClassBuilderInterface acbi,
										MethodBuilder mb,
										Optimizable optTable)
						throws StandardException
	{
        ExpressionClassBuilder acb = (ExpressionClassBuilder) acbi;
		MethodBuilder qualMethod = acb.newUserExprFun();

		/* Generate a method that returns that expression */
//IC see: https://issues.apache.org/jira/browse/DERBY-2583
		acb.generateNull(qualMethod, operand.getTypeCompiler(),
				operand.getTypeServices().getCollationType());
		qualMethod.methodReturn();
		qualMethod.complete();

		/* Return an expression that evaluates to the GeneratedMethod */
		acb.pushMethodReference(mb, qualMethod);
	}

	/** @see RelationalOperator#generateAbsoluteColumnId */
	public void generateAbsoluteColumnId(MethodBuilder mb,
										Optimizable optTable)
	{
		// Get the absolute 0-based column position for the column
		int columnPosition = getAbsoluteColumnPosition(optTable);

		mb.push(columnPosition);
	}

	/** @see RelationalOperator#generateRelativeColumnId */
	public void generateRelativeColumnId(MethodBuilder mb,
											   Optimizable optTable)
	{
		// Get the absolute 0-based column position for the column
		int columnPosition = getAbsoluteColumnPosition(optTable);
		// Convert the absolute to the relative 0-based column position
		columnPosition = optTable.convertAbsoluteToRelativeColumnPosition(
								columnPosition);

		mb.push(columnPosition);
	}

	/**
	 * Get the absolute 0-based column position of the ColumnReference from 
	 * the conglomerate for this Optimizable.
	 *
	 * @param optTable	The Optimizable
	 *
	 * @return The absolute 0-based column position of the ColumnReference
	 */
	private int getAbsoluteColumnPosition(Optimizable optTable)
	{
		ColumnReference	cr = (ColumnReference) operand;
		int columnPosition;
		ConglomerateDescriptor bestCD;

		/* Column positions are one-based, store is zero-based */
		columnPosition = cr.getSource().getColumnPosition();

		bestCD =
			optTable.getTrulyTheBestAccessPath().getConglomerateDescriptor();

		/*
		** If it's an index, find the base column position in the index
		** and translate it to an index column position.
		*/
		if (bestCD.isIndex())
		{
			columnPosition = bestCD.getIndexDescriptor().
			  getKeyColumnPosition(columnPosition);

			if (SanityManager.DEBUG)
			{
				SanityManager.ASSERT(columnPosition > 0,
					"Base column not found in index");
			}
		}

		// return the 0-based column position
		return columnPosition - 1;
	}

	/** @see RelationalOperator#orderedNulls */
	public boolean orderedNulls()
	{
		return true;
	}

	/** @see RelationalOperator#isQualifier */
	public boolean isQualifier(Optimizable optTable, boolean forPush)
	{
		/*
		** It's a Qualifier if the operand is a ColumnReference referring
		** to a column in the given Optimizable table.
		*/
		if ( ! (operand instanceof ColumnReference))
			return false;

		ColumnReference cr = (ColumnReference) operand;
		FromTable ft = (FromTable) optTable;

		if (cr.getTableNumber() != ft.getTableNumber())
			return false;

		return true;
	}

	/** 
	 * @see RelationalOperator#getOrderableVariantType 
	 *
	 * @exception StandardException	thrown on error
	 */
	public int getOrderableVariantType(Optimizable optTable) 
		throws StandardException
	{
		return operand.getOrderableVariantType();
	}
}
