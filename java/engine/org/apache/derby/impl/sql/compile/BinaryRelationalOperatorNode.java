/*

   Derby - Class org.apache.derby.impl.sql.compile.BinaryRelationalOperatorNode

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

import org.apache.derby.iapi.reference.ClassName;
import org.apache.derby.iapi.reference.JDBC30Translation;

import org.apache.derby.iapi.util.JBitSet;

import org.apache.derby.iapi.services.loader.GeneratedMethod;

import org.apache.derby.iapi.services.compiler.MethodBuilder;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.sql.compile.C_NodeTypes;
import org.apache.derby.iapi.sql.compile.Optimizable;

import org.apache.derby.iapi.sql.dictionary.ConglomerateDescriptor;

import org.apache.derby.iapi.store.access.Qualifier;
import org.apache.derby.iapi.store.access.ScanController;

import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.TypeId;
import org.apache.derby.iapi.types.DataValueDescriptor;

import org.apache.derby.iapi.types.Orderable;

import org.apache.derby.impl.sql.compile.ExpressionClassBuilder;

import java.sql.Types;

/**
 * This class represents the 6 binary operators: LessThan, LessThanEquals,
 * Equals, NotEquals, GreaterThan and GreaterThanEquals.
 *
 * @author Manish Khettry
 */

public class BinaryRelationalOperatorNode
	extends BinaryComparisonOperatorNode
	implements RelationalOperator
{
	private int operatorType;
	/* RelationalOperator Interface */

	public void init(Object leftOperand, Object rightOperand)
	{
		String methodName = "";
		String operatorName = "";

		switch (getNodeType())
		{
			case C_NodeTypes.BINARY_EQUALS_OPERATOR_NODE:
				methodName = "equals";
				operatorName = "=";
				operatorType = RelationalOperator.EQUALS_RELOP;
				break;

			case C_NodeTypes.BINARY_GREATER_EQUALS_OPERATOR_NODE:
				methodName = "greaterOrEquals";
				operatorName = ">=";
				operatorType = RelationalOperator.GREATER_EQUALS_RELOP;
				break;

			case C_NodeTypes.BINARY_GREATER_THAN_OPERATOR_NODE:
				methodName = "greaterThan";
				operatorName = ">";
				operatorType = RelationalOperator.GREATER_THAN_RELOP;
				break;

			case C_NodeTypes.BINARY_LESS_EQUALS_OPERATOR_NODE:
				methodName = "lessOrEquals";
				operatorName = "<=";
				operatorType =  RelationalOperator.LESS_EQUALS_RELOP;
				break;

			case C_NodeTypes.BINARY_LESS_THAN_OPERATOR_NODE:
				methodName = "lessThan";
				operatorName = "<";
				operatorType = RelationalOperator.LESS_THAN_RELOP;
				break;
			case C_NodeTypes.BINARY_NOT_EQUALS_OPERATOR_NODE:
				methodName = "notEquals";
				operatorName = "<>";
				operatorType = RelationalOperator.NOT_EQUALS_RELOP;
				break;

			default:
				if (SanityManager.DEBUG)
				{
					SanityManager.THROWASSERT("init for BinaryRelationalOperator called with wrong nodeType = " + getNodeType());
				}
			    break;
		}
		super.init(leftOperand, rightOperand, operatorName, methodName);
	}

	/** @see RelationalOperator#getColumnOperand */
	public ColumnReference getColumnOperand(
								Optimizable optTable,
								int columnPosition)
	{
		FromTable	ft = (FromTable) optTable;

		return getColumnOperand(ft.getTableNumber(), columnPosition);
	}

	/** @see RelationalOperator#getColumnOperand */
	public ColumnReference getColumnOperand(
								int tableNumber,
								int columnPosition)
	{
		ColumnReference	cr;

		if (leftOperand instanceof ColumnReference)
		{
			/*
			** The left operand is a column reference.
			** Is it the correct column?
			*/
			cr = (ColumnReference) leftOperand;
			if (cr.getTableNumber() == tableNumber)
			{
				/*
				** The table is correct, how about the column position?
				*/
				if (cr.getSource().getColumnPosition() == columnPosition)
				{
					/* We've found the correct column - return it */
					return cr;
				}
			}
		}

		if (rightOperand instanceof ColumnReference)
		{
			/*
			** The right operand is a column reference.
			** Is it the correct column?
			*/
			cr = (ColumnReference) rightOperand;
			if (cr.getTableNumber() == tableNumber)
			{
				/*
				** The table is correct, how about the column position?
				*/
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

		if (leftOperand instanceof ColumnReference)
		{
			/*
			** The left operand is a column reference.
			** Is it the correct column?
			*/
			cr = (ColumnReference) leftOperand;
			if (cr.getTableNumber() == optTable.getTableNumber())
			{
				/*
				** The table is correct.
				*/
				return cr;
			}
		}

		if (rightOperand instanceof ColumnReference)
		{
			/*
			** The right operand is a column reference.
			** Is it the correct column?
			*/
			cr = (ColumnReference) rightOperand;
			if (cr.getTableNumber() == optTable.getTableNumber())
			{
				/*
				** The table is correct
				*/
				return cr;
			}
		}

		/* Neither side is the column we're looking for */
		return null;
	}

	/**
	 * @see RelationalOperator#getExpressionOperand
	 */
	public ValueNode getExpressionOperand(
								int tableNumber,
								int columnPosition)
	{
		ColumnReference	cr;

		if (leftOperand instanceof ColumnReference)
		{
			/*
			** The left operand is a column reference.
			** Is it the correct column?
			*/
			cr = (ColumnReference) leftOperand;
			if (cr.getTableNumber() == tableNumber)
			{
				/*
				** The table is correct, how about the column position?
				*/
				if (cr.getSource().getColumnPosition() == columnPosition)
				{
					/*
					** We've found the correct column -
					** return the other side
					*/
					return rightOperand;
				}
			}
		}

		if (rightOperand instanceof ColumnReference)
		{
			/*
			** The right operand is a column reference.
			** Is it the correct column?
			*/
			cr = (ColumnReference) rightOperand;
			if (cr.getTableNumber() == tableNumber)
			{
				/*
				** The table is correct, how about the column position?
				*/
				if (cr.getSource().getColumnPosition() == columnPosition)
				{
					/*
					** We've found the correct column -
					** return the other side
					*/
					return leftOperand;
				}
			}
		}

		return null;
	}

	/**
	 * @see RelationalOperator#generateExpressionOperand
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void generateExpressionOperand(
								Optimizable optTable,
								int columnPosition,
								ExpressionClassBuilder acb,
								MethodBuilder mb)
						throws StandardException
	{
		ColumnReference	cr;
		FromBaseTable	ft;

		if (SanityManager.DEBUG)
		{
    		SanityManager.ASSERT(optTable instanceof FromBaseTable);
    	}
		ft = (FromBaseTable) optTable;

		ValueNode exprOp =
					getExpressionOperand(ft.getTableNumber(), columnPosition);

		if (SanityManager.DEBUG)
		{
			if (exprOp == null)
			{
				SanityManager.THROWASSERT(
					"ColumnReference for correct column (columnPosition = " +
					columnPosition +
					", exposed table name = " + ft.getExposedName() +
				") not found on either side of BinaryRelationalOperator");
			}
		}

		exprOp.generateExpression(acb, mb);
	}

	/** @see RelationalOperator#selfComparison 
	 *
	 * @exception StandardException		Thrown on error
	 */
	public boolean selfComparison(ColumnReference cr)
		throws StandardException
	{
		ValueNode	otherSide;
		JBitSet		tablesReferenced;

		/*
		** Figure out which side the given ColumnReference is on,
		** and look for the same table on the other side.
		*/
		if (leftOperand == cr)
		{
			otherSide = rightOperand;
		}
		else if (rightOperand == cr)
		{
			otherSide = leftOperand;
		}
		else
		{
			otherSide = null;
			if (SanityManager.DEBUG)
			{
				SanityManager.THROWASSERT(
			  		"ColumnReference not found on either side of binary comparison.");
			}
		}

		tablesReferenced = otherSide.getTablesReferenced();

		/* Return true if the table we're looking for is in the bit map */
		return tablesReferenced.get(cr.getTableNumber());
	}

	/** @see RelationalOperator#usefulStartKey */
	public boolean usefulStartKey(Optimizable optTable)
	{
		/*
		** Determine whether this operator is a useful start operator
		** with knowledge of whether the key column is on the left or right.
		*/
		int	columnSide = columnOnOneSide(optTable);

		if (columnSide == NEITHER)
			return false;
		else
			return usefulStartKey(columnSide == LEFT);
	}

	/**
	 * Return true if a key column for the given table is found on the
	 * left side of this operator, false if it is found on the right
	 * side of this operator.
	 *
	 * NOTE: This method assumes that a key column will be found on one
	 * side or the other.  If you don't know whether a key column exists,
	 * use the columnOnOneSide() method (below).
	 *
	 * @param optTable	The Optimizable table that we're looking for a key
	 *					column on.
	 *
	 * @return true if a key column for the given table is on the left
	 *			side of this operator, false if one is found on the right
	 *			side of this operator.
	 */
	protected boolean keyColumnOnLeft(Optimizable optTable)
	{
		ColumnReference	cr;
		FromTable	ft;
		boolean			left = false;

		ft = (FromTable) optTable;

		/* Is the key column on the left or the right? */
		if (leftOperand instanceof ColumnReference)
		{
			/*
			** The left operand is a column reference.
			** Is it the correct column?
			*/
			cr = (ColumnReference) leftOperand;
			if (cr.getTableNumber() == ft.getTableNumber())
			{
				/* The left operand is the key column */
				left = true;
			}
		}
		else
		{
    		if (SanityManager.DEBUG)
	    	{
		    	SanityManager.ASSERT((rightOperand instanceof ColumnReference) &&
			    		(((ColumnReference) rightOperand).getTableNumber() ==
    														ft.getTableNumber()),
	    				"Key column not found on either side.");
	    	}
			/* The right operand is the key column */
			left = false;
		}

		return left;
	}

	/* Return values for columnOnOneSide */
	protected static final int LEFT = -1;
	protected static final int NEITHER = 0;
	protected static final int RIGHT = 1;

	/**
	 * Determine whether there is a column from the given table on one side
	 * of this operator, and if so, which side is it on?
	 *
	 * @param optTable	The Optimizable table that we're looking for a key
	 *					column on.
	 *
	 * @return	LEFT if there is a column on the left, RIGHT if there is
	 *			a column on the right, NEITHER if no column found on either
	 *			side.
	 */
	protected int columnOnOneSide(Optimizable optTable)
	{
		ColumnReference	cr;
		boolean			left = false;

		/* Is a column on the left */
		if (leftOperand instanceof ColumnReference)
		{
			/*
			** The left operand is a column reference.
			** Is it the correct column?
			*/
			cr = (ColumnReference) leftOperand;
			if (cr.getTableNumber() == optTable.getTableNumber())
			{
				/* Key column found on left */
				return LEFT;
			}
		}

		if (rightOperand instanceof ColumnReference)
		{
			/*
			** The right operand is a column reference.
			** Is it the correct column?
			*/
			cr = (ColumnReference) rightOperand;
			if (cr.getTableNumber() == optTable.getTableNumber())
			{
				/* Key column found on right */
				return RIGHT;
			}
		}

		return NEITHER;
	}

	/** @see RelationalOperator#usefulStopKey */
	public boolean usefulStopKey(Optimizable optTable)
	{
		/*
		** Determine whether this operator is a useful start operator
		** with knowledge of whether the key column is on the left or right.
		*/
		int	columnSide = columnOnOneSide(optTable);

		if (columnSide == NEITHER)
			return false;
		else
			return usefulStopKey(columnSide == LEFT);
	}

	/**
	 * Determine whether this comparison operator is a useful stop key
	 * with knowledge of whether the key column is on the left or right.
	 *
	 * @param left	true means the key column is on the left, false means
	 *				it is on the right.
	 *
	 * @return	true if this is a useful stop key
	 */
	/** @see RelationalOperator#generateAbsoluteColumnId */
	public void generateAbsoluteColumnId(MethodBuilder mb,
											   Optimizable optTable)
	{
		// Get the absolute column position for the column
		int columnPosition = getAbsoluteColumnPosition(optTable);

		mb.push(columnPosition);
	}

	/** @see RelationalOperator#generateRelativeColumnId */
	public void generateRelativeColumnId(MethodBuilder mb,
											   Optimizable optTable)
	{
		// Get the absolute column position for the column
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
		ColumnReference	cr;
		ConglomerateDescriptor bestCD;
		int columnPosition;

		if (keyColumnOnLeft(optTable))
		{
			cr = (ColumnReference) leftOperand;
		}
		else
		{
			cr = (ColumnReference) rightOperand;
		}

		bestCD = optTable.getTrulyTheBestAccessPath().
												getConglomerateDescriptor();

		/*
		** Column positions are one-based, store is zero-based.
		*/
		columnPosition = cr.getSource().getColumnPosition();

		/*
		** If it's an index, find the base column position in the index
		** and translate it to an index column position.
		*/
		if (bestCD != null && bestCD.isIndex())
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

	/**
	 * @exception StandardException		Thrown on error
	 */
	public void generateQualMethod(ExpressionClassBuilder acb,
								   MethodBuilder mb,
								   Optimizable optTable)
						throws StandardException
	{
		/* Generate a method that returns the expression */
		MethodBuilder qualMethod = acb.newUserExprFun();

		/*
		** Generate the expression that's on the opposite side
		** of the key column
		*/
		if (keyColumnOnLeft(optTable))
		{
			rightOperand.generateExpression(acb, qualMethod);
		}
		else
		{
			leftOperand.generateExpression(acb, qualMethod);
		}

		qualMethod.methodReturn();
		qualMethod.complete();

		/* push an expression that evaluates to the GeneratedMethod */
		acb.pushMethodReference(mb, qualMethod);
	}

	/** @see RelationalOperator#generateOrderedNulls */
	public void generateOrderedNulls(MethodBuilder mb)
	{
		mb.push(false);
	}

	/** @see RelationalOperator#orderedNulls */
	public boolean orderedNulls()
	{
		return false;
	}

	/** @see RelationalOperator#isQualifier 
	 *
	 * @exception StandardException		Thrown on error
	 */
	public boolean isQualifier(Optimizable optTable)
		throws StandardException
	{
		FromTable	ft;
		ValueNode	otherSide = null;
		JBitSet		tablesReferenced;
		ColumnReference	cr = null;
		boolean	found = false;

		ft = (FromTable) optTable;

		if (leftOperand instanceof ColumnReference)
		{
			/*
			** The left operand is a column reference.
			** Is it the correct column?
			*/
			cr = (ColumnReference) leftOperand;
			if (cr.getTableNumber() == ft.getTableNumber())
			{
				otherSide = rightOperand;
				found = true;
			}
		}

		if ( ( ! found) && (rightOperand instanceof ColumnReference) )
		{
			/*
			** The right operand is a column reference.
			** Is it the correct column?
			*/
			cr = (ColumnReference) rightOperand;
			if (cr.getTableNumber() == ft.getTableNumber())
			{
				otherSide = leftOperand;
				found = true;
			}
		}

		/* Have we found a ColumnReference on either side? */
		if ( ! found)
		{
			/*
			** Neither side is a ColumnReference to the table we're looking
			** for, so it can't be a Qualifier
			*/
			return false;
		}

		/*
		** One side is a ColumnReference to the correct table.  It is a
		** Qualifier if the other side does not refer to the table we are
		** optimizing.
		*/
		tablesReferenced = otherSide.getTablesReferenced();

		return  ! (tablesReferenced.get(ft.getTableNumber()));
	}

	/** 
	 * @see RelationalOperator#getOrderableVariantType 
	 *
	 * @exception StandardException	thrown on error
	 */
	public int getOrderableVariantType(Optimizable optTable) 
		throws StandardException
	{
		/* The Qualifier's orderable is on the opposite side from
		 * the key column.
		 */
		if (keyColumnOnLeft(optTable))
		{
			return rightOperand.getOrderableVariantType();
		}
		else
		{
			return leftOperand.getOrderableVariantType();
		}
	}

	/** @see RelationalOperator#compareWithKnownConstant */
	public boolean compareWithKnownConstant(Optimizable optTable, boolean considerParameters)
	{
		ValueNode	node = null;
		node = keyColumnOnLeft(optTable) ? rightOperand : leftOperand;

		if (considerParameters)
		{
			return (node instanceof ConstantNode) ||
						((node.isParameterNode()) &&
						 (((ParameterNode)node).getDefaultValue() != null));
		}
		else
		{
			return node instanceof ConstantNode;
		}
	}

	/**
	 * @see RelationalOperator#getCompareValue
	 *
	 * @exception StandardException		Thrown on error
	 */
	public DataValueDescriptor getCompareValue(Optimizable optTable) 
        throws StandardException
	{
		ValueNode	node = null;

		/* The value being compared to is on the opposite side from
		** the key column.
		*/
		node = keyColumnOnLeft(optTable) ? rightOperand : leftOperand;

		if (node instanceof ConstantNode) 
		{
			return ((ConstantNode)node).getValue();
		}
		else if (node.isParameterNode())
		{
			return ((ParameterNode)node).getDefaultValue();
		}
		else
		{	
			return null;
		}
	}


	/**
	 * Return 50% if this is a comparison with a boolean column, a negative
	 * selectivity otherwise.
	 */
	protected double booleanSelectivity(Optimizable optTable)
	{
		TypeId	typeId = null;
		double				retval = -1.0d;
		int					columnSide;

		columnSide = columnOnOneSide(optTable);

		if (columnSide == LEFT)
			typeId = leftOperand.getTypeId();
		else if (columnSide == RIGHT)
			typeId = rightOperand.getTypeId();

		if (typeId != null && (typeId.getJDBCTypeId() == Types.BIT ||
		typeId.getJDBCTypeId() == JDBC30Translation.SQL_TYPES_BOOLEAN))
			retval = 0.5d;

		return retval;
	}

	/**
	 * The methods generated for this node all are on Orderable.  
	 * Overrides this method
	 * in BooleanOperatorNode for code generation purposes.
	 */
	public String getReceiverInterfaceName() {
	    return ClassName.DataValueDescriptor;
	}
	
	/**
	 * Returns the negation of this operator; negation of Equals is NotEquals.
	 */
	BinaryOperatorNode getNegation(ValueNode leftOperand, 
										  ValueNode rightOperand)
		throws StandardException
	{
		BinaryOperatorNode negation;
		if (SanityManager.DEBUG)
			SanityManager.ASSERT(dataTypeServices != null,
								 "dataTypeServices is expected to be non-null");
		/* xxxRESOLVE: look into doing this in place instead of allocating a new node */
		negation = (BinaryOperatorNode)
			getNodeFactory().getNode(getNegationNode(),
									 leftOperand, rightOperand,
									 getContextManager());
		negation.setType(dataTypeServices);
		return negation;
	}

	/* map current node to its negation */
	private int getNegationNode()
	{
		switch (getNodeType())
		{
			case C_NodeTypes.BINARY_EQUALS_OPERATOR_NODE:
				return C_NodeTypes.BINARY_NOT_EQUALS_OPERATOR_NODE;

			case C_NodeTypes.BINARY_GREATER_EQUALS_OPERATOR_NODE:
				return C_NodeTypes.BINARY_LESS_THAN_OPERATOR_NODE;

			case C_NodeTypes.BINARY_GREATER_THAN_OPERATOR_NODE:
				return C_NodeTypes.BINARY_LESS_EQUALS_OPERATOR_NODE;

			case C_NodeTypes.BINARY_LESS_THAN_OPERATOR_NODE:
				return C_NodeTypes.BINARY_GREATER_EQUALS_OPERATOR_NODE;
				
			case C_NodeTypes.BINARY_LESS_EQUALS_OPERATOR_NODE:
				return C_NodeTypes.BINARY_GREATER_THAN_OPERATOR_NODE;

			case C_NodeTypes.BINARY_NOT_EQUALS_OPERATOR_NODE:				
				return C_NodeTypes.BINARY_EQUALS_OPERATOR_NODE;
		}
		
		if (SanityManager.DEBUG)
		{
			SanityManager.THROWASSERT("getNegationNode called with invalid nodeType: " + getNodeType());
		}

		return -1;
	}	
	
	/**
	 * is this is useful start key? for example a predicate of the from
	 * <em>column Lessthan 5</em> is not a useful start key but is a useful stop
	 * key. However <em>5 Lessthan column </em> is a useful start key.
	 *
	 * @param columnOnLeft 	is true if the column is the left hand side of the
	 * binary operator.
	 */
	protected boolean usefulStartKey(boolean columnOnLeft)
	{
		switch (operatorType)
		{
			case RelationalOperator.EQUALS_RELOP:
				return true;
			case RelationalOperator.NOT_EQUALS_RELOP:
				return false;
			case RelationalOperator.GREATER_THAN_RELOP:
			case RelationalOperator.GREATER_EQUALS_RELOP:
				// col > 1
				return columnOnLeft;
			case RelationalOperator.LESS_THAN_RELOP:
			case RelationalOperator.LESS_EQUALS_RELOP:
				// col < 1
				return !columnOnLeft;
			default:
				return false;
		}


	}

	/** @see RelationalOperator#usefulStopKey */
	protected boolean usefulStopKey(boolean columnOnLeft)
	{
		switch (operatorType)
		{
			case RelationalOperator.EQUALS_RELOP:
				return true;
			case RelationalOperator.NOT_EQUALS_RELOP:
				return false;
			case RelationalOperator.GREATER_THAN_RELOP:
			case RelationalOperator.GREATER_EQUALS_RELOP:
				// col > 1
				return !columnOnLeft;
			case RelationalOperator.LESS_EQUALS_RELOP:
			case RelationalOperator.LESS_THAN_RELOP:
				// col < 1
				return columnOnLeft;
			default:
				return false;
		}
	}
	
	/** @see RelationalOperator#getStartOperator */
	public int getStartOperator(Optimizable optTable)
	{
		switch (operatorType)
		{
			case RelationalOperator.EQUALS_RELOP:
			case RelationalOperator.LESS_EQUALS_RELOP:
			case RelationalOperator.GREATER_EQUALS_RELOP:
				return ScanController.GE;
			case RelationalOperator.LESS_THAN_RELOP:
			case RelationalOperator.GREATER_THAN_RELOP:
				return ScanController.GT;
			case RelationalOperator.NOT_EQUALS_RELOP:				
				if (SanityManager.DEBUG)
					SanityManager.THROWASSERT("!= cannot be a start operator");
				return ScanController.NA;
			default:
				return ScanController.NA;

		}
	}
	
	/** @see RelationalOperator#getStopOperator */
	public int getStopOperator(Optimizable optTable)
	{
		switch (operatorType)
		{
			case RelationalOperator.EQUALS_RELOP:
			case RelationalOperator.GREATER_EQUALS_RELOP:
			case RelationalOperator.LESS_EQUALS_RELOP:
				return ScanController.GT;			
			case RelationalOperator.LESS_THAN_RELOP:
			case RelationalOperator.GREATER_THAN_RELOP:
				return ScanController.GE;			
			case RelationalOperator.NOT_EQUALS_RELOP:				
				if (SanityManager.DEBUG)
					SanityManager.THROWASSERT("!= cannot be a stop operator");
				return ScanController.NA;				
			default:
				return ScanController.NA;				
		}
	}

	/** @see RelationalOperator#generateOperator */
	public void generateOperator(MethodBuilder mb,
								 Optimizable optTable)
	{
		switch (operatorType)
		{
			case RelationalOperator.EQUALS_RELOP:
				mb.push(Orderable.ORDER_OP_EQUALS);
				break;

			case RelationalOperator.NOT_EQUALS_RELOP:
				mb.push(Orderable.ORDER_OP_EQUALS);
				break;

			case RelationalOperator.LESS_THAN_RELOP:
			case RelationalOperator.GREATER_EQUALS_RELOP:
				mb.push(keyColumnOnLeft(optTable) ? 
						Orderable.ORDER_OP_LESSTHAN : Orderable.ORDER_OP_LESSOREQUALS);
				break;
			case RelationalOperator.LESS_EQUALS_RELOP:
			case RelationalOperator.GREATER_THAN_RELOP:
				mb.push(keyColumnOnLeft(optTable) ? 
						Orderable.ORDER_OP_LESSOREQUALS : Orderable.ORDER_OP_LESSTHAN);
				
		}											
	}
	
	/** @see RelationalOperator#generateNegate */
	public void generateNegate(MethodBuilder mb, Optimizable optTable)
	{
		switch (operatorType)
		{
			case RelationalOperator.EQUALS_RELOP:
				mb.push(false);
				break;
			case RelationalOperator.NOT_EQUALS_RELOP:
				mb.push(true);
				break;
			case RelationalOperator.LESS_THAN_RELOP:
			case RelationalOperator.LESS_EQUALS_RELOP:
				mb.push(!keyColumnOnLeft(optTable));
				break;
			case RelationalOperator.GREATER_THAN_RELOP:
			case RelationalOperator.GREATER_EQUALS_RELOP:
				mb.push(keyColumnOnLeft(optTable));
				break;
		}
		
		return;
	}
		
	/** @see RelationalOperator#getOperator */
	public int getOperator()
	{
		return operatorType;
	}

	/** return the selectivity of this predicate.
	 */
	public double selectivity(Optimizable optTable)
	{
		double retval = booleanSelectivity(optTable);
		
		if (retval >= 0.0d)
			return retval;
			
		switch (operatorType)
		{
			case RelationalOperator.EQUALS_RELOP:
				return 0.1;
			case RelationalOperator.NOT_EQUALS_RELOP:
			case RelationalOperator.LESS_THAN_RELOP:
			case RelationalOperator.LESS_EQUALS_RELOP:
			case RelationalOperator.GREATER_EQUALS_RELOP:
				if (getBetweenSelectivity())
					return 0.5d;
				/* fallthrough -- only */
			case RelationalOperator.GREATER_THAN_RELOP:
				return 0.33;
		}
		
		return 0.0;
	}

	/** @see RelationalOperator#getTransitiveSearchClause */
	public RelationalOperator getTransitiveSearchClause(ColumnReference otherCR)
		throws StandardException
	{
		return (RelationalOperator)getNodeFactory().getNode(getNodeType(),
														  otherCR,
														  rightOperand,
														  getContextManager());
	}
	
	public boolean equalsComparisonWithConstantExpression(Optimizable optTable)
	{
		if (operatorType != EQUALS_RELOP)
			return false;

		boolean retval = false;
		ValueNode comparand = null;

		int side = columnOnOneSide(optTable);
		if (side == LEFT)
		{
			retval = rightOperand.isConstantExpression();
		}
		else if (side == RIGHT)
		{
			retval = leftOperand.isConstantExpression();
		}

		return retval;
	}
	
	/** @see ValueNode#isRelationalOperator */
	public boolean isRelationalOperator()
	{
		return true;
	}
	
	public boolean isBinaryEqualsOperatorNode()
	{
		return (operatorType == RelationalOperator.EQUALS_RELOP);
	}

	/** @see ValueNode#optimizableEqualityNode */
	public boolean optimizableEqualityNode(Optimizable optTable, 
										   int columnNumber, 
										   boolean isNullOkay)
		throws StandardException
	{
		if (operatorType != EQUALS_RELOP)
			return false;

		ColumnReference cr = getColumnOperand(optTable,
											  columnNumber);
		if (cr == null)
			return false;

		if (selfComparison(cr))
			return false;
		
		if (implicitVarcharComparison())
			return false;
		
		return true;
	}
	
	/**
	 * Return whether or not this binary relational predicate requires an implicit
	 * (var)char conversion.  This is important when considering
	 * hash join since this type of equality predicate is not currently
	 * supported for a hash join.
	 *
	 * @return	Whether or not an implicit (var)char conversion is required for
	 *			this binary relational operator.
	 *
	 * @exception StandardException		Thrown on error
	 */

	private boolean implicitVarcharComparison()
		throws StandardException
	{
		TypeId leftType = leftOperand.getTypeId();
		TypeId rightType = rightOperand.getTypeId();
		
		if (leftType.isStringTypeId() && !rightType.isStringTypeId())
			return true;

		if (rightType.isStringTypeId() && (!leftType.isStringTypeId()))
			return true;

		return false;
	}
	
	/* @see BinaryOperatorNode#genSQLJavaSQLTree
	 * @see BinaryComparisonOperatorNode#genSQLJavaSQLTree
	 */
	public ValueNode genSQLJavaSQLTree() throws StandardException
	{
		if (operatorType == EQUALS_RELOP)
			return this;
		
		return super.genSQLJavaSQLTree();
	}
}	


