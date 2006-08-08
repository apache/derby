/*

   Derby - Class org.apache.derby.impl.sql.compile.BinaryRelationalOperatorNode

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

	// Visitor for finding base tables beneath optimizables and column
	// references.  Created once and re-used thereafter.
	private BaseTableNumbersVisitor btnVis;

	// Bit sets for holding base tables beneath optimizables and
	// column references.  Created once and re-used thereafter.
	JBitSet optBaseTables;
	JBitSet valNodeBaseTables;

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
		btnVis = null;
	}

	/** @see RelationalOperator#getColumnOperand */
	public ColumnReference getColumnOperand(
								Optimizable optTable,
								int columnPosition)
	{
		FromTable	ft = (FromTable) optTable;

		// When searching for a matching column operand, we search
		// the entire subtree (if there is one) beneath optTable
		// to see if we can find any FromTables that correspond to
		// either of this op's column references.

		ColumnReference	cr;
		boolean walkSubtree = true;
		if (leftOperand instanceof ColumnReference)
		{
			/*
			** The left operand is a column reference.
			** Is it the correct column?
			*/
			cr = (ColumnReference) leftOperand;
			if (valNodeReferencesOptTable(cr, ft, false, walkSubtree))
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
			walkSubtree = false;
		}

		if (rightOperand instanceof ColumnReference)
		{
			/*
			** The right operand is a column reference.
			** Is it the correct column?
			*/
			cr = (ColumnReference) rightOperand;
			if (valNodeReferencesOptTable(cr, ft, false, walkSubtree))
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

		boolean walkSubtree = true;
		if (leftOperand instanceof ColumnReference)
		{
			/*
			** The left operand is a column reference.
			** Is it the correct column?
			*/
			cr = (ColumnReference) leftOperand;
			if (valNodeReferencesOptTable(
				cr, (FromTable)optTable, false, walkSubtree))
			{
				/*
				** The table is correct.
				*/
				return cr;
			}
			walkSubtree = false;
		}

		if (rightOperand instanceof ColumnReference)
		{
			/*
			** The right operand is a column reference.
			** Is it the correct column?
			*/
			cr = (ColumnReference) rightOperand;
			if (valNodeReferencesOptTable(cr,
				(FromTable)optTable, false, walkSubtree))
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
								int columnPosition,
								FromTable ft)
	{
		ColumnReference	cr;
		boolean walkSubtree = true;

		if (leftOperand instanceof ColumnReference)
		{
			/*
			** The left operand is a column reference.
			** Is it the correct column?
			*/
			cr = (ColumnReference) leftOperand;
			if (valNodeReferencesOptTable(cr, ft, false, walkSubtree))
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
			walkSubtree = false;
		}

		if (rightOperand instanceof ColumnReference)
		{
			/*
			** The right operand is a column reference.
			** Is it the correct column?
			*/
			cr = (ColumnReference) rightOperand;
			if (valNodeReferencesOptTable(cr, ft, false, walkSubtree))
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
	 * @see RelationalOperator#getOperand
	 */
	public ValueNode getOperand(ColumnReference cRef,
		int refSetSize, boolean otherSide)
	{
		// Following call will initialize/reset the btnVis,
		// valNodeBaseTables, and optBaseTables fields of this object.
		initBaseTableVisitor(refSetSize, true);

		// We search for the column reference by getting the *base*
		// table number for each operand and checking to see if
		// that matches the *base* table number for the cRef
		// that we're looking for.  If so, then we the two
		// reference the same table so we go on to check
		// column position.
		try {

			// Use optBaseTables for cRef's base table numbers.
			btnVis.setTableMap(optBaseTables);
			cRef.accept(btnVis);

			// Use valNodeBaseTables for operand base table nums.
			btnVis.setTableMap(valNodeBaseTables);

			ColumnReference	cr;
			if (leftOperand instanceof ColumnReference)
			{
				/*
				** The left operand is a column reference.
				** Is it the correct column?
				*/
				cr = (ColumnReference) leftOperand;
				cr.accept(btnVis);
				valNodeBaseTables.and(optBaseTables);
				if (valNodeBaseTables.getFirstSetBit() != -1)
				{
					/*
					** The table is correct, how about the column position?
					*/
					if (cr.getSource().getColumnPosition() ==
						cRef.getColumnNumber())
					{
						/*
						** We've found the correct column -
						** return the appropriate side.
						*/
						if (otherSide)
							return rightOperand;
						return leftOperand;
					}
				}
			}

			if (rightOperand instanceof ColumnReference)
			{
				/*
				** The right operand is a column reference.
				** Is it the correct column?
				*/
				valNodeBaseTables.clearAll();
				cr = (ColumnReference) rightOperand;
				cr.accept(btnVis);
				valNodeBaseTables.and(optBaseTables);
				if (valNodeBaseTables.getFirstSetBit() != -1)
				{
					/*
					** The table is correct, how about the column position?
					*/
					if (cr.getSource().getColumnPosition() == 
						cRef.getColumnNumber())
					{
						/*
						** We've found the correct column -
						** return the appropriate side
						*/
						if (otherSide)
							return leftOperand;
						return rightOperand;
					}
				}
			}

		} catch (StandardException se) {
            if (SanityManager.DEBUG)
            {
                SanityManager.THROWASSERT("Failed when trying to " +
                    "find base table number for column reference check:\n" +
                    se.getMessage());
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

		ValueNode exprOp = getExpressionOperand(
			ft.getTableNumber(), columnPosition, ft);

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
		boolean			left = false;

		/* Is the key column on the left or the right? */
		if (leftOperand instanceof ColumnReference)
		{
			/*
			** The left operand is a column reference.
			** Is it the correct column?
			*/
			cr = (ColumnReference) leftOperand;
			if (valNodeReferencesOptTable(
				cr, (FromTable)optTable, false, true))
			{
				/* The left operand is the key column */
				left = true;
			}
		}

		// Else the right operand must be the key column.
    	if (SanityManager.DEBUG)
		{
			if (!left)
			{
		    	SanityManager.ASSERT(
					(rightOperand instanceof ColumnReference) &&
					valNodeReferencesOptTable((ColumnReference)rightOperand,
			    		(FromTable)optTable, false, true),
					"Key column not found on either side.");
			}
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
		boolean			walkSubtree = true;

		/* Is a column on the left */
		if (leftOperand instanceof ColumnReference)
		{
			/*
			** The left operand is a column reference.
			** Is it the correct column?
			*/
			cr = (ColumnReference) leftOperand;
			if (valNodeReferencesOptTable(
				cr, (FromTable)optTable, false, walkSubtree))
			{
				/* Key column found on left */
				return LEFT;
			}
			walkSubtree = false;
		}

		if (rightOperand instanceof ColumnReference)
		{
			/*
			** The right operand is a column reference.
			** Is it the correct column?
			*/
			cr = (ColumnReference) rightOperand;
			if (valNodeReferencesOptTable(
				cr, (FromTable)optTable, false, walkSubtree))
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
	public boolean isQualifier(Optimizable optTable, boolean forPush)
		throws StandardException
	{
		FromTable	ft;
		ValueNode	otherSide = null;
		JBitSet		tablesReferenced;
		ColumnReference	cr = null;
		boolean	found = false;
		boolean walkSubtree = true;

		ft = (FromTable) optTable;

		if (leftOperand instanceof ColumnReference)
		{
			/*
			** The left operand is a column reference.
			** Is it the correct column?
			*/
			cr = (ColumnReference) leftOperand;
			if (valNodeReferencesOptTable(cr, ft, forPush, walkSubtree))
			{
				otherSide = rightOperand;
				found = true;
			}
			walkSubtree = false;
		}

		if ( ( ! found) && (rightOperand instanceof ColumnReference) )
		{
			/*
			** The right operand is a column reference.
			** Is it the correct column?
			*/
			cr = (ColumnReference) rightOperand;
			if (valNodeReferencesOptTable(cr, ft, forPush, walkSubtree))
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
		return !valNodeReferencesOptTable(otherSide, ft, forPush, true);
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
						((node.requiresTypeFromContext()) &&
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
		else if (node.requiresTypeFromContext())
		{
			ParameterNode pn;
			if (node instanceof UnaryOperatorNode) 
	  			pn = ((UnaryOperatorNode)node).getParameterOperand();
			else
	  			pn = (ParameterNode) (node);
			return pn.getDefaultValue();
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
	throws StandardException
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
	throws StandardException
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

	/**
	 * Take a ResultSetNode and return a column reference that is scoped for
	 * for the received ResultSetNode, where "scoped" means that the column
	 * reference points to a specific column in the RSN.  This is used for
	 * remapping predicates from an outer query down to a subquery. 
	 *
	 * For example, assume we have the following query:
	 *
	 *  select * from
	 *    (select i,j from t1 union select i,j from t2) X1,
	 *    (select a,b from t3 union select a,b from t4) X2
	 *  where X1.j = X2.b;
	 *
	 * Then assume that this BinaryRelationalOperatorNode represents the
	 * "X1.j = X2.b" predicate and that the childRSN we received as a
	 * parameter represents one of the subqueries to which we want to push
	 * the predicate; let's say it's:
	 *
	 *    select i,j from t1
	 *
	 * Then what we want to do in this method is map one of the operands
	 * X1.j or X2.b (depending on the 'whichSide' parameter) to the childRSN,
	 * if possible.  Note that in our example, "X2.b" should _NOT_ be mapped
	 * because it doesn't apply to the childRSN for the subquery "select i,j
	 * from t1"; thus we should leave it as it is.  "X1.j", however, _does_
	 * need to be scoped, and so this method will return a ColumnReference
	 * pointing to "T1.j" (or whatever the corresponding column in T1 is).
	 *
	 * ASSUMPTION: We should only get to this method if we know that
	 * at least one operand in the predicate to which this operator belongs
	 * can and should be mapped to the received childRSN. 
	 *
     * @param whichSide The operand are we trying to scope (LEFT or RIGHT)
     * @param parentRSNsTables Set of all table numbers referenced by
     *  the ResultSetNode that is _parent_ to the received childRSN.
     *  We need this to make sure we don't scope the operand to a
     *  ResultSetNode to which it doesn't apply.
     * @param childRSN The result set node to which we want to create
     *  a scoped predicate.
     * @return A column reference scoped to the received childRSN, if possible.
     *  If the operand is a ColumnReference that is not supposed to be scoped,
	 *  we return a _clone_ of the reference--this is necessary because the
	 *  reference is going to be pushed to two places (left and right children
	 *  of the parentRSN) and if both children are referencing the same
	 *  instance of the column reference, they'll interfere with each other
	 *  during optimization.
	 */
	public ValueNode getScopedOperand(int whichSide,
		JBitSet parentRSNsTables, ResultSetNode childRSN)
		throws StandardException
	{
		ResultColumn rc = null;
		ColumnReference cr = 
			whichSide == LEFT
				? (ColumnReference)leftOperand
				: (ColumnReference)rightOperand;

		// The first thing we need to do is see if this ColumnReference
		// is supposed to be scoped for childRSN.  We do that by figuring
		// out what underlying base table the column reference is pointing
		// to and then seeing if that base table is included in the list of
		// table numbers from the parentRSN.
		JBitSet crTables = new JBitSet(parentRSNsTables.size());
		BaseTableNumbersVisitor btnVis =
			new BaseTableNumbersVisitor(crTables);
		cr.accept(btnVis);

		// If the column reference doesn't reference any tables,
		// then there's no point in mapping it to the child result
		// set; just return a clone of the operand.
		if (crTables.getFirstSetBit() == -1)
		{
			return (ValueNode)cr.getClone();
		}

		/* If the column reference in question is not intended for
		 * the received result set node, just leave the operand as
		 * it is (i.e. return a clone).  In the example mentioned at
		 * the start of this method, this will happen when the operand
		 * is X2.b and childRSN is either "select i,j from t1" or
		 * "select i,j from t2", in which case the operand does not
		 * apply to childRSN.  When we get here and try to map the
		 * "X1.j" operand, though, the following "contains" check will
		 * return true and thus we can go ahead and return a scoped
		 * version of that operand.
		 */
		if (!parentRSNsTables.contains(crTables))
		{
			return (ValueNode)cr.getClone();
		}

		// If the column reference is already pointing to the
		// correct table, then there's no need to change it.
		if ((childRSN.getReferencedTableMap() != null) &&
			childRSN.getReferencedTableMap().get(cr.getTableNumber()))
		{
			return cr;
		}

		/* Find the target ResultColumn in the received result set.  At
		 * this point we know that we do in fact need to scope the column
		 * reference for childRSN, so go ahead and do it.  We get the
		 * target column by column position instead of by name because
		 * it's possible that the name given for the query doesn't match
		 * the name of the actual column we're looking for.  Ex.
		 *
		 *  select * from
		 *    (select i,j from t1 union select i,j from t2) X1 (x,y),
		 *    (select a,b from t3 union select a,b from t4) X2
		 *  where X1.x = X2.b;
		 *
		 * If we searched for "x" in the childRSN "select i,j from t1"
		 * we wouldn't find it.  So we have to look based on position.
		 */

		rc = childRSN.getResultColumns().getResultColumn(cr.getColumnNumber());

		// rc shouldn't be null; if there was no matching ResultColumn at all,
		// then we shouldn't have made it this far.
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(rc != null,
				"Failed to locate result column when trying to " +
				"scope operand '" + cr.getTableName() + "." +
				cr.getColumnName() + "'.");
		}

		/* If the ResultColumn we found has an expression that is a
		 * ColumnReference, then that column reference has all of the info
		 * we need, with one exception: the columnNumber.  Depending on our
		 * depth in the tree, the ResultColumn's ColumnReference could be
		 * pointing to a base column in the FromBaseTable.  In that case the
		 * ColumnReference will hold the column position as it is with respect
		 * to the FromBaseTable.  But when we're scoping a column reference,
		 * we're scoping it to a ResultSetNode that sits (either directly or
		 * indirectly) above a ProjectRestrictNode that in turn sits above the
		 * FromBaseTable. This means that the scoped reference's columnNumber
		 * needs to be with respect to the PRN that sits above the base table,
		 * _not_ with respect to the FromBaseTable itself.  This is important
		 * because column "1" in the PRN might not be column "1" in the
		 * underlying base table. For example, if we have base table TT with
		 * four columns (a int, b int, i int, j int) and the PRN above it only
		 * projects out columns (i,j), then column "1" for the PRN is "i", but
		 * column "1" for base table TT is "a".  On the flip side, column "3"
		 * for base table TT is "i", but if we search the PRN's result columns
		 * (which match the result columns for the ResultSetNode to which
		 * we're scoping) for column "3", we won't find it.
		 *
		 * So what does all of that mean?  It means that if the ResultColumn
		 * we found has an expression that's a ColumnReference, we can simply
		 * return that ColumnReference IF we set it's columnNumber correctly.
		 * Thankfully the column reference we're trying to scope ("cr") came
		 * from further up the tree and so it knows what the correct column
		 * position (namely, the position w.r.t the ProjectRestrictNode above
		 * the FromBaseTable) needs to be.  So that's the column number we
		 * use.
		 *
		 * As a final note, we have to be sure we only set the column
		 * reference's column number if the reference points to a base table.
		 * If the reference points to some other ResultSetNode--esp. another
		 * subquery node--then it (the reference) already holds the correct
		 * number with respect to that ResultSetNode and we don't change
		 * it.  The reason is that the reference could end up getting pushed
		 * down further to that ResultSetNode, in which case we'll do another
		 * scoping operation and, in order for that to be successful, the
		 * reference to be scoped has to know what the target column number
		 * is w.r.t to that ResultSetNode (i.e. it'll be playing the role of
		 * "cr" as described here).
		 */
		if (rc.getExpression() instanceof ColumnReference)
		{
			// Make sure the ColumnReference's columnNumber is correct,
			// then just return that reference.  Note: it's okay to overwrite
			// the columnNumber directly because when it eventually makes
			// it down to the PRN over the FromBaseTable, it will be remapped
			// for the FromBaseTable and the columnNumber will then be set
			// correctly.  That remapping is done in the pushOptPredicate()
			// method of ProjectRestrictNode.
			ColumnReference cRef = (ColumnReference)rc.getExpression();
			if (cRef.pointsToBaseTable())
				cRef.setColumnNumber(cr.getColumnNumber());
			return cRef;
		}

		/* We can get here if the ResultColumn's expression isn't a
		 * ColumnReference.  For example, the expression would be a
		 * constant expression if childRSN represented something like:
		 *
		 *   select 1, 1 from t1
		 *
		 * In this case we just return a clone of the column reference
		 * because it's scoped as far as we can take it.
		 */
		return (ValueNode)cr.getClone();
	}

	/**
	 * Determine whether or not the received ValueNode (which will
	 * usually be a ColumnReference) references either the received
	 * optTable or else a base table in the subtree beneath that
	 * optTable.
	 *
	 * @param valNode The ValueNode that has the reference(s).
	 * @param optTable The table/subtree node to which we're trying
	 *  to find a reference.
	 * @param forPush Whether or not we are searching with the intent
	 *  to push this operator to the target table.
	 * @param walkOptTableSubtree Should we walk the subtree beneath
	 *  optTable to find base tables, or not?  Will be false if we've
	 *  already done it for the left operand and now we're here
	 *  for the right operand.
	 * @return True if valNode contains a reference to optTable or
	 *  to a base table in the subtree beneath optTable; false
	 *  otherwise.
	 */
	private boolean valNodeReferencesOptTable(ValueNode valNode,
		FromTable optTable, boolean forPush, boolean walkOptTableSubtree)
	{
		// Following call will initialize/reset the btnVis,
		// valNodeBaseTables, and optBaseTables fields of this object.
		initBaseTableVisitor(optTable.getReferencedTableMap().size(),
			walkOptTableSubtree);

		boolean found = false;
		try {

			// Find all base tables beneath optTable and load them
			// into this object's optBaseTables map.  This is the
			// list of table numbers we'll search to see if the
			// value node references any tables in the subtree at
			// or beneath optTable.
			if (walkOptTableSubtree)
				buildTableNumList(optTable, forPush);

			// Now get the base table numbers that are in valNode's
			// subtree.  In most cases valNode will be a ColumnReference
			// and this will return a single base table number.
			btnVis.setTableMap(valNodeBaseTables);
			valNode.accept(btnVis);

			// And finally, see if there's anything in common.
			valNodeBaseTables.and(optBaseTables);
			found = (valNodeBaseTables.getFirstSetBit() != -1);

		} catch (StandardException se) {
			if (SanityManager.DEBUG)
			{
				SanityManager.THROWASSERT("Failed when trying to " +
					"find base table numbers for reference check:\n" +
					se.getMessage());
			}
		}

		return found;
	}

	/**
	 * Initialize the fields used for retrieving base tables in
	 * subtrees, which allows us to do a more extensive search
	 * for table references.  If the fields have already been
	 * created, then just reset their values.
	 *
	 * @param numTablesInQuery Used for creating JBitSets that
	 *  can hold table numbers for the query.
	 * @param initOptBaseTables Whether or not we should clear out
	 *  or initialize the optBaseTables bit set.
	 */
	private void initBaseTableVisitor(int numTablesInQuery,
		boolean initOptBaseTables)
	{
		if (valNodeBaseTables == null)
			valNodeBaseTables = new JBitSet(numTablesInQuery);
		else
			valNodeBaseTables.clearAll();

		if (initOptBaseTables)
		{
			if (optBaseTables == null)
				optBaseTables = new JBitSet(numTablesInQuery);
			else
				optBaseTables.clearAll();
		}

		// Now create the visitor.  We give it valNodeBaseTables
		// here for sake of creation, but this can be overridden
		// (namely, by optBaseTables) by the caller of this method.
		if (btnVis == null)
			btnVis = new BaseTableNumbersVisitor(valNodeBaseTables);
	}

	/**
	 * Create a set of table numbers to search when trying to find
	 * which (if either) of this operator's operands reference the
	 * received target table.  At the minimum this set should contain
	 * the target table's own table number.  After that, if we're
	 * _not_ attempting to push this operator (or more specifically,
	 * the predicate to which this operator belongs) to the target
	 * table, we go on to search the subtree beneath the target
	 * table and add any base table numbers to the searchable list.
	 *
	 * @param ft Target table for which we're building the search
	 *  list.
	 * @param forPush Whether or not we are searching with the intent
	 *  to push this operator to the target table.
	 */
	private void buildTableNumList(FromTable ft, boolean forPush)
		throws StandardException
	{
		// Start with the target table's own table number.  Note
		// that if ft is an instanceof SingleChildResultSet, its
		// table number could be negative.
		if (ft.getTableNumber() >= 0)
			optBaseTables.set(ft.getTableNumber());

		if (forPush)
		// nothing else to do.
			return;

		// Add any table numbers from the target table's
		// reference map.
		optBaseTables.or(ft.getReferencedTableMap());

		// The table's reference map is not guaranteed to have
		// all of the tables that are actually used--for example,
		// if the table is a ProjectRestrictNode or a JoinNode
		// with a subquery as a child, the ref map will contain
		// the number for the PRN above the subquery, but it
		// won't contain the table numbers referenced by the
		// subquery.  So here we go through and find ALL base
		// table numbers beneath the target node.
		btnVis.setTableMap(optBaseTables);
		ft.accept(btnVis);
		return;
	}

}
