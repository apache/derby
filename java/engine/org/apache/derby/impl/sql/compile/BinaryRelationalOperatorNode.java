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

	/* If this BinRelOp was created for an IN-list "probe predicate"
	 * then we keep a pointer to the original IN-list.  This serves
	 * two purposes: 1) if this field is non-null then we know that
	 * this BinRelOp is for an IN-list probe predicate; 2) if the
	 * optimizer chooses a plan for which the probe predicate is
	 * not usable as a start/stop key then we'll "revert" the pred
	 * back to the InListOperatorNode referenced here.  NOTE: Once
	 * set, this variable should *only* ever be accessed via the
	 * isInListProbeNode() or getInListOp() methods--see comments
	 * in the latter method for more.
	 */
	private InListOperatorNode inListProbeSource = null;

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

	/**
	 * Same as init() above except takes a third argument that is
	 * an InListOperatorNode.  This version is used during IN-list
	 * preprocessing to create a "probe predicate" for the IN-list.
	 * See InListOperatorNode.preprocess() for more.
	 */
	public void init(Object leftOperand, Object rightOperand, Object inListOp)
	{
		init(leftOperand, rightOperand);
		this.inListProbeSource = (InListOperatorNode)inListOp;
	}

	/**
	 * If this rel op was created for an IN-list probe predicate then return
	 * the underlying InListOperatorNode.  Will return null if this rel
	 * op is a "legitimate" relational operator (as opposed to a disguised
	 * IN-list).  With the exception of nullability checking via the
	 * isInListProbeNode() method, all access to this.inListProbeSource
	 * MUST come through this method, as this method ensures that the
	 * left operand of the inListProbeSource is set correctly before
	 * returning it.
	 */
	protected InListOperatorNode getInListOp()
	{
		if (inListProbeSource != null)
		{
			/* Depending on where this probe predicate currently sits
			 * in the query tree, this.leftOperand *may* have been
			 * transformed, replaced, or remapped one or more times
			 * since inListProbeSource was last referenced. Since the
			 * leftOperand of the IN list should be the same regardless
			 * of which "version" of the operation we're looking at
			 * (i.e. the "probe predicate" version (this node) vs the
			 * original version (inListProbeSource)), we have to make
			 * sure that all of the changes made to this.leftOperand
			 * are reflected in inListProbeSource's leftOperand, as
			 * well.  In doing so we ensure the caller of this method
			 * will see an up-to-date version of the InListOperatorNode--
			 * and thus, if the caller references the InListOperatorNode's
			 * leftOperand, it will see the correct information. One
			 * notable example of this is at code generation time, where
			 * if this probe predicate is deemed "not useful", we'll
			 * generate the underlying InListOperatorNode instead of
			 * "this".  For that to work correctly, the InListOperatorNode
			 * must have the correct leftOperand. DERBY-3253.
			 *
			 * That said, since this.leftOperand will always be "up-to-
			 * date" w.r.t. the current query tree (because this probe
			 * predicate sits in the query tree and so all relevant
			 * transformations will be applied here), the simplest way
			 * to ensure the underlying InListOperatorNode also has an
			 * up-to-date leftOperand is to set it to this.leftOperand.
			 */
			inListProbeSource.setLeftOperand(this.leftOperand);
		}

		return inListProbeSource;
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
                    "find base table number for column reference check:", se);
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
		/* If this rel op is for an IN-list probe predicate then we never
		 * treat it as a qualifer.  The reason is that if we treat it as
		 * a qualifier then we could end up generating it as a qualifier,
		 * which would lead to the generation of an equality qualifier
		 * of the form "col = <val>" (where <val> is the first value in
		 * the IN-list).  That would lead to wrong results (missing rows)
		 * because that restriction is incorrect.
		 */
		if (isInListProbeNode())
			return false;

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
		typeId.getJDBCTypeId() == Types.BOOLEAN))
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
			SanityManager.ASSERT(getTypeServices() != null,
								 "dataTypeServices is expected to be non-null");
		/* xxxRESOLVE: look into doing this in place instead of allocating a new node */
		negation = (BinaryOperatorNode)
			getNodeFactory().getNode(getNegationNode(),
									 leftOperand, rightOperand,
									 getContextManager());
		negation.setType(getTypeServices());
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
		/* If this rel op is for a probe predicate then we do not call
		 * it a "relational operator"; it's actually a disguised IN-list
		 * operator.
		 */
		return !isInListProbeNode();
	}
	
	/** @see ValueNode#isBinaryEqualsOperatorNode */
	public boolean isBinaryEqualsOperatorNode()
	{
		/* If this rel op is for a probe predicate then we do not treat
		 * it as an "equals operator"; it's actually a disguised IN-list
		 * operator.
		 */
		return !isInListProbeNode() &&
			(operatorType == RelationalOperator.EQUALS_RELOP);
	}

	/**
	 * @see ValueNode#isInListProbeNode
	 *
	 * It's okay for this method to reference inListProbeSource directly
	 * because it does not rely on the contents of inListProbeSource's
	 * leftOperand, and a caller of this method cannot gain access to
	 * inListProbeSource's leftOperand through this method.
	 */
	public boolean isInListProbeNode()
	{
		return (inListProbeSource != null);
	}

	/** @see ValueNode#optimizableEqualityNode */
	public boolean optimizableEqualityNode(Optimizable optTable, 
										   int columnNumber, 
										   boolean isNullOkay)
		throws StandardException
	{
		if (operatorType != EQUALS_RELOP)
			return false;

		/* If this rel op is for a probe predicate then we do not treat
		 * it as an equality node; it's actually a disguised IN-list node.
		 */
		if (isInListProbeNode())
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
	 * exactly one operand in the predicate to which this operator belongs
	 * can and should be mapped to the received childRSN. 
	 *
     * @param whichSide The operand are we trying to scope (LEFT or RIGHT)
     * @param parentRSNsTables Set of all table numbers referenced by
     *  the ResultSetNode that is _parent_ to the received childRSN.
     *  We need this to make sure we don't scope the operand to a
     *  ResultSetNode to which it doesn't apply.
     * @param childRSN The result set node to which we want to create
     *  a scoped predicate.
     * @param whichRC If not -1 then this tells us which ResultColumn
     *  in the received childRSN we need to use for the scoped predicate;
     *  if -1 then the column position of the scoped column reference
     *  will be stored in this array and passed back to the caller.
     * @return A column reference scoped to the received childRSN, if possible.
     *  If the operand is a ColumnReference that is not supposed to be scoped,
	 *  we return a _clone_ of the reference--this is necessary because the
	 *  reference is going to be pushed to two places (left and right children
	 *  of the parentRSN) and if both children are referencing the same
	 *  instance of the column reference, they'll interfere with each other
	 *  during optimization.
	 */
	public ValueNode getScopedOperand(int whichSide,
		JBitSet parentRSNsTables, ResultSetNode childRSN,
		int [] whichRC) throws StandardException
	{
		ResultColumn rc = null;
		ColumnReference cr = 
			whichSide == LEFT
				? (ColumnReference)leftOperand
				: (ColumnReference)rightOperand;

		/* When we scope a predicate we only scope one side of it--the
		 * side that is to be evaluated against childRSN.  We figure out
		 * if "cr" is that side by using table numbers, as seen below.
		 * This means that for every scoped predicate there will be one
		 * operand that is scoped and one operand that is not scoped.  
		 * When we get here for the operand that will not be scoped,
		 * we'll just return a clone of that operand.  So in the example
		 * mentioned above, the scoped predicate for the left child of
		 * X1 would be
		 *
		 *   T1.j <scoped> = X2.b <clone> 
		 *
		 * That said, the first thing we need to do is see if this
		 * ColumnReference is supposed to be scoped for childRSN.  We
		 * do that by figuring out what underlying base table the column
		 * reference is pointing to and then seeing if that base table
		 * is included in the list of table numbers from the parentRSN.
		 */
		JBitSet crTables = new JBitSet(parentRSNsTables.size());
		BaseTableNumbersVisitor btnVis =
			new BaseTableNumbersVisitor(crTables);
		cr.accept(btnVis);

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
			return (ColumnReference)cr.getClone();

		/* Find the target ResultColumn in the received result set.  At
		 * this point we know that we do in fact need to scope the column
		 * reference for childRSN, so go ahead and do it.  The way in
		 * which we get the scope target column differs depending on
		 * if childRSN corresponds to the left or right child of the
		 * UNION node.  Before explaining that, though, note that it's
		 * not good enough to just search for the target column by
		 * name.  The reason is that it's possible the name provided
		 * for the column reference to be scoped doesn't match the
		 * name of the actual underlying column.  Ex.
		 *
		 *  select * from
		 *    (select i,j from t1 union select i,j from t2) X1 (x,y),
		 *    (select a,b from t3 union select a,b from t4) X2
		 *  where X1.x = X2.b;
		 *
		 * If we were scoping "X1.x" and we searched for "x" in the
		 * childRSN "select i,j from t1" we wouldn't find it.
		 *
		 * It is similarly incorrect to search for the target column
		 * by position (DERBY-1633).  This is a bit more subtle, but
		 * if the child to which we're scoping is a subquery whose RCL
		 * does not match the column ordering of the RCL for cr's source
		 * result set, then searching by column position can yield the
		 * wrong results, as well.  For a detailed example of how this
		 * can happen, see the fix description attached to DERBY-1633.
		 * 
		 * So how do we find the target column, then? As mentioned
		 * above, the way in which we get the scope target column
		 * differs depending on if childRSN corresponds to the left
		 * or right child of the parent UNION node.  And that said,
		 * we can tell if we're scoping a left child by looking at
		 * "whichRC" argument: if it is -1 then we know we're scoping
		 * to the left child of a Union; otherwise we're scoping to
		 * the right child.
		 */
		if (whichRC[0] == -1)
		{
			/*
			 * For the left side we start by figuring out what the source
			 * result set and column position for "cr" are.  Then, since
			 * a) cr must be pointing to a result column in the parentRSN's
			 * ResultColumnList,  b) we know that the parent RSN is a
			 * SetOperatorNode (at least for now, since we only get here
			 * for Union nodes), and c) SetOpNode's RCLs are built from the
			 * left child's RCL (see bindResultColumns() in SetOperatorNode),
			 * we know that if we search the child's RCL for a reference
			 * whose source result column is the same as cr's source result
			 * column, we'll find a match.  Once found, the position of the
			 * matching column w.r.t childRSN's RCL will be stored in the
			 * whichRC parameter.
			 */

			// Find the source result set and source column position of cr.
			int [] sourceColPos = new int[] {-1};
			ResultSetNode sourceRSN = cr.getSourceResultSet(sourceColPos);

			if (SanityManager.DEBUG)
			{
				/* We assumed that if we made it here "cr" was pointing
				 * to a base table somewhere down the tree.  If that's
				 * true then sourceRSN won't be null.  Make sure our
				 * assumption was correct.
				 */
				SanityManager.ASSERT(sourceRSN != null,
					"Failed to find source result set when trying to " +
					"scope column reference '" + cr.getTableName() +
					"." + cr.getColumnName());
			}

			// Now search for the corresponding ResultColumn in childRSN.
			rc = childRSN.getResultColumns()
					.getResultColumn(sourceColPos[0], sourceRSN, whichRC);
		}
		else
		{
			/*
			 * For the right side the story is slightly different.  If we were
			 * to search the right child's RCL for a reference whose source
			 * result column was the same as cr's, we wouldn't find it.  This
			 * is because cr's source result column comes from the left child's
			 * RCL and thus the right child doesn't know about it.  That said,
			 * though, for set operations like UNION, the left and right RCL's
			 * are correlated by position--i.e. the operation occurs between
			 * the nth column in the left RCL and the nth column in the right
			 * RCL.  So given that we will already have found the scope target
			 * in the left child's RCL at the position in whichRC, we know that
			 * that scope target for the right child's RCL is simply the
			 * whichRC'th column in that RCL.
			 */
			rc = childRSN.getResultColumns().getResultColumn(whichRC[0]);
		}

		// rc shouldn't be null; if there was no matching ResultColumn at all,
		// then we shouldn't have made it this far.
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(rc != null,
				"Failed to locate scope target result column when trying to " +
				"scope operand '" + cr.getTableName() + "." +
				cr.getColumnName() + "'.");
		}

		/* If the ResultColumn we found has an expression that is a
		 * ColumnReference, then that column reference has all of the
		 * info we need.
		 *
		 * It is, however, possible that the ResultColumn's expression
		 * is NOT a ColumnReference.  For example, the expression would
		 * be a constant expression if childRSN represented something
		 * like:
		 *
		 *   select 1, 1 from t1
		 *
		 * In this case the expression does not directly reference a
		 * column in the underlying result set and is therefore
		 * "scoped" as far as it can go.  This means that the scoped
		 * predicate will not necessarily have column references on
		 * both sides, even though the predicate that we're scoping
		 * will.  That's not a problem, though, since a predicate with
		 * a column reference on one side and a non-ColumnReference
		 * on the other is still valid.
		 */

		if (rc.getExpression() instanceof ColumnReference)
		{
			/* We create a clone of the column reference and mark
			 * the clone as "scoped" so that we can do the right
			 * thing when it comes time to remap the predicate;
			 * see Predicate.remapScopedPred() for more.
			 */
			ColumnReference cRef = (ColumnReference)
				((ColumnReference)rc.getExpression()).getClone();
			cRef.markAsScoped();
			return cRef;
		}

		/* Else just return rc's expression.  This means the scoped
		 * predicate will have one operand that is _not_ a column
		 * reference--but that's okay, so long as we account for
		 * that when pushing/remapping the scoped predicate down
		 * the query tree (see esp. "isScopedToSourceResultSet()"
		 * in Predicate.java).
		 */
		return rc.getExpression();
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
					"find base table numbers for reference check:", se);
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
