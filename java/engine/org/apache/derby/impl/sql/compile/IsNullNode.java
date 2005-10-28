/*

   Derby - Class org.apache.derby.impl.sql.compile.IsNullNode

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

import org.apache.derby.iapi.services.compiler.MethodBuilder;
import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.sql.compile.C_NodeTypes;
import org.apache.derby.iapi.sql.compile.Optimizable;

import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;

import org.apache.derby.iapi.store.access.ScanController;

import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.DataValueFactory;
import org.apache.derby.iapi.types.TypeId;

import org.apache.derby.iapi.types.Orderable;

import org.apache.derby.impl.sql.compile.ExpressionClassBuilder;

import java.sql.Types;

/**
 * This node represents either a unary 
 * IS NULL or IS NOT NULL comparison operator
 *
 * @author Jerry Brenner
 */

public final class IsNullNode extends UnaryComparisonOperatorNode
						implements RelationalOperator
{

	Object nullValue = null;

	public void setNodeType(int nodeType)
	{
		String operator;
		String methodName;

		if (nodeType == C_NodeTypes.IS_NULL_NODE)
		{
			/* By convention, the method name for the is null operator is "isNull" */
			operator = "is null";
			methodName = "isNullOp";
		}
		else
		{
			if (SanityManager.DEBUG)
			{
				if (nodeType != C_NodeTypes.IS_NOT_NULL_NODE)
				{
					SanityManager.THROWASSERT(
						"Unexpected nodeType = " + nodeType);
				}
			}
			/* By convention, the method name for the is not null operator is 
			 * "isNotNull" 
			 */
			operator = "is not null";
			methodName = "isNotNull";
		}
		setOperator(operator);
		setMethodName(methodName);
		super.setNodeType(nodeType);
	}

	/**
	 * Negate the comparison.
	 *
	 * @param operand	The operand of the operator
	 *
	 * @return UnaryOperatorNode	The negated expression
	 *
	 * @exception StandardException		Thrown on error
	 */
	UnaryOperatorNode getNegation(ValueNode operand)
				throws StandardException
	{
		UnaryOperatorNode negation;

		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(dataTypeServices != null,
						"dataTypeServices is expected to be non-null");
		}

		if (isNullNode())
		{
			setNodeType(C_NodeTypes.IS_NOT_NULL_NODE);
		}
		else
		{
			if (SanityManager.DEBUG)
			{
				if (! isNotNullNode())
				{
					SanityManager.THROWASSERT(
						"Unexpected nodeType = " + getNodeType());
				}
			}
			setNodeType(C_NodeTypes.IS_NULL_NODE);
		}
		return this;
	}

	/**
	 * Bind a ? parameter operand of the IS [NOT] NULL predicate.
	 *
	 * @return	Nothing
	 *
	 * @exception StandardException		Thrown on error
	 */

	void bindParameter()
			throws StandardException
	{
		/*
		** If IS [NOT] NULL has a ? operand, we assume
		** its type is varchar with the implementation-defined maximum length
		** for a varchar.
		*/

		((ParameterNode) operand).setDescriptor(
							new DataTypeDescriptor(TypeId.getBuiltInTypeId(Types.VARCHAR), true));
	}

	/* RelationalOperator interface */

	/** @see RelationalOperator#usefulStartKey */
	public boolean usefulStartKey(Optimizable optTable)
	{
		// IS NULL is start/stop key, IS NOT NULL is not
		return (isNullNode());
	}

	/** @see RelationalOperator#usefulStopKey */
	public boolean usefulStopKey(Optimizable optTable)
	{
		// IS NULL is start/stop key, IS NOT NULL is not
		return (isNullNode());
	}

	/** @see RelationalOperator#getStartOperator */
	public int getStartOperator(Optimizable optTable)
	{
		if (SanityManager.DEBUG)
		{
			if (! isNullNode())
			{
				SanityManager.THROWASSERT(
					"getNodeType() not expected to return " + getNodeType());
			}
		}
		return ScanController.GE;
	}

	/** @see RelationalOperator#getStopOperator */
	public int getStopOperator(Optimizable optTable)
	{
		if (SanityManager.DEBUG)
		{
			if (! isNullNode())
			{
				SanityManager.THROWASSERT(
					"getNodeType() not expected to return " + getNodeType());
			}
		}
		return ScanController.GT;
	}

	/** @see RelationalOperator#generateOperator */
	public void generateOperator(MethodBuilder mb,
										Optimizable optTable)
	{
		mb.push(Orderable.ORDER_OP_EQUALS);
	}

	/** @see RelationalOperator#generateNegate */
	public void generateNegate(MethodBuilder mb,
										Optimizable optTable)
	{
		mb.push(isNotNullNode());
	}

	/** @see RelationalOperator#getOperator */
	public int getOperator()
	{
		int operator;
		if (isNullNode())
		{
			operator = IS_NULL_RELOP;
		}
		else
		{
			if (SanityManager.DEBUG)
			{
				if (! isNotNullNode())
				{
					SanityManager.THROWASSERT(
						"Unexpected nodeType = " + getNodeType());
				}
			}
			operator = IS_NOT_NULL_RELOP;
		}

		return operator;
	}

	/** @see RelationalOperator#compareWithKnownConstant */
	public boolean compareWithKnownConstant(Optimizable optTable, boolean considerParameters)
	{
		return true;
	}

	/**
	 * @see RelationalOperator#getCompareValue
	 *
	 * @exception StandardException		Thrown on error
	 */
	public DataValueDescriptor getCompareValue(Optimizable optTable)
					throws StandardException
	{
		if (nullValue == null)
		{
			nullValue = operand.getTypeId().getNull();
		}

		return (DataValueDescriptor) nullValue;
	}

	/** @see RelationalOperator#equalsComparisonWithConstantExpression */
	public boolean equalsComparisonWithConstantExpression(Optimizable optTable)
	{
		boolean retval = false;

		// Always return false for NOT NULL
		if (isNotNullNode())
		{
			return false;
		}

		/*
		** Is the operand a column in the given table?
		*/
		if (operand instanceof ColumnReference)
		{
			int tabNum = ((ColumnReference) operand).getTableNumber();
			if (optTable.hasTableNumber() &&
				(optTable.getTableNumber() == tabNum))
			{
				retval = true;
			}
		}

		return retval;
	}

	/** 
	 * @see RelationalOperator#getTransitiveSearchClause 
	 *
	 * @exception StandardException	thrown on error
	 */
	public RelationalOperator getTransitiveSearchClause(ColumnReference otherCR)
		throws StandardException
	{
		return (RelationalOperator) getNodeFactory().getNode(
									getNodeType(),
									otherCR,
									getContextManager());
	}

	/**
	 * null operators are defined on DataValueDescriptor.
	 * Overrides method in UnaryOperatorNode for code generation purposes.
	 */
	public String getReceiverInterfaceName() {
	    return ClassName.DataValueDescriptor;
	}

	/** IS NULL is like =, so should have the same selectivity */
	public double selectivity(Optimizable optTable) 
	{
		if (isNullNode())
		{
			return 0.1d;
		}
		else
		{
			if (SanityManager.DEBUG)
			{
				if (! isNotNullNode())
				{
					SanityManager.THROWASSERT(
						"Unexpected nodeType = " + getNodeType());
				}
			}
			/* IS NOT NULL is like <>, so should have same selectivity */
			return 0.9d;
		}
	}

	private boolean isNullNode()
	{
		return getNodeType() == C_NodeTypes.IS_NULL_NODE;
	}

	private boolean isNotNullNode()
	{
		return getNodeType() == C_NodeTypes.IS_NOT_NULL_NODE;
	}
	
	/** @see ValueNode#isRelationalOperator */
	public boolean isRelationalOperator()
	{
		return true;
	}

	/** @see ValueNode#optimizableEqualityNode */
	public boolean optimizableEqualityNode(Optimizable optTable, 
										   int columnNumber, 
										   boolean isNullOkay)
	{
		if (!isNullNode() || !isNullOkay)
			return false;
		
		ColumnReference cr = getColumnOperand(optTable,
											  columnNumber);
		if (cr == null)
		{
			return false;
		}

		return true;
	}

}
