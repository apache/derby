/*

   Derby - Class org.apache.derby.impl.sql.compile.BinaryArithmeticOperatorNode

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

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.sql.dictionary.DataDictionary;

import org.apache.derby.iapi.sql.compile.C_NodeTypes;

import org.apache.derby.iapi.types.TypeId;

import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.NumberDataValue;

import org.apache.derby.iapi.sql.compile.TypeCompiler;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.impl.sql.compile.ActivationClassBuilder;
import org.apache.derby.iapi.reference.ClassName;

import java.sql.Types;

import java.util.Vector;

/**
 * This node represents a binary arithmetic operator, like + or *.
 *
 * @author Jeff Lichtman
 */

public final class BinaryArithmeticOperatorNode extends BinaryOperatorNode
{
	/**
	 * Initializer for a BinaryArithmeticOperatorNode
	 *
	 * @param leftOperand	The left operand
	 * @param rightOperand	The right operand
	 */

	public void init(
					Object leftOperand,
					Object rightOperand)
	{
		super.init(leftOperand, rightOperand,
				ClassName.NumberDataValue, ClassName.NumberDataValue);
	}

	public void setNodeType(int nodeType)
	{
		String operator = null;
		String methodName = null;

		switch (nodeType)
		{
			case C_NodeTypes.BINARY_DIVIDE_OPERATOR_NODE:
				operator = TypeCompiler.DIVIDE_OP;
				methodName = "divide";
				break;

			case C_NodeTypes.BINARY_MINUS_OPERATOR_NODE:
				operator = TypeCompiler.MINUS_OP;
				methodName = "minus";
				break;

			case C_NodeTypes.BINARY_PLUS_OPERATOR_NODE:
				operator = TypeCompiler.PLUS_OP;
				methodName = "plus";
				break;

			case C_NodeTypes.BINARY_TIMES_OPERATOR_NODE:
				operator = TypeCompiler.TIMES_OP;
				methodName = "times";
				break;

			case C_NodeTypes.MOD_OPERATOR_NODE:
				operator = TypeCompiler.MOD_OP;
				methodName = "mod";
				break;

			default:
				if (SanityManager.DEBUG)
				{
					SanityManager.THROWASSERT(
						"Unexpected nodeType = " + nodeType);
				}
		}
		setOperator(operator);
		setMethodName(methodName);
		super.setNodeType(nodeType);
	}

	/**
	 * Bind this operator
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
		FromList	fromList, SubqueryList subqueryList,
		Vector aggregateVector)
			throws StandardException
	{
		super.bindExpression(fromList, subqueryList,
				aggregateVector);

		TypeId	leftType = leftOperand.getTypeId();
		TypeId	rightType = rightOperand.getTypeId();
		DataTypeDescriptor	leftDTS = leftOperand.getTypeServices();
		DataTypeDescriptor	rightDTS = rightOperand.getTypeServices();

		/* Do any implicit conversions from (long) (var)char. */
		if (leftType.isStringTypeId() && rightType.isNumericTypeId())
		{
			boolean nullableResult;
			nullableResult = leftDTS.isNullable() ||
		 					 rightDTS.isNullable();
			/* If other side is decimal/numeric, then we need to diddle
			 * with the precision, scale and max width in order to handle
			 * computations like:  1.1 + '0.111'
			 */
			int precision = rightDTS.getPrecision();
			int scale	  = rightDTS.getScale();
			int maxWidth  = rightDTS.getMaximumWidth();

			if (rightType.isDecimalTypeId())
			{
				int charMaxWidth = leftDTS.getMaximumWidth();
				precision += (2 * charMaxWidth);								
				scale += charMaxWidth;								
				maxWidth = precision + 3;
			}

			leftOperand = (ValueNode)
					getNodeFactory().getNode(
						C_NodeTypes.CAST_NODE,
						leftOperand, 
						new DataTypeDescriptor(rightType, precision,
											scale, nullableResult, 
											maxWidth),
						getContextManager());
			((CastNode) leftOperand).bindCastNodeOnly();
		}
		else if (rightType.isStringTypeId() && leftType.isNumericTypeId())
		{
			boolean nullableResult;
			nullableResult = leftDTS.isNullable() ||
		 					 rightDTS.isNullable();
			/* If other side is decimal/numeric, then we need to diddle
			 * with the precision, scale and max width in order to handle
			 * computations like:  1.1 + '0.111'
			 */
			int precision = leftDTS.getPrecision();
			int scale	  = leftDTS.getScale();
			int maxWidth  = leftDTS.getMaximumWidth();

			if (leftType.isDecimalTypeId())
			{
				int charMaxWidth = rightDTS.getMaximumWidth();
				precision += (2 * charMaxWidth);								
				scale += charMaxWidth;								
				maxWidth = precision + 3;
			}

			rightOperand =  (ValueNode)
					getNodeFactory().getNode(
						C_NodeTypes.CAST_NODE,
						rightOperand, 
						new DataTypeDescriptor(leftType, precision,
											scale, nullableResult, 
											maxWidth),
						getContextManager());
			((CastNode) rightOperand).bindCastNodeOnly();
		}

		/*
		** Set the result type of this operator based on the operands.
		** By convention, the left operand gets to decide the result type
		** of a binary operator.
		*/
		setType(leftOperand.getTypeCompiler().
					resolveArithmeticOperation(
						leftOperand.getTypeServices(),
						rightOperand.getTypeServices(),
						operator
							)
				);

		return this;
	}
}
