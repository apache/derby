/*

   Derby - Class org.apache.derby.impl.sql.compile.BinaryArithmeticOperatorNode

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
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.ClassName;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derby.iapi.sql.compile.TypeCompiler;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.TypeId;

/**
 * This node represents a binary arithmetic operator, like + or *.
 *
 */

public final class BinaryArithmeticOperatorNode extends BinaryOperatorNode
{
    // Allowed kinds
    final static int K_DIVIDE = 0;
    final static int K_MINUS = 1;
    final static int K_PLUS = 2;
    final static int K_TIMES = 3;
    final static int K_MOD = 4;

    /**
     * This class is used to hold logically different objects for
     * space efficiency. {@code kind} represents the logical object
     * type. See also {@link ValueNode#isSameNodeKind}.
     */
    final int kind;

	/**
     * Constructor for a BinaryArithmeticOperatorNode
	 *
     * @param kind          The kind of operator
	 * @param leftOperand	The left operand
	 * @param rightOperand	The right operand
     * @param cm            The context manager
	 */

    BinaryArithmeticOperatorNode(
            int kind,
            ValueNode leftOperand,
            ValueNode rightOperand,
            ContextManager cm)
	{
        super(leftOperand,
              rightOperand,
              ClassName.NumberDataValue,
              ClassName.NumberDataValue,
              cm);
        this.kind = kind;

        final String op;
        final String mNam;

        switch (kind)
		{
            case K_DIVIDE:
                op = TypeCompiler.DIVIDE_OP;
                mNam = "divide";
				break;

            case K_MINUS:
                op = TypeCompiler.MINUS_OP;
                mNam = "minus";
				break;

            case K_PLUS:
                op = TypeCompiler.PLUS_OP;
                mNam = "plus";
				break;

            case K_TIMES:
                op = TypeCompiler.TIMES_OP;
                mNam = "times";
				break;

            case K_MOD:
                op = TypeCompiler.MOD_OP;
                mNam = "mod";
				break;
            default:
                if (SanityManager.DEBUG) {
                    SanityManager.NOTREACHED();
                }
                op = null;
                mNam = null;
        }
        setOperator(op);
        setMethodName(mNam);
	}

	/**
	 * Bind this operator
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
        FromList fromList, SubqueryList subqueryList, List<AggregateNode> aggregates)
			throws StandardException
	{
        super.bindExpression(fromList, subqueryList, aggregates);

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

            leftOperand = new CastNode(
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

            rightOperand = new CastNode(
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

    @Override
    boolean isSameNodeKind(ValueNode o) {
        return super.isSameNodeKind(o) &&
                ((BinaryArithmeticOperatorNode)o).kind == kind;
    }
}
