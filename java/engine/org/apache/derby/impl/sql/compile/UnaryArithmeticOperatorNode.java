/*

   Derby - Class org.apache.derby.impl.sql.compile.UnaryArithmeticOperatorNode

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

import org.apache.derby.iapi.sql.dictionary.DataDictionary;

import org.apache.derby.iapi.types.TypeId;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.services.compiler.MethodBuilder;
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.sql.compile.C_NodeTypes;
import org.apache.derby.impl.sql.compile.ExpressionClassBuilder;

import java.sql.Types;
import java.util.Vector;

/**
 * This node represents a unary arithmetic operator
 *
 * @author Manish Khettry
 */

public class UnaryArithmeticOperatorNode extends UnaryOperatorNode
{
	private final static int UNARY_PLUS	= 0;
	private final static int UNARY_MINUS	= 1;
	private final static int SQRT = 2;
	private final static int ABSOLUTE = 3;
	private final static String[] UNARY_OPERATORS = {"+","-","SQRT", "ABS/ABSVAL"};
	private final static String[] UNARY_METHODS = {"plus","minus","sqrt", "absolute"};

	private int operatorType;


	/**
	 * Initializer for a UnaryArithmeticOperatorNode
	 *
	 * @param operand		The operand of the node
	 */
	public void init(Object operand)
	{
		switch(getNodeType())
		{
			case C_NodeTypes.UNARY_PLUS_OPERATOR_NODE:
				operatorType = UNARY_PLUS;
				break;
			case C_NodeTypes.UNARY_MINUS_OPERATOR_NODE:
				operatorType = UNARY_MINUS;
				break;
			case C_NodeTypes.SQRT_OPERATOR_NODE:
				operatorType = SQRT;
				break;
			case C_NodeTypes.ABSOLUTE_OPERATOR_NODE:
				operatorType = ABSOLUTE;
				break;
			default:
				if (SanityManager.DEBUG)
				{
					SanityManager.THROWASSERT("init for UnaryArithmeticOperator called with wrong nodeType = " + getNodeType());
				}
			    break;
		}
		init(operand, UNARY_OPERATORS[this.operatorType], 
				UNARY_METHODS[this.operatorType]);
	}

	/**
	 * By default unary operators don't accept ? parameters as operands.
	 * This can be over-ridden for particular unary operators.
	 *
	 *	We throw an exception if the parameter doesn't have a datatype
	 *	assigned to it yet.
	 *
	 * @return	Nothing
	 *
	 * @exception StandardException		Thrown if ?  parameter doesn't
	 *									have a type bound to it yet.
	 *									? parameter where it isn't allowed.
	 */

	void bindParameter() throws StandardException
	{
		if (operatorType == SQRT || operatorType == ABSOLUTE)
		{
			((ParameterNode) operand).setDescriptor(
				new DataTypeDescriptor(TypeId.getBuiltInTypeId(Types.DOUBLE), true));
		}
		else if (operand.getTypeServices() == null)
		{
			throw StandardException.newException(SQLState.LANG_UNARY_OPERAND_PARM, operator);
		}
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
		Vector	aggregateVector)
			throws StandardException
	{
		super.bindExpression(fromList, subqueryList,
				aggregateVector);

		if (operatorType == SQRT || operatorType == ABSOLUTE)
		{
			bindSQRTABS();
		}
		else if (operatorType == UNARY_PLUS || operatorType == UNARY_MINUS)
		{
			TypeId operandType = operand.getTypeId();

			if ( ! operandType.isNumericTypeId())
			{
			
				throw StandardException.newException(SQLState.LANG_UNARY_ARITHMETIC_BAD_TYPE, 
					(operatorType == UNARY_PLUS) ? "+" : "-", 
					operandType.getSQLTypeName());
			}
		}
		/*
		** The result type of a +, -, SQRT, ABS is the same as its operand.
		*/
		setType(operand.getTypeServices());
		return this;
	}

	/**
	 * Do code generation for this unary plus operator
	 *
	 * @param acb	The ExpressionClassBuilder for the class we're generating
	 * @param mb	The method the expression will go into
	 *
	 * @exception StandardException		Thrown on error
	 */

	public void generateExpression(ExpressionClassBuilder acb,
											MethodBuilder mb)
									throws StandardException
	{
		/* Unary + doesn't do anything.  Just return the operand */
		if (operatorType == UNARY_PLUS)
			operand.generateExpression(acb, mb);
		else
			super.generateExpression(acb, mb);
	}
	/**
	 * Bind SQRT or ABS
	 *
	 * @exception StandardException		Thrown on error
	 */
	private void bindSQRTABS()
			throws StandardException
	{
		TypeId	operandType;
		int 	jdbcType;

		/*
		** Check the type of the operand 
		*/
		operandType = operand.getTypeId();

		/*
	 	 * If the operand is not a build-in type, generate a bound conversion
		 * tree to build-in types.
		 */
		if( ! operandType.systemBuiltIn() )
		{
			operand = operand.genSQLJavaSQLTree();
		}
		/* DB2 doesn't cast string types to numeric types for numeric functions  */

		jdbcType = operandType.getJDBCTypeId();

		/* Both SQRT and ABS are only allowed on numeric types */
		if (!operandType.isNumericTypeId())
			throw StandardException.newException(
						SQLState.LANG_UNARY_FUNCTION_BAD_TYPE, 
						getOperatorString(), operandType.getSQLTypeName());

		/* For SQRT, if operand is not a DOUBLE, convert it to DOUBLE */
		if (operatorType == SQRT && jdbcType != Types.DOUBLE)
		{
			operand = (ValueNode) getNodeFactory().getNode(
					C_NodeTypes.CAST_NODE,
					operand,
					new DataTypeDescriptor(TypeId.getBuiltInTypeId(Types.DOUBLE), true),
					getContextManager());
			((CastNode) operand).bindCastNodeOnly();
		}
	}
}
