/*

   Derby - Class org.apache.derby.impl.sql.compile.BinaryLogicalOperatorNode

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

import org.apache.derby.iapi.types.BooleanDataValue;
import org.apache.derby.iapi.types.TypeId;

import org.apache.derby.iapi.sql.compile.C_NodeTypes;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.compiler.MethodBuilder;
import org.apache.derby.iapi.services.compiler.LocalField;

import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.reference.ClassName;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.types.DataTypeDescriptor;

import java.lang.reflect.Modifier;
import org.apache.derby.impl.sql.compile.ExpressionClassBuilder;
import org.apache.derby.impl.sql.compile.ActivationClassBuilder;
import org.apache.derby.iapi.services.classfile.VMOpcode;

import java.util.Vector;

public abstract class BinaryLogicalOperatorNode extends BinaryOperatorNode
{
	boolean	shortCircuitValue;

	/**
	 * Initializer for a BinaryLogicalOperatorNode
	 *
	 * @param leftOperand	The left operand of the comparison
	 * @param rightOperand	The right operand of the comparison
	 * @param shortCircuitValue	The value which, if found on the left, means
	 *							we don't have to evaluate the right.
	 * @param methodName	The name of the method to call in the generated
	 *						class.  In this case, it's actually an operator
	 *						name.
	 */

	public void init(
				Object	leftOperand,
				Object	rightOperand,
				Object		shortCircuitValue,
				Object		methodName)
	{
		/* For logical operators, the operator and method names are the same */
		super.init(leftOperand, rightOperand, methodName, methodName,
				ClassName.BooleanDataValue, ClassName.BooleanDataValue);
		this.shortCircuitValue = ((Boolean) shortCircuitValue).booleanValue();
	}

	/**
	 * Bind this logical operator.  All that has to be done for binding
	 * a logical operator is to bind the operands, check that both operands
	 * are BooleanDataValue, and set the result type to BooleanDataValue.
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
		FromList fromList, SubqueryList subqueryList,
		Vector aggregateVector)
			throws StandardException
	{
		//following is to check if we have something like "? AND 1=1" or "2>1 OR ?" 
		if (leftOperand.isParameterNode() || rightOperand.isParameterNode())
			throw StandardException.newException(SQLState.LANG_NON_BOOLEAN_WHERE_CLAUSE, "PARAMETER" );

		super.bindExpression(fromList, subqueryList, aggregateVector);

		return this;
	}

	/**
	 * Verify that eliminateNots() did its job correctly.  Verify that
	 * there are no NotNodes above the top level comparison operators
	 * and boolean expressions.
	 *
	 * @return		Boolean which reflects validity of the tree.
	 */
	boolean verifyEliminateNots()
	{
		if (SanityManager.ASSERT)
		{
			return (leftOperand.verifyEliminateNots() &&
					rightOperand.verifyEliminateNots());
		}
		else
		{
			return true;
		}
	}

	/**
	 * Do code generation for this logical binary operator.
	 *
	 * @param acb	The ExpressionClassBuilder for the class we're generating
	 * @param mb	The method the code to place the code
	 *
	 *
	 * @exception StandardException		Thrown on error
	 */

	public void generateExpression(ExpressionClassBuilder acb,
											MethodBuilder mb)
		throws StandardException
	{
		/*
		** This generates the following code:
		**
		** (<fieldx> = <leftOperand>).equals(shortCircuitValue) ?
		**	 <fieldy> = <shortCircuitValue, nullability> :
		**   fieldx.<and/or>(<rightOperand>, nullability)
		**
		** The ?: operator accomplishes the short-circuiting.  We save the
		** value of the left operand in a field so we don't have to evaluate
		** it twice.  We save the return value of the getBoolean() call so
		** we can re-use that object rather than allocate a new one every
		** time this method is called.
		*/

		/*
		** Save the evaluation of the left operand in a field.
		** Generated code is:
		**		(<fieldx> = <leftOperand>)
		*/
		LocalField leftOperandSaver = acb.newFieldDeclaration(Modifier.PRIVATE,
												ClassName.BooleanDataValue);

		/*
		** See whether the left operand equals the short-circuit value.
		** Generated code is:
		**		.equals(shortCircuitValue)
		*/

		leftOperand.generateExpression(acb, mb);
		mb.putField(leftOperandSaver);
		mb.push(shortCircuitValue);
		mb.callMethod(VMOpcode.INVOKEINTERFACE, (String) null, "equals", "boolean", 1);

		/*
		** Generate the if expression.  This is what accomplishes
		** short-circuiting.
		**
		** Generated code is:
		**
		**		<test for short circuiting> ?
		**			<call to getBooleanDataValue> : <call to operator method>
		*/

		mb.conditionalIf();

		/*
		** Generate the return value if the left operand equals the short-
		** circuit value.  Generated code calls a static method in the
		** boolean datatype implementation that allocates a new object
		** if necessary, and re-uses the object if it already exists.
		*/
		LocalField reusableBoolean = acb.newFieldDeclaration(Modifier.PRIVATE,
												ClassName.BooleanDataValue);

		mb.push(shortCircuitValue);
		acb.generateDataValue(mb, getTypeCompiler(), reusableBoolean);


		mb.startElseCode();

		/*
		** Generate the return value if the left operand does not equal the
		** short-circuit value.  This is the call to "and" or "or".
		**
		** Generated code is:
		**
		**	<fieldx>.<methodName>(<rightOperand>)
		*/

		mb.getField(leftOperandSaver);

		rightOperand.generateExpression(acb, mb);
		mb.upCast(ClassName.BooleanDataValue);

		mb.callMethod(VMOpcode.INVOKEINTERFACE, (String) null, methodName, ClassName.BooleanDataValue, 1);

		mb.completeConditional();
	}

	DataTypeDescriptor resolveLogicalBinaryOperator(
								DataTypeDescriptor leftType,
								DataTypeDescriptor rightType)
							throws StandardException
	{
		if ( ( ! (leftType.getTypeId().isBooleanTypeId()) ) ||
			 ( ! (rightType.getTypeId().isBooleanTypeId()) ) )
		{
			throw StandardException.newException(SQLState.LANG_BINARY_LOGICAL_NON_BOOLEAN);
		}

		return new DataTypeDescriptor(leftType,
					leftType.isNullable() || rightType.isNullable());
	}
}
