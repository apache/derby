/*

   Derby - Class org.apache.derby.impl.sql.compile.SimpleStringOperatorNode

   Copyright 1998, 2004 The Apache Software Foundation or its licensors, as applicable.

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

import org.apache.derby.iapi.sql.compile.C_NodeTypes;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.sql.dictionary.DataDictionary;

import org.apache.derby.iapi.types.TypeId;
import org.apache.derby.iapi.types.DataTypeDescriptor;

import org.apache.derby.iapi.types.StringDataValue;

import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.reference.ClassName;

import java.sql.Types;

import java.util.Vector;

/**
 * This node represents a unary upper or lower operator
 *
 * @author Jerry
 */

public class SimpleStringOperatorNode extends UnaryOperatorNode
{
	/**
	 * Initializer for a SimpleOperatorNode
	 *
	 * @param operand		The operand
	 * @param methodName	The method name
	 */

	public void init(Object operand, Object methodName)
	{
		super.init(operand, methodName, methodName);
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
		TypeId	operandType;

		super.bindExpression(fromList, subqueryList, 
				aggregateVector);

		/*
		** Check the type of the operand - this function is allowed only on
		** string value (char and bit) types.
		*/
		operandType = operand.getTypeId();

		switch (operandType.getJDBCTypeId())
		{
				case Types.CHAR:
				case Types.VARCHAR:
				case Types.LONGVARCHAR:
				case Types.CLOB:
					break;
				case org.apache.derby.iapi.reference.JDBC20Translation.SQL_TYPES_JAVA_OBJECT:
				case Types.OTHER:	
				{
					throw StandardException.newException(SQLState.LANG_UNARY_FUNCTION_BAD_TYPE, 
										methodName,
										operandType.getSQLTypeName());
				}

				default:
					operand =  (ValueNode)
						getNodeFactory().getNode(
							C_NodeTypes.CAST_NODE,
							operand,
							DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, true, 
										  operand.getTypeCompiler().
											getCastToCharWidth(
												operand.getTypeServices())),
							getContextManager());
					((CastNode) operand).bindCastNodeOnly();
					operandType = operand.getTypeId();
		}

		/*
		** The result type of upper()/lower() is the type of the operand.
		*/

		setType(new DataTypeDescriptor(operandType,
				operand.getTypeServices().isNullable(),
				operand.getTypeCompiler().
					getCastToCharWidth(operand.getTypeServices())
						)
				);

		return this;
	}

	/**
	 * Bind a ? parameter operand of the upper/lower function.
	 *
	 * @return	Nothing
	 *
	 * @exception StandardException		Thrown on error
	 */

	void bindParameter()
			throws StandardException
	{
		/*
		** According to the SQL standard, if bit_length has a ? operand,
		** its type is bit varying with the implementation-defined maximum length
		** for a bit.
		*/

		((ParameterNode) operand).setDescriptor(
							DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR));
	}

	/**
	 * This is a length operator node.  Overrides this method
	 * in UnaryOperatorNode for code generation purposes.
	 */
	public String getReceiverInterfaceName() {
	    return ClassName.StringDataValue;
	}
}
