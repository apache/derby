/*

   Derby - Class org.apache.derby.impl.sql.compile.LengthOperatorNode

   Copyright 2000, 2004 The Apache Software Foundation or its licensors, as applicable.

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

import org.apache.derby.iapi.sql.compile.C_NodeTypes;

import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.types.TypeId;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.ConcatableDataValue;
import org.apache.derby.iapi.sql.compile.TypeCompiler;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.ClassName;
import org.apache.derby.iapi.reference.JDBC20Translation;

import java.sql.Types;

import java.util.Vector;

/**
 * This node represents a unary XXX_length operator
 *
 * @author Jeff Lichtman
 */

public final class LengthOperatorNode extends UnaryOperatorNode
{
	private int parameterType;
	private int parameterWidth;

	public void setNodeType(int nodeType)
	{
		String operator = null;
		String methodName = null;

		if (nodeType == C_NodeTypes.CHAR_LENGTH_OPERATOR_NODE)
		{
				operator = "char_length";
				methodName = "charLength";
				parameterType = Types.VARCHAR;
				parameterWidth = TypeId.VARCHAR_MAXWIDTH;
		}
		else
		{
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
		Vector	aggregateVector)
			throws StandardException
	{
		TypeId	operandType;

		super.bindExpression(fromList, subqueryList,
				aggregateVector);

		/*
		** Check the type of the operand - this function is allowed only on
		** string value types.  
		*/
		operandType = operand.getTypeId();
		switch (operandType.getJDBCTypeId())
		{
				case Types.CHAR:
				case Types.VARCHAR:
				case Types.BINARY:
				case Types.VARBINARY:
				case Types.LONGVARBINARY:
				case Types.LONGVARCHAR:
                case JDBC20Translation.SQL_TYPES_BLOB:
                case JDBC20Translation.SQL_TYPES_CLOB:
					break;
			
				default:
					throw StandardException.newException(SQLState.LANG_UNARY_FUNCTION_BAD_TYPE,
											getOperatorString(),
											operandType.getSQLTypeName());
		}

		/*
		** The result type of XXX_length is int.
		*/
		setType(new DataTypeDescriptor(
							TypeId.INTEGER_ID,
							operand.getTypeServices().isNullable()
						)
				);
		return this;
	}

	/**
	 * Bind a ? parameter operand of the XXX_length function.
	 *
	 * @return	Nothing
	 *
	 * @exception StandardException		Thrown on error
	 */

	void bindParameter()
			throws StandardException
	{
		/*
		** According to the SQL standard, if XXX_length has a ? operand,
		** its type is varchar with the implementation-defined maximum length
		** for a varchar.
		*/

		((ParameterNode) operand).setDescriptor(
							DataTypeDescriptor.getBuiltInDataTypeDescriptor(parameterType, true, 
												parameterWidth));
	}

	/**
	 * This is a length operator node.  Overrides this method
	 * in UnaryOperatorNode for code generation purposes.
	 */
	public String getReceiverInterfaceName() {
	    return ClassName.ConcatableDataValue;
	}
}
