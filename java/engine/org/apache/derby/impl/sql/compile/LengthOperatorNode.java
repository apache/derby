/*

   Derby - Class org.apache.derby.impl.sql.compile.LengthOperatorNode

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

import java.sql.Types;
import java.util.List;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.ClassName;
import org.apache.derby.shared.common.reference.SQLState;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.TypeId;

/**
 * This node represents a unary XXX_length operator
 *
 */

public final class LengthOperatorNode extends UnaryOperatorNode
{
	private int parameterType;
	private int parameterWidth;

    LengthOperatorNode(ValueNode operator, ContextManager cm)
            throws StandardException {
        super(operator, cm);

        String op = "char_length";
        String methodNam = "charLength";
        parameterType = Types.VARCHAR;
        parameterWidth = TypeId.VARCHAR_MAXWIDTH;

        setOperator(op);
        setMethodName(methodNam);
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
		TypeId	operandType;

        bindOperand(fromList, subqueryList, aggregates);

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
                case Types.BLOB:
                case Types.CLOB:
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
	 * @exception StandardException		Thrown on error
	 */
    @Override
	void bindParameter()
			throws StandardException
	{
		/*
		** According to the SQL standard, if XXX_length has a ? operand,
		** its type is varchar with the implementation-defined maximum length
		** for a varchar.
		** Also, for XXX_length, it doesn't matter what is VARCHAR's collation 
		** (since for XXX_length, no collation sensitive processing is 
		** is required) and hence we will not worry about the collation setting
		*/

		operand.setType(DataTypeDescriptor.getBuiltInDataTypeDescriptor(parameterType, true, 
												parameterWidth));
	}

	/**
	 * This is a length operator node.  Overrides this method
	 * in UnaryOperatorNode for code generation purposes.
	 */
    @Override
    String getReceiverInterfaceName() {
	    return ClassName.ConcatableDataValue;
	}
}
