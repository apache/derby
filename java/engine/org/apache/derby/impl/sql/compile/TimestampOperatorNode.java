/*

   Derby - Class org.apache.derby.impl.sql.compile.TimestampOperatorNode

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
import org.apache.derby.iapi.services.classfile.VMOpcode;
import org.apache.derby.iapi.services.compiler.MethodBuilder;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.TypeId;

/**
 * The TimestampOperatorNode class implements the timestamp( date, time) function.
 */

class TimestampOperatorNode extends BinaryOperatorNode
{

    /**
     * Constructor for a TimestampOperatorNode.
     *
     * @param date The date
     * @param time The time
     * @param cm   The context manager
     */

    TimestampOperatorNode(ValueNode date,
                          ValueNode time,
                          ContextManager cm)
    {
        super(cm);
        leftOperand = date;
        rightOperand = time;
        operator = "timestamp";
        methodName = "getTimestamp";
    }


	/**
	 * Bind this expression.  This means binding the sub-expressions,
	 * as well as figuring out what the return type is for this expression.
	 *
	 * @param fromList		The FROM list for the query this
	 *				expression is in, for binding columns.
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
		leftOperand = leftOperand.bindExpression(fromList, subqueryList, 
            aggregates);
		rightOperand = rightOperand.bindExpression(fromList, subqueryList, 
            aggregates);

		//Set the type if there is a parameter involved here 
		if (leftOperand.requiresTypeFromContext()) {
			leftOperand.setType(DataTypeDescriptor.getBuiltInDataTypeDescriptor( Types.DATE));
		}
		//Set the type if there is a parameter involved here 
		if (rightOperand.requiresTypeFromContext()) {
			rightOperand.setType(DataTypeDescriptor.getBuiltInDataTypeDescriptor( Types.TIME));
		}

		TypeId leftTypeId = leftOperand.getTypeId();
        TypeId rightTypeId = rightOperand.getTypeId();
        if( !(leftOperand.requiresTypeFromContext() || leftTypeId.isStringTypeId() || leftTypeId.getJDBCTypeId() == Types.DATE))
            throw StandardException.newException(SQLState.LANG_BINARY_OPERATOR_NOT_SUPPORTED, 
                                                 operator, leftTypeId.getSQLTypeName(), rightTypeId.getSQLTypeName());
        if( !(rightOperand.requiresTypeFromContext() || rightTypeId.isStringTypeId() || rightTypeId.getJDBCTypeId() == Types.TIME))
            throw StandardException.newException(SQLState.LANG_BINARY_OPERATOR_NOT_SUPPORTED, 
                                                 operator, leftTypeId.getSQLTypeName(), rightTypeId.getSQLTypeName());
        setType(DataTypeDescriptor.getBuiltInDataTypeDescriptor( Types.TIMESTAMP));
		return genSQLJavaSQLTree();
	} // end of bindExpression

    /**
	 * Do code generation for this binary operator.
	 *
	 * @param acb	The ExpressionClassBuilder for the class we're generating
	 * @param mb	The method the code to place the code
	 *
	 *
	 * @exception StandardException		Thrown on error
	 */
    @Override
    void generateExpression(ExpressionClassBuilder acb, MethodBuilder mb)
		throws StandardException
	{
        acb.pushDataValueFactory(mb);
		leftOperand.generateExpression(acb, mb);
        mb.cast( ClassName.DataValueDescriptor);
		rightOperand.generateExpression(acb, mb);
        mb.cast( ClassName.DataValueDescriptor);
        mb.callMethod( VMOpcode.INVOKEINTERFACE, null, methodName, ClassName.DateTimeDataValue, 2);
    } // end of generateExpression
}
