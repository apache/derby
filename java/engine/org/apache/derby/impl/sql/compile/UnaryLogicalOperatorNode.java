/*

   Derby - Class org.apache.derby.impl.sql.compile.UnaryLogicalOperatorNode

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
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.TypeId;

public abstract class UnaryLogicalOperatorNode extends UnaryOperatorNode
{
    UnaryLogicalOperatorNode(ValueNode operand,
            String methodName,
            ContextManager cm) throws StandardException {
        super(operand, methodName, methodName, cm);
    }

    /**
	 * Bind this logical operator.  All that has to be done for binding
	 * a logical operator is to bind the operand, check that the operand
	 * is SQLBoolean, and set the result type to SQLBoolean.
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
        bindOperand(fromList, subqueryList, aggregates);

		/*
		** Logical operators work only on booleans.  If the operand 
		** is not boolean, throw an exception.
		**
		** For now, this exception will never happen, because the grammar
		** does not allow arbitrary expressions with NOT.  But when
		** we start allowing generalized boolean expressions, we will modify
		** the grammar, so this test will become useful.
		*/

		if ( ! operand.getTypeServices().getTypeId().isBooleanTypeId())
		{
			throw StandardException.newException(SQLState.LANG_UNARY_LOGICAL_NON_BOOLEAN);
		}

		/* Set the type info */
		setFullTypeInfo();

		return this;
	}
		
	/**
	 * Set all of the type info (nullability and DataTypeServices) for
	 * this node.  Extracts out tasks that must be done by both bind()
	 * and post-bind() AndNode generation.
	 *
	 * @exception StandardException		Thrown on error
	 */
	protected void setFullTypeInfo()
		throws StandardException
	{
		boolean nullableResult;

		/*
		** Set the result type of this comparison operator based on the
		** operands.  The result type is always SQLBoolean - the only question
		** is whether it is nullable or not.  If either of the operands is
		** nullable, the result of the comparison must be nullable, too, so
		** we can represent the unknown truth value.
		*/
		nullableResult = operand.getTypeServices().isNullable();
		setType(new DataTypeDescriptor(TypeId.BOOLEAN_ID, nullableResult));
	}
}
