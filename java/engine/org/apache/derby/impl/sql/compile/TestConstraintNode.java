/*

   Derby - Class org.apache.derby.impl.sql.compile.TestConstraintNode

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
import org.apache.derby.catalog.UUID;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.ClassName;
import org.apache.derby.iapi.services.classfile.VMOpcode;
import org.apache.derby.iapi.services.compiler.MethodBuilder;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.sql.dictionary.ConstraintDescriptor;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.TypeId;

/**
 * A TestConstraintNode is used to determine when a constraint
 * has been violated.
 *
 */

class TestConstraintNode extends UnaryLogicalOperatorNode
{
    private final String sqlState;
    private final String tableName;
    private final UUID cid;
    private final boolean deferrable;
    private final String constraintName;

    /**
     * @param booleanValue The operand of the constraint test
     * @param sqlState The SQLState of the exception to throw if the
    *              constraint has failed
     * @param tableName The name of the table that the constraint is on
     * @param cd The descriptor of the constraint being checked
     * @param cm context manager
     * @throws StandardException
     */
    TestConstraintNode(
            ValueNode booleanValue,
            String sqlState,
            String tableName,
            ConstraintDescriptor cd,
            ContextManager cm) throws StandardException {
        super(booleanValue,
                cd.deferrable() ?
                        "throwExceptionIfImmediateAndFalse" :
                        "throwExceptionIfFalse",
                cm);
        this.sqlState = sqlState;
        this.tableName = tableName;
        this.cid = cd.getUUID();
        this.deferrable = cd.deferrable();
        this.constraintName = cd.getConstraintName();
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
    ValueNode bindExpression(
        FromList fromList, SubqueryList subqueryList, List<AggregateNode> aggregates)
			throws StandardException
	{
        bindOperand(fromList, subqueryList, aggregates);

		/*
		** If the operand is not boolean, cast it.
		*/

		if (!operand.getTypeServices().getTypeId().isBooleanTypeId())
		{
            operand = new CastNode(
					operand,
					new DataTypeDescriptor(TypeId.BOOLEAN_ID, true),
					getContextManager());
			((CastNode) operand).bindCastNodeOnly();
		}

		/* Set the type info */
		setFullTypeInfo();

		return this;
	}

	/**
	 * Do code generation for the TestConstraint operator.
	 *
	 * @param acb	The ExpressionClassBuilder for the class we're generating
	 * @param mb	The method the expression will go into
	 *
	 *
	 * @exception StandardException		Thrown on error
	 */
    @Override
    void generateExpression(ExpressionClassBuilder acb, MethodBuilder mb)
									throws StandardException
	{

		/*
		** This generates the following code:
		**
		** operand.testConstraint(sqlState, tableName, constraintName)
		*/

		operand.generateExpression(acb, mb);

		mb.push(sqlState);
		mb.push(tableName);
        mb.push(constraintName);

        if (deferrable) {
            acb.pushThisAsActivation(mb); // arg 4
            mb.push(acb.addItem(cid)); // arg 5

            mb.callMethod(
                VMOpcode.INVOKEINTERFACE,
                ClassName.BooleanDataValue,
                "throwExceptionIfImmediateAndFalse",
                ClassName.BooleanDataValue,
                5);
        } else {
            mb.callMethod(VMOpcode.INVOKEINTERFACE, ClassName.BooleanDataValue,
                    "throwExceptionIfFalse", ClassName.BooleanDataValue, 3);
        }
	}
}
