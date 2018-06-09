/*

   Derby - Class org.apache.derby.impl.sql.compile.AndNoShortCircuitNode

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

package org.apache.derby.impl.sql.compile;

import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.shared.common.reference.ClassName;
import org.apache.derby.iapi.services.classfile.VMOpcode;
import org.apache.derby.iapi.services.compiler.MethodBuilder;
import org.apache.derby.iapi.services.context.ContextManager;

/**
 * Used for deferrable CHECK constraint. When we evaluate check constraints for
 * a row where at least one constraint is deferrable, we need to know exactly
 * which set of constraints violated the checks.  The normal evaluation of
 * check constraints is generated as one big (NOT c1) AND (NOT c2) AND ...  AND
 * (NOT cn) using short-circuited (McCarthy) boolean evaluation.
 * <p>
 * This kind of evaluation of the expression can only tell us the first failing
 * constraint, so we use full evaluation instead, as embodied in this node.
 * See also {@link
 * org.apache.derby.iapi.types.BooleanDataValue#throwExceptionIfImmediateAndFalse}.
 */
class AndNoShortCircuitNode extends AndNode
{
    /**
     * @param leftOperand The left operand of the AND
     * @param rightOperand The right operand of the AND
     * @param cm context manager
     * @throws StandardException standard error policy
     */
    AndNoShortCircuitNode(
            ValueNode leftOperand,
            ValueNode rightOperand,
            ContextManager cm) throws StandardException {
        super(leftOperand, rightOperand, "andnoshortcircuitnode", cm);
    }

    /**
     * Generate code for no short-circuiting AND operator. Used to evaluate
     * check constraints where at least one is deferrable, since we need to
     * know exactly which constraint(s) violated the checks.
     * @throws StandardException standard error policy
     */
    @Override
    void generateExpression(ExpressionClassBuilder acb, MethodBuilder mb)
        throws StandardException
    {
        /*
        ** This generates the following code:
        **
        **   <leftOperand>.<and/or>(<rightOperand>)
        **/
        leftOperand.generateExpression(acb, mb);
        // stack - left
        mb.upCast(ClassName.BooleanDataValue);

        rightOperand.generateExpression(acb, mb);
        mb.upCast(ClassName.BooleanDataValue);

        // stack - left, right
        mb.callMethod(VMOpcode.INVOKEINTERFACE,
                      (String) null,
                      "and",
                      ClassName.BooleanDataValue,
                      1);
        // stack - result
    }
}
