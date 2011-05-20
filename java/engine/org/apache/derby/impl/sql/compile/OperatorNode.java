/*

   Derby - Class org.apache.derby.impl.sql.compile.OperatorNode

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

import java.lang.reflect.Modifier;
import org.apache.derby.iapi.services.classfile.VMOpcode;
import org.apache.derby.iapi.services.compiler.LocalField;
import org.apache.derby.iapi.services.compiler.MethodBuilder;
import org.apache.derby.iapi.types.SqlXmlUtil;

/**
 * Abstract base-class for the various operator nodes: UnaryOperatorNode,
 * BinaryOperatorNode and TernarnyOperatorNode.
 */
abstract class OperatorNode extends ValueNode {

    /**
     * <p>
     * Generate code that pushes an SqlXmlUtil instance onto the stack. The
     * instance will be created and cached in the activation the first time
     * the code is executed, so that we don't need to create a new instance
     * for every row.
     * </p>
     *
     * <p>
     * If the {@code xmlQuery} parameter is non-null, there will also be code
     * that compiles the query when the SqlXmlUtil instance is created.
     * </p>
     *
     * @param acb builder for the class in which the generated code lives
     * @param mb builder for the method that implements this operator
     * @param xmlQuery the XML query to be executed by the operator, or
     * {@code null} if this isn't an XMLEXISTS or XMLQUERY operator
     * @param xmlOpName the name of the operator (ignored if {@code xmlQuery}
     * is {@code null})
     */
    static void pushSqlXmlUtil(
            ExpressionClassBuilder acb, MethodBuilder mb,
            String xmlQuery, String xmlOpName) {

        // Create a field in which the instance can be cached.
        LocalField sqlXmlUtil = acb.newFieldDeclaration(
                Modifier.PRIVATE, SqlXmlUtil.class.getName());

        // Read the cached value.
        mb.getField(sqlXmlUtil);

        // Check if the cached value is null. If it is, create a new instance.
        // Otherwise, we're happy with the stack as it is (the cached instance
        // will be on top of it), and nothing more is needed.
        mb.dup();
        mb.conditionalIfNull();

        // The cached value is null. Pop it from the stack so that we can put
        // a fresh instance there in its place.
        mb.pop();

        // Create a new instance and cache it in the field. Its value will be
        // on the top of the stack after this sequence.
        mb.pushNewStart(SqlXmlUtil.class.getName());
        mb.pushNewComplete(0);
        mb.putField(sqlXmlUtil);

        // If a query is specified, compile it.
        if (xmlQuery != null) {
            mb.dup();
            mb.push(xmlQuery);
            mb.push(xmlOpName);
            mb.callMethod(
                    VMOpcode.INVOKEVIRTUAL, SqlXmlUtil.class.getName(),
                    "compileXQExpr", "void", 2);
        }

        mb.completeConditional();
    }
}
