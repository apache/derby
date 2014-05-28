/*

   Derby - Class org.apache.derby.impl.sql.compile.CachedValueNode

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
import java.util.List;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.ClassName;
import org.apache.derby.iapi.services.compiler.LocalField;
import org.apache.derby.iapi.services.compiler.MethodBuilder;
import org.apache.derby.iapi.sql.compile.Visitor;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.util.JBitSet;

/**
 * <p>
 * A wrapper class for a {@code ValueNode} that is referenced multiple
 * places in the abstract syntax tree, but should only be evaluated once.
 * This node will cache the return value the first time the expression
 * is evaluated, and simply return the cached value the next time.
 * </p>
 *
 * <p>For example, an expression such as</p>
 *
 * <pre>
 *   CASE expr1
 *     WHEN expr2 THEN expr3
 *     WHEN expr4 THEN expr5
 *   END
 * </pre>
 *
 * <p>is rewritten by the parser to</p>
 *
 * <pre>
 *   CASE
 *     WHEN expr1 = expr2 THEN expr3
 *     WHEN expr1 = expr4 THEN expr5
 *   END
 * </pre>
 *
 * <p>
 * In this case, we want {@code expr1} to be evaluated only once, even
 * though it's referenced twice in the rewritten tree. By wrapping the
 * {@code ValueNode} for {@code expr1} in a {@code CachedValueNode}, we
 * make sure {@code expr1} is only evaluated once, and the second reference
 * to it will use the cached return value from the first evaluation.
 * </p>
 */
class CachedValueNode extends ValueNode {

    /** The node representing the expression whose value should be cached. */
    private ValueNode value;

    /** The field in the {@code Activation} class where the value is cached. */
    private LocalField field;

    /**
     * Wrap the value in a {@code CachedValueNode}.
     * @param value the value to wrap
     */
    CachedValueNode(ValueNode value) {
        super(value.getContextManager());
        this.value = value;
    }

    /**
     * Generate code that returns the value that this expression evaluates
     * to. For the first occurrence of this node in the abstract syntax
     * tree, this method generates the code needed to evaluate the expression.
     * Additionally, it stores the returned value in a field in the {@code
     * Activation} class. For subsequent occurrences of this node, it will
     * simply generate code that reads the value of that field, so that
     * reevaluation is not performed.
     *
     * @param acb the class builder
     * @param mb  the method builder
     * @throws StandardException if an error occurs
     */
    @Override
    void generateExpression(ExpressionClassBuilder acb, MethodBuilder mb)
            throws StandardException {
        if (field == null) {
            // This is the first occurrence of the node, so we generate
            // code for evaluating the expression and storing the returned
            // value in a field.
            field = acb.newFieldDeclaration(
                    Modifier.PRIVATE, ClassName.DataValueDescriptor);
            value.generateExpression(acb, mb);
            mb.putField(field);
        } else {
            // This is not the first occurrence of the node, so we can
            // simply read the cached value from the field instead of
            // reevaluating the expression.
            mb.getField(field);
        }
    }

    /**
     * Generate code that clears the field that holds the cached value, so
     * that it can be garbage collected.
     *
     * @param mb the method builder that should have the code
     */
    void generateClearField(MethodBuilder mb) {
        if (field != null) {
            mb.pushNull(ClassName.DataValueDescriptor);
            mb.setField(field);
        }
    }

    // Overrides for various ValueNode methods. Simply forward the calls
    // to the wrapped ValueNode.

    @Override
    ValueNode bindExpression(FromList fromList, SubqueryList subqueryList,
                                 List<AggregateNode> aggregates)
            throws StandardException {
        value = value.bindExpression(fromList, subqueryList, aggregates);
        return this;
    }

    @Override
    ValueNode preprocess(int numTables,
                         FromList outerFromList,
                         SubqueryList outerSubqueryList,
                         PredicateList outerPredicateList)
            throws StandardException {
        value = value.preprocess(numTables, outerFromList,
                                 outerSubqueryList, outerPredicateList);
        return this;
    }

    @Override
    boolean isEquivalent(ValueNode other) throws StandardException {
        if (other instanceof CachedValueNode) {
            CachedValueNode that = (CachedValueNode) other;
            return this.value.isEquivalent(that.value);
        } else {
            return false;
        }
    }

    @Override
    void acceptChildren(Visitor v) throws StandardException {
        super.acceptChildren(v);

        if (value != null) {
            value = (ValueNode) value.accept(v);
        }
    }

    @Override
    DataTypeDescriptor getTypeServices() {
        return value.getTypeServices();
    }

    @Override
    void setType(DataTypeDescriptor dtd) throws StandardException {
        value.setType(dtd);
    }

    @Override
    boolean requiresTypeFromContext() {
        return value.requiresTypeFromContext();
    }

    @Override
    ValueNode remapColumnReferencesToExpressions() throws StandardException {
        value = value.remapColumnReferencesToExpressions();
        return this;
    }

    @Override
    boolean categorize(JBitSet referencedTabs, boolean simplePredsOnly)
            throws StandardException {
        return value.categorize(referencedTabs, simplePredsOnly);
    }
}
