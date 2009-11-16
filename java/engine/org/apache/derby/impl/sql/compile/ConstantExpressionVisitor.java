/*

   Derby - Class org.apache.derby.impl.sql.compile.ConstantExpressionVisitor

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

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.compile.Visitable;
import org.apache.derby.iapi.sql.compile.Visitor;

/**
 * <p>
 * This visitor replaces a {@code ValueNode} with a node representing a
 * constant value, if the {@code ValueNode} is known to always evaluate to the
 * same value. It may for instance replace a sub-tree representing {@code 1=1}
 * with a constant {@code TRUE}.
 * </p>
 *
 * <p>
 * The actual evaluation of the {@code ValueNode}s is performed by invoking
 * {@link ValueNode#evaluateConstantExpressions()} on every {@code ValueNode}
 * in the query tree.
 * </p>
 *
 * <p>
 * In contrast to most other visitors, this visitor walks the tree bottom-up.
 * Top-down processing of the tree would only evaluate constant expressions
 * at the leaf level, so for instance {@code (1=1)=(1=2)} would only be
 * simplified to {@code TRUE=FALSE}. With bottom-up processing, the top-level
 * = node will be processed after the leaves, and it sees the intermediate
 * tree {@code TRUE=FALSE} which it is able to transform into the even simpler
 * tree {@code FALSE}.
 * </p>
 */
class ConstantExpressionVisitor implements Visitor {

    /**
     * Visit the node and call {@code evaluateConstantExpressions()} if it
     * is a {@code ValueNode}.
     *
     * @see ValueNode#evaluateConstantExpressions()
     */
    public Visitable visit(Visitable node) throws StandardException {
        if (node instanceof ValueNode) {
            node = ((ValueNode) node).evaluateConstantExpressions();
        }
        return node;
    }

    /**
     * {@inheritDoc}
     * @return {@code false}, since the entire tree should be visited
     */
    public boolean stopTraversal() {
        return false;
    }

    /**
     * {@inheritDoc}
     * @return {@code false}, since the entire tree should be visited
     */
    public boolean skipChildren(Visitable node) {
        return false;
    }

    /**
     * {@inheritDoc}
     * @return {@code true}, since the tree should be walked bottom-up
     */
    public boolean visitChildrenFirst(Visitable node) {
        return true;
    }

}
