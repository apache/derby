/*

   Derby - Class org.apache.derby.impl.sql.compile.OffsetOrderVisitor

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

import java.util.Comparator;
import java.util.SortedSet;
import java.util.TreeSet;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.compile.Visitable;
import org.apache.derby.iapi.sql.compile.Visitor;
import org.apache.derby.shared.common.sanity.SanityManager;

/**
 * Get all nodes of a certain type in a query tree, and return them in
 * the order in which they appear in the original SQL text. This visitor
 * is useful when rewriting SQL queries by replacing certain tokens in
 * the original query.
 *
 * @param <T> the type of nodes to collect
 */
class OffsetOrderVisitor<T extends QueryTreeNode> implements Visitor {

    /** Comparator that orders nodes by ascending begin offset. */
    private static final Comparator<QueryTreeNode>
            COMPARATOR = new Comparator<QueryTreeNode>() {
        public int compare(QueryTreeNode node1, QueryTreeNode node2) {
            return node1.getBeginOffset() - node2.getBeginOffset();
        }
    };

    private final Class<T> nodeClass;
    private final TreeSet<T> nodes = new TreeSet<T>(COMPARATOR);
    private final int lowOffset;
    private final int highOffset;

    /**
     * Create a new {@code OffsetOrderVisitor} that collects nodes of the
     * specified type. The nodes must have begin offset and end offset in
     * the range given by the {@code low} and {@code high} parameters.
     *
     * @param nodeClass the type of nodes to collect
     * @param low the lowest begin offset to accept (inclusive)
     * @param high the highest end offset to accept (exclusive)
     */
    OffsetOrderVisitor(Class<T> nodeClass, int low, int high) {
        this.nodeClass = nodeClass;
        this.lowOffset = low;
        this.highOffset = high;

        if (SanityManager.DEBUG) {
            // We should only collect nodes with non-negative offset. Nodes
            // with negative offset are synthetic and did not exist as tokens
            // in the original query text.
            SanityManager.ASSERT(lowOffset >= 0 && highOffset >= 0,
                                 "offsets should be non-negative");
        }
    }

    @Override
    public Visitable visit(Visitable node) throws StandardException {
        if (nodeClass.isInstance(node)) {
            T qtn = nodeClass.cast(node);
            if (qtn.getBeginOffset() >= lowOffset
                    && qtn.getEndOffset() < highOffset) {
                nodes.add(qtn);
            }
        }

        return node;
    }

    @Override
    public boolean visitChildrenFirst(Visitable node) {
        return false;
    }

    @Override
    public boolean stopTraversal() {
        return false;
    }

    @Override
    public boolean skipChildren(Visitable node) throws StandardException {
        return false;
    }

    SortedSet<T> getNodes() {
        return nodes;
    }
}
