/*

   Derby - Class org.apache.derby.impl.sql.compile.ReplaceNodeVisitor

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
 * Replace all occurrences of a specific node with another node.
 */
class ReplaceNodeVisitor implements Visitor {

    private final Visitable nodeToReplace;
    private final Visitable replacement;

    ReplaceNodeVisitor(Visitable nodeToReplace, Visitable replacement) {
        this.nodeToReplace = nodeToReplace;
        this.replacement = replacement;
    }

    @Override
    public Visitable visit(Visitable node) throws StandardException {
        return (node == nodeToReplace) ? replacement : node;
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
}
