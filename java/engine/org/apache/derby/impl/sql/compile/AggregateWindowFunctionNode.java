/*
   Derby - Class org.apache.derby.impl.sql.compile.AggregateWindowFunctionNode

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

import java.util.List;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derby.shared.common.reference.SQLState;
import org.apache.derby.iapi.services.context.ContextManager;


/**
 * Represents aggregate function calls on a window
 */
final class AggregateWindowFunctionNode extends WindowFunctionNode
{

    private AggregateNode aggregateFunction;

    AggregateWindowFunctionNode(
            WindowNode w,
            AggregateNode agg,
            ContextManager cm) throws StandardException {

        super(null, "?", w, cm);
        aggregateFunction = agg;

        throw StandardException.newException(
            SQLState.NOT_IMPLEMENTED,
            "WINDOW/" + aggregateFunction.getAggregateName());
    }

    /**
     * ValueNode override.
     * @see ValueNode#bindExpression
     */
    @Override
    ValueNode bindExpression(
        FromList fromList, SubqueryList subqueryList, List<AggregateNode> aggregates)
            throws StandardException
    {
        aggregateFunction.bindExpression(
            fromList, subqueryList, aggregates);
        return this;
    }



    /**
     * QueryTreeNode override. Prints the sub-nodes of this object.
     * @see QueryTreeNode#printSubNodes
     *
     * @param depth     The depth of this node in the tree
     */
    @Override
    public void printSubNodes(int depth)
    {
        if (SanityManager.DEBUG)
        {
            super.printSubNodes(depth);

            printLabel(depth, "aggregate: ");
            aggregateFunction.treePrint(depth + 1);
        }
    }
}
