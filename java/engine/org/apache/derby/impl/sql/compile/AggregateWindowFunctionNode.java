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

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.types.TypeId;
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.reference.SQLState;

import java.sql.Types;
import java.util.Vector;

/**
 * Represents aggregate function calls on a window
 */
public final class AggregateWindowFunctionNode extends WindowFunctionNode
{

    private AggregateNode aggregateFunction;

    /**
     * Initializer. QueryTreeNode override.
     *
     * @param arg1 The window definition or reference
     * @param arg2 aggregate function node
     *
     * @exception StandardException
     */
    public void init(Object arg1, Object arg2)
        throws StandardException
    {
        super.init(null, "?", arg1);
        aggregateFunction = (AggregateNode)arg2;

        throw StandardException.newException(
            SQLState.NOT_IMPLEMENTED,
            "WINDOW/" + aggregateFunction.getAggregateName());
    }


    /**
     * ValueNode override.
     * @see ValueNode#bindExpression
     */
    public ValueNode bindExpression(
                    FromList            fromList,
                    SubqueryList        subqueryList,
                    Vector              aggregateVector)
            throws StandardException
    {
        aggregateFunction.bindExpression(
            fromList, subqueryList, aggregateVector);
        return this;
    }



    /**
     * QueryTreeNode override. Prints the sub-nodes of this object.
     * @see QueryTreeNode#printSubNodes
     *
     * @param depth     The depth of this node in the tree
     */

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
