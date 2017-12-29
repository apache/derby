/*
   Derby - Class org.apache.derby.impl.sql.compile.RowNumberFunctionNode

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

import java.sql.Types;
import java.util.List;
import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.types.TypeId;

/**
 * Class that represents a call to the ROW_NUMBER() window function.
 */
public final class RowNumberFunctionNode extends WindowFunctionNode
{
    /**
     *
     * @param op operand (null for now)
     * @param w The window definition or reference
     */
    RowNumberFunctionNode(ValueNode op, WindowNode w, ContextManager cm)
            throws StandardException {
        super(op, "ROW_NUMBER", w, cm);
        setType( TypeId.getBuiltInTypeId( Types.BIGINT ),
                 TypeId.LONGINT_PRECISION,
                 TypeId.LONGINT_SCALE,
                 false,
                 TypeId.LONGINT_MAXWIDTH);
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
        super.bindExpression(fromList, subqueryList, aggregates);
        return this;
    }
}
