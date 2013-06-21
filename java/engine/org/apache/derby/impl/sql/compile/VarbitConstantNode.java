/*

   Derby - Class org.apache.derby.impl.sql.compile.VarbitConstantNode

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

import java.sql.Types;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.sql.compile.C_NodeTypes;
import org.apache.derby.iapi.types.TypeId;

public final class VarbitConstantNode extends BitConstantNode
{
    /**
     * Construct constant node for one of VARBINARY, LONG VARBINARY and
     * BLOB types.
     * @param t the type for which we want a constant node
     * @param cm context manager
     * @throws StandardException
     */
    VarbitConstantNode(TypeId t, ContextManager cm)
            throws StandardException {
        super(t, cm);

        int nodeType = 0;
        switch (t.getJDBCTypeId()) {
            case Types.VARBINARY:
                nodeType = C_NodeTypes.VARBIT_CONSTANT_NODE;
                break;
            case Types.LONGVARBINARY:
                nodeType = C_NodeTypes.LONGVARBIT_CONSTANT_NODE;
                break;
            case Types.BLOB:
                nodeType = C_NodeTypes.BLOB_CONSTANT_NODE;
                break;
            default:
                if (SanityManager.DEBUG) {
                    SanityManager.NOTREACHED();
                }
        }

        setNodeType(nodeType);
    }

    VarbitConstantNode(
            String hexValue,
            int bitLength,
            ContextManager cm) throws StandardException {
        super(hexValue, bitLength, cm);
        setNodeType(C_NodeTypes.VARBIT_CONSTANT_NODE);
    }
}
