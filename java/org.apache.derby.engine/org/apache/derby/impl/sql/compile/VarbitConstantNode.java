/*

   Derby - Class org.apache.derby.impl.sql.compile.VarbitConstantNode

//IC see: https://issues.apache.org/jira/browse/DERBY-1377
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
import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derby.iapi.types.TypeId;

public final class VarbitConstantNode extends BitConstantNode
{
    // Allowed kinds
//IC see: https://issues.apache.org/jira/browse/DERBY-673
    final static int K_VAR = 0;
    final static int K_LONGVAR = 1;
    final static int K_BLOB = 2;

    /**
     * This class is used to hold logically different objects for
     * space efficiency. {@code kind} represents the logical object
     * type. See also {@link ValueNode#isSameNodeKind}.
     */
    final int kind;

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

        switch (t.getJDBCTypeId()) {
            case Types.VARBINARY:
//IC see: https://issues.apache.org/jira/browse/DERBY-673
                kind = K_VAR;
                break;
            case Types.LONGVARBINARY:
                kind = K_LONGVAR;
                break;
            case Types.BLOB:
                kind = K_BLOB;
                break;
            default:
                if (SanityManager.DEBUG) {
                    SanityManager.NOTREACHED();
                }
                kind = -1;
        }
    }

    VarbitConstantNode(
            String hexValue,
            int bitLength,
            ContextManager cm) throws StandardException {
        super(hexValue, bitLength, cm);
//IC see: https://issues.apache.org/jira/browse/DERBY-673
        kind = K_VAR;
    }

    @Override
    boolean isSameNodeKind(ValueNode o) {
        return super.isSameNodeKind(o) && ((VarbitConstantNode)o).kind == kind;
    }
}
