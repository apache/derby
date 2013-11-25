/*

   Derby - Class org.apache.derby.impl.sql.compile.SetConstraintsNode

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

package     org.apache.derby.impl.sql.compile;

import java.util.List;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.execute.ConstantAction;

/**
 * A SetConstraintsNode is the root of a QueryTree that represents a
 * SET CONSTRAINTS statement.
 */

class SetConstraintsNode extends MiscellaneousStatementNode
{
    /**
     * List of strings representing the constraints we want to
     * set. If empty, this means ALL.
     */
    final private List<TableName> constraints;

    /**
     * Encodes IMMEDIATE (false), DEFERRED (true)
     */
    final private boolean deferred;

    /**
     *
     * @param constraints List of strings representing the constraints
     *                    we want to set (empty means ALL).
     * @param deferred    Encodes IMMEDIATE ({@code false}) or DEFERRED
     *                    ({@code true})
     * @param cm          The context manager
     * @throws StandardException
     */
    SetConstraintsNode(
            List<TableName> constraints,
            boolean deferred,
            ContextManager cm) {
        super(cm);
        this.constraints = constraints;
        this.deferred = deferred;
    }

    /**
     * Convert this object to a String.  See comments in QueryTreeNode.java
     * for how this should be done for tree printing.
     *
     * @return  This object as a String
     */
    @Override
    public String toString()
    {
        if (SanityManager.DEBUG) {
            return super.toString() + formatList(constraints) + ":" +
                    (deferred ? " DEFERRED" : " IMMEDIATE")  + "\n";
        } else {
            return "";
        }
    }

    String formatList(List<TableName> constraints) {
        StringBuilder sb = new StringBuilder();

        for (TableName tn : constraints) {
            sb.append(tn);
            sb.append(", ");
        }

        return sb.substring(0, Math.max(0, sb.length() - 2));
    }

    public String statementToString()
    {
        return "SET CONSTRAINTS";
    }

    /**
     * Create the Constant information that will drive the guts of
     * Execution.
     *
     * @exception StandardException         Thrown on failure
     */
    @Override
    public ConstantAction   makeConstantAction() throws StandardException
    {
        return getGenericConstantActionFactory().
            getSetConstraintsConstantAction(constraints, deferred);
    }

    @Override
    public void bindStatement() throws StandardException
    {
        final DataDictionary dd = getDataDictionary();

        if (constraints != null) {
            for (TableName c : constraints) {
                c.bind(dd);
            }
        }
    }

}
