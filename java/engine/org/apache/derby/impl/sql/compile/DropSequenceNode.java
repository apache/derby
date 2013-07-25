/*

   Derby - Class org.apache.derby.impl.sql.compile.DropSequenceNode

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

import org.apache.derby.iapi.sql.execute.ConstantAction;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;
import org.apache.derby.iapi.sql.dictionary.SequenceDescriptor;

/**
 * A DropSequenceNode  represents a DROP SEQUENCE statement.
 */

class DropSequenceNode extends DDLStatementNode {
    private TableName dropItem;

    /**
     * Constructor for a DropSequenceNode
     *
     * @param dropSequenceName The name of the sequence being dropped
     * @param cm               The context manager
     * @throws StandardException
     */
    DropSequenceNode(TableName dropSequenceName, ContextManager cm) {
        super(dropSequenceName, cm);
        dropItem = dropSequenceName;
    }

    public String statementToString() {
        return "DROP ".concat(dropItem.getTableName());
    }

    /**
     * Bind this DropSequenceNode.
     *
     * @throws StandardException Thrown on error
     */
    @Override
    public void bindStatement() throws StandardException {
        DataDictionary dataDictionary = getDataDictionary();
        String sequenceName = getRelativeName();

        SequenceDescriptor seqDesc = null;
        SchemaDescriptor sd = getSchemaDescriptor();

        if (sd.getUUID() != null) {
            seqDesc = dataDictionary.getSequenceDescriptor
                    (sd, sequenceName);
        }
        if (seqDesc == null) {
            throw StandardException.newException(SQLState.LANG_OBJECT_DOES_NOT_EXIST, statementToString(), sequenceName);
        }

        // Statement is dependent on the SequenceDescriptor
        getCompilerContext().createDependency(seqDesc);
    }

    // inherit generate() method from DDLStatementNode


    /**
     * Create the Constant information that will drive the guts of Execution.
     *
     * @throws StandardException Thrown on failure
     */
    @Override
    public ConstantAction makeConstantAction() throws StandardException {
        return getGenericConstantActionFactory().getDropSequenceConstantAction(getSchemaDescriptor(), getRelativeName());
	}

}
