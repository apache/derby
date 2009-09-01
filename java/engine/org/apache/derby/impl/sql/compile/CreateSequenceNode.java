/*

   Derby - Class org.apache.derby.impl.sql.compile.CreateSequenceNode

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
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.sql.compile.CompilerContext;
import org.apache.derby.iapi.sql.execute.ConstantAction;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;

/**
 * A CreateSequenceNode is the root of a QueryTree that
 * represents a CREATE SEQUENCE statement.
 */

public class CreateSequenceNode extends DDLStatementNode {
    private TableName sequenceName;

    public static final int SEQUENCE_ELEMENT_COUNT = 1;

    /**
     * Initializer for a CreateSequenceNode
     *
     * @param sequenceName The name of the new sequence
     * @throws org.apache.derby.iapi.error.StandardException
     *          Thrown on error
     */
    public void init(Object sequenceName) throws StandardException {
        this.sequenceName = (TableName) sequenceName;
        initAndCheck(sequenceName);

        // automcatically create the schema if it doesn't exist
        implicitCreateSchema = true;
    }

    /**
     * Convert this object to a String.  See comments in QueryTreeNode.java
     * for how this should be done for tree printing.
     *
     * @return This object as a String
     */

    public String toString() {
        if (SanityManager.DEBUG) {
            return super.toString() +
                    "sequenceName: " + "\n" + sequenceName + "\n";
        } else {
            return "";
        }
    }

    /**
     * Bind this CreateSequenceNode.
     * The main objectives of this method are to resolve the schema name, determine privilege checks,
     * and vet the variables in the CREATE SEQUENCE statement.
     */
    public void bindStatement() throws StandardException {
        CompilerContext cc = getCompilerContext();

        // implicitly create the schema if it does not exist.
        // this method also compiles permissions checks
        SchemaDescriptor sd = getSchemaDescriptor();

//        sequenceName.bind( getDataDictionary() );
        // set the default schema name if the user did not explicitly specify a schema
        if (sequenceName.getSchemaName() == null) {
            sequenceName.setSchemaName(sd.getSchemaName());
        }
    }

    public String statementToString() {
        return "CREATE SEQUENCE";
    }

    // We inherit the generate() method from DDLStatementNode.

    /**
     * Create the Constant information that will drive the guts of Execution.
     *
     * @throws org.apache.derby.iapi.error.StandardException
     *          Thrown on failure
     */
    public ConstantAction makeConstantAction() {
        return getGenericConstantActionFactory().
                getCreateSequenceConstantAction(sequenceName);
    }
}
