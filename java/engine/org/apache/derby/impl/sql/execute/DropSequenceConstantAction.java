/*

   Derby - Class org.apache.derby.impl.sql.execute.DropSequenceConstantAction

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

package org.apache.derby.impl.sql.execute;

import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.depend.DependencyManager;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.SequenceDescriptor;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.shared.common.reference.SQLState;

/**
 * This class  describes actions that are ALWAYS performed for a
 * DROP SEQUENCE Statement at Execution time.
 */

class DropSequenceConstantAction extends DDLConstantAction {


    private final String sequenceName;
    private final SchemaDescriptor schemaDescriptor;

    // CONSTRUCTORS

    /**
     * Make the ConstantAction for a DROP SEQUENCE statement.
     *
     * @param sequenceName sequence name to be dropped
     */
    DropSequenceConstantAction(SchemaDescriptor sd, String sequenceName) {
        this.sequenceName = sequenceName;
        this.schemaDescriptor = sd;
    }

    ///////////////////////////////////////////////
    //
    // OBJECT SHADOWS
    //
    ///////////////////////////////////////////////

    public String toString() {
        // Do not put this under SanityManager.DEBUG - it is needed for
        // error reporting.
        return "DROP SEQUENCE " + sequenceName;
    }

    // INTERFACE METHODS


    /**
     * This is the guts of the Execution-time logic for DROP SEQUENCE.
     *
     * @see org.apache.derby.iapi.sql.execute.ConstantAction#executeConstantAction
     */
    public void executeConstantAction(Activation activation)
            throws StandardException {
        LanguageConnectionContext lcc =
                activation.getLanguageConnectionContext();
        DataDictionary dd = lcc.getDataDictionary();
        TransactionController tc = lcc.getTransactionExecute();

        /*
        ** Inform the data dictionary that we are about to write to it.
        ** There are several calls to data dictionary "get" methods here
        ** that might be done in "read" mode in the data dictionary, but
        ** it seemed safer to do this whole operation in "write" mode.
        **
        ** We tell the data dictionary we're done writing at the end of
        ** the transaction.
        */
        dd.startWriting(lcc);
        dd.clearSequenceCaches();

        SequenceDescriptor sequenceDescriptor = dd.getSequenceDescriptor(schemaDescriptor, sequenceName);

        if (sequenceDescriptor == null) {

            throw StandardException.newException(SQLState.LANG_OBJECT_NOT_FOUND_DURING_EXECUTION, "SEQUENCE",
                    (schemaDescriptor.getObjectName() + "." + sequenceName));
        }

        sequenceDescriptor.drop(lcc);
    }
}
