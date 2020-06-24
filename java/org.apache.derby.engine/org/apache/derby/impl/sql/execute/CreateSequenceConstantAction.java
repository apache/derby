/*

   Derby - Class org.apache.derby.impl.sql.execute.CreateSequenceConstantAction

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
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;
import org.apache.derby.iapi.sql.dictionary.SequenceDescriptor;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.DataDescriptorGenerator;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.shared.common.reference.SQLState;

/**
 * This class performs actions that are ALWAYS performed for a
 * CREATE SEQUENCE statement at execution time.
 * These SQL objects are stored in the SYS.SYSSEQUENCES table.
 */
class CreateSequenceConstantAction extends DDLConstantAction {

    private String _sequenceName;
    private String _schemaName;
    private DataTypeDescriptor _dataType;
    private long _initialValue;
    private long _stepValue;
    private long _maxValue;
    private long _minValue;
    private boolean _cycle;

    // CONSTRUCTORS
    /**
     * Make the ConstantAction for a CREATE SEQUENCE statement.
     * When executed, will create a sequence by the given name.
     *
     * @param sequenceName The name of the sequence being created
     * @param dataType Exact numeric type of the new sequence
     * @param initialValue Starting value
     * @param stepValue Increment amount
     * @param maxValue Largest value returned by the sequence generator
     * @param minValue Smallest value returned by the sequence generator
     * @param cycle True if the generator should wrap around, false otherwise
     */
    public CreateSequenceConstantAction
    (
            String schemaName,
            String sequenceName,
            DataTypeDescriptor dataType,
            long initialValue,
            long stepValue,
            long maxValue,
            long minValue,
            boolean cycle
    )
    {
        this._schemaName = schemaName;
        this._sequenceName = sequenceName;
        this._dataType = dataType;
        this._initialValue = initialValue;
        this._stepValue = stepValue;
        this._maxValue = maxValue;
        this._minValue = minValue;
        this._cycle = cycle;
    }

    // INTERFACE METHODS

    /**
     * This is the guts of the Execution-time logic for CREATE SEQUENCE.
     *
     * @throws org.apache.derby.shared.common.error.StandardException
     *          Thrown on failure
     * @see org.apache.derby.iapi.sql.execute.ConstantAction#executeConstantAction
     */
    public void executeConstantAction(Activation activation)
            throws StandardException {
        SchemaDescriptor schemaDescriptor;
        LanguageConnectionContext lcc =
                activation.getLanguageConnectionContext();
        DataDictionary dd = lcc.getDataDictionary();
        TransactionController tc = lcc.getTransactionExecute();
        DataDescriptorGenerator ddg = dd.getDataDescriptorGenerator();

        dd.startWriting(lcc);

        schemaDescriptor = DDLConstantAction.getSchemaDescriptorForCreate(dd, activation, _schemaName);

        //
        // Check if this sequence already exists. If it does, throw.
        //
        SequenceDescriptor seqDef = dd.getSequenceDescriptor(schemaDescriptor, _sequenceName);

        if (seqDef != null) {
            throw StandardException.
                    newException(SQLState.LANG_OBJECT_ALREADY_EXISTS,
                            seqDef.getDescriptorType(), _sequenceName);
        }

        seqDef = ddg.newSequenceDescriptor(
                schemaDescriptor,
                dd.getUUIDFactory().createUUID(),
                _sequenceName,
                _dataType,
//IC see: https://issues.apache.org/jira/browse/DERBY-6856
                _initialValue,   // current value
                _initialValue,
                _minValue,
                _maxValue,
                _stepValue,
                _cycle);        // whether the sequence can wrap-around

        dd.addDescriptor(seqDef,
                null,  // parent
                DataDictionary.SYSSEQUENCES_CATALOG_NUM,
                false, // duplicatesAllowed
                tc);
    }

    // OBJECT SHADOWS

    public String toString() {
        // Do not put this under SanityManager.DEBUG - it is needed for
        // error reporting.
        return "CREATE SEQUENCE " + _sequenceName;
    }
}
