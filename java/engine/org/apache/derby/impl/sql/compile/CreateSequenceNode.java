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
import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derby.iapi.sql.execute.ConstantAction;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.shared.common.reference.SQLState;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.sql.compile.Visitor;
import org.apache.derby.iapi.types.TypeId;


/**
 * A CreateSequenceNode is the root of a QueryTree that
 * represents a CREATE SEQUENCE statement.
 */

class CreateSequenceNode extends DDLStatementNode
{
    private TableName _sequenceName;
    private DataTypeDescriptor _dataType;
    private Long _initialValue;
    private Long _stepValue;
    private Long _maxValue;
    private Long _minValue;
    private boolean _cycle;

    public static final int SEQUENCE_ELEMENT_COUNT = 1;

    /**
     * Constructor for a CreateSequenceNode
     *
     * @param sequenceName The name of the new sequence
     * @param dataType Exact numeric type of the new sequence
     * @param initialValue Starting value
     * @param stepValue Increment amount
     * @param maxValue Largest value returned by the sequence generator
     * @param minValue Smallest value returned by the sequence generator
     * @param cycle True if the generator should wrap around, false otherwise
     * @param cm Context manager
     * @throws org.apache.derby.iapi.error.StandardException on error
     */
    CreateSequenceNode
        (
         TableName sequenceName,
         DataTypeDescriptor dataType,
         Long initialValue,
         Long stepValue,
         Long maxValue,
         Long minValue,
         boolean cycle,
         ContextManager cm
         ) throws StandardException {

        super(sequenceName, cm);
        this._sequenceName = sequenceName;

        if (dataType != null) {
            _dataType = dataType;
        } else {
            _dataType = DataTypeDescriptor.INTEGER;
        }

        _stepValue = (stepValue != null ? stepValue : Long.valueOf(1));

        if (_dataType.getTypeId().equals(TypeId.SMALLINT_ID)) {
            _minValue =
                (minValue != null ? minValue : Long.valueOf(Short.MIN_VALUE));
            _maxValue =
                (maxValue != null ? maxValue : Long.valueOf(Short.MAX_VALUE));
        } else if (_dataType.getTypeId().equals(TypeId.INTEGER_ID)) {
            _minValue =
                (minValue != null ? minValue : Long.valueOf(Integer.MIN_VALUE));
            _maxValue =
                (maxValue != null ? maxValue : Long.valueOf(Integer.MAX_VALUE));
        } else {
            // Could only be BIGINT
            _minValue = (minValue != null ? minValue : Long.MIN_VALUE);
            _maxValue = (maxValue != null ? maxValue : Long.MAX_VALUE);
        }

        if (initialValue != null) {
            _initialValue = initialValue;
        } else {
            if (_stepValue.longValue() > 0L) {
                _initialValue = _minValue;
            } else {
                _initialValue = _maxValue;
            }
        }
        _cycle = cycle;

        // automatically create the schema if it doesn't exist
        implicitCreateSchema = true;
    }

    /**
     * Convert this object to a String.  See comments in QueryTreeNode.java
     * for how this should be done for tree printing.
     *
     * @return This object as a String
     */
    @Override
    public String toString() {
        if (SanityManager.DEBUG) {
            return super.toString() +
                    "sequenceName: " + "\n" + _sequenceName + "\n";
        } else {
            return "";
        }
    }

    /**
     * Bind this CreateSequenceNode.
     * The main objectives of this method are to resolve the schema name, determine privilege checks,
     * and vet the variables in the CREATE SEQUENCE statement.
     */
    @Override
    public void bindStatement() throws StandardException {
        // implicitly create the schema if it does not exist.
        // this method also compiles permissions checks
        SchemaDescriptor sd = getSchemaDescriptor();

        // set the default schema name if the user did not explicitly specify a schema
        if (_sequenceName.getSchemaName() == null) {
            _sequenceName.setSchemaName(sd.getSchemaName());
        }

        if (_dataType.getTypeId().equals(TypeId.SMALLINT_ID)) {
            if (_minValue.longValue() < Short.MIN_VALUE || _minValue.longValue() >= Short.MAX_VALUE) {
                throw StandardException.newException(
                        SQLState.LANG_SEQ_ARG_OUT_OF_DATATYPE_RANGE,
                        "MINVALUE",
                        "SMALLINT",
                        Short.MIN_VALUE + "",
                        Short.MAX_VALUE + "");
            }
            if (_maxValue.longValue() <= Short.MIN_VALUE || _maxValue.longValue() > Short.MAX_VALUE) {
                throw StandardException.newException(
                        SQLState.LANG_SEQ_ARG_OUT_OF_DATATYPE_RANGE,
                        "MAXVALUE",
                        "SMALLINT",
                        Short.MIN_VALUE + "",
                        Short.MAX_VALUE + "");
            }
        } else if (_dataType.getTypeId().equals(TypeId.INTEGER_ID)) {
            if (_minValue.longValue() < Integer.MIN_VALUE || _minValue.longValue() >= Integer.MAX_VALUE) {
                throw StandardException.newException(
                        SQLState.LANG_SEQ_ARG_OUT_OF_DATATYPE_RANGE,
                        "MINVALUE",
                        "INTEGER",
                        Integer.MIN_VALUE + "",
                        Integer.MAX_VALUE + "");
            }
            if (_maxValue.longValue() <= Integer.MIN_VALUE || _maxValue.longValue() > Integer.MAX_VALUE) {
                throw StandardException.newException(
                        SQLState.LANG_SEQ_ARG_OUT_OF_DATATYPE_RANGE,
                        "MAXVALUE",
                        "INTEGER",
                        Integer.MIN_VALUE + "",
                        Integer.MAX_VALUE + "");
            }
        } else {
            // BIGINT
            if (_minValue.longValue() < Long.MIN_VALUE || _minValue.longValue() >= Long.MAX_VALUE) {
                throw StandardException.newException(
                        SQLState.LANG_SEQ_ARG_OUT_OF_DATATYPE_RANGE,
                        "MINVALUE",
                        "BIGINT",
                        Long.MIN_VALUE + "",
                        Long.MAX_VALUE + "");
            }
            if (_maxValue.longValue() <= Long.MIN_VALUE || _maxValue.longValue() > Long.MAX_VALUE) {
                throw StandardException.newException(
                        SQLState.LANG_SEQ_ARG_OUT_OF_DATATYPE_RANGE,
                        "MAXVALUE",
                        "BIGINT",
                        Long.MIN_VALUE + "",
                        Long.MAX_VALUE + "");
            }
        }

        if (_minValue.longValue() >= _maxValue.longValue()) {
            throw StandardException.newException(
                    SQLState.LANG_SEQ_MIN_EXCEEDS_MAX,
                    _minValue.toString(),
                    _maxValue.toString());
        }

        if (_initialValue.longValue() < _minValue.longValue() || _initialValue.longValue() > _maxValue.longValue()) {
             throw StandardException.newException(
                     SQLState.LANG_SEQ_INVALID_START,
                     _initialValue.toString(),
                     _minValue.toString(),
                     _maxValue.toString());
        }       

        if (_stepValue.longValue() == 0L) {
            throw StandardException.newException(
                    SQLState.LANG_SEQ_INCREMENT_ZERO);
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
    @Override
    public ConstantAction makeConstantAction() {
             return getGenericConstantActionFactory().
                getCreateSequenceConstantAction(
                        _sequenceName,
                        _dataType,
                        _initialValue.longValue(),
                        _stepValue.longValue(),
                        _maxValue.longValue(),
                        _minValue.longValue(),
                        _cycle);
    }

    @Override
    void acceptChildren(Visitor v) throws StandardException {
        super.acceptChildren(v);

        if (_sequenceName != null) {
            _sequenceName = (TableName) _sequenceName.accept(v);
        }
    }
}
