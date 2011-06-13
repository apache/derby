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
import org.apache.derby.iapi.sql.dictionary.SequenceDescriptor;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.types.TypeId;


/**
 * A CreateSequenceNode is the root of a QueryTree that
 * represents a CREATE SEQUENCE statement.
 */

public class CreateSequenceNode extends DDLStatementNode
{
    private TableName _sequenceName;
    private DataTypeDescriptor _dataType;
    private Long _initialValue;
    private Long _stepValue;
    private Long _maxValue;
    private Long _minValue;
    private Boolean _cycle;

    public static final int SEQUENCE_ELEMENT_COUNT = 1;

    /**
     * Initializer for a CreateSequenceNode
     *
     * @param sequenceName The name of the new sequence
     * @param dataType Exact numeric type of the new sequence
     * @param initialValue Starting value
     * @param stepValue Increment amount
     * @param maxValue Largest value returned by the sequence generator
     * @param minValue Smallest value returned by the sequence generator
     * @param cycle True if the generator should wrap around, false otherwise
     *
     * @throws org.apache.derby.iapi.error.StandardException on error
     */
    public void init
        (
         Object sequenceName,
         Object dataType,
         Object initialValue,
         Object stepValue,
         Object maxValue,
         Object minValue,
         Object cycle
         ) throws StandardException {

        _sequenceName = (TableName) sequenceName;
        initAndCheck(_sequenceName);

        if (dataType != null) {
            _dataType = (DataTypeDescriptor) dataType;
        } else {
            _dataType = DataTypeDescriptor.INTEGER;
        }

        _stepValue = (stepValue != null ? (Long) stepValue : new Long(1));

        Long[]  minMax = SequenceDescriptor.computeMinMax( _dataType, minValue, maxValue );
        _minValue = minMax[ SequenceDescriptor.MIN_VALUE ];
        _maxValue = minMax[ SequenceDescriptor.MAX_VALUE ];

        if (initialValue != null) {
            _initialValue = (Long) initialValue;
        } else {
            if (_stepValue.longValue() > 0L) {
                _initialValue = _minValue;
            } else {
                _initialValue = _maxValue;
            }
        }
        _cycle = (cycle != null ? (Boolean) cycle : Boolean.FALSE);

        // automatically create the schema if it doesn't exist
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
    public void bindStatement() throws StandardException {
        CompilerContext cc = getCompilerContext();

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
    public ConstantAction makeConstantAction() {
             return getGenericConstantActionFactory().
                getCreateSequenceConstantAction(
                        _sequenceName,
                        _dataType,
                        _initialValue.longValue(),
                        _stepValue.longValue(),
                        _maxValue.longValue(),
                        _minValue.longValue(),
                        _cycle.booleanValue());
    }


}
