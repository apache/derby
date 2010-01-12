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
     * @param sequenceName The name of the new sequence
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

        _dataType = (DataTypeDescriptor) dataType;
        _initialValue = (Long) initialValue;
        _stepValue = (Long) stepValue;
        _maxValue = (Long) maxValue;
        _minValue = (Long) minValue;
        _cycle = (Boolean) cycle;

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

        // Right now we only support vanilla sequences
        if ( (_dataType != null) && ( !_dataType.getTypeId().equals( TypeId.INTEGER_ID ) ) ) { throw unimplementedFeature(); }
        if ( (_initialValue != null) && ( _initialValue.longValue() != -2147483648L ) ) { throw unimplementedFeature(); }
        if ( (_stepValue != null) && ( _stepValue.longValue() != 1L ) ) { throw unimplementedFeature(); }
        if ( (_maxValue != null) && ( _maxValue.longValue() != 2147483647L ) ) { throw unimplementedFeature(); }
        if ( (_minValue != null) && ( _minValue.longValue() != -2147483648L ) ) { throw unimplementedFeature(); }
        if ( (_cycle != null) && ( _cycle != Boolean.FALSE ) ) { throw unimplementedFeature(); }
        
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
                getCreateSequenceConstantAction(_sequenceName);
    }

    /** Report an unimplemented feature */
    private StandardException unimplementedFeature()
    {
        return StandardException.newException( SQLState.BTREE_UNIMPLEMENTED_FEATURE );
    }

}
