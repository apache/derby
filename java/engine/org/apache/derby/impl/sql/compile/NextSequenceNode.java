/*

   Derby - Class org.apache.derby.impl.sql.compile.NextSequenceNode

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
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.reference.ClassName;
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.services.compiler.MethodBuilder;
import org.apache.derby.iapi.services.compiler.LocalField;
import org.apache.derby.iapi.services.classfile.VMOpcode;
import org.apache.derby.iapi.sql.dictionary.SequenceDescriptor;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;

import java.util.Vector;
import java.sql.Types;

/**
 * A class that represents a value obtained from a Sequence using 'NEXT VALUE'
 */
public class NextSequenceNode extends ValueNode {

    private TableName sequenceName;
    private SequenceDescriptor sequenceDescriptor;

    /**
     * Initializer for a NextSequenceNode
     *
     * @param sequenceName The name of the sequence being called
     * @throws org.apache.derby.iapi.error.StandardException
     *          Thrown on error
     */
    public void init(Object sequenceName) throws StandardException {
        this.sequenceName = (TableName) sequenceName;
    }

    /**
     * Bind this expression.  This means binding the sub-expressions,
     * as well as figuring out what the return type is for this expression.
     *
     * @param fromList        The FROM list for the query this
     *                        expression is in, for binding columns.
     * @param subqueryList    The subquery list being built as we find SubqueryNodes
     * @param aggregateVector The aggregate vector being built as we find AggregateNodes
     * @return The new top of the expression tree.
     * @throws StandardException Thrown on error
     */
    public ValueNode bindExpression(
            FromList fromList, SubqueryList subqueryList,
            Vector aggregateVector, boolean forQueryRewrite)
            throws StandardException {

        // lookup sequence object in the data dictionary
        SchemaDescriptor sd = getSchemaDescriptor(sequenceName.getSchemaName());
        sequenceDescriptor = getDataDictionary().getSequenceDescriptor(sd, sequenceName.getTableName());

        if ( sequenceDescriptor == null )
        {
                throw StandardException.newException(SQLState.LANG_OBJECT_NOT_FOUND, "SEQUENCE", sequenceName.getFullTableName());
        }

        // set the datatype of the value node
        this.setType(sequenceDescriptor.getDataType());

        ValueNode returnNode = this;

        // set up dependency on sequence and compile a check for USAGE
        // priv if needed
        getCompilerContext().createDependency( sequenceDescriptor );

        if ( isPrivilegeCollectionRequired() )
        {
            getCompilerContext().addRequiredUsagePriv( sequenceDescriptor );
        }

        return returnNode;
    }


    public void generateExpression
        (
         ExpressionClassBuilder acb,
         MethodBuilder mb
         )
        throws StandardException
    {
        String sequenceUUIDstring = sequenceDescriptor.getUUID().toString();
        int dataTypeFormatID = sequenceDescriptor.getDataType().getNull().getTypeFormatId();
        
		mb.pushThis();
		mb.push( sequenceUUIDstring );
		mb.push( dataTypeFormatID );
		mb.callMethod
            (
             VMOpcode.INVOKEVIRTUAL,
             ClassName.BaseActivation,
             "getCurrentValueAndAdvance",
             ClassName.NumberDataValue,
             2
             );
    }

    /**
     * Dummy implementation to return a constant. Will be replaced with actual NEXT VALUE logic.
     *
     * @param ecb The ExpressionClassBuilder for the class being built
     * @param mb The method the expression will go into
     * @throws StandardException on error
     */
    public void generateConstant
            (
                    ExpressionClassBuilder ecb,
                    MethodBuilder mb
            ) throws StandardException {
        switch (getTypeServices().getJDBCTypeId()) {
            case Types.INTEGER:
                mb.push(1);
                break;
            default:
                if (SanityManager.DEBUG) {
                    SanityManager.THROWASSERT(
                            "Unexpected dataType = " + getTypeServices().getJDBCTypeId());
                }
        }

    }

    /**
     * Convert this object to a String.  See comments in QueryTreeNode.java
     * for how this should be done for tree printing.
     *
     * @return This object as a String
     */

    public String toString() {
        if (SanityManager.DEBUG) {
            return super.toString();
        } else {
            return "";
        }
    }

    protected boolean isEquivalent(ValueNode other) throws StandardException {
        return false;
    }
}
