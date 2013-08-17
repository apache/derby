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

import java.sql.Types;
import java.util.List;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.ClassName;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.services.classfile.VMOpcode;
import org.apache.derby.iapi.services.compiler.MethodBuilder;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derby.iapi.sql.compile.CompilerContext;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;
import org.apache.derby.iapi.sql.dictionary.SequenceDescriptor;

/**
 * A class that represents a value obtained from a Sequence using 'NEXT VALUE'
 */
class NextSequenceNode extends ValueNode {

    private TableName sequenceName;
    private SequenceDescriptor sequenceDescriptor;

    /**
     * Constructor for a NextSequenceNode
     *
     * @param sequenceName The name of the sequence being called
     * @param cm           The context manager
     */
    NextSequenceNode(TableName sequenceName,
                     ContextManager cm) {
        super(cm);
        this.sequenceName = sequenceName;
    }

    /**
     * Bind this expression.  This means binding the sub-expressions,
     * as well as figuring out what the return type is for this expression.
     *
     * @param fromList        The FROM list for the query this
     *                        expression is in, for binding columns.
     * @param subqueryList    The subquery list being built as we find SubqueryNodes
     * @param aggregates      The aggregate list being built as we find AggregateNodes
     * @return The new top of the expression tree.
     * @throws StandardException Thrown on error
     */
    @Override
    ValueNode bindExpression(FromList fromList,
                             SubqueryList subqueryList,
                             List<AggregateNode> aggregates,
                             boolean forQueryRewrite) throws StandardException
    {
        //
        // Higher level bind() logic may try to redundantly re-bind this node. Unfortunately,
        // that causes us to think that the sequence is being referenced more than once
        // in the same statement. If the sequence generator is already filled in, then
        // this node has already been bound and we can exit quickly. See DERBY-4803.
        //
        if ( sequenceDescriptor != null ) { return this; }
        
        CompilerContext cc = getCompilerContext();
        
        if ( (cc.getReliability() & CompilerContext.NEXT_VALUE_FOR_ILLEGAL) != 0 )
        {
            throw StandardException.newException( SQLState.LANG_NEXT_VALUE_FOR_ILLEGAL );
        }

        // lookup sequence object in the data dictionary
        SchemaDescriptor sd = getSchemaDescriptor(sequenceName.getSchemaName());
        sequenceDescriptor = getDataDictionary().getSequenceDescriptor(sd, sequenceName.getTableName());

        if ( sequenceDescriptor == null )
        {
                throw StandardException.newException(SQLState.LANG_OBJECT_NOT_FOUND, "SEQUENCE", sequenceName.getFullTableName());
        }

        // set the datatype of the value node
        this.setType(sequenceDescriptor.getDataType());

        //
        // The statement is only allowed to refer to a given sequence once.
        // See DERBY-4513.
        //
        if ( cc.isReferenced( sequenceDescriptor ) )
        {
            throw StandardException.newException
                ( SQLState.LANG_SEQUENCE_REFERENCED_TWICE, sequenceName.getFullTableName() );
        }
        cc.addReferencedSequence( sequenceDescriptor );

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


    @Override
    void generateExpression
        (
         ExpressionClassBuilder acb, MethodBuilder mb)
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
    void generateConstant
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
    @Override
    public String toString() {
        if (SanityManager.DEBUG) {
            return super.toString();
        } else {
            return "";
        }
    }

    boolean isEquivalent(ValueNode other) throws StandardException {
        return false;
    }
}
