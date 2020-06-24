/*

   Derby - Class org.apache.derby.impl.sql.execute.AlterConstraintConstantAction

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

import org.apache.derby.catalog.UUID;
import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.shared.common.reference.SQLState;
import org.apache.derby.iapi.services.property.PropertyUtil;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.StatementType;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.depend.DependencyManager;
import org.apache.derby.iapi.sql.dictionary.ConstraintDescriptor;
import org.apache.derby.iapi.sql.dictionary.ConstraintDescriptorList;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.ForeignKeyConstraintDescriptor;
import org.apache.derby.iapi.sql.dictionary.ReferencedKeyConstraintDescriptor;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;
import org.apache.derby.iapi.sql.execute.ConstantAction;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.impl.sql.catalog.SYSCONSTRAINTSRowFactory;
import org.apache.derby.impl.sql.compile.ConstraintDefinitionNode;

/**
 *  This class  describes actions that are ALWAYS performed for a
 *  alter constraint at Execution time.
 */

public class AlterConstraintConstantAction extends ConstraintConstantAction
{

    private String constraintSchemaName;
    boolean[] characteristics;

    /**
     *  Constructor.
     *
     *  @param constraintName           The constraint name.
     *  @param constraintSchemaName     The schema that constraint lives in.
     *  @param characteristics          The (presumably) altered enforcement
     *                                  characteristics.
     *  @param tableName                Table name.
     *  @param tableId                  The UUID of table.
     *  @param tableSchemaName          The schema that table lives in.
     *  @param indexAction              IndexConstantAction for constraint
     */
    AlterConstraintConstantAction(
                       String               constraintName,
                       String               constraintSchemaName,
                       boolean[]            characteristics,
                       String               tableName,
                       UUID                 tableId,
                       String               tableSchemaName,
                       IndexConstantAction indexAction)
    {
        super(constraintName, DataDictionary.DROP_CONSTRAINT, tableName,
              tableId, tableSchemaName, indexAction);

        this.constraintSchemaName = constraintSchemaName;
        this.characteristics = characteristics.clone();
    }

    @Override
    public  String  toString()
    {
        // Do not put this under SanityManager.DEBUG - it is needed for
        // error reporting.
        String ss = constraintSchemaName == null ? schemaName : constraintSchemaName;
        return "ALTER CONSTRAINT " + ss + "." + constraintName;
    }

    /**
     *  This is the guts of the Execution-time logic for ALTER CONSTRAINT.
     *
     *  @see ConstantAction#executeConstantAction
     *
     * @exception StandardException     Thrown on failure
     */
    public void executeConstantAction(
            Activation activation ) throws StandardException {

        final LanguageConnectionContext lcc =
                activation.getLanguageConnectionContext();
        final DataDictionary dd = lcc.getDataDictionary();
        final DependencyManager dm = dd.getDependencyManager();
        final TransactionController tc = lcc.getTransactionExecute();


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

        final TableDescriptor td = dd.getTableDescriptor(tableId);

        if (td == null)
        {
            throw StandardException.newException(SQLState.LANG_TABLE_NOT_FOUND_DURING_EXECUTION, tableName);
        }

        /* Table gets locked in AlterTableConstantAction */

        /*
        ** If the schema descriptor is null, then
        ** we must have just read ourselves in.
        ** So we will get the corresponding schema
        ** descriptor from the data dictionary.
        */

        SchemaDescriptor tdSd = td.getSchemaDescriptor();
        SchemaDescriptor constraintSd = constraintSchemaName == null
                ? tdSd
                : dd.getSchemaDescriptor(constraintSchemaName, tc, true);


        /* Get the constraint descriptor for the index, along
         * with an exclusive row lock on the row in sys.sysconstraints
         * in order to ensure that no one else compiles against the
         * index.
         */
        final ConstraintDescriptor conDesc =
                dd.getConstraintDescriptorByName(td, constraintSd, constraintName, true);


        if (conDesc == null) {
            throw StandardException.newException(
                    SQLState.LANG_DROP_OR_ALTER_NON_EXISTING_CONSTRAINT,
                     constraintSd.getSchemaName() + "."+ constraintName,
                    td.getQualifiedName());
        }

        if (characteristics[2] != ConstraintDefinitionNode.ENFORCED_DEFAULT) {
//IC see: https://issues.apache.org/jira/browse/DERBY-532
//IC see: https://issues.apache.org/jira/browse/DERBY-3330
//IC see: https://issues.apache.org/jira/browse/DERBY-6419
            dd.checkVersion(DataDictionary.DD_VERSION_DERBY_10_11,
                            "DEFERRED CONSTRAINTS");

//IC see: https://issues.apache.org/jira/browse/DERBY-532
            if (constraintType == DataDictionary.FOREIGNKEY_CONSTRAINT ||
                constraintType == DataDictionary.NOTNULL_CONSTRAINT ||
                !characteristics[2] /* not enforced */) {

                // Remove when feature DERBY-532 is completed
                if (!PropertyUtil.getSystemProperty(
                        "derby.constraintsTesting", "false").equals("true")) {
                    throw StandardException.newException(
                        SQLState.NOT_IMPLEMENTED,
                        "non-default enforcement");
                }
            }
        }

        // The first two characteristics are unused during ALTER CONSTRAINT; only
        // enforcement can change.
        conDesc.setEnforced(characteristics[2]);

        int[] colsToSet = new int[1];
        colsToSet[0] = SYSCONSTRAINTSRowFactory.SYSCONSTRAINTS_STATE;
        dd.updateConstraintDescriptor(conDesc, conDesc.getUUID(), colsToSet, tc);
    }
}
