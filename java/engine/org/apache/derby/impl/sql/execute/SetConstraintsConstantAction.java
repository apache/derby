/*

   Derby - Class org.apache.derby.impl.sql.execute.SetConstraintsConstantAction

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

import java.util.Enumeration;
import java.util.Hashtable;
import java.util.List;

import org.apache.derby.catalog.UUID;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.depend.DependencyManager;
import org.apache.derby.iapi.sql.dictionary.CheckConstraintDescriptor;
import org.apache.derby.iapi.sql.dictionary.ConstraintDescriptor;
import org.apache.derby.iapi.sql.dictionary.ConstraintDescriptorList;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.ForeignKeyConstraintDescriptor;
import org.apache.derby.iapi.sql.dictionary.IndexRowGenerator;
import org.apache.derby.iapi.sql.dictionary.ReferencedKeyConstraintDescriptor;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;
import org.apache.derby.iapi.sql.execute.ConstantAction;
import org.apache.derby.iapi.sql.execute.ExecIndexRow;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.store.access.ConglomerateController;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.derby.impl.sql.compile.TableName;

/**
 * This class describes actions that are performed for a
 * set constraint at Execution time.  
 * <p>
 * Note that the dependency action we send is SET_CONSTRAINTS
 * rather than ALTER_TABLE.  We do this because we want
 * to distinguish SET_CONSTRAINTS from ALTER_TABLE for
 * error messages.
 *
 */
class SetConstraintsConstantAction extends DDLConstantAction
{

    final private boolean   initiallyDeferred;
    private List<TableName> constraints;


	// CONSTRUCTORS
    /**
     * @param constraints               List of constraints to set; null if all.
     * @param initiallyDeferred         Encodes IMMEDIATE (false), DEFERRED (true)
     */
    SetConstraintsConstantAction(
            List<TableName>             constraints,
            boolean                     initiallyDeferred) {
        this.constraints = constraints;
        this.initiallyDeferred = initiallyDeferred;
	}

    @Override
	public	String	toString()
	{
		// Do not put this under SanityManager.DEBUG - it is needed for
		// error reporting.
		return "SET CONSTRAINTS";
	}

	/**
	 *	This is the guts of the Execution-time logic for DROP CONSTRAINT.
	 *
	 *	@see ConstantAction#executeConstantAction
	 *
	 * @exception StandardException		Thrown on failure
	 */
    public void executeConstantAction(
            Activation activation) throws StandardException {

        final LanguageConnectionContext lcc =
                activation.getLanguageConnectionContext();

        final DataDictionary dd = lcc.getDataDictionary();
        ConstraintDescriptorList cl = new ConstraintDescriptorList();

        if (constraints != null) {
            for (TableName c : constraints) {
                SchemaDescriptor sd = dd.getSchemaDescriptor(
                    c.getSchemaName(),
                    lcc.getTransactionExecute(),
                    true);

                ConstraintDescriptor cd =
                    dd.getConstraintDescriptor(c.getTableName(), sd.getUUID());

                if (cd == null) {
                    throw StandardException.newException(
                            SQLState.LANG_OBJECT_NOT_FOUND,
                            "CONSTRAINT",
                            c.getFullSQLName());
                }

                cl.add(cd);
            }
        } else {
            cl = dd.getConstraintDescriptors(null); // all
        }

        throw StandardException.newException(SQLState.NOT_IMPLEMENTED,
                       "SET CONSTRAINT");
    }
}
