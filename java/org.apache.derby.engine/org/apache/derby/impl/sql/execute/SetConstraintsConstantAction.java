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

import java.util.ArrayList;
import java.util.List;
import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.shared.common.reference.SQLState;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.dictionary.ConstraintDescriptor;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;
import org.apache.derby.iapi.sql.execute.ConstantAction;
import org.apache.derby.iapi.util.IdUtil;
import org.apache.derby.impl.sql.compile.TableName;

/**
 * This class describes actions that are performed for a
 * set constraint at execution time.
 * <p>
 * Note that the dependency action we send is SET_CONSTRAINTS
 * rather than ALTER_TABLE.  We do this because we want
 * to distinguish SET_CONSTRAINTS from ALTER_TABLE for
 * error messages.
 *
 */
class SetConstraintsConstantAction extends DDLConstantAction
{

//IC see: https://issues.apache.org/jira/browse/DERBY-532
//IC see: https://issues.apache.org/jira/browse/DERBY-3330
//IC see: https://issues.apache.org/jira/browse/DERBY-6419
    final private boolean   deferred;
    final private List<TableName> constraints;


	// CONSTRUCTORS
    /**
     * @param constraints      List of constraints to set; null if all.
     * @param deferred         Encodes IMMEDIATE (false), DEFERRED (true)
     */
    SetConstraintsConstantAction(
            List<TableName>             constraints,
            boolean                     deferred) {
        this.constraints = constraints;
        this.deferred = deferred;
	}

    @Override
	public	String	toString()
	{
		// Do not put this under SanityManager.DEBUG - it is needed for
		// error reporting.
		return "SET CONSTRAINTS";
	}

	/**
     * This is the guts of the execution time logic for SET CONSTRAINT.
	 *
     * @param activation
     *
     * @see ConstantAction#executeConstantAction
	 *
	 * @exception StandardException		Thrown on failure
	 */
    public void executeConstantAction(
            Activation activation) throws StandardException {

        final LanguageConnectionContext lcc =
                activation.getLanguageConnectionContext();

        final DataDictionary dd = lcc.getDataDictionary();
        final List<String> boundConstraints = new ArrayList<String>();
//IC see: https://issues.apache.org/jira/browse/DERBY-532

        if (constraints != null) {
            for (TableName c : constraints) {

                final SchemaDescriptor sd = dd.getSchemaDescriptor(
                    c.getSchemaName(),
                    lcc.getTransactionExecute(),
                    true);

                final ConstraintDescriptor cd =
                    dd.getConstraintDescriptor(c.getTableName(), sd.getUUID());

                if (cd == null) {
                    throw StandardException.newException(
                            SQLState.LANG_OBJECT_NOT_FOUND,
                            "CONSTRAINT",
                            c.getFullSQLName());
                }

//IC see: https://issues.apache.org/jira/browse/DERBY-532
                final String bound =
                        IdUtil.normalToDelimited(sd.getSchemaName()) + "." +
                        IdUtil.normalToDelimited(cd.getConstraintName());

                if (boundConstraints.contains(bound)) {
                    throw StandardException.newException(
                            SQLState.LANG_DB2_DUPLICATE_NAMES,
                            cd.getConstraintName(),
                            bound);
                } else {
                    boundConstraints.add(bound);
                }

                if (deferred && !cd.deferrable()) {
                    throw StandardException.newException(
                            SQLState.LANG_SET_CONSTRAINT_NOT_DEFERRABLE,
                            cd.getConstraintName());
                }

//IC see: https://issues.apache.org/jira/browse/DERBY-6670
//IC see: https://issues.apache.org/jira/browse/DERBY-6665
                lcc.setConstraintDeferred(
                    activation,
                    cd,
                    deferred);
            }
        } else {
            lcc.setDeferredAll(activation, deferred);
        }


    }
}
