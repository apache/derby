/*

   Derby - Class org.apache.derby.impl.sql.execute.SetRoleConstantAction

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


import org.apache.derby.iapi.sql.execute.ConstantAction;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.RoleGrantDescriptor;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.ParameterValueSet;
import org.apache.derby.iapi.sql.StatementType;
import org.apache.derby.iapi.sql.depend.DependencyManager;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.shared.common.reference.SQLState;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.iapi.util.IdUtil;
import org.apache.derby.iapi.util.StringUtil;

/**
 *  This class describes actions that are ALWAYS performed for a
 *  SET ROLE Statement at Execution time.
 *
 */

class SetRoleConstantAction implements ConstantAction
{

    private final String  roleName;
    private final int     type;

    // CONSTRUCTORS

    /**
     * Make the ConstantAction for a SET ROLE statement.
     *
     *  @param roleName Name of role.
     *  @param type     type of set role (literal role name or ?)
     */
    SetRoleConstantAction(String roleName, int type)
    {
        this.roleName = roleName;
        this.type = type;
    }

    ///////////////////////////////////////////////
    //
    // OBJECT SHADOWS
    //
    ///////////////////////////////////////////////

    public String toString()
    {
        // Do not put this under SanityManager.DEBUG - it is needed for
        // error reporting.
        // If the error happens after we have figured out the role name for
        // dynamic we want to use it rather than ?
        return "SET ROLE " +
            ((type == StatementType.SET_ROLE_DYNAMIC && roleName == null) ?
             "?" : roleName);
    }

    // INTERFACE METHODS

    /**
     *  This is the guts of the Execution-time logic for SET ROLE.
     *
     *  @see ConstantAction#executeConstantAction
     *
     * @exception StandardException     Thrown on failure
     */
    public void executeConstantAction( Activation activation )
                        throws StandardException {

        LanguageConnectionContext   lcc;
        DataDictionary              dd;

        // find the language context.
        lcc = activation.getLanguageConnectionContext();

        dd = lcc.getDataDictionary();
        String thisRoleName = roleName;

        final String currentAuthId = lcc.getCurrentUserId(activation);
        final String dbo = lcc.getDataDictionary().
            getAuthorizationDatabaseOwner();

        TransactionController tc = lcc.getTransactionExecute();

        // SQL 2003, section 18.3, General rule 1:
        if (!tc.isIdle()) {
            throw StandardException.newException
                (SQLState.INVALID_TRANSACTION_STATE_ACTIVE_CONNECTION);
        }

        if (type == StatementType.SET_ROLE_DYNAMIC) {
            ParameterValueSet pvs = activation.getParameterValueSet();
            DataValueDescriptor dvs = pvs.getParameter(0);
            // SQL 2003, section 18.3, GR2: trim whitespace first, and
            // interpret as identifier, then we convert it to case normal form
            // here.
            String roleId = dvs.getString();

            if (roleId == null) {
                throw StandardException.newException(SQLState.ID_PARSE_ERROR);
            }

            thisRoleName = IdUtil.parseRoleId(roleId);
        }

        RoleGrantDescriptor rdDef = null;

        try {
            String oldRole = lcc.getCurrentRoleId(activation);

            if (oldRole != null && !oldRole.equals(thisRoleName)) {
                rdDef = dd.getRoleDefinitionDescriptor(oldRole);

                if (rdDef != null) {
                    dd.getDependencyManager().invalidateFor(
                        rdDef,
                        DependencyManager.RECHECK_PRIVILEGES,
                        lcc);
                } // else: old role else no longer exists, so ignore.
            }

            if (thisRoleName != null) {
                rdDef = dd.getRoleDefinitionDescriptor(thisRoleName);

                // SQL 2003, section 18.3, General rule 4:
                if (rdDef == null) {
                    throw StandardException.newException
                        (SQLState.ROLE_INVALID_SPECIFICATION, thisRoleName);
                }

                if (!lcc.roleIsSettable(activation, thisRoleName)) {
                    throw StandardException.newException
                              (SQLState. ROLE_INVALID_SPECIFICATION_NOT_GRANTED,
                               thisRoleName);
                }
            }
        } finally {
            // reading above changes idle state, so reestablish it
            lcc.userCommit();
        }

        lcc.setCurrentRole(activation, rdDef != null ? thisRoleName : null);
    }
}
