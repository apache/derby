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
import org.apache.derby.iapi.sql.dictionary.RoleDescriptor;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.ParameterValueSet;
import org.apache.derby.iapi.sql.StatementType;
import org.apache.derby.iapi.sql.conn.Authorizer;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.reference.Limits;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.store.access.TransactionController;

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

        final String currentAuthId = lcc.getAuthorizationId();
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
            thisRoleName = dvs.getString();
        }

        RoleDescriptor rd = null;

        if (thisRoleName != null) {
            try {
                rd = dd.getRoleDefinitionDescriptor(thisRoleName);

                // SQL 2003, section 18.3, General rule 4:
                if (rd == null) {
                    throw StandardException.newException
                        (SQLState.ROLE_INVALID_SPECIFICATION, thisRoleName);
                }

                if (!currentAuthId.equals(dbo)) {
                    // is it granted to us mere mortals?
                    rd = dd.getRoleGrantDescriptor(thisRoleName,
                                                   currentAuthId,
                                                   dbo);
                    if (rd == null) {
                        // or if not, via PUBLIC?
                        rd = dd.getRoleGrantDescriptor
                            (thisRoleName,
                             Authorizer.PUBLIC_AUTHORIZATION_ID,
                             dbo);

                        // Nope, we can't set this role, so throw.
                        if (rd == null) {
                            throw StandardException.newException
                              (SQLState. ROLE_INVALID_SPECIFICATION_NOT_GRANTED,
                               thisRoleName);
                        }
                    }
                }
            } finally {
                // reading above changes idle state, so reestablish it
                lcc.userCommit();
            }
        }

        lcc.setCurrentRole(activation, rd != null ? thisRoleName : null);
    }
}
