/*

   Derby - Class org.apache.derby.impl.sql.execute.RevokeRoleConstantAction

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

import java.util.Iterator;
import java.util.List;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.dictionary.RoleDescriptor;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.shared.common.reference.SQLState;
import org.apache.derby.iapi.services.sanity.SanityManager;


/**
 *  This class performs actions that are ALWAYS performed for a
 *  REVOKE role statement at execution time.
 *
 */
class RevokeRoleConstantAction extends DDLConstantAction {

    private List roleNames;
    private List grantees;
    private final boolean withAdminOption = false; // not impl.

    // CONSTRUCTORS
    /**
     *  Make the ConstantAction for a CREATE ROLE statement.
     *  When executed, will create a role by the given name.
     *
     *  @param roleNames     List of the name of the role names being revoked
     *  @param grantees       List of the authorization ids granted to role
     */
    public RevokeRoleConstantAction(List roleNames, List grantees) {
        this.roleNames = roleNames;
        this.grantees = grantees;
    }

    // INTERFACE METHODS

    /**
     *  This is the guts of the Execution-time logic for REVOKE role.
     *
     *  @see ConstantAction#executeConstantAction
     *
     * @exception StandardException     Thrown on failure
     */
    public void executeConstantAction(Activation activation)
            throws StandardException {

        LanguageConnectionContext lcc =
            activation.getLanguageConnectionContext();
        DataDictionary dd = lcc.getDataDictionary();
        TransactionController tc = lcc.getTransactionExecute();

        final String grantor = lcc.getAuthorizationId();

        dd.startWriting(lcc);

        for (Iterator rIter = roleNames.iterator(); rIter.hasNext();) {
            String role = (String)rIter.next();

            for (Iterator gIter = grantees.iterator(); gIter.hasNext();) {
                String grantee = (String)gIter.next();

                // check that role exists
                RoleDescriptor rd = dd.getRoleDefinitionDescriptor(role);

                if (rd == null) {
                    throw StandardException.
                        newException(SQLState.ROLE_INVALID_SPECIFICATION, role);
                }

                // Check that role is granted to us (or PUBLIC) with
                // WITH ADMIN option so we can grant (and hence
                // revoke) it. For database owner, a role definition
                // always fulfills this requirement.  If we implement
                // granting with WITH ADMIN option later, we need to
                // look for a grant to us or to PUBLIC which has WITH
                // ADMIN. The role definition descriptor will not
                // suffice in that case, so we need something like:
                //
                // rd = dd.findRoleGrantWithAdminToRoleOrPublic(grantor)
                // if (rd != null) {
                //   :
                if (grantor.equals(rd.getGrantee())) {
                    // All ok, we are database owner
                    if (SanityManager.DEBUG) {
                        SanityManager.ASSERT(
                            lcc.getDataDictionary().
                            getAuthorizationDatabaseOwner().
                            equals(grantor),
                            "expected database owner in role descriptor");
                        SanityManager.ASSERT(
                            rd.isWithAdminOption(),
                            "expected role definition to have ADMIN OPTION");
                    }
                } else {
                    throw StandardException.newException
                        (SQLState.AUTH_ROLE_DBO_ONLY, "REVOKE role");
                }

                rd = dd.getRoleGrantDescriptor(role, grantee, grantor);

                if (rd != null && withAdminOption) {
                    // NOTE: Never called yet, withAdminOption not yet
                    // implemented.

                    // revoke only the ADMIN OPTION from grantee
                    //
                    if (rd.isWithAdminOption()) {
                        // Remove old descriptor and add a new one
                        // without admin option.
                        rd.drop(lcc);
                        rd.setWithAdminOption(false);
                        dd.addDescriptor(rd,
                                         null,  // parent
                                         DataDictionary.SYSROLES_CATALOG_NUM,
                                         false, // no duplicatesAllowed
                                         tc);
                    } else {
                        activation.addWarning
                            (StandardException.newWarning
                             (SQLState.LANG_WITH_ADMIN_OPTION_NOT_REVOKED,
                              role, grantee));
                    }
                } else if (rd != null) {
                    // normal revoke of role from grantee
                    //
                    rd.drop(lcc);
                } else {
                    activation.addWarning
                        (StandardException.newWarning
                         (SQLState.LANG_ROLE_NOT_REVOKED, role, grantee));
                }
            }
        }
    }


    // OBJECT SHADOWS

    public String toString()
    {
        // Do not put this under SanityManager.DEBUG - it is needed for
        // error reporting.

        StringBuffer sb1 = new StringBuffer();
        for (Iterator it = roleNames.iterator(); it.hasNext();) {
            if( sb1.length() > 0) {
                sb1.append( ", ");
            }
            sb1.append( it.next().toString());
        }

        StringBuffer sb2 = new StringBuffer();
        for (Iterator it = grantees.iterator(); it.hasNext();) {
            if( sb2.length() > 0) {
                sb2.append( ", ");
            }
            sb2.append( it.next().toString());
        }
        return ("REVOKE " +
                sb1.toString() +
                " FROM: " +
                sb2.toString() +
                "\n");
    }
}
