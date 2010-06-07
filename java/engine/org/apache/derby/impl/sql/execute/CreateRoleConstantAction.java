/*

   Derby - Class org.apache.derby.impl.sql.execute.CreateRoleConstantAction

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

import org.apache.derby.iapi.services.property.PropertyUtil;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.util.IdUtil;
import org.apache.derby.iapi.jdbc.AuthenticationService;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.conn.Authorizer;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.dictionary.DataDescriptorGenerator;
import org.apache.derby.iapi.sql.dictionary.RoleGrantDescriptor;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.impl.jdbc.authentication.BasicAuthenticationServiceImpl;
import org.apache.derby.shared.common.reference.SQLState;
import org.apache.derby.iapi.reference.Property;

/**
 *  This class performs actions that are ALWAYS performed for a
 *  CREATE ROLE statement at execution time.
 *  These SQL objects are stored in the SYS.SYSROLES table.
 *
 */
class CreateRoleConstantAction extends DDLConstantAction {

    private String roleName;

    // CONSTRUCTORS
    /**
     *  Make the ConstantAction for a CREATE ROLE statement.
     *  When executed, will create a role by the given name.
     *
     *  @param roleName     The name of the role being created
     */
    public CreateRoleConstantAction(String roleName)
    {
        this.roleName = roleName;
    }

    // INTERFACE METHODS

    /**
     *  This is the guts of the Execution-time logic for CREATE ROLE.
     *
     *  @see ConstantAction#executeConstantAction
     *
     * @exception StandardException     Thrown on failure
     */
    public void executeConstantAction(Activation activation)
        throws StandardException
    {

        LanguageConnectionContext lcc =
            activation.getLanguageConnectionContext();
        DataDictionary dd = lcc.getDataDictionary();
        TransactionController tc = lcc.getTransactionExecute();
        DataDescriptorGenerator ddg = dd.getDataDescriptorGenerator();

        if (roleName.equals(Authorizer.PUBLIC_AUTHORIZATION_ID)) {
            throw StandardException.
                newException(SQLState.AUTH_PUBLIC_ILLEGAL_AUTHORIZATION_ID);
        }

        // currentAuthId is currently always the database owner since
        // role definition is a database owner power. This may change
        // in the future since this SQL is more liberal.
        //
        final String currentAuthId = lcc.getCurrentUserId(activation);

        dd.startWriting(lcc);

        //
        // Check if this role already exists. If it does, throw.
        //
        RoleGrantDescriptor rdDef = dd.getRoleDefinitionDescriptor(roleName);

        if (rdDef != null) {
            throw StandardException.
                newException(SQLState.LANG_OBJECT_ALREADY_EXISTS,
                             rdDef.getDescriptorType(), roleName);
        }

        // Check if the proposed role id exists as a user id in
        // a privilege grant or as a built-in user ("best effort"; we
        // can't guarantee against collision if users are externally
        // defined or added later).
        if (knownUser(roleName, currentAuthId, lcc, dd, tc)) {
            throw StandardException.
                newException(SQLState.LANG_OBJECT_ALREADY_EXISTS,
                             "User", roleName);
        }

        rdDef = ddg.newRoleGrantDescriptor(
            dd.getUUIDFactory().createUUID(),
            roleName,
            currentAuthId,// grantee
            Authorizer.SYSTEM_AUTHORIZATION_ID,// grantor
            true,         // with admin option
            true);        // is definition

        dd.addDescriptor(rdDef,
                         null,  // parent
                         DataDictionary.SYSROLES_CATALOG_NUM,
                         false, // duplicatesAllowed
                         tc);
    }


    // OBJECT SHADOWS

    public String toString()
    {
        // Do not put this under SanityManager.DEBUG - it is needed for
        // error reporting.
        return "CREATE ROLE " + roleName;
    }

    // PRIVATE METHODS

    /**
     * Heuristically, try to determine is a proposed role identifier
     * is already known to Derby as a user name. Method: If BUILTIN
     * authentication is used, check if there is such a user. If
     * external authentication is used, we lose.  If there turns out
     * to be collision, and we can't detect it here, we should block
     * such a user from connecting (FIXME), since there is now a role
     * with that name.
     */
    private boolean knownUser(String roleName,
                              String currentUser,
                              LanguageConnectionContext lcc,
                              DataDictionary dd,
                              TransactionController tc)
            throws StandardException {
        //
        AuthenticationService s = lcc.getDatabase().getAuthenticationService();

        if (currentUser.equals(roleName)) {
            return true;
        }

        if (s instanceof BasicAuthenticationServiceImpl) {
            // Derby builtin authentication

            if (PropertyUtil.existsBuiltinUser(tc,roleName)) {
                return true;
            }
        } else {
            // Does LDAP  offer a way to ask if a user exists?
            // User supplied authentication?
            // See DERBY-866. Would be nice to have a dictionary table of users
            // synchronized against external authentication providers.
        }

        // Goto through all grants to see if there is a grant to an
        // authorization identifier which is not a role (hence, it
        // must be a user).
        if (dd.existsGrantToAuthid(roleName, tc)) {
            return true;
        }

        // Go through all schemas to see if any one of them is owned by a authid
        // the same as the proposed roleName.
        if (dd.existsSchemaOwnedBy(roleName, tc)) {
            return true;
        }

        return false;
    }
}
