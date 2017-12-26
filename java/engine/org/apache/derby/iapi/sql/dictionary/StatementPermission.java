/*

   Derby - Class org.apache.derby.iapi.sql.dictionary.StatementPermission

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

package org.apache.derby.iapi.sql.dictionary;

import org.apache.derby.shared.common.reference.SQLState;
import org.apache.derby.iapi.sql.conn.Authorizer;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.iapi.sql.execute.ExecPreparedStatement;
import org.apache.derby.iapi.sql.depend.DependencyManager;
import org.apache.derby.iapi.services.context.ContextManager;

/**
 * This class describes a permission require by a statement.
 */

public abstract class StatementPermission
{
	/**
	 * Restrict implementations to this package to reduce
	 * risk of external code spoofing the GRANT/REVOKE system
	 * by providing its own fake implementations.
	 *
	 */
	StatementPermission()
	{
	}
	/**
	 * @param lcc				LanguageConnectionContext
	 * @param forGrant
	 * @param activation        activation for statement needing check
	 *
	 * @exception StandardException if the permission has not been granted
	 */
	public abstract void check( LanguageConnectionContext lcc,
								boolean forGrant,
								Activation activation) throws StandardException;

	/**
	 * 
	 * Get the PermissionsDescriptor for the passed authorization id for this
	 * object. This method gets called during the execution phase of create 
	 * view/constraint/trigger. The return value of this method is saved in
	 * dependency system to keep track of views/constraints/triggers 
	 * dependencies on required permissions. This happens in execution phase 
	 * after it has been established that passed authorization id has all the 
	 * permissions it needs to create that view/constraint/trigger. Which means 
	 * that we can only get to writing into dependency system once all the required 
	 * privileges are confirmed. 
	 *   
	 * @param authid	AuthorizationId
	 * @param dd	DataDictionary
	 * 
	 * @return PermissionsDescriptor	The PermissionsDescriptor for the passed
	 *  authorization id on this object
	 * 
	 * @exception StandardException
	 */
	public abstract PermissionsDescriptor getPermissionDescriptor(String authid, DataDictionary dd)
	throws StandardException;

    /**
     * Return true if the passed in permission matches the one required by this
     * StatementPermission.
     */
    public boolean isCorrectPermission( PermissionsDescriptor pd ) throws StandardException
    { return false; }

    /**
     * Get the privileged object associated with this permission.
     */
    public PrivilegedSQLObject getPrivilegedObject( DataDictionary dd ) throws StandardException
    { return null; }

    /**
     * Get the type of the privileged object.
     */
    public String getObjectType()
    { return null; }

    /**
     * Generic logic called by check() for USAGE and EXECUTE privileges. Throws
     * an exception if the correct permission cannot be found.
     */
	public void genericCheck
        (
         LanguageConnectionContext lcc,
         boolean forGrant,
         Activation activation,
         String privilegeType )
        throws StandardException
	{
		DataDictionary dd = lcc.getDataDictionary();
		TransactionController tc = lcc.getTransactionExecute();
		ExecPreparedStatement ps = activation.getPreparedStatement();

        PermissionsDescriptor perm =
            getPermissionDescriptor( lcc.getCurrentUserId(activation), dd );
		if( !isCorrectPermission( perm ) ) { perm = getPermissionDescriptor(Authorizer.PUBLIC_AUTHORIZATION_ID, dd ); }

        // if the user has the correct permission, we're done
		if ( isCorrectPermission( perm ) ) { return; }

		boolean resolved = false;

		// Since no permission exists for the current user or PUBLIC,
		// check if a permission exists for the current role (if set).
		String role = lcc.getCurrentRoleId(activation);

		if (role != null) {

			// Check that role is still granted to current user or
			// to PUBLIC: A revoked role which is current for this
			// session, is lazily set to none when it is attemped
			// used.
			String dbo = dd.getAuthorizationDatabaseOwner();
			RoleGrantDescriptor rd = dd.getRoleGrantDescriptor
                (role, lcc.getCurrentUserId(activation), dbo);

			if (rd == null) {
				rd = dd.getRoleGrantDescriptor(
					role,
					Authorizer.PUBLIC_AUTHORIZATION_ID,
					dbo);
			}

			if (rd == null) {
				// We have lost the right to set this role, so we can't
				// make use of any permission granted to it or its
				// ancestors.
				lcc.setCurrentRole(activation, null);
			} else {
				// The current role is OK, so we can make use of
				// any permission granted to it.
				//
				// Look at the current role and, if necessary, the
				// transitive closure of roles granted to current role to
				// see if permission has been granted to any of the
				// applicable roles.

				RoleClosureIterator rci =
					dd.createRoleClosureIterator
					(activation.getTransactionController(),
					 role, true );

				String r;
				while (!resolved && (r = rci.next()) != null)
                {
					perm = getPermissionDescriptor( r, dd );

					if ( isCorrectPermission( perm ) ) { resolved = true; }
				}
			}

			if (resolved ) {
				// Also add a dependency on the role (qua provider), so that if
				// role is no longer available to the current user (e.g. grant
				// is revoked, role is dropped, another role has been set), we
				// are able to invalidate the ps or activation (the latter is
				// used if the current role changes).
				DependencyManager dm = dd.getDependencyManager();
				RoleGrantDescriptor rgd = dd.getRoleDefinitionDescriptor(role);
				ContextManager cm = lcc.getContextManager();
				dm.addDependency(ps, rgd, cm);
				dm.addDependency(activation, rgd, cm);
			}
		}

		if (!resolved)
        {
            PrivilegedSQLObject pso = getPrivilegedObject( dd );

			if( pso == null )
            {
				throw StandardException.newException
                    ( SQLState.AUTH_INTERNAL_BAD_UUID, getObjectType() );
            }

			SchemaDescriptor sd = pso.getSchemaDescriptor();

			if( sd == null)
            {
				throw StandardException.newException(
					SQLState.AUTH_INTERNAL_BAD_UUID, "SCHEMA");
            }

			throw StandardException.newException(
				(forGrant
				 ? SQLState.AUTH_NO_GENERIC_PERMISSION_FOR_GRANT
				 : SQLState.AUTH_NO_GENERIC_PERMISSION),
                lcc.getCurrentUserId(activation),
                privilegeType,
				getObjectType(),
				sd.getSchemaName(),
				pso.getName());
		}

	} // end of genericCheck

}
