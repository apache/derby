/*

   Derby - Class org.apache.derby.iapi.sql.dictionary.StatementRoutinePermission

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

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.catalog.UUID;
import org.apache.derby.iapi.sql.conn.Authorizer;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.sql.dictionary.RoutinePermsDescriptor;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.execute.ExecPreparedStatement;
import org.apache.derby.iapi.sql.depend.DependencyManager;
import org.apache.derby.iapi.services.context.ContextManager;
/**
 * This class describes a routine execute permission
 * required by a statement.
 */

public final class StatementRoutinePermission extends StatementPermission
{
	private UUID routineUUID;

	public StatementRoutinePermission( UUID routineUUID)
	{
		this.routineUUID = routineUUID;
	}
									 
	/**
	 * Return routine UUID for this access descriptor
	 *
	 * @return	Routine UUID
	 */
	public UUID getRoutineUUID()
	{
		return routineUUID;
	}

	/**
	 * @see StatementPermission#check
	 */
	public void check( LanguageConnectionContext lcc,
					   String authorizationId,
					   boolean forGrant,
					   Activation activation) throws StandardException
	{
		DataDictionary dd = lcc.getDataDictionary();
		TransactionController tc = lcc.getTransactionExecute();
		ExecPreparedStatement ps = activation.getPreparedStatement();
		
		RoutinePermsDescriptor perms = dd.getRoutinePermissions( routineUUID, authorizationId);
		if( perms == null || ! perms.getHasExecutePermission())
			perms = dd.getRoutinePermissions(routineUUID, Authorizer.PUBLIC_AUTHORIZATION_ID);

		if (perms != null && perms.getHasExecutePermission()) {
			// The user or PUBLIC has execute permission, all is well.
			return;
		}

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
				(role, authorizationId, dbo);

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
					 role, true /* inverse relation*/);

				String r;
				while (!resolved && (r = rci.next()) != null) {
					perms = dd.
						getRoutinePermissions(routineUUID, r);

					if (perms != null &&
							perms.getHasExecutePermission()) {
						resolved = true;
					}
				}
			}

			if (resolved /* using a role*/) {
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

		if (!resolved) {
			AliasDescriptor ad = dd.getAliasDescriptor( routineUUID);

			if( ad == null)
				throw StandardException.newException(
					SQLState.AUTH_INTERNAL_BAD_UUID, "routine");

			SchemaDescriptor sd = dd.getSchemaDescriptor(
				ad.getSchemaUUID(), tc);

			if( sd == null)
				throw StandardException.newException(
					SQLState.AUTH_INTERNAL_BAD_UUID, "schema");

			throw StandardException.newException(
				(forGrant
				 ? SQLState.AUTH_NO_EXECUTE_PERMISSION_FOR_GRANT
				 : SQLState.AUTH_NO_EXECUTE_PERMISSION),
				authorizationId,
				ad.getDescriptorType(),
				sd.getSchemaName(),
				ad.getDescriptorName());
		}
	} // end of check

	/**
	 * @see StatementPermission#getPermissionDescriptor
	 */
	public PermissionsDescriptor getPermissionDescriptor(String authid, DataDictionary dd)
	throws StandardException
	{
		return dd.getRoutinePermissions(routineUUID,authid);
	}


	public String toString()
	{
		return "StatementRoutinePermission: " + routineUUID;
	}
}
