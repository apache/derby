/*

   Derby - Class org.apache.derby.iapi.sql.dictionary.StatementTablePermission

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
import org.apache.derby.shared.common.reference.SQLState;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.execute.ExecPreparedStatement;
import org.apache.derby.iapi.sql.depend.DependencyManager;
import org.apache.derby.iapi.services.context.ContextManager;

/**
 * This class describes a table permission required by a statement.
 */

public class StatementTablePermission extends StatementPermission
{
	UUID tableUUID;
	int privType; // One of Authorizer.SELECT_PRIV, UPDATE_PRIV, etc.

	/**
	 * Constructor for StatementTablePermission. Creates an instance of
	 * table permission requested for the given access.
	 * 
	 * @param tableUUID	UUID of the table
	 * @param privType	Access privilege requested
	 *
	 */
	public StatementTablePermission(UUID tableUUID, int privType)
	{
		this.tableUUID = tableUUID;
		this.privType = privType;
	}

	/**
	 * Return privilege access requested for this access descriptor
	 *
	 * @return	Privilege access
	 */
	public int getPrivType()
	{
		return privType;
	}

	/**
	 * Return table UUID for this access descriptor
	 *
	 * @return	Table UUID
	 */
	public UUID getTableUUID()
	{
		return tableUUID;
	}

	/**
	 * Routine to check if another instance of access descriptor matches this.
	 * Used to ensure only one access descriptor for a table of given privilege is created.
	 * Otherwise, every column reference from a table may create a descriptor for that table.
	 *
	 * @param obj	Another instance of StatementPermission
	 *
	 * @return	true if match
	 */
	public boolean equals( Object obj)
	{
		if( obj == null)
			return false;
		if( getClass().equals( obj.getClass()))
		{
			StatementTablePermission other = (StatementTablePermission) obj;
			return privType == other.privType && tableUUID.equals( other.tableUUID);
		}
		return false;
	} // end of equals

	/**
	 * Return hash code for this instance
	 *
	 * @return	Hashcode
	 *
	 */
	public int hashCode()
	{
		return privType + tableUUID.hashCode();
	}
	
	/**
	 * @see StatementPermission#check
	 */
	public void check( LanguageConnectionContext lcc,
					   boolean forGrant,
					   Activation activation)
		throws StandardException
	{
		ExecPreparedStatement ps = activation.getPreparedStatement();

        if (!hasPermissionOnTable(lcc, activation, forGrant, ps)) {
		    DataDictionary dd = lcc.getDataDictionary();
			TableDescriptor td = getTableDescriptor( dd);
            throw StandardException.newException(
                (forGrant ? SQLState.AUTH_NO_TABLE_PERMISSION_FOR_GRANT
                 : SQLState.AUTH_NO_TABLE_PERMISSION),
                lcc.getCurrentUserId(activation),
                getPrivName(),
                td.getSchemaName(),
                td.getName());
		}
	} // end of check

	protected TableDescriptor getTableDescriptor(DataDictionary dd)  throws StandardException
	{
		TableDescriptor td = dd.getTableDescriptor( tableUUID);
		if( td == null)
			throw StandardException.newException(SQLState.AUTH_INTERNAL_BAD_UUID, "table");
		return td;
	} // end of getTableDescriptor

	/**
	 * Check if current session has permission on the table (current user,
	 * PUBLIC or role) and, if applicable, register a dependency of ps on the
	 * current role.
	 *
	 * @param lcc the current language connection context
	 * @param activation the activation of ps
	 * @param forGrant true if FOR GRANT is required
	 * @param ps the prepared statement for which we are checking necessary
	 *        privileges
	 */
	protected boolean hasPermissionOnTable(LanguageConnectionContext lcc,
										   Activation activation,
										   boolean forGrant,
										   ExecPreparedStatement ps)
		throws StandardException
	{
		DataDictionary dd = lcc.getDataDictionary();
        String currentUserId = lcc.getCurrentUserId(activation);

		boolean result =
			oneAuthHasPermissionOnTable(dd,
										Authorizer.PUBLIC_AUTHORIZATION_ID,
										forGrant) ||
			oneAuthHasPermissionOnTable(dd,
                                        currentUserId,
										forGrant);
		if (!result) {
			// Since no permission exists for the current user or PUBLIC,
			// check if a permission exists for the current role (if set).
			String role = lcc.getCurrentRoleId(activation);

			if (role != null) {

				// Check that role is still granted to current user or
				// to PUBLIC: A revoked role which is current for this
				// session, is lazily set to none when it is attempted
				// used.
				String dbo = dd.getAuthorizationDatabaseOwner();
				RoleGrantDescriptor rd = dd.getRoleGrantDescriptor
                    (role, currentUserId, dbo);

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

					while (!result && (r = rci.next()) != null) {
						result = oneAuthHasPermissionOnTable
							(dd, r, forGrant);
					}

					if (result) {
						// Also add a dependency on the role (qua provider), so
						// that if role is no longer available to the current
						// user (e.g. grant is revoked, role is dropped,
						// another role has been set), we are able to
						// invalidate the ps or activation (the latter is used
						// if the current role changes).
						DependencyManager dm = dd.getDependencyManager();
						RoleGrantDescriptor rgd =
							dd.getRoleDefinitionDescriptor(role);
						ContextManager cm = lcc.getContextManager();

						dm.addDependency(ps, rgd, cm);
						dm.addDependency(activation, rgd, cm);
					}
				}
			}
		}
		return result;
	}

	protected boolean oneAuthHasPermissionOnTable(DataDictionary dd, String authorizationId, boolean forGrant)
		throws StandardException
	{
		TablePermsDescriptor perms = dd.getTablePermissions( tableUUID, authorizationId);
		if( perms == null)
			return false;
		
		String priv = null;
			
		switch( privType)
		{
		case Authorizer.SELECT_PRIV:
		case Authorizer.MIN_SELECT_PRIV:
			priv = perms.getSelectPriv();
			break;
		case Authorizer.UPDATE_PRIV:
			priv = perms.getUpdatePriv();
			break;
		case Authorizer.REFERENCES_PRIV:
			priv = perms.getReferencesPriv();
			break;
		case Authorizer.INSERT_PRIV:
			priv = perms.getInsertPriv();
			break;
		case Authorizer.DELETE_PRIV:
			priv = perms.getDeletePriv();
			break;
		case Authorizer.TRIGGER_PRIV:
			priv = perms.getTriggerPriv();
			break;
		}

		return "Y".equals(priv) || (!forGrant) && "y".equals( priv);
	} // end of hasPermissionOnTable

	/**
	 * @see StatementPermission#getPermissionDescriptor
	 */
	public PermissionsDescriptor getPermissionDescriptor(String authid, DataDictionary dd)
	throws StandardException
	{
		//if the required type of privilege exists for the given authorizer,
		//then pass the permission descriptor for it.
		if (oneAuthHasPermissionOnTable( dd, authid, false))
			return dd.getTablePermissions(tableUUID, authid);
		else return null;
	}

	/**
	 * Return privilege needed for this access as string
	 *
	 * @return	privilege string
	 */
	public String getPrivName( )
	{
		switch( privType)
		{
		case Authorizer.SELECT_PRIV:
		case Authorizer.MIN_SELECT_PRIV:
			return "SELECT";
		case Authorizer.UPDATE_PRIV:
			return "UPDATE";
		case Authorizer.REFERENCES_PRIV:
			return "REFERENCES";
		case Authorizer.INSERT_PRIV:
			return "INSERT";
		case Authorizer.DELETE_PRIV:
			return "DELETE";
		case Authorizer.TRIGGER_PRIV:
			return "TRIGGER";
		}
		return "?";
	} // end of getPrivName

	public String toString()
	{
		return "StatementTablePermission: " + getPrivName() + " " + tableUUID;
	}
}
