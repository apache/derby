/*

   Derby - Class org.apache.derby.iapi.sql.dictionary.StatementRoutinePermission

   Copyright 2005 The Apache Software Foundation or its licensors, as applicable.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

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
	 * @see StatementPermission#check
	 */
	public void check( LanguageConnectionContext lcc,
					   String authorizationId,
					   boolean forGrant) throws StandardException
	{
		DataDictionary dd = lcc.getDataDictionary();
		TransactionController tc = lcc.getTransactionExecute();
		
		RoutinePermsDescriptor perms = dd.getRoutinePermissions( routineUUID, authorizationId);
		if( perms == null || ! perms.getHasExecutePermission())
			perms = dd.getRoutinePermissions(routineUUID, Authorizer.PUBLIC_AUTHORIZATION_ID);

		if( perms == null || ! perms.getHasExecutePermission())
		{
			AliasDescriptor ad = dd.getAliasDescriptor( routineUUID);
			if( ad == null)
				throw StandardException.newException( SQLState.AUTH_INTERNAL_BAD_UUID, "routine");
			SchemaDescriptor sd = dd.getSchemaDescriptor( ad.getSchemaUUID(), tc);
			if( sd == null)
				throw StandardException.newException( SQLState.AUTH_INTERNAL_BAD_UUID, "schema");
			throw StandardException.newException( forGrant ? SQLState.AUTH_NO_EXECUTE_PERMISSION_FOR_GRANT
												  : SQLState.AUTH_NO_EXECUTE_PERMISSION,
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
}
