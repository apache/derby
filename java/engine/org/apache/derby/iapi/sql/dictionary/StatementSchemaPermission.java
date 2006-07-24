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
import org.apache.derby.iapi.sql.conn.Authorizer;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.store.access.TransactionController;

/**
 * This class describes a schema permission required by a statement.
 */

public class StatementSchemaPermission extends StatementPermission
{
	private String schemaName;
	private String aid;
	private boolean privType;

	public StatementSchemaPermission(String schemaName, String aid, boolean privType)
	{
		this.schemaName = schemaName;
		this.aid 	= aid;
		this.privType	= privType;
	}

	/**
	 * @see StatementPermission#check
	 */
	public void check( LanguageConnectionContext lcc,
					   String authid,
					   boolean forGrant) throws StandardException
	{
		DataDictionary dd =	lcc.getDataDictionary();
		TransactionController tc = lcc.getTransactionExecute();
	
		if (privType == Authorizer.MODIFY_SCHEMA_PRIV)
		{
			SchemaDescriptor sd = dd.getSchemaDescriptor(schemaName, tc, false);
			// If schema hasn't been created already, no need to check
			if (sd == null)
				return;

			if (!authid.equals(sd.getAuthorizationId()))
				throw StandardException.newException(
					SQLState.AUTH_NO_ACCESS_NOT_OWNER, authid, schemaName);
		}
		else
		{
			// Non-DBA Users can only create schemas that match their authid
			// Also allow only DBA to set authid to another user
			// Note that for DBA, check interface wouldn't be called at all
			if (!schemaName.equals(authid) || (aid != null && !aid.equals(authid)))
				throw StandardException.newException(
					SQLState.AUTH_NOT_DATABASE_OWNER, authid, schemaName);
		}
	}

	/**
	 * Schema level permission is never required as list of privileges required
	 * for triggers/constraints/views and hence we don't do any work here, but
	 * simply return null
	 * 
	 * @see StatementPermission#check
	 */
	public PermissionsDescriptor getPermissionDescriptor(String authid, DataDictionary dd)
	throws StandardException
	{
		return null;
	}
}
