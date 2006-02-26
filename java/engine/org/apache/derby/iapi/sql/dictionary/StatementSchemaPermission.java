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
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;
import org.apache.derby.iapi.store.access.TransactionController;

/**
 * This class describes a schema permission used (required) by a statement.
 */

public class StatementSchemaPermission extends StatementPermission
{
	protected UUID schemaUUID;

	public StatementSchemaPermission(UUID schemaUUID)
	{
		this.schemaUUID = schemaUUID;
	}

	/**
	 * @param tc the TransactionController
	 * @param dd A DataDictionary
	 * @param authorizationId A user
	 * @param forGrant
	 *
	 * @exception StandardException if schema authorization not granted
	 */
	public void check(TransactionController tc,
					   DataDictionary dd,
					   String authorizationId,
					   boolean forGrant) throws StandardException
	{
		SchemaDescriptor sd = dd.getSchemaDescriptor(schemaUUID, tc);
		if (!authorizationId.equals(sd.getAuthorizationId()))
			throw StandardException.newException(SQLState.AUTH_NO_ACCESS_NOT_OWNER,
				 authorizationId, sd.getSchemaName());
	}
}
