/*

   Derby - Class org.apache.derby.impl.sql.execute.RoutinePrivilegeInfo

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

import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.iapi.sql.depend.DependencyManager;
import org.apache.derby.iapi.sql.dictionary.RoutinePermsDescriptor;
import org.apache.derby.iapi.sql.dictionary.AliasDescriptor;
import org.apache.derby.iapi.sql.dictionary.StatementRoutinePermission;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.DataDescriptorGenerator;
import org.apache.derby.iapi.error.StandardException;

import java.util.Iterator;
import java.util.List;

public class RoutinePrivilegeInfo extends PrivilegeInfo
{
	private AliasDescriptor aliasDescriptor;

	public RoutinePrivilegeInfo( AliasDescriptor aliasDescriptor)
	{
		this.aliasDescriptor = aliasDescriptor;
	}
	
	/**
	 *	This is the guts of the Execution-time logic for GRANT/REVOKE of a routine execute privilege
	 *
	 * @param activation
	 * @param grant true if grant, false if revoke
	 * @param grantees a list of authorization ids (strings)
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public void executeGrantRevoke( Activation activation,
									boolean grant,
									List grantees)
		throws StandardException
	{
		// Check that the current user has permission to grant the privileges.
		LanguageConnectionContext lcc = activation.getLanguageConnectionContext();
		DataDictionary dd = lcc.getDataDictionary();
		String currentUser = lcc.getAuthorizationId();
		TransactionController tc = lcc.getTransactionExecute();

		// Check that the current user has permission to grant the privileges.
		checkOwnership( currentUser,
						aliasDescriptor,
						dd.getSchemaDescriptor( aliasDescriptor.getSchemaUUID(), tc),
						dd);
		
		DataDescriptorGenerator ddg = dd.getDataDescriptorGenerator();

		RoutinePermsDescriptor routinePermsDesc = ddg.newRoutinePermsDescriptor( aliasDescriptor, currentUser);

		dd.startWriting(lcc);
		for( Iterator itr = grantees.iterator(); itr.hasNext();)
		{
			String grantee = (String) itr.next();
			if (dd.addRemovePermissionsDescriptor( grant, routinePermsDesc, grantee, tc))
				//Derby currently supports only restrict form of revoke execute
				//privilege and that is why, we are sending invalidation action 
				//as REVOKE_PRIVILEGE_RESTRICT rather than REVOKE_PRIVILEGE
        		dd.getDependencyManager().invalidateFor(routinePermsDesc, DependencyManager.REVOKE_PRIVILEGE_RESTRICT, lcc);

		}
	} // end of executeConstantAction
}
