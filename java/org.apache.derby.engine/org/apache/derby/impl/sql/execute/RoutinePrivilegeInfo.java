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
import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.iapi.sql.depend.DependencyManager;
import org.apache.derby.iapi.sql.dictionary.RoutinePermsDescriptor;
import org.apache.derby.iapi.sql.dictionary.AliasDescriptor;
import org.apache.derby.iapi.sql.dictionary.StatementRoutinePermission;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.DataDescriptorGenerator;
import org.apache.derby.shared.common.error.StandardException;

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
//IC see: https://issues.apache.org/jira/browse/DERBY-4551
        String currentUser = lcc.getCurrentUserId(activation);
		TransactionController tc = lcc.getTransactionExecute();

		// Check that the current user has permission to grant the privileges.
		checkOwnership( currentUser,
						aliasDescriptor,
//IC see: https://issues.apache.org/jira/browse/DERBY-464
						dd.getSchemaDescriptor( aliasDescriptor.getSchemaUUID(), tc),
						dd);
		
		DataDescriptorGenerator ddg = dd.getDataDescriptorGenerator();

		RoutinePermsDescriptor routinePermsDesc = ddg.newRoutinePermsDescriptor( aliasDescriptor, currentUser);

		dd.startWriting(lcc);
		for( Iterator itr = grantees.iterator(); itr.hasNext();)
		{
			// Keep track to see if any privileges are revoked by a revoke 
			// statement. If a privilege is not revoked, we need to raise a
			// warning.
			boolean privileges_revoked = false;
			String grantee = (String) itr.next();
//IC see: https://issues.apache.org/jira/browse/DERBY-1643
//IC see: https://issues.apache.org/jira/browse/DERBY-1582
			if (dd.addRemovePermissionsDescriptor( grant, routinePermsDesc, grantee, tc)) 
			{
				privileges_revoked = true;	
				//Derby currently supports only restrict form of revoke execute
				//privilege and that is why, we are sending invalidation action 
				//as REVOKE_PRIVILEGE_RESTRICT rather than REVOKE_PRIVILEGE
//IC see: https://issues.apache.org/jira/browse/DERBY-2594
				dd.getDependencyManager().invalidateFor
					(routinePermsDesc,
					 DependencyManager.REVOKE_PRIVILEGE_RESTRICT, lcc);

				// When revoking a privilege from a Routine we need to
				// invalidate all GPSs refering to it. But GPSs aren't
				// Dependents of RoutinePermsDescr, but of the
				// AliasDescriptor itself, so we must send
				// INTERNAL_RECOMPILE_REQUEST to the AliasDescriptor's
				// Dependents.
				dd.getDependencyManager().invalidateFor
					(aliasDescriptor,
					 DependencyManager.INTERNAL_RECOMPILE_REQUEST, lcc);
			}
			
			addWarningIfPrivilegeNotRevoked(activation, grant, privileges_revoked, grantee);
		}
	} // end of executeConstantAction
}
