/*

   Derby - Class org.apache.derby.impl.sql.execute.PrivilegeInfo

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

package org.apache.derby.impl.sql.execute;

import org.apache.derby.catalog.UUID;
import org.apache.derby.iapi.sql.dictionary.TupleDescriptor;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.iapi.error.StandardException;

import java.util.List;

public abstract class PrivilegeInfo
{

	/**
	 *	This is the guts of the Execution-time logic for GRANT/REVOKE
	 *
	 * @param activation
	 * @param grant true if grant, false if revoke
	 * @param grantees a list of authorization ids (strings)
	 *
	 * @exception StandardException		Thrown on failure
	 */
	abstract public void executeGrantRevoke( Activation activation,
											 boolean grant,
											 List grantees)
		throws StandardException;

	/**
	 * Determines whether a user is the owner of an object
	 * (table, function, or procedure).
	 *
	 * @param user
	 * @param objectDescriptor
	 * @param sd
	 * @param DataDictionary
	 *
	 * @exception StandardException if user does not own the object
	 */
	protected void checkOwnership( String user,
								   TupleDescriptor objectDescriptor,
								   SchemaDescriptor sd,
								   DataDictionary dd)
		throws StandardException
	{
		if (!user.equals(sd.getAuthorizationId()) &&
				!user.equals(dd.getAuthorizationDBA()))
			throw StandardException.newException(SQLState.AUTH_NOT_OWNER,
									  user,
									  objectDescriptor.getDescriptorType(),
									  sd.getSchemaName(),
									  objectDescriptor.getDescriptorName());
	}
}
