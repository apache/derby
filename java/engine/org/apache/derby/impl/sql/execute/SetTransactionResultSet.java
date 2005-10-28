/*

   Derby - Class org.apache.derby.impl.sql.execute.SetTransactionResultSet

   Copyright 1998, 2004 The Apache Software Foundation or its licensors, as applicable.

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

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.Activation;

import org.apache.derby.iapi.services.sanity.SanityManager;


/**
 *	This is a wrapper class which invokes the Execution-time logic for
 *	SET TRANSACTION statements. The real Execution-time logic lives inside the
 *	executeConstantAction() method of the Execution constant.
 *
 *	@author Jerry Brenner
 */

class SetTransactionResultSet extends MiscResultSet
{
	/**
     * Construct a SetTransactionResultSet
	 *
	 *  @param activation		Describes run-time environment.
	 *
	 *  @exception StandardException Standard Cloudscape error policy.
     */
    SetTransactionResultSet(Activation activation)
		 throws StandardException
    {
		super(activation);
	}

	/**
	 * Does this ResultSet cause a commit or rollback.
	 *
	 * @return Whether or not this ResultSet cause a commit or rollback.
	 */
	public boolean doesCommit()
	{
		return true;
	}
}
