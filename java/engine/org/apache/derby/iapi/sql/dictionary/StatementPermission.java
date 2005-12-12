/*

   Derby - Class org.apache.derby.iapi.sql.dictionary.StatementPermission

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

import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.iapi.error.StandardException;

/**
 * This class describes a permission used (required) by a statement.
 */

public abstract class StatementPermission
{
	/**
	 * @param tc the TransactionController
	 * @param dd A DataDictionary
	 * @param authorizationId A user
	 * @param forGrant
	 *
	 * @exception StandardException if the permission has not been granted
	 */
	public abstract void check( TransactionController tc,
								DataDictionary dd,
								String authorizationId,
								boolean forGrant) throws StandardException;
}
