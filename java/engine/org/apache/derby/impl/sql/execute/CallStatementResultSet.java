/*

   Derby - Class org.apache.derby.impl.sql.execute.CallStatementResultSet

   Copyright 1997, 2004 The Apache Software Foundation or its licensors, as applicable.

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

import org.apache.derby.iapi.sql.execute.ExecutionContext;

import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.ResultSet;
import org.apache.derby.iapi.sql.ResultDescription;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;

import org.apache.derby.iapi.services.loader.GeneratedMethod;

import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.iapi.store.access.ConglomerateController;

import org.apache.derby.iapi.services.sanity.SanityManager;

/**
 * Call the specified expression, ignoring the return, if any.
 *
 * @author jerry
 */
class CallStatementResultSet extends NoRowsResultSetImpl
{

	private final GeneratedMethod methodCall;

    /*
     * class interface
     *
     */
    CallStatementResultSet(
				GeneratedMethod methodCall,
				Activation a) 
			throws StandardException
    {
		super(a);
		this.methodCall = methodCall;
	}

	/**
		@exception StandardException Standard Cloudscape error policy
	*/
	public void open() throws StandardException
	{
		methodCall.invoke(activation);
		close();
    }

	/**
	 * @see ResultSet#cleanUp
	 */
	public void	cleanUp() 
	{
		/* Nothing to do */
	}
}
