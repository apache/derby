/*

   Derby - Class org.apache.derby.impl.sql.compile.TransactionStatementNode

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


package	org.apache.derby.impl.sql.compile;

import org.apache.derby.iapi.services.context.ContextManager;

import org.apache.derby.iapi.services.sanity.SanityManager;

/**
 * A TransactionStatementNode represents any type of Transaction statement: 
 * SET TRANSACTION, COMMIT, and ROLLBACK.
 *
 * @author Ames Carlson
 */

public abstract class TransactionStatementNode extends StatementNode
{
	int activationKind()
	{
		   return StatementNode.NEED_NOTHING_ACTIVATION;
	}
	/**
	 * COMMIT and ROLLBACK are allowed to commit
	 * and rollback, duh.
	 *
	 * @return false 
	 */	
	public boolean isAtomic() 
	{
		return false;
	}

	/**
	 * Returns whether or not this Statement requires a set/clear savepoint
	 * around its execution.  The following statement "types" do not require them:
	 *		Cursor	- unnecessary and won't work in a read only environment
	 *		Xact	- savepoint will get blown away underneath us during commit/rollback
	 *
	 * @return boolean	Whether or not this Statement requires a set/clear savepoint
	 */
	public boolean needsSavepoint()
	{
		return false;
	}
}
