/*

   Derby - Class org.apache.derby.impl.sql.execute.SavepointConstantAction

   Copyright 2003, 2004 The Apache Software Foundation or its licensors, as applicable.

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

import org.apache.derby.iapi.sql.execute.ConstantAction;

import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;

import org.apache.derby.iapi.sql.Activation;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.sql.conn.StatementContext;
import org.apache.derby.iapi.reference.SQLState;

/**
 *	This class  describes actions that are ALWAYS performed for a
 *	Savepoint (rollback, release and set savepoint) Statement at Execution time.
 */

class SavepointConstantAction extends DDLConstantAction
{

	private final String savepointName; //name of the savepoint
	private final int	savepointStatementType; //Type of savepoint statement ie rollback, release or set savepoint

	/**
	 *	Make the ConstantAction for a set savepoint, rollback or release statement.
	 *
	 *  @param savepointName	Name of the savepoint.
	 *  @param statementType	set savepoint, rollback savepoint or release savepoint
	 */
	SavepointConstantAction(
								String			savepointName,
								int				savepointStatementType)
	{
		this.savepointName = savepointName;
		this.savepointStatementType = savepointStatementType;
	}

	// OBJECT METHODS
	public	String	toString()
	{
		if (savepointStatementType == 1)
			return constructToString("SAVEPOINT ", savepointName + " ON ROLLBACK RETAIN CURSORS ON ROLLBACK RETAIN LOCKS");
		else if (savepointStatementType == 2)
			return constructToString("ROLLBACK WORK TO SAVEPOINT ", savepointName);
		else
			return constructToString("RELEASE TO SAVEPOINT ", savepointName);
	}

	// INTERFACE METHODS


	/**
	 *	This is the guts of the Execution-time logic for CREATE TABLE.
	 *
	 *	@see ConstantAction#executeConstantAction
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public void	executeConstantAction( Activation activation )
		throws StandardException
	{
		LanguageConnectionContext lcc = activation.getLanguageConnectionContext();

			//Bug 4507 - savepoint not allowed inside trigger
			StatementContext stmtCtxt = lcc.getStatementContext();
			if (stmtCtxt!= null && stmtCtxt.inTrigger())
				throw StandardException.newException(SQLState.NO_SAVEPOINT_IN_TRIGGER);

		if (savepointStatementType == 1) { //this is set savepoint
			if (savepointName.startsWith("SYS")) //to enforce DB2 restriction which is savepoint name can't start with SYS
				throw StandardException.newException(SQLState.INVALID_SCHEMA_SYS, "SYS");
			lcc.languageSetSavePoint(savepointName, savepointName);
		} else if (savepointStatementType == 2) { //this is rollback savepoint
			lcc.internalRollbackToSavepoint(savepointName,true, savepointName);
		} else { //this is release savepoint
			lcc.releaseSavePoint(savepointName, savepointName);
		}
	}

}
