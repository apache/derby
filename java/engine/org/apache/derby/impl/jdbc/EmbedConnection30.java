/*

   Derby - Class org.apache.derby.impl.jdbc.EmbedConnection30

   Copyright 2001, 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derby.impl.jdbc;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.conn.StatementContext;

import org.apache.derby.impl.jdbc.EmbedConnection;
import org.apache.derby.impl.jdbc.Util;
import org.apache.derby.jdbc.Driver169;

import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.reference.DB2Limit;

import org.apache.derby.iapi.error.ExceptionSeverity;

import java.sql.Savepoint;
import java.sql.SQLException;

import java.util.Properties;
import java.util.Vector;


/**
 * This class extends the EmbedConnection20 class in order to support new
 * methods and classes that come with JDBC 3.0.
 *
 * @see org.apache.derby.impl.jdbc.EmbedConnection20
 *
 */
public class EmbedConnection30 extends EmbedConnection20
{

	//////////////////////////////////////////////////////////
	// CONSTRUCTORS
	//////////////////////////////////////////////////////////

	public EmbedConnection30(
			Driver169 driver,
			String url,
			Properties info)
		throws SQLException
	{
		super(driver, url, info);
	}

	public EmbedConnection30(
			EmbedConnection inputConnection)
	{
		super(inputConnection);
	}

 	/////////////////////////////////////////////////////////////////////////
	//
	//	JDBC 3.0	-	New public methods
	//
	/////////////////////////////////////////////////////////////////////////

	/**
	 * Creates an unnamed savepoint in the current transaction and
	 * returns the new Savepoint object that represents it.
	 *
	 *
	 * @return  The new Savepoint object
	 *
	 * @exception SQLException if a database access error occurs or
	 * this Connection object is currently in auto-commit mode
	 */
	public Savepoint setSavepoint()
		throws SQLException
	{
		return commonSetSavepointCode(null, false);
	}

	/**
	 * Creates a savepoint with the given name in the current transaction and
	 * returns the new Savepoint object that represents it.
	 *
	 *
	 * @param name  A String containing the name of the savepoint
	 *
	 * @return  The new Savepoint object
	 *
	 * @exception SQLException if a database access error occurs or
	 * this Connection object is currently in auto-commit mode
	 */
	public Savepoint setSavepoint(
			String name)
		throws SQLException
	{
		return commonSetSavepointCode(name, true);
	}

	/**
	 * Creates a savepoint with the given name(if it is a named savepoint else we will generate a name
	 * becuase Cloudscape only supports named savepoints internally) in the current transaction and
	 * returns the new Savepoint object that represents it.
	 *
	 * @param name  A String containing the name of the savepoint. Will be null if this is an unnamed savepoint
	 * @param userSuppliedSavepointName  If true means it's a named user defined savepoint.
	 *
	 * @return  The new Savepoint object
	 */
	private Savepoint commonSetSavepointCode(String name, boolean userSuppliedSavepointName) throws SQLException
	{
		synchronized (getConnectionSynchronization()) {
			setupContextStack();
			try {
				verifySavepointCommandIsAllowed();
				if (userSuppliedSavepointName && (name == null))//make sure that if it is a named savepoint then supplied name is not null
					throw newSQLException(SQLState.NULL_NAME_FOR_SAVEPOINT);
				//make sure that if it is a named savepoint then supplied name length is not > 128
				if (userSuppliedSavepointName && (name.length() > DB2Limit.DB2_MAX_IDENTIFIER_LENGTH128))
					throw newSQLException(SQLState.LANG_IDENTIFIER_TOO_LONG, name, String.valueOf(DB2Limit.DB2_MAX_IDENTIFIER_LENGTH128));
				if (userSuppliedSavepointName && name.startsWith("SYS")) //to enforce DB2 restriction which is savepoint name can't start with SYS
					throw newSQLException(SQLState.INVALID_SCHEMA_SYS, "SYS");
				Savepoint savePt = new EmbedSavepoint30(this, name);
				return savePt;
			} catch (StandardException e) {
				throw handleException(e);
			} finally {
			    restoreContextStack();
			}
		}
	}

	/**
	 * Undoes all changes made after the given Savepoint object was set.
	 * This method should be used only when auto-commit has been disabled.
	 *
	 *
	 * @param savepoint  The Savepoint object to rollback to
	 *
	 * @exception SQLException  if a database access error occurs,
	 * the Savepoint object is no longer valid, or this Connection
	 * object is currently in auto-commit mode
	 */
	public void rollback(
			Savepoint savepoint)
		throws SQLException
	{
		synchronized (getConnectionSynchronization()) {
			setupContextStack();
			try {
				verifySavepointCommandIsAllowed();
				verifySavepointArg(savepoint);
				//Need to cast and get the name because JDBC3 spec doesn't support names for
				//unnamed savepoints but Cloudscape keeps names for named & unnamed savepoints.
				getLanguageConnection().internalRollbackToSavepoint(((EmbedSavepoint30)savepoint).getInternalName(),true, savepoint);
			} catch (StandardException e) {
				throw handleException(e);
			} finally {
			    restoreContextStack();
			}
		}
	}

	/**
	 * Removes the given Savepoint object from the current transaction.
	 * Any reference to the savepoint after it has been removed will cause
	 * an SQLException to be thrown
	 *
	 *
	 * @param savepoint  The Savepoint object to be removed
	 *
	 * @exception SQLException  if a database access error occurs or the
	 * given Savepoint object is not a valid savepoint in the current transaction
	 */
	public void releaseSavepoint(
			Savepoint savepoint)
		throws SQLException
	{
		synchronized (getConnectionSynchronization()) {
			setupContextStack();
			try {
				verifySavepointCommandIsAllowed();
				verifySavepointArg(savepoint);
				//Need to cast and get the name because JDBC3 spec doesn't support names for
				//unnamed savepoints but Cloudscape keeps name for named & unnamed savepoints.
				getLanguageConnection().releaseSavePoint(((EmbedSavepoint30)savepoint).getInternalName(), savepoint);
			} catch (StandardException e) {
				throw handleException(e);
			} finally {
			    restoreContextStack();
			}
		}
	}

	// used by setSavepoint to check autocommit is false and not inside the trigger code
	private void verifySavepointCommandIsAllowed() throws SQLException
	{
		if (autoCommit)
			throw newSQLException(SQLState.NO_SAVEPOINT_WHEN_AUTO);

		//Bug 4507 - savepoint not allowed inside trigger
		StatementContext stmtCtxt = getLanguageConnection().getStatementContext();
		if (stmtCtxt!= null && stmtCtxt.inTrigger())
			throw newSQLException(SQLState.NO_SAVEPOINT_IN_TRIGGER);
	}

	// used by release/rollback to check savepoint argument
	private void verifySavepointArg(Savepoint savepoint) throws SQLException
	{
		//bug 4451 - Check for null savepoint
		EmbedSavepoint30 lsv = (EmbedSavepoint30) savepoint;
	    // bug 4451 need to throw error for null Savepoint
	    if (lsv == null)
		throw
		    Util.generateCsSQLException(SQLState.XACT_SAVEPOINT_NOT_FOUND, "null");

		//bug 4468 - verify that savepoint rollback is for a savepoint from the current
		// connection
		if (!lsv.sameConnection(this))
			throw newSQLException(SQLState.XACT_SAVEPOINT_RELEASE_ROLLBACK_FAIL);
      
		return;
	}
}
