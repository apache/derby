/*

   Derby - Class org.apache.derby.iapi.sql.conn.StatementContext

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

package org.apache.derby.iapi.sql.conn;

import org.apache.derby.iapi.services.context.Context;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.sql.execute.NoPutResultSet;

import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.PreparedStatement;
import org.apache.derby.iapi.sql.ResultSet;
import org.apache.derby.iapi.sql.ParameterValueSet;

import org.apache.derby.iapi.sql.depend.Dependency;

import org.apache.derby.iapi.types.DataValueFactory;
import org.apache.derby.iapi.sql.LanguageFactory;

/**
 * StatementContext keeps the context for a statement.
 */
public interface StatementContext extends Context {

	/**
	 * Mark this context as being in use.
	 *
	 *	@param	parentInTrigger	true if the parent started in the context of a trigger
	 *	@param	isAtomic true if the statement must be executed
	 *		atomically
	 *  @param stmtText the text of the statement.  Needed for any language
	 * 	statement (currently, for any statement that can cause a trigger
	 * 	to fire).  Please set this unless you are some funky jdbc setXXX
	 *	method or something.
	 *	@param	pvs	parameter value set, if it has one
	 */
	public void setInUse(boolean inTrigger, boolean isAtomic, String stmtText, ParameterValueSet pvs);

	/**
	 * Mark this context as not in use.  This is important because we
	 * always leave the top statement context on the stack, and we don't
	 * want to clean it up if a statement level exception happens while the
	 * context is not in use.
	 */
	public void clearInUse();

	/**
	 * Set a save point for the current statement.
	 * NOTE: This needs to be off of the StatementContext so that it gets
	 * cleared on a statement error.
	 *
	 * @exception StandardException Thrown on error
	 */
	public void setSavePoint() throws StandardException;

	/**
	 * If this statement context has a savepoint, then
	 * it is reset to the current point.  Otherwise, it
	 * is a noop.
	 *
	 * @exception StandardException Thrown on error
	 */
	public void resetSavePoint() throws StandardException;

	/**
	 * Clear the save point for the current statement.
	 *
	 * @exception StandardException Thrown on error
	 */
	public void clearSavePoint() throws StandardException;

	/**
	 * Set the top ResultSet in the ResultSet tree for close down on
	 * an error.
	 *
	 * @param topResultSet			The top ResultSet in the ResultSet tree
	 * @param subqueryTrackingArray	(Sparse) of tops of subquery ResultSet trees
	 *
	 * @exception StandardException Thrown on error
	 * @return Nothing.
	 */
	public void setTopResultSet(ResultSet topResultSet,
								NoPutResultSet[] subqueryTrackingArray)
		 throws StandardException;

	/**
	 * Set the appropriate entry in the subquery tracking array for
	 * the specified subquery.
	 * Useful for closing down open subqueries on an exception.
	 *
	 * @param subqueryNumber	The subquery # for this subquery
	 * @param subqueryResultSet	The NoPutResultSet at the top of the subquery
	 * @param numSubqueries		The total # of subqueries in the entire query
	 *
	 * @return Nothing.
	 * @exception StandardException Thrown on error
	 */
	public void setSubqueryResultSet(int subqueryNumber,
									 NoPutResultSet subqueryResultSet,
									 int numSubqueries)
		throws StandardException;

	/**
	 * Get the subquery tracking array for this query.
	 * (Useful for runtime statistics.)
	 *
	 * @return NoPutResultSet[]	The	(sparse) array of tops of subquery ResultSet trees
	 * @exception StandardException Thrown on error
	 */
	public NoPutResultSet[] getSubqueryTrackingArray()
		throws StandardException;


	/**
	 * Track a Dependency within this StatementContext.
	 * (We need to clear any dependencies added within this
	 * context on an error.
	 *
	 * @param dy	The dependency to track.
	 *
	 * @return Nothing.
	 * @exception StandardException Thrown on error
	 */
	public void addDependency(Dependency dy)
		throws StandardException;

	/**
	  *	Reports whether this StatementContext is on the context stack.
	  *
	  *	@return	true if this StatementContext is on the context stack. false otherwise.
	  */
	public	boolean	onStack();

	/**
	 * Returns whether we started from within the context of a trigger
	 * or not.
	 *
	 * @return	true if we are in a trigger context
	 */
	public	boolean	inTrigger();
	
	/**
	 * Indicates whether the statement needs to be executed atomically
	 * or not, i.e., whether a commit/rollback is permitted by a
 	 * connection nested in this statement.
	 *
	 * @return true if needs to be atomic
	 */
	public boolean isAtomic();

	/**
	 * Is this statement context in use or not.
	 *
	 * @return true if in use
	 */
	public boolean inUse();
	
	/**
	 * Return the text of the current statement.
	 * Note that this may be null.  It is currently
	 * not set up correctly for ResultSets that aren't
	 * single row result sets (e.g SELECT)
	 * and setXXXX/getXXXX jdbc methods.
	 *
	 * @return the statement text
	 */
	public String getStatementText();

	/**
		Set the level of SQL allowed in this and subsequent
		nested statements due to a routine call. Value must be one of
		RoutineAliasInfo.{MODIFIES_SQL_DATA, READS_SQL_DATA, CONTAINS_SQL, NO_SQL}

		@param force set to true to override more restrictive setting. Used to
		reset the permissions after a function call.

	*/
	public void setSQLAllowed(short allow, boolean force);

	/**
		Get the setting of the SQL allowed state.
	*/
	public short getSQLAllowed();


	/**
		Set to indicate statement is system code.
		For example a system procedure, view, function etc.
	*/
	public void setSystemCode();

	/**
		Return true if this statement is system code.
	*/
	public boolean getSystemCode();

	/**
		Indicate that, in the event of a statement-level exception,
		this context is NOT the last one that needs to be rolled
		back--rather, it is nested within some other statement
		context, and that other context needs to be rolled back,
		too.
	*/
	public void setParentRollback();

}
