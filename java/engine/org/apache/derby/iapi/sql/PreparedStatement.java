/*

   Derby - Class org.apache.derby.iapi.sql.PreparedStatement

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

package org.apache.derby.iapi.sql;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;

import org.apache.derby.iapi.sql.depend.Dependent;
import org.apache.derby.iapi.sql.depend.Provider;

import org.apache.derby.iapi.types.DataTypeDescriptor;
import java.sql.Timestamp;
import java.sql.SQLWarning;

/**
 * The PreparedStatement interface provides methods to execute prepared
 * statements, store them, and get metadata about them.
 *
 *	@author Jeff Lichtman
 */
public interface PreparedStatement
	extends Dependent, Provider
{

	/**
	 * Checks whether this PreparedStatement is up to date.
	 * A PreparedStatement can become out of date if any of several
	 * things happen:
	 *
	 *	A schema used by the statement is dropped
	 *	A table used by the statement is dropped
	 *	A table used by the statement, or a column in such a table,
	 *		is altered in one of several ways: a column is dropped,
	 *		a privilege is dropped, a constraint is added or
	 *		dropped, an index is dropped.
	 *	A view used by the statement is dropped.
	 *
	 * In general, anything that happened since the plan was generated
	 * that might cause the plan to fail, or to generate incorrect results,
	 * will cause this method to return FALSE.
	 *
	 * @return	TRUE if the PreparedStatement is up to date,
	 *		FALSE if it is not up to date
	 */
	boolean	upToDate() throws StandardException;

	/**
	 * Re-prepare the statement if it is not up to date or,
	 * if requested, simply not optimal.
	 * If there are open cursors using this prepared statement,
	 * then we will not be able to recompile the statement.
	 *
	 * @param lcc			The LanguageConnectionContext.
	 *
	 * @exception StandardException thrown if unable to perform
	 */
	void rePrepare(LanguageConnectionContext lcc) 
		throws StandardException;

	/**
	 * PreparedStatements are re-entrant - that is, more than one
	 * execution can be active at a time for a single prepared statement.
	 * An Activation contains all the local state information to
	 * execute a prepared statement (as opposed to the constant
	 * information, such as literal values and code). Each Activation
	 * class contains the code specific to the prepared statement
	 * represented by an instance of this class (PreparedStatement).
	 *
	 * @param lcc			The LanguageConnectionContext.
	 * @return	The new activation.
	 *
	 * @exception StandardException		Thrown on failure
	 */
	Activation	getActivation(LanguageConnectionContext lcc, boolean scrollable) throws StandardException;

	/**
	 * Execute the PreparedStatement and return results.
	 *<p>
	 * There is no executeQuery() or
	 * executeUpdate(); a method is provided in
	 * ResultSet to tell whether to expect rows to be returned.
	 *
	 * @param activation The activation containing all the local state
	 *		to execute the plan.
	 * @param executeQuery		Whether or not called from a Statement.executeQuery()
	 * @param executeUpdate	Whether or not called from a Statement.executeUpdate()
 	 * @param rollbackParentContext True if 1) the statement context is
	 *  NOT a top-level context, AND 2) in the event of a statement-level
	 *	 exception, the parent context needs to be rolled back, too.
	 *
	 * @return	A ResultSet for a statement. A ResultSet represents
	 *		the results returned from the statement, if any.
	 *		Will return NULL if the plan for the PreparedStatement
	 *		has aged out of cache, or the plan is out of date.
	 *
	 * @exception StandardException		Thrown on failure
	 */
	ResultSet	execute(Activation activation, boolean executeQuery, boolean executeUpdate,
		boolean rollbackParentContext) throws StandardException;

	/**
		Simple form of execute(). Creates a new single use activation and executes it,
		but also passes rollbackParentContext parameter (see above).
	*/
	ResultSet	execute(LanguageConnectionContext lcc, boolean rollbackParentContext)
		throws StandardException;

	/**
	 * Get the ResultDescription for the statement.  The ResultDescription
	 * describes what the results look like: what are the rows and columns?
	 * <p>
	 * This is available here and on the ResultSet so that users can
	 * see the shape of the result before they execute.
	 *
	 * @return	A ResultDescription describing the results.
	 *
	 */
	ResultDescription	getResultDescription();

	/**
	 * Return true if the query node for this statement references SESSION schema tables.
	 *
	 * @return	true if references SESSION schema tables, else false
	 */
	boolean referencesSessionSchema();

	/**
	 * Get an array of DataTypeDescriptors describing the types of the
	 * parameters of this PreparedStatement. The Nth element of the array
	 * describes the Nth parameter.
	 *
	 * @return		An array of DataTypeDescriptors telling the
	 *			type, length, precision, scale, etc. of each
	 *			parameter of this PreparedStatement.
	 */
	DataTypeDescriptor[]	getParameterTypes();

	/**
	 *	Return the SQL string that this statement is for.
	 *
	 *	@return the SQL string this statement is for.
	 */
	String getSource();

	/**
	 *	Return the SPS Name for this statement.
	 *
	 *	@return the SPS Name for this statement
	 */
	String getSPSName();

	/**
	 * Get the total compile time for the associated query in milliseconds.
	 * Compile time can be divided into parse, bind, optimize and generate times.
	 * 
	 * @return long		The total compile time for the associated query in milliseconds.
	 */
	public long getCompileTimeInMillis();

	/**
	 * Get the parse time for the associated query in milliseconds.
	 * 
	 * @return long		The parse time for the associated query in milliseconds.
	 */
	public long getParseTimeInMillis();

	/**
	 * Get the bind time for the associated query in milliseconds.
	 * 
	 * @return long		The bind time for the associated query in milliseconds.
	 */
	public long getBindTimeInMillis();

	/**
	 * Get the optimize time for the associated query in milliseconds.
	 * 
	 * @return long		The optimize time for the associated query in milliseconds.
	 */
	public long getOptimizeTimeInMillis();

	/**
	 * Get the generate time for the associated query in milliseconds.
	 * 
	 * @return long		The generate time for the associated query in milliseconds.
	 */
	public long getGenerateTimeInMillis();

	/**
	 * Get the timestamp for the beginning of compilation
	 *
	 * @return Timestamp	The timestamp for the beginning of compilation.
	 */
	public Timestamp getBeginCompileTimestamp();

	/**
	 * Get the timestamp for the end of compilation
	 *
	 * @return Timestamp	The timestamp for the end of compilation.
	 */
	public Timestamp getEndCompileTimestamp();

	/**
	 * Returns whether or not this Statement requires should
	 * behave atomically -- i.e. whether a user is permitted
	 * to do a commit/rollback during the execution of this
	 * statement.
	 *
	 * @return boolean	Whether or not this Statement is atomic
	 */
	boolean isAtomic();

	/**
		Return any compile time warnings. Null if no warnings exist.
	*/
	public SQLWarning getCompileTimeWarnings();

}
