/*

   Derby - Class org.apache.derby.iapi.sql.ResultSet

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

import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;
import org.apache.derby.iapi.sql.Row;

import java.sql.Timestamp;
import java.sql.SQLWarning;

/**
 * The ResultSet interface provides a method to tell whether a statement
 * returns rows, and if so, a method to get the rows. It also provides a
 * method to get metadata about the contents of the rows. It also provide
 * a method to accept rows as input.
 * <p>
 * There is no single implementation of the ResultSet interface. Instead,
 * the various support operations involved in executing statements
 * implement this interface.
 * <p>
 * Although ExecRow is used on the interface, it is not available to
 * users of the API. They should use Row, the exposed super-interface
 * of ExecRow.  <<I couldn't find another way to perform this mapping...>>
 * <p>
 * Valid transitions: <ul>
 * <li> open->close</li>
 * <li> close->open</li>
 * <li> close->finished</li>
 * <li> finished->open</li>
 * </ul>
 *
 * @author Jeff Lichtman
 */

public interface ResultSet
{
	/* Get time only spent in this ResultSet */
	public static final int CURRENT_RESULTSET_ONLY = 0;
	/* Get time spent in this ResultSet and below */
	public static final int ENTIRE_RESULTSET_TREE = 1;

	// cursor check positioning
	public static final int ISBEFOREFIRST = 101;
	public static final int ISFIRST = 102;
	public static final int ISLAST = 103;
	public static final int ISAFTERLAST = 104;

	/**
	 * Returns TRUE if the statement returns rows (i.e. is a SELECT
	 * or FETCH statement), FALSE if it returns no rows.
	 *
	 * @return	TRUE if the statement returns rows, FALSE if not.
	 */
	 boolean	returnsRows();

	/**
	 * Returns the number of rows affected by the statement.
	   Only valid of returnsRows() returns false.
	 * For other DML statements, it returns the number of rows
	 * modified by the statement. For statements that do not affect rows
	 * (like DDL statements), it returns zero.
	 *
	 * @return	The number of rows affect by the statement, so far.
	 */
	int	modifiedRowCount();

	/**
	 * Returns a ResultDescription object, which describes the results
	 * of the statement this ResultSet is in. This will *not* be a
	 * description of this particular ResultSet, if this is not the
	 * outermost ResultSet.
	 *
	 * @return	A ResultDescription describing the results of the
	 *		statement.
	 */
	ResultDescription	getResultDescription();

	/**
	 * Needs to be called before the result set will do anything.
	 * Need to call before getNextRow(), or for a result set
	 * that doesn't return rows, this is the call that will
	 * cause all the work to be done.
	 *
	 * @return	Nothing
	 *
	 * @exception StandardException		Thrown on failure
	 */
	void	open() throws StandardException;

	/**
	 * Returns the row at the absolute position from the query, 
	 * and returns NULL when there is no such position.
	 * (Negative position means from the end of the result set.)
	 * Moving the cursor to an invalid position leaves the cursor
	 * positioned either before the first row (negative position)
	 * or after the last row (positive position).
	 * NOTE: An exception will be thrown on 0.
	 *
	 * @param row	The position.
	 * @return	The row at the absolute position, or NULL if no such position.
	 *
	 * @exception StandardException		Thrown on failure
	 * @see Row
	 */
	ExecRow	getAbsoluteRow(int row) throws StandardException;

	/**
	 * Returns the row at the relative position from the current
	 * cursor position, and returns NULL when there is no such position.
	 * (Negative position means toward the beginning of the result set.)
	 * Moving the cursor to an invalid position leaves the cursor
	 * positioned either before the first row (negative position)
	 * or after the last row (positive position).
	 * NOTE: 0 is valid.
	 * NOTE: An exception is thrown if the cursor is not currently
	 * positioned on a row.
	 *
	 * @param row	The position.
	 * @return	The row at the relative position, or NULL if no such position.
	 *
	 * @exception StandardException		Thrown on failure
	 * @see Row
	 */
	ExecRow	getRelativeRow(int row) throws StandardException;

	/**
	 * Sets the current position to before the first row and returns NULL
	 * because there is no current row.
	 *
	 * @return	NULL.
	 *
	 * @exception StandardException		Thrown on failure
	 * @see Row
	 */
	ExecRow	setBeforeFirstRow() throws StandardException;

	/**
	 * Returns the first row from the query, and returns NULL when there
	 * are no rows.
	 *
	 * @return	The first row, or NULL if no rows.
	 *
	 * @exception StandardException		Thrown on failure
	 * @see Row
	 */
	ExecRow	getFirstRow() throws StandardException;

	/**
	 * Returns the next row from the query, and returns NULL when there
	 * are no more rows.
	 *
	 * @return	The next row, or NULL if no more rows.
	 *
	 * @exception StandardException		Thrown on failure
	 * @see Row
	 */
	ExecRow	getNextRow() throws StandardException;

	/**
	 * Returns the previous row from the query, and returns NULL when there
	 * are no more previous rows.
	 *
	 * @return	The previous row, or NULL if no more previous rows.
	 *
	 * @exception StandardException		Thrown on failure
	 * @see Row
	 */
	ExecRow	getPreviousRow() throws StandardException;

	/**
	 * Returns the last row from the query, and returns NULL when there
	 * are no rows.
	 *
	 * @return	The last row, or NULL if no rows.
	 *
	 * @exception StandardException		Thrown on failure
	 * @see Row
	 */
	ExecRow	getLastRow() throws StandardException;

	/**
	 * Sets the current position to after the last row and returns NULL
	 * because there is no current row.
	 *
	 * @return	NULL.
	 *
	 * @exception StandardException		Thrown on failure
	 * @see Row
	 */
	ExecRow	setAfterLastRow() throws StandardException;

    /**
		Determine if the result set is at one of the positions
		according to the constants above (ISBEFOREFIRST etc).
		Only valid and called for scrollable cursors.
     * @return true if at the requested position.
	 * @exception StandardException Thrown on error.
     */
    public boolean checkRowPosition(int isType) throws StandardException;

	/**
	 * Returns the row number of the current row.  Row
	 * numbers start from 1 and go to 'n'.  Corresponds
	 * to row numbering used to position current row
	 * in the result set (as per JDBC).

		Only valid and called for scrollable cursors.
	 * @return	the row number, or 0 if not on a row
	 *
	 */
	int	getRowNumber();

	/**
	 * Tells the system that there will be no more calls to getNextRow()
	 * (until the next open() call), so it can free up the resources
	 * associated with the ResultSet.
	 *
	 * @return	Nothing
	 *
	 * @exception StandardException		Thrown on error.
	 */
	void	close() throws StandardException;

	/**
	 * Tells the system to clean up on an error.
	 *
	 * @return	Nothing
	 *
	 * @exception StandardException		Thrown on error.
	 */
	void	cleanUp() throws StandardException;

	/**
		Find out if the ResultSet is closed or not.
		Will report true for result sets that do not return rows.

		@return true if the ResultSet has been closed.
	 */
	boolean isClosed();

	/**
	 * Tells the system that there will be no more access
	 * to any database information via this result set;
	 * in particular, no more calls to open().
	 * Will close the result set if it is not already closed.
	 *
	 * @return	Nothing
	 * @exception StandardException	on error
	 */
	void	finish() throws StandardException;

	/**
	 * Get the execution time in milliseconds.
	 *
	 * @return long		The execution time in milliseconds.
	 */
	public long getExecuteTime();

	/**
	 * Get the Timestamp for the beginning of execution.
	 *
	 * @return Timestamp		The Timestamp for the beginning of execution.
	 */
	public Timestamp getBeginExecutionTimestamp();

	/**
	 * Get the Timestamp for the end of execution.
	 *
	 * @return Timestamp		The Timestamp for the end of execution.
	 */
	public Timestamp getEndExecutionTimestamp();

	/**
	 * Return the total amount of time spent in this ResultSet
	 *
	 * @param type	CURRENT_RESULTSET_ONLY - time spent only in this ResultSet
	 *				ENTIRE_RESULTSET_TREE  - time spent in this ResultSet and below.
	 *
	 * @return long		The total amount of time spent (in milliseconds).
	 */
	public long getTimeSpent(int type);	

	/**
	 * Get the subquery ResultSet tracking array from the top ResultSet.
	 * (Used for tracking open subqueries when closing down on an error.)
	 *
	 * @param numSubqueries		The size of the array (For allocation on demand.)
	 *
	 * @return NoPutResultSet[]	Array of NoPutResultSets for subqueries.
	 */
	public NoPutResultSet[] getSubqueryTrackingArray(int numSubqueries);

	/**
	 * ResultSet for rowss inserted into the table (contains auto-generated keys columns only)
	 *
	 * @return NoPutResultSet	NoPutResultSets for rows inserted into the table.
	 */
	public ResultSet getAutoGeneratedKeysResultset();

	/**
	 * Returns the name of the cursor, if this is cursor statement of some
	 * type (declare, open, fetch, positioned update, positioned delete,
	 * close).
	 *
	 * @return	A String with the name of the cursor, if any. Returns
	 *		NULL if this is not a cursor statement.
	 */
	public String	getCursorName();

	/**
		Return the set of warnings generated during the execution of
		this result set. The warnings are cleared once this call returns.
	*/
	public SQLWarning getWarnings();
}
