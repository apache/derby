/*

   Derby - Class org.apache.derby.iapi.db.TriggerExecutionContext

   Copyright 1999, 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derby.iapi.db;

import java.sql.ResultSet;
import java.sql.SQLException;
import org.apache.derby.catalog.UUID;

/**
 * A trigger execution context holds information that is
 * available from the context of a trigger invocation.
 * 
 * <P>
 * <I>IBM Corp. reserves the right to change, rename, or
 * remove this interface at any time.</I>
 */
public interface TriggerExecutionContext
{
	/**
	 * Return value from </I>getEventType()</I> for
	 * an update trigger.
	 */ 
	public static final int UPDATE_EVENT	= 1;

	/**
	 * Return value from </I>getEventType()</I> for
	 * a delete trigger.
	 */ 
	public static final int DELETE_EVENT	= 2;

	/**
	 * Return value from </I>getEventType()</I> for
	 * an insert trigger.
	 */ 
	public static final int INSERT_EVENT	= 3;


	/**
	 * Get the target table name upon which the 
	 * trigger event is declared.
	 *
	 * @return the target table
	 */
	public String getTargetTableName();

	/**
	 * Get the target table UUID upon which the 
	 * trigger event is declared.
	 *
	 * @return the uuid of the target table
	 */
	public UUID getTargetTableId();

	/**
	 * Get the type for the event that caused the
	 * trigger to fire.
	 *
	 * @return the event type (e.g. UPDATE_EVENT)
	 */
	public int getEventType();

	/**
	 * Get the text of the statement that caused the
	 * trigger to fire.
	 *
	 * @return the statement text.
	 */
	public String getEventStatementText();

	/**
	 * Get the columns that have been modified by the statement
	 * that caused this trigger to fire.  If all columns are
	 * modified, will return null (e.g. for INSERT or DELETE
	 * return null).
	 *
	 * @return an array of Strings
	 */
	public String[] getModifiedColumns();

	/**
	 * Find out if a column was changed, by column name.
	 *
	 * @param columnName the column to check
 	 *
	 * @return true if the column was modified by this statement.
	 * Note that this will always return true for INSERT
	 * and DELETE regardless of the column name passed in.
	 */
	public boolean wasColumnModified(String columnName);

	/**
	 * Find out if a column was changed, by column number
	 *
	 * @param columnNumber the column to check
 	 *
	 * @return true if the column was modified by this statement.
	 * Note that this will always return true for INSERT
	 * and DELETE regardless of the column name passed in.
	 */
	public boolean wasColumnModified(int columnNumber);

	/**
	 * Returns a result set of the old (before) images of the changed rows.
	 * For a row trigger, this result set will have a single row.  For
	 * a statement trigger, this result set has every row that has
	 * changed or will change.  If a statement trigger does not affect 
	 * a row, then the result set will be empty (i.e. ResultSet.next()
	 * will return false).
	 * <p>
	 * Will return null if the call is inapplicable for the trigger
	 * that is currently executing.  For example, will return null if called
	 * during a the firing of an INSERT trigger.
	 *
	 * @return the ResultSet containing before images of the rows 
	 * changed by the triggering event.  May return null.
	 *
	 * @exception SQLException if called after the triggering event has
	 * completed
	 */
	public ResultSet getOldRowSet() throws SQLException;

	/**
	 * Returns a result set of the new (after) images of the changed rows.
	 * For a row trigger, this result set will have a single row.  For
	 * a statement trigger, this result set has every row that has
	 * changed or will change.  If a statement trigger does not affect 
	 * a row, then the result set will be empty (i.e. ResultSet.next()
	 * will return false).
	 * <p>
	 * Will return null if the call is inapplicable for the trigger
	 * that is currently executing.  For example, will return null if 
	 * called during the firing of a DELETE trigger.
	 *
	 * @return the ResultSet containing after images of the rows 
	 * changed by the triggering event.  May return null.
	 *
	 * @exception SQLException if called after the triggering event has
	 * completed
	 */
	public ResultSet getNewRowSet() throws SQLException;

	/**
	 * Like getOldRowSet(), but returns a result set positioned
	 * on the first row of the before (old) result set.  Used as a convenience
	 * to get a column for a row trigger.  Equivalent to getOldRowSet()
	 * followed by next().
	 * <p>
	 * Will return null if the call is inapplicable for the trigger
	 * that is currently executing.  For example, will return null if called
	 * during a the firing of an INSERT trigger.
	 *
	 * @return the ResultSet positioned on the old row image.  May
	 * return null.
	 *
	 * @exception SQLException if called after the triggering event has
	 * completed
	 */
	public ResultSet getOldRow() throws SQLException;

	/**
	 * Like getNewRowSet(), but returns a result set positioned
	 * on the first row of the after (new) result set.  Used as a convenience
	 * to get a column for a row trigger.  Equivalent to getNewRowSet()
	 * followed by next().
	 * <p>
	 * Will return null if the call is inapplicable for the trigger
	 * that is currently executing.  For example, will return null if 
	 * called during the firing of a DELETE trigger.
	 *
	 * @return the ResultSet positioned on the new row image.  May
	 * return null.
	 *
	 * @exception SQLException if called after the triggering event has
	 * completed
	 */
	public ResultSet getNewRow() throws SQLException;
}
