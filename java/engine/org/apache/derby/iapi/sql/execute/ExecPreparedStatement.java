/*

   Derby - Class org.apache.derby.iapi.sql.execute.ExecPreparedStatement

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package org.apache.derby.iapi.sql.execute;

import org.apache.derby.iapi.services.loader.GeneratedClass;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;

import org.apache.derby.iapi.sql.PreparedStatement;

import java.util.List;
import org.apache.derby.iapi.sql.dictionary.StatementPermission;

/**
 * Execution extends prepared statement to add methods it needs
 * for execution purposes (that should not be on the Database API).
 *
 */
public interface ExecPreparedStatement 
	extends PreparedStatement {

	/**
	 * set the statement text
	 *
	 * @param txt the source text
	 */
	void setSource(String txt);

	/**
	 *	Get the Execution constants. This routine is called at Execution time.
	 *
	 *	@return	ConstantAction	The big structure enclosing the Execution constants.
	 */
	ConstantAction	getConstantAction( );

	/**
	 *	Get a saved object by number.  This is called during execution to
	 *  access objects created at compile time.  These are meant to be
	 *  read-only at run time.
	 *
	 *	@return	Object	A saved object.  The caller has to know what
	 *	it is requesting and cast it back to the expected type.
	 */
	Object	getSavedObject(int objectNum);

	/**
	 *	Get all the saved objects.  Used for stored prepared
	 * 	statements.
	 *
     *  @return a list with all the saved objects
	 */
    List<Object> getSavedObjects();

	/**
	 *	Get the saved cursor info.  Used for stored prepared
	 * 	statements.
	 *
	 *	@return	Object	the cursor info
	 */
	Object	getCursorInfo();

	/**
	 *  Get the class generated for this prepared statement.
	 *  Used to confirm compatability with auxilary structures.
	 *
	 * @exception StandardException on error obtaining class
	 *	(probably when a stored prepared statement is loading)
	 */
	GeneratedClass getActivationClass() throws StandardException;

    /**
     * <p>
     * Checks whether this PreparedStatement is up to date and its activation
     * class is identical to the supplied generated class. A call to {@code
     * upToDate(gc)} is supposed to perform the same work as the following code
     * in one atomic operation:
     * </p>
     *
     * <pre>
     * getActivationClass() == gc && upToDate()
     * </pre>
     *
     * @param gc a generated class that must be identical to {@code
     * getActivationClass()} for this method to return {@code true}
     * @return {@code true} if this statement is up to date and its activation
     * class is identical to {@code gc}, {@code false} otherwise
     * @see PreparedStatement#upToDate()
     * @see #getActivationClass()
     */
    boolean upToDate(GeneratedClass gc) throws StandardException;

	/**
	 *  Mark the statement as unusable, i.e. the system is
	 * finished with it and no one should be able to use it.
	 */
	void finish(LanguageConnectionContext lcc);

	/**
	 * Does this statement need a savpoint
	 *
	 * @return true if needs a savepoint
	 */
	boolean needsSavepoint();

	/**
	 * Get a new prepared statement that is a shallow copy
	 * of the current one.
	 *
	 * @return a new prepared statement
	 *
	 * @exception StandardException on error 
	 */
	public ExecPreparedStatement getClone() throws StandardException;

	/* Methods from old CursorPreparedStatement */

	/**
	 * the update mode of the cursor
	 *
	 * @return	The update mode of the cursor
	 */
	int	getUpdateMode();

	/**
	 * the target table of the cursor
	 *
	 * @return	target table of the cursor
	 */
	ExecCursorTableReference getTargetTable();

    /**
     * Check if this prepared statement has a cursor with columns that
     * can be updated.
     */
    boolean hasUpdateColumns();

    /**
     * Check if the specified column name is one of the update columns.
     */
    boolean isUpdateColumn(String columnName);

	/**
	 * set this prepared statement to be valid
	 */
	void setValid();

	/**
	 * Indicate that the statement represents an SPS action
	 */
	void setSPSAction();

	/**
	 * @return the list of permissions required to execute this statement. May be null if
	 *         the database does not use SQL standard authorization
	 */
    List<StatementPermission> getRequiredPermissionsList();

    // Methods for stale plan checking.

    /**
     * Increment and return the execution count for this statement.
     * @return execution count for this statement after the last compilation
     */
    int incrementExecutionCount();

    /**
     * Get the initial row count of the specified result set. If the initial
     * row count has not yet been set, initialize it with the value of the
     * current row count.
     * @param rsNum the result set to get the initial row count for
     * @param currentRowCount the current row count for the result set
     * @return the row count from the first execution of the result set
     */
    long getInitialRowCount(int rsNum, long currentRowCount);

    /**
     * Set the stale plan check interval.
     * @param interval the stale plan check interval
     */
    void setStalePlanCheckInterval(int interval);

    /**
     * Get the stale plan check interval.
     * @return the stale plan check interval, or zero if it has not been
     * initialized yet
     */
    int getStalePlanCheckInterval();
}
