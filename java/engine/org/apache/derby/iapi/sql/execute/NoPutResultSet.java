/*

   Derby - Class org.apache.derby.iapi.sql.execute.NoPutResultSet

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

package org.apache.derby.iapi.sql.execute;

import org.apache.derby.iapi.sql.ResultSet;
import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.types.RowLocation;
import org.apache.derby.iapi.store.access.RowLocationRetRowSource;

/**
 * The NoPutResultSet interface is used to provide additional
 * operations on result sets that can be used in returning rows
 * up a ResultSet tree.
 * <p>
 * Since the ResulSet operations must also be supported by
 * NoPutResultSets, we extend that interface here as well.
 *
 * @author jerry
 */
public interface NoPutResultSet extends ResultSet, RowLocationRetRowSource 
{
	// method names for use with SQLState.LANG_RESULT_SET_NOT_OPEN exception

	public	static	final	String	ABSOLUTE		=	"absolute";
	public	static	final	String	RELATIVE		=	"relative";
	public	static	final	String	FIRST			=	"first";
	public	static	final	String	NEXT			=	"next";
	public	static	final	String	LAST			=	"last";
	public	static	final	String	PREVIOUS		=	"previous";

	/**
	 * Mark the ResultSet as the topmost one in the ResultSet tree.
	 * Useful for closing down the ResultSet on an error.
	 *
	 * @return Nothing.
	 */
	public void markAsTopResultSet();

	/**
	 * open a scan on the table. scan parameters are evaluated
	 * at each open, so there is probably some way of altering
	 * their values...
	 * <p>
	 * openCore() can only be called on a closed result
	 * set.  see reopenCore if you want to reuse an open
	 * result set.
	 *
	 * @exception StandardException thrown if cursor finished.
	 */
	public void openCore() throws StandardException;

	/**
     * reopen the scan.  behaves like openCore() but is 
	 * optimized where appropriate (e.g. where scanController
	 * has special logic for us).  
	 * <p>
	 * used by joiners
	 * <p>
	 * scan parameters are evaluated
     * at each open, so there is probably some way of altering
     * their values...  
	 *
	 * @exception StandardException thrown if cursor finished.
     */
	public void reopenCore() throws StandardException;

	/**
     * Return the requested values computed
     * from the next row (if any) for which
     * the restriction evaluates to true.
     * <p>
     * restriction and projection parameters
     * are evaluated for each row.
	 *
	 * @exception StandardException thrown on failure.
	 *
	 * @return the next row in the result
	 */
	public ExecRow	getNextRowCore() throws StandardException;

	/**
	 * Return the point of attachment for this subquery.
	 * (Only meaningful for Any and Once ResultSets, which can and will only
	 * be at the top of a ResultSet for a subquery.)
	 *
	 * @return int	Point of attachment (result set number) for this
	 *			    subquery.  (-1 if not a subquery - also Sanity violation)
	 */
	public int getPointOfAttachment();

	/**
	 * Return the isolation level of the scan in the result set.
	 * Only expected to be called for those ResultSets that
	 * contain a scan.
	 *
	 * @return The isolation level of the scan (in TransactionController constants).
	 */
	public int getScanIsolationLevel();

	/**
	 * Notify a NPRS that it is the source for the specified 
	 * TargetResultSet.  This is useful when doing bulk insert.
	 *
	 * @param trs	The TargetResultSet.
	 *
	 * @return Nothing.
	 */
	public void setTargetResultSet(TargetResultSet trs);

	/**
	 * Set whether or not the NPRS need the row location when acting
	 * as a row source.  (The target result set determines this.)
	 * 
	 * @param boolean needsRowLocation
	 *
	 * @return Nothing.
	 */
	public void setNeedsRowLocation(boolean needsRowLocation);

	/**
	 * Get the estimated row count from this result set.
	 *
	 * @return	The estimated row count (as a double) from this result set.
	 */
	public double getEstimatedRowCount();

	/**
	 * Get the number of this ResultSet, which is guaranteed to be unique
	 * within a statement.
	 */
	public int resultSetNumber();

	/**
	 * Set the current row to the row passed in.
	 *
	 * @param row the new current row
	 *
	 */
	public void setCurrentRow(ExecRow row);

	/**
	 * Clear the current row
	 *
	 */
	public void clearCurrentRow();

	/**
	 * Do we need to relock the row when going to the heap.
	 *
	 * @return Whether or not we need to relock the row when going to the heap.
	 */

	public boolean requiresRelocking();
	
	/**
	 * Is this ResultSet or it's source result set for update
	 *
	 * @return Whether or not the result set is for update.
	 */
	public boolean isForUpdate();

}
