/*

   Derby - Class org.apache.derby.impl.sql.execute.CurrentOfResultSet

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

import org.apache.derby.iapi.sql.execute.CursorResultSet;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.iapi.sql.execute.CursorActivation;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;

import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.ResultSet;
import org.apache.derby.iapi.sql.PreparedStatement;

import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;

import org.apache.derby.iapi.types.RowLocation;

import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.sql.depend.DependencyManager;

/**
 * Takes a cursor name and returns the current row
 * of the cursor; for use in generating the source
 * row and row location for positioned update/delete operations.
 * <p>
 * This result set returns only one row.
 *
 * @author ames
 */
public class CurrentOfResultSet extends NoPutResultSetImpl
	implements CursorResultSet {

    private boolean next;
	private RowLocation rowLocation;

	private CursorResultSet cursor;
	private CursorResultSet target;
	private ExecRow			sparseRow;

    // set in constructor and not altered during
    // life of object.
	private final String cursorName;
	private final String psName;

    //
    // class interface
    //
    public CurrentOfResultSet(String cursorName, Activation activation, 
							  int resultSetNumber, String psName)
	{
		super(activation, resultSetNumber, 0.0d, 0.0d);
		if (SanityManager.DEBUG)
			SanityManager.ASSERT( cursorName!=null, "current of scan must get cursor name");
        this.cursorName = cursorName;
		this.psName = psName;
    }

	//
	// ResultSet interface (leftover from NoPutResultSet)
	//
	/**
     * open a scan on the table. scan parameters are evaluated
     * at each open, so there is probably some way of altering
     * their values...
	 *
	 * @exception StandardException thrown on failure to open
     */
	public void	openCore() throws StandardException {
		if (SanityManager.DEBUG)
	    	SanityManager.ASSERT( ! isOpen, "CurrentOfResultSet already open");

		// get the cursor
		getCursor();

		next = false;
	    isOpen = true;
	}

	/**
     * If open and not returned yet, returns the row.
	 *
	 * @exception StandardException thrown on failure.
     */
	public ExecRow	getNextRowCore() throws StandardException {

		if ( isOpen ) {
	        if ( ! next ) {
	            next = true;
				if (SanityManager.DEBUG)
					SanityManager.ASSERT(! cursor.isClosed(), "cursor closed");

				ExecRow cursorRow = cursor.getCurrentRow();

				// requalify the current row
				if (cursorRow == null) {
				     throw StandardException.newException(SQLState.LANG_NO_CURRENT_ROW, cursorName);
				}
				// we know it will be requested, may as well get it now.
				rowLocation = cursor.getRowLocation();

				// get the row from the base table, which is the real result
				// row for the CurrentOfResultSet
				currentRow = target.getCurrentRow();
				if (SanityManager.DEBUG)
					SanityManager.ASSERT(currentRow != null,
									"No target row when there is a cursor row");

				/* beetle 3865: updateable cursor using index.  If underlying is a covering
				 * index, target is a TableScanRS (instead of a IndexRow2BaseRowRS) for the
				 * index scan.  But the problem is it returns a compact row in index key order.
				 * However the ProjectRestrictRS above us that sets up the old and new column
				 * values expects us to return a sparse row in heap order.  We have to do the
				 * wiring here, since we don't have IndexRow2BaseRowRS to do this work.  This
				 * problem was not exposed before, because we never used index scan for updateable
				 * cursors.
				 */
				if (target instanceof TableScanResultSet)
				{
					TableScanResultSet scan = (TableScanResultSet) target;
					if (scan.indexCols != null && currentRow != null)
						currentRow = getSparseRow(currentRow, scan.indexCols);
				}
				/* If we are updating rows from cached RIDs, we should compare with forward-most
				 * scan key when deciding whether to add RID to hash table or not.
				 */
				TableScanResultSet scan = (TableScanResultSet) activation.getForUpdateIndexScan();
				if (scan != null)
				{
					if (target instanceof IndexRowToBaseRowResultSet)
						scan.compareToLastKey = ((IndexRowToBaseRowResultSet) target).currentRowPrescanned;
					else if (target instanceof TableScanResultSet)
						scan.compareToLastKey = ((TableScanResultSet) target).currentRowPrescanned;
				}

				// REMIND: verify the row is still there
				// at present we get an ugly exception from the store,
				// Hopefully someday we can just do this:
				//
				// if (!rowLocation.rowExists())
				//     throw StandardException.newException(SQLState.LANG_NO_CURRENT_ROW, cursorName);
			}
			else {
				currentRow = null;
				rowLocation = null;
			}
	    }
		else {
			currentRow = null;
			rowLocation = null;
		}
		setCurrentRow(currentRow);
	    return currentRow;
	}

	/**
	 * Return a sparse heap row, based on a compact index row.
	 *
	 * @param row		compact referenced index row
	 * @param indexCols	base column positions of index keys, signed with asc/desc info
	 *
	 * @return			a sparse heap row with referenced columns
	 */
	private ExecRow getSparseRow(ExecRow row, int[] indexCols) throws StandardException
	{
		int colPos;
		if (sparseRow == null)
		{
			int numCols = 1;
			for (int i = 0; i < indexCols.length; i++)
			{
				colPos = (indexCols[i] > 0) ? indexCols[i] : -indexCols[i];
				if (colPos > numCols)
					numCols = colPos;
			}
			sparseRow = new ValueRow(numCols);
		}
		for (int i = 1; i <= indexCols.length; i++)
		{
			colPos = (indexCols[i-1] > 0) ? indexCols[i-1] : -indexCols[i-1];
			sparseRow.setColumn(colPos, row.getColumn(i));
		}

		return sparseRow;
	}

	/**
	 * If the result set has been opened,
	 * close the open scan.
	 *
	 * @exception StandardException thrown on error
	 */
	public void	close() throws StandardException
	{
	    if ( isOpen ) {
			// we don't want to keep around a pointer to the
			// row ... so it can be thrown away.
			// REVISIT: does this need to be in a finally
			// block, to ensure that it is executed?
	    	clearCurrentRow();
	        next = false;
			currentRow = null;

			super.close();
	    }
		else
			if (SanityManager.DEBUG)
				SanityManager.DEBUG("CloseRepeatInfo","Close of CurrentOfResultSet repeated");
	}
	public void	finish() throws StandardException
	{
		finishAndRTS();
	}

	/**
	 * Return the total amount of time spent in this ResultSet
	 *
	 * @param type	CURRENT_RESULTSET_ONLY - time spent only in this ResultSet
	 *				ENTIRE_RESULTSET_TREE  - time spent in this ResultSet and below.
	 *
	 * @return long		The total amount of time spent (in milliseconds).
	 */
	public long getTimeSpent(int type)
	{
		/* RESOLVE - RunTimeStats not implemented yet */
		return 0;
	}

	/**
	 * This result set has its row location from
	 * the last fetch done. If it is closed,
	 * a null is returned.
	 *
	 * @see CursorResultSet
	 *
	 * @return the row location of the current row.
	 * @exception StandardException thrown on failure to get row location
	 */
	public RowLocation getRowLocation()  {
		return rowLocation;
	}

	/**
	 * @see CursorResultSet
	 *
	 * @return the last row returned by getNextRow.
	 */
	public ExecRow getCurrentRow() {
		return currentRow;
	}

	//
	// class implementation
	//
	/**
		Because the positioned operation only gets one location
		per execution, and the cursor could be completely different
		for each execution (closed and reopened, perhaps), we 
		determine where caching the cursor could be applied.
		<p>
		When cached, we check if the cursor was closed'd, 
		and if so, throw it out and 
		see if there's one in the cache with our name. 

	 */
	private void getCursor() throws StandardException {

		// need to look again if cursor was closed
		if (cursor != null) {
			if (cursor.isClosed())
			{
				cursor = null;
				target = null;
			}
		}

		if (cursor == null) {

			LanguageConnectionContext lcc = getLanguageConnectionContext();

			CursorActivation cursorActivation = lcc.lookupCursorActivation(cursorName);

			if (cursorActivation != null)
			{
				// check we are compiled against the correct cursor
				PreparedStatement cps = cursorActivation.getPreparedStatement();

				if (psName.equals(cps.getObjectName())) {
					cursor = cursorActivation.getCursorResultSet();
					target = cursorActivation.getTargetResultSet();
					/* beetle 3865: updateable cursor using index. 2 way communication between
					 * update activation and cursor activation. Cursor passes index scan to
					 * update and update passes heap conglom controller to cursor.
					 */
					activation.setForUpdateIndexScan(cursorActivation.getForUpdateIndexScan());
					if (cursorActivation.getHeapConglomerateController() != null)
						cursorActivation.getHeapConglomerateController().close();
					cursorActivation.setHeapConglomerateController(activation.getHeapConglomerateController());
				} else {

					// our prepared statement is now invalid since there
					// exists another cursor with the same name but a different
					// statement.
					activation.getPreparedStatement().makeInvalid(DependencyManager.CHANGED_CURSOR, lcc);
				}
			}
		}

		if (cursor == null || cursor.isClosed()) {
			throw StandardException.newException(SQLState.LANG_CURSOR_CLOSED, cursorName);	
		}
	}

}
