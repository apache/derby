/*

   Derby - Class org.apache.derby.impl.sql.execute.BulkTableScanResultSet

   Copyright 1998, 2004 The Apache Software Foundation or its licensors, as applicable.

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

import org.apache.derby.iapi.types.RowLocation;
import org.apache.derby.iapi.types.DataValueDescriptor;

import org.apache.derby.iapi.sql.ResultSet;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.ExecIndexRow;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;

import org.apache.derby.iapi.sql.Activation;

import org.apache.derby.iapi.store.access.GroupFetchScanController;
import org.apache.derby.iapi.store.access.Qualifier;
import org.apache.derby.iapi.store.access.ScanController;
import org.apache.derby.iapi.store.access.StaticCompiledOpenConglomInfo;
import org.apache.derby.iapi.store.access.TransactionController;

import org.apache.derby.iapi.types.RowLocation;

import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.services.loader.GeneratedMethod;

import org.apache.derby.iapi.services.io.FormatableBitSet;

/**
 * Read a base table or index in bulk.  Most of the
 * work for this method is inherited from TableScanResultSet.
 * This class overrides getNextRowCore (and extends 
 * re/openCore) to use a row array and fetch rows
 * from the Store in bulk  (using fetchNextGroup).
 * <p>
 * Since it retrieves rows in bulk, locking is not
 * as is usual -- locks may have already been released
 * on rows as they are returned to the user.  Hence,
 * this ResultSet is not suitable for a query running
 * Isolation Level 1, cursor stability.
 * <p>
 * Note that this code is only accessable from an
 * optimizer override.  If it makes sense to have the
 * optimizer select bulk reads, then this should
 * probably be rolled into TableScanResultSet.
 *
 * @author jamie
 */
public class BulkTableScanResultSet extends TableScanResultSet
	implements CursorResultSet
{
	private DataValueDescriptor[][] rowArray;
	private int curRowPosition;
	private int numRowsInArray;

	private static int OUT_OF_ROWS = 0;

    /**
 	 * Constructor.  Just save off the rowsPerRead argument
	 * and pass everything else down to TableScanResultSet
	 * 
	 * @see org.apache.derby.iapi.sql.execute.ResultSetFactory#getBulkTableScanResultSet
	 *
	 * @exception StandardException thrown on failure to open
	 */
    public BulkTableScanResultSet(long conglomId,
		StaticCompiledOpenConglomInfo scoci, Activation activation, 
		GeneratedMethod resultRowAllocator, 
		int resultSetNumber,
		GeneratedMethod startKeyGetter, int startSearchOperator,
		GeneratedMethod stopKeyGetter, int stopSearchOperator,
		boolean sameStartStopPosition,
		Qualifier[][] qualifiers,
		String tableName,
		String indexName,
		boolean isConstraint,
		boolean forUpdate,
		int colRefItem,
		int indexColItem,
		int lockMode,
		boolean tableLocked,
		int isolationLevel,
		int rowsPerRead,
		boolean oneRowScan,
		double optimizerEstimatedRowCount,
		double optimizerEstimatedCost,
		GeneratedMethod closeCleanup)
			throws StandardException
    {
		super(conglomId,
			scoci,
			activation,
			resultRowAllocator,
			resultSetNumber,
			startKeyGetter,
			startSearchOperator,
			stopKeyGetter,
			stopSearchOperator,
			sameStartStopPosition,
			qualifiers,
			tableName,
			indexName,
			isConstraint,
			forUpdate,
			colRefItem,
			indexColItem,
			lockMode,
			tableLocked,
			isolationLevel,
			rowsPerRead,
			oneRowScan,
			optimizerEstimatedRowCount,
			optimizerEstimatedCost,
			closeCleanup);

		if (SanityManager.DEBUG)
		{
			/* Bulk fetch of size 1 is same as a regular table scan
			 * and is supposed to be detected at compile time.
			 */
			if (rowsPerRead == 1)
			{
				SanityManager.THROWASSERT(
					"rowsPerRead not expected to be 1");
			}
			/* Bulk table scan implies that scan is not
			 * a 1 row scan.
			 */
			if (oneRowScan)
			{
				SanityManager.THROWASSERT(
					"oneRowScan expected to be false - " +
					"rowsPerRead = " + rowsPerRead);
			}
		}
    }

    /**
 	 * Open the scan controller
	 *
	 * @param transaction controller will open one if null
     *
	 * @exception StandardException thrown on failure to open
	 */
	protected void openScanController(TransactionController tc)
		throws StandardException
	{
		DataValueDescriptor[] startPositionRow = startPosition == null ? null : startPosition.getRowArray();
		DataValueDescriptor[] stopPositionRow = stopPosition == null ? null : stopPosition.getRowArray();

		// Clear the Qualifiers's Orderable cache 
		if (qualifiers != null)
		{
			clearOrderableCache(qualifiers);
		}

		// Get the current transaction controller
		if (tc == null)
			tc = activation.getTransactionController();
		scanController = tc.openCompiledScan(
				activation.getResultSetHoldability(),
				(forUpdate ? TransactionController.OPENMODE_FORUPDATE : 0),
                lockMode,
                isolationLevel,
				accessedCols,
				startPositionRow,
					// not used when giving null start position
				startSearchOperator,
				qualifiers,
				stopPositionRow,
					// not used when giving null stop position
				stopSearchOperator,
				scoci,
				dcoci);

		/* Remember that we opened the scan */
		scanControllerOpened = true;

		rowsThisScan = 0;

		/*
		** Inform the activation of the estimated number of rows.  Only
		** do it here, not in reopen, so that we don't do this costly
		** check too often.
		*/
		activation.informOfRowCount(
									this,
									scanController.getEstimatedRowCount()
									);
	}

	/**
	 * Open up the result set.  Delegate
	 * most work to TableScanResultSet.openCore().
	 * Create a new array with <rowsPerRead> rows
	 * for use in fetchNextGroup().
	 *
	 * @exception StandardException thrown on failure to open
	 */
	public void openCore() throws StandardException
	{
		super.openCore();
		/*
		** Add the extra time we spent after
		** the super class -- TableScanResultSet()
		** already added up its time in openCore().
		*/
		beginTime = getCurrentTimeMillis();
		rowArray = new DataValueDescriptor[rowsPerRead][];

		// we only allocate the first row -- the
		// store clones as needed for the rest
		// of the rows
		rowArray[0] = candidate.getRowArrayClone();
		numRowsInArray = 0;
		curRowPosition = -1;
		
		openTime += getElapsedMillis(beginTime);
	}

	/**
	 * Reopen the result set.  Delegate
	 * most work to TableScanResultSet.reopenCore().
	 * Reuse the array of rows.
	 *
	 * @exception StandardException thrown on failure to open
	 */
	public void reopenCore() throws StandardException
	{
		super.reopenCore();
		numRowsInArray = 0;
		curRowPosition = -1;
	}
		
	/**
	 * Return the next row (if any) from the scan (if open).
	 * Reload the rowArray as necessary.
	 *
	 * @exception StandardException thrown on failure to get next row
	 */
	public ExecRow getNextRowCore() throws StandardException
	{
	    ExecRow result = null;

		beginTime = getCurrentTimeMillis();
		if (isOpen && scanControllerOpened)
		{
			if (currentRow == null)
			{
				currentRow =
					getCompactRow(candidate,
									accessedCols,
									(FormatableBitSet) null,
									isKeyed);
			}

outer:		for (;;)
			{
				if (curRowPosition >= numRowsInArray - 1)
				{
					if (reloadArray() == OUT_OF_ROWS)
					{
						setCurrentRow(null);
						setRowCountIfPossible(rowsThisScan);
						return null;
					}
				}	

				while (++curRowPosition < numRowsInArray)
				{
					candidate.setRowArray(rowArray[curRowPosition]);
					currentRow = setCompactRow(candidate, currentRow);
					rowsSeen++;
					rowsThisScan++;

					/*
					** Skip rows where there are start or stop positioners
					** that do not implement ordered null semantics and
					** there are columns in those positions that contain
					** null.
					*/
					if (skipRow(candidate))
					{
						rowsFiltered++;
						continue;
					}

					result = currentRow;
					break outer;
				}
			}
		}

		setCurrentRow(result);
		nextTime += getElapsedMillis(beginTime);
	    return result;
	}

	/*
	** Load up rowArray with a batch of
	** rows.
	*/
	private int reloadArray() throws StandardException
	{
		curRowPosition = -1;
		numRowsInArray =
				((GroupFetchScanController) scanController).fetchNextGroup(
                                               rowArray, (RowLocation[]) null);

		return numRowsInArray;

	}
	/**
	 * If the result set has been opened,
	 * close the open scan.  Delegate most
	 * of the work to TableScanResultSet.
	 *
	 * @exception StandardException on error
	 */
	public void	close() throws StandardException
	{
		/*
		** We'll let TableScanResultSet track
		** the time it takes to close up, so
		** no timing here.
		*/
		super.close();
		numRowsInArray = -1;
		curRowPosition = -1;
		rowArray = null;
	}

	/**
	 * Can we get instantaneous locks when getting share row
	 * locks at READ COMMITTED.
	 */
	protected boolean canGetInstantaneousLocks()
	{
		return true;
	}

	/**
	 * @see NoPutResultSet#requiresRelocking
	 */
	public boolean requiresRelocking()
	{
		// IndexRowToBaseRow needs to relock if we didn't keep the lock
		return(
          isolationLevel == TransactionController.ISOLATION_READ_COMMITTED   ||
          isolationLevel == TransactionController.ISOLATION_READ_COMMITTED_NOHOLDLOCK ||
          isolationLevel == TransactionController.ISOLATION_READ_UNCOMMITTED);
	}
}
