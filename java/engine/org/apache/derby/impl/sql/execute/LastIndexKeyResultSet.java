/*

   Derby - Class org.apache.derby.impl.sql.execute.LastIndexKeyResultSet

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

package org.apache.derby.impl.sql.execute;

import org.apache.derby.iapi.sql.execute.CursorResultSet;
import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.sql.ResultSet;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.ExecIndexRow;
import org.apache.derby.iapi.sql.execute.ExecutionContext;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;

import org.apache.derby.iapi.sql.Activation;

import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;

import org.apache.derby.iapi.store.access.ConglomerateController;
import org.apache.derby.iapi.store.access.GenericScanController;
import org.apache.derby.iapi.store.access.Qualifier;
import org.apache.derby.iapi.store.access.ScanController;
import org.apache.derby.iapi.store.access.TransactionController;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.services.stream.HeaderPrintWriter;

import org.apache.derby.iapi.services.loader.GeneratedMethod;

import org.apache.derby.iapi.services.io.FormatableBitSet;

import java.util.Properties;

/**
 * Return the last key in an index.  Used to perform
 * max().
 *
 * @author jamie
 */
public class LastIndexKeyResultSet extends NoPutResultSetImpl
{
	protected	ExecRow		candidate;

	// set in constructor and not altered during
	// life of object.
	protected long conglomId;
	protected GeneratedMethod resultRowAllocator;
	protected GeneratedMethod startKeyGetter;
	protected int startSearchOperator;
	protected GeneratedMethod stopKeyGetter;
	protected int stopSearchOperator;
	protected Qualifier[][] qualifiers;
	protected GeneratedMethod closeCleanup;
	public String tableName;
	public String indexName;
	protected boolean runTimeStatisticsOn;
	protected FormatableBitSet accessedCols;

	public int isolationLevel;
	public int lockMode;

	// Run time statistics
	public String stopPositionString;
	public boolean coarserLock;
	public boolean returnedRow;

	/**
	 * A last index key result set returns the last row from
	 * the index in question.  It is used as an ajunct to max().
	 *
	 * @param activation 		the activation for this result set,
	 *		which provides the context for the row allocation operation.
	 * @param resultSetNumber	The resultSetNumber for the ResultSet
	 * @param resultRowAllocator a reference to a method in the activation
	 * 						that creates a holder for the result row of the scan.  May
	 *						be a partial row.  <verbatim>
	 *		ExecRow rowAllocator() throws StandardException; </verbatim>
	 * @param conglomId 		the conglomerate of the table to be scanned.
	 * @param tableName			The full name of the table
	 * @param indexName			The name of the index, if one used to access table.
	 * @param colRefItem		An saved item for a bitSet of columns that
	 *							are referenced in the underlying table.  -1 if
	 *							no item.
	 * @param lockMode			The lock granularity to use (see
	 *							TransactionController in access)
	 * @param tableLocked		Whether or not the table is marked as using table locking
	 *							(in sys.systables)
	 * @param isolationLevel	Isolation level (specified or not) to use on scans
	 * @param optimizerEstimatedRowCount	Estimated total # of rows by
	 * 										optimizer
	 * @param optimizerEstimatedCost		Estimated total cost by optimizer
	 * @param closeCleanup		any cleanup the activation needs to do on close.
	 *
	 * @exception StandardException thrown when unable to create the
	 * 				result set
	 */
	public LastIndexKeyResultSet
	(
		Activation activation, 
		int	resultSetNumber,
		GeneratedMethod resultRowAllocator, 
		long conglomId, 
		String tableName,
		String indexName,
		int colRefItem,
		int lockMode,
		boolean tableLocked,
		int isolationLevel,
		double optimizerEstimatedRowCount,
		double optimizerEstimatedCost,
		GeneratedMethod closeCleanup
	) throws StandardException
	{
		super(activation,
				resultSetNumber,
				optimizerEstimatedRowCount,
				optimizerEstimatedCost);

		this.conglomId = conglomId;

		if (SanityManager.DEBUG) 
		{
			SanityManager.ASSERT( activation!=null, "this scan must get activation context");
			SanityManager.ASSERT( resultRowAllocator!= null, "this scan must get row allocator");
		}

		this.resultRowAllocator = resultRowAllocator;
		this.tableName = tableName;
		this.indexName = indexName;
		this.lockMode = lockMode;
		if (colRefItem != -1)
		{
			this.accessedCols = (FormatableBitSet)(activation.getPreparedStatement().
						getSavedObject(colRefItem));
		}
		/* Isolation level - translate from language to store */
		// If not specified, get current isolation level
		if (isolationLevel == ExecutionContext.UNSPECIFIED_ISOLATION_LEVEL)
		{
			isolationLevel = lcc.getCurrentIsolationLevel();
		}

        if (isolationLevel == ExecutionContext.SERIALIZABLE_ISOLATION_LEVEL)
        {
            this.isolationLevel = TransactionController.ISOLATION_SERIALIZABLE;
        }
        else
        {
            /* NOTE: always do row locking on READ COMMITTED/UNCOMMITTED 
             *       and repeatable read scans unless the table is marked as 
             *       table locked (in sys.systables).
             *
             *		 We always get instantaneous locks as we will complete
             *		 the scan before returning any rows and we will fully
             *		 requalify the row if we need to go to the heap on a next().
             */

            if (! tableLocked)
            {
                this.lockMode = TransactionController.MODE_RECORD;
            }

            if (isolationLevel == 
                    ExecutionContext.READ_COMMITTED_ISOLATION_LEVEL)
            {
                this.isolationLevel = 
                    TransactionController.ISOLATION_READ_COMMITTED_NOHOLDLOCK;
            }
            else if (isolationLevel == 
                        ExecutionContext.READ_UNCOMMITTED_ISOLATION_LEVEL)
            {
                this.isolationLevel = 
                    TransactionController.ISOLATION_READ_UNCOMMITTED;
            }
            else if (isolationLevel == 
                        ExecutionContext.REPEATABLE_READ_ISOLATION_LEVEL)
            {
                this.isolationLevel = 
                    TransactionController.ISOLATION_REPEATABLE_READ;
            }
        }

        if (SanityManager.DEBUG)
        {
            SanityManager.ASSERT(
                ((isolationLevel == 
                      ExecutionContext.READ_COMMITTED_ISOLATION_LEVEL)   ||
                 (isolationLevel == 
                      ExecutionContext.READ_UNCOMMITTED_ISOLATION_LEVEL) ||
                 (isolationLevel == 
                      ExecutionContext.REPEATABLE_READ_ISOLATION_LEVEL)  ||
                 (isolationLevel == 
                      ExecutionContext.SERIALIZABLE_ISOLATION_LEVEL)),

                "Invalid isolation level - " + isolationLevel);
        }

		this.closeCleanup = closeCleanup;

		runTimeStatisticsOn = getLanguageConnectionContext().getRunTimeStatisticsMode();

		/* Only call row allocators once */
		candidate = (ExecRow) resultRowAllocator.invoke(activation);
		constructorTime += getElapsedMillis(beginTime);

		/*
		** If scan tracing is turned on, print information about this
		** LastIndexKeyResultSet when it is first opened.  
		*/
		if (SanityManager.DEBUG)
		{
			if (SanityManager.DEBUG_ON("ScanTrace"))
			{
				//traceScanParameters();
			}
		}

		activation.informOfRowCount(this, 1);
    }

	/////////////////////////////////////////////////////
	// 
	// ResultSet interface (leftover from NoPutResultSet)
	// 
	/////////////////////////////////////////////////////

	/**
	* open a scan on the table. scan parameters are evaluated
	* at each open, so there is probably some way of altering
	* their values...
	 *
	 * @exception StandardException thrown on failure to open
	*/
	public void	openCore() throws StandardException
	{
		ExecRow candidateCopy = candidate.getClone();

		beginTime = getCurrentTimeMillis();
		if (SanityManager.DEBUG)
		{
		    SanityManager.ASSERT(!isOpen, "LastIndexKeyResultSet already open");
		}

		isOpen = true;
		TransactionController tc = activation.getTransactionController();

		/*
		** Grab the last row.  Note that if there are deletes
		** left lying around and no real row to return, then
		** the row array gets set even though the scan doesn't
		** return a row, so be careful to handle this correctly.
		*/
		if (tc.fetchMaxOnBtree(
					conglomId,  // conglomerate to open
					0, 			// open mode
					lockMode,
					isolationLevel,
					accessedCols,
					candidateCopy.getRowArray()))
		{
			currentRow =
			  getCompactRow(candidateCopy, accessedCols, (FormatableBitSet) null, true);
			setCurrentRow(currentRow);
		}
		else
		{
			currentRow = null;
		    clearCurrentRow();
		}
			
		numOpens++;
		openTime += getElapsedMillis(beginTime);
	}

	/**
	 * Return the next row (if any) from the scan (if open).
	 *
	 * @exception StandardException thrown on failure to get next row
	 */
	public ExecRow getNextRowCore() throws StandardException
	{
		if (returnedRow || !isOpen)
		{
			currentRow = null;
		    clearCurrentRow();
		} 
		else
		{
			returnedRow = true;
		}
		return currentRow;
	}

	/**
	 * If the result set has been opened,
	 * close the open scan.
	 * @exception StandardException thrown on failure to close
	 */
	public void	close() throws StandardException
	{
		beginTime = getCurrentTimeMillis();
		if (isOpen)
	    {
			isOpen = false;
			currentRow = null;
		    clearCurrentRow();
			if (closeCleanup != null) 
			{
				closeCleanup.invoke(activation); // let activation tidy up
			}

			super.close();
	    }
		else
		{
			if (SanityManager.DEBUG)
			{
				SanityManager.DEBUG("CloseRepeatInfo","Close of LastIndexKeyResultSet repeated");
			}
		}

		closeTime += getElapsedMillis(beginTime);
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
		long totTime = constructorTime + openTime + nextTime + closeTime;

		/* RESOLVE - subtract out store time later, when available */
		if (type == NoPutResultSet.CURRENT_RESULTSET_ONLY)
		{
			return	totTime;
		}
		else
		{
			return totTime;
		}
	}


	/**
	 * This result set has its row from the last fetch done. 
	 * If the cursor is closed, a null is returned.
	 *
	 * @see CursorResultSet
	 *
	 * @return the last row returned;
	 * @exception StandardException thrown on failure.
	 */
	public ExecRow getCurrentRow() throws StandardException 
	{
		return currentRow;
	}

	/**
	 * Print the parameters that constructed this result set to the
	 * trace stream.
	 */
/*
	private final void traceScanParameters()
	{
		if (SanityManager.DEBUG)
		{
			HeaderPrintWriter traceStream = SanityManager.GET_DEBUG_STREAM();

			traceStream.println("");
			traceStream.println("LastIndexKeyResultSet number " +
								resultSetNumber +
								" parameters:");

			traceStream.println("");
			traceStream.println("\tTable name: " + tableName);
			if (indexName != null)
			{
				traceStream.println("\tIndex name: " + indexName);
			}
			traceStream.println("");
		}
	}
*/

}
