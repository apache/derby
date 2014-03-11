/*

   Derby - Class org.apache.derby.impl.sql.execute.TableScanResultSet

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

package org.apache.derby.impl.sql.execute;

import java.util.Properties;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.services.i18n.MessageService;
import org.apache.derby.iapi.services.loader.GeneratedMethod;
import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.execute.CursorResultSet;
import org.apache.derby.iapi.sql.execute.ExecIndexRow;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;
import org.apache.derby.iapi.store.access.BackingStoreHashtable;
import org.apache.derby.iapi.store.access.ConglomerateController;
import org.apache.derby.iapi.store.access.DynamicCompiledOpenConglomInfo;
import org.apache.derby.iapi.store.access.Qualifier;
import org.apache.derby.iapi.store.access.ScanController;
import org.apache.derby.iapi.store.access.StaticCompiledOpenConglomInfo;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.RowLocation;

/**
 * Takes a table and a table filter and returns
 * the table's rows satisfying the filter as a result set.
 *
 * There are several things we could do during object
 * construction that are done in the open & next calls, to
 * improve performance.
 *
 */
class TableScanResultSet extends ScanResultSet
	implements CursorResultSet, Cloneable
{
    protected ScanController scanController;
	protected boolean		scanControllerOpened;
	protected boolean		isKeyed;
	protected boolean		firstScan = true;
	protected ExecIndexRow	startPosition;
	protected ExecIndexRow	stopPosition;

    // set in constructor and not altered during
    // life of object.
	protected long conglomId;
    protected DynamicCompiledOpenConglomInfo dcoci;
    protected StaticCompiledOpenConglomInfo scoci;
	protected GeneratedMethod startKeyGetter;
	protected int startSearchOperator;
	protected GeneratedMethod stopKeyGetter;
	protected int stopSearchOperator;
	public    Qualifier[][] qualifiers;
	public String userSuppliedOptimizerOverrides;
	protected boolean runTimeStatisticsOn;
    protected int[] indexCols;		//index keys base column position array
	public int rowsPerRead;
	public boolean forUpdate;
	final boolean sameStartStopPosition;
    protected boolean nextDone;
	private RowLocation rlTemplate;

	// Run time statistics
	private Properties scanProperties;
	public String startPositionString;
	public String stopPositionString;
	public boolean isConstraint;
	public boolean coarserLock;
	public boolean oneRowScan;

	protected long	rowsThisScan;

	private long estimatedRowCount;

    /**
     * This field is used by beetle 3865, updateable cursor using index. It
     * is a hash table containing updated rows that are thrown into future
     * direction of the index scan, and as a result we'll hit it again but
     * should skip it. The hash table will spill to disk if it grows too big
     * to be kept in memory.
     */
    protected BackingStoreHashtable past2FutureTbl;

	// For Scrollable insensitive updatable result sets, only qualify a row the 
	// first time it's been read, since an update can change a row so that it 
	// no longer qualifies
    protected boolean qualify;

	// currentRowIsValid is set to the result of positioning at a rowLocation.
	// It will be true if the positioning was successful and false if the row 
	// was deleted under our feet. Whenenver currentRowIsValid is false it means 
	// that the row has been deleted.
    protected boolean currentRowIsValid;
	
	// Indicates whether the scan has been positioned back to a previously read
	// row, or it is accessing a row for the first time.
    protected boolean scanRepositioned;

    //
    // class interface
    //
    TableScanResultSet(long conglomId,
		StaticCompiledOpenConglomInfo scoci, 
		Activation activation, 
		int resultRowTemplate,
		int resultSetNumber,
		GeneratedMethod startKeyGetter, int startSearchOperator,
		GeneratedMethod stopKeyGetter, int stopSearchOperator,
		boolean sameStartStopPosition,
		Qualifier[][] qualifiers,
		String tableName,
		String userSuppliedOptimizerOverrides,
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
		double optimizerEstimatedCost)
			throws StandardException
    {
		super(activation,
				resultSetNumber,
				resultRowTemplate,
				lockMode, tableLocked, isolationLevel,
                colRefItem,
				optimizerEstimatedRowCount,
				optimizerEstimatedCost);

		this.conglomId = conglomId;

		/* Static info created at compile time and can be shared across
		 * instances of the plan.
		 * Dynamic info created on 1st opening of this ResultSet as
		 * it cannot be shared.
		 */
        this.scoci = scoci;

		if (SanityManager.DEBUG) {
			SanityManager.ASSERT( activation!=null, "table scan must get activation context");
			if (sameStartStopPosition)
			{
				SanityManager.ASSERT(stopKeyGetter == null,
					"stopKeyGetter expected to be null when sameStartStopPosition is true");
			}
		}

		this.startKeyGetter = startKeyGetter;
		this.startSearchOperator = startSearchOperator;
		this.stopKeyGetter = stopKeyGetter;
		this.stopSearchOperator = stopSearchOperator;
		this.sameStartStopPosition = sameStartStopPosition;
		this.qualifiers = qualifiers;
		this.tableName = tableName;
		this.userSuppliedOptimizerOverrides = userSuppliedOptimizerOverrides;
		this.indexName = indexName;
		this.isConstraint = isConstraint;
		this.forUpdate = forUpdate;
		this.rowsPerRead = rowsPerRead;
		this.oneRowScan = oneRowScan;

		if (indexColItem != -1)
		{
			this.indexCols = (int[])(activation.getPreparedStatement().
						getSavedObject(indexColItem));
		}
		if (indexCols != null)
			activation.setForUpdateIndexScan(this);

		runTimeStatisticsOn = (activation != null &&
							   activation.getLanguageConnectionContext().getRunTimeStatisticsMode());
		
		/* Always qualify the first time a row is being read */
		qualify = true;
		currentRowIsValid = false;
		scanRepositioned = false;
		
		recordConstructorTime();
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
	public void	openCore() throws StandardException
	{
		if (SanityManager.DEBUG)
		    SanityManager.ASSERT( ! isOpen, "TableScanResultSet already open");

        // Get the current transaction controller
        TransactionController tc = activation.getTransactionController();

		initIsolationLevel();

		if (dcoci == null)
			dcoci = tc.getDynamicCompiledConglomInfo(conglomId);

        initStartAndStopKey();

		/* NOTE: We always open the ScanController on the 1st open
		 * to do the keyed conglomerate check.
		 */

		// Determine whether the conglomerate is keyed.  This determines
		// how we find the RowLocation for the base heap.  For non-keyed
		// conglomerates, we ask the scan.  For keyed conglomerates, it
		// is the last column in the row.
		//
		// Do this here, rather than in the constructor, so we can avoid
		// throwing exceptions from the constructor
		if (firstScan)
		{
			openScanController(tc);

			isKeyed = scanController.isKeyed();

			/*
			** If scan tracing is turned on, print information about this
			** TableScanResultSet when it is first opened.  We would like
			** to do this when it is constructed, but it is not always
			** possible to get the start and stop positioners at the time
			** this object is constructed (because they may depend on outer
			** rows).
			*/
			if (SanityManager.DEBUG)
			{
				if (SanityManager.DEBUG_ON("ScanTrace"))
				{
					//traceScanParameters();
				}
			}
		}

		// Check whether there are any comparisons with unordered nulls
		// on either the start or stop position.  If there are, we can
		// (and must) skip the scan, because no rows can qualify
		if (skipScan(startPosition, stopPosition))
		{
			scanControllerOpened = false;
		}
		/* NOTE: We always open the ScanController on the 1st open
		 * to do the keyed conglomerate check, so we only need to
		 * do it here if not the 1st scan.
		 */
		else if (! firstScan)
		{
			openScanController(tc);
		}

		/* If the scan is on an index and opened for update,
		 * then we cache the scan controller and conglomerate
		 * number in the activation so that the scan controller
		 * can be re-used by the update/delete if the index
		 * that we are scanning also needs to be updated.
		 */
		if (forUpdate && isKeyed)
		{
			activation.setIndexScanController(scanController);
			activation.setIndexConglomerateNumber(conglomId);
		}

		firstScan = false;
	    isOpen = true;
		numOpens++;
		nextDone = false;
		openTime += getElapsedMillis(beginTime);
	}

    /**
     * Initialize the {@code startPosition} and {@code stopPosition} fields
     * which are used to limit the rows returned by the scan.
     */
    void initStartAndStopKey() throws StandardException {
        if (startKeyGetter != null) {
            startPosition = (ExecIndexRow) startKeyGetter.invoke(activation);
            if (sameStartStopPosition) {
                stopPosition = startPosition;
            }
        }
        if (stopKeyGetter != null) {
            stopPosition = (ExecIndexRow) stopKeyGetter.invoke(activation);
        }
    }

	/*
	** Open the scan controller
	**
	** @param transaction controller will open one if null
	*/
	protected void openScanController(TransactionController tc)
		throws StandardException
	{
		DataValueDescriptor[] startPositionRow = 
            startPosition == null ? null : startPosition.getRowArray();
		DataValueDescriptor[] stopPositionRow = 
            stopPosition == null ? null : stopPosition.getRowArray();

		// Clear the Qualifiers's Orderable cache 
		if (qualifiers != null)
		{
			clearOrderableCache(qualifiers);
		}

		// Get the current transaction controller
		if (tc == null)
			tc = activation.getTransactionController();

        int openMode = 0;
        if (forUpdate)
        {
            openMode = TransactionController.OPENMODE_FORUPDATE;

            if (activation.isCursorActivation())
                openMode |= TransactionController.OPENMODE_USE_UPDATE_LOCKS;
        }

		scanController = tc.openCompiledScan(
				activation.getResultSetHoldability(),
				openMode,
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
		estimatedRowCount = scanController.getEstimatedRowCount();
		activation.informOfRowCount(
									this,
									scanController.getEstimatedRowCount()
									);
	}

	/*
	** reopen the scan controller
	*/
	protected void reopenScanController() throws StandardException
	{
		DataValueDescriptor[] startPositionRow = 
            startPosition == null ? null : startPosition.getRowArray();
		DataValueDescriptor[] stopPositionRow = 
            stopPosition == null ? null : stopPosition.getRowArray();

        rowsThisScan = 0;

		// Clear the Qualifiers's Orderable cache 
		if (qualifiers != null)
		{
			clearOrderableCache(qualifiers);
		}

		scanController.reopenScan(
						startPositionRow,
						startSearchOperator,
						qualifiers,
						stopPositionRow,
						stopSearchOperator);

		/* Remember that we opened the scan */
		scanControllerOpened = true;
	}

	/**
     * Reopen a table scan.  Here we take advantage
	 * of the reopenScan() interface on scanController
	 * for optimimal performance on joins where we are
	 * an inner table.
	 *
	 * @exception StandardException thrown on failure to open
     */
	public void	reopenCore() throws StandardException
	{
		beginTime = getCurrentTimeMillis();
		if (SanityManager.DEBUG)
		    SanityManager.ASSERT(isOpen, "TableScanResultSet not open, cannot reopen");

        initStartAndStopKey();

		// Check whether there are any comparisons with unordered nulls
		// on either the start or stop position.  If there are, we can
		// (and must) skip the scan, because no rows can qualify
		if (skipScan(startPosition, stopPosition))
		{
			scanControllerOpened = false;
		}
		else
		{
			if (scanController == null)
				openScanController((TransactionController)null);
			else
				reopenScanController();
		
		}

		numOpens++;
		nextDone = false;
		openTime += getElapsedMillis(beginTime);
	}

	/**
     * Return the next row (if any) from the scan (if open).
	 *
	 * @exception StandardException thrown on failure to get next row
	 */
	public ExecRow getNextRowCore() throws StandardException
	{
		if( isXplainOnlyMode() )
			return null;

        checkCancellationFlag();
            
		if (currentRow == null || scanRepositioned)
		{
			currentRow =
				getCompactRow(candidate, accessedCols, isKeyed);
		}

		beginTime = getCurrentTimeMillis();

		ExecRow result = null;

	    if ( isOpen  && !nextDone)
	    {
			/* Only need to do 1 next per scan
			 * for 1 row scans.
			 */
			nextDone = oneRowScan;

			if (scanControllerOpened)
			{
				boolean moreRows;

				while (moreRows =
							scanController.fetchNext(candidate.getRowArray()))
				{
					rowsSeen++;
					rowsThisScan++;

					/*
					** Skip rows where there are start or stop positioners
					** that do not implement ordered null semantics and
					** there are columns in those positions that contain
					** null.
					** No need to check if start and stop positions are the
					** same, since all predicates in both will be ='s,
					** and hence evaluated in the store.
					*/
					if ((! sameStartStopPosition) && skipRow(candidate))
					{
						rowsFiltered++;
						continue;
					}

					/* beetle 3865, updateable cursor use index. If we have a hash table that
					 * holds updated records, and we hit it again, skip it, and remove it from
					 * hash since we can't hit it again, and we have a space in hash, so can
					 * stop scanning forward.
					 */
					if (past2FutureTbl != null)
					{
						RowLocation rowLoc = (RowLocation) currentRow.getColumn(currentRow.nColumns());
						if (past2FutureTbl.remove(rowLoc) != null){
						    continue;
						}
					}

					result = currentRow;

					break;
				}

				/*
				** If we just finished a full scan of the heap, update
				** the number of rows in the scan controller.
				**
				** NOTE: It would be more efficient to only update the
				** scan controller if the optimizer's estimated number of
				** rows were wrong by more than some threshold (like 10%).
				** This would require a little more work than I have the
				** time for now, however, as the row estimate that is given
				** to this result set is the total number of rows for all
				** scans, not the number of rows per scan.
				*/
				if (! moreRows)
				{
					setRowCountIfPossible(rowsThisScan);
					currentRow = null;
				}
			}
	    }

		setCurrentRow(result);
		currentRowIsValid = true;
		scanRepositioned = false;
		qualify = true;

		nextTime += getElapsedMillis(beginTime);
	    return result;
	}

	/**
	 * If the result set has been opened,
	 * close the open scan.
	 * @exception StandardException on error
	 */
	public void	close() throws StandardException
	{
		beginTime = getCurrentTimeMillis();
		if ( isOpen )
	    {
			/*
			** If scan tracing is turned on, print information about this
			** TableScanResultSet when it is closed.
			*/
			if (SanityManager.DEBUG)
			{
				if (SanityManager.DEBUG_ON("ScanTrace"))
				{
					//traceClose();
				}
			}

			// we don't want to keep around a pointer to the
			// row ... so it can be thrown away.
			// REVISIT: does this need to be in a finally
			// block, to ensure that it is executed?
		    clearCurrentRow();
;
			if (scanController != null)
			{
				// This is where we get the positioner info for inner tables
				if (runTimeStatisticsOn)
				{
					// This is where we get the scan properties for a subquery
					scanProperties = getScanProperties();
					startPositionString = printStartPosition();
					stopPositionString = printStopPosition();
				}
	        	scanController.close();
				scanController = null; // should not access after close

                // Updatable index scans are cached in the activation for
                // easy access from IndexChanger. Remember to clear the cached
                // info here, but only if this is the result set that cached
                // it in the first place (DERBY-4585).
                if (forUpdate && isKeyed) {
                    activation.clearIndexScanInfo();
                }

			}
			scanControllerOpened = false;
			startPosition = null;
			stopPosition = null;

			super.close();

			if (indexCols != null)
			{
				ConglomerateController borrowedBaseCC = activation.getHeapConglomerateController();
				if (borrowedBaseCC != null)
				{
					borrowedBaseCC.close();
					activation.clearHeapConglomerateController();
				}
			}

            if (past2FutureTbl != null)
            {
                past2FutureTbl.close();
            }
	    }
		else
			if (SanityManager.DEBUG)
				SanityManager.DEBUG("CloseRepeatInfo","Close of TableScanResultSet repeated");

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


	//
	// CursorResultSet interface
	//

	/**
	 * This result set has its row location from
	 * the last fetch done. If the cursor is closed, 
	 * or the row has been deleted a null is returned.
	 *
	 * @see CursorResultSet
	 *
	 * @return the row location of the current cursor row.
	 * @exception StandardException thrown on failure to get row location
	 */
	public RowLocation getRowLocation() throws StandardException
	{
		RowLocation rl;

		if (! isOpen) return null;

		if ( ! scanControllerOpened)
			return null;

		/*
		** If the conglomerate is keyed, the row location of the base row
		** is in the last column of the current row.  If it's not keyed,
		** we get the row location from the scan of the heap.
		*/
		if (isKeyed)
		{
			if (SanityManager.DEBUG)
			{
				SanityManager.ASSERT(currentRow != null,
				  "There must be a current row when fetching the row location");
			}

			rl = (RowLocation) currentRow.getColumn(
													currentRow.nColumns());
		}
		else
		{
			if (currentRowIsValid) {
				// we reuse the same rowlocation object across several calls.
				if (rlTemplate == null)
					rlTemplate = scanController.newRowLocationTemplate();
				rl = rlTemplate;
				try {
					scanController.fetchLocation(rl);
				} catch (StandardException se) {
					if (se.getMessageId().
						equals(SQLState.HEAP_SCAN_NOT_POSITIONED)) {
						//Have a easier to understand error message than what 
						//we get from store 
						throw StandardException.
							newException(SQLState.NO_CURRENT_ROW);
					}
                    throw se;
				}
			} else {
				rl = null;
			}
		}

		return rl;
	}

	/**
	 * This result set has its row from the last fetch done. 
	 * If the cursor is closed, the row has been deleted, or
	 * no longer qualifies (for forward only result sets) a 
	 * null is returned.
	 *
	 * @see CursorResultSet
	 *
	 * @return the last row returned;
	 * @exception StandardException thrown on failure.
	 */
	/* RESOLVE - this should return activation.getCurrentRow(resultSetNumber),
	 * once there is such a method.  (currentRow is redundant)
	 */
	public ExecRow getCurrentRow() throws StandardException 
	{
		if (SanityManager.DEBUG)
			SanityManager.ASSERT(isOpen, "TSRS expected to be open");

		/* Nothing to do if we're not currently on a row or
		 * if the current row get deleted out from under us
		 * or if there is no current scan (can happen if the
		 * scan is being skipped) or if the current position
		 * no longer qualifies.
		 */
		try
		{
			if ((currentRow == null)                        ||
			(!currentRowIsValid)                            ||
			(!scanControllerOpened)                         ||
			(qualify && scanController.isCurrentPositionDeleted())     ||
			(qualify && (!scanController.doesCurrentPositionQualify())))
			{
				return null;
			}
		}
		catch (StandardException se)
		{
			if (se.getMessageId().equals(SQLState.AM_SCAN_NOT_POSITIONED))
			{
				//bug 4515 - Have a easier to understand error message than what we get from store 
				se=StandardException.newException(SQLState.NO_CURRENT_ROW);
				throw se;
			}
		}

        resultRowBuilder.reset(candidate);
        currentRow = getCompactRow(candidate, accessedCols, isKeyed);

        try
        {
            scanController.fetchWithoutQualify(candidate.getRowArray());
        }
        catch (StandardException se)
        {
            if (se.getMessageId().equals(SQLState.AM_RECORD_NOT_FOUND))
            {
                // Somehow the row got deleted between the above 
                // doesCurrentPositionQualify() call and here (one way is if
                // this scan is read uncommitted isolation level).
                return null;
            }
            else
            {
                throw se;
            }
        }

        setCurrentRow(candidate);
	    return currentRow;
	}

	/**
	 * @see NoPutResultSet#positionScanAtRowLocation
	 * 
	 * Also sets qualify to false so that later calls to getCurrentRow
	 * will not attempt to re-qualify the current row. 
	 */
	public void positionScanAtRowLocation(RowLocation rl) 
		throws StandardException 
	{
		// Check if the scanController is a B-tree scan controller. Do not
		// attempt to re-position a b-tree controller.
		if (!isKeyed) {
			currentRowIsValid = scanController.positionAtRowLocation(rl);
		}
		qualify = false;
		scanRepositioned = true;
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
			traceStream.println("TableScanResultSet number " +
								resultSetNumber +
								" parameters:");

			traceStream.println("");
			traceStream.println("\tTable name: " + tableName);
			if (indexName != null)
			{
				traceStream.println("\tIndex name: " + indexName);
			}
			traceStream.println("");
			traceStream.println("\tStart position is: ");
			tracePrintPosition(traceStream,
								startSearchOperator,
								startKeyGetter);
			traceStream.println("");
			traceStream.println("\tStop position is: " );
			tracePrintPosition(traceStream,
								stopSearchOperator,
								stopKeyGetter);
			traceStream.println("");
			traceStream.println("\tQualifiers are: ");
			tracePrintQualifiers(traceStream, qualifiers, 2);
			traceStream.println("");
		}
	}
*/

	/**
	 * Print I/O statistics about a scan when it closes.
	 */
/*
	private final void traceClose()
	{
		if (SanityManager.DEBUG)
		{
			InfoStreams			infoStreams;
			HeaderPrintWriter	traceStream;

			traceStream = SanityManager.GET_DEBUG_STREAM();

			traceStream.println("TableScanResultSet number " +
								resultSetNumber +
								" closed.");
			if (isKeyed)
			{
				traceStream.println("\t" +
									rowCount() +
									" row(s) qualified from " +
									"keyed" +
									" table " +
									tableName +
									" using index " +
									indexName);
			}
			else
			{
				traceStream.println("\t" +
									rowCount() +
									" row(s) qualified from " +
									"non-keyed" +
									" table " +
									tableName);
			}
			traceStream.println("");
		}
	}
*/

	/**
	 * Print a start or stop positioner to the trace stream.
	 */
/*
	private final void tracePrintPosition(HeaderPrintWriter traceStream,
										  int searchOperator,
										  GeneratedMethod positionGetter)
	{
		if (SanityManager.DEBUG)
		{
			if (positionGetter == null)
			{
				traceStream.println("\t\tNone");
				return;
			}

			ExecIndexRow	positioner = null;

			try
			{
				positioner = (ExecIndexRow) positionGetter.invoke(activation);
			}
			catch (StandardException e)
			{
				traceStream.println("\t\tUnexpected exception " +
									e +
									" getting positioner.");
				e.printStackTrace(traceStream.getPrintWriter());
				return;
			}

			if (positioner == null)
			{
				traceStream.println("\t\tNone");
				return;
			}

			String searchOp = null;

			switch (searchOperator)
			{
			  case ScanController.GE:
				searchOp = "GE";
				break;

			  case ScanController.GT:
				searchOp = "GT";
				break;

			  default:
				searchOp = "unknown value (" + searchOperator + ")";
				break;
			}

			traceStream.println("\t\t" +
								searchOp +
								" on first " +
								positioner.nColumns() +
								" column(s).");

			traceStream.print(
					"\t\tOrdered null semantics on the following columns: ");
			for (int position = 0; position < positioner.nColumns(); position++)
			{
				if (positioner.areNullsOrdered(position))
				{
					traceStream.print(position + " ");
				}
			}
			traceStream.println("");
		}
	}
*/


	/**
	 * Print an array of Qualifiers to the trace stream.
	 */
/*
	private final void tracePrintQualifiers(HeaderPrintWriter traceStream,
											Qualifier[][] qualifiers,
											int depth)
	{
		if (SanityManager.DEBUG)
		{
			char[] indentchars = new char[depth];

			/*
			** Form an array of tab characters for indentation.
			*
			while (depth > 0)
			{
				indentchars[depth - 1] = '\t';
				depth--;
			}
			String indent = new String(indentchars);

			if (qualifiers == null)
			{
				traceStream.println(indent +
									MessageService.getTextMessage(
										SQLState.LANG_NONE)
									);
				return;
			}

            // RESOLVE (mikem) We don't support 2-d qualifiers yet.
            if (SanityManager.DEBUG)
            {
                SanityManager.ASSERT(qualifiers.length == 1);
            }

			for (int i = 0; i < qualifiers[0].length; i++)
			{
				Qualifier qual = qualifiers[0][i];

				traceStream.println("");
				traceStream.println(indent + "Column Id: " + qual.getColumnId());
				
				int operator = qual.getOperator();
				String opString = null;
				switch (operator)
				{
				  case Orderable.ORDER_OP_EQUALS:
					opString = "=";
					break;

				  case Orderable.ORDER_OP_LESSOREQUALS:
					opString = "<=";
					break;

				  case Orderable.ORDER_OP_LESSTHAN:
					opString = "<";
					break;

				  default:
					opString = "unknown value (" + operator + ")";
					break;
				}
				traceStream.println(indent + "Operator: " + opString);
				traceStream.println(indent + "Ordered nulls: " +
											qual.getOrderedNulls());
				traceStream.println(indent + "Unknown return value: " +
											qual.getUnknownRV());
				traceStream.println(indent + "Negate comparison result: " +
											qual.negateCompareResult());
				traceStream.println("");
			}
		}
	}
*/

	public String printStartPosition()
	{
		return printPosition(startSearchOperator, startKeyGetter, startPosition);
	}

	public String printStopPosition()
	{
		if (sameStartStopPosition)
		{
			return printPosition(stopSearchOperator, startKeyGetter, startPosition);
		}
		else
		{
			return printPosition(stopSearchOperator, stopKeyGetter, stopPosition);
		}
	}

	/**
	 * Return a start or stop positioner as a String.
	 *
	 * If we already generated the information, then use
	 * that.  Otherwise, invoke the activation to get it.
	 */
	private String printPosition(int searchOperator,
								 GeneratedMethod positionGetter,
								 ExecIndexRow positioner)
	{
		String output = "";
		if (positionGetter == null)
		{
			return "\t" +
					MessageService.getTextMessage(SQLState.LANG_NONE) +
					"\n";
		}
		
		if (positioner == null)
		{
			if (numOpens == 0)
				return "\t" + MessageService.getTextMessage(
					SQLState.LANG_POSITION_NOT_AVAIL) +
                                    "\n";
			try
			{
				positioner = (ExecIndexRow)positionGetter.invoke(activation);
			}
			catch (StandardException e)
			{
				return "\t" + MessageService.getTextMessage(
						SQLState.LANG_UNEXPECTED_EXC_GETTING_POSITIONER,
						e.toString());
			}
		}
		if (positioner == null)
		{
			return "\t" +
					MessageService.getTextMessage(SQLState.LANG_NONE) +
					"\n";
		}
		String searchOp = null;

		switch (searchOperator)
		{
			case ScanController.GE:
				searchOp = ">=";
				break;

			case ScanController.GT:
				searchOp = ">";
				break;

			default:
				if (SanityManager.DEBUG)
				{
					SanityManager.THROWASSERT("Unknown search operator " +
												searchOperator);
				}

				// NOTE: This does not have to be internationalized because
				// this code should never be reached.
				searchOp = "unknown value (" + searchOperator + ")";
				break;
		}

		output = output + "\t" +
						MessageService.getTextMessage(
							SQLState.LANG_POSITIONER,
							searchOp,
							String.valueOf(positioner.nColumns())) +
						"\n";

		output = output + "\t" +
					MessageService.getTextMessage(
						SQLState.LANG_ORDERED_NULL_SEMANTICS) +
					"\n";
		boolean colSeen = false;
		for (int position = 0; position < positioner.nColumns(); position++)
		{
			if (positioner.areNullsOrdered(position))
			{
				output = output + position + " ";
				colSeen = true;
			}

			if (colSeen && position == positioner.nColumns() - 1) {
				output = output +  "\n";
			}
		}

		return output;
	}

	public Properties getScanProperties()
	{
		if (scanProperties == null)
		{
			scanProperties = new Properties();
		}
		try
		{
			if (scanController != null)
			{
				scanController.getScanInfo().getAllScanInfo(scanProperties);
				/* Did we get a coarser lock due to
				 * a covering lock, lock escalation
				 * or configuration?
				 */
				coarserLock = scanController.isTableLocked() &&
					(lockMode == TransactionController.MODE_RECORD);
			}
		}
		catch(StandardException se)
		{
			// ignore
		}

		return scanProperties;
	}

	/**
	 * @see NoPutResultSet#requiresRelocking
	 */
	public boolean requiresRelocking()
	{
		return(
            isolationLevel == 
                TransactionController.ISOLATION_READ_COMMITTED_NOHOLDLOCK);
	}

	/**
	 * Update the number of rows in the scan controller.
	 *
	 * NOTE: It would be more efficient to only update the
	 * scan controller if the optimizer's estimated number of
	 * rows were wrong by more than some threshold (like 10%).
	 * This would require a little more work than I have the
	 * time for now, however, as the row estimate that is given
	 * to this result set is the total number of rows for all
	 * scans, not the number of rows per scan.
	 *
	 *
	 * @param rowsThisScan	The number of rows to update the scanController to
	 *
	 * @exception StandardException		Thrown on error
	 */
	protected final void setRowCountIfPossible(long rowsThisScan)
					throws StandardException
	{
		/*
		** Is it a heap scan with no qualifiers (full table scan?)
		** and is it not for update (we don't want to count rows we're
		** about to delete.
		*/
		if ( ( ! scanController.isKeyed() ) &&
			(qualifiers == null || qualifiers.length == 0) &&
			( ! forUpdate ) )
		{

			// Only update rows if different by more than 10%
			long diff = rowsThisScan - estimatedRowCount;

			long tenPerCent = estimatedRowCount  / 10;

			if (diff < 0)
				diff = -diff;

			if (diff > tenPerCent)
				scanController.setEstimatedRowCount(rowsThisScan);
		}
	}

	/**
	 * Can we get instantaneous locks when getting share row
	 * locks at READ COMMITTED.
	 */
	protected boolean canGetInstantaneousLocks()
	{
		return false;
	}


	/**
	 * Is this ResultSet or it's source result set for update
	 * 
	 * @return Whether or not the result set is for update.
	 */
	public boolean isForUpdate()
	{
		return forUpdate;
	}

	/**
	 * Shallow clone this result set.  Used in trigger reference.
	 * beetle 4373.
	 */
	public Object clone()
	{
		Object clo = null;
		try {
			clo = super.clone();
		}
		catch (CloneNotSupportedException e) {}
		return clo;
	}
}
