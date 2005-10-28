/*

   Derby - Class org.apache.derby.impl.sql.execute.IndexRowToBaseRowResultSet

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

import org.apache.derby.iapi.services.monitor.Monitor;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.services.stream.HeaderPrintWriter;
import org.apache.derby.iapi.services.stream.InfoStreams;

import org.apache.derby.iapi.sql.execute.CursorResultSet;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;

import org.apache.derby.iapi.types.DataValueDescriptor;

import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.ResultSet;

import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.conn.StatementContext;

import org.apache.derby.iapi.store.access.ConglomerateController;
import org.apache.derby.iapi.store.access.DynamicCompiledOpenConglomInfo;
import org.apache.derby.iapi.store.access.StaticCompiledOpenConglomInfo;
import org.apache.derby.iapi.store.access.TransactionController;

import org.apache.derby.iapi.services.loader.GeneratedMethod;

import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.types.RowLocation;

import org.apache.derby.iapi.services.io.FormatableBitSet;

import org.apache.derby.catalog.types.ReferencedColumnsDescriptorImpl;

/**
 * Takes a result set with a RowLocation as the last column, and uses the
 * RowLocation to get and return a row from the given base conglomerate.
 * Normally, the input result set will be a TableScanResultSet scanning an
 * index conglomerate.
 *
 * @author jeff
 */
public class IndexRowToBaseRowResultSet extends NoPutResultSetImpl
	implements CursorResultSet {

    // set in constructor and not altered during
    // life of object.
	private long conglomId;
    public NoPutResultSet source;
	private GeneratedMethod resultRowAllocator;
	private GeneratedMethod restriction;
    private long baseConglomId;
    private GeneratedMethod closeCleanup;
	public FormatableBitSet accessedHeapCols;
	private FormatableBitSet accessedIndexCols;
	//caching accessed columns (heap+index) beetle 3865
	private FormatableBitSet accessedAllCols;
	public String indexName;
	private int[] indexCols;
	private DynamicCompiledOpenConglomInfo dcoci;
	private StaticCompiledOpenConglomInfo scoci;

	// set in open() and not changed after that
	private ConglomerateController	baseCC;
	private boolean                 closeBaseCCHere;
	private ExecRow					resultRow;
	private ExecRow					compactRow;
	private boolean					forUpdate;
	private DataValueDescriptor[]	rowArray;

	// changed a whole bunch
	RowLocation	baseRowLocation;

	/* Remember whether or not we have copied any
	 * columns from the source row to our row yet.
	 */
	boolean copiedFromSource;

	/* Run time statistics variables */
	public long restrictionTime;

	protected boolean currentRowPrescanned;
	private boolean sourceIsForUpdateIndexScan;

    //
    // class interface
    //
    public IndexRowToBaseRowResultSet(
					long conglomId,
					int scociItem,
					Activation a,
					NoPutResultSet source,
					GeneratedMethod resultRowAllocator,
					int resultSetNumber,
					String indexName,
					int heapColRefItem,
					int indexColRefItem,
					int indexColMapItem,
					GeneratedMethod restriction,
					boolean forUpdate,
					double optimizerEstimatedRowCount,
					double optimizerEstimatedCost,
					GeneratedMethod closeCleanup) 
		throws StandardException
	{
		super(a, resultSetNumber, optimizerEstimatedRowCount, optimizerEstimatedCost);
        scoci = (StaticCompiledOpenConglomInfo)(activation.getPreparedStatement().
						getSavedObject(scociItem));
		TransactionController tc = activation.getTransactionController();
		dcoci = tc.getDynamicCompiledConglomInfo(conglomId);
        this.source = source;
		this.resultRowAllocator = resultRowAllocator;
		this.indexName = indexName;
		this.forUpdate = forUpdate;
		this.restriction = restriction;

		/* RESOLVE - once we push Qualifiers into the store we
		 * need to clear their Orderable cache on each open/reopen.
		 */
        this.closeCleanup = closeCleanup;

		// retrieve the valid column list from
		// the saved objects, if it exists
		this.accessedHeapCols = null;
		if (heapColRefItem != -1)
		{
			this.accessedHeapCols = (FormatableBitSet)(a.getPreparedStatement().
						getSavedObject(heapColRefItem));
		}
		if (indexColRefItem != -1)
		{
			this.accessedIndexCols = (FormatableBitSet)(a.getPreparedStatement().
						getSavedObject(indexColRefItem));
		}
		if (accessedIndexCols == null)
			accessedAllCols = accessedHeapCols;
		else
		{
			accessedAllCols = new FormatableBitSet(accessedHeapCols);
			accessedAllCols.or(accessedIndexCols);
		}
			
		// retrieve the array of columns coming from the index
		indexCols = ((ReferencedColumnsDescriptorImpl) (a.getPreparedStatement().
						getSavedObject(indexColMapItem))).getReferencedColumnPositions();

		/* Get the result row template */
		resultRow = (ExecRow) resultRowAllocator.invoke(activation);

		compactRow =
			getCompactRow(resultRow,
							accessedHeapCols,
							accessedIndexCols,
							false);

		/* If there's no partial row bit map, then we want the entire
		 * row, otherwise we need to diddle with the row array so that
		 * we only get the columns coming from the heap on the fetch.
		 */
		if (accessedHeapCols == null)
		{
			rowArray = resultRow.getRowArray();
		}
		else
		{
			// Figure out how many columns are coming from the heap
			int arraySize = accessedHeapCols.getNumBitsSet();
			int accessedHeapColsSize = accessedHeapCols.size();

			rowArray = new DataValueDescriptor[accessedHeapColsSize];

			// Now, fill in rowArray with the desired columns
			int partialIndex = 0;
			int numFromIndex = 0;
			for (int index = 0; index < accessedHeapColsSize; index++)
			{
				if (accessedIndexCols != null && accessedIndexCols.get(index))
				{
					numFromIndex++;
					continue;
				}
				if (accessedHeapCols.get(index))
				{
					rowArray[index] =
						resultRow.getRowArray()[index];
					partialIndex++;
				}
			}
		}

		constructorTime += getElapsedMillis(beginTime);
    }

	//
	// ResultSet interface (leftover from NoPutResultSet)
	//

	/**
     * open this ResultSet.
	 *
	 * @exception StandardException thrown if cursor finished.
     */
	public void	openCore() throws StandardException 
	{
		boolean						lockingRequired = false;
		TransactionController		tc;

		// REVISIT: through the direct DB API, this needs to be an
		// error, not an ASSERT; users can open twice. Only through JDBC
		// is access to open controlled and ensured valid.
		if (SanityManager.DEBUG)
		{
	    	SanityManager.ASSERT( ! isOpen,
								"IndexRowToBaseRowResultSet already open");
		}

		beginTime = getCurrentTimeMillis();

		source.openCore();
		if ((source instanceof TableScanResultSet) && 
			((TableScanResultSet) source).indexCols != null)
			sourceIsForUpdateIndexScan = true;

		/* Get a ConglomerateController for the base conglomerate 
		 * NOTE: We only need to acquire locks on the data pages when
		 * going through the index when we are at READ COMMITTED and
		 * the source is a BulkTableScan or HashScan.  (The underlying
		 * row will not be guaranteed to be locked.)
		 */
		if (source.requiresRelocking())
		{
			lockingRequired = true;
		}

		tc = activation.getTransactionController();

		int openMode;
		int isolationLevel;
		
		if (forUpdate)
		{
			openMode = TransactionController.OPENMODE_FORUPDATE;
		}
		else
		{
			openMode = 0;
		}
		isolationLevel = source.getScanIsolationLevel();

		if (!lockingRequired)
		{
            // flag indicates that lock has already been acquired by access to
            // the secondary index, and need not be gotten again in the base
            // table.
			openMode |= TransactionController.OPENMODE_SECONDARY_LOCKED;
		}
		
		/* Try to get the ConglomerateController from the activation
		 * first, for the case that we are part of an update or delete.
		 * If so, then the RowChangerImpl did the correct locking.
		 * If not there, then we go off and open it ourself.
		 */
		if (forUpdate)
		{
			baseCC = activation.getHeapConglomerateController();
		}

		if (baseCC == null)
		{
			baseCC = 
		        tc.openCompiledConglomerate(
                    activation.getResultSetHoldability(),
				    openMode,
					// consistent with FromBaseTable's updateTargetLockMode
					TransactionController.MODE_RECORD,
	                isolationLevel,
					scoci,
					dcoci);
			closeBaseCCHere = true;
		}

		isOpen = true;
		numOpens++;
		openTime += getElapsedMillis(beginTime);
	}

	/**
	 * reopen this ResultSet.
	 *
	 * @exception StandardException thrown if cursor finished.
	 */
	public void	reopenCore() throws StandardException {
		TransactionController		tc;

		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(isOpen,
					"IndexRowToBaseRowResultSet already open");
		}

		beginTime = getCurrentTimeMillis();

		source.reopenCore();

		numOpens++;
		openTime += getElapsedMillis(beginTime);
	}

	/**
     * Return the requested values computed
     * from the next row (if any) for which
     * the restriction evaluates to true.
     * <p>
     * restriction and projection parameters
     * are evaluated for each row.
	 *
	 * @exception StandardException thrown on failure.
	 * @exception StandardException ResultSetNotOpen thrown if not yet open.
	 *
	 * @return the next row in the result
	 */
	public ExecRow	getNextRowCore() throws StandardException {

	    ExecRow sourceRow = null;
		ExecRow retval = null;
	    boolean restrict = false;
	    DataValueDescriptor restrictBoolean;
		long	beginRT = 0;

		beginTime = getCurrentTimeMillis();
	    if ( ! isOpen ) {
			throw StandardException.newException(SQLState.LANG_RESULT_SET_NOT_OPEN, "next");
		}

		/* beetle 3865, updateable cursor using index.  When in-memory hash table was full, we
		 * read forward and saved future row id's in a virtual-memory-like temp table.  So if
		 * we have rid's saved, and we are here, it must be non-covering index.  Intercept it
		 * here, so that we don't have to go to underlying index scan.  We get both heap cols
		 * and index cols together here for better performance.
		 */
		if (sourceIsForUpdateIndexScan && ((TableScanResultSet) source).futureForUpdateRows != null)
		{
			currentRowPrescanned = false;
			TableScanResultSet src = (TableScanResultSet) source;

			if (src.futureRowResultSet == null)
			{
				src.futureRowResultSet = (TemporaryRowHolderResultSet) src.futureForUpdateRows.getResultSet();
				src.futureRowResultSet.openCore();
			}

			ExecRow ridRow = src.futureRowResultSet.getNextRowCore();

			currentRow = null;

			if (ridRow != null)
			{
				/* To maximize performance, we only use virtual memory style heap, no
				 * position index is ever created.  And we save and retrieve rows from the
				 * in-memory part of the heap as much as possible.  We can also insert after
				 * we start retrieving, the assumption is that we delete the current row right
				 * after we retrieve it.
				 */
				src.futureRowResultSet.deleteCurrentRow();
				baseRowLocation = (RowLocation) ridRow.getColumn(1);
               	baseCC.fetch(
                  	      baseRowLocation, compactRow.getRowArray(), accessedAllCols);

				currentRow = compactRow;
				currentRowPrescanned = true;
			}
			else if (src.sourceDrained)
				currentRowPrescanned = true;

			if (currentRowPrescanned)
			{
				setCurrentRow(currentRow);

				nextTime += getElapsedMillis(beginTime);
	 	   		return currentRow;
			}
		}

		/* Loop until we get a row from the base page that qualifies or
		 * there's no more rows from the index that qualify. (If the RID
		 * returned by the index does not qualify, then we have to go back
		 * to the index to see if there is another RID to consider.)
		 */
		do 
		{
			sourceRow = source.getNextRowCore();

			if (sourceRow != null) {

				if (SanityManager.DEBUG) {
					SanityManager.ASSERT(
						sourceRow.getColumn(sourceRow.nColumns())
														instanceof RowLocation,
						"Last column of source row is not a RowLocation"
							);
				}

				baseRowLocation = (RowLocation)
						sourceRow.getColumn(sourceRow.nColumns());

				// Fetch the columns coming from the heap
				boolean row_exists = 
                    baseCC.fetch(
                        baseRowLocation, rowArray, accessedHeapCols);

                if (row_exists)
                {
					/* We only need to copy columns from the index row 
					 * to our result row once as we will be reusing the
					 * wrappers in that case.
					 * NOTE: When the underlying ResultSet got an 
					 * instantaneous lock (BulkTableScan or HashScan)
					 * then we will be getting all of the columns anew
					 * from the index (indexCols == null).
					 */
					if (! copiedFromSource)
					{
						copiedFromSource = true;

						// Copy the columns coming from the index into resultRow
						for (int index = 0; index < indexCols.length; index++)
						{
							if (indexCols[index] != -1)
							{
								compactRow.setColumn(
											index + 1,
											sourceRow.getColumn(indexCols[index] + 1));
							}
						}
					}

                    setCurrentRow(compactRow);

                    restrictBoolean = (DataValueDescriptor) 
                        ((restriction == null) ? 
                             null : restriction.invoke(activation));

                    restrictionTime += getElapsedMillis(beginRT);

                    // if the result is null, we make it false --
                    // so the row won't be returned.
                    restrict = (restrictBoolean == null) ||
                                ((! restrictBoolean.isNull()) &&
                                    restrictBoolean.getBoolean());
                }

				if (! restrict || ! row_exists)
				{
					rowsFiltered++;
					clearCurrentRow();
					baseRowLocation = null;

					currentRow = null;
				}
				else
				{
					currentRow = compactRow;
				}

				/* Update the run time statistics */
				rowsSeen++;

				retval = currentRow;
		    } else {
				currentRow = null;
				clearCurrentRow();
				baseRowLocation = null;

				retval = null;
			}
	    } 
		while ( (sourceRow != null) && (! restrict ) );

		nextTime += getElapsedMillis(beginTime);
    	return retval;
	}

	/**
	 * If the result set has been opened,
	 * close the open scan.
	 *
	 * @exception StandardException thrown on error
	 */
	public void	close() throws StandardException
	{
		beginTime = getCurrentTimeMillis();
	    if ( isOpen ) {

			// we don't want to keep around a pointer to the
			// row ... so it can be thrown away.
			// REVISIT: does this need to be in a finally
			// block, to ensure that it is executed?
	    	clearCurrentRow();
			if (closeCleanup != null) {
				closeCleanup.invoke(activation); // let activation tidy up
			}
			currentRow = null;
			if (closeBaseCCHere)
			{
                // This check should only be needed in the error case where
                // we may call this close() routine as part of transaction
                // backout cleanup if any of the following routines fail.
                // If one of the subsequent statements gets an error, we
                // will try to close this result set as part of transaction
                // cleanup, and without this check we get a null pointer
                // exception because we have null'd out baseCC.
              
                if (baseCC != null)
                    baseCC.close();
			}

			/* Make sure to null out baseCC since
			 * we check for null baseCC after looking
			 * in the StatementContext.
			 */
			baseCC = null;
	        source.close();

			super.close();
	    }
		else if (SanityManager.DEBUG) {
			SanityManager.DEBUG("CloseRepeatInfo","Close of IndexRowToBaseRowResultSet repeated");
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

		if (type == NoPutResultSet.CURRENT_RESULTSET_ONLY)
		{
			return	totTime - source.getTimeSpent(ENTIRE_RESULTSET_TREE);
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
	 * Return the RowLocation of the base row.
	 *
	 * @see CursorResultSet
	 *
	 * @return the row location of the current cursor row.
	 * @exception StandardException thrown on failure.
	 */
	public RowLocation getRowLocation() throws StandardException {
		return baseRowLocation;
	}

	/**
	 * Gets last row returned.
	 *
	 * @see CursorResultSet
	 *
	 * @return the last row returned.
	 * @exception StandardException thrown on failure.
	 */
	/* RESOLVE - this should return activation.getCurrentRow(resultSetNumber),
	 * once there is such a method.  (currentRow is redundant)
	 */
	public ExecRow getCurrentRow() throws StandardException {
	    ExecRow sourceRow = null;

		if (SanityManager.DEBUG) {
			SanityManager.ASSERT(isOpen,
					"IndexRowToBaseRowResultSet is expected to be open");
		}

		if (currentRowPrescanned)
			return currentRow;

		/* Nothing to do if we're not currently on a row */
		if (currentRow == null)
		{
			return null;
		}

		/* Call the child result set to get it's current row.
		 * If no row exists, then return null, else requalify it
		 * before returning.
		 */
		sourceRow = ((CursorResultSet) source).getCurrentRow();
		if (sourceRow != null)
		{
			/*
			** Presumably, if the index row is still there, the RowLocation
			** it contains is still valid.  This means we don't have to
			** check whether the row is still there.
			*/
			if (SanityManager.DEBUG) {
				SanityManager.ASSERT(
					sourceRow.getColumn(sourceRow.nColumns())
														instanceof RowLocation,
					"Last column of source row is not a RowLocation"
						);
			}

			baseRowLocation = (RowLocation)
						sourceRow.getColumn(sourceRow.nColumns());

			// Fetch the columns coming from the heap
			boolean row_exists = 
                baseCC.fetch(
                    baseRowLocation, rowArray, (FormatableBitSet) null);

            if (row_exists)
            {
				// Copy the columns coming from the index into resultRow
				for (int index = 0; index < indexCols.length; index++)
				{
					if (indexCols[index] != -1)
					{
						compactRow.setColumn(
										index + 1,
										sourceRow.getColumn(indexCols[index] + 1));
					}
				}

                setCurrentRow(compactRow);

                currentRow = compactRow;
            }
            else
            {
                clearCurrentRow();

                currentRow = null;
            }

        } else {
			clearCurrentRow();

			currentRow = null;
		}
		
		return currentRow;
	}

	/**
	 * Is this ResultSet or it's source result set for update.
	 * beetle 3865: updateable cursor using index scan.  We didn't need this function
	 * before because we couldn't use index for update cursor.
	 * 
	 * @return Whether or not the result set is for update.
	 */
	public boolean isForUpdate()
	{
		return source.isForUpdate();
	}

}
