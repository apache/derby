/*

   Derby - Class org.apache.derby.impl.sql.execute.HashScanResultSet

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

import java.util.List;
import java.util.Properties;

import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.shared.common.reference.SQLState;
import org.apache.derby.shared.common.i18n.MessageService;
import org.apache.derby.iapi.services.io.FormatableArrayHolder;
import org.apache.derby.iapi.services.io.FormatableIntHolder;
import org.apache.derby.iapi.services.loader.GeneratedMethod;
import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.execute.CursorResultSet;
import org.apache.derby.iapi.sql.execute.ExecIndexRow;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;
import org.apache.derby.iapi.store.access.BackingStoreHashtable;
import org.apache.derby.iapi.store.access.KeyHasher;
import org.apache.derby.iapi.store.access.Qualifier;
import org.apache.derby.iapi.store.access.RowUtil;
import org.apache.derby.iapi.store.access.ScanController;
import org.apache.derby.iapi.store.access.StaticCompiledOpenConglomInfo;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.RowLocation;

/**
 * Takes a conglomerate and a table filter builds a hash table on the 
 * specified column of the conglomerate on the 1st open.  Look up into the
 * hash table is done on the hash key column.  The hash table consists of
 * either <code>DataValueDescriptor[]</code>s or <code>List</code>s of
 * <code>DataValueDescriptor[]</code>. The store builds the hash table. When a
 * collision occurs, the store builds a <code>List</code> with the colliding
 * <code>DataValueDescriptor[]</code>s.
 */
public class HashScanResultSet extends ScanResultSet
	implements CursorResultSet
{
	private boolean		hashtableBuilt;
	private ExecIndexRow	startPosition;
	private ExecIndexRow	stopPosition;
	protected	ExecRow		compactRow;

	// Variable for managing next() logic on hash entry
	protected boolean	firstNext = true;
	private int			numFetchedOnNext;
	private int			entryVectorSize;
	private List		entryVector;

    // set in constructor and not altered during
    // life of object.
    private long conglomId;
    protected StaticCompiledOpenConglomInfo scoci;
	private GeneratedMethod startKeyGetter;
	private int startSearchOperator;
	private GeneratedMethod stopKeyGetter;
	private int stopSearchOperator;
	public Qualifier[][] scanQualifiers;
	public Qualifier[][] nextQualifiers;
	private int initialCapacity;
	private float loadFactor;
	private int maxCapacity;
	public String userSuppliedOptimizerOverrides;
	public boolean forUpdate;
	private boolean runTimeStatisticsOn;
	public int[] keyColumns;
	private boolean sameStartStopPosition;
	private boolean skipNullKeyColumns;
	private boolean keepAfterCommit;

	protected BackingStoreHashtable hashtable;
	protected boolean eliminateDuplicates;		// set to true in DistinctScanResultSet

	// Run time statistics
	public Properties scanProperties;
	public String startPositionString;
	public String stopPositionString;
	public int hashtableSize;
	public boolean isConstraint;

	public static final	int	DEFAULT_INITIAL_CAPACITY = -1;
	public static final float DEFAULT_LOADFACTOR = (float) -1.0;
	public static final	int	DEFAULT_MAX_CAPACITY = -1;


    //
    // class interface
    //
    HashScanResultSet(long conglomId,
		StaticCompiledOpenConglomInfo scoci, Activation activation, 
		int resultRowTemplate,
		int resultSetNumber,
		GeneratedMethod startKeyGetter, int startSearchOperator,
		GeneratedMethod stopKeyGetter, int stopSearchOperator,
		boolean sameStartStopPosition,
		Qualifier[][] scanQualifiers,
		Qualifier[][] nextQualifiers,
		int initialCapacity,
		float loadFactor,
		int maxCapacity,
		int hashKeyItem,
		String tableName,
		String userSuppliedOptimizerOverrides,
		String indexName,
		boolean isConstraint,
		boolean forUpdate,
		int colRefItem,
		int lockMode,
		boolean tableLocked,
		int isolationLevel,
		boolean skipNullKeyColumns,
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
        this.scoci = scoci;
        this.conglomId = conglomId;

		if (SanityManager.DEBUG) {
			SanityManager.ASSERT( activation!=null, "hash scan must get activation context");
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
		this.scanQualifiers = scanQualifiers;
		this.nextQualifiers = nextQualifiers;
		this.initialCapacity = initialCapacity;
		this.loadFactor = loadFactor;
		this.maxCapacity = maxCapacity;
        this.tableName = tableName;
        this.userSuppliedOptimizerOverrides = userSuppliedOptimizerOverrides;
        this.indexName = indexName;
		this.isConstraint = isConstraint;
		this.forUpdate = forUpdate;
		this.skipNullKeyColumns = skipNullKeyColumns;
		this.keepAfterCommit = activation.getResultSetHoldability();

		/* Retrieve the hash key columns */
		FormatableArrayHolder fah = (FormatableArrayHolder)
										(activation.getPreparedStatement().
											getSavedObject(hashKeyItem));
        FormatableIntHolder[] fihArray =
                fah.getArray(FormatableIntHolder[].class);
		keyColumns = new int[fihArray.length];
		for (int index = 0; index < fihArray.length; index++)
		{
			keyColumns[index] = fihArray[index].getInt();
		}

		runTimeStatisticsOn = 
            getLanguageConnectionContext().getRunTimeStatisticsMode();

        // determine whether we should fetch row locations
        setRowLocationsState();
        
		compactRow =
				getCompactRow(candidate, accessedCols, false);
		recordConstructorTime();
    }

	//
	// ResultSet interface (leftover from NoPutResultSet)
	//

	/**
	 * Can we get instantaneous locks when getting share row
	 * locks at READ COMMITTED.
	 */
	boolean canGetInstantaneousLocks() {
		return true;
	}

	/**
     * open a scan on the table. scan parameters are evaluated
     * at each open, so there is probably some way of altering
     * their values...
	 *
	 * @exception StandardException thrown on failure to open
     */
	public void	openCore() throws StandardException
	{
	    TransactionController tc;

		beginTime = getCurrentTimeMillis();
		if (SanityManager.DEBUG)
		    SanityManager.ASSERT( ! isOpen, "HashScanResultSet already open");

        // Get the current transaction controller
        tc = activation.getTransactionController();

		initIsolationLevel();

		if (startKeyGetter != null)
		{
			startPosition = (ExecIndexRow) startKeyGetter.invoke(activation);
			if (sameStartStopPosition)
			{
				stopPosition = startPosition;
			}
		}
		if (stopKeyGetter != null)
		{
			stopPosition = (ExecIndexRow) stopKeyGetter.invoke(activation);
		}

		// Check whether there are any comparisons with unordered nulls
		// on either the start or stop position.  If there are, we can
		// (and must) skip the scan, because no rows can qualify
		if (skipScan(startPosition, stopPosition))
		{
			// Do nothing
			;
		}
		else if (! hashtableBuilt)
		{
			DataValueDescriptor[] startPositionRow = 
                startPosition == null ? null : startPosition.getRowArray();
			DataValueDescriptor[] stopPositionRow = 
                stopPosition == null ? null : stopPosition.getRowArray();

            hashtable = 
                tc.createBackingStoreHashtableFromScan(
                    conglomId,          // conglomerate to open
                    (forUpdate ? TransactionController.OPENMODE_FORUPDATE : 0),
                    lockMode,
                    isolationLevel,
                    accessedCols, 
                    startPositionRow,   
                    startSearchOperator,
                    scanQualifiers,
                    stopPositionRow,   
                    stopSearchOperator,
                    -1,                 // no limit on total rows.
                    keyColumns,      
                    eliminateDuplicates,// remove duplicates?
                    -1,                 // RESOLVE - is there a row estimate?
                    maxCapacity,
                    initialCapacity,    // in memory Hashtable initial capacity
                    loadFactor,         // in memory Hashtable load factor
                    runTimeStatisticsOn,
					skipNullKeyColumns,
					keepAfterCommit,
					fetchRowLocations);

			if (runTimeStatisticsOn)
			{
				hashtableSize = hashtable.size();

				if (scanProperties == null)
				{
					scanProperties = new Properties();
				}

				try
				{
					if (hashtable != null)
					{
                        hashtable.getAllRuntimeStats(scanProperties);
					}
				}
				catch(StandardException se)
				{
					// ignore
				}
			}


			/* Remember that we created the hash table */
			hashtableBuilt = true;

			/*
			** Tell the activation about the number of qualifying rows.
			** Do this only here, not in reopen, because we don't want
			** to do this costly operation too often.
			*/
			activation.informOfRowCount(this, (long) hashtableSize);
		}

	    isOpen = true;

		resetProbeVariables();

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
					"HashScanResultSet already open");
		}

		beginTime = getCurrentTimeMillis();

		resetProbeVariables();

		numOpens++;
		openTime += getElapsedMillis(beginTime);
	}

	private void resetProbeVariables() throws StandardException
	{
		firstNext = true;
		numFetchedOnNext = 0;
		entryVector = null;
		entryVectorSize = 0;

		if (nextQualifiers != null)
		{
			clearOrderableCache(nextQualifiers);
		}
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

	    ExecRow result = null;
		DataValueDescriptor[] columns = null;

		beginTime = getCurrentTimeMillis();
	    if ( isOpen && hashtableBuilt)
	    {
			/* We use a do/while loop to ensure that we continue down
			 * the duplicate chain, if one exists, until we find a
			 * row that matches on all probe predicates (or the
			 * duplicate chain is exhausted.)
			 */
			do 
			{
				if (firstNext)
				{			  
					firstNext = false;

					/* Hash key could be either a single column or multiple columns.
					 * If a single column, then it is the datavalue wrapper, otherwise
					 * it is a KeyHasher.
					 */
					Object hashEntry;
					if (keyColumns.length == 1)
					{
						hashEntry = hashtable.get(nextQualifiers[0][0].getOrderable());
					}
					else
					{
						KeyHasher mh = new KeyHasher(keyColumns.length);

                        if (SanityManager.DEBUG)
                        {
                            SanityManager.ASSERT(nextQualifiers.length == 1);
                        }

						for (int index = 0; index < keyColumns.length; index++)
						{
                            // For hashing only use the AND qualifiers 
                            // located in nextQualifiers[0][0...N], OR 
                            // qualifiers are checked down a bit by calling
                            // qualifyRow on rows returned from hash.

                            DataValueDescriptor dvd = 
                                nextQualifiers[0][index].getOrderable();

                            if (dvd == null)
                            {
                                mh = null;
                                break;
                            }
							mh.setObject(
                                index, nextQualifiers[0][index].getOrderable());
						}
						hashEntry = (mh == null) ? null : hashtable.get(mh);
					}

					if (hashEntry instanceof List)
					{
						entryVector = (List) hashEntry;
						entryVectorSize = entryVector.size();
						columns = unpackHashValue( entryVector.get( 0 ) );
					}
					else
					{
						entryVector = null;
						entryVectorSize = 0;
						columns = unpackHashValue( hashEntry );
					}
				}
				else if (numFetchedOnNext < entryVectorSize)
				{
					// We are walking a list and there are more rows left.
					columns = unpackHashValue( entryVector.get( numFetchedOnNext ) );
				}

				if (columns != null)
				{
					// See if the entry satisfies all of the other qualifiers

					/* We've already "evaluated" the 1st keyColumns qualifiers 
                     * when we probed into the hash table, but we need to 
                     * evaluate them again here because of the behavior of 
                     * NULLs.  NULLs are treated as equal when building and 
                     * probing the hash table so that we only get a single 
                     * entry.  However, NULL does not equal NULL, so the 
                     * compare() method below will eliminate any row that
					 * has a key column containing a NULL.
                     *
                     * The following code will also evaluate any OR clauses
                     * that may exist, while the above hashing does not 
                     * include them.
					 */

					if (RowUtil.qualifyRow(columns, nextQualifiers))
					{
						setCompatRow(compactRow, columns);

						rowsSeen++;

						result = compactRow;
					}
					else
					{
						result = null;
					}

					numFetchedOnNext++;
				}
				else
				{
					result = null;
				}
			}
			while (result == null && numFetchedOnNext < entryVectorSize);

		}

		setCurrentRow(result);

		nextTime += getElapsedMillis(beginTime);
	    return result;
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
		if ( isOpen )
	    {
			// we don't want to keep around a pointer to the
			// row ... so it can be thrown away.
			// REVISIT: does this need to be in a finally
			// block, to ensure that it is executed?
		    clearCurrentRow();

			if (hashtableBuilt)
			{
				// This is where we get the scan properties for a subquery
				scanProperties = getScanProperties();
				// This is where we get the positioner info for inner tables
				if (runTimeStatisticsOn)
				{
					startPositionString = printStartPosition();
					stopPositionString = printStopPosition();
				}

				// close the hash table, eating any exception
				hashtable.close();
				hashtable = null;
				hashtableBuilt = false;
			}
			startPosition = null;
			stopPosition = null;

			super.close();
	    }
		else
			if (SanityManager.DEBUG)
				SanityManager.DEBUG("CloseRepeatInfo","Close of HashScanResultSet repeated");

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
	 * @see NoPutResultSet#requiresRelocking
	 */
	public boolean requiresRelocking()
	{
		// IndexRowToBaseRow needs to relock if we didn't keep the lock
		return(
            ((isolationLevel == 
                 TransactionController.ISOLATION_READ_COMMITTED)            ||
             (isolationLevel == 
                 TransactionController.ISOLATION_READ_COMMITTED_NOHOLDLOCK) ||
             (isolationLevel == 
                 TransactionController.ISOLATION_READ_UNCOMMITTED)));

	}

	//
	// CursorResultSet interface
	//

	/**
	 * This result set has its row location from
	 * the last fetch done. If the cursor is closed,
	 * a null is returned.
	 *
	 * @see CursorResultSet
	 *
	 * @return the row location of the current cursor row.
	 * @exception StandardException thrown on failure to get row location
	 */
	public RowLocation getRowLocation() throws StandardException
	{
		if (! isOpen) return null;

		if ( ! hashtableBuilt)
			return null;

		/* This method should only be called if the last column
		 * in the current row is a RowLocation.
		 */
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(currentRow != null,
			  "There must be a current row when fetching the row location");
			Object rlCandidate =  currentRow.getColumn(
													currentRow.nColumns());
			if (! (rlCandidate instanceof RowLocation))
			{
				SanityManager.THROWASSERT(
					"rlCandidate expected to be instanceof RowLocation, not " +
					rlCandidate.getClass().getName());
			}
		}

		return (RowLocation) currentRow.getColumn(
											currentRow.nColumns());
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
	/* RESOLVE - this should return activation.getCurrentRow(resultSetNumber),
	 * once there is such a method.  (currentRow is redundant)
	 */
	public ExecRow getCurrentRow() throws StandardException 
	{
		/* Doesn't make sense to call this method for this node since
		 * joins are not updatable.
		 */
		if (SanityManager.DEBUG)
		{
			SanityManager.THROWASSERT( 
			 "getCurrentRow() not expected to be called for HSRS");
		}

		return null;
	}

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
	 */
	private String printPosition(int searchOperator,
								 GeneratedMethod positionGetter,
								 ExecIndexRow eiRow)
	{
		String idt = "";

		String output = "";
		if (positionGetter == null)
		{
			return "\t" +
					MessageService.getTextMessage(SQLState.LANG_NONE) +
					"\n";
		}

		ExecIndexRow	positioner = null;

		try
		{
			positioner = (ExecIndexRow) positionGetter.invoke(activation);
		}
		catch (StandardException e)
		{

			if (eiRow == null)
			{
				return "\t" + MessageService.getTextMessage(
											SQLState.LANG_POSITION_NOT_AVAIL);
			}
			return "\t" + MessageService.getTextMessage(
							SQLState.LANG_UNEXPECTED_EXC_GETTING_POSITIONER) +
							"\n";
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

				// This is not internationalized because we should never
				// reach here.
				searchOp = "unknown value (" + searchOperator + ")";
				break;
		}

		output += "\t" + MessageService.getTextMessage(
										SQLState.LANG_POSITIONER,
										searchOp,
										String.valueOf(positioner.nColumns()))
										+ "\n";
			
		output += "\t" + MessageService.getTextMessage(
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
		return scanProperties;
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
}
