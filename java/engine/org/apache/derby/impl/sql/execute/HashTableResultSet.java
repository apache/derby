/*

   Derby - Class org.apache.derby.impl.sql.execute.HashTableResultSet

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

import org.apache.derby.iapi.services.loader.GeneratedMethod;

import org.apache.derby.iapi.services.monitor.Monitor;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.services.io.Storable;

import org.apache.derby.iapi.services.stream.HeaderPrintWriter;
import org.apache.derby.iapi.services.stream.InfoStreams;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.conn.StatementContext;


import org.apache.derby.iapi.sql.execute.CursorResultSet;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;

import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.ResultSet;

import org.apache.derby.iapi.store.access.Qualifier;
import org.apache.derby.iapi.store.access.RowSource;
import org.apache.derby.iapi.store.access.TransactionController;

import org.apache.derby.iapi.types.RowLocation;

import org.apache.derby.iapi.store.access.BackingStoreHashtable;
import org.apache.derby.iapi.services.io.FormatableArrayHolder;
import org.apache.derby.iapi.services.io.FormatableIntHolder;
import org.apache.derby.iapi.store.access.KeyHasher;

import org.apache.derby.catalog.types.ReferencedColumnsDescriptorImpl;

import java.util.Properties;
import java.util.Vector;

/**
 * Builds a hash table on the underlying result set tree.
 *
 * @author jerry
 */
public class HashTableResultSet extends NoPutResultSetImpl
	implements CursorResultSet 
{
	/* Run time statistics variables */
	public long restrictionTime;
	public long projectionTime;
	public int  hashtableSize;
	public Properties scanProperties;

    // set in constructor and not altered during
    // life of object.
    public NoPutResultSet source;
    public GeneratedMethod singleTableRestriction;
	public Qualifier[][] nextQualifiers;
    private GeneratedMethod projection;
	private int[]			projectMapping;
    private GeneratedMethod closeCleanup;
	private boolean runTimeStatsOn;
	private ExecRow			mappedResultRow;
	public boolean reuseResult;
	public int[]			keyColumns;
	private boolean			removeDuplicates;
	private long			maxInMemoryRowCount;
    private	int				initialCapacity;
    private	float			loadFactor;
	private boolean			skipNullKeyColumns;

	// Variable for managing next() logic on hash entry
	private boolean		firstNext = true;
	private int			numFetchedOnNext;
	private int			entryVectorSize;
	private Vector		entryVector;

	private boolean hashTableBuilt;
	private boolean firstIntoHashtable = true;

	private ExecRow nextCandidate;
	private ExecRow projRow;

	private BackingStoreHashtable ht;

    //
    // class interface
    //
    public HashTableResultSet(NoPutResultSet s,
					Activation a,
					GeneratedMethod str,
					Qualifier[][] nextQualifiers,
					GeneratedMethod p,
					int resultSetNumber,
					int mapRefItem,
					boolean reuseResult,
					int keyColItem,
					boolean removeDuplicates,
					long maxInMemoryRowCount,
					int	initialCapacity,
					float loadFactor,
					boolean skipNullKeyColumns,
				    double optimizerEstimatedRowCount,
					double optimizerEstimatedCost,
					GeneratedMethod c) 
		throws StandardException
	{
		super(a, resultSetNumber, optimizerEstimatedRowCount, optimizerEstimatedCost);
        source = s;
		// source expected to be non-null, mystery stress test bug
		// - sometimes get NullPointerException in openCore().
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(source != null,
				"HTRS(), source expected to be non-null");
		}
        singleTableRestriction = str;
		this.nextQualifiers = nextQualifiers;
        projection = p;
		projectMapping = ((ReferencedColumnsDescriptorImpl) a.getPreparedStatement().getSavedObject(mapRefItem)).getReferencedColumnPositions();
		FormatableArrayHolder fah = (FormatableArrayHolder) a.getPreparedStatement().getSavedObject(keyColItem);
		FormatableIntHolder[] fihArray = (FormatableIntHolder[]) fah.getArray(FormatableIntHolder.class);
		keyColumns = new int[fihArray.length];
		for (int index = 0; index < fihArray.length; index++)
		{
			keyColumns[index] = fihArray[index].getInt();
		}

		this.reuseResult = reuseResult;
		this.removeDuplicates = removeDuplicates;
		this.maxInMemoryRowCount = maxInMemoryRowCount;
		this.initialCapacity = initialCapacity;
		this.loadFactor = loadFactor;
		this.skipNullKeyColumns = skipNullKeyColumns;
        closeCleanup = c;

		// Allocate a result row if all of the columns are mapped from the source
		if (projection == null)
		{
			mappedResultRow = activation.getExecutionFactory().getValueRow(projectMapping.length);
		}
		constructorTime += getElapsedMillis(beginTime);

		/* Remember whether or not RunTimeStatistics is on */
		runTimeStatsOn = getLanguageConnectionContext().getRunTimeStatisticsMode();
    }

	//
	// NoPutResultSet interface 
	//

	/**
     * open a scan on the table. scan parameters are evaluated
     * at each open, so there is probably some way of altering
     * their values...
	 *
	 * @exception StandardException thrown if cursor finished.
     */
	public void	openCore() throws StandardException 
	{
	    TransactionController tc;

		beginTime = getCurrentTimeMillis();

		// source expected to be non-null, mystery stress test bug
		// - sometimes get NullPointerException in openCore().
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(source != null,
				"HTRS().openCore(), source expected to be non-null");
		}

		// REVISIT: through the direct DB API, this needs to be an
		// error, not an ASSERT; users can open twice. Only through JDBC
		// is access to open controlled and ensured valid.
		if (SanityManager.DEBUG)
		    SanityManager.ASSERT( ! isOpen, "HashTableResultSet already open");

        // Get the current transaction controller
        tc = activation.getTransactionController();

		if (! hashTableBuilt)
		{
	        source.openCore();

			/* Create and populate the hash table.  We pass
			 * ourself in as the row source.  This allows us
			 * to apply the single table predicates to the
			 * rows coming from our child as we build the
			 * hash table.
			 */
			ht = new BackingStoreHashtable(tc,
										   this,
										   keyColumns,
										   removeDuplicates,
										   (int) optimizerEstimatedRowCount,
										   maxInMemoryRowCount,
										   (int) initialCapacity,
										   loadFactor,
										   skipNullKeyColumns);

			if (runTimeStatsOn)
			{
				hashtableSize = ht.size();

				if (scanProperties == null)
				{
					scanProperties = new Properties();
				}

				try
				{
					if (ht != null)
					{
                        ht.getAllRuntimeStats(scanProperties);
					}
				}
				catch(StandardException se)
				{
					// ignore
				}
			}

			isOpen = true;
			hashTableBuilt = true;
		}

		resetProbeVariables();

		numOpens++;

		openTime += getElapsedMillis(beginTime);
	}

	/**
     * reopen a scan on the table. scan parameters are evaluated
     * at each open, so there is probably some way of altering
     * their values...
	 *
	 * @exception StandardException thrown if cursor finished.
     */
	public void	reopenCore() throws StandardException 
	{

		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(isOpen,
					"HashTableResultSet already open");
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
	    ExecRow result = null;
		DataValueDescriptor[] columns = null;

		beginTime = getCurrentTimeMillis();
	    if ( isOpen )
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

					/* Hash key could be either a single column or multiple 
                     * columns.  If a single column, then it is the datavalue 
                     * wrapper, otherwise it is a KeyHasher.
					 */
					Object hashEntry;
					if (keyColumns.length == 1)
					{
						hashEntry = ht.get(nextQualifiers[0][0].getOrderable());
					}
					else
					{
						KeyHasher mh = 
                            new KeyHasher(keyColumns.length);

						for (int index = 0; index < keyColumns.length; index++)
						{
                            // RESOLVE (mikem) - will need to change when we
                            // support OR's in qualifiers.
							mh.setObject(
                                index, nextQualifiers[0][index].getOrderable());
						}
						hashEntry = ht.get(mh);
					}

					if (hashEntry instanceof Vector)
					{
						entryVector = (Vector) hashEntry;
						entryVectorSize = entryVector.size();
						columns = 
                            (DataValueDescriptor[]) entryVector.firstElement();
					}
					else
					{
						entryVector = null;
						entryVectorSize = 0;
						columns = (DataValueDescriptor[]) hashEntry;
					}
				}
				else if (numFetchedOnNext < entryVectorSize)
				{
					/* We walking a Vector and there's 
					 * more rows left in the vector.
					 */
					columns = (DataValueDescriptor[]) 
                        entryVector.elementAt(numFetchedOnNext);
				}

				if (columns != null)
				{
					if (SanityManager.DEBUG)
					{
						// Columns is really a Storable[]
						for (int i = 0; i < columns.length; i++)
						{
							if (! (columns[0] instanceof Storable))
							{
								SanityManager.THROWASSERT(
								"columns[" + i + "] expected to be Storable, not " +
								columns[i].getClass().getName());
							}
						}
					}

					// See if the entry satisfies all of the other qualifiers
					boolean qualifies = true;

					/* We've already "evaluated" the 1st keyColumns qualifiers 
                     * when we probed into the hash table, but we need to 
                     * evaluate them again here because of the behavior of 
                     * NULLs.  NULLs are treated as equal when building and 
                     * probing the hash table so that we only get a single 
                     * entry.  However, NULL does not equal NULL, so the 
                     * compare() method below will eliminate any row that
					 * has a key column containing a NULL.
					 */

                    // RESOLVE (mikem) will have to change when qualifiers 
                    // support OR's.

                    if (SanityManager.DEBUG)
                    {
                        // we don't support 2 d qualifiers yet.
                        SanityManager.ASSERT(nextQualifiers.length == 1);
                    }
					for (int index = 0; index < nextQualifiers[0].length; index++)
					{
                        Qualifier q = nextQualifiers[0][index];

						qualifies = 
                            columns[q.getColumnId()].compare(
                                q.getOperator(),
                                q.getOrderable(),
                                q.getOrderedNulls(),
                                q.getUnknownRV());

						if (q.negateCompareResult()) 
						{ 
							qualifies = !(qualifies);
						} 

						// Stop if any predicate fails
						if (! qualifies)
						{
							break;
						}
					}

					if (qualifies)
					{

						for (int index = 0; index < columns.length; index++)
						{
							nextCandidate.setColumn(index + 1, columns[index]);
						}

						result = doProjection(nextCandidate);
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

		currentRow = result;
		setCurrentRow(result);

		nextTime += getElapsedMillis(beginTime);

		if (runTimeStatsOn)
		{
			if (! isTopResultSet)
			{
				/* This is simply for RunTimeStats */
				/* We first need to get the subquery tracking array via the StatementContext */
				StatementContext sc = activation.getLanguageConnectionContext().getStatementContext();
				subqueryTrackingArray = sc.getSubqueryTrackingArray();
			}
			nextTime += getElapsedMillis(beginTime);
		}
    	return result;
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

		if (type == CURRENT_RESULTSET_ONLY)
		{
			return	totTime - source.getTimeSpent(ENTIRE_RESULTSET_TREE);
		}
		else
		{
			return totTime;
		}
	}

	// ResultSet interface

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
	        source.close();

			super.close();

			if (hashTableBuilt)
			{
				// close the hash table, eating any exception
				ht.close();
				ht = null;
				hashTableBuilt = false;
			}
	    }
		else
			if (SanityManager.DEBUG)
				SanityManager.DEBUG("CloseRepeatInfo","Close of ProjectRestrictResultSet repeated");

		closeTime += getElapsedMillis(beginTime);
	}

	//
	// CursorResultSet interface
	//

	/**
	 * Gets information from its source. We might want
	 * to have this take a CursorResultSet in its constructor some day,
	 * instead of doing a cast here?
	 *
	 * @see CursorResultSet
	 *
	 * @return the row location of the current cursor row.
	 * @exception StandardException thrown on failure.
	 */
	public RowLocation getRowLocation() throws StandardException {
		if (SanityManager.DEBUG)
			SanityManager.ASSERT(source instanceof CursorResultSet, "source not instance of CursorResultSet");
		return ( (CursorResultSet)source ).getRowLocation();
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
	    ExecRow candidateRow = null;
	    ExecRow result = null;
	    boolean restrict = false;
	    DataValueDescriptor restrictBoolean;

		if (SanityManager.DEBUG)
			SanityManager.ASSERT(isOpen, "PRRS is expected to be open");

		/* Nothing to do if we're not currently on a row */
		if (currentRow == null)
		{
			return null;
		}

		/* Call the child result set to get it's current row.
		 * If no row exists, then return null, else requalify it
		 * before returning.
		 */
		candidateRow = ((CursorResultSet) source).getCurrentRow();
		if (candidateRow != null) {
			setCurrentRow(candidateRow);
				/* If restriction is null, then all rows qualify */
            restrictBoolean = (DataValueDescriptor) 
					((singleTableRestriction == null) ? null : singleTableRestriction.invoke(activation));

            // if the result is null, we make it false --
			// so the row won't be returned.
            restrict = (restrictBoolean == null) ||
						((! restrictBoolean.isNull()) &&
							restrictBoolean.getBoolean());
		}

	    if (candidateRow != null && restrict) 
		{
			result = doProjection(candidateRow);
        }

		currentRow = result;
		/* Clear the current row, if null */
		if (result == null) {
			clearCurrentRow();
		}

		return currentRow;
	}

	/**
	 * Do the projection against the source row.  Use reflection
	 * where necessary, otherwise get the source column into our
	 * result row.
	 *
	 * @param sourceRow		The source row.
	 *
	 * @return		The result of the projection
	 *
	 * @exception StandardException thrown on failure.
	 */
	private ExecRow doProjection(ExecRow sourceRow)
		throws StandardException
	{
		// No need to use reflection if reusing the result
		if (reuseResult && projRow != null)
		{
			return projRow;
		}

		ExecRow result;

		// Use reflection to do as much of projection as required
		if (projection != null)
		{
	        result = (ExecRow) projection.invoke(activation);
		}
		else
		{
			result = mappedResultRow;
		}

		// Copy any mapped columns from the source
		for (int index = 0; index < projectMapping.length; index++)
		{
			if (projectMapping[index] != -1)
			{
				result.setColumn(index + 1, sourceRow.getColumn(projectMapping[index]));
			}
		}

		/* We need to reSet the current row after doing the projection */
		setCurrentRow(result);

		/* Remember the result if reusing it */
		if (reuseResult)
		{
			projRow = result;
		}
		return result;
	}

	// RowSource interface
		
	/** 
	 * @see RowSource#getNextRowFromRowSource
	 * @exception StandardException on error
	 */
	public DataValueDescriptor[] getNextRowFromRowSource()
		throws StandardException
	{
		ExecRow execRow = source.getNextRowCore();

		/* Use the single table predicates, if any,
		 * to filter out rows while populating the
		 * hash table.
		 */
 		while (execRow != null)
		{
		    boolean restrict = false;
		    DataValueDescriptor restrictBoolean;

			rowsSeen++;

			/* If restriction is null, then all rows qualify */
            restrictBoolean = (DataValueDescriptor) 
					((singleTableRestriction == null) ? null : singleTableRestriction.invoke(activation));

            // if the result is null, we make it false --
			// so the row won't be returned.
            restrict = (restrictBoolean == null) ||
						((! restrictBoolean.isNull()) &&
							restrictBoolean.getBoolean());
			if (!restrict)
			{
				execRow = source.getNextRowCore();
				continue;
			}

			if (targetResultSet != null)
			{
				/* Let the target preprocess the row.  For now, this
				 * means doing an in place clone on any indexed columns
				 * to optimize cloning and so that we don't try to drain
				 * a stream multiple times.  This is where we also
				 * enforce any check constraints.
				 */
				clonedExecRow = targetResultSet.preprocessSourceRow(execRow);
			}


			/* Get a single ExecRow of the same size
			 * on the way in so that we have a row
			 * to use on the way out.
			 */
			if (firstIntoHashtable)
			{
				nextCandidate = activation.getExecutionFactory().getValueRow(execRow.nColumns());
				firstIntoHashtable = false;
			}

			return execRow.getRowArray();
		}

		return null;
	}

	/**
	 * Is this ResultSet or it's source result set for update
	 * 
	 * @return Whether or not the result set is for update.
	 */
	public boolean isForUpdate()
	{
		if (source == null) 
		{
			return false;
		}
		return source.isForUpdate();
	}

}
