/*

   Derby - Class org.apache.derby.impl.sql.execute.MaterializedResultSet

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

import org.apache.derby.iapi.services.monitor.Monitor;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.services.stream.HeaderPrintWriter;
import org.apache.derby.iapi.services.stream.InfoStreams;

import org.apache.derby.iapi.sql.ResultSet;

import org.apache.derby.iapi.types.RowLocation;

import org.apache.derby.iapi.sql.execute.CursorResultSet;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.ExecutionContext;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;
import org.apache.derby.iapi.sql.execute.ExecutionFactory;

import org.apache.derby.iapi.sql.Activation;

import org.apache.derby.iapi.services.loader.GeneratedMethod;

import org.apache.derby.iapi.store.access.ConglomerateController;
import org.apache.derby.iapi.store.access.ScanController;
import org.apache.derby.iapi.store.access.TransactionController;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.iapi.services.io.FormatableBitSet;

/**
 * Materialize the underlying ResultSet tree into a temp table on the 1st open.
 * Return rows from temp table on subsequent scans.
 */

public class MaterializedResultSet extends NoPutResultSetImpl
	implements CursorResultSet
{
	/*
    ** Set in constructor and not altered during life of object.
	*/

    public NoPutResultSet	source;



	private	ExecRow						materializedRowBuffer;
	protected long						materializedCID;
	public    boolean					materializedCreated;
	private   boolean					fromSource = true;
	protected ConglomerateController	materializedCC;
	protected ScanController			materializedScan;
	private TransactionController		tc;
	private   boolean					sourceDrained;

	public	  long						createTCTime;
	public	  long						fetchTCTime;


    private GeneratedMethod closeCleanup;

	/**
	 * Constructor for a MaterializedResultSet
	 *
	 * @param source					The NoPutResultSet from which to get rows
	 *									to be materialized
	 * @param activation				The activation for this execution
	 * @param resultSetNumber			The resultSetNumber
	 *
	 * @exception StandardException	on error
	 */

	public MaterializedResultSet(NoPutResultSet source,
							  Activation activation, int resultSetNumber,
							  double optimizerEstimatedRowCount,
							  double optimizerEstimatedCost,
							  GeneratedMethod c) throws StandardException
	{
		super(activation, resultSetNumber, 
			  optimizerEstimatedRowCount, optimizerEstimatedCost);
		this.source = source;

        // Get the current transaction controller
        tc = activation.getTransactionController();

        closeCleanup = c;
		constructorTime += getElapsedMillis(beginTime);
	}


	//
	// ResultSet interface (leftover from NoPutResultSet)
	//

	/**
     * open a scan on the source. scan parameters are evaluated
     * at each open, so there is probably some way of altering
     * their values...
	 *
 	 * @exception StandardException thrown on failure 
     */
	public void	openCore() throws StandardException
	{
		beginTime = getCurrentTimeMillis();
		if (SanityManager.DEBUG)
	    	SanityManager.ASSERT( ! isOpen, "MaterializedResultSet already open");

        source.openCore();
	    isOpen = true;
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
		boolean constantEval = true;

		beginTime = getCurrentTimeMillis();

		if (SanityManager.DEBUG)
		    SanityManager.ASSERT(isOpen, "MaterializedResultSet already open");

		// Finish draining the source into the temp table
		while (! sourceDrained)
		{
			getNextRowFromSource();
		}

		// Results will now come from the temp table
		fromSource = false;

		// Close the temp table if open
		if (materializedScan != null)
		{
			materializedScan.close();
		}

		/* Open a scan on the temp conglomerate,
		 * if one exists.
		 */
		if (materializedCID != 0)
		{
			materializedScan = 
                tc.openScan(materializedCID,
                    false,		// hold
                    0,          // for update
                    TransactionController.MODE_TABLE,
                    TransactionController.ISOLATION_SERIALIZABLE,
                    (FormatableBitSet) null, // all fields as objects
                    null,		// start key value
                    0,			// start operator
                    null,		// qualifier
                    null,		// stop key value
                    0);			// stop operator
		
			isOpen = true;
		}

		numOpens++;

		openTime += getElapsedMillis(beginTime);
	}

	/**
	 *
 	 * @exception StandardException thrown on failure 
	 */
	public ExecRow	getNextRowCore() throws StandardException
	{
		ExecRow result = null;

		beginTime = getCurrentTimeMillis();
		if (!isOpen)
			throw StandardException.newException(SQLState.LANG_RESULT_SET_NOT_OPEN, "next");

		/* Should we get the next row from the source or the materialized result set? */
		if (fromSource)
		{
			result = getNextRowFromSource();
		}
		else
		{
			result = getNextRowFromTempTable();
		}

		if (result != null)
		{
			rowsSeen++;
		}

		currentRow = result;
		setCurrentRow(currentRow);

		nextTime += getElapsedMillis(beginTime);

		return result;
	}

	/* Get the next row from the source ResultSet tree and insert into the temp table */
	private ExecRow getNextRowFromSource() throws StandardException
	{
		// Nothing to do if source is already drained
		if (sourceDrained)
		{
			return null;
		}

		ExecRow		sourceRow = null;
		ExecRow		result = null;

		sourceRow = source.getNextRowCore();

		if (sourceRow != null)
		{
			long beginTCTime = getCurrentTimeMillis();
			/* If this is the first row from the source then clone it as our own
			 * for use when fetching from temp table.
			 * This is also the place where we create the temp table.
			 */
			if (materializedRowBuffer == null)
			{
				materializedRowBuffer = sourceRow.getClone();

				tc = activation.getTransactionController();

				materializedCID = tc.createConglomerate("heap",	materializedRowBuffer.getRowArray(),
														null, null,
														TransactionController.IS_TEMPORARY |
														TransactionController.IS_KEPT);
				materializedCreated = true;
				materializedCC = 
                    tc.openConglomerate(
                        materializedCID, 
                        false,
                        TransactionController.OPENMODE_FORUPDATE,
                        TransactionController.MODE_TABLE,
                        TransactionController.ISOLATION_SERIALIZABLE);
			}
			materializedCC.insert(sourceRow.getRowArray());

			createTCTime += getElapsedMillis(beginTCTime);
		}
		// Remember whether or not we've drained the source
		else
		{
			sourceDrained = true;
		}

		return sourceRow;
	}

	/* Get the next Row from the temp table */
	private ExecRow getNextRowFromTempTable() throws StandardException
	{
		long beginTCTime = getCurrentTimeMillis();
		/* Get and return the next row from the temp conglomerate,
		 * if one exists.
		 */
		if (materializedScan != null && materializedScan.next())
		{
			materializedScan.fetch(materializedRowBuffer.getRowArray());
			fetchTCTime += getElapsedMillis(beginTCTime);
			return materializedRowBuffer;
		}
		else
		{
			return null;
		}
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
			if (closeCleanup != null) 
			{
				closeCleanup.invoke(activation); // let activation tidy up
			} 
			currentRow = null;
	        source.close();

			if (materializedScan != null)
				materializedScan.close();
			materializedScan = null;

			if (materializedCC != null)
				materializedCC.close();
			materializedCC = null;

			if (materializedCreated)
				tc.dropConglomerate(materializedCID);

			materializedCreated = false;

			super.close();
	    }
		else
			if (SanityManager.DEBUG)
				SanityManager.DEBUG("CloseRepeatInfo","Close of MaterializedResultSet repeated");

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
	 * Gets information from its source. We might want
	 * to have this take a CursorResultSet in its constructor some day,
	 * instead of doing a cast here?
	 *
	 * @see CursorResultSet
	 *
	 * @return the row location of the current cursor row.
	 *
 	 * @exception StandardException thrown on failure 
	 */
	public RowLocation getRowLocation() throws StandardException 
	{
		if (SanityManager.DEBUG)
			SanityManager.ASSERT(source instanceof CursorResultSet, "source not CursorResultSet");
		return ( (CursorResultSet)source ).getRowLocation();
	}

	/**
	 * Gets information from last getNextRow call.
	 *
	 * @see CursorResultSet
	 *
	 * @return the last row returned.
	 */
	/* RESOLVE - this should return activation.getCurrentRow(resultSetNumber),
	 * once there is such a method.  (currentRow is redundant)
	 */
	public ExecRow getCurrentRow() 
	{
		return currentRow;
	}

	//
	// class implementation
	//
}
