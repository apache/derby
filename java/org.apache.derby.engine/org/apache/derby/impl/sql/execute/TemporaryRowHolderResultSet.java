/*

   Derby - Class org.apache.derby.impl.sql.execute.TemporaryRowHolderResultSet

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

import java.sql.SQLWarning;
import java.sql.Timestamp;
import org.w3c.dom.Element;

import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.ResultDescription;
import org.apache.derby.iapi.sql.ResultSet;
import org.apache.derby.iapi.sql.Row;
import org.apache.derby.iapi.sql.dictionary.TriggerDescriptor;
import org.apache.derby.iapi.sql.execute.CursorResultSet;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;
import org.apache.derby.iapi.sql.execute.RowChanger;
import org.apache.derby.iapi.sql.execute.TargetResultSet;
import org.apache.derby.iapi.store.access.ConglomerateController;
import org.apache.derby.iapi.store.access.ScanController;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.derby.iapi.types.SQLLongint;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;

/**
 * A result set to scan temporary row holders.  Ultimately, this
 * may be returned to users, hence the extra junk from the ResultSet
 * interface.
 *
 */
//IC see: https://issues.apache.org/jira/browse/DERBY-1112
class TemporaryRowHolderResultSet implements CursorResultSet, NoPutResultSet, Cloneable
{
	private ExecRow[] 				rowArray;
	private int						numRowsOut;
	private ScanController			scan;
	private TransactionController	tc;
	private boolean 				isOpen;
	private boolean 				finished;
	private ExecRow					currentRow;
	private ResultDescription		resultDescription;
	private boolean                 isAppendable = false;
	private long                    positionIndexConglomId;
	private boolean 				isVirtualMemHeap;
	private boolean 				currRowFromMem;
	private TemporaryRowHolderImpl	holder;

	// the following is used by position based scan, as well as virtual memory style heap
	ConglomerateController			heapCC;
	private RowLocation				baseRowLocation;

	/**
	 * Constructor
	 *
	 * @param tc the xact controller
	 * @param rowArray the row array
	 * @param resultDescription value returned by getResultDescription()
	 */
	public TemporaryRowHolderResultSet
	(
		TransactionController		tc,
		ExecRow[]					rowArray,
//IC see: https://issues.apache.org/jira/browse/DERBY-4610
//IC see: https://issues.apache.org/jira/browse/DERBY-3049
		ResultDescription			resultDescription,
		boolean						isVirtualMemHeap,
		TemporaryRowHolderImpl		holder
   	)
	{

		this(tc, rowArray, resultDescription, isVirtualMemHeap, false, 0, holder);


	}

	/**
	 * Constructor
	 *
	 * @param tc the xact controller
	 * @param rowArray the row array
	 * @param resultDescription value returned by getResultDescription()
	 * @param isAppendable true,if we can insert rows after this result is created
	 * @param positionIndexConglomId conglomId of the index which has order rows
	 *                               are inserted and their row location 
	 */
	public TemporaryRowHolderResultSet
	(
		TransactionController		tc,
		ExecRow[]					rowArray,
//IC see: https://issues.apache.org/jira/browse/DERBY-4610
//IC see: https://issues.apache.org/jira/browse/DERBY-3049
		ResultDescription			resultDescription,
		boolean						isVirtualMemHeap,
		boolean                     isAppendable,
		long                        positionIndexConglomId,
		TemporaryRowHolderImpl		holder
	) 
	{
		this.tc = tc;
		this.rowArray = rowArray;
//IC see: https://issues.apache.org/jira/browse/DERBY-4610
//IC see: https://issues.apache.org/jira/browse/DERBY-3049
		this.resultDescription = resultDescription;
		this.numRowsOut = 0;
		isOpen = false;
		finished = false;
		this.isVirtualMemHeap = isVirtualMemHeap;
		this.isAppendable = isAppendable;
		this.positionIndexConglomId = positionIndexConglomId;

		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(rowArray != null, "rowArray is null");
			SanityManager.ASSERT(rowArray.length > 0, "rowArray has no elements, need at least one");
		}

		this.holder = holder;
	}

	/**
	 * Reset the exec row array and reinitialize
	 *
	 * @param rowArray the row array
	 */
	public void reset(ExecRow[]	rowArray)
	{
		this.rowArray = rowArray;
		this.numRowsOut = 0;
		isOpen = false;
		finished = false;

		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(rowArray != null, "rowArray is null");
			SanityManager.ASSERT(rowArray.length > 0, "rowArray has no elements, need at least one");
		}
	}


	/**
	 * postion scan to start from after where we stopped earlier
	 */
	public void reStartScan(long currentConglomId, long pconglomId) throws  StandardException 
	{
		if(isAppendable)
		{
//IC see: https://issues.apache.org/jira/browse/DERBY-3221
            if (SanityManager.DEBUG) {
                SanityManager.ASSERT(currentConglomId == holder.getTemporaryConglomId(),
                        "currentConglomId(" + currentConglomId + 
                        ") == holder.getTemporaryConglomeateId (" + 
                        holder.getTemporaryConglomId() + ")");
            }
			positionIndexConglomId = pconglomId;
			setupPositionBasedScan(numRowsOut);
		}else
		{
			numRowsOut--;
		}
	}

	//Make an array which is a superset of the 2 passed column arrays.
	//The superset will not have any duplicates
	private static int[] supersetofAllColumns(int[] columnsArray1, int[] columnsArray2)
	{
		int maxLength = columnsArray1.length + columnsArray2.length;
		int[] maxArray = new int[maxLength];
		for (int i=0; i<maxLength; i++) maxArray[i]=-1;
		
		//First simply copy the first array into superset
		for (int i=0; i<columnsArray1.length; i++) {
			maxArray[i] = columnsArray1[i];
		}
		
		//Now copy only new values from second array into superset
		int validColsPosition=columnsArray1.length;
		for (int i=0; i<columnsArray2.length; i++) {
			boolean found = false;
			for (int j=0;j<validColsPosition;j++) {
				if (maxArray[j] == columnsArray2[i]) {
					found = true;
					break;
				}
			}
			if (!found) {
				maxArray[validColsPosition] = columnsArray2[i];
				validColsPosition++;
			}
		}
		maxArray = shrinkArray(maxArray);
		java.util.Arrays.sort(maxArray);
		return maxArray;
	}

	//The passed array can have some -1 elements and some +ve elements
	// Return an array containing just the +ve elements
	private static int[] shrinkArray(int[] columnsArrary) {
		int countOfColsRefedInArray = 0;
		int numberOfColsInTriggerTable = columnsArrary.length;

		//Count number of non -1 entries
		for (int i=0; i < numberOfColsInTriggerTable; i++) {
			if (columnsArrary[i] != -1)
				countOfColsRefedInArray++;
		}

		if (countOfColsRefedInArray > 0){
			int[] tempArrayOfNeededColumns = new int[countOfColsRefedInArray];
			int j=0;
			for (int i=0; i < numberOfColsInTriggerTable; i++) {
				if (columnsArrary[i] != -1)
					tempArrayOfNeededColumns[j++] = columnsArrary[i];
			}
			return tempArrayOfNeededColumns;
		} else
			return null;
	}

	//Return an array which contains the column positions of all the 
	// +ve columns in the passed array
	private static int[] justTheRequiredColumnsPositions(int[] columnsArrary) {
		int countOfColsRefedInArray = 0;
		int numberOfColsInTriggerTable = columnsArrary.length;

		//Count number of non -1 entries
		for (int i=0; i < numberOfColsInTriggerTable; i++) {
			if (columnsArrary[i] != -1)
				countOfColsRefedInArray++;
		}

		if (countOfColsRefedInArray > 0){
			int[] tempArrayOfNeededColumns = new int[countOfColsRefedInArray];
			int j=0;
			for (int i=0; i < numberOfColsInTriggerTable; i++) {
				if (columnsArrary[i] != -1)
					tempArrayOfNeededColumns[j++] = i+1;
			}
			return tempArrayOfNeededColumns;
		} else
			return null;
	}

	/**
	 * Whip up a new Temp ResultSet that has a single
	 * row. This row will either have all the columns from
	 * the current row of the passed resultset or a subset 
	 * of the columns from the passed resulset. It all depends
	 * on what columns are needed by the passed trigger and what
	 * columns exist in the resulset. The Temp resulset
	 * should only have the columns required by the trigger.
	 * 
	 * @param triggerd We are building Temp resultset for this trigger
	 * @param activation the activation
	 * @param rs the result set 
	 * @param colsReadFromTable The passed resultset is composed of
	 *   these columns. We will create a temp resultset which
	 *   will have either all these columns or only a subset of
	 *   these columns. It all depends on what columns are needed
	 *   by the trigger. If this param is null, then that means that
	 *   all the columns from the trigger table have been read into
	 *   the passed resultset.
	 * 
	 * @return a single row result set
	 *
	 * @exception StandardException on error
	 */
	public static TemporaryRowHolderResultSet getNewRSOnCurrentRow
	(
			TriggerDescriptor triggerd,
		Activation				activation,
		CursorResultSet 		rs,
		int[]	colsReadFromTable
	) throws StandardException
	{
		TemporaryRowHolderImpl singleRow;
		DataDictionary dd = activation.getLanguageConnectionContext().getDataDictionary();
		// In soft upgrade mode, we could be dealing with databases created 
		// with 10.8 or prior and for such databases, we do not want to do
		// any column reading optimization to maintain backward compatibility
		if (!dd.checkVersion(DataDictionary.DD_VERSION_DERBY_10_9,null)) {
	    	singleRow =
	    		new TemporaryRowHolderImpl(activation, null,
						   rs.getResultDescription());
			singleRow.insert(rs.getCurrentRow());
			return (TemporaryRowHolderResultSet) singleRow.getResultSet();
		}

		//Get columns referenced in trigger action through REFERENCING clause
    	int[] referencedColsInTriggerAction = triggerd.getReferencedColsInTriggerAction();
    	// Get trigger column. If null, then it means that all the columns
    	// have been read because this trigger can be fired for any of the
    	// columns in the table
    	int[] referencedColsInTrigger = triggerd.getReferencedCols();

	    if ((referencedColsInTrigger != null) && //this means not all the columns are being read
	    		(triggerd.isRowTrigger() && referencedColsInTriggerAction!=null &&
	    		referencedColsInTriggerAction.length != 0)) {
	    	//If we are here, then trigger is defined on specific columns and
	    	// it has trigger action columns used through REFERENCING clause

	    	//Make an array which is a superset of trigger columns and 
	    	// trigger action columns referenced through REFERENCING clause.
	    	//This superset is what the trigger is looking for in it's
	    	// resulset. 
		    int[] colsInTrigger = supersetofAllColumns(referencedColsInTrigger,referencedColsInTriggerAction);
	    	int colsCountInTrigger = colsInTrigger.length;
		    int[] colsReallyNeeded = new int[colsCountInTrigger];

		    //Here, we find out what columns make up the passed resulset
	    	int[] actualColsReadFromTable;
    		if (colsReadFromTable != null) //this means not all the columns are being read
    			actualColsReadFromTable = justTheRequiredColumnsPositions(colsReadFromTable);
    		else {
    			int colsInTriggerTable = triggerd.getTableDescriptor().getNumberOfColumns();
    			actualColsReadFromTable = new int[colsInTriggerTable];
    			for (int i=1; i<=colsInTriggerTable; i++)
    				actualColsReadFromTable[i-1] = i;
    		}
	    	
    		//Now we have what columns make up the passed resulset and what
    		// columns are needed by the trigger. We will map a temporary
    		// resultset for the trigger out of the above information using
    		// the passed resultset
	    	int indexInActualColsReadFromTable = 0;
	    	for (int i=0; i<colsCountInTrigger; i++) {

	    		for (;indexInActualColsReadFromTable < actualColsReadFromTable.length; indexInActualColsReadFromTable++)
	    		{
	    			/* Return 1-based key column position if column is in the key */
	    			if (actualColsReadFromTable[indexInActualColsReadFromTable] 
	    			                            == colsInTrigger[i])
	    			{
	    				colsReallyNeeded[i] = indexInActualColsReadFromTable+1;
	    				break;
	    			}
	    		}
	    	}    			
	    	singleRow =
	    		new TemporaryRowHolderImpl(activation, null,
	    				activation.getLanguageConnectionContext().getLanguageFactory().
	    				getResultDescription(rs.getResultDescription(),colsReallyNeeded));
			ExecRow				row = activation.getExecutionFactory().getValueRow( colsCountInTrigger );
			for (int i=0; i<colsCountInTrigger; i++)
				row.setColumn(i+1, rs.getCurrentRow().getColumn(colsReallyNeeded[i]));
			singleRow.insert(row);
	    } else {
	    	singleRow =
//IC see: https://issues.apache.org/jira/browse/DERBY-4610
//IC see: https://issues.apache.org/jira/browse/DERBY-3049
	    		new TemporaryRowHolderImpl(activation, null,
						   rs.getResultDescription());
			singleRow.insert(rs.getCurrentRow());
	    }
	    
		return (TemporaryRowHolderResultSet) singleRow.getResultSet();
	}

	/////////////////////////////////////////////////////////
	//
	// NoPutResultSet
	// 
	/////////////////////////////////////////////////////////
	/**
	 * Mark the ResultSet as the topmost one in the ResultSet tree.
	 * Useful for closing down the ResultSet on an error.
	 */
	public void markAsTopResultSet()
	{ }

	/**
	 * Open the scan and evaluate qualifiers and the like.
	 * For us, there are no qualifiers, this is really a
	 * noop.
	 */
	public void openCore() throws StandardException
	{
		this.numRowsOut = 0;
		isOpen = true;
		currentRow = null;

		if(isAppendable)
			setupPositionBasedScan(numRowsOut);
	}

	/**
	 * Reopen the scan.  Typically faster than open()/close()
	 *
	 * @exception StandardException on error
	 */
	public void reopenCore() throws StandardException
	{
		numRowsOut = 0;
		isOpen = true;
		currentRow = null;

		if(isAppendable)
		{
			setupPositionBasedScan(numRowsOut);
			return;
		}

		if (scan != null)
		{
			scan.reopenScan(
                (DataValueDescriptor[]) null,		// start key value
                0,						// start operator
                null,					// qualifier
                (DataValueDescriptor[]) null,		// stop key value
                0);						// stop operator
		}
	}

	/**
	 * Get the next row.
	 *
	 * @return the next row, or null if none
	 *
	 * @exception StandardException on error
	 */
	public ExecRow getNextRowCore()
		throws StandardException
	{

		if (!isOpen)
		{
			return (ExecRow)null;
		}
			
		if(isAppendable)
		{
			return getNextAppendedRow() ;
		}

		if (isVirtualMemHeap && holder.lastArraySlot >= 0)
		{
			numRowsOut++;
			currentRow = rowArray[holder.lastArraySlot];
			currRowFromMem = true;
			return currentRow;
		}
		else if (numRowsOut++ <= holder.lastArraySlot)
		{
			currentRow = rowArray[numRowsOut-1];
			return currentRow;
		}

//IC see: https://issues.apache.org/jira/browse/DERBY-3221
		if (holder.getTemporaryConglomId() == 0)
		{
			return (ExecRow)null;
		}
			
		/*
		** Advance in the temporary conglomerate
		*/
		if (scan == null)
		{
			scan = 
                tc.openScan(
//IC see: https://issues.apache.org/jira/browse/DERBY-3221
                    holder.getTemporaryConglomId(),
                    false,					// hold
                    0, 		// open read only
                    TransactionController.MODE_TABLE,
                    TransactionController.ISOLATION_SERIALIZABLE,
                    (FormatableBitSet) null, 
                    (DataValueDescriptor[]) null,		// start key value
                    0,						// start operator
                    null,					// qualifier
                    (DataValueDescriptor[]) null,		// stop key value
                    0);						// stop operator
		}
		else if (isVirtualMemHeap && holder.state == TemporaryRowHolderImpl.STATE_INSERT)
		{
			holder.state = TemporaryRowHolderImpl.STATE_DRAIN;
			scan.reopenScan(
                (DataValueDescriptor[]) null,		// start key value
                0,						// start operator
                null,					// qualifier
                (DataValueDescriptor[]) null,		// stop key value
                0);						// stop operator
		}

		if (scan.next())
		{
			currentRow = rowArray[0].getNewNullRow();
			scan.fetch(currentRow.getRowArray());
			currRowFromMem = false;
			return currentRow;
		}
		return (ExecRow)null;
	}

	public void deleteCurrentRow()
		throws StandardException
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(isVirtualMemHeap, "deleteCurrentRow is not implemented");
		}
		if (currRowFromMem)
		{
			if (holder.lastArraySlot > 0)				// 0 is kept for template
				rowArray[holder.lastArraySlot] = null;  // erase reference
			holder.lastArraySlot--;
		}
		else
		{
			if (baseRowLocation == null)
				baseRowLocation = scan.newRowLocationTemplate();
			scan.fetchLocation(baseRowLocation);
			if(heapCC == null)
			{
//IC see: https://issues.apache.org/jira/browse/DERBY-3221
                heapCC = tc.openConglomerate(holder.getTemporaryConglomId(),
											  false,
											  TransactionController.OPENMODE_FORUPDATE,
											  TransactionController.MODE_TABLE,
											  TransactionController.ISOLATION_SERIALIZABLE);
			}
			heapCC.delete(baseRowLocation);
		}
	}


	//following variables are specific to the position based scans.
	DataValueDescriptor[] indexRow;
	ScanController indexsc;

	//open the scan of the temporary heap and the position index
	private void setupPositionBasedScan(long position) throws StandardException
	{

		//incase nothing is inserted yet into the temporary row holder
//IC see: https://issues.apache.org/jira/browse/DERBY-3221
        if (holder.getTemporaryConglomId() == 0)
			return;
		if(heapCC == null)
		{
			heapCC = tc.openConglomerate( holder.getTemporaryConglomId(),
										  false,
										  0,
										  TransactionController.MODE_TABLE,
										  TransactionController.ISOLATION_SERIALIZABLE);

		}

		currentRow = rowArray[0].getNewNullRow();
		indexRow = new DataValueDescriptor[2];
		indexRow[0] = new SQLLongint(position);
		indexRow[1] = 	heapCC.newRowLocationTemplate();

		DataValueDescriptor[] searchRow =  new DataValueDescriptor[1];
		searchRow[0] = new SQLLongint(position);

		if(indexsc == null)
		{
			indexsc = tc.openScan(positionIndexConglomId,
								  false,                           // don't hold open across commit
								  0,                               // for read
								  TransactionController.MODE_TABLE,
								  TransactionController.ISOLATION_SERIALIZABLE,
								  (FormatableBitSet) null,                  // all fields as objects
								  searchRow,            	          // start position - first row
								  ScanController.GE,               // startSearchOperation
								  null,                            //scanQualifier,
								  null,                           // stop position - through last row
								  ScanController.GT);              // stopSearchOperation
		}else
		{

			indexsc.reopenScan(
						searchRow,                      	// startKeyValue
						ScanController.GE,            		// startSearchOp
						null,                         		// qualifier
						null, 		                        // stopKeyValue
						ScanController.GT             		// stopSearchOp 
						);
		}
		
	} 


	//get the next row inserted into the temporary holder
	private ExecRow getNextAppendedRow() throws StandardException
	{
		if (indexsc == null) return null;
		if (!indexsc.fetchNext(indexRow))
		{
			return null;
		}
		
		RowLocation baseRowLocation =  (RowLocation) indexRow[1];
		boolean base_row_exists = 
            heapCC.fetch(
                baseRowLocation, currentRow.getRowArray(), (FormatableBitSet) null);

        if (SanityManager.DEBUG)
        {
            SanityManager.ASSERT(base_row_exists, "base row disappeared.");
        }
		numRowsOut++; 
		return currentRow;
	}



	/**
	 * Return the point of attachment for this subquery.
	 * (Only meaningful for Any and Once ResultSets, which can and will only
	 * be at the top of a ResultSet for a subquery.)
	 *
	 * @return int	Point of attachment (result set number) for this
	 *			    subquery.  (-1 if not a subquery - also Sanity violation)
	 */
	public int getPointOfAttachment()
	{
		return -1;
	}

	/**
	 * Return the isolation level of the scan in the result set.
	 * Only expected to be called for those ResultSets that
	 * contain a scan.
	 *
	 * @return The isolation level of the scan (in TransactionController constants).
	 */
	public int getScanIsolationLevel()
	{
		return TransactionController.ISOLATION_SERIALIZABLE;	
	}

	/**
	 * Notify a NPRS that it is the source for the specified 
	 * TargetResultSet.  This is useful when doing bulk insert.
	 *
	 * @param trs	The TargetResultSet.
	 */
	public void setTargetResultSet(TargetResultSet trs)
	{
	}

	/**
	 * Set whether or not the NPRS need the row location when acting
	 * as a row source.  (The target result set determines this.)
	 * 
	 */
	public void setNeedsRowLocation(boolean needsRowLocation)
	{
	}

	/**
	 * Get the estimated row count from this result set.
	 *
	 * @return	The estimated row count (as a double) from this result set.
	 */
	public double getEstimatedRowCount()
	{
		return 0d;
	}

	/**
	 * Get the number of this ResultSet, which is guaranteed to be unique
	 * within a statement.
	 */
	public int resultSetNumber()
	{
		return TEMPORARY_RESULT_SET_NUMBER;
	}

	/**
	 * Set the current row to the row passed in.
	 *
	 * @param row the new current row
	 *
	 */
	public void setCurrentRow(ExecRow row)
	{
		currentRow = row;
	}

	/**
	 * Clear the current row
	 *
	 */
	public void clearCurrentRow()
	{
		currentRow = null;
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
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(isOpen, "resultSet expected to be open");
		}

		return currentRow;
	}

	/**
	 * Returns the row location of the current base table row of the cursor.
	 * If this cursor's row is composed of multiple base tables' rows,
	 * i.e. due to a join, then a null is returned.  For
	 * a temporary row holder, we always return null.
	 *
	 * @return the row location of the current cursor row.
	 */
	public RowLocation getRowLocation()
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(isOpen, "resultSet expected to be open");
		}
		return (RowLocation)null;
	}


	/**
	 * Clean up
	 *
	 * @exception StandardException thrown on error
	 */
	public void	close() throws StandardException
	{
		isOpen = false;
		numRowsOut = 0;
		currentRow = null;
		if (scan != null)
		{
			scan.close();
			scan = null;
		}
	}


	//////////////////////////////////////////////////////////////////////////
	//
	// MISC FROM RESULT SET
	//
	/////////////////////////////////////////////////////////////////////////

	/**
	 * Returns TRUE if the statement returns rows (i.e. is a SELECT
	 * or FETCH statement), FALSE if it returns no rows.
	 *
	 * @return	TRUE if the statement returns rows, FALSE if not.
	 */
	public boolean	returnsRows()
	{
		return true;
	}

	public long modifiedRowCount() { return 0L;};

	/**
	 * Returns a ResultDescription object, which describes the results
	 * of the statement this ResultSet is in. This will *not* be a
	 * description of this particular ResultSet, if this is not the
	 * outermost ResultSet.
	 *
	 * @return	A ResultDescription describing the results of the
	 *		statement.
	 */
	public ResultDescription	getResultDescription()
	{
//IC see: https://issues.apache.org/jira/browse/DERBY-4610
//IC see: https://issues.apache.org/jira/browse/DERBY-3049
		return resultDescription;
	}

	/**
	 * Tells the system that there will be calls to getNextRow().
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public void open() throws StandardException
	{
		openCore();
	}

	/**
	 * Returns the row at the absolute position from the query, 
	 * and returns NULL when there is no such position.
	 * (Negative position means from the end of the result set.)
	 * Moving the cursor to an invalid position leaves the cursor
	 * positioned either before the first row (negative position)
	 * or after the last row (positive position).
	 * NOTE: An exception will be thrown on 0.
	 *
	 * @param row	The position.
	 * @return	The row at the absolute position, or NULL if no such position.
	 *
	 * @exception StandardException		Thrown on failure
	 * @see Row
	 */
	public ExecRow	getAbsoluteRow(int row) throws StandardException
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.THROWASSERT(
				"getAbsoluteRow() not expected to be called yet.");
		}

		return null;
	}

	/**
	 * Returns the row at the relative position from the current
	 * cursor position, and returns NULL when there is no such position.
	 * (Negative position means toward the beginning of the result set.)
	 * Moving the cursor to an invalid position leaves the cursor
	 * positioned either before the first row (negative position)
	 * or after the last row (positive position).
	 * NOTE: 0 is valid.
	 * NOTE: An exception is thrown if the cursor is not currently
	 * positioned on a row.
	 *
	 * @param row	The position.
	 * @return	The row at the relative position, or NULL if no such position.
	 *
	 * @exception StandardException		Thrown on failure
	 * @see Row
	 */
	public ExecRow	getRelativeRow(int row) throws StandardException
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.THROWASSERT(
				"getRelativeRow() not expected to be called yet.");
		}

		return null;
	}

	/**
	 * Sets the current position to before the first row and returns NULL
	 * because there is no current row.
	 *
	 * @return	NULL.
	 *
	 * @exception StandardException		Thrown on failure
	 * @see Row
	 */
	public ExecRow	setBeforeFirstRow() 
		throws StandardException
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.THROWASSERT(
				"setBeforeFirstRow() not expected to be called yet.");
		}

		return null;
	}

	/**
	 * Returns the first row from the query, and returns NULL when there
	 * are no rows.
	 *
	 * @return	The first row, or NULL if no rows.
	 *
	 * @exception StandardException		Thrown on failure
	 * @see Row
	 */
	public ExecRow	getFirstRow() 
		throws StandardException
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.THROWASSERT(
				"getFirstRow() not expected to be called yet.");
		}

		return null;
	}

	/**
	 * Returns the next row from the query, and returns NULL when there
	 * are no more rows.
	 *
	 * @return	The next row, or NULL if no more rows.
	 *
	 * @exception StandardException		Thrown on failure
	 * @see Row
	 */
	public ExecRow	getNextRow() throws StandardException
	{
		return getNextRowCore();
	}

	/**
	 * Returns the previous row from the query, and returns NULL when there
	 * are no more previous rows.
	 *
	 * @return	The previous row, or NULL if no more previous rows.
	 *
	 * @exception StandardException		Thrown on failure
	 * @see Row
	 */
	public ExecRow	getPreviousRow() 
		throws StandardException
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.THROWASSERT(
				"getPreviousRow() not expected to be called yet.");
		}

		return null;
	}

	/**
	 * Returns the last row from the query, and returns NULL when there
	 * are no rows.
	 *
	 * @return	The last row, or NULL if no rows.
	 *
	 * @exception StandardException		Thrown on failure
	 * @see Row
	 */
	public ExecRow	getLastRow()
		throws StandardException
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.THROWASSERT(
				"getLastRow() not expected to be called yet.");
		}

		return null;
	}

	/**
	 * Sets the current position to after the last row and returns NULL
	 * because there is no current row.
	 *
	 * @return	NULL.
	 *
	 * @exception StandardException		Thrown on failure
	 * @see Row
	 */
	public ExecRow	setAfterLastRow() 
		throws StandardException
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.THROWASSERT(
				"getLastRow() not expected to be called yet.");
		}

		return null;
	}

    /**
     * Determine if the cursor is before the first row in the result 
     * set.   
     *
     * @return true if before the first row, false otherwise. Returns
     * false when the result set contains no rows.
     */
    public boolean checkRowPosition(int isType)
	{
		return false;
	}

	/**
	 * Returns the row number of the current row.  Row
	 * numbers start from 1 and go to 'n'.  Corresponds
	 * to row numbering used to position current row
	 * in the result set (as per JDBC).
	 *
	 * @return	the row number, or 0 if not on a row
	 *
	 */
	public int getRowNumber()
	{
		return 0;
	}

	/**
	 * Tells the system to clean up on an error.
	 *
	 * @exception StandardException		Thrown on error.
	 */
	public void	cleanUp() throws StandardException
	{
		close();
	}


	/**
		Find out if the ResultSet is closed or not.
		Will report true for result sets that do not return rows.

		@return true if the ResultSet has been closed.
	 */
	public boolean isClosed()
	{
		return !isOpen;
	}

	/**
	 * Tells the system that there will be no more access
	 * to any database information via this result set;
	 * in particular, no more calls to open().
	 * Will close the result set if it is not already closed.
	 *
	 * @exception StandardException	on error
	 */
	public void finish() throws StandardException
	{
		finished = true;
		close();
	}


	/**
	 * Get the execution time in milliseconds.
	 *
	 * @return long		The execution time in milliseconds.
	 */
	public long getExecuteTime()
	{
		return 0L;
	}

	/**
	 * @see ResultSet#getAutoGeneratedKeysResultset
	 */
	public ResultSet getAutoGeneratedKeysResultset()
	{
		//A non-null resultset would be returned only for an insert statement 
		return (ResultSet)null;
	}

	/**
	 * Get the Timestamp for the beginning of execution.
	 *
	 * @return Timestamp		The Timestamp for the beginning of execution.
	 */
	public Timestamp getBeginExecutionTimestamp()
	{
		return (Timestamp)null;
	}

	/**
	 * Get the Timestamp for the end of execution.
	 *
	 * @return Timestamp		The Timestamp for the end of execution.
	 */
	public Timestamp getEndExecutionTimestamp()
	{
		return (Timestamp)null;
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
		return 0L;
	}


	/**
	 * Get the subquery ResultSet tracking array from the top ResultSet.
	 * (Used for tracking open subqueries when closing down on an error.)
	 *
	 * @param numSubqueries		The size of the array (For allocation on demand.)
	 *
	 * @return NoPutResultSet[]	Array of NoPutResultSets for subqueries.
	 */
	public NoPutResultSet[] getSubqueryTrackingArray(int numSubqueries)
	{
		return (NoPutResultSet[])null;
	}

	/**
	 * Returns the name of the cursor, if this is cursor statement of some
	 * type (declare, open, fetch, positioned update, positioned delete,
	 * close).
	 *
	 * @return	A String with the name of the cursor, if any. Returns
	 *		NULL if this is not a cursor statement.
	 */
	public String	getCursorName()
	{
		return (String) null;
	}

	/**
	 * @see NoPutResultSet#requiresRelocking
	 */
	public boolean requiresRelocking()
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.THROWASSERT(
				"requiresRelocking() not expected to be called for " +
				getClass().getName());
		}
		return false;
	}

	/////////////////////////////////////////////////////////
	//
	// Access/RowSource -- not implemented
	// 
	/////////////////////////////////////////////////////////
	/**
		Get the next row as an array of column objects. The column objects can
		be a JBMS Storable or any
		Serializable/Externalizable/Formattable/Streaming type.
		<BR>
		A return of null indicates that the complete set of rows has been read.

		<p>
		A null column can be specified by leaving the object null, or indicated
		by returning a non-null getValidColumns.  On streaming columns, it can
		be indicated by returning a non-null get FieldStates.

		<p>
        If RowSource.needToClone() is true then the returned row (the 
        DataValueDescriptor[]) is guaranteed not to be modified by drainer of 
        the RowSource (except that the input stream will be read, of course) 
        and drainer will keep no reference to it before making the subsequent 
        nextRow call.  So it is safe to return the same DataValueDescriptor[] 
        in subsequent nextRow calls if that is desirable for performance 
        reasons.  
		<p>
        If RowSource.needToClone() is false then the returned row (the 
        DataValueDescriptor[]) may be be modified by drainer of the RowSource, 
        and the drainer may keep a reference to it after making the subsequent 
        nextRow call.  In this case the client should severe all references to 
        the row after returning it from getNextRowFromRowSource().

		@exception StandardException Standard Derby Error Policy
	 */
	public DataValueDescriptor[] getNextRowFromRowSource() throws StandardException
	{ 
		return null;
	}

	/**
        Does the caller of getNextRowFromRowSource() need to clone the row
        in order to keep a reference to the row past the 
        getNextRowFromRowSource() call which returned the row.  This call
        must always return the same for all rows in a RowSource (ie. the
        caller will call this once per scan from a RowSource and assume the
        behavior is true for all rows in the RowSource).

	 */
	public boolean needsToClone()
	{
		return false;
	}


	/**
	  getValidColumns describes the DataValueDescriptor[] returned by all 
      calls to the getNextRowFromRowSource() call. 

	  If getValidColumns returns null, the number of columns is given by the
	  DataValueDescriptor.length where DataValueDescriptor[] is returned by the
      preceeding getNextRowFromRowSource() call.  Column N maps to 
      DataValueDescriptor[N], where column numbers start at zero.

	  If getValidColumns return a non null validColumns FormatableBitSet the number of
	  columns is given by the number of bits set in validColumns.  Column N is
	  not in the partial row if validColumns.get(N) returns false.  Column N is
	  in the partial row if validColumns.get(N) returns true.  If column N is
	  in the partial row then it maps to DataValueDescriptor[M] where M is the
//IC see: https://issues.apache.org/jira/browse/DERBY-6856
      count of calls to validColumns.get(i) that return true where i &lt; N.  If
	  DataValueDescriptor.length is greater than the number of columns 
      indicated by validColumns the extra entries are ignored.  
	*/
	public FormatableBitSet getValidColumns()
	{
		return null;
	}

	/**
		closeRowSource tells the RowSource that it will no longer need to
		return any rows and it can release any resource it may have.
		Subsequent call to any method on the RowSource will result in undefined
		behavior.  A closed rowSource can be closed again.
	*/
	public void closeRowSource()
	{ }


	/////////////////////////////////////////////////////////
	//
	// Access/RowLocationRetRowSource -- not implemented
	// 
	/////////////////////////////////////////////////////////
	/**
		needsRowLocation returns true iff this the row source expects the
		drainer of the row source to call rowLocation after getting a row from
		getNextRowFromRowSource.

		@return true iff this row source expects some row location to be
		returned 
		@see #rowLocation
	 */
	public boolean needsRowLocation()
	{
		return false;
	}

    public void setHasDeferrableChecks() {
//IC see: https://issues.apache.org/jira/browse/DERBY-532
        if (SanityManager.DEBUG) {
            SanityManager.NOTREACHED();
        }
    }

    public boolean needsRowLocationForDeferredCheckConstraints()
    {
        return false;
    }


	/**
		rowLocation is a callback for the drainer of the row source to return
		the rowLocation of the current row, i.e, the row that is being returned
		by getNextRowFromRowSource.  This interface is for the purpose of
		loading a base table with index.  In that case, the indices can be
		built at the same time the base table is laid down once the row
		location of the base row is known.  This is an example pseudo code on
		how this call is expected to be used:
		
		<BR><pre>
		boolean needsRL = rowSource.needsRowLocation();
		DataValueDescriptor[] row;
		while((row = rowSource.getNextRowFromRowSource()) != null)
		{
			RowLocation rl = heapConglomerate.insertRow(row);
			if (needsRL)
				rowSource.rowLocation(rl);
		}
		</pre><BR>

		NeedsRowLocation and rowLocation will ONLY be called by a drainer of
		the row source which CAN return a row location.  Drainer of row source
		which cannot return rowLocation will guarentee to not call either
		callbacks. Conversely, if NeedsRowLocation is called and it returns
		true, then for every row return by getNextRowFromRowSource, a
		rowLocation callback must also be issued with the row location of the
		row.  Implementor of both the source and the drain of the row source
		must be aware of this protocol.

		<BR>
		The RowLocation object is own by the caller of rowLocation, in other
		words, the drainer of the RowSource.  This is so that we don't need to
		new a row location for every row.  If the Row Source wants to keep the
		row location, it needs to clone it (RowLocation is a ClonableObject).
		@exception StandardException on error
	 */
	public void rowLocation(RowLocation rl) throws StandardException
	{ }

    public void offendingRowLocation(
//IC see: https://issues.apache.org/jira/browse/DERBY-532
            RowLocation rl, long containdId) throws StandardException {
        if (SanityManager.DEBUG) {
            SanityManager.NOTREACHED();
        }
    }

	/**
	 * @see NoPutResultSet#positionScanAtRowLocation
	 *
	 * This method is result sets used for scroll insensitive updatable 
	 * result sets for other result set it is a no-op.
	 */
	public void positionScanAtRowLocation(RowLocation rl) 
//IC see: https://issues.apache.org/jira/browse/DERBY-690
		throws StandardException 
	{
		// Only used for Scrollable insensitive result sets otherwise no-op
	}

	// Class implementation

	/**
	 * Is this ResultSet or it's source result set for update
	 * This method will be overriden in the inherited Classes
	 * if it is true
	 * @return Whether or not the result set is for update.
	 */
	public boolean isForUpdate()
	{
		return false;
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

    public void addWarning(SQLWarning w) {
        getActivation().addWarning(w);
    }

	public SQLWarning getWarnings() {
//IC see: https://issues.apache.org/jira/browse/DERBY-3049
		return null;
	}

	/**
	 * @see NoPutResultSet#updateRow
	 *
	 * This method is result sets used for scroll insensitive updatable 
	 * result sets for other result set it is a no-op.
	 */
	public void updateRow(ExecRow row, RowChanger rowChanger)
//IC see: https://issues.apache.org/jira/browse/DERBY-4198
			throws StandardException {
		// Only ResultSets of type Scroll Insensitive implement
		// detectability, so for other result sets this method
		// is a no-op
	}
 
	/**
	 * @see NoPutResultSet#markRowAsDeleted
	 *
	 * This method is result sets used for scroll insensitive updatable 
	 * result sets for other result set it is a no-op.
	 */
	public void markRowAsDeleted() throws StandardException {
		// Only ResultSets of type Scroll Insensitive implement
		// detectability, so for other result sets this method
		// is a no-op
	}

	/**
	 * Return the <code>Activation</code> for this result set.
	 *
	 * @return activation
	 */
	public final Activation getActivation() {
//IC see: https://issues.apache.org/jira/browse/DERBY-1112
		return holder.activation;
	}
    
    public Element toXML( Element parentNode, String tag ) throws Exception
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-6266
        return BasicNoPutResultSetImpl.childrenToXML( BasicNoPutResultSetImpl.toXML( parentNode, tag, this ), this );
    }

}
