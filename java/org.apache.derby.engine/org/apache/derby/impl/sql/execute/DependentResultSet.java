/*

   Derby - Class org.apache.derby.impl.sql.execute.DependentResultSet

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
import java.util.Vector;

import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.shared.common.reference.SQLState;
import org.apache.derby.shared.common.i18n.MessageService;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.services.loader.GeneratedMethod;
import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.execute.CursorResultSet;
import org.apache.derby.iapi.sql.execute.ExecIndexRow;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.TemporaryRowHolder;
import org.apache.derby.iapi.store.access.ConglomerateController;
import org.apache.derby.iapi.store.access.DynamicCompiledOpenConglomInfo;
import org.apache.derby.iapi.store.access.Qualifier;
import org.apache.derby.iapi.store.access.ScanController;
import org.apache.derby.iapi.store.access.StaticCompiledOpenConglomInfo;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.iapi.types.RowLocation;



/**
 * DependentResultSet should be used by only ON DELETE CASCADE/ON DELETE SET NULL ref
 * actions implementation to gather the rows from the dependent tables.  
 * Idea is to scan the foreign key index for the rows in 
 * the source table materialized temporary result set.
 * Scanning of foreign key index gives us the
 * rows that needs to be deleted on dependent tables. Using the row location 
 * we got from the index , base row is fetched.
*/
@SuppressWarnings("UseOfObsoleteCollectionType")
class DependentResultSet extends ScanResultSet implements CursorResultSet
{


	ConglomerateController heapCC;
	RowLocation	baseRowLocation;  // base row location we got from the index
	ExecRow indexRow; //templeate to fetch the index row
	IndexRow indexQualifierRow; // template for the index qualifier row
	ScanController indexSC;  // Index Scan Controller
	StaticCompiledOpenConglomInfo  indexScoci;
	DynamicCompiledOpenConglomInfo indexDcoci;
	int numFkColumns;
	boolean isOpen; // source result set is opened or not
	TemporaryRowHolderResultSet source; // Current parent table result set
	TransactionController tc;
	String parentResultSetId;
	int[] fkColArray;
	RowLocation rowLocation;
    TemporaryRowHolder[] sourceRowHolders;
	TemporaryRowHolderResultSet[] sourceResultSets;
	int[] sourceOpened;
	int    sArrayIndex;
    Vector<TemporaryRowHolder> sVector;

//IC see: https://issues.apache.org/jira/browse/DERBY-673

    protected ScanController scanController;
	protected boolean		scanControllerOpened;
	protected boolean		isKeyed;
	protected boolean		firstScan = true;
	protected ExecIndexRow	startPosition;
	protected ExecIndexRow	stopPosition;

    // set in constructor and not altered during
    // life of object.
	protected long conglomId;
    protected DynamicCompiledOpenConglomInfo heapDcoci;
    protected StaticCompiledOpenConglomInfo heapScoci;
	protected int startSearchOperator;
	protected int stopSearchOperator;
	protected Qualifier[][] qualifiers;
	public String userSuppliedOptimizerOverrides;
	protected boolean runTimeStatisticsOn;
	public int rowsPerRead;
	public boolean forUpdate;

	// Run time statistics
	private Properties scanProperties;
	public String startPositionString;
	public String stopPositionString;
	public boolean isConstraint;
	public boolean coarserLock;
	public boolean oneRowScan;
	protected long	rowsThisScan;

	//
    // class interface
    //
//IC see: https://issues.apache.org/jira/browse/DERBY-1700
	DependentResultSet(
		long conglomId,
		StaticCompiledOpenConglomInfo scoci, 
		Activation activation, 
		int resultRowTemplate,
		int resultSetNumber,
		GeneratedMethod startKeyGetter, int startSearchOperator,
		GeneratedMethod stopKeyGetter, int stopSearchOperator,
		boolean sameStartStopPosition,
		Qualifier[][] qualifiers,
		String tableName,
//IC see: https://issues.apache.org/jira/browse/DERBY-573
		String userSuppliedOptimizerOverrides,
		String indexName,
		boolean isConstraint,
		boolean forUpdate,
		int colRefItem,
		int lockMode,
		boolean tableLocked,
		int isolationLevel,		// ignored
		int rowsPerRead,
		boolean oneRowScan,
		double optimizerEstimatedRowCount,
		double optimizerEstimatedCost,
		String parentResultSetId, 
		long fkIndexConglomId,
		int fkColArrayItem,
		int rltItem
		)	throws StandardException
	{
//IC see: https://issues.apache.org/jira/browse/DERBY-6003
		super(activation, resultSetNumber, resultRowTemplate,
			  lockMode, tableLocked,
			  //Because the scan for the tables in this result set are done
			  //internally for delete cascades, isolation should be set to
			  //REPEATABLE READ irrespective what the user level isolation
			  //level is.
			  TransactionController.ISOLATION_REPEATABLE_READ,
              colRefItem,
			  optimizerEstimatedRowCount, optimizerEstimatedCost);

		this.conglomId = conglomId;

		/* Static info created at compile time and can be shared across
		 * instances of the plan.
		 * Dynamic info created on 1st instantiation of this ResultSet as
		 * it cannot be shared.
		 */
        this.heapScoci = scoci;
        heapDcoci = activation.getTransactionController().getDynamicCompiledConglomInfo(conglomId);

		if (SanityManager.DEBUG) {
            // This ResultSet doesn't use start or stop keys, so expect them
            // to be null.
//IC see: https://issues.apache.org/jira/browse/DERBY-6003
            SanityManager.ASSERT(startKeyGetter == null, "start key not null");
            SanityManager.ASSERT(stopKeyGetter == null, "stop key not null");
		}

		this.startSearchOperator = startSearchOperator;
		this.stopSearchOperator = stopSearchOperator;
		this.qualifiers = qualifiers;
		this.tableName = tableName;
//IC see: https://issues.apache.org/jira/browse/DERBY-573
		this.userSuppliedOptimizerOverrides = userSuppliedOptimizerOverrides;
		this.indexName = "On Foreign Key";  // RESOLVE , get actual indexName;
		this.isConstraint = isConstraint;
		this.forUpdate = forUpdate;
		this.rowsPerRead = rowsPerRead;
		this.oneRowScan = oneRowScan;
		
        runTimeStatisticsOn = (activation.getLanguageConnectionContext().getRunTimeStatisticsMode());
//IC see: https://issues.apache.org/jira/browse/DERBY-673

		tc = activation.getTransactionController();
		//values required to scan the forein key index.
		indexDcoci = tc.getDynamicCompiledConglomInfo(fkIndexConglomId);
		indexScoci = tc.getStaticCompiledConglomInfo(fkIndexConglomId);
		
		this.parentResultSetId = parentResultSetId;
		this.fkColArray = (int[])(activation.getPreparedStatement().
						getSavedObject(fkColArrayItem));

		this.rowLocation = (RowLocation)(activation.getPreparedStatement().
										 getSavedObject(rltItem));
		numFkColumns = fkColArray.length;
		indexQualifierRow = new IndexRow(numFkColumns);
		recordConstructorTime();
	}


	/**
	 * Get a scan controller positioned using searchRow as
	 * the start/stop position.  The assumption is that searchRow
	 * is of the same format as the index being opened. 
	 * @param searchRow			the row to match
	 * @exception StandardException on error
	 */


	private ScanController openIndexScanController(ExecRow searchRow)	throws StandardException
	{
		setupQualifierRow(searchRow);
		indexSC = tc.openCompiledScan(
					  false,                       				// hold 
					  TransactionController.OPENMODE_FORUPDATE, // update only
                      lockMode,									// lock Mode
					  isolationLevel,                           //isolation level
                      (FormatableBitSet)null, 							// retrieve all fields
                      indexQualifierRow.getRowArray(),    		// startKeyValue
                      ScanController.GE,            			// startSearchOp
                      null,                         			// qualifier
                      indexQualifierRow.getRowArray(),    		// stopKeyValue
                      ScanController.GT,             			// stopSearchOp 
					  indexScoci,
					  indexDcoci
                      );

		return indexSC;

	}

	
	//reopen the scan with a differnt search row
	private void reopenIndexScanController(ExecRow searchRow)	throws   StandardException
	{

		setupQualifierRow(searchRow);
		indexSC.reopenScan(
						indexQualifierRow.getRowArray(),    	// startKeyValue
						ScanController.GE,            		// startSearchOp
						null,                         		// qualifier
						indexQualifierRow.getRowArray(), 		// stopKeyValue
						ScanController.GT             		// stopSearchOp 
						);
	}

	
	/*
	** Do reference copy for the qualifier row.  No cloning.
	** So we cannot get another row until we are done with
	** this one.
	*/
	private void setupQualifierRow(ExecRow searchRow)
	{
        @SuppressWarnings("MismatchedReadAndWriteOfArray")
		Object[] indexColArray = indexQualifierRow.getRowArray();
		Object[] baseColArray = searchRow.getRowArray();

		for (int i = 0; i < numFkColumns; i++)
		{
			indexColArray[i] = baseColArray[fkColArray[i] - 1];
		}
	}


	private void  openIndexScan(ExecRow searchRow) throws StandardException
	{

		if (indexSC == null)
		{
			indexSC =  openIndexScanController(searchRow);
			//create a template for the index row
			indexRow = indexQualifierRow.getClone();
			indexRow.setColumn(numFkColumns + 1, rowLocation.cloneValue(false));
//IC see: https://issues.apache.org/jira/browse/DERBY-4520

		}else
		{
			reopenIndexScanController(searchRow);
		}
	}


	/**
	  Fetch a row from the index scan.

	  @return The row or null. Note that the next call to fetch will
	  replace the columns in the returned row.
	  @exception StandardException Ooops
	  */
	private ExecRow fetchIndexRow()
		 throws StandardException
	{ 
		if (!indexSC.fetchNext(indexRow.getRowArray()))
		{
			return null;
		}
		return indexRow;
	}

	

	/**
	  Fetch the base row corresponding to the current index row

	  @return The base row row or null.
	  @exception StandardException Ooops
	  */
	private ExecRow fetchBaseRow()
		 throws StandardException
	{ 

		if (currentRow == null)
		{
			currentRow =
				getCompactRow(candidate, accessedCols, isKeyed);
		} 

		baseRowLocation = (RowLocation) indexRow.getColumn(indexRow.getRowArray().length);
		boolean base_row_exists = 
            heapCC.fetch(
                baseRowLocation, candidate.getRowArray(),accessedCols);

        if (SanityManager.DEBUG)
        {
            SanityManager.ASSERT(base_row_exists, "base row disappeared.");
        }

		return currentRow;
	}
	

	ExecRow searchRow = null; //the current row we are searching for

	//this function will return an index row on dependent table 
	public ExecRow	getNextRowCore() throws StandardException 
	{
		
//IC see: https://issues.apache.org/jira/browse/DERBY-6216
		if( isXplainOnlyMode() )
			return null;

		beginTime = getCurrentTimeMillis();
		if (searchRow == null)
		{
			//we are searching for a row first time
			if((searchRow = getNextParentRow())!=null)
			   openIndexScan(searchRow);
		}	
	
		ExecRow currentIndexRow = null;
	    while(searchRow != null)
		{
			//get if the current search row has  more 
			//than one row in the dependent tables
			currentIndexRow = fetchIndexRow();
	
			if(currentIndexRow !=null)
				break;
			if((searchRow = getNextParentRow())!=null)
			   openIndexScan(searchRow);
		}

		nextTime += getElapsedMillis(beginTime);
		if(currentIndexRow!= null)
		{
			rowsSeen++;
			return fetchBaseRow();
		}else
		{
			return currentIndexRow;
		}
		
		
	}


	//this function will return the rows from the parent result sets 
	private ExecRow	getNextParentRow() throws StandardException 
	{

		ExecRow cRow;
		TemporaryRowHolder rowHolder;

		if(sourceOpened[sArrayIndex] == 0)
		{
			rowHolder = sourceRowHolders[sArrayIndex];
			source = (TemporaryRowHolderResultSet)rowHolder.getResultSet();
			source.open(); //open the cursor result set
			sourceOpened[sArrayIndex] = -1;
			sourceResultSets[sArrayIndex] = source;
		}

		if(sourceOpened[sArrayIndex] == 1)
		{
			source = sourceResultSets[sArrayIndex];
			source.reStartScan(sourceRowHolders[sArrayIndex].getTemporaryConglomId(),
							  sourceRowHolders[sArrayIndex].getPositionIndexConglomId());
			sourceOpened[sArrayIndex] = -1;
			
		}

		if(sVector.size() > sourceRowHolders.length)
		{
			addNewSources();
		}

		cRow = source.getNextRow();
		while(cRow == null &&  (sArrayIndex+1) <  sourceRowHolders.length)
		{

			//opening the next source;
			sArrayIndex++;
			if(sourceOpened[sArrayIndex] == 0)
			{
				rowHolder = sourceRowHolders[sArrayIndex];
				source = (TemporaryRowHolderResultSet)rowHolder.getResultSet();
				source.open(); //open the cursor result set
				sourceOpened[sArrayIndex] = -1;
				sourceResultSets[sArrayIndex] = source;
			}

			if(sourceOpened[sArrayIndex] == 1)
			{
				source = sourceResultSets[sArrayIndex];
				source.reStartScan(sourceRowHolders[sArrayIndex].getTemporaryConglomId(),
								  sourceRowHolders[sArrayIndex].getPositionIndexConglomId());
				sourceOpened[sArrayIndex] = -1;
			}
		
			cRow = source.getNextRow();
		}

		if(cRow == null)
		{
			//which means no source has any more  currently rows.
			sArrayIndex = 0;
			//mark all the sources to  restartScan.
			for(int i =0 ; i < sourceOpened.length ; i++)
				sourceOpened[i] = 1;
		}
		
		return cRow;
	}



	/*
	** Open the heap Conglomerate controller
	**
	** @param transaction controller will open one if null
	*/
	public ConglomerateController openHeapConglomerateController()
		throws StandardException
	{
		return tc.openCompiledConglomerate(
                    false,
				    TransactionController.OPENMODE_FORUPDATE,
					lockMode,
					isolationLevel,
					heapScoci,
					heapDcoci);
	}




	/**
	  Close the all the opens we did in this result set.
	  */
    @Override
	public void close()
        throws StandardException
	{
        // save the information for the runtime statistics
		// This is where we get the scan properties for the reference index scans
		if (runTimeStatisticsOn)
		{
			startPositionString = printStartPosition();
			stopPositionString = printStopPosition();
			scanProperties = getScanProperties();
		}

		if (indexSC != null) 
		{
			indexSC.close();
			indexSC = null;
		}

		if ( heapCC != null )
		{
			heapCC.close();
			heapCC = null;
		}
		if(isOpen)
		{
			source.close();  
		}
		
		closeTime += getElapsedMillis(beginTime);
	}

    @Override
	public void	finish() throws StandardException
	{
		if (source != null)
			source.finish();
		finishAndRTS();
	}

	public void openCore() throws StandardException
	{
		initIsolationLevel();
		sVector = activation.getParentResultSet(parentResultSetId);
		int size = sVector.size();
		sourceRowHolders = new TemporaryRowHolder[size];
		sourceOpened = new int[size];
		sourceResultSets = new TemporaryRowHolderResultSet[size];
		for(int i = 0 ; i < size ; i++)
		{
//IC see: https://issues.apache.org/jira/browse/DERBY-673
            sourceRowHolders[i] = sVector.elementAt(i);
			sourceOpened[i] = 0;
		}

		//open the table scan
		heapCC = openHeapConglomerateController();
		numOpens++;
		openTime += getElapsedMillis(beginTime);
	}


	private void addNewSources()
	{
		int size = sVector.size();
		TemporaryRowHolder[] tsourceRowHolders = new TemporaryRowHolder[size];
		int[] tsourceOpened = new int[size];
		TemporaryRowHolderResultSet[] tsourceResultSets = new TemporaryRowHolderResultSet[size];
		
		//copy the source we have now
		System.arraycopy(sourceRowHolders, 0, tsourceRowHolders, 0 , sourceRowHolders.length);
		System.arraycopy(sourceOpened, 0, tsourceOpened , 0 ,sourceOpened.length);
		System.arraycopy(sourceResultSets , 0, tsourceResultSets ,0 ,sourceResultSets.length);

		//copy the new sources
		for(int i = sourceRowHolders.length; i < size ; i++)
		{
//IC see: https://issues.apache.org/jira/browse/DERBY-673
            tsourceRowHolders[i] = sVector.elementAt(i);
			tsourceOpened[i] = 0;
		}

		sourceRowHolders = tsourceRowHolders;
		sourceOpened = tsourceOpened ;
		sourceResultSets = tsourceResultSets;
	}



	/**
	 * Can we get instantaneous locks when getting share row
	 * locks at READ COMMITTED.
	 */
	boolean canGetInstantaneousLocks()
	{
		return false;
	}


	public long getTimeSpent(int type)
	{
		return constructorTime + openTime + nextTime + closeTime;
	}


	//Cursor result set information.
	public RowLocation getRowLocation() throws StandardException
	{
		return baseRowLocation;
	}

	public ExecRow getCurrentRow() throws StandardException 
	{
		return currentRow;
	}


	public Properties getScanProperties()
	{
		if (scanProperties == null)
		{
			scanProperties = new Properties();
		}
		try
		{
			if (indexSC != null)
			{
				indexSC.getScanInfo().getAllScanInfo(scanProperties);
				/* Did we get a coarser lock due to
				 * a covering lock, lock escalation
				 * or configuration?
				 */
				coarserLock = indexSC.isTableLocked() && 
					(lockMode == TransactionController.MODE_RECORD);
			}
		}
		catch(StandardException se)
		{
				// ignore
		}

		return scanProperties;
	}

	public String printStartPosition()
	{
		return printPosition(ScanController.GE, indexQualifierRow);
	}

	public String printStopPosition()
	{
		return printPosition(ScanController.GT, indexQualifierRow);
	}


	/**
	 * Return a start or stop positioner as a String.
	 *
	 * If we already generated the information, then use
	 * that.  Otherwise, invoke the activation to get it.
	 */
	private String printPosition(int searchOperator, ExecIndexRow positioner)
	{
		String output = "";

//IC see: https://issues.apache.org/jira/browse/DERBY-673
        String searchOp;
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

		if(positioner !=null)
		{
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
//IC see: https://issues.apache.org/jira/browse/DERBY-4087
					colSeen = true;
				}

				if (colSeen && position == positioner.nColumns() - 1) {
					output = output +  "\n";
				}
			}
		}
	
		return output;
	}


	/**
	 * Return an array of Qualifiers as a String
	 */
	public String printQualifiers()
	{
		//There are no qualifiers in thie result set for index scans.
		String idt = "";
		return idt + MessageService.getTextMessage(SQLState.LANG_NONE);
	}

}











