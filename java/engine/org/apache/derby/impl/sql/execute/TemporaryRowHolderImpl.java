/*

   Derby - Class org.apache.derby.impl.sql.execute.TemporaryRowHolderImpl

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

import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.CursorResultSet;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.ExecutionFactory;
import org.apache.derby.iapi.sql.execute.TemporaryRowHolder;
import org.apache.derby.iapi.sql.ResultDescription;
import org.apache.derby.iapi.store.access.ConglomerateController;
import org.apache.derby.iapi.store.access.ScanController;
import org.apache.derby.iapi.store.access.TransactionController;

import org.apache.derby.iapi.types.CloneableObject;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.SQLRef;
import org.apache.derby.iapi.types.SQLLongint;


import org.apache.derby.iapi.services.io.FormatableBitSet;
import java.util.Properties;

/**
 * This is a class that is used to temporarily
 * (non-persistently) hold rows that are used in
 * language execution.  It will store them in an
 * array, or a temporary conglomerate, depending
 * on the number of rows.  
 * <p>
 * It is used for deferred DML processing.
 *
 * @author jamie
 */
public class TemporaryRowHolderImpl implements TemporaryRowHolder
{
	public static final int DEFAULT_OVERFLOWTHRESHOLD = 5;

	protected static final int STATE_UNINIT = 0;
	protected static final int STATE_INSERT = 1;
	protected static final int STATE_DRAIN = 2;


	protected ExecRow[] 	rowArray;
	protected int 		lastArraySlot;
	private int			numRowsIn;
	protected int		state = STATE_UNINIT;

	protected	long				CID;
	private boolean					conglomCreated;
	private ConglomerateController	cc;
	private Properties				properties;
	private ScanController			scan;
	private TransactionController	tc;
	private	ResultDescription		resultDescription;

	private boolean     isUniqueStream;

	/* beetle 3865 updateable cursor use index. A virtual memory heap is a heap that has in-memory
	 * part to get better performance, less overhead. No position index needed. We read from and write
	 * to the in-memory part as much as possible. And we can insert after we start retrieving results.
	 * Could be used for other things too.
	 */
	private boolean     isVirtualMemHeap;
	private boolean     uniqueIndexCreated;
	private boolean     positionIndexCreated;
	private long        uniqueIndexConglomId;
	private long        positionIndexConglomId;
	private ConglomerateController uniqueIndex_cc;
	private ConglomerateController positionIndex_cc;
	private DataValueDescriptor[]  uniqueIndexRow = null;
	private DataValueDescriptor[]  positionIndexRow = null;
	private RowLocation            destRowLocation; //row location in the temporary conglomerate
	private SQLLongint             position_sqllong;
	

	/**
	 * Uses the default overflow to
 	 * a conglomerate threshold (5).
	 *
	 * @param tc the xact controller
	 * @param properties the properties of the original table.  Used
	 *		to help the store use optimal page size, etc.
	 * @param the result description.  Relevant for the getResultDescription
	 * 		call on the result set returned by getResultSet.  May be null
	 */
	public TemporaryRowHolderImpl
	(
		TransactionController	tc, 
		Properties 				properties, 
		ResultDescription		resultDescription
	) 
	{
		this(tc, properties, resultDescription, DEFAULT_OVERFLOWTHRESHOLD, false, false);
	}
	
	/**
	 * Uses the default overflow to
 	 * a conglomerate threshold (5).
	 *
	 * @param tc the xact controller
	 * @param properties the properties of the original table.  Used
	 *		to help the store use optimal page size, etc.
	 * @param the result description.  Relevant for the getResultDescription
	 * 		call on the result set returned by getResultSet.  May be null
	 * @param isUniqueStream - true , if it has to be temporary row holder unique stream
	 */
	public TemporaryRowHolderImpl
	(
		TransactionController	tc, 
		Properties 				properties, 
		ResultDescription		resultDescription,
		boolean                 isUniqueStream
	) 
	{
		this(tc, properties, resultDescription, 1, isUniqueStream, false);
	}


	/**
	 * Create a temporary row holder with the defined overflow to conglom
	 *
	 * @param tc the xact controller
	 * @param properties the properties of the original table.  Used
	 *		to help the store use optimal page size, etc.
	 * @param the result description.  Relevant for the getResultDescription
	 * 		call on the result set returned by getResultSet.  May be null
	 * @param spillToConglomSize on an attempt to insert
	 * 		this number of rows, the rows will be put
 	 *		into a temporary conglomerate.
	 */
	public TemporaryRowHolderImpl
	(
		TransactionController 	tc, 
		Properties				properties,
		ResultDescription		resultDescription,
		int 					overflowToConglomThreshold,
		boolean                 isUniqueStream,
		boolean					isVirtualMemHeap
	)
	{
		if (SanityManager.DEBUG)
		{
			if (overflowToConglomThreshold <= 0)
			{
				SanityManager.THROWASSERT("It is assumed that "+
					"the overflow threshold is > 0.  "+
					"If you you need to change this you have to recode some of "+
					"this class.");
			}
		}

		this.tc = tc;
		this.properties = properties;
		this.resultDescription = resultDescription;
		this.isUniqueStream = isUniqueStream;
		this.isVirtualMemHeap = isVirtualMemHeap;
		rowArray = new ExecRow[overflowToConglomThreshold];
		lastArraySlot = -1;
	}

	/* Avoid materializing a stream just because it goes through a temp table.  It is OK to
	 * have a stream in the temp table (in memory or spilled to disk).  The assumption is
	 * that one stream does not appear in two rows.  For "update", one stream can be in two
	 * rows and the materialization is done in UpdateResultSet.  Note to future users of this
	 * class who may insert a stream into this temp holder: (1) As mentioned above, one
	 * un-materialized stream can't appear in two rows; you need to objectify it first otherwise.
	 * (2) If you need to retrieve a un-materialized stream more than once from the temp holder,
	 * you need to either materialize the stream the first time, or, if there's a memory constraint,
	 * in the first time create a RememberBytesInputStream with the byte holder being
	 * BackingStoreByteHolder, finish it, and reset it after usage.
	 * beetle 4896.
	 */
	private ExecRow cloneRow(ExecRow inputRow)
	{
		DataValueDescriptor[] cols = inputRow.getRowArray();
		int ncols = cols.length;
		ExecRow cloned = ((ValueRow) inputRow).cloneMe();
		for (int i = 0; i < ncols; i++)
		{
			if (cols[i] != null)
			{
				/* Rows are 1-based, cols[] is 0-based */
				cloned.setColumn(i + 1, (DataValueDescriptor)((CloneableObject) cols[i]).cloneObject());
			}
		}
		if (inputRow instanceof IndexValueRow)
			return new IndexValueRow(cloned);
		else
			return cloned;
	}

	/**
	 * Insert a row
	 *
	 * @param ef	ExecutionFactory to use for cloning.
	 * @param row the row to insert 
	 *
	 * @exception StandardException on error
 	 */
	public void insert(ExecRow inputRow)
		throws StandardException
	{

		if (SanityManager.DEBUG)
		{
			if(!isUniqueStream && !isVirtualMemHeap)
				SanityManager.ASSERT(state != STATE_DRAIN, "you cannot insert rows after starting to drain");
		}
		if (! isVirtualMemHeap)
			state = STATE_INSERT;

		if(uniqueIndexCreated)
		{
			if(isRowAlreadyExist(inputRow))
				return;
		}

		numRowsIn++;

		if (lastArraySlot + 1 < rowArray.length)
		{
			rowArray[++lastArraySlot] = cloneRow(inputRow);
			
			//In case of unique stream we push every thing into the
			// conglomerates for time being, we keep one row in the array for
			// the template.
			if(!isUniqueStream)
				return;  
		}
			
		if (!conglomCreated)
		{
			/*
			** Create the conglomerate with the template row.
			*/
			CID = tc.createConglomerate("heap",
										inputRow.getRowArray(),
										null, //column sort order - not required for heap
										properties,
										TransactionController.IS_TEMPORARY | 
										TransactionController.IS_KEPT);
			conglomCreated = true;

			cc = tc.openConglomerate(CID, 
                                false,
                                TransactionController.OPENMODE_FORUPDATE,
                                TransactionController.MODE_TABLE,
                                TransactionController.ISOLATION_SERIALIZABLE);
			if(isUniqueStream)
			   destRowLocation = cc.newRowLocationTemplate();

		}

		int status = 0;
		if(isUniqueStream)
		{
			cc.insertAndFetchLocation(inputRow.getRowArray(), destRowLocation);
			insertToPositionIndex(numRowsIn -1, destRowLocation);
			//create the unique index based on input row ROW Location
			if(!uniqueIndexCreated)
				isRowAlreadyExist(inputRow);

		}else
		{
			status = cc.insert(inputRow.getRowArray());
			if (isVirtualMemHeap)
				state = STATE_INSERT;
		}

		if (SanityManager.DEBUG)
		{
			if (status != 0)
			{
				SanityManager.THROWASSERT("got funky status ("+status+") back from "+
						"ConglomerateConstroller.insert()");
			}
		}
	}


	/**
	 * Maintain an unique index based on the input row's row location in the
	 * base table, this index make sures that we don't insert duplicate rows 
	 * into the temporary heap.
	 * @param inputRow  the row we are inserting to temporary row holder 
	 * @exception StandardException on error
 	 */


	private boolean isRowAlreadyExist(ExecRow inputRow) throws  StandardException
	{
		DataValueDescriptor		rlColumn;
		RowLocation	baseRowLocation;
		rlColumn = inputRow.getColumn(inputRow.nColumns());

		if(CID!=0 && rlColumn instanceof SQLRef)
		{
			baseRowLocation = 
				(RowLocation) (rlColumn).getObject();
		
			if(!uniqueIndexCreated)
			{
				int numKeys = 2;
				uniqueIndexRow = new DataValueDescriptor[numKeys];
				uniqueIndexRow[0] = baseRowLocation;
				uniqueIndexRow[1] = baseRowLocation;
				Properties props = makeIndexProperties(uniqueIndexRow, CID);
				uniqueIndexConglomId =
					tc.createConglomerate("BTREE",uniqueIndexRow , null,  props, 
										  TransactionController.IS_TEMPORARY | 
										  TransactionController.IS_KEPT);
				uniqueIndex_cc = tc.openConglomerate(
								uniqueIndexConglomId, 
								false,
								TransactionController.OPENMODE_FORUPDATE,
								TransactionController.MODE_TABLE,
								TransactionController.ISOLATION_SERIALIZABLE);
				uniqueIndexCreated = true;
			}

			uniqueIndexRow[0] = baseRowLocation;
			uniqueIndexRow[1] = baseRowLocation;
			// Insert the row into the secondary index.
			int status;
			if ((status = uniqueIndex_cc.insert(uniqueIndexRow))!= 0)
			{
				if(status == ConglomerateController.ROWISDUPLICATE)
				{
					return true ; // okay; we don't insert duplicates
				}
				else
				{
					if (SanityManager.DEBUG)
					{
						if (status != 0)
						{
							SanityManager.THROWASSERT("got funky status ("+status+") back from "+
													  "Unique Index insert()");
						}
					}
				}
			}
		}

		return false;
	}


	/**
	 * Maintain an index that will allow us to read  from the 
	 * temporary heap in the order we inserted.
	 * @param position - the number of the row we are inserting into heap
	 * @param rl the row to Location in the temporary heap 
	 * @exception StandardException on error
 	 */

	private void insertToPositionIndex(int position, RowLocation rl ) throws  StandardException
	{
		if(!positionIndexCreated)
		{
			int numKeys = 2;
			position_sqllong = new SQLLongint();
			positionIndexRow = new DataValueDescriptor[numKeys];
			positionIndexRow[0] = position_sqllong;
			positionIndexRow[1] = rl;				
			Properties props = makeIndexProperties(positionIndexRow, CID);
			positionIndexConglomId =
				tc.createConglomerate("BTREE", positionIndexRow, null,  props, 
									  TransactionController.IS_TEMPORARY |
									  TransactionController.IS_KEPT);
			positionIndex_cc = tc.openConglomerate(
													positionIndexConglomId, 
													false,
													TransactionController.OPENMODE_FORUPDATE,
													TransactionController.MODE_TABLE,
													TransactionController.ISOLATION_SERIALIZABLE);
			positionIndexCreated = true;
		}
		
		position_sqllong.setValue(position);
		positionIndexRow[0] = position_sqllong;
		positionIndexRow[1] = rl;
		//insert the row location to position index
		positionIndex_cc.insert(positionIndexRow);
	}

	/**
	 * Get a result set for scanning what has been inserted
 	 * so far.
	 *
	 * @return a result set to use
	 */
	public CursorResultSet getResultSet()
	{
		state = STATE_DRAIN;
		if(isUniqueStream)
		{
			return new TemporaryRowHolderResultSet(tc, rowArray,
												   resultDescription, isVirtualMemHeap,
												   true, positionIndexConglomId, this);
		}
		else
		{
			return new TemporaryRowHolderResultSet(tc, rowArray, resultDescription, isVirtualMemHeap, this);

		}
	}

	/**
	 * Purge the row holder of all its rows.
	 * Resets the row holder so that it can
	 * accept new inserts.  A cheap way to
	 * recycle a row holder.
	 *
	 * @exception StandardException on error
	 */
	public void truncate() throws StandardException
	{
		close();

		for (int i = 0; i < rowArray.length; i++)
		{
			rowArray[i] = null;
		}
		lastArraySlot = -1;
		numRowsIn = 0;
		state = STATE_UNINIT;

		/*
		** We are not expecting this to be called
		** when we have a temporary conglomerate
		** but just to be on the safe side, drop
		** it.  We'd like do something cheaper,
		** but there is no truncate on congloms.
		*/
		if (conglomCreated)
		{
			tc.dropConglomerate(CID);
			conglomCreated = false;
		}
	}

	public long getTemporaryConglomId()
	{
		return CID;
	}

	public long getPositionIndexConglomId()
	{
		return positionIndexConglomId;
	}



	private Properties makeIndexProperties(DataValueDescriptor[]
											   indexRowArray, long conglomId ) throws StandardException {
		int nCols = indexRowArray.length;
		Properties props = new Properties();
		props.put("allowDuplicates", "false");
		// all columns form the key, (currently) required
		props.put("nKeyFields", String.valueOf(nCols));
		props.put("nUniqueColumns", String.valueOf(nCols-1));
		props.put("rowLocationColumn", String.valueOf(nCols-1));
		props.put("baseConglomerateId", String.valueOf(conglomId));
		return props;
	}

	public void setRowHolderTypeToUniqueStream()
	{
		isUniqueStream = true;
	}

	/**
	 * Clean up
	 *
	 * @exception StandardException on error
	 */
	public void close() throws StandardException
	{
		if (scan != null)
		{
			scan.close();
			scan = null;
		}

		if (cc != null)
		{
			cc.close();
			cc = null;
		}

		if (uniqueIndex_cc != null)
		{
			uniqueIndex_cc.close();
			uniqueIndex_cc = null;
		}

		if (positionIndex_cc != null)
		{
			positionIndex_cc.close();
			positionIndex_cc = null;
		}

		if (uniqueIndexCreated)
		{
			tc.dropConglomerate(uniqueIndexConglomId);
			uniqueIndexCreated = false;
		}

		if (positionIndexCreated)
		{
			tc.dropConglomerate(positionIndexConglomId);
			uniqueIndexCreated = false;
		}

		if (conglomCreated)
		{
			tc.dropConglomerate(CID);
			conglomCreated = false;
		}

		state = STATE_UNINIT;
		lastArraySlot = -1;
	}
}

