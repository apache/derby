/*

   Derby - Class org.apache.derby.impl.sql.execute.DMLWriteResultSet

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

import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;
import org.apache.derby.iapi.services.io.StreamStorable;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.ConstantAction;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;

import org.apache.derby.iapi.store.access.DynamicCompiledOpenConglomInfo;
import org.apache.derby.iapi.store.access.StaticCompiledOpenConglomInfo;
import org.apache.derby.iapi.store.access.TransactionController;

import org.apache.derby.catalog.UUID;
import org.apache.derby.iapi.services.io.FormatableBitSet;

/**
 * For INSERT/UPDATE/DELETE impls.  Used to tag them.
 */
abstract class DMLWriteResultSet extends NoRowsResultSetImpl 
{
	protected WriteCursorConstantAction constantAction;
	protected int[] baseRowReadMap;
	protected int[] streamStorableHeapColIds;
	protected ExecRow	deferredSparseRow;
	protected DynamicCompiledOpenConglomInfo heapDCOCI;
	protected DynamicCompiledOpenConglomInfo[] indexDCOCIs;
	private boolean needToObjectifyStream;


	public int rowCount;


	/**
	 * Constructor
	 *
 	 * @param activation		an activation
	 * @param constantAction	a write constant action
	 *
 	 * @exception StandardException on error
	 */
	protected DMLWriteResultSet(Activation activation)
		throws StandardException
	{
		this(activation, activation.getConstantAction());
	}
	protected DMLWriteResultSet(Activation activation, ConstantAction constantAction)
		throws StandardException
	{
		super(activation);

		this.constantAction = (WriteCursorConstantAction) constantAction;
		baseRowReadMap = this.constantAction.getBaseRowReadMap();
		streamStorableHeapColIds = this.constantAction.getStreamStorableHeapColIds();

		TransactionController tc = activation.getTransactionController();

		// Special handling for updatable VTIs
		if (! (constantAction instanceof UpdatableVTIConstantAction))
		{
			heapDCOCI = tc.getDynamicCompiledConglomInfo(this.constantAction.conglomId);
			if (this.constantAction.indexCIDS.length != 0)
			{
				indexDCOCIs = new DynamicCompiledOpenConglomInfo[this.constantAction.indexCIDS.length];
				for (int index = 0; index < this.constantAction.indexCIDS.length; index++)
				{
					indexDCOCIs[index] = tc.getDynamicCompiledConglomInfo(
												this.constantAction.indexCIDS[index]);
				}
			}
		}

		/* We only need to objectify the streams here if they are exposed to the users through the
		 * trigger context.  For "before" trigger, we could just return the stream wrapped in
		 * RememberBytesInputStream to the user and reset it after usage, but this wouldn't work
		 * because the user may get the stream in trigger action and do something with it in parallel
		 * with the store doing insert.  We could also delay the materializing until the stream is
		 * fetched in before trigger but that would complicate the code.  For "after" trigger, we have
		 * to materialize it here because store only keeps a page's amount for each round.  For other
		 * reasons of "deferred" operations we don't need to objectify here.  Simply going through a
		 * temp table (either in memory part or spilled to disk) is fine for the stream, unless a
		 * same stream appears in two rows in the temp table, which could happen for an "update", in
		 * which case we do the objectifying in UpdateResultSet.  Beetle 4896.  Related bug entries:
		 * 2432, 3383.
		 */
		needToObjectifyStream = (this.constantAction.getTriggerInfo(
						activation.getLanguageConnectionContext().getExecutionContext()) != null);
	}

	public final int	modifiedRowCount() { return rowCount; }


	/**
	 * Get next row from the source result set.
	 * 
	 * @param source		SourceResultSet
	 * Also look at Track#2432/change 12433
	 */
	protected ExecRow getNextRowCore(NoPutResultSet source)
		throws StandardException
	{
		ExecRow row = source.getNextRowCore();
		if (needToObjectifyStream)
		{
			/* 
			   See comments in the constructor. We also need to load the column
			   if it is part of an index on an insert but that is done in
			   insertResultSet#normalInsertCore or IRS#changedRow
			*/
			objectifyStreams(row);
		}
		return row;
	}

	private void objectifyStreams(ExecRow row) throws StandardException 
	{
		// if the column is a streamStorable, we need to materialize the object
		// therefore, the object can be used to multiple rows.
		if ((row != null) && (streamStorableHeapColIds != null))
		{
			for (int ix=0; ix < streamStorableHeapColIds.length; ix++)
			{
				int heapIx = streamStorableHeapColIds[ix];
				int readIx = (baseRowReadMap == null) ?
					heapIx :
					baseRowReadMap[heapIx];

				DataValueDescriptor col = row.getColumn(readIx+1);
				((StreamStorable)col).loadStream();
			}
		}
	}

	/**
	 * For deferred update, get a deferred sparse row based on the
	 * deferred non-sparse row. Share the underlying columns. If there
	 * is no column bit map, make them the same row.
	 *
	 * @exception StandardException		Thrown on error
	 */
	protected ExecRow makeDeferredSparseRow(
							ExecRow deferredBaseRow,
							FormatableBitSet baseRowReadList,
							LanguageConnectionContext lcc)
				throws StandardException
	{
		ExecRow deferredSparseRow;

		if (baseRowReadList == null)
		{
			/* No sparse row */
			deferredSparseRow = deferredBaseRow;
		}
		else
		{
			/*
			** We need to do a fetch doing a partial row
			** read.  We need to shift our 1-based bit
			** set to a zero based bit set like the store
			** expects.
			*/
			deferredSparseRow =
				RowUtil.getEmptyValueRow(
								baseRowReadList.getLength() - 1,
								lcc);
			/*
			** getColumn(), setColumn(), and baseRowReadList are
			** one-based.
			*/
			int fromPosition = 1;
			for (int i = 1; i <= deferredSparseRow.nColumns(); i++)
			{
				if (baseRowReadList.isSet(i))
				{
					deferredSparseRow.setColumn(
						i,
						deferredBaseRow.getColumn(fromPosition++)
						);
				}
			}
		}

		return deferredSparseRow;
	}
	
	/**
	 * get the index name given the conglomerate id of the index.
	 * 
	 * @param indexCID		conglomerate ID of the index.
	 * 
	 * @return index name of given index.
	 */
	String getIndexNameFromCID(long indexCID)
	{
		return this.constantAction.getIndexNameFromCID(indexCID);
	}
}
