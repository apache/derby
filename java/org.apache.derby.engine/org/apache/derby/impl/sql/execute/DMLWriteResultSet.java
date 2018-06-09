/*

   Derby - Class org.apache.derby.impl.sql.execute.DMLWriteResultSet

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

import java.io.InputStream;
import org.apache.derby.catalog.UUID;
import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.services.io.StreamStorable;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.ResultColumnDescriptor;
import org.apache.derby.iapi.sql.ResultDescription;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.execute.ConstantAction;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;
import org.apache.derby.iapi.store.access.DynamicCompiledOpenConglomInfo;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.iapi.transaction.TransactionControl;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.shared.common.sanity.SanityManager;

/**
 * For INSERT/UPDATE/DELETE impls.  Used to tag them.
 */
abstract public class DMLWriteResultSet extends NoRowsResultSetImpl
{
	protected WriteCursorConstantAction constantAction;
	protected int[] baseRowReadMap;
	protected int[] streamStorableHeapColIds;
	protected DynamicCompiledOpenConglomInfo heapDCOCI;
	protected DynamicCompiledOpenConglomInfo[] indexDCOCIs;
	private boolean needToObjectifyStream;


	public long rowCount;

	// divined at run time
    protected   ResultDescription 		resultDescription;

    /**
     * This array contains data value descriptors that can be used (and reused)
     * to hold the normalized column values.
     */
    protected DataValueDescriptor[] cachedDestinations;

	/**
	 * Constructor
	 *
 	 * @param activation		an activation
	 *
 	 * @exception StandardException on error
	 */
	DMLWriteResultSet(Activation activation)
		throws StandardException
	{
		this(activation, activation.getConstantAction());
	}
	DMLWriteResultSet(Activation activation, ConstantAction constantAction)
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
        needToObjectifyStream = (this.constantAction.getTriggerInfo() != null);
	}

    @Override
	public final long	modifiedRowCount() { return rowCount + RowUtil.getRowCountBase(); }

	/**
     * Returns the description of the inserted rows.
     * REVISIT: Do we want this to return NULL instead?
     *
     * @return the description of the inserted rows
	 */
    @Override
	public ResultDescription getResultDescription()
	{
	    return resultDescription;
	}

	/**
	 * Get next row from the source result set.
	 * 
	 * @param source		SourceResultSet
	 * Also look at Track#2432/change 12433
     * @return             The next row in the result set
     * @throws StandardException
     *                     Standard error policy
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
				
				// Derby-4779
				if ( col != null ) {
					InputStream stream = ((StreamStorable)col).returnStream();
					((StreamStorable)col).loadStream();

					// DERBY-3238
					// fix up any duplicate streams, for instance in the case of an update with a trigger,
					// all the columns are read as update columns even if they are not updated, so 
					// the update column will still have a reference to the original stream.
					// If we knew from this context that this was an update and we knew the number
					// of columns in the base table we would be able to calculate exactly the offset to 
					// check, but we don't have that information from this context.
					// If DERBY-1482 is fixed, perhaps this code can be removed.

					if (stream != null)
						for (int i = 1; i <= row.nColumns(); i++)
						{
							DataValueDescriptor c = row.getColumn(i);
							if (c instanceof StreamStorable)
								if (((StreamStorable)c).returnStream() == stream)
									row.setColumn(i, col.cloneValue(false));
						}
					}
				}
			}
	}

	/**
	 * For deferred update, get a deferred sparse row based on the
	 * deferred non-sparse row. Share the underlying columns. If there
	 * is no column bit map, make them the same row.
	 *
     * @param deferredBaseRow  the deferred non-sparse row
     * @param baseRowReadList  the columns to include (1-based bit map)
     * @param lcc              the language connection context
     * @return                 the deferred sparse row
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
     * Decode the update lock mode.
     * <p>
     * The value for update lock mode is in the second most significant byte for
     * TransactionControl.SERIALIZABLE_ISOLATION_LEVEL isolation level. Otherwise
     * (REPEATABLE READ, READ COMMITTED, and READ UNCOMMITTED) the lock mode is
     * located in the least significant byte.
     * <p>
     * This is done to override the optimizer choice to provide maximum 
     * concurrency of record level locking except in SERIALIZABLE where table
     * level locking is required in heap scans for correctness.
     *
     * @param lockMode the compiled encoded lock mode for this query
     * @return the lock mode (record or table) to use to open the result set
     * @see org.apache.derby.impl.sql.compile.FromBaseTable#updateTargetLockMode
     */
    int decodeLockMode(int lockMode) {

        if (SanityManager.DEBUG) {
            // we want to decode lock mode when the result set is opened, not
            // in the constructor
            SanityManager.ASSERT(!isClosed());
        }

        if ((lockMode >>> 16) == 0) {
            return lockMode;
        }

        // Note that isolation level encoding from getCurrentIsolationLevel()
        // returns TransactionControl.*ISOLATION_LEVEL constants, not
        // TransactionController.ISOLATION* constants.

        int isolationLevel = lcc.getCurrentIsolationLevel();

        if (isolationLevel == TransactionControl.SERIALIZABLE_ISOLATION_LEVEL) {
            return lockMode >>> 16;
        }

        return lockMode & 0xff;
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

    /**
     * <p>
     * Normalize a row as part of the INSERT/UPDATE action of a MERGE statement.
     * This applies logic usually found in a NormalizeResultSet, which is missing for
     * the MERGE statement.
     * </p>
     * @param sourceResultSet        the result set for which this action is
     *                               to be performed
     * @param row                    the row to be normalized
     * @return                       the normalized row
     * @throws StandardException     Standard error policy
     */
    protected   ExecRow normalizeRow( NoPutResultSet sourceResultSet, ExecRow row )
        throws StandardException
    {
        //
        // Make sure that the evaluated expressions fit in the base table row.
        //
        int count = resultDescription.getColumnCount();
        if ( cachedDestinations == null )
        {
            cachedDestinations = new DataValueDescriptor[ count ];
            for ( int i = 0; i < count; i++)
            {
                int         position = i + 1;
                ResultColumnDescriptor  colDesc = resultDescription.getColumnDescriptor( position );
                cachedDestinations[ i ] = colDesc.getType().getNull();
            }
        }

        for ( int i = 0; i < count; i++ )
        {
            int         position = i + 1;
            DataTypeDescriptor  dtd = resultDescription.getColumnDescriptor( position ).getType();

            if ( row.getColumn( position ) == null )
            {
                row.setColumn( position, dtd.getNull() );
            }

            row.setColumn
                (
                 position,
                 NormalizeResultSet.normalizeColumn
                 (
                  dtd,
                  row,
                  position,
                  cachedDestinations[ i ],
                  resultDescription
                  )
                 );
        }

        // put the row where expressions in constraints can access it
        activation.setCurrentRow( row, sourceResultSet.resultSetNumber() );

        return row;
    }

    public void rememberConstraint(UUID cid) throws StandardException {
        if (SanityManager.DEBUG) {
            // This method should be overriden by InsertResultSet and
            // UpdateResultSet, other shouldn't need it.
            SanityManager.NOTREACHED();
        }
    }
}
