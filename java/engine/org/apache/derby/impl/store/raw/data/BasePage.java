/*

   Derby - Class org.apache.derby.impl.store.raw.data.BasePage

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

package org.apache.derby.impl.store.raw.data;

import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.services.io.DynamicByteArrayOutputStream;
import org.apache.derby.iapi.services.io.DynamicByteArrayOutputStream;

import org.apache.derby.iapi.services.locks.C_LockFactory;
import org.apache.derby.iapi.services.locks.Lockable;
import org.apache.derby.iapi.services.locks.Latch;
import org.apache.derby.iapi.services.locks.VirtualLockTable;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.services.io.LimitObjectInput;
import org.apache.derby.iapi.services.io.TypedFormat;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.store.raw.AuxObject;
import org.apache.derby.iapi.store.raw.ContainerHandle;
import org.apache.derby.iapi.store.raw.ContainerKey;
import org.apache.derby.iapi.store.raw.FetchDescriptor;
import org.apache.derby.iapi.store.raw.Page;
import org.apache.derby.iapi.store.raw.PageKey;
import org.apache.derby.iapi.store.raw.RecordHandle;
import org.apache.derby.iapi.store.raw.RawStoreFactory;
import org.apache.derby.iapi.store.raw.xact.RawTransaction;
import org.apache.derby.iapi.store.raw.log.LogInstant;

import org.apache.derby.iapi.store.access.Qualifier;
import org.apache.derby.iapi.store.access.conglomerate.LogicalUndo;

import org.apache.derby.iapi.types.DataValueDescriptor;

import java.io.IOException;
import java.io.OutputStream;
import java.io.ObjectInput;

import java.util.Hashtable;
import java.util.Observer;
import java.util.Observable;


/**

  This class implements all the the generic locking behaviour for a Page.
  It leaves method used to log and store the records up to sub-classes.
  It is intended that the object can represent multiple pages from different
  containers during its lifetime.
  <P>
  A page contains a set of records, which can be accessed by "slot",
  which defines the order of the records on the page, or by "id" which
  defines the identity of the records on the page.  Clients access
  records by both slot and id, depending on their needs.
  <P>
  BasePage implements Observer to watch the ContainerHandle which notifies
  its Observers when it is closing.

  <BR>
  MT - mutable

 **/


public abstract class BasePage implements Page, Lockable, Observer, TypedFormat
{

	/**
		auxiliary object

		MT - mutable - content dynamic : single thread required. This reference is
		set while the page is latched and returned to callers of page while the page is latched.
		For correct MT behaviour it is assumed that the caller discards any reference to an
		auxiliary object once the page is unlatched. The reference mya be cleared while
		the page is latched, or while the page is being cleaned from the cache. In the latter
		case the cache manager ensures that only a single thread can access this object.		
	*/
	private AuxObject auxObj;

	/**
		this page's identity
		<BR>
		MT - immutable - content dynamic : single thread required
	*/
	protected PageKey identity;

	/**
		In-memory slot table, array of StoredRecordHeaders.
		<BR>
		MT - Immutable - Content Dynamic : Single thread required.
	*/
	private StoredRecordHeader[]	headers;		// in memory slot table

	private int   recordCount;

	/**
		Page owner during exclusive access.

		MT - mutable : single thread required, provided by Lockable single thread required. 
	*/
	protected BaseContainerHandle	owner;

	/**
		Count of times a latch is held nested during an abort
	*/
	private int nestedLatch;

	/**
		LockManager held latch during exclusive access.
		When this is not null, latch.getQualifier() == owner
	*/
	private Latch	myLatch;
	
	protected boolean		inClean;	// is the page being cleaned

    /**
     * MT - mutable
     *
     * There are 3 latch states for a page:
     *
     * UNLATCHED - (owner == null) 
     * PRELATCH  - (owner != null) && preLatch
     * LATCHED   - (owner != null) && !preLatch
     *
     * A page may be "cleaned" while it is either UNLATCHED, or PRELATCH, but
     * it must wait for it to be not LATCHED.
     *
     * A page may move from UNLATCHED to PRELATCH, while being cleaned.
     * A page must wait for !inClean before it can move from PRELATCH to 
     * LATCHED.
     **/
	protected boolean		preLatch;

	/**
		Instant of last log record that updated this page.

		<BR> MT - mutable : latched
	*/
	private LogInstant lastLog;

	/**
		Version of the page.

		<BR> MT - mutable : single thread required - The page must be latched to access
		this variable or the page muts be in the noidentiy state.
	*/
	private long	pageVersion = 0;	// version of the page

	/**
		Status of the page
	 */
	private byte	pageStatus;

	/**
		Values for pageStatus flag 

		page goes thru the following transition:
		VALID_PAGE <-> deallocated page -> free page <-> VALID_PAGE

		deallocated and free page are both INVALID_PAGE as far as BasePage is concerned.
		When a page is deallocated, it transitioned from VALID to INVALID.
		When a page is allocated, it trnasitioned from INVALID to VALID.

	*/
	public static final byte VALID_PAGE = 1;
	public static final byte INVALID_PAGE = 2;

	/**
		Init page flag.

		INIT_PAGE_REUSE - set if page is being initialized for reuse
		INIT_PAGE_OVERFLOW - set if page will be an overflow page
		INIT_PAGE_REUSE_RECORDID - set if page is being reused and its record
						id can be reset to RecordHandle.FIRST_RECORD_ID, rather
						to 1+ next recordId on the page
	*/
	public static final int INIT_PAGE_REUSE = 0x1;
	public static final int INIT_PAGE_OVERFLOW = 0x2;
	public static final int INIT_PAGE_REUSE_RECORDID = 0x4;

	/**
		Log Record flag.  Why the before image of this record is being logged

		LOG_RECORD_FOR_UPDATE - set if the record is being logged for update.
		LOG_RECORD_DEFAULT - for non update.
		LOG_RECORD_FOR_PURGE - set if the record is being logged for purges 
		                       and no data required to ve logged.
		The other cases (copy, purge, delete), we don't need to distinguish,
		leave no bit set. 
	 */
	public static final int LOG_RECORD_DEFAULT = 0x0;
	public static final int LOG_RECORD_FOR_UPDATE = 0x1;
	public static final int LOG_RECORD_FOR_PURGE = 0x2;

	/**
	 ** Create a new, empty page.
	 **/
	
	protected BasePage()
	{
		
	}

	/**
		Initialize the object, ie. perform work normally perfomed in constructor.
		Called by setIdentity() and createIdentity().
	*/
	protected void initialize()
	{
		setAuxObject(null);
		identity = null;
		recordCount = 0;
		clearLastLogInstant();

		if (SanityManager.DEBUG)
		{
			if (nestedLatch != 0)
				SanityManager.THROWASSERT("nestedLatch is non-zero in initialize - value =  " + nestedLatch);
			if (inClean)
				SanityManager.THROWASSERT("inClean is true in initialize");
			if (preLatch)
				SanityManager.THROWASSERT("preLatch is true in initialize");
		}

	}

	/**
		Must be called by a sub-class before calling setHeaderAtSlot.

	*/
	protected void initializeHeaders(int numRecords) 
    {

		if (SanityManager.DEBUG)
		{
			if (recordCount != 0)
				SanityManager.THROWASSERT(
						"record count = " + recordCount + 
						" before initSlotTable is called"); 
		}

        headers = new StoredRecordHeader[numRecords];
	}


	/*
	** Cacheable methods
	*/

	protected void fillInIdentity(PageKey key) {
		if (SanityManager.DEBUG) {
			SanityManager.ASSERT(identity == null);
		}

		identity = key;
	}

	public void clearIdentity() {

		if (SanityManager.DEBUG) {
			SanityManager.ASSERT(!isLatched());
		}

		identity = null;

		cleanPageForReuse();
	}


	/**
		Initialized this page for reuse or first use
	*/
	protected void cleanPageForReuse()
	{
		setAuxObject(null);
		recordCount = 0;
	}


	/**	
		OK to hand object outside to cache.. 
	*/
	public Object getIdentity() {
		return identity;
	}

	/*
	** Methods of Page
	*/

	private static final RecordHandle InvalidRecordHandle = 
		new RecordId(
			new PageKey(
				new ContainerKey(0,0), ContainerHandle.INVALID_PAGE_NUMBER),
				RecordHandle.INVALID_RECORD_HANDLE);

	public final RecordHandle getInvalidRecordHandle()
	{
		// a static invalid record handle
		return InvalidRecordHandle;
	}

	public static final RecordHandle MakeRecordHandle(PageKey pkey, int recordHandleConstant)
		 throws StandardException
	{
		if (recordHandleConstant >= RecordHandle.FIRST_RECORD_ID)
        {
			throw StandardException.newException(
                SQLState.DATA_CANNOT_MAKE_RECORD_HANDLE, 
                new Long(recordHandleConstant));
        }

		return new RecordId(pkey, recordHandleConstant);
	}

	public final RecordHandle makeRecordHandle(int recordHandleConstant)
		 throws StandardException
	{
		return MakeRecordHandle(getPageId(), recordHandleConstant);
	}

	/** @see Page#getPageNumber */
	public final long getPageNumber() {
		if (SanityManager.DEBUG) {
			SanityManager.ASSERT(isLatched(), "page is not latched.");
			SanityManager.ASSERT(identity != null, "identity is null.");
		}

		return identity.getPageNumber();
	}

	public final RecordHandle getRecordHandle(int recordId) {
		if (SanityManager.DEBUG) {
			SanityManager.ASSERT(isLatched());
		}

		int slot = findRecordById(recordId, FIRST_SLOT_NUMBER);
		if (slot < 0)
			return null;

		return getRecordHandleAtSlot(slot);
	}

	public final RecordHandle getRecordHandleAtSlot(int slot) {
		return getHeaderAtSlot(slot).getHandle(getPageId(), slot);
	}

	/**
	  @see Page#recordExists
	  @exception StandardException recordHandle is not a valid record handle
	*/
	public final boolean recordExists(RecordHandle handle, boolean ignoreDelete)
		 throws StandardException
	{
		if (SanityManager.DEBUG) {
			SanityManager.ASSERT(isLatched());
		}

		if (handle.getId() < RecordHandle.FIRST_RECORD_ID)
        {
			throw StandardException.newException(
                    SQLState.DATA_INVALID_RECORD_HANDLE, handle);
        }

		if (handle.getPageNumber() != getPageNumber())
			return false;

		int slot = findRecordById(handle.getId(), handle.getSlotNumberHint());
		return (slot >= FIRST_SLOT_NUMBER &&
				(ignoreDelete || !isDeletedAtSlot(slot)));
	}

	/**
		<OL>
		<LI>Lock the record (according to the locking policy)
		<LI>If the record is deleted then return null. We must check after we hold the lock to
		ensure that we don't look at the delete status of an uncommitted record.
		<LI>Fetch the record
		<LI>Unlock the record (according to the locking policy)
		</OL>

		@see Page#fetch

		@exception StandardException messageId equals StandardException.newException(SQLState.RECORD_VANISHED
		If the record identfied by handle does not exist on this page.

		@exception StandardException	Standard Cloudscape error policy
		@exception StandardException   record is not on page with message id equal to
			StandardException.newException(SQLState.RECORD_VANISHED.
	*/

	public RecordHandle fetch(
    RecordHandle            handle, 
    Object[]   row, 
    FormatableBitSet                 validColumns, 
    boolean                 forUpdate)
	throws StandardException {

		if (SanityManager.DEBUG) {
			SanityManager.ASSERT(isLatched());
		}

		owner.getLockingPolicy().lockRecordForRead(myLatch, handle, forUpdate);

		// See if the record is deleted or not.
		int slot = getSlotNumber(handle);

        StoredRecordHeader recordHeader = getHeaderAtSlot(slot);

		if (recordHeader.isDeleted())
			return null;

        FetchDescriptor hack_fetch = 
            new FetchDescriptor(
                    row.length, validColumns, (Qualifier[][]) null);

		// magic to copy rows across ...
		restoreRecordFromSlot(
            slot, row, hack_fetch, handle, recordHeader, true);

		owner.getLockingPolicy().unlockRecordAfterRead(
                owner.getTransaction(), owner, handle, forUpdate, true);

		return handle;
	}


	public RecordHandle fetchFromSlot(
    RecordHandle            rh, 
    int                     slot, 
    Object[]   row,
    FetchDescriptor         fetchDesc,
    boolean                 ignoreDelete)
		 throws StandardException
	{
		if (SanityManager.DEBUG) {
			SanityManager.ASSERT(isLatched());

			if (rh != null)
				SanityManager.ASSERT(getSlotNumber(rh) == slot);
		}

		checkSlotOnPage(slot);

        StoredRecordHeader recordHeader = getHeaderAtSlot(slot);

		if (rh == null)
			rh = recordHeader.getHandle(getPageId(), slot);

		if (!ignoreDelete && recordHeader.isDeleted())
			return null;

        /*
        SanityManager.DEBUG_PRINT("fetchFromSlot", "before.");
        SanityManager.showTrace(new Throwable());
        SanityManager.DEBUG_PRINT("fetchFromSlot", "fetchDesc = " + fetchDesc);

        if (fetchDesc != null)
        {
            SanityManager.DEBUG_PRINT("fetchFromSlot", 
                ";fetchDesc.getMaxFetchColumnId() = " + 
                    fetchDesc.getMaxFetchColumnId() +
                ";fetchDesc.getValidColumns() = " + 
                    fetchDesc.getValidColumns() +
                ";fetchDesc.getQualifierList() = " + 
                    fetchDesc.getQualifierList()
            );
        }
        */

        return(
            restoreRecordFromSlot(
                slot, row, fetchDesc, rh, recordHeader, true) ? rh : null);
	}


	/**
		@exception StandardException	Standard Cloudscape error policy
		@see Page#fetchFieldFromSlot
	 */
	public final RecordHandle fetchFieldFromSlot(
    int                 slot, 
    int                 fieldId, 
    Object column)
		throws StandardException
	{
        // need to allocate row with fieldId cols because of sparse row change
        // needs to be RESOLVED
		Object[] row = new Object[fieldId + 1];
		row[fieldId] = column;
		FormatableBitSet singleColumn = new FormatableBitSet(fieldId + 1);

		singleColumn.set(fieldId);

        FetchDescriptor fetchDesc = 
            new FetchDescriptor(fieldId + 1, singleColumn,(Qualifier[][]) null);

		return(fetchFromSlot(null, slot, row, fetchDesc, true));
	}

	/** 
		@exception StandardException Record does not exist on this page.

        @see Page#getSlotNumber
	 */
	public final int getSlotNumber(RecordHandle handle)
		 throws StandardException
	{
		if (SanityManager.DEBUG) {
			SanityManager.ASSERT(isLatched());
		}

		int slot = findRecordById(handle.getId(), handle.getSlotNumberHint());

		if (slot < 0)
        {
			throw StandardException.newException(
                    SQLState.RAWSTORE_RECORD_VANISHED, handle);
        }

		return slot;
	}

	/** 
		@exception StandardException Record does not exist on this page.

        @see Page#getNextSlotNumber
	 */
	public final int getNextSlotNumber(RecordHandle handle)
		 throws StandardException
	{
		if (SanityManager.DEBUG) {
			SanityManager.ASSERT(isLatched());
		}

		int slot = findNextRecordById(handle.getId());

		return slot;
	}

	/** @see Page#insertAtSlot
		@exception StandardException	Standard Cloudscape error policy
	 */
	public RecordHandle insertAtSlot(
    int                     slot, 
    Object[]   row, 
    FormatableBitSet                 validColumns, 
    LogicalUndo             undo,
    byte                    insertFlag, 
    int                     overflowThreshold)
		throws StandardException
	{
		if (SanityManager.DEBUG) {
			if (overflowThreshold == 0)
				SanityManager.THROWASSERT("overflowThreshold cannot be 0");
		}

		if ((insertFlag & Page.INSERT_DEFAULT) == Page.INSERT_DEFAULT) {
			return (insertNoOverflow(slot, row, validColumns, undo, insertFlag, overflowThreshold));
		} else {
			if (SanityManager.DEBUG) {
				if (undo != null)
					SanityManager.THROWASSERT("logical undo with overflow allowed on insert " + undo.toString());
			}
			return (insertAllowOverflow(slot,
				row, validColumns, 0, insertFlag, overflowThreshold, (RecordHandle) null));
		}
	}
	
	protected RecordHandle insertNoOverflow(
    int                     slot, 
    Object[]   row, 
    FormatableBitSet                 validColumns,
    LogicalUndo             undo,
    byte                    insertFlag, 
    int                     overflowThreshold)
		throws StandardException
	{
		
		if (SanityManager.DEBUG) {
			SanityManager.ASSERT(isLatched());
		}

		if (!owner.updateOK())
        {
			throw StandardException.newException(
                    SQLState.DATA_CONTAINER_READ_ONLY);
        }

		if (slot < FIRST_SLOT_NUMBER || slot > recordCount)
        {
			throw StandardException.newException(
                    SQLState.DATA_SLOT_NOT_ON_PAGE);
        }

		if (!allowInsert())
			return null;

		RawTransaction t = owner.getTransaction();

		// logical operations not allowed in internal transactions.
		if (undo != null) {
			t.checkLogicalOperationOk();
		}

        int recordId;
        RecordHandle handle;

        do {

            // loop until we get a new record id we can get a lock on.

            // If we can't get the lock without waiting then assume the record
            // id is owned by another xact.  The current heap overflow 
            // algorithm makes this likely, as it first try's to insert a row
            // telling raw store to fail if it doesn't fit on the page getting
            // a lock on an id that never makes it to disk.   The inserting
            // transaction will hold a lock on this "unused" record id until
            // it commits.  The page can leave the cache at this point, and
            // the inserting transaction has not dirtied the page (it failed
            // after getting the lock but before logging anything), another
            // inserting transaction will then get the same id as the 
            // previous inserter - thus the loop on lock waits.
            //
            // The lock we request indicates that this is a lock for insert,
            // which the locking policy may use to perform locking concurrency
            // optimizations.

            recordId = newRecordIdAndBump();
            handle   = new RecordId(getPageId(), recordId, slot);
            
        } while(!owner.getLockingPolicy().lockRecordForWrite(
                    t, handle, 
                    true  /* lock is for insert */, 
                    false /* don't wait for grant */));


		owner.getActionSet().actionInsert(t, this, slot, recordId, row, validColumns,
			undo, insertFlag, 0, false, -1, (DynamicByteArrayOutputStream) null, -1, overflowThreshold);

		// at this point the insert has been logged and made on the physical 
        // page the in-memory manipulation of the slot table is also performed
        // by the PageActions object that implements actionInsert.

		return handle;
	}

	/** @see Page#insert
		@exception StandardException	Standard Cloudscape error policy
	 */
	public final RecordHandle insert(
    Object[]   row, 
    FormatableBitSet                 validColumns,
    byte                    insertFlag, 
    int                     overflowThreshold)
		throws StandardException
	{

		if (SanityManager.DEBUG) {
			if (overflowThreshold == 0)
				SanityManager.THROWASSERT("overflowThreshold much be greater than 0");
		}

		if (((insertFlag & Page.INSERT_DEFAULT) == Page.INSERT_DEFAULT)) {
			return (insertAtSlot(recordCount, row, validColumns,
				(LogicalUndo) null, insertFlag, overflowThreshold));
		} else {
			return (insertAllowOverflow(recordCount, row, validColumns, 0,
				insertFlag, overflowThreshold, (RecordHandle) null));
		}
	}

	/**
		Insert a row allowing overflow.

		If handle is supplied then the record at that hanlde will be updated
		to indicate it is a partial row and it has an overflow portion.

		@exception StandardException	Standard Cloudscape error policy
	*/
	public RecordHandle insertAllowOverflow(
    int                     slot, 
    Object[]   row, 
    FormatableBitSet                 validColumns,
    int                     startColumn, 
    byte                    insertFlag, 
    int                     overflowThreshold, 
    RecordHandle            nextPortionHandle)
		throws StandardException 
	{

		BasePage curPage = this;

		if (!curPage.owner.updateOK())
        {
			throw StandardException.newException(
                    SQLState.DATA_CONTAINER_READ_ONLY);
        }


		// Handle of the first portion of the chain
		RecordHandle headHandle = null;
		RecordHandle handleToUpdate = null;

		RawTransaction t = curPage.owner.getTransaction();

		for (;;) {

			if (SanityManager.DEBUG) {
				SanityManager.ASSERT(curPage.isLatched());
			}

			if (!curPage.allowInsert())
				return null;

			// 'this' is the head page
			if (curPage != this)
				slot = curPage.recordCount;

			boolean isLongColumns   = false;
			int     realStartColumn = -1;
			int     realSpaceOnPage = -1;

			DynamicByteArrayOutputStream logBuffer = null;

            // allocate new record id and handle
            int          recordId = curPage.newRecordIdAndBump();
			RecordHandle handle   = 
                new RecordId(curPage.getPageId(), recordId, slot);

			if (curPage == this) {


				// Lock the row, if it is the very first portion of the record.
				if (handleToUpdate == null) {

                    while (!owner.getLockingPolicy().lockRecordForWrite(
                                t, handle, 
                                true  /* lock is for insert */, 
                                false /* don't wait for grant */)) {

                        // loop until we get a new record id we can get a lock 
                        // on.  If we can't get the lock without waiting then 
                        // assume the record id is owned by another xact.  The 
                        // current heap overflow algorithm makes this likely, 
                        // as it first try's to insert a row telling raw store
                        // to fail if it doesn't fit on the page getting a lock
                        // on an id that never makes it to disk.   The 
                        // inserting transaction will hold a lock on this
                        // "unused" record id until it commits.  The page can 
                        // leave the cache at this point, and the inserting 
                        // transaction has not dirtied the page (it failed
                        // after getting the lock but before logging anything),
                        // another inserting transaction will then get the 
                        // same id as the previous inserter - thus the loop on
                        // lock waits.
                        //
                        // The lock we request indicates that this is a lock 
                        // for insert, which the locking policy may use to 
                        // perform locking concurrency optimizations.

                        // allocate new record id and handle
                        recordId = curPage.newRecordIdAndBump();
                        handle   = 
                            new RecordId(curPage.getPageId(), recordId, slot);
                    }
				}

				headHandle = handle;
			}

			do {

				// do this loop at least once.  If we caught a long Column,
				// then, we redo the insert with saved logBuffer.
				try {

					startColumn = 
                        owner.getActionSet().actionInsert(
                            t, curPage, slot, recordId,
                            row, validColumns, (LogicalUndo) null, 
                            insertFlag, startColumn, false,
                            realStartColumn, logBuffer, realSpaceOnPage, 
                            overflowThreshold);
					isLongColumns = false;

				} catch (LongColumnException lce) {


					// we caught a long column exception
					// three things should happen here:
					// 1. insert the long column into overflow pages.
					// 2. append the overflow field header in the main chain.
					// 3. continue the insert in the main data chain.
					logBuffer = new DynamicByteArrayOutputStream(lce.getLogBuffer());

					// step 1: insert the long column ... use the same
					// insertFlag as the rest of the row.
					RecordHandle longColumnHandle = 
						insertLongColumn(curPage, lce, insertFlag);

					// step 2: append the overflow field header to the log buffer
					int overflowFieldLen = 0;
					try {
						overflowFieldLen +=
							appendOverflowFieldHeader((DynamicByteArrayOutputStream)logBuffer, longColumnHandle);
					} catch (IOException ioe) {
						// YYZ: revisit...  ioexception, insert failed...
						return null;
					}

					// step 3: continue the insert in the main data chain
					// need to pass the log buffer, and start column to the next insert.
					realStartColumn = lce.getNextColumn() + 1;
					realSpaceOnPage = lce.getRealSpaceOnPage() - overflowFieldLen;

					isLongColumns = true;
				}
			} while (isLongColumns);

			if (handleToUpdate != null) {
				// update the recordheader on the previous page
				updateOverflowDetails(handleToUpdate, handle);
			}

			// all done
			if (startColumn == -1) {

				if (curPage != this)
					curPage.unlatch();

				if (nextPortionHandle != null) {
					// need to update the overflow details of the last portion
					// to point to the existing portion
					updateOverflowDetails(handle, nextPortionHandle);
				}

				return headHandle;
			}

			handleToUpdate = handle;
				
			BasePage nextPage = 
                curPage.getOverflowPageForInsert(
                    slot, row, validColumns,startColumn);

			if (curPage != this)
				curPage.unlatch();
			curPage = nextPage;
		}

	}

	/**
	  
		When we update a column, it turned into a long column.  Need to change
		the update to effectively insert a new long column chain.

		@exception StandardException Unexpected exception from the implementation
	 */
	protected RecordHandle insertLongColumn(BasePage mainChainPage,
			LongColumnException lce, byte insertFlag)
		throws StandardException
	{

		// Object[] row = new Object[1];
		// row[0] = (Object) lce.getColumn();
		Object[] row = new Object[1];
		row[0] = lce.getColumn();

		RecordHandle firstHandle = null;
		RecordHandle handle = null;
		RecordHandle prevHandle = null;
		BasePage curPage = mainChainPage;
		BasePage prevPage = null;
		boolean isFirstPage = true;

		// when inserting a long column startCOlumn is just used
		// as a flag. -1 means the insert is complete, != -1 indicates
		// more inserts are required.
		int startColumn = 0;
		RawTransaction t = curPage.owner.getTransaction();

		do {
			// in this loop, we do 3 things:
			// 1. get a new overflow page
			// 2. insert portion of a long column
			// 3. update previous handle, release latch on previous page

			if (!isFirstPage) {
				prevPage = curPage;
				prevHandle = handle;
			}

			// step 1. get a new overflow page
			curPage = (BasePage) getNewOverflowPage();

			if (SanityManager.DEBUG) {
				SanityManager.ASSERT(curPage.isLatched());
				SanityManager.ASSERT(curPage.allowInsert());
			}

			int slot = curPage.recordCount;

			int recordId = curPage.newRecordId();
			handle = new RecordId(curPage.getPageId(), recordId, slot);

			if (isFirstPage)
				firstHandle = handle;

			// step 2: insert column portion
			startColumn = owner.getActionSet().actionInsert(t, curPage, slot, recordId,
				row, (FormatableBitSet)null, (LogicalUndo) null, insertFlag,
				startColumn, true, -1, (DynamicByteArrayOutputStream) null, -1, 100);

			// step 3: if it is not the first page, update previous page,
			// then release latch on prevPage
			if (!isFirstPage) {
				// for the previous page, add an overflow field header,
				// and update the record header to show 2 fields
				prevPage.updateFieldOverflowDetails(prevHandle, handle);
				prevPage.unlatch();
				prevPage = null;
			} else
				isFirstPage = false;

		} while (startColumn != (-1)) ;

		if (curPage != null) {
			curPage.unlatch();
			curPage = null;
		}

		return (firstHandle);
	}


	/**
		The page or its header is about to be modified.
		Loggable actions use this to make sure the page gets cleaned if a
		checkpoint is taken after any log record is sent to the log stream but
		before the page is actually dirtied.
	*/
	public abstract void preDirty();

	/**
		Update the overflow pointer for a long row

		<BR> MT - latched - page latch must be held

		@param handle			handle of the record for long row
		@param overflowHandle	the overflow (continuation) pointer for the long row

		@exception StandardException	Standard Cloudscape error policy
	*/
	public abstract void updateOverflowDetails(RecordHandle handle, RecordHandle overflowHandle)
		throws StandardException;

	/**
		Update the overflow pointer for a long column

		<BR> MT - latched - page latch must be held

		@param handle			handle of the record for long row
		@param overflowHandle	the overflow (continuation) pointer for the long row

		@exception StandardException	Standard Cloudscape error policy
	*/
	public abstract void updateFieldOverflowDetails(RecordHandle handle, RecordHandle overflowHandle)
		throws StandardException;

	/**
		Append an overflow pointer to a partly logged row,
		to point to a long column that just been logged.

		<BR> MT - latched - page latch must be held

		@param logBuffer		The buffer that contains the partially logged row.
		@param overflowHandle	the overflow (continuation) pointer
								to the beginning of the long column

		@exception StandardException	Standard Cloudscape error policy
	*/
	public abstract int appendOverflowFieldHeader(DynamicByteArrayOutputStream logBuffer, RecordHandle overflowHandle)
		throws StandardException, IOException;

	public abstract BasePage getOverflowPageForInsert(
    int        slot, 
    Object[]   row, 
    FormatableBitSet    validColumns, 
    int        startColumn)
		throws StandardException;

	protected abstract BasePage getNewOverflowPage()
		throws StandardException;

	public final boolean update(
    RecordHandle            handle, 
    Object[]   row, 
    FormatableBitSet                 validColumns)
		throws StandardException
	{
		if (SanityManager.DEBUG) {
			SanityManager.ASSERT(isLatched());
		}

		if (!owner.updateOK())
        {
			throw StandardException.newException(
                    SQLState.DATA_CONTAINER_READ_ONLY);
        }

		RawTransaction t = owner.getTransaction();

		owner.getLockingPolicy().lockRecordForWrite(myLatch, handle);

		int slot = getSlotNumber(handle);

		if (isDeletedAtSlot(slot))
			return false;

		doUpdateAtSlot(t, slot, handle.getId(), row, validColumns);
		
		return true;
	}


	/** @see Page#delete
	    @see BasePage#deleteAtSlot
		@exception StandardException Standard exception policy. 
	*/
	public boolean delete(RecordHandle handle, LogicalUndo undo)
		throws StandardException
	{
		if (SanityManager.DEBUG) {
			SanityManager.ASSERT(isLatched());
		}

		owner.getLockingPolicy().lockRecordForWrite(myLatch, handle);

		int slot = getSlotNumber(handle);

		if (isDeletedAtSlot(slot))
			return false;

		deleteAtSlot(slot, true, undo);
		return true;
	}

	/** @see Page#updateAtSlot
		@exception StandardException	Standard Cloudscape error policy
		@exception StandardException	StandardException.newException(SQLState.UPDATE_DELETED_RECORD
		if the record is already deleted
		@exception StandardException	StandardException.newException(SQLState.CONTAINER_READ_ONLY
		if the container is read only
	 */
	public final RecordHandle updateAtSlot(
    int                     slot, 
    Object[]   row, 
    FormatableBitSet                 validColumns)
		 throws	StandardException
	{
		if (SanityManager.DEBUG) {
			SanityManager.ASSERT(isLatched());
		}

		if (!owner.updateOK())
        {
			throw StandardException.newException(
                    SQLState.DATA_CONTAINER_READ_ONLY);
        }

		if (isDeletedAtSlot(slot))
        {
			throw StandardException.newException(
                    SQLState.DATA_UPDATE_DELETED_RECORD);
        }


		RecordHandle handle = getRecordHandleAtSlot(slot);

		RawTransaction t = owner.getTransaction();

		doUpdateAtSlot(t, slot, handle.getId(), row, validColumns);
		
		return handle;
	}

	public abstract void doUpdateAtSlot(
    RawTransaction          t, 
    int                     slot, 
    int                     id, 
    Object[]   row, 
    FormatableBitSet                 validColumns)
		throws	StandardException;

	/** @see Page#updateFieldAtSlot
		@exception StandardException	Standard Cloudscape error policy
		@exception StandardException	StandardException.newException(SQLState.UPDATE_DELETED_RECORD
		if the record is already deleted
		@exception StandardException	StandardException.newException(SQLState.CONTAINER_READ_ONLY
		if the container is read only
	*/
	public RecordHandle updateFieldAtSlot(
    int                 slot, 
    int                 fieldId, 
    Object newValue, 
    LogicalUndo         undo)
		throws StandardException
	{
		if (SanityManager.DEBUG) {
			SanityManager.ASSERT(isLatched());
			SanityManager.ASSERT(newValue != null);
		}

		if (!owner.updateOK())
        {
			throw StandardException.newException(
                    SQLState.DATA_CONTAINER_READ_ONLY);
        }

		if (isDeletedAtSlot(slot))
        {
			throw StandardException.newException(
                    SQLState.DATA_UPDATE_DELETED_RECORD);
        }

		RawTransaction t = owner.getTransaction();
		RecordHandle handle = getRecordHandleAtSlot(slot);

		owner.getActionSet().actionUpdateField(t, this, slot,
				handle.getId(), fieldId, newValue, undo);

		return handle;
	}

	/** @see Page#fetchNumFields
		@exception StandardException Standard exception policy. 
	*/
	public final int fetchNumFields(RecordHandle handle)
		 throws StandardException
	{
		if (SanityManager.DEBUG) {
			SanityManager.ASSERT(isLatched());
		}

		return fetchNumFieldsAtSlot(getSlotNumber(handle));
	}

	/** @see Page#fetchNumFieldsAtSlot
		@exception StandardException Standard exception policy. 
	*/
	public int fetchNumFieldsAtSlot(int slot)
		 throws StandardException
	{
		if (SanityManager.DEBUG) {
			SanityManager.ASSERT(isLatched());
		}

		return getHeaderAtSlot(slot).getNumberFields();
	}

	/** 
		@see Page#deleteAtSlot

		@param slot		the slot number
		@param delete	true if this record is to be deleted, false if this
						deleted record is to be marked undeleted
		@param undo		logical undo logic if necessary

		@exception StandardException Standard exception policy. 
		@exception StandardException	StandardException.newException(SQLState.UPDATE_DELETED_RECORD
		if an attempt to delete a record that is already deleted
		@exception StandardException	StandardException.newException(SQLState.UNDELETE_RECORD
		if an attempt to undelete a record that is not deleted
	 */
	public RecordHandle deleteAtSlot(int slot, boolean delete, LogicalUndo undo)
		throws StandardException
	{
		if (SanityManager.DEBUG) {
			SanityManager.ASSERT(isLatched());
		}

		if (!owner.updateOK())
        {
			throw StandardException.newException(
                    SQLState.DATA_CONTAINER_READ_ONLY);
        }

		if (delete)
		{
			if (isDeletedAtSlot(slot))
            {
				throw StandardException.newException(
                        SQLState.DATA_UPDATE_DELETED_RECORD);
            }

		}
		else					// undelete a deleted record
		{
			if (!isDeletedAtSlot(slot))
            {
				throw StandardException.newException(
                        SQLState.DATA_UNDELETE_RECORD);
            }
		}

		RawTransaction t = owner.getTransaction();

		// logical operations not allowed in internal transactions.
		if (undo != null) {
			t.checkLogicalOperationOk();
		}

		RecordHandle handle = getRecordHandleAtSlot(slot);

		owner.getActionSet().actionDelete(t, this, slot, handle.getId(), delete, undo);
		
		// delete/undelete the record in the stored version
		// and in the in memory version performed by the PageActions item 

		return handle;
	}


	/** 
		Purge one or more rows on a non-overflow page.

		@see Page#purgeAtSlot
		@exception StandardException Standard exception policy. 
	 */
	public void purgeAtSlot(int slot, int numpurges, boolean needDataLogged)
		throws StandardException
	{
		if (SanityManager.DEBUG) 
        {
			SanityManager.ASSERT(isLatched());

            if (isOverflowPage())
                SanityManager.THROWASSERT(
                    "purge committed deletes on an overflow page.  Page = " + 
                    this);
		}


		if (numpurges <= 0)
			return;

		if (!owner.updateOK())
        {
			throw StandardException.newException(
                    SQLState.DATA_CONTAINER_READ_ONLY);
        }

		if ((slot < 0) || ((slot+numpurges) > recordCount))
        {

			throw StandardException.newException(
                    SQLState.DATA_SLOT_NOT_ON_PAGE);
        }

		RawTransaction t = owner.getTransaction();

		// lock the records to be purged
		int[] recordIds = new int[numpurges];

		PageKey pageId = getPageId(); // RESOLVE: MT problem ?

		for (int i = 0; i < numpurges; i++)
		{
			recordIds[i] = getHeaderAtSlot(slot + i).getId();

			// get row lock on head row piece
			RecordHandle handle = getRecordHandleAtSlot(slot);
			owner.getLockingPolicy().lockRecordForWrite(t, handle, false, true);

			// Before we purge these rows, we need to make sure they don't have
			// overflow rows and columns.  Only clean up long rows and long
			// columns if this is not a temporary container, otherwise, just
			// loose the space.

			if (owner.isTemporaryContainer() ||	entireRecordOnPage(slot+i))
				continue;

			// row[slot+i] has overflow rows and/or long columns, reclaim
			// them in a loop.
			RecordHandle headRowHandle = 
				getHeaderAtSlot(slot+i).getHandle(pageId, slot+i);
			purgeRowPieces(t, slot+i, headRowHandle, needDataLogged);
		}

		owner.getActionSet().actionPurge(t, this, slot, numpurges, recordIds, needDataLogged);

	}

	/**
		Purge all the overflow columns and overflow rows of the record at slot.
		@exception StandardException Standard exception policy. 
	 */
	protected abstract void purgeRowPieces(RawTransaction t, int slot, 
										   RecordHandle headRowHandle, 
										   boolean needDataLogged)
		 throws StandardException; 


	/** @see Page#copyAndPurge
		@exception StandardException Standard exception policy. 
	*/
	public void copyAndPurge(Page destPage, int src_slot, int num_rows, int dest_slot)
		 throws StandardException
	{
		if (SanityManager.DEBUG) {
			SanityManager.ASSERT(isLatched());
		}

		if (num_rows <= 0)
        {
            throw StandardException.newException(SQLState.DATA_NO_ROW_COPIED);
        }

		if (!owner.updateOK())
        {
			throw StandardException.newException(
                    SQLState.DATA_CONTAINER_READ_ONLY);
        }

		if ((src_slot < 0) || ((src_slot + num_rows) > recordCount))
        {
			throw StandardException.newException(
                    SQLState.DATA_SLOT_NOT_ON_PAGE);
        }

		if (SanityManager.DEBUG) {
			// first copy into the destination page, let it do the work
			// if no problem, then purge from this page
			SanityManager.ASSERT((destPage instanceof BasePage), "must copy from BasePage to BasePage");
		}

		BasePage dpage = (BasePage)destPage;

		// make sure they are from the same container - this means they are of
		// the same size and have the same page and record format.

		PageKey pageId = getPageId(); // RESOLVE: MT problem ?

		if (!pageId.getContainerId().equals(dpage.getPageId().getContainerId()))
        {
			throw StandardException.newException(
                    SQLState.DATA_DIFFERENT_CONTAINER, 
                    pageId.getContainerId(), 
                    dpage.getPageId().getContainerId());
        }

		int[] recordIds = new int[num_rows];

		RawTransaction t = owner.getTransaction();

		// lock the records to be purged and calculate total space needed
		for (int i = 0; i < num_rows; i++)
		{
			RecordHandle handle = getRecordHandleAtSlot(src_slot+i);
			owner.getLockingPolicy().lockRecordForWrite(t, handle, false, true);

			recordIds[i] = getHeaderAtSlot(src_slot + i).getId();
		}

		// first copy num_rows into destination page
		dpage.copyInto(this, src_slot, num_rows, dest_slot);

		// Now purge num_rows from this page
		// Do NOT purge overflow rows, if it has such a thing.  This operation
		// is called by split and if the key has overflow, spliting the head
		// page does not copy over the remaining pieces, i.e.,the new head page
		// still points to those pieces.

		owner.getActionSet().actionPurge (t, this, src_slot, num_rows,
										  recordIds, true);
	}



	/**
		Unlatch the page.
		@see Page#unlatch
	*/
	public void unlatch() {
		if (SanityManager.DEBUG) {
			SanityManager.ASSERT(isLatched());
		}

	   releaseExclusive();
	}

	/** @see Page#isLatched */
	public boolean isLatched() {
		if (SanityManager.DEBUG) {

			synchronized(this)
			{
				SanityManager.ASSERT(identity != null);
				if (owner != null) {
					if (owner != myLatch.getQualifier())
						SanityManager.THROWASSERT("Page incorrectly latched - " + owner + " " + myLatch.getQualifier());
				}
			}
		}

		return owner != null;
	}

	/** @see Page#recordCount */
	public final int recordCount() {
		if (SanityManager.DEBUG) {
			SanityManager.ASSERT(isLatched());
		}

		return recordCount;
	}

	/**
		get record count without checking for latch
	*/
	protected abstract int internalDeletedRecordCount();

	/**
		get record count without checking for latch
	*/
	protected int internalNonDeletedRecordCount()
	{
		// deallocated or freed page, don't count
		if (pageStatus != VALID_PAGE)
			return 0;

		int deletedCount = internalDeletedRecordCount();

		if (deletedCount == -1) {
			int count = 0;
			int	maxSlot = recordCount;
			for (int slot = FIRST_SLOT_NUMBER ; slot < maxSlot; slot++) {
				if (!isDeletedOnPage(slot))
					count++;
			}
			return count;

		} else  {

			if (SanityManager.DEBUG) {
				int delCount = 0;
				int	maxSlot = recordCount;
				for (int slot = FIRST_SLOT_NUMBER ; slot < maxSlot; slot++) {
					if (recordHeaderOnDemand(slot).isDeleted())
						delCount++;
				}
				if (delCount != deletedCount)
					SanityManager.THROWASSERT("incorrect deleted row count.  Should be: "
						+ delCount + ", instead got: " + deletedCount
						+ ", maxSlot = " + maxSlot + ", recordCount = " + recordCount);
			}

			return (recordCount - deletedCount);
		}
	}

	/** @see Page#nonDeletedRecordCount */
	public int nonDeletedRecordCount() {
		if (SanityManager.DEBUG) {
			SanityManager.ASSERT(isLatched());
		}

		return internalNonDeletedRecordCount();

	}

	// no need to check for slot on page, call already checked
	protected final boolean isDeletedOnPage(int slot)
	{
		return getHeaderAtSlot(slot).isDeleted();
	}


	/** @see Page#isDeletedAtSlot
		@exception StandardException Standard exception policy. 
	 */
	public boolean isDeletedAtSlot(int slot)
		 throws StandardException
	{
		if (SanityManager.DEBUG) {
			SanityManager.ASSERT(isLatched());
		}

		checkSlotOnPage(slot);

		return isDeletedOnPage(slot);
	}

	/**
		Set the aux object.

		<BR> MT - single thread required. Calls via the Page interface will have the
			page latched, thus providing single threadedness. Otherwise calls via this class
			are only made when the class has no-identity, thus only a single thread can see the object. 

		@see Page#setAuxObject
	*/
	public void setAuxObject(AuxObject obj)
	{
		if (SanityManager.DEBUG) {
			SanityManager.ASSERT((identity == null) || isLatched());
		}

		if (auxObj != null) {
			auxObj.auxObjectInvalidated();
		}

		auxObj = obj;
	}

	/**
		Get the aux object.
		<BR> MT - latched - It is required the caller throws away the returned reference
		when the page is unlatched.

		@see Page#getAuxObject
	*/
	public AuxObject getAuxObject()
	{
		if (SanityManager.DEBUG) {
			SanityManager.ASSERT(isLatched());
		}

		return auxObj;
	}

	/*
	** Methods from Lockable, just require a single exclusive locker
	*/

	/**
		Latch me.
		<BR>
		MT - single thread required (methods of Lockable)
		@see Lockable#lockEvent
	*/
	public void lockEvent(Latch lockInfo) {
		if (SanityManager.DEBUG) {
			SanityManager.ASSERT(owner == null, "Should only be called when not locked");
		}

		synchronized (this) {

			myLatch = lockInfo;

            // Move page state from UNLATCHED to PRELATCH, setExclusiveNo*()
            // routines do the work of completing the latch - using the 
            // preLatch status.  This is so that
            // we don't have to wait for a clean() initiated I/O here while
            // holding the locking system monitor.
			(owner = (BaseContainerHandle) lockInfo.getQualifier()).addObserver(this);
            preLatch = true;
		}
	}

	/**
		Is another request compatible, no never.
		<BR> MT - single thread required (methods of Lockable)
		@see Lockable#requestCompatible
	*/
	public boolean requestCompatible(Object requestedQualifier, Object grantedQualifier) {
		if (SanityManager.DEBUG) {
			SanityManager.ASSERT(owner != null, "Should only be called when locked");
		}

		return false;
	}

	/**
		Is another request compatible, no never.
		<BR> MT - single thread required (methods of Lockable)
		@see Lockable#requestCompatible
	*/
	public boolean lockerAlwaysCompatible() {
		if (SanityManager.DEBUG) {
			SanityManager.ASSERT(owner != null, "Should only be called when locked");
		}

		return false;
	}

	/**
		Unlatch me, only to be called from lock manager.
		<BR> MT - single thread required (methods of Lockable)

		@see Lockable#requestCompatible
	*/
	public void unlockEvent(Latch lockInfo) {
		if (SanityManager.DEBUG) {
			SanityManager.ASSERT(owner != null, "Should only be called when locked");
		}

		synchronized (this) {

			if (SanityManager.DEBUG) {
				if (nestedLatch != 0)
					SanityManager.THROWASSERT("nestedLatch is non-zero on unlockEvent - value = " + nestedLatch);
			}

			owner.deleteObserver(this);
			owner = null;
			myLatch = null;
			if (inClean)
				notifyAll();
		}
	}


	/*
	** Methods of Observer.
	*/

	/**
		This object is set to observe the BaseContainerHandle it was obtained by,
		that handle will notify its observers when it is being closed. In that case
		we will release the latch on the page held by that container.

		<BR>
		MT - latched

		@see Observer#update
	*/

	public void update(Observable obj, Object arg) {

		if (SanityManager.DEBUG) {
			SanityManager.ASSERT(isLatched());
			SanityManager.ASSERT(obj == owner);
		}

		releaseExclusive();
	}

	/*
	** Implementation specific methods
	*/

	/**
		Get the Page identifer

		<BR> MT - RESOLVE
	*/
	public PageKey getPageId() {
		if (SanityManager.DEBUG) {
			SanityManager.ASSERT(identity != null);
		}

		return identity;
	}

	/**
		Get an exclusive latch on the page.
		<BR>
		MT - thread safe
		@exception StandardException Standard Cloudscape policy.
	*/
	public void setExclusive(BaseContainerHandle requester) 
		throws StandardException {

		RawTransaction t = requester.getTransaction();

		// In some cases latches are held until after a commit or an abort
		// (currently internal and nested top transactions.
		// If this is the case then during an abort a latch
		// request will be made for a latch that is already held.
		// We do not allow the latch to be obtained multiple times
		// because i) lock manager might assume latches are exclusive for
		// performance, ii) holding a page latched means that the page is
		// on the container handle's obervers list, if we latched it twice
		// then the paeg would have to be on the list twice, which is not supported
		// since the page has value equality. To be on the list twice reference
		// equality would be required, which would mean pushing a ReferenceObservable
		// object for every latch; iii) other unknown reasons :-)
		synchronized (this)
		{
			// need synchronized block because owner may be set to null in the
			// middle if another thread is in the process of unlatching the
			// page 
			if ((owner != null) && (t == owner.getTransaction())) {

				if (t.inAbort()) {
					//
					nestedLatch++;
					return;
				}
			}
			// just deadlock out ...
		}


		// Latch the page, owner is set through the Lockable call backs.
		t.getLockFactory().latchObject(
            t, this, requester, C_LockFactory.WAIT_FOREVER);		

        // latch granted, but cleaner may "own" the page.  

		if (SanityManager.DEBUG) {
			SanityManager.ASSERT(isLatched(), "page not latched");
		}

        synchronized (this)
        {
            // lockEvent() will grant latch, even if cleaner "owns" the page.
            // Wait here unil cleaner is done.  This is safe as now we own the
            // latch, and have yet to do anything to the in-memory data 
            // structures.
            // 
            // Previously we would wait in lockEvent, but that caused the code 
            // to block on I/O while holding the locking system monitor.

            while (inClean) 
            {
                try 
                {
                    // Expect notify from clean() routine.
                    wait();
                } 
                catch (InterruptedException ie) 
                {
                }
            }

            // no clean taking place, so safe to move to full LATCHED state.
            preLatch = false;
        }

	}

	/**
		Get an exclusive latch on the page, but only if I don't have to wait.
		<BR>
		MT - thread safe
	*/
	boolean setExclusiveNoWait(BaseContainerHandle requester) throws StandardException {

		RawTransaction t = requester.getTransaction();

		// comment in setExclusive()
		synchronized (this)
		{
			if ((owner != null) && (t == owner.getTransaction())) {

				if (t.inAbort()) {
					//
					nestedLatch++;
					return true;
				}
			}
			// just deadlock out ...
		}

		// Latch the page, owner is set through the Lockable call backs.
		boolean gotLatch = t.getLockFactory().latchObject(t, this, requester, C_LockFactory.NO_WAIT);
		if (!gotLatch)
			return false;

        synchronized (this)
        {
            // lockEvent() will grant latch, even if cleaner "owns" the page.
            // Wait here unil cleaner is done.  This is safe as now we own the
            // latch, and have yet to do anything to the in-memory data 
            // structures.
            // 
            // Previously we would wait in lockEvent, but that caused the code 
            // to block on I/O while holding the locking system monitor.

            while (inClean) 
            {
                //if (SanityManager.DEBUG)
                 //   SanityManager.DEBUG_PRINT("setExclusiveNoWait", "in while loop.");

                try 
                {
                    // Expect notify from clean() routine.
                    wait();
                } 
                catch (InterruptedException ie) 
                {
                }
            }

            // no clean taking place, so safe to move to full LATCHED state.
            preLatch = false;
        }

		if (SanityManager.DEBUG) {
			SanityManager.ASSERT(isLatched(), "page not latched");
		}

		return true;
	}

	/**
		Release the exclusive latch on the page.
		<BR>
		MT - latched
	*/
	protected void releaseExclusive() /* throws StandardException */ {

		if (SanityManager.DEBUG) {
            if (!isLatched())
            {
                SanityManager.THROWASSERT(
                    "releaseExclusive failed, nestedLatch = " + nestedLatch);
            }
		}

		if (nestedLatch > 0) {
			nestedLatch--;
			return;
		}

		RawTransaction t = owner.getTransaction();
		t.getLockFactory().unlatch(myLatch);
	}

	/*
	** Manipulation of the in-memory version of the slot table.
	*/

	/**
		Must be called by any non-abstract sub-class to initialise the slot
		table.
	*/

	protected final void setHeaderAtSlot(int slot, StoredRecordHeader rh) {

        if (slot < headers.length)
        {
			// check that array "cache" of headers is big enough.
            if (rh != null)
            {
                headers[slot] = rh;
            }
        }
        else
        {
            // need to grow the array, just allocate new array and copy.
            StoredRecordHeader[] new_headers = new StoredRecordHeader[slot + 1];

            System.arraycopy(headers, 0, new_headers, 0, headers.length);

            headers = new_headers;

            headers[slot] = rh;
        }
	}

	protected final void bumpRecordCount(int number) {
		recordCount += number;
	}

	public final StoredRecordHeader getHeaderAtSlot(int slot) {

        if (slot < headers.length)
        {
            StoredRecordHeader rh = headers[slot];

            return((rh != null) ? rh : recordHeaderOnDemand(slot));
        }
        else
        {
			return recordHeaderOnDemand(slot);
        }
	}

	/**
		Returns true if the entire record of that slot fits inside of this
		page.  Returns false if part of the record on this slot overflows to
		other pages, either due to long row or long column. 

		<BR>
		MT - latched

		@exception StandardException Standard Cloudscape error policy
	 */
	public abstract boolean entireRecordOnPage(int slot) 
		 throws StandardException;


	public abstract StoredRecordHeader recordHeaderOnDemand(int slot);

	/**
		Is the given slot number on the page?

		<BR>
		MT - latched
	*/
	private final void checkSlotOnPage(int slot)
		throws StandardException {

		if (SanityManager.DEBUG) {
			SanityManager.ASSERT(isLatched());
		}

		if (slot >= FIRST_SLOT_NUMBER && slot < recordCount)
        {
				return;
        }

		throw StandardException.newException(SQLState.DATA_SLOT_NOT_ON_PAGE);
	}

	/**
		Mark the record at the passed in slot as deleted.

		return code comes from StoredRecordHeader class:
			return	1, if delete status from not deleted to deleted
			return -1, if delete status from deleted to not deleted
			return  0, if status unchanged.
		<BR>
		<B>Any sub-class must call this method when deleting a record.</B>

		<BR>
		MT - latched

		@exception StandardException Standard Cloudscape error policy
		@exception IOException IO error accessing page
	*/
	public int setDeleteStatus(int slot, boolean delete) throws StandardException, IOException {

		if (SanityManager.DEBUG) {
			// latch check performed in checkSlotOnPage
			checkSlotOnPage(slot);;
		}

		return (getHeaderAtSlot(slot).setDeleted(delete));
	}

	/**
		Mark this page as being deallocated

		@exception StandardException Cloudscape Standard error policy
	*/
	public void deallocatePage() throws StandardException
	{
		if (SanityManager.DEBUG) {
			SanityManager.ASSERT(isLatched());
		}

		if (!owner.updateOK())
        {
			throw StandardException.newException(
                    SQLState.DATA_CONTAINER_READ_ONLY);
        }

		RawTransaction t = owner.getTransaction();

		owner.getActionSet().actionInvalidatePage(t, this);
	}

	/**
		Mark this page as being allocated and initialize it to a pristine page
		@exception StandardException Cloudscape Standard error policy
	*/
	public void initPage(int initFlag, long pageOffset) 
		 throws StandardException 
	{
		if (SanityManager.DEBUG) {
			SanityManager.ASSERT(isLatched());
		}

		if (!owner.updateOK())
        {
			throw StandardException.newException(
                    SQLState.DATA_CONTAINER_READ_ONLY);
        }

		RawTransaction t = owner.getTransaction();

		owner.getActionSet().actionInitPage(
			t, this, initFlag, getTypeFormatId(), pageOffset);
	}

	/**
		Find the slot for the record with the passed in identifier.

		<BR>
		This method returns the record regardless of its deleted status.
        <BR>
        The "slotHint" argument is a hint about what slot the record id might
        be in.  Callers may save the last slot where the record was across
        latch/unlatches to the page, and then pass that slot back as a hint - 
        if the page has not shuffled slots since the last reference then the
        hint will succeed and a linear search is saved.  If the caller has
        no idea where it may be, then FIRST_SLOT_NUMBER is passed in and a
        linear search is performed.
		<BR>
		MT - latched

        @param recordId  record id of the record to search for.
        @param slotHint "hint" about which slot the record might be in.
		
	*/
	public int findRecordById(int recordId, int slotHint) {

		if (SanityManager.DEBUG) {
			SanityManager.ASSERT(isLatched());
		}

		if (slotHint == FIRST_SLOT_NUMBER)
			slotHint = recordId - RecordHandle.FIRST_RECORD_ID;

		int	maxSlot = recordCount();

       if ((slotHint > FIRST_SLOT_NUMBER)        &&
            (slotHint < maxSlot)                   && 
            (recordId == getHeaderAtSlot(slotHint).getId())) {
            return(slotHint);
        } else {
            for (int slot = FIRST_SLOT_NUMBER; slot < maxSlot; slot++) {
				if (recordId == getHeaderAtSlot(slot).getId()) {
                    return slot;
				}
            }
        }

		return -1;
	}

	/**
		Find the slot for the first record on the page with an id greater than 
        the passed in identifier.

		<BR>
        Returns the slot of the first record on the page with an id greater 
        than the one passed in.  Usefulness of this functionality depends on the
        clients use of the raw store interfaces.  If all "new" records are
        always inserted at the end of the page, and the raw store continues
        to guarantee that all record id's will be allocated in increasing order
        on a given page, then a page is always sorted
        in record id order.  For instance current heap tables function this
        way.  If the client ever inserts at a particular slot number, rather
        than at the "end" then the record id's will not be sorted.
        <BR>
        In the case where all record id's are always sorted on a page, then
        this routine can be used by scan's which "lose" their position because
        the row they have as a position was purged.  They can reposition their
        scan at the "next" row after the row that is now missing from the table.
		<BR>
		This method returns the record regardless of its deleted status.
		<BR>
		MT - latched

        @param recordId  record id of the first record on the page with a 
                         record id higher than the one passed in.  If no 
                         such record exists, -1 is returned.
	*/
	private int findNextRecordById(int recordId) 
    {
		if (SanityManager.DEBUG) 
        {
			SanityManager.ASSERT(isLatched());
		}

		int	maxSlot = recordCount();

        for (int slot = FIRST_SLOT_NUMBER; slot < maxSlot; slot++) 
        {
            if (getHeaderAtSlot(slot).getId() > recordId)
            {
                return(slot);
            }
        }

		return -1;
	}

	/**
		Copy num_rows from srcPage, src_slot into this page starting at dest_slot.
		This is destination page of the the copy half of copy and Purge.

		@return An array of the new record identifiers.
		@see Page#copyAndPurge
	 */
	private void copyInto(BasePage srcPage, int src_slot, int num_rows, 
						  int dest_slot)
		 throws StandardException
	{
		if ((dest_slot < 0) || dest_slot > recordCount)
        {
			throw StandardException.newException(
                    SQLState.DATA_SLOT_NOT_ON_PAGE);
        }

		RawTransaction t = owner.getTransaction();

		// get num_rows row locks, need to predict what those recordIds will be

		int[] recordIds = new int[num_rows];

		PageKey pageId = getPageId(); // RESOLVE - MT problem ?

		// get new recordIds for the rows from this page 
		// RESOLVE: we should also record the amount of reserved space

		for (int i = 0; i < num_rows; i++)
		{
			if (i == 0)
				recordIds[i] = newRecordId();
			else
				recordIds[i] = newRecordId(recordIds[i-1]);

			RecordHandle handle = new RecordId(pageId, recordIds[i], i);
			owner.getLockingPolicy().lockRecordForWrite(t, handle, false, true);
		}

		// RESOLVE: need try block here to invalidate self and crash the system
		owner.getActionSet().actionCopyRows(t, this, srcPage,
												  dest_slot, num_rows, src_slot,
												  recordIds);
	}

	/**
		Remove the slot at the in-memory slot table, i.e.,
		slots from 0 to deleteSlot-1 is untouched, deleteSlot is removed from
		in memory slot table, deleteSlot+1 .. recordCount()-1 move to
		down one slot.

		<BR>
		MT - latched
	*/
	protected void removeAndShiftDown(int slot)
	{
		if (SanityManager.DEBUG) {
			SanityManager.ASSERT(isLatched());

			SanityManager.ASSERT(slot >= 0 && slot < recordCount);
		}

        // just copy the records down in the array (copying over the slot
        // entry that is being eliminated) and null out the last entry,
        // it is ok for the array to be larger than necessary.
        //
        // source of copy: slot + 1
        // dest   of copy: slot
        // length of copy: (length of array - source of copy)
        System.arraycopy(
            headers, slot + 1, headers, slot, headers.length - (slot + 1));
        headers[headers.length - 1] = null;

		recordCount--;
	}


	/**
		Shift all records in the in-memory slot table up one slot,
		starting at and including the record in slot 'low'
		A new slot is added to accomdate the move.

		<BR>
		MT - latched
	*/
	protected StoredRecordHeader shiftUp(int low) 
    {

		if (SanityManager.DEBUG) 
        {
			SanityManager.ASSERT(isLatched());

			if ((low < 0) || (low > recordCount))
            {
				SanityManager.THROWASSERT(
                    "shiftUp failed, low must be between 0 and recordCount." + 
                    "  low = " + low + ", recordCount = " + recordCount);
            }
		}

        if (low < headers.length)
        {
            // just copy the records up in the array (copying over the slot
            // entry that is being eliminated) and null out the entry at "low",
            // it is ok for the array to be shorter than necessary.
            //
            // This code throws away the "last" entry in
            // the array, which will cause a record header cache miss if it 
            // is needed.  This delays the object allocation of a new array
            // object until we really need that entry, vs. doing it on the
            // insert.
            //
            // source of copy: low
            // dest   of copy: low + 1
            // length of copy: (length of array - dest of copy)


			// adding in the middle
            System.arraycopy(
                headers, low, headers, low + 1, headers.length - (low + 1));

            headers[low] = null;
		}

		return(null);
	}



	/**
		Try to compact this record.  Deleted record are treated the same way as
		nondeleted record.  This page must not be an overflow page.  The record
		may already have been purged from the page.

	  	<P>
		<B>Locking Policy</B>
		<P>
		No locks are obtained.

		<BR>
		MT - latched

		<P>
		<B>NOTE : CAVEAT </B><BR>
		This operation will physically get rid of any reserved space this
		record may have, or it may compact the record by merging strung out row
		pieces together.  Since the freed reserved space is immediately usable
		by other transactions which latched the page, it is only safe to use
		this operation if the caller knows that it has exclusive access to the
		page for the duration of the transaction, i.e., effectively holding a
		page lock on the page, AND that the record has no uncommitted
		updates.

	  @param record Handle to deleted or non-deleted record
	  @see ContainerHandle#compactRecord

	  @exception StandardException	Standard Cloudscape error policy
	*/
	public void compactRecord(RecordHandle handle) throws StandardException
	{
		if (SanityManager.DEBUG) {
			SanityManager.ASSERT(isLatched());
		}

		if (!owner.updateOK())
        {
			throw StandardException.newException(
                    SQLState.DATA_CONTAINER_READ_ONLY);
        }

		if (handle.getId() < RecordHandle.FIRST_RECORD_ID)
        {
			throw StandardException.newException(
                    SQLState.DATA_INVALID_RECORD_HANDLE, handle);
        }

		if (handle.getPageNumber() != getPageNumber())
        {
			throw StandardException.newException(
                    SQLState.DATA_WRONG_PAGE_FOR_HANDLE, handle);
        }

		if (isOverflowPage())
        {
			throw StandardException.newException(
                    SQLState.DATA_UNEXPECTED_OVERFLOW_PAGE, handle);
        }

		int slot = findRecordById(handle.getId(), handle.getSlotNumberHint());

		if (slot >= 0)
		{
			compactRecord(owner.getTransaction(), slot, handle.getId());
		}
		// else record gone, no compaction necessary
	}


	/*
	** Methods that read/store records/fields based upon calling methods
	** a sub-calls provides to do the actual storage work.
	*/

	/*
	** Page LastLog Instant control
	*/

	public final LogInstant getLastLogInstant()
	{
		return lastLog;
	}

	protected final void clearLastLogInstant() {
		lastLog = null;
	}

	protected final void updateLastLogInstant(LogInstant instant)
	{
		if (SanityManager.DEBUG) {
			SanityManager.ASSERT(isLatched());
		}

		// we should not null out the log instant even if this page is being
		// updated by a non-logged action, there may have been logged action
		// before this and we don't want to loose that pointer
		if (instant != null)
			lastLog = instant;
	}

	/*
	** Page Version control
	*/

	/**
		Return the current page version.
	*/
	public final long getPageVersion()
	{
		return pageVersion;
	}

	/**
		increment the version by one and return the new version.
	*/
	protected final long bumpPageVersion() 
	{
		if (SanityManager.DEBUG) {
			SanityManager.ASSERT(isLatched());
		}
		return ++pageVersion;
	}

	/**
		set it when the page is read from disk.

		<BR> MT - single thread required - Only called while the page has no identity which
		requires that only a single caller can be accessing it.
	*/
	public final void setPageVersion(long v)
	{
		pageVersion = v;
	}


	/**
		Set page status based on passed in status flag.
	*/
	protected void setPageStatus(byte status)
	{
		pageStatus = status;
	}


	/**
		Get the page status, one of the values in the above page status flag
	*/
	public byte getPageStatus()
	{
		return pageStatus;
	}


	/*
	** abstract methods that an implementation must provide.
	**
	** <BR> MT - latched, page is latched when these methods are called.
	*/

    /**
     * Read the record at the given slot into the given row.
     * <P>
     * This reads and initializes the columns in the row array from the raw 
     * bytes stored in the page associated with the given slot.  If validColumns
     * is non-null then it will only read those columns indicated by the bit
     * set, otherwise it will try to read into every column in row[].  
     * <P>
     * If there are more columns than entries in row[] then it just stops after
     * every entry in row[] is full.
     * <P>
     * If there are more entries in row[] than exist on disk, the requested 
     * excess columns will be set to null by calling the column's object's
     * restoreToNull() routine 
     * (ie.  ((Object) column).restoreToNull() ).
     * <P>
     * If a qualifier list is provided then the row will only be read from
     * disk if all of the qualifiers evaluate true.  Some of the columns may
     * have been read into row[] in the process of evaluating the qualifier.
     *
     * <BR> MT - latched, page is latched when this methods is called.
     *
     *
     * @param slot              the slot number
     * @param row (out)         filled in sparse row
     * @param validColumns      A bit map indicating which columns to return, if
     *                          null return all the columns.
     * @param qualifier_list    An array of qualifiers to apply to the row, only
     *                          return row if qualifiers are all true, if array
     *                          is null always return the row.
     * @param recordToLock      the record handle for the row at top level,
     *                          and is used in OverflowInputStream to lock the 
     *                          row for Blobs/Clobs.
     * @param isHeadRow         Is the head row portion of the row, false if
     *                          a long row and the 2-N'th portion of the long
     *                          row.
     *
     * @return  false if a qualifier_list is provided and the row does not 
     *          qualifier (no row read in that case), else true.
     *
     * @exception StandardException	Standard Cloudscape error policy
     **/
	protected abstract boolean restoreRecordFromSlot(
    int                     slot, 
    Object[]   row,
    FetchDescriptor         fetchDesc,
    RecordHandle            rh,
    StoredRecordHeader      recordHeader,
    boolean                 isHeadRow)
		throws StandardException;


	/**
		Read portion of a log record at the given slot into the given byteHolder.

		<BR> MT - latched, page is latched when this methods is called.

		@param slot is the slot number
		@param row filled in row

		@exception StandardException	Standard Cloudscape error policy
	*/
	protected abstract void restorePortionLongColumn(OverflowInputStream fetchStream)
		throws StandardException, IOException;

	/**
		Create a new record identifier.

		<BR> MT - latched, page is latched when this methods is called.

		@exception StandardException	Standard Cloudscape error policy
	*/
	public abstract int newRecordId() throws StandardException;

	/**
		Create a new record identifier, and bump to next recordid.

		<BR> MT - latched, page is latched when this methods is called.

		@exception StandardException	Standard Cloudscape error policy
	*/
	public abstract int newRecordIdAndBump() throws StandardException;

	/**
		Create a new record identifier, the passed in one is the last one created.
		Use this method to collect and reserve multiple recordIds in one
		stroke.  Given the same input recordId, the subclass MUST return the
		same recordId every time.

		<BR> MT - latched, page is latched when this methods is called.

		@exception StandardException	Standard Cloudscape error policy
	*/
	protected abstract int newRecordId(int recordId) throws StandardException;

	/**
		Is there space for copying this many rows which takes this many bytes
		on the page

		<BR> MT - latched, page is latched when this methods is called.

		@exception StandardException Standard Cloudscape policy.
	*/
	public abstract boolean spaceForCopy(int num_rows, int[] spaceNeeded)
		 throws StandardException;


	/**
		Return the total number of bytes used, reserved, or wasted by the
		record at this slot.

		<BR> MT - latched, page is latched when this methods is called.

		@exception StandardException Standard Cloudscape policy.
	*/
	public abstract int getTotalSpace(int slot) throws StandardException;

	/**
		Return the total number of bytes reserved by the
		record at this slot.

		<BR> MT - latched, page is latched when this methods is called.

		@exception IOException Thrown by InputStream methods potential I/O errors
	*/
	public abstract int getReservedCount(int slot) throws IOException;

	/*
	** Methods that our super-class (BasePage) requires we implement.
	** Here we only implement the methods that correspond to the logical
	** operations that require logging, any other methods that are storage
	** specific we leave to our sub-class.
	**
	** All operations that are logged must bump this page's version number
	** and update this page's last log instant.  
	** These should be sanity checked on each logAndDo (similarly, it should
	** be checked in CompensationOperation.doMe)
	*/


	/*
	** Methods that any sub-class must implement. These allow generic log operations.
	*/

	/**
		Get the stored length of a record. This must match the amount of data
		written by logColumn and logField.

		<BR> MT - latched - page latch must be held
	*/

	public abstract int getRecordLength(int slot) throws IOException;

	/**
		Restore a storable row from a InputStream that was used to
		store the row after a logRecord call.

		<BR> MT - latched - page latch must be held

		@exception StandardException	Standard Cloudscape error policy
		@exception IOException object exceeds the available data in the stream.

	*/
	public abstract void restoreRecordFromStream(
    LimitObjectInput        in, 
    Object[]   row) 
		 throws StandardException, IOException;

	/**
		Log a currently stored record to the output stream.
		The logged version of the record must be readable by storeRecord.

		<BR> MT - latched - page latch must be held


		@param slot		Slot number the record is stored in.
		@param flag		LOG_RECORD_*, the reason for logging the record.
		@param recordId Record identifier of the record.
		@param validColumns which columns needs to be logged
		@param out		Where to write the logged form.
		@param headRowHandle	the recordHandle of the head row piece, used
						for post commit cleanup for update. 

		@exception StandardException	Standard Cloudscape error policy
	*/
	public abstract void logRecord(int slot, int flag, int recordId,
								   FormatableBitSet validColumns, OutputStream out,
								   RecordHandle headRowHandle)
		throws StandardException, IOException;


	/**
		Log the row that will be stored at the given slot to the given OutputStream.
		The logged form of the Row must be readable by storeRecord.

		<BR> MT - latched - page latch must be held

		@param slot				Slot number the record will be stored in.
		@param forInsert		True if the row is being logged for an insert,
								false for an update.
		@param recordId			Record identifier of the record.
		@param row				The row version of the record.
		@param validColumns		FormatableBitSet of which columns in row are valid,
								null indicates all are valid
		@param out				Where to write the logged form.
		@param startColumn		The first column that is being logged in this row.
								This is used when logging portion of long rows.
		@param insertFlag		To indicate whether the insert would allow overflow.
		@param realStartColumn	This is used when a long column is detected.
								Portion of the row may already be logged and stored
								in the 'out' buffer.  After we log the long column,
								the saved buffer was passed here, and we need to
								continue to log the row.  realStartColumn is the starting
								column for the continuation of the logRow operation.
								Pass in (-1) if realStartColumn is not significant.
		@param realSpaceOnPage	Being used in conjunction with realStartColumn,
								to indicate the real free space left on the page.

		@exception StandardException	Standard Cloudscape error policy
	*/
	public abstract int logRow(
    int                     slot, 
    boolean                 forInsert, 
    int                     recordId,
    Object[]                row, 
    FormatableBitSet                 validColumns,
    DynamicByteArrayOutputStream  out, 
    int                     startColumn, 
    byte                    insertFlag,
    int                     realStartColumn, 
    int                     realSpaceOnPage, 
    int                     overflowThreshold)
		throws StandardException, IOException;

	/**
		Log a currently stored field.
		The logged version of the field must be readable by storeField.

		<BR> MT - latched - page latch must be held

		@param slot		Slot number the record is stored in.
		@param fieldNumber Number of the field (starts at 0).
		@param out		Where to write the logged form.

		@exception StandardException	Standard Cloudscape error policy
	*/
	public abstract void logField(int slot, int fieldNumber, OutputStream out)
		throws StandardException, IOException;
	/**
		Log a to be stored column.

		<BR> MT - latched - page latch must be held

		@param slot		slot of the current record
		@param fieldId	field number of the column being updated
		@param column column version of the field.
		@param out		Where to write the logged form.

		@exception StandardException	Standard Cloudscape error policy
	*/
	public abstract void logColumn(
    int                     slot, 
    int                     fieldId, 
    Object                  column, 
    DynamicByteArrayOutputStream  out,
    int                     overflowThreshold)
		throws StandardException, IOException;

	/**
		Log a to be stored long column.  return -1 when done.

		<BR> MT - latched - page latch must be held

		@param slot			slot of the current record
		@param recordId		the id of the long column record
		@param column		column version of the field.
		@param out			Where to write the logged form.

		@exception StandardException	Standard Cloudscape error policy
	*/
	public abstract int logLongColumn(
    int                     slot, 
    int                     recordId,
    Object                  column, 
    DynamicByteArrayOutputStream  out)
		throws StandardException, IOException;

	/**
		Read a previously stored record written by logRecord or logRow and store
		it on the data page at the given slot with the given record identifier.
		Any previously stored record must be replaced.

		<BR> MT - latched - page latch must be held

		@exception StandardException Standard Cloudscape error policy
		@exception IOException Thrown by InputStream methods potential I/O errors
		while writing the page
		
	*/
	public abstract void storeRecord(LogInstant instant, int slot, boolean forInsert, ObjectInput in) 
		throws StandardException, IOException;

	/**
		Read a previously stored field written by logField or logColumn and store
		it on the data page at thge given slot with the given record identifier
		and field number. Any previously stored field is replaced.

		<BR> MT - latched - page latch must be held

		@exception StandardException Standard Cloudscape error policy
		@exception IOException Thrown by InputStream methods and potential I/O errors
		while writing the page.
	*/
	public abstract void storeField(LogInstant instant, int slot, 
									   int fieldId, 
									   ObjectInput in)
		throws StandardException, IOException;

	/**
		Reserve the required number of bytes for the record in the specified slot.

		<BR> MT - latched - page latch must be held

		@exception StandardException Standard Cloudscape error policy
		@exception IOException Thrown by InputStream methods and potential I/O errors
		while writing the page.
	*/
	public abstract void reserveSpaceForSlot(LogInstant instant, int slot, int spaceToReserve)
		throws StandardException, IOException;


	/**
		Skip a previously stored field written by logField or logColumn.

		<BR> MT - latched - page latch must be held
		
		@exception StandardException Standard Cloudscape error policy
		@exception IOException Thrown by InputStream methods

	*/
	public abstract void skipField(ObjectInput in)
		 throws StandardException, IOException;

	public abstract void skipRecord(ObjectInput in) throws StandardException, IOException;

	/**
		Set the delete status of a record from the page.

		<BR> MT - latched - page latch must be held

		@param slot the slot to delete or undelete
		@param delete set delete status to this value 

		@exception StandardException Standard Cloudscape error policy
		@exception IOException IO error accessing page
	*/
	public abstract void setDeleteStatus(LogInstant instant, int slot, boolean delete)
		 throws StandardException, IOException;

	/**
		Purge a record from the page.

		<BR> MT - latched - page latch must be held

		@param slot the slot to purge
		@param recordId the id of the record that is to be purged

		@exception StandardException Standard Cloudscape error policy
		@exception IOException Thrown by potential I/O errors
		while writing the page.
	*/
	public abstract void purgeRecord(LogInstant instant, int slot, 
										int recordId) 
		throws StandardException, IOException;

	/**
		Subclass implementation of compactRecord.
		@see BasePage#compactRecord
		@exception StandardException Standard Cloudscape error policy
	 */
	protected abstract void compactRecord(RawTransaction t, int slot, int recordId)
		 throws StandardException;

	/**
		Set the page status underneath a log record

		<BR> MT - latched - page latch must be held

		@param instant the log instant of the log record
		@param status the page status

		@exception StandardException Standard Cloudscape error policy
	*/
	public abstract void setPageStatus(LogInstant instant, byte status)
		throws StandardException;


	/**
		initialize a page for the first time or for reuse

		All subtypes are expected to overwrite this method if it has something to clean up

		@exception StandardException Standard Cloudscape error policy
	*/
	public abstract void initPage(LogInstant instant, byte status, 
								  int recordId, boolean overflow, boolean reuse)
		 throws StandardException;

	/**
		Set the reserved space for this row to value.
		@exception StandardException Standard Cloudscape error policy
	*/
	public abstract void setReservedSpace(LogInstant instant, int slot, int value) 
		 throws StandardException, IOException;

	/**
		Return true if the page is an overflow page, false if not.
		For implementation that don't have overflow pages, return false.
	*/
	public abstract boolean isOverflowPage();

	/**
		Returns false if an insert is not to be allowed in the page.
	*/
	public abstract boolean allowInsert();

	/**
		Returns true if an insert is allowed in the page and the page is
		relatively unfilled - let specific implementation decide what
		relatively unfilled means
	*/
	public abstract boolean unfilled();

	/**
		Set the number of rows in the container - the page uses this to decide
		whether it needs to aggressive set the container's row count when it
		changes. 
	 */
	public abstract void setContainerRowCount(long count);

												 
	/*
	** Debugging methods
	*/

	/** Debugging, print slot table information */
	protected String slotTableToString()
	{
		String str = null;

		if (SanityManager.DEBUG)
		{
            StoredRecordHeader rh;
			str = new String();

            for (int slot = FIRST_SLOT_NUMBER; slot < recordCount; slot++) {
                rh = getHeaderAtSlot(slot);
                if (rh != null)
                    str += "Slot " + slot + " recordId " + rh.getId();
                else
                    str += "Slot " + slot + " null record";
                str += "\n";
			}
		}
		return str;
	}

	/**
		This lockable wants to participate in the Virtual Lock table.
	 */
	public boolean lockAttributes(int flag, Hashtable attributes)
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(attributes != null, 
				"cannot call lockProperties with null attribute list");
		}

		if ((flag & VirtualLockTable.LATCH) == 0)
			return false;

		// by the time this is called, the page may be unlatched.
		PageKey pageId = identity;

		// not latched
		if (pageId == null)
			return false;

		attributes.put(VirtualLockTable.CONTAINERID, 
					   new Long(pageId.getContainerId().getContainerId()));
		attributes.put(VirtualLockTable.LOCKNAME, pageId.toString());
		attributes.put(VirtualLockTable.LOCKTYPE, "LATCH");

		// don't new unecesary things for now
		// attributes.put(VirtualLockTable.SEGMENTID, new Long(pageId.getContainerId().getSegmentId()));
		// attributes.put(VirtualLockTable.PAGENUM, new Long(pageId.getPageNumber()));

		return true;
	}
		

}
