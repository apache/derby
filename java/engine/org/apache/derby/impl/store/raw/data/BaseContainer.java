/*

   Derby - Class org.apache.derby.impl.store.raw.data.BaseContainer

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
import org.apache.derby.iapi.services.locks.Lockable;
import org.apache.derby.iapi.services.locks.Latch;
import org.apache.derby.iapi.services.locks.C_LockFactory;
import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.iapi.store.access.SpaceInfo;

import org.apache.derby.iapi.store.raw.ContainerHandle;
import org.apache.derby.iapi.store.raw.LockingPolicy;
import org.apache.derby.iapi.store.raw.Page;
import org.apache.derby.iapi.store.raw.PageKey;
import org.apache.derby.iapi.store.raw.PageTimeStamp;
import org.apache.derby.iapi.store.raw.RecordHandle;
import org.apache.derby.iapi.store.raw.Transaction;
import org.apache.derby.iapi.store.raw.ContainerKey;
import org.apache.derby.iapi.store.raw.data.RawContainerHandle;
import org.apache.derby.iapi.store.raw.log.LogInstant;
import org.apache.derby.iapi.store.raw.xact.RawTransaction;

import org.apache.derby.iapi.util.ByteArray;

import java.util.Properties;
import java.util.Hashtable;

/**
	BaseContainer is an abstract class that provides the locking bahaviour
	for an object representing an active container, that is the actual
	storage container, not the ContainerHandle interface. This class is designed
	so that it can change the container it represents to avoid creating
	a new object for every container.
	<P>
	This object implements lockable to provide an object to lock while a page is being
	allocated.
	<BR> MT - Mutable - mutable identity : 
*/
public abstract class BaseContainer implements Lockable {

	/**
		Identity of the container.

		<BR> MT - Mutable
	*/
	protected ContainerKey identity;

	
	/**
		Dropped state of the container.

		<BR> MT - mutable : single thread required. Either the container must be exclusive
		locked by this thread, or the container must have no identity (ie. it is being created
		or opened).
	*/
	protected boolean	isDropped;


	/**
		Committed Drop state of the container.  If a post comit action
		determined that the drop container operation is committed, the whole
		container may be removed and space reclaimed.

		<BR> MT - mutable : single thread required. Either the container must be exclusive
		locked by this thread, or the container must have no identity (ie. it is being created
		or opened).
	*/
	protected boolean isCommittedDrop;


	/**
		Is reusable recordId.  By default, record Ids are not reusable when a
		page is reused.  However, under special circumstances, clients to raw
		store may decide that record Ids may be reused after the page is
		reused.   When this flag is set, pages that are reused will have its
		next recordId set to RecordHandle.FIRST_RECORD_ID
	*/
	protected boolean isReusableRecordId = false;

	protected BaseContainer() {
	}

	/*
	** portions of Cacheable interface, interface is actually implemented by
	** sub-class. This section also contains methods related to this interface.
	*/

	protected void fillInIdentity(ContainerKey key) {

		if (SanityManager.DEBUG) {
			SanityManager.ASSERT(identity == null || (identity == key));
		}

		identity = key;
	}

	public void clearIdentity() {
		if (SanityManager.DEBUG) {
			SanityManager.ASSERT(identity != null);
		}

		identity = null;
	}

	public Object getIdentity() {
		return identity;
	}

	/*
	** Methods from Lockable, just require a single exclusive locker
	*/

	public void lockEvent(Latch lockInfo) {
		if (SanityManager.DEBUG) {
			SanityManager.ASSERT(identity != null);
		}
	}

	public boolean requestCompatible(Object requestedQualifier, Object grantedQualifier) {
		if (SanityManager.DEBUG) {
			SanityManager.ASSERT(identity != null);
		}
		return false;
	}

	public boolean lockerAlwaysCompatible() {
		if (SanityManager.DEBUG) {
			SanityManager.ASSERT(identity != null);
		}
		return false;
	}

	public void unlockEvent(Latch lockInfo) {
		if (SanityManager.DEBUG) {
			SanityManager.ASSERT(identity != null);
		}
	}

	/*
	** Implementation specific methods
	*/


	/**
		Add a page to this container.

		<BR> MT - thread aware - 

		The add page operation involves 2 transactions, one is the user
		transaction (the transaction which owns the passed in handle), the
		other one is a NestedTopTransaction created by this BaseContainer.

		The nestedTopTransaction is used by the underlying container to change
		high contention structures, such as link list anchor or bit map pages.
		The nestedTopTransaction commits or aborts before this routine returns.

		The user transaction is used to latch the newly created page.

		@exception StandardException Standard Cloudscape error policy
	*/
	public Page addPage(BaseContainerHandle handle, boolean isOverflow) throws StandardException {
		
		RawTransaction ntt = handle.getTransaction().startNestedTopTransaction();

		int mode = handle.getMode(); 

		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT((mode & ContainerHandle.MODE_FORUPDATE) ==
								 ContainerHandle.MODE_FORUPDATE, 
								 "addPage handle not for update");
		}

		// if we are not in the same transaction as the one which created the
		// container and the container may have logged some operation already, 
		// then we need to log allocation regardless of whether user changes
		// are logged.  Otherwise, the database will be corrupted if it
		// crashed. 
		if ((mode & ContainerHandle.MODE_CREATE_UNLOGGED) == 0 &&
			(mode & ContainerHandle.MODE_UNLOGGED) ==
						ContainerHandle.MODE_UNLOGGED) 
			mode &= ~ContainerHandle.MODE_UNLOGGED;

		// make a handle which is tied to the ntt, not to the user transaction this
		// handle is tied to.  The container is already locked by the user transaction,
		// open it nolock
		BaseContainerHandle allocHandle = (BaseContainerHandle)ntt.openContainer
			(identity, (LockingPolicy)null, mode);

		if (allocHandle == null)
        {
			throw StandardException.newException(
                    SQLState.DATA_ALLOC_NTT_CANT_OPEN, 
                    new Long(getSegmentId()), 
                    new Long(getContainerId()));
        }

		// Latch this container, the commit will release the latch
		ntt.getLockFactory().lockObject(
                ntt, ntt, this, null, C_LockFactory.WAIT_FOREVER);

		BasePage newPage = null;
		try
		{
			newPage = newPage(handle, ntt, allocHandle, isOverflow);
		}
		finally
		{
			if (newPage != null)
            {
                // it is ok to commit without syncing, as it is ok if this
                // transaction never makes it to the db, if no subsequent
                // log record makes it to the log.  If any subsequent log
                // record is sync'd then this transaction will be sync'd
                // as well.
				ntt.commitNoSync(Transaction.RELEASE_LOCKS);
            }
			else
            {      
				ntt.abort();
            }
			ntt.close();
		}

		if (SanityManager.DEBUG) {
			SanityManager.ASSERT(newPage.isLatched());
		}

		if (!this.identity.equals(newPage.getPageId().getContainerId())) {

			if (SanityManager.DEBUG) {
				SanityManager.THROWASSERT("BaseContainer.addPage(), just got a new page from a different container"
					+ "\n this.identity = " + this.identity
					+ "\n newPage.getPageId().getContainerId() = " + newPage.getPageId().getContainerId()
					+ "\n handle is: " + handle
					+ "\n allocHandle is: " + allocHandle
					+ "\n this container is: " + this);
			}

			throw StandardException.newException(
                    SQLState.DATA_DIFFERENT_CONTAINER,
                    this.identity, newPage.getPageId().getContainerId());
		}

		return newPage;
	}

    /**
     * Request the system properties associated with a container.
     * <p>
     * Request the value of properties that are associated with a container.  
     * The following properties can be requested:
     *     derby.storage.pageSize 
     *     derby.storage.pageReservedSpace
     *     derby.storage.minimumRecordSize
     * <p>
     * To get the value of a particular property add it to the property list,
     * and on return the value of the property will be set to it's current 
     * value.  For example:
     *
     * get_prop(BaseContainer base)
     * {
     *     Properties prop = new Properties();
     *     prop.put("derby.storage.pageSize", "");
     *     base.getContainerProperties(prop);
     *
     *     System.out.println(
     *         "container's page size = " + 
     *         prop.getProperty("derby.storage.pageSize");
     * }
     *
     * @param prop   Property list to fill in.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public abstract void getContainerProperties(Properties prop)
		throws StandardException;

	/**
		Remove a page from this container.  The page will be unlatched by this
		routine before it returns.

		Unlike addPage, this method done as part of the user transaction.  
		The removed page is not usable by anyone until the user transaction 
        comits.
		If the user transaction rolls back, the removed page is un-removed.

		<BR> MT - thread aware -

		@param handle the container handle that has opened the container and latched the page
		@param page the latched page that is to be deallocated

		@exception StandardException Standard Cloudscape error policy
	*/
	protected void removePage(BaseContainerHandle handle, BasePage page) 
		 throws StandardException
	{
		try
		{
			if (SanityManager.DEBUG)
			{
				SanityManager.ASSERT(page.isLatched(), "page is not latched");
			}

			// get dealloc lock nowait on the page to be deallocated
			// this lock is held until this transaction commits.
			// then gc can free this page
			RecordHandle deallocLock = 
				page.makeRecordHandle(RecordHandle.DEALLOCATE_PROTECTION_HANDLE);

			// don't get deallocLock wait because caller have a page latched
			if (!getDeallocLock(handle, deallocLock, 
								false /* no wait */,
								false /* not zeroDuration */))
            {
				throw StandardException.newException(
                        SQLState.DATA_CANNOT_GET_DEALLOC_LOCK, 
                        page.getIdentity());
            }

			deallocatePage(handle, page);
		}
		finally
		{
			if (page != null)
				page.unlatch();
		}

	}

	/**
		Get the special dealloc lock on the page - the lock is gotten by the
		transaction that owns the container handle

		@exception StandardException Standard Cloudscape error policy
	*/
	protected boolean getDeallocLock(BaseContainerHandle handle, 
									 RecordHandle deallocLock, 
									 boolean wait,
									 boolean zeroDuration)
		 throws StandardException
	{
		// get deallocate lock on page so that the GC won't attempt to 
		// free and re-allocate it until the transaction commits
		RawTransaction tran = handle.getTransaction();

		LockingPolicy lp = 
            tran.newLockingPolicy(
                LockingPolicy.MODE_RECORD,
                TransactionController.ISOLATION_REPEATABLE_READ, 
                true); // striterOK
		
		PageKey pkey = new PageKey(identity, deallocLock.getPageNumber());
		if (lp != null)
        {
			if (zeroDuration)
				return lp.zeroDurationLockRecordForWrite(
                        tran, deallocLock, false, wait); 
			else
				return lp.lockRecordForWrite(tran, deallocLock, false, wait);
        }
		else
		{
			throw StandardException.newException(
                    SQLState.DATA_CANNOT_GET_DEALLOC_LOCK, pkey);
		}
	}


	/**
		Get an allocation page and latch it.
		@exception StandardException Standard Cloudscape error policy
	*/
	protected Page getAllocPage(BaseContainerHandle handle, long pageNumber, boolean wait)
		 throws StandardException
	{
		return latchPage(handle, getAllocPage(pageNumber), wait);
	}

	/**
		Get any page and latch it .
		@exception StandardException Standard Cloudscape error policy
	*/
	protected Page getAnyPage(BaseContainerHandle handle, long pageNumber, boolean wait)
		 throws StandardException
	{
		return latchPage(handle, getAnyPage(handle, pageNumber), wait);
	}


	/**
		Get the first valid page. Result is latched.
		@exception StandardException Standard Cloudscape error policy
	*/
	protected Page getFirstPage(BaseContainerHandle handle) throws StandardException
	{
		return getFirstHeadPage(handle, true /* wait */);
	}

	/**
		Get the next valid page and latch it
		@exception StandardException Standard Cloudscape error policy
	*/
	protected Page getNextPage(BaseContainerHandle handle, long pageNumber)
        throws StandardException
	{
		return getNextHeadPage(handle, pageNumber, true /* wait */);
	}

	/*
		utility to latch a page
	*/
	protected BasePage latchPage(BaseContainerHandle handle, BasePage foundPage, boolean wait)
		 throws StandardException
	{
		if (foundPage != null) {
			 if (wait) {
				foundPage.setExclusive(handle);
			 } else {
				 if (!foundPage.setExclusiveNoWait(handle))
				 {
					 // sub-class will release page from the cache if required.
					 return null;
				 }
			 }
		}

		if (SanityManager.DEBUG) {
			SanityManager.ASSERT((foundPage == null) || foundPage.isLatched());
		}

		return foundPage;

	}


	/**
		Lock the container and mark the container as in-use by this container handle.

		@param droppedOK if true, use this container even if it is dropped.,
		@return true if the container can be used, false if it has been dropped
		since the lock was requested and droppedOK is not true.

		@exception StandardException I cannot be opened for update.
	*/
	protected boolean use(BaseContainerHandle handle, boolean forUpdate,
						  boolean droppedOK) 
		throws StandardException {

		// see if the container can be updated
		if (forUpdate && !canUpdate())
        {
			throw StandardException.newException(
                    SQLState.DATA_CONTAINER_READ_ONLY);
        }

		// if the container is dropped, cannot see if unless droppedOK is set
		if (!droppedOK && (getDroppedState() || getCommittedDropState())) {
			return false;
		}

		return true;
	}

	/**
		Discontinue use of this container. Note that the unlockContainer
		call made from this method may not release any locks. The container
		lock may be held until the end of the transaction.

	*/
	protected void letGo(BaseContainerHandle handle) {

		RawTransaction t = handle.getTransaction();

		handle.getLockingPolicy().unlockContainer(t, handle);
	}

	protected boolean getDroppedState() {
		return isDropped;
	}

	protected boolean getCommittedDropState()
	{
		return isCommittedDrop;
	}


	protected boolean isReusableRecordId()
	{
		return isReusableRecordId;
	}

	public int getContainerStatus()
	{
		if (getCommittedDropState())
			return RawContainerHandle.COMMITTED_DROP;

		if (getDroppedState())
			return RawContainerHandle.DROPPED;

		return RawContainerHandle.NORMAL;
	}

	public long getContainerId() {
		return identity.getContainerId();
	}

	public long getSegmentId() {
		return identity.getSegmentId();
	}


	//public int getPageSize() {
	//	return pageSize();
	//}

	/*
	**	Methods that need to be provided by a sub-class.
	*/

    /**
    Get information about space used by the container.
    **/
    protected abstract SpaceInfo getSpaceInfo(BaseContainerHandle handle)
        throws StandardException;

	/**
		Can the container be updated.

		@return true if the container can be updated, false otherwise.
	*/
	protected abstract boolean canUpdate();

	/**
		The container is about to be modified.
		Loggable actions use this to make sure the container gets cleaned if a
		checkpoint is taken after any log record is sent to the log stream but
		before the container is actually dirtied.
	 */
	protected abstract void preDirty(boolean preDirtyOn);


	/**
		Return a BasePage that represents the given page number in this container.
        The resulting page is latched.

		@exception StandardException Standard Cloudscape error policy
	*/
	protected abstract BasePage getPage(BaseContainerHandle handle, long pageNumber,
        boolean wait) throws StandardException;

	/**
		Return a BasePage that represents the given alloc page number in this container.

		@exception StandardException Standard Cloudscape error policy
	*/
	protected abstract BasePage getAllocPage(long pageNumber) throws StandardException;

	/**
		Return a BasePage that represents any page - alloc page, valid page, free page,
		dealloced page etc.  The only requirement is that the page is initialized...

		@exception StandardException Cloudscape Standard error policy
	*/
	protected abstract BasePage getAnyPage(BaseContainerHandle handle, long pageNumber)
		 throws StandardException;

	/**
		ReCreate a page for load tran.  The argument passed in need to be
		sufficient for the page cache to materialize a brand new page and write
		it to disk.  The reason why this method is necessary is because we
		first create the page, write it to disk, and then log the init page log
		record to make sure there are enough space on disk.  During load tran,
		the page creation part is missing.

		<p>Load tran will do no preallocation.

		<p>Only called during recovery redo.

		@exception StandardException Cloudscape Standard error policy
	 */
	protected abstract BasePage
	reCreatePageForLoadTran(BaseContainerHandle handle,
							int pageFormat,
							long pageNumber,
							long pageOffset)
		 throws StandardException;

	/**
		Log all information on the container creation necessary to recreate teh
		container during a load tran.

		@exception StandardException Cloudscape Standard error policy
	 */
	 protected abstract ByteArray logCreateContainerInfo()
		 throws StandardException;


	/**
		Get only a valid, non-overflow page.  If page number is either invalid
		or overflow, returns null

		@exception StandardException Cloudscape Standard error policy
	 */
	protected abstract BasePage getHeadPage(BaseContainerHandle handle,
        long pagenumber, boolean wait) throws StandardException;

	/**
		Get the first page in the container.
		@exception StandardException Standard Cloudscape error policy
	*/
	protected abstract BasePage getFirstHeadPage(BaseContainerHandle handle,
        boolean wait) throws StandardException;

	/**
		Get the next page in the container.
		@exception StandardException Standard Cloudscape error policy
	*/
	protected abstract BasePage getNextHeadPage(BaseContainerHandle handle,
        long pageNumber, boolean wait) throws StandardException;

	/**
		Get a potentially suitable page for insert and latch it.
		@exception StandardException Standard Cloudscape error policy
	 */
	protected abstract BasePage getPageForInsert(BaseContainerHandle handle,
												 int flag)
		 throws StandardException;


	/**
		Create a new page in the container.

		@exception StandardException Standard Cloudscape error policy
	*/
	protected abstract BasePage newPage(BaseContainerHandle userhandle,
										RawTransaction t,
										BaseContainerHandle allocHandle,
										boolean isOverflow) throws StandardException;


	/**
		Deallocate a page from the container.

		@exception StandardException Standard Cloudscape error policy
	*/
	protected abstract void deallocatePage(BaseContainerHandle userhandle,
										   BasePage page) throws StandardException;


	protected void truncate(BaseContainerHandle handle) throws StandardException {
		if (SanityManager.DEBUG) {
			SanityManager.THROWASSERT("truncate not supported");
		}
	}

	/**
		Mark the container as drop or not drop depending on the input value.

	*/
	protected abstract void dropContainer(LogInstant instant, boolean drop);


	/**
		Remove the container and reclaim its space.  Once executed, this
		operation cannot be undone - as opposed to dropContainer which only
		marks the container as dropped and can be rolled back.
		<BR><B> This operation should only be called by post commit clean up </B>

		@param leaveStub if true, leave a stub.  If false, remove everything
		@see org.apache.derby.iapi.store.raw.data.RawContainerHandle#removeContainer

		@exception StandardException Standard Cloudscape error policy
	*/
	protected abstract void removeContainer(LogInstant instant, boolean leaveStub) throws StandardException;

	/**
		Get the logged container version.

		@exception StandardException Standard Cloudscape error policy
	*/
	protected abstract long getContainerVersion() throws StandardException;

	/**
		Flush all outstanding changes in this container to persistent storage.

		@exception StandardException Standard Cloudscape error policy
	*/
	protected abstract void flushAll() throws StandardException;

	/**
		The container will be grown vastly, prepare for it.
	*/
	protected abstract void prepareForBulkLoad(BaseContainerHandle handle,
											   int numPage);

	/**
		The container will have no pre-allocate threshold, i.e., if the
		implementation supports it, page preallocation will happen
		the next time a new page is allocated.
	*/
	protected abstract void clearPreallocThreshold();

	/*
		Cost estimates
	*/
	/**
		@see ContainerHandle#getEstimatedRowCount
		@exception StandardException Standard Cloudscape error policy
	 */
	public abstract long getEstimatedRowCount(int flag) throws StandardException;

	/**
		@see ContainerHandle#setEstimatedRowCount
		@exception StandardException Standard Cloudscape error policy
	 */
	public abstract void setEstimatedRowCount(long count, int flag) throws StandardException;

	/**
		@see ContainerHandle#getEstimatedPageCount
		@exception StandardException Standard Cloudscape error policy
	 */
	public abstract long getEstimatedPageCount(BaseContainerHandle handle, int flag) throws StandardException;


	/*
	** Methods to be used by sub-classes.
	*/

	/**
		Set the container's dropped state
	*/
	protected void setDroppedState(boolean isDropped) {
		this.isDropped = isDropped;
	}

	protected void setCommittedDropState(boolean isCommittedDrop)
	{
		this.isCommittedDrop = isCommittedDrop;
	}


	protected void setReusableRecordIdState(boolean isReusableRecordId)
	{
		this.isReusableRecordId = isReusableRecordId;
	}

	//protected void setPageSize(int pageSize) {
	//	identity.setPageSize(pageSize);
	//}

	// Not interested in participating in the diagnostic virtual lock table.
	public boolean lockAttributes(int flag, Hashtable attributes)
	{
		return false;
	}

}

