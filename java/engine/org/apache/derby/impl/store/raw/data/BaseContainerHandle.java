/*

   Derby - Class org.apache.derby.impl.store.raw.data.BaseContainerHandle

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
import org.apache.derby.iapi.services.locks.VirtualLockTable;
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.store.access.SpaceInfo;
import org.apache.derby.iapi.store.raw.ContainerHandle;
import org.apache.derby.iapi.store.raw.ContainerLock;
import org.apache.derby.iapi.store.raw.LockingPolicy;
import org.apache.derby.iapi.store.raw.Page;
import org.apache.derby.iapi.store.raw.PageKey;
import org.apache.derby.iapi.store.raw.PageTimeStamp;
import org.apache.derby.iapi.store.raw.RecordHandle;
import org.apache.derby.iapi.store.raw.ContainerKey;

import org.apache.derby.iapi.store.raw.data.RawContainerHandle;
import org.apache.derby.iapi.store.raw.xact.RawTransaction;
import org.apache.derby.iapi.store.raw.log.LogInstant;

import org.apache.derby.iapi.util.ByteArray;
import org.apache.derby.catalog.UUID;

import java.util.Hashtable;
import java.util.Observable;
import java.util.Observer;
import java.util.Properties;

/**
	A handle to an open container, implememts RawContainerHandle.
	<P>
	This class is an Observer to observe RawTransactions
	and is also a Observable to
	handle the list of pages accessed thorough this handle.
	<BR>
	This class implements Lockable (defined to be ContainerHandle) and is
	the object used to logically lock the container.

	<BR> MT - Mutable - Immutable identity - Thread Aware
*/

public class BaseContainerHandle extends Observable 
    implements RawContainerHandle, Observer 
{

	/*
	** Fields
	*/

	/**
		Container identifier
		<BR> MT - Immutable
	*/
	protected /*final*/ ContainerKey		identity;

	/**
		Is this ContainerHandle active.

		<BR> MT - Mutable : scoped
	*/
	protected boolean				active;	

	/**
		The actual container we are accessing. Only valid when active is true.

		<BR> MT - Mutable : scoped
	*/
	protected BaseContainer		container;

	/**
		the locking policy we opened the container with. 
        Only valid when active is true.

		<BR> MT - Mutable : scoped
	*/

	private	LockingPolicy		locking;

	/**
		our transaction. Only valid when active is true.

		<BR> MT - Mutable : scoped
	*/
	protected	RawTransaction		xact;

	/**
		are we going to update?

		<BR> MT - Immutable after container handle becomes active
	*/
	private	boolean		forUpdate;

	protected int mode;	// mode the container was opened in


	protected PageActions		actionsSet;
	protected AllocationActions allocActionsSet;


	/*
	** Constructor
	*/

	/**
		Create an object that is only used for locking the container.
	*/
	public BaseContainerHandle(UUID rawStoreId, RawTransaction xact,
		ContainerKey identity, LockingPolicy locking, int mode) 
    {
		this.identity = identity;
		this.xact = xact;
		this.locking = locking;
		this.mode = mode;
		this.forUpdate = (mode & MODE_FORUPDATE) == MODE_FORUPDATE;
	}

	/**
		Create a container handle that is used to actually access the container.
	*/
	public BaseContainerHandle(
    UUID                rawStoreId, 
    RawTransaction      xact,
    PageActions         actionsSet, 
    AllocationActions   allocActionsSet, 
    LockingPolicy       locking,
	BaseContainer       container, 
    int                 mode)
	{
		this(rawStoreId, xact, 
                (ContainerKey) container.getIdentity(), locking, mode);


		this.actionsSet      = actionsSet;
		this.allocActionsSet = allocActionsSet;
		this.container       = container;

		// we are inactive until useContainer is called.
	}

	/*
	** Methods from ContainerHandle
	*/

	/**
		Add a page to the container
		The page returned will be observing me.

		@see BaseContainer#addPage
		@see ContainerHandle#addPage
		@exception StandardException Standard Cloudscape error policy
	*/
	public Page addPage() throws StandardException 
    {
		checkUpdateOpen();

		Page page = container.addPage(this, false /* not an overflow page */);
			
		return page;
	}

	/**
		Add a page to the container, if flag == ContainerHandle.ADD_PAGE_BULK,
		tell the container about it.

		The page returned will be observing me.

		@see BaseContainer#addPage
		@see ContainerHandle#addPage
		@exception StandardException Standard Cloudscape error policy
	*/
	public Page addPage(int flag) throws StandardException {

		if ((flag & ContainerHandle.ADD_PAGE_BULK) != 0 && active && forUpdate)
		{
			// start preallocating immediatelly, don't wait for the
			// preallocation threshold to be crossed.  Don't go wild and
			// preallocate a bunch of pages either, use preAllocate for that. 
			container.clearPreallocThreshold();
		}

		return addPage();
	}

	/**
		Preallocate numPage if possible.
	*/
	public void preAllocate(int numPage)
	{
		if (numPage > 0 && active && forUpdate)
			container.prepareForBulkLoad(this, numPage);
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
     * get_prop(BaseContainerHandle ch)
     * {
     *     Properties prop = new Properties();
     *     prop.put("derby.storage.pageSize", "");
     *     ch.getContainerProperties(prop);
     *
     *     System.out.println(
     *         "conatainer's page size = " + 
     *         prop.getProperty("derby.storage.pageSize");
     * }
     *
     * @param prop   Property list to fill in.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public void getContainerProperties(Properties prop)
		throws StandardException
    {
		checkOpen();

        container.getContainerProperties(prop);

        return;
    }

	/**
		Remove a page from the container.  

		@see ContainerHandle#removePage
		@exception StandardException Standard Cloudscape error policy
	*/
	public void removePage(Page page) throws StandardException
	{
		if (!active)
		{
			if (page != null)
				page.unlatch();
			throw StandardException.newException(
                    SQLState.DATA_CONTAINER_CLOSED);
		}

		if (!forUpdate)
		{
			if (page != null)
				page.unlatch();
			throw StandardException.newException(
                    SQLState.DATA_CONTAINER_READ_ONLY);
		}

		container.removePage(this, (BasePage)page);
	}

	public Page getPage(long pageNumber) throws StandardException 
    {

		checkOpen();

		return container.getPage(this, pageNumber, true);
	}


	public Page getAllocPage(long pageNumber) throws StandardException 
    {
		checkOpen();

		return container.getAllocPage(this, pageNumber, true);
	}

	public Page getUserPageNoWait(long pageNumber) 
        throws StandardException 
    {
		checkOpen();

		return container.getHeadPage(this, pageNumber, false);
	}
	public Page getUserPageWait(long pageNumber) 
        throws StandardException 
    {
		checkOpen();

		return container.getHeadPage(this, pageNumber, true);
	}

	public Page getPageNoWait(long pageNumber) 
        throws StandardException 
    {
		checkOpen();

		return container.getPage(this, pageNumber, false);
	}

	public Page getFirstPage() throws StandardException 
    {
		checkOpen();

		return container.getFirstPage(this);
	}

	public Page getNextPage(long pageNumber) throws StandardException 
    {
		checkOpen();

		return container.getNextPage(this, pageNumber);
	}

	public Page getPageForInsert(int flag) 
		 throws StandardException
	{
		checkUpdateOpen();

		return container.getPageForInsert(this, flag);
	}

	/**
		@see ContainerHandle#isReadOnly()
	*/
    public final boolean isReadOnly()
    {
        return(!forUpdate);
    }

	/**
		@see ContainerHandle#close
	*/

	public void close() 
    {

        if (xact == null) 
        {
            // Probably be closed explicitly by a client, after closing 
            // automatically after an abort.
            
            if (SanityManager.DEBUG)
                SanityManager.ASSERT(!active);

            return;
        }

		// notify our observers (Pages) that we are closing ...
		informObservers();

		active = false;

		getLockingPolicy().unlockContainer(xact, this);

		// let go of the container
		if (container != null) 
        {
			container.letGo(this);
			container = null;
		}

		// and remove ourseleves from this transaction
        xact.deleteObserver(this);

		xact = null;
		
	}

	/* cost estimation */

	/**
		@see ContainerHandle#getEstimatedRowCount
		@exception StandardException Standard Cloudscape error policy
	 */
	public long getEstimatedRowCount(int flag) throws StandardException
	{
		checkOpen();

		return container.getEstimatedRowCount(flag);
	}

	/**
		@see ContainerHandle#setEstimatedRowCount
		@exception StandardException Standard Cloudscape error policy
	 */
	public void setEstimatedRowCount(long count, int flag) 
        throws StandardException
	{
		checkOpen();

		container.setEstimatedRowCount(count, flag);
	}

	/**
		@see ContainerHandle#getEstimatedPageCount
		@exception StandardException Standard Cloudscape error policy
	 */
	public long getEstimatedPageCount(int flag) 
        throws StandardException
	{
		checkOpen();

		return container.getEstimatedPageCount(this, flag);
	}

	/**
		@see ContainerHandle#flushContainer
		@exception StandardException Standard Cloudscape error policy
	 */
	public void flushContainer() 
        throws StandardException
	{
		checkUpdateOpen();

		// raw store may override unlog mode when log is Archived.
		// if ((mode & MODE_CREATE_UNLOGGED) == 0)
		//	throw StandardException.newException(
        //	    SQLState.DATA_NOT_CREATE_UNLOGGED, identity);

		container.flushAll();

	}

	/**
		@see ContainerHandle#compactRecord
		@exception StandardException Standard Cloudscape error policy
	 */
	public void compactRecord(RecordHandle record) 
        throws StandardException
	{
		if (!forUpdate)
        {
			throw StandardException.newException(
                    SQLState.DATA_CONTAINER_READ_ONLY);
        }

		PageKey pkey = (PageKey)record.getPageId();
		BasePage headPage = (BasePage)getPage(pkey.getPageNumber());

		if (headPage != null)
		{
			// The page could have been null if it was deallocated after the
			// row lock is gotten.  We are doing all of these post commit so
			// the record may not even be there and we got a lock for nothing.
			try
			{
				headPage.compactRecord(record);
			}
			finally
			{
				headPage.unlatch();
			}
		}
	}


	/*
	** Methods of RawContainerHandle - methods are called underneath the log
	*/

	/**
		Get the container status.  

		@exception StandardException Standard Cloudscape error policy		
		@see RawContainerHandle#getContainerStatus
	*/
	public int getContainerStatus() throws StandardException
	{
		checkOpen();

		return container.getContainerStatus();
	}

	/**
		remove the container

		@exception StandardException Standard Cloudscape error policy		
		@see RawContainerHandle#removeContainer
	*/
	public void removeContainer(LogInstant instant) throws StandardException
	{
		checkUpdateOpen();

		// This call can only be issued by within rawStore.
		// while the container is dropped, no client of raw store
		// should be able to access the container (it is 
		// exclusively locked).  
		// Then as postcommit processing, 
		// the container iw 

		container.removeContainer(instant, true);
	}

	/**
		@see ContainerHandle#getId
	 */
	public ContainerKey getId()
	{
		return identity;
	}

	/**
		@see ContainerHandle#getUniqueId
	 */
	public Object getUniqueId()
	{
        return(this);
	}


	/**
		@exception StandardException  Standard cloudscape exception policy
		@see RawContainerHandle#dropContainer
	*/
	public void dropContainer(LogInstant instant, boolean drop) 
        throws StandardException
	{
		checkUpdateOpen();

		container.dropContainer(instant, drop);
	}

	/**
		@exception StandardException  Standard cloudscape exception policy
		@see RawContainerHandle#getContainerVersion
	*/
	public long getContainerVersion() 
        throws StandardException
	{
		checkOpen();

		return container.getContainerVersion();
	}


	/**
		Get this page with no check - any page type or status is fine.
		Caller must be prepared to handle freed, deallocated,or alloc page
		Called by recovery ONLY.

		@exception StandardException Cloudscape Standard error policy
	*/
	public Page getAnyPage(long pageNumber) throws StandardException
	{
		checkOpen();

		return container.getAnyPage(this, pageNumber, true /* wait */);
	}

	/**
		Re-create this page for load tran.  Called by recovery redo ONLY
		@exception StandardException Cloudscape Standard error policy
	 */
	public Page reCreatePageForLoadTran(
    int     pageFormat,
    long    pageNumber, 
    long    pageOffset)
		 throws StandardException
	{
		checkUpdateOpen();

		return container.reCreatePageForLoadTran(
					this, pageFormat, pageNumber, pageOffset);
	}

	/**
		Log all information necessary to recreate the container during a load
		tran.

		@exception StandardException Standard Cloudscape error policy
	 */
	public ByteArray logCreateContainerInfo() 
        throws StandardException
	{
		checkUpdateOpen();

		return container.logCreateContainerInfo();
	}
	
	/**
		Return a record handle that is initialized to the given page number and
        record id.

		@exception StandardException Standard cloudscape exception policy.

		@param pageNumber   the page number of the RecordHandle.
		@param recordId     the record id of the RecordHandle.

		@see RecordHandle
	*/
	public RecordHandle makeRecordHandle(long pageNumber, int recordId)
		 throws	StandardException
    {
        return new RecordId(identity, pageNumber, recordId);
    }


	/*
	**	Methods of Observer
	*/

	/**
		Called when the transaction is about to complete.

		@see Observer#update
	*/
	public void update(Observable obj, Object arg) 
    {
		if (SanityManager.DEBUG) 
        {
			if (arg == null)
				SanityManager.THROWASSERT("still on observr list " + this);
		}

		// already been removed from the list
		if (xact == null) 
        {
			return;
		}

		if (SanityManager.DEBUG) 
        {
			// just check reference equality

			if (obj != xact)
            {
				SanityManager.THROWASSERT(
                    "Observable passed to update is incorrect expected " + 
                    xact + " got " + obj);
            }
		}

		// close on a commit, abort or drop of this container.
		if (arg.equals(RawTransaction.COMMIT) || 
            arg.equals(RawTransaction.ABORT)  || 
            arg.equals(identity)) 
        {
			// close the container		
			close();
			return;

		}
		
		if (arg.equals(RawTransaction.SAVEPOINT_ROLLBACK)) 
        {

			// unlatch any pages but remain open
			informObservers();

			// remain open
			return;
		}

		// Transaction is notifying us that our container
		// has undergone some lock escalation. We re-get
		// our table lock which will promote us 
		// if possible
		
		if (arg.equals(RawTransaction.LOCK_ESCALATE)) 
        {

			// only attempt escalation on RowLocking modes.
			if (getLockingPolicy().getMode() != LockingPolicy.MODE_RECORD)
				return;

			try 
            {
				getLockingPolicy().lockContainer(
                    getTransaction(), this, false, forUpdate);
			} 
            catch (StandardException se) 
            {
				xact.setObserverException(se);
			}
		}
	}

	/*
	** Implementation specific methods, these are public so that they can be 
    ** called in other packages that are specific implementations of Data, ie.
	** a directory at the level
	**
	** com.ibm.db2j.impl.Database.Storage.RawStore.Data.*
	*/

	public PageActions getActionSet() 
    {
		return actionsSet;
	}
	
	public AllocationActions getAllocationActionSet() 
    {
		return allocActionsSet;
	}

	/**
		Attach me to a container. If this method returns false then
		I cannot be used anymore, and any reference to me must be discarded.

		@param droppedOK if true, use this container even if it is dropped,
		otherwise, return false if container is dropped.  

		@param waitForLock if true, wait on lock, otherwise, get lock no wait.

		@exception StandardException Standard Cloudscape error policy
	*/
	public boolean useContainer(
    boolean droppedOK, 
    boolean waitForLock) 
        throws StandardException 
    {

		if (SanityManager.DEBUG) 
        {
			SanityManager.ASSERT(!active);
		}

		boolean gotLock = 
			getLockingPolicy().lockContainer(
                getTransaction(), this, waitForLock, forUpdate);

		if (gotLock == false)
		{
			// this is a lockingPolicy error, if waitForLock should either 
			// return true or throw a deadlock exception
			if (SanityManager.DEBUG)
				SanityManager.ASSERT(waitForLock == false, 
					"lockContainer wait returns false");

			container = null;

            throw StandardException.newException(SQLState.LOCK_TIMEOUT);
		}

		if ((mode & ContainerHandle.MODE_OPEN_FOR_LOCK_ONLY) == 0) 
        {

			if (SanityManager.DEBUG) 
            {
				SanityManager.ASSERT(container != null);
			}

			if (!container.use(this, forUpdate, droppedOK)) 
            {

                // If we got a lock, but for some reason we can't open the
                // table (like it doesn't exist), then call unlockContainer().
                // In the normal case it would be called when the container
                // handle was closed, but in this case the user is never going
                // to get an "open" container handle back.  We can't call 
                // close() here as we haven't done all the "open" stuff.
                getLockingPolicy().unlockContainer(xact, this);

				container = null;

				return false;
			}
			active = true;
		} 
        else 
        {
			// lock only, we only observe the transaction if
			// we are performing row level locking.
			if (getLockingPolicy().getMode() != LockingPolicy.MODE_RECORD)
				return true;
		}

		// watch transaction so we will close handle just before xact completes.
		xact.addObserver(this);


		// Add special objects implementing certain behaviour at commit/rollback

		if ((mode & (ContainerHandle.MODE_READONLY | 
                     ContainerHandle.MODE_NO_ACTIONS_ON_COMMIT)) == 0) 
        {
			if ((mode & MODE_TRUNCATE_ON_COMMIT) == MODE_TRUNCATE_ON_COMMIT) 
            {
				xact.addObserver(
                    new TruncateOnCommit(identity, true /* always */));
			} 
            else if ((mode & MODE_TRUNCATE_ON_ROLLBACK) == 
                                        MODE_TRUNCATE_ON_ROLLBACK) 
            {
				xact.addObserver(
                    new TruncateOnCommit(identity, false /* rollbacks only */));
			}

			if ((mode & MODE_DROP_ON_COMMIT) == MODE_DROP_ON_COMMIT) 
            {
				xact.addObserver(new DropOnCommit(identity));				
			}

			if ((mode & MODE_FLUSH_ON_COMMIT) == MODE_FLUSH_ON_COMMIT) 
            {
				xact.addObserver(new SyncOnCommit(identity));
			}
		}

		return true;
	}

	/**
		Return the RawTransaction I was opened in.
	*/
	public final RawTransaction getTransaction() 
    {

		if (SanityManager.DEBUG) 
        {
			SanityManager.ASSERT(xact != null);
		}

		return xact;
	}

	/**
		Return my locking policy, may be different from the Transaction's
		default locking policy.
	*/
	public final LockingPolicy getLockingPolicy() 
    {

		if (SanityManager.DEBUG) 
        {
			SanityManager.ASSERT(locking != null);
		}

		return locking;
	}

	public final void setLockingPolicy(LockingPolicy newLockingPolicy) 
    {
		locking = newLockingPolicy;
	}

	/**
		Was I opened for updates?

		<BR> MT - thread safe
	*/
	public final boolean updateOK() 
    {
		return forUpdate;
	}

	/**
		Get the mode I was opened with.
	*/
	public int getMode() 
    {
		return mode;
	}

	/**
	   The container is about to be modified.
	   Loggable actions use this to make sure the container gets cleaned if a
	   checkpoint is taken after any log record is sent to the log stream but
	   before the container is actually dirtied.

		@exception StandardException Standard Cloudscape error policy
	 */
	public void preDirty(boolean preDirtyOn) throws StandardException 
    {

		checkUpdateOpen();

		container.preDirty(preDirtyOn);

	}

	/**
		@see ContainerHandle#isTemporaryContainer
		@exception StandardException Standard Cloudscape error policy
	 */
	public boolean isTemporaryContainer() throws StandardException 
    {

		checkOpen();

		return (identity != null && 
				identity.getSegmentId() == ContainerHandle.TEMPORARY_SEGMENT);
	}

	/*
	** Implementation specific methods for myself and my sub-classes
	*/

	protected void checkOpen() throws StandardException 
    {
		if (!active)
			throw StandardException.newException(
                    SQLState.DATA_CONTAINER_CLOSED);
	}


    private void checkUpdateOpen() throws StandardException 
    {

		if (!active)
        {
			throw StandardException.newException(
                    SQLState.DATA_CONTAINER_CLOSED);
        }

		if (!forUpdate)
        {
			throw StandardException.newException(
                    SQLState.DATA_CONTAINER_READ_ONLY);
        }
	}

	protected void informObservers() 
    {

		// notify our observers (Pages) that we are closing, 
        // or undergoing some state change ...

		if (countObservers() != 0) 
        {
			setChanged();
			notifyObservers();
		}
	}


    /**
    Get information about space used by the container.
    **/
    public SpaceInfo getSpaceInfo()
        throws StandardException
    {
        return container.getSpaceInfo(this);
    }

    
    public String toString()
    {
        if (SanityManager.DEBUG)
        {
            String str = new String();
            str += "BaseContainerHandle:(" + identity.toString() + ")";
            return(str);
        }
        else
        {
            return(super.toString());
        }
    }

}
