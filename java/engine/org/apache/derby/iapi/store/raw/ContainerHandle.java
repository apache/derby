/*

   Derby - Class org.apache.derby.iapi.store.raw.ContainerHandle

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

package org.apache.derby.iapi.store.raw;

import org.apache.derby.iapi.store.access.SpaceInfo;
import org.apache.derby.iapi.error.StandardException;

import java.util.Properties;

/**
	A Container contains a contigious address space of pages, the pages
	start at page number Container.FIRST_PAGE_NUMBER and are numbered sequentially.
	
	The page size is set at addContainer() time.


	RESOLVE: this style of coding is not currently enforced
	If the caller calls getPage (or one of its variants) more than once on the 
    same page, the caller must call unlatch a corresponding number of times in 
    order to ensure that the page is latched.  For example:
	<p>
	<blockquote><pre>
    Container c;
	Page p1 = c.getPage(Container.FIRST_PAGE_NUMBER);
	Page p2 = c.getPage(Container.FIRST_PAGE_NUMBER);
	p1.unlatch();  -- Page is still latched.
	p2.unlatch();  -- Page is now unlatched.
	</pre></blockquote>

	<p>
	There is no restriction on the order in which latching and unlatching is 
    done.  In the example, p1 could have been unlatched after p2 with no ill 	
    effects.

	<P>	<B>Open container modes</B>
	ContainerHandle.MODE are used to open or create the container.
	Unlike TableProperties, MODEs are not permanantely associated with the
	container, it is effective only for the lifetime of the containerHandle
	itself.
	<BR>A container may use any of these mode flags when it is opened.
	<UL>
	<LI>MODE_READONLY - Open the container in read only mode.
	<LI>MODE_FORUPDATE - Open the container in update mode, if the underlying 
    storage does not allow updates
	then the container will be opned in read only mode.
	<LI>MODE_UNLOGGED - If Unset, any changes to the container are logged.
	If set, any user changes to the container are unlogged. It is guaranteed
    at commit time that all changes made during the transaction will have been 
    flushed to disk. Using this mode automatically opens the container in 
    container locking, isolation 3 level. The state of the container following
    an abort or any type of rollback is unspecified.
	<LI>MODE_CREATE_UNLOGGED - If set, not only are user changes to the
	container are unlogged, page allocations are also unlogged.  This MODE is
	only useful for container is created in the same statement and no change on
	the container (other than the create) is ever logged.  The difference
	between MODE_UNLOGGED and MODE_CREATE_UNLOGGED is that page allocation is
	also unlogged and commit of nested transaction will not cause the container
	to be forced from the cache.  Unlike MODE_UNLOGGED, MODE_CREATE_UNLOGGED
	does not force the cache.  It is up to the client of raw store to force the
	cache at the appropriate time - this allows a statement to create and open
	the container serveral times for bulk loading without logging or doing any
	synchronous I/O. 
	<LI>MODE_LOCK_NOWAIT - if set, then don't wait for the container lock, else
	wait for the container lock.  This flag only dictates whether the lock
	should be waited for or not.  After the container is successfully opened,
	whether this bit is set or not has no effect on the container handle.
	</UL>
	If neither or both of the {MODE_READONLY, MODE_FORUPDATE} modes are 
    specified then the behaviour of the container is unspecified.
	<BR>
	MODE_UNLOGGED must be set for MODE_CREATE_UNLOGGED to be set.
	<P>
	<B>Temporary Containers</B><BR>
	If when creating a container the segment used is 
    ContainerHandle.TEMPORARY_SEGMENT then the container is a temporary 
    container. Temporary containers are not logged or locked and do not live 
    across re-boots of the system. In addition any abort or rollback including
    rollbacks to savepoints truncate the container if it has been opened for 
    update since the last commit or abort.  Temporary containers are private 
    to a transaction and must only be used a single thread within the 
    transaction at any time, these restrictions are not currently enforced.
	<BR>
	When opening a temporary container for update access these additional mode
    flags may be used
	<UL>
	<LI> MODE_TRUNCATE_ON_COMMIT - At commit/abort time container is truncated.
	<LI> MODE_DROP_ON_COMMIT - At commit/abort time the container is dropped.
	<LI> MODE_TEMP_IS_KEPT - At commit/abort time the container is kept around.
	</UL>
	If a temporary container is opened multiple times in the same transaction 
    with different modes then the most severe mode is used, ie. none &lt; 
    truncate on commit &lt; drop on commit.
	The MODE_UNLOGGED, MODE_CREAT_UNLOGGED flags are ignored when opening a 
    temporary container, not logged is always assumed.  */

public interface ContainerHandle 
{

	/**
		Used in add container.
	*/
	public static final int DEFAULT_PAGESIZE = -1;

	public static final int DEFAULT_SPARESPACE = -1;

	public static final int DEFAULT_ASSIGN_ID = 0;

	/**
		See comments above for these modes.
	 */
	public static final int MODE_DEFAULT               = 0x00000000;
	public static final int MODE_UNLOGGED              = 0x00000001;
	public static final int MODE_CREATE_UNLOGGED       = 0x00000002;
	public static final int MODE_FORUPDATE             = 0x00000004;
	public static final int MODE_READONLY	           = 0x00000008;
	public static final int MODE_TRUNCATE_ON_COMMIT    = 0x00000010;
	public static final int MODE_DROP_ON_COMMIT        = 0x00000020;
	public static final int MODE_OPEN_FOR_LOCK_ONLY    = 0x00000040;
	public static final int MODE_LOCK_NOWAIT           = 0x00000080;
	public static final int MODE_TRUNCATE_ON_ROLLBACK  = 0x00000100; // internal raw store
	public static final int MODE_FLUSH_ON_COMMIT       = 0x00000200; // internal raw store
	public static final int MODE_NO_ACTIONS_ON_COMMIT  = 0x00000400; // internal raw store
	public static final int MODE_TEMP_IS_KEPT		   = 0x00000800; // internal raw store

	public static final int MODE_USE_UPDATE_LOCKS	   = 0x00001000; // external access
    public static final int MODE_SECONDARY_LOCKED      = 0x00002000; // external access
    public static final int MODE_BASEROW_INSERT_LOCKED = 0x00004000; // external access

	public static final int TEMPORARY_SEGMENT = -1;


	/**
		The first valid page number
	*/
	public static final long FIRST_PAGE_NUMBER = 1;
	
	/**
		A page number that is guaranteed to be invalid.
	*/
	public static final long INVALID_PAGE_NUMBER = -1;

	/**
		Return my identifier.
	*/
	public ContainerKey getId();

	/**
		Return my unique identifier, this identifier will be unique to each
        instance of an open container handle.  This id is used by the locking
        system to group locks to an open container handle.
	*/
	public Object getUniqueId();

    /**
     * Is the container opened for read only or update?
     *
	 * @return true if container is opened for read only, else false.
     **/
    boolean isReadOnly();

	/**
		Add an empty page to the container and obtain exclusive access to it.
		<P>
		Note that the added page may not be the last page in the Container.

		Once the Page is no longer required the Page's unlatch() method must 
        be called.

		@return a reference to the page that was added.

		@see Page#unlatch

		@exception StandardException	Standard Cloudscape error policy
		@exception StandardException If a page could not be allocated.
	*/
	public Page addPage() throws StandardException;


	/**	
		Add an empty page to the container and obtain exclusive access to it.
		<P>
		If flag == ADD_PAGE_DEFAULT, this call is identical to addPage().
		<BR>
		If flag == ADD_PAGE_BULK, then this call signifies to the container that
		this addPage is part of a large number of additional pages and it is
		desirable to do whatever possible to facilitate adding many subsequent pages.
		The actual container implementation will decide whether or not to heed
		this hint and what to do about it.

		@return a reference to the page that was added.

		@see Page#unlatch

		@exception StandardException	Standard Cloudscape error policy
		@exception StandardException If a page could not be allocated.

	*/
	public Page addPage(int flag) throws StandardException;
	public static final int ADD_PAGE_DEFAULT = 0x1;
	public static final int ADD_PAGE_BULK = 0x2;


	/**
		Try to preallocate numPage new pages if possible.
	 */
	public void preAllocate(int numPage);


	/**
		Remove this page from the container and unlatch the page.  <B>Caller
		should commit or abort this transaction ASAP because failure to do so
		will slow down page allocation of this container. </B>

		<BR>The page to be removed must be latched and gotten (or added) by
		this ContainerHandle.  The page should not be used again after this
		call as if it has been unlatched.  If the call to removePage is
		successful, this page is invalid should not be gotten again with
		getPage. 

		<BR>RemovePage will guarantee to unlatch the page even if a
		StandardException is thrown. 

		<P>
		<B>Locking Policy</B>
		<BR>
		The page will not be freed until the transaction that removed the page 
		commits.  A special RecordHandle.DEALLOC_PROTECTION_HANDLE lock will be 
		gotten for the transaction and which is used to prevent the page from 
		being freed.  This lock will be held regardless of the default locking 
		policy of the transaction that called removedPage.

		@see LockingPolicy
		@see RecordHandle

		@exception StandardException Standard Cloudscape error policy 
	*/
	public void removePage(Page page) throws StandardException;


	/**
		Obtain exclusive access to the page with the given page number.
		
		Once the Page is no longer required the Page's unlatch() method must 
        be called.

		<P>
		The Page object is guaranteed to remain in-memory and exclusive to the 
        caller until its unlatch() method is called.

		@return the required Page or null if the page does not exist or is not 
        valid (i.e, it has been deallocated or freed or never initialized)
		Note that an overflow page will be returned since it is a valid page.

		@exception StandardException	Standard Cloudscape error policy
	*/
	public Page getPage(long pageNumber)
		throws StandardException;

	/**
		Identical to getPage but returns null immediately if the desired page
        is already latched by another Container.

		@return the required Page or null if the page does not exist or the page
		is already latched.

		@exception StandardException	Standard Cloudscape error policy

	*/
	public Page getPageNoWait(long pageNumber) throws StandardException;

	/**
        Obtain exclusive access to the page with the given page number.

        Will only return a valid, non-overflow user page - so can be used by
        routines in post commit to get pages to attempt deleted row space
        reclamation.  If for some reason a request is made for an overflow
        page a null will be returned.

		Once the Page is no longer required the Page's unlatch() method must 
        be called.

		<P>
		The Page object is guaranteed to remain in-memory and exclusive to the 
        caller until its unlatch() method is called.

		@return the required Page or null if the page does not exist or is not 
        valid (i.e, it has been deallocated, freed, never initialized, or is
        an allocation page or overflow page)

		@exception StandardException	Standard Cloudscape error policy
	*/
	public Page getUserPageNoWait(long pageNumber) throws StandardException;
	/**
        Obtain exclusive access to the page with the given page number.

        Will only return a valid, non-overflow user page - so can be used by
        routines in post commit to get pages to attempt deleted row space
        reclamation.  If for some reason a request is made for an overflow
        page a null will be returned.

		Once the Page is no longer required the Page's unlatch() method must 
        be called.

		<P>
		The Page object is guaranteed to remain in-memory and exclusive to the 
        caller until its unlatch() method is called.

		@return the required Page or null if the page does not exist or is not 
        valid (i.e, it has been deallocated, freed, never initialized, or is
        an allocation page or overflow page)

		@exception StandardException	Standard Cloudscape error policy
	*/
	public Page getUserPageWait(long pageNumber) throws StandardException;

	/**
		Obtain exclusive access to the current first page of the container.
		Only a valid, non overflow page will be returned.
		Pages in the container are ordered in an internally defined ordering.
		<P>
		Note that once this method returns this page may no longer be the 
		first page of the container.  I.e, other threads may allocate pages 
		prior to this page number while this page is latched.  It is up to
		the caller of this routine to synchronize this call with addPage to 
		assure that this is the first page.  
		<BR>
		As long as the client provide the necessary lock to ensure 
		that no addPage is called, then this page is guaranteed to be the
		first page of the container in some internally defined ordering of
		the pages.

		@return latched page or null if there is no page in the container
		@exception StandardException	Standard Cloudscape error policy

		@see ContainerHandle#getPage
	*/
	public Page getFirstPage() throws StandardException;

	/**
		Obtain exclusive access to the next valid page of the given page number 
		in the container. Only a valid, non overflow page will be returned.
		Pages in the container are ordered in an internally defined ordering.
		<P>
		Note that once this method returns this page may no longer be the 
		next page of the container.  I.e, other threads may allocate pages 
		prior to this page number while this page is latched.  It is up to
		the caller of this routine to synchronize this call with addPage to 
		assure that this is the first page.  
		<BR>
		As long as the client provide the necessary lock to ensure 
		that no addPage is called, then this page is guaranteed to be the
		next page of the container in some internally defined ordering of
		the pages.
		<BR>
		If no pages are added or removed, then an iteration such as:
		<PRE>
		for (Page p = containerHandle.getFirstPage();
			 p != null;
			 p = containerHandle.getNextPage(p.getPageNumber()))
		<PRE>
		will guarentee to iterate thru and latched all the valid pages 
		in the container

		@param prevNum the pagenumber of the page previous to the page
		that is to be gotten.  The page which correspond to prevNum
		may or may not be latched by the caller, but it must be gotten 
		via a page which was (or currently still is) latched, and the page
		number must be gotten while the container must not have been closed 
		or dropped or removed in the interim.

		In other words, if the user manufactures a page number, or remembers 
		the page number from a previous session or a previous openContainer, 
		then the behavior of this routine is undefined.

		@return latched page or null if there is no next page in the container
		@exception StandardException	Standard Cloudscape error policy

		@see ContainerHandle#getPage
	*/
	public Page getNextPage(long prevNum) throws StandardException;


	/**
		Get a page for insert.  If RawStore thinks it knows where a potentially
		suitable page is for insert, it will return it.  If RawStore doesn't
		know where a suitable page for insert is, or if there are no allocated
		page, then null is returned.  If a page is returned, it will be a
		valid, non-overflow page.   A potentially suitable page is one which
		has enough space for a minium sized record.

		@return a valid, non-overflow page.  Or null if RawStore doesn't know
		where to find a good valid, non-overflow page.

		@param flag a GET_PAGE_* flag.

		@exception StandardException Standard Cloudscape error policy 
	*/
	public Page getPageForInsert(int flag) 
		 throws StandardException;

	// Try to get a page that is unfilled, 'unfill-ness' is defined by the
	// page.  Since unfill-ness is defined by the page, the only thing RawStore
	// guarentees about the page is that it has space for a a minimum sized
	// record.
	//
	// If this bit is not set, then getPageForInsert will get the page that was
	// last gotten, provided it has space for a minimum sized record.
	//
	// If for whatever reasons RawStore is unable to come up with such a page,
	// null will be returned.
	public static final int GET_PAGE_UNFILLED = 0x1;



    /**
     * Request the system properties associated with a container. 
     * <p>
     * Request the value of properties that are associated with a table.  The
     * following properties can be requested:
     *     derby.storage.pageSize 
     *     derby.storage.pageReservedSpace
     *     derby.storage.minimumRecordSize
     * <p>
     * To get the value of a particular property add it to the property list,
     * and on return the value of the property will be set to it's current 
     * value.  For example:
     *
     * get_prop(ConglomerateController cc)
     * {
     *     Properties prop = new Properties();
     *     prop.put("derby.storage.pageSize", "");
     *     cc.getTableProperties(prop);
     *
     *     System.out.println(
     *         "table's page size = " + 
     *         prop.getProperty("derby.storage.pageSize");
     * }
     *
     * @param prop   Property list to fill in.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    void getContainerProperties(Properties prop)
		throws StandardException;

	/**
		Close me. After using this method the caller must throw away the
		reference to the Container object, e.g.
		<PRE>
			ref.close();
			ref = null;
		</PRE>
		<BR>
		The container will be closed automatically at the commit or abort
		of the transaction if this method is not called explictly.
		<BR>
		Any pages that were obtained using me and have not been released
		using Page's unlatch method are released, and references to them must be
		thrown away.


		@see Page#unlatch
		@see Page#fetch
	*/
	public void close();

	/**
		Cost estimation
	*/

	/**
		Get the total estimated number of rows in the container, not including
		overflow rows.  This number is a rough estimate and may be grossly off.

		@param flag different flavors of row count (reserved for future use)
		@exception StandardException	Standard Cloudscape error policy
	 */
	public long getEstimatedRowCount(int flag) throws StandardException;

	/**
		Set the total estimated number of rows in the container.  Often, after
		a scan, the client of RawStore has a much better estimate of the number
		of rows in the container then what RawStore has.  Use this better
		number for future reference.
		<BR>
		It is OK for a ReadOnly ContainerHandle to set the estimated row count.

		@param count the estimated number of rows in the container.
		@param flag different flavors of row count (reserved for future use)

		@exception StandardException	Standard Cloudscape error policy
	 */
	public void setEstimatedRowCount(long count, int flag) throws StandardException;

	/**
		Get the total estimated number of allocated (not freed, not
		deallocated) user pages in the container, including overflow pages.
		this number is a rough estimate and may be grossly off.

		@param flag different flavors of page count (reserved for future use)

		@exception StandardException	Standard Cloudscape error policy
	 */
	public long getEstimatedPageCount(int flag) throws StandardException;


	/**
		Flush all dirty pages of the container to disk.  Used mainly for
		UNLOGGED or CREATE_UNLOGGED operation.

		@exception StandardException	Standard Cloudscape error policy
	*/
	public void flushContainer() throws StandardException;

	/**
		Return the locking policy for this open container.
	*/
	public LockingPolicy getLockingPolicy();

	/**
		Set the locking policy for this open container
	*/
	public void setLockingPolicy(LockingPolicy newLockingPolicy);

	/**
		Return a record handle that is initialized to the given segment id,
        container id, page number and record id.

		@exception StandardException Standard cloudscape exception policy.

		@param pageNumber   the page number of the RecordHandle.
		@param recordId     the record id of the RecordHandle.

		@see RecordHandle
	*/
	public RecordHandle makeRecordHandle(long pageNumber, int recordId)
		 throws	StandardException;


	/**
		This record probably has shrunk considerably.  Free its reserved space
		or compact it.

		@param record	The record handle, the record must have been locked execlusively already.
		@exception StandardException Standard cloudscape exception policy.
	*/
	public void compactRecord(RecordHandle record) throws StandardException;

	/**
		Return true if this containerHandle refers to a temporary container.
		@exception StandardException Standard cloudscape exception policy.
	 */
	public boolean isTemporaryContainer() throws StandardException;

    /**
    Get information about space used by the container.
    **/
    public SpaceInfo getSpaceInfo() throws StandardException;

}
