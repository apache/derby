/*

   Derby - Class org.apache.derby.impl.store.raw.data.FileContainer

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

package org.apache.derby.impl.store.raw.data;

import org.apache.derby.shared.common.reference.Property;

import org.apache.derby.shared.common.reference.Limits;
import org.apache.derby.shared.common.reference.SQLState;

import org.apache.derby.iapi.services.cache.Cacheable;
import org.apache.derby.iapi.services.cache.CacheManager;
import org.apache.derby.iapi.services.context.ContextService;
import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derby.iapi.services.io.FormatIdOutputStream;
import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.iapi.services.io.TypedFormat;

import org.apache.derby.iapi.util.InterruptStatus;
import org.apache.derby.iapi.util.InterruptDetectedException;

import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.iapi.store.raw.ContainerHandle;
import org.apache.derby.iapi.store.raw.ContainerKey;
import org.apache.derby.iapi.store.raw.Page;
import org.apache.derby.iapi.store.raw.PageKey;
import org.apache.derby.iapi.store.raw.RecordHandle;
import org.apache.derby.iapi.store.raw.RawStoreFactory;
import org.apache.derby.iapi.store.raw.Transaction;

import org.apache.derby.iapi.store.raw.log.LogInstant;
import org.apache.derby.iapi.store.raw.xact.RawTransaction;

import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.iapi.store.access.AccessFactory;
import org.apache.derby.iapi.store.access.SpaceInfo;

import org.apache.derby.iapi.services.io.ArrayInputStream;
import org.apache.derby.iapi.services.io.ArrayOutputStream;
import org.apache.derby.iapi.services.property.PropertyUtil;
import org.apache.derby.iapi.util.ByteArray;

import java.io.IOException;
import java.io.DataInput;

import java.security.PrivilegedAction;
import java.security.AccessController;

import java.util.Properties;
import java.util.zip.CRC32;

import org.apache.derby.io.StorageRandomAccessFile;

/**
	FileContainer is an abstract base class for containers
	which are based on files.

	This class extends BaseContainer and implements Cacheable and TypedFormat
*/

abstract class FileContainer 
    extends BaseContainer implements Cacheable, TypedFormat
{

	/*
	 * typed format
	 */

	protected static final int formatIdInteger = 
        StoredFormatIds.RAW_STORE_SINGLE_CONTAINER_FILE; 

	// format Id must fit in 4 bytes

	/**
		Return my format identifier.
	*/
	public int getTypeFormatId() 
    {
		return StoredFormatIds.RAW_STORE_SINGLE_CONTAINER_FILE;
	}

	/*
	** Immutable fields
	*/

	protected final CacheManager          pageCache;		// my page's cache
	protected final CacheManager          containerCache;   // cache I am in.
	protected final BaseDataFileFactory   dataFactory;      // creating factory

	/*
	** Fields that are mutable only during identity changes
	*/

	protected int pageSize;		        // size of my pages
	protected int spareSpace;	        // % space kept free on page in inserts
	protected int minimumRecordSize;	// minimum space a record should 
                                        // occupy on the page.

	protected short initialPages;	    // initial number of pages preallocated
                                        // to the container when created
                                        
	protected boolean canUpdate;        // can I be written to?

	private int PreAllocThreshold;      // how many pages before preallocation 
                                        // kicks in, only stored in memory
	private int PreAllocSize;	        // how many pages to preallocate at once
								        // only stored in memory
	private boolean bulkIncreaseContainerSize;// if true, the next addPage will
										 // attempt to preallocate a larger
										 // than normal number of pages.
                                         //
	// preallocation parameters
	private static final int PRE_ALLOC_THRESHOLD    = 8;
	private static final int MIN_PRE_ALLOC_SIZE     = 1;
	private static final int DEFAULT_PRE_ALLOC_SIZE = 8;
	private static final int MAX_PRE_ALLOC_SIZE     = 1000;

	/* 
	** Mutable fields, only valid when the identity is valid.
	*/

	// RESOLVE: if we run out of bytes in the container, we can change
	// containerVersion from a long to an int because this number is only
	// bumped when the container is dropped (and rolled back), so it is almost
	// impossible for the containverVersion to get beyond a short, let alone
	// and int - someone will have to write an application that attempt to drop
	// the container 2 billion times for that to happen.
	protected long			firstAllocPageNumber; // first alloc page number
	protected long			firstAllocPageOffset; // first alloc page offset
	protected long			containerVersion;     // the logged version number
	protected long			estimatedRowCount;    // value is changed unlogged
	protected LogInstant    lastLogInstant;       // last time this container 
                                                  // object was touched.
	/** 
	 * The sequence number for reusable recordIds . 
	 * As long as this number does not change, recordIds will be stable within
	 * the container.
	 **/
	private long reusableRecordIdSequenceNumber;


	/**
		The page that was last inserted into.  Use this for getPageForInsert.
		Remember the last allocated non-overflow page, and remember it in
		memory only.
		Use Get/Set method to access this field except when we know it is
		being single thread access.
	 */
	private long lastInsertedPage[];
	private int  lastInsertedPage_index;

	/** 
		The last unfilled page found.  Use this for getPageForInsert.
		Remember the last unfilled page found, and remember it in memory only.
		Use Get/Set method to access this field except when we know it is
		being single thread access.
	*/
	private long lastUnfilledPage;

	/**
		The last allocated page.  This global var is access *without*
		synchronization.  It is used as a hint for page allocation to find the
		next reusable page.
	 */
	private long lastAllocatedPage;

	/**
		An estimated page count. Use this for getEstimatedPagecount.
		Remember it in memory only.
	 */
	private long estimatedPageCount;


	// The isDirty flag indicates if the container has been modified.  The
	// preDirty flag indicates that the container is about to be modified.  The
	// reason for these 2 flags instead of just one is to accomodate
	// checkpoint.  After a clean container sends a log record to the log
	// stream but before that conatiner is dirtied by the log operation, a
	// checkpoint could be taken.  If so, then the redoLWM will be after the
	// log record but, without preDirty, the cache cleaning will not have
	// waited for the change.  So the preDirty bit is to stop the cache
	// cleaning from skipping over this container even though it has not really
	// been modified yet.
	protected boolean			preDirty;
	protected boolean			isDirty;

	/*
		allocation information cached by the container object.  

		<P>MT -
		Access to the allocation cache MUST be synchronized on the allocCache
		object.  FileContainer manages all MT issue w/r to AllocationCache.
		The AllocationCache object itself is not MT-safe.
		<P>
		The protocol for accessing both the allocation cache and the alloc page
		is: get the alloc cache semaphore, then get the alloc page.  Once both
		are held, they can be released in any order.
		<BR>
		It is legal to get one or the other, i.e, it is legal to only get the
		alloc cache semaphore without latching the alloc page, and it is legal
		to get the alloc page latch without the alloc cache semaphore.
		<BR>
		it is illegal to hold alloc page latch and then get the allocation
		cache semaphore
		<PRE>
		Writer to alloc Page (to invalidate alloc cache)
		1) synchronized(allocCache)
		2) invalidate cache
		3) get latch on alloc Page
		4) release synchonized(allocCache)

		Reader:
		1) synchronized(allocCache)
		2) if valid, read value and release synchronized(allocCache)
		3) if cache is invalid, get latch on alloc page
		4) validate cache
		5) release alloc page latch
		6) read value
		7) release synchonized(allocCache)
		</PRE>
	*/
	protected AllocationCache	allocCache;

	/*
	 * array to store persistently stored fields
	 */
	byte[] containerInfo;

	private	CRC32		checksum;		// holder for the checksum

	/*
	** buffer for encryption/decryption
	*/
	private byte[] encryptionBuffer;

	/*
	 * constants
	 */

	/** the container format must fit in this many bytes */
	private static final int CONTAINER_FORMAT_ID_SIZE = 4; 

	/* the checksum size */
	protected static final int CHECKSUM_SIZE = 8;

	/** 
		The size of the persistently stored container info
		ContainerHeader contains the following information:
		4 bytes int	FormatId
		4 bytes	int	status
		4 bytes int	pageSize
		4 bytes int	spareSpace
		4 bytes int minimumRecordSize
		2 bytes short initialPages
		2 bytes short spare1
		8 bytes	long	first Allocation page number
		8 bytes	long	first Allocation page offset
		8 bytes	long	container version
		8 bytes long	estimated number of rows
		8 bytes long	reusable recordId sequence number
		8 bytes long	spare3
		8 bytes	long	checksum
		container info size is 80 bytes, with 10 bytes of spare space
	*/
	protected static final int CONTAINER_INFO_SIZE = 
		CONTAINER_FORMAT_ID_SIZE+4+4+4+4+2+2+8+8+8+8+CHECKSUM_SIZE+8+8;

	/**
	 * where the first alloc page is located - 
	 * the logical page number and the physical page offset
	 * NOTE if it is not 0 this is not going to work for Stream 
	 * file which doesn't support seek
	 */
	public static final long FIRST_ALLOC_PAGE_NUMBER = 0L;
	public static final long FIRST_ALLOC_PAGE_OFFSET = 0L;

	// file status for persistent storage
	private static final int FILE_DROPPED        = 0x1;
	private static final int FILE_COMMITTED_DROP = 0x2;

	// recordId in this container can be reused when a page is reused.
	private static final int FILE_REUSABLE_RECORDID = 0x8;

	protected static final String SPACE_TRACE = 
        (SanityManager.DEBUG ? "SpaceTrace" : null);

	FileContainer(BaseDataFileFactory factory) 
    {
		dataFactory = factory;
		pageCache = factory.getPageCache();
		containerCache = factory.getContainerCache();
		
		initContainerHeader(true);
	}

    /**
    Get information about space used by the container.
    **/
    public SpaceInfo getSpaceInfo(BaseContainerHandle handle)
            throws StandardException
    {
        SpaceInformation spaceInfo;
        synchronized(allocCache)
        {
            spaceInfo = 
                allocCache.getAllPageCounts(handle,firstAllocPageNumber);
        }
        spaceInfo.setPageSize(pageSize);
        return spaceInfo;
    }

	/*
	** Methods of Cacheable
	**
	** getIdentity() and clearIdentity() are implemented by BaseContainer
	*/

	/**
		Containers
	*/

	/**
		Open the container.

		@return a valid object if the container was successfully opened, null if
		it does not exist.

		@exception StandardException Some problem in opening a container.

		@see Cacheable#setIdentity
	*/
	public Cacheable setIdentity(Object key) throws StandardException 
    {
        ContainerKey newIdentity = (ContainerKey) key;

        // If the new identity represents a temporary container, switch to
        // TempRAFContainer.
        if (newIdentity.getSegmentId() == ContainerHandle.TEMPORARY_SEGMENT) {
            return new TempRAFContainer(dataFactory).setIdent(newIdentity);
        }

        return setIdent(newIdentity);
	}

    /**
     * Open the container.
     * <p>
     * Open the container with key "newIdentity".
     * <p>
     * should be same name as setIdentity but seems to cause method resolution 
     * ambiguities
     *
     * @exception StandardException Some problem in opening a container.
     *
     * @see Cacheable#setIdentity
     **/
	protected Cacheable setIdent(ContainerKey newIdentity) 
        throws StandardException 
    {
		boolean ok          = openContainer(newIdentity);

        initializeLastInsertedPage(1);
		lastUnfilledPage    = ContainerHandle.INVALID_PAGE_NUMBER;
		lastAllocatedPage   = ContainerHandle.INVALID_PAGE_NUMBER;

		estimatedPageCount  = -1;

		if (ok) 
        {
			// set up our identity.
			// If we raise an exception after this we must clear our identity.
			fillInIdentity(newIdentity);
			return this;
		}
        else
        { 
            return null;
        }
	}

	public Cacheable createIdentity(Object key, Object createParameter) 
        throws StandardException 
    {
        ContainerKey newIdentity = (ContainerKey) key;

        // If the new identity represents a temporary container, switch to
        // TempRAFContainer.
        if (newIdentity.getSegmentId() == ContainerHandle.TEMPORARY_SEGMENT) {
            TempRAFContainer tmpContainer = new TempRAFContainer(dataFactory);
            return tmpContainer.createIdent(newIdentity, createParameter);
        }

        return createIdent(newIdentity, createParameter);
	}


	// should be same name as createIdentity but seems to cause method 
    // resolution ambiguities
	protected Cacheable createIdent(
    ContainerKey    newIdentity, 
    Object          createParameter) 
		 throws StandardException 
	{
		// createParameter will be this object if this method is being called 
        // from itself to re-initialize the container (only for tempRAF)
		// if createParameter == this, do not reinitialize the header, this
		// object is not being reused for another container
		if (createParameter != this) 
		{
			initContainerHeader(true /* change to different container */);

			if (createParameter != null && 
				(createParameter instanceof ByteArray))
			{
				// this is called during load tran, the create container
				// Operation has a byte array created by logCreateContainerInfo
				// which contains all the information necessary to recreate the
				// container.  Use that to recreate the container properties.

				createInfoFromLog((ByteArray)createParameter);
			}
			else
			{
				if (SanityManager.DEBUG)
				{
					if (createParameter != null &&
						!(createParameter instanceof Properties))
					{
						SanityManager.THROWASSERT(
							"Expecting a  non-null createParameter to a " +
                            "Properties instead of " +
							createParameter.getClass().getName());
					}
				}

				createInfoFromProp((Properties)createParameter);
			}
		}
		else
		{
			// we don't need to completely re-initialize the header
			// just re-initialize the relevant fields
			initContainerHeader(false);
		}

		if (initialPages > 1)
		{
			PreAllocThreshold           = 0;
			PreAllocSize                = initialPages;
			bulkIncreaseContainerSize   = true;
		}
		else
		{
			PreAllocThreshold           = PRE_ALLOC_THRESHOLD;
		}

		createContainer(newIdentity);

		setDirty(true);

		// set up our identity.
		// If we raise an exception after this we must clear our identity.
		fillInIdentity(newIdentity);

		return this;
	}

	public void clearIdentity() 
    {

		closeContainer();

        initializeLastInsertedPage(1);
		lastUnfilledPage = ContainerHandle.INVALID_PAGE_NUMBER;
		lastAllocatedPage = ContainerHandle.INVALID_PAGE_NUMBER;

		canUpdate = false;
		super.clearIdentity();
	}

	/**
		We treat this container as dirty if it has the container file open.
		@see Cacheable#isDirty
	*/
	public boolean isDirty() 
    {
		synchronized (this) 
        {
			return isDirty;
		}
	}

	public void preDirty(boolean preDirtyOn) 
    {
		synchronized (this) 
        {
			if (preDirtyOn)
			{
				// prevent the cleaner from cleaning this container or skipping
				// over it until the operation which preDirtied it got a chance
				// to do the change.
				preDirty = true;
			}
			else
			{
				preDirty = false;
				// if a cleaner is waiting on the dirty bit, wake it up
				notifyAll();
			}
		}
	}

	protected void setDirty(boolean dirty)
	{
		synchronized(this) 
        {
			preDirty = false;
			isDirty  = dirty;

			// if a cleaner is waiting on the dirty bit, wake it up
			notifyAll();
		}
	}

	/*
	** Container creation, opening, and closing
	*/

    /**
     * Create a new container.
     * <p>
     * Create a new container, all references to identity must be through the
     * passed in identity, this object will no identity until after this 
     * method returns.
     *
     * @exception StandardException Derby Standard error policy
     **/
	abstract void createContainer(ContainerKey newIdentity) 
        throws StandardException;
	

    /**
     * Open a container.
     * <p>
     * Longer descrption of routine.
     * <p>
     * Open a container. Open the file that maps to this container, if the
     * file does not exist then we assume the container was never created.
     * If the file exists but we have trouble opening it then we throw some 
     * exception.
     *
     * <BR> MT - single thread required - Enforced by cache manager.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
	abstract boolean openContainer(ContainerKey newIdentity) 
        throws StandardException;

	abstract void closeContainer();

    /**
     * Drop Container.
     * <p>
     *
     * @see Transaction#dropContainer
     *
     **/
	protected void dropContainer(
    LogInstant  instant, 
    boolean     isDropped)
	{
		synchronized(this)
		{
			setDroppedState(isDropped);
			setDirty(true);
			bumpContainerVersion(instant);
		}
	}


	/**
		increment the version by one and return the new version.

		<BR> MT - caller must synchronized this in the same sync block that
		modifies the container header.
	*/
	protected final void bumpContainerVersion(LogInstant instant) 
	{	
		lastLogInstant = instant;
		++containerVersion;
	}

	protected long getContainerVersion()
	{
		// it is not really necessary to synchronized this because the only time the
		// container version is looked at is during recovery, which is single
		// threaded at the moment.  Put it in an sync block anyway just in case
		// some other people want to look at this for some bizarre reasons
		synchronized(this)
		{
			return containerVersion;
		}
	}

    /**
     * Request the system properties associated with a container. 
     * <p>
     * Request the value of properties that are associated with a container. 
     * The following properties can be requested:
     *     derby.storage.pageSize 
     *     derby.storage.pageReservedSpace
     *     derby.storage.minimumRecordSize
     *     derby.storage.reusableRecordId
     *     derby.storage.initialPages
     * <p>
     * To get the value of a particular property add it to the property list,
     * and on return the value of the property will be set to it's current 
     * value.  For example:
     *
     * get_prop(ConglomerateController cc)
     * {
     *     Properties prop = new Properties();
     *     prop.put("derby.storage.pageSize", "");
     *     cc.getContainerProperties(prop);
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
    public void getContainerProperties(Properties prop)
		throws StandardException
    {
        // derby.storage.pageSize
        if (prop.getProperty(Property.PAGE_SIZE_PARAMETER) != null)
        {
            prop.put(
                Property.PAGE_SIZE_PARAMETER, 
                Integer.toString(pageSize));
        }

        // derby.storage.minimumRecordSize
        if (prop.getProperty(RawStoreFactory.MINIMUM_RECORD_SIZE_PARAMETER) != 
                null)
        {
            prop.put(
                RawStoreFactory.MINIMUM_RECORD_SIZE_PARAMETER, 
                Integer.toString(minimumRecordSize));
        }

        // derby.storage.pageReservedSpace
        if (prop.getProperty(RawStoreFactory.PAGE_RESERVED_SPACE_PARAMETER) != 
                null)
        {
            prop.put(
                RawStoreFactory.PAGE_RESERVED_SPACE_PARAMETER, 
                Integer.toString(spareSpace));
        }

		// derby.storage.reusableRecordId
		if (prop.getProperty(RawStoreFactory.PAGE_REUSABLE_RECORD_ID) != null)
		{
			Boolean bool = isReusableRecordId();
			prop.put(RawStoreFactory.PAGE_REUSABLE_RECORD_ID,
					 bool.toString());
		}

		// derby.storage.initialPages
		if (prop.getProperty(RawStoreFactory.CONTAINER_INITIAL_PAGES) != null)
		{
			prop.put(RawStoreFactory.CONTAINER_INITIAL_PAGES,
					 Integer.toString(initialPages));
		}

    }

	/**
        Read the container's header.

        When this method is called, the embryonic page that is passed in must
        have been read directly from the file or the input stream, even if the
        alloc page may still be in cache.  This is because a stubbify operation
        only writes the stub to disk, it does not get rid of any stale page
        from the page cache.  So if it so happens that the stubbified container
        object is aged out of the container cache but the first alloc page
        hasn't, then when any stale page of this container wants to be written
        out, the container needs to be reopened, which is when this routine is
        called.  We must not get the alloc page in cache because it may be
        stale page and it may still say the container has not been dropped.

		<BR> MT - single thread required - Enforced by caller.

        @param epage the embryonic page to read the header from
		@exception StandardException Derby Standard error policy
		@exception IOException error in reading the header from file
	*/
	protected void readHeader(byte[] epage)
		 throws IOException, StandardException
	{
		// read persistent container header into containerInfo
		AllocPage.ReadContainerInfo(containerInfo, epage);

		// initialize header from information stored in containerInfo
		readHeaderFromArray(containerInfo);
	}

	// initialize header information so this container object can be safely
	// reused as if this container object has just been new'ed
	private void initContainerHeader(boolean changeContainer)
	{
		if (containerInfo == null)
			containerInfo = new byte[CONTAINER_INFO_SIZE];

		if (checksum == null)
			checksum = new CRC32();
		else
			checksum.reset();

		if (allocCache == null)
			allocCache = new AllocationCache();
		else
			allocCache.reset();

		if (changeContainer)
		{
			pageSize = 0;
			spareSpace = 0;
			minimumRecordSize = 0;
		}

		initialPages = 1;
		firstAllocPageNumber = ContainerHandle.INVALID_PAGE_NUMBER;	
		firstAllocPageOffset = -1;
		containerVersion = 0;
		estimatedRowCount = 0;
		reusableRecordIdSequenceNumber = 0;

		setDroppedState(false);
		setCommittedDropState(false);
		setReusableRecordIdState(false);

		// instance variables that are not stored on disk
		lastLogInstant = null;

        initializeLastInsertedPage(1);
		lastUnfilledPage = ContainerHandle.INVALID_PAGE_NUMBER;
		lastAllocatedPage = ContainerHandle.INVALID_PAGE_NUMBER;
		estimatedPageCount = -1;

		PreAllocThreshold = PRE_ALLOC_THRESHOLD;
		PreAllocSize = DEFAULT_PRE_ALLOC_SIZE;
		bulkIncreaseContainerSize = false;
	}


	/**
		Read containerInfo from a byte array
		The container Header array must be written by or of
		the same format as put together by writeHeaderFromArray.

		@exception StandardException Derby Standard error policy
		@exception IOException error in reading the header from file
	*/
	private void readHeaderFromArray(byte[] a)
		 throws StandardException, IOException
	{
		ArrayInputStream inStream = new ArrayInputStream(a);

		inStream.setLimit(CONTAINER_INFO_SIZE);
		int fid = inStream.readInt();
		if (fid != formatIdInteger)
        {
			throw StandardException.newException(
                SQLState.DATA_UNKNOWN_CONTAINER_FORMAT, getIdentity(), 
                fid);
        }

		int status = inStream.readInt();
		pageSize = inStream.readInt();
		spareSpace = inStream.readInt();
		minimumRecordSize = inStream.readInt();
		initialPages = inStream.readShort(); 
		PreAllocSize = inStream.readShort();
		firstAllocPageNumber = inStream.readLong();
		firstAllocPageOffset = inStream.readLong();
		containerVersion = inStream.readLong();
		estimatedRowCount = inStream.readLong();
		reusableRecordIdSequenceNumber = inStream.readLong();
		lastLogInstant = null;

		if (PreAllocSize == 0)	// pre 2.0, we don't store this.
			PreAllocSize = DEFAULT_PRE_ALLOC_SIZE;

		long spare3 = inStream.readLong();	// read spare long

		// upgrade - if this is a container that was created before
		// initialPages was stored, it will have a zero value.  Set it to the
		// default of 1.
		if (initialPages == 0)	
			initialPages = 1;

		// container read in from disk, reset preAllocation values
		PreAllocThreshold = PRE_ALLOC_THRESHOLD;

		// validate checksum
		long onDiskChecksum = inStream.readLong();
		checksum.reset();
		checksum.update(a, 0, CONTAINER_INFO_SIZE - CHECKSUM_SIZE);

		if (onDiskChecksum != checksum.getValue())
		{
			PageKey pk = new PageKey(identity, FIRST_ALLOC_PAGE_NUMBER);

			throw dataFactory.markCorrupt
				(StandardException.newException(
                    SQLState.FILE_BAD_CHECKSUM, 
                    pk, 
                    checksum.getValue(), 
                    onDiskChecksum, 
                    org.apache.derby.iapi.util.StringUtil.hexDump(a)));
		}

		allocCache.reset();

		// set the in memory state
		setDroppedState((status & FILE_DROPPED) != 0);
		setCommittedDropState((status & FILE_COMMITTED_DROP) != 0);
		setReusableRecordIdState((status & FILE_REUSABLE_RECORDID) != 0);
	}


	/**
		Write the container header to a page array (the first allocation page)

		@exception StandardException Derby Standard error policy
		@exception IOException error in writing the header to file
	*/
	protected void writeHeader(
    Object          identity, 
    byte[]          pageData)
		 throws StandardException, IOException
	{
		// write out the current containerInfo in the borrowed space to byte
		// array containerInfo
		writeHeaderToArray(containerInfo);

        try
        {
		AllocPage.WriteContainerInfo(containerInfo, pageData, false);
	}
        catch (StandardException  se)
        {
			throw StandardException.newException(
                SQLState.DATA_BAD_CONTAINERINFO_WRITE, se, identity);
        }
	}

	/**
		Write the container header directly to file.

		Subclasses that can writes the container header is expected to
		manufacture a DataOutput stream which is used here.

		<BR> MT - single thread required - Enforced by caller

		@exception StandardException Derby Standard error policy
		@exception IOException error in writing the header to file
	 */
	protected void writeHeader(
    Object                  identity,
    StorageRandomAccessFile file,
    boolean                 create, 
    byte[]                  epage)
		 throws IOException, StandardException
	{
		// write out the current containerInfo in the borrowed space to byte
		// array containerInfo
		writeHeaderToArray(containerInfo);

		// RESOLVE: get no wait on the page cache to see if allocation page is
		// there, if so, use that instead of making a new array and a static
		// function.
        try
        {
            AllocPage.WriteContainerInfo(containerInfo, epage, create);
        }
        catch (StandardException  se)
        {
			throw StandardException.newException(
                SQLState.DATA_BAD_CONTAINERINFO_WRITE, se, identity);
        }

		// now epage has the containerInfo written inside it

		// force WAL - and check to see if database is corrupt or is frozen.
		dataFactory.flush(lastLogInstant);
		if (lastLogInstant != null)
			lastLogInstant = null;

		// write it out
		dataFactory.writeInProgress();
		try
		{
            writeAtOffset(file, epage, FIRST_ALLOC_PAGE_OFFSET);
		}
		finally
		{
			dataFactory.writeFinished();
		}
	}

    /**
     * Write a sequence of bytes at the given offset in a file. This method
     * is not thread safe, so the caller must make sure that no other thread
     * is performing operations that may change current position in the file.
     *
     * @param file the file to write to
     * @param bytes the bytes to write
     * @param offset the offset to start writing at
     * @throws IOException if an I/O error occurs while writing
	 * @exception StandardException  Derby Standard error policy
     */
    void writeAtOffset(StorageRandomAccessFile file, byte[] bytes, long offset)
            throws IOException, StandardException
    {
        file.seek(offset);
        file.write(bytes);
    }

	/**
		Get an embryonic page from the dataInput stream.

		The embryonic page will be read
		in from the input stream (fileData), which is assumed to be 
		positioned at the beginning of the first allocation page.

		@exception IOException error in read the embryonic page from file
		@exception StandardException  Derby Standard error policy
	*/
	protected byte[] getEmbryonicPage(DataInput fileData) throws
		IOException, StandardException
	{
		byte[] epage = new byte[AllocPage.MAX_BORROWED_SPACE];

		fileData.readFully(epage);

		return epage;
	}

    /**
     * Read an embryonic page (that is, a section of the first alloc page that
     * is so large that we know all the borrowed space is included in it) from
     * the specified offset in a {@code StorageRandomAccessFile}. This method
     * is not thread safe, so the caller must make sure that no other thread
     * is performing operations that may change current position in the file.
     *
     * @param file the file to read from
     * @param offset where to start reading (normally
     * {@code FileContainer.FIRST_ALLOC_PAGE_OFFSET})
     * @return a byte array containing the embryonic page
     * @throws IOException if an I/O error occurs while reading
	 * @throws StandardException  Derby Standard error policy
     */
    byte[] getEmbryonicPage(StorageRandomAccessFile file, long offset)
            throws IOException, StandardException
    {
        file.seek(offset);
        return getEmbryonicPage(file);
    }

	/**
		Write containerInfo into a byte array
		The container Header thus put together can be read by readHeaderFromArray.

		@exception IOException error in writing the header
	*/
	private void writeHeaderToArray(byte[] a) throws IOException
	{
		if (SanityManager.DEBUG)
			SanityManager.ASSERT(a.length >= CONTAINER_INFO_SIZE,
								 "header won't fit in array");

		ArrayOutputStream a_out = new ArrayOutputStream(a);
		FormatIdOutputStream outStream = new FormatIdOutputStream(a_out);

		int status = 0;
		if (getDroppedState()) status |= FILE_DROPPED;
		if (getCommittedDropState()) status |= FILE_COMMITTED_DROP;
		if (isReusableRecordId()) status |= FILE_REUSABLE_RECORDID;

		a_out.setPosition(0);
		a_out.setLimit(CONTAINER_INFO_SIZE);
		outStream.writeInt(formatIdInteger);
		outStream.writeInt(status);
		outStream.writeInt(pageSize);
		outStream.writeInt(spareSpace);
		outStream.writeInt(minimumRecordSize);
		outStream.writeShort(initialPages);
		outStream.writeShort(PreAllocSize);		// write spare1
		outStream.writeLong(firstAllocPageNumber);
		outStream.writeLong(firstAllocPageOffset);
		outStream.writeLong(containerVersion);
		outStream.writeLong(estimatedRowCount);
		outStream.writeLong(reusableRecordIdSequenceNumber);
		outStream.writeLong(0);		//Write spare3

		checksum.reset();
		checksum.update(a, 0, CONTAINER_INFO_SIZE - CHECKSUM_SIZE);

		// write the checksum to the array
		outStream.writeLong(checksum.getValue());

		a_out.clearLimit();
	}

	/**
		Log all information on the container creation necessary to recreate the
		container during a load tran.

		@exception StandardException Derby Standard error policy
	 */
	protected ByteArray logCreateContainerInfo() 
		 throws  StandardException
	{
		// just write out the whole container header
		byte[] array = new byte[CONTAINER_INFO_SIZE];

		try
		{
			writeHeaderToArray(array);
		}
		catch (IOException ioe)
		{
			throw StandardException.newException(
                SQLState.DATA_UNEXPECTED_EXCEPTION, ioe);
		}

		return new ByteArray(array);
	}

	/**
		Set container properties from the passed in ByteArray, which is created
		by logCreateContainerInfo.  This information is used to recreate the
		container during recovery load tran.

		The following container properties are set:

		pageSize
		spareSpace
		minimumRecordSize
		isReusableRecordId
		initialPages

	 */
	private void createInfoFromLog(ByteArray byteArray) 
		 throws StandardException 
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(byteArray != null,
				"setCreateContainerInfoFromLog: ByteArray is null");
			SanityManager.ASSERT(byteArray.getLength() == 
								 CONTAINER_INFO_SIZE,
				"setCreateContainerInfoFromLog: ByteArrays.length() != CONTAINER_INFO_SIZE");
		}

		byte[] array = byteArray.getArray();
		
		// now extract the relevant information from array - basically
		// duplicate the code in readHeaderFromArray 
		ArrayInputStream inStream = new ArrayInputStream(array);

		int status = 0;

		try
		{			
			inStream.setLimit(CONTAINER_INFO_SIZE);

			int fid = inStream.readInt();
			if (fid != formatIdInteger)
			{
				// RESOLVE: do something about this when we have > 1 container format
				throw StandardException.newException(
                    SQLState.DATA_UNKNOWN_CONTAINER_FORMAT, 
                    getIdentity(), fid);
			}

			status = inStream.readInt();
			pageSize = inStream.readInt();
			spareSpace = inStream.readInt();
			minimumRecordSize = inStream.readInt();
			initialPages = inStream.readShort(); 

		}
		catch (IOException ioe)
		{
			throw StandardException.newException(
                    SQLState.DATA_UNEXPECTED_EXCEPTION, ioe);
		}

		// set reusable record id property
		setReusableRecordIdState((status & FILE_REUSABLE_RECORDID) != 0);

		// sanity check to make sure we are not encoutering any
		// dropped Container 
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT((status & FILE_DROPPED) == 0 &&
								 (status & FILE_COMMITTED_DROP) == 0,
				"cannot load a dropped container");
		}
	}

	/**
		Set container properties from the passed in createArgs.  
		The following container properties are set:

		pageSize
		spareSpace
		minimumRecordSize
		isReusableRecordId
		initialPages

		RESOLVE - in the future setting parameters should be overridable
		by sub-class, e.g. one implementation of Container may require a
		minimum page size of 4k.
	 */
	private void createInfoFromProp(Properties createArgs)
		 throws StandardException
	{
		// Need a TransactionController to get database/service wide properties.
		AccessFactory af = (AccessFactory)
			getServiceModule(dataFactory, AccessFactory.MODULE);

		// RESOLVE: sku defectid 2014
		TransactionController tc = 
            (af == null) ? 
                null : 
                af.getTransaction(
                        getContextService().getCurrentContextManager());

		pageSize = 
			PropertyUtil.getServiceInt(tc, createArgs,
				Property.PAGE_SIZE_PARAMETER,  
				Limits.DB2_MIN_PAGE_SIZE, 
				Limits.DB2_MAX_PAGE_SIZE, 
				RawStoreFactory.PAGE_SIZE_DEFAULT); 

        // rather than throw error, just automatically set page size to 
        // default if bad value given.
        if ((pageSize != 4096)  &&
            (pageSize != 8192)  &&
            (pageSize != 16384) &&
            (pageSize != 32768))
        {
            pageSize= RawStoreFactory.PAGE_SIZE_DEFAULT;
        }

		spareSpace = 
			PropertyUtil.getServiceInt(tc, createArgs,
				RawStoreFactory.PAGE_RESERVED_SPACE_PARAMETER, 
				0, 100, 20);

		PreAllocSize = 
			PropertyUtil.getServiceInt(tc, createArgs,
					RawStoreFactory.PRE_ALLOCATE_PAGE,
					MIN_PRE_ALLOC_SIZE,
					MAX_PRE_ALLOC_SIZE,				   
					DEFAULT_PRE_ALLOC_SIZE /* default */);

		// RESOLVE - in the future, we will allow user to set minimumRecordSize
		// to be larger than pageSize, when long rows are supported.
		if (createArgs == null) {
			// if the createArgs is null, then the following method call
			// will get the system properties from the appropriete places.
			// we want to make sure minimumRecrodSize is set to at least
			// the default value MINIMUM_RECORD_SIZE_DEFAULT (12)
			// as set in rawStoreFactory.
			minimumRecordSize = 
				PropertyUtil.getServiceInt(tc,
					RawStoreFactory.MINIMUM_RECORD_SIZE_PARAMETER, 
					RawStoreFactory.MINIMUM_RECORD_SIZE_DEFAULT, // this is different from the next call
					// reserving 100 bytes for record/field headers
					(pageSize * (1 - spareSpace/100) - 100), 
					RawStoreFactory.MINIMUM_RECORD_SIZE_DEFAULT);
		} else {
			// if the createArgs is not null, then it has already been set
			// by upper layer or create statement, then, we allow the minimum
			// value of this to be MINIMUM_RECORD_SIZE_MINIMUM (1).
			minimumRecordSize = 
				PropertyUtil.getServiceInt(tc, createArgs,
					RawStoreFactory.MINIMUM_RECORD_SIZE_PARAMETER, 
					RawStoreFactory.MINIMUM_RECORD_SIZE_MINIMUM,  // this is different from the last call
					// reserving 100 bytes for record/field headers
					(pageSize * (1 - spareSpace/100) - 100), 
					RawStoreFactory.MINIMUM_RECORD_SIZE_DEFAULT);
		}

		// For the following properties, do not check value set in global
		// properties, we only listen to what access has to say about them.
		//
		// whether or not container's recordIds can be reused
		// if container is to be created with a large number of pages
		if (createArgs != null)
		{
			String reusableRecordIdParameter = 
				createArgs.getProperty(RawStoreFactory.PAGE_REUSABLE_RECORD_ID);
			if (reusableRecordIdParameter != null)
			{	
                Boolean reusableRecordId = Boolean.parseBoolean(reusableRecordIdParameter);
				setReusableRecordIdState(reusableRecordId.booleanValue());
			}

			String containerInitialPageParameter =
				createArgs.getProperty(RawStoreFactory.CONTAINER_INITIAL_PAGES);
			if (containerInitialPageParameter != null)
			{
				initialPages = 
					Short.parseShort(containerInitialPageParameter);
				if (initialPages > 1)
				{
					if (initialPages > RawStoreFactory.MAX_CONTAINER_INITIAL_PAGES)
						initialPages = RawStoreFactory.MAX_CONTAINER_INITIAL_PAGES;
				}
			}
		}
	}

	/**
	*/
	protected boolean canUpdate() {
		return canUpdate;
	}

	/**
		Deallocate a page from the container.  

		@param handle the container handle doing the deallocation
		@param page the page to be deallocated.  It is latched upon entry and
		will be unlatched by the caller of this function

		@exception StandardException Derby Standard error policy
	*/
	protected void deallocatePage(BaseContainerHandle handle, BasePage page)
		 throws StandardException
	{
		if (SanityManager.DEBUG) {
			SanityManager.ASSERT(page.isLatched(), "page is not latched");
			SanityManager.ASSERT(page.getPageNumber() != FIRST_ALLOC_PAGE_NUMBER, 
								 "cannot deallocate an alloc page");
		}

		long pnum = page.getPageNumber();

		// dealloc the page from the alloc page
		deallocatePagenum(handle, pnum);

		// mark the page as deallocated.  Page should not be touched after this
		// the page latch is released by the BaseContainer upon return of this
		// method.  Regardless of whether this operation is successful or not,
		// the page will be unlatched by BaseContainer.
		page.deallocatePage();
		
	}

	/** deallocate the page from the alloc page */
	private void deallocatePagenum(BaseContainerHandle handle, long pnum)
		 throws StandardException
	{
		synchronized(allocCache)
		{
			long allocPageNum = allocCache.getAllocPageNumber(handle, pnum, firstAllocPageNumber);

			if (SanityManager.DEBUG)
			{
				if (allocPageNum == ContainerHandle.INVALID_PAGE_NUMBER)
					allocCache.dumpAllocationCache();

				if (allocPageNum == ContainerHandle.INVALID_PAGE_NUMBER)
					SanityManager.THROWASSERT(
									 "can't find alloc page for page number " +
									 pnum);
			}
			// get the alloc page to deallocate this pnum
			AllocPage allocPage = (AllocPage)handle.getAllocPage(allocPageNum);
			if (allocPage == null)
			{
				PageKey pkey = new PageKey(identity, allocPageNum);

				throw StandardException.newException(
                        SQLState.FILE_NO_ALLOC_PAGE, pkey);
			}

			try
			{
				allocCache.invalidate(allocPage, allocPageNum); 

				// Unlatch alloc page.  The page is protected by the dealloc
				// lock. 
				allocPage.deallocatePage(handle, pnum);
			}
			finally
			{
				allocPage.unlatch();
			}
		}
		// make sure this page gets looked at when someone needs a new page
		if (pnum <= lastAllocatedPage) 
		{
			lastAllocatedPage = pnum - 1;
		}

	}

	/**
	  Compress free space from container.

	  <BR> MT - thread aware - It is assumed that our caller (our super class)
	  has already arranged a logical lock on page allocation to only allow a
	  single thread through here.

      Compressing free space is done in allocation page units, working
      it's way from the end of the container to the beginning.  Each
      loop operates on the last allocation page in the container.

      Freeing space in the container page involves 2 transactions, an
      update to an allocation page, N data pages, and possibly the delete
      of the allocation page.
	  The User Transaction (UT) initiated the compress call.
	  The Nested Top Transaction (NTT) is the transaction started by RawStore
	  inside the compress call.  This NTT is committed before compress returns.
	  The NTT is used to access high traffic data structures such as the 
      AllocPage.

	  This is outline of the algorithm used in compressing the container.

      Until a non free page is found loop, in each loop return to the OS
         all space at the end of the container occupied by free pages, including
         the allocation page itself if all of it's pages are free.  
      
	  1) Find last 2 allocation pages in container (last if there is only one).
	  2) invalidate the allocation information cached by the container.
		 Without the cache no page can be gotten from the container.  Pages
		 already in the page cache are not affected.  Thus by latching the 
		 allocPage and invalidating the allocation cache, this NTT blocks out 
		 all page gets from this container until it commits.
	  3) the allocPage determines which pages can be released to the OS, 
         mark that in its data structure (the alloc extent).  Mark the 
         contiguous block of nallocated/free pages at the end of the file
         as unallocated.  This change is associated with the NTT.
      4) The NTT calls the OS to deallocate the space from the file.  Note
         that the system can handle being booted and asked to get an allocated
         page which is past end of file, it just extends the file automatically.
	  5) If freeing all space on the alloc page, and there is more than one
         alloc page, then free the alloc page - this requires an update to the 
         previous alloc page which the loop has kept latched also.
      6) if the last alloc page was deleted, restart loop at #1

      All NTT latches are released before this routine returns.
	  If we use an NTT, the caller has to commit the NTT to release the
	  allocPage latch.  If we don't use an NTT, the allocPage latch is released
	  as this routine returns.

	  @param ntt - the nested top transaction for the purpose of freeing space.
						If ntt is null, use the user transaction for allocation.
	  #param allocHandle - the container handle opened by the ntt, 
						use this to latch the alloc page

	  @exception StandardException Standard Derby error policy 
	*/
	protected void compressContainer(
    RawTransaction      ntt,
    BaseContainerHandle allocHandle)
		 throws StandardException 
	{
		AllocPage alloc_page      = null;
		AllocPage prev_alloc_page = null;

		if (firstAllocPageNumber == ContainerHandle.INVALID_PAGE_NUMBER)
        {
            // no allocation pages in container, no work to do!
			return;
        }

        
        // make sure we don't execute redo recovery on any page
        // which is getting truncated.  At this point we have an exclusive
        // table lock on the table, so after checkpoint no page change
        // can happen between checkpoint log record and compress of space.
        dataFactory.getRawStoreFactory().checkpoint();

        // block the backup, If backup is already in progress wait 
        // for the backup to finish. Otherwise restore from the backup
        // can start recovery at different checkpoint and possibly
        // do redo on pages that are going to get truncated.
        ntt.blockBackup(true);

		try
		{
            synchronized(allocCache)
            {
                // loop until last 2 alloc pages are reached.
                alloc_page = (AllocPage) 
                    allocHandle.getAllocPage(firstAllocPageNumber);

                while (!alloc_page.isLast())
                {
                    if (prev_alloc_page != null)
                    {
                        // there are more than 2 alloc pages, unlatch the 
                        // earliest one.
                        prev_alloc_page.unlatch();
                    }
                    prev_alloc_page = alloc_page;
                    alloc_page      = null;

                    long nextAllocPageNumber = 
                        prev_alloc_page.getNextAllocPageNumber();
                    long nextAllocPageOffset = 
                        prev_alloc_page.getNextAllocPageOffset();

                    alloc_page = (AllocPage) 
                        allocHandle.getAllocPage(nextAllocPageNumber);
                }

                // invalidate cache before compress changes cached information,
                // while holding synchronization on cache and latch on 
                // allocation page.  This should guarantee that only new info
                // is seen after this operation completes.
				allocCache.invalidate(); 

                // reset, as pages may not exist after compress
                lastUnfilledPage    = ContainerHandle.INVALID_PAGE_NUMBER;
                lastAllocatedPage   = ContainerHandle.INVALID_PAGE_NUMBER;


                alloc_page.compress(ntt, this);
            }

		}
        finally
        {
			if (alloc_page != null)
            {
				alloc_page.unlatch();
                alloc_page = null;
            }
			if (prev_alloc_page != null)
            {
				prev_alloc_page.unlatch();
				prev_alloc_page = null;
            }

            // flush all changes to this file from cache.
            flushAll();

            // make sure all truncated pages are removed from the cache,
            // as it will get confused in the future if we allocate the same
            // page again, but find an existing copy of it in the cache - 
            // it expects to not find new pages in the cache.  Could just
            // get rid of truncated pages, iterface allows one page or
            // all pages.
            pageCache.discard(identity);
        }
	}

	/**
	 * Get the reusable RecordId sequence number for the container.
	 * @see BaseContainer#getReusableRecordIdSequenceNumber
	 * @return reusable RecordId sequence number for the container.
	 */
	public final long getReusableRecordIdSequenceNumber() {
		synchronized(this) {
			return reusableRecordIdSequenceNumber;
		}
	}
	
	/**
	 * Increment the reusable RecordId version sequence number.
	 */
	protected final void incrementReusableRecordIdSequenceNumber()
	{
		final boolean readOnly = dataFactory.isReadOnly();
		
		synchronized (this) {
			reusableRecordIdSequenceNumber++;
			if (!readOnly)
			{
				isDirty = true;
			}
		}
	}


	/**
	  Create a new page in the container.

	  <BR> MT - thread aware - It is assumed that our caller (our super class)
	  has already arranged a logical lock on page allocation to only allow a
	  single thread through here.

	  Adding a new page involves 2 transactions and 2 pages.  
	  The User Transaction (UT) initiated the addPage call and expects a
	  latched page (owns by the UT) to be returned.
	  The Nested Top Transaction (NTT) is the transaction started by RawStore
	  inside an addPage call.  This NTT is committed before the page is
	  returned.  The NTT is used to accessed high traffic data structure such
	  as the AllocPage.

	  This is outline of the algorithm used in adding a page:
	  1) find or make an allocPage which can handle the addding of a new page.
		Latch the allocPage with the NTT.
	  2) invalidate the allocation information cached by the container.
		Without the cache no page can be gotten from the container.  Pages
		already in the page cache is not affected.  Thus by latching the 
		allocPage and invalidating the allocation cache, this NTT blocks out 
		all page gets from this container until it commits.
	  3) the allocPage determines which page can be allocated, mark that in its
		data structure (the alloc extent) and returns the page number of the
		new page.  This change is associated with the NTT.
	  4) the NTT gets or creates the new page in the page cache (bypassing the
		lookup of the allocPage since that is already latched by the NTT and
		will deadlock).
	  5) the NTT initializes the page (mark it is being a VALID page).
	  6) the page latch is transfered to the UT from the NTT.
	  7) the new page is returned, latched by UT

	  If we use an NTT, the caller has to commit the NTT to release the
	  allocPage latch.  If we don't use an NTT, the allocPage latch is released
	  as this routine returns.

	  @param userHandle - the container handle opened by the user transaction, 
						use this to latch the new user page
	  @param ntt - the nested top transaction for the purpose of allocating the new page
						If ntt is null, use the user transaction for allocation.
	  #param allocHandle - the container handle opened by the ntt, 
						use this to latch the alloc page

	  @exception StandardException Standard Derby error policy 
	*/
	protected BasePage newPage(BaseContainerHandle userHandle,
							   RawTransaction ntt,
							   BaseContainerHandle allocHandle,
							   boolean isOverflow) 
		 throws StandardException 
	{
		// NOTE: we are single threaded thru this method, see MT comment

		boolean useNTT = (ntt != null);

		// if ntt is null, use user transaction
		if (!useNTT)
			ntt = userHandle.getTransaction();

		long lastPage;			// last allocated page
		long lastPreallocPage;	// last pre-allcated page
        long pageNumber =
            ContainerHandle.INVALID_PAGE_NUMBER; // init to appease compiler
                                // the page number of the new page
		PageKey pkey;			// the identity of the new page
		boolean reuse;			// if true, we are trying to reuse a page

		/* in case the page recommeded by allocPage is not committed yet, may
		/* need to retry a couple of times */
		boolean retry;
		int numtries = 0;

        int maxTries = InterruptStatus.MAX_INTERRUPT_RETRIES;

        long startSearch = lastAllocatedPage;

		AllocPage allocPage = null;	// the alloc page
		BasePage page = null;	// the new page

		try
		{
			do
			{
				retry = false;		// we don't expect we need to retry

				synchronized(allocCache)
				{
					if (SanityManager.DEBUG)
					{
						SanityManager.ASSERT(
                            ntt.getId().equals(
                                allocHandle.getTransaction().getId()));

						if (useNTT)
							SanityManager.ASSERT(
                                !ntt.getId().equals(
                                    userHandle.getTransaction().getId()));
					}

                    /* find an allocation page that can handle adding a new 
                     * page.
                     *
                     * allocPage is unlatched when the ntt commits. The new 
                     * page is initialized by the ntt but the latch is 
                     * transfered to the user transaction before the allocPage 
                     * is unlatched.  The allocPage latch prevents almost any 
                     * other reader or writer from finding the new page until 
                     * the ntt is committed and the new page is latched by the
                     * user transaction.
                     *
                     * (If the page is being reused, it is possible for another
                     * xact which kept a handle on the reused page to find the 
                     * page during the transfer UT -> NTT. If this unlikely 
                     * even occurs and the transfer fails [see code relating 
                     * to transfer below], we retry from the beginning.)
                     *
                     * After the NTT commits a reader (getNextPageNumber) may 
                     * get the page number of the newly allocated page and it 
                     * will wait for the new page and latch it when the user 
                     * transaction commits, aborts or unlatches the new page. 
                     * Whether the user transaction commits or aborts, the new 
                     * page stay allocated.
                     *
                     * RESOLVE: before NTT rolls back (or commits) the latch is
                     * released.  To repopulate the allocation cache, need to 
                     * get either the container lock on add page, or get a per 
                     * allocation page lock.
                     *
                     * This blocks all page read (getPage) from accessing this 
                     * alloc page in this container until the alloc page is 
                     * unlatched.  Those who already have a page handle into 
                     * this container are unaffected.
                     *
                     * In other words, allocation blocks out reader (of any 
                     * page that is managed by this alloc page) by the latch 
                     * on the allocation page.
                     *
                     * Note that write page can proceed as usual.
                     */
                    try {
                        allocPage =
                            findAllocPageForAdd(allocHandle, ntt, startSearch);
                    } catch (InterruptDetectedException e) {
                        // Retry. We needed to back all the way up here in the
                        // case of the container having been closed due to an
                        // interrupt on another thread, since that thread's
                        // recovery needs the monitor to allocCache which we
                        // hold. We release it when we do "continue" below.
                        if (--maxTries > 0) {
                            // Clear firstAllocPageNumber, i.e. undo side
                            // effect of makeAllocPage, so retry will work
                            firstAllocPageNumber =
                                ContainerHandle.INVALID_PAGE_NUMBER;
                            retry = true;

                            // Wait a bit so recovery can take place before
                            // we re-grab monitor on "this" (which recovery
                            // needs) and retry writeRAFHeader.
                            try {
                                Thread.sleep(
                                    InterruptStatus.INTERRUPT_RETRY_SLEEP);
                            } catch (InterruptedException ee) {
                                // This thread received an interrupt as
                                // well, make a note.
                                InterruptStatus.setInterrupted();
                            }

                            continue;
                        } else {
                            throw StandardException.newException(
                                SQLState.FILE_IO_INTERRUPTED, e);
                        }
                    }


					allocCache.invalidate(allocPage, allocPage.getPageNumber());
				}

				if (SanityManager.DEBUG)
				{
					if (allocPage == null)
						allocCache.dumpAllocationCache();

					SanityManager.ASSERT(allocPage != null,
                         "findAllocPageForAdd returned a null alloc page");
				}

				//
				// get the next free page's number.
				// for case 1, page number > lastPreallocPage
				// for case 2, page number <= lastPage
				// for case 3, lastPage < page number <= lastPreallocPage
				//
				pageNumber = allocPage.nextFreePageNumber(startSearch);

				// need to distinguish between the following 3 cases:
				// 1) the page has not been allocate or initalized.
				//		Create it in the page cache and sync it to disk.
				// 2) the page is being re-allocated.
				//		We need to read it in to re-initialize it
				// 3) the page has been preallocated.
				//		Create it in the page cache and don't sync it to disk
				//
				// first find out the current last initialized page and
				// preallocated page before the new page is added
				lastPage         = allocPage.getLastPagenum();
				lastPreallocPage = allocPage.getLastPreallocPagenum();

				reuse = pageNumber <= lastPage;

				// no address translation necessary
				pkey = new PageKey(identity, pageNumber);


				if (reuse)
				{
					// if re-useing a page, make sure the deallocLock on the new
					// page is not held.  We only need a zero duration lock on
					// the new page because the allocPage is latched and this
					// is the only thread which can be looking at this
					// pageNumber.

					RecordHandle deallocLock = BasePage.MakeRecordHandle(pkey,
								 RecordHandle.DEALLOCATE_PROTECTION_HANDLE);

					if (!getDeallocLock(allocHandle, deallocLock,
										false /* nowait */,
										true /* zeroDuration */))
					{

						// The transaction which deallocated this page has not
						// committed yet. Try going to some other page.  If
						// this is the first time we fail to get the dealloc
						// lock, try from the beginning of the allocated page.
						// If we already did that and still fail, keep going
						// until we get a brand new page.
						if (numtries == 0)
						{
							startSearch = ContainerHandle.INVALID_PAGE_NUMBER;
							lastAllocatedPage = pageNumber;
						}
						else	// continue from where we were
							startSearch = pageNumber;

						numtries++;

						// We have to unlatch the allocPage so that if that
						// transaction rolls back, it won't deadlock with this
						// transaction.
						allocPage.unlatch();
						allocPage = null;

						retry = true;
					}
					else
					{
						// we got the lock, next time start from there
						lastAllocatedPage = pageNumber;
					}
				}
				else
				{
					// we got a new page, next time, start from beginning of
					// the bit map again if we suspect there are some some
					// deallocated pages
					if (numtries > 0)
						lastAllocatedPage = ContainerHandle.INVALID_PAGE_NUMBER;
					else
						lastAllocatedPage = pageNumber;
				}

                // Retry from the beginning if necessary.
                if (retry)
                    continue;

                // If we get past here must have (retry == false)
                if (SanityManager.DEBUG)
                {
                    SanityManager.ASSERT(retry == false);
                }

			    // Now we have verified that the allocPage is latched and we 
                // can get the zeroDuration deallocLock nowait.  This means the
                // transaction which freed the page has committed.  Had that 
                // transaction aborted, we would have retried.

			    if (SanityManager.DEBUG)
			    {
				    // ASSERT lastPage <= lastPreallocPage
				    if (lastPage > lastPreallocPage)
                    {
					    SanityManager.THROWASSERT("last page " +
						    lastPage + " > lastPreallocPage " + 
                            lastPreallocPage);
                    }
			    }

			    // No I/O at all if this new page is requested as part of a 
                // create and load statement or this new page is in a temporary
                // container.
                //
			    // In the former case, BaseContainer will allow the 
                // MODE_UNLOGGED bit to go thru to the nested top transaction 
                // alloc handle.  In the later case, there is no nested top 
                // transaction and the alloc handle is the user handle, which 
                // is UNLOGGED.
			    boolean noIO = 
                    (allocHandle.getMode() & ContainerHandle.MODE_UNLOGGED) ==
                        ContainerHandle.MODE_UNLOGGED;

			    // If we do not need the I/O (either because we are in a
			    // create_unlogged mode or we are dealing with a temp table), 
                // don't do any preallocation.  Otherwise, see if we should be
			    // pre-Allocating page by now.  We don't call it before
			    // nextFreePageNumber because finding a reusable page may be
			    // expensive and we don't want to start preAllocation unless 
                // there is no more reusable page.  Unless we are called 
                // explicitly to bulk increase the container size in a preload 
                // or in a create container.
			    if (!noIO && 
                    (bulkIncreaseContainerSize ||
					 (pageNumber > lastPreallocPage && 
                      pageNumber > PreAllocThreshold)))
			    {
				    allocPage.preAllocatePage(
                        this, PreAllocThreshold, PreAllocSize);
			    }

			    // update last preAllocated Page, it may have been changed by 
                // the preAllocatePage call.  We don't want to do the sync if 
			    // preAllocatePage already took care of it.
			    lastPreallocPage = allocPage.getLastPreallocPagenum();
			    boolean prealloced = pageNumber <= lastPreallocPage;

			    // Argument to the create is an array of ints.
			    // The array is only used for new page creation or for creating
                // a preallocated page, not for reuse.
			    // 0'th element is the page format
			    // 1'st element is whether or not to sync the page to disk
			    // 2'nd element is pagesize
			    // 3'rd element is spareSpace

                PageCreationArgs createPageArgs = new PageCreationArgs(
                        StoredPage.FORMAT_NUMBER,
                        prealloced ? 0 : (noIO ? 0 : CachedPage.WRITE_SYNC),
                        pageSize,
                        spareSpace,
                        minimumRecordSize,
                        0 /* containerInfoSize - unused for StoredPage */);

			    // RESOLVE: right now, there is no re-mapping of pages, so
			    // pageOffset = pageNumber*pageSize
			    long pageOffset = pageNumber * pageSize;

			    // initialize a new user page
			    // we first use the NTT to initialize the new page - in case the
			    // allocation failed, it is rolled back with the NTT.
			    // Later, we transfer the latch to the userHandle so it won't be
			    // released when the ntt commits

                try
                {
			    page = initPage(allocHandle, pkey, createPageArgs, pageOffset,
				    			reuse, isOverflow);
                }
                catch (StandardException se)
                {
                    if (SanityManager.DEBUG) {
                        SanityManager.DEBUG_PRINT("FileContainer",
                            "got exception from initPage:"  +
                            "\nreuse = " + reuse +
                            "\nsyncFlag = " + createPageArgs.syncFlag +
                            "\nallocPage = " + allocPage
                            );
                    }
                    allocCache.dumpAllocationCache();

                    throw se;
                }

			    if (SanityManager.DEBUG)
			    {
				    SanityManager.ASSERT(
                        page != null, "initPage returns null page");
				    SanityManager.ASSERT(
                        page.isLatched(), "initPage returns unlatched page");
			    }

			    // allocate the page in the allocation page bit map
			    allocPage.addPage(this, pageNumber, ntt, userHandle);

			    if (useNTT)
			    {
				    // transfer the page latch from NTT to UT.
                    //
				    // after the page is unlatched by NTT, it is still 
                    // protected from being found by almost everybody else 
                    // because the alloc page is still latched and the alloc 
                    // cache is invalidated.
                    //
                    // However it is possible for the page to be 
                    // found by threads who specifically ask for this 
                    // pagenumber (e.g. HeapPostCommit).
                    // We may find that such a thread has latched the page. 
                    // We shouldn't wait for it because we have the alloc page 
                    // latch, and this could cause deadlock (e.g. 
                    // HeapPostCommit might call removePage and this would wait
                    // on the alloc page).
                    //
                    // We may instead find that we can latch the page, but that
                    // another thread has managed to get hold of it during the 
                    // transfer and either deallocated it or otherwise change it
                    // (add rows, delete rows etc.)
                    //
                    // Since this doesn't happen very often, we retry in these 
                    // 2 cases (we give up the alloc page and page and we start
                    // this method from scratch).
                    //
                    // If the lock manager were changed to allow latches to be 
                    // transferred between transactions, wouldn't need to 
                    // unlatch to do the transfer, and would avoid having to 
                    // retry in these cases (DERBY-2337).

				    page.unlatch();
				    page = null;

				    // need to find it in the cache again since unlatch also 
                    // unkept the page from the cache
				    page = (BasePage)pageCache.find(pkey);
				    page = latchPage(
                                userHandle, page, 
                                false /* don't wait, it might deadlock */);

                    if (page == null ||
                        // recordCount will only return true if there are no 
                        // rows (including deleted rows)
                        page.recordCount() != 0 ||
                        page.getPageStatus() != BasePage.VALID_PAGE)
                    {
                        retry = true;
                        if (page != null)
                        {
                            page.unlatch();
                            page = null;
                        }
                        allocPage.unlatch();
                        allocPage = null;
                    }

                }
    			// if ntt is null, no need to transfer.  Page is latched by user
	    		// transaction already.  Will be no need to retry.
		    	// the alloc page is unlatched in the finally block.
            }
            while (retry == true);

            // At this point, should have a page suitable for returning
            if (SanityManager.DEBUG)
                SanityManager.ASSERT(page.isLatched());
		}
		catch (StandardException se)
		{
			if (page != null)
				page.unlatch();
			page = null;

			throw se;			// rethrow error
		}
		finally
		{
			if (!useNTT && allocPage != null)
			{
				allocPage.unlatch();
				allocPage = null;
			}

			// NTT is committed by the caller
		}

		if (SanityManager.DEBUG)
			SanityManager.ASSERT(page.isLatched());


		// if bulkIncreaseContainerSize is set, that means this newPage call
		// may have greatly expanded the container size due to preallocation.
		// Regardless of how many page it actually created, reset preAllocSize
		// to the default so we won't attempt to always preallocate 1000 pages
		// at a time in the future.
		if (bulkIncreaseContainerSize)
		{
			bulkIncreaseContainerSize = false;
			PreAllocSize = DEFAULT_PRE_ALLOC_SIZE;
		}

		if (!isOverflow && page != null)
			setLastInsertedPage(pageNumber);


		// increase estimated page count - without any synchronization or
		// logging, this is an estimate only
		if (estimatedPageCount >= 0)
			estimatedPageCount++;

		if (!this.identity.equals(page.getPageId().getContainerId())) {

			if (SanityManager.DEBUG) {
				SanityManager.THROWASSERT(
                    "just created a new page from a different container"
					+ "\n this.identity = " + this.identity
					+ "\n page.getPageId().getContainerId() = " + 
                        page.getPageId().getContainerId()
					+ "\n userHandle is: " + userHandle
					+ "\n allocHandle is: " + allocHandle
					+ "\n this container is: " + this);
			}

			throw StandardException.newException(
                    SQLState.DATA_DIFFERENT_CONTAINER, 
                    this.identity, page.getPageId().getContainerId());
		}

		return page;			// return the newly added page
	}

	protected void clearPreallocThreshold()
	{
		// start life with preallocated page if possible
		PreAllocThreshold = 0;
	}

	protected void prepareForBulkLoad(BaseContainerHandle handle, int numPage)
	{
		clearPreallocThreshold();
		RawTransaction tran = handle.getTransaction();

		// find the last allocation page - do not invalidate the alloc cache,
		// we don't want to prevent other people from reading or writing
		// pages. 
		AllocPage allocPage = findLastAllocPage(handle, tran);

		// preallocate numPages.  Do whatever this allocPage can handle, if it
		// is full, too bad.  We don't guarentee that we will preallocate this
		// many pages, we only promise to try.
		if (allocPage != null)
		{
			allocPage.preAllocatePage(this, 0, numPage); 
			allocPage.unlatch();	
		}
	}

	private boolean pageValid(BaseContainerHandle handle, long pagenum)
		 throws StandardException
	{
		boolean retval = false;
        boolean done;
        int maxTries = InterruptStatus.MAX_INTERRUPT_RETRIES;

        do {
            done = true;
            synchronized(allocCache) {
                try {
                    if (pagenum <= allocCache.getLastPageNumber(
                                handle, firstAllocPageNumber) &&
                            (allocCache.getPageStatus(
                                handle, pagenum, firstAllocPageNumber) ==
                                 AllocExtent.ALLOCATED_PAGE)) {

                        retval = true;
                    }
                } catch (InterruptDetectedException e) {
                    // Retry. We needed to back all the way up here in the case
                    // of the (file) container having been closed due to an
                    // interrupt since the recovery needs the monitor to
                    // allocCache
                    if (--maxTries > 0) {
                        done = false;

                        // Wait a bit so recovery can take place before
                        // we re-grab monitor on "this" (which recovery
                        // needs) and retry writeRAFHeader.
                        try {
                            Thread.sleep(InterruptStatus.INTERRUPT_RETRY_SLEEP);
                        } catch (InterruptedException ee) {
                            // This thread received an interrupt as
                            // well, make a note.
                            InterruptStatus.setInterrupted();
                        }

                        continue;
                    } else {
                        throw StandardException.newException(
                            SQLState.FILE_IO_INTERRUPTED, e);
                    }
                }
            }
        } while (!done);

		return retval;
	}

	protected long getLastPageNumber(BaseContainerHandle handle) 
        throws StandardException
	{
		long retval;
		synchronized(allocCache)
		{
            // check if the first alloc page number is valid, it is invalid 
            // if some one attempts to access the container info before the 
            // first alloc page got created. One such case is online backup. 
            // If first alloc page itself is invalid, then there are no pages
            // on the disk yet for this container, just return
            // ContainerHandle.INVALID_PAGE_NUMBER, caller can decide what to
            // do. 
            
            if (firstAllocPageNumber == ContainerHandle.INVALID_PAGE_NUMBER)	
            {
                retval = ContainerHandle.INVALID_PAGE_NUMBER;
            }
            else
            {
                retval = 
                    allocCache.getLastPageNumber(handle, firstAllocPageNumber);
            }
		}
		return retval;
	}

	/*
		Find or allocate an allocation page which can handle adding a new page.
		Return a latched allocPage.

		<BR> MT - single thread required - called as part of add page
	*/
	private AllocPage findAllocPageForAdd(BaseContainerHandle allocHandle,
										  RawTransaction ntt, long lastAllocatedPage)
		 throws StandardException
	{
		AllocPage allocPage = null;
		AllocPage oldAllocPage = null; // in case we need to walk the alloc page chain
		boolean success = false; // set this for clean up

		try
		{
			if (firstAllocPageNumber == ContainerHandle.INVALID_PAGE_NUMBER)
			{
				// make and return a latched new allocation page
				allocPage = makeAllocPage(ntt, allocHandle, FIRST_ALLOC_PAGE_NUMBER,
										  FIRST_ALLOC_PAGE_OFFSET, CONTAINER_INFO_SIZE);

				if (SanityManager.DEBUG)
				{
					SanityManager.ASSERT(firstAllocPageNumber == FIRST_ALLOC_PAGE_NUMBER,
										 "first Alloc Page number is still not set");
					SanityManager.ASSERT(firstAllocPageOffset == FIRST_ALLOC_PAGE_OFFSET,
										 "first Alloc Page offset is still not set");
				}
			}
			else
			{
				// an allocation page already exist, go get it
				allocPage = (AllocPage)allocHandle.getAllocPage(firstAllocPageNumber);
			}

			/* allocPage is latched by allocHandle */

			if (!allocPage.canAddFreePage(lastAllocatedPage))
			{
				// allocPage cannot manage the addition of one more page, walk the
				// alloc page chain till we find an allocPage that can
				// RESOLVE: always start with the first page for now...

				boolean found = false; // found an alloc page that can handle 
										// adding a new page

				while(allocPage.isLast() != true)
				{
					long nextAllocPageNumber = allocPage.getNextAllocPageNumber();
					long nextAllocPageOffset = allocPage.getNextAllocPageOffset();

					// RESOLVE (future): chain this info to in memory structure so
					// getAllocPage can find this alloc page

					allocPage.unlatch();
					allocPage = null;

					// the nextAllocPage is stable once set - even though it is
					// save to get the next page latch before releasing this
					// allocPage.
					allocPage = (AllocPage)allocHandle.getAllocPage(nextAllocPageNumber);

					if (allocPage.canAddFreePage(lastAllocatedPage))
					{
						found = true;
						break;
					}
				}

				if (!found)
				{
					// allocPage is last and it is full
					oldAllocPage = allocPage;
					allocPage = null;

					if (SanityManager.DEBUG)
						SanityManager.ASSERT(oldAllocPage.getLastPagenum() ==
											 oldAllocPage.getMaxPagenum(),
											 "expect allocpage to be full but last pagenum != maxpagenum");

					long newAllocPageNum = oldAllocPage.getMaxPagenum() + 1;
					long newAllocPageOffset = newAllocPageNum; // no translation

					allocPage = makeAllocPage(ntt, allocHandle,
											  newAllocPageNum,
											  newAllocPageOffset,
											  0 /* no containerInfo */);

					// this writes out the new alloc page and return a latched page
					// nobody can find the new alloc page until oldAllocPage is unlatched.

					// oldAllocPage is no longer the last alloc page, 
					// it has a pointer to the new last alloc page
					oldAllocPage.chainNewAllocPage(allocHandle, newAllocPageNum, newAllocPageOffset);
					oldAllocPage.unlatch();
					oldAllocPage = null;
				}
			}

			/* no error handling necessary */
			success = true;
		}
		finally					// unlatch allocation page if any error happened
		{
			if (!success)
			{
				if (oldAllocPage != null)
					oldAllocPage.unlatch();

				if (allocPage != null)
					allocPage.unlatch();

				allocPage = null;
			}

			// if success drop out of finally block
		}

		return allocPage;
	}

	/**
		Find the last alloc page, returns null if no alloc page is found
	 */
	private AllocPage findLastAllocPage(BaseContainerHandle handle,
										RawTransaction tran)
	{
		AllocPage allocPage = null;
		AllocPage oldAllocPage = null;

		if (firstAllocPageNumber == ContainerHandle.INVALID_PAGE_NUMBER)
			return null;

		try
		{
			allocPage = (AllocPage)handle.getAllocPage(firstAllocPageNumber);
			while(!allocPage.isLast())
			{
				long nextAllocPageNumber = allocPage.getNextAllocPageNumber();
				long nextAllocPageOffset = allocPage.getNextAllocPageOffset();

				allocPage.unlatch();
				allocPage = null;

				allocPage = (AllocPage)handle.getAllocPage(nextAllocPageNumber);
			}
		}
		catch (StandardException se)
		{
			if (allocPage != null)
				allocPage.unlatch();
			allocPage = null;
		}

		return allocPage;

	}


	/*
		Make a new alloc page, latch it with the passed in container handle.
	*/
	private AllocPage makeAllocPage(RawTransaction ntt, 
									BaseContainerHandle handle, 
									long pageNumber, 
									long pageOffset,
									int containerInfoSize)
		 throws StandardException
	{
		if (SanityManager.DEBUG)
		{
			if (containerInfoSize != 0 && 
								 containerInfoSize != CONTAINER_INFO_SIZE)
				SanityManager.THROWASSERT(
								 "expect 0 or " + CONTAINER_INFO_SIZE +
								 ", got " + containerInfoSize);

			if (pageNumber != FIRST_ALLOC_PAGE_NUMBER &&
								 containerInfoSize != 0)
				SanityManager.THROWASSERT(
								 "Not first alloc page but container info size "
								 + containerInfoSize);
		}

		// argument to the create is an array of ints
		// 0'th element is the page format
		// 1'st element is whether or not to sync the page to disk
		// 2'nd element is the pagesize
		// 3'rd element is spareSpace
		// 4'th element is number of bytes to reserve for the container header
		// 5'th element is the minimumRecordSize
		// NOTE: the arg list here must match the one in allocPage

		// No I/O at all if this new page is requested as part of a create
		// and load statement or this new alloc page is in a temporary 
        // container.
		// In the former case, BaseContainer will allow the MODE_UNLOGGED
		// bit to go thru to the nested top transaction alloc handle.
		// In the later case, there is no nested top transaction and the
		// alloc handle is the user handle, which is UNLOGGED.

		boolean noIO = (handle.getMode() & ContainerHandle.MODE_UNLOGGED) ==
			ContainerHandle.MODE_UNLOGGED;

		PageCreationArgs createAllocPageArgs = new PageCreationArgs(
                AllocPage.FORMAT_NUMBER,
                noIO ? 0 : CachedPage.WRITE_SYNC,
                pageSize,
                0,        // allocation page has no need for spare
                minimumRecordSize,
                containerInfoSize);

		if (SanityManager.DEBUG)
        {
            if (SanityManager.DEBUG_ON(SPACE_TRACE))
            {
                SanityManager.DEBUG(
                    SPACE_TRACE, "making new allocation page at " + pageNumber);
            }
        }

		if (pageNumber == FIRST_ALLOC_PAGE_NUMBER)
		{
			// RESOLVE: make sure the following is true
			// 
			// firstAllocPageNumber and Offset can be set and access without
			// synchronization since the first allocation page is
			// created as part of the container create, this value is set
			// before any other transaction has a chance to open the container.
			// Once set, the first allocation page does not move or change
			// position 
			firstAllocPageNumber = pageNumber;
			firstAllocPageOffset = pageOffset;

		}

		PageKey pkey = new PageKey(identity, pageNumber);

		// return a latched new alloc page
		return (AllocPage)initPage(handle, pkey, createAllocPageArgs, 
								   pageOffset,
								   false, /* not reuse */
								   false /* not overflow */);
	}

	/**
		Initialize a page 

		@return a latched page that has been initialized.

		@param allochandle the contianer handle to initialize the page with - the ntt
		@param pkey the page number of the page to be initialized
		@param createArgs the arguments for page creation
		@param reuse is true if we are reusing a page that has 
				already been initialized once

		@exception StandardException Derby Standard error policy
	*/
	protected BasePage initPage(BaseContainerHandle allochandle, 
								PageKey pkey,
								PageCreationArgs createArgs,
								long pageOffset,
								boolean reuse,
								boolean overflow) throws StandardException
	{
		BasePage page = null;

		boolean releasePage = true;

		try
		{
			if (reuse)				//  read the page in first
			{
				// Cannot go thru the container handle because all read pages are blocked.  
				// do it underneath the handle and directly to the cache.  
				// Nobody can get thru becuase getPage will block at getting the alloc page.

				if (SanityManager.DEBUG)
                {
                    if (SanityManager.DEBUG_ON(SPACE_TRACE))
                    {
                        SanityManager.DEBUG(
                            SPACE_TRACE, "reusing page " + pkey);
                    }
                }

				page = (BasePage)pageCache.find(pkey);
				if (page == null)	// hmmm?
                {
					throw StandardException.newException(
                            SQLState.FILE_REUSE_PAGE_NOT_FOUND, pkey);
                }
			}
			else
			{
				if (SanityManager.DEBUG)
                {
                    if (SanityManager.DEBUG_ON(SPACE_TRACE))
                    {
                        SanityManager.DEBUG(
                            SPACE_TRACE, "allocation new page " + pkey);
                    }
                }

				// a brand new page, initialize and a new page in cache
				page = (BasePage) pageCache.create(pkey, createArgs);

				if (SanityManager.DEBUG)
					SanityManager.ASSERT(page != null, "page Cache create return a null page");
			}
			releasePage = false;
            page = latchPage(allochandle, page, true /* may need to wait, track3822 */);

			if (page == null)
            {
				throw StandardException.newException(
                        SQLState.FILE_NEW_PAGE_NOT_LATCHED, pkey);
            }     

			// page is either brand new or is read from disk, in either case,
			// it knows how to get itself initialized.
			int initPageFlag = 0;
			if (reuse) initPageFlag |= BasePage.INIT_PAGE_REUSE;
			if (overflow) initPageFlag |= BasePage.INIT_PAGE_OVERFLOW;
			if (reuse && isReusableRecordId())
				initPageFlag |= BasePage.INIT_PAGE_REUSE_RECORDID;

			page.initPage(initPageFlag, pageOffset);
			page.setContainerRowCount(estimatedRowCount);

		}
		finally
		{
			if (releasePage && page != null)
			{
				// release the new page from cache if it errors 
				// out before the exclusive lock is set
				pageCache.release((Cacheable)page);
				page = null;
			}
		}

		return page;
	}


	/**
		Get a page in the container.    

		Get User page is the generic base routine for all user (client to raw
		store) getPage.  This routine coordinate with allocation/deallocation
		to ensure that no page can be gotten from the container while page is
		in the middle of being allocated or deallocated.
        This routine latches the page.

		@param handle the container handle
		@param pageNumber the page number of the page to get
		@param overflowOK if true then an overflow page is OK,
				if false, then only non-overflow page is OK
        @param wait if true then wait for a latch
        @return the latched page

		<BR> MT - thread safe

		@exception StandardException Standard Derby error policy
	*/
	private BasePage getUserPage(BaseContainerHandle handle, long pageNumber,
        boolean overflowOK, boolean wait)
		 throws StandardException
	{

		if (SanityManager.DEBUG) 
        {
			SanityManager.ASSERT(
                pageNumber != FIRST_ALLOC_PAGE_NUMBER,
                "getUserPage trying to get an alloc page, pageNumber = " + 
                pageNumber);

			if (pageNumber < ContainerHandle.FIRST_PAGE_NUMBER)
				SanityManager.THROWASSERT("pageNumber = " + pageNumber);
		}

		if (pageNumber < ContainerHandle.FIRST_PAGE_NUMBER)
			return null;

		if (getCommittedDropState()) // committed and dropped, cannot get a page
			return null;

		if (!pageValid(handle, pageNumber))
		{
			return null;
		}

		// RESOLVE: no translation!

		PageKey pageSearch = new PageKey(identity, pageNumber);
		BasePage page = (BasePage)pageCache.find(pageSearch);

		if (page == null)
		{
			return page;
		}

        // latch the page
        if (latchPage(handle,page,wait) == null)
        {
			// page was already released from cache
            return null;
        }

		// double check for overflow and deallocated page
		// a page that was valid before maybe invalid by now if it was
		// deallocated in the interum.
		// a page that is invalid can also become valid in the interim, but
		// we do not handle that.  The client must supply other locking
		// mechanism to prevent that (an allocatino happenning where there are
		// readers) if that is needed
		if ((page.isOverflowPage() && !overflowOK) ||
			(page.getPageStatus() != BasePage.VALID_PAGE))
		{
			// unlatch releases page from cache, see StoredPage.releaseExclusive()
            page.unlatch();
			page = null;
		}

		return page;
	}

	protected void trackUnfilledPage(long pagenumber, boolean unfilled)
	{
		if (!dataFactory.isReadOnly())
			allocCache.trackUnfilledPage(pagenumber, unfilled);
	}

	/**
		Get a valid (non-deallocated or free) page in the container.
		Overflow page is OK. Resulting page is latched.

		<BR> MT - thread safe

		@exception StandardException Standard Derby error policy
	*/
	protected BasePage getPage(BaseContainerHandle handle, long pageNumber,
        boolean wait)
		 throws StandardException
	{
		return getUserPage(handle, pageNumber, true /* overflow page OK */,
            wait);
	}


	/**
		Get any old page - turn off all validation

		@exception StandardException Derby Standard error policy
	*/
	protected BasePage getAnyPage(BaseContainerHandle handle, long pageNumber) throws StandardException
	{
		// get AllocPage get a page without any validation (exception a
		// committed dropped container)

		if (getCommittedDropState()) // committed and dropped, cannot get a page
			return null;

		// make sure alloc cache has no stale info
		synchronized(allocCache)
		{
			allocCache.invalidate();
		}
		
		PageKey pageSearch = new PageKey(identity, pageNumber);
		BasePage page = (BasePage) pageCache.find(pageSearch);

		return page;
	}

    /**
     * ReCreate a page for rollforward recovery.  
     * <p>
     * During redo recovery it is possible for the system to try to redo
     * the creation of a page (ie. going from non-existence to version 0).
     * It first trys to read the page from disk, but a few different types
     * of errors can occur:
     *     o the page does not exist at all on disk, this can happen during
     *       rollforward recovery applied to a backup where the file was
     *       copied and the page was added to the file during the time frame
     *       of the backup but after the physical file was copied.
     *     o space in the file exists, but it was never initalized.  This
     *       can happen if you happen to crash at just the right moment during
     *       the allocation process.  Also
     *       on some OS's it is possible to read from a part of the file that
     *       was not ever written - resulting in garbage from the store's 
     *       point of view (often the result is all 0's).  
     *
     * All these errors are easy to recover from as the system can easily 
     * create a version 0 from scratch and write it to disk.
     *
     * Because the system does not sync allocation of data pages, it is also
     * possible at this point that whlie writing the version 0 to disk to 
     * create it we may encounter an out of disk space error (caught in this
     * routine as a StandardException from the create() call.  We can't 
     * recovery from this without help from outside, so the caught exception
     * is nested and a new exception thrown which the recovery system will
     * output to the user asking them to check their disk for space/errors.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
	protected BasePage reCreatePageForRedoRecovery(
    BaseContainerHandle handle,
    int                 pageFormat,
    long                pageNumber,
    long                pageOffset)
		 throws StandardException
	{
		// recreating a page should be done only if are in the middle of 
        // rollforward recovery or if derby.storage.patchInitPageRecoverError 
        // is set to true.

		//check if we are in rollforward recovery
		boolean rollForwardRecovery = 
			((RawTransaction)handle.getTransaction()).inRollForwardRecovery();

		if (!rollForwardRecovery && !(PropertyUtil.getSystemBoolean(
                    RawStoreFactory.PATCH_INITPAGE_RECOVER_ERROR)))
		{
			return null;
		}

		// RESOLVE: first need to verify that the page is really NOT in the
		// container!

		// no address translation necessary
		PageKey pkey = new PageKey(identity, pageNumber);

		PageCreationArgs reCreatePageArgs;

		if (pageFormat == StoredPage.FORMAT_NUMBER)
		{
            reCreatePageArgs = new PageCreationArgs(
                    pageFormat,
                    CachedPage.WRITE_SYNC,
                    pageSize,
                    spareSpace,
                    minimumRecordSize,
                    0 /* containerInfoSize - unused for StoredPage */);
		}
		else if (pageFormat == AllocPage.FORMAT_NUMBER)
		{

			// only the first allocation page have borrowed space for the
			// container info

			int containerInfoSize = 0;
			if (pageNumber == FIRST_ALLOC_PAGE_NUMBER)
			{
				containerInfoSize = CONTAINER_INFO_SIZE;
				firstAllocPageNumber = pageNumber;
				firstAllocPageOffset = pageOffset;
			}

            reCreatePageArgs = new PageCreationArgs(
                    pageFormat,
                    CachedPage.WRITE_SYNC,
                    pageSize,
                    0, // allocation page has no need for spare
                    minimumRecordSize,
                    containerInfoSize);

		}
		else
		{
			throw StandardException.newException(
                    SQLState.DATA_UNKNOWN_PAGE_FORMAT, pkey);
		}

        if (SanityManager.DEBUG) 
        {
			if (SanityManager.DEBUG_ON("LoadTran"))
				SanityManager.DEBUG_PRINT(
                    "Trace", "recreating page " + pkey + " for load tran");
        }

		// Can't just call initPage because that wants to log an initPage
		// operation, whereas we are here because of an initPage operation in
		// the log already.
		BasePage page = null;
		boolean releasePage = true;

		try
		{
            try
            {
                // a brand new page, initialize a new page in cache
                page = (BasePage) pageCache.create(pkey, reCreatePageArgs);
            }
            catch (StandardException se)
            {
                throw StandardException.newException(
                    SQLState.FILE_NEW_PAGE_DURING_RECOVERY, se, pkey);
            }

            if (page != null)
            {
                releasePage = false;
                page = latchPage(handle, page, false /* never need to wait */);

                if (page == null)
                {
                    throw StandardException.newException(
                            SQLState.FILE_NEW_PAGE_NOT_LATCHED, pkey);
                }
            }
            else
            {
                throw StandardException.newException(
                    SQLState.FILE_NEW_PAGE_DURING_RECOVERY, pkey);
            }

		}
		finally
		{
			if (releasePage && page != null)
			{
				// release the new page from cache if it errors out before 
                // the exclusive lock is set error in roll forward recovery.
                // , we are doomed anyway
				pageCache.release((Cacheable)page);
				page = null;
			}
		}

		return page;

	}


	/** 
		Get an alloc page - only accessible to the raw store 
		(container and recovery)

		@exception StandardException Derby Standard error policy
	 */
	protected BasePage getAllocPage(long pageNumber) throws StandardException 
	{
		if (getCommittedDropState()) // committed and dropped, cannot get a page
			return null;

		PageKey pageSearch = new PageKey(identity, pageNumber);
		BasePage page = (BasePage) pageCache.find(pageSearch);

		if (SanityManager.DEBUG)
		{
			if (page == null)
				SanityManager.THROWASSERT(
					"getting a null alloc page page " + 
					getIdentity() + pageNumber);

			if ( ! (page instanceof AllocPage))
				SanityManager.THROWASSERT(
					"trying to get a user page as an alloc page " + 
					getIdentity() + pageNumber); 
		}

		// assuming that allocation page lives in the page cache...
		return page;
	}

	/**
		Get only a valid, non-overflow page.  If page number is either invalid
		or overflow, returns null

		@exception StandardException Derby Standard error policy
	 */
	protected BasePage getHeadPage(BaseContainerHandle handle, long pageNumber,
        boolean wait)
		 throws StandardException
	{
		return getUserPage(handle, pageNumber, false /* overflow not ok */,
            wait);
	}

	/**
		Get the first valid page in the container

		@exception StandardException Derby Standard error policy
	 */
	protected BasePage getFirstHeadPage(BaseContainerHandle handle, boolean wait)
		 throws StandardException
	{
		return getNextHeadPage(handle, ContainerHandle.FIRST_PAGE_NUMBER-1, wait);
	}

	/**
		Get the next page in the container.
		@exception StandardException Standard Derby error policy
	*/
	protected BasePage getNextHeadPage(BaseContainerHandle handle,
        long pageNumber, boolean wait)
		 throws StandardException
	{
		long nextNumber;

		while(true)
		{
			synchronized(allocCache)
			{
				// ask the cache for the next pagenumber
				nextNumber = allocCache.getNextValidPage(handle, pageNumber, firstAllocPageNumber);
			}

			if (nextNumber == ContainerHandle.INVALID_PAGE_NUMBER)
				return null;

			// optimistically go for the next page
			BasePage p = getUserPage(handle, nextNumber,
                false /* no overflow page*/, wait);
			if (p != null)
				return p;

			pageNumber = nextNumber;
		}
	}


	private BasePage getInsertablePage(BaseContainerHandle handle,
									   long pageNumber,
									   boolean wait,
									   boolean overflowOK)
		 throws StandardException
	{
		if (pageNumber == ContainerHandle.INVALID_PAGE_NUMBER)
			return null;

		BasePage p = getUserPage(handle, pageNumber, overflowOK, wait);
		if (p != null)
		{
            // make sure the page is not too full
            if (!p.allowInsert())
            {
                p.unlatch();
                p = null;

                // it is too full, make sure we are tracking it so we won't
                // see it again.
                allocCache.trackUnfilledPage(pageNumber, false);
            }
		}
        /*
        RESOLVE track 3757
        Need to check if this fix resolves the bug.
        This is commented out because we can't conclude here that this is not
        a user page, it may just be that we failed to get a latch on the page.
        In a high contention scenario this could cause alot of relatively empty
        pages to not be considered for insert.
        TODO
        May be a good idea to move the trackUnfilledPage call below to some of
        the lines in the getUserPage method.

		else
		{
			// it is not a user page, make sure we are tracking its fillness so
			// we won't consider it as a 1/2 filled page ever
			allocCache.trackUnfilledPage(pageNumber, false);
		}
        */
		return p;
	}

    /**
     * Get candidate page to move a row for compressing the table.
     * <p>
     * The caller is moving rows from the end of the table toward the beginning,
     * with the goal of freeing up a block of empty pages at the end of the
     * container which can be returned to the OS.
     * <p>
     * On entry pageno will be latched by the caller.  Only return pages with
     * numbers below pageno.  Attempting to return pageno will result in a
     * latch/latch deadlock on the same thread.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
	protected BasePage getPageForCompress(
    BaseContainerHandle handle,
    int                 flag,
    long                pageno)
		 throws StandardException
	{
		BasePage p = null;
		boolean getLastInserted = 
            (flag & ContainerHandle.GET_PAGE_UNFILLED) == 0;

		if (getLastInserted)
		{
			// There is nothing protecting lastInsertePage from being changed
			// by another thread.  Make a local copy.
			long localLastInsertedPage = getLastInsertedPage();

            if ((localLastInsertedPage < pageno) &&
                (localLastInsertedPage != ContainerHandle.INVALID_PAGE_NUMBER))
            {
                // First try getting last inserted page.

                p = getInsertablePage(
                        handle, 
                        localLastInsertedPage,
                        true, /* wait */
                        false /* no overflow page */);

                // if localLastInsertedPage is not an insertable page, 
                // don't waste time getting it again.
                if (p == null)
                {
                    // There is a slight possibility that lastUnfilledPage and
                    // lastInsertedPage will change between the if and the
                    // assignment.  The worse that will happen is we lose the
                    // optimization.  Don't want to slow down allocation by 
                    // adding more synchronization.

                    if (localLastInsertedPage == getLastUnfilledPage())
                        setLastUnfilledPage(
                            ContainerHandle.INVALID_PAGE_NUMBER);

                    if (localLastInsertedPage == getLastInsertedPage())
                        setLastInsertedPage(
                            ContainerHandle.INVALID_PAGE_NUMBER);
                }
            }
		}
		else					
		{
            // get a relatively unfilled page that is not the last Inserted page

			long localLastUnfilledPage = getLastUnfilledPage();

			if (localLastUnfilledPage == ContainerHandle.INVALID_PAGE_NUMBER ||
                localLastUnfilledPage >= pageno ||
				localLastUnfilledPage == getLastInsertedPage())
            {
                // get an unfilled page, searching from beginning of container.
				localLastUnfilledPage = 
                    getUnfilledPageNumber(handle, 0);
            }

			if ((localLastUnfilledPage != 
                    ContainerHandle.INVALID_PAGE_NUMBER) &&
                (localLastUnfilledPage < pageno))
			{
				p = getInsertablePage(
                        handle, localLastUnfilledPage, true, false);
			}

			// return this page for insert
			if (p != null)
			{
				setLastUnfilledPage(localLastUnfilledPage);
				setLastInsertedPage(localLastUnfilledPage);
			}
		}

		return p;
    }

	/**
		Get a potentially suitable page for insert and latch it.
		@exception StandardException Standard Derby error policy
	 */
	protected BasePage getPageForInsert(BaseContainerHandle handle,
										int flag)
		 throws StandardException
	{
		BasePage p = null;
		boolean getLastInserted = (flag & ContainerHandle.GET_PAGE_UNFILLED) == 0;

		if (getLastInserted)
		{
			// There is nothing protecting lastInsertePage from being changed
			// by another thread.  Make a local copy.
			long localLastInsertedPage = getLastInsertedPage();

			if (localLastInsertedPage != ContainerHandle.INVALID_PAGE_NUMBER)
            {
                // First try getting last allocated page, NOWAIT

				p = getInsertablePage(handle, localLastInsertedPage,
									  false, /* wait */
									  false /* no overflow page */);

                if (p == null)
                {
                    // most likely we could not get the latch NOWAIT, try again
                    // with a new page, and tell the system to switch to 
                    // multi-page mode.
                    /* switchToMultiInsertPageMode(handle); */

                    localLastInsertedPage = getLastInsertedPage();

                    p = getInsertablePage(handle, localLastInsertedPage,
                                          true, /* wait */
                                          false /* no overflow page */);
                }
            }

			// if lastUnfilledPage is not an insertable page, don't waste time
			// getting it again.
			if (p == null)
			{
				// There is a slight possibility that lastUnfilledPage and
				// lastInsertedPage will change between the if and the
				// assignment.  The worse that will happen is we lose the
				// optimization.  Don't want to slow down allocation by adding
				// more synchronization.

				if (localLastInsertedPage == getLastUnfilledPage())
					setLastUnfilledPage(ContainerHandle.INVALID_PAGE_NUMBER);

				if (localLastInsertedPage == getLastInsertedPage())
					setLastInsertedPage(ContainerHandle.INVALID_PAGE_NUMBER);
			}
		}
		else					// get a relatively unfilled page that is not
		{						// the last Inserted page
			long localLastUnfilledPage = getLastUnfilledPage();

			if (localLastUnfilledPage == ContainerHandle.INVALID_PAGE_NUMBER ||
				localLastUnfilledPage == getLastInsertedPage())
				localLastUnfilledPage = getUnfilledPageNumber(handle, localLastUnfilledPage);

			if (localLastUnfilledPage != ContainerHandle.INVALID_PAGE_NUMBER)
			{
				// try the last unfilled page we found - this could be
				// different from lastInserted if the last unfilled one we
				// found does not have enough space for the insert and the
				// client wants to get a brand new page.
				p = getInsertablePage(handle, localLastUnfilledPage, true, false);

				// try again
				if (p == null)
				{
					localLastUnfilledPage = getUnfilledPageNumber(handle, localLastUnfilledPage);
					if (localLastUnfilledPage != ContainerHandle.INVALID_PAGE_NUMBER)
					{
						p = getInsertablePage(handle, localLastUnfilledPage, true,
											  false);
					}
				}
			}

			// return this page for insert
			if (p != null)
			{
				setLastUnfilledPage(localLastUnfilledPage);
				setLastInsertedPage(localLastUnfilledPage);
			}
		}

		return p;

	}


	/** 
	 *  Get a latched page. Incase of backup page Latch is necessary to 
	 *  prevent modification to the page when it is being written to the backup.
	 *  Backup process relies on latches to get consistent snap
	 *  shot of the page , user level table/page/row locks are NOT 
	 *  acquired  by the online backup mechanism.
     *
     *  @param handle the container handle used to latch the page
     *  @param pageNumber the page number of the page to get
     *  @return the latched page
	 *	@exception StandardException Standard Derby error policy
	 */
	protected BasePage getLatchedPage(BaseContainerHandle handle, 
                                        long pageNumber) 
		throws StandardException 
	{
		PageKey pageKey = new PageKey(identity, pageNumber);
		BasePage page = (BasePage) pageCache.find(pageKey);
				
		if (SanityManager.DEBUG){
			SanityManager.ASSERT(page != null, "page is not found :" + pageKey);
		}
		
        // latch the page
        page = latchPage(handle, page, true);
		
		if (SanityManager.DEBUG){
			SanityManager.ASSERT(page.isLatched(), "page is not latched:" + pageKey);
		}

		return page;
	}

	

	private long getUnfilledPageNumber(BaseContainerHandle handle, long pagenum)
		 throws StandardException
	{
		synchronized(allocCache)
		{
			return allocCache.
				getUnfilledPageNumber(handle, firstAllocPageNumber, pagenum);
		}
	}		

	/*
		Cost estimates
	*/
	/**
		<BR>MT - this routine is NOT MT-safe and clients don't need to provide
		synchronization.

		@see ContainerHandle#getEstimatedRowCount
	 */
	public long getEstimatedRowCount(int flag)
	{
		return estimatedRowCount;
	}

	/**
		@see ContainerHandle#setEstimatedRowCount
	 */
	public void setEstimatedRowCount(long count, int flag)
	{
		boolean readOnly = dataFactory.isReadOnly();

		synchronized(this)
		{
			estimatedRowCount = count;

			if (!readOnly)
				isDirty = true;
		}
	}

	/**
		Update estimated row count by page as it leaves the cache.
		The estimated row count is updated without logging!
	 */
	protected void updateEstimatedRowCount(int delta)
	{
		boolean readOnly = dataFactory.isReadOnly();

		synchronized(this)
		{
			estimatedRowCount += delta;
			if (estimatedRowCount < 0)
				estimatedRowCount = 0;

			// mark the container as dirty without bumping the container
			// version because row count changes are not logged.
			if (!readOnly)
				isDirty = true;
		}
	}


	/**
		@see ContainerHandle#getEstimatedPageCount
		@exception StandardException Standard Derby error policy
	 */
	public long getEstimatedPageCount(BaseContainerHandle handle, int flag)
		 throws StandardException 
	{
		// page count is set once per container materialization in cache

		if (estimatedPageCount < 0)
		{
			synchronized(allocCache)
			{
				estimatedPageCount = 
					allocCache.getEstimatedPageCount(handle, firstAllocPageNumber);
			}
		}

		if (SanityManager.DEBUG)
			SanityManager.ASSERT(estimatedPageCount >= 0,
								 "AllocCache returns negatie estimatedPageCount");

		return estimatedPageCount;
	}

	/*
	** Methods used solely by StoredPage
	*/

	/**
		Read a page into the supplied array.

		<BR> MT - thread safe
		@exception IOException error reading page
		@exception StandardException standard Derby error message
	*/
	protected abstract void readPage(long pageNumber, byte[] pageData)
		 throws IOException, StandardException;
	

	/**
		Write a page from the supplied array.

		<BR> MT - thread safe
		@exception IOException error writing page
		@exception StandardException Standard Derby error policy
	*/
	protected abstract void writePage(long pageNumber, byte[] pageData, boolean syncPage) 
		throws IOException, StandardException;

	/*
	 * Encryption/decryption
	 */
	/**
		Decrypts a page

		<BR>MT - MT safe.

		@exception StandardException Standard Derby error policy
	 */
	protected void decryptPage(byte[] pageData, int pageSize)
		 throws StandardException
	{
		// because all our page header looks identical, the 
		// checksum is moved to the front so that it will hopefully
		// encrypt differently from page to page
		synchronized(this)
		{
			if (encryptionBuffer == null || encryptionBuffer.length < pageSize)
				encryptionBuffer = new byte[pageSize];

			int len = dataFactory.decrypt(pageData, 0, pageSize,
										  encryptionBuffer, 0);

            if (SanityManager.DEBUG)
    			SanityManager.ASSERT(len == pageSize,
								 "Encrypted page length != page length");

			// put the checksum where it belongs
			System.arraycopy(encryptionBuffer, 8, pageData, 0, pageSize-8);
			System.arraycopy(encryptionBuffer, 0, pageData, pageSize-8, 8);
		}
	}

	/**
		Encrypts a page.

		<BR> MT - not safe, call within synchronized block and only use the
		returned byte array withing synchronized block. 

		@exception StandardException Standard Derby error policy
	 */
	protected byte[] encryptPage(byte[] pageData, 
                                 int pageSize, 
                                 byte[] encryptionBuffer,
                                 boolean newEngine)
        throws StandardException
	{
		// because all our page header looks identical, move the
		// checksum to the front so that it will hopefully encrypt
		// differently from page to page

		System.arraycopy(pageData, pageSize-8, encryptionBuffer, 0, 8);
		System.arraycopy(pageData, 0, encryptionBuffer, 8, pageSize-8);

		int len = dataFactory.encrypt(encryptionBuffer, 0, pageSize,
									  encryptionBuffer, 0, newEngine);

        if (SanityManager.DEBUG)
    		SanityManager.ASSERT(len == pageSize,
							 "Encrypted page length != page length");

		return encryptionBuffer;
	}


    /** 
     * Get encryption buffer.
     *  MT - not safe, call within synchronized block and only use the
     *  returned byte array withing synchronized block. 
     * @return byte array to be used for encryping a page.
     */
    protected byte[] getEncryptionBuffer() {

        if (encryptionBuffer == null || encryptionBuffer.length < pageSize)
			encryptionBuffer = new byte[pageSize];
        return encryptionBuffer;
    }
    
    

	/*
	 * page preallocation
	 */

	/**
		preAllocate writes out the preallocated pages to disk if necessary.

		<BR>Make sure the container is large enough and the
		pages are well formatted.  The only reason to do this is to save some
		I/O during page initialization.  Once the initPage log record is
		written, it is expected that the page really do exist and is well
		formed or recovery will fail.  However, we can gain some performance by
		writing a bunch of pages at a time rather than one at a time.

		<BR>If it doesn't make sense for the the implementation to have 
		pre-allocation, just return 0. 

		<BR>If the container is not being logged, don't actually do anything,
		just return 0.  

		@return number of successfully preallocated page, or 0 if
				no page has been preallocated

		@param lastPreallocPagenum the last preallocated page number as known
				by the allocation page
		@param preAllocSize try to preallocate this page number of pages.
				Since only the container knows how many pages are actually on
				disk, it may determine that certain number of pages that the
				allocation page thinks need to be preallocated is already
				allocated, in those case, act as if the preallocation is
				successful.
	*/
	protected abstract int preAllocate(long lastPreallocPagenum, int preAllocSize);

	/**
		Preallocate the pages - actually doing it, called by subclass only
	*/
	protected int doPreAllocatePages(long lastPreallocPagenum,
									 int preAllocSize)
	{
		if (SanityManager.DEBUG)
			SanityManager.ASSERT(!dataFactory.isReadOnly(), 
								 "how can we be Preallocating pages in a read only database?");

		// initialize and a new page in cache
        PageCreationArgs createArgs = new PageCreationArgs(
                StoredPage.FORMAT_NUMBER, // default is a stored page
                CachedPage.WRITE_NO_SYNC, // write it but no sync
                pageSize,
                spareSpace,
                minimumRecordSize,
                0 /* containerInfoSize - unused for StoredPage */);

		StoredPage page = new StoredPage();
		page.setFactory(dataFactory);

		boolean error = false;
		int count = 0;

		while(count < preAllocSize)
		{
			PageKey pkey = new PageKey(identity, 
									   lastPreallocPagenum+count+1);
			try
			{
				// create Identity will do a writePage
				page.createIdentity(pkey, createArgs);

				// if create identity somehow failed to do a write page
				if (SanityManager.DEBUG)
					SanityManager.ASSERT(!page.isDirty(),
										 "create identity failed to do a write page");

				page.clearIdentity(); // ready the page for the next loop 

			}
			catch (StandardException se)
			{
				// if something went wrong, stop and return how many we did
				// successfully 
				error = true;
			}

			if (error)
				break;

			count++;
		}

		return count;
	}

	protected int getPageSize() {
		return pageSize;
	}
	protected int getSpareSpace() {
		return spareSpace;
	}
	protected int getMinimumRecordSize() {
		return minimumRecordSize;
	}

	private synchronized void switchToMultiInsertPageMode(
    BaseContainerHandle handle)
        throws StandardException
    {
        if (lastInsertedPage.length == 1)
        {
            long last = lastInsertedPage[0];

            lastInsertedPage = new long[4];
            lastInsertedPage[0] = last;

            for (int i = 3; i > 0; i--)
            {
                Page page = addPage(handle, false);
                lastInsertedPage[i] = page.getPageNumber();
                page.unlatch();
            }
        }
    }

	/*
	 * Setting and getting lastInserted Page and lastUnfilledPage in a thead
	 * safe manner. 
	 */
	private synchronized long getLastInsertedPage()
	{
        if (lastInsertedPage.length == 1)
        {
            if (SanityManager.DEBUG)
                SanityManager.ASSERT(lastInsertedPage_index == 0);

            // optimize the usual case where no concurrent insert has kicked us
            // into multi-page mode - ie. only ONE last page.  
            return(lastInsertedPage[0]);
        }
        else
        {
            long ret = lastInsertedPage[lastInsertedPage_index++];

            if (lastInsertedPage_index > (lastInsertedPage.length - 1))
            {
                lastInsertedPage_index = 0;
            }

            return(ret);
        }
	}

	private synchronized long getLastUnfilledPage()
	{
		return lastUnfilledPage;
	}

	private synchronized void initializeLastInsertedPage(int size)
	{
        lastInsertedPage = new long[size];

        for (int i = lastInsertedPage.length - 1; i >= 0; i--)
            lastInsertedPage[i] = ContainerHandle.INVALID_PAGE_NUMBER;

        lastInsertedPage_index = 0;
	}

	private synchronized void setLastInsertedPage(long val)
	{
		lastInsertedPage[lastInsertedPage_index] = val;
	}

	private synchronized void setLastUnfilledPage(long val)
	{
		lastUnfilledPage = val;
	}



	/*
	** Hide our super-classes methods to ensure that cache management
	** is correct when the container is obtained and release.
	*/

	/**
		The container is kept by the find() in File.openContainer. 
	*/
	protected void letGo(BaseContainerHandle handle) {
		super.letGo(handle);

		containerCache.release(this);
	}

	protected BasePage latchPage(BaseContainerHandle handle, BasePage foundPage, boolean wait)
		throws StandardException {

		if (foundPage == null)
			return null;

		BasePage ret = super.latchPage(handle, foundPage, wait);
		if (ret == null) {
			// page is still cached
			pageCache.release((Cacheable) foundPage);
		}
		return ret;
	}
	


	/**
     * backup the container.
     * 
     * @param handle the container handle.
     * @param backupLocation location of the backup container. 
     * @exception StandardException Standard Derby error policy 
     */
	protected abstract void backupContainer(BaseContainerHandle handle,	
                                            String backupLocation)
	    throws StandardException;
    
    /**
     * Privileged lookup of the ContextService. Must be limited to
     * package visibility so that user code
     * can't call this entry point.
     */
    static  ContextService    getContextService()
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-6648
        return AccessController.doPrivileged
            (
             new PrivilegedAction<ContextService>()
             {
                 public ContextService run()
                 {
                     return ContextService.getFactory();
                 }
             }
             );
    }
    /**
     * Privileged module lookup. Must be private so that user code
     * can't call this entry point.
     */
    private static  Object getServiceModule( final Object serviceModule, final String factoryInterface )
    {
        return AccessController.doPrivileged
            (
             new PrivilegedAction<Object>()
             {
                 public Object run()
                 {
                     return Monitor.getServiceModule( serviceModule, factoryInterface );
                 }
             }
             );
    }

}
