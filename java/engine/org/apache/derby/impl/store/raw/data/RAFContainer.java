/*

   Derby - Class org.apache.derby.impl.store.raw.data.RAFContainer

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
import org.apache.derby.impl.store.raw.data.BaseContainer;
import org.apache.derby.impl.store.raw.data.BaseContainerHandle;
import org.apache.derby.impl.store.raw.data.BasePage;

import org.apache.derby.iapi.services.cache.Cacheable;
import org.apache.derby.iapi.services.context.ContextService;
import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.iapi.services.diag.Performance;
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.services.io.FormatIdUtil;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.store.raw.ContainerHandle;
import org.apache.derby.iapi.store.raw.ContainerKey;
import org.apache.derby.iapi.store.raw.Loggable;
import org.apache.derby.iapi.store.raw.log.LogInstant;
import org.apache.derby.iapi.store.raw.xact.RawTransaction;

import org.apache.derby.io.StorageFactory;
import org.apache.derby.io.WritableStorageFactory;
import org.apache.derby.io.StorageFile;
import org.apache.derby.io.StorageRandomAccessFile;

import java.util.Vector;

import java.io.DataInput;
import java.io.IOException;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;
import java.security.PrivilegedActionException;

/**
	RAFContainer (short for RandomAccessFileContainer) is a concrete subclass of FileContainer
	for FileContainers which are implemented on java.io.RandomAccessFile.
*/

public class RAFContainer extends FileContainer implements PrivilegedExceptionAction
{

	/*
	 * Immutable fields
	 */
	protected StorageRandomAccessFile fileData;

	/* 
	** Mutable fields, only valid when the identity is valid.
	*/
	protected boolean			needsSync;

    /* privileged actions */
    private int actionCode;
    private static final int GET_FILE_NAME_ACTION = 1;
    private static final int CREATE_CONTAINER_ACTION = 2;
    private static final int REMOVE_FILE_ACTION = 3;
    private static final int OPEN_CONTAINER_ACTION = 4;
    private static final int STUBBIFY_ACTION = 5;
    private ContainerKey actionIdentity;
    private boolean actionStub;
    private boolean actionErrorOK;
    private boolean actionTryAlternatePath;
    private StorageFile actionFile;
    private LogInstant actionInstant;
    
	/*
	 * Constructors
	 */

	protected RAFContainer(BaseDataFileFactory factory) {
		super(factory);
	}

	/*
	** Methods overriding super-class
	*/

	synchronized public boolean isDirty() {
		return super.isDirty() || needsSync;
	}

	/*
	** Methods of Cacheable
	*/

	/**
		Set container's identity
		@exception StandardException Standard Cloudscape error policy
	*/
	public Cacheable setIdentity(Object key) throws StandardException {

		ContainerKey newIdentity = (ContainerKey) key;

		// if this is an open for a temp container then return an object of that type
		if (newIdentity.getSegmentId() == ContainerHandle.TEMPORARY_SEGMENT) {

			TempRAFContainer tmpContainer = new TempRAFContainer(dataFactory);
			return tmpContainer.setIdent(newIdentity);
		}

		return setIdent(newIdentity);
	}

	/**
		@exception StandardException Standard Cloudscape error policy
	 */
	public Cacheable createIdentity(Object key, Object createParameter) throws StandardException {

		ContainerKey newIdentity = (ContainerKey) key;

		if (newIdentity.getSegmentId() == ContainerHandle.TEMPORARY_SEGMENT) {
			TempRAFContainer tmpContainer = new TempRAFContainer(dataFactory);
			return tmpContainer.createIdent(newIdentity, createParameter);
		}

		return createIdent(newIdentity, createParameter);
	}


	/*
	** Container creation, opening, and closing
	*/

	/**
		Remove the container

		@exception StandardException Standard Cloudscape error policy
	*/
	protected void removeContainer(LogInstant instant, boolean leaveStub)
		 throws StandardException
	{
		// discard all of my pages in the cache
		pageCache.discard(identity);
		stubbify(instant);

		// RESOLVE: leaveStub false
	}

	protected final void closeContainer() {

		if (fileData != null) {
			try {
				fileData.close();
			} catch (IOException ioe) {
			} finally {

				fileData = null;
			}
		}
	}


	/*
	** Methods used solely by StoredPage
	*/

	/**
		Read a page into the supplied array.

		<BR> MT - thread safe
		@exception IOException exception reading page
		@exception StandardException Standard Cloudscape error policy
	*/
	protected void readPage(long pageNumber, byte[] pageData)
		 throws IOException, StandardException
	{
		if (SanityManager.DEBUG) {
			SanityManager.ASSERT(!getCommittedDropState());
		}

		long pageOffset = pageNumber * pageSize;

		synchronized (this) {

			fileData.seek(pageOffset);

			fileData.readFully(pageData, 0, pageSize);
		}

		if (dataFactory.databaseEncrypted() &&
			pageNumber != FIRST_ALLOC_PAGE_NUMBER)
		{
			decryptPage(pageData, pageSize);
		}
	}

	/**
		Write a page from the supplied array.

		<BR> MT - thread safe

		@exception StandardException Standard Cloudscape error policy
		@exception IOException IO error accessing page
	*/
	protected void writePage(long pageNumber, byte[] pageData, boolean syncPage)
		 throws IOException, StandardException
	{
		synchronized(this)
		{

			// committed and dropped, do nothing.
			// This file container may only be a stub
			if (getCommittedDropState())
				return;

			if (pageNumber == FIRST_ALLOC_PAGE_NUMBER)
			{
				// write header into the alloc page array regardless of dirty
				// bit because the alloc page have zero'ed out the borrowed
				// space
				writeHeader(pageData);

				if (SanityManager.DEBUG) {
					if (FormatIdUtil.readFormatIdInteger(pageData) != AllocPage.FORMAT_NUMBER)
						SanityManager.THROWASSERT(
							"expect " +
							AllocPage.FORMAT_NUMBER +
							"got " +
							FormatIdUtil.readFormatIdInteger(pageData));
				}

			}

		///////////////////////////////////////////////////
		//
		// RESOLVE: right now, no logical -> physical mapping.
		// We can calculate the offset.  In the future, we may need to
		// look at the allocation page or the in memory translation table
		// to figure out where the page should go
		//
		/////////////////////////////////////////////////

			long pageOffset = pageNumber * pageSize;

			try
			{
				fileData.seek(pageOffset);

				/**
					On EPOC (www.symbian.com) a seek beyond the end of
					a file just moves the file pointer to the end of the file.

				*/
				if (fileData.getFilePointer() != pageOffset)
					padFile(fileData, pageOffset);

				byte[] dataToWrite;

				if (dataFactory.databaseEncrypted() 
					&& pageNumber != FIRST_ALLOC_PAGE_NUMBER)
				{
					// We cannot encrypt the page in place because pageData is
					// still being accessed as clear text.  The encryption
					// buffer is shared by all who access this container and can
					// only be used within the synchronized block.
					dataToWrite = encryptPage(pageData, pageSize);
				} else {
					dataToWrite = pageData;
				}

				dataFactory.writeInProgress();
				try
				{
					fileData.write(dataToWrite, 0, pageSize);
				}
				finally
				{
					dataFactory.writeFinished();
				}
			}
			catch (IOException ioe)
			{
				// On some platforms, if we seek beyond the end of file, or try
				// to write beyond the end of file (not appending to it, but
				// skipping some bytes), it will give IOException.
				// Try writing zeros from the current end of file to pageOffset
				// and see if we can then do the seek/write.  The difference
				// between pageOffset and current end of file is almost always
				// going to be the multiple of pageSize

				if (!padFile(fileData, pageOffset))
					throw ioe;	// not writing beyond EOF, rethrow exception

				if (SanityManager.DEBUG)
					SanityManager.ASSERT(fileData.length() >= pageOffset,
										 "failed to blank filled missing pages");
				fileData.seek(pageOffset);
				dataFactory.writeInProgress();
				try
				{
					fileData.write(pageData, 0, pageSize);
				}
				finally
				{
					dataFactory.writeFinished();
				}
			}

			if (syncPage)
			{
				dataFactory.writeInProgress();
				try
				{
                    if (Performance.MEASURE)
                    {
                        if (!dataFactory.dataNotSyncedAtAllocation)
                            fileData.sync( false);
                    }
                    else
                    {
                        fileData.sync( false);
                    }
				}
				finally
				{
					dataFactory.writeFinished();
				}
			}
			else
            {
				needsSync = true;
            }
		}

	}

	/**
		Pad the file upto the passed in page offset.
		Returns true if the file needed padding.
	*/

	private boolean padFile(StorageRandomAccessFile file, long pageOffset)
		throws IOException, StandardException {

		long currentEOF = file.length();
		if (currentEOF >= pageOffset)
			return false;

		// all objects in java are by definition initialized
		byte zero[] = new byte[pageSize];

		file.seek(currentEOF);

		while(currentEOF < pageOffset)
		{
			dataFactory.writeInProgress();
			try
			{
				long len = pageOffset - currentEOF;
				if (len > pageSize)
					len = pageSize;

				file.write(zero, 0, (int) len);
			}
			finally
			{
				dataFactory.writeFinished();
			}
			currentEOF += pageSize;
		}

		return true;
	}

	/**
		@exception StandardException Standard Cloudscape error policy
	*/
	public void clean(boolean forRemove) throws StandardException
	{
		boolean waited = false;

		synchronized (this) {

			// committed and dropped, do nothing.  
			// This file container has already been stubbified
			if (getCommittedDropState()) {
				clearDirty();
				return;
			}

			// The container is about to change, need to wait till it is really
			// changed.  We are in the predirty state only for the duration
			// where the log record that changed the container has been sent to
			// the log and before the change actually happened.
			while(preDirty == true)
			{
				waited = true;
				try
				{
					wait();
				}
				catch (InterruptedException ie)
				{
					throw StandardException.interrupt(ie);
				}
			}

			if (waited)
			{
				// someone else may have stubbified this while we waited 
				if (getCommittedDropState())
				{
					clearDirty();
					return;
				}
			}


			if (forRemove) {

				//				removeFile()
				//				clearDirty();

			} else if (isDirty()) {
 
				try {

					// Cannot get the alloc page and write it out
					// because in order to do so, the alloc page will need to 
					// find this container object.  But this container object
					// is in the middle of being cleaned and may not be 
					// 'found' and we will hang.
					//
					// Instead, just clobber the container info, which is 
					// checksum'ed seperately from the alloc page
					//
					writeRAFHeader(fileData,
								   false,  // don't create, container exists 
								   true);  // syncfile

					clearDirty();

				} catch (IOException ioe) {

					throw dataFactory.markCorrupt(
                        StandardException.newException(
                            SQLState.FILE_CONTAINER_EXCEPTION, ioe, this));
				}
			}
		}
	}

	private void clearDirty() {
		isDirty = false;
		needsSync = false;
	}


	/**
		Preallocate some pages if need be
	*/
	protected int preAllocate(long lastPreallocPagenum, 
							  int preAllocSize)
	{  
	
		/* we had a condition here , that looks at the file size before
		 * preallocation to handle the optimization cases like , we 
		 * preallocated the space and then crashed, as we don;t log the 
		 * preallocated length, we don't have updated value until AlocExtent
		 * page get flushed to the disk. only way to find out that the pages
		 * we want already exist  is to look at the file length.
		 * Althought it was nice thing to do, we had bug no: 3813 from
		 * customer , who for some unexplainable reasons he gets lots of
		 * junk at the end of the file. As junk is not initialized with
		 * format-ID , we get into recovery problem.
		 * To avoid such unforseen conditions, removed the file size check 
		 * condition , as it is better not to fail in recovery than 
		 * losing some special case performance improvement.
		 */
  
		int n = doPreAllocatePages(lastPreallocPagenum, preAllocSize); 

		if (n > 0)				// sync the file
		{
			synchronized(this)
			{
				boolean inwrite = false;
				try
				{
					dataFactory.writeInProgress();
					inwrite = true;

                    if (Performance.MEASURE)
                    {
                        if (!dataFactory.dataNotSyncedAtAllocation)
                            fileData.sync( false);
                    }
                    else
                    {
                        fileData.sync( false);
                    }
				}
				catch (IOException ioe)
				{
					// The disk may have run out of space. 
					// Don't error out in pre-allocation since the user may not
					// actually need this page.
					n = 0;
				}
				catch (StandardException se)
				{
					// some problem calling writeInProgress
					n = 0;
				}
				finally
				{
					if (inwrite)
						dataFactory.writeFinished();
				}
			}
		}

		return n;
	}


	/*
		Write the header of a random access file and sync it
		@param create if true, the container is being created
				if false, the container already exist
		@param syncFile if true, sync the file
	*/
	private void writeRAFHeader(StorageRandomAccessFile file, boolean create, 
								boolean syncFile) 
		 throws IOException, StandardException
	{
		byte[] epage;
		if (create)
		{
			// the file doesn't exist yet, get an embryonic page buffer
			epage = getEmbryonicPage((DataInput)null); 
		}
		else
		{
			file.seek(FIRST_ALLOC_PAGE_OFFSET);
			epage = getEmbryonicPage(file);
		}

		// need to check for frozen state


		file.seek(FIRST_ALLOC_PAGE_OFFSET);
		writeHeader(file, create, epage);

		// leave the end of the file at a page boundry. This
		// is to work around bugs in the EPOC jvm where a seek
		// beyond the end of a file does not throw an exception
		// but just moves the offset to the end of the file. This only
		// occurs when the second page is written after the header has
		// been written, ending up with the page at the incorrect offset.
		if (create) {
			padFile(file, pageSize);
		}

		if (syncFile)
		{
			dataFactory.writeInProgress();
			try
			{
                if (Performance.MEASURE)
                {
                    if (!dataFactory.dataNotSyncedAtCheckpoint)
                        file.sync( false);
                }
                else
                {
                    file.sync( false);
                }
			}
			finally
			{
				dataFactory.writeFinished();
			}
		}

		epage = null;
	}

	/**
		flush the cache to ensure all of my pages are written to disk

		@exception StandardException Standard Cloudscape error policy
	*/
	protected void flushAll() throws StandardException {

		pageCache.clean(identity);

		// now clean myself which will sync all my pages.
		clean(false);
	}


	 protected synchronized StorageFile getFileName(ContainerKey identity, boolean stub,
											 boolean errorOK, boolean tryAlternatePath)
		 throws StandardException
	 {
         // RESOLVE - READ ONLY

         actionCode = GET_FILE_NAME_ACTION;
         actionIdentity = identity;
         actionStub = stub;
         actionErrorOK = errorOK;
         actionTryAlternatePath = tryAlternatePath;
         try
         {
             return (StorageFile) AccessController.doPrivileged( this);
         }
         catch( PrivilegedActionException pae){ throw (StandardException) pae.getException();}
         finally{ actionIdentity = null; }
	 }

    protected StorageFile privGetFileName(ContainerKey identity, boolean stub,
                                    boolean errorOK, boolean tryAlternatePath)
        throws StandardException
    {
        StorageFile container = dataFactory.getContainerPath( identity, stub);

        // retry with small case 'c' and 'd'
        // bug fix for track 3444
        if (!container.exists() && tryAlternatePath)
        {
            container = dataFactory.getAlternateContainerPath( identity, stub);
        }

        if (!container.exists()) {

            StorageFile directory = container.getParentDir();

            if (!directory.exists())
            {
                // make sure only 1 thread can create a segment at one time
                synchronized(dataFactory)
                {
                    if (!directory.exists())
                    {
                        if (!directory.mkdirs())
                        {
                            if (errorOK)
                            {
                                return null;
                            }
                            else
                            {
                                throw StandardException.newException(
                                    SQLState.FILE_CANNOT_CREATE_SEGMENT,
                                    directory);
                            }
                        }
                    }
                }
            }
        }

        return container;
    } // end of privGetFileName


	protected synchronized void createContainer(ContainerKey newIdentity)
        throws StandardException
    {

		if (SanityManager.DEBUG) {
			if ((spareSpace < 0) || (spareSpace > 100))
				SanityManager.THROWASSERT("invalid spare space " + spareSpace);
		}

        actionCode = CREATE_CONTAINER_ACTION;
        actionIdentity = newIdentity;
        try
        {
            AccessController.doPrivileged( this);
        }
        catch( PrivilegedActionException pae){ throw (StandardException) pae.getException();}
        finally{ actionIdentity = null; }
    } // end of createContainer

	protected synchronized boolean removeFile(StorageFile file)
        throws SecurityException, StandardException
    {
        actionCode = REMOVE_FILE_ACTION;
        actionFile = file;
        try
        {
            return AccessController.doPrivileged( this) != null;
        }
        catch( PrivilegedActionException pae){ throw (StandardException) pae.getException();}
        finally{ actionFile = null; }
    } // end of removeFile

    private boolean privRemoveFile(StorageFile file)
        throws StandardException
    {
		closeContainer();

		dataFactory.writeInProgress();
		try
		{
            if( file.exists())
                return file.delete();
		}
		finally
		{
			dataFactory.writeFinished();
		}

		return true;
    } // end of privRemoveFile

	protected synchronized boolean openContainer(ContainerKey newIdentity)
        throws StandardException
    {
        actionCode = OPEN_CONTAINER_ACTION;
        actionIdentity = newIdentity;
        try
        {
            return AccessController.doPrivileged( this) != null;
        }
        catch( PrivilegedActionException pae){ throw (StandardException) pae.getException();}
        finally{ actionIdentity = null; }
    }

	protected synchronized void stubbify(LogInstant instant)
        throws StandardException
	{
         // update header, synchronized this in case the cache is cleaning
         // this container at the same time.  Make sure the clean and
         // stubbify is mutually exclusive.
         setDroppedState(true);
         setCommittedDropState(true);

		 // The whole container should be shrunk into a 'stub'.
		 // If the file system supports truncation, we can just truncate the
		 // file after the header.  Since it doesn't, we need to write out a
		 // seperate file (the stub), then reset fileData to point to that,
		 // then remove the current file.
		 //
		 // There may still be dirty pages that belongs to this file which are
		 // still in the page cache.  They need not really
		 // be written since they don't really exist anymore
		 //
		 // there are 3 pieces of information on disk :
		 // 1) the log operation that caused this file to be stubbified
		 // 2) the stub
		 // 3) the file
		 //
		 // The order of event, as far as persisent store is concerned, is
		 // A) stub shows up
		 // B) the file disappear
		 // C) the log operation got flushed
		 // (B and C may swap order)
		 //
		 // If neither A or B happens (we crashed before the sync call),
		 // then nothing happened.
		 //
		 // if A happened but B and C did not, then when we recover, we will not
		 // know the file has been stubbified.  Hopefully, it will be stubbified
		 // again if the post-commit queue manager is alerted to the fact.
		 //
		 // if A and B happened but C did not, then the file is stubbified but
		 // there is no log record to indicate that.  This is undesirable but
		 // still safe because the only time we stubbify is on a post commit
		 // operation, i.e., either a create container has rolled back or a
		 // dropped container has committed.  We end up having a a container
		 // stub which behaves the same as a dropped container - only that all
		 // the redo work is unnecessary because we 'know' it will
		 // eventually be dropped and committed.
		 //
		 // If A and C happened and not B, then during redo, this stubbify
		 // routine will be called again and the file will be deleted again
		 //
		 // The reason why A has to be sync'ed out is that we don't want B to
		 // happen but A did not and the system crashed.  Then we are left
		 // with neither the file nor the stub and maybe even no log record.
		 // Then the system is not recoverable.

		actionIdentity = (ContainerKey)getIdentity();
        actionInstant = instant;
        actionCode = STUBBIFY_ACTION;
        try
        {
            AccessController.doPrivileged( this);
        }
        catch( PrivilegedActionException pae){ throw (StandardException) pae.getException();}
        finally
        {
            actionIdentity = null;
            actionInstant = null;
        }
    }

     // PrivilegedExceptionAction method
     public Object run() throws StandardException
     {
         switch( actionCode)
         {
         case GET_FILE_NAME_ACTION:
             return privGetFileName( actionIdentity, actionStub, actionErrorOK, actionTryAlternatePath);

         case CREATE_CONTAINER_ACTION:
         {
             StorageFile file = privGetFileName( actionIdentity, false, false, false);

             try {
                 if (file.exists()) {
                     // note I'm left in the no-identity state as fillInIdentity()
                     // hasn't been called.
                     throw StandardException.newException( SQLState.FILE_EXISTS, file);
                 }
             } catch (SecurityException se) {
                 throw StandardException.newException( SQLState.FILE_CREATE, se, file);
             }

             try {

                 // OK not to force WAL here, in fact, this operation preceeds the
                 // creation of the log record to ensure sufficient space.

                 dataFactory.writeInProgress();
                 try
                 {
                     fileData = file.getRandomAccessFile( "rw");
                 }
                 finally
                 {
                     dataFactory.writeFinished();
                 }

                 // This container format specifies that the first page is an
                 // allocation page and the container information is stored within
                 // it.  The allocation page needs to be somewhat formatted
                 // because if the system crashed after the create container log
                 // operation is written, it needs to be well formed enough to get
                 // the container information back out of it.
                 //
                 // Don't try to go thru the page cache here because the container
                 // object cannot be found in the container cache at this point
                 // yet.  However, if we use the page cache to store the first
                 // allocation page, then in order to write itself out, it needs to
                 // ask the container to do so, which is going to create a
                 // deadlock.  The allocation page cannot write itself out without
                 // going thru the container because it doesn't know where its
                 // offset is.  Here we effectively hardwired page 0 at offset 0 of
                 // the container file to be the first allocation page.

                 // create an embryonic page - if this is not a temporary container,
                 // synchronously write out the file header.
                 writeRAFHeader(fileData, true,
                                (actionIdentity.getSegmentId() != ContainerHandle.TEMPORARY_SEGMENT));

             } catch (SecurityException se) {

                 // only thrown by the RandomeAccessFile constructor,
                 // so the file won't exist
                 throw StandardException.newException( SQLState.FILE_CREATE, se, file);

             } catch (IOException ioe) {

                 boolean fileDeleted;
                 try {
                     fileDeleted = privRemoveFile(file);
                 } catch (SecurityException se) {
                     throw StandardException.newException( SQLState.FILE_CREATE_NO_CLEANUP, ioe, file, se);
                 }

                 if (!fileDeleted) {
                     throw StandardException.newException( SQLState.FILE_CREATE_NO_CLEANUP, ioe, file, null);
                 }

                 throw StandardException.newException( SQLState.FILE_CREATE, ioe, file);
             }

             canUpdate = true;
             return null;
         } // end of case CREATE_CONTAINER_ACTION

         case REMOVE_FILE_ACTION:
             return privRemoveFile( actionFile) ? this : null;

         case OPEN_CONTAINER_ACTION:
         {
             boolean isStub = false;	// is this a stub?

             StorageFile file = privGetFileName( actionIdentity, false, true, true);
             if (file == null)
                 return null;

             try {
                 if (!file.exists()) {

                     // file does not exist, may be it has been stubbified
                     file = privGetFileName( actionIdentity, true, true, true);
                     if (!file.exists())
                         return null;
                     isStub = true;
                 }
             } catch (SecurityException se) {
                 throw StandardException.newException(
                     SQLState.DATA_UNEXPECTED_EXCEPTION, se);
             }

             canUpdate = false;
             try {
                 if (!dataFactory.isReadOnly() && file.canWrite())
                     canUpdate = true;
             } catch (SecurityException se) {
                 // just means we can't write to it.
             }

             try {

                 fileData = file.getRandomAccessFile(canUpdate ? "rw" : "r");
                 fileData.seek(FIRST_ALLOC_PAGE_OFFSET);
                 readHeader(fileData);

                 if (SanityManager.DEBUG)
                 {
                     if (isStub)
                         SanityManager.ASSERT(getDroppedState() && getCommittedDropState(),
                                              "a stub failed to set drop state");
                 }

             } catch (IOException ioe) {

                 if (isStub)
                 {
                     throw dataFactory.markCorrupt(
                         StandardException.newException(
                             SQLState.FILE_CONTAINER_EXCEPTION, ioe, this, ioe));
                 }

                 // maybe it is being stubbified... try that
                 StorageFile stub = privGetFileName( actionIdentity, true, true, true);
                 if (stub.exists())
                 {
                     try
                     {
                         if (SanityManager.DEBUG)
                             SanityManager.DEBUG_PRINT("RAFContainer",
                                                       "removing file because we opened it while it is being stubbified");

                         privRemoveFile(file);

                         fileData = stub.getRandomAccessFile(canUpdate ? "rw" : "r");
                         readHeader(fileData);
                     }
                     catch (IOException ioe2)
                     {
                         throw dataFactory.markCorrupt(
                             StandardException.newException(
                                 SQLState.FILE_CONTAINER_EXCEPTION, ioe2, this, ioe2));
                     }

                     // RESOLVE: this is a temporary hack

                 }
                 else
                     throw dataFactory.markCorrupt(
                         StandardException.newException(
                             SQLState.FILE_CONTAINER_EXCEPTION, ioe, this, ioe));
             }

             return this;
         } // end of case OPEN_CONTAINER_ACTION

         case STUBBIFY_ACTION:
         {
             StorageFile file = privGetFileName( actionIdentity, false, false, true);
             StorageFile stub = privGetFileName( actionIdentity, true, false, false);

             StorageRandomAccessFile stubData = null;

             try
             {
                 // !!!!!
                 // bumpContainerVersion();
                 //
                 // do NOT bump the container version.  We WANT the stubbify
                 // operation to get redone every time.  This is because this
                 // operation first writes out the stub and then remove the
                 // container file.  If we bump the version, then the stub will
                 // contain the new version.  And if the system crashes right then,
                 // then we will skip the whole operation during redo even though
                 // the container file may not have been removed.  Since we don't
                 // want to have the remove happen before the stub is written, we
                 // cannot sync it and therefore cannot be sure the remove
                 // happened before the system crashed.

                 if (!stub.exists())
                 {
                     // write the header to the stub
                     stubData = stub.getRandomAccessFile( "rw");

                     writeRAFHeader(stubData,
                                    true, /* create */
                                    true); /* sync */

                     stubData.close();
                     stubData = null;
                 }


                 // Force WAL and check for database corruption before removing file.
                 // This is one operation where the container is changed on disk
                 // directly without going thru the container cache, which otherwise
                 // would have force WAL.  Take care of it here.
                 dataFactory.flush(actionInstant);

                 // try to remove the container file
                 // fileDate is not null only if we are redoing a removeContainer
                 // (stubbify) operation.  Then fileData acutally is opened against
                 // the stub and the original container file does not exist.
                 // Then we need to close it here because this method is called by
                 // cache.remove and nobody will be able to see fileData after this.
                 privRemoveFile(file);

             }
             catch (SecurityException se)
             {
                 throw StandardException.newException(
                     SQLState.FILE_CANNOT_REMOVE_FILE, se, file, se);
             }
             catch (IOException ioe)
             {
                 // exception thrown while in creating the stub.  Remove the
                 // (half-baked) stub
                 try
                 {
                     if (stubData != null)
                     {
                         stubData.close();
                         stub.delete();
                         stubData = null;
                     }

                     if (fileData != null)
                     {
                         fileData.close();
                         fileData = null;
                     }
                 }
                 catch (IOException ioe2)
                 {
                     throw StandardException.newException(
                         SQLState.FILE_CANNOT_REMOVE_FILE, ioe2, file, ioe);
                 }
                 catch (SecurityException se)
                 {
                     throw StandardException.newException(
                         SQLState.FILE_CANNOT_REMOVE_FILE, se, file, stub);
                 }
             }
	
             //let the data factory know about this the stub file;It
             // could  remove when next checkpoint occurs if it's not necessary for recovery
             dataFactory.stubFileToRemoveAfterCheckPoint(stub,actionInstant, getIdentity());
             return null;
         } // end of case STUBBIFY_ACTION
         }
         return null;
     } // end of run
}
