/*

   Derby - Class org.apache.derby.impl.store.raw.data.TempRAFContainer

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

import org.apache.derby.impl.store.raw.data.BaseContainerHandle;
import org.apache.derby.impl.store.raw.data.BasePage;

import org.apache.derby.iapi.services.cache.Cacheable;
import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.store.raw.xact.RawTransaction;
import org.apache.derby.iapi.store.raw.ContainerHandle;
import org.apache.derby.iapi.store.raw.ContainerKey;
import org.apache.derby.iapi.store.raw.Page;
import org.apache.derby.iapi.store.raw.log.LogInstant;
import org.apache.derby.iapi.store.raw.data.RawContainerHandle;

import org.apache.derby.io.StorageFactory;
import org.apache.derby.io.StorageFile;

import java.io.IOException;

/**
	needsSync is never true - DONE
	An exception never marks the store as corrupt
	clean() does not stubbify
	preAllocate() does nothing - DONE
	getFileName() returns a file in the tmp directory - DONE
	flushAll does nothing - DONE
	file descriptor is never synced
*/
class TempRAFContainer extends RAFContainer {

	protected int inUseCount;

	TempRAFContainer(BaseDataFileFactory factory) {
		super(factory);
	}

	/**
		@exception StandardException Standard Cloudscape error policy
	*/
	public Cacheable setIdentity(Object key) throws StandardException {

		ContainerKey newIdentity = (ContainerKey) key;
		if (newIdentity.getSegmentId() != ContainerHandle.TEMPORARY_SEGMENT) {

			RAFContainer realContainer = new RAFContainer(dataFactory);
			return realContainer.setIdent(newIdentity);
		}

		return super.setIdentity(newIdentity);

	}

	/**
		@exception StandardException Standard Cloudscape error policy
	*/
	public Cacheable createIdentity(Object key, Object createParameter) throws StandardException {

		ContainerKey newIdentity = (ContainerKey) key;

		if (newIdentity.getSegmentId() != ContainerHandle.TEMPORARY_SEGMENT) {
			RAFContainer realContainer = new RAFContainer(dataFactory);
			return realContainer.createIdentity(newIdentity, createParameter);
		}

		return createIdent(newIdentity, createParameter);
	}

	/**
		@exception StandardException Standard Cloudscape error policy
	*/
	public void removeContainer(LogInstant instant, boolean leaveStub) throws StandardException
	{
		// discard all of my pages in the cache
		pageCache.discard(identity);
		
		synchronized(this) {	
			// prevent anybody from looking at this container again
			setDroppedState(true);
			setCommittedDropState(true);
			setDirty(false);
			needsSync = false;

		}

		removeFile(getFileName(identity, false, false, false));
	}

	/**
		Preallocate page.  Since we don't sync when we write page anyway, no
		need to preallocate page.
	*/
	protected int preAllocate(long lastPreallocPagenum, int preAllocSize)
	{
		return 0;
	}


	/**
		Write the page, if it's within range of the current page range of the container.
		If we do write it then don't request that it be synced.

		@exception StandardException Standard Cloudscape error policy
	*/
	protected void writePage(long pageNumber, byte[] pageData, boolean syncPage) throws IOException, StandardException {
		if (!this.getDroppedState()) {
			super.writePage(pageNumber, pageData, false);
		}
		needsSync = false;
	}

	protected StorageFile getFileName(ContainerKey identity, boolean stub,
        boolean errorOK, boolean tryAlternatePath)
	{
		return privGetFileName( identity, stub, errorOK, tryAlternatePath);
	}

	protected StorageFile privGetFileName(ContainerKey identity, boolean stub,
        boolean errorOK, boolean tryAlternatePath)
	{
		return dataFactory.storageFactory.newStorageFile( dataFactory.storageFactory.getTempDir(),
                                                    "T" + identity.getContainerId() + ".tmp");
	}

	/**
		Add a page without locking the container, only one user will be accessing this
		table at a time.

		@exception StandardException Standard Cloudscape error policy
	*/
	public Page addPage(BaseContainerHandle handle, boolean isOverflow) throws StandardException {

		BasePage newPage = newPage(handle, (RawTransaction) null, handle, isOverflow);

		if (SanityManager.DEBUG) {
			SanityManager.ASSERT(newPage.isLatched());
		}

		return newPage;
	}

	/**
		@exception StandardException Standard Cloudscape error policy
	*/
	public void truncate(BaseContainerHandle handle) throws StandardException {

		// stop anyone from writing any of my pages out
		synchronized(this)
		{
			setDroppedState(true);
			setCommittedDropState(true);
			setDirty(false);
			needsSync = false;
		}

		// discard all of my pages in the cache
		while (pageCache.discard(identity) != true)
			;

		removeFile(getFileName(identity, false, true, false));

		createIdent(identity, this);

		addPage(handle, false).unlatch();
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

		if (super.use(handle, forUpdate, droppedOK)) {
			inUseCount++;
			return true;
		}

		return false;
	}

	/**
		Discontinue use of this container. Note that the unlockContainer
		call made from this method may not release any locks. The container
		lock may be held until the end of the transaction.

	*/
	protected void letGo(BaseContainerHandle handle) {

		inUseCount--;
		super.letGo(handle);
	}


	/**
		Returns true if only a single handle is connected to this container.
	*/
	public boolean isSingleUser() {
		return inUseCount == 1;
	}
}
