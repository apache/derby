/*

   Derby - Class org.apache.derby.iapi.store.raw.data.DataFactory

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

package org.apache.derby.iapi.store.raw.data;

import org.apache.derby.iapi.store.raw.ContainerKey;

import org.apache.derby.iapi.services.daemon.Serviceable;
import org.apache.derby.iapi.services.context.ContextManager;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.store.raw.ContainerHandle;
import org.apache.derby.iapi.store.raw.Corruptable;
import org.apache.derby.iapi.store.raw.LockingPolicy;
import org.apache.derby.iapi.store.raw.RawStoreFactory;
import org.apache.derby.iapi.store.raw.RecordHandle;
import org.apache.derby.iapi.store.raw.StreamContainerHandle;
import org.apache.derby.iapi.store.raw.xact.RawTransaction;

import org.apache.derby.io.StorageFactory;

import org.apache.derby.iapi.store.access.FileResource;
import org.apache.derby.iapi.store.access.RowSource;
import org.apache.derby.iapi.store.raw.log.LogInstant;
import org.apache.derby.iapi.util.ByteArray;
import org.apache.derby.catalog.UUID;

import java.util.Properties;

public interface DataFactory extends Corruptable {

	public static final String MODULE = "org.apache.derby.iapi.store.raw.data.DataFactory";

	/**
		The temporary segment is called "tmp"
	 */
	public static final String TEMP_SEGMENT_NAME = "tmp";

	/**
		The database lock
	 */
	public static final String DB_LOCKFILE_NAME = "db.lck";

	/*
	** file name that is used to acquire exclusive lock on DB.
	**/
	public static final String DB_EX_LOCKFILE_NAME = "dbex.lck";

	/**
		Is the store read-only.
	*/
	public boolean isReadOnly();


	/**
		Open a container that is not droped.

		@param t the raw transaction that is opening the container
		@param containerId the container's identity
		@param locking the locking policy
		@param mode see the different mode in @see ContainerHandle
		@param droppedOK if true, then the container may be dropped.  If false,
		then will return a null handle if the container is dropped.

		@return the handle to the opened container
		@exception StandardException Standard Cloudscape error policy

	 */
	public ContainerHandle openContainer(RawTransaction t,
										 ContainerKey containerId,
										 LockingPolicy locking,
										 int mode)
		 throws StandardException;

	/**
		Open a container that may have been dropped.
		Only internal raw store code should call this, e.g. recovery.

		@see #openContainer
		@exception StandardException Standard Cloudscape error policy
	*/
	public RawContainerHandle openDroppedContainer(RawTransaction t,
												   ContainerKey containerId,
												   LockingPolicy locking,
												   int mode)
		throws StandardException;

	/**
		Add a container.

		@param t the transaction that is creating the container
		@param segmentId the segment where the container is to go
		@param mode whether or not to LOGGED or not.  The effect of this mode
				is only for this addContainer call, not persisently stored
				throughout the lifetime of the container
		@param tablePropertes properties of the container that is persistently
				stored throughout the lifetime of the container

		@return the containerId of the newly created container

		@exception StandardException Standard Cloudscape Error policy

	 */
	public long addContainer(
    RawTransaction  t,
    long            segmentId,
    long            containerid,
    int             mode,
    Properties      tableProperties,
    int             temporaryFlag)
		throws StandardException;

	/**
		Create and load a stream container.

		@param t the transaction that is creating the container
		@param segmentId the segment where the container is to go
		@param tablePropertes properties of the container that is persistently
				stored throughout the lifetime of the container
		@param rowSource the data to load the container with

		@return the containerId of the newly created stream container

		@exception StandardException Standard Cloudscape Error policy

	 */
	public long addAndLoadStreamContainer(RawTransaction t, long segmentId,
			Properties tableProperties, RowSource rowSource)
		 throws StandardException;

	/**
		Open a stream container.

		@return a valid StreamContainerHandle or null if the container does not exist.

		@exception StandardException  Standard cloudscape exception policy

	*/
	public StreamContainerHandle openStreamContainer(
    RawTransaction  t,
    long            segmentId,
    long            containerId,
    boolean         hold)
		throws StandardException;

	/**
		Drop and remove a stream container.

		@exception StandardException  Standard cloudscape exception policy
	*/
	public void dropStreamContainer(RawTransaction t, long segmentId, long containerId)
		throws StandardException;

	/**
		re-Create a container during recovery load tran.

		@exception StandardException Standard Cloudscape Error policy
	 */
	public void reCreateContainerForLoadTran(RawTransaction t,
			long segmentId, long containerId, ByteArray containerInfo)
		 throws StandardException;


	public void dropContainer(RawTransaction t, ContainerKey containerId)
		throws StandardException;

	public void checkpoint() throws StandardException;

	public void idle() throws StandardException;

	/**
		Return the identifier that uniquely identifies this raw store at runtime.
		This identifier is to be used as part of the lokcing key for objects
		locked in the raw store by value (e.g. Containers).
	*/
	public UUID getIdentifier();

	/**
		make data factory aware of which raw store factory it belongs to
		Also need to boot the LogFactory

		@exception StandardException cannot boot the log factory
	*/
	public void setRawStoreFactory(RawStoreFactory rsf, boolean create,
								   Properties properties)
		 throws StandardException ;

	/**
		Return a record handle that is initialized to the given page number and
        record id.

		@exception StandardException Standard cloudscape exception policy.

		@param segmentId    segment where the RecordHandle belongs.
		@param containerId  container where the RecordHandle belongs.
		@param pageNumber   the page number of the RecordHandle.
		@param recordId     the record id of the RecordHandle.

		@see RecordHandle
	*/
//	public RecordHandle makeRecordHandle(long segmentId, long containerId, long pageNumber, int recordId)
//		 throws	StandardException;

	/**
		Database creation finished

		@exception StandardException Standard cloudscape exception policy.
	*/
	public void createFinished() throws StandardException;

	/**
		Get an object to handle non-transactional files.
	*/
	public FileResource getFileHandler();

	/**
		Tell the data factory it is OK to remove committed deleted containers
		when the data factory shuts down.
	 */
	public void removeStubsOK();

	/**
		Get cache statistics for the specified cache
	*/
	public long[] getCacheStats(String cacheName);

	/**
		Reset the cache statistics for the specified cache
	*/
	public void resetCacheStats(String cacheName);

	/**
		Reclaim space used by this factory.  Called by post commit daemon.
		@exception StandardException  Standard cloudscape exception policy
	*/
	public int reclaimSpace(Serviceable work, ContextManager contextMgr)
		 throws StandardException;

	/**
		Called after recovery is performed.

		@exception StandardException Standard Cloudscape Error Policy
	*/
	public void postRecovery() throws StandardException;

	/**
		Encrypt cleartext into ciphertext.

		@see org.apache.derby.iapi.services.crypto.CipherProvider#encrypt
		@exception StandardException Standard Cloudscape Error Policy
	 */
	public int encrypt(byte[] cleartext, int offset, int length,
					   byte[] ciphertext, int outputOffset)
		 throws StandardException ;

	/**
		Decrypt cleartext from ciphertext.

		@see org.apache.derby.iapi.services.crypto.CipherProvider#decrypt
		@exception StandardException Standard Cloudscape Error Policy
	 */
	public int decrypt(byte[] ciphertext, int offset, int length,
					   byte[] cleartext, int outputOffset)
		 throws StandardException ;

	/**
		Return the encryption block size used by the algorithm at time of
		encrypted database creation
	 */
	public int getEncryptionBlockSize();

	/**
	 * Backup restore - stop writing dirty pages or container to disk
	 * @exception StandardException Standard Cloudscape error policy
	 */
	public void freezePersistentStore() throws StandardException;

	/**
	 * Backup restore - start writing dirty pages or container to disk
	 */
	public void unfreezePersistentStore();

	/**
	 * Backup restore - don't allow the persistent store to be frozen - or if
	 * it is already frozen, block.   A write is about to commence.
	 * @exception StandardException Standard Cloudscape error policy
	 */
	public void writeInProgress() throws StandardException;

	/**
	 * Backup restore - write finished, if this is the last writer, allow the
	 * persistent store to proceed.
	 */
	public void writeFinished();

    /**
     * Return an id which can be used to create a container.
     * <p>
     * Return an id number with is greater than any existing container
     * in the current database.  Caller will use this to allocate future
     * container numbers - most likely caching the value and then incrementing
     * it as it is used.
     * <p>
     *
	 * @return The an id which can be used to create a container.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    long getMaxContainerId()
		throws StandardException;

	/**
	 * This function is called after a checkpoint to remove the stub files thar are not required
	 * during recovery. Crash recovery  uses these files to identify the dropped
	 * containers.   Stub files(d*.dat) gets creates  when a
	 * table/index(containers) dropped.
	 * @exception StandardException Standard Cloudscape error policy
	 **/
    public void removeDroppedContainerFileStubs(LogInstant redoLWM) throws StandardException;

    /**
     * @return The StorageFactory used by this dataFactory
     */
    public StorageFactory getStorageFactory();

	public void	stop();
}
