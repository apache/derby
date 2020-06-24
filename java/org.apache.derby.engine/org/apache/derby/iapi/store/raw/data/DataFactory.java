/*

   Derby - Class org.apache.derby.iapi.store.raw.data.DataFactory

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

package org.apache.derby.iapi.store.raw.data;

import org.apache.derby.iapi.store.raw.ContainerKey;

import org.apache.derby.iapi.services.daemon.DaemonService;
import org.apache.derby.iapi.services.daemon.Serviceable;
import org.apache.derby.iapi.services.context.ContextManager;

import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.iapi.store.raw.ContainerHandle;
import org.apache.derby.iapi.store.raw.Corruptable;
import org.apache.derby.iapi.store.raw.LockingPolicy;
import org.apache.derby.iapi.store.raw.RawStoreFactory;
import org.apache.derby.iapi.store.raw.StreamContainerHandle;
import org.apache.derby.iapi.store.raw.xact.RawTransaction;
import org.apache.derby.iapi.store.raw.Transaction;
import org.apache.derby.iapi.store.raw.UndoHandler;
import org.apache.derby.io.StorageFactory;
import org.apache.derby.iapi.store.access.FileResource;
import org.apache.derby.iapi.store.access.RowSource;
import org.apache.derby.iapi.store.raw.log.LogInstant;
import org.apache.derby.iapi.util.ByteArray;
import org.apache.derby.catalog.UUID;

import java.util.Properties;
import java.io.File;

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

	/**
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
		then will return a null handle if the container is dropped.

		@return the handle to the opened container
		@exception StandardException Standard Derby error policy

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
		@exception StandardException Standard Derby error policy
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
		@param tableProperties properties of the container that is persistently
				stored throughout the lifetime of the container

		@return the containerId of the newly created container

		@exception StandardException Standard Derby Error policy

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
		@param tableProperties properties of the container that is persistently
				stored throughout the lifetime of the container
		@param rowSource the data to load the container with

		@return the containerId of the newly created stream container

		@exception StandardException Standard Derby Error policy

	 */
	public long addAndLoadStreamContainer(RawTransaction t, long segmentId,
			Properties tableProperties, RowSource rowSource)
		 throws StandardException;

	/**
		Open a stream container.

		@return a valid StreamContainerHandle or null if the container does not exist.

		@exception StandardException  Standard Derby exception policy

	*/
	public StreamContainerHandle openStreamContainer(
    RawTransaction  t,
    long            segmentId,
    long            containerId,
    boolean         hold)
		throws StandardException;

	/**
		Drop and remove a stream container.

		@exception StandardException  Standard Derby exception policy
	*/
	public void dropStreamContainer(RawTransaction t, long segmentId, long containerId)
		throws StandardException;

	/**
		re-Create a container during redo recovery.

        Used if container is found to not exist during redo recovery of
        log records creating the container.

		@exception StandardException Standard Derby Error policy
	 */
	public void reCreateContainerForRedoRecovery(RawTransaction t,
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

		@exception StandardException Standard Derby exception policy.

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

		@exception StandardException Standard Derby exception policy.
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
		Reclaim space used by this factory.  Called by post commit daemon.
		@exception StandardException  Standard Derby exception policy
	*/
	public int reclaimSpace(Serviceable work, ContextManager contextMgr)
		 throws StandardException;

	/**
		Called after recovery is performed.

		@exception StandardException Standard Derby Error Policy
	*/
	public void postRecovery() throws StandardException;

    /**
     * Set up the data factory's caches to use the specified daemon service for
     * background cleaning.
     *
     * @param daemon daemon service to use for background cleaning
     */
    public void setupCacheCleaner(DaemonService daemon);

	/**
		Encrypt cleartext into ciphertext.

		@see org.apache.derby.iapi.services.crypto.CipherProvider#encrypt
		@exception StandardException Standard Derby Error Policy
	 */
	public int encrypt(byte[] cleartext, int offset, int length,
//IC see: https://issues.apache.org/jira/browse/DERBY-1156
					   byte[] ciphertext, int outputOffset, 
                       boolean newEngine)
		 throws StandardException ;

	/**
		Decrypt cleartext from ciphertext.

		@see org.apache.derby.iapi.services.crypto.CipherProvider#decrypt
		@exception StandardException Standard Derby Error Policy
	 */
	public int decrypt(byte[] ciphertext, int offset, int length,
					   byte[] cleartext, int outputOffset)
		 throws StandardException ;

    /**
     * Decrypts all the containers in the data segment.
     *
     * @param t the transaction that is decrypting the container
     * @exception StandardException Standard Derby Error Policy
	 */
//IC see: https://issues.apache.org/jira/browse/DERBY-5792
    void decryptAllContainers(RawTransaction t)
            throws StandardException;

    /**
	 * Encrypt all the containers in the data segment.
     * @param t the transaction that is encrypting the containers.
     * @exception StandardException Standard Derby Error Policy
	 */
	public void encryptAllContainers(RawTransaction t) 
        throws StandardException;

    /**
     * Removes old versions of the containers after a cryptographic operation
     * on the database.
     */
    public void removeOldVersionOfContainers()
        throws StandardException;

    /**
     * Sets whether the database is encrypted.
     *
     * @param isEncrypted {@code true} if the database is encrypted,
     *      {@code false} otherwise
     */
    public void setDatabaseEncrypted(boolean isEncrypted);

	/**
		Return the encryption block size used by the algorithm at time of
		encrypted database creation
	 */
	public int getEncryptionBlockSize();

	/**
	 * Backup restore - stop writing dirty pages or container to disk
	 * @exception StandardException Standard Derby error policy
	 */
	public void freezePersistentStore() throws StandardException;

	/**
	 * Backup restore - start writing dirty pages or container to disk
	 */
	public void unfreezePersistentStore();

	/**
	 * Backup restore - don't allow the persistent store to be frozen - or if
	 * it is already frozen, block.   A write is about to commence.
	 * @exception StandardException Standard Derby error policy
	 */
	public void writeInProgress() throws StandardException;

	/**
	 * Backup restore - write finished, if this is the last writer, allow the
	 * persistent store to proceed.
	 */
	public void writeFinished();

	/**
	 * Back up the data segment of the database.
	 */
	public void backupDataFiles(Transaction rt, File backupDir) throws StandardException;

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
	 * @exception StandardException Standard Derby error policy
	 **/
    public void removeDroppedContainerFileStubs(LogInstant redoLWM) throws StandardException;

    /**
     * @return The StorageFactory used by this dataFactory
     */
    public StorageFactory getStorageFactory();

    /**
     * <p>
     * Get the root directory of the data storage area. It is always
     * guaranteed to be an absolute path, and it is prefixed with the
     * JDBC sub-sub-protocol if it is not a directory database. Examples:
     * </p>
     *
     * <dl>
     *     <dt>{@code /path/to/database}</dt>
     *     <dd>in case of a directory database</dd>
     *     <dt>{@code memory:/path/to/database}</dt>
     *     <dd> in case of a memory database</dd>
     * </dl>
     *
     * @return the root directory of the data storage area
     */
    String getRootDirectory();
//IC see: https://issues.apache.org/jira/browse/DERBY-6733

	public void	stop();

    /**
     * Returns if data base is in encrypted mode.
     * @return true if database encrypted false otherwise
     */
    public boolean databaseEncrypted();

    /**
        Register a handler class for insert undo events.
        <P>
        Register a class to be called when an undo of an insert 
        is executed.  When an undo of an event is executed by
        the raw store UndoHandler.insertUndoNotify() will be
        called, allowing upper level callers to execute code
        as necessary.  The initial need is for the access layer
        to be able to queue post commit reclaim space in the
        case of inserts which are aborted (including the normal
        case of inserts failed for duplicate key violations)
        (see DERBY-4057)
        <p>
        Currently the handler is only called on abort of inserts on
        non-overflow pages that meet either of the following 2 
        requirements:
        1) the row has either overflow columns (long columns) or
           the row columns span multiple pages (long rows).
        2) after the action all user rows on the page are marked deleted.

        @param undo_handle client code supplied undo_handle. 

    */
    public void setUndoInsertEventHandler(UndoHandler undo_handle);
}
