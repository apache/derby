/*

   Derby - Class org.apache.derby.iapi.store.raw.RawStoreFactory

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

import org.apache.derby.iapi.services.daemon.DaemonService;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.services.locks.LockFactory;

import org.apache.derby.iapi.services.property.PersistentSet;

import org.apache.derby.iapi.store.access.TransactionInfo;

import org.apache.derby.iapi.store.raw.log.LogInstant;
import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.catalog.UUID;
import org.apache.derby.iapi.store.access.DatabaseInstant;
import org.apache.derby.iapi.error.ExceptionSeverity;
import java.util.Properties;
import java.io.Serializable;
import java.io.File;

/**
	RawStoreFactory implements a single unit of transactional
	storage. A RawStoreFactory contains Segments and Segments
	contain Containers.
	<P>
	Segments are identified
	by integer identifiers that are unique within a RawStoreFactory.
	<P>
	Containers are also identified by unique integer identifiers
	within a RawStoreFactory, but will overlap with segment identifiers.
	<P><B>LIMITS</B><BR>
	This is a list of (hopefully) all limits within the raw store. Where a size 
    has more than one limit all are documented (rather than just the most 
    restrictive) so that the correct limit can be found if the most restictive 
    is every removed.
	<UL>
	<LI>Field - 
		<UL>
		<LI>Max length 2^31 - 1  (2147483647) - 
		</UL>
	<LI>Record - 
		<UL>
		<LI>Max number of fields 2^31 - 1  (2147483647) - from use of Object[] 
        array to represent row, which can "only" have int sized number of array
        members.
		</UL>
	<LI>Page -
	<LI>Container -
	<LI>Segment -
	<LI>Raw Store -
	</UL>

	<P>
	Access and RawStore work together to provide the ACID properties of
	transactions. On a high level, RawStore deals with anything that directly
	impacts persistency. On a more detailed level, RawStore provides
	logging, rollback and recovery, data management on page, page allocation
	and deallocation, container allocation and deallocation.  


	<P>
	RawStore is organized as 3 branches, transaction, data, and
	logging.  These branches each have its own "factory", the transaction
	factory hands out transactions, the data factory hands out containers,
	and the log factory hands out logger (or log buffers) for transactions to
	write on.  For a more detailed description on these factories, please see
	their corresponding javadocs.


	MT - Thread Safe

	@see ContainerHandle */


public interface RawStoreFactory extends Corruptable {


	/**
		Default value for PAGE_SIZE_PARAMETER (4096).
	*/
	public static final int PAGE_SIZE_DEFAULT = 4096;

	/**
		Minimum page size we will accept (1024).
	*/
	public static final int PAGE_SIZE_MINIMUM = 1024;


	public static final String PAGE_SIZE_STRING = "2048";


	/** Property name for the page cache size to be used in the storage area.
	Equal to 'derby.storage.pageCacheSize'
	*/
	public static final String PAGE_CACHE_SIZE_PARAMETER = 
        "derby.storage.pageCacheSize";

	/**
		Default value for PAGE_CACHE_SIZE_PARAMETER (1000).
	*/
	public static final int PAGE_CACHE_SIZE_DEFAULT = 1000;

	/**
		Minimum page cache size we will accept (40).
	*/
	public static final int PAGE_CACHE_SIZE_MINIMUM = 40;

	/**
		Maximum page cache size we will accept (MAXINT).
	*/
	public static final int PAGE_CACHE_SIZE_MAXIMUM = Integer.MAX_VALUE;

	/**
		Maximum number of initial pages when a container is created
	*/
	public static final short MAX_CONTAINER_INITIAL_PAGES = 1000;

	/** Property name for the default minimum record size to be used in the 
        storage area. Minimum record size is the minimum number of bytes that a 
        record will reserve on disk.
	*/
	public static final String MINIMUM_RECORD_SIZE_PARAMETER = 
        "derby.storage.minimumRecordSize";
	/**
		Default value for MINIMUM_RECORD_SIZE_PARAMETER	for heap tables that 
        allow overflow.  By setting minimumRecordSize to 12 bytes, we 
        guarantee there is enough space to update the row even there is not
        enough space on the page.  The 12 bytes will guarantee there is room
        for an overflow pointer (page + id).
	*/
	public static final int MINIMUM_RECORD_SIZE_DEFAULT = 12;

	/**
		Minimum value for MINIMUM_RECORD_SIZE_PARAMETER (1).
	*/
	public static final int MINIMUM_RECORD_SIZE_MINIMUM = 1;

	/** Property name for percentage of space to leave free on page for updates.
	*/
	public static final String PAGE_RESERVED_SPACE_PARAMETER = 
        "derby.storage.pageReservedSpace";

	public static final String PAGE_RESERVED_ZERO_SPACE_STRING = "0";

	/** Property name for the number of pages we try to pre-allocate in one
	/** synchronous I/O
	*/
	public static final String PRE_ALLOCATE_PAGE = 
        "derby.storage.pagePerAllocate";


	/**
		Property name for container which reuses recordId when a page is
		reused.  Defaults to false, which means recordId is never reused. 	

		This property should NOT be set by the end user, only Access should set
		it for special conglomerates which does not count on permanant unique
		recordIds for all records.
	*/
	public static final String PAGE_REUSABLE_RECORD_ID = 
        "derby.storage.reusableRecordId";

	/**
		Property name for buffer size to be used in the stream file container.
		Equal to 'derby.storage.streamFileBufferSize'
	*/
	public static final String STREAM_FILE_BUFFER_SIZE_PARAMETER = 
        "derby.storage.streamFileBufferSize";

	/**
		Default value for STREAM_FILE_BUFFER_SIZE_PARAMETER (16384).
	*/
	public static final int STREAM_FILE_BUFFER_SIZE_DEFAULT = 16384;

	/**
		Minimum stream file buffer size we will accept (1024).
	*/
	public static final int STREAM_FILE_BUFFER_SIZE_MINIMUM = 1024;

	/**
		Maximum stream file buffer size we will accept (MAXINT).
	*/
	public static final int STREAM_FILE_BUFFER_SIZE_MAXIMUM = 
        Integer.MAX_VALUE;

	/**

		Property name for container which attempts to be created with an
		initial size of this many pages.  Defaults to 1 page.  

		<BR>All containers are guarenteed to be created with at least 1 page,
		if this property is set, it will attempt to allocate
		CONTAINER_INITIAL_PAGES, but with no guarentee.
		CONTAIENR_INITIAL_PAGES legally ranges from 1 to
		MAX_CONTAINER_INITIAL_PAGES.  Values < 1 will
		be set to 1 and values > MAX_CONTAINER_INITIAL_PAGES will be set to
		MAX_CONTAINER_INITIAL_PAGES

		This property should only be set in the PROPERTIES list in a CREATE
		TABLE or CREATE INDEX statement.  The global setting of this property
		has no effect. 
	*/
	public static final String CONTAINER_INITIAL_PAGES = 
        "derby.storage.initialPages";

	/**
		encryption alignment requirement.
	 */
	public static final int ENCRYPTION_ALIGNMENT = 8;

	/**
		default encryption block size
		In old existing databases (ie 5.1.x), the default
		encryption block size used is 8. Do not change this value unless you 
		account for downgrade issues
	 */
	public static final int DEFAULT_ENCRYPTION_BLOCKSIZE = 8;

	/**
		encryption block size used during creation of encrypted database
		This property is not set by the user; it is set by the engine when
		RawStore boots up during creation of an encrypted database
	*/
	public static final String ENCRYPTION_BLOCKSIZE = "derby.encryptionBlockSize";

	/**

		This variable is used to store the encryption scheme to allow
		for any future changes in encryption schemes of data 
		This property has been introduced in version 10
		Value starts at 1
	 */
	public static final String DATA_ENCRYPT_ALGORITHM_VERSION="data_encrypt_algorithm_version";

	/**
                Store the encryption scheme used for logging
		This will allow for any future changes in encryption schemes of logs
		This variable has been introduced in version 10 and value starts at 1.
 	 */
	public static final String LOG_ENCRYPT_ALGORITHM_VERSION="log_encrypt_algorithm_version";

	/**
		If dataEncryption is true, store the encrypted key in
		services.properties file. It is really the encrypted
		key, but the property key is called the encryptedBootPassword.

	 */
	public static final String ENCRYPTED_KEY = 
        "encryptedBootPassword";


	/**
	 *  for debugging, keep all transaction logs intact.
	 */
	public static final String KEEP_TRANSACTION_LOG = 
        "derby.storage.keepTransactionLog";

    /**
      * The following is a to enable patch for databases with recovery
      * errors during redo of InitPage. If this property is set and
	  * the page on the disk is corrupted and is getting exceptions like
	  * invalid page format ids, we cook up the page during the recovery time.
	  * We have seen this kind of problem with 1.5.1 databases from
	  * customer Tridium ( Bug no: 3813).
	  * This patch needs to be kept unless we find the problem is during
	  * recovery process. If we discover this problem is actaully happening
	  * at the recovery then this patch should be backed out.
	  **/
	public static final String PATCH_INITPAGE_RECOVER_ERROR = 
        "derby.storage.patchInitPageRecoverError";


	/** module name */
	public static final String MODULE = 
        "org.apache.derby.iapi.store.raw.RawStoreFactory";

	/**
		Is the store read-only.
	*/
	public boolean isReadOnly();

	/**
		Get the LockFactory to use with this store.
	*/
	public LockFactory getLockFactory();


	/**
		Create a user transaction, almost all work within the raw store is
        performed in the context of a transaction.
		<P>
		Starting a transaction always performs the following steps.
		<OL>
		<LI>Create an raw store transaction context
		<LI>Create a new idle transaction and then link it to the context.
		</UL>
		Only one user transaction and one nested user transaction can be active
        in a context at any one time.
		After a commit the transaction may be re-used.
		<P>
		<B>Raw Store Transaction Context Behaviour</B>
		<BR>
		The cleanupOnError() method of this context behaves as follows:
		<UL>
		<LI>
		If error is an instance of StandardException that
		has a severity less than ExceptionSeverity.TRANSACTION_SEVERITY then
        no action is taken.
		<LI>
		If error is an instance of StandardException that
		has a severity equal to ExceptionSeverity.TRANSACTION_SEVERITY then
        the context's transaction is aborted, and the transaction returned to
        the idle state.
		<LI>
		If error is an instance of StandardException that
		has a severity greater than  ExceptionSeverity.TRANSACTION_SEVERITY
        then the context's transaction is aborted, the transaction closed, and
        the context is popped off the stack.
		<LI>
		If error is not an instance of StandardException then the context's
		transaction is aborted, the transaction closed, and the
		context is popped off the stack.
		</UL>

		@param contextMgr is the context manager to use.  An exception will be
		thrown if context is not the current context.
        @param transName is the name of the transaction. Thsi name will be displayed
        by the transactiontable VTI.

		@exception StandardException Standard Cloudscape error policy

		@see Transaction
		@see org.apache.derby.iapi.services.context.Context
		@see StandardException
	*/

	public Transaction startTransaction(
    ContextManager contextMgr,
    String transName)
        throws StandardException;

	/**
		Create a global user transaction, almost all work within the raw store
        is performed in the context of a transaction.
		<P>
        The (format_id, global_id, branch_id) triplet is meant to come exactly
        from a javax.transaction.xa.Xid.  We don't use Xid so that the system
        can be delivered on a non-1.2 vm system and not require the javax 
        classes in the path.  
        <P>
		Starting a transaction always performs the following steps.
		<OL>
		<LI>Create an raw store transaction context
		<LI>Create a new idle transaction and then link it to the context.
		</UL>
		Only one user transaction can be active in a context at any one time.
		After a commit the transaction may be re-used.
		<P>
		<B>Raw Store Transaction Context Behaviour</B>
		<BR>
		The cleanupOnError() method of this context behaves as follows:
		<UL>
		<LI>
		If error is an instance of StandardException that
		has a severity less than ExceptionSeverity.TRANSACTION_SEVERITY then 
        no action is taken.
		<LI>
		If error is an instance of StandardException that
		has a severity equal to ExceptionSeverity.TRANSACTION_SEVERITY then
        the context's transaction is aborted, and the transaction returned to 
        the idle state.
		<LI>
		If error is an instance of StandardException that
		has a severity greater than  ExceptionSeverity.TRANSACTION_SEVERITY 
        then the context's transaction is aborted, the transaction closed, and 
        the context is popped off the stack.
		<LI>
		If error is not an instance of StandardException then the context's
		transaction is aborted, the transaction closed, and the
		context is popped off the stack.
		</UL>

		@param contextMgr is the context manager to use.  An exception will be
		                  thrown if context is not the current context.
        @param format_id  the format id part of the Xid - ie. Xid.getFormatId().
        @param global_id  the global transaction identifier part of XID - ie.
                          Xid.getGlobalTransactionId().
        @param branch_id  The branch qualifier of the Xid - ie. 
                          Xid.getBranchQaulifier()

		@exception StandardException Standard Cloudscape error policy

		@see Transaction
		@see org.apache.derby.iapi.services.context.Context
		@see StandardException
	*/
	public Transaction startGlobalTransaction(
    ContextManager contextMgr,
    int            format_id,
    byte[]         global_id,
    byte[]         local_id)
        throws StandardException;


	/**
		Find a user transaction in the context manager, which must be the
		current context manager.  If a user transaction does not already exist,
		then create one @see #startTransaction

		@param context is the context manager to use.  An exception will be
		thrown if context is not the current context.
        @param transName  If a new transaction is started, it will be given this name.
        The name is displayed in the transactiontable VTI.

		@exception StandardException Standard Cloudscape error policy

		@see #startTransaction
	*/
	public Transaction findUserTransaction(
        ContextManager contextMgr,
        String transName) throws StandardException;


	/**
		Create an internal transaction.
		<P>
		Starting an internal transaction always performs the following steps.
		<OL>
		<LI>Create an raw store internal transaction context
		<LI>Create a new idle internal transaction and then link it to the 
            context.
		</UL>
		<P>
		AN internal transaction is identical to a user transaction with the 
        exception that
		<UL>
		<LI> Logical operations are not supported
		<LI> Savepoints are not supported
		<LI> Containers are not closed when commit() is called.
		<LI> Pages are not unlatched (since containers are not closed) when 
             commit() is called.
		<LI> During recovery time internal transactions are rolled back before 
             user transactions.
		</UL>
		Only one internal transaction can be active in a context at any one time.
		After a commit the transaction may be re-used.
		<P>
		<B>Raw Store Internal Transaction Context Behaviour</B>
		<BR>
		The cleanupOnError() method of this context behaves as follows:
		<UL>
		<LI>
		If error is an instance of StandardException that
		has a severity less than ExceptionSeverity.TRANSACTION_SEVERITY then
		the internal transaction is aborted, the internal transaction is closed,        the context is popped off the stack, and an exception of severity 
        Transaction exception is re-thrown.
		<LI>
		If error is an instance of StandardException that has a severity 
        greater than or equal to ExceptionSeverity.TRANSACTION_SEVERITY then
        the context's internal transaction is aborted, the internal 
        transaction is closed and the context is popped off the stack.
		<LI>
		If error is not an instance of StandardException then the context's
		internal transaction is aborted, the internal transaction is closed 
        and the context is popped off the stack.
		</UL>

		@exception StandardException Standard Cloudscape error policy

		@see Transaction
		@see org.apache.derby.iapi.services.context.Context
		@see StandardException
	*/
	public Transaction startInternalTransaction(ContextManager contextMgr) throws StandardException;

	/**
		Create a nested user transaction, almost all work within the raw store 
        is performed in the context of a transaction.
		<P>
        A nested user transaction is exactly the same as a user transaction,
        except that one can specify a compatibility space to associate with
        the transaction.
		Starting a transaction always performs the following steps.
		<OL>
		<LI>Create an raw store transaction context
		<LI>Create a new idle transaction and then link it to the context.
		</UL>
		Only one user transaction and one nested user transaction can be active
        in a context at any one time.
		After a commit the transaction may be re-used.
		<P>
		<B>Raw Store Transaction Context Behaviour</B>
		<BR>
		The cleanupOnError() method of this context behaves as follows:
		<UL>
		<LI>
		If error is an instance of StandardException that
		has a severity less than ExceptionSeverity.TRANSACTION_SEVERITY then
        no action is taken.
		<LI>
		If error is an instance of StandardException that
		has a severity equal to ExceptionSeverity.TRANSACTION_SEVERITY then
        the context's transaction is aborted, and the transaction returned to
        the idle state.  If a user transaction exists on the context stack
        then that transaction is aborted also.
		<LI>
		If error is an instance of StandardException that
		has a severity greater than  ExceptionSeverity.TRANSACTION_SEVERITY
        then the context's transaction is aborted, the transaction closed, and
        the context is popped off the stack.
		<LI>
		If error is not an instance of StandardException then the context's
		transaction is aborted, the transaction closed, and the
		context is popped off the stack.
		</UL>

		@param compatibilitySpace compatibility space to use for locks.
		@param contextMgr is the context manager to use.  An exception will be
		thrown if context is not the current context.
        @param transName is the name of the transaction. This name will be 
        displayed by the transactiontable VTI.

		@exception StandardException Standard Cloudscape error policy

		@see Transaction
		@see org.apache.derby.iapi.services.context.Context
		@see StandardException
	*/

	public Transaction startNestedReadOnlyUserTransaction(
    Object         compatibilitySpace,
    ContextManager contextMgr,
    String         transName)
        throws StandardException;

	/**
		Create a nested user transaction, almost all work within the raw store 
        is performed in the context of a transaction.
		<P>
        A nested user transaction is exactly the same as a user transaction,
        except that one can specify a compatibility space to associate with
        the transaction.
		Starting a transaction always performs the following steps.
		<OL>
		<LI>Create an raw store transaction context
		<LI>Create a new idle transaction and then link it to the context.
		</UL>
		Only one user transaction and one nested user transaction can be active
        in a context at any one time.
		After a commit the transaction may be re-used.
		<P>
		<B>Raw Store Transaction Context Behaviour</B>
		<BR>
		The cleanupOnError() method of this context behaves as follows:
		<UL>
		<LI>
		If error is an instance of StandardException that
		has a severity less than ExceptionSeverity.TRANSACTION_SEVERITY then
        no action is taken.
		<LI>
		If error is an instance of StandardException that
		has a severity equal to ExceptionSeverity.TRANSACTION_SEVERITY then
        the context's transaction is aborted, and the transaction returned to
        the idle state.  If a user transaction exists on the context stack
        then that transaction is aborted also.
		<LI>
		If error is an instance of StandardException that
		has a severity greater than  ExceptionSeverity.TRANSACTION_SEVERITY
        then the context's transaction is aborted, the transaction closed, and
        the context is popped off the stack.
		<LI>
		If error is not an instance of StandardException then the context's
		transaction is aborted, the transaction closed, and the
		context is popped off the stack.
		</UL>

		@param compatibilitySpace compatibility space to use for locks.
		@param contextMgr is the context manager to use.  An exception will be
		thrown if context is not the current context.
        @param transName is the name of the transaction. This name will be 
        displayed by the transactiontable VTI.

		@exception StandardException Standard Cloudscape error policy

		@see Transaction
		@see org.apache.derby.iapi.services.context.Context
		@see StandardException
	*/

	public Transaction startNestedUpdateUserTransaction(
    ContextManager contextMgr,
    String         transName)
        throws StandardException;


	/**
	  @see org.apache.derby.iapi.store.access.AccessFactory#getTransactionInfo
	 */
	public TransactionInfo[] getTransactionInfo();

	/**
	  * Freeze the database temporarily so a backup can be taken.
	  * <P>Please see cloudscape on line documentation on backup and restore.
	  *
	  * @exception StandardException Thrown on error
	  */
	public void freeze() throws StandardException;

	/**
	  * Unfreeze the database after a backup has been taken.
	  * <P>Please see cloudscape on line documentation on backup and restore.
	  *
	  * @exception StandardException Thrown on error
	  */
	public void unfreeze() throws StandardException;

	/**
	  * Backup the database to backupDir.  
	  * <P>Please see cloudscape on line documentation on backup and restore.
	  *
	  * @param backupDir the name of the directory where the backup should be
	  *		stored.
	  *
	  * @exception StandardException Thrown on error
	  */
	public void backup(String backupDir) throws StandardException;

	/**
	  * Backup the database to backupDir.  
	  * <P>Please see cloudscape on line documentation on backup and restore.
	  *
	  * @param backupDir the directory where the backup should be stored.
	  *
	  * @exception StandardException Thrown on error
	  */
	public void backup(File backupDir) throws StandardException;


		
	/**
	 * Backup the database to a backup directory and enable the log archive
	 * mode that will keep the archived log files required for roll-forward
	 * from this version backup.
	 * @param backupDir the directory name where the database backup should
	 *   go.  This directory will be created if not it does not exist.
	 * @param deleteOnlineArchivedLogFiles  If true deletes online archived log files
	 * that exist before this backup, delete will occur only after backup is complete.
	 * @exception StandardException Thrown on error
	 */
	public void backupAndEnableLogArchiveMode(String backupDir, 
											  boolean
											  deleteOnlineArchivedLogFiles) 
		throws StandardException;
		
	/**
	 * Backup the database to a backup directory and enable the log archive
	 * mode that will keep the archived log files required for roll-forward
	 * from this version backup.
	 * @param backupDir the directory name where the database backup should
	 *   go.  This directory will be created if not it does not exist.
	 * @param deleteOnlineArchivedLogFiles  If true deletes online archived log files
	 * that exist before this backup, delete will occur only after backup is complete.
	 * @exception StandardException Thrown on error
	 */
	public void backupAndEnableLogArchiveMode(File backupDir, 
											  boolean
											  deleteOnlineArchivedLogFiles) 
		throws StandardException;
	
	/**
	 * disables the log archival process, i.e No old log files
	 * will be kept around for a roll-forward recovery.
	 * @param deleteOnlineArchivedLogFiles  If true deletes all online archived log files
	 * that exist before this call immediately; Only restore that can be performed
	 * after disabling log archive mode is version recovery.
	 * @exception StandardException Thrown on error
	 */
	public void disableLogArchiveMode(boolean deleteOnlineArchivedLogFiles)
		throws StandardException;


	/**
		Try to checkpoint the database to minimize recovery time.
		The raw store does not guarentee that a checkpoint will indeed have
		happened by the time this routine returns.

		@exception StandardException Standard Cloudscape error policy
	*/
	public void checkpoint() throws StandardException;


	/**
		Idle the raw store as much as possible. 
		@exception StandardException Standard Cloudscape error policy

	*/
	public void idle() throws StandardException;

	/**
	    Get a flushed scan.
		@param start The instant for the beginning of the scan.
		@param groupsIWant log record groups the caller wants to scan.
		@exception StandardException StandardCloudscape error policy
		*/
	ScanHandle openFlushedScan(DatabaseInstant start, int groupsIWant) 
		 throws StandardException;

	
	/**
		If this raw store has a daemon that services its need, return the
		daemon.  If not, return null
	*/
	public DaemonService getDaemon();


	/*
	 * return the transaction factory module 
	 */
	public String getTransactionFactoryModule();

	/*
	 * return the data factory module 
	 */
	public String getDataFactoryModule();

	/*
	 * return the Log factory module 
	 */
	public String getLogFactoryModule();

	/*
	 * Return the module providing XAresource interface to the transaction 
     * table. 
     *
	 * @exception StandardException Standard cloudscape exception policy.
	 */
	public /* XAResourceManager */ Object getXAResourceManager()
        throws StandardException;

	/*
	 * the database creation phase is finished
	 * @exception StandardException Standard cloudscape exception policy.
	 */
	public void createFinished() throws StandardException;

	/**
	 * Get JBMS properties relavent to raw store
	 *
	 * @exception StandardException Standard cloudscape exception policy.
	 */
	public void getRawStoreProperties(PersistentSet tc) 
		 throws StandardException; 

	/**
	 *  Backup / restore support
	 */

	/**
	 * Freeze the database from altering any persistent storage.
	 *
	 * @exception StandardException Standard cloudscape exception policy.
	 */
	public void freezePersistentStore() throws StandardException;

	/**
	 * Unfreeze the database, persistent storage can now be altered.
	 *
	 * @exception StandardException Standard cloudscape exception policy.
	 */
	public void unfreezePersistentStore() throws StandardException;

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
	 	Returns the encryption block size used during creation of the encrypted database
	 */
	public int getEncryptionBlockSize();

	/**
		Returns a secure random number for this raw store - if database is not
		encrypted, returns 0.
	 */
	public int random();

	/**
		Change the boot password.  Return the encrypted form of the secret key.
		The new value must be a String of the form: oldBootPassword, newBootPassword

		@exception StandardException Standard Cloudscape Error Policy
	 */
	public Serializable changeBootPassword(Properties properties, Serializable changePassword)
		 throws StandardException ;

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
}
