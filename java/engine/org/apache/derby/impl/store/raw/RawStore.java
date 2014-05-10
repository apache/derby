/*

   Derby - Class org.apache.derby.impl.store.raw.RawStore

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

package org.apache.derby.impl.store.raw;

import org.apache.derby.iapi.services.daemon.DaemonFactory;
import org.apache.derby.iapi.services.daemon.DaemonService;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.services.context.ContextService;
import org.apache.derby.iapi.services.crypto.CipherFactoryBuilder;
import org.apache.derby.iapi.services.crypto.CipherFactory;
import org.apache.derby.iapi.services.crypto.CipherProvider;
import org.apache.derby.iapi.services.locks.CompatibilitySpace;
import org.apache.derby.iapi.services.locks.LockFactory;
import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.iapi.services.monitor.ModuleControl;
import org.apache.derby.iapi.services.monitor.ModuleSupportable;
import org.apache.derby.iapi.services.monitor.PersistentService;
import org.apache.derby.shared.common.sanity.SanityManager;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.i18n.MessageService;

import org.apache.derby.iapi.services.property.PersistentSet;
import org.apache.derby.iapi.store.access.TransactionInfo;
import org.apache.derby.iapi.store.access.AccessFactoryGlobals;
import org.apache.derby.iapi.store.access.FileResource;
import org.apache.derby.iapi.store.raw.ScanHandle;
import org.apache.derby.iapi.store.raw.RawStoreFactory;
import org.apache.derby.iapi.store.raw.Transaction;
import org.apache.derby.iapi.store.raw.xact.RawTransaction;
import org.apache.derby.iapi.store.raw.xact.TransactionFactory;
import org.apache.derby.iapi.store.raw.data.DataFactory;
import org.apache.derby.iapi.store.raw.log.LogFactory;
import org.apache.derby.iapi.store.raw.log.LogInstant;
import org.apache.derby.iapi.util.StringUtil;
import org.apache.derby.impl.services.monitor.UpdateServiceProperties;

import org.apache.derby.io.StorageFactory;
import org.apache.derby.io.StorageRandomAccessFile;
import org.apache.derby.io.WritableStorageFactory;
import org.apache.derby.io.StorageFile;
import org.apache.derby.iapi.store.access.DatabaseInstant;
import org.apache.derby.iapi.services.io.FileUtil;
import org.apache.derby.iapi.util.ReuseFactory;
import org.apache.derby.iapi.reference.Attribute;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.reference.MessageId;
import org.apache.derby.iapi.reference.Property;

import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.SecureRandom;

import java.util.Date;
import java.util.Properties;
import java.io.Serializable;
import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.URL;

import java.security.PrivilegedExceptionAction;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;

import org.apache.derby.iapi.store.replication.master.MasterFactory;
import org.apache.derby.iapi.store.replication.slave.SlaveFactory;

/**
	A Raw store that implements the RawStoreFactory module by delegating all the
	work to the lower modules TransactionFactory, LogFactory and DataFactory.
	<PRE>
	String TransactionFactoryId=<moduleIdentifier>
	</PRE>
	
	<P>
	Class is final as it has methods with privilege blocks
	and implements PrivilegedExceptionAction.
*/

public final class RawStore implements RawStoreFactory, ModuleControl, ModuleSupportable, PrivilegedExceptionAction<Object>
{
	private static final String BACKUP_HISTORY = "BACKUP.HISTORY";
	protected TransactionFactory	xactFactory;
	protected DataFactory			dataFactory;
	protected LogFactory			logFactory;
    private SlaveFactory slaveFactory;
    private StorageFactory storageFactory;

	private SecureRandom random;
	private boolean isEncryptedDatabase;
	private CipherProvider encryptionEngine;
	private CipherProvider decryptionEngine;
    private CipherProvider newEncryptionEngine;
	private CipherProvider newDecryptionEngine;
	private CipherFactory  currentCipherFactory;
    private CipherFactory newCipherFactory = null;
	private int counter_encrypt;
	private int counter_decrypt;
	private int encryptionBlockSize = RawStoreFactory.DEFAULT_ENCRYPTION_BLOCKSIZE;

	// this daemon takes care of all daemon work for this raw store
	protected DaemonService			rawStoreDaemon;

    private int actionCode;
    private static final int FILE_WRITER_ACTION = 1;
    private StorageFile actionStorageFile;
    private StorageFile actionToStorageFile;
    private boolean actionAppend;
    private static final int REGULAR_FILE_EXISTS_ACTION = 2;
    private File actionRegularFile;
    private static final int STORAGE_FILE_EXISTS_ACTION = 3;
    private static final int REGULAR_FILE_DELETE_ACTION = 4;
    private static final int REGULAR_FILE_MKDIRS_ACTION = 5;
    private static final int REGULAR_FILE_IS_DIRECTORY_ACTION = 6;
    private static final int REGULAR_FILE_REMOVE_DIRECTORY_ACTION = 7;
    private static final int REGULAR_FILE_RENAME_TO_ACTION = 8;
    private File actionRegularFile2;
    private static final int COPY_STORAGE_DIRECTORY_TO_REGULAR_ACTION = 9;
    private byte[] actionBuffer;
    private String[] actionFilter;
    private boolean actionCopySubDirs;
    private static final int COPY_REGULAR_DIRECTORY_TO_STORAGE_ACTION = 10;
    private static final int COPY_REGULAR_FILE_TO_STORAGE_ACTION = 11;
    private static final int REGULAR_FILE_LIST_DIRECTORY_ACTION = 12;
    private static final int STORAGE_FILE_LIST_DIRECTORY_ACTION = 13;
    private static final int COPY_STORAGE_FILE_TO_REGULAR_ACTION = 14;
    private static final int REGULAR_FILE_GET_CANONICALPATH_ACTION = 15;
    private static final int STORAGE_FILE_GET_CANONICALPATH_ACTION = 16;
    private static final int COPY_STORAGE_FILE_TO_STORAGE_ACTION = 17;
    private static final int STORAGE_FILE_DELETE_ACTION = 18;
    private static final int README_FILE_OUTPUTSTREAM_WRITER_ACTION = 19;

	public RawStore() {
	}

	/*
	** Methods of ModuleControl
	*/

	/**
	  We use this RawStore for all databases.
	  */
	public boolean canSupport(Properties startParams) {
		return true;
	}

	public void	boot(boolean create, Properties properties)
		throws StandardException
	{

        boolean transformExistingData = false;
        boolean inReplicationSlaveMode = false;

        String slave = properties.getProperty(SlaveFactory.REPLICATION_MODE);
        if (slave != null && slave.equals(SlaveFactory.SLAVE_MODE)) {
            inReplicationSlaveMode = true;
        }

		DaemonFactory daemonFactory =
			(DaemonFactory)Monitor.startSystemModule(org.apache.derby.iapi.reference.Module.DaemonFactory);
		rawStoreDaemon = daemonFactory.createNewDaemon("rawStoreDaemon");
		xactFactory = (TransactionFactory)
					Monitor.bootServiceModule(
						create, this, getTransactionFactoryModule(), properties);

		dataFactory = (DataFactory)
					Monitor.bootServiceModule(
					  create, this, getDataFactoryModule(), properties);
		storageFactory = dataFactory.getStorageFactory();

        String restoreFromBackup = null;

		if (properties != null)
		{
            // check if this is a restore from a backup copy. 
            restoreFromBackup = properties.getProperty(Attribute.CREATE_FROM);
            if(restoreFromBackup == null)
                restoreFromBackup = properties.getProperty(Attribute.RESTORE_FROM);
            if(restoreFromBackup == null)
                restoreFromBackup =
                    properties.getProperty(Attribute.ROLL_FORWARD_RECOVERY_FROM);

        }

        // setup database encryption engines.
        if (create) {
            transformExistingData = setupEncryptionEngines(create, properties);
            if (SanityManager.DEBUG) {
                SanityManager.ASSERT(!transformExistingData,
                        "no crypto data transformation for a new db");
            }
        }


		// let everyone knows who their rawStoreFactory is and they can use it
		// to get to other modules
		// pass in create and properties to dataFactory so it can boot the log
		// factory

		dataFactory.setRawStoreFactory(this, create, properties);
		xactFactory.setRawStoreFactory(this);

        if( properties instanceof UpdateServiceProperties)
        {
            if( storageFactory instanceof WritableStorageFactory)
                ((UpdateServiceProperties)properties).setStorageFactory( (WritableStorageFactory) storageFactory);
        }
        
		// log factory is booted by the data factory
		logFactory =(LogFactory) Monitor.findServiceModule(this, getLogFactoryModule());

		// if this is a restore from backup, restore the jar files.
		if(restoreFromBackup !=null)
		{
			restoreRemainingFromBackup(restoreFromBackup);
		}

		// If the log is at another location, make sure  service.properties
		// file has it.
		String logDevice = properties.getProperty(Attribute.LOG_DEVICE);
		if (logDevice !=null)
		{
            if (!isReadOnly() // We do not care about log location if read only
                && (create 
                    || !logDevice.equals(logFactory.getCanonicalLogPath()) 
                    || restoreFromBackup!=null))
			{
				// get the real location from the log factory
				properties.put(Attribute.LOG_DEVICE, logFactory.getCanonicalLogPath());
				//make the log device param stored in backup is same as current log device.
				properties.put(Property.LOG_DEVICE_AT_BACKUP, logFactory.getCanonicalLogPath());
			}
	
		}else{
			//when we restore from a backup logDevice param does not exists 
			//in service.properties to support restore using OS commands to work. 
			//Instead of logDevice, we user logDeviceWhenBackedUp parameter to
			//identify the log location while restoring createFrom/restoreFrom/rollForwardRecoveryFrom
			//attribute , following make sures the logDevice parameter gets 
			//into service.propertues in such cases.
			if(restoreFromBackup!=null && logFactory.getCanonicalLogPath()!=null)
			{
				//logdevice might have got changed because of backup restore. 
				properties.put(Attribute.LOG_DEVICE,  logFactory.getCanonicalLogPath());
			}
			else{
				//might have been OS copy restore. We default log to db home
				properties.remove(Property.LOG_DEVICE_AT_BACKUP);
			}
		}

		
		// save the service properties to a file if we are doing a 
		// restore from. This marks the end of restore from backup.
		if (restoreFromBackup !=null)
		{
			((UpdateServiceProperties)properties).saveServiceProperties();
		}


		/**
		 * Note: service.properties file acts as flags to indicate
		 * that the copy from backup is successful.
		 * If we reached so far while restoring from backup means
		 * we copied all the necessary data from backup. Only thing
		 * that remains is roll forwarding the logs. Incase if we crash at this
		 * point and user re boots the datbase again without any restore flags
		 * it shoud boot without any problem.
		 **/


        // setup database encryption engine
        if (!create) 
        {
            // check if the engine crashed while re-encrypting an 
            // encrypted database or while encryption and 
            // existing database.
            if(properties.getProperty(
                              RawStoreFactory.DB_ENCRYPTION_STATUS) !=null) 
            {   
                handleIncompleteDbCryptoOperation(properties);
            }

            transformExistingData = setupEncryptionEngines(create, properties);
        }

        if (isEncryptedDatabase) {
            // let log factory know if the database is encrypted . 
            logFactory.setDatabaseEncrypted(true, false);
            // let data factory know if the database is encrypted. 
            dataFactory.setDatabaseEncrypted(true);
        }

        // RawStoreFactory is used by LogFactory.recover() and by
        // SlaveFactory.startSlave (for the SlaveFactory case, it is
        // only used if the database is encrypted)
        logFactory.setRawStoreFactory(this);

        // If SlaveFactory is to be booted, the boot has to happen
        // before logFactory.recover since that method will be blocked
        // when in replication slave mode.
        if (inReplicationSlaveMode) {
            // The LogFactory has already been booted in slave mode.
            // Can now start slave replication by booting the
            // SlaveFactory service
            slaveFactory = (SlaveFactory) 
                Monitor.bootServiceModule(create, this,
                                          getSlaveFactoryModule(),
                                          properties);
            slaveFactory.startSlave(this, logFactory);
        }

		// no need to tell log factory which raw store factory it belongs to
		// since this is passed into the log factory for recovery
		// after the factories are loaded, recover the database
		logFactory.recover(dataFactory, xactFactory);

        // If user requested to encrypt an un-encrypted database or encrypt with
        // a new alogorithm then do that now.
        if (transformExistingData) {
            applyBulkCryptoOperation(properties, newCipherFactory);
        }
	}

	public void	stop() {

		if (SanityManager.DEBUG)
		{
            if (isEncryptedDatabase) {
				SanityManager.DEBUG_PRINT("encryption statistics",
						"Encryption called " + counter_encrypt + " times, " +
						"decryption called " + counter_decrypt + " times");
            }
		}

		if (rawStoreDaemon != null)
			rawStoreDaemon.stop();

		if (logFactory == null)
			return;

		try {

			if (logFactory.checkpoint(this, dataFactory, xactFactory, false))
			{
                // checkpoint with "wait" = false.  If set to true shutdown
                // might loop forever if we are shutting down on a serious 
                // error where some factory has already shut down and thus
                // log checkpoint always returns false.

				if (dataFactory != null)
					dataFactory.removeStubsOK();
			}

		} catch (StandardException se) {
			// checkpoint failed, stop all factory from shutting down normally
			markCorrupt(se);
		}
	}

	/*
	** Methods of RawStoreFactory
	*/

	/**
		Is the store read-only.
		@see RawStoreFactory#isReadOnly
	*/
	public boolean isReadOnly() {
		return dataFactory.isReadOnly();
	}

	public LockFactory getLockFactory() {
		return xactFactory.getLockFactory();
	}

    
    /**
		Get the Transaction Factory to use with this store.
	*/
	public TransactionFactory getXactFactory() {
        return xactFactory;  
    }

	/*
	 * Return the module providing XAresource interface to the transaction
     * table.
     *
	 * @exception StandardException Standard Derby exception policy.
	 */
	public /* XAResourceManager */ Object getXAResourceManager()
        throws StandardException
    {
        return(xactFactory.getXAResourceManager());
    }


	public Transaction startGlobalTransaction(
    ContextManager  contextMgr,
    int             format_id,
    byte[]          global_id,
    byte[]          branch_id)
        throws StandardException
    {
		return xactFactory.startGlobalTransaction(
                    this, contextMgr, format_id, global_id, branch_id);
	}

	public Transaction startTransaction(ContextManager contextMgr, String transName)
        throws StandardException
    {
		return xactFactory.startTransaction(this, contextMgr, transName);
	}

	public Transaction startNestedReadOnlyUserTransaction(
    Transaction parentTransaction,
    CompatibilitySpace compatibilitySpace,
    ContextManager  contextMgr,
    String          transName)
        throws StandardException
    {
		return(
            xactFactory.startNestedReadOnlyUserTransaction
            (
             this, (RawTransaction) parentTransaction, compatibilitySpace, contextMgr, transName)
            );
	}

	public Transaction startNestedUpdateUserTransaction(
    Transaction parentTransaction,
    ContextManager  contextMgr,
    String          transName,
    boolean         flush_log_on_xact_end)
        throws StandardException
    {
		return(
            xactFactory.startNestedUpdateUserTransaction
            (
             this, (RawTransaction) parentTransaction, contextMgr, transName, flush_log_on_xact_end)
            );
	}

	public Transaction findUserTransaction(
        ContextManager contextMgr,
        String transName)
		 throws StandardException
	{
		return xactFactory.findUserTransaction(this, contextMgr, transName);
	}


	public Transaction startInternalTransaction(ContextManager contextMgr) throws StandardException {

		return xactFactory.startInternalTransaction(this, contextMgr);
	}

	public void checkpoint() throws StandardException
	{
		logFactory.checkpoint(this, dataFactory, xactFactory, true);
	}

    /**
     * Start the replication master role for this database
     * @param dbmaster The master database that is being replicated.
     * @param host The hostname for the slave
     * @param port The port the slave is listening on
     * @param replicationMode The type of replication contract.
     * Currently only asynchronous replication is supported, but
     * 1-safe/2-safe/very-safe modes may be added later.
     * @exception StandardException 1) If replication is started on a read-only
     *                                 database
     *                              2) If replication is started when unlogged
     *                                 operations are running
     *                              3) If an error occurs while trying to boot
     *                                 the master.
     */
    public void startReplicationMaster(String dbmaster, String host, int port,
                                       String replicationMode)
        throws StandardException {

        if (isReadOnly()) {
            throw StandardException.newException(
                      SQLState.LOGMODULE_DOES_NOT_SUPPORT_REPLICATION);
        }
        
        RawTransaction t = 
                xactFactory.findUserTransaction(
                this,
                ContextService.getFactory().getCurrentContextManager(), 
                AccessFactoryGlobals.USER_TRANS_NAME);

        //back up blocking operations are unlogged operations.
        //Check if any unlogged operations are active and if
        //yes do not allow replication to start.
        if (t.isBlockingBackup())
        {
            throw StandardException.newException(
                  SQLState.REPLICATION_UNLOGGED_OPERATIONS_IN_PROGRESS);  
        }

        Properties replicationProps = new Properties();

        replicationProps.setProperty(MasterFactory.REPLICATION_MODE,
                                     replicationMode);

        MasterFactory masterFactory = (MasterFactory)
            Monitor.bootServiceModule(true, this, getMasterFactoryModule(),
                                      replicationProps);
        masterFactory.startMaster(this, dataFactory, logFactory,
                host, port, dbmaster);

    }
    
    /**
     * Stop the replication master role for this database.
     * 
     * @exception StandardException Standard Derby exception policy,
     * thrown on error.
     */
    public void stopReplicationMaster() throws StandardException {
        
        MasterFactory masterFactory = null;
        
        if (isReadOnly()) {
            throw StandardException.newException(
                      SQLState.LOGMODULE_DOES_NOT_SUPPORT_REPLICATION);
        }

        try {
            masterFactory = (MasterFactory) 
                Monitor.findServiceModule(this, getMasterFactoryModule());
        }
        catch (StandardException se) {
            throw StandardException.newException(
                      SQLState.REPLICATION_NOT_IN_MASTER_MODE);
        }
        masterFactory.stopMaster();
    }
    
        
    /**
     * @see org.apache.derby.iapi.store.raw.RawStoreFactory#failover(String dbname).
     */
    public void failover(String dbname) throws StandardException {
        MasterFactory masterFactory = null;
        
        if (isReadOnly()) {
            throw StandardException.newException(
                      SQLState.LOGMODULE_DOES_NOT_SUPPORT_REPLICATION);
        }

        try {
            masterFactory = (MasterFactory) 
                Monitor.findServiceModule(this, getMasterFactoryModule());
        }
        catch (StandardException se) {
            throw StandardException.newException
                    (SQLState.REPLICATION_NOT_IN_MASTER_MODE);
        }
        masterFactory.startFailover();
    }

	public void freeze() throws StandardException
	{
		logFactory.checkpoint(this, dataFactory, xactFactory, true);
		dataFactory.freezePersistentStore();
		logFactory.freezePersistentStore();
	}

	public void unfreeze() throws StandardException
	{
		logFactory.unfreezePersistentStore();
		dataFactory.unfreezePersistentStore();
	}

    /**
     * Backup the database to a backup directory.
     *
     * @param backupDir the name of the directory where the backup should be
     *                  stored. This directory will be created if it 
     *                  does not exist.
     * @param wait if <tt>true</tt>, waits for  all the backup blocking 
     *             operations in progress to finish.
     * @exception StandardException thrown on error
     */
    public void backup(String backupDir, boolean wait) 
        throws StandardException 
    {
		if (backupDir == null || backupDir.equals(""))
        {
			throw StandardException.newException(
                SQLState.RAWSTORE_CANNOT_CREATE_BACKUP_DIRECTORY, (File)null);
        }

		// in case this is an URL form
		String backupDirURL = null;
		try {
			URL url = new URL(backupDir);
			backupDirURL = url.getFile();
		} catch (MalformedURLException ex) {}

		if (backupDirURL != null)
			backupDir = backupDirURL;


		// find the user transaction, it is necessary for online backup 
		// to open the container through page cache
		RawTransaction t = 
            xactFactory.findUserTransaction(this,
                ContextService.getFactory().getCurrentContextManager(), 
                AccessFactoryGlobals.USER_TRANS_NAME);

		try {

            // check if  any backup blocking operations are in progress
            // in the same transaction backup is being executed? Backup is 
            // not allowed if the transaction has uncommitted
            // unlogged operations that are blocking the backup.
            
            if (t.isBlockingBackup())
            {
                throw StandardException.newException(
                      SQLState.BACKUP_OPERATIONS_NOT_ALLOWED);  
            }


            // check if any backup blocking operations are in progress
            // and stop new ones from starting until the backup is completed.
            if (!xactFactory.blockBackupBlockingOperations(wait))
            {
                throw StandardException.newException(
                      SQLState.BACKUP_BLOCKING_OPERATIONS_IN_PROGRESS);  
            }

            // perform backup
            backup(t, new File(backupDir));
        }finally {
            // let the xactfatory know that backup is done, so that
            // it can allow backup blocking operations. 
            xactFactory.unblockBackupBlockingOperations();
        }
	}


	/*
	 * Backup the database.
	 * Online backup copies all the database files (log, seg0  ...Etc) to the
	 * specified backup location without blocking any user operation for the 
	 * duration of the backup. Stable copy is made of each page using 
     * page level latches and in some cases with the help of monitors.  
     * Transaction log is also backed up, this is used to bring the database to 
     * the consistent state on restore.
	 * 
     * <P> MT- only one thread  is allowed to perform backup at any given time. 
     *  Synchronized on this. Parallel backups are not supported. 
	 */
	public synchronized void backup(Transaction t, File backupDir) 
        throws StandardException
	{
        if (!privExists(backupDir))
		{
            // if backup dir does not exist, go ahead and create it.
            createBackupDirectory(backupDir);
		}
		else
		{
            // entity with backup name exists, make sure it is a directory.

            if (!privIsDirectory(backupDir))
            {
				throw StandardException.newException(
                    SQLState.RAWSTORE_CANNOT_BACKUP_TO_NONDIRECTORY,
                    (File) backupDir);
            }

            // check if a user has given the backup as a database directory by
            // mistake, backup path can not be a derby database directory. 
            // If a directory contains PersistentService.PROPERTIES_NAME, it 
            // is assumed to be a derby database directory because derby 
            // databases always have this file. 
 
            if (privExists(
                    new File(backupDir, PersistentService.PROPERTIES_NAME))) 
            { 
                throw StandardException.newException(
                    SQLState.RAWSTORE_CANNOT_BACKUP_INTO_DATABASE_DIRECTORY,
                    (File) backupDir); 
            }
		}
		
		boolean error = true;
		boolean renamed = false;
		boolean renameFailed = false;
		File oldbackup = null;
		File backupcopy = null;
		OutputStreamWriter historyFile = null;
        StorageFile dbHistoryFile = null;
        File backupHistoryFile = null;
		LogInstant backupInstant = logFactory.getFirstUnflushedInstant();
        
		try
		{
			// get name of the current db, ie. database directory of current db.
			StorageFile dbase           = storageFactory.newStorageFile(null); 
            String      canonicalDbName = storageFactory.getCanonicalName();
            String      dbname = StringUtil.shortDBName( canonicalDbName, storageFactory.getSeparator() );

			// append to end of history file
			historyFile = 
                privFileWriter(
                    storageFactory.newStorageFile(BACKUP_HISTORY), true);
            
			backupcopy = new File(backupDir, dbname);

			logHistory(
                historyFile,
                MessageService.getTextMessage(
                    MessageId.STORE_BACKUP_STARTED, 
                    canonicalDbName, 
                    getFilePath(backupcopy)));

            
            // check if a backup copy of this database already exists,
            if (privExists(backupcopy))
			{
				// first make a backup of the backup
				oldbackup = new File(backupDir, dbname+".OLD");
                if (privExists(oldbackup))
				{
                    if (privIsDirectory(oldbackup))
                        privRemoveDirectory(oldbackup);
					else
                        privDelete(oldbackup);
				}

                if (!privRenameTo(backupcopy,oldbackup))
                {
                    renameFailed = true;
                    throw StandardException.
                        newException(SQLState.RAWSTORE_ERROR_RENAMING_FILE,
                                     backupcopy, oldbackup);
                }
				else
				{
					logHistory(
                        historyFile,
                        MessageService.getTextMessage(
                            MessageId.STORE_MOVED_BACKUP,
                            getFilePath(backupcopy),
                            getFilePath(oldbackup)));
					renamed = true;
				}
			}

            // create the backup database directory
            createBackupDirectory(backupcopy);

            dbHistoryFile = storageFactory.newStorageFile(BACKUP_HISTORY);
            backupHistoryFile = new File(backupcopy, BACKUP_HISTORY); 

            // copy the history file into the backup. 
            if(!privCopyFile(dbHistoryFile, backupHistoryFile))
                throw StandardException. 
                    newException(SQLState.RAWSTORE_ERROR_COPYING_FILE,
                                 dbHistoryFile, backupHistoryFile);  


            // if they are any jar file stored in the database, copy them into
            // the backup. 
            StorageFile jarDir = 
                storageFactory.newStorageFile(FileResource.JAR_DIRECTORY_NAME);

            if (privExists(jarDir)) 
            {
                // find the list of schema directories under the jar dir and
                // then copy only the plain files under those directories. One 
                // could just use the recursive copy of directory to copy all 
                // the files under the jar dir, but the problem with that is if
                // a user gives jar directory as the backup path by mistake, 
                // copy will fail while copying the backup dir onto itself in 
                // recursion

                String [] jarDirContents = privList(jarDir);
                File backupJarDir = new File(backupcopy, 
                                             FileResource.JAR_DIRECTORY_NAME);
                // Create the backup jar directory
                createBackupDirectory(backupJarDir);

                LanguageConnectionContext lcc = 
                    (LanguageConnectionContext)ContextService.getContextOrNull(
                        LanguageConnectionContext.CONTEXT_ID);
        
                // DERBY-5357 UUIDs introduced in jar file names in 10.9
                boolean uuidSupported =
                    lcc.getDataDictionary().
                    checkVersion(DataDictionary.DD_VERSION_DERBY_10_9, null);

                if (uuidSupported) {
                    // no subdirectories
                    for (int i = 0; i < jarDirContents.length; i++) {
                        StorageFile jar = storageFactory.newStorageFile(
                            jarDir, jarDirContents[i]);
                        File backupJar =
                            new File(backupJarDir, jarDirContents[i]);

                        if (privIsDirectory(new File(jar.getPath()))) {
                            continue; // We no longer expect directories inside
                                      // 'jar'. Need check to make the weird
                                      // test #2 in BackupPathTests.java work:
                                      // it does a backup of the db into its
                                      // own(!) jar file directory, so trying
                                      // to copy that db file into itself two
                                      // levels down would fail.
                        }

                        if (!privCopyFile(jar, backupJar)) {
                            throw StandardException.
                                newException(
                                    SQLState.RAWSTORE_ERROR_COPYING_FILE,
                                    jar, backupJar);
                        }
                    }
                } else {
                    for (int i = 0; i < jarDirContents.length; i++) {
                        StorageFile jarSchemaDir =
                            storageFactory.newStorageFile(
                                jarDir, jarDirContents[i]);

                        File backupJarSchemaDir =
                            new File(backupJarDir, jarDirContents[i]);

                        if (!privCopyDirectory(
                                    jarSchemaDir, backupJarSchemaDir,
                                    (byte[])null, null, false)) {
                            throw StandardException.
                                newException(
                                    SQLState.RAWSTORE_ERROR_COPYING_FILE,
                                    jarSchemaDir, backupJarSchemaDir);
                        }
                    }
                }
            }


            // save service properties into the backup, Read in property 
            // from service.properties file, remove logDevice from it, 
            // then write it to the backup.

            StorageFile logdir = logFactory.getLogDirectory();
            
            try 
            {
                String name = Monitor.getMonitor().getServiceName(this);
                PersistentService ps = 
                    Monitor.getMonitor().getServiceType(this);
                String fullName = ps.getCanonicalServiceName(name);
                Properties prop = 
                    ps.getServiceProperties(fullName, (Properties)null);

                StorageFile defaultLogDir = 
                    storageFactory.newStorageFile(
                        LogFactory.LOG_DIRECTORY_NAME);

                if (!logdir.equals(defaultLogDir))  
                {
                    prop.remove(Attribute.LOG_DEVICE);
                    if (SanityManager.DEBUG)
                    {
                        SanityManager.ASSERT(
                            prop.getProperty(Attribute.LOG_DEVICE) == null,
                            "cannot get rid of logDevice property");
                    }

                    logHistory(historyFile,
                               MessageService.getTextMessage(
                               MessageId.STORE_EDITED_SERVICEPROPS));
                }
            
                // save the service properties into the backup.
                ps.saveServiceProperties(backupcopy.getPath(), prop);

            }
            catch(StandardException se) 
            {
                logHistory(
                   historyFile,
                   MessageService.getTextMessage(
                       MessageId.STORE_ERROR_EDIT_SERVICEPROPS) + se);

                return; // skip the rest and let finally block clean up
            }

            // Incase of encrypted database and the key is an external 
            // encryption key, there is an extra file with name  
            // Attribute.CRYPTO_EXTERNAL_KEY_VERIFY_FILE, this file should be
            // copied in to the backup.
            StorageFile verifyKeyFile = 
                storageFactory.newStorageFile(
                                 Attribute.CRYPTO_EXTERNAL_KEY_VERIFY_FILE);
            if (privExists(verifyKeyFile)) 
            {
                File backupVerifyKeyFile = 
                    new File(
                        backupcopy, Attribute.CRYPTO_EXTERNAL_KEY_VERIFY_FILE);

                if(!privCopyFile(verifyKeyFile, backupVerifyKeyFile))
                   throw StandardException.
                       newException(SQLState.RAWSTORE_ERROR_COPYING_FILE,
                                    verifyKeyFile, backupVerifyKeyFile);  
            }
                
			File logBackup = 
                new File(backupcopy, LogFactory.LOG_DIRECTORY_NAME);

			// this is wierd, delete it
            if (privExists(logBackup))
			{
                privRemoveDirectory(logBackup);
			}

			// Create the log directory
            createBackupDirectory(logBackup);

			// do a checkpoint to get the persistent store up to date.
			logFactory.checkpoint(this, dataFactory, xactFactory, true);
			
			// start the transaction log  backup. 
            logFactory.startLogBackup(logBackup);

			File segBackup = new File(backupcopy, "seg0");
			
			// Create the data segment directory
            createBackupDirectory(segBackup);

			// backup all the information in the data segment.
			dataFactory.backupDataFiles(t, segBackup);

            logHistory(historyFile,
                   MessageService.getTextMessage(
                   MessageId.STORE_DATA_SEG_BACKUP_COMPLETED,
                   getFilePath(segBackup)));


            // copy the log that got generated after the backup started to
			// backup location and tell the logfactory that backup has come 
            // to end.
			logFactory.endLogBackup(logBackup);
																		  
			logHistory(historyFile,
                MessageService.getTextMessage(
                    MessageId.STORE_COPIED_LOG,
                    getFilePath(logdir),
                    getFilePath(logBackup)));

			error = false;
		}
		catch (IOException ioe)
		{
			throw StandardException.newException(
                    SQLState.RAWSTORE_UNEXPECTED_EXCEPTION, ioe);
		}
		finally
		{

			try
			{
				if (error)
				{
					
					// Abort all activity related to backup in the log factory.
					logFactory.abortLogBackup();

					// remove the half backed up copy
					// unless the error occured during  rename process;
					// inwhich case 'backupcopy' refers to the previous backup
					// not an half backed one.
					if(!renameFailed)
						privRemoveDirectory(backupcopy);

					if (renamed)
						// recover the old backup
                        privRenameTo(oldbackup,backupcopy);

					logHistory(historyFile,
                        MessageService.getTextMessage(
                            MessageId.STORE_BACKUP_ABORTED));

 				}
				else
				{
					// success, remove the old backup copy
                    if (renamed && privExists(oldbackup))
					{
						// get rid of the old backup
                        privRemoveDirectory(oldbackup);
						logHistory(historyFile,
                            MessageService.getTextMessage(
                                MessageId.STORE_REMOVED_BACKUP,
                                getFilePath(oldbackup)));
 					}
					logHistory(historyFile,
                        MessageService.getTextMessage(
                            MessageId.STORE_BACKUP_COMPLETED,
                            backupInstant));

                    // copy the updated version of history file with current
                    // backup information into the backup.
                    if(!privCopyFile(dbHistoryFile, backupHistoryFile))
                        throw StandardException. 
                            newException(SQLState.RAWSTORE_ERROR_COPYING_FILE,
                                         dbHistoryFile, backupHistoryFile);  
				}

				historyFile.close();
			}
			catch (IOException ioe)
			{
                try
                {
                    historyFile.close();
                }
                catch (IOException ioe2){};
                throw StandardException.newException(
                        SQLState.RAWSTORE_UNEXPECTED_EXCEPTION, ioe);
			}
		}

	}

    /**
     * Create a directory for backup.
     *
     * @param dir the directory to create
     * @throws StandardException if the directory could not be created
     */
    private void createBackupDirectory(File dir) throws StandardException {
        boolean created = false;
        IOException ex = null;

        try {
            created = privMkdirs(dir);
        } catch (IOException ioe) {
            ex = ioe;
        }

        // If the directory wasn't created, report it. Also include the
        // underlying IOException, if there was one.
        if (!created) {
            throw StandardException.newException(
                    SQLState.RAWSTORE_CANNOT_CREATE_BACKUP_DIRECTORY, ex, dir);
        }
    }

    /**
     * Backup the database to a backup directory and enable the log archive
	 * mode that will keep the archived log files required for roll-forward
	 * from this version backup.
     *
     * @param backupDir the name of the directory where the backup should be
     *                  stored. This directory will be created if it 
     *                  does not exist.   
     *
     * @param deleteOnlineArchivedLogFiles  
     *                  If true deletes online archived 
     *                  log files that exist before this backup, delete 
     *                  will occur  only after the backup is  complete.
     *
     * @param wait      if <tt>true</tt>, waits for  all the backup blocking 
     *                  operations in progress to finish.
     *
     * @exception StandardException thrown on error.
     */
    public void backupAndEnableLogArchiveMode(
    String backupDir,
    boolean deleteOnlineArchivedLogFiles,
    boolean wait) 
		throws StandardException
	{
        boolean enabledLogArchive = false;
        try {
            // Enable the log archive mode, if it is not already enabled.
            if(!logFactory.logArchived()) {
                logFactory.enableLogArchiveMode();
                enabledLogArchive = true ;
            }

            backup(backupDir, wait);
            
            // After successful backup delete the archived log files
            // that are not necessary to do a roll-forward recovery
            // from this backup if requested.
            if (deleteOnlineArchivedLogFiles)
            {
                logFactory.deleteOnlineArchivedLogFiles();
            }
        }catch (Throwable error) {
            // On any errors , disable the log archive, if it 
            // is enabled on this call. 
            if (enabledLogArchive)
                logFactory.disableLogArchiveMode();
            throw StandardException.plainWrapException(error);
        }
	}


    /*
     * Disable the log archive mode and delete the archived log files 
     * if requested. 
     *
     * @param deleteOnlineArchivedLogFiles  
     *              If true deletes online archived 
     *              log files that exist before this backup, delete 
     *              will occur  only after the backup is  complete.
     * @exception StandardException thrown on error.
     */
	public void disableLogArchiveMode(boolean deleteOnlineArchivedLogFiles)
		throws StandardException
	{
		logFactory.disableLogArchiveMode();
		if(deleteOnlineArchivedLogFiles)
		{
            logFactory.deleteOnlineArchivedLogFiles();
        }
	}

	
	/*
	 * Restore any remaining files from backup that are not 
	 * restored by the individual factories.  
	 *  1) copy jar files from backup..
	 *  2) copy backup history file. 
	 */
	private void restoreRemainingFromBackup(String backupPath) 
		throws StandardException 
	{
	
		// if they are any jar files in the backup copy, 
		// copy them into the database directory, if they
		// are not already there. 

		File backupJarDir = new File(backupPath, 
									 FileResource.JAR_DIRECTORY_NAME);

		StorageFile dbJarDir = 
			storageFactory.newStorageFile(FileResource.JAR_DIRECTORY_NAME);
		
		if (!privExists(dbJarDir) && privExists(backupJarDir)) 
		{
			if (!privCopyDirectory(backupJarDir, dbJarDir)) {
				throw StandardException.newException(
                         SQLState.UNABLE_TO_COPY_FILE_FROM_BACKUP, 
                         backupJarDir, dbJarDir);
			}
		}

		// copy the backup history file from the backup. 
		StorageFile dbHistoryFile = 
			storageFactory.newStorageFile(BACKUP_HISTORY);
		File backupHistoryFile = new File(backupPath, BACKUP_HISTORY);
	
		// if this is a roll-forward recovery, backup history file 
		// will already there in the database and will be the latest 
		// copy; if it exists, do not copy from backup.
		// Backup history may not exist at all if we did an offline
		// backup with os copy commands. In that case, don't try to 
		// copy the history file. (DERBY-3035)
		if (privExists(backupHistoryFile) && !privExists(dbHistoryFile))
			if (!privCopyFile(backupHistoryFile, dbHistoryFile))
				throw StandardException. 
					newException(SQLState.RAWSTORE_ERROR_COPYING_FILE,
								 backupHistoryFile, dbHistoryFile);  
	}


	public void idle() throws StandardException {
		dataFactory.idle();
	}


	public TransactionInfo[] getTransactionInfo()
	{
		return xactFactory.getTransactionInfo();
	}


	public ScanHandle openFlushedScan(DatabaseInstant start, int groupsIWant)
		 throws StandardException
	{
		return logFactory.openFlushedScan(start,groupsIWant);
	}

	public DaemonService getDaemon()
	{
		return rawStoreDaemon;
	}

	public void createFinished() throws StandardException
	{
		xactFactory.createFinished();
		dataFactory.createFinished();
	}

	/**
	 * Get JBMS properties relevant to raw store
	 * @exception StandardException Standard Derby Error Policy
	 */
	public void getRawStoreProperties(PersistentSet set)
		 throws StandardException
	{
		logFactory.getLogFactoryProperties(set);
	}


	/*
	** backup restore
	*/
	/**
		Freeze persistent store.  Reads can still happen, only cannot write.
		@exception StandardException Standard Derby Error Policy
	 */
	public void freezePersistentStore() throws StandardException
	{
		// do a checkpoint to get the persistent store up to date.
		logFactory.checkpoint(this, dataFactory, xactFactory, true);
		logFactory.freezePersistentStore();

	}

	/**
		Freeze persistent store.  Reads can still happen, only cannot write.
		@exception StandardException Standard Derby Error Policy
	 */
	public void unfreezePersistentStore() throws StandardException
	{
		logFactory.unfreezePersistentStore();
	}


	/*
	** data encryption/decryption support
	*/


    /**
     * Setup encryption engines according to the user properties and the
     * current database state.
     *
     * @param create whether a new database is being created, or if this is
     *      an existing database
     * @param properties database properties, including connection attributes
     * @return {@code true} if the existing data in the database should be
     *      transformed by applying a cryptographic operation.
     * @throws StandardException if the properties are conflicting, if the
     *      requested configuration is denied, or if something else goes wrong
     */
    private boolean setupEncryptionEngines(boolean create,
                                           Properties properties)
        throws StandardException
    {
        // Check if user has requested to decrypt the database.
        boolean decryptDatabase = isTrue(properties, Attribute.DECRYPT_DATABASE);
        // Check if user has requested to encrypt the database.
        boolean encryptDatabase = isTrue(properties, Attribute.DATA_ENCRYPTION);
        boolean reEncrypt = false;

        if (!create) {
            // Check if database is already encrypted, by directly peeking at
            // the database service propertes instead of the properties passed
            // to this method. By looking at properties passed to the boot
            // method, one can not differentiate between a request to encrypt
            // a plaintext database and booting an already encrypted database.
            // Attribute.DATA_ENCRYPTION is stored in the service properties
            // for encrypted database, and this takes precedence over the
            // value specified by the user.

            String name = Monitor.getMonitor().getServiceName(this);
            PersistentService ps = Monitor.getMonitor().getServiceType(this);
            String canonicalName = ps.getCanonicalServiceName(name);
            Properties serviceprops = ps.getServiceProperties(canonicalName,
                                                              (Properties)null);
            isEncryptedDatabase =
                    isTrue(serviceprops, Attribute.DATA_ENCRYPTION);

            if (isEncryptedDatabase) {
                // Check if the user has requested to re-encrypt an
                // encrypted datbase with a new encryption password/key.
                // See also attribute check in EmbedConnection.
                reEncrypt = isSet(properties, Attribute.NEW_BOOT_PASSWORD) ||
                        isSet(properties, Attribute.NEW_CRYPTO_EXTERNAL_KEY);
                encryptDatabase = reEncrypt;
            } else if (encryptDatabase && decryptDatabase) {
                // We cannot both encrypt and decrypt at the same time.
                throw StandardException.newException(
                        SQLState.CONFLICTING_BOOT_ATTRIBUTES,
                        Attribute.DECRYPT_DATABASE + ", " +
                        Attribute.DATA_ENCRYPTION);
            }

            // NOTE: if user specifies Attribute.DATA_ENCRYPTION on the
            // connection URL by mistake on an already encrypted database,
            // it is ignored.

            // Prevent attempts to (re)encrypt or decrypt a read-only database.
            if ((encryptDatabase || decryptDatabase) && isReadOnly()) {
                throw StandardException.newException(
                        SQLState.CANNOT_ENCRYPT_READONLY_DATABASE);
            }
        }

        // setup encryption engines.
        if (isEncryptedDatabase || encryptDatabase) {
            // Check if database is or will be encrypted. We save encryption
            // properties as service properties, such that
            // user does not have to specify them on the URL everytime.
            // Incase of re-encryption of an already of encrypted database
            // only some information needs to updated; it is not treated
            // like the configuring the database for encryption first time.
            boolean setupEncryption = create || (encryptDatabase && !reEncrypt);

            // start the cipher factory module, that is is used to create
            // instances of the cipher factory with specific enctyption
            // properties.

            CipherFactoryBuilder cb = (CipherFactoryBuilder)
                Monitor.startSystemModule(org.apache.derby.iapi.reference.Module.CipherFactoryBuilder);

            // create instance of the cipher factory with the
            // specified encryption properties.
            currentCipherFactory = cb.createCipherFactory(setupEncryption,
                                                          properties,
                                                          false);

            // The database can be encrypted using an encryption key that is
            // specified in the connection URL. For security reasons this key
            // is not persisted in the database, but it is necessary to verify
            // the encryption key whenever booting the database. This needs to
            // happen before we access the data/logs to avoid the risk of
            // corrupting the database because of a wrong encryption key.

            // Please note this verification process does not provide any added
            // security. Its purpose is to allow us to fail gracefully if an
            // incorrect encryption key is used during boot time.
            currentCipherFactory.verifyKey(
                    setupEncryption, storageFactory, properties);

            // Initializes the encryption and decryption engines.
            encryptionEngine = currentCipherFactory.createNewCipher(
                    CipherFactory.ENCRYPT);

            // At creation time of an encrypted database, store the encryption
            // block size for the algorithm. Store this value as property given
            // by RawStoreFactory.ENCRYPTION_BLOCKSIZE. This value is persisted
            // by storing it in service.properties
            // To connect to an existing database, retrieve the value and use
            // it for appropriate padding. The default value of encryption
            // block size is 8, to allow for downgrade issues.
            // Before support for AES (beetle6023), default encryption block
            // size supported was 8.

            if(setupEncryption) {
                encryptionBlockSize = encryptionEngine.getEncryptionBlockSize();
                // In case of database create, store the encryption block
                // size. In the case of reconfiguring an existing database,
                // this will be saved after encrypting the exisiting data.
                if (create) {
                    properties.put(RawStoreFactory.ENCRYPTION_BLOCKSIZE,
                                   String.valueOf(encryptionBlockSize));
                }
            } else {
                if (isSet(properties, RawStoreFactory.ENCRYPTION_BLOCKSIZE)) {
                    encryptionBlockSize =
                        Integer.parseInt(properties.getProperty(
                                RawStoreFactory.ENCRYPTION_BLOCKSIZE));
                } else {
                    encryptionBlockSize =
                        encryptionEngine.getEncryptionBlockSize();
                }
            }

            decryptionEngine = currentCipherFactory.
                createNewCipher(CipherFactory.DECRYPT);

            random = currentCipherFactory.getSecureRandom();

            if (encryptDatabase) {
                if (reEncrypt) {
                    // Create new cipher factory with the new encryption
                    // properties specified by the user. This cipher factory
                    // is used to create the new encryption/decryption
                    // engines to re-encrypt the database with the new
                    // encryption keys.
                    newCipherFactory = cb.createCipherFactory(setupEncryption,
                                                              properties,
                                                              true);
                    newDecryptionEngine =
                        newCipherFactory.createNewCipher(CipherFactory.DECRYPT);
                    newEncryptionEngine =
                        newCipherFactory.createNewCipher(CipherFactory.ENCRYPT);
                } else {
                    // There is only one engine when configuring an
                    // un-encrypted database for encryption.
                    newDecryptionEngine = decryptionEngine;
                    newEncryptionEngine = encryptionEngine;
                }
            }

            // Save the encryption properties if encryption is enabled
            // at database creation time.
            if(create) {
                currentCipherFactory.saveProperties(properties);
                isEncryptedDatabase = true;
            }
        }
        // We need to transform existing data if we are (re-)encrypting an
        // existing database, or decrypting an already encrypted database.
        return (!create &&
                (encryptDatabase || (isEncryptedDatabase && decryptDatabase)));
    }

	/**
		Encrypt cleartext into ciphertext.

		@see CipherProvider#encrypt

		@exception StandardException Standard Derby Error Policy
	 */
	public int encrypt(byte[] cleartext, int offset, int length,
					   byte[] ciphertext, int outputOffset, 
                       boolean newEngine)
		 throws StandardException
	{
        if ((encryptionEngine == null && newEncryptionEngine == null)) {
            throw StandardException.newException(
                        SQLState.STORE_FEATURE_NOT_IMPLEMENTED);
        }

		counter_encrypt++;

        if (newEngine) {
            return newEncryptionEngine.encrypt(cleartext, offset, length,
                                            ciphertext, outputOffset);
        } else {
            return encryptionEngine.encrypt(cleartext, offset, length,
                                            ciphertext, outputOffset);
        }
	}

	/**
		Decrypt cleartext from ciphertext.

		@see CipherProvider#decrypt

		@exception StandardException Standard Derby Error Policy
	 */
	public int decrypt(byte[] ciphertext, int offset, int length,
					   byte[] cleartext, int outputOffset) 
		 throws StandardException
	{
		if (isEncryptedDatabase == false || decryptionEngine == null) {
            throw StandardException.newException(
                        SQLState.STORE_FEATURE_NOT_IMPLEMENTED);
        }

		counter_decrypt++;
        return decryptionEngine.decrypt(ciphertext, offset, length,
                                            cleartext, outputOffset);
	}

	/**
		Returns the encryption block size used by the algorithm at time of
		creation of an encrypted database
	 */
	public int getEncryptionBlockSize()
	{
		return encryptionBlockSize;
	}

	public int random()
	{
		// don't synchronize it, the more random the better.
		return isEncryptedDatabase ? random.nextInt() : 0;
	}

	public Serializable changeBootPassword(Properties properties, Serializable changePassword)
		 throws StandardException
	{
		if (isReadOnly())
			throw StandardException.newException(SQLState.DATABASE_READ_ONLY);

		if (!isEncryptedDatabase)
			throw StandardException.newException(SQLState.DATABASE_NOT_ENCRYPTED);

		if (changePassword == null)
			throw StandardException.newException(SQLState.NULL_BOOT_PASSWORD);

		if (!(changePassword instanceof String))
			throw StandardException.newException(SQLState.NON_STRING_BP);

		// the new bootPassword is expected to be of the form
		// oldkey , newkey.
		String changeString = (String)changePassword;

		return currentCipherFactory.changeBootPassword((String)changePassword, properties, encryptionEngine);

	}


    /**
     * (re) encryption testing debug flags that are used to 
     * simulate error/crash conditions for testing purposes.
     * When any one of the following flags are set to true
     * in the debug mode, re-encryption will fail at that point.
     */

	public static final String TEST_REENCRYPT_CRASH_BEFORE_COMMT  = 
        SanityManager.DEBUG ? "TEST_REENCRYPT_CRASH_BEFORE_COMMT" : null ;
    public static final String TEST_REENCRYPT_CRASH_AFTER_COMMT  = 
        SanityManager.DEBUG ? "TEST_REENCRYPT_CRASH_AFTER_COMMT" : null ;
    public static final String TEST_REENCRYPT_CRASH_AFTER_SWITCH_TO_NEWKEY  = 
        SanityManager.DEBUG ? "TEST_REENCRYPT_CRASH_AFTER_SWITCH_TO_NEWKEY" : null ;
    public static final String TEST_REENCRYPT_CRASH_AFTER_CHECKPOINT  = 
        SanityManager.DEBUG ? "TEST_REENCRYPT_CRASH_AFTER_CHECKPOINT" : null ;
    public static final String 
        TEST_REENCRYPT_CRASH_AFTER_RECOVERY_UNDO_LOGFILE_DELETE =
        SanityManager.DEBUG ?
        "TEST_REENCRYPT_CRASH_AFTER_RECOVERY_UNDO_LOGFILE_DELETE" : null;
    public static final String 
        TEST_REENCRYPT_CRASH_AFTER_RECOVERY_UNDO_REVERTING_KEY =
        SanityManager.DEBUG ?
        "TEST_REENCRYPT_CRASH_AFTER_RECOVERY_UNDO_REVERTING_KEY" : null;
    public static final String 
        TEST_REENCRYPT_CRASH_BEFORE_RECOVERY_FINAL_CLEANUP =
        SanityManager.DEBUG ?
        "TEST_REENCRYPT_CRASH_BEFORE_RECOVERY_FINAL_CLEANUP" : null;
    
    

    /** 
     * when the input debug flag is set, an expception 
     * is throw when run in the debug mode.
     */
    private void crashOnDebugFlag(String debugFlag, 
                                  boolean reEncrypt) 
        throws StandardException
    {
        if (SanityManager.DEBUG)
        {
            // if the test debug flag is set, throw an 
            // exception to simulate error cases.
            if (SanityManager.DEBUG_ON(debugFlag))
            {
               StandardException se = StandardException.newException(
                                      SQLState.DATABASE_ENCRYPTION_FAILED,
                                      debugFlag);
               markCorrupt(se);
               throw se;
            }
        }
    }

    /*
     * Configure the database for encryption, with the  specified 
     * encryption  properties.
     *
     * Basic idea is to encrypt all the containers with new password/key 
     * specified by the user and keep old versions of the data to 
     * rollback the database to the state before the configuration of database 
     * with new encryption attributes. Users can configure the database with 
     * new encryption  attributes at boot time only; advantage of this approach
     * is that there will not be any concurrency issues to handle because
     * no users will be modifying the data. 
     *
     * First step is to encrypt the existing data with new encryption 
     * attributes  and then update the encryption properties for 
     * the database. Configuring  an un-encrypted database for 
     * encryption problem is a minor variation of  re-encrypting an 
     * encrypted database with new encryption key. The database 
     * reconfiguration with new encryption attributes is done under one
     * transaction, if there is a crash/error before it is committed, 
     * then it  is rolled back and the database will be brought back to the
     * state it was before the encryption.  
     *
     * One trickey case in (re) encrypion of database is 
     * unlike standard protocol transaction  commit means all done, 
     * database (re) encryption process has to perform a checkpoint
     *  with a newly generated key then only database  (re) encrption 
     * is complete, Otherwise the problem  is recovery has to deal 
     * with transaction log that is encrypted with old encryption key and 
     * the new encryption key. This probelm is avoided  writing COMMIT
     * and new  CHECKPOINT log record  to a new log file and encrypt the 
     * with a new key, if there is  crash before checkpoint records 
     * are updated , then on next boot the log file after the checkpoint 
     * is deleted before reovery,  which will be the one that is  
     * written with new encryption key and also contains COMMIT record, 
     * so the COMMIT record is also gone when  log file is deleted. 
     * Recovery will not see the commit , so it will  rollback the (re)
     * encryption and revert all the containers to the 
     * original versions. 
     * 
     * Old container versions are deleted only when the check point 
     * with new encryption key is successful, not on post-commit. 
     *
     * @param properties  properties related to this database.
     * @exception StandardException Standard Derby Error Policy
     */
    private void applyBulkCryptoOperation(Properties properties,
                                          CipherFactory newCipherFactory)
        throws StandardException 
    {
        boolean decryptDatabase = (isEncryptedDatabase &&
                isTrue(properties, Attribute.DECRYPT_DATABASE));
        boolean reEncrypt = (isEncryptedDatabase && (
                isSet(properties, Attribute.NEW_BOOT_PASSWORD) ||
                isSet(properties, Attribute.NEW_CRYPTO_EXTERNAL_KEY)));
        if (SanityManager.DEBUG) {
            SanityManager.ASSERT(decryptDatabase || reEncrypt || (
                    !isEncryptedDatabase &&
                    isSet(properties, Attribute.DATA_ENCRYPTION)));
        }

        // Check if the cryptographic operation can be performed.
        cryptoOperationAllowed(reEncrypt, decryptDatabase);

        boolean externalKeyEncryption =
                isSet(properties, Attribute.CRYPTO_EXTERNAL_KEY);

        // check point the datase, so that encryption does not have
        // to encrypt the existing transactions logs. 
 
        logFactory.checkpoint(this, dataFactory, xactFactory, true);

        // start a transaction that is to be used for encryting the database
        RawTransaction transaction =
            xactFactory.startTransaction(
                   this,
                    ContextService.getFactory().getCurrentContextManager(),
                    AccessFactoryGlobals.USER_TRANS_NAME);

        try 
		{
			
            if (decryptDatabase) {
                dataFactory.decryptAllContainers(transaction);
            } else {
                dataFactory.encryptAllContainers(transaction);
            }

            // all the containers are (re) encrypted, now mark the database as
            // encrypted if a plain database is getting configured for encryption
            // or update the encryption the properties, in the 
            // service.properties ..etc.

            
            if (SanityManager.DEBUG) {
                crashOnDebugFlag(TEST_REENCRYPT_CRASH_BEFORE_COMMT, reEncrypt);
            }

            // check if the checkpoint is currently in the last log file, 
            // otherwise force a checkpoint and then do a log switch, 
            // after setting up a new encryption key
            if (!logFactory.isCheckpointInLastLogFile()) 
            {
                // perfrom a checkpoint, this is a reference checkpoint 
                // to find if the re(encryption) is complete. 
                logFactory.checkpoint(this, dataFactory, xactFactory, true);
            }

            // Let the log factory and data factory know whether the
            // database is encrypted.
            if (decryptDatabase) {
                isEncryptedDatabase = false;
                logFactory.setDatabaseEncrypted(false, true);
                dataFactory.setDatabaseEncrypted(false);
            } else {
                // Let the log factory know that database is
                // (re-)encrypted and ask it to flush the log
                // before enabling encryption of the log with
                // the new key.
                logFactory.setDatabaseEncrypted(true, true);

                if (reEncrypt) {
                    // Switch the encryption/decryption engine to the new ones.
                    decryptionEngine = newDecryptionEngine;
                    encryptionEngine = newEncryptionEngine;
                    currentCipherFactory = newCipherFactory;
                } else {
                    // Mark in the raw store that the database is encrypted.
                    isEncryptedDatabase = true;
                    dataFactory.setDatabaseEncrypted(true);
                }
            }

            
            // make the log factory ready to encrypt
            // the transaction log with the new encryption 
            // key by switching to a new log file. 
            // If re-encryption is aborted for any reason, 
            // this new log file will be deleted, during
            // recovery.

            logFactory.startNewLogFile();

            // mark that re-encryption is in progress in the 
            // service.properties, so that (re) encryption 
            // changes that can not be undone using the transaction 
            // log can be un-done before recovery starts.
            // (like the changes to service.properties and 
            // any log files the can not be understood by the
            // old encryption key), incase engine crashes
            // after this point. 

            // if the crash occurs before this point, recovery
            // will rollback the changes using the transaction 
            // log.

            properties.put(RawStoreFactory.DB_ENCRYPTION_STATUS,
                           String.valueOf(
                               RawStoreFactory.DB_ENCRYPTION_IN_PROGRESS));

            if (reEncrypt) 
            {
                // incase re-encryption, save the old 
                // encryption related properties, before
                // doing updates with new values.

                if (externalKeyEncryption) 
                {
                    // save the current copy of verify key file.
                    StorageFile verifyKeyFile = 
                        storageFactory.newStorageFile(
                                 Attribute.CRYPTO_EXTERNAL_KEY_VERIFY_FILE);
                    StorageFile oldVerifyKeyFile = 
                        storageFactory.newStorageFile(
                          RawStoreFactory.CRYPTO_OLD_EXTERNAL_KEY_VERIFY_FILE);

                    if(!privCopyFile(verifyKeyFile, oldVerifyKeyFile))
                        throw StandardException.
                            newException(SQLState.RAWSTORE_ERROR_COPYING_FILE,
                                         verifyKeyFile, oldVerifyKeyFile); 

                    // update the verify key file with the new key info.
                    currentCipherFactory.verifyKey(reEncrypt, 
                                                   storageFactory, 
                                                   properties);
                } else 
                {
                    // save the current generated encryption key 
                    String keyString = 
                        properties.getProperty(
                                               RawStoreFactory.ENCRYPTED_KEY);
                    if (keyString != null)
                        properties.put(RawStoreFactory.OLD_ENCRYPTED_KEY,
                                       keyString);
                }
            } else if (decryptDatabase) {
                // We cannot remove the encryption properties here, as we may
                // have to revert back to the encrypted database. Instead we set
                // dataEncryption to false and leave all other encryption
                // attributes unchanged. This requires that Derby doesn't store
                // dataEncryption=false for un-encrypted database, otherwise
                // handleIncompleteDbCryptoOperation will be confused.
                properties.put(Attribute.DATA_ENCRYPTION, "false");
            } else {
                // save the encryption block size;
                properties.put(RawStoreFactory.ENCRYPTION_BLOCKSIZE,
                               String.valueOf(encryptionBlockSize));
            }

            // save the new encryption properties into service.properties
            currentCipherFactory.saveProperties(properties) ;
 
            if (SanityManager.DEBUG) {
                crashOnDebugFlag(
                                 TEST_REENCRYPT_CRASH_AFTER_SWITCH_TO_NEWKEY,
                                 reEncrypt);
            }

            // commit the transaction that is used to 
            // (re) encrypt the database. Note that 
            // this will be logged with newly generated 
            // encryption key in the new log file created 
            // above.
            transaction.commit();

            if (SanityManager.DEBUG) {
                crashOnDebugFlag(TEST_REENCRYPT_CRASH_AFTER_COMMT, 
                                 reEncrypt);
            }

            // force the checkpoint with new encryption key.
            logFactory.checkpoint(this, dataFactory, xactFactory, true);

            if (SanityManager.DEBUG) {
                crashOnDebugFlag(TEST_REENCRYPT_CRASH_AFTER_CHECKPOINT, 
                                 reEncrypt);
            }

            // once the checkpont makes it to the log, re-encrption 
            // is complete. only cleanup is remaining ; update the 
            // re-encryption status flag to cleanup. 
            properties.put(RawStoreFactory.DB_ENCRYPTION_STATUS,
                           String.valueOf(
                               RawStoreFactory.DB_ENCRYPTION_IN_CLEANUP));

            // database is (re)encrypted successfuly, 
            // remove the old version of the container files.
            dataFactory.removeOldVersionOfContainers();
                
            if (decryptDatabase) {
                // By now we can remove all cryptographic properties.
                removeCryptoProperties(properties);
            } else if (reEncrypt) {
                if (externalKeyEncryption)
                {
                    // remove the saved copy of the verify.key file
                    StorageFile oldVerifyKeyFile = 
                        storageFactory.newStorageFile(
                          RawStoreFactory.CRYPTO_OLD_EXTERNAL_KEY_VERIFY_FILE);
                    if (!privDelete(oldVerifyKeyFile))
                        throw StandardException.newException(
                                    SQLState.UNABLE_TO_DELETE_FILE, 
                                    oldVerifyKeyFile);
                } else 
                {
                    // remove the old encryption key property.
                    properties.remove(RawStoreFactory.OLD_ENCRYPTED_KEY);
                }
            }

            // (re) encrypion is done,  remove the (re) 
            // encryption status property. 

            properties.remove(RawStoreFactory.DB_ENCRYPTION_STATUS);

            // close the transaction. 
            transaction.close(); 

        } catch (StandardException se) {
            throw StandardException.newException(
                SQLState.DATABASE_ENCRYPTION_FAILED, se, se.getMessage());
        } finally {
            // clear the new encryption engines.
            newDecryptionEngine = null;   
            newEncryptionEngine = null;
        }
    }


    /**
     * Engine might have crashed during encryption of un-encrypted datbase
     * or while re-encryptin an already encrypted database with a new key
     * after all the containers or (re) encrypted. If crash has occured
     * before all containers are encrypted, recovery wil un-do re-encryption
     * using the transaction log, nothing to be done here.
     *
     * If crash has occured after database encryption status flag 
     * (RawStoreFactory.DB_ENCRYPTION_STATUS) is set, this method 
     * will do any cleanup necessary for the recovery to correctly
     * perform the rollback if required. 
     *
     *
     *
     * @param properties  properties related to this database.
     * @exception StandardException Standard Derby Error Policy
     *
     */
    public void handleIncompleteDbCryptoOperation(Properties properties)
        throws StandardException
    {
        // find what was the encryption status before database crashed. 
        int dbEncryptionStatus = 0; 
        String dbEncryptionStatusStr = 
            properties.getProperty(RawStoreFactory.DB_ENCRYPTION_STATUS);
        if ( dbEncryptionStatusStr != null) 
            dbEncryptionStatus = Integer.parseInt(dbEncryptionStatusStr);

        boolean reEncryption = false;
        boolean decryptionFailed =
                isSet(properties, Attribute.DATA_ENCRYPTION) &&
                !isTrue(properties, Attribute.DATA_ENCRYPTION);
        // check if engine crashed when (re) encryption was in progress.
        if (dbEncryptionStatus == RawStoreFactory.DB_ENCRYPTION_IN_PROGRESS)
        {

            // check if it crashed immediately after completion or
            // before. if the checkpoint is in the last log file 
            // encrypted with new encryption key, it is as good 
            // as complete. In this case just cleanup any uncleared
            // flags and mark that database is encrypted.

            if(logFactory.isCheckpointInLastLogFile()) 
            {
                // database (re)encryption was successful, only 
                // cleanup is remaining. change the status to cleanup. 
                dbEncryptionStatus = RawStoreFactory.DB_ENCRYPTION_IN_CLEANUP;
            }else {

                // crash occured before re-encrytion was completed. 
                // update the db re-encryption status and write to 
                // the service.properties that re-encryption 
                // needs to be undone. The reason this status need 
                // to be made persistent, it will help to correctly 
                // handle a crash in this routine after the log file 
                // encrypted with new key is deleted. If this flag
                // is not set, on next reboot, above check 
                // will find checkpoint in the last log file and 
                // incorrecly assume (re) encryption is
                // successful.

                dbEncryptionStatus =  RawStoreFactory.DB_ENCRYPTION_IN_UNDO;
                properties.put(RawStoreFactory.DB_ENCRYPTION_STATUS,
                               String.valueOf(dbEncryptionStatus));
            }
        }

        
        if (dbEncryptionStatus == RawStoreFactory.DB_ENCRYPTION_IN_UNDO)
        {
            // delete the log file after the log file that has the checkpoint , 
            // it has the data encrypted with the new key, including the commit
            // record for the transaction that was used to (re)encrypt 
            // the database. By Deleting the log file, we are forcing the
            // recovery to rollback the (re)encryption of the database. 

            logFactory.deleteLogFileAfterCheckpointLogFile();
                
            if (SanityManager.DEBUG) {
                crashOnDebugFlag(
                   TEST_REENCRYPT_CRASH_AFTER_RECOVERY_UNDO_LOGFILE_DELETE, 
                   reEncryption);
            }

            // Note : If a crash occurs at this point, then on reboot 
            // it will again be in the DB_ENRYPTION_IN__UNDO state, 
            // there will not be a file after the checkpoint log file, 
            // so no file will be deleted. 

            // check if this is a external key encryption and 
            // if it replace the current verify key file with 
            // the old copy. 

            StorageFile verifyKeyFile = 
                storageFactory.newStorageFile(
                                 Attribute.CRYPTO_EXTERNAL_KEY_VERIFY_FILE);
            
            if (privExists(verifyKeyFile))
            {
                StorageFile oldVerifyKeyFile = 
                    storageFactory.newStorageFile(
                      RawStoreFactory.CRYPTO_OLD_EXTERNAL_KEY_VERIFY_FILE);
            
                if (privExists(oldVerifyKeyFile)) 
                {
                    if(!privCopyFile(oldVerifyKeyFile, verifyKeyFile))
                        throw StandardException.
                            newException(SQLState.RAWSTORE_ERROR_COPYING_FILE,
                                         oldVerifyKeyFile, verifyKeyFile);  
                    
                    // only incase of re-encryption there should
                    // be old verify key file. 
                    reEncryption = true;
                } else if (!decryptionFailed) {
                    // remove the verify key file. 
                    if (!privDelete(verifyKeyFile))
                        throw StandardException.newException(
                             SQLState.UNABLE_TO_DELETE_FILE, 
                             verifyKeyFile);
                }

            } else 
            {
                // database enrypted with boot password. 
                
                // replace the current encryption key with the old key
                // in the service.properties file. 
                // retreive the old encryption key 

                String OldKeyString = 
                    properties.getProperty(RawStoreFactory.OLD_ENCRYPTED_KEY);

                if (OldKeyString != null) {
                    // set the current encrypted key to the old one. 
                    properties.put(RawStoreFactory.ENCRYPTED_KEY,
                                   OldKeyString);
                    
                    // only incase of re-encryption there should
                    // be old encryted key . 
                    reEncryption = true;
                }
            }

            if (SanityManager.DEBUG) {
                SanityManager.ASSERT(!(decryptionFailed && reEncryption));
                crashOnDebugFlag(
                    TEST_REENCRYPT_CRASH_AFTER_RECOVERY_UNDO_REVERTING_KEY, 
                    reEncryption);
            }

            if (!decryptionFailed && !reEncryption) {
                // Crash occurred when an un-encrypted database was being
                // encrypted, all encryption properties should be removed from
                // service.properties since we are undoing the attempt.
                removeCryptoProperties(properties);
            }
        } // end of UNDO


        if (dbEncryptionStatus == RawStoreFactory.DB_ENCRYPTION_IN_CLEANUP)
        {
            // remove all the old versions of the  containers. 
            dataFactory.removeOldVersionOfContainers();
        }
        
        if (SanityManager.DEBUG) {
                crashOnDebugFlag(
                   TEST_REENCRYPT_CRASH_BEFORE_RECOVERY_FINAL_CLEANUP, 
                   reEncryption);
        }

        // either the (re) encryption was complete , 
        // or undone (except for rollback that needs to be 
        // done by the recovery). Remove re-encryption specific
        // flags from the service.properties and old copy 
        // of the verify key file.
        
        // delete the old verify key file , if it exists. 
        StorageFile oldVerifyKeyFile = 
            storageFactory.newStorageFile(
                      RawStoreFactory.CRYPTO_OLD_EXTERNAL_KEY_VERIFY_FILE);
        if (privExists(oldVerifyKeyFile)) 
        {
            if (!privDelete(oldVerifyKeyFile))
                throw StandardException.newException(
                        SQLState.UNABLE_TO_DELETE_FILE, 
                        oldVerifyKeyFile);
        } else 
        {
            // remove the old encryption key property.
            properties.remove(RawStoreFactory.OLD_ENCRYPTED_KEY);
        }

        // Finalize cleanup for failed decryption attempts.
        if (decryptionFailed) {
            if (dbEncryptionStatus == RawStoreFactory.DB_ENCRYPTION_IN_UNDO) {
                // This action is not idempotent in the sense that once set
                // Derby can't detect that what failed was a decryption attempt
                // if DB_ENCRYPTION_STATUS is kept unchanged. Too reduce the
                // window of opportunity this is done here, but should really
                // be atomic with the removal of DB_ENCRYPTION_STATUS.
                properties.setProperty(Attribute.DATA_ENCRYPTION, "true");
            } else {
                removeCryptoProperties(properties);
            }
        }
        // remove the re-encryptin status flag. 
        properties.remove(RawStoreFactory.DB_ENCRYPTION_STATUS);
    }




    /**
     * checks if the database is in the right state to (re)encrypt it.
     *
     * @param reEncrypt {@code true} if the database is getting encrypted with
     *      a new password/key
     * @param decrypt {@code true} if the database is getting decrypted
     * @exception  StandardException  
     *             if there is global transaction in the prepared state or
     *             if the database is not at the required version or above,
     *             this feature is not supported or
     *             if the log is archived for the database.
     */
    private void cryptoOperationAllowed(boolean reEncrypt, boolean decrypt)
        throws StandardException 
    {

        String feature;
        if (decrypt) {
            feature = Attribute.DECRYPT_DATABASE + " attribute";
        } else if (reEncrypt) {
            feature = Attribute.NEW_BOOT_PASSWORD + "/" +
                    Attribute.NEW_CRYPTO_EXTERNAL_KEY + " attribute";
        } else {
            feature = Attribute.DATA_ENCRYPTION +
                    " attribute on an existing database";
        }

        // Check if database version is sufficient for the requested feature.
        // Encryption or re-encryption of the database is supported only in
        // version 10.2 or above, decryption is only supported in version
        // 10.10 or above.
        int requiredMinorVersion = decrypt ?
                RawStoreFactory.DERBY_STORE_MINOR_VERSION_10 :
                RawStoreFactory.DERBY_STORE_MINOR_VERSION_2;

        logFactory.checkVersion(
                       RawStoreFactory.DERBY_STORE_MAJOR_VERSION_10, 
                       requiredMinorVersion,
                       feature);

        // database can not be (re)encrypted if there 
        // are any global transactions in the prepared state 
        // after the recovery. The reason for this restriction 
        // is that any transaction log before the encryption can not 
        // be read once database is reconfigure with new encryption 
        // key.
        if (xactFactory.hasPreparedXact()) {
            throw StandardException.newException(
                    SQLState.ENCRYPTION_PREPARED_XACT_EXIST);
        }


        // check if the database has the log archived. 
        // database can not be congured of encryption or
        // or re-encrypt it with a new key when the database 
        // log is being archived. The reason for this restriction is 
        // it will create a scenarion where users will 
        // have some logs encrypted with new key and some with old key 
        // when rollforward recovery is performed. 
    
        if (logFactory.logArchived()) {
            throw StandardException.newException(
                   SQLState.CANNOT_ENCRYPT_LOG_ARCHIVED_DATABASE);
        }
    }


	/*
	**
	*/

	public StandardException markCorrupt(StandardException originalError) {

		logFactory.markCorrupt(originalError);
		dataFactory.markCorrupt(originalError);
		xactFactory.markCorrupt(originalError);

		return originalError;
	}

	/*
	 * class specific methods
	 */

	/* subclass can override this method to load different submodules */
	public String getTransactionFactoryModule()
	{
		return TransactionFactory.MODULE;
	}

    public String getSlaveFactoryModule() {
        return SlaveFactory.MODULE;
    }

    public String getMasterFactoryModule()
    {
        return MasterFactory.MODULE;
    }

	public String getDataFactoryModule()
	{
		return DataFactory.MODULE;
	}

	public String getLogFactoryModule()
	{
		return LogFactory.MODULE;
	}


	private void logHistory(OutputStreamWriter historyFile, String msg) throws IOException
	{
		Date d = new Date();
		historyFile.write(d.toString() + ":" + msg + "\n");
		historyFile.flush();
	}

    /*
     * Get the file path. If the canonical path can be obtained then return the 
     * canonical path, otherwise just return the abstract path. Typically if
     * there are no permission to read user.dir when  running under security
     * manager canonical path can not be obtained.
     *
     * This method is used to a write path name to error/status log file, where it
     * would be nice to print full paths but not esstential that the user 
     * grant permissions to read user.dir property.
     */
    private String getFilePath(StorageFile file) {
        String path = privGetCanonicalPath(file);
        if(path != null ) {
            return path;
        }else {
            //can not get the canoncal path, 
            // return the abstract path
            return file.getPath();
        }
    }

    /*
     * Get the file path.  If the canonical path can be obtained then return the 
     * canonical path, otherwise just return the abstract path. Typically if
     * there are no permission to read user.dir when  running under security
     * manager canonical path can not be obtained.
     *
     * This method is used to a write a file path name to error/status log file, 
     * where it would be nice to print full paths but not esstential that the user
     * grant permissions to read user.dir property.
     *
     */
    private String getFilePath(File file) {
        String path = privGetCanonicalPath(file);
        if(path != null ) {
            return path;
        }else {
            // can not get the canoncal path, 
            // return the abstract path
            return file.getPath();
        }
    }

	protected boolean privCopyDirectory(StorageFile from, File to)
            throws StandardException
	{
		return privCopyDirectory(from, to, (byte[])null, 
                                 (String[])null, true);
	}

	protected boolean privCopyDirectory(File from, StorageFile to)
	{
		return privCopyDirectory(from, to, (byte[])null, (String[])null);
	}

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
    public long getMaxContainerId()
		throws StandardException
    {
        return(dataFactory.getMaxContainerId());
    }

    /**
     *  Check to see if a database has been upgraded to the required
     *  level in order to use a store feature.
     *
     * @param requiredMajorVersion  required database Engine major version
     * @param requiredMinorVersion  required database Engine minor version
     * @param feature               Non-null to throw an exception, null to 
     *                              return the state of the version match.
     *
     * @return <code> true </code> if the database has been upgraded to 
     *         the required level, <code> false </code> otherwise.
     *
     * @exception  StandardException 
     *             if the database is not at the require version 
     *             when <code>feature</code> feature is 
     *             not <code> null </code>. 
     */
	public boolean checkVersion(
    int     requiredMajorVersion, 
    int     requiredMinorVersion, 
    String  feature) 
        throws StandardException
    {
        return(
            logFactory.checkVersion(
                requiredMajorVersion, requiredMinorVersion, feature));
    }

    /**
     * Removes properties related to encrypted databases.
     *
     * @param properties property set to remove from
     */
    private void removeCryptoProperties(Properties properties) {
        // Common props for external key or password.
        properties.remove(Attribute.DATA_ENCRYPTION);
        properties.remove(RawStoreFactory.LOG_ENCRYPT_ALGORITHM_VERSION);
        properties.remove(RawStoreFactory.DATA_ENCRYPT_ALGORITHM_VERSION);
        properties.remove(RawStoreFactory.ENCRYPTION_BLOCKSIZE);

        // Properties specific to password based encryption.
        properties.remove(Attribute.CRYPTO_KEY_LENGTH);
        properties.remove(Attribute.CRYPTO_PROVIDER);
        properties.remove(Attribute.CRYPTO_ALGORITHM);
        properties.remove(RawStoreFactory.ENCRYPTED_KEY);
    }
	
    /*
        These methods require Priv Blocks when run under a security manager.
    */

	private synchronized OutputStreamWriter privFileWriter( StorageFile fileName, boolean append) throws IOException
	{
        actionCode = FILE_WRITER_ACTION;
        actionStorageFile = fileName;
        actionAppend = append;
		try{
			return (OutputStreamWriter) java.security.AccessController.doPrivileged( this);
		}catch (java.security.PrivilegedActionException pae)
        {
            throw (IOException) pae.getException();
        }
        finally
        {
            actionStorageFile = null;
        }
	}

	private synchronized boolean privExists( File file)
    {
        actionCode = REGULAR_FILE_EXISTS_ACTION;
        actionRegularFile = file;

        try
        {
            Object ret = AccessController.doPrivileged( this);
            return ((Boolean) ret).booleanValue();
        }
        catch( PrivilegedActionException pae) { return false;} // does not throw an exception
        finally
        {
            actionRegularFile = null;
        }
    }

	private synchronized boolean privExists(final StorageFile file)
    {
        actionCode = STORAGE_FILE_EXISTS_ACTION;
        actionStorageFile = file;

        try
        {
            Object ret = AccessController.doPrivileged( this);
            return ((Boolean) ret).booleanValue();
        }
        catch( PrivilegedActionException pae) { return false;} // does not throw an exception
        finally
        {
            actionStorageFile = null;
        }
    }

    private synchronized OutputStreamWriter privGetOutputStreamWriter(StorageFile file)
        throws IOException
    {
        actionCode = README_FILE_OUTPUTSTREAM_WRITER_ACTION;
        actionStorageFile = file;
        try
        {
            return (OutputStreamWriter) java.security.AccessController.doPrivileged(this);
        }
        catch (java.security.PrivilegedActionException pae)
        {
            throw (IOException) pae.getException();
        }
    }

    private synchronized boolean privDelete( File file)
    {
        actionCode = REGULAR_FILE_DELETE_ACTION;
        actionRegularFile = file;

        try
        {
            Object ret = AccessController.doPrivileged( this);
            return ((Boolean) ret).booleanValue();
        }
        catch( PrivilegedActionException pae) { return false;} // does not throw an exception
        finally
        {
            actionRegularFile = null;
        }
    }

    private synchronized boolean privDelete(StorageFile file)
    {
        actionCode = STORAGE_FILE_DELETE_ACTION;
        actionStorageFile = file;

        try
        {
            Object ret = AccessController.doPrivileged( this);
            return ((Boolean) ret).booleanValue();
        }
        catch( PrivilegedActionException pae) { return false;} // does not throw an exception
        finally
        {
            actionStorageFile = null;
        }
    }



    private synchronized boolean privMkdirs(File file) throws IOException
    {
        actionCode = REGULAR_FILE_MKDIRS_ACTION;
        actionRegularFile = file;

        try
        {
            Object ret = AccessController.doPrivileged( this);
            return ((Boolean) ret).booleanValue();
        }
        catch (PrivilegedActionException pae) {
            throw (IOException) pae.getCause();
        }
        finally
        {
            actionRegularFile = null;
        }
    }


    private synchronized boolean privIsDirectory( File file)
    {
        actionCode = REGULAR_FILE_IS_DIRECTORY_ACTION;
        actionRegularFile = file;

        try
        {
            Object ret = AccessController.doPrivileged( this);
            return ((Boolean) ret).booleanValue();
        }
        catch( PrivilegedActionException pae) { return false;} // does not throw an exception
        finally
        {
            actionRegularFile = null;
        }
    }

    private synchronized boolean privRemoveDirectory( File file)
    {
        actionCode = REGULAR_FILE_REMOVE_DIRECTORY_ACTION;
        actionRegularFile = file;

        try
        {
            Object ret = AccessController.doPrivileged( this);
            return ((Boolean) ret).booleanValue();
        }
        catch( PrivilegedActionException pae) { return false;} // does not throw an exception
        finally
        {
            actionRegularFile = null;
        }
    }

    private synchronized boolean privRenameTo( File file1, File file2)
    {
        actionCode = REGULAR_FILE_RENAME_TO_ACTION;
        actionRegularFile = file1;
        actionRegularFile2 = file2;

        try
        {
            Object ret = AccessController.doPrivileged( this);
            return ((Boolean) ret).booleanValue();
        }
        catch( PrivilegedActionException pae) { return false;} // does not throw an exception
        finally
        {
            actionRegularFile = null;
            actionRegularFile2 = null;
        }
    }

    private synchronized boolean privCopyDirectory(StorageFile from, 
                                                   File to, 
                                                   byte[] buffer, 
                                                   String[] filter,
                                                   boolean copySubdirs)
            throws StandardException
    {
        actionCode = COPY_STORAGE_DIRECTORY_TO_REGULAR_ACTION;
        actionStorageFile = from;
        actionRegularFile = to;
        actionBuffer = buffer;
        actionFilter = filter;
        actionCopySubDirs = copySubdirs;

        try
        {
            Object ret = AccessController.doPrivileged( this);
            return ((Boolean) ret).booleanValue();
        }
        catch( PrivilegedActionException pae) {
            throw (StandardException)pae.getCause();
        }
        finally
        {
            actionStorageFile = null;
            actionRegularFile = null;
            actionBuffer = null;
            actionFilter = null;
        }
    }


    private synchronized boolean privCopyDirectory( File from, StorageFile to, byte[] buffer, String[] filter)
    {
        actionCode = COPY_REGULAR_DIRECTORY_TO_STORAGE_ACTION;
        actionStorageFile = to;
        actionRegularFile = from;
        actionBuffer = buffer;
        actionFilter = filter;

        try
        {
            Object ret = AccessController.doPrivileged( this);
            return ((Boolean) ret).booleanValue();
        }
        catch( PrivilegedActionException pae) { return false;} // does not throw an exception
        finally
        {
            actionStorageFile = null;
            actionRegularFile = null;
            actionBuffer = null;
            actionFilter = null;
        }
    }

	
    private synchronized boolean privCopyFile( File from, StorageFile to)
    {
        actionCode = COPY_REGULAR_FILE_TO_STORAGE_ACTION;
        actionStorageFile = to;
        actionRegularFile = from;

        try
        {
            Object ret = AccessController.doPrivileged( this);
            return ((Boolean) ret).booleanValue();
        }
        catch( PrivilegedActionException pae) { return false;} // does not throw an exception
        finally
        {
            actionStorageFile = null;
            actionRegularFile = null;
        }
    }

    private synchronized boolean privCopyFile( StorageFile from, File to)
            throws StandardException
    {
        actionCode = COPY_STORAGE_FILE_TO_REGULAR_ACTION;
        actionStorageFile = from;
        actionRegularFile = to;

        try
        {
            Object ret = AccessController.doPrivileged( this);
            return ((Boolean) ret).booleanValue();
        }
        catch( PrivilegedActionException pae) {
            throw (StandardException)pae.getCause();
        }
        finally
        {
            actionStorageFile = null;
            actionRegularFile = null;
        }
    }


    
    private synchronized boolean privCopyFile( StorageFile from, StorageFile to)
    {
        actionCode = COPY_STORAGE_FILE_TO_STORAGE_ACTION;
        actionStorageFile = from;
        actionToStorageFile = to;

        try
        {
            Object ret = AccessController.doPrivileged( this);
            return ((Boolean) ret).booleanValue();
        }
        catch( PrivilegedActionException pae) { return false;} // does not throw an exception
        finally
        {
            actionStorageFile = null;
            actionToStorageFile = null;
        }
    }


    private synchronized String[] privList(final StorageFile file)
    {
        actionCode = STORAGE_FILE_LIST_DIRECTORY_ACTION;
        actionStorageFile = file;

        try
        {
            return (String[]) AccessController.doPrivileged( this);
        }
        catch( PrivilegedActionException pae) { return null;} // does not throw an exception
        finally
        {
            actionStorageFile = null;
        }
    }


    private synchronized String privGetCanonicalPath(final StorageFile file)
    {
        actionCode = STORAGE_FILE_GET_CANONICALPATH_ACTION;
        actionStorageFile = file;

        try
        {
            return (String) AccessController.doPrivileged( this);
        }
        catch( PrivilegedActionException pae) {
            return null;
        } // does not throw an exception
        catch(SecurityException se) {
            // there are no permission to get canonical path 
            // just return null.
            return null;
        }
        finally
        {
            actionStorageFile = null;
        }
    }


    private synchronized String privGetCanonicalPath(final File file)
    {
        actionCode = REGULAR_FILE_GET_CANONICALPATH_ACTION;
        actionRegularFile = file;

        try
        {
            return (String) AccessController.doPrivileged( this);
        }
        catch( PrivilegedActionException pae) {
            return null;
        } // does not throw an exception
        catch(SecurityException se) { 
            // there are no permission to get canonical path 
            // just return null.
            return null;
        }
        finally
        {
            actionRegularFile = null;
        }
    }


    // PrivilegedExceptionAction method
    public final Object run() throws IOException, StandardException
    {
        switch(actionCode)
        {
        case FILE_WRITER_ACTION:
            // SECURITY PERMISSION - MP1
            return new OutputStreamWriter( actionStorageFile.getOutputStream( actionAppend));

        case REGULAR_FILE_EXISTS_ACTION:
            return ReuseFactory.getBoolean(actionRegularFile.exists());

        case STORAGE_FILE_EXISTS_ACTION:
            return ReuseFactory.getBoolean(actionStorageFile.exists());

        case REGULAR_FILE_DELETE_ACTION:
            return ReuseFactory.getBoolean(actionRegularFile.delete());

        case STORAGE_FILE_DELETE_ACTION:
            return ReuseFactory.getBoolean(actionStorageFile.delete());

        case REGULAR_FILE_MKDIRS_ACTION:
            // SECURITY PERMISSION - OP4
            boolean created = actionRegularFile.mkdirs();

            FileUtil.limitAccessToOwner(actionRegularFile);

            return ReuseFactory.getBoolean(created);

        case REGULAR_FILE_IS_DIRECTORY_ACTION:
            // SECURITY PERMISSION - MP1
            return ReuseFactory.getBoolean(actionRegularFile.isDirectory());

        case REGULAR_FILE_REMOVE_DIRECTORY_ACTION:
            // SECURITY PERMISSION - MP1, OP5
            return ReuseFactory.getBoolean(FileUtil.removeDirectory(actionRegularFile));

        case REGULAR_FILE_RENAME_TO_ACTION:
            // SECURITY PERMISSION - OP4
            return ReuseFactory.getBoolean(actionRegularFile.renameTo(actionRegularFile2));

        case COPY_STORAGE_DIRECTORY_TO_REGULAR_ACTION:
            // SECURITY PERMISSION - MP1, OP4
            return ReuseFactory.getBoolean(FileUtil.copyDirectory(storageFactory,
                                                                  actionStorageFile,
                                                                  actionRegularFile,
                                                                  actionBuffer,
                                                                  actionFilter,
                                                                  actionCopySubDirs));

        case COPY_REGULAR_DIRECTORY_TO_STORAGE_ACTION:
            // SECURITY PERMISSION - MP1, OP4
            return ReuseFactory.getBoolean(FileUtil.copyDirectory((WritableStorageFactory)storageFactory,
                                                                  actionRegularFile,
                                                                  actionStorageFile,
                                                                  actionBuffer,
                                                                  actionFilter));

        case COPY_REGULAR_FILE_TO_STORAGE_ACTION:
            // SECURITY PERMISSION - MP1, OP4
            return ReuseFactory.getBoolean(FileUtil.copyFile((WritableStorageFactory) storageFactory,
                                                             actionRegularFile,
                                                             actionStorageFile));

        case REGULAR_FILE_LIST_DIRECTORY_ACTION:
            // SECURITY PERMISSION - MP1
            return (String[])(actionRegularFile.list());

        case STORAGE_FILE_LIST_DIRECTORY_ACTION:
            // SECURITY PERMISSION - MP1
            return (String[])(actionStorageFile.list());

        case COPY_STORAGE_FILE_TO_REGULAR_ACTION:
            // SECURITY PERMISSION - MP1, OP4
            return ReuseFactory.getBoolean(FileUtil.copyFile(
                                           (WritableStorageFactory) storageFactory,
                                           actionStorageFile,
                                           actionRegularFile));

            
        case COPY_STORAGE_FILE_TO_STORAGE_ACTION:
            // SECURITY PERMISSION - MP1, OP4
            return ReuseFactory.getBoolean(FileUtil.copyFile(
                                           (WritableStorageFactory) storageFactory,
                                           actionStorageFile,
                                           actionToStorageFile));

        case REGULAR_FILE_GET_CANONICALPATH_ACTION:
            // SECURITY PERMISSION - MP1
            return (String)(actionRegularFile.getCanonicalPath());
            
        case STORAGE_FILE_GET_CANONICALPATH_ACTION:
            // SECURITY PERMISSION - MP1
            return (String)(actionStorageFile.getCanonicalPath());
        case README_FILE_OUTPUTSTREAM_WRITER_ACTION:
        	return(new OutputStreamWriter(actionStorageFile.getOutputStream(),"UTF8"));
        }
        return null;
    } // end of run

    /** Tells if the attribute/property has been specified. */
    private static boolean isSet(Properties p, String attribute) {
        return p.getProperty(attribute) != null;
    }

    /** Tells if the attribute/property has been set to {@code true}. */
    private static boolean isTrue(Properties p, String attribute) {
        return Boolean.valueOf(p.getProperty(attribute)).booleanValue();
    }

    /** @see RawStoreFactory#createDataWarningFile */
    public void createDataWarningFile() throws StandardException {
        StorageFile fileReadMe = storageFactory.newStorageFile(
            "seg0",
            PersistentService.DB_README_FILE_NAME);
        OutputStreamWriter osw = null;

        if (!privExists(fileReadMe)) {
            try {
                osw = privGetOutputStreamWriter(fileReadMe);
                osw.write(MessageService.getTextMessage(
                    MessageId.README_AT_SEG_LEVEL));
            }
            catch (IOException ioe)
            {
            }
            finally
            {
                if (osw != null)
                {
                    try
                    {
                        osw.close();
                    }
                    catch (IOException ioe)
                    {
                        // Ignore exception on close
                    }
                }
            }
        }
    }
}
