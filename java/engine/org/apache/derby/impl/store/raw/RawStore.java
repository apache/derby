/*

   Derby - Class org.apache.derby.impl.store.raw.RawStore

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

package org.apache.derby.impl.store.raw;

import org.apache.derby.iapi.services.daemon.DaemonFactory;
import org.apache.derby.iapi.services.daemon.DaemonService;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.services.context.ContextService;
import org.apache.derby.iapi.services.crypto.CipherFactoryBuilder;
import org.apache.derby.iapi.services.crypto.CipherFactory;
import org.apache.derby.iapi.services.crypto.CipherProvider;
import org.apache.derby.iapi.services.locks.LockFactory;
import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.iapi.services.monitor.ModuleControl;
import org.apache.derby.iapi.services.monitor.ModuleSupportable;
import org.apache.derby.iapi.services.monitor.PersistentService;
import org.apache.derby.iapi.services.sanity.SanityManager;

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
import org.apache.derby.impl.services.monitor.UpdateServiceProperties;

import org.apache.derby.io.StorageFactory;
import org.apache.derby.io.WritableStorageFactory;
import org.apache.derby.io.StorageFile;
import org.apache.derby.iapi.store.access.DatabaseInstant;
import org.apache.derby.catalog.UUID;
import org.apache.derby.iapi.services.property.PropertyUtil;
import org.apache.derby.iapi.services.io.FileUtil;
import org.apache.derby.iapi.util.ReuseFactory;
import org.apache.derby.iapi.util.StringUtil;
import org.apache.derby.iapi.reference.Attribute;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.reference.MessageId;
import org.apache.derby.iapi.reference.Property;

import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.security.SecureRandom;

import java.util.Date;
import java.util.Properties;
import java.io.Serializable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.io.OutputStreamWriter;

import java.net.MalformedURLException;
import java.net.URL;

import java.security.PrivilegedExceptionAction;
import java.lang.SecurityException;


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

public final class RawStore implements RawStoreFactory, ModuleControl, ModuleSupportable, PrivilegedExceptionAction
{
	private static final String BACKUP_HISTORY = "BACKUP.HISTORY";
	protected TransactionFactory	xactFactory;
	protected DataFactory			dataFactory;
	protected LogFactory			logFactory;
    private StorageFactory storageFactory;

	private SecureRandom random;
	private boolean databaseEncrypted;
    private boolean encryptDatabase;
	private CipherProvider encryptionEngine;
	private CipherProvider decryptionEngine;
    private CipherProvider newEncryptionEngine;
	private CipherProvider newDecryptionEngine;
	private CipherFactory  currentCipherFactory;
	private int counter_encrypt;
	private int counter_decrypt;
	private int encryptionBlockSize = RawStoreFactory.DEFAULT_ENCRYPTION_BLOCKSIZE;

	String dataDirectory; 					// where files are stored	

	// this daemon takes care of all daemon work for this raw store
	protected DaemonService			rawStoreDaemon;

    private int actionCode;
    private static final int FILE_WRITER_ACTION = 1;
    private StorageFile actionStorageFile;
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
		dataDirectory = properties.getProperty(PersistentService.ROOT);
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
        boolean reEncrypt = false;
        CipherFactory newCipherFactory = null;

		if (properties != null)
		{
            // check if this is a restore from a backup copy. 
            restoreFromBackup = properties.getProperty(Attribute.CREATE_FROM);
            if(restoreFromBackup == null)
                restoreFromBackup = properties.getProperty(Attribute.RESTORE_FROM);
            if(restoreFromBackup == null)
                restoreFromBackup =
                    properties.getProperty(Attribute.ROLL_FORWARD_RECOVERY_FROM);


			/***********************************************
			 * encryption
			 **********************************************/
            
            // check if user has requested to encrypt the database or it is an
            // encrypted database.

            String dataEncryption = 
                properties.getProperty(Attribute.DATA_ENCRYPTION);
            databaseEncrypted = Boolean.valueOf(dataEncryption).booleanValue(); 


            if (!create && restoreFromBackup == null) {
                // check if database is already encrypted, by directly peeking at the
                // database service propertes instead of the properties passed 
                // to this method. By looking at properties to the boot method ,
                // one can not differentiate if user is requesting for database
                // encryption or the database is already encrypted because 
                // Attribute.DATA_ENCRYPTION is used  to store in the 
                // service properties to indicate that database
                // is encrypted and also users can specify it as URL attribute 
                // to encrypt and existing database. 
                               
                String name = Monitor.getMonitor().getServiceName(this);
                PersistentService ps = Monitor.getMonitor().getServiceType(this);
                String canonicalName = ps.getCanonicalServiceName(name);
                Properties serviceprops = ps.getServiceProperties(canonicalName, 
                                                                  (Properties)null);
                dataEncryption = serviceprops.getProperty(Attribute.DATA_ENCRYPTION);
                boolean encryptedDatabase = Boolean.valueOf(dataEncryption).booleanValue();

                if (!encryptedDatabase  && databaseEncrypted) {
                    // it it not an encrypted database, user is asking to 
                    // encrypt an un-encrypted database. 
                    encryptDatabase = true;
                    // set database as un-encrypted, we will set it as encrypted 
                    // after encrypting the existing data. 
                    databaseEncrypted = false;
                } else {
                    // check if the user has requested to renecrypt  an
                    // encrypted datbase with new encryption password/key.
                    if (encryptedDatabase) {
                        if (properties.getProperty(
                                       Attribute.NEW_BOOT_PASSWORD) != null) {
                            reEncrypt = true;
                        }
                        else if (properties.getProperty(
                                       Attribute.NEW_CRYPTO_EXTERNAL_KEY) != null){
                            reEncrypt = true;
                        };
                        encryptDatabase = reEncrypt;
                    }

                }
                
                // NOTE: if user specifies Attribute.DATA_ENCRYPTION on the
                // connection URL by mistake on an already encrypted database, 
                // it is ignored.

            }

            // setup encryption engines. 
			if (databaseEncrypted || encryptDatabase)
			{
                // check if database is configured for encryption, during
                // configuration  some of the properties database; so that
                // user does not have to specify them on the URL everytime.
                // Incase of re-encryption of an already of encrypted database
                // only some information needs to updated; it is not treated 
                // like the configuring the database for encryption first time. 
                boolean setupEncryption = create || (encryptDatabase &&  !reEncrypt);

                // start the cipher factory module, that is is used to create 
                // instances of the cipher factory with specific enctyption 
                // properties. 

                CipherFactoryBuilder cb =  (CipherFactoryBuilder)
                    Monitor.startSystemModule(org.apache.derby.iapi.reference.Module.CipherFactoryBuilder);

                // create instance of the cipher factory with the 
                // specified encryption properties. 
                currentCipherFactory = cb.createCipherFactory(setupEncryption, 
                                                              properties, 
                                                              false);

                // The database can be encrypted using an encryption key that is given at
                 // connection url. For security reasons, this key is not made persistent
                // in the database. But it is necessary to verify the encryption key 
                // whenever booting the database if it is similar to the key that was used
                // during creation time. This needs to happen before we access the data/logs to 
                // avoid the risk of corrupting the database because of a wrong encryption key.
                
                // Please note this verification process does not provide any added security
                // but is intended to allow to fail gracefully if a wrong encryption key 
                // is used during boot time
  

                currentCipherFactory.verifyKey(setupEncryption, storageFactory, properties);

                // Initializes the encryption and decryption engines
                encryptionEngine = currentCipherFactory.
                    createNewCipher(CipherFactory.ENCRYPT);
                
                // At creation time of an encrypted database, store the encryption block size
                // for the algorithm. Store this value as property given by  
                // RawStoreFactory.ENCRYPTION_BLOCKSIZE. This value
                // is made persistent by storing it in service.properties
                // To connect to an existing database, retrieve the value and use it for
                // appropriate padding.
                // The  default value of encryption block size is 8,
                // to allow for downgrade issues
                // Before support for AES (beetle6023), default encryption block size supported
                // was 8

                if(setupEncryption) 
                {
                    encryptionBlockSize = encryptionEngine.getEncryptionBlockSize();
                    // in case of database create, store the encryption block
                    // size. Incase of reconfiguring the existing datbase, this
                    // will be saved after encrypting the exisiting data. 
                    if (create)
                        properties.put(RawStoreFactory.ENCRYPTION_BLOCKSIZE,
                                       String.valueOf(encryptionBlockSize));
                }
                else
                {
                    if(properties.getProperty(RawStoreFactory.ENCRYPTION_BLOCKSIZE) != null)
                        encryptionBlockSize = Integer.parseInt(properties.getProperty
                                                               (RawStoreFactory.ENCRYPTION_BLOCKSIZE));
                    else
                        encryptionBlockSize = encryptionEngine.getEncryptionBlockSize();
                }   

                decryptionEngine = currentCipherFactory.
                    createNewCipher(CipherFactory.DECRYPT);

                random = currentCipherFactory.getSecureRandom();
                    
                if (encryptDatabase) {

                    if (reEncrypt) {
                        // create new cipher factory with the new encrytpion
                        // properties specified by the user. This cipher factory
                        // is used to create the new encryption/decryption
                        // engines to reencrypt the database with the new
                        // encryption keys. 
                        newCipherFactory = 
                            cb.createCipherFactory(setupEncryption, 
                                                   properties, 
                                                   true);
                        newDecryptionEngine = 
                            newCipherFactory.createNewCipher(CipherFactory.DECRYPT);
                        newEncryptionEngine = 
                            newCipherFactory.createNewCipher(CipherFactory.ENCRYPT);
                    } else {
                        // there is only one engine when configuring an 
                        // unencrypted database for encryption 
                        newDecryptionEngine = decryptionEngine;
                        newEncryptionEngine = encryptionEngine;

                    }
                }

                // save the encryption properties if encryption is enabled 
                // at database creation time. 
                if(create)
                    currentCipherFactory.saveProperties(properties) ;
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

        if (databaseEncrypted) {
            // let log factory know if the database is encrypted . 
            logFactory.setDatabaseEncrypted();
            // let data factory know if the database is encrypted. 
            dataFactory.setDatabaseEncrypted();
        }


		//save the service properties to a file if we are doing a restore from
		if(restoreFromBackup !=null)
		{
			//copy the jar files.etc from backup if they don't exist
			restoreRemainingFromBackup(restoreFromBackup);
			((UpdateServiceProperties)properties).saveServiceProperties();
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

		
		//save the service properties to a file if we are doing a restore from
		if(restoreFromBackup !=null)
		{
			//copy the jar files.etc from backup if they don't exist
			restoreRemainingFromBackup(restoreFromBackup);
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

		// no need to tell log factory which raw store factory it belongs to
		// since this is passed into the log factory for recovery
		// after the factories are loaded, recover the database
		logFactory.recover(this, dataFactory, xactFactory);

        // if user requested to encrpty an unecrypted database or encrypt with
        // new alogorithm then do that now.  
        if (encryptDatabase) {
            configureDatabaseForEncryption(properties, 
                                           reEncrypt, 
                                           newCipherFactory);
        }
	}

	public void	stop() {

		if (SanityManager.DEBUG)
		{
			if (databaseEncrypted)
				SanityManager.DEBUG_PRINT("encryption statistics",
						"Encryption called " + counter_encrypt + " times, " +
						"decryption called " + counter_decrypt + " times");
		}

		if (rawStoreDaemon != null)
			rawStoreDaemon.stop();

		if (logFactory == null)
			return;

		try {

			if (logFactory.checkpoint(this, dataFactory, xactFactory, false))
			{
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
	 * @exception StandardException Standard cloudscape exception policy.
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
    Object          compatibilitySpace,
    ContextManager  contextMgr,
    String          transName)
        throws StandardException
    {
		return(
            xactFactory.startNestedReadOnlyUserTransaction(
                this, compatibilitySpace, contextMgr, transName));
	}

	public Transaction startNestedUpdateUserTransaction(
    ContextManager  contextMgr,
    String          transName)
        throws StandardException
    {
		return(
            xactFactory.startNestedUpdateUserTransaction(
                this, contextMgr, transName));
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
		logFactory.checkpoint(this, dataFactory, xactFactory, false);
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
	 * duration of the backup. Stable copy is made of each page using using 
     * page level latches and in some cases with the help of monitors.  
     * Transaction log is also backed up, this is used to bring the database to 
     * the consistent state on restore.
	 * 
     * <P> MT- only one thread  is allowed to perform backup at any given time. 
     *  Synchronized on this. Parallel backups are not supported. 
	 */
	public synchronized void backup(Transaction t, 
                                    File backupDir) 
        throws StandardException
	{
        if (!privExists(backupDir))
		{
            if (!privMkdirs(backupDir))
            {
                throw StandardException.newException(
                    SQLState.RAWSTORE_CANNOT_CREATE_BACKUP_DIRECTORY,
                    (File) backupDir);
            }
		}
		else
		{
            if (!privIsDirectory(backupDir))
            {
				throw StandardException.newException(
                    SQLState.RAWSTORE_CANNOT_BACKUP_TO_NONDIRECTORY,
                    (File) backupDir);
            }

            // check if a user has given the backup as a database directory by
            // mistake, backup path can not be a derby database directory. 
            // If a directory contains PersistentService.PROPERTIES_NAME, it 
            // is assumed as derby database directory because derby databases
            // always has this file. 
 
            if (privExists(new File(backupDir, PersistentService.PROPERTIES_NAME))) { 
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
			// first figure out our name
			StorageFile dbase = storageFactory.newStorageFile( null); // The database directory
            String canonicalDbName = storageFactory.getCanonicalName();
            int lastSep = canonicalDbName.lastIndexOf( storageFactory.getSeparator());
			String dbname = canonicalDbName.substring( lastSep + 1);

			// append to end of history file
			historyFile = privFileWriter( storageFactory.newStorageFile( BACKUP_HISTORY), true);
            
			backupcopy = new File(backupDir, dbname);

			logHistory(historyFile,
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
            if (!privMkdirs(backupcopy))
            {
                throw StandardException.newException(
                    SQLState.RAWSTORE_CANNOT_CREATE_BACKUP_DIRECTORY,
                    (File) backupcopy);
            }

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
            if (privExists(jarDir)) {

                // find the list of schema directories under the jar dir and
                // then copy only the plain files under those directories. One could
                // just use the recursive copy of directory to copy all the files
                // under the jar dir, but the problem with that is if a user 
                // gives jar directory as the backup path by mistake, copy will 
                // fail while copying the backup dir onto itself in recursion

                String [] jarSchemaList = privList(jarDir);
                File backupJarDir = new File(backupcopy, 
                                             FileResource.JAR_DIRECTORY_NAME);
                // Create the backup jar directory
                if (!privMkdirs(backupJarDir))
                {
                    throw StandardException.newException(
                          SQLState.RAWSTORE_CANNOT_CREATE_BACKUP_DIRECTORY,
                          (File) backupJarDir);
                }

                for (int i = 0; i < jarSchemaList.length; i++)
                {
                    StorageFile jarSchemaDir = 
                        storageFactory.newStorageFile(jarDir, jarSchemaList[i]);
                    File backupJarSchemaDir = new File(backupJarDir, jarSchemaList[i]);
                    if (!privCopyDirectory(jarSchemaDir, backupJarSchemaDir, 
                                           (byte[])null, null, false)) {
                        throw StandardException.
                            newException(SQLState.RAWSTORE_ERROR_COPYING_FILE,
                                         jarSchemaDir, backupJarSchemaDir);  
                    }
                }
            }


            // save service properties into the backup, Read in property 
            // from service.properties file, remove logDevice from it, 
            // then write it to the backup.

            StorageFile logdir = logFactory.getLogDirectory();
            
            try {
                
                String name = Monitor.getMonitor().getServiceName(this);
                PersistentService ps = Monitor.getMonitor().getServiceType(this);
                String fullName = ps.getCanonicalServiceName(name);
                Properties prop = ps.getServiceProperties(fullName, (Properties)null);
                StorageFile defaultLogDir = 
                    storageFactory.newStorageFile( LogFactory.LOG_DIRECTORY_NAME);

                if (!logdir.equals(defaultLogDir))  
                {
                    prop.remove(Attribute.LOG_DEVICE);
                    if (SanityManager.DEBUG)
                        SanityManager.ASSERT(prop.getProperty(Attribute.LOG_DEVICE) == null,
                                             "cannot get rid of logDevice property");
                    logHistory(historyFile,
                               MessageService.getTextMessage(
                               MessageId.STORE_EDITED_SERVICEPROPS));
                }
            
                // save the service properties into the backup.
                ps.saveServiceProperties( backupcopy.getPath(), prop, false);

            }catch(StandardException se) {
                logHistory(historyFile,
                           MessageService.getTextMessage(
                           MessageId.STORE_ERROR_EDIT_SERVICEPROPS)
                           + se);

                return; // skip the rest and let finally block clean up
            }

            // Incase of encrypted database and the key is an external 
            // encryption key, there is an extra file with name  
            // Attribute.CRYPTO_EXTERNAL_KEY_VERIFY_FILE , this file should be
            // copied in to the backup.
            StorageFile verifyKeyFile = 
                storageFactory.newStorageFile(
                                 Attribute.CRYPTO_EXTERNAL_KEY_VERIFY_FILE);
            if (privExists(verifyKeyFile)) {
                File backupVerifyKeyFile = 
                    new File(backupcopy, Attribute.CRYPTO_EXTERNAL_KEY_VERIFY_FILE);
                if(!privCopyFile(verifyKeyFile, backupVerifyKeyFile))
                   throw StandardException.
                       newException(SQLState.RAWSTORE_ERROR_COPYING_FILE,
                                    verifyKeyFile, backupVerifyKeyFile);  
            }
                
			File logBackup = new File(backupcopy, LogFactory.LOG_DIRECTORY_NAME);

			// this is wierd, delete it
            if (privExists(logBackup))
			{
                privRemoveDirectory(logBackup);
			}

			// Create the log directory
            if (!privMkdirs(logBackup))
            {
                throw StandardException.newException(
                    SQLState.RAWSTORE_CANNOT_CREATE_BACKUP_DIRECTORY,
                    (File) logBackup);
            }

			// do a checkpoint to get the persistent store up to date.
			logFactory.checkpoint(this, dataFactory, xactFactory, true);
			
			// start the transaction log  backup. 
            logFactory.startLogBackup(logBackup);

			File segBackup = new File(backupcopy, "seg0");
			
			// Create the data segment directory
            if (!privMkdirs(segBackup))
            {
                throw StandardException.newException(
                    SQLState.RAWSTORE_CANNOT_CREATE_BACKUP_DIRECTORY,
                    (File) segBackup);
            }


			// backup all the information in the data segment.
			dataFactory.backupDataFiles(t, segBackup);

            logHistory(historyFile,
                   MessageService.getTextMessage(
                   MessageId.STORE_DATA_SEG_BACKUP_COMPLETED,
                   getFilePath(segBackup)));


            // copy the log that got generated after the backup started to
			// backup location and tell the logfactory that backup has come to end.
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

	
	//copies the files from the backup that does not need
	//any special handling like jars.
	private void restoreRemainingFromBackup(String backupPath) throws StandardException
	{
		/** 
		 *copy the files from the backup except the ones that we already
		 *copied in the boot methods(like log directory and data segments)
		 *AND Service.properties file which we create last to
		 *indicate the end of copy from backup.
		 */

		File backuploc = new File(backupPath);
		String[] fromList = privList(backuploc);
		for(int i =0 ; i < fromList.length ; i++)
		{
			StorageFile toFile = storageFactory.newStorageFile( fromList[i]);
			if(privExists(toFile) || 
			   fromList[i].equals(PersistentService.PROPERTIES_NAME)){
				continue;
			}

			File fromFile = new File(backuploc, fromList[i]);
			if(privIsDirectory(fromFile))
			{
				if (!privCopyDirectory(fromFile, toFile)){
					throw StandardException.newException(
                         SQLState.UNABLE_TO_COPY_FILE_FROM_BACKUP, 
                         fromFile, toFile);
				}
			}else{
				if (!privCopyFile(fromFile, toFile)){
					throw StandardException.newException(
                         SQLState.UNABLE_TO_COPY_FILE_FROM_BACKUP,
                         fromFile, toFile);
				}
			}
		}
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
	 * Get JBMS properties relavent to raw store
	 * @exception StandardException Standard Cloudscape Error Policy
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
		@exception StandardException Standard Cloudscape Error Policy
	 */
	public void freezePersistentStore() throws StandardException
	{
		// do a checkpoint to get the persistent store up to date.
		logFactory.checkpoint(this, dataFactory, xactFactory,true);
		logFactory.freezePersistentStore();

	}

	/**
		Freeze persistent store.  Reads can still happen, only cannot write.
		@exception StandardException Standard Cloudscape Error Policy
	 */
	public void unfreezePersistentStore() throws StandardException
	{
		logFactory.unfreezePersistentStore();
	}


	/*
	** data encryption/decryption support
	*/


    

	/**
		Encrypt cleartext into ciphertext.

		@see CipherProvider#encrypt

		@exception StandardException Standard Cloudscape Error Policy
	 */
	public int encrypt(byte[] cleartext, int offset, int length,
					   byte[] ciphertext, int outputOffset, 
                       boolean newEngine)
		 throws StandardException
	{
		if ((databaseEncrypted == false && encryptDatabase == false) || 
            (encryptionEngine == null && newEncryptionEngine == null))
        {
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

		@exception StandardException Standard Cloudscape Error Policy
	 */
	public int decrypt(byte[] ciphertext, int offset, int length,
					   byte[] cleartext, int outputOffset) 
		 throws StandardException
	{
		if (databaseEncrypted == false || decryptionEngine == null)
        {
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
		return databaseEncrypted ? random.nextInt() : 0;
	}

	public Serializable changeBootPassword(Properties properties, Serializable changePassword)
		 throws StandardException
	{
		if (isReadOnly())
			throw StandardException.newException(SQLState.DATABASE_READ_ONLY);

		if (!databaseEncrypted)
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

     * First step is to encrypt the existing data with new encryption 
     * attributes  and then update the encryption properties for 
     * the database. Configuring  an un-encrypted database for 
     * encryption problem is a minor variation of  re-encrypting an 
     * encrypted database with new encryption key. The database 
     * reconfiguration with new encryption attributes is done under one
     * transaction, if there is a crash/error before it is committed, 
     * then it  is rolled back and the database will be brought back to the
     * state it was before the encryption.  
     * @param properties  properties related to this database.
     * @exception StandardException Standard Cloudscape Error Policy
     */
    public void configureDatabaseForEncryption(Properties properties,
                                               boolean reEncrypt, 
                                               CipherFactory newCipherFactory) 
        throws StandardException 
    {

        // check point the datase, so that encryption does not have
        // to encrypt the existing transactions logs. 
 
        logFactory.checkpoint(this, dataFactory, xactFactory, true);

        // start a transaction that is to be used for encryting the database
        RawTransaction transaction =
            xactFactory.startTransaction(
                   this,
                    ContextService.getFactory().getCurrentContextManager(),
                    AccessFactoryGlobals.USER_TRANS_NAME);
        boolean error = true;
        try {
            dataFactory.encryptAllContainers(transaction);
            error = false;
        }finally {
            
            // encryption is finished. close the transaction.
            if (error) { 
                transaction.abort();
            }
            else {
                transaction.commit();

                // TODO : handle the case where if engine crashes
                // after the commit but before the new database
                // encryption properties are made persistent. 
                
                // let log factory and data factory know that 
                // database is encrypted.
                logFactory.setDatabaseEncrypted();
                logFactory.setupLogEncryption();
                dataFactory.setDatabaseEncrypted();
                
                // mark in the raw store that the database is 
                // encrypted. 
                databaseEncrypted = true;
                encryptDatabase = false;
                //switch the encryption/decryption engine to the new ones.
                if (reEncrypt) {
                    decryptionEngine = newDecryptionEngine;  
                    encryptionEngine = newEncryptionEngine;
                    currentCipherFactory = newCipherFactory;
                }

                //force a checkpoint with new encryption algorithm
                logFactory.checkpoint(this, dataFactory, xactFactory, true);
                // store the encryption block size;
                properties.put(RawStoreFactory.ENCRYPTION_BLOCKSIZE,
                               String.valueOf(encryptionBlockSize));
                // save the encryption properties.
                currentCipherFactory.saveProperties(properties) ;

                // incase of rencrytion of database, save information needed 
                // to verify the new key on a next boot. 
                if (reEncrypt) {
                    currentCipherFactory.verifyKey(reEncrypt, 
                                               storageFactory, 
                                               properties);
                }

            }                
            newDecryptionEngine = null;   
            newEncryptionEngine = null;
            transaction.close(); 
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


    private synchronized boolean privMkdirs( File file)
    {
        actionCode = REGULAR_FILE_MKDIRS_ACTION;
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
        catch( PrivilegedActionException pae) { return false;} // does not throw an exception
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
    {
        actionCode = COPY_STORAGE_FILE_TO_REGULAR_ACTION;
        actionStorageFile = from;
        actionRegularFile = to;

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


    private synchronized String[] privList(final File file)
    {
        actionCode = REGULAR_FILE_LIST_DIRECTORY_ACTION;
        actionRegularFile = file;

        try
        {
            return (String[]) AccessController.doPrivileged( this);
        }
        catch( PrivilegedActionException pae) { return null;} // does not throw an exception
        finally
        {
            actionRegularFile = null;
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
    public final Object run() throws IOException
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

        case REGULAR_FILE_MKDIRS_ACTION:
            // SECURITY PERMISSION - OP4
            return ReuseFactory.getBoolean(actionRegularFile.mkdirs());

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
            
        case REGULAR_FILE_GET_CANONICALPATH_ACTION:
            // SECURITY PERMISSION - MP1
            return (String)(actionRegularFile.getCanonicalPath());
            
        case STORAGE_FILE_GET_CANONICALPATH_ACTION:
            // SECURITY PERMISSION - MP1
            return (String)(actionStorageFile.getCanonicalPath());
        }
        return null;
    } // end of run
}
