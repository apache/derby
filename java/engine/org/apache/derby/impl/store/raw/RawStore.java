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

import org.apache.derby.iapi.store.raw.ScanHandle;
import org.apache.derby.iapi.store.raw.RawStoreFactory;
import org.apache.derby.iapi.store.raw.Transaction;
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

/**
	A Raw store that implements the RawStoreFactory module by delegating all the
	work to the lower modules TransactionFactory, LogFactory and DataFactory.
	<PRE>
	String TransactionFactoryId=<moduleIdentifier>
	</PRE>
*/

public class RawStore implements RawStoreFactory, ModuleControl, ModuleSupportable, PrivilegedExceptionAction
{
	private static final String BACKUP_HISTORY = "BACKUP.HISTORY";
	private static final String[] BACKUP_FILTER =
	{ DataFactory.TEMP_SEGMENT_NAME, DataFactory.DB_LOCKFILE_NAME, DataFactory.DB_EX_LOCKFILE_NAME, LogFactory.LOG_DIRECTORY_NAME };

	protected TransactionFactory	xactFactory;
	protected DataFactory			dataFactory;
	protected LogFactory			logFactory;
    private StorageFactory storageFactory;

	private SecureRandom random;
	private boolean databaseEncrypted;
	private CipherProvider encryptionEngine;
	private CipherProvider decryptionEngine;
	private CipherFactory cipherFactory;
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
    private static final int COPY_REGULAR_DIRECTORY_TO_STORAGE_ACTION = 10;
    private static final int COPY_REGULAR_FILE_TO_STORAGE_ACTION = 11;
    private static final int REGULAR_FILE_LIST_DIRECTORY_ACTION = 12;
    
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

		if (properties != null)
		{

			/***********************************************
			 * encryption
			 **********************************************/

			String dataEncryption = properties.getProperty(Attribute.DATA_ENCRYPTION);
			databaseEncrypted = Boolean.valueOf(dataEncryption).booleanValue();


			if (SanityManager.DEBUG)
			{
				if (!databaseEncrypted)
				{
					// check for system property if running under sanity - this
					// gives more test coverage for those that that hard code
					// connection URL in the test or somehow go thru the test
					// harness in a strange way.
					String testEncryption =
						PropertyUtil.getSystemProperty("testDataEncryption");

					if (testEncryption != null)
					{
						properties.put(Attribute.DATA_ENCRYPTION, "true");
						properties.put(Attribute.BOOT_PASSWORD, testEncryption);
						databaseEncrypted = true;
                    }
				}
			}

			if (databaseEncrypted)
			{
					cipherFactory =
                        (CipherFactory)Monitor.bootServiceModule(create, this,
						org.apache.derby.iapi.reference.Module.CipherFactory, properties);

					// The database can be encrypted using an encryption key that is given at
					// connection url. For security reasons, this key is not made persistent
					// in the database. But it is necessary to verify the encryption key 
					// whenever booting the database if it is similar to the key that was used
					// during creation time. This needs to happen before we access the data/logs to 
					// avoid the risk of corrupting the database because of a wrong encryption key.

					// Please note this verification process does not provide any added security
				        // but is intended to allow to fail gracefully if a wrong encryption key 
					// is used during boot time
  
					cipherFactory.verifyKey(create,storageFactory,properties);

					// Initializes the encryption and decryption engines
					encryptionEngine = cipherFactory.
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

					if(create)
					{
						encryptionBlockSize = encryptionEngine.getEncryptionBlockSize();
						properties.put(RawStoreFactory.ENCRYPTION_BLOCKSIZE,
								String.valueOf(encryptionBlockSize));
					}
					else
					{
						if(properties.getProperty(RawStoreFactory.ENCRYPTION_BLOCKSIZE) != null)
						    encryptionBlockSize = Integer.parseInt(properties.getProperty
										(RawStoreFactory.ENCRYPTION_BLOCKSIZE));
					}

					decryptionEngine = cipherFactory.
						createNewCipher(CipherFactory.DECRYPT);

					random = cipherFactory.getSecureRandom();

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

		String restoreFromBackup =null;
		restoreFromBackup = properties.getProperty(Attribute.CREATE_FROM);
		if(restoreFromBackup == null)
			restoreFromBackup = properties.getProperty(Attribute.RESTORE_FROM);
		if(restoreFromBackup == null)
			restoreFromBackup =
				properties.getProperty(Attribute.ROLL_FORWARD_RECOVERY_FROM);


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
			if (create || !logDevice.equals(logFactory.getCanonicalLogPath()) ||
				restoreFromBackup!=null)
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

	public void backup(String backupDir) throws StandardException
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

		backup(new File(backupDir));
	}


	public synchronized void backup(File backupDir) throws StandardException
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
		}

		boolean error = true;
		boolean renamed = false;
		boolean renameFailed = false;
		File oldbackup = null;
		File backupcopy = null;
		OutputStreamWriter historyFile = null;
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

			logHistory(historyFile,
                        MessageService.getTextMessage(
                            MessageId.STORE_BACKUP_STARTED, canonicalDbName));

			// if a backup copy of this database already exists,
			backupcopy = new File(backupDir, dbname);

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
					throw StandardException.newException(
                        SQLState.RAWSTORE_ERROR_RENAMING_FILE,
                        backupcopy, oldbackup, (Throwable)null);
                }
				else
				{
					logHistory(
                        historyFile,
                        MessageService.getTextMessage(
                            MessageId.STORE_MOVED_BACKUP,
                            backupcopy.getCanonicalPath(),
                            oldbackup.getCanonicalPath()));
					renamed = true;
				}
			}

			// checkpoint the database and freeze it
			freeze();

			// copy everything from the dataDirectory to the
			// backup directory (except temp files)

            if (!privCopyDirectory(dbase, backupcopy, (byte[])null, BACKUP_FILTER))
            {
				throw StandardException.newException(
                    SQLState.RAWSTORE_ERROR_COPYING_FILE,
					dbase, backupcopy, (Throwable)null);
            }

			logHistory(historyFile,
                MessageService.getTextMessage(
                    MessageId.STORE_COPIED_DB_DIR,
                    canonicalDbName,
                    backupcopy.getCanonicalPath()));

			StorageFile logdir = logFactory.getLogDirectory();

			// munge service.properties file if necessary
			StorageFile defaultLogDir = storageFactory.newStorageFile( LogFactory.LOG_DIRECTORY_NAME);
			if (!logdir.equals(defaultLogDir))
			{
				// Read in property from service.properties file, remove
				// logDevice from it, then write it out again.
				try
				{
					String name = Monitor.getMonitor().getServiceName(this);
					PersistentService ps = Monitor.getMonitor().getServiceType(this);
					String fullName = ps.getCanonicalServiceName(name);
					Properties prop = ps.getServiceProperties(fullName, (Properties)null);

					prop.remove(Attribute.LOG_DEVICE);

					if (SanityManager.DEBUG)
						SanityManager.ASSERT(prop.getProperty(Attribute.LOG_DEVICE) == null,
											 "cannot get rid of logDevice property");

					ps.saveServiceProperties( backupcopy.getCanonicalPath(), prop, true);

					logHistory(historyFile,
                        MessageService.getTextMessage(
                            MessageId.STORE_EDITED_SERVICEPROPS));

				}
				catch(StandardException se)
				{
					logHistory(historyFile,
                        MessageService.getTextMessage(
                            MessageId.STORE_ERROR_EDIT_SERVICEPROPS)
                            + se);

					return; // skip the rest and let finally block clean up
				}
			}

			File logBackup = new File(backupcopy, LogFactory.LOG_DIRECTORY_NAME);

			// this is wierd, delete it
            if (privExists(logBackup))
			{
                privRemoveDirectory(logBackup);
			}

			//Create the log directory
            if (!privMkdirs(logBackup))
            {
                throw StandardException.newException(
                    SQLState.RAWSTORE_CANNOT_CREATE_BACKUP_DIRECTORY,
                    (File) logBackup);
            }

		
			// copy the log to the backup location
			if(!logFactory.copyActiveLogFiles(logBackup))
			{
				throw StandardException.newException(
													 SQLState.RAWSTORE_ERROR_COPYING_FILE,
													 logdir, logBackup, (Throwable)null);
			}	

			logHistory(historyFile,
                MessageService.getTextMessage(
                    MessageId.STORE_COPIED_LOG,
                    logdir.getCanonicalPath(),
                    logBackup.getCanonicalPath()));

			error = false;
		}
		catch (IOException ioe)
		{
			throw StandardException.newException(
                    SQLState.RAWSTORE_UNEXPECTED_EXCEPTION, ioe);
		}
		finally
		{
			// unfreeze db ASAP
			unfreeze();

			try
			{
				if (error)
				{
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
                                oldbackup.getCanonicalPath()));
 					}
					logHistory(historyFile,
                        MessageService.getTextMessage(
                            MessageId.STORE_BACKUP_COMPLETED,
                            backupInstant));

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


	public void backupAndEnableLogArchiveMode(String backupDir,boolean
											  deleteOnlineArchivedLogFiles) 
		throws StandardException
	{
		enableLogArchiveMode();
		backup(backupDir);
		//After successful backup delete the archived log files
		//that are not necessary to do a roll-forward recovery
		//from this backup if requested.
		if(deleteOnlineArchivedLogFiles)
		{
			logFactory.deleteOnlineArchivedLogFiles();
		}
	}


	public void backupAndEnableLogArchiveMode(File backupDir,boolean
											  deleteOnlineArchivedLogFiles) 
		throws StandardException
	{
		enableLogArchiveMode();
		backup(backupDir);
		//After successful backup delete the archived log files
		//that are not necessary to do a roll-forward recovery
		//from this backup if requested.
		if(deleteOnlineArchivedLogFiles)
		{
			logFactory.deleteOnlineArchivedLogFiles();
		}
	}


	private void enableLogArchiveMode() throws StandardException
	{
		logFactory.enableLogArchiveMode();
	}

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
					   byte[] ciphertext, int outputOffset)
		 throws StandardException
	{
		if (databaseEncrypted == false || encryptionEngine == null)
        {
            throw StandardException.newException(
                        SQLState.STORE_FEATURE_NOT_IMPLEMENTED);
        }

		counter_encrypt++;

		return encryptionEngine.encrypt(cleartext, offset, length,
										ciphertext, outputOffset);
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

		return cipherFactory.changeBootPassword((String)changePassword, properties, encryptionEngine);

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


	protected boolean privCopyDirectory(StorageFile from, File to)
	{
		return privCopyDirectory(from, to, (byte[])null, (String[])null);
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

	protected synchronized boolean privExists( File file)
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

	protected synchronized boolean privExists(final StorageFile file)
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


    protected synchronized boolean privDelete( File file)
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


    protected synchronized boolean privMkdirs( File file)
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


    protected synchronized boolean privIsDirectory( File file)
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

    protected synchronized boolean privRemoveDirectory( File file)
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

    protected synchronized boolean privRenameTo( File file1, File file2)
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

    protected synchronized boolean privCopyDirectory( StorageFile from, File to, byte[] buffer, String[] filter)
    {
        actionCode = COPY_STORAGE_DIRECTORY_TO_REGULAR_ACTION;
        actionStorageFile = from;
        actionRegularFile = to;
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


    protected synchronized boolean privCopyDirectory( File from, StorageFile to, byte[] buffer, String[] filter)
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

	
    protected synchronized boolean privCopyFile( File from, StorageFile to)
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

    protected synchronized String[] privList(final File file)
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

    // PrivilegedExceptionAction method
    public Object run() throws IOException
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
                                                                  actionFilter));

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
        }
        return null;
    } // end of run
}
