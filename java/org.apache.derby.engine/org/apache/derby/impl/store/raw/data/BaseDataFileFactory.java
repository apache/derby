/*

   Derby - Class org.apache.derby.impl.store.raw.data.BaseDataFileFactory

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


import org.apache.derby.shared.common.reference.MessageId;
import org.apache.derby.iapi.security.SecurityUtil;
import org.apache.derby.shared.common.info.ProductVersionHolder;

import org.apache.derby.database.Database;
import org.apache.derby.iapi.services.cache.CacheFactory;
import org.apache.derby.iapi.services.cache.CacheManager;
import org.apache.derby.iapi.services.cache.Cacheable;
import org.apache.derby.iapi.services.cache.CacheableFactory;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.services.daemon.DaemonService;
import org.apache.derby.iapi.services.daemon.Serviceable;
import org.apache.derby.iapi.services.monitor.ModuleControl;
import org.apache.derby.iapi.services.monitor.ModuleFactory;
import org.apache.derby.iapi.services.monitor.ModuleSupportable;
import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.iapi.services.monitor.PersistentService;
import org.apache.derby.iapi.services.diag.Performance;
import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derby.shared.common.stream.HeaderPrintWriter;

import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.shared.common.i18n.MessageService;
import org.apache.derby.iapi.store.access.AccessFactoryGlobals;
import org.apache.derby.iapi.store.access.FileResource;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.iapi.store.raw.data.DataFactory;
import org.apache.derby.iapi.store.raw.data.RawContainerHandle;
import org.apache.derby.iapi.store.raw.log.LogFactory;
import org.apache.derby.iapi.store.raw.log.LogInstant;
import org.apache.derby.iapi.store.raw.ContainerHandle;
import org.apache.derby.iapi.store.raw.ContainerKey;
import org.apache.derby.iapi.store.raw.LockingPolicy;
import org.apache.derby.iapi.store.raw.Page;
import org.apache.derby.iapi.store.raw.PageKey;
import org.apache.derby.iapi.store.raw.RawStoreFactory;
import org.apache.derby.iapi.store.raw.StreamContainerHandle;
import org.apache.derby.iapi.store.raw.Transaction;
import org.apache.derby.iapi.store.raw.UndoHandler;
import org.apache.derby.iapi.store.raw.xact.RawTransaction;

import org.apache.derby.iapi.store.access.RowSource;

import org.apache.derby.io.StorageFactory;
import org.apache.derby.io.WritableStorageFactory;
import org.apache.derby.io.StorageFile;
import org.apache.derby.io.StorageRandomAccessFile;
import org.apache.derby.iapi.services.uuid.UUIDFactory;
import org.apache.derby.catalog.UUID;
import org.apache.derby.shared.common.reference.Attribute;
import org.apache.derby.shared.common.reference.Property;
import org.apache.derby.shared.common.reference.SQLState;
import org.apache.derby.iapi.util.ByteArray;

import org.apache.derby.iapi.util.InterruptStatus;
import org.apache.derby.iapi.services.io.FileUtil;
import org.apache.derby.iapi.services.property.PropertyUtil;

import java.util.Date;
import java.util.Properties;
import java.util.Hashtable;
import java.util.Enumeration;

import java.io.File;
import java.io.IOException;

import java.net.URL;

import java.security.AccessController;
import java.security.CodeSource;
import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;
import java.security.PrivilegedActionException;

/**

Provides the abstract class with most of the implementation of DataFactory and
ModuleControl shared by all the different filesystem implementations.
<p>
RESOLVE (mikem - 2/19/98) -
Currently only getContainerClass() is abstract, there are probably more 
routines which should be abstract.  Also the other implementations should 
probably inherit from the abstract class, rather than from the DataFileFactory
class.  Also there probably should be a generic directory and the rest of the
filesystem implementations parallel to it.
I wanted to limit the changes going into the branch and then fix 
inheritance stuff in main.
<p>
The code in this class was moved over from DataFileFactory.java and then
that file was made to inherit from this one.

**/

public class BaseDataFileFactory
//IC see: https://issues.apache.org/jira/browse/DERBY-6213
    implements DataFactory, CacheableFactory, ModuleControl, ModuleSupportable, PrivilegedExceptionAction<Object>
{

    StorageFactory storageFactory;

    /* writableStorageFactory == (WritableStorageFactory) storageFactory if 
     * storageFactory also implements WritableStorageFactory, null if the 
     * storageFactory is read-only.
     */
    WritableStorageFactory writableStorageFactory;

	private     long		    nextContainerId = System.currentTimeMillis();
	private     boolean         databaseEncrypted;

	private     CacheManager	pageCache;
	private     CacheManager	containerCache;

	private     LogFactory	    logFactory;

	private     ProductVersionHolder jbmsVersion;
	
	private     String          jvmVersion;
	
	private     String          osInfo;
	
	private     String          jarCPath;

	private     RawStoreFactory	rawStoreFactory; // associated raw store factory

	private     String			dataDirectory;	 // root directory of files.

    private     boolean         throwDBlckException; // if true throw db.lck
                                                 // exception, even on systems
                                                 // where lock file is not
                                                 // guaranteed.

	private     UUID            identifier;      // unique id for locking 

	private final Object freezeSemaphore = new Object();


    // is the data store frozen - protected by freezeSemaphore
	private     boolean         isFrozen;	     


    // how many writers are currently active in the data store - 
    // protected by freezeSemaphore
	private     int             writersInProgress; 


	private boolean removeStubsOK;
	private boolean isCorrupt;

    // the database is being created, no logging
	private boolean inCreateNoLog;	

	// lock against other JBMS opening the same database
	private StorageRandomAccessFile fileLockOnDB;
	private StorageFile exFileLock; //file handle to get exclusive lock
	private HeaderPrintWriter istream;
	private static final String LINE = 
        "----------------------------------------------------------------";

    // disable syncing of data during page allocation.  DERBY-888 changes
    // the system to not require data syncing at allocation.  
    boolean dataNotSyncedAtAllocation = true;

    // disable syncing of data during checkpoint.
    boolean dataNotSyncedAtCheckpoint = false;

	// these fields can be accessed directly by subclasses if it needs a
	// different set of actions
	private PageActions       loggablePageActions; 
	private AllocationActions loggableAllocActions;

	private boolean		    readOnly; 		// is this a read only data store
    private boolean supportsRandomAccess;
	private FileResource	    fileHandler;	// my file handler, set by a 
                                                // sub-class in its boot method.


	//hash table to keep track of information about dropped containers stubs
	private Hashtable<LogInstant,Object[]> droppedTableStubInfo;

	private Hashtable<String,StorageFile> postRecoveryRemovedFiles;

    // PrivilegedAction actions
    private int actionCode;
    private static final int REMOVE_TEMP_DIRECTORY_ACTION           = 2;
    private static final int GET_CONTAINER_PATH_ACTION              = 3;
    private static final int GET_ALTERNATE_CONTAINER_PATH_ACTION    = 4;
    private static final int FIND_MAX_CONTAINER_ID_ACTION           = 5;
    private static final int DELETE_IF_EXISTS_ACTION                = 6;
    private static final int GET_PATH_ACTION                        = 7;
    private static final int POST_RECOVERY_REMOVE_ACTION            = 8;
    private static final int REMOVE_STUBS_ACTION                    = 9;
    private static final int BOOT_ACTION                            = 10;
    private static final int GET_LOCK_ON_DB_ACTION                  = 11;
    private static final int RELEASE_LOCK_ON_DB_ACTION              = 12;
    private static final int RESTORE_DATA_DIRECTORY_ACTION          = 13;
    private static final int GET_CONTAINER_NAMES_ACTION             = 14;

    private ContainerKey    containerId;
    private boolean         stub;
    private StorageFile     actionFile;
    private UUID            myUUID;
    private UUIDFactory     uuidFactory;
    private String          databaseDirectory;

    private File            backupRoot;
    private String[]        bfilelist;


    // Class to use to notify upon undo of deletes
    private UndoHandler undo_handler = null;

	/*
	** Constructor
	*/

	public BaseDataFileFactory() 
    {
        // Verify that we have permission to execute this method.
//IC see: https://issues.apache.org/jira/browse/DERBY-6636
        SecurityUtil.checkDerbyInternalsPrivilege();
	}

	/*
	** Methods of ModuleControl
	*/

	public boolean canSupport(Properties startParams) 
    {

		String serviceType = startParams.getProperty(PersistentService.TYPE);
		if (serviceType == null)
			return false;

//IC see: https://issues.apache.org/jira/browse/DERBY-927
		if (!handleServiceType(serviceType))
			return false;

		if (startParams.getProperty(PersistentService.ROOT) == null)
			return false;

		return true;
	}

	public void	boot(boolean create, Properties startParams) 
        throws StandardException 
    {

//IC see: https://issues.apache.org/jira/browse/DERBY-6648
		jbmsVersion = getMonitor().getEngineVersion();
		
//IC see: https://issues.apache.org/jira/browse/DERBY-4715
		jvmVersion = buildJvmVersion();
		
//IC see: https://issues.apache.org/jira/browse/DERBY-5240
		osInfo = buildOSinfo();
		
		jarCPath = jarClassPath(getClass());

		dataDirectory = startParams.getProperty(PersistentService.ROOT);

		UUIDFactory uf = getMonitor().getUUIDFactory();
//IC see: https://issues.apache.org/jira/browse/DERBY-6648

		identifier = uf.createUUID();

        PersistentService ps = getMonitor().getServiceType(this);

        try
        {
            storageFactory =
            ps.getStorageFactoryInstance(
                true,
                dataDirectory,
                startParams.getProperty(
                    Property.STORAGE_TEMP_DIRECTORY,
                    PropertyUtil.getSystemProperty(
                        Property.STORAGE_TEMP_DIRECTORY)),
                identifier.toANSIidentifier());
        }
        catch(IOException ioe)
        {
            if (create)
            {
                throw StandardException.newException(
                    SQLState.SERVICE_DIRECTORY_CREATE_ERROR, 
                    ioe, dataDirectory);
            }
            else
            {
                throw StandardException.newException(
                    SQLState.DATABASE_NOT_FOUND, ioe, dataDirectory);
            }
        }

        // you can't encrypt a database if the Lucene plugin is loaded
//IC see: https://issues.apache.org/jira/browse/DERBY-590
        if ( luceneLoaded() )
        {
            String  encryptionProp = startParams.getProperty( Attribute.DATA_ENCRYPTION );
            if ( (encryptionProp != null) && "TRUE".equals( encryptionProp.toUpperCase() ) )
            {
                throw StandardException.newException( SQLState.LUCENE_ENCRYPTED_DB );
            }
        }

        if (storageFactory instanceof WritableStorageFactory)
            writableStorageFactory = (WritableStorageFactory) storageFactory;

        actionCode = BOOT_ACTION;

        try
        {
            AccessController.doPrivileged( this);
        }
        catch (PrivilegedActionException pae)
        { 
            // BOOT_ACTION does not throw any exceptions.
        }
        
        String value =
            startParams.getProperty(Property.FORCE_DATABASE_LOCK,
                PropertyUtil.getSystemProperty(Property.FORCE_DATABASE_LOCK));
        throwDBlckException =
            Boolean.valueOf(
                (value != null ? value.trim() : value)).booleanValue();

		if (!isReadOnly())		// read only db, not interested in filelock
			getJBMSLockOnDB(identifier, uf, dataDirectory);


		//If the database is being restored/created from backup
		//the restore the data directory(seg*) from backup
		String restoreFrom =null;
		restoreFrom = startParams.getProperty(Attribute.CREATE_FROM);
		if(restoreFrom == null)
			restoreFrom = startParams.getProperty(Attribute.RESTORE_FROM);
		if(restoreFrom == null)
			restoreFrom = startParams.getProperty(Attribute.ROLL_FORWARD_RECOVERY_FROM);

		if (restoreFrom !=null)
		{
			try
            {
                // restoreFrom and createFrom operations also need to know if database 
                // is encrypted
//IC see: https://issues.apache.org/jira/browse/DERBY-1156
                String dataEncryption = 
                    startParams.getProperty(Attribute.DATA_ENCRYPTION);
                databaseEncrypted = Boolean.valueOf(dataEncryption).booleanValue();
				restoreDataDirectory(restoreFrom);
			}
            catch(StandardException se)
			{
				releaseJBMSLockOnDB();
				throw se;
			}
		}

		logMsg(LINE);
        String messageID = (isReadOnly())  ?
            MessageId.STORE_BOOT_MSG_READ_ONLY
            : MessageId.STORE_BOOT_MSG;
        boolean logBootTrace = Boolean.valueOf(startParams.getProperty(Property.LOG_BOOT_TRACE,
               PropertyUtil.getSystemProperty(Property.LOG_BOOT_TRACE))).booleanValue();
        logMsg(new Date() +
			   MessageService.getTextMessage(messageID,
                                             jbmsVersion,
                                             identifier,
                                             dataDirectory,
                                             // cast to Object so we get object hash code
                                             (Object) this.getClass().getClassLoader(),
                                             jarCPath
                                             ));
		//Log the JVM version info
		logMsg(jvmVersion);

		//Log the OS info
		logMsg(osInfo);
//IC see: https://issues.apache.org/jira/browse/DERBY-5240

		//Log derby.system.home It will have null value if user didn't set it
//IC see: https://issues.apache.org/jira/browse/DERBY-4853
		logMsg(Property.SYSTEM_HOME_PROPERTY+"=" + 
				PropertyUtil.getSystemProperty(Property.SYSTEM_HOME_PROPERTY));
		
		//Log properties related to redirection of derby.log 
		String target = 
//IC see: https://issues.apache.org/jira/browse/DERBY-6350
			PropertyUtil.getSystemProperty(Property.ERRORLOG_STYLE_PROPERTY);
		if (target != null)
			logMsg(Property.ERRORLOG_STYLE_PROPERTY+"=" + target);
        
		target = 
			PropertyUtil.getSystemProperty(Property.ERRORLOG_FILE_PROPERTY);
		if (target != null)
			logMsg(Property.ERRORLOG_FILE_PROPERTY+"=" + target);
		
		target = 
			PropertyUtil.getSystemProperty(Property.ERRORLOG_METHOD_PROPERTY);
		if (target != null)
			logMsg(Property.ERRORLOG_METHOD_PROPERTY+"=" + target);
		
		target = 
			PropertyUtil.getSystemProperty(Property.ERRORLOG_FIELD_PROPERTY);
		if (target != null)
			logMsg(Property.ERRORLOG_FIELD_PROPERTY+"=" + target);

        if (logBootTrace)
           Monitor.logThrowable(new Throwable("boot trace"));
		uf = null;



		CacheFactory cf = (CacheFactory) 
//IC see: https://issues.apache.org/jira/browse/DERBY-6648
            startSystemModule(
                org.apache.derby.shared.common.reference.Module.CacheFactory);
//IC see: https://issues.apache.org/jira/browse/DERBY-6945

        // Initialize the page cache
	    int pageCacheSize = getIntParameter(
					RawStoreFactory.PAGE_CACHE_SIZE_PARAMETER,
                    null,
                    RawStoreFactory.PAGE_CACHE_SIZE_DEFAULT,
                    RawStoreFactory.PAGE_CACHE_SIZE_MINIMUM,
                    RawStoreFactory.PAGE_CACHE_SIZE_MAXIMUM);

		pageCache =
//IC see: https://issues.apache.org/jira/browse/DERBY-3734
            cf.newCacheManager(
                this, "PageCache", pageCacheSize / 2, pageCacheSize);

        // Initialize the container cache
	    int fileCacheSize = getIntParameter(
                    RawStoreFactory.CONTAINER_CACHE_SIZE_PARAMETER,
                    null,
                    RawStoreFactory.CONTAINER_CACHE_SIZE_DEFAULT,
                    RawStoreFactory.CONTAINER_CACHE_SIZE_MINIMUM,
                    RawStoreFactory.CONTAINER_CACHE_SIZE_MAXIMUM);

		containerCache = 
            cf.newCacheManager(
                this, "ContainerCache", fileCacheSize / 2, fileCacheSize);

        // Register MBeans that allow users to monitor the page cache
        // and the container cache.
//IC see: https://issues.apache.org/jira/browse/DERBY-6733
        pageCache.registerMBean(dataDirectory);
        containerCache.registerMBean(dataDirectory);

		if (create)
		{
			String noLog =
				startParams.getProperty(Property.CREATE_WITH_NO_LOG);

			inCreateNoLog = 
                (noLog != null && Boolean.valueOf(noLog).booleanValue());
		}

		droppedTableStubInfo = new Hashtable<LogInstant,Object[]>();
//IC see: https://issues.apache.org/jira/browse/DERBY-6213

        // If derby.system.durability=test then set flags to disable sync of
        // data pages at allocation when file is grown, disable sync of data
        // writes during checkpoint
//IC see: https://issues.apache.org/jira/browse/DERBY-218
        if (Property.DURABILITY_TESTMODE_NO_SYNC.equalsIgnoreCase(
            PropertyUtil.getSystemProperty(Property.DURABILITY_PROPERTY)))
        {
            // - disable syncing of data during checkpoint.
            dataNotSyncedAtCheckpoint = true;

            // log message stating that derby.system.durability
            // is set to a mode, where syncs wont be forced and the
            // possible consequences of setting this mode
            Monitor.logMessage(MessageService.getTextMessage(
            	MessageId.STORE_DURABILITY_TESTMODE_NO_SYNC,
            	Property.DURABILITY_PROPERTY,
                Property.DURABILITY_TESTMODE_NO_SYNC));
		}
        else if (Performance.MEASURE)
        {
            // development build only feature, must by hand set the 
            // Performance.MEASURE variable and rebuild.  Useful during
            // development to compare/contrast effect of syncing, release
            // users can use the above relaxed durability option to disable
            // all syncing.  

            // debug only flag - disable syncing of data during checkpoint.
            dataNotSyncedAtCheckpoint = 
                PropertyUtil.getSystemBoolean(
                    Property.STORAGE_DATA_NOT_SYNCED_AT_CHECKPOINT);

            if (dataNotSyncedAtCheckpoint)
                Monitor.logMessage(
                    "Warning: " + 
                    Property.STORAGE_DATA_NOT_SYNCED_AT_CHECKPOINT +
                    "set to true.");
		}

        fileHandler = new RFResource( this);
	} // end of boot

	public void	stop() 
    {
		boolean OK = false;

		if (rawStoreFactory != null)
		{
			DaemonService rawStoreDaemon = rawStoreFactory.getDaemon();
			if (rawStoreDaemon != null)
				rawStoreDaemon.stop();
		}

		boolean logBootTrace = PropertyUtil.getSystemBoolean(Property.LOG_BOOT_TRACE);
//IC see: https://issues.apache.org/jira/browse/DERBY-4873
		logMsg(LINE);
//IC see: https://issues.apache.org/jira/browse/DERBY-4755
		logMsg(new Date() +
                MessageService.getTextMessage(
                    MessageId.STORE_SHUTDOWN_MSG,
//IC see: https://issues.apache.org/jira/browse/DERBY-4598
//IC see: https://issues.apache.org/jira/browse/DERBY-4601
                    getIdentifier(),
                    getRootDirectory(),
                    // print object and ide of classloader.
                    // Cast to object so we don't get just the toString() 
                    // method
                    (Object) this.getClass().getClassLoader()));
	
		if (logBootTrace)
			Monitor.logThrowable(new Throwable("shutdown trace"));
			
		if (!isCorrupt) 
        {
			try 
            {
				if (pageCache != null && containerCache != null) 
                {
					pageCache.shutdown();
					containerCache.shutdown();

					OK = true;
				}

			} 
            catch (StandardException se) 
            {
				se.printStackTrace(istream.getPrintWriter());
			}
		}

		removeTempDirectory();

		if (isReadOnly())		// do enough to close all files, then return 
        {
//IC see: https://issues.apache.org/jira/browse/DERBY-5916
			if (storageFactory != null)
				storageFactory.shutdown();
			return;
        }


		// re-enable stub removal until a better method can be found.
		// only remove stub if caches are cleaned
		if (removeStubsOK && OK)	
			removeStubs();

		releaseJBMSLockOnDB();
        
        if ( writableStorageFactory != null ) { writableStorageFactory.shutdown(); }
	} // end of stop

	/*
	** CacheableFactory
	*/
	public Cacheable newCacheable(CacheManager cm) 
    {
		if (cm == pageCache) 
        {
			StoredPage sp = new StoredPage();
			sp.setFactory(this);
			return sp;
		}

		// container cache
		return newContainerObject();
	}

	/**
		Database creation finished

		@exception StandardException Standard Derby exception policy.
	*/
	public void createFinished() throws StandardException
	{
		if (!inCreateNoLog)
        {
			throw StandardException.newException(
                SQLState.FILE_DATABASE_NOT_IN_CREATE);
        }

		// the changes in cache are not logged, they have to be flushed to disk
		checkpoint();
		inCreateNoLog = false;
	}

	/*
	** Methods of DataFactory
	*/
	
	public ContainerHandle openContainer(
    RawTransaction  t, 
    ContainerKey    containerId, 
    LockingPolicy   locking, 
    int             mode)
		throws StandardException 
    {
        return openContainer(
                t, containerId, locking, mode, false /* is not dropped */);
	}


	/**
		@see DataFactory#openDroppedContainer
		@exception StandardException Standard Derby error policy
	*/
	public RawContainerHandle openDroppedContainer(
    RawTransaction  t, 
    ContainerKey    containerId,  
    LockingPolicy   locking, 
    int             mode)
		 throws StandardException  
	{
		// since we are opening a possible dropped container
		// lets not add any actions that will take palce on a commit.
		mode |= ContainerHandle.MODE_NO_ACTIONS_ON_COMMIT;

        return openContainer(
                t, containerId, locking, mode, true /* droppedOK */);
	}

	/**
		@see DataFactory#openContainer
		@exception StandardException Standard Derby error policy
	*/
	private RawContainerHandle openContainer(
    RawTransaction  t, 
    ContainerKey    identity,
    LockingPolicy   locking, 
    int             mode, 
    boolean         droppedOK)
		 throws StandardException
	{

		if (SanityManager.DEBUG) 
        {

			if ((mode & (ContainerHandle.MODE_READONLY | ContainerHandle.MODE_FORUPDATE))
				== (ContainerHandle.MODE_READONLY | ContainerHandle.MODE_FORUPDATE))
            {
				SanityManager.THROWASSERT("update and readonly mode specified");
            }

		}

		boolean waitForLock = ((mode & ContainerHandle.MODE_LOCK_NOWAIT) == 0);


		if ((mode & ContainerHandle.MODE_OPEN_FOR_LOCK_ONLY) != 0) 
        {
			// Open a container for lock only, we don't care if it exists, is 
            // deleted or anything about it. The container handle we return is
            // closed and cannot be used for fetch or update etc.
			BaseContainerHandle lockOnlyHandle =  
                new BaseContainerHandle(
                    getIdentifier(), t, identity, locking, mode);

			if (lockOnlyHandle.useContainer(true, waitForLock))
				return lockOnlyHandle;
			else
				return null;
		}


		BaseContainerHandle c;

		// see if the container exists	
		FileContainer container = (FileContainer) containerCache.find(identity);
		if (container == null)
			return null;
		
		if (identity.getSegmentId() == ContainerHandle.TEMPORARY_SEGMENT) 
        {

			if (SanityManager.DEBUG) 
            {
				SanityManager.ASSERT(container instanceof TempRAFContainer);
			}

			if ((mode & ContainerHandle.MODE_TEMP_IS_KEPT) == 
                    ContainerHandle.MODE_TEMP_IS_KEPT) 
            {
				// if the mode is kept, then, we do not want to truncate 
				mode |= ContainerHandle.MODE_UNLOGGED;
			} 
            else 
            {
				// this should be OK even if the table was opened read-only
				mode |= 
                    (ContainerHandle.MODE_UNLOGGED | 
                     ContainerHandle.MODE_TRUNCATE_ON_ROLLBACK);
			}
			
			locking = 
                t.newLockingPolicy(
                    LockingPolicy.MODE_NONE, 
                    TransactionController.ISOLATION_NOLOCK, true);
		} 
        else 
        {
			// real tables
			if (inCreateNoLog)
            {
				mode |= 
                    (ContainerHandle.MODE_UNLOGGED | 
                     ContainerHandle.MODE_CREATE_UNLOGGED);
            } else {
//IC see: https://issues.apache.org/jira/browse/DERBY-239

				// make sure everything is logged if logArchived is turn on
				// clear all UNLOGGED flag
//IC see: https://issues.apache.org/jira/browse/DERBY-3551
				if (logFactory.logArchived() || 
                                        logFactory.inReplicationMasterMode()) {
					mode &= ~(ContainerHandle.MODE_UNLOGGED |
							  ContainerHandle.MODE_CREATE_UNLOGGED);

				} else {

					// block the online backup if the container is being 
					// opened in unlogged mode, if the backup is already 
					// running then convert all unlogged opens to logged ones,
					// otherwise online backup copy will be inconsistent.

					if (((mode & ContainerHandle.MODE_UNLOGGED) == 
						 ContainerHandle.MODE_UNLOGGED) || 
						((mode & ContainerHandle.MODE_CREATE_UNLOGGED) == 
						 ContainerHandle.MODE_CREATE_UNLOGGED))									   
					{
//IC see: https://issues.apache.org/jira/browse/DERBY-239
						if (!t.blockBackup(false)) {
							// when a backup is in progress transaction can not
                            // block the backup, so convert  unlogged opens 
                            // to logged mode.
							mode &= ~(ContainerHandle.MODE_UNLOGGED |
									  ContainerHandle.MODE_CREATE_UNLOGGED);
						}
					}

				}

			}

			// if mode is UNLOGGED but not CREATE_UNLOGGED, then force the
			// container from cache when the transaction commits.  For
			// CREATE_UNLOGGED, client has the responsibility of forcing the
			// cache. 
			if (((mode & ContainerHandle.MODE_UNLOGGED) == 
                    ContainerHandle.MODE_UNLOGGED) &&
				((mode & ContainerHandle.MODE_CREATE_UNLOGGED) == 0)) 
            {
				mode |= ContainerHandle.MODE_FLUSH_ON_COMMIT;
			}
		}

		PageActions       pageActions  = null;
		AllocationActions allocActions = null;

		if ((mode & ContainerHandle.MODE_FORUPDATE) ==
			ContainerHandle.MODE_FORUPDATE)  
		{

			if ((mode & ContainerHandle.MODE_UNLOGGED) == 0)
			{
				// get the current loggable actions
				pageActions  = getLoggablePageActions();
				allocActions = getLoggableAllocationActions();
				
			} 
            else 
            {
				// unlogged
				pageActions  = new DirectActions();
				allocActions = new DirectAllocActions();
			}
		}

		c = new BaseContainerHandle(
                getIdentifier(), t, pageActions, 
                allocActions, locking, container, mode);	

		// see if we can use the container
		try 
        {
			if (!c.useContainer(droppedOK, waitForLock)) 
            {
				containerCache.release(container);
				return null;
			}
		} 
        catch (StandardException se) 
        {
			containerCache.release(container);
			throw se;
		}

		return c;
	}

	/** Add a container with a specified page size to a segment.
		@exception StandardException Standard Derby error policy
	*/
	public long addContainer(
    RawTransaction  t, 
    long            segmentId, 
    long            input_containerid, 
    int             mode,
    Properties      tableProperties,
    int             temporaryFlag)
        throws StandardException
	{
		if (SanityManager.DEBUG) 
        {
			if ((mode & ContainerHandle.MODE_CREATE_UNLOGGED) != 0)
				SanityManager.ASSERT(
                    (mode & ContainerHandle.MODE_UNLOGGED) != 0,
                    "cannot have CREATE_UNLOGGED set but UNLOGGED not set");
		}

        // If client has provided a containerid then use it, else use the 
        // internally generated one from getNextId().
        long containerId = 
            ((input_containerid != ContainerHandle.DEFAULT_ASSIGN_ID) ? 
                 input_containerid : getNextId());

		ContainerKey identity = new ContainerKey(segmentId, containerId);

		boolean tmpContainer = (segmentId == ContainerHandle.TEMPORARY_SEGMENT);

		ContainerHandle ch = null;
		LockingPolicy   cl = null;

		if (!tmpContainer) 
        {
			// lock the container before we create it.

			if (isReadOnly())
            {
				throw StandardException.newException(
                        SQLState.DATA_CONTAINER_READ_ONLY);
            }

			cl = t.newLockingPolicy(LockingPolicy.MODE_CONTAINER,
					TransactionController.ISOLATION_SERIALIZABLE, true);
			
			if (SanityManager.DEBUG)
				SanityManager.ASSERT(cl != null);

			ch = t.openContainer(identity, cl, 
                   (ContainerHandle.MODE_FORUPDATE | 
                    ContainerHandle.MODE_OPEN_FOR_LOCK_ONLY));
		}

		FileContainer container = 
			(FileContainer) containerCache.create(identity, tableProperties); 

		// create the first alloc page and the first user page, 
		// if this fails for any reason the transaction
		// will roll back and the container will be dropped (removed)
		ContainerHandle containerHdl = null;
		Page            firstPage    = null;

		try 
        {
			// if opening a temporary container with IS_KEPT flag set,
			// make sure to open it with IS_KEPT too.
			if (tmpContainer && 
				((temporaryFlag & TransactionController.IS_KEPT) == 
                     TransactionController.IS_KEPT)) 
            {

				mode |= ContainerHandle.MODE_TEMP_IS_KEPT;
			}

			// open no-locking as we already have the container locked
			containerHdl = 
                t.openContainer(
                    identity, null, (ContainerHandle.MODE_FORUPDATE | mode)); 

			// we just added it, containerHdl should not be null
            if (SanityManager.DEBUG)
                SanityManager.ASSERT(containerHdl != null);

			if (!tmpContainer) 
            {
				// make it persistent (in concept if not in reality)
				RawContainerHandle rch = (RawContainerHandle)containerHdl;

				ContainerOperation lop = 
					new ContainerOperation(rch, ContainerOperation.CREATE);

				// mark the container as pre-dirtied so that if a checkpoint
				// happens after the log record is sent to the log stream, the
				// cache cleaning will wait for this change.
				rch.preDirty(true);
				try
				{
					t.logAndDo(lop);

					// flush the log to reduce the window between where
					// the container is created & synced and the log record
					// for it makes it to disk. If we fail in this
					// window we will leave a stranded container file.
					flush(t.getLastLogInstant());
				}
				finally
				{
					// in case logAndDo fail, make sure the container is not
					// stuck in preDirty state.
					rch.preDirty(false);
				}
			}

			firstPage = containerHdl.addPage();

		} 
        finally 
        {

			if (firstPage != null) 
            {
				firstPage.unlatch();
				firstPage = null;
			}
			
			containerCache.release(container);

			if (containerHdl != null) 
            {
				containerHdl.close();
				containerHdl = null;
			}

			if (!tmpContainer) 
            {
                // this should do nothing, since we requested isolation 3
                // but we can't assume that, so call the policy correctly.

				cl.unlockContainer(t, ch);	
			}
		}

		return containerId;
	}

	/** Add and load a stream container
		@exception StandardException Standard Derby error policy
	*/
	public long addAndLoadStreamContainer(
    RawTransaction  t, 
    long            segmentId,
    Properties      tableProperties, 
    RowSource       rowSource)
		throws StandardException
	{
		long containerId = getNextId();

		ContainerKey identity = new ContainerKey(segmentId, containerId);

		// create and load the stream container
		StreamFileContainer sContainer = 
            new StreamFileContainer(identity, this, tableProperties);
		sContainer.load(rowSource);

		return containerId;
	}


	/**
		open an exsisting streamContainer

		@see DataFactory#openStreamContainer
		@exception StandardException Standard Derby error policy
	*/
	public StreamContainerHandle openStreamContainer(
    RawTransaction  t, 
    long            segmentId, 
    long            containerId,
    boolean         hold)
		 throws StandardException
	{

		ContainerKey identity = new ContainerKey(segmentId, containerId);

		StreamFileContainerHandle c;

		// open the container with the identity
		StreamFileContainer container = new StreamFileContainer(identity, this);
		container = container.open(false);
		if (container == null)
			return null;

		c = new StreamFileContainerHandle(getIdentifier(), t, container, hold);	

		// see if we can use the container
		if (c.useContainer())
			return c;
		else
			return null;
	}

	/**
		Drop a stream container.

	    <P><B>Synchronisation</B>
		<P>
		This call will remove the container.

		@exception StandardException Standard Derby error policy
	*/
	public void dropStreamContainer(
    RawTransaction  t, 
    long            segmentId, 
    long            containerId) 
		throws StandardException
	{

		boolean tmpContainer = (segmentId == ContainerHandle.TEMPORARY_SEGMENT);

		StreamContainerHandle containerHdl = null;

		try
		{
			ContainerKey ckey = new ContainerKey(segmentId, containerId);

			// close all open containers and 'onCommit' objects of the container
			t.notifyObservers(ckey);

			containerHdl = t.openStreamContainer(segmentId, containerId, false);
			if (tmpContainer && (containerHdl != null))
			{
				containerHdl.removeContainer();
				return;
			}
		}
		finally
		{
			if (containerHdl != null)
				containerHdl.close();
		}
	}

	/**
		re-Create a container during redo recovery.

		called ONLY during recovery load tran.

		@exception StandardException Standard Derby Error policy
	 */
	public void reCreateContainerForRedoRecovery(
    RawTransaction  t, 
    long            segmentId, 
    long            containerId, 
    ByteArray       containerInfo)
		 throws StandardException
	{
		if (SanityManager.DEBUG)
			SanityManager.ASSERT(segmentId != ContainerHandle.TEMPORARY_SEGMENT,
				"Cannot recreate temp container during load tran");

		ContainerKey identity = new ContainerKey(segmentId, containerId);

		// no need to lock container during load tran
		// no need to create any page for the container, they will be created
		// as their log records are encountered later in load tran

		FileContainer container = 
			(FileContainer)containerCache.create(identity, containerInfo); 

		containerCache.release(container);
	}

	/**
		Drop a container.

	    <P><B>Synchronisation</B>
		<P>
		This call will mark the container as dropped and then obtain an CX lock
		(table level exclusive lock) on the container. Once a container has 
        been marked as dropped it cannot be retrieved by an openContainer() 
        call unless explicitly with droppedOK.
		<P>
		Once the exclusive lock has been obtained the container is removed
		and all its pages deallocated. The container will be fully removed
		at the commit time of the transaction.

		@exception StandardException Standard Derby error policy
	*/
	public void dropContainer(
    RawTransaction  t, 
    ContainerKey    ckey) 
		 throws StandardException
	{
		boolean tmpContainer = 
            (ckey.getSegmentId() == ContainerHandle.TEMPORARY_SEGMENT);

		LockingPolicy cl = null;

		if (!tmpContainer) 
        {
			if (isReadOnly())
            {
				throw StandardException.newException(
                        SQLState.DATA_CONTAINER_READ_ONLY);
            }

			cl = 
                t.newLockingPolicy(
                    LockingPolicy.MODE_CONTAINER,
                    TransactionController.ISOLATION_SERIALIZABLE, true);
		
			if (SanityManager.DEBUG)
				SanityManager.ASSERT(cl != null);
		}

		// close all open containers and 'onCommit' objects of this container
		t.notifyObservers(ckey);

		RawContainerHandle containerHdl = (RawContainerHandle)
			t.openContainer(ckey, cl, ContainerHandle.MODE_FORUPDATE);

		// If container is already dropped or is no longer there, throw
		// containerVanished exception unless container is temporary, in that
		// case just return.  Upper layer is supposed to prevent such from
		// happening thru some means other than the lock we are getting here.
		try
		{
			if (containerHdl == null || 
				containerHdl.getContainerStatus() != RawContainerHandle.NORMAL) 
			{
				// If we are a temp container, don't worry about it.
				if (tmpContainer)
				{
					if (containerHdl != null)
						containerHdl.removeContainer((LogInstant)null);
					return;
				}
				else
                {
					throw StandardException.newException(
                            SQLState.DATA_CONTAINER_VANISHED, ckey);
                }
			}

			// Container exist, is updatable and we got the lock.
			if (tmpContainer)
			{
				containerHdl.dropContainer((LogInstant)null, true);
				containerHdl.removeContainer((LogInstant)null);
			}
			else
			{
				ContainerOperation lop = 
					new ContainerOperation(
                            containerHdl, ContainerOperation.DROP);

				// mark the container as pre-dirtied so that if a checkpoint
				// happens after the log record is sent to the log stream, the
				// cache cleaning will wait for this change.
				containerHdl.preDirty(true);
				try
				{
					t.logAndDo(lop);
				}
				finally
				{
					// in case logAndDo fail, make sure the container is not
					// stuck in preDirty state.
					containerHdl.preDirty(false);
				}


				// remember this as a post commit work item
				Serviceable p = 
                    new ReclaimSpace(
                            ReclaimSpace.CONTAINER, 
                            ckey, 
                            this, 
                            true /* service ASAP */);

				if (SanityManager.DEBUG)
                {
                    if (SanityManager.DEBUG_ON(DaemonService.DaemonTrace))
                    {
                        SanityManager.DEBUG(
                            DaemonService.DaemonTrace, 
                            "Add post commit work " + p);
                    }
                }

				t.addPostCommitWork(p);
			}

		}
		finally
		{
			if (containerHdl != null)
				containerHdl.close();
		}


	}


    /**
     * Implement checkpoint operation, write/sync all pages in cache.
     * <p>
     * The derby write ahead log algorithm uses checkpoint of the data
     * cache to determine points of the log no longer required by
     * restart recovery.  
     * <p>
     * This implementation uses the 2 cache interfaces to force all dirty
     * pages to disk:
     *
     * WRITE DIRTY PAGES TO OS:
     * In the first step all pages in the page cache
     * are written, but not synced (pagecache.cleanAll).  The cachemanager
     * cleanAll() interface guarantees that every dirty page that exists
     * when this call is first made will have it's clean() method called.
     * The data cache (CachedPage.clean()), will call writePage but not
     * sync the page.  
     * By using the java write then sync, the checkpoint is
     * usually doing async I/O, allowing the OS to schedule multiple I/O's
     * to the file as efficiently as it can.
     * Note that it has been observed that checkpoints
     * can flood the I/O system because these writes are not synced, see
     * DERBY-799 - checkpoint should probably somehow restrict the rate
     * it sends out those I/O's - it was observed a simple sleep every
     * N writes fixed most of the problem.  
     *
     * FORCE THOSE DIRTY WRITES TO DISK:
     * To force the I/O's to disk, the system calls each open dirty file
     * and uses the java interface to sync any outstanding dirty pages to
     * disk (containerCache.cleanAll()).  The open container cache does
     * this work in RAFContainer.clean() by writing it's header out and
     * syncing the file.  (Note if any change is made to checkpoint to
     * sync the writes vs. syncing the file, one probably still needs to 
     * write the container header out and sync it).
     *
	 * @exception  StandardException  Standard exception policy.
     **/
	public void checkpoint() throws StandardException 
    {
		pageCache.cleanAll();
		containerCache.cleanAll();
	}

	public void idle() throws StandardException 
    {
		pageCache.ageOut();
		containerCache.ageOut();
	}

	public void setRawStoreFactory(
    RawStoreFactory rsf, 
    boolean         create,
    Properties      startParams)
		 throws StandardException
	{

		rawStoreFactory = rsf;

		/*
		 * boot the log factory here because different implementation of the
		 * data	factory wants different types of log factory
		 */
		bootLogFactory(create, startParams);

	}


	/**
		Return my unique identifier

		@see DataFactory#getIdentifier
	*/
	public UUID getIdentifier() 
    {
		return identifier;
	}

	/*
	** Called by post commit daemon, calling ReclaimSpace.performWork()
	*/
	public int reclaimSpace(
    Serviceable     work, 
    ContextManager  contextMgr)
		 throws StandardException
	{
		if (work == null)
			return Serviceable.DONE;

		Transaction tran = 
            rawStoreFactory.findUserTransaction(
                contextMgr, AccessFactoryGlobals.SYS_TRANS_NAME);

		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(tran != null, "null transaction");

			if (SanityManager.DEBUG_ON(DaemonService.DaemonTrace))
				SanityManager.DEBUG(DaemonService.DaemonTrace, 
									"Performing post commit work " + work);
		}

		return ReclaimSpaceHelper.reclaimSpace(this, (RawTransaction)tran,
											   (ReclaimSpace)work); 
	}

	/**
		Really this is just a convience routine for callers that might not
		have access to a log factory.
	*/
	public StandardException markCorrupt(StandardException originalError) 
    {
		boolean firsttime = !isCorrupt;

		isCorrupt = true;
		if (getLogFactory() != null)
			getLogFactory().markCorrupt(originalError);

		// if firsttime markCorrupt is called, release the JBMS lock so user
		// can move the database if so desired.
		if (firsttime)
		{
			// get rid of everything from the cache without first cleaning them
			if (pageCache != null)
				pageCache.discard(null);

			if (containerCache != null)
				containerCache.discard(null);

			// don't read in any more pages 
			pageCache = null;
			containerCache = null;

			releaseJBMSLockOnDB();
		}

		return originalError;
	}

	public FileResource getFileHandler() 
    {			
		return fileHandler;
	}

	public void removeStubsOK()
	{
		removeStubsOK = true;
	}

	/*
	** Implementation specific methods
	*/

    /**
     * Register a handler class for insert undo events.
     * <p>
     * Register a class to be called when an undo of an insert is executed.  
     * When an undo of an event is executed by the raw store 
     * UndoHandler.insertUndoNotify() will be called, allowing upper level 
     * callers to execute code as necessary.  The initial need is for the 
     * access layer to be able to queue post commit reclaim space in the case 
     * of inserts which are aborted (including the normal case of inserts 
     * failed for duplicate key violations) (see DERBY-4057)
     * <p>
     *
     * @param input_undo_handle Class to use to notify callers of an undo of 
     *                          an insert.
     *
     * @exception  StandardException  Standard exception policy.
     **/
    public void setUndoInsertEventHandler(
        UndoHandler input_undo_handle)
    {
        undo_handler = input_undo_handle;
    }
    
    /**
     * Notify through set handler that an undo of an insert has happened.
     * <p>
     * When an undo of an event is executed by the raw store 
     * UndoHandler.insertUndoNotify() will be called, allowing upper level 
     * callers to execute code as necessary.  The initial need is for the 
     * access layer to be able to queue post commit reclaim space in the case 
     * of inserts which are aborted (including the normal case of inserts 
     * failed for duplicate key violations) (see DERBY-4057)
     * Longer descrption of routine.
     * <p>
     *
     * @param rxact     raw transaction of the aborted insert.
     * @param page_key  page key of the aborted insert.
     *
     * @exception  StandardException  Standard exception policy.
     **/
    protected void insertUndoNotify(
    RawTransaction  rxact,
    PageKey         page_key)
        throws StandardException
    {
        if (undo_handler != null)
        {
            undo_handler.insertUndoNotify(rxact, page_key);
        }
    }

	public int getIntParameter(
    String      parameterName, 
    Properties  properties, 
    int         defaultValue, 
    int         minimumValue, 
    int         maximumValue) 
    {

		int newValue;

		String parameter = null;
		
		if (properties != null)
			parameter = properties.getProperty(parameterName);

		if (parameter == null)
			parameter = PropertyUtil.getSystemProperty(parameterName);

		if (parameter != null) 
        {
			try 
            {
				newValue = Integer.parseInt(parameter);

				if ((newValue >= minimumValue) && (newValue <= maximumValue)) 
                    return newValue;
			} 
            catch (NumberFormatException nfe) 
            {
				// just leave the size at the default.				
			}
		}

		return defaultValue;
	}

	CacheManager getContainerCache() 
    {
		return containerCache;
	}

	CacheManager getPageCache() 
    {
		return pageCache;
	}

	/**
		Ask the log factory to flush up to this log instant.

		@exception StandardException cannot sync log file
	*/
	void flush(LogInstant instant)
		 throws StandardException
	{
		getLogFactory().flush(instant);
	}

	LogFactory getLogFactory() 
	{
		return logFactory;
	}


	RawStoreFactory getRawStoreFactory() 
    {
		return rawStoreFactory;
	}

    @Override
	public String getRootDirectory() 
    {
		return dataDirectory;
	}

    /**
     * Produces new container objects.
     * <p>
     * Concrete implementations of a DataFactory must implement this routine
     * to indicate what kind of containers are produced. This class produces
     * file-based containers - RAFContainer objects for files that support
     * random access and InputStreamContainer object for others, such as data
     * files in JARs.
     * <p>
     * @return A new file container object.
     *
     **/
    Cacheable newContainerObject()
    {
        if( supportsRandomAccess)
//IC see: https://issues.apache.org/jira/browse/DERBY-801
            return newRAFContainer(this);
        else
            return new InputStreamContainer( this);
    } 

    /**
     * Creates a RAFContainer object.
     * This method is overridden in BaseDataFileFactoryJ4 to produce
     * RAFContainer4 objects instead of RAFContainer objects.
     */
    protected Cacheable newRAFContainer(BaseDataFileFactory factory) {
        return new RAFContainer(factory);
    }

	/*
	 * Get the loggable page action that is associated with this implementation
	 *
	 * @return the PageActions
	 * @exception StandardExceptions Standard Derby Error Policy
	 */
	private PageActions getLoggablePageActions() throws StandardException
	{
		if (loggablePageActions == null)
			loggablePageActions = new LoggableActions();
		return loggablePageActions;
	}

	/**
	 * Get the loggable allocation action associated with this implementation
	 *
	 * @return the PageActions
	 */
	private AllocationActions getLoggableAllocationActions() 
	{
		if (loggableAllocActions == null)
			loggableAllocActions = new LoggableAllocActions();
		return loggableAllocActions;
	}

    private synchronized void removeTempDirectory()
    {
        if( storageFactory != null)
        {
            actionCode = REMOVE_TEMP_DIRECTORY_ACTION;
            try
            {
                AccessController.doPrivileged( this);
            }
            catch (PrivilegedActionException pae)
            {
                // removeTempDirectory does not throw an exception
            }
        }
    } 

    /**
     * Return the path to a container file.
     * <p>
     * Return the path to a container file that is relative to the root 
     * directory.
     * <p>
     * The format of the name of an existing container file is:
     *     segNNN/cXXX.dat
     * The format of the name of a stub describing a dropped container file is:
     *     segNNN/dXXX.dat
     *
     * NNN = segment number, currently 0 is where normal db files are found.
     * XXX = The hex representation of the container number
     *
     * The store will always create containers with this format name, but 
     * the store will also recognize the following two formats when attempting
     * to open files - as some copy tools have uppercased our filesnames when
     * moving across operating systems:
     *
     * The format of the name of an existing container file is:
     *     segNNN/CXXX.DAT
     * The format of the name of a stub describing a dropped container file is:
     *     segNNN/DXXX.DAT
     * <p>
     *
     *
     * @param containerId The container being opened/created
     * @param stub        True if the file name for the stub is requested, 
     *                      otherwise the file name for the data file
     *
	 * @return The StorageFile representing path to container relative to root.
     *
     **/
	public StorageFile getContainerPath(
    ContainerKey    containerId, 
    boolean         stub) 
    {
        return getContainerPath(containerId, stub, GET_CONTAINER_PATH_ACTION);
    }

    private synchronized StorageFile getContainerPath(
    ContainerKey    containerId, 
    boolean         stub,
    int             code)
    {
        actionCode = code;
        try
        {
            this.containerId = containerId;
            this.stub = stub;
            try
            {
                return (StorageFile) AccessController.doPrivileged( this);
            }
            catch (PrivilegedActionException pae)
            { 
                // getContainerPath does not throw an exception
                return null;
            }
        }
        finally 
        { 
            this.containerId = null; 
        }
	}


	/**
		Return an alternate path to container file relative to the root directory.
        The alternate path uses upper case 'C','D', and 'DAT' instead of 
        lower case - there have been cases of people copying the database and
        somehow upper casing all the file names.

        The intended use is as a bug fix for track 3444.

		@param containerId The container being opened/created
		@param stub True if the file name for the stub is requested, otherwise the file name for the data file

	*/
	public StorageFile getAlternateContainerPath(
    ContainerKey    containerId, 
    boolean         stub)
    {
        return getContainerPath(
                    containerId, stub, GET_ALTERNATE_CONTAINER_PATH_ACTION);
	}



	/**
		Remove stubs in this database.  Stubs are committed deleted containers
	*/
	private synchronized void removeStubs()
	{
//IC see: https://issues.apache.org/jira/browse/DERBY-5916
        if( storageFactory != null) 
        {
            actionCode = REMOVE_STUBS_ACTION;
            try
            {
                AccessController.doPrivileged( this);
            }
            catch (PrivilegedActionException pae)
            {
                // removeStubs does not throw an exception
            } 
        }
	}

	/**
	 * keeps track of information about the stub files of the  committed deleted
	 * containers. We use the info to delete them at checkpoints.
	 * In addition to the file info , we also keep track of the identity of the
	 * container; which helps to remove entry in the cache and the log instant
	 * when the stub was created, which helps us to figure out whether we
	 * require the stub file for the crash recovery.
	 * We maintain the information in a hashtable:
	 * key(LOG INSTANT) Values: File handle , and ContainerIdentity.
	 **/
	public void stubFileToRemoveAfterCheckPoint(
    StorageFile file, 
    LogInstant  logInstant, 
    Object      identity) 
    {
		if(droppedTableStubInfo != null)
		{
			Object[] removeInfo = new Object[2];
			removeInfo[0]       = file;
			removeInfo[1]       = identity;
			droppedTableStubInfo.put(logInstant, removeInfo);
		}
	}    

	/**
	 * Delete the stub files that are not required for recovery. A stub file
	 * is not required to be around if the recovery is not going to see
	 * any log record that belongs to that container. Since the stub files
	 * are created as a post commit operation, they are not necessary during
	 * undo operation of the recovery.
	 *
	 * To remove a stub file we have to be sure that it was created before the
	 * redoLWM in the check point record. We can be sure that the stub is not
	 * required if the log instant when it was created is less than the redoLWM.
	 */
	public void removeDroppedContainerFileStubs(
    LogInstant redoLWM) 
        throws StandardException
	{
	
		if (droppedTableStubInfo != null) 
		{
			synchronized(droppedTableStubInfo)
			{
//IC see: https://issues.apache.org/jira/browse/DERBY-6213
				for (Enumeration<LogInstant> e = droppedTableStubInfo.keys(); 
                     e.hasMoreElements(); ) 
				{
					LogInstant logInstant  = e.nextElement();
					if(logInstant.lessThan(redoLWM))
					{
						
						Object[] removeInfo = 
                            droppedTableStubInfo.get(logInstant);
						Object identity = removeInfo[1];
						//delete the entry in the container cache.
						Cacheable ccentry =	containerCache.findCached(identity);
						if(ccentry!=null)
							containerCache.remove(ccentry);

						//delete the stub we don't require it during recovery
                        synchronized( this)
                        {
                            actionFile = (StorageFile)removeInfo[0];
                            actionCode = DELETE_IF_EXISTS_ACTION;
                            try
                            {
                                if (AccessController.doPrivileged(this) != null) 
                                {
                                    //if we successfuly delete the file remove 
                                    //it from the hash table.
                                    droppedTableStubInfo.remove(logInstant);
                                }
                            }
                            catch (PrivilegedActionException pae)
                            {
                                // DELETE_IF_EXISTS does not throw an exception
                            }
                        }
					}
				}
			}
		}
	}






    /**
     * Find the largest containerid is seg 0.
     * <p>
     * Do a file list of the files in seg0 and return the highest numbered
     * file found.
     * <p>
     * Until I figure out some reliable place to store this information across
     * a boot of the system, this is what is used following a boot to assign
     * the next conglomerate id when a new conglomerate is created.  It is
     * only called at most once, and then the value is cached by calling store
     * code.
     * <p>
     *
	 * @return The largest containerid in seg0.
     **/
	private synchronized long findMaxContainerId()
	{
        actionCode = FIND_MAX_CONTAINER_ID_ACTION;
        try
        {
            return ((Long) AccessController.doPrivileged( this)).longValue();
        }
        catch (PrivilegedActionException pae)
        { 
            // findMaxContainerId does not throw an exception
            return 0;
        }
	}

	private void bootLogFactory(
    boolean     create, 
    Properties  startParams) 
        throws StandardException 
    {

		if (isReadOnly())
        {
			startParams.put(
                LogFactory.RUNTIME_ATTRIBUTES, LogFactory.RT_READONLY);
        }

		logFactory = (LogFactory)
//IC see: https://issues.apache.org/jira/browse/DERBY-6648
			bootServiceModule(
                create, this, 
                rawStoreFactory.getLogFactoryModule(), startParams);
	}


	/**
		Does this factory support this service type.
	*/
	private boolean handleServiceType(
    String      type) 
    {
        try
        {
            PersistentService ps = 
//IC see: https://issues.apache.org/jira/browse/DERBY-6648
                getMonitor().getServiceProvider(type);
            return ps != null && ps.hasStorageFactory();
        }
        catch (StandardException se)
        { 
            return false;
        }
	}

	/**
		check to see if we are the only JBMS opened against this database.

		<BR>This method does nothing if this database is read only or we cannot
		access files directly on the database directory.

		<BR>We first see if a file named db.lck exists on the top database
		directory (i.e., the directory where service.properties lives).  If it
		doesn't exist, we create it and write to it our identity which is
		generated per boot of the JBMS.

		<BR>If the db.lck file already exists when we boot this database, we
		try to delete it first, assuming that an opened RandomAccessFile can
		act as a file lock against delete.  If that succeeds, we may hold a
		file lock against subsequent JBMS that tries to attach to this
		database before we exit.

		<BR>We test to see if we think an opened file will prevent it from
		being deleted, if so, we will hold on to the open file descriptor and
		use it as a filelock.  If not, and we started out deleting an existing
		db.lck file, we issue a warning message to the info stream that we are
		about to attached to a database which may already have another JBMS
		attached to it. Then we overwrite that db.lck file with our identity.

		<BR>Upon shutdown, we delete the db.lck file.  If the system crash
		instead of shutdown cleanly, it will be cleaned up the next time the
		system boots

		@exception StandardException another JBMS is already attached to the
		database at this directory
	*/
	private void getJBMSLockOnDB(
    UUID        myUUID, 
    UUIDFactory uuidFactory, 
    String      databaseDirectory)
		 throws StandardException
	{
		if (fileLockOnDB != null) // I already got the lock!
			return;

		if (isReadOnly())
			return;
		if (SanityManager.DEBUG)
		{
			if (myUUID == null)
				SanityManager.THROWASSERT("myUUID == null");
		}

        synchronized( this)
        {
            actionCode = GET_LOCK_ON_DB_ACTION;
            this.myUUID = myUUID;
            this.uuidFactory = uuidFactory;
            this.databaseDirectory = databaseDirectory;
            
            try
            {
                AccessController.doPrivileged( this);
            }
            catch (PrivilegedActionException pae) 
            { 
                throw (StandardException) pae.getException(); 
            }
            finally
            {
                this.myUUID = null;
                this.uuidFactory = null;
                this.databaseDirectory = null;
            }
        }

		// OK file lock is reliable, we think... keep the fileLockOnDB file
		// descriptor open to prevent other JBMS from booting
		// fileLockOnDB is not null in this case
	}

    // Called from within a privilege block
    private void privGetJBMSLockOnDB() throws StandardException
    {
        boolean fileLockExisted = false;
        String blownUUID = null;

        StorageFile fileLock = storageFactory.newStorageFile( DB_LOCKFILE_NAME);

        try
        {
            // assume we are not read only
            // SECURITY PERMISSION MP1
            if (fileLock.exists())
            {
                fileLockExisted = true;

                // see what it says in case we cannot count on delete failing
                // when someone else have an opened file descriptor.
                // I may be blowing this JBMS's lock away
                // SECURITY PERMISSION MP1
                // SECURITY PERMISSION OP4
                fileLockOnDB = fileLock.getRandomAccessFile( "rw");
                try
                {
                    blownUUID = fileLockOnDB.readUTF();
                }
                catch (IOException ioe)
                {
                    // The previous owner of the lock may have died before
                    // finish writing its UUID down.
                    fileLockExisted = false;
                }

                fileLockOnDB.close();
                fileLockOnDB = null;

                // SECURITY PERMISSION OP5
                if (!fileLock.delete())
                {
                    throw StandardException.newException(
                        SQLState.DATA_MULTIPLE_JBMS_ON_DB,
                        databaseDirectory);
                }
            }

            // if file does not exists, we grab it immediately - there is a
            // possibility that some other JBMS got to it sooner than we do,
            // check the UUID after we write it to make sure
            // SECURITY PERMISSION MP1
            // SECURITY PERMISSION OP5
            fileLockOnDB = fileLock.getRandomAccessFile( "rw");
            fileLock.limitAccessToOwner();

            // write it out for future reference
            fileLockOnDB.writeUTF(myUUID.toString()); 

//IC see: https://issues.apache.org/jira/browse/DERBY-4963
            fileLockOnDB.sync();
            fileLockOnDB.seek(0);
            // check the UUID
            UUID checkUUID = uuidFactory.recreateUUID(fileLockOnDB.readUTF());
            if (!checkUUID.equals(myUUID))
            {
                throw StandardException.newException(
                    SQLState.DATA_MULTIPLE_JBMS_ON_DB, databaseDirectory);
            }
        }
        catch (IOException ioe)
        {
            // probably a read only db, don't do anything more
            readOnly = true;
            try
            {
                if (fileLockOnDB != null)
                    fileLockOnDB.close();
            }
            catch (IOException ioe2)
            { /* did the best I could */ }
            fileLockOnDB = null;

            return;
        }

        if (fileLock.delete())
        {
            // if I can delete it while I am holding a opened file descriptor,
            // then the file lock is unreliable - send out a warning if I
            // have blown off another JBMS's lock on the DB

            Object[] args = new Object[3];
            args[0] = myUUID;
            args[1] = databaseDirectory;
            args[2] = blownUUID;

            //Try the exlcusive file lock method approach available in jdk1.4 or
            //above jvms where delete machanism  does not reliably prevent 
            //double booting of derby databases. If we don't get a reliable 
            //exclusive lock still we send out a warning.

            int exLockStatus = StorageFile.NO_FILE_LOCK_SUPPORT ;
            //If user has chosen to force lock option don't bother
            //about applying exclusive file lock mechanism 
            if(!throwDBlckException)
            {
                exFileLock   = 
                    storageFactory.newStorageFile( DB_EX_LOCKFILE_NAME);
                exLockStatus = exFileLock.getExclusiveFileLock();
            }

            if (exLockStatus == StorageFile.NO_FILE_LOCK_SUPPORT)
            {
                if (fileLockExisted && !throwDBlckException)
                {

                    String warningMsg = 
//IC see: https://issues.apache.org/jira/browse/DERBY-6262
                      MessageService.getTextMessage(
                          SQLState.DATA_MULTIPLE_JBMS_WARNING, args);

                    logMsg(warningMsg);

                    // RESOLVE - need warning support.  Output to
                    // system.err.println rather than just send warning 
                    // message to derby.log.
                    System.err.println(warningMsg);

                }
            }

            // filelock is unreliable, but we should at least leave a file
            // there to warn the next person
            try
            {
                // the existing fileLockOnDB file descriptor may already be
                // deleted by the delete call, close it and create the file 
                // again
                if(fileLockOnDB != null)
                    fileLockOnDB.close();
                fileLockOnDB = fileLock.getRandomAccessFile( "rw");
                fileLock.limitAccessToOwner();
//IC see: https://issues.apache.org/jira/browse/DERBY-5363
//IC see: https://issues.apache.org/jira/browse/DERBY-5363

                // write it out for future reference
                fileLockOnDB.writeUTF(myUUID.toString()); 

//IC see: https://issues.apache.org/jira/browse/DERBY-4963
                fileLockOnDB.sync();
                fileLockOnDB.close();
            }
            catch (IOException ioe)
            {
                try
                {
                    fileLockOnDB.close();
                }
                catch (IOException ioe2)
                { 
                    /* did the best I could */ 
                }
            }
            finally
            {
                fileLockOnDB = null;
            }

            if (fileLockExisted && throwDBlckException)
            {
                // user has chosen that we always throw exception, throw it
                // now that we have reinstated the lock file.
                throw StandardException.newException(
                    SQLState.DATA_MULTIPLE_JBMS_FORCE_LOCK, args);
            }
		
            if(exLockStatus == StorageFile.EXCLUSIVE_FILE_LOCK_NOT_AVAILABLE)
            {
				
                throw StandardException.newException(
                    SQLState.DATA_MULTIPLE_JBMS_ON_DB,
                    databaseDirectory);
            }

        }
    } // end of privGetJBMSLockOnDB

	private void releaseJBMSLockOnDB()
	{
		if (isReadOnly())
			return;

        synchronized( this)
        {
            actionCode = RELEASE_LOCK_ON_DB_ACTION;
            try
            {
                AccessController.doPrivileged( this);
            }
            catch (PrivilegedActionException pae)
            {
                // do nothing - it may be read only medium, who knows what the
                // problem is
            }
            finally
            {
                fileLockOnDB = null;
            }
        }
	}

    private void privReleaseJBMSLockOnDB() throws IOException
    {
        if (fileLockOnDB != null)
            fileLockOnDB.close();

        if (storageFactory != null)
        {
            StorageFile fileLock = 
                storageFactory.newStorageFile(DB_LOCKFILE_NAME);

//IC see: https://issues.apache.org/jira/browse/DERBY-32
            fileLock.delete();
        }

		//release the lock that is acquired using tryLock() to prevent
		//multiple jvm booting the same database on Unix environments.
		if(exFileLock != null)
			exFileLock.releaseExclusiveFileLock();
    } // end of privReleaseJBMSLockOnDB
        
	private void logMsg(String msg)
	{
		if (istream == null)
		{
			istream = Monitor.getStream();
		}

		istream.println(msg);
	}

	public final boolean databaseEncrypted()
	{
		return databaseEncrypted;
	}

    /** {@inheritDoc} */
    public void setDatabaseEncrypted(boolean isEncrypted)
	{
//IC see: https://issues.apache.org/jira/browse/DERBY-5792
        databaseEncrypted = isEncrypted;
	}

	public int encrypt(
    byte[]  cleartext, 
    int     offset, 
    int     length, 
    byte[]  ciphertext, 
    int     outputOffset,
    boolean newEngine)
		 throws StandardException
	{
		return rawStoreFactory.encrypt(
//IC see: https://issues.apache.org/jira/browse/DERBY-1156
                    cleartext, offset, length, 
                    ciphertext, outputOffset, 
                    newEngine);
	}

	public int decrypt(
    byte[]  ciphertext, 
    int     offset, 
    int     length,
    byte[]  cleartext, 
    int     outputOffset)
		 throws StandardException
	{
		return rawStoreFactory.decrypt(
                ciphertext, offset, length, cleartext, outputOffset);
	}

    /** {@inheritDoc} */
    public void decryptAllContainers(RawTransaction t)
            throws StandardException {
//IC see: https://issues.apache.org/jira/browse/DERBY-5792
        EncryptOrDecryptData containerDecrypter = new EncryptOrDecryptData(this);
        containerDecrypter.decryptAllContainers(t);
    }

    /** {@inheritDoc} */
    public void encryptAllContainers(RawTransaction t)
            throws StandardException {
        EncryptOrDecryptData containerEncrypter = new EncryptOrDecryptData(this);
        containerEncrypter.encryptAllContainers(t);
    }
 
    /** {@inheritDoc} */
    public void removeOldVersionOfContainers()
            throws StandardException {
        EncryptOrDecryptData containerCryptoOp = new EncryptOrDecryptData(this);
        containerCryptoOp.removeOldVersionOfContainers();
    }
    
    /**
     * Return a jar file by asking the class's 
     * class loader for the location where the class was loaded from. 
     * If no value, it returns null
     * @param cls the Class to ask to print the class name of an object
     *
     * @return the ClassPath of a jar file
     **/
    private static String jarClassPath(final Class cls)
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-6213
        return AccessController.doPrivileged( new PrivilegedAction<String>()
        {
          public String run()
          {
//IC see: https://issues.apache.org/jira/browse/DERBY-4715
              CodeSource cs = null;
              try {
                  cs = cls.getProtectionDomain().getCodeSource();
              }
              catch (SecurityException se) {
                  return se.getMessage();
              }
  
//IC see: https://issues.apache.org/jira/browse/DERBY-4944
              if ( cs == null || cs.getLocation() == null )
                  return null;        
      
              URL result = cs.getLocation ();
      
              return result.toString();
          }
        });
    }
    
    /**
     * Return values of system properties that identify the OS.
     * Will catch SecurityExceptions and note them for displaying information.
     * @return the Java system property value for the OS or a string capturing a
     * security exception.
     */
    private static String buildOSinfo () {
//IC see: https://issues.apache.org/jira/browse/DERBY-6213
    	return AccessController.doPrivileged(new PrivilegedAction<String>(){
    		public String run() {
//IC see: https://issues.apache.org/jira/browse/DERBY-5240
    			String osInfo = "";
    			try {
    				String currentProp = PropertyUtil.getSystemProperty("os.name");
    				if (currentProp != null)
    					osInfo = "os.name="+currentProp+"\n";
    				if ((currentProp = PropertyUtil.getSystemProperty("os.arch")) != null)
    					osInfo += "os.arch="+currentProp+"\n";
    				if ((currentProp = PropertyUtil.getSystemProperty("os.version")) != null)
    					osInfo += "os.version="+currentProp;
    			}
    			catch(SecurityException se){
    				return se.getMessage();
    			}
    			return osInfo;
    		}
    	});
    }
    
    /**
     * Return values of system properties that identify the JVM. 
     * Will catch SecurityExceptions and note them for displaying information.
     * @return the Java system property value from the JVM or a string capturing a
     * security exception.
     */
    private static String buildJvmVersion () {
//IC see: https://issues.apache.org/jira/browse/DERBY-6213
        return AccessController.doPrivileged( new PrivilegedAction<String>()
        {
           public String run()
           {      
             String jvmversion = "";
             try {
                 String currentProp  = PropertyUtil.getSystemProperty("java.vendor");
                 if ( currentProp != null)
                     jvmversion = "java.vendor=" + currentProp;
                 if ((currentProp = PropertyUtil.getSystemProperty("java.runtime.version")) != null)
                     jvmversion += "\njava.runtime.version=" + currentProp;
                 if ((currentProp = PropertyUtil.getSystemProperty("java.fullversion")) != null)
                     jvmversion += "\njava.fullversion=" + currentProp ;         
//IC see: https://issues.apache.org/jira/browse/DERBY-4853
                 if ((currentProp = PropertyUtil.getSystemProperty("user.dir")) != null)
                     jvmversion += "\nuser.dir=" + currentProp ;         
              }
              catch (SecurityException se) {
                   return se.getMessage();
              }
              return jvmversion;
            }
        });
    } // end of buildjvmVersion
        
	/**
		Returns the encryption block size used by the algorithm at time of
		creation of an encrypted database
	 */
	public int getEncryptionBlockSize()
	{
		return rawStoreFactory.getEncryptionBlockSize();
	}

	public String getVersionedName(String name, long generationId) 
    {
		return name.concat(".G".concat(Long.toString(generationId)));
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
        return(findMaxContainerId());
    }

	synchronized long getNextId() {
		return nextContainerId++;
	}

	/** return a secure random number */
	int random()
	{
		return databaseEncrypted ? rawStoreFactory.random() : 0;
	}

	/**
		Add a file to the list of files to be removed post recovery.
	*/
	void fileToRemove( StorageFile file, boolean remove) 
    {
		if (postRecoveryRemovedFiles == null)
//IC see: https://issues.apache.org/jira/browse/DERBY-6213
			postRecoveryRemovedFiles = new Hashtable<String,StorageFile>();
        String path = null;
        synchronized( this)
        {
            actionCode = GET_PATH_ACTION;
            actionFile = file;
            try
            {
                path = (String) AccessController.doPrivileged( this);
            }
            catch (PrivilegedActionException pae) 
            {
                // GET_PATH does not throw an exception
            } 
            finally
            {
                actionFile = null;
            }
        }
		if (remove)  // to be removed
			postRecoveryRemovedFiles.put(path, file);
		else
			postRecoveryRemovedFiles.remove(path);
	
	}

	/**
		Called after recovery is performed.

		@exception StandardException Standard Derby Error Policy
	*/
	public void postRecovery() throws StandardException 
    {

		DaemonService daemon = rawStoreFactory.getDaemon();

		if (daemon == null)
			return;

		if (postRecoveryRemovedFiles != null) 
        {
            synchronized( this)
            {
                actionCode = POST_RECOVERY_REMOVE_ACTION;
                try
                {
                    AccessController.doPrivileged( this);
                }
                catch (PrivilegedActionException pae)
                {
                    // POST_RECOVERY_REMOVE does not throw an exception
                }
            }
			postRecoveryRemovedFiles = null;
		}
	}

    /**
     * Set up the cache cleaner for the container cache and the page cache.
     */
    public void setupCacheCleaner(DaemonService daemon) {
//IC see: https://issues.apache.org/jira/browse/DERBY-3131
        containerCache.useDaemonService(daemon);
        pageCache.useDaemonService(daemon);
    }

	public void freezePersistentStore() throws StandardException
	{
		synchronized(freezeSemaphore)
		{
			if (isFrozen)
            {
				throw StandardException.newException(
                        SQLState.RAWSTORE_NESTED_FREEZE);
            }

			// set this to true first to stop all writes from starting after
			// this.
			isFrozen = true;

			// wait for all in progress write to finish
			try
			{
				while(writersInProgress > 0)
				{
					try
					{
						freezeSemaphore.wait();
					}
					catch (InterruptedException ie) 
					{
                        InterruptStatus.setInterrupted();
					}
				}
			}
			catch (RuntimeException rte)
			{
				// make sure we are not stuck in frozen state if we
				// caught a run time exception and the calling thread may not
				// have a chance to call unfreeze
				isFrozen = false;
				freezeSemaphore.notifyAll();
				throw rte;		// rethrow run time exception
			}

			if (SanityManager.DEBUG)
				SanityManager.ASSERT(writersInProgress == 0 && 
									 isFrozen == true,
									 "data store is not properly frozen");
		}
	}

	public void unfreezePersistentStore() 
	{
		synchronized(freezeSemaphore)
		{
			isFrozen = false;
			freezeSemaphore.notifyAll();
		}
	}

	public void writeInProgress() throws StandardException
	{
		synchronized(freezeSemaphore)
		{
			// do not start write, the persistent store is frozen
			while(isFrozen)
			{
				try
				{
					freezeSemaphore.wait();
				}
				catch (InterruptedException ie)
				{
//IC see: https://issues.apache.org/jira/browse/DERBY-4741
//IC see: https://issues.apache.org/jira/browse/DERBY-4741
                    InterruptStatus.setInterrupted();
				}
			}

			// store is not frozen, proceed to write - do this last
			writersInProgress++;
		}
	}
	
	public void writeFinished() 
	{
		synchronized(freezeSemaphore)
		{
			if (SanityManager.DEBUG)
				SanityManager.ASSERT(writersInProgress > 0, 
									 "no writers in progress"); 

			writersInProgress--;
			freezeSemaphore.notifyAll(); // wake up the freezer
		}
	}


	/*
	 *  Find all the all the containers stored in the seg0 directory and 
	 *  backup each container to the specified backup location.
	 */
	public void backupDataFiles(Transaction rt, File backupDir) throws StandardException
	{
				
		/*
		 * List of containers that needs to be backed up are identified by 
		 * simply reading the list of files in seg0. 
		 * All container that are created after the container list is created 
		 * when backup is in progress are recreated on restore using the
		 * transaction log.
		 */

//IC see: https://issues.apache.org/jira/browse/DERBY-239
		String[] files = getContainerNames();
		
		if (files != null) {
			// No user visible locks are acquired to backup the database. A stable backup 
			// is made by latching the pages and internal synchronization
			// mechanisms.
			LockingPolicy lockPolicy = 	rt.newLockingPolicy(LockingPolicy.MODE_NONE, 
															TransactionController.ISOLATION_NOLOCK, 
															false);
			long segmentId = 0;

			// loop through all the files in seg0 and backup all valid containers.
			for (int f = files.length-1; f >= 0 ; f--) {
				long containerId;
				try	{
					containerId = 
						Long.parseLong(files[f].substring(1, (files[f].length() -4)), 16);
				}
				catch (Throwable t)
				{
					// ignore errors from parse, it just means that someone put
					// a file in seg0 that we didn't expect.  Continue with the
					// next one.
					continue;
				}

				ContainerKey identity = new ContainerKey(segmentId, containerId);

				/* Not necessary to get the container thru the transaction.
				 * Backup opens in container in read only mode , No need to 
				 * transition the transaction to active state. 
				 * 
				 *  dropped container stubs also has to be backed up 
				 *  for restore to work correctly. That is 
				 *  why we are using a open call that let us
				 *  open dropped containers.
				 */

				ContainerHandle containerHdl = openDroppedContainer((RawTransaction)rt, 
																	identity, lockPolicy, 
																	ContainerHandle.MODE_READONLY);
				/*
				 * Note 1:
				 * If a container creation is  in progress , open call will wait 
				 * until it is complete; It will never return a handle to a 
				 * container that is partially created. (see cache manager code
				 * for more details)
				 *
				 * Note 2: 
				 * if a container creation failed in the middle after the list 
				 * of the names are read from seg0, it will not exist in
				 * the database any more, so nothing to backup.  Attempt 
				 * to open such container will return null.
				 * 
				 */

				if( containerHdl !=  null) {
					containerHdl.backupContainer(backupDir.getPath());
					containerHdl.close();
				}
			}
		} else
		{
			if (SanityManager.DEBUG) 
				SanityManager.THROWASSERT("backup process is unable to read container names in seg0");
		}
	}

	/**
     * get all the names of the files in seg 0.
     * MT - This method needs to be synchronized to avoid conflicts 
     * with other privileged actions execution in this class.
     * @return An array of all the file names in seg0.
     **/
//IC see: https://issues.apache.org/jira/browse/DERBY-1156
    synchronized String[] getContainerNames()
	{
        actionCode = GET_CONTAINER_NAMES_ACTION;
        try{
            return (String[]) AccessController.doPrivileged( this);
        }
        catch( PrivilegedActionException pae){ return null;}
	}



	/**
	 * removes the data directory(seg*) from database home directory and
	 * restores it from backup location.
	 * This function gets called only when any of the folling attributes
	 * are specified on connection URL:
	 * Attribute.CREATE_FROM (Create database from backup if it does not exist)
	 * Attribute.RESTORE_FROM (Delete the whole database if it exists and 
     *     then restore * it from backup)
	 * Attribute.ROLL_FORWARD_RECOVERY_FROM:(Perform Rollforward Recovery;
	 * except for the log directory everthing else is replced  by the copy  from
	 * backup. log files in the backup are copied to the existing online log 
     * directory.
	 *
	 * In all the cases, data directory(seg*) is replaced by the data directory
	 * directory from backup when this function is called.
	 */
	private void restoreDataDirectory(String backupPath) 
        throws StandardException
	{
        // Root dir of backup db
        final File backupRoot = new java.io.File(backupPath);		

        /* To be safe we first check if the backup directory exist and it has
         * atleast one seg* directory before removing the current data directory.
         *
         * This will fail with a security exception unless the database engine 
         * and all its callers have permission to read the backup directory.
         */
//IC see: https://issues.apache.org/jira/browse/DERBY-6213
        String[] bfilelist = AccessController.doPrivileged(
                                            new PrivilegedAction<String[]>() {
                                                public String[] run() {
                                                    return backupRoot.list();
                                                }
                                            });
        if(bfilelist !=null)
        {
            boolean segmentexist = false;
            for (int i = 0; i < bfilelist.length; i++) 
            {
                //check if it is a  seg* directory
                if(bfilelist[i].startsWith("seg"))
                {
                    // Segment directory in the backup
                    final File bsegdir = new File(backupRoot , bfilelist[i]);
                    boolean bsegdirExists = (
                            AccessController.doPrivileged(
//IC see: https://issues.apache.org/jira/browse/DERBY-6213
                                new PrivilegedAction<Boolean>() {
                                    public Boolean run() {
//IC see: https://issues.apache.org/jira/browse/DERBY-5641
                                        return Boolean.valueOf(bsegdir.exists());
                                    }
                            })).booleanValue();
                    if (bsegdirExists) {
                        // Make sure the file object points at a directory.
                        boolean isDirectory = (
                            AccessController.doPrivileged(
                            new PrivilegedAction<Boolean>() {
                                public Boolean run() {
//IC see: https://issues.apache.org/jira/browse/DERBY-5641
                                    return Boolean.valueOf(bsegdir.isDirectory());
                                }
                            })).booleanValue();
                        if (isDirectory) {
                            segmentexist = true;
                            break;
                        }
                    }
                }
            }
		
            if(!segmentexist)
            {
                throw
                  StandardException.newException(
                      SQLState.DATA_DIRECTORY_NOT_FOUND_IN_BACKUP, backupRoot);
            }
        }
        else
        {
			
            throw StandardException.newException(
                    SQLState.DATA_DIRECTORY_NOT_FOUND_IN_BACKUP, backupRoot);
        }

        synchronized (this)
        {
            actionCode = RESTORE_DATA_DIRECTORY_ACTION;
            this.backupRoot = backupRoot;
            this.bfilelist = bfilelist;
            try
            {
                AccessController.doPrivileged( this);
            }
            catch (PrivilegedActionException pae)
            { 
                throw (StandardException) pae.getException();
            }
            finally
            {
                this.backupRoot = null;
                this.bfilelist = null;
            }
        }
    }

    private void privRestoreDataDirectory() throws StandardException
    {
        StorageFile csegdir;	//segment directory in the current db home
        StorageFile dataRoot = 
            storageFactory.newStorageFile( null); //root dir of db

        //Remove the seg* directories in the current database home directory
        String[] cfilelist = dataRoot.list();
        if(cfilelist!=null)
        {
            for (int i = 0; i < cfilelist.length; i++) 
            {
                //delete only the seg* directories in the database home
//IC see: https://issues.apache.org/jira/browse/DERBY-590
                if(cfilelist[i].startsWith("seg") || Database.LUCENE_DIR.equals( cfilelist[i] ))
                {
                    csegdir = storageFactory.newStorageFile( cfilelist[i]);
                    if(!csegdir.deleteAll())
                    {
                        throw
                          StandardException.newException(
                              SQLState.UNABLE_TO_REMOVE_DATA_DIRECTORY, 
                              csegdir);
                    }
                }
            }
        }

        //copy the seg* directories from backup to current database home
        for (int i = 0; i < bfilelist.length; i++) 
        {
            //copy only the seg* directories and copy them from backup
//IC see: https://issues.apache.org/jira/browse/DERBY-590
            if (bfilelist[i].startsWith("seg") || Database.LUCENE_DIR.equals( bfilelist[i] ))
            {
                csegdir = storageFactory.newStorageFile( bfilelist[i]);
                File bsegdir1 = new java.io.File(backupRoot, bfilelist[i]);
                if (!FileUtil.copyDirectory( 
                        writableStorageFactory, bsegdir1, csegdir))
                {
                    throw
                      StandardException.newException(
                          SQLState.UNABLE_TO_COPY_DATA_DIRECTORY, 
                          bsegdir1, csegdir);
                }
            }
            else if (databaseEncrypted && 
                     bfilelist[i].startsWith(
                         Attribute.CRYPTO_EXTERNAL_KEY_VERIFY_FILE))
            {
                // Case of encrypted database and usage of an external 
                // encryption key, there is an extra file with name given by 
                // Attribute.CRYPTO_EXTERNAL_KEY_VERIFY_FILE that needs to be
                // copied over during createFrom/restore operations.

                //copy the file
                File        fromFile = new File(backupRoot,bfilelist[i]);
                StorageFile toFile   = 
                    storageFactory.newStorageFile(bfilelist[i]);

                if (!FileUtil.copyFile(writableStorageFactory,fromFile,toFile))
                {
                    throw StandardException.newException(
                            SQLState.UNABLE_TO_COPY_DATA_DIRECTORY, 
                            bfilelist[i], toFile);
                }
            }
        }

    } // end of privRestoreDataDirectory

	/**
		Is the store read-only.
	*/
	public boolean isReadOnly() 
    {
		// return what the baseDataFileFactory thinks
		return readOnly;
	}

    /** Return true if the Lucene plugin is loaded */
    public  boolean luceneLoaded()
//IC see: https://issues.apache.org/jira/browse/DERBY-590
        throws StandardException
    {
        try {
            return AccessController.doPrivileged
                (
                 new PrivilegedExceptionAction<Boolean>()
                 {
                     public Boolean run()
                     {
                         StorageFactory storageFactory = getStorageFactory();
                         StorageFile luceneDir = storageFactory.newStorageFile( Database.LUCENE_DIR );

                         return luceneDir.exists();
                     }
                 }
                 ).booleanValue();
        }
        catch (PrivilegedActionException pae) { throw StandardException.plainWrapException( pae ); }
    }

    /**
     * @return The StorageFactory used by this dataFactory
     */
    public StorageFactory getStorageFactory()
    {
        return storageFactory;
    }

    // PrivilegedExceptionAction method
    public final Object run() throws IOException, StandardException
    {
        switch( actionCode)
        {
        case BOOT_ACTION:
            readOnly = storageFactory.isReadOnlyDatabase();
            supportsRandomAccess = storageFactory.supportsRandomAccess();
            return null;
            
        case REMOVE_TEMP_DIRECTORY_ACTION:
            StorageFile tempDir = storageFactory.getTempDir();
            if( tempDir != null)
                tempDir.deleteAll();
            return null;

        case GET_CONTAINER_PATH_ACTION:
        case GET_ALTERNATE_CONTAINER_PATH_ACTION:
        {
            StringBuffer sb = new StringBuffer("seg");
            sb.append(containerId.getSegmentId());
            sb.append(storageFactory.getSeparator());
            if( actionCode == GET_CONTAINER_PATH_ACTION)
            {
                sb.append(stub ? 'd' : 'c');
                sb.append(Long.toHexString(containerId.getContainerId()));
                sb.append(".dat");
            }
            else
            {
                sb.append(stub ? 'D' : 'C');
                sb.append(Long.toHexString(containerId.getContainerId()));
                sb.append(".DAT");
            }
            return storageFactory.newStorageFile( sb.toString());
        } // end of cases GET_CONTAINER_PATH_ACTION & GET_ALTERNATE_CONTAINER_PATH_ACTION

        case REMOVE_STUBS_ACTION:
        {
            char separator = storageFactory.getSeparator();
            StorageFile root = storageFactory.newStorageFile( null);

            // get all the non-temporary data segment, they start with "seg"
            String[] segs = root.list();
            for (int s = segs.length-1; s >= 0; s--)
            {
                if (segs[s].startsWith("seg"))
                {
                    StorageFile seg = 
                        storageFactory.newStorageFile(root, segs[s]);

                    if (seg.exists() && seg.isDirectory())
                    {
                        String[] files = seg.list();
                        for (int f = files.length-1; f >= 0 ; f--)
                        {
                            // stub
                            if (files[f].startsWith("D") ||
                                files[f].startsWith("d"))
                            {
                                StorageFile stub = 
                                    storageFactory.newStorageFile(
                                        root, segs[s] + separator + files[f]);

                                boolean delete_status = stub.delete();
                                
                                if (SanityManager.DEBUG)
                                {
                                    // delete should always work, code which
                                    // created the StorageFactory already 
                                    // checked for existence.
                                    if (!delete_status)
                                    {
                                        SanityManager.THROWASSERT(
                                            "delete of stub (" + 
                                            stub + ") failed.");
                                    }
                                }
                            }
                        }
                    }
                }
            }
            break;
        } // end of case REMOVE_STUBS_ACTION

        case FIND_MAX_CONTAINER_ID_ACTION:
        {
            long maxnum = 1;
            StorageFile seg = storageFactory.newStorageFile( "seg0");

            if (seg.exists() && seg.isDirectory())
            {
                // create an array with names of all files in seg0
                String[] files = seg.list();

                // loop through array looking for maximum containerid.
                for (int f = files.length-1; f >= 0 ; f--)
                {
                    try
                    {
                        long fileNumber = 
                          Long.parseLong(
                              files[f].substring(
                                  1, (files[f].length() -4)), 16);

                        if (fileNumber > maxnum)
                            maxnum = fileNumber;
                    }
                    catch (Throwable t)
                    {
                        // ignore errors from parse, it just means that someone 
                        // put a file in seg0 that we didn't expect.  Continue 
                        // with the next one.
                    }
                }
            }
//IC see: https://issues.apache.org/jira/browse/DERBY-6885
            return maxnum;
		} // end of case FIND_MAX_CONTAINER_ID_ACTION

        case DELETE_IF_EXISTS_ACTION:
        {
            boolean ret = actionFile.exists() && actionFile.delete();
            actionFile = null;
            return ret ? this : null;
        } // end of case DELETE_IF_EXISTS_ACTION

        case GET_PATH_ACTION:
        {
            String path = actionFile.getPath();
            actionFile = null;
            return path;
        } // end of case GET_PATH_ACTION

        case POST_RECOVERY_REMOVE_ACTION:
        {
//IC see: https://issues.apache.org/jira/browse/DERBY-6213
			for (Enumeration<StorageFile> e = postRecoveryRemovedFiles.elements(); 
                    e.hasMoreElements(); )
            {
				StorageFile f = e.nextElement();
				if (f.exists())
                {
					boolean delete_status = f.delete();

                    if (SanityManager.DEBUG)
                    {
                        // delete should always work, code which
                        // created the StorageFactory already 
                        // checked for existence.
                        if (!delete_status)
                        {
                            SanityManager.THROWASSERT(
                                "delete of stub (" + stub + ") failed.");
                        }
                    }
                }
			}
            return null;
        }

        case GET_LOCK_ON_DB_ACTION:
            privGetJBMSLockOnDB();
            return null;

        case RELEASE_LOCK_ON_DB_ACTION:
            privReleaseJBMSLockOnDB();
            return null;

        case RESTORE_DATA_DIRECTORY_ACTION:
            privRestoreDataDirectory();
            return null;
//IC see: https://issues.apache.org/jira/browse/DERBY-239
		case GET_CONTAINER_NAMES_ACTION:
        {
            StorageFile seg = storageFactory.newStorageFile( "seg0");
            if (seg.exists() && seg.isDirectory())
            {
                // return the  names of all files in seg0
				return seg.list();
            }
            return null;
        }  // end of case GET_CONTAINER_NAMES_ACTION
		
		}
        return null;
    } // end of run
    
    /**
     * Privileged Monitor lookup. Must be private so that user code
     * can't call this entry point.
     */
    private  static  ModuleFactory  getMonitor()
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-6648
        return AccessController.doPrivileged
            (
             new PrivilegedAction<ModuleFactory>()
             {
                 public ModuleFactory run()
                 {
                     return Monitor.getMonitor();
                 }
             }
             );
    }

    
    /**
     * Privileged startup. Must be private so that user code
     * can't call this entry point.
     */
    private  static  Object  startSystemModule( final String factoryInterface )
        throws StandardException
    {
        try {
            return AccessController.doPrivileged
                (
                 new PrivilegedExceptionAction<Object>()
                 {
                     public Object run()
                         throws StandardException
                     {
                         return Monitor.startSystemModule( factoryInterface );
                     }
                 }
                 );
        } catch (PrivilegedActionException pae)
        {
            throw StandardException.plainWrapException( pae );
        }
    }

    /**
     * Privileged startup. Must be private so that user code
     * can't call this entry point.
     */
    private  static  Object bootServiceModule
        (
         final boolean create, final Object serviceModule,
         final String factoryInterface, final Properties properties
         )
        throws StandardException
    {
        try {
            return AccessController.doPrivileged
                (
                 new PrivilegedExceptionAction<Object>()
                 {
                     public Object run()
                         throws StandardException
                     {
                         return Monitor.bootServiceModule( create, serviceModule, factoryInterface, properties );
                     }
                 }
                 );
        } catch (PrivilegedActionException pae)
        {
            throw StandardException.plainWrapException( pae );
        }
    }

}
