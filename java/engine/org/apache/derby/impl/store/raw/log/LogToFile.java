/*

   Derby - Class org.apache.derby.impl.store.raw.log.LogToFile

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

package org.apache.derby.impl.store.raw.log;

import org.apache.derby.iapi.services.diag.Performance;

import org.apache.derby.iapi.services.info.ProductVersionHolder;

import org.apache.derby.shared.common.reference.MessageId;
import org.apache.derby.iapi.reference.Property;
import org.apache.derby.shared.common.reference.SQLState;

import org.apache.derby.iapi.services.daemon.DaemonService;
import org.apache.derby.iapi.services.daemon.Serviceable;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.services.context.ContextService;
import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.iapi.services.monitor.ModuleControl;
import org.apache.derby.iapi.services.monitor.ModuleFactory;
import org.apache.derby.iapi.services.monitor.ModuleSupportable;
import org.apache.derby.iapi.services.monitor.PersistentService;
import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derby.iapi.services.io.Formatable;
import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.iapi.error.ErrorStringBuilder;
import org.apache.derby.shared.common.error.ShutdownException;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.i18n.MessageService;
import org.apache.derby.iapi.store.access.AccessFactory;
import org.apache.derby.iapi.store.access.AccessFactoryGlobals;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.iapi.store.raw.Loggable;
import org.apache.derby.iapi.store.raw.RawStoreFactory;
import org.apache.derby.iapi.store.raw.ScanHandle;
import org.apache.derby.iapi.store.raw.log.LogFactory;
import org.apache.derby.iapi.store.raw.log.Logger;
import org.apache.derby.iapi.store.raw.log.LogInstant;
import org.apache.derby.iapi.store.raw.log.LogScan;
import org.apache.derby.iapi.store.raw.xact.RawTransaction;
import org.apache.derby.iapi.store.raw.xact.TransactionFactory;
import org.apache.derby.iapi.store.raw.data.DataFactory;
import org.apache.derby.iapi.services.property.PersistentSet;

//for replication
import org.apache.derby.iapi.store.replication.master.MasterFactory;
import org.apache.derby.iapi.store.replication.slave.SlaveFactory;
import org.apache.derby.iapi.services.io.ArrayInputStream;

import org.apache.derby.iapi.store.access.DatabaseInstant;
import org.apache.derby.iapi.services.property.PropertyUtil;
import org.apache.derby.shared.common.reference.Attribute;
import org.apache.derby.iapi.services.io.FileUtil;

import org.apache.derby.io.WritableStorageFactory;
import org.apache.derby.io.StorageFile;
import org.apache.derby.io.StorageRandomAccessFile;

import org.apache.derby.iapi.util.InterruptStatus;

import java.io.File; // Plain files are used for backups
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.SyncFailedException;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.FileNotFoundException;

import java.net.MalformedURLException;
import java.net.URL;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;
import java.security.PrivilegedActionException;

import java.util.Properties;
import java.util.zip.CRC32;

/**

	This is an implementation of the log using a non-circular file system file.
	No support for incremental log backup or media recovery.
	Only crash recovery is supported.  
	<P>
	The 'log' is a stream of log records.  The 'log' is implemented as
	a series of numbered log files.  These numbered log files are logically
	continuous so a transaction can have log records that span multiple log files.
	A single log record cannot span more then one log file.  The log file number
	is monotonically increasing.
	<P>
	The log belongs to a log factory of a RawStore.  In the current implementation,
	each RawStore only has one log factory, so each RawStore only has one log
	(which composed of multiple log files).
	At any given time, a log factory only writes new log records to one log file,
	this log file is called the 'current log file'.
	<P>
	A log file is named log<em>logNumber</em>.dat
	<P>
	Everytime a checkpoint is taken, a new log file is created and all subsequent
	log records will go to the new log file.  After a checkpoint is taken, old
	and useless log files will be deleted.
	<P>
	RawStore exposes a checkpoint method which clients can call, or a checkpoint is
	taken automatically by the RawStore when
	<OL>
	<LI> the log file grows beyond a certain size (configurable, default 100K bytes)
	<LI> RawStore is shutdown and a checkpoint hasn't been done "for a while"
	<LI> RawStore is recovered and a checkpoint hasn't been done "for a while"
	</OL>
	<P>
	This LogFactory is responsible for the formats of 2 kinds of file: the log
	file and the log control file.  And it is responsible for the format of the
	log record wrapper.
	<P> <PRE>

	Format of log control file 

	@derby.formatId	FILE_STREAM_LOG_FILE
	@derby.purpose	The log control file contains information about which log files
	are present and where the last checkpoint log record is located.
	@derby.upgrade	
	@derby.diskLayout
		int format id
		int obsolete log file version
		long the log instant (LogCounter) of the last completed checkpoint
		   (logfile counter, position)
		int Derby major version
		int Derby minor version
		int subversion revision/build number
		byte Flags (beta flag (0 or 1), test durability flag (0 or 1))
		byte spare (0)
		byte spare (0)
		byte spare (0)
		long spare (value set to 0)
		long checksum for control data written

	@derby.endFormat
	</PRE>	
	<HR WIDTH="100%">
	<PRE>

	Format of the log file

	@derby.formatId	FILE_STREAM_LOG_FILE
	@derby.purpose	The log file contains log record which record all the changes
	to the database.  The complete transaction log is composed of a series of
	log files.
	@derby.upgrade
	@derby.diskLayout
		int format id - 	the format Id of this log file
		int obsolete log file version - not used
		long log file number - this number orders the log files in a
						series to form the complete transaction log
		long prevLogRecord - log instant of the previous log record, in the
				previous log file. 

		[log record wrapper]* one or more log records with wrapper

		int endMarker - value of zero.  The beginning of a log record wrapper
				is the length of the log record, therefore it is never zero
		[int fuzzy end]* zero or more int's of value 0, in case this log file
				has been recovered and any incomplete log record set to zero.
	@derby.endFormat
	</PRE>	
	<HR WIDTH="100%">
	<PRE>

	Format of the log record wrapper

	@derby.formatId none.  The format is implied by the FILE_STREAM_LOG_FILE
	@derby.purpose	The log record wrapper provides information for the log scan.
	@derby.upgrade
	@derby.diskLayout
		length(int) length of the log record (for forward scan)
		instant(long) LogInstant of the log record
		logRecord(byte[length]) byte array that is written by the FileLogger
		length(int) length of the log record (for backward scan)
	@derby.endFormat
	</PRE>


	<P>Multithreading considerations:<BR>
	Log Factory must be MT-safe.
	<P>
	Class is final as it has methods with privilege blocks
	and implements PrivilegedExceptionAction.
	*/

public final class LogToFile implements LogFactory, ModuleControl, ModuleSupportable,
								  Serviceable, java.security.PrivilegedExceptionAction<Object>
{

	private static final    long INT_LENGTH = 4L;

	private static int fid = StoredFormatIds.FILE_STREAM_LOG_FILE; 

	// format Id must fit in 4 bytes
 
	/**
		Return my format identifier.
	*/
	public int getTypeFormatId() {
		return StoredFormatIds.FILE_STREAM_LOG_FILE;
	}

	// at the beginning of every log file is the following information:
	// the log file formatId
	// the log file version (int)
	// the log file number (long)
	// the log instant at the end of the last log record in the previous file (long)
	public static final int LOG_FILE_HEADER_SIZE = 24;

	protected static final int LOG_FILE_HEADER_PREVIOUS_LOG_INSTANT_OFFSET = LOG_FILE_HEADER_SIZE-8;

	// Number of bytes overhead of each log record.
	// 4 bytes of length at the beginning, 8 bytes of log instant,
	// and 4 bytes of length at the end
	public static final int LOG_RECORD_OVERHEAD = 4+8+4;

	public static final String DBG_FLAG = SanityManager.DEBUG ? "LogTrace" : null;
	public static final String DUMP_LOG_ONLY = SanityManager.DEBUG ? "DumpLogOnly" : null;
	public static final String DUMP_LOG_FROM_LOG_FILE = 
		SanityManager.DEBUG ? "derby.storage.logDumpStart" : null;
	protected static final String LOG_SYNC_STATISTICS = "LogSyncStatistics";

	// If you change this number, then JBMS 1.1x and 1.2x will give a really
	// horrendous error message when booting against a db created by you.  When
	// we decided that we don't need to worry about people mis-using the
	// product that way, then we can change this.  Just remember, before we do,
	// all existing database will have the number 9 in there.
	private static final int OBSOLETE_LOG_VERSION_NUMBER = 9;

	/* how big the log file should be before checkpoint or log switch is taken */
	private static final int DEFAULT_LOG_SWITCH_INTERVAL = 1024*1024;		
	private static final int LOG_SWITCH_INTERVAL_MIN     = 100000;
	private static final int LOG_SWITCH_INTERVAL_MAX     = 128*1024*1024;
	private static final int CHECKPOINT_INTERVAL_MIN     = 100000;
	private static final int CHECKPOINT_INTERVAL_MAX     = 128*1024*1024;
	private static final int DEFAULT_CHECKPOINT_INTERVAL = 10*1024*1024;

	//log buffer size values
	private static final int DEFAULT_LOG_BUFFER_SIZE = 32768; //32K
	private static final int LOG_BUFFER_SIZE_MIN = 8192; //8k
	private static final int LOG_BUFFER_SIZE_MAX = LOG_SWITCH_INTERVAL_MAX;
	private int logBufferSize = DEFAULT_LOG_BUFFER_SIZE;

	/* Log Control file flags. */
	private static final byte IS_BETA_FLAG = 0x1;
	
    /**
     * When the derby.system.durability property is set to 'test', the store 
     * system will not force sync calls in the following cases
     * - for the log file at each commit
     * - for the log file before data page is forced to disk
     * - for page allocation when file is grown
     * - for data writes during checkpoint
     * This means it is possible that the recovery system may not work properly,
     * committed transactions may be lost, and/or database may not
     * be in a consistent state.
     * In order that we recognize this case that the database was previously
     * at any time booted in this mode, this value is written out
     * into the log control file. This will help prevent us from 
     * wasting time to resolve issues in such cases. 
     * @see org.apache.derby.iapi.reference.Property#DURABILITY_PROPERTY
     * This value is written as part of the log control file flags byte.
     */
    private static final byte IS_DURABILITY_TESTMODE_NO_SYNC_FLAG = 0x2;
	
    /**
     * keeps track of if the database was booted previously at any time with 
     * derby.system.durability=test
     */
    private static boolean wasDBInDurabilityTestModeNoSync = false;
    
	/* to err on the conservative side, unless otherwise set, assume log
	 *	archive is ON 
	 */
	private static final String DEFAULT_LOG_ARCHIVE_DIRECTORY = "DEFAULT";

	private int     logSwitchInterval   = DEFAULT_LOG_SWITCH_INTERVAL;
	private int     checkpointInterval  = DEFAULT_CHECKPOINT_INTERVAL;

	String dataDirectory; 					// where files are stored
    private WritableStorageFactory logStorageFactory;
    
	private boolean logBeingFlushed; // is the log in the middle of a flush
									 // (access of the variable should sync on this)

	protected LogAccessFile logOut;		// an output stream to the log file
								// (access of the variable should sync on this)
	private   StorageRandomAccessFile firstLog = null;
	protected long		     endPosition = -1; // end position of the current log file
	long					 lastFlush = 0;	// the position in the current log
											// file that has been flushed to disk

    // logFileNumber and bootTimeLogFileNumber:
    // ----------------------------------------
    // There are three usages of the log file number in this class:
    //
    //  1 Recover uses the log file number determined at boot-time to
    //    find which log file the redo pass should start to read.
    //  2 If the database is booted in slave replication mode, a slave
    //    thread will apply log records to the tail of the log.
    //    switchLogFile() allocates new log files when the current log
    //    file is full. logFileNumber needs to point to the log file
    //    with the highest number for switchLogFile() to work
    //    correctly.
    //  3 After the database has been fully booted, i.e. after boot()
    //    and recover(), and users are allowed to connect to the
    //    database, logFileNumber is used by switchLogFile() to
    //    allocate new log files when the current is full.
    //
    // Usage 2 and 3 are very similar. The only difference is that 1
    // and 2 are performed concurrently, whereas 3 is performed afterwards.
    // Because usage 1 and 2 are required in concurrent threads (when
    // booted in slave replication mode) that must not interfere, two
    // versions of the log file number are required:
    //
    // bootTimeLogFileNumber: Set to point to the log file with the
    //   latest checkpoint during boot(). This is done before the
    //   slave replication thread has started to apply log records
    //   received from the master. Used by recover() during the time
    //   interval in which slave replication mode may be active, i.e.
    //   during the redo pass. After the redo pass has completed,
    //   recover starts using logFileNumber (which will point to the
    //   highest log file when recovery has completed). 
    // logFileNumber: Used by recovery after the redo pass, and by
    //   switchLogFile (both in slave replication mode and after the
    //   database has been fully booted) when a new log file is
    //   allocated.
	long					 logFileNumber = -1; // current log file number.
								// Other than during boot and recovery time,
								// and during initializeReplicationSlaveRole if in
								// slave replication mode,
								// logFileNumber is only changed by
								// switchLogFile, which is synchronized.
								// 
								// MT - Anyone accessing this number should
								// synchronized on this if the current log file
								// must not be changed. If not synchronized,
								// the log file may have been switched.

    // Initially set to point to the log file with the latest
    // checkpoint (in boot()). Only used by recovery() after that
    long                     bootTimeLogFileNumber = -1;

	long					 firstLogFileNumber = -1;
								// first log file that makes up the active
								// portion (with active transactions) of the
								// log.  
								// 
								// MT - This value is set during recovery or
								// during log truncation.  In the former single
								// thread is assumed.  In the latter
								// must be synchronized with this to access
								// or change.
	
	private long              maxLogFileNumber = LogCounter.MAX_LOGFILE_NUMBER;
	private CheckpointOperation		 currentCheckpoint;
								// last checkpoint successfully taken
								// 
								// MT - only changed or access in recovery or
								// checkpoint, both are single thread access

	long					 checkpointInstant;
								// log instant of te curerntCheckpoint

	private DaemonService	 checkpointDaemon;	// the background worker thread who is going to
								// do checkpoints for this log factory.

	private	int			myClientNumber;	
								// use this number to talk to checkpoint Daemon

	private volatile boolean checkpointDaemonCalled; 
								// checkpoint Daemon called already - it is not
								// important that this value is correct, the
								// daemon just need to be called once in a
								// while.  Deamon can handle multiple posts.

	private long logWrittenFromLastCheckPoint = 0;
	                            // keeps track of the amout of log written between checkpoints
	private RawStoreFactory rawStoreFactory; 
	// use this only when in slave mode or after recovery is finished

	protected DataFactory dataFactory;
								// use this only after revocery is finished

	protected boolean	ReadOnlyDB;	// true if this db is read only, i.e, cannot
								// append log records

    // <START USED BY REPLICATION>
    // initialized if this Derby has the MASTER role for this database
    private MasterFactory masterFactory; 
    private boolean inReplicationMasterMode = false;

    // initialized if this Derby has the SLAVE role for this database
    private boolean inReplicationSlaveMode = false;
    /** If this exception is set while in replication slave mode, the 
     * exception will be thrown by the thread doing recovery will. 
     * Effectively, this will shut down the database. */
    private volatile StandardException replicationSlaveException = null;

    /** True if the database has been booted in replication slave pre
     * mode, effectively turning off writes to the log file.
     * @see SlaveFactory */
    private boolean inReplicationSlavePreMode = false;

    private Object slaveRecoveryMonitor; // for synchronization in slave mode

    // The highest log file number the recovery thread is allowed to
    // read while in slave replication mode. Remains -1 until the
    // first time switchLogFile is called. This call will only happen 
    // when the slave replication thread has applied log records
    // received from the master, which happens after slave replication
    // has been initialized. From that point on, it will have a value
    // of one less than logFileNumber.
    private long allowedToReadFileNumber = -1;
    // <STOP USED BY REPLICATION>

	// DEBUG DEBUG - do not truncate log files
	private boolean keepAllLogs;

	// if database is encrypted, the content of the log files are encrypted
	private boolean databaseEncrypted; 

	// the following booleans are used to put the log factory into various
	// states
	private boolean			 recoveryNeeded = true; // log needs to be recovered
	private boolean			 inCheckpoint = false; 	// in the middle of a checkpoint
	private boolean			 inRedo = false;        // in the middle of redo loop
	private boolean          inLogSwitch = false;

	// make sure we don't do anything after the log factory has been stopped
	private boolean			 stopped = false;

	// if log is to go to another device, this variable is set.  If null, then
	// log goes to the log subdirectory underneath the data directory
	String logDevice;

    // disable syncing of log file when running in derby.system.durability=test
    private boolean logNotSynced = false;

	private volatile boolean logArchived = false;
	private boolean logSwitchRequired = false;

	/** DEBUG test only */
	int test_logWritten = 0;
	int test_numRecordToFillLog = -1;
	private int mon_flushCalls;
	private int mon_syncCalls;
	private int mon_numLogFlushWaits;
	private boolean mon_LogSyncStatistics;
	private int mon_numBytesToLog;


	/**
		If not null then something is corrupt in the raw store and this represents the original error.
	*/
	protected volatile StandardException corrupt;

	/**
		If frozen, don't allow anything on disk to change.
	 */
	private boolean isFrozen;

	/**
	  Product Version information. Invarient after boot.
	  */
	ProductVersionHolder jbmsVersion;

	/**
		On disk database version information. When running in soft upgrade this version
		may be different to jbmsVersion.
	*/
	private int onDiskMajorVersion;
	private int onDiskMinorVersion;
	private boolean onDiskBeta;
	
	private CRC32 checksum = new CRC32(); // holder for the checksum

 	
	/**
	 * Note: Why logging system support file sync and write sync ?
	 * Note : The reason to support file and write sync of logs is 
	 * there was no support to do write sync until jdk1.4 and then
	 * there was write sync jvm bug in jdk1.4.1, only in jdk1.4.2 write 
	 * sync(rws and rwd modes) mechanism can be used correctly.
	 * Default in JVMS &gt;= jdk1.4.2 is write sync(see the boot method for jvm checks).
	 *
	 * Write sync mechanism support is added  for performance reasons. 
	 * On commits, logging system has to make sure the log for committed
	 * transaction is on disk. With out write  sync , log is written to the 
	 * disk and then fsync() is used on commits to make log is written to the 
	 * disk for sure. On most of the OS , fsync() calls are expensive. 
	 * On heavy commit oriented systems, file sync make the system run slow.
	 * This problem is solved by using write sync on preallocated log file. 
	 * write sync is much faster than doing write and file sync to a file. 
	 * File should be preallocated for write syncs to perform better than
	 * the file sync method. Whenever a new log file is created, 
	 * logSwitchInterval size is preallocated by writing zeros after file after the header. 
	 */

	/*If set to true , write sync will be used to do log write other file 
	 * level sync is used.
	 */
	private boolean isWriteSynced = false;

    /**
     * Status for whether the check on the sync error on some JVMs has been
     * done or not. See the checkJvmSyncError method for details.
     */
    private boolean jvmSyncErrorChecked = false;
    
    // log file that is yet to be copied to backup, updates to this variable 
    // needs to visible  checkpoint thread. 
	private volatile long logFileToBackup ; 
    // It is set to true when  online backup is in progress,  updates to 
    // this variable needs to visible to checkpoint thread. 
    private volatile boolean backupInProgress = false; 
   

	/**
		MT- not needed for constructor
	*/
	public LogToFile() {
		keepAllLogs = PropertyUtil.getSystemBoolean(RawStoreFactory.KEEP_TRANSACTION_LOG);
		

		if (Performance.MEASURE)
			mon_LogSyncStatistics = PropertyUtil.getSystemBoolean(LOG_SYNC_STATISTICS);
	}

	/*
	** Methods of Corruptable
	*/

	/**
     * Once the log factory is marked as corrupt then the raw store will
     * shut down.
	*/
	public StandardException markCorrupt(StandardException originalError) {

		boolean firsttime = false;

		synchronized (this) 
		{
			if (corrupt == null && originalError != null)
			{
				corrupt = originalError;
				firsttime = true;
			}
		}

		// only print the first error
		if (corrupt == originalError)
			logErrMsg(corrupt);


		// this is the first time someone detects error, shutdown the
		// system as much as possible without further damaging it
		if (firsttime)
		{
			synchronized(this)
			{
				stopped = true;

				if (logOut != null)
				{
					try
					{
						logOut.corrupt(); // get rid of open file descriptor
					}
					catch (IOException ioe)
					{ 
						// don't worry about it, just trying to clean up 
					}
				}

				// NullPointerException is preferred over corrupting the database
				logOut = null;
			}

			if (dataFactory != null)
				dataFactory.markCorrupt(null);

		}

		return originalError;
	}

	private void checkCorrupt() throws StandardException
	{
		synchronized (this) 
        {
			if (corrupt != null)
            {
				throw StandardException.newException(
                        SQLState.LOG_STORE_CORRUPT, corrupt);
            }
		}
	}

	/*
	** Methods of LogFactory
	*/

	/**
		MT- not needed
	*/
	public Logger getLogger() {

		if (ReadOnlyDB)
			return null;
		else
			return new FileLogger(this);
	}

	/**
		Make log factory aware of which raw store factory it belongs to
	*/
	public void setRawStoreFactory(RawStoreFactory rsf) {
		rawStoreFactory = rsf;
	}

	/**
		Recover the rawStore to a consistent state using the log.

		<P>
		In this implementation, the log is a stream of log records stored in
		one or more flat files.  Recovery is done in 2 passes: redo and undo.
		<BR> <B>Redo pass</B>
		<BR> In the redo pass, reconstruct the state of the rawstore by
		repeating exactly what happened before as recorded in the log.
		<BR><B>Undo pass</B>
		<BR> In the undo pass, all incomplete transactions are rolled back in
		the order from the most recently started to the oldest.

		<P>MT - synchronization provided by caller - RawStore boot.
		This method is guaranteed to be the only method being called and can
		assume single thread access on all fields.

		@see Loggable#needsRedo
		@see FileLogger#redo

		@exception StandardException Standard Derby error policy
	*/
	public void recover(
    DataFactory         df, 
    TransactionFactory  tf)
		 throws StandardException
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(df != null,  "data factory == null");
		}

		checkCorrupt();

		dataFactory     = df;
		
		// initialize the log writer only after the rawstorefactory is available, 
		// log writer requires encryption block size info from rawstore factory 
		// to encrypt checksum log records. 
		if (firstLog != null) 
			logOut = new LogAccessFile(this, firstLog, logBufferSize);

        // If booted in slave mode, the recovery thread is not allowed
        // to do any recovery work until the SlaveFactory tells the
        // thread it may do so. Effectively, the recovery thread is blocked 
        // here until allowedToReadFileNumber != -1. We cannot rely on the
        // inReplicationSlaveMode block in getLogFileAtBeginning
        // because that method is used to open consecutive log files,
        // but not the first one. While the recovery thread waits
        // here, the slave replication thread can perform necessary
        // initialization without causing serialization conflicts.
        if (inReplicationSlaveMode) {
            synchronized (slaveRecoveryMonitor) {
                // Recheck inReplicationSlaveMode==true every time
                // because slave replication may have been stopped
                // while this thread waited on the monitor
                while (inReplicationSlaveMode &&
                       (allowedToReadFileNumber<bootTimeLogFileNumber)) {
                    // Wait until the first log file can be read.
                    if (replicationSlaveException != null) {
                        throw replicationSlaveException;
                    }
                    try {
                        slaveRecoveryMonitor.wait();
                    } catch (InterruptedException ie) {
                        InterruptStatus.setInterrupted();
                    }
                }
            }
        }

		// we don't want to set ReadOnlyDB before recovery has a chance to look
		// at the latest checkpoint and determine that the database is shutdown
		// cleanly.  If the medium is read only but there are logs that need
		// to be redone or in flight transactions, we are hosed.  The logs that
        // are redone will leave dirty pages in the cache.

		if (recoveryNeeded)
		{
			try
			{
				/////////////////////////////////////////////////////////////
				//
				// During boot time, the log control file is accessed and
				// bootTimeLogFileNumber is determined.  LogOut is not set up.
				// bootTimeLogFileNumber is the log file the latest checkpoint
				// lives in,
				// or 1.  It may not be the latest log file (the system may have
				// crashed between the time a new log was generated and the
				// checkpoint log written), that can only be determined at the
				// end of recovery redo.
				//
				/////////////////////////////////////////////////////////////
			
				FileLogger logger = (FileLogger)getLogger();

				/////////////////////////////////////////////////////////////
				//
				// try to find the most recent checkpoint 
				//
				/////////////////////////////////////////////////////////////
				if (checkpointInstant != LogCounter.INVALID_LOG_INSTANT)
                {
					currentCheckpoint = 
                        findCheckpoint(checkpointInstant, logger);
                }

				// if we are only interested in dumping the log, start from the
				// beginning of the first log file
				if (SanityManager.DEBUG)
				{
					if (SanityManager.DEBUG_ON(DUMP_LOG_ONLY))
					{
						currentCheckpoint = null;

						System.out.println("Dump log only");

						// unless otherwise specified, 1st log file starts at 1
						String beginLogFileNumber =
							PropertyUtil.getSystemProperty(
                                DUMP_LOG_FROM_LOG_FILE);

						if (beginLogFileNumber != null)
                        {
							bootTimeLogFileNumber = 
                                Long.valueOf(beginLogFileNumber).longValue();
                        }
						else
                        {
							bootTimeLogFileNumber = 1;
                        }
					}
				}

				if (SanityManager.DEBUG)
				{
					if (SanityManager.DEBUG_ON("setCheckpoint"))
					{
						currentCheckpoint = null;

						System.out.println("Set Checkpoint.");

						// unless otherwise specified, 1st log file starts at 1
						String checkpointStartLogStr =
							PropertyUtil.getSystemProperty(
                                "derby.storage.checkpointStartLog");

						String checkpointStartOffsetStr =
							PropertyUtil.getSystemProperty(
                                "derby.storage.checkpointStartOffset");


						if ((checkpointStartLogStr != null) && 
                            (checkpointStartOffsetStr != null))
                        {
							checkpointInstant = 
                                LogCounter.makeLogInstantAsLong(
                                    Long.valueOf(checkpointStartLogStr).longValue(),
                                    Long.valueOf(checkpointStartOffsetStr).longValue());
                        }
                        else
                        {
                            SanityManager.THROWASSERT(
                                "must set derby.storage.checkpointStartLog and derby.storage.checkpointStartOffset, if setting setCheckpoint.");
                        }

                        currentCheckpoint = 
                            findCheckpoint(checkpointInstant, logger);
					}
				}

				long redoLWM     = LogCounter.INVALID_LOG_INSTANT;
				long undoLWM     = LogCounter.INVALID_LOG_INSTANT;
				long ttabInstant = LogCounter.INVALID_LOG_INSTANT;

				StreamLogScan redoScan = null;
				if (currentCheckpoint != null)
				{	
					Formatable transactionTable = null;

					// RESOLVE: sku 
					// currentCheckpoint.getTransactionTable();

					// need to set the transaction table before the undo
					tf.useTransactionTable(transactionTable);

					redoLWM = currentCheckpoint.redoLWM();
					undoLWM = currentCheckpoint.undoLWM();

					if (transactionTable != null)
						ttabInstant = checkpointInstant;

					if (SanityManager.DEBUG)
					{
						if (SanityManager.DEBUG_ON(DBG_FLAG))
						{
							SanityManager.DEBUG(DBG_FLAG, 
                                "Found checkpoint at " +
                                LogCounter.toDebugString(checkpointInstant) + 
                                " " + currentCheckpoint.toString());
						}
					}

					firstLogFileNumber = LogCounter.getLogFileNumber(redoLWM);

					// figure out where the first interesting log file is.
					if (LogCounter.getLogFileNumber(undoLWM) < 
                            firstLogFileNumber)
                    {
						firstLogFileNumber = 
                            LogCounter.getLogFileNumber(undoLWM);
                    }


					// if the checkpoint record doesn't have a transaction
					// table, we need to rebuild it by scanning the log from
					// the undoLWM.  If it does have a transaction table, we
					// only need to scan the log from the redoLWM

					redoScan = (StreamLogScan) 
                        openForwardsScan(undoLWM, (LogInstant)null);

				}
				else
				{
					// no checkpoint
					tf.useTransactionTable((Formatable)null);

					long start = 
						LogCounter.makeLogInstantAsLong(
                            bootTimeLogFileNumber, LOG_FILE_HEADER_SIZE);

					// no checkpoint, start redo from the beginning of the 
                    // file - assume this is the first log file
					firstLogFileNumber = bootTimeLogFileNumber;

					redoScan = (StreamLogScan) 
                        openForwardsScan(start, (LogInstant)null);
				}

				// open a transaction that is used for redo and rollback
				RawTransaction recoveryTransaction =
                    tf.startTransaction(
                        rawStoreFactory,
                        getContextService().getCurrentContextManager(),
                        AccessFactoryGlobals.USER_TRANS_NAME);

				// make this transaction aware that it is a recovery transaction
				// and don't spew forth post commit work while replaying the log
				recoveryTransaction.recoveryTransaction();

				/////////////////////////////////////////////////////////////
				//
				//  Redo loop - in FileLogger
				//
				/////////////////////////////////////////////////////////////

				// 
				// set log factory state to inRedo so that if redo caused any
				// dirty page to be written from the cache, it won't flush the
				// log since the end of the log has not been determined and we
				// know the log record that caused the page to change has
				// already been written to the log.  We need the page write to
				// go thru the log factory because if the redo has a problem,
				// the log factory is corrupt and the only way we know not to
				// write out the page in a checkpoint is if it check with the
				// log factory, and that is done via a flush - we use the WAL
				// protocol to stop corrupt pages from writing to the disk.
				//
				inRedo = true;	

				long logEnd = 
                    logger.redo(
                        recoveryTransaction, tf, redoScan, redoLWM, 
                        ttabInstant);

				inRedo = false;
				

                // Replication slave: When recovery has completed the
                // redo pass, the database is no longer in replication
                // slave mode and only the recover thread will access
                // this object until recover has complete. We
                // therefore do not need two versions of the log file
                // number anymore. From this point on, logFileNumber
                // is used for all references to the current log file
                // number; bootTimeLogFileNumber is no longer used.
                logFileNumber = bootTimeLogFileNumber;
				
				// if we are only interested in dumping the log, don't alter
				// the database and prevent anyone from using the log
				if (SanityManager.DEBUG)
				{
					if (SanityManager.DEBUG_ON(LogToFile.DUMP_LOG_ONLY))
					{
						Monitor.logMessage("_____________________________________________________");
						Monitor.logMessage("\n\t\t Log dump finished");
						Monitor.logMessage("_____________________________________________________");
                        // just in case, it has not been set anyway
						logOut = null; 

						return;
					}
				}


				/////////////////////////////////////////////////////////////
				//
				// determine where the log ends
				//
				/////////////////////////////////////////////////////////////
				StorageRandomAccessFile theLog = null;


				// if logend == LogCounter.INVALID_LOG_SCAN, that means there 
                // is no log record in the log - most likely it is corrupted in
                // some way ...
				if (logEnd == LogCounter.INVALID_LOG_INSTANT)
				{
					Monitor.logTextMessage(MessageId.LOG_LOG_NOT_FOUND);

					StorageFile logFile = getLogFileName(logFileNumber);

                    if (privExists(logFile))
					{
						// if we can delete this strange corrupted file, do so,
						// otherwise, skip it
                        if (!privDelete(logFile))
						{
							logFile = getLogFileName(++logFileNumber);
						}
					}
					IOException accessException = null;
					try
					{
                        theLog =   privRandomAccessFile(logFile, "rw");
					}
					catch (IOException ioe)
					{
						theLog = null;
						accessException = ioe;
					}

                    if (theLog == null || !privCanWrite(logFile))
					{
						if (theLog != null)
							theLog.close();

						theLog = null;
						Monitor.logTextMessage(MessageId.LOG_CHANGED_DB_TO_READ_ONLY);
						if (accessException != null)
							Monitor.logThrowable(accessException);
						ReadOnlyDB = true;
					}
					else
					{
						try
						{
							// no previous log file or previous log position
							if (!initLogFile(
                                    theLog, logFileNumber,
                                    LogCounter.INVALID_LOG_INSTANT))
                            {
								throw markCorrupt(
                                    StandardException.newException(
                                        SQLState.LOG_SEGMENT_NOT_EXIST,
                                        logFile.getPath()));
                            }
						}
						catch (IOException ioe)
						{
							throw markCorrupt(
                                StandardException.newException(
                                    SQLState.LOG_IO_ERROR, ioe));
						}

                        // successfully init'd the log file - set up markers,
                        // and position at the end of the log.
						setEndPosition( theLog.getFilePointer() );
						lastFlush   = endPosition;
						
						//if write sync is true , prellocate the log file
						//and reopen the file in rwd mode.
						if(isWriteSynced)
						{
							//extend the file by wring zeros to it
							preAllocateNewLogFile(theLog);
							theLog.close();
							theLog = openLogFileInWriteMode(logFile);
							//postion the log at the current end postion
							theLog.seek(endPosition);
						}
						
						if (SanityManager.DEBUG)
						{
							SanityManager.ASSERT(
                                endPosition == LOG_FILE_HEADER_SIZE,
                                "empty log file has wrong size");
						}
						
						//because we already incrementing the log number
						//here, no special log switch required for
						//backup recoveries.
						logSwitchRequired = false;
					}
				}
				else
				{
					// logEnd is the instant of the next log record in the log
					// it is used to determine the last known good position of
					// the log
					logFileNumber = LogCounter.getLogFileNumber(logEnd);

					ReadOnlyDB = df.isReadOnly();

					StorageFile logFile = getLogFileName(logFileNumber);

					if (!ReadOnlyDB)
					{
						// if datafactory doesn't think it is readonly, we can
						// do some futher test of our own
						IOException accessException = null;
						try
						{
							if(isWriteSynced)
								theLog = openLogFileInWriteMode(logFile);
							else
								theLog = privRandomAccessFile(logFile, "rw");
						}
						catch (IOException ioe)
						{
							theLog = null;
                            accessException = ioe;
						}
                        if (theLog == null || !privCanWrite(logFile))
						{
							if (theLog != null)
								theLog.close();
							theLog = null;
							Monitor.logTextMessage(MessageId.LOG_CHANGED_DB_TO_READ_ONLY);
							if (accessException != null)
								Monitor.logThrowable(accessException);	
							ReadOnlyDB = true;
											
						}
					}

					if (!ReadOnlyDB)
					{
						setEndPosition( LogCounter.getLogFilePosition(logEnd) );

						//
						// The end of the log is at endPosition.  Which is where
						// the next log should be appending.
						//
						// if the last log record ends before the end of the
                        // log file, then this log file has a fuzzy end.
                        // Zap all the bytes to between endPosition to EOF to 0.
						//
						// the end log marker is 4 bytes (of zeros)
						//
						// if endPosition + 4 == logOut.length, we have a
                        // properly terminated log file
						//
						// if endPosition + 4 is > logOut.length, there are 0,
                        // 1, 2, or 3 bytes of 'fuzz' at the end of the log. We
                        // can ignore that because it is guaranteed to be
                        // overwritten by the next log record.
						//
						// if endPosition + 4 is < logOut.length, we have a
                        // partial log record at the end of the log.
						//
						// We need to overwrite all of the incomplete log
                        // record, because if we start logging but cannot
                        // 'consume' all the bad log, then the log will truly
                        // be corrupted if the next 4 bytes (the length of the
                        // log record) after that is small enough that the next
                        // time the database is recovered, it will be
                        // interpreted that the whole log record is in the log
                        // and will try to objectify, only to get classNotFound
                        // error or worse.
						//

						//find out if log had incomplete log records at the end.
						if (redoScan.isLogEndFuzzy())
						{
							theLog.seek(endPosition);
							long eof = theLog.length();

							Monitor.logTextMessage(MessageId.LOG_INCOMPLETE_LOG_RECORD,
								logFile, endPosition, eof);

							/* Write zeros from incomplete log record to end of file */
							long nWrites = (eof - endPosition)/logBufferSize;
							int rBytes = (int)((eof - endPosition) % logBufferSize);
							byte zeroBuf[]= new byte[logBufferSize];
							
							//write the zeros to file
							while(nWrites-- > 0)
								theLog.write(zeroBuf);
							if(rBytes !=0)
								theLog.write(zeroBuf, 0, rBytes);
							
							if(!isWriteSynced)
								syncFile(theLog);
						}

						if (SanityManager.DEBUG)
						{
							if (theLog.length() != endPosition)
							{
								SanityManager.ASSERT(
                                    theLog.length() > endPosition,
                                    "log end > log file length, bad scan");
							}
						}

						// set the log to the true end position,
                        // and not the end of the file

						lastFlush = endPosition;
						theLog.seek(endPosition);
					}
				}

				if (theLog != null)
                {
                    if (logOut != null)
                    {
                        // Close the currently open log file, if there is
                        // one. DERBY-5937.
                        logOut.close();
                    }
					logOut = new LogAccessFile(this, theLog, logBufferSize);
                }
				
				if(logSwitchRequired)
					switchLogFile();


				boolean noInFlightTransactions = tf.noActiveUpdateTransaction();

				if (ReadOnlyDB)
				{
					// in the unlikely event that someone detects we are
					// dealing with a read only db, check to make sure the
					// database is quiesce when it was copied with no unflushed
					// dirty buffer
					if (!noInFlightTransactions)
                    {
						throw StandardException.newException(
                                SQLState.LOG_READ_ONLY_DB_NEEDS_UNDO);
                    }
				}

				/////////////////////////////////////////////////////////////
				//
				// Undo loop - in transaction factory.  It just gets one
				// transaction at a time from the transaction table and calls
				// undo, no different from runtime.
				//
				/////////////////////////////////////////////////////////////

                if (SanityManager.DEBUG)
                {
                    if (SanityManager.DEBUG_ON(LogToFile.DBG_FLAG))
                        SanityManager.DEBUG(LogToFile.DBG_FLAG,
                            "About to call undo(), transaction table =" +
                            tf.getTransactionTable());
                }

				if (!noInFlightTransactions)
				{
					if (SanityManager.DEBUG)
					{
						if (SanityManager.DEBUG_ON(LogToFile.DBG_FLAG))
							SanityManager.DEBUG(LogToFile.DBG_FLAG,
                                "In recovery undo, rollback inflight transactions");
					}

					tf.rollbackAllTransactions(recoveryTransaction, rawStoreFactory);

					if (SanityManager.DEBUG)
					{
						if (SanityManager.DEBUG_ON(LogToFile.DBG_FLAG))
							SanityManager.DEBUG(
                                LogToFile.DBG_FLAG, "finish recovery undo,");
					}
				}
				else
				{
					if (SanityManager.DEBUG)
					{
						if (SanityManager.DEBUG_ON(LogToFile.DBG_FLAG))
							SanityManager.DEBUG(LogToFile.DBG_FLAG,
                                "No in flight transaction, no recovery undo work");
					}
				}

				/////////////////////////////////////////////////////////////
				//
				// XA prepared xact loop - in transaction factory.  At this
                // point only prepared transactions should be left in the
                // transaction table, all others should have been aborted or
                // committed and removed from the transaction table.  It just
                // gets one transaction at a time from the transaction table,
                // creates a real context and transaction, reclaims locks,
                // and leaves the new xact in the transaction table.
				//
				/////////////////////////////////////////////////////////////

                if (SanityManager.DEBUG)
                {
                    if (SanityManager.DEBUG_ON(LogToFile.DBG_FLAG))
                        SanityManager.DEBUG(LogToFile.DBG_FLAG,
                            "About to call rePrepare(), transaction table =" +
                            tf.getTransactionTable());
                }

                tf.handlePreparedXacts(rawStoreFactory);

                if (SanityManager.DEBUG)
                {
                    if (SanityManager.DEBUG_ON(LogToFile.DBG_FLAG))
                        SanityManager.DEBUG(LogToFile.DBG_FLAG,
                            "Finished rePrepare(), transaction table =" +
                            tf.getTransactionTable());
                }

				/////////////////////////////////////////////////////////////
				//
				// End of recovery.
				//
				/////////////////////////////////////////////////////////////

				// recovery is finished.  Close the transaction
				recoveryTransaction.close();


				// notify the dataFactory that recovery is completed,
				// but before the checkpoint is written.
				dataFactory.postRecovery();


				//////////////////////////////////////////////////////////////
				// set the transaction factory short id, we have seen all the
				// trasactions in the log, and at the minimum, the checkpoint
				// transaction will be there.  Set the shortId to the next
				// value.
				//////////////////////////////////////////////////////////////
				tf.resetTranId();

				// do a checkpoint (will flush the log) if there is any rollback
				// if can't checkpoint for some reasons, flush log and carry on
				if (!ReadOnlyDB)
				{
					boolean needCheckpoint = true;

					// if we can figure out there there is very little in the
					// log (less than 1000 bytes), we haven't done any
                    // rollbacks, then don't checkpoint. Otherwise checkpoint.
					if (currentCheckpoint != null && noInFlightTransactions &&
						redoLWM != LogCounter.INVALID_LOG_INSTANT &&
						undoLWM != LogCounter.INVALID_LOG_INSTANT)
					{
						if ((logFileNumber == LogCounter.getLogFileNumber(redoLWM))
							&& (logFileNumber == LogCounter.getLogFileNumber(undoLWM))
							&& (endPosition < (LogCounter.getLogFilePosition(redoLWM) + 1000)))
							needCheckpoint = false;
					}

						if (needCheckpoint && !checkpoint(rawStoreFactory, df, tf, false))
							flush(logFileNumber, endPosition);
				}

				logger.close();

				recoveryNeeded = false;
			}
			catch (IOException ioe)
			{
				if (SanityManager.DEBUG)
					ioe.printStackTrace();

				throw markCorrupt(
                    StandardException.newException(SQLState.LOG_IO_ERROR, ioe));
			}
			catch (ClassNotFoundException cnfe)
			{
				throw markCorrupt(
                    StandardException.newException(
                        SQLState.LOG_CORRUPTED, cnfe));
			}
			catch (StandardException se)
			{
				throw markCorrupt(se);
			}
			catch (Throwable th)
			{
				if (SanityManager.DEBUG)
                {
                    SanityManager.showTrace(th);
					th.printStackTrace();
                }

				throw markCorrupt(
                    StandardException.newException(
                        SQLState.LOG_RECOVERY_FAILED, th));
			}
		}
		else
		{

			tf.useTransactionTable((Formatable)null);

			// set the transaction factory short id
			tf.resetTranId();
		}

        // done with recovery        
        
		/////////////////////////////////////////////////////////////
		// setup checkpoint daemon and cache cleaner
		/////////////////////////////////////////////////////////////
		checkpointDaemon = rawStoreFactory.getDaemon();
		if (checkpointDaemon != null)
        {
			myClientNumber =
                checkpointDaemon.subscribe(this, true /*onDemandOnly */);

            // use the same daemon for the cache cleaner
            dataFactory.setupCacheCleaner(checkpointDaemon);
        }
	}

 
	/**
		Checkpoint the rawStore.

		<P> MT- Only one checkpoint is to be taking place at any given time.

		<P> The steps of a checkpoint are

		<OL>
		<LI> switch to a new log file if possible
		<PRE>
			freeze the log (for the transition to a new log file)
				flush current log file
				create and flush the new log file (with file number 1 higher
                than the previous log file). The new log file becomes the
                current log file.
			unfreeze the log
		</PRE>
		<LI> start checkpoint transaction
		<LI> gather interesting information about the rawStore:
					the current log instant (redoLWM)
					the earliest active transaction begin tran log record 
                    instant (undoLWM), all the truncation LWM set by clients 
                    of raw store (replication)
		<LI> clean the buffer cache 
		<LI> log the next checkpoint log record, which contains 
				(repPoint, undoLWM, redoLWM) and commit checkpoint transaction.
		<LI> synchronously write the control file containing the next checkpoint
				log record log instant
		<LI> the new checkpoint becomes the current checkpoint.
				Somewhere near the beginning of each log file should be a
				checkpoint log record (not guarenteed to be there)
		<LI> see if the log can be truncated

		<P>
		The earliest useful log record is determined by the repPoint and the 
        undoLWM, whichever is earlier. 
		<P>
		Every log file whose log file number is smaller than the earliest 
        useful log record's log file number can be deleted.

		<P><PRE>
			Transactions can be at the following states w/r to a checkpoint -
			consider the log as a continous stream and not as series of log 
            files for the sake of clarity.  
			|(BT)-------(ET)| marks the begin and end of a transaction.
			.                          checkpoint started
			.       |__undoLWM          |
			.       V                   |___redoLWM
			.                           |___TruncationLWM
			.                           |
			.                           V
			1 |-----------------|
			2       |--------------------------------|
			3           |-------|
			4               |--------------------------------------(end of log)
			5                                       |-^-|
			.                                   Checkpoint Log Record
			---A---&gt;|&lt;-------B---------&gt;|&lt;-------------C-----------
		</PRE>

		<P>
		There are only 3 periods of interest : <BR>
			A) before undoLWM,  B) between undo and redo LWM, C) after redoLWM.

		<P>
		Transaction 1 started in A and terminates in B.<BR>
			During redo, we should only see log records and endXact from this
			transaction in the first phase (between undoLWM and redoLWM).  No
			beginXact log record for this transaction will be seen.

		<P>
		Transaction 2 started in B (right on the undoLWM) and terminated in C.<BR>
			Any transaction that terminates in C must have a beginXact at or 
            after undoLWM.  In other words, no transaction can span A, B and C.
			During redo, we will see beginXact, other log records and endXact 
            for this transaction.

		<P>
		Transaction 3 started in B and ended in B.<BR>
			During redo, we will see beginXact, other log records and endXact 
            for this transaction.

		<P>
		Transaction 4 begins in B and never ends.<BR>
			During redo, we will see beginXact, other log records.
			In undo, this loser transaction will be rolled back.

		<P>
		Transaction 5 is the transaction taking the checkpoint.<BR>
		    The checkpoint action started way back in time but the checkpoint
			log record is only written after the buffer cache has been flushed.

		<P>
		Note that if any time elapse between taking the undoLWM and the
			redoLWM, then it will create a 4th period of interest.

		@exception StandardException - encounter exception while doing 
                                       checkpoint.

        @param rsf          The RawStoreFactory to use to do the checkpoint.
        @param df           The DataFactory to use to do the checkpoint. 
        @param tf           The TransactionFactory to use to do the checkpoint.
        @param wait         If an existing checkpoint is in progress, then if
                            wait=true then this routine will wait for the 
                            checkpoint to complete and the do another checkpoint
                            and wait for it to finish before returning.
	*/

	public boolean checkpoint(
    RawStoreFactory     rsf,
    DataFactory         df,
    TransactionFactory  tf, 
    boolean             wait)
		 throws StandardException
	{

		if (inReplicationSlavePreMode) 
        {
			// Writing a checkpoint updates the log files and the log.ctrl
			// file. This cannot be allowed in slave pre mode because the slave
			// and master log files need to be equal when the database is
			// booted in slave mode (the next phase of the start slave command).
			return true;
		}

		// call checkpoint with no pre-started transaction
		boolean done = checkpointWithTran(null, rsf, df, tf, wait);

		return done;
	}

	/**
		checkpoint with pre-start transaction

        @param rsf          The RawStoreFactory to use to do the checkpoint.
        @param df           The DataFactory to use to do the checkpoint. 
        @param tf           The TransactionFactory to use to do the checkpoint.
        @param wait         If an existing checkpoint is in progress, then if
                            wait=true then this routine will wait for the 
                            checkpoint to complete and the do another checkpoint
                            and wait for it to finish before returning.

		@exception StandardException Derby Standard Error Policy 
	*/
    private boolean checkpointWithTran(
    RawTransaction      cptran, 
    RawStoreFactory     rsf,
    DataFactory         df,
    TransactionFactory  tf,
    boolean             wait)
		 throws StandardException
	{
		LogInstant  redoLWM;

        // we may be called to stop the database after a bad error, make sure
		// logout is set
		if (logOut == null)
		{
			return false;
		}

		long approxLogLength;

		boolean     proceed = true;
        do
        {
            synchronized (this)
            {
                if (corrupt != null)
                {
                    // someone else found a problem in the raw store.  

                    throw StandardException.newException(
                            SQLState.LOG_STORE_CORRUPT, corrupt);
                }

                approxLogLength = endPosition; // current end position

                if (!inCheckpoint)
                {
                    // no checkpoint in progress, change status to indicate
                    // this code is doing the checkpoint.
                    inCheckpoint = true;

                    // break out of loop and continue to execute checkpoint
                    // in this routine.
                    break;
                }
                else
                {
                    // There is a checkpoint in progress.

                    if (wait)
                    {
                        // wait until the thread executing the checkpoint 
                        // completes.


                        // In some cases like backup and compress it is not 
                        // enough that a checkpoint is in progress, the timing 
                        // is important.
                        // In the case of compress it is necessary that the 
                        // redo low water mark be moved forward past all 
                        // operations up to the current time, so that a redo of
                        // the subsequent compress operation is guaranteed
                        // to not encounter any log record on the container 
                        // previous to the compress.  In this case the 'wait'
                        // flag is passed in as 'true'.
                        //
                        // When wait is true and another thread is currently
                        // executing the checkpoint, execution waits here until
                        // the other thread which is actually doing the the 
                        // checkpoint completes.  And then the code will loop
                        // until this thread executes the checkpoint.
 
                        while (inCheckpoint)
                        {
                            try
                            {
                                wait();
                            }	
                            catch (InterruptedException ie)
                            {
                                InterruptStatus.setInterrupted();
                            }	
                        }
                    }
                    else
                    {
                        // caller did not want to wait for already executing
                        // checkpoint to finish.  Routine will return false
                        // upon exiting the loop.
                        proceed = false;
                    }
                }

                // don't return from inside of a sync block
            }
        }
        while (proceed);

		if (!proceed)
		{
			return false;
		}

		// needCPtran == true if not supplied with a pre-started transaction
		boolean needCPTran = (cptran == null);

		if (SanityManager.DEBUG)
		{
			if (logSwitchInterval == 0)
			{
				SanityManager.THROWASSERT(
                    "switching log file: Approx log length = " + 
                    approxLogLength + " logSwitchInterval = 0");
			}
		}


		try
		{
			if (approxLogLength > logSwitchInterval)
			{
				switchLogFile();

				//log switch is occuring in conjuction with the 
				//checkpoint, set the amount of log written from last 
                //checkpoint to zero.
				logWrittenFromLastCheckPoint = 0;
			}
            else
			{
				//checkpoint is happening without the log switch,
				//in the middle of a log file. Amount of log written already for
				//the current log file should not be included in caluculation 
				//of when next check point is due. By assigning the negative
				//value of amount of log written for this file. Later it will
				//be subtracted when we switch the log file or while 
                //calculating whether we are due a for checkpoint at flush time.
				logWrittenFromLastCheckPoint = -endPosition;
			}

			if (SanityManager.DEBUG)
			{
				// if this debug flag is set on, just switch log file

				if (SanityManager.DEBUG_ON(TEST_LOG_SWITCH_LOG))
					return false;	
			}


			// start a checkpoint transaction 
			if (needCPTran)
				cptran = tf.startInternalTransaction(rsf,
				getContextService().getCurrentContextManager());

			/////////////////////////////////////////////////////
			// gather a snapshot of the various interesting points of the log
			/////////////////////////////////////////////////////
			long undoLWM_long;
			long redoLWM_long;

			synchronized(this)	// we could synchronized on something else, it
				// doesn't matter as long as logAndDo sync on
				// the same thing
			{
				// The redo LWM is the current log instant.  We are going to 
                // clean the cache shortly, any log record before this point 
                // will not ever need to be redone.
				redoLWM_long = currentInstant();
				redoLWM = new LogCounter(redoLWM_long);

                // The undo LWM is what we need to rollback all transactions.
                // Synchronize this with the starting of a new transaction so 
                // that the transaction factory can have a consistent view
                // See FileLogger.logAndDo

				LogCounter undoLWM = (LogCounter)(tf.firstUpdateInstant());
				if (undoLWM == null)
					undoLWM_long = redoLWM_long; // no active transaction 
				else
					undoLWM_long = undoLWM.getValueAsLong();

			}

			/////////////////////////////////////////////////////
			// clean the buffer cache
			/////////////////////////////////////////////////////
			df.checkpoint();


			/////////////////////////////////////////////////////
			// write out the checkpoint log record
			/////////////////////////////////////////////////////
		
			// send the checkpoint record to the log
			Formatable transactionTable = tf.getTransactionTable();

			CheckpointOperation nextCheckpoint = 
				new CheckpointOperation(
                    redoLWM_long, undoLWM_long, transactionTable);

			cptran.logAndDo(nextCheckpoint);

			LogCounter checkpointInstant = 
                (LogCounter)(cptran.getLastLogInstant());

			if (checkpointInstant != null)
            {
                // since checkpoint is an internal transaction, I need to 
                // flush it to make sure it actually goes to the log
				flush(checkpointInstant); 
            }
			else
            {
				throw StandardException.newException(
                        SQLState.LOG_CANNOT_LOG_CHECKPOINT);
            }

			cptran.commit();

			if (needCPTran)
			{
				cptran.close();	// if we started it, we will close it
				cptran = null;
			}

			/////////////////////////////////////////////////////
			// write out the log control file which contains the last
			// successful checkpoint log record
			/////////////////////////////////////////////////////

			if (!writeControlFile(getControlFileName(),
								  checkpointInstant.getValueAsLong()))
			{
				throw StandardException.newException(
                        SQLState.LOG_CONTROL_FILE, getControlFileName());
			}

			// next checkpoint becomes the current checkpoint
			currentCheckpoint = nextCheckpoint;


			////////////////////////////////////////////////////
			// see if we can reclaim some log space
			////////////////////////////////////////////////////

			if (!logArchived())
			{
				truncateLog(currentCheckpoint);
			}

			// delete the committted container drop stubs 
            // that are no longer required during recovery. 
            // If a backup is in progress don't delete the stubs until 
            // it is done. Backup needs to copy all the stubs that 
            // are needed to recover from the backup checkpoint on restore.
            if(!backupInProgress)
                df.removeDroppedContainerFileStubs(redoLWM);
		
		}
		catch (IOException ioe)
		{
			throw markCorrupt(
                    StandardException.newException(SQLState.LOG_IO_ERROR, ioe));
		}
		finally 
		{
			synchronized(this)
			{
				

				inCheckpoint = false;
				notifyAll();
			}

			if (cptran != null && needCPTran)
			{
				try 
				{
					cptran.commit();
					cptran.close();
				}
				catch (StandardException se)
				{
					throw markCorrupt(StandardException.newException(
                                            SQLState.LOG_CORRUPTED, se));
				}
			}
		}

		return true;
	}

	/**
		Flush all unwritten log record up to the log instance indicated to disk
		and sync.
		Also check to see if database is frozen or corrupt.

		<P>MT - not needed, wrapper method

		@param where flush log up to here

		@exception StandardException Standard Derby error policy
	*/
	public void flush(LogInstant where) throws StandardException
	{
		long fileNumber;
		long wherePosition;

		if (where == null) {	
			// don't flush, just use this to check if database is frozen or
			// corrupt 
			fileNumber = 0;
			wherePosition = LogCounter.INVALID_LOG_INSTANT;
		} else {
			LogCounter whereC = (LogCounter) where;
			fileNumber = whereC.getLogFileNumber();
			wherePosition = whereC.getLogFilePosition();
		}
		flush(fileNumber, wherePosition);
	}

	/**
		Flush all unwritten log record to disk and sync.
		Also check to see if database is frozen or corrupt.

		<P>MT - not needed, wrapper method

		@exception StandardException Standard Derby error policy
	*/
	public void flushAll() throws StandardException
	{
		long fnum;
		long whereTo;

		synchronized(this)
		{
			fnum = logFileNumber;
			whereTo = endPosition;
		}

		flush(fnum, whereTo);
	}

	/*
	 * Private methods that helps to implement methods of LogFactory
	 */

	/**
		Verify that we the log file is of the right format and of the right
		version and log file number.

		<P>MT - not needed, no global variables used

		@param logFileName the name of the log file
		@param number the log file number
		@return true if the log file is of the current version and of the
		correct format

		@exception StandardException Standard Derby error policy
	*/
	private boolean verifyLogFormat(StorageFile logFileName, long number)
		 throws StandardException 
	{
		boolean ret = false;
		try 
		{
			StorageRandomAccessFile log = privRandomAccessFile(logFileName, "r");
			ret = verifyLogFormat(log, number);
			log.close();
		}
		catch (IOException ioe)
		{
			
		}

		return ret;
	}

	/**
		Verify that we the log file is of the right format and of the right
		version and log file number.  The log file position is set to the
		beginning.

		<P>MT - MT-unsafe, caller must synchronize

		@param log the log file
		@param number the log file number
		@return true if the log file is of the current version and of the
		correct format

		@exception StandardException Standard Derby error policy
	*/
	private boolean verifyLogFormat(StorageRandomAccessFile log, long number) 
		 throws StandardException
	{
		try 
		{
			log.seek(0);
			int logfid = log.readInt();
			int obsoleteLogVersion = log.readInt(); // this value is useless, for
								// backwards compatibility
			long logNumber = log.readLong();

			if (logfid != fid || logNumber != number)
            {
				throw StandardException.newException(
                        SQLState.LOG_INCOMPATIBLE_FORMAT, dataDirectory);
            }
		}
		catch (IOException ioe)
		{
			throw StandardException.newException(
                    SQLState.LOG_CANNOT_VERIFY_LOG_FORMAT, ioe, dataDirectory);
		}

		return true;
	}

	/**
		Initialize the log to the correct format with the given version and
		log file number.  The new log file must be empty.  After initializing,
		the file is synchronously written to disk.

		<P>MT - synchornization provided by caller

		@param newlog the new log file to be initialized
		@param number the log file number
		@param prevLogRecordEndInstant the end position of the  previous log record

		@return true if the log file is empty, else false.

		@exception IOException if new log file cannot be accessed or initialized
	*/

	private boolean initLogFile(StorageRandomAccessFile newlog, long number,
								long prevLogRecordEndInstant)
		 throws IOException, StandardException
	{
		if (newlog.length() != 0)
			return false;

		if (SanityManager.DEBUG)
		{
			if ( SanityManager.DEBUG_ON(TEST_LOG_FULL))
				testLogFull();
		}
		if (SanityManager.DEBUG)
		{
			if (SanityManager.DEBUG_ON(TEST_SWITCH_LOG_FAIL1))
				throw new IOException("TestLogSwitchFail1");
		}


		newlog.seek(0);

		newlog.writeInt(fid);
		newlog.writeInt(OBSOLETE_LOG_VERSION_NUMBER); // for silly backwards compatibility reason
		newlog.writeLong(number);
		newlog.writeLong(prevLogRecordEndInstant);

		syncFile(newlog);

		return true;
	}

	/**
		Switch to the next log file if possible.

		<P>MT - log factory is single threaded thru a log file switch, the log
		is frozen for the duration of the switch
	*/
	public void switchLogFile() throws StandardException
	{
		boolean switchedOver = false;

		/////////////////////////////////////////////////////
		// Freeze the log for the switch over to a new log file.
		// This blocks out any other threads from sending log
		// record to the log stream.
		// 
		// The switching of the log file and checkpoint are really
		// independent events, they are tied together just because a
		// checkpoint is the natural place to switch the log and vice
		// versa.  This could happen before the cache is flushed or
		// after the checkpoint log record is written.
		/////////////////////////////////////////////////////
		synchronized (this)
		{

			// Make sure that this thread of control is guaranteed to complete
            // it's work of switching the log file without having to give up
            // the semaphore to a backup or another flusher.  Do this by looping
            // until we have the semaphore, the log is not being flushed, and
            // the log is not frozen for backup.  Track (2985). 
			while(logBeingFlushed | isFrozen)
			{
				try
				{
					wait();
				}
				catch (InterruptedException ie)
				{
                    InterruptStatus.setInterrupted();
				}	
			}

			// we have an empty log file here, refuse to switch.
			if (endPosition == LOG_FILE_HEADER_SIZE)
			{
				if (SanityManager.DEBUG)
				{
					Monitor.logMessage("not switching from an empty log file (" +
						   logFileNumber + ")");
				}	
				return;
			}

			// log file isn't being flushed right now and logOut is not being
			// used.
			StorageFile newLogFile = getLogFileName(logFileNumber+1);

			if (logFileNumber+1 >= maxLogFileNumber)
            {
				throw StandardException.newException(
                        SQLState.LOG_EXCEED_MAX_LOG_FILE_NUMBER, 
                        maxLogFileNumber); 
            }

			StorageRandomAccessFile newLog = null;	// the new log file
			try 
			{
				// if the log file exist and cannot be deleted, cannot
				// switch log right now
                if (privExists(newLogFile) && !privDelete(newLogFile))
				{
					logErrMsg(MessageService.getTextMessage(
                        MessageId.LOG_NEW_LOGFILE_EXIST,
					    newLogFile.getPath()));
					return;
				}

				try
				{
                    newLog =   privRandomAccessFile(newLogFile, "rw");
				}
				catch (IOException ioe)
				{
					newLog = null;
				}

                if (newLog == null || !privCanWrite(newLogFile))
				{
					if (newLog != null)
						newLog.close();
					newLog = null;

					return;
				}

				if (initLogFile(newLog, logFileNumber+1,
								LogCounter.makeLogInstantAsLong(logFileNumber, endPosition)))
				{

					// New log file init ok, close the old one and
					// switch over, after this point, need to shutdown the
					// database if any error crops up
					switchedOver = true;

					// write out an extra 0 at the end to mark the end of the log
					// file.
					
					logOut.writeEndMarker(0);

					setEndPosition( endPosition + INT_LENGTH );
					//set that we are in log switch to prevent flusher 
					//not requesting  to switch log again 
					inLogSwitch = true; 
					// flush everything including the int we just wrote
					flush(logFileNumber, endPosition);
					
					
					// simulate out of log error after the switch over
					if (SanityManager.DEBUG)
					{
						if (SanityManager.DEBUG_ON(TEST_SWITCH_LOG_FAIL2))
							throw new IOException("TestLogSwitchFail2");
					}


					logOut.close();		// close the old log file
					
					logWrittenFromLastCheckPoint += endPosition;

					setEndPosition( newLog.getFilePointer() );
					lastFlush = endPosition;
					
					if(isWriteSynced)
					{
						//extend the file by wring zeros to it
						preAllocateNewLogFile(newLog);
						newLog.close();
						newLog = openLogFileInWriteMode(newLogFile);
						newLog.seek(endPosition);
					}

					logOut = new LogAccessFile(this, newLog, logBufferSize);
					newLog = null;


					if (SanityManager.DEBUG)
					{
						if (endPosition != LOG_FILE_HEADER_SIZE)
							SanityManager.THROWASSERT(
											"new log file has unexpected size" +
											 + endPosition);
					}
					logFileNumber++;

					if (SanityManager.DEBUG)
					{
						SanityManager.ASSERT(endPosition == LOG_FILE_HEADER_SIZE,
											 "empty log file has wrong size");
					}

				}
				else	// something went wrong, delete the half baked file
				{
					newLog.close();
					newLog = null;

					if (privExists(newLogFile))
					    privDelete(newLogFile);

					logErrMsg(MessageService.getTextMessage(
                        MessageId.LOG_CANNOT_CREATE_NEW,
                        newLogFile.getPath()));
					newLogFile = null;
 				}

			}
			catch (IOException ioe)
			{

				inLogSwitch = false;
				// switching log file is an optional operation and there is no direct user
				// control.  Just sends a warning message to whomever, if any,
				// system adminstrator there may be

                logErrMsg(MessageService.getTextMessage(
                    MessageId.LOG_CANNOT_CREATE_NEW_DUETO,
                    newLogFile.getPath(),
                    ioe.toString()));

				try
				{
					if (newLog != null)
					{
						newLog.close();
						newLog = null;
					}
				}
				catch (IOException ioe2) {}

                if (newLogFile != null && privExists(newLogFile))
				{
                    privDelete(newLogFile);
					newLogFile = null;
				}

				if (switchedOver)	// error occur after old log file has been closed!
				{
					logOut = null; // limit any damage
					throw markCorrupt(
                        StandardException.newException(
                                SQLState.LOG_IO_ERROR, ioe));
				}
			}
			
            // Replication slave: Recovery thread should be allowed to
            // read the previous log file
            if (inReplicationSlaveMode) {
                allowedToReadFileNumber = logFileNumber-1;
                synchronized (slaveRecoveryMonitor) {
                    slaveRecoveryMonitor.notify();
                }
            }
			inLogSwitch = false;
		}
		// unfreezes the log
	}

	/**
		Flush all unwritten log record up to the log instance indicated to disk
		without syncing.

		<P>MT - not needed, wrapper method

		@param wherePosition flush log up to here

		@exception IOException Failed to flush to the log
	*/
	private void flushBuffer(long fileNumber, long wherePosition)
		throws IOException, StandardException
	{
		synchronized (this) {
			if (fileNumber < logFileNumber)	// history
				return;

			// A log instant indicates the start of a log record
			// but not how long it is. Thus the amount of data in
			// the logOut buffer is irrelevant. We can only
			// not flush the buffer if the real synced flush
			// included this required log instant. This is because
			// we never flush & sync partial log records.

			if (wherePosition < lastFlush) // already flushed
				return;

			// We don't update lastFlush here because lastFlush
			// is the last position in the log file that has been
			// flushed *and* synced to disk. Here we only flush.
			// ie. lastFlush should be renamed lastSync.
			//
			// We could have another variable indicating to which
			// point the log has been flushed which this routine
			// could take advantage of. This would only help rollbacks though.

			logOut.flushLogAccessFile();
		}
	}
	/** Get rid of old and unnecessary log files

		<P> MT- only one truncate log is allowed to be taking place at any
		given time.  Synchronized on this.

	 */
	private void truncateLog(CheckpointOperation checkpoint)
	{
		long firstLogNeeded;
		if ((firstLogNeeded = getFirstLogNeeded(checkpoint))==-1)
			return;
		truncateLog(firstLogNeeded);
	}

	/** Get rid of old and unnecessary log files
	 * @param firstLogNeeded The log file number of the oldest log file
	 * needed for recovery.
	 */
	private void truncateLog(long firstLogNeeded) {
		long oldFirstLog;
		if (keepAllLogs)
			return;
		
		// when  backup is in progress, log files that are yet to
        // be copied to the backup should not be deleted,  even 
        // if they are not required  for crash recovery.
        if(backupInProgress) {
            long logFileNeededForBackup = logFileToBackup;
            // check if the log file number is yet to be copied 
            // to the backup is less than the log file required 
            // for crash recovery, if it is then make the first 
            // log file that should not be deleted is the log file 
            // that is yet to  be copied to the backup.  
            if (logFileNeededForBackup < firstLogNeeded)
                firstLogNeeded = logFileNeededForBackup;
        }

		oldFirstLog = firstLogFileNumber;
		firstLogFileNumber = firstLogNeeded;

		while(oldFirstLog < firstLogNeeded)
		{
			StorageFile uselessLogFile = null;
			try
			{
				uselessLogFile = getLogFileName(oldFirstLog);
                if (privDelete(uselessLogFile))
				{
					if (SanityManager.DEBUG)
					{
						if (SanityManager.DEBUG_ON(LogToFile.DBG_FLAG))
							SanityManager.DEBUG(DBG_FLAG, "truncating useless log file " + uselessLogFile.getPath());
					}
				}
				else
				{
					if (SanityManager.DEBUG)
					{
						if (SanityManager.DEBUG_ON(LogToFile.DBG_FLAG))
							SanityManager.DEBUG(DBG_FLAG, "Fail to truncate useless log file " + uselessLogFile.getPath());
					}
				}
			}
			catch (StandardException se)
			{
				if (SanityManager.DEBUG)
					SanityManager.THROWASSERT("error opening log segment while deleting "
											  + uselessLogFile.getPath(), se);

				// if insane, just leave it be
			}

			oldFirstLog++;
		}
	}

   

    /**
     * Return the "oldest" log file still needed by recovery. 
     * <p>
     * Returns the log file that contains the undoLWM, ie. the oldest
     * log record of all uncommitted transactions in the given checkpoint.
     * 
     * If no checkpoint is given then returns -1, indicating all log records
     * may be necessary.
     *
     **/
	private long getFirstLogNeeded(CheckpointOperation checkpoint)
    {
		long firstLogNeeded;

		// one truncation at a time
		synchronized (this)
		{
			firstLogNeeded = 
                (checkpoint != null ? 
                     LogCounter.getLogFileNumber(checkpoint.undoLWM()) : -1);

			if (SanityManager.DEBUG)
			{
				if (SanityManager.DEBUG_ON(LogToFile.DBG_FLAG))
					SanityManager.DEBUG(DBG_FLAG, 
                       "truncatLog: undoLWM firstlog needed " + firstLogNeeded);
			}

			if (SanityManager.DEBUG)
			{
				if (SanityManager.DEBUG_ON(LogToFile.DBG_FLAG))
				{
                    SanityManager.DEBUG(DBG_FLAG, 
                      "truncatLog: checkpoint truncationLWM firstlog needed " +
                      firstLogNeeded);
                    SanityManager.DEBUG(DBG_FLAG, 
                      "truncatLog: firstLogFileNumber = " + firstLogFileNumber);
				}
			}
		}
		return firstLogNeeded;
	}


	/**
		Carefully write out this value to the control file.
        We do safe write of this data by writing the data 
        into two files every time we write the control data.
        we write checksum at the end of the file, so if by
        chance system crashes while writing into the file,
        using the checksum we find that the control file
        is hosed then we  use the mirror file, which will have
        the control data written at last check point.

		see comment at beginning of file for log control file format.

		<P> MT- synchronized by caller
	*/
	// When changing this code, also update the comment at the beginning of
	// this class, the ControlFileReader of DERBY-5195, and the description
	// on the web page in http://db.apache.org/derby/papers/logformats.html
	boolean writeControlFile(StorageFile logControlFileName, long value)
		 throws IOException, StandardException
	{
		StorageRandomAccessFile logControlFile = null;

		ByteArrayOutputStream baos = new ByteArrayOutputStream(64);
		DataOutputStream daos = new DataOutputStream(baos);

		daos.writeInt(fid);

		// so that when this db is booted by 1.1x and 1.2x JBMS, a IOException
		// stack trace rather than some error message that tells
		// the user to delete the database will show up.
		daos.writeInt(OBSOLETE_LOG_VERSION_NUMBER);
		daos.writeLong(value);

		if (onDiskMajorVersion == 0) {
			onDiskMajorVersion = jbmsVersion.getMajorVersion();
			onDiskMinorVersion = jbmsVersion.getMinorVersion();
			onDiskBeta = jbmsVersion.isBeta();
		}

		// previous to 1.3, that's all we wrote.  
		// from 1.3 and onward, also write out the JBMSVersion 
		daos.writeInt(onDiskMajorVersion);
		daos.writeInt(onDiskMinorVersion);

		// For 2.0 beta we added the build number and the isBeta indication.
		// (5 bytes from our first spare long)
		daos.writeInt(jbmsVersion.getBuildNumberAsInt());

		byte flags = 0;
		if (onDiskBeta) 
            flags |= IS_BETA_FLAG;
        
        // When database is booted with derby.system.durability=test,
        // this mode does not guarantee that 
        // - database will recover 
        // - committed transactions will not be lost
        // - database will be in a consistent state
        // Hence necessary to keep track of this state so we don't 
        // waste time resolving issues in such cases.
        // wasDBInDurabilityTestModeNoSync has information if database was
        // previously booted at any time in this mode
        if (logNotSynced || wasDBInDurabilityTestModeNoSync)
            flags |= IS_DURABILITY_TESTMODE_NO_SYNC_FLAG;
 		daos.writeByte(flags);

		//
		// write some spare bytes after 2.0 we have 3 + 2(8) spare bytes.
 		long spare = 0;
       
		daos.writeByte(0);
		daos.writeByte(0);
        daos.writeByte(0);
		daos.writeLong(spare);
		daos.flush();
		// write the checksum for the control data written
		checksum.reset();
		checksum.update(baos.toByteArray(), 0, baos.size());
		daos.writeLong(checksum.getValue());
		daos.flush();

		try
		{
            checkCorrupt();

			try
			{
                logControlFile = privRandomAccessFile(logControlFileName, "rw");
			}
			catch (IOException ioe)
			{
				logControlFile = null;
				return false;
			}

            if (!privCanWrite(logControlFileName))
				return false;

			if (SanityManager.DEBUG)
			{
				if (SanityManager.DEBUG_ON(TEST_LOG_FULL))
					testLogFull();
			}

			logControlFile.seek(0);
			logControlFile.write(baos.toByteArray());
            syncFile(logControlFile);
            logControlFile.close();

			// write the same data to mirror control file
			try
			{
				logControlFile =
                    privRandomAccessFile(getMirrorControlFileName(), "rw");
			}
			catch (IOException ioe)
			{
				logControlFile = null;
				return false;
			}

			logControlFile.seek(0);
			logControlFile.write(baos.toByteArray());
            syncFile(logControlFile);

		}
		finally
		{
			if (logControlFile != null)
				logControlFile.close();
		}

		return true;

	}

	/*
		Carefully read the content of the control file.

		<P> MT- read only
	*/
	private long readControlFile(StorageFile logControlFileName, Properties startParams)
		 throws IOException, StandardException
	{
		StorageRandomAccessFile logControlFile = null;
		ByteArrayInputStream bais = null;
        DataInputStream dais = null;
		logControlFile =  privRandomAccessFile(logControlFileName, "r");
		boolean upgradeNeeded = false;
		long value = LogCounter.INVALID_LOG_INSTANT;
		long onDiskChecksum = 0;
		long controlFilelength = logControlFile.length();
		byte barray[] = null;

		try
		{
			// The length of the file is less than the minimum in any version
            // It is possibly hosed , no point in reading data from this file
            // skip reading checksum  control file is before 1.5
            if (controlFilelength < 16)
				onDiskChecksum = -1;
			else if (controlFilelength == 16)
			{
				barray = new byte[16];
				logControlFile.readFully(barray);
			}else if (controlFilelength > 16)
            {
				barray = new byte[(int) logControlFile.length() - 8];
				logControlFile.readFully(barray);
				onDiskChecksum = logControlFile.readLong();
				if (onDiskChecksum !=0 )
				{
					checksum.reset();
					checksum.update(barray, 0, barray.length);
				}
			}

			if ( onDiskChecksum == checksum.getValue() || onDiskChecksum ==0)
			{

				bais = new ByteArrayInputStream(barray);
				dais = new DataInputStream(bais);

				if (dais.readInt() != fid)
	            {
	                throw StandardException.newException(
	                        SQLState.LOG_INCOMPATIBLE_FORMAT, dataDirectory);
	            }
	
				int obsoleteVersion = dais.readInt();
				value = dais.readLong();
	
				if (SanityManager.DEBUG)
				{
					if (SanityManager.DEBUG_ON(LogToFile.DBG_FLAG))
	                    SanityManager.DEBUG(LogToFile.DBG_FLAG, 
	                        "log control file ckp instance = " + 
	                        LogCounter.toDebugString(value));
				}
	
	
				// from version 1.5 onward, we added an int for storing JBMS
				// version and an int for storing checkpoint interval
				// and log switch interval
				onDiskMajorVersion = dais.readInt();
				onDiskMinorVersion = dais.readInt();
				int dbBuildNumber = dais.readInt();
				int flags = dais.readByte();
				
				// check if the database was booted previously at any time with
                // derby.system.durability=test mode
                // If yes, then on a boot error we report that this setting is
                // probably the cause for the error and also log a warning
                // in the derby.log that this mode was set previously
                wasDBInDurabilityTestModeNoSync = 
                    (flags & IS_DURABILITY_TESTMODE_NO_SYNC_FLAG) != 0;

                if (SanityManager.DEBUG) {
                    if (SanityManager.DEBUG_ON(LogToFile.DBG_FLAG))
                        SanityManager.DEBUG(LogToFile.DBG_FLAG,
                        "log control file, was derby.system.durability set to test = " +
                        wasDBInDurabilityTestModeNoSync);
                }
                    
				
				onDiskBeta = (flags & IS_BETA_FLAG) != 0;
				if (onDiskBeta)
				{
					// if is beta, can only be booted by exactly the same
					// version
					if (!jbmsVersion.isBeta() ||
						onDiskMajorVersion != jbmsVersion.getMajorVersion() ||
						onDiskMinorVersion != jbmsVersion.getMinorVersion())
					{
						boolean forceBetaUpgrade = false;
						if (SanityManager.DEBUG)
						{
							// give ourselves an out for this beta check for debugging purposes
							if (SanityManager.DEBUG_ON("forceBetaUpgrade"))
							{
								Monitor.logMessage("WARNING !! : forcing beta upgrade.");
								forceBetaUpgrade =true;
							}
						}

						if (!forceBetaUpgrade)
	                    {
							throw StandardException.newException(
	                            SQLState.LOG_CANNOT_UPGRADE_BETA, 
	                            dataDirectory,
								ProductVersionHolder.simpleVersionString(onDiskMajorVersion, onDiskMinorVersion, onDiskBeta));
	                    }
					}
				}
					
	
				// JBMS_VERSION must be numbered in a way so that it is ever
				// increasing.  We are backwards compatible but not forwards
				// compatible 
				//
				if (onDiskMajorVersion > jbmsVersion.getMajorVersion() ||
					(onDiskMajorVersion == jbmsVersion.getMajorVersion() &&
					 onDiskMinorVersion > jbmsVersion.getMinorVersion()))
				{
					// don't need to worry about point release, no format
					// upgrade is allowed. 
					throw StandardException.newException(
	                        SQLState.LOG_INCOMPATIBLE_VERSION, 
	                        dataDirectory,
							ProductVersionHolder.simpleVersionString(onDiskMajorVersion, onDiskMinorVersion, onDiskBeta));
				}

				// Ensure that upgrade has been requested for a major or minor upgrade
				// maintaince (point) versions should not require an upgrade.
				if ((onDiskMajorVersion != jbmsVersion.getMajorVersion()) ||
					(onDiskMinorVersion != jbmsVersion.getMinorVersion()))
				{
					upgradeNeeded = true;
				}
				// if checksum is zeros in  version > 3.5 file is hosed
				// except incase of upgrade from versions <= 3.5
				if (onDiskChecksum == 0 && 
					(!(onDiskMajorVersion <= 3 && onDiskMinorVersion <=5) ||
					onDiskMajorVersion == 0))
					value = LogCounter.INVALID_LOG_INSTANT; 
			}
		}
		finally
		{
			if (logControlFile != null)
				logControlFile.close();
			if (bais != null)
				bais.close();
			if (dais != null)
				dais.close();
		}

		if (upgradeNeeded)
		{
			if (isFullUpgrade(startParams,
				ProductVersionHolder.simpleVersionString(onDiskMajorVersion, onDiskMinorVersion, onDiskBeta))) {

				onDiskMajorVersion = jbmsVersion.getMajorVersion();
				onDiskMinorVersion = jbmsVersion.getMinorVersion();
				onDiskBeta = jbmsVersion.isBeta();

				// Write out the new log control file with the new
				// version, the database has been upgraded

				if (!writeControlFile(logControlFileName, value))
				{
					throw StandardException.newException(
							SQLState.LOG_CONTROL_FILE, logControlFileName);
				}
			}
		}

		return value;

	}



    /**
     * Create the directory where transaction log should go.
     * @exception StandardException Standard Error Policy
    */
	private void createLogDirectory() throws StandardException
	{
		StorageFile logDir = 
            logStorageFactory.newStorageFile(LogFactory.LOG_DIRECTORY_NAME);

        if (privExists(logDir)) {
            // make sure log directory is empty.
            String[] logfiles = privList(logDir);
            if (logfiles != null) {
                if(logfiles.length != 0) {
                    throw StandardException.newException(
                        SQLState.LOG_SEGMENT_EXIST, logDir.getPath());
                }
            }
            
        }else {
            // create the log directory.
            IOException ex = null;
            boolean created = false;
            try {
                created = privMkdirs(logDir);
            } catch (IOException ioe) {
                ex = ioe;
            }
            if (!created) {
                throw StandardException.newException(
                    SQLState.LOG_SEGMENT_NOT_EXIST, ex, logDir.getPath());
            }
            createDataWarningFile();
        }
    }
	
    /**
     * Create readme file in log directory warning users against touching
     *  any files in the directory
     * @throws StandardException
     */
    public void createDataWarningFile() throws StandardException {
        //Put a readme file in the log directory, alerting users to not 
        // touch or remove any of the files there 
        StorageFile fileReadMe = logStorageFactory.newStorageFile(
            LogFactory.LOG_DIRECTORY_NAME,
            PersistentService.DB_README_FILE_NAME);
        if (!privExists(fileReadMe)) {
            OutputStreamWriter osw = null;
            try {
                osw = privGetOutputStreamWriter(fileReadMe);
                osw.write(MessageService.getTextMessage(
                    MessageId.README_AT_LOG_LEVEL));
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

	/*
		Return the directory the log should go.

		<P> MT- read only
		@exception StandardException Derby Standard Error Policy
	*/
	public StorageFile getLogDirectory() throws StandardException
	{
		StorageFile logDir = null;

		logDir = logStorageFactory.newStorageFile( LogFactory.LOG_DIRECTORY_NAME);

        if (!privExists(logDir))
		{
			throw StandardException.newException(
                    SQLState.LOG_SEGMENT_NOT_EXIST, logDir.getPath());
		}

		return logDir;
	}

	public String getCanonicalLogPath()
	{
		if (logDevice == null)
			return null;
		else
		{
			try
			{
				return logStorageFactory.getCanonicalName();
			}
			catch (IOException ioe)
			{
				return null;
				// file not found
			}
		}
	}


	/**
		Return the control file name 

		<P> MT- read only
	*/
	private StorageFile getControlFileName() throws StandardException
	{
		return logStorageFactory.newStorageFile( getLogDirectory(), "log.ctrl");
	}

	/**
		Return the mirror control file name 

		<P> MT- read only
	*/
	private StorageFile getMirrorControlFileName() throws StandardException
	{
		return logStorageFactory.newStorageFile( getLogDirectory(), "logmirror.ctrl");
	}

	/**
		Given a log file number, return its file name 

		<P> MT- read only
	*/
	private StorageFile getLogFileName(long filenumber) throws StandardException
	{
		return logStorageFactory.newStorageFile( getLogDirectory(), "log" + filenumber + ".dat");
	}

	/*
		Find a checkpoint log record at the checkpointInstant

		<P> MT- read only
	*/
	private CheckpointOperation findCheckpoint(long checkpointInstant, FileLogger logger)
		 throws IOException, StandardException, ClassNotFoundException
	{
		StreamLogScan scan = (StreamLogScan)
			openForwardsScan(checkpointInstant, (LogInstant)null);

		// estimated size of a checkpoint log record, which contains 3 longs
		// and assorted other log record overhead
		Loggable lop = logger.readLogRecord(scan, 100);
								
		scan.close();

		if (lop instanceof CheckpointOperation)
			return (CheckpointOperation)lop;
		else
			return null;
	}


	/*
	 * Functions to help the Logger open a log scan on the log.
	 */

	/**
		Scan backward from start position.

		<P> MT- read only

		@exception IOException cannot access the log
		@exception StandardException Standard Derby error policy
	*/
	protected LogScan openBackwardsScan(long startAt, LogInstant stopAt)  
		 throws IOException, StandardException
	{
		checkCorrupt();

		// backward from end of log
		if (startAt == LogCounter.INVALID_LOG_INSTANT)
			return openBackwardsScan(stopAt);


		// ensure any buffered data is written to the actual file
		flushBuffer(LogCounter.getLogFileNumber(startAt),
			        LogCounter.getLogFilePosition(startAt));

		return new Scan(this, startAt, stopAt, Scan.BACKWARD);
	}

	/**
		Scan backward from end of log.
		<P> MT- read only

		@exception IOException cannot access the log
		@exception StandardException Standard Derby error policy
	*/
	protected LogScan openBackwardsScan(LogInstant stopAt)
		 throws IOException, StandardException
	{
		checkCorrupt();

		// current instant log instant of the next log record to be
		// written out, which is at the end of the log
		// ensure any buffered data is written to the actual file
		long startAt;
		synchronized (this)
		{
			// flush the whole buffer to ensure the complete
			// end of log is in the file.
			logOut.flushLogAccessFile();
			startAt = currentInstant();	
		}

		return new Scan(this, startAt, stopAt, Scan.BACKWARD_FROM_LOG_END);
	}

	/**
	  @see LogFactory#openFlushedScan
	  @exception StandardException Ooops.
	 */
	public ScanHandle openFlushedScan(DatabaseInstant start,int groupsIWant)
		 throws StandardException
	{
		return new FlushedScanHandle(this,start,groupsIWant);
	}
	


	/**
		Scan Forward from start position.

		<P> MT- read only

		@param startAt - if startAt == INVALID_LOG_INSTANT,
			start from the beginning of the log. Otherwise, start scan from startAt.
		@param stopAt - if not null, stop at this log instant (inclusive).
			Otherwise, stop at the end of the log

		@exception IOException cannot access the log
		@exception StandardException Standard Derby error policy
	*/
	protected LogScan openForwardsScan(long startAt, LogInstant stopAt)  
		 throws IOException, StandardException
	{
		checkCorrupt();

		if (startAt == LogCounter.INVALID_LOG_INSTANT)
		{
			startAt = firstLogInstant();
		}

		// ensure any buffered data is written to the actual file
		if (stopAt != null) {
			LogCounter stopCounter = (LogCounter) stopAt;
			flushBuffer(stopCounter.getLogFileNumber(),
						stopCounter.getLogFilePosition());
		} else {
			synchronized (this) {
				if (logOut != null)
					// flush to the end of the log
					logOut.flushLogAccessFile();
			}
		}

		return new Scan(this, startAt, stopAt, Scan.FORWARD);
	}

	/*
	 * Methods to help a log scan switch from one log file to the next 
	 */

	/**
		Open a log file and position the file at the beginning.
		Used by scan to switch to the next log file

		<P> MT- read only </p>

		<p> When the database is in slave replication mode only:
		Assumes that only recover() will call this method after
		initializeReplicationSlaveRole() has been called, and until slave
		replication has ended. If this changes, the current
		implementation will fail.</p>

		@exception StandardException Standard Derby error policy
		@exception IOException cannot access the log at the new position.
	*/
	protected StorageRandomAccessFile getLogFileAtBeginning(long filenumber)
		 throws IOException, StandardException
	{

        // <SLAVE REPLICATION CODE>
        //
        // When in slave replication mode, the recovery processing
        // will not start until after the first call to
        // switchLogRecord, at which time allowedToReadFileNumber will
        // be set to one less than the current log file number. The
        // call to switchLogRecord comes as a result of the
        // SlaveController appending log records received from the
        // master. This implies that the initialization steps (boot
        // and initializeReplicationSlaveRole) have completed.
        // 
        // Before recovery processing is started, log scans will be
        // allowed unrestricted access to the log files through this
        // method. This is needed because boot() and
        // initializeReplicationSlaveRole() use this method to find
        // the log end. Once the recovery thread is allowed to start
        // processing (i.e., allowedToReadFileNumber != -1), it will
        // use this method to read log files. From this point on, this
        // method will not return until allowedToReadFileNumber =>
        // filenumber. In other words, while in replication slave
        // mode, the method is blocking until allowedToReadFileNumber
        // is high enough to read the requested log file.
        //
        // Currently, only recover() uses this method (through
        // openForwardsScan) up to the point where the database has
        // been fully recovered. The database cannot fully recover
        // until it is no longer in slave mode. If this changes (i.e.
        // another thread also needs access to the log files while in
        // slave mode), this code will not work.
        if (inReplicationSlaveMode && (allowedToReadFileNumber != -1)) {
            synchronized (slaveRecoveryMonitor) {
                // Recheck inReplicationSlaveMode == true because it
                // may have changed while the thread was waiting.
                while (inReplicationSlaveMode &&
                       (filenumber > allowedToReadFileNumber)) {
                    if (replicationSlaveException != null) {
                        throw replicationSlaveException;
                    }
                    try {
                        slaveRecoveryMonitor.wait();
                    } catch (InterruptedException ie) {
                        InterruptStatus.setInterrupted();
                    }
                }
            }
        }
        // </SLAVE REPLICATION CODE>

        long instant = LogCounter.makeLogInstantAsLong(filenumber,
                                                       LOG_FILE_HEADER_SIZE);
        return getLogFileAtPosition(instant);
    }


    /**
        Get a read-only handle to the log file positioned at the stated position

        <P> MT- read only

        @return null if file does not exist or of the wrong format
        @exception IOException cannot access the log at the new position.
        @exception StandardException Standard Derby error policy
    */
    protected StorageRandomAccessFile getLogFileAtPosition(long logInstant)
         throws IOException, StandardException
	{
		checkCorrupt();

		long filenum = LogCounter.getLogFileNumber(logInstant);
		long filepos = LogCounter.getLogFilePosition(logInstant);

		StorageFile fileName = getLogFileName(filenum);
        if (!privExists(fileName))
		{
			if (SanityManager.DEBUG)
			{
				if (SanityManager.DEBUG_ON(LogToFile.DBG_FLAG))
					SanityManager.DEBUG(LogToFile.DBG_FLAG, fileName.getPath() + " does not exist");
			}

			return null;
		}


		StorageRandomAccessFile log = null;

		try
		{
            log = privRandomAccessFile(fileName, "r");

			// verify that the log file is of the right format
			if (!verifyLogFormat(log, filenum))
			{
				if (SanityManager.DEBUG)
				{
					if (SanityManager.DEBUG_ON(LogToFile.DBG_FLAG))
						SanityManager.DEBUG(LogToFile.DBG_FLAG, fileName.getPath() + " format mismatch");
				}

				log.close();
				log = null;
			}
			else
			{
				log.seek(filepos);
			}
		}
		catch (IOException ioe)
		{
			try
			{
				if (log != null)
				{
					log.close();
					log = null;
				}

				if (SanityManager.DEBUG)
				{
					SanityManager.THROWASSERT("cannot get to position " + filepos +
											  " for log file " + fileName.getPath(), ioe);
				}
			}
			catch (IOException ioe2)
			{}
			throw ioe;
		}

		return log;

	}

	/*
	** Methods of ModuleControl
	*/

	public boolean canSupport(Properties startParams)
	{
		String runtimeLogAttributes = startParams.getProperty(LogFactory.RUNTIME_ATTRIBUTES);
		if (runtimeLogAttributes != null) {
			if (runtimeLogAttributes.equals(LogFactory.RT_READONLY))
				return false;
		}

		return	true;
	}




	/**
		Boot up the log factory.
		<P> MT- caller provide synchronization

		@exception StandardException log factory cannot start up
	*/
	public void	boot(boolean create, Properties startParams) throws StandardException
	{
        // Is the database booted in replication slave mode?
        String mode = startParams.getProperty(SlaveFactory.REPLICATION_MODE);
        if (mode != null && mode.equals(SlaveFactory.SLAVE_MODE)) {
            inReplicationSlaveMode = true; 
            slaveRecoveryMonitor = new Object();
        } else if (mode != null && mode.equals(SlaveFactory.SLAVE_PRE_MODE)) {
            inReplicationSlavePreMode = true;
        }

		dataDirectory = startParams.getProperty(PersistentService.ROOT);
        
		logDevice = startParams.getProperty(Attribute.LOG_DEVICE);
        if( logDevice != null)
        {
			// in case the user specifies logDevice in URL form
			String logDeviceURL = null;
			try {
				URL url = new URL(logDevice);
				logDeviceURL = url.getFile();
			} catch (MalformedURLException ex) {}
			if (logDeviceURL != null)
				logDevice = logDeviceURL;
        }


        if(create) {
            getLogStorageFactory();
            createLogDirectory();
            
        } else {
            // check if the database is being restored from the backup,
            // if it is then restore the logs.
            if (!restoreLogs(startParams)) {
                // set the log storage factory.
                getLogStorageFactory();
                if (logDevice != null)
                {
                    // Make sure we find the log, do not assume 
                    // it is OK that the log is not there because 
                    // it could be a user typo(like when users edit
                    // service.properties to change the log device 
                    // while restoring from backups using OS copy.
                    StorageFile checklogDir =
                        logStorageFactory.newStorageFile( 
                                 LogFactory.LOG_DIRECTORY_NAME);
                    if (!privExists(checklogDir))
                    {
                        throw
                            StandardException.newException(
                            SQLState.LOG_FILE_NOT_FOUND, checklogDir.getPath());

                    }
                }
            }
        }
        		
		//if user does not set the right value for the log buffer size,
		//default value is used instead.
		logBufferSize =  PropertyUtil.getSystemInt(org.apache.derby.iapi.reference.Property.LOG_BUFFER_SIZE, 
												   LOG_BUFFER_SIZE_MIN, 
												   LOG_BUFFER_SIZE_MAX, 
												   DEFAULT_LOG_BUFFER_SIZE);
		jbmsVersion = getMonitor().getEngineVersion();

		
		String logArchiveMode = 
            startParams.getProperty(Property.LOG_ARCHIVE_MODE);
		logArchived = Boolean.valueOf(logArchiveMode).booleanValue();
		
		//get log factorty properties if any set in derby.properties
		getLogFactoryProperties(null);

		/* check if the storage factory supports write sync (rws and rwd). If
		 * so, use it unless derby.storage.fileSyncTransactionLog property is
		 * set true by user.
		 */

		if (logStorageFactory.supportsWriteSync())
        {
			//write sync can be used in the jvm that database is running on.
			//disable write sync if derby.storage.fileSyncTransactionLog is true
			isWriteSynced =
				!(PropertyUtil.getSystemBoolean(Property.FILESYNC_TRANSACTION_LOG));
        }
		else
		{
			isWriteSynced = false;
		}


        // If derby.system.durability=test is set,then set flag to 
        // disable sync of log records at commit and log file before 
        // data page makes it to disk
        if (Property.DURABILITY_TESTMODE_NO_SYNC.equalsIgnoreCase(
               PropertyUtil.getSystemProperty(Property.DURABILITY_PROPERTY)))
        {
		    // disable syncing of log.
		    logNotSynced = true;
  		    //if log not being synced;files shouldn't be open in write sync mode
		    isWriteSynced = false;	
		}
        else if (Performance.MEASURE)
        {
            // development build only feature, must by hand set the 
            // Performance.MEASURE variable and rebuild.  Useful during
            // development to compare/contrast effect of syncing, release
            // users can use the above relaxed durability option to disable
            // all syncing.  

            logNotSynced = 
                PropertyUtil.getSystemBoolean(
                    Property.STORAGE_LOG_NOT_SYNCED);

            if (logNotSynced)
            {
                isWriteSynced = false;
                Monitor.logMessage("Performance.logNotSynced = true");
            }
        }

		// try to access the log
		// if it doesn't exist, create it.
		// if it does exist, run recovery

		boolean createNewLog = create;

		if (SanityManager.DEBUG)
			SanityManager.ASSERT(fid != -1, "invalid log format Id");

		checkpointInstant = LogCounter.INVALID_LOG_INSTANT;
		try
		{
			StorageFile logControlFileName = getControlFileName();

			StorageFile logFile;

			if (!createNewLog)
			{
                if (privExists(logControlFileName))
				{
					checkpointInstant = 
                        readControlFile(logControlFileName, startParams);

					// in case system was running previously with 
                    // derby.system.durability=test then print a message 
                    // to the derby log
                    if (wasDBInDurabilityTestModeNoSync)
                    {
                        // print message stating that the database was
                        // previously atleast at one time running with
                        // derby.system.durability=test mode
                        Monitor.logMessage(MessageService.getTextMessage(
			           		MessageId.LOG_WAS_IN_DURABILITY_TESTMODE_NO_SYNC,
			           		Property.DURABILITY_PROPERTY,
                            Property.DURABILITY_TESTMODE_NO_SYNC));
                    }
						
					if (checkpointInstant == LogCounter.INVALID_LOG_INSTANT &&
										privExists(getMirrorControlFileName()))
                    {
						checkpointInstant =
                            readControlFile(
                                getMirrorControlFileName(), startParams);
                    }

				}
				else if (logDevice != null)
				{
					// Do not throw this error if logDevice is null because
					// in a read only configuration, it is acceptable
					// to not have a log directory.  But clearly, if the
					// logDevice property is set, then it should be there.
					throw StandardException.newException(
                            SQLState.LOG_FILE_NOT_FOUND,
                            logControlFileName.getPath());
				}

				if (checkpointInstant != LogCounter.INVALID_LOG_INSTANT)
					logFileNumber = LogCounter.getLogFileNumber(checkpointInstant);
				else
					logFileNumber = 1;

				logFile = getLogFileName(logFileNumber);

				// if log file is not there or if it is of the wrong format, create a
				// brand new log file and do not attempt to recover the database

                if (!privExists(logFile))
				{
					if (logDevice != null)
                    {
                        throw StandardException.newException(
                                SQLState.LOG_FILE_NOT_FOUND,
                                logControlFileName.getPath());
                    }

					logErrMsg(MessageService.getTextMessage(
                        MessageId.LOG_MAYBE_INCONSISTENT,
                        logFile.getPath()));

					createNewLog = true;
				}
				else if (!verifyLogFormat(logFile, logFileNumber))
				{
					Monitor.logTextMessage(MessageId.LOG_DELETE_INCOMPATIBLE_FILE, logFile);

					// blow away the log file if possible
                    if (!privDelete(logFile) && logFileNumber == 1)
                    {
                        logErrMsgForDurabilityTestModeNoSync();
						throw StandardException.newException(
                            SQLState.LOG_INCOMPATIBLE_FORMAT, dataDirectory);
                    }

					// If logFileNumber > 1, we are not going to write that 
                    // file just yet.  Just leave it be and carry on.  Maybe 
                    // when we get there it can be deleted.

					createNewLog = true;
				}
			}

			if (createNewLog)
			{
				// brand new log.  Start from log file number 1.

				// create or overwrite the log control file with an invalid
				// checkpoint instant since there is no checkpoint yet
				if (writeControlFile(logControlFileName,
									 LogCounter.INVALID_LOG_INSTANT))
				{
					firstLogFileNumber = 1;
					logFileNumber = 1;
					if (SanityManager.DEBUG)
					{
						if (SanityManager.DEBUG_ON(TEST_MAX_LOGFILE_NUMBER))
						{
							// set the value to be two less than max possible
							// log number, test case will perform some ops to 
							// hit the max number case.
							firstLogFileNumber = 
                                LogCounter.MAX_LOGFILE_NUMBER -2;

							logFileNumber = LogCounter.MAX_LOGFILE_NUMBER -2;
						}
					}
					logFile = getLogFileName(logFileNumber);

                    if (privExists(logFile))
					{
						// this log file maybe there because the system may have
						// crashed right after a log switch but did not write
                        // out any log record
						Monitor.logTextMessage(
                            MessageId.LOG_DELETE_OLD_FILE, logFile);

                        if (!privDelete(logFile))
                        {
                            logErrMsgForDurabilityTestModeNoSync();
							throw StandardException.newException(
                                    SQLState.LOG_INCOMPATIBLE_FORMAT,
                                    dataDirectory);
                        }
					}

					// don't need to try to delete it, we know it isn't there
                    firstLog = privRandomAccessFile(logFile, "rw");

					if (!initLogFile(firstLog, logFileNumber, LogCounter.INVALID_LOG_INSTANT))
                    {
						throw StandardException.newException(
                            SQLState.LOG_SEGMENT_NOT_EXIST, logFile.getPath());
                    }

					setEndPosition( firstLog.getFilePointer() );
					lastFlush = firstLog.getFilePointer();

                    //if write sync is true , prellocate the log file
                    //and reopen the file in rwd mode.
                    if(isWriteSynced)
                    {
                        //extend the file by wring zeros to it
                        preAllocateNewLogFile(firstLog);
                        firstLog.close();
                        firstLog = openLogFileInWriteMode(logFile);
                        //postion the log at the current log end postion
                        firstLog.seek(endPosition);
                    }

					if (SanityManager.DEBUG)
					{
						SanityManager.ASSERT(
                            endPosition == LOG_FILE_HEADER_SIZE,
                            "empty log file has wrong size");
					}
				}
				else
				{
					Monitor.logTextMessage(MessageId.LOG_CHANGED_DB_TO_READ_ONLY);
					Monitor.logThrowable(new Exception("Error writing control file"));
					// read only database
					ReadOnlyDB = true;
					logOut = null;
					firstLog = null;
				}

				recoveryNeeded = false;
			}
			else
			{
				// log file exist, need to run recovery
				recoveryNeeded = true;
			}

		}
		catch (IOException ioe)
		{
			throw Monitor.exceptionStartingModule(ioe);
		}
			
		// Number of the log file that can be created in Derby is increased from 
		// 2^22 -1 to 2^31 -1 in version 10.1. But if the database is running on
		// engines 10.1 or above on a  softupgrade  from versions 10.0 or
		// before, the max log file number  that can be created is  
		// still limited to 2^22 -1, because users can revert back to older  versions 
		// which does not have logic to handle a log file number greater than
		// 2^22-1. 

		// set max possible log file number to derby 10.0 limit, if the database is not 
		// fully upgraded to or created in version 10.1 or above. 
		if (!checkVersion(RawStoreFactory.DERBY_STORE_MAJOR_VERSION_10, 
						  RawStoreFactory.DERBY_STORE_MINOR_VERSION_1))
			maxLogFileNumber = LogCounter.DERBY_10_0_MAX_LOGFILE_NUMBER;

		bootTimeLogFileNumber = logFileNumber;
	} // end of boot

    private void getLogStorageFactory() throws StandardException
    {
        if( logDevice == null)
        {
            DataFactory df = (DataFactory) findServiceModule( this, DataFactory.MODULE);
            logStorageFactory = (WritableStorageFactory) df.getStorageFactory();
        }
        else
        {
            try
            {
                PersistentService ps = getMonitor().getServiceType(this);
                logStorageFactory = (WritableStorageFactory) ps.getStorageFactoryInstance( false, logDevice, null, null);
            }
            catch( IOException ioe)
            {
                if( SanityManager.DEBUG)
                    SanityManager.NOTREACHED();
                throw StandardException.newException( SQLState.LOG_FILE_NOT_FOUND, ioe, logDevice);
            }
        }
    } // end of getLogStorageFactory
  
	/**
	    Stop the log factory
		<P> MT- caller provide synchronization
		(RESOLVE: this should be called AFTER dataFactory and transFactory are
		stopped)
	*/
	public  void stop() {


		// stop our checkpoint 
		if (checkpointDaemon != null) {
			checkpointDaemon.unsubscribe(myClientNumber);
			checkpointDaemon.stop();
		}

		synchronized(this)
		{
			stopped = true;

			if (logOut != null) {
				try {
					logOut.flushLogAccessFile();
					logOut.close();
				}
				catch (IOException ioe) {}
				catch(StandardException se){}
				logOut = null;
			}
		}

	  
		if (SanityManager.DEBUG &&
            Performance.MEASURE &&
            mon_LogSyncStatistics)
		{
			Monitor.logMessage("number of times someone waited = " +
						   mon_numLogFlushWaits +
						   "\nnumber of times flush is called = " +
						   mon_flushCalls +
						   "\nnumber of sync is called = " +
						   mon_syncCalls +
						   "\ntotal number of bytes written to log = " +
						   LogAccessFile.mon_numBytesToLog +
						   "\ntotal number of writes to log file = " +
						   LogAccessFile.mon_numWritesToLog);
		}
		

		// delete obsolete log files,left around by earlier crashes
		if(corrupt == null && ! logArchived() && !keepAllLogs && !ReadOnlyDB)	
			deleteObsoleteLogfiles();

        if( logDevice != null)
            logStorageFactory.shutdown();
        logStorageFactory = null;
	}



	/* delete the log files, that might have been left around if we crashed
	 * immediately after the checkpoint before truncations of logs completed.
	 * see bug no: 3519 , for more details.
	 */

	private void deleteObsoleteLogfiles(){
		StorageFile logDir;
		//find the first  log file number that is  useful
		long firstLogNeeded = getFirstLogNeeded(currentCheckpoint);
        if (firstLogNeeded == -1)
			return;

        // when  backup is in progress, log files that are yet to
        // be copied to the backup should not be deleted,  even 
        // if they are not required  for crash recovery.
        if(backupInProgress) {
            long logFileNeededForBackup = logFileToBackup;
            // check if the log file number is yet to be copied 
            // to the backup is less than the log file required 
            // for crash recovery, if it is then make the first 
            // log file that should not be deleted is the log file 
            // that is yet to  be copied to the backup.  
            if (logFileNeededForBackup < firstLogNeeded)
                firstLogNeeded = logFileNeededForBackup;
        }

		try{
			logDir = getLogDirectory();
		}catch (StandardException se)
		{
			if (SanityManager.DEBUG)
				if (SanityManager.DEBUG_ON(LogToFile.DBG_FLAG))
					SanityManager.DEBUG(DBG_FLAG, "error opening log segment dir");
			return;
		}
			
		String[] logfiles = privList(logDir);
		if (logfiles != null)
		{
			StorageFile uselessLogFile = null;
			long fileNumber;
			for(int i=0 ; i < logfiles.length; i++)
			{
				// delete the log files that are not needed any more
				if(logfiles[i].startsWith("log") && logfiles[i].endsWith(".dat"))
				{
					fileNumber = Long.parseLong(logfiles[i].substring(3, (logfiles[i].length() -4)));
					if(fileNumber < firstLogNeeded )
					{
						uselessLogFile = logStorageFactory.newStorageFile(logDir, logfiles[i]);
						if (privDelete(uselessLogFile))
						{
							if (SanityManager.DEBUG)
							{
								if (SanityManager.DEBUG_ON(LogToFile.DBG_FLAG))
									SanityManager.DEBUG(DBG_FLAG, "truncating obsolete log file " + uselessLogFile.getPath());
							}
						}
						else
						{
							if (SanityManager.DEBUG)
							{
								if (SanityManager.DEBUG_ON(LogToFile.DBG_FLAG))
									SanityManager.DEBUG(DBG_FLAG, "Fail to truncate obsolete log file " + uselessLogFile.getPath());
							}
						}
					}
				}
			}
		}
	}

	/*
	 * Serviceable methods
	 */

	public boolean serviceASAP()
	{
		return false;
	}

	// @return true, if this work needs to be done on a user thread immediately
	public boolean serviceImmediately()
	{
		return false;
	}	


	public void getLogFactoryProperties(PersistentSet set)
		 throws StandardException
	{
		String lsInterval;
		String cpInterval;
		if(set == null)
		{
			lsInterval=PropertyUtil.getSystemProperty(org.apache.derby.iapi.reference.Property.LOG_SWITCH_INTERVAL);
			cpInterval=PropertyUtil.getSystemProperty(org.apache.derby.iapi.reference.Property.CHECKPOINT_INTERVAL);
		}else
		{
			lsInterval = PropertyUtil.getServiceProperty(set, org.apache.derby.iapi.reference.Property.LOG_SWITCH_INTERVAL);
			cpInterval = PropertyUtil.getServiceProperty(set, org.apache.derby.iapi.reference.Property.CHECKPOINT_INTERVAL);
		}

		/* log switch interval */
		if (lsInterval != null)
		{
			logSwitchInterval = Integer.parseInt(lsInterval);
					// make sure checkpoint and log switch interval are within range
			if (logSwitchInterval < LOG_SWITCH_INTERVAL_MIN)
				logSwitchInterval = LOG_SWITCH_INTERVAL_MIN;
			else if (logSwitchInterval > LOG_SWITCH_INTERVAL_MAX)
				logSwitchInterval = LOG_SWITCH_INTERVAL_MAX;
		}

		/* checkpoint interval */
		if (cpInterval != null)
		{
			checkpointInterval = Integer.parseInt(cpInterval);
			if (checkpointInterval < CHECKPOINT_INTERVAL_MIN)
				checkpointInterval = CHECKPOINT_INTERVAL_MIN;
			else if(checkpointInterval  > CHECKPOINT_INTERVAL_MAX)
				checkpointInterval = CHECKPOINT_INTERVAL_MAX;
		}
	}

	public int performWork(ContextManager context)
	{
		synchronized(this)
		{
			if (corrupt != null)
				return Serviceable.DONE; // don't do this again.
		}

		// check to see if checkpointInterval and logSwitchInterval has changed
		AccessFactory af = 
            (AccessFactory)getServiceModule(this, AccessFactory.MODULE);

		try
		{
			if (af != null)
			{
				TransactionController tc = null;
				try
				{
					tc = af.getAndNameTransaction(
                            context, AccessFactoryGlobals.SYS_TRANS_NAME);

					getLogFactoryProperties(tc);
				}
				finally
				{
					if (tc != null)
						tc.commit();
				}
			}

			// checkpoint will start its own internal transaction on the current
			// context.
			rawStoreFactory.checkpoint();
		}
		catch (StandardException se)
		{
			Monitor.logTextMessage(MessageId.LOG_CHECKPOINT_EXCEPTION);
			logErrMsg(se);
		}
        catch (ShutdownException shutdown)
        {
            // If we are shutting down, just ignore the error and let the 
            // system go down without printing errors to the log.
        }

		checkpointDaemonCalled = false;

		return Serviceable.DONE;
	}


	/*
	** Implementation specific methods
	*/

	/**
		Append length bytes of data to the log prepended by a long log instant
		and followed by 4 bytes of length information.

		<P>
		This method is synchronized to ensure log records are added sequentially
		to the end of the log.

		<P>MT- single threaded through this log factory.  Log records are
		appended one at a time.

		@exception StandardException Log Full.

	*/
	public long appendLogRecord(byte[] data, int offset, int length,
			byte[] optionalData, int optionalDataOffset, int optionalDataLength) 
		 throws StandardException
	{
        if (inReplicationSlavePreMode) {
            // Return the *current* end of log without adding the log
            // record to the log file. Effectively, this call to
            // appendLogRecord does not do anything
            return LogCounter.makeLogInstantAsLong(logFileNumber, endPosition);
        }

		long instant;
		boolean testIncompleteLogWrite = false;

		if (ReadOnlyDB)
        {
			throw StandardException.newException(
                SQLState.LOG_READ_ONLY_DB_UPDATE);
        }

		if (length <= 0)
        {
            throw StandardException.newException(
                    SQLState.LOG_ZERO_LENGTH_LOG_RECORD);
        }

		// RESOLVE: calculate checksum here
		if (SanityManager.DEBUG)
		{
			if (SanityManager.DEBUG_ON(TEST_LOG_INCOMPLETE_LOG_WRITE))
			{
				/// /// /// /// /// /// /// /// /// /// 
				//
				// go into this alternate route instead
				//
				/// /// /// /// /// /// /// /// /// /// 
				return logtest_appendPartialLogRecord(data, offset, length,
													  optionalData,
													  optionalDataOffset,
													  optionalDataLength);

			}

		}

		try
		{
			if (SanityManager.DEBUG)
			{
				if (SanityManager.DEBUG_ON(TEST_LOG_FULL))
					testLogFull();	// if log is 'full' this routine will throw an
								// exception 
			}

			synchronized (this)
			{
				// has someone else found a problem in the raw store?
				if (corrupt != null)
                {
					throw StandardException.newException(
                            SQLState.LOG_STORE_CORRUPT, corrupt);
                }

				if (logOut == null)
                {
					throw StandardException.newException(SQLState.LOG_NULL);
                }

				/*
				 * NOTE!!
				 *
				 * subclass which logs special record to the stream depends on
				 * the EXACT byte sequence of the following segment of code.  
				 * If you change this, not only will you need to write upgrade
				 * code for this class, you also need to find all the subclass
				 * which write log record to log stream directly to make sure 
				 * they are OK
				 */

				// see if the log file is too big, if it is, switch to the next
				// log file. account for an extra INT_LENGTH because switchLogFile()
                // writes an extra 0 at the end of the log. in addition, a checksum log record
                // may need to be written (see DERBY-2254).
                int     checksumLogRecordSize = logOut.getChecksumLogRecordSize();
				if ( (endPosition + LOG_RECORD_OVERHEAD + length + INT_LENGTH + checksumLogRecordSize) >=
                     LogCounter.MAX_LOGFILE_SIZE)
				{
					switchLogFile();

					// still too big??  Giant log record?
                    if ( (endPosition + LOG_RECORD_OVERHEAD + length + INT_LENGTH + checksumLogRecordSize) >=
                         LogCounter.MAX_LOGFILE_SIZE)
                    {
						throw StandardException.newException(
                                SQLState.LOG_EXCEED_MAX_LOG_FILE_SIZE, 
                                logFileNumber, 
                                endPosition, 
                                length, 
                                LogCounter.MAX_LOGFILE_SIZE);
                    }
				}

				//reserve the space for the checksum log record
				setEndPosition( endPosition + logOut.reserveSpaceForChecksum(length, logFileNumber,endPosition) );

				// don't call currentInstant since we are already in a
				// synchronzied block 
				instant = 
                    LogCounter.makeLogInstantAsLong(logFileNumber, endPosition);

                logOut.writeLogRecord(
                    length, instant, data, offset, 
                    optionalData, optionalDataOffset, optionalDataLength);

				if (optionalDataLength != 0) 
                {
					if (SanityManager.DEBUG)
					{
						if (optionalData == null)
							SanityManager.THROWASSERT(
							"optionalDataLength = " + optionalDataLength +
							" with null Optional data");

						if (optionalData.length <
											 (optionalDataOffset+optionalDataLength))
							SanityManager.THROWASSERT(
							"optionalDataLength = " + optionalDataLength +
							" optionalDataOffset = " + optionalDataOffset + 
							" optionalData.length = " + optionalData.length);
					}
				}

				setEndPosition( endPosition + (length + LOG_RECORD_OVERHEAD) );
			}
		}
		catch (IOException ioe)
		{
			throw markCorrupt(StandardException.newException(
                    SQLState.LOG_FULL, ioe));
		}

		return instant;
	}

	/*
	 * Misc private functions to access the log
	 */

	/**
		Get the current log instant - this is the log instant of the Next log
		record to be written out
		<P> MT - This method is synchronized to ensure that it always points to
		the end of a log record, not the middle of one. 
	*/
	protected synchronized long currentInstant()
	{
		return LogCounter.makeLogInstantAsLong(logFileNumber, endPosition);
	}

	protected synchronized long endPosition()
	{
		return endPosition;
	}

	/**
		Return the current log file number.

		<P> MT - this method is synchronized so that
		it is not in the middle of being changed by swithLogFile
	*/
	private synchronized long getLogFileNumber()
	{
		return logFileNumber;
	}

	/** 
		Get the first valid log instant - this is the beginning of the first
		log file

		<P>MT- synchronized on this
	*/
	private synchronized long firstLogInstant()
	{
		return LogCounter.makeLogInstantAsLong(firstLogFileNumber, LOG_FILE_HEADER_SIZE);
	}

	/**
		Flush the log such that the log record written with the instant 
        wherePosition is guaranteed to be on disk.

		<P>MT - only one flush is allowed to be taking place at any given time 
		(RESOLVE: right now it single thread thru the log factory while the log
		is frozen) 

		@exception StandardException cannot sync log file

	*/
	protected void flush(long fileNumber, long wherePosition) throws StandardException
	{

		long potentialLastFlush = 0;

		synchronized (this)
		{
			if (Performance.MEASURE)
				mon_flushCalls++;
			try
			{
				boolean waited;
				do
				{
					// THIS CORRUPT CHECK MUST BE FIRST, before any check that
					// sees if the log has already been flushed to this
					// point. This is based upon the assumption that every
					// dirty page in the cache must call flush() before it is
					// written out.  has someone else found a problem in the
					// raw store?

					if (corrupt != null)
					{
						throw StandardException.newException(
                                SQLState.LOG_STORE_CORRUPT, corrupt);
					}

					// now check if database is frozen
					while (isFrozen)
					{
						try 
						{
							wait();
						} 
						catch (InterruptedException ie) 
						{
                            InterruptStatus.setInterrupted();
						}
					}

					// if we are just testing to see to see the database is 
                    // frozen or corrupt (wherePosition == INVALID_LOG_INSTANT)
                    // then we can return now.
					// if the log file is already flushed up to where we are 
					// interested in, just return.
					if (wherePosition == LogCounter.INVALID_LOG_INSTANT ||
						fileNumber < logFileNumber ||
						wherePosition < lastFlush)
					{
						return;
					}

					// In non-replicated databases, if we are not
					// corrupt and we are in the middle of redo, we
					// know the log record has already been flushed
					// since we haven't written any log yet. If in
					// slave replication mode, however, log records
					// received from the master may have been
					// written to the log.
					if (recoveryNeeded && inRedo && !inReplicationSlaveMode) 
					{
						return;
					}


					if (SanityManager.DEBUG)
					{
						if (fileNumber > getLogFileNumber())
							SanityManager.THROWASSERT(
							  "trying to flush a file that is not there yet " +
								 fileNumber + " " + logFileNumber);

						if (SanityManager.DEBUG_ON(LogToFile.DBG_FLAG))
                        {
							SanityManager.DEBUG(
                                DBG_FLAG, "Flush log to " + wherePosition);
                        }
					}

					// There could be multiple threads who wants to flush the 
                    // log file, see if I can be the one.
					if (logBeingFlushed)
					{
						waited = true;
						try
						{
							if (Performance.MEASURE)
								mon_numLogFlushWaits++;
							wait();	// release log semaphore to let non-flushing
							// threads log stuff while all the flushing 
							// threads wait.

							// now we continue back to see if the sync
							// we waited for, flushed the portion
							// of log we are interested in.
						}
						catch (InterruptedException ie)
						{
                            InterruptStatus.setInterrupted();
						}
					}
					else
					{
						waited = false;

						// logBeingFlushed is false, I am flushing the log now.
						if(!isWriteSynced)
						{
							// Flush any data from the buffered log
							logOut.flushLogAccessFile();
						}else
						{
							//add active buffers to dirty buffer list
							//to flush to the disk.
							logOut.switchLogBuffer();
						}

						potentialLastFlush = endPosition; // we will flush to to the end

						// once logBeingFlushed is set, need to release
						// the logBeingFlushed flag in finally block.
						logBeingFlushed = true;	

                        // If in Replication Master mode - Notify the
                        // MasterFactory that log has been flushed to
                        // disk. At this point, we know that this is a
                        // "real" flush, not just a call to check
                        // whether the database is frozen/corrupted
                        // (i.e., wherePosition ==
                        // LogCounter.INVALID_LOG_INSTANT has already
                        // been checked)
                        if (inReplicationMasterMode) {
                            masterFactory.flushedTo(LogCounter.
                                       makeLogInstantAsLong(fileNumber,
                                                            wherePosition));
                        }
                    }

				} while (waited) ;
				// if I have waited, go down do loop again - hopefully,
				// someone else have already flushed it for me already.
			}
			catch (IOException ioe)
			{
				throw markCorrupt(StandardException.newException(
                    SQLState.LOG_CANNOT_FLUSH, 
                    ioe,
                    getLogFileName(logFileNumber).getPath()));
			}
            catch (NullPointerException e) {
                if (SanityManager.DEBUG) {
                    SanityManager.DEBUG_PRINT("DERBY-5003 [1]:", this.toString());
                }
                throw e;
            }
		} // unfreeze log manager to accept more log records

		boolean syncSuceed = false;
		try
		{
			if (SanityManager.DEBUG)
			{
				SanityManager.ASSERT(logBeingFlushed, 
									 "flushing log without logBeingFlushed set");
				SanityManager.ASSERT(potentialLastFlush > 0,
									 "potentialLastFlush not set");

				if (SanityManager.DEBUG_ON(TEST_LOG_FULL))
					testLogFull();

				if (SanityManager.DEBUG_ON(LogToFile.DBG_FLAG))
					SanityManager.DEBUG(DBG_FLAG, "Begin log sync...");
			}

			
			if (Performance.MEASURE)
				mon_syncCalls++;

			if (isWriteSynced)
			{
				//LogAccessFile.flushDirtyBuffers() will allow only one write
				//sync at a time, flush requests will get queued 
				logOut.flushDirtyBuffers();
			}
			else
			{
				if (!logNotSynced)
				    logOut.syncLogAccessFile();
			}

			syncSuceed = true;

			if (SanityManager.DEBUG)
			{
				if (SanityManager.DEBUG_ON(LogToFile.DBG_FLAG))
					SanityManager.DEBUG(DBG_FLAG, "end log sync.");
			}
		}
		catch (SyncFailedException sfe) 
		{
			throw markCorrupt(StandardException.newException(
                SQLState.LOG_CANNOT_FLUSH, 
                sfe,
                getLogFileName(logFileNumber).getPath()));
		}
		catch (IOException ioe)
		{
			throw markCorrupt(StandardException.newException(
                SQLState.LOG_CANNOT_FLUSH, 
                ioe,
                getLogFileName(logFileNumber).getPath()));
		}
        catch (NullPointerException e) {
            if (SanityManager.DEBUG) {
                SanityManager.DEBUG_PRINT("DERBY-5003 [2]", this.toString());
            }
            throw e;
        }
		finally
		{
			synchronized(this)
			{
				logBeingFlushed = false; // done flushing

				// update lastFlush under synchronized this instead of synchronized(logOut)
				if (syncSuceed)
				{
					lastFlush = potentialLastFlush;
				}


				// We may actually have flushed more than that because someone
				// may have done a logOut.flushBuffer right before the sync
				// call. But this is guarenteed to be flushed.
				notifyAll();
			}
		}

		
		// get checkpoint Daemon to work
		if ((logWrittenFromLastCheckPoint + potentialLastFlush) > checkpointInterval &&
					checkpointDaemon != null &&	!checkpointDaemonCalled && !inLogSwitch)
		{
			// following synchronized block is required to make 
			// sure only one checkpoint request get scheduled.
			synchronized(this)
			{
				// recheck if checkpoint is still required, it is possible some other
				// thread might have already scheduled a checkpoint and completed it. 
				if ((logWrittenFromLastCheckPoint + potentialLastFlush) > checkpointInterval &&
					checkpointDaemon != null &&	!checkpointDaemonCalled && !inLogSwitch)
				{
					checkpointDaemonCalled = true;
					checkpointDaemon.serviceNow(myClientNumber);
				}
			}

		}else
		{
			// switch the log if required, this case will occur 
			// if log switch interval is less than the checkpoint interval
			// other wise , checkpoint daemon would be doing log switches along
			// with the checkpoints.
			if (potentialLastFlush > logSwitchInterval &&
				!checkpointDaemonCalled && !inLogSwitch)
			{
				// following synchronized block is required to make sure only
				// one thread switches the log file at a time.
				synchronized(this)
				{
					// recheck if log switch is still required, it is possible some other
					// thread might have already switched the log file. 
					if (potentialLastFlush > logSwitchInterval &&
						!checkpointDaemonCalled && !inLogSwitch)
					{
						inLogSwitch = true;
						switchLogFile();
					}
				}
			}
		}
	}

    /**
     * Utility routine to call sync() on the input file descriptor.
     * <p> 
    */
    private void syncFile( StorageRandomAccessFile raf) 
        throws StandardException
    {
        for( int i=0; ; )
        {
            // 3311: JVM sync call sometimes fails under high load against NFS 
            // mounted disk.  We re-try to do this 20 times.
            try
            {
                raf.sync();

                // the sync succeed, so return
                break;
            }
            catch (IOException ioe)
            {
                i++;
                try
                {
                    // wait for .2 of a second, hopefully I/O is done by now
                    // we wait a max of 4 seconds before we give up
                    Thread.sleep(200);
                }
                catch( InterruptedException ie )
                {   
                    InterruptStatus.setInterrupted();
                }

                if( i > 20 )
                {
                    throw StandardException.newException(
                                SQLState.LOG_FULL, ioe);
                }
            }
        }
    }


	/**
	  Open a forward scan of the transaction log.

	  <P> MT- read only
	  @exception StandardException  Standard Derby exception policy
	*/
	public LogScan openForwardsFlushedScan(LogInstant startAt)
		 throws StandardException
	{
		checkCorrupt();

		// no need to flush the buffer as it's a flushed scan

		return new FlushedScan(this,((LogCounter)startAt).getValueAsLong());
	}


	/**
	  Get a forwards scan

	  @exception StandardException Standard Derby error policy
	  */
	public LogScan openForwardsScan(LogInstant startAt,LogInstant stopAt)
		 throws StandardException
	{
		try
		{
			long startLong;
		
			if (startAt == null)
				startLong = LogCounter.INVALID_LOG_INSTANT;
			else
				startLong = ((LogCounter)startAt).getValueAsLong();

			return openForwardsScan(startLong, stopAt);
		}

		catch (IOException ioe) 
		{
			throw markCorrupt(StandardException.newException(
                                        SQLState.LOG_IO_ERROR, ioe));
		}

	}

	public final boolean databaseEncrypted()
	{
		return databaseEncrypted;
	}


    /** {@inheritDoc} */
    public void setDatabaseEncrypted(boolean isEncrypted, boolean flushLog)
        throws StandardException
    {
        if (flushLog)  {
            flushAll();
        }
        databaseEncrypted = isEncrypted;
    }


    /*
     * set up a new log file to start writing 
     * the log records into the new log file 
     * after this call.
     *
     * <P>MT - synchronization provided by caller - RawStore boot,
     * This method is called while re-encrypting the database 
     * at database boot time.
     */
    public void startNewLogFile() throws StandardException
    {
        // switch the database to a new log file.
        switchLogFile();
    }


    /*
     * find if the checkpoint is in the last log file. 
     *
     * <P>MT - synchronization provided by caller - RawStore boot,
     * This method is called only if a crash occured while 
     * re-encrypting the database at boot time. 
     * @return <code> true </code> if if the checkpoint is 
     *                in the last log file, otherwise 
     *                 <code> false </code>.
     */
    public boolean isCheckpointInLastLogFile() 
        throws StandardException
    {
        // check if the checkpoint is done in the last log file. 
        long logFileNumberAfterCheckpoint = 
            LogCounter.getLogFileNumber(checkpointInstant) + 1;

        // check if there is a log file after
        // the log file that has the last 
        // checkpoint record.
        StorageFile logFileAfterCheckpoint = 
            getLogFileName(logFileNumberAfterCheckpoint);
        // System.out.println("checking " + logFileAfterCheckpoint);
        if (privExists(logFileAfterCheckpoint))
            return false;
        else 
            return true;
    }
    
    /*
     * delete the log file after the checkpoint. 
     *
     * <P>MT - synchronization provided by caller - RawStore boot,
     * This method is called only if a crash occured while 
     * re-encrypting the database at boot time. 
     */
    public void deleteLogFileAfterCheckpointLogFile() 
        throws StandardException
    {
        long logFileNumberAfterCheckpoint = 
            LogCounter.getLogFileNumber(checkpointInstant) + 1;

        StorageFile logFileAfterCheckpoint = 
            getLogFileName(logFileNumberAfterCheckpoint);

        // System.out.println("deleting " + logFileAfterCheckpoint);

        if (privExists(logFileAfterCheckpoint)) 
        {
            // delete the log file (this must have beend encrypted 
            // with the new key.
            if (!privDelete(logFileAfterCheckpoint))
            {
                // throw exception, recovery can not be performed
                // without deleting the log file encyrpted with new key.
                throw StandardException.newException(
                           SQLState.UNABLE_TO_DELETE_FILE, 
                           logFileAfterCheckpoint);
            }
        }
    }


	/**
		@see RawStoreFactory#encrypt
		@exception StandardException Standard Derby Error Policy
	 */
	public int encrypt(byte[] cleartext, int offset, int length, 
						  byte[] ciphertext, int outputOffset)
		 throws StandardException
	{
        return rawStoreFactory.encrypt(cleartext, offset, length, 
                                       ciphertext, outputOffset, false);
	}

	/**
		@see RawStoreFactory#decrypt
		@exception StandardException Standard Derby Error Policy
	 */
	public int decrypt(byte[] ciphertext, int offset, int length,
							 byte[] cleartext, int outputOffset)
		 throws StandardException
	{
		return rawStoreFactory.decrypt(ciphertext, offset, length, cleartext, outputOffset);
	}

	/**
		return the encryption block size used during encrypted db creation
	 */
        public int getEncryptionBlockSize()
	{
		return rawStoreFactory.getEncryptionBlockSize();
	}

	/**
	   returns the length that will make the data to be multiple of encryption
	   block size based on the given length. Block cipher algorithms like DES 
	   and Blowfish ..etc  require their input to be an exact multiple of the block size.
	*/
	public int getEncryptedDataLength(int length)
	{
		if ((length % getEncryptionBlockSize()) != 0)
		{
			return length + getEncryptionBlockSize() - (length % getEncryptionBlockSize());
		}

		return length;
	}



	/**
	  Get the instant of the first record which was not
	  flushed.

	  <P>This only works after running recovery the first time.
	  <P>MT - RESOLVE:
	  */
    public synchronized LogInstant getFirstUnflushedInstant()
	{
		if (SanityManager.DEBUG)
			SanityManager.ASSERT(logFileNumber > 0 && lastFlush > 0);

		return new LogCounter(logFileNumber,lastFlush);
	}

    public synchronized long getFirstUnflushedInstantAsLong() {
        if (SanityManager.DEBUG) {
            SanityManager.ASSERT(logFileNumber > 0 && lastFlush > 0);
        }
        return LogCounter.makeLogInstantAsLong(logFileNumber,lastFlush);
    }

	/**
	 * Backup restore - stop sending log record to the log stream
	 * @exception StandardException Standard Derby error policy
	 */
	public void freezePersistentStore() throws StandardException
	{
		// if I get into this synchronized block, I know I am not in the middle
		// of a write because writing to the log file is synchronized under this.
		synchronized(this)
		{
			isFrozen = true;
		}			
	}

	/**
	 * Backup restore - start sending log record to the log stream
	 * @exception StandardException Standard Derby error policy
	 */
	public void unfreezePersistentStore() throws StandardException
	{
		synchronized(this)
		{
			isFrozen = false;
			notifyAll();
		}			
	}

	/**
	 * Backup restore - is the log being archived to some directory?
	 * if log archive mode is enabled return true else false
	 */
	public boolean logArchived()
	{
		return logArchived;
	}

	/**
	   Check to see if a database has been upgraded to the required
	   level in order to use a store feature.
	   @param requiredMajorVersion  required database Engine major version
	   @param requiredMinorVersion  required database Engine minor version
	   @return True if the database has been upgraded to the required level, false otherwise.
	**/
	boolean checkVersion(int requiredMajorVersion, int requiredMinorVersion) 
	{
		if(onDiskMajorVersion > requiredMajorVersion )
		{
			return true;
		}
		else
		{
			if(onDiskMajorVersion == requiredMajorVersion &&  
			   onDiskMinorVersion >= requiredMinorVersion)
				return true;
		}
		
		return false;
	}


    /**
     *  Check to see if a database has been upgraded to the required
     *  level in order to use a store feature.
     *
     * @param requiredMajorVersion  required database Engine major version
     * @param requiredMinorVersion  required database Engine minor version
     * @param feature Non-null to throw an exception, null to return the 
     *                state of the version match.
     * @return <code> true </code> if the database has been upgraded to 
     *         the required level, <code> false </code> otherwise.
     * @exception  StandardException 
     *             if the database is not at the require version 
     *             when <code>feature</code> feature is 
     *             not <code> null </code>. 
     */
	public boolean checkVersion(int requiredMajorVersion, 
                                int requiredMinorVersion, 
                                String feature) throws StandardException 
    {
        
        boolean isRequiredVersion = 
            checkVersion(requiredMajorVersion, requiredMinorVersion);

        // if the database is not at the required version , throw exception 
        // if the feature is non-null . 
        if (!isRequiredVersion && feature != null) 
        {
            throw StandardException.newException(
                  SQLState.LANG_STATEMENT_UPGRADE_REQUIRED, feature,
                  ProductVersionHolder.simpleVersionString(onDiskMajorVersion, 
                                                           onDiskMinorVersion, 
                                                           onDiskBeta),
                  ProductVersionHolder.simpleVersionString(requiredMajorVersion, 
                                                           requiredMinorVersion, 
                                                           false));
        }

        return isRequiredVersion;
    }


	/*
	** Sending information to the user without throwing exception.
	** There are times when unusual external or system related things happen in
	** the log which the user may be interested in but which doesn't impede on
	** the normal operation of the store.  When such an event occur, just send
	** a message or a warning message to the user rather than throw an
	** exception, which will rollback a user transaction or crash the database.
	**
	** logErrMsg - sends a warning message to the user 
	*/


	/**
		Print error message to user about the log
		MT - not needed, informational only
	*/
	protected void logErrMsg(String msg)
	{
       	logErrMsgForDurabilityTestModeNoSync();
		Monitor.logTextMessage(MessageId.LOG_BEGIN_ERROR);
		Monitor.logMessage(msg);
		Monitor.logTextMessage(MessageId.LOG_END_ERROR);
	}

	/**
		Print error message to user about the log
		MT - not needed, informational only
	*/
	protected void logErrMsg(Throwable t)
	{
		logErrMsgForDurabilityTestModeNoSync();
		if (corrupt != null)
		{
			Monitor.logTextMessage(MessageId.LOG_BEGIN_CORRUPT_STACK);
			printErrorStack(corrupt);
			Monitor.logTextMessage(MessageId.LOG_END_CORRUPT_STACK);
		}

		if (t != corrupt)
		{
			Monitor.logTextMessage(MessageId.LOG_BEGIN_ERROR_STACK);
			printErrorStack(t);
			Monitor.logTextMessage(MessageId.LOG_END_ERROR_STACK);
		}
	}


    /**
     * In case of boot errors, and if database is either booted
     * with derby.system.durability=test or was previously at any time booted in
     * this mode, mention in the error message that the error is probably 
     * because the derby.system.durability was set. 
     * Dont want to waste time to resolve issues in such
     * cases
     * <p>
     * MT - not needed, informational only
     */
    private void logErrMsgForDurabilityTestModeNoSync()
    {
        if (logNotSynced || wasDBInDurabilityTestModeNoSync)
        {
            Monitor.logTextMessage(
                MessageId.LOG_DURABILITY_TESTMODE_NO_SYNC_ERR,
                Property.DURABILITY_PROPERTY,
                Property.DURABILITY_TESTMODE_NO_SYNC);
        }
    }

    /**
     * print stack trace from the Throwable including
     * its nested exceptions 
     * @param t trace starts from this error
     */
	private void printErrorStack(Throwable t)
	{
		ErrorStringBuilder esb = 
            new ErrorStringBuilder(Monitor.getStream().getHeader());
		esb.stackTrace(t);
        Monitor.logMessage(esb.get().toString());
        esb.reset();
    }


	/**
	 *  Testing support
	 */
	/** 
		Writes out a partial log record - takes the appendLogRecord.
		Need to shutdown the database before another log record gets written,
		or the database is not recoverable.
	*/
	private long logtest_appendPartialLogRecord(byte[] data, int offset, 
												int	length,
												byte[] optionalData, 
												int optionalDataOffset, 
												int optionalDataLength)
		throws StandardException
	{
		if (SanityManager.DEBUG)
		{
			int bytesToWrite = 1;

			String TestPartialLogWrite = PropertyUtil.getSystemProperty(TEST_LOG_PARTIAL_LOG_WRITE_NUM_BYTES);
			if (TestPartialLogWrite != null)
			{
				bytesToWrite = Integer.parseInt(TestPartialLogWrite);
			}

			Monitor.logMessage("TEST_LOG_INCOMPLETE_LOG_WRITE: writing " + bytesToWrite + 
				   " bytes out of " + length + " + " + LOG_RECORD_OVERHEAD + " log record");

			long instant;
			try
			{
								
				synchronized (this)
				{
					// reserve the space for the checksum log record
					// NOTE:  bytesToWrite include the log record overhead.
					setEndPosition( endPosition +
						logOut.reserveSpaceForChecksum(((length + LOG_RECORD_OVERHEAD) 
														< bytesToWrite ? length :
														(bytesToWrite - LOG_RECORD_OVERHEAD)),
													   logFileNumber,endPosition) );
					instant = currentInstant();

					//check if the length of the records to be written is 
					//actually smaller than the number of bytesToWrite 
					if(length + LOG_RECORD_OVERHEAD < bytesToWrite)
                    { setEndPosition( endPosition + (length + LOG_RECORD_OVERHEAD) ); }
					else
                    { setEndPosition( endPosition + bytesToWrite ); }

					while(true)		// so we can break out without returning out of
						// sync block...
					{
							if (bytesToWrite < 4)
							{
								int shift = 3;
								while(bytesToWrite-- > 0)
								{
									logOut.write((byte)((length >>> 8*shift) & 0xFF));
									shift--;
								}
								break;
							}

							// the length before the log record
							logOut.writeInt(length);
							bytesToWrite -= 4;

							if (bytesToWrite < 8)
							{
								int shift = 7;
								while(bytesToWrite-- > 0)
								{
									logOut.write((byte)((instant >>> 8*shift) & 0xFF));
									shift--;
								}
								break;
							}

							// the log instant
							logOut.writeLong(instant);
							bytesToWrite -= 8;

							if (bytesToWrite < length)
							{
								int dataLength = length - optionalDataLength;
								if(bytesToWrite < dataLength)
									logOut.write(data, offset,bytesToWrite);
								else
								{
									logOut.write(data, offset, dataLength);
									bytesToWrite -= dataLength ;
									if(optionalDataLength != 0 && bytesToWrite > 0)
										logOut.write(optionalData, optionalDataOffset, bytesToWrite);
								}
								break;
							}

							// the log data
							logOut.write(data, offset, length - optionalDataLength);
							//write optional data
							if(optionalDataLength != 0)
								logOut.write(optionalData, optionalDataOffset, optionalDataLength);

							bytesToWrite -= length;

							if (bytesToWrite < 4)
							{
								int shift = 3;
								while(bytesToWrite-- > 0)
								{
									logOut.write((byte)((length >>> 8*shift) & 0xFF));
									shift--;
								}
								break;
							}

							// the length after the log record
							logOut.writeInt(length);
							break;

						}

						// do make sure the partial write gets on disk by sync'ing it
						flush(logFileNumber, endPosition);

					}


			}
			catch (IOException ioe)
			{
				throw StandardException.newException(SQLState.LOG_FULL, ioe);
			}

			return instant;
		}
		return 0;
	}

	/**
		Simulate a log full condition

		if TEST_LOG_FULL is set to true, then the property
		TEST_RECORD_TO_FILL_LOG indicates the number of times this function is
		call before an IOException simulating a log full condition is raised.

		If TEST_RECORD_TO_FILL_LOG is not set, it defaults to 100 log record
	*/
	protected void testLogFull() throws IOException
	{
		if (SanityManager.DEBUG)
		{
			if (test_numRecordToFillLog < 0)
			{
				String RecordToFillLog = PropertyUtil.getSystemProperty(TEST_RECORD_TO_FILL_LOG);
				if (RecordToFillLog != null)
					test_numRecordToFillLog = Integer.parseInt(RecordToFillLog);
				else
					test_numRecordToFillLog = 100;
			}

			if (++test_logWritten > test_numRecordToFillLog)
				throw new IOException("TestLogFull " + test_numRecordToFillLog +
									  " written " + test_logWritten);

		}	
	}

	/**
	 * Get the log file to Simulate a log corruption 
	 * FOR UNIT TESTING USAGE ONLY 
	*/
	public StorageRandomAccessFile getLogFileToSimulateCorruption(long filenum) throws IOException, StandardException
	{
		if (SanityManager.DEBUG)
		{
			//long filenum = LogCounter.getLogFileNumber(logInstant);
			//			long filepos = LogCounter.getLogFilePosition(logInstant);
			StorageFile fileName = getLogFileName(filenum);
			StorageRandomAccessFile log = null;
			return privRandomAccessFile(fileName, "rw");
		}
		
		return null;

	}
        
        /**
         * Used to determine if the replication master mode has been started,
         * and the logging for unlogged operations needs to be enabled.
         *
         * @return true If the master replication mode is turned on and the 
         *              unlogged operations need to be logged.
         *         false If the master replication mode is turned off and the 
         *               unlogged operations need not be logged.
         */
        public boolean inReplicationMasterMode() {
            return inReplicationMasterMode;
        }
	

	/*********************************************************************
	 * Log Testing
	 * 
	 * Implementations may use these strings to simulate error conditions for
	 * testing purposes.
	 *
	 *********************************************************************/

	/**
	  Set to true if we want the checkpoint to only switch the log but not
	  actually do the checkpoint
	*/
	public static final String TEST_LOG_SWITCH_LOG = SanityManager.DEBUG ? "TEST_LOG_SWITCH_LOG" : null ;

	/**
	  Set to true if we want the upcoming log record to be only partially
	  written.  The database is corrupted if not immediately shutdown.
	  Set TEST_LOG_PARTIAL_LOG_WRITE_NUM_BYTES to the number of bytes to write
	  out, default is 1 byte.
	*/
	public static final String TEST_LOG_INCOMPLETE_LOG_WRITE = SanityManager.DEBUG ? "TEST_LOG_INCOMPLETE_LOG_WRITE" : null;

	/**
	  Set to the number of bytes we want the next log record to actually write
	  out, only used when TEST_LOG_INCOMPLETE_LOG_WRITE is on.  Default is 1
	  byte.
	*/
	public static final String TEST_LOG_PARTIAL_LOG_WRITE_NUM_BYTES = SanityManager.DEBUG ? "derbyTesting.unittest.partialLogWrite" : null;

	/**
	  Set to true if we want to simulate a log full condition
	*/
	public static final String TEST_LOG_FULL = 
        SanityManager.DEBUG ? "TEST_LOG_FULL" : null;

	/**
	  Set to true if we want to simulate a log full condition while switching log
	*/
	public static final String TEST_SWITCH_LOG_FAIL1 = 
        SanityManager.DEBUG ? "TEST_SWITCH_LOG_FAIL1" : null;
	public static final String TEST_SWITCH_LOG_FAIL2 = 
        SanityManager.DEBUG ? "TEST_SWITCH_LOG_FAIL2" : null;


	/**
	  Set to the number of log record we want to write before the log is
	  simulated to be full.
	*/
	public static final String TEST_RECORD_TO_FILL_LOG = 
        SanityManager.DEBUG ? "derbyTesting.unittest.recordToFillLog" : null;

	/**
	 * Set to true if we want to simulate max possible log file number is 
     * being used.
	*/
	public static final String TEST_MAX_LOGFILE_NUMBER = 
        SanityManager.DEBUG ? "testMaxLogFileNumber" : null;

	
	//enable the log archive mode
	public void enableLogArchiveMode() throws StandardException
	{

		//if the log archive mode is already enabled; thre is nothing to do
		if(!logArchived)
		{
			logArchived = true;
			AccessFactory af = 
            (AccessFactory)getServiceModule(this, AccessFactory.MODULE);

			if (af != null)
			{
				TransactionController tc = null;
				tc = af.getTransaction(
                        getContextService().getCurrentContextManager());
				tc.setProperty(Property.LOG_ARCHIVE_MODE , "true", true);
			}
		}
	}

	// disable the log archive mode
	public void disableLogArchiveMode() throws StandardException
	{
		AccessFactory af = 
            (AccessFactory)getServiceModule(this, AccessFactory.MODULE);
		if (af != null)
		{
			TransactionController tc = null;
			tc = af.getTransaction(getContextService().getCurrentContextManager());
			tc.setProperty(Property.LOG_ARCHIVE_MODE , "false", true);
		}
        logArchived = false;
	}

	//delete the online archived log files
	public void deleteOnlineArchivedLogFiles()
	{
		deleteObsoleteLogfiles();
	}


	/*
	 * Start the transaction log backup.  
     *
     * The transaction log is required to bring the database to the consistent 
     * state on restore. 
     *
	 * All the log files that are created after the backup starts 
	 * must be kept around until they are copied into the backup,
	 * even if there are checkpoints when backup is in progress. 
	 *
	 * Copy the log control files to the backup (the checkpoint recorded in the
     * control files is the backup checkpoint). Restore will use the checkpoint 
     * info in these control files to perform recovery to bring 
	 * the database to the consistent state.  
     *
     * Find first log file that needs to be copied into the backup to bring 
     * the database to the consistent state on restore. 
	 * 
     * In the end, existing log files that are needed to recover from the backup
     * checkpoint are copied into the backup, any log that gets generated after
     * this call are also copied into the backup after all the information 
     * in the data containers is written to the backup, when endLogBackup() 
     * is called.
	 *
     * @param toDir - location where the log files should be copied to.
     * @exception StandardException Standard Derby error policy
	 *
	 */
	public void startLogBackup(File toDir) throws StandardException
	{
		
		// synchronization is necessary to make sure NO parallel 
		// checkpoint happens when the current checkpoint information 
		// is being copied to the backup.

		synchronized(this) 
		{
			// wait until the thread that is doing the checkpoint completes it.
			while(inCheckpoint)
			{
				try
				{
					wait();
				}	
				catch (InterruptedException ie)
				{
                    InterruptStatus.setInterrupted();
				}	
			}
		
			backupInProgress = true;
		
			// copy the control files. 
			StorageFile fromFile;
			File toFile;
			// copy the log control file
			fromFile = getControlFileName();
			toFile = new File(toDir,fromFile.getName());
			if(!privCopyFile(fromFile, toFile))
			{
				throw StandardException.newException(
                    SQLState.RAWSTORE_ERROR_COPYING_FILE, fromFile, toFile);
			}

			// copy the log mirror control file
			fromFile = getMirrorControlFileName();
			toFile = new File(toDir,fromFile.getName());
			if(!privCopyFile(fromFile, toFile))
			{
				throw StandardException.newException(
                    SQLState.RAWSTORE_ERROR_COPYING_FILE, fromFile, toFile);
			}

			// find the first log file number that is active
			logFileToBackup = getFirstLogNeeded(currentCheckpoint);
		}

		// copy all the log files that have to go into the backup 
		backupLogFiles(toDir, getLogFileNumber() - 1);
	}	

	/*
	 * copy the log files into the given backup location
     *
     * @param toDir               - location to copy the log files to
     * @param lastLogFileToBackup - last log file that needs to be copied.
	 **/
	private void backupLogFiles(File toDir, long lastLogFileToBackup) 
        throws StandardException
	{

		while(logFileToBackup <= lastLogFileToBackup)
		{
			StorageFile fromFile = getLogFileName(logFileToBackup);
			File toFile = new File(toDir, fromFile.getName());
			if(!privCopyFile(fromFile, toFile))
			{
				throw StandardException.newException(
                    SQLState.RAWSTORE_ERROR_COPYING_FILE, fromFile, toFile);
			}
			logFileToBackup++;
		}
	}

	/*
	 * copy all the log files that have to go into the backup
	 * and mark that backup is compeleted. 
     *
     * @param toDir - location where the log files should be copied to.
     * @exception StandardException Standard Derby error policy
	 */
	public void endLogBackup(File toDir) throws StandardException
	{
		long lastLogFileToBackup;


        // Make sure all log records are synced to disk.  The online backup
        // copied data "through" the cache, so may have picked up dirty pages
        // which have not yet synced the associated log records to disk. 
        // Without this force, the backup may end up with page versions 
        // in the backup without their associated log records.
        flush(logFileNumber, endPosition);

		if (logArchived)
		{
			// when the log is being archived for roll-forward recovery
			// we would like to switch to a new log file.
			// otherwise during restore logfile in the backup could 
			// overwrite the more uptodate log files in the 
			// online log path. And also we would like to mark the end
			// marker for the log file other wise during roll-forward recovery,
			// if we see a log file with fuzzy end, we think that is the 
			// end of the recovery.
			switchLogFile();
			lastLogFileToBackup = getLogFileNumber() - 1;
		}
        else
		{
			// for a plain online backup partial filled up log file is ok, 
			// no need to do a log switch.
			lastLogFileToBackup = getLogFileNumber();	
		}

		// backup all the log that got generated after the backup started.
		backupLogFiles(toDir, lastLogFileToBackup);

		// mark that backup is completed.
		backupInProgress = false;
	}


	/*
	 * backup is not in progress any more, it failed for some reason.
	 **/
	public void abortLogBackup()
	{
		backupInProgress = false;
	}


	// Is the transaction in rollforward recovery
	public boolean inRFR()
	{
		/*
		 *Logging System does not differentiate between the
		 *crash-recovery and a rollforward recovery.
		 *Except in case of rollforward atttempt on 
		 *read only databases to check for pending Transaction.
		 *(See the comments in recovery() function)
		 */

		if(recoveryNeeded)
		{
			boolean readOnly = false;
			try{
				readOnly = !privCanWrite(getControlFileName());
			}catch(StandardException se)
			{
				//Exception should never have come here
				//because getControlFileName() is called 
				//earlier at boot time, if there were problems
				//it should have showed up earlier.							
				//We just ignore this error and hope that
				//datafactory must have market it as read only if that is the case.
			}

			readOnly = readOnly || (dataFactory == null ? false :dataFactory.isReadOnly());
			return !readOnly;
		}else{
			return false;
		}
	}

	/**	
	 *	redo a checkpoint during rollforward recovery
     * 
     * @throws org.apache.derby.iapi.error.StandardException 
     */
	public void checkpointInRFR(LogInstant cinstant, long redoLWM,
								long undoLWM, DataFactory df)
								throws StandardException
	{
		//sync the data
		df.checkpoint();

		//write the log control file; this will make sure that restart of the 
		//rollfoward recovery will start this log instant next time instead of
		//from the beginning.
		try{
			if (!writeControlFile(getControlFileName(), ((LogCounter)cinstant).getValueAsLong()))
			{
				throw StandardException.newException(
												 SQLState.LOG_CONTROL_FILE, getControlFileName());
			}
		}
		catch (IOException ioe)
		{
			throw markCorrupt(
                    StandardException.newException(SQLState.LOG_IO_ERROR, ioe));
		}
		//remove the stub files
		df.removeDroppedContainerFileStubs(new LogCounter(redoLWM));
		
		if (inReplicationSlaveMode) {
			truncateLog(LogCounter.getLogFileNumber(undoLWM));
		}
	}

    /**
     * Make this LogFactory pass log records to the MasterFactory
     * every time a log record is appended to the log on disk, and
     * notify the MasterFactory when a log disk flush has taken place.
     * @param masterFactory The MasterFactory service responsible for
     * controlling the master side replication behaviour.
     * @exception StandardException Standard Derby exception policy,
     * thrown on replication startup error. Will only be thrown if
     * replication is attempted started on a readonly database, i.e,
     * never thrown here.
     */
    public void startReplicationMasterRole(MasterFactory masterFactory) 
        throws StandardException {
        this.masterFactory = masterFactory;
        synchronized(this) {
            inReplicationMasterMode = true;
            logOut.setReplicationMasterRole(masterFactory);
        }
    }

    /**
     * Stop this LogFactory from passing log records to the
     * MasterFactory and from notifying the MasterFactory when a log
     * disk flush has taken place.
     */
    public void stopReplicationMasterRole() {
        inReplicationMasterMode = false;
        masterFactory = null;
        if(logOut != null) {
            logOut.stopReplicationMasterRole();
        }
    }

    /**
     * Stop the slave functionality for this LogFactory. Calling this
     * method causes the thread currently doing recovery to stop the
     * recovery process and throw a StandardException with SQLState
     * SHUTDOWN_DATABASE. This should only be done when the database
     * will be shutdown.
     * @throws StandardException Standard Derby exception policy
     * @see org.apache.derby.impl.db.SlaveDatabase
     */
    public void stopReplicationSlaveRole() throws StandardException {
        // Do not set inReplicationSlaveMode=false here because that
        // will let the thread currently doing recover complete the
        // boot process. Setting replicationSlaveException aborts the
        // boot process.
        if (!stopped) {
            flushAll();
        }
        replicationSlaveException =
                StandardException.newException(
                SQLState.SHUTDOWN_DATABASE);

        synchronized (slaveRecoveryMonitor) {
            slaveRecoveryMonitor.notify();
        }
    }

    /**
     * Used by LogAccessFile to check if it should take the
     * replication master role, and thereby send log records to the
     * MasterFactory.
     * @param log The LogAccessFile that will take the replication
     * master role iff this database is master.
     */
    protected void checkForReplication(LogAccessFile log) {
        if (inReplicationMasterMode) {
            log.setReplicationMasterRole(masterFactory);
        } else if (inReplicationSlaveMode) {
            log.setReplicationSlaveRole();
        }
    }

    /**
     * Initializes logOut so that log received from the replication
     * master can be appended to the log file.
     *
     * Normally, logOut (the file log records are appended to) is set
     * up as part of the recovery process. When the database is booted
     * in replication slave mode, however, recovery will not get to
     * the point where logOut is initialized until this database is no
     * longer in slave mode. Since logOut is needed to append log
     * records received from the master, logOut needs to be set up for
     * replication slave mode.
     *
     * This method finds the last log record in the log file with the
     * highest number. logOut is set up so that log records will be
     * appended to the end of that file, and the endPosition and
     * lastFlush variables are set to point to the end of the same
     * file. All this is normally done as part of recovery.
     *
     * After the first log file switch resulting from applying log
     * received from the master, recovery will be allowed to read up
     * to, but not including, the current log file which is the file
     * numbered logFileNumber.
     *
     * Note that this method must not be called until LogToFile#boot()
     * has completed. Currently, this is ensured because RawStore#boot
     * starts the SlaveFactory (in turn calling this method) after
     * LogFactory.boot() has completed. Race conditions for
     * logFileNumber may occur if this is changed.
     *
     * @exception StandardException Standard Derby error policy
     */
    public void initializeReplicationSlaveRole()
        throws StandardException{

        if (SanityManager.DEBUG) {
            SanityManager.ASSERT(inReplicationSlaveMode, 
                                 "This method should only be used when"
                                 + " in slave replication mode");
        }

        /*
         * Find the end of the log, i.e the highest log file and the
         * end position in that file
         */

        try {
            // Find the log file with the highest file number on disk
            while (getLogFileAtBeginning(logFileNumber+1) != null) {
                logFileNumber++;
            }

            // Scan the highest log file to find it's end.
            long startInstant =
                LogCounter.makeLogInstantAsLong(logFileNumber,
                                                LOG_FILE_HEADER_SIZE);
            long logEndInstant = LOG_FILE_HEADER_SIZE;

            StreamLogScan scanOfHighestLogFile =
                (StreamLogScan) openForwardsScan(startInstant,
                                                 (LogInstant)null);
            ArrayInputStream scanInputStream = new ArrayInputStream();
            while(scanOfHighestLogFile.getNextRecord(scanInputStream, null, 0)
                  != null){
                logEndInstant = scanOfHighestLogFile.getLogRecordEnd();
            }

            setEndPosition( LogCounter.getLogFilePosition(logEndInstant) );

            // endPosition and logFileNumber now point to the end of the
            // highest log file. This is where a new log record should be
            // appended.

            /*
             * Open the highest log file and make sure log records are
             * appended at the end of it
             */

            StorageRandomAccessFile logFile = null;
            if(isWriteSynced) {
                logFile = openLogFileInWriteMode(
                              getLogFileName(logFileNumber));
            } else {
                logFile = privRandomAccessFile(getLogFileName(logFileNumber),
                                               "rw");
            }
            logOut = new LogAccessFile(this, logFile, logBufferSize);

            lastFlush = endPosition;
            logFile.seek(endPosition); // append log records at the end of
            // the file

        } catch (IOException ioe) {
            throw StandardException.newException
                (SQLState.REPLICATION_UNEXPECTED_EXCEPTION, ioe);
        }
    }
    
    /**
     * Used to make the slave stop appending log records, complete recovery 
     * and boot the database.
     */
    public void failoverSlave() {
        if (!stopped) {
            try {
                flushAll();
            } catch (StandardException ex) {
            // do nothing
            }
        }
        inReplicationSlaveMode = false;
        synchronized (slaveRecoveryMonitor) {
            slaveRecoveryMonitor.notify();
        }
    }

	/**
	 *
	 * This function restores logs based on the  following attributes
	 * are specified on connection URL:
	 * Attribute.CREATE_FROM (Create database from backup if it does not exist)
	 * Attribute.RESTORE_FROM (Delete the whole database if it exists and then 
     * restore it from backup)
	 * Attribute.ROLL_FORWARD_RECOVERY_FROM:(Perform Rollforward Recovery;
	 * except for the log directory everything else is replaced by the copy from
	 * backup. log files in the backup are copied to the existing online log 
     * directory.
	 *
	 * In case of RESTORE_FROM, the whole database directory
	 * is removed in Directory.java while restoring service.properties
	 * so even the log directory is removed.
	 * In case of CREATE_FROM, log directory will not exist if
	 * we came so far because it should fail if a database already exists.
	 * In case ROLL_FORWARD_RECOVERY_FROM log directory should not be removed.
	 * So only thing that needs to be done here is create a
	 * a log directory if it does not exists and copy the 
	 * log files(including control files) that exists in the backup from which 
     * we are are trying to restore the database to the online log directory.
	 */
	private boolean restoreLogs(Properties properties) throws StandardException
	{

		String backupPath =null;
		boolean isCreateFrom = false; 
		boolean isRestoreFrom = false;

		//check if the user requested for restore/recovery/create from backup
		backupPath = properties.getProperty(Attribute.CREATE_FROM);
        if (backupPath != null) {
            isCreateFrom = true;
        } else {
			backupPath = properties.getProperty(Attribute.RESTORE_FROM);
            if (backupPath != null) {
                isRestoreFrom = true;
            } else {
                backupPath = properties.getProperty(
                                  Attribute.ROLL_FORWARD_RECOVERY_FROM);
                // if the backup is not NULL then it is a rollforward recovery.
            }
        }

		if(backupPath !=null)
		{
			if(!isCreateFrom){
				if(logDevice == null){
					/**
					 * In restoreFrom/rollForwardRecoveryFrom mode when no 
                     * logDevice on URL then the log is restored to the same 
                     * location where the log was when backup was taken.
					 * In createFrom mode behaviour is same as when create=true,
					 * i.e unless user specifies the logDevice on URL, log will
                     * be copied to the database home dir.
					 * Note: LOG_DEVICE_AT_BACKUP will get set if log is not in
					 * default location(db home). 
					 */
					logDevice = 
                        properties.getProperty(Property.LOG_DEVICE_AT_BACKUP);
				}
			}	 
        
            getLogStorageFactory();
			StorageFile logDir;
			logDir = logStorageFactory.newStorageFile( 
                             LogFactory.LOG_DIRECTORY_NAME);
				
			//remove the log directory in case of restoreFrom 
			//if it exist, this happens if the log device is on seperate
			//location than the db home.
			if (isRestoreFrom && logDevice != null)
			{
				if(!privRemoveDirectory(logDir))
				{
					//it may be just a file, try deleting it
					if(!privDelete(logDir))
                    {
						throw StandardException.newException(
                            SQLState.UNABLE_TO_REMOVE_DATA_DIRECTORY,
                            getLogDirPath( logDir));
                    }
				}
			}

            // if it is a create/restore from backup, 
            // create the log directory.
            if (isCreateFrom || isRestoreFrom) {
                createLogDirectory();
            }

			File backupLogDir = new File(backupPath, LogFactory.LOG_DIRECTORY_NAME);
			String[] logfilelist = privList(backupLogDir);
			if(logfilelist !=null)
			{
				for (int i = 0; i < logfilelist.length; i++) 
				{
					File blogFile = new File(backupLogDir, logfilelist[i]);
					StorageFile clogFile = logStorageFactory.newStorageFile(logDir, logfilelist[i]);
					if(!privCopyFile(blogFile , clogFile))
					{
						throw
							StandardException.newException(SQLState.UNABLE_TO_COPY_LOG_FILE, blogFile, clogFile);
					}
				}
			}else
			{
				throw StandardException.newException(SQLState.LOG_DIRECTORY_NOT_FOUND_IN_BACKUP,backupLogDir);
			}
			//we need to switch the log file after redo while
			//doing recovery from backups, otherwise we will 
			//be replacing updated log after a restore withe 
			// a log in the backup on next restore.
			logSwitchRequired = true;

            // log is restored from backup.
            return true;
		} else {
            // log is not restored from backup.
            return false;
        }
	}

	/*preallocate the given log File to the logSwitchInterval size;
	 *file is extended by writing zeros after the header till 
	 *the log file size the set by the user.
	 */	
	private void preAllocateNewLogFile(StorageRandomAccessFile log) throws IOException, StandardException
    {
        //preallocate a file by writing zeros into it . 

        if (SanityManager.DEBUG)
        {
            int currentPostion = (int)log.getFilePointer();
            SanityManager.ASSERT(currentPostion == LOG_FILE_HEADER_SIZE, 
                                 "New Log File Is not Correctly Initialized");
        }

        int amountToWrite = logSwitchInterval - LOG_FILE_HEADER_SIZE ;
        int bufferSize = logBufferSize * 2;
        byte[] emptyBuffer = new byte[bufferSize];
        int nWrites = amountToWrite/bufferSize;
        int remainingBytes = amountToWrite % bufferSize;
        
        try{
            while(nWrites-- > 0)
                log.write(emptyBuffer);

            if(remainingBytes !=0)
                log.write(emptyBuffer , 0 ,remainingBytes);

            //sync the file
            syncFile(log);
        }catch(IOException ie)
        {
            //ignore io exceptions during preallocations
            //because this more for performance improvements
            //system shoulf work fine even without preallocations. 

            //RESOLVE: If  the exception is because of no 
            //space, might be good idea to trigger a checkpoint.

            //In debug mode throw the exception 
            if (SanityManager.DEBUG)
            {
                throw ie;
            }
        }
    } // end of preAllocateNewLogFile


	/**
	 * open the given log file name for writes; if file can not be 
	 * be opened in write sync mode then disable the write sync mode and 
	 * open the file in "rw" mode.
 	 */
	private StorageRandomAccessFile openLogFileInWriteMode(StorageFile logFile) throws IOException
	{
        /* Some JVMs have an error in the code for write syncing. If this error
           is present we disable write syncing and fall back to doing writes
           followed by an explicit sync operation. See the details about this
           problem in the checkJvmSyncError() method. This code should be
           removed when we no longer support the JVMs with this problem. */
        if ( !jvmSyncErrorChecked ) {
            if ( checkJvmSyncError(logFile) ) {
                // To work around the problem of error for write syncing we
                // disable write sync and open the file in "rw" mode
                isWriteSynced = false;
                return privRandomAccessFile(logFile, "rw");
            }
        }

		StorageRandomAccessFile log = privRandomAccessFile(logFile, "rwd");
		return log ;
	}


    private String getLogDirPath( StorageFile logDir)
    {
        if( logDevice == null)
            return logDir.toString();
        return logDevice + logStorageFactory.getSeparator() + logDir.toString();
    } // end of getLogDirPath


    /**
     * In Java 1.4.2 and newer rws and rwd modes for RandomAccessFile
     * are supported. Still, on some JVMs (e.g. early versions of 1.4.2
     * and 1.5 on Mac OS and FreeBSD) the support for rws and rwd is
     * not working. This method attempts to detect this by opening an
     * existing file in "rws" mode. If this fails, Derby should fall
     * back to use "rw" mode for the log files followed by explicit
     * syncing of the log.
     *
     * Note: it is important to use "rws" for the test. If "rwd" is used, no
     * exception is thrown when opening the file, but the syncing does not
     * take place.
     *
     * For more details see DERBY-1 (and DERBY-2020).
     *
     * @param logFile information about the log file to be opened
     *
     * @return true if a JVM error is detected, false otherwise
     *
     * @exception StandardException Standard Derby exception
     */
    private boolean checkJvmSyncError(StorageFile logFile) throws IOException
    {
        boolean hasJvmSyncError = false;
        StorageRandomAccessFile rwsTest;

        // Normally this log file already exists but in case it does
        // not we open the file using "rw" mode. This is needed in
        // order to ensure that the file already exists when it is
        // opened in "rws" mode. This should succeed on all JVMs
        rwsTest = privRandomAccessFile(logFile, "rw");
        rwsTest.close();

        // Try to re-open the file in "rws" mode
        try{
            rwsTest = privRandomAccessFile(logFile, "rws");
            rwsTest.close();
        }
        catch (FileNotFoundException ex) {
            // Normally this exception should never occur. For some
            // reason currently on some Mac and FreeBSD JVM 1.4.2 and
            // 1.5 FileNotFoundException exception is thrown if a file
            // is opened in "rws" mode and if it already
            // exists. Please refer to DERBY-1 for more details on
            // this issue.  Temporary workaround to avoid this problem
            // is to make the logging system use file sync mechanism.
            logErrMsg("LogToFile.checkJvmSyncError: Your JVM seems to have a " +
                      "problem with implicit syncing of log files. Will use " +
                      "explicit syncing instead.");

            hasJvmSyncError = true;
        }

        // Set this variable to true to avoid that this method is called
        // multiple times
        jvmSyncErrorChecked = true;

        return hasJvmSyncError;
    }


    /*
        Following  methods require Priv Blocks to run under a security manager.
    */
	private int action;
	private StorageFile activeFile;
	private File toFile;
	private String activePerms;

    protected boolean privExists(StorageFile file)
    {
		return runBooleanAction(0, file);
	}

    protected boolean privDelete(StorageFile file)
    {
		return runBooleanAction(1, file);
    }

    private synchronized StorageRandomAccessFile privRandomAccessFile(StorageFile file, String perms)
        throws IOException
    {
		action = 2;
        activeFile = file;
        activePerms = perms;
        try
        {
            return (StorageRandomAccessFile) java.security.AccessController.doPrivileged(this);
        }
        catch (java.security.PrivilegedActionException pae)
        {
            throw (IOException) pae.getException();
        }
    }

    private synchronized OutputStreamWriter privGetOutputStreamWriter(StorageFile file)
        throws IOException
    {
        action = 10;
        activeFile = file;
        try
        {
            return (OutputStreamWriter) java.security.AccessController.doPrivileged(this);
        }
        catch (java.security.PrivilegedActionException pae)
        {
            throw (IOException) pae.getException();
        }
    }

    protected boolean privCanWrite(StorageFile file)
    {
		return runBooleanAction(3, file);
    }

    protected boolean privMkdirs(StorageFile file) throws IOException
    {
        this.action = 4;
        this.activeFile = file;
        try {
            return ((Boolean) AccessController.doPrivileged(this));
        } catch (PrivilegedActionException pae) {
            throw (IOException) pae.getCause();
        }
    }

	private synchronized String[] privList(File file)
    {
		action = 8;
        toFile = file;

        try
        {
			return (String[]) java.security.AccessController.doPrivileged(this);
		}
        catch (java.security.PrivilegedActionException pae)
        {
            return null;
        }
	}
    
	private synchronized String[] privList(StorageFile file)
    {
		action = 5;
        activeFile = file;

        try
        {
			return (String[]) java.security.AccessController.doPrivileged(this);
		}
        catch (java.security.PrivilegedActionException pae)
        {
            return null;
        }
	}


	private synchronized boolean privCopyFile(StorageFile from, File to)
            throws StandardException
	{
		action = 6;
		activeFile = from;
		toFile = to;
        try
        {
			return ((Boolean) java.security.AccessController.doPrivileged(this)).booleanValue();
		}
        catch (java.security.PrivilegedActionException pae)
        {
            if (pae.getCause() instanceof StandardException) {
                throw (StandardException)pae.getCause();
            }

            return false;
        }
	}

	private synchronized boolean privCopyFile(File from, StorageFile to)
	{
		action = 9;
		activeFile = to;
		toFile = from;
        try
        {
			return ((Boolean) java.security.AccessController.doPrivileged(this)).booleanValue();
		}
        catch (java.security.PrivilegedActionException pae)
        {
            return false;
        }	
	}

	private boolean privRemoveDirectory(StorageFile file)
	{
		return runBooleanAction(7, file);
	}


	private synchronized boolean runBooleanAction(int action, StorageFile file) {
		this.action = action;
		this.activeFile = file;

		try {
			return ((Boolean) java.security.AccessController.doPrivileged(this)).booleanValue();
		} catch (java.security.PrivilegedActionException pae) {
			return false;
		}
	}

    /** set the endPosition of the log and make sure the new position won't spill off the end of the log */
    private void    setEndPosition( long newPosition )
    {
		if (SanityManager.DEBUG)
        {
			SanityManager.ASSERT(newPosition < LogCounter.MAX_LOGFILE_SIZE,
							 "log file would spill past its legal end if the end were set to = " + newPosition );
		}

        endPosition = newPosition;
    }

	

    public final Object run() throws IOException, StandardException {
		switch (action) {
		case 0:
			// SECURITY PERMISSION - MP1
			return activeFile.exists();
		case 1:
			// SECURITY PERMISSION - OP5
           return activeFile.delete();
		case 2:
			// SECURITY PERMISSION - MP1 and/or OP4
			// dependening on the value of activePerms
            boolean exists = activeFile.exists();
            Object result = activeFile.getRandomAccessFile(activePerms);

            if (!exists) {
                activeFile.limitAccessToOwner();
            }

            return result;
		case 3:
			// SECURITY PERMISSION - OP4
			return activeFile.canWrite();
		case 4:
			// SECURITY PERMISSION - OP4
            boolean created = activeFile.mkdirs();

            if (created) {
                activeFile.limitAccessToOwner();
            }

            return created;
		case 5:
			// SECURITY PERMISSION - MP1
			return activeFile.list();
		case 6:
			// SECURITY PERMISSION - OP4 (Have to check these codes ??)
			return FileUtil.copyFile(logStorageFactory, activeFile, toFile);
		case 7:
			// SECURITY PERMISSION - OP4
            return !activeFile.exists() || activeFile.deleteAll();
        case 8:
            return toFile.list();
        case 9:
            return FileUtil.copyFile(logStorageFactory, toFile, activeFile);
        case 10:
        	return(new OutputStreamWriter(activeFile.getOutputStream(),"UTF8"));

		default:
			return null;
		}
	}
    
    @Override
    @SuppressWarnings("StringConcatenationInsideStringBufferAppend")
    public String toString() {
        StringBuilder sb = new StringBuilder();
        if (SanityManager.DEBUG) { // to reduce footprint in insane code
            sb.append("LogToFile: [\n");
            sb.append("  logOut=" + logOut + "\n");
            sb.append("  dataDirectory=" + dataDirectory + "\n");
            sb.append("  logStorageFactory=" + logStorageFactory + "\n");
            sb.append("  logBeingFlushed=" + logBeingFlushed + "\n");
            sb.append("  firstLog=" + firstLog + "\n");
            sb.append("  endPosition=" + endPosition + "\n");
            sb.append("  lastFlush=" + lastFlush + "\n");
            sb.append("  logFileNumber=" + logFileNumber + "\n");
            sb.append("  bootTimeLogFileNumber=" + bootTimeLogFileNumber + "\n");
            sb.append("  firstLogFileNumber=" + firstLogFileNumber + "\n");
            sb.append("  maxLogFileNumber=" + maxLogFileNumber + "\n");
            sb.append("  currentCheckpoint=" + currentCheckpoint + "\n");
            sb.append("  checkpointInstant=" + checkpointInstant + "\n");
            sb.append("  currentCheckpoint=" + currentCheckpoint + "\n");
            sb.append("  checkpointDaemon=" + checkpointDaemon + "\n");
            sb.append("  myClientNumber=" + myClientNumber + "\n");
            sb.append("  checkpointDaemonCalled=" + checkpointDaemonCalled + "\n");
            sb.append("  logWrittenFromLastCheckPoint=" + logWrittenFromLastCheckPoint + "\n");
            sb.append("  rawStoreFactory=" + rawStoreFactory + "\n");
            sb.append("  dataFactory=" + dataFactory + "\n");
            sb.append("  ReadOnlyDB=" + ReadOnlyDB + "\n");
            sb.append("  masterFactory=" + masterFactory + "\n");
            sb.append("  inReplicationMasterMode=" + inReplicationMasterMode + "\n");
            sb.append("  inReplicationSlaveMode=" + inReplicationSlaveMode + "\n");
            sb.append("  replicationSlaveException=" + replicationSlaveException + "\n");
            sb.append("  inReplicationSlaveMode=" + inReplicationSlaveMode + "\n");
            sb.append("  replicationSlaveException=" + replicationSlaveException + "\n");
            sb.append("  inReplicationSlavePreMode=" + inReplicationSlavePreMode + "\n");
            sb.append("  replicationSlaveException=" + replicationSlaveException + "\n");
            sb.append("  slaveRecoveryMonitor=" + slaveRecoveryMonitor + "\n");
            sb.append("  allowedToReadFileNumber=" + allowedToReadFileNumber + "\n");
            sb.append("  slaveRecoveryMonitor=" + slaveRecoveryMonitor + "\n");
            sb.append("  keepAllLogs=" + keepAllLogs + "\n");
            sb.append("  databaseEncrypted=" + databaseEncrypted + "\n");
            sb.append("  keepAllLogs=" + keepAllLogs + "\n");
            sb.append("  recoveryNeeded=" + recoveryNeeded + "\n");
            sb.append("  inCheckpoint=" + inCheckpoint + "\n");
            sb.append("  inRedo=" + inRedo + "\n");
            sb.append("  inLogSwitch=" + inLogSwitch + "\n");
            sb.append("  stopped=" + stopped + "\n");
            sb.append("  logDevice=" + logDevice + "\n");
            sb.append("  logNotSynced=" + logNotSynced + "\n");
            sb.append("  logArchived=" + logArchived + "\n");
            sb.append("  logSwitchRequired=" + logSwitchRequired + "\n");
            sb.append("  test_logWritten=" + test_logWritten + "\n");
            sb.append("  test_numRecordToFillLog=" + test_numRecordToFillLog + "\n");
            sb.append("  mon_flushCalls=" + mon_flushCalls + "\n");
            sb.append("  mon_syncCalls=" + mon_syncCalls + "\n");
            sb.append("  mon_numLogFlushWaits=" + mon_numLogFlushWaits + "\n");
            sb.append("  mon_LogSyncStatistics=" + mon_LogSyncStatistics + "\n");
            sb.append("  corrupt=" + corrupt + "\n");
            sb.append("  isFrozen=" + isFrozen + "\n");
            sb.append("  jbmsVersion=" + jbmsVersion + "\n");
            sb.append("  onDiskMajorVersion=" + onDiskMajorVersion + "\n");
            sb.append("  onDiskMinorVersion=" + onDiskMinorVersion + "\n");
            sb.append("  onDiskBeta=" + onDiskBeta + "\n");
            sb.append("  checksum=" + checksum + "\n");
            sb.append("  onDiskBeta=" + onDiskBeta + "\n");
            sb.append("  isWriteSynced=" + isWriteSynced + "\n");
            sb.append("  jvmSyncErrorChecked=" + jvmSyncErrorChecked + "\n");
            sb.append("  logFileToBackup=" + logFileToBackup + "\n");
            sb.append("  backupInProgress=" + backupInProgress + "]\n");
        }
        return sb.toString();
    }
    
    /**
     * Privileged lookup of the ContextService. Must be private so that user code
     * can't call this entry point.
     */
    private  static  ContextService    getContextService()
    {
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
     * Privileged Monitor lookup. Must be private so that user code
     * can't call this entry point.
     */
    private  static  ModuleFactory  getMonitor()
    {
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
    private  static  Object findServiceModule( final Object serviceModule, final String factoryInterface)
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
                         return Monitor.findServiceModule( serviceModule, factoryInterface );
                     }
                 }
                 );
        } catch (PrivilegedActionException pae)
        {
            throw StandardException.plainWrapException( pae );
        }
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
    
    /**
     * Privileged startup. Must be private so that user code
     * can't call this entry point.
     */
    private  static  boolean isFullUpgrade( final Properties startParams, final String oldVersionInfo )
        throws StandardException
    {
        try {
            return AccessController.doPrivileged
                (
                 new PrivilegedExceptionAction<Boolean>()
                 {
                     public Boolean run()
                         throws StandardException
                     {
                         return Monitor.isFullUpgrade( startParams, oldVersionInfo );
                     }
                 }
                 ).booleanValue();
        } catch (PrivilegedActionException pae)
        {
            throw StandardException.plainWrapException( pae );
        }
    }
}


