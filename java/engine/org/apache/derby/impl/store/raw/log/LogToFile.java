/*

   Derby - Class org.apache.derby.impl.store.raw.log.LogToFile

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

package org.apache.derby.impl.store.raw.log;

import org.apache.derby.iapi.services.diag.Performance;

import org.apache.derby.impl.store.raw.log.CheckpointOperation;
import org.apache.derby.impl.store.raw.log.LogCounter;
import org.apache.derby.impl.store.raw.log.LogRecord;
import org.apache.derby.impl.store.raw.log.StreamLogScan;

// need this to print nested exception that corrupts the database
import org.apache.derby.iapi.services.context.ErrorStringBuilder;

import org.apache.derby.iapi.services.info.ProductGenusNames;
import org.apache.derby.iapi.services.info.ProductVersionHolder;

import org.apache.derby.iapi.reference.MessageId;
import org.apache.derby.iapi.reference.Property;
import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.iapi.services.daemon.DaemonService;
import org.apache.derby.iapi.services.daemon.Serviceable;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.services.context.ContextService;
import org.apache.derby.iapi.services.context.ShutdownException;
import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.iapi.services.monitor.ModuleControl;
import org.apache.derby.iapi.services.monitor.ModuleSupportable;
import org.apache.derby.iapi.services.monitor.PersistentService;
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.services.io.Formatable;
import org.apache.derby.iapi.services.io.TypedFormat;
import org.apache.derby.iapi.services.io.FormatIdUtil;
import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.iapi.services.stream.HeaderPrintWriter;
import org.apache.derby.iapi.services.stream.PrintWriterGetHeader;
import org.apache.derby.iapi.services.stream.InfoStreams;
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
import org.apache.derby.iapi.store.raw.Transaction;
import org.apache.derby.iapi.store.raw.xact.RawTransaction;
import org.apache.derby.iapi.store.raw.xact.TransactionFactory;
import org.apache.derby.iapi.store.raw.data.DataFactory;
import org.apache.derby.iapi.services.property.PersistentSet;

import org.apache.derby.iapi.store.access.DatabaseInstant;
import org.apache.derby.catalog.UUID;
import org.apache.derby.iapi.services.uuid.UUIDFactory;
import org.apache.derby.iapi.services.property.PropertyUtil;
import org.apache.derby.iapi.reference.Attribute;
import org.apache.derby.iapi.services.io.FileUtil;
import org.apache.derby.iapi.util.ReuseFactory;

import org.apache.derby.io.StorageFactory;
import org.apache.derby.io.WritableStorageFactory;
import org.apache.derby.io.StorageFile;
import org.apache.derby.io.StorageRandomAccessFile;

import java.io.File; // Plain files are used for backups
import java.io.IOException;
import java.io.SyncFailedException;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;

import java.net.MalformedURLException;
import java.net.URL;

import java.util.Properties;
import java.util.Vector;
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

	@format_id	FILE_STREAM_LOG_FILE
	@purpose	The log control file contains information about which log files
	are present and where the last checkpoint log record is located.
	@upgrade	
	@disk_layout
	(pre-v15)
		int format id
		int log file version
		long the log instant (LogCounter) of the last completed checkpoint
	(v15 onward)
		int format id
		int obsolete log file version
		long the log instant (LogCounter) of the last completed checkpoint
		int JBMS version
		int checkpoint interval
		long spare (value set to 0)
		long spare (value set to 0)
		long spare (value set to 0)

	@end_format
	</PRE>	
	<HR WIDTH="100%">
	<PRE>

	Format of the log file

	@format_id	FILE_STREAM_LOG_FILE
	@purpose	The log file contains log record which record all the changes
	to the database.  The complete transaction log is composed of a series of
	log files.
	@upgrade
	@disk_layout
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
	@end_format
	</PRE>	
	<HR WIDTH="100%">
	<PRE>

	Format of the log record wrapper

	@format_id none.  The format is implied by the FILE_STREAM_LOG_FILE
	@purpose	The log record wrapper provides information for the log scan.
	@upgrade
	@disk_layout
		length(int) length of the log record (for forward scan)
		instant(long) LogInstant of the log record
		logRecord(byte[length]) byte array that is written by the FileLogger
		length(int) length of the log record (for backward scan)
	@end_format
	</PRE>


	<P>Multithreading considerations:<BR>
	Log Factory must be MT-safe.
*/

public class LogToFile implements LogFactory, ModuleControl, ModuleSupportable,
								  Serviceable, java.security.PrivilegedExceptionAction
{

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

	protected long		     endPosition = -1; // end position of the current log file
	long					 lastFlush = 0;	// the position in the current log
											// file that has been flushed to disk

	long					 logFileNumber = -1; // current log file number
								// other than during boot and recovery time,
								// logFileNumber is only changed by
								// switchLogFile, which is synchronized.
								// 
								// MT - Anyone accessing this number should
								// synchronized on this if the current log file
								// must not be changed. If not synchronized,
								// the log file may have been switched.

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
								// use this only after recovery is finished

	protected DataFactory dataFactory;
								// use this only after revocery is finished

	protected boolean	ReadOnlyDB;	// true if this db is read only, i.e, cannot
								// append log records


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

    // debug only flag - disable syncing of log file for debugging performance.
    private boolean logNotSynced = false;

	private boolean logArchived = false;
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
	 * sync(rws mode)  mechanism can be used corretly.
	 * Default in JVMS >= jdk1.4.2 is write sync(see the boot method for jvm checks).
	 *
	 * Write sync mechanism support is added  for performance reasons. 
	 * On commits, logging system has to make sure the log for committed
	 * transaction is on disk. With out write  sync , log is written to the 
	 * disk and then fsync() is used on commits to make log is written to the 
	 * disk for sure. On most of the OS , fsync() calls are expensive. 
	 * On heavey commit oriented systems , file sync make the system run slow. 
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
		Once the log factory is makred as corrupt then the raw sto
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

		@exception StandardException Standard Cloudscape error policy
	*/
	public void recover(
    RawStoreFactory     rsf, 
    DataFactory         df, 
    TransactionFactory  tf)
		 throws StandardException
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(rsf != null, "raw store factory == null");
			SanityManager.ASSERT(df != null,  "data factory == null");
		}

		checkCorrupt();

		rawStoreFactory = rsf;
		dataFactory     = df;

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
				// logFileNumber is determined.  LogOut is not set up.
				// LogFileNumber is the log file the latest checkpoint lives in,
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
							logFileNumber = 
                                Integer.valueOf(beginLogFileNumber).intValue();
                        }
						else
                        {
							logFileNumber = 1;
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
                            logFileNumber, LOG_FILE_HEADER_SIZE);

					// no checkpoint, start redo from the beginning of the 
                    // file - assume this is the first log file
					firstLogFileNumber = logFileNumber;

					redoScan = (StreamLogScan) 
                        openForwardsScan(start, (LogInstant)null);
				}

				// open a transaction that is used for redo and rollback
				RawTransaction recoveryTransaction =
                    tf.startTransaction(
                        rsf,
                        ContextService.getFactory().getCurrentContextManager(),
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

					try
					{
                        theLog =   privRandomAccessFile(logFile, "rw");
					}
					catch (IOException ioe)
					{
						theLog = null;
					}

                    if (theLog == null || !privCanWrite(logFile))
					{
						if (theLog != null)
							theLog.close();

						theLog = null;

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
						endPosition = theLog.getFilePointer();
						lastFlush   = endPosition;
						
						//if write sync is true , prellocate the log file
						//and reopen the file in rws mode.
						if(isWriteSynced)
						{
							//extend the file by wring zeros to it
							preAllocateNewLogFile(theLog);
							theLog.close();
							theLog=  privRandomAccessFile(logFile, "rws");
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
						try
						{
							if(isWriteSynced)
								theLog = privRandomAccessFile(logFile, "rws");
							else
								theLog = privRandomAccessFile(logFile, "rw");
						}
						catch (IOException ioe)
						{
							theLog = null;
						}
                        if (theLog == null || !privCanWrite(logFile))
						{
							if (theLog != null)
								theLog.close();
							theLog = null;

							ReadOnlyDB = true;
						}
					}

					if (!ReadOnlyDB)
					{
						endPosition = LogCounter.getLogFilePosition(logEnd);

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
								logFile, new Long(endPosition), new Long(eof));

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
					logOut = new LogAccessFile(theLog, logBufferSize);
				
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

					tf.rollbackAllTransactions(recoveryTransaction, rsf);

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

                tf.handlePreparedXacts(rsf);

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

						if (needCheckpoint && !checkpoint(rsf, df, tf, false))
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
		// setup checktpoint daemon
		/////////////////////////////////////////////////////////////
		checkpointDaemon = rawStoreFactory.getDaemon();
		if (checkpointDaemon != null)
        {
			myClientNumber =
                checkpointDaemon.subscribe(this, true /*onDemandOnly */);
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
			---A--->|<-------B--------->|<-------------C-----------
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
	*/
	public boolean checkpoint(RawStoreFactory rsf,
							  DataFactory df,
							  TransactionFactory tf, 
							  boolean wait)
		 throws StandardException
	{

		// call checkpoint with no pre-started transaction
		boolean done = checkpointWithTran(null, rsf, df, tf);

		//above checpoint call will return 'false'  without
		//performing the checkpoint if some other  thread is doing checkpoint. 
		//In  cases like backup it is necesary to wait for the 
		//checkpoint to complete before copying the files. 'wait' flag get passed 
		//in as 'true' by  such cases.
		//When wait flag is true, we will wait here until the other thread which
		//is actually doing the the checkpoint completes.
 
		if(!done && wait)
		{
			synchronized(this)
			{
				//wait until the thread that is doing the checkpoint completes it.
				while(inCheckpoint)
				{
					try
					{
						wait();
					}	
					catch (InterruptedException ie)
					{
						throw StandardException.interrupt(ie);
					}	
				}
				done = true;
			}
		}

		return done;
	}


	/**
		checkpoint with pre-start transaction

		@exception StandardException Cloudscape Standard Error Policy 
	*/
	protected boolean checkpointWithTran(RawTransaction cptran, 
							   RawStoreFactory rsf,
							   DataFactory df,
							   TransactionFactory tf)
		 throws StandardException
	{
		boolean proceed = true;
		LogInstant redoLWM;

		// we may be called to stop the database after a bad error, make sure
		// logout is set
		if (logOut == null)
		{
			return false;
		}

		long approxLogLength;

		synchronized (this)
		{
			// has someone else found a problem in the raw store?  
			if (corrupt != null)
            {
				throw StandardException.newException(SQLState.LOG_STORE_CORRUPT, corrupt);
            }

			// if another checkpoint is in progress, don't do anything
			if (inCheckpoint == true)
				proceed = false;
			else
				inCheckpoint = true;

			approxLogLength = endPosition; // current end position

			// don't return from inside of a sync block
		}

		if (!proceed)
		{
			return false;
		}

		// needCPtran == true if we are not supplied with a pre-started transaction
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
				//checkpoint, set the amount of log written from last checkpoint to zero.
				logWrittenFromLastCheckPoint = 0;
			}else
			{
				//checkpoint is happening without the log switch,
				//in the middle of a log file. Amount of log written already for
				//the current log file should not be included in caluculation 
				//of when next check point is due. By assigning the negative
				//value of amount of log writtent for this file. Later it will
				//be subtracted when we switch the log file or while calculating whether 
				//we are due a for checkpoint a flush time.
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
				ContextService.getFactory().getCurrentContextManager());

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

			//delete the committted container drop stubs that are no longer
			//required during recovery.
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

		@exception StandardException Standard Cloudscape error policy
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

		@exception StandardException Standard Cloudscape error policy
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
		@param version the log version 
		@param number the log file number
		@return true if the log file is of the current version and of the
		correct format

		@exception StandardException Standard Cloudscape error policy
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

		@exception StandardException Standard Cloudscape error policy
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

		@param newLog the new log file to be initialized
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
	private void switchLogFile() throws StandardException
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
					throw StandardException.interrupt(ie);
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

			if (logFileNumber+1 >= LogCounter.MAX_LOGFILE_NUMBER)
            {
				throw StandardException.newException(
                        SQLState.LOG_EXCEED_MAX_LOG_FILE_NUMBER, 
                        new Long(LogCounter.MAX_LOGFILE_NUMBER)); 
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
					logOut.writeInt(0);

					endPosition += 4;
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

					endPosition = newLog.getFilePointer();
					lastFlush = endPosition;
					
					if(isWriteSynced)
					{
						//extend the file by wring zeros to it
						preAllocateNewLogFile(newLog);
						newLog.close();
						newLog=  privRandomAccessFile(newLogFile, "rws");
						newLog.seek(endPosition);
					}

					logOut = new LogAccessFile(newLog, logBufferSize);
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
					newLogFile = null;

					logErrMsg(MessageService.getTextMessage(
                        MessageId.LOG_CANNOT_CREATE_NEW,
                        newLogFile.getPath()));
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
			
			inLogSwitch = false;
		}
		// unfreezes the log
	}

	/**
		Flush all unwritten log record up to the log instance indicated to disk
		without syncing.

		<P>MT - not needed, wrapper method

		@param where flush log up to here

		@exception IOException Failed to flush to the log
	*/
	private void flushBuffer(long fileNumber, long wherePosition)
		throws IOException
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
		long oldFirstLog;
		long firstLogNeeded;

		if (keepAllLogs)
			return;
		if ((firstLogNeeded = getFirstLogNeeded(checkpoint))==-1)
			return;
		
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

   

	private long getFirstLogNeeded(CheckpointOperation checkpoint){

		long firstLogNeeded;

		// one truncation at a time
		synchronized (this)
		{
			firstLogNeeded = LogCounter.getLogFileNumber(checkpoint.undoLWM());

			if (SanityManager.DEBUG)
			{
				if (SanityManager.DEBUG_ON(LogToFile.DBG_FLAG))
					SanityManager.DEBUG(DBG_FLAG, "truncatLog: undoLWM firstlog needed " + firstLogNeeded);
			}

			if (SanityManager.DEBUG)
			{
				if (SanityManager.DEBUG_ON(LogToFile.DBG_FLAG))
				{
				SanityManager.DEBUG(DBG_FLAG, "truncatLog: checkpoint truncationLWM firstlog needed " + firstLogNeeded);
				SanityManager.DEBUG(DBG_FLAG, "truncatLog: firstLogFileNumber = " + firstLogFileNumber);
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
        the condrol data written at last check point.

		see comment at beginning of file for log control file format.

		<P> MT- synchronized by caller
	*/
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
		if (onDiskBeta) flags |= IS_BETA_FLAG;
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
			if (Monitor.isFullUpgrade(startParams,
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

	/*
		Return the directory the log should go.

		<P> MT- read only
		@exception StandardException Cloudscape Standard Error Policy
	*/
	public StorageFile getLogDirectory() throws StandardException
	{
		StorageFile logDir = null;

		logDir = logStorageFactory.newStorageFile( LogFactory.LOG_DIRECTORY_NAME);

        if (!privExists(logDir) && !privMkdirs(logDir))
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
		@exception StandardException Standard Cloudscape error policy
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
		@exception StandardException Standard Cloudscape error policy
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
		@exception StandardException Standard Cloudscape error policy
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

		<P> MT- read only

		@exception StandardException Standard Cloudscape error policy
		@exception IOException cannot access the log at the new position.
	*/
	protected StorageRandomAccessFile getLogFileAtBeginning(long filenumber)
		 throws IOException, StandardException
	{
		long instant = LogCounter.makeLogInstantAsLong(filenumber,
													   LOG_FILE_HEADER_SIZE);
		return getLogFileAtPosition(instant);
	}


	/**
		Get a read-only handle to the log file positioned at the stated position

		<P> MT- read only

		@return null if file does not exist or of the wrong format
		@exception IOException cannot access the log at the new position.
		@exception StandardException Standard Cloudscape error policy
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

		//check whether we are restoring from backup
		restoreLogs(startParams);
		
		//if user does not set the right value for the log buffer size,
		//default value is used instead.
		logBufferSize =  PropertyUtil.getSystemInt(org.apache.derby.iapi.reference.Property.LOG_BUFFER_SIZE, 
												   LOG_BUFFER_SIZE_MIN, 
												   LOG_BUFFER_SIZE_MAX, 
												   DEFAULT_LOG_BUFFER_SIZE);

        if( logStorageFactory == null)
            getLogStorageFactory();
		if (logDevice != null)
		{
			// in case the user specifies logDevice in URL form
			String logDeviceURL = null;
			try {
				URL url = new URL(logDevice);
				logDeviceURL = url.getFile();
			} catch (MalformedURLException ex) {}
			if (logDeviceURL != null)
				logDevice = logDeviceURL;

			// Make sure we find the log, do not assume it is OK that the log
			// is not there because it could be a user typo.
			if (!create)
			{
				StorageFile checklogDir =
					logStorageFactory.newStorageFile( LogFactory.LOG_DIRECTORY_NAME);

                if (!privExists(checklogDir))
				{

					throw
                        StandardException.newException(
                            SQLState.LOG_FILE_NOT_FOUND, checklogDir.getPath());

				}
			}
		}


		jbmsVersion = Monitor.getMonitor().getEngineVersion();

		String dataEncryption = 
            startParams.getProperty(Attribute.DATA_ENCRYPTION);

		databaseEncrypted = Boolean.valueOf(dataEncryption).booleanValue();

		
		String logArchiveMode = 
            startParams.getProperty(Property.LOG_ARCHIVE_MODE);
		logArchived = Boolean.valueOf(logArchiveMode).booleanValue();
		
		//get log factorty properties if any set in derby.properties
		getLogFactoryProperties(null);

		/* check if the storage factory supports write sync(rws).  If so, use it unless
		 * derby.storage.fileSyncTransactionLog property is set true by user.
		 */

		if (logStorageFactory.supportsRws())
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

		if (Performance.MEASURE)
		{
			// debug only flag - disable syncing of log.
			logNotSynced = 
				PropertyUtil.getSystemBoolean(Property.STORAGE_LOG_NOT_SYNCED);

			if (logNotSynced)
			{
				Monitor.logMessage("logNotSynced = true");
				//if log is Not being synced;files should not be open in write sync mode
				isWriteSynced = false;
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
					checkpointInstant = readControlFile(logControlFileName, startParams);
					if (checkpointInstant == LogCounter.INVALID_LOG_INSTANT &&
										getMirrorControlFileName().exists())
						checkpointInstant =
									readControlFile(getMirrorControlFileName(), startParams);

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
						throw StandardException.newException(
                            SQLState.LOG_INCOMPATIBLE_FORMAT, dataDirectory);
                    }

					// If logFileNumber > 1, we are not going to write that file just
					// yet.  Just leave it be and carry on.  Maybe when we get there it
					// can be deleted.

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
					logFile = getLogFileName(logFileNumber);

                    if (privExists(logFile))
					{
						// this log file maybe there because the system may have
						// crashed right after a log switch but did not write
                        // out any log record
						Monitor.logTextMessage(MessageId.LOG_DELETE_OLD_FILE, logFile);

                        if (!privDelete(logFile))
                        {
							throw StandardException.newException(
                                    SQLState.LOG_INCOMPATIBLE_FORMAT,
                                    dataDirectory);
                        }
					}

					// don't need to try to delete it, we know it isn't there
                    StorageRandomAccessFile theLog = privRandomAccessFile(logFile, "rw");

					if (!initLogFile(theLog, logFileNumber, LogCounter.INVALID_LOG_INSTANT))
                    {
						throw StandardException.newException(
                            SQLState.LOG_SEGMENT_NOT_EXIST, logFile.getPath());
                    }

					endPosition = theLog.getFilePointer();
					lastFlush = theLog.getFilePointer();

                    //if write sync is true , prellocate the log file
                    //and reopen the file in rws mode.
                    if(isWriteSynced)
                    {
                        //extend the file by wring zeros to it
                        preAllocateNewLogFile(theLog);
                        theLog.close();
                        theLog=  privRandomAccessFile(logFile, "rws");
                        //postion the log at the current log end postion
                        theLog.seek(endPosition);
                    }
					logOut = new LogAccessFile(theLog, logBufferSize);

					if (SanityManager.DEBUG)
					{
						SanityManager.ASSERT(
                            endPosition == LOG_FILE_HEADER_SIZE,
                            "empty log file has wrong size");
					}
				}
				else
				{
					// read only database
					ReadOnlyDB = true;
					logOut = null;
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
	} // end of boot

    private void getLogStorageFactory() throws StandardException
    {
        if( logDevice == null)
        {
            DataFactory df = (DataFactory) Monitor.findServiceModule( this, DataFactory.MODULE);
            logStorageFactory = (WritableStorageFactory) df.getStorageFactory();
        }
        else
        {
            try
            {
                PersistentService ps = Monitor.getMonitor().getServiceType(this);
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
				} catch (IOException ioe) {
				}
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
            (AccessFactory)Monitor.getServiceModule(this, AccessFactory.MODULE);

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
	protected long appendLogRecord(byte[] data, int offset, int length,
			byte[] optionalData, int optionalDataOffset, int optionalDataLength) 
		 throws StandardException
	{
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
				// log file
				if ((endPosition + LOG_RECORD_OVERHEAD + length) >=
					LogCounter.MAX_LOGFILE_SIZE)
				{
					switchLogFile();

					// still too big??  Giant log record?
					if ((endPosition + LOG_RECORD_OVERHEAD + length) >=
						LogCounter.MAX_LOGFILE_SIZE) 
                    {
						throw StandardException.newException(
                                SQLState.LOG_EXCEED_MAX_LOG_FILE_SIZE, 
                                new Long(logFileNumber), 
                                new Long(endPosition), 
                                new Long(length), 
                                new Long(LogCounter.MAX_LOGFILE_SIZE));
                    }
				}

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

				endPosition += (length + LOG_RECORD_OVERHEAD);
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
							throw StandardException.interrupt(ie);
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

					// if we are not corrupt and we are in the middle of redo, 
                    // we know the log record has already been flushed since we haven't written any log 
                    // yet.
					if (recoveryNeeded && inRedo) 
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
							throw StandardException.interrupt(ie);
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

			if(isWriteSynced)
			{
				//LogAccessFile.flushDirtyBuffers() will allow only one write
				//sync at a time, flush requests will get queued 
				logOut.flushDirtyBuffers();
			}
			else
			{
				if (Performance.MEASURE)
				{
					if (!logNotSynced)
						logOut.syncLogAccessFile();
				}
				else
				{
					logOut.syncLogAccessFile();
				}
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
                raf.sync( false);

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
                    //does not matter weather I get interrupted or not
                }

                if( i > 20 )
                {
                    throw StandardException.newException(
                                SQLState.LOG_FULL, ioe, null );
                }
            }
        }
    }


	/**
	  Open a forward scan of the transaction log.

	  <P> MT- read only
	  @exception StandardException  Standard cloudscape exception policy
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

	  @exception StandardException Standard Cloudscape error policy
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

	/**
		@see RawStoreFactory#encrypt
		@exception StandardException Standard Cloudscape Error Policy
	 */
	public int encrypt(byte[] cleartext, int offset, int length, 
						  byte[] ciphertext, int outputOffset)
		 throws StandardException
	{
		return rawStoreFactory.encrypt(cleartext, offset, length, ciphertext, outputOffset);
	}

	/**
		@see RawStoreFactory#decrypt
		@exception StandardException Standard Cloudscape Error Policy
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


	/**
	 * Backup restore - stop sending log record to the log stream
	 * @exception StandardException Standard Cloudscape error policy
	 */
	public void freezePersistentStore() throws StandardException
	{
		// if I get into this synchronized block, I know I am not in the middle
		// of a write because writing to the log file is synchronized under this.
		synchronized(this)
		{
			//when the log is being  archived for roll-frward recovery
			//we would like to switch to  a new log file.
			//otherwise during restore  logfile in the backup could 
			//overwrite the more uptodate log files in the 
			//online log path. And also we would like to mark the end
			//marker for the log file other wise during roll-forward recovery,
			//if we see a log file with fuzzy end , we think that is the 
			//end of the recovery.
			if(logArchived)
				switchLogFile();

			isFrozen = true;
		}			
	}

	/**
	 * Backup restore - start sending log record to the log stream
	 * @exception StandardException Standard Cloudscape error policy
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

	private void printErrorStack(Throwable t)
	{
		Monitor.logMessage("-------------------------\n");
		t.printStackTrace(Monitor.getStream().getPrintWriter());
		Monitor.logMessage("-------------------------\n");
		ErrorStringBuilder esb = new ErrorStringBuilder(Monitor.getStream().getHeader());
		esb.stackTrace(t);
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
				bytesToWrite = Integer.valueOf(TestPartialLogWrite).intValue();
			}

			Monitor.logMessage("TEST_LOG_INCOMPLETE_LOG_WRITE: writing " + bytesToWrite + 
				   " bytes out of " + length + " + " + LOG_RECORD_OVERHEAD + " log record");




			long instant = currentInstant();
			try
			{
				synchronized (this)
				{
						//check if the length of the records to be written is 
						//actually smaller than the number of bytesToWrite 
					if(length + LOG_RECORD_OVERHEAD < bytesToWrite)
						endPosition += (length + LOG_RECORD_OVERHEAD);
					else
						endPosition += bytesToWrite;

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
					test_numRecordToFillLog = Integer.valueOf(RecordToFillLog).intValue();
				else
					test_numRecordToFillLog = 100;
			}

			if (++test_logWritten > test_numRecordToFillLog)
				throw new IOException("TestLogFull " + test_numRecordToFillLog +
									  " written " + test_logWritten);

		}	
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
	  Set to true if we want the up comming log record to be only partially
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
	public static final String TEST_LOG_PARTIAL_LOG_WRITE_NUM_BYTES = SanityManager.DEBUG ? "db2j.unittest.partialLogWrite" : null;

	/**
	  Set to true if we want to simulate a log full condition
	*/
	public static final String TEST_LOG_FULL = SanityManager.DEBUG ? "TEST_LOG_FULL" : null;

	/**
	  Set to true if we want to simulate a log full condition while switching log
	*/
	public static final String TEST_SWITCH_LOG_FAIL1 = SanityManager.DEBUG ? "TEST_SWITCH_LOG_FAIL1" : null;
	public static final String TEST_SWITCH_LOG_FAIL2 = SanityManager.DEBUG ? "TEST_SWITCH_LOG_FAIL2" : null;


	/**
	  Set to the number of log record we want to write before the log is
	  simulated to be full.
	*/
	public static final String TEST_RECORD_TO_FILL_LOG = SanityManager.DEBUG ? "db2j.unittest.recordToFillLog" : null;





	//enable the log archive mode
	public void enableLogArchiveMode() throws StandardException
	{

		//if the log archive mode is already enabled; thre is nothing to do
		if(!logArchived)
		{
			logArchived = true;
			AccessFactory af = 
            (AccessFactory)Monitor.getServiceModule(this, AccessFactory.MODULE);

			if (af != null)
			{
				TransactionController tc = null;
				tc = af.getTransaction(ContextService.getFactory().getCurrentContextManager());
				tc.setProperty(Property.LOG_ARCHIVE_MODE , "true", true);
			}
		}
	}

	//disable the log archive mode
	public void disableLogArchiveMode() throws StandardException
	{
		logArchived = false;
		AccessFactory af = 
            (AccessFactory)Monitor.getServiceModule(this, AccessFactory.MODULE);
		if (af != null)
		{
			TransactionController tc = null;
			tc = af.getTransaction(ContextService.getFactory().getCurrentContextManager());
			tc.setProperty(Property.LOG_ARCHIVE_MODE , "false", true);
		}
	}

	//delete the online archived log files
	public void deleteOnlineArchivedLogFiles()
	{
		deleteObsoleteLogfiles();
	}

	//copy all the active log files and the control files
	//to the given directory from the log directory
	public synchronized boolean copyActiveLogFiles(File toDir) throws StandardException
	{
		//find the first  log file number that is  active
		long logNumber = getFirstLogNeeded(currentCheckpoint);
		//if there is nothing to copy return
		if (logNumber== -1)
			return true;

		StorageFile fromFile = getLogFileName(logNumber);
		File toFile = null;
		//copy all the active log files to the bakcup directory
		//except the current log file , because log files is swicthed
		//before this call when we freeze the database if the log is being 
		//archived. If the log is not archived(the log switch does not occur in
		//this case) copy all the log files 
		long lastLogFileToCopy = (logArchived ? getLogFileNumber()-1 : getLogFileNumber());
		while(logNumber <= lastLogFileToCopy)
		{
			toFile = new File(toDir, fromFile.getName());
			if(!privCopyFile(fromFile, toFile))
				return false;
			fromFile = getLogFileName(++logNumber);	
		}

		//copy the log control file
		fromFile = getControlFileName();
		toFile = new File(toDir,fromFile.getName());
		if(!privCopyFile(fromFile, toFile))
			return false;

		//copy the log mirror control file
		fromFile = getMirrorControlFileName();
		toFile = new File(toDir,fromFile.getName());
		if(!privCopyFile(fromFile, toFile))
			return false;

		return true;
	}	



	//Is the transaction in rollforward recovery
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
	*/
	public void checkpointInRFR(LogInstant cinstant, long redoLWM, DataFactory df) throws StandardException
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
		
	}


	/**
	 *
	 * This function restores logs based on the  following attributes
	 * are specified on connection URL:
	 * Attribute.CREATE_FROM (Create database from backup if it does not exist)
	 * Attribute.RESTORE_FROM (Delete the whole database if it exists and then restore
	 * it from backup)
	 * Attribute.ROLL_FORWARD_RECOVERY_FROM:(Perform Rollforward Recovery;
	 * except for the log directory everthing else is replced  by the copy  from
	 * backup. log files in the backup are copied to the existing online log directory.
	 *
	 * In cases of RESTORE_FROM whole databases directoy is 
	 * is removed in Directory.java while restoring service.properties
	 * so even the log directory is removed.
	 * In case of CREATE_FROM , log directoy will not exist if 
	 * we came so far bacause it should fail if a database already exists.
	 * In case ROLL_FORWARD_RECOVERY_FROM log directotry should not be removed.
	 * So only thing that needs to be done here is create a
	 * a log directory if it does not exists and copy the 
	 * log files(including control files) that exists in the backup from which we are
	 * are trying to restore the database to the onlie log directory.
	 */
	private void restoreLogs(Properties properties) throws StandardException
	{

		String backupPath =null;
		boolean isCreateFrom = false; 
		boolean isRestoreFrom = false;

		//check if the user request for restore/recovery/create from backup
		backupPath = properties.getProperty(Attribute.CREATE_FROM);
		if(backupPath == null)
			backupPath = properties.getProperty(Attribute.RESTORE_FROM);
		else
			isCreateFrom = true;

		if(backupPath == null)
			backupPath =
				properties.getProperty(Attribute.ROLL_FORWARD_RECOVERY_FROM);
		else
			isRestoreFrom = true;
		

		if(backupPath !=null)
		{
			if(!isCreateFrom){
				if(logDevice == null){
					/**
					 * In  restoreFrom/rollForwardRecoveryFrom  mode when no logDevice on
					 * URL then the log is restored to the same location where the log was 
					 * when backup was taken.
					 * In createFrom mode behaviour is same as when create=true , 
					 * i.e unless user specifies the logDevice on URL, log will be copied to
					 * the database home dir.
					 * Note: LOG_DEVICE_AT_BACKUP will get set if log is not in
					 * default location(db home). 
					 */
					logDevice = properties.getProperty(Property.LOG_DEVICE_AT_BACKUP);
				}
			}	 
            getLogStorageFactory();

			StorageFile logDir;
			logDir = logStorageFactory.newStorageFile( LogFactory.LOG_DIRECTORY_NAME);
			if(isCreateFrom){
				//log dir should not exist if we are doing create from
				if(privExists(logDir))
					throw StandardException.newException(SQLState.LOG_SEGMENT_EXIST, getLogDirPath( logDir));
			}

				
			//remove the log directory in case of restoreFrom 
			//if it exist, this happens if the log device is on seperate
			//location than the db home.
			if (isRestoreFrom && logDevice != null)
			{
				if(!privRemoveDirectory(logDir))
				{
					//it may be just a file, try deleting it
					if(!privDelete(logDir))
						throw StandardException.newException(SQLState.UNABLE_TO_REMOVE_DATA_DIRECTORY,
                                                             getLogDirPath( logDir));
				}
			}


			logDir = getLogDirectory();
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

	/*open the given log file name for writes; if write sync 
	 *is enabled open in rws mode otherwise in rw mode. 
	 */
	public StorageRandomAccessFile openLogFileInWriteMode(StorageFile logFile) throws IOException
	{
		if(isWriteSynced)
			return privRandomAccessFile(logFile, "rws");
		else
			return privRandomAccessFile(logFile, "rw");
	}


    private String getLogDirPath( StorageFile logDir)
    {
        if( logDevice == null)
            return logDir.toString();
        return logDevice + logStorageFactory.getSeparator() + logDir.toString();
    } // end of getLogDirPath

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

    protected synchronized StorageRandomAccessFile privRandomAccessFile(StorageFile file, String perms)
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

    protected boolean privCanWrite(StorageFile file)
    {
		return runBooleanAction(3, file);
    }

    protected boolean privMkdirs(StorageFile file)
    {
		return runBooleanAction(4, file);
    }

	protected synchronized String[] privList(File file)
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
    
	protected synchronized String[] privList(StorageFile file)
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


	protected synchronized boolean privCopyFile(StorageFile from, File to)
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
            return false;
        }	
	}

	protected synchronized boolean privCopyFile(File from, StorageFile to)
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

	protected boolean privRemoveDirectory(StorageFile file)
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


	

	public Object run() throws IOException {
		switch (action) {
		case 0:
			// SECURITY PERMISSION - MP1
			return ReuseFactory.getBoolean(activeFile.exists());
		case 1:
			// SECURITY PERMISSION - OP5
           return ReuseFactory.getBoolean(activeFile.delete());
		case 2:
			// SECURITY PERMISSION - MP1 and/or OP4
			// dependening on the value of activePerms
			return activeFile.getRandomAccessFile(activePerms);
		case 3:
			// SECURITY PERMISSION - OP4
			return ReuseFactory.getBoolean(activeFile.canWrite());
		case 4:
			// SECURITY PERMISSION - OP4
			return ReuseFactory.getBoolean(activeFile.mkdirs());
		case 5:
			// SECURITY PERMISSION - MP1
			return activeFile.list();
		case 6:
			// SECURITY PERMISSION - OP4 (Have to check these codes ??)
			return ReuseFactory.getBoolean(FileUtil.copyFile(logStorageFactory, activeFile, toFile));
		case 7:
			// SECURITY PERMISSION - OP4
            if( ! activeFile.exists())
                return ReuseFactory.getBoolean( true);
			return ReuseFactory.getBoolean(activeFile.deleteAll());
        case 8:
            return toFile.list();
        case 9:
            return ReuseFactory.getBoolean(FileUtil.copyFile( logStorageFactory, toFile, activeFile));

		default:
			return null;
		}
	}
}
