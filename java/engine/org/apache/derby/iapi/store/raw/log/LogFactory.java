/*

   Derby - Class org.apache.derby.iapi.store.raw.log.LogFactory

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

package org.apache.derby.iapi.store.raw.log;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.property.PersistentSet;
import org.apache.derby.iapi.store.raw.data.DataFactory;
import org.apache.derby.iapi.store.raw.Corruptable;
import org.apache.derby.iapi.store.raw.RawStoreFactory;
import org.apache.derby.iapi.store.raw.ScanHandle;
import org.apache.derby.iapi.store.raw.ScannedTransactionHandle;
import org.apache.derby.iapi.store.raw.xact.TransactionFactory;
import org.apache.derby.io.StorageFile;
import org.apache.derby.iapi.store.access.DatabaseInstant;
import org.apache.derby.iapi.reference.Property;
import org.apache.derby.catalog.UUID;
import java.io.File;

public interface LogFactory extends Corruptable {

	/**
		The name of a runtime property in the service set that defines any runtime
		attributes a log factory should have. It is (or will be) a comma separated list
		of attributes.
		At the moment only one attribute is known and checked for.
	*/
	public static final String RUNTIME_ATTRIBUTES = Property.PROPERTY_RUNTIME_PREFIX + "storage.log";

	/**
		An attribute that indicates the database is readonly
	*/
	public static final String RT_READONLY = "readonly";

	/**
		The name of the default log directory.
	 */
	public static final String LOG_DIRECTORY_NAME = "log";



	public static final String MODULE = "org.apache.derby.iapi.store.raw.log.LogFactory";

	public Logger getLogger();

	/**
		Recover the database to a consistent state using the log. 
		Each implementation of the log factory has its own recovery algorithm,
		please see the implementation for a description of the specific
		recovery algorithm it uses.

		@param rawStoreFactory - the raw store
		@param dataFactory - the data factory
		@param transactionFactory - the transaction factory

		@exception StandardException - encounter exception while recovering.
	 */
	public void recover(RawStoreFactory rawStoreFactory,
						DataFactory dataFactory,
						TransactionFactory transactionFactory)
		 throws StandardException;

	/**
		Checkpoint the rawstore.

		The frequency of checkpoint is determined by 2 persistent service
		properties,
		RawStore.LOG_SWITCH_INTERVAL and RawStore.CHECKPOINT_INTERVAL.  

		By default, LOG_SWITCH_INTERVAL is every 100K bytes of log record
		written.  User can change this value by setting the property to some
		other values during boot time.   The legal range of LOG_SWITCH_INTERVAL
		is from 100K to 128M.

		By default, CHECKPOINT_INTERVAL equals LOG_SWITCH_INTERVAL, but user
		can set it to less if more frequent checkpoint is desired.  The legal
		range of CHECKPOINT_INTERVAL is from 100K to LOG_SWITCH_INTERVAL.

		@param rawStoreFactory - the raw store
		@param dataFactory - the data factory
		@param transactionFactory - the transaction factory
		@param wait - if true; waits for the checkpoint to completed even if it is being done my an another thread.
		@return true if checkpoint is successful
		@exception StandardException - encounter exception while doing checkpoint.
	*/
	public boolean checkpoint(RawStoreFactory rawStoreFactory,
							  DataFactory dataFactory,
							  TransactionFactory transactionFactory, 
							  boolean wait)
		 throws StandardException;

	/**
		Flush all unwritten log record up to the log instance indicated to disk.

		@param where flush log up to here

		@exception StandardException cannot flush log file due to sync error
	*/
	public void flush(LogInstant where) throws StandardException;


	/**
		Get a LogScan to scan flushed records from the log.

		<P> MT- read only

		@param startAt - the LogInstant where we start our scan. null means
		start at the beginning of the log. This function raises an error
		if startAt is a LogInstant which is not in the log.

		@return the LogScan.

		@exception StandardException StandardCloudscape error policy
	    NOTE: This will be removed after the LogSniffer Rewrite.
	*/
	LogScan openForwardsFlushedScan(LogInstant startAt)
		 throws StandardException;

	/**
	    Get a ScanHandle to scan flushed records from the log.

		<P> MT- read only

		@param startAt - the LogInstant where we start our scan. null means
		start at the beginning of the log. This function raises an error
		if startAt is a LogInstant which is not in the log.
		@param groupsIWant - log record groups the scanner wants.
		@return the LogScan.
		@exception StandardException StandardCloudscape error policy
		*/
	ScanHandle openFlushedScan(DatabaseInstant start, int groupsIWant)
		 throws StandardException;

	/**
		Get a LogScan to scan the log in a forward direction.

		<P> MT- read only

		@param startAt - the LogInstant where we start our scan. null means
		start at the beginning of the log. This function raises an error
		if startAt is a LogInstant which is not in the log.
		@param stopAt - the LogInstant where we stop our scan. null means
		stop at the end of the log. This function raises an error
 		if stopAt is a LogInstant which is not in the log.
		@return the LogScan.

		@exception StandardException StandardCloudscape error policy
	*/
	LogScan openForwardsScan(LogInstant startAt,LogInstant stopAt)
		 throws StandardException;
    /**
	  Get the instant for the last record in the log.
	  */
    LogInstant getFirstUnflushedInstant();

	/**
		Backup restore support
	 */

	/**
		Stop making any change to the persistent store
		@exception StandardException Standard cloudscape exception policy.
	 */
	public void freezePersistentStore() throws StandardException;
		 
	/**
		Can start making change to the persistent store again
		@exception StandardException Standard cloudscape exception policy.
	 */
	public void unfreezePersistentStore() throws StandardException;

	/**
	   checks whether is log archive mode is enabled or not.
	   @return true if the log is being archived.
	*/
	public boolean logArchived();

	/**
		Get JBMS properties relavent to the log factory
		@exception StandardException Standard Cloudscape Error Policy
	 */
	public void getLogFactoryProperties(PersistentSet set) 
		 throws StandardException;

	 /**
		Return the location of the log directory.
		@exception StandardException Standard Cloudscape Error Policy
	  */
	public StorageFile getLogDirectory() throws StandardException;

	 /**
		Return the canonical directory of the PARENT of the log directory.  The
		log directory live in the "log" subdirectory of this path.  If the log
		is at the default location (underneath the database directory), this
		returns null.  Should only be called after the log factory is booted.
	  */
	public String getCanonicalLogPath();


	/*
	  copies the active log files and control files to the given directory
	  Used copy the necessary log files while doing online backup
	  @param toDir - location where the log files should be copied to.
	  @return true if log files copy is  successful
	  @exception StandardException - encounter exception while doing checkpoint.
	*/
	public boolean copyActiveLogFiles(File toDir) throws StandardException;

	/*
	 * Enable the log archive mode, when log archive mode is 
	 * on the system keeps all the old log files instead
	 * of deleting them at the checkpoint.
	 * logArchive mode is persistent across the boots.
	 * @exception StandardException - thrown on error
	*/
	public void enableLogArchiveMode() throws StandardException;

		
	/*
	 * Disable the log archive mode, when log archive mode is 
	 * off the system will delete  old log files(not required 
	 * for crash recovery) after each checkpoint. 
	 * @exception StandardException - thrown on error
	*/
	public void disableLogArchiveMode() throws StandardException;

	/*
	 * Deletes the archived log files store in the log directory path.
	 * This call is typically used after a successful version level
	 * backup to clean up the old log files that are no more
	 * required to do roll-forward recovery from the last
	 * backup taken.
	*/
	public void deleteOnlineArchivedLogFiles();

	//Is the transaction in rollforward recovery
	public boolean inRFR();

	/**	
	 * redoing a checkpoint  during rollforward recovery
	 @param cinstant The LogInstant of the checkpoint
	 @param redoLWM  Redo Low Water Mark in the check point record
	 @param dataFactory - the data factory
	 @exception StandardException - encounter exception during checkpoint
	 */
	public void checkpointInRFR(LogInstant cinstant, long redoLWM, 
								DataFactory df) throws StandardException;


}

