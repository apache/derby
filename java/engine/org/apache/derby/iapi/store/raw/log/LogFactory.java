/*

   Derby - Class org.apache.derby.iapi.store.raw.log.LogFactory

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

package org.apache.derby.iapi.store.raw.log;

import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.iapi.services.property.PersistentSet;
import org.apache.derby.iapi.store.replication.master.MasterFactory;
import org.apache.derby.iapi.store.raw.data.DataFactory;
import org.apache.derby.iapi.store.raw.Corruptable;
import org.apache.derby.iapi.store.raw.RawStoreFactory;
import org.apache.derby.iapi.store.raw.ScanHandle;
import org.apache.derby.iapi.store.raw.xact.TransactionFactory;
import org.apache.derby.io.StorageFile;
import org.apache.derby.iapi.store.access.DatabaseInstant;
import org.apache.derby.shared.common.reference.Property;
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
     * Create readme file in log directory warning users against touching
     *  any files in the directory
     * @throws StandardException
     */
    public void createDataWarningFile() throws StandardException;

	/**
		Make log factory aware of which raw store factory it belongs to
	*/
	public void setRawStoreFactory(RawStoreFactory rsf);

	/**
		Recover the database to a consistent state using the log. 
		Each implementation of the log factory has its own recovery algorithm,
		please see the implementation for a description of the specific
		recovery algorithm it uses.

		@param dataFactory - the data factory
		@param transactionFactory - the transaction factory

		@exception StandardException - encounter exception while recovering.
	 */
	public void recover(DataFactory dataFactory,
						TransactionFactory transactionFactory)
		 throws StandardException;

	/**
		Checkpoint the rawstore.

		The frequency of checkpoint is determined by 2 persistent service
		properties,
		RawStore.LOG_SWITCH_INTERVAL and RawStore.CHECKPOINT_INTERVAL.  

		By default, LOG_SWITCH_INTERVAL is every 1M bytes of log record
		written.  User can change this value by setting the property to some
		other values during boot time.   The legal range of LOG_SWITCH_INTERVAL
		is from 100K to 128M.

		By default, CHECKPOINT_INTERVAL equals 10M, but user
		can set it to less if more frequent checkpoint is desired.  The legal
		range of CHECKPOINT_INTERVAL is from 100K to 128M.

		@param rawStoreFactory - the raw store
		@param dataFactory - the data factory
		@param transactionFactory - the transaction factory
		@param wait - if true waits for any existing checkpoint to complete 
                         and then executes and waits for another checkpoint.
                      if false if another thead is executing a checkpoint 
                      routine will return immediately.

		@return true if checkpoint is successful,  Will return false if wait
                is false and the routine finds another thread executing a 
                checkpoint.

		@exception StandardException - got exception while doing checkpoint.
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

		@exception StandardException Standard Derby error policy
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
		@exception StandardException Standard Derby error policy
		*/
	ScanHandle openFlushedScan(DatabaseInstant startAt, int groupsIWant)
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

		@exception StandardException Standard Derby error policy
	*/
	LogScan openForwardsScan(LogInstant startAt,LogInstant stopAt)
		 throws StandardException;
    /**
	  Get the instant for the last record in the log.
	  */
    LogInstant getFirstUnflushedInstant();

    /**
     * Get the log instant long value of the first log record that has not 
     * been flushed. Only works after recover() has finished, or (if in slave 
     * replication mode) after calling initializeReplicationSlaveRole.
     *
     * @return the log instant long value of the first log record that has not 
     * been flushed
     */
    public long getFirstUnflushedInstantAsLong();

	/**
		Backup restore support
	 */

	/**
		Stop making any change to the persistent store
		@exception StandardException Standard Derby exception policy.
	 */
	public void freezePersistentStore() throws StandardException;
		 
	/**
		Can start making change to the persistent store again
		@exception StandardException Standard Derby exception policy.
	 */
	public void unfreezePersistentStore() throws StandardException;

	/**
	   checks whether is log archive mode is enabled or not.
	   @return true if the log is being archived.
	*/
	public boolean logArchived();
        
        /**
         * Used to determine if the replication master mode has been started,
         * and the logging for unlogged operations needs to be enabled.
         *
         * @return true If the master replication mode is turned on and the 
         *              unlogged operations need to be logged.
         *         false If the master replication mode is turned off and the 
         *               unlogged operations need not be logged.
         */
        public boolean inReplicationMasterMode();

	/**
		Get JBMS properties relevant to the log factory
		@exception StandardException Standard Derby Error Policy
	 */
	public void getLogFactoryProperties(PersistentSet set) 
		 throws StandardException;

	 /**
		Return the location of the log directory.
		@exception StandardException Standard Derby Error Policy
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
	 * @param cinstant The LogInstant of the checkpoint
	 * @param redoLWM  Redo Low Water Mark in the check point record
	 * @param undoLWM Undo Low Water Mark in the checkpoint
	 * @param df - the data factory
	 * @exception StandardException - encounter exception during checkpoint
	 */
	public void checkpointInRFR(LogInstant cinstant, long redoLWM, long undoLWM,
								DataFactory df) throws StandardException;

	
	/*
	 * start the transaction log backup, the transaction log is  is required
	 * to bring the database to the consistent state on restore. 
	 * copies the log control information , active log files to the given 
	 * backup directory and marks that backup is in progress.
     * @param toDir - location where the log files should be copied to.
     * @exception StandardException Standard Derby error policy
	*/
	public void startLogBackup(File toDir) throws StandardException;

	
	/*
	 * copy all the log files that has to go into the backup directory
	 * and mark that backup has come to an end. 
     * @param toDir - location where the log files should be copied to.
     * @exception StandardException Standard Derby error policy
	*/
	public void endLogBackup(File toDir) throws StandardException;

	
	/*
	 * Abort any activity related to backup in the log factory.
	 * Backup is not in progress any more, it failed for some reason.
	 **/
	public void abortLogBackup();

    /**
     * Sets whether the database is encrypted, all the transaction log has
     * to be encrypted, and flush the log if requested.
     * <p>
     * Log needs to be flushed first if the cryptographic state of the database
     * changes (for instance re-encryption with a new key).
	 *
     * @param isEncrypted {@code true} if the database is encrypted,
     *      {@code false} if not
	 * @param flushLog {@code true} if log needs to be flushed,
     *      {@code false} otherwise
     */
    public void setDatabaseEncrypted(boolean isEncrypted, boolean flushLog)
		throws StandardException;

    
    /*
     * set up a new log file to start writing 
     * the log records into the new log file 
     * after this call.
     *
     * <P>MT - synchronization provided by caller - RawStore boot,
     * This method is called while re-encrypting the database 
     * at database boot time.
     */
    public void startNewLogFile() throws StandardException;

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
        throws StandardException;
    
    /*
     * delete the log file after the checkpoint. 
     *
     * <P>MT - synchronization provided by caller - RawStore boot,
     * This method is called only if a crash occured while 
     * re-encrypting the database at boot time. 
     */
    public void deleteLogFileAfterCheckpointLogFile() 
        throws StandardException;

    
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
                                String feature) 
        throws StandardException;

    /**
     * Make this LogFactory pass log records to the MasterFactory
     * every time a log record is appended to the log on disk, and
     * notify the MasterFactory when a log disk flush has taken place.
     * Not implemented by ReadOnly.
     * @param masterFactory The MasterFactory service responsible for
     * controlling the master side replication behaviour.
     * @exception StandardException Standard Derby exception policy,
     * thrown on replication startup error. Will only be thrown if
     * replication is attempted started on a readonly database.
     */
    public void startReplicationMasterRole(MasterFactory masterFactory)
        throws StandardException;

    /**
     * Stop this LogFactory from passing log records to the
     * MasterFactory and from notifying the MasterFactory when a log
     * disk flush has taken place. Not implemented by ReadOnly.
     */
    public void stopReplicationMasterRole();

}

