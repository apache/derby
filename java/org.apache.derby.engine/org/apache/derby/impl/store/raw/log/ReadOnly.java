/*

   Derby - Class org.apache.derby.impl.store.raw.log.ReadOnly

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

import org.apache.derby.shared.common.reference.SQLState;

import org.apache.derby.iapi.services.monitor.ModuleControl;
import org.apache.derby.iapi.services.monitor.ModuleSupportable;
import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derby.iapi.services.io.Formatable;

import org.apache.derby.iapi.services.property.PersistentSet;
import org.apache.derby.iapi.store.raw.Compensation;
import org.apache.derby.iapi.store.raw.Loggable;
import org.apache.derby.iapi.store.raw.RawStoreFactory;
import org.apache.derby.iapi.store.raw.ScanHandle;
import org.apache.derby.iapi.store.raw.log.LogFactory;
import org.apache.derby.iapi.store.raw.log.LogInstant;
import org.apache.derby.iapi.store.raw.log.Logger;
import org.apache.derby.iapi.store.raw.log.LogScan;
import org.apache.derby.iapi.store.replication.master.MasterFactory;

import org.apache.derby.iapi.store.raw.data.DataFactory;
import org.apache.derby.iapi.store.raw.xact.TransactionFactory;
import org.apache.derby.iapi.store.raw.xact.RawTransaction;
import org.apache.derby.iapi.store.raw.xact.TransactionId;

import org.apache.derby.shared.common.error.StandardException;

import org.apache.derby.io.StorageFile;
import org.apache.derby.iapi.store.access.DatabaseInstant;
import org.apache.derby.catalog.UUID;

import java.util.Properties;
import java.io.File;

/**
	A read-only version of the log factory.
	It doesn't do anything, it doesn't check that
	the database needs recovery or not.
	<P>
	It doesn't handle undo.  No recovery.

	<P>Multithreading considerations:<BR>
	This class must be MT-safe.
*/

public class ReadOnly implements LogFactory, ModuleSupportable {

	private String logArchiveDirectory = null;

	/* 
	** Methods of Log Factory
	*/

	public Logger getLogger() {
		return null;
	}

    /** Not applicable in readonly databases */
    public void createDataWarningFile() throws StandardException {
    }

	/** Not applicable in readonly databases */
	public void setRawStoreFactory(RawStoreFactory rsf) {
	}

	/**
	  MT - not needed, no work is done
	  @exception StandardException Standard Derby Error Policy
	*/
	public void recover(DataFactory dataFactory,
						TransactionFactory transactionFactory)
		 throws StandardException
	{
		if (transactionFactory != null)
			transactionFactory.useTransactionTable((Formatable)null);
	}

	/**
	  MT - not needed, no work is done
	*/
	public boolean checkpoint(RawStoreFactory rawStoreFactory,
							  DataFactory dataFactory,
							  TransactionFactory transactionFactory,
							  boolean wait)
	{
		return true;
	}

	public StandardException markCorrupt(StandardException originalError) {
		return originalError;
	}

	public void flush(LogInstant where) throws StandardException {
	}

	/*
	** Methods of ModuleControl
	*/

	public boolean canSupport(Properties startParams) {

		String runtimeLogAttributes = startParams.getProperty(LogFactory.RUNTIME_ATTRIBUTES);
		if (runtimeLogAttributes == null)
			return false;

		return runtimeLogAttributes.equals(LogFactory.RT_READONLY);
	}

	/*
	 * truncation point support (not supported)
	 */

	public LogInstant setTruncationLWM(UUID name,
									   LogInstant instant,
									   RawStoreFactory rawStoreFactory, 
									  TransactionFactory transFactory)
		 throws StandardException
	{
		if (SanityManager.DEBUG)
			SanityManager.THROWASSERT("functionality not implemented");

        throw StandardException.newException(
                SQLState.STORE_FEATURE_NOT_IMPLEMENTED);

	}

	/**
	  @exception StandardException functionality not implemented
	*/
	public void setTruncationLWM(UUID name, LogInstant instant) throws StandardException
	{
		if (SanityManager.DEBUG)
			SanityManager.THROWASSERT("functionality not implemented");

        throw StandardException.newException(
                SQLState.STORE_FEATURE_NOT_IMPLEMENTED);
	}


	/**
	  @exception StandardException functionality not implemented
	*/
	public void removeTruncationLWM(UUID name,
							 RawStoreFactory rawStoreFactory, 
							 TransactionFactory transFactory)
		 throws StandardException
	{
		if (SanityManager.DEBUG)
			SanityManager.THROWASSERT("functionality not implemented");

        throw StandardException.newException(
                SQLState.STORE_FEATURE_NOT_IMPLEMENTED);
	}


	/**
	  @exception StandardException functionality not implemented
	*/
	public LogInstant getTruncationLWM(UUID name) throws StandardException
	{
		if (SanityManager.DEBUG)
			SanityManager.THROWASSERT("functionality not implemented");

        throw StandardException.newException(
                SQLState.STORE_FEATURE_NOT_IMPLEMENTED);
	}

	/**
	  @exception StandardException functionality not implemented
	*/
	public void removeTruncationLWM(UUID name) throws StandardException
	{
		if (SanityManager.DEBUG)
			SanityManager.THROWASSERT("functionality not implemented");

        throw StandardException.newException(
                SQLState.STORE_FEATURE_NOT_IMPLEMENTED);
	}

	/**
	  @exception StandardException functionality not implemented
	*/
	public ScanHandle openFlushedScan(DatabaseInstant i, int groupsIWant)
		 throws StandardException
	{
		if (SanityManager.DEBUG)
			SanityManager.THROWASSERT("functionality not implemented");

        throw StandardException.newException(
                SQLState.STORE_FEATURE_NOT_IMPLEMENTED);
	}

	/**
	  @exception StandardException functionality not implemented
	*/
	public LogScan openForwardsScan(LogInstant startAt,LogInstant stopAt)
		 throws StandardException
	{
		if (SanityManager.DEBUG)
			SanityManager.THROWASSERT("functionality not implemented");

        throw StandardException.newException(
                SQLState.STORE_FEATURE_NOT_IMPLEMENTED);
	}

	/**
	  */
    public LogInstant getFirstUnflushedInstant()
	{
		if (SanityManager.DEBUG)
			SanityManager.THROWASSERT("functionality not implemented");

		return null;
	}

	public long getFirstUnflushedInstantAsLong() {
		if (SanityManager.DEBUG) {
			SanityManager.THROWASSERT("functionality not implemented");
		}
		return LogCounter.INVALID_LOG_INSTANT;
	}

	/**
	  @exception StandardException functionality not implemented
	  */
	public LogScan openForwardsFlushedScan(LogInstant startAt)
		 throws StandardException
	{
		if (SanityManager.DEBUG)
			SanityManager.THROWASSERT("functionality not implemented");

        throw StandardException.newException(
                SQLState.STORE_FEATURE_NOT_IMPLEMENTED);
	}

	/**
	 * Backup restore - stop sending log record to the log stream
	 * @exception StandardException Standard Derby error policy
	 */
	public void freezePersistentStore() throws StandardException
	{
		// read only, do nothing
	}

	/**
	 * Backup restore - start sending log record to the log stream
	 * @exception StandardException Standard Derby error policy
	 */
	public void unfreezePersistentStore() throws StandardException
	{
		// read only, do nothing
	}

	/**
	 * Backup restore - is the log being archived to some directory?
	 * if RawStore.LOG_ARCHIVAL_DIRECTORY is set to some value, that means the
	 * log is meant to be archived.  Else, log not archived.
	 */
	public boolean logArchived()
	{
		return (logArchiveDirectory != null);
	}

	/**
		Get JBMS properties relevant to the log factory
	 */
	public void getLogFactoryProperties(PersistentSet set) 
	{
		// do nothing
	}
	
	public StorageFile getLogDirectory()
	{
		return null;
	}

	public String getCanonicalLogPath()
	{
		return null;
	}

	
	//roll-forward recovery support routines
	//Nothing to be done for read only databases
	public void enableLogArchiveMode()
	{
		//do nothing
	}

	public void disableLogArchiveMode()
	{
		//do nothing
	}

	//this function is suppose to delete all the logs 
	//before this call that are not active logs.
	public void deleteOnlineArchivedLogFiles()
	{
		//do nothing
	}


	//Is the transaction in rollforward recovery
	public boolean inRFR()
	{
		return false;
	}

	/**	
	 *	Perform a checkpoint during rollforward recovery.
     * 
     * @throws org.apache.derby.shared.common.error.StandardException 
     */
	public void checkpointInRFR(LogInstant cinstant, long redoLWM, long undoLWM,
								DataFactory df) throws StandardException
	{
		//do nothing
	}

		
	/*
	 * There are no log files to backup for  read  only databases, nothing to be
     * done here. 
     * @param toDir - location where the log files should be copied to.
     * @exception StandardException Standard Derby error policy
	*/
	public void startLogBackup(File toDir) throws StandardException
	{
		// nothing to do for read only databases.
	}

	
	/* 
     * There are no log files to backup for read only databases, 
     * nothing to be done here. 
     *
     * @param toDir - location where the log files should be copied to.
     * @exception StandardException Standard Derby error policy
	*/
	public void endLogBackup(File toDir) throws StandardException
	{
		// nothing to do for read only databases.
	}

	
	/*
     * Log backup is not started for for read only databases, no work to do
     * here.
	 **/
	public void abortLogBackup()
	{
		// nothing to do for read only databases.
	}

    /**
     * Sets whether the database is encrypted.
     * <p>
     * Read-only database can not be re-encrypted, nothing to do in this case.
     */
    public void setDatabaseEncrypted(boolean isEncrypted, boolean flushLog)
    {
        // nothing to do for a read-only database.
    }


    /*
     * set up a new log file to start writing 
     * the log records into the new log file 
     * after this call.
     *
     * <P>MT - synchronization provided by caller - RawStore boot,
     * This method is called while re-encrypting the database 
     * at databse boot time. 
     *
     * Read-only database can not be reencrypted, 
     * nothing to do in this case. 
     */
    public void startNewLogFile() throws StandardException 
    {
        // nothing to do for a read-only database. 
    }

    /*
     * find if the checkpoint is in the last log file. 
     *
     * <P>MT - synchronization provided by caller - RawStore boot,
     * This method is called only if a crash occured while 
     * re-encrypting the database at boot time. 

     * Read-only database can not be re-encrypted, 
     * nothing to do in this case. 
     */
    public boolean isCheckpointInLastLogFile() 
        throws StandardException 
    {
        // nothing to do for a read-only database. 
        return false;
    }
    
    /*
     * delete the log file after the checkpoint. 
     *
     * <P>MT - synchronization provided by caller - RawStore boot,
     * This method is called only if a crash occured while 
     * re-encrypting the database at boot time. 
     *
     * Read-only database can not be re-encrypted, 
     * nothing to do in this case. 
     */
    public void deleteLogFileAfterCheckpointLogFile() 
        throws StandardException 
    {
        // nothing to do for a read-only database. 
    }



    /**
     *  Check to see if a database has been upgraded to the required
     *  level in order to use a store feature.
     *
     * This method is generally used to prevent writes to 
     * data/log file by a particular store feature until the 
     * database is upgraded to the required version. 
     * In read-only database writes are not allowed, so nothing to do
     * for this method in this implementation of the log factory.
     *
     * @param requiredMajorVersion  required database Engine major version
     * @param requiredMinorVersion  required database Engine minor version
     * @param feature Non-null to throw an exception, null to return the 
     *                state of the version match.
     *
     * @exception  StandardException 
     *             not implemented exception is thrown
     */
	public boolean checkVersion(int requiredMajorVersion, 
                                int requiredMinorVersion, 
                                String feature) 
        throws StandardException
    {
        // nothing to do for read only databases; 
        throw StandardException.newException(
                  SQLState.STORE_FEATURE_NOT_IMPLEMENTED);
    }

    /** Replication not applicable on readonly databases 
     * @exception StandardException always thrown, indicating that
     * ReadOnly databases can not be replicated
     */
    public void startReplicationMasterRole(MasterFactory masterFactory)
//IC see: https://issues.apache.org/jira/browse/DERBY-3051
        throws StandardException {
        throw StandardException.newException(
                  SQLState.LOGMODULE_DOES_NOT_SUPPORT_REPLICATION);
    }
    
    /**
     * Replication not applicable on readonly databases.
     *
     * @return false always since replication is not applicable here.
     */
    
    public boolean inReplicationMasterMode() {
//IC see: https://issues.apache.org/jira/browse/DERBY-3551
        return false;
    }

    /** Replication not applicable on readonly databases */
    public void stopReplicationMasterRole() {
    }

}
