/*

   Derby - Class org.apache.derby.impl.store.raw.log.ReadOnly

   Copyright 1998, 2004 The Apache Software Foundation or its licensors, as applicable.

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

import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.iapi.services.monitor.ModuleControl;
import org.apache.derby.iapi.services.monitor.ModuleSupportable;
import org.apache.derby.iapi.services.sanity.SanityManager;
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

import org.apache.derby.iapi.store.raw.data.DataFactory;
import org.apache.derby.iapi.store.raw.xact.TransactionFactory;
import org.apache.derby.iapi.store.raw.xact.RawTransaction;
import org.apache.derby.iapi.store.raw.xact.TransactionId;

import org.apache.derby.iapi.error.StandardException;

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

	/**
	  MT - not needed, no work is done
	  @exception StandardException Cloudscape Standard Error Policy
	*/
	public void recover(RawStoreFactory rawStoreFactory,
						DataFactory dataFactory,
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
	  @exception StandardException functionality not implmented
	*/
	public void setTruncationLWM(UUID name, LogInstant instant) throws StandardException
	{
		if (SanityManager.DEBUG)
			SanityManager.THROWASSERT("functionality not implemented");

        throw StandardException.newException(
                SQLState.STORE_FEATURE_NOT_IMPLEMENTED);
	}


	/**
	  @exception StandardException functionality not implmented
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
	  @exception StandardException functionality not implmented
	*/
	public LogInstant getTruncationLWM(UUID name) throws StandardException
	{
		if (SanityManager.DEBUG)
			SanityManager.THROWASSERT("functionality not implemented");

        throw StandardException.newException(
                SQLState.STORE_FEATURE_NOT_IMPLEMENTED);
	}

	/**
	  @exception StandardException functionality not implmented
	*/
	public void removeTruncationLWM(UUID name) throws StandardException
	{
		if (SanityManager.DEBUG)
			SanityManager.THROWASSERT("functionality not implemented");

        throw StandardException.newException(
                SQLState.STORE_FEATURE_NOT_IMPLEMENTED);
	}

	/**
	  @exception StandardException functionality not implmented
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
	  @exception StandardException functionality not implmented
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

	/**
	  @exception StandardException functionality not implmented
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
	 * @exception StandardException Standard Cloudscape error policy
	 */
	public void freezePersistentStore() throws StandardException
	{
		// read only, do nothing
	}

	/**
	 * Backup restore - start sending log record to the log stream
	 * @exception StandardException Standard Cloudscape error policy
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
		Get JBMS properties relavent to the log factory
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
		perform a  checkpoint during rollforward recovery
	*/
	public void checkpointInRFR(LogInstant cinstant, long redoLWM, 
								DataFactory df) throws StandardException
	{
		//do nothing
	}

	public boolean copyActiveLogFiles(File toDir) throws StandardException
	{
		//do nothing
		return false;
	}
}
