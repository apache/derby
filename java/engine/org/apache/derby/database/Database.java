/*

   Derby - Class org.apache.derby.database.Database

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

package org.apache.derby.database;

/*
  The org.apache.derby.iapi.db.Database interface is all the externally available
  methods on a database.  These are methods that might be called from an
  SQL-J CALL statement. It is extended by com.ibm.db2j.impl.Database.DatabaseInterface.Database
  which adds internal methods which are only called from within cloudscape code.

  The Javadoc comment that follows is for external consumption.
*/


import org.apache.derby.catalog.UUID;

import java.sql.Timestamp;
import java.sql.SQLException;
import java.util.Locale;
import java.io.File;

/**
 * The Database interface provides control over a database
 * (that is, the stored data and the files the data are stored in),
 * operations on the database such as  backup and recovery,
 * and all other things that are associated with the database itself.
 * 
 * <P>
 * <I>IBM Corp. reserves the right to change, rename, or
 * remove this interface at any time.</I>

   @see Factory
 */
public interface Database
{

	/**
	 * Tells whether the Database is configured as read-only, or the
	 * Database was started in read-only mode.
	 *
	 * @return	TRUE means the Database is read-only, FALSE means it is
	 *		not read-only.
	 */
	public boolean		isReadOnly();

	/**
	 * Delete all stored prepared statements that were
	 * created for JDBC MetaData queries.
	 *
	 * @exception SQLException thrown on error deleting
	 *		the stored prepared statements, most likely
	 *		a deadlock or timeout.
	 */
	public void dropAllJDBCMetaDataSPSes()
		throws SQLException;

	/**
	 * Backup the database to a backup directory.  See on line documentation
	 * for more detail about how to use this feature.
	 *
	 * @param backupDir the directory name where the database backup should
	 *   go.  This directory will be created if not it does not exist.
	 *
	 * @exception SQLException Thrown on error
	 */
	public void backup(String backupDir) throws SQLException;

	/**
	 * Backup the database to a backup directory.  See on line documentation
	 * for more detail about how to use this feature.
	 *
	 * @param backupDir the directory where the database backup should
	 *   go.  This directory will be created if not it does not exist.
	 *
	 * @exception SQLException Thrown on error
	 */
	public void backup(File backupDir) throws SQLException;



	/**
	 * Backup the database to a backup directory and enable the log archive
	 * mode that will keep the archived log files required for roll-forward
	 * from this version backup.
	 * @param backupDir the directory name where the database backup should
	 *   go.  This directory will be created if not it does not exist.
	 * @param deleteOnlineArchivedLogFiles  If true deletes online archived log files
	 * that exist before this backup; otherwise they will not be deleted. 
	 * Deletion will occur only after backup is complete.
	 * @exception SQLException Thrown on error
	 */

	public void backupAndEnableLogArchiveMode(String backupDir,
											  boolean
											  deleteOnlineArchivedLogFiles) 
		throws SQLException;


	/**
	 * Backup the database to a backup directory and enable the log archive
	 * mode that will keep the archived log files required for roll-forward
	 * from this version backup.
	 * @param backupDir the directory name where the database backup should
	 *   go.  This directory will be created if not it does not exist.
	 * @param deleteOnlineArchivedLogFiles  If true deletes online archived log files
	 * that exist before this backup; otherwise they will not be deleted. 
	 * Deletion will occur only after backup is complete.
	 * @exception SQLException Thrown on error
	 */
	public void backupAndEnableLogArchiveMode(File backupDir, 
											  boolean
											  deleteOnlineArchivedLogFiles) 
		throws SQLException;
	

	/**
	 * Disables the log archival process, i.e No old log files
	 * will be kept around for a roll-forward recovery. Only restore that can 
	 * be performed after disabling log archive mode is version recovery.
	 * @param deleteOnlineArchivedLogFiles  If true deletes all online archived log files
	 * that exist before this call immediately; otherwise they will not be deleted.
	 * @exception SQLException Thrown on error
	 */
	public void disableLogArchiveMode(boolean deleteOnlineArchivedLogFiles) 
		throws SQLException;


	/**
	  * Freeze the database temporarily so a backup can be taken.
	  * <P>Please see Cloudscape on line documentation on backup and restore.
	  *
	  * @exception SQLException Thrown on error
	  */
	public void freeze() throws SQLException;

	/**
	  * Unfreeze the database after a backup has been taken.
	  * <P>Please see Cloudscape on line documentation on backup and restore.
	  *
	  * @exception SQLException Thrown on error
	  */
	public void unfreeze() throws SQLException;

	/**
	 * Checkpoints the database, that is, flushes all dirty data to disk.
	 * Records a checkpoint in the transaction log, if there is a log.
	 *
	 * @return	Nothing
	 *
	 * @exception SQLException Thrown on error
	 */
	public void checkpoint() throws SQLException;

	/**
	 * Get the Locale for this database.
	 */
	public Locale getLocale();

	/**
		Return the UUID of this database.
		@deprecated No longer supported.

	*/
	public UUID getId();
}	



