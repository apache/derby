/*

   Derby - Class org.apache.derby.database.Database

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package org.apache.derby.database;

/*
//IC see: https://issues.apache.org/jira/browse/DERBY-2400
  The org.apache.derby.iapi.db.Database interface is all the externally 
  available methods on a database.  These are methods that might be called from 
  an SQL-J CALL statement. 

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
 *  @see org.apache.derby.iapi.db.Factory
 */
public interface Database
{
    // where the Lucene plugin writes its indexes
    public  static  final   String  LUCENE_DIR = "LUCENE";

	/**
	 * Tells whether the Database is configured as read-only, or the
	 * Database was started in read-only mode.
	 *
	 * @return	TRUE means the Database is read-only, FALSE means it is
	 *		not read-only.
	 */
	public boolean		isReadOnly();

    /**
     * Backup the database to a backup directory.  See online documentation
     * for more detail about how to use this feature.
     *
     * @param backupDir the directory name where the database backup should
     *         go.  This directory will be created if not it does not exist.
     * @param wait if <tt>true</tt>, waits for  all the backup blocking 
     *             operations in progress to finish.
     * @exception SQLException Thrown on error
     */
    public void backup(String backupDir, boolean wait) 
        throws SQLException;


    /**
     * Backup the database to a backup directory and enable the log archive
     * mode that will keep the archived log files required for roll-forward
     * from this version backup.
     *
     * @param backupDir                     The directory name where the 
     *                                      database backup should go.  This 
     *                                      directory will be created if it 
     *                                      does not exist.
     *
     * @param deleteOnlineArchivedLogFiles  If true deletes online archived log
     *                                      files that exist before this backup;
     *                                      otherwise they will not be deleted. 
     *
     *                                      Deletion will occur only after 
     *                                      backup is complete.
     *
     * @param wait                          if <tt>true</tt>, waits for all 
     *                                      the backup blocking operations in 
     *                                      progress to finish.
     *
     * @exception SQLException Thrown on error
     */
    public void backupAndEnableLogArchiveMode(
    String  backupDir,
    boolean deleteOnlineArchivedLogFiles,
    boolean wait) 
        throws SQLException;

	/**
	 * Disables the log archival process, i.e No old log files
	 * will be kept around for a roll-forward recovery. Only restore that can 
	 * be performed after disabling log archive mode is version recovery.
     *
	 * @param deleteOnlineArchivedLogFiles  If true deletes all online archived
     *                                      log files that exist before this 
     *                                      call immediately; otherwise they 
     *                                      will not be deleted.
     *
	 * @exception SQLException Thrown on error
	 */
	public void disableLogArchiveMode(boolean deleteOnlineArchivedLogFiles) 
		throws SQLException;
//IC see: https://issues.apache.org/jira/browse/DERBY-239
//IC see: https://issues.apache.org/jira/browse/DERBY-523

	/**
	  * Freeze the database temporarily so a backup can be taken.
	  * <P>Please see the Derby documentation on backup and restore.
	  *
	  * @exception SQLException Thrown on error
	  */
	public void freeze() throws SQLException;

	/**
	  * Unfreeze the database after a backup has been taken.
	  * <P>Please see the Derby documentation on backup and restore.
	  *
	  * @exception SQLException Thrown on error
	  */
	public void unfreeze() throws SQLException;

	/**
	 * Checkpoints the database, that is, flushes all dirty data to disk.
	 * Records a checkpoint in the transaction log, if there is a log.
	 *
	 * @exception SQLException Thrown on error
	 */
	public void checkpoint() throws SQLException;

	/**
	 * Get the Locale for this database.
     *
     * @return the Locale for this database.
	 */
	public Locale getLocale();

	/**
		Return the UUID of this database.
		@deprecated No longer supported.

        @return the UUID of this database.
	*/
	public UUID getId();
}	



