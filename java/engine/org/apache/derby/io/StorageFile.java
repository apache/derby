/*

   Derby - Class org.apache.derby.io.StorageFile

   Copyright 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derby.io;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * This interface abstracts file naming. Any method in this interface
 * that also appears in the java.io.File class should behave as the java.io.File method does.
 *<p>
 * When used by the database engine all files will be under either the database
 * directory, specified by the databaseName argument of the {@link
 * StorageFactory#init StorageFactory.init} method, or under the temporary file
 * directory returned by the {@link StorageFactory#getTempDir
 * StorageFactory.getTempDir} method. All relative path names are relative to
 * the database directory.
 *<p>
 * The database engine will call this interface's methods from its own privilege blocks.
 *<p>
 * Different threads may operate on the same underlying file at the same time, either through the
 * same or different StorageFile objects. The StiFile implementation must be capable of handling this.
 *<p>
 * @see <a href="http://java.sun.com/j2se/1.4.2/docs/api/java/io/File.html">java.io.File</a>
 */
public interface StorageFile
{

	//constant values related to exclusive lock mechanism
	public static final int NO_FILE_LOCK_SUPPORT              = 0;
	public static final int EXCLUSIVE_FILE_LOCK               = 1;
	public static final int EXCLUSIVE_FILE_LOCK_NOT_AVAILABLE = 2;

    /**
     * Get the names of all files and sub-directories in the directory named by this path name.
     *
     * This method is only used in a writable database.
     *
     * @return An array of the names of the files and directories in this
     *         directory denoted by this abstract pathname. The returned array will have length 0
     *         if this directory is empty. Returns null if this StorageFile is not  a directory, or
     *         if an I/O error occurs. The return value is undefined if the database is read-only.
     */
    public String[] list();

    /**
     * Determine whether the named file is writable.
     *
     * @return <b>true</b> if the file exists and is writable, <b>false</b> if not.
     */
    public boolean canWrite();

    /**
     * Tests whether the named file exists.
     *
     * @return <b>true</b> if the named file exists, <b>false</b> if not.
     */
    public boolean exists();

    /**
     * Tests whether the named file is a directory, or not. This is only called in writable storage factories.
     *
     * @return <b>true</b> if named file exists and is a directory, <b>false</b> if not.
     *          The return value is undefined if the storage is read-only.
     */
    public boolean isDirectory();

    /**
     * Deletes the named file or empty directory. This method does not delete non-empty directories.
     *
     * @return <b>true</b> if the named file or directory is successfully deleted, <b>false</b> if not
     */
    public boolean delete();

    /**
     * Deletes the named file and, if it is a directory, all the files and directories it contains.
     *
     * @return <b>true</b> if the named file or directory is successfully deleted, <b>false</b> if not
     */
    public boolean deleteAll();

    /**
     * Converts this StorageFile into a pathname string. The character returned by StorageFactory.getSeparator()
     * is used to separate the directory and file names in the sequence.
     *
     *<p>
     *<b>The returned path may include the database directory. Therefore it cannot be directly used to make an StorageFile
     * equivalent to this one.</b>
     *
     * @return The pathname as a string.
     *
     * @see StorageFactory#getSeparator
     */
    public String getPath();

    /**
     * Converts this StorageFile into a canonical pathname string. The form of the canonical path is system dependent.
     *
     * @return The pathname as a string.
     *
     * @exception IOException if an I/O error occurred while finding the canonical name
     */
    public String getCanonicalPath() throws IOException;

    /**
     * @return The last segment in the path name, "" if the path name sequence is empty.
     */
    public String getName();
    
    /**
     * If the named file does not already exist then create it as an empty normal file.
     *
     * The implementation
     * must synchronize with other threads accessing the same file (in the same or a different process).
     * If two threads both attempt to create a file with the same name
     * at the same time then at most one should succeed.
     *
     * @return <b>true</b> if this thread's invocation of createNewFile successfully created the named file;
     *         <b>false</b> if not, i.e. <b>false</b> if the named file already exists or if another concurrent thread created it.
     *
     * @exception IOException - If the directory does not exist or some other I/O error occurred
     */
    public boolean createNewFile() throws IOException;

    /**
     * Rename the file denoted by this name. Note that StorageFile objects are immutable. This method
     * renames the underlying file, it does not change this StorageFile object. The StorageFile object denotes the
     * same name as before, however the exists() method will return false after the renameTo method
     * executes successfully.
     *
     *<p>It is not specified whether this method will succeed if a file already exists under the new name.
     *
     * @param newName the new name.
     *
     * @return <b>true</b> if the rename succeeded, <b>false</b> if not.
     */
    public boolean renameTo( StorageFile newName);
    
    /**
     * Creates the named directory.
     *
     * @return <b>true</b> if the directory was created; <b>false</b> if not.
     */
    public boolean mkdir();

    /**
     * Creates the named directory, and all nonexistent parent directories.
     *
     * @return <b>true</b> if the directory was created, <b>false</b> if not
     */
    public boolean mkdirs();

    /**
     * Returns the length of the named file if it is not a directory. The return value is not specified
     * if the file is a directory.
     *
     * @return The length, in bytes, of the named file if it exists and is not a directory,
     *         0 if the file does not exist, or any value if the named file is a directory.
     */
    public long length();

    /**
     * Get the name of the parent directory if this name includes a parent.
     *
     * @return An StorageFile denoting the parent directory of this StorageFile, if it has a parent, null if
     *         it does not have a parent.
     */
    public StorageFile getParentDir();

    /**
     * Make the named file or directory read-only. This interface does not specify whether this
     * also makes the file undeletable.
     *
     * @return <b>true</b> if the named file or directory was made read-only, or it already was read-only;
     *         <b>false</b> if not.
     */
    public boolean setReadOnly();

    /**
     * Creates an output stream from a file name. If a normal file already exists with this name it
     * will first be truncated to zero length.
     *
     * @return an output stream suitable for writing to the file.
     *
     * @exception FileNotFoundException if the file exists but is a directory
     *            rather than a regular file, does not exist but cannot be created, or
     *            cannot be opened for any other reason.
     */
    public OutputStream getOutputStream( ) throws FileNotFoundException;

    /**
     * Creates an output stream from a file name.
     *
     * @param append If true then data will be appended to the end of the file, if it already exists.
     *               If false and a normal file already exists with this name the file will first be truncated
     *               to zero length.
     *
     * @return an output stream suitable for writing to the file.
     *
     * @exception FileNotFoundException if the file exists but is a directory
     *            rather than a regular file, does not exist but cannot be created, or
     *            cannot be opened for any other reason.
     */
    public OutputStream getOutputStream( boolean append) throws FileNotFoundException;
    
    /**
     * Creates an input stream from a file name.
     *
     * @return an input stream suitable for reading from the file.
     *
     * @exception FileNotFoundException if the file is not found.
     */
    public InputStream getInputStream( ) throws FileNotFoundException;

    /**
     * Get an exclusive lock with this name. This is used to ensure that two or more JVMs do not open the same database
     * at the same time.
     *
     * @return EXCLUSIVE_FILE_LOCK_NOT_AVAILABLE if the lock cannot be acquired because it is already held.<br>
     *    EXCLUSIVE_FILE_LOCK if the lock was successfully acquired.<br>
     *    NO_FILE_LOCK_SUPPORT if the system does not support exclusive locks.<br>
     */
    public int getExclusiveFileLock();

	/**
     * Release the resource associated with an earlier acquired exclusive lock
     *
     * @see #getExclusiveFileLock
     */
	public void releaseExclusiveFileLock();

    /**
     * Get a random access file.
     *
     * This method is not called if the StorageFactory is read only. It is unspecified if the StorageFactory
     * that created it is not a WritableStorageFactory.
     *
     * @param mode "r", "rw", "rws", or "rwd". The "rws" and "rwd" modes specify
     *             that the data is to be written to persistent store, consistent with the
     *             java.io.RandomAccessFile class ("synchronized" with the persistent
     *             storage, in the file system meaning of the word "synchronized").  However
     *             the implementation is not required to implement the "rws" or "rwd"
     *             modes. If the "rws" andr "rwd" modes are supported then the supportsRws() method
     *             of the StorageFactory returns true. If supportsRws() returns false then the
     *             implementation may treat "rws" and "rwd" as "rw". It is up to
     *             the user of this interface to call the StorageRandomAccessFile.sync
     *             method if necessary. However, if the "rws" or "rwd" modes are supported and the
     *             RandomAccessFile was opened in "rws" or "rwd" mode then the
     *             implementation of StorageRandomAccessFile.sync need not do anything.
     *
     * @return an object that can be used for random access to the file.
     *
     * @exception IllegalArgumentException if the mode argument is not equal to one of "r", "rw", "rws", or "rwd".
     * @exception FileNotFoundException if the file exists but is a directory rather than a regular
     *              file, or cannot be opened or created for any other reason .
     *
     * @see <a href="http://java.sun.com/j2se/1.4.2/docs/api/java/io/RandomAccessFile.html">java.io.RandomAccessFile</a>
     */
    public StorageRandomAccessFile getRandomAccessFile( String mode) throws FileNotFoundException;

    /**
     * Get the file name for diagnostic purposes. Usually the same as getPath().
     *
     * @return the file name
     */
    public String toString();
}
