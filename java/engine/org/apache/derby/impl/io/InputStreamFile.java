/*

   Derby - Class org.apache.derby.impl.io.InputStreamFile

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

package org.apache.derby.impl.io;

import org.apache.derby.io.StorageFactory;
import org.apache.derby.io.StorageFile;
import org.apache.derby.io.StorageRandomAccessFile;

import org.apache.derby.iapi.services.sanity.SanityManager;

import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.IOException;
import java.io.FileNotFoundException;

/**
 * This class provides the base for read-only stream implementations of the StorageFile interface. It is used with the
 * classpath, jar, http, and https subsubprotocols
 */
abstract class InputStreamFile implements StorageFile
{

    final String path;
    final int nameStart; // getName() = path.substring( nameStart)
    final BaseStorageFactory storageFactory;

    InputStreamFile( BaseStorageFactory storageFactory, String path)
    {
        this.storageFactory = storageFactory;
        if( path == null || path.length() == 0)
        {
            this.path = storageFactory.dataDirectory;
            nameStart = -1;
        }
        else
        {
            StringBuffer sb = new StringBuffer( storageFactory.separatedDataDirectory);
            if( File.separatorChar != '/')
                sb.append( path.replace( File.separatorChar, '/'));
            else
                sb.append( path);
            this.path = sb.toString();
            nameStart = this.path.lastIndexOf( '/') + 1;
        }
    }

    InputStreamFile( BaseStorageFactory storageFactory, String parent, String name)
    {
        this.storageFactory = storageFactory;
        StringBuffer sb = new StringBuffer( storageFactory.separatedDataDirectory);
        if( File.separatorChar != '/')
        {
            sb.append( parent.replace( File.separatorChar, '/'));
            sb.append( '/');
            sb.append( name.replace( File.separatorChar, '/'));
        }
        else
        {
            sb.append( parent);
            sb.append( '/');
            sb.append( name);
        }
        path = sb.toString();
        nameStart = this.path.lastIndexOf( '/') + 1;
    }

    InputStreamFile( InputStreamFile dir, String name)
    {
        this.storageFactory = dir.storageFactory;
        StringBuffer sb = new StringBuffer( dir.path);
        sb.append( '/');
        if( File.separatorChar != '/')
            sb.append( name.replace( File.separatorChar, '/'));
        else
            sb.append( name);
        path = sb.toString();
        nameStart = this.path.lastIndexOf( '/') + 1;
    }

    InputStreamFile( BaseStorageFactory storageFactory, String child, int pathLen)
    {
        this.storageFactory = storageFactory;
        path = child.substring( 0, pathLen);
        nameStart = this.path.lastIndexOf( '/') + 1;
    }

    public boolean equals( Object other)
    {
        if( other == null || ! getClass().equals( other.getClass()))
            return false;
        InputStreamFile otherFile = (InputStreamFile) other;
        return path.equals( otherFile.path);
    }

    public int hashCode()
    {
        return path.hashCode();
    }

    /**
     * Get the names of all files and sub-directories in the directory named by this path name.
     *
     * @return An array of the names of the files and directories in this
     *         directory denoted by this abstract pathname. The returned array will have length 0
     *         if this directory is empty. Returns null if this StorageFile is not  a directory, or
     *         if an I/O error occurs.
     */
    public String[] list()
    {
        return null;
    }

    /**
     * Determine whether the named file is writable.
     *
     * @return <b>true</b> if the file exists and is writable, <b>false</b> if not.
     */
    public boolean canWrite()
    {
        return false;
    }

    /**
     * Tests whether the named file exists.
     *
     * @return <b>true</b> if the named file exists, <b>false</b> if not.
     */
    public abstract boolean exists();

    /**
     * Tests whether the named file is a directory, or not. This is only called in writable storage factories.
     *
     * @return <b>true</b> if named file exists and is a directory, <b>false</b> if not.
     *          The return value is undefined if the storage is read-only.
     */
    public boolean isDirectory()
    {
        return false;
    }

    /**
     * Deletes the named file or empty directory. This method does not delete non-empty directories.
     *
     * @return <b>true</b> if the named file or directory is successfully deleted, <b>false</b> if not
     */
    public boolean delete()
    {
        return false;
    }

    /**
     * Deletes the named file and, if it is a directory, all the files and directories it contains.
     *
     * @return <b>true</b> if the named file or directory is successfully deleted, <b>false</b> if not
     */
    public boolean deleteAll()
    {
        return false;
    }

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
    public String getPath()
    {
        if( File.separatorChar != '/')
            return path.replace( '/', File.separatorChar);
        return path;
    } // end of getPath

    public String getCanonicalPath() throws IOException
    {
        return storageFactory.getCanonicalName() + "/" + path;
    }
    
    /**
     * @return The last segment in the path name, "" if the path name sequence is empty.
     */
    public String getName()
    {
        return (nameStart < 0) ? "" : path.substring( nameStart);
    }

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
    public boolean createNewFile() throws IOException
    {
        throw new IOException( "createNewFile called in a read-only file system.");
    }

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
    public boolean renameTo( StorageFile newName)
    {
        return false;
    }
    
    /**
     * Creates the named directory.
     *
     * @return <b>true</b> if the directory was created; <b>false</b> if not.
     */
    public boolean mkdir()
    {
        return false;
    }

    /**
     * Creates the named directory, and all nonexistent parent directories.
     *
     * @return <b>true</b> if the directory was created, <b>false</b> if not
     */
    public boolean mkdirs()
    {
        return false;
    }

    /**
     * Returns the length of the named file if it is not a directory. The return value is not specified
     * if the file is a directory.
     *
     * @return The length, in bytes, of the named file if it exists and is not a directory,
     *         0 if the file does not exist, or any value if the named file is a directory.
     */
    public long length()
    {
        try
        {
            InputStream is = getInputStream();
            if( is == null)
                return 0;
            long len = is.available();
            is.close();
            return len;
        }
        catch( IOException e){ return 0;}
    } // end of length

    /**
     * Get the name of the parent directory if this name includes a parent.
     *
     * @return An StorageFile denoting the parent directory of this StorageFile, if it has a parent, null if
     *         it does not have a parent.
     */
    public StorageFile getParentDir()
    {
        if( path.length() <= storageFactory.separatedDataDirectory.length())
            return null;
        return getParentDir( path.lastIndexOf( '/'));
    }

    /**
     * Get the parent of this file.
     *
     * @param pathLen the length of the parent's path name.
     */
    abstract StorageFile getParentDir( int pathLen);

    /**
     * Make the named file or directory read-only. This interface does not specify whether this
     * also makes the file undeletable.
     *
     * @return <b>true</b> if the named file or directory was made read-only, or it already was read-only;
     *         <b>false</b> if not.
     */
    public boolean setReadOnly()
    {
        return true;
    }

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
    public OutputStream getOutputStream( ) throws FileNotFoundException
    {
        throw new FileNotFoundException( "Attempt to write into a read only file system.");
    }

    
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
    public OutputStream getOutputStream( boolean append) throws FileNotFoundException
    {
        throw new FileNotFoundException( "Attempt to write into a read only file system.");
    }

    
    /**
     * Creates an input stream from a file name.
     *
     * @return an input stream suitable for reading from the file.
     *
     * @exception FileNotFoundException if the file is not found.
     */
    abstract public InputStream getInputStream( ) throws FileNotFoundException;

    /**
     * Get an exclusive lock with this name. This is used to ensure that two or more JVMs do not open the same database
     * at the same time.
     *
     * @return EXCLUSIVE_FILE_LOCK_NOT_AVAILABLE if the lock cannot be acquired because it is already held.<br>
     *    EXCLUSIVE_FILE_LOCK if the lock was successfully acquired.<br>
     *    NO_FILE_LOCK_SUPPORT if the system does not support exclusive locks.<br>
     */
    public int getExclusiveFileLock()
    {
        return NO_FILE_LOCK_SUPPORT;
    }

	/**
     * Release the resource associated with an earlier acquired exclusive lock
     *
     * @see #getExclusiveFileLock
     */
	public void releaseExclusiveFileLock()
    {}

    /**
     * Get a random access file.
     *
     * @param name The name of the file.
     * @param mode "r", "rw", "rws", or "rwd". The "rws" and "rwd" modes specify
     *             that the data is to be written to persistent store, consistent with the
     *             java.io.RandomAccessFile class ("synchronized" with the persistent
     *             storage, in the file system meaning of the word "synchronized").  However
     *             the implementation is not required to implement the "rws" or "rwd"
     *             modes. The implementation may treat "rws" and "rwd" as "rw". It is up to
     *             the user of this interface to call the StorageRandomAccessFile.sync
     *             method. However, if the "rws" or "rwd" modes are supported and the
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
    public StorageRandomAccessFile getRandomAccessFile( String mode) throws FileNotFoundException
    {
        if( SanityManager.DEBUG)
            SanityManager.NOTREACHED();
        return null;
    }

    /**
     * Get the file name for diagnostic purposes. Usually the same as getPath().
     *
     * @return the file name
     */
    public String toString()
    {
        return path;
    }
}
