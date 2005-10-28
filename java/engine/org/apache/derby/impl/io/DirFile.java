/*

   Derby - Class org.apache.derby.impl.io.DirFile

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

import org.apache.derby.io.StorageFile;
import org.apache.derby.io.StorageRandomAccessFile;

import org.apache.derby.iapi.services.sanity.SanityManager;

import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.FileOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;

/**
 * This class provides a disk based implementation of the StorageFile interface. It is used by the
 * database engine to access persistent data and transaction logs under the directory (default) subsubprotocol.
 */
class DirFile extends File implements StorageFile
{

    /**
     * Construct a DirFile from a path name.
     *
     * @param path The path name.
     */
    DirFile( String path)
    {
        super( path);
    }

    /**
     * Construct a DirFile from a directory name and a file name.
     *
     * @param directory The directory part of the path name.
     * @param fileName The name of the file within the directory.
     */
    DirFile( String directoryName, String fileName)
    {
        super( directoryName, fileName);
    }

    /**
     * Construct a DirFile from a directory name and a file name.
     *
     * @param directory The directory part of the path name.
     * @param fileName The name of the file within the directory.
     */
    DirFile( DirFile directoryName, String fileName)
    {
        super( (File) directoryName, fileName);
    }

    /**
     * Get the name of the parent directory if this name includes a parent.
     *
     * @return An StorageFile denoting the parent directory of this StorageFile, if it has a parent, null if
     *         it does not have a parent.
     */
    public StorageFile getParentDir()
    {
        String parent = getParent();
        if( parent == null)
            return null;
        return new DirFile( parent);
    }
    
    /**
     * Get the name of the directory of temporary files.
     *
     * @return The abstract name of the temp directory;
     */
    static StorageFile getTempDir() throws IOException
    {
        File temp = File.createTempFile("derby", "tmp");
        StorageFile parent = new DirFile( temp.getParent());
        temp.delete();

		return parent;
	} // End of getTempDir

    /**
     * Creates an output stream from a file name.
     *
     * @param name The file name.
     *
     * @return an output stream suitable for writing to the file.
     *
     * @exception FileNotFoundException if the file exists but is a directory
     *            rather than a regular file, does not exist but cannot be created, or
     *            cannot be opened for any other reason.
     */
    public OutputStream getOutputStream( ) throws FileNotFoundException
    {
        return new FileOutputStream( (File) this);
    }
    
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
    public OutputStream getOutputStream( final boolean append) throws FileNotFoundException
    {
        return new FileOutputStream( getPath(), append);
    }

    /**
     * Creates an input stream from a file name.
     *
     * @param name The file name.
     *
     * @return an input stream suitable for reading from the file.
     *
     * @exception FileNotFoundException if the file is not found.
     */
    public InputStream getInputStream( ) throws FileNotFoundException
    {
        return new FileInputStream( (File) this);
    }

    /**
     * Get an exclusive lock. This is used to ensure that two or more JVMs do not open the same database
     * at the same time.
     *
     * @param lockFile The name of the lock. In a file system implementation it will be the name of a
     *                 lock file.
     *
     * @return EXCLUSIVE_FILE_LOCK_NOT_AVAILABLE if the lock cannot be acquired because it is already held.<br>
     *    EXCLUSIVE_FILE_LOCK if the lock was successfully acquired.<br>
     *    NO_FILE_LOCK_SUPPORT if the system does not support exclusive locks.<br>
     */
    public synchronized int getExclusiveFileLock()
	{
		if (exists())
		{
			delete();
		}
		try
        {
			//Just create an empty file
			RandomAccessFile lockFileOpen = new RandomAccessFile( (File) this, "rw");
			lockFileOpen.getFD().sync( );
			lockFileOpen.close();
		}catch(IOException ioe)
		{
			// do nothing - it may be read only medium, who knows what the
			// problem is
			if (SanityManager.DEBUG)
			{
				SanityManager.THROWASSERT("Unable to create Exclusive Lock File " + getPath());
			}
		}
		
		return NO_FILE_LOCK_SUPPORT;
	} // end of getExclusiveFileLock

	/**
     * Release the resource associated with an earlier acquired exclusive lock
     *
     * @param lockFile the name of the lock file
     *
     * @see #getExclusiveFileLock
     */
	public synchronized void releaseExclusiveFileLock()
	{
		if( exists())
		{
			delete(); 
		}
	} // End of releaseExclusiveFileLock

    /**
     * Get a random access (read/write) file.
     *
     * @param name The name of the file.
     * @param mode "r", "rw", "rws", or "rwd". The "rws" and "rwd" modes specify
     *             that the data is to be written to persistent store, consistent with the
     *             java.io.RandomAccessFile class ("synchronized" with the persistent
     *             storage, in the file system meaning of the word "synchronized").  However
     *             the implementation is not required to implement the "rws" or "rwd"
     *             modes. The implementation may treat "rws" and "rwd" as "rw". It is up to
     *             the user of this interface to call the StorageRandomAccessFile.sync
     *             method. If the "rws" or "rwd" modes are supported and the
     *             RandomAccessFile was opened in "rws" or "rwd" mode then the
     *             implementation of StorageRandomAccessFile.sync need not do anything.
     *
     * @return an object that can be used for random access to the file.
     *
     * @exception IllegalArgumentException if the mode argument is not equal to one of "r", "rw".
     * @exception FileNotFoundException if the file exists but is a directory rather than a regular
     *              file, or cannot be opened or created for any other reason .
     */
    public StorageRandomAccessFile getRandomAccessFile( String mode) throws FileNotFoundException
    {
        // Assume that modes "rws" and "rwd" are not supported.
        if( "rws".equals( mode) || "rwd".equals( mode))
            mode = "rw";
        return new DirRandomAccessFile( (File) this, mode);
    } // end of getRandomAccessFile

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
        return super.renameTo( (File) newName);
    }

    /**
     * Deletes the named file and, if it is a directory, all the files and directories it contains.
     *
     * @return <b>true</b> if the named file or directory is successfully deleted, <b>false</b> if not
     */
    public boolean deleteAll()
    {
        if( !exists())
            return false;
        if( isDirectory())
        {
            String[] childList = super.list();
            String parentName = getPath();
            for( int i = 0; i < childList.length; i++)
            {
                if( childList[i].equals( ".") || childList[i].equals( ".."))
                    continue;
                DirFile child = new DirFile( parentName, childList[i]);
                if( ! child.deleteAll())
                    return false;
            }
        }
        return delete();
    } // end of deleteAll
}
