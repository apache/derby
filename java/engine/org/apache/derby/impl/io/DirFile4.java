/*

   Derby - Class org.apache.derby.impl.io.DirFile4

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

import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.io.StorageFile;
import org.apache.derby.io.StorageRandomAccessFile;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;

/**
 * This class implements the StorageFile interface using features of Java 1.4 not available in earlier
 * versions of Java.
 */
class DirFile4 extends DirFile
{

    private RandomAccessFile lockFileOpen;
    private FileChannel lockFileChannel;
    private FileLock dbLock;

	private final boolean rwsOK;

    /**
     * Construct a DirFile from a path name.
     *
     * @param path The path name.
     */
    DirFile4( String path, boolean rwsOK)
    {
        super( path);
		this.rwsOK = rwsOK;
    }

    /**
     * Construct a DirFile from a directory name and a file name.
     *
     * @param directory The directory part of the path name.
     * @param fileName The name of the file within the directory.
     */
    DirFile4( String directoryName, String fileName, boolean rwsOK)
    {
        super( directoryName, fileName);
		this.rwsOK = rwsOK;
    }

    /**
     * Construct a DirFile from a directory name and a file name.
     *
     * @param directory The directory part of the path name.
     * @param fileName The name of the file within the directory.
     */
    DirFile4( DirFile directoryName, String fileName, boolean rwsOK)
    {
        super( directoryName, fileName);
		this.rwsOK = rwsOK;
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
        return new DirFile4( parent, rwsOK);
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
        return new FileOutputStream( (File) this, append);
    }

    public synchronized int getExclusiveFileLock()
    {
		boolean validExclusiveLock = false;
		int status;

		/*
		** There can be  a scenario where there is some other JVM that is before jkdk1.4
		** had booted the system and jdk1.4 trying to boot it, in this case we will get the 
		** Exclusive Lock even though some other JVM has already booted the database. But
		** the lock is not a reliable one , so we should  still throw the warning.
		** The Way we identify this case is if "dbex.lck" file size  is differen
		** for pre jdk1.4 jvms and jdk1.4 or above.
 		** Zero size "dbex.lck" file  is created by a jvm i.e before jdk1.4 and
        ** File created by jdk1.4 or above writes EXCLUSIVE_FILE_LOCK value into the file.
		** If we are unable to acquire the lock means other JVM that
		** currently booted the system is also JDK1.4 or above;
		** In this case we could confidently throw a exception instead of 
		** of a warning.
		**/

		try
		{
			//create the file that us used to acquire exclusive lock if it does not exists.
			if(createNewFile())
			{
				validExclusiveLock = true;
			}	
			else
			{
				if(length() > 0)
					validExclusiveLock = true;
			}

			//If we can acquire a reliable exclusive lock , try to get it.
			if(validExclusiveLock)
			{
				lockFileOpen = new RandomAccessFile((File) this, "rw");
				lockFileChannel = lockFileOpen.getChannel();
				dbLock =lockFileChannel.tryLock();
				if(dbLock == null)
				{
					lockFileChannel.close();
					lockFileChannel=null;
					lockFileOpen.close();
					lockFileOpen = null;
					status = EXCLUSIVE_FILE_LOCK_NOT_AVAILABLE;
				}
				else
				{	
					lockFileOpen.writeInt(EXCLUSIVE_FILE_LOCK);
					lockFileChannel.force(true);
					status = EXCLUSIVE_FILE_LOCK;
				}
			}
			else
			{
				status = NO_FILE_LOCK_SUPPORT;
			}
		
		}catch(IOException ioe)
		{
			// do nothing - it may be read only medium, who knows what the
			// problem is

			//release all the possible resource we created in this functions.
			releaseExclusiveFileLock();
			status = NO_FILE_LOCK_SUPPORT;
			if (SanityManager.DEBUG)
			{
				SanityManager.THROWASSERT("Unable to Acquire Exclusive Lock on "
										  + getPath());
			}
		}
    
		return status;
	} // end of getExclusiveFileLock

    public synchronized void releaseExclusiveFileLock()
    {
		try
		{
			if(dbLock!=null)
			{
				dbLock.release();
				dbLock =null;
			}
		
			if(lockFileChannel !=null)
			{
				lockFileChannel.close();
				lockFileChannel = null;
			}

			if(lockFileOpen !=null)
			{
				lockFileOpen.close();
				lockFileOpen = null;
			}

			//delete the exclusive lock file name.
            super.releaseExclusiveFileLock();
		}catch (IOException ioe)
		{ 
			// do nothing - it may be read only medium, who knows what the
			// problem is
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
        if(!rwsOK && "rws".equals( mode) || "rwd".equals( mode))
            mode = "rw";
        return new DirRandomAccessFile4( (File) this, mode);
    } // end of getRandomAccessFile
}
