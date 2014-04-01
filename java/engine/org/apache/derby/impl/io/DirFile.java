/*

   Derby - Class org.apache.derby.impl.io.DirFile

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

package org.apache.derby.impl.io;

import org.apache.derby.io.StorageFile;
import org.apache.derby.io.StorageRandomAccessFile;

import org.apache.derby.shared.common.sanity.SanityManager;

import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.FileOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.nio.channels.AsynchronousCloseException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.FileUtil;
import org.apache.derby.iapi.util.InterruptStatus;

/**
 * This class provides a disk based implementation of the StorageFile interface. It is used by the
 * database engine to access persistent data and transaction logs under the directory (default) subsubprotocol.
 */
class DirFile extends File implements StorageFile
{
    private RandomAccessFile lockFileOpen;
    private FileChannel lockFileChannel;
    private FileLock dbLock;

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
     * @param directoryName The directory part of the path name.
     * @param fileName The name of the file within the directory.
     */
    DirFile( String directoryName, String fileName)
    {
        super( directoryName, fileName);
    }

    /**
     * Construct a DirFile from a directory name and a file name.
     *
     * @param directoryName The directory part of the path name.
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
     * Creates an output stream from a file name.
     *
     * @return an output stream suitable for writing to the file.
     *
     * @exception FileNotFoundException if the file exists but is a directory
     *            rather than a regular file, does not exist but cannot be created, or
     *            cannot be opened for any other reason.
     */
    public OutputStream getOutputStream( ) throws FileNotFoundException
    {
        return getOutputStream(false);
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
        boolean exists = exists();
        OutputStream result = new FileOutputStream(this, append);

        if (!exists) {
            try {
                limitAccessToOwner();
            } catch (FileNotFoundException fnfe) {
                // Throw FileNotFoundException unchanged.
                throw fnfe;
            } catch (IOException ioe) {
                // Other IOExceptions should be wrapped, since
                // FileNotFoundException is the only one we are allowed
                // to throw here.
                FileNotFoundException e = new FileNotFoundException();
                e.initCause(ioe);
                throw e;
            }
        }

        return result;
    }

    /**
     * Creates an input stream from a file name.
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
     *
     * @return EXCLUSIVE_FILE_LOCK_NOT_AVAILABLE if the lock cannot be acquired because it is already held.<br>
     *    EXCLUSIVE_FILE_LOCK if the lock was successfully acquired.<br>
     *    NO_FILE_LOCK_SUPPORT if the system does not support exclusive locks.<br>
     */
    public synchronized int getExclusiveFileLock() throws StandardException
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
            if (createNewFile())
            {
                validExclusiveLock = true;
            }
            else if (length() > 0)
            {
                validExclusiveLock = true;
            }

            //If we can acquire a reliable exclusive lock , try to get it.
            if (validExclusiveLock)
            {
                int retries = InterruptStatus.MAX_INTERRUPT_RETRIES;
                while (true) {
                    lockFileOpen = new RandomAccessFile((File) this, "rw");
                    limitAccessToOwner(); // tamper-proof..
                    lockFileChannel = lockFileOpen.getChannel();

                    try {
                        dbLock =lockFileChannel.tryLock();
                        if(dbLock == null) {
                            lockFileChannel.close();
                            lockFileChannel=null;
                            lockFileOpen.close();
                            lockFileOpen = null;
                            status = EXCLUSIVE_FILE_LOCK_NOT_AVAILABLE;
                        } else {
                            lockFileOpen.writeInt(EXCLUSIVE_FILE_LOCK);
                            lockFileChannel.force(true);
                            status = EXCLUSIVE_FILE_LOCK;
                        }
                    } catch (AsynchronousCloseException e) {
                        // JDK bug 6979009: use AsynchronousCloseException
                        // instead of the logically correct
                        // ClosedByInterruptException

                        InterruptStatus.setInterrupted();
                        lockFileOpen.close();

                        if (retries-- > 0) {
                            continue;
                        } else {
                            throw e;
                        }
                    }

                    break;
                }
            }
            else
            {
                status = NO_FILE_LOCK_SUPPORT;
            }

        } catch(IOException ioe)
        {
            // do nothing - it may be read only medium, who knows what the
            // problem is

            //release all the possible resource we created in this functions.
            releaseExclusiveFileLock();
            status = NO_FILE_LOCK_SUPPORT;
            if (SanityManager.DEBUG)
            {
                SanityManager.THROWASSERT("Unable to Acquire Exclusive Lock on "
                                          + getPath(), ioe);
            }
        } catch (OverlappingFileLockException ofle)
        {
            //
            // Under Java 6 and later, this exception is raised if the database
            // has been opened by another Derby instance in a different
            // ClassLoader in this VM. See DERBY-700.
            //
            // The OverlappingFileLockException is raised by the
            // lockFileChannel.tryLock() call above.
            //
            try {
                lockFileChannel.close();
                lockFileOpen.close();
            } catch (IOException e)
            {
                if (SanityManager.DEBUG)
                {
                    SanityManager.THROWASSERT("Error closing file channel "
                                              + getPath(), e);
                }
            }
            lockFileChannel = null;
            lockFileOpen = null;
            status = EXCLUSIVE_FILE_LOCK_NOT_AVAILABLE;
        }

        return status;
    } // end of getExclusiveFileLock

	/**
     * Release the resource associated with an earlier acquired exclusive lock
     *
     * @see #getExclusiveFileLock
     */
    public synchronized void releaseExclusiveFileLock()
    {
        try
        {
            if (dbLock!=null)
            {
                dbLock.release();
                dbLock = null;
            }

            if (lockFileChannel != null)
            {
                lockFileChannel.close();
                lockFileChannel = null;
            }

            if (lockFileOpen != null)
            {
                lockFileOpen.close();
                lockFileOpen = null;
            }

            // delete the exclusive lock file name.
            if (exists())
            {
                delete();
            }
        }
        catch (IOException ioe)
        {
            // do nothing - it may be read only medium, who knows what the
            // problem is
        }
    } // End of releaseExclusiveFileLock

    /**
     * Get a random access (read/write) file.
     *
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
        return new DirRandomAccessFile( (File) this, mode);
    } // end of getRandomAccessFile

    /**
     * Rename the file denoted by this name. 
     *
     * Note that StorageFile objects are immutable. This method renames the 
     * underlying file, it does not change this StorageFile object. The 
     * StorageFile object denotes the same name as before, however the exists()
     * method will return false after the renameTo method executes successfully.
     *
     * <p>
     * It is not specified whether this method will succeed if a file 
     * already exists under the new name.
     *
     * @param newName the new name.
     *
     * @return <b>true</b> if the rename succeeded, <b>false</b> if not.
     */
    public boolean renameTo( StorageFile newName)
    {
        boolean rename_status = super.renameTo( (File) newName);
        int     retry_count   = 1;

        while (!rename_status && (retry_count <= 5))
        {
            // retry operation, hoping a temporary I/O resource issue is 
            // causing the failure.

            try
            {
                Thread.sleep(1000 * retry_count);
            }
            catch (InterruptedException ie)
            {
                // This thread received an interrupt as well, make a note.
                InterruptStatus.setInterrupted();
            }

            rename_status = super.renameTo((File) newName);

            retry_count++;
        }

        return(rename_status);
    }

    /**
     * Deletes the named file and, if it is a directory, all the files and directories it contains.
     *
     * @return <b>true</b> if the named file or directory is successfully deleted, <b>false</b> if not
     */
    public boolean deleteAll()
    {
        // Nothing to do if the file doesn't exist.
        if (!exists()) {
            return false;
        }

        // If the file is a directory, delete its contents recursively.
        // File.list() will return null if it is not a directory, or if the
        // contents of the directory cannot be read. Skip the recursive step
        // in both of those cases. If it turns out that the file in fact is a
        // directory, and we couldn't delete its contents, the delete() call
        // at the end of this method will return false to notify the caller
        // that the directory could not be deleted.
        String[] childList = super.list();
        if (childList != null)
        {
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

        // Finally, attempt to delete the file (or directory) and return
        // whether or not we succeeded.
        return delete();
    } // end of deleteAll

    public void limitAccessToOwner() throws IOException {
        FileUtil.limitAccessToOwner(this);
    }
}
