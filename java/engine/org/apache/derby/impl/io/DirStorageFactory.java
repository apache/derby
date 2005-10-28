/*

   Derby - Class org.apache.derby.impl.io.DirStorageFactory

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

import org.apache.derby.io.WritableStorageFactory;
import org.apache.derby.io.StorageFile;
import org.apache.derby.io.StorageRandomAccessFile;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.IOException;
import java.io.SyncFailedException;

import java.util.Properties;

/**
 * This class provides a disk based implementation of the StorageFactory interface. It is used by the
 * database engine to access persistent data and transaction logs under the directory (default) subsubprotocol.
 */

public class DirStorageFactory extends BaseStorageFactory
    implements WritableStorageFactory
{
    /**
     * Construct a StorageFile from a path name.
     *
     * @param path The path name of the file
     *
     * @return A corresponding StorageFile object
     */
    public final StorageFile newStorageFile( String path)
    {
        return newPersistentFile( path);
    }
    
    /**
     * Construct a StorageFile from a directory and file name.
     *
     * @param directoryName The directory part of the path name.
     * @param fileName The name of the file within the directory.
     *
     * @return A corresponding StorageFile object
     */
    public final StorageFile newStorageFile( String directoryName, String fileName)
    {
       return newPersistentFile( directoryName, fileName);
    }
    
    /**
     * Construct a StorageFile from a directory and file name.
     *
     * @param directoryName The directory part of the path name.
     * @param fileName The name of the file within the directory.
     *
     * @return A corresponding StorageFile object
     */
    public final StorageFile newStorageFile( StorageFile directoryName, String fileName)
    {
        return newPersistentFile( directoryName, fileName);
    }
    /**
     * Construct a persistent StorageFile from a path name.
     *
     * @param path The path name of the file. Guaranteed not to be in the temporary file directory. If null
     *             then the database directory should be returned.
     *
     * @return A corresponding StorageFile object
     */
    StorageFile newPersistentFile( String path)
    {
        if( path == null)
            return new DirFile( dataDirectory);
        return new DirFile(dataDirectory, path);
    }

    /**
     * Construct a persistent StorageFile from a directory and path name.
     *
     * @param directory The path name of the directory. Guaranteed not to be in the temporary file directory.
     *                  Guaranteed not to be null
     * @param fileName The name of the file within the directory. Guaranteed not to be null.
     *
     * @return A corresponding StorageFile object
     */
    StorageFile newPersistentFile( String directoryName, String fileName)
    {
        return new DirFile( separatedDataDirectory + directoryName, fileName);
    }

    /**
     * Construct a persistent StorageFile from a directory and path name.
     *
     * @param directory The path name of the directory. Guaranteed not to be to be null. Guaranteed to be
     *                  created by a call to one of the newPersistentFile methods.
     * @param fileName The name of the file within the directory. Guaranteed not to be null.
     *
     * @return A corresponding StorageFile object
     */
    StorageFile newPersistentFile( StorageFile directoryName, String fileName)
    {
        return new DirFile( (DirFile) directoryName, fileName);
    }

    /**
     * Force the data of an output stream out to the underlying storage. That is, ensure that
     * it has been made persistent. If the database is to be transient, that is, if the database
     * does not survive a restart, then the sync method implementation need not do anything.
     *
     * @param stream The stream to be synchronized.
     * @param metaData If true then this method must force both changes to the file's
     *          contents and metadata to be written to storage; if false, it need only force file content changes
     *          to be written. The implementation is allowed to ignore this parameter and always force out
     *          metadata changes.
     *
     * @exception IOException if an I/O error occurs.
     * @exception SyncFailedException Thrown when the buffers cannot be flushed,
     *            or because the system cannot guarantee that all the buffers have been
     *            synchronized with physical media.
     */
    public void sync( OutputStream stream, boolean metaData) throws IOException, SyncFailedException
    {
        ((FileOutputStream) stream).getFD().sync();
    }

    /**
     * This method tests whether the "rws" and "rwd" modes are implemented. If the "rws" method is supported
     * then the database engine will conclude that the write methods of "rws" mode StorageRandomAccessFiles are
     * slow but the sync method is fast and optimize accordingly.
     *
     * @return <b>true</b> if an StIRandomAccess file opened with "rws" or "rwd" modes immediately writes data to the
     *         underlying storage, <b>false</b> if not.
     */
    public boolean supportsRws()
    {
        return false;
    }

    public boolean isReadOnlyDatabase()
    {
        return false;
    }

    /**
     * Determine whether the storage supports random access. If random access is not supported then
     * it will only be accessed using InputStreams and OutputStreams (if the database is writable).
     *
     * @return <b>true</b> if the storage supports random access, <b>false</b> if it is writable.
     */
    public boolean supportsRandomAccess()
    {
        return true;
    }

    void doInit() throws IOException
    {
        if( dataDirectory != null)
        {
            File dataDirectoryFile = new File( dataDirectory);
            File databaseRoot = null;
            if( dataDirectoryFile.isAbsolute())
                databaseRoot = dataDirectoryFile;
            else if( home != null && dataDirectory.startsWith( home))
                databaseRoot = dataDirectoryFile;
            else
                databaseRoot = new File( home, dataDirectory);
            canonicalName = databaseRoot.getCanonicalPath();
            createTempDir();
            separatedDataDirectory = dataDirectory + getSeparator();
        }
        else if( home != null)
        {
            File root = new File( home);
            dataDirectory = root.getCanonicalPath();
            separatedDataDirectory = dataDirectory + getSeparator();
        }
    } // end of doInit
}
