/*

   Derby - Class org.apache.derby.impl.io.BaseStorageFactory

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

import org.apache.derby.iapi.store.raw.data.DataFactory;

import org.apache.derby.io.StorageFactory;
import org.apache.derby.io.StorageFile;

import java.io.File;
import java.io.IOException;

/**
 * This class provides a base for implementations of the StorageFactory interface. It is used by the
 * database engine to access persistent data and transaction logs under the directory (default) subsubprotocol.
 */

abstract class BaseStorageFactory implements StorageFactory
{

    String home;
    protected StorageFile tempDir;
    protected String tempDirPath;
    protected String dataDirectory;
    protected String separatedDataDirectory; // dataDirectory + separator
    protected String uniqueName;
    protected String canonicalName;
    private static final String TEMP_DIR_PREFIX = "derbytmp_";

    /**
     * Most of the initialization is done in the init method.
     */
    BaseStorageFactory()
    {}

    /**
     * Classes implementing the StorageFactory interface must have a null
     * constructor.  This method is called when the database is booted up to
     * initialize the class. It should perform all actions necessary to start the
     * basic storage, such as creating a temporary file directory.
     *
     * The init method will be called once, before any other method is called, and will not
     * be called again.
     *
     * @param home The name of the directory containing the database. It comes from the system.home system property.
     *             It may be null. A storage factory may decide to ignore this parameter. (For instance the classpath
     *             storage factory ignores it.
     * @param databaseName The name of the database (directory). All relative pathnames are relative to this directory.
     *                     If null then the storage factory will only be used to deal with the directory containing
     *                     the databases.
     * @param create If true then the database is being created.
     * @param tempDirName The name of the temporary file directory set in properties. If null then a default
     *                    directory should be used. Each database should get a separate temporary file
     *                    directory within this one to avoid collisions.
     * @param uniqueName A unique name that can be used to create the temporary file directory for this database.
     *
     * @exception IOException on an error (unexpected).
     */
    public void init( String home, String databaseName, String tempDirName, String uniqueName)
        throws IOException
    {
        if( databaseName != null)
        {
            dataDirectory = databaseName;
            separatedDataDirectory = databaseName + getSeparator();
        }
        this.home = home;
        this.uniqueName = uniqueName;
        tempDirPath = tempDirName;
        doInit();
    } // end of init

    abstract void doInit() throws IOException;
    
    public void shutdown()
    {
    }
    

    /**
     * Get the canonical name of the database. This is a name that uniquely identifies it. It is system dependent.
     *
     * The normal, disk based implementation uses method java.io.File.getCanonicalPath on the directory holding the
     * database to construct the canonical name.
     *
     * @return the canonical name
     *
     * @exception IOException if an IO error occurred during the construction of the name.
     */
    public String getCanonicalName() throws IOException
    {
        return canonicalName;
    }
    
    /**
     * Construct a StorageFile from a path name.
     *
     * @param path The path name of the file
     *
     * @return A corresponding StorageFile object
     */
    public StorageFile newStorageFile( String path)
    {
        if( path != null && tempDirPath != null && path.startsWith( tempDirPath))
            return new DirFile( path);
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
    public StorageFile newStorageFile( String directoryName, String fileName)
    {
        if( directoryName == null)
            return newStorageFile( fileName);
        else if( tempDirPath != null && directoryName.startsWith( tempDirPath))
            return new DirFile(directoryName, fileName);
        else
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
    public StorageFile newStorageFile( StorageFile directoryName, String fileName)
    {
        if( directoryName == null)
            return newStorageFile( fileName);
        if( fileName == null)
            return directoryName;
        else if (tempDirPath != null && directoryName.getPath().startsWith(tempDirPath))
            return new DirFile( (DirFile) directoryName, fileName);
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
    abstract StorageFile newPersistentFile( String path);

    /**
     * Construct a persistent StorageFile from a directory and path name.
     *
     * @param directory The path name of the directory. Guaranteed not to be in the temporary file directory.
     *                  Guaranteed not to be null
     * @param fileName The name of the file within the directory. Guaranteed not to be null.
     *
     * @return A corresponding StorageFile object
     */
    abstract StorageFile newPersistentFile( String directoryName, String fileName);

    /**
     * Construct a persistent StorageFile from a directory and path name.
     *
     * @param directory The path name of the directory. Guaranteed not to be to be null. Guaranteed to be
     *                  created by a call to one of the newPersistentFile methods.
     * @param fileName The name of the file within the directory. Guaranteed not to be null.
     *
     * @return A corresponding StorageFile object
     */
    abstract StorageFile newPersistentFile( StorageFile directoryName, String fileName);
    
    /**
     * Get the pathname separator character used by the StorageFile implementation.
     *
     * @return the pathname separator character. (Normally '/' or '\').
     */
    public char getSeparator()
    {
        // Temp files are always java.io.File's and use its separator.
        return File.separatorChar;
    }

    /**
     * Get the abstract name of the directory that holds temporary files.
     *
     * @return a directory name
     */
    public StorageFile getTempDir()
    {
        return tempDir;
    }

    /**
     * This method is used to determine whether the storage is fast (RAM based) or slow (disk based).
     * It may be used by the database engine to determine the default size of the page cache.
     *
     * @return <b>true</b> if the storage is fast, <b>false</b> if it is slow.
     */
    public boolean isFast()
    {
        return false;
    }

    public boolean isReadOnlyDatabase()
    {
        return true;
    }

    /**
     * Determine whether the storage supports random access. If random access is not supported then
     * it will only be accessed using InputStreams and OutputStreams (if the database is writable).
     *
     * @return <b>true</b> if the storage supports random access, <b>false</b> if it is writable.
     */
    public boolean supportsRandomAccess()
    {
        return false;
    }

    void createTempDir() throws java.io.IOException
    {
        if( uniqueName == null)
            return;

        if( tempDirPath != null)
            tempDir = new DirFile( tempDirPath, TEMP_DIR_PREFIX.concat(uniqueName));
        else if( isReadOnlyDatabase())
            tempDir = new DirFile( readOnlyTempRoot(), TEMP_DIR_PREFIX.concat(uniqueName));
        else
            tempDir = new DirFile( canonicalName, DataFactory.TEMP_SEGMENT_NAME);
            
        // blow away any temporary directory
        tempDir.deleteAll();

        tempDir.mkdirs();
        tempDirPath = tempDir.getPath();
    } // end of createTempDir

	private String readOnlyTempRoot() throws java.io.IOException
    {
		// return the system temp dir by creating a temp file
		// and finding its parent.
		File temp = File.createTempFile("derby", "tmp");
		String parent = temp.getParent();
		temp.delete();

		return parent;
	}

    public int getStorageFactoryVersion()
    {
        return StorageFactory.VERSION_NUMBER;
    }
}
