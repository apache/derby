/*

   Derby - Class org.apache.derby.impl.io.BaseStorageFactory

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
package org.apache.derbyTesting.functionTests.util.corruptio;
import org.apache.derby.io.WritableStorageFactory;
import org.apache.derby.io.StorageFile;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.IOException;
import java.io.SyncFailedException;

/**
 * This class provides a proxy base implementation of the 
 * WritableStorageFactory interface to instrument I/O operations for testing 
 * purposes. 
 * Some methods in this class adds support for corrupting the I/O operation 
 * sent by the engine before invoking the real storage factory underneath. 
 * By deault all the calls will go to the real storage factory defined by the 
 * concrete class, unless corruption is enabled through CorruptibleIo instance. 
 * 
 * @see CorruptibleIo
 * @see WritableStorageFactory
 * 
 */

abstract class CorruptBaseStorageFactory implements WritableStorageFactory
{

	protected WritableStorageFactory realStorageFactory;

    /**
     * Most of the initialization is done in the init method.
     */
    CorruptBaseStorageFactory()
    {}

    /**
     * Classes implementing the StorageFactory interface must have a null
     * constructor.  This method is called when the database is booted up to
     * initialize the class. It should perform all actions necessary to start 
     * the basic storage, such as creating a temporary file directory.
     *
     * The init method will be called once, before any other method is called, 
     * and will not be called again.
     *
     * @param home          The name of the directory containing the database. 
     *                      It comes from the system.home system property.
     *                      It may be null. A storage factory may decide to 
     *                      ignore this parameter. (For instance the classpath
     *                      storage factory ignores it.)
     *
     * @param databaseName  The name of the database (directory). 
     *                      All relative pathnames are relative to this 
     *                      directory.
     *                      If null then the storage factory will only be used 
     *                      to deal with the directory containing the databases.
     * @param tempDirName   The name of the temporary file directory set in 
     *                      properties. If null then a default directory should
     *                      be used. Each database should get a separate 
     *                      temporary file directory within this one to avoid 
     *                      collisions.
     *
     * @param uniqueName    A unique name that can be used to create the 
     *                      temporary file directory for this database.
     *
     * @exception IOException on an error (unexpected).
     */
    public void init( String home, String databaseName, String tempDirName, String uniqueName)
        throws IOException
    {
		realStorageFactory = getRealStorageFactory();
		realStorageFactory.init(home, databaseName, tempDirName,  uniqueName);
    } // end of init

    
    public void shutdown()
    {
		realStorageFactory.shutdown();
    }
    

    /**
     * Get the canonical name of the database. 
     *
     * This is a name that uniquely identifies it. It is system dependent.
     *
     * The normal, disk based implementation uses method 
     * java.io.File.getCanonicalPath on the directory holding the
     * database to construct the canonical name.
     *
     * @return the canonical name
     *
     * @exception IOException if an IO error occurred during the construction 
     *                        of the name.
     */
    public String getCanonicalName() throws IOException
    {
		return realStorageFactory.getCanonicalName();
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
        return new CorruptFile(realStorageFactory.newStorageFile(path));
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
		return new CorruptFile(realStorageFactory.newStorageFile(directoryName, fileName));
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
		StorageFile realDirFile = ((CorruptFile) directoryName).getRealFileInstance();
		return new CorruptFile(realStorageFactory.newStorageFile(realDirFile, fileName));
    }
    
    /**
     * Get the pathname separator character used by the StorageFile 
     * implementation.
     *
     * @return the pathname separator character. (Normally '/' or '\').
     */
    public char getSeparator()
    {
		return realStorageFactory.getSeparator();
    }

    /**
     * Get the abstract name of the directory that holds temporary files.
     *
     * @return a directory name
     */
    public StorageFile getTempDir()
    {
		return new CorruptFile(realStorageFactory.getTempDir());
    }

    /**
     * This method is used to determine whether the storage is fast 
     * (RAM based) or slow (disk based).
     *
     * It may be used by the database engine to determine the default size of 
     * the page cache.
     *
     * @return <b>true</b> if the storage is fast, <b>false</b> if it is slow.
     */
    public boolean isFast()
    {
		return realStorageFactory.isFast();
    }

    public boolean isReadOnlyDatabase()
    {
		return realStorageFactory.isReadOnlyDatabase();
    }

    /**
     * Determine whether the storage supports random access. 
     * If random access is not supported then it will only be accessed using 
     * InputStreams and OutputStreams (if the database is writable).
     *
     * @return <b>true</b> if the storage supports random access, <b>false</b> if it is writable.
     */
    public boolean supportsRandomAccess()
    {
		return realStorageFactory.supportsRandomAccess();
    }

    public int getStorageFactoryVersion()
    {
		return realStorageFactory.getStorageFactoryVersion();
    }


	
    /**
     * Force the data of an output stream out to the underlying storage. 
     *
     * That is, ensure that it has been made persistent. If the database is to 
     * be transient, that is, if the database does not survive a restart, then 
     * the sync method implementation need not do anything.
     *
     * @param stream    The stream to be synchronized.
     * @param metaData  If true then this method must force both changes to the
     *                  file's contents and metadata to be written to storage; 
     *                  if false, it need only force file content changes to be
     *                  written. The implementation is allowed to ignore this 
     *                  parameter and always force out metadata changes.
     *
     * @exception IOException if an I/O error occurs.
     * @exception SyncFailedException Thrown when the buffers cannot be flushed,
     *            or because the system cannot guarantee that all the buffers 
     *            have been synchronized with physical media.
     */
    public void sync( OutputStream stream, boolean metaData) throws IOException, SyncFailedException
    {
		realStorageFactory.sync(stream, metaData);
    }

    /**
     * This method tests whether the "rws" and "rwd" modes are implemented. 
     *
     * If the "rws" and "rwd" modes are supported then the database engine will
     * conclude that the write methods of "rws" mode StorageRandomAccessFiles
     * are slow but the sync method is fast and optimize accordingly.
     *
     * @return <b>true</b> if an StIRandomAccess file opened with "rws" or "rwd" modes immediately writes data to the
     *         underlying storage, <b>false</b> if not.
     */
    public boolean supportsWriteSync()
    {
		return realStorageFactory.supportsWriteSync();
    }

	
	/**
     * get the  real storage factory
     *
     */
	abstract WritableStorageFactory getRealStorageFactory();

    /**
     * Create and returns a temporary file in temporary file system of database.
     *
     * @param prefix String to prefix the random name generator. It can be null
     * @param suffix String to suffix the random name generator. ".tmp" will be
     *               used if null.
     * @return StorageFile
     */
    public StorageFile createTemporaryFile(String prefix, String suffix)
                                                            throws IOException {
        return new CorruptFile(realStorageFactory.createTemporaryFile(
                prefix, suffix));
    }

}
