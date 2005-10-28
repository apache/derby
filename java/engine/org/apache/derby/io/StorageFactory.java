/*

   Derby - Class org.apache.derby.io.StorageFactory

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

import java.io.IOException;

/**
 * This interface provides basic storage functions needed for read only databases. Most storage
 * implementations will be read-write and implement the WritableStorageFactory extension of this
 * interface.
 *
 *<p>
 * The database engine uses this interface to access storage. The normal database engine
 * implements this interface using disk files and the standard java.io classes.
 *
 *<p>
 * The storage factory must implement writable temporary files, even if the database is read-only or
 * if the storage factory is read-only (i.e. it does not implement the WritableStorageFactory extension of this
 * interface). Temporary files are those created under the temporary file directory. See
 * {@link #getTempDir method getTempDir()}.
 *
 *<p>The database engine can be turned into a RAM based engine by providing a RAM based implementation of this interface.
 *
 *<p>There is one instance of the StorageFactory per database if the log files are kept in the database directory.
 * If the log files are kept on a separate device then a second StorageFactory is instantiated to hold the log files.
 * The database or log device name is set when the init method is called.
 * The init method is called once per instance, before any other StorageFactory method.
 *
 *<p>The class implementing this interface must have a public niladic constructor. The init method will be called
 * before any other method to set the database directory name, to tell the factory to create the database
 * directory if necessary, and to allow the implementation to perform any initializations it requires. The
 * database name set in the init method forms a separate name space. Different StorageFactory instances, with
 * different database directory names, must ensure that their files do not clash. So, for instance,
 * storageFactory1.newStorageFile( "x") must be a separate file from storageFactory2.newStorageFile( "x").
 *
 *<p>The database engine will call this interface's methods from its own privilege blocks. This does not give
 * a StorageFactory implementation carte blanche: a security manager can still forbid the implemeting class from
 * executing a privileged action. However, the security manager will not look in the calling stack beyond the
 * database engine.
 *
 *<p>Each StorageFactory instance may be concurrently used by multiple threads. Each StorageFactory implementation
 * must be thread safe.
 *
 *<p>A StorageFactory implementation is plugged into the database engine via a sub-protocol. Sub-protocol <i>xxx</i> is
 * tied to a StorageFactory implementation class via the derby.subSubProtocol.<i>xxx</i> system property. So,
 * to use StorageFactory implementation class MyStorageFactory with database myDB you would set the system
 * property "derby.subSubProtocol.mysf=MyStorageFactory" and use the URL "jdbc:derby:mysf:myDB" to
 * connect to the database.
 *
 * @see WritableStorageFactory
 * @see StorageFile
 * @see StorageRandomAccessFile
 * @see <a href="http://java.sun.com/j2se/1.4.2/docs/api/java/io/File.html">java.io.File</a>
 * @see <a href="http://java.sun.com/j2se/1.4.2/docs/api/java/io/RandomAccessFile.html">java.io.RandomAccessFile</a>
 * @see <a href="http://java.sun.com/j2se/1.4.2/docs/api/java/io/InputStream.html">java.io.InputStream</a>
 * @see <a href="http://java.sun.com/j2se/1.4.2/docs/api/java/io/OutputStream.html">java.io.OutputStream</a>
 */
public interface StorageFactory
{

    /**
     * Classes implementing the StorageFactory interface must have a null
     * constructor.  The init method is called when the database is booted up to
     * initialize the class. It should perform all actions necessary to start the
     * basic storage, such as creating a temporary file directory.
     *
     * This method should not create the database directory.
     *<p>
     * The init method will be called once, before any other method is called, and will not
     * be called again.
     *
     * @param home The name of the directory containing the database. It comes from the system.home system property.
     *             It may be null. A storage factory may decide to ignore this parameter. (For instance the classpath
     *             storage factory ignores it).
     * @param databaseName The name of the database (directory). The name does not include the subsubprotocol.
     *                     If null then the storage factory will only be used to deal with the directory containing
     *                     the databases.
     * @param tempDirName The name of the temporary file directory set in properties. If null then a default
     *                    directory should be used. Each database should get a separate temporary file
     *                    directory within this one to avoid collisions.
     * @param uniqueName A unique name that can be used to create the temporary file directory for this database.
     *                   If null then temporary files will not be created in this StorageFactory instance, and the
     *                   temporary file directory should not be created.
     *
     * @exception IOException
     */
    public void init( String home, String databaseName, String tempDirName, String uniqueName)
        throws IOException;

    /**
     * The shutdown method is called during the normal shutdown of the database. However, the database
     * engine cannot guarantee that shutdown will be called. If the JVM terminates abnormally then it will
     * not be called.
     */
    public void shutdown();

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
    public String getCanonicalName() throws IOException;
    
    /**
     * Construct a StorageFile from a path name.
     *
     * @param path The path name of the file. If null then return the database directory.
     *             If this parameter denotes the temp directory or a directory under the temp
     *             directory then the resulting StorageFile denotes a temporary file. Otherwise
     *             the path must be relative to the database and the resulting StorageFile denotes a
     *             regular database file (non-temporary).
     *
     * @return A corresponding StorageFile object
     */
    public StorageFile newStorageFile( String path);

    /**
     * Construct a non-temporary StorageFile from a directory and file name.
     *
     * @param directoryName The directory part of the path name. If this parameter denotes the
     *                      temp directory or a directory under the temp directory then the resulting
     *                      StorageFile denotes a temporary file. Otherwise the directory name must be
     *                      relative to the database and the resulting StorageFile denotes a
     *                      regular database file (non-temporary).
     * @param fileName The name of the file within the directory.
     *
     * @return A corresponding StorageFile object
     */
    public StorageFile newStorageFile( String directoryName, String fileName);

    /**
     * Construct a StorageFile from a directory and file name. The StorageFile may denote a temporary file
     * or a non-temporary database file, depending upon the directoryName parameter.
     *
     * @param directoryName The directory part of the path name. If this parameter denotes the
     *                      temp directory or a directory under the temp directory then the resulting
     *                      StorageFile denotes a temporary file. Otherwise the resulting StorageFile denotes a
     *                      regular database file (non-temporary).
     * @param fileName The name of the file within the directory.
     *
     * @return A corresponding StorageFile object
     */
    public StorageFile newStorageFile( StorageFile directoryName, String fileName);

    /**
     * Get the pathname separator character used by the StorageFile implementation. This is the
     * separator that must be used in directory and file name strings.
     *
     * @return the pathname separator character. (Normally '/' or '\').
     */
    public char getSeparator();

    /**
     * Get the abstract name of the directory that holds temporary files.
     *<p>
     * The StorageFactory implementation
     * is not required to make temporary files persistent. That is, files created in the temp directory are
     * not required to survive a shutdown of the database engine.
     *<p>
     * However, files created in the temp directory must be writable, <b>even if the database is
     * otherwise read-only</b>.
     *
     * @return a directory name
     */
    public StorageFile getTempDir();

    /**
     * This method is used to determine whether the storage is fast (RAM based) or slow (disk based).
     * It may be used by the database engine to determine the default size of the page cache.
     *
     * @return <b>true</b> if the storage is fast, <b>false</b> if it is slow.
     */
    public boolean isFast();

    /**
     * Determine whether the database is read only. The database engine supports read-only databases, even
     * in file systems that are writable.
     *
     * @return <b>true</b> if the storage is read only, <b>false</b> if it is writable.
     */
    public boolean isReadOnlyDatabase();

    /**
     * Determine whether the storage supports random access. If random access is not supported then
     * it will only be accessed using InputStreams and OutputStreams (if the database is writable).
     *
     * @return <b>true</b> if the storage supports random access, <b>false</b> if it is writable.
     */
    public boolean supportsRandomAccess();

    /**
     * The version number of this version of the StorageFactory interface and its subsidiary interfaces.
     */
    int VERSION_NUMBER = 1;

    /**
     * @return the StorageFactory version supported by this implementation
     */
    public int getStorageFactoryVersion();
}
