/*

   Derby - Class org.apache.derby.impl.io.VFMemoryStorageFactory

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

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

import org.apache.derby.impl.io.vfmem.PathUtil;
import org.apache.derby.impl.io.vfmem.DataStore;
import org.apache.derby.impl.io.vfmem.VirtualFile;

import org.apache.derby.io.StorageFactory;
import org.apache.derby.io.StorageFile;
import org.apache.derby.io.WritableStorageFactory;

/**
 * A storage factory for virtual files, where the contents of the files are
 * stored in main memory.
 */
public class VFMemoryStorageFactory
        implements StorageFactory, WritableStorageFactory {

    /** References to the databases created / existing. */
    //@GuardedBy("DATABASES")
    private static final Map DATABASES = new HashMap();

    /**
     * Deletes the database if it exists.
     *
     * @param dbName the database name
     * @return {@code true} if the database was deleted, {@code false} otherwise
     */
    public static boolean purgeDatabase(final String dbName) {
        // TODO: Should we check if the database is booted / active?
        synchronized (DATABASES) {
            DataStore store = (DataStore)DATABASES.remove(dbName);
            if (store != null) {
                // Delete everything.
                store.purge();
                return true;
            }
            return false;
        }
    }

    /** The canonical (unique) name of the database (absolute path). */
    private String canonicalName;
    /** The data directory of the database. */
    private StorageFile dataDirectory;
    /** The temporary directory for the database (absolute path). */
    private StorageFile tempDir;
    /** The data store used for the database. */
    private DataStore dbData;

    /**
     * Creates a new, uninitialized instance of the storage factory.
     * <p>
     * To initialize the instance, {@code init} must be called.
     *
     * @see #init
     */
    public VFMemoryStorageFactory() {
        // Do nothing here, see the init-method.
    }

    /**
     * Initializes the storage factory instance by setting up a temporary
     * directory, the database directory and checking if the database being
     * named already exists.
     *
     * @param home the value of {@code system.home} for this storage factory
     * @param databaseName the name of the database, all relative pathnames are
     *      relative to this name
     * @param tempDirNameIgnored ignored
     * @param uniqueName used to determine when the temporary directory can be
     *      created, but not to name the temporary directory itself
     *
     * @exception IOException on an error (unexpected).
     */
    public void init(String home, String databaseName,
                     String tempDirNameIgnored, String uniqueName)
            throws IOException {
        // Handle cases where a database name is specified.
        if (databaseName != null) {
            if (home != null &&
                    !new File(databaseName).isAbsolute()) {
                canonicalName = new File(home, databaseName).getCanonicalPath();
            } else {
                canonicalName = new File(databaseName).getCanonicalPath();
            }
            synchronized (DATABASES) {
                if (DATABASES.containsKey(canonicalName)) {
                    // Fetch the existing data store.
                    this.dbData = (DataStore)DATABASES.get(canonicalName);
                } else {
                    // Create a new data store.
                    this.dbData = new DataStore(canonicalName);
                    DATABASES.put(canonicalName, dbData);
                }
            }
            // Specify the data directory and the temp directory.
            dataDirectory = new VirtualFile(canonicalName, dbData);
            tempDir = new VirtualFile(normalizePath(canonicalName, "tmp"),
                                      dbData);

        // Handle cases where the database name is null, but a system home
        // directory has been specified.
        } else if (home != null) {
            // Return the "system home directory" and create a temporary
            // directory for it.
            final String absHome = new File(home).getCanonicalPath();
            synchronized (DATABASES) {
                dbData = (DataStore)DATABASES.get(absHome);
                if (dbData == null) {
                    // Create a new data store for the specified home.
                    dbData = new DataStore(absHome);
                    DATABASES.put(absHome, dbData);
                }
            }
            dataDirectory = new VirtualFile(absHome, dbData);
            tempDir = new VirtualFile(getSeparator() + "tmp", dbData);
        }

        // Create the temporary directory, if one has been specified.
        // Creating the temporary directory too early casues the
        // BaseDataFileFactory to fail, hence the check for uniqueName.
        // This check is also used by BaseStorageFactory.
        if (uniqueName != null && tempDir != null && !tempDir.exists()) {
            tempDir.mkdirs();
        }
    }

    public void shutdown() {
        // For now, do nothing.
        // TODO: Deleting stuff doesn't seem to play nice when running the
        // regression tests, as CleanDatabaseTestSetup fails then. The cause
        // is unknown and should be investigated.
    }

    public String getCanonicalName() {
        return canonicalName;
    }

    /**
     * Returns a handle to the specific storage file.
     *
     * @param path the path of the file or directory
     * @return A path handle.
     */
    public StorageFile newStorageFile(String path) {
        // No need to separate between temporary and non-temporary files, since
        // all files are non-persistant and the path will determine where the
        // files are stored.
        if (path == null) {
            // Return the database directory as described by StorageFactory.
            return dataDirectory;
        }
        return new VirtualFile(normalizePath(path), dbData);
    }

    /**
     * Returns a handle to the specified storage file.
     *
     * @param directoryName the name of the parent directory
     * @param fileName the name of the file
     * @return A path handle.
     */
    public StorageFile newStorageFile(String directoryName, String fileName) {
            return new VirtualFile(
                                normalizePath(directoryName, fileName), dbData);
    }

    /**
     * Returns a handle to the specified storage file.
     *
     * @param directoryName the name of the parent directory
     * @param fileName the name of the file
     * @return A path handle.
     */
    public StorageFile newStorageFile(StorageFile directoryName,
                                      String fileName) {
        return newStorageFile(directoryName == null ? null
                                                    : directoryName.getPath(),
                              fileName);
    }

    /**
     * Returns the temporary directory for this storage factory instance.
     *
     * @return A {@code StorageFile}-object representing the temp directory.
     */
    public StorageFile getTempDir() {
        return tempDir;
    }

    /**
     * The service is fast and supports random access.
     *
     * @return {@code true}
     */
    public boolean isFast() {
        return true;
    }

    /**
     * The service supports writes.
     *
     * @return {@code false}
     */
    public boolean isReadOnlyDatabase() {
        return false;
    }

    /**
     * The service supports random access.
     *
     * @return {@code true}
     */
    public boolean supportsRandomAccess() {
        return true;
    }

    public int getStorageFactoryVersion() {
        return StorageFactory.VERSION_NUMBER;
    }

    /**
     * Creates a handle to a temporary file.
     *
     * @param prefix requested prefix for the file name
     * @param suffix requested suffix for the file name, if {@code null} then
     *      {@code .tmp} will be used
     * @return A handle to the temporary file.
     */
    public StorageFile createTemporaryFile(String prefix, String suffix) {
        String name;
        if (suffix == null) {
            suffix = ".tmp";
        }
        if (prefix == null) {
            name = dbData.getTempFileCounter() + suffix;
        } else {
            name = prefix + dbData.getTempFileCounter() + suffix;
        }
        return newStorageFile(tempDir, name);
    }

    /**
     * Returns the path separator used by this storage factory.
     *
     * @return {@code PathUtil.SEP}
     */
    public char getSeparator() {
        return PathUtil.SEP;
    }

    /**
     * The sync method is a no-op for this storage factory.
     *
     * @param stream ignored
     * @param metaData ignored
     */
    public void sync(OutputStream stream, boolean metaData) {
        // Does nothing, data is stored only in memory.
        // TODO: Are there any streams that needs to be flushed?
    }

    public boolean supportsWriteSync() {
        // TODO: What will give us the best performance here?
        return true;
    }

    /**
     * Returns a normalized absolute path.
     *
     * @param dir parent directory, if {@code null} the {@code dataDirectory}
     *      will be used
     * @param file the file name ({@code null} not allowed)
     * @return A path.
     * @throws NullPointerException if {@code file} is {@code null}
     */
    private String normalizePath(String dir, String file) {
        if (dir == null || dir.equals("")) {
            dir = dataDirectory.getPath();
        } else if (!new File(dir).isAbsolute()) {
            dir = new File(dataDirectory.getPath(), dir).getPath();
        }
        // We now have an absolute path for the directory.
        // Use java.io.File to get consistent behavior.
        return (new File(dir, file).getPath());
    }

    /**
     * Returns a normalized absolute path.
     *
     * @param path path, if {@code null} the {@code dataDirectory} will be used
     * @return A path.
     */
    private String normalizePath(String path) {
        if (path == null || path.equals("")) {
            return dataDirectory.getPath();
        } else if (new File(path).isAbsolute()) {
            return path;
        } else {
            return new File(dataDirectory.getPath(), path).getPath();
        }
    }
}
