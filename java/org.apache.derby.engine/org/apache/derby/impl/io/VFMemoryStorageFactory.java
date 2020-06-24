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
 * <p>
 * Note that data store deletion may happen inside one of two different methods;
 * either in {@code shutdown} or in {@code init}. This is due to the current
 * implementation and the fact that dropping a database is done through the
 * file IO interface by deleting the service root. As the deletion then becomes
 * a two step process, someone else may boot the database again before the
 * reference to the store has been removed. To avoid this, the
 * {@code init}-method will never initialize with a store scheduled for
 * deletion. I have only seen this issue in heavily loaded multithreaded
 * environments (2 CPUs/cores should be enough to reproduce).
 */
public class VFMemoryStorageFactory
        implements StorageFactory, WritableStorageFactory {

    /** References to the databases created / existing. */
    //@GuardedBy("DATABASES")
    private static final Map<String,DataStore> DATABASES = new HashMap<String,DataStore>();

    /**
     * Dummy store used to carry out frequent operations that don't
     * require a "proper store", for instance getting the canonical name
     * of the data store.
     */
    private static final DataStore DUMMY_STORE = new DataStore("::DUMMY::");

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
//IC see: https://issues.apache.org/jira/browse/DERBY-4093
                     String tempDirNameIgnored, String uniqueName)
            throws IOException {
        // Handle cases where a database name is specified.
        if (databaseName != null) {
            if (home != null &&
//IC see: https://issues.apache.org/jira/browse/DERBY-4125
                    !new File(databaseName).isAbsolute()) {
                canonicalName = new File(home, databaseName).getCanonicalPath();
            } else {
                canonicalName = new File(databaseName).getCanonicalPath();
            }
            synchronized (DATABASES) {
//IC see: https://issues.apache.org/jira/browse/DERBY-4428
                this.dbData = (DataStore)DATABASES.get(canonicalName);
                // If the store has been scheduled for deletion, purge it.
                if (dbData != null && dbData.scheduledForDeletion()) {
                    DATABASES.remove(canonicalName);
                    dbData.purge();
                    dbDropCleanupInDummy(canonicalName);
                    dbData = null;
                }
                if (dbData == null) {
                    if (uniqueName != null) {
                        // Create a new data store.
                        this.dbData = new DataStore(canonicalName);
                        DATABASES.put(canonicalName, dbData);
//IC see: https://issues.apache.org/jira/browse/DERBY-4432
                    } else {
                        // We have a database name, but no unique name.
                        // Assume that the client only wants to do some
                        // "book-keeping" operations, like getting the
                        // canonical name.
                        this.dbData = DUMMY_STORE;
                    }
                }
            }
            // Specify the data directory and the temp directory.
            dataDirectory = new VirtualFile(canonicalName, dbData);
//IC see: https://issues.apache.org/jira/browse/DERBY-4094
            tempDir = new VirtualFile(normalizePath(canonicalName, "tmp"),
                                      dbData);

        // Handle cases where the database name is null, but a system home
        // directory has been specified.
        } else if (home != null) {
            // Return the "system home directory" and specify a temporary
            // directory for it (may never by used).
            // As databases are created, the dummy will contain the
            // directory names of the database locations, but the
            // databases themselves will be stored in separate stores.
            final String absHome = new File(home).getCanonicalPath();
//IC see: https://issues.apache.org/jira/browse/DERBY-4432
            dbData = DUMMY_STORE;
            dataDirectory = new VirtualFile(absHome, dbData);
            tempDir = new VirtualFile(getSeparator() + "tmp", dbData);
        }

        // Create the temporary directory, if one has been specified.
        // Creating the temporary directory too early casues the
        // BaseDataFileFactory to fail, hence the check for uniqueName.
        // This check is also used by BaseStorageFactory.
//IC see: https://issues.apache.org/jira/browse/DERBY-4093
        if (uniqueName != null && tempDir != null && !tempDir.exists()) {
            tempDir.mkdirs();
//IC see: https://issues.apache.org/jira/browse/DERBY-5363
            tempDir.limitAccessToOwner(); // nop, but follow pattern
        }
    }

    /**
     * Normally does nothing, but if the database is in a state such that it
     * should be deleted this will happen here.
     */
    public void shutdown() {
        // If the data store has been scheduled for deletion, which happens
        // when the store detects that the service root has been deleted, then
        // delete the whole store to release the memory.
//IC see: https://issues.apache.org/jira/browse/DERBY-4428
        if (dbData.scheduledForDeletion()) {
            DataStore store;
            synchronized (DATABASES) {
                store = (DataStore)DATABASES.remove(canonicalName);
                // Must clean up the dummy while holding monitor.
                if (store != null && store == dbData) {
                    dbDropCleanupInDummy(canonicalName);
                }
            }
            // If this is the correct store, purge it now.
            if (store != null && store == dbData) {
                dbData.purge(); // Idempotent.
                dbData = null;
            }
        }
    }

    public String getCanonicalName() {
        return canonicalName;
    }

    /**
     * Set the canonicalName. May need adjustment due to DERBY-5096
     * 
     * @param name uniquely identifiable name for this database
     */
    public void setCanonicalName(String name) {
       canonicalName = name;
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
//IC see: https://issues.apache.org/jira/browse/DERBY-4094
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
//IC see: https://issues.apache.org/jira/browse/DERBY-4094
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
//IC see: https://issues.apache.org/jira/browse/DERBY-4094
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
//IC see: https://issues.apache.org/jira/browse/DERBY-5393
        if (dir == null || dir.length() == 0) {
//IC see: https://issues.apache.org/jira/browse/DERBY-4094
            dir = dataDirectory.getPath();
//IC see: https://issues.apache.org/jira/browse/DERBY-4125
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
//IC see: https://issues.apache.org/jira/browse/DERBY-5393
        if (path == null || path.length() == 0) {
            return dataDirectory.getPath();
//IC see: https://issues.apache.org/jira/browse/DERBY-4125
        } else if (new File(path).isAbsolute()) {
            return path;
        } else {
            return new File(dataDirectory.getPath(), path).getPath();
        }
    }

    /**
     * Cleans up the internal dummy data store after a database has been
     * dropped.
     *
     * @param dbPath absolute path of the dropped database
     */
    private void dbDropCleanupInDummy(String dbPath) {
//IC see: https://issues.apache.org/jira/browse/DERBY-4428
        while (dbPath != null && DUMMY_STORE.deleteEntry(dbPath)) {
            dbPath = new File(dbPath).getParent();
        }
    }
}
