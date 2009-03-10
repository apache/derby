/*

   Derby - Class org.apache.derby.impl.io.vfmem.DataStore

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

package org.apache.derby.impl.io.vfmem;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Iterator;

import org.apache.derby.io.StorageFile;

/**
 * A virtual data store, keeping track of all the virtual files existing and
 * offering a set of high-level operations on virtual files.
 */
public final class DataStore {

    /** The path separator used. */
    private static final char SEP = PathUtil.SEP;

    /** Constant for the empty String array. */
    private static final String[] EMPTY_STR_ARR = new String[0];

    /** Lock object for the file map. */
    private final Object LOCK = new Object();
    /** Lock object for the temporary file counter. */
    private final Object TMP_COUNTER_LOCK = new Object();
    /**
     * The files exsiting in the store.
     * <p>
     * The initial size is set to the number of initial files of a Derby
     * database, pluss a few more.
     */
    private final Map files = new HashMap(80);

    /** The name of the database this store serves. */
    private final String databaseName;
    /** Counter used to generate unique temporary file names. */
    private long tmpFileCounter = 0;

    /**
     * Creates a new data store.
     *
     * @param databaseName the name of the assoicated database
     */
    public DataStore(String databaseName) {
        this.databaseName = databaseName;
        // Create the absolute root.
        createEntry(String.valueOf(SEP), true);

    }

    /**
     * Returns the database name.
     *
     * @return The database name.
     */
    public String getDatabaseName() {
        return this.databaseName;
    }

    /**
     * Creates a new entry in the data store.
     * <p>
     * This method returns {@code null} if the path already exists, if one of
     * the parent directories doesn't exist, or if one of the parents is a
     * file instead of a directory.
     *
     * @param iPath the path of the entry
     * @param isDir tells if the new entry shall be directory or a file
     * @return A {@code DataStoreEntry}-instance if the entry was successfully
     *      created, {@code null} otherwise
     */
    public DataStoreEntry createEntry(String iPath, boolean isDir) {
        synchronized (LOCK) {
            if (files.containsKey(iPath)) {
                return null;
            }
            // Make sure the the parent directories exists.
            String parent = PathUtil.getParent(iPath);
            while (parent != null) {
                DataStoreEntry entry = (DataStoreEntry)files.get(parent);
                if (entry == null) {
                    return null;
                } else if (!entry.isDirectory()) {
                    return null;
                }
                parent = PathUtil.getParent(parent);
            }
            DataStoreEntry newEntry = new DataStoreEntry(iPath, isDir);
            files.put(iPath, newEntry);
            return newEntry;
        }
    }

    /**
     * Creates all the parents of the specified path.
     *
     * @return {@code true} if all parents either already existed as directories
     *      or were created, {@code false} otherwise
     */
    public boolean createAllParents(String path) {
        if (path.charAt(path.length() -1) == SEP) {
            path = path.substring(0, path.length() -1);
        }
        // If there is no path separator, only one entry will be created.
        if (path.indexOf(SEP) == -1) {
            return true;
        }
        synchronized (LOCK) {
            int index = path.indexOf(SEP, 1); // The root always exists

            while (index > 0) {
                String subPath = path.substring(0, index);
                DataStoreEntry entry = (DataStoreEntry)files.get(subPath);
                if (entry == null) {
                    createEntry(subPath, true);
                } else if (!entry.isDirectory()) {
                    return false;
                }
                index = path.indexOf(SEP, index +1);
            }
        }
        return true;
    }

    /**
     * Deletes the specified entry.
     * <p>
     * If the specified entry is a directory, it is only deleted if it is
     * empty. Read-only entries are deleted.
     *
     * @param iPath path of the entry to delete
     * @return {@code true} if the entry was deleted, {@code false} otherwise.
     */
    public boolean deleteEntry(String iPath) {
        DataStoreEntry entry;
        synchronized (LOCK) {
            entry = (DataStoreEntry)files.remove(iPath);
            if (entry != null) {
                if (entry.isDirectory()) {
                    String[] children = listChildren(iPath);
                    if (children == null || children.length == 0){
                        entry.release();
                        // Re-add the entry.
                        files.put(iPath, entry);
                        return false;
                    }
                } else {
                    entry.release();
                }
            }
        }
        return (entry != null);
    }

    /**
     * Returns the entry with the specified path.
     *
     * @param iPath path of the entry to fetch
     * @return {@code null} if the entry doesn't exist, the
     *      {@code DataStoreEntry}-object otherwise.
     */
    public DataStoreEntry getEntry(String iPath) {
        synchronized (LOCK) {
            return (DataStoreEntry)files.get(iPath);
        }
    }

    /**
     * Deletes the specified entry and all its children.
     *
     * @param iPath the root entry
     * @return {@code true} if the entry and all its children were deleted,
     *      {@code false} if the root doesn't exist.
     */
    public boolean deleteAll(String iPath) {
        synchronized (LOCK) {
            DataStoreEntry entry = (DataStoreEntry)files.remove(iPath);
            if (entry == null) {
                // Delete root doesn't exist.
                return false;
            } else if (entry.isDirectory()) {
                // Delete root is a directory.
                return _deleteAll(iPath);
            } else {
                // Delete root is a file.
                entry.release();
                return true;
            }
        }
    }

    /**
     * Lists the childen of the specified path.
     *
     * @param iPath the directory to list the children of
     * @return An array with the relative paths of the children.
     */
    public String[] listChildren(String iPath) {
        // TODO: Disallow the empty string, or use databaseName?
        if (iPath.equals("")) {
            throw new IllegalArgumentException(
                    "The empty string is not a valid path");
        }
        // Make sure the search path ends with the separator.
        if (iPath.charAt(iPath.length() -1) != SEP) {
            iPath += SEP;
        }
        ArrayList children = new ArrayList();
        synchronized (LOCK) {
            Iterator paths = files.keySet().iterator();
            String candidate;
            while (paths.hasNext()) {
                candidate = (String)paths.next();
                if (candidate.startsWith(iPath)) {
                    children.add(candidate.substring(iPath.length()));
                }
            }
        }
        return (String[])children.toArray(EMPTY_STR_ARR);
    }

    /**
     * Moves / renames a file.
     *
     * @param currentFile the current file
     * @param newFile the new file
     * @return {@code true} if the file was moved, {@code false} if the new
     *      file already existed or the existing file doesn't exist.
     */
    public boolean move(StorageFile currentFile, StorageFile newFile) {
        synchronized (LOCK) {
            if (files.containsKey(newFile.getPath())) {
                return false;
            }
            DataStoreEntry current = (DataStoreEntry)
                    files.remove(currentFile.getPath());
            if (current == null) {
                return false;
            }
            files.put(newFile.getPath(), current);
            return true;
        }
    }

    /**
     * Deletes every child of the root path specified.
     * <p>
     * Note that the root itself must be removed outside of this method.
     *
     * @param prefixPath the root path to start deleting from
     * @return {@code true} if all children of the root path were deleted,
     *      {@code false} otherwise.
     */
    private boolean _deleteAll(String prefixPath) {
        ArrayList toDelete = new ArrayList();
        Iterator paths = files.keySet().iterator();
        // Find all the entries to delete.
        while (paths.hasNext()) {
            String path = (String)paths.next();
            if (path.startsWith(prefixPath)) {
                toDelete.add(path);
            }
        }
        // Note that the root itself has already been removed before this
        // method was called. In this case, the root has to be a directory.
        // Iterate through all entries found and release them.
        Iterator keys = toDelete.iterator();
        while (keys.hasNext()) {
            DataStoreEntry entry = (DataStoreEntry)
                    files.remove((String)keys.next());
            if (!entry.isDirectory()) {
                entry.release();
            }
        }
        return true;
    }

    /**
     * Returns an identifier for a temporary file.
     *
     * @return An integer uniquely identifying a temporary file.
     */
    public long getTempFileCounter() {
        synchronized (TMP_COUNTER_LOCK) {
            return ++tmpFileCounter;
        }
    }
}
