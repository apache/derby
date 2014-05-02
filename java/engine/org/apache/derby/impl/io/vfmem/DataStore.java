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

import java.io.File;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Iterator;

import org.apache.derby.io.StorageFile;

/**
 * A virtual data store, keeping track of all the virtual files existing and
 * offering a set of high-level operations on virtual files.
 * <p>
 * A newly created data store doesn't contain a single existing directory.
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
    private final Map<String,DataStoreEntry> files = new HashMap<String,DataStoreEntry>(80);

    /**
     * The name of the database this store serves, expected to be the absolute
     * path of the service root (i.e. /tmp/myDB if the database myDB is created
     * in /tmp).
     */
    private final String databaseName;
    /** Counter used to generate unique temporary file names. */
    private long tmpFileCounter = 0;
    /** Tells if this store is scheduled for deletion. */
    private boolean deleteMe;

    /**
     * Creates a new data store.
     *
     * @param databaseName the name of the assoicated database, expected to be
     *      the absolute path of the service root.
     */
    public DataStore(String databaseName) {
        this.databaseName = databaseName;
    }

    /**
     * Returns the database name, which is expected to equal the path of the
     * service root.
     *
     * @return The database name.
     */
    public String getDatabaseName() {
        return this.databaseName;
    }

    /**
     * Tells if this data store is scheduled for deletion.
     *
     * @return {@code true} if the store is awaiting deletion,
     *      {@code false} otherwise.
     */
    public boolean scheduledForDeletion() {
        return this.deleteMe;
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
        // Normalize the path.
        final String nPath = new File(iPath).getPath();
        synchronized (LOCK) {
            if (files.containsKey(nPath)) {
                return null;
            }
            // Make sure the the parent directories exists.
            String[] parents = getParentList(nPath);
            for (int i=parents.length -1; i >= 0; i--) {
                DataStoreEntry entry = files.get(parents[i]);
                if (entry == null) {
                    return null;
                } else if (!entry.isDirectory()) {
                    return null;
                }
            }
            DataStoreEntry newEntry = new DataStoreEntry(nPath, isDir);
            files.put(nPath, newEntry);
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
        final String nPath = new File(path).getPath();
        // Iterate through the list and create the missing parents.
        String[] parents = getParentList(nPath);
        synchronized (LOCK) {
            for (int i=parents.length -1; i >= 0; i--) {
                String subPath = parents[i];
                DataStoreEntry entry = files.get(subPath);
                if (entry == null) {
                    createEntry(subPath, true);
                } else if (!entry.isDirectory()) {
                    // Fail if one of the parents is a regular file.
                    return false;
                }
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
        final String nPath = new File(iPath).getPath();
        DataStoreEntry entry;
        synchronized (LOCK) {
            entry = files.remove(nPath);
            if (entry != null) {
                if (entry.isDirectory()) {
                    String[] children = listChildren(nPath);
                    if (children.length > 0) {
                        // Re-add the entry.
                        files.put(nPath, entry);
                        return false;
                    }
                    // Check if we just deleted the service root. Normally the
                    // service would be deleted using deleteAll.
                    if (nPath.equals(databaseName) &&
                            files.get(databaseName) == null) {
                        // Service root deleted, mark this store for removal.
                        deleteMe = true;
                    }
                }
                entry.release();
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
            // Use java.io.File to normalize the path.
            return files.get(new File(iPath).getPath());
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
        final String nPath = new File(iPath).getPath();
        synchronized (LOCK) {
            DataStoreEntry entry = files.remove(nPath);
            if (entry == null) {
                // Delete root doesn't exist.
                return false;
            } else if (entry.isDirectory()) {
                // Delete root is a directory.
                boolean deleted = _deleteAll(nPath);
                if (files.get(databaseName) == null) {
                    // The service root has been deleted, which means that all
                    // the data has been deleted. Mark this store for removal.
                    deleteMe = true;
                }
                return deleted;
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
        String nPath = new File(iPath).getPath();
        // Make sure the search path ends with the separator.
        if (nPath.charAt(nPath.length() -1) != SEP) {
            nPath += SEP;
        }
        ArrayList<String> children = new ArrayList<String>();
        synchronized (LOCK) {
            Iterator<String> paths = files.keySet().iterator();
            String candidate;
            while (paths.hasNext()) {
                candidate = paths.next();
                if (candidate.startsWith(nPath)) {
                    candidate = candidate.substring(nPath.length());
                    // don't include grandchildren
                    if ( candidate.indexOf( PathUtil.SEP_STR ) < 0 )
                    {
                        children.add(candidate);
                    }
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
        final String currentPath = new File(currentFile.getPath()).getPath();
        final String newPath = new File(newFile.getPath()).getPath();
        synchronized (LOCK) {
            if (files.containsKey(newPath)) {
                return false;
            }
            DataStoreEntry current = files.remove(currentPath);
            if (current == null) {
                return false;
            }
            files.put(newPath, current);
            return true;
        }
    }

    /**
     * Purges the database and releases all files associated with it.
     */
    public void purge() {
        synchronized (LOCK) {
            Iterator<DataStoreEntry> fileIter = files.values().iterator();
            while (fileIter.hasNext()) {
                (fileIter.next()).release();
            }
            // Clear all the mappings.
            files.clear();
        }
    }

    /**
     * Deletes every child of the root path specified.
     * <p>
     * Note that the root itself must be removed outside of this method.
     *
     * @param prefixPath the normalized root path to start deleting from
     * @return {@code true} if all children of the root path were deleted,
     *      {@code false} otherwise.
     */
    //@GuardedBy("LOCK")
    private boolean _deleteAll(String prefixPath) {
        // Make sure the search path ends with the separator.
        if (prefixPath.charAt(prefixPath.length() -1) != SEP) {
            prefixPath += SEP;
        }
        ArrayList<String> toDelete = new ArrayList<String>();
        Iterator<String> paths = files.keySet().iterator();
        // Find all the entries to delete.
        while (paths.hasNext()) {
            String path = paths.next();
            if (path.startsWith(prefixPath)) {
                toDelete.add(path);
            }
        }
        // Note that the root itself has already been removed before this
        // method was called. In this case, the root has to be a directory.
        // Iterate through all entries found and release them.
        Iterator keys = toDelete.iterator();
        while (keys.hasNext()) {
            DataStoreEntry entry = files.remove((String)keys.next());
            entry.release();
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

    /**
     * Returns the list of parents for the specified path.
     * <p>
     * The lowest level parent is listed first in the list, so all absolute
     * paths will have the root listed as the last element.
     *
     * @param path the normalized path to create a parent list for
     * @return A list of parents.
     */
    private String[] getParentList(String path) {
        ArrayList<String> parents = new ArrayList<String>();
        String parent = path;
        // Build the list of parents.
        while ((parent = new File(parent).getParent()) != null) {
            parents.add(parent);
        }
        return (String[])parents.toArray(new String[parents.size()]);
    }
}
