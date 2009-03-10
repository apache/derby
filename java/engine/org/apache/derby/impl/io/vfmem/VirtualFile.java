/*

   Derby - Class org.apache.derby.impl.io.vfmem.VirtualFile

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

import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.derby.io.StorageFile;
import org.apache.derby.io.StorageRandomAccessFile;

/**
 * Represents a file in the virtual file system.
 * <p>
 * A virtual file is not created until one of the following methods are invoked:
 * <ul> <li>{@code createNewFile}
 *      <li>{@code getOutputStream}
 *      <li>{@code getRandomAccessFile}
 *      <li>{@code mkdir}
 *      <li>{@code mkdirs}
 * </ul>
 * <p>
 * When a method that requires access to the file data or to know if the file
 * exists or not, the assoicated data store is consulted.
 */
public class VirtualFile
        implements StorageFile {

    /** The path of this virtual file. */
    private final String path;
    /** The data store this virtual file belongs to. */
    private final DataStore dStore;

    /**
     * Creates a new virtual file handle.
     *
     * @param path the path of this virtual file
     * @param dbData the store this handle belongs to
     */
    public VirtualFile(String path, DataStore dbData) {
        this.path = path;
        this.dStore = dbData;
    }

    /**
     * Returns the contents of the directory denoted by this file, including
     * any sub directories and their contents.
     *
     * @return A list of all files and directories, or {@code null} if this file
     *      doesn't denote a directory or doesn't exist.
     */
    public String[] list() {
        DataStoreEntry entry = getEntry();
        if (entry == null || !entry.isDirectory()) {
            return null;
        }
        return dStore.listChildren(path);
    }

    /**
     * Tells if this file can be written to.
     *
     * @return {@code true} if this file exists and can be written to,
     *      {@code false} otherwise.
     */
    public boolean canWrite() {
        return (getEntry() != null && !getEntry().isReadOnly());
    }

    /**
     * Tells if this file exists.
     *
     * @return {@code true} if this file exists, {@code false} otherwise.
     */
    public boolean exists() {
        return (getEntry() != null);
    }

    /**
     * Tells if this file is a directory.
     * <p>
     * Note that {@code false} is returned if this path doesn't exist.
     *
     * @return {@code true} if this file represents an existing directoy,
     *      {@code false} otherwise.
     */
    public boolean isDirectory() {
        DataStoreEntry entry = getEntry();
        if (entry != null && entry.isDirectory()) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * Deletes this file, of if exists.
     *
     * @return {@code true} if this file exists and is successfully deleted,
     *      {@code false} otherwise.
     */
    public boolean delete() {
        return dStore.deleteEntry(path);
    }

    /**
     * Deletes the path denoted by this file and all its contents, including
     * sub directories.
     *
     * @return {@code true} if this file and all contents are successfully
     *      deleted, {@code false} otherwise.
     */
    public boolean deleteAll() {
        DataStoreEntry entry = getEntry();
        if (entry == null) {
            return false;
        }
        if (entry.isDirectory()) {
            return dStore.deleteAll(path);
        } else {
            return delete();
        }
    }

    /**
     * Returns the path of this file.
     *
     * @return The path of this file.
     */
    public String getPath() {
        return path;
    }

    public String getCanonicalPath() {
        // TODO: Should we return something that is canonical here?
        //       This would typically be to include the database directory.
        return getPath();
    }

    public String getName() {
        return PathUtil.getBaseName(path);
    }

    public java.net.URL getURL()
            throws java.net.MalformedURLException {
        throw new UnsupportedOperationException("Not supported.");
    }

    /**
     * Creates the the file denoted by this virtual file object.
     *
     * @return {@code true} if the file was successfully created, {@code false}
     *      otherwise
     */
    public boolean createNewFile() {
        return (dStore.createEntry(path, false) != null);
    }

    /**
     * Renames the file denoted by this handle.
     *
     * @param newName the new name
     * @return {@code true} if the fail was renamed, {@code false} otherwise
     */
    public boolean renameTo(StorageFile newName) {
        // TODO: How to safely handle this, with regards to paths?
        // TODO: What to do if the path denotes a non-empty directory?
        return dStore.move(this, newName);
    }

    /**
     * Creates the directory denoted by this virtual file if it doesn't exist.
     * <p>
     * For the directory to be created, it cannot exist already (either as a
     * file or a directory), and any parent directories must exist.
     *
     * @return {@code true} if the directory was created, {@code false}
     *      otherwise.
     */
    public boolean mkdir() {
        DataStoreEntry entry = getEntry();
        if (entry == null) {
            return (dStore.createEntry(path, true) != null);
        }
        return false;
    }

    /**
     * Creates the directory and any parent directories denoted by this virtual
     * file.
     * <p>
     * For the directory to be created, it cannot exist already (either as a
     * file or a directory), and all the parent elements most denote either
     * existing directories or non-existing paths.
     *
     * @return {@code true} if the directory was created, {@code false}
     *      otherwise.
     */
    public boolean mkdirs() {
        DataStoreEntry entry = getEntry();
        if (entry == null) {
            return (dStore.createAllParents(path) &&
                    (dStore.createEntry(path, true) != null));
        }
        return false;
    }

    /**
     * Returns the length of the file.
     * <p>
     * If the file doesn't exists, or is a directory, {@code 0} is returned.
     *
     * @return The length of the existing file, or {@code 0} if the path denotes
     *      a directory or a non-existing file.
     */
    public long length() {
        DataStoreEntry entry = getEntry();
        if (entry != null && !entry.isDirectory()) {
            return entry.length();
        } else {
            return 0L;
        }
    }

    public StorageFile getParentDir() {
        String parent = PathUtil.getParent(path);
        if (parent == null) {
            return null;
        } else {
            return new VirtualFile(parent, dStore);
        }
    }

    public boolean setReadOnly() {
        // TODO: How to handle directories? Should its children/contents be
        //       marked read-only too?
        DataStoreEntry entry = getEntry();
        if (entry == null) {
            return false;
        } else {
            entry.setReadOnly();
            return true;
        }
    }

    /**
     * Obtains an output stream for the file denoted.
     * <p>
     * If the file already exists, it will be truncated.
     *
     * @return An {@code OutputStream}-instance.
     * @throws FileNotFoundException if the denoted path is a directory, the
     *      file is read-only, the file cannot be created or any other reason
     *      why an {@code OutputStream} cannot be created for the file
     */
    public OutputStream getOutputStream()
            throws FileNotFoundException {
        return getOutputStream(false);
    }

    /**
     * Obtains an output stream for the file denoted.
     *
     * @param append tells if the file should be appended or truncated
     * @return An {@code OutputStream}-instance.
     * @throws FileNotFoundException if the denoted path is a directory, the
     *      file is read-only, the file cannot be created or any other reason
     *      why an {@code OutputStream} cannot be created for the file
     */
    public OutputStream getOutputStream(boolean append)
            throws FileNotFoundException {
        DataStoreEntry entry = getEntry();
        if (entry == null) {
            entry = dStore.createEntry(path, false);
            // TODO: Creation will fail if the parent directories don't exist.
            //       Is this okay, or shall we try mkdirs too?
            if (entry == null) {
                throw new FileNotFoundException("Unable to create file: " +
                        path);
            }
        }
        // The method in DataStore checks if the entry is read-only or a dir.
        return entry.getOutputStream(append);
    }

    /**
     * Returns an input stream for the file denoted.
     *
     * @return An {@code InputStream} instance.
     * @throws FileNotFoundException if the file doesn't exists or it is a
     *      directory
     */
    public InputStream getInputStream()
            throws FileNotFoundException {
        DataStoreEntry entry = getEntry();
        if (entry == null) {
            throw new FileNotFoundException(path);
        }
        // The method in DataStore checks if the entry is a directory or not.
        return entry.getInputStream();
    }

    public int getExclusiveFileLock() {
        // Just return success.
        // Since the databases created by this storeage factory can only be
        // accessed by the JVM in which it is running, there is no need to
        // implement a locking mechansim here.
        return StorageFile.EXCLUSIVE_FILE_LOCK;
    }

    public void releaseExclusiveFileLock() {}

    /**
     * Creates a random access file that can be used to read and write
     * from/into the file.
     *
     * @param mode file mode, one of "r", "rw", "rws" or "rwd" (lower-case
     *      letters are required)
     * @return A {@code StorageRandomAccessFile}-instance.
     * @throws IllegalArgumentException if the specificed mode is invalid
     * @throws FileNotFoundException if the file denoted is a directory,
     */
    public StorageRandomAccessFile getRandomAccessFile(String mode)
            throws FileNotFoundException {
        if (!(mode.equals("r") || mode.equals("rw")
                || mode.equals("rws") || mode.equals("rwd"))) {
            throw new IllegalArgumentException("Invalid mode: " + mode);
        }
        DataStoreEntry entry = getEntry();
        if (entry == null) {
            if (mode.equals("r")) {
                throw new FileNotFoundException(
                    "Cannot read from non-existing file: " + path +
                    " (mode=" + mode + ")");
            }
            // Try to create a new empty file.
            entry = dStore.createEntry(path, false);
            // TODO: Creation will fail if the parent directories don't exist.
            //       Is this okay, or shall we try mkdirs too?
            if (entry == null) {
                throw new FileNotFoundException("Unable to create file: " +
                        path + " (mode=" + mode + ")");
            }
        }
        // Checks for read-only and directory happens in the constructor.
        // Separate between read and write modes only.
        return new VirtualRandomAccessFile(entry, mode.equals("r"));
    }

    /**
     * Returns a textual representation of this file.
     *
     * @return Textual representation.
     */
    public String toString() {
        return "(db=" + dStore.getDatabaseName() + ")" + path + "#exists=" +
                exists() + ", isDirectory=" + isDirectory() + ", length=" +
                length() + ", canWrite=" + canWrite();
    }

    /**
     * Returns the data store entry denoted by this file, if it exists.
     *
     * @return The assoiciated {@code DataStoreEntry} if it exists,
     *      {@code null} if it doesn't exist.
     */
    private DataStoreEntry getEntry() {
       return dStore.getEntry(path);
    }
}
