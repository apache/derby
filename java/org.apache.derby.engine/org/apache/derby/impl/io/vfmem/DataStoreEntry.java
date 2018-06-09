/*

   Derby - Class org.apache.derby.impl.io.vfmem.DataStoreEntry

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

/**
 * A data store entry representing either a file or a directory.
 * <p>
 * If the entry is a directory, it doesn't create a data object.
 */
public class DataStoreEntry {

    /** The path of this entry. */
    private final String path;
    /** Tells if this entry is a directory or a regular file. */
    private final boolean isDir;
    /** Tells if this entry is read-only or not. */
    private boolean isReadOnly = false;
    /** The data of the entry. */
    private final BlockedByteArray src;
    /** Tells if the entry has been released or not. */
    private volatile boolean released = false;

    /**
     * Creates a new data store entry.
     *
     * @param path the path of the entry
     * @param isDir whether the entry is a directory or a regular file
     */
    public DataStoreEntry(String path, boolean isDir) {
        this.path = path;
        this.isDir = isDir;
        if (isDir) {
            src = null;
        } else {
            src = new BlockedByteArray();
        }
    }

    /**
     * Tells if this entry is a directory.
     *
     * @return {@code true} if directory, {@code false} otherwise.
     */
    public boolean isDirectory() {
        checkIfReleased();
        return isDir;
    }

    /**
     * Returns an input stream to read from this entry.
     *
     * @return An {@code InputStream}-object.
     * @throws FileNotFoundException if this entry is a directory
     */
    BlockedByteArrayInputStream getInputStream()
            throws FileNotFoundException {
        checkIfReleased();
        if (isDir) {
            // As according to StorageFile
            throw new FileNotFoundException("'" + path + "' is a directory");
        }
        return src.getInputStream();
    }

    /**
     * Returns an output stream to write into this entry.
     *
     * @param append tells whether the entry should be appended or not
     * @return An {@code OutputStream}-object.
     * @throws FileNotFoundException if this entry is a directory, or is
     *      read-only
     */
    BlockedByteArrayOutputStream getOutputStream(boolean append)
            throws FileNotFoundException {
        checkIfReleased();
        if (isDir) {
            // As according to StorageFile
            throw new FileNotFoundException("'" + path + "' is a directory");
        }
        if (isReadOnly) {
            // As according to StorageFile
            throw new FileNotFoundException("'" + path + "' is read-only");
        }
        BlockedByteArrayOutputStream out;
        if (append) {
            out = src.getOutputStream(src.length());
        } else {
            // Truncate existing data.
            src.setLength(0L);
            out = src.getOutputStream(0L);
        }
        return out;
    }

    /**
     * Returns the length of this entry.
     *
     * @return The length in bytes.
     */
    public long length() {
        checkIfReleased();
        // Will fail with a NullPointerException if this entry is a directory.
        return src.length();
    }

    /**
     * Makes this entry read-only.
     */
    public void setReadOnly() {
        checkIfReleased();
        this.isReadOnly = true;
    }

    /**
     * Tells if this entry is read-only.
     *
     * @return {@code true} is read-only, {@code false} if not.
     */
    public boolean isReadOnly() {
        checkIfReleased();
        return this.isReadOnly;
    }

    /**
     * Relases this entry.
     */
    void release() {
        released = true;
        if (src != null) {
            src.release();
        }
    }

    /**
     * Sets the length of this entry.
     *
     * @param newLength the length in number of bytes
     */
    public void setLength(long newLength) {
        checkIfReleased();
        src.setLength(newLength);
    }

    /**
     * Checks if this entry has been released.
     *
     * @throws IllegalStateException if the entry has been released
     */
    private void checkIfReleased() {
        if (released) {
            throw new IllegalStateException("Entry has been released.");
        }
    }
}
