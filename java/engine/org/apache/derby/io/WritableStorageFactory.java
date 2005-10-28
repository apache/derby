/*

   Derby - Class org.apache.derby.io.WritableStorageFactory

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

import java.io.OutputStream;
import java.io.IOException;
import java.io.SyncFailedException;

/**
 * This interface extends StorageFactory to provide read/write access to storage.
 *<p>
 * The database engine will call this interface's methods from its own privilege blocks.
 *<p>
 * Each WritableStorageFactory instance may be concurrently used by multiple threads.
 *
 */
public interface WritableStorageFactory extends StorageFactory
{


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
    public void sync( OutputStream stream, boolean metaData) throws IOException, SyncFailedException;

    /**
     * This method tests whether the StorageRandomAccessFile "rws" and "rwd" modes
     * are implemented. If the "rws" method is supported then the database
     * engine will conclude that the write methods of "rws" mode
     * StorageRandomAccessFiles are slow but the sync method is fast and optimize
     * accordingly.
     *
     * @return <b>true</b> if an StIRandomAccess file opened with "rws" or "rwd" modes immediately writes data to the
     *         underlying storage, <b>false</b> if not.
     */
    public boolean supportsRws();
}
