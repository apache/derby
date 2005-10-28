/*

   Derby - Class org.apache.derby.impl.io.DirRandomAccessFile4

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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.SyncFailedException;
import java.nio.channels.ClosedChannelException;

/**
 * This class provides a disk based implementation of the StIRandomAccess File interface. It is used by the
 * database engine to access persistent data and transaction logs under the directory (default) subsubprotocol.
 *
 * This class extends DirRandomAccessFile to use the java.nio.channels.FileChannel.force() method to
 * implement sync(). Java.nio.channels.FileChannel was introduced in Java 1.4; it was not available in Java 1.3.
 */
class DirRandomAccessFile4 extends DirRandomAccessFile
{


    /**
     * Construct a StorageRandomAccessFileImpl.
     *
     * @param name The file name.
     * @param mode The file open mode: "r", "rw", "rws", or "rwd". The  "rws" and "rwd" modes specify that the file is to
     *             be synchronized, consistent with the java.io.RandomAccessFile class. However the
     *             StorageRandomAccessFile.sync() method will be called even if the file was opened
     *             in "rws" or "rwd" mode.  If the "rws" or "rwd" modes are supported then the implementation
     *             of StorageRandomAccessFile.sync need not do anything.
     *
     * @exception IllegalArgumentException if the mode argument is not equal to one of "r", "rw".
     * @exception FileNotFoundException if the file exists but is a directory rather than a regular
     *              file, or cannot be opened or created for any other reason .
     */
    DirRandomAccessFile4( File name, String mode) throws FileNotFoundException
    {
        super( name, mode);
    }

    /**
     * Force any changes out to the persistent store.
     *
     * @param metaData If true then this method is required to force changes to both the file's
     *          content and metadata to be written to storage; otherwise, it need only force content changes
     *          to be written.
     *
     * @exception IOException If an IO error occurs.
     */
    public void sync( boolean metaData) throws IOException
    {
        try
        {
            getChannel().force( metaData);
        }
        catch( ClosedChannelException cce) { throw cce;}
        catch( IOException ioe)
        {
            SyncFailedException sne = new SyncFailedException( ioe.getMessage());
            sne.initCause( ioe);
            throw sne;
        }
    } // end of sync
}
