/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.io
   (C) Copyright IBM Corp. 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

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
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_2004;


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
