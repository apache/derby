/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.io
   (C) Copyright IBM Corp. 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.impl.io;

import org.apache.derby.io.StorageFile;
import org.apache.derby.iapi.services.info.JVMInfo;

import java.io.IOException;

/**
 * This class implements the WritableStorageFactory interface using features found in Java 1.4 but
 * not in earlier versions of Java.
 */
public class DirStorageFactory4 extends DirStorageFactory
{
	/**
		IBM Copyright &copy notice.
	*/
    private static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_2004;

	private static final boolean	rwsOK = JVMInfo.JDK_ID >= 5;
    
    /**
     * Most of the initialization is done in the init method.
     */
    public DirStorageFactory4()
    {
        super();
    }

    /**
     * Construct a persistent StorageFile from a path name.
     *
     * @param path The path name of the file. Guaranteed not to be in the temporary file directory. If null
     *             then the database directory should be returned.
     *
     * @return A corresponding StorageFile object
     */
    StorageFile newPersistentFile( String path)
    {
        if( path == null)
            return new DirFile4( dataDirectory, rwsOK);
        return new DirFile4(dataDirectory, path, rwsOK);
    }

    /**
     * Construct a persistent StorageFile from a directory and path name.
     *
     * @param directory The path name of the directory. Guaranteed not to be in the temporary file directory.
     *                  Guaranteed not to be null
     * @param fileName The name of the file within the directory. Guaranteed not to be null.
     *
     * @return A corresponding StorageFile object
     */
    StorageFile newPersistentFile( String directoryName, String fileName)
    {
        return new DirFile4( separatedDataDirectory + directoryName, fileName, rwsOK);
    }

    /**
     * Construct a persistent StorageFile from a directory and path name.
     *
     * @param directory The path name of the directory. Guaranteed not to be to be null. Guaranteed to be
     *                  created by a call to one of the newPersistentFile methods.
     * @param fileName The name of the file within the directory. Guaranteed not to be null.
     *
     * @return A corresponding StorageFile object
     */
    StorageFile newPersistentFile( StorageFile directoryName, String fileName)
    {
        return new DirFile4( (DirFile) directoryName, fileName, rwsOK);
    }

	
    /**
     * This method tests whether the "rws" and "rwd" modes are implemented. If the "rws" method is supported
     * then the database engine will conclude that the write methods of "rws" mode StorageRandomAccessFiles are
     * slow but the sync method is fast and optimize accordingly.
     *
     * @return <b>true</b> if an StIRandomAccess file opened with "rws" or "rwd" modes immediately writes data to the
     *         underlying storage, <b>false</b> if not.
     */
    public boolean supportsRws()
    {
        return rwsOK;
    }
	

}
