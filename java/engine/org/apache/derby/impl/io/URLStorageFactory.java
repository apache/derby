/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.io
   (C) Copyright IBM Corp. 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.impl.io;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.io.StorageFactory;
import org.apache.derby.io.StorageFile;

import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.IOException;

/**
 * This class provides a http based implementation of the StorageFactory interface. It is used by the
 * database engine to access persistent data and transaction logs under the http and https subsubprotocols.
 */

public class URLStorageFactory extends BaseStorageFactory
{
	/**
		IBM Copyright &copy notice.
	*/
    private static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_2004;
    
    /**
     * Construct a persistent StorageFile from a path name.
     *
     * @param path The path name of the file
     *
     * @return A corresponding StorageFile object
     */
    StorageFile newPersistentFile( String path)
    {
        return new URLFile( this, path);
    }

    /**
     * Construct a StorageFile from a directory and file name.
     *
     * @param directoryName The directory part of the path name. Must not be null, nor may it be in the temp dir.
     * @param fileName The name of the file within the directory.
     *
     * @return A corresponding StorageFile object
     */
    StorageFile newPersistentFile( String directoryName, String fileName)
    {
        if( directoryName == null || directoryName.length() == 0)
            return newPersistentFile( fileName);
        return new URLFile( this, directoryName, fileName);
    }

    /**
     * Construct a StorageFile from a directory and file name.
     *
     * @param directoryName The directory part of the path name.
     * @param fileName The name of the file within the directory.
     *
     * @return A corresponding StorageFile object
     */
    StorageFile newPersistentFile( StorageFile directoryName, String fileName)
    {
        if( directoryName == null)
            return newPersistentFile( fileName);
        return new URLFile( (URLFile) directoryName, fileName);
    }

    void doInit() throws IOException
    {
        if( dataDirectory != null)
        {
            if( dataDirectory.endsWith( "/"))
            {
                separatedDataDirectory = dataDirectory;
                dataDirectory = dataDirectory.substring( 0, dataDirectory.length() - 1);
            }
            else
                separatedDataDirectory = dataDirectory + '/'; // URLs use '/' as a separator
            canonicalName = dataDirectory;
            createTempDir();
        }
    } // end of doInit
}
