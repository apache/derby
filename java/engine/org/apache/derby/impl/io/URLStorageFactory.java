/*

   Derby - Class org.apache.derby.impl.io.URLStorageFactory

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
