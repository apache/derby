/*

   Derby - Class org.apache.derby.impl.io.JarStorageFactory

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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.IOException;

import java.util.Properties;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

/**
 * This class provides a Jar file based implementation of the StorageFactory interface. It is used by the
 * database engine to access persistent data and transaction logs under the jar subsubprotocol.
 */

public class JarStorageFactory extends BaseStorageFactory
{
    ZipFile zipData;
    
    /**
     * Construct a persistent StorageFile from a path name.
     *
     * @param path The path name of the file
     *
     * @return A corresponding StorageFile object
     */
    StorageFile newPersistentFile( String path)
    {
        return new JarDBFile( this, path);
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
        return new JarDBFile( this, directoryName, fileName);
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
        return new JarDBFile( (JarDBFile) directoryName, fileName);
    }

    void doInit() throws IOException
    {
        if( dataDirectory == null)
            return;
        // Parse the dataDirectory name. It should be of the form "(jar-file)directory" or "jar-file"
        int offset = 0;
        while( offset < dataDirectory.length() & Character.isSpaceChar( dataDirectory.charAt( offset)))
            offset ++;
        int leftParen = -1;
        int rightParen = -1;
        if( offset < dataDirectory.length())
        {
            leftParen = dataDirectory.indexOf( '(', offset);
            if( leftParen >= 0)
                rightParen = dataDirectory.indexOf( ')', leftParen + 1);
        }
        File jarFile = null;
        if( rightParen > 0)
        {
            jarFile = getJarFile( dataDirectory.substring( leftParen + 1, rightParen));
            offset = rightParen + 1;
            while( offset < dataDirectory.length() & Character.isSpaceChar( dataDirectory.charAt( offset)))
                offset ++;
            dataDirectory = dataDirectory.substring( offset, dataDirectory.length());
        }
        else
        {
            jarFile = getJarFile( dataDirectory);
            dataDirectory = "";
        }
        zipData = new ZipFile( jarFile);
        canonicalName = "(" + jarFile.getCanonicalPath() + ")" + dataDirectory;
        separatedDataDirectory = dataDirectory + '/'; // Zip files use '/' as a separator
        createTempDir();
    } // end of doInit

    private File getJarFile( String name)
    {
        File jarFile = new File( name);
        if( home != null && !jarFile.isAbsolute())
            jarFile = new File( home, name);
        return jarFile;
    } // end of getJarFile
}
