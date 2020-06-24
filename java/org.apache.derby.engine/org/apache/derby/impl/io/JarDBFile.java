/*

   Derby - Class org.apache.derby.impl.io.JarDBFile

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package org.apache.derby.impl.io;

import org.apache.derby.io.StorageFile;
import java.io.InputStream;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.util.zip.ZipEntry;

/**
 * This class provides a jar file based implementation of the StorageFile interface. It is used by the
 * database engine to access persistent data and transaction logs under the jar subsubprotocol.
 */
//IC see: https://issues.apache.org/jira/browse/DERBY-6535
class JarDBFile extends InputStreamFile<JarStorageFactory>
{

    JarDBFile( JarStorageFactory storageFactory, String path)
    {
        super( storageFactory, path);
    }

    JarDBFile( JarStorageFactory storageFactory, String parent, String name)
    {
        super( storageFactory, parent, name);
    }

    JarDBFile( JarDBFile dir, String name)
    {
        super( dir,name);
    }

    private JarDBFile( JarStorageFactory storageFactory, String child, int pathLen)
    {
        super( storageFactory, child, pathLen);
    }

    /**
     * Tests whether the named file exists.
     *
     * @return <b>true</b> if the named file exists, <b>false</b> if not.
     */
    public boolean exists()
    {
        return getEntry() != null;
    } // end of exists

    private ZipEntry getEntry()
    {
        return storageFactory.zipData.getEntry( path);
    }

    /**
     * Get the name of the parent directory if this name includes a parent.
     *
     * @return An StorageFile denoting the parent directory of this StorageFile, if it has a parent, null if
     *         it does not have a parent.
     */
    StorageFile getParentDir( int pathLen)
    {
        return new JarDBFile( storageFactory, path, pathLen);
    }
    
    /**
     * Creates an input stream from a file name.
     *
     * @return an input stream suitable for reading from the file.
     *
     * @exception FileNotFoundException if the file is not found.
     */
    public InputStream getInputStream( ) throws FileNotFoundException
    {
        ZipEntry zipEntry = getEntry( );
		if (zipEntry == null)
			throw new java.io.FileNotFoundException(path);

        try
        {
            return storageFactory.zipData.getInputStream(zipEntry);
        }
        catch( IOException ioe){ throw new java.io.FileNotFoundException(path);}
    } // end of getInputStream

    /**
     * Get the file name for diagnostic purposes. Usually the same as getPath().
     *
     * @return the file name
     */
    public String toString()
    {
        return path;
    }
}
