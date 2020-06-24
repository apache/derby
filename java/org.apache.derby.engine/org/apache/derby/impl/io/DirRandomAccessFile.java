/*

   Derby - Class org.apache.derby.impl.io.DirRandomAccessFile

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

import org.apache.derby.shared.common.sanity.SanityManager;

import org.apache.derby.io.StorageFile;
import org.apache.derby.io.StorageRandomAccessFile;

import java.io.File;
import java.io.RandomAccessFile;
import java.io.IOException;
import java.io.FileNotFoundException;

/**
 * This class provides a disk based implementation of the StIRandomAccess File interface. It is used by the
 * database engine to access persistent data and transaction logs under the directory (default) subsubprotocol.
 */
class DirRandomAccessFile extends RandomAccessFile implements StorageRandomAccessFile
{
    // for cloning
    private final   File    _name;
    private final   String  _mode;

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
    DirRandomAccessFile( File name, String mode) throws FileNotFoundException
    {
        super( name, mode);
//IC see: https://issues.apache.org/jira/browse/DERBY-590
        _name = name;
        _mode = mode;
    }

    /** Clone this file abstaction */
    public  DirRandomAccessFile clone()
    {
        try {
            return new DirRandomAccessFile( _name, _mode );
        }
        catch (IOException ioe)
        {
            throw new RuntimeException( ioe.getMessage(), ioe );
        }
    }

    /**
     * Force any changes out to the persistent store.
     *
     * @exception IOException If an IO error occurs.
     */
    public void sync( ) throws IOException
    {
        getFD().sync();
    }
}
    
