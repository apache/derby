/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.io
   (C) Copyright IBM Corp. 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.impl.io;

import org.apache.derby.io.StorageFile;
import org.apache.derby.io.StorageRandomAccessFile;

import org.apache.derby.iapi.services.sanity.SanityManager;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.IOException;
import java.io.FileNotFoundException;

/**
 * This class provides a class path based implementation of the StorageFile interface. It is used by the
 * database engine to access persistent data and transaction logs under the classpath subsubprotocol.
 */
class CPFile extends InputStreamFile
{

    private final CPStorageFactory storageFactory;
    private int actionCode;
    private static final int EXISTS_ACTION = 1;

    CPFile( CPStorageFactory storageFactory, String path)
    {
        super( storageFactory, path);
        this.storageFactory = storageFactory;
    }

    CPFile( CPStorageFactory storageFactory, String parent, String name)
    {
        super( storageFactory, parent, name);
        this.storageFactory = storageFactory;
    }

    CPFile( CPFile dir, String name)
    {
        super( dir,name);
        this.storageFactory = dir.storageFactory;
    }

    private CPFile( CPStorageFactory storageFactory, String child, int pathLen)
    {
        super( storageFactory, child, pathLen);
        this.storageFactory = storageFactory;
    }

    /**
     * Tests whether the named file exists.
     *
     * @return <b>true</b> if the named file exists, <b>false</b> if not.
     */
    public boolean exists()
    {
        if( storageFactory.useContextLoader)
        {
            ClassLoader cl = Thread.currentThread().getContextClassLoader();
            if( cl != null && cl.getResource( path) != null)
                return true;
        }
        if( getClass().getResource( path) != null)
        {
            if( storageFactory.useContextLoader)
                storageFactory.useContextLoader = false;
            return true;
        }
        return false;
    } // end of exists

    /**
     * Get the parent of this file.
     *
     * @param pathLen the length of the parent's path name.
     */
    StorageFile getParentDir( int pathLen)
    {
        return new CPFile( storageFactory, path, pathLen);
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
        InputStream is = null;
        if( storageFactory.useContextLoader)
        {
            ClassLoader cl = Thread.currentThread().getContextClassLoader();
            is = cl.getResourceAsStream( path);
            if( is != null)
                return is;
        }
        is = getClass().getResourceAsStream( path);
        if( is != null && storageFactory.useContextLoader)
            storageFactory.useContextLoader = false;
        if( is == null)
            throw new FileNotFoundException( "Not in class path: " + path);
        return is;
    } // end of getInputStream
}
