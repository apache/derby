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

import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;
import java.security.PrivilegedActionException;

/**
 * This class provides a jar file based implementation of the StorageFile interface. It is used by the
 * database engine to access persistent data and transaction logs under the jar subsubprotocol.
 */
class JarDBFile extends InputStreamFile
{
	/**
		IBM Copyright &copy notice.
	*/
    private static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_2004;

    private final JarStorageFactory storageFactory;

    JarDBFile( JarStorageFactory storageFactory, String path)
    {
        super( storageFactory, path);
        this.storageFactory = storageFactory;
    }

    JarDBFile( JarStorageFactory storageFactory, String parent, String name)
    {
        super( storageFactory, parent, name);
        this.storageFactory = storageFactory;
    }

    JarDBFile( JarDBFile dir, String name)
    {
        super( dir,name);
        this.storageFactory = dir.storageFactory;
    }

    private JarDBFile( JarStorageFactory storageFactory, String child, int pathLen)
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
        return getEntry() != null;
    } // end of exists

    private ZipEntry getEntry()
    {
        return storageFactory.zipData.getEntry( path);
    }

    /**
     * Returns the length of the named file if it is not a directory. The return value is not specified
     * if the file is a directory.
     *
     * @return The length, in bytes, of the named file if it exists and is not a directory,
     *         0 if the file does not exist, or any value if the named file is a directory.
     */
    public long length()
    {
        ZipEntry entry = getEntry();
        if( entry == null)
            return 0;
        return entry.getSize();
    } // end of length

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
