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

import java.util.Properties;

/**
 * This class provides a class path based implementation of the StorageFactory interface. It is used by the
 * database engine to access persistent data and transaction logs under the classpath subsubprotocol.
 */

public class CPStorageFactory extends BaseStorageFactory
{
	/**
		IBM Copyright &copy notice.
	*/
    private static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_2004;
    boolean useContextLoader = true;
    
    /**
     * Classes implementing the StorageFactory interface must have a null
     * constructor.  This method is called when the database is booted up to
     * initialize the class. It should perform all actions necessary to start the
     * basic storage, such as creating a temporary file directory.
     *
     * The init method will be called once, before any other method is called, and will not
     * be called again.
     *
     * @param home The name of the directory containing the database. It comes from the system.home system property.
     *             It may be null. A storage factory may decide to ignore this parameter. (For instance the classpath
     *             storage factory ignores it.
     * @param databaseName The name of the database (directory). All relative pathnames are relative to this directory.
     *                     If null then the storage factory will only be used to deal with the directory containing
     *                     the databases.
     * @param create If true then the database is being created.
     * @param tempDirName The name of the temporary file directory set in properties. If null then a default
     *                    directory should be used. Each database should get a separate temporary file
     *                    directory within this one to avoid collisions.
     * @param uniqueName A unique name that can be used to create the temporary file directory for this database.
     *
     * @exception IOException on an error (unexpected).
     */
    public void init( String home, String databaseName, String tempDirName, String uniqueName)
        throws IOException
    {
        // Prefix the database name with a '/' so that the class loader will not use a Cloudscape
        // internal package.
        if( databaseName == null
            || ( databaseName.length() > 0
                 && (databaseName.charAt( 0) == '/' || databaseName.charAt( 0) == getSeparator())))
            super.init( home, databaseName, tempDirName, uniqueName);
        else
            super.init( home, "/" + databaseName, tempDirName, uniqueName);
    }
    
    /**
     * Construct a persistent StorageFile from a path name.
     *
     * @param path The path name of the file
     *
     * @return A corresponding StorageFile object
     */
    StorageFile newPersistentFile( String path)
    {
        return new CPFile( this, path);
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
        return new CPFile( this, directoryName, fileName);
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
        return new CPFile( (CPFile) directoryName, fileName);
    }

    void doInit() throws IOException
    {
        if( dataDirectory != null)
        {
            separatedDataDirectory = dataDirectory + '/'; // Class paths use '/' as a separator
            canonicalName = dataDirectory;
            createTempDir();
        }
    } // end of doInit
}
