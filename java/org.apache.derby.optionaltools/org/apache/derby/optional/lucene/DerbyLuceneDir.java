/*

   Class org.apache.derby.optional.lucene.LuceneSupport

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

*/

package org.apache.derby.optional.lucene;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockFactory;
import org.apache.lucene.store.SingleInstanceLockFactory;

import org.apache.derby.database.Database;
import org.apache.derby.io.StorageFactory;
import org.apache.derby.io.StorageFile;
import org.apache.derby.shared.common.reference.SQLState;

import org.apache.derby.optional.utils.ToolUtilities;

/**
 * <p>
 * Derby implementation of Lucene Directory.
 * </p>
 */
class DerbyLuceneDir extends Directory
{
    /////////////////////////////////////////////////////////////////////
    //
    //  CONSTANTS
    //
    /////////////////////////////////////////////////////////////////////

    /////////////////////////////////////////////////////////////////////
    //
    //  STATE
    //
    /////////////////////////////////////////////////////////////////////

    // constructor args
    private final   StorageFactory  _storageFactory;
    private final   StorageFile     _directory;
    private final   String              _schema;
    private final   String              _table;
    private final   String              _textcol;

    // files open for output which may need to be sync'd
    private HashMap<String,DerbyIndexOutput>    _outputFiles = new HashMap<String,DerbyIndexOutput>();

    // Lucene lock factory
    private LockFactory             _lockFactory;
    
    private boolean _closed = false;

    // only supply one DerbyLuceneDir per database
    private static  HashMap<String,DerbyLuceneDir>  _openDirectories = new HashMap<String,DerbyLuceneDir>();

    /////////////////////////////////////////////////////////////////////
    //
    //  CONSTRUCTOR AND FACTORY METHODS
    //
    /////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Lookup a directory, creating its path as necessary.
     * </p>
     */
    static  synchronized    DerbyLuceneDir  getDirectory
//IC see: https://issues.apache.org/jira/browse/DERBY-590
        ( StorageFactory storageFactory, String schema, String table, String textcol )
        throws SQLException
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-590
        DerbyLuceneDir  candidate = new DerbyLuceneDir( storageFactory, schema, table, textcol );
        String              key = getKey( candidate );
        DerbyLuceneDir  result = _openDirectories.get( key );

        if ( result == null )
        {
            result = candidate;
            result.setLockFactory( new SingleInstanceLockFactory() );
            _openDirectories.put( key, result );
        }

        return result;
    }

    /**
     * <p>
     * Remove a directory from the map.
     * </p>
     */
    private static  synchronized    void    removeDir( DerbyLuceneDir dir )
    {
        _openDirectories.remove( getKey( dir ) );
    }

    /** Get the key associated with a directory */
    private static  String  getKey( DerbyLuceneDir dir )
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-590
        return dir._directory.getPath();
    }
    
    /**
     * <p>
     * Construct from the database StorageFactory and a directory path
     * of the form lucene/$schemaName/$tableName/$columnName.
     * Creates the directory if it does not already exist.
     * </p>
     */
    private DerbyLuceneDir( StorageFactory storageFactory, String schema, String table, String textcol )
//IC see: https://issues.apache.org/jira/browse/DERBY-590
        throws SQLException
    {
        _storageFactory = storageFactory;
//IC see: https://issues.apache.org/jira/browse/DERBY-590
        _schema = schema;
        _table = table;
        _textcol = textcol;
        _directory = createPath( _storageFactory, _schema, _table, _textcol );
    }

    /////////////////////////////////////////////////////////////////////
    //
    //  WRAPPERS FOR StorageFactory METHODS
    //
    /////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Get a file in this directory.
     * </p>
     */
    StorageFile     getFile( String fileName )
    {
        return _storageFactory.newStorageFile( _directory, fileName );
    }

    /**
     * <p>
     * Get the Derby directory backing this Lucene directory.
     * </p>
     */
    StorageFile getDirectory()
    {
        return _directory;
    }

    /////////////////////////////////////////////////////////////////////
    //
    //  Directory METHODS
    //
    /////////////////////////////////////////////////////////////////////

    /** Set the lock factory used by this Directory. */
    public  void    setLockFactory( LockFactory lockFactory ) { _lockFactory = lockFactory; }
    
    /** Get the lock factory used by this Directory. */
    public  LockFactory getLockFactory() { return _lockFactory; }

    /** Clear the lock */
    public void clearLock( String name )
//IC see: https://issues.apache.org/jira/browse/DERBY-590
        throws IOException
    {
        _lockFactory.clearLock( name );
    }

    /** Make a lock */
    public Lock makeLock( String name )
    {
        return _lockFactory.makeLock( name );
    }

    /**
     * <p>
     * Close this directory and remove it from the map of open directories.
     * </p>
     */
    public void close()    throws IOException
    {
        // close the output files
        for ( String fileName : _outputFiles.keySet() )
        {
            _outputFiles.get( fileName ).close();
        }

        _outputFiles.clear();
        _closed = true;
        removeDir( this );
    }

    /**  Create a new, empty file for writing */
    public DerbyIndexOutput createOutput
        ( String name, IOContext context )
        throws IOException
    {
        checkIfClosed();

        DerbyIndexOutput    indexOutput = _outputFiles.get( name );
        if ( indexOutput != null )
        {
            indexOutput.close();
        }

        StorageFile file = getStorageFile( name );
        if ( file.exists() ) { deleteFile( name ); }
        
        indexOutput = new DerbyIndexOutput( file, this );
        _outputFiles.put( name, indexOutput );

        return indexOutput;
    }

    public void deleteFile( String name ) throws IOException
    {
        checkIfClosed();
        
        StorageFile file = getStorageFile( name );

        if ( file.exists() )
        {
            if ( !file.delete() )
            {
                throw newIOException( SQLState.UNABLE_TO_DELETE_FILE, file.getPath() );
            }
        }
    }

    public boolean fileExists( String name )  throws IOException
    {
        checkIfClosed();
        
        return getStorageFile( name ).exists();
    }

    public long fileLength( String name ) throws IOException
    {
        checkIfClosed();
        
        DerbyIndexInput indexInput = openInput( name, null );

        try {
            return indexInput.length();
        }
        finally
        {
            indexInput.close();
        }
    }

    public String[] listAll()   throws IOException
    {
        checkIfClosed();
        
        return _directory.list();
    }

    public DerbyIndexInput openInput
        ( String name, IOContext context )
        throws IOException
    {
        checkIfClosed();
        
        StorageFile file = getStorageFile( name );
        if ( !file.exists() )
        {
            throw new FileNotFoundException( file.getPath() );
        }

        return getIndexInput( file );
    }

    public void sync( Collection<String> names )
        throws IOException
    {
        for ( String name : names )
        {
            DerbyIndexOutput    indexOutput = _outputFiles.get( name );

            if ( indexOutput != null )
            {
                indexOutput.flush();
            }
        }
    }

    /////////////////////////////////////////////////////////////////////
    //
    //  FOR USE WHEN CLOSING CHILD FILES
    //
    /////////////////////////////////////////////////////////////////////

    /** Remove the named file from the list of output files */
    void    removeIndexOutput( String name )
    {
        _outputFiles.remove( name );
    }

    /////////////////////////////////////////////////////////////////////
    //
    //  MINIONS
    //
    /////////////////////////////////////////////////////////////////////

    /** Get a DerbyIndexInput on the named file */
    private DerbyIndexInput getIndexInput( String name )
        throws IOException
    {
        return getIndexInput( getStorageFile( name ) );
    }
    private DerbyIndexInput getIndexInput( StorageFile file )
        throws IOException
    {
        return new DerbyIndexInput( file );
    }

    /** Turn a file name into a StorageFile handle */
    private StorageFile getStorageFile( String name )
    {
        return _storageFactory.newStorageFile( _directory, name );
    }

    /** Make an IOException with the given SQLState and args */
    private IOException newIOException( String sqlState, Object... args )
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-6825
        return new IOException( ToolUtilities.newSQLException( sqlState, args ).getMessage() );
    }

    /** Raise an exception if this directory is closed */
    private void    checkIfClosed() throws IOException
    {
        if ( _closed )
        {
            throw newIOException( SQLState.DATA_CONTAINER_CLOSED );
        }
    }

	/**
	 * Create the path if necessary.
	 */
    private static StorageFile createPath
//IC see: https://issues.apache.org/jira/browse/DERBY-590
        ( final StorageFactory storageFactory, final String schema, final String table, final String textcol )
//IC see: https://issues.apache.org/jira/browse/DERBY-590
        throws SQLException
    {
        StorageFile    luceneDir = createPathLeg( storageFactory, null, Database.LUCENE_DIR );
        StorageFile    schemaDir = createPathLeg( storageFactory, luceneDir, schema );
        StorageFile    tableDir = createPathLeg( storageFactory, schemaDir, table );
        StorageFile    indexDir = createPathLeg( storageFactory, tableDir, textcol );

        return indexDir;
    }

	/**
	 * Create the path if necessary.
	 */
    private static StorageFile createPathLeg
        ( final StorageFactory storageFactory, final StorageFile parentDir, final String fileName )
//IC see: https://issues.apache.org/jira/browse/DERBY-590
        throws SQLException
    {
        try {
            return AccessController.doPrivileged(
             new PrivilegedExceptionAction<StorageFile>()
             {
                 public StorageFile run() throws SQLException
                 {
//IC see: https://issues.apache.org/jira/browse/DERBY-6825
                     String         normalizedName = ToolUtilities.derbyIdentifier( fileName );
                     StorageFile    file = parentDir == null ?
                         storageFactory.newStorageFile( normalizedName  ) :
                         storageFactory.newStorageFile( parentDir, normalizedName );

                     if ( !file.exists() ) { file.mkdir(); }
                     if ( !file.exists() )
                     {
//IC see: https://issues.apache.org/jira/browse/DERBY-6825
                         throw ToolUtilities.newSQLException
                             ( SQLState.SERVICE_DIRECTORY_CREATE_ERROR, normalizedName );
                     }
                     else { return file; }
                 }
             }
             );
//IC see: https://issues.apache.org/jira/browse/DERBY-590
        } catch (PrivilegedActionException pae) {
            throw (SQLException) pae.getCause();
        }
    }
    
}

