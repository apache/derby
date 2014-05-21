/*

   Class org.apache.derby.optional.lucene.LuceneListIndexesVTI

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

import java.io.IOException;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Properties;

import org.apache.derby.database.Database;
import org.apache.derby.io.StorageFactory;
import org.apache.derby.io.StorageFile;

import org.apache.derby.shared.common.reference.SQLState;
import org.apache.derby.vti.StringColumnVTI;

/**
 * Provides a table interface to the Lucene indexes in this database.
 * See org.apache.derby.optional.lucene.LuceneSupport.listIndexes.
 * 
 */
public class LuceneListIndexesVTI extends StringColumnVTI
{
    private Connection  connection;
	private StorageFile[] indexes;
	private int row = -1;

    private String      schema;
    private String      table;
    private String      column;
    private Properties  rowProperties;

	/**
	 * Return a new LuceneListIndexesVTI.
	 */
	public LuceneListIndexesVTI()
        throws SQLException
    {
		super
            ( new String[]
                {
                    "SCHEMANAME",
                    "TABLENAME",
                    "COLUMNNAME",
                    "LASTUPDATED",
                    "LUCENEVERSION",
                    "ANALYZER",
                    "ANALYZERMAKER",
                }
              );
		
        connection = LuceneSupport.getDefaultConnection();
        StorageFactory  dir = LuceneSupport.getStorageFactory( connection );
		
		StorageFile luceneDir = dir.newStorageFile( Database.LUCENE_DIR );
        ArrayList<StorageFile> allIndexes = new ArrayList<StorageFile>();

        StorageFile[]  schemas = listDirectories( dir, luceneDir );
        if ( schemas != null )
        {
            for ( StorageFile schema : schemas )
            {
                StorageFile[]  tables = listDirectories( dir, schema );
                for ( StorageFile table : tables )
                {
                    StorageFile[]  indexes = listDirectories( dir, table );
                    for ( StorageFile index : indexes )
                    {
                        allIndexes.add( index );
                    }
                }
            }
        }

        indexes = new StorageFile[ allIndexes.size() ];
        allIndexes.toArray( indexes );
	}

	public void close() throws SQLException
    {
		connection = null;
        indexes = null;
        schema = null;
        table = null;
        column = null;
        rowProperties = null;
	}

	public boolean next() throws SQLException
    {
        schema = null;
        table = null;
        column = null;
        rowProperties = null;
        
		row++;
		if (row < indexes.length) {
			return true;
		}
		return false;
	}

	/**
	 * columns:
	 * 1 == id
	 * 2 == schema
	 * 3 == table
	 * 4 == column name
	 * 5 == last modified
	 */
	protected String getRawColumn( int col ) throws SQLException
    {
        readSchemaTableColumn();
        
        switch( col )
        {
        case 1: return schema;
        case 2: return table;
        case 3: return column;
        case 5: return getProperty( LuceneSupport.LUCENE_VERSION );
        case 6: return getProperty( LuceneSupport.ANALYZER );
        case 7: return getProperty( LuceneSupport.ANALYZER_MAKER );
        default:
            throw LuceneSupport.newSQLException
                (
                 SQLState.LANG_INVALID_COLUMN_POSITION,
                 new Integer( col ),
                 new Integer( getColumnCount() )
                 );
        }
	}

    /** Get the timestamp value of the 1-based column id */
    public  Timestamp   getTimestamp( int col ) throws SQLException
    {
        if ( col != 4 )
        {
            throw LuceneSupport.newSQLException
                (
                 SQLState.LANG_INVALID_COLUMN_POSITION,
                 new Integer( col ),
                 new Integer( getColumnCount() )
                 );
        }

        try {
            long    timestampMillis = Long.parseLong( getProperty( LuceneSupport.UPDATE_TIMESTAMP ) );

            return new Timestamp( timestampMillis );
        }
        catch (NumberFormatException nfe) { throw LuceneSupport.wrap( nfe ); }
    }
    
    /** Fill in the schema, table, and column names */
    private void    readSchemaTableColumn()
        throws SQLException
    {
        if ( column != null ) { return; }
        
        StorageFile    columnDir = indexes[ row ];
        column = columnDir.getName();
        StorageFile    tableDir = columnDir.getParentDir();
        table = tableDir.getName();
        StorageFile    schemaDir = tableDir.getParentDir();
        schema = schemaDir.getName();
    }

    /** get the string value of a property from the row properties */
    private String  getProperty( String key )
        throws SQLException
    {
        return getRowProperties().getProperty( key );
    }
    
    /** get the properties of the current row */
    private Properties  getRowProperties()
        throws SQLException
    {
        if ( rowProperties == null )
        {
            try {
                readSchemaTableColumn();
                StorageFile    indexPropertiesFile = LuceneSupport.getIndexPropertiesFile( connection, schema, table, column );
                rowProperties = readIndexProperties( indexPropertiesFile );
            }
            catch (IOException ioe) { throw LuceneSupport.wrap( ioe ); }
            catch (PrivilegedActionException pae) { throw LuceneSupport.wrap( pae ); }
        }

        return rowProperties;
    }

    /** List files */
    private static  StorageFile[]  listDirectories( final StorageFactory storageFactory, final StorageFile dir )
    {
        return AccessController.doPrivileged
            (
             new PrivilegedAction<StorageFile[]>()
             {
                public StorageFile[] run()
                {
                    ArrayList<StorageFile>  subdirectories = new ArrayList<StorageFile>();
                    String[]    fileNames = dir.list();

                    for ( String fileName : fileNames )
                    {
                        StorageFile candidate = storageFactory.newStorageFile( dir, fileName );
                        if ( candidate.isDirectory() ) { subdirectories.add( candidate ); }
                    }

                    StorageFile[]   result = new StorageFile[ subdirectories.size() ];
                    subdirectories.toArray( result );
                    
                    return result;
                }
             }
             );
    }

    /** Read the index properties file */
    private static  Properties readIndexProperties( final StorageFile file )
        throws IOException
    {
        try {
            return AccessController.doPrivileged
            (
             new PrivilegedExceptionAction<Properties>()
             {
                public Properties run() throws IOException
                {
                    return LuceneSupport.readIndexPropertiesNoPrivs( file );
                }
             }
             );
        } catch (PrivilegedActionException pae) {
            throw (IOException) pae.getCause();
        }
    }

}