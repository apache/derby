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

import org.apache.derby.optional.utils.ToolUtilities;

/**
 * Provides a table interface to the Lucene indexes in this database.
 * See org.apache.derby.optional.lucene.LuceneSupport.listIndexes.
 * 
 */
//IC see: https://issues.apache.org/jira/browse/DERBY-6621
class LuceneListIndexesVTI extends StringColumnVTI
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
//IC see: https://issues.apache.org/jira/browse/DERBY-590
        throws SQLException
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-590
		super
            ( new String[]
                {
                    "SCHEMANAME",
                    "TABLENAME",
                    "COLUMNNAME",
                    "LASTUPDATED",
                    "LUCENEVERSION",
                    "ANALYZER",
//IC see: https://issues.apache.org/jira/browse/DERBY-590
                    "INDEXDESCRIPTORMAKER",
                }
              );
		
        connection = LuceneSupport.getDefaultConnection();
//IC see: https://issues.apache.org/jira/browse/DERBY-590
        StorageFactory  dir = LuceneSupport.getStorageFactory( connection );
		
//IC see: https://issues.apache.org/jira/browse/DERBY-590
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

//IC see: https://issues.apache.org/jira/browse/DERBY-590
        indexes = new StorageFile[ allIndexes.size() ];
        allIndexes.toArray( indexes );
	}

	public void close() throws SQLException
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-590
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
//IC see: https://issues.apache.org/jira/browse/DERBY-590
        case 7: return getProperty( LuceneSupport.INDEX_DESCRIPTOR_MAKER );
        default:
            throw ToolUtilities.newSQLException
                (
                 SQLState.LANG_INVALID_COLUMN_POSITION,
                 col,
                 getColumnCount()
                 );
        }
	}

    /** Get the timestamp value of the 1-based column id */
    public  Timestamp   getTimestamp( int col ) throws SQLException
    {
        if ( col != 4 )
        {
//IC see: https://issues.apache.org/jira/browse/DERBY-6825
//IC see: https://issues.apache.org/jira/browse/DERBY-6825
            throw ToolUtilities.newSQLException
                (
                 SQLState.LANG_INVALID_COLUMN_POSITION,
//IC see: https://issues.apache.org/jira/browse/DERBY-6856
//IC see: https://issues.apache.org/jira/browse/DERBY-6856
                 col,
                 getColumnCount()
                 );
        }

        try {
            long    timestampMillis = Long.parseLong( getProperty( LuceneSupport.UPDATE_TIMESTAMP ) );

            return new Timestamp( timestampMillis );
        }
//IC see: https://issues.apache.org/jira/browse/DERBY-6825
        catch (NumberFormatException nfe) { throw ToolUtilities.wrap( nfe ); }
    }
    
    /** Fill in the schema, table, and column names */
    private void    readSchemaTableColumn()
        throws SQLException
    {
        if ( column != null ) { return; }
        
//IC see: https://issues.apache.org/jira/browse/DERBY-590
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
//IC see: https://issues.apache.org/jira/browse/DERBY-6730
                String      delimitedColumnName = LuceneSupport.delimitID( column );
                StorageFile    indexPropertiesFile = LuceneSupport.getIndexPropertiesFile( connection, schema, table, delimitedColumnName );
//IC see: https://issues.apache.org/jira/browse/DERBY-590
                rowProperties = readIndexProperties( indexPropertiesFile );
            }
//IC see: https://issues.apache.org/jira/browse/DERBY-6825
            catch (IOException ioe) { throw ToolUtilities.wrap( ioe ); }
            catch (PrivilegedActionException pae) { throw ToolUtilities.wrap( pae ); }
        }

        return rowProperties;
    }

    /** List files */
    private static  StorageFile[]  listDirectories( final StorageFactory storageFactory, final StorageFile dir )
    {
        return AccessController.doPrivileged
            (
//IC see: https://issues.apache.org/jira/browse/DERBY-590
             new PrivilegedAction<StorageFile[]>()
             {
                public StorageFile[] run()
                {
//IC see: https://issues.apache.org/jira/browse/DERBY-590
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
//IC see: https://issues.apache.org/jira/browse/DERBY-590
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
//IC see: https://issues.apache.org/jira/browse/DERBY-590
        } catch (PrivilegedActionException pae) {
            throw (IOException) pae.getCause();
        }
    }

}
