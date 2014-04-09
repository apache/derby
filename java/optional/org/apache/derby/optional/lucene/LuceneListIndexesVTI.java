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

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Properties;

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
	private File[] indexes;
	private int row = -1;

    private String      schema;
    private String      table;
    private String      column;
    private Properties  rowProperties;

	/**
	 * Return a new LuceneListIndexesVTI.
	 * 
	 * @throws IOException
	 */
	public LuceneListIndexesVTI()
        throws IOException, PrivilegedActionException, SQLException
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
		String dir = LuceneSupport.getIndexLocation( connection, null, null, null );
		
		File luceneDir = new File(dir);
        DirFilter   dirFilter = new DirFilter();
        ArrayList<File> allIndexes = new ArrayList<File>();

        File[]  schemas = listFiles( luceneDir, dirFilter );
        if ( schemas != null )
        {
            for ( File schema : schemas )
            {
                File[]  tables = listFiles( schema, dirFilter );
                for ( File table : tables )
                {
                    File[]  indexes = listFiles( table, dirFilter );
                    for ( File index : indexes )
                    {
                        allIndexes.add( index );
                    }
                }
            }
        }

        indexes = new File[ allIndexes.size() ];
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
    
	
    public  static  class   DirFilter   implements  FileFilter
    {
        public  boolean accept( File file ) { return file.isDirectory(); }
    }

    /** Fill in the schema, table, and column names */
    private void    readSchemaTableColumn()
        throws SQLException
    {
        if ( column != null ) { return; }
        
        File    columnDir = indexes[ row ];
        column = columnDir.getName();
        File    tableDir = columnDir.getParentFile();
        table = tableDir.getName();
        File    schemaDir = tableDir.getParentFile();
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
                File    indexPropertiesFile = LuceneSupport.getIndexPropertiesFile( connection, schema, table, column );
                rowProperties = readIndexProperties( indexPropertiesFile );
            }
            catch (IOException ioe) { throw LuceneSupport.wrap( ioe ); }
            catch (PrivilegedActionException pae) { throw LuceneSupport.wrap( pae ); }
        }

        return rowProperties;
    }

    /** List files */
    private static  File[]  listFiles( final File file, final FileFilter fileFilter )
        throws IOException, PrivilegedActionException
    {
        return AccessController.doPrivileged
            (
             new PrivilegedExceptionAction<File[]>()
             {
                public File[] run() throws IOException
                {
                    if ( fileFilter == null )   { return file.listFiles(); }
                    else { return file.listFiles( fileFilter ); }
                }
             }
             );
    }

    /** Read the index properties file */
    private static  Properties readIndexProperties( final File file )
        throws IOException, PrivilegedActionException
    {
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
    }

}