/*

   Class org.apache.derby.impl.optional.lucene.LuceneListIndexesVTI

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

package org.apache.derby.impl.optional.lucene;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.security.PrivilegedActionException;
import java.sql.SQLException;
import java.text.DateFormat;
import java.util.ArrayList;

import org.apache.derby.shared.common.reference.SQLState;
import org.apache.derby.vti.StringColumnVTI;

/**
 * Provides a table interface to the Lucene indexes in this database.
 * See org.apache.derby.impl.optional.lucene.LuceneSupport.listIndexes.
 * 
 */
public class LuceneListIndexesVTI extends StringColumnVTI {
	
	File[] indexes;
	int row = -1;
	String schema, table;
	
	/**
	 * Return a new LuceneListIndexesVTI.
	 * 
	 * @throws IOException
	 */
	public LuceneListIndexesVTI()
        throws IOException, PrivilegedActionException, SQLException
    {
		super(new String[]{"ID","SCHEMANAME","TABLENAME","COLUMNNAME","LASTUPDATED"});
		
		String dir = LuceneSupport.getIndexLocation( LuceneSupport.getDefaultConnection(), null, null, null );
		
		File luceneDir = new File(dir);
        DirFilter   dirFilter = new DirFilter();
        ArrayList<File> allIndexes = new ArrayList<File>();

        File[]  schemas = LuceneSupport.listFiles( luceneDir, dirFilter );
        if ( schemas != null )
        {
            for ( File schema : schemas )
            {
                File[]  tables = LuceneSupport.listFiles( schema, dirFilter );
                for ( File table : tables )
                {
                    File[]  indexes = LuceneSupport.listFiles( table, dirFilter );
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

	public void close() throws SQLException {
		
	}

	public boolean next() throws SQLException {
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
	protected String getRawColumn(int col) throws SQLException {

        File    columnDir = indexes[ row ];
        String columnPart = columnDir.getName();
        File    tableDir = columnDir.getParentFile();
        String  tablePart = tableDir.getName();
        File    schemaDir = tableDir.getParentFile();
        String  schemaPart = schemaDir.getName();

		if (col == 1) {
			return Integer.toString(row+1);
		} else if (col == 2) {
			return schemaPart;
		} else if (col == 3) {
			return tablePart;
		} else if (col == 4) {
			return columnPart;
		} else if (col == 5) {
            try {
                DateFormat df = DateFormat.getDateTimeInstance();
                return df.format( LuceneSupport.getLastModified( columnDir ) );
            }
            catch (Exception e) { throw LuceneSupport.wrap( e ); }
		}
        else
        {
            throw LuceneSupport.newSQLException
                (
                 SQLState.LANG_INVALID_COLUMN_POSITION,
                 new Integer( col ),
                 new Integer( getColumnCount() )
                 );
            }
	}
	
    public  static  class   DirFilter   implements  FileFilter
    {
        public  boolean accept( File file ) { return file.isDirectory(); }
    }

}