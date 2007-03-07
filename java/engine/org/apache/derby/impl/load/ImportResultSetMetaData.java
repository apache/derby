/*

   Derby - Class org.apache.derby.impl.load.ImportResultSetMetaData

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

package org.apache.derby.impl.load;

import java.sql.SQLException;
import org.apache.derby.vti.VTIMetaDataTemplate;

import org.apache.derby.iapi.reference.Limits;

class ImportResultSetMetaData extends VTIMetaDataTemplate {

  private final int numberOfColumns;
  private final String[] columnNames;
  private final int[] columnWidths;
  // types of the table columns that the data is imported. 
  private final int[] tableColumnTypes ; 

  public ImportResultSetMetaData(int numberOfColumns, 
                                 String[] columnNames,
                                 int[] columnWidths, 
                                 int[] tableColumnTypes) {
    this.numberOfColumns = numberOfColumns;
    this.columnNames = columnNames;
    this.columnWidths = columnWidths;
    this.tableColumnTypes = tableColumnTypes;
  }

	public int getColumnCount() {
    return numberOfColumns;
  }

	public String getColumnName(int column) {
        return columnNames[column-1];
  }

	public int getColumnType(int column) {

        /* By default all the data in the import file is assumed
         * to be in varchar format. Appropriate casting is applied 
         * while executing the select on the import VTI. Using this 
         * approach import vti does not have to do the data conversion, 
         * casting will do that. 
         *
         * But for some types like binary types there is no casting 
         * support from varchar or the data in the file is hex format, 
         * so data  needs to be converted to binary format first. And 
         * incase of blobs/clobs stored in an exteranl file memory usage 
         * will  be less if data is supplied as stream, instead of 
         * materializing the column data as one string. For these
         * types import vti result set will return resultset column
         * type is same as the column type of the import table. Data 
         * for the blob, clob or binary type columns is returned by 
         * the getXXX() calls used by the VTI Resultset to read the 
         * data for that particular type. For example, Blob data 
         * is read using getBlob() method, which will return a 
         * Blob object that contains the data in the import file 
         * for a column. 
         */

        int colType;
        switch (tableColumnTypes[column -1])
        {
        case java.sql.Types.BLOB: 
            // blob 
            colType = java.sql.Types.BLOB;
            break;
        case java.sql.Types.CLOB: 
            // clob 
            colType = java.sql.Types.CLOB;
            break;
        case java.sql.Types.LONGVARBINARY: 
            // LONG VARCHAR FOR BIT DATA
            colType = java.sql.Types.LONGVARBINARY; 
            break;
        case java.sql.Types.VARBINARY: 
            // VARCHAR FOR BIT DATA
            colType = java.sql.Types.VARBINARY;
            break;
        case java.sql.Types.BINARY: 
            // CHAR FOR BIT DATA 
            colType = java.sql.Types.BINARY;
            break;
        default: 
            // all other data in the import file is 
            // assumed to be in varchar format.
            colType = java.sql.Types.VARCHAR;
        }

        return colType;
    }

	public int isNullable(int column) {
    return columnNullableUnknown;
  }
	public int getColumnDisplaySize(int column) {
    if (columnWidths == null)
       return Limits.DB2_VARCHAR_MAXWIDTH;
    else
       return columnWidths[column-1];
  }

}
