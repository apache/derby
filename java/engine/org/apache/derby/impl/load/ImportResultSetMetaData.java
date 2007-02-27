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

        // if the table column type is BLOB/CLOB , then the 
        // data in the import files will converted to 
        // BLOB/CLOB type objects. So the vti result column 
        // type for blob/clob is same as  table column type. 
        // Data for Other types is considered is of VARCHAR type, 
        // and they are casted to table column type, if needed 
        // while doing the select from the VTI. 

		if (tableColumnTypes[column -1] ==  java.sql.Types.BLOB)
			return java.sql.Types.BLOB;
		else
            if (tableColumnTypes[column -1] ==  java.sql.Types.CLOB)
                return java.sql.Types.CLOB;
            else
                return java.sql.Types.VARCHAR;
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
