/*

   Derby - Class org.apache.derby.impl.load.ImportResultSetMetaData

   Copyright 1998, 2004 The Apache Software Foundation or its licensors, as applicable.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

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

import org.apache.derby.iapi.reference.DB2Limit;

class ImportResultSetMetaData extends VTIMetaDataTemplate {

  private final int numberOfColumns;
  private final String[] columnNames;
  private final int[] columnWidths;

  public ImportResultSetMetaData(int numberOfColumns, String[] columnNames,
  int[] columnWidths) {
    this.numberOfColumns = numberOfColumns;
    this.columnNames = columnNames;
    this.columnWidths = columnWidths;
  }

	public int getColumnCount() {
    return numberOfColumns;
  }

	public String getColumnName(int column) {
    return columnNames[column-1];
  }

	public int getColumnType(int column) {
    return java.sql.Types.VARCHAR;
  }

	public int isNullable(int column) {
    return columnNullableUnknown;
  }
	public int getColumnDisplaySize(int column) {
    if (columnWidths == null)
       return DB2Limit.DB2_VARCHAR_MAXWIDTH;
    else
       return columnWidths[column-1];
  }
}
