/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.load
   (C) Copyright IBM Corp. 1998, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.impl.load;

import java.sql.SQLException;
import org.apache.derby.vti.VTIMetaDataTemplate;

import org.apache.derby.iapi.reference.DB2Limit;

class ImportResultSetMetaData extends VTIMetaDataTemplate {
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1998_2004;

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
