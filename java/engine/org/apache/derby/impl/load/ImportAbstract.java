/*

   Derby - Class org.apache.derby.impl.load.ImportAbstract

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
import java.sql.SQLWarning;
import java.sql.ResultSetMetaData;
import org.apache.derby.vti.VTITemplate;
import java.util.ArrayList;

/**
 * 
 * <P>
 */
abstract class ImportAbstract extends VTITemplate {

  ControlInfo controlFileReader;
  ImportReadData importReadData;

  String[] columnNames;
  int numberOfColumns;
  int[] columnWidths;

  String[] nextRow;

  ResultSetMetaData importResultSetMetaData;
  int noOfColumnsExpected;

  protected boolean lobsInExtFile = false;

  String tableColumnTypesStr;
  int[] tableColumnTypes;
  private boolean wasNull;

	static final String COLUMNNAMEPREFIX = "COLUMN";

  abstract ImportReadData getImportReadData() throws Exception;

  /** Does all the work
 	* @exception	Exception if there is an error
	*/
  void doAllTheWork() throws Exception {

    //prepare the input file for import. Get the number of columns per row
    //from the input file.
    importReadData = getImportReadData();
    numberOfColumns = importReadData.getNumberOfColumns();
	if(numberOfColumns == 0)
	{
		//file is empty. Assume same number of columns expected 
		//and return no data , But No rows gets insereted.
		this.numberOfColumns = noOfColumnsExpected;
	}

    columnWidths = controlFileReader.getColumnWidths();
    columnNames = new String[numberOfColumns];
    loadColumnNames();
    nextRow = new String[numberOfColumns];
    tableColumnTypes = ColumnInfo.getExpectedVtiColumnTypes(tableColumnTypesStr,
                                                            numberOfColumns);
	// get the ResultSetMetaData now as we know it's needed
	importResultSetMetaData =
		new ImportResultSetMetaData(numberOfColumns, columnNames, columnWidths,
                                    tableColumnTypes);


    //FIXME don't go through the resultset here. just for testing
//    while (next()) ;
  }
  //the column names will be Column#
  void loadColumnNames() {
    for (int i=1; i<=numberOfColumns; i++)
      columnNames[i-1] = COLUMNNAMEPREFIX + i;

  }


  /** Gets the resultset meta data
 	* @exception	SQLException if there is an error
	*/
  public ResultSetMetaData getMetaData() {
    return importResultSetMetaData;
  }

  //all the resultset interface methods
  /** gets the next row
 	* @exception	SQLException if there is an error
	*/
  public int getRow() throws SQLException {
    return (importReadData.getCurrentRowNumber());
  }
  
  public boolean next() throws SQLException {
    try {
      return (importReadData.readNextRow(nextRow));
    } catch (Exception ex) {
		throw LoadError.unexpectedError(ex);
	}
  }

  /** closes the resultset
 	* @exception	SQLException if there is an error
	*/
  public void close() throws SQLException {
    try {
		if(importReadData!=null)
			importReadData.closeStream();
    } catch (Exception ex) {
		throw LoadError.unexpectedError(ex);
    }
  }

  public boolean wasNull() {
		return wasNull;
  }

  /**
 	* @exception	SQLException if there is an error
	*/
  public String getString(int columnIndex) throws SQLException {

    if (columnIndex <= numberOfColumns) {
		String val = nextRow[columnIndex-1];
		if (isColumnInExtFile(columnIndex)) {
            // a clob column data is stored in an external 
            // file, the reference to it is in the main file. 
            // read the data from the external file using the 
            // reference from the main file. 
			val = importReadData.getClobColumnFromExtFile(val);
        }
		wasNull = (val == null);
		return val;
    }
    else {
       throw LoadError.invalidColumnNumber(numberOfColumns);
    }
  }

	
    /**
     * Returns <code> java.sql.Blob </code> type object that 
     * contains the columnn data from the import file. 
     * @param columnIndex number of the column. starts at 1.
     * @exception SQLException if any occurs during create of the blob object.
     */
	public java.sql.Blob getBlob(int columnIndex) throws SQLException {

		if (lobsInExtFile) 
        {
            // lob data is in another file, read from the external file.
            return importReadData.getBlobColumnFromExtFile(
                                         nextRow[columnIndex-1]);
        } else {
            // data is in the main export file, stored in hex format.
            return new ImportBlob(nextRow[columnIndex-1]);
        }
	}


    /**
     * Check if for this column type, real data is stored in an 
     * external file and only the reference is in the main import 
     * file.
     * @param colIndex number of the column. starts at 1.
     * @return         true, if the column data in a different file 
     *                 from the main import file , otherwise false.
     */
	private boolean isColumnInExtFile(int colIndex) 
	{
		if (lobsInExtFile && 
            (tableColumnTypes[colIndex -1] == java.sql.Types.BLOB || 
             tableColumnTypes[colIndex -1] == java.sql.Types.CLOB ))
			return true;
		else 
			return false;

	}
}
