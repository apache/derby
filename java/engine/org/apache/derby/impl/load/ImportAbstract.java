/*

   Derby - Class org.apache.derby.impl.load.ImportAbstract

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
import java.sql.SQLWarning;
import java.sql.ResultSetMetaData;
import org.apache.derby.vti.VTITemplate;
import java.util.ArrayList;

/**
 * 
 * <P>
 */
abstract class ImportAbstract extends VTITemplate {

  protected ControlInfo controlFileReader;
  protected ImportReadData importReadData;

  protected String[] columnNames;
  protected int numberOfColumns;
  protected int[] columnWidths;

  protected String[] nextRow;

  protected ResultSetMetaData importResultSetMetaData;
  protected int noOfColumnsExpected;

  private boolean wasNull;

	protected static final String COLUMNNAMEPREFIX = "COLUMN";

  protected abstract ImportReadData getImportReadData() throws Exception;

  /** Does all the work
 	* @exception	Exception if there is an error
	*/
  protected void doAllTheWork() throws Exception {

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

	// get the ResultSetMetaData now as we know it's needed
	importResultSetMetaData =
		new ImportResultSetMetaData(numberOfColumns, columnNames, columnWidths);


    //FIXME don't go through the resultset here. just for testing
//    while (next()) ;
  }
  //the column names will be Column#
  protected void loadColumnNames() {
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
		wasNull = (val == null);
       return val;
    }
    else {
       throw LoadError.invalidColumnNumber(numberOfColumns);
    }
  }
}
