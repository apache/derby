/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.load
   (C) Copyright IBM Corp. 1998, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

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
