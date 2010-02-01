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

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.ResultSetMetaData;
import org.apache.derby.vti.VTITemplate;
import java.util.ArrayList;
import java.util.HashMap;
import org.apache.derby.iapi.util.StringUtil;
import org.apache.derby.iapi.error.PublicAPI;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.error.StandardException;


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

  int lineNumber = 0;
  String[] nextRow;

  ImportResultSetMetaData importResultSetMetaData;
  int noOfColumnsExpected;

  protected boolean lobsInExtFile = false;

  String tableColumnTypesStr;
  int[] tableColumnTypes;
  String columnTypeNamesString;
  String[] columnTypeNames;
  String  udtClassNamesString;
  HashMap udtClasses;
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
    columnTypeNames =  ColumnInfo.getExpectedColumnTypeNames( columnTypeNamesString, numberOfColumns );
    udtClasses = ColumnInfo.getExpectedUDTClasses( udtClassNamesString );
	// get the ResultSetMetaData now as we know it's needed
	importResultSetMetaData =
		new ImportResultSetMetaData(numberOfColumns, columnNames, columnWidths,
                                    tableColumnTypes, columnTypeNames, udtClasses );


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
  
    /** gets the current line number */
    public    int getCurrentLineNumber() { return lineNumber; }
    
  public boolean next() throws SQLException {
    try {
      lineNumber++;
      return (importReadData.readNextRow(nextRow));
    } catch (Exception ex) {
		throw importError(ex);
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
			val = importReadData.getClobColumnFromExtFileAsString(val, 
                                                                  columnIndex);
        }
		wasNull = (val == null);
		return val;
    }
    else {
       throw LoadError.invalidColumnNumber(numberOfColumns);
    }
  }


    /**
     * Returns <code> java.sql.Clob </code> type object that 
     * contains the column data from the import file. 
     * @param columnIndex number of the column. starts at 1.
     * @exception SQLException if any occurs during create of the clob object.
     */
	public java.sql.Clob getClob(int columnIndex) throws SQLException {

        java.sql.Clob clob = null;
		if (lobsInExtFile) 
        {
            // lob data is in another file, read from the external file.
            clob = importReadData.getClobColumnFromExtFile(
                                         nextRow[columnIndex-1], columnIndex);
        } else {
            // data is in the main export file.
            String data =  nextRow[columnIndex-1];
            if (data != null) {
                clob = new ImportClob(data);                
            }
        }
        
        wasNull = (clob == null);
        return clob;
	}

	
    /**
     * Returns <code> java.sql.Blob </code> type object that 
     * contains the column data from the import file. 
     * @param columnIndex number of the column. starts at 1.
     * @exception SQLException if any occurs during create of the blob object.
     */
	public java.sql.Blob getBlob(int columnIndex) throws SQLException {

        java.sql.Blob blob = null;
		if (lobsInExtFile) 
        {
            // lob data is in another file, read from the external file.
            blob = importReadData.getBlobColumnFromExtFile(
                                         nextRow[columnIndex-1], columnIndex);
        } else {
            // data is in the main export file, stored in hex format.
            String hexData = nextRow[columnIndex-1];
            byte[] data = null;
            if (hexData != null) {
                // Derby export calls Resultset.getString() method
                // when blob column data is not exported to an 
                // external file. Derby getString() method return 
                // the data in hex format for binary types, by 
                // calling  StringUtil.toHexString(). If the data 
                // is being imported from a file that exported 
                // from non-derby source, hex data is expected to be 
                // same format as one written using 
                // StringUtil.toHexString(). StringUtil.fromHexString() 
                // is used to covert the hex data  to byte array. 

                data = StringUtil.fromHexString(
                               hexData, 0, hexData.length());
                // fromHexString() returns null if the hex string 
                // is invalid one. It is invalid if the data string 
                // length is not multiple of 2 or the data string 
                // contains non-hex characters. 
                if (data == null) {
                    throw PublicAPI.wrapStandardException(
                      StandardException.newException(
                      SQLState.IMPORTFILE_HAS_INVALID_HEXSTRING, 
                      hexData));
                }

                blob = new ImportBlob(data);                
            }
        }
        
        wasNull = (blob == null);
        return blob;
	}

    /**
     * Returns Object that contains the column data 
     * from the import file. 
     * @param columnIndex number of the column. starts at 1.
     * @exception SQLException if any error occurs.
     */
	public Object getObject(int columnIndex) throws SQLException
    {
        byte[] bytes = getBytes( columnIndex );

        try {
            Class udtClass = importResultSetMetaData.getUDTClass( columnIndex );
            
            Object obj = readObject( bytes );
            
            //
            // We need to make sure that the user is not trying to import some
            // other object into the target column. This could happen if, for instance,
            // you try to import the exported contents of a table which has the same
            // shape as the target table except that its udt columns are of different type.
            //
            if ( (obj !=null) && (!udtClass.isInstance( obj )) )
            {
                throw new ClassCastException( obj.getClass().getName() + " -> " + udtClass.getName() );
            }
            
            return obj;
        }
        catch (Exception e) { throw importError( e ); }
    }

    /** Read a serializable from a set of bytes. */
    public static Object readObject( byte[] bytes ) throws Exception
    {
        ByteArrayInputStream bais = new ByteArrayInputStream( bytes );
        ObjectInputStream ois = new ObjectInputStream( bais );

        return ois.readObject();
    }

    /** Read an object which was serialized to a string using StringUtil */
    public static Object destringifyObject( String raw ) throws Exception
    {
        byte[] bytes = StringUtil.fromHexString( raw, 0, raw.length());

        return readObject( bytes );
    }


    /**
     * Returns byte array that contains the column data 
     * from the import file. 
     * @param columnIndex number of the column. starts at 1.
     * @exception SQLException if any error occurs.
     */
	public byte[] getBytes(int columnIndex) throws SQLException {
        
        // This method is called to import data into 
        // LONG VARCHAR FOR BIT DATA VARCHAR FOR BIT DATA,  
        // and CHAR FOR BIT DATA  type columns. Data for 
        // these type of columns expected to be in the  
        // main import file in hex format.  

        // convert the binary data in the hex format to a byte array.
        String hexData = nextRow[columnIndex-1];
        // if hex data is null, then column value is SQL NULL
        wasNull = (hexData == null);
        byte[] data = null;
        if (hexData != null) {
            // Derby export calls Resultset.getString() method
            // to write binary data types.  Derby getString() 
            // method return the data in hex format for binary types,
            // by  calling  StringUtil.toHexString(). If the data 
            // is being imported from a file that is exported 
            // from non-derby source, hex data is expected to be 
            // same format as one written using 
            // StringUtil.toHexString(). StringUtil.fromHexString() 
            // is used to covert the hex data  to byte array. 

            data = StringUtil.fromHexString(hexData, 0, hexData.length());
            // fromHexString() returns null if the hex string is invalid one.
            // It is invalid if the data string length is not multiple of 2 
            // or the data string contains non-hex characters. 
            if (data == null) {
                throw PublicAPI.wrapStandardException(
                      StandardException.newException(
                      SQLState.IMPORTFILE_HAS_INVALID_HEXSTRING, 
                      hexData));
            }
        }
        return data;
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
        
        /**
         * Close the stream and wrap exception in a SQLException
         * 
         * @param ex  Exception causing the import error
         * @throws SQLException
         */
        public  SQLException importError(Exception ex) {
            Exception closeException = null;
            if (importReadData != null)
                try {
                    importReadData.closeStream(); 
                } catch (Exception e) {
                    closeException = e;
                }
                SQLException le = LoadError.unexpectedError(ex);
                if (closeException != null)
                    le.setNextException(LoadError.unexpectedError(closeException));
                return le;
        }
}
