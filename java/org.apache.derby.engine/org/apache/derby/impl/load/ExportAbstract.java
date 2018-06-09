/*

   Derby - Class org.apache.derby.impl.load.ExportAbstract

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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Types;
import java.util.Date;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.io.Reader;

import org.apache.derby.iapi.services.io.DynamicByteArrayOutputStream;
import org.apache.derby.iapi.util.StringUtil;

/**
 * 
 * <P>
 */
abstract class ExportAbstract {

  protected ControlInfo controlFileReader;
  protected ExportResultSetForObject exportResultSetForObject;
  protected ExportWriteDataAbstract exportWriteData;
  protected Connection con;
  protected String entityName;  //this can be a plain table name or qualified with schema also
  protected String schemaName;
  protected String selectStatement ;
  protected boolean lobsInExtFile = false;

  //following makes the resultset using select * from entityName
  protected ResultSet resultSetForEntity() throws Exception {
    exportResultSetForObject = new ExportResultSetForObject(con, schemaName,
															entityName, 
															selectStatement);

    ResultSet rs = exportResultSetForObject.getResultSet();
    return rs;
  }

    /** convert resultset data for the current row to string array. 
     * If large objects are being exported to an external file, 
     * then write the lob  data into the external file and store 
     * the lob data location  in the string array for that column.
     * @param rs   resultset that contains the data to export.
     * @param isLargeBinary  boolean array, whose elements will
     *                      be true, if the column type is blob/or 
     *                      other large binary type, otherwise false. 
     * @param isLargeChar   boolean array, whose elements will
     *                      be true, if the column type is clob/ 
     *                      other large char type, otherwise false. 
     * @return A string array of the row data to write to export file.
     * @exception  Exception  if any errors during conversion. 
     */
    private String[] getOneRowAtATime(ResultSet rs, 
                                      boolean[] isLargeBinary, 
                                      boolean[] isLargeChar) 
        throws Exception 
	{

    if (rs.next()){
       int columnCount = exportResultSetForObject.getColumnCount();
	   ResultSetMetaData rsm=rs.getMetaData();
       String[] rowObjects = new String[columnCount];
       for (int colNum = 0; colNum < columnCount; colNum++) {
           if (lobsInExtFile && 
               (isLargeChar[colNum] || isLargeBinary[colNum])) 
           {	
               String LobExtLocation;
               if (isLargeBinary[colNum]) {

                   // get input stream that has the column value as a 
                   // stream of uninterpreted bytes; if the value is SQL NULL, 
                   // the return value  is null
                   InputStream is = rs.getBinaryStream(colNum + 1);
                   LobExtLocation = 
                       exportWriteData.writeBinaryColumnToExternalFile(is);
               } else {
                   // It is clob data, get character stream that has 
                   // the column value. if the value is SQL NULL, the 
                   // return value  is null
                   Reader ir = rs.getCharacterStream(colNum + 1);
                   LobExtLocation  = 
                       exportWriteData.writeCharColumnToExternalFile(ir);
               }
               rowObjects[colNum]= LobExtLocation;

               // when lob data is written to the main export file, binary 
               // data is written in hex format. getString() call on binary 
               // columns returns the data in hex format, no special handling 
               // required. In case of large char tpe like Clob, data 
               // is written to main export file  similar to other char types. 
               
               // TODO : handling of Nulls. 
           }
		   else {
               String columnValue;
               int jdbcColumnNumber = colNum + 1;
               
               if ( rsm.getColumnType( jdbcColumnNumber ) == java.sql.Types.JAVA_OBJECT )
               { columnValue = stringifyObject( rs.getObject( jdbcColumnNumber ) ); }
               else { columnValue = rs.getString( jdbcColumnNumber ); }
               
			   rowObjects[colNum] = columnValue;
           }
       }
       return rowObjects;
    }
    rs.close();
	exportResultSetForObject.close();
    return null;
  }

    // write a Serializable as a string
    public static String stringifyObject( Object udt ) throws Exception
    {
        DynamicByteArrayOutputStream dbaos = new DynamicByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream( dbaos );
        
        oos.writeObject( udt );
        
        byte[] buffer = dbaos.getByteArray();
        int length = dbaos.getUsed();
        
        return StringUtil.toHexString( buffer, 0, length );
    }

  //returns the control file reader corresponding to the control file passed
  protected ControlInfo getControlFileReader(){
	  return controlFileReader; 
  }

  protected abstract ExportWriteDataAbstract getExportWriteData() throws Exception;

  protected void doAllTheWork() throws Exception {

	ResultSet rs = null;
	try {
    	rs = resultSetForEntity();
    	if (rs != null) {
			ResultSetMetaData rsmeta = rs.getMetaData();
			int ncols = rsmeta.getColumnCount();
			boolean[] isNumeric = new boolean[ncols];
			boolean[] isLargeChar = new boolean[ncols];
			boolean[] isLargeBinary = new boolean[ncols];
			for (int i = 0; i < ncols; i++) {
				int ctype = rsmeta.getColumnType(i+1);
				if (ctype == Types.BIGINT || ctype == Types.DECIMAL || ctype == Types.DOUBLE ||
						ctype == Types.FLOAT ||ctype == Types.INTEGER || ctype == Types.NUMERIC ||
						ctype == Types.REAL ||ctype == Types.SMALLINT || ctype == Types.TINYINT)
    				isNumeric[i] = true;
				else 
					isNumeric[i] = false;
					
				if (ctype == Types.CLOB)
					isLargeChar[i] = true;
				else 
					isLargeChar[i]= false;
				
				if (ctype == Types.BLOB) 
					isLargeBinary[i] = true;
				else 
					isLargeBinary[i] = false;
			}


			exportWriteData = getExportWriteData();
			exportWriteData.writeColumnDefinitionOptionally(
						exportResultSetForObject.getColumnDefinition(),
						exportResultSetForObject.getColumnTypes());
			exportWriteData.setColumnLengths(controlFileReader.getColumnWidths());

       		// get one row at a time and write it to the output file
            String[] oneRow = getOneRowAtATime(rs, 
                                               isLargeBinary, 
                                               isLargeChar);
       		while (oneRow != null) {
         		exportWriteData.writeData(oneRow, isNumeric);
                oneRow = getOneRowAtATime(rs, isLargeBinary, isLargeChar);
       		}
		}
	} finally {
		//cleanup work after no more rows
		if (exportWriteData != null)
			exportWriteData.noMoreRows();
		if (rs != null)
			rs.close();
    }
  }
}
