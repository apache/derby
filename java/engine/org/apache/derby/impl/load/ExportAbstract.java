/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.load
   (C) Copyright IBM Corp. 1998, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.impl.load;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Types;
import java.util.Date;

/**
 * 
 * <P>
 */
abstract class ExportAbstract {
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1998_2004;

  protected ControlInfo controlFileReader;
  protected ExportResultSetForObject exportResultSetForObject;
  protected ExportWriteDataAbstract exportWriteData;
  protected Connection con;
  protected String entityName;  //this can be a plain table name or qualified with schema also
  protected String schemaName;
  protected String selectStatement ;
  

  //following makes the resultset using select * from entityName
  protected ResultSet resultSetForEntity() throws Exception {
    exportResultSetForObject = new ExportResultSetForObject(con, schemaName,
															entityName, 
															selectStatement);

    ResultSet rs = exportResultSetForObject.getResultSet();
    return rs;
  }

  //convert resultset data to string array
  public String[] getOneRowAtATime(ResultSet rs) throws Exception {
    int columnCount = exportResultSetForObject.getColumnCount();

	ResultSetMetaData rsm=rs.getMetaData();
    if (rs.next()){
       String[] rowObjects = new String[columnCount];
       for (int colNum = 0; colNum < columnCount; colNum++) {
           if (rs.getObject(colNum + 1) != null)
			{
				rowObjects[colNum]=rs.getString(colNum + 1);
			}
       }
       return rowObjects;
    }
    rs.close();
	exportResultSetForObject.close();
    return null;
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
			for (int i = 0; i < ncols; i++) {
				int ctype = rsmeta.getColumnType(i+1);
				if (ctype == Types.BIGINT || ctype == Types.DECIMAL || ctype == Types.DOUBLE ||
						ctype == Types.FLOAT ||ctype == Types.INTEGER || ctype == Types.NUMERIC ||
						ctype == Types.REAL ||ctype == Types.SMALLINT || ctype == Types.TINYINT)
    				isNumeric[i] = true;
				else
					isNumeric[i] = false;
			}
			exportWriteData = getExportWriteData();
			exportWriteData.writeColumnDefinitionOptionally(
						exportResultSetForObject.getColumnDefinition(),
						exportResultSetForObject.getColumnTypes());
			exportWriteData.setColumnLengths(controlFileReader.getColumnWidths());

       		//get one row at a time and write it to the output file
       		String[] oneRow = getOneRowAtATime(rs);
       		while (oneRow != null) {
         		exportWriteData.writeData(oneRow, isNumeric);
         		oneRow = getOneRowAtATime(rs);
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
