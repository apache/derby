/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.load
   (C) Copyright IBM Corp. 1998, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.impl.load;

import java.sql.Connection;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;

//uses the passed connection and table/view name to make the resultset on
//that entity. If the entity to be exported has non-sql types in it, an
//exception will be thrown
class ExportResultSetForObject {

  private Connection con;
  private String entityName;
  private String selectStatement;
  private ResultSet rs;
  private int columnCount;
  private String columnNames[];
  private String columnTypes[];
  private int columnLengths[];

	private Statement expStmt = null; 

	//uses the passed connection and table/view name to make the resultset on
	//that entity.
	public ExportResultSetForObject(Connection con, String schemaName, 
									String tableName, String selectStatement 
									) 
	{
		this.con = con;
		if( selectStatement == null)
			this.entityName = (schemaName == null ? tableName : schemaName + "." + tableName); 
		this.selectStatement = selectStatement;
	}


  public ResultSet getResultSet() throws Exception {
    rs = null;
    String queryString = getQuery();
    //execute select on passed enitity and keep it's meta data info ready
    Statement expStmt = con.createStatement();
    rs = expStmt.executeQuery(queryString);
    getMetaDataInfo();
    return rs;
  }

  public String getQuery(){
	  if(selectStatement != null)
		  return selectStatement;
	  else
	  {
		  selectStatement = "select * from " + entityName;
		  return selectStatement;
	  }
  }

  public int getColumnCount() {
    return columnCount;
  }

  public String[] getColumnDefinition() {
    return columnNames;
  }

  public String[] getColumnTypes() {
    return columnTypes;
  }

  public int[] getColumnLengths() {
    return columnLengths;
  }

  //if the entity to be exported has non-sql types in it, an exception will be thrown
  private void getMetaDataInfo() throws Exception {
    ResultSetMetaData metaData = rs.getMetaData();
    columnCount = metaData.getColumnCount();
	  int numColumns = columnCount;
    columnNames = new String[numColumns];
	columnTypes = new String[numColumns];
    columnLengths = new int[numColumns];
    for (int i=0; i<numColumns; i++) {
	  int jdbcTypeId = metaData.getColumnType(i+1);
	  columnNames[i] = metaData.getColumnName(i+1);
	  columnTypes[i] = metaData.getColumnTypeName(i+1);
	  if(!ColumnInfo.importExportSupportedType(jdbcTypeId))
	  {
		  throw LoadError.nonSupportedTypeColumn(columnNames[i],
												 columnTypes[i]); 
      }
      	 
      columnLengths[i] = metaData.getColumnDisplaySize(i+1);
    }
  }

	public void  close() throws Exception
	{
		if(expStmt !=null)
			expStmt.close();
	}
}





