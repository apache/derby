/*

   Derby - Class org.apache.derby.impl.load.ExportResultSetForObject

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
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;

//uses the passed connection and table/view name to make the resultset on
//that entity. If the entity to be exported has non-sql types in it, an
//exception will be thrown
class ExportResultSetForObject {

    private Connection con;
    private String selectQuery;
    private ResultSet rs;
    private int columnCount;
    private String columnNames[];
    private String columnTypes[];
    private int columnLengths[];

    private Statement expStmt = null; 
    private String schemaName;
    private String tableName;

	/* set up the connection and table/view name or the select query
	 * to make the result set, whose data is exported. 
	 **/
	public ExportResultSetForObject(Connection con, String schemaName, 
									String tableName, String selectQuery 
									) 
	{
		this.con = con;
		if( selectQuery == null)
		{
			this.schemaName = schemaName;
			this.tableName = tableName;
			
			// delimit schema Name and table Name using quotes because
			// they can be case-sensitive names or SQL reserved words. Export
			// procedures are expected to be called with case-senisitive names. 
			// undelimited names are passed in upper case, because that is
			// the form database stores them. 
			
			this.selectQuery = "select * from " + 
				(schemaName == null ? "\"" + tableName + "\"" : 
				 "\"" + schemaName + "\"" + "." + "\"" + tableName + "\""); 
		}
        else
		{
			this.selectQuery = selectQuery;
		}
	}


    public ResultSet getResultSet() throws SQLException {
        rs = null;
        //execute the select query and keep it's meta data info ready
        expStmt = con.createStatement();
        rs = expStmt.executeQuery(selectQuery);
        getMetaDataInfo();
        return rs;
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
    private void getMetaDataInfo() throws SQLException {
        ResultSetMetaData metaData = rs.getMetaData();
        columnCount                = metaData.getColumnCount();
        int numColumns             = columnCount;
        columnNames                = new String[numColumns];
        columnTypes                = new String[numColumns];
        columnLengths              = new int[numColumns];

        for (int i=0; i<numColumns; i++) {
            int jdbcTypeId = metaData.getColumnType(i+1);
            columnNames[i] = metaData.getColumnName(i+1);
            columnTypes[i] = metaData.getColumnTypeName(i+1);
            if(!ColumnInfo.importExportSupportedType(jdbcTypeId))
            {
                throw LoadError.nonSupportedTypeColumn(
                            columnNames[i], columnTypes[i]); 
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
