/*

   Derby - Class org.apache.derby.impl.load.ColumnInfo

   Copyright 2004 The Apache Software Foundation or its licensors, as applicable.

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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.Connection;
import java.sql.ResultSetMetaData;
import java.sql.DatabaseMetaData;
import java.util.*;

/**
 *	
 * This class provides supportto  create casting/conversions required to 
 * perform import. Import VTI  gives all the data in VARCHAR type becuase data
 * in the files is in CHAR format. There is no implicit cast availabile from
 * VARCHAR to some of the types. In cases where explicit casting is allowed, 
 * columns are casted with  explict cast to the type of table column; in case of 
 * double/real explicit casting is also not allowd , scalar fuction DOUBLE is
 * used in those cases.
 *  
 * @author suresht
 */
class ColumnInfo {

	private ArrayList vtiColumnNames ;
	private ArrayList insertColumnNames;
    private ArrayList columnTypes ;
	private int noOfColumns;
	private ArrayList columnPositions;
	private boolean createolumnNames = true;
	private int expectedNumberOfCols ; //number of Columns that are suppose
	                                       // to be in the file to imported  
	private Connection conn;
	private String tableName;
	private String schemaName;

	/**
	 * Initialize the column type and name  information
	 * @param conn  - connection to use for metadata queries
	 * @param sName - table's schema
	 * @param tName - table Name
	 * @param columnList - comma seperared insert statement column list 
	 * @param  vtiColumnIndex - Indexes in the file
	 * @param  vtiColumnPrefix - Prefix to use to generate column names to select from VTI
	 * @exception Exception on error 
	 */
	public ColumnInfo(Connection conn,
					  String sName, 
					  String tName,
					  String insertColumnList, 
					  String vtiColumnIndexes,
					  String vtiColumnPrefix)
		throws SQLException 
	{

		vtiColumnNames = new ArrayList(1);
		insertColumnNames = new ArrayList(1);
		columnTypes = new ArrayList(1);
		noOfColumns = 0;
		this.conn = conn;
		this.schemaName = (sName !=null ? sName.toUpperCase(java.util.Locale.ENGLISH):sName);
		this.tableName =  (tName !=null ? tName.toUpperCase(java.util.Locale.ENGLISH):tName);

		if(insertColumnList!=null)
		{
			//break the comma seperated column list and initialze column info
			//eg: c2 , c1 , c3
			StringTokenizer st = new StringTokenizer(insertColumnList , ",");
			while (st.hasMoreTokens()) 
			{
				String columnName = (st.nextToken()).trim();
				if(!initializeColumnInfo(columnName))
				{
					if(tableExists())
						throw  LoadError.invalidColumnName(columnName);
					else
					{
						String entityName = (schemaName !=null ? 
											 schemaName + "." + tableName :tableName); 
						throw LoadError.tableNotFound(entityName);
					}
				}
			}
		}else
		{
			//All columns in the table
			if(!initializeColumnInfo(null))
			{
				String entityName = (schemaName !=null ? 
									 schemaName + "." + tableName :tableName); 
				throw LoadError.tableNotFound(entityName);
			}
		}
		
				
		//break the comma seperated column indexes for import file give by the user
		//eg: "1, 3, 5, 7"
		if(vtiColumnIndexes !=null)
		{
			
			StringTokenizer st = new StringTokenizer(vtiColumnIndexes, ",");
			while (st.hasMoreTokens()) 
			{
				String columnIndex  = (st.nextToken()).trim();
				vtiColumnNames.add(vtiColumnPrefix + columnIndex);
				int cIndex = (new Integer(columnIndex )).intValue();
				if(cIndex > expectedNumberOfCols )
					expectedNumberOfCols= cIndex ;
			}

		}


		//if column indexes are not specified  ; create names for all collumns requested
		if(vtiColumnNames.size() < 1)
		{
			for(int index = 1 ; index <= noOfColumns; index++)
			{
				vtiColumnNames.add(vtiColumnPrefix + index);
			}
			expectedNumberOfCols = noOfColumns ;
		}
	}


	private boolean initializeColumnInfo(String columnPattern)
		throws SQLException
	{
		DatabaseMetaData dmd = conn.getMetaData();
		ResultSet rs = dmd.getColumns(null, 
									  schemaName,
									  tableName,
									  (columnPattern !=null ? columnPattern.toUpperCase(java.util.Locale.ENGLISH):columnPattern));
		boolean foundTheColumn=false;
		while (rs.next())
		{

			// 4.COLUMN_NAME String => column name
			String columnName = rs.getString(4);

			// 5.DATA_TYPE short => SQL type from java.sql.Types
			short dataType = rs.getShort(5);

			// 6.TYPE_NAME String => Data source dependent type name
			String typeName = rs.getString(6);

			
			// 7.COLUMN_SIZE int => column size. For char or date types
			// this is the maximum number of characters, for numeric or
			// decimal types this is precision.
			int columnSize = rs.getInt(7);

			// 9.DECIMAL_DIGITS int => the number of fractional digits
			int decimalDigits = rs.getInt(9);

			// 10.NUM_PREC_RADIX int => Radix (typically either 10 or 2)
			int numPrecRadix = rs.getInt(10);
			foundTheColumn = true;
			if(importExportSupportedType(dataType))
			{

				insertColumnNames.add(columnName);
				String sqlType = typeName + getTypeOption(typeName , columnSize , columnSize , decimalDigits);
				columnTypes.add(noOfColumns , sqlType);
				noOfColumns++;
			}else
			{
				rs.close();
				throw
					LoadError.nonSupportedTypeColumn(columnName,typeName);
			}

		}

		rs.close();
		return foundTheColumn;
	}


	//return true if the given type is supported by import/export
	public  static final boolean importExportSupportedType(int type){

		return !(type == java.sql.Types.BINARY ||
				 type == java.sql.Types.BIT ||
				 type == java.sql.Types.JAVA_OBJECT ||
				 type == java.sql.Types.OTHER ||
				 type == java.sql.Types.CLOB ||
				 type == java.sql.Types.BLOB); 
	}


	private String getTypeOption(String type , int length , int precision , int scale)
	{

			if ((type.equals("CHAR") ||
				 type.equals("BLOB") ||
				 type.equals("CLOB") ||
				 type.equals("VARCHAR")) && length != 0)
			{
				 return "(" + length + ")";
			}

			if (type.equals("FLOAT")  && precision != 0)
				return  "(" + precision + ")";

			//there are three format of decimal and numeric. Plain decimal, decimal(x)
			//and decimal(x,y). x is precision and y is scale.
			if (type.equals("DECIMAL") ||
				type.equals("NUMERIC")) 
			{
				if ( precision != 0 && scale == 0)
					return "(" + precision + ")";
				else if (precision != 0 && scale != 0)
					return "(" + precision + "," + scale + ")";
				else if(precision == 0 && scale!=0)
					return "(" + scale + ")";
			}

			if ((type.equals("DECIMAL") ||
				 type.equals("NUMERIC")) && scale != 0)
				return "(" + scale + ")";

			//no special type option
			return "";
	}


	/*
	 * Returns a  string of columns with proper casting/conversion
	 * to be used to select from import VTI.
	 */
	public String getColumnNamesWithCasts()
	{
		StringBuffer sb = new StringBuffer();
		boolean first = true;
		int noOfVtiCols =  vtiColumnNames.size();
		for(int index = 0 ; index < noOfColumns && index < noOfVtiCols; index++)
		{
			if(!first)
				sb.append(", ");
			else
				first = false;
			String type = (String) columnTypes.get(index);
			String columnName = (String) vtiColumnNames.get(index);
		   
			if(type.startsWith("SMALLINT") ||
			   type.startsWith("INTEGER") ||
			   type.startsWith("DECIMAL") ||
			   type.startsWith("BIGINT") ||
			   type.startsWith("NUMERIC"))							  
			{
				//these types require explicit casting
				sb.append(" cast" + "(" + columnName + " AS " + type + ") "); 

			}else
			{
				//if it is DOUBLE use scalar DOUBLE function no explicit casting allowed
				if(type.startsWith("DOUBLE"))
				{
					sb.append(" DOUBLE" + "(" + columnName + ") ");

				}else
				{
					//REAL: use DOUBLE function to convert from string and the cast to REAL
					if(type.startsWith("REAL"))
					{
						sb.append("cast" + "(" + 
								  " DOUBLE" + "(" + columnName + ") " + 
								  " AS " +  "REAL" + ") ");
					}else
					{
						//all other types does  not need any special casting
						sb.append(" " + columnName +  " "); 
					}
				}

			}
		}

		//there is no column info available
		if(first)
			return " * ";
		else
			return sb.toString();
	}

	/* returns comma seperated column Names for insert statement
	 * eg: c1, c2 , c3 , c4 
	 */
	public String getInsertColumnNames()
	{
		StringBuffer sb = new StringBuffer();
		boolean first = true;
		for(int index = 0 ; index < noOfColumns; index++)
		{
			if(!first)
				sb.append(", ");
			else
				first = false;
			sb.append(insertColumnNames.get(index));
		}
	
		//there is no column info available
		if(first)
			return null;
		else
			return sb.toString();
	}

	/*
	  Returns number of columns expected to be in  the file from the user input paramters.
	 */
	public int getExpectedNumberOfColumnsInFile()
	{
		return expectedNumberOfCols;
	}

	//Return true if the given table exists in the database
	private boolean tableExists() throws SQLException
	{
		DatabaseMetaData dmd = conn.getMetaData();
		ResultSet rs = dmd.getTables(null, schemaName, tableName, null);
		boolean foundTable = false;
		if(rs.next())
		{
			//found the entry
			foundTable = true;
		}
		
		rs.close();
		return foundTable;
	}

}








