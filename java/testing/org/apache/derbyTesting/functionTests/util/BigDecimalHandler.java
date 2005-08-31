/*

Derby - Class org.apache.derbyTesting.functionTests.util

Copyright 2005 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derbyTesting.functionTests.util;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 *  BigDecimalHandler provides wrappers for JDBC API methods which use BigDecimal.
 *  When writing tests which use BigDecimal, the methods in this class can be called
 *  instead of directly calling JDBC methods. This way the same test can be used in JVMs 
 *  like J2ME/CDC/Foundation Profile, which do not have BigDecimal class. 
 * 
 *  * @author deepa
 *
 */
public class BigDecimalHandler {
	
	static int representation;
	static final int STRING_REPRESENTATION = 1;
	static final int BIGDECIMAL_REPRESENTATION = 2;
	
	static{
		try{
			Class.forName("java.math.BigDecimal");
			representation = BIGDECIMAL_REPRESENTATION;
		}
		catch(ClassNotFoundException e){
			//Used for J2ME/Foundation
			representation = STRING_REPRESENTATION;
		}
	}
	
	/** This method is a wrapper for the ResultSet method getBigDecimal(int columnIndex).
	 * 
	 * @param rs ResultSet 
	 * @param columnIndex Column Index 
	 * @return String value of getXXX(columnIndex)method on the ResultSet
	 * @throws SQLException
	 */
	public static String getBigDecimalString(ResultSet rs, int columnIndex) throws SQLException{
		String bigDecimalString=null;
		
		switch(representation){
			case BIGDECIMAL_REPRESENTATION:
				//Call toString() only for non-null values, else return null
				if(rs.getBigDecimal(columnIndex) != null)
					bigDecimalString = rs.getBigDecimal(columnIndex).toString();
				break;
			case STRING_REPRESENTATION:
				bigDecimalString = rs.getString(columnIndex);
				if((bigDecimalString != null) && !canConvertToDecimal(rs,columnIndex))
					throw new SQLException("Invalid data conversion. Method not called.");
				break;
			default:	
				new Exception("Failed: Invalid Big Decimal representation").printStackTrace();
		}
		return bigDecimalString;
	}
	
	/** This method is a wrapper for ResultSet method getBigDecimal(String columnName).
	 * 
	 * @param rs ResultSet
	 * @param columnName Column Name
	 * @param columnIndex Coulumn Index
	 * @return String value of getXXX(columnName)method on the ResultSet
	 * @throws SQLException
	 */
	public static String getBigDecimalString(ResultSet rs, String columnName, int columnIndex) throws SQLException{
		String bigDecimalString = null;
				
		switch(representation){
			case BIGDECIMAL_REPRESENTATION:
				//Call toString() only for non-null values, else return null
				if(rs.getBigDecimal(columnName) != null){
					bigDecimalString = rs.getBigDecimal(columnName).toString();
				}
				break;
			case STRING_REPRESENTATION:
				bigDecimalString = rs.getString(columnName);
				if((bigDecimalString != null) && !canConvertToDecimal(rs,columnIndex))
					throw new SQLException("Invalid data conversion. Method not called.");
				break;
			default:	
				new Exception("Failed: Invalid Big Decimal representation").printStackTrace();
		}
		return bigDecimalString;
	}
	
	/** This method is a wrapper for ResultSet method getObject(int columnIndex) 
	 * 
	 * @param rs ResultSet
	 * @param columnIndex ColumnIndex
	 * @return String value of getXXX(columnIndex) method on the ResultSet
	 * @throws SQLException
	 */
	public static String getObjectString(ResultSet rs, int columnIndex) throws SQLException{
		String objectString = null;
		
		switch(representation){
			case BIGDECIMAL_REPRESENTATION:
				//Call toString() only for non-null values, else return null
				if(rs.getObject(columnIndex) != null)
					objectString = rs.getObject(columnIndex).toString();
				break;
			case STRING_REPRESENTATION:
				int columnType= rs.getMetaData().getColumnType(columnIndex);
				if(columnType == java.sql.Types.DECIMAL){
					objectString = rs.getString(columnIndex);
				}	
				else
					//Call toString() only for non-null values, else return null
					if(rs.getObject(columnIndex) != null)
						objectString = rs.getObject(columnIndex).toString();
					break;
			default:	
				new Exception("Failed: Invalid Big Decimal representation").printStackTrace();
		}
		return objectString;
	}	
	
	/** This method is a wrapper for ResultSet method getObject(String columnName)
	 * @param rs ResultSet
	 * @param columnName Column Name
	 * @param columnIndex Column Index
	 * @return String value of getXXX(columnName) method on the ResultSet
	 * @throws SQLException
	 */
	public static String getObjectString(ResultSet rs, String columnName, int columnIndex) throws SQLException{
		String objectString = null;
				
		switch(representation){
			case BIGDECIMAL_REPRESENTATION:
				//Call toString() only for non-null values, else return null
				if(rs.getObject(columnName) != null)
					objectString = rs.getObject(columnName).toString();
				break;
			case STRING_REPRESENTATION:
				int columnType= rs.getMetaData().getColumnType(columnIndex);
				if(columnType == java.sql.Types.DECIMAL){
					objectString = rs.getString(columnName);
				}	
				else
					//Call toString() only for non-null values, else return null					
					if(rs.getObject(columnName) != null)
						objectString = rs.getObject(columnName).toString();
					break;
			default:	
				new Exception("Failed: Invalid Big Decimal representation").printStackTrace();
		}
		return objectString;
	}
	
	/** This method checks that the SQL type can be converted to Decimal
	 * 
	 * @param rs ResultSet
	 * @param columnIndex Column Index
	 * @return true if the SQL type is convertible to DECIMAL, false otherwise.
	 * @throws SQLException
	 */
	protected static boolean canConvertToDecimal(ResultSet rs,int columnIndex) throws SQLException{
		int columnType= rs.getMetaData().getColumnType(columnIndex);
		if(columnType == java.sql.Types.BIGINT || 
		   columnType == java.sql.Types.DECIMAL || 
		   columnType == java.sql.Types.DOUBLE || 
		   columnType == java.sql.Types.FLOAT || 
		   columnType == java.sql.Types.INTEGER || 
		   columnType == java.sql.Types.NUMERIC || 
		   columnType == java.sql.Types.REAL || 
		   columnType == java.sql.Types.SMALLINT || 
		   columnType == java.sql.Types.TINYINT){
			return true;
		}
		return false;
	}
	
}
