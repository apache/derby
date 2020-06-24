/*

Derby - Class org.apache.derbyTesting.functionTests.util

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

package org.apache.derbyTesting.functionTests.util;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.math.BigDecimal;
import java.lang.reflect.*;

/**
 *  BigDecimalHandler provides wrappers for JDBC API methods which use BigDecimal.
 *  When writing tests which use BigDecimal, the methods in this class can be called
 *  instead of directly calling JDBC methods. This way the same test can be used in JVMs 
 *  like J2ME/CDC/Foundation Profile 1.0, which do not have BigDecimal class, or
 *  JSR169 Profile, which does not support method calls using BigDecimal (such 
 *  as ResultSet.getBigDecimal(..).
 *  
 * 
 *
 */
public class BigDecimalHandler {
	
	public static int representation;
	public static final int STRING_REPRESENTATION = 1;
	public static final int BIGDECIMAL_REPRESENTATION = 2;
	
	static{
		try{
			Class.forName("java.math.BigDecimal");
			representation = BIGDECIMAL_REPRESENTATION;
			// This class will attempt calls to ResultSet.getBigDecimal,
			// which may not be available with jvms that support JSR169,
			// even if BigDecimal itself has been made available (e.g. 
			// supporting J2ME/CDC/Foundation Profile 1.1).
//IC see: https://issues.apache.org/jira/browse/DERBY-2224
//IC see: https://issues.apache.org/jira/browse/DERBY-2225
			Method getbd = ResultSet.class.getMethod("getBigDecimal", new Class[] {int.class});
			representation = BIGDECIMAL_REPRESENTATION;
		}
		catch(ClassNotFoundException e){
			//Used for J2ME/Foundation
			representation = STRING_REPRESENTATION;
		}
		catch(NoSuchMethodException e){
			//Used for J2ME/Foundation
			representation = STRING_REPRESENTATION;
		}

	}
	
	//Type conversions supported by ResultSet getBigDecimal method - JDBC3.0 Table B-6 
	private static final int[] bdConvertibleTypes = 
//IC see: https://issues.apache.org/jira/browse/DERBY-453
	{	java.sql.Types.TINYINT,
		java.sql.Types.SMALLINT,
		java.sql.Types.INTEGER,
		java.sql.Types.BIGINT,
		java.sql.Types.REAL,
		java.sql.Types.FLOAT,
		java.sql.Types.DOUBLE,
		java.sql.Types.DECIMAL,
		java.sql.Types.NUMERIC,
		java.sql.Types.BIT,
		//java.sql.Types.BOOLEAN,	//Not supported in jdk13
		java.sql.Types.CHAR,
		java.sql.Types.VARCHAR,
		java.sql.Types.LONGVARCHAR
	};
	
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
				int columnType= rs.getMetaData().getColumnType(columnIndex);
				if((bigDecimalString != null) && !canConvertToDecimal(columnType))
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
				int columnType= rs.getMetaData().getColumnType(columnIndex);
//IC see: https://issues.apache.org/jira/browse/DERBY-453
//IC see: https://issues.apache.org/jira/browse/DERBY-453
				if((bigDecimalString != null) && !canConvertToDecimal(columnType))
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
	
	/** This method is a wrapper for ResultSet method 
	 * updateBigDecimal(int columnIndex, BigDecimal x)
	 * @param rs ResultSet
	 * @param columnIndex Column Index
	 * @param bdString String to be used in updateXXX method
	 * @throws SQLException
	 */
	public static void updateBigDecimalString(ResultSet rs, int columnIndex, String bdString) throws SQLException{
				
//IC see: https://issues.apache.org/jira/browse/DERBY-398
		switch(representation){
			case BIGDECIMAL_REPRESENTATION:
				BigDecimal bd = (bdString == null) ? null : new BigDecimal(bdString);
				rs.updateBigDecimal(columnIndex, bd);
				break;
			case STRING_REPRESENTATION:
				rs.updateString(columnIndex, bdString);
				break;
			default:	
				new Exception("Failed: Invalid Big Decimal representation").printStackTrace();
		}
	}
	
	/** This method is a wrapper for ResultSet method 
	 * updateBigDecimal(String columnName, BigDecimal x)
	 * @param rs ResultSet
	 * @param columnName Column Name
	 * @param bdString String to be used in updateXXX method
	 * @throws SQLException
	 */
	public static void updateBigDecimalString(ResultSet rs, String columnName,String bdString) throws SQLException{
				
		switch(representation){
			case BIGDECIMAL_REPRESENTATION:
				BigDecimal bd = (bdString == null) ? null : new BigDecimal(bdString);
				rs.updateBigDecimal(columnName, bd);
				break;
			case STRING_REPRESENTATION:
				rs.updateString(columnName, bdString);
				break;
			default:	
				new Exception("Failed: Invalid Big Decimal representation").printStackTrace();
		}
	}

	/** This method is a wrapper for the CallableStatement method getBigDecimal(int parameterIndex).
	 * The wrapper method needs the parameterType as an input since ParameterMetaData is not available in JSR169.
	 * 
	 * @param cs CallableStatement 
	 * @param parameterIndex Parameter Index
	 * @param parameterType Parameter Type
	 * @return String value of getXXX(parameterIndex)method on the CallableStatement
	 * @throws SQLException
	 */
	public static String getBigDecimalString(CallableStatement cs, int parameterIndex, int parameterType) throws SQLException{
//IC see: https://issues.apache.org/jira/browse/DERBY-453
		String bigDecimalString = null;
		
		switch(representation){
			case BIGDECIMAL_REPRESENTATION:
				//Call toString() only for non-null values, else return null
				if(cs.getBigDecimal(parameterIndex) != null)
					bigDecimalString = cs.getBigDecimal(parameterIndex).toString();
				break;
			case STRING_REPRESENTATION:
				bigDecimalString = cs.getString(parameterIndex);
				if((bigDecimalString != null) && !canConvertToDecimal(parameterType))
					throw new SQLException("Invalid data conversion. Method not called.");
				break;
			default:	
				new Exception("Failed: Invalid Big Decimal representation").printStackTrace();
		}
		return bigDecimalString;
	}	

	/** This method is a wrapper for the PreparedStatement method setBigDecimal(int parameterIndex,BigDecimal x)
	 * 
	 * @param ps PreparedStatement 
	 * @param parameterIndex Parameter Index
	 * @param bdString String to be used in setXXX method
	 * @throws SQLException
	 */
	public static void setBigDecimalString(PreparedStatement ps, int parameterIndex, String bdString) throws SQLException{
		
		switch(representation){
			case BIGDECIMAL_REPRESENTATION:
//IC see: https://issues.apache.org/jira/browse/DERBY-398
				BigDecimal bd = (bdString == null) ? null : new BigDecimal(bdString);
				ps.setBigDecimal(parameterIndex, bd);
				break;
			case STRING_REPRESENTATION:
				//setString is used since setBigDecimal is not available in JSR169
				//If bdString cannot be converted to short,int or long, this will throw
				//"Invalid character string format exception" 
				ps.setString(parameterIndex,bdString);
				break;
			default:	
				new Exception("Failed: Invalid Big Decimal representation").printStackTrace();
		}
	}
	
	/** This method is a wrapper for the PreparedStatement method setObject(int parameterIndex, Object x) 
	 * 
	 * @param ps PreparedStatement 
	 * @param parameterIndex Parameter Index
	 * @param objectString String to be used in setObject method
	 * @throws SQLException
	 */
	public static void setObjectString(PreparedStatement ps, int parameterIndex, String objectString) throws SQLException{
		
		switch(representation){
			case BIGDECIMAL_REPRESENTATION:
//IC see: https://issues.apache.org/jira/browse/DERBY-398
				BigDecimal bd = (objectString == null) ? null : new BigDecimal(objectString);
				ps.setObject(parameterIndex,bd);
				break;
			case STRING_REPRESENTATION:
				ps.setObject(parameterIndex,objectString);
				break;
			default:	
				new Exception("Failed: Invalid Big Decimal representation").printStackTrace();
		}
	}	
	
	/** This method checks that the SQL type can be converted to Decimal
	 * 
	 * @param type the type to check
	 * @return true if the SQL type is convertible to DECIMAL, false otherwise.
	 * @throws SQLException
	 */
	protected static boolean canConvertToDecimal(int type) throws SQLException{
		boolean  canConvert = false;
		
//IC see: https://issues.apache.org/jira/browse/DERBY-453
		for (int bdType = 0; bdType < bdConvertibleTypes.length; bdType++){
			if(type == bdConvertibleTypes[bdType]){
				canConvert = true;
				break;
			}
		}
		
		return canConvert;
	}
	
}
