/*

   Derby - Class org.apache.derby.impl.load.ColumnInfo

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

import org.apache.derby.iapi.util.IdUtil;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Types;
import java.util.*;
import org.apache.derby.iapi.jdbc.EngineConnection;

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
 */
class ColumnInfo {

	private ArrayList<String> vtiColumnNames ;
    private ArrayList<String> insertColumnNames;
    private ArrayList<String> columnTypes ;
    private ArrayList<Integer> jdbcColumnTypes;
	private int noOfColumns;
	private ArrayList columnPositions;
	private boolean createolumnNames = true;
	private int expectedNumberOfCols ; //number of Columns that are suppose
                                       // to be in the file to imported  
	private Connection conn;
	private String tableName;
	private String schemaName;
    private HashMap<String,String> udtClassNames;

	/**
	 * Initialize the column type and name  information
	 * @param conn  - connection to use for metadata queries
	 * @param sName - table's schema
	 * @param tName - table Name
	 * @param insertColumnList - comma seperared insert statement column list 
	 * @param  vtiColumnIndexes - Indexes in the file
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

		vtiColumnNames = new ArrayList<String>(1);
		insertColumnNames = new ArrayList<String>(1);
		columnTypes = new ArrayList<String>(1);
        jdbcColumnTypes = new ArrayList<Integer>(1);
        udtClassNames = new HashMap<String,String>();
		noOfColumns = 0;
		this.conn = conn;

        if (sName == null) {
            // Use the current schema if no schema is specified.
            sName = ((EngineConnection) conn).getCurrentSchemaName();
        }

		this.schemaName = sName;
		this.tableName =  tName;

		if(insertColumnList!=null)
		{
			//break the comma seperated column list and initialze column info
			//eg: C2 , C1 , C3
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
				int cIndex = Integer.parseInt(columnIndex );
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
									  columnPattern);
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
				columnTypes.add(sqlType);
                jdbcColumnTypes.add((int) dataType);
				noOfColumns++;

                if ( dataType == java.sql.Types.JAVA_OBJECT )
                {
                    udtClassNames.put( "COLUMN" +  noOfColumns, getUDTClassName( dmd, typeName ) );
                }
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

    // look up the class name of a UDT
    private String getUDTClassName( DatabaseMetaData dmd, String sqlTypeName )
        throws SQLException
    {
        String className = null;
        
        try {
            // special case for system defined types
            if ( sqlTypeName.charAt( 0 ) != '"' ) { return sqlTypeName; }

            String[] nameParts = IdUtil.parseMultiPartSQLIdentifier( sqlTypeName );

            String schemaName = nameParts[ 0 ];
            String unqualifiedName = nameParts[ 1 ];

            ResultSet rs = dmd.getUDTs( null, schemaName, unqualifiedName, new int[] { java.sql.Types.JAVA_OBJECT } );

            if ( rs.next() )
            {
                className = rs.getString( 4 );
            }
            rs.close();
        }
        catch (Exception e) { throw LoadError.unexpectedError( e ); }

        if ( className == null ) { className = "???"; }
        
        return className;
    }


	//return true if the given type is supported by import/export
	public  static final boolean importExportSupportedType(int type){

		return !(type == java.sql.Types.BIT ||
				 type == java.sql.Types.OTHER ||
                 type == Types.SQLXML );
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

    /**
     * Get the column type names.
     */
    public String getColumnTypeNames()
        throws Exception
    {
        // we use the object serializer logic
        return ExportAbstract.stringifyObject( columnTypes );
    }

    /**
     * Get the class names of udt columns as a string.
     */
    public String getUDTClassNames()
        throws Exception
    {
        // we use the object serializer logic
        return ExportAbstract.stringifyObject( udtClassNames );
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

	/* returns comma seperated column Names delimited by quotes for the insert 
     * statement
	 * eg: "C1", "C2" , "C3" , "C4" 
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
            // Column names can be SQL reserved words, or they can contain
            // spaces and special characters, so it is necessary delimit them
            // for insert to work correctly.
            String name = (String) insertColumnNames.get(index);
            sb.append(IdUtil.normalToDelimited(name));
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


    /*
     * Returns the the expected vti data column types in a String format. 
     * Format : (COLUMN NAME : TYPE [, COLUMN NAME : TYPE]*)
     * eg: COLUMN1:1 (java.sql.Types.CHAR) , COLUMN2: -1(LONGVARCHAR) , 
     * COLUMN3 : 2004 (BLOB)
     */
	public String getExpectedVtiColumnTypesAsString() {

        StringBuffer vtiColumnTypes = new StringBuffer();
        // expected types of data in the import file, based on 
        // the how columns in the data file are  mapped to 
        // the table  columns. 
        boolean first = true;
        for (int i =0 ; i < noOfColumns && i < vtiColumnNames.size(); i++) {
            if (first) 
                first = false;
            else
                vtiColumnTypes.append(",");

            vtiColumnTypes.append(vtiColumnNames.get(i) + ":" + 
                                  jdbcColumnTypes.get(i));
        }   

		if(first) {
            // there is no information about column types.
			return null;
        }
		else
			return vtiColumnTypes.toString();
	}


    /*
     * Get the expected vti data column types. This information was 
     * earlier passed as a string to the vti. This routine extracts the 
     * information from the string.
     * @param columnTypesStr  import data column type information , 
     *                        encoded as string. 
     * @param noOfColumns     number of columns in the import file.
     * 
     * @see getExpectedVtiColumnTypesAsString()
     */
    public static int[] getExpectedVtiColumnTypes(String columnTypesStr, 
                                                  int noOfColumns) 
    {
        // extract the table column types. Break the comma seperated 
        // column types into java.sql.Types int values from the columnTypes 
        // string that got passed to the import VTI.

        //eg: COLUMN1:1 (java.sql.Types.CHAR) , COLUMN2: -1(LONGVARCHAR) , 
        //COLUMN3 : 2004 (BLOB)

        int[] vtiColumnTypes = new int[noOfColumns];

        // expected column type information is only available 
        // for the columns that are being imported from the file.
        // columns type information is not required when 
        // a column in the data file is not one of the 
        // imported column, just assume they are of VARCHAR type. 
        
        for (int i = 0 ; i < noOfColumns ; i++)
            vtiColumnTypes[i] = java.sql.Types.VARCHAR;

        StringTokenizer st = new StringTokenizer(columnTypesStr , ",");

        while (st.hasMoreTokens()) 
        {
            String colTypeInfo = (st.nextToken()).trim();
            int colTypeOffset = colTypeInfo.indexOf(":");

            // column names format is "COLUMN" + columnNumner
            int colIndex = Integer.parseInt(colTypeInfo.substring(6, colTypeOffset));
            int colType = Integer.parseInt(colTypeInfo.substring(colTypeOffset+1));

            // column numbers start with 1. Check if user by mistake has 
            // specified a column number that is large than than the 
            // number of columns exist in the file, if that is the case
            // don't assign the type.
            if (colIndex <=  noOfColumns) 
                vtiColumnTypes[colIndex-1] = colType;
            
        }
        
        return vtiColumnTypes;
    }


    /*
     * Get the expected vti column type names. This information was 
     * passed earlier as a string to the vti. This routine extracts the 
     * information from the string.
     * @param columnTypeNamesString  import data column type information, encoded as string. 
     * @param noOfColumns     number of columns in the import file.
     * 
     * @see getColumnTypeNames()
     */
    public static String[] getExpectedColumnTypeNames
        ( String columnTypeNamesString, int noOfColumns )
        throws Exception
    {
        ArrayList list = (ArrayList)
                ImportAbstract.destringifyObject( columnTypeNamesString );

        String[] retval = new String[ list.size() ];

        for (int i = 0; i < retval.length; i++) {
            retval[i] = (String) list.get(i);
        }

        return retval;
    }

    /*
     * Get the expected classes bound to UDT columns. This information was 
     * passed earlier as a string to the vti. This routine extracts the 
     * information from the string.
     * @param stringVersion The result of calling toString() on the original HashMap<String><String>.
     * @return a HashMap<String><Class> mapping column names to their udt classes
     * 
     * @see initializeColumnInfo()
     */
    public static HashMap getExpectedUDTClasses( String stringVersion )
        throws Exception
    {
        // deserialize the original HashMap<String><String>
        HashMap stringMap = deserializeHashMap( stringVersion );

        if ( stringMap == null ) { return null; }
        
        HashMap<String,Class<?>> retval = new HashMap<String,Class<?>>();
        Iterator entries = stringMap.entrySet().iterator();

        while ( entries.hasNext() )
        {
            Map.Entry entry = (Map.Entry)entries.next();
            String columnName = (String) entry.getKey();
            String className = (String) entry.getValue();

            Class<?> classValue = Class.forName( className );

            retval.put( columnName, classValue );
        }

        return retval;
    }
    
    /*
     * Deserialize a HashMap produced by ExportAbstract.stringifyObject()
     */
    public static HashMap deserializeHashMap( String stringVersion )
        throws Exception
    {
        if ( stringVersion == null ) { return null; }

        HashMap retval = (HashMap) ImportAbstract.destringifyObject( stringVersion );

        return retval;
    }
    
}





