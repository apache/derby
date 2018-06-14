/*

   Derby - Class org.apache.derby.catalog.GetProcedureColumns

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

package org.apache.derby.catalog;

import java.sql.Types;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.DatabaseMetaData;

import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.DataTypeUtilities;
import org.apache.derby.iapi.sql.ResultColumnDescriptor;
import org.apache.derby.impl.jdbc.EmbedResultSetMetaData;
import org.apache.derby.catalog.types.RoutineAliasInfo;

/**
    <P>Use of VirtualTableInterface to provide support for
    DatabaseMetaData.getProcedureColumns().
	

    <P>This class is called from a Query constructed in 
    java/org.apache.derby.impl.jdbc/metadata.properties:
    The VTI will return columns 3-14, an extra column to the specification
    METHOD_ID is returned to distinguish between overloaded methods.
    </P>
    
  <OL>
        <LI><B>PROCEDURE_CAT</B> String =&gt; procedure catalog (may be null)
        <LI><B>PROCEDURE_SCHEM</B> String =&gt; procedure schema (may be null)
        <LI><B>PROCEDURE_NAME</B> String =&gt; procedure name
        <LI><B>COLUMN_NAME</B> String =&gt; column/parameter name 
        <LI><B>COLUMN_TYPE</B> Short =&gt; kind of column/parameter:
      <UL>
      <LI> procedureColumnUnknown - nobody knows
      <LI> procedureColumnIn - IN parameter
      <LI> procedureColumnInOut - INOUT parameter
      <LI> procedureColumnOut - OUT parameter
      <LI> procedureColumnReturn - procedure return value
      <LI> procedureColumnResult - result column in ResultSet
      </UL>
  <LI><B>DATA_TYPE</B> int =&gt; SQL type from java.sql.Types
        <LI><B>TYPE_NAME</B> String =&gt; SQL type name, for a UDT type the
  type name is fully qualified
        <LI><B>PRECISION</B> int =&gt; precision
        <LI><B>LENGTH</B> int =&gt; length in bytes of data
        <LI><B>SCALE</B> short =&gt; scale
        <LI><B>RADIX</B> short =&gt; radix
        <LI><B>NULLABLE</B> short =&gt; can it contain NULL?
      <UL>
      <LI> procedureNoNulls - does not allow NULL values
      <LI> procedureNullable - allows NULL values
      <LI> procedureNullableUnknown - nullability unknown
      </UL>
        <LI><B>REMARKS</B> String =&gt; comment describing parameter/column
        <LI><B>METHOD_ID</B> Short =&gt; Derby extra column (overloading)
        <LI><B>PARAMETER_ID</B> Short =&gt; Derby extra column (output order)
  </OL>

*/

public class GetProcedureColumns extends org.apache.derby.vti.VTITemplate 
{
	private int translate(int val) {
		if (!isFunction) { return val; }
		switch (val) {
		case DatabaseMetaData.procedureColumnUnknown:
            return DatabaseMetaData.functionColumnUnknown;
		case DatabaseMetaData.procedureColumnIn:
            return DatabaseMetaData.functionColumnIn;
		case DatabaseMetaData.procedureColumnInOut:
            return DatabaseMetaData.functionColumnInOut;
		case DatabaseMetaData.procedureColumnOut:
            return DatabaseMetaData.functionColumnOut;
		case DatabaseMetaData.procedureColumnReturn:
            return DatabaseMetaData.functionReturn;
		default:
            return DatabaseMetaData.functionColumnUnknown;
		}
    }

	private boolean isProcedure;
	private boolean isFunction;
	private int          rowCount;
	private int          returnedTableColumnCount;
	private TypeDescriptor tableFunctionReturnType;
    
	// state for procedures.
	private RoutineAliasInfo procedure;
	private int paramCursor;
    private short method_count;
    private short param_number;

    private TypeDescriptor sqlType;
    private String columnName;
    private short columnType;
    private final short nullable;

    public ResultSetMetaData getMetaData()
    {        
        return metadata;
    }

    //
    // Instantiates the vti given a class name and methodname.
    // 
    // @exception SQLException  Thrown if there is a SQL error.
    //
    //
    public GetProcedureColumns(AliasInfo aliasInfo, String aliasType) throws SQLException
    {
		int     functionParamCursor = -2;

		// compile time aliasInfo will be null.
		if (aliasInfo != null) {
			isProcedure = aliasType.equals("P");
			isFunction = aliasType.equals("F");
			procedure = (RoutineAliasInfo) aliasInfo;
			method_count = (short) procedure.getParameterCount();
            
			rowCount = procedure.getParameterCount();
			if ( procedure.isTableFunction() ) {
			    tableFunctionReturnType = procedure.getReturnType();
			    returnedTableColumnCount = tableFunctionReturnType.getRowColumnNames().length;
			    rowCount += returnedTableColumnCount;
			    functionParamCursor = -1;
		        }
		}
		if (aliasType == null) { 
			nullable = 0;
			return;
		}

		if (isFunction) {
            nullable = (short) DatabaseMetaData.functionNullable;
			sqlType = procedure.getReturnType();
			columnName = "";  // COLUMN_NAME is VARCHAR NOT NULL
            columnType = (short) DatabaseMetaData.functionReturn;
			paramCursor = functionParamCursor;
			return;
		}
		nullable = (short) DatabaseMetaData.procedureNullable;

		paramCursor = -1;
    }

    public boolean next() throws SQLException {
		if (++paramCursor >= rowCount)
			return false;

		if ( procedure.isTableFunction() && (  paramCursor >= procedure.getParameterCount() ) ) {
			int     idx = paramCursor - procedure.getParameterCount();
            
			sqlType      = tableFunctionReturnType.getRowTypes()[ idx ];
			columnName   = tableFunctionReturnType.getRowColumnNames()[ idx ];
            columnType   = (short) DatabaseMetaData.functionColumnResult;
		}
		else if (paramCursor > -1) {
			sqlType      = procedure.getParameterTypes()[paramCursor];
			columnName   = procedure.getParameterNames()[paramCursor];
			columnType   = 
				(short)translate(procedure.getParameterModes()[paramCursor]);
		}
        
		param_number = (short) paramCursor;

		return true;
	}   

    //
    // Get the value of the specified data type from a column.
    // 
    // @exception SQLException  Thrown if there is a SQL error.
    //
    //
    public String getString(int column) throws SQLException 
    {
        switch (column) 
        {
		case 1: // COLUMN_NAME:
			return columnName;

		case 4: //_TYPE_NAME: 
               return sqlType.getTypeName();
               
		case 10: // REMARKS:
                return null;

            default: 
                return super.getString(column);  // throw exception
        }
    }

    //
    // Get the value of the specified data type from a column.
    // 
    // @exception SQLException  Thrown if there is a SQL error.
    //
    //
    public int getInt(int column) throws SQLException 
    {
        switch (column) 
        {
        case 3: // DATA_TYPE:
            if (sqlType != null) {
                return sqlType.getJDBCTypeId();
            }
            return java.sql.Types.JAVA_OBJECT;

		case 5: // PRECISION:
                if (sqlType != null)
                {
                    int type = sqlType.getJDBCTypeId();
                    if (DataTypeDescriptor.isNumericType(type))
                        return sqlType.getPrecision();
                    else if (type == Types.DATE || type == Types.TIME
                             || type == Types.TIMESTAMP)
                        return DataTypeUtilities.getColumnDisplaySize(type, -1);
                    else
                        return sqlType.getMaximumWidth();
                }

                // No corresponding SQL type
                return 0;

		case 6: // LENGTH (in bytes):
                if (sqlType != null)
                    return sqlType.getMaximumWidthInBytes();

                // No corresponding SQL type
                return 0;
          
            default:
                return super.getInt(column);  // throw exception
        }
    }

    //
    // Get the value of the specified data type from a column.
    // 
    // @exception SQLException  Thrown if there is a SQL error.
    //
    //
    public short getShort(int column) throws SQLException 
    {
        switch (column) 
        {
		case 2: // COLUMN_TYPE:
			return columnType;

		case 7: // SCALE:
                if (sqlType != null)
                    return (short)sqlType.getScale();

                // No corresponding SQL type
                return 0;

		case 8: // RADIX:
                if (sqlType != null)
                {
                    int sqlTypeID = sqlType.getJDBCTypeId();
                    if (sqlTypeID == java.sql.Types.REAL ||
                        sqlTypeID == java.sql.Types.FLOAT ||
                        sqlTypeID == java.sql.Types.DOUBLE)
                    {
                        return 2;
                    }
                    return 10;
                }

                // No corresponding SQL type
                return 0;

		//FIXME
		case 9: // NULLABLE:
			return nullable;

		case 11: // METHOD_ID: 
                return method_count;

		case 12: // PARAMETER_ID: 
                return param_number;

            default:
                return super.getShort(column);  // throw exception
        }
    }

    public void close()
    {
    }

	/*
	** Metadata
	*/
	private static final ResultColumnDescriptor[] columnInfo = {

		EmbedResultSetMetaData.getResultColumnDescriptor("COLUMN_NAME",				 Types.VARCHAR, false, 128),
		EmbedResultSetMetaData.getResultColumnDescriptor("COLUMN_TYPE",				 Types.SMALLINT, false),
		EmbedResultSetMetaData.getResultColumnDescriptor("DATA_TYPE",				 Types.INTEGER, false),
		EmbedResultSetMetaData.getResultColumnDescriptor("TYPE_NAME",				 Types.VARCHAR, false, 22),
		EmbedResultSetMetaData.getResultColumnDescriptor("PRECISION",				 Types.INTEGER, false),
		EmbedResultSetMetaData.getResultColumnDescriptor("LENGTH",					 Types.INTEGER, false),
		EmbedResultSetMetaData.getResultColumnDescriptor("SCALE",					 Types.SMALLINT, false),

		EmbedResultSetMetaData.getResultColumnDescriptor("RADIX",					 Types.SMALLINT, false),
		EmbedResultSetMetaData.getResultColumnDescriptor("NULLABLE",				 Types.SMALLINT, false),
		EmbedResultSetMetaData.getResultColumnDescriptor("REMARKS",					 Types.VARCHAR, true, 22),
		EmbedResultSetMetaData.getResultColumnDescriptor("METHOD_ID",				 Types.SMALLINT, false),
		EmbedResultSetMetaData.getResultColumnDescriptor("PARAMETER_ID",			 Types.SMALLINT, false),
	};

    private static final ResultSetMetaData metadata =
        new EmbedResultSetMetaData(columnInfo);
}
