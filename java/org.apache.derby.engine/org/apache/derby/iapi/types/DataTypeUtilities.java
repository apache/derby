/*

   Derby - Class org.apache.derby.iapi.types.DataTypeUtilities

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package org.apache.derby.iapi.types;

import org.apache.derby.shared.common.reference.JDBC40Translation;

import java.sql.Timestamp;
import java.sql.Types;
import java.sql.ResultSetMetaData;

/**
	A set of static utility methods for data types.
 */
public abstract class DataTypeUtilities  {

    /** Clone a Timestamp because they are mutable */
    public  static  Timestamp   clone( Timestamp original )
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-6202
        return original == null ? null : (Timestamp) original.clone();
    }
    
	/**
		Get the precision of the datatype.
		@param	dtd			data type descriptor
	*/
	public static int getPrecision(DataTypeDescriptor dtd) {
		int typeId = dtd.getTypeId().getJDBCTypeId();

		switch ( typeId )
		{
		case Types.CHAR: // CHAR et alia return their # characters...
		case Types.VARCHAR:
		case Types.LONGVARCHAR:
		case Types.CLOB:
		case Types.BINARY:     	// BINARY types return their # bytes...
		case Types.VARBINARY:
		case Types.LONGVARBINARY:
		case Types.BLOB:
//IC see: https://issues.apache.org/jira/browse/DERBY-2438
        case Types.SQLXML:
				return dtd.getMaximumWidth();
			case Types.SMALLINT:
				return 5;
//IC see: https://issues.apache.org/jira/browse/DERBY-3484
			case Types.BOOLEAN:
				return 1;
		}
    	
		return dtd.getPrecision();
	}

	/**
		Get the precision of the datatype, in decimal digits
		This is used by EmbedResultSetMetaData.
		@param	dtd			data type descriptor
	*/
	public static int getDigitPrecision(DataTypeDescriptor dtd) {
		int typeId = dtd.getTypeId().getJDBCTypeId();

		switch ( typeId )
		{
			case Types.FLOAT:
			case Types.DOUBLE:
				return TypeId.DOUBLE_PRECISION_IN_DIGITS;
			case Types.REAL:
				return TypeId.REAL_PRECISION_IN_DIGITS;
			default: return getPrecision(dtd);
		}

	}


	/**
		Is the data type case sensitive.
		@param	dtd			data type descriptor
	*/
	public static boolean isCaseSensitive(DataTypeDescriptor dtd) {
		int typeId = dtd.getTypeId().getJDBCTypeId();

		return (typeId == Types.CHAR ||
		          typeId == Types.VARCHAR ||
		          typeId == Types.CLOB ||
		          typeId == Types.LONGVARCHAR ||
//IC see: https://issues.apache.org/jira/browse/DERBY-2438
                  typeId == Types.SQLXML);
	}
	/**
		Is the data type nullable.
		@param	dtd			data type descriptor
	*/
	public static int isNullable(DataTypeDescriptor dtd) {
		return dtd.isNullable() ?
				ResultSetMetaData.columnNullable :
				ResultSetMetaData.columnNoNulls;
	}

	/**
		Is the data type signed.
		@param	dtd			data type descriptor
	*/
	public static boolean isSigned(DataTypeDescriptor dtd) {
		int typeId = dtd.getTypeId().getJDBCTypeId();

		return ( typeId == Types.INTEGER ||
		     		typeId == Types.FLOAT ||
		     		typeId == Types.DECIMAL ||
		     		typeId == Types.SMALLINT ||
		     		typeId == Types.BIGINT ||
		     		typeId == Types.TINYINT ||
		     		typeId == Types.NUMERIC ||
		     		typeId == Types.REAL ||
		     		typeId == Types.DOUBLE );
	}

	/**
	  *	Gets the display width of a column of a given type.
	  *
	  *	@param	dtd			data type descriptor
	  *
	  *	@return	associated column display width
	  */
	public	static	int getColumnDisplaySize(DataTypeDescriptor dtd)
	{
		int typeId = dtd.getTypeId().getJDBCTypeId();
		int	storageLength = dtd.getMaximumWidth();
		return DataTypeUtilities.getColumnDisplaySize(typeId, storageLength);
	}

	public	static	int getColumnDisplaySize(int typeId, int storageLength)
	{
		int size;
		switch (typeId)
		{
			case Types.TIMESTAMP:
//IC see: https://issues.apache.org/jira/browse/DERBY-4614
				size = 29;
				break;
			case Types.DATE:
				size = 10;
				break;	
			case Types.TIME:
				size = 8;
				break;
			case Types.INTEGER:
				size = 11;
				break;
			case Types.SMALLINT :
				size = 6;
				break;
			case Types.REAL :
			case Types.FLOAT :
//IC see: https://issues.apache.org/jira/browse/DERBY-3398
				size = 15;
				break;
			case Types.DOUBLE:
				size = 24;
				break;
			case Types.TINYINT :
				size = 15;
				break;

			case Types.BINARY:
			case Types.VARBINARY:
			case Types.LONGVARBINARY:
            case Types.BLOB:
				size =  2*storageLength;
				if (size < 0)
					size = Integer.MAX_VALUE;
                break;

			case Types.BIGINT:
				size = 20;
				break;
			case Types.BIT:
//IC see: https://issues.apache.org/jira/browse/DERBY-3484
			case Types.BOOLEAN:
				// Types.BIT == SQL BOOLEAN, so 5 chars for 'false'
				// In JDBC 3.0, Types.BIT or Types.BOOLEAN = SQL BOOLEAN
				size = 5;
				break;
			default: 
				// MaximumWidth is -1 when it is unknown.
				int w = storageLength;
//IC see: https://issues.apache.org/jira/browse/DERBY-2438
                size = (w > 0 ? w : JDBC40Translation.DEFAULT_COLUMN_DISPLAY_SIZE);
				break;
		}
		return size;
	}

    /**
     * Compute the maximum width (column display width) of a decimal or numeric data value,
     * given its precision and scale.
     *
     * @param precision The precision (number of digits) of the data value.
     * @param scale The number of fractional digits (digits to the right of the decimal point).
     *
     * @return The maximum number of chracters needed to display the value.
     */
    public static int computeMaxWidth( int precision, int scale)
    {
	// There are 3 possible cases with respect to finding the correct max
	// width for DECIMAL type.
	// 1. If scale = 0, only sign should be added to precision.
	// 2. scale=precision, 3 should be added to precision for sign, decimal and an additional char '0'.
	// 3. precision > scale > 0, 2 should be added to precision for sign and decimal.
//IC see: https://issues.apache.org/jira/browse/DERBY-836
	return (scale ==0) ? (precision +1) : ((scale == precision) ? (precision + 3) : (precision + 2));
    }
}

