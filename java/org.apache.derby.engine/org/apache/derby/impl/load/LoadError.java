/*

   Derby - Class org.apache.derby.impl.load.LoadError

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

import java.io.IOException;
import java.sql.SQLException;
import org.apache.derby.shared.common.error.ExceptionSeverity;
import org.apache.derby.shared.common.reference.SQLState;
import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.shared.common.error.PublicAPI;

/**
 * These exceptions are thrown by the import and export modules.
 * 
 *
	@see SQLException
 */
class LoadError {
	
	/**
//IC see: https://issues.apache.org/jira/browse/DERBY-2400
	 Raised if, the Derby database connection is null.
	*/

	static SQLException connectionNull() {
		return PublicAPI.wrapStandardException(
			   StandardException.newException(SQLState.CONNECTION_NULL));
	}

	/**
	   Raised if, there is data found between the stop delimiter and field/record spearator.
	   @param lineNumber Found invalid data on this line number in the data file
	   @param columnNumber Found invalid data for this column number in the data file
	*/
	static SQLException dataAfterStopDelimiter(int lineNumber, int columnNumber) {
		return PublicAPI.wrapStandardException(
			   StandardException.newException(SQLState.DATA_AFTER_STOP_DELIMITER,
//IC see: https://issues.apache.org/jira/browse/DERBY-6856
											  lineNumber, columnNumber));				 	
	}

	/**
	   Raised if, the passed data file can't be found.
	   @param fileName the data file name 
	   @param ex the exception that prevented us from opening the file
	*/
	static SQLException dataFileNotFound(String fileName, Exception ex) {

		return PublicAPI.wrapStandardException(
//IC see: https://issues.apache.org/jira/browse/DERBY-2472
			   StandardException.newException(SQLState.DATA_FILE_NOT_FOUND, ex,
											  fileName));
	}

  
	/**
	   Raised if, null is passed for data file url.
	*/
	static SQLException dataFileNull() {
    return PublicAPI.wrapStandardException(
			   StandardException.newException(SQLState.DATA_FILE_NULL));
	}
        /**
	   Raised if, data file exists.
	*/
	static SQLException dataFileExists(String fileName) {
    return PublicAPI.wrapStandardException(
			   StandardException.newException(SQLState.DATA_FILE_EXISTS, fileName));
	}
	/**
           Raised if, lob file exists.
        */
        static SQLException lobsFileExists(String fileName) {
    return PublicAPI.wrapStandardException(
                           StandardException.newException(SQLState.LOB_DATA_FILE_EXISTS, fileName));
        }
	/**
	   Raised if, the entity (ie table/view) for import/export is missing in the database.
	*/

	static SQLException entityNameMissing() {
    return PublicAPI.wrapStandardException(
		   StandardException.newException(SQLState.ENTITY_NAME_MISSING));

	}


	/**
//IC see: https://issues.apache.org/jira/browse/DERBY-6856
	   Raised if, field and record separators are substring of each other.
	*/
	static SQLException fieldAndRecordSeparatorsSubset() {
		return  PublicAPI.wrapStandardException(
				StandardException.newException(SQLState.FIELD_IS_RECORD_SEPERATOR_SUBSET));
	}

	/**
	   Raised if, no column by given name is found in the resultset while importing.
	   @param columnName the resultset doesn't have this column name
	*/
	static SQLException invalidColumnName(String columnName) {
		return  PublicAPI.wrapStandardException(
				StandardException.newException(SQLState.INVALID_COLUMN_NAME , columnName));

	}


	/**
	   Raised if, no column by given number is found in the resultset while importing.
	   @param numberOfColumns the resultset doesn't have this column number
	*/
	static SQLException invalidColumnNumber(int numberOfColumns) {
		
		return PublicAPI.wrapStandardException(
//IC see: https://issues.apache.org/jira/browse/DERBY-6856
				StandardException.newException(SQLState.INVALID_COLUMN_NUMBER, numberOfColumns ));
	}

	/**
	   Raised if, trying to export/import from an entity which has non supported
	   type columns in it.
	*/
	static SQLException nonSupportedTypeColumn(String columnName, String typeName) {
		return  PublicAPI.wrapStandardException(
				StandardException.newException(SQLState.UNSUPPORTED_COLUMN_TYPE,
											   columnName,
											   typeName));
	}


	/**
	   Raised if, in case of fixed format, don't find the record separator for a row in the data file.
	   @param lineNumber the line number with the missing record separator in the data file
	*/
	static SQLException recordSeparatorMissing(int lineNumber) {

		return  PublicAPI.wrapStandardException(
				StandardException.newException(SQLState.RECORD_SEPERATOR_MISSING,
											   lineNumber));
	}

	/**
	   Raised if, in case of fixed format, reach end of file before reading data for all the columns.
	*/
	static SQLException unexpectedEndOfFile(int lineNumber) {
    return  PublicAPI.wrapStandardException(
			StandardException.newException(SQLState.UNEXPECTED_END_OF_FILE,
//IC see: https://issues.apache.org/jira/browse/DERBY-6856
//IC see: https://issues.apache.org/jira/browse/DERBY-6856
										   lineNumber));
	}

	/**
	   Raised if, got IOException while writing data to the file.
	*/
	static SQLException errorWritingData(IOException ioe) {
		return PublicAPI.wrapStandardException(
//IC see: https://issues.apache.org/jira/browse/DERBY-2858
			StandardException.newException(SQLState.ERROR_WRITING_DATA, ioe));
	}


	/*
	 * Raised if period(.) is used a character delimiter
	 */
	static SQLException periodAsCharDelimiterNotAllowed()
	{
		return PublicAPI.wrapStandardException(
			   StandardException.newException(SQLState.PERIOD_AS_CHAR_DELIMITER_NOT_ALLOWED));
	}

	/*
	 * Raised if same delimiter character is used for more than one delimiter
	 * type . For eg using ';' for both column delimter and character delimter
	 */
	static SQLException delimitersAreNotMutuallyExclusive()
	{
		return PublicAPI.wrapStandardException(
			   StandardException.newException(SQLState.DELIMITERS_ARE_NOT_MUTUALLY_EXCLUSIVE));
	}


	static SQLException tableNotFound(String tableName)
	{
	
		return PublicAPI.wrapStandardException(
			   StandardException.newException(SQLState.TABLE_NOT_FOUND, tableName));
	}

		
	/* Wrapper to throw an unknown excepton duing Import/Export.
	 * Typically this can be some IO error which is not generic error
	 * like the above error messages. 
	 */

	static SQLException unexpectedError(Throwable t )
	{
		if (!(t instanceof SQLException))  
		{
			return PublicAPI.wrapStandardException(StandardException.plainWrapException(t));
		}
		else
			return (SQLException) t;
	}
	


	
}





