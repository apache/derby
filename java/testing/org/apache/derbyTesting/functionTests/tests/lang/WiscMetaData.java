/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.lang.WiscMetaData

   Copyright 1998, 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derbyTesting.functionTests.tests.lang;

import org.apache.derby.vti.VTIMetaDataTemplate;

import java.sql.Types;
import java.sql.SQLException;
import java.sql.ResultSetMetaData;

/**
 * This class gives the metadata for the VTI for loading the Wisconsin
 * benchmark schema.
 */
class WiscMetaData extends VTIMetaDataTemplate {
 
	public int getColumnCount() {
		return 16;
	}

	public int getColumnType(int column) throws SQLException {
		switch (column) {
		  case 1:
		  case 2:
		  case 3:
		  case 4:
		  case 5:
		  case 6:
		  case 7:
		  case 8:
		  case 9:
		  case 10:
		  case 11:
		  case 12:
		  case 13:
			return Types.INTEGER;

		  case 14:
		  case 15:
		  case 16:
			return Types.CHAR;

		  default:
			throw new SQLException("Invalid column number " + column);
		}
	}

	public int isNullable(int column) throws SQLException {
		if (column < 1 || column > 16) {
			throw new SQLException(
					"isNullable: column number " + column + " out of range.");
		}

		return ResultSetMetaData.columnNoNulls;
	}

	public String getColumnName(int column) throws SQLException {
		switch (column) {
		  case 1:
			return "unique1";

		  case 2:
			return "unique2";

		  case 3:
			return "two";

		  case 4:
			return "four";

		  case 5:
			return "ten";

		  case 6:
			return "twenty";

		  case 7:
			return "onePercent";

		  case 8:
			return "tenPercent";

		  case 9:
			return "twentyPercent";

		  case 10:
			return "fiftyPercent";

		  case 11:
			return "unique3";

		  case 12:
			return "evenOnePercent";

		  case 13:
			return "oddOnePercent";

		  case 14:
			return "stringu1";

		  case 15:
			return "stringu2";

		  case 16:
			return "string4";
		}

		throw new SQLException(
				"getColumnName: column number " + column + " out of range.");
	}

	public int getColumnDisplaySize(int column) throws SQLException {
		if (column < 1 || column > 16) {
			throw new SQLException(
					"getColumnDisplaySize: column number " + column + " out of range.");
		}

		/* All columns up to 14 are ints, all columns after 14 are char(52) */
		if (column < 14)
			return 10;
		else
			return 52;
	}
}
