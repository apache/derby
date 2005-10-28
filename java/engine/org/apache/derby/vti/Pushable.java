/*

   Derby - Class org.apache.derby.vti.Pushable

   Copyright 2002, 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derby.vti;

import java.sql.SQLException;

/**
	Support for pushing SQL statement information
	down into a virtual table.

  A read-write virtual tables (one that implements java.sql.PreparedStatement)
  implements this interface to support pushing information into the VTI.

  <BR>
  Read-only VTIs (those that implement java.sql.ResultSet) do not support the Pushable interface.
*/
public interface Pushable {


	/**
		Indicates the columns that must be returned by a read-write VTI's ResultSet.
		This method is called only during the runtime execution of the VTI, after it has been
		constructed and before the executeQuery() method is called.
		At compile time the VTI needs to describe the complete set of columns it can return.
		<BR>
		The column identifiers contained in projectedColumns
		map to the columns described by the VTI's PreparedStatement's
		ResultSetMetaData. The ResultSet returned by
		PreparedStatement.executeQuery() must contain
		these columns in the order given. Column 1 in this
		ResultSet maps the the column of the VTI identified
		by projectedColumns[0], column 2 maps to projectedColumns[1] etc.
		<BR>
		Any additional columns contained in the ResultSet are ignored
		by the database engine. The ResultSetMetaData returned by
		ResultSet.getMetaData() must match the ResultSet.
		<P>
		PreparedStatement's ResultSetMetaData column list {"id", "desc", "price", "tax", "brand"}
		<BR>
		projectedColumns = { 2, 3, 5}
		<BR>
		results in a ResultSet containing at least these 3 columns
		{"desc", "price", "brand"}


		The  JDBC column numbering scheme (1 based) ise used for projectedColumns.


		@exception SQLException Error processing the request.
	*/
	public boolean pushProjection(VTIEnvironment vtiEnvironment, int[] projectedColumns)
		throws SQLException;

}
