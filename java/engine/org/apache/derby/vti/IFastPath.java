/*

   Derby - Class org.apache.derby.vti.IFastPath

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

import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.error.StandardException;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
	An internal api for VTIs to allow VTI's written
	in terms of the datatype system, e.g. returning rows.
	This allows passing of data from the VTI into the
	query engine without a conversion through a JDBC ResultSet.
*/
public interface IFastPath {

	/**
		Indicates nextRow() has completed its scan.
	*/
	public int SCAN_COMPLETED = -1;
	/**
		Indicates nextRow() found a row..
	*/
	public int GOT_ROW = 0;
	/**
		Indicates nextRow() has completed its scan but executeQuery must be called to
		complete the query.
	*/
	public int NEED_RS = 1;

	/**
		Start a query.
		Returns true if the VTI will start
		out as a fast path query and thus rows will be returned
		by nextRow().
		Returns false if the engine must call the VTI's PreparedStatement.executeQuery()
		method to execute as a regular ResultSet VTI.
	*/
	public boolean executeAsFastPath()
		throws StandardException, SQLException;

	/**
		When operating in fast path mode return the next row into the passed in row parameter.
		Returns GOT_ROW if a valid row is found.
		Returns SCAN_COMPLETED if the scan is complete.
		Returns NEED_RS if the rest of the query must be handled as a regular ResultSet VTI by
		the engine calling the VTI's PreparedStatement.executeQuery()

	*/
	public int nextRow(DataValueDescriptor[] row)
		throws StandardException, SQLException;


	/**
		A call from the VTI execution layer back into the supplied VTI.
		Presents the row just processed as an array of DataValueDescriptors.
		This only called when the VTI is being executed as a regular ResultSet VTI
	*/
	public void currentRow(ResultSet rs, DataValueDescriptor[] row)
		throws StandardException, SQLException;


    /**
		Called once the ResultSet returned by executeQuery() has emptied all of its
		rows (next() has returned false).
     */
    public void rowsDone() throws StandardException, SQLException;
}
