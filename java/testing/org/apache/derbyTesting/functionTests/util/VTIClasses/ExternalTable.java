/*

   Derby - Class org.apache.derbyTesting.functionTests.util.VTIClasses.ExternalTable

   Copyright 2003, 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derbyTesting.functionTests.util.VTIClasses;

import org.apache.derby.vti.UpdatableVTITemplate;

import java.sql.Connection;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.DriverManager;

/**
 * A read-write VTI class; instantiating this VTI within an SQL-J statement
 * gives access to a table in an external Cloudscape database.
 * This class automatically loads the embedded URL;
 * use the embedded form of the database connection URL as the first
 * argument.<p>
 * Here is an example SQL-J statement using this VTI class as an
 * ExternalVirtualTable:<p>
 * <code>INSERT INTO NEW org.apache.derbyTesting.functionTests.util.VTIClasses.ExternalTable(
 * 'jdbc:derby:history', 'HotelBookings')
 * SELECT *  FROM HotelBookings</code><p>
 *
 * NOTE: There is no need to define the getResultSetConcurrency method
 * in this class, since the implementation in UpdatableVTITemplate 
 * already returns ResultSet.CONCUR_UPDATABLE.<p>
 * Use the client
 * form of the URL as the first argument. However, this class does not
 * automatically load the driver. You will have to load the client driver.
 * in an SQL-J statement before instantiating the VTI. 
 *
 */
public class ExternalTable extends UpdatableVTITemplate  {

	private Connection	conn;
	private int		columnCount = 0;
	private String		url;
	private String		tableName;
	private boolean closed = false;

	/**
	 *  Construct the read-write VTI class.
	 *
	 * @param url     URL to the external Cloudscape database.
     * @param tableName Name of the table in that Cloudscape database.
	 */
	public ExternalTable(String url, String tableName) {
		
		this.url = url;
		this.tableName = tableName;
	}

	/**
	 * Provide the ResultSet for the external Table. In this case,
	 * instantiate an ECTResult.
	 *
	 * @see ECTResult
	 *
	 *
	 * @return the result set for the query
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
	public ResultSet executeQuery() throws SQLException {
		if (closed) {
	        throw new SQLException("close - already closed");
		}
		
		return new ECTResult(getAConnection(), tableName);
	}

    /**
     *   Provide the metadata for the query.
     *
     *   @return the result set metadata for the query
	 *
     *   @exception SQLException thrown by JDBC calls
     */
    public ResultSetMetaData getMetaData() throws SQLException {
		if (closed) {
	        throw new SQLException("close - already closed");
		}

		Statement s = null;
		ResultSet rs = null;
        ResultSetMetaData rsmd = null;

		try {
			s = getAConnection().createStatement();

        	rs = s.executeQuery("select * from " + tableName);

        	rsmd = rs.getMetaData();
			columnCount = rsmd.getColumnCount();
		} finally {
			if (rs != null) {
				rs.close();
				rs = null;
			}

			if (s != null) {
				s.close();
				s = null;
			}
		}
		

        return rsmd;
    }

	/**
	 * Close this class. Called by Cloudscape only
	 * at the end compiling a select.
	 * 
	 * @see java.sql.Statement
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
	public void close() throws SQLException {
		if (closed) {
	        throw new SQLException("close - already closed");
		}

		if (conn != null) {
			if (!conn.isClosed())
				conn.close();
		}
		

		closed = true;
	}

	/**
	 * Return the number of columns in the table.
	 *
	 * @exception SQLException	Thrown if there is an error getting the
	 *							metadata.
	 */
	int getColumnCount() throws SQLException {
		if (closed) {
	        throw new SQLException("close - already closed");
		}

		if (columnCount == 0) {
			getMetaData();
		}
		

		return columnCount;
	}

	/**
	 * Get a connection for the user-supplied URL. Get it only once
	 * per use of this VTI.
	 */
	private Connection getAConnection() throws SQLException {
		if (conn == null || conn.isClosed()) {
			conn = DriverManager.getConnection(url);
			conn.setAutoCommit(false);
		}


		return conn;
	}

}
