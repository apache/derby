/*

   Derby - Class org.apache.derbyTesting.functionTests.util.VTIClasses.ECTResult

   Copyright 2000, 2004 The Apache Software Foundation or its licensors, as applicable.

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

import org.apache.derby.vti.VTITemplate;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Types;

import java.math.BigDecimal;

	/**
	 * Constructs an updatable result set for a table in an external
	 * Cloudscape database and is used by the read-write VTI class
	 * ExternalCloudscapeTable. Cloudscape does not support JDBC
	 * updatable ResultSets,
	 * so this class uses an updatable cursor to mimic the behavior of an
	 * updatable ResultSet.
	 */

public class ECTResult extends VTITemplate {
	private Connection				conn;
	private int						columnCount;
	private boolean[]				insertNulls;
	private Object[]				insertRow;
	private PreparedStatement		insertPS;
	private PreparedStatement		selectPS;
	private ResultSet				selectRS;
	private PreparedStatement		deletePS;
	private String					tableName;
	private String                  cursorName="EIRESULTSETCURSORNAME";

	/**
	 * Constructs an updatable result set for a table in an external
	 * Cloudscape database and is used by the read-write VTI class
	 * ExternalCloudscapeTable. Cloudscape does not support JDBC
	 * updatable ResultSets,
	 * so this class uses an updatable cursor to mimic the behavior of an
	 * updatable ResultSet.
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */

	ECTResult(Connection conn, String tableName)
		throws SQLException {
		

		this.conn = conn;
		this.tableName = tableName;

		// Cook up a select statement that selects all columns, and turn
		// it into an updatable cursor. (Cloudscape does not support
		// updatable ResultSets, so we need an updatable cursor
		// to mimic that behavior.)
		selectPS = conn.prepareStatement("SELECT * FROM " + tableName + 
										 " FOR UPDATE ");
		selectPS.setCursorName(cursorName);
	}

	/**
	 * see java.sql.ResultSet
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
    public ResultSetMetaData getMetaData() throws SQLException {
		return getSelectResultSet().getMetaData();
	}

	
	/**
	 * Close the result set, all its resources, and commit the connection.
	 * Cloudscape calls this method when it is done executing the
	 * SQL statement that references the VTI class. 
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
	public void close() throws SQLException {
		// Release all resources
		if (insertPS != null) {
			insertPS.close();
			insertPS = null;
		}
		if (selectPS != null) {
			selectPS.close();
			selectPS = null;
		}
		if (selectRS != null) {
			selectRS.close();
			selectRS = null;
		}
		if (deletePS != null) {
			deletePS.close();
			deletePS = null;
		}
		if (conn != null) {
			conn.commit();
			
			conn.close();
			conn = null;
		}
		
	}

	/**
	 * Get the next row from the cursor.
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
	public boolean next() throws SQLException {

		return getSelectResultSet().next();

	}

	/**
	 * This method gets the result set for the cursor, and also allocates
	 * the null and row arrays for inserts.
	 *
	 * @exception SQLException
	 */
	private ResultSet getSelectResultSet() throws SQLException {
		if (selectRS == null) {
			selectRS = selectPS.executeQuery();

			ResultSetMetaData rsmd = selectRS.getMetaData();

			columnCount = rsmd.getColumnCount();

			// This array keeps track of which columns should have nulls
			// inserted into them.
			insertNulls = new boolean[columnCount];

			// This array keeps track of non-null values to insert into columns.
			insertRow = new Object[columnCount];
		}

		return selectRS;
	}

	/**
	 * Cloudscape calls this method before inserting a row.
	 * 
	 * 
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
	public void moveToInsertRow() throws SQLException {
		// With Cloudscape, the cursor is always positioned on
		//the insert row, so no need
		// to do anything.
		
		
	}

	/**
	 * Cloudscape calls this method for each column in the row being inserted.
	 * 
	 * 
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
	public void updateObject(int columnIndex, Object x) throws SQLException {
		// Make sure insertNulls and inserRow arrays are intialized
		getSelectResultSet();

		// If we're being asked to insert a null, remember it in the insertNulls
		// array, otherwise remember the value in the insertRow array.
		if (x == null) {
			insertNulls[columnIndex - 1] = true;
		} else {
			insertNulls[columnIndex - 1] = false;
			insertRow[columnIndex - 1] = x;
		}
		
		
	}

	/**
	 * Cloudscape calls this to insert a row (after calling moveToInsertRow for the row
	 * and updateObject for each column in the row).
	 *
	 * 
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
	public void insertRow() throws SQLException {
		if (insertPS == null) {
			// Cook up an insert statement where the values are all ? parameters
			String insertString = "insert into " + tableName + " values (?";
			for (int index = 1; index < columnCount; index++) {
				insertString = insertString + " ,?";
			}
			insertString = insertString + ")";
			insertPS = conn.prepareStatement(insertString);
		}

		// Go through all the columns, and set the ? parameter value to
		// null if we're inserting a null, otherwise set the ? parameter
		// value to the value of the Object the user is trying to insert.
		for (int index = 0; index < insertRow.length; index++) {
			if (insertNulls[index]) {
				insertPS.setNull(index + 1,
   								getMetaData().getColumnType(index + 1));
			} else {
				insertPS.setObject(index + 1, insertRow[index]);
			}
		}

		// Execute the insert statement.
		insertPS.executeUpdate();
		
		
	}

	/**
	 * Cloudscape calls this method for each row it deletes. 
	 *
	 * 
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
	public void deleteRow() throws SQLException {
		if (deletePS == null) {
			// Cook up a delete statement that deletes the current row.
			// Do it now, rather than in constructor, because the cursor
			// must be open before we can prepare this statement.
			deletePS = conn.prepareStatement("delete from " + tableName +
					 				" where current of \"" + cursorName + "\"");
		}

		// Delete the row at the current cursor position.
		deletePS.executeUpdate();
		
		
	}

	// Methods of ResultSet dispatched directly to the select ResultSet.

	/**
	 * 
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
    public boolean wasNull() throws SQLException {
		
        return selectRS.wasNull();
    }

	/**
	 * 
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
    public String getString(int columnIndex) throws SQLException {
		
        return selectRS.getString(columnIndex);
    }

	/**
	 * 
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
    public boolean getBoolean(int columnIndex) throws SQLException {
        return selectRS.getBoolean(columnIndex);
    }

	/**
	 * 
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
    public byte getByte(int columnIndex) throws SQLException {
        return selectRS.getByte(columnIndex);
    }

	/**
	 * 
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
    public short getShort(int columnIndex) throws SQLException {
        return selectRS.getShort(columnIndex);
    }

	/**
	 * 
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
    public int getInt(int columnIndex) throws SQLException {
		
        return selectRS.getInt(columnIndex);
    }

	/**
	 * 
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
    public long getLong(int columnIndex) throws SQLException {
        return selectRS.getLong(columnIndex);
    }

	/**
	 * 
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
   public float getFloat(int columnIndex) throws SQLException {
        return selectRS.getFloat(columnIndex);
    }

	/**
	 * 
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
    public double getDouble(int columnIndex) throws SQLException {
        return selectRS.getDouble(columnIndex);
    }

	/**
	 * 
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        return selectRS.getBigDecimal(columnIndex, scale);
    }

	/**
	 * 
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
    public byte[] getBytes(int columnIndex) throws SQLException {
        return selectRS.getBytes(columnIndex);
    }

	/**
	 * 
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
    public java.sql.Date getDate(int columnIndex) throws SQLException {
		
        return selectRS.getDate(columnIndex);
    }

	/**
	 * 
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
    public java.sql.Time getTime(int columnIndex) throws SQLException {
        return selectRS.getTime(columnIndex);
    }

	/**
	 * 
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
    public java.sql.Timestamp getTimestamp(int columnIndex) throws SQLException {
        return selectRS.getTimestamp(columnIndex);
    }

	/**
	 * 
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
    public java.io.InputStream getAsciiStream(int columnIndex) throws SQLException {
        return selectRS.getAsciiStream(columnIndex);
    }

	/**
	 * 
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
    public java.io.InputStream getUnicodeStream(int columnIndex) throws SQLException {
        return selectRS.getUnicodeStream(columnIndex);
    }

	/**
	 * 
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
    public java.io.InputStream getBinaryStream(int columnIndex) throws SQLException {
        return selectRS.getBinaryStream(columnIndex);
    }

	/**
	 * 
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
    public String getString(String columnName) throws SQLException {
		
        return selectRS.getString(columnName);
    }

	/**
	 * 
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
    public boolean getBoolean(String columnName) throws SQLException {
        return selectRS.getBoolean(columnName);
    }

	/**
	 * 
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
    public byte getByte(String columnName) throws SQLException {
        return selectRS.getByte(columnName);
    }

	/**
	 * 
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
    public short getShort(String columnName) throws SQLException {
        return selectRS.getShort(columnName);
    }

	/**
	 * 
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
    public int getInt(String columnName) throws SQLException {
        return selectRS.getInt(columnName);
    }

	/**
	 * 
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
    public long getLong(String columnName) throws SQLException {
        return selectRS.getLong(columnName);
    }

	/**
	 * 
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
    public float getFloat(String columnName) throws SQLException {
        return selectRS.getFloat(columnName);
    }

	/**
	 * 
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
    public double getDouble(String columnName) throws SQLException {
        return selectRS.getDouble(columnName);
    }

	/**
	 * 
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
    public BigDecimal getBigDecimal(String columnName, int scale) throws SQLException {
        return selectRS.getBigDecimal(columnName, scale);
    }

	/**
	 * 
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
    public byte[] getBytes(String columnName) throws SQLException {
        return selectRS.getBytes(columnName);
    }

	/**
	 * 
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
    public java.sql.Date getDate(String columnName) throws SQLException {
		
        return selectRS.getDate(columnName);
    }

	/**
	 * 
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
    public java.sql.Time getTime(String columnName) throws SQLException {
        return selectRS.getTime(columnName);
    }

	/**
	 * 
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
    public java.sql.Timestamp getTimestamp(String columnName) throws SQLException {
        return selectRS.getTimestamp(columnName);
    }

	/**
	 * 
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
    public java.io.InputStream getAsciiStream(String columnName) throws SQLException {
        return selectRS.getAsciiStream(columnName);
    }

	/**
	 * 
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
    public java.io.InputStream getUnicodeStream(String columnName) throws SQLException {
        return selectRS.getUnicodeStream(columnName);
    }

	/**
	 * 
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
    public java.io.InputStream getBinaryStream(String columnName) throws SQLException {
        return selectRS.getBinaryStream(columnName);
    }

	/**
	 * 
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
    public SQLWarning getWarnings() throws SQLException {
        return selectRS.getWarnings();
    }

	/**
	 * 
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
    public void clearWarnings() throws SQLException {
        selectRS.clearWarnings();
    }

	/**
	 * 
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
    public String getCursorName() throws SQLException {
        return selectRS.getCursorName();
    }

	/**
	 * 
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
    public Object getObject(int columnIndex) throws SQLException {
		
        return selectRS.getObject(columnIndex);
    }

	/**
	 * 
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
    public Object getObject(String columnName) throws SQLException {
		
        return selectRS.getObject(columnName);
    }

	/**
	 * 
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
    public int findColumn(String columnName) throws SQLException {
		
        return selectRS.findColumn(columnName);
    }

	
	//public int getConcurrency() {

		
	//return 0;

	//}
}
