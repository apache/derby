/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.jdbcapi.metadataMultiConn

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


package org.apache.derbyTesting.functionTests.tests.jdbcapi;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSetMetaData;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;

import org.apache.derby.tools.ij;
import org.apache.derby.tools.JDBCDisplayUtil;
import java.io.*;
import java.sql.PreparedStatement;
import java.util.Properties;

public class metadataMultiConn
{

	public static Connection getConnection(String[] args, boolean autoCommit)
		throws Exception
	{
		// use the ij utility to read the property file and
		// make the initial connection.
		ij.getPropertyArg(args);
		Connection conn = ij.startJBMS();
		conn.setAutoCommit(autoCommit);
		return conn;
	}

	public static void main(String[] args)
		throws Exception
	{
		System.out.println("Test metadataMultiConn starting");

		Class.forName("org.apache.derby.jdbc.EmbeddedDriver");

		//Open 1st  connection
		Connection conn1 = getConnection(args, false);
		metadataCalls(conn1);

		Connection conn2= getConnection(args, false);

		metadataCalls(conn2);

		Connection conn3 = getConnection(args, false);
		metadataCalls(conn3);

		conn1.commit();
		conn2.commit();
		checkConsistencyOfAllTables(conn3);

		System.out.println("Test metadataMultiConn finishes.");
	}


	public static void metadataCalls(Connection conn)
		throws Exception
	{
		System.out.println("A new connection is doing metadata calls, but never commit...");

		DatabaseMetaData dmd = conn.getMetaData();
		getTypeInfo(dmd,System.out);
		getTables(dmd,System.out);
		getColumnInfo(dmd, "%",System.out);
		getPrimaryKeys(dmd, "%",System.out);
		getExportedKeys(dmd, "%",System.out);

	}

	public static void getTypeInfo(DatabaseMetaData dmd,PrintStream out)
			throws SQLException
		{
			ResultSet rs = dmd.getTypeInfo();
			out.println("Submitted getTypeInfo request");
			while (rs.next())
			{
				// 1.TYPE_NAME String => Type name
				String typeName = rs.getString(1);

				// 2.DATA_TYPE short => SQL data type from java.sql.Types
				short dataType = rs.getShort(2);

				// 3.PRECISION int => maximum precision
				int precision = rs.getInt(3);

				// 4.LITERAL_PREFIX String => prefix used to quote a literal
				// (may be null)
				String literalPrefix = rs.getString(4);

				// 5.LITERAL_SUFFIX String => suffix used to quote a literal
				// (may be null)
				String literalSuffix = rs.getString(5);

				// 6.CREATE_PARAMS String => parameters used in creating the type
				// (may be null)
				String createParams = rs.getString(6);

				// 7.NULLABLE short => can you use NULL for this type?
	            //   typeNoNulls - does not allow NULL values
	            //   typeNullable - allows NULL values
	            //   typeNullableUnknown - nullability unknown
				short nullable = rs.getShort(7);

				// 8.CASE_SENSITIVE boolean=> is it case sensitive?
				boolean caseSensitive = rs.getBoolean(8);

				// 9.SEARCHABLE short => can you use "WHERE" based on this type:
	            //   typePredNone - No support
	            //   typePredChar - Only supported with WHERE .. LIKE
	            //   typePredBasic - Supported except for WHERE .. LIKE
	            //   typeSearchable - Supported for all WHERE ..
				short searchable = rs.getShort(9);

				// 10.UNSIGNED_ATTRIBUTE boolean => is it unsigned?
				boolean unsignedAttribute = rs.getBoolean(10);

				// 11.FIXED_PREC_SCALE boolean => can it be a money value?
				boolean fixedPrecScale = rs.getBoolean(11);

				// 12.AUTO_INCREMENT boolean => can it be used for an
				// auto-increment value?
				boolean autoIncrement = rs.getBoolean(12);

				// 13.LOCAL_TYPE_NAME String => localized version of type name
				// (may be null)
				String localTypeName = rs.getString(13);

				// 14.MINIMUM_SCALE short => minimum scale supported
				short minimumScale = rs.getShort(14);

				// 15.MAXIMUM_SCALE short => maximum scale supported
				short maximumScale = rs.getShort(15);

				// 16.SQL_DATA_TYPE int => unused

				// 17.SQL_DATETIME_SUB int => unused

				// 18.NUM_PREC_RADIX int => usually 2 or 10

				//out.println(typeName);
			}
			rs.close();
		}

		public static void getTables(DatabaseMetaData dmd,PrintStream out)
			throws SQLException
		{
			String types[] = new String[1];
			types[0] = "TABLE";
			ResultSet rs = dmd.getTables(null, null, null, types);
			while (rs.next())
			{
				// 1.TABLE_CAT String => table catalog (may be null)
				String tableCat = rs.getString(1);

				// 2.TABLE_SCHEM String => table schema (may be null)
				String tableSchem = rs.getString(2);

				// 3.TABLE_NAME String => table name
				String tableName = rs.getString(3);

				// 4.TABLE_TYPE String => table type.
				// Typical types are "TABLE", "VIEW",
				//  "SYSTEM TABLE", "GLOBAL TEMPORARY", "LOCAL TEMPORARY",
				//  "ALIAS", "SYNONYM".
				String tableType = rs.getString(4);

				// 5.REMARKS String => explanatory comment on the table
				String remarks = rs.getString(5);
			}
			rs.close();
		}

		public static void getColumnInfo(DatabaseMetaData dmd, String tablePattern,PrintStream out)
			throws SQLException
		{
			out.println("Getting column info for " + tablePattern);
			ResultSet rs = dmd.getColumns(null, null, tablePattern, "%");
			while (rs.next())
			{
				// 1.TABLE_CAT String => table catalog (may be null)
				String tableCat = rs.getString(1);

				// 2.TABLE_SCHEM String => table schema (may be null)
				String tableSchem = rs.getString(2);

				// 3.TABLE_NAME String => table name
				String tableName = rs.getString(3);

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

				// 8.BUFFER_LENGTH is not used.

				// 9.DECIMAL_DIGITS int => the number of fractional digits
				int decimalDigits = rs.getInt(9);

				// 10.NUM_PREC_RADIX int => Radix (typically either 10 or 2)
				int numPrecRadix = rs.getInt(10);

				// 11.NULLABLE int => is NULL allowed?
			    //	   columnNoNulls - might not allow NULL values
				//	   columnNullable - definitely allows NULL values
				//	   columnNullableUnknown - nullability unknown
				int nullable = rs.getInt(11);

				// 12.REMARKS String => comment describing column (may be null)
				String remarks = rs.getString(12);

				// 13.COLUMN_DEF String => default value (may be null)
				String columnDef = rs.getString(13);

				// 14.SQL_DATA_TYPE int => unused

				// 15.SQL_DATETIME_SUB int => unused

				// 16.CHAR_OCTET_LENGTH int => for char types the maximum
				// number of bytes in the column
				int charOctetLength = rs.getInt(16);

				// 17.ORDINAL_POSITION int => index of column in table
				// (starting at 1)
				//-int ordinalPosition = rs.getInt(17);

				// 18.IS_NULLABLE String => "NO" means column definitely
				// does not allow NULL values; "YES" means the column might
				// allow NULL values. An empty string means nobody knows.
				//-String isNullable = rs.getString(18);

				// let's not print this, for it's so much stuff
				//out.println(tableName + " " + columnName + " " + typeName);
			}
			rs.close();
		}

		public static void getPrimaryKeys(DatabaseMetaData dmd, String tablePattern,PrintStream out)
			throws SQLException
		{
			ResultSet rs = dmd.getPrimaryKeys(null, null, tablePattern);
			while (rs.next())
			{
				// 1.TABLE_CAT String => table catalog (may be null)
				String tableCat = rs.getString(1);

				// 2.TABLE_SCHEM String => table schema (may be null)
				String tableSchem = rs.getString(2);

				// 3.TABLE_NAME String => table name
				String tableName = rs.getString(3);

				// 4.COLUMN_NAME String => column name
				String columnName = rs.getString(4);

				// 5.KEY_SEQ short => sequence number within primary key
				short keySeq = rs.getShort(5);

				// 6.PK_NAME String => primary key name (may be null)
				String pkName = rs.getString(6);
			}
			rs.close();
		}

		public static void getExportedKeys(DatabaseMetaData dmd, String tablePattern,PrintStream out)
			throws SQLException
		{
			ResultSet rs = dmd.getExportedKeys(null, null, tablePattern);
			while (rs.next())
			{
				// 1.PKTABLE_CAT String => primary key table catalog (may be null)
				String pkTableCat = rs.getString(1);

				// 2.PKTABLE_SCHEM String => primary key table schema (may be null)
				String pkTableSchem = rs.getString(2);

				// 3.PKTABLE_NAME String => primary key table name
				String pkTableName = rs.getString(3);

				// 4.PKCOLUMN_NAME String => primary key column name
				String pkColumnName = rs.getString(4);

				// 5.FKTABLE_CAT String => foreign key table catalog
				// (may be null) being exported (may be null)
				String fkTableCat = rs.getString(5);

				// 6.FKTABLE_SCHEM String => foreign key table schema
				// (may be null) being exported (may be null)
				String fkTableSchem = rs.getString(6);

				// 7.FKTABLE_NAME String => foreign key table name being exported
				String fkTableName = rs.getString(7);

				// 8.FKCOLUMN_NAME String => foreign key column name being exported
				String fkColumnName = rs.getString(8);

				// 9.KEY_SEQ short => sequence number within foreign key
				short keySeq = rs.getShort(9);

				// 10.UPDATE_RULE short => What happens to foreign key when
				// primary is updated:
				//	   importedNoAction - do not allow update of primary key if
				//                        it has been imported
				//	   importedKeyCascade - change imported key to agree
				//                          with primary key update
				//	   importedKeySetNull - change imported key to NULL if its
				//                          primary key has been updated
				//	   importedKeySetDefault - change imported key to default
				//                             values if its primary key has
				//                             been updated
				//	   importedKeyRestrict - same as importedKeyNoAction
				//                           (for ODBC 2.x compatibility)
				short updateRule = rs.getShort(10);

				// 11.DELETE_RULE short => What happens to the foreign key
				// when primary is deleted.
				//	   importedKeyNoAction - do not allow delete of primary key
				//                           if it has been imported
				//	   importedKeyCascade - delete rows that import a deleted key
				//	   importedKeySetNull - change imported key to NULL if
				//                          its primary key has been deleted
				//	   importedKeyRestrict - same as importedKeyNoAction
				//                           (for ODBC 2.x compatibility)
				//	   importedKeySetDefault - change imported key to default
				//                             if its primary key has
				//	   been deleted
				short deleteRule = rs.getShort(11);

				// 12.FK_NAME String => foreign key name (may be null)
				String fkName = rs.getString(12);

				// 13.PK_NAME String => primary key name (may be null)
				String pkName = rs.getString(13);

				// 14.DEFERRABILITY short => can the evaluation of foreign key
				// constraints be deferred until commit
				//	   importedKeyInitiallyDeferred - see SQL92 for definition
				//	   importedKeyInitiallyImmediate - see SQL92 for definition
				//	   importedKeyNotDeferrable - see SQL92 for definition
				short deferrability = rs.getShort(14);

			}
			rs.close();
		}

    /**
     * Runs the consistency checker.
     *
     * @param conn    a connection to the database.
	 *
	 * @exception SQLException if there is a database error.
     */

	public static void checkConsistencyOfAllTables(Connection conn) throws SQLException {
	
		//check consistency of all tables in the database
		Statement s = conn.createStatement();
		ResultSet rs = s.executeQuery("SELECT schemaname, tablename, " +
									  "SYSCS_UTIL.SYSCS_CHECK_TABLE(schemaname, tablename) " +
									  "FROM sys.sysschemas s, sys.systables t " +
									  "WHERE s.schemaid = t.schemaid");
		boolean consistent = true;
		boolean allconsistent = true;
		while (rs.next()) {
			consistent = rs.getBoolean(3);
			if (!consistent) {
				allconsistent = false;
				System.out.println(rs.getString(1) + "." + rs.getString(2) + " is not consistent.");
			}
		}
		rs.close();
		if (allconsistent)
			System.out.println("All tables are consistent.");
		s.close();
	}
}

