/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.db
   (C) Copyright IBM Corp. 2000, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.iapi.db;

import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.conn.ConnectionUtil;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.error.PublicAPI;
import java.sql.SQLException;

/**
 *
 * ConnectionInfo class provides static methods for getting information 
 * related to a JDBC connection.
 * 
 * When called from within the query language,
 * each method returns information about the connection from which it was called.
 * <p>
 * Use the methods of this class only within an SQL-J statement; do not call 
 * them directly. 
 * <p>
 * <i>IBM Corp. reserves the right to change, rename or remove this class or
 * any of the methods in the class at any time.</i>
 */
 
public abstract class ConnectionInfo
{

	/** no requirement for a constructor */
	private ConnectionInfo() {}

	/**
	 * Get the last autoincrement value inserted into the column by 
	 * a statement in this connection.

	 <BR><B> In JDBC 3.0 an application should use the standard methods provided by
	 JDBC 3.0 to obtain generated key values. See java.sql.Statement.getGeneratedKeys().</B>
	 * 
	 * @param 	schemaName		Name of the schema.
	 * @param	tableName		Name of the table.
	 * @param 	columnName		Name of the column.
	 * 
	 * @return  the last value to be inserted into the named autoincrement
	 * column by this connection. Returns null if this connection has never
	 * inserted into this column.
	 *
	 * @exception SQLException if the current connection could not be
	 * 			  established properly.
	 */
	public static Long lastAutoincrementValue(String schemaName, 
											  String tableName,
											  String columnName)
			throws SQLException								  
	{
		// a static method can manipulate lcc?
		LanguageConnectionContext lcc = ConnectionUtil.getCurrentLCC();
		return lcc.lastAutoincrementValue(schemaName, tableName, columnName);
	}	
	
	/**
	 * <B>INTERNAL USE ONLY</B>
	 * (<B>THIS METHOD MAY BE REMOVED IN A FUTURE RELEASE</B>.)
	 * @throws SQLException on error
	 **/
	public static long nextAutoincrementValue(String schemaName,
											  String tableName,
											  String columnName
											  )
	     throws SQLException
	{
		LanguageConnectionContext lcc = ConnectionUtil.getCurrentLCC();
		try
		{ 
			return 
				lcc.nextAutoincrementValue(schemaName, tableName, columnName);
		}
		catch (StandardException se)
		{ 
			throw PublicAPI.wrapStandardException(se);
		}
	}	
}

