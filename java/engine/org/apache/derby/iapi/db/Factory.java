/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.db
   (C) Copyright IBM Corp. 1998, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.iapi.db;

import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.conn.ConnectionUtil;
import java.sql.SQLException;

/**
 *  <P>
 *  Callers of these methods must be within the context of a
 *  Cloudscape statement execution otherwise a SQLException will be thrown.
 *  <BR>
 *  There are two basic ways to call these methods.
 *  <OL>
 *  <LI>
 *  Within a SQL statement.
 *  <PRE>
 *		-- checkpoint the database
 *		CALL org.apache.derby.iapi.db.Factory::
 *				getDatabaseOfConnection().checkpoint();
 *  </PRE>
 *  <LI>
 *  In a server-side JDBC method.
 *  <PRE>
 *		import org.apache.derby.iapi.db.*;
 *
 *		...
 *
 *	// checkpoint the database
 *	    Database db = Factory.getDatabaseOfConnection();
 *		db.checkpoint();
 *
 *  </PRE>
 *  </OL>
  This class can only be used within an SQL-J statement, a Java procedure or a server side Java method.
  <p>This class can be accessed using the class alias <code> FACTORY </code> in SQL-J statements.
 * <P>
 * <I>IBM Corp. reserves the right to change, rename, or
 * remove this interface at any time.</I>
 */

public class Factory
{
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1998_2004;


	/**
	<P>
	Returns the Database object associated with the current connection.
		@exception SQLException Not in a connection context.
	**/
	public static org.apache.derby.database.Database getDatabaseOfConnection()
		throws SQLException
	{
		// Get the current language connection context.  This is associated
		// with the current database.
		LanguageConnectionContext lcc = ConnectionUtil.getCurrentLCC();
		return lcc.getDatabase();
	}

	/** 
	 * Get the TriggerExecutionContext for the current connection
	 * of the connection.
	 *
	 * @return the TriggerExecutionContext if called from the context
	 * of a trigger; otherwise, null.

		@exception SQLException Not in a connection or trigger context.
	 */
	public static TriggerExecutionContext getTriggerExecutionContext()
		throws SQLException
	{
		LanguageConnectionContext lcc = ConnectionUtil.getCurrentLCC();
		return lcc.getTriggerExecutionContext();
	}
}
