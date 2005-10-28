/*

   Derby - Class org.apache.derby.iapi.db.Factory

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
