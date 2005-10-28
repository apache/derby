/*

   Derby - Class org.apache.derby.impl.jdbc.EmbedSQLWarning

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

package org.apache.derby.impl.jdbc;

import org.apache.derby.iapi.services.i18n.MessageService;
import org.apache.derby.iapi.error.ExceptionSeverity;

import org.apache.derby.iapi.error.StandardException;

import java.sql.SQLWarning;

/**
	This class understands the message protocol and looks up
	SQLExceptions based on keys, so that the Local JDBC driver's
	messages can be localized.

	REMIND: May want to investigate putting some of this in the protocol
	side, for the errors that any Cloudscape JDBC driver might return.

	The ASSERT mechanism is a wrapper of the basic services,
	to ensure that failed asserts at this level will behave
	well in a JDBC environment.

	@author ames
*/
public class EmbedSQLWarning extends SQLWarning {

	/*
	** instance fields
	*/

	/*
	** Constructor
	*/

	/**
	 * Because SQLWarning does not have settable fields,
	 * the caller of the constructor must do message lookup,
	 * and pass the appropriate values here for message and SQLState,
	 */
	protected EmbedSQLWarning(String message, String sqlstate) {

		super(message, sqlstate, ExceptionSeverity.WARNING_SEVERITY);
	}

	/*
	** Methods of Throwable
	*/


	/*
	** Methods of Object
	*/

	/**
		Override Throwable's toString() to avoid the class name
		appearing in the message. 
	*/
	public String toString() {
		return "SQL Warning: " + getMessage();
	}

	// class implementation
	public static SQLWarning newEmbedSQLWarning(String messageId) {
		return newEmbedSQLWarning(messageId, null);
	}

	/**
		This looks up the message and sqlstate values and generates
		the appropriate exception off of them.
	 */
	public static SQLWarning newEmbedSQLWarning(String messageId,
			Object arg) {
		return new EmbedSQLWarning(
			MessageService.getCompleteMessage(messageId, new Object[] {arg}),
			StandardException.getSQLStateFromIdentifier(messageId));
	}

	/** Generate an SQL Warning from a Standard Exception
	 * @param se Exception to convert to a warning
	 * @return new SQLWarning with message and SQLState of the se
	 */
	public static SQLWarning generateCsSQLWarning(StandardException se) {
		return new EmbedSQLWarning(
					   se.getMessage(), se.getSQLState());
	}

}

