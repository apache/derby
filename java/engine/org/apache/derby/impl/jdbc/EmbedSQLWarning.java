/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.jdbc
   (C) Copyright IBM Corp. 1998, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

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
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1998_2004;

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

