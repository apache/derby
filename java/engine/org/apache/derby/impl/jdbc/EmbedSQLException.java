/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.jdbc
   (C) Copyright IBM Corp. 1999, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.impl.jdbc;

import org.apache.derby.iapi.error.StandardException;

import java.sql.SQLException;
import java.io.PrintStream;
import java.io.PrintWriter;

/**
	This class is what gets send over the wire in client/server
    configuration. When running embedded, this has the detailed
    stack trace for exceptions. In case of client/server, server
    has all the stack trace information but client doesn't get
    the stack trace, just the sql exception. The reason for this
    implementation is the stack trace information is more relevant
    on the server side and it also decreases the size of client
    jar file tremendously.
*/
public class EmbedSQLException extends SQLException {

	private Object[] arguments;
	private String messageId;

	/**
		Java exception that caused this exception, can be null.
	*/
    //Because it's transient, it doesn't get sent over to the client
    //side and hence the classes which needs to be included in the
    //client.jar file decreases 5 folds.
	transient protected Throwable javaException;

	/**
	 * Because SQLException does not have settable fields,
	 * the caller of the constructor must do message lookup,
	 * and pass the appropriate values here for message, messageId,
	 * and next exception.
	 */
	EmbedSQLException(String message, String messageId,
		SQLException nextException, int severity, Object[] args) {

		super(message, StandardException.getSQLStateFromIdentifier(messageId), severity);
		this.messageId = messageId;
		arguments = args;
		if (nextException !=null)
			this.setNextException(nextException);
	}

	public EmbedSQLException(String message, String messageId,
		SQLException nextException, int severity, Throwable t, Object[] args) {

		super(message, StandardException.getSQLStateFromIdentifier(messageId), severity);
		this.messageId = messageId;
		arguments = args;
		if (nextException !=null)
			this.setNextException(nextException);
		javaException = t;
	}
    
	public Throwable getJavaException() {
		return javaException;
	}

	public String getMessageId() {
		return messageId;
	}

	public Object[] getArguments() {
		return arguments;
	}

	/**
		Print the stack trace of the wrapped java exception or this
		exception if there is none.

		@see Throwable#printStackTrace
	*/
	public void printStackTrace() {
		Throwable je = getJavaException();
		if (je != null)
			je.printStackTrace();
		else
			super.printStackTrace();
	}
	/**
		Print the stack trace of the wrapped java exception or this
		exception if there is none.

		@see Throwable#printStackTrace
	*/
	public void printStackTrace(PrintStream s) {
		Throwable je = getJavaException();
		if (je != null)
			je.printStackTrace(s);
		else
			super.printStackTrace(s);
	}
	/**
		Print the stack trace of the wrapped java exception or this
		exception if there is none.

		@see Throwable#printStackTrace
	*/
	public void printStackTrace(PrintWriter s) {
		Throwable je = getJavaException();
		if (je != null)
			je.printStackTrace(s);
		else
			super.printStackTrace(s);
	}

	/*
	** Methods of Object
	*/

	/**
		Override Throwables toString() to avoid the class name
		appearing in the message.
	*/
	public String toString() {
		return "SQL Exception: " + getMessage();
	}

	/*
	** Some hack methods for 3.0.1. These will get cleaned up in main
	** with the exception re-work.
	*/
	private transient boolean simpleWrapper;
	public static SQLException wrapStandardException(String message, String messageId, int code, Throwable se) {
		EmbedSQLException csqle = new EmbedSQLException(message, messageId, (SQLException) null, code, se, (se instanceof StandardException) ? ((StandardException)se).getArguments() : null);
		csqle.simpleWrapper = true;
		return csqle;	
	}
	public boolean isSimpleWrapper() {
		return simpleWrapper;
	}
}
