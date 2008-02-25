/*

   Derby - Class org.apache.derby.impl.jdbc.EmbedSQLException

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package org.apache.derby.impl.jdbc;

import org.apache.derby.iapi.error.DerbySQLException;
import java.sql.SQLException;

import org.apache.derby.iapi.error.StandardException;

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
public class EmbedSQLException extends SQLException implements DerbySQLException {

	private transient Object[] arguments;
	private String messageId;

	/**
	 * Because SQLException does not have settable fields,
	 * the caller of the constructor must do message lookup,
	 * and pass the appropriate values here for message, messageId,
	 * and next exception.
	 */
	EmbedSQLException(String message, String messageId,
		SQLException nextException, int severity, Throwable t, Object[] args) {

		super(message, StandardException.getSQLStateFromIdentifier(messageId), severity);
		this.messageId = messageId;
		arguments = args;
		if (nextException !=null)
			this.setNextException(nextException);

		// if no cause has been specified, let nextException be the cause (this
		// improves error reporting since the cause is included in the output
		// from printStackTrace())
		if (t == null) {
			t = nextException;
		}

		if (t != null) {
			initCause(t);
		}
	}

	public String getMessageId() {
		return messageId;
	}

	public Object[] getArguments() {
		return arguments;
	}

	/*
	** Methods of Object
	*/

	/**
		Override Throwables toString() to avoid the class name
		appearing in the message.
	*/
	public String toString() {
        // We use java.sql.SQLException rather than the default toString(),
        // which returns org.apache.derby.impl.jdbc.EmbedSQLException, so
        // that (a) we're not exposing an internal class name and (b) so
        // this is consistent with the network client, where SQLExceptions
        // are vanilla java.sql classes and not our own subclass
		return "java.sql.SQLException: " + getMessage();
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
