/*

   Derby - Class org.apache.derby.impl.jdbc.Util

   Copyright 1997, 2004 The Apache Software Foundation or its licensors, as applicable.

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

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.i18n.MessageService;

import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.types.TypeId;

import org.apache.derby.iapi.error.ExceptionSeverity;

import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.reference.MessageId;
import org.apache.derby.iapi.reference.JDBC30Translation;

import java.sql.SQLException;
import java.sql.Types;
import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;

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
//In the past, this class was sent on the wire to the client and because it
//has the message protcol stuff and also the detailed stack trace as one
//of it's member variable, the client.jar files was really big. To get
//around this problem, now we have added EmbedSQLException which is
//just a java sql exception with the stack trace information variable
//transient so it doesn't get transported to the client side and thus
//reducing the size of client.jar The bug for this fix was 1850. The
//p4 number for it will have the details of all the files impacted and
//the actual changes made.
public abstract class Util  {

	/*
	** Methods of Throwable
	*/

	// class implementation

	/**
		This looks up the message and sqlstate values and generates
		the appropriate exception off of them.
	 */

	private static SQLException newEmbedSQLException(String messageId,
			Object[] args, SQLException next, int severity, Throwable t) {
		return new EmbedSQLException(
			MessageService.getCompleteMessage(messageId, args),
			messageId, next, severity, t, args);
	}

	public static SQLException newEmbedSQLException(String messageId,
			Object[] args, int severity) {
		return newEmbedSQLException(messageId, args, (SQLException) null, severity, (Throwable) null);
	}

	private static SQLException newEmbedSQLException(String messageId,
			Object[] args, int severity, Throwable t) {
		return newEmbedSQLException(messageId,args, (SQLException)  null, severity, t);
	}

	private static SQLException newEmbedSQLException(
			String messageId, int severity) {
		return newEmbedSQLException(messageId, (Object[]) null, (SQLException) null, severity, (Throwable) null);
	}

	// class interface


	/**
		Mimic SanityManager.ASSERT in a JDBC-friendly way,
		and providing system cleanup for JDBC failures.
		We need the connection to do cleanup...

		@exception SQLException the exception
	 */
	public static void ASSERT(EmbedConnection conn, boolean mustBeTrue, String msg) throws SQLException {
		if (SanityManager.DEBUG) {
			try {
				SanityManager.ASSERT(mustBeTrue, msg);
			} catch (Throwable t) {
				SQLException se = conn.handleException(t);
				// get around typing constraints.
				// it must be a Util, we wrapped it.
				SanityManager.ASSERT(se instanceof EmbedSQLException);
				throw (EmbedSQLException)se;
			}
		}
	}

	/**
		Mimic SanityManager.THROWASSERT in a JDBC-friendly way,
		and providing system cleanup for JDBC failures.
		We need the connection to do cleanup...
	 */
	static void THROWASSERT(EmbedConnection conn, String msg) throws SQLException {
		if (SanityManager.DEBUG) {
			try {
				SanityManager.THROWASSERT(msg);
			} catch (Throwable t) {
				SQLException se = conn.handleException(t);
				// get around typing constraints.
				// it must be a Util, we wrapped it.
				SanityManager.ASSERT(se instanceof EmbedSQLException);
				throw (EmbedSQLException)se;
			}
		}
	}

	/*
	** There is at least one static method for each message id.
	** Its parameters are specific to its message.
	** These will throw SQLException when the message repository
	** cannot be located.
	** Note that these methods call the static method newEmbedSQLException,
	** they don't directly do a new Util.
	*/

	/* 3 arguments */
	static SQLException newException(String messageID, Object a1,
			Object a2, Object a3) {
		return newEmbedSQLException(messageID, new Object[] {a1, a2, a3},
        		StandardException.getSeverityFromIdentifier(messageID));
	}


	public static SQLException generateCsSQLException(String error) {
		return newEmbedSQLException(error,
        		StandardException.getSeverityFromIdentifier(error));
	}

	public static SQLException generateCsSQLException(String error, Object arg1)     {
		return newEmbedSQLException(error,
			new Object[] {arg1},
                StandardException.getSeverityFromIdentifier(error));
	}

	public static SQLException generateCsSQLException(
                             String error, Object arg1, Object arg2){
		return newEmbedSQLException(error,
			new Object[] {arg1, arg2},
                StandardException.getSeverityFromIdentifier(error));
	}

	public static SQLException generateCsSQLException(
		String error, Object arg1, Object arg2, Object arg3) {

		return newEmbedSQLException(error,
			new Object[] {arg1, arg2, arg3},
                StandardException.getSeverityFromIdentifier(error));
	}


	static SQLException generateCsSQLException(
                    String error, Object arg1, Throwable t) {
		return newEmbedSQLException(error,
			new Object[] {arg1},
                StandardException.getSeverityFromIdentifier(error), t);
	}

	public static SQLException generateCsSQLException(StandardException se) {
		return new EmbedSQLException(
            	se.getMessage(), se.getMessageId(), null, se.getSeverity(), se, se.getArguments());
	}

	public static SQLException noCurrentConnection() {
		return newEmbedSQLException(SQLState.NO_CURRENT_CONNECTION,
        		StandardException.getSeverityFromIdentifier(SQLState.NO_CURRENT_CONNECTION));
	}

	public static SQLException javaException(Throwable t) {
		String name, msg;

		msg = t.getMessage();
		if (msg == null) msg = "";
		name = t.getClass().getName();
		return newEmbedSQLException(SQLState.JAVA_EXCEPTION,
			new Object[] {name, msg}, ExceptionSeverity.NO_APPLICABLE_SEVERITY, t);
	}


	public static SQLException notImplemented() {

		return newEmbedSQLException(SQLState.NOT_IMPLEMENTED,
			new Object[] {MessageService.getTextMessage(MessageId.CONN_NO_DETAILS)},
                StandardException.getSeverityFromIdentifier(SQLState.NOT_IMPLEMENTED));
	}

	public static SQLException notImplemented(String feature) {
		return newEmbedSQLException(SQLState.NOT_IMPLEMENTED,
			new Object[] {feature},
                StandardException.getSeverityFromIdentifier(SQLState.NOT_IMPLEMENTED));
	}

	static SQLException setStreamFailure(IOException e) {
		String msg;

		msg = e.getMessage();
		if (msg == null) 
			msg = e.getClass().getName();
		return newEmbedSQLException(SQLState.SET_STREAM_FAILURE,
			new Object[] {msg},
                StandardException.getSeverityFromIdentifier(SQLState.SET_STREAM_FAILURE));
	}

	static SQLException typeMisMatch(int targetSQLType) {
		return newEmbedSQLException(SQLState.TYPE_MISMATCH,
			new Object[] {typeName(targetSQLType)},
                StandardException.getSeverityFromIdentifier(SQLState.TYPE_MISMATCH));
	}


  public static String typeName(int jdbcType) {
		switch (jdbcType) {
			case Types.BIT 		:  return TypeId.BIT_NAME;
			case JDBC30Translation.SQL_TYPES_BOOLEAN  : return TypeId.BOOLEAN_NAME;
			case Types.TINYINT 	:  return TypeId.TINYINT_NAME;
			case Types.SMALLINT	:  return TypeId.SMALLINT_NAME;
			case Types.INTEGER 	:  return TypeId.INTEGER_NAME;
			case Types.BIGINT 	:  return TypeId.LONGINT_NAME;

			case Types.FLOAT 	:  return TypeId.FLOAT_NAME;
			case Types.REAL 	:  return TypeId.REAL_NAME;
			case Types.DOUBLE 	:  return TypeId.DOUBLE_NAME;

			case Types.NUMERIC 	:  return TypeId.NUMERIC_NAME;
			case Types.DECIMAL	:  return TypeId.DECIMAL_NAME;

			case Types.CHAR		:  return TypeId.CHAR_NAME;
			case Types.VARCHAR 	:  return TypeId.VARCHAR_NAME;
			case Types.LONGVARCHAR 	:  return "LONGVARCHAR";
            case Types.CLOB     :  return TypeId.CLOB_NAME;

			case Types.DATE 		:  return TypeId.DATE_NAME;
			case Types.TIME 		:  return TypeId.TIME_NAME;
			case Types.TIMESTAMP 	:  return TypeId.TIMESTAMP_NAME;

			case Types.BINARY			:  return TypeId.BINARY_NAME;
			case Types.VARBINARY	 	:  return TypeId.VARBINARY_NAME;
			case Types.LONGVARBINARY 	:  return TypeId.LONGVARBINARY_NAME;
            case Types.BLOB             :  return TypeId.BLOB_NAME;

			case Types.OTHER		:  return "OTHER";
			case Types.JAVA_OBJECT	:  return "Types.JAVA_OBJECT";
			default : return String.valueOf(jdbcType);
		}
	}
}
