/*

   Derby - Class org.apache.derby.iapi.error.StandardException

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

package org.apache.derby.iapi.error;

import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.impl.jdbc.EmbedSQLException;
import org.apache.derby.iapi.error.ExceptionSeverity;
import org.apache.derby.iapi.services.i18n.MessageService;
import org.apache.derby.iapi.services.sanity.SanityManager;

import java.sql.SQLException;
import java.sql.SQLWarning;

/**
	StandardException is the root of all exceptions that are handled
	in a standard fashion by the database code, mainly in the language code.
	<P>
    This class is abstract to ensure that an implementation only throws
	a specific exception (e.g. TransactionException) which is a sub-class
	<P>
	A method in an iterface in a protocol under com.ibm.db2j.protocol.Database must
	only throw a StandardException (if it needs to throw an exception).
	This indicates that the method can throw an exception and therefore its
	caller must ensure that any resources it allocates will be cleaned up
	in the event of an exception in the StandardException hierarchy.
	<P>
	Implementations of methods that throw StandardException can have throws
	clause that are more specific than StandardException.
*/

public class StandardException extends Exception 
{
	public static final int REPORT_DEFAULT = 0;
	public static final int REPORT_NEVER = 1;
	public static final int REPORT_ALWAYS = 2;

	/*
	 * Exception State
	 */
	private Throwable nestedException;
	private Object[] arguments;
	private int severity;
	private String textMessage;
	private String sqlState;
	private int report;

	/*
	** End of constructors
	*/
	
	protected StandardException(String messageID)
	{
		this(messageID, (Throwable) null, (Object[]) null);

	}

	protected StandardException(String messageID, Object[] args)
	{
		this(messageID, (Throwable) null, args);
	}

	protected StandardException(String messageID, Throwable t, Object[] args)
	{
		super(messageID);

		this.severity = getSeverityFromIdentifier(messageID);
		this.sqlState = getSQLStateFromIdentifier(messageID);
		this.nestedException = t;
		this.arguments = args;

		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(messageID != null,
					"StandardException with no messageID");
		}
	}

	/**
	 * This constructor is used when we already have the
	 * message text.
	 * 
	 * @param sqlState the sql state of the message
	 * @param text the text of the message
	 */
	private StandardException(String sqlState, String text)
	{
		this(sqlState);
		textMessage = text;
	}

	/*
	** End of constructors
	*/
	/**
	 * Sets the arguments for this exception.
	 */
	private final void setArguments(Object[] arguments)
	{
		this.arguments = arguments;
	}

	/**
	 * Returns the arguments for this exception,
	 * if there are any.
	 */
	public final Object[] getArguments()
	{
		return arguments;
	}

	/**
	 * Sets the nested exception for this exception.
	 */
	public final void setNestedException(Throwable nestedException)
	{
		this.nestedException = nestedException;
	}

	/**
	 * Returns the nested exception for this exception,
	 * if there is one.
	 */
	public final Throwable getNestedException()
	{
		return nestedException;
	}

	/**
		Yes, report me. Errors that need this method to return
		false are in the minority.
	*/
	public final int report() {
		return report;
	}

	/**
		Set my report type.
	*/
	public final void setReport(int report) {
		this.report = report;
	}

	public final void setSeverity(int severity) {
		this.severity = severity;
	}


	public final int getSeverity() {
		return severity;
	}

	public final int getErrorCode() {
		return severity;
	}

	/**
		Return the 5 character SQL State.
		If you need teh identifier that was used to create the
		message, then use getMessageId(). getMessageId() will return the
		string that corresponds to the field in org.apache.derby.iapi.reference.SQLState.
	*/
	public final String getSQLState()
	{
		return sqlState;
	}

	/**
		Convert a message identifer from org.apache.derby.iapi.reference.SQLState to
		a SQLState five character string.
	 *	@param messageID - the sql state id of the message from cloudscape
	 *	@return String 	 - the 5 character code of the SQLState ID to returned to the user 
	*/
	public static String getSQLStateFromIdentifier(String messageID) {

		if (messageID.length() == 5)
			return messageID;
		return messageID.substring(0, 5);
	}

	/**
		Get the severity given a message identifier from org.apache.derby.iapi.reference.SQLState.
	*/
	public static int getSeverityFromIdentifier(String messageID) {

		int lseverity = ExceptionSeverity.NO_APPLICABLE_SEVERITY;

		switch (messageID.length()) {
		case 5:
			switch (messageID.charAt(0)) {
			case '0':
				switch (messageID.charAt(1)) {
				case '1':
					lseverity = ExceptionSeverity.WARNING_SEVERITY;
					break;
				case 'A':
				case '7':
					lseverity = ExceptionSeverity.STATEMENT_SEVERITY;
					break;
				case '8':
					lseverity = ExceptionSeverity.SESSION_SEVERITY;
					break;
				}
				break;	
			case '2':
			case '3':
				lseverity = ExceptionSeverity.STATEMENT_SEVERITY;
				break;
			case '4':
				switch (messageID.charAt(1)) {
				case '0':
					lseverity = ExceptionSeverity.TRANSACTION_SEVERITY;
					break;
				case '2':
					lseverity = ExceptionSeverity.STATEMENT_SEVERITY;
					break;
				}
				break;	
			}
			break;

		default:
			switch (messageID.charAt(6)) {
			case 'M':
				lseverity = ExceptionSeverity.SYSTEM_SEVERITY;
				break;
			case 'D':
				lseverity = ExceptionSeverity.DATABASE_SEVERITY;
				break;
			case 'C':
				lseverity = ExceptionSeverity.SESSION_SEVERITY;
				break;
			case 'T':
				lseverity = ExceptionSeverity.TRANSACTION_SEVERITY;
				break;
			case 'S':
				lseverity = ExceptionSeverity.STATEMENT_SEVERITY;
				break;
			case 'U':
				lseverity = ExceptionSeverity.NO_APPLICABLE_SEVERITY;
				break;
			}
			break;
		}

		return lseverity;
	}

	/*
	** Set of static methods to obtain exceptions.
	**
	** Possible parameters:
	** String sqlState - SQL State
	** int severity - Severity of message
	** Throwable t - exception to wrap
	** Object aN - argument to error message
	**
	** Calls that can be made after the exception has been created.
	**
	** setExceptionCategory()
	** setReport()
	*/

	/* specific exceptions */

	public	static	StandardException	normalClose()
	{
		StandardException	se = newException( SQLState.NORMAL_CLOSE );
		se.report = REPORT_NEVER;
		return se;
	}

	public	static	StandardException	errorClose( Throwable t )
	{
		StandardException	se = newException( SQLState.ERROR_CLOSE, t );
		se.report = REPORT_NEVER;
		return se;
	}

	/* 0 arguments */

	public static StandardException newException(String messageID) {
		return new StandardException(messageID);
	}
	public static StandardException newException(String messageID, Throwable t) {
		return new StandardException(messageID, t, (Object[]) null);
	}

	/* 1 argument */

	public static StandardException newException(String messageID, Object a1) {
		Object[] oa = new Object[] {a1};
		return new StandardException(messageID, oa);
	}
	public static StandardException newException(String messageID, Throwable t, Object a1) {
		Object[] oa = new Object[] {a1};
		return new StandardException(messageID, t, oa);
	}

	/* 2 arguments */

	public static StandardException newException(String messageID, Object a1, Object a2) {
		Object[] oa = new Object[] {a1, a2};
		return new StandardException(messageID, oa);
	}
	public static StandardException newException(String messageID, Throwable t, Object a1, Object a2) {
		Object[] oa = new Object[] {a1, a2};
		return new StandardException(messageID, t, oa);
	}

	/* 3 arguments */

	public static StandardException newException(String messageID, Object a1, Object a2, Object a3) {
		Object[] oa = new Object[] {a1, a2, a3};
		return new StandardException(messageID, oa);
	}
	public static StandardException newException(String messageID, Throwable t, Object a1, Object a2, Object a3) {
		Object[] oa = new Object[] {a1, a2, a3};
		return new StandardException(messageID, t, oa);
	}

	/* 4 arguments */

	public static StandardException newException(String messageID, Object a1, Object a2, Object a3, Object a4) {
		Object[] oa = new Object[] {a1, a2, a3, a4};
		return new StandardException(messageID, oa);
	}
	public static StandardException newException(String messageID, Throwable t, Object a1, Object a2, Object a3, Object a4) {
		Object[] oa = new Object[] {a1, a2, a3, a4};
		return new StandardException(messageID, t, oa);
	}
 
	/* 5 arguments */
	public static StandardException newException(String messageID, Object a1, Object a2, Object a3, Object a4, Object a5) {
		Object[] oa = new Object[] {a1, a2, a3, a4, a5};
		return new StandardException(messageID, oa);
	}
	public static StandardException newException(String messageID, Throwable t, Object a1, Object a2, Object a3, Object a4, Object a5) {
		Object[] oa = new Object[] {a1, a2, a3, a4, a5};
		return new StandardException(messageID, t, oa);
	}

	/* 6 arguments */
	public static StandardException newException(String messageID, Object a1, Object a2, Object a3, Object a4, Object a5, Object a6) {
		Object[] oa = new Object[] {a1, a2, a3, a4, a5, a6};
		return new StandardException(messageID, oa);
	}
	public static StandardException newException(String messageID, Throwable t, Object a1, Object a2, Object a3, Object a4, Object a5, Object a6) {
		Object[] oa = new Object[] {a1, a2, a3, a4, a5, a6};
		return new StandardException(messageID, t, oa);
	}

	/* 7 arguments */
	public static StandardException newException(String messageID, Object a1, Object a2, Object a3, Object a4, Object a5, Object a6, Object a7) {
		Object[] oa = new Object[] {a1, a2, a3, a4, a5, a6, a7};
		return new StandardException(messageID, oa);
	}
	public static StandardException newException(String messageID, Throwable t, Object a1, Object a2, Object a3, Object a4, Object a5, Object a6, Object a7) {
		Object[] oa = new Object[] {a1, a2, a3, a4, a5, a6, a7};
		return new StandardException(messageID, t, oa);
	}

	/* 8 arguments */
	public static StandardException newException(String messageID, Object a1, Object a2, Object a3, Object a4, Object a5, Object a6, Object a7, Object a8) {
		Object[] oa = new Object[] {a1, a2, a3, a4, a5, a6, a7, a8};
		return new StandardException(messageID, oa);
	}
	public static StandardException newException(String messageID, Throwable t, Object a1, Object a2, Object a3, Object a4, Object a5, Object a6, Object a7, Object a8) {
		Object[] oa = new Object[] {a1, a2, a3, a4, a5, a6, a7, a8};
		return new StandardException(messageID, t, oa);
	}

    /**
     * Creates a new StandardException using message text that has already been localized.
     *
     * @param MessageID The SQLState and severity are derived from the ID. However the text message is not.
     * @param t The Throwable that caused this exception, null if this exception was not caused by another Throwable.
     * @param localizedMessage The message associated with this exception.
     *        <b>It is the caller's responsibility to ensure that this message is properly localized.</b>
     *
     * @see org.apache.derby.iapi.tools.i18n.LocalizedResource
     */
    public static StandardException newPreLocalizedException( String MessageID,
                                                              Throwable t,
                                                              String localizedMessage)
    {
        StandardException se = new StandardException( MessageID, localizedMessage);
        if( t != null)
            se.nestedException = t;
        return se;
    }

	public static StandardException unexpectedUserException(Throwable t)
	{
		/*
		** If we have a SQLException that isn't a Util
		** (i.e. it didn't come from cloudscape), then we check
		** to see if it is a valid user defined exception range 
		** (38001-38XXX).  If so, then we convert it into a 
		** StandardException without further ado.
		*/ 
		if ((t instanceof SQLException) &&
		    !(t instanceof EmbedSQLException)) 
		{
			SQLException sqlex  = (SQLException)t;
			String state = sqlex.getSQLState();
			if ((state != null) && 
				(state.length() == 5) &&
				state.startsWith("38") &&
				!state.equals("38000"))
			{
				StandardException se = new StandardException(state, sqlex.getMessage());
				if (sqlex.getNextException() != null)		
				{	
					se.setNestedException(sqlex.getNextException());
				}
				return se;
			}
		}

		// Look for simple wrappers for 3.0.1 - will be cleaned up in main
		if (t instanceof EmbedSQLException) {
			EmbedSQLException csqle = (EmbedSQLException) t;
			if (csqle.isSimpleWrapper()) {
				Throwable wrapped = csqle.getJavaException();
				if (wrapped instanceof StandardException)
					return (StandardException) wrapped;
			}
		}


		// no need to wrap a StandardException
		if (t instanceof StandardException) 
		{
			return (StandardException) t;
		}
		else
		{
			/*
			** 
			** The exception at this point could be a:
			**
			**    standard java exception, e.g. NullPointerException
			**    SQL Exception - from some server-side JDBC
			**    3rd party exception - from some application
			**    some cloudscape exception that is not a standard exception.
			**    
			**    
			** We don't want to call t.toString() here, because the JVM is
			** inconsistent about whether it includes a detail message
			** with some exceptions (esp. NullPointerException).  In those
			** cases where there is a detail message, t.toString() puts in
			** a colon character, even when the detail message is blank.
			** So, we do our own string formatting here, including the colon
			** only when there is a non-blank message.
			**
			** The above is because our test canons contain the text of
			** error messages.
			**
			** In addition we don't want to place the class name in an
			** exception when the class is from cloudscape because
			** the class name changes in obfuscated builds. Thus for
			** exceptions that are in a package below com.ibm.db2j
			** we use toString(). If this returns an empty or null
			** then we use the class name to make tracking the problem
			** down easier, though the lack of a message should be seen
			** as a bug.
			*/
			String	detailMessage;
			boolean cloudscapeException = false;

			if (t instanceof EmbedSQLException) {
				detailMessage = ((EmbedSQLException) t).toString();
				cloudscapeException = true;
			}
			else {
				detailMessage = t.getMessage();
			}

			if (detailMessage == null)
			{
				detailMessage = "";
			} else {
				detailMessage = detailMessage.trim();
			}

			// if no message, use the class name
			if (detailMessage.length() == 0) {
				detailMessage = t.getClass().getName();
			}
			else {

				if (!cloudscapeException) {
					detailMessage = t.getClass().getName() + ": " + detailMessage;
				}
			}

			StandardException se =
				newException(SQLState.LANG_UNEXPECTED_USER_EXCEPTION, t, detailMessage);
			return se;
		}
	}

	/**
		Similar to unexpectedUserException but makes no assumtion about
		when the execption is being called. The error is wrapped as simply
		as possible.
	*/

	public static StandardException plainWrapException(Throwable t) {

		if (t instanceof StandardException)
			return (StandardException) t;

		if (t instanceof SQLException) {

			SQLException sqle = (SQLException) t;

			String sqlState = sqle.getSQLState();
			if (sqlState != null) {

				StandardException se = new StandardException(sqlState, "(" + sqle.getErrorCode()  + ") " + sqle.getMessage());
				sqle = sqle.getNextException();
				if (sqle != null)
					se.setNestedException(plainWrapException(sqle));
				return se;
			}
		}

		String	detailMessage = t.getMessage();

		if (detailMessage == null)
		{
			detailMessage = "";
		} else {
			detailMessage = detailMessage.trim();
		}
		
		StandardException se =
				newException(SQLState.JAVA_EXCEPTION, t, detailMessage, t.getClass().getName());
		return se;
	}

	/**
	** A special exception to close a session.
	*/
	public static StandardException closeException() {
		StandardException se = newException(SQLState.CLOSE_REQUEST);
		se.setReport(REPORT_NEVER);
		return se;
	}
	/*
	** Message handling
	*/

	/**
		The message stored in the super class Throwable must be set
		up object creation. At this time we cannot get any information
		about the object itself (ie. this) in order to determine the
		natural language message. Ie. we need to class of the objec in
		order to look up its message, but we can't get the class of the
		exception before calling the super class message.
		<P>
		Thus the message stored by Throwable and obtained by the
		getMessage() of Throwable (ie. super.getMessage() in this
		class) is the message identifier. The actual text message
		is stored in this class at the first request.

	*/

	public String getMessage() {
		if (textMessage == null)
			textMessage = MessageService.getCompleteMessage(getMessageId(), getArguments());
		return textMessage;
	}

	/**
		Return the message identifier that is used to look up the
		error message text in the messages.properties file.
	*/
	public final String getMessageId() {
		return super.getMessage();
	}


	/**
		Get the error code for an error given a type. The value of
		the property messageId.type will be returned, e.g.
		deadlock.sqlstate.
	*/
	public String getErrorProperty(String type) {
		return getErrorProperty(getMessageId(), type);
	}

	/**
		Don't print the class name in the toString() method.
	*/
	public String toString() {
		String msg = getMessage();

		return "ERROR " + getSQLState() + ": " + msg;
	}

	/*
	** Static methods
	*/

	private static String getErrorProperty(String messageId, String type) {
		return MessageService.getProperty(messageId, type);
	}

	public static StandardException interrupt(InterruptedException ie) {
		StandardException se = StandardException.newException(SQLState.CONN_INTERRUPT, ie);
		return se;
	}
	/*
	** SQL warnings
	*/

	public static SQLWarning newWarning(String messageId) {

		return newWarningCommon( messageId, (Object[]) null );

	}

	public static SQLWarning newWarning(String messageId, Object a1) {

		Object[] oa = new Object[] {a1};

		return newWarningCommon( messageId, oa );
	}

	public static SQLWarning newWarning(String messageId, Object a1, Object a2) {

		Object[] oa = new Object[] {a1, a2};

		return newWarningCommon( messageId, oa );
	}

	private	static	SQLWarning	newWarningCommon( String messageId, Object[] oa )
	{
		String		message = MessageService.getCompleteMessage(messageId, oa);
		String		state = StandardException.getSQLStateFromIdentifier(messageId);
		SQLWarning	sqlw = new SQLWarning(message, state, ExceptionSeverity.WARNING_SEVERITY);

		return sqlw;
	}
}
