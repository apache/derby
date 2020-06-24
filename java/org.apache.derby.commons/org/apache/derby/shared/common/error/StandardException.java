/*

   Derby - Class org.apache.derby.shared.common.error.StandardException

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

package org.apache.derby.shared.common.error;

import java.lang.reflect.InvocationTargetException;
import java.security.PrivilegedActionException;
import java.sql.SQLException;
import java.sql.SQLWarning;

import org.apache.derby.shared.common.error.ExceptionSeverity;
import org.apache.derby.shared.common.reference.SQLState;
import org.apache.derby.shared.common.util.ArrayUtil;
import org.apache.derby.shared.common.i18n.MessageService;
import org.apache.derby.shared.common.sanity.SanityManager;

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
	private transient Object[] arguments;
	private int severity;
	private String textMessage;
	private String sqlState;
	private transient int report;
    private transient boolean isForPublicAPI;
    private transient SQLException next;

	/*
	** End of constructors
	*/
	
	protected StandardException(String messageID)
	{
		this(messageID, (Throwable) null, (Object[]) null);

	}

	protected StandardException(String messageID, Throwable t, Object[] args)
	{
		super(messageID);

		this.severity = getSeverityFromIdentifier(messageID);
		this.sqlState = getSQLStateFromIdentifier(messageID);
		this.arguments = args;
//IC see: https://issues.apache.org/jira/browse/DERBY-2472
		if (t != null) {
			initCause(t);
		}

		if (SanityManager.DEBUG)
		{
//IC see: https://issues.apache.org/jira/browse/DERBY-336
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

	/**
	 * Returns the arguments for this exception,
	 * if there are any.
	 */
	public final Object[] getArguments()    { return ArrayUtil.copy( arguments ); }

	/**
		Yes, report me. Errors that need this method to return
		false are in the minority.

        @return the report type
	*/
	public final int report() {
		return report;
	}

	/**
		Set my report type.

        @param report The report type
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
		string that corresponds to the field in org.apache.derby.shared.common.reference.SQLState.
//IC see: https://issues.apache.org/jira/browse/DERBY-6945

        @return the 5 character SQL state
	*/
	public final String getSQLState()
	{
		return sqlState;
	}

    /**
     * Get the next {@code SQLException} that should be put into the parent
     * exception when this instance is converted to an {@code SQLException}.
     * @return the next exception
     */
    public final SQLException getNextException() {
//IC see: https://issues.apache.org/jira/browse/DERBY-6488
        return next;
    }

    /**
     * Mark this exception as one that is thrown by a public API method.
     * The purpose is to signal that this should be a top-level exception,
     * so that it doesn't get wrapped inside multiple layers of other
     * SQLExceptions or StandardExceptions as it travels up through the
     * code layers.
     * @see PublicAPI
     */
    final void markAsPublicAPI() {
        isForPublicAPI = true;
    }

	/**
//IC see: https://issues.apache.org/jira/browse/DERBY-6945
		Convert a message identifer from org.apache.derby.shared.common.reference.SQLState to
		a SQLState five character string.
	 *	@param messageID - the sql state id of the message from Derby
	 *	@return String 	 - the 5 character code of the SQLState ID to returned to the user 
	*/
	public static String getSQLStateFromIdentifier(String messageID) {

		if (messageID.length() == 5)
			return messageID;
		return messageID.substring(0, 5);
	}

	/**
		Get the severity given a message identifier from org.apache.derby.shared.common.reference.SQLState.
//IC see: https://issues.apache.org/jira/browse/DERBY-6945

        @param messageID The handle on the message
        @return the severity associated with the message
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

    public static StandardException
//IC see: https://issues.apache.org/jira/browse/DERBY-6254
            newException(String messageId, Object... args) {
        return newException(messageId, (Throwable) null, args);
    }

    public static StandardException
            newException(String messageId, Throwable t, Object... args) {
        return new StandardException(messageId, t, args);
    }

	/* 2 arguments */

    /**
     * Dummy exception to catch incorrect use of
     * StandardException.newException(), at compile-time. If you get a
     * compilation error because this exception isn't caught, it means
     * that you are using StandardException.newException(...)
     * incorrectly. The nested exception should always be the second
     * argument.
     * @see StandardException#newException(String, Object, Throwable)
     * @see StandardException#newException(String, Object, Object, Throwable)
     */
    public static class BadMessageArgumentException extends Throwable {}

    /**
     * Dummy overload which should never be called. Only used to
     * detect incorrect usage, at compile time.
     * @param messageID - the sql state id of the message
     * @param a1 - Message arg
     * @param t - Incorrectly placed exception to be nested
     * @return nothing - always throws
     * @throws BadMessageArgumentException - always (dummy)
     */
    public static StandardException newException(String messageID, 
                                                 Object a1, 
                                                 Throwable t) 
        throws BadMessageArgumentException {
        throw new BadMessageArgumentException();
    }

	/* 3 arguments */

    /**
     * Dummy overload which should never be called. Only used to
     * detect incorrect usage, at compile time.
     * @param messageID - the sql state id of the message
     * @param a1 - First message arg
     * @param a2 - Second message arg
     * @param t - Incorrectly placed exception to be nested
     * @return nothing - always throws
     * @throws BadMessageArgumentException - always (dummy)
     */
    public static StandardException newException(String messageID, 
//IC see: https://issues.apache.org/jira/browse/DERBY-336
                                                 Object a1, 
                                                 Object a2,
                                                 Throwable t) 
        throws BadMessageArgumentException {
        throw new BadMessageArgumentException(); 
    }

    /**
     * Creates a new StandardException using message text that has already been localized.
     *
     * @param MessageID The SQLState and severity are derived from the ID. However the text message is not.
     * @param t The Throwable that caused this exception, null if this exception was not caused by another Throwable.
     * @param localizedMessage The message associated with this exception.
     *        <b>It is the caller's responsibility to ensure that this message is properly localized.</b>
     *
     * See org.apache.derby.iapi.tools.i18n.LocalizedResource
     *
     *
     * @return a Derby exception
     */
    public static StandardException newPreLocalizedException( String MessageID,
                                                              Throwable t,
                                                              String localizedMessage)
    {
        StandardException se = new StandardException( MessageID, localizedMessage);
        if( t != null)
//IC see: https://issues.apache.org/jira/browse/DERBY-2472
            se.initCause(t);
        return se;
    }
    
    
	/**
     * Unpack the exception, looking for a StandardException, which carries
	 * the Derby messageID and arguments.
     *
	 * See org.apache.derby.impl.jdbc.SQLExceptionFactory
	 * See org.apache.derby.impl.jdbc.Util
     *
     * @param se A SQLException
     *
     * @return a Derby exception wrapping the contents of the SQLException
	 */
    public static StandardException getArgumentFerry(SQLException se)
	{
//IC see: https://issues.apache.org/jira/browse/DERBY-6488
        Throwable cause = se.getCause();
        return (cause instanceof StandardException)
                ? (StandardException) cause : null;
	}

    /**
     * Check if the top-level throwable is just a vacuous wrapper that does
     * not carry any useful information except what's returned by the
     * {@link Throwable#getCause()} method.
     *
     * @param t the throwable to check
     * @return {@code true} if the throwable is a vacuous wrapper and the
     *   only useful piece of information is the cause, or {@code false}
     *   otherwise
     */
    private static boolean isVacuousWrapper(Throwable t) {
        // The only interesting information in an InvocationTargetException
        // or a PrivilegedActionException is the cause, so consider them
        // vacuous if they have a cause.
//IC see: https://issues.apache.org/jira/browse/DERBY-6493
        if (t instanceof InvocationTargetException
                || t instanceof PrivilegedActionException) {
            return (t.getCause() != null);
        }

        // All other exceptions are non-vacuous.
        return false;
    }

	public static StandardException unexpectedUserException(Throwable t)
	{

        // If there is no useful information in the top-level throwable,
        // peel it off and only report the cause.
        if (isVacuousWrapper(t)) {
            return unexpectedUserException(t.getCause());
        }

        // If the exception is an SQLException generated by Derby, it has an
        // argument ferry which is a StandardException. Use this to check
        // whether the exception was generated by Derby.
        StandardException ferry = null;
        if (t instanceof SQLException) {
            SQLException sqle = (SQLException) t;
            ferry = getArgumentFerry(sqle);

            // If the ferry is marked for public API, it means we shouldn't
            // wrap it inside an "unexpected user exception", so just peel
            // off the parent SQLException and return the ferry.
            if (ferry != null && ferry.isForPublicAPI) {
                // If the parent SQLException has any next exceptions, we
                // need to store a reference to them before the parent is
                // discarded.
                ferry.next = sqle.getNextException();
                return ferry;
            }
        }

		/*
        ** If we have a SQLException that didn't come from Derby, then we check
		** to see if it is a valid user defined exception range 
		** (38001-38XXX).  If so, then we convert it into a 
		** StandardException without further ado.
		*/ 
//IC see: https://issues.apache.org/jira/browse/DERBY-1440
//IC see: https://issues.apache.org/jira/browse/DERBY-2472
		if ((t instanceof SQLException) && (ferry == null))
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
//IC see: https://issues.apache.org/jira/browse/DERBY-2472
					se.initCause(sqlex.getNextException());
				}
				return se;
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
			**    some Derby exception that is not a standard exception.
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
			** In the past we didn't want to place the class name in
			** an exception because Cloudscape builds were
			** obfuscated, so the class name would change from build
            ** to build. This is no longer true for Derby.
            ** If the exception has no detail message
			** then we use the class name to make tracking the 
            ** problem down easier, though the lack of a message
			** should be seen as a bug.
			*/
            String detailMessage = t.getMessage();
//IC see: https://issues.apache.org/jira/browse/DERBY-6488

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
//IC see: https://issues.apache.org/jira/browse/DERBY-6488
            else
            {
                detailMessage = t.getClass().getName() + ": " + detailMessage;
			}

            return
				newException(SQLState.LANG_UNEXPECTED_USER_EXCEPTION, t, detailMessage);
		}
	}

	/**
		Similar to unexpectedUserException but makes no assumtion about
		when the execption is being called. The error is wrapped as simply
		as possible.

        @param t The original error which should be wrapped

        @return a Derby exception which wraps the original error
	*/

	public static StandardException plainWrapException(Throwable t) {

        // If there is no useful information in the top-level throwable,
        // peel it off and only report the cause.
//IC see: https://issues.apache.org/jira/browse/DERBY-6493
        if (isVacuousWrapper(t)) {
            return plainWrapException(t.getCause());
        }

		if (t instanceof StandardException)
			return (StandardException) t;

		if (t instanceof SQLException) {

			SQLException sqle = (SQLException) t;

			String sqlState = sqle.getSQLState();
			if (sqlState != null) {

				StandardException se = new StandardException(sqlState, "(" + sqle.getErrorCode()  + ") " + sqle.getMessage());
				sqle = sqle.getNextException();
				if (sqle != null)
//IC see: https://issues.apache.org/jira/browse/DERBY-2472
					se.initCause(plainWrapException(sqle));
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
    *
    * @return a "session close" exception
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
//IC see: https://issues.apache.org/jira/browse/DERBY-6945
        <P>
		The message stored in the super class Throwable must be set
		up object creation. At this time we cannot get any information
		about the object itself (ie. this) in order to determine the
		natural language message. Ie. we need to class of the objec in
		order to look up its message, but we can't get the class of the
		exception before calling the super class message.
		</P>
		<P>
		Thus the message stored by Throwable and obtained by the
		getMessage() of Throwable (ie. super.getMessage() in this
		class) is the message identifier. The actual text message
		is stored in this class at the first request.
		</P>

        @return the message text
	*/

	public String getMessage() {
//IC see: https://issues.apache.org/jira/browse/DERBY-6262
        if (textMessage == null) {
            textMessage = MessageService.getTextMessage(
                                getMessageId(), getArguments());
        }

		return textMessage;
	}

	/**
		Return the message identifier that is used to look up the
		error message text in the messages.properties file.

        @return the message id
	*/
	public final String getMessageId() {
		return super.getMessage();
	}


	/**
		Get the error code for an error given a type. The value of
		the property messageId.type will be returned, e.g.
		deadlock.sqlstate.

        @param type An error type
        @return the corresponding error code
	*/
	public String getErrorProperty(String type) {
		return getErrorProperty(getMessageId(), type);
	}

	/**
		Don't print the class name in the toString() method.
	*/
	public String toString() {
        // Add the SQLState to the message. This should be kept consistent
        // with SqlException.toString() in the client driver.
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

    public static SQLWarning newWarning(String messageId, Object... oa)
	{
//IC see: https://issues.apache.org/jira/browse/DERBY-6262
        String      message = MessageService.getTextMessage(messageId, oa);
		String		state = StandardException.getSQLStateFromIdentifier(messageId);
		SQLWarning	sqlw = new SQLWarning(message, state, ExceptionSeverity.WARNING_SEVERITY);

		return sqlw;
	}

    /**
     * Is this a lock timeout exception.
     * <p>
     *
     * @return true if this exception is a lock timeout.
     *
     **/
    public final boolean isLockTimeout() {

        return(SQLState.LOCK_TIMEOUT.equals(getSQLState()));
    }

    /**
     * Is this a self-deadlock exception caused by a nested transaction
     * being blocked by its parent's locks.
     * <p>
     *
     * @return true if this exception is a self-deadlock.
     *
     **/
    public final boolean isSelfDeadlock() {

//IC see: https://issues.apache.org/jira/browse/DERBY-6554
        return(SQLState.SELF_DEADLOCK.equals(getSQLState()));
    }

    /**
     * Is this a lock timeout or lock deadlock exception.
     * <p>
     *
     * @return true if this exception is a lock timeout or lock deadlock.
     *
     **/
    public final boolean isLockTimeoutOrDeadlock() {

        return(SQLState.LOCK_TIMEOUT.equals(getSQLState()) ||
               SQLState.DEADLOCK.equals(getSQLState()));
    }
}
