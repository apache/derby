/*

   Derby - Class org.apache.derby.client.am.SqlException

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

*/

package org.apache.derby.client.am;

import java.sql.SQLException;

import org.apache.derby.shared.common.i18n.MessageUtil;
import org.apache.derby.shared.common.error.ExceptionUtil;
import org.apache.derby.shared.common.reference.MessageId;
import org.apache.derby.shared.common.reference.SQLState;


// The signature of the stored procedure SQLCAMessage I have come out so far is as follows:
// SQLCAMessage (
//     IN  SQLCode       INTEGER,
//     IN  SQLErrml      SMALLINT,
//     IN  SQLErrmc      VARCHAR(70),
//     IN  SQLErrp       CHAR(8),
//     IN  SQLErrd0      INTEGER,
//     IN  SQLErrd1      INTEGER,
//     IN  SQLErrd2      INTEGER,
//     IN  SQLErrd3      INTEGER,
//     IN  SQLErrd4      INTEGER,
//     IN  SQLErrd5      INTEGER,
//     IN  SQLWarn       CHAR(11),
//     IN  SQLState      CHAR(5),
//     IN  Locale        CHAR(5),
//     IN  BufferSize    SMALLINT,
//     IN  LineWidth     SMALLINT,
//     OUT Message       VARCHAR(2400))
//
// Some issues have been identified:
// 1. What would be the schema name of the stored procedue SQLCAMessage?
// 2. What is the format and type of the Locale parameter? If there does, I would really like to know the format of the locale in order to decide the type of the Locale parameter. Even there does not either, the Locale parameter probably still needs to be kept there for future extension, and we need to figure out the format of the locale.
// 3. What would be the format of the output message? Is this full message text ok or do we only need the explanation message corresponding to an SQL code. This somehow matters whether we need the Buffersize and Linewidth parameters for the stored procedure.
// 4. What if the invocation of stored procedure failed (due to, eg, connection dropping)? In this case, we probably need to return some client-side message.
//
// Note that this class does NOT extend java.sql.SQLException.  This is because
// in JDBC 4 there will be multiple subclasses of SQLException defined by the
// spec.  So we can't also extend SQLException without having to create our
// own mirror hierarchy of subclasses.
//
// When Derby is ready to throw an exception to the application, it catches
// SqlException and converts it to a java.sql.SQLException by calling the
// method getSQLException.
//
// It is also possible that internal routines may call public methods.
// In these cases, it will need to wrap a java.sql.SQLException inside
// a Derby SqlException so that the internal method does not have to throw
// java.sql.SQLException.  Otherwise the chain of dependencies would quickly
// force the majority of internal methods to throw java.sql.SQLException.
// You can wrap a java.sql.SQLException inside a SqlException by using
// the constructor <code>new SqlException(java.sql.SQLException wrapMe)</code)
//
public class SqlException extends Exception implements Diagnosable {
    private static final int DEFAULT_ERRCODE = 99999;
    private transient Sqlca sqlca_ = null; // for engine generated errors only
    /** Tells which of the messages in the SQLCA this exception refers to
     * (counting from 0). For engine generated errors only. */
    private transient int messageNumber_;
    private String message_ = null;
    private String cachedMessage_ = null;
    private String batchPositionLabel_; // for batched exceptions only
    private String sqlstate_ = null;
    private int errorcode_ = DEFAULT_ERRCODE;
    private String causeString_ = null;
    protected SqlException nextException_;
    
    public static final String CLIENT_MESSAGE_RESOURCE_NAME =
        "org.apache.derby.loc.clientmessages";
    
    /**
     * SQLException factory is initialized with default factory.
     * It will be over written by the SQLException factory of the
     * supported JDBC version.
     */
    private static SQLExceptionFactory
            exceptionFactory = new SQLExceptionFactory ();
    
    /** 
     *  The message utility instance we use to find messages
     *  It's primed with the name of the client message bundle so that
     *  it knows to look there if the message isn't found in the
     *  shared message bundle.
     */
    private static MessageUtil msgutil_;
    
    /**
     * This routine provides singleton access to an instance of MessageUtil
     * that is constructed for client messages.  It is recommended to use
     * this singleton rather than create your own instance.
     *
     * The only time you need this instance is if you need to directly
     * format an internationalized message string.  In most instances this
     * is done for you when you invoke a SqlException constructor
     *
     * @return a singleton instance of MessageUtil configured for client
     *   messages
     */
    public static MessageUtil getMessageUtil() {
        if ( msgutil_ == null ) {
            msgutil_ = new MessageUtil(CLIENT_MESSAGE_RESOURCE_NAME);
        }
        
        return msgutil_;
    }

    /** 
     * The wrapped SQLException, if one exists
     */
    protected SQLException wrappedException_;
  
    //-----------------constructors-----------------------------------------------
    // New constructors that support internationalized messages
    // The message id is wrapped inside a class so that we can distinguish
    // between the signatures of the new constructors and the old constructors
    
    /**
     * Create a SqlException.  This constructor is the "base" constructor;
     * all other constructors (which take a ClientMessageId) delegate to this
     * constructor
     *
     * @param logwriter
     *      Can be null, but if provided, it is used to log this exception
     *
     * @param msgid
     *      The message id for this message.  ClientMessageId is a simple type-safe
     *      wrapper for org.apache.derby.shared.common.reference.SQLState message id
     *      strings.
     *
     * @param args
     *      The set of substitution arguments for the message.  The Java message
     *      formatter will substitute these arguments into the internationalized
     *      strings using the substitution ({0}, {1}, etc.) markers in the string.
     *      Any object can be passed, but if you want it to be readable, make sure
     *      toString() for the object returns something useful.
     *
     * @param cause
     *      Can be null.  Indicates the cause of this exception.  If this is
     *      an instance of SqlException or java.sql.SQLException then the exception
     *      is chained into the nextException chain.  Otherwise it is chained
     *      using initCause().
     */
    SqlException(LogWriter logwriter,
        ClientMessageId msgid, Object[] args, Throwable cause)
    {
        this(
            logwriter,
            cause,
            getMessageUtil().getCompleteMessage(
                msgid.msgid,
                args),
            ExceptionUtil.getSQLStateFromIdentifier(msgid.msgid),
            ExceptionUtil.getSeverityFromIdentifier(msgid.msgid));        
    }

    // Use the following SQLExceptions when you want to override the error
    // code that is derived from the severity of the message id.
    SqlException(LogWriter logWriter, ClientMessageId msgid, Object[] args,
        SqlCode sqlcode, Throwable t) {
        this(logWriter, msgid, args, t);
        this.errorcode_ = sqlcode.getCode();
    }

    public SqlException(LogWriter logWriter, ClientMessageId msgid,
                        SqlCode sqlcode, Object... args) {
        this(logWriter, msgid, args, sqlcode, (Throwable)null);
    }

    // The following constructors are all wrappers around the base constructor,
    // created to make it easy to code against them (you don't have to pass
    // null arguments or construct object arrays).  See the javadoc for the
    // "base" constructor for an explanation of the parameters

    public SqlException(LogWriter logWriter, ClientMessageId msgid,
                        Throwable cause, Object... args) {
        this(logWriter, msgid, args, cause);
    }
    
    public SqlException(LogWriter logwriter,
                        ClientMessageId msgid,
                        Object... args)
    {
        this(logwriter, msgid, args, (Throwable)null);
    }

    /**
     * Create an exception for an engine generated error.
     *
     * @param logWriter object used for tracing
     * @param sqlca the SQLCA sent from the server
     */
    public SqlException(LogWriter logWriter, Sqlca sqlca) {
        this(sqlca, 0, true);
        // only set the error code for the first exception in the chain (we
        // don't know the error code for the rest)
        errorcode_ = sqlca.getErrorCode();
        if ( logWriter != null )
        {
            logWriter.traceDiagnosable(SqlException.this);
        }
    }

    /**
     * Create one of the exceptions in an exception chain generated by the
     * engine. This constructor calls itself recursively to create the rest of
     * the exception chain if {@code chain} is
     * {@code true}.
     *
     * @param sqlca the SQLCA sent from the server
     * @param number the message number for this exception (counting from 0)
     * @param chain if {@code true}, generate the rest of the exception
     * chain recursively and link it to this exception
     */
    private SqlException(Sqlca sqlca, int number, boolean chain) {
        this.sqlca_ = sqlca;
        messageNumber_ = number;
        sqlstate_ = sqlca.getSqlState(number);

        // If the SQLState indicates that this is a java.sql.DataTruncation
        // type of exception, generate one right away.
        if (SQLState.DATA_TRUNCATION_READ.equals(sqlstate_)) {
            wrappedException_ = sqlca.getDataTruncation();
        }

        int nextMsg = number + 1;
        if (chain && (sqlca.numberOfMessages() > nextMsg)) {
            setThrowable(new SqlException(sqlca, nextMsg, true));
        }
    }
    
    // Once all messages are internationalized, these methods should become
    // private
    private SqlException(LogWriter logWriter, String reason, String sqlState,
        int errorCode)
    {
        this(logWriter, (Throwable)null, reason, sqlState, errorCode);
    }

    private SqlException(LogWriter logWriter, Throwable throwable,
        String reason, String sqlState, int errorCode ) {
        message_ = reason;
        sqlstate_ = sqlState;
        errorcode_ = errorCode;

        setThrowable(throwable);
        
        if (logWriter != null) {
            logWriter.traceDiagnosable(SqlException.this);
        }
        
    }
    
    /**
     * Set the cause of this exception based on its type.
     * {@code SQLException}s and
     * {@code SqlException}s are
     * linked with {@code setNextException()} and
     * {@code initCause()}.
     * All other exception types are linked with {@code initCause()}.
     */
    private void setThrowable(Throwable throwable)
    {
        if ( throwable instanceof SqlException )
        {
            setNextException((SqlException) throwable);
        }
        else if ( throwable instanceof SQLException )
        {
            setNextException((SQLException) throwable );
        }

        if (throwable != null) {
            initCause(throwable);
        }
    }
        
    /**
     * Wrap a SQLException in a SqlException.  This is used by internal routines
     * so the don't have to throw SQLException, which, through the chain of 
     * dependencies would force more and more internal routines to throw
     * SQLException
     */
    public SqlException(SQLException wrapme)
    {
        wrappedException_ = wrapme;
    }
                
    
    /**
     * Convert this SqlException into a java.sql.SQLException
     */
    public SQLException getSQLException()
    {
        if ( wrappedException_ != null )
        {
            return wrappedException_;
        }
                        
        // When we have support for JDBC 4 SQLException subclasses, this is
        // where we decide which exception to create
        SQLException sqle = exceptionFactory.getSQLException(getMessage(), getSQLState(), 
            getErrorCode());
        sqle.initCause(this);

        // Set up the nextException chain
        if ( nextException_ != null )
        {
            // The exception chain gets constructed automatically through 
            // the beautiful power of recursion
            sqle.setNextException(nextException_.getSQLException());
        }
        
        return sqle;
    }    

    // Label an exception element in a batched update exception chain.
    // This text will be prepended onto the exceptions message text dynamically
    // when getMessage() is called.
    // Called by the Agent.
    void setBatchPositionLabel(int index) {
        batchPositionLabel_ = getMessageUtil().getTextMessage(MessageId.BATCH_POSITION_ID) + 
            index + ": ";
    }

    public Sqlca getSqlca() {
        return sqlca_;
    }

    @Override
    public String toString() {
        // Match what the embedded driver does in StandardException.toString().
        return "ERROR " + getSQLState() + ": " + getMessage();
    }

    @Override
    public String getMessage() {
        if ( wrappedException_ != null )
        {
            return wrappedException_.getMessage();
        }
        
        // The Net JDBC message is retrieved and cached if we have a valid
        // SQLCA handle.
        // It is possible that we don't have one in case of a serialized
        // SqlException for instance. In this case, we set the message to the
        // last one cached previously (if any available).
        // For serialized SqlException, we can serialize the SQLCA as the
        // object handle would become invalid, upon deserialization, causing
        // the connection and JDBC not being retrievable (hence why it is
        // being cached here).
        if (sqlca_ != null) {
            cachedMessage_ = message_ =
                    ((Sqlca) sqlca_).getJDBCMessage(messageNumber_);
        }
        else if (cachedMessage_ != null) {
            // SQLCA is no longer valid, set the message to the previously
            // cached one
            message_ = cachedMessage_;
        }
        
        if (batchPositionLabel_ != null) {
            message_ = batchPositionLabel_ + message_;
        }
        
        if ( causeString_ != null ) {
            // Append the string indicating the cause of the exception
            // (this happens only in JDK13 environments)
            message_ += causeString_;
        }
        
        return message_;
    }

    public String getSQLState() {
        if ( wrappedException_ != null )
        {
            return wrappedException_.getSQLState();
        }

        return sqlstate_;
    }

    public int getErrorCode() {
        if ( wrappedException_ != null )
        {
            return wrappedException_.getErrorCode();
        }
        
        return errorcode_;
    }

    public SqlException getNextException()
    {
        if ( wrappedException_ != null )
        {
            return new SqlException(wrappedException_.getNextException());
        }
        else
        {
            return nextException_;
        }
    }
    
    public void setNextException(SqlException nextException)
    {
        if ( wrappedException_ != null )
        {
            wrappedException_.setNextException(nextException.getSQLException());
        }
        else
        {
            nextException_ = nextException;
        }        
    }
    
    public void setNextException(SQLException nextException)
    {
        if ( wrappedException_ != null )
        {
            wrappedException_.setNextException(nextException);
        }
        else
        {
            // Add this exception to the end of the chain
            SqlException theEnd = this;
            while (theEnd.nextException_ != null) {
                theEnd = theEnd.nextException_;
            }
            theEnd.nextException_ = new SqlException(nextException);
        }
    }

    /**
     * Helper method to construct an exception which basically says that
     * we encountered an underlying Java exception
     */
    static SqlException javaException(LogWriter logWriter, Throwable e) {
        return new SqlException(logWriter, 
            new ClientMessageId (SQLState.JAVA_EXCEPTION), 
            new Object[] {e.getClass().getName(), e.getMessage()}, e);
    }
    
    // Return a single SQLException without the "next" pointing to another SQLException.
    // Because the "next" is a private field in java.sql.SQLException,
    // we have to create a new SqlException in order to break the chain with "next" as null.
    SqlException copyAsUnchainedSQLException(LogWriter logWriter) {
        if (sqlca_ != null) {
            // server error
            return new SqlException(sqlca_, messageNumber_, false);
        } else {
            return new SqlException(logWriter, getMessage(), getSQLState(), getErrorCode()); // client error
        }
    }
}

// An intermediate exception encapsulation to provide code-reuse
// for common ResultSet data conversion exceptions.

class ColumnTypeConversionException extends SqlException {
    ColumnTypeConversionException(LogWriter logWriter, String targetType,
        String sourceType) {
        super(logWriter,
            new ClientMessageId(SQLState.LANG_DATA_TYPE_GET_MISMATCH),
            targetType, sourceType);
    }
}

// An intermediate exception encapsulation to provide code-reuse
// for common CrossConverters data conversion exceptions.

class OutsideRangeForDataTypeException extends SqlException {
    OutsideRangeForDataTypeException(LogWriter logWriter, String instance) {
        super(logWriter,
              new ClientMessageId(SQLState.LANG_OUTSIDE_RANGE_FOR_DATATYPE), 
              instance);
    }
}
