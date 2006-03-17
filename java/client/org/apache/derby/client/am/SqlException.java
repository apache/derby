/*

   Derby - Class org.apache.derby.client.am.SqlException

   Copyright (c) 2001, 2005 The Apache Software Foundation or its licensors, where applicable.

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

package org.apache.derby.client.am;

import java.sql.SQLException;
import java.util.TreeMap;

import org.apache.derby.client.resources.ResourceKeys;
import org.apache.derby.shared.common.info.JVMInfo;
import org.apache.derby.shared.common.i18n.MessageUtil;
import org.apache.derby.shared.common.error.ExceptionUtil;


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
    protected static final int DEFAULT_ERRCODE = 99999;
    protected Sqlca sqlca_ = null; // for engine generated errors only
    protected String message_ = null;
    private String batchPositionLabel_; // for batched exceptions only
    protected String sqlstate_ = null;
    protected int errorcode_ = DEFAULT_ERRCODE;
    protected String causeString_ = null;
    protected SqlException nextException_;
    protected Throwable throwable_;
    
    public static String CLIENT_MESSAGE_RESOURCE_NAME =
        "org.apache.derby.loc.clientmessages";
    
    
    /** 
     *  The message utility instance we use to find messages
     *  It's primed with the name of the client message bundle so that
     *  it knows to look there if the message isn't found in the
     *  shared message bundle.
     */
    private static MessageUtil msgutil_ = 
        new MessageUtil(CLIENT_MESSAGE_RESOURCE_NAME);

    /** 
     * The wrapped SQLException, if one exists
     */
    protected SQLException wrappedException_;
  
    //-----------------constructors-----------------------------------------------
    // New constructors that support internationalized messages
    // The message id is wrapped inside a class so that we can distinguish
    // between the signatures of the new constructors and the old constructors
    public SqlException(LogWriter logwriter, 
        MessageId msgid, Object[] args, Throwable cause)
    {
        this(
            logwriter,
            msgutil_.getCompleteMessage(
                msgid.msgid,
                args),
            ExceptionUtil.getSQLStateFromIdentifier(msgid.msgid),
            ExceptionUtil.getSeverityFromIdentifier(msgid.msgid));
        
        this.setThrowable(cause);
    }

    public SqlException(LogWriter logWriter, MessageId messageId, Throwable cause)
    {
        this(logWriter,messageId,null,cause);
    }
    
    public SqlException(LogWriter logwriter, MessageId msgid, Object[] args)
    {
        this(logwriter, msgid, args, null);
    }
    
    public SqlException (LogWriter logwriter, MessageId msgid)
    {
        this(logwriter, msgid, (Object[])null);
    }
    
    public SqlException(LogWriter logwriter, MessageId msgid, Object arg1)
    {
        this(logwriter, msgid, new Object[] { arg1 });
    }
    
    public SqlException(LogWriter logwriter,
        MessageId msgid, Object arg1, Object arg2)
    {
        this(logwriter, msgid, new Object[] { arg1, arg2 });
    }
    
    public SqlException(LogWriter logwriter,
        MessageId msgid, Object arg1, Object arg2, Object arg3)
    {
        this(logwriter, msgid, new Object[] { arg1, arg2, arg3 });
    }
    
    public SqlException(LogWriter logWriter, Sqlca sqlca) {
        this.sqlca_ = sqlca;
        if ( logWriter != null )
        {
            logWriter.traceDiagnosable(this);
        }
    }
    
    // Once all messages are internationalized, these methods should become
    // private
    public SqlException(LogWriter logWriter, String reason, String sqlState,
        int errorCode)
    {
        this(logWriter, null, reason, sqlState, errorCode);
    }

    public SqlException(LogWriter logWriter, java.lang.Throwable throwable, 
        String reason, String sqlState, int errorCode ) {
        message_ = reason;
        sqlstate_ = sqlState;
        errorcode_ = errorCode;

        setThrowable(throwable);
        
        if (logWriter != null) {
            logWriter.traceDiagnosable(this);
        }
        
    }
    
    /**
     * Set the cause of this exception based on its type and
     * the current runtime version of Java
     */
    protected void setThrowable(Throwable throwable)
    {
        throwable_ = throwable;
        
        // If the throwable is a SQL exception, use nextException rather
        // than chained exceptions
        if ( throwable instanceof SqlException )
        {
            setNextException((SqlException) throwable);
        }
        else if ( throwable instanceof SQLException )
        {
            setNextException((SQLException) throwable );
        }
        else if ( throwable != null )
        {
            // Set up a string indicating the cause if the current runtime
            // doesn't support the initCause() method.  This is then used
            // by getMessage() when it composes the message string.
            if (JVMInfo.JDK_ID < JVMInfo.J2SE_14 )
            {
                causeString_ = " Caused by exception " + 
                    throwable.getClass() + ": " + throwable.getMessage();
            }
            else
            {
                initCause(throwable);
            }
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
        
    // Constructors for backward-compatibility while we're internationalizng
    // all the messages
    public SqlException(LogWriter logWriter) {
        if (logWriter != null) {
            logWriter.traceDiagnosable(this);
        }
    }

    public SqlException(LogWriter logWriter, String reason) {
        this(logWriter, reason, null, DEFAULT_ERRCODE);
    }

    public SqlException(LogWriter logWriter, java.lang.Throwable throwable, String reason) {
        this(logWriter, throwable, reason, null, DEFAULT_ERRCODE);
    }

    public SqlException(LogWriter logWriter, java.lang.Throwable throwable, String reason, SqlState sqlstate) {
        this(logWriter, throwable, reason, sqlstate.getState(), DEFAULT_ERRCODE);
    }

    public SqlException(LogWriter logWriter, java.lang.Throwable throwable, String reason, String sqlstate) {
        this(logWriter, throwable, reason, sqlstate, DEFAULT_ERRCODE);
    }

    public SqlException(LogWriter logWriter, String reason, SqlState sqlState) {
        this(logWriter, reason, sqlState.getState(), DEFAULT_ERRCODE);
    }

    public SqlException(LogWriter logWriter, String reason, String sqlState) {
        this(logWriter, reason, sqlState, DEFAULT_ERRCODE);
    }

    public SqlException(LogWriter logWriter, String reason, SqlState sqlState, SqlCode errorCode) {
        this(logWriter, reason, sqlState.getState(), errorCode.getCode());
    }
    

    public SqlException(LogWriter logWriter, java.lang.Throwable throwable, String reason, SqlState sqlState, SqlCode errorCode) {
        this(logWriter, throwable, reason, sqlState.getState(), 
            errorCode.getCode());
    }

    //--- End backward-compatibility constructors ----------------------
    
    
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
        SQLException sqle = new SQLException(getMessage(), getSQLState(), 
            getErrorCode());

        // If we're in a runtime that supports chained exceptions, set the cause 
        // of the SQLException to be this SqlException.  Otherwise the stack
        // trace is lost.
         if (JVMInfo.JDK_ID >= JVMInfo.J2SE_14 )
        {
            sqle.initCause(this);
        }

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
        batchPositionLabel_ = "Error for batch element #" + index + ": ";
    }

    public Sqlca getSqlca() {
        return sqlca_;
    }

    public java.lang.Throwable getThrowable() {
        return throwable_;
    }

    public String getMessage() {
        if ( wrappedException_ != null )
        {
            return wrappedException_.getMessage();
        }
        
        if (sqlca_ != null) {
            message_ = ((Sqlca) sqlca_).getJDBCMessage();
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
        
        if (sqlca_ == null) {
            return sqlstate_;
        } else {
            return sqlca_.getSqlState();
        }
    }

    public int getErrorCode() {
        if ( wrappedException_ != null )
        {
            return wrappedException_.getErrorCode();
        }
        
        if (sqlca_ == null) {
            return errorcode_;
        } else {
            return sqlca_.getSqlCode();
        }
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

    public void printTrace(java.io.PrintWriter printWriter, String header) {
        ExceptionFormatter.printTrace(this, printWriter, header);
    }
    
    // Return a single SQLException without the "next" pointing to another SQLException.
    // Because the "next" is a private field in java.sql.SQLException,
    // we have to create a new SqlException in order to break the chain with "next" as null.
    SqlException copyAsUnchainedSQLException(LogWriter logWriter) {
        if (sqlca_ != null) {
            return new SqlException(logWriter, sqlca_); // server error
        } else {
            return new SqlException(logWriter, getMessage(), getSQLState(), getErrorCode()); // client error
        }
    }
}

// An intermediate exception encapsulation to provide code-reuse
// for common ResultSet and ResultSetMetaData column access exceptions.

class ColumnIndexOutOfBoundsException extends SqlException {
    ColumnIndexOutOfBoundsException(LogWriter logWriter, Throwable throwable, int resultSetColumn) {
        super(logWriter, throwable,
                "Invalid argument:" +
                " Result column index " + resultSetColumn + " is out of range.");
    }
}

// An intermediate exception encapsulation to provide code-reuse
// for common ResultSet data conversion exceptions.

class NumberFormatConversionException extends SqlException {
    NumberFormatConversionException(LogWriter logWriter, String instance) {
        super(logWriter,
                "Invalid data conversion:" +
                " Result column instance " +
                instance +
                " is either an invalid numeric representation" +
                " or is out of range.");
    }
}

// An intermediate exception encapsulation to provide code-reuse
// for common ResultSet data conversion exceptions.

class ColumnTypeConversionException extends SqlException {
    ColumnTypeConversionException(LogWriter logWriter) {
        super(logWriter,
                "Invalid data conversion:" +
                " Wrong result column type for requested conversion.");
    }
}

// An intermediate exception encapsulation to provide code-reuse
// for common CrossConverters data conversion exceptions.

class LossOfPrecisionConversionException extends SqlException {
    LossOfPrecisionConversionException(LogWriter logWriter, String instance) {
        super(logWriter,
                "Invalid data conversion:" +
                "Requested conversion would result in a loss of precision of " +
                instance);
    }
}
