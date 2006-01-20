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

public class SqlException extends java.sql.SQLException implements Diagnosable {
    protected static final int DEFAULT_ERRCODE = 99999;
    java.lang.Throwable throwable_ = null;
    protected Sqlca sqlca_ = null; // for engine generated errors only
    protected String message_ = null;
    private String batchPositionLabel_; // for batched exceptions only
    
    public static String CLIENT_MESSAGE_RESOURCE_NAME =
        "org.apache.derby.loc.clientmessages";
    
    // The message utility instance we use to find messages
    // It's primed with the name of the client message bundle so that
    // it knows to look there if the message isn't found in the
    // shared message bundle.
    private static MessageUtil msgutil_ = 
        new MessageUtil(CLIENT_MESSAGE_RESOURCE_NAME);

    
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
    }
    
    public SqlException(LogWriter logwriter, MessageId msgid, Object[] args)
    {
        this(logwriter, msgid, args, null);
    }
    
    public SqlException (LogWriter logwriter, MessageId msgid)
    {
        this(logwriter, msgid, null);
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

    private SqlException(LogWriter logWriter, java.lang.Throwable throwable, 
        String reason, String sqlState, int errorCode ) {
        super(reason, sqlState, errorCode);
        message_ = reason;
        throwable_ = throwable;

        setCause();
        
        if (logWriter != null) {
            logWriter.traceDiagnosable(this);
        }
        
    }
        
    protected void setCause()
    {
        // Store the throwable correctly depending upon its class
        // and whether initCause() is available
        if (throwable_ != null  )
        {
            if ( throwable_ instanceof SQLException )
            {
                setNextException((SQLException)throwable_);
            }
            else if ( JVMInfo.JDK_ID >= JVMInfo.J2SE_14 )
            {
    			initCause(throwable_);
            }
            else
            {
                message_ = message_ + " Caused by exception " + 
                    throwable_.getClass() + ": " + throwable_.getMessage();

            }
        }
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
        if (sqlca_ != null) {
            message_ = ((Sqlca) sqlca_).getJDBCMessage();
        }

        if (batchPositionLabel_ == null) {
            return message_;
        }

        return batchPositionLabel_ + message_;
    }

    public String getSQLState() {
        if (sqlca_ == null) {
            return super.getSQLState();
        } else {
            return sqlca_.getSqlState();
        }
    }

    public int getErrorCode() {
        if (sqlca_ == null) {
            return super.getErrorCode();
        } else {
            return sqlca_.getSqlCode();
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
