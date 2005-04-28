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

import org.apache.derby.client.resources.ResourceKeys;


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
public class SqlException extends java.sql.SQLException implements Diagnosable
{
  java.lang.Throwable throwable_ = null;
  protected Sqlca sqlca_ = null; // for engine generated errors only
  private String batchPositionLabel_; // for batched exceptions only

  //-----------------constructors-----------------------------------------------

  public SqlException (LogWriter logWriter, ErrorKey errorKey)
  {
    super (ResourceUtilities.getResource (ResourceKeys.driverOriginationIndicator) +
           ResourceUtilities.getResource (errorKey.getResourceKey()),
           errorKey.getSQLState(),
           errorKey.getErrorCode());
    if (logWriter != null) logWriter.traceDiagnosable (this);
  }

  public SqlException (LogWriter logWriter, ErrorKey errorKey, Object[] args)
  {
    super (ResourceUtilities.getResource (ResourceKeys.driverOriginationIndicator) +
           ResourceUtilities.getResource (errorKey.getResourceKey(), args),
           errorKey.getSQLState(),
           errorKey.getErrorCode());
    if (logWriter != null) logWriter.traceDiagnosable (this);
  }

  public SqlException (LogWriter logWriter, ErrorKey errorKey, Object arg)
  {
    this (logWriter, errorKey, new Object[] {arg});
  }

  public SqlException (LogWriter logWriter, Sqlca sqlca)
  {
    super ();
    sqlca_ = sqlca;
    if (logWriter != null) logWriter.traceDiagnosable (this);
  }

  // Temporary constructor until all error keys are defined.
  public SqlException (LogWriter logWriter)
  {
    super (null, null, -99999);
    if (logWriter != null) logWriter.traceDiagnosable (this);
  }

  /*
  // Temporary constructor until all error keys are defined.
  public SQLException (LogWriter logWriter, java.lang.Throwable throwable)
  {
    super ();
    throwable_ = throwable;
    if (logWriter != null) logWriter.traceDiagnosable (this);
  }
  */

  // Temporary constructor until all error keys are defined.
  public SqlException (LogWriter logWriter, String reason)
  {
    super (reason, null, -99999);
    if (logWriter != null) logWriter.traceDiagnosable (this);
  }

  // Temporary constructor until all error keys are defined.
  public SqlException (LogWriter logWriter, java.lang.Throwable throwable, String reason)
  {
    super (reason, null, -99999);
    throwable_ = throwable;
    if (logWriter != null) logWriter.traceDiagnosable (this);
  }

  // Temporary constructor until all error keys are defined.
  public SqlException (LogWriter logWriter, java.lang.Throwable throwable, String reason, SqlState sqlstate)
  {
    super (reason, sqlstate.getState(), -99999);
    throwable_ = throwable;
    if (logWriter != null) logWriter.traceDiagnosable (this);
  }

  // Temporary constructor until all error keys are defined, for subsystem use only.
  public SqlException (LogWriter logWriter, java.lang.Throwable throwable, String reason, String sqlstate)
  {
    super (reason, sqlstate, -99999);
    throwable_ = throwable;
    if (logWriter != null) logWriter.traceDiagnosable (this);
  }

  // Temporary constructor until all error keys are defined.
  public SqlException (LogWriter logWriter, String reason, SqlState sqlState)
  {
    super (reason, sqlState.getState(), -99999);
    if (logWriter != null) logWriter.traceDiagnosable (this);
  }

  // Temporary constructor until all error keys are defined, for subsystem use only.
  public SqlException (LogWriter logWriter, String reason, String sqlState)
  {
    super (reason, sqlState, -99999);
    if (logWriter != null) logWriter.traceDiagnosable (this);
  }

  // Temporary constructor until all error keys are defined.
  public SqlException (LogWriter logWriter, String reason, SqlState sqlState, SqlCode errorCode)
  {
    super (reason, (sqlState == null) ? null : sqlState.getState(), errorCode.getCode());
    if (logWriter != null) logWriter.traceDiagnosable (this);
  }

  // Temporary constructor until all error keys are defined, for subsystem use only.
  public SqlException (LogWriter logWriter, String reason, String sqlState, int errorCode)
  {
    super (reason, sqlState, errorCode);
    if (logWriter != null) logWriter.traceDiagnosable (this);
  }

  // Temporary constructor until all error keys are defined.
  public SqlException (LogWriter logWriter, java.lang.Throwable throwable, String reason, SqlState sqlState, SqlCode errorCode)
  {
    super (reason, sqlState.getState(), errorCode.getCode());
    throwable_ = throwable;
    if (logWriter != null) logWriter.traceDiagnosable (this);
  }

  // Temporary constructor until all error keys are defined, for subsystem use only.
  public SqlException (LogWriter logWriter, java.lang.Throwable throwable, String reason, String sqlState, int errorCode)
  {
    super (reason, sqlState, errorCode);
    throwable_ = throwable;
    if (logWriter != null) logWriter.traceDiagnosable (this);
  }

  // Label an exception element in a batched update exception chain.
  // This text will be prepended onto the exceptions message text dynamically
  // when getMessage() is called.
  // Called by the Agent.
  void setBatchPositionLabel (int index)
  {
    batchPositionLabel_ = "Error for batch element #" + index + ": ";
  }

  public Sqlca getSqlca ()
  {
    return sqlca_;
  }

  public java.lang.Throwable getThrowable ()
  {
    return throwable_;
  }

  public String getMessage ()
  {
    String message;

    if (sqlca_ == null)
      message = super.getMessage();
    else
      message = ((Sqlca) sqlca_).getJDBCMessage();

    if (batchPositionLabel_ == null)
      return message;

    return batchPositionLabel_ + message;
  }

  public String getSQLState ()
  {
    if (sqlca_ == null)
      return super.getSQLState();
    else
      return sqlca_.getSqlState();
  }

  public int getErrorCode ()
  {
    if (sqlca_ == null)
      return super.getErrorCode();
    else
      return sqlca_.getSqlCode();
  }

  public void printTrace (java.io.PrintWriter printWriter, String header)
  {
    ExceptionFormatter.printTrace (this, printWriter, header);
  }

  // Return a single SQLException without the "next" pointing to another SQLException.
  // Because the "next" is a private field in java.sql.SQLException,
  // we have to create a new SqlException in order to break the chain with "next" as null.
  SqlException copyAsUnchainedSQLException (LogWriter logWriter)
  {
    if (sqlca_ != null)
      return new SqlException (logWriter, sqlca_); // server error
    else
      return new SqlException (logWriter, getMessage(), getSQLState(), getErrorCode()); // client error
  }
}

// An intermediate exception encapsulation to provide code-reuse
// for common ResultSet and ResultSetMetaData column access exceptions.
class ColumnIndexOutOfBoundsException extends SqlException
{
  ColumnIndexOutOfBoundsException (LogWriter logWriter, Throwable throwable, int resultSetColumn)
  {
    super (logWriter, throwable,
           "Invalid argument:" +
           " Result column index " + resultSetColumn + " is out of range.");
  }
}

// An intermediate exception encapsulation to provide code-reuse
// for common ResultSet data conversion exceptions.
class NumberFormatConversionException extends SqlException
{
  NumberFormatConversionException (LogWriter logWriter, String instance)
  {
    super (logWriter,
            "Invalid data conversion:" +
           " Result column instance " +
           instance +
           " is either an invalid numeric representation" +
           " or is out of range.");
  }
}

// An intermediate exception encapsulation to provide code-reuse
// for common ResultSet data conversion exceptions.
class ColumnTypeConversionException extends SqlException
{
  ColumnTypeConversionException (LogWriter logWriter)
  {
    super (logWriter,
           "Invalid data conversion:" +
           " Wrong result column type for requested conversion.");
  }
}

// An intermediate exception encapsulation to provide code-reuse
// for common CrossConverters data conversion exceptions.
class LossOfPrecisionConversionException extends SqlException
{
  LossOfPrecisionConversionException (LogWriter logWriter, String instance)
  {
    super (logWriter,
           "Invalid data conversion:" +
           "Requested conversion would result in a loss of precision of " +
           instance);
  }
}
