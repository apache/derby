/*

   Derby - Class org.apache.derby.client.am.SqlWarning

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

// Not yet done:
//   Assign an ErrorKey, ResourceKey, and Resource for each throw statement.
//   Save for future pass to avoid maintenance during development.

// Until the "Error Cycle" pass is complete.
// Use the temporary constructors at the bottom.
public class SqlWarning extends java.sql.SQLWarning implements Diagnosable
{
  private java.lang.Throwable throwable_ = null;
  protected Sqlca sqlca_ = null; // for engine generated errors only

  //-----------------constructors-----------------------------------------------

  public SqlWarning (LogWriter logWriter, ErrorKey errorKey)
  {
    super (ResourceUtilities.getResource (ResourceKeys.driverOriginationIndicator) +
           ResourceUtilities.getResource (errorKey.getResourceKey()),
           errorKey.getSQLState(),
           errorKey.getErrorCode());
    if (logWriter != null) logWriter.traceDiagnosable (this);
  }

  public SqlWarning (LogWriter logWriter, ErrorKey errorKey, Object[] args)
  {
    super (ResourceUtilities.getResource (ResourceKeys.driverOriginationIndicator) +
           ResourceUtilities.getResource (errorKey.getResourceKey(), args),
           errorKey.getSQLState(),
           errorKey.getErrorCode());
    if (logWriter != null) logWriter.traceDiagnosable (this);
  }

  public SqlWarning (LogWriter logWriter, ErrorKey errorKey, Object arg)
  {
    this (logWriter, errorKey, new Object[] {arg});
  }

  public SqlWarning (LogWriter logWriter, Sqlca sqlca)
  {
    super ();
    sqlca_ = sqlca;
    if (logWriter != null) logWriter.traceDiagnosable (this);
  }

  // Temporary constructor until all error keys are defined.
  public SqlWarning (LogWriter logWriter)
  {
    super ();
    if (logWriter != null) logWriter.traceDiagnosable (this);
  }

  // Temporary constructor until all error keys are defined.
  public SqlWarning (LogWriter logWriter, String text)
  {
    super (text);
    if (logWriter != null) logWriter.traceDiagnosable (this);
  }

    // Temporary constructor until all error keys are defined.
  public SqlWarning (LogWriter logWriter, java.lang.Throwable throwable, String text)
  {
    super (text);
    throwable_ = throwable;
    if (logWriter != null) logWriter.traceDiagnosable (this);
  }

  // Temporary constructor until all error keys are defined.
  public SqlWarning (LogWriter logWriter, String text, SqlState sqlState)
  {
    super (text, sqlState.getState());
    if (logWriter != null) logWriter.traceDiagnosable (this);
  }

  // Temporary constructor until all error keys are defined, for subsystem use only
  public SqlWarning (LogWriter logWriter, String text, String sqlState)
  {
    super (text, sqlState);
    if (logWriter != null) logWriter.traceDiagnosable (this);
  }

  // Temporary constructor until all error keys are defined.
  public SqlWarning (LogWriter logWriter, String text, SqlState sqlState, SqlCode errorCode)
  {
    super (text, sqlState.getState(), errorCode.getCode());
    if (logWriter != null) logWriter.traceDiagnosable (this);
  }

  // Temporary constructor until all error keys are defined, for subsystem use only.
  public SqlWarning (LogWriter logWriter, String text, String sqlState, int errorCode)
  {
    super (text, sqlState, errorCode);
    if (logWriter != null) logWriter.traceDiagnosable (this);
  }

  public java.lang.Throwable getThrowable ()
  {
    return throwable_;
  }

  public Sqlca getSqlca ()
  {
    return sqlca_;
  }

  public String getMessage ()
  {
    if (sqlca_ == null)
      return super.getMessage();
    else
      return ((Sqlca) sqlca_).getJDBCMessage();
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
}

