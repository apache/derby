/*

   Derby - Class org.apache.derby.client.am.XaException

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


public class XaException extends javax.transaction.xa.XAException implements Diagnosable {
    java.lang.Throwable throwable_ = null;

    //-----------------constructors-----------------------------------------------

    public XaException(LogWriter logWriter) {
        super();
        if (logWriter != null) {
            logWriter.traceDiagnosable(this);
        }
    }

    public XaException(LogWriter logWriter, java.lang.Throwable throwable) {
        super();
        throwable_ = throwable;
        if (((org.apache.derby.client.am.Configuration.jreLevelMajor == 1) &&
                (org.apache.derby.client.am.Configuration.jreLevelMinor >= 4)) ||
                (org.apache.derby.client.am.Configuration.jreLevelMajor > 1)) { // jre 1.4 or above, init the cause
            initCause(throwable);
        }
        if (logWriter != null) {
            logWriter.traceDiagnosable(this);
        }
    }

    public XaException(LogWriter logWriter, int errcode) {
        super();
        errorCode = errcode;
        if (logWriter != null) {
            logWriter.traceDiagnosable(this);
        }
    }

    public XaException(LogWriter logWriter, java.lang.Throwable throwable, int errcode) {
        super();
        errorCode = errcode;
        throwable_ = throwable;
        if (((org.apache.derby.client.am.Configuration.jreLevelMajor == 1) &&
                (org.apache.derby.client.am.Configuration.jreLevelMinor >= 4)) ||
                (org.apache.derby.client.am.Configuration.jreLevelMajor > 1)) { // jre 1.4 or above, init the cause
            initCause(throwable);
        }
        if (logWriter != null) {
            logWriter.traceDiagnosable(this);
        }
    }

    public XaException(LogWriter logWriter, String s) {
        super(s);
        if (logWriter != null) {
            logWriter.traceDiagnosable(this);
        }
    }

    public XaException(LogWriter logWriter, java.lang.Throwable throwable, String s) {
        super(s);
        throwable_ = throwable;
        if (((org.apache.derby.client.am.Configuration.jreLevelMajor == 1) &&
                (org.apache.derby.client.am.Configuration.jreLevelMinor >= 4)) ||
                (org.apache.derby.client.am.Configuration.jreLevelMajor > 1)) { // jre 1.4 or above, init the cause
            initCause(throwable);
        }
        if (logWriter != null) {
            logWriter.traceDiagnosable(this);
        }
    }

    public Sqlca getSqlca() {
        return null;
    }

    public java.lang.Throwable getThrowable() {
        return throwable_;
    }

    public void printTrace(java.io.PrintWriter printWriter, String header) {
        ExceptionFormatter.printTrace(this, printWriter, header);
    }

    // Return a single XaException without the "next" pointing to another SQLException.
    // Because the "next" is a private field in java.sql.SQLException,
    // we have to create a new XaException in order to break the chain with "next" as null.
    XaException copyAsUnchainedXAException(LogWriter logWriter) {
        XaException xae = new XaException(logWriter, this.getThrowable(), getMessage()); // client error
        xae.errorCode = this.errorCode;
        return xae;
    }
}



