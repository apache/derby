/*

   Derby - Class org.apache.derby.client.am.XaException

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


public class XaException extends javax.transaction.xa.XAException implements Diagnosable {

    //-----------------constructors-----------------------------------------------

    public XaException(LogWriter logWriter) {
        super();
        if (logWriter != null) {
            logWriter.traceDiagnosable(this);
        }
    }

    public XaException(LogWriter logWriter, java.lang.Throwable throwable) {
        super();
        initCause(throwable);
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
        initCause(throwable);
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
        initCause(throwable);
        if (logWriter != null) {
            logWriter.traceDiagnosable(this);
        }
    }

    public Sqlca getSqlca() {
        return null;
    }

    public void printTrace(java.io.PrintWriter printWriter, String header) {
        ExceptionFormatter.printTrace(this, printWriter, header);
    }

    // Return a single XaException without the "next" pointing to another SQLException.
    // Because the "next" is a private field in java.sql.SQLException,
    // we have to create a new XaException in order to break the chain with "next" as null.
    XaException copyAsUnchainedXAException(LogWriter logWriter) {
        XaException xae = new XaException(logWriter, getCause(), getMessage());
        xae.errorCode = this.errorCode;
        return xae;
    }
}



