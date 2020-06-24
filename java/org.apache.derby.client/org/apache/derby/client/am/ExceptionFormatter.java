/*

   Derby - Class org.apache.derby.client.am.ExceptionFormatter

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

import java.io.PrintWriter;
import java.sql.BatchUpdateException;
import java.sql.DataTruncation;
import java.sql.SQLException;
import java.sql.SQLWarning;
import javax.transaction.xa.XAException;

//IC see: https://issues.apache.org/jira/browse/DERBY-6125
class ExceptionFormatter {
    // returnTokensOnly is true only when exception tracing is enabled so
    // that we don't try to go to the server for a message while we're in
    // the middle of parsing an Sqlca reply.
    // Without this, if e.getMessage() fails, we would have infinite recursion
    // when TRACE_DIAGNOSTICS is on  because tracing occurs within the exception constructor.
    static void printTrace(SqlException e,
                                  PrintWriter printWriter,
//IC see: https://issues.apache.org/jira/browse/DERBY-852
                                  String messageHeader,
                                  boolean returnTokensOnly) {
        String header;
        synchronized (printWriter) {
            while (e != null) {
                header = messageHeader + "[" + "SQLException@" + Integer.toHexString(e.hashCode()) + "]";
                printWriter.println(header + " java.sql.SQLException");

//IC see: https://issues.apache.org/jira/browse/DERBY-6125
                Throwable throwable = e.getCause();
                if (throwable != null) {
                    printTrace(throwable, printWriter, header);
                }
//IC see: https://issues.apache.org/jira/browse/DERBY-5076
                Sqlca sqlca = e.getSqlca();
                if (sqlca != null) {
                    printTrace(sqlca, printWriter, header);
                    // JDK stack trace calls e.getMessage(), so we must set some state on the sqlca that says return tokens only.
                    sqlca.returnTokensOnlyInMessageText(returnTokensOnly);
                }

                printWriter.println(header + " SQL state  = " + e.getSQLState());
                printWriter.println(header + " Error code = " + String.valueOf(e.getErrorCode()));
                if (e.getSqlca() == null) { // Too much has changed, so escape out here.
                    printWriter.println(header + " Message    = " + e.getMessage());
                } else { // This is server-side error.
                    sqlca = e.getSqlca();
                    if (returnTokensOnly) {
                        // print message tokens directly.
                        printWriter.println(header + " Tokens     = " + sqlca.getSqlErrmc()); // a string containing error tokens only
                    } else {
                        // Try to get message text from server.
                        String message = e.getMessage();
                        if (!sqlca.messageTextRetrievedContainsTokensOnly_) { // got the message text.
                            printWriter.println(header + " Message    = " + message);
                        } else { // got only message tokens.
//IC see: https://issues.apache.org/jira/browse/DERBY-1061
                            SqlException mysteryException = sqlca.exceptionThrownOnStoredProcInvocation_;
                            if (mysteryException != null &&
                                    (mysteryException.getErrorCode() == -440 || mysteryException.getErrorCode() == -444)) {
                                printWriter.println(header + " Unable to obtain message text from server." +
                                        " Only message tokens are available." +
                                        " The stored procedure SYSIBM.SQLCAMESSAGE is not installed on server." +
                                        " Contact your DBA.");
                            } else {
                                printWriter.println(header + " Error occurred while trying to obtain message text from server. " +
                                        "Only message tokens are available.");
                            }
                            printWriter.println(header + " Tokens     = " + message);
                        }
                    }
                }

                printWriter.println(header + " Stack trace follows");
                e.printStackTrace(printWriter);

//IC see: https://issues.apache.org/jira/browse/DERBY-5076
                sqlca = e.getSqlca();
                if (sqlca != null) {
                    // JDK stack trace calls e.getMessage(), now that it is finished,
                    // we can reset the state on the sqlca that says return tokens only.
                    sqlca.returnTokensOnlyInMessageText(false);
                }

                e = e.getNextException();
            }

            printWriter.flush();
        }
    }

    static void printTrace(SQLException e,
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
                                  PrintWriter printWriter,
                                  String messageHeader,
                                  boolean returnTokensOnly) {
        String header;
        synchronized (printWriter) {
            while (e != null) {
                if (e instanceof DataTruncation) {
                    header = messageHeader + "[" + "DataTruncation@" + Integer.toHexString(e.hashCode()) + "]";
                    printWriter.println(header + " java.sql.DataTruncation");
                } else if (e instanceof SQLWarning) {
                    header = messageHeader + "[" + "SQLWarning@" + Integer.toHexString(e.hashCode()) + "]";
                    printWriter.println(header + " java.sql.SQLWarning");
                } else if (e instanceof BatchUpdateException) {
                    header = messageHeader + "[" + "BatchUpdateException@" + Integer.toHexString(e.hashCode()) + "]";
                    printWriter.println(header + " java.sql.BatchUpdateException");
                } else { // e instanceof SQLException
                    header = messageHeader + "[" + "SQLException@" + Integer.toHexString(e.hashCode()) + "]";
                    printWriter.println(header + " java.sql.SQLException");
                }

                printWriter.println(header + " SQL state  = " + e.getSQLState());
                printWriter.println(header + " Error code = " + String.valueOf(e.getErrorCode()));
                printWriter.println(header + " Message    = " + e.getMessage());
//IC see: https://issues.apache.org/jira/browse/DERBY-852

//IC see: https://issues.apache.org/jira/browse/DERBY-6125
                if (e instanceof DataTruncation) {
                    printWriter.println(header + " Index         = " +
                                        ((DataTruncation) e).getIndex());
                    printWriter.println(header + " Parameter     = " +
                                        ((DataTruncation) e).getParameter());
                    printWriter.println(header + " Read          = " +
                                        ((DataTruncation) e).getRead());
                    printWriter.println(header + " Data size     = " +
                                        ((DataTruncation) e).getDataSize());
                    printWriter.println(header + " Transfer size = " +
                                        ((DataTruncation) e).getTransferSize());
                }

                if (e instanceof BatchUpdateException) {
                    printWriter.println(
                        header + " Update counts = " +
                        Utils.getStringFromInts(
                            ((BatchUpdateException)e).getUpdateCounts()));
                }

                printWriter.println(header + " Stack trace follows");
                e.printStackTrace(printWriter);

                e = e.getNextException();
            }

            printWriter.flush();
        }
    }

    private static void printTrace(Sqlca sqlca,
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
                                  PrintWriter printWriter,
                                  String messageHeader) {
        String header = messageHeader + "[" + "Sqlca@" + Integer.toHexString(sqlca.hashCode()) + "]";
        synchronized (printWriter) {
            printWriter.println(header + " DERBY SQLCA from server");
            printWriter.println(header + " SqlCode        = " + sqlca.getSqlCode());
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
            printWriter.println(header + " SqlErrd        = " + sqlca.formatSqlErrd());
            printWriter.println(header + " SqlErrmc       = " + sqlca.getSqlErrmc());
            printWriter.println(header + " SqlErrp        = " + sqlca.getSqlErrp());
            printWriter.println(header + " SqlState       = " + sqlca.getSqlState());
            printWriter.println(header + " SqlWarn        = " + sqlca.getSqlWarn());
        }
    }

    static void printTrace(Throwable e,
                                  PrintWriter printWriter,
                                  String messageHeader) {
        String header = messageHeader + "[" + "Throwable@" + Integer.toHexString(e.hashCode()) + "]";
        synchronized (printWriter) {
            printWriter.println(header + " " + e.getClass().getName());
            printWriter.println(header + " Message = " + e.getMessage());
            printWriter.println(header + " Stack trace follows");
            e.printStackTrace(printWriter);
        }
    }

    static void printTrace(XAException e,
                                  PrintWriter printWriter,
                                  String messageHeader) {
        String header = messageHeader + "[" + "XAException@" + Integer.toHexString(e.hashCode()) + "]";
        synchronized (printWriter) {
            printWriter.println(header + " javax.transaction.xa.XAException");
            printWriter.println(header + " Message = " + e.getMessage());
            printWriter.println(header + " Stack trace follows");

            e.printStackTrace(printWriter);
        }
    }
}
