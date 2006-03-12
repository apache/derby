/*

   Derby - Class org.apache.derby.client.am.ExceptionFormatter

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

public class ExceptionFormatter {
    // returnTokensOnly is true only when exception tracing is enabled so
    // that we don't try to go to the server for a message while we're in
    // the middle of parsing an Sqlca reply.
    // Without this, if e.getMessage() fails, we would have infinite recursion
    // when TRACE_DIAGNOSTICS is on  because tracing occurs within the exception constructor.
    static public void printTrace(SqlException e,
                                  java.io.PrintWriter printWriter,
                                  String messageHeader,
                                  boolean returnTokensOnly) {
        String header;
        synchronized (printWriter) {
            while (e != null) {
                header = messageHeader + "[" + "SQLException@" + Integer.toHexString(e.hashCode()) + "]";
                printWriter.println(header + " java.sql.SQLException");

                java.lang.Throwable throwable = null;
                try {
                    throwable = ((Diagnosable) e).getThrowable();
                } catch (java.lang.NoSuchMethodError doNothing) {
                }
                if (throwable != null) {
                    printTrace(throwable, printWriter, header);
                }
                Sqlca sqlca = ((Diagnosable) e).getSqlca();
                if (sqlca != null) {
                    printTrace(sqlca, printWriter, header);
                    // JDK stack trace calls e.getMessage(), so we must set some state on the sqlca that says return tokens only.
                    ((Sqlca) sqlca).returnTokensOnlyInMessageText(returnTokensOnly);
                }

                printWriter.println(header + " SQL state  = " + e.getSQLState());
                printWriter.println(header + " Error code = " + String.valueOf(e.getErrorCode()));
                if (((Diagnosable) e).getSqlca() == null) { // Too much has changed, so escape out here.
                    printWriter.println(header + " Message    = " + e.getMessage());
                } else { // This is server-side error.
                    sqlca = ((Diagnosable) e).getSqlca();
                    if (returnTokensOnly) {
                        // print message tokens directly.
                        printWriter.println(header + " Tokens     = " + sqlca.getSqlErrmc()); // a string containing error tokens only
                    } else {
                        // Try to get message text from server.
                        String message = e.getMessage();
                        if (!sqlca.messageTextRetrievedContainsTokensOnly_) { // got the message text.
                            printWriter.println(header + " Message    = " + message);
                        } else { // got only message tokens.
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

                if (e instanceof Diagnosable) {
                    sqlca = (Sqlca) ((Diagnosable) e).getSqlca();
                    if (sqlca != null) {
                        // JDK stack trace calls e.getMessage(), now that it is finished,
                        // we can reset the state on the sqlca that says return tokens only.
                        sqlca.returnTokensOnlyInMessageText(false);
                    }
                }

                e = e.getNextException();
            }

            printWriter.flush();
        }
    }

    static public void printTrace(java.sql.SQLException e,
                                  java.io.PrintWriter printWriter,
                                  String messageHeader,
                                  boolean returnTokensOnly) {
        String header;
        synchronized (printWriter) {
            while (e != null) {
                if (e instanceof java.sql.DataTruncation) {
                    header = messageHeader + "[" + "DataTruncation@" + Integer.toHexString(e.hashCode()) + "]";
                    printWriter.println(header + " java.sql.DataTruncation");
                } else if (e instanceof java.sql.SQLWarning) {
                    header = messageHeader + "[" + "SQLWarning@" + Integer.toHexString(e.hashCode()) + "]";
                    printWriter.println(header + " java.sql.SQLWarning");
                } else if (e instanceof java.sql.BatchUpdateException) {
                    header = messageHeader + "[" + "BatchUpdateException@" + Integer.toHexString(e.hashCode()) + "]";
                    printWriter.println(header + " java.sql.BatchUpdateException");
                } else { // e instanceof java.sql.SQLException
                    header = messageHeader + "[" + "SQLException@" + Integer.toHexString(e.hashCode()) + "]";
                    printWriter.println(header + " java.sql.SQLException");
                }

                printWriter.println(header + " SQL state  = " + e.getSQLState());
                printWriter.println(header + " Error code = " + String.valueOf(e.getErrorCode()));
                printWriter.println(header + " Message    = " + e.getMessage());

                if (e instanceof java.sql.DataTruncation) {
                    printWriter.println(header + " Index         = " + ((java.sql.DataTruncation) e).getIndex());
                    printWriter.println(header + " Parameter     = " + ((java.sql.DataTruncation) e).getParameter());
                    printWriter.println(header + " Read          = " + ((java.sql.DataTruncation) e).getRead());
                    printWriter.println(header + " Data size     = " + ((java.sql.DataTruncation) e).getDataSize());
                    printWriter.println(header + " Transfer size = " + ((java.sql.DataTruncation) e).getTransferSize());
                }

                if (e instanceof java.sql.BatchUpdateException) {
                    printWriter.println(header + " Update counts = " + Utils.getStringFromInts(((java.sql.BatchUpdateException) e).getUpdateCounts()));
                }

                printWriter.println(header + " Stack trace follows");
                e.printStackTrace(printWriter);

                e = e.getNextException();
            }

            printWriter.flush();
        }
    }

    static public void printTrace(Sqlca sqlca,
                                  java.io.PrintWriter printWriter,
                                  String messageHeader) {
        String header = messageHeader + "[" + "Sqlca@" + Integer.toHexString(sqlca.hashCode()) + "]";
        synchronized (printWriter) {
            printWriter.println(header + " DERBY SQLCA from server");
            printWriter.println(header + " SqlCode        = " + sqlca.getSqlCode());
            printWriter.println(header + " SqlErrd        = " + Utils.getStringFromInts(sqlca.getSqlErrd()));
            printWriter.println(header + " SqlErrmc       = " + sqlca.getSqlErrmc());
            printWriter.println(header + " SqlErrmcTokens = " + Utils.getStringFromStrings(sqlca.getSqlErrmcTokens()));
            printWriter.println(header + " SqlErrp        = " + sqlca.getSqlErrp());
            printWriter.println(header + " SqlState       = " + sqlca.getSqlState());
            printWriter.println(header + " SqlWarn        = " + new String(sqlca.getSqlWarn()));
        }
    }

    static public void printTrace(java.lang.Throwable e,
                                  java.io.PrintWriter printWriter,
                                  String messageHeader) {
        String header = messageHeader + "[" + "Throwable@" + Integer.toHexString(e.hashCode()) + "]";
        synchronized (printWriter) {
            printWriter.println(header + " " + e.getClass().getName());
            printWriter.println(header + " Message = " + e.getMessage());
            printWriter.println(header + " Stack trace follows");
            e.printStackTrace(printWriter);
        }
    }

    static public void printTrace(javax.transaction.xa.XAException e,
                                  java.io.PrintWriter printWriter,
                                  String messageHeader) {
        String header = messageHeader + "[" + "XAException@" + Integer.toHexString(e.hashCode()) + "]";
        synchronized (printWriter) {
            printWriter.println(header + " javax.transaction.xa.XAException");
            printWriter.println(header + " Message = " + e.getMessage());
            printWriter.println(header + " Stack trace follows");

            e.printStackTrace(printWriter);

            if (!((org.apache.derby.client.am.Configuration.jreLevelMajor == 1) &&
                    (org.apache.derby.client.am.Configuration.jreLevelMinor >= 4)) ||
                    (org.apache.derby.client.am.Configuration.jreLevelMajor > 1)) { // If not jre 1.4 or above, we need to print the cause if there is one
                // For jre 1.4 or above, e.printStackTrace() will print the cause automatically
                if (e instanceof Diagnosable) {
                    java.lang.Throwable throwable = null;
                    try {
                        throwable = ((Diagnosable) e).getThrowable();
                    } catch (java.lang.NoSuchMethodError doNothing) {
                    }
                    if (throwable != null) {
                        printWriter.print("Caused by: ");
                        if (throwable instanceof java.sql.SQLException) {
                            throwable.printStackTrace(printWriter);
                        } else {
                            printTrace(throwable, printWriter, header);
                        }
                    }
                }
            }
        }
    }
}
