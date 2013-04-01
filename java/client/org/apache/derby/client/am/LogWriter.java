/*

   Derby - Class org.apache.derby.client.am.LogWriter

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

import java.io.IOException;
import java.io.PrintWriter;
import java.security.AccessController;
import java.security.PrivilegedExceptionAction;
import java.sql.SQLException;
import java.util.Properties;
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.jdbc.ClientBaseDataSourceRoot;
import org.apache.derby.jdbc.ClientDataSourceInterface;
import org.apache.derby.shared.common.reference.Attribute;
import org.apache.derby.shared.common.reference.SQLState;

public class LogWriter {
    final protected PrintWriter printWriter_;
    final private int traceLevel_;

    private boolean driverConfigurationHasBeenWrittenToJdbc1Stream_ = false;
    private boolean driverConfigurationHasBeenWrittenToJdbc2Stream_ = false;

    // It is assumed that this constructor is never called when logWriter is null.
    public LogWriter(java.io.PrintWriter printWriter, int traceLevel) {
        printWriter_ = printWriter;
        traceLevel_ = traceLevel;
    }

    final protected boolean loggingEnabled(int traceLevel) {
        if (SanityManager.DEBUG) {
            if (printWriter_ == null) {
                SanityManager.THROWASSERT(
                        "Broken invariant: printWriter_ == null");
            }
        }

        return (traceLevel & traceLevel_) != 0;
    }

    // When garbage collector doesn't kick in in time
    // to close file descriptors, "Too many open files"
    // exception may occur (currently found on Linux).
    // To minimize the chance of this problem happening,
    // the print writer needs to be closed (after this
    // DNC log writer is closed) when each connection has
    // its own trace file (i.e. traceDirectory is specified).
    public boolean printWriterNeedsToBeClosed_;

    void close() {
        if (printWriterNeedsToBeClosed_) {
            printWriter_.close();
            printWriterNeedsToBeClosed_ = false;
        }
        // printWriter_ = null; // help GC.
    }

    // ---------------------------------------------------------------------------

    public void dncprintln(String s) {
        synchronized (printWriter_) {
            printWriter_.println("[derby] " + s);
            printWriter_.flush();
        }
    }

    private void dncprint(String s) {
        synchronized (printWriter_) {
            printWriter_.print("[derby] " + s);
            printWriter_.flush();
        }
    }

    private void dncprintln(String header, String s) {
        synchronized (printWriter_) {
            printWriter_.println("[derby]" + header + " " + s);
            printWriter_.flush();
        }
    }

    private void dncprint(String header, String s) {
        synchronized (printWriter_) {
            printWriter_.print("[derby]" + header + " " + s);
            printWriter_.flush();
        }
    }

    // ------------------------ tracepoint api -----------------------------------

    public void tracepoint(String component, int tracepoint,
                           String classContext, String methodContext) {
        String staticContextTracepointRecord =
                component +
                "[time:" + System.currentTimeMillis() + "]" +
                "[thread:" + Thread.currentThread().getName() + "]" +
                "[tracepoint:" + tracepoint + "]" +
                "[" + classContext + "." + methodContext + "]";
        dncprintln(staticContextTracepointRecord);
    }

    // ------------- API entry and exit trace methods ----------------------------
    // Entry and exit are be traced separately because input arguments need
    // to be traced before any potential exception can occur.
    // Exit tracing is only performed on methods that return values.
    // Entry tracing is only performed on methods that update state,
    // so entry tracing is not performed on simple getter methods.
    // We could decide in the future to restrict entry tracing only to methods with input arguments.

    private void traceExternalMethod(Object instance, String className, String methodName) {
        dncprint(buildExternalMethodHeader(instance, className), methodName);
    }

    private void traceExternalDeprecatedMethod(Object instance, String className, String methodName) {
        dncprint(buildExternalMethodHeader(instance, className), "Deprecated " + methodName);
    }

    private String buildExternalMethodHeader(Object instance, String className) {
        return
                "[Time:" + System.currentTimeMillis() + "]" +
                "[Thread:" + Thread.currentThread().getName() + "]" +
                "[" + className + "@" + Integer.toHexString(instance.hashCode()) + "]";
    }

    private String getClassNameOfInstanceIfTraced(Object instance) {
        if (instance == null) // this prevents NPE from instance.getClass() used below
        {
            return null;
        } else if (instance instanceof Connection && loggingEnabled(
                ClientDataSourceInterface.TRACE_CONNECTION_CALLS)) {
            return "Connection";
        } else if (instance instanceof ResultSet && loggingEnabled(
                ClientDataSourceInterface.TRACE_RESULT_SET_CALLS)) {
            return "ResultSet";
        } else if (instance instanceof CallableStatement && loggingEnabled(
                ClientDataSourceInterface.TRACE_STATEMENT_CALLS)) {
            return "CallableStatement";
        } else if (instance instanceof PreparedStatement && loggingEnabled(
                ClientDataSourceInterface.TRACE_STATEMENT_CALLS)) {
            return "PreparedStatement";
        } else if (instance instanceof Statement && loggingEnabled(
                ClientDataSourceInterface.TRACE_STATEMENT_CALLS)) {
            return "Statement";
        }
        // Not yet externalizing Blob tracing, except for trace_all
        else if (instance instanceof Blob && loggingEnabled(
                ClientDataSourceInterface.TRACE_ALL)) // add a trace level for
                                                      // lobs !!
        {
            return "Blob";
        }
        // Not yet externalizing Clob tracing, except for trace_all
        else if (instance instanceof Clob && loggingEnabled(
                ClientDataSourceInterface.TRACE_ALL)) // add a trace level for
                                                      // bobs !!
        {
            return "Clob";
        }
        // Not yet externalizing dbmd catalog call tracing, except for trace_all
        else if (instance instanceof DatabaseMetaData && loggingEnabled(
                ClientDataSourceInterface.TRACE_ALL)) // add a trace level for
                                                      // dbmd ??
        {
            return "DatabaseMetaData";
        }
        // we don't use instanceof javax.transaction.XAResource to avoid dependency on j2ee.jar
        else if (loggingEnabled(ClientDataSourceInterface.TRACE_XA_CALLS) &&
                instance.getClass().getName().startsWith("org.apache.derby.client.net.NetXAResource")) {
            return "NetXAResource";
        } else if (loggingEnabled(ClientDataSourceInterface.TRACE_ALL) &&
                instance.getClass().getName().equals("org.apache.derby.client.ClientPooledConnection")) {
            return "ClientPooledConnection";
        } else if (loggingEnabled(ClientDataSourceInterface.TRACE_ALL) &&
                instance.getClass().getName().equals("org.apache.derby.jdbc.ClientConnectionPoolDataSource")) {
            return "ClientConnectionPoolDataSource";
        } else if (loggingEnabled(ClientDataSourceInterface.TRACE_ALL) &&
                instance.getClass().getName().equals("org.apache.derby.client.ClientXAConnection")) {
            return "ClientXAConnection";
        } else if (loggingEnabled(ClientDataSourceInterface.TRACE_ALL) &&
                instance.getClass().getName().equals("org.apache.derby.jdbc.ClientDataSource")) {
            return "ClientDataSource";
        } else if (loggingEnabled(ClientDataSourceInterface.TRACE_ALL) &&
                instance.getClass().getName().equals("org.apache.derby.jdbc.ClientXADataSource")) {
            return "ClientXADataSource";
        } else {
            return instance.getClass().getName();
        }
    }

    // --------------------------- method exit tracing --------------------------

    public void traceExit(Object instance, String methodName, Object returnValue) {
        String className = getClassNameOfInstanceIfTraced(instance);
        if (className == null) {
            return;
        }
        synchronized (printWriter_) {
            traceExternalMethod(instance, className, methodName);
            printWriter_.println(" () returned " + returnValue);
            printWriter_.flush();
        }
    }

    public void traceDeprecatedExit(Object instance, String methodName, Object returnValue) {
        String className = getClassNameOfInstanceIfTraced(instance);
        if (className == null) {
            return;
        }
        synchronized (printWriter_) {
            traceExternalDeprecatedMethod(instance, className, methodName);
            printWriter_.println(" () returned " + returnValue);
            printWriter_.flush();
        }
    }

    public void traceExit(Object instance, String methodName, ResultSet resultSet) {
        String returnValue = (resultSet == null) ? "ResultSet@null" : "ResultSet@" + Integer.toHexString(resultSet.hashCode());
        traceExit(instance, methodName, returnValue);
    }

    public void traceExit(Object instance, String methodName, CallableStatement returnValue) {
        traceExit(instance, methodName, "CallableStatement@" + Integer.toHexString(returnValue.hashCode()));
    }

    public void traceExit(Object instance, String methodName, PreparedStatement returnValue) {
        traceExit(instance, methodName, "PreparedStatement@" + Integer.toHexString(returnValue.hashCode()));
    }

    public void traceExit(Object instance, String methodName, Statement returnValue) {
        traceExit(instance, methodName, "Statement@" + Integer.toHexString(returnValue.hashCode()));
    }

    public void traceExit(Object instance, String methodName, Blob blob) {
        String returnValue = (blob == null) ? "Blob@null" : "Blob@" + Integer.toHexString(blob.hashCode());
        traceExit(instance, methodName, returnValue);
    }

    public void traceExit(Object instance, String methodName, Clob clob) {
        String returnValue = (clob == null) ? "Clob@null" : "Clob@" + Integer.toHexString(clob.hashCode());
        traceExit(instance, methodName, returnValue);
    }

    public void traceExit(Object instance, String methodName, DatabaseMetaData returnValue) {
        traceExit(instance, methodName, "DatabaseMetaData@" + Integer.toHexString(returnValue.hashCode()));
    }

    public void traceExit(Object instance, String methodName, Connection returnValue) {
        traceExit(instance, methodName, "Connection@" + Integer.toHexString(returnValue.hashCode()));
    }

    public void traceExit(Object instance, String methodName, ColumnMetaData returnValue) {
        traceExit(instance, methodName, "MetaData@" + (returnValue != null ? Integer.toHexString(returnValue.hashCode()) : null));
    }

    public void traceExit(Object instance, String methodName, byte[] returnValue) {
        traceExit(instance, methodName, Utils.getStringFromBytes(returnValue));
    }

    public void traceExit(Object instance, String methodName, int[] returnValue) {
        traceExit(instance, methodName, Utils.getStringFromInts(returnValue));
    }

    public void traceDeprecatedExit(Object instance, String methodName, byte[] returnValue) {
        traceDeprecatedExit(instance, methodName, Utils.getStringFromBytes(returnValue));
    }

    public void traceExit(Object instance, String methodName, byte returnValue) {
        traceExit(instance, methodName, "0x" + Integer.toHexString(returnValue & 0xff));
    }

    public void traceExit(Object instance, String methodName, int returnValue) {
        traceExit(instance, methodName, String.valueOf(returnValue));
    }

    public void traceExit(Object instance, String methodName, boolean returnValue) {
        traceExit(instance, methodName, String.valueOf(returnValue));
    }

    public void traceExit(Object instance, String methodName, long returnValue) {
        traceExit(instance, methodName, String.valueOf(returnValue));
    }

    public void traceExit(Object instance, String methodName, float returnValue) {
        traceExit(instance, methodName, String.valueOf(returnValue));
    }

    public void traceExit(Object instance, String methodName, double returnValue) {
        traceExit(instance, methodName, String.valueOf(returnValue));
    }

    // --------------------------- method entry tracing --------------------------

    private void traceEntryAllArgs(Object instance, String methodName, String argList) {
        String className = getClassNameOfInstanceIfTraced(instance);
        if (className == null) {
            return;
        }
        synchronized (printWriter_) {
            traceExternalMethod(instance, className, methodName);
            printWriter_.println(" " + argList + " called");
            printWriter_.flush();
        }
    }

    private void traceDeprecatedEntryAllArgs(Object instance, String methodName, String argList) {
        String className = getClassNameOfInstanceIfTraced(instance);
        if (className == null) {
            return;
        }
        synchronized (printWriter_) {
            traceExternalDeprecatedMethod(instance, className, methodName);
            printWriter_.println(" " + argList + " called");
            printWriter_.flush();
        }
    }

    // ---------------------- trace entry of methods w/ no args ------------------

    public void traceEntry(Object instance, String methodName) {
        traceEntryAllArgs(instance, methodName, "()");
    }

    // ---------------------- trace entry of methods w/ 1 arg --------------------

    public void traceEntry(Object instance, String methodName, Object argument) {
        traceEntryAllArgs(instance, methodName,
                "(" + argument + ")");
    }

    public void traceEntry(Object instance, String methodName, boolean argument) {
        traceEntryAllArgs(instance, methodName,
                "(" + argument + ")");
    }

    public void traceEntry(Object instance, String methodName, int argument) {
        traceEntryAllArgs(instance, methodName,
                "(" + argument + ")");
    }

    public void traceDeprecatedEntry(Object instance, String methodName, int argument) {
        traceDeprecatedEntryAllArgs(instance, methodName,
                "(" + argument + ")");
    }

    public void traceDeprecatedEntry(Object instance, String methodName, Object argument) {
        traceDeprecatedEntryAllArgs(instance, methodName,
                "(" + argument + ")");
    }

    // ---------------------- trace entry of methods w/ 2 args -------------------

    public void traceEntry(Object instance, String methodName, Object arg1, Object arg2) {
        traceEntryAllArgs(instance, methodName,
                "(" + arg1 + ", " + arg2 + ")");
    }

    public void traceEntry(Object instance, String methodName, int arg1, Object arg2) {
        traceEntryAllArgs(instance, methodName,
                "(" + arg1 + ", " + arg2 + ")");
    }

    public void traceEntry(Object instance, String methodName, int arg1, byte[] arg2) {
        traceEntryAllArgs(instance, methodName,
                "(" + arg1 + ", " + Utils.getStringFromBytes(arg2) + ")");
    }

    public void traceDeprecatedEntry(Object instance, String methodName, int arg1, int arg2) {
        traceDeprecatedEntryAllArgs(instance, methodName,
                "(" + arg1 + ", " + arg2 + ")");
    }

    public void traceDeprecatedEntry(Object instance, String methodName, Object arg1, int arg2) {
        traceDeprecatedEntryAllArgs(instance, methodName,
                "(" + arg1 + ", " + arg2 + ")");
    }

    public void traceEntry(Object instance, String methodName, int arg1, boolean arg2) {
        traceEntryAllArgs(instance, methodName,
                "(" + arg1 + ", " + arg2 + ")");
    }

    public void traceEntry(Object instance, String methodName, int arg1, byte arg2) {
        traceEntryAllArgs(instance, methodName,
                "(" + arg1 + ", 0x" + Integer.toHexString(arg2 & 0xff) + ")");
    }

    public void traceEntry(Object instance, String methodName, int arg1, short arg2) {
        traceEntryAllArgs(instance, methodName,
                "(" + arg1 + ", " + arg2 + ")");
    }

    public void traceEntry(Object instance, String methodName, int arg1, int arg2) {
        traceEntryAllArgs(instance, methodName,
                "(" + arg1 + ", " + arg2 + ")");
    }

    public void traceEntry(Object instance, String methodName, int arg1, long arg2) {
        traceEntryAllArgs(instance, methodName,
                "(" + arg1 + ", " + arg2 + ")");
    }

    public void traceEntry(Object instance, String methodName, int arg1, float arg2) {
        traceEntryAllArgs(instance, methodName,
                "(" + arg1 + ", " + arg2 + ")");
    }

    public void traceEntry(Object instance, String methodName, int arg1, double arg2) {
        traceEntryAllArgs(instance, methodName,
                "(" + arg1 + ", " + arg2 + ")");
    }

    public void traceEntry(Object instance, String methodName, Object arg1, boolean arg2) {
        traceEntryAllArgs(instance, methodName,
                "(" + arg1 + ", " + arg2 + ")");
    }

    public void traceEntry(Object instance, String methodName, Object arg1, byte arg2) {
        traceEntryAllArgs(instance, methodName,
                "(" + arg1 + ", 0x" + Integer.toHexString(arg2 & 0xff) + ")");
    }

    public void traceEntry(Object instance, String methodName, Object arg1, short arg2) {
        traceEntryAllArgs(instance, methodName,
                "(" + arg1 + ", " + arg2 + ")");
    }

    public void traceEntry(Object instance, String methodName, Object arg1, int arg2) {
        traceEntryAllArgs(instance, methodName,
                "(" + arg1 + ", " + arg2 + ")");
    }

    public void traceEntry(Object instance, String methodName, Object arg1, long arg2) {
        traceEntryAllArgs(instance, methodName,
                "(" + arg1 + ", " + arg2 + ")");
    }

    public void traceEntry(Object instance, String methodName, Object arg1, float arg2) {
        traceEntryAllArgs(instance, methodName,
                "(" + arg1 + ", " + arg2 + ")");
    }

    public void traceEntry(Object instance, String methodName, Object arg1, double arg2) {
        traceEntryAllArgs(instance, methodName,
                "(" + arg1 + ", " + arg2 + ")");
    }

    // ---------------------- trace entry of methods w/ 3 args -------------------

    public void traceEntry(Object instance, String methodName,
                           Object arg1, Object arg2, Object arg3) {
        traceEntryAllArgs(instance, methodName,
                "(" + arg1 + ", " + arg2 + ", " + arg3 + ")");
    }

    public void traceEntry(Object instance, String methodName,
                           int arg1, Object arg2, Object arg3) {
        traceEntryAllArgs(instance, methodName,
                "(" + arg1 + ", " + arg2 + ", " + arg3 + ")");
    }

    public void traceEntry(Object instance, String methodName,
                           Object arg1, Object arg2, int arg3) {
        traceEntryAllArgs(instance, methodName,
                "(" + arg1 + ", " + arg2 + ", " + arg3 + ")");
    }

    public void traceEntry(Object instance, String methodName,
                           int arg1, Object arg2, int arg3) {
        traceEntryAllArgs(instance, methodName,
                "(" + arg1 + ", " + arg2 + ", " + arg3 + ")");
    }

    public void traceDeprecatedEntry(Object instance, String methodName,
                                     int arg1, Object arg2, int arg3) {
        traceEntryAllArgs(instance, methodName,
                "(" + arg1 + ", " + arg2 + ", " + arg3 + ")");
    }

    public void traceEntry(Object instance, String methodName,
                           int arg1, int arg2, Object arg3) {
        traceEntryAllArgs(instance, methodName,
                "(" + arg1 + ", " + arg2 + ", " + arg3 + ")");
    }

    public void traceEntry(Object instance, String methodName,
                           int arg1, int arg2, int arg3) {
        traceEntryAllArgs(instance, methodName,
                "(" + arg1 + ", " + arg2 + ", " + arg3 + ")");
    }

    public void traceEntry(Object instance, String methodName,
                           Object arg1, int arg2, int arg3) {
        traceEntryAllArgs(instance, methodName,
                "(" + arg1 + ", " + arg2 + ", " + arg3 + ")");
    }

    public void traceEntry(Object instance, String methodName,
                           Object arg1, int arg2, Object arg3) {
        traceEntryAllArgs(instance, methodName,
                "(" + arg1 + ", " + arg2 + ", " + arg3 + ")");
    }

    public void traceEntry(Object instance, String methodName,
                           Object arg1, boolean arg2, boolean arg3) {
        traceEntryAllArgs(instance, methodName,
                "(" + arg1 + ", " + arg2 + ", " + arg3 + ")");
    }

    public void traceEntry(Object instance, String methodName,
                           Object arg1, boolean arg2, int arg3) {
        traceEntryAllArgs(instance, methodName,
                "(" + arg1 + ", " + arg2 + ", " + arg3 + ")");
    }

    // ---------------------- trace entry of methods w/ 4 args -------------------

    public void traceEntry(Object instance, String methodName,
                           Object arg1, Object arg2, Object arg3, Object arg4) {
        traceEntryAllArgs(instance, methodName,
                "(" + arg1 + ", " + arg2 + ", " + arg3 + ", " + arg4 + ")");
    }

    public void traceEntry(Object instance, String methodName,
                           int arg1, Object arg2, Object arg3, Object arg4) {
        traceEntryAllArgs(instance, methodName,
                "(" + arg1 + ", " + arg2 + ", " + arg3 + ", " + arg4 + ")");
    }

    public void traceEntry(Object instance, String methodName,
                           int arg1, Object arg2, int arg3, int arg4) {
        traceEntryAllArgs(instance, methodName,
                "(" + arg1 + ", " + arg2 + ", " + arg3 + ", " + arg4 + ")");
    }

    public void traceEntry(Object instance, String methodName,
                           Object arg1, int arg2, int arg3, int arg4) {
        traceEntryAllArgs(instance, methodName,
                "(" + arg1 + ", " + arg2 + ", " + arg3 + ", " + arg4 + ")");
    }

    public void traceEntry(Object instance, String methodName,
                           Object arg1, Object arg2, int arg3, int arg4) {
        traceEntryAllArgs(instance, methodName,
                "(" + arg1 + ", " + arg2 + ", " + arg3 + ", " + arg4 + ")");
    }

    // ---------------------- trace entry of methods w/ 5 args -------------------

    public void traceEntry(Object instance, String methodName,
                           Object arg1, Object arg2, Object arg3, int arg4, boolean arg5) {
        traceEntryAllArgs(instance, methodName,
                "(" + arg1 + ", " + arg2 + ", " + arg3 + ", " + arg4 + ", " + arg5 + ")");
    }

    public void traceEntry(Object instance, String methodName,
                           Object arg1, Object arg2, Object arg3, boolean arg4, boolean arg5) {
        traceEntryAllArgs(instance, methodName,
                "(" + arg1 + ", " + arg2 + ", " + arg3 + ", " + arg4 + ", " + arg5 + ")");
    }

    // ---------------------- trace entry of methods w/ 6 args -------------------

    public void traceEntry(Object instance, String methodName,
                           Object arg1, Object arg2, Object arg3, Object arg4, Object arg5, Object arg6) {
        traceEntryAllArgs(instance, methodName,
                "(" + arg1 + ", " + arg2 + ", " + arg3 + ", " + arg4 + ", " + arg5 + ", " + arg6 + ")");
    }

    // ---------------------------tracing exceptions and warnings-----------------

    public void traceDiagnosable(SqlException e) {
        if (!loggingEnabled(ClientDataSourceInterface.TRACE_DIAGNOSTICS)) {
            return;
        }
        synchronized (printWriter_) {
            dncprintln("BEGIN TRACE_DIAGNOSTICS");
            ExceptionFormatter.printTrace(e, printWriter_, "[derby]", true); // true means return tokens only
            dncprintln("END TRACE_DIAGNOSTICS");
        }
    }
    public void traceDiagnosable(java.sql.SQLException e) {
        if (!loggingEnabled(ClientDataSourceInterface.TRACE_DIAGNOSTICS)) {
            return;
        }
        synchronized (printWriter_) {
            dncprintln("BEGIN TRACE_DIAGNOSTICS");
            ExceptionFormatter.printTrace(e, printWriter_, "[derby]", true); // true means return tokens only
            dncprintln("END TRACE_DIAGNOSTICS");
        }
    }

    public void traceDiagnosable(javax.transaction.xa.XAException e) {
        if (!loggingEnabled(ClientDataSourceInterface.TRACE_DIAGNOSTICS)) {
            return;
        }
        synchronized (printWriter_) {
            dncprintln("BEGIN TRACE_DIAGNOSTICS");
            ExceptionFormatter.printTrace(e, printWriter_, "[derby]");
            dncprintln("END TRACE_DIAGNOSTICS");
        }
    }
    // ------------------------ meta data tracing --------------------------------

    public void traceParameterMetaData(Statement statement, ColumnMetaData columnMetaData) {
        if (!loggingEnabled(
                ClientDataSourceInterface.TRACE_PARAMETER_META_DATA) ||
                columnMetaData == null) {
            return;
        }
        synchronized (printWriter_) {
            String header = "[ParameterMetaData@" + Integer.toHexString(columnMetaData.hashCode()) + "]";
            try {
                dncprintln(header, "BEGIN TRACE_PARAMETER_META_DATA");
                dncprintln(header, "Parameter meta data for statement Statement@" + Integer.toHexString(statement.hashCode()));
                dncprintln(header, "Number of parameter columns: " + columnMetaData.getColumnCount());
                traceColumnMetaData(header, columnMetaData);
                dncprintln(header, "END TRACE_PARAMETER_META_DATA");
            } catch (SQLException e) {
                dncprintln(header, "Encountered an SQL exception while trying to trace parameter meta data");
                dncprintln(header, "END TRACE_PARAMETER_META_DATA");
            }
        }
    }

    public void traceResultSetMetaData(Statement statement, ColumnMetaData columnMetaData) {
        if (!loggingEnabled(
                ClientDataSourceInterface.TRACE_RESULT_SET_META_DATA) ||
                columnMetaData == null) {
            return;
        }
        synchronized (printWriter_) {
            String header = "[ResultSetMetaData@" + Integer.toHexString(columnMetaData.hashCode()) + "]";
            try {
                dncprintln(header, "BEGIN TRACE_RESULT_SET_META_DATA");
                dncprintln(header, "Result set meta data for statement Statement@" + Integer.toHexString(statement.hashCode()));
                dncprintln(header, "Number of result set columns: " + columnMetaData.getColumnCount());
                traceColumnMetaData(header, columnMetaData);
                dncprintln(header, "END TRACE_RESULT_SET_META_DATA");
            } catch (SQLException e) {
                dncprintln(header, "Encountered an SQL exception while trying to trace result set meta data");
                dncprintln(header, "END TRACE_RESULT_SET_META_DATA");
            }
        }
    }

    //-----------------------------transient state--------------------------------

    private void traceColumnMetaData(String header, ColumnMetaData columnMetaData) {
        try {
            synchronized (printWriter_) {

                for (int column = 1; column <= columnMetaData.getColumnCount(); column++) {
                    dncprint(header, "Column " + column + ": { ");
                    printWriter_.print("label=" + columnMetaData.getColumnLabel(column) + ", ");
                    printWriter_.print("name=" + columnMetaData.getColumnName(column) + ", ");
                    printWriter_.print("type name=" + columnMetaData.getColumnTypeName(column) + ", ");
                    printWriter_.print("type=" + columnMetaData.getColumnType(column) + ", ");
                    printWriter_.print("nullable=" + columnMetaData.isNullable(column) + ", ");
                    printWriter_.print("precision=" + columnMetaData.getPrecision(column) + ", ");
                    printWriter_.print("scale=" + columnMetaData.getScale(column) + ", ");
                    printWriter_.print("schema name=" + columnMetaData.getSchemaName(column) + ", ");
                    printWriter_.print("table name=" + columnMetaData.getTableName(column) + ", ");
                    printWriter_.print("writable=" + columnMetaData.isWritable(column) + ", ");
                    printWriter_.print("sqlPrecision=" + (columnMetaData.sqlPrecision_ == null ? "<null>" : "" + columnMetaData.sqlPrecision_[column - 1]) + ", ");
                    printWriter_.print("sqlScale=" + (columnMetaData.sqlScale_ == null ? "<null>" : "" + columnMetaData.sqlScale_[column - 1]) + ", ");
                    printWriter_.print("sqlLength=" + (columnMetaData.sqlLength_ == null ? "<null>" : "" + columnMetaData.sqlLength_[column - 1]) + ", ");
                    printWriter_.print("sqlType=" + (columnMetaData.sqlType_ == null ? "<null>" : "" + columnMetaData.sqlType_[column - 1]) + ", ");
                    printWriter_.print("sqlCcsid=" + (columnMetaData.sqlCcsid_ == null ? "<null>" : "" + columnMetaData.sqlCcsid_[column - 1]) + ", ");
                    printWriter_.print("sqlName=" + (columnMetaData.sqlName_ == null ? "<null>" : columnMetaData.sqlName_[column - 1]) + ", ");
                    printWriter_.print("sqlLabel=" + (columnMetaData.sqlLabel_ == null ? "<null>" : columnMetaData.sqlLabel_[column - 1]) + ", ");
                    printWriter_.print("sqlUnnamed=" + (columnMetaData.sqlUnnamed_ == null ? "<null>" : "" + columnMetaData.sqlUnnamed_[column - 1]) + ", ");
                    printWriter_.print("sqlComment=" + (columnMetaData.sqlComment_ == null ? "<null>" : columnMetaData.sqlComment_[column - 1]) + ", ");
                    printWriter_.print("sqlxKeymem=" + (columnMetaData.sqlxKeymem_ == null ? "<null>" : "" + columnMetaData.sqlxKeymem_[column - 1]) + ", ");
                    printWriter_.print("sqlxGenerated=" + (columnMetaData.sqlxGenerated_ == null ? "<null>" : "" + columnMetaData.sqlxGenerated_[column - 1]) + ", ");
                    printWriter_.print("sqlxParmmode=" + (columnMetaData.sqlxParmmode_ == null ? "<null>" : "" + columnMetaData.sqlxParmmode_[column - 1]) + ", ");
                    printWriter_.print("sqlxCorname=" + (columnMetaData.sqlxCorname_ == null ? "<null>" : columnMetaData.sqlxCorname_[column - 1]) + ", ");
                    printWriter_.print("sqlxName=" + (columnMetaData.sqlxName_ == null ? "<null>" : columnMetaData.sqlxName_[column - 1]) + ", ");
                    printWriter_.print("sqlxBasename=" + (columnMetaData.sqlxBasename_ == null ? "<null>" : columnMetaData.sqlxBasename_[column - 1]) + ", ");
                    printWriter_.print("sqlxUpdatable=" + (columnMetaData.sqlxUpdatable_ == null ? "<null>" : "" + columnMetaData.sqlxUpdatable_[column - 1]) + ", ");
                    printWriter_.print("sqlxSchema=" + (columnMetaData.sqlxSchema_ == null ? "<null>" : columnMetaData.sqlxSchema_[column - 1]) + ", ");
                    printWriter_.print("sqlxRdbnam=" + (columnMetaData.sqlxRdbnam_ == null ? "<null>" : columnMetaData.sqlxRdbnam_[column - 1]) + ", ");
                    printWriter_.print("internal type=" + columnMetaData.types_[column - 1] + ", ");
                    printWriter_.println(" }");
                }
                dncprint(header, "{ ");
                printWriter_.print("sqldHold=" + columnMetaData.sqldHold_ + ", ");
                printWriter_.print("sqldReturn=" + columnMetaData.sqldReturn_ + ", ");
                printWriter_.print("sqldScroll=" + columnMetaData.sqldScroll_ + ", ");
                printWriter_.print("sqldSensitive=" + columnMetaData.sqldSensitive_ + ", ");
                printWriter_.print("sqldFcode=" + columnMetaData.sqldFcode_ + ", ");
                printWriter_.print("sqldKeytype=" + columnMetaData.sqldKeytype_ + ", ");
                printWriter_.print("sqldRdbnam=" + columnMetaData.sqldRdbnam_ + ", ");
                printWriter_.print("sqldSchema=" + columnMetaData.sqldSchema_);
                printWriter_.println(" }");
                printWriter_.flush();
            }
        } catch (SQLException e) {
            dncprintln(header, "Encountered an SQL exception while trying to trace column meta data");
        }
    }

    // ---------------------- 3-way tracing connects -----------------------------
    // Including protocol manager levels, and driver configuration

    // Jdbc 2
    public void traceConnectEntry(ClientBaseDataSourceRoot dataSource) {
        if (loggingEnabled(
                ClientDataSourceInterface.TRACE_DRIVER_CONFIGURATION)) {
            traceDriverConfigurationJdbc2();
        }
        if (loggingEnabled(ClientDataSourceInterface.TRACE_CONNECTS)) {
            traceConnectsEntry(dataSource);
        }
    }

    // Jdbc 1
    public void traceConnectEntry(String server,
                                  int port,
                                  String database,
                                  java.util.Properties properties) {
        if (loggingEnabled(
                ClientDataSourceInterface.TRACE_DRIVER_CONFIGURATION)) {
            traceDriverConfigurationJdbc1();
        }
        if (loggingEnabled(ClientDataSourceInterface.TRACE_CONNECTS)) {
            traceConnectsEntry(server, port, database, properties);
        }
    }

    public void traceConnectResetEntry(
            Object instance, LogWriter logWriter,
            String user, ClientBaseDataSourceRoot ds) {

        traceEntry(instance, "reset", logWriter, user, "<escaped>", ds);
        if (loggingEnabled(ClientDataSourceInterface.TRACE_CONNECTS)) {
            traceConnectsResetEntry(ds);
        }
    }

    public void traceConnectExit(Connection connection) {
        if (loggingEnabled(ClientDataSourceInterface.TRACE_CONNECTS)) {
            traceConnectsExit(connection);
        }
    }

    public void traceConnectResetExit(Connection connection) {
        if (loggingEnabled(ClientDataSourceInterface.TRACE_CONNECTS)) {
            traceConnectsResetExit(connection);
        }
    }


    // ---------------------- tracing connects -----------------------------------

    private void traceConnectsResetEntry(ClientBaseDataSourceRoot dataSource) {
        try {
            traceConnectsResetEntry(dataSource.getServerName(),
                    dataSource.getPortNumber(),
                    dataSource.getDatabaseName(),
                    getProperties(dataSource));
        } catch ( SqlException se ) {
            dncprintln("Encountered an SQL exception while trying to trace connection reset entry");
        }
    }

    private void traceConnectsEntry(ClientBaseDataSourceRoot dataSource) {
        try {
            traceConnectsEntry(dataSource.getServerName(),
                    dataSource.getPortNumber(),
                    dataSource.getDatabaseName(),
                    getProperties(dataSource));
        } catch ( SqlException se ) {
            dncprintln("Encountered an SQL exception while trying to trace connection entry");
        }
        
    }

    private void traceConnectsResetEntry(String server,
                                         int port,
                                         String database,
                                         java.util.Properties properties) {
        dncprintln("BEGIN TRACE_CONNECT_RESET");
        dncprintln("Connection reset requested for " + server + ":" + port + "/" + database);
        dncprint("Using properties: ");
        writeProperties(properties);
        dncprintln("END TRACE_CONNECT_RESET");
    }

    private void traceConnectsEntry(String server,
                                    int port,
                                    String database,
                                    java.util.Properties properties) {
        synchronized (printWriter_) {
            dncprintln("BEGIN TRACE_CONNECTS");
            dncprintln("Attempting connection to " + server + ":" + port + "/" + database);
            dncprint("Using properties: ");
            writeProperties(properties);
            dncprintln("END TRACE_CONNECTS");
        }
    }

    // Specialized by NetLogWriter.traceConnectsExit()
    public void traceConnectsExit(Connection c) {
        synchronized (printWriter_) {
            String header = "[Connection@" + Integer.toHexString(c.hashCode()) + "]";
            try {
                dncprintln(header, "BEGIN TRACE_CONNECTS");
                dncprintln(header, "Successfully connected to server " + c.databaseMetaData_.getURL());
                dncprintln(header, "User: " + c.databaseMetaData_.getUserName());
                dncprintln(header, "Database product name: " + c.databaseMetaData_.getDatabaseProductName());
                dncprintln(header, "Database product version: " + c.databaseMetaData_.getDatabaseProductVersion());
                dncprintln(header, "Driver name: " + c.databaseMetaData_.getDriverName());
                dncprintln(header, "Driver version: " + c.databaseMetaData_.getDriverVersion());
                dncprintln(header, "END TRACE_CONNECTS");
            } catch (java.sql.SQLException e) {
                dncprintln(header, "Encountered an SQL exception while trying to trace connection exit");
                dncprintln(header, "END TRACE_CONNECTS");
            }
        }
    }

    public void traceConnectsResetExit(org.apache.derby.client.am.Connection c) {
        synchronized (printWriter_) {
            String header = "[Connection@" + Integer.toHexString(c.hashCode()) + "]";
            try {
                dncprintln(header, "BEGIN TRACE_CONNECT_RESET");
                dncprintln(header, "Successfully reset connection to server " + c.databaseMetaData_.getURL());
                dncprintln(header, "User: " + c.databaseMetaData_.getUserName());
                dncprintln(header, "Database product name: " + c.databaseMetaData_.getDatabaseProductName());
                dncprintln(header, "Database product version: " + c.databaseMetaData_.getDatabaseProductVersion());
                dncprintln(header, "Driver name: " + c.databaseMetaData_.getDriverName());
                dncprintln(header, "Driver version: " + c.databaseMetaData_.getDriverVersion());
                dncprintln(header, "END TRACE_CONNECT_RESET");
            } catch (java.sql.SQLException e) {
                dncprintln(header, "Encountered an SQL exception while trying to trace connection reset exit");
                dncprintln(header, "END TRACE_CONNECT_RESET");
            }
        }
    }


    // properties.toString() will print out passwords,
    // so this method was written to escape the password property value.
    // printWriter_ synchronized by caller.
    private void writeProperties(java.util.Properties properties) {
        printWriter_.print("{ ");
        for (java.util.Iterator i = properties.entrySet().iterator(); i.hasNext();) {
            java.util.Map.Entry e = (java.util.Map.Entry) (i.next());
            if ("password".equals(e.getKey())) {
                printWriter_.print("password=" + escapePassword((String) e.getValue()));
            } else {
                printWriter_.print(e.getKey() + "=" + e.getValue());
            }
            if (i.hasNext()) {
                printWriter_.print(", ");
            }
        }
        printWriter_.println(" }");
        printWriter_.flush();
    }

    private String escapePassword(String pw) {
        StringBuilder sb = new StringBuilder(pw);
        for (int j = 0; j < pw.length(); j++) {
            sb.setCharAt(j, '*');
        }
        return sb.toString();
    }
    //-------------------------tracing driver configuration-----------------------

    private void traceDriverConfigurationJdbc2() {
        synchronized (printWriter_) {
            if (!driverConfigurationHasBeenWrittenToJdbc2Stream_) {
                writeDriverConfiguration();
                driverConfigurationHasBeenWrittenToJdbc2Stream_ = true;
            }
        }
    }

    private void traceDriverConfigurationJdbc1() {
        synchronized (printWriter_) {
            if (!driverConfigurationHasBeenWrittenToJdbc1Stream_) {
                writeDriverConfiguration();
                driverConfigurationHasBeenWrittenToJdbc1Stream_ = true;
            }
        }
    }

    public void writeDriverConfiguration() {
        org.apache.derby.client.am.Version.writeDriverConfiguration(printWriter_);
    }

    public static java.io.PrintWriter getPrintWriter(final String fileName, final boolean fileAppend) throws SqlException {
        java.io.PrintWriter printWriter = null;
        //Using an anonymous class to deal with the PrintWriter because the  
        //method java.security.AccessController.doPrivileged requires an 
        //instance of a class(which implements 
        //java.security.PrivilegedExceptionAction). Since getPrintWriter method
        //is static, we can't simply pass "this" to doPrivileged method and 
        //have LogWriter implement PrivilegedExceptionAction.
        //To get around the static nature of method getPrintWriter, have an
        //anonymous class implement PrivilegedExceptionAction. That class will 
        //do the work related to PrintWriter in it's run method and return 
        //PrintWriter object.
        try {
            printWriter = AccessController.doPrivileged(
                new PrivilegedExceptionAction<PrintWriter>(){
                    public PrintWriter run() throws IOException {
                        String fileCanonicalPath = new java.io.File(fileName).getCanonicalPath();
                        return new java.io.PrintWriter(
                                new java.io.BufferedOutputStream(
                                        new java.io.FileOutputStream(
                                                fileCanonicalPath, fileAppend), 4096), true);
                        }
                    });
        } catch (java.security.PrivilegedActionException pae) {
            throw new SqlException(null, 
                new ClientMessageId(SQLState.UNABLE_TO_OPEN_FILE),
                new Object[] { fileName, pae.getMessage() },
                pae);
        }
        return printWriter;
    }
    
    /**
     * Obtain a set of Properties for the client data source.
     */
    private Properties getProperties(ClientBaseDataSourceRoot cds)
    throws SqlException {
        
        Properties properties = ClientBaseDataSourceRoot.getProperties(cds);

        if (properties.getProperty(Attribute.PASSWORD_ATTR) != null) {
            properties.setProperty(Attribute.PASSWORD_ATTR, "********");
        }

        return properties;
    }
}
