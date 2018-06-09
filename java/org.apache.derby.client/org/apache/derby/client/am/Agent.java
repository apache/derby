/*

   Derby - Class org.apache.derby.client.am.Agent

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
import java.sql.Types;
import org.apache.derby.client.ClientAutoloadedDriver;
import org.apache.derby.shared.common.reference.JDBC40Translation;
import org.apache.derby.shared.common.reference.SQLState;

public abstract class Agent {
    SqlException accumulatedReadExceptions_ = null;

    private boolean enableBatchedExceptionTracking_;
    private int batchedExceptionLabelIndex_;
    private boolean[] batchedExceptionGenerated_;

    ClientConnection connection_; // made friendly for lobs only, refactor !!

    SectionManager sectionManager_ = null;

    public LogWriter logWriter_ = null;

    final CrossConverters crossConverters_;

    // Exceptions that occur on dnc's implementation of SqlException.getMessage() via stored proc
    // cannot be thrown on the getMessage() invocation because the signature of getMessage() does not
    // allow for throwing an exception.
    // Therefore, we must save the exception and throw it at our very first opportunity.
    private SqlException deferredException_;

    void checkForDeferredExceptions() throws SqlException {
        if (deferredException_ != null) {
            SqlException temp = deferredException_;
            deferredException_ = null;
            throw temp;
        }
    }

    /**
     * Checks whether a data type is supported for
     * <code>setObject(int, Object, int)</code> and
     * <code>setObject(int, Object, int, int)</code>.
     *
     * @param dataType the data type to check
     * @exception SqlException if the type is not supported
     */
    void checkForSupportedDataType(int dataType) throws SqlException {

        // JDBC 4.0 javadoc for setObject() says:
        //
        // Throws: (...) SQLFeatureNotSupportedException - if
        // targetSqlType is a ARRAY, BLOB, CLOB, DATALINK,
        // JAVA_OBJECT, NCHAR, NCLOB, NVARCHAR, LONGNVARCHAR, REF,
        // ROWID, SQLXML or STRUCT data type and the JDBC driver does
        // not support this data type
        //
        // Of these types, we only support BLOB, CLOB and
        // (sort of) JAVA_OBJECT.

        switch (dataType) {
        case Types.ARRAY:
        case Types.DATALINK:
        case Types.DISTINCT:
        case Types.NCHAR:
        case Types.NCLOB:
        case Types.NVARCHAR:
        case Types.LONGNVARCHAR:
        case Types.NULL:
        case Types.OTHER:
        case Types.REF:
        case JDBC40Translation.REF_CURSOR:
        case Types.ROWID:
        case Types.SQLXML:
        case Types.STRUCT:
            throw new SqlException
                (logWriter_,
                 new ClientMessageId(SQLState.DATA_TYPE_NOT_SUPPORTED),
                 ClientTypes.getTypeString(dataType));
        }
    }

    void accumulateDeferredException(SqlException e) {
        if (deferredException_ == null) {
            deferredException_ = e;
        } else {
            deferredException_.setNextException(e);
        }
    }

    protected Agent(ClientConnection connection, LogWriter logWriter) {
        connection_ = connection;
        logWriter_ = logWriter;
        crossConverters_ = new CrossConverters(this);
    }

    private void resetAgent(LogWriter logWriter) {
        // sectionManager_ is set elsewhere
        accumulatedReadExceptions_ = null;
        enableBatchedExceptionTracking_ = false;
        batchedExceptionLabelIndex_ = 0;
        batchedExceptionGenerated_ = null;
        logWriter_ = logWriter;
        deferredException_ = null;
    }

    void resetAgent(
        ClientConnection connection,
        LogWriter logWriter,
        int loginTimeout,
        String server,
        int port) throws SqlException {

        resetAgent(logWriter);
        resetAgent_(logWriter, loginTimeout, server, port);
    }

    abstract protected void resetAgent_(LogWriter logWriter, int loginTimeout, String server, int port) throws SqlException;

    //-------------------- entry points ------------------------------------------

    public final boolean loggingEnabled() {
        return logWriter_ != null;
    }

    public final void setLogWriter(LogWriter logWriter) {
        synchronized (connection_) {
            if (logWriter_ != null) {
                logWriter_.close();
            }
            logWriter_ = logWriter;
        }
    }

    public final PrintWriter getLogWriter() {
        return (logWriter_ == null) ? null : logWriter_.printWriter_;
    }

    //----------------------------------------------------------------------------


    public final void accumulateReadException(SqlException e) {
        if (enableBatchedExceptionTracking_) {
            batchedExceptionGenerated_[batchedExceptionLabelIndex_] = true;
            labelAsBatchedException(e, batchedExceptionLabelIndex_);
        }
        if (accumulatedReadExceptions_ == null) {
            accumulatedReadExceptions_ = e;
        } else {
            accumulatedReadExceptions_.setNextException(e);
        }
    }

    // Called only for disconnect event
    private void accumulateDisconnectException(DisconnectException e) {
        if (enableBatchedExceptionTracking_) {
            batchedExceptionGenerated_[batchedExceptionLabelIndex_] = true;
            labelAsBatchedException(e, batchedExceptionLabelIndex_);
        }
        if (accumulatedReadExceptions_ != null) {
            e.setNextException(accumulatedReadExceptions_);
        }

        accumulatedReadExceptions_ = null;
    }

    // For now, it looks like the only time we accumulate chain breaking exceptions
    // is for disconnect exceptions.
    public final void accumulateChainBreakingReadExceptionAndThrow(DisconnectException e) throws DisconnectException {
        accumulateDisconnectException(e); // tacks disconnect exc to end of chain
        markChainBreakingException_(); // sets a severity code in the NET agent
        throw e; // disconnect will be caught in Reply classes, and front of original chain thrown
    }

    abstract protected void markChainBreakingException_();

    abstract public void checkForChainBreakingException_() throws SqlException;

    private final void enableBatchedExceptionTracking(int batchSize) {
        enableBatchedExceptionTracking_ = true;
        batchedExceptionGenerated_ = new boolean[batchSize];
        batchedExceptionLabelIndex_ = 0;
    }

    final void disableBatchedExceptionTracking() {
        enableBatchedExceptionTracking_ = false;
    }

    public final void setBatchedExceptionLabelIndex(int index) {
        batchedExceptionLabelIndex_ = index;
    }

    private final SqlException labelAsBatchedException(SqlException e, int index) {
        SqlException firstInChain = e;
        while (e != null) {
            e.setBatchPositionLabel(index);
            e = (SqlException) e.getNextException();
        }
        return firstInChain;
    }

    protected final void checkForExceptions() throws SqlException {
        if (accumulatedReadExceptions_ != null) {
            SqlException e = accumulatedReadExceptions_;
            accumulatedReadExceptions_ = null;
            throw e;
        }
    }

    public final void flow(ClientStatement statement) throws SqlException {
        endWriteChain();
        flush_();
        beginReadChain(statement);
    }

    final void flowBatch(ClientStatement statement, int batchSize)
            throws SqlException {
        endBatchedWriteChain();
        flush_();
        beginBatchedReadChain(statement, batchSize);
    }

    public final void flowOutsideUOW() throws SqlException {
        endWriteChain();
        flush_();
        beginReadChainOutsideUOW();
    }

    // flush() means to send all chained requests.
    abstract public void flush_() throws DisconnectException;

    // Close client resources associated with this agent, such as socket and streams for the net.
    abstract public void close_() throws SqlException;

    public void close() throws SqlException {
        close_();
        if (logWriter_ != null) {
            logWriter_.close();
        }
    }

    final void disconnectEvent() {
        // closes client-side resources associated with database connection
        try {
            close();
        } catch (SqlException doNothing) {
        }
        connection_.completeChainBreakingDisconnect();
    }
    
    abstract public void beginWriteChainOutsideUOW() throws SqlException;

    public void beginWriteChain(ClientStatement statement) throws SqlException {
        connection_.writeTransactionStart(statement);
    }

    final void beginBatchedWriteChain(ClientStatement statement)
            throws SqlException {
        beginWriteChain(statement);
    }

    abstract protected void endWriteChain();

    private final void endBatchedWriteChain() {
        endWriteChain();
    }

    protected void beginReadChain(ClientStatement statement)
            throws SqlException {
        connection_.readTransactionStart();
    }

    private void beginBatchedReadChain(
        ClientStatement statement,
        int batchSize)
            throws SqlException {

        enableBatchedExceptionTracking(batchSize);
        beginReadChain(statement);
    }

    protected void beginReadChainOutsideUOW() throws SqlException {
    }

    public void endReadChain() throws SqlException {
        checkForExceptions();
    }

    final void endBatchedReadChain(long[] updateCounts,
                                   SqlException accumulatedExceptions)
            throws BatchUpdateException {

        disableBatchedExceptionTracking();
        for (int i = 0; i < batchedExceptionGenerated_.length; i++) {
            if (batchedExceptionGenerated_[i]) {
                updateCounts[i] = -3;
            }
        }
        if (accumulatedExceptions == null) {
            try {
                endReadChain();
            } catch (SqlException e) {
                accumulatedExceptions = e;
            }
        }
        if (accumulatedExceptions != null) {
            throw ClientAutoloadedDriver.getFactory().newBatchUpdateException(logWriter_,
                new ClientMessageId(SQLState.BATCH_NON_ATOMIC_FAILURE),
                null, updateCounts, accumulatedExceptions);
        }
    }
}


