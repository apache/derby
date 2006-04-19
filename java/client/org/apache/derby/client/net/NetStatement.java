/*

   Derby - Class org.apache.derby.client.net.NetStatement

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

package org.apache.derby.client.net;

import org.apache.derby.client.am.ColumnMetaData;
import org.apache.derby.client.am.Section;
import org.apache.derby.client.am.SqlException;
import org.apache.derby.client.am.Statement;
import org.apache.derby.jdbc.ClientDriver;

public class NetStatement implements org.apache.derby.client.am.MaterialStatement {

    Statement statement_;


    // Alias for (NetConnection) statement_.connection
    NetConnection netConnection_;

    // Alias for (NetAgent) statement_.agent
    NetAgent netAgent_;


    // If qryrowset is sent on opnqry then it also needs to be sent on every subsequent cntqry.
    public boolean qryrowsetSentOnOpnqry_ = false;

    //---------------------constructors/finalizer---------------------------------

    private NetStatement() {
        initNetStatement();
    }

    private void resetNetStatement() {
        initNetStatement();
    }

    private void initNetStatement() {
        qryrowsetSentOnOpnqry_ = false;
    }

    // Relay constructor for NetPreparedStatement.
    NetStatement(org.apache.derby.client.am.Statement statement, NetAgent netAgent, NetConnection netConnection) {
        this();
        initNetStatement(statement, netAgent, netConnection);
    }

    void resetNetStatement(org.apache.derby.client.am.Statement statement, NetAgent netAgent, NetConnection netConnection) {
        resetNetStatement();
        initNetStatement(statement, netAgent, netConnection);
    }

    private void initNetStatement(org.apache.derby.client.am.Statement statement, NetAgent netAgent, NetConnection netConnection) {
        netAgent_ = netAgent;
        netConnection_ = netConnection;
        statement_ = statement;
        statement_.materialStatement_ = this;
    }

    // Called by abstract Connection.createStatement().newStatement() for jdbc 1 statements
    NetStatement(NetAgent netAgent, NetConnection netConnection) throws SqlException {
        this(ClientDriver.getFactory().newStatement(netAgent, netConnection),
                netAgent,
                netConnection);
    }

    void netReset(NetAgent netAgent, NetConnection netConnection) throws SqlException {
        statement_.resetStatement(netAgent, netConnection);
        resetNetStatement(statement_, netAgent, netConnection);
    }

    public void reset_() {
        qryrowsetSentOnOpnqry_ = false;
    }

    // Called by abstract Connection.createStatement().newStatement() for jdbc 2 statements with scroll attributes
    NetStatement(NetAgent netAgent, NetConnection netConnection, int type, int concurrency, int holdability) throws SqlException {
        this(ClientDriver.getFactory().newStatement(netAgent, netConnection, type, concurrency, holdability, java.sql.Statement.NO_GENERATED_KEYS, null),
                netAgent,
                netConnection);
    }

    void resetNetStatement(NetAgent netAgent, NetConnection netConnection, int type, int concurrency, int holdability) throws SqlException {
        statement_.resetStatement(netAgent, netConnection, type, concurrency, holdability, java.sql.Statement.NO_GENERATED_KEYS, null);
        resetNetStatement(statement_, netAgent, netConnection);
    }

    protected void finalize() throws java.lang.Throwable {
        super.finalize();
    }

    // ------------------------abstract box car methods-----------------------------------------------

    public void writeSetSpecialRegister_(java.util.ArrayList sqlsttList) throws SqlException {
        netAgent_.statementRequest_.writeSetSpecialRegister(sqlsttList);
    }

    public void readSetSpecialRegister_() throws SqlException {
        netAgent_.statementReply_.readSetSpecialRegister(statement_);
    }

    public void writeExecuteImmediate_(String sql,
                                       Section section) throws SqlException {
        netAgent_.statementRequest_.writeExecuteImmediate(this, sql, section);
    }

    public void readExecuteImmediate_() throws SqlException {
        netAgent_.statementReply_.readExecuteImmediate(statement_);
    }

    // NOTE: NET processing does not require parameters supplied on the "read-side" so parameter sql is ignored.
    public void readExecuteImmediateForBatch_(String sql) throws SqlException {
        readExecuteImmediate_();
    }

    public void writePrepareDescribeOutput_(String sql,
                                            Section section) throws SqlException {
        netAgent_.statementRequest_.writePrepareDescribeOutput(this, sql, section);
    }

    public void readPrepareDescribeOutput_() throws SqlException {
        netAgent_.statementReply_.readPrepareDescribeOutput(statement_);
    }

    public void writeOpenQuery_(Section section,
                                int fetchSize,
                                int resultSetType)
            throws SqlException {
        netAgent_.statementRequest_.writeOpenQuery(this,
                section,
                fetchSize,
                resultSetType);
    }

    public void readOpenQuery_() throws SqlException {
        netAgent_.statementReply_.readOpenQuery(statement_);
    }

    public void writeExecuteCall_(boolean outputExpected,
                                  String procedureName,
                                  Section section,
                                  int fetchSize,
                                  boolean suppressResultSets,
                                  int resultSetType,
                                  ColumnMetaData parameterMetaData,
                                  Object[] inputs) throws SqlException {
        netAgent_.statementRequest_.writeExecuteCall(this,
                outputExpected,
                procedureName,
                section,
                fetchSize,
                suppressResultSets,
                resultSetType,
                parameterMetaData,
                inputs);
    }

    public void readExecuteCall_() throws SqlException {
        netAgent_.statementReply_.readExecuteCall(statement_);
    }

    public void writePrepare_(String sql, Section section) throws SqlException {
        netAgent_.statementRequest_.writePrepare(this, sql, section);
    }

    public void readPrepare_() throws SqlException {
        netAgent_.statementReply_.readPrepare(statement_);
    }

    public void markClosedOnServer_() {
    }
}
