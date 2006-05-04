/*

   Derby - Class org.apache.derby.client.net.NetCallableStatement

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

import org.apache.derby.client.am.CallableStatement;
import org.apache.derby.client.am.ColumnMetaData;
import org.apache.derby.client.am.MaterialPreparedStatement;
import org.apache.derby.client.am.Section;
import org.apache.derby.client.am.SqlException;
import org.apache.derby.jdbc.ClientDriver;
import org.apache.derby.client.am.ClientJDBCObjectFactory;
import org.apache.derby.client.ClientPooledConnection;

public class NetCallableStatement extends NetPreparedStatement
        implements MaterialPreparedStatement {

    CallableStatement callableStatement_;

    //-----------------------------state------------------------------------------

    //---------------------constructors/finalizer---------------------------------

    private void initNetCallableStatement() {
        callableStatement_ = null;
    }

    // Relay constructor for all NetCallableStatement constructors
    NetCallableStatement(CallableStatement statement,
                         NetAgent netAgent,
                         NetConnection netConnection) throws SqlException {
        super(statement, netAgent, netConnection);
        initNetCallableStatement();
        initNetCallableStatement(statement);
    }

    void resetNetCallableStatement(CallableStatement statement,
                                   NetAgent netAgent,
                                   NetConnection netConnection) throws SqlException {
        super.resetNetPreparedStatement(statement, netAgent, netConnection);
        initNetCallableStatement();
        initNetCallableStatement(statement);
    }

    private void initNetCallableStatement(CallableStatement statement) throws SqlException {
        callableStatement_ = statement;
        callableStatement_.materialCallableStatement_ = this;

    }


    // Called by abstract Connection.prepareCall().newCallableStatement()
    // for jdbc 2 callable statements with scroll attributes.
    NetCallableStatement(NetAgent netAgent,
                         NetConnection netConnection,
                         String sql,
                         int type,
                         int concurrency,
                         int holdability,
                         ClientPooledConnection cpc) throws SqlException {
        this(ClientDriver.getFactory().newCallableStatement(netAgent,
                netConnection, sql, type, concurrency, holdability,cpc),
                netAgent,
                netConnection);
    }

    void resetNetCallableStatement(NetAgent netAgent,
                                   NetConnection netConnection,
                                   String sql,
                                   int type,
                                   int concurrency,
                                   int holdability) throws SqlException {
        callableStatement_.resetCallableStatement(netAgent, netConnection, sql, type, concurrency, holdability);
        resetNetCallableStatement(callableStatement_, netAgent, netConnection);
    }

    void resetNetCallableStatement(NetAgent netAgent,
                                   NetConnection netConnection,
                                   String sql,
                                   Section section) throws SqlException {
        callableStatement_.resetCallableStatement(netAgent, netConnection, sql, section);
        resetNetCallableStatement(callableStatement_, netAgent, netConnection);
    }


    void resetNetCallableStatement(NetAgent netAgent,
                                   NetConnection netConnection,
                                   String sql,
                                   Section section,
                                   ColumnMetaData parameterMetaData,
                                   ColumnMetaData resultSetMetaData) throws SqlException {
        callableStatement_.resetCallableStatement(netAgent, netConnection, sql, section, parameterMetaData, resultSetMetaData);
        resetNetCallableStatement(callableStatement_, netAgent, netConnection);
    }

    protected void finalize() throws java.lang.Throwable {
        super.finalize();
    }

}
