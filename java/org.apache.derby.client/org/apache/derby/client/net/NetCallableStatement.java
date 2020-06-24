/*

   Derby - Class org.apache.derby.client.net.NetCallableStatement

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
package org.apache.derby.client.net;

import org.apache.derby.client.am.ClientCallableStatement;
import org.apache.derby.client.am.MaterialPreparedStatement;
import org.apache.derby.client.am.SqlException;
import org.apache.derby.client.ClientAutoloadedDriver;
import org.apache.derby.client.ClientPooledConnection;

//IC see: https://issues.apache.org/jira/browse/DERBY-6125
class NetCallableStatement extends NetPreparedStatement
        implements MaterialPreparedStatement {

    ClientCallableStatement callableStatement_;
//IC see: https://issues.apache.org/jira/browse/DERBY-6125

    //-----------------------------state------------------------------------------

    //---------------------constructors/finalizer---------------------------------

    private void initNetCallableStatement() {
        callableStatement_ = null;
    }

    // Relay constructor for all NetCallableStatement, constructors
    private NetCallableStatement(ClientCallableStatement statement,
                         NetAgent netAgent,
                         NetConnection netConnection) throws SqlException {
        super(statement, netAgent, netConnection);
        initNetCallableStatement();
        initNetCallableStatement(statement);
    }

    private void resetNetCallableStatement(ClientCallableStatement statement,
                                   NetAgent netAgent,
                                   NetConnection netConnection) throws SqlException {
        super.resetNetPreparedStatement(statement, netAgent, netConnection);
        initNetCallableStatement();
        initNetCallableStatement(statement);
    }

    private void initNetCallableStatement(ClientCallableStatement statement) {
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
//IC see: https://issues.apache.org/jira/browse/DERBY-941
                         ClientPooledConnection cpc) throws SqlException {
//IC see: https://issues.apache.org/jira/browse/DERBY-6945
        this(ClientAutoloadedDriver.getFactory().newCallableStatement(netAgent,
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


}
