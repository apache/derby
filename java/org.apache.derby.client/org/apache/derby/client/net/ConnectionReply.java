/*

   Derby - Class org.apache.derby.client.net.ConnectionReply

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

import org.apache.derby.client.am.Agent;
import org.apache.derby.client.am.ConnectionCallbackInterface;
import org.apache.derby.client.am.SqlException;


//IC see: https://issues.apache.org/jira/browse/DERBY-6125
class ConnectionReply {
    private ConnectionReplyInterface materialConnectionReply_;
    Agent agent_;
//IC see: https://issues.apache.org/jira/browse/DERBY-6125

    ConnectionReply(
            Agent agent,
            ConnectionReplyInterface materialConnectionReply) {

        agent_ = agent;
        materialConnectionReply_ = materialConnectionReply;
    }

//IC see: https://issues.apache.org/jira/browse/DERBY-6125
    void readCommitSubstitute(ConnectionCallbackInterface connection)
            throws SqlException {
        materialConnectionReply_.readCommitSubstitute(connection);
        agent_.checkForChainBreakingException_();
    }

    void readLocalCommit(ConnectionCallbackInterface connection)
            throws SqlException {
        materialConnectionReply_.readLocalCommit(connection);
        agent_.checkForChainBreakingException_();
    }

    void readLocalRollback(ConnectionCallbackInterface connection)
            throws SqlException {
        materialConnectionReply_.readLocalRollback(connection);
        agent_.checkForChainBreakingException_();
    }

    void readLocalXAStart(ConnectionCallbackInterface connection)
            throws SqlException {
        materialConnectionReply_.readLocalXAStart(connection);
        agent_.checkForChainBreakingException_();
    }

    void readLocalXACommit(ConnectionCallbackInterface connection)
            throws SqlException {
        materialConnectionReply_.readLocalXACommit(connection);
        agent_.checkForChainBreakingException_();
    }

    void readLocalXARollback(ConnectionCallbackInterface connection)
            throws SqlException {
        materialConnectionReply_.readLocalXARollback(connection);
        agent_.checkForChainBreakingException_();
    }
}
