/*

   Derby - Class org.apache.derby.client.net.ConnectionReply

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

import org.apache.derby.client.am.ConnectionCallbackInterface;
import org.apache.derby.client.am.SqlException;


public class ConnectionReply {
    private ConnectionReplyInterface materialConnectionReply_;
    org.apache.derby.client.am.Agent agent_;

    ConnectionReply(org.apache.derby.client.am.Agent agent, ConnectionReplyInterface materialConnectionReply) {
        agent_ = agent;
        materialConnectionReply_ = materialConnectionReply;
    }

    public void readCommitSubstitute(ConnectionCallbackInterface connection) throws SqlException {
        materialConnectionReply_.readCommitSubstitute(connection);
        agent_.checkForChainBreakingException_();
    }

    public void readLocalCommit(ConnectionCallbackInterface connection) throws SqlException {
        materialConnectionReply_.readLocalCommit(connection);
        agent_.checkForChainBreakingException_();
    }

    public void readLocalRollback(ConnectionCallbackInterface connection) throws SqlException {
        materialConnectionReply_.readLocalRollback(connection);
        agent_.checkForChainBreakingException_();
    }

    public void readLocalXAStart(ConnectionCallbackInterface connection) throws SqlException {
        materialConnectionReply_.readLocalXAStart(connection);
        agent_.checkForChainBreakingException_();
    }

    public void readLocalXACommit(ConnectionCallbackInterface connection) throws SqlException {
        materialConnectionReply_.readLocalXACommit(connection);
        agent_.checkForChainBreakingException_();
    }

    public void readLocalXARollback(ConnectionCallbackInterface connection) throws SqlException {
        materialConnectionReply_.readLocalXARollback(connection);
        agent_.checkForChainBreakingException_();
    }
}
