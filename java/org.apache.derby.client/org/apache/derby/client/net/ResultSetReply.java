/*

   Derby - Class org.apache.derby.client.net.ResultSetReply

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
import org.apache.derby.client.am.ResultSetCallbackInterface;
import org.apache.derby.client.am.SqlException;


//IC see: https://issues.apache.org/jira/browse/DERBY-6125
class ResultSetReply extends StatementReply {
    private ResultSetReplyInterface materialResultSetReply_;

    ResultSetReply(Agent agent,
                          ResultSetReplyInterface materialResultSetReply,
                          StatementReplyInterface materialStatementReply,
                          ConnectionReplyInterface materialConnectionReply) {
        super(agent, materialStatementReply, materialConnectionReply);
        materialResultSetReply_ = materialResultSetReply;
    }

//IC see: https://issues.apache.org/jira/browse/DERBY-6125
    void readFetch(ResultSetCallbackInterface resultSet) throws SqlException {
        materialResultSetReply_.readFetch(resultSet);
        agent_.checkForChainBreakingException_();
    }


    // think about splitting out the position cursor stuff from the fetch stuff
    void readScrollableFetch(ResultSetCallbackInterface resultSet)
            throws SqlException {
        materialResultSetReply_.readScrollableFetch(resultSet);
        agent_.checkForChainBreakingException_();
    }

    public void readPositioningFetch(ResultSetCallbackInterface resultSet) throws SqlException {
        materialResultSetReply_.readPositioningFetch(resultSet);
        agent_.checkForChainBreakingException_();
    }

//IC see: https://issues.apache.org/jira/browse/DERBY-6125
    void readCursorClose(ResultSetCallbackInterface resultSet)
            throws SqlException {
        materialResultSetReply_.readCursorClose(resultSet);
        agent_.checkForChainBreakingException_();
    }
}
