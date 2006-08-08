/*

   Derby - Class org.apache.derby.client.net.StatementReply

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
import org.apache.derby.client.am.PreparedStatementCallbackInterface;
import org.apache.derby.client.am.SqlException;
import org.apache.derby.client.am.StatementCallbackInterface;

public class StatementReply extends ConnectionReply {
    private StatementReplyInterface materialStatementReply_;

    StatementReply(Agent agent,
                   StatementReplyInterface materialStatementReply,
                   ConnectionReplyInterface materialConnectionReply) {
        super(agent, materialConnectionReply);
        materialStatementReply_ = materialStatementReply;
    }

    public void readPrepareDescribeOutput(StatementCallbackInterface statement) throws SqlException {
        materialStatementReply_.readPrepareDescribeOutput(statement);
        agent_.checkForChainBreakingException_();
    }

    public void readExecuteImmediate(StatementCallbackInterface statement) throws SqlException {
        materialStatementReply_.readExecuteImmediate(statement);
        agent_.checkForChainBreakingException_();
    }

    public void readOpenQuery(StatementCallbackInterface statement) throws SqlException {
        materialStatementReply_.readOpenQuery(statement);
        agent_.checkForChainBreakingException_();
    }

    public void readExecute(PreparedStatementCallbackInterface preparedStatement) throws SqlException {
        materialStatementReply_.readExecute(preparedStatement);
        agent_.checkForChainBreakingException_();
    }

    public void readPrepare(StatementCallbackInterface statement) throws SqlException {
        materialStatementReply_.readPrepare(statement);
        agent_.checkForChainBreakingException_();
    }

    public void readDescribeInput(PreparedStatementCallbackInterface preparedStatement) throws SqlException {
        materialStatementReply_.readDescribeInput(preparedStatement);
        agent_.checkForChainBreakingException_();
    }

    public void readDescribeOutput(PreparedStatementCallbackInterface preparedStatement) throws SqlException {
        materialStatementReply_.readDescribeOutput(preparedStatement);
        agent_.checkForChainBreakingException_();
    }

    public void readExecuteCall(StatementCallbackInterface statement) throws SqlException {
        materialStatementReply_.readExecuteCall(statement);
        agent_.checkForChainBreakingException_();
    }


    public void readSetSpecialRegister(StatementCallbackInterface statement) throws SqlException {
        materialStatementReply_.readSetSpecialRegister(statement);
        agent_.checkForChainBreakingException_();
    }
}
