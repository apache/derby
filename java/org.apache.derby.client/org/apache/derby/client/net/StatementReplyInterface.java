/*

   Derby - Class org.apache.derby.client.net.StatementReplyInterface

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

import org.apache.derby.client.am.DisconnectException;
import org.apache.derby.client.am.PreparedStatementCallbackInterface;
import org.apache.derby.client.am.StatementCallbackInterface;

interface StatementReplyInterface {
    public void readPrepareDescribeOutput(StatementCallbackInterface statement) throws DisconnectException;

    public void readExecuteImmediate(StatementCallbackInterface statement) throws DisconnectException;

    public void readOpenQuery(StatementCallbackInterface statement) throws DisconnectException;

    public void readExecute(PreparedStatementCallbackInterface preparedStatement) throws DisconnectException;

    public void readPrepare(StatementCallbackInterface statement) throws DisconnectException;

    public void readDescribeInput(PreparedStatementCallbackInterface preparedStatement) throws DisconnectException;

    public void readDescribeOutput(PreparedStatementCallbackInterface preparedStatement) throws DisconnectException;

    public void readExecuteCall(StatementCallbackInterface statement) throws DisconnectException;

    public void readSetSpecialRegister(StatementCallbackInterface statement) throws DisconnectException;
}
