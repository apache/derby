/*

   Derby - Class org.apache.derby.client.am.DisconnectException

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

import org.apache.derby.shared.common.reference.SQLState;

public class DisconnectException extends SqlException {
    private DisconnectException(Agent agent, ClientMessageId msgid,
        Object[] args, SqlCode sqlcode, Throwable t)  {
        super(agent != null ? agent.logWriter_ : null, msgid,
            args, sqlcode, t);
        
        // make the call to close the streams and socket.
        if (agent != null) {
            agent.disconnectEvent();
        }
    }

    public DisconnectException(Agent agent, ClientMessageId msgid,
                               Throwable t, Object... args) {
        this(agent, msgid, args, SqlCode.disconnectError, (Throwable)t);
    }

    public DisconnectException(Agent agent, ClientMessageId msgid,
                               Object... args) {
        this(agent, msgid, (Throwable) null, args);
    }
    
    public DisconnectException(Agent agent, SqlException e) {
        super(agent.logWriter_,
            new ClientMessageId(SQLState.DRDA_CONNECTION_TERMINATED),
            e, e.getMessage());
    }
}
