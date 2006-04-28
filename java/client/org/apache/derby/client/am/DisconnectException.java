/*

   Derby - Class org.apache.derby.client.am.DisconnectException

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

package org.apache.derby.client.am;

public class DisconnectException extends SqlException {
    public DisconnectException(Agent agent, ClientMessageId msgid, SqlCode sqlcode) {
        super(agent != null ? agent.logWriter_ : null, msgid, sqlcode);
    }

    public DisconnectException(Agent agent, ClientMessageId msgid) {
        super(agent != null ? agent.logWriter_ : null, msgid, 
            SqlCode.disconnectError);
        
        // make the call to close the streams and socket.
        if (agent != null) {
            agent.disconnectEvent();
        }
    }
        
    // Old constructors for backward compatibility until all classes
    // have been internationalized
    public DisconnectException(Agent agent, String reason, SqlState sqlstate, SqlCode sqlcode) {
        super(agent.logWriter_, reason, sqlstate, sqlcode);
    }

    public DisconnectException(Agent agent, String reason, SqlState sqlstate) {
        super(agent.logWriter_, reason, sqlstate, SqlCode.disconnectError);
        // make the call to close the streams and socket.
        if (agent != null) {
            agent.disconnectEvent();
        }
    }

    public DisconnectException(java.lang.Throwable throwable, Agent agent, String reason, SqlState sqlstate) {
        super(agent.logWriter_, throwable, reason, sqlstate, SqlCode.disconnectError);
        // make the call to close the streams and socket.
        if (agent != null) {
            agent.disconnectEvent();
        }
    }

    public DisconnectException(Agent agent) {
        this(agent, null, SqlState.undefined);
    }

    public DisconnectException(java.lang.Throwable throwable, Agent agent) {
        this(throwable, agent, null, SqlState.undefined);
    }

    public DisconnectException(Agent agent, String reason) {
        this(agent, reason, SqlState.undefined);
    }

    public DisconnectException(Throwable throwable, Agent agent, String reason) {
        this(throwable, agent, reason, SqlState.undefined);
    }

    public DisconnectException(Agent agent, SqlException e) {
        this(agent, e.getMessage());
        setNextException(e);
    }
}


