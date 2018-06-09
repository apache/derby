/*

   Derby - Class org.apache.derby.client.net.NetSqlca

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

import org.apache.derby.client.am.Sqlca;
import org.apache.derby.client.am.ClientConnection;

class NetSqlca extends Sqlca {
    // these are the same variables that are in the Sqlca except ccsids
    // are a little different

    NetSqlca(ClientConnection connection,
             int sqlCode,
             String sqlState,
             byte[] sqlErrpBytes) {
        super(connection);
        sqlCode_ = sqlCode;
        sqlState_ = sqlState;
        sqlErrpBytes_ = sqlErrpBytes;
    }

    NetSqlca(ClientConnection connection,
            int sqlCode,
            byte[] sqlState,
            byte[] sqlErrpBytes) {
       super(connection);
       sqlCode_ = sqlCode;
       sqlState_ = bytes2String(sqlState,0,sqlState.length);
       sqlErrpBytes_ = sqlErrpBytes;
    }

    void setSqlerrd(int[] sqlErrd) {
        sqlErrd_ = sqlErrd;
    }

    void setSqlwarnBytes(byte[] sqlWarnBytes) {
        sqlWarnBytes_ = sqlWarnBytes;
    }

    void setSqlerrmcBytes(byte[] sqlErrmcBytes) {
        sqlErrmcBytes_ = sqlErrmcBytes;
    }
}
