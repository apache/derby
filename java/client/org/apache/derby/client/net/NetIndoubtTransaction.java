/*

   Derby - Class org.apache.derby.client.net.NetXACallInfo

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

import javax.transaction.xa.Xid;

/**
 * <p>Title: dnc Project</p> <p>Description: </p>
 *
 * @version 1.0
 */

public class NetIndoubtTransaction {

    Xid xid_;
    byte[] uowid_;
    byte[] cSyncLog_;
    byte[] pSyncLog_;
    String ipaddr_;
    int port_;

    protected NetIndoubtTransaction(Xid xid,
                                    byte[] uowid,
                                    byte[] cSyncLog,
                                    byte[] pSyncLog,
                                    String ipaddr,
                                    int port) {
        xid_ = xid;
        uowid_ = uowid;
        cSyncLog_ = cSyncLog;
        pSyncLog_ = pSyncLog;
        ipaddr_ = ipaddr;
        port_ = port;
    }

    protected Xid getXid() {
        return xid_;
    }

    protected byte[] getUOWID() {
        return uowid_;
    }

    protected byte[] getCSyncLog() {
        return cSyncLog_;
    }

    protected byte[] getPSyncLog() {
        return pSyncLog_;
    }

    protected String getIpAddr() {
        return ipaddr_;
    }

    protected int getPort() {
        return port_;
    }
}
