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
/**********************************************************************
 *
 *
 *  Component Name =
 *
 *  Package Name = org.apache.derby.client.net
 *
 *  Descriptive Name = XACallInfo class
 *
 *  Function = Handle XA information
 *
 *  List of Classes
 *              - NetXACallInfo
 *
 *  Restrictions : None
 *
 **********************************************************************/
package org.apache.derby.client.net;

import java.io.InputStream;
import java.io.OutputStream;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;


//IC see: https://issues.apache.org/jira/browse/DERBY-6125
class NetXACallInfo {
    Xid xid_;                         // current xid
    int xaFlags_;                     // current xaFlags
    /** XA transaction timeout in milliseconds. The value less than 0 means
      * that the time out is not specified. The value 0 means infinite timeout. */
//IC see: https://issues.apache.org/jira/browse/DERBY-2432
    long xaTimeoutMillis_;
    // may not be needed!!!~~~
    int xaFunction_;                  // queued XA function being performed
    int xaRetVal_;                    // xaretval from server
    //  rollback(), or prepare() on RDONLY
    //  one or more times, overrides empty transaction

    NetXAConnection actualConn_; // the actual connection object, not necessarily
    // the user's connection object
    /* only the first connection object is actually used. The other connection
     * objects are used only for their TCP/IP variables to simulate
     * suspend / resume
     */

    private InputStream in_;
    private OutputStream out_;

    public NetXACallInfo() {
        xid_ = null;
        xaFlags_ = XAResource.TMNOFLAGS;
        xaTimeoutMillis_ = -1;
        actualConn_ = null;
        xaRetVal_ = 0;
    }

//IC see: https://issues.apache.org/jira/browse/DERBY-6125
    NetXACallInfo(Xid xid, int flags, NetXAConnection actualConn) {
        xid_ = xid;
        xaFlags_ = flags;
//IC see: https://issues.apache.org/jira/browse/DERBY-2432
//IC see: https://issues.apache.org/jira/browse/DERBY-2432
        xaTimeoutMillis_ = -1;
        actualConn_ = actualConn;
        xaRetVal_ = 0;
    }

    void saveConnectionVariables() {
//IC see: https://issues.apache.org/jira/browse/DERBY-1192
        in_ = actualConn_.getNetConnection().getInputStream();
        out_ = actualConn_.getNetConnection().getOutputStream();
    }

    public InputStream getInputStream() {
        return in_;
    }

    public OutputStream getOutputStream() {
        return out_;
    }
}









