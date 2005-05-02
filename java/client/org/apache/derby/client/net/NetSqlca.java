/*

   Derby - Class org.apache.derby.client.net.NetSqlca

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

import org.apache.derby.client.am.Sqlca;

public class NetSqlca extends Sqlca {
    // these are the same variables that are in the Sqlca except ccsids
    // are a little different

    NetSqlca(org.apache.derby.client.am.Connection connection,
             int sqlCode,
             byte[] sqlStateBytes,
             byte[] sqlErrpBytes,
             int ccsid) {
        super(connection);
        sqlCode_ = sqlCode;
        sqlStateBytes_ = sqlStateBytes;
        sqlErrpBytes_ = sqlErrpBytes;
        ccsid_ = ccsid;
    }

    protected void setSqlerrd(int[] sqlErrd) {
        sqlErrd_ = sqlErrd;
    }

    protected void setSqlwarnBytes(byte[] sqlWarnBytes) {
        sqlWarnBytes_ = sqlWarnBytes;
    }

    protected void setSqlerrmcBytes(byte[] sqlErrmcBytes, int sqlErrmcCcsid) {
        sqlErrmcBytes_ = sqlErrmcBytes;
        sqlErrmcCcsid_ = sqlErrmcCcsid;
    }

    public long getRowCount(Typdef typdef) throws org.apache.derby.client.am.DisconnectException {
        int byteOrder = typdef.getByteOrder();
        long num = (byteOrder == org.apache.derby.client.am.SignedBinary.BIG_ENDIAN) ?
                super.getRowCount() : ((long) sqlErrd_[1] << 32) + sqlErrd_[0];
        return num;
    }
}
