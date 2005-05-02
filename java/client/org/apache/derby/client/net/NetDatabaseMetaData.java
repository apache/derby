/*

   Derby - Class org.apache.derby.client.net.NetDatabaseMetaData

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

import org.apache.derby.client.am.Configuration;
import org.apache.derby.client.am.ProductLevel;
import org.apache.derby.client.am.SqlException;


public class NetDatabaseMetaData extends org.apache.derby.client.am.DatabaseMetaData {

    private final NetAgent netAgent_;


    public NetDatabaseMetaData(NetAgent netAgent, NetConnection netConnection) {
        // Consider setting product level during parse
        super(netAgent, netConnection, new ProductLevel(netConnection.productID_,
                netConnection.targetSrvclsnm_,
                netConnection.targetSrvrlslv_));
        // Set up cheat-links
        netAgent_ = netAgent;
    }

    //---------------------------call-down methods--------------------------------

    public String getURL_() throws SqlException {
        String urlProtocol;

        urlProtocol = Configuration.jdbcDerbyNETProtocol;

        return
                urlProtocol +
                connection_.serverNameIP_ +
                ":" +
                connection_.portNumber_ +
                "/" +
                connection_.databaseName_;
    }

    //-----------------------------helper methods---------------------------------

    // Set flags describing the level of support for this connection.
    // Flags will be set based on manager level and/or specific product identifiers.
    // Support for a specific server version can be set as follows. For example
    // if (productLevel_.greaterThanOrEqualTo(11,1,0))
    //  supportsTheBestThingEver = true
    protected void computeFeatureSet_() {
        if (connection_.resultSetHoldability_ == 0)  // property not set
        {
            setDefaultResultSetHoldability();
        }

    }


    public void setDefaultResultSetHoldability() {
        connection_.resultSetHoldability_ = org.apache.derby.jdbc.ClientDataSource.HOLD_CURSORS_OVER_COMMIT;
    }

}
