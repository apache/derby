/*

   Derby - Class org.apache.derby.client.net.NetDatabaseMetaData

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

import org.apache.derby.client.am.Configuration;
import org.apache.derby.client.am.ProductLevel;
import org.apache.derby.client.am.SqlException;

public class NetDatabaseMetaData extends org.apache.derby.client.am.DatabaseMetaData {

    /** True if the server supports QRYCLSIMP. */
    private boolean supportsQryclsimp_;
    
    private boolean supportsLayerBStreaming_;

    /**
     * True if the server supports session data caching
     */
    private boolean supportsSessionDataCaching_;

    /** True if the server supports UDTs */
    private boolean supportsUDTs_;
    
    public NetDatabaseMetaData(NetAgent netAgent, NetConnection netConnection) {
        // Consider setting product level during parse
        super(netAgent, netConnection, new ProductLevel(netConnection.productID_,
                netConnection.targetSrvclsnm_,
                netConnection.targetSrvrlslv_));
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
    //
    // WARNING WARNING WARNING !!!!
    //
    // If you define an instance variable of NetDatabaseMetaData that
    // you want computeFeatureSet_() to compute, DO NOT assign an
    // initial value to the variable in the
    // declaration. NetDatabaseMetaData's constructor will invoke
    // DatabaseMetaData's constructor, which then invokes
    // computeFeatureSet_(). Initialization of instance variables in
    // NetDatabaseMetaData will happen *after* the invocation of
    // computeFeatureSet_() and will therefore overwrite the computed
    // values. So, LEAVE INSTANCE VARIABLES UNINITIALIZED!
    //
    // END OF WARNING
    protected void computeFeatureSet_() {

        // Support for QRYCLSIMP was added in 10.2.0
        if (productLevel_.greaterThanOrEqualTo(10, 2, 0)) {
            supportsQryclsimp_ = true;
        } else {
            supportsQryclsimp_ = false;
        }
        
        supportsLayerBStreaming_ = 
            productLevel_.greaterThanOrEqualTo(10, 3, 0);

        supportsSessionDataCaching_ =
                productLevel_.greaterThanOrEqualTo(10, 4, 0);

        supportsUDTs_ =
                productLevel_.greaterThanOrEqualTo(10, 6, 0);
    }

    /**
     * Check whether the server has full support for the QRYCLSIMP
     * parameter in OPNQRY.
     *
     * @return true if QRYCLSIMP is fully supported
     */
    final boolean serverSupportsQryclsimp() {
        return supportsQryclsimp_;
    }

    final boolean serverSupportsLayerBStreaming() {
        return supportsLayerBStreaming_;
    }

    /**
     * Check if server supports session data caching
     * @return true if the server supports this
     */
    final boolean serverSupportsSessionDataCaching() {
        return supportsSessionDataCaching_;
    }

    /**
     * Check if server supports UDTs
     * @return true if the server supports this
     */
    final boolean serverSupportsUDTs() {
        return supportsSessionDataCaching_;
    }
}
