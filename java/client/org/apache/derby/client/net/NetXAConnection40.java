/*
 
   Derby - Class org.apache.derby.client.net.NetXAConnection40
 
   Copyright (c) 2006 The Apache Software Foundation or its licensors, where applicable.
 
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

import org.apache.derby.client.am.SqlException;

/**
 * NetXAConnection for jdbc4.0.
 */
public class NetXAConnection40 extends NetXAConnection {
    
    /**
     * creates NetXAConnection40.
     */
    public NetXAConnection40(NetLogWriter netLogWriter,
            String user,
            String password,
            org.apache.derby.jdbc.ClientDataSource dataSource,
            int rmId,
            boolean isXAConn) throws SqlException {
        super(netLogWriter, user, password,
                dataSource, rmId, isXAConn);
    }
    
    /**
     * create and returns NetConnection40.
     * of the supported jdbc version.
     * @param netLogWriter
     * @param user
     * @param password
     * @param dataSource
     * @param rmId
     * @param isXAConn
     * @return NetConnection
     */
    protected NetConnection createNetConnection(NetLogWriter netLogWriter,
            String user,
            String password,
            org.apache.derby.jdbc.ClientDataSource dataSource,
            int rmId,
            boolean isXAConn) throws SqlException {
        return new NetConnection40(netLogWriter, user, password,
                dataSource, rmId, isXAConn);
    }
    
}
