/*

   Derby - Class org.apache.derby.impl.drda.XADatabase.java

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

package org.apache.derby.impl.drda;

import java.sql.SQLException;
import java.util.Properties;
import javax.sql.XAConnection;
import javax.transaction.xa.XAResource;
import org.apache.derby.shared.common.error.ExceptionSeverity;
import org.apache.derby.iapi.jdbc.EngineConnection;
import org.apache.derby.iapi.jdbc.ResourceAdapter;
import org.apache.derby.shared.common.reference.MessageId;
import org.apache.derby.shared.common.i18n.MessageService;
import org.apache.derby.shared.common.info.JVMInfo;
import org.apache.derby.jdbc.EmbeddedXADataSourceInterface;
import org.apache.derby.jdbc.EmbeddedXADataSource;

/**
 * This class contains database state specific to XA,
 * specifically the XAResource that will be used for XA commands.
 */

class XADatabase extends Database {


    // XA Datasource used by all the XA connection requests
    private EmbeddedXADataSourceInterface xaDataSource;

    private XAResource xaResource;
    private XAConnection xaConnection;
    private ResourceAdapter ra;

    
    XADatabase (String dbName)
    {
        super(dbName);
    }

    /**
     * Make a new connection using the database name and set 
     * the connection in the database
     * @throws java.sql.SQLException
     */
    @Override
    synchronized void makeConnection(Properties p) throws SQLException
    {
        if (xaDataSource == null) {
            Class<?> clazz;
            try {
                if (JVMInfo.hasJNDI()) {
                    clazz = Class.forName("org.apache.derby.jdbc.EmbeddedXADataSource");
                    xaDataSource = (EmbeddedXADataSourceInterface) clazz.getConstructor().newInstance();
                } else {
                    clazz = Class.forName("org.apache.derby.jdbc.BasicEmbeddedXADataSource40");
                    xaDataSource = (EmbeddedXADataSourceInterface) clazz.getConstructor().newInstance();
                }
            } catch (Exception e) {
                SQLException ne = new SQLException(
                        MessageService.getTextMessage(
                            MessageId.CORE_DATABASE_NOT_AVAILABLE),
                        "08006",
                        ExceptionSeverity.DATABASE_SEVERITY);
                ne.initCause(e);
                throw ne;
            }
        }

        xaDataSource.setDatabaseName(getShortDbName());
        appendAttrString(p);
        if (attrString != null)
            xaDataSource.setConnectionAttributes(attrString);
        
        EngineConnection conn = getConnection();
        // If we have no existing connection. this is a brand new XAConnection.
        if (conn == null)
        {
            xaConnection = xaDataSource.getXAConnection(userId,password);
            ra = xaDataSource.getResourceAdapter();
            setXAResource(xaConnection.getXAResource());
        }
        else // this is just a connection reset. Close the logical connection.
        {
            conn.close();
        }
        
        // Get a new logical connection.
        // Contract between network server and embedded engine
        // is that any connection returned implements EngineConnection.
        conn = (EngineConnection) xaConnection.getConnection();
        // Client will always drive the commits so connection should
        // always be autocommit false on the server. DERBY-898/DERBY-899
        conn.setAutoCommit(false);
        setConnection(conn);        
    }

    /** SetXAResource
     * @param resource XAResource for this connection
     */
    protected void setXAResource (XAResource resource)
    {
        this.xaResource = resource;
    }

    /**
     * get XA Resource for this connection
     */
    protected XAResource getXAResource ()
    {
        return this.xaResource;
    }

    /**
     * @return The ResourceAdapter instance for
     *         the underlying database.
     */
    ResourceAdapter getResourceAdapter()
    {
        return this.ra;
    }
}

