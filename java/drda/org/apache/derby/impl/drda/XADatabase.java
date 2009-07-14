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

import java.sql.Connection;
import java.sql.SQLException;

import javax.transaction.xa.XAResource;
import javax.sql.XADataSource;
import javax.sql.XAConnection;

import java.util.Hashtable;
import java.util.Properties;
import java.util.Enumeration;


import org.apache.derby.jdbc.EmbeddedXADataSource;
import org.apache.derby.iapi.jdbc.BrokeredConnection;
import org.apache.derby.iapi.jdbc.EngineConnection;
import org.apache.derby.iapi.jdbc.ResourceAdapter;

/**
 * This class contains database state specific to XA,
 * specifically the XAResource that will be used for XA commands.
 */

class XADatabase extends Database {


	// XA Datasource used by all the XA connection requests
	private EmbeddedXADataSource xaDataSource;

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
	 **/
	synchronized void makeConnection(Properties p) throws
 SQLException
	{
		if (xaDataSource == null)
		{
			xaDataSource = new EmbeddedXADataSource();
		}

		xaDataSource.setDatabaseName(shortDbName);
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

