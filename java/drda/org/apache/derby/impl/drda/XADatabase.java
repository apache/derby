/*

   Derby - Class org.apache.derby.impl.drda.XADatabase.java

   Copyright 2004 The Apache Software Foundation or its licensors, as applicable.

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

/**
 * This class contains database state specific to XA,
 * specifically the XAResource that will be used for XA commands.
 * @author kmarsden@Sourcery.Org
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
import org.apache.derby.impl.drda.DRDAXid;
import  org.apache.derby.iapi.jdbc.BrokeredConnection;

class XADatabase extends Database {


	// XA Datasource used by all the XA connection requests
	private EmbeddedXADataSource xaDataSource;

	private XAResource xaResource;
	private XAConnection xaConnection;

	
	protected XADatabase (String dbName)
	{
		super(dbName);
		forXA = true;
	}

	/**
	 * Make a new connection using the database name and set 
	 * the connection in the database
	 **/
	protected synchronized Connection makeConnection(Properties p) throws
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
		xaConnection = xaDataSource.getXAConnection(userId,password);
		xaResource = xaConnection.getXAResource();

		Connection conn = xaConnection.getConnection();
		setConnection(conn);
		return conn;
		
	}

	/** SetXAResource
	 * @param resource XAResource for this connection
	 */
	protected void setXAResource (XAResource resource)
	{
		this.xaResource = resource;
	}

	/** Set DRDA id for this connection
	 * @param drdaID
	 */
	protected void setDrdaID(String drdaID)
	{
		if (getConnection() != null)
			((BrokeredConnection) getConnection()).setDrdaID(drdaID);
	}


	/**
	 *  Set the internal isolation level to use for preparing statements.
	 *  Subsequent prepares will use this isoalation level
	 * @param level internal isolation level 
	 *
	 * @throws SQLException
	 * @see BrokeredConnection#setPrepareIsolation
	 * 
	 */
	protected void setPrepareIsolation(int level) throws SQLException
	{
		((BrokeredConnection) getConnection()).setPrepareIsolation(level);
	}

	/** get prepare isolation level for this connection.
	 * 
	 */
	protected int getPrepareIsolation() throws SQLException
	{
		return ((BrokeredConnection) getConnection()).getPrepareIsolation();
	}

	/**
	 * get XA Resource for this connection
	 */
	protected XAResource getXAResource ()
	{
		return this.xaResource;
	}


}









