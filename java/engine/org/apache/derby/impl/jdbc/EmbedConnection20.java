/*

   Derby - Class org.apache.derby.impl.jdbc.EmbedConnection20

   Copyright 1999, 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derby.impl.jdbc;

import java.sql.SQLException;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;

import org.apache.derby.impl.jdbc.EmbedConnection;
import org.apache.derby.impl.jdbc.Util;
import org.apache.derby.jdbc.Driver169;

import org.apache.derby.iapi.store.access.XATransactionController;
import org.apache.derby.iapi.reference.SQLState;

import java.util.Properties;


/**
 * This class extends the EmbedConnection class in order to support new
 * methods and classes that come with JDBC 2.0.
 *
 *	@see org.apache.derby.impl.jdbc.EmbedConnection
 *
 */
public class EmbedConnection20 extends EmbedConnection
{

	//////////////////////////////////////////////////////////
	// CONSTRUCTORS
	//////////////////////////////////////////////////////////

	public EmbedConnection20(Driver169 driver, String url, Properties info)
		 throws SQLException 
	{
		super(driver, url, info);
	}

	public EmbedConnection20(EmbedConnection inputConnection) 
	{
		super(inputConnection);
	}

	/**
	 * Drop all the declared global temporary tables associated with this connection. This gets called
	 * when a getConnection() is done on a PooledConnection. This will ensure all the temporary tables
	 * declared on earlier connection handle associated with this physical database connection are dropped
	 * before a new connection handle is issued on that same physical database connection.
	 *
	 */
	public void dropAllDeclaredGlobalTempTables() throws SQLException {
		synchronized (getConnectionSynchronization())
		{
			setupContextStack();
			try {
				getLanguageConnection().dropAllDeclaredGlobalTempTables();
			} catch (StandardException t) {
				throw handleException(t);
			}
			finally
			{
				restoreContextStack();
			}
		}
	}

 	/////////////////////////////////////////////////////////////////////////
	//
	//	JDBC 2.0	-	New public methods
	//
	/////////////////////////////////////////////////////////////////////////

    /**
     *
	 * Get the type-map object associated with this connection.
	 * By default, the map returned is empty.
	 * JDBC 2.0 - java.util.Map requires JDK 1
     *
     */
    public java.util.Map getTypeMap() {
		// just return an empty map
		return new java.util.Hashtable(0);
    }

    /** 
	 * Install a type-map object as the default type-map for
	 * this connection.
	 * JDBC 2.0 - java.util.Map requires JDK 1
     *
     * @exception SQLException Feature not implemented for now.
	 */
    public void setTypeMap(java.util.Map map) throws SQLException {

        if( map == null)
            throw Util.generateCsSQLException(SQLState.INVALID_API_PARAMETER,map,"map",
                                              "java.sql.Connection.setTypeMap");
        if(!(map.isEmpty()))
            throw Util.notImplemented();
    }

	/*
	** methods to be overridden by subimplementations wishing to insert
	** their classes into the mix.
	** The reason we need to override them is because we want to create a
	** Local20/LocalStatment object (etc) rather than a Local/LocalStatment
	** object (etc).
	*/


	/*
	** XA support
	*/

	public final int xa_prepare() throws SQLException {

		synchronized (getConnectionSynchronization())
		{
            setupContextStack();
			try
			{
				XATransactionController tc = 
					(XATransactionController) getLanguageConnection().getTransactionExecute();

				int ret = tc.xa_prepare();

				if (ret == XATransactionController.XA_RDONLY)
				{
					// On a prepare call, xa allows an optimization that if the
					// transaction is read only, the RM can just go ahead and
					// commit it.  So if store returns this read only status -
					// meaning store has taken the liberty to commit already - we
					// needs to turn around and call internalCommit (without
					// committing the store again) to make sure the state is
					// consistent.  Since the transaction is read only, there is
					// probably not much that needs to be done.

					getLanguageConnection().internalCommit(false /* don't commitStore again */);
				}
				return ret;
			} catch (StandardException t)
			{
				throw handleException(t);
			}
			finally
			{
				restoreContextStack();
			}
		}
	}


	public final void xa_commit(boolean onePhase) throws SQLException {

		synchronized (getConnectionSynchronization())
		{
            setupContextStack();
			try
			{
		    	getLanguageConnection().xaCommit(onePhase);
			} catch (StandardException t)
			{
				throw handleException(t);
			}
			finally 
			{
				restoreContextStack();
			}
		}
	}
	public final void xa_rollback() throws SQLException {

		synchronized (getConnectionSynchronization())
		{
            setupContextStack();
			try
			{
		    	getLanguageConnection().xaRollback();
			} catch (StandardException t)
			{
				throw handleException(t);
			}
			finally 
			{
				restoreContextStack();
			}
		}
	}
}
