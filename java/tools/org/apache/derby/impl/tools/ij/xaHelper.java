/*

   Derby - Class org.apache.derby.impl.tools.ij.xaHelper

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

package org.apache.derby.impl.tools.ij;

import org.apache.derby.iapi.tools.i18n.LocalizedResource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Locale;
import java.util.Vector;

import javax.transaction.xa.Xid;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.XAException;
import javax.sql.PooledConnection;
import javax.sql.XAConnection;
import javax.sql.XADataSource;
import javax.sql.DataSource;
import javax.sql.ConnectionPoolDataSource;

/*
 * The real xa helper class.  Load this class only if we know the javax classes
 * are in the class path.
 */
class xaHelper implements xaAbstractHelper
{

	private XADataSource currentXADataSource;
	private XAConnection currentXAConnection;

	private String databaseName;

	// non xa stuff
	private DataSource currentDataSource;
	private ConnectionPoolDataSource currentCPDataSource;
	private PooledConnection currentPooledConnection;

  private String framework_property;

  xaHelper()
  {
  }
	  
	  
   public void setFramework(String framework)
  {
    this.framework_property = framework_property;
  }
	
	private Xid makeXid(int xid)
	{
		return new ijXid(xid, databaseName.getBytes());
	}

	public void XADataSourceStatement(ij parser, Token dbname, Token shutdown,
									String create)
		 throws SQLException
	{
		try
		{
			  currentXADataSource = (XADataSource) getXADataSource();

			  databaseName = parser.stringValue(dbname.image);
			  xaHelper.setDataSourceProperty(currentXADataSource, "databaseName", databaseName);
			  xaHelper.setDataSourceProperty(currentXADataSource, "dataSourceName", databaseName);

			if (shutdown != null && shutdown.toString().toLowerCase(Locale.ENGLISH).equals("shutdown"))
			{
			  xaHelper.setDataSourceProperty(currentXADataSource, "shutdownDatabase", "shutdown");

				// do a getXAConnection to shut it down */
				currentXADataSource.getXAConnection().getConnection();

				currentXADataSource = null;
				currentXAConnection = null;
			}
			else if (create != null)
			{
				if (create.toLowerCase(java.util.Locale.ENGLISH).equals("create"))
				{
					xaHelper.setDataSourceProperty(currentXADataSource, "createDatabase", "create");

					/* do a getXAConnection to create it */
					XAConnection conn = currentXADataSource.getXAConnection();
					conn.close();

					xaHelper.setDataSourceProperty(currentXADataSource, "createDatabase", null);
				}
			}
		}
		catch (Throwable t)
		{
			handleException(t);
		}	
	}


	public void XAConnectStatement(ij parser, Token user, Token pass, String id)
		 throws SQLException
	{
		try
		{
			if (currentXAConnection != null)
			{
				try {
					currentXAConnection.close();
				} catch (SQLException sqle) {
				}

				currentXAConnection = null;
			}

			String username = null;
			String password = "";

			if (pass != null)
				password = parser.stringValue(pass.image);

			if (user != null)
			{
				username = parser.stringValue(user.image);

				currentXAConnection = 
					currentXADataSource.getXAConnection(username, password);
			}
			else
			{

				currentXAConnection = currentXADataSource.getXAConnection();
			}

		}
		catch (Throwable t)
		{
			handleException(t);
		}
	}

	public void XADisconnectStatement(ij parser, String n) throws SQLException
	{
		if (currentXAConnection == null)
			throw ijException.noSuchConnection("XAConnection");
		currentXAConnection.close();
		currentXAConnection = null;
	}

	public Connection XAGetConnectionStatement(ij parser, String n) throws SQLException
	{
		try
		{
			return currentXAConnection.getConnection();
		}
		catch(Throwable t)
		{
			handleException(t);
		}
		return null;
	}

	public void CommitStatement(ij parser, Token onePhase, Token twoPhase, 
								int xid) 
		 throws SQLException
	{
		try
		{	
			currentXAConnection.getXAResource().commit(makeXid(xid), (onePhase != null));
		}
		catch(Throwable t)
		{
			handleException(t);
		}
	}

	public void EndStatement(ij parser, int flag, int xid) throws SQLException
	{
		try
		{	
			currentXAConnection.getXAResource().end(makeXid(xid), flag);
		}
		catch(Throwable t)
		{
			handleException(t);
		}
	}

	public void ForgetStatement(ij parser, int xid) throws SQLException
	{
		try
		{	
			currentXAConnection.getXAResource().forget(makeXid(xid));
		}
		catch(Throwable t)
		{
			handleException(t);
		}
	}

	public void PrepareStatement(ij parser, int xid) throws SQLException
	{
		try
		{	
			currentXAConnection.getXAResource().prepare(makeXid(xid));
		}
		catch(Throwable t)
		{
			handleException(t);
		}
	}

	public ijResult RecoverStatement(ij parser, int flag) throws SQLException
	{
		Object[] val = null;

		try
		{	
			val = currentXAConnection.getXAResource().recover(flag);
		}
		catch(Throwable t)
		{
			handleException(t);
		}

		Vector v = new Vector();
		v.addElement("");
		v.addElement(LocalizedResource.getMessage("IJ_Reco0InDoubT", LocalizedResource.getNumber(val.length)));
		v.addElement("");
		for (int i = 0; i < val.length; i++)
			v.addElement(LocalizedResource.getMessage("IJ_Tran01", LocalizedResource.getNumber(i+1), val[i].toString()));

		return new ijVectorResult(v,null);

	}

	public void RollbackStatement(ij parser, int xid) throws SQLException
	{
		try
		{	
			currentXAConnection.getXAResource().rollback(makeXid(xid));
		}
		catch(Throwable t)
		{
			handleException(t);
		}

	}

	public void StartStatement(ij parser, int flag, int xid) throws SQLException
	{
		try
		{	
			currentXAConnection.getXAResource().start(makeXid(xid), flag);
		}
		catch(Throwable t)
		{
			handleException(t);
		}
	}

	private void handleException(Throwable t) throws SQLException
	{
		if (t instanceof SQLException)
		{
			// let ij handle it
			throw (SQLException)t;
		}
		if (t instanceof XAException)
		{
			int errorCode = ((XAException)t).errorCode;
			String error = LocalizedResource.getMessage("IJ_IlleValu");

			// XA_RBBASE 100
			// XA_RBROLLBACK 100
			// XA_RBCOMMFAIL 101
			// XA_RBDEADLOCK 102
			// XA_RBINTEGRITY 103
			// XA_RBOTHER 104
			// XA_RBPROTO 105
			// XA_RBTIMEOUT 106
			// XA_RBTRANSIENT 107
			// XA_RBEND 107
			//
			// XA_RDONLY 3
			// XA_RETRY 4
			// XA_HEURMIX 5
			// XA_HEURRB 6
			// XA_HEURCOM 7
			// XA_HEURHAZ 8
			// XA_NOMIGRATE 9
			//
			// XAER_ASYNC -2
			// XAER_RMERR -3
			// XAER_NOTA -4
			// XAER_INVAL -5
			// XAER_PROTO -6
			// XAER_RMFAIL -7
			// XAER_DUPID -8
			// XAER_OUTSIDE -9

			switch(errorCode)
			{
			case XAException.XA_HEURCOM : error = "XA_HEURCOM "; break;
			case XAException.XA_HEURHAZ : error = "XA_HEURHAZ"; break;
			case XAException.XA_HEURMIX : error = "XA_HEURMIX"; break;
			case XAException.XA_HEURRB : error = "XA_HEURRB "; break;
			case XAException.XA_NOMIGRATE : error = "XA_NOMIGRATE "; break;
				// case XAException.XA_RBBASE : error = "XA_RBBASE "; break;
			case XAException.XA_RBCOMMFAIL : error = "XA_RBCOMMFAIL "; break;
			case XAException.XA_RBDEADLOCK : error = "XA_RBDEADLOCK "; break;
				// case XAException.XA_RBEND : error = "XA_RBEND "; break;
			case XAException.XA_RBINTEGRITY : error = "XA_RBINTEGRITY "; break;
			case XAException.XA_RBOTHER : error = "XA_RBOTHER "; break;
			case XAException.XA_RBPROTO : error = "XA_RBPROTO "; break;
			case XAException.XA_RBROLLBACK : error = "XA_RBROLLBACK "; break;
			case XAException.XA_RBTIMEOUT : error = "XA_RBTIMEOUT "; break;
			case XAException.XA_RBTRANSIENT : error = "XA_RBTRANSIENT "; break;
			case XAException.XA_RDONLY : error = "XA_RDONLY "; break;
			case XAException.XA_RETRY : error = "XA_RETRY "; break;
			case XAException.XAER_ASYNC : error = "XAER_ASYNC "; break;
			case XAException.XAER_DUPID : error = "XAER_DUPID "; break;
			case XAException.XAER_INVAL : error = "XAER_INVAL "; break;
			case XAException.XAER_NOTA : error = "XAER_NOTA "; break;
			case XAException.XAER_OUTSIDE : error = "XAER_OUTSIDE "; break;
			case XAException.XAER_PROTO : error = "XAER_PROTO "; break;
			case XAException.XAER_RMERR : error = "XAER_RMERR "; break;
			case XAException.XAER_RMFAIL : error = "XAER_RMFAIL "; break;
			}
			throw new ijException(error);

		}
		else // StandardException or run time exception, log it first
		{
			String info = LocalizedResource.getMessage("IJ_01SeeClouLog", t.toString(), t.getMessage());
			t.printStackTrace(System.out);
			throw new ijException(info);
		}
	}


	// non-xa stuff. DataSource and ConnectionPoolDataSource
	public Connection DataSourceStatement(ij parser, Token dbname, Token protocol,
									Token userT, Token passT, String id)
		 throws SQLException
	{

		try {
			currentDataSource = (DataSource) (Class.forName("org.apache.derby.jdbc.EmbeddedDataSource").newInstance());
		} catch (Exception e) {
			throw new SQLException(e.toString());
		}
		databaseName = parser.stringValue(dbname.image);
		xaHelper.setDataSourceProperty(currentDataSource, "databaseName", databaseName);

		// make a connection
		Connection c = null;
		String username = null;
		String password = "";

		if (passT != null)
			password = parser.stringValue(passT.image);

		if (userT != null)
		{
			username = parser.stringValue(userT.image);
			c = currentDataSource.getConnection(username, password);
		}
		else
		{
			c = currentDataSource.getConnection();
		}

		return c;

	}

	public void CPDataSourceStatement(ij parser, Token dbname, Token protocol)
		 throws SQLException
	{
		try {
			currentCPDataSource = (ConnectionPoolDataSource) (Class.forName("org.apache.derby.jdbc.EmbeddedConnectionPoolDataSource").newInstance());
		} catch (Exception e) {
			throw new SQLException(e.toString());
		}
		databaseName = parser.stringValue(dbname.image);
		xaHelper.setDataSourceProperty(currentCPDataSource, "databaseName", databaseName);
	}

	public void CPConnectStatement(ij parser, Token userT, Token passT, String n)
		 throws SQLException
	{
		String username = null;
		String password = "";

		if (passT != null)
			password = parser.stringValue(passT.image);

		if (userT != null)
		{
			username = parser.stringValue(userT.image);
			currentPooledConnection =
				currentCPDataSource.getPooledConnection(username, password);
		}
		else
		{
			currentPooledConnection =
				currentCPDataSource.getPooledConnection();
		}
	}

	public Connection CPGetConnectionStatement(ij parser, String n) 
		 throws SQLException 
	{
		return currentPooledConnection.getConnection();
	}

	public void CPDisconnectStatement(ij parser, String n) throws SQLException
	{
		if (currentPooledConnection == null)
			throw ijException.noSuchConnection(LocalizedResource.getMessage("IJ_Pool"));
		currentPooledConnection.close();
		currentPooledConnection = null;
	}

	/**
	 * Get a DataSource that supports distributed transactions.
	 *
	 * @return XADataSource object 
	 *
	 * @exception Exception if XaDataSource is not in class path.
	 */
	private XADataSource getXADataSource() throws Exception
	{
		// We need to construct this object in this round about fashion because
		// if we new it directly, then it will the tools.jar file to bloat.
		try
		{
			return (XADataSource)(Class.forName("org.apache.derby.jdbc.EmbeddedXADataSource").newInstance());
		}
		catch(ClassNotFoundException cnfe) {
			throw new ijException(LocalizedResource.getMessage("IJ_XAClass"));
		}
		catch (InstantiationException e) { }
		catch (IllegalAccessException e) { }

		throw new ijException(LocalizedResource.getMessage("IJ_XANoI"));
	}
	private static final Class[] STRING_P = { "".getClass() };
	private static final Class[] INT_P = { Integer.TYPE };

	private static void setDataSourceProperty(Object ds, String property, String value) throws SQLException {

		String methodName =
			"set" + Character.toUpperCase(property.charAt(0)) + property.substring(1);

		try {
			java.lang.reflect.Method m = ds.getClass().getMethod(methodName, STRING_P);
			m.invoke(ds, new Object[] {value});
			return;
		} catch (/*NoSuchMethod*/Exception nsme) {
			throw new SQLException(property + " ???");
			//java.lang.reflect.Method m = ds.getClass().getMethod("set" + property, INT_P);
			//m.invoke(ds, new Object[] {Integer.valueOf(value)});
		}
	}
}

class ijXid implements Xid, java.io.Serializable
{
  private static final long serialVersionUID = 64467452100036L;

	private final int format_id;
	private final byte[] global_id;
	private final byte[] branch_id;


	ijXid(int xid, byte[] id)
	{
		format_id = xid;
		global_id = id;
		branch_id = id;
		
	}
    /**
     * Obtain the format id part of the Xid.
     * <p>
     *
     * @return Format identifier. O means the OSI CCR format.
     **/
    public int getFormatId()
    {
        return(format_id);
    }

    /**
     * Obtain the global transaction identifier part of XID as an array of 
     * bytes.
     * <p>
     *
	 * @return A byte array containing the global transaction identifier.
     **/
    public byte[] getGlobalTransactionId()
    {
        return(global_id);
    }

    /**
     * Obtain the transaction branch qualifier part of the Xid in a byte array.
     * <p>
     *
	 * @return A byte array containing the branch qualifier of the transaction.
     **/
    public byte[] getBranchQualifier()
    {
        return(branch_id);
    }
}


