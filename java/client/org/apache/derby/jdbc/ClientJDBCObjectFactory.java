/*

   Derby - Class org.apache.derby.jdbc.ClientJDBCObjectFactory

   Copyright (c) 2005 The Apache Software Foundation or its licensors, where applicable.

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

package org.apache.derby.jdbc;

import org.apache.derby.client.ClientPooledConnection;
import org.apache.derby.client.am.CallableStatement;
import org.apache.derby.client.am.PreparedStatement;
import org.apache.derby.client.am.Configuration;
import org.apache.derby.client.am.LogWriter;
import org.apache.derby.client.am.Agent;
import org.apache.derby.client.am.Section;
import org.apache.derby.client.am.Cursor;
import org.apache.derby.client.net.NetAgent;
import org.apache.derby.client.net.NetConnection;
import org.apache.derby.client.net.NetStatement;
import org.apache.derby.client.net.NetLogWriter;
import org.apache.derby.client.net.NetResultSet;
import org.apache.derby.client.net.NetDatabaseMetaData;
import java.lang.reflect.*;
import java.sql.Connection;
import java.sql.SQLException;

public class ClientJDBCObjectFactory
{	
	public static ClientPooledConnection newClientPooledConnection(ClientDataSource ds,LogWriter logWriter,String user,String password) throws SQLException
	{
		ClientPooledConnection cpc=null;
		Class argsClass[] = new Class[]	{ClientDataSource.class,LogWriter.class,String.class,String.class};
		Object objArgs[] = new Object[] {ds,logWriter,user,password};
		try
		{
			if(Configuration.jreLevelMajor == 1 && Configuration.jreLevelMinor == 6)
			{
				Class cpcClass = Class.forName("org.apache.derby.jdbc.ClientPooledConnection40");
				Constructor cpcConst = cpcClass.getConstructor(argsClass);
				cpc = (ClientPooledConnection) cpcConst.newInstance(objArgs);
			}
			else
			{
				
				Class cpcClass = Class.forName("org.apache.derby.jdbc.ClientPooledConnection");
				Constructor cpcConst = cpcClass.getConstructor(argsClass);
				cpc = (ClientPooledConnection) cpcConst.newInstance(objArgs);
			}
			
		}
		catch(Exception e)
		{
		        SQLException sqle = new SQLException(e.getMessage());
    		    	throw sqle;

		}

	return cpc;	
				
	}	
	public static ClientPooledConnection newClientPooledConnection(LogWriter logWriter,String user,String password,int rmId) throws SQLException
	{
		ClientPooledConnection cpc=null;
		Integer rmIdClass = new Integer(rmId);
		Class argsClass[] = new Class[]	{LogWriter.class,String.class,String.class,int.class};
		Object objArgs[] = new Object[] {logWriter,user,password,rmIdClass};
		try
		{
			if(Configuration.jreLevelMajor == 1 && Configuration.jreLevelMinor == 6)
			{
				Class cpcClass = Class.forName("org.apache.derby.jdbc.ClientPooledConnection40");
				Constructor cpcConst = cpcClass.getConstructor(argsClass);
				cpc = (ClientPooledConnection) cpcConst.newInstance(objArgs);
			}
			else
			{
				
				Class cpcClass = Class.forName("org.apache.derby.jdbc.ClientPooledConnection");
				Constructor cpcConst = cpcClass.getConstructor(argsClass);
				cpc = (ClientPooledConnection) cpcConst.newInstance(objArgs);
			}
			
		}
		catch(Exception e)
		{
		        SQLException sqle = new SQLException(e.getMessage());
    		    	throw sqle;
		}

	return cpc;	
	}	

	public static CallableStatement newCallableStatement(Agent agent,Connection connection, String sql,int type,int concurrency,int holdability) throws org.apache.derby.client.am.SqlException
	{
		CallableStatement cs = null;
		Integer typeClass = new Integer(type);
		Integer concurrencyClass = new Integer(concurrency);	
		Integer holdabilityClass = new Integer(holdability);

		
		Class argsClass[] = new Class[]	{Agent.class,Connection.class,String.class,int.class,int.class,int.class};
		Object objArgs[] = new Object[] {agent,connection,sql,typeClass,concurrencyClass,holdabilityClass};
		try
		{
			if(Configuration.jreLevelMajor == 1 && Configuration.jreLevelMinor == 6)
			{
				Class csClass = Class.forName("org.apache.derby.client.am.CallableStatement40");
				Constructor csConst = csClass.getConstructor(argsClass);
				cs = (CallableStatement) csConst.newInstance(objArgs);
			}
			else
			{
				
				Class csClass = Class.forName("org.apache.derby.client.am.CallableStatement");
				Constructor csConst = csClass.getConstructor(argsClass);
				cs = (CallableStatement) csConst.newInstance(objArgs);
			}
		}
		catch(Exception e)
		{
		        org.apache.derby.client.am.SqlException sqle = new org.apache.derby.client.am.SqlException(agent.logWriter_,e.getMessage());
    		    	throw sqle;
		}

	return cs;
	}
	public static PreparedStatement newPreparedStatement(Agent agent,org.apache.derby.client.am.Connection connection,String sql,Section section) throws org.apache.derby.client.am.SqlException
	{
		
		PreparedStatement ps = null;
		
		Class argsClass[] = new Class[]	{Agent.class,org.apache.derby.client.am.Connection.class,String.class,Section.class};
		Object objArgs[] = new Object[] {agent,connection,sql,section};
		try
		{
			if(Configuration.jreLevelMajor == 1 && Configuration.jreLevelMinor == 6)
			{
				Class psClass = Class.forName("org.apache.derby.client.am.PreparedStatement40");
				Constructor psConst = psClass.getConstructor(argsClass);
				ps = (PreparedStatement) psConst.newInstance(objArgs);
			}
			else
			{
				
				Class psClass = Class.forName("org.apache.derby.client.am.PreparedStatement");
				Constructor psConst = psClass.getConstructor(argsClass);
				ps = (PreparedStatement) psConst.newInstance(objArgs);
			}
		}
		catch(Exception e)
		{
		        org.apache.derby.client.am.SqlException sqle = new org.apache.derby.client.am.SqlException(agent.logWriter_,e.getMessage());
    		    	throw sqle;
		}
	return ps;
	}

	public static PreparedStatement newPreparedStatement(Agent agent,org.apache.derby.client.am.Connection connection,String sql,int type,int concurrency,int holdability,int autoGeneratedKeys,String [] columnNames) throws org.apache.derby.client.am.SqlException
	{
		
		PreparedStatement ps = null;
		Integer typeClass = new Integer(type);
		Integer concurrencyClass = new Integer(concurrency);
		Integer holdabilityClass = new Integer(holdability);
		Integer autoGeneratedKeysClass = new Integer(autoGeneratedKeys);
 	        Class argsClass [] = new Class [] {Agent.class,org.apache.derby.client.am.Connection.class,String.class,int.class,int.class,int.class,int.class,new String[0].getClass()};
		Object objArgs[] = new Object[] {agent,connection,sql,typeClass,concurrencyClass,holdabilityClass,autoGeneratedKeysClass,columnNames};
		try
		{
			if(Configuration.jreLevelMajor == 1 && Configuration.jreLevelMinor == 6)
			{
				Class psClass = Class.forName("org.apache.derby.client.am.PreparedStatement40");
				Constructor psConst = psClass.getConstructor(argsClass);
				ps = (PreparedStatement) psConst.newInstance(objArgs);
			}
			else
			{
				
				Class psClass = Class.forName("org.apache.derby.client.am.PreparedStatement");
				Constructor psConst = psClass.getConstructor(argsClass);
				ps = (PreparedStatement) psConst.newInstance(objArgs);
			}
		}
		catch(Exception e)
		{	
			org.apache.derby.client.am.SqlException sqle = new org.apache.derby.client.am.SqlException(agent.logWriter_,e.getMessage());
			throw sqle;
		}
	return ps;
	}
	public static NetConnection newNetConnection(NetLogWriter netLogWriter,String databaseName,java.util.Properties properties) throws SQLException
	{
		NetConnection nc=null;
		Class argsClass[] = new Class[]	{NetLogWriter.class,String.class,java.util.Properties.class};
		Object objArgs[] = new Object[] {netLogWriter,databaseName,properties};
		try
		{
			nc = retNetConnectionObject(argsClass,objArgs);	
		}
		catch(SQLException e)
		{
			throw e;
		}
		return nc;
	}
	public static NetConnection newNetConnection(NetLogWriter netLogWriter,org.apache.derby.jdbc.ClientDataSource clientDataSource,String user,String password) throws SQLException
	{
		NetConnection nc=null;
		Class argsClass[] = new Class[]	{NetLogWriter.class,org.apache.derby.jdbc.ClientDataSource.class,String.class,String.class};
		Object objArgs[] = new Object[] {netLogWriter,clientDataSource,user,password};
		try
		{
			nc = retNetConnectionObject(argsClass,objArgs);	
		}
		catch(SQLException e)
		{
			throw e;
		}
		return nc;
	}
	public static NetConnection newNetConnection(NetLogWriter netLogWriter,int driverManagerLoginTimeout,String serverName,int portNumber,String databaseName,java.util.Properties properties) throws SQLException
	{
		NetConnection nc=null;
		Integer timeoutClass = new Integer(driverManagerLoginTimeout);
		Integer portClass = new Integer(portNumber);
		Class argsClass[] = new Class[]	{NetLogWriter.class,int.class,String.class,int.class,String.class,java.util.Properties.class};
		Object objArgs[] = new Object[] {netLogWriter,timeoutClass,serverName,portClass,databaseName,properties};
		try
		{
			nc = retNetConnectionObject(argsClass,objArgs);	
		}
		catch(SQLException e)
		{
			throw e;
		}
		return nc;
		
	}
	public static NetConnection newNetConnection(NetLogWriter netLogWriter,String user,String password,org.apache.derby.jdbc.ClientDataSource dataSource,int rmId,boolean isXAConn) throws SQLException
	{
		NetConnection nc=null;
		Integer rmIdClass = new Integer(rmId);
		Boolean isxaconnClass = new Boolean(isXAConn);	
		Class argsClass[] = new Class[]	{NetLogWriter.class,String.class,String.class,org.apache.derby.jdbc.ClientDataSource.class,int.class,boolean.class};
		Object objArgs[] = new Object[] {netLogWriter,user,password,dataSource,rmIdClass,isxaconnClass};
		try
		{
			nc = retNetConnectionObject(argsClass,objArgs);	
		}
		catch(SQLException e)
		{
			throw e;
		}
		return nc;
	}
	public static NetConnection newNetConnection(NetLogWriter netLogWriter,String ipaddr,int portNumber,org.apache.derby.jdbc.ClientDataSource dataSource,boolean isXAConn) throws SQLException
	{
		NetConnection nc=null;
		Integer portNumberClass = new Integer(portNumber);
		Boolean isxaconnClass = new Boolean(isXAConn);	
		Class argsClass[] = new Class[]	{NetLogWriter.class,String.class,int.class,org.apache.derby.jdbc.ClientDataSource.class,boolean.class};
		Object objArgs[] = new Object[] {netLogWriter,ipaddr,portNumberClass,dataSource,isxaconnClass};
		try
		{
			nc = retNetConnectionObject(argsClass,objArgs);	
		}
		catch(SQLException e)
		{
			throw e;
		}
		return nc;
			
	}
	private static NetConnection retNetConnectionObject(Class argsClass[],Object objArgs[]) throws SQLException
	{
		NetConnection nc=null;
		try
		{
			if(Configuration.jreLevelMajor == 1 && Configuration.jreLevelMinor == 6)
			{
				Class psClass = Class.forName("org.apache.derby.client.net.NetConnection40");
				Constructor psConst = psClass.getConstructor(argsClass);
				nc = (NetConnection) psConst.newInstance(objArgs);
			}
			else
			{
				
				Class ncClass = Class.forName("org.apache.derby.client.net.NetConnection");
				Constructor ncConst = ncClass.getConstructor(argsClass);
				nc = (NetConnection) ncConst.newInstance(objArgs);
			}
		}
		catch(Exception e)
		{
		        SQLException sqle = new SQLException(e.getMessage());
    		    	throw sqle;
		}
	return nc;
	}
	public static NetResultSet newNetResultSet(NetAgent netAgent,NetStatement netStatement,Cursor cursor,int sqlcsrhld,int qryattscr,int qryattsns,int qryattset,long qryinsid,int actualResultSetType,int actualResultSetConcurrency,int actualResultSetHoldability) throws SQLException
	{
		NetResultSet rs=null;

		Integer arg1Class = new Integer(sqlcsrhld);
		Integer arg2Class = new Integer(qryattscr);
		Integer arg3Class = new Integer(qryattsns);
		Integer arg4Class = new Integer(qryattset);
		Long arg5Class = new Long(qryinsid);
		Integer arg6Class = new Integer(actualResultSetType);
		Integer arg7Class = new Integer(actualResultSetConcurrency);
		Integer arg8Class = new Integer(actualResultSetHoldability);
		
		
		Class argsClass[] = new Class[]	{NetAgent.class,NetStatement.class,Cursor.class,int.class,int.class,int.class,int.class,long.class,int.class,int.class,int.class};
		Object objArgs[] = new Object[] {netAgent,netStatement,cursor,arg1Class,arg2Class,arg3Class,arg4Class,arg5Class,arg6Class,arg7Class,arg8Class};

		
		try
		{
			if(Configuration.jreLevelMajor == 1 && Configuration.jreLevelMinor == 6)
			{
				Class rsClass = Class.forName("org.apache.derby.client.net.NetResultSet40");
				Constructor rsConst = rsClass.getConstructor(argsClass);
				rs = (NetResultSet) rsConst.newInstance(objArgs);
			}
			else
			{
				
				Class rsClass = Class.forName("org.apache.derby.client.net.NetResultSet");
				Constructor rsConst = rsClass.getConstructor(argsClass);
		 	 	rs = (NetResultSet) rsConst.newInstance(objArgs);
			}
		}
		catch(Exception e)
		{
		        SQLException sqle = new SQLException(e.getMessage());
    		    	throw sqle;
		}
	return rs;
	} 
	public static NetDatabaseMetaData newNetDatabaseMetaData(NetAgent netAgent,NetConnection netConnection) throws SQLException
	{
		NetDatabaseMetaData dmd=null;

		Class argsClass[] = new Class[]	{NetAgent.class,NetConnection.class};
		Object objArgs[] = new Object[] {netAgent,netConnection};
		
		try
		{
			if(Configuration.jreLevelMajor == 1 && Configuration.jreLevelMinor == 6)
			{
				Class dmdClass = Class.forName("org.apache.derby.client.net.NetDatabaseMetaData40");
				Constructor dmdConst = dmdClass.getConstructor(argsClass);
				dmd = (NetDatabaseMetaData) dmdConst.newInstance(objArgs);
			}
			else
			{
				
				Class dmdClass = Class.forName("org.apache.derby.client.net.NetDatabaseMetaData");
				Constructor dmdConst = dmdClass.getConstructor(argsClass);
		 	 	dmd = (NetDatabaseMetaData) dmdConst.newInstance(objArgs);
			}
		}
		catch(Exception e)
		{
		        SQLException sqle = new SQLException(e.getMessage());
    		    	throw sqle;
		}
	return dmd;
	} 
}
	
