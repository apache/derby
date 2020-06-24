/*

   Derby - Class org.apache.derby.impl.tools.ij.ConnectionEnv

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

package org.apache.derby.impl.tools.ij;

import java.io.File;
import java.io.FileNotFoundException;
import java.lang.reflect.InvocationTargetException;

import java.util.Hashtable;
import java.util.Enumeration;
import java.util.Properties;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.apache.derby.tools.JDBCDisplayUtil;
import org.apache.derby.iapi.tools.i18n.LocalizedOutput;

/**
	To enable multi-user use of ij.Main2

 */
class ConnectionEnv {
//IC see: https://issues.apache.org/jira/browse/DERBY-6213
	Hashtable<String,Session> sessions = new Hashtable<String,Session>();
	private Session currSession;
	private String tag;
	private boolean only;
	private static final String CONNECTION_PROPERTY = "ij.connection";
    private String protocol;

	ConnectionEnv(int userNumber, boolean printUserNumber, boolean theOnly) {
		if (printUserNumber)
			tag = "("+(userNumber+1)+")";
		only = theOnly;
	}

	/**
		separate from the constructor so that connection
		failure does not prevent object creation.
	 */
    void init(LocalizedOutput out) throws SQLException, ClassNotFoundException, InstantiationException, IllegalAccessException, NoSuchMethodException, InvocationTargetException {
//IC see: https://issues.apache.org/jira/browse/DERBY-6856

		Connection c = util.startJBMS(null,null);

		// only load up ij.connection.* properties if there is
		// only one ConnectionEnv in the system.
		if (only) {
//IC see: https://issues.apache.org/jira/browse/DERBY-6213
            Properties p = AccessController.doPrivileged(new PrivilegedAction<Properties>() {
            	public Properties run() {
                	return System.getProperties();
            	}
            });
            protocol = p.getProperty(ij.PROTOCOL_PROPERTY);

	        String prefix = CONNECTION_PROPERTY + ".";
		    for (Enumeration e = p.propertyNames(); e.hasMoreElements(); )
		    {
		    	String key = (String)e.nextElement();
		    	if (key.startsWith(prefix)) {
		    		String name = key.substring(prefix.length());
		    		installConnection(name.toUpperCase(java.util.Locale.ENGLISH), 
						p.getProperty(key), out);
		    	}
		    }
		}

		if (c!=null) // have a database from the startup?
		{
			String sname=Session.DEFAULT_NAME+sessions.size();
			Session s = new Session(c,tag,sname);
			sessions.put(sname,s);
			currSession = s;
		}

	}

	void doPrompt(boolean newStatement, LocalizedOutput out) {
		if (currSession != null) currSession.doPrompt(newStatement, out, sessions.size()>1);
		else utilMain.doPrompt(newStatement, out, tag);
	}
	
	Connection getConnection() {
		if (currSession == null) return null;
		return currSession.getConnection();
	}

	/**
		Making a new connection, add it to the pool, and make it current.
	 */
	void addSession(Connection conn,String name) {
		String aName;
		if (name == null) aName = getUniqueConnectionName();
		else aName = name;
		Session s = new Session(conn, tag, aName);
		sessions.put(aName, s);
		currSession = s;
	}

  //returns a unique Connection# name by going through existing sessions
  public String getUniqueConnectionName() {
    int newNum = 0;
    boolean newConnectionNameOk = false;
    String newConnectionName = "";
//IC see: https://issues.apache.org/jira/browse/DERBY-6213
    Enumeration<String> e;
    while (!newConnectionNameOk){
      newConnectionName = Session.DEFAULT_NAME + newNum;
      newConnectionNameOk = true;
      e = sessions.keys();
      while (e.hasMoreElements() && newConnectionNameOk){
        if ((e.nextElement()).equals(newConnectionName))
           newConnectionNameOk = false;
      }
      newNum = newNum + 1;
    }
    return newConnectionName;
  }

	Session getSession() {
		return currSession;
	}

//IC see: https://issues.apache.org/jira/browse/DERBY-6213
	Hashtable<String,Session> getSessions() {
		return sessions;
	}

	Session getSession(String name) {
		return sessions.get(name);
	}

	Session setCurrentSession(String name) {
		currSession = sessions.get(name);
		return currSession;
	}

	boolean haveSession(String name) {
		return (name != null) && (sessions.size()>0) && (null != sessions.get(name));
	}

	void removeCurrentSession() throws SQLException {
		if (currSession ==null) return;
		sessions.remove(currSession.getName());
		currSession.close();
		currSession = null;
	}

	void removeSession(String name) throws SQLException {
//IC see: https://issues.apache.org/jira/browse/DERBY-6213
		Session s = sessions.remove(name);
		s.close();
		if (currSession == s)
			currSession = null;
	}

	void removeAllSessions() throws SQLException {
		if (sessions == null || sessions.size() == 0)
			return;
		else
//IC see: https://issues.apache.org/jira/browse/DERBY-6213
			for (Enumeration<String> e = sessions.keys(); e.hasMoreElements(); ) {
				String n = e.nextElement();
				removeSession(n);
			}
	}

	private void installConnection(String name, String value, LocalizedOutput out) throws SQLException {
		// add protocol if no driver matches url
		boolean noDriver = false;
		try {
			// if we have a full URL, make sure it's loaded first
			try {
				if (value.startsWith("jdbc:"))
					util.loadDriverIfKnown(value);
			} catch (Exception e) {
				// want to continue with the attempt
			}
			DriverManager.getDriver(value);
		} catch (SQLException se) {
			noDriver = true;
		}
		if (noDriver && (protocol != null)) {
			value = protocol + value;
		}

		if (sessions.get(name) != null) {
			throw ijException.alreadyHaveConnectionNamed(name);
		}
		try {
			
			String user = util.getSystemProperty("ij.user");
			String password = util.getSystemProperty("ij.password");
			Properties connInfo =  util.updateConnInfo(user, password,null);
														   
			Connection theConnection = 
				DriverManager.getConnection(value, connInfo);
																			   
		    addSession(theConnection,name);
		} catch (Throwable t) {
			JDBCDisplayUtil.ShowException(out,t);
		}
	}

}
