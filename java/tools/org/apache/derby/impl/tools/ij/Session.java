/*

   Derby - Class org.apache.derby.impl.tools.ij.Session

   Copyright 1998, 2004 The Apache Software Foundation or its licensors, as applicable.

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

import org.apache.derby.iapi.tools.i18n.LocalizedOutput;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Hashtable;

/**
	Session holds the objects local to a particular database session,
	which starts with a connection and is all other JDBC
	stuff used on that connection, along with some ij state
	that is connection-based as well.

	This is separated out to localize database objects and
	also group objects by session.

	@author ames
 */
class Session {
	static final String DEFAULT_NAME="CONNECTION";

	boolean singleSession = true;
	Connection conn = null;
	String tag, name;
	Hashtable prepStmts = new Hashtable();
	Hashtable cursorStmts = new Hashtable();
	Hashtable cursors = new Hashtable();
	Hashtable asyncStmts = new Hashtable();
	boolean isJCC= false;      // Is this the IBM UNIVERSAL DRIVER.

	Session(Connection newConn, String newTag, String newName) {
		conn = newConn;
		tag = newTag;
		name = newName;

		try
		{
			isJCC = conn.getMetaData().getDriverName().startsWith("IBM DB2 JDBC Universal Driver");
		}
		catch (SQLException se)
		{
			// if there is a problem getting the driver name we will 
			// assume it is not jcc.
		}
	}

	Connection getConnection() {
		// CHECK: should never be null
		return conn;
	}

	boolean getIsJCC()
	{
		return isJCC;
	}

	String getName() {
		return name;
	}

	Object addPreparedStatement(String name, PreparedStatement ps) {
		return prepStmts.put(name,ps);
	}

	Object addCursorStatement(String name, Statement s) {
		return cursorStmts.put(name, s);
	}

	Object addCursor(String name, ResultSet rs) {
		return cursors.put(name, rs);
	}

	Object addAsyncStatement(String name, AsyncStatement s) {
		return asyncStmts.put(name, s);
	}

	PreparedStatement getPreparedStatement(String name) {
		return (PreparedStatement) prepStmts.get(name);
	}

	Statement getCursorStatement(String name) {
		return (Statement)cursorStmts.get(name);
	}

	ResultSet getCursor(String name) {
		return (ResultSet)cursors.get(name);
	}

	AsyncStatement getAsyncStatement(String name) {
		return (AsyncStatement)asyncStmts.get(name);
	}

	boolean removePreparedStatement(String name) {
		return prepStmts.remove(name)!=null;
	}

	boolean removeCursorStatement(String name) {
		return cursorStmts.remove(name)!=null;
	}

	boolean removeCursor(String name) {
		return cursors.remove(name)!=null;
	}

    void doPrompt(boolean newStatement, LocalizedOutput out, boolean multiSessions) {
		// check if tag should be increased...
		if (multiSessions && singleSession) {
			singleSession = false;

			if (tag == null) tag = "("+name+")";
			else tag = tag.substring(0,tag.length()-1)+":"+name+")";
		}

		// check if tag should be reduced...
		if (!multiSessions && !singleSession) {
			singleSession = true;

			if (tag == null) {}
			else if (tag.length() == name.length()+2) tag = null;
			else tag = tag.substring(0,tag.length()-2-name.length())+")";
		}

		utilMain.doPrompt(newStatement, out, tag);
	}

	void close() throws SQLException {

		if (!conn.isClosed())
		{
			if  (!conn.getAutoCommit() && name != null && ! name.startsWith("XA"))
				conn.rollback();
			conn.close();
		}

		prepStmts.clear(); // should we check & close them individually?
		cursorStmts.clear();
		cursors.clear();
		asyncStmts.clear();

		conn = null;
	}

}
