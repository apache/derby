/*

   Derby - Class org.apache.derby.diag.StatementCache

   Copyright 2002, 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derby.diag;

import org.apache.derby.vti.VTITemplate;

import org.apache.derby.iapi.sql.conn.ConnectionUtil;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.impl.sql.GenericPreparedStatement;
import org.apache.derby.impl.sql.GenericStatement;

import org.apache.derby.iapi.sql.ResultColumnDescriptor;
import org.apache.derby.impl.jdbc.EmbedResultSetMetaData;
import org.apache.derby.iapi.reference.DB2Limit;
import org.apache.derby.iapi.util.StringUtil;

import java.sql.Types;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;

import org.apache.derby.impl.sql.conn.CachedStatement;
import org.apache.derby.impl.services.cache.CachedItem;


import java.util.Vector;
import java.util.Enumeration;

/**
	StatementCache is a virtual table that shows the contents of the SQL statement cache.
	
	This virtual table can be invoked by calling it directly.
	<PRE> select * from new org.apache.derby.diag.StatementCache() t</PRE>


	<P>The StatementCache virtual table has the following columns:
	<UL>
	<LI> ID CHAR(36) - not nullable.  Internal identifier of the compiled statement.
	<LI> SCHEMANAME VARCHAR(128) - nullable.  Schema the statement was compiled in.
	<LI> SQL_TEXT VARCHAR(32672) - not nullable.  Text of the statement
	<LI> UNICODE BIT/BOOLEAN - not nullable.  True if the statement is compiled as a pure unicode string, false if it handled unicode escapes.
	<LI> VALID BIT/BOOLEAN - not nullable.  True if the statement is currently valid, false otherwise
	<LI> COMPILED_AT TIMESTAMP nullable - time statement was compiled, requires STATISTICS TIMING to be enabled.


	</UL>
	<P>
	The internal identifier of a cached statement matches the toString() method of a PreparedStatement object for a Cloudscape database.

	<P>
	This class also provides a static method to empty the statement cache, StatementCache.emptyCache()

*/
public final class StatementCache extends VTITemplate {

	private int position = -1;
	private Vector data;
	private GenericPreparedStatement currentPs;
	private boolean wasNull;

	/**
		Empty the statement cache. Must be called from a SQL statement, e.g.
		<PRE>
		CALL org.apache.derby.diag.StatementCache::emptyCache()
		</PRE>

	*/
	public static void emptyCache() throws SQLException {

		org.apache.derby.impl.sql.conn.GenericLanguageConnectionContext lcc =
			(org.apache.derby.impl.sql.conn.GenericLanguageConnectionContext) ConnectionUtil.getCurrentLCC();

		lcc.emptyCache();
	}

	public StatementCache() throws SQLException {

		org.apache.derby.impl.sql.conn.GenericLanguageConnectionContext lcc =
			(org.apache.derby.impl.sql.conn.GenericLanguageConnectionContext) ConnectionUtil.getCurrentLCC();

		if (lcc.statementCache != null) {

			java.util.Hashtable stmtCache = (java.util.Hashtable) lcc.statementCache;
			data = new Vector(stmtCache.size());
			for (Enumeration e = stmtCache.elements(); e.hasMoreElements(); ) {


				CachedItem ci = (CachedItem) e.nextElement();
				CachedStatement cs = (CachedStatement) ci.getEntry();

				GenericPreparedStatement ps = (GenericPreparedStatement) cs.getPreparedStatement();

				data.addElement(ps);
			}
		}

	}

	public boolean next() {

		if (data == null)
			return false;

		position++;

		for (; position < data.size(); position++) {
			currentPs = (GenericPreparedStatement) data.elementAt(position);
	
			if (currentPs != null)
				return true;
		}

		data = null;
		return false;
	}

	public void close() {
		data = null;
		currentPs = null;
	}


	public String getString(int colId) {
		wasNull = false;
		switch (colId) {
		case 1:
			return currentPs.getObjectName();
		case 2:
			return ((GenericStatement) currentPs.statement).getCompilationSchema();
		case 3:
			String sql = currentPs.getSource();
			sql = StringUtil.truncate(sql, DB2Limit.DB2_VARCHAR_MAXWIDTH);
			return sql;
		default:
			return null;
		}
	}

	public boolean getBoolean(int colId) {
		wasNull = false;
		switch (colId) {
		case 4:
			return currentPs.statement.getUnicode();
		case 5:
			return currentPs.isValid();
		default:
			return false;
		}
	}

	public Timestamp getTimestamp(int colId) {

		Timestamp ts = currentPs.getEndCompileTimestamp();
		wasNull = (ts == null);
		return ts;
	}

	public boolean wasNull() {
		return wasNull;
	}

	/*
	** Metadata
	*/
	private static final ResultColumnDescriptor[] columnInfo = {

		EmbedResultSetMetaData.getResultColumnDescriptor("ID",		  Types.CHAR, false, 36),
		EmbedResultSetMetaData.getResultColumnDescriptor("SCHEMANAME",    Types.VARCHAR, true, 128),
		EmbedResultSetMetaData.getResultColumnDescriptor("SQL_TEXT",  Types.VARCHAR, false, DB2Limit.DB2_VARCHAR_MAXWIDTH),
		EmbedResultSetMetaData.getResultColumnDescriptor("UNICODE",   Types.BIT, false),
		EmbedResultSetMetaData.getResultColumnDescriptor("VALID",  Types.BIT, false),
		EmbedResultSetMetaData.getResultColumnDescriptor("COMPILED_AT",  Types.TIMESTAMP, true),

	};
	
	private static final ResultSetMetaData metadata = new EmbedResultSetMetaData(columnInfo);

	public ResultSetMetaData getMetaData() {

		return metadata;
	}
}
