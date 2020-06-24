/*

   Derby - Class org.apache.derby.diag.StatementCache

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

package org.apache.derby.diag;

import java.security.PrivilegedAction;
import java.security.AccessController;
import java.sql.ResultSetMetaData;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Collection;
import java.util.Iterator;
import java.util.Vector;

import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.shared.common.reference.Limits;
import org.apache.derby.iapi.services.cache.CacheManager;
import org.apache.derby.iapi.services.context.Context;
import org.apache.derby.iapi.services.context.ContextService;
import org.apache.derby.iapi.sql.ResultColumnDescriptor;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.util.StringUtil;
import org.apache.derby.impl.jdbc.EmbedResultSetMetaData;
import org.apache.derby.impl.sql.GenericPreparedStatement;
import org.apache.derby.impl.sql.GenericStatement;
import org.apache.derby.impl.sql.conn.CachedStatement;
import org.apache.derby.vti.VTITemplate;

/**
	StatementCache is a virtual table that shows the contents of the SQL statement cache.
	
	This virtual table can be invoked by calling it directly.
	<PRE> select * from new org.apache.derby.diag.StatementCache() t</PRE>


	<P>The StatementCache virtual table has the following columns:
	<UL>
	<LI> ID CHAR(36) - not nullable.  Internal identifier of the compiled statement.
	<LI> SCHEMANAME VARCHAR(128) - nullable.  Schema the statement was compiled in.
	<LI> SQL_TEXT VARCHAR(32672) - not nullable.  Text of the statement
//IC see: https://issues.apache.org/jira/browse/DERBY-6065
	<LI> UNICODE BIT/BOOLEAN - not nullable.  Always true.
	<LI> VALID BIT/BOOLEAN - not nullable.  True if the statement is currently valid, false otherwise
	<LI> COMPILED_AT TIMESTAMP nullable - time statement was compiled, requires STATISTICS TIMING to be enabled.


	</UL>
	<P>
	The internal identifier of a cached statement matches the toString() method of a PreparedStatement object for a Derby database.
//IC see: https://issues.apache.org/jira/browse/DERBY-2400

	<P>
	This class also provides a static method to empty the statement cache, StatementCache.emptyCache()

*/
public final class StatementCache extends VTITemplate {

	private int position = -1;
	private Vector<GenericPreparedStatement> data;
	private GenericPreparedStatement currentPs;
	private boolean wasNull;

	public StatementCache() throws StandardException {

        DiagUtil.checkAccess();
        
        LanguageConnectionContext lcc = (LanguageConnectionContext)
            getContextOrNull(LanguageConnectionContext.CONTEXT_ID);
//IC see: https://issues.apache.org/jira/browse/DERBY-6648

//IC see: https://issues.apache.org/jira/browse/DERBY-2772
        CacheManager statementCache =
            lcc.getLanguageConnectionFactory().getStatementCache();

		if (statementCache != null) {
			final Collection values = statementCache.values();
//IC see: https://issues.apache.org/jira/browse/DERBY-6213
			data = new Vector<GenericPreparedStatement>(values.size());
			for (Iterator i = values.iterator(); i.hasNext(); ) {
				final CachedStatement cs = (CachedStatement) i.next();
				final GenericPreparedStatement ps =
					(GenericPreparedStatement) cs.getPreparedStatement();
//IC see: https://issues.apache.org/jira/browse/DERBY-5060
				data.add(ps);
			}
		}
	}

	public boolean next() {

		if (data == null)
			return false;

		position++;

		for (; position < data.size(); position++) {
//IC see: https://issues.apache.org/jira/browse/DERBY-5060
			currentPs = (GenericPreparedStatement) data.get(position);
	
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
//IC see: https://issues.apache.org/jira/browse/DERBY-104
			sql = StringUtil.truncate(sql, Limits.DB2_VARCHAR_MAXWIDTH);
			return sql;
		default:
			return null;
		}
	}

	public boolean getBoolean(int colId) {
		wasNull = false;
		switch (colId) {
		case 4:
			// was/is UniCode column, but since Derby 10.0 all
			// statements are compiled and submitted as UniCode.
//IC see: https://issues.apache.org/jira/browse/DERBY-571
			return true;
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
//IC see: https://issues.apache.org/jira/browse/DERBY-104
		EmbedResultSetMetaData.getResultColumnDescriptor("SQL_TEXT",  Types.VARCHAR, false, Limits.DB2_VARCHAR_MAXWIDTH),
		EmbedResultSetMetaData.getResultColumnDescriptor("UNICODE",   Types.BIT, false),
		EmbedResultSetMetaData.getResultColumnDescriptor("VALID",  Types.BIT, false),
		EmbedResultSetMetaData.getResultColumnDescriptor("COMPILED_AT",  Types.TIMESTAMP, true),

	};
	
    private static final ResultSetMetaData metadata =
        new EmbedResultSetMetaData(columnInfo);
//IC see: https://issues.apache.org/jira/browse/DERBY-1984

	public ResultSetMetaData getMetaData() {

		return metadata;
	}
    
    /**
     * Privileged lookup of a Context. Must be private so that user code
     * can't call this entry point.
     */
    private  static  Context    getContextOrNull( final String contextID )
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-6648
        if ( System.getSecurityManager() == null )
        {
            return ContextService.getContextOrNull( contextID );
        }
        else
        {
            return AccessController.doPrivileged
                (
                 new PrivilegedAction<Context>()
                 {
                     public Context run()
                     {
                         return ContextService.getContextOrNull( contextID );
                     }
                 }
                 );
        }
    }

}
