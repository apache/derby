/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.db
   (C) Copyright IBM Corp. 1998, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.impl.db;

import org.apache.derby.iapi.services.context.ContextImpl;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.services.context.ContextService;
import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.iapi.db.Database;
import org.apache.derby.iapi.db.DatabaseContext;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.error.ExceptionSeverity;


/**
	A context that shutdowns down the database on a databsae exception.
*/
class DatabaseContextImpl extends ContextImpl implements DatabaseContext
{
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1998_2004;

	private final Database	db;

	DatabaseContextImpl(ContextManager cm, Database db) {
		super(cm, DatabaseContextImpl.CONTEXT_ID);
		this.db = db;
	}

	public void cleanupOnError(Throwable t) {
		if (!(t instanceof StandardException)) return;
		StandardException se = (StandardException)t;
		if (se.getSeverity() != ExceptionSeverity.DATABASE_SEVERITY) return;
		popMe();
		ContextService.getFactory().notifyAllActiveThreads(this);
		Monitor.getMonitor().shutdown(db);
	}

	public boolean equals(Object other) {
		if (other instanceof DatabaseContext) {
			return ((DatabaseContextImpl) other).db == db;
		}
		return false;
	}

	public int hashCode() {
		return db.hashCode();
	}

	public Database getDatabase() {return db;}
}
