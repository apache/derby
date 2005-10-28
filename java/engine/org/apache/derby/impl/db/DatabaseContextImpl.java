/*

   Derby - Class org.apache.derby.impl.db.DatabaseContextImpl

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
