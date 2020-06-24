/*

   Derby - Class org.apache.derby.impl.db.DatabaseContextImpl

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

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
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.services.monitor.ModuleFactory;
import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.iapi.db.Database;
import org.apache.derby.iapi.db.DatabaseContext;
import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.shared.common.error.ExceptionSeverity;

import java.security.PrivilegedAction;
import java.security.AccessController;

/**
	A context that shutdowns down the database on a databsae exception.
*/
//IC see: https://issues.apache.org/jira/browse/DERBY-1095
final class DatabaseContextImpl extends ContextImpl implements DatabaseContext
{

	private final Database	db;

	DatabaseContextImpl(ContextManager cm, Database db) {
		super(cm, DatabaseContextImpl.CONTEXT_ID);
		this.db = db;
	}

	public void cleanupOnError(Throwable t) {
		if (!(t instanceof StandardException)) return;
		StandardException se = (StandardException)t;

        // Ensure the context is popped if the session is
        // going away.
//IC see: https://issues.apache.org/jira/browse/DERBY-1095
        if (se.getSeverity() < ExceptionSeverity.SESSION_SEVERITY)
            return;

        popMe();
        
//IC see: https://issues.apache.org/jira/browse/DERBY-5108
        if (se.getSeverity() >= ExceptionSeverity.DATABASE_SEVERITY) {
            // DERBY-5108: Shut down the istat daemon thread before shutting
            // down the various modules belonging to the database. An active
            // istat daemon thread at the time of shutdown may result in
            // containers being reopened after the container cache has been
            // shut down. On certain platforms, this results in database
            // files that can't be deleted until the VM exits.
            DataDictionary dd = db.getDataDictionary();
            // dd is null if the db is an active slave db (replication)
            if (dd != null) {
                dd.disableIndexStatsRefresher();
            }
        }

        if (se.getSeverity() == ExceptionSeverity.DATABASE_SEVERITY) {
//IC see: https://issues.apache.org/jira/browse/DERBY-6648
		    getContextService().notifyAllActiveThreads(this);
            // This may be called multiple times, but is short-circuited
            // in the monitor.
//IC see: https://issues.apache.org/jira/browse/DERBY-6648
		    getMonitor().shutdown(db);
        }
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
    
    /**
     * Privileged lookup of the ContextService. Must be private so that user code
     * can't call this entry point.
     */
    private  static  ContextService    getContextService()
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-6648
        return AccessController.doPrivileged
            (
             new PrivilegedAction<ContextService>()
             {
                 public ContextService run()
                 {
                     return ContextService.getFactory();
                 }
             }
             );
    }

    /**
     * Privileged Monitor lookup. Must be private so that user code
     * can't call this entry point.
     */
    private  static  ModuleFactory  getMonitor()
    {
        return AccessController.doPrivileged
            (
             new PrivilegedAction<ModuleFactory>()
             {
                 public ModuleFactory run()
                 {
                     return Monitor.getMonitor();
                 }
             }
             );
    }

}
