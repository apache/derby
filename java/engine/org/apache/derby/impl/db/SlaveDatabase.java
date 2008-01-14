/*

   Derby - Class org.apache.derby.impl.db.SlaveDatabase

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

import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.jdbc.AuthenticationService;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.services.context.ContextService;
import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.iapi.services.replication.slave.SlaveFactory;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.impl.services.monitor.UpdateServiceProperties;

import java.util.Properties;

/**
 * SlaveDatabase is an instance of Database, and is booted instead of
 * BasicDatabase if this database will have the replication slave
 * role. SlaveDatabase differs from BasicDatabase in the following
 * ways:
 *
 * 1: When starting a non-replicated database (i.e., BasicDatabase),
 *    only one thread is used to start all modules of the database.
 *    When booted in slave mode, the thread that boots the store
 *    module will be blocked during log recovery. To remedy this,
 *    SlaveDatabase runs the boot method of BasicDatabase in a
 *    separate thread. This ensures that the connection attempt that
 *    started slave replication mode will not hang.
 *
 * 2: While the database is in replication slave mode, the
 *    authentication services are not available because these require
 *    that the store module has been booted first. Calling
 *    getAuthenticationService when in slave mode will raise an
 *    exception.
 *
 * 3: While the database is in replication slave mode, connections are
 *    not accepted since the database cannot process transaction
 *    requests. Calling setupConnection when in slave mode will raise
 *    an exception.
 *
 * 4: If the failover command has been executed for this database, it
 *    is no longer in replication slave mode. When this has
 *    happened, SlaveDatabase works exactly as BasicDatabase.
 */

public class SlaveDatabase extends BasicDatabase {
    /** True until SlaveDatabaseBootThread has successfully booted the
     * database. Does not happen until the failover command has been
     * executed for this database */
    private volatile boolean inReplicationSlaveMode;

    /////////////////////////////
    // ModuleControl interface //
    /////////////////////////////
    /**
     * Determines whether this Database implementation should be used
     * to boot the database.
     * @param startParams The properties used to decide if
     * SlaveDatabase is the correct implementation of Database for the
     * database to be booted. 
     * @return true if the database is updatable (not read-only) and
     * replication slave mode is specified in startParams
     */
    public boolean canSupport(Properties startParams) {

        boolean supported =
            Monitor.isDesiredCreateType(startParams, getEngineType());
        if (supported) {
            String repliMode =
                startParams.getProperty(SlaveFactory.REPLICATION_MODE);
            if (repliMode == null ||
                !repliMode.equals(SlaveFactory.SLAVE_MODE)) {
                supported = false;
            }
        }

        return supported;
    }

    public void boot(boolean create, Properties startParams)
        throws StandardException {

        inReplicationSlaveMode = true;

        // SlaveDatabaseBootThread is an internal class
        SlaveDatabaseBootThread dbBootThread =
            new SlaveDatabaseBootThread(create, startParams);
        new Thread(dbBootThread).start();

        try {
            // We cannot claim to be booted until the storage factory
            // has been set in the startParams because
            // TopService.bootModule (the caller of this method) uses
            // the storage factory object. The storage factory is set
            // in RawStore.boot, and we have to wait for this to
            // happen.
            UpdateServiceProperties usp =
                (UpdateServiceProperties) startParams;
            while (usp.getStorageFactory() == null){
                Thread.sleep(500);
            }
        } catch (Exception e) {
            //Todo: report exception to derby.log
        }

        // This module has now been booted (hence active=true) even
        // though submodules like store and authentication may not
        // have completed their boot yet. We deal with that by raising
        // an error on attempts to use these 
        active=true;
    }

    /////////////////////
    // Class interface //
    /////////////////////
    public SlaveDatabase() {
    }

    ////////////////////////
    // Database interface //
    ////////////////////////
    public LanguageConnectionContext setupConnection(ContextManager cm, 
                                                     String user, 
                                                     String drdaID, 
                                                     String dbname)
        throws StandardException {

        if (inReplicationSlaveMode) {
            // do not allow connections to a database that is
            // currently in replication slave move
            throw StandardException.newException(
                        SQLState.CANNOT_CONNECT_TO_DB_IN_SLAVE_MODE, dbname);
        }
        return super.setupConnection(cm, user, drdaID, dbname);
    }

    public AuthenticationService getAuthenticationService() {
        if (inReplicationSlaveMode) {
            // Cannot get authentication service for a database that
            // is currently in replication slave move
            // Todo: throw exception
        }
        return super.getAuthenticationService();
    }

    /////////////////
    // Inner Class //
    /////////////////
    /**
     * Thread that boots the slave database. Will be blocked in
     * LogFactory.recover until database is no longer in slave
     * replication mode.
     */
    private class SlaveDatabaseBootThread implements Runnable {

        private boolean create;
        private Properties params;

        public SlaveDatabaseBootThread(boolean create, Properties startParams){
            this.create = create;
            params = startParams;
        }

        public void run() {

            // The thread needs a ContextManager since two threads
            // cannot share a context
            ContextManager bootThreadCm;
            try {

                bootThreadCm = ContextService.getFactory().newContextManager();
                ContextService.getFactory().
                    setCurrentContextManager(bootThreadCm);

                bootBasicDatabase(create, params); // will be blocked

            } catch (StandardException se) {
                //todo - report exception
            } finally {
                inReplicationSlaveMode = false;
                //todo: tear down context
            }
        }
    }

    private void bootBasicDatabase(boolean create, Properties params)
        throws StandardException {
        // This call will be blocked while slave replication mode is
        // active
        super.boot(create, params);
    }
}
