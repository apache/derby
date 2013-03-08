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

import org.apache.derby.iapi.error.PublicAPI;
import org.apache.derby.iapi.reference.Attribute;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.jdbc.AuthenticationService;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.services.context.ContextService;
import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.iapi.util.InterruptStatus;
import org.apache.derby.iapi.store.replication.slave.SlaveFactory;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;

import org.apache.derby.jdbc.InternalDriver;

import java.sql.SQLException;
import java.util.Properties;
import org.apache.derby.iapi.reference.MessageId;
import org.apache.derby.impl.store.replication.ReplicationLogger;

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
    private volatile boolean shutdownInitiated;

    /** True until this database has been successfully booted. Any
     * exception that occurs while inBoot is true will be handed to
     * the client thread booting this database. */
    private volatile boolean inBoot;

    /** Set by the database boot thread if it fails before slave mode
     * has been started properly (i.e., if inBoot is true). This
     * exception will then be reported to the client connection. */
    private volatile StandardException bootException;
    private String dbname; // The name of the replicated database
    private volatile SlaveFactory slaveFac;

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
        inBoot = true;
        shutdownInitiated = false;

        dbname = startParams.getProperty(SlaveFactory.SLAVE_DB);

        // SlaveDatabaseBootThread is an internal class
        SlaveDatabaseBootThread dbBootThread =
            new SlaveDatabaseBootThread(create, startParams);
        Thread sdbThread = 
                new Thread(dbBootThread, "derby.slave.boot-" + dbname);
        sdbThread.setDaemon(true);
        sdbThread.start();

        // Check that the database was booted successfully, or throw
        // the exception that caused the boot to fail.
        verifySuccessfulBoot();

        inBoot = false;

        // This module has now been booted (hence active=true) even
        // though submodules like store and authentication may not
        // have completed their boot yet. We deal with that by raising
        // an error on attempts to use these 
        active=true;
    }

    /**
     * Called by Monitor when this module is stopped, i.e. when the
     * database is shut down. When the database is shut down using the
     * stopSlave command, the stopReplicationSlave method has already
     * been called when this method is called. In this case, the
     * replication functionality has already been stopped. If the
     * database is shutdown as part of a system shutdown, however, we
     * need to cleanup slave replication as part of database shutdown.
     */
    public void stop() {
        if (inReplicationSlaveMode && slaveFac != null) {
            try {
                slaveFac.stopSlave(true);
            } catch (StandardException ex) {
            } finally {
                slaveFac = null;
            }
        }
        super.stop();
    }
    
    /////////////////////
    // Class interface //
    /////////////////////
    public SlaveDatabase() {
    }

    ////////////////////////
    // Database interface //
    ////////////////////////
    public boolean isInSlaveMode() {
        return inReplicationSlaveMode;
    }
    
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

    public AuthenticationService getAuthenticationService()
        throws StandardException{
        if (inReplicationSlaveMode) {
            // Cannot get authentication service for a database that
            // is currently in replication slave move
            throw StandardException.newException(
                SQLState.CANNOT_CONNECT_TO_DB_IN_SLAVE_MODE, dbname);
        }
        return super.getAuthenticationService();
    }

    /**
     * Verify that a connection to stop the slave has been made from
     * here. If verified, the database context is given to the method
     * caller. This will ensure this database is shutdown when an
     * exception with database severity is thrown. If not verified, an
     * exception is thrown.
     * 
     * @exception StandardException Thrown if a stop slave connection
     * attempt was not made from this class
     */
    public void verifyShutdownSlave() throws StandardException {
        if (!shutdownInitiated) {
            throw StandardException.
                newException(SQLState.REPLICATION_STOPSLAVE_NOT_INITIATED);
        }
        pushDbContext(ContextService.getFactory().
                      getCurrentContextManager());
    }

    /**
     * Stop replication slave mode if replication slave mode is active and 
     * the network connection with the master is down
     * 
     * @exception SQLException Thrown on error, if not in replication 
     * slave mode or if the network connection with the master is not down
     */
    public  void stopReplicationSlave() throws SQLException {

        if (shutdownInitiated) {
            // The boot thread has failed or stopReplicationSlave has
            // already been called. There is nothing more to do to
            // stop slave replication mode.
            return;
        }
        
        if (!inReplicationSlaveMode) {
            StandardException se = StandardException.
                newException(SQLState.REPLICATION_NOT_IN_SLAVE_MODE);
            throw PublicAPI.wrapStandardException(se);
        }

        // stop slave without using force, meaning that this method
        // call will fail with an exception if the network connection
        // with the master is up
        try {
            slaveFac.stopSlave(false);
        } catch (StandardException se) {
            throw PublicAPI.wrapStandardException(se);
        }

        slaveFac = null;
    }

    public void failover(String dbname) throws StandardException {
        if (inReplicationSlaveMode) {
            slaveFac.failover();
            // SlaveFactory#failover will make the
            // SlaveDatabaseBootThread complete booting of the store
            // modules, and inReplicationSlaveMode will then be set to
            // false (see SlaveDatabaseBootThread#run).
            // Wait until store is completely booted before returning from
            // this method
            while (inReplicationSlaveMode) {
                try {
                    Thread.sleep(500);
                } catch (InterruptedException ie) {
                    InterruptStatus.setInterrupted();
                }
            }
        } else {
            // If failover is performed on a master that has been a slave
            // earlier
            super.failover(dbname);
        }
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
            ContextManager bootThreadCm = null;
            try {

                bootThreadCm = ContextService.getFactory().newContextManager();
                ContextService.getFactory().
                    setCurrentContextManager(bootThreadCm);

                bootBasicDatabase(create, params); // will be blocked

                // if we get here, failover has been called and the
                // database can now be connected to
                inReplicationSlaveMode = false; 

                if (bootThreadCm != null) {
                    ContextService.getFactory().
                        resetCurrentContextManager(bootThreadCm);
                    bootThreadCm = null;
                }
            } catch (Exception e) {
                // We get here when SlaveController#stopSlave has been called,
                // a fatal Derby exception has been thrown, or if a run-time
                // error is thrown.  Log the error unconditionally to make sure
                // it can be observed, since if this happens during or after
                // recovery on a failover, there will be no connection attempt
                // failing with the error. New connection attempts will just
                // hang...

                ReplicationLogger rl = new ReplicationLogger(dbname);
                rl.logError(MessageId.REPLICATION_FATAL_ERROR, e);
                
                if (e instanceof StandardException) {
                    handleShutdown((StandardException)e);
                }
            } 
        }
    }

    ////////////////////
    // Private Methods//
    ////////////////////

    /**
     * Verify that the slave functionality has been properly started.
     * This method will block until a successful slave startup has
     * been confirmed, or it will throw the exception that caused it
     * to fail.
     */
    private void verifySuccessfulBoot() throws StandardException {
        while (!(isSlaveFactorySet() && slaveFac.isStarted())) {
            if (bootException != null) {
                throw bootException;
            } else {
                try {
                    Thread.sleep(500);
                } catch (InterruptedException ie) {
                    InterruptStatus.setInterrupted();
                }
            }
        }

        if (bootException != null) {

            // DERBY-4186: This is a corner case. Master made us shut down
            // before the initial connect which establishes the slave has
            // finalized it setting up of the slave and returned control to the
            // application. bootException is set while we (application thread)
            // are waiting in the sleep in the loop above (by the
            // SlaveDatabaseBootThread thread in its call to handleShutdown),
            // and this was previously ignored.
            throw bootException;
        }
    }

    /** 
     * If slaveFac (the reference to the SlaveFactory) has not already
     * been set, this method will try to set it by calling
     * Monitor.findServiceModule. If slavFac was already set, the
     * method does not do anything.
     *
     * @return true if slaveFac is set after calling this method,
     * false otherwise
     */
    private boolean isSlaveFactorySet() {
        if (slaveFac != null) {
            return true;
        }

        try {
            slaveFac = (SlaveFactory)Monitor.
                findServiceModule(this, SlaveFactory.MODULE);
            return true;
        } catch (StandardException se) {
            // We get a StandardException if SlaveFactory has not been 
            // booted yet. Safe to retry later.
            return false;
        }
    }

    /**
     * Used to shutdown this database. 
     *
     * If an error occurs as part of the database boot process, we
     * hand the exception that caused boot to fail to the client
     * thread. The client thread will in turn shut down this database.
     *
     * If an error occurs at a later stage than during boot, we shut
     * down the database by setting up a connection with the shutdown
     * attribute. The internal connection is required because database
     * shutdown requires EmbedConnection to do cleanup.
     *
     * @param shutdownCause the reason why the database needs to be
     * shutdown
     */
    private void handleShutdown(StandardException shutdownCause) {
        if (inBoot) {
            bootException = shutdownCause;
            return;
        } 
        try {
            shutdownInitiated = true;

            String conStr = "jdbc:derby:"+dbname+";"+
                Attribute.REPLICATION_INTERNAL_SHUTDOWN_SLAVE+
                "=true";

            InternalDriver driver = InternalDriver.activeDriver();
            if (driver != null) {
                driver.connect( conStr, (Properties) null, 0 );
            }
        } catch (Exception e) {
            // Todo: report error to derby.log if exception is not
            // SQLState.SHUTDOWN_DATABASE
        }
    }

    private void bootBasicDatabase(boolean create, Properties params)
        throws StandardException {
        // This call will be blocked while slave replication mode is
        // active
        super.boot(create, params);
    }
}
