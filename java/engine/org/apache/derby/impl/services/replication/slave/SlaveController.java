/*
 
   Derby - Class
   org.apache.derby.impl.services.replication.slave.SlaveController
 
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

package org.apache.derby.impl.services.replication.slave;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.services.monitor.ModuleControl;
import org.apache.derby.iapi.services.monitor.ModuleSupportable;

import org.apache.derby.iapi.store.raw.RawStoreFactory;
import org.apache.derby.iapi.store.raw.log.LogFactory;

import org.apache.derby.iapi.services.replication.slave.SlaveFactory;

import java.util.Properties;

/**
 * <p> 
 * This is an implementation of the replication slave controller
 * service. The service is booted when this instance of Derby will
 * have the replication slave role for this database.
 * </p> 
 * <p>
 * Note: The current version of the class is far from complete. Code
 * to control the replication slave behavior will be added as more
 * parts of the replication functionality is added to Derby. 
 * </p>
 *
 * @see SlaveFactory
 */
public class SlaveController implements SlaveFactory, ModuleControl,
                                        ModuleSupportable {

    private RawStoreFactory rawStoreFactory;
    private LogFactory logFactory;
    // waiting for code to go into trunk:
    //    private NetworkReceive connection; 

    private int slaveport;

    /**
     * Empty constructor required by Monitor.bootServiceModule
     */
    public SlaveController() { }

    ////////////////////////////////////////////////////////////
    // Implementation of methods from interface ModuleControl //
    ////////////////////////////////////////////////////////////

    /**
     * Used by Monitor.bootServiceModule to start the service. It will
     * set up basic variables 
     *
     * Not implemented yet
     *
     * @param create Currently ignored
     * @param properties Properties used to start the service in the
     * correct mode
     * @exception StandardException Standard Derby exception policy,
     * thrown on error.
     */
    public void boot(boolean create, Properties properties)
        throws StandardException {

        String port = properties.getProperty(SlaveFactory.SLAVE_PORT);
        if (port != null) {
            slaveport = new Integer(port).intValue();
        }

        // Added when Network Service has been committed to trunk
        // connection = new NetworkReceive();

        System.out.println("SlaveController booted");
    }

    /**
     * Will tear down the replication slave service. Should be called
     * after either stopSlave or failover have been called.
     *
     * Not implemented yet
     */
    public void stop() { }

    ////////////////////////////////////////////////////////////////
    // Implementation of methods from interface ModuleSupportable //
    ////////////////////////////////////////////////////////////////

    /**
     * Used by Monitor.bootServiceModule to check if this class is
     * usable for replication. To be usable, we require that slave
     * replication mode is specified in startParams by checking that a
     * property with key SlaveFactory.REPLICATION_MODE has the value
     * SlaveFactory.SLAVE_MODE.
     * @param startParams The properties used to start replication
     * @return true if slave repliation is specified, meaning that
     * this MasterController is a suitable implementation for the
     * SlaveFactory service. False otherwise.
     * @see ModuleSupportable#canSupport 
     */
    public boolean canSupport(Properties startParams) {
        String modeParam =
            startParams.getProperty(SlaveFactory.REPLICATION_MODE);

        // currently only one attribute: slave replication mode
        if (modeParam != null && 
            modeParam.equals(SlaveFactory.SLAVE_MODE)) {
            return true;
        } else {
            return false;
        }
    }

    ///////////////////////////////////////////////////////////
    // Implementation of methods from interface SlaveFactory //
    ///////////////////////////////////////////////////////////

    /**
     * Start slave replication. This method establishes a network
     * connection with the associated replication master and starts a
     * daemon that applies operations received from the master (in the
     * form of log records) to the local slave database.
     *
     * Not implemented yet
     *
     * @param rawStore The RawStoreFactory for the database
     * @param logFac The LogFactory ensuring recoverability for this database
     */
    public void startSlave(RawStoreFactory rawStore, LogFactory logFac) {
        // Added when Network Service has been committed to trunk:
        // connection.connect(); // sets up a network connection to the slave

        rawStoreFactory = rawStore;
        logFactory = logFac;

        // Add code that initializes replication by setting up a
        // network connection with the master, receiving the database
        // from the master, make a DaemonService for applying log
        // records etc. Repliation should be up and running when this
        // method returns.

        System.out.println("SlaveController started");
    }

    /**
     * Will perform all work that is needed to stop replication
     *
     * Not implemented yet
     */
    public void stopSlave() {
        System.out.println("SlaveController stopped");
    }

    /**
     * <p>
     * Used to turn this slave instance of the database into a normal
     * instance that clients can connect to. This is typically done in
     * cases where a fatal error has happened on the master instance
     * of the database, or when the master database is unreachable due
     * to network problems.
     * </p>
     * <p>
     * By calling failover, this slave instance of the database will
     * be recovered so that all committed operations that have been
     * received from the master are reflected here. On the other hand,
     * operations from transactions where the commit log record has
     * not been received from the master will not be reflected.
     * </p>
     * <p>
     * Note that even though an operation has been executed (and even
     * committed) on the master, it is not neccessarily reflected in
     * the slave instance of the database. This depends on the
     * replication strategy used by the MasterFactory.
     * </p>
     *
     * @see org.apache.derby.iapi.services.replication.master.MasterFactory
     * @see org.apache.derby.impl.services.replication.master.MasterController#flushedTo
     */
    public void failover() {
        // Apply all received log records, thus completing the boot of
        // this database. The database can be connected to after this.

        // // complete recovery of the database 
        // logFactory.setReplicationMode(false); 

        // Added when Network Service has been committed to trunk:
        // connection.shutdown();

        System.out.println("SlaveController failover");
    }


}
