/*
 
   Derby - Class
   org.apache.derby.iapi.services.replication.slave.SlaveFactory
 
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

package org.apache.derby.iapi.services.replication.slave;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.Property;

import org.apache.derby.iapi.store.raw.RawStoreFactory;
import org.apache.derby.iapi.store.raw.log.LogFactory;

/**
 * <p> 
 * This is the interface for the replication slave controller
 * service. The slave controller service is booted when this instance
 * of Derby will have the replication slave role for this database.
 * </p> 
 * <p> 
 * The replication slave service is responsible for managing all
 * replication related functionality on the slave side of replication.
 * This includes connecting to the master and apply log records
 * received from the master.
 * </p> 
 */
public interface SlaveFactory {

    /** The name of the Slave Factory, used to boot the service.  */
    public static final String MODULE =
        "org.apache.derby.iapi.services.replication.slave.SlaveFactory";

    /* Strings used as keys in the Properties objects*/

    /** Property key to specify which host to listen to */
    public static final String SLAVE_HOST =
        Property.PROPERTY_RUNTIME_PREFIX + "replication.slave.slavehost";

    /** Property key to specify which port to listen to */
    public static final String SLAVE_PORT =
        Property.PROPERTY_RUNTIME_PREFIX + "replication.slave.slaveport";

    /** Property key to specify the name of the database */
    public static final String SLAVE_DB =
        Property.PROPERTY_RUNTIME_PREFIX + "replication.slave.dbname";

    /** Property key to specify replication mode */
    public static final String REPLICATION_MODE =
        Property.PROPERTY_RUNTIME_PREFIX + "replication.slave.mode";

    /* Strings used as values in the Properties objects */

    /**
     * Property value used to indicate that the service should be
     * booted in asynchronous replication mode.
     */
    public static final String SLAVE_MODE =
        Property.PROPERTY_RUNTIME_PREFIX + "slavemode";


    /* Required methods */

    /**
     * Start slave replication. This method establishes a network
     * connection with the associated replication master and starts a
     * daemon that applies operations received from the master (in the
     * form of log records) to the local slave database.
     *
     * @param rawStore The RawStoreFactory for the database
     * @param logFac The LogFactory ensuring recoverability for this database
     *
     * @exception StandardException Thrown if the slave could not be
     * started.
     */
    public void startSlave(RawStoreFactory rawStore, LogFactory logFac)
        throws StandardException;

    /**
     * Will perform all work that is needed to stop replication
     */
    public void stopSlave();

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
    public void failover();

}
