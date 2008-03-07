/*
 
   Derby - Class
   org.apache.derby.iapi.store.replication.master.MasterFactory
 
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

package org.apache.derby.iapi.store.replication.master;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.Property;

import org.apache.derby.iapi.store.raw.RawStoreFactory;
import org.apache.derby.iapi.store.raw.log.LogFactory;
import org.apache.derby.iapi.store.raw.data.DataFactory;


/**
 * <p> 
 * This is the interface for the replication master controller
 * service. The master controller service is booted when this instance
 * of Derby will have the replication master role for this database.
 * </p> 
 * <p> 
 * The replication master service is responsible for managing all
 * replication related functionality on the master side of replication.
 * This includes connecting to the slave, setting up a log buffer to
 * temporarily store log records from the LogFactory, and to ship
 * these log records to the slave.
 * </p> 
 * <p> 
 * The master controller currently only supports asynchronous
 * replication. This means that there are no guarantees that
 * transactions that have committed here (the master side) are also
 * reflected on the slave side. However, the slave version of the
 * database IS guaranteed to be transaction consistent. This implies
 * that: <br>
 *
 * <ul>
 *  <li>A transaction t that is committed on the master will either be
 *  fully reflected or not be reflected at all on the slave when the
 *  slave database is turned into a non-replicated database (that is,
 *  at failover time)</li>
 *
 *  <li>Slave execution of operations is in the same serial order as
 *  on the master because replication is based on redoing log records
 *  to the slave. By definition, log records are in serial order. This
 *  implies that if transaction t1 commits before t2 on the master,
 *  and t2 has been committed on the slave, t1 is also guaranteed to
 *  have committed on the slave.</li>
 * </ul>
 * </p>
 */
public interface MasterFactory {

    /** The name of the Master Factory, used to boot the service.  */
    public static final String MODULE =
        "org.apache.derby.iapi.store.replication.master.MasterFactory";

    /* Property names that are used as key values in the Properties objects*/
    
    /** Property key to specify replication mode */
    public static final String REPLICATION_MODE =
        Property.PROPERTY_RUNTIME_PREFIX + "replication.master.mode";

    /* Property values */

    /**
     * Property value used to indicate that the service should be
     * booted in asynchronous replication mode.
     */
    public static final String ASYNCHRONOUS_MODE =
        Property.PROPERTY_RUNTIME_PREFIX + "asynch";


    /* Methods */

    /**
     * Will perform all the work that is needed to set up replication
     *
     * @param rawStore The RawStoreFactory for the database
     * @param dataFac The DataFactory for this database
     * @param logFac The LogFactory ensuring recoverability for this database
     * @param slavehost The hostname for the slave
     * @param slaveport The port the slave is listening on
     * @param dbname The master database that is being replicated.
     * @exception StandardException Standard Derby exception policy,
     * thrown on replication startup error. 
     */
    public void startMaster(RawStoreFactory rawStore,
                            DataFactory dataFac,
                            LogFactory logFac,
                            String slavehost,
                            int slaveport,
                            String dbname)
                            throws StandardException;

    /**
     * Will perform all work that is needed to shut down replication.
     *
     * @throws StandardException If the replication master has been stopped
     *                           already.
     */
    public void stopMaster() throws StandardException;
    
    /**
     * Will perform all work needed to failover
     *
     * @throws StandardException 1) If the failover succeeds, an exception is
     *                              thrown to indicate that the master database
     *                              was shutdown after a successful failover
     *                           2) If a failure occurs during network 
     *                              communication with slave.
     */
    public void startFailover() throws StandardException;

    /**
     * Append a chunk of log records to the log buffer.
     *
     * @param greatestInstant   the instant of the log record that was
     *                          added last to this chunk of log
     * @param log               the chunk of log records
     * @param logOffset         offset in log to start copy from
     * @param logLength         number of bytes to copy, starting
     *                          from logOffset
     **/
    public void appendLog(long greatestInstant,
                          byte[] log, int logOffset, int logLength);

    /**
     * Used by the LogFactory to notify the replication master
     * controller that the log records up to this instant have been
     * flushed to disk. The master controller takes action according
     * to the current replication strategy when this method is called.
     *
     * When the asynchronous replication strategy is used, the method
     * does not force log shipping to the slave; the log records may
     * be shipped now or later at the MasterFactory's discretion.
     *
     * However, if another strategy like 2-safe replication is
     * implemented in the future, a call to this method may force log
     * shipment before returning control to the caller.
     *
     * Currently, only asynchronous replication is supported.
     *
     * @param instant The highest log instant that has been flushed to
     * disk
     * @see LogFactory#flush
     */
    public void flushedTo(long instant);
    
    /**
     * Used to notify the log shipper that a log buffer element is full.
     */
    public void workToDo(); 

}
