/*
 
   Derby - Class
   org.apache.derby.impl.store.replication.master.MasterController
 
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

package org.apache.derby.impl.store.replication.master;

import java.io.IOException;
import java.net.SocketTimeoutException;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.MessageId;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.services.monitor.ModuleControl;
import org.apache.derby.iapi.services.monitor.ModuleSupportable;
import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.iapi.services.property.PropertyUtil;
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.reference.Property;

import org.apache.derby.iapi.store.raw.RawStoreFactory;
import org.apache.derby.iapi.store.raw.log.LogFactory;
import org.apache.derby.iapi.store.raw.data.DataFactory;

import org.apache.derby.iapi.store.replication.master.MasterFactory;

import org.apache.derby.impl.store.replication.net.ReplicationMessage;
import org.apache.derby.impl.store.replication.ReplicationLogger;
import org.apache.derby.impl.store.replication.net.ReplicationMessageTransmit;
import org.apache.derby.impl.store.replication.buffer.ReplicationLogBuffer;
import org.apache.derby.impl.store.replication.buffer.LogBufferFullException;

import java.util.Properties;

/**
 * <p> 
 * This is an implementation of the replication master controller
 * service. The service is booted when this instance of Derby will
 * have the replication master role for this database.
 * </p> 
 * <p>
 * Note: The current version of the class is far from complete. Code
 * to control the replication master behavior will be added as more
 * parts of the replication functionality is added to Derby. 
 * </p>
 *
 * @see MasterFactory
 */
public class MasterController 
        implements MasterFactory, ModuleControl, ModuleSupportable {

    private static final int DEFAULT_LOG_BUFFER_SIZE = 32768; //32K
    private static final int LOG_BUFFER_SIZE_MIN = 8192; //8KB
    private static final int LOG_BUFFER_SIZE_MAX = 1024*1024; //1MB

    private RawStoreFactory rawStoreFactory;
    private DataFactory dataFactory;
    private LogFactory logFactory;
    private ReplicationLogBuffer logBuffer;
    private AsynchronousLogShipper logShipper;
    private ReplicationMessageTransmit transmitter; 
    private ReplicationLogger repLogger;

    private String replicationMode;
    private String slavehost;
    private int slaveport;
    private String dbname;
    private int logBufferSize = 0;
    
    //Indicates whether the Master Controller is currently
    //active
    private boolean active = false;

    //How long to wait before reporting the failure to
    //establish a connection with the slave.
    // TODO: make this configurable through a property
    private static final int SLAVE_CONNECTION_ATTEMPT_TIMEOUT = 5000;


    /**
     * Empty constructor required by Monitor.bootServiceModule
     */
    public MasterController() { }

    ////////////////////////////////////////////////////////////
    // Implementation of methods from interface ModuleControl //
    ////////////////////////////////////////////////////////////

    /**
     * Used by Monitor.bootServiceModule to start the service. Currently
     * only used to set up the replication mode.
     *
     * @param create Currently ignored
     * @param properties Properties used to start the service in the
     *                   correct mode. Currently initializes only the
     *                   replicationMode property.
     * @exception StandardException Standard Derby exception policy,
     * thrown on error.
     */
    public void boot(boolean create, Properties properties)
        throws StandardException {
        //The boot method is loaded only once, because of that the
        //boot time parameters once wrong would result in repeated
        //startMaster attempts failing. In order to allow for
        //multiple start master attempts the slave host name, port
        //number and the dbname have been moved to the startMaster
        //method.
        replicationMode =
            properties.getProperty(MasterFactory.REPLICATION_MODE);
    }

    ////////////////////////////////////////////////////////////////
    // Implementation of methods from interface ModuleSupportable //
    ////////////////////////////////////////////////////////////////

    /**
     * Used by Monitor.bootServiceModule to check if this class is
     * usable for replication. To be usable, we require that
     * asynchronous replication is specified in startParams by
     * checking that a property with key
     * MasterFactory.REPLICATION_MODE has the value
     * MasterFactory.ASYNCHRONOUS_MODE. 
     * @param startParams The properties used to boot replication
     * @return true if asynchronous replication is requested, meaning
     * that this MasterController is a suitable implementation for the
     * MasterFactory service. False otherwise
     * @see ModuleSupportable#canSupport 
     */
    public boolean canSupport(Properties startParams) {
        String modeParam =
            startParams.getProperty(MasterFactory.REPLICATION_MODE);

        // currently only one attribute: asynchronous replication mode
        if (modeParam != null && 
            modeParam.equals(MasterFactory.ASYNCHRONOUS_MODE)) {
            return true;
        } else {
            return false;
        }
    }
    
    /**
     * Will stop the replication master service.
     */
    public void stop() { 
        try {
            stopMaster();
        } catch (StandardException se) {
            repLogger.
                logError(MessageId.REPLICATION_MASTER_STOPPED, se);
        }
       
    }

    ////////////////////////////////////////////////////////////
    // Implementation of methods from interface MasterFactory //
    ////////////////////////////////////////////////////////////

    /**
     * Will perform all the work that is needed to set up replication.
     *
     * @param rawStore The RawStoreFactory for the database
     * @param dataFac The DataFactory for this database
     * @param logFac The LogFactory ensuring recoverability for this database
     * @param slavehost The hostname of the slave
     * @param slaveport The port the slave is listening on
     * @param dbname The master database that is being replicated.
     * @exception StandardException Standard Derby exception policy,
     *                              1) thrown on replication startup error
     *                              2) thrown if the master has already been
     *                                 booted.
     *                              3) thrown if the specified replication mode
     *                                 is not supported.
     */
    public void startMaster(RawStoreFactory rawStore,
                            DataFactory dataFac,
                            LogFactory logFac,
                            String slavehost,
                            int slaveport,
                            String dbname)
                            throws StandardException {
        if (active) {
            //It is wrong to attempt startMaster on a already
            //started master.
            throw StandardException.newException
                    (SQLState.REPLICATION_MASTER_ALREADY_BOOTED, dbname);
        }

        this.slavehost = slavehost;
        this.slaveport = new Integer(slaveport).intValue();
        this.dbname = dbname;

        rawStoreFactory = rawStore;
        dataFactory = dataFac;
        logFactory = logFac;

        repLogger = new ReplicationLogger(dbname);
        getMasterProperties();
        logBuffer = new ReplicationLogBuffer(logBufferSize, this);

        try {
            logFactory.startReplicationMasterRole(this);
        
            rawStoreFactory.unfreeze();

            setupConnection();

            if (replicationMode.equals(MasterFactory.ASYNCHRONOUS_MODE)) {
                logShipper = new AsynchronousLogShipper(logBuffer,
                                                        transmitter,
                                                        this,
                                                        repLogger);
                logShipper.setDaemon(true);
                logShipper.start();
            }
        } catch (StandardException se) {
            // cleanup everything that may have been started before
            // the exception was thrown
            repLogger.logError(MessageId.REPLICATION_FATAL_ERROR, se);
            logFactory.stopReplicationMasterRole();
            teardownNetwork();
            throw se;
        }

        //The master has been started successfully.
        active = true;

        // Add code that initializes replication by sending the
        // database to the slave, making logFactory add logrecords to
        // the buffer etc. Repliation should be up and running when
        // this method returns.

        Monitor.logTextMessage(MessageId.REPLICATION_MASTER_STARTED, dbname);
    }

    /**
     * Will perform all work that is needed to shut down replication.
     *
     * @throws StandardException If the replication master has been stopped
     *                           already.
     */
    public void stopMaster() throws StandardException {
        if (!active) {
            throw StandardException.newException
                    (SQLState.REPLICATION_NOT_IN_MASTER_MODE);
        }
        active = false;
        logFactory.stopReplicationMasterRole();
        try {
            logShipper.flushBuffer();
        } catch (IOException ioe) {
            repLogger.
                logError(MessageId.REPLICATION_LOGSHIPPER_EXCEPTION, ioe);
        } catch(StandardException se) {
            repLogger.
                logError(MessageId.REPLICATION_LOGSHIPPER_EXCEPTION, se);
        } finally {
            teardownNetwork();
        }
        Monitor.logTextMessage(MessageId.REPLICATION_MASTER_STOPPED, dbname);
    }

    /**
     * @see MasterFactory#startFailover()
     */
    public void startFailover() throws StandardException {
        if (!active) {
            //It is not correct to stop the master and then attempt a failover.
            //The control would come here because the master module is already
            //loaded and a findService for the master module will not fail. But
            //since this module has been stopped failover does not suceed.
            throw StandardException.newException
                    (SQLState.REPLICATION_NOT_IN_MASTER_MODE);
        }

        //acknowledgment returned from the slave containing
        //the status of the failover performed.
        ReplicationMessage ack = null;
        
        //A failover stops the master controller and shuts down
        //the master database.
        active = false;
        
        //freeze the database to stop clients when this command is received
        rawStoreFactory.freeze();
        
        try {
            //Flush the log buffer of any remaining log records.
            logShipper.flushBuffer();
            
            //Send the failover message to the slave and wait for 
            //acknowledgement.
            ReplicationMessage mesg = new ReplicationMessage(
                        ReplicationMessage.TYPE_FAILOVER, null);
            transmitter.sendMessage(mesg);
            ack = transmitter.readMessage();
        } catch (IOException ioe) {
            handleFailoverFailure(ioe);
        } catch (StandardException se) {
            handleFailoverFailure(se);
        } catch (ClassNotFoundException cnfe) {
            handleFailoverFailure(cnfe);
        }
        
        //check the contents of the acknowledgement received from the slave
        //and perform appropriate actions.
        if (ack == null) {
            //ack can be null if the wait on the socket stream timed out
            handleFailoverFailure(null);
        } else if (ack.getType() == ReplicationMessage.TYPE_ACK) {
            //An exception is thrown to indicate the successful completion 
            //of failover. Also the AsynchronousLogShipper thread is terminated.
            //The socket connection that is obtained needs to be torn down.
            //The exception thrown is of Database Severity, this shuts
            //down the master database.
            teardownNetwork();

            //If we require an exception of Database Severity to shutdown the
            //database to shutdown the database we need to unfreeze first
            //before throwing the exception. Unless we unfreeze the shutdown
            //hangs.
            rawStoreFactory.unfreeze();

            throw StandardException.newException
                    (SQLState.REPLICATION_FAILOVER_SUCCESSFUL, dbname);  
        } else {
            //TYPE_ACK is the only type that is returned. ack can
            //ideally not contain any other type. The program should
            //ideally not come here.
           handleFailoverFailure(null);
        }
    }

    /**
     * Load relevant system property: replication log buffer size
     */
    private void getMasterProperties() {
        logBufferSize =
            PropertyUtil.getSystemInt(Property.REPLICATION_LOG_BUFFER_SIZE,
                                      DEFAULT_LOG_BUFFER_SIZE);

        if (logBufferSize < LOG_BUFFER_SIZE_MIN) {
            logBufferSize = LOG_BUFFER_SIZE_MIN;
            if (SanityManager.DEBUG) {
                repLogger.logText("Replication log buffer size " +
                                  "property too small. Set to " +
                                  "minimum value: " + logBufferSize,
                                  false);
            }
        }  else if (logBufferSize > LOG_BUFFER_SIZE_MAX) {
            logBufferSize = LOG_BUFFER_SIZE_MAX;
            if (SanityManager.DEBUG) {
                repLogger.logText("Replication log buffer size " +
                                  "property too big. Set to " +
                                  "maximum value: " + logBufferSize,
                                  false);
            }
        }
    }
    
    /**
     * used to handle the case when an attempt to failover the database
     * fails.
     *
     * @param t        The throwable which resulted in the aborted failover
     *                 attempt.
     * 
     * @throws StandardException Indicating the reason for the aborted
     *                          failover attempt. 
     */
    private void handleFailoverFailure(Throwable t) 
    throws StandardException {
        teardownNetwork();
        rawStoreFactory.unfreeze();
        if (t != null) {
            throw StandardException.newException
                        (SQLState.REPLICATION_FAILOVER_UNSUCCESSFUL, t, dbname);
        } else {
            throw StandardException.newException
                        (SQLState.REPLICATION_FAILOVER_UNSUCCESSFUL, dbname);
        }
    }
    
    /**
     * Append a chunk of log records to the log buffer. The method is not 
     * threadsafe; only one thread should access this method at a time. 
     *
     * @param greatestInstant   the instant of the log record that was
     *                          added last to this chunk of log
     * @param log               the chunk of log records
     * @param logOffset         offset in log to start copy from
     * @param logLength         number of bytes to copy, starting
     *                          from logOffset
     **/
    public void appendLog(long greatestInstant,
                          byte[] log, int logOffset, int logLength){

        try {
            logBuffer.appendLog(greatestInstant, log, logOffset, logLength);
        } catch (LogBufferFullException lbfe) {
            try {
                logShipper.forceFlush();
                // There should now be room for this log chunk in the buffer
                appendLog(greatestInstant, log, logOffset, logLength);
            } catch (IOException ioe) {
                printStackAndStopMaster(ioe);
            } catch (StandardException se) {
                printStackAndStopMaster(se);
            }
        }
    }

    /**
     * Used by the LogFactory to notify the replication master
     * controller that the log records up to this instant have been
     * flushed to disk. The master controller takes action according
     * to the current replication strategy when this method is called.
     *
     * When the asynchronous replication strategy is used, the method
     * does not force log shipping to the slave; the log records may
     * be shipped now or later at the MasterController's discretion.
     *
     * However, if another strategy like 2-safe replication is
     * implemented in the future, a call to this method may force log
     * shipment before returning control to the caller.
     *
     * Currently, only asynchronous replication is supported.
     *
     * Not implemented yet
     *
     * @param instant The highest log instant that has been flushed to
     * disk
     *
     * @see MasterFactory#flushedTo
     * @see LogFactory#flush
     */
    public void flushedTo(long instant) {
        logShipper.flushedInstance(instant); 
    }
    
    /**
     * Connects to the slave being replicated to.
     *
     * @throws StandardException If a failure occurs while trying to open
     *                           the connection to the slave.
     */
    private void setupConnection() throws StandardException {
        try {
            transmitter = new ReplicationMessageTransmit(slavehost, slaveport);
            // getHighestShippedInstant is -1 until the first log
            // chunk has been shipped to the slave. If a log chunk has
            // been shipped, use the instant of the latest shipped log
            // record to synchronize log files. If no log has been
            // shipped yet, use the end position of the log (i.e.,
            // logToFile.getFirstUnflushedInstantAsLong). 
            if (logShipper != null && 
                logShipper.getHighestShippedInstant() != -1) {
                transmitter.initConnection(SLAVE_CONNECTION_ATTEMPT_TIMEOUT,
                                           logShipper.
                                           getHighestShippedInstant());
            } else {
                transmitter.initConnection(SLAVE_CONNECTION_ATTEMPT_TIMEOUT,
                                           logFactory.
                                           getFirstUnflushedInstantAsLong());
            }
        } catch (SocketTimeoutException ste) {
            throw StandardException.newException
                    (SQLState.REPLICATION_MASTER_TIMED_OUT, dbname);
        } catch (IOException ioe) {
            throw StandardException.newException
                    (SQLState.REPLICATION_CONNECTION_EXCEPTION, ioe, 
                     dbname, slavehost, String.valueOf(slaveport));
        } catch (StandardException se) {
            throw se;
        } catch (Exception e) {
            throw StandardException.newException
                    (SQLState.REPLICATION_CONNECTION_EXCEPTION, e,
                     dbname, slavehost, String.valueOf(slaveport));
        }
    }
    
    /**
     * Used to handle the exceptions (IOException and StandardException) from 
     * the log shipper.
     *
     * @param exception the exception which caused the log shipper to terminate
     *                  in an unexcepted manner.
     */
    void handleExceptions(Exception exception) {
        if (exception instanceof IOException) {
            repLogger.logError(MessageId.REPLICATION_LOGSHIPPER_EXCEPTION,
                               exception);
            Monitor.logTextMessage(MessageId.REPLICATION_MASTER_RECONN, dbname);
            
            while (active) {
                try {
                    transmitter = new ReplicationMessageTransmit
                            (slavehost, slaveport);

                    // see comment in setupConnection
                    if (logShipper != null &&
                        logShipper.getHighestShippedInstant() != -1) {
                        transmitter.
                            initConnection(SLAVE_CONNECTION_ATTEMPT_TIMEOUT,
                                           logShipper.
                                           getHighestShippedInstant());
                    } else {
                        transmitter.
                            initConnection(SLAVE_CONNECTION_ATTEMPT_TIMEOUT,
                                           logFactory.
                                           getFirstUnflushedInstantAsLong());
                    }

                    break;
                } catch (SocketTimeoutException ste) {
                    continue;
                } catch (IOException ioe) {
                    continue;
                } catch (Exception e) {
                    printStackAndStopMaster(e);
                }
            }
        } else if (exception instanceof StandardException) {
            printStackAndStopMaster(exception);
        }
    }
    
    /**
     * used to print the error stack for the given exception and
     * stop the master.
     *
     * @param e the exception that needs to be handled.
     */
    private void printStackAndStopMaster(Exception e) {
        repLogger.logError(MessageId.REPLICATION_LOGSHIPPER_EXCEPTION, e);
        try {
            stopMaster();
        } catch (StandardException se) {
            //The stop master threw an exception saying the replication
            //has been stopped already.
            repLogger.
                logError(MessageId.REPLICATION_MASTER_STOPPED, se);
        }
    }
    
    /**
     * Used to notify the log shipper that a log buffer element is full.
     */
    public void workToDo() {
        logShipper.workToDo();
    }

    /**
     * Stop log shipping, notify slave that replication is stopped and
     * tear down network connection with slave.
     */
    private void teardownNetwork() {

        if (logShipper != null) {
            logShipper.stopLogShipment();
        }

        if (transmitter != null) {
            try {
                ReplicationMessage mesg =
                    new ReplicationMessage(ReplicationMessage.TYPE_STOP, null);
                transmitter.sendMessage(mesg);
            } catch (IOException ioe) {}
            try {
                transmitter.tearDown();
            } catch (IOException ioe) {}
        }
    }

    /**
     * <p>
     * Returns a name of a database associated with this master
     * controller.
     * </p>
     * 
     * <p>
     * Note: The only purpose of the method as of now is to give a 
     * meaningful name to a log shipper thread. The log shipper thread 
     * name should contain a name of a corresponding master database,
     * and this method is used to access it.
     * </p>
     * 
     * @return a master database name
     */
    String getDbName() {
        return this.dbname;
    }
}
