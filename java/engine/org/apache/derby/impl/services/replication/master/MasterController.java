/*
 
   Derby - Class
   org.apache.derby.impl.services.replication.master.MasterController
 
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

package org.apache.derby.impl.services.replication.master;

import java.io.IOException;
import java.net.SocketTimeoutException;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.MessageId;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.services.monitor.ModuleControl;
import org.apache.derby.iapi.services.monitor.ModuleSupportable;
import org.apache.derby.iapi.services.monitor.Monitor;

import org.apache.derby.iapi.store.raw.RawStoreFactory;
import org.apache.derby.iapi.store.raw.log.LogFactory;
import org.apache.derby.iapi.store.raw.data.DataFactory;

import org.apache.derby.iapi.services.replication.master.MasterFactory;

import org.apache.derby.impl.services.replication.net.ReplicationMessage;
import org.apache.derby.impl.services.replication.ReplicationLogger;
import org.apache.derby.impl.services.replication.net.ReplicationMessageTransmit;
import org.apache.derby.impl.services.replication.buffer.ReplicationLogBuffer;
import org.apache.derby.impl.services.replication.buffer.LogBufferFullException;

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

    private RawStoreFactory rawStoreFactory;
    private DataFactory dataFactory;
    private LogFactory logFactory;
    private ReplicationLogBuffer logBuffer;
    private AsynchronousLogShipper logShipper;
    private ReplicationMessageTransmit transmitter; 

    private String replicationMode;
    private String slavehost;
    private int slaveport;
    private String dbname;
    
    //Set to true when stopMaster is called
    private boolean stopMasterController = false;

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
     * Used by Monitor.bootServiceModule to start the service. Will:
     *
     * Set up basic variables
     * Connect to the slave using the network service (DERBY-2921)
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

        replicationMode =
            properties.getProperty(MasterFactory.REPLICATION_MODE);

        slavehost = properties.getProperty(MasterFactory.SLAVE_HOST);

        String port = properties.getProperty(MasterFactory.SLAVE_PORT);
        if (port != null) {
            slaveport = new Integer(port).intValue();
        }

        dbname = properties.getProperty(MasterFactory.MASTER_DB);
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
     * Will stop the replication master service
     *
     * Not implemented yet
     */
    public void stop() { }

    ////////////////////////////////////////////////////////////
    // Implementation of methods from interface MasterFactory //
    ////////////////////////////////////////////////////////////

    /**
     * Will perform all the work that is needed to set up replication.
     *
     * @param rawStore The RawStoreFactory for the database
     * @param dataFac The DataFactory for this database
     * @param logFac The LogFactory ensuring recoverability for this database
     * @exception StandardException Standard Derby exception policy,
     * thrown on replication startup error. 
     */
    public void startMaster(RawStoreFactory rawStore,
                            DataFactory dataFac, LogFactory logFac) 
                            throws StandardException {
        stopMasterController = false;
        rawStoreFactory = rawStore;
        dataFactory = dataFac;
        logFactory = logFac;
        logBuffer = new ReplicationLogBuffer(DEFAULT_LOG_BUFFER_SIZE, this);

        try {
            logFactory.startReplicationMasterRole(this);
        
            rawStoreFactory.unfreeze();

            setupConnection();

            if (replicationMode.equals(MasterFactory.ASYNCHRONOUS_MODE)) {
                logShipper = new AsynchronousLogShipper(logBuffer,
                                                        transmitter,
                                                        this);
                ((Thread)logShipper).start();
            }
        } catch (StandardException se) {
            // cleanup everything that may have been started before
            // the exception was thrown
            ReplicationLogger.logError(MessageId.REPLICATION_FATAL_ERROR, null,
                                       dbname);
            logFactory.stopReplicationMasterRole();
            teardownNetwork();
            throw se;
        }

        // Add code that initializes replication by sending the
        // database to the slave, making logFactory add logrecords to
        // the buffer etc. Repliation should be up and running when
        // this method returns.

        Monitor.logTextMessage(MessageId.REPLICATION_MASTER_STARTED, dbname);
    }

    /**
     * Will perform all work that is needed to shut down replication
     */
    public void stopMaster() {
        stopMasterController = true;
        logFactory.stopReplicationMasterRole();
        try {
            logShipper.flushBuffer();
            teardownNetwork();
        } catch (IOException ioe) {
            ReplicationLogger.
                logError(MessageId.REPLICATION_LOGSHIPPER_EXCEPTION,
                         ioe, dbname);
        } catch(StandardException se) {
            ReplicationLogger.
                logError(MessageId.REPLICATION_LOGSHIPPER_EXCEPTION, 
                         se, dbname);
        }
        Monitor.logTextMessage(MessageId.REPLICATION_MASTER_STOPPED, dbname);
    }

    /**
     * @see org.apache.derby.iapi.services.replication.master.MasterFactory#startFailover()
     */
    public void startFailover() throws StandardException {
        //acknowledgment returned from the slave containing
        //the status of the failover performed.
        ReplicationMessage ack = null;
        
        stopMasterController = true;
        
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
                          byte[] log, int logOffset, int logLength){

        try {
            logBuffer.appendLog(greatestInstant, log, logOffset, logLength);
        } catch (LogBufferFullException lbfe) {
            try {
                logShipper.forceFlush();
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
            ReplicationLogger.
                logError(MessageId.REPLICATION_LOGSHIPPER_EXCEPTION, 
                         exception, dbname);
            Monitor.logTextMessage(MessageId.REPLICATION_MASTER_RECONN, dbname);
            
            while (!stopMasterController) {
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
     * @param t the throwable that needs to be handled.
     */
    private void printStackAndStopMaster(Throwable t) {
        ReplicationLogger.
            logError(MessageId.REPLICATION_LOGSHIPPER_EXCEPTION, t, dbname);
        stopMaster();
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

}
