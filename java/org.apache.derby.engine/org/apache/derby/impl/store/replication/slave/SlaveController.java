/*
 
   Derby - Class
   org.apache.derby.impl.store.replication.slave.SlaveController
 
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

package org.apache.derby.impl.store.replication.slave;

import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.shared.common.reference.Attribute;
import org.apache.derby.shared.common.reference.MessageId;
import org.apache.derby.shared.common.reference.SQLState;
import org.apache.derby.iapi.services.monitor.ModuleControl;
import org.apache.derby.iapi.services.monitor.ModuleSupportable;
import org.apache.derby.iapi.services.monitor.Monitor;

import org.apache.derby.iapi.store.raw.RawStoreFactory;
import org.apache.derby.impl.store.raw.log.LogCounter;
import org.apache.derby.iapi.store.raw.log.LogFactory;
import org.apache.derby.impl.store.raw.log.LogToFile;
import org.apache.derby.impl.store.replication.net.SlaveAddress;

import org.apache.derby.impl.store.replication.ReplicationLogger;
import org.apache.derby.impl.store.replication.net.ReplicationMessage;
import org.apache.derby.impl.store.replication.net.ReplicationMessageReceive;
import org.apache.derby.iapi.store.replication.slave.SlaveFactory;

import java.io.EOFException;
import java.io.IOException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
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
public class SlaveController
    implements SlaveFactory, ModuleControl, ModuleSupportable {


    // How long to wait for a connection to be established with the
    // master before timing out. Note that this is done so that we can
    // detect if replication slave mode has been stopped. If
    // replication mode has not been stopped, a new attempt is made to
    // set up the connection. 
    // TODO: make this configurable through a property
    private static final int DEFAULT_SOCKET_TIMEOUT = 1000; // 1 second

    private RawStoreFactory rawStoreFactory;
    private LogToFile logToFile;
    private ReplicationMessageReceive receiver;
    private ReplicationLogger repLogger;

    private SlaveAddress slaveAddr;
    private String dbname; // The name of the replicated database

    /** The instant of the latest log record received from the master 
     * and processed so far. Used to check that master and slave log files 
     * are in synch */
    private volatile long highestLogInstant = -1;

    /**
     * Whether or not replication slave mode is still on. Will be set
     * to false when slave replication is shut down. The value of this
     * variable is checked after every timeout when trying to set up a
     * connection to the master, and by the thread that applies log
     * chunks received from the master. */
    private volatile boolean inReplicationSlaveMode = true;

    /** Whether or not this SlaveController has been successfully
     * started, including setting up a connection with the master and
     * starting the log receiver thread. The client connection that
     * initiated slave replication mode on this database will not
     * report that slave mode was successfully started (i.e., it will
     * hang) until startupSuccessful has been set to true */
    private volatile boolean startupSuccessful = false;

    // Used to parse chunks of log records received from the master.
    private ReplicationLogScan logScan;

    // Thread that listens for log chunk messages from the master, and
    // applies these to the local log
    private SlaveLogReceiverThread logReceiverThread;

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
     * @param create Currently ignored
     * @param properties Properties used to start the service in the
     * correct mode
     * @exception StandardException Standard Derby exception policy,
     * thrown on error.
     */
    public void boot(boolean create, Properties properties)
        throws StandardException {

        String port = properties.getProperty(Attribute.REPLICATION_SLAVE_PORT);
        
//IC see: https://issues.apache.org/jira/browse/DERBY-3489
        try {
            //if slavePort is -1 the default port
            //value will be used.
            int slavePort = -1;
            if (port != null) {
//IC see: https://issues.apache.org/jira/browse/DERBY-6856
                slavePort = Integer.parseInt(port);
            }
            slaveAddr = new SlaveAddress(
                    properties.getProperty(Attribute.REPLICATION_SLAVE_HOST), 
                    slavePort);
        } catch (UnknownHostException uhe) {
            throw StandardException.newException
                    (SQLState.REPLICATION_CONNECTION_EXCEPTION, uhe, 
                     dbname, getHostName(), String.valueOf(getPortNumber()));
        }

        dbname = properties.getProperty(SlaveFactory.SLAVE_DB);
//IC see: https://issues.apache.org/jira/browse/DERBY-3388
        repLogger = new ReplicationLogger(dbname);
    }

    /**
     * Will tear down the replication slave service. 
     */
    public void stop() { 
        if (inReplicationSlaveMode) {
            // For some reason, stopSlave or failover have not been
            // called yet. Force slave to stop.
            try {
                stopSlave(true);
            } catch (StandardException se) {
                // do nothing
            }
        }
    }

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
     * thread that applies operations received from the master (in the
     * form of log records) to the local slave database.
     *
     * @param rawStore The RawStoreFactory for the database
     * @param logFac The LogFactory ensuring recoverability for this
     * database
     *
     * @exception StandardException Thrown if the slave could not be
     * started.
     */
    public void startSlave(RawStoreFactory rawStore, LogFactory logFac)
        throws StandardException {

        rawStoreFactory = rawStore;

        try {
            logToFile = (LogToFile)logFac;
        } catch (ClassCastException cce) {
            // Since there are only two implementing classes of
            // LogFactory, the class type has to be ReadOnly if it is
            // not LogToFile.
            throw StandardException.newException(
                SQLState.LOGMODULE_DOES_NOT_SUPPORT_REPLICATION);
        }

        logToFile.initializeReplicationSlaveRole();

        // Retry to setup a connection with the master until a
        // connection has been established or until we are no longer
        // in replication slave mode
//IC see: https://issues.apache.org/jira/browse/DERBY-3489
        receiver = new ReplicationMessageReceive(slaveAddr, dbname);
        while (!setupConnection()) {
            if (!inReplicationSlaveMode) {
                // If we get here, another thread has called
                // stopSlave() while we waited for a connection with
                // the master. The thread shutting the slave down will
                // clean up anything we did during setupConnection, so
                // simply return.
                return;
            }
        }

        // Setup the log scan used to parse chunks of log received
        // from the master
        logScan = new ReplicationLogScan();

        startLogReceiverThread();
        startupSuccessful = true;
//IC see: https://issues.apache.org/jira/browse/DERBY-3361
//IC see: https://issues.apache.org/jira/browse/DERBY-3356

        Monitor.logTextMessage(MessageId.REPLICATION_SLAVE_STARTED, dbname);
    }

    /**
     * Will perform all work that is needed to stop replication
     */
    private void stopSlave() throws StandardException {
//IC see: https://issues.apache.org/jira/browse/DERBY-3254
        inReplicationSlaveMode = false;
        teardownNetwork();

        logToFile.stopReplicationSlaveRole();

        Monitor.logTextMessage(MessageId.REPLICATION_SLAVE_STOPPED, dbname);
    }

    /**
     * @see SlaveFactory#stopSlave
     */
    public void stopSlave(boolean forcedStop) 
            throws StandardException {
//IC see: https://issues.apache.org/jira/browse/DERBY-3527
        if (!forcedStop && isConnectedToMaster()){
            throw StandardException.newException(
                    SQLState.SLAVE_OPERATION_DENIED_WHILE_CONNECTED);
        }
        stopSlave();
    }

    public void failover() throws StandardException {
//IC see: https://issues.apache.org/jira/browse/DERBY-3527
        if (isConnectedToMaster()){
            throw StandardException.newException(
                SQLState.SLAVE_OPERATION_DENIED_WHILE_CONNECTED);
        }
        doFailover();
        teardownNetwork();
    } 

    /**
     * Performs failover on this database. May be called because a
     * failover command has been received from the master, or because
     * a client has requested a failover after the network connection
     * with the master has been lost.
     * @see SlaveFactory#failover
     */
    private void doFailover() {
        inReplicationSlaveMode = false;
        logToFile.failoverSlave();
        Monitor.logTextMessage
                (MessageId.REPLICATION_FAILOVER_SUCCESSFUL, dbname);
    }

    /**
     * @see SlaveFactory#isStarted
     */
    public boolean isStarted() {
//IC see: https://issues.apache.org/jira/browse/DERBY-3361
//IC see: https://issues.apache.org/jira/browse/DERBY-3356
        return startupSuccessful;
    }

    ////////////////////////////////////////////////////////////
    // Private Methods                                        //
    ////////////////////////////////////////////////////////////

    /**
     * Establish a connection with the replication master. Listens for
     * a connection on the slavehost/port for DEFAULT_SOCKET_TIMEOUT
     * milliseconds. 
     *
     * @return true if a connection has been set up with the master,
     * false if the connection attempt timed out.
     *
     * @exception StandardException if an unexpected exception occured
     * that prevented a connection with the master.
     */
    private boolean setupConnection() throws StandardException {

//IC see: https://issues.apache.org/jira/browse/DERBY-3021
//IC see: https://issues.apache.org/jira/browse/DERBY-3071
        try {
            // highestLogInstant is -1 until the first log chunk has
            // been received from the master. If a log chunk has been
            // received, use the instant of the latest received log
            // record to synchronize log files. If no log has been
            // received yet, use the end position of the log (i.e.,
            // logToFile.getFlushedInstant)
            if (highestLogInstant != -1) {
                // timeout to check if still in replication slave mode
                receiver.initConnection(DEFAULT_SOCKET_TIMEOUT,
                                        highestLogInstant,
                                        dbname);
            } else {
                // timeout to check if still in replication slave mode
                receiver.initConnection(DEFAULT_SOCKET_TIMEOUT,
                                        logToFile.
                                        getFirstUnflushedInstantAsLong(),
                                        dbname);
            }
            return true; // will not reach this if timeout
        } catch (StandardException se) {
            throw se;
//IC see: https://issues.apache.org/jira/browse/DERBY-4910
//IC see: https://issues.apache.org/jira/browse/DERBY-4812
        } catch (SocketTimeoutException ste) {
            // Got a timeout. Return normally and let the caller retry.
            return false;
        } catch (Exception e) {
            throw StandardException.newException
                    (SQLState.REPLICATION_CONNECTION_EXCEPTION, e,
//IC see: https://issues.apache.org/jira/browse/DERBY-3489
                    dbname, getHostName(), String.valueOf(getPortNumber()));
        }
    }

    /**
     * Write the reason for the lost connection to the log (derby.log)
     * and reconnect with the master. Once the network is up and
     * running, a new LogReceiverThread is started. The method returns
     * without doing anything if inReplicationSlaveMode=false, which
     * means that stopSlave() has been called by another thread.
     *
     * @param e The reason the connection to the master was lost
     */

    private void handleDisconnect(Exception e) {
        if (!inReplicationSlaveMode) {
            return;
        }

        repLogger.logError(MessageId.REPLICATION_SLAVE_LOST_CONN, e);
//IC see: https://issues.apache.org/jira/browse/DERBY-3388

        try {
            while (!setupConnection()) {
                if (!inReplicationSlaveMode) {
                    // stopSlave may have been called, turning
                    // replication slave mode off. Simply return if
                    // that is the case. The thread that called
                    // stopSlave will clean up everything.
                    return;
                }
            }

            startLogReceiverThread();
        } catch (StandardException se) {
            handleFatalException(se);
        }
    }

    /**
     * Check if the repliation network connection to the master is working
     * @return true if the network connection is working, false otherwise
     */
    private boolean isConnectedToMaster() {
//IC see: https://issues.apache.org/jira/browse/DERBY-3527
        if (receiver == null) {
            return false;
        } else {
            return receiver.isConnectedToMaster();
        }
    }

    /**
     * Starts the LogReceiverThread that will listen for chunks of log
     * records from the master and apply the log records to the local
     * log file.
     */
    private void startLogReceiverThread() {
        logReceiverThread = new SlaveLogReceiverThread();
//IC see: https://issues.apache.org/jira/browse/DERBY-3447
        logReceiverThread.setDaemon(true);
        logReceiverThread.start();
    }

    /**
     * Handles fatal errors for slave replication functionality. These
     * are errors that requires us to stop replication. Calling this
     * method has the following effects:
     *
     * 1) Debug messages are written to the log file (usually
     *    derby.log) if ReplicationLogger#LOG_REPLICATION_MESSAGES is
     *    true.
     *
     * 2) If the network connection is up, the master is notified of
     *    the problem.
     *
     * 3) All slave replication functionality is stopped, and the
     *    database is then shut down without being booted.
     *
     * The method will return without doing anything if
     * inReplicationSlaveMode=false, meaning that stopSlave has been
     * called.
     *
     * @param e The fatal exception that is the reason for calling
     * this method
     */
    private void handleFatalException(Exception e) {
        // If inReplicationSlaveMode is false, the stopSlave method in
        // this controller has already been called. If so, we ignore
        // this fatal error.
        if (!inReplicationSlaveMode) {
            return;
        }

        repLogger.logError(MessageId.REPLICATION_FATAL_ERROR, e);
//IC see: https://issues.apache.org/jira/browse/DERBY-3388

        // todo: notify master of the problem
        try {
            stopSlave();
        } catch (StandardException se) {
            repLogger.logError(MessageId.REPLICATION_FATAL_ERROR, se);
        }
    }

    private void teardownNetwork() {
        try {
            // Unplug the replication network connection layer
//IC see: https://issues.apache.org/jira/browse/DERBY-3361
//IC see: https://issues.apache.org/jira/browse/DERBY-3356
            if (receiver != null) {
                receiver.tearDown();
                receiver = null;
            }
        } catch (IOException ioe) {
//IC see: https://issues.apache.org/jira/browse/DERBY-3388
            repLogger.logError(null, ioe);
        }
    }
    
    /**
     * Used to return the host name of the slave.
     *
     * @return a String containing the host name of the slave.
     */
    private String getHostName() {
//IC see: https://issues.apache.org/jira/browse/DERBY-3489
        return slaveAddr.getHostAddress().getHostName();
    }
    
    /**
     * Used to return the port number of the slave.
     *
     * @return an Integer that represents the port number of the slave.
     */
    private int getPortNumber() {
        return slaveAddr.getPortNumber();
    }

    ///////////////////////////////////////////////////////////////////////////
    // Inner Class - Thread used to apply chunks of log received from master //
    ///////////////////////////////////////////////////////////////////////////

    /**
     * Thread that listens for incoming messages from the master and
     * applies chunks of log records to the local log files.
     */
    private class SlaveLogReceiverThread extends Thread {
        
        /**
         * Creates a new instance of <tt>SlaveLogReceiverThread</tt>
         * with a debugging-friendly thread name.
         */
//IC see: https://issues.apache.org/jira/browse/DERBY-3437
        SlaveLogReceiverThread() {
            super("derby.slave.logger-" + dbname);
        }
        
        public void run() {
            try {
                ReplicationMessage message;
                while (inReplicationSlaveMode) {
                    message = receiver.readMessage();

                    switch (message.getType()){
                    case ReplicationMessage.TYPE_LOG:
                        byte[] logChunk = (byte[])message.getMessage();
                        handleLogChunk(logChunk);
                        break;
//IC see: https://issues.apache.org/jira/browse/DERBY-3254
                    case ReplicationMessage.TYPE_FAILOVER:
                        doFailover();
                        ReplicationMessage ack = new ReplicationMessage
                            (ReplicationMessage.TYPE_ACK, "failover succeeded");
                        receiver.sendMessage(ack);
                        teardownNetwork();
                        break;
//IC see: https://issues.apache.org/jira/browse/DERBY-3235
                    case ReplicationMessage.TYPE_STOP:
                        stopSlave();
                        break;
                    default:
                        // debug; will be removed
                        System.out.println("Not handling non-log messages yet "
                                           +"- got a type "+message.getType());
                        break;
                    }
                }

            } catch (EOFException eofe) {
                // Network connection with master has been lost.
                handleDisconnect(eofe);
            } catch (StandardException se) {
                handleFatalException(se);
            } catch (Exception e) {
                // Exceptions not caused by disconnect are unexpected,
                // and therefore fatal
                StandardException se = 
                    StandardException.newException
                    (SQLState.REPLICATION_UNEXPECTED_EXCEPTION, e);
                handleFatalException(se);
            }
        }

        /**
         * Parses a chunk of log received from the master, and applies
         * the individual log records to the local log file.
         *
         * @param logChunk A chunk of log records received from the
         * master
         * @exception StandardException If the chunk of log records
         * could not be parsed or the local log file is out of synch
         * with the master log file.
         */
        private void handleLogChunk(byte[] logChunk)
            throws StandardException{
            logScan.init(logChunk);

            while (logScan.next()){
                if (logScan.isLogFileSwitch()) {
                    logToFile.switchLogFile();
                } else {

                    long localInstant = logToFile.
                        appendLogRecord(logScan.getData(), 
                                        0, 
                                        logScan.getDataLength(), 
                                        null, 
                                        0, 
                                        0);

                    // If the log instant of the received log does not
                    // match with the local log instant, the log
                    // records are not written to the same physical
                    // location in the log files. This is fatal since
                    // log records are identified by their physical
                    // location in the log files.
                    if (logScan.getInstant() != localInstant) {
                        throw StandardException.newException
                            (SQLState.REPLICATION_LOG_OUT_OF_SYNCH,
                             dbname,
//IC see: https://issues.apache.org/jira/browse/DERBY-6856
                             LogCounter.getLogFileNumber(logScan.getInstant()),
                             LogCounter.getLogFilePosition(logScan.getInstant()),
                             LogCounter.getLogFileNumber(localInstant),
                             LogCounter.getLogFilePosition(localInstant));
                    }
                    highestLogInstant = localInstant;
                }
            }
        }
    }
    ///////////////////////////////////////////////////////////
    // END Inner Class                                       //
    ///////////////////////////////////////////////////////////

}
