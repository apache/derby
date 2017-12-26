/*
 
   Derby - Class org.apache.derby.impl.store.replication.master.AsynchronousLogShipper
 
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
import java.util.NoSuchElementException;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.Property;
import org.apache.derby.shared.common.reference.SQLState;
import org.apache.derby.iapi.services.property.PropertyUtil;
import org.apache.derby.shared.common.sanity.SanityManager;

import org.apache.derby.impl.store.replication.ReplicationLogger;
import org.apache.derby.impl.store.replication.buffer.ReplicationLogBuffer;
import org.apache.derby.impl.store.replication.net.ReplicationMessage;
import org.apache.derby.impl.store.replication.net.ReplicationMessageTransmit;
import org.apache.derby.iapi.util.InterruptStatus;

/**
 * <p>
 * Does asynchronous shipping of log records from the master to the slave being
 * replicated to. The implementation does not ship log records as soon as they
 * become available in the log buffer (synchronously), instead it does log
 * shipping in the following two-fold scenarios
 *
 * 1) Periodically (i.e.) at regular intervals of time.
 *
 * 2) when a request is sent from the master controller (force flushing of
 *    the log buffer).
 *
 * 3) when a notification is received from the log shipper about a log
 *    buffer element becoming full and the load on the log buffer so
 *    warrants a ship.
 * </p>
 */
public class AsynchronousLogShipper extends Thread implements
    LogShipper {
    
    /**
     * Replication log buffer that contains the log records that need to
     * be transmitted to the slave.
     */
    final private ReplicationLogBuffer logBuffer;
    
    /**
     * Replication message transmitter that is used for the network
     * transmission of the log records retrieved from the log buffer
     * (on the master) to the slave being replicated to.
     */
    private ReplicationMessageTransmit transmitter;
    
    /**
     * Time interval (in milliseconds) at which the log shipping takes place.
     */
    private long shippingInterval;
    /**
     * Minimum interval (in milliseconds) between log shipping.
     * Defaults to MIN, but can be configured using system property
     * derby.replication.minLogShippingInterval
     * @see #MIN
     */
    private long minShippingInterval;
    /**
     * Minimum interval (in milliseconds) between log shipping.
     * Defaults to MAX, but can be configured using system property
     * derby.replication.maxLogShippingInterval
     * @see #MAX
     */
    private long maxShippingInterval;
    
    /**
     * Will store the time at which the last shipping happened. Will be used
     * to calculate the interval between the log ships upon receiving a
     * notification from the log buffer.
     */
    private long lastShippingTime;
    
    /**
     * Indicates whether a stop shipping request has been sent.
     * true - stop shipping log records
     * false - shipping can continue without interruption.
     */
    private volatile boolean stopShipping = false;
    
    /**
     * The master controller that initialized this log shipper.
     */
    private MasterController masterController = null;
    
    /**
     * Object used to synchronize on while the log shipper thread
     * is moved into the wait state, or while notifying it.
     */
    private Object objLSTSync = new Object(); // LST->Log Shippper Thread

    /** Used to synchronize forceFlush calls */
    private Object forceFlushSemaphore = new Object();

    /** The number of millis a call to forceFlush will wait before giving
     * up sending a chunk of log to the slave */
    public static final int DEFAULT_FORCEFLUSH_TIMEOUT = 5000;
    
    /**
     * Store the log chunk that failed during a previous shipping attempt
     * so that it can be re-shipped to the slave.
     */
    private ReplicationMessage failedChunk = null;
    /** The highest log instant in failedChunk  */
    private long failedChunkHighestInstant = -1;
    /** The highest log instant shipped so far  */
    private long highestShippedInstant = -1;
    
    /**
     * Fill information value indicative of a low load in the log buffer.
     */
    private static final int FI_LOW = 10;
    
    /**
     * Fill information value indicative of a high load in the log buffer.
     */
    private static final int FI_HIGH = 80;
    
    
    /**
     * If the fill information (obtained from the log buffer) is less than 
     * FI_HIGH but greater than FI_LOW the log shipper will ship with a MIN ms delay.
     * MIN is a value that is only as large as not to affect the performance
     * of the master database significantly.
     */
    private static final long MIN = 100;
    
    /**
     * If the fill information is less than FI_LOW the log shipper will ship 
     * with a MAX ms delay or when a buffer becomes full whichever comes 
     * first. The delay however will not be smaller than MIN. 
     * max(MAX, DEFAULT_NUMBER_LOG_BUFFERS*MIN) is the maximum delay between a 
     * log record is committed at the master until it is replicated  to the 
     * slave. Hence the default latency should be atleast greater than the maximum
     * latency offered by the choice of MIN, hence MAX &gt; DEFAULT_NUMBER_LOG_BUFFERS*MIN.
     */
    private static final long MAX = 5000;

    private final ReplicationLogger repLogger;

    /**
     * Constructor initializes the log buffer, the replication message
     * transmitter, the shipping interval and the master controller.
     *
     * @param logBuffer the replication log buffer that contains the log record
     *                  chunks to be transmitted to the slave.
     * @param transmitter the replication message transmitter that is used for
     *                    network transmission of retrieved log records.
     * @param masterController The master controller that initialized this log
     *                         shipper.
     * @param repLogger The replication logger that will write messages to
     * the log file (typically derby.log)
     */
    public AsynchronousLogShipper(ReplicationLogBuffer logBuffer,
                                  ReplicationMessageTransmit transmitter,
                                  MasterController masterController,
                                  ReplicationLogger repLogger) {
        super("derby.master.logger-" + masterController.getDbName());
        this.logBuffer = logBuffer;
        this.transmitter = transmitter;
        this.masterController = masterController;
        this.stopShipping = false;
        this.repLogger = repLogger;

        getLogShipperProperties();
        shippingInterval = minShippingInterval;

        lastShippingTime = System.currentTimeMillis();
    }
    
    /**
     * Ships log records from the log buffer to the slave being replicated to.
     * The log shipping happens between shipping intervals of time, the 
     * shipping interval being derived from the fill information (an indicator
     * of load in the log buffer) obtained from the log buffer. The shipping
     * can also be triggered in the following situations,
     * 
     * 1) Based on notifications from the log buffer, where the fill 
     *    information is again used as the basis to decide whether a
     *    shipping should happen or not
     * 2) On a forceFlush triggered by the log buffer becoming full
     *    and the LogBufferFullException being thrown. 
     */
    public void run() {
        while (!stopShipping) {
            try {
                synchronized (forceFlushSemaphore) {
                    shipALogChunk();
                    // Wake up a thread waiting for forceFlush, if any
                    forceFlushSemaphore.notify();
                }
                //calculate the shipping interval (wait time) based on the
                //fill information obtained from the log buffer.
                shippingInterval = calculateSIfromFI();
                if (shippingInterval != -1) {
                    synchronized(objLSTSync) {
                        objLSTSync.wait(shippingInterval);
                    }
                }
            } catch (InterruptedException ie) {
                InterruptStatus.setInterrupted();
            } catch (IOException ioe) {
                //The transmitter is recreated if the connection to the
                //slave can be re-established.
                transmitter = masterController.handleExceptions(ioe);
                //The transmitter cannot be recreated hence stop the log
                //shipper thread.
                if (transmitter != null) {
                    continue;
                }
            } catch (StandardException se) {
                masterController.handleExceptions(se);
            }
        }
    }
    
    /**
     * Retrieves a chunk of log records, if available, from the log buffer and
     * transmits them to the slave. Used for both periodic and forced shipping.
     *
     * @throws IOException If an exception occurs while trying to ship the
     *                     replication message (containing the log records)
     *                     across the network.
     * @throws StandardException If an exception occurs while trying to read
     *                           log records from the log buffer.
     * 
     * @return true if a chunk of log records was shipped.
     *         false if no log records were shipped because log buffer is empty.
     */
    private synchronized boolean shipALogChunk()
    throws IOException, StandardException {
        byte [] logRecords = null;
        ReplicationMessage mesg = null;
        try {
            //Check to see if a previous log record exists that needs
            //to be re-transmitted. If there is then transmit that
            //log record and then transmit the next log record in the
            //log buffer.
            if (failedChunk != null) {
                transmitter.sendMessage(failedChunk);
                highestShippedInstant = failedChunkHighestInstant;
                failedChunk = null;
            }
            //transmit the log record that is at the head of
            //the log buffer.
            if (logBuffer.next()) {
                logRecords = logBuffer.getData();
                
                mesg = new ReplicationMessage(
                    ReplicationMessage.TYPE_LOG, logRecords);
                
                transmitter.sendMessage(mesg);
                highestShippedInstant = logBuffer.getLastInstant();
                lastShippingTime = System.currentTimeMillis();
                return true;
            } 
        } catch (NoSuchElementException nse) {
            //Although next() returns true a request for data on the buffer
            //fails implying that there has been a fatal exception in the
            //buffer.
            masterController.handleExceptions(StandardException.newException
                (SQLState.REPLICATION_UNEXPECTED_EXCEPTION, nse));
        } catch (IOException ioe) {
            //An exception occurred while transmitting the log record.
            //Store the previous log record so that it can be re-transmitted
            if (mesg != null) {
                failedChunk = mesg;
                failedChunkHighestInstant = logBuffer.getLastInstant();
            }
            throw ioe;
        }
        return false;
    }
    
    /**
     *
     * Transmits all the log records in the log buffer to the slave.
     *
     * @throws IOException If an exception occurs while trying to ship the
     *                     replication message (containing the log records)
     *                     across the network.
     * @throws StandardException If an exception occurs while trying to read
     *                           log records from the log buffer.
     */
    public void flushBuffer() throws IOException, StandardException {
        while (shipALogChunk());
    }
    
    /**
     * Transmits a chunk of log record from the log buffer to the slave, used
     * by the master controller when the log buffer is full and some space
     * needs to be freed for further log records.
     *
     * @throws IOException If an exception occurs while trying to ship the
     *                     replication message (containing the log records)
     *                     across the network.
     * @throws StandardException If an exception occurs while trying to read
     *                           log records from the log buffer.
     */
    public void forceFlush() throws IOException, StandardException 
    {
        if (stopShipping) return;
        synchronized (forceFlushSemaphore) {
            synchronized (objLSTSync) {
                // Notify the log shipping thread that
                // it is time for another send.
                objLSTSync.notify();
            }

            try {
                forceFlushSemaphore.wait(DEFAULT_FORCEFLUSH_TIMEOUT);
            } catch (InterruptedException ex) {
                InterruptStatus.setInterrupted();
            }
        }
    }
    
    /**
     * Get the highest log instant shipped so far
     * @return the highest log instant shipped so far
     */
    public long getHighestShippedInstant() {
        return highestShippedInstant;
    }

    /**
     * updates the information about the latest instance of the log record
     * that has been flushed to the disk. Calling this method has no effect
     * in this asynchronous implementation of the log shipper.
     *
     *
     * @param latestInstanceFlushedToDisk a long that contains the latest
     *        instance of the log record that has been flushed to the disk.
     */
    public void flushedInstance(long latestInstanceFlushedToDisk) {
        //Currently the Asynchronous log shipper
        //does not worry about the last instance flushed.
    }
    
    /**
     * Stop shipping log records. If a ship is currently in progress
     * it will not be interrupted, shipping will stop only after the
     * current shipment is done.
     */
    public void stopLogShipment() {
        stopShipping = true;
    }
    
    /**
     * Used to notify the log shipper that a log buffer element is full.
     * This method would basically use the following steps to decide on the 
     * action to be taken when a notification from the log shipper is received,
     * 
     * a) Get FI from log buffer
     * b) If FI &gt;= FI_HIGH
     *     b.1) notify the log shipper thread.
     * c) Else If the time elapsed since last ship is greater than
     *    minShippingInterval
     *     c.1) notify the log shipper thread.
     */
    public void workToDo() {
        //Fill information obtained from the log buffer
        int fi;
        
        fi = logBuffer.getFillInformation();
        
        if (fi >= FI_HIGH || 
                (System.currentTimeMillis() - lastShippingTime) >
                 minShippingInterval) {
            synchronized (objLSTSync) {
                objLSTSync.notify();
            }
        }
    }
    
    /**
     * Will be used to calculate the shipping interval based on the fill
     * information obtained from the log buffer. This method uses the following
     * steps to arrive at the shipping interval,
     * 
     * a) FI &gt;= FI_HIGH return -1 (signifies that the waiting time should be 0)
     * b) FI &gt;  FI_LOW and FI &lt; FI_HIGH return minShippingInterval
     * c) FI &lt;= FI_LOW return maxShippingInterval.
     * 
     * @return the shipping interval based on the fill information.
     */
    private long calculateSIfromFI() {
        //Fill information obtained from the log buffer.
        int fi;
        
        //shipping interval derived from the fill information.
        long si;
        
        fi = logBuffer.getFillInformation();
        
        if (fi >= FI_HIGH) {
            si = -1;
        } else if (fi > FI_LOW && fi < FI_HIGH) {
            si = minShippingInterval;
        } else {
            si = maxShippingInterval;
        }
        
        return si;
    }

    /**
     * Load relevant system properties: max and min log shipping interval
     */
    private void getLogShipperProperties() {
        minShippingInterval = PropertyUtil.
            getSystemInt(Property.REPLICATION_MIN_SHIPPING_INTERVAL, (int)MIN);
        maxShippingInterval = PropertyUtil.
            getSystemInt(Property.REPLICATION_MAX_SHIPPING_INTERVAL, (int)MAX);

        // To guarantee a maximum log shipping delay,
        // minShippingInterval cannot be higher than
        // maxShippingInterval / #logbuffers. See javadoc for MAX
        int buffers = ReplicationLogBuffer.DEFAULT_NUMBER_LOG_BUFFERS;
        if (minShippingInterval > maxShippingInterval / buffers) {
            minShippingInterval = maxShippingInterval / buffers;
            if (SanityManager.DEBUG) {
                repLogger.logText("Minimum log shipping " +
                                  "interval too large to guarantee " +
                                  "the current maximum interval (" +
                                  maxShippingInterval +
                                  "). New minimum interval: " +
                                  minShippingInterval,
                                  false);
            }
        }
    }

}
