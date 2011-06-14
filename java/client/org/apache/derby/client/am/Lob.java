/*

   Derby - Class org.apache.derby.client.am.Lob

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

*/

package org.apache.derby.client.am;

import java.io.InputStream;
import java.io.IOException;
import java.util.ArrayList;

import java.sql.SQLException;

import org.apache.derby.client.net.NetConfiguration;
import org.apache.derby.client.net.NetConnection;

import org.apache.derby.shared.common.reference.SQLState;

public abstract class Lob implements UnitOfWorkListener {
    // The following flags specify the data type(s) a LOB instance currently contains
    public static final int STRING = 2;
    public static final int ASCII_STREAM = 4;
    public static final int UNICODE_STREAM = 8;
    public static final int CHARACTER_STREAM = 16;
    public static final int BINARY_STREAM = 32;
    public static final int BINARY_STRING = 64;
    public static final int LOCATOR = 128;

    public static final int INVALID_LOCATOR = -1;
    //---------------------navigational members-----------------------------------
    protected Agent agent_;

    //-----------------------------state------------------------------------------
    protected int dataType_ = 0;      // data type(s) the LOB instance currently contains
    protected int locator_ = INVALID_LOCATOR; // locator id for this LOB

    private long sqlLength_;// length of the LOB value, as defined by the server
    private boolean lengthObtained_;
    
    /**
     * This boolean variable indicates whether the Lob object has been
     * invalidated by calling free() on it
     */
    protected boolean isValid_ = true;

    final private boolean willBeLayerBStreamed_;
    
        
    //A running counter that keeps track
    //of whether a update has been done
    //on this LOB value. We do not need
    //to bother about the limit imposed
    //by this counter because we just check
    //whether its latest value matches hence
    //for all practical purposes there is no 
    //limit imposed.
    private long updateCount;

    /**
     * This integer identifies which transaction the Lob is associated with
     */
    private int transactionID_;

    //-----------------------------messageId------------------------------------------
    final static protected ClientMessageId LOB_OBJECT_LENGTH_UNKNOWN_YET =
        new ClientMessageId( SQLState.LOB_OBJECT_LENGTH_UNKNOWN_YET );
    
    
    //---------------------constructors/finalizer---------------------------------
    protected Lob(Agent agent,
                  boolean willBeLayerBStreamed) {
        agent_ = agent;
        lengthObtained_ = false;
        willBeLayerBStreamed_ = willBeLayerBStreamed;
        transactionID_ = agent_.connection_.getTransactionID();
    }

    // ---------------------------jdbc 2------------------------------------------

    /**
     * Return the length of the Lob value represented by this Lob
     * object.  If length is not already known, and Lob is locator
     * based, length will be retrieved from the server.  If not,
     * locator based, Lob will first be materialized.  NOTE: The
     * caller needs to deal with synchronization.
     *
     * @throws SqlException on execution errors while materializing the stream, 
     *         or if Layer B streaming is used and length not yet obtained.
     * @return length of Lob value
     */
    long sqlLength() throws SqlException 
    {
        if (lengthObtained_) return sqlLength_;
        
        if (isLocator()) {
            sqlLength_ = getLocatorLength();
            lengthObtained_ = true;
        } else if (willBeLayerBStreamed()) {
            throw new SqlException(agent_.logWriter_,
                                   LOB_OBJECT_LENGTH_UNKNOWN_YET);
        } else {
            materializeStream();  // Will set sqlLength_
        }

        return sqlLength_;
    }

    /**
     * Update the registered length of the Lob value.  To be called by
     * methods that make changes to the length of the Lob.
     * NOTE: The caller needs to deal with synchronization.
     *
     * @param length the new length of the Lob value
     */
    void setSqlLength(long length)
    {
        sqlLength_ = length;
        lengthObtained_ = true;
    }

    /**
     * Get the length of locator based Lob from the server.  This is a
     * dummy implementation that is supposed to be overridden by
     * subclasses.  A stored procedure call will be made to get the
     * length from the server.
     * 
     * @throws org.apache.derby.client.am.SqlException 
     * @return length of Lob
     */
    long getLocatorLength() throws SqlException
    {
        return -1;
    }


    //-----------------------event callback methods-------------------------------

    public void listenToUnitOfWork() {
        agent_.connection_.CommitAndRollbackListeners_.put(this,null);
    }

    public void completeLocalCommit(java.util.Iterator listenerIterator) {
        listenerIterator.remove();
    }

    public void completeLocalRollback(java.util.Iterator listenerIterator) {
        listenerIterator.remove();
    }

    //----------------------------helper methods----------------------------------

    public Agent getAgent() {
        return agent_;
    }

    void checkForClosedConnection() throws SqlException {
        if (agent_.connection_.isClosedX()) {
            agent_.checkForDeferredExceptions();
            throw new SqlException(agent_.logWriter_, 
                new ClientMessageId(SQLState.LOB_METHOD_ON_CLOSED_CONNECTION));
        } else {
            agent_.checkForDeferredExceptions();
        }
    }

    void completeLocalRollback() {
        ;
    }

    void completeLocalCommit() {
        ;
    }

    
    /**
     * Method to be implemented by subclasses, so that
     * #materializedStream(InputStream, String) can be called with subclass
     * specific parameters and the result assigned to the right stream.
     *
     * @throws SqlException
     */
    protected abstract void materializeStream() throws SqlException;


    /**
     * Materialize the given stream into memory and update the internal
     * length variable.
     *
     * @param is stream to use for input
     * @param typeDesc description of the data type we are inserting,
     *      for instance <code>java.sql.Clob</code>
     * @return a stream whose source is the materialized data
     * @throws SqlException if the stream exceeds 2 GB, or an error happens
     *      while reading from the stream
     */
    protected InputStream materializeStream(InputStream is, String typeDesc)
            throws SqlException {
        final int GROWBY = 32 * 1024; // 32 KB
        ArrayList byteArrays = new ArrayList();
        byte[] curBytes = new byte[GROWBY];
        int totalLength = 0;
        int partLength = 0;
        // Read all data from the stream, storing it in a number of arrays.
        try {
            do {
                partLength = is.read(curBytes, 0, curBytes.length);
                if (partLength == curBytes.length) {
                    byteArrays.add(curBytes);
                    // Make sure we don't exceed 2 GB by checking for overflow.
                    int newLength = totalLength + GROWBY;
                    if (newLength < 0 || newLength == Integer.MAX_VALUE) {
                        curBytes = new byte[Integer.MAX_VALUE - totalLength];
                    } else {
                        curBytes = new byte[GROWBY];
                    }
                }
                if (partLength > 0) {
                    totalLength += partLength;
                }
            } while (partLength == GROWBY);
            // Make sure stream is exhausted.
            if (is.read() != -1) {
                // We have exceeded 2 GB.
                throw new SqlException(
                            null,
                            new ClientMessageId(
                                SQLState.LANG_OUTSIDE_RANGE_FOR_DATATYPE),
                                typeDesc
                        );
            }
            if (partLength > 0) {
                byteArrays.add(curBytes);
            }

            // Cleanup and set state.
            curBytes = null;
            sqlLength_ = totalLength;
            lengthObtained_ = true;
            // Return a stream whose source is a list of byte arrays. 
            // This avoids having to copy all the data into a single big array.
            return new ByteArrayCombinerStream(byteArrays, totalLength);
        } catch (IOException ioe) {
            throw new SqlException(null,
                        new ClientMessageId(
                            SQLState.LANG_STREAMING_COLUMN_I_O_EXCEPTION),
                            ioe,
                            typeDesc
                    );
        }
    }

    public abstract long length() throws SQLException;
    
    protected static boolean isLayerBStreamingPossible( Agent agent ){
        
        final NetConnection netConn = 
            ( NetConnection  ) agent.connection_ ;
        
        final int securityMechanism = 
            netConn.getSecurityMechanism();

        return 
            netConn.serverSupportsLayerBStreaming() &&
            securityMechanism != NetConfiguration.SECMEC_EUSRIDDTA &&
            securityMechanism != NetConfiguration.SECMEC_EUSRPWDDTA;
        
    }
    
    public boolean willBeLayerBStreamed() {
        return willBeLayerBStreamed_;
    }

    /**
     * Check whether this Lob is based on a locator
     * @return true if Lob is based on locator, false otherwise
     */
    public boolean isLocator() {
        return ((dataType_ & LOCATOR) == LOCATOR);
    }

    /**
     * Get locator for this Lob
     * @return locator for this Lob, INVALID_LOCATOR if Lob is not
     *         based on locator
     */
    public int getLocator() {
        return locator_;
    }


    /**
     * Checks the <code>pos</code> and <code>length</code>.
     *
     * @param pos a long that contains the position that needs to be checked
     * @param length a long that contains the length that needs to be checked
     * @throws SQLException if
     *         a) pos <= 0
     *         b) pos > (length of LOB)
     *         c) length < 0
     *         d) (pos -1) + length > (length of LOB)
     */
    protected void checkPosAndLength(long pos, long length)
    throws SQLException {
        if (pos <= 0) {
            throw new SqlException(agent_.logWriter_,
                new ClientMessageId(SQLState.BLOB_BAD_POSITION),
                new Long(pos)).getSQLException();
        }
        if (length < 0) {
            throw new SqlException(agent_.logWriter_,
                new ClientMessageId(SQLState.BLOB_NONPOSITIVE_LENGTH),
                new Integer((int)length)).getSQLException();
        }
        if (length > (this.length() - (pos -1))) {
            throw new SqlException(agent_.logWriter_,
                new ClientMessageId(SQLState.POS_AND_LENGTH_GREATER_THAN_LOB),
                new Long(pos), new Long(length)).getSQLException();
        }
    }
    
        
    /**
     * Increments and returns the new updateCount 
     * of this <code>Lob</code>. The method needs to be 
     * synchronized since multiple updates can 
     * happen on this <code>Lob</code> simultaneously. 
     * It will be called from the
     * 1) Locator Writers
     * 2) Locator OutputStreams
     * 3) From the update methods
     *    within the Lobs like setString, truncate.
     * since all of the above acesses are inside
     * the am package, this method will have
     * default access. We do not need to worry
     * about the non-locator streams since
     * non-locator InputStreams would not
     * depend on updateCount for invalidation
     */
    protected synchronized void incrementUpdateCount() {
        updateCount++;
    }
    
    /**
     * Returns the current updateCount of the Clob.
     */
    long getUpdateCount() {
        return updateCount;
    }
    
    /**
     * Calls SqlLength() to check if the Locator associated
     * with the underlying Lob is valid. If it is not
     * it throws an exception.
     *
     * @throws SqlException
     * 
     */
    void checkForLocatorValidity() throws SqlException {
        // As of now there is no other way of determining that
        //the locator associated with the underlying LOB is not
        //valid
        sqlLength();
    }
    
    /**
     * Checks if isValid is true and whether the transaction that
     * created the Lob is still active. If any of which is not true throws
     * a SQLException stating that a method has been called on
     * an invalid LOB object.
     *
     * @throws SQLException if isValid is not true or the transaction that
     * created the Lob is not active
     */
    protected void checkValidity() throws SQLException{

        // If there isn't an open connection, the Lob is invalid.
        try {
            agent_.connection_.checkForClosedConnection();
        } catch (SqlException se) {
            throw se.getSQLException();
        }

        if(!isValid_ || (isLocator()  && 
        		(transactionID_ != agent_.connection_.getTransactionID())))
            throw new SqlException(null,new ClientMessageId(SQLState.LOB_OBJECT_INVALID))
                                                  .getSQLException();
    }
}
