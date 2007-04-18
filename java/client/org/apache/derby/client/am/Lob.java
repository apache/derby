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

    //---------------------navigational members-----------------------------------
    protected Agent agent_;

    //-----------------------------state------------------------------------------
    protected int dataType_ = 0;      // data type(s) the LOB instance currently contains

    private long sqlLength_;// length of the LOB value, as defined by the server
    private boolean lengthObtained_;
    
    final private boolean willBeLayerBStreamed_;

    //-----------------------------messageId------------------------------------------
    final static protected ClientMessageId LOB_OBJECT_LENGTH_UNKNOWN_YET =
        new ClientMessageId( SQLState.LOB_OBJECT_LENGTH_UNKNOWN_YET );
    
    
    //---------------------constructors/finalizer---------------------------------
    protected Lob(Agent agent,
                  boolean willBeLayerBStreamed) {
        agent_ = agent;
        lengthObtained_ = false;
        willBeLayerBStreamed_ = willBeLayerBStreamed;
    }

    protected void finalize() throws java.lang.Throwable {
        super.finalize();
    }

    // ---------------------------jdbc 2------------------------------------------

    /**
     * Return the length of the Lob value represented by this Lob object.
     * If length is not already known, Lob will first be materialized.
     * NOTE: The caller needs to deal with synchronization.
     *
     * @throws SqlException on execution errors while materializing the stream, 
     *         or if Layer B streaming is used and length not already obtained.
     * @return length of Lob value
     */
    long sqlLength() throws SqlException 
    {
        if (lengthObtained_) return sqlLength_;
        
        if (willBeLayerBStreamed()) {
            throw new SqlException(agent_.logWriter_,
                                   LOB_OBJECT_LENGTH_UNKNOWN_YET);
        }

        materializeStream();  // Will set sqlLength_
        return sqlLength_;
    }

    /**
     * Update the registered length of the Lob value.  To be called by
     * methods that make changes to the length of the Lob.
     * NOTE: The caller needs to deal with synchronization.
     *
     * @param the new length of the Lob value
     */
    void setSqlLength(long length)
    {
        sqlLength_ = length;
        lengthObtained_ = true;
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
                        typeDesc,
                        ioe
                    );
        }
    }
    
    public static boolean isLengthObtained(Lob l){
        return l.lengthObtained_;
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
     * Checks the <code>pos</code> and <code>length</code>.
     *
     * @param pos a long that contains the position that needs to be checked
     * @param length a long that contains the length that needs to be checked
     * @throws SQLException if
     *         a) pos <= 0
     *         b) pos > (length of LOB)
     *         c) length < 0
     *         d) pos + length > (length of LOB)
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
        if (length > (this.length() - pos)) {
            throw new SqlException(agent_.logWriter_,
                new ClientMessageId(SQLState.POS_AND_LENGTH_GREATER_THAN_LOB),
                new Long(pos), new Long(length)).getSQLException();
        }
    }
}
