/*

   Derby - Class org.apache.derby.client.am.Lob

   Copyright (c) 2001, 2005 The Apache Software Foundation or its licensors, where applicable.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

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

    protected long sqlLength_;      // length of the LOB value, as defined by the server
    protected boolean lengthObtained_;

    //---------------------constructors/finalizer---------------------------------
    protected Lob(Agent agent) {
        agent_ = agent;
        lengthObtained_ = false;
    }

    protected void finalize() throws java.lang.Throwable {
        super.finalize();
    }

    // ---------------------------jdbc 2------------------------------------------

    // should only be called by a synchronized method.


    // should only be called by a synchronized method.
    public long sqlLength() throws SqlException {
        checkForClosedConnection();

        return sqlLength_;
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
}
