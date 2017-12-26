/*
   Derby - Class org.apache.derby.impl.drda.EXTDTAReaderInputStream

   Licensed to the Apache Software Foundation (ASF) under one
   or more contributor license agreements.  See the NOTICE file
   distributed with this work for additional information
   regarding copyright ownership.  The ASF licenses this file
   to you under the Apache License, Version 2.0 (the
   "License"); you may not use this file except in compliance
   with the License.  You may obtain a copy of the License at
   
   http://www.apache.org/licenses/LICENSE-2.0
   
   Unless required by applicable law or agreed to in writing,
   software distributed under the License is distributed on an
   "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
   KIND, either express or implied.  See the License for the
   specific language governing permissions and limitations
   under the License.
*/
package org.apache.derby.impl.drda;

import java.io.IOException;
import java.io.InputStream;
import org.apache.derby.iapi.reference.DRDAConstants;
import org.apache.derby.shared.common.reference.SQLState;
import org.apache.derby.iapi.services.i18n.MessageService;
import org.apache.derby.iapi.services.io.DerbyIOException;
import org.apache.derby.shared.common.reference.MessageId;

/**
 * Implementation of InputStream which get EXTDTA from the DDMReader.
 * <p>
 * This class can be used to stream LOBs from Network client to the
 * Network server.
 * <p>
 * To be able to correctly stream data from the client without reading the
 * while value up front, a trailing Derby-specific status byte was introduced
 * (version 10.6). It is used by the client to tell the server if the data it
 * received was valid, or if it detected an error while streaming the data.
 * The DRDA protocol, or at least Derby's implementation of it, doesn't enable
 * the client to inform the server about the error whilst streaming (there is a
 * mechanism in DRDA to interrupt a running request, but it didn't seem like a
 * feasible approach in this case).
 */
abstract class EXTDTAReaderInputStream
        extends InputStream {

    /** Whether or not the subclass is a layer B stream. */
    protected final boolean isLayerBStream;
    /** Whether or not to read the trailing Derby-specific status byte. */
    protected final boolean readStatusByte;
    /**
     * Tells if the status byte has been set.
     *
     * @see #checkStatus(int)
     */
    private boolean statusSet;
    /**
     * The status Derby-specific status byte, if any.
     * @see #isStatusSet()
     */
    private byte status;
    /**
     * Whether or not to suppress the exception when an error is indicated by
     * the status byte.
     */
    private boolean suppressException;

    /**
     * Initializes the class.
     *
     * @param layerB whether or not DDM layer B streaming is being used
     * @param readStatusByte whether or not to read the trailing Derby-specific
     *      status byte
     */
    protected EXTDTAReaderInputStream(boolean layerB, boolean readStatusByte) {
        this.isLayerBStream = layerB;
        this.readStatusByte = readStatusByte;
    }

    /**
     * Saves the status byte read off the wire.
     *
     * @param status the status
     * @see DRDAConstants
     */
    // Private for now, as the method is currently used only by checkStatus.
    private void setStatus(int status) {
        this.status = (byte)(status & 0xFF);
        this.statusSet = true;
    }

    /**
     * Returns whether the status has been set or not.
     *
     * @return {@code true} if set, {@code false} if not.
     */
    public boolean isStatusSet() {
        return statusSet;
    }

    /**
     * Returns the status byte.
     * <p>
     * <em>NOTE:</em> Check if the status byte has been set by calling
     * {@linkplain #isStatusSet()}.
     *
     * @return The status byte.
     */
    public byte getStatus() {
        if (!statusSet) {
            throw new IllegalStateException("status hasn't been set");
        }
        return status;
    }

    /**
     * Sets whether or not to suppress the exception when setting the status.
     *
     * @param flag {@code true} to suppress, {@code false} to throw exception
     *      if an error condition is indicated by the status flag
     */
    void setSuppressException(boolean flag) {
        this.suppressException = flag;
    }

    public boolean isLayerBStream() {
        return isLayerBStream;
    }

    /**
     * Interprets the Derby-specific status byte, and throws an exception if an
     * error condition has been detected on the client.
     *
     * @param clientStatus the status flag sent by the client
     * @throws IOException if the status byte indicates an error condition
     */
    protected void checkStatus(int clientStatus)
            throws IOException {
        // Note that in some cases we don't want to throw an exception here
        // even if the status byte tells us an exception happened on the client
        // side when reading the data stream. This is because sometimes EXTDTAs
        // are // fully read before they are passed to the statement. If we
        // throw the exception here, we cause DRDA protocol errors (it would
        // probably be possible to code around this, but it is far easier to
        // just have the embedded statement execution fail).

        setStatus(clientStatus);
        if (!suppressException && status != DRDAConstants.STREAM_OK) {
            // Ask the sub-class to clean up.
            onClientSideStreamingError();
            throwEXTDTATransferException(clientStatus);
        }
    }

    /**
     * Performs necessary clean up when an error is signalled by the client.
     */
    protected abstract void onClientSideStreamingError();

    /**
     * Throws an exception as mandated by the EXTDTA status byte.
     *
     * @param status the EXTDTA status byte received from the client, should
     *      not be {@linkplain DRDAConstants#STREAM_OK}
     * @throws IOException the exception generated based on the status byte
     */
    static void throwEXTDTATransferException(int status)
            throws IOException {
        switch (status) {
            case DRDAConstants.STREAM_READ_ERROR:
                throw new IOException(
                        MessageService.getTextMessage(
                            MessageId.STREAM_DRDA_CLIENTSIDE_EXTDTA_READ_ERROR)
                         );
            case DRDAConstants.STREAM_TOO_SHORT:
            case DRDAConstants.STREAM_TOO_LONG:
                throw new DerbyIOException(
                        MessageService.getTextMessage(
                            SQLState.SET_STREAM_INEXACT_LENGTH_DATA),
                        SQLState.SET_STREAM_INEXACT_LENGTH_DATA);
            case DRDAConstants.STREAM_OK:
                // Safe-guard, this method should not be invoked when the
                // transfer was successful.
                throw new IllegalStateException(
                        "throwEXTDTATransferException invoked with EXTDTA " +
                        "status byte STREAM_OK");
            default:
                throw new IOException(
                        "Invalid stream EXTDTA status code: " + status);
        }
    }
}
