/*

   Derby - Class org.apache.derby.client.net.Request

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
package org.apache.derby.client.net;

import org.apache.derby.client.am.DateTime;
import org.apache.derby.client.am.DateTimeValue;
import org.apache.derby.client.am.Decimal;
import org.apache.derby.client.am.DisconnectException;
import org.apache.derby.client.am.ClientMessageId;
import org.apache.derby.client.am.SqlException;
import org.apache.derby.shared.common.reference.SQLState;
import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derby.iapi.reference.DRDAConstants;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import org.apache.derby.shared.common.error.ExceptionUtil;


public class Request {

    // byte array buffer used for constructing requests.
    // currently requests are built starting at the beginning of the buffer.
    protected ByteBuffer buffer;

    // a stack is used to keep track of offsets into the buffer where 2 byte
    // ddm length values are located.  these length bytes will be automatically updated
    // by this object when construction of a particular object has completed.
    // right now the max size of the stack is 10. this is an arbitrary number which
    // should be sufficiently large enough to handle all situations.
    private final static int MAX_MARKS_NESTING = 10;
    private int[] markStack_ = new int[MAX_MARKS_NESTING];
    private int top_ = 0;

    //  This Object tracks the location of the current
    //  Dss header length bytes.  This is done so
    //  the length bytes can be automatically
    //  updated as information is added to this stream.
    private int dssLengthLocation_ = 0;

    // tracks the request correlation ID to use for commands and command objects.
    // this is automatically updated as commands are built and sent to the server.
    private int correlationID_ = 0;

    private boolean simpleDssFinalize = false;

    // Used to mask out password when trace is on.
    protected boolean passwordIncluded_ = false;
    protected int passwordStart_ = 0;
    protected int passwordLength_ = 0;

    protected NetAgent netAgent_;


    // construct a request object specifying the minimum buffer size
    // to be used to buffer up the built requests.  also specify the ccsid manager
    // instance to be used when building ddm character data.
    Request(NetAgent netAgent, int minSize) {
        netAgent_ = netAgent;
        buffer = ByteBuffer.allocate(minSize);
        clearBuffer();
    }

    protected final void clearBuffer() {
        buffer.clear();
        top_ = 0;
        for (int i = 0; i < markStack_.length; i++) {
            if (markStack_[i] != 0) {
                markStack_[i] = 0;
            } else {
                break;
            }
        }
        dssLengthLocation_ = 0;
    }

    final void initialize() {
        clearBuffer();
        correlationID_ = 0;
    }

    // ensure length at the end of the buffer for a certain amount of data.
    // if the buffer does not contain sufficient room for the data, the buffer
    // will be expanded by the larger of (2 * current size) or (current size + length).
    // the data from the previous buffer is copied into the larger buffer.
    protected final void ensureLength(int length) {
        if (length > buffer.remaining()) {
            int newLength =
                Math.max(buffer.capacity() * 2, buffer.position() + length);
            // copy the old buffer into a new one
            buffer.flip();
            buffer = ByteBuffer.allocate(newLength).put(buffer);
        }
    }

    // creates an request dss in the buffer to contain a ddm command
    // object.  calling this method means any previous dss objects in
    // the buffer are complete and their length and chaining bytes can
    // be updated appropriately.
    protected final void createCommand() {
        buildDss(false, false, false, DssConstants.GDSFMT_RQSDSS, ++correlationID_, false);
    }

    // creates an request dss in the buffer to contain a ddm command
    // object.  calling this method means any previous dss objects in
    // the buffer are complete and their length and chaining bytes can
    // be updated appropriately.
    protected void createXACommand() {
        buildDss(false, false, false, DssConstants.GDSFMT_RQSDSS_NOREPLY, ++correlationID_, false);
    }

    // creates an object dss in the buffer to contain a ddm command
    // data object.  calling this method means any previous dss objects in
    // the buffer are complete and their length and chaining bytes can
    // be updated appropriately.
    final void createCommandData() {
        buildDss(true,
                false,
                false,
                DssConstants.GDSFMT_OBJDSS,
                correlationID_,
                false);
    }

    final void createEncryptedCommandData() {
        if (netAgent_.netConnection_.getSecurityMechanism() == NetConfiguration.SECMEC_EUSRIDDTA ||
                netAgent_.netConnection_.getSecurityMechanism() == NetConfiguration.SECMEC_EUSRPWDDTA) {
            buildDss(true, false, false, DssConstants.GDSFMT_ENCOBJDSS, correlationID_, false);
        } else {
            buildDss(true,
                    false,
                    false,
                    DssConstants.GDSFMT_OBJDSS,
                    correlationID_,
                    false);
        }
    }


    // experimental lob section

    private final void buildDss(boolean dssHasSameCorrelator,
                                boolean chainedToNextStructure,
                                boolean nextHasSameCorrelator,
                                int dssType,
                                int corrId,
                                boolean simpleFinalizeBuildingNextDss) {
        if (doesRequestContainData()) {
            if (simpleDssFinalize) {
                finalizeDssLength();
            } else {
                finalizePreviousChainedDss(dssHasSameCorrelator);
            }
        }

        // RQSDSS header is 6 bytes long: (ll)(Cf)(rc)
        ensureLength(6);

        // Save the position of the length bytes, so they can be updated with a
        // different value at a later time.
        dssLengthLocation_ = buffer.position();
        // Dummy values for the DSS length (token ll above).
        // The correct length will be inserted when the DSS is finalized.
        buffer.putShort((short) 0xFFFF);

        // Insert the mandatory 0xD0 (token C).
        buffer.put((byte) 0xD0);

        // Insert the dssType (token f), which also tells if the DSS is chained
        // or not. See DSSFMT in the DRDA specification for details.
        if (chainedToNextStructure) {
            dssType |= DssConstants.GDSCHAIN;
            if (nextHasSameCorrelator) {
                dssType |= DssConstants.GDSCHAIN_SAME_ID;
            }
        }
        buffer.put((byte) dssType);

        // Write the request correlation id (two bytes, token rc).
        // use method that writes a short
        buffer.putShort((short) corrId);

        simpleDssFinalize = simpleFinalizeBuildingNextDss;
    }
    
    
    final void writeScalarStream(boolean chained,
                                 boolean chainedWithSameCorrelator,
                                 int codePoint,
                                 java.io.InputStream in,
                                 boolean writeNullByte,
                                 int parameterIndex) throws DisconnectException, SqlException {
        
        writePlainScalarStream(chained,
                               chainedWithSameCorrelator,
                               codePoint,
                               in,
                               writeNullByte,
                               parameterIndex);
        
    }
    
    
    final void writeScalarStream(boolean chained,
                                 boolean chainedWithSameCorrelator,
                                 int codePoint,
                                 long length,
                                 java.io.InputStream in,
                                 boolean writeNullByte,
                                 int parameterIndex) throws DisconnectException, SqlException {

        if (netAgent_.netConnection_.getSecurityMechanism() == NetConfiguration.SECMEC_EUSRIDDTA ||
            netAgent_.netConnection_.getSecurityMechanism() == NetConfiguration.SECMEC_EUSRPWDDTA) {
            // DERBY-4706
            // The network server doesn't support the security mechanisms above.
            // Further, the code in writeEncryptedScalarStream is/was in a bad
            // state.
            // Throw an exception for now until we're positive the code can be
            // ditched, later this comment/code itself can also be removed.
            throw new SqlException(netAgent_.logWriter_,
                    new ClientMessageId(SQLState.NOT_IMPLEMENTED),
                    "encrypted scalar streams");

        }else{
            
            writePlainScalarStream(chained,
                                   chainedWithSameCorrelator,
                                   codePoint,
                                   length,
                                   in,
                                   writeNullByte,
                                   parameterIndex);
            
        }

    }
    
    /**
     * Writes a stream with a known length onto the wire.
     * <p>
     * To avoid DRDA protocol exceptions, the data is truncated or padded as
     * required to complete the transfer. This can be avoided by implementing
     * the request abort mechanism specified by DRDA, but it is rather complex
     * and may not be worth the trouble.
     * <p>
     * Also note that any exceptions generated while writing the stream will
     * be accumulated and raised at a later time.
     *
     * @param length the byte length of the stream
     * @param in the stream to transfer
     * @param writeNullByte whether or not to write a NULL indicator
     * @param parameterIndex one-based parameter index
     * @throws DisconnectException if a severe error condition is encountered,
     *      causing the connection to be broken
     */
    final private void writePlainScalarStream(boolean chained,
                                              boolean chainedWithSameCorrelator,
                                              int codePoint,
                                              long length,
                                              java.io.InputStream in,
                                              boolean writeNullByte,
                                              int parameterIndex) throws DisconnectException, SqlException {
        // We don't have the metadata available when we create this request
        // object, so we have to check here if we are going to write the status
        // byte or not.
        final boolean writeEXTDTAStatusByte =
                netAgent_.netConnection_.serverSupportsEXTDTAAbort();

        // If the Derby specific status byte is sent, the number of bytes to
        // send differs from the number of bytes to read (off by one byte).
        long leftToRead = length;
        long bytesToSend = writeEXTDTAStatusByte ? leftToRead + 1 : leftToRead;
        int extendedLengthByteCount = prepScalarStream(chained,
                                                       chainedWithSameCorrelator,
                                                       writeNullByte,
                                                       bytesToSend);
        int nullIndicatorSize = writeNullByte ? 1 : 0;
        int dssMaxDataLength = DssConstants.MAX_DSS_LEN - 6 - 4 -
                nullIndicatorSize - extendedLengthByteCount;
        int bytesToRead = (int)Math.min(bytesToSend, dssMaxDataLength);

        // If we are sending the status byte and we can send the user value as
        // one DSS, correct for the status byte (otherwise we read one byte too
        // much from the stream).
        if (writeEXTDTAStatusByte && bytesToRead == bytesToSend) {
            bytesToRead--;
        }

        buildLengthAndCodePointForLob(codePoint,
                                      bytesToSend,
                                      writeNullByte,
                                      extendedLengthByteCount);
        byte status = DRDAConstants.STREAM_OK;
        int bytesRead = 0;
        do {
            do {
                try {
                    bytesRead =
                        in.read(buffer.array(), buffer.position(), bytesToRead);
                } catch (IOException ioe) {
                    if (netAgent_.getOutputStream() == null) {
                        // The exception has taken down the connection, so we 
                        // check if it was caused by attempting to 
                        // read the stream from our own connection...
                        for (Throwable t = ioe; t != null; t = t.getCause()) {
                            if (t instanceof SqlException
                                    && ((SqlException) t).getSQLState().equals(ExceptionUtil.getSQLStateFromIdentifier(SQLState.NET_WRITE_CHAIN_IS_DIRTY))) {
                                throw new SqlException(netAgent_.logWriter_,
                                        new ClientMessageId(SQLState.NET_LOCATOR_STREAM_PARAMS_NOT_SUPPORTED),
                                        ioe, parameterIndex);
                            }
                        }
                        // Something else has killed the connection, fast forward to despair...
                        throw new SqlException(netAgent_.logWriter_,
                                new ClientMessageId(SQLState.NET_DISCONNECT_EXCEPTION_ON_READ),
                                ioe, parameterIndex, ioe.getMessage());
                    }
                    // The OutPutStream is still intact so try to finish request
                    // with what we managed to read

                    status = DRDAConstants.STREAM_READ_ERROR;
                    padScalarStreamForError(leftToRead, bytesToRead,
                            writeEXTDTAStatusByte, status);
                    // set with SQLSTATE 01004: The value of a string was truncated when assigned to a host variable.
                    netAgent_.accumulateReadException(
                        new SqlException(
                            netAgent_.logWriter_,
                            new ClientMessageId(SQLState.NET_EXCEPTION_ON_READ),
                            parameterIndex, ioe.getMessage(), ioe));

                    return;
                }
                if (bytesRead == -1) {
                    status = DRDAConstants.STREAM_TOO_SHORT;
                    padScalarStreamForError(leftToRead, bytesToRead,
                            writeEXTDTAStatusByte, status);
                    // set with SQLSTATE 01004: The value of a string was truncated when assigned to a host variable.
                    netAgent_.accumulateReadException(
                        new SqlException(netAgent_.logWriter_,
                            new ClientMessageId(SQLState.NET_PREMATURE_EOS),
                            parameterIndex));
                    return;
                } else {
                    bytesToRead -= bytesRead;
                    buffer.position(buffer.position() + bytesRead);
                    leftToRead -= bytesRead;
                }
            } while (bytesToRead > 0);

            bytesToRead = flushScalarStreamSegment(leftToRead, bytesToRead);
        } while (leftToRead > 0);

        // check to make sure that the specified length wasn't too small
        try {
            if (in.read() != -1) {
                status = DRDAConstants.STREAM_TOO_LONG;
                // set with SQLSTATE 01004: The value of a string was truncated when assigned to a host variable.
                netAgent_.accumulateReadException(new SqlException(
                        netAgent_.logWriter_,
                        new ClientMessageId(
                            SQLState.NET_INPUTSTREAM_LENGTH_TOO_SMALL),
                        parameterIndex));
            }
        } catch (Exception e) {
            status = DRDAConstants.STREAM_READ_ERROR;
            netAgent_.accumulateReadException(new SqlException(
                    netAgent_.logWriter_,
                    new ClientMessageId(
                        SQLState.NET_EXCEPTION_ON_STREAMLEN_VERIFICATION),
                    parameterIndex, e.getMessage(), e));
        }
        // Write the status byte to the send buffer.
        if (writeEXTDTAStatusByte) {
            writeEXTDTAStatus(status);
        }
    }



    /**
     * Writes a stream with unknown length onto the wire.
     * <p>
     * To avoid DRDA protocol exceptions, the data is truncated or padded as
     * required to complete the transfer. This can be avoided by implementing
     * the request abort mechanism specified by DRDA, but it is rather complex
     * and may not be worth the trouble.
     * <p>
     * Also note that any exceptions generated while writing the stream will
     * be accumulated and raised at a later time.
     * <p>
     * <em>Implementation note:</em> This method does not support sending
     * values with a specified length using layer B streaming and at the same
     * time applying length checking. For large values layer B streaming may be
     * more efficient than using layer A streaming.
     *
     * @param in the stream to transfer
     * @param writeNullByte whether or not to write a NULL indicator
     * @param parameterIndex one-based parameter index
     * @throws DisconnectException if a severe error condition is encountered,
     *      causing the connection to be broken
     */
    final private void writePlainScalarStream(boolean chained,
                                              boolean chainedWithSameCorrelator,
                                              int codePoint,
                                              java.io.InputStream in,
                                              boolean writeNullByte,
                                              int parameterIndex)
            throws DisconnectException {
        
        // We don't have the metadata available when we create this request
        // object, so we have to check here if we are going to write the status
        // byte or not.
        final boolean writeEXTDTAStatusByte =
                netAgent_.netConnection_.serverSupportsEXTDTAAbort();

        in = new BufferedInputStream( in );

        flushExistingDSS();
        
        ensureLength(DssConstants.MAX_DSS_LEN - buffer.position());
        
        buildDss(true,
                 chained,
                 chainedWithSameCorrelator,
                 DssConstants.GDSFMT_OBJDSS,
                 correlationID_,
                 true);
        
        int spareInDss;
        
        if (writeNullByte) {
            spareInDss = DssConstants.MAX_DSS_LEN - 6 - 4 - 1;
        } else {
            spareInDss = DssConstants.MAX_DSS_LEN - 6 - 4;
        }
                
        buildLengthAndCodePointForLob(codePoint,
                                      writeNullByte);
        
        try{
            
            int bytesRead = 0;
            
            while( ( bytesRead = 
                     in.read(buffer.array(), buffer.position(), spareInDss)
                     ) > -1 ) {
                
                spareInDss -= bytesRead;
                buffer.position(buffer.position() + bytesRead);

                if( spareInDss <= 0 ){
                    
                    if( ! peekStream( (  BufferedInputStream ) in ) )
                        break;
                    
                    flushScalarStreamSegment();

                    buffer.putShort((short) 0xFFFF);
                    
                    spareInDss = DssConstants.MAX_DSS_LEN - 2;
                    
                }
                
            }
        } catch (Exception e) {
            if (writeEXTDTAStatusByte) {
                writeEXTDTAStatus(DRDAConstants.STREAM_READ_ERROR);
            }
            final SqlException sqlex = 
                new SqlException(netAgent_.logWriter_,
                                 new ClientMessageId(SQLState.NET_EXCEPTION_ON_READ),
                                 parameterIndex, e.getMessage(), e);

            netAgent_.accumulateReadException(sqlex);
            
                    return;
        }

        if (writeEXTDTAStatusByte) {
            writeEXTDTAStatus(DRDAConstants.STREAM_OK);
        }
    }


    // Throw DataTruncation, instead of closing connection if input size mismatches
    // An implication of this, is that we need to extend the chaining model
    // for writes to accomodate chained write exceptoins
    final void writeScalarStream(boolean chained,
                                 boolean chainedWithSameCorrelator,
                                 int codePoint,
                                 int length,
                                 java.io.Reader r,
                                 boolean writeNullByte,
                                 int parameterIndex) throws DisconnectException, 
                                                            SqlException{

        writeScalarStream(chained,
                          chainedWithSameCorrelator,
                          codePoint,
                          length * 2L,
                          EncodedInputStream.createUTF16BEStream(r),
                          writeNullByte,
                          parameterIndex);
    }
    
    
    final void writeScalarStream(boolean chained,
                                 boolean chainedWithSameCorrelator,
                                 int codePoint,
                                 java.io.Reader r,
                                 boolean writeNullByte,
                                 int parameterIndex) throws DisconnectException, 
                                                            SqlException{
        writeScalarStream(chained,
                          chainedWithSameCorrelator,
                          codePoint,
                          EncodedInputStream.createUTF16BEStream(r),
                          writeNullByte,
                          parameterIndex);
    }
    
    
    // prepScalarStream does the following prep for writing stream data:
    // 1.  Flushes an existing DSS segment, if necessary
    // 2.  Determines if extended length bytes are needed
    // 3.  Creates a new DSS/DDM header and a null byte indicator, if applicable
    protected final int prepScalarStream(boolean chained,
                                         boolean chainedWithSameCorrelator,
                                         boolean writeNullByte,
                                         long leftToRead)
            throws DisconnectException {
        int nullIndicatorSize = writeNullByte ? 1 : 0;
        int extendedLengthByteCount = calculateExtendedLengthByteCount(
                    leftToRead + 4 + nullIndicatorSize);

        // flush the existing DSS segment if this stream will not fit in the send buffer
        if ((10 + extendedLengthByteCount + nullIndicatorSize +
                leftToRead + buffer.position()) > DssConstants.MAX_DSS_LEN) {
            try {
                if (simpleDssFinalize) {
                    finalizeDssLength();
                } else {
                    finalizePreviousChainedDss(true);
                }
                sendBytes(netAgent_.getOutputStream());
            } catch (java.io.IOException e) {
                netAgent_.throwCommunicationsFailure(e);
            }
        }

        if (netAgent_.netConnection_.getSecurityMechanism() == NetConfiguration.SECMEC_EUSRIDDTA ||
                netAgent_.netConnection_.getSecurityMechanism() == NetConfiguration.SECMEC_EUSRPWDDTA) {
            buildDss(true,
                    chained,
                    chainedWithSameCorrelator,
                    DssConstants.GDSFMT_ENCOBJDSS,
                    correlationID_,
                    true);
        } else
        // buildDss should not call ensure length.
        {
            buildDss(true,
                    chained,
                    chainedWithSameCorrelator,
                    DssConstants.GDSFMT_OBJDSS,
                    correlationID_,
                    true);
        }

        return extendedLengthByteCount;
    }

    
    protected final void flushExistingDSS() throws DisconnectException {
        
        try {
            if (simpleDssFinalize) {
                finalizeDssLength();
            } else {
                finalizePreviousChainedDss(true);
            }
            sendBytes(netAgent_.getOutputStream());
        } catch (java.io.IOException e) {
            netAgent_.throwCommunicationsFailure(e);
        }
        
    }


    // Writes out a scalar stream DSS segment, along with DSS continuation headers,
    // if necessary.
    protected final int flushScalarStreamSegment(long leftToRead,
                                                 int bytesToRead) throws DisconnectException {
        int newBytesToRead = bytesToRead;

        // either at end of data, end of dss segment, or both.
        if (leftToRead != 0) {
            // 32k segment filled and not at end of data.
            if ((Math.min(2 + leftToRead, 32767)) > buffer.remaining()) {
                try {
                    sendBytes(netAgent_.getOutputStream());
                } catch (java.io.IOException ioe) {
                    netAgent_.throwCommunicationsFailure(ioe);
                }
            }
            dssLengthLocation_ = buffer.position();
            buffer.putShort((short) 0xFFFF);
            newBytesToRead = (int)Math.min(leftToRead, 32765L);
        }

        return newBytesToRead;
    }
    
    protected final int flushScalarStreamSegment() throws DisconnectException {
        
        try {
            sendBytes(netAgent_.getOutputStream());
        } catch (java.io.IOException ioe) {
            netAgent_.throwCommunicationsFailure(ioe);
        }
        
        dssLengthLocation_ = buffer.position();
        return DssConstants.MAX_DSS_LEN;
    }
    

    /**
     * Pads a value with zeros until it has reached its defined length.
     * <p>
     * This functionality was introduced to handle the error situation where
     * the actual length of the user stream is shorter than specified. To avoid
     * DRDA protocol errors (or in this case a hang), we have to pad the data
     * until the specified length has been reached. In a later increment the
     * Derby-specific EXTDTA status flag was introduced to allow the client to
     * inform the server that the value sent is invalid.
     *
     * @param leftToRead total number of bytes left to read
     * @param bytesToRead remaining bytes to read before flushing
     * @param writeStatus whether or not to wrote the Derby-specific trailing
     *      EXTDTA status flag (see DRDAConstants)
     * @param status the EXTDTA status (for this data value), ignored if
     *      {@code writeStatus} is {@code false}
     * @throws DisconnectException if flushing the buffer fails
     */
    protected final void padScalarStreamForError(long leftToRead,
                                                 int bytesToRead,
                                                 boolean writeStatus,
                                                 byte status)
            throws DisconnectException {
        do {
            do {
                buffer.put((byte) 0x0); // use 0x0 as the padding byte
                bytesToRead--;
                leftToRead--;
            } while (bytesToRead > 0);

            bytesToRead = flushScalarStreamSegment(leftToRead, bytesToRead);
        } while (leftToRead > 0);

        // Append the EXTDTA status flag if appropriate.
        if (writeStatus) {
            writeEXTDTAStatus(status);
        }
    }

    private final void writeExtendedLengthBytes(int extendedLengthByteCount, long length) {
        int shiftSize = (extendedLengthByteCount - 1) * 8;
        for (int i = 0; i < extendedLengthByteCount; i++) {
            buffer.put((byte) (length >>> shiftSize));
            shiftSize -= 8;
        }
    }

    // experimental lob section - end

    // used to finialize a dss which is already in the buffer
    // before another dss is built.  this includes updating length
    // bytes and chaining bits.
    protected final void finalizePreviousChainedDss(boolean dssHasSameCorrelator) {
        finalizeDssLength();
        int pos = dssLengthLocation_ + 3;
        byte value = buffer.get(pos);
        value |= 0x40;
        if (dssHasSameCorrelator) // for blobs
        {
            value |= 0x10;
        }
        buffer.put(pos, value);
    }

    // method to determine if any data is in the request.
    // this indicates there is a dss object already in the buffer.
    protected final boolean doesRequestContainData() {
        return buffer.position() != 0;
    }

    /**
     * Signal the completion of a DSS Layer A object.
     * <p>
     * The length of the DSS object will be calculated based on the difference
     * between the start of the DSS, saved in the variable
     * {@link #dssLengthLocation_}, and the current offset into the buffer which
     * marks the end of the data.
     * <p>
     * In the event the length requires the use of continuation DSS headers,
     * one for each 32k chunk of data, the data will be shifted and the
     * continuation headers will be inserted with the correct values as needed.
     * Note: In the future, we may try to optimize this approach
     * in an attempt to avoid these shifts.
     */
    protected final void finalizeDssLength() {
        // calculate the total size of the dss and the number of bytes which would
        // require continuation dss headers.  The total length already includes the
        // the 6 byte dss header located at the beginning of the dss.  It does not
        // include the length of any continuation headers.
        int totalSize = buffer.position() - dssLengthLocation_;
        int bytesRequiringContDssHeader = totalSize - 32767;

        // determine if continuation headers are needed
        if (bytesRequiringContDssHeader > 0) {

            // the continuation headers are needed, so calculate how many.
            // after the first 32767 worth of data, a continuation header is
            // needed for every 32765 bytes (32765 bytes of data + 2 bytes of
            // continuation header = 32767 Dss Max Size).
            int contDssHeaderCount = bytesRequiringContDssHeader / 32765;
            if (bytesRequiringContDssHeader % 32765 != 0) {
                contDssHeaderCount++;
            }

            // right now the code will shift to the right.  In the future we may want
            // to try something fancier to help reduce the copying (maybe keep
            // space in the beginning of the buffer??).
            // the offset points to the next available offset in the buffer to place
            // a piece of data, so the last dataByte is at offset -1.
            // various bytes will need to be shifted by different amounts
            // depending on how many dss headers to insert so the amount to shift
            // will be calculated and adjusted as needed.  ensure there is enough room
            // for all the conutinuation headers and adjust the offset to point to the
            // new end of the data.
            int dataByte = buffer.position() - 1;
            int shiftOffset = contDssHeaderCount * 2;
            ensureLength(shiftOffset);
            buffer.position(buffer.position() + shiftOffset);

            // mark passOne to help with calculating the length of the final (first or
            // rightmost) continuation header.
            boolean passOne = true;
            do {
                // calculate chunk of data to shift
                int dataToShift = bytesRequiringContDssHeader % 32765;
                if (dataToShift == 0) {
                    dataToShift = 32765;
                }

                // perform the shift
                dataByte -= dataToShift;
                byte[] array = buffer.array();
                System.arraycopy(array, dataByte + 1,
                        array, dataByte + shiftOffset + 1, dataToShift);

                // calculate the value the value of the 2 byte continuation dss header which
                // includes the length of itself.  On the first pass, if the length is 32767
                // we do not want to set the continuation dss header flag.
                int twoByteContDssHeader = dataToShift + 2;
                if (passOne) {
                    passOne = false;
                } else {
                    if (twoByteContDssHeader == 32767) {
                        twoByteContDssHeader = 0xFFFF;
                    }
                }

                // insert the header's length bytes
                buffer.putShort(dataByte + shiftOffset - 1,
                                (short) twoByteContDssHeader);

                // adjust the bytesRequiringContDssHeader and the amount to shift for
                // data in upstream headers.
                bytesRequiringContDssHeader -= dataToShift;
                shiftOffset -= 2;

                // shift and insert another header for more data.
            } while (bytesRequiringContDssHeader > 0);

            // set the continuation dss header flag on for the first header
            totalSize = 0xFFFF;

        }

        // insert the length bytes in the 6 byte dss header.
        buffer.putShort(dssLengthLocation_, (short) totalSize);
    }

    // mark the location of a two byte ddm length field in the buffer,
    // skip the length bytes for later update, and insert a ddm codepoint
    // into the buffer.  The value of the codepoint is not checked.
    // this length will be automatically updated when construction of
    // the ddm object is complete (see updateLengthBytes method).
    // Note: this mechanism handles extended length ddms.
    protected final void markLengthBytes(int codePoint) {
        ensureLength(4);

        // save the location of length bytes in the mark stack.
        mark();

        // skip the length bytes and insert the codepoint
        buffer.position(buffer.position() + 2);
        buffer.putShort((short) codePoint);
    }

    // mark an offest into the buffer by placing the current offset value on
    // a stack.
    private final void mark() {
        markStack_[top_++] = buffer.position();
    }

    // remove and return the top offset value from mark stack.
    private final int popMark() {
        return markStack_[--top_];
    }

    protected final void markForCachingPKGNAMCSN() {
        mark();
    }

    protected final int popMarkForCachingPKGNAMCSN() {
        return popMark();
    }

    // Called to update the last ddm length bytes marked (lengths are updated
    // in the reverse order that they are marked).  It is up to the caller
    // to make sure length bytes were marked before calling this method.
    // If the length requires ddm extended length bytes, the data will be
    // shifted as needed and the extended length bytes will be automatically
    // inserted.
    protected final void updateLengthBytes() throws SqlException {
        // remove the top length location offset from the mark stack\
        // calculate the length based on the marked location and end of data.
        int lengthLocation = popMark();
        int length = buffer.position() - lengthLocation;

        // determine if any extended length bytes are needed.  the value returned
        // from calculateExtendedLengthByteCount is the number of extended length
        // bytes required. 0 indicates no exteneded length.
        int extendedLengthByteCount = calculateExtendedLengthByteCount(length);
        if (extendedLengthByteCount != 0) {

            // ensure there is enough room in the buffer for the extended length bytes.
            ensureLength(extendedLengthByteCount);

            // calculate the length to be placed in the extended length bytes.
            // this length does not include the 4 byte llcp.
            int extendedLength = length - 4;

            // shift the data to the right by the number of extended length bytes needed.
            int extendedLengthLocation = lengthLocation + 4;
            byte[] array = buffer.array();
            System.arraycopy(array,
                    extendedLengthLocation,
                    array,
                    extendedLengthLocation + extendedLengthByteCount,
                    extendedLength);

            // write the extended length
            int shiftSize = (extendedLengthByteCount - 1) * 8;
            for (int i = 0; i < extendedLengthByteCount; i++) {
                buffer.put(extendedLengthLocation++,
                           (byte) (extendedLength >>> shiftSize));
                shiftSize -= 8;
            }
            // adjust the offset to account for the shift and insert
            buffer.position(buffer.position() + extendedLengthByteCount);

            // the two byte length field before the codepoint contains the length
            // of itself, the length of the codepoint, and the number of bytes used
            // to hold the extended length.  the 2 byte length field also has the first
            // bit on to indicate extended length bytes were used.
            length = extendedLengthByteCount + 4;
            length |= 0x8000;
        }

        // write the 2 byte length field (2 bytes before codepoint).
        buffer.putShort(lengthLocation, (short) length);
    }

    // helper method to calculate the minimum number of extended length bytes needed
    // for a ddm.  a return value of 0 indicates no extended length needed.
    private final int calculateExtendedLengthByteCount(long ddmSize) //throws SqlException
    {
        // according to Jim and some tests perfomred on Lob data,
        // the extended length bytes are signed.  Assume that
        // if this is the case for Lobs, it is the case for
        // all extended length scenarios.
        if (ddmSize <= 0x7FFF) {
            return 0;
        } else if (ddmSize <= 0x7FFFFFFFL) {
            return 4;
        } else if (ddmSize <= 0x7FFFFFFFFFFFL) {
            return 6;
        } else {
            return 8;
        }
    }

    // insert the padByte into the buffer by length number of times.
    final void padBytes(byte padByte, int length) {
        ensureLength(length);
        for (int i = 0; i < length; i++) {
            buffer.put(padByte);
        }
    }

    // insert an unsigned single byte value into the buffer.
    final void write1Byte(int value) {
        writeByte((byte) value);
    }

    // insert 3 unsigned bytes into the buffer.  this was
    // moved up from NetStatementRequest for performance
    final void buildTripletHeader(int tripletLength,
                                  int tripletType,
                                  int tripletId) {
        ensureLength(3);
        buffer.put((byte) tripletLength);
        buffer.put((byte) tripletType);
        buffer.put((byte) tripletId);
    }

    final void writeLidAndLengths(int[][] lidAndLengthOverrides, int count, int offset) {
        ensureLength(count * 3);
        for (int i = 0; i < count; i++, offset++) {
            buffer.put((byte) lidAndLengthOverrides[offset][0]);
            buffer.putShort((short) lidAndLengthOverrides[offset][1]);
        }
    }

    // if mdd overrides are not required, lids and lengths are copied straight into the
    // buffer.
    // otherwise, lookup the protocolType in the map.  if an entry exists, substitute the
    // protocolType with the corresponding override lid.
    final void writeLidAndLengths(int[][] lidAndLengthOverrides,
                                  int count,
                                  int offset,
                                  boolean mddRequired,
                                  java.util.Hashtable map) {
        if (!mddRequired) {
            writeLidAndLengths(lidAndLengthOverrides, count, offset);
        }
        // if mdd overrides are required, lookup the protocolType in the map, and substitute
        // the protocolType with the override lid.
        else {
            ensureLength(count * 3);
            int protocolType, overrideLid;
            Object entry;
            for (int i = 0; i < count; i++, offset++) {
                protocolType = lidAndLengthOverrides[offset][0];
                // lookup the protocolType in the protocolType->overrideLid map
                // if an entry exists, replace the protocolType with the overrideLid
                entry = map.get(protocolType);
                overrideLid = (entry == null) ? protocolType : ((Integer) entry).intValue();
                buffer.put((byte) overrideLid);
                buffer.putShort((short) lidAndLengthOverrides[offset][1]);
            }
        }
    }

// perf end

    // insert a big endian unsigned 2 byte value into the buffer.
    final void write2Bytes(int value) {
        writeShort((short) value);
    }

    // insert a big endian unsigned 4 byte value into the buffer.
    final void write4Bytes(long value) {
        writeInt((int) value);
    }

    // copy length number of bytes starting at offset 0 of the byte array, buf,
    // into the buffer.  it is up to the caller to make sure buf has at least length
    // number of elements.  no checking will be done by this method.
    final void writeBytes(byte[] buf, int length) {
        ensureLength(length);
        buffer.put(buf, 0, length);
    }

    final void writeBytes(byte[] buf) {
        writeBytes(buf, buf.length);
    }

    // insert a pair of unsigned 2 byte values into the buffer.
    final void writeCodePoint4Bytes(int codePoint, int value) {                                                      // should this be writeCodePoint2Bytes
        ensureLength(4);
        buffer.putShort((short) codePoint);
        buffer.putShort((short) value);
    }

    // insert a 4 byte length/codepoint pair and a 1 byte unsigned value into the buffer.
    // total of 5 bytes inserted in buffer.
    protected final void writeScalar1Byte(int codePoint, int value) {
        ensureLength(5);
        buffer.put((byte) 0x00);
        buffer.put((byte) 0x05);
        buffer.putShort((short) codePoint);
        buffer.put((byte) value);
    }

    // insert a 4 byte length/codepoint pair and a 2 byte unsigned value into the buffer.
    // total of 6 bytes inserted in buffer.
    final void writeScalar2Bytes(int codePoint, int value) {
        ensureLength(6);
        buffer.put((byte) 0x00);
        buffer.put((byte) 0x06);
        buffer.putShort((short) codePoint);
        buffer.putShort((short) value);
    }

    // insert a 4 byte length/codepoint pair and a 4 byte unsigned value into the
    // buffer.  total of 8 bytes inserted in the buffer.
    protected final void writeScalar4Bytes(int codePoint, long value) {
        ensureLength(8);
        buffer.put((byte) 0x00);
        buffer.put((byte) 0x08);
        buffer.putShort((short) codePoint);
        buffer.putInt((int) value);
    }

    // insert a 4 byte length/codepoint pair and a 8 byte unsigned value into the
    // buffer.  total of 12 bytes inserted in the buffer.
    final void writeScalar8Bytes(int codePoint, long value) {
        ensureLength(12);
        buffer.put((byte) 0x00);
        buffer.put((byte) 0x0C);
        buffer.putShort((short) codePoint);
        buffer.putLong(value);
    }

    // insert a 4 byte length/codepoint pair into the buffer.
    // total of 4 bytes inserted in buffer.
    // Note: the length value inserted in the buffer is the same as the value
    // passed in as an argument (this value is NOT incremented by 4 before being
    // inserted).
    final void writeLengthCodePoint(int length, int codePoint) {
        ensureLength(4);
        buffer.putShort((short) length);
        buffer.putShort((short) codePoint);
    }

    // insert a 4 byte length/codepoint pair into the buffer followed
    // by length number of bytes copied from array buf starting at offset 0.
    // the length of this scalar must not exceed the max for the two byte length
    // field.  This method does not support extended length.  The length
    // value inserted in the buffer includes the number of bytes to copy plus
    // the size of the llcp (or length + 4). It is up to the caller to make sure
    // the array, buf, contains at least length number of bytes.
    final void writeScalarBytes(int codePoint, byte[] buf, int length) {
        writeScalarBytes(codePoint, buf, 0, length);
    }

    // insert a 4 byte length/codepoint pair into the buffer.
    // total of 4 bytes inserted in buffer.
    // Note: datalength will be incremented by the size of the llcp, 4,
    // before being inserted.
    final void writeScalarHeader(int codePoint, int dataLength) {
        writeLengthCodePoint(dataLength + 4, codePoint);
        ensureLength(dataLength);
    }

    /**
     * Write string with no minimum or maximum limit.
     * @param codePoint codepoint to write  
     * @param string    value to write
     * @throws SqlException
     */
    final void writeScalarString(int codePoint, String string) throws SqlException {
        writeScalarString(codePoint, string, 0,Integer.MAX_VALUE,null);
        
    } 
   
    /**
     *  insert a 4 byte length/codepoint pair plus ddm character data into
     * the buffer.  This method assumes that the String argument can be
     * converted by the ccsid manager.  This should be fine because usually
     * there are restrictions on the characters which can be used for ddm
     * character data. 
     * The two byte length field will contain the length of the character data
     * and the length of the 4 byte llcp.  This method does not handle
     * scenarios which require extended length bytes.
     * 
     * @param codePoint  codepoint to write 
     * @param string     value
     * @param byteMinLength minimum length. String will be padded with spaces 
     * if value is too short. Assumes space character is one byte.
     * @param byteLengthLimit  Limit to string length. SQLException will be 
     * thrown if we exceed this limit.
     * @param sqlState  SQLState to throw with string as param if byteLengthLimit
     * is exceeded.
     * @throws SqlException if string exceeds byteLengthLimit
     */
    final void writeScalarString(int codePoint, String string, int byteMinLength,
            int byteLengthLimit, String sqlState) throws SqlException {
        
        /* Grab the current CCSID MGR from the NetAgent */ 
        CcsidManager currentCcsidMgr = netAgent_.getCurrentCcsidManager();

        // We don't know the length of the string yet, so set it to 0 for now.
        // Will be updated later.
        int lengthPos = buffer.position();
        writeLengthCodePoint(0, codePoint);

        int stringByteLength = encodeString(string);
        if (stringByteLength > byteLengthLimit) {
            throw new SqlException(netAgent_.logWriter_,
                    new ClientMessageId(sqlState), string);
        }

        // pad if we don't reach the byteMinLength limit
        if (stringByteLength < byteMinLength) {
            padBytes(currentCcsidMgr.space_, byteMinLength - stringByteLength);
            stringByteLength = byteMinLength;
        }

        // Update the length field. The length includes two bytes for the
        // length field itself and two bytes for the codepoint.
        buffer.putShort(lengthPos, (short) (stringByteLength + 4));
    }

    /**
     * Encode a string and put it into the buffer. A larger buffer will be
     * allocated if the current buffer is too small to hold the entire string.
     *
     * @param string the string to encode
     * @return the number of bytes in the encoded representation of the string
     */
    private int encodeString(String string) throws SqlException {
        int startPos = buffer.position();
        CharBuffer src = CharBuffer.wrap(string);
        CcsidManager ccsidMgr = netAgent_.getCurrentCcsidManager();
        ccsidMgr.startEncoding();
        while (!ccsidMgr.encode(src, buffer, netAgent_)) {
            // The buffer was too small to hold the entire string. Let's
            // allocate a larger one. We don't know how much more space we
            // need, so we just tell ensureLength() that we need more than
            // what we have, until we manage to encode the entire string.
            // ensureLength() typically doubles the size of the buffer, so
            // we shouldn't have to call it many times before we get a large
            // enough buffer.
            ensureLength(buffer.remaining() + 1);
        }
        return buffer.position() - startPos;
    }

    // this method writes a 4 byte length/codepoint pair plus the bytes contained
    // in array buff to the buffer.
    // the 2 length bytes in the llcp will contain the length of the data plus
    // the length of the llcp.  This method does not handle scenarios which
    // require extended length bytes.
    final void writeScalarBytes(int codePoint, byte[] buff) {
        writeScalarBytes(codePoint, buff, 0, buff.length);
    }

    // this method inserts a 4 byte length/codepoint pair plus length number of bytes
    // from array buff starting at offset start.
    // Note: no checking will be done on the values of start and length with respect
    // the actual length of the byte array.  The caller must provide the correct
    // values so an array index out of bounds exception does not occur.
    // the length will contain the length of the data plus the length of the llcp.
    // This method does not handle scenarios which require extended length bytes.
    final void writeScalarBytes(int codePoint, byte[] buff, int start, int length) {
        writeLengthCodePoint(length + 4, codePoint);
        ensureLength(length);
        buffer.put(buff, start, length);
    }

    // insert a 4 byte length/codepoint pair plus ddm binary data into the
    // buffer.  The binary data is padded if needed with the padByte
    // if the data is less than paddedLength.
    // Note: this method is not to be used for truncation and buff.length
    // must be <= paddedLength.
    // The llcp length bytes will contain the length of the data plus
    // the length of the llcp or 4.
    // This method does not handle scenarios which require extended length bytes.
    final void writeScalarPaddedBytes(int codePoint, byte[] buff, int paddedLength, byte padByte) {
        writeLengthCodePoint(paddedLength + 4, codePoint);
        writeScalarPaddedBytes(buff, paddedLength, padByte);
    }

    // this method inserts binary data into the buffer and pads the
    // data with the padByte if the data length is less than the paddedLength.
    // Not: this method is not to be used for truncation and buff.length
    // must be <= paddedLength.
    final void writeScalarPaddedBytes(byte[] buff, int paddedLength, byte padByte) {
        writeBytes(buff);
        padBytes(padByte, paddedLength - buff.length);
    }

    // write the request to the OutputStream and flush the OutputStream.
    // trace the send if PROTOCOL trace is on.
    protected void flush(java.io.OutputStream socketOutputStream) throws java.io.IOException {
        if (doesRequestContainData()) {
            finalizeDssLength();
            sendBytes(socketOutputStream);
        }
    }

    protected void sendBytes(java.io.OutputStream socketOutputStream) throws java.io.IOException {
        try {
            netAgent_.markWriteChainAsDirty();
            socketOutputStream.write(buffer.array(), 0, buffer.position());
            socketOutputStream.flush();
        } finally {
            if (netAgent_.logWriter_ != null && passwordIncluded_) {
                // if password is in the buffer, need to mask it out.
                maskOutPassword();
                passwordIncluded_ = false;
            }
            if (netAgent_.loggingEnabled()) {
                ((NetLogWriter) netAgent_.logWriter_).traceProtocolFlow(
                        buffer.array(),
                        0,
                        buffer.position(),
                        NetLogWriter.TYPE_TRACE_SEND,
                        "Request",
                        "flush",
                        1); // tracepoint
            }
            clearBuffer();
        }
    }

    final void maskOutPassword() {
        int savedPos = buffer.position();
        try {
            String maskChar = "*";
            // construct a mask using the maskChar.
            StringBuffer mask = new StringBuffer();
            for (int i = 0; i < passwordLength_; i++) {
                mask.append(maskChar);
            }
            // try to write mask over password.
            buffer.position(passwordStart_);
            encodeString(mask.toString());
        } catch (SqlException sqle) {
            // failed to convert mask,
            // them simply replace with 0xFF.
            for (int i = 0; i < passwordLength_; i++) {
                buffer.put(passwordStart_ + i, (byte) 0xFF);
            }
        } finally {
            buffer.position(savedPos);
        }
    }

    // insert a java byte into the buffer.
    final void writeByte(byte v) {
        ensureLength(1);
        buffer.put(v);
    }

    // insert a java short into the buffer.
    final void writeShort(short v) {
        ensureLength(2);
        buffer.putShort(v);
    }

    // insert a java int into the buffer.
    void writeInt(int v) {
        ensureLength(4);
        buffer.putInt(v);
    }

    /**
     * Writes a long into the buffer, using six bytes.
     *
     * @param v the value to write
     * @throws IllegalArgumentException if the long value is too large to be
     *      represented by six bytes.
     */
    final void writeLong6Bytes(long v) {
        ensureLength(6);
        buffer.putShort((short) (v >> 32));
        buffer.putInt((int) v);
    }

    // insert a java long into the buffer.
    final void writeLong(long v) {
        ensureLength(8);
        buffer.putLong(v);
    }

    //-- The following are the write short/int/long in bigEndian byte ordering --

    // when writing Fdoca data.
    protected void writeShortFdocaData(short v) {
        writeShort(v);
    }

    // when writing Fdoca data.
    protected void writeIntFdocaData(int v) {
        writeInt(v);
    }

    // when writing Fdoca data.
    protected void writeLongFdocaData(long v) {
        writeLong(v);
    }

    // insert a java float into the buffer.
    protected void writeFloat(float v) {
        writeInt(Float.floatToIntBits(v));
    }

    // insert a java double into the buffer.
    protected void writeDouble(double v) {
        writeLong(Double.doubleToLongBits(v));
    }

    // insert a java.math.BigDecimal into the buffer.
    final void writeBigDecimal(java.math.BigDecimal v,
                               int declaredPrecision,
                               int declaredScale) throws SqlException {
        ensureLength(16);
        int length = Decimal.bigDecimalToPackedDecimalBytes(
                buffer.array(), buffer.position(),
                v, declaredPrecision, declaredScale);
        buffer.position(buffer.position() + length);
    }

    final void writeDate(DateTimeValue date) throws SqlException {
        try
        {
            ensureLength(10);
            DateTime.dateToDateBytes(buffer.array(), buffer.position(), date);
            buffer.position(buffer.position() + 10);
        } catch (java.io.UnsupportedEncodingException e) {
            throw new SqlException(netAgent_.logWriter_, 
                    new ClientMessageId(SQLState.UNSUPPORTED_ENCODING),
                    "java.sql.Date", "DATE", e);
        }
    }

    final void writeTime(DateTimeValue time) throws SqlException {
        try{
            ensureLength(8);
            DateTime.timeToTimeBytes(buffer.array(), buffer.position(), time);
            buffer.position(buffer.position() + 8);
        } catch(UnsupportedEncodingException e) {
            throw new SqlException(netAgent_.logWriter_, 
                    new ClientMessageId(SQLState.UNSUPPORTED_ENCODING),
                    "java.sql.Time", "TIME", e);
      }
    }

    final void writeTimestamp(DateTimeValue timestamp) throws SqlException {
        try{
            boolean supportsTimestampNanoseconds = netAgent_.netConnection_.serverSupportsTimestampNanoseconds();
            int length = DateTime.getTimestampLength( supportsTimestampNanoseconds );
            ensureLength(length);
            DateTime.timestampToTimestampBytes(
                    buffer.array(), buffer.position(),
                    timestamp, supportsTimestampNanoseconds);
            buffer.position(buffer.position() + length);
        }catch(UnsupportedEncodingException e) {
            throw new SqlException(netAgent_.logWriter_,  
                    new ClientMessageId(SQLState.UNSUPPORTED_ENCODING),
                    "java.sql.Timestamp", "TIMESTAMP", e);
        }
    }

    // insert a java boolean into the buffer.  the boolean is written
    // as a signed byte having the value 0 or 1.
    final void writeBoolean(boolean v) {
        write1Byte(v ? 1 : 0);
    }

    // follows the TYPDEF rules (note: don't think ddm char data is ever length
    // delimited)
    // should this throw SqlException
    // Will write a varchar mixed or single
    //  this was writeLDString
    final void writeSingleorMixedCcsidLDString(String s, String encoding) throws SqlException {
        byte[] b;
        try {
            b = s.getBytes(encoding);
        } catch (UnsupportedEncodingException e) {
            throw new SqlException(netAgent_.logWriter_,  
                    new ClientMessageId(SQLState.UNSUPPORTED_ENCODING),
                    "String", "byte", e);
        }
        if (b.length > 0x7FFF) {
            throw new SqlException(netAgent_.logWriter_, 
                new ClientMessageId(SQLState.LANG_STRING_TOO_LONG),
                "32767");
        }
        writeLDBytes(b);
    }


    final void writeLDBytes(byte[] bytes) {
        writeLDBytesX(bytes.length, bytes);
    }

    // private helper method which should only be called by a Request method.
    // must call ensureLength before calling this method.
    // added for code reuse and helps perf by reducing ensureLength calls.
    // ldSize and bytes.length may not be the same.  this is true
    // when writing graphic ld strings.
    private final void writeLDBytesX(int ldSize, byte[] bytes) {
        writeLDBytesXSubset( ldSize, bytes.length, bytes );
    }

    // private helper method for writing just a subset of a byte array
    private final void writeLDBytesXSubset( int ldSize, int bytesToCopy, byte[] bytes )
    {
        writeShort((short) ldSize);
        writeBytes(bytes, bytesToCopy);
    }

    // should not be called if val is null
    final void writeUDT( Object val ) throws SqlException
    {
        byte[] buffer = null;
        int length = 0;
        
        try
        {
            PublicBufferOutputStream pbos = new PublicBufferOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream( pbos );

            oos.writeObject( val );

            buffer = pbos.getBuffer();
            length = pbos.size();
        }
        catch (Exception e)
        {
            throw new SqlException
                (
                 netAgent_.logWriter_, 
                 new ClientMessageId (SQLState.NET_MARSHALLING_UDT_ERROR),
                 e.getMessage(),
                 e
                 );
        }

        if ( length > DRDAConstants.MAX_DRDA_UDT_SIZE )
        {
            throw new SqlException
                (
                 netAgent_.logWriter_, 
                 new ClientMessageId(SQLState.LANG_OUTSIDE_RANGE_FOR_DATATYPE),
                 Integer.toString( DRDAConstants.MAX_DRDA_UDT_SIZE ),
                 val.getClass().getName()
                 );
        }

        writeLDBytesXSubset( length, length, buffer );
    }

    // does it follows
    // ccsid manager or typdef rules.  should this method write ddm character
    // data or fodca data right now it is coded for ddm char data only
    final void writeDDMString(String s) throws SqlException {
        encodeString(s);
    }

    private void buildLengthAndCodePointForLob(int codePoint,
                                               long leftToRead,
                                               boolean writeNullByte,
                                               int extendedLengthByteCount) throws DisconnectException {
        int nullIndicatorSize = writeNullByte ? 1 : 0;
        if (extendedLengthByteCount > 0) {
            // method should never ensure length
            writeLengthCodePoint(0x8004 + extendedLengthByteCount, codePoint);
            writeExtendedLengthBytes(
                    extendedLengthByteCount, leftToRead + nullIndicatorSize);
        } else {
            if (SanityManager.DEBUG) {
                SanityManager.ASSERT(leftToRead +4 + nullIndicatorSize <=
                        DssConstants.MAX_DSS_LEN);
            }
            writeLengthCodePoint((int)(leftToRead + 4 + nullIndicatorSize),
                    codePoint);
        }

        // write the null byte, if necessary
        if (writeNullByte) {
            write1Byte(0x0);
        }

    }
    
    
    private void buildLengthAndCodePointForLob(int codePoint,
                                               boolean writeNullByte) throws DisconnectException {
        
        //0x8004 is for Layer B Streaming. 
        //See DRDA, Version 3, Volume 3: Distributed Data Management (DDM) Architecture page 315.
        writeLengthCodePoint(0x8004, codePoint);
        
        // write the null byte, if necessary
        if (writeNullByte) {
            write1Byte(0x0);
        }
        
    }

    /**
     * Writes the Derby-specific EXTDTA status flag to the send buffer.
     * <p>
     * The existing buffer is flushed to make space for the flag if required.
     *
     * @param flag the Derby-specific EXTDTA status flag
     * @throws DisconnectException if flushing the buffer fails
     */
    private void writeEXTDTAStatus(byte flag)
            throws DisconnectException {
        // Write the status byte to the send buffer.
        // Make sure we have enough space for the status byte.

        if (buffer.remaining() == 0) {
            flushScalarStreamSegment(1, 0); // Trigger a flush.
        }
        buffer.put(flag);
        // The last byte will be sent on the next flush.
    }

    public void setDssLengthLocation(int location) {
        dssLengthLocation_ = location;
    }
    
    
    public void setCorrelationID(int id) {
        correlationID_ = id;
    }
    
    
    private static boolean peekStream( BufferedInputStream in ) 
        throws IOException {
        
        in.mark( 1 );
        boolean notYet =  in.read() > -1;
        in.reset();
        return notYet;
        
    }

}
