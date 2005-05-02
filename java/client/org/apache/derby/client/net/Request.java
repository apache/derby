/*

   Derby - Class org.apache.derby.client.net.Request

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
package org.apache.derby.client.net;

import org.apache.derby.client.am.DisconnectException;
import org.apache.derby.client.am.EncryptionManager;
import org.apache.derby.client.am.SqlException;
import org.apache.derby.client.am.Utils;

public class Request {

    // byte array buffer used for constructing requests.
    // currently requests are built starting at the beginning of the buffer.
    protected byte[] bytes_;

    // keeps track of the next position to place a byte in the buffer.
    // so the last valid byte in the message is at bytes_[offset - 1]
    protected int offset_;

    // a stack is used to keep track of offsets into the buffer where 2 byte
    // ddm length values are located.  these length bytes will be automatically updated
    // by this object when construction of a particular object has completed.
    // right now the max size of the stack is 10. this is an arbitrary number which
    // should be sufficiently large enough to handle all situations.
    private final static int MAX_MARKS_NESTING = 10;
    private int[] markStack_ = new int[MAX_MARKS_NESTING];
    private int top_ = 0;

    // the ccsid manager for the connection is stored in this object.  it will
    // be used when constructing character ddm data.  it will NOT be used for
    // building any FDOCA data.
    protected CcsidManager ccsidManager_;

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
    Request(NetAgent netAgent, int minSize, CcsidManager ccsidManager) {
        netAgent_ = netAgent;
        bytes_ = new byte[minSize];
        ccsidManager_ = ccsidManager;
        clearBuffer();
    }

    // construct a request object specifying the ccsid manager instance
    // to be used when building ddm character data.  This will also create
    // a buffer using the default size (see final static DEFAULT_BUFFER_SIZE value).
    Request(NetAgent netAgent, CcsidManager ccsidManager, int bufferSize) {
        //this (netAgent, Request.DEFAULT_BUFFER_SIZE, ccsidManager);
        this(netAgent, bufferSize, ccsidManager);
    }

    protected final void clearBuffer() {
        offset_ = 0;
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

    // set the ccsid manager value.  this method allows the ccsid manager to be
    // changed so a request object can be reused by different connections with
    // different ccsid managers.
    final void setCcsidMgr(CcsidManager ccsidManager) {
        ccsidManager_ = ccsidManager;
    }

    // ensure length at the end of the buffer for a certain amount of data.
    // if the buffer does not contain sufficient room for the data, the buffer
    // will be expanded by the larger of (2 * current size) or (current size + length).
    // the data from the previous buffer is copied into the larger buffer.
    protected final void ensureLength(int length) {
        if (length > bytes_.length) {
            byte newBytes[] = new byte[Math.max(bytes_.length << 1, length)];
            System.arraycopy(bytes_, 0, newBytes, 0, offset_);
            bytes_ = newBytes;
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

        ensureLength(offset_ + 6);

        // save the length position and skip
        // note: the length position is saved so it can be updated
        // with a different value later.
        dssLengthLocation_ = offset_;
        // always turn on chaining flags... this is helpful for lobs...
        // these bytes will get rest if dss lengths are finalized.
        bytes_[offset_++] = (byte) 0xFF;
        bytes_[offset_++] = (byte) 0xFF;

        // insert the manditory 0xD0 and the dssType
        bytes_[offset_++] = (byte) 0xD0;
        if (chainedToNextStructure) {
            dssType |= DssConstants.GDSCHAIN;
            if (nextHasSameCorrelator) {
                dssType |= DssConstants.GDSCHAIN_SAME_ID;
            }
        }
        bytes_[offset_++] = (byte) (dssType & 0xff);

        // write the request correlation id
        // use method that writes a short
        bytes_[offset_++] = (byte) ((corrId >>> 8) & 0xff);
        bytes_[offset_++] = (byte) (corrId & 0xff);

        simpleDssFinalize = simpleFinalizeBuildingNextDss;
    }

    // We need to reuse the agent's sql exception accumulation mechanism
    // for this write exception, pad if the length is too big, and truncation if the length is too small
    final void writeScalarStream(boolean chained,
                                 boolean chainedWithSameCorrelator,
                                 int codePoint,
                                 int length,
                                 java.io.InputStream in,
                                 boolean writeNullByte,
                                 int parameterIndex) throws DisconnectException, SqlException {
        int leftToRead = length;
        int extendedLengthByteCount = prepScalarStream(chained,
                chainedWithSameCorrelator,
                writeNullByte,
                leftToRead);
        int bytesToRead;

        if (writeNullByte) {
            bytesToRead = Utils.min(leftToRead, DssConstants.MAX_DSS_LEN - 6 - 4 - 1 - extendedLengthByteCount);
        } else {
            bytesToRead = Utils.min(leftToRead, DssConstants.MAX_DSS_LEN - 6 - 4 - extendedLengthByteCount);
        }

        if (netAgent_.netConnection_.getSecurityMechanism() == NetConfiguration.SECMEC_EUSRIDDTA ||
                netAgent_.netConnection_.getSecurityMechanism() == NetConfiguration.SECMEC_EUSRPWDDTA) {

            byte[] lengthAndCodepoint;
            lengthAndCodepoint = buildLengthAndCodePointForEncryptedLob(codePoint,
                    leftToRead,
                    writeNullByte,
                    extendedLengthByteCount);



            // we need to stream the input, rather than fully materialize it
            // write the data

            byte[] clearedBytes = new byte[leftToRead];
            int bytesRead = 0;
            int totalBytesRead = 0;
            int pos = 0;
            do {
                try {
                    bytesRead = in.read(clearedBytes, pos, leftToRead);
                    totalBytesRead += bytesRead;
                } catch (java.io.IOException e) {
                    padScalarStreamForError(leftToRead, bytesToRead);
                    // set with SQLSTATE 01004: The value of a string was truncated when assigned to a host variable.
                    netAgent_.accumulateReadException(new SqlException(netAgent_.logWriter_,
                            "Encountered an IOException reading InputStream, parameter #" +
                            parameterIndex +
                            ".  Remaining data has been padded with 0x0. Message: " +
                            e.getMessage()));
                    return;
                }
                if (bytesRead == -1) {
                    //padScalarStreamForError(leftToRead, bytesToRead);
                    // set with SQLSTATE 01004: The value of a string was truncated when assigned to a host variable.
                    /*throw new SqlException(netAgent_.logWriter_,
                        "End of Stream prematurely reached while reading InputStream, parameter #" +
                        parameterIndex +
                        ".  Remaining data has been padded with 0x0.");*/
                    //is it OK to do a chain break Exception here. It's not good to
                    //pad it with 0 and encrypt and send it to the server because it takes too much time
                    //can't just throw a SQLException either because some of the data PRPSQLSTT etc have already
                    //been sent to the server, and server is waiting for EXTDTA, server hangs for this.
                    netAgent_.accumulateChainBreakingReadExceptionAndThrow(new org.apache.derby.client.am.DisconnectException(netAgent_,
                            "End of Stream prematurely reached while reading InputStream, parameter #" +
                            parameterIndex +
                            ". "));
                    return;

                    /*netAgent_.accumulateReadException(
                        new SqlException(netAgent_.logWriter_,
                        "End of Stream prematurely reached while reading InputStream, parameter #" +
                        parameterIndex +
                        ".  Remaining data has been padded with 0x0."));
                    return;*/
                } else {
                    pos += bytesRead;
                    //offset_ += bytesRead;  //comment this out for data stream encryption.
                    leftToRead -= bytesRead;
                }

            } while (leftToRead > 0);

            // check to make sure that the specified length wasn't too small
            try {
                if (in.read() != -1) {
                    // set with SQLSTATE 01004: The value of a string was truncated when assigned to a host variable.
                    netAgent_.accumulateReadException(new SqlException(netAgent_.logWriter_,
                            "The specified size of the InputStream, parameter #" +
                            parameterIndex +
                            ", is less than the actual InputStream length"));
                }
            } catch (java.io.IOException e) {
                netAgent_.accumulateReadException(new SqlException(netAgent_.logWriter_,
                        "Encountered error in stream length verification for InputStream, parameter #" +
                        parameterIndex +
                        ".  Message: " + e.getMessage()));
            }

            byte[] newClearedBytes = new byte[clearedBytes.length +
                    lengthAndCodepoint.length];
            System.arraycopy(lengthAndCodepoint, 0, newClearedBytes, 0,
                    lengthAndCodepoint.length);
            System.arraycopy(clearedBytes, 0, newClearedBytes, lengthAndCodepoint.length, clearedBytes.length);
            //it's wrong here, need to add in the real length after the codepoing 146c
            byte[] encryptedBytes;
            encryptedBytes = netAgent_.netConnection_.getEncryptionManager().
                    encryptData(newClearedBytes,
                            NetConfiguration.SECMEC_EUSRIDPWD,
                            netAgent_.netConnection_.getTargetPublicKey(),
                            netAgent_.netConnection_.getTargetPublicKey());

            int encryptedBytesLength = encryptedBytes.length;
            int sendingLength = bytes_.length - offset_;
            if (encryptedBytesLength > (bytes_.length - offset_)) {

                System.arraycopy(encryptedBytes, 0, bytes_, offset_, (bytes_.length - offset_));
                offset_ = 32767;
                try {
                    sendBytes(netAgent_.getOutputStream());
                } catch (java.io.IOException ioe) {
                    netAgent_.throwCommunicationsFailure("Request.writeScalarStream(...,InputStream)",
                            "OutputStream.flush()",
                            ioe.getMessage(),
                            "*");
                }
            } else {
                System.arraycopy(encryptedBytes, 0, bytes_, offset_, encryptedBytesLength);
                offset_ = offset_ + encryptedBytes.length;
            }

            encryptedBytesLength = encryptedBytesLength - sendingLength;
            while (encryptedBytesLength > 0) {
                //dssLengthLocation_ = offset_;
                offset_ = 0;

                if ((encryptedBytesLength - 32765) > 0) {
                    bytes_[offset_++] = (byte) (0xff);
                    bytes_[offset_++] = (byte) (0xff);
                    System.arraycopy(encryptedBytes, sendingLength, bytes_, offset_, 32765);
                    encryptedBytesLength -= 32765;
                    sendingLength += 32765;
                    offset_ = 32767;
                    try {
                        sendBytes(netAgent_.getOutputStream());
                    } catch (java.io.IOException ioe) {
                        netAgent_.throwCommunicationsFailure("Request.writeScalarStream(...,InputStream)",
                                "OutputStream.flush()",
                                ioe.getMessage(),
                                "*");
                    }
                } else {
                    int leftlength = encryptedBytesLength + 2;
                    bytes_[offset_++] = (byte) ((leftlength >>> 8) & 0xff);
                    bytes_[offset_++] = (byte) (leftlength & 0xff);

                    System.arraycopy(encryptedBytes, sendingLength, bytes_, offset_, encryptedBytesLength);

                    offset_ += encryptedBytesLength;
                    dssLengthLocation_ = offset_;
                    encryptedBytesLength = 0;
                }

            }
        } else //if not data strteam encryption
        {
            buildLengthAndCodePointForLob(codePoint,
                    leftToRead,
                    writeNullByte,
                    extendedLengthByteCount);

            int bytesRead = 0;
            int totalBytesRead = 0;
            do {
                do {
                    try {
                        bytesRead = in.read(bytes_, offset_, bytesToRead);
                        totalBytesRead += bytesRead;
                    } catch (java.io.IOException e) {
                        padScalarStreamForError(leftToRead, bytesToRead);
                        // set with SQLSTATE 01004: The value of a string was truncated when assigned to a host variable.
                        netAgent_.accumulateReadException(new SqlException(netAgent_.logWriter_,
                                "Encountered an IOException reading InputStream, parameter #" + parameterIndex +
                                ".  Remaining data has been padded with 0x0. Message: " + e.getMessage()));
                        return;
                    }
                    if (bytesRead == -1) {
                        padScalarStreamForError(leftToRead, bytesToRead);
                        // set with SQLSTATE 01004: The value of a string was truncated when assigned to a host variable.
                        netAgent_.accumulateReadException(new SqlException(netAgent_.logWriter_,
                                "End of Stream prematurely reached while reading InputStream, parameter #" + parameterIndex +
                                ".  Remaining data has been padded with 0x0."));
                        return;
                    } else {
                        bytesToRead -= bytesRead;
                        offset_ += bytesRead;
                        leftToRead -= bytesRead;
                    }
                } while (bytesToRead > 0);

                bytesToRead = flushScalarStreamSegment(leftToRead, bytesToRead);
            } while (leftToRead > 0);

            // check to make sure that the specified length wasn't too small
            try {
                if (in.read() != -1) {
                    // set with SQLSTATE 01004: The value of a string was truncated when assigned to a host variable.
                    netAgent_.accumulateReadException(new SqlException(netAgent_.logWriter_,
                            "The specified size of the InputStream, parameter #" + parameterIndex +
                            ", is less than the actual InputStream length"));
                }
            } catch (java.io.IOException e) {
                netAgent_.accumulateReadException(new SqlException(netAgent_.logWriter_,
                        "Encountered error in stream length verification for InputStream, parameter #" + parameterIndex +
                        ".  Message: " + e.getMessage()));
            }

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
                                 int parameterIndex) throws DisconnectException {
        int leftToRead = length * 2; // the bytes to read
        int extendedLengthByteCount = prepScalarStream(chained,
                chainedWithSameCorrelator,
                writeNullByte,
                leftToRead);
        int bytesToRead;

        if (writeNullByte) {
            bytesToRead = Utils.min(leftToRead, DssConstants.MAX_DSS_LEN - 6 - 4 - 1 - extendedLengthByteCount);
        } else {
            bytesToRead = Utils.min(leftToRead, DssConstants.MAX_DSS_LEN - 6 - 4 - extendedLengthByteCount);
        }


        if (netAgent_.netConnection_.getSecurityMechanism() != NetConfiguration.SECMEC_EUSRIDDTA &&
                netAgent_.netConnection_.getSecurityMechanism() != NetConfiguration.SECMEC_EUSRPWDDTA) {
            buildLengthAndCodePointForLob(codePoint,
                    leftToRead,
                    writeNullByte,
                    extendedLengthByteCount);


            // write the data
            int charsRead = 0;
            boolean haveHalfChar = false;
            byte halfChar = (byte) 0x0;
            char[] buf = new char[1 + 32765 / 2]; // enough for one DSS segment

            do {
                do {
                    // fill in a half-character if we have one from a previous segment
                    if (haveHalfChar) {
                        bytes_[offset_++] = halfChar;
                        bytesToRead--;
                        leftToRead--;
                        haveHalfChar = false;
                    }

                    if (bytesToRead == 1) {
                        try {
                            charsRead = r.read(buf, 0, 1);
                        } catch (java.io.IOException e) {
                            padScalarStreamForError(leftToRead, bytesToRead);
                            // set with SQLSTATE 01004: The value of a string was truncated when assigned to a host variable.
                            netAgent_.accumulateReadException(new SqlException(netAgent_.logWriter_,
                                    "Encountered an IOException reading Reader, parameter #" +
                                    parameterIndex +
                                    ".  Remaining data has been padded with 0x0. Message: " +
                                    e.getMessage()));
                            return;
                        }
                        if (charsRead == -1) {
                            padScalarStreamForError(leftToRead, bytesToRead);
                            // set with SQLSTATE 01004: The value of a string was truncated when assigned to a host variable.
                            netAgent_.accumulateReadException(new SqlException(netAgent_.logWriter_,
                                    "End of Stream prematurely reached while reading Reader, parameter #" +
                                    parameterIndex +
                                    ".  Remaining data has been padded with 0x0."));
                            return;
                        }
                        // set first half-char in buffer and save the other half for later
                        bytes_[offset_++] = (byte) (buf[0] >>> 8);
                        halfChar = (byte) buf[0];
                        haveHalfChar = true;
                        bytesToRead--;
                        leftToRead--;
                    } else if (bytesToRead != 0) {
                        try {
                            // read as many whole characters as needed to fill the buffer
                            // half characters are handled above
                            charsRead = r.read(buf, 0, bytesToRead / 2);
                        } catch (java.io.IOException e) {
                            padScalarStreamForError(leftToRead, bytesToRead);
                            // set with SQLSTATE 01004: The value of a string was truncated when assigned to a host variable.
                            netAgent_.accumulateReadException(new SqlException(netAgent_.logWriter_, e,
                                    "Encountered an IOException reading Reader, parameter #" +
                                    parameterIndex +
                                    ".  Remaining data has been padded with 0x0. Message: " +
                                    e.getMessage()));
                            return;
                        }

                        if (charsRead == -1) {
                            padScalarStreamForError(leftToRead, bytesToRead);
                            // set with SQLSTATE 01004: The value of a string was truncated when assigned to a host variable.
                            netAgent_.accumulateReadException(new SqlException(netAgent_.logWriter_,
                                    "End of Stream prematurely reached while reading Reader, parameter #" +
                                    parameterIndex +
                                    ".  Remaining data has been padded with 0x0."));
                            return;
                        }
                        for (int i = 0; i < charsRead; i++) {
                            bytes_[offset_++] = (byte) (buf[i] >>> 8);
                            bytes_[offset_++] = (byte) (buf[i]);
                        }

                        bytesToRead -= 2 * charsRead;
                        leftToRead -= 2 * charsRead;
                    }
                } while (bytesToRead > 0);

                bytesToRead = flushScalarStreamSegment(leftToRead, bytesToRead);
            } while (leftToRead > 0);

            // check to make sure that the specified length wasn't too small
            try {
                if (r.read() != -1) {
                    netAgent_.accumulateReadException(new SqlException(netAgent_.logWriter_,
                            "The specified size of the Reader, parameter #" +
                            parameterIndex +
                            ", is less than the actual InputStream length"));
                }
            } catch (java.io.IOException e) {
                netAgent_.accumulateReadException(new SqlException(netAgent_.logWriter_, e,
                        "Encountered error in stream length verification for Reader, parameter #" +
                        parameterIndex + ".  Message: " + e.getMessage()));
            }
        } else {  //data stream encryption

            byte[] lengthAndCodepoint;
            lengthAndCodepoint = buildLengthAndCodePointForEncryptedLob(codePoint,
                    leftToRead,
                    writeNullByte,
                    extendedLengthByteCount);

            // write the data
            int charsRead = 0;
            char[] buf = new char[leftToRead / 2];
            byte[] clearedBytes = new byte[leftToRead];
            int pos = 0;


            do {
                // fill in a half-character if we have one from a previous segment

                try {
                    // read as many whole characters as needed to fill the buffer
                    // half characters are handled above
                    charsRead = r.read(buf, 0, leftToRead / 2);
                } catch (java.io.IOException e) {
                    padScalarStreamForError(leftToRead, bytesToRead);
                    // set with SQLSTATE 01004: The value of a string was truncated when assigned to a host variable.
                    netAgent_.accumulateReadException(new SqlException(netAgent_.logWriter_, e,
                            "Encountered an IOException reading Reader, parameter #" +
                            parameterIndex +
                            ".  Remaining data has been padded with 0x0. Message: " +
                            e.getMessage()));
                    return;
                }

                if (charsRead == -1) {
                    padScalarStreamForError(leftToRead, bytesToRead);
                    // set with SQLSTATE 01004: The value of a string was truncated when assigned to a host variable.
                    netAgent_.accumulateReadException(new SqlException(netAgent_.logWriter_,
                            "End of Stream prematurely reached while reading Reader, parameter #" +
                            parameterIndex +
                            ".  Remaining data has been padded with 0x0."));
                    return;
                }
                for (int i = 0; i < charsRead; i++) {
                    clearedBytes[pos++] = (byte) (buf[i] >>> 8);
                    clearedBytes[pos++] = (byte) (buf[i]);
                }

                bytesToRead -= 2 * charsRead;
                leftToRead -= 2 * charsRead;
            } while (leftToRead > 0);

            // check to make sure that the specified length wasn't too small
            try {
                if (r.read() != -1) {
                    netAgent_.accumulateReadException(new SqlException(netAgent_.logWriter_,
                            "The specified size of the Reader, parameter #" +
                            parameterIndex +
                            ", is less than the actual InputStream length"));
                }
            } catch (java.io.IOException e) {
                netAgent_.accumulateReadException(new SqlException(netAgent_.logWriter_, e,
                        "Encountered error in stream length verification for Reader, parameter #" +
                        parameterIndex + ".  Message: " + e.getMessage()));
            }

            byte[] newClearedBytes = new byte[clearedBytes.length +
                    lengthAndCodepoint.length];
            System.arraycopy(lengthAndCodepoint, 0, newClearedBytes, 0,
                    lengthAndCodepoint.length);
            System.arraycopy(clearedBytes, 0, newClearedBytes, lengthAndCodepoint.length, clearedBytes.length);
            int encryptedBytesLength = 0;
            byte[] encryptedBytes = null;
            try {
                EncryptionManager encryptionMgr = netAgent_.netConnection_.getEncryptionManager();
                byte[] publicKey = netAgent_.netConnection_.getTargetPublicKey();
                encryptedBytes = encryptionMgr.encryptData(newClearedBytes,
                        NetConfiguration.SECMEC_EUSRIDPWD,
                        publicKey,
                        publicKey);
                encryptedBytesLength = encryptedBytes.length;
            } catch (Exception e) {
                e.printStackTrace();
            }


            int sendingLength = bytes_.length - offset_;
            if (encryptedBytesLength > bytes_.length - offset_) {


                System.arraycopy(encryptedBytes, 0, bytes_, offset_, (bytes_.length - offset_));
                offset_ = 32767;
                try {
                    sendBytes(netAgent_.getOutputStream());
                } catch (java.io.IOException ioe) {
                    netAgent_.throwCommunicationsFailure("Request.writeScalarStream(...,InputStream)",
                            "OutputStream.flush()",
                            ioe.getMessage(),
                            "*");
                }
            } else {
                System.arraycopy(encryptedBytes, 0, bytes_, offset_, encryptedBytesLength);
                offset_ = offset_ + encryptedBytes.length;
            }

            encryptedBytesLength = encryptedBytesLength - sendingLength;
            while (encryptedBytesLength > 0) {
                offset_ = 0;

                if ((encryptedBytesLength - 32765) > 0) {
                    bytes_[offset_++] = (byte) (0xff);
                    bytes_[offset_++] = (byte) (0xff);
                    System.arraycopy(encryptedBytes, sendingLength, bytes_, offset_, 32765);
                    encryptedBytesLength -= 32765;
                    sendingLength += 32765;
                    offset_ = 32767;
                    try {
                        sendBytes(netAgent_.getOutputStream());
                    } catch (java.io.IOException ioe) {
                        netAgent_.throwCommunicationsFailure("Request.writeScalarStream(...,InputStream)",
                                "OutputStream.flush()",
                                ioe.getMessage(),
                                "*");
                    }
                } else {
                    int leftlength = encryptedBytesLength + 2;
                    bytes_[offset_++] = (byte) ((leftlength >>> 8) & 0xff);
                    bytes_[offset_++] = (byte) (leftlength & 0xff);

                    System.arraycopy(encryptedBytes, sendingLength, bytes_, offset_, encryptedBytesLength);

                    offset_ += encryptedBytesLength;
                    dssLengthLocation_ = offset_;
                    encryptedBytesLength = 0;
                }

            }


        }
    }


    // prepScalarStream does the following prep for writing stream data:
    // 1.  Flushes an existing DSS segment, if necessary
    // 2.  Determines if extended length bytes are needed
    // 3.  Creates a new DSS/DDM header and a null byte indicator, if applicable
    protected final int prepScalarStream(boolean chained,
                                         boolean chainedWithSameCorrelator,
                                         boolean writeNullByte,
                                         int leftToRead) throws DisconnectException {
        int extendedLengthByteCount;

        int nullIndicatorSize = 0;
        if (writeNullByte) {
            // leftToRead is cast to (long) on the off chance that +4+1 pushes it outside the range of int
            extendedLengthByteCount = calculateExtendedLengthByteCount((long) leftToRead + 4 + 1);
            nullIndicatorSize = 1;
        } else {
            extendedLengthByteCount = calculateExtendedLengthByteCount(leftToRead + 4);
        }

        // flush the existing DSS segment if this stream will not fit in the send buffer
        // leftToRead is cast to (long) on the off chance that +4+1 pushes it outside the range of int
        if (10 + extendedLengthByteCount + nullIndicatorSize + (long) leftToRead + offset_ > DssConstants.MAX_DSS_LEN) {
            try {
                if (simpleDssFinalize) {
                    finalizeDssLength();
                } else {
                    finalizePreviousChainedDss(true);
                }
                sendBytes(netAgent_.getOutputStream());
            } catch (java.io.IOException e) {
                netAgent_.throwCommunicationsFailure("Request.writeScalarStream(...,InputStream)",
                        "OutputStream.flush()",
                        e.getMessage(),
                        "*");
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


    // Writes out a scalar stream DSS segment, along with DSS continuation headers,
    // if necessary.
    protected final int flushScalarStreamSegment(int leftToRead,
                                                 int bytesToRead) throws DisconnectException {
        int newBytesToRead = bytesToRead;

        // either at end of data, end of dss segment, or both.
        if (leftToRead != 0) {
            // 32k segment filled and not at end of data.
            if ((Utils.min(2 + leftToRead, 32767)) > (bytes_.length - offset_)) {
                try {
                    sendBytes(netAgent_.getOutputStream());
                } catch (java.io.IOException ioe) {
                    netAgent_.throwCommunicationsFailure("Request.writeScalarStream(...,InputStream)",
                            "OutputStream.flush()",
                            ioe.getMessage(),
                            "*");
                }
            }
            dssLengthLocation_ = offset_;
            bytes_[offset_++] = (byte) (0xff);
            bytes_[offset_++] = (byte) (0xff);
            newBytesToRead = Utils.min(leftToRead, 32765);
        }

        return newBytesToRead;
    }

    // the offset_ must not be updated when an error is encountered
    // note valid data may be overwritten
    protected final void padScalarStreamForError(int leftToRead, int bytesToRead) throws DisconnectException {
        do {
            do {
                bytes_[offset_++] = (byte) (0x0); // use 0x0 as the padding byte
                bytesToRead--;
                leftToRead--;
            } while (bytesToRead > 0);

            bytesToRead = flushScalarStreamSegment(leftToRead, bytesToRead);
        } while (leftToRead > 0);
    }

    private final void writeExtendedLengthBytes(int extendedLengthByteCount, long length) {
        int shiftSize = (extendedLengthByteCount - 1) * 8;
        for (int i = 0; i < extendedLengthByteCount; i++) {
            bytes_[offset_++] = (byte) ((length >>> shiftSize) & 0xff);
            shiftSize -= 8;
        }
    }

    private final byte[] writeExtendedLengthBytesForEncryption(int extendedLengthByteCount, long length) {
        int shiftSize = (extendedLengthByteCount - 1) * 8;
        byte[] extendedLengthBytes = new byte[extendedLengthByteCount];
        for (int i = 0; i < extendedLengthByteCount; i++) {
            extendedLengthBytes[i] = (byte) ((length >>> shiftSize) & 0xff);
            shiftSize -= 8;
        }
        return extendedLengthBytes;
    }

    // experimental lob section - end

    // used to finialize a dss which is already in the buffer
    // before another dss is built.  this includes updating length
    // bytes and chaining bits.
    protected final void finalizePreviousChainedDss(boolean dssHasSameCorrelator) {
        finalizeDssLength();
        bytes_[dssLengthLocation_ + 3] |= 0x40;
        if (dssHasSameCorrelator) // for blobs
        {
            bytes_[dssLengthLocation_ + 3] |= 0x10;
        }
    }

    // method to determine if any data is in the request.
    // this indicates there is a dss object already in the buffer.
    protected final boolean doesRequestContainData() {
        return offset_ != 0;
    }

    // signal the completion of a Dss Layer A object. The length of
    // dss object will be calculated based on the difference between the
    // start of the dss, saved on the beginDss call, and the current
    // offset into the buffer which marks the end of the data.  In the event
    // the length requires the use of continuation Dss headers, one for each 32k
    // chunk of data, the data will be shifted and the continuation headers
    // will be inserted with the correct values as needed.
    // Note: In the future, we may try to optimize this approach
    // in an attempt to avoid these shifts.
    protected final void finalizeDssLength() {
        // calculate the total size of the dss and the number of bytes which would
        // require continuation dss headers.  The total length already includes the
        // the 6 byte dss header located at the beginning of the dss.  It does not
        // include the length of any continuation headers.
        int totalSize = offset_ - dssLengthLocation_;
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
            int dataByte = offset_ - 1;
            int shiftOffset = contDssHeaderCount * 2;
            ensureLength(offset_ + shiftOffset);
            offset_ += shiftOffset;

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
                for (int i = 0; i < dataToShift; i++) {
                    bytes_[dataByte + shiftOffset] = bytes_[dataByte];
                    dataByte--;
                }

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
                bytes_[dataByte + shiftOffset - 1] = (byte) ((twoByteContDssHeader >>> 8) & 0xff);
                bytes_[dataByte + shiftOffset] = (byte) (twoByteContDssHeader & 0xff);

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
        bytes_[dssLengthLocation_] = (byte) ((totalSize >>> 8) & 0xff);
        bytes_[dssLengthLocation_ + 1] = (byte) (totalSize & 0xff);
    }

    // mark the location of a two byte ddm length field in the buffer,
    // skip the length bytes for later update, and insert a ddm codepoint
    // into the buffer.  The value of the codepoint is not checked.
    // this length will be automatically updated when construction of
    // the ddm object is complete (see updateLengthBytes method).
    // Note: this mechanism handles extended length ddms.
    protected final void markLengthBytes(int codePoint) {
        ensureLength(offset_ + 4);

        // save the location of length bytes in the mark stack.
        mark();

        // skip the length bytes and insert the codepoint
        offset_ += 2;
        bytes_[offset_++] = (byte) ((codePoint >>> 8) & 0xff);
        bytes_[offset_++] = (byte) (codePoint & 0xff);
    }

    // mark an offest into the buffer by placing the current offset value on
    // a stack.
    private final void mark() {
        markStack_[top_++] = offset_;
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
        int length = offset_ - lengthLocation;

        // determine if any extended length bytes are needed.  the value returned
        // from calculateExtendedLengthByteCount is the number of extended length
        // bytes required. 0 indicates no exteneded length.
        int extendedLengthByteCount = calculateExtendedLengthByteCount(length);
        if (extendedLengthByteCount != 0) {

            // ensure there is enough room in the buffer for the extended length bytes.
            ensureLength(offset_ + extendedLengthByteCount);

            // calculate the length to be placed in the extended length bytes.
            // this length does not include the 4 byte llcp.
            int extendedLength = length - 4;

            // shift the data to the right by the number of extended length bytes needed.
            int extendedLengthLocation = lengthLocation + 4;
            System.arraycopy(bytes_,
                    extendedLengthLocation,
                    bytes_,
                    extendedLengthLocation + extendedLengthByteCount,
                    extendedLength);

            // write the extended length
            int shiftSize = (extendedLengthByteCount - 1) * 8;
            for (int i = 0; i < extendedLengthByteCount; i++) {
                bytes_[extendedLengthLocation++] = (byte) ((extendedLength >>> shiftSize) & 0xff);
                shiftSize -= 8;
            }
            // adjust the offset to account for the shift and insert
            offset_ += extendedLengthByteCount;

            // the two byte length field before the codepoint contains the length
            // of itself, the length of the codepoint, and the number of bytes used
            // to hold the extended length.  the 2 byte length field also has the first
            // bit on to indicate extended length bytes were used.
            length = extendedLengthByteCount + 4;
            length |= 0x8000;
        }

        // write the 2 byte length field (2 bytes before codepoint).
        bytes_[lengthLocation] = (byte) ((length >>> 8) & 0xff);
        bytes_[lengthLocation + 1] = (byte) (length & 0xff);
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
        ensureLength(offset_ + length);
        for (int i = 0; i < length; i++) {
            bytes_[offset_++] = padByte;
        }
    }

    // insert an unsigned single byte value into the buffer.
    final void write1Byte(int value) {
        ensureLength(offset_ + 1);
        bytes_[offset_++] = (byte) (value & 0xff);
    }

    // insert 3 unsigned bytes into the buffer.  this was
    // moved up from NetStatementRequest for performance
    final void buildTripletHeader(int tripletLength,
                                  int tripletType,
                                  int tripletId) {
        ensureLength(offset_ + 3);
        bytes_[offset_++] = (byte) (tripletLength & 0xff);
        bytes_[offset_++] = (byte) (tripletType & 0xff);
        bytes_[offset_++] = (byte) (tripletId & 0xff);
    }

    final void writeLidAndLengths(int[][] lidAndLengthOverrides, int count, int offset) {
        ensureLength(offset_ + (count * 3));
        for (int i = 0; i < count; i++, offset++) {
            bytes_[offset_++] = (byte) (lidAndLengthOverrides[offset][0] & 0xff);
            bytes_[offset_++] = (byte) ((lidAndLengthOverrides[offset][1] >>> 8) & 0xff);
            bytes_[offset_++] = (byte) (lidAndLengthOverrides[offset][1] & 0xff);
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
            ensureLength(offset_ + (count * 3));
            int protocolType, overrideLid;
            Object entry;
            for (int i = 0; i < count; i++, offset++) {
                protocolType = lidAndLengthOverrides[offset][0];
                // lookup the protocolType in the protocolType->overrideLid map
                // if an entry exists, replace the protocolType with the overrideLid
                entry = map.get(new Integer(protocolType));
                overrideLid = (entry == null) ? protocolType : ((Integer) entry).intValue();
                bytes_[offset_++] = (byte) (overrideLid & 0xff);
                bytes_[offset_++] = (byte) ((lidAndLengthOverrides[offset][1] >>> 8) & 0xff);
                bytes_[offset_++] = (byte) (lidAndLengthOverrides[offset][1] & 0xff);
            }
        }
    }

// perf end

    // insert a big endian unsigned 2 byte value into the buffer.
    final void write2Bytes(int value) {
        ensureLength(offset_ + 2);
        bytes_[offset_++] = (byte) ((value >>> 8) & 0xff);
        bytes_[offset_++] = (byte) (value & 0xff);
    }

    // insert a big endian unsigned 4 byte value into the buffer.
    final void write4Bytes(long value) {
        ensureLength(offset_ + 4);
        bytes_[offset_++] = (byte) ((value >>> 24) & 0xff);
        bytes_[offset_++] = (byte) ((value >>> 16) & 0xff);
        bytes_[offset_++] = (byte) ((value >>> 8) & 0xff);
        bytes_[offset_++] = (byte) (value & 0xff);
    }

    // copy length number of bytes starting at offset 0 of the byte array, buf,
    // into the buffer.  it is up to the caller to make sure buf has at least length
    // number of elements.  no checking will be done by this method.
    final void writeBytes(byte[] buf, int length) {
        ensureLength(offset_ + length);
        System.arraycopy(buf, 0, bytes_, offset_, length);
        offset_ += length;
    }

    final void writeBytes(byte[] buf) {
        ensureLength(offset_ + buf.length);
        System.arraycopy(buf, 0, bytes_, offset_, buf.length);
        offset_ += buf.length;
    }

    // insert a pair of unsigned 2 byte values into the buffer.
    final void writeCodePoint4Bytes(int codePoint, int value) {                                                      // should this be writeCodePoint2Bytes
        ensureLength(offset_ + 4);
        bytes_[offset_++] = (byte) ((codePoint >>> 8) & 0xff);
        bytes_[offset_++] = (byte) (codePoint & 0xff);
        bytes_[offset_++] = (byte) ((value >>> 8) & 0xff);
        bytes_[offset_++] = (byte) (value & 0xff);
    }

    // insert a 4 byte length/codepoint pair and a 1 byte unsigned value into the buffer.
    // total of 5 bytes inserted in buffer.
    protected final void writeScalar1Byte(int codePoint, int value) {
        ensureLength(offset_ + 5);
        bytes_[offset_++] = 0x00;
        bytes_[offset_++] = 0x05;
        bytes_[offset_++] = (byte) ((codePoint >>> 8) & 0xff);
        bytes_[offset_++] = (byte) (codePoint & 0xff);
        bytes_[offset_++] = (byte) (value & 0xff);
    }

    // insert a 4 byte length/codepoint pair and a 2 byte unsigned value into the buffer.
    // total of 6 bytes inserted in buffer.
    final void writeScalar2Bytes(int codePoint, int value) {
        ensureLength(offset_ + 6);
        bytes_[offset_++] = 0x00;
        bytes_[offset_++] = 0x06;
        bytes_[offset_++] = (byte) ((codePoint >>> 8) & 0xff);
        bytes_[offset_++] = (byte) (codePoint & 0xff);
        bytes_[offset_++] = (byte) ((value >>> 8) & 0xff);
        bytes_[offset_++] = (byte) (value & 0xff);
    }

    // insert a 4 byte length/codepoint pair and a 4 byte unsigned value into the
    // buffer.  total of 8 bytes inserted in the buffer.
    protected final void writeScalar4Bytes(int codePoint, long value) {
        ensureLength(offset_ + 8);
        bytes_[offset_++] = 0x00;
        bytes_[offset_++] = 0x08;
        bytes_[offset_++] = (byte) ((codePoint >>> 8) & 0xff);
        bytes_[offset_++] = (byte) (codePoint & 0xff);
        bytes_[offset_++] = (byte) ((value >>> 24) & 0xff);
        bytes_[offset_++] = (byte) ((value >>> 16) & 0xff);
        bytes_[offset_++] = (byte) ((value >>> 8) & 0xff);
        bytes_[offset_++] = (byte) (value & 0xff);
    }

    // insert a 4 byte length/codepoint pair and a 8 byte unsigned value into the
    // buffer.  total of 12 bytes inserted in the buffer.
    final void writeScalar8Bytes(int codePoint, long value) {
        ensureLength(offset_ + 12);
        bytes_[offset_++] = 0x00;
        bytes_[offset_++] = 0x0C;
        bytes_[offset_++] = (byte) ((codePoint >>> 8) & 0xff);
        bytes_[offset_++] = (byte) (codePoint & 0xff);
        bytes_[offset_++] = (byte) ((value >>> 56) & 0xff);
        bytes_[offset_++] = (byte) ((value >>> 48) & 0xff);
        bytes_[offset_++] = (byte) ((value >>> 40) & 0xff);
        bytes_[offset_++] = (byte) ((value >>> 32) & 0xff);
        bytes_[offset_++] = (byte) ((value >>> 24) & 0xff);
        bytes_[offset_++] = (byte) ((value >>> 16) & 0xff);
        bytes_[offset_++] = (byte) ((value >>> 8) & 0xff);
        bytes_[offset_++] = (byte) (value & 0xff);
    }

    // insert a 4 byte length/codepoint pair into the buffer.
    // total of 4 bytes inserted in buffer.
    // Note: the length value inserted in the buffer is the same as the value
    // passed in as an argument (this value is NOT incremented by 4 before being
    // inserted).
    final void writeLengthCodePoint(int length, int codePoint) {
        ensureLength(offset_ + 4);
        bytes_[offset_++] = (byte) ((length >>> 8) & 0xff);
        bytes_[offset_++] = (byte) (length & 0xff);
        bytes_[offset_++] = (byte) ((codePoint >>> 8) & 0xff);
        bytes_[offset_++] = (byte) (codePoint & 0xff);
    }

    final byte[] writeEXTDTALengthCodePointForEncryption(int length, int codePoint) {
        //how to encure length and offset later?
        byte[] clearedBytes = new byte[4];
        clearedBytes[0] = (byte) ((length >>> 8) & 0xff);
        clearedBytes[1] = (byte) (length & 0xff);
        clearedBytes[2] = (byte) ((codePoint >>> 8) & 0xff);
        clearedBytes[3] = (byte) (codePoint & 0xff);
        return clearedBytes;
    }

    // insert a 4 byte length/codepoint pair into the buffer followed
    // by length number of bytes copied from array buf starting at offset 0.
    // the length of this scalar must not exceed the max for the two byte length
    // field.  This method does not support extended length.  The length
    // value inserted in the buffer includes the number of bytes to copy plus
    // the size of the llcp (or length + 4). It is up to the caller to make sure
    // the array, buf, contains at least length number of bytes.
    final void writeScalarBytes(int codePoint, byte[] buf, int length) {
        ensureLength(offset_ + length + 4);
        bytes_[offset_++] = (byte) (((length + 4) >>> 8) & 0xff);
        bytes_[offset_++] = (byte) ((length + 4) & 0xff);
        bytes_[offset_++] = (byte) ((codePoint >>> 8) & 0xff);
        bytes_[offset_++] = (byte) (codePoint & 0xff);
        for (int i = 0; i < length; i++) {
            bytes_[offset_++] = buf[i];
        }
    }

    // insert a 4 byte length/codepoint pair into the buffer.
    // total of 4 bytes inserted in buffer.
    // Note: datalength will be incremented by the size of the llcp, 4,
    // before being inserted.
    final void writeScalarHeader(int codePoint, int dataLength) {
        ensureLength(offset_ + dataLength + 4);
        bytes_[offset_++] = (byte) (((dataLength + 4) >>> 8) & 0xff);
        bytes_[offset_++] = (byte) ((dataLength + 4) & 0xff);
        bytes_[offset_++] = (byte) ((codePoint >>> 8) & 0xff);
        bytes_[offset_++] = (byte) (codePoint & 0xff);
    }

    // insert a 4 byte length/codepoint pair plus ddm character data into
    // the buffer.  This method assumes that the String argument can be
    // converted by the ccsid manager.  This should be fine because usually
    // there are restrictions on the characters which can be used for ddm
    // character data. This method also assumes that the string.length() will
    // be the number of bytes following the conversion.
    // The two byte length field will contain the length of the character data
    // and the length of the 4 byte llcp.  This method does not handle
    // scenarios which require extended length bytes.
    final void writeScalarString(int codePoint, String string) throws SqlException {
        int stringLength = string.length();
        ensureLength(offset_ + stringLength + 4);
        bytes_[offset_++] = (byte) (((stringLength + 4) >>> 8) & 0xff);
        bytes_[offset_++] = (byte) ((stringLength + 4) & 0xff);
        bytes_[offset_++] = (byte) ((codePoint >>> 8) & 0xff);
        bytes_[offset_++] = (byte) (codePoint & 0xff);
        offset_ = ccsidManager_.convertFromUCS2(string, bytes_, offset_, netAgent_);
    }

    // insert a 4 byte length/codepoint pair plus ddm character data into the
    // buffer.  The ddm character data is padded if needed with the ccsid manager's
    // space character if the length of the character data is less than paddedLength.
    // Note: this method is not to be used for String truncation and the string length
    // must be <= paddedLength.
    // This method assumes that the String argument can be
    // converted by the ccsid manager.  This should be fine because usually
    // there are restrictions on the characters which can be used for ddm
    // character data. This method also assumes that the string.length() will
    // be the number of bytes following the conversion.  The two byte length field
    // of the llcp will contain the length of the character data including the pad
    // and the length of the llcp or 4.  This method will not handle extended length
    // scenarios.
    final void writeScalarPaddedString(int codePoint, String string, int paddedLength) throws SqlException {
        int stringLength = string.length();
        ensureLength(offset_ + paddedLength + 4);
        bytes_[offset_++] = (byte) (((paddedLength + 4) >>> 8) & 0xff);
        bytes_[offset_++] = (byte) ((paddedLength + 4) & 0xff);
        bytes_[offset_++] = (byte) ((codePoint >>> 8) & 0xff);
        bytes_[offset_++] = (byte) (codePoint & 0xff);
        offset_ = ccsidManager_.convertFromUCS2(string, bytes_, offset_, netAgent_);
        for (int i = 0; i < paddedLength - stringLength; i++) {
            bytes_[offset_++] = ccsidManager_.space_;
        }
    }

    // this method inserts ddm character data into the buffer and pad's the
    // data with the ccsid manager's space character if the character data length
    // is less than paddedLength.
    // Not: this method is not to be used for String truncation and the string length
    // must be <= paddedLength.
    // This method assumes that the String argument can be
    // converted by the ccsid manager.  This should be fine because usually
    // there are restrictions on the characters which can be used for ddm
    // character data. This method also assumes that the string.length() will
    // be the number of bytes following the conversion.
    final void writeScalarPaddedString(String string, int paddedLength) throws SqlException {
        int stringLength = string.length();
        ensureLength(offset_ + paddedLength);
        offset_ = ccsidManager_.convertFromUCS2(string, bytes_, offset_, netAgent_);
        for (int i = 0; i < paddedLength - stringLength; i++) {
            bytes_[offset_++] = ccsidManager_.space_;
        }
    }

    // this method writes a 4 byte length/codepoint pair plus the bytes contained
    // in array buff to the buffer.
    // the 2 length bytes in the llcp will contain the length of the data plus
    // the length of the llcp.  This method does not handle scenarios which
    // require extended length bytes.
    final void writeScalarBytes(int codePoint, byte[] buff) {
        int buffLength = buff.length;
        ensureLength(offset_ + buffLength + 4);
        bytes_[offset_++] = (byte) (((buffLength + 4) >>> 8) & 0xff);
        bytes_[offset_++] = (byte) ((buffLength + 4) & 0xff);
        bytes_[offset_++] = (byte) ((codePoint >>> 8) & 0xff);
        bytes_[offset_++] = (byte) (codePoint & 0xff);
        System.arraycopy(buff, 0, bytes_, offset_, buffLength);
        offset_ += buffLength;
    }

    // this method inserts a 4 byte length/codepoint pair plus length number of bytes
    // from array buff starting at offset start.
    // Note: no checking will be done on the values of start and length with respect
    // the actual length of the byte array.  The caller must provide the correct
    // values so an array index out of bounds exception does not occur.
    // the length will contain the length of the data plus the length of the llcp.
    // This method does not handle scenarios which require extended length bytes.
    final void writeScalarBytes(int codePoint, byte[] buff, int start, int length) {
        ensureLength(offset_ + length + 4);
        bytes_[offset_++] = (byte) (((length + 4) >>> 8) & 0xff);
        bytes_[offset_++] = (byte) ((length + 4) & 0xff);
        bytes_[offset_++] = (byte) ((codePoint >>> 8) & 0xff);
        bytes_[offset_++] = (byte) (codePoint & 0xff);
        System.arraycopy(buff, start, bytes_, offset_, length);
        offset_ += length;
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
        int buffLength = buff.length;
        ensureLength(offset_ + paddedLength + 4);
        bytes_[offset_++] = (byte) (((paddedLength + 4) >>> 8) & 0xff);
        bytes_[offset_++] = (byte) ((paddedLength + 4) & 0xff);
        bytes_[offset_++] = (byte) ((codePoint >>> 8) & 0xff);
        bytes_[offset_++] = (byte) (codePoint & 0xff);
        System.arraycopy(buff, 0, bytes_, offset_, buffLength);
        offset_ += buffLength;

        for (int i = 0; i < paddedLength - buffLength; i++) {
            bytes_[offset_++] = padByte;
        }
    }

    // this method inserts binary data into the buffer and pads the
    // data with the padByte if the data length is less than the paddedLength.
    // Not: this method is not to be used for truncation and buff.length
    // must be <= paddedLength.
    final void writeScalarPaddedBytes(byte[] buff, int paddedLength, byte padByte) {
        int buffLength = buff.length;
        ensureLength(offset_ + paddedLength);
        System.arraycopy(buff, 0, bytes_, offset_, buffLength);
        offset_ += buffLength;

        for (int i = 0; i < paddedLength - buffLength; i++) {
            bytes_[offset_++] = padByte;
        }
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
            socketOutputStream.write(bytes_, 0, offset_);
            socketOutputStream.flush();
        } finally {
            if (netAgent_.logWriter_ != null && passwordIncluded_) {
                // if password is in the buffer, need to mask it out.
                maskOutPassword();
                passwordIncluded_ = false;
            }
            if (netAgent_.loggingEnabled()) {
                ((NetLogWriter) netAgent_.logWriter_).traceProtocolFlow(bytes_,
                        0,
                        offset_,
                        NetLogWriter.TYPE_TRACE_SEND,
                        "Request",
                        "flush",
                        1); // tracepoint
            }
            clearBuffer();
        }
    }

    final void maskOutPassword() {
        try {
            String maskChar = "*";
            // construct a mask using the maskChar.
            StringBuffer mask = new StringBuffer();
            for (int i = 0; i < passwordLength_; i++) {
                mask.append(maskChar);
            }
            // try to write mask over password.
            ccsidManager_.convertFromUCS2(mask.toString(), bytes_, passwordStart_, netAgent_);
        } catch (SqlException sqle) {
            // failed to convert mask,
            // them simply replace with 0xFF.
            for (int i = 0; i < passwordLength_; i++) {
                bytes_[passwordStart_ + i] = (byte) 0xFF;
            }
        }
    }

    // insert a java byte into the buffer.
    final void writeByte(byte v) {
        ensureLength(offset_ + 1);
        bytes_[offset_++] = v;
    }

    // insert a java short into the buffer.
    final void writeShort(short v) {
        ensureLength(offset_ + 2);
        org.apache.derby.client.am.SignedBinary.shortToBigEndianBytes(bytes_, offset_, v);
        offset_ += 2;
    }

    // insert a java int into the buffer.
    void writeInt(int v) {
        ensureLength(offset_ + 4);
        org.apache.derby.client.am.SignedBinary.intToBigEndianBytes(bytes_, offset_, v);
        offset_ += 4;
    }

    // insert a java long into the buffer.
    final void writeLong(long v) {
        ensureLength(offset_ + 8);
        org.apache.derby.client.am.SignedBinary.longToBigEndianBytes(bytes_, offset_, v);
        offset_ += 8;
    }

    //-- The following are the write short/int/long in bigEndian byte ordering --

    // when writing Fdoca data.
    protected void writeShortFdocaData(short v) {
        ensureLength(offset_ + 2);
        org.apache.derby.client.am.SignedBinary.shortToBigEndianBytes(bytes_, offset_, v);
        offset_ += 2;
    }

    // when writing Fdoca data.
    protected void writeIntFdocaData(int v) {
        ensureLength(offset_ + 4);
        org.apache.derby.client.am.SignedBinary.intToBigEndianBytes(bytes_, offset_, v);
        offset_ += 4;
    }

    // when writing Fdoca data.
    protected void writeLongFdocaData(long v) {
        ensureLength(offset_ + 8);
        org.apache.derby.client.am.SignedBinary.longToBigEndianBytes(bytes_, offset_, v);
        offset_ += 8;
    }

    // insert a java float into the buffer.
    protected void writeFloat(float v) {
        ensureLength(offset_ + 4);
        org.apache.derby.client.am.FloatingPoint.floatToIeee754Bytes(bytes_, offset_, v);
        offset_ += 4;
    }

    // insert a java double into the buffer.
    protected void writeDouble(double v) {
        ensureLength(offset_ + 8);
        org.apache.derby.client.am.FloatingPoint.doubleToIeee754Bytes(bytes_, offset_, v);
        offset_ += 8;
    }

    // insert a java.math.BigDecimal into the buffer.
    final void writeBigDecimal(java.math.BigDecimal v,
                               int declaredPrecision,
                               int declaredScale) throws SqlException {
        ensureLength(offset_ + 16);
        try {
            int length = org.apache.derby.client.am.Decimal.bigDecimalToPackedDecimalBytes(bytes_, offset_, v, declaredPrecision, declaredScale);
            offset_ += length;
        } catch (org.apache.derby.client.am.ConversionException e) {
            throw new SqlException(netAgent_.logWriter_, e,
                    "BigDecimal conversion exception " + e.getMessage() + ". See attached Throwable.");
        }
    }

    final void writeDate(java.sql.Date date) throws SqlException {
        try {
            ensureLength(offset_ + 10);
            org.apache.derby.client.am.DateTime.dateToDateBytes(bytes_, offset_, date);
            offset_ += 10;
        } catch (org.apache.derby.client.am.ConversionException e) {
            throw new SqlException(netAgent_.logWriter_, e,
                    "Date conversion exception " + e.getMessage() + ". See attached Throwable.");
        }
    }

    final void writeTime(java.sql.Time time) throws SqlException {
        ensureLength(offset_ + 8);
        org.apache.derby.client.am.DateTime.timeToTimeBytes(bytes_, offset_, time);
        offset_ += 8;
    }

    final void writeTimestamp(java.sql.Timestamp timestamp) throws SqlException {
        try {
            ensureLength(offset_ + 26);
            org.apache.derby.client.am.DateTime.timestampToTimestampBytes(bytes_, offset_, timestamp);
            offset_ += 26;
        } catch (org.apache.derby.client.am.ConversionException e) {
            throw new SqlException(netAgent_.logWriter_, e,
                    "Timestamp conversion exception " + e.getMessage() + ". See attached Throwable.");
        }
    }

    // insert a java boolean into the buffer.  the boolean is written
    // as a signed byte having the value 0 or 1.
    final void writeBoolean(boolean v) {
        ensureLength(offset_ + 1);
        bytes_[offset_++] = (byte) ((v ? 1 : 0) & 0xff);
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
        } catch (java.io.UnsupportedEncodingException e) {
            throw new SqlException(netAgent_.logWriter_, e,
                    "Unsupported encoding " + e.getMessage() + ". See attached Throwable.");
        }
        if (b.length > 0x7FFF) {
            throw new SqlException(netAgent_.logWriter_, "string exceed maximum length 32767");
        }
        ensureLength(offset_ + b.length + 2);
        writeLDBytesX(b.length, b);
    }


    final void writeLDBytes(byte[] bytes) {
        ensureLength(offset_ + bytes.length + 2);
        writeLDBytesX(bytes.length, bytes);
    }

    // private helper method which should only be called by a Request method.
    // must call ensureLength before calling this method.
    // added for code reuse and helps perf by reducing ensureLength calls.
    // ldSize and bytes.length may not be the same.  this is true
    // when writing graphic ld strings.
    private final void writeLDBytesX(int ldSize, byte[] bytes) {
        bytes_[offset_++] = (byte) ((ldSize >>> 8) & 0xff);
        bytes_[offset_++] = (byte) (ldSize & 0xff);
        System.arraycopy(bytes, 0, bytes_, offset_, bytes.length);
        offset_ += bytes.length;
    }

    // does it follows
    // ccsid manager or typdef rules.  should this method write ddm character
    // data or fodca data right now it is coded for ddm char data only
    final void writeDDMString(String s) throws SqlException {
        ensureLength(offset_ + s.length());
        offset_ = ccsidManager_.convertFromUCS2(s, bytes_, offset_, netAgent_);
    }


    private byte[] buildLengthAndCodePointForEncryptedLob(int codePoint,
                                                          int leftToRead,
                                                          boolean writeNullByte,
                                                          int extendedLengthByteCount) throws DisconnectException {
        byte[] lengthAndCodepoint = new byte[4];
        byte[] extendedLengthBytes = new byte[extendedLengthByteCount];

        if (extendedLengthByteCount > 0) {
            // method should never ensure length
            lengthAndCodepoint = writeEXTDTALengthCodePointForEncryption(0x8004 + extendedLengthByteCount, codePoint);

            if (writeNullByte) {

                extendedLengthBytes = writeExtendedLengthBytesForEncryption(extendedLengthByteCount, leftToRead + 1);
            } else {
                extendedLengthBytes = writeExtendedLengthBytesForEncryption(extendedLengthByteCount, leftToRead);
            }
        } else {
            if (writeNullByte) {
                lengthAndCodepoint = writeEXTDTALengthCodePointForEncryption(leftToRead + 4 + 1, codePoint);
            } else {
                lengthAndCodepoint = writeEXTDTALengthCodePointForEncryption(leftToRead + 4, codePoint);
            }
        }

        if (extendedLengthByteCount > 0) {
            byte[] newLengthAndCodepoint = new byte[4 + extendedLengthBytes.length];
            System.arraycopy(lengthAndCodepoint, 0, newLengthAndCodepoint, 0, lengthAndCodepoint.length);
            System.arraycopy(extendedLengthBytes, 0, newLengthAndCodepoint, lengthAndCodepoint.length, extendedLengthBytes.length);
            lengthAndCodepoint = newLengthAndCodepoint;
        }

        if (writeNullByte) {
            byte[] nullByte = new byte[1 + lengthAndCodepoint.length];
            System.arraycopy(lengthAndCodepoint, 0, nullByte, 0, lengthAndCodepoint.length);
            nullByte[lengthAndCodepoint.length] = 0;
            lengthAndCodepoint = nullByte;
        }
        return lengthAndCodepoint;
    }


    private void buildLengthAndCodePointForLob(int codePoint,
                                               int leftToRead,
                                               boolean writeNullByte,
                                               int extendedLengthByteCount) throws DisconnectException {
        if (extendedLengthByteCount > 0) {
            // method should never ensure length
            writeLengthCodePoint(0x8004 + extendedLengthByteCount, codePoint);

            if (writeNullByte) {
                writeExtendedLengthBytes(extendedLengthByteCount, leftToRead + 1);
            } else {
                writeExtendedLengthBytes(extendedLengthByteCount, leftToRead);
            }
        } else {
            if (writeNullByte) {
                writeLengthCodePoint(leftToRead + 4 + 1, codePoint);
            } else {
                writeLengthCodePoint(leftToRead + 4, codePoint);
            }
        }

        // write the null byte, if necessary
        if (writeNullByte) {
            write1Byte(0x0);
        }

    }

    public void setDssLengthLocation(int location) {
        dssLengthLocation_ = location;
    }

    public void setCorrelationID(int id) {
        correlationID_ = id;
    }
}
