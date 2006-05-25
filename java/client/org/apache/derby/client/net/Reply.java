/*

   Derby - Class org.apache.derby.client.net.Reply

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


import java.io.ByteArrayOutputStream;
import java.util.Arrays;

import org.apache.derby.client.am.SignedBinary;
import org.apache.derby.client.am.SqlException;
import org.apache.derby.client.am.DisconnectException;
import org.apache.derby.client.am.SqlState;
import org.apache.derby.client.am.ClientMessageId;

import org.apache.derby.shared.common.reference.SQLState;
import org.apache.derby.shared.common.reference.MessageId;

public class Reply {
    protected org.apache.derby.client.am.Agent agent_;
    protected NetAgent netAgent_; //cheat-link to (NetAgent) agent_

    private CcsidManager ccsidManager_;
    protected final static int DEFAULT_BUFFER_SIZE = 32767;
    protected byte[] buffer_;
    protected int pos_;
    protected int count_;

    private int topDdmCollectionStack_;
    private final static int MAX_MARKS_NESTING = 10;
    private int[] ddmCollectionLenStack_;
    private int ddmScalarLen_; // a value of -1 -> streamed ddm -> length unknown
    private final static int EMPTY_STACK = -1;

    protected boolean ensuredLengthForDecryption_ = false; // A layer lengths have already been ensured in decrypt method.
    protected byte[] longBufferForDecryption_ = null;
    protected int longPosForDecryption_ = 0;
    protected byte[] longValueForDecryption_ = null;
    protected int longCountForDecryption_ = 0;

    protected int dssLength_;
    protected boolean dssIsContinued_;
    private boolean dssIsChainedWithSameID_;
    private boolean dssIsChainedWithDiffID_;
    protected int dssCorrelationID_;

    protected int peekedLength_ = 0;
    protected int peekedCodePoint_ = END_OF_COLLECTION;    // saves the peeked codept
    private int peekedNumOfExtendedLenBytes_ = 0;
    private int currentPos_ = 0;

    public final static int END_OF_COLLECTION = -1;
    public final static int END_OF_SAME_ID_CHAIN = -2;

    Reply(NetAgent netAgent, int bufferSize) {
        buffer_ = new byte[bufferSize];
        agent_ = netAgent_ = netAgent;
        ccsidManager_ = netAgent.targetCcsidManager_;
        ddmCollectionLenStack_ = new int[Reply.MAX_MARKS_NESTING];
        initialize();
    }

    final void initialize() {
        pos_ = 0;
        count_ = 0;
        topDdmCollectionStack_ = Reply.EMPTY_STACK;
        Arrays.fill(ddmCollectionLenStack_, 0);
        ddmScalarLen_ = 0;
        dssLength_ = 0;
        dssIsContinued_ = false;
        dssIsChainedWithSameID_ = false;
        dssIsChainedWithDiffID_ = false;
        dssCorrelationID_ = 1;
    }

    final int getDdmLength() {
        return ddmScalarLen_;
    }

    // This is a helper method which shifts the buffered bytes from
    // wherever they are in the current buffer to the beginning of
    // different buffer (note these buffers could be the same).
    // State information is updated as needed after the shift.
    private final void shiftBuffer(byte[] destinationBuffer) {
        // calculate the size of the data in the current buffer.
        int sz = count_ - pos_;

        // copy this data to the new buffer starting at position 0.
        System.arraycopy(buffer_, pos_, destinationBuffer, 0, sz);

        // update the state information for data in the new buffer.
        pos_ = 0;
        count_ = sz;

        // replace the old buffer with the new buffer.
        buffer_ = destinationBuffer;
    }

    // This method makes sure there is enough room in the buffer
    // for a certain number of bytes.  This method will allocate
    // a new buffer if needed and shift the bytes in the current buffer
    // to make ensure space is available for a fill.  Right now
    // this method will shift bytes as needed to make sure there is
    // as much room as possible in the buffer before trying to
    // do the read.  The idea is to try to have space to get as much data as possible
    // if we need to do a read on the socket's stream.
    protected final void ensureSpaceInBufferForFill(int desiredSpace) {
        // calculate the total unused space in the buffer.
        // this includes any space at the end of the buffer and any free
        // space at the beginning resulting from bytes already read.
        int currentAvailableSpace = (buffer_.length - count_) + pos_;

        // check to see if there is enough free space.
        if (currentAvailableSpace < desiredSpace) {

            // there is not enough free space so we need more storage.
            // we are going to double the buffer unless that happens to still be too small.
            // if more than double the buffer is needed, use the smallest amount over this as possible.
            int doubleBufferSize = (2 * buffer_.length);

            int minumNewBufferSize = (desiredSpace - currentAvailableSpace) + buffer_.length;
            int newsz = minumNewBufferSize <= doubleBufferSize ? doubleBufferSize : minumNewBufferSize;

            byte[] newBuffer = new byte[newsz];

            // shift everything from the old buffer to the new buffer
            shiftBuffer(newBuffer);
        } else {

            // there is enough free space in the buffer but let's make sure it is all at the end.
            // this is also important because if we are going to do a read, it would be nice
            // to get as much data as possible and making room at the end if the buffer helps to
            // ensure this.
            if (pos_ != 0) {
                shiftBuffer(buffer_);
            }
        }
    }

    // This method will attempt to read a minimum number of bytes
    // from the underlying stream.  This method will keep trying to
    // read bytes until it has obtained at least the minimum number.
    // Now returns the total bytes read for decryption, use to return void.
    protected int fill(int minimumBytesNeeded) throws DisconnectException {
        // make sure that there is enough space in the buffer to hold
        // the minimum number of bytes needed.
        ensureSpaceInBufferForFill(minimumBytesNeeded);

        // read until the minimum number of bytes needed is now in the buffer.
        // hopefully the read method will return as many bytes as it can.
        int totalBytesRead = 0;
        int actualBytesRead = 0;
        do {
            try {
                // oops, we shouldn't expose the agent's input stream here, collapse this into a read method on the agent
                actualBytesRead = netAgent_.getInputStream().read(buffer_, count_, buffer_.length - count_);
            } catch (java.io.IOException ioe) {
                netAgent_.throwCommunicationsFailure(ioe);
            } finally {
                if (agent_.loggingEnabled()) {
                    ((NetLogWriter) netAgent_.logWriter_).traceProtocolFlow(buffer_,
                            count_,
                            actualBytesRead,
                            NetLogWriter.TYPE_TRACE_RECEIVE,
                            "Reply",
                            "fill",
                            2); // tracepoint
                }
            }
            count_ += actualBytesRead;
            totalBytesRead += actualBytesRead;

        } while ((totalBytesRead < minimumBytesNeeded) && (actualBytesRead != -1));

        if (actualBytesRead == -1) {
            if (totalBytesRead < minimumBytesNeeded) {
                netAgent_.accumulateChainBreakingReadExceptionAndThrow(
                    new DisconnectException(netAgent_,
                        new ClientMessageId(SQLState.NET_INSUFFICIENT_DATA),
                        new Integer(minimumBytesNeeded),
                        new Integer(totalBytesRead)));
            }
        }
        return totalBytesRead;
    }

    // Make sure a certain amount of Layer A data is in the buffer.
    // The data will be in the buffer after this method is called.
    // Now returns the total bytes read for decryption, use to return void.
    protected final int ensureALayerDataInBuffer(int desiredDataSize) throws DisconnectException {
        int totalBytesRead = 0;
        // calulate the the number of bytes in the buffer.
        int avail = count_ - pos_;

        // read more bytes off the network if the data is not in the buffer already.
        if (avail < desiredDataSize) {
            totalBytesRead = fill(desiredDataSize - avail);
        }
        return totalBytesRead;
    }

    protected final void ensureBLayerDataInBuffer(int desiredDataSize) throws DisconnectException {
        if (dssIsContinued_ && (desiredDataSize > dssLength_)) {
            int continueDssHeaderCount =
                    (((desiredDataSize - dssLength_) / 32767) + 1);
            ensureALayerDataInBuffer(desiredDataSize + (continueDssHeaderCount * 2));
            compressBLayerData(continueDssHeaderCount);
            return;
        }
        ensureALayerDataInBuffer(desiredDataSize);
    }

    // this will probably never be called.
    // it is included here in the highly unlikely event that a reply object
    // exceeds 32K.  for opimization purposes, we should consider
    // removing this.  removing this should be ok since we handle most
    // big stuff returned from the server (qrydta's for example) by
    // copying out the data into some other storage.  any extended dss header
    // info will be removed in the copying process.
    private final void compressBLayerData(int continueDssHeaderCount) throws DisconnectException {
        int tempPos = 0;

        // jump to the last continuation header.
        for (int i = 0; i < continueDssHeaderCount; i++) {
            // the first may be less than the size of a full dss
            if (i == 0) {
                // only jump by the number of bytes remaining in the current dss
                tempPos = pos_ + dssLength_;
            } else {
                // all other jumps are for a full continued dss
                tempPos += 32767;
            }
        }

        // for each of the dss headers to remove,
        // read out the continuation header and increment the dss length by the
        // size of the conitnation bytes,  then shift the continuation data as needed.
        int shiftSize = 0;
        int bytesToShift = 0;
        int continueHeaderLength = 0;
        int newDssLength = 0;
        for (int i = 0; i < continueDssHeaderCount; i++) {

            continueHeaderLength = ((buffer_[tempPos] & 0xFF) << 8) +
                    ((buffer_[tempPos + 1] & 0xFF) << 0);

            if (i == 0) {
                // if this is the last one (farthest down stream and first to strip out)

                if ((continueHeaderLength & 0x8000) == 0x8000) {
                    // the last dss header is again continued
                    continueHeaderLength = 32767;
                    dssIsContinued_ = true;
                } else {
                    // the last dss header was not contiued so update continue state flag
                    dssIsContinued_ = false;
                }
                // the very first shift size is 2
                shiftSize = 2;
            } else {
                // already removed the last header so make sure the chaining flag is on
                if ((continueHeaderLength & 0x8000) == 0x8000) {
                    continueHeaderLength = 32767;
                } else {
                    // this is a syntax error but not really certain which one.
                    // for now pick 0x02 which is Dss header Length does not match the number
                    // of bytes of data found.
                    doSyntaxrmSemantics(CodePoint.SYNERRCD_DSS_LENGTH_BYTE_NUMBER_MISMATCH);
                }
                // increase the shift size by 2
                shiftSize += 2;
            }

            // it is a syntax error if the dss continuation is less than or equal to two
            if (continueHeaderLength <= 2) {
                doSyntaxrmSemantics(CodePoint.SYNERRCD_DSS_CONT_LESS_OR_EQUAL_2);
            }

            newDssLength += (continueHeaderLength - 2);

            // calculate the number of bytes to shift
            if (i != (continueDssHeaderCount - 1)) {
                bytesToShift = 32767;
            } else {
                bytesToShift = dssLength_;
            }

            tempPos -= (bytesToShift - 2);
            System.arraycopy(buffer_, tempPos - shiftSize, buffer_, tempPos , bytesToShift);
        }
        // reposition the start of the data after the final dss shift.
        pos_ = tempPos;
        dssLength_ = dssLength_ + newDssLength;
    }

    protected final void readDssHeader() throws DisconnectException {
        int correlationID = 0;
        int nextCorrelationID = 0;
        ensureALayerDataInBuffer(6);

        // read out the dss length
        dssLength_ =
                ((buffer_[pos_++] & 0xFF) << 8) +
                ((buffer_[pos_++] & 0xFF) << 0);

        // Remember the old dss length for decryption only.
        int oldDssLength = dssLength_;

        // check for the continuation bit and update length as needed.
        if ((dssLength_ & 0x8000) == 0x8000) {
            dssLength_ = 32767;
            dssIsContinued_ = true;
        } else {
            dssIsContinued_ = false;
        }

        if (dssLength_ < 6) {
            doSyntaxrmSemantics(CodePoint.SYNERRCD_DSS_LESS_THAN_6);
        }

        // If the GDS id is not valid, or
        // if the reply is not an RPYDSS nor
        // a OBJDSS, then throw an exception.
        if ((buffer_[pos_++] & 0xFF) != 0xd0) {
            doSyntaxrmSemantics(CodePoint.SYNERRCD_CBYTE_NOT_D0);
        }

        int gdsFormatter = buffer_[pos_++] & 0xFF;
        if (((gdsFormatter & 0x02) != 0x02)
                && ((gdsFormatter & 0x03) != 0x03)
                && ((gdsFormatter & 0x04) != 0x04)) {
            doSyntaxrmSemantics(CodePoint.SYNERRCD_FBYTE_NOT_SUPPORTED);
        }

        // Determine if the current DSS is chained with the
        // next DSS, with the same or different request ID.
        if ((gdsFormatter & 0x40) == 0x40) {    // on indicates structure chained to next structure
            if ((gdsFormatter & 0x10) == 0x10) {
                dssIsChainedWithSameID_ = true;
                dssIsChainedWithDiffID_ = false;
                nextCorrelationID = dssCorrelationID_;
            } else {
                dssIsChainedWithSameID_ = false;
                dssIsChainedWithDiffID_ = true;
                nextCorrelationID = dssCorrelationID_ + 1;
            }
        } else {
            // chaining bit not b'1', make sure DSSFMT bit3 not b'1'
            if ((gdsFormatter & 0x10) == 0x10) {  // Next DSS can not have same correlator
                doSyntaxrmSemantics(CodePoint.SYNERRCD_CHAIN_OFF_SAME_NEXT_CORRELATOR);
            }

            // chaining bit not b'1', make sure no error continuation
            if ((gdsFormatter & 0x20) == 0x20) { // must be 'do not continue on error'
                doSyntaxrmSemantics(CodePoint.SYNERRCD_CHAIN_OFF_ERROR_CONTINUE);
            }

            dssIsChainedWithSameID_ = false;
            dssIsChainedWithDiffID_ = false;
            nextCorrelationID = 1;
        }

        correlationID =
                ((buffer_[pos_++] & 0xFF) << 8) +
                ((buffer_[pos_++] & 0xFF) << 0);

        // corrid must be the one expected or a -1 which gets returned in some error cases.
        if ((correlationID != dssCorrelationID_) && (correlationID != 0xFFFF)) {
            doSyntaxrmSemantics(CodePoint.SYNERRCD_INVALID_CORRELATOR);
        } else {
            dssCorrelationID_ = nextCorrelationID;
        }
        dssLength_ -= 6;
        if ((gdsFormatter & 0x04) == 0x04) {
            decryptData(gdsFormatter, oldDssLength);  //we have to decrypt data here because
        }
        //we need the decrypted codepoint. If
        //Data is very long > 32767, we have to
        //get all the data first because decrypt
        //piece by piece doesn't work.
    }


    private final void decryptData(int gdsFormatter, int oldDssLength) throws DisconnectException {
        boolean readHeader;

        if (dssLength_ == 32761) {
            ByteArrayOutputStream baos;
            int copySize = 0;

            baos = new ByteArrayOutputStream();

            // set the amount to read for the first segment
            copySize = dssLength_; // note: has already been adjusted for headers

            do {
                // determine if a continuation header needs to be read after the data
                if (dssIsContinued_) {
                    readHeader = true;
                } else {
                    readHeader = false;
                }

                // read the segment
                ensureALayerDataInBuffer(copySize);
                adjustLengths(copySize);
                baos.write(buffer_, pos_, copySize);
                pos_ += copySize;

                // read the continuation header, if necessary
                if (readHeader) {
                    readDSSContinuationHeader();
                }

                copySize = dssLength_;
            } while (readHeader == true);
            byte[] cipherBytes = baos.toByteArray();
            byte[] clearedByte = null;
            try {
                clearedByte = netAgent_.netConnection_.getEncryptionManager().decryptData(cipherBytes,
                        NetConfiguration.SECMEC_EUSRIDPWD,
                        netAgent_.netConnection_.getTargetPublicKey(),
                        netAgent_.netConnection_.getTargetPublicKey());
            } catch (SqlException e) {
                //throw new SqlException (agent_.logWriter_, "error in decrypting data");
            }

            //The decrypted data is for one codepoint only. We need to save the data follows this codepoint
            longBufferForDecryption_ = new byte[buffer_.length - pos_];
            longPosForDecryption_ = 0;
            count_ = count_ - pos_;
            longCountForDecryption_ = count_;
            System.arraycopy(buffer_, pos_, longBufferForDecryption_, 0, buffer_.length - pos_);

            //copy the clear data to buffer_
            if (clearedByte.length >= 32767) {
                System.arraycopy(clearedByte, 0, buffer_, 0, 32767);
            } else {
                System.arraycopy(clearedByte, 0, buffer_, 0, clearedByte.length);
            }

            pos_ = 0;
            dssLength_ = buffer_.length;

            int lobLength = 0;
            if (clearedByte.length > 32767) {  //for extended length, length is the 4 bytes that follow codepoint
                lobLength = ((clearedByte[4] & 0xFF) << 24) +
                        ((clearedByte[5] & 0xFF) << 16) +
                        ((clearedByte[6] & 0xFF) << 8) +
                        ((clearedByte[7] & 0xFF) << 0);
                longValueForDecryption_ = new byte[lobLength];
                System.arraycopy(clearedByte, 8, longValueForDecryption_, 0, clearedByte.length - 8);
            } else {
                lobLength = ((clearedByte[0] & 0xFF) << 8) +
                        ((clearedByte[1] & 0xFF) << 0);
                longValueForDecryption_ = new byte[lobLength - 4];
                System.arraycopy(clearedByte, 4, longValueForDecryption_, 0, clearedByte.length - 4);
            }
        } else {
            int bytesRead = ensureALayerDataInBuffer(dssLength_);  //we need to get back all the data here, and then decrypt
            if (bytesRead > 0) //we ensuredALayerDAtaInBuffer here and set the flag to true, so we don't need do this again later
            {
                ensuredLengthForDecryption_ = true;
            }
            byte[] encryptedByte = new byte[dssLength_];
            System.arraycopy(buffer_, pos_, encryptedByte, 0, dssLength_);
            byte[] array1 = new byte[pos_];
            System.arraycopy(buffer_, 0, array1, 0, pos_);  //save the data before encrypted data in array1
            byte[] array3 = new byte[buffer_.length - dssLength_ - pos_];
            System.arraycopy(buffer_, pos_ + dssLength_, array3, 0, buffer_.length - dssLength_ - pos_); //save the data follows encrypted data in array3
            byte[] clearedByte = null;
            try {
                clearedByte = netAgent_.netConnection_.getEncryptionManager().decryptData(encryptedByte,
                        NetConfiguration.SECMEC_EUSRIDPWD,
                        netAgent_.netConnection_.getTargetPublicKey(),
                        netAgent_.netConnection_.getTargetPublicKey());
            } catch (SqlException e) {
                //throw new SqlException (agent_.logWriter_, "error in decrypting data");
            }
            dssLength_ -= (encryptedByte.length - clearedByte.length);
            byte[] buffer = new byte[array1.length + clearedByte.length + array3.length];
            System.arraycopy(array1, 0, buffer, 0, array1.length);
            System.arraycopy(clearedByte, 0, buffer, array1.length, clearedByte.length);
            System.arraycopy(array3, 0, buffer, array1.length + clearedByte.length, array3.length);
            buffer_ = buffer;
            int oldCount = count_;
            count_ = count_ - (encryptedByte.length - clearedByte.length);
            if (((clearedByte[2] & 0xff) << 8) + ((clearedByte[3] & 0xff) << 0) == 0x146c) {
                int firstLobLength = ((clearedByte[0] & 0xFF) << 8) +
                        ((clearedByte[1] & 0xFF) << 0);

                boolean flag = false;
                if (gdsFormatter == 0x54) {
                    flag = true;
                }
                if (flag) {
                    if (oldCount - oldDssLength < 6) {
                        int totalBytesRead = fill(6); //sometimes the 2nd EXTDTA doesn't come back, need to fetch again to get it
                        if (totalBytesRead > 0) {
                            longBufferForDecryption_ = new byte[totalBytesRead];
                            longPosForDecryption_ = 0;
                            System.arraycopy(buffer_, pos_ + firstLobLength, longBufferForDecryption_, 0,
                                    totalBytesRead);
                        }

                    } else {
                        longBufferForDecryption_ = new byte[count_ - pos_ - firstLobLength];
                        longPosForDecryption_ = 0;
                        System.arraycopy(buffer_, pos_ + firstLobLength, longBufferForDecryption_, 0,
                                longBufferForDecryption_.length);

                    }
                } //end if(flag)
                int lobLength = ((clearedByte[0] & 0xFF) << 8) +
                        ((clearedByte[1] & 0xFF) << 0) - 4;

                longValueForDecryption_ = new byte[lobLength];

                System.arraycopy(clearedByte, 4, longValueForDecryption_, 0, clearedByte.length - 4);  //copy the decrypted lob value (excluded length an dcodepoint) to longValue_
            } else if (((clearedByte[2] & 0xff) << 8) + ((clearedByte[3] & 0xff) << 0) == 0x241B) {
                int length = ((clearedByte[0] & 0xFF) << 8) +
                        ((clearedByte[1] & 0xFF) << 0);
                boolean noData = false;
                if (clearedByte[4] == -1 && clearedByte[5] == -1) {
                    noData = true; //there is no data, no need to do the copy
                }
                if (!noData) {
                    if (length == 32776) {
                        length = ((clearedByte[4] & 0xFF) << 24) +
                                ((clearedByte[5] & 0xFF) << 16) +
                                ((clearedByte[6] & 0xFF) << 8) +
                                ((clearedByte[7] & 0xFF) << 0);
                        longValueForDecryption_ = new byte[length];
                        System.arraycopy(clearedByte, 8, longValueForDecryption_, 0,
                                clearedByte.length - 8);
                        longCountForDecryption_ = count_ - (pos_ + length + 8);
                        longBufferForDecryption_ = new byte[buffer_.length - pos_ - length - 8];
                        System.arraycopy(buffer_, pos_ + length + 8, longBufferForDecryption_, 0,
                                longBufferForDecryption_.length);

                    } else {
                        longPosForDecryption_ = 0;
                        longCountForDecryption_ = count_ - (pos_ + length);
                        longBufferForDecryption_ = new byte[buffer_.length - pos_ - length];
                        System.arraycopy(buffer_, pos_ + length, longBufferForDecryption_, 0,
                                longBufferForDecryption_.length);

                        longValueForDecryption_ = new byte[length - 4];

                        System.arraycopy(clearedByte, 4, longValueForDecryption_, 0,
                                clearedByte.length - 4);
                    }
                }
            }
        }
    }


    final int readUnsignedShort() throws DisconnectException {
        // should we be checking dss lengths and ddmScalarLengths here
        // if yes, i am not sure this is the correct place if we should be checking
        ensureBLayerDataInBuffer(2);
        adjustLengths(2);
        return ((buffer_[pos_++] & 0xff) << 8) +
                ((buffer_[pos_++] & 0xff) << 0);
    }

    final short readShort() throws DisconnectException {
        // should we be checking dss lengths and ddmScalarLengths here
        ensureBLayerDataInBuffer(2);
        adjustLengths(2);
        short s = SignedBinary.getShort(buffer_, pos_);

        pos_ += 2;

        return s;
    }

    final int readInt() throws DisconnectException {
        // should we be checking dss lengths and ddmScalarLengths here
        ensureBLayerDataInBuffer(4);
        adjustLengths(4);
        int i = SignedBinary.getInt(buffer_, pos_);
        pos_ += 4;

        return i;
    }

    final void readIntArray(int[] array) throws DisconnectException {
        ensureBLayerDataInBuffer(array.length * 4);
        adjustLengths(array.length * 4);

        for (int i = 0; i < array.length; i++) {
            array[i] = SignedBinary.getInt(buffer_, pos_);
            pos_ += 4;
        }
    }


    final long readLong() throws DisconnectException {
        // should we be checking dss lengths and ddmScalarLengths here
        ensureBLayerDataInBuffer(8);
        adjustLengths(8);
        long l = SignedBinary.getLong(buffer_, pos_);

        pos_ += 8;

        return l;
    }


    final int[] readUnsignedShortList() throws DisconnectException {
        int len = ddmScalarLen_;
        ensureBLayerDataInBuffer(len);
        adjustLengths(len);

        int count = len / 2;
        int[] list = new int[count];

        for (int i = 0; i < count; i++) {
            list[i] = ((buffer_[pos_++] & 0xff) << 8) +
                    ((buffer_[pos_++] & 0xff) << 0);
        }

        return list;
    }

    final int readUnsignedByte() throws DisconnectException {
        ensureBLayerDataInBuffer(1);
        adjustLengths(1);
        return (buffer_[pos_++] & 0xff);
    }

    final byte readByte() throws DisconnectException {
        ensureBLayerDataInBuffer(1);
        adjustLengths(1);
        return (byte) (buffer_[pos_++] & 0xff);
    }

    final boolean readBoolean() throws DisconnectException {
        ensureBLayerDataInBuffer(1);
        adjustLengths(1);
        return buffer_[pos_++] != 0;
    }

    final String readString(int length) throws DisconnectException {
        ensureBLayerDataInBuffer(length);
        adjustLengths(length);

        String result = ccsidManager_.convertToUCS2(buffer_, pos_, length);
        pos_ += length;
        return result;
    }

    final String readString(int length, String encoding) throws DisconnectException {
        ensureBLayerDataInBuffer(length);
        adjustLengths(length);
        String s = null;

        try {
            s = new String(buffer_, pos_, length, encoding);
        } catch (java.io.UnsupportedEncodingException e) {
            agent_.accumulateChainBreakingReadExceptionAndThrow(
                new DisconnectException(agent_,
                    new ClientMessageId(SQLState.NET_ENCODING_NOT_SUPPORTED), 
                    e));
        }

        pos_ += length;
        return s;
    }

    final String readString() throws DisconnectException {
        int len = ddmScalarLen_;
        ensureBLayerDataInBuffer(len);
        adjustLengths(len);
        String result = ccsidManager_.convertToUCS2(buffer_, pos_, len);
        pos_ += len;
        return result;
    }

    final byte[] readBytes(int length) throws DisconnectException {
        ensureBLayerDataInBuffer(length);
        adjustLengths(length);

        byte[] b = new byte[length];
        System.arraycopy(buffer_, pos_, b, 0, length);
        pos_ += length;
        return b;
    }

    final byte[] readBytes() throws DisconnectException {
        int len = ddmScalarLen_;
        ensureBLayerDataInBuffer(len);
        adjustLengths(len);

        byte[] b = new byte[len];
        System.arraycopy(buffer_, pos_, b, 0, len);
        pos_ += len;
        return b;
    }

    final byte[] readLDBytes() throws DisconnectException {
        ensureBLayerDataInBuffer(2);
        int len = ((buffer_[pos_++] & 0xff) << 8) + ((buffer_[pos_++] & 0xff) << 0);

        if (len == 0) {
            adjustLengths(2);
            return null;
        }

        ensureBLayerDataInBuffer(len);
        adjustLengths(len + 2);

        byte[] b = new byte[len];
        System.arraycopy(buffer_, pos_, b, 0, len);
        pos_ += len;
        return b;
    }

    final void skipBytes(int length) throws DisconnectException {
        ensureBLayerDataInBuffer(length);
        adjustLengths(length);
        pos_ += length;
    }

    final void skipBytes() throws DisconnectException {
        int len = ddmScalarLen_;
        ensureBLayerDataInBuffer(len);
        adjustLengths(len);
        pos_ += len;
    }

    // This will be the new and improved getData that handles all QRYDTA/EXTDTA
    // Returns the stream so that the caller can cache it
    final ByteArrayOutputStream getData(ByteArrayOutputStream existingBuffer) throws DisconnectException {
        boolean readHeader;
        int copySize;
        ByteArrayOutputStream baos;

        // note: an empty baos can yield an allocated and empty byte[]
        if (existingBuffer != null) {
            baos = existingBuffer;
        } else {
            if (ddmScalarLen_ != -1) {
                // allocate a stream based on a known amount of data
                baos = new ByteArrayOutputStream(ddmScalarLen_);
            } else {
                // allocate a stream to hold an unknown amount of data
                baos = new ByteArrayOutputStream();
                //isLengthAndNullabilityUnknown = true;
            }
        }

        // set the amount to read for the first segment
        copySize = dssLength_; // note: has already been adjusted for headers

        do {
            // determine if a continuation header needs to be read after the data
            if (dssIsContinued_) {
                readHeader = true;
            } else {
                readHeader = false;
            }

            // read the segment
            ensureALayerDataInBuffer(copySize);
            adjustLengths(copySize);
            baos.write(buffer_, pos_, copySize);
            pos_ += copySize;

            // read the continuation header, if necessary
            if (readHeader) {
                readDSSContinuationHeader();
            }

            copySize = dssLength_;
        } while (readHeader == true);

        return baos;
    }

    // reads a DSS continuation header
    // prereq: pos_ is positioned on the first byte of the two-byte header
    // post:   dssIsContinued_ is set to true if the continuation bit is on, false otherwise
    //         dssLength_ is set to DssConstants.MAX_DSS_LEN - 2 (don't count the header for the next read)
    // helper method for getEXTDTAData
    protected final void readDSSContinuationHeader() throws DisconnectException {
        ensureALayerDataInBuffer(2);

        dssLength_ =
                ((buffer_[pos_++] & 0xFF) << 8) +
                ((buffer_[pos_++] & 0xFF) << 0);

        if ((dssLength_ & 0x8000) == 0x8000) {
            dssLength_ = DssConstants.MAX_DSS_LEN;
            dssIsContinued_ = true;
        } else {
            dssIsContinued_ = false;
        }
        // it is a syntax error if the dss continuation header length
        // is less than or equal to two
        if (dssLength_ <= 2) {
            doSyntaxrmSemantics(CodePoint.SYNERRCD_DSS_CONT_LESS_OR_EQUAL_2);
        }

        dssLength_ -= 2;  // avoid consuming the DSS cont header
    }


    // As part of parsing the reply, the client can detect that the
    // data sent from the target agent does not structurally
    // conform to the requirements of the DDM architecture.  These are
    // the same checks performed by the target server on the messages
    // it receives from the protocolj code.  Server side detected errors
    // result in a SYNTAXRM being returned from the AS.  According to the
    // DDM manual, parsing of the DSS is terminated when the error is
    // detected.  The Syntax Error Code, SYNERRCD, describes the various errors.
    //
    // Note: Not all of these may be valid at the client.  See descriptions for
    // which ones make sense for client side errors/checks.
    // Syntax Error Code                  Description of Error
    // -----------------                  --------------------
    // 0x01                               Dss header Length is less than 6.
    // 0x02                               Dss header Length does not match the
    //                                    number of bytes of data found.
    // 0x03                               Dss header C-byte not D0.
    // 0x04                               Dss header f-bytes either not
    //                                    recognized or not supported.
    // 0x05                               DSS continuation specified but not found.
    //                                    For example, DSS continuation is specified
    //                                    on the last DSS, and the SNA LU 6.2 communication
    //                                    facility returned the SEND indicator.
    // 0x06                               DSS chaining specified but no DSS found.
    //                                    For example, DSS chaining is specified
    //                                    on the last DSS, and the SNA LU 6.2 communication
    //                                    facility returned the SEND indicator.
    // 0x07                               Object length less than four.  For example,
    //                                    a command parameter's length is specified
    //                                    as two, or a command's length is specified as three.
    // 0x08                               Object length does not match the number of bytes of data
    //                                    found.  For example, a RQSDSS with a length of 150
    //                                    contains a command whose length is 125 or a SRVDGN parameter
    //                                    specifies a length of 200 but there are only 50
    //                                    bytes left in the DSS.
    // 0x09                               Object length greater than maximum allowed.
    //                                    For example, a RECCNT parameter specifies a
    //                                    length of ten, but the parameter is defined
    //                                    to have a maximum length of eight.
    // 0x0A                               Object length less than the minimum required.
    //                                    For example, a SVRCOD parameter specifies a
    //                                    length of five, but the parameter is defined
    //                                    to have a fixed length of six.
    // 0x0B                               Object length not allowed.  For example,
    //                                    a FILEXDPT parameter is specified with a length of
    //                                    11, but this would indicate that only half of the hours
    //                                    field is present instead of the complete hours field.
    // 0x0C                               Incorrect large object extended length field (see
    //                                    description of DSS).  For example, an extended
    //                                    length field is present, but it is only three bytes
    //                                    long when it is defined to be a multiple of two bytes.
    // 0x0D                               Object code point index not supported.
    //                                    For example, a code point of 8032 is encountered
    //                                    but x'8' is a reserved code point index.
    // 0x0E                               Required object not found.  For example, a CLEAR
    //                                    command does not have a filnam parameter present,
    //                                    or a MODREC command is not followed by a RECORD
    //                                    command data object.
    // 0x0F                               Too many command data objects sent.  For example,
    //                                    a MODREC command is followed by two RECORD command
    //                                    command data objects, or a DECREC command is followed
    //                                    by RECORD object.
    // 0x10                               Mutually exclusive objects present.
    //                                    For example, a CRTDIRF command specifies both
    //                                    a DCLNAM and FILNAM parameters.
    // 0x11                               Too few command data objects sent.
    //                                    For example, an INSRECEF command that
    //                                    specified RECCNT95) is followed by only
    //                                    4 RECORD command data objects.
    // 0x12                               Duplicate object present.
    //                                    For example, a LSTFAT command has tow FILNAM
    //                                    parameters specified.
    // 0x13                               Invalid request correlator specified.
    //                                    Use PRCCNVRM with PRCCNVDC of 04 or 05 instead
    //                                    of this error code.  This error code is being retained
    //                                    for compatibility with Level 1 of the architecture.
    // 0x14                               Required value not found.
    // 0x15                               Reserved value not allowed.  For example,
    //                                    a INSRECEF command specified a RECCNT(0) parameter.
    // 0x16                               DSS continuation less than or equal to two.
    //                                    For example, the length bytes of the DSS continuation
    //                                    have the value of one.
    // 0x17                               Objects not in required order.  For example, a RECAL
    //                                    object contains a RECORD object followed by a RECNBR
    //                                    object with is not in the defined order.
    // 0x18                               DSS chaining byt not b'1', but DSSFMT bit3 set to b'1'.
    // 0x19                               Previous DSS indicated current DSS has the same
    //                                    request correlator, but the request correlators are
    //                                    not the same.
    // 0x1A                               DSS cahining bit not b'1', but error continuation requested.
    // 0x1B                               Mutually exclusive parameter values not specified.
    //                                    For example, an OPEN command specified PRPSHD(TRUE)
    //                                    and FILSHR(READER).
    // 0x1D                               Code point not valid command.  For example, the first
    //                                    code point in RQSDSS either is not in the dictionary
    //                                    or is not a code point for a command.
    //
    // When the client detects these errors, it will be handled as if a SYNTAXRM is returned
    // from the server.  In this SYNTAXRM case, PROTOCOL architects an SQLSTATE of 58008 or 58009.
    //
    // Messages
    // SQLSTATE : 58009
    //     Execution failed due to a distribution protocol error that caused deallocation of the conversation.
    //     SQLCODE : -30020
    //     Execution failed because of a Distributed Protocol
    //         Error that will affect the successful execution of subsequent
    //         commands and SQL statements: Reason Code <reason-code>.
    //      Some possible reason codes include:
    //      121C Indicates that the user is not authorized to perform the requested command.
    //      1232 The command could not be completed because of a permanent error.
    //          In most cases, the server will be in the process of an abend.
    //      220A The target server has received an invalid data description.
    //          If a user SQLDA is specified, ensure that the fields are
    //          initialized correctly. Also, ensure that the length does not
    //          exceed the maximum allowed length for the data type being used.
    //
    //      The command or statement cannot be processed.  The current
    //          transaction is rolled back and the application is disconnected
    //          from the remote database.
    final void doSyntaxrmSemantics(int syntaxErrorCode) throws DisconnectException {
        agent_.accumulateChainBreakingReadExceptionAndThrow(
            new DisconnectException(agent_,
                new ClientMessageId(SQLState.DRDA_CONNECTION_TERMINATED),
                SqlException.getMessageUtil().getTextMessage(
                    MessageId.CONN_DRDA_DATASTREAM_SYNTAX_ERROR,
                    new Integer(syntaxErrorCode))));
    }


// the names of these methods start with a letter z.
// the z will be removed when they are finalized...

    protected final void pushLengthOnCollectionStack() {
        ddmCollectionLenStack_[++topDdmCollectionStack_] = ddmScalarLen_;
        ddmScalarLen_ = 0;
    }

    protected final void adjustLengths(int length) {
        ddmScalarLen_ -= length;
        adjustCollectionAndDssLengths(length);
        /*
        for (int i = 0; i <= topDdmCollectionStack_; i++) {
          ddmCollectionLenStack_[i] -= length;
        }
        dssLength_ -= length;
        */
    }

    protected int adjustDdmLength(int ddmLength, int length) {
        ddmLength -= length;
        if (ddmLength == 0) {
            adjustLengths(getDdmLength());
        }
        return ddmLength;
    }

    // Pop the collection Length stack.
    // pre:  The collection length stack must not be empty and the top value
    //       on the stack must be 0.
    // post: The top 0 value on the stack will be popped.
    protected final void popCollectionStack() {
        topDdmCollectionStack_--;
    }

    protected final int peekCodePoint() throws DisconnectException {
        if (topDdmCollectionStack_ != EMPTY_STACK) {
            if (ddmCollectionLenStack_[topDdmCollectionStack_] == 0) {
                return END_OF_COLLECTION;
            } else if (ddmCollectionLenStack_[topDdmCollectionStack_] < 4) {
                // error
            }
        }

        // if there is no more data in the current dss, and the dss is not
        // continued, indicate the end of the same Id chain or read the next dss header.
        if ((dssLength_ == 0) && (!dssIsContinued_)) {
            if (!dssIsChainedWithSameID_) {
                return END_OF_SAME_ID_CHAIN;
            }
            readDssHeader();
        }

        if (longBufferForDecryption_ == null)  //we don't need to do this if it's data stream encryption
        {
            ensureBLayerDataInBuffer(4);
        }
        peekedLength_ = ((buffer_[pos_] & 0xff) << 8) + ((buffer_[pos_ + 1] & 0xff) << 0);
        peekedCodePoint_ = ((buffer_[pos_ + 2] & 0xff) << 8) + ((buffer_[pos_ + 3] & 0xff) << 0);

        // check for extended length
        if ((peekedLength_ & 0x8000) == 0x8000) {
            peekExtendedLength();
        } else {
            peekedNumOfExtendedLenBytes_ = 0;
        }
        return peekedCodePoint_;
    }

    // Read out the 2-byte length without moving the pos_ pointer.
    protected final int peekLength() throws DisconnectException {
        ensureBLayerDataInBuffer(2);
        return (((buffer_[pos_] & 0xff) << 8) +
                ((buffer_[pos_ + 1] & 0xff) << 0));
    }

    // Read "length" number of bytes from the buffer into the byte array b starting from offset
    // "offset".  The current offset in the buffer does not change.
    protected final int peekFastBytes(byte[] b, int offset, int length) throws DisconnectException {
        for (int i = 0; i < length; i++) {
            b[offset + i] = buffer_[pos_ + i];
        }
        return offset + length;
    }

    protected final void parseLengthAndMatchCodePoint(int expectedCodePoint) throws DisconnectException {
        int actualCodePoint = 0;
        if (peekedCodePoint_ == END_OF_COLLECTION) {
            actualCodePoint = readLengthAndCodePoint();
        } else {
            actualCodePoint = peekedCodePoint_;
            pos_ += (4 + peekedNumOfExtendedLenBytes_);
            ddmScalarLen_ = peekedLength_;
            if (peekedNumOfExtendedLenBytes_ == 0 && ddmScalarLen_ != -1) {
                adjustLengths(4);
            } else {
                adjustCollectionAndDssLengths(4 + peekedNumOfExtendedLenBytes_);
            }
            peekedLength_ = 0;
            peekedCodePoint_ = END_OF_COLLECTION;
            peekedNumOfExtendedLenBytes_ = 0;
        }

        if (actualCodePoint != expectedCodePoint) {
            agent_.accumulateChainBreakingReadExceptionAndThrow(
                new DisconnectException(agent_, 
                    new ClientMessageId(SQLState.NET_NOT_EXPECTED_CODEPOINT), 
                    new Integer(actualCodePoint), 
                    new Integer(expectedCodePoint)));
        }
    }

    protected final int readLengthAndCodePoint() throws DisconnectException {
        if (topDdmCollectionStack_ != EMPTY_STACK) {
            if (ddmCollectionLenStack_[topDdmCollectionStack_] == 0) {
                return END_OF_COLLECTION;
            } else if (ddmCollectionLenStack_[topDdmCollectionStack_] < 4) {
                agent_.accumulateChainBreakingReadExceptionAndThrow(
                    new DisconnectException(agent_, 
                    new ClientMessageId(SQLState.NET_DDM_COLLECTION_TOO_SMALL)));
            }
        }

        // if there is no more data in the current dss, and the dss is not
        // continued, indicate the end of the same Id chain or read the next dss header.
        if ((dssLength_ == 0) && (!dssIsContinued_)) {
            if (!dssIsChainedWithSameID_) {
                return END_OF_SAME_ID_CHAIN;
            }
            readDssHeader();
        }

        ensureBLayerDataInBuffer(4);
        ddmScalarLen_ =
                ((buffer_[pos_++] & 0xff) << 8) +
                ((buffer_[pos_++] & 0xff) << 0);
        int codePoint = ((buffer_[pos_++] & 0xff) << 8) +
                ((buffer_[pos_++] & 0xff) << 0);
        adjustLengths(4);

        // check for extended length
        if ((ddmScalarLen_ & 0x8000) == 0x8000) {
            readExtendedLength();
        }
        return codePoint;
    }

    private final void readExtendedLength() throws DisconnectException {
        int numberOfExtendedLenBytes = (ddmScalarLen_ - 0x8000); // fix scroll problem was - 4
        int adjustSize = 0;
        switch (numberOfExtendedLenBytes) {
        case 4:
            ensureBLayerDataInBuffer(4);
            ddmScalarLen_ =
                    ((buffer_[pos_++] & 0xff) << 24) +
                    ((buffer_[pos_++] & 0xff) << 16) +
                    ((buffer_[pos_++] & 0xff) << 8) +
                    ((buffer_[pos_++] & 0xff) << 0);
            adjustSize = 4;
            break;
        case 0:
            ddmScalarLen_ = -1;
            adjustSize = 0;
            break;
        default:
            doSyntaxrmSemantics(CodePoint.SYNERRCD_INCORRECT_EXTENDED_LEN);
        }

        adjustCollectionAndDssLengths(adjustSize);
        /*
        // adjust the lengths here.  this is a special case since the
        // extended length bytes do not include their own length.
        for (int i = 0; i <= topDdmCollectionStack_; i++) {
          ddmCollectionLenStack_[i] -= adjustSize;
        }
        dssLength_ -= adjustSize;
        */
    }

    private final void adjustCollectionAndDssLengths(int length) {
        // adjust the lengths here.  this is a special case since the
        // extended length bytes do not include their own length.
        for (int i = 0; i <= topDdmCollectionStack_; i++) {
            ddmCollectionLenStack_[i] -= length;
        }
        dssLength_ -= length;
    }

    protected final void startSameIdChainParse() throws DisconnectException {
        readDssHeader();
        netAgent_.clearSvrcod();
    }

    protected final void endOfSameIdChainData() throws DisconnectException {
        netAgent_.targetTypdef_ = netAgent_.originalTargetTypdef_;
        netAgent_.targetSqlam_ = netAgent_.orignalTargetSqlam_;

        if (this.topDdmCollectionStack_ != Reply.EMPTY_STACK) {
            agent_.accumulateChainBreakingReadExceptionAndThrow(
                new DisconnectException(agent_, 
                new ClientMessageId(SQLState.NET_COLLECTION_STACK_NOT_EMPTY)));
        }
        if (this.dssLength_ != 0) {
            agent_.accumulateChainBreakingReadExceptionAndThrow(
                new DisconnectException(agent_, 
                new ClientMessageId(SQLState.NET_DSS_NOT_ZERO)));
        }
        if (dssIsChainedWithSameID_ == true) {
            agent_.accumulateChainBreakingReadExceptionAndThrow(
                new DisconnectException(agent_, 
                new ClientMessageId(SQLState.NET_DSS_CHAINED_WITH_SAME_ID)));
        }
    }
    
    protected final int peekTotalColumnCount(int tripletLength) throws DisconnectException {
        int columnCount = 0;
        int offset = 0;
        int tripletType = FdocaConstants.CPT_TRIPLET_TYPE;
        while (tripletType == FdocaConstants.CPT_TRIPLET_TYPE) {
            columnCount += ((tripletLength - 3) / 3);
            // Peek ahead for the next triplet's tripletLength and tripletType.
            // The number of bytes to skip before the next tripletType is tripletLength - 3.
            ensureBLayerDataInBuffer(tripletLength - 3);
            offset += (tripletLength - 3);
            tripletLength = (buffer_[pos_ + offset++] & 0xff);
            tripletType = (buffer_[pos_ + offset++] & 0xff);
            // Skip the 1-byte tripletId.
            offset++;
        }
        return columnCount;
    }

    private final void peekExtendedLength() throws DisconnectException {
        peekedNumOfExtendedLenBytes_ = (peekedLength_ - 0x8004);
        switch (peekedNumOfExtendedLenBytes_) {
        case 4:
            // L   L   C   P  Extended Length
            // -->2-bytes<--  --->4-bytes<---
            // We are only peeking the length here, the actual pos_ is still before LLCP.  We ensured
            // 4-bytes in peedCodePoint() for the LLCP, and we need to ensure 4-bytes(of LLCP) + the
            // extended length bytes here.
            if (longBufferForDecryption_ == null) //we ddon't need to do this if it's data stream encryption
            {
                ensureBLayerDataInBuffer(4 + 4);
            }
            // The ddmScalarLen_ we peek here does not include the LLCP and the extended length bytes
            // themselves.  So we will add those back to the ddmScalarLen_ so it can be adjusted
            // correctly in parseLengthAndMatchCodePoint(). (since the adjustLengths() method will
            // subtract the length from ddmScalarLen_)
            peekedLength_ =
                    ((buffer_[pos_ + 4] & 0xff) << 24) +
                    ((buffer_[pos_ + 5] & 0xff) << 16) +
                    ((buffer_[pos_ + 6] & 0xff) << 8) +
                    ((buffer_[pos_ + 7] & 0xff) << 0);
            break;
        case 0:
            peekedLength_ = -1; // this ddm is streamed, so set -1 -> length unknown
            break;
        default:
            doSyntaxrmSemantics(CodePoint.SYNERRCD_INCORRECT_EXTENDED_LEN);
        }
    }

    final int readFastUnsignedByte() throws DisconnectException {
        return (buffer_[pos_++] & 0xff);
    }

    final short readFastShort() throws DisconnectException {
        short s = SignedBinary.getShort(buffer_, pos_);
        pos_ += 2;
        return s;
    }

    final int readFastUnsignedShort() throws DisconnectException {
        return ((buffer_[pos_++] & 0xff) << 8) +
                ((buffer_[pos_++] & 0xff) << 0);
    }

    final int readFastInt() throws DisconnectException {
        int i = SignedBinary.getInt(buffer_, pos_);
        pos_ += 4;
        return i;
    }

    final String readFastString(int length) throws DisconnectException {
        String result = ccsidManager_.convertToUCS2(buffer_, pos_, length);
        pos_ += length;
        return result;
    }

    final byte[] readFastBytes(int length) throws DisconnectException {
        byte[] b = new byte[length];
        System.arraycopy(buffer_, pos_, b, 0, length);
        pos_ += length;
        return b;
    }

    protected final int peekFastLength() throws DisconnectException {
        return (((buffer_[pos_] & 0xff) << 8) +
                ((buffer_[pos_ + 1] & 0xff) << 0));
    }

    final void skipFastBytes(int length) throws DisconnectException {
        pos_ += length;
    }

    final void readFastIntArray(int[] array) throws DisconnectException {
        for (int i = 0; i < array.length; i++) {
            array[i] = SignedBinary.getInt(buffer_, pos_);
            pos_ += 4;
        }
    }

    final String readFastString(int length, String encoding) throws DisconnectException {
        String s = null;

        try {
            s = new String(buffer_, pos_, length, encoding);
        } catch (java.io.UnsupportedEncodingException e) {
            agent_.accumulateChainBreakingReadExceptionAndThrow(
                new DisconnectException(agent_,
                    new ClientMessageId(SQLState.NET_ENCODING_NOT_SUPPORTED),
                    e));
        }
        pos_ += length;
        return s;
    }

    final byte[] readFastLDBytes() throws DisconnectException {
        int len = ((buffer_[pos_++] & 0xff) << 8) + ((buffer_[pos_++] & 0xff) << 0);
        if (len == 0) {
            return null;
        }

        byte[] b = new byte[len];
        System.arraycopy(buffer_, pos_, b, 0, len);
        pos_ += len;
        return b;
    }

    final long readFastLong() throws DisconnectException {
        long l = SignedBinary.getLong(buffer_, pos_);
        pos_ += 8;
        return l;
    }

    final byte readFastByte() throws DisconnectException {
        return (byte) (buffer_[pos_++] & 0xff);
    }

    final void mark() {
        currentPos_ = pos_;
    }

    // remove and return the top offset value from mark stack.
    final int popMark() {
        return currentPos_;
    }

    final int getFastSkipSQLCARDrowLength() {
        return pos_ - popMark();
    }

    // The only difference between this method and the original getData() method is this method
    // is not doing an ensureALayerDataInBuffer
    final ByteArrayOutputStream getFastData(ByteArrayOutputStream existingBuffer) throws DisconnectException {
        boolean readHeader;
        int copySize;
        ByteArrayOutputStream baos;

        // note: an empty baos can yield an allocated and empty byte[]
        if (existingBuffer != null) {
            baos = existingBuffer;
        } else {
            if (ddmScalarLen_ != -1) {
                // allocate a stream based on a known amount of data
                baos = new ByteArrayOutputStream(ddmScalarLen_);
            } else {
                // allocate a stream to hold an unknown amount of data
                baos = new ByteArrayOutputStream();
                //isLengthAndNullabilityUnknown = true;
            }
        }

        // set the amount to read for the first segment
        copySize = dssLength_; // note: has already been adjusted for headers

        do {
            // determine if a continuation header needs to be read after the data
            if (dssIsContinued_) {
                readHeader = true;
            } else {
                readHeader = false;
            }

            // read the segment
            //ensureALayerDataInBuffer (copySize);
            adjustLengths(copySize);
            baos.write(buffer_, pos_, copySize);
            pos_ += copySize;

            // read the continuation header, if necessary
            if (readHeader) {
                readDSSContinuationHeader();
            }

            copySize = dssLength_;
        } while (readHeader == true);

        return baos;
    }

    // This method is only used to match the codePoint for those class instance variables
    // that are embedded in other reply messages.
    final protected void matchCodePoint(int expectedCodePoint) throws DisconnectException {
        int actualCodePoint = 0;
        actualCodePoint = peekedCodePoint_;
        pos_ += 4;
        if (actualCodePoint != expectedCodePoint) {
            agent_.accumulateChainBreakingReadExceptionAndThrow(
                new DisconnectException(agent_, 
                    new ClientMessageId(SQLState.NET_NOT_EXPECTED_CODEPOINT), 
                    new Integer(actualCodePoint), 
                    new Integer(expectedCodePoint)));
        }
    }


    protected final int peekNumOfColumns() throws DisconnectException {
        // skip the 4-byte LLCP and any extended length bytes + 1-byte null sqlcagrp null indicator
        int offset = (4 + peekedNumOfExtendedLenBytes_ + 1);

        offset = skipSQLDHROW(offset);

        return SignedBinary.getShort(buffer_, pos_ + offset);
    }

    protected final boolean peekForNullSqlcagrp() {
        // skip the 4-byte LLCP and any extended length bytes
        int offset = (4 + peekedNumOfExtendedLenBytes_);
        int nullInd = buffer_[pos_ + offset] & 0xff;
        return (nullInd == CodePoint.NULLDATA);
    }

    private final int skipSQLDHROW(int offset) throws DisconnectException {
        int sqldhrowgrpNullInd = buffer_[pos_ + offset++] & 0xff;
        if (sqldhrowgrpNullInd == CodePoint.NULLDATA) {
            return offset;
        }

        offset += 12;

        // skip sqldrdbnam
        int stringLength = ((buffer_[pos_ + offset++] & 0xff) << 8) +
                ((buffer_[pos_ + offset++] & 0xff) << 0);
        offset += stringLength;

        // skip sqldschema
        stringLength = ((buffer_[pos_ + offset++] & 0xff) << 8) +
                ((buffer_[pos_ + offset++] & 0xff) << 0);
        offset += stringLength;

        stringLength = ((buffer_[pos_ + offset++] & 0xff) << 8) +
                ((buffer_[pos_ + offset++] & 0xff) << 0);
        offset += stringLength;

        return offset;
    }
}




