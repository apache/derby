/*

   Derby - Class org.apache.derby.impl.drda.DDMWriter

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

package org.apache.derby.impl.drda;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CoderResult;
import java.nio.charset.CodingErrorAction;
import java.sql.DataTruncation;
import java.sql.SQLException;
import java.util.Arrays;
import org.apache.derby.shared.common.reference.DRDAConstants;
import org.apache.derby.shared.common.reference.Property;
import org.apache.derby.iapi.services.io.DynamicByteArrayOutputStream;
import org.apache.derby.iapi.services.property.PropertyUtil;
import org.apache.derby.shared.common.sanity.SanityManager;

/**
//IC see: https://issues.apache.org/jira/browse/DERBY-5896
    The DDMWriter is used to write DRDA protocol.   The DRDA Protocol is
    described in the DDMReader class.
    For more details, see DRDA Volume 3 (Distributed Data Management(DDM)
        Architecture (DDS definition)
*/
class DDMWriter
{

    // number of nesting levels for collections.  We need to mark the length
    // location of the collection so that we can update it as we add more stuff
    // to the collection
    private final static int MAX_MARKS_NESTING = 10;

    // Default buffer size
    private final static int DEFAULT_BUFFER_SIZE = 32767;

    /**
     * The maximum length in bytes for strings sent by {@code writeLDString()},
     * which is the maximum unsigned integer value that fits in two bytes.
     */
    final static int MAX_VARCHAR_BYTE_LENGTH = 0xFFFF;

    /**
     * Output buffer.
     */
    private ByteBuffer buffer;

    // A saved mark in the stream is saved temporarily to revisit the location.
    private int[] markStack = new int[MAX_MARKS_NESTING];

    // top of the stack
    private int top;

    // CCSID manager for translation of strings in the protocol to UTF-8 and EBCDIC
    private EbcdicCcsidManager ebcdicCcsidManager;
    private Utf8CcsidManager utf8CcsidManager;
    
    // Current CCSID manager
    private CcsidManager ccsidManager;

    // DRDA connection thread for this writer
    private DRDAConnThread agent;

    //  This Object tracks the location of the current
    //  Dss header length bytes.    This is done so
    //  the length bytes can be automatically
    //  updated as information is added to this stream.
    private int dssLengthLocation;

    // Current correlation ID
    private int correlationID;

    // Next correlation ID
    private int nextCorrelationID;

    // is this DRDA protocol or CMD protocol
    private boolean isDRDAProtocol;
    // trace object of the associated session
    private DssTrace dssTrace;

    // Location of the start of the header
    // of the DSS most recently written to the buffer.
    private int prevHdrLocation;

    // Correlation id of the last DSS that was written to buffer.
    private int previousCorrId;

    // Chaining bit of the last DSS that was written to buffer.
    private byte previousChainByte;

    // Whether or not the current DSS is a continuation DSS.
    private boolean isContinuationDss;

    // In situations where we want to "mark" a buffer location so that
    // we can "back-out" of a write to handle errors, this holds the
    // location within the buffer of the start of the header
    // that immediately precedes the mark.
    private int lastDSSBeforeMark;

    /** Encoder which encodes strings with the server's default encoding. */
    private final CharsetEncoder encoder;

    // For JMX statistics. Volatile to ensure we 
    // get one complete long, but we don't bother to synchronize, 
    // since this is just statistics.
    
//IC see: https://issues.apache.org/jira/browse/DERBY-3435
    volatile long totalByteCount = 0;
    
//IC see: https://issues.apache.org/jira/browse/DERBY-728
//IC see: https://issues.apache.org/jira/browse/DERBY-5896
    DDMWriter (DRDAConnThread agent, DssTrace dssTrace)
    {
        // Create instances of the two ccsid managers and default to EBCDIC
        this.ebcdicCcsidManager = new EbcdicCcsidManager();
        this.utf8CcsidManager = new Utf8CcsidManager();
        this.ccsidManager = this.ebcdicCcsidManager;
        
//IC see: https://issues.apache.org/jira/browse/DERBY-2936
        this.buffer = ByteBuffer.allocate(DEFAULT_BUFFER_SIZE);
        this.agent = agent;
        this.prevHdrLocation = -1;
        this.previousCorrId = DssConstants.CORRELATION_ID_UNKNOWN;
        this.previousChainByte = DssConstants.DSS_NOCHAIN;
        this.isContinuationDss = false;
//IC see: https://issues.apache.org/jira/browse/DERBY-5
//IC see: https://issues.apache.org/jira/browse/DERBY-5
        this.lastDSSBeforeMark = -1;
        reset(dssTrace);
        // create an encoder which inserts the charset's default replacement
        // character for characters it can't encode
//IC see: https://issues.apache.org/jira/browse/DERBY-2936
        encoder = NetworkServerControlImpl.DEFAULT_CHARSET.newEncoder()
            .onMalformedInput(CodingErrorAction.REPLACE)
            .onUnmappableCharacter(CodingErrorAction.REPLACE);
    }

    // Switch the ccsidManager to the UTF-8 instance
    protected void setUtf8Ccsid() {
//IC see: https://issues.apache.org/jira/browse/DERBY-728
        ccsidManager = utf8CcsidManager;
    }
    
    // Switch the ccsidManager to the EBCDIC instance
    protected void setEbcdicCcsid() {
        ccsidManager = ebcdicCcsidManager;
    }
    
    // Get the current ccsidManager
    protected CcsidManager getCurrentCcsidManager() {
        return ccsidManager;
    }
    
    /**
     * reset values for sending next message
     *
     */
    protected void reset(DssTrace dssTrace)
    {
        buffer.clear();
        top = 0;
        dssLengthLocation = 0;
        nextCorrelationID = 1;
        correlationID = DssConstants.CORRELATION_ID_UNKNOWN;
        isDRDAProtocol = true;
        this.dssTrace = dssTrace;
    }

    /**
     * Get the current position in the output buffer.
     * @return current position
     */
    protected int getBufferPosition() {
        return buffer.position();
    }

    /**
     * Change the current position in the output buffer.
     * @param position new position
     */
    protected void setBufferPosition(int position) {
        buffer.position(position);
    }

    /**
     * Get a copy of a subsequence of the output buffer, starting at the
     * specified position and ending at the current buffer position.
     *
     * @param startPos the position of the first byte to copy
     * @return all bytes from {@code startPos} up to the current position
     */
    protected byte[] getBufferContents(int startPos) {
        byte[] bytes = new byte[buffer.position() - startPos];
        System.arraycopy(buffer.array(), startPos, bytes, 0, bytes.length);
        return bytes;
    }

    /**
     * set protocol to CMD protocol
     */
    protected void setCMDProtocol()
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-5896
        isDRDAProtocol = false;
    }

    /**
     * Create DSS reply object
     */
    protected void createDssReply()
    {
        beginDss(DssConstants.DSSFMT_RPYDSS, true);
    }

    /**
     * Create DSS request object
     * NOTE: This is _ONLY_ used for testing the protocol
    * (via the ProtocolTestAdapter.java file in this package)!
     * We should never create a DSS request in normal
     * DRDA processing (we should only create DSS replies
     * and DSS objects).
     */
    protected void createDssRequest()
    {
        beginDss(DssConstants.DSSFMT_RQSDSS, true);
    }

    /**
     * Create DSS data object
     */
    protected void createDssObject()
    {
        beginDss(DssConstants.DSSFMT_OBJDSS, true);
    }

    /**
     * Mark the DSS that we're currently writing as
     * a continued DSS, which is done by setting
     * the high-order bit to "1", per DDM spec.
     * This means:
     *
     *  1. One or more continuation DSSes will immediately
     *      follow the current (continued) DSS.
     *  2. All continuation DSSes will have a 2-byte
     *      continuation header, followed by data; in
     *      other words, chaining state, correlation
     *      id, dss format info, and code point will
     *      NOT be included.  All of that info is
     *      present ONLY in the FIRST DSS in the
     *      list of continued DSSes.
     *
     *  NOTE: A DSS can be a "continuation" DSS _and_
     *  a "continued" DSS at the same time.  However,
     *  the FIRST DSS to be continued canNOT be
     *  a continuation DSS.
     */
    private void markDssAsContinued(boolean forLob)
    {

        if (!forLob) {
        // continuation bit defaults to '1' for lobs, so
        // we only have to switch it if we're not writing
        // lobs.
//IC see: https://issues.apache.org/jira/browse/DERBY-2936
            byte b = (byte) (buffer.get(dssLengthLocation) | 0x80);
            buffer.put(dssLengthLocation, b);
        }

        // We need to set the chaining state, but ONLY
        // IF this is the FIRST DSS in the continuation
        // list (only the first one has chaining state
        // in it's header; the others do not).
        if (!isContinuationDss)
            endDss(!forLob);

    }

    /**
     * End DSS header by writing the length in the length location
     * and setting the chain bit.  Unlike the other two endDss
     * methods, this one overrides the default chaining byte
     * (which is set in beginDss) with the chaining byte that
     * is passed in.  NOTE: This method is only used in
     * association with createDssRequest, and thus is for
    * TESTING purposes only (via ProtocolTestAdpater.java).  No calls
     * should be made to this method in normal DRDA processing
     * (because for normal processing, chaining must be
     * determined automatically based on DSS requests).
     */
    protected void endDss(byte chainByte)
    {

        // Do regular endDss processing.
        endDss(true);

        // Now override default chain state.
//IC see: https://issues.apache.org/jira/browse/DERBY-2936
        overrideChainByte(dssLengthLocation + 3, chainByte);
        previousChainByte = chainByte;

    }

    /**
     * Override the default chaining byte with the chaining byte that is passed
     * in.
     *
     * @param pos the position on which the chaining byte is located
     * @param chainByte the chaining byte that overrides the default
     */
    private void overrideChainByte(int pos, byte chainByte) {
        byte b = buffer.get(pos);
        b &= 0x0F;              // Zero out default
        b |= chainByte;
        buffer.put(pos, b);
    }

    /**
     * End DSS header by writing the length in the length location
     * and setting the chain bit.
     */
    protected void endDss() {
//IC see: https://issues.apache.org/jira/browse/DERBY-1302
//IC see: https://issues.apache.org/jira/browse/DERBY-326
//IC see: https://issues.apache.org/jira/browse/DERBY-5896
        endDss(true);
    }

    /**
     * End DSS header by writing the length in the length location
     * and setting the chain bit.
     */
    private void endDss (boolean finalizeLength)
    {

        if (finalizeLength)
            finalizeDssLength();

        if (isContinuationDss) {
        // no chaining information for this DSS; so we're done.
            isContinuationDss = false;
            return;
        }

        previousCorrId = correlationID;
        prevHdrLocation = dssLengthLocation;
        previousChainByte = DssConstants.DSSCHAIN_SAME_ID;

    }

    /**
     * End final DDM and DSS header by writing the length in the length location
     *
     */
    protected void endDdmAndDss ()
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-5896
        endDdm();   // updates last DDM object
        endDss();
    }
    /**
     * Copy Data to End
     * Create a buffer and copy from the position given to the end of data
     *
     * Note that the position given is treated as relative to the
     * current DSS, for there may be other DSS blocks (chained, presumably)
     * which are sitting unwritten in the buffer. The caller doesn't
     * know this, though, and works only with the current DSS.
     *
     * getDSSLength, copyDSSDataToEnd, and truncateDSS work together to
     * provide a sub-protocol for DRDAConnThread to use in its
     * implementation of the LMTBLKPRC protocol. They enable the caller
     * to determine when it has written too much data into the current
     * DSS, to reclaim the extra data that won't fit, and to truncate
     * that extra data once it has been reclaimed and stored elsewhere.
     * Note that this support only works for the current DSS. Earlier,
     * chained DSS blocks cannot be accessed using these methods. For
     * additional background information, the interested reader should
     * investigate bugs DERBY-491 and 492 at:
     * http://issues.apache.org/jira/browse/DERBY-491 and
     * http://issues.apache.org/jira/browse/DERBY-492
     *
     * @param start
     */
    protected byte [] copyDSSDataToEnd(int start)
    {
        start = start + dssLengthLocation;
        int length = buffer.position() - start;
        byte [] temp = new byte[length];
//IC see: https://issues.apache.org/jira/browse/DERBY-2936
        buffer.position(start);
        buffer.get(temp);
        return temp;
    }

    // Collection methods

    /**
     * Mark the location of the length bytes for the collection so they
     * can be updated later
     *
     */
    protected void startDdm (int codePoint)
    {
        // save the location of the beginning of the collection so
        // that we can come back and fill in the length bytes
//IC see: https://issues.apache.org/jira/browse/DERBY-2936
        final int offset = buffer.position();
        markStack[top++] = offset;
        ensureLength (4); // verify space for length bytes and code point
        // move past the length bytes before writing the code point
        buffer.position(offset + 2);
        buffer.putShort((short) codePoint);
    }

    /**
     * Erase all writes for the current ddm and reset the
     * top
     */
    protected void clearDdm ()
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-2936
        buffer.position(markStack[top--]);
    }

    /**
     * Clear the entire send buffer
     *
     */
    protected void clearBuffer()
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-2936
//IC see: https://issues.apache.org/jira/browse/DERBY-2936
        buffer.clear();
        top = 0;
        dssLengthLocation = 0;
        correlationID = DssConstants.CORRELATION_ID_UNKNOWN;
        nextCorrelationID = 1;
        isDRDAProtocol = true;
    }

    /**
     * End the current DDM
     *
     */
    protected void endDdm ()
    {
        // remove the top length location offset from the mark stack
        // calculate the length based on the marked location and end of data.
        int lengthLocation = markStack[--top];
        int length = buffer.position() - lengthLocation;

        // determine if any extended length bytes are needed.   the value returned
        // from calculateExtendedLengthByteCount is the number of extended length
        // bytes required. 0 indicates no exteneded length.
        int extendedLengthByteCount = calculateExtendedLengthByteCount (length);
        if (extendedLengthByteCount != 0)
        {
            // ensure there is enough room in the buffer for the extended length bytes.
            ensureLength (extendedLengthByteCount);

            // calculate the length to be placed in the extended length bytes.
            // this length does not include the 4 byte llcp.
            int extendedLength = length - 4;

            // the extended length should be written right after the length and
            // the codepoint (2+2 bytes)
            final int extendedLengthLocation = lengthLocation + 4;
//IC see: https://issues.apache.org/jira/browse/DERBY-2936

            // shift the data to the right by the number of extended
            // length bytes needed.
            buffer.position(extendedLengthLocation + extendedLengthByteCount);
            buffer.put(buffer.array(), extendedLengthLocation, extendedLength);

            // write the extended length (a variable number of bytes in
            // big-endian order)
//IC see: https://issues.apache.org/jira/browse/DERBY-2936
            for (int pos = extendedLengthLocation + extendedLengthByteCount - 1;
                 pos >= extendedLengthLocation; pos--) {
                buffer.put(pos, (byte) extendedLength);
                extendedLength >>= 8;
            }

            // the two byte length field before the codepoint contains the length
            // of itself, the length of the codepoint, and the number of bytes used
            // to hold the extended length. the 2 byte length field also has the first
            // bit on to indicate extended length bytes were used.
            length = extendedLengthByteCount + 4;
            length |= DssConstants.CONTINUATION_BIT;
        }

        // write the 2 byte length field (2 bytes before codepoint).
//IC see: https://issues.apache.org/jira/browse/DERBY-2936
        buffer.putShort(lengthLocation, (short) length);
    }

    /**
     * Get the length of the current DSS block we're working on. This is
     * used by the LMTBLKPRC protocol, which does its own conversational
     * blocking protocol above the layer of the DRDA blocking. The LMTBLKPRC
     * implementation (in DRDAConnThread) needs to be able to truncate a
     * DSS block when splitting a QRYDTA response.
     *
     * @return current DSS block length
    */
    protected int getDSSLength()
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-2936
        return buffer.position() - dssLengthLocation;
    }
 
    /**
     * Truncate the current DSS. Before making this call, you should ensure
     * that you have copied the data to be truncated somewhere else, by
     * calling copyDSSDataToEnd
     *
     * @param value DSS length
    */
    protected void truncateDSS(int value)
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-2936
        buffer.position(dssLengthLocation + value);
    }


    // Write routines

    /**
     * Write byte
     *
     * @param   value   byte to be written
     */
    protected void writeByte (int value)
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-5896
        if (SanityManager.DEBUG)
        {
            if (value > 255)
                SanityManager.THROWASSERT(
                                       "writeByte value: " + value +
                                       " may not be > 255");
        }

        ensureLength (1);
        buffer.put((byte) value);
    }


    /**
     * Write network short
     *
     * @param   value   value to be written
     */
    protected void writeNetworkShort (int value)
    {
        ensureLength (2);
        buffer.putShort((short) value);
    }

    /**
     * Write network int
     *
     * @param   value   value to be written
     */
    protected void writeNetworkInt (int value)
    {
        ensureLength (4);
//IC see: https://issues.apache.org/jira/browse/DERBY-2936
        buffer.putInt(value);
    }


    /**
     * Write byte array
     *
     * @param   buf byte array to be written
     * @param   length  - length to write
     */
    protected void writeBytes (byte[] buf, int length)
    {
        writeBytes(buf, 0,length);
    }



    /**
     * Write byte array
     *
     * @param   buf byte array to be written
     * @param   start  - starting position
     * @param   length  - length to write
     */
    protected void writeBytes (byte[] buf, int start, int length)
    {

        if (SanityManager.DEBUG)
        {
            if (buf == null && length > 0)
                SanityManager.THROWASSERT("Buf is null");
            if (length + start - 1 > buf.length)
                SanityManager.THROWASSERT("Not enough bytes in buffer");

        }
        ensureLength (length);
//IC see: https://issues.apache.org/jira/browse/DERBY-2936
        buffer.put(buf, start, length);
    }
    /**
     * Write byte array
     *
     * @param   buf byte array to be written
     **/
    protected void writeBytes (byte[] buf)
    {
        writeBytes(buf,buf.length);
    }



    protected void writeLDBytes(byte[] buf)
    {
        writeLDBytes(buf, 0);
    }

    protected void writeLDBytes(byte[] buf, int index)
    {
        int writeLen =  buf.length;

        writeShort(writeLen);

        writeBytes(buf,0,writeLen);
    }


    /**
     * Write code point and 4 bytes
     *
     * @param   codePoint - code point to write
     * @param   value  - value to write after code point
     */
    void writeCodePoint4Bytes (int codePoint, int value)
    {
        ensureLength (4);
        buffer.putShort((short) codePoint);
        buffer.putShort((short) value);
    }

    /**
     * Write scalar 1 byte object includes length, codepoint and value
     *
     * @param   codePoint - code point to write
     * @param   value  - value to write after code point
     */
    void writeScalar1Byte (int codePoint, int value)
    {
        ensureLength (5);
//IC see: https://issues.apache.org/jira/browse/DERBY-2936
        buffer.putShort((short) 0x0005);
        buffer.putShort((short) codePoint);
//IC see: https://issues.apache.org/jira/browse/DERBY-2936
        buffer.put((byte) value);
    }

    /**
     * Write scalar 2 byte object includes length, codepoint and value
     *
     * @param   codePoint - code point to write
     * @param   value  - value to write after code point
     */
    protected void writeScalar2Bytes (int codePoint, int value)
    {
        ensureLength (6);
//IC see: https://issues.apache.org/jira/browse/DERBY-2936
        buffer.putShort((short) 0x0006);
//IC see: https://issues.apache.org/jira/browse/DERBY-2936
        buffer.putShort((short) codePoint);
        buffer.putShort((short) value);
    }

    protected void writeScalar2Bytes ( int value)
    {
        ensureLength (2);
//IC see: https://issues.apache.org/jira/browse/DERBY-2936
        buffer.putShort((short) value);
    }
    
    protected void writeScalarStream (boolean chainedWithSameCorrelator,
                                      int codePoint,
//IC see: https://issues.apache.org/jira/browse/DERBY-326
                      EXTDTAInputStream in,
                                      boolean writeNullByte) 
        throws DRDAProtocolException
    {

        

        // Stream equivalent of "beginDss"...
        int spareDssLength = prepScalarStream( chainedWithSameCorrelator,
                                            codePoint,
                                            writeNullByte);
        
        // write the data
                try {
                    
        OutputStream out = 
            placeLayerBStreamingBuffer( agent.getOutputStream() );
        
        boolean isLastSegment = false;
        
        while( !isLastSegment ){
            
            if( SanityManager.DEBUG ){
        
//IC see: https://issues.apache.org/jira/browse/DERBY-2054
            if( PropertyUtil.getSystemBoolean("derby.debug.suicideOfLayerBStreaming") )
                throw new IOException();
                }

            // read as many bytes as possible directly into the backing array
            final int offset = buffer.position();
//IC see: https://issues.apache.org/jira/browse/DERBY-2936
            final int bytesRead =
                in.read(buffer.array(), offset,
                        Math.min(spareDssLength, buffer.remaining()));

            // update the buffer position
            buffer.position(offset + bytesRead);

            spareDssLength -= bytesRead;

            isLastSegment = peekStream(in) < 0;
            
            if(isLastSegment || 
               spareDssLength == 0){
            
            flushScalarStreamSegment (isLastSegment, 
                          out);
            
            if( ! isLastSegment )
                spareDssLength = DssConstants.MAX_DSS_LENGTH - 2;

            }
            
        }
        
        out.flush();
        
        }catch(IOException e){
//IC see: https://issues.apache.org/jira/browse/DERBY-2933
        agent.markCommunicationsFailure (e,"DDMWriter.writeScalarStream()",
                         "",
                         e.getMessage(),
                         "*");
        }
                
    }
    
    /**
     * Begins a DSS stream (for writing LOB data).
     */
    private void beginDss (boolean chainedToNextStructure,
                           int dssType)
    {
        beginDss(dssType, false);   // false => don't ensure length.
//IC see: https://issues.apache.org/jira/browse/DERBY-5896

        // always turn on continuation flags... this is helpful for lobs...
        // these bytes will get rest if dss lengths are finalized.
        buffer.putShort(dssLengthLocation, (short) 0xFFFF);
//IC see: https://issues.apache.org/jira/browse/DERBY-2936

        // Set whether or not this DSS should be chained to
        // the next one.  If it's chained, it has to be chained
        // with same id (that's the nature of EXTDTA chaining).
        if (chainedToNextStructure) {
            dssType |= DssConstants.GDSCHAIN_SAME_ID;
        }

//IC see: https://issues.apache.org/jira/browse/DERBY-2936
        buffer.put(dssLengthLocation + 3, (byte) dssType);
    }


    /**
     * prepScalarStream does the following prep for writing stream data:
     * 1.  Flushes an existing DSS segment, if necessary
     * 2.  Determines if extended length bytes are needed
     * 3.  Creates a new DSS/DDM header and a null byte indicator, if applicable
     *
     * If value of length was less than 0, this method processes streaming as Layer B Streaming.
     * cf. page 315 of specification of DRDA, Version 3, Volume 3 
     *
     */
  private int prepScalarStream( boolean chainedWithSameCorrelator,
                                   int codePoint,
                                   boolean writeNullByte) throws DRDAProtocolException
  {

//IC see: https://issues.apache.org/jira/browse/DERBY-2936
      ensureLength( DEFAULT_BUFFER_SIZE - buffer.position() );
      
      final int nullIndicatorSize = writeNullByte ? 1:0;
//IC see: https://issues.apache.org/jira/browse/DERBY-326
//IC see: https://issues.apache.org/jira/browse/DERBY-1302
//IC see: https://issues.apache.org/jira/browse/DERBY-326

    
      // flush the existing DSS segment ,
      // if this stream will not fit in the send buffer or 
      // length of this stream is unknown.
      // Here, 10 stands for sum of headers of layer A and B.

      try {
        // The existing DSS segment was finalized by endDss; all
        // we have to do is send it across the wire.
        sendBytes(agent.getOutputStream());
      }
      catch (java.io.IOException e) {
         agent.markCommunicationsFailure ("DDMWriter.writeScalarStream()",
                                              "OutputStream.flush()",
                                              e.getMessage(),"*");
      }

    // buildStreamDss should not call ensure length.
    beginDss(chainedWithSameCorrelator, DssConstants.GDSFMT_OBJDSS);
//IC see: https://issues.apache.org/jira/browse/DERBY-5896

      writeLengthCodePoint(0x8004,codePoint);

//IC see: https://issues.apache.org/jira/browse/DERBY-326

    // write the null byte, if necessary
    if (writeNullByte)
      writeByte(0x0);

      //Here, 6 stands for header of layer A and 
      //4 stands for header of layer B.
      return DssConstants.MAX_DSS_LENGTH - 6 - 4 - nullIndicatorSize;


  }


  // method to determine if any data is in the request.
  // this indicates there is a dss object already in the buffer.
    protected boolean doesRequestContainData()
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-2936
//IC see: https://issues.apache.org/jira/browse/DERBY-5896
        return buffer.position() != 0;
    }


    // Writes out a scalar stream DSS segment, along with DSS continuation
    // headers if necessary.
    private void flushScalarStreamSegment ( boolean lastSegment,
//IC see: https://issues.apache.org/jira/browse/DERBY-326
                            OutputStream out)
        throws DRDAProtocolException
    {

        // either at end of data, end of dss segment, or both.
        if (! lastSegment) {

        // 32k segment filled and not at end of data.
                try {
                // Mark current DSS as continued, set its chaining state,
                // then send the data across.
//IC see: https://issues.apache.org/jira/browse/DERBY-5896
                    markDssAsContinued(true);   // true => for lobs
                    sendBytes (out,
                           false);
                
            }catch (java.io.IOException ioe) {
                    agent.markCommunicationsFailure ("DDMWriter.flushScalarStreamSegment()",
                                               "",
                                               ioe.getMessage(),
                                               "*");
                }


            // Prepare a DSS continuation header for next DSS.
//IC see: https://issues.apache.org/jira/browse/DERBY-2936
            dssLengthLocation = buffer.position();
            buffer.putShort((short) 0xFFFF);
            isContinuationDss = true;
        }else{
        // we're done writing the data, so end the DSS.
            endDss();

    }

  }


  // insert a 4 byte length/codepoint pair into the buffer.
  // total of 4 bytes inserted in buffer.
  // Note: the length value inserted in the buffer is the same as the value
  // passed in as an argument (this value is NOT incremented by 4 before being
  // inserted).
  void writeLengthCodePoint (int length, int codePoint)
  {
    ensureLength (4);
//IC see: https://issues.apache.org/jira/browse/DERBY-2936
//IC see: https://issues.apache.org/jira/browse/DERBY-2936
//IC see: https://issues.apache.org/jira/browse/DERBY-5896
    buffer.putShort((short) length);
    buffer.putShort((short) codePoint);
  }

    /**
     * Write scalar object header includes length and codepoint
     *
     * @param   codePoint - code point to write
     * @param   dataLength - length of object data
     */
    protected void writeScalarHeader (int codePoint, int dataLength)
    {
        ensureLength (dataLength + 4);
//IC see: https://issues.apache.org/jira/browse/DERBY-2936
        buffer.putShort((short) (dataLength + 4));
        buffer.putShort((short) codePoint);
    }

    /**
     * Write scalar string object includes length, codepoint and value
     * the string is converted into the appropriate codeset (EBCDIC)
     *
     * @param   codePoint - code point to write
     * @param   string - string to be written
     */
    void writeScalarString (int codePoint, String string)
    {
        int stringLength = ccsidManager.getByteLength(string);
        ensureLength ((stringLength * 2)  + 4);
        buffer.putShort((short) (stringLength + 4));
        buffer.putShort((short) codePoint);
        ccsidManager.convertFromJavaString(string, buffer);
    }

    /**
     * Write padded scalar string object includes length, codepoint and value
     * the string is converted into the appropriate codeset (EBCDIC)
     *
     * @param   codePoint - code point to write
     * @param   string - string to be written
     * @param   paddedLength - length to pad string to
     */
    void writeScalarPaddedString (int codePoint, String string, int paddedLength)
    {
        int stringLength = ccsidManager.getByteLength(string);
        int fillLength = paddedLength - stringLength;
        ensureLength (paddedLength + 4);
        buffer.putShort((short) (paddedLength + 4));
        buffer.putShort((short) codePoint);
//IC see: https://issues.apache.org/jira/browse/DERBY-728
//IC see: https://issues.apache.org/jira/browse/DERBY-728
//IC see: https://issues.apache.org/jira/browse/DERBY-728
        ccsidManager.convertFromJavaString(string, buffer);
        padBytes(ccsidManager.space, fillLength);
    }

    /**
     * Write padded scalar <code>DRDAString</code> object value. The
     * string is converted into the appropriate codeset.
     *
     * @param drdaString string to be written
     * @param paddedLength length to pad string to
     */
    protected void writeScalarPaddedString (DRDAString drdaString, int paddedLength)
    {
        /* This .length() call is valid as this is a DRDAString */
        int stringLength = drdaString.length();
        int fillLength = paddedLength - stringLength;
        ensureLength(paddedLength);
        buffer.put(drdaString.getBytes(), 0, stringLength);
//IC see: https://issues.apache.org/jira/browse/DERBY-2936
        padBytes(ccsidManager.space, fillLength);
    }

    /**
     * Write padded scalar byte array object includes length, codepoint and value
     *
     * @param   codePoint - code point to write
     * @param   buf - byte array to be written
     * @param   paddedLength - length to pad string to
     * @param   padByte - byte to be used for padding
     */
    protected void writeScalarPaddedBytes (int codePoint, byte[] buf, int paddedLength, byte padByte)
    {
        ensureLength (paddedLength + 4);
        buffer.putShort((short) (paddedLength + 4));
        buffer.putShort((short) codePoint);
        buffer.put(buf);
        padBytes(padByte, paddedLength - buf.length);
    }

    /**
     * Write padded scalar byte array object  value
     *
     * @param   buf - byte array to be written
     * @param   paddedLength - length to pad string to
     * @param   padByte - byte to be used for padding
     */
    protected void writeScalarPaddedBytes (byte[] buf, int paddedLength, byte padByte)
    {
        ensureLength (paddedLength);
        buffer.put(buf);
//IC see: https://issues.apache.org/jira/browse/DERBY-2936
//IC see: https://issues.apache.org/jira/browse/DERBY-2936
        padBytes(padByte, paddedLength - buf.length);
    }

    /**
     * Write scalar byte array object includes length, codepoint and value
     *
     * @param   codePoint - code point to write
     * @param   buf - byte array to be written
     */
    protected void writeScalarBytes (int codePoint, byte[] buf)
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-2936
        ensureLength(buf.length + 4);
        buffer.putShort((short) (buf.length + 4));
        buffer.putShort((short) codePoint);
        buffer.put(buf);
    }

    // The following methods write data in the platform format
    // The platform format was indicated during connection time as ASC since
    // JCC doesn't read JVM platform (yet)

    /**
     * Write platform short
     *
     * @param   v   value to be written
     */
    protected void writeShort (int v)
    {
        writeNetworkShort(v);
    }

    /**
     * Write boolean as short
     * @param b boolean value true = 1 false = 0
     *
     */
    protected void writeShort(boolean b)
    {
        writeNetworkShort(b ? 1 : 0);
    }

    /**
     * Write platform int
     *
     * @param   v   value to be written
     */
    protected void writeInt (int v)
    {
        writeNetworkInt(v);
    }

    /**
     * Write platform long
     *
     * @param   v   value to be written
     */
    protected void writeLong (long v)
    {
        ensureLength (8);
//IC see: https://issues.apache.org/jira/browse/DERBY-2936
        buffer.putLong(v);
    }

    /**
     * Write platform float
     *
     * @param   v   value to be written
     */
    protected void writeFloat (float v)
    {
        writeInt (Float.floatToIntBits (v));
    }

    /**
     * Write platform double
     *
     * @param   v   value to be written
     */
    protected void writeDouble (double v)
    {
        writeLong (Double.doubleToLongBits (v));
    }

    /**
     * Write platform boolean
     *
     * @param   v   value to be written
     */
    protected void writeBoolean (boolean v)
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-2936
        writeByte(v ? 1 : 0);
    }

    /**
     * Write length delimited string
     *
     * @param s value to be written with integer
     *
     * @exception DRDAProtocolException
     */
    protected void writeLDString(String s) throws DRDAProtocolException
    {
        writeLDString(s, 0, null, false);
    }

    /**
     * Write a value of a user defined type.
     *
     * @param val object to be written
     *
     * @exception DRDAProtocolException
     */
    protected void writeUDT( Object val, int index ) throws DRDAProtocolException
    {
        // should not be called if val is null
        if (SanityManager.DEBUG)
        {
            if ( val == null )
            {
                SanityManager.THROWASSERT( "UDT is null" );
            }
        }

        byte[] buffer = null;
        int length = 0;

        try {
            DynamicByteArrayOutputStream dbaos = new DynamicByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream( dbaos );

            oos.writeObject( val );

            buffer = dbaos.getByteArray();
            length = dbaos.getUsed();
            
        } catch(IOException e)
        {
            agent.markCommunicationsFailure
                ( e,"DDMWriter.writeUDT()", "", e.getMessage(), "" );
        }

        if ( length > DRDAConstants.MAX_DRDA_UDT_SIZE )
        {
            agent.markCommunicationsFailure
                ( "DDMWriter.writeUDT()", "User defined type is longer than " + DRDAConstants.MAX_DRDA_UDT_SIZE + " bytes.", "", "" );
        }
        else
        {
            writeShort( length );
            writeBytes( buffer, 0, length );
        }
    }

    /**
     * Find the maximum number of bytes needed to represent the string in the
     * default encoding.
     *
     * @param s the string to encode
     * @return an upper limit for the number of bytes needed to encode the
     * string
     */
    private int maxEncodedLength(String s) {
        // maxBytesPerChar() returns a float, which can only hold 24 bits of an
        // integer. Therefore, promote the float to a double so that all bits
        // are preserved in the intermediate result.
//IC see: https://issues.apache.org/jira/browse/DERBY-2936
//IC see: https://issues.apache.org/jira/browse/DERBY-5896
        return (int) (s.length() * (double) encoder.maxBytesPerChar());
    }

    /**
     * Write length delimited string
     *
     * @param s              value to be written with integer
     * @param index          column index to put in warning
     * @param stmt           the executing statement (null if not invoked as
     *                       part of statement execution)
     * @param isParameter    true if the value written is for an output
     *                       parameter in a procedure call
     * @exception DRDAProtocolException
     */
    protected void writeLDString(String s, int index, DRDAStatement stmt,
                                 boolean isParameter)
            throws DRDAProtocolException
    {
        // Position on which to write the length of the string (in bytes). The
        // actual writing of the length is delayed until we have encoded the
        // string.
        final int lengthPos = buffer.position();

        // Reserve two bytes for the length field and move the position to
        // where the string should be inserted.
        ensureLength(2);
        final int stringPos = lengthPos + 2;
        buffer.position(stringPos);

        // Write the string.
        writeString(s);

        // Find out how long strings the client supports, and possibly
        // truncate the string before sending it.

        int maxByteLength = MAX_VARCHAR_BYTE_LENGTH;
        boolean warnOnTruncation = true;

        AppRequester appRequester = agent.getSession().appRequester;
        if (appRequester != null && !appRequester.supportsLongerLDStrings()) {
            // The client suffers from DERBY-5236, and it doesn't support
            // receiving as long strings as newer clients do. It also doesn't
            // know exactly what to do with a DataTruncation warning, so skip
            // sending it to old clients.
            maxByteLength = FdocaConstants.LONGVARCHAR_MAX_LEN;
            warnOnTruncation = false;
        }

        int byteLength = buffer.position() - stringPos;

        // If the byte representation of the string is too long, it needs to
        // be truncated.
        if (byteLength > maxByteLength) {
            // Truncate the string down to the maximum byte length.
            byteLength = maxByteLength;
            // Align with character boundaries so that we don't send over
            // half a character.
            while (isContinuationByte(buffer.get(stringPos + byteLength))) {
                byteLength--;
            }

            // Check how many chars that were truncated.
            int truncatedChars = 0;
            for (int i = stringPos + byteLength; i < buffer.position(); i++) {
                if (!isContinuationByte(buffer.get(i))) {
                    truncatedChars++;
                }
            }

            // Set the buffer position right after the truncated string.
            buffer.position(stringPos + byteLength);

            // If invoked as part of statement execution, and the client
            // supports receiving DataTruncation warnings, add a warning about
            // the string being truncated.
            if (warnOnTruncation && stmt != null) {
                DataTruncation dt = new DataTruncation(
                        index,
                        isParameter,
                        true,  // this is a warning for a read operation
                        s.length(),                   // dataSize
                        s.length() - truncatedChars); // transferSize
                stmt.addTruncationWarning(dt);
            }
        }

        // Go back and write the length in bytes.
        buffer.putShort(lengthPos, (short) byteLength);
    }

    /**
     * Check if a byte value represents a continuation byte in a UTF-8 byte
     * sequence. Continuation bytes in UTF-8 always match the bit pattern
     * {@code 10xxxxxx}.
     *
     * @param b the byte to check
     * @return {@code true} if {@code b} is a continuation byte, or
     * {@code false} if it is the first byte in a UTF-8 sequence
     */
    private static boolean isContinuationByte(byte b) {
        // Check the values of the two most significant bits. If they are
        // 10xxxxxx, it's a continuation byte.
        return (b & 0xC0) == 0x80;
    }

    /**
     * Write string with default encoding
     *
     * @param s value to be written
     *
     * @exception DRDAProtocolException
     */
    protected void writeString(String s) throws DRDAProtocolException
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-2936
//IC see: https://issues.apache.org/jira/browse/DERBY-5896
        ensureLength(maxEncodedLength(s));
        CharBuffer input = CharBuffer.wrap(s);
//IC see: https://issues.apache.org/jira/browse/DERBY-5331
//IC see: https://issues.apache.org/jira/browse/DERBY-5331
        encoder.reset();
        CoderResult res = encoder.encode(input, buffer, true);
        if (res == CoderResult.UNDERFLOW) {
            res = encoder.flush(buffer);
        }
        if (SanityManager.DEBUG) {
            SanityManager.ASSERT(res == CoderResult.UNDERFLOW,
                                 "CharBuffer was not exhausted: res = " + res);
        }
    }

    /**
     * Write pad bytes using spaceChar
     *
     * @param   val value to be written
     * @param   length      length to be written
     */
    protected void padBytes (byte val, int length)
    {
        final int offset = buffer.position();
        final int end = offset + length;
//IC see: https://issues.apache.org/jira/browse/DERBY-2936
        Arrays.fill(buffer.array(), offset, end, val);
        buffer.position(end);
    }

    /**
     * Flush buffer to outputstream
     *
     *
     * @exception IOException
     */
    protected void flush () throws java.io.IOException
    {
        flush(agent.getOutputStream());
    }

    /**
     * Flush buffer to specified stream
     *
     * @param socketOutputStream
     *
     * @exception IOException
     */
    protected void flush(OutputStream socketOutputStream)
        throws java.io.IOException
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-2936
        final byte[] bytes = buffer.array();
        final int length = buffer.position();
        try {
            socketOutputStream.write (bytes, 0, length);
            socketOutputStream.flush();
        }
        finally {
            if ((dssTrace != null) && dssTrace.isComBufferTraceOn()) {
              dssTrace.writeComBufferData (bytes,
                                           0,
                                           length,
                                           DssTrace.TYPE_TRACE_SEND,
                                           "Reply",
                                           "flush",
                                           5);
            }
            reset(dssTrace);
        }
//IC see: https://issues.apache.org/jira/browse/DERBY-3435
        totalByteCount += length;
    }

    // private methods

    /**
     * Write DSS header
     * DSS Header format is
     *  2 bytes - length
     *  1 byte  - 'D0'  - indicates DDM data
     *  1 byte  - DSS format
     *      |---|---------|----------|
     *      | 0 |   flags | type     |
     *      |---|---------|----------|
     *      | 0 | 1 2   3 | 4 5 6 7  |
     *      |---|---------|----------|
     *      bit 0 - '0'
     *      bit 1 - '0' - unchained, '1' - chained
     *      bit 2 - '0' - do not continue on error, '1' - continue on error
     *      bit 3 - '0' - next DSS has different correlator, '1' - next DSS has
     *                      same correlator
     *      type - 1 - Request DSS
     *           - 2 - Reply DSS
     *           - 3 - Object DSS
     *           - 4 - Communications DSS
     *           - 5 - Request DSS where no reply is expected
     */
    private void beginDss (int dssType, boolean ensureLen)
    {

        // save length position, the length will be written at the end
        dssLengthLocation = buffer.position();
//IC see: https://issues.apache.org/jira/browse/DERBY-2936

        // Should this really only be for non-stream DSSes?
        if (ensureLen)
            ensureLength(6);

        // Skip past length; we'll come back and set it later.
        buffer.position(dssLengthLocation + 2);

        // write gds info
        buffer.put((byte) 0xD0);

        // Write DSS type, and default chain bit to be 
        // DssConstants.DSSCHAIN_SAME_ID.  This default
        // will be overridden by calls to "finalizeChain()"
        // and/or calls to "beginDss(boolean, int)" for
        // writing LOB data.
        buffer.put((byte) (dssType | DssConstants.DSSCHAIN_SAME_ID));

        // save correlationID for use in error messages while processing
        // this DSS
        correlationID = getCorrelationID();

        // write the reply correlation id
        buffer.putShort((short) correlationID);
    }

    /**
     * Finish a DSS Layer A object.
     * The length of dss object will be calculated based on the difference between the
     * start of the dss, saved on the beginDss call, and the current
     * offset into the buffer which marks the end of the data.  In the event
     * the length requires the use of continuation Dss headers, one for each 32k
     * chunk of data, the data will be shifted and the continuation headers
     * will be inserted with the correct values as needed.
     */
    private void finalizeDssLength ()
    {
        // initial position in the byte buffer
        final int offset = buffer.position();
//IC see: https://issues.apache.org/jira/browse/DERBY-2936

        // calculate the total size of the dss and the number of bytes which would
        // require continuation dss headers.    The total length already includes the
        // the 6 byte dss header located at the beginning of the dss.   It does not
        // include the length of any continuation headers.
        int totalSize = offset - dssLengthLocation;
        int bytesRequiringContDssHeader = totalSize - DssConstants.MAX_DSS_LENGTH;

        // determine if continuation headers are needed
        if (bytesRequiringContDssHeader > 0)
        {
            // the continuation headers are needed, so calculate how many.
            // after the first 32767 worth of data, a continuation header is
            // needed for every 32765 bytes (32765 bytes of data + 2 bytes of
            // continuation header = 32767 Dss Max Size).
            int contDssHeaderCount = bytesRequiringContDssHeader / 32765;
            if (bytesRequiringContDssHeader % 32765 != 0)
                contDssHeaderCount++;

            // right now the code will shift to the right.  In the future we may want
            // to try something fancier to help reduce the copying (maybe keep
            // space in the beginning of the buffer??).
            // the offset points to the next available offset in the buffer to place
            // a piece of data, so the last dataByte is at offset -1.
            // various bytes will need to be shifted by different amounts
            // depending on how many dss headers to insert so the amount to shift
            // will be calculated and adjusted as needed.   ensure there is enough room
            // for all the conutinuation headers and adjust the offset to point to the
            // new end of the data.
            int dataByte = offset - 1;
            int shiftSize = contDssHeaderCount * 2;
            ensureLength (shiftSize);

            // We're going to access the buffer with absolute positions, so
            // just move the current position pointer right away to where it's
            // supposed to be after we have finished the shifting.
            buffer.position(offset + shiftSize);
//IC see: https://issues.apache.org/jira/browse/DERBY-2936

            // Notes on the behavior of the Layer B segmenting loop below:
            //
            // We start with the right most chunk. For a 3-segment object we'd
            // shift 2 segments: shift the first (rightmost) one 4 bytes and 
            // the second one 2. Note that by 'first' we mean 'first time
            // through the loop', but that is actually the last segment
            // of data since we are moving right-to-left. For an object
            // of K segments we will pass through this loop K-1 times.
            // The 0th (leftmost) segment is not shifted, as it is
            // already in the right place. When we are done, we will
            // have made room in each segment for an additional
            // 2 bytes for the continuation header. Thus, each
            // segment K is shifted K*2 bytes to the right.
            //
            // Each time through the loop, "dataByte" points to the
            // last byte in the segment; "dataToShift" is the amount of
            // data that we need to shift, and "shiftSize" is the
            // distance that we need to shift it. Since dataByte points
            // at the last byte, not one byte beyond it (as with the
            // "offset" variable used elsewhere in DDMWriter), the start
            // of the segement is actually at (dataByte-dataToShift+1).
            //
            // After we have shifted the segment, we move back to the
            // start of the segment and set the value of the 2-byte DSS
            // continuation header, which needs to hold the length of
            // this segment's data, together with the continuation flag
            // if this is not the rightmost (passOne) segment.
            //
            // In general, each segment except the rightmost will contain
            // 32765 bytes of data, plus the 2-byte header, and its
            // continuation flag will be set, so the header value will
            // be 0xFFFF. The rightmost segment will not have the
            // continuation flag set, so its value may be anything from
            // 0x0001 to 0x7FFF, depending on the amount of data in that
            // segment.
            //
            // Note that the 0th (leftmost) segment also has a 2-byte
            // DSS header, which needs to have its continuation flag set.
            // This is done by resetting the "totalSize" variable below,
            // at which point that variable no longer holds the total size
            // of the object, but rather just the length of segment 0. The
            // total size of the object was written using extended length
            // bytes by the endDdm() method earlier.
            //
            // Additional information about this routine is available in the
            // bug notes for DERBY-125:
            // http://issues.apache.org/jira/browse/DERBY-125
            
            // mark passOne to help with calculating the length of the final (first or
            // rightmost) continuation header.
            boolean passOne = true;
            do {
                // calculate chunk of data to shift
                int dataToShift = bytesRequiringContDssHeader % 32765;
                if (dataToShift == 0)
                    dataToShift = 32765;
                int startOfCopyData = dataByte - dataToShift + 1;
                // perform the shift directly on the backing array
//IC see: https://issues.apache.org/jira/browse/DERBY-2936
                final byte[] bytes = buffer.array();
                System.arraycopy(bytes,startOfCopyData, bytes, 
                                 startOfCopyData + shiftSize, dataToShift);
                dataByte -= dataToShift;


                // calculate the value the value of the 2 byte continuation dss
                // header which includes the length of itself.  On the first pass,
                // if the length is 32767
                // we do not want to set the continuation dss header flag.
                int twoByteContDssHeader = dataToShift + 2;
                if (passOne)
                    passOne = false;
                else
                {
                    if (twoByteContDssHeader == DssConstants.MAX_DSS_LENGTH)
                    twoByteContDssHeader = (twoByteContDssHeader |
                        DssConstants.CONTINUATION_BIT);

                }

                // insert the header's length bytes
//IC see: https://issues.apache.org/jira/browse/DERBY-2936
                buffer.putShort(dataByte + shiftSize - 1,
                                (short) twoByteContDssHeader);

                // adjust the bytesRequiringContDssHeader and the amount to shift for
                // data in upstream headers.
                bytesRequiringContDssHeader -= dataToShift;
                shiftSize -= 2;

                // shift and insert another header for more data.
            }
            while (bytesRequiringContDssHeader > 0);

            // set the continuation dss header flag on for the first header
            totalSize = (DssConstants.MAX_DSS_LENGTH |
                    DssConstants.CONTINUATION_BIT);


        }

        // insert the length bytes in the 6 byte dss header.
//IC see: https://issues.apache.org/jira/browse/DERBY-2936
        buffer.putShort(dssLengthLocation, (short) totalSize);
    }

    protected void writeExtendedLength(long size)
    {
        int numbytes = calculateExtendedLengthByteCount(size);
        if (size > 0)
            writeInt(0x8000 | numbytes);
        else
            writeInt(numbytes);
    }


    /**
     * Calculate extended length byte count which follows the DSS header
     * for extended DDM.
     *
     * @param ddmSize - size of DDM command
     * @return minimum number of extended length bytes needed. 0 indicates no
     *  extended length needed.
     */
    private int calculateExtendedLengthByteCount (long ddmSize)
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-1302
//IC see: https://issues.apache.org/jira/browse/DERBY-326
        if (ddmSize <= 0x7fff)
            return 0;
        // JCC does not support 2 at this time, so we always send
        // at least 4
        //      else if (ddmSize <= 0xffff)
        //  return 2;
        else if (ddmSize <= 0xffffffffL)
            return 4;
        else if (ddmSize <= 0xffffffffffffL)
            return 6;
        else if (ddmSize <= 0x7fffffffffffffffL)
            return 8;
        else
            // shouldn't happen
            // XXX - add sanity debug stuff here
            return 0;
    }

    /**
     * Ensure that there is space in the buffer
     *
     * @param length space required
     */
    private void ensureLength (int length)
    {
        if (buffer.remaining() < length) {
            if (SanityManager.DEBUG)
            {
                agent.trace("DANGER - Expensive expansion of  buffer");
            }
            int newLength =
                Math.max(buffer.capacity() * 2, buffer.position() + length);
            // copy the old buffer into a new one
            buffer.flip();
            buffer = ByteBuffer.allocate(newLength).put(buffer);
        }
    }


    /**
     * Write a Java <code>java.math.BigDecimal</code> to packed decimal bytes.
     *
     * @param b BigDecimal to write
     * @param precision Precision of decimal or numeric type
     * @param scale declared scale
     *
     * @exception SQLException Thrown if # digits &gt; 31
     */
//IC see: https://issues.apache.org/jira/browse/DERBY-2936
    void writeBigDecimal(BigDecimal b, int precision, int scale)
    throws SQLException
    {
        final int encodedLength = precision / 2 + 1;
        ensureLength(encodedLength);

        // The bytes are processed from right to left. Therefore, save starting
        // offset and use absolute positioning.
//IC see: https://issues.apache.org/jira/browse/DERBY-2936
//IC see: https://issues.apache.org/jira/browse/DERBY-2936
        final int offset = buffer.position();
        // Move current position to the end of the encoded decimal.
        buffer.position(offset + encodedLength);

        int declaredPrecision = precision;
        int declaredScale = scale;

        // packed decimal may only be up to 31 digits.
//IC see: https://issues.apache.org/jira/browse/DERBY-5896
        if (declaredPrecision > 31) // this is a bugcheck only !!!
        {
//IC see: https://issues.apache.org/jira/browse/DERBY-1302
//IC see: https://issues.apache.org/jira/browse/DERBY-326
            clearDdm ();
            throw new java.sql.SQLException ("Packed decimal may only be up to 31 digits!");
        }

        // get absolute unscaled value of the BigDecimal as a String.
        String unscaledStr = b.unscaledValue().abs().toString();

        // get precision of the BigDecimal.
        int bigPrecision = unscaledStr.length();

        if (bigPrecision > 31)
        {
            clearDdm ();
//IC see: https://issues.apache.org/jira/browse/DERBY-5896
            throw new SQLException ("The numeric literal \"" +
                             b.toString() +
                             "\" is not valid because its value is out of range.",
                             "42820",
                             -405);
        }
        int bigScale = b.scale();
        int bigWholeIntegerLength = bigPrecision - bigScale;
        if ( (bigWholeIntegerLength > 0) && (!unscaledStr.equals ("0")) ) {
            // if whole integer part exists, check if overflow.
            int declaredWholeIntegerLength = declaredPrecision - declaredScale;
            if (bigWholeIntegerLength > declaredWholeIntegerLength)
            {
                clearDdm ();
                throw new SQLException ("Overflow occurred during numeric data type conversion of \"" +
                                       b.toString() +
                                       "\".",
                                       "22003",
                                       -413);
            }
        }

        // convert the unscaled value to a packed decimal bytes.

        // get unicode '0' value.
        int zeroBase = '0';

        // start index in target packed decimal.
        int packedIndex = declaredPrecision-1;

        // start index in source big decimal.
        int bigIndex;

        byte signByte = (byte) ((b.signum() >= 0) ? 12 : 13);
//IC see: https://issues.apache.org/jira/browse/DERBY-2936

        if (bigScale >= declaredScale) {
          // If target scale is less than source scale,
          // discard excessive fraction.

          // set start index in source big decimal to ignore excessive fraction.
          bigIndex = bigPrecision-1-(bigScale-declaredScale);

//IC see: https://issues.apache.org/jira/browse/DERBY-2936
          if (bigIndex >= 0) {
              // process the last nybble together with the sign nybble.
              signByte |= (unscaledStr.charAt(bigIndex) - zeroBase) << 4;
          }
          buffer.put(offset + (packedIndex+1)/2, signByte);
          packedIndex-=2;
          bigIndex-=2;
        }
        else {
          // If target scale is greater than source scale,
          // pad the fraction with zero.

          // set start index in source big decimal to pad fraction with zero.
          bigIndex = declaredScale-bigScale-1;

          // process the sign nybble.
          buffer.put(offset + (packedIndex+1)/2, signByte);
//IC see: https://issues.apache.org/jira/browse/DERBY-2936

          for (packedIndex-=2, bigIndex-=2; bigIndex>=0; packedIndex-=2, bigIndex-=2)
              buffer.put(offset + (packedIndex+1)/2, (byte) 0);

          if (bigIndex == -1) {
            byte bt = (byte)
                ((unscaledStr.charAt(bigPrecision - 1) - zeroBase) << 4);
            buffer.put(offset + (packedIndex+1)/2, bt);
            packedIndex-=2;
            bigIndex = bigPrecision-3;
          }
          else {
            bigIndex = bigPrecision-2;
          }
        }

        // process the rest.
        for (; bigIndex>=0; packedIndex-=2, bigIndex-=2) {
//IC see: https://issues.apache.org/jira/browse/DERBY-2936
            byte bt = (byte)
                (((unscaledStr.charAt(bigIndex)-zeroBase) << 4) | // high nybble
                  (unscaledStr.charAt(bigIndex+1)-zeroBase));     // low nybble
            buffer.put(offset + (packedIndex+1)/2, bt);
        }

        // process the first nybble when there is one left.
        if (bigIndex == -1) {
            buffer.put(offset + (packedIndex+1)/2,
                       (byte) (unscaledStr.charAt(0) - zeroBase));

            packedIndex-=2;
        }

        // pad zero in front of the big decimal if necessary.
        for (; packedIndex>=-1; packedIndex-=2)
            buffer.put(offset + (packedIndex+1)/2, (byte) 0);
    }


    private void sendBytes (java.io.OutputStream socketOutputStream) 
//IC see: https://issues.apache.org/jira/browse/DERBY-326
//IC see: https://issues.apache.org/jira/browse/DERBY-5896
    throws java.io.IOException{
    
    sendBytes(socketOutputStream,
          true);
    
    }
    

  private void sendBytes (java.io.OutputStream socketOutputStream,
              boolean flashStream ) 
//IC see: https://issues.apache.org/jira/browse/DERBY-1302
//IC see: https://issues.apache.org/jira/browse/DERBY-326
      throws java.io.IOException
  {
    resetChainState();
//IC see: https://issues.apache.org/jira/browse/DERBY-2936
    final byte[] bytes = buffer.array();
    final int length = buffer.position();
    try {
      socketOutputStream.write(bytes, 0, length);
//IC see: https://issues.apache.org/jira/browse/DERBY-3435
      totalByteCount += length;
      if(flashStream)
      socketOutputStream.flush();
    }
    finally {
        if ((dssTrace != null) && dssTrace.isComBufferTraceOn()) {
            dssTrace.writeComBufferData (bytes,
                                           0,
//IC see: https://issues.apache.org/jira/browse/DERBY-2936
                                           length,
                                           DssTrace.TYPE_TRACE_SEND,
                                           "Reply",
                                           "flush",
                                           5);
      }
      clearBuffer();
    }
  }

    /**
     * Reset any chaining state that needs to be reset
     * at time of the send
     */
    protected void resetChainState()
    {
        prevHdrLocation = -1;
    }

    /**
     * Looks at chaining info for previous DSS written, and use
     * that to figure out what the correlation id for the current
     * DSS should be.  Return that correlation id.
     */
    private int getCorrelationID() {

        int cId;
//IC see: https://issues.apache.org/jira/browse/DERBY-1302
//IC see: https://issues.apache.org/jira/browse/DERBY-326
        if (previousCorrId != DssConstants.CORRELATION_ID_UNKNOWN) {
            if (previousChainByte == DssConstants.DSSCHAIN_SAME_ID)
            // then we have to use the last correlation id we sent.
                cId = previousCorrId;
            else
            // get correlation id as normal.
                cId = nextCorrelationID++;
        }
        else {
        // must be the case that this is the first DSS we're
        // writing for this connection (because we haven't
        // called "endDss" yet).  So, get the corr id as
        // normal.
            cId = nextCorrelationID++;
        }

        return cId;

    }

    /**
     * Finalize the current DSS chain and send it if
     * needed.
     *
     * Updates the chaining state of the most recently-written-
     * to-buffer DSS to correspond to the most recently-read-
     * from-client request.  If that chaining state indicates
     * we've reached the end of a chain, then we go ahead
     * and send the buffer across the wire.
     * @param socketOutputStream Output stream to which we're flushing.
     */
    protected void finalizeChain(byte currChainByte,
        OutputStream socketOutputStream) throws DRDAProtocolException
    {

        // Go back to previous DSS and override the default
        // chain state (WITH_SAME_ID) with whatever the last
        // request dictates.

//IC see: https://issues.apache.org/jira/browse/DERBY-1302
//IC see: https://issues.apache.org/jira/browse/DERBY-326
        if (prevHdrLocation != -1) {
        // Note: == -1 => the previous DSS was already sent; this
        // should only happen in cases where the buffer filled up
        // and we had to send it (which means we were probably
        // writing EXTDTA).  In such cases, proper chaining
        // should already have been handled @ time of send.
//IC see: https://issues.apache.org/jira/browse/DERBY-2936
            overrideChainByte(prevHdrLocation + 3, currChainByte);
        }

        // previousChainByte needs to match what we just did.
        previousChainByte = currChainByte;

        if (currChainByte != DssConstants.DSS_NOCHAIN)
        // then we're still inside a chain, so don't send.
            return;

        // Else, we just ended the chain, so send it across.

        if ((SanityManager.DEBUG) && (agent != null))
            agent.trace("Sending data");

        resetChainState();
//IC see: https://issues.apache.org/jira/browse/DERBY-706
        if (doesRequestContainData()) {
            try {
                flush(socketOutputStream);
            } catch (java.io.IOException e) {
                agent.markCommunicationsFailure(
                    "DDMWriter.finalizeChain()",
                    "OutputStream.flush()",
                    e.getMessage(),"*");
            }
        }

    }

    /**
     * Takes note of the location of the most recently completed
     * DSS in the buffer, and then returns the current offset.
     * This method is used in conjunction with "clearDSSesBackToMark"
     * to allow for DRDAConnThread to "back-out" DSSes in the
     * event of errors.
     */
    protected int markDSSClearPoint()
    {

//IC see: https://issues.apache.org/jira/browse/DERBY-5
        lastDSSBeforeMark = prevHdrLocation;
        return buffer.position();
//IC see: https://issues.apache.org/jira/browse/DERBY-2936

    }

    /**
     * Does a logical "clear" of everything written to the buffer after
     * the received mark.  It's assumed that this method will be used
     * in error cases when we've started writing one or more DSSes,
     * but then hit an error and need to back out.  After backing out,
     * we'll always need to write _something_ back to the client to
     * indicate an error (typically, we just write an SQLCARD) but what
     * exactly gets written is handled in DRDAConnThread.  Here, we
     * just do the necessary prep so that whatever comes next will
     * succeed.
     */
    protected void clearDSSesBackToMark(int mark)
    {

        // Logical clear.
        buffer.position(mark);
//IC see: https://issues.apache.org/jira/browse/DERBY-2936

        // Because we've just cleared out the most recently-
        // written DSSes, we have to make sure the next thing
        // we write will have the correct correlation id.  We
        // do this by setting the value of 'nextCorrelationID'
        // based on the chaining byte from the last remaining
        // DSS (where "remaining" means that it still exists
        // in the buffer after the clear).
//IC see: https://issues.apache.org/jira/browse/DERBY-1302
//IC see: https://issues.apache.org/jira/browse/DERBY-326
        if (lastDSSBeforeMark == -1)
        // we cleared out the entire buffer; reset corr id.
            nextCorrelationID = 1;
        else {
        // last remaining DSS had chaining, so we set "nextCorrelationID"
        // to be 1 greater than whatever the last remaining DSS had as
        // its correlation id.
//IC see: https://issues.apache.org/jira/browse/DERBY-2936
//IC see: https://issues.apache.org/jira/browse/DERBY-5896
            nextCorrelationID =
                (buffer.getShort(lastDSSBeforeMark + 4) & 0xFFFF) + 1;
        }

    }

    
    private static int peekStream(InputStream in) throws IOException{
        
    in.mark(1);
//IC see: https://issues.apache.org/jira/browse/DERBY-326

    try{
        return in.read();
        
    }finally{
        in.reset();
        
    }
    }

    
    private static int getLayerBStreamingBufferSize(){
    return PropertyUtil.getSystemInt( Property.DRDA_PROP_STREAMOUTBUFFERSIZE , 0 );
    }
    
    
    private static OutputStream placeLayerBStreamingBuffer(OutputStream original){
    
    int size = getLayerBStreamingBufferSize();
    
    if(size < 1)
        return original;
    else
        return new BufferedOutputStream( original, size );

    }
    
}

