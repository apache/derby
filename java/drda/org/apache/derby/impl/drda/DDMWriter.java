/*

   Derby - Class org.apache.derby.impl.drda.DDMWriter

   Copyright 2001, 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derby.impl.drda;

import java.io.OutputStream;
import org.apache.derby.iapi.services.sanity.SanityManager;
import java.sql.SQLException;
import java.sql.DataTruncation;
import java.math.BigDecimal;
import org.apache.derby.iapi.error.ExceptionSeverity;
import java.util.Arrays;

/**
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


	static final BigDecimal ZERO = BigDecimal.valueOf(0L);

	// output buffer
	private byte[] bytes;

	// offset into output buffer
	private int offset;

	// A saved mark in the stream is saved temporarily to revisit the location.
	private int[] markStack = new int[MAX_MARKS_NESTING];

	// top of the stack
	private int top;

	// CCSID manager for translation of strings in the protocol to EBCDIC
	private CcsidManager ccsidManager;

	// DRDA connection thread for this writer
	private DRDAConnThread agent;

	//	This Object tracks the location of the current
	//	Dss header length bytes.	This is done so
	//	the length bytes can be automatically
	//	updated as information is added to this stream.
	private int dssLengthLocation;

	// Current correlation ID
	private	int correlationID;

	// Next correlation ID
	private int nextCorrelationID;

	// is this DRDA protocol or CMD protocol
	private boolean isDRDAProtocol;
	// trace object of the associated session
	private DssTrace dssTrace;

	// Location within the "bytes" array of the start of the header
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
	// location within the "bytes" array of the start of the header
	// that immediately precedes the mark.
	private int lastDSSBeforeMark;

	// Constructors
	DDMWriter (int minSize, CcsidManager ccsidManager, DRDAConnThread agent, DssTrace dssTrace)
	{
		this.bytes = new byte[minSize];
		this.ccsidManager = ccsidManager;
		this.agent = agent;
		this.prevHdrLocation = -1;
		this.previousCorrId = DssConstants.CORRELATION_ID_UNKNOWN;
		this.previousChainByte = DssConstants.DSS_NOCHAIN;
		this.isContinuationDss = false;
		this.lastDSSBeforeMark = -1;
		reset(dssTrace);
	}

	DDMWriter (CcsidManager ccsidManager, DRDAConnThread agent, DssTrace dssTrace)
	{
		this.bytes = new byte[DEFAULT_BUFFER_SIZE];
		this.ccsidManager = ccsidManager;
		this.agent = agent;
		this.prevHdrLocation = -1;
		this.previousCorrId = DssConstants.CORRELATION_ID_UNKNOWN;
		this.previousChainByte = DssConstants.DSS_NOCHAIN;
		this.isContinuationDss = false;
		this.lastDSSBeforeMark = -1;
		reset(dssTrace);
	}

	/**
	 * reset values for sending next message
	 *
	 */
	protected void reset(DssTrace dssTrace)
	{
		offset = 0;
		top = 0;
		dssLengthLocation = 0;
		nextCorrelationID = 1;
		correlationID = DssConstants.CORRELATION_ID_UNKNOWN;
		isDRDAProtocol = true;
		this.dssTrace = dssTrace;
	}

	/**
	 * set protocol to CMD protocol
	 */
	protected void setCMDProtocol()
	{
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
	 * (via the TestProto.java file in this package)!
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
	 *	1. One or more continuation DSSes will immediately
	 * 		follow the current (continued) DSS.
	 *	2. All continuation DSSes will have a 2-byte
	 * 		continuation header, followed by data; in
	 * 		other words, chaining state, correlation
	 *		id, dss format info, and code point will
	 * 		NOT be included.  All of that info is 
	 * 		present ONLY in the FIRST DSS in the
	 *		list of continued DSSes.
	 *
	 *	NOTE: A DSS can be a "continuation" DSS _and_
	 * 	a "continued" DSS at the same time.  However,
	 * 	the FIRST DSS to be continued canNOT be
	 *	a continuation DSS.
	 */
	private void markDssAsContinued(boolean forLob)
	{

		if (!forLob) {
		// continuation bit defaults to '1' for lobs, so
		// we only have to switch it if we're not writing
		// lobs.
			bytes[dssLengthLocation] |= 0x80;
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
	 * TESTING purposes only (via TestProto.java).  No calls
	 * should be made to this method in normal DRDA processing
	 * (because for normal processing, chaining must be
	 * determined automatically based on DSS requests).
	 */
	protected void endDss(byte chainByte)
	{

		// Do regular endDss processing.
		endDss(true);

		// Now override default chain state.
		bytes[dssLengthLocation + 3] &= 0x0F;	// Zero out default
		bytes[dssLengthLocation + 3] |= chainByte;
		previousChainByte = chainByte;

	}

	/**
	 * End DSS header by writing the length in the length location
	 * and setting the chain bit.
	 */
	protected void endDss() {
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
		endDdm();	// updates last DDM object
		endDss();
	}
	/**
	 * Copy Data to End
	 * Create a buffer and copy from the position given to the end of data
	 *
	 * @param start
	 */
	protected byte [] copyDataToEnd(int start)
	{
		int length = offset - start;
		byte [] temp = new byte[length];
		System.arraycopy(bytes,start,temp,0,length);
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
		markStack[top++] = offset;
		offset += 2; // move past the length bytes before writing the code point
		bytes[offset] = (byte) ((codePoint >>> 8) & 0xff);
		bytes[offset + 1] = (byte) (codePoint & 0xff);
		offset += 2;
	}

	/**
	 * Erase all writes for the current ddm and reset the
	 * top
	 */
	protected void clearDdm ()
	{
		offset = markStack[top--];
	}

	/**
	 * Clear the entire send buffer
	 *
	 */
	protected void clearBuffer()
	{
		offset = 0;
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
		int length = offset - lengthLocation;

		// determine if any extended length bytes are needed.	the value returned
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

			// shift the data to the right by the number of extended
			// length bytes needed.
			int extendedLengthLocation = lengthLocation + 4;
			System.arraycopy (bytes,
					              extendedLengthLocation,
					              bytes,
					              extendedLengthLocation + extendedLengthByteCount,
					              extendedLength);

			// write the extended length
			int shiftSize = (extendedLengthByteCount -1) * 8;
			for (int i = 0; i < extendedLengthByteCount; i++)
			{
				bytes[extendedLengthLocation++] =
					(byte) ((extendedLength >>> shiftSize ) & 0xff);
				shiftSize -= 8;
			}

			// adjust the offset to account for the shift and insert
			offset += extendedLengthByteCount;

			// the two byte length field before the codepoint contains the length
			// of itself, the length of the codepoint, and the number of bytes used
			// to hold the extended length.	the 2 byte length field also has the first
			// bit on to indicate extended length bytes were used.
			length = extendedLengthByteCount + 4;
			length |= DssConstants.CONTINUATION_BIT;
		}

		// write the 2 byte length field (2 bytes before codepoint).
		bytes[lengthLocation] = (byte) ((length >>> 8) & 0xff);
		bytes[lengthLocation+1] = (byte) (length & 0xff);

	}

	/**
	 * Get offset
	 *
	 * @return offset into the buffer
	 */
	protected int getOffset()
	{
		return offset;
	}

	/**
	 * Set offset
	 *
	 * @param value new offset value
	 */
	protected void setOffset(int value)
	{
		offset = value;
	}



	// Write routines

	/**
	 * Write byte
	 *
	 * @param 	value	byte to be written
	 */
	protected void writeByte (int value)
	{
		if (SanityManager.DEBUG)
		{
			if (value > 255)
				SanityManager.THROWASSERT(
									   "writeByte value: " + value +
									   " may not be > 255");
		}

		ensureLength (1);
		bytes[offset++] = (byte) (value & 0xff);
	}


	/**
	 * Write network short
	 *
	 * @param 	value	value to be written
	 */
	protected void writeNetworkShort (int value)
	{
		ensureLength (2);
		bytes[offset] = (byte) ((value >>> 8) & 0xff);
		bytes[offset + 1] = (byte) (value & 0xff);
		offset += 2;
	}

	/**
	 * Write network int
	 *
	 * @param 	value	value to be written
	 */
	protected void writeNetworkInt (int value)
	{
		ensureLength (4);
		bytes[offset] = (byte) ((value >>> 24) & 0xff);
		bytes[offset + 1] = (byte) ((value >>> 16) & 0xff);
		bytes[offset + 2] = (byte) ((value >>> 8) & 0xff);
		bytes[offset + 3] = (byte) (value & 0xff);
		offset += 4;
	}


	/**
	 * Write byte array
	 *
	 * @param 	buf	byte array to be written
	 * @param	length  - length to write
	 */
	protected void writeBytes (byte[] buf, int length)
	{
		writeBytes(buf, 0,length);
	}



	/**
	 * Write byte array
	 *
	 * @param 	buf	byte array to be written
	 * @param	start  - starting position
	 * @param	length  - length to write
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
		System.arraycopy(buf,start,bytes,offset,length);
		offset += length;
	}
	/**
	 * Write byte array
	 *
	 * @param 	buf	byte array to be written
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

		int length = buf.length;
		int writeLen =  buf.length;

		writeShort(writeLen);

		writeBytes(buf,0,writeLen);
	}


	/**
	 * Write code point and 4 bytes
	 *
	 * @param 	codePoint - code point to write
	 * @param	value  - value to write after code point
	 */
	void writeCodePoint4Bytes (int codePoint, int value)
	{
		ensureLength (4);
		bytes[offset] = (byte) ((codePoint >>> 8) & 0xff);
		bytes[offset + 1] = (byte) (codePoint & 0xff);
		bytes[offset + 2] = (byte) ((value >>> 8) & 0xff);
		bytes[offset + 3] = (byte) (value & 0xff);
		offset += 4;
	}

	/**
	 * Write scalar 1 byte object includes length, codepoint and value
	 *
	 * @param 	codePoint - code point to write
	 * @param	value  - value to write after code point
	 */
	void writeScalar1Byte (int codePoint, int value)
	{
		ensureLength (5);
		bytes[offset] = 0x00;
		bytes[offset + 1] = 0x05;
		bytes[offset + 2] = (byte) ((codePoint >>> 8) & 0xff);
		bytes[offset + 3] = (byte) (codePoint & 0xff);
		bytes[offset + 4] = (byte) (value & 0xff);
		offset += 5;
	}

	/**
	 * Write scalar 2 byte object includes length, codepoint and value
	 *
	 * @param 	codePoint - code point to write
	 * @param	value  - value to write after code point
	 */
	protected void writeScalar2Bytes (int codePoint, int value)
	{
		ensureLength (6);
		bytes[offset] = 0x00;
		bytes[offset + 1] = 0x06;
		bytes[offset + 2] = (byte) ((codePoint >>> 8) & 0xff);
		bytes[offset + 3] = (byte) (codePoint & 0xff);
		bytes[offset + 4] = (byte) ((value >>> 8) & 0xff);
		bytes[offset + 5] = (byte) (value & 0xff);
		offset += 6;
	}

	protected void writeScalar2Bytes ( int value)
	{
		ensureLength (2);
		bytes[offset] = (byte) ((value >>> 8) & 0xff);
		bytes[offset + 1] = (byte) (value & 0xff);
		offset += 2;
	}

	/**
	 * Write length and codepoint
	 *
	 * @param 	length - length of object
	 * @param 	codePoint - code point to write
	 */
	protected void startDdm (int length, int codePoint)
	{
		ensureLength (4);
		bytes[offset] = (byte) ((length >>> 8) & 0xff);
		bytes[offset + 1] = (byte) (length & 0xff);
		bytes[offset + 2] = (byte) ((codePoint >>> 8) & 0xff);
		bytes[offset + 3] = (byte) (codePoint & 0xff);
		offset += 4;
	}

	/**
	 * Write scalar byte array object includes length, codepoint and value
	 *
	 * @param 	codePoint - code point to write
	 * @param	buf  - value to write after code point
	 * @param	length - number of bytes to write
	 */
	protected void writeScalarBytes (int codePoint, byte[] buf, int length)
	{
		if (SanityManager.DEBUG)
		{
			if (buf == null && length > 0)
		    	SanityManager.THROWASSERT("Buf is null");
			if (length > buf.length)
		    	SanityManager.THROWASSERT("Not enough bytes in buffer");
		}
		ensureLength (length + 4);
		bytes[offset] = (byte) (((length+4) >>> 8) & 0xff);
		bytes[offset + 1] = (byte) ((length+4) & 0xff);
		bytes[offset + 2] = (byte) ((codePoint >>> 8) & 0xff);
		bytes[offset + 3] = (byte) (codePoint & 0xff);
		System.arraycopy(buf,0,bytes,offset + 4, length);
		offset += length + 4;
	}


	protected int  writeScalarStream (boolean chainedWithSameCorrelator,
									  int codePoint,
									  int length,
									  java.io.InputStream in,
									  boolean writeNullByte) 
		throws DRDAProtocolException
	{

		// Stream equivalent of "beginDss"...
		int leftToRead = length;
		int bytesToRead = prepScalarStream (chainedWithSameCorrelator,
											codePoint,
											writeNullByte,
											leftToRead);

		if (length == 0)
			return 0;

		// write the data
		int bytesRead = 0;
		int totalBytesRead = 0;
		do {
			do {
				try {
					bytesRead = in.read (bytes, offset, bytesToRead);
					totalBytesRead += bytesRead;
				}
				catch (java.io.IOException e) {
					padScalarStreamForError (leftToRead, bytesToRead);
					return totalBytesRead;
				}
				if (bytesRead == -1) {
					padScalarStreamForError (leftToRead, bytesToRead);
					return totalBytesRead;
				}
				else {
					bytesToRead -= bytesRead;
					offset += bytesRead;
					leftToRead -= bytesRead;
				}
			} while (bytesToRead > 0);

			bytesToRead = flushScalarStreamSegment (leftToRead, bytesToRead);
		} while (leftToRead > 0);
		
		// check to make sure that the specified length wasn't too small
		try {
			if (in.read() != -1) {
				totalBytesRead += 1;
			}
		}
		catch (java.io.IOException e) {
			// Encountered error in stream length verification for 
			// InputStream, parameter #" + parameterIndex + ".  
			// Don't think we need to error for this condition
		}
		return totalBytesRead;
	}
	
	/**
	 * Begins a DSS stream (for writing LOB data).
	 */
	private void beginDss (boolean chainedToNextStructure,
						   int dssType)
	{
		beginDss(dssType, false);	// false => don't ensure length.

		// always turn on continuation flags... this is helpful for lobs...
		// these bytes will get rest if dss lengths are finalized.
  		bytes[dssLengthLocation] = (byte) 0xFF;
  		bytes[dssLengthLocation + 1] = (byte) 0xFF;

		// Set whether or not this DSS should be chained to
		// the next one.  If it's chained, it has to be chained
		// with same id (that's the nature of EXTDTA chaining).
		if (chainedToNextStructure) {
			dssType |= DssConstants.GDSCHAIN_SAME_ID;
		}

		bytes[dssLengthLocation + 3] = (byte) (dssType & 0xff);
	}


  // prepScalarStream does the following prep for writing stream data:
  // 1.  Flushes an existing DSS segment, if necessary
  // 2.  Determines if extended length bytes are needed
  // 3.  Creates a new DSS/DDM header and a null byte indicator, if applicable
  protected int prepScalarStream  (boolean chainedWithSameCorrelator,
                                   int codePoint,
                                   boolean writeNullByte,
                                   int leftToRead) throws DRDAProtocolException
  {
    int extendedLengthByteCount;

    int nullIndicatorSize = 0;
    if (writeNullByte) 
		nullIndicatorSize = 1;
	extendedLengthByteCount = calculateExtendedLengthByteCount (leftToRead + 4 + nullIndicatorSize);

    // flush the existing DSS segment if this stream will not fit in the send buffer
    if (10 + extendedLengthByteCount + nullIndicatorSize + leftToRead + offset > DssConstants.MAX_DSS_LENGTH) {
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
    }

    // buildStreamDss should not call ensure length.
	beginDss(chainedWithSameCorrelator, DssConstants.GDSFMT_OBJDSS);

    if (extendedLengthByteCount > 0) {
      // method should never ensure length
      writeLengthCodePoint (0x8004 + extendedLengthByteCount, codePoint);

      if (writeNullByte)
        writeExtendedLengthBytes (extendedLengthByteCount, leftToRead + 1);
      else
        writeExtendedLengthBytes (extendedLengthByteCount, leftToRead);
    }
    else {
      if (writeNullByte)
        writeLengthCodePoint (leftToRead + 4 + 1, codePoint);
      else
        writeLengthCodePoint (leftToRead + 4, codePoint);
    }

    // write the null byte, if necessary
    if (writeNullByte)
      writeByte(0x0);

    int bytesToRead;

    if (writeNullByte)
      bytesToRead = Math.min (leftToRead, DssConstants.MAX_DSS_LENGTH - 6 - 4 - 1 - extendedLengthByteCount);
    else
      bytesToRead = Math.min (leftToRead, DssConstants.MAX_DSS_LENGTH - 6 - 4 - extendedLengthByteCount);

    return bytesToRead;
  }


  // method to determine if any data is in the request.
  // this indicates there is a dss object already in the buffer.
	protected boolean doesRequestContainData()
	{
		return offset != 0;
	}


	// Writes out a scalar stream DSS segment, along with DSS continuation
	// headers if necessary.
	protected int flushScalarStreamSegment (int leftToRead,
											int bytesToRead)
		throws DRDAProtocolException
	{
		int newBytesToRead = bytesToRead;

		// either at end of data, end of dss segment, or both.
		if (leftToRead != 0) {
		// 32k segment filled and not at end of data.

			if ((Math.min (2 + leftToRead, 32767)) > (bytes.length - offset)) {
				try {
				// Mark current DSS as continued, set its chaining state,
				// then send the data across.
					markDssAsContinued(true); 	// true => for lobs
					sendBytes (agent.getOutputStream());
				}
				catch (java.io.IOException ioe) {
					agent.markCommunicationsFailure ("DDMWriter.flushScalarStreamSegment()",
                                               "",
                                               ioe.getMessage(),
                                               "*");
				}
			}
			else {
			// DSS is full, but we still have space in the buffer.  So
			// end the DSS, then start the next DSS right after it.
				endDss(false);		// false => don't finalize length.
			}

			// Prepare a DSS continuation header for next DSS.
			dssLengthLocation = offset;
			bytes[offset++] = (byte) (0xff);
			bytes[offset++] = (byte) (0xff);
			newBytesToRead = Math.min (leftToRead,32765);
			isContinuationDss = true;
  		}
		else {
		// we're done writing the data, so end the DSS.
			endDss();
		}

		return newBytesToRead;

	}

  // the offset must not be updated when an error is encountered
  // note valid data may be overwritten
  protected void padScalarStreamForError (int leftToRead, int bytesToRead) throws DRDAProtocolException
  {
    do {
      do {
        bytes[offset++] = (byte)(0x0); // use 0x0 as the padding byte
        bytesToRead--;
        leftToRead--;
      } while (bytesToRead > 0);

      bytesToRead = flushScalarStreamSegment (leftToRead, bytesToRead);
    } while(leftToRead > 0);
  }



	private void writeExtendedLengthBytes (int extendedLengthByteCount, long length)
	{
	int shiftSize = (extendedLengthByteCount -1) * 8;
    for (int i = 0; i < extendedLengthByteCount; i++) {
      bytes[offset + i] = (byte) ((length >>> shiftSize) & 0xff);
      shiftSize -= 8;
    }
	offset += extendedLengthByteCount;
  }


  // insert a 4 byte length/codepoint pair into the buffer.
  // total of 4 bytes inserted in buffer.
  // Note: the length value inserted in the buffer is the same as the value
  // passed in as an argument (this value is NOT incremented by 4 before being
  // inserted).
  void writeLengthCodePoint (int length, int codePoint)
  {
    ensureLength (4);
    bytes[offset] = (byte) ((length >>> 8) & 0xff);
    bytes[offset + 1] = (byte) (length & 0xff);
    bytes[offset + 2] = (byte) ((codePoint >>> 8) & 0xff);
    bytes[offset + 3] = (byte) (codePoint & 0xff);
	offset +=4;
  }

	/**
	 * Write scalar object header includes length and codepoint
	 *
	 * @param 	codePoint - code point to write
	 * @param	dataLength - length of object data
	 * @param	length - number of bytes to write
	 */
	protected void writeScalarHeader (int codePoint, int dataLength)
	{
		ensureLength (dataLength + 4);
		bytes[offset] = (byte) (((dataLength+4) >>> 8) & 0xff);
		bytes[offset + 1] = (byte) ((dataLength+4) & 0xff);
		bytes[offset + 2] = (byte) ((codePoint >>> 8) & 0xff);
		bytes[offset + 3] = (byte) (codePoint & 0xff);
		offset += 4;
	}

	/**
	 * Write scalar string object includes length, codepoint and value
	 * the string is converted into the appropriate codeset (EBCDIC)
	 *
	 * @param 	codePoint - code point to write
	 * @param	string - string to be written
	 */
	void writeScalarString (int codePoint, String string)
	{
		int stringLength = string.length();
		ensureLength ((stringLength * 2)  + 4);
		bytes[offset] = (byte) (((stringLength+4) >>> 8) & 0xff);
		bytes[offset + 1] = (byte) ((stringLength+4) & 0xff);
		bytes[offset + 2] = (byte) ((codePoint >>> 8) & 0xff);
		bytes[offset + 3] = (byte) (codePoint & 0xff);
		offset = ccsidManager.convertFromUCS2 (string, bytes, offset + 4);
	}

	/**
	 * Write padded scalar string object includes length, codepoint and value
	 * the string is converted into the appropriate codeset (EBCDIC)
	 *
	 * @param 	codePoint - code point to write
	 * @param	string - string to be written
	 * @param 	paddedLength - length to pad string to
	 */
	void writeScalarPaddedString (int codePoint, String string, int paddedLength)
	{
		int stringLength = string.length();
		int fillLength = paddedLength - stringLength;
		ensureLength (paddedLength + 4);
		bytes[offset] = (byte) (((paddedLength+4) >>> 8) & 0xff);
		bytes[offset + 1] = (byte) ((paddedLength+4) & 0xff);
		bytes[offset + 2] = (byte) ((codePoint >>> 8) & 0xff);
		bytes[offset + 3] = (byte) (codePoint & 0xff);
		offset = ccsidManager.convertFromUCS2 (string, bytes, offset + 4);
		Arrays.fill(bytes,offset, offset + fillLength,ccsidManager.space);
		offset += fillLength;
	}

	/**
	 * Write padded scalar string object value
	 * the string is converted into the appropriate codeset (EBCDIC)
	 *
	 * @param	string - string to be written
	 * @param 	paddedLength - length to pad string to
	 */
	protected void writeScalarPaddedString (String string, int paddedLength)
	{
		int stringLength = string.length();

		int fillLength = paddedLength -stringLength;
		ensureLength (paddedLength);
		offset = ccsidManager.convertFromUCS2 (string, bytes, offset);
		Arrays.fill(bytes,offset, offset + fillLength,ccsidManager.space);
		offset += fillLength;
	}

	/**
	 * Write padded scalar byte array object includes length, codepoint and value
	 *
	 * @param 	codePoint - code point to write
	 * @param	buf - byte array to be written
	 * @param 	paddedLength - length to pad string to
	 * @param	padByte - byte to be used for padding
	 */
	protected void writeScalarPaddedBytes (int codePoint, byte[] buf, int paddedLength, byte padByte)
	{
		int bufLength = buf.length;
		ensureLength (paddedLength + 4);
		bytes[offset] = (byte) (((paddedLength+4) >>> 8) & 0xff);
		bytes[offset + 1] = (byte) ((paddedLength+4) & 0xff);
		bytes[offset + 2] = (byte) ((codePoint >>> 8) & 0xff);
		bytes[offset + 3] = (byte) (codePoint & 0xff);
		offset += 4;
		System.arraycopy(buf,0,bytes,offset,bufLength);
		offset += bufLength;
		int fillLength = paddedLength - bufLength;
		Arrays.fill(bytes,offset,offset + fillLength,padByte);
		offset += fillLength;
	}

	/**
	 * Write padded scalar byte array object  value
	 *
	 * @param	buf - byte array to be written
	 * @param 	paddedLength - length to pad string to
	 * @param	padByte - byte to be used for padding
	 */
	protected void writeScalarPaddedBytes (byte[] buf, int paddedLength, byte padByte)
	{
		int bufLength = buf.length;
		int fillLength = paddedLength - bufLength;
		ensureLength (paddedLength);
		System.arraycopy(buf,0,bytes,offset,bufLength);
		offset +=bufLength;
		Arrays.fill(bytes,offset,offset + fillLength,padByte);
		offset += fillLength;
	}

	/**
	 * Write scalar byte array object includes length, codepoint and value
	 *
	 * @param 	codePoint - code point to write
	 * @param	buf - byte array to be written
	 */
	protected void writeScalarBytes (int codePoint, byte[] buf)
	{
		int bufLength = buf.length;
		ensureLength (bufLength + 4);
		bytes[offset] = (byte) (((bufLength+4) >>> 8) & 0xff);
		bytes[offset + 1] = (byte) ((bufLength+4) & 0xff);
		bytes[offset + 2] = (byte) ((codePoint >>> 8) & 0xff);
		bytes[offset + 3] = (byte) (codePoint & 0xff);
		System.arraycopy(buf,0,bytes,offset + 4,bufLength);
		offset += bufLength + 4;
	}

	/**
	 * Write scalar byte array object includes length, codepoint and value
	 *
	 * @param 	codePoint - code point to write
	 * @param	buf - byte array to be written
	 * @param	start - starting point
	 * @param 	length - length to write
	 */
	protected void writeScalarBytes (int codePoint, byte[] buf, int start, int length)
	{
		if (SanityManager.DEBUG)
		{
			if (buf == null && length > start)
		    	SanityManager.THROWASSERT("Buf is null");
			if (length - start > buf.length)
				SanityManager.THROWASSERT("Not enough bytes in buffer");
		}
		int numBytes = length - start;
		ensureLength (numBytes + 4);
		bytes[offset] = (byte) (((numBytes+4) >>> 8) & 0xff);
		bytes[offset + 1] = (byte) ((numBytes+4) & 0xff);
		bytes[offset + 2] = (byte) ((codePoint >>> 8) & 0xff);
		bytes[offset + 3] = (byte) (codePoint & 0xff);
		offset += 4;
		System.arraycopy(buf,start,bytes,offset,numBytes);
		offset += numBytes;
	}
	// The following methods write data in the platform format
	// The platform format was indicated during connection time as ASC since
	// JCC doesn't read JVM platform (yet)

	/**
	 * Write platform short
	 *
	 * @param 	v	value to be written
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
	 * @param 	v	value to be written
	 */
	protected void writeInt (int v)
	{
		writeNetworkInt(v);
	}

	/**
	 * Write platform long
	 *
	 * @param 	v	value to be written
	 */
	protected void writeLong (long v)
	{
		ensureLength (8);
		bytes[offset] =	(byte) ((v >>> 56) & 0xff);
		bytes[offset + 1] =	(byte) ((v >>> 48) & 0xff);
		bytes[offset + 2] =	(byte) ((v >>> 40) & 0xff);
		bytes[offset + 3] =	(byte) ((v >>> 32) & 0xff);
		bytes[offset + 4] =	(byte) ((v >>> 24) & 0xff);
		bytes[offset + 5] =	(byte) ((v >>> 16) & 0xff);
		bytes[offset + 6] =	(byte) ((v >>>  8) & 0xff);
		bytes[offset + 7] =	(byte) ((v >>>  0) & 0xff);
		offset += 8;
	}

	/**
	 * Write platform float
	 *
	 * @param 	v	value to be written
	 */
	protected void writeFloat (float v)
	{
		writeInt (Float.floatToIntBits (v));
	}

	/**
	 * Write platform double
	 *
	 * @param 	v	value to be written
	 */
	protected void writeDouble (double v)
	{
		writeLong (Double.doubleToLongBits (v));
	}

	/**
	 * Write big decimal to buffer
	 *
	 * @param v value to write
	 * @param precision Precison of decimal or numeric type
	 * @param declared scale
	 * @exception SQLException thrown if number of digits > 31
	 */
	protected void writeBigDecimal (java.math.BigDecimal v, int precision, int scale)
		throws SQLException
	{
		int length = precision / 2 + 1;
		ensureLength (offset + length);
		bigDecimalToPackedDecimalBytes (v,precision, scale);
		offset += length;
	}

	/**
	 * Write platform boolean
	 *
	 * @param 	v	value to be written
	 */
	protected void writeBoolean (boolean v)
	{
		ensureLength (1);
		bytes[offset++] = (byte) ((v ? 1 : 0) & 0xff);
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
		writeLDString(s,0);
	}


	/**
	 * Write length delimited string
	 *
	 * @param s              value to be written with integer
	 * @param index          column index to put in warning
	 * @exception DRDAProtocolException
	 */
	protected void writeLDString(String s, int index) throws DRDAProtocolException
	{
		try {
			byte [] byteval = s.getBytes(DB2jServerImpl.DEFAULT_ENCODING);
			int origLen = byteval.length;
			boolean multiByteTrunc = false;
			int writeLen =
				java.lang.Math.min(FdocaConstants.LONGVARCHAR_MAX_LEN,
								   origLen);
			/*
			Need to make sure we truncate on character boundaries.
			We are assuming
			http://www.sun.com/developers/gadc/technicalpublications/articles/utf8.html
			To find the beginning of a multibyte character:
			1) Does the current byte start with the bit pattern 10xxxxxx?
			2) If yes, move left and go to step #1.
			3) Finished
			We assume that DB2jServerImpl.DEFAULT_ENCODING remains UTF-8
			*/

			if (SanityManager.DEBUG)
			{
				if (!(DB2jServerImpl.DEFAULT_ENCODING.equals("UTF8")))
					SanityManager.THROWASSERT("Encoding assumed to be UTF8, but is actually" + DB2jServerImpl.DEFAULT_ENCODING);
			}

			if (writeLen != origLen)
				// first position on the first byte of the multibyte char
				while ((byteval[writeLen -1] & 0xC0) == 0x80)
				{
					multiByteTrunc = true;
					writeLen--;
					// Then subtract one more to get to the end of the
					// previous character
					if (multiByteTrunc == true)
					{
						writeLen = writeLen -1;
					}
				}

			writeShort(writeLen);
			writeBytes(byteval,writeLen);
		}
		catch (Exception e) {
			//this should never happen
			agent.agentError("Encoding " + DB2jServerImpl.DEFAULT_ENCODING + " not supported");
		}
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
		try {
			writeBytes(s.getBytes(DB2jServerImpl.DEFAULT_ENCODING));
		} catch (Exception e) {
			//this should never happen
			agent.agentError("Encoding " + DB2jServerImpl.DEFAULT_ENCODING + " not supported");
		}
	}

	/**
	 * Write string with default encoding and specified length
	 *
	 * @param s value to be written
	 * @param length number of bytes to be written
	 *
	 * @exception DRDAProtocolException
	 */
	protected void writeString(String s, int length) throws DRDAProtocolException
	{
		byte[] bs = null;
		try {
			bs = s.getBytes(DB2jServerImpl.DEFAULT_ENCODING);
		} catch (Exception e) {
			//this should never happen
			agent.agentError("Encoding " + DB2jServerImpl.DEFAULT_ENCODING + " not supported");
		}
		int len = bs.length;
		if (len >= length)
			writeBytes(bs, length);
		else
		{
			writeBytes(bs);
			padBytes(DB2jServerImpl.SPACE_CHAR, length-len);
		}
	}

	/**
	 * Write pad bytes using spaceChar
	 *
	 * @param   val	value to be written
	 * @param	length		length to be written
	 */
	protected void padBytes (byte val, int length)
	{
		Arrays.fill(bytes,offset, offset + length,val);
		offset += length;
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
		try {
			socketOutputStream.write (bytes, 0, offset);
			socketOutputStream.flush();
		}
		finally {
			if ((dssTrace != null) && dssTrace.isComBufferTraceOn()) {
			  dssTrace.writeComBufferData (bytes,
			                               0,
			                               offset,
			                               DssTrace.TYPE_TRACE_SEND,
			                               "Reply",
			                               "flush",
			                               5);
			}
			reset(dssTrace);
		}
	}

	// private methods

	/**
	 * Write DSS header
	 * DSS Header format is
	 * 	2 bytes	- length
	 *	1 byte	- 'D0'	- indicates DDM data
	 * 	1 byte	- DSS format
	 *		|---|---------|----------|
	 *		| 0	|	flags |	type     |
	 *		|---|---------|----------|
	 *		| 0 | 1	2	3 | 4 5 6 7	 |
	 *		|---|---------|----------|
	 *		bit 0 - '0'
	 *		bit 1 - '0' - unchained, '1' - chained
	 *		bit 2 - '0'	- do not continue on error, '1' - continue on error
	 *		bit 3 - '0' - next DSS has different correlator, '1' - next DSS has
	 *						same correlator
	 *		type - 1 - Request DSS
	 *			 - 2 - Reply DSS
	 *			 - 3 - Object DSS
	 *			 - 4 - Communications DSS
	 *			 - 5 - Request DSS where no reply is expected
	 */
	private void beginDss (int dssType, boolean ensureLen)
	{

		// save length position, the length will be written at the end
		dssLengthLocation = offset;

		// Should this really only be for non-stream DSSes?
		if (ensureLen)
			ensureLength(6);

		// Skip past length; we'll come back and set it later.
		offset += 2;

		// write gds info
		bytes[offset] = (byte) 0xD0;

		// Write DSS type, and default chain bit to be 
		// DssConstants.DSSCHAIN_SAME_ID.  This default
		// will be overridden by calls to "finalizeChain()"
		// and/or calls to "beginDss(boolean, int)" for
		// writing LOB data.
		bytes[offset + 1] = (byte) dssType;
		bytes[offset + 1] |= DssConstants.DSSCHAIN_SAME_ID;

		// save correlationID for use in error messages while processing
		// this DSS
		correlationID = getCorrelationID();

		// write the reply correlation id
		bytes[offset + 2] = (byte) ((correlationID >>> 8) & 0xff);
		bytes[offset + 3] = (byte) (correlationID & 0xff);
		offset += 4;
	}

	/**
     * Finish a DSS Layer A object.
	 * The length of dss object will be calculated based on the difference between the
	 * start of the dss, saved on the beginDss call, and the current
	 * offset into the buffer which marks the end of the data.	In the event
	 * the length requires the use of continuation Dss headers, one for each 32k
	 * chunk of data, the data will be shifted and the continuation headers
	 * will be inserted with the correct values as needed.
	 */
	private void finalizeDssLength ()
	{
		// calculate the total size of the dss and the number of bytes which would
		// require continuation dss headers.	The total length already includes the
		// the 6 byte dss header located at the beginning of the dss.	It does not
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

			// right now the code will shift to the right.	In the future we may want
			// to try something fancier to help reduce the copying (maybe keep
			// space in the beginning of the buffer??).
			// the offset points to the next available offset in the buffer to place
			// a piece of data, so the last dataByte is at offset -1.
			// various bytes will need to be shifted by different amounts
			// depending on how many dss headers to insert so the amount to shift
			// will be calculated and adjusted as needed.	ensure there is enough room
			// for all the conutinuation headers and adjust the offset to point to the
			// new end of the data.
			int dataByte = offset - 1;
			int shiftSize = contDssHeaderCount * 2;
			ensureLength (shiftSize);
			offset += shiftSize;

			// mark passOne to help with calculating the length of the final (first or
			// rightmost) continuation header.
			boolean passOne = true;
			do {
				// calculate chunk of data to shift
				int dataToShift = bytesRequiringContDssHeader % 32765;
				if (dataToShift == 0)
					dataToShift = 32765;
				// We start with the right most chunk. If we had to copy two
				// chunks we would shift the first one 4 bytes and then 
				// the second one
				// 2 when we come back on the next loop so they would each have
				// 2 bytes for the continuation header
				int startOfCopyData = dataByte - dataToShift;
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
				    	twoByteContDssHeader = DssConstants.CONTINUATION_BIT;
				}

				// insert the header's length bytes
				bytes[dataByte + shiftSize - 1] = (byte)
					((twoByteContDssHeader >>> 8) & 0xff);
				bytes[dataByte + shiftSize] = (byte)
					(twoByteContDssHeader & 0xff);

				// adjust the bytesRequiringContDssHeader and the amount to shift for
				// data in upstream headers.
				bytesRequiringContDssHeader -= dataToShift;
				shiftSize -= 2;

				// shift and insert another header for more data.
			}
			while (bytesRequiringContDssHeader > 0);

			// set the continuation dss header flag on for the first header
			totalSize = DssConstants.CONTINUATION_BIT;

		}

		// insert the length bytes in the 6 byte dss header.
		bytes[dssLengthLocation] = (byte) ((totalSize >>> 8) & 0xff);
		bytes[dssLengthLocation + 1] = (byte) (totalSize & 0xff);
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
	 * 	extended length needed.
	 */
	private int calculateExtendedLengthByteCount (long ddmSize)
	{
		if (ddmSize <= 0x7fff)
			return 0;
		// JCC does not support 2 at this time, so we always send
		// at least 4
		//		else if (ddmSize <= 0xffff)
		//	return 2;
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
		length += offset;
		if (length > bytes.length) {
			if (SanityManager.DEBUG)
			{
				agent.trace("DANGER - Expensive expansion of  buffer");
			}
			byte newBytes[] = new byte[Math.max (bytes.length << 1, length)];
			System.arraycopy (bytes, 0, newBytes, 0, offset);
			bytes = newBytes;
		}
	}


	/**
	 * Write a Java <code>java.math.BigDecimal</code> to packed decimal bytes.
	 *
	 * @param b BigDecimal to write
	 * @param precision Precision of decimal or numeric type
	 * @return length written.
	 *
	 * @exception SQLException Thrown if # digits > 31
	 */
	private int bigDecimalToPackedDecimalBytes (java.math.BigDecimal b,
												int precision, int scale)
	throws SQLException
	{
		int declaredPrecision = precision;
		int declaredScale = scale;

		// packed decimal may only be up to 31 digits.
		if (declaredPrecision > 31) // this is a bugcheck only !!!
		{
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

        if (bigScale >= declaredScale) {
          // If target scale is less than source scale,
          // discard excessive fraction.

          // set start index in source big decimal to ignore excessive fraction.
          bigIndex = bigPrecision-1-(bigScale-declaredScale);

          if (bigIndex < 0) {
            // all digits are discarded, so only process the sign nybble.
            bytes[offset+(packedIndex+1)/2] =
              (byte) ( (b.signum()>=0)?12:13 ); // sign nybble
          }
          else {
            // process the last nybble together with the sign nybble.
            bytes[offset+(packedIndex+1)/2] =
              (byte) ( ( (unscaledStr.charAt(bigIndex)-zeroBase) << 4 ) + // last nybble
                     ( (b.signum()>=0)?12:13 ) ); // sign nybble
          }
          packedIndex-=2;
          bigIndex-=2;
        }
        else {
          // If target scale is greater than source scale,
          // pad the fraction with zero.

          // set start index in source big decimal to pad fraction with zero.
          bigIndex = declaredScale-bigScale-1;

          // process the sign nybble.
          bytes[offset+(packedIndex+1)/2] =
            (byte) ( (b.signum()>=0)?12:13 ); // sign nybble

          for (packedIndex-=2, bigIndex-=2; bigIndex>=0; packedIndex-=2, bigIndex-=2)
            bytes[offset+(packedIndex+1)/2] = (byte) 0;

          if (bigIndex == -1) {
            bytes[offset+(packedIndex+1)/2] =
              (byte) ( (unscaledStr.charAt(bigPrecision-1)-zeroBase) << 4 ); // high nybble

            packedIndex-=2;
            bigIndex = bigPrecision-3;
          }
          else {
            bigIndex = bigPrecision-2;
          }
        }

        // process the rest.
        for (; bigIndex>=0; packedIndex-=2, bigIndex-=2) {
          bytes[offset+(packedIndex+1)/2] =
            (byte) ( ( (unscaledStr.charAt(bigIndex)-zeroBase) << 4 ) + // high nybble
                   ( unscaledStr.charAt(bigIndex+1)-zeroBase ) ); // low nybble
        }

        // process the first nybble when there is one left.
        if (bigIndex == -1) {
          bytes[offset+(packedIndex+1)/2] =
            (byte) (unscaledStr.charAt(0) - zeroBase);

          packedIndex-=2;
        }

        // pad zero in front of the big decimal if necessary.
        for (; packedIndex>=-1; packedIndex-=2)
          bytes[offset+(packedIndex+1)/2] = (byte) 0;

        return declaredPrecision/2 + 1;
	}


	/***
	 * Prepend zeros to numeric string
	 *
	 * @param s string
	 * @param precision - length of padded string
 	 *
	 * @return zero padded string
	 */
	public static String zeroPadString(String s, int precision)
	{

		if (s == null)
			return s;

		int slen = s.length();
		if (precision == slen)
			return s;
		else if (precision > slen)
		{
			char[] ca  = new char[precision - slen];
			Arrays.fill(ca,0,precision - slen,'0');
			return new String(ca) + s;
		}
		else
		{
			// Shouldn't happen but just in case 
			// truncate
			return s.substring(0,precision);
		}

	}



  private void sendBytes (java.io.OutputStream socketOutputStream) throws java.io.IOException
  {
	resetChainState();
    try {
      socketOutputStream.write (bytes, 0, offset);
      socketOutputStream.flush();
    }
    finally {
		if ((dssTrace != null) && dssTrace.isComBufferTraceOn()) {
			dssTrace.writeComBufferData (bytes,
			                               0,
			                               offset,
			                               DssTrace.TYPE_TRACE_SEND,
			                               "Reply",
			                               "flush",
			                               5);
      }
      clearBuffer();
    }
  }


	private static int min (int i, int j)
	{
		return (i < j) ? i : j;
	}

	protected String toDebugString(String indent)
	{
		String s = indent + "***** DDMWriter toDebugString ******\n";
		int byteslen = 0;
		if ( bytes != null)
			byteslen = bytes.length;
		s += indent + "byte array length  = " + bytes.length + "\n";
		return s;
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

		if (prevHdrLocation != -1) {
		// Note: == -1 => the previous DSS was already sent; this
		// should only happen in cases where the buffer filled up
		// and we had to send it (which means we were probably
		// writing EXTDTA).  In such cases, proper chaining
		// should already have been handled @ time of send.
			bytes[prevHdrLocation + 3] &= 0x0F;	// Zero out old chain value.
			bytes[prevHdrLocation + 3] |= currChainByte;
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
		if (offset != 0) {
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

		lastDSSBeforeMark = prevHdrLocation;
		return getOffset();

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
		setOffset(mark);

		// Because we've just cleared out the most recently-
		// written DSSes, we have to make sure the next thing
		// we write will have the correct correlation id.  We
		// do this by setting the value of 'nextCorrelationID'
		// based on the chaining byte from the last remaining
		// DSS (where "remaining" means that it still exists
		// in the buffer after the clear).
		if (lastDSSBeforeMark == -1)
		// we cleared out the entire buffer; reset corr id.
			nextCorrelationID = 1;
		else {
		// last remaining DSS had chaining, so we set "nextCorrelationID"
		// to be 1 greater than whatever the last remaining DSS had as
		// its correlation id.
 			nextCorrelationID = 1 + (int)
				(((bytes[lastDSSBeforeMark + 4] & 0xff) << 8) +
				(bytes[lastDSSBeforeMark + 5] & 0xff));
		}

	}

}

