/*

   Derby - Class org.apache.derby.iapi.types.SQLClob

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

package org.apache.derby.iapi.types;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.jdbc.CharacterStreamDescriptor;

import org.apache.derby.iapi.services.io.ArrayInputStream;
import org.apache.derby.iapi.services.io.InputStreamUtil;
import org.apache.derby.iapi.services.io.StoredFormatIds;

import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.util.UTF8Util;

import org.apache.derby.shared.common.reference.SQLState;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.sql.Clob;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.RuleBasedCollator;
import java.util.Calendar;


/**
 * SQLClob represents a CLOB value with UCS_BASIC collation.
 * CLOB supports LIKE operator only for collation.
 */
public class SQLClob
	extends SQLVarchar
{

    /** The maximum number of bytes used by the stream header. */
    private static final int MAX_STREAM_HEADER_LENGTH = 5;

    /** The header generator used for 10.4 (or older) databases. */
    private static final StreamHeaderGenerator TEN_FOUR_CLOB_HEADER_GENERATOR =
            new ClobStreamHeaderGenerator(true);

    /** The header generator used for 10.5 databases. */
    private static final StreamHeaderGenerator TEN_FIVE_CLOB_HEADER_GENERATOR =
            new ClobStreamHeaderGenerator(false);

    /**
     * The descriptor for the stream. If there is no stream this should be
     * {@code null}, which is also true if the descriptor hasen't been
     * constructed yet.
     * <em>Note</em>: Always check if {@code stream} is non-null before using
     * the information stored in the descriptor internally.
     */
    private CharacterStreamDescriptor csd;

    /** Tells if the database is being accessed in soft upgrade mode. */
    private Boolean inSoftUpgradeMode = null;

	/*
	 * DataValueDescriptor interface.
	 *
	 * These are actually all implemented in the super-class, but we need
	 * to duplicate some of them here so they can be called by byte-code
	 * generation, which needs to know the class the method appears in.
	 */

	public String getTypeName()
	{
		return TypeId.CLOB_NAME;
	}

	/*
	 * DataValueDescriptor interface
	 */

	/** @see DataValueDescriptor#getClone */
	public DataValueDescriptor getClone()
	{
        // TODO: Should this be rewritten to clone the stream instead of
        //       materializing the value if possible?
		try
		{
            SQLClob clone = new SQLClob(getString());
            // Copy the soft upgrade mode state.
            clone.inSoftUpgradeMode = inSoftUpgradeMode;
            return clone;
		}
		catch (StandardException se)
		{
			if (SanityManager.DEBUG)
				SanityManager.THROWASSERT("Unexpected exception", se);
			return null;
		}
	}

	/**
	 * @see DataValueDescriptor#getNewNull
	 *
	 */
	public DataValueDescriptor getNewNull()
	{
        SQLClob newClob = new SQLClob();
        // Copy the soft upgrade mode state.
        newClob.inSoftUpgradeMode = inSoftUpgradeMode;
        return newClob;
	}

	/** @see StringDataValue#getValue(RuleBasedCollator) */
	public StringDataValue getValue(RuleBasedCollator collatorForComparison)
	{
		if (collatorForComparison == null)
		{//null collatorForComparison means use UCS_BASIC for collation
		    return this;			
		} else {
			//non-null collatorForComparison means use collator sensitive
			//implementation of SQLClob
		     CollatorSQLClob s = new CollatorSQLClob(collatorForComparison);
		     s.copyState(this);
		     return s;
		}
	}

	/*
	 * Storable interface, implies Externalizable, TypedFormat
	 */

	/**
		Return my format identifier.

		@see org.apache.derby.iapi.services.io.TypedFormat#getTypeFormatId
	*/
	public int getTypeFormatId() {
		return StoredFormatIds.SQL_CLOB_ID;
	}

	/*
	 * constructors
	 */

	public SQLClob()
	{
	}

	public SQLClob(String val)
	{
		super(val);
	}

	public SQLClob(Clob val)
	{
		super(val);
	}

	/*
	 * DataValueDescriptor interface
	 */

	/* @see DataValueDescriptor#typePrecedence */
	public int typePrecedence()
	{
		return TypeId.CLOB_PRECEDENCE;
	}

	/*
	** disable conversions to/from most types for CLOB.
	** TEMP - real fix is to re-work class hierachy so
	** that CLOB is towards the root, not at the leaf.
	*/

	public boolean	getBoolean() throws StandardException
	{
		throw dataTypeConversion("boolean");
	}

	public byte	getByte() throws StandardException
	{
		throw dataTypeConversion("byte");
	}

	public short	getShort() throws StandardException
	{
		throw dataTypeConversion("short");
	}

	public int	getInt() throws StandardException
	{
		throw dataTypeConversion("int");
	}

    /**
     * Returns the character length of this Clob.
     * <p>
     * If the value is stored as a stream, the stream header will be read. If
     * the stream header doesn't contain the stream length, the whole stream
     * will be decoded to determine the length.
     *
     * @return The character length of this Clob.
     * @throws StandardException if obtaining the length fails
     */
    public int getLength() throws StandardException {
        if (stream == null) {
            return super.getLength();
        }
        // The Clob is represented as a stream.
        // Make sure we have a stream descriptor.
        boolean repositionStream = (csd != null);
        if (csd == null) {
            getStreamWithDescriptor();
            // We know the stream is at the first char position here.
        }
        if (csd.getCharLength() != 0) {
            return (int)csd.getCharLength();
        }
        // We now know that the Clob is represented as a stream, but not if the
        // length is unknown or actually zero. Check.
        if (SanityManager.DEBUG) {
            // The stream isn't expecetd to be position aware here.
            SanityManager.ASSERT(!csd.isPositionAware());
        }
        long charLength = 0;
        try {
            if (repositionStream) {
                rewindStream(csd.getDataOffset());
            }
            charLength = UTF8Util.skipUntilEOF(stream);
            // We just drained the whole stream. Reset it.
            rewindStream(0);
        } catch (IOException ioe) {
            throwStreamingIOException(ioe);
        }
        // Update the descriptor in two ways;
        //   (1) Set the char length, whether it is zero or not.
        //   (2) Set the current byte pos to zero.
        csd = new CharacterStreamDescriptor.Builder().copyState(csd).
                charLength(charLength).curBytePos(0).
                curCharPos(CharacterStreamDescriptor.BEFORE_FIRST).build();
        return (int)charLength;
    }

	public long	getLong() throws StandardException
	{
		throw dataTypeConversion("long");
	}

	public float	getFloat() throws StandardException
	{
		throw dataTypeConversion("float");
	}

	public double	getDouble() throws StandardException
	{
		throw dataTypeConversion("double");
	}
	public int typeToBigDecimal() throws StandardException
	{
		throw dataTypeConversion("java.math.BigDecimal");
	}
	public byte[]	getBytes() throws StandardException
	{
		throw dataTypeConversion("byte[]");
	}

	public Date	getDate(java.util.Calendar cal) throws StandardException
	{
		throw dataTypeConversion("java.sql.Date");
	}

    /**
     * Returns a descriptor for the input stream for this CLOB value.
     * <p>
     * The descriptor contains information about header data, current positions,
     * length, whether the stream should be buffered or not, and if the stream
     * is capable of repositioning itself.
     * <p>
     * When this method returns, the stream is positioned on the first
     * character position, such that the next read will return the first
     * character in the stream.
     *
     * @return A descriptor for the stream, which includes a reference to the
     *      stream itself. If the value cannot be represented as a stream,
     *      {@code null} is returned instead of a decsriptor.
     * @throws StandardException if obtaining the descriptor fails
     */
    public CharacterStreamDescriptor getStreamWithDescriptor()
            throws StandardException {
        if (stream == null) {
            // Lazily reset the descriptor here, to avoid further changes in
            // {@code SQLChar}.
            csd = null;
            return null;
        }
        // NOTE: Getting down here several times is potentially dangerous.
        // When the stream is published, we can't assume we know the position
        // any more. The best we can do, which may hurt performance to some
        // degree in some non-recommended use-cases, is to reset the stream if
        // possible.
        if (csd != null) {
            if (stream instanceof Resetable) {
                try {
                    ((Resetable)stream).resetStream();
                    // Make sure the stream is in sync with the descriptor.
                    InputStreamUtil.skipFully(stream, csd.getCurBytePos());
                } catch (IOException ioe) {
                    throwStreamingIOException(ioe);
                }
            } else {
                if (SanityManager.DEBUG) {
                    SanityManager.THROWASSERT("Unable to reset stream when " +
                            "fetched the second time: " + stream.getClass());
                }
            }
        }

        if (csd == null) {
            // First time, read the header format of the stream.
            try {
                // Assume new header format, adjust later if necessary.
                byte[] header = new byte[MAX_STREAM_HEADER_LENGTH];
                int read = stream.read(header);
                // Expect at least two header bytes.
                if (SanityManager.DEBUG) {
                    SanityManager.ASSERT(read > 1,
                            "Too few header bytes: " + read);
                }
                HeaderInfo hdrInfo = investigateHeader(header, read);
                if (read > hdrInfo.headerLength()) {
                    // We have read too much. Reset the stream.
                    read = hdrInfo.headerLength();
                    rewindStream(read);
                }
                csd = new CharacterStreamDescriptor.Builder().stream(stream).
                    bufferable(false).positionAware(false).
                    curCharPos(read == 0 ?
                        CharacterStreamDescriptor.BEFORE_FIRST : 1).
                    curBytePos(read).
                    dataOffset(hdrInfo.headerLength()).
                    byteLength(hdrInfo.byteLength()).
                    charLength(hdrInfo.charLength()).build();
            } catch (IOException ioe) {
                // Check here to see if the root cause is a container closed
                // exception. If so, this most likely means that the Clob was
                // accessed after a commit or rollback on the connection.
                Throwable rootCause = ioe;
                while (rootCause.getCause() != null) {
                    rootCause = rootCause.getCause();
                }
                if (rootCause instanceof StandardException) {
                    StandardException se = (StandardException)rootCause;
                    if (se.getMessageId().equals(
                            SQLState.DATA_CONTAINER_CLOSED)) {
                        throw StandardException.newException(
                                SQLState.BLOB_ACCESSED_AFTER_COMMIT, ioe);
                    }
                }
                throwStreamingIOException(ioe);
            }
        }
        return this.csd;
    }

	public Time	getTime(java.util.Calendar cal) throws StandardException
	{
		throw dataTypeConversion("java.sql.Time");
	}

	public Timestamp	getTimestamp(java.util.Calendar cal) throws StandardException
	{
		throw dataTypeConversion("java.sql.Timestamp");
	}
    
    /**
     * Gets a trace representation of the CLOB for debugging.
     *
     * @return a trace representation of the CLOB.
     */
    public final String getTraceString() throws StandardException {
        // Check if the value is SQL NULL.
        if (isNull()) {
            return "NULL";
        }

        // Check if we have a stream.
        if (getStream() != null) {
            return (getTypeName() + "(" + getStream().toString() + ")");
        }

        return (getTypeName() + "(" + getLength() + ")");
    }
    
    /**
     * Normalization method - this method may be called when putting
     * a value into a SQLClob, for example, when inserting into a SQLClob
     * column.  See NormalizeResultSet in execution.
     * Per the SQL standard ,if the clob column is not big enough to 
     * hold the value being inserted,truncation error will result
     * if there are trailing non-blanks. Truncation of trailing blanks
     * is allowed.
     * @param desiredType   The type to normalize the source column to
     * @param sourceValue   The value to normalize
     *
     *
     * @exception StandardException             Thrown for null into
     *                                          non-nullable column, and for
     *                                          truncation error
     */

    public void normalize(
                DataTypeDescriptor desiredType,
                DataValueDescriptor sourceValue)
                    throws StandardException
    {
        // if sourceValue is of type clob, and has a stream,
        // dont materialize it here (as the goal of using a stream is to
        // not have to materialize whole object in memory in the server), 
        // but instead truncation checks will be done when data is streamed in.
        // (see ReaderToUTF8Stream) 
        // if sourceValue is not a stream, then follow the same
        // protocol as varchar type for normalization
        if( sourceValue instanceof SQLClob)
        {
            SQLClob clob = (SQLClob)sourceValue;
            if (clob.stream != null)
            {
                copyState(clob);
                return;
            }
        }
        
        super.normalize(desiredType,sourceValue);
    }

	public void setValue(Time theValue, Calendar cal) throws StandardException
	{
		throwLangSetMismatch("java.sql.Time");
	}
	
	public void setValue(Timestamp theValue, Calendar cal) throws StandardException
	{
		throwLangSetMismatch("java.sql.Timestamp");
	}
	
	public void setValue(Date theValue, Calendar cal) throws StandardException
	{
		throwLangSetMismatch("java.sql.Date");
	}
	
	public void setBigDecimal(Number bigDecimal) throws StandardException
	{
		throwLangSetMismatch("java.math.BigDecimal");
	}

    /**
     * Sets a new stream for this CLOB.
     *
     * @param stream the new stream
     */
    public final void setStream(InputStream stream) {
        super.setStream(stream);
        // Discard the old stream descriptor.
        this.csd = null;
    }

    public final void restoreToNull() {
        this.csd = null;
        super.restoreToNull();
    }

	public void setValue(int theValue) throws StandardException
	{
		throwLangSetMismatch("int");
	}

	public void setValue(double theValue) throws StandardException
	{
		throwLangSetMismatch("double");
	}

	public void setValue(float theValue) throws StandardException
	{
		throwLangSetMismatch("float");
	}
 
	public void setValue(short theValue) throws StandardException
	{
		throwLangSetMismatch("short");
	}

	public void setValue(long theValue) throws StandardException
	{
		throwLangSetMismatch("long");
	}


	public void setValue(byte theValue) throws StandardException
	{
		throwLangSetMismatch("byte");
	}

	public void setValue(boolean theValue) throws StandardException
	{
		throwLangSetMismatch("boolean");
	}

	public void setValue(byte[] theValue) throws StandardException
	{
		throwLangSetMismatch("byte[]");
	}
    
    /**
     * Set the value from an non-null Java.sql.Clob object.
     */
    final void setObject(Object theValue)
        throws StandardException
    {
        Clob vc = (Clob) theValue;
        
        try {
            long vcl = vc.length();
            if (vcl < 0L || vcl > Integer.MAX_VALUE)
                throw this.outOfRange();
            // For small values, just materialize the value.
            // NOTE: Using streams for the empty string ("") isn't supported
            // down this code path when in soft upgrade mode, because the code
            // reading the header bytes ends up reading zero bytes (i.e., it
            // doesn't get the header / EOF marker).
            if (vcl < 32*1024) {
                setValue(vc.getSubString(1, (int)vcl));
            } else {
                ReaderToUTF8Stream utfIn = new ReaderToUTF8Stream(
                        vc.getCharacterStream(), (int) vcl, 0, TypeId.CLOB_NAME,
                        getStreamHeaderGenerator());
                setValue(utfIn, (int) vcl);
            }
        } catch (SQLException e) {
            throw dataTypeConversion("DAN-438-tmp");
       }
    }

    /**
     * Writes the CLOB data value to the given destination stream using the
     * modified UTF-8 format.
     *
     * @param out destination stream
     * @throws IOException if writing to the destination stream fails
     */
    public void writeExternal(ObjectOutput out)
            throws IOException {
        super.writeClobUTF(out);
    }

    /**
     * Returns a stream header generator for a Clob.
     * <p>
     * <em>NOTE</em>: To guarantee a successful generation, one of the following
     * two conditions must be met at header or EOF generation time:
     * <ul> <li>{@code setSoftUpgradeMode} has been invoked before the header
     *          generator was obtained.</li>
     *      <li>There is context at generation time, such that the mode can be
     *          determined by obtaining the database context and by consulting
     *          the data dictionary.</li>
     * </ul>
     *
     * @return A stream header generator.
     */
    public StreamHeaderGenerator getStreamHeaderGenerator() {
        if (inSoftUpgradeMode == null) {
            // We don't know which mode we are running in, return a generator
            // the will check this when asked to generate the header.
            return new ClobStreamHeaderGenerator(this);
        } else {
            if (inSoftUpgradeMode == Boolean.TRUE) {
                return TEN_FOUR_CLOB_HEADER_GENERATOR;
            } else {
                return TEN_FIVE_CLOB_HEADER_GENERATOR;
            }
        }
    }

    /**
     * Tells whether the database is being accessed in soft upgrade mode or not.
     *
     * @param inSoftUpgradeMode {@code TRUE} if the database is accessed in
     *      soft upgrade mode, {@code FALSE} is not, or {@code null} if unknown
     */
    public void setSoftUpgradeMode(Boolean inSoftUpgradeMode) {
        this.inSoftUpgradeMode = inSoftUpgradeMode;
    }

    /**
     * Investigates the header and returns length information.
     *
     * @param hdr the raw header bytes
     * @param bytesRead number of bytes written into the raw header bytes array
     * @return The information obtained from the header.
     * @throws IOException if the header format is invalid, or the stream
     *      seems to have been corrupted
     */
    private HeaderInfo investigateHeader(byte[] hdr, int bytesRead)
            throws IOException {
        int dataOffset = MAX_STREAM_HEADER_LENGTH;
        int utfLen = -1;
        int strLen = -1;

        // Peek at the magic byte.
        if (bytesRead < dataOffset || (hdr[2] & 0xF0) != 0xF0) {
            // We either have a very short value with the old header
            // format, or the stream is corrupted.
            // Assume the former and check later (see further down).
            dataOffset = 2;
        }

        // Do we have a pre 10.5 header?
        if (dataOffset == 2) {
            // Note that we add the two bytes holding the header to the total
            // length only if we know how long the user data is.
            utfLen = ((hdr[0] & 0xFF) << 8) | ((hdr[1] & 0xFF));
            // Sanity check for small streams:
            // The header length pluss the encoded length must be equal to the
            // number of bytes read.
            if (bytesRead < MAX_STREAM_HEADER_LENGTH) {
                if (dataOffset + utfLen != bytesRead) {
                    throw new IOException("Corrupted stream; headerLength=" +
                            dataOffset + ", utfLen=" + utfLen + ", bytesRead=" +
                            bytesRead);
                }
            }
            if (utfLen > 0) {
                utfLen += dataOffset;
            }
        } else if (dataOffset == 5) {
            // We are dealing with the 10.5 stream header format.
            int hdrFormat = hdr[2] & 0x0F;
            switch (hdrFormat) {
                case 0: // 0xF0
                    strLen = (
                                ((hdr[0] & 0xFF) << 24) |
                                ((hdr[1] & 0xFF) << 16) |
                                // Ignore the third byte (index 2).
                                ((hdr[3] & 0xFF) <<  8) |
                                ((hdr[4] & 0xFF) <<  0)
                             );
                    break;
                default:
                    // We don't know how to handle this header format.
                    throw new IOException("Invalid header format " +
                            "identifier: " + hdrFormat + "(magic byte is 0x" +
                            Integer.toHexString(hdr[2] & 0xFF) + ")");
            }
        }
        if (SanityManager.DEBUG) {
            SanityManager.ASSERT(utfLen > -1 || strLen > -1);
        }
        return new HeaderInfo(dataOffset, dataOffset == 5 ? strLen : utfLen);
    }

    /**
     * Reads and materializes the CLOB value from the stream.
     *
     * @param in source stream
     * @throws java.io.UTFDataFormatException if an encoding error is detected
     * @throws IOException if reading from the stream fails, or the content of
     *      the stream header is invalid
     */
    public void readExternal(ObjectInput in)
            throws IOException {
        HeaderInfo hdrInfo;
        if (csd != null) {
            int hdrLen = (int)csd.getDataOffset();
            int valueLength = (hdrLen == 5) ? (int)csd.getCharLength()
                                            : (int)csd.getByteLength();
            hdrInfo = new HeaderInfo(hdrLen, valueLength);
            // Make sure the stream is correctly positioned.
            rewindStream(hdrLen);
        } else {
            final boolean markSet = stream.markSupported();
            if (markSet) {
                stream.mark(MAX_STREAM_HEADER_LENGTH);
            }
            byte[] header = new byte[MAX_STREAM_HEADER_LENGTH];
            int read = in.read(header);
            // Expect at least two header bytes.
            if (SanityManager.DEBUG) {
                SanityManager.ASSERT(read > 1, "Too few header bytes: " + read);
            }
            hdrInfo = investigateHeader(header, read);
            if (read > hdrInfo.headerLength()) {
                // We read too much data, reset and position on the first byte
                // of the user data.
                // First see if we set a mark on the stream and can reset it.
                // If not, try using the Resetable interface.
                if (markSet) {
                    // Stream is not a store Resetable one, use mark/reset
                    // functionality instead.
                    stream.reset();
                    InputStreamUtil.skipFully(stream, hdrInfo.headerLength());
                } else if (stream instanceof Resetable) {
                    // We have a store stream.
                    rewindStream(hdrInfo.headerLength());
                }
            }
        }
        // The data will be materialized in memory, in a char array.
        // Subtract the header length from the byte length if there is a byte
        // encoded in the header, otherwise the decode routine will try to read
        // too many bytes.
        int byteLength = 0; // zero is interpreted as unknown / unset
        if (hdrInfo.byteLength() != 0) {
            byteLength = hdrInfo.byteLength() - hdrInfo.headerLength();
        }
        super.readExternal(in, byteLength, hdrInfo.charLength());
    }

    /**
     * Reads and materializes the CLOB value from the stream.
     *
     * @param in source stream
     * @throws java.io.UTFDataFormatException if an encoding error is detected
     * @throws IOException if reading from the stream fails, or the content of
     *      the stream header is invalid
     */
    public void readExternalFromArray(ArrayInputStream in)
            throws IOException {
        // It is expected that the position of the array input stream has been
        // set to the correct position before this method is invoked.
        int prevPos = in.getPosition();
        byte[] header = new byte[MAX_STREAM_HEADER_LENGTH];
        int read = in.read(header);
        // Expect at least two header bytes.
        if (SanityManager.DEBUG) {
            SanityManager.ASSERT(read > 1, "Too few header bytes: " + read);
        }
        HeaderInfo hdrInfo = investigateHeader(header, read);
        if (read > hdrInfo.headerLength()) {
            // Reset stream. This path will only be taken for Clobs stored
            // with the pre 10.5 stream header format.
            // Note that we set the position to before the header again, since
            // we know the header will be read again.
            in.setPosition(prevPos);
            super.readExternalFromArray(in);
        } else {
            // We read only header bytes, next byte is user data.
            super.readExternalClobFromArray(in, hdrInfo.charLength());
        }
    }

    /**
     * Rewinds the stream to the beginning and then skips the specified number
     * of bytes.
     *
     * @param pos number of bytes to skip
     * @throws IOException if resetting or reading from the stream fails
     */
    private void rewindStream(long pos)
            throws IOException {
        try {
            ((Resetable)stream).resetStream();
            InputStreamUtil.skipFully(stream, pos);
        } catch (StandardException se) {
            IOException ioe = new IOException(se.getMessage());
            ioe.initCause(se);
            throw ioe;
        }
    }

    /**
     * Holder class for header information gathered from the raw byte header in 
     * the stream.
     */
    //@Immutable
    private static class HeaderInfo {

        /** The value length, either in bytes or characters. */
        private final int valueLength;
        /** The header length in bytes. */
        private final int headerLength;

        /**
         * Creates a new header info object.
         *
         * @param headerLength the header length in bytes
         * @param valueLength the value length (chars or bytes)
         */
        HeaderInfo(int headerLength, int valueLength) {
            this.headerLength = headerLength;
            this.valueLength = valueLength;
        }

        /**
         * Returns the header length in bytes.
         *
         * @return Number of bytes occupied by the header.
         */
       int headerLength() {
           return this.headerLength;
       }

       /**
        * Returns the character length encoded in the header, if any.
        *
        * @return A positive integer if a character count was encoded in the
        *       header, or {@code 0} (zero) if the header contained byte length
        *       information.
        */
       int charLength() {
           return isCharLength() ? valueLength : 0;
       }

       /**
        * Returns the byte length encoded in the header, if any.
        *
        * @return A positive integer if a byte count was encoded in the
        *       header, or {@code 0} (zero) if the header contained character
        *       length information.
        */
       int byteLength() {
           return isCharLength() ? 0 : valueLength;
       }

       /**
        * Tells whether the encoded length was in characters or bytes.
        *
        * @return {@code true} if the header contained a character count,
        *       {@code false} if it contained a byte count.
        */
       boolean isCharLength() {
           return (headerLength == 5);
       }

       /**
        * Returns a textual representation.
        */
       public String toString() {
           return ("headerLength=" + headerLength + ", valueLength= " +
                   valueLength + ", isCharLength=" + isCharLength());
       }
    }
}
