/*

   Derby - Class org.apache.derby.iapi.types.ClobStreamHeaderGenerator

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
package org.apache.derby.iapi.types;

import java.io.IOException;
import java.io.ObjectOutput;
import org.apache.derby.iapi.db.DatabaseContext;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.context.ContextService;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;

/**
 * Generates stream headers for Clob data values.
 * <p>
 * <em>THREAD SAFETY NOTE</em>: This class is considered thread safe, even
 * though it strictly speaking isn't. However, with the assumption that an
 * instance of this class cannot be shared across databases with different
 * versions, the only bad thing that can happen is that the mode is obtained
 * several times.
 */
//@ThreadSafe
public final class ClobStreamHeaderGenerator
    implements StreamHeaderGenerator {

    /** Magic byte for the 10.5 stream header format. */
    private static final byte MAGIC_BYTE = (byte)0xF0;

    /** Bytes for a 10.5 unknown length header. */
    private static final byte[] UNKNOWN_LENGTH = new byte[] {
                                            0x00, 0x00, MAGIC_BYTE, 0x00, 0x00};

    /**
     * Header generator for the pre 10.5 header format. This format is used
     * for Clobs as well if the database version is pre 10.5.
     */
    private static final CharStreamHeaderGenerator CHARHDRGEN =
            new CharStreamHeaderGenerator();

    /**
     * Reference to "owning" DVD, used to update it with information about
     * whether the database is being accessed in soft upgrade mode or not.
     * <p>
     * This is an optimization to avoid having to consult the data dictionary
     * on every request to generate a header when a data value descriptor is
     * reused.
     */
    private final StringDataValue callbackDVD;
    /**
     * {@code true} if database is being accessed in soft upgrade mode,
     * {@code false} is not. If {@code null}, the mode will be determined by
     * obtaining the database context through the context service.
     */
    private Boolean inSoftUpgradeMode;

    /**
     * Creates a new generator that will use the context manager to determine
     * if the database is being accessed in soft upgrade mode or not.
     *
     * @param dvd the owning data value descriptor
     */
    public ClobStreamHeaderGenerator(StringDataValue dvd) {
        if (dvd == null) {
            throw new IllegalStateException("dvd cannot be null");
        }
        this.callbackDVD = dvd;
    }

    /**
     * Creates a new generator for a database in the given mode.
     *
     * @param inSoftUpgradeMode {@code true} if the database is being accessed
     *      in soft upgrade mode, {@code false} if not
     */
    public ClobStreamHeaderGenerator(boolean inSoftUpgradeMode) {
        // Do not try to determine if we are in soft upgrade mode, use the
        // specified value for it.
        this.callbackDVD = null;
        this.inSoftUpgradeMode = Boolean.valueOf(inSoftUpgradeMode);
    }

    /**
     * Tells if the header encodes a character or byte count.
     * <p>
     * Currently the header expects a character count if the header format is
     * 10.5 (or newer), and a byte count if we are accessing a database in
     * soft upgrade mode.
     *
     * @return {@code false} if in soft upgrade mode, {@code true} if not.
     */
    public boolean expectsCharCount() {
        if (callbackDVD != null && inSoftUpgradeMode == null) {
            determineMode();
        }
        // Expect byte count if in soft upgrade mode, char count otherwise.
        return !inSoftUpgradeMode.booleanValue();
    }

    /**
     * Generates the header for the specified length and writes it into the
     * provided buffer, starting at the specified offset.
     *
     * @param buf the buffer to write into
     * @param offset starting offset in the buffer
     * @param valueLength the length to encode in the header
     * @return The number of bytes written into the buffer.
     */
    public int generateInto(byte[] buf, int offset, long valueLength) {
        if (callbackDVD != null && inSoftUpgradeMode == null) {
            determineMode();
        }
        int headerLength = 0;
        if (inSoftUpgradeMode == Boolean.FALSE) {
            // Write a 10.5 stream header format.
            // Assume the length specified is a char count.
            if (valueLength >= 0){
                // Encode the character count in the header.
                buf[offset + headerLength++] = (byte)(valueLength >>> 24);
                buf[offset + headerLength++] = (byte)(valueLength >>> 16);
                buf[offset + headerLength++] = MAGIC_BYTE;
                buf[offset + headerLength++] = (byte)(valueLength >>>  8);
                buf[offset + headerLength++] = (byte)(valueLength >>>  0);
            } else {
                // Write an "unknown length" marker.
                headerLength = UNKNOWN_LENGTH.length;
                System.arraycopy(UNKNOWN_LENGTH, 0, buf, offset, headerLength);
            }
        } else {
            // Write a pre 10.5 stream header format.
            headerLength = CHARHDRGEN.generateInto(buf, offset, valueLength);
        }
        return headerLength;
    }

    /**
     * Generates the header for the specified length.
     *
     * @param out the destination stream
     * @param valueLength the length to encode in the header
     * @return The number of bytes written to the destination stream.
     * @throws IOException if writing to the destination stream fails
     */
    public int generateInto(ObjectOutput out, long valueLength)
            throws IOException {
        if (callbackDVD != null && inSoftUpgradeMode == null) {
            determineMode();
        }
        int headerLength = 0;
        if (inSoftUpgradeMode == Boolean.FALSE) {
            // Write a 10.5 stream header format.
            headerLength = 5;
            // Assume the length specified is a char count.
            if (valueLength > 0){
                // Encode the character count in the header.
                out.writeByte((byte)(valueLength >>> 24));
                out.writeByte((byte)(valueLength >>> 16));
                out.writeByte(MAGIC_BYTE);
                out.writeByte((byte)(valueLength >>>  8));
                out.writeByte((byte)(valueLength >>>  0));
            } else {
                // Write an "unknown length" marker.
                out.write(UNKNOWN_LENGTH);
            }
        } else {
            // Write a pre 10.5 stream header format.
            headerLength = CHARHDRGEN.generateInto(out, valueLength);
        }
        return headerLength;
    }

    /**
     * Writes a Derby-specific end-of-stream marker to the buffer for a stream
     * of the specified character length, if required.
     *
     * @param buffer the buffer to write into
     * @param offset starting offset in the buffer
     * @param valueLength the length of the stream
     * @return Number of bytes written (zero or more).
     */
    public int writeEOF(byte[] buffer, int offset, long valueLength) {
        if (callbackDVD != null && inSoftUpgradeMode == null) {
            determineMode();
        }
        if (!inSoftUpgradeMode.booleanValue()) {
            if (valueLength < 0) {
                System.arraycopy(DERBY_EOF_MARKER, 0,
                                 buffer, offset, DERBY_EOF_MARKER.length);
                return DERBY_EOF_MARKER.length;
            } else {
                return 0;
            }
        } else {
            return CHARHDRGEN.writeEOF(buffer, offset, valueLength);
        }
    }

    /**
     * Writes a Derby-specific end-of-stream marker to the destination stream
     * for the specified character length, if required.
     *
     * @param out the destination stream
     * @param valueLength the length of the stream
     * @return Number of bytes written (zero or more).
     */
    public int writeEOF(ObjectOutput out, long valueLength)
            throws IOException {
        if (callbackDVD != null && inSoftUpgradeMode == null) {
            determineMode();
        }
        if (!inSoftUpgradeMode.booleanValue()) {
            if (valueLength < 0) {
                out.write(DERBY_EOF_MARKER);
                return DERBY_EOF_MARKER.length;
            } else {
                return 0;
            }
        } else {
            return CHARHDRGEN.writeEOF(out, valueLength);
        }
    }

    /**
     * Determines if the database being accessed is accessed in soft upgrade
     * mode or not.
     */
    private void determineMode() {
        DatabaseContext dbCtx = (DatabaseContext)
                ContextService.getContext(DatabaseContext.CONTEXT_ID);
        if (dbCtx == null) {
            throw new IllegalStateException("No context, unable to determine " +
                    "which stream header format to generate");
        } else {
            DataDictionary dd = dbCtx.getDatabase().getDataDictionary();
            try {
                inSoftUpgradeMode = Boolean.valueOf(!dd.checkVersion(
                        DataDictionary.DD_VERSION_DERBY_10_5, null));
            } catch (StandardException se) {
                // This should never happen as long as the second argument
                // above is null. If it happens, just bomb out.
                IllegalStateException ise =
                        new IllegalStateException(se.getMessage());
                ise.initCause(se);
                throw ise;
            }
            // Update the DVD with information about the mode the database is
            // being accessed in. It is assumed that a DVD is only shared
            // within a single database, i.e. the mode doesn't change during
            // the lifetime of the DVD.
            callbackDVD.setSoftUpgradeMode(inSoftUpgradeMode);
        }
    }
}
