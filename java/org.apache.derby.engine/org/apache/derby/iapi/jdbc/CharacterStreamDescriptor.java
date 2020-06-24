/*

   Derby - Class org.apache.derby.iapi.jdbc.CharacterStreamDescriptor

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
package org.apache.derby.iapi.jdbc;

import java.io.InputStream;
import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derby.iapi.types.PositionedStream;

/**
 * A description of a byte stream representing characters. The description is
 * used by decoders to properly configure themselves. Note that encoding is not
 * included in the description, because all internal byte streams are expected
 * to be using the modified UTF-8 encoding (see DataInput).
 * <p>
 * The information in the description is only guaranteed to be valid at the
 * moment it is passed to the decoder object. As the decoder works on the
 * stream, the information in the descriptor will be outdated.
 * <p>
 * To create a stream descriptor, obtain a {@code Builder} instance and set the
 * required parameters.
 *
 * @see Builder
 */
//@Immutable
public class CharacterStreamDescriptor {

    /**
     * Constant for the character position, when it is positioned before the
     * first character in the stream (i.e. at the very beginning of the stream
     * or in the header).
     */
    public static final long BEFORE_FIRST = 0;

    /** First data byte in the byte stream. */
    private final long dataOffset;
    /** The current byte position. */
    private final long curBytePos;
    /** The current character position. */
    private final long curCharPos;
    /** The byte length of the stream, {@code 0} if unknown. */
    private final long byteLength;
    /** The character length of the stream, {@code 0} if unknown. */
    private final long charLength;
    /** The maximum allowed character length. */
    private final long maxCharLength;
    /** Tells if the stream can be buffered or not. */
    private final boolean bufferable;
    /** Tells if the stream is aware of its own position. */
    private final boolean positionAware;
    /** Reference to the stream we are describing. */
    private final InputStream stream;

    /**
     * Creates a character stream descriptor, using the supplied builder.
     * <p>
     * Use the builder to create instances of this class.
     *
     * @param b object builder
     * @see Builder
     */
    private CharacterStreamDescriptor(Builder b) {
        bufferable = b.bufferable;
        positionAware = b.positionAware;
        dataOffset = b.dataOffset;
        curBytePos = b.curBytePos;
        curCharPos = b.curCharPos;
        byteLength = b.byteLength;
        charLength = b.charLength;
        maxCharLength = b.maxCharLength;
//IC see: https://issues.apache.org/jira/browse/DERBY-3936
        stream = b.stream;
    }

    /**
     * Tells if the described stream should be buffered or not.
     * <p>
     * Some of the reasons a stream should not be buffered at this level, are
     * the stream is already buffered, or it serves bytes directly from a byte
     * array in memory.
     *
     * @return {@code true} if the stream should be buffered for improved
     *      performance, {@code false} if it should not be buffered.
     */
    public boolean isBufferable() {
        return bufferable;
    }

    /**
     * Tells if the described stream is aware of its own position, and that it
     * can reposition itself on request.
     *
     * @return {@code true} if the stream is position aware, @{code false}
     *      otherwise.
     */
    public boolean isPositionAware() {
        return positionAware;
    }

    public long getByteLength() {
        return byteLength;
    }

    public long getCharLength() {
        return charLength;
    }

    public long getCurBytePos() {
        return curBytePos;
    }

    /**
     * Returns the current character position.
     *
     * @return The current character position, where the first character is at
     *      position {@code 1}, or {@code BEFORE_FIRST} if the stream is
     *      positioned before the first character.
     */
    public long getCurCharPos() {
        return curCharPos;
    }

    /**
     * Returns the first index of the described stream that contains real data.
     * <p>
     * The information is typically used to filter out meta data at the head of
     * the stream, and to correctly reset the stream.
     *
     * @return The first position in the stream containing real data.
     */
    public long getDataOffset() {
        return dataOffset;
    }

    /**
     * Returns the imposed maximum character length on the described stream.
     * <p>
     * The default value is {@code Long.MAX_VALUE}.
     *
     * @return The max allowed character length of the stream, or {@code 0} if
     *      no limit has been set.
     */
    public long getMaxCharLength() {
        return maxCharLength;
    }

    /**
     * Returns the associated stream.
     *
     * @return An {@code InputStream} reference.
     */
    public InputStream getStream() {
//IC see: https://issues.apache.org/jira/browse/DERBY-3936
        return stream;
    }

    /**
     * Returns the associated positioned stream, if the stream is position
     * aware.
     *
     * @return A {@code PositionedStream} reference.
     * @throws ClassCastException if the stream cannot be cast to
     *      {@code PositionedStream}
     * @throws IllegalArgumentException if the method is called and the
     *      assoicated stream isn't described as position aware.
     * @see #isPositionAware
     */
    public PositionedStream getPositionedStream() {
        if (!positionAware) {
            throw new IllegalStateException("stream is not position aware: " +
                    stream.getClass().getName());
        }
        return (PositionedStream)stream;
    }

    public String toString() {
        return ("CharacterStreamDescriptor-" + hashCode() +"#bufferable=" +
                bufferable + ":positionAware=" +
                positionAware + ":byteLength=" + byteLength + ":charLength=" +
                charLength + ":curBytePos=" + curBytePos + ":curCharPos=" +
//IC see: https://issues.apache.org/jira/browse/DERBY-3936
                curCharPos + ":dataOffset=" + dataOffset + ":stream=" +
                stream.getClass());
    }

    /**
     * The builder for the {@code CharacterStreamDescriptor} class. The builder
     * is used to avoid having a large set of constructors. See the
     * {@linkplain #build} method for pre-build field validation. Note that the
     * validation is only performed in sane builds.
     */
    public static class Builder {
 
        /** Default max character length is unlimited. */
        private static final long DEFAULT_MAX_CHAR_LENGTH = Long.MAX_VALUE;

        // See documentation for the fields in the CharacterStreamDescriptor
        // class. The values below are the field defaults.
        private boolean bufferable = false;
        private boolean positionAware = false;
        private long curBytePos = 0;
        private long curCharPos = 1;
        private long byteLength = 0;
        private long charLength = 0;
        private long dataOffset = 0;
        private long maxCharLength = DEFAULT_MAX_CHAR_LENGTH;
        private InputStream stream;

        /**
         * Creates a builder object.
         */
        public Builder() {}

        /**
         * Sets if the stream should be buffered, defaults to {@code false}.
         *
         * @param bufferable {@code true} if buffering is advised, {@code false}
         *      if not
         * @return The builder.
         */
        public Builder bufferable(boolean bufferable) {
            this.bufferable = bufferable;
            return this;
        }

        /**
         * Sets if the stream can reposition itself or not, defaults to
         * {@code false}.
         *
         * @param positionAware {@code true} if the stream can reposition
         *      itself, {@code false} if not
         * @return The builder.
         */
        public Builder positionAware(boolean positionAware) {
            this.positionAware = positionAware;
            return this;
        }

        /**
         * Sets the current byte position, defaults to {@code 0}.
         *
         * @param pos the current byte position
         * @return The builder.
         */
        public Builder curBytePos(long pos) {
            this.curBytePos = pos;
            return this;
        }

        /**
         * Sets the current character position, defaults to {@code 1}.
         * <p>
         * There is a special value for when the stream is position in the
         * header area - {@code BEFORE_FIRST}.
         *
         * @param pos the current character position,starting at {@code 1}
         * @return The builder.
         * @see #BEFORE_FIRST
         */
        public Builder curCharPos(long pos) {
            this.curCharPos = pos;
            return this;
        }

        /**
         * Sets the byte length of the stream, defaults to {@code 0}.
         * <p>
         * A length of {@code 0} means the length is unknown.
         *
         * @param length the byte length of the stream (including header)
         * @return The builder.
         */
        public Builder byteLength(long length) {
            this.byteLength = length;
            return this;
        }

        /**
         * Copies the state of the specified descriptor.
         *
         * @param csd the descriptor to copy
         * @return The builder.
         */
        public Builder copyState(CharacterStreamDescriptor csd) {
//IC see: https://issues.apache.org/jira/browse/DERBY-3936
            this.bufferable = csd.bufferable;
            this.byteLength = csd.byteLength;
            this.charLength = csd.charLength;
            this.curBytePos = csd.curBytePos;
            this.curCharPos = csd.curCharPos;
            this.dataOffset = csd.dataOffset;
            this.maxCharLength = csd.maxCharLength;
            this.positionAware = csd.positionAware;
            this.stream = csd.stream;
            return this;
        }

        /**
         * Sets the character length of the stream, defaults to {@code 0}.
         * <p>
         * Headers are not included in this length, only the user data.
         * A length of {@code 0} means the length is unknown.
         *
         * @param length the character length of the stream
         * @return The builder.
         */
        public Builder charLength(long length) {
            this.charLength = length;
            return this;
        }

        /**
         * Sets the offset of the user data, defaults to {@code 0}.
         *
         * @param offset first index with user data, zero based
         * @return The builder.
         */
        public Builder dataOffset(long offset) {
            this.dataOffset = offset;
            return this;
        }

        /**
         * Imposes a length limit on the stream, expressed in number of
         * characters, defaults to {@code Long.MAX_VALUE}.
         *
         * @param length maximum number of characters
         * @return The builder.
         */
        public Builder maxCharLength(long length) {
            this.maxCharLength = length;
            return this;
        }

        /**
         * Sets the stream described by the descriptor.
         * <p>
         * The stream is not allowed to be {@code null}.
         *
         * @param stream the stream
         * @return The builder.
         */
        public Builder stream(InputStream stream) {
//IC see: https://issues.apache.org/jira/browse/DERBY-3936
            if (SanityManager.DEBUG) {
                SanityManager.ASSERT(stream != null);
            }
            this.stream = stream;
            return this;
        }

        /**
         * Creates a descriptor object based on the parameters kept in the
         * builder instance.
         * <p>
         * Default values will be used for parameters for which a value hasn't
         * been set.
         * <p>
         * <b>NOTE</b>: Parameter validation is only performed in sane builds.
         *
         * @return A character stream descriptor instance.
         */
        public CharacterStreamDescriptor build() {
            // Do validation only in sane builds.
            if (SanityManager.DEBUG) {
//IC see: https://issues.apache.org/jira/browse/DERBY-3907
                SanityManager.ASSERT(curBytePos >= 0, "Negative curBytePos");
                SanityManager.ASSERT(curCharPos >= 1 ||
                        curCharPos == BEFORE_FIRST, "Invalid curCharPos " +
                        "(BEFORE_FIRST=" + BEFORE_FIRST + "), " + toString());
                SanityManager.ASSERT(byteLength >= 0, "Negative byteLength");
                SanityManager.ASSERT(charLength >= 0, "Negative charLength");
                SanityManager.ASSERT(dataOffset >= 0, "Negative dataOffset");
                SanityManager.ASSERT(maxCharLength >= 0, "Negative max length");

                // If current byte pos is set, require char pos to be set too.
                if ((curBytePos != 0 && curCharPos == 0) || 
                        (curBytePos == 0 && curCharPos > 1)) {
                    SanityManager.THROWASSERT("Invalid byte/char pos: " +
                            curBytePos + "/" + curCharPos);
                }
                // The byte position cannot be smaller than the character
                // position minus one (at least one byte per char).
                SanityManager.ASSERT(curBytePos >= curCharPos -1);
                // If we're in the header section, the character position must
                // be before the first character.
                if (curBytePos < dataOffset) {
//IC see: https://issues.apache.org/jira/browse/DERBY-3907
                    SanityManager.ASSERT(curCharPos == BEFORE_FIRST,
                            "curCharPos in header, " + toString());
                }
                // Byte length minus data offset must be equal to or greater
                // then the character length.
//IC see: https://issues.apache.org/jira/browse/DERBY-3936
                if (byteLength > 0 && charLength > 0) {
                    SanityManager.ASSERT(byteLength - dataOffset >= charLength,
                            "Less than one byte per char, " + toString());
                }
                SanityManager.ASSERT(stream != null, "Stream cannot be null");
                if (positionAware) {
                    SanityManager.ASSERT(stream instanceof PositionedStream,
                            "Stream not a positioned stream, " + toString());
                }
                // Note that the character position can be greater than the
                // maximum character length, because the limit might be imposed
                // as part of extracting a substring of the contents.
            }
            return new CharacterStreamDescriptor(this);
        }

        /**
         * Returns a textual representation of the builder.
         *
         * @return The textual representation of the builder.
         */
        public String toString() {
//IC see: https://issues.apache.org/jira/browse/DERBY-3907
            String str = "CharacterStreamBuiler@"  + hashCode() +
                    ":bufferable=" + bufferable + ", isPositionAware=" +
                    positionAware + ", curBytePos=" + curBytePos +
                    ", curCharPos=" + curCharPos + ", dataOffset=" +
                    dataOffset + ", byteLength=" + byteLength +
                    ", charLength=" + charLength + ", maxCharLength=" +
                    maxCharLength + ", stream=" + stream.getClass();
            return str;
        }
    }
}
