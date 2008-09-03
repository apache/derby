/*

   Derby - Class org.apache.derby.impl.jdbc.InternalClob

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
package org.apache.derby.impl.jdbc;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.Writer;

import java.sql.SQLException;

/**
 * A set of operations available on internal Clob content representations.
 * <p>
 * The methods defined by {@link java.sql.Clob} must be implemented on top of
 * this interface. In addition, there are some methods to aid internal tasks and
 * organization, like transferring one internal Clob representation to another
 * one.
 */
interface InternalClob {

    /**
     * Gets the number of characters in the Clob.
     *
     * @return Number of characters in the Clob.
     * @throws IOException if accessing underlying I/O resources fail
     * @throws SQLException if accessing underlying resources fail
     */
    long getCharLength() throws IOException, SQLException;

    /**
     * Returns a stream serving the raw bytes of the Clob.
     * <p>
     * Note that it is up to the caller of this method to handle the issue of
     * encoding. There is no predetermined encoding associated with this byte
     * stream, it is up to the Clob representation which one it uses.
     * <p>
     * This stream may be an internal store stream, and should not be directly
     * published to the end user (returned through the JDBC API). There are
     * three reasons for this:
     * <ul> <li>the stream may be closed by the end user when it is
     *          not supposed to</li>
     *      <li>operations on the stream might throw exceptions we don't want to
     *          present to the end user unwrapped</li>
     *      <li>the stream may contain a Derby specific end-of-stream marker
     *      </li>
     * </ul>
     * <p>
     * The primary use of this method is to clone the Clob contents without
     * going via char (or String). Make sure the clone uses the same encoding
     * as the original Clob representation.
     *
     * @return A stream of bytes representing the content of the Clob,
     *      initialized at byte position 0.
     * @throws IOException if accessing underlying I/O resources fail
     * @throws SQLException if accessing underlying resources fail
     */
    InputStream getRawByteStream() throws IOException, SQLException;

    /**
     * Returns a reader for the Clob content, initialized at the specified
     * character position.
     *
     * @param characterPosition character position. The first character is at
     *      position <code>1</code>.
     * @return A {@code Reader} serving the content of the Clob.
     * @throws EOFException if the position is larger then the Clob
     * @throws IOException if accessing underlying I/O resources fail
     * @throws SQLException if accessing underlying resources fail
     */
    Reader getReader(long characterPosition) throws IOException, SQLException;

    /**
     * Returns a writer to write data into the Clob.
     * <p>
     * The semantics of the writer is the same as for {@link #insertString}.
     *
     * @param charPos the starting character position. The first character is
     *      at position <code>1</code>.
     * @return A writer initialized at the specified character position.
     * @throws IOException if writing to the Clob fails
     * @throws SQLException if accessing underlying resources fail
     * @throws UnsupportedOperationException if the Clob representation is
     *      read-only
     */
    Writer getWriter(long charPos) throws IOException, SQLException;

    /**
     * Inserts the given string at the specified character position.
     * <p>
     * The behavior of this method can be defined by the following examples on
     * the Clob <code>clob</code> with value <code>"ABCDEFG"</code>;
     * <ul> <li><code>clob.setString(2, "XX")</code> - "AXXDEFG"
     *      <li><code>clob.setString(1, "XX")</code> - "XXCDEFG"
     *      <li><code>clob.setString(8, "XX")</code> - "ABCDEFGXX"
     *      <li><code>clob.setString(7, "XX")</code> - "ABCDEFXX"
     *      <li><code>clob.setString(9, "XX")</code> - throws exception
     * </ul>
     *
     * @param str the string to insert
     * @param pos the character position the string will be inserted at. Must be
     *      between <code>1</code> and <code>clob.length() +1</code>, inclusive.
     * @return The number of characters inserted.
     * @throws IOException if writing to the I/O resources fail
     * @throws SQLException it the position is invalid
     * @throws IllegalArgumentException if the string is <code>null</code>
     * @throws UnsupportedOperationException if the Clob representation is
     *      read-only
     */
    long insertString(String str, long pos) throws IOException, SQLException;

    /**
     * Tells if the Clob representation is intended to be writable.
     * <p>
     * Note that even if this method returns <code>true</code>, it might not be
     * possible to write to the Clob. If this happens, it is because the
     * assoicated database is read-only, and the internal Clob representation is
     * unable to obtain the resources it require (could be an area on disk to
     * write temporary data).
     *
     * @return <code>true</code> if the Clob is intended to be writable, 
     *      <code>false</code> if modifying the Clob is definitely not possible.
     */
    boolean isWritable();

    /**
     * Frees the resources held by the internal Clob representation.
     * <p>
     * After calling this method, all other operations on the Clob will be
     * invalid and throw an exception.
     *
     * @throws IOException if freeing associated I/O resources fails
     * @throws SQLException if freeing associated resources fails
     */
    void release() throws IOException, SQLException;

    /**
     *
     * @param newLength the length in characters to truncate to
     *
     * @throws IOException if accessing the underlying I/O resources fails
     * @throws SQLException if accessing underlying resources fail
     * @throws UnsupportedOperationException if the Clob representation is
     *      read-only
     */
    void truncate(long newLength) throws IOException, SQLException;
} // End interface InternalClob
