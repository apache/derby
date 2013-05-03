/*

   Derby - Class org.apache.derby.client.net.CcsidManager

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

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import org.apache.derby.client.am.Agent;
import org.apache.derby.client.am.SqlException;

// Performs character conversions as required to send and receive PROTOCOL control data.
// User data uses the JVM's built in converters, i18n.jar,

abstract class CcsidManager {
    byte space_; // ' ' character
    byte dot_;   // '.' character

    // Byte array used to convert numbers into
    // bytes containing the character representation "value" for the particular ccsid.
    byte[] numToCharRepresentation_;

    // Special byte array to convert first half byte of CRRTKNs TCPIP address and port number
    // to a character.  This is required for SNA hopping.
    // This was specifically added to help build the CRRTKNs.
    byte[] numToSnaRequiredCrrtknChar_;

    /* DRDA CCSID levels for UTF8 and EBCDIC */
    static final int UTF8_CCSID = 1208;
    
    CcsidManager(byte space,
                 byte dot,
                 byte[] numToCharRepresentation,
                 byte[] numToSnaRequiredCrrtknChar) {
        space_ = space;
        dot_ = dot;
        numToCharRepresentation_ = numToCharRepresentation;
        numToSnaRequiredCrrtknChar_ = numToSnaRequiredCrrtknChar;
    }


    // Convert a Java String into bytes for a particular ccsid.
    //
    // @param sourceString A Java String to convert.
    // @return A new byte array representing the String in a particular ccsid.
    public abstract byte[] convertFromJavaString(
        String sourceString,
        Agent agent) throws SqlException;

    // Convert a byte array representing characters in a particular ccsid into a Java String.
    //
    // @param sourceBytes An array of bytes to be converted.
    // @param offset  An offset indicating first byte to convert.
    // @param numToConvert The number of bytes to be converted.
    // @return A new Java String Object created after conversion.
    abstract String convertToJavaString(byte[] sourceBytes, int offset, int numToConvert);

    /**
     * Initialize this instance for encoding a new string. This method resets
     * any internal state that may be left after earlier calls to
     * {@link #encode} on this instance. For example, it may reset the
     * internal {@code java.nio.charset.CharsetEncoder}, if the implementation
     * uses one to do the encoding.
     */
    public abstract void startEncoding();

    /**
     * Encode the contents of a {@code CharBuffer} into a {@code ByteBuffer}.
     * The method will return {@code true} if all the characters were encoded
     * and copied to the destination. If the receiving byte buffer is too small
     * to hold the entire encoded representation of the character buffer, the
     * method will return {@code false}. The caller should then allocate a
     * larger byte buffer, copy the contents from the old byte buffer to the
     * new one, and then call this method again to get the remaining characters
     * encoded.
     *
     * @param src buffer holding the characters to encode
     * @param dest buffer receiving the encoded bytes
     * @param agent where to report errors
     * @return {@code true} if all characters were encoded, {@code false} if
     * the destination buffer is full and there still are more characters to
     * encode
     * @throws SqlException if the characters cannot be encoded using this
     * CCSID manager's character encoding
     */
    public abstract boolean encode(
            CharBuffer src, ByteBuffer dest, Agent agent) throws SqlException;
}

