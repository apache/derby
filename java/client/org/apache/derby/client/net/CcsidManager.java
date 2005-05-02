/*

   Derby - Class org.apache.derby.client.net.CcsidManager

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

// Performs character conversions as required to send and receive PROTOCOL control data.
// User data uses the JVM's built in converters, i18n.jar,

public abstract class CcsidManager {
    public byte space_; // ' ' character
    byte dot_;   // '.' character

    // Byte array used to convert numbers into
    // bytes containing the character representation "value" for the particular ccsid.
    byte[] numToCharRepresentation_;

    // Special byte array to convert first half byte of CRRTKNs TCPIP address and port number
    // to a character.  This is required for SNA hopping.
    // This was specifically added to help build the CRRTKNs.
    byte[] numToSnaRequiredCrrtknChar_;

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
    public abstract byte[] convertFromUCS2(String sourceString, org.apache.derby.client.am.Agent agent) throws org.apache.derby.client.am.SqlException;


    // Convert a Java String into bytes for a particular ccsid.
    // The String is converted into a buffer provided by the caller.
    //
    // @param sourceString  A Java String to convert.
    // @param buffer        The buffer to convert the String into.
    // @param offset        Offset in buffer to start putting output.
    // @return An int containing the buffer offset after conversion.
    public abstract int convertFromUCS2(String sourceString,
                                        byte[] buffer,
                                        int offset,
                                        org.apache.derby.client.am.Agent agent) throws org.apache.derby.client.am.SqlException;

    // Convert a byte array representing characters in a particular ccsid into a Java String.
    //
    // @param sourceBytes An array of bytes to be converted.
    // @return String A new Java String Object created after conversion.
    abstract String convertToUCS2(byte[] sourceBytes);


    // Convert a byte array representing characters in a particular ccsid into a Java String.
    //
    // @param sourceBytes An array of bytes to be converted.
    // @param offset  An offset indicating first byte to convert.
    // @param numToConvert The number of bytes to be converted.
    // @return A new Java String Object created after conversion.
    abstract String convertToUCS2(byte[] sourceBytes, int offset, int numToConvert);


    // Convert a byte representing a char in a particular ccsid into a Java char.
    //
    // @param sourceByte The byte to be converted
    // @return The converted Java char.
    abstract char convertToUCS2Char(byte sourceByte);

}

