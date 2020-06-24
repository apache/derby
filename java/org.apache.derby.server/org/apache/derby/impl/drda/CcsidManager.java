/*

   Derby - Class org.apache.derby.impl.drda.CcsidManager

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

import java.nio.ByteBuffer;

// Peforms character conversions.
abstract class CcsidManager
{
  byte space; // ' ' character
  byte dot;   // '.' character

  // Byte array used to convert numbers into
  // bytes containing the character representation "value" for the particular ccsid.
  byte[] numToCharRepresentation;

  /* DRDA CCSID level for UTF8 */
  public static final int UTF8_CCSID = 1208;
  
  CcsidManager (byte space, byte dot, byte[] numToCharRepresentation)
  {
    this.space = space;
    this.dot = dot;
    this.numToCharRepresentation = numToCharRepresentation;
  }

  /**
   * Returns the length in bytes for the String str using a particular ccsid.
   * @param str The Java String from which to obtain the length.
   * @return The length in bytes of the String str.
   */
//IC see: https://issues.apache.org/jira/browse/DERBY-4746
  abstract int getByteLength (String str);
  
  // Convert a Java String into bytes for a particular ccsid.
  //
  // @param sourceString A Java String to convert.
  // @return A new byte array representing the String in a particular ccsid.
  abstract byte[] convertFromJavaString (String sourceString);

//IC see: https://issues.apache.org/jira/browse/DERBY-728

    /**
     * Convert a Java String into bytes for a particular ccsid.
     * The String is converted into a buffer provided by the caller.
     *
     * @param sourceString  A Java String to convert.
     * @param buffer        The buffer to convert the String into.
     */
    abstract void convertFromJavaString(String sourceString, ByteBuffer buffer);
//IC see: https://issues.apache.org/jira/browse/DERBY-728

  // Convert a byte array representing characters in a particular ccsid into a Java String.
  //
  // @param sourceBytes An array of bytes to be converted.
  // @return String A new Java String Object created after conversion.
  abstract String convertToJavaString (byte[] sourceBytes);


  /**
   * Convert a byte array representing characters in a particular ccsid into a Java String.
   * 
   * Mind the fact that for certain encodings (e.g. UTF8), the offset and numToConvert
   * actually represent characters and 1 character does not always equal to 1 byte.
   * 
   * @param sourceBytes An array of bytes to be converted.
   * @param offset An offset indicating first byte to convert.
   * @param numToConvert The number of bytes to be converted.
   * @return A new Java String Object created after conversion.
   */
  abstract String convertToJavaString (byte[] sourceBytes, int offset, int numToConvert);

}
