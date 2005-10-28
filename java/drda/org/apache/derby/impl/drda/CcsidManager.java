/*

   Derby - Class org.apache.derby.impl.drda.CcsidManager

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

// Peforms character conversions.
abstract class CcsidManager
{
  byte space; // ' ' character
  byte dot;   // '.' character

  // Byte array used to convert numbers into
  // bytes containing the character representation "value" for the particular ccsid.
  byte[] numToCharRepresentation;

  CcsidManager (byte space, byte dot, byte[] numToCharRepresentation)
  {
    this.space = space;
    this.dot = dot;
    this.numToCharRepresentation = numToCharRepresentation;
  }


  // Convert a Java String into bytes for a particular ccsid.
  //
  // @param sourceString A Java String to convert.
  // @return A new byte array representing the String in a particular ccsid.
  abstract byte[] convertFromUCS2 (String sourceString);


  // Convert a Java String into bytes for a particular ccsid.
  // The String is converted into a buffer provided by the caller.
  //
  // @param sourceString  A Java String to convert.
  // @param buffer        The buffer to convert the String into.
  // @param offset        Offset in buffer to start putting output.
  // @return An int containing the buffer offset after conversion.
  abstract int convertFromUCS2 (String sourceString,
                                byte[] buffer,
                                int offset);

  // Convert a byte array representing characters in a particular ccsid into a Java String.
  //
  // @param sourceBytes An array of bytes to be converted.
  // @return String A new Java String Object created after conversion.
  abstract String convertToUCS2 (byte[] sourceBytes);


  // Convert a byte array representing characters in a particular ccsid into a Java String.
  //
  // @param sourceBytes An array of bytes to be converted.
  // @param offset  An offset indicating first byte to convert.
  // @param numToConvert The number of bytes to be converted.
  // @return A new Java String Object created after conversion.
  abstract String convertToUCS2 (byte[] sourceBytes, int offset, int numToConvert);

}
