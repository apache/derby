/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.drda
   (C) Copyright IBM Corp. 2001, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */
package org.apache.derby.impl.drda;

// Peforms character conversions.
abstract class CcsidManager
{
	/**
		IBM Copyright &copy notice.
	*/

  private static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_2001_2004;
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
