/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.services.uuid
   (C) Copyright IBM Corp. 1998, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.iapi.services.uuid;

import org.apache.derby.catalog.UUID;

/*
	Internal comment (not for user documentation):
  Although this is an abstract interface, I believe that the
  underlying implementation of UUID will have to be DCE UUID.
  This is because the string versions of UUIDs get stored in
  the source code.  In other words, no matter what implementation
  is used for UUIDs, strings that look like this
  <blockquote><pre>
	E4900B90-DA0E-11d0-BAFE-0060973F0942
  </blockquote></pre>
  will always have to be turned into universally unique objects
  by the recreateUUID method
 */
/**
	
  Generates and recreates unique identifiers.
  
  An example of such an identifier is:
  <blockquote><pre>
	E4900B90-DA0E-11d0-BAFE-0060973F0942
  </blockquote></pre>
  These resemble DCE UUIDs, but use a different implementation.
  <P>
  The string format is designed to be the same as the string
  format produced by Microsoft's UUIDGEN program, although at
  present the bit fields are probably not the same.
  
 **/
public interface UUIDFactory 
{
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1998_2004;
	/**
	  Create a new UUID.  The resulting object is guaranteed
	  to be unique "across space and time".
	  @return		The UUID.
	**/
 	public UUID createUUID();

	/**
	  Recreate a UUID from a string produced by UUID.toString.
	  @return		The UUID.
	**/
	public UUID recreateUUID(String uuidstring);

	/**
	  Recreate a UUID from a byte array produced by UUID.toByteArray.
	  @return		The UUID.
	  @see UUID#toByteArray
	**/
	public UUID recreateUUID(byte[] b);
}

