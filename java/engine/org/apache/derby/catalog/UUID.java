/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.catalog
   (C) Copyright IBM Corp. 1997, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.catalog;

/**

 An interface for accessing Cloudscape UUIDs, unique identifiers.
		
	<p>The values in the
	system catalog held in ID columns with a type of CHAR(36) are the
	string representations of these UUIDs.

	<p>A UUID implements equals() and hashCode based on value equality.

 */

public interface UUID extends java.io.Externalizable
{
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1997_2004;
	/**
	  UUID_BYTE_LENGTH

	  The number of bytes in the array toByteArray returns.
	  */
	static int UUID_BYTE_LENGTH = 16;
	
	/**
		Produce a string representation of this UUID which
		is suitable for use as a unique ANSI identifier.
	 */
	String toANSIidentifier();

	/**
	    Produce a byte array representation of this UUID
		which can be passed to UUIDFactory.recreateUUID later
		on to reconstruct it.
	*/
	byte[] toByteArray();

	/**
	  Clone this UUID.

	  @return	a copy of this UUID
	  */
	UUID cloneMe();

	/**
	  Create a hex string representation of this UUID.
	  */
	String toHexString();
}

