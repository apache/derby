/*

   Derby - Class org.apache.derby.catalog.UUID

   Copyright 1997, 2004 The Apache Software Foundation or its licensors, as applicable.

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

