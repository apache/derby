/*

   Derby - Class org.apache.derby.iapi.services.uuid.UUIDFactory

   Copyright 1998, 2004 The Apache Software Foundation or its licensors, as applicable.

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

